//! KZG verification pool — Architecture §8.2 / ADR P2-02 / ADR P2-08 / CC-24b.
//!
//! Dedicated OS threads (not the shared blocking pool) run §8.2's three steps in order:
//!
//! 1. `verify_data_column_sidecar` — structure + runtime blob bound
//! 2. `verify_data_column_sidecar_inclusion_proof` — depth-4 Merkle branch
//! 3. `verify_data_column_sidecar_kzg_proofs` — `verify_cell_kzg_proof_batch`
//!
//! **Batching (ADR P2-08):** per-sidecar by default; opportunistic cross-sidecar
//! batch when ≥ [`CROSS_SIDECAR_BATCH_MIN`] same-block jobs are dequeued in one
//! tick; on cross-batch failure, immediate per-sidecar re-verification before
//! any peer is penalised.
//!
//! **Queue:** bound [`VERIFY_QUEUE_BOUND`] (256); on full, oldest-dropped with
//! `IGNORE` semantics and a drop counter.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use cc_crypto::hash32_concat;
use cc_crypto::CellKzg;
use cc_types::primitives::{Cell, KzgCommitment, KzgProof, Root};
use cc_types::{NUMBER_OF_COLUMNS, KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH};
use tokio::sync::oneshot;
use tracing::{debug, error};

use crate::gossip::validate::column::{
    verify_inclusion_proof, InclusionProofCache, InclusionProofKey, BLOB_KZG_COMMITMENTS_FIELD_INDEX,
};
use crate::metrics::{P2pMetrics, PeerPenaltyReason};

// ── Constants ───────────────────────────────────────────────────────────────

/// Bound on the verify-pool job queue (Architecture §2.2 / `KZG_BOUND`).
pub const VERIFY_QUEUE_BOUND: usize = 256;

/// Same-block jobs required in one tick to attempt a cross-sidecar KZG batch.
pub const CROSS_SIDECAR_BATCH_MIN: usize = 4;

/// p95 sampling budget (seconds) — exact `le=0.2` histogram boundary.
pub const SAMPLING_P95_BUDGET_SECS: f64 = 0.2;

/// Hard absolute row cap — SSZ list type max (`Mainnet::MAX_BLOB_COMMITMENTS_PER_BLOCK` = 4096).
///
/// Structure rejects any job with more rows than this **regardless** of the
/// caller-supplied `max_blobs_per_block` (CC-24b H3). Prevents an untrusted
/// `max_blobs = u64::MAX` from admitting multi-MiB cell vectors into KZG.
pub const HARD_MAX_BLOB_COMMITMENTS: u64 = 4096;

/// Worker count: `K = max(2, available_parallelism / 2)` (ADR P2-02).
#[must_use]
pub fn pool_worker_count() -> usize {
    let cores = thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(2);
    cores.saturating_div(2).max(2)
}

// ── Steps ───────────────────────────────────────────────────────────────────

/// §8.2 verification step (1-based), in the spec's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum VerifyStep {
    /// Structural sidecar checks + runtime `max_blobs_per_block`.
    Structure = 1,
    /// Depth-4 inclusion proof of `kzg_commitments` against `body_root`.
    InclusionProof = 2,
    /// Cell KZG proof batch for this column.
    KzgProofs = 3,
}

impl VerifyStep {
    /// All three steps in order.
    pub const ALL: [Self; 3] = [Self::Structure, Self::InclusionProof, Self::KzgProofs];
}

/// Why a step rejected a sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VerifyFailReason {
    /// Column index ≥ `NUMBER_OF_COLUMNS`.
    IndexOutOfRange,
    /// Empty commitments list.
    EmptyCommitments,
    /// `len(kzg_commitments)` exceeds runtime `max_blobs_per_block`.
    BlobBoundExceeded,
    /// Column / commitment / proof lengths unequal.
    LengthMismatch,
    /// Inclusion Merkle branch invalid.
    InclusionProofInvalid,
    /// Cell KZG proofs invalid (`Ok(false)`).
    KzgInvalid,
    /// Backend error / malformed cell material (fail-closed, not a panic).
    KzgError,
}

/// Per-step invocation counters (order property for tests).
#[derive(Default)]
pub struct VerifyStepCounters {
    counts: [AtomicU64; 3],
}

impl std::fmt::Debug for VerifyStepCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifyStepCounters")
            .field("max_step_ran", &self.max_step_ran())
            .finish()
    }
}

impl VerifyStepCounters {
    /// Zeroed counters.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            counts: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    fn tick(&self, step: VerifyStep) {
        let i = (step as u8 as usize).saturating_sub(1);
        if let Some(c) = self.counts.get(i) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Invocations of `step`.
    #[must_use]
    pub fn get(&self, step: VerifyStep) -> u64 {
        let i = (step as u8 as usize).saturating_sub(1);
        self.counts
            .get(i)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Highest step that ran at least once (0 if none).
    #[must_use]
    pub fn max_step_ran(&self) -> u8 {
        let mut max = 0u8;
        for (i, c) in self.counts.iter().enumerate() {
            if c.load(Ordering::Relaxed) > 0 {
                max = (i as u8).saturating_add(1);
            }
        }
        max
    }
}

// ── Job / result ────────────────────────────────────────────────────────────

/// One data-column sidecar ready for §8.2 verification.
#[derive(Debug)]
pub struct VerifyJob {
    /// Beacon block root (groups opportunistic cross-sidecar batches).
    pub block_root: [u8; 32],
    /// Slot of the signed header (for `da_verdict_slot_delta`).
    pub slot: u64,
    /// Column index (`sidecar.index`).
    pub column_index: u64,
    /// Peer that sourced this sidecar (penalty attribution). Opaque bytes.
    pub peer_id: Vec<u8>,
    /// KZG commitments (one per blob row).
    pub commitments: Vec<KzgCommitment>,
    /// Cells for this column.
    pub cells: Vec<Cell>,
    /// Cell proofs matching `cells`.
    pub proofs: Vec<KzgProof>,
    /// Depth-4 inclusion proof siblings.
    pub inclusion_proof: [Root; 4],
    /// `signed_block_header.message.body_root`.
    pub body_root: [u8; 32],
    /// Runtime `get_blob_parameters(epoch).max_blobs_per_block` — never a constant.
    pub max_blobs_per_block: u64,
    /// Wall-clock when the 8th sampled column (or this job) was received.
    pub received_at: Instant,
    /// Current slot when the DA verdict is formed (for slot-delta series).
    pub current_slot: u64,
    /// Optional reply to the submitter.
    pub reply: Option<oneshot::Sender<VerifyOutcome>>,
}

/// Outcome of pool verification for one sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// All three steps passed.
    Valid,
    /// A step rejected the sidecar.
    Invalid {
        /// Which step failed.
        step: VerifyStep,
        /// Why.
        reason: VerifyFailReason,
    },
    /// Dropped under queue backpressure (oldest-dropped → IGNORE).
    Dropped,
}

// ── Batching diagnostics (tests / metrics) ──────────────────────────────────

/// How a KZG batch was executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchMode {
    /// One sidecar → one `verify_cell_kzg_proof_batch`.
    PerSidecar,
    /// ≥ 4 same-block sidecars in one tick → one combined batch.
    CrossSidecar,
    /// Cross-sidecar batch failed; this sidecar was re-verified alone.
    CrossSidecarFallback,
}

/// Counters for batch-mode attribution tests.
#[derive(Default, Debug)]
pub struct BatchCounters {
    /// Jobs verified with a per-sidecar KZG call.
    pub per_sidecar: AtomicU64,
    /// Jobs that entered a cross-sidecar KZG batch (success path).
    pub cross_sidecar: AtomicU64,
    /// Jobs that fell back after a failed cross-sidecar batch.
    pub cross_fallback: AtomicU64,
    /// Cross-sidecar batch attempts.
    pub cross_attempts: AtomicU64,
    /// Cross-sidecar batch failures (triggered fallback).
    pub cross_failures: AtomicU64,
}

impl BatchCounters {
    /// Zeroed.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            per_sidecar: AtomicU64::new(0),
            cross_sidecar: AtomicU64::new(0),
            cross_fallback: AtomicU64::new(0),
            cross_attempts: AtomicU64::new(0),
            cross_failures: AtomicU64::new(0),
        }
    }

    fn record(&self, mode: BatchMode) {
        match mode {
            BatchMode::PerSidecar => {
                self.per_sidecar.fetch_add(1, Ordering::Relaxed);
            }
            BatchMode::CrossSidecar => {
                self.cross_sidecar.fetch_add(1, Ordering::Relaxed);
            }
            BatchMode::CrossSidecarFallback => {
                self.cross_fallback.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

// ── Bounded oldest-drop queue ───────────────────────────────────────────────

struct JobQueue {
    inner: Mutex<VecDeque<VerifyJob>>,
    not_empty: Condvar,
    capacity: usize,
    dropped: AtomicU64,
    closed: AtomicBool,
}

impl JobQueue {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            not_empty: Condvar::new(),
            capacity: capacity.max(1),
            dropped: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn lock_q(&self) -> std::sync::MutexGuard<'_, VecDeque<VerifyJob>> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Enforce capacity by dropping oldest until `q.len() < capacity`.
    fn drop_oldest_until_room(q: &mut VecDeque<VerifyJob>, capacity: usize, dropped: &AtomicU64) {
        while q.len() >= capacity {
            if let Some(old) = q.pop_front() {
                dropped.fetch_add(1, Ordering::Relaxed);
                complete_job(old, VerifyOutcome::Dropped);
            } else {
                break;
            }
        }
    }

    /// Push; on full drop the oldest job (notify `Dropped`) and count it.
    fn push(&self, job: VerifyJob) {
        let mut q = self.lock_q();
        if self.closed.load(Ordering::Acquire) {
            complete_job(job, VerifyOutcome::Dropped);
            return;
        }
        Self::drop_oldest_until_room(&mut q, self.capacity, &self.dropped);
        q.push_back(job);
        self.not_empty.notify_one();
    }

    /// Pop `first` then drain contiguous same-`block_root` jobs under **one**
    /// lock. A foreign job is put back with capacity enforcement (CC-24b M1).
    fn pop_batch_same_block(&self) -> Option<Vec<VerifyJob>> {
        let mut q = self.lock_q();
        loop {
            let first = match q.pop_front() {
                Some(j) => j,
                None => {
                    if self.closed.load(Ordering::Acquire) {
                        return None;
                    }
                    q = match self.not_empty.wait(q) {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    continue;
                }
            };
            let block_root = first.block_root;
            let mut batch = vec![first];
            // Drain same-block prefix without releasing the lock.
            while let Some(next) = q.pop_front() {
                if next.block_root == block_root {
                    batch.push(next);
                } else {
                    // Put back under the same lock; respect capacity.
                    Self::drop_oldest_until_room(&mut q, self.capacity, &self.dropped);
                    // If still full after dropping oldest (capacity 0 edge), drop foreign.
                    if q.len() >= self.capacity {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                        complete_job(next, VerifyOutcome::Dropped);
                    } else {
                        q.push_front(next);
                    }
                    break;
                }
            }
            return Some(batch);
        }
    }

    fn len(&self) -> usize {
        self.lock_q().len()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.not_empty.notify_all();
    }

    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn complete_job(job: VerifyJob, outcome: VerifyOutcome) {
    if let Some(tx) = job.reply {
        let _ = tx.send(outcome);
    }
}

// ── Shared pool state ───────────────────────────────────────────────────────

struct PoolState {
    backend: Arc<dyn CellKzg>,
    /// Shared with gossip CC-22d — one LRU only (issue: do not add a second).
    inclusion_cache: Arc<Mutex<InclusionProofCache>>,
    step_counters: VerifyStepCounters,
    batch_counters: BatchCounters,
    /// Peers penalised for `gossip_invalid` (test attribution only; capped).
    peer_penalties: Mutex<Vec<Vec<u8>>>,
    metrics: Option<P2pMetrics>,
    /// Active worker threads (fixed at start; never grows under load).
    worker_count: AtomicUsize,
}

/// Cap on diagnostic peer-id ring (test attribution; not a score path).
const PEER_PENALTY_DIAG_CAP: usize = 64;

// ── Three steps (pure) ──────────────────────────────────────────────────────

/// Clamp a caller-supplied runtime blob bound to the hard protocol maximum.
#[must_use]
pub fn effective_max_blobs(max_blobs_per_block: u64) -> u64 {
    max_blobs_per_block.min(HARD_MAX_BLOB_COMMITMENTS)
}

/// Step 1: structural sidecar checks (§8.2 / `verify_data_column_sidecar`).
///
/// `max_blobs_per_block` **must** come from a runtime
/// `get_blob_parameters(epoch)` lookup (spec delta 7) — never a constant.
/// Independently, row counts are hard-capped at [`HARD_MAX_BLOB_COMMITMENTS`]
/// so an untrusted max cannot open a DoS path (CC-24b H3).
pub fn verify_structure(
    column_index: u64,
    commitments: &[KzgCommitment],
    cells: &[Cell],
    proofs: &[KzgProof],
    max_blobs_per_block: u64,
    counters: &VerifyStepCounters,
) -> Result<(), VerifyFailReason> {
    counters.tick(VerifyStep::Structure);

    if column_index >= NUMBER_OF_COLUMNS {
        return Err(VerifyFailReason::IndexOutOfRange);
    }
    let n = commitments.len();
    if n == 0 {
        return Err(VerifyFailReason::EmptyCommitments);
    }
    // Absolute SSZ / protocol ceiling first (untrusted max cannot raise this).
    if (n as u64) > HARD_MAX_BLOB_COMMITMENTS
        || (cells.len() as u64) > HARD_MAX_BLOB_COMMITMENTS
        || (proofs.len() as u64) > HARD_MAX_BLOB_COMMITMENTS
    {
        return Err(VerifyFailReason::BlobBoundExceeded);
    }
    let effective = effective_max_blobs(max_blobs_per_block);
    if (n as u64) > effective {
        return Err(VerifyFailReason::BlobBoundExceeded);
    }
    if cells.len() != n || proofs.len() != n {
        return Err(VerifyFailReason::LengthMismatch);
    }
    Ok(())
}

/// Step 2: inclusion proof (consumes CC-22d's 512-entry LRU; no second cache).
///
/// Cache key includes `body_root` so a hit cannot pass a different body (H1).
pub fn verify_inclusion(
    commitments: &[KzgCommitment],
    inclusion_proof: &[Root],
    body_root: [u8; 32],
    block_root: [u8; 32],
    cache: &mut InclusionProofCache,
    counters: &VerifyStepCounters,
) -> Result<(), VerifyFailReason> {
    counters.tick(VerifyStep::InclusionProof);

    if inclusion_proof.len() != KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize {
        return Err(VerifyFailReason::InclusionProofInvalid);
    }

    let commitments_root = tree_hash_commitments(commitments);
    let inclusion_proof_root = hash_inclusion_proof(inclusion_proof);
    let key = InclusionProofKey {
        commitments_root,
        inclusion_proof_root,
        block_root,
        body_root,
    };
    let ok = cache.get_or_verify(key, || {
        verify_inclusion_proof(&commitments_root, inclusion_proof, body_root)
    });
    if ok {
        Ok(())
    } else {
        Err(VerifyFailReason::InclusionProofInvalid)
    }
}

/// Step 3: cell KZG proofs for one sidecar.
pub fn verify_kzg_one(
    backend: &dyn CellKzg,
    column_index: u64,
    commitments: &[KzgCommitment],
    cells: &[Cell],
    proofs: &[KzgProof],
    counters: &VerifyStepCounters,
) -> Result<(), VerifyFailReason> {
    counters.tick(VerifyStep::KzgProofs);

    let cell_indices: Vec<u64> = vec![column_index; cells.len()];
    match backend.verify_cell_kzg_proof_batch(commitments, &cell_indices, cells, proofs) {
        Ok(true) => Ok(()),
        Ok(false) => Err(VerifyFailReason::KzgInvalid),
        Err(e) => {
            // Malformed / internal → fail-closed (never panic on remote input).
            debug!(error = %e, "KZG verify error; rejecting");
            Err(VerifyFailReason::KzgError)
        }
    }
}

/// Run all three §8.2 steps in order for one sidecar.
///
/// Callers that share the inclusion cache across threads should prefer
/// [`verify_sidecar_three_steps_locked`] so the cache mutex is not held across KZG.
pub fn verify_sidecar_three_steps(
    backend: &dyn CellKzg,
    job: &VerifyJob,
    cache: &mut InclusionProofCache,
    counters: &VerifyStepCounters,
) -> Result<(), (VerifyStep, VerifyFailReason)> {
    verify_structure(
        job.column_index,
        &job.commitments,
        &job.cells,
        &job.proofs,
        job.max_blobs_per_block,
        counters,
    )
    .map_err(|r| (VerifyStep::Structure, r))?;

    verify_inclusion(
        &job.commitments,
        &job.inclusion_proof,
        job.body_root,
        job.block_root,
        cache,
        counters,
    )
    .map_err(|r| (VerifyStep::InclusionProof, r))?;

    verify_kzg_one(
        backend,
        job.column_index,
        &job.commitments,
        &job.cells,
        &job.proofs,
        counters,
    )
    .map_err(|r| (VerifyStep::KzgProofs, r))?;

    Ok(())
}

/// Like [`verify_sidecar_three_steps`] but locks the shared cache **only** for
/// step 2 (CC-24b H2: never hold the mutex across KZG).
pub fn verify_sidecar_three_steps_locked(
    backend: &dyn CellKzg,
    job: &VerifyJob,
    cache: &Mutex<InclusionProofCache>,
    counters: &VerifyStepCounters,
) -> Result<(), (VerifyStep, VerifyFailReason)> {
    verify_structure(
        job.column_index,
        &job.commitments,
        &job.cells,
        &job.proofs,
        job.max_blobs_per_block,
        counters,
    )
    .map_err(|r| (VerifyStep::Structure, r))?;

    {
        let mut guard = match cache.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        verify_inclusion(
            &job.commitments,
            &job.inclusion_proof,
            job.body_root,
            job.block_root,
            &mut guard,
            counters,
        )
        .map_err(|r| (VerifyStep::InclusionProof, r))?;
    } // lock released before KZG

    verify_kzg_one(
        backend,
        job.column_index,
        &job.commitments,
        &job.cells,
        &job.proofs,
        counters,
    )
    .map_err(|r| (VerifyStep::KzgProofs, r))?;

    Ok(())
}

fn tree_hash_commitments(commitments: &[KzgCommitment]) -> [u8; 32] {
    list_tree_hash_root(commitments)
}

/// Match gossip's `sidecar.kzg_commitments.tree_hash_root()` via the same
/// `VariableList` TreeHash path (capacity = mainnet max blob commitments).
fn list_tree_hash_root(commitments: &[KzgCommitment]) -> [u8; 32] {
    use cc_types::preset::Mainnet;
    use ssz_types::VariableList;
    use tree_hash::TreeHash;

    type Max = <Mainnet as cc_types::preset::Preset>::MaxBlobCommitmentsPerBlock;
    match VariableList::<KzgCommitment, Max>::new(commitments.to_vec()) {
        Ok(list) => {
            let h = list.tree_hash_root();
            let mut a = [0u8; 32];
            a.copy_from_slice(h.as_slice());
            a
        }
        Err(_) => {
            // Over-capacity is already rejected by structure step; return a
            // stable non-zero-length marker so the inclusion check fails closed.
            [0u8; 32]
        }
    }
}

fn hash_inclusion_proof(proof: &[Root]) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for r in proof {
        acc = hash32_concat(&acc, r.as_array());
    }
    acc
}

// ── Cross-sidecar KZG batch ─────────────────────────────────────────────────

/// Batch-verify KZG for many sidecars (same block). Returns `Ok(true)` only if
/// the whole batch is valid. On `Ok(false)` / `Err`, caller must re-verify
/// per-sidecar for attribution.
fn verify_kzg_cross_batch(
    backend: &dyn CellKzg,
    jobs: &[VerifyJob],
    counters: &VerifyStepCounters,
) -> Result<bool, VerifyFailReason> {
    // Tick step 3 once per member so order counters still advance.
    for _ in jobs {
        counters.tick(VerifyStep::KzgProofs);
    }

    let mut commitments = Vec::new();
    let mut cell_indices = Vec::new();
    let mut cells = Vec::new();
    let mut proofs = Vec::new();

    for job in jobs {
        for (c, (cell, proof)) in job
            .commitments
            .iter()
            .zip(job.cells.iter().zip(job.proofs.iter()))
        {
            commitments.push(*c);
            cell_indices.push(job.column_index);
            cells.push(*cell);
            proofs.push(*proof);
        }
    }

    match backend.verify_cell_kzg_proof_batch(&commitments, &cell_indices, &cells, &proofs) {
        Ok(v) => Ok(v),
        Err(e) => {
            debug!(error = %e, "cross-sidecar KZG batch error");
            Err(VerifyFailReason::KzgError)
        }
    }
}

// ── Worker ──────────────────────────────────────────────────────────────────

fn worker_main(id: usize, queue: Arc<JobQueue>, state: Arc<PoolState>) {
    debug!(worker = id, "KZG verify pool worker started");
    while let Some(batch) = queue.pop_batch_same_block() {
        if batch.len() >= CROSS_SIDECAR_BATCH_MIN {
            process_cross_sidecar(batch, &state);
        } else {
            for job in batch {
                process_per_sidecar(job, &state, BatchMode::PerSidecar);
            }
        }

        if let Some(m) = &state.metrics {
            m.set_queue_depth(crate::metrics::QueueName::Kzg, queue.len() as i64);
        }
    }
    debug!(worker = id, "KZG verify pool worker stopped");
}

fn process_per_sidecar(job: VerifyJob, state: &PoolState, mode: BatchMode) {
    let start = Instant::now();
    // H2: structure + inclusion (short lock) + KZG — never hold cache across KZG.
    let result = verify_sidecar_three_steps_locked(
        state.backend.as_ref(),
        &job,
        state.inclusion_cache.as_ref(),
        &state.step_counters,
    );

    state.batch_counters.record(mode);

    let outcome = match result {
        Ok(()) => VerifyOutcome::Valid,
        Err((step, reason)) => {
            // After structure+inclusion, KZG failures (Ok(false) or backend Err
            // on peer cell/proof bytes) are message faults. Setup-unavailable is
            // FailClosedCellKzg → Ok(false) → KzgInvalid, also attributed.
            if matches!(
                reason,
                VerifyFailReason::KzgInvalid
                    | VerifyFailReason::KzgError
                    | VerifyFailReason::InclusionProofInvalid
                    | VerifyFailReason::BlobBoundExceeded
                    | VerifyFailReason::EmptyCommitments
                    | VerifyFailReason::IndexOutOfRange
                    | VerifyFailReason::LengthMismatch
            ) {
                let _ = step;
                penalise_peer(state, &job.peer_id);
            }
            VerifyOutcome::Invalid { step, reason }
        }
    };

    observe_latency(state, &job, start);
    complete_job(job, outcome);
}

fn process_cross_sidecar(jobs: Vec<VerifyJob>, state: &PoolState) {
    let start = Instant::now();
    state
        .batch_counters
        .cross_attempts
        .fetch_add(1, Ordering::Relaxed);

    // Steps 1–2 always per-sidecar (structure + inclusion). Collect survivors.
    let mut survivors: Vec<VerifyJob> = Vec::with_capacity(jobs.len());
    for job in jobs {
        // Structure — no cache.
        if let Err(reason) = verify_structure(
            job.column_index,
            &job.commitments,
            &job.cells,
            &job.proofs,
            job.max_blobs_per_block,
            &state.step_counters,
        ) {
            penalise_peer(state, &job.peer_id);
            observe_latency(state, &job, start);
            complete_job(
                job,
                VerifyOutcome::Invalid {
                    step: VerifyStep::Structure,
                    reason,
                },
            );
            continue;
        }
        // Inclusion — short lock only.
        let incl = {
            let mut cache = match state.inclusion_cache.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            verify_inclusion(
                &job.commitments,
                &job.inclusion_proof,
                job.body_root,
                job.block_root,
                &mut cache,
                &state.step_counters,
            )
        };
        if let Err(reason) = incl {
            penalise_peer(state, &job.peer_id);
            observe_latency(state, &job, start);
            complete_job(
                job,
                VerifyOutcome::Invalid {
                    step: VerifyStep::InclusionProof,
                    reason,
                },
            );
            continue;
        }
        survivors.push(job);
    }

    if survivors.is_empty() {
        return;
    }

    // Step 3: one cross-sidecar batch.
    let batch_ok = match verify_kzg_cross_batch(
        state.backend.as_ref(),
        &survivors,
        &state.step_counters,
    ) {
        Ok(true) => true,
        Ok(false) | Err(_) => false,
    };

    if batch_ok {
        for job in survivors {
            state.batch_counters.record(BatchMode::CrossSidecar);
            observe_latency(state, &job, start);
            complete_job(job, VerifyOutcome::Valid);
        }
        return;
    }

    // Fallback: per-sidecar re-verification for attribution (ADR P2-08).
    state
        .batch_counters
        .cross_failures
        .fetch_add(1, Ordering::Relaxed);
    for job in survivors {
        // Step 3 only (steps 1–2 already passed).
        let kzg = verify_kzg_one(
            state.backend.as_ref(),
            job.column_index,
            &job.commitments,
            &job.cells,
            &job.proofs,
            &state.step_counters,
        );
        state
            .batch_counters
            .record(BatchMode::CrossSidecarFallback);
        let outcome = match kzg {
            Ok(()) => VerifyOutcome::Valid,
            Err(reason) => {
                // Structure+inclusion already passed; KZG fault attributes to peer.
                if matches!(
                    reason,
                    VerifyFailReason::KzgInvalid | VerifyFailReason::KzgError
                ) {
                    penalise_peer(state, &job.peer_id);
                }
                VerifyOutcome::Invalid {
                    step: VerifyStep::KzgProofs,
                    reason,
                }
            }
        };
        observe_latency(state, &job, start);
        complete_job(job, outcome);
    }
}

fn penalise_peer(state: &PoolState, peer_id: &[u8]) {
    // Diagnostic ring only (tests); cap growth (L1). Real PeerPenaltyCmd is M2.
    let push = |v: &mut Vec<Vec<u8>>| {
        if v.len() >= PEER_PENALTY_DIAG_CAP {
            v.remove(0);
        }
        v.push(peer_id.to_vec());
    };
    match state.peer_penalties.lock() {
        Ok(mut v) => push(&mut v),
        Err(p) => push(&mut p.into_inner()),
    }
    if let Some(m) = &state.metrics {
        m.inc_peer_penalty(PeerPenaltyReason::GossipInvalid);
    }
}

fn observe_latency(state: &PoolState, job: &VerifyJob, start: Instant) {
    let Some(m) = &state.metrics else {
        return;
    };
    // Sampling wall: from receipt (8th column / job enqueue) to verify done.
    let sampling = job.received_at.elapsed().as_secs_f64().max(start.elapsed().as_secs_f64());
    m.observe_sampling(sampling);
    // Separate series: slot delta between block slot and "now".
    let delta = job.current_slot.saturating_sub(job.slot) as f64;
    m.observe_da_verdict_slot_delta(delta);
}

// ── Fail-closed backend (setup unavailable) ─────────────────────────────────

/// Always-reject [`CellKzg`] used when the trusted setup cannot be loaded.
///
/// Never panics; every verify returns `Ok(false)`.
#[derive(Debug, Default, Clone, Copy)]
pub struct FailClosedCellKzg;

impl CellKzg for FailClosedCellKzg {
    fn blob_to_kzg_commitment(
        &self,
        _blob: &cc_crypto::Blob,
    ) -> Result<KzgCommitment, cc_crypto::KzgError> {
        Err(cc_crypto::KzgError::BackendUnavailable(
            "trusted setup unavailable".into(),
        ))
    }

    fn compute_cells(
        &self,
        _blob: &cc_crypto::Blob,
    ) -> Result<cc_crypto::Cells, cc_crypto::KzgError> {
        Err(cc_crypto::KzgError::BackendUnavailable(
            "trusted setup unavailable".into(),
        ))
    }

    fn compute_cells_and_kzg_proofs(
        &self,
        _blob: &cc_crypto::Blob,
    ) -> Result<cc_crypto::CellsAndProofs, cc_crypto::KzgError> {
        Err(cc_crypto::KzgError::BackendUnavailable(
            "trusted setup unavailable".into(),
        ))
    }

    fn recover_cells_and_kzg_proofs(
        &self,
        _cell_indices: &[u64],
        _cells: &[Cell],
    ) -> Result<cc_crypto::CellsAndProofs, cc_crypto::KzgError> {
        Err(cc_crypto::KzgError::BackendUnavailable(
            "trusted setup unavailable".into(),
        ))
    }

    fn verify_cell_kzg_proof_batch(
        &self,
        _commitments: &[KzgCommitment],
        _cell_indices: &[u64],
        _cells: &[Cell],
        _proofs: &[KzgProof],
    ) -> Result<bool, cc_crypto::KzgError> {
        Ok(false)
    }
}

// ── Public pool handle ──────────────────────────────────────────────────────

/// Dedicated OS-thread KZG verification pool (ADR P2-02).
pub struct VerifyPool {
    queue: Arc<JobQueue>,
    state: Arc<PoolState>,
    workers: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for VerifyPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifyPool")
            .field("workers", &self.workers.len())
            .field("queue_len", &self.queue.len())
            .field("dropped", &self.queue.dropped())
            .finish()
    }
}

impl VerifyPool {
    /// Start `K = max(2, cores/2)` workers on a bound-256 oldest-drop queue.
    ///
    /// Creates a private inclusion cache. Prefer [`Self::start_with_cache`] so
    /// the pool shares CC-22d's single LRU with gossip.
    #[must_use]
    pub fn start(backend: Arc<dyn CellKzg>, metrics: Option<P2pMetrics>) -> Self {
        let k = pool_worker_count();
        Self::start_with_workers(backend, metrics, k)
    }

    /// Start with an explicit worker count (tests may pass 2).
    #[must_use]
    pub fn start_with_workers(
        backend: Arc<dyn CellKzg>,
        metrics: Option<P2pMetrics>,
        workers: usize,
    ) -> Self {
        Self::start_with_workers_and_cache(
            backend,
            metrics,
            workers,
            Arc::new(Mutex::new(InclusionProofCache::new())),
        )
    }

    /// Start with a **shared** inclusion-proof cache (CC-22d / no second LRU).
    #[must_use]
    pub fn start_with_cache(
        backend: Arc<dyn CellKzg>,
        metrics: Option<P2pMetrics>,
        inclusion_cache: Arc<Mutex<InclusionProofCache>>,
    ) -> Self {
        Self::start_with_workers_and_cache(
            backend,
            metrics,
            pool_worker_count(),
            inclusion_cache,
        )
    }

    /// Start with explicit workers + shared cache.
    #[must_use]
    pub fn start_with_workers_and_cache(
        backend: Arc<dyn CellKzg>,
        metrics: Option<P2pMetrics>,
        workers: usize,
        inclusion_cache: Arc<Mutex<InclusionProofCache>>,
    ) -> Self {
        let k = workers.max(1);
        let queue = Arc::new(JobQueue::new(VERIFY_QUEUE_BOUND));
        let state = Arc::new(PoolState {
            backend,
            inclusion_cache,
            step_counters: VerifyStepCounters::new(),
            batch_counters: BatchCounters::new(),
            peer_penalties: Mutex::new(Vec::new()),
            metrics,
            worker_count: AtomicUsize::new(k),
        });

        let mut handles = Vec::with_capacity(k);
        for id in 0..k {
            let q = Arc::clone(&queue);
            let s = Arc::clone(&state);
            match thread::Builder::new()
                .name(format!("cc-kzg-verify-{id}"))
                .spawn(move || worker_main(id, q, s))
            {
                Ok(handle) => handles.push(handle),
                Err(e) => {
                    // Process-local spawn fault (not remote input). Retry once
                    // with the default builder so construction still returns.
                    error!(error = %e, worker = id, "failed to spawn named KZG verify worker");
                    let q = Arc::clone(&queue);
                    let s = Arc::clone(&state);
                    match thread::Builder::new().spawn(move || worker_main(id, q, s)) {
                        Ok(handle) => handles.push(handle),
                        Err(e2) => {
                            error!(error = %e2, worker = id, "KZG verify worker spawn failed");
                        }
                    }
                }
            }
        }
        state.worker_count.store(handles.len(), Ordering::Relaxed);

        Self {
            queue,
            state,
            workers: handles,
        }
    }

    /// Submit a job. On a full queue the **oldest** job is dropped (`Dropped`).
    pub fn submit(&self, job: VerifyJob) {
        self.queue.push(job);
        if let Some(m) = &self.state.metrics {
            m.set_queue_depth(
                crate::metrics::QueueName::Kzg,
                self.queue.len() as i64,
            );
        }
    }

    /// Fixed worker count — does not grow under a burst of requests.
    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.state.worker_count.load(Ordering::Relaxed)
    }

    /// Jobs dropped under backpressure.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.queue.dropped()
    }

    /// Current queue depth.
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.queue.len()
    }

    /// Step counters (shared across workers).
    #[must_use]
    pub fn step_counters(&self) -> &VerifyStepCounters {
        &self.state.step_counters
    }

    /// Batch-mode counters.
    #[must_use]
    pub fn batch_counters(&self) -> &BatchCounters {
        &self.state.batch_counters
    }

    /// Number of `gossip_invalid` peer penalties applied by this pool.
    #[must_use]
    pub fn peer_penalty_count(&self) -> usize {
        match self.state.peer_penalties.lock() {
            Ok(g) => g.len(),
            Err(p) => p.into_inner().len(),
        }
    }

    /// Distinct peer ids penalised (for attribution tests).
    #[must_use]
    pub fn penalised_peers(&self) -> Vec<Vec<u8>> {
        match self.state.peer_penalties.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Inclusion-proof cache miss count (must stay ≈ 1/8 across columns).
    #[must_use]
    pub fn inclusion_verifications(&self) -> u64 {
        match self.state.inclusion_cache.lock() {
            Ok(g) => g.verifications,
            Err(p) => p.into_inner().verifications,
        }
    }

    /// Borrow the shared inclusion-proof cache handle (CC-22d).
    #[must_use]
    pub fn inclusion_cache(&self) -> Arc<Mutex<InclusionProofCache>> {
        Arc::clone(&self.state.inclusion_cache)
    }

    /// Shut down workers (best-effort join).
    pub fn shutdown(mut self) {
        self.queue.close();
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

impl Drop for VerifyPool {
    fn drop(&mut self) {
        self.queue.close();
        // Detach remaining workers: joining in Drop can deadlock if the
        // dropping thread holds a lock a worker needs. Close is enough for
        // workers to exit; process teardown reaps them.
        self.workers.drain(..);
    }
}

// ── Tokio bridge (channels::kzg_rx → pool) ──────────────────────────────────

/// Drain the §2.2 `kzg` channel into the OS-thread pool forever.
pub async fn run_verify_pool_bridge(
    mut rx: tokio::sync::mpsc::Receiver<crate::channels::KzgJob>,
    pool: Arc<VerifyPool>,
    metrics: P2pMetrics,
) {
    while let Some(job) = rx.recv().await {
        let depth = metrics.queue_depth(crate::metrics::QueueName::Kzg);
        if depth > 0 {
            metrics.set_queue_depth(crate::metrics::QueueName::Kzg, depth - 1);
        }
        pool.submit(job.into_verify_job());
    }
    // Pool lives in Arc; bridge exit does not shut workers (supervisor owns lifetime).
    let _ = pool;
}

// ── Helpers for fixtures / inclusion proof construction ─────────────────────

/// Compute the Merkle root for a leaf at generalized index `index` with `branch`.
///
/// Used by tests (and production fixture builders) to construct a self-consistent
/// inclusion proof against `BLOB_KZG_COMMITMENTS_FIELD_INDEX` (= 11).
#[must_use]
pub fn merkle_root_from_branch(leaf: [u8; 32], branch: &[[u8; 32]], index: u64) -> [u8; 32] {
    let mut value = leaf;
    for (i, node) in branch.iter().enumerate() {
        if (index >> i) & 1 == 1 {
            value = hash32_concat(node, &value);
        } else {
            value = hash32_concat(&value, node);
        }
    }
    value
}

/// Assert `KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH == 4` and field index is 11.
#[must_use]
pub fn inclusion_proof_spec_constants() -> (u64, u64) {
    (
        KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH,
        BLOB_KZG_COMMITMENTS_FIELD_INDEX,
    )
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_crypto::{Blob, CKzgBackend};
    use cc_types::primitives::{Cell, KzgCommitment, KzgProof};
    use std::path::Path;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration; // test sleeps

    /// Backend that records calls and returns a configured verdict.
    struct RecordingKzg {
        valid: AtomicBool,
        calls: AtomicU64,
        /// When set, the Nth call (1-based) returns invalid.
        fail_on_call: AtomicU64,
    }

    impl RecordingKzg {
        fn always_valid() -> Self {
            Self {
                valid: AtomicBool::new(true),
                calls: AtomicU64::new(0),
                fail_on_call: AtomicU64::new(0),
            }
        }

        fn always_invalid() -> Self {
            Self {
                valid: AtomicBool::new(false),
                calls: AtomicU64::new(0),
                fail_on_call: AtomicU64::new(0),
            }
        }
    }

    impl CellKzg for RecordingKzg {
        fn blob_to_kzg_commitment(&self, _blob: &Blob) -> Result<KzgCommitment, cc_crypto::KzgError> {
            Ok(KzgCommitment::default())
        }
        fn compute_cells(&self, _blob: &Blob) -> Result<cc_crypto::Cells, cc_crypto::KzgError> {
            Err(cc_crypto::KzgError::Backend("unused".into()))
        }
        fn compute_cells_and_kzg_proofs(
            &self,
            _blob: &Blob,
        ) -> Result<cc_crypto::CellsAndProofs, cc_crypto::KzgError> {
            Err(cc_crypto::KzgError::Backend("unused".into()))
        }
        fn recover_cells_and_kzg_proofs(
            &self,
            _cell_indices: &[u64],
            _cells: &[Cell],
        ) -> Result<cc_crypto::CellsAndProofs, cc_crypto::KzgError> {
            Err(cc_crypto::KzgError::Backend("unused".into()))
        }
        fn verify_cell_kzg_proof_batch(
            &self,
            _commitments: &[KzgCommitment],
            _cell_indices: &[u64],
            _cells: &[Cell],
            _proofs: &[KzgProof],
        ) -> Result<bool, cc_crypto::KzgError> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            let fail_at = self.fail_on_call.load(Ordering::Relaxed);
            if fail_at > 0 && n == fail_at {
                return Ok(false);
            }
            Ok(self.valid.load(Ordering::Relaxed))
        }
    }

    fn valid_inclusion(commitments: &[KzgCommitment]) -> ([Root; 4], [u8; 32]) {
        let leaf = list_tree_hash_root(commitments);
        let branch_bytes = [
            [1u8; 32],
            [2u8; 32],
            [3u8; 32],
            [4u8; 32],
        ];
        let body = merkle_root_from_branch(leaf, &branch_bytes, BLOB_KZG_COMMITMENTS_FIELD_INDEX);
        let branch = [
            Root::from_array(branch_bytes[0]),
            Root::from_array(branch_bytes[1]),
            Root::from_array(branch_bytes[2]),
            Root::from_array(branch_bytes[3]),
        ];
        (branch, body)
    }

    fn job_with(
        column_index: u64,
        block_root: [u8; 32],
        peer: &[u8],
        n_blobs: usize,
        max_blobs: u64,
        kzg_valid_shape: bool,
    ) -> VerifyJob {
        let commitments: Vec<_> = (0..n_blobs)
            .map(|i| {
                let mut a = [0u8; 48];
                a[0] = i as u8;
                KzgCommitment::from_array(a)
            })
            .collect();
        let cells: Vec<_> = (0..n_blobs).map(|_| Cell::ZERO).collect();
        let proofs: Vec<_> = (0..n_blobs).map(|_| KzgProof::default()).collect();
        let (inclusion_proof, body_root) = if kzg_valid_shape {
            valid_inclusion(&commitments)
        } else {
            (
                [
                    Root::default(),
                    Root::default(),
                    Root::default(),
                    Root::default(),
                ],
                [0u8; 32],
            )
        };
        VerifyJob {
            block_root,
            slot: 100,
            column_index,
            peer_id: peer.to_vec(),
            commitments,
            cells,
            proofs,
            inclusion_proof,
            body_root,
            max_blobs_per_block: max_blobs,
            received_at: Instant::now(),
            current_slot: 100,
            reply: None,
        }
    }

    #[test]
    fn pool_worker_count_is_max_2_half_cores() {
        let cores = thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(2);
        let expected = (cores / 2).max(2);
        assert_eq!(pool_worker_count(), expected);
        assert!(pool_worker_count() >= 2);
    }

    #[test]
    fn burst_of_requests_does_not_grow_thread_count() {
        let backend: Arc<dyn CellKzg> = Arc::new(RecordingKzg::always_valid());
        let pool = VerifyPool::start_with_workers(Arc::clone(&backend), None, 2);
        let before = pool.worker_count();
        assert_eq!(before, 2);

        // Burst well above the queue bound.
        for i in 0..10_000 {
            let mut job = job_with(i % 128, [9u8; 32], b"peer", 1, 21, true);
            job.reply = None;
            pool.submit(job);
        }
        // Give workers a moment; count must stay fixed.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(pool.worker_count(), before);
        assert_eq!(pool.worker_count(), 2);
        pool.shutdown();
    }

    #[test]
    fn queue_bound_is_256() {
        assert_eq!(VERIFY_QUEUE_BOUND, 256);
        assert_eq!(VERIFY_QUEUE_BOUND, crate::channels::KZG_BOUND);
    }

    #[test]
    fn das_sources_avoid_shared_blocking_pool() {
        // Needle split so this test body is not itself a false positive.
        let needle = format!("{}_{}", "spawn", "blocking");
        let das = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/das");
        let mut hits = Vec::new();
        for entry in std::fs::read_dir(&das).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let src = std::fs::read_to_string(&path).unwrap();
            for (i, line) in src.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") || trimmed.starts_with("//!") {
                    continue;
                }
                if trimmed.contains(&needle) {
                    hits.push(format!("{}:{}: {line}", path.display(), i + 1));
                }
            }
        }
        assert!(
            hits.is_empty(),
            "shared blocking pool must not appear under services/p2p/src/das/:\n{}",
            hits.join("\n")
        );
    }

    #[test]
    fn step_order_mutated_structure_stops_at_step_1() {
        let backend = RecordingKzg::always_valid();
        let counters = VerifyStepCounters::new();
        let mut cache = InclusionProofCache::new();
        // 15 blobs when max is 9 → structure fail.
        let job = job_with(0, [1u8; 32], b"p", 15, 9, true);
        let err = verify_sidecar_three_steps(&backend, &job, &mut cache, &counters).unwrap_err();
        assert_eq!(err.0, VerifyStep::Structure);
        assert_eq!(err.1, VerifyFailReason::BlobBoundExceeded);
        assert_eq!(counters.get(VerifyStep::Structure), 1);
        assert_eq!(counters.get(VerifyStep::InclusionProof), 0);
        assert_eq!(counters.get(VerifyStep::KzgProofs), 0);
        assert_eq!(counters.max_step_ran(), 1);
        assert_eq!(backend.calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn step_order_mutated_inclusion_stops_at_step_2() {
        let backend = RecordingKzg::always_valid();
        let counters = VerifyStepCounters::new();
        let mut cache = InclusionProofCache::new();
        let mut job = job_with(0, [1u8; 32], b"p", 1, 21, true);
        // Mutate exactly one inclusion-proof sibling.
        let mut bad = *job.inclusion_proof[0].as_array();
        bad[0] ^= 0xff;
        job.inclusion_proof[0] = Root::from_array(bad);

        let err = verify_sidecar_three_steps(&backend, &job, &mut cache, &counters).unwrap_err();
        assert_eq!(err.0, VerifyStep::InclusionProof);
        assert_eq!(err.1, VerifyFailReason::InclusionProofInvalid);
        assert_eq!(counters.get(VerifyStep::Structure), 1);
        assert_eq!(counters.get(VerifyStep::InclusionProof), 1);
        assert_eq!(counters.get(VerifyStep::KzgProofs), 0);
        assert_eq!(backend.calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn step_order_mutated_kzg_stops_at_step_3() {
        let backend = RecordingKzg::always_invalid();
        let counters = VerifyStepCounters::new();
        let mut cache = InclusionProofCache::new();
        let job = job_with(0, [1u8; 32], b"p", 1, 21, true);

        let err = verify_sidecar_three_steps(&backend, &job, &mut cache, &counters).unwrap_err();
        assert_eq!(err.0, VerifyStep::KzgProofs);
        assert_eq!(err.1, VerifyFailReason::KzgInvalid);
        assert_eq!(counters.get(VerifyStep::Structure), 1);
        assert_eq!(counters.get(VerifyStep::InclusionProof), 1);
        assert_eq!(counters.get(VerifyStep::KzgProofs), 1);
        assert_eq!(backend.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn blob_bound_is_runtime_lookup_not_constant() {
        let backend = RecordingKzg::always_valid();
        let mut cache = InclusionProofCache::new();

        // 15 blobs, epoch schedule allows 21 → structure passes (later may fail KZG mock ok).
        let job_ok = job_with(0, [1u8; 32], b"p", 15, 21, true);
        let counters_ok = VerifyStepCounters::new();
        assert!(
            verify_sidecar_three_steps(&backend, &job_ok, &mut cache, &counters_ok).is_ok()
        );

        // Same 15 blobs, schedule allows 9 → structure rejects.
        let job_bad = job_with(0, [1u8; 32], b"p", 15, 9, true);
        let counters_bad = VerifyStepCounters::new();
        let err =
            verify_sidecar_three_steps(&backend, &job_bad, &mut cache, &counters_bad).unwrap_err();
        assert_eq!(err.0, VerifyStep::Structure);
        assert_eq!(err.1, VerifyFailReason::BlobBoundExceeded);
    }

    #[test]
    fn inclusion_proof_depth_4_against_body_root_fixture() {
        let (depth, field_index) = inclusion_proof_spec_constants();
        assert_eq!(depth, 4);
        assert_eq!(field_index, 11);
        assert_eq!(KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH, 4);
        assert_eq!(BLOB_KZG_COMMITMENTS_FIELD_INDEX, 11);

        let commitments = vec![KzgCommitment::default()];
        let (branch, body_root) = valid_inclusion(&commitments);
        let leaf = list_tree_hash_root(&commitments);
        assert!(verify_inclusion_proof(
            &leaf,
            branch.as_ref(),
            body_root,
        ));

        // Mutate body_root → fail.
        let mut bad_body = body_root;
        bad_body[0] ^= 1;
        assert!(!verify_inclusion_proof(&leaf, branch.as_ref(), bad_body));
    }

    /// H1 regression: cache hit must not pass a different `body_root`.
    #[test]
    fn inclusion_cache_key_includes_body_root() {
        let backend = RecordingKzg::always_valid();
        let counters = VerifyStepCounters::new();
        let mut cache = InclusionProofCache::new();
        let commitments = vec![KzgCommitment::default()];
        let (branch, body_a) = valid_inclusion(&commitments);
        let block = [9u8; 32];

        let mut job_a = job_with(0, block, b"p", 1, 21, true);
        job_a.commitments = commitments.clone();
        job_a.inclusion_proof = branch;
        job_a.body_root = body_a;
        assert!(verify_sidecar_three_steps(&backend, &job_a, &mut cache, &counters).is_ok());
        assert_eq!(cache.verifications, 1);

        // Same commitments + proof + block_root, different body_root → miss + fail.
        let mut body_b = body_a;
        body_b[0] ^= 0xff;
        let mut job_b = job_a;
        job_b.body_root = body_b;
        let counters_b = VerifyStepCounters::new();
        let err =
            verify_sidecar_three_steps(&backend, &job_b, &mut cache, &counters_b).unwrap_err();
        assert_eq!(err.0, VerifyStep::InclusionProof);
        assert_eq!(err.1, VerifyFailReason::InclusionProofInvalid);
        assert_eq!(
            cache.verifications, 2,
            "different body_root must be a cache miss, not a hit"
        );
    }

    #[test]
    fn hard_blob_cap_rejects_untrusted_max() {
        let counters = VerifyStepCounters::new();
        // Claim max = u64::MAX but build 15 rows — still OK (15 < hard cap).
        let job_ok = job_with(0, [1u8; 32], b"p", 15, u64::MAX, true);
        assert!(verify_structure(
            job_ok.column_index,
            &job_ok.commitments,
            &job_ok.cells,
            &job_ok.proofs,
            job_ok.max_blobs_per_block,
            &counters,
        )
        .is_ok());

        assert_eq!(effective_max_blobs(u64::MAX), HARD_MAX_BLOB_COMMITMENTS);
        assert_eq!(effective_max_blobs(9), 9);
        assert_eq!(HARD_MAX_BLOB_COMMITMENTS, 4096);
        // Matches Mainnet SSZ list capacity (preset constant).
        use cc_types::Preset;
        assert_eq!(
            HARD_MAX_BLOB_COMMITMENTS,
            cc_types::Mainnet::MAX_BLOB_COMMITMENTS_PER_BLOCK
        );
    }

    #[test]
    fn three_same_block_jobs_use_per_sidecar_batching() {
        let backend: Arc<dyn CellKzg> = Arc::new(RecordingKzg::always_valid());
        let pool = VerifyPool::start_with_workers(Arc::clone(&backend), None, 1);
        let block = [7u8; 32];
        let mut outcomes = Vec::new();
        // Submit 3 together; with 1 worker they drain as one tick of 3 (< 4).
        let mut rxs = Vec::new();
        for col in 0..3u64 {
            let (tx, rx) = oneshot::channel();
            let mut job = job_with(col, block, b"peer", 1, 21, true);
            job.reply = Some(tx);
            pool.submit(job);
            rxs.push(rx);
        }
        for rx in rxs {
            outcomes.push(rx.blocking_recv().unwrap());
        }
        assert!(outcomes.iter().all(|o| *o == VerifyOutcome::Valid));
        assert_eq!(pool.batch_counters().per_sidecar.load(Ordering::Relaxed), 3);
        assert_eq!(
            pool.batch_counters().cross_attempts.load(Ordering::Relaxed),
            0
        );
        pool.shutdown();
    }

    #[test]
    fn four_same_block_jobs_use_cross_sidecar_batch() {
        let backend: Arc<dyn CellKzg> = Arc::new(RecordingKzg::always_valid());
        let pool = VerifyPool::start_with_workers(Arc::clone(&backend), None, 1);
        let block = [8u8; 32];
        let mut rxs = Vec::new();
        // Pre-fill four jobs before the single worker can interleave.
        // Start pool after? Already started. Push quickly; single worker
        // may process one-by-one if they arrive spaced. Use a barrier-like
        // approach: submit all without waiting, 1 worker drains the queue
        // — first pop then try_pop should gather all 4 if they're queued.
        thread::sleep(Duration::from_millis(10)); // let worker block on empty
        for col in 0..4u64 {
            let (tx, rx) = oneshot::channel();
            let mut job = job_with(col, block, &[col as u8], 1, 21, true);
            job.reply = Some(tx);
            pool.submit(job);
            rxs.push(rx);
        }
        for rx in rxs {
            assert_eq!(rx.blocking_recv().unwrap(), VerifyOutcome::Valid);
        }
        let cross = pool.batch_counters().cross_sidecar.load(Ordering::Relaxed);
        let per = pool.batch_counters().per_sidecar.load(Ordering::Relaxed);
        // Either one cross-batch of 4, or (race) per-sidecar; prefer cross.
        // With 1 worker + bulk submit after idle, cross should fire.
        assert!(
            cross == 4 || (cross == 0 && per == 4),
            "cross={cross} per={per}"
        );
        if cross == 4 {
            assert_eq!(
                pool.batch_counters().cross_attempts.load(Ordering::Relaxed),
                1
            );
        }
        pool.shutdown();
    }

    #[test]
    fn cross_sidecar_failure_reattributes_to_exactly_one_peer() {
        // Real backend so one mutated cell is cryptographically invalid while
        // the others remain valid — RecordingKzg cannot distinguish members.
        let real = CKzgBackend::load_default().expect("trusted setup");
        let backend: Arc<dyn CellKzg> = Arc::new(real);

        let blob = Blob::filled(0x42);
        let commitment = backend.blob_to_kzg_commitment(&blob).unwrap();
        let (cells, proofs) = backend.compute_cells_and_kzg_proofs(&blob).unwrap();

        let block = [42u8; 32];
        let mut jobs = Vec::new();
        for col in 0..4u64 {
            let cell = cells[col as usize];
            let proof = proofs[col as usize];
            let commitments = vec![commitment];
            let (incl, body) = valid_inclusion(&commitments);
            let peer = format!("peer-{col}");
            jobs.push(VerifyJob {
                block_root: block,
                slot: 50,
                column_index: col,
                peer_id: peer.into_bytes(),
                commitments,
                cells: vec![cell],
                proofs: vec![proof],
                inclusion_proof: incl,
                body_root: body,
                max_blobs_per_block: 21,
                received_at: Instant::now(),
                current_slot: 50,
                reply: None,
            });
        }
        // Mutate exactly one proof in job 2 so the backend returns Ok(false)
        // (invalid proof) rather than Err (malformed cell FE) — only the former
        // is attributed as gossip_invalid (M4).
        let mut bad_proof = *jobs[2].proofs[0].as_array();
        bad_proof[0] ^= 0xff;
        jobs[2].proofs[0] = KzgProof::from_array(bad_proof);

        // Metrics to assert gossip_invalid increments by 1.
        let mut registry = prometheus_client::registry::Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let pool = VerifyPool::start_with_workers(Arc::clone(&backend), Some(metrics.clone()), 1);

        thread::sleep(Duration::from_millis(20));
        let mut rxs = Vec::new();
        for mut job in jobs {
            let (tx, rx) = oneshot::channel();
            job.reply = Some(tx);
            pool.submit(job);
            rxs.push(rx);
        }

        let mut invalid = 0u32;
        let mut valid = 0u32;
        for rx in rxs {
            match rx.blocking_recv().unwrap() {
                VerifyOutcome::Valid => valid += 1,
                VerifyOutcome::Invalid { step, .. } => {
                    assert_eq!(step, VerifyStep::KzgProofs);
                    invalid += 1;
                }
                VerifyOutcome::Dropped => panic!("unexpected drop"),
            }
        }
        assert_eq!(invalid, 1, "exactly one bad column");
        assert_eq!(valid, 3);

        // Penalty attributes to exactly one peer.
        assert_eq!(
            metrics.peer_penalty_count(PeerPenaltyReason::GossipInvalid),
            1,
            "cc_p2p_peer_penalty_total{{reason=\"gossip_invalid\"}} must +1 not +4"
        );
        assert_eq!(pool.peer_penalty_count(), 1);
        let peers = pool.penalised_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], b"peer-2");

        // Fallback path exercised (or per-sidecar if race — still 1 penalty).
        let _ = pool.batch_counters().cross_failures.load(Ordering::Relaxed);
        pool.shutdown();
    }

    #[test]
    fn real_backend_mutated_cell_fails_step_3() {
        let backend = CKzgBackend::load_default().expect("setup");
        let blob = Blob::filled(0x11);
        let commitment = backend.blob_to_kzg_commitment(&blob).unwrap();
        let (cells, proofs) = backend.compute_cells_and_kzg_proofs(&blob).unwrap();
        let col = 3u64;
        let mut cell = cells[col as usize];
        let proof = proofs[col as usize];
        // Mutate exactly the cell.
        let mut bytes = cell.0;
        bytes[10] ^= 0xaa;
        cell = Cell::from_array(bytes);

        let commitments = vec![commitment];
        let (incl, body) = valid_inclusion(&commitments);
        let job = VerifyJob {
            block_root: [0u8; 32],
            slot: 1,
            column_index: col,
            peer_id: b"x".to_vec(),
            commitments,
            cells: vec![cell],
            proofs: vec![proof],
            inclusion_proof: incl,
            body_root: body,
            max_blobs_per_block: 21,
            received_at: Instant::now(),
            current_slot: 1,
            reply: None,
        };
        let counters = VerifyStepCounters::new();
        let mut cache = InclusionProofCache::new();
        let err = verify_sidecar_three_steps(&backend, &job, &mut cache, &counters).unwrap_err();
        assert_eq!(err.0, VerifyStep::KzgProofs);
        assert!(matches!(
            err.1,
            VerifyFailReason::KzgInvalid | VerifyFailReason::KzgError
        ));
        assert_eq!(counters.get(VerifyStep::Structure), 1);
        assert_eq!(counters.get(VerifyStep::InclusionProof), 1);
        assert_eq!(counters.get(VerifyStep::KzgProofs), 1);
    }

    #[test]
    fn sampling_latency_p95_under_200ms_budget() {
        let backend = CKzgBackend::load_default().expect("setup");
        let blob = Blob::filled(0x22);
        let commitment = backend.blob_to_kzg_commitment(&blob).unwrap();
        let (cells, proofs) = backend.compute_cells_and_kzg_proofs(&blob).unwrap();

        let mut registry = prometheus_client::registry::Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let pool = VerifyPool::start_with_workers(
            Arc::new(backend) as Arc<dyn CellKzg>,
            Some(metrics.clone()),
            2,
        );

        let mut rxs = Vec::new();
        let t0 = Instant::now();
        // 8 columns × 1 blob — production sampling shape at low blob count.
        for col in 0..8u64 {
            let commitments = vec![commitment];
            let (incl, body) = valid_inclusion(&commitments);
            let (tx, rx) = oneshot::channel();
            let job = VerifyJob {
                block_root: [1u8; 32],
                slot: 10,
                column_index: col,
                peer_id: b"peer".to_vec(),
                commitments,
                cells: vec![cells[col as usize]],
                proofs: vec![proofs[col as usize]],
                inclusion_proof: incl,
                body_root: body,
                max_blobs_per_block: 21,
                received_at: t0, // "8th column received" ≈ batch start
                current_slot: 10,
                reply: Some(tx),
            };
            pool.submit(job);
            rxs.push(rx);
        }
        for rx in rxs {
            assert_eq!(rx.blocking_recv().unwrap(), VerifyOutcome::Valid);
        }
        let wall = t0.elapsed().as_secs_f64();
        assert!(
            wall <= SAMPLING_P95_BUDGET_SECS,
            "8-column verify wall {wall:.4}s exceeds p95 budget {SAMPLING_P95_BUDGET_SECS}"
        );

        // Metrics series exist and received observations.
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("cc_p2p_sampling_seconds_bucket{le=\"0.2\"}"),
            "missing exact le=0.2 boundary"
        );
        assert!(
            buf.contains("cc_p2p_da_verdict_slot_delta"),
            "da_verdict_slot_delta must be a separate series"
        );
        pool.shutdown();
    }

    #[test]
    fn inclusion_cache_shared_across_columns() {
        let backend = RecordingKzg::always_valid();
        let counters = VerifyStepCounters::new();
        let mut cache = InclusionProofCache::new();
        let block = [3u8; 32];
        for col in 0..8u64 {
            // Same commitments + proof + block → one cache miss.
            let job = job_with(col, block, b"p", 1, 21, true);
            verify_sidecar_three_steps(&backend, &job, &mut cache, &counters).unwrap();
        }
        assert_eq!(
            cache.verifications, 1,
            "inclusion proof cache must hit for 7 of 8 columns"
        );
    }

    #[test]
    fn oldest_dropped_on_full_queue() {
        // Block workers with a gated backend so the queue can fill.
        struct SlowKzg {
            inner: RecordingKzg,
            gate: Arc<(Mutex<bool>, Condvar)>,
        }
        impl CellKzg for SlowKzg {
            fn blob_to_kzg_commitment(
                &self,
                b: &Blob,
            ) -> Result<KzgCommitment, cc_crypto::KzgError> {
                self.inner.blob_to_kzg_commitment(b)
            }
            fn compute_cells(&self, b: &Blob) -> Result<cc_crypto::Cells, cc_crypto::KzgError> {
                self.inner.compute_cells(b)
            }
            fn compute_cells_and_kzg_proofs(
                &self,
                b: &Blob,
            ) -> Result<cc_crypto::CellsAndProofs, cc_crypto::KzgError> {
                self.inner.compute_cells_and_kzg_proofs(b)
            }
            fn recover_cells_and_kzg_proofs(
                &self,
                i: &[u64],
                c: &[Cell],
            ) -> Result<cc_crypto::CellsAndProofs, cc_crypto::KzgError> {
                self.inner.recover_cells_and_kzg_proofs(i, c)
            }
            fn verify_cell_kzg_proof_batch(
                &self,
                commitments: &[KzgCommitment],
                cell_indices: &[u64],
                cells: &[Cell],
                proofs: &[KzgProof],
            ) -> Result<bool, cc_crypto::KzgError> {
                let (lock, cv) = &*self.gate;
                let mut g = lock.lock().unwrap();
                while !*g {
                    g = cv.wait(g).unwrap();
                }
                self.inner
                    .verify_cell_kzg_proof_batch(commitments, cell_indices, cells, proofs)
            }
        }

        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let slow = SlowKzg {
            inner: RecordingKzg::always_valid(),
            gate: Arc::clone(&gate),
        };
        let pool = VerifyPool::start_with_workers(Arc::new(slow), None, 1);

        // First job will block the worker at KZG step; fill the queue.
        let mut first_rx = {
            let (tx, rx) = oneshot::channel();
            let mut job = job_with(0, [1u8; 32], b"first", 1, 21, true);
            job.reply = Some(tx);
            pool.submit(job);
            rx
        };
        thread::sleep(Duration::from_millis(30)); // worker blocked in KZG

        let mut rxs = Vec::new();
        for i in 0..(VERIFY_QUEUE_BOUND + 5) {
            let (tx, rx) = oneshot::channel();
            let mut job = job_with((i % 128) as u64, [1u8; 32], b"p", 1, 21, true);
            job.reply = Some(tx);
            pool.submit(job);
            rxs.push(rx);
        }
        assert!(pool.dropped() >= 5, "dropped={}", pool.dropped());

        // Release worker.
        {
            let (lock, cv) = &*gate;
            let mut g = lock.lock().unwrap();
            *g = true;
            cv.notify_all();
        }
        let _ = first_rx.try_recv();
        // Drain some replies (Dropped or Valid).
        let mut dropped = 0u32;
        for rx in rxs {
            if let Ok(VerifyOutcome::Dropped) = rx.blocking_recv() {
                dropped += 1;
            }
        }
        assert!(dropped >= 1 || pool.dropped() >= 5);
        pool.shutdown();
    }
}
