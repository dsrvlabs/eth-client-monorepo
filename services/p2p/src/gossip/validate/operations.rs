//! Operation-topic validators — CC-2B / Architecture §5.4.
//!
//! Four topics, each with its full ordered condition list:
//! `voluntary_exit`, `proposer_slashing`, `attester_slashing`,
//! `bls_to_execution_change`.
//!
//! **p2p-authoritative** (ADR P2-04): REJECTed messages never reach `chain`.
//! A validated operation is scored/forwarded on gossip and **not stored**
//! (pools are Phase 5 — CC-2B/4). Signatures use `cc-crypto` only (no new
//! crypto). Validator records come from [`GetValidatorRecords`] via the
//! one-epoch-TTL LRU in [`crate::chain_stream::records`].

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cc_crypto::{
    DOMAIN_BEACON_ATTESTER, DOMAIN_BEACON_PROPOSER, DOMAIN_BLS_TO_EXECUTION_CHANGE,
    DOMAIN_VOLUNTARY_EXIT, PublicKey, Signature, SignatureSet, compute_domain,
    compute_signing_root, hash_fixed,
};
use cc_proto::p2p::{ChainView, Reason};
use cc_types::config::ChainConfig;
use cc_types::containers::{BeaconBlockHeader, Validator};
use cc_types::operations::{
    AttesterSlashing, ProposerSlashing, SignedBlsToExecutionChange, SignedVoluntaryExit,
};
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, Slot, ValidatorIndex};
use ssz::Decode;
use tree_hash::TreeHash;

use super::check_payload_len;
use crate::chain_stream::records::{RecordsError, ValidatorRecordCache, ValidatorRecordSource};
use crate::gossip::topics::TopicName;
use crate::verdict::Verdict;

/// Bound for each of the four operation index sets (§5.4).
pub const OPERATION_SEEN_BOUND: usize = 4_096;

/// Phase0 `BLS_WITHDRAWAL_PREFIX = 0x00`.
const BLS_WITHDRAWAL_PREFIX: u8 = 0x00;

/// `FAR_FUTURE_EPOCH = 2**64 - 1`.
const FAR_FUTURE_EPOCH: Epoch = Epoch::new(u64::MAX);

// ── Index sets (cleared at finalization) ────────────────────────────────────

/// Four bounded anti-replay index sets. **Cleared at finalization**, not
/// oldest-first — an exit that finalized can never re-arrive validly.
#[derive(Debug, Clone)]
pub struct OperationSeenSets {
    /// `voluntary_exit` validator indices.
    pub voluntary_exit: BoundedIndexSet,
    /// `proposer_slashing` proposer indices.
    pub proposer_slashing: BoundedIndexSet,
    /// `attester_slashing` attester indices (union of intersections).
    pub attester_slashing: BoundedIndexSet,
    /// `bls_to_execution_change` validator indices.
    pub bls_to_execution_change: BoundedIndexSet,
}

impl OperationSeenSets {
    /// Production bounds (4 096 each).
    #[must_use]
    pub fn new() -> Self {
        Self {
            voluntary_exit: BoundedIndexSet::new(OPERATION_SEEN_BOUND),
            proposer_slashing: BoundedIndexSet::new(OPERATION_SEEN_BOUND),
            attester_slashing: BoundedIndexSet::new(OPERATION_SEEN_BOUND),
            bls_to_execution_change: BoundedIndexSet::new(OPERATION_SEEN_BOUND),
        }
    }

    /// Occupancy snapshot for gauges.
    #[must_use]
    pub fn occupancy(&self) -> OperationOccupancy {
        OperationOccupancy {
            voluntary_exit: self.voluntary_exit.len(),
            proposer_slashing: self.proposer_slashing.len(),
            attester_slashing: self.attester_slashing.len(),
            bls_to_execution_change: self.bls_to_execution_change.len(),
        }
    }

    /// Clear **all four** sets at finalization (not a shrink — empty).
    pub fn clear_at_finalization(&mut self) {
        self.voluntary_exit.clear();
        self.proposer_slashing.clear();
        self.attester_slashing.clear();
        self.bls_to_execution_change.clear();
    }
}

impl Default for OperationSeenSets {
    fn default() -> Self {
        Self::new()
    }
}

/// Occupancy of the four operation index sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationOccupancy {
    /// `voluntary_exit_indices` size.
    pub voluntary_exit: usize,
    /// `proposer_slashing_indices` size.
    pub proposer_slashing: usize,
    /// `attester_slashing_indices` size.
    pub attester_slashing: usize,
    /// `bls_to_execution_change_indices` size.
    pub bls_to_execution_change: usize,
}

/// Bounded set of validator indices. Insert at capacity drops oldest.
///
/// Finalization clears the whole set via [`BoundedIndexSet::clear`].
#[derive(Debug, Clone)]
pub struct BoundedIndexSet {
    bound: usize,
    order: std::collections::VecDeque<u64>,
    set: HashSet<u64>,
}

impl BoundedIndexSet {
    /// Create with fixed bound (≥ 1).
    #[must_use]
    pub fn new(bound: usize) -> Self {
        Self {
            bound: bound.max(1),
            order: std::collections::VecDeque::new(),
            set: HashSet::new(),
        }
    }

    /// Configured capacity.
    #[must_use]
    pub const fn bound(&self) -> usize {
        self.bound
    }

    /// Current occupancy.
    #[must_use]
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Membership.
    #[must_use]
    pub fn contains(&self, index: u64) -> bool {
        self.set.contains(&index)
    }

    /// Insert. Returns `true` if newly inserted.
    pub fn insert(&mut self, index: u64) -> bool {
        if self.set.contains(&index) {
            return false;
        }
        while self.set.len() >= self.bound {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            } else {
                break;
            }
        }
        self.order.push_back(index);
        self.set.insert(index);
        true
    }

    /// Insert many indices.
    pub fn insert_all(&mut self, indices: impl IntoIterator<Item = u64>) {
        for i in indices {
            let _ = self.insert(i);
        }
    }

    /// Empty the set (finalization).
    pub fn clear(&mut self) {
        self.order.clear();
        self.set.clear();
    }
}

// ── Step counters (order property) ──────────────────────────────────────────

/// Shared step counter for a single validation run.
#[derive(Debug, Default)]
pub struct OpStepCounters {
    counts: [AtomicU64; 16],
}

impl OpStepCounters {
    /// Zeroed.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            counts: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    /// Tick a 1-based step number.
    pub fn tick(&self, step: u8) {
        let i = (step as usize).saturating_sub(1);
        if let Some(c) = self.counts.get(i) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Invocations of 1-based `step`.
    #[must_use]
    pub fn get(&self, step: u8) -> u64 {
        let i = (step as usize).saturating_sub(1);
        self.counts
            .get(i)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Highest step that ran (0 if none).
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

// Step numbers per topic (1-based, structural first).

/// `voluntary_exit` steps.
pub mod ve_step {
    /// Size check.
    pub const SIZE: u8 = 1;
    /// SSZ decode.
    pub const DECODE: u8 = 2;
    /// Seen-set membership.
    pub const SEEN: u8 = 3;
    /// Record fetch / index valid.
    pub const RECORD: u8 = 4;
    /// Active validator.
    pub const ACTIVE: u8 = 5;
    /// Not already exited.
    pub const NOT_EXITED: u8 = 6;
    /// Exit epoch not in future.
    pub const EPOCH: u8 = 7;
    /// Active long enough.
    pub const ACTIVE_LONG: u8 = 8;
    /// Signature.
    pub const SIG: u8 = 9;
    /// Accept + insert seen.
    pub const ACCEPT: u8 = 10;
}

/// `proposer_slashing` steps.
pub mod ps_step {
    /// Size.
    pub const SIZE: u8 = 1;
    /// Decode.
    pub const DECODE: u8 = 2;
    /// Seen.
    pub const SEEN: u8 = 3;
    /// Header slots match.
    pub const SLOTS: u8 = 4;
    /// Proposer indices match.
    pub const INDICES: u8 = 5;
    /// Headers differ.
    pub const DIFFER: u8 = 6;
    /// Record + slashable.
    pub const SLASHABLE: u8 = 7;
    /// Signatures.
    pub const SIG: u8 = 8;
    /// Accept.
    pub const ACCEPT: u8 = 9;
}

/// `attester_slashing` steps.
pub mod as_step {
    /// Size.
    pub const SIZE: u8 = 1;
    /// Decode.
    pub const DECODE: u8 = 2;
    /// Intersection has unseen index.
    pub const SEEN: u8 = 3;
    /// Slashable attestation data.
    pub const SLASHABLE_DATA: u8 = 4;
    /// Records for both attestations.
    pub const RECORDS: u8 = 5;
    /// Indexed attestation 1 valid.
    pub const ATT1: u8 = 6;
    /// Indexed attestation 2 valid.
    pub const ATT2: u8 = 7;
    /// Intersection has slashable validator.
    pub const SLASHABLE_VAL: u8 = 8;
    /// Accept.
    pub const ACCEPT: u8 = 9;
}

/// `bls_to_execution_change` steps.
pub mod btec_step {
    /// Size.
    pub const SIZE: u8 = 1;
    /// Decode.
    pub const DECODE: u8 = 2;
    /// Capella fork reached.
    pub const CAPELLA: u8 = 3;
    /// Seen.
    pub const SEEN: u8 = 4;
    /// Record.
    pub const RECORD: u8 = 5;
    /// BLS credentials prefix.
    pub const CREDS: u8 = 6;
    /// Pubkey hash match.
    pub const PUBKEY: u8 = 7;
    /// Signature.
    pub const SIG: u8 = 8;
    /// Accept.
    pub const ACCEPT: u8 = 9;
}

// ── Inputs / state ──────────────────────────────────────────────────────────

/// Shared mutable state for operation validators.
///
/// `seen` is behind a mutex so validators can hold only a shared reference and
/// still `await` record fetches without parking a `std::sync::MutexGuard`
/// across `.await` (the pool's outer lock is released first).
#[derive(Debug)]
pub struct OperationValidatorState {
    /// Four anti-replay index sets.
    pub seen: Mutex<OperationSeenSets>,
    /// Per-run step counters (tests assert order). Swappable under mutex.
    steps: Mutex<Arc<OpStepCounters>>,
}

impl OperationValidatorState {
    /// Fresh state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: Mutex::new(OperationSeenSets::new()),
            steps: Mutex::new(Arc::new(OpStepCounters::new())),
        }
    }

    /// Current step-counter Arc.
    #[must_use]
    pub fn steps(&self) -> Arc<OpStepCounters> {
        Arc::clone(
            &self
                .steps
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Install fresh step counters (tests).
    pub fn set_steps(&self, steps: Arc<OpStepCounters>) {
        *self
            .steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = steps;
    }

    /// Occupancy snapshot.
    #[must_use]
    pub fn occupancy(&self) -> OperationOccupancy {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .occupancy()
    }

    /// Clear all four sets at finalization.
    pub fn clear_at_finalization(&self) {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear_at_finalization();
    }
}

impl Default for OperationValidatorState {
    fn default() -> Self {
        Self::new()
    }
}

/// Inputs common to all operation topics.
#[derive(Debug)]
pub struct OperationValidateInput<'a> {
    /// Decompressed SSZ payload.
    pub payload: &'a [u8],
    /// Shared chain view (epoch, GVR, genesis time).
    pub view: &'a ChainView,
    /// Network config (fork versions).
    pub config: &'a ChainConfig,
    /// Slots per epoch.
    pub slots_per_epoch: u64,
    /// Current epoch (from view or clock).
    pub current_epoch: u64,
}

// ── Public entry points ─────────────────────────────────────────────────────

/// Validate `voluntary_exit` (ordered list).
pub async fn validate_voluntary_exit<P: Preset>(
    state: &OperationValidatorState,
    input: &OperationValidateInput<'_>,
    cache: &ValidatorRecordCache,
    source: &dyn ValidatorRecordSource,
) -> Verdict {
    let steps = state.steps();

    steps.tick(ve_step::SIZE);
    if check_payload_len::<P>(TopicName::VoluntaryExit, input.payload.len()).is_err() {
        return Verdict::reject(Reason::Invalid, vec![]);
    }

    steps.tick(ve_step::DECODE);
    let signed = match SignedVoluntaryExit::from_ssz_bytes(input.payload) {
        Ok(s) => s,
        Err(_) => return Verdict::reject(Reason::Invalid, vec![]),
    };
    let exit = &signed.message;
    let index = exit.validator_index.as_u64();
    let corr = correlation_root(&signed);

    steps.tick(ve_step::SEEN);
    {
        let seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if seen.voluntary_exit.contains(index) {
            return Verdict::ignore(Reason::Duplicate, corr);
        }
    }

    steps.tick(ve_step::RECORD);
    let records = match cache.get_many(&[index], input.current_epoch, source).await {
        Ok(m) => m,
        Err(RecordsError::Missing(_)) => {
            return Verdict::reject(Reason::Invalid, corr);
        }
        Err(RecordsError::Fetch(msg)) if msg.contains("out of range") => {
            return Verdict::reject(Reason::Invalid, corr);
        }
        Err(RecordsError::OverBound { .. }) | Err(RecordsError::Empty) => {
            return Verdict::internal(corr);
        }
        Err(_) => return Verdict::ignore(Reason::Internal, corr),
    };
    let Some(validator) = records.get(&index) else {
        return Verdict::reject(Reason::Invalid, corr);
    };

    let current_epoch = Epoch::new(input.current_epoch);

    steps.tick(ve_step::ACTIVE);
    if !is_active_validator(validator, current_epoch) {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ve_step::NOT_EXITED);
    if validator.exit_epoch != FAR_FUTURE_EPOCH {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ve_step::EPOCH);
    if current_epoch.as_u64() < exit.epoch.as_u64() {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ve_step::ACTIVE_LONG);
    let min_active = validator
        .activation_epoch
        .as_u64()
        .saturating_add(shard_committee_period::<P>());
    if current_epoch.as_u64() < min_active {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ve_step::SIG);
    if !verify_voluntary_exit_sig(input.config, input.view, validator, &signed) {
        return Verdict::reject(Reason::InvalidSignature, corr);
    }

    steps.tick(ve_step::ACCEPT);
    {
        let mut seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = seen.voluntary_exit.insert(index);
    }
    Verdict::accept(corr)
}

/// Validate `proposer_slashing`.
pub async fn validate_proposer_slashing<P: Preset>(
    state: &OperationValidatorState,
    input: &OperationValidateInput<'_>,
    cache: &ValidatorRecordCache,
    source: &dyn ValidatorRecordSource,
) -> Verdict {
    let steps = state.steps();

    steps.tick(ps_step::SIZE);
    if check_payload_len::<P>(TopicName::ProposerSlashing, input.payload.len()).is_err() {
        return Verdict::reject(Reason::Invalid, vec![]);
    }

    steps.tick(ps_step::DECODE);
    let slashing = match ProposerSlashing::from_ssz_bytes(input.payload) {
        Ok(s) => s,
        Err(_) => return Verdict::reject(Reason::Invalid, vec![]),
    };
    let header_1 = &slashing.signed_header_1.message;
    let header_2 = &slashing.signed_header_2.message;
    let proposer_index = header_1.proposer_index.as_u64();
    let corr = correlation_root(&slashing);

    steps.tick(ps_step::SEEN);
    {
        let seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if seen.proposer_slashing.contains(proposer_index) {
            return Verdict::ignore(Reason::Duplicate, corr);
        }
    }

    steps.tick(ps_step::SLOTS);
    if header_1.slot != header_2.slot {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ps_step::INDICES);
    if header_1.proposer_index != header_2.proposer_index {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ps_step::DIFFER);
    if headers_equal(header_1, header_2) {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ps_step::SLASHABLE);
    let records = match cache
        .get_many(&[proposer_index], input.current_epoch, source)
        .await
    {
        Ok(m) => m,
        Err(RecordsError::Missing(_)) => return Verdict::reject(Reason::Invalid, corr),
        Err(RecordsError::Fetch(msg)) if msg.contains("out of range") => {
            return Verdict::reject(Reason::Invalid, corr);
        }
        Err(_) => return Verdict::ignore(Reason::Internal, corr),
    };
    let Some(proposer) = records.get(&proposer_index) else {
        return Verdict::reject(Reason::Invalid, corr);
    };
    if !is_slashable_validator(proposer, Epoch::new(input.current_epoch)) {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(ps_step::SIG);
    if !verify_proposer_slashing_sigs::<P>(input.config, input.view, proposer, &slashing) {
        return Verdict::reject(Reason::InvalidSignature, corr);
    }

    steps.tick(ps_step::ACCEPT);
    {
        let mut seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = seen.proposer_slashing.insert(proposer_index);
    }
    Verdict::accept(corr)
}

/// Validate `attester_slashing`.
pub async fn validate_attester_slashing<P: Preset>(
    state: &OperationValidatorState,
    input: &OperationValidateInput<'_>,
    cache: &ValidatorRecordCache,
    source: &dyn ValidatorRecordSource,
) -> Verdict {
    let steps = state.steps();

    steps.tick(as_step::SIZE);
    if check_payload_len::<P>(TopicName::AttesterSlashing, input.payload.len()).is_err() {
        return Verdict::reject(Reason::Invalid, vec![]);
    }

    steps.tick(as_step::DECODE);
    let slashing = match AttesterSlashing::<P>::from_ssz_bytes(input.payload) {
        Ok(s) => s,
        Err(_) => return Verdict::reject(Reason::Invalid, vec![]),
    };
    let att1 = &slashing.attestation_1;
    let att2 = &slashing.attestation_2;
    let corr = correlation_root(&slashing);

    let set1: HashSet<u64> = att1.attesting_indices.iter().map(|i| i.as_u64()).collect();
    let set2: HashSet<u64> = att2.attesting_indices.iter().map(|i| i.as_u64()).collect();
    let intersection: Vec<u64> = set1.intersection(&set2).copied().collect();

    steps.tick(as_step::SEEN);
    {
        let seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let new_indices: Vec<u64> = intersection
            .iter()
            .copied()
            .filter(|i| !seen.attester_slashing.contains(*i))
            .collect();
        if new_indices.is_empty() {
            return Verdict::ignore(Reason::Duplicate, corr);
        }
    }

    steps.tick(as_step::SLASHABLE_DATA);
    if !is_slashable_attestation_data(&att1.data, &att2.data) {
        return Verdict::reject(Reason::Invalid, corr);
    }

    // Collect all indices for both attestations (bounded via cache batching).
    let mut all_indices: Vec<u64> = att1
        .attesting_indices
        .iter()
        .map(|i| i.as_u64())
        .chain(att2.attesting_indices.iter().map(|i| i.as_u64()))
        .collect();
    all_indices.sort_unstable();
    all_indices.dedup();

    steps.tick(as_step::RECORDS);
    let records = match cache
        .get_many(&all_indices, input.current_epoch, source)
        .await
    {
        Ok(m) => m,
        Err(RecordsError::Missing(_)) => return Verdict::reject(Reason::Invalid, corr),
        Err(RecordsError::Fetch(msg)) if msg.contains("out of range") => {
            return Verdict::reject(Reason::Invalid, corr);
        }
        Err(_) => return Verdict::ignore(Reason::Internal, corr),
    };
    // Every index must be present (out of range → reject).
    for &idx in &all_indices {
        if !records.contains_key(&idx) {
            return Verdict::reject(Reason::Invalid, corr);
        }
    }

    steps.tick(as_step::ATT1);
    if !is_valid_indexed_attestation(input.config, input.view, att1, &records) {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(as_step::ATT2);
    if !is_valid_indexed_attestation(input.config, input.view, att2, &records) {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(as_step::SLASHABLE_VAL);
    let epoch = Epoch::new(input.current_epoch);
    let mut slashable_any = false;
    for &idx in &intersection {
        if let Some(v) = records.get(&idx)
            && is_slashable_validator(v, epoch)
        {
            slashable_any = true;
            break;
        }
    }
    if !slashable_any {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(as_step::ACCEPT);
    {
        let mut seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        seen.attester_slashing.insert_all(intersection);
    }
    Verdict::accept(corr)
}

/// Validate `bls_to_execution_change`.
pub async fn validate_bls_to_execution_change<P: Preset>(
    state: &OperationValidatorState,
    input: &OperationValidateInput<'_>,
    cache: &ValidatorRecordCache,
    source: &dyn ValidatorRecordSource,
) -> Verdict {
    let steps = state.steps();

    steps.tick(btec_step::SIZE);
    if check_payload_len::<P>(TopicName::BlsToExecutionChange, input.payload.len()).is_err() {
        return Verdict::reject(Reason::Invalid, vec![]);
    }

    steps.tick(btec_step::DECODE);
    let signed = match SignedBlsToExecutionChange::from_ssz_bytes(input.payload) {
        Ok(s) => s,
        Err(_) => return Verdict::reject(Reason::Invalid, vec![]),
    };
    let change = &signed.message;
    let index = change.validator_index.as_u64();
    let corr = correlation_root(&signed);

    steps.tick(btec_step::CAPELLA);
    // Capella is long past on Fulu networks; still enforce the gossip condition.
    if input.current_epoch < input.config.capella_fork_epoch.as_u64() {
        return Verdict::ignore(Reason::AlreadyKnown, corr);
    }

    steps.tick(btec_step::SEEN);
    {
        let seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if seen.bls_to_execution_change.contains(index) {
            return Verdict::ignore(Reason::Duplicate, corr);
        }
    }

    steps.tick(btec_step::RECORD);
    let records = match cache.get_many(&[index], input.current_epoch, source).await {
        Ok(m) => m,
        Err(RecordsError::Missing(_)) => return Verdict::reject(Reason::Invalid, corr),
        Err(RecordsError::Fetch(msg)) if msg.contains("out of range") => {
            return Verdict::reject(Reason::Invalid, corr);
        }
        Err(_) => return Verdict::ignore(Reason::Internal, corr),
    };
    let Some(validator) = records.get(&index) else {
        return Verdict::reject(Reason::Invalid, corr);
    };

    let creds = validator.withdrawal_credentials.as_array();

    steps.tick(btec_step::CREDS);
    if creds[0] != BLS_WITHDRAWAL_PREFIX {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(btec_step::PUBKEY);
    let pubkey_hash = hash_fixed(change.from_bls_pubkey.as_slice());
    if creds[1..] != pubkey_hash[1..] {
        return Verdict::reject(Reason::Invalid, corr);
    }

    steps.tick(btec_step::SIG);
    if !verify_bls_to_execution_change_sig(input.config, input.view, &signed) {
        return Verdict::reject(Reason::InvalidSignature, corr);
    }

    steps.tick(btec_step::ACCEPT);
    {
        let mut seen = state
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = seen.bls_to_execution_change.insert(index);
    }
    Verdict::accept(corr)
}

/// Dispatch by topic name.
pub async fn validate_operation<P: Preset>(
    state: &OperationValidatorState,
    name: TopicName,
    input: &OperationValidateInput<'_>,
    cache: &ValidatorRecordCache,
    source: &dyn ValidatorRecordSource,
) -> Verdict {
    match name {
        TopicName::VoluntaryExit => validate_voluntary_exit::<P>(state, input, cache, source).await,
        TopicName::ProposerSlashing => {
            validate_proposer_slashing::<P>(state, input, cache, source).await
        }
        TopicName::AttesterSlashing => {
            validate_attester_slashing::<P>(state, input, cache, source).await
        }
        TopicName::BlsToExecutionChange => {
            validate_bls_to_execution_change::<P>(state, input, cache, source).await
        }
        _ => Verdict::internal(vec![]),
    }
}

// ── Predicates & signatures (local; no cc-state-transition dep) ─────────────

fn is_active_validator(validator: &Validator, epoch: Epoch) -> bool {
    validator.activation_epoch.as_u64() <= epoch.as_u64()
        && epoch.as_u64() < validator.exit_epoch.as_u64()
}

fn is_slashable_validator(validator: &Validator, epoch: Epoch) -> bool {
    !validator.slashed
        && validator.activation_epoch.as_u64() <= epoch.as_u64()
        && epoch.as_u64() < validator.withdrawable_epoch.as_u64()
}

fn is_slashable_attestation_data(
    data_1: &cc_types::containers::AttestationData,
    data_2: &cc_types::containers::AttestationData,
) -> bool {
    (data_1 != data_2 && data_1.target.epoch == data_2.target.epoch)
        || (data_1.source.epoch.as_u64() < data_2.source.epoch.as_u64()
            && data_2.target.epoch.as_u64() < data_1.target.epoch.as_u64())
}

fn shard_committee_period<P: Preset>() -> u64 {
    match P::NAME {
        "minimal" => 64,
        _ => 256,
    }
}

fn headers_equal(a: &BeaconBlockHeader, b: &BeaconBlockHeader) -> bool {
    a.slot == b.slot
        && a.proposer_index == b.proposer_index
        && a.parent_root == b.parent_root
        && a.state_root == b.state_root
        && a.body_root == b.body_root
}

fn gvr_from_view(view: &ChainView) -> Root {
    let mut arr = [0u8; 32];
    if view.genesis_validators_root.len() >= 32 {
        arr.copy_from_slice(&view.genesis_validators_root[..32]);
    }
    Root::from_array(arr)
}

fn decode_pubkey(pk: &cc_types::primitives::BlsPublicKey) -> Option<PublicKey> {
    PublicKey::deserialize(pk.as_array()).ok()
}

fn decode_sig(sig: &cc_types::primitives::BlsSignature) -> Option<Signature> {
    Signature::deserialize(sig.as_array()).ok()
}

fn verify_voluntary_exit_sig(
    config: &ChainConfig,
    view: &ChainView,
    validator: &Validator,
    signed: &SignedVoluntaryExit,
) -> bool {
    // Capella domain (permanent; matches process_voluntary_exit).
    let domain = compute_domain(
        DOMAIN_VOLUNTARY_EXIT,
        Some(config.capella_fork_version),
        Some(gvr_from_view(view)),
    );
    let message = *compute_signing_root(&signed.message, domain).as_array();
    let Some(pubkey) = decode_pubkey(&validator.pubkey) else {
        return false;
    };
    let Some(signature) = decode_sig(&signed.signature) else {
        return false;
    };
    // SignatureSet path (CC-2B/2 — cc-crypto only).
    let mut set = SignatureSet::new();
    set.push(pubkey, message, signature);
    set.verify()
}

fn verify_proposer_slashing_sigs<P: Preset>(
    config: &ChainConfig,
    view: &ChainView,
    proposer: &Validator,
    slashing: &ProposerSlashing,
) -> bool {
    let Some(pubkey) = decode_pubkey(&proposer.pubkey) else {
        return false;
    };
    let gvr = gvr_from_view(view);
    let mut set = SignatureSet::new();
    for signed_header in [&slashing.signed_header_1, &slashing.signed_header_2] {
        let epoch = signed_header
            .message
            .slot
            .epoch(P::SLOTS_PER_EPOCH)
            .as_u64();
        let domain = compute_domain(
            DOMAIN_BEACON_PROPOSER,
            Some(config.fork_version_at_epoch(Epoch::new(epoch))),
            Some(gvr),
        );
        let message = *compute_signing_root(&signed_header.message, domain).as_array();
        let Some(signature) = decode_sig(&signed_header.signature) else {
            return false;
        };
        set.push(pubkey, message, signature);
    }
    set.verify()
}

fn is_valid_indexed_attestation<P: Preset>(
    config: &ChainConfig,
    view: &ChainView,
    indexed: &cc_types::operations::IndexedAttestation<P>,
    records: &std::collections::HashMap<u64, Validator>,
) -> bool {
    let indices = &indexed.attesting_indices;
    if indices.is_empty() {
        return false;
    }
    for window in indices.windows(2) {
        if window[0].as_u64() >= window[1].as_u64() {
            return false;
        }
    }
    let mut pubkeys = Vec::with_capacity(indices.len());
    for idx in indices.iter() {
        let Some(v) = records.get(&idx.as_u64()) else {
            return false;
        };
        let Some(pk) = decode_pubkey(&v.pubkey) else {
            return false;
        };
        pubkeys.push(pk);
    }
    let epoch = indexed.data.target.epoch.as_u64();
    let domain = compute_domain(
        DOMAIN_BEACON_ATTESTER,
        Some(config.fork_version_at_epoch(Epoch::new(epoch))),
        Some(gvr_from_view(view)),
    );
    let message = *compute_signing_root(&indexed.data, domain).as_array();
    let Some(signature) = decode_sig(&indexed.signature) else {
        return false;
    };
    // Multi-pubkey aggregate via SignatureSet.
    let mut set = SignatureSet::new();
    set.push_aggregate(pubkeys, message, signature);
    set.verify()
}

fn verify_bls_to_execution_change_sig(
    config: &ChainConfig,
    view: &ChainView,
    signed: &SignedBlsToExecutionChange,
) -> bool {
    let domain = compute_domain(
        DOMAIN_BLS_TO_EXECUTION_CHANGE,
        Some(config.genesis_fork_version),
        Some(gvr_from_view(view)),
    );
    let message = *compute_signing_root(&signed.message, domain).as_array();
    let Some(pubkey) = decode_pubkey(&signed.message.from_bls_pubkey) else {
        return false;
    };
    let Some(signature) = decode_sig(&signed.signature) else {
        return false;
    };
    let mut set = SignatureSet::new();
    set.push(pubkey, message, signature);
    set.verify()
}

fn correlation_root<T: TreeHash>(obj: &T) -> Vec<u8> {
    obj.tree_hash_root().as_slice().to_vec()
}

/// Current epoch from view (prefer `view.epoch`, else slot // slots_per_epoch).
#[must_use]
pub fn epoch_from_view(view: &ChainView, slots_per_epoch: u64) -> u64 {
    if view.epoch > 0 {
        return view.epoch;
    }
    let spe = slots_per_epoch.max(1);
    if view.slot > 0 {
        return view.slot / spe;
    }
    if view.head_slot > 0 {
        return view.head_slot / spe;
    }
    0
}

// Silence unused import of Slot / ValidatorIndex in some builds.
const _: fn() = || {
    let _ = Slot::new(0);
    let _ = ValidatorIndex::new(0);
};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::chain_stream::records::MapValidatorRecordSource;
    use cc_crypto::SecretKey;
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::containers::{AttestationData, Checkpoint, SignedBeaconBlockHeader};
    use cc_types::operations::{BlsToExecutionChange, IndexedAttestation, VoluntaryExit};
    use cc_types::preset::Mainnet;
    use cc_types::primitives::{BlsPublicKey, BlsSignature, ForkVersion, Gwei};
    use ssz::Encode;

    fn test_config() -> ChainConfig {
        ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "test".into(),
            genesis_fork_version: ForkVersion::from_array([0, 0, 0, 1]),
            altair_fork_version: ForkVersion::from_array([1, 0, 0, 1]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([2, 0, 0, 1]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([3, 0, 0, 1]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([4, 0, 0, 1]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([5, 0, 0, 1]),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([6, 0, 0, 1]),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 12,
            blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 6,
            }])
            .unwrap(),
            deposit_chain_id: 1,
            deposit_contract_address: Default::default(),
            churn_limit_quotient: 65_536,
            min_per_epoch_churn_limit_electra: 128_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 256_000_000_000,
            shard_committee_period: Epoch::new(256),
            max_blobs_per_block_electra: 9,
        }
    }

    fn test_view(epoch: u64) -> ChainView {
        ChainView {
            slot: epoch * 32,
            epoch,
            genesis_time: 1_600_000_000,
            genesis_validators_root: vec![0xAB; 32],
            view_kind: 4,
            ..ChainView::default()
        }
    }

    fn sk(i: u64) -> SecretKey {
        let mut seed = [0u8; 32];
        seed[0] = 0x42;
        seed[1] = 0x99;
        SecretKey::from_seed_index(&seed, i).unwrap()
    }

    fn active_record(_i: u64, sk: &SecretKey) -> Validator {
        let pk = sk.public_key().serialize();
        Validator {
            pubkey: BlsPublicKey::from_array(pk),
            withdrawal_credentials: Root::from_array({
                let mut c = [0u8; 32];
                c[0] = 0x01;
                c
            }),
            effective_balance: Gwei::new(32_000_000_000),
            slashed: false,
            activation_eligibility_epoch: Epoch::new(0),
            activation_epoch: Epoch::new(0),
            exit_epoch: FAR_FUTURE_EPOCH,
            withdrawable_epoch: Epoch::new(u64::MAX),
        }
    }

    fn bls_cred_record(i: u64, from_sk: &SecretKey) -> Validator {
        let mut r = active_record(i, from_sk);
        let pk = from_sk.public_key().serialize();
        let h = hash_fixed(&pk);
        let mut creds = [0u8; 32];
        creds[0] = BLS_WITHDRAWAL_PREFIX;
        creds[1..].copy_from_slice(&h[1..]);
        r.withdrawal_credentials = Root::from_array(creds);
        // Pubkey on the validator can be different from from_bls_pubkey.
        r.pubkey = BlsPublicKey::from_array(sk(i + 1000).public_key().serialize());
        r
    }

    fn sign_exit(
        config: &ChainConfig,
        view: &ChainView,
        sk: &SecretKey,
        exit: VoluntaryExit,
    ) -> SignedVoluntaryExit {
        let domain = compute_domain(
            DOMAIN_VOLUNTARY_EXIT,
            Some(config.capella_fork_version),
            Some(gvr_from_view(view)),
        );
        let msg = *compute_signing_root(&exit, domain).as_array();
        let sig = sk.sign(&msg);
        SignedVoluntaryExit {
            message: exit,
            signature: BlsSignature::from_array(sig.serialize()),
        }
    }

    #[tokio::test]
    async fn voluntary_exit_happy_path_and_order() {
        let config = test_config();
        let view = test_view(300);
        let secret = sk(1);
        let record = active_record(1, &secret);
        let src = MapValidatorRecordSource::new();
        src.insert(1, record);
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();

        let signed = sign_exit(
            &config,
            &view,
            &secret,
            VoluntaryExit {
                epoch: Epoch::new(0),
                validator_index: ValidatorIndex::new(1),
            },
        );
        let payload = signed.as_ssz_bytes();
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 300,
        };
        let v = validate_voluntary_exit::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Accept));
        assert_eq!(state.steps().get(ve_step::ACCEPT), 1);
        assert_eq!(state.occupancy().voluntary_exit, 1);

        // Duplicate → IGNORE at SEEN; later steps must not re-run sig.
        state.set_steps(Arc::new(OpStepCounters::new()));
        let v2 = validate_voluntary_exit::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v2.acceptance, cc_proto::p2p::Acceptance::Ignore));
        assert_eq!(state.steps().get(ve_step::SEEN), 1);
        assert_eq!(state.steps().get(ve_step::SIG), 0);
        assert_eq!(state.steps().max_step_ran(), ve_step::SEEN);
    }

    #[tokio::test]
    async fn voluntary_exit_bad_sig_stops_before_accept() {
        let config = test_config();
        let view = test_view(300);
        let secret = sk(2);
        let record = active_record(2, &secret);
        let src = MapValidatorRecordSource::new();
        src.insert(2, record);
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();

        let mut signed = sign_exit(
            &config,
            &view,
            &secret,
            VoluntaryExit {
                epoch: Epoch::new(0),
                validator_index: ValidatorIndex::new(2),
            },
        );
        // Corrupt signature.
        signed.signature = BlsSignature::from_array([0x11; 96]);
        let payload = signed.as_ssz_bytes();
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 300,
        };
        let v = validate_voluntary_exit::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Reject));
        assert_eq!(state.steps().get(ve_step::SIG), 1);
        assert_eq!(state.steps().get(ve_step::ACCEPT), 0);
        assert_eq!(state.occupancy().voluntary_exit, 0);
    }

    #[tokio::test]
    async fn index_sets_cleared_at_finalization() {
        let state = OperationValidatorState::new();
        {
            let mut seen = state.seen.lock().unwrap();
            let _ = seen.voluntary_exit.insert(1);
            let _ = seen.proposer_slashing.insert(2);
            let _ = seen.attester_slashing.insert(3);
            let _ = seen.bls_to_execution_change.insert(4);
        }
        assert_eq!(state.occupancy().voluntary_exit, 1);
        state.clear_at_finalization();
        assert_eq!(state.occupancy().voluntary_exit, 0);
        assert_eq!(state.occupancy().proposer_slashing, 0);
        assert_eq!(state.occupancy().attester_slashing, 0);
        assert_eq!(state.occupancy().bls_to_execution_change, 0);
    }

    #[tokio::test]
    async fn index_sets_bounded_4096() {
        let mut set = BoundedIndexSet::new(OPERATION_SEEN_BOUND);
        for i in 0..(OPERATION_SEEN_BOUND as u64 + 10) {
            let _ = set.insert(i);
        }
        assert_eq!(set.len(), OPERATION_SEEN_BOUND);
        assert!(!set.contains(0));
        assert!(set.contains(OPERATION_SEEN_BOUND as u64 + 9));
    }

    #[tokio::test]
    async fn flood_does_not_become_query_flood() {
        let config = test_config();
        let view = test_view(300);
        let cache = ValidatorRecordCache::new();
        let src = MapValidatorRecordSource::new();
        // 200 distinct proposers, each with a real key + signed headers.
        let n_validators = 200u64;
        for i in 0..n_validators {
            let secret = sk(i);
            src.insert(i, active_record(i, &secret));
        }

        let state = OperationValidatorState::new();
        // 10_000 slashings cycling through 200 indices.
        for n in 0..10_000u64 {
            let i = n % n_validators;
            let secret = sk(i);
            let header = |body: u8| {
                let msg = BeaconBlockHeader {
                    slot: Slot::new(100),
                    proposer_index: ValidatorIndex::new(i),
                    parent_root: Root::from_array([body; 32]),
                    state_root: Root::from_array([0x22; 32]),
                    body_root: Root::from_array([0x33; 32]),
                };
                let domain = compute_domain(
                    DOMAIN_BEACON_PROPOSER,
                    Some(config.fork_version_at_epoch(msg.slot.epoch(32))),
                    Some(gvr_from_view(&view)),
                );
                let root = *compute_signing_root(&msg, domain).as_array();
                SignedBeaconBlockHeader {
                    message: msg,
                    signature: BlsSignature::from_array(secret.sign(&root).serialize()),
                }
            };
            let slashing = ProposerSlashing {
                signed_header_1: header(0xAA),
                signed_header_2: header(0xBB),
            };
            // Only first acceptance per index inserts seen; rest IGNORE at SEEN.
            // Still exercise the record path on first of each.
            let payload = slashing.as_ssz_bytes();
            let input = OperationValidateInput {
                payload: &payload,
                view: &view,
                config: &config,
                slots_per_epoch: 32,
                current_epoch: 300,
            };
            let _ = validate_proposer_slashing::<Mainnet>(&state, &input, &cache, &src).await;
        }
        // At most one RPC batch per distinct index (plus no extra after cache warm).
        assert!(
            cache.fetch_call_count() <= n_validators,
            "fetch calls {} must be ≤ distinct validators {n_validators}",
            cache.fetch_call_count()
        );
        // Source call count tracks batches; also bounded.
        assert!(src.call_count() <= n_validators);
    }

    #[tokio::test]
    async fn rejected_does_not_touch_chain_channel_and_leaves_no_pool() {
        // CC-2B/3 + CC-2B/4: local REJECT; no GossipObject; no retained pool state
        // beyond the anti-replay set (which stays empty on REJECT).
        let config = test_config();
        let view = test_view(300);
        let secret = sk(7);
        let src = MapValidatorRecordSource::new();
        src.insert(7, active_record(7, &secret));
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();

        // Future exit epoch → REJECT at EPOCH step.
        let signed = sign_exit(
            &config,
            &view,
            &secret,
            VoluntaryExit {
                epoch: Epoch::new(9999),
                validator_index: ValidatorIndex::new(7),
            },
        );
        let payload = signed.as_ssz_bytes();
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 300,
        };
        let v = validate_voluntary_exit::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Reject));
        assert_eq!(
            state.occupancy().voluntary_exit,
            0,
            "REJECT must not insert seen"
        );
        // No chain outbound in this unit path — pipeline never sends for REJECT.
    }

    #[tokio::test]
    async fn accept_leaves_only_anti_replay_not_a_pool() {
        // CC-2B/4: after ACCEPT only the index set is non-empty — no queue, no pool.
        let config = test_config();
        let view = test_view(300);
        let secret = sk(8);
        let src = MapValidatorRecordSource::new();
        src.insert(8, active_record(8, &secret));
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();
        let signed = sign_exit(
            &config,
            &view,
            &secret,
            VoluntaryExit {
                epoch: Epoch::new(0),
                validator_index: ValidatorIndex::new(8),
            },
        );
        let payload = signed.as_ssz_bytes();
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 300,
        };
        let v = validate_voluntary_exit::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Accept));
        assert_eq!(state.occupancy().voluntary_exit, 1);
        assert_eq!(state.occupancy().proposer_slashing, 0);
        assert_eq!(state.occupancy().attester_slashing, 0);
        assert_eq!(state.occupancy().bls_to_execution_change, 0);
        // Finalization empties completely.
        state.clear_at_finalization();
        assert_eq!(state.occupancy().voluntary_exit, 0);
    }

    #[tokio::test]
    async fn bls_to_execution_change_happy_path() {
        let config = test_config();
        let view = test_view(10);
        let from_sk = sk(20);
        let record = bls_cred_record(20, &from_sk);
        let src = MapValidatorRecordSource::new();
        src.insert(20, record);
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();

        let change = BlsToExecutionChange {
            validator_index: ValidatorIndex::new(20),
            from_bls_pubkey: BlsPublicKey::from_array(from_sk.public_key().serialize()),
            to_execution_address: Default::default(),
        };
        let domain = compute_domain(
            DOMAIN_BLS_TO_EXECUTION_CHANGE,
            Some(config.genesis_fork_version),
            Some(gvr_from_view(&view)),
        );
        let msg = *compute_signing_root(&change, domain).as_array();
        let signed = SignedBlsToExecutionChange {
            message: change,
            signature: BlsSignature::from_array(from_sk.sign(&msg).serialize()),
        };
        let payload = signed.as_ssz_bytes();
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 10,
        };
        let v = validate_bls_to_execution_change::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Accept));
    }

    #[tokio::test]
    async fn oversize_rejects_at_size_step() {
        let config = test_config();
        let view = test_view(1);
        let src = MapValidatorRecordSource::new();
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();
        let max = crate::gossip::validate::max_container_bytes::<Mainnet>(TopicName::VoluntaryExit);
        let payload = vec![0u8; max + 1];
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 1,
        };
        let v = validate_voluntary_exit::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Reject));
        assert_eq!(state.steps().get(ve_step::SIZE), 1);
        assert_eq!(state.steps().get(ve_step::DECODE), 0);
    }

    #[tokio::test]
    async fn proposer_slashing_identical_headers_reject_before_sig() {
        let config = test_config();
        let view = test_view(10);
        let secret = sk(3);
        let src = MapValidatorRecordSource::new();
        src.insert(3, active_record(3, &secret));
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();
        let msg = BeaconBlockHeader {
            slot: Slot::new(5),
            proposer_index: ValidatorIndex::new(3),
            parent_root: Root::from_array([1; 32]),
            state_root: Root::from_array([2; 32]),
            body_root: Root::from_array([3; 32]),
        };
        let sh = SignedBeaconBlockHeader {
            message: msg,
            signature: BlsSignature::default(),
        };
        let slashing = ProposerSlashing {
            signed_header_1: sh,
            signed_header_2: sh,
        };
        let payload = slashing.as_ssz_bytes();
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 10,
        };
        let v = validate_proposer_slashing::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Reject));
        assert_eq!(state.steps().get(ps_step::DIFFER), 1);
        assert_eq!(state.steps().get(ps_step::SIG), 0);
    }

    // Keep IndexedAttestation / AttestationData imports used in attester path live
    // for type-check; full attester happy path is heavy — exercise structure reject.
    #[tokio::test]
    async fn attester_slashing_non_slashable_data_rejects() {
        let config = test_config();
        let view = test_view(10);
        let src = MapValidatorRecordSource::new();
        let cache = ValidatorRecordCache::new();
        let state = OperationValidatorState::new();

        let data = AttestationData {
            slot: Slot::new(1),
            index: Default::default(),
            beacon_block_root: Root::from_array([9; 32]),
            source: Checkpoint {
                epoch: Epoch::new(0),
                root: Root::from_array([1; 32]),
            },
            target: Checkpoint {
                epoch: Epoch::new(1),
                root: Root::from_array([2; 32]),
            },
        };
        // Identical data → not slashable.
        let att = IndexedAttestation::<Mainnet> {
            attesting_indices: ssz_types::VariableList::new(vec![ValidatorIndex::new(0)]).unwrap(),
            data,
            signature: BlsSignature::default(),
        };
        let slashing = AttesterSlashing {
            attestation_1: att.clone(),
            attestation_2: att,
        };
        let payload = slashing.as_ssz_bytes();
        let input = OperationValidateInput {
            payload: &payload,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            current_epoch: 10,
        };
        let v = validate_attester_slashing::<Mainnet>(&state, &input, &cache, &src).await;
        assert!(matches!(v.acceptance, cc_proto::p2p::Acceptance::Reject));
        assert_eq!(state.steps().get(as_step::SLASHABLE_DATA), 1);
        assert_eq!(state.steps().get(as_step::ATT1), 0);
    }
}
