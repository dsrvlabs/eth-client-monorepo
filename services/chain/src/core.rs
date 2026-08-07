//! Dedicated OS core thread owning fork-choice [`Store`] by value (ADR-P1-09).
//!
//! Communication: `tokio::sync::mpsc` (capacity 64) with `blocking_recv` on the
//! thread side and `oneshot` replies. `ImportBlock` uses `send_timeout(2 s)` →
//! `RESOURCE_EXHAUSTED` on backpressure.
//!
//! ```text
//! loop { recv(); handle(); /* snapshot + events inside import */ }
//! ```
//!
//! CC-1F state-requiring reads (`GetCommitteeShuffling`, `GetValidatorPubkeys`)
//! and CC-27a `GetValidatorRecords` go through the single FIFO
//! [`CoreCommand::Query`] path (§7.1) — no second copy of the head state is held
//! on the gRPC side. Epoch-scoped data for `ChainView` is published via
//! [`EpochContextStore`] (second `ArcSwap`, Architecture §16/4).

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use cc_fork_choice::{
    PeerDasAvailability, Store, is_optimistic, is_optimistic_node, on_tick,
};
use cc_proto::chain::{
    ApplyAttestationsRequest, ApplyAttestationsResponse, ImportBlockRequest, ImportBlockResponse,
};
use cc_proto::common::Source;
use cc_state_transition::helpers::accessors::get_active_validator_indices;
use cc_state_transition::{
    BlockSignatureStrategy, compute_shuffled_active_indices, decision_root_for_epoch,
    get_committee_count_per_slot, get_current_epoch, get_or_compute_shuffling,
};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root};
use ssz::Encode;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use crate::apply_attestations::apply_attestations;
use crate::da::{DEFAULT_DA_PENDING_TIMEOUT_SLOTS, PendingDa};
use crate::engine_client::poll_engine_online;
use crate::epoch_context::{EpochContext, EpochContextStore};
use crate::fcu_driver::{FcuDriver, GrpcFcuSink};
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::import::{ImportCounters, ImportOutcome, import_block_with_early};
use crate::metrics::ChainMetrics;
use crate::pending_engine::{DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS, PendingEngine};
use crate::residency::{DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency};

/// Command channel capacity (Architecture §7.2).
pub const COMMAND_CHANNEL_CAPACITY: usize = 64;

/// `ImportBlock` send timeout before `RESOURCE_EXHAUSTED` (Architecture §7.2).
pub const IMPORT_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Shutdown join timeout (Architecture §7.4).
pub const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Max indices accepted by `GetValidatorPubkeys` (CC-1F; same discipline as
/// Phase 2's 256-bound `GetValidatorRecords`).
pub const MAX_VALIDATOR_PUBKEYS_PER_REQUEST: u64 = 256;

/// Max indices accepted by `GetValidatorRecords` (CC-27a / §5.4a).
pub const MAX_VALIDATOR_RECORDS_PER_REQUEST: u64 = 256;

/// Commands handled by the core thread.
#[derive(Debug)]
pub enum CoreCommand {
    /// Full import path.
    ImportBlock {
        request: ImportBlockRequest,
        reply: oneshot::Sender<Result<ImportBlockResponse, Status>>,
    },
    /// Gossip-path import: early ACCEPT notify before state transition (CC-27c).
    ImportBlockGossip {
        request: ImportBlockRequest,
        /// Fired once after cheap gossip checks pass, before `on_block`.
        early_accept: Option<oneshot::Sender<()>>,
        reply: oneshot::Sender<Result<ImportOutcome, Status>>,
    },
    /// Batched free-floating `on_attestation` (CC-1E). No per-item head recompute.
    ApplyAttestations {
        request: ApplyAttestationsRequest,
        reply: oneshot::Sender<Result<ApplyAttestationsResponse, Status>>,
    },
    /// State-requiring read (single FIFO queue in Phase 1; priority lane is Phase 6).
    ///
    /// Used by CC-1F (`GetCommitteeShuffling`, `GetValidatorPubkeys`) and head probes.
    Query {
        request: QueryRequest,
        reply: oneshot::Sender<Result<QueryReply, Status>>,
    },
    /// Block the core thread for `duration` (tests: GetHead bypass).
    BlockFor {
        duration: Duration,
        reply: oneshot::Sender<()>,
    },
    /// Sampling complete for `root` (CC-24d / Architecture §8.3).
    ///
    /// Marks [`PeerDasAvailability`] and re-drives a parked `pending_da` entry
    /// when present. Order-independent with block arrival.
    DataAvailable { root: Root, slot: u64 },
    /// Per-slot fcU floor tick (CC-33 /7) — re-points a restarted EL with no block.
    SlotTick,
    /// Graceful shutdown.
    Shutdown { done: oneshot::Sender<()> },
}

/// Request variants for [`CoreCommand::Query`].
#[derive(Debug, Clone)]
pub enum QueryRequest {
    /// Head root / slot from the store (diagnostic / tests).
    Head,
    /// Packed committee shuffling for `epoch` from the head state (CC-1F).
    CommitteeShuffling { epoch: u64 },
    /// Validator pubkeys by resolved index list (CC-1F). Indices already bound-checked.
    ValidatorPubkeys { indices: Vec<u64> },
    /// SSZ `Validator` records by index list (CC-27a). Indices already bound-checked.
    ValidatorRecords { indices: Vec<u64> },
    /// CC-3B: optimistic status from fork choice (proto-array only — never engine).
    ///
    /// `root: None` → node-level [`is_optimistic_node`] (CC-34c both branches).
    /// `root: Some` → per-root [`is_optimistic`]; `known=false` when absent.
    IsOptimistic { root: Option<Root> },
}

/// Reply for the Phase-1 / CC-27a / CC-3B `Query` command.
#[derive(Debug, Clone)]
pub enum QueryReply {
    /// Head probe.
    Head { head_root: Root, head_slot: u64 },
    /// Served shuffling for one epoch.
    CommitteeShuffling {
        shuffled_indices: Vec<u64>,
        dependent_root: Root,
        epoch: u64,
        committees_per_slot: u64,
    },
    /// Served pubkeys (parallel to the requested indices).
    ValidatorPubkeys {
        indices: Vec<u64>,
        pubkeys: Vec<Vec<u8>>,
    },
    /// Served SSZ validator records + the head slot they were read at.
    ValidatorRecords { ssz: Vec<Vec<u8>>, slot: u64 },
    /// CC-3B: tri-state optimistic answer (`known=false` ⇒ ignore `is_optimistic`).
    IsOptimistic { is_optimistic: bool, known: bool },
}

/// Configuration for spawning the core thread.
#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub max_resident_states: usize,
    pub body_ring_capacity: usize,
    pub verify: BlockSignatureStrategy,
    /// Shared PeerDAS available-root set (same `Arc` as the store's DA).
    ///
    /// `None` when the store was seeded with a non-PeerDAS harness DA (tests).
    pub peer_das: Option<Arc<PeerDasAvailability>>,
    /// Slots a deferred block may wait for `DataAvailable` (default 4).
    pub da_pending_timeout_slots: u64,
    /// Slots a deferred block may wait for the execution engine (default 8).
    pub engine_pending_timeout_slots: u64,
    /// gRPC URI for `EngineService` (CC-32b). Not a health peer (ADR P3-02).
    pub engine_uri: String,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            max_resident_states: DEFAULT_MAX_RESIDENT_STATES,
            body_ring_capacity: DEFAULT_BODY_RING_CAPACITY,
            verify: BlockSignatureStrategy::NoVerification,
            peer_das: None,
            da_pending_timeout_slots: DEFAULT_DA_PENDING_TIMEOUT_SLOTS,
            engine_pending_timeout_slots: DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS,
            engine_uri: crate::engine_client::DEFAULT_ENGINE_URI.to_owned(),
        }
    }
}

/// Handle to a running core thread (non-generic: channel + shared state only).
#[derive(Debug, Clone)]
pub struct CoreHandle {
    cmd_tx: mpsc::Sender<CoreCommand>,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    metrics: ChainMetrics,
    counters: Arc<ImportCounters>,
}

impl CoreHandle {
    /// Shared head snapshot (also used by `GetHead`).
    pub fn head(&self) -> &HeadSnapshotStore {
        &self.head
    }

    /// Shared epoch context (used by `ChainView` producer; §16/4).
    pub fn epoch_context(&self) -> &EpochContextStore {
        &self.epoch
    }

    /// Metrics handle.
    pub fn metrics(&self) -> &ChainMetrics {
        &self.metrics
    }

    /// Transition-invocation counter (DUPLICATE short-circuit tests).
    pub fn transition_count(&self) -> u64 {
        self.counters.transition_count()
    }

    /// Clone of the command sender (tests that fill the channel).
    pub fn command_sender(&self) -> mpsc::Sender<CoreCommand> {
        self.cmd_tx.clone()
    }

    /// Import a block via the core channel with the 2 s send timeout.
    pub async fn import_block(
        &self,
        request: ImportBlockRequest,
    ) -> Result<ImportBlockResponse, Status> {
        let (reply, rx) = oneshot::channel();
        let cmd = CoreCommand::ImportBlock { request, reply };
        match self.cmd_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
            Ok(()) => {}
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                self.metrics.inc_import_rejected_backpressure();
                return Err(Status::resource_exhausted(
                    "import command channel full after 2s send_timeout",
                ));
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
        }
        // Queue depth gauge: approximate via capacity residual.
        self.metrics.set_import_queue_depth(
            (COMMAND_CHANNEL_CAPACITY.saturating_sub(self.cmd_tx.capacity())) as u64,
        );
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped import reply"))?
    }

    /// Gossip-path import (CC-27c): optional early-ACCEPT oneshot before transition.
    ///
    /// When cheap gossip conditions pass, `early_accept` is completed **before**
    /// `on_block` runs so the stream can emit a `Verdict` under the 100 ms budget.
    /// The returned [`ImportOutcome`] carries late-reject / late-internal flags
    /// for the post-transition application-score path (no second gossip report).
    pub async fn import_block_for_gossip(
        &self,
        request: ImportBlockRequest,
        early_accept: Option<oneshot::Sender<()>>,
    ) -> Result<ImportOutcome, Status> {
        let (reply, rx) = oneshot::channel();
        let cmd = CoreCommand::ImportBlockGossip {
            request,
            early_accept,
            reply,
        };
        match self.cmd_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
            Ok(()) => {}
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                self.metrics.inc_import_rejected_backpressure();
                return Err(Status::resource_exhausted(
                    "import command channel full after 2s send_timeout",
                ));
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
        }
        self.metrics.set_import_queue_depth(
            (COMMAND_CHANNEL_CAPACITY.saturating_sub(self.cmd_tx.capacity())) as u64,
        );
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped gossip import reply"))?
    }

    /// Apply a batch of free-floating attestations (CC-1E).
    ///
    /// Uses the same 2 s send timeout as [`Self::import_block`] so a stalled
    /// core does not hang gRPC workers indefinitely.
    pub async fn apply_attestations(
        &self,
        request: ApplyAttestationsRequest,
    ) -> Result<ApplyAttestationsResponse, Status> {
        let (reply, rx) = oneshot::channel();
        let cmd = CoreCommand::ApplyAttestations { request, reply };
        match self.cmd_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
            Ok(()) => {}
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                return Err(Status::resource_exhausted(
                    "apply_attestations command channel full after 2s send_timeout",
                ));
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
        }
        self.metrics.set_import_queue_depth(
            (COMMAND_CHANNEL_CAPACITY.saturating_sub(self.cmd_tx.capacity())) as u64,
        );
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped apply_attestations reply"))?
    }

    /// `Query` command (single FIFO queue in Phase 1).
    ///
    /// Uses the same 2 s send timeout as [`Self::import_block`] so a stalled
    /// core does not hang gRPC workers on state reads (CC-1F / CC-27a).
    pub async fn query(&self, request: QueryRequest) -> Result<QueryReply, Status> {
        let (reply, rx) = oneshot::channel();
        let cmd = CoreCommand::Query { request, reply };
        match self.cmd_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
            Ok(()) => {}
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                return Err(Status::resource_exhausted(
                    "query command channel full after 2s send_timeout",
                ));
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
        }
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped query reply"))?
    }

    /// Block the core thread (tests).
    pub async fn block_for(&self, duration: Duration) -> Result<(), Status> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(CoreCommand::BlockFor { duration, reply })
            .await
            .map_err(|_| Status::unavailable("chain core thread is shut down"))?;
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped block_for reply"))?;
        Ok(())
    }

    /// Notify the core that sampling completed for `root` (CC-24d).
    ///
    /// Fire-and-forget on the command channel (no reply). Uses the same 2 s
    /// send timeout as import so a stalled core surfaces as unavailable.
    pub async fn notify_data_available(&self, root: Root, slot: u64) -> Result<(), Status> {
        let cmd = CoreCommand::DataAvailable { root, slot };
        match self.cmd_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
            Ok(()) => Ok(()),
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => Err(Status::resource_exhausted(
                "data_available command channel full after 2s send_timeout",
            )),
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                Err(Status::unavailable("chain core thread is shut down"))
            }
        }
    }

    /// Enqueue [`CoreCommand::Shutdown`] and return the done receiver (no wait).
    ///
    /// Prefer [`CoreThread::shutdown_and_join`] for production teardown so the
    /// oneshot wait and OS join share a **single** [`SHUTDOWN_JOIN_TIMEOUT`].
    pub async fn begin_shutdown(&self) -> Option<oneshot::Receiver<()>> {
        let (done, rx) = oneshot::channel();
        match self.cmd_tx.send(CoreCommand::Shutdown { done }).await {
            Ok(()) => Some(rx),
            Err(_) => None,
        }
    }

    /// Ask the core thread to exit and wait up to [`SHUTDOWN_JOIN_TIMEOUT`] for
    /// the done oneshot only (does **not** join the OS thread).
    ///
    /// Production pre-drain uses [`CoreThread::shutdown_and_join`] so Shutdown
    /// and OS join share one 2 s envelope under the 5 s SIGTERM process budget.
    /// This method remains for tests that join separately.
    pub async fn shutdown(&self) {
        if let Some(rx) = self.begin_shutdown().await {
            let _ = tokio::time::timeout(SHUTDOWN_JOIN_TIMEOUT, rx).await;
        }
    }
}

/// Spawn result: handle + optional join for process shutdown.
#[derive(Debug)]
pub struct CoreThread {
    pub handle: CoreHandle,
    join: Option<JoinHandle<()>>,
}

impl CoreThread {
    /// Join the OS thread (after [`CoreHandle::shutdown`] / [`Self::shutdown_and_join`]).
    pub fn join(mut self) {
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }

    /// Enqueue `Shutdown` and join the OS thread under a **single**
    /// [`SHUTDOWN_JOIN_TIMEOUT`] (Architecture §7.4).
    ///
    /// Avoids stacking two 2 s waits (oneshot + join) that would push SIGTERM
    /// past the 5 s process budget when combined with the 3 s drain.
    pub async fn shutdown_and_join(mut self) {
        use std::time::Instant;

        let deadline = Instant::now() + SHUTDOWN_JOIN_TIMEOUT;
        let rx = self.handle.begin_shutdown().await;
        let join = self.join.take();

        if let Some(rx) = rx {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                let _ = tokio::time::timeout(remaining, rx).await;
            }
        }

        if let Some(j) = join {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tracing::warn!(
                    timeout_secs = SHUTDOWN_JOIN_TIMEOUT.as_secs(),
                    "chain-core Shutdown oneshot exhausted budget; abandoning OS join"
                );
                // Detach: JoinHandle drop does not abort the OS thread; process
                // exit reaps it. Prefer not to block drain past the envelope.
                return;
            }
            let join_task = tokio::task::spawn_blocking(move || {
                let _ = j.join();
            });
            match tokio::time::timeout(remaining, join_task).await {
                Ok(Ok(())) => {
                    tracing::info!("chain-core thread joined");
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "chain-core join task failed");
                }
                Err(_) => {
                    tracing::warn!(
                        timeout_secs = SHUTDOWN_JOIN_TIMEOUT.as_secs(),
                        "chain-core join timed out within single Shutdown+join budget; continuing drain"
                    );
                }
            }
        }
    }
}

/// Spawn the dedicated OS core thread owning `store` **by value**.
///
/// This is a dedicated OS thread (ADR-P1-09) — not a tokio task or blocking pool worker.
///
/// When `epoch` is `None`, a fresh [`EpochContextStore`] is created and published
/// at bootstrap. Callers that need the store before spawn (e.g. gRPC service)
/// pass their own.
pub fn spawn_core_thread<P: Preset + 'static>(
    store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> CoreThread {
    spawn_core_thread_with_epoch(
        store,
        config,
        head,
        EpochContextStore::new(),
        event_tx,
        metrics,
        core_cfg,
    )
}

/// Like [`spawn_core_thread`] but reuses a caller-owned [`EpochContextStore`].
pub fn spawn_core_thread_with_epoch<P: Preset + 'static>(
    store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> CoreThread {
    let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let counters = Arc::new(ImportCounters::default());
    let counters_thread = Arc::clone(&counters);
    let head_thread = head.clone();
    let epoch_thread = epoch.clone();
    let metrics_thread = metrics.clone();

    // Publish initial snapshots from the seeded store so GetHead / ChainView
    // work immediately after spawn (tests / post-bootstrap).
    publish_initial_snapshot(&store, &head);
    publish_epoch_context_from_store(&store, &config, &epoch, 0);

    // Per-slot fcU floor ticker (CC-33 /7). try_send so a busy import queue
    // never blocks the ticker; drops under load are fine (next slot retries).
    let tick_tx = cmd_tx.clone();
    let tick_secs = config.seconds_per_slot.max(1);
    let _fcu_ticker = thread::Builder::new()
        .name("chain-fcu-floor".into())
        .spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(tick_secs));
                if tick_tx.try_send(CoreCommand::SlotTick).is_err() {
                    // Channel full or closed — exit if closed; otherwise skip.
                    if tick_tx.is_closed() {
                        break;
                    }
                }
            }
        })
        .ok();

    // Capture multi-threaded runtime handle for GrpcFcuSink (§2.4). Absent in
    // pure unit tests that spawn the core off a runtime → fcU stays disabled.
    let rt_handle = tokio::runtime::Handle::try_current().ok();

    let join = thread::Builder::new()
        .name("chain-core".into())
        .spawn(move || {
            core_loop(
                store,
                config,
                head_thread,
                epoch_thread,
                event_tx,
                metrics_thread,
                counters_thread,
                core_cfg,
                cmd_rx,
                rt_handle,
            );
        })
        .unwrap_or_else(|e| {
            // OS thread spawn failure is unrecoverable for the process.
            tracing::error!(error = %e, "failed to spawn chain-core thread");
            std::process::abort();
        });

    CoreThread {
        handle: CoreHandle {
            cmd_tx,
            head,
            epoch,
            metrics,
            counters,
        },
        join: Some(join),
    }
}

/// Resolve the head block root from the store.
fn head_root_of<P: Preset>(store: &Store<P>) -> Root {
    store
        .last_head_root()
        .or_else(|| store.head_cache().map(|c| c.head_root))
        .unwrap_or_else(|| store.justified_checkpoint().root)
}

/// Head state for Query handlers: store block_states (seeded at bootstrap and
/// kept in sync with residency pins on every import).
fn head_state<'a, P: Preset>(
    store: &'a Store<P>,
    _residency: &'a Residency<P>,
    head_root: Root,
) -> Result<&'a BeaconState<P>, Status> {
    store.block_state(&head_root).ok_or_else(|| {
        Status::failed_precondition("head state not resident; chain not ready for state queries")
    })
}

/// Decision root returned to callers for the served epoch window.
///
/// - When `start_slot(epoch) − 1` is already historical: the true decision root
///   (same as [`decision_root_for_epoch`] / `get_or_compute_shuffling`).
/// - When serving **next** epoch mid-epoch (dependent slot still current/future):
///   the **current** epoch's true decision root — stable for the whole epoch on
///   a branch, branch-distinguishing across forks, and never thrashing per slot.
///   Next-epoch seed is already fixed from RANDAO (`MIN_SEED_LOOKAHEAD`); the
///   assignment does not change as head advances within the epoch.
fn dependent_root_for_epoch_served<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
) -> Result<Root, Status> {
    match decision_root_for_epoch(state, epoch) {
        Ok(root) => Ok(root),
        Err(_) => {
            // Next-epoch mid-window: stable provisional = current epoch decision root.
            let current = get_current_epoch(state);
            decision_root_for_epoch(state, current).map_err(|e| {
                Status::internal(format!(
                    "stable provisional dependent root (current epoch {}) for next epoch {}: {e}",
                    current.as_u64(),
                    epoch.as_u64()
                ))
            })
        }
    }
}

/// Packed shuffling for `epoch`.
///
/// When the strict decision root is available, uses [`get_or_compute_shuffling`]
/// so the head-state [`cc_types::ShufflingCache`] key matches in-process
/// `get_beacon_committee`. When only a provisional root is available, computes
/// without inserting under a foreign key (avoids duplicate/orphan cache entries).
fn shuffled_for_epoch<P: Preset>(state: &BeaconState<P>, epoch: Epoch) -> Result<Vec<u64>, Status> {
    if decision_root_for_epoch(state, epoch).is_ok() {
        let shuffling = get_or_compute_shuffling(state, epoch).map_err(|e| {
            Status::internal(format!(
                "compute shuffling for epoch {}: {e}",
                epoch.as_u64()
            ))
        })?;
        return Ok(shuffling.shuffled.iter().map(|vi| vi.as_u64()).collect());
    }
    // Provisional next-epoch path: seed-only compute; do not warm ShufflingCache
    // under a non-strict key (that would never match get_or_compute_shuffling).
    let computed = compute_shuffled_active_indices(state, epoch).map_err(|e| {
        Status::internal(format!(
            "compute shuffling for epoch {}: {e}",
            epoch.as_u64()
        ))
    })?;
    Ok(computed.shuffled.iter().map(|vi| vi.as_u64()).collect())
}

fn handle_query<P: Preset>(
    store: &Store<P>,
    residency: &Residency<P>,
    request: QueryRequest,
) -> Result<QueryReply, Status> {
    let head_root = head_root_of(store);
    match request {
        QueryRequest::Head => {
            let head_slot = store
                .blocks()
                .get(&head_root)
                .map(|h| h.slot.as_u64())
                .unwrap_or(0);
            Ok(QueryReply::Head {
                head_root,
                head_slot,
            })
        }
        QueryRequest::CommitteeShuffling { epoch } => {
            let state = head_state(store, residency, head_root)?;
            let current = get_current_epoch(state).as_u64();
            let next = current.saturating_add(1);
            if epoch != current && epoch != next {
                return Err(Status::failed_precondition(format!(
                    "GetCommitteeShuffling serves only head current and next epoch \
                     (current={current}, next={next}, requested={epoch})"
                )));
            }
            let epoch_ty = Epoch::new(epoch);
            let dependent_root = dependent_root_for_epoch_served(state, epoch_ty)?;
            let shuffled_indices = shuffled_for_epoch(state, epoch_ty)?;
            let committees_per_slot = get_committee_count_per_slot(state, epoch_ty);
            Ok(QueryReply::CommitteeShuffling {
                shuffled_indices,
                dependent_root,
                epoch,
                committees_per_slot,
            })
        }
        QueryRequest::ValidatorPubkeys { indices } => {
            if indices.len() as u64 > MAX_VALIDATOR_PUBKEYS_PER_REQUEST {
                return Err(Status::invalid_argument(format!(
                    "GetValidatorPubkeys bound is {MAX_VALIDATOR_PUBKEYS_PER_REQUEST} indices; \
                     got {}",
                    indices.len()
                )));
            }
            let state = head_state(store, residency, head_root)?;
            let mut pubkeys = Vec::with_capacity(indices.len());
            for &idx in &indices {
                let v = state.validators_get(idx as usize).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "validator index {idx} out of range (registry len {})",
                        state.validators_len()
                    ))
                })?;
                pubkeys.push(v.pubkey.as_slice().to_vec());
            }
            Ok(QueryReply::ValidatorPubkeys { indices, pubkeys })
        }
        QueryRequest::ValidatorRecords { indices } => {
            if indices.len() as u64 > MAX_VALIDATOR_RECORDS_PER_REQUEST {
                return Err(Status::invalid_argument(format!(
                    "GetValidatorRecords bound is {MAX_VALIDATOR_RECORDS_PER_REQUEST} indices; \
                     got {}",
                    indices.len()
                )));
            }
            let state = head_state(store, residency, head_root)?;
            let slot = state.slot().as_u64();
            let mut ssz = Vec::with_capacity(indices.len());
            for &idx in &indices {
                let v = state.validators_get(idx as usize).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "validator index {idx} out of range (registry len {})",
                        state.validators_len()
                    ))
                })?;
                ssz.push(v.as_ssz_bytes());
            }
            Ok(QueryReply::ValidatorRecords { ssz, slot })
        }
        // CC-3B: answers from fork choice only (is_optimistic / is_optimistic_node).
        // EngineState is never consulted here — el_offline is EngineService's job.
        QueryRequest::IsOptimistic { root } => match root {
            None => Ok(QueryReply::IsOptimistic {
                is_optimistic: is_optimistic_node(store),
                known: true,
            }),
            Some(r) => match is_optimistic(store, r) {
                Some(flag) => Ok(QueryReply::IsOptimistic {
                    is_optimistic: flag,
                    known: true,
                }),
                // Unknown root: known=false so Phase 6 cannot invent is_optimistic=false.
                None => Ok(QueryReply::IsOptimistic {
                    is_optimistic: false,
                    known: false,
                }),
            },
        },
    }
}

/// Build and publish [`EpochContext`] from the head state's registry / lookahead.
pub fn build_epoch_context<P: Preset>(
    state: &BeaconState<P>,
    config: &ChainConfig,
    sequence: u64,
) -> EpochContext {
    let epoch = get_current_epoch(state);
    let active = get_active_validator_indices(state, epoch);
    let mut proposer_lookahead = Vec::with_capacity(state.proposer_lookahead_len());
    let mut proposer_pubkeys = Vec::with_capacity(state.proposer_lookahead_len());
    for i in 0..state.proposer_lookahead_len() {
        let idx = state
            .proposer_lookahead_get(i)
            .map(|v| v.as_u64())
            .unwrap_or(0);
        proposer_lookahead.push(idx);
        let pk = state
            .validators_get(idx as usize)
            .map(|v| v.pubkey.as_slice().to_vec())
            .unwrap_or_else(|| vec![0u8; 48]);
        proposer_pubkeys.push(pk);
    }
    EpochContext {
        epoch,
        proposer_lookahead,
        proposer_pubkeys,
        active_validator_count: active.len() as u64,
        genesis_time: state.genesis_time(),
        genesis_validators_root: state.genesis_validators_root(),
        seconds_per_slot: config.seconds_per_slot,
        slots_per_epoch: P::SLOTS_PER_EPOCH,
        sequence,
    }
}

fn publish_epoch_context_from_store<P: Preset>(
    store: &Store<P>,
    config: &ChainConfig,
    epoch_store: &EpochContextStore,
    sequence: u64,
) {
    let head_root = head_root_of(store);
    if let Some(state) = store.block_state(&head_root) {
        epoch_store.store(build_epoch_context(state, config, sequence));
    } else {
        // No resident state yet — publish stable chain params only.
        epoch_store.store(EpochContext {
            seconds_per_slot: config.seconds_per_slot,
            slots_per_epoch: P::SLOTS_PER_EPOCH,
            sequence,
            ..EpochContext::default()
        });
    }
}

fn publish_initial_snapshot<P: Preset>(store: &Store<P>, head: &HeadSnapshotStore) {
    let justified = store.justified_checkpoint();
    let finalized = store.finalized_checkpoint();
    let head_root = store.last_head_root().unwrap_or(justified.root);
    let head_slot = store
        .blocks()
        .get(&head_root)
        .map(|h| h.slot)
        .unwrap_or_else(|| store.get_current_slot());
    let head_state_root = store
        .blocks()
        .get(&head_root)
        .map(|h| h.state_root)
        .unwrap_or(Root::ZERO);
    head.store(HeadSnapshot {
        head_root,
        head_slot,
        head_state_root,
        justified,
        finalized,
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        current_epoch_target_root: Root::ZERO,
        dependent_root: Root::ZERO,
        // CC-3B: node-level predicate from fork choice (not engine liveness).
        is_optimistic: is_optimistic_node(store),
        sequence: 0,
    });
}

#[allow(clippy::too_many_arguments)]
fn core_loop<P: Preset>(
    mut store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    counters: Arc<ImportCounters>,
    core_cfg: CoreConfig,
    mut cmd_rx: mpsc::Receiver<CoreCommand>,
    rt_handle: Option<tokio::runtime::Handle>,
) {
    let mut residency =
        Residency::<P>::new(core_cfg.max_resident_states, core_cfg.body_ring_capacity);
    // Seed residency from the anchor (finalized / justified root).
    let anchor_root = store.finalized_checkpoint().root;
    if let Some(st) = store.block_state(&anchor_root).cloned() {
        residency.seed_anchor(anchor_root, st);
        metrics.set_resident_states(residency.resident_count() as u64);
    }

    let mut snapshot_sequence: u64 = 0;
    let mut epoch_sequence: u64 = epoch.load().sequence;
    let mut last_published_epoch = epoch.load().epoch.as_u64();
    let verify = core_cfg.verify;
    let peer_das = core_cfg.peer_das;
    let da_timeout_slots = core_cfg.da_pending_timeout_slots.max(1);
    let engine_timeout_slots = core_cfg.engine_pending_timeout_slots.max(1);
    let mut pending_da = PendingDa::new();
    let mut pending_engine = PendingEngine::new();
    // Last observed engine Online bit (CC-36a Offline→Online redrive edge).
    let mut last_engine_online = false;
    let engine_uri = core_cfg.engine_uri.clone();
    // Keep a handle for GetEngineState polling (fcU sink consumes its own clone).
    let poll_handle = rt_handle.clone();

    // CC-33: forkchoiceUpdated driver (off attestation path — after import /
    // on slot tick). Requires a multi-threaded runtime handle for gRPC.
    let fcu: Option<FcuDriver<GrpcFcuSink>> = rt_handle.map(|h| {
        let sink = Arc::new(GrpcFcuSink::new(h, engine_uri.clone()));
        tracing::info!(
            engine_uri = %engine_uri,
            session_id = sink.session_id(),
            "fcU driver armed (CC-33)"
        );
        FcuDriver::new(sink)
    });
    if fcu.is_none() {
        tracing::debug!("fcU driver disabled (no tokio runtime handle on core spawn)");
    }

    while let Some(cmd) = cmd_rx.blocking_recv() {
        metrics.set_import_queue_depth(cmd_rx.len() as u64);
        // Slot-bounded timeout: drop permanently unavailable parked blocks.
        expire_pending_da(
            &mut pending_da,
            store.get_current_slot().as_u64(),
            da_timeout_slots,
            &metrics,
        );
        expire_pending_engine(
            &mut pending_engine,
            store.get_current_slot().as_u64(),
            engine_timeout_slots,
            &metrics,
        );
        if let Some(ref da) = peer_das {
            metrics.set_da_available_occupancy(da.len() as u64);
        }
        match cmd {
            CoreCommand::ImportBlock { request, reply } => {
                let outcome = import_block_with_early(
                    &mut store,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &metrics,
                    &counters,
                    &mut snapshot_sequence,
                    request,
                    verify,
                    None,
                    None,
                    None,
                    Some(&mut pending_da),
                    Some(&mut pending_engine),
                );
                metrics.set_da_pending_occupancy(pending_da.len() as u64);
                metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
                // Republish EpochContext when the head epoch advances (§16/4).
                if outcome.is_ok() {
                    maybe_publish_epoch_context(
                        &store,
                        &config,
                        &epoch,
                        &mut epoch_sequence,
                        &mut last_published_epoch,
                    );
                    // Post-import fcU (off attestation path — after snapshot publish).
                    emit_fcu_head(&store, fcu.as_ref());
                }
                let _ = reply.send(outcome.map(|o| o.response));
            }
            CoreCommand::ImportBlockGossip {
                request,
                early_accept,
                reply,
            } => {
                let epoch_snapshot = epoch.load();
                let outcome = import_block_with_early(
                    &mut store,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &metrics,
                    &counters,
                    &mut snapshot_sequence,
                    request,
                    verify,
                    Some(epoch_snapshot.as_ref()),
                    early_accept,
                    None, // production: no inject
                    Some(&mut pending_da),
                    Some(&mut pending_engine),
                );
                metrics.set_da_pending_occupancy(pending_da.len() as u64);
                metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
                if outcome.is_ok() {
                    maybe_publish_epoch_context(
                        &store,
                        &config,
                        &epoch,
                        &mut epoch_sequence,
                        &mut last_published_epoch,
                    );
                    emit_fcu_head(&store, fcu.as_ref());
                }
                let _ = reply.send(outcome);
            }
            CoreCommand::ApplyAttestations { request, reply } => {
                let outcome = apply_attestations(
                    &mut store,
                    &head,
                    &event_tx,
                    &metrics,
                    &mut snapshot_sequence,
                    request,
                );
                // Attestations can move head; re-point EL when they do.
                if outcome.is_ok() {
                    emit_fcu_head(&store, fcu.as_ref());
                }
                let _ = reply.send(outcome);
            }
            CoreCommand::Query { request, reply } => {
                let outcome = handle_query(&store, &residency, request);
                let _ = reply.send(outcome);
            }
            CoreCommand::BlockFor { duration, reply } => {
                thread::sleep(duration);
                let _ = reply.send(());
            }
            CoreCommand::DataAvailable { root, slot } => {
                handle_data_available(
                    &mut store,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &metrics,
                    &counters,
                    &mut snapshot_sequence,
                    &mut pending_da,
                    peer_das.as_ref(),
                    verify,
                    root,
                    slot,
                    &mut epoch_sequence,
                    &mut last_published_epoch,
                    &epoch,
                );
                // Re-import may have moved head.
                emit_fcu_head(&store, fcu.as_ref());
            }
            CoreCommand::SlotTick => {
                // Advance store time so slot-bounded pending_* expiries fire
                // (CC-36a: 8-slot pending_engine must not stick forever).
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                if let Err(e) = on_tick(&mut store, now) {
                    tracing::debug!(error = %e, now, "on_tick on SlotTick skipped");
                }
                let current_slot = store.get_current_slot().as_u64();
                expire_pending_da(
                    &mut pending_da,
                    current_slot,
                    da_timeout_slots,
                    &metrics,
                );
                expire_pending_engine(
                    &mut pending_engine,
                    current_slot,
                    engine_timeout_slots,
                    &metrics,
                );

                // Offline → Online: drain pending_engine and re-drive (CC-36a / §4.9).
                if let Some(ref h) = poll_handle {
                    let online = poll_engine_online(h, &engine_uri);
                    if online && !last_engine_online && !pending_engine.is_empty() {
                        tracing::info!(
                            n = pending_engine.len(),
                            "engine online; re-driving pending_engine"
                        );
                        redrive_pending_engine(
                            &mut store,
                            &mut residency,
                            &config,
                            &head,
                            &event_tx,
                            &metrics,
                            &counters,
                            &mut snapshot_sequence,
                            &mut pending_da,
                            &mut pending_engine,
                            verify,
                            &mut epoch_sequence,
                            &mut last_published_epoch,
                            &epoch,
                        );
                        emit_fcu_head(&store, fcu.as_ref());
                    }
                    last_engine_online = online;
                }

                // Per-slot floor even with no new block (CC-33 /7).
                if let Some(driver) = fcu.as_ref() {
                    let slot = store.get_current_slot();
                    if let Err(e) = driver.on_slot(slot) {
                        tracing::warn!(error = %e, slot = slot.as_u64(), "fcU per-slot floor failed");
                    }
                }
            }
            CoreCommand::Shutdown { done } => {
                let _ = done.send(());
                break;
            }
        }
        metrics.set_import_queue_depth(cmd_rx.len() as u64);
    }
}

/// Post-import / post-attestation fcU emission (errors are logged, never fatal).
fn emit_fcu_head<P: Preset>(store: &Store<P>, fcu: Option<&FcuDriver<GrpcFcuSink>>) {
    let Some(driver) = fcu else {
        return;
    };
    let head_root = head_root_of(store);
    match driver.on_head_update(store, head_root) {
        Ok(true) => {
            tracing::debug!(?head_root, "fcU emitted after head update");
        }
        Ok(false) => {
            tracing::debug!(?head_root, "fcU dropped as superseded");
        }
        Err(e) => {
            tracing::warn!(error = %e, ?head_root, "fcU emission failed");
        }
    }
}

/// Drop timed-out `pending_da` entries and bump the metric.
fn expire_pending_da(
    pending: &mut PendingDa,
    current_slot: u64,
    timeout_slots: u64,
    metrics: &ChainMetrics,
) {
    let dropped = pending.expire(current_slot, timeout_slots);
    if !dropped.is_empty() {
        metrics.inc_da_pending_dropped(dropped.len() as u64);
        for e in &dropped {
            tracing::debug!(
                root = %e.root,
                slot = e.slot,
                parked_at_slot = e.parked_at_slot,
                current_slot,
                "pending_da entry dropped after timeout"
            );
        }
    }
    metrics.set_da_pending_occupancy(pending.len() as u64);
}

/// Mark root available and re-drive a parked block when present (CC-24d).
#[allow(clippy::too_many_arguments)]
fn handle_data_available<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head: &HeadSnapshotStore,
    event_tx: &mpsc::Sender<crate::events::EventInput>,
    metrics: &ChainMetrics,
    counters: &ImportCounters,
    snapshot_sequence: &mut u64,
    pending_da: &mut PendingDa,
    peer_das: Option<&Arc<PeerDasAvailability>>,
    verify: BlockSignatureStrategy,
    root: Root,
    slot: u64,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
    epoch: &EpochContextStore,
) {
    if let Some(da) = peer_das {
        da.mark_available(root);
        metrics.set_da_available_occupancy(da.len() as u64);
    } else {
        tracing::trace!(
            %root,
            slot,
            "DataAvailable received but no PeerDasAvailability handle; mark skipped"
        );
    }

    let Some(entry) = pending_da.take(&root) else {
        // Signal arrived before the block — import will succeed on first attempt.
        metrics.set_da_pending_occupancy(pending_da.len() as u64);
        tracing::debug!(%root, slot, "DataAvailable; no pending_da entry");
        return;
    };
    metrics.set_da_pending_occupancy(pending_da.len() as u64);
    tracing::debug!(%root, slot, "DataAvailable; re-driving pending_da entry");

    let request = ImportBlockRequest {
        ssz: entry.ssz.to_vec(),
        fork: entry.fork,
        root: root.as_slice().to_vec(),
        source: if entry.source == 0 {
            Source::Gossip as i32
        } else {
            entry.source
        },
    };
    let outcome = import_block_with_early(
        store,
        residency,
        config,
        head,
        event_tx,
        metrics,
        counters,
        snapshot_sequence,
        request,
        verify,
        None,
        None,
        None,
        Some(pending_da),
        None, // re-drive is DA-only; engine map is separate
    );
    if outcome.is_ok() {
        maybe_publish_epoch_context(store, config, epoch, epoch_sequence, last_published_epoch);
    }
}

/// Drop timed-out `pending_engine` entries and bump the metric (CC-36a).
fn expire_pending_engine(
    pending: &mut PendingEngine,
    current_slot: u64,
    timeout_slots: u64,
    metrics: &ChainMetrics,
) {
    let dropped = pending.expire(current_slot, timeout_slots);
    if !dropped.is_empty() {
        metrics.inc_pending_engine_dropped(dropped.len() as u64);
        for e in &dropped {
            tracing::debug!(
                root = %e.root,
                parked_at_slot = e.parked_at_slot,
                current_slot,
                "pending_engine entry dropped after timeout"
            );
        }
    }
    metrics.set_pending_engine_occupancy(pending.len() as u64);
}

/// Re-import every parked engine-deferred block (Offline → Online edge).
#[allow(clippy::too_many_arguments)]
fn redrive_pending_engine<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head: &HeadSnapshotStore,
    event_tx: &mpsc::Sender<crate::events::EventInput>,
    metrics: &ChainMetrics,
    counters: &ImportCounters,
    snapshot_sequence: &mut u64,
    pending_da: &mut PendingDa,
    pending_engine: &mut PendingEngine,
    verify: BlockSignatureStrategy,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
    epoch: &EpochContextStore,
) {
    let entries = pending_engine.drain_oldest_first();
    metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
    for entry in entries {
        let root = entry.root;
        let request = ImportBlockRequest {
            ssz: entry.ssz.to_vec(),
            fork: entry.fork,
            root: root.as_slice().to_vec(),
            source: if entry.source == 0 {
                Source::Gossip as i32
            } else {
                entry.source
            },
        };
        tracing::debug!(%root, slot = entry.slot, "re-driving pending_engine entry");
        let outcome = import_block_with_early(
            store,
            residency,
            config,
            head,
            event_tx,
            metrics,
            counters,
            snapshot_sequence,
            request,
            verify,
            None,
            None,
            None,
            Some(pending_da),
            Some(pending_engine),
        );
        metrics.set_da_pending_occupancy(pending_da.len() as u64);
        metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
        if outcome.is_ok() {
            maybe_publish_epoch_context(
                store,
                config,
                epoch,
                epoch_sequence,
                last_published_epoch,
            );
        }
    }
}

/// Publish a new [`EpochContext`] when the head state's epoch has advanced.
fn maybe_publish_epoch_context<P: Preset>(
    store: &Store<P>,
    config: &ChainConfig,
    epoch_store: &EpochContextStore,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
) {
    let head_root = head_root_of(store);
    let Some(state) = store.block_state(&head_root) else {
        return;
    };
    let current = get_current_epoch(state).as_u64();
    if current == *last_published_epoch && epoch_store.load().sequence > 0 {
        return;
    }
    *epoch_sequence = epoch_sequence.saturating_add(1);
    *last_published_epoch = current;
    epoch_store.store(build_epoch_context(state, config, *epoch_sequence));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

/// Private always-Valid test harness (CC-32b: production stub deleted; not exported).
#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl<P: cc_types::preset::Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: cc_state_transition::NewPayloadRequest<'_, P>,
    ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
        Ok(cc_state_transition::PayloadStatus::Valid)
    }
}

    use super::*;
    use std::sync::Arc;

    use cc_fork_choice::{HarnessAvailability, get_forkchoice_store};
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState};
    use prometheus_client::registry::Registry;
    use tree_hash::TreeHash;

    use crate::events::{EventsConfig, EventsHandle};

    fn minimal_config() -> ChainConfig {
        ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
            genesis_fork_version: ForkVersion::from_array([0x00, 0x00, 0x00, 0x01]),
            altair_fork_version: ForkVersion::from_array([0x01, 0x00, 0x00, 0x01]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([0x02, 0x00, 0x00, 0x01]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([0x03, 0x00, 0x00, 0x01]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([0x04, 0x00, 0x00, 0x01]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([0x05, 0x00, 0x00, 0x01]),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([0x06, 0x00, 0x00, 0x01]),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 6,
            blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 9,
            }])
            .unwrap(),
            deposit_chain_id: 0,
            deposit_contract_address: ExecutionAddress::ZERO,
        }
    }

    fn seeded_store() -> (Store<Minimal>, Root, ChainConfig) {
        let config = minimal_config();
        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        let anchor_block = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            config.seconds_per_slot,
        )
        .unwrap();
        let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        (store, anchor_root, config)
    }

    #[tokio::test]
    async fn core_thread_is_os_thread_query_works() {
        let (store, anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig {
            ring_capacity: 16,
            subscriber_queue_capacity: 8,
            session_id: Some(1),
        });
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head.clone(),
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );
        let q = core.handle.query(QueryRequest::Head).await.unwrap();
        let head_root = match q {
            QueryReply::Head {
                head_root,
                head_slot: _,
            } => head_root,
            QueryReply::CommitteeShuffling { .. }
            | QueryReply::ValidatorPubkeys { .. }
            | QueryReply::ValidatorRecords { .. }
            | QueryReply::IsOptimistic { .. } => {
                unreachable!("Head request must yield Head reply")
            }
        };
        assert_eq!(head_root, anchor);
        // GetHead snapshot was seeded.
        assert_eq!(head.load().head_root, anchor);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn get_head_unaffected_while_core_blocked() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head.clone(),
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        // Kick off a 2s block on the core without waiting.
        let h = core.handle.clone();
        let block_task = tokio::spawn(async move {
            h.block_for(Duration::from_secs(2)).await.unwrap();
        });
        // Give the core thread time to enter sleep.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let start = std::time::Instant::now();
        let snap = head.load();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(10),
            "GetHead snapshot load took {elapsed:?}, expected < 10ms"
        );
        assert_eq!(snap.sequence, 0);

        block_task.await.unwrap();
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }
}
