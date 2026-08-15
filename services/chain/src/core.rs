//! Dedicated OS core thread owning fork-choice [`Store`] by value (ADR-P1-09).
//!
//! Communication: dedicated Loop B lanes plus the leftover mixed
//! `tokio::sync::mpsc` (capacity 64). The core thread first-match-wins across
//! tick → import → query_p0 → mixed, then parks until a producer notifies.
//! `oneshot` replies. `ImportBlock` uses `send_timeout(2 s)` →
//! `RESOURCE_EXHAUSTED` on backpressure (policy A).
//!
//! Wired lanes ([ARCH] §3.2):
//! - `tick` — never-shed `SlotTick` + `Shutdown` ([S0-A-14] / S0-A-15)
//! - `import` — `ImportBlock` / `ImportBlockGossip` / `DataAvailable` (FIFO 64)
//! - `query_p0` — `Query{Head, IsOptimistic}` + head probes (FIFO 64)
//!
//! The mixed channel stays for commands not yet moved (S0-A-16 / deleted at
//! S0-A-17). First-match-wins: tick → import → query_p0 → mixed.
//!
//! ```text
//! loop { recv(); handle(); /* snapshot + events inside import */ }
//! ```
//!
//! CC-1F state-requiring reads (`GetCommitteeShuffling`, `GetValidatorPubkeys`)
//! and CC-27a `GetValidatorRecords` stay on the mixed
//! [`CoreCommand::Query`] path until S0-A-16 — no second copy of the head
//! state is held on the gRPC side. Epoch-scoped data for `ChainView` is
//! published via [`EpochContextStore`] (second `ArcSwap`, Architecture §16/4).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use cc_fork_choice::{PeerDasAvailability, Store, is_optimistic, is_optimistic_node, on_tick};
use cc_proto::chain::{
    ApplyAttestationsRequest, ApplyAttestationsResponse, ImportBlockRequest, ImportBlockResponse,
};
use cc_proto::common::Source;
use cc_scheduler::{
    ChainLane, Enqueue, IMPORT_LANE_DEPTH, Manager, QUERY_P0_LANE_DEPTH, QueueSizes,
    TICK_LANE_DEPTH,
};
use cc_state_transition::helpers::accessors::get_active_validator_indices;
use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::{
    BlockSignatureStrategy, compute_shuffled_active_indices, decision_root_for_epoch,
    get_committee_count_per_slot, get_current_epoch, get_or_compute_shuffling,
};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, Slot};
use ssz::Encode;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use crate::apply_attestations::apply_attestations;
use crate::da::{DEFAULT_DA_PENDING_TIMEOUT_SLOTS, PendingDa};
use crate::engine_client::{fire_fetch_blobs, poll_engine_online};
use crate::epoch_context::{EpochContext, EpochContextStore};
use crate::fcu_driver::{FcuDriver, GrpcFcuSink};
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::import::{ImportCounters, ImportOutcome, import_block_with_early};
use crate::metrics::ChainMetrics;
use crate::pending_engine::{DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS, PendingEngine};
use crate::residency::{DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency};
use crate::tick::{
    DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY, GossipClock, TickWork, advance_store_clock,
    duration_until_next_slot_boundary, unix_now_millis, unix_now_secs,
};

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
    /// State-requiring read. `query_p0` variants
    /// ([`QueryRequest::is_query_p0`]) ride their own lane; the rest stay on
    /// this mixed channel until S0-A-16.
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

/// Work that rides the `import` lane ([ARCH] §3.2 / S0-A-15).
///
/// FIFO depth [`IMPORT_LANE_DEPTH`], policy A (can shed). `DataAvailable`
/// stays here — it re-drives a parked block, so promoting it starves imports.
#[derive(Debug)]
pub enum ImportWork {
    ImportBlock {
        request: ImportBlockRequest,
        reply: oneshot::Sender<Result<ImportBlockResponse, Status>>,
    },
    ImportBlockGossip {
        request: ImportBlockRequest,
        early_accept: Option<oneshot::Sender<()>>,
        reply: oneshot::Sender<Result<ImportOutcome, Status>>,
    },
    DataAvailable {
        root: Root,
        slot: u64,
    },
}

impl From<ImportWork> for CoreCommand {
    fn from(work: ImportWork) -> Self {
        match work {
            ImportWork::ImportBlock { request, reply } => Self::ImportBlock { request, reply },
            ImportWork::ImportBlockGossip {
                request,
                early_accept,
                reply,
            } => Self::ImportBlockGossip {
                request,
                early_accept,
                reply,
            },
            ImportWork::DataAvailable { root, slot } => Self::DataAvailable { root, slot },
        }
    }
}

/// Work that rides the `query_p0` lane (Lighthouse `ApiRequestP0`).
///
/// FIFO depth [`QUERY_P0_LANE_DEPTH`]. Head probes must not wait behind a
/// mixed-channel attestation flood or a `query_p1` read.
#[derive(Debug)]
pub enum QueryP0Work {
    Query {
        request: QueryRequest,
        reply: oneshot::Sender<Result<QueryReply, Status>>,
    },
}

impl From<QueryP0Work> for CoreCommand {
    fn from(work: QueryP0Work) -> Self {
        match work {
            QueryP0Work::Query { request, reply } => Self::Query { request, reply },
        }
    }
}

/// First-match-wins inbound from the wired lanes plus the leftover mixed channel.
enum Incoming {
    Tick(TickWork),
    Import(ImportWork),
    QueryP0(QueryP0Work),
    Mixed(CoreCommand),
}

/// Park/unpark so dedicated-lane sends wake an idle core without a second runtime.
#[derive(Debug)]
struct LaneWake {
    signaled: AtomicBool,
    parked: Mutex<Option<thread::Thread>>,
}

impl LaneWake {
    fn new() -> Self {
        Self {
            signaled: AtomicBool::new(false),
            parked: Mutex::new(None),
        }
    }

    fn notify(&self) {
        self.signaled.store(true, Ordering::SeqCst);
        if let Some(t) = self.parked_lock().clone() {
            t.unpark();
        }
    }

    fn park_current(&self) {
        *self.parked_lock() = Some(thread::current());
        if !self.signaled.swap(false, Ordering::SeqCst) {
            thread::park();
            self.signaled.store(false, Ordering::SeqCst);
        }
    }

    fn parked_lock(&self) -> std::sync::MutexGuard<'_, Option<thread::Thread>> {
        self.parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Wakes a parked core after this clone's channel sender is dropped.
#[derive(Debug, Clone)]
struct NotifyOnDrop(Arc<LaneWake>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify();
    }
}

/// Test/producer sender that unparks the core after a successful send (and on drop).
#[derive(Debug, Clone)]
pub struct WakingSender<T> {
    tx: mpsc::Sender<T>,
    wake: Arc<LaneWake>,
}

impl<T> WakingSender<T> {
    fn new(tx: mpsc::Sender<T>, wake: Arc<LaneWake>) -> Self {
        Self { tx, wake }
    }

    /// `try_send` then unpark so an idle core observes the item.
    pub fn try_send(&self, msg: T) -> Result<(), mpsc::error::TrySendError<T>> {
        self.tx.try_send(msg).inspect(|()| self.wake.notify())
    }

    #[must_use]
    pub fn max_capacity(&self) -> usize {
        self.tx.max_capacity()
    }
}

impl<T> Drop for WakingSender<T> {
    fn drop(&mut self) {
        self.wake.notify();
    }
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
    /// CC-44a /3: one canonical root per slot in `[start_slot, end_slot]`.
    CanonicalRoots { start_slot: u64, end_slot: u64 },
    /// Fork-choice store clock (`store.time` / `get_current_slot`).
    StoreClock,
}

impl QueryRequest {
    /// `query_p0` / Lighthouse `ApiRequestP0`: head, optimistic status, clock.
    ///
    /// `query_p1` variants stay on the mixed channel until S0-A-16.
    #[must_use]
    pub const fn is_query_p0(&self) -> bool {
        matches!(
            self,
            Self::Head | Self::IsOptimistic { .. } | Self::StoreClock
        )
    }
}

/// Reply for the Phase-1 / CC-27a / CC-3B / CC-44a `Query` command.
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
    /// CC-44a: canonical roots for the requested inclusive slot range.
    CanonicalRoots { roots: Vec<Root> },
    /// Fork-choice store clock.
    StoreClock { time: u64, slot: u64 },
}

/// Configuration for spawning the core thread.
#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub max_resident_states: usize,
    pub body_ring_capacity: usize,
    /// Unary `ImportBlock` / `on_block` signature strategy.
    ///
    /// Default [`BlockSignatureStrategy::VerifyIndividual`]. Restore replay
    /// overrides to [`BlockSignatureStrategy::NoVerification`] in
    /// [`crate::restore::apply_restore_set`] (already-verified stored blocks).
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
    /// Spawn the per-slot `SlotTick` floor (fcU floor + pending_* expiry + wall-clock
    /// `on_tick`).
    ///
    /// **Default `false`.** Fixture/integration tests carefully seed store time
    /// (e.g. Hoodi offline replay); a wall-clock tick would jump past the
    /// imported chain and leave `get_head` stranded. Production `main` enables
    /// this (CC-33 /7, CC-36a). Also gates wall-clock `on_tick` at the top of
    /// each import (P0-12).
    pub slot_tick_enabled: bool,
    /// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` (config; never inlined at the check).
    pub maximum_gossip_clock_disparity: Duration,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            max_resident_states: DEFAULT_MAX_RESIDENT_STATES,
            body_ring_capacity: DEFAULT_BODY_RING_CAPACITY,
            verify: BlockSignatureStrategy::VerifyIndividual,
            peer_das: None,
            da_pending_timeout_slots: DEFAULT_DA_PENDING_TIMEOUT_SLOTS,
            engine_pending_timeout_slots: DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS,
            engine_uri: crate::engine_client::DEFAULT_ENGINE_URI.to_owned(),
            slot_tick_enabled: false,
            maximum_gossip_clock_disparity: DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY,
        }
    }
}

/// Handle to a running core thread (non-generic: channel + shared state only).
#[derive(Debug, Clone)]
pub struct CoreHandle {
    cmd_tx: mpsc::Sender<CoreCommand>,
    /// Never-shed tick lane ([ARCH] §3.2). Depth [`TICK_LANE_DEPTH`].
    tick_tx: mpsc::Sender<TickWork>,
    /// Import lane ([ARCH] §3.2). Depth [`IMPORT_LANE_DEPTH`], policy A.
    import_tx: mpsc::Sender<ImportWork>,
    /// `query_p0` lane ([ARCH] §3.2). Depth [`QUERY_P0_LANE_DEPTH`].
    query_p0_tx: mpsc::Sender<QueryP0Work>,
    /// After the senders so last-handle drop closes channels, then unparks.
    _close_wake: NotifyOnDrop,
    lane_wake: Arc<LaneWake>,
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

    /// Clone of the leftover mixed-channel sender (tests that fill the channel).
    pub fn command_sender(&self) -> mpsc::Sender<CoreCommand> {
        self.cmd_tx.clone()
    }

    /// Clone of the never-shed tick-lane sender (tests).
    pub fn tick_sender(&self) -> mpsc::Sender<TickWork> {
        self.tick_tx.clone()
    }

    /// Clone of the import-lane sender (tests that fill the lane).
    pub fn import_sender(&self) -> WakingSender<ImportWork> {
        WakingSender::new(self.import_tx.clone(), Arc::clone(&self.lane_wake))
    }

    /// Clone of the `query_p0` sender (tests that fill the lane).
    pub fn query_p0_sender(&self) -> WakingSender<QueryP0Work> {
        WakingSender::new(self.query_p0_tx.clone(), Arc::clone(&self.lane_wake))
    }

    /// `try_send` a [`TickWork::SlotTick`]. `false` if the lane is full or closed.
    ///
    /// The production ticker uses `blocking_send` so a full lane waits rather
    /// than shedding. Tests use this to observe capacity without blocking.
    #[must_use]
    pub fn try_send_slot_tick(&self) -> bool {
        let ok = self.tick_tx.try_send(TickWork::SlotTick).is_ok();
        if ok {
            self.lane_wake.notify();
        }
        ok
    }

    /// Import a block via the core channel with the 2 s send timeout.
    pub async fn import_block(
        &self,
        request: ImportBlockRequest,
    ) -> Result<ImportBlockResponse, Status> {
        let (reply, rx) = oneshot::channel();
        let cmd = ImportWork::ImportBlock { request, reply };
        match self.import_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
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
            (IMPORT_LANE_DEPTH.saturating_sub(self.import_tx.capacity())) as u64,
        );
        self.lane_wake.notify();
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
        let cmd = ImportWork::ImportBlockGossip {
            request,
            early_accept,
            reply,
        };
        match self.import_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
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
            (IMPORT_LANE_DEPTH.saturating_sub(self.import_tx.capacity())) as u64,
        );
        self.lane_wake.notify();
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
        self.lane_wake.notify();
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped apply_attestations reply"))?
    }

    /// `Query` command. P0 variants go to `query_p0`; the rest stay mixed.
    ///
    /// Uses the same 2 s send timeout as [`Self::import_block`] so a stalled
    /// core does not hang gRPC workers on state reads (CC-1F / CC-27a).
    pub async fn query(&self, request: QueryRequest) -> Result<QueryReply, Status> {
        let (reply, rx) = oneshot::channel();
        if request.is_query_p0() {
            let cmd = QueryP0Work::Query { request, reply };
            match self
                .query_p0_tx
                .send_timeout(cmd, IMPORT_SEND_TIMEOUT)
                .await
            {
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
        } else {
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
        }
        self.lane_wake.notify();
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
        self.lane_wake.notify();
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped block_for reply"))?;
        Ok(())
    }

    /// Notify the core that sampling completed for `root` (CC-24d).
    ///
    /// Fire-and-forget on the import lane (no reply). Uses the same 2 s
    /// send timeout as import so a stalled core surfaces as unavailable.
    pub async fn notify_data_available(&self, root: Root, slot: u64) -> Result<(), Status> {
        let cmd = ImportWork::DataAvailable { root, slot };
        match self.import_tx.send_timeout(cmd, IMPORT_SEND_TIMEOUT).await {
            Ok(()) => {
                self.lane_wake.notify();
                Ok(())
            }
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => Err(Status::resource_exhausted(
                "data_available command channel full after 2s send_timeout",
            )),
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                Err(Status::unavailable("chain core thread is shut down"))
            }
        }
    }

    /// Enqueue [`TickWork::Shutdown`] on the never-shed tick lane.
    ///
    /// Does not share the mixed or import FIFOs, so SIGTERM pre-drain cannot
    /// wait behind a busy import / `query_p0` lane. Prefer
    /// [`CoreThread::shutdown_and_join`] for production teardown so the
    /// oneshot wait and OS join share a **single** [`SHUTDOWN_JOIN_TIMEOUT`].
    pub async fn begin_shutdown(&self) -> Option<oneshot::Receiver<()>> {
        let (done, rx) = oneshot::channel();
        match self.tick_tx.send(TickWork::Shutdown { done }).await {
            Ok(()) => {
                self.lane_wake.notify();
                Some(rx)
            }
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
    let (tick_tx, tick_rx) = mpsc::channel(TICK_LANE_DEPTH);
    let (import_tx, import_rx) = mpsc::channel(IMPORT_LANE_DEPTH);
    let (query_p0_tx, query_p0_rx) = mpsc::channel(QUERY_P0_LANE_DEPTH);
    let lane_wake = Arc::new(LaneWake::new());
    let counters = Arc::new(ImportCounters::default());
    let counters_thread = Arc::clone(&counters);
    let head_thread = head.clone();
    let epoch_thread = epoch.clone();
    let metrics_thread = metrics.clone();

    // Publish initial snapshots from the seeded store so GetHead / ChainView
    // work immediately after spawn (tests / post-bootstrap).
    publish_initial_snapshot(&store, &head);
    publish_epoch_context_from_store(&store, &config, &epoch, 0);

    // Genesis-aligned never-shed ticker (P0-12 / S0-A-14). `blocking_send` on
    // the tick lane; a droppable `SlotTick` on the mixed channel is only a
    // wakeup for leftover mixed work. Gated by `slot_tick_enabled` so
    // fixture tests keep exclusive control of store time.
    let _fcu_ticker = if core_cfg.slot_tick_enabled {
        Some(spawn_slot_tick_driver(
            tick_tx.clone(),
            cmd_tx.clone(),
            Arc::clone(&lane_wake),
            store.genesis_time(),
            config.seconds_per_slot,
        ))
    } else {
        None
    };

    // Capture multi-threaded runtime handle for GrpcFcuSink (§2.4). Absent in
    // pure unit tests that spawn the core off a runtime → fcU stays disabled.
    let rt_handle = tokio::runtime::Handle::try_current().ok();
    let lane_wake_thread = Arc::clone(&lane_wake);

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
                tick_rx,
                import_rx,
                query_p0_rx,
                lane_wake_thread,
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
            tick_tx,
            import_tx,
            query_p0_tx,
            _close_wake: NotifyOnDrop(Arc::clone(&lane_wake)),
            lane_wake,
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
        // CC-44a /3: walk the head's parent chain; one root per slot in range.
        QueryRequest::CanonicalRoots {
            start_slot,
            end_slot,
        } => {
            let finalized = store.finalized_checkpoint();
            let finalized_slot = compute_start_slot_at_epoch::<P>(finalized.epoch).as_u64();
            if start_slot < finalized_slot {
                return Err(crate::service::status_below_finalized(
                    start_slot,
                    finalized_slot,
                ));
            }
            let head = head_root_of(store);
            let mut roots =
                Vec::with_capacity(end_slot.saturating_sub(start_slot).saturating_add(1) as usize);
            for s in start_slot..=end_slot {
                let root = store
                    .proto_array()
                    .get_ancestor(head, Slot::new(s))
                    .unwrap_or(head);
                roots.push(root);
            }
            Ok(QueryReply::CanonicalRoots { roots })
        }
        QueryRequest::StoreClock => Ok(QueryReply::StoreClock {
            time: store.time(),
            slot: store.get_current_slot().as_u64(),
        }),
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
    mut tick_rx: mpsc::Receiver<TickWork>,
    mut import_rx: mpsc::Receiver<ImportWork>,
    mut query_p0_rx: mpsc::Receiver<QueryP0Work>,
    lane_wake: Arc<LaneWake>,
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
    let slot_tick_enabled = core_cfg.slot_tick_enabled;
    let gossip_disparity = core_cfg.maximum_gossip_clock_disparity;
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

    let mut tick_mgr: Manager<ChainLane, TickWork> =
        match Manager::loop_b(QueueSizes::new(0, P::SLOTS_PER_EPOCH)) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "tick lane manager config");
                std::process::abort();
            }
        };

    loop {
        if let Some(done) = drain_tick_lane(
            &mut tick_mgr,
            &mut tick_rx,
            &mut store,
            &mut pending_da,
            &mut pending_engine,
            da_timeout_slots,
            engine_timeout_slots,
            &metrics,
        ) {
            let _ = done.send(());
            break;
        }
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
        let incoming =
            match try_recv_first_match(&mut tick_rx, &mut import_rx, &mut query_p0_rx, &mut cmd_rx)
            {
                Some(w) => w,
                None if tick_rx.is_closed()
                    && import_rx.is_closed()
                    && query_p0_rx.is_closed()
                    && cmd_rx.is_closed() =>
                {
                    break;
                }
                None => {
                    lane_wake.park_current();
                    continue;
                }
            };
        metrics.set_import_queue_depth(import_rx.len() as u64);
        let cmd = match incoming {
            Incoming::Tick(TickWork::Shutdown { done }) => {
                let _ = done.send(());
                break;
            }
            Incoming::Tick(TickWork::SlotTick) => {
                apply_tick_clock(
                    &mut store,
                    &mut pending_da,
                    &mut pending_engine,
                    da_timeout_slots,
                    engine_timeout_slots,
                    &metrics,
                );
                continue;
            }
            Incoming::Import(work) => CoreCommand::from(work),
            Incoming::QueryP0(work) => CoreCommand::from(work),
            Incoming::Mixed(cmd) => cmd,
        };
        match cmd {
            CoreCommand::ImportBlock { request, reply } => {
                if slot_tick_enabled {
                    advance_store_clock(&mut store);
                }
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
                    import_gossip_clock(slot_tick_enabled, gossip_disparity),
                );
                metrics.set_da_pending_occupancy(pending_da.len() as u64);
                metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
                // CC-38a: fire template-sized FetchBlobs on DA-defer (never cells).
                if let Ok(ref o) = outcome
                    && let Some(trigger) = o.block_branch.as_ref()
                    && let Some(h) = poll_handle.as_ref()
                {
                    fire_fetch_blobs(h, &engine_uri, trigger.to_proto());
                }
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
                if slot_tick_enabled {
                    advance_store_clock(&mut store);
                }
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
                    import_gossip_clock(slot_tick_enabled, gossip_disparity),
                );
                metrics.set_da_pending_occupancy(pending_da.len() as u64);
                metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
                // CC-38a block-branch (same as ImportBlock; template-only).
                if let Ok(ref o) = outcome
                    && let Some(trigger) = o.block_branch.as_ref()
                    && let Some(h) = poll_handle.as_ref()
                {
                    fire_fetch_blobs(h, &engine_uri, trigger.to_proto());
                }
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
                    &config,
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
                if slot_tick_enabled {
                    advance_store_clock(&mut store);
                }
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
                    import_gossip_clock(slot_tick_enabled, gossip_disparity),
                );
                // Re-import may have moved head.
                emit_fcu_head(&store, fcu.as_ref());
            }
            CoreCommand::SlotTick => {
                // Wakeup from the mixed channel, or a test-injected tick.
                // The never-shed lane is drained at the top of the loop; this
                // arm keeps the old command-path tick working until S0-A-17.
                handle_slot_tick(
                    &mut store,
                    &mut pending_da,
                    &mut pending_engine,
                    da_timeout_slots,
                    engine_timeout_slots,
                    &metrics,
                    poll_handle.as_ref(),
                    &engine_uri,
                    &mut last_engine_online,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &counters,
                    &mut snapshot_sequence,
                    &mut epoch_sequence,
                    &mut last_published_epoch,
                    &epoch,
                    fcu.as_ref(),
                    verify,
                );
            }
            CoreCommand::Shutdown { done } => {
                let _ = done.send(());
                break;
            }
        }
        if let Some(done) = drain_tick_lane(
            &mut tick_mgr,
            &mut tick_rx,
            &mut store,
            &mut pending_da,
            &mut pending_engine,
            da_timeout_slots,
            engine_timeout_slots,
            &metrics,
        ) {
            let _ = done.send(());
            break;
        }
        metrics.set_import_queue_depth(import_rx.len() as u64);
    }
}

fn import_gossip_clock(enabled: bool, disparity: Duration) -> Option<GossipClock> {
    enabled.then_some(GossipClock {
        now_millis: unix_now_millis(),
        disparity,
    })
}

/// Genesis-aligned never-shed ticker. `blocking_send` on the tick lane;
/// `try_send` on the mixed command channel is a droppable idle-wakeup only.
fn spawn_slot_tick_driver(
    tick_tx: mpsc::Sender<TickWork>,
    wakeup: mpsc::Sender<CoreCommand>,
    lane_wake: Arc<LaneWake>,
    genesis_time: u64,
    seconds_per_slot: u64,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("chain-slot-tick".into())
        .spawn(move || {
            loop {
                let wait = duration_until_next_slot_boundary(
                    std::time::SystemTime::now(),
                    genesis_time,
                    seconds_per_slot,
                );
                thread::sleep(wait);
                if tick_tx.blocking_send(TickWork::SlotTick).is_err() {
                    break;
                }
                lane_wake.notify();
                // Wake an idle mixed `try_recv`. Full mixed channel: drop — the
                // real tick is already on the never-shed lane.
                if wakeup.try_send(CoreCommand::SlotTick).is_err() && wakeup.is_closed() {
                    break;
                }
            }
        })
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "failed to spawn chain-slot-tick thread");
            std::process::abort();
        })
}

/// First-match-wins across tick → import → query_p0 → mixed ([ARCH] §3.2).
fn try_recv_first_match(
    tick_rx: &mut mpsc::Receiver<TickWork>,
    import_rx: &mut mpsc::Receiver<ImportWork>,
    query_p0_rx: &mut mpsc::Receiver<QueryP0Work>,
    cmd_rx: &mut mpsc::Receiver<CoreCommand>,
) -> Option<Incoming> {
    if let Ok(work) = tick_rx.try_recv() {
        return Some(Incoming::Tick(work));
    }
    if let Ok(work) = import_rx.try_recv() {
        return Some(Incoming::Import(work));
    }
    if let Ok(work) = query_p0_rx.try_recv() {
        return Some(Incoming::QueryP0(work));
    }
    match cmd_rx.try_recv() {
        Ok(cmd) => Some(Incoming::Mixed(cmd)),
        Err(_) => None,
    }
}

fn pop_tick_work(
    manager: &mut Manager<ChainLane, TickWork>,
    tick_rx: &mut mpsc::Receiver<TickWork>,
) -> Option<TickWork> {
    if let Some(sel) = manager.select() {
        manager.note_idle();
        return Some(sel.item);
    }
    match tick_rx.try_recv() {
        Ok(work) => match manager.push(ChainLane::Tick, work) {
            Enqueue::Accepted => {
                let sel = manager.select()?;
                manager.note_idle();
                Some(sel.item)
            }
            Enqueue::WouldShed(work)
            | Enqueue::DroppedNew(work)
            | Enqueue::EvictedOldest(work)
            | Enqueue::UnknownLane(work) => Some(work),
        },
        Err(_) => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_tick_lane<P: Preset>(
    manager: &mut Manager<ChainLane, TickWork>,
    tick_rx: &mut mpsc::Receiver<TickWork>,
    store: &mut Store<P>,
    pending_da: &mut PendingDa,
    pending_engine: &mut PendingEngine,
    da_timeout_slots: u64,
    engine_timeout_slots: u64,
    metrics: &ChainMetrics,
) -> Option<oneshot::Sender<()>> {
    while let Some(work) = pop_tick_work(manager, tick_rx) {
        match work {
            TickWork::SlotTick => {
                // Lane clock-only so a saturated mixed channel cannot stall
                // `store.time` behind an engine dial. fcU / engine redrive stay
                // on the command-path `SlotTick` wakeup until S0-A-17.
                apply_tick_clock(
                    store,
                    pending_da,
                    pending_engine,
                    da_timeout_slots,
                    engine_timeout_slots,
                    metrics,
                );
            }
            TickWork::Shutdown { done } => return Some(done),
        }
    }
    None
}

fn apply_tick_clock<P: Preset>(
    store: &mut Store<P>,
    pending_da: &mut PendingDa,
    pending_engine: &mut PendingEngine,
    da_timeout_slots: u64,
    engine_timeout_slots: u64,
    metrics: &ChainMetrics,
) {
    let now = unix_now_secs();
    if let Err(e) = on_tick(store, now) {
        tracing::debug!(error = %e, now, "on_tick on SlotTick skipped");
    }
    let current_slot = store.get_current_slot().as_u64();
    expire_pending_da(pending_da, current_slot, da_timeout_slots, metrics);
    expire_pending_engine(pending_engine, current_slot, engine_timeout_slots, metrics);
}

#[allow(clippy::too_many_arguments)]
fn handle_slot_tick<P: Preset>(
    store: &mut Store<P>,
    pending_da: &mut PendingDa,
    pending_engine: &mut PendingEngine,
    da_timeout_slots: u64,
    engine_timeout_slots: u64,
    metrics: &ChainMetrics,
    poll_handle: Option<&tokio::runtime::Handle>,
    engine_uri: &str,
    last_engine_online: &mut bool,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head: &HeadSnapshotStore,
    event_tx: &mpsc::Sender<crate::events::EventInput>,
    counters: &Arc<ImportCounters>,
    snapshot_sequence: &mut u64,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
    epoch: &EpochContextStore,
    fcu: Option<&FcuDriver<GrpcFcuSink>>,
    verify: BlockSignatureStrategy,
) {
    apply_tick_clock(
        store,
        pending_da,
        pending_engine,
        da_timeout_slots,
        engine_timeout_slots,
        metrics,
    );

    if let Some(h) = poll_handle {
        let online = poll_engine_online(h, engine_uri);
        if online && !*last_engine_online && !pending_engine.is_empty() {
            tracing::info!(
                n = pending_engine.len(),
                "engine online; re-driving pending_engine"
            );
            redrive_pending_engine(
                store,
                residency,
                config,
                head,
                event_tx,
                metrics,
                counters,
                snapshot_sequence,
                pending_da,
                pending_engine,
                verify,
                epoch_sequence,
                last_published_epoch,
                epoch,
            );
            emit_fcu_head(store, fcu);
        }
        *last_engine_online = online;
    }

    if let Some(driver) = fcu {
        let slot = store.get_current_slot();
        if let Err(e) = driver.on_slot(slot) {
            tracing::warn!(error = %e, slot = slot.as_u64(), "fcU per-slot floor failed");
        }
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
    gossip_clock: Option<GossipClock>,
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
        gossip_clock,
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
            None,
        );
        metrics.set_da_pending_occupancy(pending_da.len() as u64);
        metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
        if outcome.is_ok() {
            maybe_publish_epoch_context(store, config, epoch, epoch_sequence, last_published_epoch);
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
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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
    use cc_proto::chain::ImportBlockRequest;
    use cc_scheduler::{IMPORT_LANE_DEPTH, QUERY_P0_LANE_DEPTH, TICK_LANE_DEPTH};
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState};
    use prometheus_client::registry::Registry;
    use tokio::sync::{mpsc, oneshot};
    use tree_hash::TreeHash;

    use crate::events::{EventsConfig, EventsHandle};
    use crate::tick::unix_now_secs;

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
            churn_limit_quotient: 32,
            min_per_epoch_churn_limit_electra: 64_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
            shard_committee_period: Epoch::new(64),
            max_blobs_per_block_electra: 9,
        }
    }

    #[test]
    fn core_config_default_verify_is_verify_individual() {
        assert_eq!(
            CoreConfig::default().verify,
            BlockSignatureStrategy::VerifyIndividual
        );
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
            ring_bytes: usize::MAX,
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
            | QueryReply::IsOptimistic { .. }
            | QueryReply::CanonicalRoots { .. }
            | QueryReply::StoreClock { .. } => {
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

    async fn store_clock(handle: &CoreHandle) -> (u64, u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match handle.query(QueryRequest::StoreClock).await {
                Ok(QueryReply::StoreClock { time, slot }) => return (time, slot),
                Ok(other) => panic!("expected StoreClock, got {other:?}"),
                Err(e) if e.code() == tonic::Code::ResourceExhausted => {
                    if std::time::Instant::now() >= deadline {
                        panic!("StoreClock still backpressured: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("StoreClock query failed: {e}"),
            }
        }
    }

    /// [ARCH] §2.2 policy-D conformance: ticks are not silently dropped when
    /// the mixed 64-deep command channel is full.
    #[tokio::test]
    async fn slot_tick_is_never_shed() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let tx = core.handle.command_sender();
        let mut held = Vec::new();
        for _ in 0..COMMAND_CHANNEL_CAPACITY {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            match tx.try_send(CoreCommand::Query {
                request: QueryRequest::Head,
                reply,
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(e) => panic!("unexpected send error: {e}"),
            }
        }
        assert!(
            matches!(
                tx.try_send(CoreCommand::SlotTick),
                Err(mpsc::error::TrySendError::Full(_))
            ),
            "mixed channel must be full so the old try_send path would shed"
        );

        for i in 0..TICK_LANE_DEPTH {
            assert!(
                core.handle.try_send_slot_tick(),
                "tick {i} shed while the command channel was full"
            );
        }
        // Lane at capacity refuses rather than silently dropping.
        assert!(
            !core.handle.try_send_slot_tick(),
            "full never-shed lane must refuse, not grow"
        );

        drop(held);
        let _ = blocker.await;

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut time = 0;
        while std::time::Instant::now() < deadline {
            time = store_clock(&core.handle).await.0;
            if time > 1_000_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            time > 1_000_000,
            "never-shed ticks must advance store.time; got {time}"
        );

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    /// Saturating the leftover mixed command channel still advances
    /// `store.time` within one slot.
    #[tokio::test]
    async fn slot_tick_advances_store_time_when_other_work_saturated() {
        let (store, _anchor, config) = seeded_store();
        let slot_secs = config.seconds_per_slot.max(1);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let before = store_clock(&core.handle).await.0;

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(30)).await;

        let tx = core.handle.command_sender();
        let mut held = Vec::new();
        for _ in 0..COMMAND_CHANNEL_CAPACITY {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            match tx.try_send(CoreCommand::Query {
                request: QueryRequest::Head,
                reply,
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(e) => panic!("unexpected send error: {e}"),
            }
        }
        assert!(
            core.handle.try_send_slot_tick(),
            "tick must be accepted while every other lane/channel is saturated"
        );
        let sent_at = std::time::Instant::now();

        drop(held);
        let _ = blocker.await;

        let deadline = sent_at + Duration::from_secs(slot_secs);
        let mut time = before;
        while std::time::Instant::now() < deadline {
            time = store_clock(&core.handle).await.0;
            if time > before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            time > before,
            "store.time {time} did not advance past {before} within one slot"
        );
        assert!(
            sent_at.elapsed() < Duration::from_secs(slot_secs),
            "store.time advanced but not within one slot ({:?})",
            sent_at.elapsed()
        );

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    /// A block arriving early in its slot is not IGNOREd as `future_slot`.
    #[tokio::test]
    async fn block_gossiped_early_in_slot_is_not_ignored_as_future_slot() {
        let now = unix_now_secs();
        let mut config = minimal_config();
        let sps = config.seconds_per_slot.max(1);
        // One second into slot 1; store seeds at genesis (slot 0).
        let genesis = now.saturating_sub(sps.saturating_add(1));
        config.seconds_per_slot = sps;

        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(genesis);
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
            sps,
        )
        .unwrap();
        assert_eq!(store.get_current_slot().as_u64(), 0);
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig {
                slot_tick_enabled: true,
                ..CoreConfig::default()
            },
        );

        let block = cc_types::SignedBeaconBlock::<Minimal> {
            message: BeaconBlock {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
        let req = ImportBlockRequest {
            ssz: crate::import::encode_signed_block(&block),
            fork: 0,
            root: true_root.as_slice().to_vec(),
            source: 0,
        };
        let resp = core.handle.import_block(req).await.unwrap();
        assert_ne!(
            resp.reason, "future_slot",
            "block early in its slot must not be IGNOREd; verdict={} reason={}",
            resp.verdict, resp.reason
        );

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[test]
    fn query_p0_variants_are_head_probes() {
        assert!(QueryRequest::Head.is_query_p0());
        assert!(QueryRequest::IsOptimistic { root: None }.is_query_p0());
        assert!(QueryRequest::StoreClock.is_query_p0());
        assert!(!QueryRequest::CommitteeShuffling { epoch: 0 }.is_query_p0());
        assert!(!QueryRequest::ValidatorPubkeys { indices: vec![] }.is_query_p0());
        assert!(!QueryRequest::ValidatorRecords { indices: vec![] }.is_query_p0());
        assert!(
            !QueryRequest::CanonicalRoots {
                start_slot: 0,
                end_slot: 0
            }
            .is_query_p0()
        );
    }

    #[tokio::test]
    async fn query_p0_head_is_served_before_mixed_p1() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mixed = core.handle.command_sender();
        let mut p1_rxs = Vec::new();
        for _ in 0..3 {
            let (reply, rx) = oneshot::channel();
            p1_rxs.push(rx);
            mixed
                .try_send(CoreCommand::Query {
                    request: QueryRequest::CommitteeShuffling { epoch: 0 },
                    reply,
                })
                .expect("mixed channel must accept query_p1");
        }

        let (head_reply, head_rx) = oneshot::channel();
        core.handle
            .query_p0_sender()
            .try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply: head_reply,
            })
            .expect("query_p0 must accept Head while mixed holds p1");

        let (order_tx, mut order_rx) = mpsc::channel(4);
        let order_head = order_tx.clone();
        tokio::spawn(async move {
            let _ = head_rx.await;
            let _ = order_head.send("head").await;
        });
        let p1_first = p1_rxs.remove(0);
        tokio::spawn(async move {
            let _ = p1_first.await;
            let _ = order_tx.send("p1").await;
        });

        let _ = blocker.await;
        let first = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("lane reply timed out")
            .expect("order channel closed");
        assert_eq!(
            first, "head",
            "query_p0 must not wait behind mixed query_p1"
        );

        drop(p1_rxs);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn get_head_behind_three_imports_waits_for_import_lane() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let import = core.handle.import_sender();
        let mut import_rxs = Vec::new();
        for _ in 0..3 {
            let (reply, rx) = oneshot::channel();
            import_rxs.push(rx);
            import
                .try_send(ImportWork::ImportBlock {
                    request: ImportBlockRequest {
                        ssz: vec![],
                        fork: 0,
                        root: vec![0; 32],
                        source: 0,
                    },
                    reply,
                })
                .expect("import lane must accept");
        }

        let (head_reply, head_rx) = oneshot::channel();
        core.handle
            .query_p0_sender()
            .try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply: head_reply,
            })
            .expect("query_p0 must accept Head behind imports");

        let (order_tx, mut order_rx) = mpsc::channel(4);
        let order_import = order_tx.clone();
        let first_import = import_rxs.remove(0);
        tokio::spawn(async move {
            let _ = first_import.await;
            let _ = order_import.send("import").await;
        });
        tokio::spawn(async move {
            let _ = head_rx.await;
            let _ = order_tx.send("head").await;
        });

        let _ = blocker.await;
        let first = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("lane reply timed out")
            .expect("order channel closed");
        assert_eq!(
            first, "import",
            "import outranks query_p0; GetHead still waits for queued imports"
        );

        drop(import_rxs);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn data_available_shares_import_lane_capacity() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(300)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let import = core.handle.import_sender();
        let mut held = Vec::new();
        for _ in 0..(IMPORT_LANE_DEPTH - 1) {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            import
                .try_send(ImportWork::ImportBlock {
                    request: ImportBlockRequest::default(),
                    reply,
                })
                .expect("import lane has room");
        }
        import
            .try_send(ImportWork::DataAvailable {
                root: Root::ZERO,
                slot: 0,
            })
            .expect("DataAvailable takes the last import slot");
        assert!(
            matches!(
                import.try_send(ImportWork::DataAvailable {
                    root: Root::ZERO,
                    slot: 1,
                }),
                Err(mpsc::error::TrySendError::Full(_))
            ),
            "DataAvailable must share the import FIFO, not sit above it"
        );
        assert_eq!(import.max_capacity(), IMPORT_LANE_DEPTH);

        let (reply, _rx) = oneshot::channel();
        core.handle
            .query_p0_sender()
            .try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply,
            })
            .expect("query_p0 stays independent of a full import lane");
        assert_eq!(
            core.handle.query_p0_sender().max_capacity(),
            QUERY_P0_LANE_DEPTH
        );

        drop(held);
        let _ = blocker.await;
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_is_served_while_import_lane_is_full() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let import = core.handle.import_sender();
        let mut held = Vec::new();
        for _ in 0..IMPORT_LANE_DEPTH {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            import
                .try_send(ImportWork::ImportBlock {
                    request: ImportBlockRequest::default(),
                    reply,
                })
                .expect("import lane has room");
        }

        let sent_at = std::time::Instant::now();
        let rx = core
            .handle
            .begin_shutdown()
            .await
            .expect("shutdown must enqueue on the tick lane");
        assert!(
            sent_at.elapsed() < Duration::from_millis(200),
            "begin_shutdown send must not wait behind a full import lane ({:?})",
            sent_at.elapsed()
        );

        drop(held);
        let _ = blocker.await;
        tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("shutdown oneshot")
            .expect("core dropped shutdown reply");
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn last_handle_drop_unparks_idle_core() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let mut core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        let join = core.join.take();
        drop(core.handle);
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || join.expect("join handle").join()),
        )
        .await
        .expect("core stayed parked after last sender drop")
        .expect("join task")
        .expect("core thread panicked");
        events.shutdown().await;
    }
}
