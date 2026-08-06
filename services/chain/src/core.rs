//! Dedicated OS core thread owning fork-choice [`Store`] by value (ADR-P1-09).
//!
//! Communication: `tokio::sync::mpsc` (capacity 64) with `blocking_recv` on the
//! thread side and `oneshot` replies. `ImportBlock` uses `send_timeout(2 s)` →
//! `RESOURCE_EXHAUSTED` on backpressure.
//!
//! ```text
//! loop { recv(); handle(); /* snapshot + events inside import */ }
//! ```

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use cc_fork_choice::Store;
use cc_proto::chain::{ImportBlockRequest, ImportBlockResponse};
use cc_state_transition::BlockSignatureStrategy;
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::import::{ImportCounters, import_block};
use crate::metrics::ChainMetrics;
use crate::residency::{DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency};

/// Command channel capacity (Architecture §7.2).
pub const COMMAND_CHANNEL_CAPACITY: usize = 64;

/// `ImportBlock` send timeout before `RESOURCE_EXHAUSTED` (Architecture §7.2).
pub const IMPORT_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Shutdown join timeout (Architecture §7.4).
pub const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Commands handled by the core thread.
#[derive(Debug)]
pub enum CoreCommand {
    /// Full import path.
    ImportBlock {
        request: ImportBlockRequest,
        reply: oneshot::Sender<Result<ImportBlockResponse, Status>>,
    },
    /// State-requiring read placeholder (Phase 1 builds the command; priority
    /// lane is Phase 6). Currently returns head root from the store.
    Query {
        reply: oneshot::Sender<Result<QueryReply, Status>>,
    },
    /// Block the core thread for `duration` (tests: GetHead bypass).
    BlockFor {
        duration: Duration,
        reply: oneshot::Sender<()>,
    },
    /// Graceful shutdown.
    Shutdown { done: oneshot::Sender<()> },
}

/// Reply for the Phase-1 `Query` command.
#[derive(Debug, Clone)]
pub struct QueryReply {
    pub head_root: Root,
    pub head_slot: u64,
}

/// Configuration for spawning the core thread.
#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub max_resident_states: usize,
    pub body_ring_capacity: usize,
    pub verify: BlockSignatureStrategy,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            max_resident_states: DEFAULT_MAX_RESIDENT_STATES,
            body_ring_capacity: DEFAULT_BODY_RING_CAPACITY,
            verify: BlockSignatureStrategy::NoVerification,
        }
    }
}

/// Handle to a running core thread (non-generic: channel + shared state only).
#[derive(Debug, Clone)]
pub struct CoreHandle {
    cmd_tx: mpsc::Sender<CoreCommand>,
    head: HeadSnapshotStore,
    metrics: ChainMetrics,
    counters: Arc<ImportCounters>,
}

impl CoreHandle {
    /// Shared head snapshot (also used by `GetHead`).
    pub fn head(&self) -> &HeadSnapshotStore {
        &self.head
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

    /// `Query` command (single FIFO queue in Phase 1).
    pub async fn query(&self) -> Result<QueryReply, Status> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(CoreCommand::Query { reply })
            .await
            .map_err(|_| Status::unavailable("chain core thread is shut down"))?;
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

    /// Ask the core thread to exit and wait up to [`SHUTDOWN_JOIN_TIMEOUT`].
    pub async fn shutdown(&self) {
        let (done, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(CoreCommand::Shutdown { done })
            .await
            .is_ok()
        {
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
    /// Join the OS thread (after [`CoreHandle::shutdown`]).
    pub fn join(mut self) {
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// Spawn the dedicated OS core thread owning `store` **by value**.
///
/// This is a dedicated OS thread (ADR-P1-09) — not a tokio task or blocking pool worker.
pub fn spawn_core_thread<P: Preset + 'static>(
    store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> CoreThread {
    let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let counters = Arc::new(ImportCounters::default());
    let counters_thread = Arc::clone(&counters);
    let head_thread = head.clone();
    let metrics_thread = metrics.clone();

    // Publish an initial snapshot from the seeded store so GetHead works
    // immediately after spawn (tests / post-bootstrap).
    publish_initial_snapshot(&store, &head);

    let join = thread::Builder::new()
        .name("chain-core".into())
        .spawn(move || {
            core_loop(
                store,
                config,
                head_thread,
                event_tx,
                metrics_thread,
                counters_thread,
                core_cfg,
                cmd_rx,
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
            metrics,
            counters,
        },
        join: Some(join),
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
        is_optimistic: false,
        sequence: 0,
    });
}

#[allow(clippy::too_many_arguments)]
fn core_loop<P: Preset>(
    mut store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    counters: Arc<ImportCounters>,
    core_cfg: CoreConfig,
    mut cmd_rx: mpsc::Receiver<CoreCommand>,
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
    let verify = core_cfg.verify;

    while let Some(cmd) = cmd_rx.blocking_recv() {
        metrics.set_import_queue_depth(cmd_rx.len() as u64);
        match cmd {
            CoreCommand::ImportBlock { request, reply } => {
                let outcome = import_block(
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
                );
                let _ = reply.send(outcome.map(|o| o.response));
            }
            CoreCommand::Query { reply } => {
                let head_root = store
                    .last_head_root()
                    .or_else(|| store.head_cache().map(|c| c.head_root))
                    .unwrap_or_else(|| store.justified_checkpoint().root);
                let head_slot = store
                    .blocks()
                    .get(&head_root)
                    .map(|h| h.slot.as_u64())
                    .unwrap_or(0);
                let _ = reply.send(Ok(QueryReply {
                    head_root,
                    head_slot,
                }));
            }
            CoreCommand::BlockFor { duration, reply } => {
                thread::sleep(duration);
                let _ = reply.send(());
            }
            CoreCommand::Shutdown { done } => {
                let _ = done.send(());
                break;
            }
        }
        metrics.set_import_queue_depth(cmd_rx.len() as u64);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::sync::Arc;

    use cc_fork_choice::{AlwaysAvailable, get_forkchoice_store};
    use cc_state_transition::StubOptimisticEngine;
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
            Arc::new(StubOptimisticEngine),
            Arc::new(AlwaysAvailable),
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
        let q = core.handle.query().await.unwrap();
        assert_eq!(q.head_root, anchor);
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
