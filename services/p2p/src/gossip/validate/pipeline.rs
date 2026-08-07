//! Validation pool: topic dispatch, report via swarm cmd.
//!
//! **CC-22/4**: every registered Fulu topic family has a non-default validator
//! wired here. Operation / attestation families are IGNORE stubs (CC-2B /
//! CC-2C replace them). **CC-2D** replaces the sync-committee IGNORE stubs.
//! wired here. Operation topics are CC-2B real validators; attestation / sync
//! families remain IGNORE stubs (CC-2C / CC-2D).
//!
//! ## Ownership
//!
//! One [`ValidationPoolState`] owns a single [`ColumnValidatorState`] (seen +
//! pending + inclusion cache) plus [`SyncSeenSets`]. Block and column paths
//! share the column state — no dual sync.
//! pending + inclusion cache) and the CC-2B [`OperationValidatorState`]. Block
//! and column paths share the column state — no dual sync.
//!
//! ## Pending redrive
//!
//! After a block ACCEPT (parent known) or when `ChainView` gains a lookahead,
//! parked items are re-validated **locally**. Gossip already reported IGNORE
//! for those message ids; redrive never re-reports them.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_libp2p::PeerId;
use cc_proto::p2p::{Acceptance, ChainView, Reason};
use cc_types::config::ChainConfig;
use cc_types::preset::Mainnet;
use lru::LruCache;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use super::block::{
    note_accepted_block, validate_beacon_block_local, BlockOutcome, BlockValidateInput,
};
use super::column::{
    production_kzg_verify, validate_data_column_sidecar, ColumnOutcome, ColumnValidateInput,
    ColumnValidatorState, KzgVerify, NoopSamplingFeed, SamplingFeed,
};
use super::sync::{
    validate_sync_committee_message, validate_sync_contribution_and_proof, NoopSyncSource,
    SyncCommitteeSource, SyncContribValidateInput, SyncMessageValidateInput, SyncOutcome,
    SyncSeenSets,
use super::operations::{
    epoch_from_view, validate_operation, OperationValidateInput, OperationValidatorState,
};
use crate::chain_stream::records::{
    MapValidatorRecordSource, RpcValidatorRecordSource, ValidatorRecordCache,
    ValidatorRecordSource,
};
use crate::channels::{
    ChainInbound, ChainOutbound, GossipWork, PeerPenaltyCmd, SwarmCommand, VerdictResolution,
    GOSSIP_BOUND,
};
use crate::clock::SlotClock;
use crate::gossip::topics::TopicName;
use crate::metrics::{P2pMetrics, PeerPenaltyReason, QueueName};
use crate::verdict::{is_late_import_reject, Verdict};

/// Max tracked ACCEPTed correlation ids for late-import penalties (M2).
pub const REPORTED_ACCEPT_BOUND: usize = 1_024;

/// Registry of topic validators (non-default for every Fulu family).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidatorKind {
    /// Chain-authoritative beacon block.
    BeaconBlock,
    /// P2p-authoritative column sidecar.
    DataColumnSidecar,
    /// P2p-authoritative sync committee message / contribution (CC-2D).
    SyncCommittee,
    /// IGNORE stub (CC-2B/C).
    /// P2p-authoritative operation topics (CC-2B).
    Operation,
    /// IGNORE stub (CC-2C / CC-2D).
    StubIgnore,
}

/// Map a topic name to its validator kind.
#[must_use]
pub fn validator_kind(name: TopicName) -> ValidatorKind {
    match name {
        TopicName::BeaconBlock => ValidatorKind::BeaconBlock,
        TopicName::DataColumnSidecar(_) => ValidatorKind::DataColumnSidecar,
        TopicName::SyncCommitteeContributionAndProof | TopicName::SyncCommittee(_) => {
            ValidatorKind::SyncCommittee
        }
        TopicName::BeaconAggregateAndProof
        | TopicName::BeaconAttestation(_)
        | TopicName::VoluntaryExit
        | TopicName::ProposerSlashing
        | TopicName::AttesterSlashing
        | TopicName::BlsToExecutionChange => ValidatorKind::StubIgnore,
        TopicName::VoluntaryExit
        | TopicName::ProposerSlashing
        | TopicName::AttesterSlashing
        | TopicName::BlsToExecutionChange => ValidatorKind::Operation,
        TopicName::BeaconAggregateAndProof
        | TopicName::BeaconAttestation(_)
        | TopicName::SyncCommitteeContributionAndProof
        | TopicName::SyncCommittee(_) => ValidatorKind::StubIgnore,
    }
}

/// Every Fulu topic name expanded with `counts` has a non-default validator.
#[must_use]
pub fn all_topics_have_validators(counts: &crate::gossip::topics::SubnetCounts) -> bool {
    crate::gossip::topics::expand_fulu_topic_names(counts)
        .into_iter()
        .all(|n| {
            matches!(
                validator_kind(n),
                ValidatorKind::BeaconBlock
                    | ValidatorKind::DataColumnSidecar
                    | ValidatorKind::SyncCommittee
                    | ValidatorKind::Operation
                    | ValidatorKind::StubIgnore
            )
        })
}

/// Parse `/eth2/{digest_hex}/{name}/ssz_snappy` → topic name.
#[must_use]
pub fn parse_topic_name(topic: &str) -> Option<TopicName> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() < 5 || parts[1] != "eth2" || parts[4] != "ssz_snappy" {
        return None;
    }
    parse_path_segment(parts[3])
}

fn parse_path_segment(seg: &str) -> Option<TopicName> {
    if seg == "beacon_block" {
        return Some(TopicName::BeaconBlock);
    }
    if seg == "beacon_aggregate_and_proof" {
        return Some(TopicName::BeaconAggregateAndProof);
    }
    if seg == "sync_committee_contribution_and_proof" {
        return Some(TopicName::SyncCommitteeContributionAndProof);
    }
    if seg == "voluntary_exit" {
        return Some(TopicName::VoluntaryExit);
    }
    if seg == "proposer_slashing" {
        return Some(TopicName::ProposerSlashing);
    }
    if seg == "attester_slashing" {
        return Some(TopicName::AttesterSlashing);
    }
    if seg == "bls_to_execution_change" {
        return Some(TopicName::BlsToExecutionChange);
    }
    if let Some(rest) = seg.strip_prefix("beacon_attestation_") {
        let id = rest.parse().ok()?;
        return Some(TopicName::BeaconAttestation(id));
    }
    if let Some(rest) = seg.strip_prefix("data_column_sidecar_") {
        let id = rest.parse().ok()?;
        return Some(TopicName::DataColumnSidecar(id));
    }
    if let Some(rest) = seg.strip_prefix("sync_committee_") {
        let id = rest.parse().ok()?;
        return Some(TopicName::SyncCommittee(id));
    }
    None
}

/// Previously reported ACCEPT (for late import_invalid; no re-report).
#[derive(Debug, Clone)]
pub struct ReportedEntry {
    /// Gossip verdict we already reported.
    pub verdict: Verdict,
    /// Propagation source (app-score target).
    pub peer_id: PeerId,
}

/// Shared mutable state — **single** seen/pending owner.
#[derive(Debug)]
pub struct ValidationPoolState {
    /// Column + block seen/pending/inclusion (one owner).
    pub column: ColumnValidatorState,
    /// Sync-committee message / contribution seen sets (CC-2D).
    pub sync: SyncSeenSets,
    /// Operation-topic anti-replay sets (CC-2B). `Arc` so the operation
    /// validator can hold a shared ref across record-fetch `.await` without
    /// parking the outer pool mutex.
    pub operations: Arc<OperationValidatorState>,
    /// Last `finalized_epoch` at which operation index sets were cleared.
    pub ops_cleared_at_finalized_epoch: u64,
    /// Bounded map of ACCEPTed correlation ids → entry (historical / late path).
    pub reported: LruCache<Vec<u8>, ReportedEntry>,
    /// ACCEPT entries **pinned** until late import resolves (CC-27c H2).
    ///
    /// Not subject to LRU eviction: a late Reject after early ACCEPT must not
    /// lose the peer id to a full `reported` cache under load.
    pub late_open: std::collections::HashMap<Vec<u8>, ReportedEntry>,
}

impl ValidationPoolState {
    /// Fresh pool state.
    #[must_use]
    pub fn new() -> Self {
        let cap = NonZeroUsize::new(REPORTED_ACCEPT_BOUND).unwrap_or(NonZeroUsize::MIN);
        Self {
            column: ColumnValidatorState::new(),
            sync: SyncSeenSets::new(),
            operations: Arc::new(OperationValidatorState::new()),
            ops_cleared_at_finalized_epoch: 0,
            reported: LruCache::new(cap),
            late_open: std::collections::HashMap::new(),
        }
    }

    /// Clear operation index sets when `finalized_epoch` advances (CC-2B).
    pub fn maybe_clear_operations_at_finalization(&mut self, finalized_epoch: u64) {
        if finalized_epoch > self.ops_cleared_at_finalized_epoch {
            self.operations.clear_at_finalization();
            self.ops_cleared_at_finalized_epoch = finalized_epoch;
        }
    }

    /// Record an ACCEPT for late-import tracking (pinned + LRU).
    pub fn note_reported_accept(&mut self, corr: Vec<u8>, entry: ReportedEntry) {
        self.late_open.insert(corr.clone(), entry.clone());
        self.reported.put(corr, entry);
        // Bound late_open growth: if over 2× reported bound, drop oldest by not
        // tracking further (should not happen at 1 msg/slot; defensive).
        if self.late_open.len() > REPORTED_ACCEPT_BOUND.saturating_mul(2) {
            // Drop an arbitrary entry that is also still in late_open only —
            // prefer entries already present in reported LRU tail is complex;
            // clear half by draining some keys.
            let excess = self.late_open.len() - REPORTED_ACCEPT_BOUND;
            let drop_keys: Vec<_> = self.late_open.keys().take(excess).cloned().collect();
            for k in drop_keys {
                self.late_open.remove(&k);
            }
        }
    }

    /// Export occupancy gauges (pending + seen + operation index sets).
    pub fn export_occupancy(&self, metrics: &P2pMetrics) {
        let (ps, pb) = self.column.pending.occupancy();
        metrics.set_queue_depth(QueueName::PendingSidecar, ps as i64);
        metrics.set_queue_depth(QueueName::PendingBlock, pb as i64);
        let (cs, bs) = self.column.seen.occupancy();
        // Seen-set entry counts — **not** `cc_p2p_cache_*` (those are backfill
        // bytes only, CC-26a / OQ-P2-4). Bounds are the compile-time constants
        // `COLUMN_SEEN_BOUND` / `BLOCK_SEEN_BOUND` / `SYNC_SEEN_BOUND`.
        metrics.set_queue_depth(QueueName::SeenColumn, cs as i64);
        metrics.set_queue_depth(QueueName::SeenBlock, bs as i64);
        metrics.set_queue_depth(QueueName::SeenSync, self.sync.occupancy() as i64);
        let op = self.operations.occupancy();
        metrics.set_queue_depth(QueueName::SeenVoluntaryExit, op.voluntary_exit as i64);
        metrics.set_queue_depth(QueueName::SeenProposerSlashing, op.proposer_slashing as i64);
        metrics.set_queue_depth(QueueName::SeenAttesterSlashing, op.attester_slashing as i64);
        metrics.set_queue_depth(
            QueueName::SeenBlsToExecutionChange,
            op.bls_to_execution_change as i64,
        );
    }
}

impl Default for ValidationPoolState {
    fn default() -> Self {
        Self::new()
    }
}

/// Dependencies for the gossip validation worker.
#[allow(missing_debug_implementations)]
pub struct ValidationPool {
    /// Shared state (single owner).
    pub state: Arc<Mutex<ValidationPoolState>>,
    /// Chain config.
    pub config: Arc<ChainConfig>,
    /// Slot clock.
    pub clock: SlotClock,
    /// Chain view loader.
    pub view: Arc<dyn Fn() -> ChainView + Send + Sync>,
    /// Chain outbound.
    pub chain_out_tx: mpsc::Sender<ChainOutbound>,
    /// Swarm commands (report path).
    pub cmd_tx: mpsc::Sender<SwarmCommand>,
    /// Peer penalty path → peer manager.
    pub penalty_tx: mpsc::Sender<PeerPenaltyCmd>,
    /// Metrics.
    pub metrics: P2pMetrics,
    /// KZG seam (production: real or fail-closed).
    pub kzg: Arc<dyn KzgVerify>,
    /// Sampling seam.
    pub sampling: Arc<dyn SamplingFeed>,
    /// Sync-committee pubkey / membership source (CC-1F cache; Phase 2 default
    /// is [`NoopSyncSource`] until the query client is wired).
    pub sync_source: Arc<dyn SyncCommitteeSource>,
    /// Gossip channel bound.
    pub in_flight_cap: usize,
    /// Validator-record LRU (CC-2B / §5.4a).
    pub record_cache: Arc<ValidatorRecordCache>,
    /// Unary `GetValidatorRecords` source.
    pub record_source: Arc<dyn ValidatorRecordSource>,
}

impl ValidationPool {
    /// Production pool: real/fail-closed KZG, noop sampling, noop sync source.
    /// Production pool: real/fail-closed KZG, noop sampling, RPC record source.
    #[must_use]
    pub fn new(
        config: Arc<ChainConfig>,
        clock: SlotClock,
        view: Arc<dyn Fn() -> ChainView + Send + Sync>,
        chain_out_tx: mpsc::Sender<ChainOutbound>,
        cmd_tx: mpsc::Sender<SwarmCommand>,
        penalty_tx: mpsc::Sender<PeerPenaltyCmd>,
        metrics: P2pMetrics,
    ) -> Self {
        Self::with_record_source(
            config,
            clock,
            view,
            chain_out_tx,
            cmd_tx,
            penalty_tx,
            metrics,
            Arc::new(MapValidatorRecordSource::new()),
        )
    }

    /// Production pool wired to a live chain URI for `GetValidatorRecords`.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn with_chain_uri(
        config: Arc<ChainConfig>,
        clock: SlotClock,
        view: Arc<dyn Fn() -> ChainView + Send + Sync>,
        chain_out_tx: mpsc::Sender<ChainOutbound>,
        cmd_tx: mpsc::Sender<SwarmCommand>,
        penalty_tx: mpsc::Sender<PeerPenaltyCmd>,
        metrics: P2pMetrics,
        chain_uri: String,
    ) -> Self {
        let source: Arc<dyn ValidatorRecordSource> = if chain_uri.is_empty() {
            Arc::new(MapValidatorRecordSource::new())
        } else {
            Arc::new(RpcValidatorRecordSource { chain_uri })
        };
        Self::with_record_source(
            config,
            clock,
            view,
            chain_out_tx,
            cmd_tx,
            penalty_tx,
            metrics,
            source,
        )
    }

    /// Full constructor with an injected record source (tests).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn with_record_source(
        config: Arc<ChainConfig>,
        clock: SlotClock,
        view: Arc<dyn Fn() -> ChainView + Send + Sync>,
        chain_out_tx: mpsc::Sender<ChainOutbound>,
        cmd_tx: mpsc::Sender<SwarmCommand>,
        penalty_tx: mpsc::Sender<PeerPenaltyCmd>,
        metrics: P2pMetrics,
        record_source: Arc<dyn ValidatorRecordSource>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ValidationPoolState::new())),
            config,
            clock,
            view,
            chain_out_tx,
            cmd_tx,
            penalty_tx,
            metrics,
            kzg: production_kzg_verify(),
            sampling: Arc::new(NoopSamplingFeed),
            sync_source: Arc::new(NoopSyncSource),
            in_flight_cap: GOSSIP_BOUND,
            record_cache: Arc::new(ValidatorRecordCache::new()),
            record_source,
        }
    }
}

/// Worker loop: receive [`GossipWork`], validate, send [`SwarmCommand::ReportValidation`].
pub async fn run_validation_pool(
    pool: ValidationPool,
    mut gossip_rx: mpsc::Receiver<GossipWork>,
) {
    while let Some(work) = gossip_rx.recv().await {
        let depth = pool.metrics.queue_depth(QueueName::Gossip);
        if depth > 0 {
            pool.metrics.set_queue_depth(QueueName::Gossip, depth - 1);
        }

        // Redrive unknown-proposer parks when view has lookahead (B1).
        redrive_unknown_proposer(&pool).await;

        let verdict = validate_one(&pool, &work).await;
        report(&pool, &work, verdict).await;
    }
}

/// Drain `chain_in` for late chain opinions (B2) — no re-report.
pub async fn run_chain_in_late_verdicts(
    pool_state: Arc<Mutex<ValidationPoolState>>,
    mut chain_in_rx: mpsc::Receiver<ChainInbound>,
    penalty_tx: mpsc::Sender<PeerPenaltyCmd>,
    metrics: P2pMetrics,
) {
    while let Some(inbound) = chain_in_rx.recv().await {
        let late = Verdict::from_proto(&inbound.verdict);
        let mut guard = pool_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        apply_late_chain_verdict(&mut guard, &late, &metrics, &penalty_tx);
    }
}

async fn validate_one(pool: &ValidationPool, work: &GossipWork) -> Verdict {
    let Some(name) = parse_topic_name(&work.topic) else {
        return Verdict::ignore(Reason::Invalid, vec![]);
    };

    let view = (pool.view)();
    let current_slot = if view.slot > 0 {
        view.slot
    } else {
        pool.clock.current_slot()
    };
    let slots_per_epoch = pool.clock.slots_per_epoch();
    let disparity_slots = disparity_to_slots(
        pool.clock.maximum_gossip_clock_disparity(),
        pool.clock.seconds_per_slot(),
    );
    let finalized_slot = view.finalized_epoch.saturating_mul(slots_per_epoch);

    // Finalization prune (M1) — column/block slot-keyed + operation index sets.
    {
        let mut guard = pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.column.seen.prune_at_finalization(finalized_slot);
        guard.sync.prune_at_finalization(finalized_slot);
        // Operation sets are **cleared** when finalized_epoch advances (CC-2B).
        guard.maybe_clear_operations_at_finalization(view.finalized_epoch);
    }

    match validator_kind(name) {
        ValidatorKind::StubIgnore => Verdict::ignore(Reason::AlreadyKnown, vec![]),
        ValidatorKind::SyncCommittee => {
            let gvr = view.genesis_validators_root.clone();
            let mut guard = pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let outcome = match name {
                TopicName::SyncCommittee(subnet) => {
                    let input = SyncMessageValidateInput {
                        payload: &work.data,
                        topic_subnet: subnet,
                        current_slot,
                        disparity_slots,
                        config: &pool.config,
                        slots_per_epoch,
                        genesis_validators_root: gvr.as_slice(),
                    };
                    validate_sync_committee_message::<Mainnet>(
                        &mut guard.sync,
                        pool.sync_source.as_ref(),
                        &input,
                        None,
                    )
                }
                TopicName::SyncCommitteeContributionAndProof => {
                    let input = SyncContribValidateInput {
                        payload: &work.data,
                        current_slot,
                        disparity_slots,
                        config: &pool.config,
                        slots_per_epoch,
                        genesis_validators_root: gvr.as_slice(),
                    };
                    validate_sync_contribution_and_proof::<Mainnet>(
                        &mut guard.sync,
                        pool.sync_source.as_ref(),
                        &input,
                        None,
                    )
                }
                _ => {
                    return Verdict::internal(vec![]);
                }
            };
            guard.export_occupancy(&pool.metrics);
            match outcome {
                SyncOutcome::Done(v) => v,
                SyncOutcome::AcceptForward { verdict, object } => {
                    // Phase 5/6 seam: validated sync messages travel up the
                    // CC-27 stream; chain discards them (no pool). Fire-and-
                    // forget so the gossip ACCEPT is not blocked on chain.
                    let outbound = ChainOutbound {
                        object,
                        reply: None,
                    };
                    // Drop the state lock before await.
                    drop(guard);
                    let _ = pool.chain_out_tx.try_send(outbound);
                    verdict
                }
            }
        ValidatorKind::Operation => {
            // p2p-authoritative: never forward GossipObject to chain (CC-2B/3, /4).
            // Clone the Arc under a short lock, then await without holding it.
            let ops = {
                let guard = pool
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                Arc::clone(&guard.operations)
            };
            let current_epoch = epoch_from_view(&view, slots_per_epoch);
            let input = OperationValidateInput {
                payload: &work.data,
                view: &view,
                config: &pool.config,
                slots_per_epoch,
                current_epoch,
            };
            let verdict = validate_operation::<Mainnet>(
                ops.as_ref(),
                name,
                &input,
                pool.record_cache.as_ref(),
                pool.record_source.as_ref(),
            )
            .await;
            {
                let guard = pool
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.export_occupancy(&pool.metrics);
            }
            verdict
        }
        ValidatorKind::DataColumnSidecar => {
            let TopicName::DataColumnSidecar(subnet) = name else {
                return Verdict::internal(vec![]);
            };
            let peer_bytes = work.peer_id.to_bytes();
            let mut guard = pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let input = ColumnValidateInput {
                payload: &work.data,
                topic_subnet: subnet,
                current_slot,
                finalized_slot,
                disparity_slots,
                view: &view,
                config: &pool.config,
                slots_per_epoch,
                message_id: work.message_id.0.as_slice(),
                peer_id: peer_bytes.as_slice(),
                topic: &work.topic,
            };
            let out = validate_data_column_sidecar::<Mainnet>(
                &mut guard.column,
                &input,
                pool.kzg.as_ref(),
                pool.sampling.as_ref(),
                Some(&pool.metrics),
            );
            guard.export_occupancy(&pool.metrics);
            match out {
                ColumnOutcome::Done(v) | ColumnOutcome::Pending(v) => v,
            }
        }
        ValidatorKind::BeaconBlock => {
            let peer_bytes = work.peer_id.to_bytes();
            let out = {
                let mut guard = pool
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let input = BlockValidateInput {
                    payload: &work.data,
                    current_slot,
                    finalized_slot,
                    disparity_slots,
                    topic: &work.topic,
                    message_id: work.message_id.0.as_slice(),
                    peer_id: peer_bytes.as_slice(),
                };
                let mut pending = std::mem::take(&mut guard.column.pending);
                let out = validate_beacon_block_local::<Mainnet>(
                    &mut guard.column.seen,
                    &mut pending,
                    &input,
                    Some(&pool.metrics),
                );
                guard.column.pending = pending;
                guard.export_occupancy(&pool.metrics);
                out
            };
            match out {
                BlockOutcome::Done(v) | BlockOutcome::Pending(v) => v,
                BlockOutcome::Forward(fwd) => {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let outbound = ChainOutbound {
                        object: fwd.object,
                        reply: Some(reply_tx),
                    };
                    if pool.chain_out_tx.send(outbound).await.is_err() {
                        return Verdict::internal(fwd.block_root.to_vec());
                    }
                    let resolution = match tokio::time::timeout(
                        Duration::from_secs(12),
                        reply_rx,
                    )
                    .await
                    {
                        Ok(Ok(r)) => r,
                        Ok(Err(_)) => {
                            return Verdict::internal(fwd.block_root.to_vec());
                        }
                        Err(_) => {
                            return Verdict::ignore(Reason::Internal, fwd.block_root.to_vec());
                        }
                    };
                    let verdict = match resolution {
                        VerdictResolution::FromChain(proto) => Verdict::from_proto(&proto),
                        VerdictResolution::Timeout => {
                            Verdict::ignore(Reason::Internal, fwd.block_root.to_vec())
                        }
                    };
                    if matches!(verdict.acceptance, Acceptance::Accept) {
                        {
                            let mut g = pool
                                .state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            note_accepted_block(
                                &mut g.column.seen,
                                fwd.slot,
                                fwd.proposer_index,
                                fwd.block_root,
                            );
                            g.export_occupancy(&pool.metrics);
                        }
                        // B1: local redrive of parked sidecars/blocks for this parent.
                        redrive_for_parent(pool, &fwd.block_root).await;
                    }
                    verdict
                }
            }
        }
    }
}

/// Local-only redrive after a parent becomes known (B1).
///
/// Does **not** re-report gossipsub for the original message ids.
async fn redrive_for_parent(pool: &ValidationPool, parent_root: &[u8; 32]) {
    let (sidecars, blocks) = {
        let mut g = pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let s = g.column.pending.redrive_sidecars_for_parent(parent_root);
        let b = g.column.pending.redrive_blocks_for_parent(parent_root);
        g.export_occupancy(&pool.metrics);
        (s, b)
    };

    let view = (pool.view)();
    let current_slot = if view.slot > 0 {
        view.slot
    } else {
        pool.clock.current_slot()
    };
    let slots_per_epoch = pool.clock.slots_per_epoch();
    let disparity_slots = disparity_to_slots(
        pool.clock.maximum_gossip_clock_disparity(),
        pool.clock.seconds_per_slot(),
    );
    let finalized_slot = view.finalized_epoch.saturating_mul(slots_per_epoch);

    for sc in sidecars {
        let mut g = pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let input = ColumnValidateInput {
            payload: sc.ssz.as_ref(),
            topic_subnet: sc.topic_subnet,
            current_slot,
            finalized_slot,
            disparity_slots,
            view: &view,
            config: &pool.config,
            slots_per_epoch,
            message_id: sc.message_id.as_slice(),
            peer_id: sc.peer_id.as_slice(),
            topic: &sc.topic,
        };
        let out = validate_data_column_sidecar::<Mainnet>(
            &mut g.column,
            &input,
            pool.kzg.as_ref(),
            pool.sampling.as_ref(),
            Some(&pool.metrics),
        );
        g.export_occupancy(&pool.metrics);
        match out {
            ColumnOutcome::Done(v) if matches!(v.acceptance, Acceptance::Accept) => {
                debug!(
                    col = sc.column_index,
                    "redrive sidecar ACCEPT (local only; gossip already IGNORE)"
                );
            }
            ColumnOutcome::Pending(_) => {
                // Re-parked inside validate.
            }
            ColumnOutcome::Done(v) => {
                debug!(?v.reason, "redrive sidecar terminal non-accept (local)");
            }
        }
    }

    for blk in blocks {
        // Local redrive: re-run local stage; Forward → send to chain without gossip hold.
        let peer_bytes = blk.peer_id.clone();
        let out = {
            let mut g = pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let input = BlockValidateInput {
                payload: blk.ssz.as_ref(),
                current_slot,
                finalized_slot,
                disparity_slots,
                topic: &blk.topic,
                message_id: blk.message_id.as_slice(),
                peer_id: peer_bytes.as_slice(),
            };
            let mut pending = std::mem::take(&mut g.column.pending);
            let out = validate_beacon_block_local::<Mainnet>(
                &mut g.column.seen,
                &mut pending,
                &input,
                Some(&pool.metrics),
            );
            g.column.pending = pending;
            out
        };
        if let BlockOutcome::Forward(fwd) = out {
            let outbound = ChainOutbound {
                object: fwd.object,
                reply: None, // fire-and-forget for redrive; chain late path still works
            };
            let _ = pool.chain_out_tx.send(outbound).await;
        }
    }
}

async fn redrive_unknown_proposer(pool: &ValidationPool) {
    let view = (pool.view)();
    if view.proposer_lookahead.is_empty() {
        return;
    }
    let ready = {
        let mut g = pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let r = g.column.pending.redrive_sidecars_unknown_proposer();
        g.export_occupancy(&pool.metrics);
        r
    };
    if ready.is_empty() {
        return;
    }
    // Re-validate each with current view (local only).
    let current_slot = if view.slot > 0 {
        view.slot
    } else {
        pool.clock.current_slot()
    };
    let slots_per_epoch = pool.clock.slots_per_epoch();
    let disparity_slots = disparity_to_slots(
        pool.clock.maximum_gossip_clock_disparity(),
        pool.clock.seconds_per_slot(),
    );
    let finalized_slot = view.finalized_epoch.saturating_mul(slots_per_epoch);
    for sc in ready {
        let mut g = pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let input = ColumnValidateInput {
            payload: sc.ssz.as_ref(),
            topic_subnet: sc.topic_subnet,
            current_slot,
            finalized_slot,
            disparity_slots,
            view: &view,
            config: &pool.config,
            slots_per_epoch,
            message_id: sc.message_id.as_slice(),
            peer_id: sc.peer_id.as_slice(),
            topic: &sc.topic,
        };
        let _ = validate_data_column_sidecar::<Mainnet>(
            &mut g.column,
            &input,
            pool.kzg.as_ref(),
            pool.sampling.as_ref(),
            Some(&pool.metrics),
        );
        g.export_occupancy(&pool.metrics);
    }
}

async fn report(pool: &ValidationPool, work: &GossipWork, verdict: Verdict) {
    if matches!(verdict.acceptance, Acceptance::Accept) && !verdict.correlation_id.is_empty() {
        let mut g = pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.note_reported_accept(
            verdict.correlation_id.clone(),
            ReportedEntry {
                verdict: verdict.clone(),
                peer_id: work.peer_id,
            },
        );
    }

    // Gossip REJECT → app-score path (M6 policy).
    if matches!(verdict.acceptance, Acceptance::Reject) {
        let _ = pool
            .penalty_tx
            .try_send(PeerPenaltyCmd {
                peer_id: work.peer_id,
                reason: PeerPenaltyReason::GossipInvalid,
            });
    }

    pool.metrics
        .inc_gossip_messages(&work.topic, verdict.acceptance_label());

    let cmd = SwarmCommand::ReportValidation {
        message_id: work.message_id.clone(),
        peer_id: work.peer_id,
        verdict,
    };
    if pool.cmd_tx.send(cmd).await.is_err() {
        warn!("cmd channel closed; cannot report validation result");
    }
}

/// Apply a late chain verdict after we already reported (no re-report).
pub fn apply_late_chain_verdict(
    state: &mut ValidationPoolState,
    late: &Verdict,
    metrics: &P2pMetrics,
    penalty_tx: &mpsc::Sender<PeerPenaltyCmd>,
) {
    // Prefer pinned late_open (H2) so LRU eviction of `reported` cannot drop
    // the peer id before import completes.
    let entry = state
        .late_open
        .remove(&late.correlation_id)
        .or_else(|| state.reported.get(&late.correlation_id).cloned());
    let Some(entry) = entry else {
        return;
    };
    if is_late_import_reject(&entry.verdict, late) {
        metrics.inc_peer_penalty(PeerPenaltyReason::ImportInvalid);
        let _ = penalty_tx.try_send(PeerPenaltyCmd {
            peer_id: entry.peer_id,
            reason: PeerPenaltyReason::ImportInvalid,
        });
        debug!(
            corr = %hex::encode(&late.correlation_id),
            "late chain Reject after ACCEPT → import_invalid (no re-report)"
        );
    }
}

fn disparity_to_slots(disparity: Duration, seconds_per_slot: u64) -> u64 {
    let ms = disparity.as_millis() as u64;
    let slot_ms = seconds_per_slot.saturating_mul(1000).max(1);
    ms.div_ceil(slot_ms).max(1)
}

/// In-flight cap constant (equals gossip channel bound).
pub const IN_FLIGHT_VALIDATION_CAP: usize = GOSSIP_BOUND;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::gossip::topics::{
        SubnetCounts, TopicName, expand_fulu_topic_names, format_topic_string,
    };
    use cc_types::ForkDigest;
    use prometheus_client::registry::Registry;

    #[test]
    fn every_registered_topic_has_non_default_validator() {
        let counts = SubnetCounts {
            attestation: 64,
            sync_committee: 4,
            data_column_sidecar: 128,
        };
        assert!(all_topics_have_validators(&counts));
        for name in expand_fulu_topic_names(&counts) {
            let _ = validator_kind(name);
        }
        assert_eq!(
            validator_kind(TopicName::BeaconBlock),
            ValidatorKind::BeaconBlock
        );
        assert_eq!(
            validator_kind(TopicName::DataColumnSidecar(3)),
            ValidatorKind::DataColumnSidecar
        );
        assert_eq!(
            validator_kind(TopicName::VoluntaryExit),
            ValidatorKind::Operation
        );
        assert_eq!(
            validator_kind(TopicName::ProposerSlashing),
            ValidatorKind::Operation
        );
        assert_eq!(
            validator_kind(TopicName::BlsToExecutionChange),
            ValidatorKind::Operation
        );
        assert_eq!(
            validator_kind(TopicName::BeaconAttestation(0)),
            ValidatorKind::StubIgnore
        );
        assert_eq!(
            validator_kind(TopicName::SyncCommittee(0)),
            ValidatorKind::SyncCommittee
        );
        assert_eq!(
            validator_kind(TopicName::SyncCommitteeContributionAndProof),
            ValidatorKind::SyncCommittee
        );
    }

    #[test]
    fn parse_topic_roundtrip() {
        let digest = ForkDigest::from_array([0xaa, 0xbb, 0xcc, 0xdd]);
        for name in [
            TopicName::BeaconBlock,
            TopicName::DataColumnSidecar(127),
            TopicName::VoluntaryExit,
            TopicName::BeaconAttestation(5),
        ] {
            let s = format_topic_string(&digest, name);
            assert_eq!(parse_topic_name(&s), Some(name));
        }
    }

    #[test]
    fn in_flight_cap_is_1024() {
        assert_eq!(IN_FLIGHT_VALIDATION_CAP, 1024);
        assert_eq!(GOSSIP_BOUND, 1024);
    }

    #[test]
    fn late_reject_increments_import_invalid_and_sends_penalty() {
        let mut reg = Registry::default();
        let metrics = P2pMetrics::register(&mut reg);
        let mut state = ValidationPoolState::new();
        let corr = vec![1, 2, 3];
        let peer = {
            let kp = cc_libp2p::reexport::Keypair::generate_ed25519();
            PeerId::from_public_key(&kp.public())
        };
        state.note_reported_accept(
            corr.clone(),
            ReportedEntry {
                verdict: Verdict::accept(corr.clone()),
                peer_id: peer,
            },
        );
        let (tx, mut rx) = mpsc::channel(4);
        let late = Verdict::reject(Reason::Invalid, corr);
        apply_late_chain_verdict(&mut state, &late, &metrics, &tx);
        assert_eq!(
            metrics.peer_penalty_count(PeerPenaltyReason::ImportInvalid),
            1
        );
        let cmd = rx.try_recv().expect("penalty cmd");
        assert_eq!(cmd.peer_id, peer);
        assert_eq!(cmd.reason, PeerPenaltyReason::ImportInvalid);
        assert!(state.late_open.is_empty(), "pin released after late");
    }

    #[test]
    fn late_reject_survives_reported_lru_eviction() {
        // H2: pin in late_open so a full reported LRU cannot drop import_invalid.
        let mut reg = Registry::default();
        let metrics = P2pMetrics::register(&mut reg);
        let mut state = ValidationPoolState::new();
        let peer = {
            let kp = cc_libp2p::reexport::Keypair::generate_ed25519();
            PeerId::from_public_key(&kp.public())
        };
        let pinned = vec![0xAAu8; 32];
        state.note_reported_accept(
            pinned.clone(),
            ReportedEntry {
                verdict: Verdict::accept(pinned.clone()),
                peer_id: peer,
            },
        );
        // Flood reported LRU past its bound — pinned corr is evicted from LRU.
        for i in 0..(REPORTED_ACCEPT_BOUND + 10) {
            let mut corr = vec![0u8; 32];
            corr[..8].copy_from_slice(&(i as u64).to_le_bytes());
            state.reported.put(
                corr.clone(),
                ReportedEntry {
                    verdict: Verdict::accept(corr),
                    peer_id: peer,
                },
            );
        }
        assert!(
            state.reported.get(&pinned).is_none(),
            "pinned corr must be LRU-evicted from reported for this test"
        );
        assert!(
            state.late_open.contains_key(&pinned),
            "late_open must still hold the pin"
        );
        let (tx, mut rx) = mpsc::channel(4);
        let late = Verdict::reject(Reason::Invalid, pinned);
        apply_late_chain_verdict(&mut state, &late, &metrics, &tx);
        assert_eq!(
            metrics.peer_penalty_count(PeerPenaltyReason::ImportInvalid),
            1,
            "import_invalid must fire from late_open after LRU eviction"
        );
        let cmd = rx.try_recv().expect("penalty");
        assert_eq!(cmd.reason, PeerPenaltyReason::ImportInvalid);
    }

    #[test]
    fn reported_map_is_bounded() {
        let mut state = ValidationPoolState::new();
        let peer = {
            let kp = cc_libp2p::reexport::Keypair::generate_ed25519();
            PeerId::from_public_key(&kp.public())
        };
        for i in 0..(REPORTED_ACCEPT_BOUND + 50) {
            let mut corr = vec![0u8; 32];
            corr[..8].copy_from_slice(&(i as u64).to_le_bytes());
            state.reported.put(
                corr.clone(),
                ReportedEntry {
                    verdict: Verdict::accept(corr),
                    peer_id: peer,
                },
            );
        }
        assert!(state.reported.len() <= REPORTED_ACCEPT_BOUND);
    }

    #[test]
    fn production_kzg_is_not_always_valid() {
        // Production installer must not be AlwaysValidKzg.
        let kzg = production_kzg_verify();
        // Empty inputs must fail-closed (false) for real or fail-closed backend.
        assert!(!kzg.verify_column_kzg(0, &[], &[], &[]));
    }
}
