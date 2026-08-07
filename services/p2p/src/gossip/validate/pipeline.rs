//! Validation pool: topic dispatch, stub validators, report via swarm cmd.
//!
//! **CC-22/4**: every registered Fulu topic family has a non-default validator
//! wired here. Operation / attestation / sync families are IGNORE stubs
//! (CC-2B / CC-2C / CC-2D replace them).
//!
//! ## Ownership
//!
//! One [`ValidationPoolState`] owns a single [`ColumnValidatorState`] (seen +
//! pending + inclusion cache). Block and column paths share it — no dual sync.
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
use crate::channels::{
    ChainInbound, ChainOutbound, GossipWork, PeerPenaltyCmd, SwarmCommand, VerdictResolution,
    GOSSIP_BOUND,
};
use crate::clock::SlotClock;
use crate::gossip::seen::{BLOCK_SEEN_BOUND, COLUMN_SEEN_BOUND};
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
    /// IGNORE stub (CC-2B/C/D).
    StubIgnore,
}

/// Map a topic name to its validator kind.
#[must_use]
pub fn validator_kind(name: TopicName) -> ValidatorKind {
    match name {
        TopicName::BeaconBlock => ValidatorKind::BeaconBlock,
        TopicName::DataColumnSidecar(_) => ValidatorKind::DataColumnSidecar,
        TopicName::BeaconAggregateAndProof
        | TopicName::BeaconAttestation(_)
        | TopicName::SyncCommitteeContributionAndProof
        | TopicName::SyncCommittee(_)
        | TopicName::VoluntaryExit
        | TopicName::ProposerSlashing
        | TopicName::AttesterSlashing
        | TopicName::BlsToExecutionChange => ValidatorKind::StubIgnore,
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
    /// Bounded map of ACCEPTed correlation ids → entry (late Reject path).
    pub reported: LruCache<Vec<u8>, ReportedEntry>,
}

impl ValidationPoolState {
    /// Fresh pool state.
    #[must_use]
    pub fn new() -> Self {
        let cap = NonZeroUsize::new(REPORTED_ACCEPT_BOUND).unwrap_or(NonZeroUsize::MIN);
        Self {
            column: ColumnValidatorState::new(),
            reported: LruCache::new(cap),
        }
    }

    /// Export occupancy gauges (pending + seen).
    pub fn export_occupancy(&self, metrics: &P2pMetrics) {
        let (ps, pb) = self.column.pending.occupancy();
        metrics.set_queue_depth(QueueName::PendingSidecar, ps as i64);
        metrics.set_queue_depth(QueueName::PendingBlock, pb as i64);
        let (cs, bs) = self.column.seen.occupancy();
        // Reuse cache_* gauges as seen occupancy / bound (no new CC-29a families).
        metrics.set_cache_occupancy_bytes((cs.saturating_add(bs)) as i64);
        metrics.set_cache_bound_bytes((COLUMN_SEEN_BOUND.saturating_add(BLOCK_SEEN_BOUND)) as i64);
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
    /// Gossip channel bound.
    pub in_flight_cap: usize,
}

impl ValidationPool {
    /// Production pool: real/fail-closed KZG, noop sampling.
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
            in_flight_cap: GOSSIP_BOUND,
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

    // Finalization prune (M1).
    {
        let mut guard = pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.column.seen.prune_at_finalization(finalized_slot);
    }

    match validator_kind(name) {
        ValidatorKind::StubIgnore => Verdict::ignore(Reason::AlreadyKnown, vec![]),
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
        g.reported.put(
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
    let Some(entry) = state.reported.get(&late.correlation_id).cloned() else {
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
            ValidatorKind::StubIgnore
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
        state.reported.put(
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
