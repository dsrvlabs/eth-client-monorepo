//! `ApplyAttestations` core-path handler (CC-1E, Architecture §6.3 / §7.7).
//!
//! ```text
//! bound check → for each SSZ: decode IndexedAttestation → on_attestation
//!   → (if any applied) get_head once → publish HeadSnapshot
//! ```
//!
//! **No per-attestation head computation.** `compute_deltas` runs only inside
//! the single trailing `get_head` (at most once per batch).
//!
//! # Trust boundary (SEC-1E-1 residual)
//!
//! This RPC is a **privileged weight-injection surface**. Phase 1 does **not**
//! verify BLS signatures, committee membership, or structural
//! `is_valid_indexed_attestation` (sorted / unique / non-empty). Any principal
//! that can call `ApplyAttestations` can set vote trackers for in-range
//! validator indices toward known blocks that pass free-floating validation.
//!
//! **Until Phase 5** verifies signatures against CC-1F cached shufflings and is
//! the sole live producer:
//! - treat this as a **trusted internal** RPC only (loopback / private mesh /
//!   mTLS / network policy — not a public gateway);
//! - do not expose chain gRPC to untrusted networks without authz.
//!
//! Residual High (SEC-1E-1): unsigned SSZ is accepted by design; operational
//! isolation is the control until Phase 5.

use bytes::Bytes;
use cc_fork_choice::{Store, get_head, on_attestation};
use cc_proto::chain::{
    ApplyAttestationsRequest, ApplyAttestationsResponse, AttestationApplyResult,
    AttestationApplyVerdict,
};
use cc_types::config::ChainConfig;
use cc_types::operations::IndexedAttestation;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use ssz::Decode;
use tonic::Status;

use crate::events::EventInput;
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::metrics::ChainMetrics;

/// Maximum attestations accepted in one `ApplyAttestations` batch (Architecture §7.7).
pub const MAX_APPLY_ATTESTATIONS: usize = 128;

/// Apply a batch of free-floating attestations on the core thread.
///
/// Oversized batches return `INVALID_ARGUMENT` **before** any store mutation
/// (no silent truncation, no partial apply).
///
/// # Trailing `get_head` failure (SEC-1E-2)
///
/// Vote trackers are committed before the trailing recompute. If `get_head`
/// fails after one or more applies, this still returns **`Ok` with per-item
/// results** so the client learns what was applied. The head snapshot is left
/// unchanged (may be **stale** relative to the store until the next successful
/// recompute via import / a later batch). An error is logged; it is not
/// surfaced as gRPC `INTERNAL` that would drop the results vector.
pub fn apply_attestations<P: Preset>(
    store: &mut Store<P>,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot_sequence: &mut u64,
    request: ApplyAttestationsRequest,
    config: &ChainConfig,
) -> Result<ApplyAttestationsResponse, Status> {
    let n = request.attestations_ssz.len();
    if n > MAX_APPLY_ATTESTATIONS {
        return Err(Status::invalid_argument(format!(
            "ApplyAttestations batch size {n} exceeds bound of {MAX_APPLY_ATTESTATIONS}"
        )));
    }

    let mut results = Vec::with_capacity(n);
    let mut any_applied = false;

    for ssz in &request.attestations_ssz {
        match apply_one::<P>(store, ssz, config) {
            Ok(()) => {
                any_applied = true;
                results.push(AttestationApplyResult {
                    verdict: AttestationApplyVerdict::Applied as i32,
                    reason: String::new(),
                });
            }
            Err(reason) => {
                results.push(AttestationApplyResult {
                    verdict: AttestationApplyVerdict::Rejected as i32,
                    reason,
                });
            }
        }
    }

    // Single head recompute after the batch so GetHead (ArcSwap) observes weight
    // through the honest observation point (§6.3). No get_head per attestation.
    // On failure: keep results (SEC-1E-2) — do not discard applied outcomes.
    if any_applied
        && let Err(e) =
            recompute_and_publish_head(store, head_store, event_tx, metrics, snapshot_sequence)
    {
        tracing::error!(
            error = %e,
            applied = results
                .iter()
                .filter(|r| r.verdict == AttestationApplyVerdict::Applied as i32)
                .count(),
            "ApplyAttestations: trailing get_head failed; vote trackers applied but \
             HeadSnapshot not updated (GetHead may be stale until next recompute)"
        );
    }

    Ok(ApplyAttestationsResponse { results })
}

fn apply_one<P: Preset>(
    store: &mut Store<P>,
    ssz: &[u8],
    config: &ChainConfig,
) -> Result<(), String> {
    let indexed = IndexedAttestation::<P>::from_ssz_bytes(ssz)
        .map_err(|e| format!("failed to decode IndexedAttestation SSZ: {e:?}"))?;
    // Free-floating path (Phase 5 producer): is_from_block = false.
    on_attestation(store, &indexed, false, config).map_err(|e| e.to_string())
}

fn recompute_and_publish_head<P: Preset>(
    store: &mut Store<P>,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot_sequence: &mut u64,
) -> Result<(), Status> {
    let (head_root, reorg) = get_head(store)
        .map_err(|e| Status::internal(format!("get_head failed after ApplyAttestations: {e}")))?;
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

    *snapshot_sequence = snapshot_sequence.saturating_add(1);
    let optimistic = cc_fork_choice::is_optimistic_node(store);
    head_store.store(HeadSnapshot {
        head_root,
        head_slot,
        head_state_root,
        justified: store.justified_checkpoint(),
        finalized: store.finalized_checkpoint(),
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        current_epoch_target_root: Root::ZERO,
        dependent_root: Root::ZERO,
        // CC-3B: node-level optimistic from fork choice after fresh get_head.
        is_optimistic: optimistic,
        sequence: *snapshot_sequence,
    });
    metrics.set_head(
        head_slot.as_u64(),
        0,
        store.finalized_checkpoint().epoch.as_u64(),
    );
    metrics.is_optimistic.set(i64::from(optimistic));

    // HEAD / REORG with §4.2 payloads. Core thread uses blocking_send (F1 /
    // Phase 1 §7.3) so storage-facing events are not silently dropped.
    if let Err(tokio::sync::mpsc::error::SendError(lost)) =
        event_tx.blocking_send(EventInput::head(
            head_slot.as_u64(),
            Bytes::copy_from_slice(head_root.as_slice()),
        ))
    {
        metrics.inc_event_publish_dropped();
        tracing::error!(
            kind = ?lost.kind,
            "events channel closed; lost HEAD after ApplyAttestations (F1)"
        );
    }
    if let Some(reorg) = reorg {
        let ancestor = crate::import::common_ancestor_slot(store, reorg.old_head, reorg.new_head);
        if let Err(tokio::sync::mpsc::error::SendError(lost)) =
            event_tx.blocking_send(EventInput::chain_reorg(
                reorg.new_head_slot.as_u64(),
                Bytes::copy_from_slice(reorg.new_head.as_slice()),
                Bytes::copy_from_slice(reorg.old_head.as_slice()),
                ancestor.as_u64(),
            ))
        {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                "events channel closed; lost CHAIN_REORG after ApplyAttestations (F1)"
            );
        }
    }
    Ok(())
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

    use cc_fork_choice::{ExecutionStatus, HarnessAvailability, get_forkchoice_store};
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::containers::{AttestationData, BeaconBlockHeader, Checkpoint};
    use cc_types::operations::IndexedAttestation;
    use cc_types::preset::Minimal;
    use cc_types::primitives::{
        Epoch, ExecutionAddress, ForkVersion, Hash256, Root, Slot, ValidatorIndex,
    };

    fn test_config() -> ChainConfig {
        ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
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
    use cc_types::{BeaconBlock, BeaconState};
    use prometheus_client::registry::Registry;
    use ssz::Encode;
    use ssz_types::VariableList;
    use tokio::sync::mpsc;
    use tonic::Code;
    use tree_hash::TreeHash;

    use crate::metrics::ChainMetrics;

    fn root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn cp(epoch: u64, r: Root) -> Checkpoint {
        Checkpoint {
            epoch: Epoch::new(epoch),
            root: r,
        }
    }

    fn indexed(
        indices: &[u64],
        slot: u64,
        beacon_block_root: Root,
        target: Checkpoint,
    ) -> IndexedAttestation<Minimal> {
        let attesting_indices = VariableList::new(
            indices
                .iter()
                .map(|i| ValidatorIndex::new(*i))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        IndexedAttestation {
            attesting_indices,
            data: AttestationData {
                slot: Slot::new(slot),
                index: Default::default(),
                beacon_block_root,
                source: cp(0, beacon_block_root),
                target,
            },
            signature: Default::default(),
        }
    }

    fn seeded_store(n: usize) -> (Store<Minimal>, Root) {
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
        let mut store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        store.resize_votes(n);
        store.set_justified_balances(vec![32_000_000_000u64; n]);
        // Advance past slot 0 so free-floating slot-0 atts pass FutureSlot.
        cc_fork_choice::on_tick(&mut store, 12).unwrap();
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        (store, anchor)
    }

    fn insert_child(store: &mut Store<Minimal>, parent: Root, child: Root, slot: u64) {
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        let mut child_state = store.block_state(&parent).unwrap().clone();
        child_state.set_slot(Slot::new(slot));
        store.insert_block(
            child,
            BeaconBlockHeader {
                slot: Slot::new(slot),
                proposer_index: ValidatorIndex::new(0),
                parent_root: parent,
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            child_state,
        );
        store
            .proto_array_mut()
            .on_block(cc_fork_choice::ProtoNodeBlock {
                slot: Slot::new(slot),
                root: child,
                parent_root: Some(parent),
                state_root: Root::ZERO,
                target_root: child,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();
    }

    #[test]
    fn oversized_batch_rejected_with_no_partial_apply() {
        let (mut store, anchor) = seeded_store(2);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;

        let att = indexed(&[0], 0, anchor, cp(0, anchor));
        let ssz = att.as_ssz_bytes();
        let mut batch = Vec::with_capacity(129);
        for _ in 0..129 {
            batch.push(ssz.clone());
        }

        let before = store.mutation_counter();
        let err = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: batch,
            },
            &test_config(),
        )
        .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message().contains("128"),
            "status must name the bound: {}",
            err.message()
        );
        assert_eq!(
            store.mutation_counter(),
            before,
            "oversized batch must not mutate the store"
        );
    }

    #[test]
    fn mixed_batch_reports_applied_and_rejected() {
        let (mut store, anchor) = seeded_store(2);
        let child = root(0x44);
        insert_child(&mut store, anchor, child, 1);
        // Store already at slot 2 via on_tick(12).
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;

        let valid = indexed(&[0], 1, child, cp(0, anchor));
        let unknown = indexed(&[0], 1, root(0xEE), cp(0, anchor));
        let resp = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: vec![valid.as_ssz_bytes(), unknown.as_ssz_bytes()],
            },
            &test_config(),
        )
        .unwrap();
        assert_eq!(resp.results.len(), 2);
        assert_eq!(
            resp.results[0].verdict,
            AttestationApplyVerdict::Applied as i32
        );
        assert_eq!(
            resp.results[1].verdict,
            AttestationApplyVerdict::Rejected as i32
        );
        assert!(!resp.results[1].reason.is_empty());
    }

    /// CC-1E: a 128-attestation batch performs exactly one trailing `get_head`
    /// (the sole `compute_deltas` call site on this path), never per attestation.
    ///
    /// Asserted via `snapshot_sequence` (+1) and `compute_deltas_call_count`
    /// (process-wide; concurrent lib tests can only *increase* the delta, so
    /// `delta < 128` rules out a per-attestation `get_head` path).
    #[test]
    fn single_trailing_get_head_across_128_batch() {
        let (mut store, anchor) = seeded_store(8);
        let child = root(0x55);
        insert_child(&mut store, anchor, child, 1);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;

        let seq_before = seq;
        let deltas_before = cc_fork_choice::compute_deltas_call_count();
        let batch: Vec<Vec<u8>> = (0..MAX_APPLY_ATTESTATIONS)
            .map(|i| indexed(&[(i % 8) as u64], 1, child, cp(0, anchor)).as_ssz_bytes())
            .collect();
        let resp = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: batch,
            },
            &test_config(),
        )
        .unwrap();
        assert_eq!(resp.results.len(), MAX_APPLY_ATTESTATIONS);
        assert!(
            resp.results
                .iter()
                .all(|r| r.verdict == AttestationApplyVerdict::Applied as i32)
        );
        assert_eq!(
            seq,
            seq_before + 1,
            "exactly one trailing get_head/publish for a 128-att batch (no per-att head)"
        );
        let deltas_after = cc_fork_choice::compute_deltas_call_count();
        let delta = deltas_after.saturating_sub(deltas_before);
        assert!(
            delta >= 1,
            "trailing get_head must invoke compute_deltas at least once"
        );
        // Per-att get_head would contribute ≥128; concurrent tests may add a few.
        assert!(
            delta < MAX_APPLY_ATTESTATIONS as u64,
            "compute_deltas ran {delta} times across a 128-att batch (expected 1, \
             <128 even under concurrent test noise) — per-att head path?"
        );
    }
}
