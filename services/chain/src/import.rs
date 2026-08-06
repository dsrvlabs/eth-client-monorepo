//! Import path on the core thread (Architecture §7.2, ADR-P1-10).
//!
//! ```text
//! decode-free dedup probe → decode → root check → parent → DA → ST → FC
//!   → get_head → pin residency to FC head → prune → snapshot → events → verdict
//! ```
//!
//! The pre-computed `ImportBlockRequest.root` is a **probe only**: a hit that is
//! **fully imported** (`store.blocks` **and** proto-array) returns `DUPLICATE`
//! without decoding or re-running transition. A partial (header without
//! proto-array) falls through so `on_block` can resume (SEC-4).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use cc_fork_choice::{BlockImport, DeferralReason, OnBlockError, Store, get_head, on_block};
use cc_proto::chain::{EventKind, ImportBlockRequest, ImportBlockResponse, ImportBlockVerdict};
use cc_state_transition::BlockSignatureStrategy;
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::{ForkName, SignedBeaconBlock};
use ssz::Encode;
use tonic::Status;
use tree_hash::TreeHash;

use crate::events::EventInput;
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::metrics::{ChainMetrics, ImportResult, ImportStage};
use crate::residency::Residency;

/// Outcome of a single import attempt (core thread).
#[derive(Debug)]
pub struct ImportOutcome {
    pub response: ImportBlockResponse,
    /// True when state_transition / on_block was invoked (for DUPLICATE tests).
    pub transition_invoked: bool,
}

/// Shared counters observed by tests (DUPLICATE short-circuit).
#[derive(Debug, Default)]
pub struct ImportCounters {
    /// Times the import path entered `on_block` (post-probe).
    pub transition_invocations: AtomicU64,
}

impl ImportCounters {
    pub fn transition_count(&self) -> u64 {
        self.transition_invocations.load(Ordering::Relaxed)
    }
}

/// Fully imported iff header **and** proto-array node exist (matches `on_block`).
#[inline]
fn is_fully_imported<P: Preset>(store: &Store<P>, root: &Root) -> bool {
    store.blocks().contains_key(root) && store.proto_array().contains(root)
}

/// Run the full import path against a live store (core thread only).
#[allow(clippy::too_many_arguments)]
pub fn import_block<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    counters: &ImportCounters,
    snapshot_sequence: &mut u64,
    request: ImportBlockRequest,
    verify: BlockSignatureStrategy,
) -> Result<ImportOutcome, Status> {
    // --- 1. decode-free dedup probe (ADR-P1-10 / SEC-4) ----------------------
    // Only fully-imported roots short-circuit. Header-only partials fall through
    // so on_block can resume integration without a false DUPLICATE.
    let probe = parse_root(&request.root)?;
    if is_fully_imported(store, &probe) {
        metrics.inc_import_result(ImportResult::Duplicate);
        return Ok(ImportOutcome {
            response: ImportBlockResponse {
                verdict: ImportBlockVerdict::Duplicate as i32,
                reason: String::new(),
            },
            transition_invoked: false,
        });
    }

    // --- 2. decode ----------------------------------------------------------
    let decode_start = Instant::now();
    let signed = decode_signed_block::<P>(&request.ssz, request.fork)?;
    metrics.observe_import_stage(ImportStage::Decode, decode_start.elapsed().as_secs_f64());

    // --- 3. true root check -------------------------------------------------
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
    if true_root != probe {
        metrics.inc_import_root_mismatch();
        return Err(Status::invalid_argument(format!(
            "supplied root {probe} does not match decoded hash_tree_root {true_root}"
        )));
    }

    // --- 4. ensure parent state available (residency / body-ring replay) ----
    let parent_root = signed.message.parent_root;
    if store.blocks().contains_key(&parent_root)
        && let Err(e) = residency.ensure_in_store(store, parent_root, config)
    {
        // Deep reorg gap: record, do not panic, surface as INVALID.
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(ImportOutcome {
            response: ImportBlockResponse {
                verdict: ImportBlockVerdict::Invalid as i32,
                reason: format!("reorg gap: {e}"),
            },
            transition_invoked: false,
        });
    }

    // --- 5–6. DA → ST → FC via on_block ------------------------------------
    counters
        .transition_invocations
        .fetch_add(1, Ordering::Relaxed);
    // Wall time of the whole on_block call (ST + FC integrate). Separate
    // Transition/ForkChoice stage split needs FC-internal seams (review M6).
    let on_block_start = Instant::now();
    let outcome = on_block(store, &signed, config, verify);
    let on_block_secs = on_block_start.elapsed().as_secs_f64();
    metrics.observe_import_stage(ImportStage::Transition, on_block_secs);

    let (reason, imported_root) = match outcome {
        Ok(BlockImport::Imported(b)) => (String::new(), Some(b.root)),
        Ok(BlockImport::Deferred(DeferralReason::DataUnavailable)) => {
            metrics.inc_import_result(ImportResult::Deferred);
            return Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::DeferredDa as i32,
                    reason: "data_unavailable".into(),
                },
                transition_invoked: true,
            });
        }
        Ok(BlockImport::Deferred(DeferralReason::UnknownParent)) => {
            metrics.inc_import_result(ImportResult::UnknownParent);
            return Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::UnknownParent as i32,
                    reason: "unknown_parent".into(),
                },
                transition_invoked: true,
            });
        }
        Ok(BlockImport::Deferred(DeferralReason::FutureSlot)) => {
            // Proto has no FUTURE_SLOT verdict (CC-18a). Do **not** map to
            // DEFERRED_DA (driver would treat as DA requeue). Use INVALID with a
            // stable machine-readable reason until a proto extension lands.
            metrics.inc_import_result(ImportResult::Invalid);
            return Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::Invalid as i32,
                    reason: "future_slot".into(),
                },
                transition_invoked: true,
            });
        }
        Err(OnBlockError::NotDescendedFromFinalized) => {
            metrics.inc_import_result(ImportResult::Invalid);
            return Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::Invalid as i32,
                    reason: "not_descended_from_finalized".into(),
                },
                transition_invoked: true,
            });
        }
        Err(e) => {
            metrics.inc_import_result(ImportResult::Invalid);
            return Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::Invalid as i32,
                    reason: e.to_string(),
                },
                transition_invoked: true,
            });
        }
    };

    let block_root = imported_root.unwrap_or(true_root);
    let slot = signed.message.slot.as_u64();
    let slots_per_epoch = P::SLOTS_PER_EPOCH.max(1);
    let is_epoch_boundary = slot.is_multiple_of(slots_per_epoch);

    let validator_count = store
        .block_state(&block_root)
        .map(|s| s.validators_len() as u64)
        .unwrap_or(0);
    metrics.observe_process_block(on_block_secs, slot, slot / slots_per_epoch, validator_count);

    // Body ring + scratch pin only — no Head pin / prune yet (H2).
    if let Some(post) = store.block_state(&block_root).cloned() {
        residency.record_imported_body(block_root, Arc::new(signed.clone()), post);
    }

    // --- 7. head recompute --------------------------------------------------
    let publish_start = Instant::now();
    let (head_root, reorg) = get_head(store)
        .map_err(|e| Status::internal(format!("get_head failed after import: {e}")))?;
    let head_slot = store
        .blocks()
        .get(&head_root)
        .map(|h| h.slot)
        .unwrap_or(signed.message.slot);
    let head_state_root = store
        .blocks()
        .get(&head_root)
        .map(|h| h.state_root)
        .unwrap_or(Root::ZERO);

    // --- 7b. pin Head = FC head, then prune (H2) ----------------------------
    residency.settle_after_head(store, head_root, block_root, is_epoch_boundary);
    metrics.set_resident_states(residency.resident_count() as u64);
    metrics.set_body_ring_len(residency.body_ring_len() as u64);

    // --- 8. snapshot publish BEFORE events (ordering guarantee) ------------
    *snapshot_sequence = snapshot_sequence.saturating_add(1);
    let snapshot = HeadSnapshot {
        head_root,
        head_slot,
        head_state_root,
        justified: store.justified_checkpoint(),
        finalized: store.finalized_checkpoint(),
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        current_epoch_target_root: Root::ZERO,
        dependent_root: Root::ZERO,
        is_optimistic: false,
        sequence: *snapshot_sequence,
    };
    head_store.store(snapshot);
    metrics.set_head(
        head_slot.as_u64(),
        0,
        store.finalized_checkpoint().epoch.as_u64(),
    );

    // --- 9. event publish (non-blocking; never stall the core — SEC-2) -----
    publish_events_nonblocking(
        event_tx,
        metrics,
        slot,
        block_root,
        head_root,
        head_slot.as_u64(),
        reorg,
    );
    metrics.observe_import_stage(ImportStage::Publish, publish_start.elapsed().as_secs_f64());

    metrics.inc_import_result(ImportResult::Imported);
    Ok(ImportOutcome {
        response: ImportBlockResponse {
            verdict: ImportBlockVerdict::Imported as i32,
            reason,
        },
        transition_invoked: true,
    })
}

/// Publish import events with `try_send` only — never block the core thread.
fn publish_events_nonblocking(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    slot: u64,
    block_root: Root,
    head_root: Root,
    head_slot: u64,
    reorg: Option<cc_fork_choice::ChainReorg>,
) {
    let root_bytes = Bytes::copy_from_slice(block_root.as_slice());
    try_publish_event(
        event_tx,
        metrics,
        EventInput {
            slot,
            root: root_bytes,
            kind: EventKind::BlockImported,
            payload: Bytes::new(),
        },
    );
    try_publish_event(
        event_tx,
        metrics,
        EventInput {
            slot: head_slot,
            root: Bytes::copy_from_slice(head_root.as_slice()),
            kind: EventKind::Head,
            payload: Bytes::new(),
        },
    );
    if let Some(reorg) = reorg {
        try_publish_event(
            event_tx,
            metrics,
            EventInput {
                slot: reorg.new_head_slot.as_u64(),
                root: Bytes::copy_from_slice(reorg.new_head.as_slice()),
                kind: EventKind::ChainReorg,
                payload: Bytes::new(),
            },
        );
    }
}

fn try_publish_event(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    input: EventInput,
) {
    match event_tx.try_send(input) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            metrics.inc_event_publish_dropped();
            tracing::warn!("events channel full; dropped core event (SEC-2 non-blocking)");
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            metrics.inc_event_publish_dropped();
            tracing::warn!("events channel closed; dropped core event");
        }
    }
}

/// Parse a 32-byte root from the request probe field.
pub fn parse_root(bytes: &[u8]) -> Result<Root, Status> {
    if bytes.len() != 32 {
        return Err(Status::invalid_argument(format!(
            "root must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(Root::from_array(arr))
}

/// Decode a `SignedBeaconBlock` from SSZ bytes.
///
/// `fork` is reserved for multi-fork decode; Phase 1 is Fulu-only.
pub fn decode_signed_block<P: Preset>(
    ssz: &[u8],
    _fork: u32,
) -> Result<SignedBeaconBlock<P>, Status> {
    SignedBeaconBlock::<P>::from_ssz_bytes_with(ForkName::Fulu, ssz).map_err(|e| {
        Status::invalid_argument(format!("failed to decode SignedBeaconBlock SSZ: {e:?}"))
    })
}

/// Encode a signed block to SSZ (tests).
pub fn encode_signed_block<P: Preset>(block: &SignedBeaconBlock<P>) -> Vec<u8> {
    block.as_ssz_bytes()
}

/// Publish snapshot then events in core order (unit-testable ordering helper).
///
/// Used by tests to assert snapshot-before-event without a full state transition.
pub fn publish_snapshot_then_events(
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot: HeadSnapshot,
    slot: u64,
    block_root: Root,
) {
    let head_root = snapshot.head_root;
    let head_slot = snapshot.head_slot.as_u64();
    head_store.store(snapshot);
    publish_events_nonblocking(
        event_tx, metrics, slot, block_root, head_root, head_slot, None,
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_types::preset::Minimal;
    use cc_types::primitives::Slot;
    use prometheus_client::registry::Registry;
    use tokio::sync::mpsc;

    #[test]
    fn parse_root_rejects_wrong_length() {
        let err = parse_root(&[0u8; 16]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn encode_decode_roundtrip_empty_block() {
        let block = SignedBeaconBlock::<Minimal> {
            message: Default::default(),
            signature: Default::default(),
        };
        let bytes = encode_signed_block(&block);
        let decoded = decode_signed_block::<Minimal>(&bytes, 0).unwrap();
        assert_eq!(decoded.message.slot, block.message.slot);
    }

    #[tokio::test]
    async fn snapshot_stored_before_event_deliverable() {
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, mut rx) = mpsc::channel(4);
        let root = Root::from_array([0x11; 32]);
        let snap = HeadSnapshot {
            head_root: root,
            head_slot: Slot::new(7),
            sequence: 3,
            ..HeadSnapshot::default()
        };
        publish_snapshot_then_events(&head, &tx, &metrics, snap, 7, root);
        // Snapshot must already be visible before we read any event.
        assert_eq!(head.load().sequence, 3);
        assert_eq!(head.load().head_root, root);
        let ev = rx.recv().await.expect("block_imported");
        assert_eq!(ev.kind, EventKind::BlockImported);
        let ev = rx.recv().await.expect("head");
        assert_eq!(ev.kind, EventKind::Head);
        assert_eq!(head.load().sequence, 3);
    }
}
