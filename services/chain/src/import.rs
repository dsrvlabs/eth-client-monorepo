//! Import path on the core thread (Architecture §7.2, ADR-P1-10, CC-27c).
//!
//! ```text
//! decode-free dedup probe → decode → root check → parent → proposer →
//! finalized descent → **block proposer BLS** (always on gossip path; H1)
//!   → **early gossip ACCEPT**  (CC-27c fast path)
//!   → DA → ST → FC → get_head → pin residency → prune → snapshot → events
//!   → import result (not a second gossip verdict)
//! ```
//!
//! The pre-computed `ImportBlockRequest.root` is a **probe only**: a hit that is
//! **fully imported** (`store.blocks` **and** proto-array) returns `DUPLICATE`
//! without decoding or re-running transition. A partial (header without
//! proto-array) falls through so `on_block` can resume (SEC-4).
//!
//! # Gossip-verify fast path (CC-27c)
//!
//! Cheap gossip conditions answer the block **acceptance** before
//! `state_transition` so verdict latency p95 ≤ 100 ms is reachable. A late
//! `Reject` from the transition does **not** re-report gossip; it is an
//! application-score penalty (`import_invalid`). `Internal` never penalises.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use cc_fork_choice::{
    BlockImport, DeferralReason, OnBlockError, Store, get_checkpoint_block, get_head, on_block,
};
use cc_proto::chain::{EventKind, ImportBlockRequest, ImportBlockResponse, ImportBlockVerdict};
use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::{
    BlockError, BlockSignatureSet, BlockSignatureStrategy, GossipClass, compute_epoch_at_slot,
    push_block_proposer_signature,
};
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::{ForkName, SignedBeaconBlock};
use ssz::Encode;
use tonic::Status;
use tree_hash::TreeHash;

use crate::da::{BlockBranchTrigger, PendingDa, PendingDaEntry, block_branch_trigger_from_signed};
use crate::epoch_context::EpochContext;
use crate::events::EventInput;
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::metrics::{ChainMetrics, ImportResult, ImportStage};
use crate::pending_engine::{PendingEngine, PendingEngineEntry};
use crate::residency::Residency;

/// Outcome of a single import attempt (core thread).
#[derive(Debug)]
pub struct ImportOutcome {
    pub response: ImportBlockResponse,
    /// True when state_transition / on_block was invoked (for DUPLICATE tests).
    pub transition_invoked: bool,
    /// Cheap gossip conditions passed and early ACCEPT was (or would be) emitted.
    pub early_accept: bool,
    /// After early ACCEPT, import failed with [`GossipClass::Reject`] — late
    /// application-score penalty (`import_invalid`), **no** second gossip report.
    pub late_import_reject: bool,
    /// After early ACCEPT, import failed with [`GossipClass::Internal`] — no
    /// penalty and no REJECT.
    pub late_import_internal: bool,
    /// CC-38a block-branch fast-path trigger (template-sized only).
    ///
    /// Set when import parks on `Deferred(DataUnavailable)` with non-empty
    /// `blob_kzg_commitments`. Core fires unary `FetchBlobs` — never cells.
    pub block_branch: Option<BlockBranchTrigger>,
}

/// Map `(early_accept, class)` → late-import flags (CC-27c / §5.3).
///
/// Pure helper so tests can force Reject vs Internal without a full ST.
#[must_use]
pub fn late_import_flags(early_accept: bool, class: GossipClass) -> (bool, bool) {
    (
        early_accept && class == GossipClass::Reject,
        early_accept && class == GossipClass::Internal,
    )
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
///
/// Unary `ImportBlock` callers use this without an early-accept hook; the
/// P2pStream path passes [`Some`] so the gossip verdict can leave the process
/// **before** the state transition runs (CC-27c).
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
    import_block_with_early(
        store,
        residency,
        config,
        head_store,
        event_tx,
        metrics,
        counters,
        snapshot_sequence,
        request,
        verify,
        None,
        None,
        None,
        None,
        None,
    )
}

/// Like [`import_block`], with optional epoch context and early-accept oneshot.
///
/// When cheap gossip conditions pass, `early_accept` is completed **before**
/// `on_block` / state transition (non-blocking channel send only).
///
/// **Gossip BLS (H1):** when `early_accept_tx` is `Some`, the cheap path **always**
/// verifies the block proposer signature with [`BlockSignatureStrategy::VerifyIndividual`]
/// and **fails closed** (no early ACCEPT) on failure — never skip BLS for re-gossip.
///
/// `inject_after_early` (tests only): if set, after early ACCEPT skip `on_block` and
/// treat the injected error as the import failure (non-vacuous late-flag tests).
///
/// `pending_da` (CC-24d): when `on_block` returns `Deferred(DataUnavailable)`,
/// the signed block is parked for re-drive on `DataAvailable`.
///
/// `pending_engine` (CC-36a): when `on_block` returns
/// `Deferred(ExecutionEngineUnavailable)`, the signed block is parked for
/// re-drive when the engine returns (separate map, 64 / 8 slots).
#[allow(clippy::too_many_arguments)]
pub fn import_block_with_early<P: Preset>(
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
    epoch_ctx: Option<&EpochContext>,
    early_accept_tx: Option<tokio::sync::oneshot::Sender<()>>,
    inject_after_early: Option<OnBlockError>,
    pending_da: Option<&mut PendingDa>,
    pending_engine: Option<&mut PendingEngine>,
) -> Result<ImportOutcome, Status> {
    let gossip_path = early_accept_tx.is_some() || inject_after_early.is_some();
    // --- 1. decode-free dedup probe (ADR-P1-10 / SEC-4) ----------------------
    let probe = parse_root(&request.root)?;
    if is_fully_imported(store, &probe) {
        metrics.inc_import_result(ImportResult::Duplicate);
        return Ok(ImportOutcome {
            response: ImportBlockResponse {
                verdict: ImportBlockVerdict::Duplicate as i32,
                reason: String::new(),
            },
            transition_invoked: false,
            early_accept: false,
            late_import_reject: false,
            late_import_internal: false,
        block_branch: None,
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

    // --- 4. cheap gossip conditions (CC-27c) --------------------------------
    // Parent presence / residency, proposer index, finalized descent, signature.
    // Terminal failures here become the **only** verdict (no early ACCEPT).
    if let Some(terminal) = cheap_gossip_terminal(
        store,
        residency,
        config,
        epoch_ctx,
        &signed,
        verify,
        gossip_path,
        metrics,
    )? {
        return Ok(ImportOutcome {
            response: terminal,
            transition_invoked: false,
            early_accept: false,
            late_import_reject: false,
            late_import_internal: false,
        block_branch: None,
        });
    }

    // --- 4b. early gossip ACCEPT (before state transition) ------------------
    let early_accept = true;
    if let Some(tx) = early_accept_tx {
        let _ = tx.send(());
    }

    // Test inject: force a classified import failure without a full ST (F1).
    if let Some(err) = inject_after_early {
        let class = on_block_error_gossip_class(&err);
        let (late_import_reject, late_import_internal) = late_import_flags(early_accept, class);
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(ImportOutcome {
            response: ImportBlockResponse {
                verdict: ImportBlockVerdict::Invalid as i32,
                reason: err.to_string(),
            },
            transition_invoked: false,
            early_accept,
            late_import_reject,
            late_import_internal,
        block_branch: None,
        });
    }

    // --- 5–6. DA → ST → FC via on_block ------------------------------------
    counters
        .transition_invocations
        .fetch_add(1, Ordering::Relaxed);
    let on_block_start = Instant::now();
    let outcome = on_block(store, &signed, config, verify);
    let on_block_secs = on_block_start.elapsed().as_secs_f64();
    metrics.observe_import_stage(ImportStage::Transition, on_block_secs);

    match outcome {
        Ok(BlockImport::Imported(b)) => finish_imported(
            store,
            residency,
            head_store,
            event_tx,
            metrics,
            snapshot_sequence,
            &signed,
            b.root,
            on_block_secs,
            early_accept,
        ),
        Ok(BlockImport::Deferred(DeferralReason::DataUnavailable)) => {
            // Park for re-drive when DataAvailable lands (CC-24d / §8.3).
            if let Some(pending) = pending_da {
                let entry = PendingDaEntry {
                    root: true_root,
                    ssz: Bytes::from(signed.as_ssz_bytes()),
                    fork: request.fork,
                    source: request.source,
                    slot: signed.message.slot.as_u64(),
                    parked_at_slot: store.get_current_slot().as_u64(),
                };
                if let Some(evicted) = pending.insert(entry) {
                    metrics.inc_da_pending_dropped(1);
                    tracing::debug!(
                        root = %evicted.root,
                        "pending_da capacity eviction"
                    );
                }
                metrics.set_da_pending_occupancy(pending.len() as u64);
            }
            metrics.inc_import_result(ImportResult::Deferred);
            // CC-38a: template-sized block-branch trigger for unary FetchBlobs.
            // Never carries cells; core fires engine FetchBlobs when present.
            let block_branch = block_branch_trigger_from_signed(&signed, true_root);
            Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::DeferredDa as i32,
                    reason: "data_unavailable".into(),
                },
                transition_invoked: true,
                early_accept,
                late_import_reject: false,
                late_import_internal: false,
                block_branch,
            })
        }
        Ok(BlockImport::Deferred(DeferralReason::UnknownParent)) => {
            // Parent was present at the cheap check; race/reorg edge.
            metrics.inc_import_result(ImportResult::UnknownParent);
            Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::UnknownParent as i32,
                    reason: "unknown_parent".into(),
                },
                transition_invoked: true,
                early_accept,
                late_import_reject: false,
                late_import_internal: false,
            block_branch: None,
            })
        }
        Ok(BlockImport::Deferred(DeferralReason::FutureSlot)) => {
            metrics.inc_import_result(ImportResult::Invalid);
            Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::Invalid as i32,
                    reason: "future_slot".into(),
                },
                transition_invoked: true,
                early_accept,
                // Future slot is Ignore-class for gossip; not a late Reject penalty.
                late_import_reject: false,
                late_import_internal: false,
            block_branch: None,
            })
        }
        Ok(BlockImport::Deferred(DeferralReason::ExecutionEngineUnavailable)) => {
            // Park for re-drive when the engine returns (CC-36a / §4.9).
            if let Some(pending) = pending_engine {
                let entry = PendingEngineEntry {
                    root: true_root,
                    ssz: Bytes::from(signed.as_ssz_bytes()),
                    fork: request.fork,
                    source: request.source,
                    slot: signed.message.slot.as_u64(),
                    parked_at_slot: store.get_current_slot().as_u64(),
                };
                if let Some(evicted) = pending.insert(entry) {
                    metrics.inc_pending_engine_dropped(1);
                    tracing::debug!(
                        root = %evicted.root,
                        "pending_engine capacity eviction"
                    );
                }
                metrics.set_pending_engine_occupancy(pending.len() as u64);
            }
            metrics.inc_import_result(ImportResult::DeferredEngine);
            Ok(ImportOutcome {
                response: ImportBlockResponse {
                    // Proto has no DeferredEngine verdict; DeferredDa is the
                    // Ignore-class park (same gossip class). Metric distinguishes.
                    verdict: ImportBlockVerdict::DeferredDa as i32,
                    reason: "execution_engine_unavailable".into(),
                },
                transition_invoked: true,
                early_accept,
                late_import_reject: false,
                late_import_internal: false,
            block_branch: None,
            })
        }
        Err(e) => {
            let class = on_block_error_gossip_class(&e);
            metrics.inc_import_result(ImportResult::Invalid);
            let (late_import_reject, late_import_internal) = late_import_flags(early_accept, class);
            Ok(ImportOutcome {
                response: ImportBlockResponse {
                    verdict: ImportBlockVerdict::Invalid as i32,
                    reason: e.to_string(),
                },
                transition_invoked: true,
                early_accept,
                late_import_reject,
                late_import_internal,
            block_branch: None,
            })
        }
    }
}

/// Exhaustive [`OnBlockError`] → [`GossipClass`] (no catch-all).
///
/// Delegates transition errors to [`BlockError::gossip_class`]. Keeping the
/// match total is Phase 1 §5.3 / R-13: a new variant fails to compile.
pub fn on_block_error_gossip_class(err: &OnBlockError) -> GossipClass {
    match err {
        OnBlockError::NotDescendedFromFinalized => GossipClass::Reject,
        OnBlockError::Transition(e) => e.gossip_class(),
        OnBlockError::ProtoArray(_)
        | OnBlockError::PulledUpTip(_)
        | OnBlockError::PartialImportNeedsBody
        | OnBlockError::Validation(_) => GossipClass::Internal,
    }
}

/// Cheap gossip conditions. `Ok(Some(response))` is a terminal import result
/// without running the state transition. `Ok(None)` means ACCEPT-and-continue.
///
/// # H3 mapping notes
/// - `future_slot` → stream IGNORE (`Reason::FutureSlot`) via reason string
/// - `too_old` (slot ≤ finalized) → stream IGNORE (`Reason::AlreadyKnown`)
/// - checkpoint non-descent → still Reject-class (`not_descended_from_finalized`)
#[allow(clippy::too_many_arguments)]
fn cheap_gossip_terminal<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    epoch_ctx: Option<&EpochContext>,
    signed: &SignedBeaconBlock<P>,
    verify: BlockSignatureStrategy,
    gossip_path: bool,
    metrics: &ChainMetrics,
) -> Result<Option<ImportBlockResponse>, Status> {
    let block = &signed.message;
    let parent_root = block.parent_root;

    // Parent presence (header).
    if !store.blocks().contains_key(&parent_root) {
        metrics.inc_import_result(ImportResult::UnknownParent);
        return Ok(Some(ImportBlockResponse {
            verdict: ImportBlockVerdict::UnknownParent as i32,
            reason: "unknown_parent".into(),
        }));
    }

    // Ensure parent state is resident (same as pre-fast-path import).
    if let Err(e) = residency.ensure_in_store(store, parent_root, config) {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(ImportBlockResponse {
            verdict: ImportBlockVerdict::Invalid as i32,
            reason: format!("reorg gap: {e}"),
        }));
    }

    // Future slot relative to store time → IGNORE-class (H3).
    if store.get_current_slot().as_u64() < block.slot.as_u64() {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(ImportBlockResponse {
            // Proto has no FUTURE_SLOT verdict; reason drives stream IGNORE.
            verdict: ImportBlockVerdict::Invalid as i32,
            reason: "future_slot".into(),
        }));
    }

    // Too old (slot at/before finalized epoch start) → IGNORE-class (H3 / eth2 gossip).
    let finalized_slot = compute_start_slot_at_epoch::<P>(store.finalized_checkpoint().epoch);
    if block.slot.as_u64() <= finalized_slot.as_u64() {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(ImportBlockResponse {
            verdict: ImportBlockVerdict::Invalid as i32,
            reason: "too_old".into(),
        }));
    }

    // Not a descendant of the finalized checkpoint → Reject-class (malicious / invalid chain).
    let finalized_checkpoint_block =
        get_checkpoint_block(store, parent_root, store.finalized_checkpoint().epoch);
    if store.finalized_checkpoint().root != finalized_checkpoint_block {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(ImportBlockResponse {
            verdict: ImportBlockVerdict::Invalid as i32,
            reason: "not_descended_from_finalized".into(),
        }));
    }

    // Proposer index against lookahead (EpochContext or parent-state window).
    if let Some(expected) = expected_proposer_index::<P>(store, epoch_ctx, block.slot.as_u64())
        && expected != block.proposer_index.as_u64()
    {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(ImportBlockResponse {
            verdict: ImportBlockVerdict::Invalid as i32,
            reason: format!(
                "proposer mismatch: block={} expected={expected}",
                block.proposer_index.as_u64()
            ),
        }));
    }

    // Block proposer signature.
    // H1: gossip path **always** verifies with VerifyIndividual and fails closed.
    // Unary ImportBlock keeps the configured strategy (may be NoVerification).
    let sig_strategy = if gossip_path {
        BlockSignatureStrategy::VerifyIndividual
    } else {
        verify
    };
    if !matches!(sig_strategy, BlockSignatureStrategy::NoVerification) {
        let Some(parent_state) = store.block_state(&parent_root) else {
            // Fail closed on gossip: never early-ACCEPT without a parent state to verify against.
            metrics.inc_import_result(ImportResult::Invalid);
            return Ok(Some(ImportBlockResponse {
                verdict: ImportBlockVerdict::Invalid as i32,
                reason: "proposer_sig_parent_state_missing".into(),
            }));
        };
        match verify_block_proposer_sig(parent_state, signed, sig_strategy) {
            Ok(()) => {}
            Err(e) => {
                // Fail closed: any classified failure blocks early ACCEPT.
                // Internal (state BLS) → no peer Reject; still no early ACCEPT.
                metrics.inc_import_result(ImportResult::Invalid);
                let reason = match e.gossip_class() {
                    GossipClass::Internal => format!("internal_proposer_sig: {e}"),
                    GossipClass::Reject | GossipClass::Ignore => e.to_string(),
                };
                return Ok(Some(ImportBlockResponse {
                    verdict: ImportBlockVerdict::Invalid as i32,
                    reason,
                }));
            }
        }
    }

    Ok(None)
}

/// Look up expected proposer for `slot` from epoch context or parent-state lookahead.
fn expected_proposer_index<P: Preset>(
    store: &Store<P>,
    epoch_ctx: Option<&EpochContext>,
    slot: u64,
) -> Option<u64> {
    if let Some(ctx) = epoch_ctx
        && !ctx.proposer_lookahead.is_empty()
    {
        let ctx_spe = ctx.slots_per_epoch.max(1);
        let start_slot = ctx.epoch.as_u64().saturating_mul(ctx_spe);
        if slot >= start_slot {
            let offset = (slot - start_slot) as usize;
            if let Some(p) = ctx.proposer_lookahead.get(offset) {
                return Some(*p);
            }
        }
    }

    // Fallback: head state's Fulu proposer_lookahead window.
    let head_root = store.last_head_root()?;
    let state = store.block_state(&head_root)?;
    if state.proposer_lookahead_len() == 0 {
        return None;
    }
    let epoch = compute_epoch_at_slot::<P>(state.slot());
    let start_slot = compute_start_slot_at_epoch::<P>(epoch).as_u64();
    if slot < start_slot {
        return None;
    }
    let offset = (slot - start_slot) as usize;
    if offset >= state.proposer_lookahead_len() {
        // Same-epoch slot-relative index used by get_beacon_proposer_index.
        let idx = (slot % P::SLOTS_PER_EPOCH.max(1)) as usize;
        return state.proposer_lookahead_get(idx).map(|v| v.as_u64());
    }
    state.proposer_lookahead_get(offset).map(|v| v.as_u64())
}

fn verify_block_proposer_sig<P: Preset>(
    state: &cc_types::BeaconState<P>,
    signed: &SignedBeaconBlock<P>,
    strategy: BlockSignatureStrategy,
) -> Result<(), BlockError> {
    let mut set = BlockSignatureSet::default();
    push_block_proposer_signature(&mut set, state, signed)?;
    set.verify(strategy)
}

#[allow(clippy::too_many_arguments)]
fn finish_imported<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot_sequence: &mut u64,
    signed: &SignedBeaconBlock<P>,
    block_root: Root,
    on_block_secs: f64,
    early_accept: bool,
) -> Result<ImportOutcome, Status> {
    let slot = signed.message.slot.as_u64();
    let slots_per_epoch = P::SLOTS_PER_EPOCH.max(1);
    let is_epoch_boundary = slot.is_multiple_of(slots_per_epoch);

    let validator_count = store
        .block_state(&block_root)
        .map(|s| s.validators_len() as u64)
        .unwrap_or(0);
    // Inclusive + exclusive dual observation (CC-3Aa / §6.4). Single call site
    // so pre-engine local ≡ inclusive; after the engine is in the path only
    // local stays exclusive (CC-32b will split the span).
    metrics.observe_process_block_with_local(
        on_block_secs,
        slot,
        slot / slots_per_epoch,
        validator_count,
    );

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
    let optimistic = cc_fork_choice::is_optimistic_node(store);
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
        // CC-3B: node-level optimistic from fork choice after fresh get_head.
        is_optimistic: optimistic,
        sequence: *snapshot_sequence,
    };
    head_store.store(snapshot);
    metrics.set_head(
        head_slot.as_u64(),
        0,
        store.finalized_checkpoint().epoch.as_u64(),
    );
    metrics.is_optimistic.set(i64::from(optimistic));
    metrics
        .optimistic_nodes
        .set(store.proto_array().optimistic_node_count() as i64);

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
            reason: String::new(),
        },
        transition_invoked: true,
        early_accept,
        late_import_reject: false,
        late_import_internal: false,
    block_branch: None,
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

    /// Exhaustive `OnBlockError` → class (no catch-all) — R-13 / §5.3.
    #[test]
    fn on_block_error_gossip_class_is_total() {
        use cc_fork_choice::ProtoArrayError;
        use cc_state_transition::{BlockError, SignatureKind};
        use cc_types::primitives::{Hash256, Slot, ValidatorIndex};

        let samples: &[(OnBlockError, GossipClass)] = &[
            (OnBlockError::NotDescendedFromFinalized, GossipClass::Reject),
            (
                OnBlockError::Transition(BlockError::InvalidSignature {
                    which: SignatureKind::BlockProposer,
                }),
                GossipClass::Reject,
            ),
            (
                OnBlockError::Transition(BlockError::StateRootMismatch {
                    expected: Root::ZERO,
                    actual: Root::from_array([1u8; 32]),
                }),
                GossipClass::Reject,
            ),
            (
                OnBlockError::Transition(BlockError::Engine(
                    cc_state_transition::EngineError::Transport("x".into()),
                )),
                GossipClass::Internal,
            ),
            (
                OnBlockError::Transition(BlockError::CachePoisoned),
                GossipClass::Internal,
            ),
            (
                OnBlockError::ProtoArray(ProtoArrayError::UnknownParent(Root::ZERO)),
                GossipClass::Internal,
            ),
            (OnBlockError::PulledUpTip("x".into()), GossipClass::Internal),
            (
                OnBlockError::Transition(BlockError::UnknownParent),
                GossipClass::Ignore,
            ),
            (
                OnBlockError::Transition(BlockError::FutureSlot {
                    block_slot: Slot::new(2),
                    current_slot: Slot::new(1),
                }),
                GossipClass::Ignore,
            ),
            (
                OnBlockError::Transition(BlockError::ProposerMismatch {
                    block: ValidatorIndex::new(0),
                    expected: ValidatorIndex::new(1),
                }),
                GossipClass::Reject,
            ),
            // CC-34b / §4.8 — EL consensus failure is Internal, not peer descore.
            (
                OnBlockError::Validation(
                    cc_fork_choice::ValidationError::ValidExecutionStatusBecameInvalid {
                        block_root: Root::ZERO,
                        payload_block_hash: Hash256::ZERO,
                    },
                ),
                GossipClass::Internal,
            ),
        ];
        for (err, expected) in samples {
            assert_eq!(
                on_block_error_gossip_class(err),
                *expected,
                "mismatch for {err:?}"
            );
            // Cross-check against OnBlockError's own method when present.
            assert_eq!(err.gossip_class(), *expected, "OnBlockError method {err:?}");
        }
    }
}
