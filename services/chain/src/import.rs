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
    BlockImport, ChainReorg, DeferralReason, OnBlockError, Store, get_checkpoint_block, get_head,
    on_block,
};
use cc_proto::chain::{ImportBlockRequest, ImportBlockResponse, ImportBlockVerdict};
use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::{
    BlockError, BlockSignatureSet, BlockSignatureStrategy, GossipClass, compute_epoch_at_slot,
    push_block_proposer_signature,
};
use cc_types::config::ChainConfig;
use cc_types::containers::Checkpoint;
use cc_types::preset::Preset;
use cc_types::primitives::{Root, Slot};
use cc_types::{ForkName, SignedBeaconBlock};
use ssz::Encode;
use ssz_derive::Encode as SszEncode;
use tonic::Status;
use tree_hash::TreeHash;

use crate::da::{BlockBranchTrigger, PendingDa, PendingDaEntry, block_branch_trigger_from_signed};
use crate::epoch_context::EpochContext;
use crate::events::EventInput;
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::metrics::{ChainMetrics, ImportResult, ImportStage};
use crate::pending_engine::{PendingEngine, PendingEngineEntry};
use crate::tick::{GossipClock, admit_block_slot_if_within_disparity};

/// First payload byte for `BLOCK_IMPORTED` after a successful import (Architecture §4.2).
pub const BLOCK_PAYLOAD_VERDICT_IMPORTED: u8 = ImportBlockVerdict::Imported as u8;
/// First payload byte for `BLOCK_IMPORTED` after `DEFERRED_DA` (same kind, different disc.).
pub const BLOCK_PAYLOAD_VERDICT_DEFERRED_DA: u8 = ImportBlockVerdict::DeferredDa as u8;

/// SSZ layout matching `cc_store::meta::ForkChoiceScalars` (Architecture §2.5 / §4.2).
///
/// Defined here so `cc-chain` does not take a `cc-store` dependency (DAG forbids
/// `cc-chain → cc-store`). **Must stay field-for-field identical** to
/// `crates/store/src/meta.rs::ForkChoiceScalars`:
///
/// ```text
/// time: u64
/// proposer_boost_root: Root          // 32
/// justified: Checkpoint              // epoch:u64 ‖ root:32
/// finalized: Checkpoint
/// unrealized_justified: Checkpoint
/// unrealized_finalized: Checkpoint
/// head_root: Root                    // 32
/// head_slot: Slot                    // u64
/// ```
///
/// Fixed-part length = 8 + 32 + 4×40 + 32 + 8 = **240** bytes. A unit test pins
/// that length and the field order; storage decodes with the store type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, SszEncode)]
pub struct ForkChoiceScalarsPayload {
    pub time: u64,
    pub proposer_boost_root: Root,
    pub justified: Checkpoint,
    pub finalized: Checkpoint,
    pub unrealized_justified: Checkpoint,
    pub unrealized_finalized: Checkpoint,
    pub head_root: Root,
    pub head_slot: Slot,
}

/// Fixed SSZ byte length of [`ForkChoiceScalarsPayload`] / store `ForkChoiceScalars`.
pub const FORK_CHOICE_SCALARS_SSZ_LEN: usize = 240;

/// Build `BLOCK_IMPORTED` payload: `[verdict_byte] ‖ SignedBeaconBlock SSZ`.
pub fn block_imported_payload(verdict: u8, block_ssz: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(1 + block_ssz.len());
    out.push(verdict);
    out.extend_from_slice(block_ssz);
    Bytes::from(out)
}

/// Snapshot fork-choice scalars for a `FINALIZED_CHECKPOINT` payload (SSZ).
pub fn fork_choice_scalars_ssz<P: Preset>(
    store: &Store<P>,
    head_root: Root,
    head_slot: Slot,
) -> Bytes {
    let scalars = ForkChoiceScalarsPayload {
        time: store.time(),
        proposer_boost_root: store.proposer_boost_root(),
        justified: store.justified_checkpoint(),
        finalized: store.finalized_checkpoint(),
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        head_root,
        head_slot,
    };
    Bytes::from(scalars.as_ssz_bytes())
}

fn parent_of<P: Preset>(store: &Store<P>, root: Root) -> Option<Root> {
    if let Some(node) = store.proto_array().get(&root) {
        if let Some(p_idx) = node.parent {
            return store.proto_array().nodes().get(p_idx).map(|n| n.root);
        }
        return None;
    }
    store.blocks().get(&root).map(|h| h.parent_root)
}

fn slot_of<P: Preset>(store: &Store<P>, root: Root) -> Slot {
    store
        .proto_array()
        .get(&root)
        .map(|n| n.slot)
        .or_else(|| store.blocks().get(&root).map(|h| h.slot))
        .unwrap_or(Slot::new(0))
}

/// Common-ancestor slot of a reorg (for `CHAIN_REORG` payload; Architecture §4.2).
pub fn common_ancestor_slot<P: Preset>(store: &Store<P>, old_head: Root, new_head: Root) -> Slot {
    let bound = store
        .proto_array()
        .len()
        .saturating_add(store.blocks().len())
        .saturating_add(1);
    let mut new_chain: std::collections::HashMap<Root, Slot> = std::collections::HashMap::new();
    let mut cur = new_head;
    for _ in 0..bound {
        new_chain.insert(cur, slot_of(store, cur));
        match parent_of(store, cur) {
            Some(p) if p != cur && p != Root::ZERO => cur = p,
            _ => break,
        }
    }
    let mut cur = old_head;
    for _ in 0..bound {
        if let Some(&slot) = new_chain.get(&cur) {
            return slot;
        }
        match parent_of(store, cur) {
            Some(p) if p != cur && p != Root::ZERO => cur = p,
            _ => break,
        }
    }
    Slot::new(0)
}
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
///
/// `gossip_clock`: when set, a future `block.slot` inside
/// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` of *that* slot's start is admitted by
/// ticking the store to that slot start. Current-slot imports do not jump.
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
    gossip_clock: Option<GossipClock>,
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
        gossip_clock,
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
    // M13: emit cache vs registry lengths on the parent state *before* STF
    // so a short cache is visible even if on_block returns CachePoisoned.
    // Cheap gossip already required the header + `ensure_in_store`; a missing
    // state here is a programming bug, not a skippable scrape.
    let Some(parent_state) = store.block_state(&signed.message.parent_root) else {
        debug_assert!(
            false,
            "parent state resident after cheap-gossip ensure_in_store"
        );
        return Err(Status::internal(
            "parent state missing after cheap-gossip ensure_in_store",
        ));
    };
    metrics.observe_import_state(parent_state);
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
            // F2: arrival bytes — byte-identical to what crossed the RPC, not
            // a re-encode of the decoded container.
            &request.ssz,
            b.root,
            on_block_secs,
            early_accept,
        ),
        Ok(BlockImport::Deferred(DeferralReason::DataUnavailable)) => {
            // Park for re-drive when DataAvailable lands (CC-24d / §8.3).
            // Prefer arrival `request.ssz` (F2) for both parking and the event.
            let arrival_ssz = &request.ssz;
            if let Some(pending) = pending_da {
                let entry = PendingDaEntry {
                    root: true_root,
                    ssz: Bytes::copy_from_slice(arrival_ssz),
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
            // CC-44a / §4.2: same BLOCK_IMPORTED kind with deferred discriminator
            // so storage can write da_status in the same batch as the block.
            publish_deferred_block_event(
                event_tx,
                metrics,
                signed.message.slot.as_u64(),
                true_root,
                arrival_ssz,
            );
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
    gossip_clock: Option<GossipClock>,
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
    // Disparity admits *this* block's slot only — never an unconditional
    // on_tick into whatever slot comes next.
    if store.get_current_slot().as_u64() < block.slot.as_u64() {
        let admitted = gossip_clock.is_some_and(|clock| {
            admit_block_slot_if_within_disparity(
                store,
                block.slot.as_u64(),
                clock.now_millis,
                clock.disparity,
            )
        });
        if !admitted {
            metrics.inc_import_result(ImportResult::Invalid);
            return Ok(Some(ImportBlockResponse {
                // Proto has no FUTURE_SLOT verdict; reason drives stream IGNORE.
                verdict: ImportBlockVerdict::Invalid as i32,
                reason: "future_slot".into(),
            }));
        }
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
    // Unary ImportBlock uses the configured strategy (`CoreConfig::default` is
    // VerifyIndividual). Restore replay overrides via `on_block`, not this path.
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
    // Arrival `ImportBlockRequest.ssz` (F2 — not a re-encode).
    arrival_ssz: &[u8],
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
    // Capture prior finalized *before* overwriting the snapshot so we can emit
    // FINALIZED_CHECKPOINT only on a real change.
    let prev_finalized = head_store.load().finalized;
    *snapshot_sequence = snapshot_sequence.saturating_add(1);
    let optimistic = cc_fork_choice::is_optimistic_node(store);
    let finalized = store.finalized_checkpoint();
    let snapshot = HeadSnapshot {
        head_root,
        head_slot,
        head_state_root,
        justified: store.justified_checkpoint(),
        finalized,
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        current_epoch_target_root: Root::ZERO,
        dependent_root: Root::ZERO,
        // CC-3B: node-level optimistic from fork choice after fresh get_head.
        is_optimistic: optimistic,
        sequence: *snapshot_sequence,
    };
    head_store.store(snapshot);
    metrics.set_head(head_slot.as_u64(), 0, finalized.epoch.as_u64());
    metrics.is_optimistic.set(i64::from(optimistic));
    metrics
        .optimistic_nodes
        .set(store.proto_array().optimistic_node_count() as i64);

    // --- 9. event publish (backpressure; Phase 1 §7.3 / F1) ----------------
    // Data-carrying events use `blocking_send` so a slow events task applies
    // backpressure rather than silently dropping BLOCK_IMPORTED / FINALIZED.
    let finalized_state_root = store
        .blocks()
        .get(&finalized.root)
        .map(|h| h.state_root)
        .unwrap_or(Root::ZERO);
    publish_import_events(
        event_tx,
        metrics,
        store,
        slot,
        block_root,
        arrival_ssz,
        BLOCK_PAYLOAD_VERDICT_IMPORTED,
        head_root,
        head_slot,
        reorg,
        prev_finalized,
        finalized,
        finalized_state_root,
    );
    // Prune may have dropped optimistic side-branches; refresh after events.
    metrics
        .optimistic_nodes
        .set(store.proto_array().optimistic_node_count() as i64);
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

/// Publish import events with **backpressure** (Phase 1 §7.3 / F1).
///
/// Data-carrying events (`BLOCK_IMPORTED`, `FINALIZED_CHECKPOINT`) and the
/// accompanying HEAD/REORG use `blocking_send` on the core thread so a wedged
/// events task stalls the producer rather than silently dropping payload
/// bytes that storage needs for the same-transaction write-behind.
///
/// Payloads follow Architecture §4.2. `verdict_byte` is the first payload byte
/// of `BLOCK_IMPORTED` so `DEFERRED_DA` and `IMPORTED` share one kind.
/// `arrival_ssz` is the wire `ImportBlockRequest.ssz` (F2).
#[allow(clippy::too_many_arguments)]
fn publish_import_events<P: Preset>(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    store: &mut Store<P>,
    slot: u64,
    block_root: Root,
    arrival_ssz: &[u8],
    verdict_byte: u8,
    head_root: Root,
    head_slot: Slot,
    reorg: Option<ChainReorg>,
    prev_finalized: Checkpoint,
    finalized: Checkpoint,
    finalized_state_root: Root,
) {
    publish_event_blocking(
        event_tx,
        metrics,
        EventInput::block_imported_with_payload(
            slot,
            Bytes::copy_from_slice(block_root.as_slice()),
            block_imported_payload(verdict_byte, arrival_ssz),
        ),
    );
    publish_event_blocking(
        event_tx,
        metrics,
        EventInput::head(
            head_slot.as_u64(),
            Bytes::copy_from_slice(head_root.as_slice()),
        ),
    );
    if let Some(reorg) = reorg {
        let ancestor_slot = common_ancestor_slot(store, reorg.old_head, reorg.new_head);
        publish_event_blocking(
            event_tx,
            metrics,
            EventInput::chain_reorg(
                reorg.new_head_slot.as_u64(),
                Bytes::copy_from_slice(reorg.new_head.as_slice()),
                Bytes::copy_from_slice(reorg.old_head.as_slice()),
                ancestor_slot.as_u64(),
            ),
        );
    }
    if finalized != prev_finalized {
        // P0-10 / S0-A-21: proto-array prune hangs off FINALIZED, after the
        // REORG walk above still had the pre-prune tree (old head may be a
        // now-invalid side branch).
        store.prune_on_finalized();
        let scalars = fork_choice_scalars_ssz(store, head_root, head_slot);
        publish_event_blocking(
            event_tx,
            metrics,
            EventInput::finalized_checkpoint(
                finalized.epoch.as_u64(),
                Bytes::copy_from_slice(finalized.root.as_slice()),
                Bytes::copy_from_slice(finalized_state_root.as_slice()),
                scalars,
            ),
        );
    }
}

/// Publish a standalone `BLOCK_IMPORTED` for a DA-deferred import (same kind,
/// deferred discriminator — Architecture §4.2 rule 3). Uses backpressure (F1).
fn publish_deferred_block_event(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    slot: u64,
    block_root: Root,
    arrival_ssz: &[u8],
) {
    publish_event_blocking(
        event_tx,
        metrics,
        EventInput::block_imported_with_payload(
            slot,
            Bytes::copy_from_slice(block_root.as_slice()),
            block_imported_payload(BLOCK_PAYLOAD_VERDICT_DEFERRED_DA, arrival_ssz),
        ),
    );
}

/// Core-thread publish with backpressure (F1 / Phase 1 §7.3).
///
/// - Oversize payload → metric + **error** log; not enqueued (SEC-44a-2).
/// - Channel full → `blocking_send` waits (backpressure to the core).
/// - Channel closed → metric + **error** log (loud failure, not a silent drop).
fn publish_event_blocking(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    input: EventInput,
) {
    if !input.payload_within_cap() {
        metrics.inc_event_payload_rejected();
        tracing::error!(
            kind = ?input.kind,
            payload_len = input.payload.len(),
            cap = crate::events::MAX_EVENT_PAYLOAD_BYTES,
            "rejected oversize event payload before ring (SEC-44a-2)"
        );
        return;
    }
    match event_tx.blocking_send(input) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::SendError(lost)) => {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                slot = lost.slot,
                payload_len = lost.payload.len(),
                "events channel closed; lost data-carrying event (F1 loud path)"
            );
        }
    }
}

/// Test / ordering helper: non-blocking publish (does not apply backpressure).
fn try_publish_event(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    input: EventInput,
) {
    if !input.payload_within_cap() {
        metrics.inc_event_payload_rejected();
        tracing::error!(
            kind = ?input.kind,
            payload_len = input.payload.len(),
            "rejected oversize event payload (SEC-44a-2)"
        );
        return;
    }
    match event_tx.try_send(input) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(lost)) => {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                "events channel full; dropped event (test helper try_send)"
            );
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(lost)) => {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                "events channel closed; dropped event"
            );
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
/// Payload for `BLOCK_IMPORTED` is a minimal `[IMPORTED] ‖ []` so ordering tests
/// do not need a live store.
pub fn publish_snapshot_then_events(
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot: HeadSnapshot,
    slot: u64,
    block_root: Root,
) {
    let head_root = snapshot.head_root;
    let head_slot = snapshot.head_slot;
    head_store.store(snapshot);
    // Ordering-only helper: no store → skip reorg/finalized; payload disc only.
    try_publish_event(
        event_tx,
        metrics,
        EventInput::block_imported_with_payload(
            slot,
            Bytes::copy_from_slice(block_root.as_slice()),
            block_imported_payload(BLOCK_PAYLOAD_VERDICT_IMPORTED, &[]),
        ),
    );
    try_publish_event(
        event_tx,
        metrics,
        EventInput::head(
            head_slot.as_u64(),
            Bytes::copy_from_slice(head_root.as_slice()),
        ),
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_fork_choice::{
        ExecutionStatus, HarnessAvailability, ProtoNodeBlock, get_forkchoice_store,
    };
    use cc_types::containers::BeaconBlockHeader;
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, Hash256, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState};
    use prometheus_client::registry::Registry;
    use tokio::sync::mpsc;
    use tree_hash::TreeHash;

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
        assert_eq!(ev.kind, cc_proto::chain::EventKind::BlockImported);
        assert_eq!(
            ev.payload.first().copied(),
            Some(BLOCK_PAYLOAD_VERDICT_IMPORTED)
        );
        let ev = rx.recv().await.expect("head");
        assert_eq!(ev.kind, cc_proto::chain::EventKind::Head);
        assert_eq!(ev.payload.len(), 8);
        assert_eq!(head.load().sequence, 3);
    }

    /// F3: `ForkChoiceScalarsPayload` layout matches store meta field order/size.
    ///
    /// Storage decodes with `cc_store::meta::ForkChoiceScalars` — both must be
    /// 240-byte fixed containers with identical field order (see type doc).
    #[test]
    fn fork_choice_scalars_payload_layout_matches_store_meta() {
        use cc_types::primitives::Epoch;

        let v = ForkChoiceScalarsPayload {
            time: 0x0102_0304_0506_0708,
            proposer_boost_root: Root::from_array([0x11; 32]),
            justified: Checkpoint {
                epoch: Epoch::new(3),
                root: Root::from_array([0x22; 32]),
            },
            finalized: Checkpoint {
                epoch: Epoch::new(2),
                root: Root::from_array([0x33; 32]),
            },
            unrealized_justified: Checkpoint {
                epoch: Epoch::new(3),
                root: Root::from_array([0x44; 32]),
            },
            unrealized_finalized: Checkpoint {
                epoch: Epoch::new(1),
                root: Root::from_array([0x55; 32]),
            },
            head_root: Root::from_array([0x66; 32]),
            head_slot: Slot::new(99),
        };
        let bytes = v.as_ssz_bytes();
        assert_eq!(
            bytes.len(),
            FORK_CHOICE_SCALARS_SSZ_LEN,
            "fixed SSZ length must match store ForkChoiceScalars (240 B)"
        );
        // Field order: time | proposer_boost | justified | finalized | …
        assert_eq!(&bytes[0..8], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&bytes[8..40], &[0x11u8; 32]);
        assert_eq!(&bytes[40..48], &3u64.to_le_bytes()); // justified.epoch
        assert_eq!(&bytes[48..80], &[0x22u8; 32]); // justified.root
        assert_eq!(&bytes[80..88], &2u64.to_le_bytes()); // finalized.epoch
        assert_eq!(&bytes[88..120], &[0x33u8; 32]);
        // head_slot is the last 8 bytes.
        assert_eq!(&bytes[232..240], &99u64.to_le_bytes());
        // Default encodes to the same length (store Default path).
        assert_eq!(
            ForkChoiceScalarsPayload::default().as_ssz_bytes().len(),
            FORK_CHOICE_SCALARS_SSZ_LEN
        );
    }

    /// SEC-44a-2: oversize payload is rejected with metric, not enqueued.
    #[test]
    fn oversize_payload_rejected_before_enqueue() {
        use crate::events::MAX_EVENT_PAYLOAD_BYTES;

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let (tx, mut rx) = mpsc::channel(4);
        let huge = vec![0u8; MAX_EVENT_PAYLOAD_BYTES + 1];
        let input = EventInput::block_imported_with_payload(
            1,
            Bytes::from(vec![0u8; 32]),
            Bytes::from(huge),
        );
        assert!(!input.payload_within_cap());
        try_publish_event(&tx, &metrics, input);
        assert_eq!(metrics.event_payload_rejected_count(), 1);
        assert!(
            rx.try_recv().is_err(),
            "oversize must not enter the channel"
        );
    }

    /// F2: block_imported_payload preserves arrival bytes after the discriminator.
    #[test]
    fn block_imported_payload_preserves_arrival_bytes() {
        let arrival = b"wire-ssz-bytes-not-reencoded";
        let payload = block_imported_payload(BLOCK_PAYLOAD_VERDICT_IMPORTED, arrival);
        assert_eq!(payload[0], BLOCK_PAYLOAD_VERDICT_IMPORTED);
        assert_eq!(&payload[1..], arrival.as_slice());
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

    /// M13 / F2: empty pubkey cache + non-empty registry must set the alert
    /// through `import_block_with_early`, not only a source-order pin.
    #[test]
    fn import_block_with_early_fires_m13_when_parent_cache_empty() {
        use crate::residency::Residency;
        use cc_fork_choice::{HarnessAvailability, get_forkchoice_store, on_tick};
        use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
        use cc_types::containers::Validator;
        use cc_types::primitives::{BlsPublicKey, ExecutionAddress, ForkVersion};
        use std::sync::Arc;

        #[derive(Debug, Default, Clone, Copy)]
        struct M13AcceptEngine;
        impl<P: Preset> cc_state_transition::ExecutionEngine<P> for M13AcceptEngine {
            fn verify_and_notify_new_payload(
                &self,
                _request: cc_state_transition::NewPayloadRequest<'_, P>,
            ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError>
            {
                Ok(cc_state_transition::PayloadStatus::Valid)
            }
        }

        let config = ChainConfig {
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
        };

        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        for i in 0u8..3 {
            let mut raw = [0u8; 48];
            raw[0] = i.saturating_add(1);
            state
                .validators_push(Validator {
                    pubkey: BlsPublicKey::from_array(raw),
                    ..Validator::default()
                })
                .unwrap();
        }
        assert!(state.caches().pubkeys.is_empty());
        assert!(state.validators_len() > 0);

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
            Arc::new(M13AcceptEngine),
            Arc::new(HarnessAvailability),
            config.seconds_per_slot,
        )
        .unwrap();
        on_tick(&mut store, config.seconds_per_slot * 2).unwrap();
        let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        let parent = store.block_state(&anchor_root).unwrap();
        assert!(parent.caches().pubkeys.is_empty());
        assert!(parent.validators_len() > 0);

        let child = SignedBeaconBlock::<Minimal> {
            message: BeaconBlock {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor_root,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let true_root = Root::from_hash256(TreeHash::tree_hash_root(&child.message));
        let request = ImportBlockRequest {
            ssz: encode_signed_block(&child),
            fork: 0,
            root: true_root.as_slice().to_vec(),
            source: 0,
        };

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (event_tx, _) = mpsc::channel(4);
        let counters = ImportCounters::default();
        let mut residency = Residency::<Minimal>::new(64, 32);
        let mut snap_seq = 0u64;
        let _ = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            request,
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(
            metrics.pubkey_cache_alert_firing(),
            "empty pubkey cache vs non-empty registry must fire M13"
        );
    }

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

    fn test_root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
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
            .on_block(ProtoNodeBlock {
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

    fn drain_finalized(rx: &mut mpsc::Receiver<EventInput>) -> usize {
        let mut n = 0;
        while let Ok(ev) = rx.try_recv() {
            if ev.kind == cc_proto::chain::EventKind::FinalizedCheckpoint {
                n += 1;
            }
        }
        n
    }

    /// P0-10 / S0-A-21: two FINALIZED publishes shrink the proto-array.
    #[test]
    fn two_finalized_events_decrease_proto_array_node_count() {
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
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        let a = test_root(0xA1);
        let b = test_root(0xB1);
        let side = test_root(0x51);
        let c = test_root(0xC1);
        let d = test_root(0xD1);
        insert_child(&mut store, anchor, a, 1);
        insert_child(&mut store, a, b, 8);
        insert_child(&mut store, a, side, 8);
        insert_child(&mut store, b, c, 9);
        let before_first = store.proto_array().len();
        assert_eq!(before_first, 5);

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let (tx, mut rx) = mpsc::channel(16);
        let prev0 = store.finalized_checkpoint();
        store.update_checkpoints(
            Checkpoint {
                epoch: Epoch::new(1),
                root: b,
            },
            Checkpoint {
                epoch: Epoch::new(1),
                root: b,
            },
        );
        let f1 = store.finalized_checkpoint();
        publish_import_events(
            &tx,
            &metrics,
            &mut store,
            9,
            c,
            &[],
            BLOCK_PAYLOAD_VERDICT_IMPORTED,
            c,
            Slot::new(9),
            None,
            prev0,
            f1,
            Root::ZERO,
        );
        let after_first = store.proto_array().len();
        assert!(
            after_first < before_first,
            "first FINALIZED must prune (before={before_first} after={after_first})"
        );
        assert_eq!(drain_finalized(&mut rx), 1);
        assert!(!store.proto_array().contains(&side));

        insert_child(&mut store, c, d, 16);
        let before_second = store.proto_array().len();
        let prev1 = store.finalized_checkpoint();
        store.update_checkpoints(
            Checkpoint {
                epoch: Epoch::new(2),
                root: d,
            },
            Checkpoint {
                epoch: Epoch::new(2),
                root: d,
            },
        );
        let f2 = store.finalized_checkpoint();
        publish_import_events(
            &tx,
            &metrics,
            &mut store,
            16,
            d,
            &[],
            BLOCK_PAYLOAD_VERDICT_IMPORTED,
            d,
            Slot::new(16),
            None,
            prev1,
            f2,
            Root::ZERO,
        );
        let after_second = store.proto_array().len();
        assert!(
            after_second < before_second,
            "second FINALIZED must prune (before={before_second} after={after_second})"
        );
        assert_eq!(drain_finalized(&mut rx), 1);
        assert!(store.proto_array().contains(&d));
        assert!(!store.proto_array().contains(&b));
    }
}
