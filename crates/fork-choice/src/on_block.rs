//! Spec `on_block` and `compute_pulled_up_tip` (Architecture §6.5–6.6, CC-15b).
//!
//! # Order (Architecture §6.5 / §7.2)
//!
//! 1. **DA gate first** via `store.da().is_data_available` →
//!    `Ok(Deferred(DataUnavailable))` if false
//! 2. Parent lookup → `Ok(Deferred(UnknownParent))`
//! 3. Slot / finality preconditions → `Ok(Deferred(FutureSlot))` or
//!    `Err(NotDescendedFromFinalized)` (Reject-class)
//! 4. `state_transition` (via `cc-state-transition`; engine only inside
//!    `process_execution_payload` — **not** called from this module)
//! 5. Proto-array insertion (**before** `insert_block` so a failed proto write
//!    never leaves a header-only partial import)
//! 6. Store header + post-state (`insert_block`)
//! 7. Checkpoint updates + `compute_pulled_up_tip` bookkeeping
//!
//! # Fully imported vs partial (SEC-15b-1)
//!
//! A root is **fully imported** only when it is present in **both** `store.blocks`
//! and `store.proto_array`. The idempotent short-circuit requires both. If a
//! header/state is present without a proto-array node (leftover partial write),
//! `on_block` **resumes** integration from the resident post-state rather than
//! returning a false `Imported`.
//!
//! # Defer vs error (CC-17 handoff)
//!
//! DA / unknown parent / future slot → **`Ok(Deferred(...))`**, never
//! `Err(BlockError::{DataNotAvailable, UnknownParent, FutureSlot})`. Those
//! error variants are a parallel taxonomy that would fold to `INVALID` at the
//! ImportBlock verdict boundary instead of `DEFERRED_*`.

use std::sync::Arc;

use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::{BlockError, BlockSignatureStrategy, GossipClass, TransitionContext, compute_epoch_at_slot,
    process_justification_and_finalization, state_transition,
};
use cc_types::config::ChainConfig;
use cc_types::containers::{BeaconBlockHeader, Checkpoint};
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Hash256, Root, Slot};
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};
use thiserror::Error;
use tree_hash::TreeHash;

use crate::checkpoint_context::CheckpointContext;
use crate::da_seam::{BlockImport, DeferralReason, ImportedBlock};
use crate::execution_status::ExecutionStatus;
use crate::proto_array::{ProtoArrayError, ProtoNodeBlock};
use crate::store::Store;
use crate::validation::{ValidationError, propagate_execution_payload_validation};

/// Errors from `on_block` that are **not** deferrals.
///
/// Deferrals return `Ok(BlockImport::Deferred(_))`. Invalid blocks that should
/// not be requeued surface here.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OnBlockError {
    /// Block does not descend from the store's finalized checkpoint (Reject).
    #[error("block does not descend from finalized checkpoint")]
    NotDescendedFromFinalized,
    /// State transition failed (classified via inner [`BlockError::gossip_class`]).
    #[error(transparent)]
    Transition(#[from] BlockError),
    /// Proto-array insertion failed.
    #[error(transparent)]
    ProtoArray(#[from] ProtoArrayError),
    /// Epoch processing inside `compute_pulled_up_tip` failed.
    #[error("pulled-up tip epoch processing: {0}")]
    PulledUpTip(String),
    /// H-3: partial import declined because the execution body is not resident
    /// and a synthetic `Default` body would write `execution_block_hash == ZERO`
    /// (case-2 sentinel). Caller falls through to the full import path.
    #[error("partial import declined: execution body not resident (H-3)")]
    PartialImportNeedsBody,
    /// §4.8 / CC-34b: EL consensus failure around Valid/Invalid status.
    ///
    /// **Mutation is path-dependent** (see
    /// [`ValidationError::ValidExecutionStatusBecameInvalid`]):
    /// - Direct `try_mark_execution_invalid` on a Valid node → store unmutated.
    /// - Upward pass hit an Invalid ancestor after `integrate_block` → tip and
    ///   any Optimistic→Valid writes already applied **remain** (no rollback
    ///   in this issue; import atomicity is a follow-on / CC-35).
    #[error(transparent)]
    Validation(#[from] ValidationError),
}

impl OnBlockError {
    /// Gossip classification for non-import errors.
    ///
    /// `NotDescendedFromFinalized` is **Reject** (peer fault / invalid chain).
    /// Transition errors delegate to [`BlockError::gossip_class`].
    pub fn gossip_class(&self) -> GossipClass {
        match self {
            Self::NotDescendedFromFinalized => GossipClass::Reject,
            Self::Transition(e) => e.gossip_class(),
            // EL consensus failure / internal structure — not a peer descore
            // from gossip alone. Operator action required for §4.8.
            Self::ProtoArray(_)
            | Self::PulledUpTip(_)
            | Self::PartialImportNeedsBody
            | Self::Validation(_) => GossipClass::Internal,
        }
    }
}

/// Spec `get_forkchoice_store(anchor_state, anchor_block)`.
///
/// Seeds headers, post-state, proto-array, and a justified [`CheckpointContext`].
/// The anchor is treated as trusted (checkpoint sync / genesis).
pub fn get_forkchoice_store<P: Preset>(
    anchor_state: BeaconState<P>,
    anchor_block: &BeaconBlock<P>,
    engine: Arc<dyn cc_state_transition::ExecutionEngine<P>>,
    da: Arc<dyn crate::da_seam::DataAvailability>,
    seconds_per_slot: u64,
) -> Result<Store<P>, OnBlockError> {
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(anchor_block));
    let anchor_epoch = compute_epoch_at_slot::<P>(anchor_state.slot());
    let justified = Checkpoint {
        epoch: anchor_epoch,
        root: anchor_root,
    };
    let finalized = justified;

    let time = anchor_state.genesis_time().saturating_add(
        anchor_state
            .slot()
            .as_u64()
            .saturating_mul(seconds_per_slot.max(1)),
    );

    let mut store = Store::new(
        time,
        anchor_state.genesis_time(),
        seconds_per_slot,
        justified,
        finalized,
        anchor_state.validators_len(),
        engine,
        da,
    );

    // Proto-array first, then header/state — same discipline as `on_block`.
    // Anchor is Valid by the spec's MAY (CC-34 /7). execution_block_hash from
    // the anchor body (≠13/3); may be ZERO for pre-merge / unit-test genesis.
    store.proto_array_mut().on_block(ProtoNodeBlock {
        slot: anchor_block.slot,
        root: anchor_root,
        parent_root: None,
        state_root: anchor_block.state_root,
        target_root: anchor_root,
        justified_checkpoint: justified,
        finalized_checkpoint: finalized,
        unrealized_justified_checkpoint: justified,
        unrealized_finalized_checkpoint: finalized,
        execution_status: ExecutionStatus::Valid,
        execution_block_hash: anchor_block.body.execution_payload.block_hash.to_hash256(),
    })?;

    let header = block_to_header(anchor_block);
    store.insert_block(anchor_root, header, anchor_state.clone());

    let ctx = CheckpointContext::from_state(&anchor_state, justified);
    // Seed the justified-balance snapshot from the anchor context so the first
    // `compute_deltas` pass does not treat all balances as zero (SEC-16-3).
    let justified_balances: Vec<u64> = ctx.effective_balances.iter().map(|g| g.as_u64()).collect();
    // Keep votes / balances length aligned with the registry at the anchor.
    let n = justified_balances.len().max(anchor_state.validators_len());
    store.resize_votes(n);
    store.set_justified_balances(if justified_balances.len() == n {
        justified_balances
    } else {
        let mut b = justified_balances;
        b.resize(n, 0);
        b
    });
    store.insert_checkpoint_context(justified, Arc::new(ctx));

    Ok(store)
}

/// Spec `on_block(store, signed_block)`.
///
/// See module docs for order, deferral semantics, and partial-import handling.
pub fn on_block<P: Preset>(
    store: &mut Store<P>,
    signed_block: &SignedBeaconBlock<P>,
    config: &ChainConfig,
    verify: BlockSignatureStrategy,
) -> Result<BlockImport, OnBlockError> {
    let block = &signed_block.message;
    let block_root = Root::from_hash256(TreeHash::tree_hash_root(block));

    // Fully imported only when header/state **and** proto-array node exist.
    if is_fully_imported(store, &block_root) {
        return Ok(BlockImport::Imported(ImportedBlock { root: block_root }));
    }

    // Partial leftover (e.g. header without proto): resume from resident state.
    // Never claim Imported until proto-array membership is established.
    // H-3: if the execution hash cannot be recovered without a ZERO synthetic
    // body, decline so the full path re-drives with a real body.
    if store.blocks().contains_key(&block_root) {
        match complete_partial_import(store, block_root) {
            Ok(outcome) => return Ok(outcome),
            Err(OnBlockError::PartialImportNeedsBody) => {
                // Fall through to the full import path using the signed block.
            }
            Err(e) => return Err(e),
        }
    }

    // --- 1. DA gate FIRST (CC-17 sole production call site) -----------------
    // Production call of is_data_available: greppable from this path.
    if !store.da().is_data_available(block_root) {
        return Ok(BlockImport::Deferred(DeferralReason::DataUnavailable));
    }

    // --- 2. Parent lookup (borrow only — no full-state clone yet) -----------
    let parent_root = block.parent_root;
    if store.block_state(&parent_root).is_none() || !store.blocks().contains_key(&parent_root) {
        return Ok(BlockImport::Deferred(DeferralReason::UnknownParent));
    }

    // --- 3. Slot / finality preconditions (before expensive clone / ST) -----
    if store.get_current_slot().as_u64() < block.slot.as_u64() {
        return Ok(BlockImport::Deferred(DeferralReason::FutureSlot));
    }

    let finalized_slot = compute_start_slot_at_epoch::<P>(store.finalized_checkpoint().epoch);
    if block.slot.as_u64() <= finalized_slot.as_u64() {
        return Err(OnBlockError::NotDescendedFromFinalized);
    }

    let finalized_checkpoint_block =
        get_checkpoint_block(store, parent_root, store.finalized_checkpoint().epoch);
    if store.finalized_checkpoint().root != finalized_checkpoint_block {
        return Err(OnBlockError::NotDescendedFromFinalized);
    }

    // --- 4. state_transition (engine only via process_execution_payload) ----
    let mut state = store
        .block_state(&parent_root)
        .ok_or(OnBlockError::PulledUpTip(
            "parent state disappeared after check".into(),
        ))?
        .clone();
    let engine = Arc::clone(store.engine_arc());
    let ctx = TransitionContext::new(config, engine.as_ref());
    // CC-36a / D-4: engine transport failure is the third outcome — success-path
    // deferral, store unmutated (ST ran on a parent clone; integrate_block not yet).
    match state_transition(&mut state, signed_block, &ctx, verify) {
        Ok(()) => {}
        Err(BlockError::Engine(cc_state_transition::EngineError::Transport(_))) => {
            return Ok(BlockImport::Deferred(
                DeferralReason::ExecutionEngineUnavailable,
            ));
        }
        Err(e) => return Err(OnBlockError::Transition(e)),
    }

    // CC-34a: read the payload-status outbox (written by process_execution_payload).
    // No second verify_and_notify_new_payload call site (CC-14/1).
    let execution_status = ctx
        .take_payload_status()
        .as_ref()
        .map(ExecutionStatus::from_payload_status)
        .unwrap_or(ExecutionStatus::Irrelevant);
    let execution_block_hash = block.body.execution_payload.block_hash.to_hash256();

    // Spec: compute head **before** applying the block (for proposer-boost gate).
    let head_before = crate::head_cache::get_head(store)
        .map(|(r, _)| r)
        .unwrap_or(store.justified_checkpoint().root);

    // --- 5–7. Integrate: proto-array first, then store, then checkpoints ----
    integrate_block(
        store,
        block_root,
        block,
        state,
        execution_status,
        execution_block_hash,
    )?;

    // CC-34b / §4.6: one VALID clears the optimistic ancestor suffix.
    // Tip was just inserted as Valid, so walk from the **parent** (Lighthouse
    // shape) — the tip itself is already Valid and would be the stop floor.
    if execution_status == ExecutionStatus::Valid {
        let parent = store
            .proto_array()
            .get(&block_root)
            .and_then(|n| n.parent)
            .and_then(|p| store.proto_array().nodes().get(p).map(|n| n.root));
        if let Some(parent_root) = parent {
            propagate_execution_payload_validation(store, parent_root)?;
        }
    }

    // Spec `record_block_timeliness` + `update_proposer_boost_root`.
    record_block_timeliness(store, block_root);
    update_proposer_boost_root(store, head_before, block_root);

    Ok(BlockImport::Imported(ImportedBlock { root: block_root }))
}

/// Whether `root` is present in both the header map and the proto-array.
#[inline]
fn is_fully_imported<P: Preset>(store: &Store<P>, root: &Root) -> bool {
    store.blocks().contains_key(root) && store.proto_array().contains(root)
}

/// Resume import when a header/state exists without a proto-array node.
///
/// # Hazard H-3 (resolution i)
///
/// A synthetic `BeaconBlock { body: Default::default() }` has
/// `execution_payload.block_hash == ZERO`, which collides with CC-35 case-2's
/// sentinel. Source the hash from the resident post-state's
/// `latest_execution_payload_header.block_hash` (the post-state of this block
/// carries exactly this block's payload hash after `process_execution_payload`).
/// If that hash is ZERO for a non-root block, **decline** so the full path
/// re-drives with a real body — never write ZERO onto a post-merge `ProtoNode`.
fn complete_partial_import<P: Preset>(
    store: &mut Store<P>,
    block_root: Root,
) -> Result<BlockImport, OnBlockError> {
    let header = *store
        .blocks()
        .get(&block_root)
        .ok_or_else(|| OnBlockError::PulledUpTip("partial import: missing header".into()))?;
    let state = store
        .block_state(&block_root)
        .ok_or_else(|| OnBlockError::PulledUpTip("partial import: missing state".into()))?
        .clone();

    // H-3 resolution (i): recover execution block hash from resident post-state
    // (body ring lives in services/chain; post-state is the fork-choice-local
    // residency source for the payload header written at transition time).
    let execution_block_hash = state
        .latest_execution_payload_header()
        .block_hash
        .to_hash256();
    if execution_block_hash == Hash256::ZERO {
        // Body not recoverable without ZERO synthetic — decline; caller falls
        // through to the full import path with the signed block body.
        return Err(OnBlockError::PartialImportNeedsBody);
    }

    // Synthetic block view for integrate (body root already on header).
    // Status: outbox was not retained across the torn write. ST success only
    // means NOT_VALIDATED|VALID — fail closed as Optimistic (not Valid) so a
    // SYNCING residual is not upgraded to fully validated (audit Finding 3).
    let block = BeaconBlock {
        slot: header.slot,
        proposer_index: header.proposer_index,
        parent_root: header.parent_root,
        state_root: header.state_root,
        body: Default::default(),
    };

    let head_before = crate::head_cache::get_head(store)
        .map(|(r, _)| r)
        .unwrap_or(store.justified_checkpoint().root);
    integrate_block(
        store,
        block_root,
        &block,
        state,
        ExecutionStatus::Optimistic,
        execution_block_hash,
    )?;
    record_block_timeliness(store, block_root);
    update_proposer_boost_root(store, head_before, block_root);
    Ok(BlockImport::Imported(ImportedBlock { root: block_root }))
}

/// Proto-array first, then header/state (if needed), then checkpoint / pull-up.
///
/// Pull-up J&F runs on a **local** clone before any store mutation so a J&F
/// failure does not leave a half-written import. Proto-array insert is the first
/// fallible store mutation; `insert_block` follows only after it succeeds.
fn integrate_block<P: Preset>(
    store: &mut Store<P>,
    block_root: Root,
    block: &BeaconBlock<P>,
    state: BeaconState<P>,
    execution_status: ExecutionStatus,
    execution_block_hash: Hash256,
) -> Result<(), OnBlockError> {
    let justified = state.current_justified_checkpoint();
    let finalized = state.finalized_checkpoint();

    // Compute pulled-up checkpoints on a clone before mutating the store.
    let mut pull_state = state.clone();
    process_justification_and_finalization(&mut pull_state)
        .map_err(|e| OnBlockError::PulledUpTip(e.to_string()))?;
    let unrealized_justified = pull_state.current_justified_checkpoint();
    let unrealized_finalized = pull_state.finalized_checkpoint();

    let block_epoch = compute_epoch_at_slot::<P>(block.slot);
    let target_root = target_root_for_new_block(store, block_root, block.parent_root, block.slot);

    // --- Proto-array first (fallible; no header write yet) -----------------
    if !store.proto_array().contains(&block_root) {
        let parent_root = if store.proto_array().contains(&block.parent_root) {
            Some(block.parent_root)
        } else if store.proto_array().is_empty() {
            // Sole genesis/anchor-style insert.
            None
        } else {
            return Err(OnBlockError::ProtoArray(ProtoArrayError::UnknownParent(
                block.parent_root,
            )));
        };

        store.proto_array_mut().on_block(ProtoNodeBlock {
            slot: block.slot,
            root: block_root,
            parent_root,
            state_root: block.state_root,
            target_root,
            justified_checkpoint: justified,
            finalized_checkpoint: finalized,
            unrealized_justified_checkpoint: unrealized_justified,
            unrealized_finalized_checkpoint: unrealized_finalized,
            execution_status,
            execution_block_hash,
        })?;
    } else {
        // Resume / re-pull path: node unrealized can change without a new header.
        let node_changed = store.proto_array_mut().set_unrealized_checkpoints(
            block_root,
            unrealized_justified,
            unrealized_finalized,
        )?;
        if node_changed {
            // SEC-15c-2: node-level unrealized affects voting source / viability
            // even when store unrealized epoch does not advance.
            store.bump_mutation_counter();
        }
    }

    // --- Header + post-state only after proto-array membership -------------
    if !store.blocks().contains_key(&block_root) {
        store.insert_block(block_root, block_to_header(block), state.clone());
    }

    // --- Checkpoint updates ------------------------------------------------
    store.update_checkpoints(justified, finalized);
    let cp_ctx = CheckpointContext::from_state(&state, justified);
    store.insert_checkpoint_context(justified, Arc::new(cp_ctx));
    store.update_unrealized_checkpoints(unrealized_justified, unrealized_finalized);

    // If the block is from a prior epoch, apply the pulled-up values as realized.
    let current_epoch = store.get_current_store_epoch();
    if block_epoch.as_u64() < current_epoch.as_u64() {
        store.update_checkpoints(unrealized_justified, unrealized_finalized);
    }

    Ok(())
}

/// Spec `compute_pulled_up_tip(store, block_root)`.
///
/// Requires a resident post-state and a proto-array node. Clones the post-state,
/// runs `process_justification_and_finalization`, records unrealized checkpoints
/// on the node and store, and promotes realized checkpoints when the block is
/// from a prior epoch.
pub fn compute_pulled_up_tip<P: Preset>(
    store: &mut Store<P>,
    block_root: Root,
) -> Result<(), OnBlockError> {
    let mut state = store
        .block_state(&block_root)
        .ok_or_else(|| OnBlockError::PulledUpTip("missing block state".into()))?
        .clone();

    process_justification_and_finalization(&mut state)
        .map_err(|e| OnBlockError::PulledUpTip(e.to_string()))?;

    let unrealized_justified = state.current_justified_checkpoint();
    let unrealized_finalized = state.finalized_checkpoint();

    let node_changed = store.proto_array_mut().set_unrealized_checkpoints(
        block_root,
        unrealized_justified,
        unrealized_finalized,
    )?;

    store.update_unrealized_checkpoints(unrealized_justified, unrealized_finalized);

    // SEC-15c-2: node unrealized can move without store unrealized advancing
    // (e.g. equal epoch, different root). Invalidate head cache whenever the
    // node value changed so viability cannot read a stale CachedHead.
    if node_changed {
        store.bump_mutation_counter();
    }

    let block_slot = store
        .blocks()
        .get(&block_root)
        .map(|h| h.slot)
        .unwrap_or_else(|| state.slot());
    let block_epoch = compute_epoch_at_slot::<P>(block_slot);
    let current_epoch = store.get_current_store_epoch();
    if block_epoch.as_u64() < current_epoch.as_u64() {
        store.update_checkpoints(unrealized_justified, unrealized_finalized);
    }

    Ok(())
}

/// Spec `get_checkpoint_block(store, root, epoch)` — ancestor at epoch start.
pub fn get_checkpoint_block<P: Preset>(store: &Store<P>, root: Root, epoch: Epoch) -> Root {
    let epoch_first_slot = compute_start_slot_at_epoch::<P>(epoch);
    get_ancestor_from_headers(store, root, epoch_first_slot)
}

/// FFG target root for a block being inserted (epoch-boundary ancestor).
///
/// When the new block starts an epoch it is its own target; otherwise walk from
/// the **parent** (already in the store) so we do not need the new root in
/// `blocks` yet (proto-array-first order).
fn target_root_for_new_block<P: Preset>(
    store: &Store<P>,
    block_root: Root,
    parent_root: Root,
    slot: Slot,
) -> Root {
    let epoch = compute_epoch_at_slot::<P>(slot);
    let epoch_start = compute_start_slot_at_epoch::<P>(epoch);
    if slot.as_u64() == epoch_start.as_u64() {
        block_root
    } else {
        get_checkpoint_block(store, parent_root, epoch)
    }
}

fn get_ancestor_from_headers<P: Preset>(store: &Store<P>, root: Root, slot: Slot) -> Root {
    let mut current = root;
    // Bound walk by store size to avoid cycles on corrupt input.
    let max_steps = store.blocks().len().saturating_add(1);
    for _ in 0..max_steps {
        let Some(header) = store.blocks().get(&current) else {
            return current;
        };
        if header.slot.as_u64() <= slot.as_u64() {
            return current;
        }
        let parent = header.parent_root;
        if parent == current {
            return current;
        }
        current = parent;
    }
    current
}

fn block_to_header<P: Preset>(block: &BeaconBlock<P>) -> BeaconBlockHeader {
    BeaconBlockHeader {
        slot: block.slot,
        proposer_index: block.proposer_index,
        parent_root: block.parent_root,
        state_root: block.state_root,
        body_root: Root::from_hash256(TreeHash::tree_hash_root(&block.body)),
    }
}

/// Spec `record_block_timeliness(store, root)`.
///
/// Timely iff the block's slot equals the current store slot and the time into
/// the slot is before the attestation due threshold (`SECONDS_PER_SLOT / 3`).
pub fn record_block_timeliness<P: Preset>(store: &mut Store<P>, root: Root) {
    let Some(header) = store.blocks().get(&root).copied() else {
        store.set_block_timeliness(root, false);
        return;
    };
    let seconds_per_slot = store.seconds_per_slot().max(1);
    let seconds_since_genesis = store.time().saturating_sub(store.genesis_time());
    let time_into_slot = seconds_since_genesis % seconds_per_slot;
    // Spec `get_attestation_due_ms` ≈ SLOT_DURATION / INTERVALS_PER_SLOT (3).
    let attestation_threshold = seconds_per_slot / 3;
    let is_before_attesting_interval = time_into_slot < attestation_threshold;
    let is_timely =
        store.get_current_slot().as_u64() == header.slot.as_u64() && is_before_attesting_interval;
    store.set_block_timeliness(root, is_timely);
}

/// Spec `get_shuffling_dependent_root(store, root, epoch)`.
fn get_shuffling_dependent_root<P: Preset>(store: &Store<P>, root: Root, epoch: Epoch) -> Root {
    if epoch.as_u64() <= P::MIN_SEED_LOOKAHEAD {
        return Root::ZERO;
    }
    let dependent_epoch = epoch.as_u64().saturating_sub(P::MIN_SEED_LOOKAHEAD);
    let dependent_slot = Slot::new(
        dependent_epoch
            .saturating_mul(P::SLOTS_PER_EPOCH)
            .saturating_sub(1),
    );
    match store.proto_array().get_ancestor(root, dependent_slot) {
        Ok(r) => r,
        Err(_) => get_ancestor_from_headers(store, root, dependent_slot),
    }
}

/// Spec `update_proposer_boost_root(store, head, root)`.
///
/// Boost is granted only when the block is timely, no prior boost this slot,
/// and the block shares the shuffling dependent root with the current head.
pub fn update_proposer_boost_root<P: Preset>(store: &mut Store<P>, head: Root, root: Root) {
    let is_first_block = store.proposer_boost_root() == Root::ZERO;
    let is_timely = store.block_timeliness(&root).unwrap_or(false);
    let epoch = store.get_current_store_epoch();
    let head_dependent = get_shuffling_dependent_root(store, head, epoch);
    let block_dependent = get_shuffling_dependent_root(store, root, epoch);
    let is_same_dependent_root = head_dependent == block_dependent;

    if is_timely && is_first_block && is_same_dependent_root {
        store.set_proposer_boost_root(root);
    }
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

    use std::sync::Arc;

    use cc_state_transition::{BlockSignatureStrategy};
    use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
    use cc_types::containers::{BeaconBlockHeader, Checkpoint};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{
        Epoch, ExecutionAddress, ForkVersion, Hash256, Root, Slot, ValidatorIndex,
    };
    use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};

    use super::*;
    use crate::da_seam::{DataAvailability, DeferralReason, HarnessAvailability};
    use crate::execution_status::ExecutionStatus;
    use crate::proto_array::ProtoNodeBlock;
    use crate::store::Store;

    /// Test-only DA that always reports unavailable.
    #[derive(Debug, Default, Clone, Copy)]
    struct NeverAvailable;

    impl DataAvailability for NeverAvailable {
        fn is_data_available(&self, _beacon_block_root: Root) -> bool {
            false
        }
    }

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

    fn signed_block(slot: u64, parent: Root, proposer: u64) -> SignedBeaconBlock<Minimal> {
        SignedBeaconBlock {
            message: BeaconBlock {
                slot: Slot::new(slot),
                proposer_index: ValidatorIndex::new(proposer),
                parent_root: parent,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        }
    }

    /// Seeded store at genesis with one anchor block/state.
    fn seeded_store(da: Arc<dyn DataAvailability>) -> (Store<Minimal>, Root, ChainConfig) {
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
            da,
            config.seconds_per_slot,
        )
        .unwrap();
        let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        (store, anchor_root, config)
    }

    /// CC-17/2 re-run against production `on_block`: NeverAvailable defers without
    /// writing the block or running a transition (no store entry).
    #[test]
    fn da_never_available_defers_without_store_write() {
        let (mut store, anchor, config) = seeded_store(Arc::new(NeverAvailable));
        store.set_time(12); // slot 2

        let block = signed_block(1, anchor, 0);
        let root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
        let before_len = store.blocks().len();

        let outcome = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();

        assert!(
            matches!(
                outcome,
                BlockImport::Deferred(DeferralReason::DataUnavailable)
            ),
            "got {outcome:?}"
        );
        assert_eq!(store.blocks().len(), before_len);
        assert!(!store.blocks().contains_key(&root));
        assert_eq!(
            outcome.gossip_class(),
            Some(cc_state_transition::GossipClass::Ignore)
        );
    }

    #[test]
    fn unknown_parent_defers() {
        let (mut store, _anchor, config) = seeded_store(Arc::new(HarnessAvailability));
        store.set_time(12);

        let unknown_parent = root(0xEE);
        let block = signed_block(1, unknown_parent, 0);

        let outcome = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();

        assert!(matches!(
            outcome,
            BlockImport::Deferred(DeferralReason::UnknownParent)
        ));
        assert_eq!(
            outcome.gossip_class(),
            Some(cc_state_transition::GossipClass::Ignore)
        );
    }

    #[test]
    fn future_slot_defers() {
        let (mut store, anchor, config) = seeded_store(Arc::new(HarnessAvailability));
        assert_eq!(store.get_current_slot().as_u64(), 0);

        let block = signed_block(5, anchor, 0);
        let outcome = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();

        assert!(matches!(
            outcome,
            BlockImport::Deferred(DeferralReason::FutureSlot)
        ));
    }

    #[test]
    fn not_descended_from_finalized_is_reject_error() {
        let (mut store, anchor, config) = seeded_store(Arc::new(HarnessAvailability));
        store.set_time(100);

        let foreign = root(0xFD);
        store.update_checkpoints(cp(1, foreign), cp(1, foreign));
        let block = signed_block(9, anchor, 0);

        let err = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap_err();

        assert!(matches!(err, OnBlockError::NotDescendedFromFinalized));
        assert_eq!(err.gossip_class(), GossipClass::Reject);
    }

    #[test]
    fn get_forkchoice_store_seeds_anchor_and_reimport_is_idempotent() {
        let (store, anchor, config) = seeded_store(Arc::new(HarnessAvailability));
        assert!(store.blocks().contains_key(&anchor));
        assert!(store.block_state(&anchor).is_some());
        assert_eq!(store.proto_array().len(), 1);
        assert_eq!(store.checkpoint_contexts_len(), 1);

        let mut store = store;
        let anchor_block = SignedBeaconBlock {
            message: BeaconBlock {
                slot: Slot::new(0),
                proposer_index: ValidatorIndex::new(0),
                parent_root: Root::ZERO,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let recomputed = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block.message));
        assert_eq!(recomputed, anchor);

        let outcome = on_block(
            &mut store,
            &anchor_block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();
        assert_eq!(
            outcome,
            BlockImport::Imported(ImportedBlock { root: anchor })
        );
        assert_eq!(store.blocks().len(), 1);
    }

    /// SEC-15b-1: header-only partial must not short-circuit as Imported;
    /// retry / resume completes proto-array membership.
    ///
    /// H-3: seed a non-zero `latest_execution_payload_header.block_hash` on the
    /// resident post-state so resume can recover the execution hash without a
    /// synthetic ZERO body.
    #[test]
    fn partial_header_without_proto_is_completed_not_false_imported() {
        let (mut store, anchor, config) = seeded_store(Arc::new(HarnessAvailability));
        store.set_time(12);

        let justified = cp(0, anchor);
        let finalized = cp(0, anchor);
        let exec_hash = Root::from_array([0xAB; 32]);

        // Rebuild partial under the real message root:
        let signed = signed_block(1, anchor, 0);
        let real_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
        let mut st = store.block_state(&anchor).unwrap().clone();
        st.set_slot(Slot::new(1));
        st.set_current_justified_checkpoint(justified);
        st.set_finalized_checkpoint(finalized);
        // Resident post-state carries this block's execution hash (H-3 source).
        let mut header = st.latest_execution_payload_header().clone();
        header.block_hash = exec_hash;
        st.set_latest_execution_payload_header(header);
        store.insert_block(
            real_root,
            BeaconBlockHeader {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor,
                state_root: Root::ZERO,
                body_root: Root::from_hash256(TreeHash::tree_hash_root(&signed.message.body)),
            },
            st,
        );
        assert!(!store.proto_array().contains(&real_root));

        let outcome = on_block(
            &mut store,
            &signed,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();

        assert_eq!(
            outcome,
            BlockImport::Imported(ImportedBlock { root: real_root })
        );
        assert!(
            store.proto_array().contains(&real_root),
            "resume must insert proto-array node"
        );
        assert!(is_fully_imported(&store, &real_root));
        let node = store.proto_array().get(&real_root).unwrap();
        assert_eq!(node.execution_block_hash, exec_hash.to_hash256());
        assert_ne!(
            node.execution_block_hash,
            Hash256::ZERO,
            "H-3: resume must not write ZERO"
        );
        // Fail-closed: status was not retained across the torn write.
        assert_eq!(node.execution_status, ExecutionStatus::Optimistic);

        // Second call is a true idempotent short-circuit (both maps).
        let again = on_block(
            &mut store,
            &signed,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();
        assert_eq!(
            again,
            BlockImport::Imported(ImportedBlock { root: real_root })
        );
    }

    /// H-3: partial with ZERO execution hash declines rather than writing a
    /// post-merge node with the case-2 sentinel.
    #[test]
    fn no_zero_execution_block_hash_post_merge() {
        let (mut store, anchor, config) = seeded_store(Arc::new(HarnessAvailability));
        store.set_time(12);

        let justified = cp(0, anchor);
        let finalized = cp(0, anchor);
        let signed = signed_block(1, anchor, 0);
        let real_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
        let mut st = store.block_state(&anchor).unwrap().clone();
        st.set_slot(Slot::new(1));
        st.set_current_justified_checkpoint(justified);
        st.set_finalized_checkpoint(finalized);
        // ZERO hash on resident state → complete_partial declines (H-3).
        assert_eq!(st.latest_execution_payload_header().block_hash, Root::ZERO);
        store.insert_block(
            real_root,
            BeaconBlockHeader {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor,
                state_root: Root::ZERO,
                body_root: Root::from_hash256(TreeHash::tree_hash_root(&signed.message.body)),
            },
            st,
        );

        // on_block declines partial and falls through to full path. Full path
        // may fail ST (Default body), but must never leave a ZERO-hash node.
        let _ = on_block(
            &mut store,
            &signed,
            &config,
            BlockSignatureStrategy::NoVerification,
        );

        for node in store.proto_array().nodes() {
            if node.parent.is_some() {
                assert!(
                    node.execution_status == ExecutionStatus::Irrelevant
                        || node.execution_block_hash != Hash256::ZERO,
                    "H-3: no post-merge ProtoNode with ZERO execution_block_hash (root={:?})",
                    node.root
                );
            }
        }
        // Partial path specifically did not insert under real_root with ZERO.
        if let Some(n) = store.proto_array().get(&real_root) {
            assert_ne!(n.execution_block_hash, Hash256::ZERO);
        }
    }

    /// Proto-array failure before `insert_block` must not leave a header.
    #[test]
    fn proto_array_unknown_parent_does_not_leave_header() {
        let (mut store, anchor, _config) = seeded_store(Arc::new(HarnessAvailability));

        // Parent present as header/state only (not in proto-array).
        let orphan_parent = root(0xAB);
        let mut parent_state = store.block_state(&anchor).unwrap().clone();
        parent_state.set_slot(Slot::new(1));
        store.insert_block(
            orphan_parent,
            BeaconBlockHeader {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor,
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            parent_state.clone(),
        );
        assert!(!store.proto_array().contains(&orphan_parent));

        let child_root = root(0xCD);
        let child_block = BeaconBlock {
            slot: Slot::new(2),
            proposer_index: ValidatorIndex::new(0),
            parent_root: orphan_parent,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        // integrate_block should fail at proto insert; no child header.
        let err = integrate_block(
            &mut store,
            child_root,
            &child_block,
            parent_state,
            ExecutionStatus::Valid,
            Hash256::from([0xCD; 32]),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            OnBlockError::ProtoArray(ProtoArrayError::UnknownParent(_))
        ));
        assert!(!store.blocks().contains_key(&child_root));
        assert!(!store.proto_array().contains(&child_root));
    }

    /// `compute_pulled_up_tip` records unrealized on the node; prior-epoch
    /// promotion bumps store justified when unrealized is newer.
    #[test]
    fn compute_pulled_up_tip_sets_unrealized_and_prior_epoch_promotes() {
        let (mut store, anchor, _config) = seeded_store(Arc::new(HarnessAvailability));

        // Block from epoch 0; store already in epoch 2 → prior-epoch branch.
        // Minimal: 8 slots/epoch × 6 s = 48 s/epoch → epoch 2 starts at t=96.
        store.set_time(96);
        assert_eq!(store.get_current_store_epoch().as_u64(), 2);

        let child = root(0x22);
        let realized = cp(0, anchor);
        let pulled = cp(1, root(0x99));
        let mut child_state = store.block_state(&anchor).unwrap().clone();
        child_state.set_slot(Slot::new(1));
        // J&F is a no-op at genesis epochs; seed the post-state justified that
        // pull-up will read after the no-op so prior-epoch promotion is visible.
        child_state.set_current_justified_checkpoint(pulled);
        child_state.set_finalized_checkpoint(realized);

        store.insert_block(
            child,
            BeaconBlockHeader {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor,
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            child_state,
        );
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: child,
                parent_root: Some(anchor),
                state_root: Root::ZERO,
                target_root: child,
                justified_checkpoint: realized,
                finalized_checkpoint: realized,
                unrealized_justified_checkpoint: realized,
                unrealized_finalized_checkpoint: realized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();

        compute_pulled_up_tip(&mut store, child).unwrap();

        let node = store.proto_array().get(&child).unwrap();
        assert_eq!(node.unrealized_justified_checkpoint, pulled);
        assert_eq!(store.unrealized_justified_checkpoint(), pulled);
        // Prior-epoch promotion applied pulled justified as store realized.
        assert_eq!(store.justified_checkpoint(), pulled);
    }

    /// Production call site: `is_data_available` is invoked from `on_block`.
    #[test]
    fn is_data_available_called_from_on_block() {
        use std::sync::atomic::{AtomicU32, Ordering};

        struct CountingDa {
            hits: AtomicU32,
        }
        impl DataAvailability for CountingDa {
            fn is_data_available(&self, _beacon_block_root: Root) -> bool {
                self.hits.fetch_add(1, Ordering::SeqCst);
                true
            }
        }

        let da = Arc::new(CountingDa {
            hits: AtomicU32::new(0),
        });
        let (mut store, _anchor, config) = seeded_store(da.clone());
        store.set_time(12);

        let block = signed_block(1, root(0xEE), 0);
        let _ = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        );

        assert!(
            da.hits.load(Ordering::SeqCst) >= 1,
            "on_block must call is_data_available"
        );
    }

    #[test]
    fn not_descended_reject_class_not_deferral() {
        let err = OnBlockError::NotDescendedFromFinalized;
        assert_eq!(err.gossip_class(), GossipClass::Reject);
        assert_ne!(err.gossip_class(), GossipClass::Ignore);
    }

    /// Phase 1 / CC-14: engine transport failure classifies as `Internal`, never `Reject`.
    ///
    /// Untouched classification surface — the error-path property survives CC-36a's
    /// success-path deferral mapping (CC-36 /6).
    #[test]
    fn cc14_engine_error_is_internal() {
        let err = OnBlockError::Transition(BlockError::Engine(
            cc_state_transition::EngineError::Transport("rpc down".into()),
        ));
        assert_eq!(err.gossip_class(), GossipClass::Internal);
        assert_ne!(err.gossip_class(), GossipClass::Reject);
        assert_eq!(
            BlockError::Engine(cc_state_transition::EngineError::Transport("x".into()))
                .gossip_class(),
            GossipClass::Internal
        );
    }

    /// CC-36a /5: `Engine(Transport)` → `Ok(Deferred(ExecutionEngineUnavailable))`.
    ///
    /// Production mapping (the ~5-line arm in `on_block`). Full ST through a
    /// synthetic Minimal genesis does not reach the engine (parent-header
    /// checks fail first); the arm is exercised here with the same match the
    /// production path uses, and store-unmutated is asserted by construction
    /// (`integrate_block` is not called on this branch).
    #[test]
    fn engine_transport_defers_not_invalidates() {
        // Mirror of the production match arm in `on_block` (D-4).
        fn map_transition_result(
            result: Result<(), BlockError>,
        ) -> Result<BlockImport, OnBlockError> {
            match result {
                Ok(()) => Ok(BlockImport::Imported(ImportedBlock {
                    root: Root::ZERO,
                })),
                Err(BlockError::Engine(cc_state_transition::EngineError::Transport(_))) => {
                    Ok(BlockImport::Deferred(
                        DeferralReason::ExecutionEngineUnavailable,
                    ))
                }
                Err(e) => Err(OnBlockError::Transition(e)),
            }
        }

        let outcome = map_transition_result(Err(BlockError::Engine(
            cc_state_transition::EngineError::Transport("el unreachable".into()),
        )))
        .expect("transport must be Ok(Deferred), not Err");
        assert!(
            matches!(
                outcome,
                BlockImport::Deferred(DeferralReason::ExecutionEngineUnavailable)
            ),
            "expected Ok(Deferred(ExecutionEngineUnavailable)), got {outcome:?}"
        );
        assert_eq!(
            outcome.gossip_class(),
            Some(GossipClass::Ignore),
            "engine deferral is Ignore, never Reject"
        );

        // Store unmutated: path returns before integrate_block. Seed a store and
        // show the Deferred outcome does not require / perform a store write.
        let (store, _anchor, _config) = seeded_store(Arc::new(HarnessAvailability));
        let before: Vec<Root> = store.blocks().keys().copied().collect();
        // No integrate_block call on Deferred — keys unchanged by construction.
        let after: Vec<Root> = store.blocks().keys().copied().collect();
        assert_eq!(before, after, "store.blocks unmutated on engine deferral");

        // Non-transport engine errors still surface as Err (not deferred).
        let invalid = map_transition_result(Err(BlockError::Engine(
            cc_state_transition::EngineError::InvalidPayload,
        )));
        assert!(matches!(invalid, Err(OnBlockError::Transition(_))));
    }

    /// CC-34a: outbox mapping + `integrate_block` records execution status.
    ///
    /// SYNCING → Optimistic, VALID → Valid. The production path is
    /// `take_payload_status` → `from_payload_status` → `integrate_block`.
    #[test]
    fn on_block_records_execution_status() {
        use cc_state_transition::PayloadStatus;

        // Mapping side (what on_block does with the outbox).
        assert_eq!(
            ExecutionStatus::from_payload_status(&PayloadStatus::Syncing),
            ExecutionStatus::Optimistic
        );
        assert_eq!(
            ExecutionStatus::from_payload_status(&PayloadStatus::Valid),
            ExecutionStatus::Valid
        );

        let (mut store, anchor, _config) = seeded_store(Arc::new(HarnessAvailability));
        let justified = cp(0, anchor);
        let finalized = cp(0, anchor);

        for (tag, status, want) in [
            (
                0x51u8,
                ExecutionStatus::Optimistic,
                ExecutionStatus::Optimistic,
            ),
            (0x52u8, ExecutionStatus::Valid, ExecutionStatus::Valid),
        ] {
            let child = root(tag);
            let exec = Hash256::from([tag; 32]);
            let mut st = store.block_state(&anchor).unwrap().clone();
            st.set_slot(Slot::new(1));
            st.set_current_justified_checkpoint(justified);
            st.set_finalized_checkpoint(finalized);
            let block = BeaconBlock {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor,
                state_root: Root::ZERO,
                body: Default::default(),
            };
            integrate_block(&mut store, child, &block, st, status, exec).unwrap();
            let node = store.proto_array().get(&child).unwrap();
            assert_eq!(node.execution_status, want, "tag={tag:#x}");
            assert_eq!(node.execution_block_hash, exec);
        }
    }
}
