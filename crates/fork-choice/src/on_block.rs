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
use cc_state_transition::{
    BlockError, BlockSignatureStrategy, GossipClass, TransitionContext, compute_epoch_at_slot,
    process_justification_and_finalization, state_transition,
};
use cc_types::config::ChainConfig;
use cc_types::containers::{BeaconBlockHeader, Checkpoint};
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, Slot};
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};
use thiserror::Error;
use tree_hash::TreeHash;

use crate::checkpoint_context::CheckpointContext;
use crate::da_seam::{BlockImport, DeferralReason, ImportedBlock};
use crate::proto_array::{ProtoArrayError, ProtoNodeBlock};
use crate::store::Store;

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
            // Internal structure faults — not a peer descore from gossip alone.
            Self::ProtoArray(_) | Self::PulledUpTip(_) => GossipClass::Internal,
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
    if store.blocks().contains_key(&block_root) {
        return complete_partial_import(store, block_root);
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
    state_transition(&mut state, signed_block, &ctx, verify)?;

    // --- 5–7. Integrate: proto-array first, then store, then checkpoints ----
    integrate_block(store, block_root, block, state)?;

    Ok(BlockImport::Imported(ImportedBlock { root: block_root }))
}

/// Whether `root` is present in both the header map and the proto-array.
#[inline]
fn is_fully_imported<P: Preset>(store: &Store<P>, root: &Root) -> bool {
    store.blocks().contains_key(root) && store.proto_array().contains(root)
}

/// Resume import when a header/state exists without a proto-array node.
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

    // Synthetic block view for integrate (body root already on header).
    let block = BeaconBlock {
        slot: header.slot,
        proposer_index: header.proposer_index,
        parent_root: header.parent_root,
        state_root: header.state_root,
        body: Default::default(),
    };

    integrate_block(store, block_root, &block, state)?;
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
        })?;
    } else {
        store.proto_array_mut().set_unrealized_checkpoints(
            block_root,
            unrealized_justified,
            unrealized_finalized,
        )?;
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

    store.proto_array_mut().set_unrealized_checkpoints(
        block_root,
        unrealized_justified,
        unrealized_finalized,
    )?;

    store.update_unrealized_checkpoints(unrealized_justified, unrealized_finalized);

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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::sync::Arc;

    use cc_state_transition::{BlockSignatureStrategy, StubOptimisticEngine};
    use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
    use cc_types::containers::{BeaconBlockHeader, Checkpoint};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};

    use super::*;
    use crate::da_seam::{AlwaysAvailable, DataAvailability, DeferralReason};
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
            Arc::new(StubOptimisticEngine),
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
        let (mut store, _anchor, config) = seeded_store(Arc::new(AlwaysAvailable));
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
        let (mut store, anchor, config) = seeded_store(Arc::new(AlwaysAvailable));
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
        let (mut store, anchor, config) = seeded_store(Arc::new(AlwaysAvailable));
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
        let (store, anchor, config) = seeded_store(Arc::new(AlwaysAvailable));
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
    #[test]
    fn partial_header_without_proto_is_completed_not_false_imported() {
        let (mut store, anchor, config) = seeded_store(Arc::new(AlwaysAvailable));
        store.set_time(12);

        let child = root(0x22);
        let justified = cp(0, anchor);
        let finalized = cp(0, anchor);
        let mut child_state = store.block_state(&anchor).unwrap().clone();
        child_state.set_slot(Slot::new(1));
        child_state.set_current_justified_checkpoint(justified);
        child_state.set_finalized_checkpoint(finalized);

        // Simulate a torn write: header+state present, proto-array missing.
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
        assert!(store.blocks().contains_key(&child));
        assert!(!store.proto_array().contains(&child));

        // Any signed block with the same tree-hash root is not required for the
        // resume path (uses resident header/state). Use a matching-slot shell
        // that hashes to a *different* root so we exercise resume by root
        // identity via a second call path: complete_partial through on_block
        // keyed by the resident root — call integrate via on_block only when
        // the block root matches. Build a SignedBeaconBlock whose message
        // root equals `child` is hard without fixing body; instead call
        // complete via on_block with a block that *is* `child` by inserting
        // under the actual hash of a synthetic block.
        //
        // Rebuild partial under the real message root:
        let signed = signed_block(1, anchor, 0);
        let real_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
        let mut st = store.block_state(&anchor).unwrap().clone();
        st.set_slot(Slot::new(1));
        st.set_current_justified_checkpoint(justified);
        st.set_finalized_checkpoint(finalized);
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

    /// Proto-array failure before `insert_block` must not leave a header.
    #[test]
    fn proto_array_unknown_parent_does_not_leave_header() {
        let (mut store, anchor, _config) = seeded_store(Arc::new(AlwaysAvailable));

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
        let err = integrate_block(&mut store, child_root, &child_block, parent_state).unwrap_err();
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
        let (mut store, anchor, _config) = seeded_store(Arc::new(AlwaysAvailable));

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
}
