//! In-memory state residency and the 64-block body ring (Architecture §7.5, ADR-P1-12).
//!
//! # Invariant
//!
//! Every proto-array node that can become head must have a post-state that is
//! either resident or re-derivable by replay from a resident ancestor within
//! the slot budget.
//!
//! # Pinned roles (max 4 by default)
//!
//! | Role | Why |
//! |---|---|
//! | head | every import's parent, pulled-up tip, query |
//! | anchor / finalized | replay origin of last resort |
//! | previous epoch-boundary | Phase 6 target lookups; shallow-reorg origin |
//! | scratch | in-flight import working copy |
//!
//! Adding a fifth requires naming a fifth role. Config: `chain.max_resident_states`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use cc_fork_choice::Store;
use cc_state_transition::{
    BlockSignatureStrategy, StubOptimisticEngine, TransitionContext, state_transition,
};
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::{Root, Slot};
use cc_types::{BeaconState, SignedBeaconBlock};
use thiserror::Error;

/// Default pinned-state budget (Architecture §7.5).
pub const DEFAULT_MAX_RESIDENT_STATES: usize = 4;

/// Default body-ring capacity for shallow-reorg replay (Architecture §7.5).
pub const DEFAULT_BODY_RING_CAPACITY: usize = 64;

/// Role labels for the four resident slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResidentRole {
    Head,
    Anchor,
    EpochBoundary,
    Scratch,
}

/// One body retained for shallow-reorg replay.
#[derive(Debug, Clone)]
pub struct BodyRingEntry<P: Preset> {
    pub root: Root,
    pub parent_root: Root,
    pub slot: Slot,
    pub block: Arc<SignedBeaconBlock<P>>,
}

/// Errors from residency / replay.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ResidencyError {
    /// Root is not resident and cannot be replayed from the body ring.
    #[error("state for root {root} not resident and not re-derivable (reorg gap)")]
    ReorgGap { root: Root },
    /// Replay state transition failed.
    #[error("replay state transition failed: {0}")]
    ReplayFailed(String),
}

/// Trait seam for post-state lookup (Phase 4 reimplements over durable storage).
///
/// Phase-1 [`Residency`] implements this over the **pinned set only**. Replay from
/// the body ring requires a store borrow and lives on [`Residency::ensure_in_store`].
pub trait StateProvider<P: Preset> {
    /// Fetch a post-state by block root from the pinned resident set.
    fn get_state(&self, root: Root) -> Result<Arc<BeaconState<P>>, ResidencyError>;
}

/// Pinned resident set + body ring.
#[derive(Debug)]
pub struct Residency<P: Preset> {
    max_resident: usize,
    body_ring_capacity: usize,
    /// Role → (root, state). At most one entry per role.
    roles: HashMap<ResidentRole, (Root, Arc<BeaconState<P>>)>,
    /// Ring of recently imported bodies (oldest at front).
    body_ring: VecDeque<BodyRingEntry<P>>,
    /// Index root → ring position for O(1) parent walk.
    body_index: HashMap<Root, usize>,
    /// Count of reorgs that exceeded the body ring (gap recorded, no panic).
    reorg_gaps: u64,
}

impl<P: Preset> Residency<P> {
    /// Construct with the architecture defaults (4 states, 64 bodies).
    pub fn new(max_resident: usize, body_ring_capacity: usize) -> Self {
        Self {
            max_resident: max_resident.max(1),
            body_ring_capacity: body_ring_capacity.max(1),
            roles: HashMap::new(),
            body_ring: VecDeque::new(),
            body_index: HashMap::new(),
            reorg_gaps: 0,
        }
    }

    /// Architecture defaults.
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_RESIDENT_STATES, DEFAULT_BODY_RING_CAPACITY)
    }

    /// Number of currently pinned states (≤ `max_resident`).
    pub fn resident_count(&self) -> usize {
        // Distinct roots across roles (scratch may equal head transiently).
        let mut roots = HashSet::new();
        for (root, _) in self.roles.values() {
            roots.insert(*root);
        }
        roots.len()
    }

    /// Configured max resident states.
    pub fn max_resident(&self) -> usize {
        self.max_resident
    }

    /// Body ring length.
    pub fn body_ring_len(&self) -> usize {
        self.body_ring.len()
    }

    /// Body ring capacity.
    pub fn body_ring_capacity(&self) -> usize {
        self.body_ring_capacity
    }

    /// Deep-reorg gap counter (never panics; increments instead).
    pub fn reorg_gaps(&self) -> u64 {
        self.reorg_gaps
    }

    /// Pin a state under `role`. Evicts the previous occupant of that role only.
    pub fn pin(&mut self, role: ResidentRole, root: Root, state: Arc<BeaconState<P>>) {
        self.roles.insert(role, (root, state));
        // Enforce max: if distinct roots exceed budget, drop scratch first, then
        // epoch-boundary (never drop head/anchor while they are still current).
        while self.resident_count() > self.max_resident {
            if self.roles.contains_key(&ResidentRole::Scratch)
                && !matches!(role, ResidentRole::Scratch)
            {
                self.roles.remove(&ResidentRole::Scratch);
                continue;
            }
            if self.roles.contains_key(&ResidentRole::EpochBoundary)
                && !matches!(role, ResidentRole::EpochBoundary)
            {
                self.roles.remove(&ResidentRole::EpochBoundary);
                continue;
            }
            // Should not happen with 4 roles and max ≥ 4; break to avoid loop.
            break;
        }
    }

    /// Clear the scratch role after an import settles.
    pub fn clear_scratch(&mut self) {
        self.roles.remove(&ResidentRole::Scratch);
    }

    /// Record an imported body in the ring (evicts oldest when over capacity).
    pub fn push_body(&mut self, entry: BodyRingEntry<P>) {
        if self.body_ring.len() >= self.body_ring_capacity
            && let Some(old) = self.body_ring.pop_front()
        {
            self.body_index.remove(&old.root);
        }
        let root = entry.root;
        self.body_ring.push_back(entry);
        self.rebuild_body_index();
        let _ = root;
    }

    fn rebuild_body_index(&mut self) {
        self.body_index.clear();
        for (i, e) in self.body_ring.iter().enumerate() {
            self.body_index.insert(e.root, i);
        }
    }

    /// Roots currently pinned (for store prune).
    pub fn pinned_roots(&self) -> HashSet<Root> {
        self.roles.values().map(|(r, _)| *r).collect()
    }

    /// Look up a pinned state without replay.
    pub fn pinned_state(&self, root: &Root) -> Option<Arc<BeaconState<P>>> {
        for (r, s) in self.roles.values() {
            if r == root {
                return Some(Arc::clone(s));
            }
        }
        None
    }

    /// Body entry for `root`, if still in the ring.
    pub fn body(&self, root: &Root) -> Option<&BodyRingEntry<P>> {
        let idx = *self.body_index.get(root)?;
        self.body_ring.get(idx)
    }

    /// Ensure `root`'s post-state is present in `store.block_states`, replaying
    /// from a resident ancestor through the body ring when needed.
    ///
    /// Returns `Ok(())` when the state is available. On a gap deeper than the
    /// ring, increments [`Self::reorg_gaps`] and returns [`ResidencyError::ReorgGap`]
    /// without panicking.
    pub fn ensure_in_store(
        &mut self,
        store: &mut Store<P>,
        root: Root,
        config: &ChainConfig,
    ) -> Result<(), ResidencyError> {
        if store.block_state(&root).is_some() {
            return Ok(());
        }
        if let Some(state) = self.pinned_state(&root) {
            store.put_block_state(root, (*state).clone());
            return Ok(());
        }

        // Walk parents via body ring until we find a resident ancestor.
        let mut path: Vec<Root> = Vec::new();
        let mut cursor = root;
        loop {
            if store.block_state(&cursor).is_some() || self.pinned_state(&cursor).is_some() {
                break;
            }
            let Some(entry) = self
                .body(&cursor)
                .map(|e| (e.parent_root, Arc::clone(&e.block)))
            else {
                self.reorg_gaps = self.reorg_gaps.saturating_add(1);
                return Err(ResidencyError::ReorgGap { root });
            };
            path.push(cursor);
            cursor = entry.0;
            // Bound walk by ring size.
            if path.len() > self.body_ring_capacity {
                self.reorg_gaps = self.reorg_gaps.saturating_add(1);
                return Err(ResidencyError::ReorgGap { root });
            }
            let _ = entry.1;
        }

        // Materialise ancestor into store if only pinned.
        if store.block_state(&cursor).is_none() {
            if let Some(state) = self.pinned_state(&cursor) {
                store.put_block_state(cursor, (*state).clone());
            } else {
                self.reorg_gaps = self.reorg_gaps.saturating_add(1);
                return Err(ResidencyError::ReorgGap { root });
            }
        }

        // Replay path oldest-first (parent → child).
        path.reverse();
        let engine = StubOptimisticEngine;
        let ctx = TransitionContext::new(config, &engine);
        for block_root in path {
            let Some(entry) = self.body(&block_root) else {
                self.reorg_gaps = self.reorg_gaps.saturating_add(1);
                return Err(ResidencyError::ReorgGap { root });
            };
            let parent = entry.parent_root;
            let block = Arc::clone(&entry.block);
            let mut state = store
                .block_state(&parent)
                .ok_or(ResidencyError::ReorgGap { root })?
                .clone();
            state_transition(
                &mut state,
                block.as_ref(),
                &ctx,
                BlockSignatureStrategy::NoVerification,
            )
            .map_err(|e| ResidencyError::ReplayFailed(e.to_string()))?;
            store.put_block_state(block_root, state);
        }
        Ok(())
    }

    /// Record a newly imported body and pin it as **scratch** (in-flight).
    ///
    /// Does **not** pin [`ResidentRole::Head`] or prune — that happens in
    /// [`Self::settle_after_head`] after `get_head` (H2 / §7.5: head role is the
    /// fork-choice head, not necessarily the just-imported block).
    pub fn record_imported_body(
        &mut self,
        block_root: Root,
        signed: Arc<SignedBeaconBlock<P>>,
        post_state: BeaconState<P>,
    ) {
        let parent_root = signed.message.parent_root;
        let slot = signed.message.slot;
        self.push_body(BodyRingEntry {
            root: block_root,
            parent_root,
            slot,
            block: signed,
        });
        // Scratch holds the in-flight import post-state until settle.
        self.pin(ResidentRole::Scratch, block_root, Arc::new(post_state));
    }

    /// After `get_head`: pin roles relative to the **fork-choice head**, then prune.
    ///
    /// Order required by H2: import → get_head → pin Head = FC head → prune →
    /// publish snapshot. `imported_root` may differ from `head_root` on a
    /// non-canonical import; the previous viable head's state is retained when
    /// it is still the FC head.
    pub fn settle_after_head(
        &mut self,
        store: &mut Store<P>,
        head_root: Root,
        imported_root: Root,
        is_epoch_boundary: bool,
    ) {
        // Resolve FC head post-state (store first, then any role including scratch).
        let head_state = store
            .block_state(&head_root)
            .map(|s| Arc::new(s.clone()))
            .or_else(|| self.pinned_state(&head_root));

        if let Some(st) = head_state {
            self.pin(ResidentRole::Head, head_root, st);
        }

        if is_epoch_boundary {
            // Epoch-boundary pin prefers the imported block when it is at a boundary;
            // fall back to head if that is the boundary block.
            let boundary_root = if store
                .blocks()
                .get(&imported_root)
                .is_some_and(|h| h.slot.as_u64().is_multiple_of(P::SLOTS_PER_EPOCH.max(1)))
            {
                imported_root
            } else {
                head_root
            };
            if let Some(st) = store
                .block_state(&boundary_root)
                .map(|s| Arc::new(s.clone()))
                .or_else(|| self.pinned_state(&boundary_root))
            {
                self.pin(ResidentRole::EpochBoundary, boundary_root, st);
            }
        }

        // Anchor tracks the store's finalized checkpoint root when available.
        let finalized_root = store.finalized_checkpoint().root;
        if let Some(st) = store
            .block_state(&finalized_root)
            .map(|s| Arc::new(s.clone()))
            .or_else(|| self.pinned_state(&finalized_root))
        {
            self.pin(ResidentRole::Anchor, finalized_root, st);
        }

        self.clear_scratch();

        // Prune store post-states to the pinned set only (after head recompute).
        let keep = self.pinned_roots();
        store.retain_block_states(|r| keep.contains(r));
        // Re-materialise any pinned state that was only held in roles.
        for (root, state) in self.roles.values() {
            if store.block_state(root).is_none() {
                store.put_block_state(*root, (**state).clone());
            }
        }
    }

    /// Role root currently pinned as Head (tests / diagnostics).
    pub fn head_root(&self) -> Option<Root> {
        self.roles.get(&ResidentRole::Head).map(|(r, _)| *r)
    }

    /// Seed roles from an anchor store (bootstrap / tests).
    pub fn seed_anchor(&mut self, root: Root, state: BeaconState<P>) {
        let arc = Arc::new(state);
        self.pin(ResidentRole::Head, root, Arc::clone(&arc));
        self.pin(ResidentRole::Anchor, root, Arc::clone(&arc));
        self.pin(ResidentRole::EpochBoundary, root, arc);
    }
}

impl<P: Preset> StateProvider<P> for Residency<P> {
    fn get_state(&self, root: Root) -> Result<Arc<BeaconState<P>>, ResidencyError> {
        if let Some(s) = self.pinned_state(&root) {
            return Ok(s);
        }
        // Without a store borrow we can only serve pinned states; replay needs
        // `ensure_in_store`. Phase 4's durable provider fills this gap.
        Err(ResidencyError::ReorgGap { root })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_types::preset::Minimal;

    fn dummy_block(slot: u64, parent: Root) -> SignedBeaconBlock<Minimal> {
        SignedBeaconBlock {
            message: cc_types::BeaconBlock {
                slot: Slot::new(slot),
                proposer_index: Default::default(),
                parent_root: parent,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        }
    }

    fn root(b: u8) -> Root {
        Root::from_array({
            let mut a = [0u8; 32];
            a[0] = b;
            a
        })
    }

    #[test]
    fn body_ring_caps_at_capacity() {
        let mut r = Residency::<Minimal>::new(4, 8);
        let mut parent = Root::ZERO;
        for i in 1..=20u64 {
            let block_root = root(i as u8);
            r.push_body(BodyRingEntry {
                root: block_root,
                parent_root: parent,
                slot: Slot::new(i),
                block: Arc::new(dummy_block(i, parent)),
            });
            parent = block_root;
            assert!(r.body_ring_len() <= 8);
        }
        assert_eq!(r.body_ring_len(), 8);
    }

    #[test]
    fn resident_count_never_exceeds_max() {
        let mut r = Residency::<Minimal>::new(4, 64);
        let state = BeaconState::<Minimal>::default();
        for i in 0..10u8 {
            r.pin(ResidentRole::Head, root(i), Arc::new(state.clone()));
            r.pin(
                ResidentRole::Scratch,
                root(100 + i),
                Arc::new(state.clone()),
            );
            r.pin(
                ResidentRole::EpochBoundary,
                root(200 + i),
                Arc::new(state.clone()),
            );
            r.pin(ResidentRole::Anchor, root(1), Arc::new(state.clone()));
            assert!(
                r.resident_count() <= 4,
                "resident_count={} after pins",
                r.resident_count()
            );
        }
    }

    #[test]
    fn deep_reorg_records_gap_without_panic() {
        let mut r = Residency::<Minimal>::new(4, 2);
        // Only two bodies in the ring; request a root that is not in the ring.
        r.push_body(BodyRingEntry {
            root: root(1),
            parent_root: Root::ZERO,
            slot: Slot::new(1),
            block: Arc::new(dummy_block(1, Root::ZERO)),
        });
        let missing = root(0xFF);
        // get_state is pin-only; gap is ReorgGap without panic.
        let err = r.get_state(missing).unwrap_err();
        assert!(matches!(err, ResidencyError::ReorgGap { .. }));
    }

    #[test]
    fn ensure_in_store_increments_reorg_gaps() {
        use cc_fork_choice::{AlwaysAvailable, Store};
        use cc_state_transition::StubOptimisticEngine;
        use cc_types::containers::Checkpoint;
        use cc_types::primitives::Epoch;

        let mut r = Residency::<Minimal>::new(4, 4);
        r.push_body(BodyRingEntry {
            root: root(1),
            parent_root: Root::ZERO,
            slot: Slot::new(1),
            block: Arc::new(dummy_block(1, Root::ZERO)),
        });
        // Empty store, no pins, root not re-derivable → gap.
        let engine: Arc<dyn cc_state_transition::ExecutionEngine<Minimal>> =
            Arc::new(StubOptimisticEngine);
        let da: Arc<dyn cc_fork_choice::DataAvailability> = Arc::new(AlwaysAvailable);
        let mut store = Store::<Minimal>::new(
            0,
            0,
            6,
            Checkpoint {
                epoch: Epoch::new(0),
                root: Root::ZERO,
            },
            Checkpoint {
                epoch: Epoch::new(0),
                root: Root::ZERO,
            },
            0,
            engine,
            da,
        );
        let config = ChainConfig {
            preset_base: cc_types::config::PresetName::Minimal,
            config_name: "minimal".into(),
            genesis_fork_version: Default::default(),
            altair_fork_version: Default::default(),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: Default::default(),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: Default::default(),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: Default::default(),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: Default::default(),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: Default::default(),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 6,
            blob_schedule: cc_types::config::BlobSchedule::try_from_entries(vec![
                cc_types::config::BlobParameters {
                    epoch: Epoch::new(0),
                    max_blobs_per_block: 9,
                },
            ])
            .unwrap(),
            deposit_chain_id: 0,
            deposit_contract_address: Default::default(),
        };
        let missing = root(0xFF);
        assert_eq!(r.reorg_gaps(), 0);
        let err = r.ensure_in_store(&mut store, missing, &config).unwrap_err();
        assert!(matches!(err, ResidencyError::ReorgGap { .. }));
        assert_eq!(r.reorg_gaps(), 1);
    }

    #[test]
    fn settle_after_head_pins_fc_head_not_imported() {
        use cc_fork_choice::{AlwaysAvailable, Store};
        use cc_state_transition::StubOptimisticEngine;
        use cc_types::containers::{BeaconBlockHeader, Checkpoint};
        use cc_types::primitives::Epoch;

        let engine: Arc<dyn cc_state_transition::ExecutionEngine<Minimal>> =
            Arc::new(StubOptimisticEngine);
        let da: Arc<dyn cc_fork_choice::DataAvailability> = Arc::new(AlwaysAvailable);
        let mut store = Store::<Minimal>::new(
            12,
            0,
            6,
            Checkpoint {
                epoch: Epoch::new(0),
                root: root(1),
            },
            Checkpoint {
                epoch: Epoch::new(0),
                root: root(1),
            },
            1,
            engine,
            da,
        );
        let head_state = BeaconState::<Minimal>::default();
        let imported_state = BeaconState::<Minimal>::default();
        store.insert_block(
            root(1),
            BeaconBlockHeader {
                slot: Slot::new(1),
                proposer_index: Default::default(),
                parent_root: Root::ZERO,
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            head_state,
        );
        store.insert_block(
            root(2),
            BeaconBlockHeader {
                slot: Slot::new(2),
                proposer_index: Default::default(),
                parent_root: root(1),
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            imported_state.clone(),
        );

        let mut r = Residency::<Minimal>::new(4, 64);
        r.record_imported_body(root(2), Arc::new(dummy_block(2, root(1))), imported_state);
        // FC head is still root(1); imported was non-canonical sibling path.
        r.settle_after_head(&mut store, root(1), root(2), false);
        assert_eq!(r.head_root(), Some(root(1)));
        // Head state retained; scratch cleared.
        assert!(store.block_state(&root(1)).is_some());
        assert!(r.pinned_state(&root(1)).is_some());
    }
}
