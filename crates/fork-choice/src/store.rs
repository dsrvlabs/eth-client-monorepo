//! Fork-choice [`Store`] (Architecture §6.2, CC-15a / CC-15b).
//!
//! Field set matches the architecture surface. Method names and argument order
//! are the **spec's own** so the `fork_choice` vector steps map one-to-one.
//!
//! Critical mutable state is private so every write path that can move the head
//! goes through methods that bump [`Store::mutation_counter`] (§6.4).
//!
//! `on_block` / `compute_pulled_up_tip` live in [`crate::on_block`]. The DA trait
//! is owned by [`crate::da_seam`] (CC-17). Checkpoint contexts: [`crate::checkpoint_context`].

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use cc_state_transition::ExecutionEngine;
use cc_types::BeaconState;
use cc_types::containers::{BeaconBlockHeader, Checkpoint};
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, Slot, ValidatorIndex};
use lru::LruCache;
use thiserror::Error;

use crate::checkpoint_context::{CheckpointContext, checkpoint_context_key};
use crate::da_seam::DataAvailability;
use crate::proto_array::ProtoArray;

/// Default capacity for the checkpoint-context LRU (Architecture §6.6: **8**).
pub const DEFAULT_CHECKPOINT_CONTEXT_CAPACITY: usize = 8;

/// Spec `LatestMessage` — dense-vector element of the store's latest-messages table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatestMessage {
    /// Target epoch of the latest attestation.
    pub epoch: Epoch,
    /// LMD head root of the latest attestation.
    pub root: Root,
}

/// Head-cache entry served while the store's mutation counter is unchanged (§6.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedHead {
    /// Cached head root.
    pub head_root: Root,
    /// Cached head slot.
    pub head_slot: Slot,
    /// Justified checkpoint at computation time.
    pub justified: Checkpoint,
    /// Finalized checkpoint at computation time.
    pub finalized: Checkpoint,
    /// Mutation counter value when this cache entry was written.
    pub computed_at_mutation: u64,
}

/// Store construction / query errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StoreError {
    /// `on_tick` must not go backwards.
    #[error("on_tick time {time} is earlier than store.time {store_time}")]
    TimeWentBackwards { time: u64, store_time: u64 },
}

/// Spec-shaped fork-choice store (Architecture §6.2).
///
/// `blocks` stores **headers, not blocks**. Post-states for unfinalized roots
/// live in `block_states` so `on_block` can run `state_transition` and
/// `compute_pulled_up_tip` (residency pruning is owned by the chain service,
/// CC-18b). `latest_messages` is a dense `Vec<Option<LatestMessage>>` so
/// `compute_deltas` is a linear scan.
///
/// # Mutation discipline
///
/// Fields that affect head are private. Mutators bump `mutation_counter` and
/// clear the head cache. Reviewers: if you write to the store, go through a
/// method that bumps the counter.
pub struct Store<P: Preset> {
    /// Current Unix time (seconds) known to the store.
    time: u64,
    /// Genesis Unix time (seconds).
    genesis_time: u64,
    /// Seconds per slot (from chain config; used by slot helpers).
    seconds_per_slot: u64,
    justified_checkpoint: Checkpoint,
    finalized_checkpoint: Checkpoint,
    unrealized_justified_checkpoint: Checkpoint,
    unrealized_finalized_checkpoint: Checkpoint,
    proposer_boost_root: Root,
    equivocating_indices: BTreeSet<ValidatorIndex>,
    blocks: HashMap<Root, BeaconBlockHeader>,
    /// Post-states keyed by block root (parent lookup + pulled-up tip).
    block_states: HashMap<Root, BeaconState<P>>,
    /// Checkpoint-context LRU (Architecture §6.6 — capacity 8). ADR-P1-08.
    checkpoint_contexts: LruCache<(Epoch, Root), Arc<CheckpointContext>>,
    latest_messages: Vec<Option<LatestMessage>>,
    proto_array: ProtoArray,
    head_cache: Option<CachedHead>,
    mutation_counter: u64,
    engine: Arc<dyn ExecutionEngine<P>>,
    da: Arc<dyn DataAvailability>,
    _preset: std::marker::PhantomData<P>,
}

impl<P: Preset> std::fmt::Debug for Store<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("time", &self.time)
            .field("genesis_time", &self.genesis_time)
            .field("seconds_per_slot", &self.seconds_per_slot)
            .field("justified_checkpoint", &self.justified_checkpoint)
            .field("finalized_checkpoint", &self.finalized_checkpoint)
            .field(
                "unrealized_justified_checkpoint",
                &self.unrealized_justified_checkpoint,
            )
            .field(
                "unrealized_finalized_checkpoint",
                &self.unrealized_finalized_checkpoint,
            )
            .field("proposer_boost_root", &self.proposer_boost_root)
            .field("equivocating_indices_len", &self.equivocating_indices.len())
            .field("blocks_len", &self.blocks.len())
            .field("block_states_len", &self.block_states.len())
            .field("checkpoint_contexts_len", &self.checkpoint_contexts.len())
            .field("latest_messages_len", &self.latest_messages.len())
            .field("proto_array_len", &self.proto_array.len())
            .field("head_cache", &self.head_cache)
            .field("mutation_counter", &self.mutation_counter)
            .finish_non_exhaustive()
    }
}

impl<P: Preset> Store<P> {
    /// Construct a store at the anchor checkpoints with empty trees.
    ///
    /// Prefer [`crate::on_block::get_forkchoice_store`] for a fully seeded store.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        time: u64,
        genesis_time: u64,
        seconds_per_slot: u64,
        justified_checkpoint: Checkpoint,
        finalized_checkpoint: Checkpoint,
        validator_count: usize,
        engine: Arc<dyn ExecutionEngine<P>>,
        da: Arc<dyn DataAvailability>,
    ) -> Self {
        let capacity =
            NonZeroUsize::new(DEFAULT_CHECKPOINT_CONTEXT_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        Self {
            time,
            genesis_time,
            seconds_per_slot: seconds_per_slot.max(1),
            justified_checkpoint,
            finalized_checkpoint,
            unrealized_justified_checkpoint: justified_checkpoint,
            unrealized_finalized_checkpoint: finalized_checkpoint,
            proposer_boost_root: Root::ZERO,
            equivocating_indices: BTreeSet::new(),
            blocks: HashMap::new(),
            block_states: HashMap::new(),
            checkpoint_contexts: LruCache::new(capacity),
            latest_messages: vec![None; validator_count],
            proto_array: ProtoArray::new(justified_checkpoint, finalized_checkpoint),
            head_cache: None,
            mutation_counter: 0,
            engine,
            da,
            _preset: std::marker::PhantomData,
        }
    }

    // --- accessors (read) ----------------------------------------------------

    /// Current store time (seconds).
    #[inline]
    pub fn time(&self) -> u64 {
        self.time
    }

    /// Genesis time (seconds).
    #[inline]
    pub fn genesis_time(&self) -> u64 {
        self.genesis_time
    }

    /// Seconds per slot used for slot math.
    #[inline]
    pub fn seconds_per_slot(&self) -> u64 {
        self.seconds_per_slot
    }

    /// Justified checkpoint for LMD-GHOST.
    #[inline]
    pub fn justified_checkpoint(&self) -> Checkpoint {
        self.justified_checkpoint
    }

    /// Highest known finalized checkpoint.
    #[inline]
    pub fn finalized_checkpoint(&self) -> Checkpoint {
        self.finalized_checkpoint
    }

    /// Unrealized justified (pulled-up).
    #[inline]
    pub fn unrealized_justified_checkpoint(&self) -> Checkpoint {
        self.unrealized_justified_checkpoint
    }

    /// Unrealized finalized (pulled-up).
    #[inline]
    pub fn unrealized_finalized_checkpoint(&self) -> Checkpoint {
        self.unrealized_finalized_checkpoint
    }

    /// Root receiving proposer boost this slot, or zero.
    #[inline]
    pub fn proposer_boost_root(&self) -> Root {
        self.proposer_boost_root
    }

    /// Monotonic mutation counter (§6.4).
    #[inline]
    pub fn mutation_counter(&self) -> u64 {
        self.mutation_counter
    }

    /// Borrow the head cache, if any.
    #[inline]
    pub fn head_cache(&self) -> Option<&CachedHead> {
        self.head_cache.as_ref()
    }

    /// Borrow the proto-array (read-only). Mutations go through store methods.
    #[inline]
    pub fn proto_array(&self) -> &ProtoArray {
        &self.proto_array
    }

    /// Borrow block headers map (read-only).
    #[inline]
    pub fn blocks(&self) -> &HashMap<Root, BeaconBlockHeader> {
        &self.blocks
    }

    /// Borrow post-state for `root`, if resident.
    #[inline]
    pub fn block_state(&self, root: &Root) -> Option<&BeaconState<P>> {
        self.block_states.get(root)
    }

    /// Number of resident block states.
    #[inline]
    pub fn block_states_len(&self) -> usize {
        self.block_states.len()
    }

    /// Borrow latest-messages table (read-only).
    #[inline]
    pub fn latest_messages(&self) -> &[Option<LatestMessage>] {
        &self.latest_messages
    }

    /// Borrow the DA seam.
    #[inline]
    pub fn da(&self) -> &dyn DataAvailability {
        self.da.as_ref()
    }

    /// Borrow the execution engine seam.
    #[inline]
    pub fn engine(&self) -> &dyn ExecutionEngine<P> {
        self.engine.as_ref()
    }

    /// Shared engine handle for callers that need an `Arc` (e.g. transition context).
    #[inline]
    pub fn engine_arc(&self) -> &Arc<dyn ExecutionEngine<P>> {
        &self.engine
    }

    /// Current number of checkpoint-context LRU entries.
    #[inline]
    pub fn checkpoint_contexts_len(&self) -> usize {
        self.checkpoint_contexts.len()
    }

    /// Borrow a checkpoint context by checkpoint (promotes to MRU).
    pub fn checkpoint_context(&mut self, checkpoint: Checkpoint) -> Option<Arc<CheckpointContext>> {
        self.checkpoint_contexts
            .get(&checkpoint_context_key(checkpoint))
            .map(Arc::clone)
    }

    // --- slot helpers --------------------------------------------------------

    /// Spec `get_slots_since_genesis`.
    #[inline]
    pub fn get_slots_since_genesis(&self) -> u64 {
        self.time.saturating_sub(self.genesis_time) / self.seconds_per_slot
    }

    /// Spec `get_current_slot`.
    #[inline]
    pub fn get_current_slot(&self) -> Slot {
        Slot::new(self.get_slots_since_genesis())
    }

    /// Spec `get_current_store_epoch`.
    #[inline]
    pub fn get_current_store_epoch(&self) -> Epoch {
        self.get_current_slot().epoch(P::SLOTS_PER_EPOCH)
    }

    /// Spec `compute_slots_since_epoch_start`.
    #[inline]
    pub fn compute_slots_since_epoch_start(slot: Slot) -> u64 {
        let epoch = slot.epoch(P::SLOTS_PER_EPOCH);
        let start = epoch.as_u64().saturating_mul(P::SLOTS_PER_EPOCH);
        slot.as_u64().saturating_sub(start)
    }

    // --- mutation paths (always bump when state changes) ---------------------

    /// Bump the mutation counter and invalidate the head cache.
    ///
    /// Call from every store writer that can affect the head.
    #[inline]
    pub fn bump_mutation_counter(&mut self) {
        self.mutation_counter = self.mutation_counter.wrapping_add(1);
        self.head_cache = None;
    }

    /// Set store time without bumping (mid-slot `on_tick`; head is slot-stable).
    pub(crate) fn set_time(&mut self, time: u64) {
        self.time = time;
    }

    /// Clear proposer boost and bump the mutation counter (slot boundary).
    pub(crate) fn clear_proposer_boost_root(&mut self) {
        if self.proposer_boost_root != Root::ZERO {
            self.proposer_boost_root = Root::ZERO;
            self.bump_mutation_counter();
        } else {
            // Spec still clears (already zero) but a new slot is a mutation for
            // head-cache purposes: boost *application* window closed.
            self.proposer_boost_root = Root::ZERO;
            self.bump_mutation_counter();
        }
    }

    /// Test / future helper: set proposer boost through a mutator (bumps).
    #[cfg(test)]
    pub(crate) fn set_proposer_boost_root_for_test(&mut self, root: Root) {
        self.proposer_boost_root = root;
        self.bump_mutation_counter();
    }

    /// Test helper: set unrealized checkpoints without bumping (seed for epoch pull-up).
    #[cfg(test)]
    pub(crate) fn seed_unrealized_for_test(
        &mut self,
        justified: Checkpoint,
        finalized: Checkpoint,
    ) {
        self.unrealized_justified_checkpoint = justified;
        self.unrealized_finalized_checkpoint = finalized;
    }

    /// Insert a block header + post-state and bump the mutation counter.
    pub fn insert_block(&mut self, root: Root, header: BeaconBlockHeader, state: BeaconState<P>) {
        self.blocks.insert(root, header);
        self.block_states.insert(root, state);
        self.bump_mutation_counter();
    }

    /// Insert / refresh a [`CheckpointContext`] in the LRU (capacity 8).
    ///
    /// Does **not** bump `mutation_counter` by itself — callers that also change
    /// justified balances should have already bumped via checkpoint updates.
    pub fn insert_checkpoint_context(
        &mut self,
        checkpoint: Checkpoint,
        context: Arc<CheckpointContext>,
    ) {
        self.checkpoint_contexts
            .put(checkpoint_context_key(checkpoint), context);
    }

    /// Spec `update_checkpoints` — promote justified/finalized when newer.
    pub fn update_checkpoints(
        &mut self,
        justified_checkpoint: Checkpoint,
        finalized_checkpoint: Checkpoint,
    ) {
        let mut changed = false;
        if justified_checkpoint.epoch.as_u64() > self.justified_checkpoint.epoch.as_u64() {
            self.justified_checkpoint = justified_checkpoint;
            self.proto_array
                .set_checkpoints(self.justified_checkpoint, self.finalized_checkpoint);
            changed = true;
        }
        if finalized_checkpoint.epoch.as_u64() > self.finalized_checkpoint.epoch.as_u64() {
            self.finalized_checkpoint = finalized_checkpoint;
            self.proto_array
                .set_checkpoints(self.justified_checkpoint, self.finalized_checkpoint);
            changed = true;
        }
        if changed {
            self.bump_mutation_counter();
        }
    }

    /// Spec `update_unrealized_checkpoints`.
    pub fn update_unrealized_checkpoints(
        &mut self,
        unrealized_justified_checkpoint: Checkpoint,
        unrealized_finalized_checkpoint: Checkpoint,
    ) {
        let mut changed = false;
        if unrealized_justified_checkpoint.epoch.as_u64()
            > self.unrealized_justified_checkpoint.epoch.as_u64()
        {
            self.unrealized_justified_checkpoint = unrealized_justified_checkpoint;
            changed = true;
        }
        if unrealized_finalized_checkpoint.epoch.as_u64()
            > self.unrealized_finalized_checkpoint.epoch.as_u64()
        {
            self.unrealized_finalized_checkpoint = unrealized_finalized_checkpoint;
            changed = true;
        }
        if changed {
            self.bump_mutation_counter();
        }
    }

    /// Mutable proto-array access for in-crate insert/weight paths (CC-15b+).
    ///
    /// Callers that mutate must also call [`Self::bump_mutation_counter`] when
    /// the mutation can move the head (insert already bumps via `insert_block`).
    pub(crate) fn proto_array_mut(&mut self) -> &mut ProtoArray {
        &mut self.proto_array
    }

    /// Checkpoint-context LRU capacity (Architecture §6.6).
    pub fn checkpoint_context_capacity(&self) -> usize {
        self.checkpoint_contexts.cap().get()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use cc_state_transition::StubOptimisticEngine;
    use cc_types::containers::Checkpoint;
    use cc_types::fork::Fork;
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, Gwei, Root};

    use super::*;
    use crate::checkpoint_context::{CheckpointContext, CommitteeCache};
    use crate::da_seam::AlwaysAvailable;

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

    fn empty_ctx(epoch: u64) -> Arc<CheckpointContext> {
        Arc::new(CheckpointContext {
            epoch: Epoch::new(epoch),
            committee_cache: CommitteeCache::default(),
            effective_balances: vec![Gwei::new(32_000_000_000)],
            total_active_balance: Gwei::new(32_000_000_000),
            fork: Fork {
                previous_version: Default::default(),
                current_version: Default::default(),
                epoch: Epoch::new(0),
            },
            genesis_validators_root: Root::ZERO,
        })
    }

    fn new_store() -> Store<Minimal> {
        let anchor = cp(0, root(1));
        Store::new(
            0,
            0,
            6,
            anchor,
            anchor,
            0,
            Arc::new(StubOptimisticEngine),
            Arc::new(AlwaysAvailable),
        )
    }

    #[test]
    fn checkpoint_context_lru_capacity_is_eight() {
        let store = new_store();
        assert_eq!(
            store.checkpoint_context_capacity(),
            DEFAULT_CHECKPOINT_CONTEXT_CAPACITY
        );
        assert_eq!(DEFAULT_CHECKPOINT_CONTEXT_CAPACITY, 8);
    }

    /// Push 12 distinct checkpoints; map size never exceeds the bound of 8.
    #[test]
    fn checkpoint_context_lru_evicts_beyond_capacity() {
        let mut store = new_store();
        for i in 0u8..12 {
            let checkpoint = cp(i as u64, root(i.wrapping_add(1)));
            store.insert_checkpoint_context(checkpoint, empty_ctx(i as u64));
            assert!(
                store.checkpoint_contexts_len() <= DEFAULT_CHECKPOINT_CONTEXT_CAPACITY,
                "len {} exceeded capacity after insert {i}",
                store.checkpoint_contexts_len()
            );
        }
        assert_eq!(
            store.checkpoint_contexts_len(),
            DEFAULT_CHECKPOINT_CONTEXT_CAPACITY
        );
        // Oldest entries (0..4) should be gone; newest (4..12) retained.
        assert!(store.checkpoint_context(cp(0, root(1))).is_none());
        assert!(store.checkpoint_context(cp(11, root(12))).is_some());
    }
}
