//! Proto-array fork-choice tree (Architecture §6.1, CC-15a).
//!
//! # Index lifetime
//!
//! **`prune` is the only operation that invalidates stored indices.** Every
//! long-lived reference must be a [`Root`], never a `usize`. A stored index that
//! survives a prune is a silent wrong-node bug — the public API therefore exposes
//! only roots for external identity. Index helpers are `pub(crate)` for
//! in-crate weight / head passes that re-resolve after any prune.
//!
//! Insertion order guarantees a parent always has a **lower** index than its
//! children, so a head computation is one backward weight/best-child pass and a
//! forward walk from the justified root along `best_descendant`.
//!
//! # Viability (skeleton)
//!
//! [`ProtoArray::node_is_viable_with`] is a **per-node** predicate used in place
//! of the spec's recursive `filter_block_tree` (Architecture §6.1). It implements
//! the leaf justified/finalized checks with:
//!
//! - **Voting source** selection by block epoch vs current epoch
//!   (`get_voting_source` shape).
//! - **Finalized ancestry** via [`ProtoArray::get_ancestor`] at the finalized
//!   epoch start slot (spec `get_checkpoint_block` shape).
//!
//! **Known gaps closed by CC-15c `get_head` / filter walk (not this skeleton):**
//!
//! - Recursive viability (parent viable iff any child branch is viable).
//! - Full weight application / best-child selection / proposer-boost delta.
//! - Recomputing `best_child` / `best_descendant` after prune (links may be
//!   cleared; head pass must rebuild them).

use std::collections::HashMap;

use cc_types::containers::Checkpoint;
use cc_types::primitives::{Epoch, Hash256, Root, Slot};
use thiserror::Error;

use crate::execution_status::{ExecutionStatus, h3_execution_hash_ok};

/// Errors from proto-array mutations and queries.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtoArrayError {
    /// Parent root is not present in the array.
    #[error("unknown parent root: {0:?}")]
    UnknownParent(Root),
    /// Block root is already present.
    #[error("duplicate block root: {0:?}")]
    DuplicateRoot(Root),
    /// Finalized root is not present (cannot prune).
    #[error("unknown finalized root: {0:?}")]
    UnknownFinalized(Root),
    /// Node root not present.
    #[error("unknown root: {0:?}")]
    UnknownRoot(Root),
    /// Weight-delta slice length does not match the node count.
    #[error("weight delta length {got} does not match node count {expected}")]
    DeltaLengthMismatch { got: usize, expected: usize },
    /// Justified checkpoint root is not in the array.
    #[error("unknown justified root: {0:?}")]
    UnknownJustified(Root),
    /// Weight arithmetic overflowed.
    #[error("weight delta overflow at node index {0}")]
    WeightOverflow(usize),
    /// Best-descendant / best-child index is out of range.
    #[error("invalid best-link index {0}")]
    InvalidBestLink(usize),
    /// Selected head is not viable for the current store checkpoints.
    #[error("selected head root {0:?} is not viable")]
    NonViableHead(Root),
    /// H-3: `Optimistic` / `Invalid` nodes must not carry the all-zeros
    /// execution hash (CC-35 case-2 `latestValidHash` sentinel).
    #[error(
        "H-3: execution_block_hash must not be ZERO for {status:?} node (root {root:?})"
    )]
    ZeroExecutionBlockHash {
        /// Block root that was refused.
        root: Root,
        /// Status that forbids ZERO.
        status: ExecutionStatus,
    },
}

/// Arguments for [`ProtoArray::on_block`] (one logical block insertion).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoNodeBlock {
    /// Block slot.
    pub slot: Slot,
    /// Block root.
    pub root: Root,
    /// Parent block root, if any.
    pub parent_root: Option<Root>,
    /// Post-state root.
    pub state_root: Root,
    /// FFG target root for this block.
    pub target_root: Root,
    /// Justified checkpoint from the block's post-state.
    pub justified_checkpoint: Checkpoint,
    /// Finalized checkpoint from the block's post-state.
    pub finalized_checkpoint: Checkpoint,
    /// Unrealized justified checkpoint (pulled-up tip).
    pub unrealized_justified_checkpoint: Checkpoint,
    /// Unrealized finalized checkpoint (pulled-up tip).
    pub unrealized_finalized_checkpoint: Checkpoint,
    /// Execution-layer validity (CC-34a / §4.1).
    pub execution_status: ExecutionStatus,
    /// `body.execution_payload.block_hash` for this block (CC-34a / §4.7).
    pub execution_block_hash: Hash256,
}

/// One node in the proto-array.
///
/// `parent`, `best_child`, and `best_descendant` are **internal** indices into
/// the node vector. Callers must not store them across a [`ProtoArray::prune`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoNode {
    /// Block slot.
    pub slot: Slot,
    /// Block root.
    pub root: Root,
    /// Parent index in `nodes`, if any.
    pub parent: Option<usize>,
    /// Post-state root.
    pub state_root: Root,
    /// FFG target root for this block (epoch-boundary ancestor).
    pub target_root: Root,
    /// Justified checkpoint carried by the block's post-state.
    pub justified_checkpoint: Checkpoint,
    /// Finalized checkpoint carried by the block's post-state.
    pub finalized_checkpoint: Checkpoint,
    /// Unrealized justified checkpoint (pulled-up tip).
    pub unrealized_justified_checkpoint: Checkpoint,
    /// Unrealized finalized checkpoint (pulled-up tip).
    pub unrealized_finalized_checkpoint: Checkpoint,
    /// LMD weight (valid only immediately after `get_head` / weight application).
    pub weight: i64,
    /// Best viable child by weight (internal index).
    pub best_child: Option<usize>,
    /// Best viable descendant by weight (internal index).
    pub best_descendant: Option<usize>,
    /// Execution-layer validity. Single source of truth for optimistic state
    /// (ADR P3-10); `is_optimistic` is derived from this field.
    pub execution_status: ExecutionStatus,
    /// `body.execution_payload.block_hash` — required by CC-35 `latestValidHash`
    /// lookup and CC-33's `ForkchoiceStateV1` triple (≠13/3).
    pub execution_block_hash: Hash256,
}

/// Previously applied proposer-boost delta (reversed on the next head pass).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PreviousProposerBoost {
    /// Root that last received the boost (or zero if none).
    pub root: Root,
    /// Boost score last applied (Gwei as i64).
    pub score: i64,
}

/// Proto-array: contiguous nodes with parent links and O(1) root lookup.
#[derive(Debug, Clone)]
pub struct ProtoArray {
    nodes: Vec<ProtoNode>,
    indices: HashMap<Root, usize>,
    justified_checkpoint: Checkpoint,
    finalized_checkpoint: Checkpoint,
    /// Last applied proposer-boost delta (Architecture §6.3 — boost is a delta).
    previous_proposer_boost: PreviousProposerBoost,
}

impl ProtoArray {
    /// Empty array with the given store checkpoints.
    pub fn new(justified_checkpoint: Checkpoint, finalized_checkpoint: Checkpoint) -> Self {
        Self {
            nodes: Vec::new(),
            indices: HashMap::new(),
            justified_checkpoint,
            finalized_checkpoint,
            previous_proposer_boost: PreviousProposerBoost::default(),
        }
    }

    /// Number of nodes currently in the array.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the array has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Borrow the node for `root`, if present.
    pub fn get(&self, root: &Root) -> Option<&ProtoNode> {
        let idx = *self.indices.get(root)?;
        self.nodes.get(idx)
    }

    /// Whether `root` is present.
    pub fn contains(&self, root: &Root) -> bool {
        self.indices.contains_key(root)
    }

    /// Store justified checkpoint mirrored from the fork-choice store.
    pub fn justified_checkpoint(&self) -> Checkpoint {
        self.justified_checkpoint
    }

    /// Store finalized checkpoint mirrored from the fork-choice store.
    pub fn finalized_checkpoint(&self) -> Checkpoint {
        self.finalized_checkpoint
    }

    /// Update the store checkpoints used by viability checks.
    pub fn set_checkpoints(&mut self, justified: Checkpoint, finalized: Checkpoint) {
        self.justified_checkpoint = justified;
        self.finalized_checkpoint = finalized;
    }

    /// Insert a node. Parent (if any) must already be present and will have a
    /// strictly lower index than the new node.
    pub fn on_block(&mut self, block: ProtoNodeBlock) -> Result<(), ProtoArrayError> {
        if self.indices.contains_key(&block.root) {
            return Err(ProtoArrayError::DuplicateRoot(block.root));
        }

        let parent = match block.parent_root {
            Some(pr) => {
                let idx = self
                    .indices
                    .get(&pr)
                    .copied()
                    .ok_or(ProtoArrayError::UnknownParent(pr))?;
                Some(idx)
            }
            None => None,
        };

        let index = self.nodes.len();
        // Parent always has a lower index by construction (append-only insert).
        if let Some(p) = parent {
            debug_assert!(p < index);
        }

        // H-3: Optimistic/Invalid + ZERO collides with CC-35 case-2 LVH sentinel.
        // Enforced at runtime (not debug_assert only) so release builds refuse it.
        // Valid+ZERO remains allowed for array-root / unit-test genesis anchors.
        if !h3_execution_hash_ok(
            block.execution_status,
            block.execution_block_hash,
            parent.is_none(),
        ) {
            return Err(ProtoArrayError::ZeroExecutionBlockHash {
                root: block.root,
                status: block.execution_status,
            });
        }

        self.nodes.push(ProtoNode {
            slot: block.slot,
            root: block.root,
            parent,
            state_root: block.state_root,
            target_root: block.target_root,
            justified_checkpoint: block.justified_checkpoint,
            finalized_checkpoint: block.finalized_checkpoint,
            unrealized_justified_checkpoint: block.unrealized_justified_checkpoint,
            unrealized_finalized_checkpoint: block.unrealized_finalized_checkpoint,
            weight: 0,
            best_child: None,
            best_descendant: None,
            execution_status: block.execution_status,
            execution_block_hash: block.execution_block_hash,
        });
        self.indices.insert(block.root, index);
        Ok(())
    }

    /// Walk `parent` links from `root` until the ancestor at or before `slot`.
    ///
    /// Returns `Err` if `root` is unknown. If every ancestor has `slot > slot`,
    /// returns the oldest ancestor reached.
    pub fn get_ancestor(&self, root: Root, slot: Slot) -> Result<Root, ProtoArrayError> {
        let mut idx = *self
            .indices
            .get(&root)
            .ok_or(ProtoArrayError::UnknownRoot(root))?;

        loop {
            let node = &self.nodes[idx];
            if node.slot.as_u64() <= slot.as_u64() {
                return Ok(node.root);
            }
            match node.parent {
                Some(p) => idx = p,
                None => return Ok(node.root),
            }
        }
    }

    /// Compact the array so `finalized_root` becomes index 0 of the surviving
    /// tree, dropping nodes that are not descendants of it, and rewrite
    /// `indices` / parent links.
    ///
    /// This is the **only** operation that invalidates previously observed
    /// `usize` indices. After prune, `best_child` / `best_descendant` may be
    /// cleared for remapped nodes whose former best was dropped — head
    /// computation must rebuild them.
    pub fn prune(&mut self, finalized_root: Root) -> Result<(), ProtoArrayError> {
        let finalized_idx = *self
            .indices
            .get(&finalized_root)
            .ok_or(ProtoArrayError::UnknownFinalized(finalized_root))?;

        let n = self.nodes.len();
        let mut keep = vec![false; n];
        keep[finalized_idx] = true;
        for i in (finalized_idx + 1)..n {
            if let Some(p) = self.nodes[i].parent
                && keep[p]
            {
                keep[i] = true;
            }
        }

        let mut old_to_new: HashMap<usize, usize> = HashMap::new();
        let mut new_nodes = Vec::new();
        for (old_i, node) in self.nodes.iter().enumerate() {
            if !keep[old_i] {
                continue;
            }
            let new_i = new_nodes.len();
            old_to_new.insert(old_i, new_i);

            let parent = if old_i == finalized_idx {
                None
            } else {
                node.parent.and_then(|p| old_to_new.get(&p).copied())
            };

            let mut new_node = node.clone();
            new_node.parent = parent;
            // Drop best links; they may point at pruned nodes or stale indices.
            // Head pass (CC-15c) rebuilds them.
            new_node.best_child = None;
            new_node.best_descendant = None;
            new_nodes.push(new_node);
        }

        let mut new_indices = HashMap::with_capacity(new_nodes.len());
        for (i, node) in new_nodes.iter().enumerate() {
            new_indices.insert(node.root, i);
        }

        self.nodes = new_nodes;
        self.indices = new_indices;
        Ok(())
    }

    /// Per-node viability using the array's mirrored store checkpoints.
    ///
    /// `slots_per_epoch` is required for voting-source epoch and finalized
    /// ancestry slot math.
    pub fn node_is_viable(
        &self,
        node: &ProtoNode,
        current_epoch: Epoch,
        slots_per_epoch: u64,
    ) -> bool {
        self.node_is_viable_with(
            node,
            self.justified_checkpoint,
            self.finalized_checkpoint,
            current_epoch,
            slots_per_epoch,
        )
    }

    /// Viability with explicit store checkpoints (testable per field).
    ///
    /// See module docs for known gaps vs recursive `filter_block_tree`.
    ///
    /// §4.5: an `Invalid` node is never viable — `Optimistic` remains viable
    /// (optimistic heads are legitimate).
    pub fn node_is_viable_with(
        &self,
        node: &ProtoNode,
        store_justified: Checkpoint,
        store_finalized: Checkpoint,
        current_epoch: Epoch,
        slots_per_epoch: u64,
    ) -> bool {
        // §4.5 — execution-status viability clause.
        if node.execution_status.is_invalidated() {
            return false;
        }
        correct_justified(node, store_justified, current_epoch, slots_per_epoch)
            && correct_finalized(self, node, store_finalized, slots_per_epoch)
    }

    /// Resolve a root to its current index.
    ///
    /// **`pub(crate)` only** — do not store the result across [`Self::prune`].
    /// Prefer keeping a [`Root`] and re-resolving. Reserved for weight / head
    /// passes (CC-15b/c); tests use it to demonstrate stale-index hazards.
    #[cfg_attr(not(test), allow(dead_code))] // consumed by get_head (CC-15c)
    pub(crate) fn index_of(&self, root: &Root) -> Option<usize> {
        self.indices.get(root).copied()
    }

    /// Borrow the root→index map for `compute_deltas` / weight application.
    ///
    /// **`pub(crate)` only** — indices are invalidated by [`Self::prune`].
    pub(crate) fn indices(&self) -> &HashMap<Root, usize> {
        &self.indices
    }

    /// Apply per-node weight deltas in place (`deltas[i]` added to `nodes[i].weight`).
    ///
    /// Called by `get_head` after [`crate::on_attestation::compute_deltas`]
    /// (CC-15c). Length must match the current node count.
    ///
    /// **Weights are only meaningful immediately after this + a full head pass.**
    pub fn apply_weight_deltas(&mut self, deltas: &[i64]) -> Result<(), ProtoArrayError> {
        if deltas.len() != self.nodes.len() {
            return Err(ProtoArrayError::DeltaLengthMismatch {
                got: deltas.len(),
                expected: self.nodes.len(),
            });
        }
        for (node, delta) in self.nodes.iter_mut().zip(deltas.iter()) {
            node.weight = node.weight.saturating_add(*delta);
        }
        Ok(())
    }

    /// Borrow all nodes (read-only public view for diagnostics / tests).
    pub fn nodes(&self) -> &[ProtoNode] {
        &self.nodes
    }

    /// Mutable node slice for in-crate weight application only.
    ///
    /// **`pub(crate)`** so successors cannot hold external `&mut [ProtoNode]`
    /// indices across prune without going through crate code that re-resolves.
    #[allow(dead_code)] // consumed by weight application (CC-15c)
    pub(crate) fn nodes_mut(&mut self) -> &mut [ProtoNode] {
        &mut self.nodes
    }

    /// Record unrealized checkpoints on a node after `compute_pulled_up_tip`.
    ///
    /// Returns `true` if either checkpoint value changed. Callers **must** bump
    /// the store `mutation_counter` when this returns `true` so head-cache /
    /// viability cannot serve a stale result (SEC-15c-2): node unrealized is
    /// the voting source for prior-epoch blocks even when store-level
    /// unrealized does not advance.
    pub fn set_unrealized_checkpoints(
        &mut self,
        root: Root,
        unrealized_justified: Checkpoint,
        unrealized_finalized: Checkpoint,
    ) -> Result<bool, ProtoArrayError> {
        let idx = *self
            .indices
            .get(&root)
            .ok_or(ProtoArrayError::UnknownRoot(root))?;
        let node = &mut self.nodes[idx];
        let changed = node.unrealized_justified_checkpoint != unrealized_justified
            || node.unrealized_finalized_checkpoint != unrealized_finalized;
        node.unrealized_justified_checkpoint = unrealized_justified;
        node.unrealized_finalized_checkpoint = unrealized_finalized;
        Ok(changed)
    }

    /// Roots of **filtered-tree leaves** under the justified root, with current
    /// node weights (post-`get_head` only).
    ///
    /// Matches pyspec `get_viable_for_head_checks`: leaves of
    /// `get_filtered_block_tree` with `get_weight`. A filtered leaf is a node
    /// with no children that passes the per-node viability predicate and is a
    /// descendant of (or equal to) `justified_root`.
    pub fn viable_for_head_leaves(
        &self,
        justified_root: Root,
        current_epoch: Epoch,
        slots_per_epoch: u64,
    ) -> Vec<(Root, i64)> {
        // Indices that appear as a parent (have at least one child).
        let mut has_child = vec![false; self.nodes.len()];
        for node in &self.nodes {
            if let Some(p) = node.parent
                && let Some(slot) = has_child.get_mut(p)
            {
                *slot = true;
            }
        }

        let mut out = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if has_child.get(i).copied().unwrap_or(true) {
                continue; // not a leaf of the full tree
            }
            if !self.node_is_viable(node, current_epoch, slots_per_epoch) {
                continue;
            }
            // Must sit under the justified root (filtered tree base).
            if !self.is_descendant_or_equal(justified_root, node.root) {
                continue;
            }
            out.push((node.root, node.weight));
        }
        // Stable order by root bytes for set comparison independence.
        out.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));
        out
    }

    /// Whether `root` equals `ancestor` or has `ancestor` on its parent chain.
    fn is_descendant_or_equal(&self, ancestor: Root, root: Root) -> bool {
        if ancestor == root {
            return true;
        }
        let Some(anc_idx) = self.indices.get(&ancestor).copied() else {
            return false;
        };
        let Some(mut idx) = self.indices.get(&root).copied() else {
            return false;
        };
        // Walk parents; bound by node count.
        for _ in 0..self.nodes.len() {
            if idx == anc_idx {
                return true;
            }
            match self.nodes.get(idx).and_then(|n| n.parent) {
                Some(p) => idx = p,
                None => return false,
            }
        }
        false
    }

    /// Borrow the previous proposer-boost application (for diagnostics / tests).
    #[inline]
    pub fn previous_proposer_boost(&self) -> PreviousProposerBoost {
        self.previous_proposer_boost
    }

    /// Apply vote deltas, proposer-boost delta, parent weight propagation, and
    /// rebuild `best_child` / `best_descendant` (Architecture §6.1 / §6.3).
    ///
    /// # Proposer boost as a delta
    ///
    /// The previously applied boost score is **reversed** on
    /// `previous_proposer_boost.root`, and the new boost (if any) is applied to
    /// `proposer_boost_root`. Boost is never a persistent field weight.
    ///
    /// # Complexity
    ///
    /// O(N) — two reverse passes over the node vector. Must not call
    /// `get_ancestor` inside the weight loop (CC-15/5).
    #[allow(clippy::too_many_arguments)]
    pub fn apply_score_changes(
        &mut self,
        mut deltas: Vec<i64>,
        justified_checkpoint: Checkpoint,
        finalized_checkpoint: Checkpoint,
        proposer_boost_root: Root,
        proposer_boost_score: i64,
        current_epoch: Epoch,
        slots_per_epoch: u64,
    ) -> Result<(), ProtoArrayError> {
        if deltas.len() != self.nodes.len() {
            return Err(ProtoArrayError::DeltaLengthMismatch {
                got: deltas.len(),
                expected: self.nodes.len(),
            });
        }

        self.justified_checkpoint = justified_checkpoint;
        self.finalized_checkpoint = finalized_checkpoint;

        let mut applied_boost_score = 0_i64;
        let prev_boost = self.previous_proposer_boost;

        // Pass 1: apply deltas + boost reverse/apply + parent back-propagation.
        for node_index in (0..self.nodes.len()).rev() {
            let node_root = self.nodes[node_index].root;
            let mut node_delta = deltas[node_index];

            // Reverse previous proposer boost on its node.
            if prev_boost.root != Root::ZERO && prev_boost.root == node_root {
                node_delta = node_delta
                    .checked_sub(prev_boost.score)
                    .ok_or(ProtoArrayError::WeightOverflow(node_index))?;
            }
            // Apply current proposer boost.
            if proposer_boost_root != Root::ZERO
                && proposer_boost_root == node_root
                && proposer_boost_score != 0
            {
                node_delta = node_delta
                    .checked_add(proposer_boost_score)
                    .ok_or(ProtoArrayError::WeightOverflow(node_index))?;
                applied_boost_score = proposer_boost_score;
            }

            let node = &mut self.nodes[node_index];
            // §4.4 part 2 — Invalid nodes contribute zero upward and hold weight
            // zero. Accumulated weight was removed once at invalidation time
            // (`remove_invalidated_subtree_weight`); here we only stop the flow.
            // Valid / Optimistic / Irrelevant take the identical path below.
            if node.execution_status.is_invalidated() {
                node.weight = 0;
                continue;
            }

            node.weight = node
                .weight
                .checked_add(node_delta)
                .ok_or(ProtoArrayError::WeightOverflow(node_index))?;

            // Back-propagate to parent so ancestor weights include this vote/boost.
            if let Some(parent_index) = node.parent {
                let parent_slot = deltas
                    .get_mut(parent_index)
                    .ok_or(ProtoArrayError::InvalidBestLink(parent_index))?;
                *parent_slot = parent_slot
                    .checked_add(node_delta)
                    .ok_or(ProtoArrayError::WeightOverflow(parent_index))?;
            }
        }

        self.previous_proposer_boost = PreviousProposerBoost {
            root: if applied_boost_score != 0 {
                proposer_boost_root
            } else {
                Root::ZERO
            },
            score: applied_boost_score,
        };

        // Pass 2: rebuild best_child / best_descendant bottom-up.
        for node_index in (0..self.nodes.len()).rev() {
            let parent_index = self.nodes[node_index].parent;
            if let Some(parent_index) = parent_index {
                self.maybe_update_best_child_and_descendant(
                    parent_index,
                    node_index,
                    current_epoch,
                    slots_per_epoch,
                )?;
            }
        }

        Ok(())
    }

    /// Select the head root starting from the justified checkpoint root.
    pub fn find_head(
        &self,
        justified_root: Root,
        current_epoch: Epoch,
        slots_per_epoch: u64,
    ) -> Result<Root, ProtoArrayError> {
        let justified_index = *self
            .indices
            .get(&justified_root)
            .ok_or(ProtoArrayError::UnknownJustified(justified_root))?;
        let justified_node = &self.nodes[justified_index];
        let best_descendant_index = justified_node.best_descendant.unwrap_or(justified_index);
        let best_node = self
            .nodes
            .get(best_descendant_index)
            .ok_or(ProtoArrayError::InvalidBestLink(best_descendant_index))?;

        if !self.node_is_viable(best_node, current_epoch, slots_per_epoch) {
            // Fall back: justified itself may still be the only viable tip
            // (empty best links after prune). Prefer justified when viable.
            if self.node_is_viable(justified_node, current_epoch, slots_per_epoch) {
                return Ok(justified_node.root);
            }
            return Err(ProtoArrayError::NonViableHead(best_node.root));
        }
        Ok(best_node.root)
    }

    fn maybe_update_best_child_and_descendant(
        &mut self,
        parent_index: usize,
        child_index: usize,
        current_epoch: Epoch,
        slots_per_epoch: u64,
    ) -> Result<(), ProtoArrayError> {
        let child = self
            .nodes
            .get(child_index)
            .ok_or(ProtoArrayError::InvalidBestLink(child_index))?
            .clone();
        let parent = self
            .nodes
            .get(parent_index)
            .ok_or(ProtoArrayError::InvalidBestLink(parent_index))?
            .clone();

        let child_leads = self.node_leads_to_viable_head(&child, current_epoch, slots_per_epoch);

        let change_to_none = (None, None);
        let change_to_child = (
            Some(child_index),
            child.best_descendant.or(Some(child_index)),
        );
        let no_change = (parent.best_child, parent.best_descendant);

        let (new_best_child, new_best_descendant) =
            if let Some(best_child_index) = parent.best_child {
                if best_child_index == child_index && !child_leads {
                    change_to_none
                } else if best_child_index == child_index {
                    change_to_child
                } else {
                    let best_child = self
                        .nodes
                        .get(best_child_index)
                        .ok_or(ProtoArrayError::InvalidBestLink(best_child_index))?
                        .clone();
                    let best_leads =
                        self.node_leads_to_viable_head(&best_child, current_epoch, slots_per_epoch);

                    if child_leads && !best_leads {
                        change_to_child
                    } else if !child_leads && best_leads {
                        no_change
                    } else if child.weight == best_child.weight {
                        // Spec / Lighthouse: higher root wins ties.
                        if child.root.as_slice() >= best_child.root.as_slice() {
                            change_to_child
                        } else {
                            no_change
                        }
                    } else if child.weight > best_child.weight {
                        change_to_child
                    } else {
                        no_change
                    }
                }
            } else if child_leads {
                change_to_child
            } else {
                no_change
            };

        let parent = &mut self.nodes[parent_index];
        parent.best_child = new_best_child;
        parent.best_descendant = new_best_descendant;
        Ok(())
    }

    /// Node itself is viable, or its `best_descendant` is viable.
    fn node_leads_to_viable_head(
        &self,
        node: &ProtoNode,
        current_epoch: Epoch,
        slots_per_epoch: u64,
    ) -> bool {
        let best_descendant_viable = node
            .best_descendant
            .and_then(|i| self.nodes.get(i))
            .is_some_and(|d| self.node_is_viable(d, current_epoch, slots_per_epoch));
        best_descendant_viable || self.node_is_viable(node, current_epoch, slots_per_epoch)
    }
}

/// Genesis epoch (`GENESIS_EPOCH = 0`).
const GENESIS_EPOCH: u64 = 0;

/// Spec `get_voting_source` shape: unrealized only when the block is from a
/// **prior** epoch; current-epoch blocks use realized justified.
fn voting_source(node: &ProtoNode, current_epoch: Epoch, slots_per_epoch: u64) -> Checkpoint {
    let spe = slots_per_epoch.max(1);
    let block_epoch = node.slot.as_u64() / spe;
    if current_epoch.as_u64() > block_epoch {
        node.unrealized_justified_checkpoint
    } else {
        node.justified_checkpoint
    }
}

fn correct_justified(
    node: &ProtoNode,
    store_justified: Checkpoint,
    current_epoch: Epoch,
    slots_per_epoch: u64,
) -> bool {
    if store_justified.epoch.as_u64() == GENESIS_EPOCH {
        return true;
    }
    let vs = voting_source(node, current_epoch, slots_per_epoch);
    // Spec leaf rule: same epoch as store justified, or within two epochs of now.
    vs.epoch.as_u64() == store_justified.epoch.as_u64()
        || vs.epoch.as_u64().saturating_add(2) >= current_epoch.as_u64()
}

/// Spec finalized check: ancestor at finalized-epoch start is the finalized root.
fn correct_finalized(
    pa: &ProtoArray,
    node: &ProtoNode,
    store_finalized: Checkpoint,
    slots_per_epoch: u64,
) -> bool {
    if store_finalized.epoch.as_u64() == GENESIS_EPOCH {
        return true;
    }
    let spe = slots_per_epoch.max(1);
    let finalized_slot = Slot::new(store_finalized.epoch.as_u64().saturating_mul(spe));
    match pa.get_ancestor(node.root, finalized_slot) {
        Ok(ancestor) => ancestor == store_finalized.root,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    const SPE: u64 = 8; // minimal slots per epoch

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

    fn insert_chain(pa: &mut ProtoArray, slots_roots: &[(u64, u8, Option<u8>)]) {
        for &(slot, r, parent) in slots_roots {
            let justified = pa.justified_checkpoint;
            let finalized = pa.finalized_checkpoint;
            pa.on_block(ProtoNodeBlock {
                slot: Slot::new(slot),
                root: root(r),
                parent_root: parent.map(root),
                state_root: root(r.wrapping_add(100)),
                target_root: root(r),
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();
        }
    }

    #[test]
    fn insertion_parent_always_lower_index() {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert_chain(
            &mut pa,
            &[
                (0, 1, None),
                (1, 2, Some(1)),
                (2, 3, Some(2)),
                (3, 4, Some(3)),
            ],
        );

        for node in pa.nodes() {
            if let Some(p) = node.parent {
                let self_idx = pa.index_of(&node.root).unwrap();
                assert!(
                    p < self_idx,
                    "parent index {p} must be < child index {self_idx}"
                );
            }
        }
        let justified = pa.justified_checkpoint;
        let finalized = pa.finalized_checkpoint;
        pa.on_block(ProtoNodeBlock {
            slot: Slot::new(2),
            root: root(5),
            parent_root: Some(root(2)),
            state_root: root(105),
            target_root: root(5),
            justified_checkpoint: justified,
            finalized_checkpoint: finalized,
            unrealized_justified_checkpoint: justified,
            unrealized_finalized_checkpoint: finalized,
            execution_status: ExecutionStatus::Valid,
            execution_block_hash: Hash256::ZERO,
        })
        .unwrap();
        let n5 = pa.get(&root(5)).unwrap();
        let i5 = pa.index_of(&root(5)).unwrap();
        assert!(n5.parent.unwrap() < i5);
    }

    #[test]
    fn get_ancestor_respects_slot_bound() {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert_chain(
            &mut pa,
            &[
                (0, 1, None),
                (1, 2, Some(1)),
                (2, 3, Some(2)),
                (5, 4, Some(3)),
            ],
        );

        assert_eq!(pa.get_ancestor(root(4), Slot::new(2)).unwrap(), root(3));
        assert_eq!(pa.get_ancestor(root(4), Slot::new(5)).unwrap(), root(4));
        assert_eq!(pa.get_ancestor(root(4), Slot::new(0)).unwrap(), root(1));
        assert_eq!(pa.get_ancestor(root(4), Slot::new(100)).unwrap(), root(4));
        assert!(matches!(
            pa.get_ancestor(root(99), Slot::new(0)),
            Err(ProtoArrayError::UnknownRoot(_))
        ));
    }

    #[test]
    fn prune_compacts_and_rewrites_indices() {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert_chain(
            &mut pa,
            &[
                (0, 1, None),
                (1, 2, Some(1)),
                (2, 3, Some(2)),
                (3, 4, Some(3)),
                (2, 5, Some(2)),
            ],
        );
        assert_eq!(pa.len(), 5);

        let idx_4_before = pa.index_of(&root(4)).unwrap();
        assert_ne!(pa.index_of(&root(3)).unwrap(), 0);

        pa.prune(root(3)).unwrap();

        assert_eq!(pa.len(), 2);
        assert!(pa.contains(&root(3)));
        assert!(pa.contains(&root(4)));
        assert!(!pa.contains(&root(1)));
        assert!(!pa.contains(&root(2)));
        assert!(!pa.contains(&root(5)));

        let n3 = pa.get(&root(3)).unwrap();
        assert!(n3.parent.is_none());
        assert_eq!(pa.index_of(&root(3)), Some(0));
        let n4 = pa.get(&root(4)).unwrap();
        assert_eq!(n4.parent, Some(0));
        assert_eq!(pa.index_of(&root(4)), Some(1));
        assert_ne!(pa.index_of(&root(4)).unwrap(), idx_4_before);
    }

    #[test]
    fn no_usize_index_retained_across_prune() {
        // Out-of-range case: stale index past new length.
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert_chain(&mut pa, &[(0, 1, None), (1, 2, Some(1)), (2, 3, Some(2))]);

        let stale_index = pa.index_of(&root(3)).unwrap();
        assert_eq!(stale_index, 2);
        assert_eq!(pa.nodes()[stale_index].root, root(3));

        pa.prune(root(2)).unwrap();

        assert!(pa.nodes().get(stale_index).is_none());
        assert_eq!(pa.get(&root(3)).map(|n| n.root), Some(root(3)));
        assert_eq!(pa.index_of(&root(3)), Some(1));
    }

    #[test]
    fn stale_index_in_bounds_maps_to_different_root_after_prune() {
        // SEC-3: when prune keeps enough nodes that a stale index stays
        // in-bounds, it addresses a *different* live root.
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        // indices: 0=1, 1=2, 2=3, 3=4, 4=5
        insert_chain(
            &mut pa,
            &[
                (0, 1, None),
                (1, 2, Some(1)),
                (2, 3, Some(2)),
                (3, 4, Some(3)),
                (4, 5, Some(4)),
            ],
        );

        // Hold index of root 3 (== 2). After prune(3), survivors are 3,4,5 at
        // indices 0,1,2 — so stale index 2 is still in-bounds but is root 5.
        let stale = pa.index_of(&root(3)).unwrap();
        assert_eq!(stale, 2);
        assert_eq!(pa.nodes()[stale].root, root(3));

        pa.prune(root(3)).unwrap();
        assert!(stale < pa.len(), "stale index still in-bounds");
        assert_ne!(
            pa.nodes()[stale].root,
            root(3),
            "stale usize silently addresses a different node"
        );
        // Correct identity only via Root re-resolution.
        assert_eq!(pa.get(&root(3)).map(|n| n.root), Some(root(3)));
        assert_eq!(pa.index_of(&root(3)), Some(0));
    }

    #[test]
    fn viability_accepts_and_rejects_per_checkpoint_field() {
        // Build a chain so finalized ancestry can be checked.
        // Slot SPE * epoch: epoch 2 starts at slot 16, epoch 3 at 24.
        let store_j = cp(3, root(10));
        let store_f = cp(2, root(9));
        let mut pa = ProtoArray::new(store_j, store_f);
        // Anchor at finalized root (epoch 2 start).
        pa.on_block(ProtoNodeBlock {
            slot: Slot::new(16),
            root: root(9),
            parent_root: None,
            state_root: root(90),
            target_root: root(9),
            justified_checkpoint: store_j,
            finalized_checkpoint: store_f,
            unrealized_justified_checkpoint: store_j,
            unrealized_finalized_checkpoint: store_f,
            execution_status: ExecutionStatus::Valid,
            execution_block_hash: Hash256::ZERO,
        })
        .unwrap();
        // Descendant leaf.
        pa.on_block(ProtoNodeBlock {
            slot: Slot::new(40),
            root: root(20),
            parent_root: Some(root(9)),
            state_root: root(21),
            target_root: root(20),
            justified_checkpoint: store_j,
            finalized_checkpoint: store_f,
            unrealized_justified_checkpoint: store_j,
            unrealized_finalized_checkpoint: store_f,
            execution_status: ExecutionStatus::Valid,
            execution_block_hash: Hash256::ZERO,
        })
        .unwrap();

        let current = Epoch::new(5);
        let good = pa.get(&root(20)).unwrap().clone();
        assert!(pa.node_is_viable_with(&good, store_j, store_f, current, SPE));

        // Wrong justified (realized) on a current-epoch block — voting source is
        // realized, so unrealized cannot rescue.
        let current_epoch_block = Epoch::new(10);
        let mut bad_j = good.clone();
        bad_j.slot = Slot::new(10 * SPE); // current-epoch block
        bad_j.justified_checkpoint = cp(6, root(99));
        bad_j.unrealized_justified_checkpoint = cp(9, root(1)); // would pass if wrongly used
        assert!(
            !pa.node_is_viable_with(&bad_j, store_j, store_f, current_epoch_block, SPE),
            "current-epoch leaf must use realized justified as voting source"
        );

        // Prior-epoch block may use unrealized voting source.
        let mut prior = good.clone();
        prior.slot = Slot::new(4 * SPE); // epoch 4 < current 10
        prior.justified_checkpoint = cp(6, root(99));
        prior.unrealized_justified_checkpoint = store_j; // epoch 3 match
        assert!(pa.node_is_viable_with(&prior, store_j, store_f, current_epoch_block, SPE));

        // Node not under finalized root → reject.
        let mut orphan = ProtoArray::new(store_j, store_f);
        orphan
            .on_block(ProtoNodeBlock {
                slot: Slot::new(40),
                root: root(77),
                parent_root: None,
                state_root: root(78),
                target_root: root(77),
                justified_checkpoint: store_j,
                finalized_checkpoint: store_f,
                unrealized_justified_checkpoint: store_j,
                unrealized_finalized_checkpoint: store_f,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();
        let orphan_node = orphan.get(&root(77)).unwrap().clone();
        assert!(
            !orphan.node_is_viable_with(&orphan_node, store_j, store_f, current, SPE),
            "must descend from store finalized root"
        );

        // Genesis store checkpoints always allow.
        let genesis_j = cp(0, root(0));
        let genesis_f = cp(0, root(0));
        assert!(orphan.node_is_viable_with(&orphan_node, genesis_j, genesis_f, current, SPE));
    }
}
