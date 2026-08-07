//! Backwards invalidation walk and its three stop conditions (Architecture §4.7, CC-35b).
//!
//! After `invalidBlock` selection (CC-35a), this module walks **parent-ward** from
//! `head_block_root`, collecting nodes to invalidate, until one of three stop
//! conditions fires:
//!
//! 1. Reached `latest_valid_ancestor` (node's `execution_block_hash` matches).
//! 2. Hit an `Irrelevant` (pre-merge) node.
//! 3. `!latest_valid_ancestor_is_descendant && node.root != head_block_root` —
//!    the supplied hash is junk or pre-finalization, so **do not walk further**.
//!    Without this stop, a bad EL response would invalidate all ancestors and
//!    force a justified-checkpoint exit (self-inflicted outage).
//!
//! `latest_valid_ancestor_is_descendant` is a **conjunction of two named `let`
//! bindings** (CC-35 /5) — collapsing them is how the walk overruns.
//!
//! Weight removal reuses [`crate::remove_invalidated_subtree_weight`] (ADR P3-11 /
//! CC-34a) once on the oldest invalidated root so both halves of §4.4 are proven
//! together after a completed walk (CC-35 /7).
//!
//! The justified-checkpoint **exit** (`fatal!`, counter, process exit) lives in
//! `services/chain/src/invalidation.rs` (CC-35 /8); this module only surfaces
//! whether the justified root was among the invalidated set.
//!
//! # Production wiring (import / fcU — out of this card)
//!
//! 1. One wrapper owns `LatestValidHash` → walk inputs (do not dual-call
//!    [`crate::apply_invalidation`] without floors).
//! 2. On `Ok`: if [`justified_checkpoint_is_invalid`] → chain exit handler.
//! 3. On `Err(ValidBecameInvalid)`: fatal / freeze import (§4.8), store already
//!    unmutated — do not leave Optimistic tips live without a product decision.
//! 4. After successful walk: `apply_score_changes` + `get_head` (best-links).
//! 5. Trusted LVH that invalidates an **Optimistic** justified root → exit is
//!    the intentional MAY (fail-closed); ops need restart budget / log alerts.

use std::collections::HashSet;

use cc_types::primitives::{Hash256, Root};
use thiserror::Error;

use crate::execution_status::{ExecutionStatus, remove_invalidated_subtree_weight};
use crate::invalidation::InvalidationError;
use crate::proto_array::{ProtoArray, ProtoArrayError};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Why the parent-ward walk halted (one of three conditions, or array root).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkStopReason {
    /// Node's `execution_block_hash` equals `latest_valid_ancestor`.
    LatestValidAncestor,
    /// Pre-merge / no-payload ancestor.
    Irrelevant,
    /// Junk or pre-finalization hash: do not invalidate further ancestors.
    JunkOrPrefinalization,
    /// Reached the array root without matching a named stop (finalized floor).
    ArrayRoot,
}

/// Outcome of one backwards walk + descendant pass + weight removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidationWalkOutcome {
    /// Roots transitioned to (or already) `Invalid`, including descendants.
    pub invalidated_roots: Vec<Root>,
    /// Why the parent-ward loop stopped.
    pub stop_reason: WalkStopReason,
    /// Half 1 of the descendant conjunction (CC-35 /5).
    pub head_descends_from_ancestor: bool,
    /// Half 2 of the descendant conjunction (CC-35 /5).
    pub ancestor_is_finalized_or_descends: bool,
    /// `head_descends_from_ancestor && ancestor_is_finalized_or_descends`.
    pub latest_valid_ancestor_is_descendant: bool,
    /// Subtree root used for §4.4 weight removal (oldest invalidated on the chain).
    pub subtree_root: Option<Root>,
    /// Nodes newly counted toward `cc_chain_invalidated_nodes_total`.
    pub nodes_invalidated: usize,
}

/// Errors from the invalidation walk.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidationWalkError {
    /// `head_block_root` is not in the proto-array.
    #[error("unknown head block: {0:?}")]
    UnknownHead(Root),
    /// EL declared a previously-`Valid` block invalid (§4.8).
    #[error(
        "EL consensus failure: Valid execution status became Invalid \
         (block_root={block_root:?}, payload_block_hash={payload_block_hash:?})"
    )]
    ValidBecameInvalid {
        /// Still-`Valid` node the walk tried to invalidate.
        block_root: Root,
        /// That node's `execution_block_hash`.
        payload_block_hash: Hash256,
    },
    /// Proto-array mutation / lookup failed.
    #[error(transparent)]
    ProtoArray(#[from] ProtoArrayError),
}

impl From<InvalidationError> for InvalidationWalkError {
    fn from(e: InvalidationError) -> Self {
        match e {
            InvalidationError::UnknownBlock(r) => Self::UnknownHead(r),
            InvalidationError::ProtoArray(p) => Self::ProtoArray(p),
            InvalidationError::BadLatestValidHashLen { .. } => {
                // Walk never decodes wire fields; map defensively.
                Self::UnknownHead(Root::ZERO)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Conjunction (CC-35 /5) — two named halves, asserted independently in tests
// ---------------------------------------------------------------------------

/// Compute `latest_valid_ancestor_is_descendant` as **two named booleans**.
///
/// Returns `(head_descends_from_ancestor, ancestor_is_finalized_or_descends,
/// conjunction)`.
///
/// A collapsed implementation that only computes one half fails at least one of
/// `descendant_conjunction_first_half` / `descendant_conjunction_second_half`.
pub fn compute_latest_valid_ancestor_is_descendant(
    proto_array: &ProtoArray,
    head_block_root: Root,
    latest_valid_ancestor: Option<Hash256>,
) -> (bool, bool, bool) {
    let latest_valid_ancestor_root =
        latest_valid_ancestor.and_then(|h| find_root_by_execution_hash(proto_array, h));

    // Two named `let` bindings — do not collapse into one expression.
    let head_descends_from_ancestor = latest_valid_ancestor_root
        .map(|ancestor_root| proto_array.is_descendant_or_equal(ancestor_root, head_block_root))
        .unwrap_or(false);

    let ancestor_is_finalized_or_descends = latest_valid_ancestor_root
        .map(|ancestor_root| proto_array.is_finalized_checkpoint_or_descendant(ancestor_root))
        .unwrap_or(false);

    let latest_valid_ancestor_is_descendant =
        head_descends_from_ancestor && ancestor_is_finalized_or_descends;

    (
        head_descends_from_ancestor,
        ancestor_is_finalized_or_descends,
        latest_valid_ancestor_is_descendant,
    )
}

// ---------------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------------

/// Propagate payload invalidation parent-ward from `head_block_root` (§4.7 step 3).
///
/// * `latest_valid_ancestor` — EL-supplied execution hash when meaningful; `None`
///   is the junk / absent case for the walk floor.
/// * `always_invalidate_head` — when true, the head is invalidated even if the
///   LVH is unknown (typical `newPayload` INVALID path).
///
/// After collecting ancestors, invalidates **descendants** of every collected
/// node, then applies §4.4 weight removal once on the oldest invalidated root.
///
/// Does **not** rebuild best-links or recompute head — caller's job after `Ok`.
/// Does **not** exit on justified invalidation — check
/// [`justified_checkpoint_is_invalid`] and hand off to chain.
pub fn propagate_execution_payload_invalidation(
    proto_array: &mut ProtoArray,
    head_block_root: Root,
    latest_valid_ancestor: Option<Hash256>,
    always_invalidate_head: bool,
) -> Result<InvalidationWalkOutcome, InvalidationWalkError> {
    let head_idx = proto_array
        .index_of(&head_block_root)
        .ok_or(InvalidationWalkError::UnknownHead(head_block_root))?;

    // --- CC-35 /5: two named `let` bindings (grep targets; never collapse) ---
    let latest_valid_ancestor_root =
        latest_valid_ancestor.and_then(|h| find_root_by_execution_hash(proto_array, h));

    let head_descends_from_ancestor = latest_valid_ancestor_root
        .map(|ancestor_root| proto_array.is_descendant_or_equal(ancestor_root, head_block_root))
        .unwrap_or(false);

    let ancestor_is_finalized_or_descends = latest_valid_ancestor_root
        .map(|ancestor_root| proto_array.is_finalized_checkpoint_or_descendant(ancestor_root))
        .unwrap_or(false);

    let latest_valid_ancestor_is_descendant =
        head_descends_from_ancestor && ancestor_is_finalized_or_descends;

    /*
     * Step 1 — parent-ward walk: collect ancestors to invalidate.
     *
     * Stop-condition order mirrors Lighthouse `propagate_execution_payload_invalidation`:
     * stop-3 and stop-1 apply only to non-`Irrelevant` nodes; `Irrelevant` is its
     * own arm so stop-2 can fire alone when LVH is the pre-merge base hash.
     */
    let mut invalidated_indices: HashSet<usize> = HashSet::new();
    let mut index = head_idx;
    let stop_reason = 'walk: {
        loop {
            let (node_root, status, exec_hash, parent) = {
                let node = &proto_array.nodes()[index];
                (
                    node.root,
                    node.execution_status,
                    node.execution_block_hash,
                    node.parent,
                )
            };

            match status {
                ExecutionStatus::Valid
                | ExecutionStatus::Invalid
                | ExecutionStatus::Optimistic => {
                    // (3) Junk / pre-finalization: do not walk past the head.
                    if !latest_valid_ancestor_is_descendant && node_root != head_block_root {
                        break 'walk WalkStopReason::JunkOrPrefinalization;
                    }
                    // (1) Reached the last valid execution hash.
                    if latest_valid_ancestor == Some(exec_hash) {
                        break 'walk WalkStopReason::LatestValidAncestor;
                    }

                    // Decide whether this node is a candidate for invalidation.
                    let should_invalidate = node_root != head_block_root
                        || always_invalidate_head
                        || latest_valid_ancestor_is_descendant;

                    if should_invalidate {
                        match status {
                            ExecutionStatus::Valid => {
                                return Err(InvalidationWalkError::ValidBecameInvalid {
                                    block_root: node_root,
                                    payload_block_hash: exec_hash,
                                });
                            }
                            ExecutionStatus::Optimistic | ExecutionStatus::Invalid => {
                                invalidated_indices.insert(index);
                            }
                            ExecutionStatus::Irrelevant => unreachable!("matched above"),
                        }
                    }
                }
                // (2) Pre-merge ancestor — stop without invalidating it.
                ExecutionStatus::Irrelevant => {
                    break 'walk WalkStopReason::Irrelevant;
                }
            }

            match parent {
                Some(p) => index = p,
                None => break 'walk WalkStopReason::ArrayRoot,
            }
        }
    };

    /*
     * Step 2 — forward pass: invalidate all descendants of collected nodes.
     */
    if !invalidated_indices.is_empty() {
        let n = proto_array.len();
        let first = invalidated_indices.iter().copied().min().unwrap_or(0);
        for i in (first + 1)..n {
            if let Some(p) = proto_array.nodes()[i].parent
                && invalidated_indices.contains(&p)
            {
                let status = proto_array.nodes()[i].execution_status;
                let root = proto_array.nodes()[i].root;
                let hash = proto_array.nodes()[i].execution_block_hash;
                match status {
                    ExecutionStatus::Valid => {
                        return Err(InvalidationWalkError::ValidBecameInvalid {
                            block_root: root,
                            payload_block_hash: hash,
                        });
                    }
                    ExecutionStatus::Optimistic
                    | ExecutionStatus::Invalid
                    | ExecutionStatus::Irrelevant => {
                        invalidated_indices.insert(i);
                    }
                }
            }
        }
    }

    /*
     * Step 3 — §4.4 weight removal once on the oldest invalidated root
     * (ADR P3-11). `remove_invalidated_subtree_weight` zeros the whole subtree
     * and subtracts the pre-zero weight from each strict ancestor.
     */
    let subtree_root = invalidated_indices
        .iter()
        .copied()
        .min()
        .map(|i| proto_array.nodes()[i].root);

    let nodes_invalidated = if let Some(root) = subtree_root {
        let bump = count_subtree(proto_array, root);
        remove_invalidated_subtree_weight(proto_array, root)?;
        crate::invalidation::bump_invalidated_nodes(bump);
        bump
    } else {
        0
    };

    let invalidated_roots = if let Some(root) = subtree_root {
        collect_subtree_roots(proto_array, root)
    } else {
        Vec::new()
    };

    Ok(InvalidationWalkOutcome {
        invalidated_roots,
        stop_reason,
        head_descends_from_ancestor,
        ancestor_is_finalized_or_descends,
        latest_valid_ancestor_is_descendant,
        subtree_root,
        nodes_invalidated,
    })
}

/// Whether the store's justified checkpoint root is now `Invalid`.
///
/// Chain service uses this to fire the justified-checkpoint exit (CC-35 /8).
#[inline]
pub fn justified_checkpoint_is_invalid(proto_array: &ProtoArray) -> bool {
    let j = proto_array.justified_checkpoint().root;
    proto_array
        .get(&j)
        .map(|n| n.execution_status.is_invalidated())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn find_root_by_execution_hash(proto_array: &ProtoArray, hash: Hash256) -> Option<Root> {
    proto_array
        .nodes()
        .iter()
        .find(|n| n.execution_block_hash == hash)
        .map(|n| n.root)
}

fn count_subtree(proto_array: &ProtoArray, root: Root) -> usize {
    let Some(invalid_idx) = proto_array.index_of(&root) else {
        return 0;
    };
    let n = proto_array.len();
    let mut in_subtree = vec![false; n];
    in_subtree[invalid_idx] = true;
    let mut count = 1usize;
    for i in (invalid_idx + 1)..n {
        if let Some(p) = proto_array.nodes()[i].parent
            && in_subtree.get(p).copied().unwrap_or(false)
        {
            in_subtree[i] = true;
            count += 1;
        }
    }
    count
}

fn collect_subtree_roots(proto_array: &ProtoArray, root: Root) -> Vec<Root> {
    let Some(invalid_idx) = proto_array.index_of(&root) else {
        return Vec::new();
    };
    let n = proto_array.len();
    let mut in_subtree = vec![false; n];
    in_subtree[invalid_idx] = true;
    let mut roots = vec![root];
    for i in (invalid_idx + 1)..n {
        if let Some(p) = proto_array.nodes()[i].parent
            && in_subtree.get(p).copied().unwrap_or(false)
        {
            in_subtree[i] = true;
            roots.push(proto_array.nodes()[i].root);
        }
    }
    roots
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::proto_array::{ProtoArray, ProtoNodeBlock};
    use cc_types::containers::Checkpoint;
    use cc_types::primitives::{Epoch, Slot};
    use std::sync::Mutex;

    static COUNTER_LOCK: Mutex<()> = Mutex::new(());

    fn root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn hash(b: u8) -> Hash256 {
        Hash256::from([b; 32])
    }

    fn cp(epoch: u64, r: Root) -> Checkpoint {
        Checkpoint {
            epoch: Epoch::new(epoch),
            root: r,
        }
    }

    fn insert(
        pa: &mut ProtoArray,
        slot: u64,
        r: u8,
        parent: Option<u8>,
        status: ExecutionStatus,
        exec: Hash256,
    ) {
        let justified = pa.justified_checkpoint();
        let finalized = pa.finalized_checkpoint();
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
            execution_status: status,
            execution_block_hash: exec,
        })
        .unwrap();
    }

    /// Linear: A(Valid) → B → C → D(head), all post-merge, finalized = A.
    fn linear_post_merge() -> ProtoArray {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(0x11));
        insert(
            &mut pa,
            1,
            2,
            Some(1),
            ExecutionStatus::Optimistic,
            hash(0x22),
        );
        insert(
            &mut pa,
            2,
            3,
            Some(2),
            ExecutionStatus::Optimistic,
            hash(0x33),
        );
        insert(
            &mut pa,
            3,
            4,
            Some(3),
            ExecutionStatus::Optimistic,
            hash(0x44),
        );
        {
            let nodes = pa.nodes_mut();
            nodes[0].weight = 40;
            nodes[1].weight = 30;
            nodes[2].weight = 20;
            nodes[3].weight = 10;
        }
        pa
    }

    // ── CC-35 /4 — three stop conditions, three tests ──────────────────────

    /// Stop condition 1 alone: reach `latest_valid_ancestor`.
    ///
    /// Other two arranged not to fire:
    /// - No Irrelevant nodes on the chain.
    /// - LVH is a known finalized-descendant ancestor of head → third stop off.
    #[test]
    fn walk_stops_at_latest_valid_ancestor() {
        let _g = COUNTER_LOCK.lock().unwrap();
        let mut pa = linear_post_merge();
        // LVH = B's exec hash → stop at B without invalidating B.
        let outcome = propagate_execution_payload_invalidation(
            &mut pa,
            root(4),
            Some(hash(0x22)),
            true,
        )
        .unwrap();

        assert_eq!(outcome.stop_reason, WalkStopReason::LatestValidAncestor);
        assert!(outcome.latest_valid_ancestor_is_descendant);
        // B (match) not invalidated; C and D are.
        assert!(!pa.get(&root(2)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(3)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        assert!(!pa.get(&root(1)).unwrap().execution_status.is_invalidated());
    }

    /// Stop condition 2 alone: hit an `Irrelevant` node.
    ///
    /// Other two arranged not to fire:
    /// - No matching LVH on the chain (`Some(unknown)` so stop-1 never matches a node).
    /// - LVH not a descendant → but we must not fire stop-3 before reaching Irrelevant.
    ///
    /// Arrangement: chain is all descendants of finalized, but LVH is a known
    /// Irrelevant node's hash? That would stop-1 at Irrelevant first...
    ///
    /// Instead: LVH is a hash that **exists** as an ancestor of head and is
    /// finalized-descendant so stop-3 is off, but we place it **below** the
    /// Irrelevant node so the walk hits Irrelevant first... Wait, Irrelevant
    /// nodes sit at the base; LVH matching an Optimistic node above them would
    /// stop-1 first.
    ///
    /// Correct arrangement for stop-2 alone:
    /// - Chain: I1(Irrelevant) → V2(Valid, ZERO-like? no) → O3 → O4(head)
    /// - LVH = None or a junk hash that is **not** found → `latest_valid_ancestor_is_descendant = false`
    /// - But then stop-3 fires at the first non-head node!
    ///
    /// Lighthouse: stop-3 is `!is_descendant && node.root != head`. So with junk
    /// LVH we never reach Irrelevant — stop-3 fires first. The Irrelevant stop
    /// only fires when `latest_valid_ancestor_is_descendant` is **true** (so we
    /// keep walking) but no node's exec hash matches LVH before we hit Irrelevant.
    ///
    /// That means: LVH must resolve to a root that head descends from and that
    /// is finalized-or-descendant — but the LVH **hash** itself must not equal
    /// any node we visit before Irrelevant. Contradiction if LVH maps to a node
    /// on the chain (we'd hit that node's hash as stop-1).
    ///
    /// Resolution: LVH maps to a node that is a finalized-descendant **ancestor
    /// of head in the beacon tree**, but we use a hash that is **not equal** to
    /// the exec hashes of the Optimistic nodes we walk — e.g. LVH is the
    /// finalized Valid node's hash, and between head and that node there is an
    /// Irrelevant? Impossible (Irrelevant is pre-merge base).
    ///
    /// Honest fixture for stop-2: chain
    /// `I1(Irrelevant) → O2 → O3(head)`, LVH = I1's exec hash (ZERO). Then:
    /// - `find_root_by_execution_hash(ZERO)` may hit I1 first.
    /// - head descends from I1, I1 is finalized (anchor) → is_descendant true.
    /// - Walk: head O3, O2, then at I1: status Irrelevant → stop-2 **before**
    ///   comparing hash? Order in our loop: stop-3, stop-1 (hash match), stop-2.
    /// - At I1: stop-3 off (is_descendant true); stop-1: LVH == ZERO == I1.hash →
    ///   would fire stop-1 first!
    ///
    /// So put LVH = a **synthetic** hash that is not on any node, but force
    /// `latest_valid_ancestor_is_descendant` true by... we can't, the helper
    /// derives both from the hash lookup.
    ///
    /// **Lighthouse order** at each node:
    /// ```
    /// if !latest_valid_ancestor_is_descendant && node.root != head { break; }
    /// else if op.latest_valid_ancestor() == Some(hash) { break; }
    /// // then match status; Irrelevant breaks inside the match after invalidate decision
    /// ```
    ///
    /// For Irrelevant stop alone with is_descendant true: need LVH that makes
    /// is_descendant true without matching the Irrelevant node's hash first.
    /// If LVH is Some(hash_of_node_X) and X is above Irrelevant, stop-1 at X.
    /// If X is the Irrelevant node, stop-1 and stop-2 race — hash check is first.
    ///
    /// Practical fixture used by reference tests: is_descendant true via a
    /// **Valid post-merge** ancestor that is also finalized; chain has an
    /// Irrelevant **between** head and that ancestor? Impossible topologically
    /// (Irrelevant only at base).
    ///
    /// So the only honest arrangement for stop-2 "alone" on a realistic chain
    /// is: LVH is zeros / hash of the Irrelevant base, and we treat stop-1 and
    /// stop-2 as both acceptable terminal conditions at that node — but the AC
    /// wants stop_reason == Irrelevant.
    ///
    /// Force stop-2 by ordering: check Irrelevant **before** hash match when
    /// status is Irrelevant. Spec-wise both mean "stop"; the test wants
    /// Irrelevant. Lighthouse checks hash before Irrelevant in the Valid/
    /// Optimistic/Invalid arm, and Irrelevant is a separate Ok arm that breaks.
    /// For an Irrelevant node, it never enters the hash-compare arm first —
    /// look again:
    /// ```
    /// match node_execution_status {
    ///   Valid|Invalid|Optimistic(hash) => {
    ///     if !is_descendant && root != head { break; }
    ///     else if lvh == Some(hash) { break; }
    ///   }
    ///   Irrelevant => break,
    /// }
    /// ```
    /// For Irrelevant, stop-3 is **not** checked inside the match — only the
    /// outer flow. Actually stop-3 is inside the Valid|Invalid|Optimistic arm!
    /// So for Irrelevant nodes, Lighthouse breaks immediately without stop-3.
    ///
    /// Our loop checks stop-3 first for every node. For an Irrelevant head's
    /// parent with junk LVH, stop-3 would fire first. To make Irrelevant fire:
    /// set is_descendant true so stop-3 is off, then at Irrelevant node either
    /// hash doesn't match or we check Irrelevant before hash.
    ///
    /// Fixture: finalized/justified = O2 (not I1). Chain I1 → O2(finalized Valid)
    /// → O3 → O4(head). LVH = O2's hash. Walk: O4, O3, at O2 hash matches →
    /// stop-1, never reaches I1.
    ///
    /// Fixture for Irrelevant: LVH = Some(hash that is_descendant) where the
    /// matched root is **below** Irrelevant — impossible.
    ///
    /// **Use LVH = Some(hash(0xFF))** unknown, but **manually** we need
    /// is_descendant true. Can't with the pure helper.
    ///
    /// Alternative reading of the AC: "arrange the other two conditions not to
    /// fire" means for the Irrelevant test:
    /// 1. No node on the walked prefix has exec hash == LVH until after we'd
    ///    have stopped (so stop-1 doesn't fire on Optimistic nodes).
    /// 2. is_descendant is true so stop-3 doesn't fire.
    ///
    /// The only way is LVH corresponding to a beacon root that is an ancestor
    /// of head and finalized-descendant, whose **execution hash** is only found
    /// at a node we never reach... if the map is first-hit and an earlier node
    /// has the same hash, etc.
    ///
    /// Simplest approach matching Lighthouse's match structure: check status
    /// Irrelevant in the same position as Lighthouse (after the stop-3/hash
    /// checks only for non-Irrelevant). For Irrelevant-only test:
    /// - Chain: I1 → O2 → O3(head), finalized=I1, LVH=None.
    /// - With LVH=None, is_descendant=false → stop-3 at O2.
    /// - That tests stop-3, not Irrelevant.
    ///
    /// Change order to match Lighthouse exactly: only apply stop-3 and stop-1
    /// inside non-Irrelevant arms. Then with LVH=None, is_descendant=false:
    /// - At head O3 (Optimistic): stop-3 skipped (is head), stop-1 no, invalidate.
    /// - At O2: stop-3 fires (junk). Never reaches I1.
    ///
    /// With LVH=Some(I1.hash)=ZERO, is_descendant true (head from I1, I1 finalized):
    /// - O3, O2 invalidated; at I1: Irrelevant arm → stop-2.
    /// - But Lighthouse also has `else if lvh == Some(hash)` only in non-Irrelevant.
    /// - At I1 Irrelevant → break with Irrelevant. Perfect! stop-1 doesn't apply.
    ///
    /// And stop-3: is_descendant true so wouldn't fire. stop-1: not checked on
    /// Irrelevant. So stop-2 alone. ✓
    ///
    /// Implement Lighthouse match order.
    #[test]
    fn walk_stops_at_irrelevant_node() {
        let _g = COUNTER_LOCK.lock().unwrap();
        // I1(Irrelevant, finalized) → O2 → O3(head)
        // LVH = I1's ZERO hash → is_descendant true; stop at Irrelevant (not hash arm).
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert(
            &mut pa,
            0,
            1,
            None,
            ExecutionStatus::Irrelevant,
            Hash256::ZERO,
        );
        insert(
            &mut pa,
            1,
            2,
            Some(1),
            ExecutionStatus::Optimistic,
            hash(0x22),
        );
        insert(
            &mut pa,
            2,
            3,
            Some(2),
            ExecutionStatus::Optimistic,
            hash(0x33),
        );
        {
            let nodes = pa.nodes_mut();
            nodes[0].weight = 30;
            nodes[1].weight = 20;
            nodes[2].weight = 10;
        }

        let outcome = propagate_execution_payload_invalidation(
            &mut pa,
            root(3),
            Some(Hash256::ZERO),
            true,
        )
        .unwrap();

        assert_eq!(
            outcome.stop_reason,
            WalkStopReason::Irrelevant,
            "Irrelevant stop must fire (not LatestValidAncestor / Junk)"
        );
        // I1 not invalidated; O2 and O3 are.
        assert_eq!(
            pa.get(&root(1)).unwrap().execution_status,
            ExecutionStatus::Irrelevant
        );
        assert!(pa.get(&root(2)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(3)).unwrap().execution_status.is_invalidated());
    }

    /// Stop condition 3 alone: junk / pre-finalization hash.
    ///
    /// Other two arranged not to fire:
    /// - No Irrelevant nodes.
    /// - LVH is a random hash matching no node → stop-1 never fires.
    /// - is_descendant false → stop-3 fires on the first parent of head.
    #[test]
    fn walk_stops_on_junk_or_prefinalization_hash() {
        let _g = COUNTER_LOCK.lock().unwrap();
        let mut pa = linear_post_merge();
        // Junk hash — no node has exec hash 0xAB.
        let outcome = propagate_execution_payload_invalidation(
            &mut pa,
            root(4),
            Some(hash(0xAB)),
            true,
        )
        .unwrap();

        assert_eq!(outcome.stop_reason, WalkStopReason::JunkOrPrefinalization);
        assert!(!outcome.latest_valid_ancestor_is_descendant);
        // Only the head is invalidated (third condition's consequence).
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        assert!(!pa.get(&root(3)).unwrap().execution_status.is_invalidated());
        assert!(!pa.get(&root(2)).unwrap().execution_status.is_invalidated());
        assert!(!pa.get(&root(1)).unwrap().execution_status.is_invalidated());
    }

    // ── CC-35 /5 — two halves asserted independently ───────────────────────

    /// First half false, second true → conjunction false.
    ///
    /// Ancestor is finalized (or descends from it) but head does **not** descend
    /// from the ancestor (ancestor on a sibling branch).
    #[test]
    fn descendant_conjunction_first_half() {
        // A(finalized) ─┬─ B ── C(head)
        //               └─ S (LVH points here)
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(0x11));
        insert(
            &mut pa,
            1,
            2,
            Some(1),
            ExecutionStatus::Optimistic,
            hash(0x22),
        );
        insert(
            &mut pa,
            2,
            3,
            Some(2),
            ExecutionStatus::Optimistic,
            hash(0x33),
        );
        insert(
            &mut pa,
            1,
            6,
            Some(1),
            ExecutionStatus::Optimistic,
            hash(0x66),
        );

        let (head_descends_from_ancestor, ancestor_is_finalized_or_descends, conjunction) =
            compute_latest_valid_ancestor_is_descendant(&pa, root(3), Some(hash(0x66)));

        assert!(
            !head_descends_from_ancestor,
            "head C does not descend from sibling S"
        );
        assert!(
            ancestor_is_finalized_or_descends,
            "S descends from finalized A"
        );
        assert!(
            !conjunction,
            "conjunction must be false when first half is false"
        );
    }

    /// First half true, second false → conjunction false.
    ///
    /// Head descends from the ancestor, but the ancestor is **pre-finalization**
    /// (not the finalized checkpoint and not a descendant of it).
    #[test]
    fn descendant_conjunction_second_half() {
        // Pre-finalization P → F(finalized) → B → C(head)
        // LVH = P's hash: head descends from P, but P is not finalized-or-descendant.
        let finalized = cp(1, root(2));
        let justified = cp(1, root(2));
        let mut pa = ProtoArray::new(justified, finalized);
        // P is the array root but finalized checkpoint points at F, not P.
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(0x11));
        insert(&mut pa, 1, 2, Some(1), ExecutionStatus::Valid, hash(0x22));
        insert(
            &mut pa,
            2,
            3,
            Some(2),
            ExecutionStatus::Optimistic,
            hash(0x33),
        );
        insert(
            &mut pa,
            3,
            4,
            Some(3),
            ExecutionStatus::Optimistic,
            hash(0x44),
        );
        // Finalized is F=root(2); P=root(1) is a strict ancestor of finalized.

        let (head_descends_from_ancestor, ancestor_is_finalized_or_descends, conjunction) =
            compute_latest_valid_ancestor_is_descendant(&pa, root(4), Some(hash(0x11)));

        assert!(
            head_descends_from_ancestor,
            "head descends from pre-finalization P"
        );
        assert!(
            !ancestor_is_finalized_or_descends,
            "P is not finalized and does not descend from finalized"
        );
        assert!(
            !conjunction,
            "conjunction must be false when second half is false"
        );
    }

    // ── CC-35 /6 — pre-finalization floor ──────────────────────────────────

    /// Pre-finalization hash invalidates **only** the block in question.
    ///
    /// Honest situation (recorded): for a checkpoint-synced, latest-fork-only
    /// client `Irrelevant` never occurs live and no walk goes past finalization.
    /// The `Irrelevant` stop is implemented for spec fidelity; this test is the
    /// practical floor.
    #[test]
    fn prefinalization_hash_invalidates_only_one() {
        let _g = COUNTER_LOCK.lock().unwrap();
        // P(pre-fin) → F(finalized Valid) → B → C(head)
        // LVH = P's exec hash → second half false → only head invalidated.
        let finalized = cp(1, root(2));
        let justified = cp(1, root(2));
        let mut pa = ProtoArray::new(justified, finalized);
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(0x11));
        insert(&mut pa, 1, 2, Some(1), ExecutionStatus::Valid, hash(0x22));
        insert(
            &mut pa,
            2,
            3,
            Some(2),
            ExecutionStatus::Optimistic,
            hash(0x33),
        );
        insert(
            &mut pa,
            3,
            4,
            Some(3),
            ExecutionStatus::Optimistic,
            hash(0x44),
        );
        {
            let nodes = pa.nodes_mut();
            nodes[0].weight = 40;
            nodes[1].weight = 30;
            nodes[2].weight = 20;
            nodes[3].weight = 10;
        }

        let outcome = propagate_execution_payload_invalidation(
            &mut pa,
            root(4),
            Some(hash(0x11)), // pre-finalization
            true,
        )
        .unwrap();

        assert_eq!(outcome.stop_reason, WalkStopReason::JunkOrPrefinalization);
        assert!(!outcome.latest_valid_ancestor_is_descendant);
        // Only the head (block in question) invalidated; no ancestor touched.
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        for r in [1u8, 2, 3] {
            assert!(
                !pa.get(&root(r)).unwrap().execution_status.is_invalidated(),
                "ancestor {r} must not be touched (pre-finalization floor)"
            );
        }
    }

    // ── CC-35 /7 — §4.4 weight assertion re-run after the walk ─────────────

    /// Same three-branch fixture as CC-34a `invalidated_weight_leaves_ancestors`,
    /// but the invalidation is driven by a **completed walk**. For every strict
    /// ancestor A of the subtree root:
    /// `A.weight_after == A.weight_before − invalidBlock.weight_before`.
    #[test]
    fn ancestor_weight_after_invalidation_walk() {
        let _g = COUNTER_LOCK.lock().unwrap();
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        // A(1) ─┬─ B(2)
        //       ├─ C(3) ── D(4) ── E(5)   ← walk from E, LVH = C's hash
        //       └─ F(6)                     → invalidBlock subtree = D+E
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(1));
        insert(&mut pa, 1, 2, Some(1), ExecutionStatus::Valid, hash(2));
        insert(&mut pa, 1, 3, Some(1), ExecutionStatus::Valid, hash(3));
        insert(
            &mut pa,
            2,
            4,
            Some(3),
            ExecutionStatus::Optimistic,
            hash(4),
        );
        insert(
            &mut pa,
            3,
            5,
            Some(4),
            ExecutionStatus::Optimistic,
            hash(5),
        );
        insert(&mut pa, 1, 6, Some(1), ExecutionStatus::Valid, hash(6));
        {
            let nodes = pa.nodes_mut();
            // indices: 0=A, 1=B, 2=C, 3=D, 4=E, 5=F
            nodes[0].weight = 100; // A
            nodes[1].weight = 20; // B
            nodes[2].weight = 50; // C
            nodes[3].weight = 30; // D
            nodes[4].weight = 10; // E
            nodes[5].weight = 15; // F
        }

        // Walk from E with LVH = C → stop at C; invalidate D and E.
        // Subtree root for weight removal = D (oldest invalidated).
        let invalid_subtree_root = root(4); // D
        let w_before = pa.get(&invalid_subtree_root).unwrap().weight;
        assert_eq!(w_before, 30);

        let mut ancestors: Vec<(Root, i64)> = Vec::new();
        let mut idx = pa.get(&invalid_subtree_root).unwrap().parent;
        while let Some(p) = idx {
            let n = &pa.nodes()[p];
            ancestors.push((n.root, n.weight));
            idx = n.parent;
        }
        assert_eq!(ancestors.len(), 2);
        let b_before = pa.get(&root(2)).unwrap().weight;
        let f_before = pa.get(&root(6)).unwrap().weight;

        let outcome = propagate_execution_payload_invalidation(
            &mut pa,
            root(5), // head = E
            Some(hash(3)), // LVH = C
            true,
        )
        .unwrap();
        assert_eq!(outcome.stop_reason, WalkStopReason::LatestValidAncestor);
        assert_eq!(outcome.subtree_root, Some(root(4)));

        // --- numeric assertion (primary; ADR P3-11 both halves) -------------
        for (r, before) in &ancestors {
            let after = pa.get(r).unwrap().weight;
            assert_eq!(
                after,
                before - w_before,
                "ancestor {r:?}: weight_after ({after}) == weight_before ({before}) − invalid.weight_before ({w_before})"
            );
        }
        assert_eq!(pa.get(&root(3)).unwrap().weight, 20); // 50 − 30
        assert_eq!(pa.get(&root(1)).unwrap().weight, 70); // 100 − 30
        assert_eq!(pa.get(&root(4)).unwrap().weight, 0);
        assert_eq!(pa.get(&root(5)).unwrap().weight, 0);
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(5)).unwrap().execution_status.is_invalidated());
        // Siblings untouched.
        assert_eq!(pa.get(&root(2)).unwrap().weight, b_before);
        assert_eq!(pa.get(&root(6)).unwrap().weight, f_before);
        assert!(!pa.get(&root(3)).unwrap().execution_status.is_invalidated()); // C is LVH stop
    }

    #[test]
    fn justified_flag_reads_status() {
        let mut pa = linear_post_merge();
        assert!(!justified_checkpoint_is_invalid(&pa));
        // Force-mark justified (root 1) Invalid via direct write for the flag test.
        pa.nodes_mut()[0].execution_status = ExecutionStatus::Invalid;
        assert!(justified_checkpoint_is_invalid(&pa));
    }
}
