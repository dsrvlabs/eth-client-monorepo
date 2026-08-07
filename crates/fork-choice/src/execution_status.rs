//! Execution-layer validity on the proto-array (Architecture §4.1, §4.4, CC-34a).
//!
//! `ProtoNode.execution_status` is the **single** source of truth for optimistic
//! bookkeeping (ADR P3-10). There is no parallel optimistic-root set —
//! [`is_optimistic`] is derived from the node field.
//!
//! §4.4 weight handling is two parts, and only both together are correct
//! (ADR P3-11):
//! 1. At invalidation time, subtract `invalidBlock.weight` (read **before** any
//!    zeroing) **once** from each strict ancestor of the subtree root, then zero
//!    the subtree ([`remove_invalidated_subtree_weight`]).
//! 2. Suppress future upward propagation for `Invalid` nodes inside
//!    [`crate::proto_array::ProtoArray::apply_score_changes`].

use cc_state_transition::PayloadStatus;
use cc_types::preset::Preset;
use cc_types::primitives::{Hash256, Root};

use crate::proto_array::{ProtoArray, ProtoArrayError};
use crate::store::Store;

/// Execution-layer validity of this block's payload (`sync/optimistic.md`).
///
/// `Optimistic` is the spec's `NOT_VALIDATED` (`SYNCING | ACCEPTED`); `Invalid`
/// is its `INVALIDATED` (`INVALID | INVALID_BLOCK_HASH`). `Irrelevant` is the
/// pre-merge case, which a latest-fork-only checkpoint-synced client never
/// produces — kept because the invalidation walk's stop conditions are written
/// against it (§4.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionStatus {
    /// EL accepted the payload as valid.
    #[default]
    Valid,
    /// EL rejected the payload (`INVALIDATED`).
    Invalid,
    /// EL has not yet validated the payload (`NOT_VALIDATED`).
    Optimistic,
    /// Pre-merge / no execution payload.
    Irrelevant,
}

impl ExecutionStatus {
    /// Spec `INVALIDATED` — this node must never be head and must not contribute weight.
    #[inline]
    pub const fn is_invalidated(self) -> bool {
        matches!(self, Self::Invalid)
    }

    /// Spec `NOT_VALIDATED` — optimistic import.
    #[inline]
    pub const fn is_not_validated(self) -> bool {
        matches!(self, Self::Optimistic)
    }

    /// Map the five-value engine [`PayloadStatus`] onto the four-value node status.
    ///
    /// | PayloadStatus | ExecutionStatus |
    /// |---|---|
    /// | `Valid` | `Valid` |
    /// | `Syncing`, `Accepted` | `Optimistic` |
    /// | `Invalid`, `InvalidBlockHash` | `Invalid` |
    #[inline]
    pub const fn from_payload_status(status: &PayloadStatus) -> Self {
        match status {
            PayloadStatus::Valid => Self::Valid,
            PayloadStatus::Syncing | PayloadStatus::Accepted => Self::Optimistic,
            PayloadStatus::Invalid { .. } | PayloadStatus::InvalidBlockHash => Self::Invalid,
        }
    }
}

/// Whether `root` is an optimistically imported block (`NOT_VALIDATED`).
///
/// Derived from [`ProtoNode::execution_status`](crate::proto_array::ProtoNode) —
/// **no** parallel set is consulted (ADR P3-10). Returns `None` if the root has
/// no proto-array node (e.g. header-only partial before resume).
pub fn is_optimistic<P: Preset>(store: &Store<P>, root: Root) -> Option<bool> {
    store
        .proto_array()
        .get(&root)
        .map(|n| n.execution_status == ExecutionStatus::Optimistic)
}

/// §4.4 part 1 — remove the invalidated subtree's accumulated weight once.
///
/// ```text
/// w := invalidBlock.weight              # READ BEFORE any zeroing
/// for N in subtree(invalidBlock):       # invalidBlock included
///     N.weight := 0
///     N.execution_status := Invalid
/// for A in strict_ancestors(invalidBlock):
///     A.weight -= w
/// ```
///
/// **`invalidBlock.weight` is already the whole subtree total** (ancestor weights
/// are cumulative). Subtracting it once from each strict ancestor of the subtree
/// root removes all of it. Walking strict ancestors from *every* node in the
/// subtree over-subtracts by the subtree's node count — that is the trap this
/// function is written to avoid.
///
/// Future vote/boost contributions are suppressed separately inside
/// `apply_score_changes` (part 2). Both halves are required.
pub fn remove_invalidated_subtree_weight(
    proto_array: &mut ProtoArray,
    invalid_root: Root,
) -> Result<(), ProtoArrayError> {
    let invalid_idx = proto_array
        .index_of(&invalid_root)
        .ok_or(ProtoArrayError::UnknownRoot(invalid_root))?;

    // READ BEFORE any zeroing — load-bearing for the numeric invariant.
    let w = proto_array.nodes()[invalid_idx].weight;
    let parent_of_invalid = proto_array.nodes()[invalid_idx].parent;

    let n = proto_array.len();
    // Subtree = invalidBlock + descendants (parent always has lower index).
    let mut in_subtree = vec![false; n];
    in_subtree[invalid_idx] = true;
    for i in (invalid_idx + 1)..n {
        if let Some(p) = proto_array.nodes()[i].parent
            && in_subtree.get(p).copied().unwrap_or(false)
        {
            in_subtree[i] = true;
        }
    }

    {
        let nodes = proto_array.nodes_mut();
        for (i, flag) in in_subtree.iter().enumerate() {
            if *flag {
                nodes[i].weight = 0;
                nodes[i].execution_status = ExecutionStatus::Invalid;
            }
        }
    }

    // Strict ancestors of the subtree root only — once each, subtract w.
    let mut idx = parent_of_invalid;
    while let Some(p) = idx {
        let node = &mut proto_array.nodes_mut()[p];
        node.weight = node
            .weight
            .checked_sub(w)
            .ok_or(ProtoArrayError::WeightOverflow(p))?;
        idx = node.parent;
    }

    Ok(())
}

/// H-3: engine-reported post-merge statuses must not claim the all-zeros
/// execution hash (case-2 sentinel for `latestValidHash` in CC-35).
///
/// `Valid` + ZERO is tolerated for array-root anchors / unit-test genesis seeds
/// (`is_array_root` is reserved for a future tightening). `Optimistic` /
/// `Invalid` always require a real hash. The partial-import path enforces the
/// stronger rule by declining rather than writing ZERO (see `on_block`).
#[inline]
pub(crate) fn h3_execution_hash_ok(
    status: ExecutionStatus,
    execution_block_hash: Hash256,
    _is_array_root: bool,
) -> bool {
    match status {
        ExecutionStatus::Irrelevant | ExecutionStatus::Valid => true,
        ExecutionStatus::Optimistic | ExecutionStatus::Invalid => {
            execution_block_hash != Hash256::ZERO
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::proto_array::{ProtoArray, ProtoArrayError, ProtoNodeBlock};
    use cc_types::containers::Checkpoint;
    use cc_types::primitives::{Epoch, Slot};

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

    /// CC-34 /2 — numeric ancestor-weight assertion (not merely "head moved").
    ///
    /// Three-branch tree with non-trivial weights. After invalidating one
    /// branch: for every strict ancestor `A` of `invalidBlock`,
    /// `A.weight_after == A.weight_before − invalidBlock.weight_before`.
    #[test]
    fn invalidated_weight_leaves_ancestors() {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        // A(1) ─┬─ B(2) weight-only sibling branch
        //       ├─ C(3) ── D(4) ── E(5)   ← invalidate at D (subtree D+E)
        //       └─ F(6)
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(1));
        insert(&mut pa, 1, 2, Some(1), ExecutionStatus::Valid, hash(2));
        insert(&mut pa, 1, 3, Some(1), ExecutionStatus::Valid, hash(3));
        insert(&mut pa, 2, 4, Some(3), ExecutionStatus::Valid, hash(4));
        insert(&mut pa, 3, 5, Some(4), ExecutionStatus::Valid, hash(5));
        insert(&mut pa, 1, 6, Some(1), ExecutionStatus::Valid, hash(6));

        // Assign cumulative-style weights directly (as after a head pass).
        // D's weight is the subtree total that ancestors above D include.
        // E=10, D=30 (includes E), C=50 (includes D), A=100 (includes all).
        {
            let nodes = pa.nodes_mut();
            // indices: 0=A, 1=B, 2=C, 3=D, 4=E, 5=F
            nodes[0].weight = 100; // A
            nodes[1].weight = 20; // B
            nodes[2].weight = 50; // C
            nodes[3].weight = 30; // D  ← invalidBlock
            nodes[4].weight = 10; // E
            nodes[5].weight = 15; // F
        }

        let invalid = root(4);
        let w_before = pa.get(&invalid).unwrap().weight;
        assert_eq!(w_before, 30);

        // Capture every strict ancestor of D before invalidation.
        let mut ancestors: Vec<(Root, i64)> = Vec::new();
        let mut idx = pa.get(&invalid).unwrap().parent;
        while let Some(p) = idx {
            let n = &pa.nodes()[p];
            ancestors.push((n.root, n.weight));
            idx = n.parent;
        }
        assert_eq!(ancestors.len(), 2, "strict ancestors of D are C and A");
        assert_eq!(ancestors[0].0, root(3)); // C
        assert_eq!(ancestors[1].0, root(1)); // A

        // Sibling branch weights before (must be untouched).
        let b_before = pa.get(&root(2)).unwrap().weight;
        let f_before = pa.get(&root(6)).unwrap().weight;

        remove_invalidated_subtree_weight(&mut pa, invalid).unwrap();

        // --- numeric assertion (primary) ------------------------------------
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

        // Subtree zeroed + marked Invalid.
        assert_eq!(pa.get(&root(4)).unwrap().weight, 0);
        assert_eq!(pa.get(&root(5)).unwrap().weight, 0);
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(5)).unwrap().execution_status.is_invalidated());

        // Siblings untouched.
        assert_eq!(pa.get(&root(2)).unwrap().weight, b_before);
        assert_eq!(pa.get(&root(6)).unwrap().weight, f_before);

        // --- head-moves assertion (secondary; passes even with the weight bug) -
        let deltas = vec![0i64; pa.len()];
        pa.apply_score_changes(deltas, anchor, anchor, Root::ZERO, 0, Epoch::new(0), 8)
            .unwrap();
        let head = pa.find_head(root(1), Epoch::new(0), 8).unwrap();
        assert_ne!(head, root(5), "head must leave the invalidated branch");
        assert_ne!(head, root(4));
    }

    /// R-2: Valid / Optimistic / Irrelevant take an identical path through
    /// `apply_score_changes` — only Invalid branches.
    #[test]
    fn valid_optimistic_irrelevant_take_same_path() {
        fn run(status: ExecutionStatus) -> Vec<i64> {
            let anchor = cp(0, root(1));
            let mut pa = ProtoArray::new(anchor, anchor);
            insert(&mut pa, 0, 1, None, status, hash(1));
            insert(&mut pa, 1, 2, Some(1), status, hash(2));
            insert(&mut pa, 2, 3, Some(2), status, hash(3));

            // Non-trivial deltas so weights differ from zero.
            let deltas = vec![5i64, 10, 7];
            pa.apply_score_changes(deltas, anchor, anchor, Root::ZERO, 0, Epoch::new(0), 8)
                .unwrap();
            pa.nodes().iter().map(|n| n.weight).collect()
        }

        let valid = run(ExecutionStatus::Valid);
        let optimistic = run(ExecutionStatus::Optimistic);
        let irrelevant = run(ExecutionStatus::Irrelevant);
        assert_eq!(valid, optimistic, "Valid vs Optimistic weight vectors");
        assert_eq!(valid, irrelevant, "Valid vs Irrelevant weight vectors");
        assert_ne!(valid, vec![0, 0, 0], "deltas must produce non-zero weights");
    }

    #[test]
    fn is_optimistic_is_derived() {
        use crate::da_seam::HarnessAvailability;
        use crate::on_block::get_forkchoice_store;
        use crate::store::Store;
        use cc_state_transition::StubOptimisticEngine;
        use cc_types::preset::Minimal;
        use cc_types::{BeaconBlock, BeaconState};
        use std::sync::Arc;

        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        let anchor_block = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: Default::default(),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let mut store: Store<Minimal> = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(StubOptimisticEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        let anchor = store.justified_checkpoint().root;
        assert_eq!(is_optimistic(&store, anchor), Some(false));

        // Insert an optimistic child by direct proto-array write.
        let child = root(0x42);
        let justified = store.justified_checkpoint();
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: child,
                parent_root: Some(anchor),
                state_root: Root::ZERO,
                target_root: child,
                justified_checkpoint: justified,
                finalized_checkpoint: justified,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: justified,
                execution_status: ExecutionStatus::Optimistic,
                execution_block_hash: hash(0x42),
            })
            .unwrap();

        assert_eq!(is_optimistic(&store, child), Some(true));
        assert_eq!(is_optimistic(&store, root(0xFF)), None);
        // No parallel optimistic-root set — derivation is the only path.
    }

    #[test]
    fn h3_refuses_optimistic_or_invalid_with_zero_hash() {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(1));

        for status in [ExecutionStatus::Optimistic, ExecutionStatus::Invalid] {
            let err = pa
                .on_block(ProtoNodeBlock {
                    slot: Slot::new(1),
                    root: root(0x99),
                    parent_root: Some(root(1)),
                    state_root: root(0x99),
                    target_root: root(0x99),
                    justified_checkpoint: anchor,
                    finalized_checkpoint: anchor,
                    unrealized_justified_checkpoint: anchor,
                    unrealized_finalized_checkpoint: anchor,
                    execution_status: status,
                    execution_block_hash: Hash256::ZERO,
                })
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    ProtoArrayError::ZeroExecutionBlockHash { status: s, .. } if s == status
                ),
                "expected ZeroExecutionBlockHash for {status:?}, got {err:?}"
            );
            assert!(!pa.contains(&root(0x99)));
        }

        // Valid + ZERO still accepted (anchor / genesis seed path).
        insert(&mut pa, 0, 2, None, ExecutionStatus::Valid, Hash256::ZERO);
        assert!(pa.contains(&root(2)));
    }

    #[test]
    fn execution_block_hash_round_trips() {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        let want = hash(0xDE);
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, want);
        assert_eq!(pa.get(&root(1)).unwrap().execution_block_hash, want);
    }

    #[test]
    fn head_viability_excludes_invalid() {
        let anchor = cp(0, root(1));
        let mut pa = ProtoArray::new(anchor, anchor);
        insert(&mut pa, 0, 1, None, ExecutionStatus::Valid, hash(1));
        insert(&mut pa, 1, 2, Some(1), ExecutionStatus::Valid, hash(2));
        insert(&mut pa, 1, 3, Some(1), ExecutionStatus::Invalid, hash(3));

        // Give the invalid branch more weight so head would prefer it without §4.5.
        {
            let nodes = pa.nodes_mut();
            nodes[1].weight = 10; // valid child
            nodes[2].weight = 100; // invalid child
        }
        pa.apply_score_changes(
            vec![0, 0, 0],
            anchor,
            anchor,
            Root::ZERO,
            0,
            Epoch::new(0),
            8,
        )
        .unwrap();

        let head = pa.find_head(root(1), Epoch::new(0), 8).unwrap();
        assert_ne!(head, root(3), "find_head must never select an Invalid node");
        assert_eq!(head, root(2));

        // All-invalid subtree under justified: justified itself remains viable.
        let mut pa2 = ProtoArray::new(anchor, anchor);
        insert(&mut pa2, 0, 1, None, ExecutionStatus::Valid, hash(1));
        insert(&mut pa2, 1, 2, Some(1), ExecutionStatus::Invalid, hash(2));
        insert(&mut pa2, 2, 3, Some(2), ExecutionStatus::Invalid, hash(3));
        pa2.apply_score_changes(
            vec![0, 0, 0],
            anchor,
            anchor,
            Root::ZERO,
            0,
            Epoch::new(0),
            8,
        )
        .unwrap();
        let head2 = pa2.find_head(root(1), Epoch::new(0), 8).unwrap();
        assert_eq!(
            head2,
            root(1),
            "all-invalid descendants → justified remains head"
        );
        assert!(!pa2.node_is_viable(pa2.get(&root(2)).unwrap(), Epoch::new(0), 8));
    }

    #[test]
    fn payload_status_maps_to_execution_status() {
        use cc_state_transition::PayloadStatus;
        assert_eq!(
            ExecutionStatus::from_payload_status(&PayloadStatus::Valid),
            ExecutionStatus::Valid
        );
        assert_eq!(
            ExecutionStatus::from_payload_status(&PayloadStatus::Syncing),
            ExecutionStatus::Optimistic
        );
        assert_eq!(
            ExecutionStatus::from_payload_status(&PayloadStatus::Accepted),
            ExecutionStatus::Optimistic
        );
        assert_eq!(
            ExecutionStatus::from_payload_status(&PayloadStatus::Invalid {
                latest_valid_hash: None
            }),
            ExecutionStatus::Invalid
        );
        assert_eq!(
            ExecutionStatus::from_payload_status(&PayloadStatus::InvalidBlockHash),
            ExecutionStatus::Invalid
        );
    }
}
