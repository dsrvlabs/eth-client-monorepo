//! Descendant invalidation and the three `latestValidHash` cases (Architecture §4.7, CC-35a).
//!
//! **Step 1** — identify `invalidBlock` from `latestValidHash` (three cases; absent ≠ all-zeros).
//! **Step 2** — mark `invalidBlock` and all *descendants* `Invalid`, apply §4.4 weight removal.
//! Sibling branches of `invalidBlock` are never touched.
//!
//! The backwards walk, stop conditions, and justified-checkpoint exit are **CC-35b**
//! ([`crate::invalidation_walk`]). Weight arithmetic lives in
//! [`crate::execution_status::remove_invalidated_subtree_weight`] (CC-34a / ADR P3-11).
//!
//! # Lookup index
//!
//! Case 1 starts as a **linear scan** over the finalization-pruned proto-array (§12/9).
//! No auxiliary execution-hash → index map is introduced here. **CC-3C**'s numbers would
//! justify an index, and any such index must be a pure **derivation** of
//! [`ProtoNode::execution_block_hash`](crate::proto_array::ProtoNode::execution_block_hash),
//! never a second source of truth.

use std::sync::atomic::{AtomicU64, Ordering};

use cc_types::primitives::{Hash256, Root};
use thiserror::Error;

use crate::execution_status::{ExecutionStatus, remove_invalidated_subtree_weight};
use crate::proto_array::{ProtoArray, ProtoArrayError};

/// Process-local source for `cc_chain_invalidated_nodes_total` (declared at CC-3Aa).
///
/// Populated here on each successful subtree invalidation (by subtree size). The
/// chain service bridges this into the registered Prometheus counter when the
/// import path wires invalidation.
static INVALIDATED_NODES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Monotonic count of nodes transitioned to `Invalid` process-wide.
#[inline]
pub fn chain_invalidated_nodes_total() -> u64 {
    INVALIDATED_NODES_TOTAL.load(Ordering::SeqCst)
}

/// Bump `cc_chain_invalidated_nodes_total` by `n` (CC-35b walk path).
#[inline]
pub fn bump_invalidated_nodes(n: usize) {
    if n > 0 {
        INVALIDATED_NODES_TOTAL.fetch_add(n as u64, Ordering::SeqCst);
    }
}

/// Three-way `latestValidHash` (Architecture §4.7).
///
/// **Absent and all-zeros are different answers** — never collapse them into a bare
/// `Option` whose `Some(ZERO)` is treated like case 1, or a bare `bytes` whose
/// empty default is indistinguishable from case 2's sentinel on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatestValidHash {
    /// JSON `null` / proto field absent → only the block with the payload in question.
    Null,
    /// `0x00…00` (32 zero bytes) → first non-`Irrelevant` ancestor on the chain.
    Zero,
    /// Meaningful execution block hash → child-of-match on the chain.
    Hash(Hash256),
}

impl LatestValidHash {
    /// Map `PayloadStatus::Invalid { latest_valid_hash: Option<Hash256> }`.
    ///
    /// `None` → [`Self::Null`]; `Some(ZERO)` → [`Self::Zero`]; otherwise [`Self::Hash`].
    #[inline]
    pub fn from_option(hash: Option<Hash256>) -> Self {
        match hash {
            None => Self::Null,
            Some(h) if h == Hash256::ZERO => Self::Zero,
            Some(h) => Self::Hash(h),
        }
    }

    /// Decode an explicitly-optional wire field (`optional bytes latest_valid_hash`).
    ///
    /// - Field **absent** → [`Self::Null`]
    /// - Field present, 32 zero bytes → [`Self::Zero`]
    /// - Field present, 32 non-zero bytes → [`Self::Hash`]
    /// - Field present, wrong length → error (never silently conflate)
    pub fn from_optional_bytes(field: Option<&[u8]>) -> Result<Self, InvalidationError> {
        match field {
            None => Ok(Self::Null),
            Some(bytes) if bytes.len() != 32 => Err(InvalidationError::BadLatestValidHashLen {
                got: bytes.len(),
            }),
            Some(bytes) if bytes.iter().all(|&b| b == 0) => Ok(Self::Zero),
            Some(bytes) => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(bytes);
                Ok(Self::Hash(Hash256::from(arr)))
            }
        }
    }
}

/// Spec-shaped invalidation op after `invalidBlock` selection (CC-35a).
///
/// All three `latestValidHash` cases reduce to [`InvalidateOne`](Self::InvalidateOne)
/// on the selected `invalidBlock`. Ancestor-walking multi-block invalidation is
/// CC-35b.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationOperation {
    /// Invalidate `block_root` and **all descendants**. No sibling of `block_root`.
    InvalidateOne {
        /// Subtree root (`invalidBlock`).
        block_root: Root,
    },
}

impl InvalidationOperation {
    /// The `invalidBlock` this operation targets.
    #[inline]
    pub const fn invalid_block(self) -> Root {
        match self {
            Self::InvalidateOne { block_root } => block_root,
        }
    }
}

/// Errors from invalidation selection / application.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidationError {
    /// `block_in_question` is not in the proto-array.
    #[error("unknown block in question: {0:?}")]
    UnknownBlock(Root),
    /// Wire `latest_valid_hash` was present but not 32 bytes.
    #[error("latest_valid_hash length {got} != 32")]
    BadLatestValidHashLen {
        /// Observed length.
        got: usize,
    },
    /// Proto-array mutation failed.
    #[error(transparent)]
    ProtoArray(#[from] ProtoArrayError),
}

/// Select `invalidBlock` from `latestValidHash` (Architecture §4.7 step 1).
///
/// | `latestValidHash` | `invalidBlock` |
/// |---|---|
/// | execution block hash | **child** of the node whose `execution_block_hash` matches, on the chain containing `block_in_question` |
/// | `0x00…00` | first non-`Irrelevant` ancestor (incl. self) on that chain |
/// | `null` | `block_in_question` only |
///
/// A meaningful hash with **no** matching node behaves exactly as `null`.
/// **This is the common case for a checkpoint-synced client**, not an edge case —
/// the EL often names a hash from before our anchor, which we have never seen.
/// Do not "fix" this path into a warning-only branch.
pub fn select_invalid_block(
    proto_array: &ProtoArray,
    block_in_question: Root,
    latest_valid_hash: LatestValidHash,
) -> Result<Root, InvalidationError> {
    // Ensure the payload block is known before any case branch.
    if proto_array.index_of(&block_in_question).is_none() {
        return Err(InvalidationError::UnknownBlock(block_in_question));
    }

    // Collapse `Hash(ZERO)` → case 2. `from_option` / `from_optional_bytes` already
    // map zeros to `Zero`; re-normalize here so a hand-built `Hash(ZERO)` cannot
    // take the case-1 scan (first ZERO hit is often the Valid+ZERO anchor).
    let latest_valid_hash = match latest_valid_hash {
        LatestValidHash::Hash(h) if h == Hash256::ZERO => LatestValidHash::Zero,
        other => other,
    };

    match latest_valid_hash {
        LatestValidHash::Null => Ok(block_in_question),
        LatestValidHash::Zero => Ok(first_non_irrelevant_ancestor(proto_array, block_in_question)),
        LatestValidHash::Hash(hash) => {
            // Linear scan keyed on ProtoNode.execution_block_hash (§12/9, ≠13/3).
            // No HashMap index — see module docs (CC-3C may justify a derivation later).
            let match_idx = find_by_execution_block_hash(proto_array, hash);
            match match_idx {
                None => {
                    // Hash-not-found SHOULD: behave exactly as null.
                    // Common for checkpoint-synced clients (anchor hides the match).
                    Ok(block_in_question)
                }
                Some(match_idx) => {
                    match child_of_match_on_chain(proto_array, block_in_question, match_idx) {
                        Some(child) => Ok(child),
                        // Match not an ancestor of block_in_question → same as null.
                        None => Ok(block_in_question),
                    }
                }
            }
        }
    }
}

/// Build an [`InvalidationOperation`] from `latestValidHash` (selection only).
pub fn invalidation_operation(
    proto_array: &ProtoArray,
    block_in_question: Root,
    latest_valid_hash: LatestValidHash,
) -> Result<InvalidationOperation, InvalidationError> {
    let block_root = select_invalid_block(proto_array, block_in_question, latest_valid_hash)?;
    Ok(InvalidationOperation::InvalidateOne { block_root })
}

/// Apply descendant invalidation (Architecture §4.7 step 2).
///
/// Marks `invalidBlock` and all descendants `Invalid`, zeroes their weights, and
/// subtracts the subtree total once from each strict ancestor (CC-34a).
/// Returns the subtree size (for `cc_chain_invalidated_nodes_total`).
///
/// **§4.8 `Valid → Invalid`:** this path currently force-marks status (including
/// prior `Valid` nodes) via `remove_invalidated_subtree_weight`. The hard-error /
/// unmutated-store transition is **not** enforced here — ownership is CC-34b
/// (`try_mark` / upward validation) and **CC-35b** (walk + justified exit). Call
/// sites must not treat a successful return as §4.8 compliance.
///
/// **Case-2 mass-invalidate / dual path (CC-35b review F2):** `Zero` can select
/// the array-root / first post-merge payload. This path has **no** stop-3 floor
/// and **no** justified exit. Production import/fcU must use
/// [`crate::propagate_execution_payload_invalidation`] (walk floors) then
/// chain's `handle_justified_checkpoint_invalidated` when justified is
/// `Invalid` — not this helper alone.
pub fn apply_invalidation(
    proto_array: &mut ProtoArray,
    op: &InvalidationOperation,
) -> Result<usize, InvalidationError> {
    let invalid_root = op.invalid_block();
    let count = subtree_size(proto_array, invalid_root)?;
    remove_invalidated_subtree_weight(proto_array, invalid_root)?;
    INVALIDATED_NODES_TOTAL.fetch_add(count as u64, Ordering::SeqCst);
    Ok(count)
}

/// Select + apply in one call. Returns `(invalidBlock, nodes_invalidated)`.
pub fn invalidate_from_latest_valid_hash(
    proto_array: &mut ProtoArray,
    block_in_question: Root,
    latest_valid_hash: LatestValidHash,
) -> Result<(Root, usize), InvalidationError> {
    let op = invalidation_operation(proto_array, block_in_question, latest_valid_hash)?;
    let invalid = op.invalid_block();
    let n = apply_invalidation(proto_array, &op)?;
    Ok((invalid, n))
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Linear scan: first node whose `execution_block_hash` equals `hash`.
fn find_by_execution_block_hash(proto_array: &ProtoArray, hash: Hash256) -> Option<usize> {
    proto_array
        .nodes()
        .iter()
        .position(|n| n.execution_block_hash == hash)
}

/// Walk parent links from `block_in_question` until the parent is `match_idx`.
/// That node is the **child** of the match on this chain.
fn child_of_match_on_chain(
    proto_array: &ProtoArray,
    block_in_question: Root,
    match_idx: usize,
) -> Option<Root> {
    let mut idx = proto_array.index_of(&block_in_question)?;
    let nodes = proto_array.nodes();
    loop {
        let node = &nodes[idx];
        match node.parent {
            Some(p) if p == match_idx => return Some(node.root),
            Some(p) => idx = p,
            None => return None,
        }
    }
}

/// Deepest (closest to array root) non-`Irrelevant` ancestor of `root`, or `root`
/// itself if it is non-`Irrelevant`. If the entire chain is `Irrelevant`, returns
/// `root` (the payload block in question).
fn first_non_irrelevant_ancestor(proto_array: &ProtoArray, root: Root) -> Root {
    let mut idx = match proto_array.index_of(&root) {
        Some(i) => i,
        None => return root,
    };
    let nodes = proto_array.nodes();
    let mut first = root;
    loop {
        let node = &nodes[idx];
        if node.execution_status != ExecutionStatus::Irrelevant {
            // Walking parent-ward: last non-Irrelevant seen is the oldest
            // (first from the array-root side).
            first = node.root;
        }
        match node.parent {
            Some(p) => idx = p,
            None => break,
        }
    }
    first
}

/// Count of `root` plus all of its descendants (parent always has lower index).
fn subtree_size(proto_array: &ProtoArray, root: Root) -> Result<usize, InvalidationError> {
    let invalid_idx = proto_array
        .index_of(&root)
        .ok_or(InvalidationError::UnknownBlock(root))?;
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
    Ok(count)
}

// ---------------------------------------------------------------------------
// Tests — table-driven: one test per case (CC-35 /1–/3)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::proto_array::{ProtoArray, ProtoNodeBlock};
    use cc_types::containers::Checkpoint;
    use cc_types::primitives::{Epoch, Slot};
    use std::sync::Mutex;

    /// Serialise tests that read/write the process-wide counter.
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

    /// Snapshot of (root, status, weight, execution_block_hash) for store compare.
    fn snapshot(pa: &ProtoArray) -> Vec<(Root, ExecutionStatus, i64, Hash256)> {
        pa.nodes()
            .iter()
            .map(|n| {
                (
                    n.root,
                    n.execution_status,
                    n.weight,
                    n.execution_block_hash,
                )
            })
            .collect()
    }

    /// Linear chain A(1) → B(2) → C(3) → D(4), all Valid with distinct exec hashes.
    fn linear_chain() -> ProtoArray {
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
        // Non-trivial weights so invalidation has something to zero.
        {
            let nodes = pa.nodes_mut();
            nodes[0].weight = 40;
            nodes[1].weight = 30;
            nodes[2].weight = 20;
            nodes[3].weight = 10;
        }
        pa
    }

    /// CC-35a: absent vs all-zeros are distinguishable and select different invalidBlocks.
    #[test]
    fn latest_valid_hash_absent_vs_zero() {
        // --- wire field: explicit optional (PayloadStatusV1) -------------------
        // Field absent → Null; field set to 32 zero bytes → Zero.
        let absent = LatestValidHash::from_optional_bytes(None).unwrap();
        let zero = LatestValidHash::from_optional_bytes(Some(&[0u8; 32])).unwrap();
        assert_eq!(absent, LatestValidHash::Null);
        assert_eq!(zero, LatestValidHash::Zero);
        assert_ne!(absent, zero);

        // Decode via generated proto message (prost maps optional bytes → Option<Vec<u8>>).
        let mut msg_absent = cc_proto::engine::PayloadStatusV1 {
            status: "INVALID".into(),
            latest_valid_hash: None,
            validation_error: None,
        };
        let mut msg_zero = cc_proto::engine::PayloadStatusV1 {
            status: "INVALID".into(),
            latest_valid_hash: Some(vec![0u8; 32]),
            validation_error: None,
        };
        // Round-trip encode/decode so we exercise the wire representation.
        {
            use prost::Message;
            let bytes = msg_absent.encode_to_vec();
            msg_absent = cc_proto::engine::PayloadStatusV1::decode(bytes.as_slice()).unwrap();
            let bytes = msg_zero.encode_to_vec();
            msg_zero = cc_proto::engine::PayloadStatusV1::decode(bytes.as_slice()).unwrap();
        }
        let from_absent =
            LatestValidHash::from_optional_bytes(msg_absent.latest_valid_hash.as_deref()).unwrap();
        let from_zero =
            LatestValidHash::from_optional_bytes(msg_zero.latest_valid_hash.as_deref()).unwrap();
        assert_eq!(from_absent, LatestValidHash::Null);
        assert_eq!(from_zero, LatestValidHash::Zero);

        // --- selection: different invalidBlocks --------------------------------
        // Chain: Irrelevant(1) → Irrelevant(2) → Valid(3) → Optimistic(4)
        // Null on block 4 → invalidBlock = 4
        // Zero on block 4 → first non-Irrelevant = 3
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
            ExecutionStatus::Irrelevant,
            Hash256::ZERO,
        );
        insert(&mut pa, 2, 3, Some(2), ExecutionStatus::Valid, hash(0x33));
        insert(
            &mut pa,
            3,
            4,
            Some(3),
            ExecutionStatus::Optimistic,
            hash(0x44),
        );

        let inv_null =
            select_invalid_block(&pa, root(4), LatestValidHash::Null).unwrap();
        let inv_zero =
            select_invalid_block(&pa, root(4), LatestValidHash::Zero).unwrap();
        assert_eq!(inv_null, root(4), "null → only the payload block");
        assert_eq!(
            inv_zero,
            root(3),
            "zero → first non-Irrelevant ancestor"
        );
        assert_ne!(
            inv_null, inv_zero,
            "absent and all-zeros must select different invalidBlocks"
        );
    }

    /// CC-35 /1 case 1 — hash selects the **child** of the match (not the match).
    #[test]
    fn lvh_hash_selects_child_of_match() {
        let _g = COUNTER_LOCK.lock().unwrap();
        let before = chain_invalidated_nodes_total();

        let mut pa = linear_chain();
        // LVH = execution hash of B (node 2, hash 0x22) → invalidBlock = C (child of B)
        let lvh = LatestValidHash::Hash(hash(0x22));
        // Lookup is keyed on ProtoNode.execution_block_hash (assert the field matches).
        assert_eq!(pa.get(&root(2)).unwrap().execution_block_hash, hash(0x22));
        assert_eq!(pa.get(&root(3)).unwrap().execution_block_hash, hash(0x33));

        let (invalid, n) =
            invalidate_from_latest_valid_hash(&mut pa, root(4), lvh).unwrap();
        assert_eq!(invalid, root(3), "invalidBlock is child of match (C), not B");
        // B (match) untouched.
        assert_eq!(pa.get(&root(2)).unwrap().execution_status, ExecutionStatus::Optimistic);
        assert!(!pa.get(&root(2)).unwrap().execution_status.is_invalidated());
        // C and D invalidated; A untouched.
        assert!(pa.get(&root(3)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        assert!(!pa.get(&root(1)).unwrap().execution_status.is_invalidated());

        // Subtree of C is {C, D} → 2.
        assert_eq!(n, 2);
        assert_eq!(chain_invalidated_nodes_total(), before + 2);
    }

    /// CC-35 /1 case 2 — all-zeros → first non-`Irrelevant` ancestor.
    #[test]
    fn lvh_zero_selects_first_non_irrelevant() {
        let _g = COUNTER_LOCK.lock().unwrap();
        let before = chain_invalidated_nodes_total();

        // Two Irrelevant (pre-merge) nodes at base, then first execution block.
        // I1(1) → I2(2) → V3(3) → O4(4) → O5(5)
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
            ExecutionStatus::Irrelevant,
            Hash256::ZERO,
        );
        insert(&mut pa, 2, 3, Some(2), ExecutionStatus::Valid, hash(0x33));
        insert(
            &mut pa,
            3,
            4,
            Some(3),
            ExecutionStatus::Optimistic,
            hash(0x44),
        );
        insert(
            &mut pa,
            4,
            5,
            Some(4),
            ExecutionStatus::Optimistic,
            hash(0x55),
        );
        {
            let nodes = pa.nodes_mut();
            for (i, w) in [50i64, 40, 30, 20, 10].into_iter().enumerate() {
                nodes[i].weight = w;
            }
        }

        let (invalid, n) = invalidate_from_latest_valid_hash(
            &mut pa,
            root(5),
            LatestValidHash::Zero,
        )
        .unwrap();
        assert_eq!(
            invalid,
            root(3),
            "first non-Irrelevant is V3, not an Irrelevant base"
        );
        // Subtree of V3: {3,4,5}
        assert_eq!(n, 3);
        assert!(pa.get(&root(3)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        assert!(pa.get(&root(5)).unwrap().execution_status.is_invalidated());
        // Irrelevant ancestors untouched (not in subtree of V3).
        assert_eq!(
            pa.get(&root(1)).unwrap().execution_status,
            ExecutionStatus::Irrelevant
        );
        assert_eq!(
            pa.get(&root(2)).unwrap().execution_status,
            ExecutionStatus::Irrelevant
        );
        assert_eq!(chain_invalidated_nodes_total(), before + 3);
    }

    /// CC-35 /1 case 3 — `null` → only the block with the payload in question.
    #[test]
    fn lvh_null_invalidates_one() {
        let _g = COUNTER_LOCK.lock().unwrap();
        let before = chain_invalidated_nodes_total();

        // Tree: A ─┬─ B ── D (payload in question)
        //          └─ C
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
            1,
            3,
            Some(1),
            ExecutionStatus::Optimistic,
            hash(0x33),
        );
        insert(
            &mut pa,
            2,
            4,
            Some(2),
            ExecutionStatus::Optimistic,
            hash(0x44),
        );
        {
            let nodes = pa.nodes_mut();
            nodes[0].weight = 40; // A
            nodes[1].weight = 20; // B
            nodes[2].weight = 15; // C
            nodes[3].weight = 10; // D
        }

        let parent_before = pa.get(&root(2)).unwrap().clone();
        let sibling_before = pa.get(&root(3)).unwrap().clone();

        let (invalid, n) =
            invalidate_from_latest_valid_hash(&mut pa, root(4), LatestValidHash::Null).unwrap();
        assert_eq!(invalid, root(4));
        assert_eq!(n, 1, "null invalidates only the one block");
        assert!(pa.get(&root(4)).unwrap().execution_status.is_invalidated());
        assert_eq!(pa.get(&root(4)).unwrap().weight, 0);

        // Parent and sibling untouched.
        assert_eq!(
            pa.get(&root(2)).unwrap().execution_status,
            parent_before.execution_status
        );
        assert_eq!(pa.get(&root(2)).unwrap().weight, parent_before.weight - 10); // ancestor weight removed
        assert_eq!(
            pa.get(&root(3)).unwrap().execution_status,
            sibling_before.execution_status
        );
        assert_eq!(pa.get(&root(3)).unwrap().weight, sibling_before.weight);
        assert_eq!(chain_invalidated_nodes_total(), before + 1);
    }

    /// CC-35 /2 — hash-not-found behaves **exactly** as null (common for checkpoint-sync).
    ///
    /// The EL may name a hash from before our anchor. Matching **no** node is the
    /// common case for a checkpoint-synced client — not an anomaly. Outcome must be
    /// byte-identical to the null case (store compare, not a description compare).
    #[test]
    fn lvh_not_found_behaves_as_null() {
        let _g = COUNTER_LOCK.lock().unwrap();

        let build = || {
            // Distinct status/weight already set in linear_chain.
            linear_chain()
        };

        let mut pa_null = build();
        let mut pa_missing = build();
        assert_eq!(
            snapshot(&pa_null),
            snapshot(&pa_missing),
            "precondition: identical stores"
        );

        let before = chain_invalidated_nodes_total();
        let (inv_null, n_null) = invalidate_from_latest_valid_hash(
            &mut pa_null,
            root(4),
            LatestValidHash::Null,
        )
        .unwrap();
        let (inv_miss, n_miss) = invalidate_from_latest_valid_hash(
            &mut pa_missing,
            root(4),
            // Meaningful hash matching **no** node in the array.
            LatestValidHash::Hash(hash(0xAB)),
        )
        .unwrap();

        assert_eq!(inv_null, inv_miss);
        assert_eq!(n_null, n_miss);
        // Byte-identical resulting stores (status + weight + exec hash per node).
        assert_eq!(
            snapshot(&pa_null),
            snapshot(&pa_missing),
            "hash-not-found MUST produce the same store as null"
        );
        assert_eq!(
            chain_invalidated_nodes_total(),
            before + n_null as u64 + n_miss as u64
        );
    }

    /// CC-35 /3 — descendants yes, siblings no.
    #[test]
    fn invalidation_spares_siblings() {
        let _g = COUNTER_LOCK.lock().unwrap();
        let before = chain_invalidated_nodes_total();

        //          A(1)
        //         /    \
        //       B(2)   S(6) ── T(7)     ← sibling of invalidBlock + its subtree
        //      /    \
        //    C(3)   E(5)                ← two descendant branches of B
        //     |
        //    D(4)
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
        insert(
            &mut pa,
            2,
            5,
            Some(2),
            ExecutionStatus::Optimistic,
            hash(0x55),
        );
        insert(
            &mut pa,
            1,
            6,
            Some(1),
            ExecutionStatus::Optimistic,
            hash(0x66),
        );
        insert(
            &mut pa,
            2,
            7,
            Some(6),
            ExecutionStatus::Optimistic,
            hash(0x77),
        );
        {
            let nodes = pa.nodes_mut();
            // indices: 0=A,1=B,2=C,3=D,4=E,5=S,6=T
            nodes[0].weight = 100;
            nodes[1].weight = 50; // B = invalidBlock (subtree total)
            nodes[2].weight = 20;
            nodes[3].weight = 10;
            nodes[4].weight = 15;
            nodes[5].weight = 30;
            nodes[6].weight = 12;
        }

        let sibling_before = snapshot(&pa)
            .into_iter()
            .filter(|(r, ..)| *r == root(6) || *r == root(7))
            .collect::<Vec<_>>();

        // Invalidate B (as if LVH pointed at A's exec hash → child B).
        let (invalid, n) = invalidate_from_latest_valid_hash(
            &mut pa,
            root(4),
            LatestValidHash::Hash(hash(0x11)),
        )
        .unwrap();
        assert_eq!(invalid, root(2), "child of A on the chain is B");
        // Subtree of B: {B, C, D, E} = 4
        assert_eq!(n, 4);
        for r in [2u8, 3, 4, 5] {
            assert!(
                pa.get(&root(r)).unwrap().execution_status.is_invalidated(),
                "descendant {r} must be INVALIDATED"
            );
            assert_eq!(pa.get(&root(r)).unwrap().weight, 0);
        }

        // Sibling S and its entire subtree T unchanged.
        let sibling_after = snapshot(&pa)
            .into_iter()
            .filter(|(r, ..)| *r == root(6) || *r == root(7))
            .collect::<Vec<_>>();
        assert_eq!(
            sibling_before, sibling_after,
            "sibling of invalidBlock and its subtree must be untouched"
        );
        assert!(!pa.get(&root(6)).unwrap().execution_status.is_invalidated());
        assert!(!pa.get(&root(7)).unwrap().execution_status.is_invalidated());
        // Parent A still Valid (weight reduced by subtree total).
        assert_eq!(pa.get(&root(1)).unwrap().execution_status, ExecutionStatus::Valid);
        assert_eq!(pa.get(&root(1)).unwrap().weight, 50); // 100 − 50
        assert_eq!(chain_invalidated_nodes_total(), before + 4);
    }

    #[test]
    fn from_option_maps_three_states() {
        assert_eq!(LatestValidHash::from_option(None), LatestValidHash::Null);
        assert_eq!(
            LatestValidHash::from_option(Some(Hash256::ZERO)),
            LatestValidHash::Zero
        );
        assert_eq!(
            LatestValidHash::from_option(Some(hash(0xAB))),
            LatestValidHash::Hash(hash(0xAB))
        );
    }

    /// Finding 3 remediation: hand-built `Hash(ZERO)` must select identically to `Zero`
    /// (store-compare), never case-1 child-of-first-ZERO-match.
    #[test]
    fn hash_zero_variant_matches_zero_case() {
        let _g = COUNTER_LOCK.lock().unwrap();

        // Irrelevant bases + Valid first payload + optimistic tip (same as case-2 fixture).
        let build = || {
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
                ExecutionStatus::Irrelevant,
                Hash256::ZERO,
            );
            insert(&mut pa, 2, 3, Some(2), ExecutionStatus::Valid, hash(0x33));
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
                for (i, w) in [40i64, 30, 20, 10].into_iter().enumerate() {
                    nodes[i].weight = w;
                }
            }
            pa
        };

        let mut pa_zero = build();
        let mut pa_hash_zero = build();
        assert_eq!(snapshot(&pa_zero), snapshot(&pa_hash_zero));

        let inv_zero = select_invalid_block(&pa_zero, root(4), LatestValidHash::Zero).unwrap();
        let inv_hash_zero =
            select_invalid_block(&pa_hash_zero, root(4), LatestValidHash::Hash(Hash256::ZERO))
                .unwrap();
        assert_eq!(inv_zero, inv_hash_zero);
        assert_eq!(
            inv_zero,
            root(3),
            "both paths select first non-Irrelevant, not child-of-ZERO-anchor"
        );

        let before = chain_invalidated_nodes_total();
        let (_, n_z) =
            invalidate_from_latest_valid_hash(&mut pa_zero, root(4), LatestValidHash::Zero)
                .unwrap();
        let (_, n_h) = invalidate_from_latest_valid_hash(
            &mut pa_hash_zero,
            root(4),
            LatestValidHash::Hash(Hash256::ZERO),
        )
        .unwrap();
        assert_eq!(n_z, n_h);
        assert_eq!(
            snapshot(&pa_zero),
            snapshot(&pa_hash_zero),
            "Hash(ZERO) apply must be store-identical to Zero"
        );
        assert_eq!(
            chain_invalidated_nodes_total(),
            before + n_z as u64 + n_h as u64
        );
    }

    #[test]
    fn unknown_block_errors() {
        let pa = linear_chain();
        let err = select_invalid_block(&pa, root(0xFF), LatestValidHash::Null).unwrap_err();
        assert!(matches!(err, InvalidationError::UnknownBlock(_)));
    }
}
