//! Ancestor validation (Architecture §4.6, §4.8, CC-34b).
//!
//! **Validation propagates *up*; invalidation propagates *down*** (delta 7).
//! This module owns the upward pass:
//!
//! - [`propagate_execution_payload_validation`] — on `NOT_VALIDATED → VALID`
//!   for block *B*, walk ancestors, transition each `Optimistic → Valid`, and
//!   **stop at the first already-`Valid` node**.
//! - [`ValidationError::ValidExecutionStatusBecameInvalid`] — the §4.8 hard
//!   error shared by two call paths with **different** mutation contracts
//!   (see the variant docs).
//!
//! One `VALID` clears the whole optimistic suffix in a single pass with **zero**
//! `newPayload` re-submissions — the pass only rewrites
//! [`ProtoNode::execution_status`](crate::proto_array::ProtoNode).

use cc_types::preset::Preset;
use cc_types::primitives::{Hash256, Root};
use thiserror::Error;

use crate::execution_status::ExecutionStatus;
use crate::proto_array::ProtoArrayError;
use crate::store::Store;

/// Errors from the upward validation pass and forbidden status transitions.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    /// EL consensus failure around a `Valid`/`Invalid` conflict (§4.8).
    ///
    /// Spec purposefully omits `VALID → INVALIDATED` — *"only possible with a
    /// faulty EE … requires manual intervention"* — so this is never a status
    /// we **record** as a deliberate transition. Mutation semantics depend on
    /// the call path:
    ///
    /// | Path | Store mutation |
    /// |---|---|
    /// | [`try_mark_execution_invalid`] on a currently-`Valid` node | **Unmutated** — no write |
    /// | [`propagate_execution_payload_validation`] hits an `Invalid` ancestor | **May have prior `Optimistic → Valid` writes** on nodes already walked (Lighthouse-equivalent partial apply). The Invalid node itself is not rewritten. |
    ///
    /// Do **not** assume full rollback from this variant alone — inspect the
    /// call site (audit F1/F5/F7). Import atomicity for the `on_block` + upward
    /// path is a follow-on (CC-35 / dedicated fix), not fixed here.
    #[error(
        "EL consensus failure: Valid execution status became Invalid \
         (block_root={block_root:?}, payload_block_hash={payload_block_hash:?})"
    )]
    ValidExecutionStatusBecameInvalid {
        /// Beacon block root at the conflict (the still-`Valid` node for
        /// `try_mark`, or the `Invalid` ancestor for the upward inverted path).
        block_root: Root,
        /// `body.execution_payload.block_hash` of that block.
        payload_block_hash: Hash256,
    },
    /// Target root is not present in the proto-array.
    #[error(transparent)]
    ProtoArray(#[from] ProtoArrayError),
}

/// Outcome of one upward validation pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PropagateValidationOutcome {
    /// Nodes examined by the walk (including the starting root and a terminal
    /// already-`Valid` / `Irrelevant` stop node, when one is hit).
    pub visited: usize,
    /// Nodes that transitioned `Optimistic → Valid`.
    pub transitioned: usize,
    /// Roots that transitioned (in walk order: tip → … → oldest Optimistic).
    pub transitioned_roots: Vec<Root>,
}

/// Propagate `VALID` upward from `root` through optimistic ancestors (§4.6).
///
/// ```text
/// loop:
///     match node.execution_status:
///         Valid       => break          # first already-Valid floor
///         Optimistic  => := Valid
///         Irrelevant  => break
///         Invalid     => Err(ValidExecutionStatusBecameInvalid)  # §4.8 inverted
///     idx := parent or break
/// ```
///
/// Call patterns:
/// 1. Tip is still `Optimistic` (async EL later reports `VALID`) → pass `tip`.
/// 2. Tip was just inserted as `Valid` (`on_block`) → pass the **parent** so
///    the walk clears the optimistic suffix above the new Valid tip.
///
/// Does **not** call the execution engine — zero `newPayload` re-submissions.
///
/// # Mutation contract
///
/// Any `Optimistic → Valid` write bumps [`Store::bump_mutation_counter`] (clears
/// head cache). On the Invalid-ancestor error path, prior Optimistic→Valid
/// writes **remain** (see [`ValidationError::ValidExecutionStatusBecameInvalid`]);
/// the counter is still bumped when any such write occurred.
pub fn propagate_execution_payload_validation<P: Preset>(
    store: &mut Store<P>,
    root: Root,
) -> Result<PropagateValidationOutcome, ValidationError> {
    let mut idx = store
        .proto_array()
        .index_of(&root)
        .ok_or(ProtoArrayError::UnknownRoot(root))?;

    let mut outcome = PropagateValidationOutcome::default();

    loop {
        outcome.visited = outcome.visited.saturating_add(1);

        // Snapshot fields needed for transitions / errors before any write.
        let (status, block_root, payload_hash, parent) = {
            let node = &store.proto_array().nodes()[idx];
            (
                node.execution_status,
                node.root,
                node.execution_block_hash,
                node.parent,
            )
        };

        match status {
            ExecutionStatus::Valid => {
                // Floor: everything above was cleared by an earlier pass.
                break;
            }
            ExecutionStatus::Irrelevant => {
                // Pre-merge ancestor — no further execution statuses to clear.
                break;
            }
            ExecutionStatus::Optimistic => {
                store.proto_array_mut().nodes_mut()[idx].execution_status =
                    ExecutionStatus::Valid;
                outcome.transitioned = outcome.transitioned.saturating_add(1);
                outcome.transitioned_roots.push(block_root);
            }
            ExecutionStatus::Invalid => {
                // Inverse of §4.8: EL said a descendant is Valid while an
                // ancestor was previously Invalid — EL consensus failure.
                // Earlier Optimistic→Valid writes (if any) already landed
                // (Lighthouse-equivalent partial apply) — store is NOT rolled
                // back. Only `try_mark_execution_invalid` on Valid guarantees
                // an unmutated store.
                if outcome.transitioned > 0 {
                    store.bump_mutation_counter();
                }
                log_valid_became_invalid(block_root, payload_hash);
                return Err(ValidationError::ValidExecutionStatusBecameInvalid {
                    block_root,
                    payload_block_hash: payload_hash,
                });
            }
        }

        match parent {
            Some(p) => idx = p,
            None => break,
        }
    }

    if outcome.transitioned > 0 {
        store.bump_mutation_counter();
    }

    Ok(outcome)
}

/// Attempt to mark `root` as [`ExecutionStatus::Invalid`].
///
/// - `Optimistic` → status becomes `Invalid` and `mutation_counter` bumps (CC-35).
/// - Already-`Invalid` → no-op success (no bump).
/// - `Valid` → **hard error**, store **unmutated** (§4.8 / CC-34 /8). This is
///   the only path for which "returned `ValidExecutionStatusBecameInvalid`"
///   implies byte-identical store state.
/// - `Irrelevant` → no-op success (pre-merge; invalidation walk stops on
///   Irrelevant separately in CC-35).
///
/// Intended choke-point for single-node `VALID → INVALID` refusal. Bulk
/// invalidation (CC-35) must not bypass this via
/// [`crate::remove_invalidated_subtree_weight`] without a Valid pre-scan
/// (audit F3) — that is a CC-35 design constraint, not fixed here.
pub fn try_mark_execution_invalid<P: Preset>(
    store: &mut Store<P>,
    root: Root,
) -> Result<(), ValidationError> {
    let idx = store
        .proto_array()
        .index_of(&root)
        .ok_or(ProtoArrayError::UnknownRoot(root))?;

    let (status, payload_hash) = {
        let node = &store.proto_array().nodes()[idx];
        (node.execution_status, node.execution_block_hash)
    };

    match status {
        ExecutionStatus::Valid => {
            log_valid_became_invalid(root, payload_hash);
            // Store unmutated — we never wrote.
            Err(ValidationError::ValidExecutionStatusBecameInvalid {
                block_root: root,
                payload_block_hash: payload_hash,
            })
        }
        ExecutionStatus::Optimistic => {
            store.proto_array_mut().nodes_mut()[idx].execution_status =
                ExecutionStatus::Invalid;
            store.bump_mutation_counter();
            Ok(())
        }
        ExecutionStatus::Invalid | ExecutionStatus::Irrelevant => Ok(()),
    }
}

fn log_valid_became_invalid(block_root: Root, payload_block_hash: Hash256) {
    // Operator-facing 3 a.m. line — names the failure class and both hashes.
    // Captured by the acceptance test's tracing assertion.
    tracing::error!(
        block_root = ?block_root,
        payload_block_hash = ?payload_block_hash,
        "EL consensus failure: Valid execution status became Invalid"
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

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

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use cc_state_transition::{ExecutionEngine, NewPayloadRequest, PayloadStatus,
    };
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Hash256, Root, Slot};
    use cc_types::{BeaconBlock, BeaconState};
    use super::*;
    use crate::da_seam::HarnessAvailability;
    use crate::execution_status::is_optimistic;
    use crate::on_block::get_forkchoice_store;
    use crate::proto_array::ProtoNodeBlock;
    use crate::store::Store;

    /// Counts `verify_and_notify_new_payload` calls — used to assert zero
    /// re-submissions across the upward pass.
    #[derive(Debug, Default)]
    struct CountingEngine {
        new_payload_calls: AtomicU64,
    }

    impl CountingEngine {
        fn calls(&self) -> u64 {
            self.new_payload_calls.load(Ordering::SeqCst)
        }
    }

    impl<P: Preset> ExecutionEngine<P> for CountingEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: NewPayloadRequest<'_, P>,
        ) -> Result<PayloadStatus, cc_state_transition::EngineError> {
            self.new_payload_calls.fetch_add(1, Ordering::SeqCst);
            Ok(PayloadStatus::Valid)
        }
    }

    fn root_of(i: u16) -> Root {
        let mut a = [0u8; 32];
        a[0] = (i & 0xff) as u8;
        a[1] = (i >> 8) as u8;
        Root::from_array(a)
    }

    fn hash_of(i: u16) -> Hash256 {
        let mut a = [0u8; 32];
        a[0] = (i & 0xff) as u8;
        a[1] = (i >> 8) as u8;
        a[31] = 0xee; // distinguish from Root layout if needed
        Hash256::from(a)
    }

    /// Seeded store with a Valid anchor at root 0 (the CC-34 /7 floor).
    fn seeded(engine: Arc<dyn ExecutionEngine<Minimal>>) -> (Store<Minimal>, Root) {
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
        let store = get_forkchoice_store(
            state,
            &anchor_block,
            engine,
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        let anchor = store.justified_checkpoint().root;
        (store, anchor)
    }

    /// CC-34 /4 — one VALID clears 64 optimistic roots in a single pass, with
    /// zero newPayload re-submissions (asserted on the request count).
    #[test]
    fn one_valid_clears_64_optimistic() {
        let engine = Arc::new(CountingEngine::default());
        let (mut store, anchor) = seeded(engine.clone());
        assert_eq!(
            store.proto_array().get(&anchor).unwrap().execution_status,
            ExecutionStatus::Valid,
            "anchor floor must be Valid"
        );

        // Build 64-deep Optimistic chain: anchor(Valid) ← 1 ← 2 ← … ← 64.
        // Nodes are numbered 1..=64; tip is 64.
        let mut parent_id: Option<u16> = None;
        // First child parents to the real anchor root, not root_of(0).
        for i in 1u16..=64 {
            let justified = store.justified_checkpoint();
            let finalized = store.finalized_checkpoint();
            let parent_root = if i == 1 {
                Some(anchor)
            } else {
                parent_id.map(root_of)
            };
            store
                .proto_array_mut()
                .on_block(ProtoNodeBlock {
                    slot: Slot::new(i as u64),
                    root: root_of(i),
                    parent_root,
                    state_root: root_of(i.wrapping_add(1000)),
                    target_root: root_of(i),
                    justified_checkpoint: justified,
                    finalized_checkpoint: finalized,
                    unrealized_justified_checkpoint: justified,
                    unrealized_finalized_checkpoint: finalized,
                    execution_status: ExecutionStatus::Optimistic,
                    execution_block_hash: hash_of(i),
                })
                .unwrap();
            parent_id = Some(i);
            assert_eq!(
                is_optimistic(&store, root_of(i)),
                Some(true),
                "node {i} must start Optimistic"
            );
        }

        let before_calls = engine.calls();
        assert_eq!(before_calls, 0, "fixture must not submit payloads");

        // One VALID for the tip — single ancestor pass, no engine traffic.
        let outcome =
            propagate_execution_payload_validation(&mut store, root_of(64)).unwrap();

        assert_eq!(
            outcome.transitioned, 64,
            "all 64 Optimistic nodes must leave in one pass"
        );
        assert_eq!(outcome.transitioned_roots.len(), 64);
        // Walk examines 64 Optimistic + the Valid anchor floor.
        assert_eq!(
            outcome.visited, 65,
            "64 Optimistic + 1 Valid-anchor stop"
        );

        for i in 1u16..=64 {
            assert_eq!(
                store
                    .proto_array()
                    .get(&root_of(i))
                    .unwrap()
                    .execution_status,
                ExecutionStatus::Valid,
                "node {i} must be Valid after the pass"
            );
            assert_eq!(is_optimistic(&store, root_of(i)), Some(false));
        }
        // Anchor still Valid (floor, not re-transitioned).
        assert_eq!(
            store.proto_array().get(&anchor).unwrap().execution_status,
            ExecutionStatus::Valid
        );

        // Zero re-submissions — assert on the request count, not a log.
        assert_eq!(
            engine.calls(),
            before_calls,
            "upward pass must not call newPayload / verify_and_notify_new_payload"
        );
    }

    /// The walk stops at the first already-Valid node: nodes below the floor
    /// are not visited (asserted on the visit counter / transitioned set).
    #[test]
    fn upward_pass_stops_at_valid() {
        let (mut store, anchor) = seeded(Arc::new(AcceptEngine));

        // Chain: anchor(Valid) ← 1..29 Optimistic ← 30 Valid ← 31..64 Optimistic.
        for i in 1u16..=64 {
            let parent_root = if i == 1 {
                Some(anchor)
            } else {
                Some(root_of(i - 1))
            };
            let status = if i == 30 {
                ExecutionStatus::Valid
            } else {
                ExecutionStatus::Optimistic
            };
            let justified = store.justified_checkpoint();
            let finalized = store.finalized_checkpoint();
            store
                .proto_array_mut()
                .on_block(ProtoNodeBlock {
                    slot: Slot::new(i as u64),
                    root: root_of(i),
                    parent_root,
                    state_root: root_of(i.wrapping_add(1000)),
                    target_root: root_of(i),
                    justified_checkpoint: justified,
                    finalized_checkpoint: finalized,
                    unrealized_justified_checkpoint: justified,
                    unrealized_finalized_checkpoint: finalized,
                    execution_status: status,
                    execution_block_hash: hash_of(i),
                })
                .unwrap();
        }

        let outcome =
            propagate_execution_payload_validation(&mut store, root_of(64)).unwrap();

        // Nodes 31..=64 (34 nodes) transition; node 30 is the Valid stop.
        assert_eq!(outcome.transitioned, 34, "nodes 31..=64 only");
        let transitioned: std::collections::HashSet<_> =
            outcome.transitioned_roots.iter().copied().collect();
        for i in 31u16..=64 {
            assert!(
                transitioned.contains(&root_of(i)),
                "node {i} must transition"
            );
            assert_eq!(
                store
                    .proto_array()
                    .get(&root_of(i))
                    .unwrap()
                    .execution_status,
                ExecutionStatus::Valid
            );
        }
        for i in 1u16..=29 {
            assert!(
                !transitioned.contains(&root_of(i)),
                "node {i} must not transition"
            );
            assert_eq!(
                store
                    .proto_array()
                    .get(&root_of(i))
                    .unwrap()
                    .execution_status,
                ExecutionStatus::Optimistic,
                "node {i} must remain Optimistic (not visited)"
            );
        }
        // Visit count = 34 Optimistic + 1 Valid stop (node 30). Nodes 1–29
        // and the anchor are not examined.
        assert_eq!(
            outcome.visited, 35,
            "visit counter is the traversal property: 31..=64 + stop@30"
        );
        assert_eq!(
            store
                .proto_array()
                .get(&root_of(30))
                .unwrap()
                .execution_status,
            ExecutionStatus::Valid
        );
    }

    /// CC-34 /8 — VALID → INVALID is a typed hard error; store unmutated;
    /// error! names it an EL consensus failure with both hashes.
    #[test]
    fn valid_became_invalid_is_hard_error() {
        let (mut store, anchor) = seeded(Arc::new(AcceptEngine));

        // Valid child of the anchor.
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        let child = root_of(7);
        let child_hash = hash_of(7);
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: child,
                parent_root: Some(anchor),
                state_root: root_of(1007),
                target_root: child,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: child_hash,
            })
            .unwrap();

        // Snapshot store.blocks + every execution_status before the attempt.
        let mut blocks_before: Vec<_> = store
            .blocks()
            .iter()
            .map(|(r, h)| (*r, *h))
            .collect();
        blocks_before.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));
        let statuses_before: Vec<_> = store
            .proto_array()
            .nodes()
            .iter()
            .map(|n| (n.root, n.execution_status, n.execution_block_hash))
            .collect();

        // Capture tracing with a scoped dispatcher so parallel tests cannot
        // steal the default subscriber (set_default is process-global).
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let make_writer = {
            let buf = Arc::clone(&buf);
            move || TestWriter(Arc::clone(&buf))
        };
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_writer(make_writer)
            .with_ansi(false)
            .with_level(true)
            .finish();

        let err = tracing::subscriber::with_default(subscriber, || {
            try_mark_execution_invalid(&mut store, child).unwrap_err()
        });
        assert!(
            matches!(
                err,
                ValidationError::ValidExecutionStatusBecameInvalid {
                    block_root,
                    payload_block_hash,
                } if block_root == child && payload_block_hash == child_hash
            ),
            "expected ValidExecutionStatusBecameInvalid with both hashes, got {err:?}"
        );

        // Store unmutated — blocks and every ProtoNode.execution_status
        // byte-identical to before.
        let mut blocks_after: Vec<_> = store
            .blocks()
            .iter()
            .map(|(r, h)| (*r, *h))
            .collect();
        blocks_after.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));
        assert_eq!(blocks_before, blocks_after, "store.blocks must be unmutated");
        let statuses_after: Vec<_> = store
            .proto_array()
            .nodes()
            .iter()
            .map(|n| (n.root, n.execution_status, n.execution_block_hash))
            .collect();
        assert_eq!(
            statuses_before, statuses_after,
            "every ProtoNode.execution_status must be unmutated"
        );
        assert_eq!(
            store.proto_array().get(&child).unwrap().execution_status,
            ExecutionStatus::Valid
        );

        // error! names it an EL consensus failure and carries both hashes.
        let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logged.contains("EL consensus failure"),
            "error! must name EL consensus failure; got:\n{logged}"
        );
        assert!(
            logged.contains("Valid execution status became Invalid")
                || logged.contains("became Invalid"),
            "error! must describe the Valid→Invalid transition; got:\n{logged}"
        );
    }

    /// CC-34 /7 — checkpoint-sync / genesis anchor is Valid without any
    /// payload submission (the floor for CC-35b's invalidation walk).
    #[test]
    fn checkpoint_anchor_is_valid() {
        let engine = Arc::new(CountingEngine::default());
        let (store, anchor) = seeded(engine.clone());
        let node = store.proto_array().get(&anchor).expect("anchor node");
        assert_eq!(
            node.execution_status,
            ExecutionStatus::Valid,
            "anchor must be Valid (spec MAY), not Optimistic"
        );
        assert_eq!(
            is_optimistic(&store, anchor),
            Some(false),
            "is_optimistic(anchor) must be false"
        );
        assert_eq!(
            engine.calls(),
            0,
            "anchor Valid without any payload submission"
        );
    }

    /// Inverse §4.8 path: upward walk that hits an Invalid ancestor errors.
    /// Prior Optimistic→Valid writes remain (not an unmutated-store guarantee).
    #[test]
    fn upward_pass_errors_on_invalid_ancestor() {
        let (mut store, anchor) = seeded(Arc::new(AcceptEngine));
        // anchor(Valid) ← Invalid ← Optimistic tip
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: root_of(1),
                parent_root: Some(anchor),
                state_root: root_of(1001),
                target_root: root_of(1),
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Invalid,
                execution_block_hash: hash_of(1),
            })
            .unwrap();
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(2),
                root: root_of(2),
                parent_root: Some(root_of(1)),
                state_root: root_of(1002),
                target_root: root_of(2),
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Optimistic,
                execution_block_hash: hash_of(2),
            })
            .unwrap();

        let before = store.mutation_counter();
        let err = propagate_execution_payload_validation(&mut store, root_of(2)).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::ValidExecutionStatusBecameInvalid {
                    block_root,
                    ..
                } if block_root == root_of(1)
            ),
            "got {err:?}"
        );
        // Tip was Optimistic and was transitioned before the Invalid stop —
        // Lighthouse-equivalent partial apply on this catastrophic path.
        assert_eq!(
            store
                .proto_array()
                .get(&root_of(2))
                .unwrap()
                .execution_status,
            ExecutionStatus::Valid
        );
        // Invalid ancestor unmutated as Invalid.
        assert_eq!(
            store
                .proto_array()
                .get(&root_of(1))
                .unwrap()
                .execution_status,
            ExecutionStatus::Invalid
        );
        // Status write still invalidates head cache.
        assert!(store.mutation_counter() > before);
    }

    /// F2: status writes bump mutation_counter (clears head cache).
    #[test]
    fn status_writes_bump_mutation_counter() {
        let (mut store, anchor) = seeded(Arc::new(AcceptEngine));
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: root_of(1),
                parent_root: Some(anchor),
                state_root: root_of(1001),
                target_root: root_of(1),
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Optimistic,
                execution_block_hash: hash_of(1),
            })
            .unwrap();

        let before = store.mutation_counter();
        let outcome =
            propagate_execution_payload_validation(&mut store, root_of(1)).unwrap();
        assert_eq!(outcome.transitioned, 1);
        assert!(
            store.mutation_counter() > before,
            "Optimistic→Valid must bump mutation_counter"
        );

        // Second pass is a no-op (already Valid floor) — no further bump.
        let mid = store.mutation_counter();
        let again =
            propagate_execution_payload_validation(&mut store, root_of(1)).unwrap();
        assert_eq!(again.transitioned, 0);
        assert_eq!(store.mutation_counter(), mid);

        // try_mark Optimistic→Invalid bumps; Valid→Invalid does not.
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(2),
                root: root_of(2),
                parent_root: Some(root_of(1)),
                state_root: root_of(1002),
                target_root: root_of(2),
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Optimistic,
                execution_block_hash: hash_of(2),
            })
            .unwrap();
        let before_inv = store.mutation_counter();
        try_mark_execution_invalid(&mut store, root_of(2)).unwrap();
        assert!(store.mutation_counter() > before_inv);
        assert_eq!(
            store
                .proto_array()
                .get(&root_of(2))
                .unwrap()
                .execution_status,
            ExecutionStatus::Invalid
        );

        let before_hard = store.mutation_counter();
        let _ = try_mark_execution_invalid(&mut store, root_of(1)).unwrap_err();
        assert_eq!(
            store.mutation_counter(),
            before_hard,
            "Valid→Invalid hard error must not bump (no write)"
        );
    }

    /// Writer that appends to a shared buffer for tracing capture.
    struct TestWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
