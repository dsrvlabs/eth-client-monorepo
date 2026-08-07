//! `get_head`, proposer-boost delta application, head cache, reorg detection
//! (Architecture §6.1 / §6.3 / §6.4, CC-15c).
//!
//! # Head cache
//!
//! [`CachedHead`] is served while the store's monotonic `mutation_counter` is
//! unchanged. Weight application inside `get_head` does **not** bump the
//! counter — only external mutations (block, attestation, tick, boost, checkpoint)
//! do.
//!
//! # Proposer boost
//!
//! Boost is applied as a **delta** during the proto-array score pass
//! (`PROPOSER_SCORE_BOOST` percent of committee weight). It is reversed on the
//! next pass when `proposer_boost_root` is cleared by `on_tick`.

use cc_types::containers::Checkpoint;
use cc_types::preset::Preset;
use cc_types::primitives::{Root, Slot};
use thiserror::Error;

use crate::on_attestation::{OnAttestationError, compute_deltas};
use crate::proto_array::ProtoArrayError;
use crate::store::{CachedHead, Store};

/// Spec `PROPOSER_SCORE_BOOST` (percent of committee weight).
pub const PROPOSER_SCORE_BOOST: u64 = 40;

/// One reorg notification from [`get_head`] (Architecture §6.4 / CC-15/4).
///
/// Emitted at most **once** per `get_head` call that changes the head to a
/// non-descendant of the previous head. CC-18b/c wire this into the events
/// task; this crate only produces the local event value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainReorg {
    /// Distance (in hops) from the old head up to the common ancestor.
    pub depth: u64,
    /// Head root before this reorg.
    pub old_head: Root,
    /// Head root after this reorg.
    pub new_head: Root,
    /// Slot of the old head, if known.
    pub old_head_slot: Slot,
    /// Slot of the new head.
    pub new_head_slot: Slot,
}

/// Errors from [`get_head`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GetHeadError {
    /// Proto-array score / find-head failed.
    #[error(transparent)]
    ProtoArray(#[from] ProtoArrayError),
    /// Vote-delta computation failed.
    #[error(transparent)]
    Deltas(#[from] OnAttestationError),
    /// Justified checkpoint has no balances and no proto nodes to fall back on.
    #[error("justified checkpoint context missing balances for head computation")]
    MissingJustifiedBalances,
}

/// Spec `get_proposer_head(store, head_root, slot)` — optional reorg-to-parent
/// for a late, weak head (phase0 fork-choice helpers).
///
/// Returns the parent root when all reorg conditions hold; otherwise `head_root`.
pub fn get_proposer_head<P: Preset>(store: &mut Store<P>, head_root: Root, slot: Slot) -> Root {
    let Some(head_header) = store.blocks().get(&head_root).copied() else {
        return head_root;
    };
    let parent_root = head_header.parent_root;
    if parent_root == Root::ZERO || !store.blocks().contains_key(&parent_root) {
        return head_root;
    }
    let Some(parent_header) = store.blocks().get(&parent_root).copied() else {
        return head_root;
    };

    // 1. Head late (not timely).
    let head_late = !store.block_timeliness(&head_root).unwrap_or(true);
    // 2. Not proposing at an epoch boundary.
    let not_epoch_boundary = !slot.as_u64().is_multiple_of(P::SLOTS_PER_EPOCH);
    // 3. FFG competitive: same unrealized justified on head and parent.
    let ffg_competitive = store
        .proto_array()
        .get(&head_root)
        .zip(store.proto_array().get(&parent_root))
        .is_some_and(|(h, p)| {
            h.unrealized_justified_checkpoint == p.unrealized_justified_checkpoint
        });
    // 4. Finalization ok: epochs since finalization ≤ 2.
    let current_epoch = slot.as_u64() / P::SLOTS_PER_EPOCH.max(1);
    let epochs_since_final =
        current_epoch.saturating_sub(store.finalized_checkpoint().epoch.as_u64());
    let finalization_ok = epochs_since_final <= 2;
    // 5. Proposing on time (within ~1/6 of the slot).
    let sps = store.seconds_per_slot().max(1);
    let time_into_slot = store.time().saturating_sub(store.genesis_time()) % sps;
    // PROPOSER_REORG_CUTOFF_BPS = 1667 / 10000 ≈ 1/6 of slot.
    let proposing_on_time = time_into_slot * 10000 <= sps * 1667;
    // 6. Single-slot reorg geometry.
    let parent_slot_ok = parent_header.slot.as_u64().saturating_add(1) == head_header.slot.as_u64();
    let current_time_ok = head_header.slot.as_u64().saturating_add(1) == slot.as_u64();
    let single_slot_reorg = parent_slot_ok && current_time_ok;
    // 7. Boost must have worn off.
    let boost_off = store.proposer_boost_root() != head_root;
    // 8–9. Head weak / parent strong via proto-array weights (post get_head).
    // REORG_HEAD_WEIGHT_THRESHOLD = 20%, REORG_PARENT_WEIGHT_THRESHOLD = 160% of
    // committee weight.
    let committee = {
        let justified = store.justified_checkpoint();
        let total = store
            .checkpoint_context(justified)
            .map(|c| c.total_active_balance.as_u64())
            .unwrap_or(0);
        total / P::SLOTS_PER_EPOCH.max(1)
    };
    let head_threshold = (committee.saturating_mul(20)) / 100;
    let parent_threshold = (committee.saturating_mul(160)) / 100;
    let head_weight = store
        .proto_array()
        .get(&head_root)
        .map(|n| n.weight.max(0) as u64)
        .unwrap_or(0);
    let parent_weight = store
        .proto_array()
        .get(&parent_root)
        .map(|n| n.weight.max(0) as u64)
        .unwrap_or(0);
    let head_weak = head_weight < head_threshold;
    let parent_strong = parent_weight > parent_threshold;

    // Proposer equivocation branch (simplified): any other block same slot+proposer.
    let proposer_equivocation = store.blocks().iter().any(|(r, h)| {
        *r != head_root
            && h.slot == head_header.slot
            && h.proposer_index == head_header.proposer_index
    });

    if head_late
        && not_epoch_boundary
        && ffg_competitive
        && finalization_ok
        && proposing_on_time
        && single_slot_reorg
        && boost_off
        && head_weak
        && parent_strong
    {
        return parent_root;
    }
    if head_weak && current_time_ok && proposer_equivocation {
        return parent_root;
    }
    head_root
}

/// Spec `get_head(store)` with head-cache and reorg detection.
///
/// Returns the head root. When the head changes off the previous chain, a
/// single [`ChainReorg`] is also returned.
pub fn get_head<P: Preset>(
    store: &mut Store<P>,
) -> Result<(Root, Option<ChainReorg>), GetHeadError> {
    // --- Cache hit: no recomputation -----------------------------------------
    let mutation = store.mutation_counter();
    if let Some(cached) = store.head_cache()
        && cached.computed_at_mutation == mutation
    {
        return Ok((cached.head_root, None));
    }

    // Snapshot fields we need before taking exclusive mut borrows.
    let justified = store.justified_checkpoint();
    let finalized = store.finalized_checkpoint();
    let proposer_boost_root = store.proposer_boost_root();
    let current_epoch = store.get_current_store_epoch();
    let slots_per_epoch = P::SLOTS_PER_EPOCH;
    let previous_head = store.last_head_root();

    // Balances from the justified CheckpointContext (Architecture §6.6).
    let new_balances = justified_balances_snapshot(store, justified)?;
    let old_balances = store.justified_balances().to_vec();
    let indices = store.proto_array().indices().clone();
    let equiv = store.equivocating_indices().clone();

    // Vote promotions are applied only after full arithmetic success (SEC-16-4).
    let votes_snapshot = store.votes().to_vec();
    let deltas = match compute_deltas(
        &indices,
        store.votes_mut(),
        &old_balances,
        &new_balances,
        &equiv,
    ) {
        Ok(d) => d,
        Err(e) => {
            store.votes_mut().copy_from_slice(&votes_snapshot);
            return Err(GetHeadError::Deltas(e));
        }
    };

    let proposer_boost_score = compute_proposer_boost_score(store, justified, slots_per_epoch);

    let apply_result = store.proto_array_mut().apply_score_changes(
        deltas,
        justified,
        finalized,
        proposer_boost_root,
        proposer_boost_score,
        current_epoch,
        slots_per_epoch,
    );
    if let Err(e) = apply_result {
        store.votes_mut().copy_from_slice(&votes_snapshot);
        return Err(GetHeadError::ProtoArray(e));
    }

    store.set_justified_balances(new_balances);

    let head_root =
        store
            .proto_array()
            .find_head(justified.root, current_epoch, slots_per_epoch)?;

    let head_slot = store
        .proto_array()
        .get(&head_root)
        .map(|n| n.slot)
        .or_else(|| store.blocks().get(&head_root).map(|h| h.slot))
        .unwrap_or(Slot::new(0));

    // Reorg detection: exactly one event when head moves off the prior chain.
    let reorg = detect_reorg(store, previous_head, head_root, head_slot);

    store.set_last_head_root(head_root);
    store.set_head_cache(CachedHead {
        head_root,
        head_slot,
        justified,
        finalized,
        computed_at_mutation: mutation,
    });

    Ok((head_root, reorg))
}

/// Spec `compute_proposer_score` shape:
/// `(total_active_balance // SLOTS_PER_EPOCH) * PROPOSER_SCORE_BOOST // 100`.
pub fn compute_proposer_boost_score<P: Preset>(
    store: &mut Store<P>,
    justified: Checkpoint,
    slots_per_epoch: u64,
) -> i64 {
    if store.proposer_boost_root() == Root::ZERO {
        return 0;
    }
    let total = store
        .checkpoint_context(justified)
        .map(|ctx| ctx.total_active_balance.as_u64())
        .unwrap_or_else(|| store.justified_balances().iter().sum());
    let spe = slots_per_epoch.max(1);
    let committee_weight = total / spe;
    ((committee_weight.saturating_mul(PROPOSER_SCORE_BOOST)) / 100) as i64
}

fn justified_balances_snapshot<P: Preset>(
    store: &mut Store<P>,
    justified: Checkpoint,
) -> Result<Vec<u64>, GetHeadError> {
    if let Some(ctx) = store.checkpoint_context(justified) {
        let mut balances: Vec<u64> = ctx.effective_balances.iter().map(|g| g.as_u64()).collect();
        // Align length with vote capacity.
        let n = store.vote_capacity();
        if balances.len() < n {
            balances.resize(n, 0);
        } else if balances.len() > n {
            balances.truncate(n);
        }
        return Ok(balances);
    }
    // Fall back to the last applied snapshot (seeded at store construction).
    let mut balances = store.justified_balances().to_vec();
    if balances.is_empty() && store.proto_array().is_empty() {
        return Err(GetHeadError::MissingJustifiedBalances);
    }
    let n = store.vote_capacity();
    if balances.len() < n {
        balances.resize(n, 0);
    }
    Ok(balances)
}

fn detect_reorg<P: Preset>(
    store: &Store<P>,
    previous_head: Option<Root>,
    new_head: Root,
    new_head_slot: Slot,
) -> Option<ChainReorg> {
    let old_head = previous_head?;
    if old_head == new_head {
        return None;
    }
    // Not a reorg if new head is a descendant of old (extension) or old is unknown.
    if is_ancestor(store, old_head, new_head) {
        return None;
    }
    // Depth: hops from old_head up to the common ancestor with new_head.
    let (depth, old_slot) = reorg_depth(store, old_head, new_head);
    Some(ChainReorg {
        depth,
        old_head,
        new_head,
        old_head_slot: old_slot,
        new_head_slot,
    })
}

/// Whether `ancestor` is on the parent chain of `descendant` (or equal).
fn is_ancestor<P: Preset>(store: &Store<P>, ancestor: Root, descendant: Root) -> bool {
    if ancestor == descendant {
        return true;
    }
    let Some(anc_slot) = store
        .proto_array()
        .get(&ancestor)
        .map(|n| n.slot)
        .or_else(|| store.blocks().get(&ancestor).map(|h| h.slot))
    else {
        return false;
    };
    match store.proto_array().get_ancestor(descendant, anc_slot) {
        Ok(a) => a == ancestor,
        Err(_) => {
            // Header walk fallback.
            let mut current = descendant;
            for _ in 0..store.blocks().len().saturating_add(1) {
                if current == ancestor {
                    return true;
                }
                let Some(h) = store.blocks().get(&current) else {
                    return false;
                };
                if h.slot.as_u64() <= anc_slot.as_u64() {
                    return current == ancestor;
                }
                current = h.parent_root;
            }
            false
        }
    }
}

/// Hops from `old_head` to the common ancestor with `new_head`.
fn reorg_depth<P: Preset>(store: &Store<P>, old_head: Root, new_head: Root) -> (u64, Slot) {
    let old_slot = store
        .proto_array()
        .get(&old_head)
        .map(|n| n.slot)
        .or_else(|| store.blocks().get(&old_head).map(|h| h.slot))
        .unwrap_or(Slot::new(0));

    // Collect ancestors of new_head (root → slot).
    let mut new_chain: std::collections::HashMap<Root, Slot> = std::collections::HashMap::new();
    let mut cur = new_head;
    for _ in 0..store
        .proto_array()
        .len()
        .saturating_add(store.blocks().len())
        .saturating_add(1)
    {
        let slot = store
            .proto_array()
            .get(&cur)
            .map(|n| n.slot)
            .or_else(|| store.blocks().get(&cur).map(|h| h.slot))
            .unwrap_or(Slot::new(0));
        new_chain.insert(cur, slot);
        let parent = parent_root_of(store, cur);
        match parent {
            Some(p) if p != cur && p != Root::ZERO => cur = p,
            _ => break,
        }
    }

    let mut depth = 0_u64;
    let mut cur = old_head;
    for _ in 0..store
        .proto_array()
        .len()
        .saturating_add(store.blocks().len())
        .saturating_add(1)
    {
        if new_chain.contains_key(&cur) {
            return (depth, old_slot);
        }
        depth = depth.saturating_add(1);
        let parent = parent_root_of(store, cur);
        match parent {
            Some(p) if p != cur && p != Root::ZERO => cur = p,
            _ => break,
        }
    }
    (depth, old_slot)
}

/// Parent root of `root` via proto-array (bounds-checked) or header map.
///
/// SEC-15c-4: never index `nodes[parent]` unchecked — a stale parent index
/// after prune would panic; treat as terminal instead.
fn parent_root_of<P: Preset>(store: &Store<P>, root: Root) -> Option<Root> {
    if let Some(node) = store.proto_array().get(&root) {
        if let Some(p_idx) = node.parent {
            return store.proto_array().nodes().get(p_idx).map(|p| p.root);
        }
        return None;
    }
    store.blocks().get(&root).map(|h| h.parent_root)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::sync::Arc;

    use cc_state_transition::StubOptimisticEngine;
    use cc_types::containers::{BeaconBlockHeader, Checkpoint};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, Gwei, Hash256, Root, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState};
    use tree_hash::TreeHash;

    use super::*;
    use crate::da_seam::HarnessAvailability;
    use crate::execution_status::ExecutionStatus;
    use crate::on_attestation::on_attestation;
    use crate::on_block::get_forkchoice_store;
    use crate::on_tick::on_tick;
    use crate::proto_array::ProtoNodeBlock;
    use crate::store::{Store, VoteTracker};
    use cc_types::containers::AttestationData;
    use cc_types::operations::IndexedAttestation;
    use ssz_types::VariableList;

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

    fn seeded_store(n_validators: usize) -> (Store<Minimal>, Root) {
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
            Arc::new(StubOptimisticEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        store.resize_votes(n_validators);
        store.set_justified_balances(vec![32_000_000_000u64; n_validators]);
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        // Seed justified context total for boost math.
        if let Some(ctx) = store.checkpoint_context(store.justified_checkpoint()) {
            let mut owned = (*ctx).clone();
            owned.total_active_balance =
                Gwei::new(32_000_000_000u64.saturating_mul(n_validators as u64));
            owned.effective_balances = vec![Gwei::new(32_000_000_000); n_validators];
            store.insert_checkpoint_context(store.justified_checkpoint(), Arc::new(owned));
        }
        (store, anchor)
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

    /// CC-15/3: equal-weight late block does not reorg a timely boosted block;
    /// boost is gone the next slot.
    #[test]
    fn proposer_boost_protects_timely_block_and_clears_next_slot() {
        let (mut store, anchor) = seeded_store(2);
        // Slot 1 time window: seconds 6..12. Timely = before 6 + 2s (1/3 of 6).
        // Place store at t=6 (start of slot 1) so a slot-1 block is timely.
        store.set_time(6);
        let timely = root(0xAA);
        insert_child(&mut store, anchor, timely, 1);
        // Manual boost as on_block would set for a timely first block.
        store.set_proposer_boost_root(timely);

        let (head1, _) = get_head(&mut store).unwrap();
        assert_eq!(head1, timely, "boosted block is head");

        // Equal-weight competing block at same parent, higher root for tie-break
        // but without boost — should not reorg while boost is live.
        let late = root(0xFF); // lexicographically higher than 0xAA...
        insert_child(&mut store, anchor, late, 1);
        // Late block does not get boost (already set / not first).
        assert_eq!(store.proposer_boost_root(), timely);

        // Give both blocks zero attestation weight; boost alone decides.
        let (head2, _) = get_head(&mut store).unwrap();
        assert_eq!(
            head2, timely,
            "late equal-weight block must not reorg boosted timely block"
        );

        // Next slot clears boost.
        on_tick(&mut store, 12).unwrap();
        assert_eq!(store.proposer_boost_root(), Root::ZERO);

        let (head3, _) = get_head(&mut store).unwrap();
        // Without boost, higher root wins the tie.
        assert_eq!(
            head3, late,
            "after boost clears, lexicographically higher root wins equal weight"
        );
        // And previous_proposer_boost must have been reversed (score 0 applied).
        assert_eq!(
            store.proto_array().previous_proposer_boost().root,
            Root::ZERO
        );
    }

    /// Head cache: repeated get_head with unchanged mutation_counter does not
    /// recompute (same computed_at_mutation; counter unchanged).
    #[test]
    fn head_cache_hit_and_recompute_after_mutations() {
        let (mut store, anchor) = seeded_store(2);
        store.set_time(6);

        let (h1, _) = get_head(&mut store).unwrap();
        assert_eq!(h1, anchor);
        let counter_after_first = store.mutation_counter();
        let cached = store.head_cache().copied().expect("cache filled");
        assert_eq!(cached.computed_at_mutation, counter_after_first);
        assert_eq!(cached.head_root, anchor);

        // Cache hit: mutation_counter unchanged, same head, cache entry retained.
        let (h2, reorg) = get_head(&mut store).unwrap();
        assert_eq!(h2, anchor);
        assert!(reorg.is_none());
        assert_eq!(store.mutation_counter(), counter_after_first);
        assert_eq!(
            store.head_cache().map(|c| c.computed_at_mutation),
            Some(counter_after_first),
            "cache hit must not rewrite via recompute"
        );

        // Five mutation kinds that invalidate the cache:
        // 1. block insertion
        let child = root(0x11);
        insert_child(&mut store, anchor, child, 1);
        assert!(store.head_cache().is_none());
        let _ = get_head(&mut store).unwrap();
        assert!(store.head_cache().is_some());

        // 2. on_attestation that changes a tracker
        store.set_time(12); // slot 2 so a slot-1 attestation is in the past
        let att = IndexedAttestation {
            attesting_indices: VariableList::new(vec![ValidatorIndex::new(0)]).unwrap(),
            data: AttestationData {
                slot: Slot::new(1),
                index: Default::default(),
                beacon_block_root: child,
                source: cp(0, anchor),
                target: cp(0, anchor),
            },
            signature: Default::default(),
        };
        on_attestation(&mut store, &att, true).unwrap();
        assert!(store.head_cache().is_none(), "attestation must invalidate");
        let _ = get_head(&mut store).unwrap();

        // 3. tick that crosses a slot
        let before = store.mutation_counter();
        on_tick(&mut store, 18).unwrap();
        assert!(store.mutation_counter() > before);
        assert!(store.head_cache().is_none(), "slot tick must invalidate");
        let _ = get_head(&mut store).unwrap();

        // 4. proposer-boost change
        store.set_proposer_boost_root(child);
        assert!(store.head_cache().is_none(), "boost change must invalidate");
        let _ = get_head(&mut store).unwrap();

        // 5. checkpoint update
        store.update_checkpoints(cp(1, child), cp(0, anchor));
        assert!(store.head_cache().is_none(), "checkpoint must invalidate");
    }

    /// CC-15/4 shape: reorg emits one ChainReorg with depth and roots.
    #[test]
    fn reorg_emits_single_chain_reorg_with_depth() {
        let (mut store, anchor) = seeded_store(4);
        store.set_time(12);
        let a = root(0xA1);
        let b = root(0xB1);
        insert_child(&mut store, anchor, a, 1);
        insert_child(&mut store, anchor, b, 1);

        // Vote everyone onto A.
        for v in store.votes_mut().iter_mut() {
            *v = VoteTracker {
                current_root: Root::ZERO,
                next_root: a,
                next_epoch: Epoch::new(0),
            };
        }
        store.bump_mutation_counter();
        let (head_a, reorg1) = get_head(&mut store).unwrap();
        assert_eq!(head_a, a);
        // First head from anchor → extension, not a reorg.
        assert!(reorg1.is_none() || reorg1.unwrap().old_head == anchor);

        // Flip all votes to B → reorg.
        for v in store.votes_mut().iter_mut() {
            v.next_root = b;
            v.next_epoch = Epoch::new(0);
        }
        store.bump_mutation_counter();
        let (head_b, reorg2) = get_head(&mut store).unwrap();
        assert_eq!(head_b, b);
        let reorg = reorg2.expect("must emit chain_reorg");
        assert_eq!(reorg.old_head, a);
        assert_eq!(reorg.new_head, b);
        assert_eq!(reorg.depth, 1, "siblings reorg at depth 1");
    }
}
