//! Spec `on_attestation`, `on_attester_slashing`, and `compute_deltas`
//! (Architecture §6.3, CC-16).
//!
//! # Batching contract
//!
//! ```text
//! on_attestation  →  O(1) per attestation: vote tracker only, no weight math
//! compute_deltas  →  O(V + N) once: called by get_head(), never by on_attestation
//! ```
//!
//! **Node weights are only correct immediately after `get_head` applies the
//! deltas from [`compute_deltas`].** Nothing else may read `node.weight` and
//! expect a meaningful number. Proposer boost is applied as an extra delta by
//! `get_head` (CC-15c) — this module leaves that hook undocumented in the
//! return vector only (caller adds boost for `proposer_boost_root`).
//!
//! # Callers must supply validated indexed attestations
//!
//! Store-level BLS / committee verification is **intentionally out of scope**
//! (Phase 1). The `fork_choice` vectors supply already-indexed attestations;
//! Phase 5 verifies in the `attestation` service before the live feed reaches
//! these handlers. Callers must only pass a validated [`IndexedAttestation`]
//! (indices resolved, signatures checked where required).
//!
//! # CheckpointContext (ADR-P1-08)
//!
//! Spec `store_target_checkpoint_state` stores a full `BeaconState`. Phase 1
//! stores a [`CheckpointContext`] instead — balances + committee shell for the
//! delta pass and future signature domain work.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use cc_state_transition::helpers::constants::GENESIS_EPOCH;
use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::helpers::predicates::is_slashable_attestation_data;
use cc_state_transition::{compute_epoch_at_slot, process_slots};
use cc_types::containers::Checkpoint;
use cc_types::operations::{AttesterSlashing, IndexedAttestation};
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, Slot, ValidatorIndex};
use thiserror::Error;

use crate::checkpoint_context::CheckpointContext;
use crate::on_block::get_checkpoint_block;
use crate::store::{Store, VoteTracker};

/// Errors from attestation / attester-slashing store handlers.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OnAttestationError {
    /// Target epoch is **after** the store's current epoch (non-block path).
    /// Defer / queue until the epoch arrives.
    #[error(
        "attestation target epoch {target} is in the future relative to store epoch {current} (defer)"
    )]
    FutureTargetEpoch { target: Epoch, current: Epoch },
    /// Target epoch is older than the previous store epoch (non-block path).
    /// Permanently invalid for free-floating attestations — do **not** requeue.
    #[error(
        "attestation target epoch {target} is older than previous store epoch (current {current})"
    )]
    PastTargetEpoch { target: Epoch, current: Epoch },
    /// Attestation slot is not yet in the past. Defer / queue.
    #[error(
        "attestation slot {slot} is not yet in the past (store slot {current}); defer consideration"
    )]
    FutureSlot { slot: Slot, current: Slot },
    /// Target epoch does not match the attestation slot's epoch.
    #[error("target epoch does not match attestation slot epoch")]
    TargetEpochMismatch { target: Epoch, slot_epoch: Epoch },
    /// Attestation target root is unknown — defer until the block is found.
    #[error("unknown attestation target root: {0:?}")]
    UnknownTarget(Root),
    /// Beacon block root is unknown — defer until the block is found.
    #[error("unknown attestation beacon block root: {0:?}")]
    UnknownBlock(Root),
    /// Beacon block slot is later than the attestation slot.
    #[error("attestation beacon block is later than attestation slot")]
    BlockInFutureOfAttestation,
    /// LMD vote is inconsistent with the FFG target checkpoint block.
    #[error("LMD vote inconsistent with FFG target")]
    InconsistentTarget,
    /// Missing post-state needed to build a [`CheckpointContext`].
    #[error("missing block state for checkpoint context at root {0:?}")]
    MissingBlockState(Root),
    /// Slot processing while building a checkpoint context failed.
    #[error("process_slots for checkpoint context: {0}")]
    ProcessSlots(String),
    /// Proto-array weight application failed (e.g. delta length mismatch).
    #[error("proto-array weight apply: {0}")]
    WeightApply(String),
    /// Attester-slashing attestation data is not slashable.
    #[error("attester slashing data is not slashable")]
    NotSlashable,
    /// Attesting index is outside the store's vote-tracker capacity.
    #[error("validator index {index} out of range (vote capacity {capacity})")]
    ValidatorIndexOutOfRange { index: u64, capacity: usize },
    /// Weight delta arithmetic overflowed for a node index.
    #[error("weight delta overflow at node index {0}")]
    DeltaOverflow(usize),
    /// Delta index was out of range for the indices map.
    #[error("invalid node delta index {0}")]
    InvalidNodeDelta(usize),
}

impl OnAttestationError {
    /// Whether this error is a **deferral** (attestation may become valid later).
    ///
    /// Spec note on `on_attestation`: an attestation asserted as invalid may be
    /// valid at a later time — schedule it for later processing.
    ///
    /// Deferrable: **future** target epoch, future slot, unknown block/target.
    /// **Not** deferrable: past target epoch outside `[previous, current]` —
    /// those never become valid later and must not be requeued (SEC-16-6).
    pub fn is_deferrable(&self) -> bool {
        matches!(
            self,
            Self::FutureTargetEpoch { .. }
                | Self::FutureSlot { .. }
                | Self::UnknownTarget(_)
                | Self::UnknownBlock(_)
        )
    }
}

// ---------------------------------------------------------------------------
// validate_on_attestation
// ---------------------------------------------------------------------------

/// Spec `validate_target_epoch_against_current_time`.
///
/// Attestations must be from the current or previous store epoch.
/// - **Future** target (`target > current`) → [`OnAttestationError::FutureTargetEpoch`]
///   (deferrable — queue until the epoch arrives).
/// - **Past** target older than previous → [`OnAttestationError::PastTargetEpoch`]
///   (not deferrable — never becomes valid later).
fn validate_target_epoch_against_current_time<P: Preset>(
    store: &Store<P>,
    target: Checkpoint,
) -> Result<(), OnAttestationError> {
    let current_epoch = store.get_current_store_epoch();
    let previous_epoch = if current_epoch.as_u64() > GENESIS_EPOCH.as_u64() {
        Epoch::new(current_epoch.as_u64() - 1)
    } else {
        GENESIS_EPOCH
    };
    if target.epoch == current_epoch || target.epoch == previous_epoch {
        return Ok(());
    }
    if target.epoch.as_u64() > current_epoch.as_u64() {
        return Err(OnAttestationError::FutureTargetEpoch {
            target: target.epoch,
            current: current_epoch,
        });
    }
    Err(OnAttestationError::PastTargetEpoch {
        target: target.epoch,
        current: current_epoch,
    })
}

/// Spec `validate_on_attestation(store, attestation, is_from_block)`.
///
/// Branch structure follows the consensus-spec asserts exactly. Failures that
/// the runner should re-queue surface as [`OnAttestationError::is_deferrable`].
///
/// `is_from_block = true` skips the current/previous-epoch scope check (block-
/// carried attestations may be older than the store's current epoch window).
pub fn validate_on_attestation<P: Preset>(
    store: &Store<P>,
    data_slot: Slot,
    data_beacon_block_root: Root,
    target: Checkpoint,
    is_from_block: bool,
) -> Result<(), OnAttestationError> {
    // If the given attestation is not from a beacon block message, check target
    // epoch scope against current time.
    if !is_from_block {
        validate_target_epoch_against_current_time::<P>(store, target)?;
    }

    // Epoch number and slot number must match.
    let slot_epoch = compute_epoch_at_slot::<P>(data_slot);
    if target.epoch != slot_epoch {
        return Err(OnAttestationError::TargetEpochMismatch {
            target: target.epoch,
            slot_epoch,
        });
    }

    // Target must be a known block.
    if !store.blocks().contains_key(&target.root) {
        return Err(OnAttestationError::UnknownTarget(target.root));
    }

    // Beacon block root must be known.
    if !store.blocks().contains_key(&data_beacon_block_root) {
        return Err(OnAttestationError::UnknownBlock(data_beacon_block_root));
    }

    // Attestations must not be for blocks in the future relative to their slot.
    let Some(header) = store.blocks().get(&data_beacon_block_root) else {
        return Err(OnAttestationError::UnknownBlock(data_beacon_block_root));
    };
    if header.slot.as_u64() > data_slot.as_u64() {
        return Err(OnAttestationError::BlockInFutureOfAttestation);
    }

    // LMD vote must be consistent with FFG vote target.
    let checkpoint_block = get_checkpoint_block(store, data_beacon_block_root, target.epoch);
    if target.root != checkpoint_block {
        return Err(OnAttestationError::InconsistentTarget);
    }

    // Attestations can only affect the fork choice of subsequent slots.
    // Delay consideration until their slot is in the past.
    let current_slot = store.get_current_slot();
    if current_slot.as_u64() < data_slot.as_u64().saturating_add(1) {
        return Err(OnAttestationError::FutureSlot {
            slot: data_slot,
            current: current_slot,
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// store_target_checkpoint_context (ADR-P1-08 replacement for
// store_target_checkpoint_state)
// ---------------------------------------------------------------------------

/// Spec `store_target_checkpoint_state` replacement: ensure a [`CheckpointContext`]
/// exists for `target` (Architecture §6.6 / ADR-P1-08).
///
/// Builds from the target root's post-state, advancing slots to the epoch start
/// when needed so balances match the checkpoint epoch.
pub fn store_target_checkpoint_context<P: Preset>(
    store: &mut Store<P>,
    target: Checkpoint,
) -> Result<(), OnAttestationError> {
    if store.checkpoint_context(target).is_some() {
        return Ok(());
    }

    let mut state = store
        .block_state(&target.root)
        .ok_or(OnAttestationError::MissingBlockState(target.root))?
        .clone();

    let epoch_start = compute_start_slot_at_epoch::<P>(target.epoch);
    if state.slot().as_u64() < epoch_start.as_u64() {
        process_slots(&mut state, epoch_start)
            .map_err(|e| OnAttestationError::ProcessSlots(e.to_string()))?;
    }

    let ctx = CheckpointContext::from_state(&state, target);
    store.insert_checkpoint_context(target, Arc::new(ctx));
    Ok(())
}

// ---------------------------------------------------------------------------
// update_latest_messages / vote trackers
// ---------------------------------------------------------------------------

/// Spec `update_latest_messages` via [`VoteTracker`] next_* writes (Architecture §6.3).
///
/// Only mutates `next_root` / `next_epoch` when the attestation is newer than
/// the tracked vote. Skips equivocating indices. Rejects indices outside the
/// store's fixed vote capacity (SEC-16-2). Returns whether any tracker changed.
fn update_latest_messages<P: Preset>(
    store: &mut Store<P>,
    attesting_indices: impl IntoIterator<Item = ValidatorIndex>,
    target_epoch: Epoch,
    beacon_block_root: Root,
) -> Result<bool, OnAttestationError> {
    let capacity = store.votes().len();
    let mut changed = false;
    for index in attesting_indices {
        if store.equivocating_indices().contains(&index) {
            continue;
        }
        let idx = index.as_u64() as usize;
        if idx >= capacity {
            return Err(OnAttestationError::ValidatorIndexOutOfRange {
                index: index.as_u64(),
                capacity,
            });
        }
        let vote = &mut store.votes_mut()[idx];
        // Spec: absent message OR target.epoch > existing.epoch.
        let should_update =
            vote.latest_message().is_none() || target_epoch.as_u64() > vote.next_epoch.as_u64();
        if should_update {
            vote.next_root = beacon_block_root;
            vote.next_epoch = target_epoch;
            changed = true;
        }
    }
    Ok(changed)
}

// ---------------------------------------------------------------------------
// on_attestation
// ---------------------------------------------------------------------------

/// Spec `on_attestation(store, attestation, is_from_block)`.
///
/// Accepts an already-**indexed** attestation (fork-choice vectors and Phase 5
/// both supply indices; store-level BLS verification is out of scope for CC-16).
///
/// **Does not touch node weights** — only vote trackers and checkpoint contexts.
/// Head weight is recomputed later by `get_head` → [`compute_deltas`].
pub fn on_attestation<P: Preset>(
    store: &mut Store<P>,
    attestation: &IndexedAttestation<P>,
    is_from_block: bool,
) -> Result<(), OnAttestationError> {
    let data = &attestation.data;
    validate_on_attestation(
        store,
        data.slot,
        data.beacon_block_root,
        data.target,
        is_from_block,
    )?;

    store_target_checkpoint_context(store, data.target)?;

    // Callers must pass a validated IndexedAttestation (see module docs).
    let changed = update_latest_messages(
        store,
        attestation.attesting_indices.iter().copied(),
        data.target.epoch,
        data.beacon_block_root,
    )?;

    if changed {
        // Architecture §6.4: any on_attestation that actually changes a tracker
        // bumps the mutation counter.
        store.bump_mutation_counter();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// on_attester_slashing
// ---------------------------------------------------------------------------

/// Spec `on_attester_slashing(store, attester_slashing)`.
///
/// Inserts the sorted-index intersection of the two indexed attestations into
/// `equivocating_indices`. A slashed validator's latest message **stops
/// contributing weight** on the next [`compute_deltas`] pass (skip + retract
/// `current_root`).
///
/// Signature verification of the slashings is not performed at the store level
/// in Phase 1 (same contract as `on_attestation`); the `is_slashable` data
/// predicate is enforced.
pub fn on_attester_slashing<P: Preset>(
    store: &mut Store<P>,
    attester_slashing: &AttesterSlashing<P>,
) -> Result<(), OnAttestationError> {
    let attestation_1 = &attester_slashing.attestation_1;
    let attestation_2 = &attester_slashing.attestation_2;

    if !is_slashable_attestation_data(&attestation_1.data, &attestation_2.data) {
        return Err(OnAttestationError::NotSlashable);
    }

    // Intersection of attesting indices (spec). Sorted by construction of
    // VariableList indices in valid slashings; we collect into a BTreeSet.
    let set2: BTreeSet<u64> = attestation_2
        .attesting_indices
        .iter()
        .map(|i| i.as_u64())
        .collect();
    let intersection: Vec<ValidatorIndex> = attestation_1
        .attesting_indices
        .iter()
        .filter(|i| set2.contains(&i.as_u64()))
        .copied()
        .collect();

    store.insert_equivocating_indices(intersection);
    Ok(())
}

// ---------------------------------------------------------------------------
// compute_deltas
// ---------------------------------------------------------------------------

/// Compute per-node weight deltas from vote trackers (Architecture §6.3).
///
/// # Contract
///
/// - **Called by `get_head` and nowhere else** outside tests.
/// - **`on_attestation` must never call this.**
/// - Returns a vector aligned with proto-array node indices: subtract each
///   validator's old balance from `current_root`, add the new balance to
///   `next_root`, then set `current_root = next_root`.
/// - Equivocating validators: retract old weight from `current_root` once and
///   zero `current_root` permanently (further `on_attestation` writes only
///   update `next_root`, which is then ignored here).
/// - **Proposer boost** is a delta applied by the caller (CC-15c) after this
///   returns — not a persistent field on the node.
///
/// # Balances
///
/// `old_balances` is the snapshot from the previous call (store
/// `justified_balances`); `new_balances` is the current justified
/// [`CheckpointContext`] effective balances. After a successful apply, the
/// caller should `store.set_justified_balances(new_balances)`.
/// Counter of successful [`compute_deltas`] completions (batching-contract instrumentation).
///
/// Used by CC-1E to assert a 128-attestation batch triggers `compute_deltas` at most once
/// (inside the single trailing `get_head`, never per `on_attestation`).
static COMPUTE_DELTAS_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Number of successful [`compute_deltas`] completions process-wide (monotonic).
#[inline]
pub fn compute_deltas_call_count() -> u64 {
    COMPUTE_DELTAS_CALLS.load(std::sync::atomic::Ordering::SeqCst)
}

/// Compute per-node weight deltas from vote trackers.
///
/// Vote mutations (`current_root` promotions / equivocation zeros) are applied
/// **only after** all arithmetic succeeds — a mid-loop error leaves trackers
/// unchanged (SEC-16-4).
pub fn compute_deltas(
    indices: &HashMap<Root, usize>,
    votes: &mut [VoteTracker],
    old_balances: &[u64],
    new_balances: &[u64],
    equivocating_indices: &BTreeSet<ValidatorIndex>,
) -> Result<Vec<i64>, OnAttestationError> {
    let mut deltas = vec![0i64; indices.len()];
    // Deferred tracker commits: (validator_index, new_current_root).
    let mut promotions: Vec<(usize, Root)> = Vec::new();

    for (val_index, vote) in votes.iter().enumerate() {
        // No score change if the validator has never voted.
        if vote.current_root == Root::ZERO && vote.next_root == Root::ZERO {
            continue;
        }

        let val = ValidatorIndex::new(val_index as u64);

        // Newly (or already) equivocating: retract current weight once, then
        // permanently zero current_root so we never re-apply the deduction.
        if equivocating_indices.contains(&val) {
            if vote.current_root != Root::ZERO {
                let old_balance = old_balances.get(val_index).copied().unwrap_or(0);
                if let Some(current_delta_index) = indices.get(&vote.current_root).copied() {
                    let slot = deltas
                        .get_mut(current_delta_index)
                        .ok_or(OnAttestationError::InvalidNodeDelta(current_delta_index))?;
                    *slot = slot
                        .checked_sub(old_balance as i64)
                        .ok_or(OnAttestationError::DeltaOverflow(current_delta_index))?;
                }
                promotions.push((val_index, Root::ZERO));
            }
            continue;
        }

        let old_balance = old_balances.get(val_index).copied().unwrap_or(0);
        let new_balance = new_balances.get(val_index).copied().unwrap_or(0);

        if vote.current_root != vote.next_root || old_balance != new_balance {
            // Retract from current (ignore unknown roots — pre-finalization).
            if vote.current_root != Root::ZERO
                && let Some(current_delta_index) = indices.get(&vote.current_root).copied()
            {
                let slot = deltas
                    .get_mut(current_delta_index)
                    .ok_or(OnAttestationError::InvalidNodeDelta(current_delta_index))?;
                *slot = slot
                    .checked_sub(old_balance as i64)
                    .ok_or(OnAttestationError::DeltaOverflow(current_delta_index))?;
            }

            // Apply to next.
            if vote.next_root != Root::ZERO
                && let Some(next_delta_index) = indices.get(&vote.next_root).copied()
            {
                let slot = deltas
                    .get_mut(next_delta_index)
                    .ok_or(OnAttestationError::InvalidNodeDelta(next_delta_index))?;
                *slot = slot
                    .checked_add(new_balance as i64)
                    .ok_or(OnAttestationError::DeltaOverflow(next_delta_index))?;
            }

            promotions.push((val_index, vote.next_root));
        }
    }

    // Commit tracker promotions only after full success.
    for (val_index, new_current) in promotions {
        votes[val_index].current_root = new_current;
    }

    COMPUTE_DELTAS_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    Ok(deltas)
}

/// Run [`compute_deltas`] against the store's votes / balances / equivocations
/// and apply the result to the proto-array.
///
/// **Intended call site: `get_head` (CC-15c).** Tests use this to assert the
/// batching contract without a full head pass. Updates `justified_balances` to
/// `new_balances` after a successful apply.
///
/// # Atomicity (SEC-16-4)
///
/// If weight application fails after vote promotions, trackers are restored
/// from a pre-`compute_deltas` snapshot so a failed apply does not leave
/// half-committed state.
///
/// Proposer-boost delta is **not** applied here — CC-15c adds it inside
/// `get_head` before/after this helper.
pub fn apply_attestation_deltas<P: Preset>(
    store: &mut Store<P>,
    new_balances: &[u64],
) -> Result<Vec<i64>, OnAttestationError> {
    let indices: HashMap<Root, usize> = store.proto_array().indices().clone();
    let old_balances = store.justified_balances().to_vec();
    let equiv = store.equivocating_indices().clone();
    // Snapshot votes so a failed weight apply can roll them back.
    let votes_snapshot = store.votes().to_vec();

    let deltas = compute_deltas(
        &indices,
        store.votes_mut(),
        &old_balances,
        new_balances,
        &equiv,
    )?;

    if let Err(e) = store.proto_array_mut().apply_weight_deltas(&deltas) {
        // Restore vote trackers — do not leave promotions committed without weights.
        let votes = store.votes_mut();
        debug_assert_eq!(votes.len(), votes_snapshot.len());
        votes.copy_from_slice(&votes_snapshot);
        return Err(OnAttestationError::WeightApply(e.to_string()));
    }

    store.set_justified_balances(new_balances.to_vec());
    // Weight application can move the head.
    store.bump_mutation_counter();
    Ok(deltas)
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

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

        use cc_types::containers::{AttestationData, BeaconBlockHeader, Checkpoint};
    use cc_types::fork::Fork;
    use cc_types::operations::{AttesterSlashing, IndexedAttestation};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, Gwei, Hash256, Root, Slot, ValidatorIndex};
    use ssz_types::VariableList;

    use super::*;
    use crate::da_seam::HarnessAvailability;
    use crate::execution_status::ExecutionStatus;
    use crate::on_block::get_forkchoice_store;
    use crate::proto_array::ProtoNodeBlock;
    use crate::store::{LatestMessage, Store, VoteTracker};
    use cc_types::BeaconBlock;
    use cc_types::BeaconState;

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

    fn indexed(
        indices: &[u64],
        slot: u64,
        beacon_block_root: Root,
        target: Checkpoint,
    ) -> IndexedAttestation<Minimal> {
        let attesting_indices: VariableList<
            ValidatorIndex,
            <Minimal as cc_types::preset::Preset>::MaxValidatorsPerSlot,
        > = VariableList::new(
            indices
                .iter()
                .map(|i| ValidatorIndex::new(*i))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        IndexedAttestation {
            attesting_indices,
            data: AttestationData {
                slot: Slot::new(slot),
                index: Default::default(),
                beacon_block_root,
                source: cp(0, beacon_block_root),
                target,
            },
            signature: Default::default(),
        }
    }

    /// Seeded store with anchor at slot 0 and time advanced so attestations for
    /// slot 0 are eligible (`current_slot >= slot + 1`).
    fn seeded_store(validator_count: usize) -> (Store<Minimal>, Root) {
        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        // Pad registry so CheckpointContext balances match validator_count.
        // Default state has zero validators; balances vectors are sized by votes.
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
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        // Resize votes to the requested capacity (get_forkchoice_store uses
        // validators_len() which is 0 on a default state). Controlled resize
        // only — attestation path rejects OOB indices (SEC-16-2).
        store.resize_votes(validator_count);
        store.set_justified_balances(vec![32_000_000_000u64; validator_count]);
        // Advance to slot 1 so slot-0 attestations pass the "slot in the past" check.
        store.set_time(6);
        let anchor = {
            // Recompute anchor root the same way get_forkchoice_store does.
            use tree_hash::TreeHash;
            Root::from_hash256(TreeHash::tree_hash_root(&anchor_block))
        };
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

    // --- validate / on_attestation ------------------------------------------

    #[test]
    fn valid_attestation_updates_latest_messages_and_bumps_counter() {
        let (mut store, anchor) = seeded_store(4);
        let before = store.mutation_counter();
        let target = cp(0, anchor);
        let att = indexed(&[0, 1], 0, anchor, target);

        on_attestation(&mut store, &att, false).unwrap();

        assert_eq!(
            store.latest_message(ValidatorIndex::new(0)),
            Some(LatestMessage {
                epoch: Epoch::new(0),
                root: anchor,
            })
        );
        assert_eq!(
            store.latest_message(ValidatorIndex::new(1)),
            Some(LatestMessage {
                epoch: Epoch::new(0),
                root: anchor,
            })
        );
        assert!(store.latest_message(ValidatorIndex::new(2)).is_none());
        assert!(
            store.mutation_counter() > before,
            "tracker change must bump mutation_counter"
        );
        // next_* written; current still zero until compute_deltas.
        assert_eq!(store.votes()[0].next_root, anchor);
        assert_eq!(store.votes()[0].current_root, Root::ZERO);
    }

    #[test]
    fn on_attestation_does_not_change_node_weights() {
        let (mut store, anchor) = seeded_store(2);
        let child = root(0x22);
        insert_child(&mut store, anchor, child, 1);
        store.set_time(12); // slot 2 so slot-1 att is past

        let weights_before: Vec<i64> = store
            .proto_array()
            .nodes()
            .iter()
            .map(|n| n.weight)
            .collect();

        let att = indexed(&[0], 1, child, cp(0, anchor));
        on_attestation(&mut store, &att, false).unwrap();
        on_attestation(&mut store, &att, false).unwrap(); // idempotent epoch

        let weights_after: Vec<i64> = store
            .proto_array()
            .nodes()
            .iter()
            .map(|n| n.weight)
            .collect();
        assert_eq!(
            weights_before, weights_after,
            "on_attestation must perform no weight arithmetic"
        );
    }

    #[test]
    fn is_from_block_skips_target_epoch_scope_check() {
        let (mut store, anchor) = seeded_store(2);
        // Store still in epoch 0 (slot 1). Build a target that claims epoch 2 —
        // invalid for the free-floating path, but is_from_block relaxes the check.
        // Epoch/slot must still match: slot 16 is epoch 2 on minimal (8 slots/epoch).
        store.set_time(6); // slot 1, epoch 0
        // Need a block at the epoch-2 boundary for target root known + consistency.
        // Without a real epoch-2 chain the remaining checks fail first. Exercise
        // only the epoch-scope branch via validate_on_attestation with a known root.
        let err =
            validate_on_attestation::<Minimal>(&store, Slot::new(16), anchor, cp(2, anchor), false)
                .unwrap_err();
        assert!(
            matches!(err, OnAttestationError::FutureTargetEpoch { .. }),
            "is_from_block=false must reject future target epoch: {err:?}"
        );

        // Same inputs with is_from_block=true: epoch scope is skipped; next failure
        // is FutureSlot (store slot 1 < 16+1) or TargetEpochMismatch — slot 16 is
        // epoch 2 so epoch matches. FutureSlot is the next assert.
        let err =
            validate_on_attestation::<Minimal>(&store, Slot::new(16), anchor, cp(2, anchor), true)
                .unwrap_err();
        assert!(
            matches!(err, OnAttestationError::FutureSlot { .. }),
            "is_from_block=true skips epoch scope; next is FutureSlot: {err:?}"
        );
    }

    #[test]
    fn future_slot_is_deferrable() {
        let (store, anchor) = seeded_store(1);
        // store at slot 1; attestation for slot 5 is future.
        let err =
            validate_on_attestation::<Minimal>(&store, Slot::new(5), anchor, cp(0, anchor), false)
                .unwrap_err();
        // slot 5 is still epoch 0, so epoch scope passes; FutureSlot fires.
        assert!(
            matches!(err, OnAttestationError::FutureSlot { .. }),
            "{err:?}"
        );
        assert!(err.is_deferrable());
    }

    #[test]
    fn future_target_epoch_is_deferrable_unit_documents_branch() {
        let (store, anchor) = seeded_store(1);
        // Branch: validate_target_epoch_against_current_time (target > current)
        let err =
            validate_on_attestation::<Minimal>(&store, Slot::new(16), anchor, cp(2, anchor), false)
                .unwrap_err();
        assert!(
            matches!(err, OnAttestationError::FutureTargetEpoch { .. }),
            "{err:?}"
        );
        assert!(err.is_deferrable());
    }

    /// Past target epoch outside `[previous, current]` is **not** deferrable
    /// (SEC-16-6 / F1) — requeueing would spin forever.
    #[test]
    fn past_target_epoch_is_not_deferrable() {
        let (mut store, anchor) = seeded_store(1);
        // Minimal: 8 slots/epoch × 6 s → epoch 2 starts at t=96 (slot 16).
        store.set_time(96);
        assert_eq!(store.get_current_store_epoch().as_u64(), 2);
        // previous = 1; target epoch 0 is past-out-of-scope.
        let err =
            validate_on_attestation::<Minimal>(&store, Slot::new(0), anchor, cp(0, anchor), false)
                .unwrap_err();
        assert!(
            matches!(err, OnAttestationError::PastTargetEpoch { .. }),
            "got {err:?}"
        );
        assert!(
            !err.is_deferrable(),
            "past target epoch must not be requeued"
        );
    }

    #[test]
    fn out_of_range_validator_index_is_rejected() {
        let (mut store, anchor) = seeded_store(2);
        let att = indexed(&[0, 99], 0, anchor, cp(0, anchor));
        let err = on_attestation(&mut store, &att, false).unwrap_err();
        assert!(
            matches!(
                err,
                OnAttestationError::ValidatorIndexOutOfRange {
                    index: 99,
                    capacity: 2
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn get_forkchoice_store_seeds_justified_balances_len() {
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
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        assert_eq!(
            store.justified_balances().len(),
            store.vote_capacity(),
            "justified_balances must be seeded to vote capacity at init (SEC-16-3)"
        );
    }

    #[test]
    fn unknown_block_is_deferrable() {
        let (store, anchor) = seeded_store(1);
        let err = validate_on_attestation::<Minimal>(
            &store,
            Slot::new(0),
            root(0xEE),
            cp(0, anchor),
            false,
        )
        .unwrap_err();
        assert!(
            matches!(err, OnAttestationError::UnknownBlock(_)),
            "{err:?}"
        );
        assert!(err.is_deferrable());
    }

    #[test]
    fn older_epoch_attestation_does_not_overwrite_newer() {
        let (mut store, anchor) = seeded_store(1);
        let child = root(0x33);
        insert_child(&mut store, anchor, child, 1);
        // Move to epoch 1 so we can accept both epoch-0 and epoch-1 messages.
        // Minimal: 8 slots/epoch × 6 s = 48 s/epoch → epoch 1 starts at t=48.
        store.set_time(54); // slot 9

        let newer = indexed(&[0], 8, child, cp(1, child));
        // target root must be known; child is at slot 1, not epoch-1 start.
        // For epoch 1, checkpoint block of child is still the epoch-start ancestor.
        // Use child as beacon block; target root must equal get_checkpoint_block.
        // Simpler path: vote for anchor at epoch 0 then try to re-vote older — both
        // epoch 0, second with same epoch does not overwrite (strict >).
        let first = indexed(&[0], 0, anchor, cp(0, anchor));
        on_attestation(&mut store, &first, true).unwrap();
        let mid = store.votes()[0];

        // Same epoch, different root: must NOT overwrite (spec uses `>` not `>=`).
        let same_epoch = indexed(&[0], 1, child, cp(0, anchor));
        // LMD consistency: target root == checkpoint of beacon at target epoch.
        // child at slot 1 epoch 0 → checkpoint is epoch-0 start = anchor (if anchor
        // is the epoch start). get_checkpoint_block(child, 0) walks to slot 0.
        on_attestation(&mut store, &same_epoch, true).unwrap();
        assert_eq!(
            store.votes()[0].next_root,
            mid.next_root,
            "same-epoch attestation must not replace an existing message"
        );

        // Force a newer epoch update via direct tracker write to avoid checkpoint
        // consistency setup for epoch 1, then confirm older is rejected by the
        // update_latest_messages path.
        store.votes_mut()[0].next_epoch = Epoch::new(1);
        store.votes_mut()[0].next_root = child;
        let changed =
            update_latest_messages(&mut store, [ValidatorIndex::new(0)], Epoch::new(0), anchor)
                .unwrap();
        assert!(!changed);
        assert_eq!(store.votes()[0].next_root, child);
        let _ = newer; // silence if unused depending on path
    }

    // --- compute_deltas -----------------------------------------------------

    #[test]
    fn compute_deltas_applies_new_votes_and_promotes_current() {
        let mut indices = HashMap::new();
        let r0 = root(1);
        let r1 = root(2);
        indices.insert(r0, 0);
        indices.insert(r1, 1);

        let mut votes = vec![
            VoteTracker {
                current_root: Root::ZERO,
                next_root: r0,
                next_epoch: Epoch::new(0),
            },
            VoteTracker {
                current_root: r0,
                next_root: r1,
                next_epoch: Epoch::new(1),
            },
        ];
        let balances = vec![32_000_000_000u64, 32_000_000_000u64];
        let equiv = BTreeSet::new();

        let deltas = compute_deltas(&indices, &mut votes, &balances, &balances, &equiv).unwrap();

        // val0: +balance on r0; val1: -balance on r0, +balance on r1
        assert_eq!(deltas[0], 0); // +32e9 - 32e9
        assert_eq!(deltas[1], 32_000_000_000i64);
        assert_eq!(votes[0].current_root, r0);
        assert_eq!(votes[1].current_root, r1);
    }

    #[test]
    fn compute_deltas_skips_equivocating_and_retracts_current() {
        let mut indices = HashMap::new();
        let r0 = root(1);
        indices.insert(r0, 0);

        let mut votes = vec![VoteTracker {
            current_root: r0,
            next_root: r0,
            next_epoch: Epoch::new(0),
        }];
        let balances = vec![10u64];
        let mut equiv = BTreeSet::new();
        equiv.insert(ValidatorIndex::new(0));

        let deltas = compute_deltas(&indices, &mut votes, &balances, &balances, &equiv).unwrap();
        assert_eq!(deltas[0], -10);
        assert_eq!(votes[0].current_root, Root::ZERO);

        // Second pass: no further retraction.
        let deltas2 = compute_deltas(&indices, &mut votes, &balances, &balances, &equiv).unwrap();
        assert_eq!(deltas2[0], 0);
    }

    #[test]
    fn compute_deltas_uses_old_and_new_balance_snapshots() {
        let mut indices = HashMap::new();
        let r0 = root(1);
        indices.insert(r0, 0);

        let mut votes = vec![VoteTracker {
            current_root: r0,
            next_root: r0,
            next_epoch: Epoch::new(0),
        }];
        let old_balances = vec![10u64];
        let new_balances = vec![20u64];
        let equiv = BTreeSet::new();

        let deltas =
            compute_deltas(&indices, &mut votes, &old_balances, &new_balances, &equiv).unwrap();
        // -old + new = +10
        assert_eq!(deltas[0], 10);
    }

    /// Store-level balance snapshot: old `justified_balances` used for retraction,
    /// `new_balances` for the applied vote (justification-change shape).
    #[test]
    fn store_balance_snapshot_old_retract_new_apply_via_apply_deltas() {
        let (mut store, anchor) = seeded_store(1);
        let att = indexed(&[0], 0, anchor, cp(0, anchor));
        on_attestation(&mut store, &att, false).unwrap();

        // First apply with old=new=32 Gwei → weight on anchor.
        let bal_32 = vec![32u64];
        store.set_justified_balances(vec![0]); // first pass: old zero, new 32
        apply_attestation_deltas(&mut store, &bal_32).unwrap();
        assert_eq!(store.proto_array().get(&anchor).unwrap().weight, 32);
        assert_eq!(store.justified_balances(), &[32]);

        // Justification change: balances double; same vote root → -32 + 64 = +32.
        let bal_64 = vec![64u64];
        apply_attestation_deltas(&mut store, &bal_64).unwrap();
        assert_eq!(store.proto_array().get(&anchor).unwrap().weight, 64);
        assert_eq!(store.justified_balances(), &[64]);
    }

    #[test]
    fn on_attestation_batch_then_deltas_change_weights() {
        let (mut store, anchor) = seeded_store(2);
        let child = root(0x44);
        insert_child(&mut store, anchor, child, 1);
        store.set_time(12);

        let w_before: Vec<i64> = store
            .proto_array()
            .nodes()
            .iter()
            .map(|n| n.weight)
            .collect();

        let att = indexed(&[0, 1], 1, child, cp(0, anchor));
        // Capture instrumented count immediately around the hot path (other tests
        // may also call compute_deltas in parallel — only assert this call gap).
        let before_batch = COMPUTE_DELTAS_CALLS.load(Ordering::SeqCst);
        on_attestation(&mut store, &att, false).unwrap();
        let after_batch = COMPUTE_DELTAS_CALLS.load(Ordering::SeqCst);
        assert!(after_batch >= before_batch, "counter is monotonic");
        // If no other test raced, equal; if raced, still no *local* call — weight
        // check below is the authoritative no-weight-arithmetic proof.
        let _ = after_batch.saturating_sub(before_batch); // observed delta (0 expected)

        let w_mid: Vec<i64> = store
            .proto_array()
            .nodes()
            .iter()
            .map(|n| n.weight)
            .collect();
        assert_eq!(
            w_before, w_mid,
            "on_attestation must perform no weight arithmetic (batching contract)"
        );

        let balances = store.justified_balances().to_vec();
        let before_apply = COMPUTE_DELTAS_CALLS.load(Ordering::SeqCst);
        let deltas = apply_attestation_deltas(&mut store, &balances).unwrap();
        let after_apply = COMPUTE_DELTAS_CALLS.load(Ordering::SeqCst);
        assert!(
            after_apply > before_apply,
            "apply path must invoke instrumented compute_deltas (before={before_apply} after={after_apply})"
        );
        assert!(deltas.iter().any(|d| *d != 0), "expected non-zero deltas");

        let w_after: Vec<i64> = store
            .proto_array()
            .nodes()
            .iter()
            .map(|n| n.weight)
            .collect();
        assert_ne!(w_mid, w_after, "weights change only after delta apply");
    }

    // --- on_attester_slashing -----------------------------------------------

    #[test]
    fn on_attester_slashing_marks_equivocating_intersection() {
        let (mut store, anchor) = seeded_store(4);
        let before = store.mutation_counter();

        // Double vote: same target epoch, different data.
        let a1 = indexed(&[0, 1, 2], 0, anchor, cp(0, anchor));
        let mut a2 = indexed(&[1, 2, 3], 0, anchor, cp(0, anchor));
        // Make data differ so is_slashable_attestation_data is true (double vote).
        a2.data.beacon_block_root = root(0xAB);
        // But root 0xAB is unknown — slashings only need data slashability at
        // store level; we don't re-validate attestation roots here.
        // Wait — double vote requires data_1 != data_2 && same target epoch.
        // a2 has different beacon_block_root → data differs. Good.

        let slashing = AttesterSlashing {
            attestation_1: a1,
            attestation_2: a2,
        };
        on_attester_slashing(&mut store, &slashing).unwrap();

        assert!(
            store
                .equivocating_indices()
                .contains(&ValidatorIndex::new(1))
        );
        assert!(
            store
                .equivocating_indices()
                .contains(&ValidatorIndex::new(2))
        );
        assert!(
            !store
                .equivocating_indices()
                .contains(&ValidatorIndex::new(0))
        );
        assert!(
            !store
                .equivocating_indices()
                .contains(&ValidatorIndex::new(3))
        );
        assert!(store.mutation_counter() > before);
    }

    #[test]
    fn slashed_validator_stops_contributing_weight_via_deltas() {
        let (mut store, anchor) = seeded_store(1);
        store.set_time(12);

        // Vote, apply deltas so current_root is set and weight is on the node.
        let att = indexed(&[0], 0, anchor, cp(0, anchor));
        on_attestation(&mut store, &att, false).unwrap();
        let balances = store.justified_balances().to_vec();
        apply_attestation_deltas(&mut store, &balances).unwrap();

        let weight_with_vote = store.proto_array().get(&anchor).unwrap().weight;
        assert!(weight_with_vote > 0);

        // Slash the validator (double vote against a synthetic conflicting att).
        let a1 = indexed(&[0], 0, anchor, cp(0, anchor));
        let mut a2 = indexed(&[0], 0, anchor, cp(0, anchor));
        a2.data.beacon_block_root = root(0xFF);
        on_attester_slashing(
            &mut store,
            &AttesterSlashing {
                attestation_1: a1,
                attestation_2: a2,
            },
        )
        .unwrap();

        apply_attestation_deltas(&mut store, &balances).unwrap();
        let weight_after = store.proto_array().get(&anchor).unwrap().weight;
        assert_eq!(
            weight_after, 0,
            "equivocating validator weight must be fully retracted"
        );
        assert_eq!(store.votes()[0].current_root, Root::ZERO);
    }

    #[test]
    fn mutation_counter_bumped_by_each_public_mutator() {
        let (mut store, anchor) = seeded_store(2);
        let c0 = store.mutation_counter();

        let att = indexed(&[0], 0, anchor, cp(0, anchor));
        on_attestation(&mut store, &att, false).unwrap();
        let c1 = store.mutation_counter();
        assert!(c1 > c0, "on_attestation bumps on change");

        // No-op same-epoch re-apply: no bump.
        on_attestation(&mut store, &att, false).unwrap();
        assert_eq!(store.mutation_counter(), c1);

        let a1 = indexed(&[0], 0, anchor, cp(0, anchor));
        let mut a2 = indexed(&[0], 0, anchor, cp(0, anchor));
        a2.data.beacon_block_root = root(0x01);
        on_attester_slashing(
            &mut store,
            &AttesterSlashing {
                attestation_1: a1,
                attestation_2: a2,
            },
        )
        .unwrap();
        let c2 = store.mutation_counter();
        assert!(c2 > c1, "on_attester_slashing bumps on new indices");

        // store_target_checkpoint_context alone does not bump (cache fill).
        // Re-insert same target: still no bump beyond prior.
        store_target_checkpoint_context(&mut store, cp(0, anchor)).unwrap();
        assert_eq!(store.mutation_counter(), c2);

        let balances = store.justified_balances().to_vec();
        apply_attestation_deltas(&mut store, &balances).unwrap();
        assert!(
            store.mutation_counter() > c2,
            "apply_attestation_deltas bumps after weight apply"
        );
    }

    #[test]
    fn store_target_checkpoint_context_uses_checkpoint_context_not_full_state() {
        let (mut store, anchor) = seeded_store(1);
        let target = cp(0, anchor);
        // get_forkchoice_store already inserted a context for justified == target.
        assert!(store.checkpoint_context(target).is_some());
        let len_before = store.checkpoint_contexts_len();

        store_target_checkpoint_context(&mut store, target).unwrap();
        assert_eq!(store.checkpoint_contexts_len(), len_before);

        // Evict by flooding LRU, then rebuild.
        for i in 1u8..=10 {
            let r = root(i);
            // Need a block state to rebuild — skip full rebuild; just confirm
            // type path returns CheckpointContext balances field.
            let ctx = CheckpointContext {
                epoch: Epoch::new(i as u64),
                committee_cache: Default::default(),
                effective_balances: vec![Gwei::new(1)],
                total_active_balance: Gwei::new(1),
                fork: Fork {
                    previous_version: Default::default(),
                    current_version: Default::default(),
                    epoch: Epoch::new(0),
                },
                genesis_validators_root: Root::ZERO,
            };
            store.insert_checkpoint_context(cp(i as u64, r), Arc::new(ctx));
        }
        // Original may have been evicted; function rebuilds from block state.
        store_target_checkpoint_context(&mut store, target).unwrap();
        let ctx = store.checkpoint_context(target).expect("rebuilt");
        assert_eq!(ctx.epoch, Epoch::new(0));
    }
}
