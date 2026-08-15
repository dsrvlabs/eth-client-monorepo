//! Spec `process_justification_and_finalization` / `weigh_justification_and_finalization`.
//!
//! Callable standalone on a cloned state (CC-15b `compute_pulled_up_tip`); takes
//! only `&mut BeaconState` — no `TransitionContext`.

use cc_types::BeaconState;
use cc_types::containers::Checkpoint;
use cc_types::preset::Preset;
use cc_types::primitives::Epoch;
use cc_types::state::JustificationBitsLength;
use ssz_types::BitVector;

use crate::epoch_cache::total_active_balance_cached;
use crate::error::EpochError;
use crate::helpers::accessors::{
    get_block_root, get_current_epoch, get_previous_epoch, get_total_balance,
    get_unslashed_participating_indices,
};
use crate::helpers::constants::{
    GENESIS_EPOCH, JUSTIFICATION_BITS_LENGTH, TIMELY_TARGET_FLAG_INDEX,
};

use super::block_to_epoch;

/// Spec `process_justification_and_finalization` (Altair+).
///
/// Skips the first two epochs (`GENESIS_EPOCH` and `GENESIS_EPOCH + 1`) so the
/// genesis FFG stub is not mutated.
pub fn process_justification_and_finalization<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    if get_current_epoch(state).as_u64() <= GENESIS_EPOCH.as_u64().saturating_add(1) {
        return Ok(());
    }

    let previous_epoch = get_previous_epoch(state);
    let current_epoch = get_current_epoch(state);

    let previous_indices =
        get_unslashed_participating_indices(state, TIMELY_TARGET_FLAG_INDEX, previous_epoch)
            .map_err(block_to_epoch)?;
    let current_indices =
        get_unslashed_participating_indices(state, TIMELY_TARGET_FLAG_INDEX, current_epoch)
            .map_err(block_to_epoch)?;

    let total_active_balance = total_active_balance_cached(state).map_err(block_to_epoch)?;
    let previous_target_balance =
        get_total_balance(state, &previous_indices).map_err(block_to_epoch)?;
    let current_target_balance =
        get_total_balance(state, &current_indices).map_err(block_to_epoch)?;

    weigh_justification_and_finalization(
        state,
        total_active_balance.as_u64(),
        previous_target_balance.as_u64(),
        current_target_balance.as_u64(),
    )
}

/// Spec `weigh_justification_and_finalization`.
pub fn weigh_justification_and_finalization<P: Preset>(
    state: &mut BeaconState<P>,
    total_active_balance: u64,
    previous_epoch_target_balance: u64,
    current_epoch_target_balance: u64,
) -> Result<(), EpochError> {
    let previous_epoch = get_previous_epoch(state);
    let current_epoch = get_current_epoch(state);
    let old_previous_justified = state.previous_justified_checkpoint();
    let old_current_justified = state.current_justified_checkpoint();

    // Process justifications: shift bits, clear bit 0, then set on supermajority.
    state.set_previous_justified_checkpoint(state.current_justified_checkpoint());

    let old_bits = state.justification_bits().clone();
    let mut new_bits: BitVector<JustificationBitsLength> = BitVector::default();
    // new[0] = 0; new[i+1] = old[i] for i in 0..3
    new_bits
        .set(0, false)
        .map_err(|_| EpochError::ArithmeticOverflow)?;
    for i in 0..JUSTIFICATION_BITS_LENGTH.saturating_sub(1) {
        let bit = old_bits
            .get(i)
            .map_err(|_| EpochError::ArithmeticOverflow)?;
        new_bits
            .set(i + 1, bit)
            .map_err(|_| EpochError::ArithmeticOverflow)?;
    }

    // previous epoch supermajority → justify previous, set bit 1
    let prev_ok = checked_supermajority(previous_epoch_target_balance, total_active_balance)?;
    if prev_ok {
        let root = get_block_root(state, previous_epoch).map_err(block_to_epoch)?;
        state.set_current_justified_checkpoint(Checkpoint {
            epoch: previous_epoch,
            root,
        });
        new_bits
            .set(1, true)
            .map_err(|_| EpochError::ArithmeticOverflow)?;
    }

    // current epoch supermajority → justify current, set bit 0
    let curr_ok = checked_supermajority(current_epoch_target_balance, total_active_balance)?;
    if curr_ok {
        let root = get_block_root(state, current_epoch).map_err(block_to_epoch)?;
        state.set_current_justified_checkpoint(Checkpoint {
            epoch: current_epoch,
            root,
        });
        new_bits
            .set(0, true)
            .map_err(|_| EpochError::ArithmeticOverflow)?;
    }

    state.set_justification_bits(new_bits);

    // Process finalizations (four rules). Snapshot bits to release the state borrow.
    let bits = state.justification_bits().clone();
    let bit = |i: usize| -> Result<bool, EpochError> {
        bits.get(i).map_err(|_| EpochError::ArithmeticOverflow)
    };

    // 2nd/3rd/4th most recent justified; 2nd used 4th as source
    if bit(1)? && bit(2)? && bit(3)? && epochs_add(old_previous_justified.epoch, 3) == current_epoch
    {
        state.set_finalized_checkpoint(old_previous_justified);
    }
    // 2nd/3rd most recent justified; 2nd used 3rd as source
    if bit(1)? && bit(2)? && epochs_add(old_previous_justified.epoch, 2) == current_epoch {
        state.set_finalized_checkpoint(old_previous_justified);
    }
    // 1st/2nd/3rd most recent justified; 1st used 3rd as source
    if bit(0)? && bit(1)? && bit(2)? && epochs_add(old_current_justified.epoch, 2) == current_epoch
    {
        state.set_finalized_checkpoint(old_current_justified);
    }
    // 1st/2nd most recent justified; 1st used 2nd as source
    if bit(0)? && bit(1)? && epochs_add(old_current_justified.epoch, 1) == current_epoch {
        state.set_finalized_checkpoint(old_current_justified);
    }

    Ok(())
}

/// `target * 3 >= total * 2` with overflow → Internal.
fn checked_supermajority(target: u64, total: u64) -> Result<bool, EpochError> {
    let lhs = target
        .checked_mul(3)
        .ok_or(EpochError::ArithmeticOverflow)?;
    let rhs = total.checked_mul(2).ok_or(EpochError::ArithmeticOverflow)?;
    Ok(lhs >= rhs)
}

fn epochs_add(epoch: Epoch, n: u64) -> Epoch {
    Epoch::new(epoch.as_u64().saturating_add(n))
}
