//! Spec `process_slashings` (Electra correlation penalty).

use cc_types::preset::Preset;
use cc_types::primitives::{Gwei, ValidatorIndex};
use cc_types::BeaconState;

use crate::error::EpochError;
use crate::epoch_cache::total_active_balance_cached;
use crate::helpers::accessors::get_current_epoch;
use crate::helpers::constants::{
    EFFECTIVE_BALANCE_INCREMENT, PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX,
};
use crate::helpers::mutators::decrease_balance;

use super::block_to_epoch;

/// Spec `process_slashings` (Electra / Bellatrix multiplier).
///
/// Applies a proportional penalty to validators whose withdrawable epoch falls
/// at `epoch + EPOCHS_PER_SLASHINGS_VECTOR / 2`. Integer-division order matches
/// the Electra formula (penalty per EB increment first).
pub fn process_slashings<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    let epoch = get_current_epoch(state);
    let total_balance = total_active_balance_cached(state).map_err(block_to_epoch)?;

    let mut slashings_sum = 0u64;
    for i in 0..state.slashings_len() {
        let s = state
            .slashings_get(i)
            .ok_or(EpochError::ArithmeticOverflow)?;
        slashings_sum = slashings_sum
            .checked_add(s.as_u64())
            .ok_or(EpochError::ArithmeticOverflow)?;
    }

    let adjusted_total_slashing_balance = slashings_sum
        .checked_mul(PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX)
        .ok_or(EpochError::ArithmeticOverflow)?
        .min(total_balance.as_u64());

    let increment = EFFECTIVE_BALANCE_INCREMENT.as_u64();
    let total_increments = total_balance.as_u64() / increment;
    if total_increments == 0 {
        return Err(EpochError::ArithmeticOverflow);
    }
    let penalty_per_effective_balance_increment = adjusted_total_slashing_balance / total_increments;

    let n = state.validators_len();
    let half_vector = P::EPOCHS_PER_SLASHINGS_VECTOR / 2;
    for index in 0..n {
        let (slashed, withdrawable, effective_balance) = {
            let v = state
                .validators_get(index)
                .ok_or(EpochError::ArithmeticOverflow)?;
            (v.slashed, v.withdrawable_epoch, v.effective_balance)
        };
        if slashed
            && epoch
                .as_u64()
                .checked_add(half_vector)
                .ok_or(EpochError::ArithmeticOverflow)?
                == withdrawable.as_u64()
        {
            let effective_balance_increments = effective_balance.as_u64() / increment;
            let penalty = Gwei::new(
                penalty_per_effective_balance_increment
                    .checked_mul(effective_balance_increments)
                    .ok_or(EpochError::ArithmeticOverflow)?,
            );
            decrease_balance(state, ValidatorIndex::new(index as u64), penalty)
                .map_err(block_to_epoch)?;
        }
    }
    Ok(())
}
