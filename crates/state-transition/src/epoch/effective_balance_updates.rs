//! Spec `process_effective_balance_updates` (Electra hysteresis).

use cc_types::preset::Preset;
use cc_types::primitives::Gwei;
use cc_types::BeaconState;

use crate::error::EpochError;
use crate::epoch_cache::note_registry_or_effective_balance_change;
use crate::helpers::constants::{
    EFFECTIVE_BALANCE_INCREMENT, HYSTERESIS_DOWNWARD_MULTIPLIER, HYSTERESIS_QUOTIENT,
    HYSTERESIS_UPWARD_MULTIPLIER,
};
use crate::helpers::predicates::get_max_effective_balance;

/// Spec `process_effective_balance_updates`.
///
/// Hysteresis thresholds use `HYSTERESIS_*` multipliers; max effective balance
/// is credential-dependent (`MIN_ACTIVATION_BALANCE` vs
/// `MAX_EFFECTIVE_BALANCE_ELECTRA`).
pub fn process_effective_balance_updates<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    let hysteresis_increment = EFFECTIVE_BALANCE_INCREMENT.as_u64() / HYSTERESIS_QUOTIENT;
    let downward_threshold = hysteresis_increment.saturating_mul(HYSTERESIS_DOWNWARD_MULTIPLIER);
    let upward_threshold = hysteresis_increment.saturating_mul(HYSTERESIS_UPWARD_MULTIPLIER);
    let increment = EFFECTIVE_BALANCE_INCREMENT.as_u64();

    let n = state.validators_len();
    let mut mutated = false;
    for index in 0..n {
        let balance = state
            .balances_get(index)
            .ok_or(EpochError::ArithmeticOverflow)?
            .as_u64();
        let (effective_balance, max_eb) = {
            let v = state
                .validators_get(index)
                .ok_or(EpochError::ArithmeticOverflow)?;
            (v.effective_balance.as_u64(), get_max_effective_balance(v).as_u64())
        };

        if balance.saturating_add(downward_threshold) < effective_balance
            || effective_balance.saturating_add(upward_threshold) < balance
        {
            let new_eb = (balance - (balance % increment)).min(max_eb);
            if new_eb != effective_balance {
                let v = state
                    .validators_get_mut(index)
                    .ok_or(EpochError::ArithmeticOverflow)?;
                v.effective_balance = Gwei::new(new_eb);
                mutated = true;
            }
        }
    }

    if mutated {
        note_registry_or_effective_balance_change(state);
    }
    Ok(())
}
