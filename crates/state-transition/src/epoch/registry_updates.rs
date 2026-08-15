//! Spec `process_registry_updates` (Electra).

use cc_types::BeaconState;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, ValidatorIndex};

use crate::epoch_cache::note_registry_or_effective_balance_change;
use crate::error::EpochError;
use crate::helpers::accessors::get_current_epoch;
use crate::helpers::constants::EJECTION_BALANCE;
use crate::helpers::misc::compute_activation_exit_epoch;
use crate::helpers::mutators::initiate_validator_exit;
use crate::helpers::predicates::{
    is_active_validator, is_eligible_for_activation, is_eligible_for_activation_queue,
};

use super::block_to_epoch;

/// Spec `process_registry_updates` (Electra).
///
/// Single pass: activation-queue eligibility, ejections (via balance-based
/// exit churn in `initiate_validator_exit`), and activations for finalized
/// eligibles. Electra removed the pre-Electra count-based activation churn.
pub fn process_registry_updates<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    let current_epoch = get_current_epoch(state);
    let activation_epoch = compute_activation_exit_epoch::<P>(current_epoch);
    let finalized_epoch = state.finalized_checkpoint().epoch;
    let n = state.validators_len();

    let mut mutated = false;
    for index in 0..n {
        let vi = ValidatorIndex::new(index as u64);
        // Snapshot fields we need before any mutator may re-borrow the registry.
        let (eligible_queue, active_eject, eligible_activate) = {
            let v = state
                .validators_get(index)
                .ok_or(EpochError::ArithmeticOverflow)?;
            (
                is_eligible_for_activation_queue(v),
                is_active_validator(v, current_epoch)
                    && v.effective_balance.as_u64() <= EJECTION_BALANCE.as_u64(),
                is_eligible_for_activation(finalized_epoch, v),
            )
        };

        if eligible_queue {
            let v = state
                .validators_get_mut(index)
                .ok_or(EpochError::ArithmeticOverflow)?;
            v.activation_eligibility_epoch = Epoch::new(current_epoch.as_u64().saturating_add(1));
            mutated = true;
        } else if active_eject {
            initiate_validator_exit(state, vi).map_err(block_to_epoch)?;
            mutated = true;
        } else if eligible_activate {
            let v = state
                .validators_get_mut(index)
                .ok_or(EpochError::ArithmeticOverflow)?;
            v.activation_epoch = activation_epoch;
            mutated = true;
        }
    }

    if mutated {
        note_registry_or_effective_balance_change(state);
    }
    Ok(())
}
