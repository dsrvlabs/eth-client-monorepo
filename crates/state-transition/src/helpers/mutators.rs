//! Spec mutators (`increase_balance` / `decrease_balance` / exit / slash).

use cc_crypto::INFINITY_SIGNATURE;
use cc_types::operations::PendingDeposit;
use cc_types::preset::Preset;
use cc_types::primitives::{BlsSignature, Epoch, Gwei, Slot, ValidatorIndex};
use cc_types::BeaconState;

use crate::epoch_cache::note_registry_or_effective_balance_change;
use crate::error::BlockError;
use crate::helpers::accessors::{
    get_activation_exit_churn_limit, get_beacon_proposer_index, get_consolidation_churn_limit,
    get_current_epoch,
};
use crate::helpers::constants::{
    COMPOUNDING_WITHDRAWAL_PREFIX, FAR_FUTURE_EPOCH, GENESIS_SLOT, MIN_ACTIVATION_BALANCE,
    MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA, MIN_VALIDATOR_WITHDRAWABILITY_DELAY, PROPOSER_WEIGHT,
    WEIGHT_DENOMINATOR, WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA,
};
use crate::helpers::misc::compute_activation_exit_epoch;

/// Spec `increase_balance(state, index, delta)`.
pub fn increase_balance<P: Preset>(
    state: &mut BeaconState<P>,
    index: ValidatorIndex,
    delta: Gwei,
) -> Result<(), BlockError> {
    let i = index.as_u64() as usize;
    let bal = state
        .balances_get(i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let new = bal
        .checked_add(delta.as_u64())
        .ok_or(BlockError::ArithmeticOverflow)?;
    state.balances_set(i, new)?;
    Ok(())
}

/// Spec `decrease_balance(state, index, delta)` — saturating at zero.
pub fn decrease_balance<P: Preset>(
    state: &mut BeaconState<P>,
    index: ValidatorIndex,
    delta: Gwei,
) -> Result<(), BlockError> {
    let i = index.as_u64() as usize;
    let bal = state
        .balances_get(i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let new = bal.saturating_sub(delta.as_u64());
    state.balances_set(i, new)?;
    Ok(())
}

/// Spec `compute_exit_epoch_and_update_churn` (Electra).
pub fn compute_exit_epoch_and_update_churn<P: Preset>(
    state: &mut BeaconState<P>,
    exit_balance: Gwei,
) -> Result<Epoch, BlockError> {
    let current_epoch = get_current_epoch(state);
    let mut earliest_exit_epoch = state
        .earliest_exit_epoch()
        .as_u64()
        .max(compute_activation_exit_epoch::<P>(current_epoch).as_u64());
    let per_epoch_churn = get_activation_exit_churn_limit(state)?;

    let mut exit_balance_to_consume = if state.earliest_exit_epoch().as_u64() < earliest_exit_epoch {
        per_epoch_churn.as_u64()
    } else {
        state.exit_balance_to_consume().as_u64()
    };

    let exit_balance = exit_balance.as_u64();
    if exit_balance > exit_balance_to_consume {
        let balance_to_process = exit_balance - exit_balance_to_consume;
        let additional_epochs = (balance_to_process.saturating_sub(1) / per_epoch_churn.as_u64())
            .saturating_add(1);
        earliest_exit_epoch = earliest_exit_epoch.saturating_add(additional_epochs);
        exit_balance_to_consume = exit_balance_to_consume
            .saturating_add(additional_epochs.saturating_mul(per_epoch_churn.as_u64()));
    }

    state.set_exit_balance_to_consume(Gwei::new(
        exit_balance_to_consume.saturating_sub(exit_balance),
    ));
    state.set_earliest_exit_epoch(Epoch::new(earliest_exit_epoch));
    Ok(Epoch::new(earliest_exit_epoch))
}

/// Spec `initiate_validator_exit` (Electra).
pub fn initiate_validator_exit<P: Preset>(
    state: &mut BeaconState<P>,
    index: ValidatorIndex,
) -> Result<(), BlockError> {
    let i = index.as_u64() as usize;
    let validator = state
        .validators_get(i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    if validator.exit_epoch != FAR_FUTURE_EPOCH {
        return Ok(());
    }
    let effective_balance = validator.effective_balance;
    let exit_queue_epoch = compute_exit_epoch_and_update_churn(state, effective_balance)?;
    let withdrawable = Epoch::new(
        exit_queue_epoch
            .as_u64()
            .saturating_add(MIN_VALIDATOR_WITHDRAWABILITY_DELAY),
    );
    let v = state
        .validators_get_mut(i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    v.exit_epoch = exit_queue_epoch;
    v.withdrawable_epoch = withdrawable;
    // Exit epoch change affects future active sets in EpochCache (CC-13b).
    note_registry_or_effective_balance_change(state);
    Ok(())
}

/// Spec `compute_consolidation_epoch_and_update_churn` (Electra).
pub fn compute_consolidation_epoch_and_update_churn<P: Preset>(
    state: &mut BeaconState<P>,
    consolidation_balance: Gwei,
) -> Result<Epoch, BlockError> {
    let current_epoch = get_current_epoch(state);
    let mut earliest_consolidation_epoch = state
        .earliest_consolidation_epoch()
        .as_u64()
        .max(compute_activation_exit_epoch::<P>(current_epoch).as_u64());
    let per_epoch_churn = get_consolidation_churn_limit(state)?;

    let mut consolidation_balance_to_consume =
        if state.earliest_consolidation_epoch().as_u64() < earliest_consolidation_epoch {
            per_epoch_churn.as_u64()
        } else {
            state.consolidation_balance_to_consume().as_u64()
        };

    let consolidation_balance = consolidation_balance.as_u64();
    if consolidation_balance > consolidation_balance_to_consume {
        let balance_to_process = consolidation_balance - consolidation_balance_to_consume;
        let additional_epochs = (balance_to_process.saturating_sub(1) / per_epoch_churn.as_u64())
            .saturating_add(1);
        earliest_consolidation_epoch = earliest_consolidation_epoch.saturating_add(additional_epochs);
        consolidation_balance_to_consume = consolidation_balance_to_consume
            .saturating_add(additional_epochs.saturating_mul(per_epoch_churn.as_u64()));
    }

    state.set_consolidation_balance_to_consume(Gwei::new(
        consolidation_balance_to_consume.saturating_sub(consolidation_balance),
    ));
    state.set_earliest_consolidation_epoch(Epoch::new(earliest_consolidation_epoch));
    Ok(Epoch::new(earliest_consolidation_epoch))
}

/// Spec `queue_excess_active_balance` (Electra).
pub fn queue_excess_active_balance<P: Preset>(
    state: &mut BeaconState<P>,
    index: ValidatorIndex,
) -> Result<(), BlockError> {
    let i = index.as_u64() as usize;
    let balance = state
        .balances_get(i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    if balance.as_u64() > MIN_ACTIVATION_BALANCE.as_u64() {
        let excess = balance
            .as_u64()
            .saturating_sub(MIN_ACTIVATION_BALANCE.as_u64());
        state.balances_set(i, MIN_ACTIVATION_BALANCE)?;
        let validator = state
            .validators_get(i)
            .ok_or(BlockError::ArithmeticOverflow)?;
        state.pending_deposits_push(PendingDeposit {
            pubkey: validator.pubkey,
            withdrawal_credentials: validator.withdrawal_credentials,
            amount: Gwei::new(excess),
            signature: BlsSignature::from_array(INFINITY_SIGNATURE),
            slot: Slot::new(GENESIS_SLOT),
        })?;
    }
    Ok(())
}

/// Spec `switch_to_compounding_validator` (Electra).
pub fn switch_to_compounding_validator<P: Preset>(
    state: &mut BeaconState<P>,
    index: ValidatorIndex,
) -> Result<(), BlockError> {
    let i = index.as_u64() as usize;
    {
        let v = state
            .validators_get_mut(i)
            .ok_or(BlockError::ArithmeticOverflow)?;
        let mut creds = *v.withdrawal_credentials.as_array();
        creds[0] = COMPOUNDING_WITHDRAWAL_PREFIX;
        v.withdrawal_credentials = cc_types::primitives::Root::from_array(creds);
    }
    queue_excess_active_balance(state, index)
}

/// Spec `slash_validator` (Electra).
pub fn slash_validator<P: Preset>(
    state: &mut BeaconState<P>,
    slashed_index: ValidatorIndex,
    whistleblower_index: Option<ValidatorIndex>,
) -> Result<(), BlockError> {
    let epoch = get_current_epoch(state);
    initiate_validator_exit(state, slashed_index)?;

    let i = slashed_index.as_u64() as usize;
    let effective_balance = {
        let v = state
            .validators_get_mut(i)
            .ok_or(BlockError::ArithmeticOverflow)?;
        v.slashed = true;
        let min_withdrawable = Epoch::new(
            epoch
                .as_u64()
                .saturating_add(P::EPOCHS_PER_SLASHINGS_VECTOR),
        );
        if v.withdrawable_epoch.as_u64() < min_withdrawable.as_u64() {
            v.withdrawable_epoch = min_withdrawable;
        }
        v.effective_balance
    };

    let slashings_i = (epoch.as_u64() % P::EPOCHS_PER_SLASHINGS_VECTOR) as usize;
    let prev = state
        .slashings_get(slashings_i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let new_slashings = prev
        .checked_add(effective_balance.as_u64())
        .ok_or(BlockError::ArithmeticOverflow)?;
    state.slashings_set(slashings_i, new_slashings)?;

    let slashing_penalty = Gwei::new(
        effective_balance.as_u64() / MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA,
    );
    decrease_balance(state, slashed_index, slashing_penalty)?;

    let proposer_index = get_beacon_proposer_index(state)?;
    let whistleblower = whistleblower_index.unwrap_or(proposer_index);
    let whistleblower_reward = Gwei::new(
        effective_balance.as_u64() / WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA,
    );
    let proposer_reward = Gwei::new(
        whistleblower_reward.as_u64() * PROPOSER_WEIGHT / WEIGHT_DENOMINATOR,
    );
    increase_balance(state, proposer_index, proposer_reward)?;
    increase_balance(
        state,
        whistleblower,
        Gwei::new(
            whistleblower_reward
                .as_u64()
                .saturating_sub(proposer_reward.as_u64()),
        ),
    )?;
    Ok(())
}
