//! Spec `process_withdrawals` / `get_expected_withdrawals` (Electra).

use cc_types::operations::Withdrawal;
use cc_types::preset::Preset;
use cc_types::primitives::{Gwei, ValidatorIndex};
use cc_types::{BeaconBlock, BeaconState};

use crate::error::{BlockError, OperationError};
use crate::helpers::accessors::get_current_epoch;
use crate::helpers::constants::{FAR_FUTURE_EPOCH, MIN_ACTIVATION_BALANCE};
use crate::helpers::misc::execution_address_from_credentials;
use crate::helpers::mutators::decrease_balance;
use crate::helpers::predicates::{
    get_max_effective_balance, is_fully_withdrawable_validator, is_partially_withdrawable_validator,
};

/// Spec `get_expected_withdrawals`.
///
/// Returns `(expected_withdrawals, processed_partial_withdrawals_count)`.
pub fn get_expected_withdrawals<P: Preset>(
    state: &BeaconState<P>,
) -> Result<(Vec<Withdrawal>, usize), BlockError> {
    let epoch = get_current_epoch(state);
    let mut withdrawal_index = state.next_withdrawal_index();
    let mut validator_index = state.next_withdrawal_validator_index();
    let mut withdrawals: Vec<Withdrawal> = Vec::new();
    let mut processed_partial_withdrawals_count = 0usize;

    let validator_count = state.validators_len();
    if validator_count == 0 {
        return Ok((withdrawals, 0));
    }

    // Consume pending partial withdrawals (Electra).
    let ppw_len = state.pending_partial_withdrawals_len();
    for i in 0..ppw_len {
        let pending = state
            .pending_partial_withdrawals_get(i)
            .ok_or(BlockError::ArithmeticOverflow)?;
        if pending.withdrawable_epoch.as_u64() > epoch.as_u64()
            || withdrawals.len() as u64 == P::MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP
        {
            break;
        }

        let v_idx = pending.validator_index.as_u64() as usize;
        let validator = state
            .validators_get(v_idx)
            .ok_or(BlockError::ArithmeticOverflow)?;
        let has_sufficient_effective_balance =
            validator.effective_balance.as_u64() >= MIN_ACTIVATION_BALANCE.as_u64();
        let total_withdrawn: u64 = withdrawals
            .iter()
            .filter(|w| w.validator_index == pending.validator_index)
            .map(|w| w.amount.as_u64())
            .sum();
        let balance = state
            .balances_get(v_idx)
            .ok_or(BlockError::ArithmeticOverflow)?
            .as_u64()
            .saturating_sub(total_withdrawn);
        let has_excess_balance = balance > MIN_ACTIVATION_BALANCE.as_u64();
        if validator.exit_epoch == FAR_FUTURE_EPOCH
            && has_sufficient_effective_balance
            && has_excess_balance
        {
            let withdrawable_balance = (balance - MIN_ACTIVATION_BALANCE.as_u64())
                .min(pending.amount.as_u64());
            withdrawals.push(Withdrawal {
                index: withdrawal_index,
                validator_index: pending.validator_index,
                address: execution_address_from_credentials(&validator.withdrawal_credentials),
                amount: Gwei::new(withdrawable_balance),
            });
            withdrawal_index = withdrawal_index
                .checked_add(1)
                .ok_or(BlockError::ArithmeticOverflow)?;
        }
        processed_partial_withdrawals_count += 1;
    }

    // Sweep for remaining full/partial withdrawals.
    let bound = validator_count.min(P::MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP as usize);
    for _ in 0..bound {
        let v_idx = validator_index.as_u64() as usize;
        let validator = state
            .validators_get(v_idx)
            .ok_or(BlockError::ArithmeticOverflow)?;
        let total_withdrawn: u64 = withdrawals
            .iter()
            .filter(|w| w.validator_index == validator_index)
            .map(|w| w.amount.as_u64())
            .sum();
        let balance = Gwei::new(
            state
                .balances_get(v_idx)
                .ok_or(BlockError::ArithmeticOverflow)?
                .as_u64()
                .saturating_sub(total_withdrawn),
        );

        if is_fully_withdrawable_validator(validator, balance, epoch) {
            withdrawals.push(Withdrawal {
                index: withdrawal_index,
                validator_index,
                address: execution_address_from_credentials(&validator.withdrawal_credentials),
                amount: balance,
            });
            withdrawal_index = withdrawal_index
                .checked_add(1)
                .ok_or(BlockError::ArithmeticOverflow)?;
        } else if is_partially_withdrawable_validator(validator, balance) {
            let max_eb = get_max_effective_balance(validator);
            let amount = balance
                .checked_sub(max_eb.as_u64())
                .ok_or(BlockError::ArithmeticOverflow)?;
            withdrawals.push(Withdrawal {
                index: withdrawal_index,
                validator_index,
                address: execution_address_from_credentials(&validator.withdrawal_credentials),
                amount,
            });
            withdrawal_index = withdrawal_index
                .checked_add(1)
                .ok_or(BlockError::ArithmeticOverflow)?;
        }

        if withdrawals.len() == P::MAX_WITHDRAWALS_PER_PAYLOAD as usize {
            break;
        }
        validator_index =
            ValidatorIndex::new((validator_index.as_u64() + 1) % validator_count as u64);
    }

    Ok((withdrawals, processed_partial_withdrawals_count))
}

/// Spec `process_withdrawals`.
pub fn process_withdrawals<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
) -> Result<(), BlockError> {
    let (expected_withdrawals, processed_partial_withdrawals_count) =
        get_expected_withdrawals(state)?;

    let payload_withdrawals = block.body.execution_payload.withdrawals.as_ref();
    if expected_withdrawals.as_slice() != payload_withdrawals {
        return Err(BlockError::InvalidOperation(OperationError::Invalid {
            op: "withdrawals",
            detail: format!(
                "expected {} withdrawals, payload has {}",
                expected_withdrawals.len(),
                payload_withdrawals.len()
            ),
        }));
    }

    for withdrawal in &expected_withdrawals {
        decrease_balance(state, withdrawal.validator_index, withdrawal.amount)?;
    }

    // Update pending partial withdrawals queue.
    state.pending_partial_withdrawals_drain_prefix(processed_partial_withdrawals_count)?;

    // Update next withdrawal index if this block contained withdrawals.
    if let Some(latest) = expected_withdrawals.last() {
        state.set_next_withdrawal_index(
            latest
                .index
                .checked_add(1)
                .ok_or(BlockError::ArithmeticOverflow)?,
        );
    }

    // Update next validator index for the next sweep.
    let validator_count = state.validators_len();
    if validator_count == 0 {
        return Ok(());
    }
    if expected_withdrawals.len() == P::MAX_WITHDRAWALS_PER_PAYLOAD as usize {
        let last = expected_withdrawals
            .last()
            .ok_or(BlockError::ArithmeticOverflow)?;
        let next =
            ValidatorIndex::new((last.validator_index.as_u64() + 1) % validator_count as u64);
        state.set_next_withdrawal_validator_index(next);
    } else {
        let next_index = state
            .next_withdrawal_validator_index()
            .as_u64()
            .saturating_add(P::MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP);
        let next = ValidatorIndex::new(next_index % validator_count as u64);
        state.set_next_withdrawal_validator_index(next);
    }

    Ok(())
}
