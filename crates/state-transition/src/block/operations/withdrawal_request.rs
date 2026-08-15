//! Spec `process_withdrawal_request` (Electra EIP-7002 / EIP-7251).
//!
//! **Invalid requests are no-ops** — the handler returns `Ok(())` without
//! mutating state rather than rejecting the block.

use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::operations::{PendingPartialWithdrawal, WithdrawalRequest};
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Gwei};

use crate::error::BlockError;
use crate::helpers::accessors::{
    get_current_epoch, get_pending_balance_to_withdraw, get_validator_index_by_pubkey,
};
use crate::helpers::constants::{
    FAR_FUTURE_EPOCH, FULL_EXIT_REQUEST_AMOUNT, MIN_ACTIVATION_BALANCE,
    MIN_VALIDATOR_WITHDRAWABILITY_DELAY,
};
use crate::helpers::misc::execution_address_from_credentials;
use crate::helpers::mutators::{compute_exit_epoch_and_update_churn, initiate_validator_exit};
use crate::helpers::predicates::{
    has_compounding_withdrawal_credential, has_execution_withdrawal_credential, is_active_validator,
};

/// Spec `process_withdrawal_request`.
///
/// Full-exit (`amount == 0`) vs partial withdrawal. Failed validity checks
/// return early with `Ok(())` (spec no-op discipline).
pub fn process_withdrawal_request<P: Preset>(
    state: &mut BeaconState<P>,
    withdrawal_request: &WithdrawalRequest,
    config: &ChainConfig,
) -> Result<(), BlockError> {
    let amount = withdrawal_request.amount.as_u64();
    let is_full_exit_request = amount == FULL_EXIT_REQUEST_AMOUNT;

    // If partial withdrawal queue is full, only full exits are processed.
    if state.pending_partial_withdrawals_len() as u64 == P::PENDING_PARTIAL_WITHDRAWALS_LIMIT
        && !is_full_exit_request
    {
        return Ok(());
    }

    let Some(index) = get_validator_index_by_pubkey(state, &withdrawal_request.validator_pubkey)
    else {
        return Ok(());
    };
    let i = index.as_u64() as usize;
    let validator = match state.validators_get(i) {
        Some(v) => *v,
        None => return Ok(()),
    };

    let has_correct_credential = has_execution_withdrawal_credential(&validator);
    let is_correct_source_address =
        execution_address_from_credentials(&validator.withdrawal_credentials)
            == withdrawal_request.source_address;
    if !(has_correct_credential && is_correct_source_address) {
        return Ok(());
    }

    let current_epoch = get_current_epoch(state);
    if !is_active_validator(&validator, current_epoch) {
        return Ok(());
    }
    if validator.exit_epoch != FAR_FUTURE_EPOCH {
        return Ok(());
    }
    if current_epoch.as_u64()
        < validator
            .activation_epoch
            .as_u64()
            .saturating_add(config.shard_committee_period.as_u64())
    {
        return Ok(());
    }

    let pending_balance_to_withdraw = get_pending_balance_to_withdraw(state, index);

    if is_full_exit_request {
        if pending_balance_to_withdraw.as_u64() == 0 {
            initiate_validator_exit(state, index, config)?;
        }
        return Ok(());
    }

    let balance = match state.balances_get(i) {
        Some(b) => b,
        None => return Ok(()),
    };
    let has_sufficient_effective_balance =
        validator.effective_balance.as_u64() >= MIN_ACTIVATION_BALANCE.as_u64();
    let has_excess_balance = balance.as_u64()
        > MIN_ACTIVATION_BALANCE
            .as_u64()
            .saturating_add(pending_balance_to_withdraw.as_u64());

    if has_compounding_withdrawal_credential(&validator)
        && has_sufficient_effective_balance
        && has_excess_balance
    {
        let to_withdraw = (balance
            .as_u64()
            .saturating_sub(MIN_ACTIVATION_BALANCE.as_u64())
            .saturating_sub(pending_balance_to_withdraw.as_u64()))
        .min(amount);
        let exit_queue_epoch =
            compute_exit_epoch_and_update_churn(state, Gwei::new(to_withdraw), config)?;
        let withdrawable_epoch = Epoch::new(
            exit_queue_epoch
                .as_u64()
                .saturating_add(MIN_VALIDATOR_WITHDRAWABILITY_DELAY),
        );
        state.pending_partial_withdrawals_push(PendingPartialWithdrawal {
            validator_index: index,
            amount: Gwei::new(to_withdraw),
            withdrawable_epoch,
        })?;
    }
    Ok(())
}
