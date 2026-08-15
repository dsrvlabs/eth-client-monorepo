//! Spec `process_consolidation_request` (Electra EIP-7251).
//!
//! **Invalid requests are no-ops** — early `Ok(())` without state mutation.

use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::operations::{ConsolidationRequest, PendingConsolidation};
use cc_types::preset::Preset;
use cc_types::primitives::Epoch;

use crate::error::BlockError;
use crate::helpers::accessors::{
    get_consolidation_churn_limit, get_current_epoch, get_pending_balance_to_withdraw,
    get_validator_index_by_pubkey,
};
use crate::helpers::constants::{
    FAR_FUTURE_EPOCH, MIN_ACTIVATION_BALANCE, MIN_VALIDATOR_WITHDRAWABILITY_DELAY,
};
use crate::helpers::misc::execution_address_from_credentials;
use crate::helpers::mutators::{
    compute_consolidation_epoch_and_update_churn, switch_to_compounding_validator,
};
use crate::helpers::predicates::{
    has_compounding_withdrawal_credential, has_eth1_withdrawal_credential,
    has_execution_withdrawal_credential, is_active_validator,
};

/// Spec `is_valid_switch_to_compounding_request`.
fn is_valid_switch_to_compounding_request<P: Preset>(
    state: &mut BeaconState<P>,
    consolidation_request: &ConsolidationRequest,
) -> bool {
    if consolidation_request.source_pubkey != consolidation_request.target_pubkey {
        return false;
    }
    let Some(source_index) =
        get_validator_index_by_pubkey(state, &consolidation_request.source_pubkey)
    else {
        return false;
    };
    let Some(source_validator) = state.validators_get(source_index.as_u64() as usize) else {
        return false;
    };

    if execution_address_from_credentials(&source_validator.withdrawal_credentials)
        != consolidation_request.source_address
    {
        return false;
    }
    if !has_eth1_withdrawal_credential(source_validator) {
        return false;
    }
    let current_epoch = get_current_epoch(state);
    if !is_active_validator(source_validator, current_epoch) {
        return false;
    }
    if source_validator.exit_epoch != FAR_FUTURE_EPOCH {
        return false;
    }
    true
}

/// Spec `process_consolidation_request`.
///
/// Switch-to-compounding special case (source == target with `0x01` credentials)
/// or source→target consolidation with churn accounting. Invalid → no-op.
pub fn process_consolidation_request<P: Preset>(
    state: &mut BeaconState<P>,
    consolidation_request: &ConsolidationRequest,
    config: &ChainConfig,
) -> Result<(), BlockError> {
    if is_valid_switch_to_compounding_request(state, consolidation_request) {
        // Re-resolve after the validity check (map may have been backfilled).
        let Some(source_index) =
            get_validator_index_by_pubkey(state, &consolidation_request.source_pubkey)
        else {
            return Ok(());
        };
        switch_to_compounding_validator(state, source_index)?;
        return Ok(());
    }

    // Verify that source != target, so a consolidation cannot be used as an exit.
    if consolidation_request.source_pubkey == consolidation_request.target_pubkey {
        return Ok(());
    }
    // If the pending consolidations queue is full, consolidation requests are ignored.
    if state.pending_consolidations_len() as u64 == P::PENDING_CONSOLIDATIONS_LIMIT {
        return Ok(());
    }
    // If there is too little available consolidation churn limit, ignore.
    if get_consolidation_churn_limit(state, config)?.as_u64() <= MIN_ACTIVATION_BALANCE.as_u64() {
        return Ok(());
    }

    let Some(source_index) =
        get_validator_index_by_pubkey(state, &consolidation_request.source_pubkey)
    else {
        return Ok(());
    };
    let Some(target_index) =
        get_validator_index_by_pubkey(state, &consolidation_request.target_pubkey)
    else {
        return Ok(());
    };

    let source_validator = match state.validators_get(source_index.as_u64() as usize) {
        Some(v) => *v,
        None => return Ok(()),
    };
    let target_validator = match state.validators_get(target_index.as_u64() as usize) {
        Some(v) => *v,
        None => return Ok(()),
    };

    let has_correct_credential = has_execution_withdrawal_credential(&source_validator);
    let is_correct_source_address =
        execution_address_from_credentials(&source_validator.withdrawal_credentials)
            == consolidation_request.source_address;
    if !(has_correct_credential && is_correct_source_address) {
        return Ok(());
    }
    if !has_compounding_withdrawal_credential(&target_validator) {
        return Ok(());
    }

    let current_epoch = get_current_epoch(state);
    if !is_active_validator(&source_validator, current_epoch) {
        return Ok(());
    }
    if !is_active_validator(&target_validator, current_epoch) {
        return Ok(());
    }
    if source_validator.exit_epoch != FAR_FUTURE_EPOCH {
        return Ok(());
    }
    if target_validator.exit_epoch != FAR_FUTURE_EPOCH {
        return Ok(());
    }
    if current_epoch.as_u64()
        < source_validator
            .activation_epoch
            .as_u64()
            .saturating_add(config.shard_committee_period.as_u64())
    {
        return Ok(());
    }
    if get_pending_balance_to_withdraw(state, source_index).as_u64() > 0 {
        return Ok(());
    }

    // Initiate source validator exit via consolidation churn and append pending.
    let exit_epoch = compute_consolidation_epoch_and_update_churn(
        state,
        source_validator.effective_balance,
        config,
    )?;
    let withdrawable_epoch = Epoch::new(
        exit_epoch
            .as_u64()
            .saturating_add(MIN_VALIDATOR_WITHDRAWABILITY_DELAY),
    );
    {
        let v = state
            .validators_get_mut(source_index.as_u64() as usize)
            .ok_or(BlockError::ArithmeticOverflow)?;
        v.exit_epoch = exit_epoch;
        v.withdrawable_epoch = withdrawable_epoch;
    }
    state.pending_consolidations_push(PendingConsolidation {
        source_index,
        target_index,
    })?;
    Ok(())
}
