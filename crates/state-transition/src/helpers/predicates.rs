//! Spec predicates for withdrawals, credentials, and validator status (Electra).

use cc_types::containers::{AttestationData, Validator};
use cc_types::primitives::{Epoch, Gwei, Root};

use crate::helpers::constants::{
    COMPOUNDING_WITHDRAWAL_PREFIX, ETH1_ADDRESS_WITHDRAWAL_PREFIX, MAX_EFFECTIVE_BALANCE_ELECTRA,
    MIN_ACTIVATION_BALANCE,
};

/// Spec `is_compounding_withdrawal_credential`.
#[inline]
pub fn is_compounding_withdrawal_credential(withdrawal_credentials: &Root) -> bool {
    withdrawal_credentials.as_array()[0] == COMPOUNDING_WITHDRAWAL_PREFIX
}

/// Spec `has_eth1_withdrawal_credential` (Capella `0x01` prefix).
#[inline]
pub fn has_eth1_withdrawal_credential(validator: &Validator) -> bool {
    validator.withdrawal_credentials.as_array()[0] == ETH1_ADDRESS_WITHDRAWAL_PREFIX
}

/// Spec `has_compounding_withdrawal_credential`.
#[inline]
pub fn has_compounding_withdrawal_credential(validator: &Validator) -> bool {
    is_compounding_withdrawal_credential(&validator.withdrawal_credentials)
}

/// Spec `has_execution_withdrawal_credential` (0x01 or 0x02).
#[inline]
pub fn has_execution_withdrawal_credential(validator: &Validator) -> bool {
    has_eth1_withdrawal_credential(validator) || has_compounding_withdrawal_credential(validator)
}

/// Spec `get_max_effective_balance`.
#[inline]
pub fn get_max_effective_balance(validator: &Validator) -> Gwei {
    if has_compounding_withdrawal_credential(validator) {
        MAX_EFFECTIVE_BALANCE_ELECTRA
    } else {
        MIN_ACTIVATION_BALANCE
    }
}

/// Spec `is_fully_withdrawable_validator` (Electra).
#[inline]
pub fn is_fully_withdrawable_validator(
    validator: &Validator,
    balance: Gwei,
    epoch: Epoch,
) -> bool {
    has_execution_withdrawal_credential(validator)
        && validator.withdrawable_epoch.as_u64() <= epoch.as_u64()
        && balance.as_u64() > 0
}

/// Spec `is_partially_withdrawable_validator` (Electra).
#[inline]
pub fn is_partially_withdrawable_validator(validator: &Validator, balance: Gwei) -> bool {
    let max_effective_balance = get_max_effective_balance(validator);
    let has_max_effective_balance = validator.effective_balance == max_effective_balance;
    let has_excess_balance = balance.as_u64() > max_effective_balance.as_u64();
    has_execution_withdrawal_credential(validator)
        && has_max_effective_balance
        && has_excess_balance
}

/// Spec `is_active_validator`.
#[inline]
pub fn is_active_validator(validator: &Validator, epoch: Epoch) -> bool {
    validator.activation_epoch.as_u64() <= epoch.as_u64()
        && epoch.as_u64() < validator.exit_epoch.as_u64()
}

/// Spec `is_slashable_validator`.
#[inline]
pub fn is_slashable_validator(validator: &Validator, epoch: Epoch) -> bool {
    !validator.slashed
        && validator.activation_epoch.as_u64() <= epoch.as_u64()
        && epoch.as_u64() < validator.withdrawable_epoch.as_u64()
}

/// Spec `is_slashable_attestation_data` (double vote or surround vote).
#[inline]
pub fn is_slashable_attestation_data(data_1: &AttestationData, data_2: &AttestationData) -> bool {
    // Double vote
    (data_1 != data_2 && data_1.target.epoch == data_2.target.epoch)
        // Surround vote
        || (data_1.source.epoch.as_u64() < data_2.source.epoch.as_u64()
            && data_2.target.epoch.as_u64() < data_1.target.epoch.as_u64())
}

/// Altair `has_flag`.
#[inline]
pub fn has_flag(flags: u8, flag_index: usize) -> bool {
    let flag = 1u8 << flag_index;
    flags & flag == flag
}

/// Altair `add_flag`.
#[inline]
pub fn add_flag(flags: u8, flag_index: usize) -> u8 {
    let flag = 1u8 << flag_index;
    flags | flag
}
