//! Spec helpers used by the transition (Architecture §5.1 `helpers/`).

pub mod accessors;
pub mod constants;
pub mod misc;
pub mod mutators;
pub mod predicates;

pub use accessors::{
    compute_time_at_slot, get_beacon_proposer_index, get_current_epoch, get_randao_mix,
};
pub use misc::{
    compute_epoch_at_slot, execution_address_from_credentials, hash_signature_root,
    kzg_commitment_to_versioned_hash, xor_bytes32,
};
pub use mutators::{decrease_balance, increase_balance, initiate_validator_exit, slash_validator};
pub use predicates::{
    get_max_effective_balance, has_compounding_withdrawal_credential,
    has_eth1_withdrawal_credential, has_execution_withdrawal_credential, is_active_validator,
    is_compounding_withdrawal_credential, is_eligible_for_activation,
    is_eligible_for_activation_queue, is_fully_withdrawable_validator,
    is_partially_withdrawable_validator, is_slashable_attestation_data, is_slashable_validator,
};
