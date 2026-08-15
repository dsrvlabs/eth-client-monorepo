//! Spec `process_voluntary_exit` (Electra).

use cc_crypto::{DOMAIN_VOLUNTARY_EXIT, compute_domain, compute_signing_root, verify};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::operations::SignedVoluntaryExit;
use cc_types::preset::Preset;

use crate::error::{BlockError, OperationError};
use crate::helpers::accessors::{get_current_epoch, get_pending_balance_to_withdraw};
use crate::helpers::constants::{FAR_FUTURE_EPOCH, network};
use crate::helpers::mutators::initiate_validator_exit;
use crate::helpers::predicates::is_active_validator;
use crate::signatures::{decode_signature, decode_state_pubkey};

fn invalid(detail: impl Into<String>) -> BlockError {
    BlockError::InvalidOperation(OperationError::Invalid {
        op: "voluntary_exit",
        detail: detail.into(),
    })
}

/// Spec `process_voluntary_exit` (Electra).
///
/// Signature domain uses **Capella** fork version (not current).
pub fn process_voluntary_exit<P: Preset>(
    state: &mut BeaconState<P>,
    signed_voluntary_exit: &SignedVoluntaryExit,
    config: &ChainConfig,
    verify_signatures: bool,
) -> Result<(), BlockError> {
    let voluntary_exit = &signed_voluntary_exit.message;
    let index = voluntary_exit.validator_index;
    let validator = state
        .validators_get(index.as_u64() as usize)
        .ok_or_else(|| invalid(format!("validator index {} out of range", index.as_u64())))?;

    let current_epoch = get_current_epoch(state);
    if !is_active_validator(validator, current_epoch) {
        return Err(invalid("validator is not active"));
    }
    if validator.exit_epoch != FAR_FUTURE_EPOCH {
        return Err(invalid("exit already initiated"));
    }
    if current_epoch.as_u64() < voluntary_exit.epoch.as_u64() {
        return Err(invalid("voluntary exit epoch is in the future"));
    }
    let min_active = validator
        .activation_epoch
        .as_u64()
        .saturating_add(network::shard_committee_period::<P>().as_u64());
    if current_epoch.as_u64() < min_active {
        return Err(invalid("validator has not been active long enough"));
    }
    // Electra: no pending withdrawals.
    if get_pending_balance_to_withdraw(state, index).as_u64() != 0 {
        return Err(invalid("validator has pending withdrawals"));
    }

    if verify_signatures {
        // Capella fork version domain (classic transcription trap: not current).
        let domain = compute_domain(
            DOMAIN_VOLUNTARY_EXIT,
            Some(config.capella_fork_version),
            Some(state.genesis_validators_root()),
        );
        let message = *compute_signing_root(voluntary_exit, domain).as_array();
        let pubkey = decode_state_pubkey(&validator.pubkey)?;
        let signature = decode_signature(&signed_voluntary_exit.signature)?;
        if !verify(&pubkey, &message, &signature) {
            return Err(invalid("invalid voluntary exit signature"));
        }
    }

    initiate_validator_exit(state, index)
}
