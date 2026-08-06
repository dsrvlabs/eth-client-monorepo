//! Spec `process_bls_to_execution_change` (Capella / Electra).

use cc_crypto::{compute_domain, compute_signing_root, hash_fixed, verify, DOMAIN_BLS_TO_EXECUTION_CHANGE};
use cc_types::config::ChainConfig;
use cc_types::operations::SignedBlsToExecutionChange;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::BeaconState;

use crate::error::{BlockError, OperationError};
use crate::helpers::constants::{BLS_WITHDRAWAL_PREFIX, ETH1_ADDRESS_WITHDRAWAL_PREFIX};
use crate::signatures::{decode_block_pubkey, decode_signature};

fn invalid(detail: impl Into<String>) -> BlockError {
    BlockError::InvalidOperation(OperationError::Invalid {
        op: "bls_to_execution_change",
        detail: detail.into(),
    })
}

/// Spec `process_bls_to_execution_change`.
///
/// Signature domain uses the **genesis** fork version (not the current one).
pub fn process_bls_to_execution_change<P: Preset>(
    state: &mut BeaconState<P>,
    signed_address_change: &SignedBlsToExecutionChange,
    config: &ChainConfig,
    verify_signatures: bool,
) -> Result<(), BlockError> {
    let address_change = &signed_address_change.message;
    let index = address_change.validator_index;
    if index.as_u64() as usize >= state.validators_len() {
        return Err(invalid(format!(
            "validator index {} out of range",
            index.as_u64()
        )));
    }

    let validator = state
        .validators_get(index.as_u64() as usize)
        .ok_or_else(|| invalid("validator missing"))?;

    let creds = validator.withdrawal_credentials.as_array();
    if creds[0] != BLS_WITHDRAWAL_PREFIX {
        return Err(invalid("withdrawal credentials are not BLS-prefixed"));
    }
    let pubkey_hash = hash_fixed(address_change.from_bls_pubkey.as_slice());
    if creds[1..] != pubkey_hash[1..] {
        return Err(invalid(
            "withdrawal credentials do not match from_bls_pubkey hash",
        ));
    }

    if verify_signatures {
        // Genesis fork version — classic transcription error is using current.
        let domain = compute_domain(
            DOMAIN_BLS_TO_EXECUTION_CHANGE,
            Some(config.genesis_fork_version),
            Some(state.genesis_validators_root()),
        );
        let message = *compute_signing_root(address_change, domain).as_array();
        // from_bls_pubkey is message-carried → block material.
        let pubkey = decode_block_pubkey(&address_change.from_bls_pubkey)?;
        let signature = decode_signature(&signed_address_change.signature)?;
        if !verify(&pubkey, &message, &signature) {
            return Err(invalid("invalid bls_to_execution_change signature"));
        }
    }

    // Rewrite credentials: 0x01 || 11 zero bytes || execution address.
    let mut new_creds = [0u8; 32];
    new_creds[0] = ETH1_ADDRESS_WITHDRAWAL_PREFIX;
    // bytes 1..12 already zero
    new_creds[12..32].copy_from_slice(address_change.to_execution_address.as_slice());

    let v = state
        .validators_get_mut(index.as_u64() as usize)
        .ok_or(BlockError::ArithmeticOverflow)?;
    v.withdrawal_credentials = Root::from_array(new_creds);
    Ok(())
}

/// Domain for BLS-to-execution-change under an explicit fork version (tests).
pub fn bls_to_execution_change_domain(
    fork_version: cc_types::primitives::ForkVersion,
    genesis_validators_root: Root,
) -> cc_types::primitives::Domain {
    compute_domain(
        DOMAIN_BLS_TO_EXECUTION_CHANGE,
        Some(fork_version),
        Some(genesis_validators_root),
    )
}
