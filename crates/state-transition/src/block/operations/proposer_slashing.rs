//! Spec `process_proposer_slashing`.

use cc_crypto::{compute_signing_root, verify, DOMAIN_BEACON_PROPOSER};
use cc_types::operations::ProposerSlashing;
use cc_types::preset::Preset;
use cc_types::BeaconState;

use crate::error::{BlockError, OperationError};
use crate::helpers::accessors::{get_current_epoch, state_get_domain};
use crate::helpers::misc::compute_epoch_at_slot;
use crate::helpers::mutators::slash_validator;
use crate::helpers::predicates::is_slashable_validator;
use crate::signatures::{decode_signature, decode_state_pubkey};

fn invalid(detail: impl Into<String>) -> BlockError {
    BlockError::InvalidOperation(OperationError::Invalid {
        op: "proposer_slashing",
        detail: detail.into(),
    })
}

/// Spec `process_proposer_slashing`.
pub fn process_proposer_slashing<P: Preset>(
    state: &mut BeaconState<P>,
    proposer_slashing: &ProposerSlashing,
    verify_signatures: bool,
) -> Result<(), BlockError> {
    let header_1 = &proposer_slashing.signed_header_1.message;
    let header_2 = &proposer_slashing.signed_header_2.message;

    if header_1.slot != header_2.slot {
        return Err(invalid("header slots do not match"));
    }
    if header_1.proposer_index != header_2.proposer_index {
        return Err(invalid("header proposer indices do not match"));
    }
    if header_1 == header_2 {
        return Err(invalid("headers are identical"));
    }

    let proposer_index = header_1.proposer_index;
    let proposer = state
        .validators_get(proposer_index.as_u64() as usize)
        .ok_or_else(|| invalid(format!("proposer index {} unknown", proposer_index.as_u64())))?;

    if !is_slashable_validator(proposer, get_current_epoch(state)) {
        return Err(invalid("proposer is not slashable"));
    }

    if verify_signatures {
        let pubkey = decode_state_pubkey(&proposer.pubkey)?;
        for signed_header in [
            &proposer_slashing.signed_header_1,
            &proposer_slashing.signed_header_2,
        ] {
            let epoch = compute_epoch_at_slot::<P>(signed_header.message.slot);
            let domain = state_get_domain(state, DOMAIN_BEACON_PROPOSER, Some(epoch));
            let message = *compute_signing_root(&signed_header.message, domain).as_array();
            let signature = decode_signature(&signed_header.signature)?;
            if !verify(&pubkey, &message, &signature) {
                return Err(invalid("invalid proposer slashing signature"));
            }
        }
    }

    slash_validator(state, proposer_index, None)
}
