//! Spec `process_attester_slashing`.

use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::operations::AttesterSlashing;
use cc_types::preset::Preset;
use cc_types::primitives::ValidatorIndex;

use crate::error::{BlockError, OperationError};
use crate::helpers::accessors::{get_current_epoch, is_valid_indexed_attestation};
use crate::helpers::mutators::slash_validator;
use crate::helpers::predicates::{is_slashable_attestation_data, is_slashable_validator};

fn invalid(detail: impl Into<String>) -> BlockError {
    BlockError::InvalidOperation(OperationError::Invalid {
        op: "attester_slashing",
        detail: detail.into(),
    })
}

/// Spec `process_attester_slashing`.
pub fn process_attester_slashing<P: Preset>(
    state: &mut BeaconState<P>,
    attester_slashing: &AttesterSlashing<P>,
    config: &ChainConfig,
    verify_signatures: bool,
) -> Result<(), BlockError> {
    let attestation_1 = &attester_slashing.attestation_1;
    let attestation_2 = &attester_slashing.attestation_2;

    if !is_slashable_attestation_data(&attestation_1.data, &attestation_2.data) {
        return Err(invalid("attestation data is not slashable"));
    }
    if !is_valid_indexed_attestation(state, attestation_1, verify_signatures)? {
        return Err(invalid("attestation_1 is not a valid indexed attestation"));
    }
    if !is_valid_indexed_attestation(state, attestation_2, verify_signatures)? {
        return Err(invalid("attestation_2 is not a valid indexed attestation"));
    }

    // Sorted-index intersection.
    let set2: std::collections::BTreeSet<u64> = attestation_2
        .attesting_indices
        .iter()
        .map(|i| i.as_u64())
        .collect();
    let mut intersection: Vec<ValidatorIndex> = attestation_1
        .attesting_indices
        .iter()
        .filter(|i| set2.contains(&i.as_u64()))
        .copied()
        .collect();
    intersection.sort_by_key(|i| i.as_u64());
    intersection.dedup();

    let epoch = get_current_epoch(state);
    let mut slashed_any = false;
    for index in intersection {
        let Some(validator) = state.validators_get(index.as_u64() as usize) else {
            // High / unknown indices make the indexed attestation invalid earlier
            // when signatures are checked; if we reach here, reject explicitly.
            return Err(invalid(format!("index {} out of range", index.as_u64())));
        };
        if is_slashable_validator(validator, epoch) {
            slash_validator(state, index, None, config)?;
            slashed_any = true;
        }
    }
    if !slashed_any {
        return Err(invalid("empty slashable intersection"));
    }
    Ok(())
}
