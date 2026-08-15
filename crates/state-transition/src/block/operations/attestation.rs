//! Spec `process_attestation` (Electra / EIP-7549).

use cc_types::BeaconState;
use cc_types::operations::Attestation;
use cc_types::preset::Preset;
use cc_types::primitives::{Gwei, ValidatorIndex};

use crate::error::{BlockError, OperationError};
use crate::helpers::accessors::{
    get_attestation_participation_flag_indices, get_attesting_indices, get_base_reward,
    get_beacon_committee, get_beacon_proposer_index, get_committee_count_per_slot,
    get_committee_indices, get_current_epoch, get_indexed_attestation, get_previous_epoch,
    is_valid_indexed_attestation,
};
use crate::helpers::constants::{
    MIN_ATTESTATION_INCLUSION_DELAY, PARTICIPATION_FLAG_WEIGHTS, PROPOSER_WEIGHT,
    WEIGHT_DENOMINATOR,
};
use crate::helpers::misc::compute_epoch_at_slot;
use crate::helpers::mutators::increase_balance;
use crate::helpers::predicates::{add_flag, has_flag};

fn invalid(detail: impl Into<String>) -> BlockError {
    BlockError::InvalidOperation(OperationError::Invalid {
        op: "attestation",
        detail: detail.into(),
    })
}

/// Options for [`process_attestation`].
#[derive(Debug, Clone, Copy)]
pub struct ProcessAttestationOpts {
    /// When true, verify the aggregate attestation signature.
    pub verify_signatures: bool,
}

impl Default for ProcessAttestationOpts {
    fn default() -> Self {
        Self {
            verify_signatures: true,
        }
    }
}

/// Spec `process_attestation` (Electra).
pub fn process_attestation<P: Preset>(
    state: &mut BeaconState<P>,
    attestation: &Attestation<P>,
    opts: ProcessAttestationOpts,
) -> Result<(), BlockError> {
    let data = &attestation.data;
    let current_epoch = get_current_epoch(state);
    let previous_epoch = get_previous_epoch(state);

    if data.target.epoch != current_epoch && data.target.epoch != previous_epoch {
        return Err(invalid("target epoch is not current or previous"));
    }
    if data.target.epoch != compute_epoch_at_slot::<P>(data.slot) {
        return Err(invalid("target epoch does not match attestation slot"));
    }
    if state.slot().as_u64()
        < data
            .slot
            .as_u64()
            .saturating_add(MIN_ATTESTATION_INCLUSION_DELAY)
    {
        return Err(invalid("attestation included before min inclusion delay"));
    }

    // Electra: data.index must be zero; committees selected via committee_bits.
    if data.index.as_u64() != 0 {
        return Err(invalid("attestation data.index must be zero post-Electra"));
    }

    let committee_indices = get_committee_indices::<P>(&attestation.committee_bits)?;
    let committees_per_slot = get_committee_count_per_slot(state, data.target.epoch);
    let mut committee_offset = 0usize;
    for committee_index in &committee_indices {
        if committee_index.as_u64() >= committees_per_slot {
            return Err(invalid(format!(
                "committee index {} >= committees_per_slot {committees_per_slot}",
                committee_index.as_u64()
            )));
        }
        let committee = get_beacon_committee(state, data.slot, *committee_index)?;
        let mut committee_attesters = 0usize;
        for i in 0..committee.len() {
            let bit_i = committee_offset + i;
            if bit_i >= attestation.aggregation_bits.len() {
                return Err(invalid(
                    "aggregation_bits shorter than selected committee union",
                ));
            }
            let bit = attestation
                .aggregation_bits
                .get(bit_i)
                .map_err(|_| BlockError::ArithmeticOverflow)?;
            if bit {
                committee_attesters += 1;
            }
        }
        if committee_attesters == 0 {
            return Err(invalid(format!(
                "no attesters in committee {}",
                committee_index.as_u64()
            )));
        }
        committee_offset += committee.len();
    }

    if attestation.aggregation_bits.len() != committee_offset {
        return Err(invalid(format!(
            "aggregation_bits len {} != committee union len {committee_offset}",
            attestation.aggregation_bits.len()
        )));
    }

    let inclusion_delay = state.slot().as_u64().saturating_sub(data.slot.as_u64());
    let participation_flag_indices =
        get_attestation_participation_flag_indices(state, data, inclusion_delay)?;

    let indexed = get_indexed_attestation(state, attestation)?;
    if !is_valid_indexed_attestation(state, &indexed, opts.verify_signatures)? {
        return Err(invalid("invalid indexed attestation / signature"));
    }

    // Participation flag updates + proposer reward.
    let current = data.target.epoch == current_epoch;
    let attesting = get_attesting_indices(state, attestation)?;
    let mut proposer_reward_numerator = 0u64;

    for index in attesting {
        let i = index.as_u64() as usize;
        let flags = if current {
            state
                .current_epoch_participation_get(i)
                .ok_or(BlockError::ArithmeticOverflow)?
        } else {
            state
                .previous_epoch_participation_get(i)
                .ok_or(BlockError::ArithmeticOverflow)?
        };
        let mut new_flags = flags;
        let base = get_base_reward(state, index)?;
        for (flag_index, weight) in PARTICIPATION_FLAG_WEIGHTS.iter().enumerate() {
            if participation_flag_indices.contains(&flag_index) && !has_flag(flags, flag_index) {
                new_flags = add_flag(new_flags, flag_index);
                proposer_reward_numerator =
                    proposer_reward_numerator.saturating_add(base.as_u64().saturating_mul(*weight));
            }
        }
        if new_flags != flags {
            if current {
                state.current_epoch_participation_set(i, new_flags)?;
            } else {
                state.previous_epoch_participation_set(i, new_flags)?;
            }
        }
    }

    let proposer_reward_denominator =
        (WEIGHT_DENOMINATOR - PROPOSER_WEIGHT) * WEIGHT_DENOMINATOR / PROPOSER_WEIGHT;
    let proposer_reward = Gwei::new(proposer_reward_numerator / proposer_reward_denominator);
    let proposer_index = get_beacon_proposer_index(state)?;
    increase_balance(state, proposer_index, proposer_reward)?;
    Ok(())
}

/// Test/export helper: resolve attesting indices over `committee_bits`.
pub fn get_attesting_indices_for_test<P: Preset>(
    state: &BeaconState<P>,
    attestation: &Attestation<P>,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    get_attesting_indices(state, attestation)
}
