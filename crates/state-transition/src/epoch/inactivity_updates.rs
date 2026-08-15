//! Spec `process_inactivity_updates` (Altair+).
//!
//! Dirties the entire `inactivity_scores` list (§3.3). Skipped in genesis epoch.

use std::collections::HashSet;

use cc_types::BeaconState;
use cc_types::preset::Preset;
use cc_types::primitives::ValidatorIndex;

use crate::error::EpochError;
use crate::helpers::accessors::{
    get_current_epoch, get_eligible_validator_indices, get_previous_epoch,
    get_unslashed_participating_indices, is_in_inactivity_leak,
};
use crate::helpers::constants::{
    GENESIS_EPOCH, INACTIVITY_SCORE_BIAS, INACTIVITY_SCORE_RECOVERY_RATE, TIMELY_TARGET_FLAG_INDEX,
};

use super::block_to_epoch;

/// Spec `process_inactivity_updates`.
pub fn process_inactivity_updates<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    // Skip the genesis epoch — scores are based on previous-epoch participation.
    if get_current_epoch(state) == GENESIS_EPOCH {
        return Ok(());
    }

    let previous_epoch = get_previous_epoch(state);
    let matching_target: HashSet<ValidatorIndex> =
        get_unslashed_participating_indices(state, TIMELY_TARGET_FLAG_INDEX, previous_epoch)
            .map_err(block_to_epoch)?
            .into_iter()
            .collect();

    let eligible = get_eligible_validator_indices(state);
    let not_leaking = !is_in_inactivity_leak(state);

    for index in eligible {
        let i = index.as_u64() as usize;
        let mut score = state
            .inactivity_scores_get(i)
            .ok_or(EpochError::ArithmeticOverflow)?;

        if matching_target.contains(&index) {
            // Decrease by min(1, score).
            score = score.saturating_sub(1);
        } else {
            score = score
                .checked_add(INACTIVITY_SCORE_BIAS)
                .ok_or(EpochError::ArithmeticOverflow)?;
        }

        if not_leaking {
            score = score.saturating_sub(INACTIVITY_SCORE_RECOVERY_RATE.min(score));
        }

        state
            .inactivity_scores_set(i, score)
            .map_err(EpochError::from)?;
    }

    Ok(())
}
