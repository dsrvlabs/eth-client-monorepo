//! Spec `process_rewards_and_penalties` and Altair+ delta helpers (CC-13b).
//!
//! `get_flag_index_deltas` / `get_inactivity_penalty_deltas` are public so the
//! `rewards` vector runner can compare per-validator deltas without applying them.

use std::collections::HashSet;

use cc_types::preset::Preset;
use cc_types::primitives::{Gwei, ValidatorIndex};
use cc_types::BeaconState;

use crate::error::EpochError;
use crate::epoch_cache::{
    base_reward_per_increment_cached, rebuild_epoch_cache, total_active_balance_cached,
};
use super::block_to_epoch;
use crate::helpers::accessors::{
    get_base_reward, get_current_epoch, get_eligible_validator_indices, get_previous_epoch,
    get_total_balance, get_unslashed_participating_indices, is_in_inactivity_leak,
};
use crate::helpers::constants::{
    EFFECTIVE_BALANCE_INCREMENT, GENESIS_EPOCH, INACTIVITY_PENALTY_QUOTIENT_BELLATRIX,
    INACTIVITY_SCORE_BIAS, PARTICIPATION_FLAG_WEIGHTS, TIMELY_HEAD_FLAG_INDEX,
    TIMELY_TARGET_FLAG_INDEX, WEIGHT_DENOMINATOR,
};
use crate::helpers::mutators::{decrease_balance, increase_balance};

/// Per-validator reward/penalty pair lists (same length as the registry).
pub type RewardPenalties = (Vec<Gwei>, Vec<Gwei>);

/// Spec `get_flag_index_deltas`.
///
/// Returns `(rewards, penalties)` vectors indexed by validator index. Uses
/// [`EpochCache`](cc_types::EpochCache) for `total_active_balance` /
/// `base_reward_per_increment` when valid.
pub fn get_flag_index_deltas<P: Preset>(
    state: &BeaconState<P>,
    flag_index: usize,
) -> Result<RewardPenalties, EpochError> {
    let n = state.validators_len();
    let mut rewards = vec![Gwei::new(0); n];
    let mut penalties = vec![Gwei::new(0); n];

    let previous_epoch = get_previous_epoch(state);
    let unslashed_participating_list =
        get_unslashed_participating_indices(state, flag_index, previous_epoch)
            .map_err(block_to_epoch)?;
    let unslashed_participating: HashSet<ValidatorIndex> =
        unslashed_participating_list.iter().copied().collect();

    let weight = PARTICIPATION_FLAG_WEIGHTS
        .get(flag_index)
        .copied()
        .ok_or(EpochError::ArithmeticOverflow)?;

    let unslashed_participating_balance =
        get_total_balance(state, &unslashed_participating_list).map_err(block_to_epoch)?;
    let unslashed_participating_increments =
        unslashed_participating_balance.as_u64() / EFFECTIVE_BALANCE_INCREMENT.as_u64();

    // Prefer epoch cache (filled once at epoch boundary).
    let total_active = total_active_balance_cached(state).map_err(block_to_epoch)?;
    let active_increments = total_active.as_u64() / EFFECTIVE_BALANCE_INCREMENT.as_u64();
    // Touch base_reward_per_increment_cached so cache instrumentation is consistent
    // when the cache is warm (get_base_reward also reads it).
    let _ = base_reward_per_increment_cached(state).map_err(block_to_epoch)?;

    let in_leak = is_in_inactivity_leak(state);

    for index in get_eligible_validator_indices(state) {
        let i = index.as_u64() as usize;
        let base_reward = get_base_reward(state, index).map_err(block_to_epoch)?;

        if unslashed_participating.contains(&index) {
            if !in_leak {
                // reward_numerator = base_reward * weight * unslashed_participating_increments
                // rewards += numerator // (active_increments * WEIGHT_DENOMINATOR)
                let numerator = base_reward
                    .as_u64()
                    .checked_mul(weight)
                    .and_then(|v| v.checked_mul(unslashed_participating_increments))
                    .ok_or(EpochError::ArithmeticOverflow)?;
                let denominator = active_increments
                    .checked_mul(WEIGHT_DENOMINATOR)
                    .ok_or(EpochError::ArithmeticOverflow)?;
                if denominator == 0 {
                    return Err(EpochError::ArithmeticOverflow);
                }
                let delta = numerator / denominator;
                rewards[i] = Gwei::new(
                    rewards[i]
                        .as_u64()
                        .checked_add(delta)
                        .ok_or(EpochError::ArithmeticOverflow)?,
                );
            }
        } else if flag_index != TIMELY_HEAD_FLAG_INDEX {
            let delta = base_reward
                .as_u64()
                .checked_mul(weight)
                .ok_or(EpochError::ArithmeticOverflow)?
                / WEIGHT_DENOMINATOR;
            penalties[i] = Gwei::new(
                penalties[i]
                    .as_u64()
                    .checked_add(delta)
                    .ok_or(EpochError::ArithmeticOverflow)?,
            );
        }
    }

    Ok((rewards, penalties))
}

/// Spec `get_inactivity_penalty_deltas` (Bellatrix+ quotient).
pub fn get_inactivity_penalty_deltas<P: Preset>(
    state: &BeaconState<P>,
) -> Result<RewardPenalties, EpochError> {
    let n = state.validators_len();
    let rewards = vec![Gwei::new(0); n];
    let mut penalties = vec![Gwei::new(0); n];

    let previous_epoch = get_previous_epoch(state);
    let matching_target: HashSet<ValidatorIndex> = get_unslashed_participating_indices(
        state,
        TIMELY_TARGET_FLAG_INDEX,
        previous_epoch,
    )
    .map_err(block_to_epoch)?
    .into_iter()
    .collect();

    let penalty_denominator = INACTIVITY_SCORE_BIAS
        .checked_mul(INACTIVITY_PENALTY_QUOTIENT_BELLATRIX)
        .ok_or(EpochError::ArithmeticOverflow)?;

    for index in get_eligible_validator_indices(state) {
        if matching_target.contains(&index) {
            continue;
        }
        let i = index.as_u64() as usize;
        let v = state
            .validators_get(i)
            .ok_or(EpochError::ArithmeticOverflow)?;
        let score = state
            .inactivity_scores_get(i)
            .ok_or(EpochError::ArithmeticOverflow)?;
        let penalty_numerator = v
            .effective_balance
            .as_u64()
            .checked_mul(score)
            .ok_or(EpochError::ArithmeticOverflow)?;
        let delta = penalty_numerator / penalty_denominator;
        penalties[i] = Gwei::new(
            penalties[i]
                .as_u64()
                .checked_add(delta)
                .ok_or(EpochError::ArithmeticOverflow)?,
        );
    }

    Ok((rewards, penalties))
}

/// Spec `process_rewards_and_penalties`.
///
/// Rebuilds [`EpochCache`](cc_types::EpochCache) once, then accumulates flag +
/// inactivity deltas and applies them via saturating balance mutators.
/// Skipped in the genesis epoch.
pub fn process_rewards_and_penalties<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    if get_current_epoch(state) == GENESIS_EPOCH {
        return Ok(());
    }

    // Fill epoch cache once for total_active_balance / base_reward_per_increment.
    rebuild_epoch_cache(state).map_err(block_to_epoch)?;

    let mut all_deltas: Vec<RewardPenalties> = Vec::with_capacity(PARTICIPATION_FLAG_WEIGHTS.len() + 1);
    for flag_index in 0..PARTICIPATION_FLAG_WEIGHTS.len() {
        all_deltas.push(get_flag_index_deltas(state, flag_index)?);
    }
    all_deltas.push(get_inactivity_penalty_deltas(state)?);

    let n = state.validators_len();
    for (rewards, penalties) in all_deltas {
        for index in 0..n {
            let vi = ValidatorIndex::new(index as u64);
            increase_balance(state, vi, rewards[index]).map_err(block_to_epoch)?;
            // decrease_balance saturates at zero (penalty > balance → 0, no panic).
            decrease_balance(state, vi, penalties[index]).map_err(block_to_epoch)?;
        }
    }

    Ok(())
}
