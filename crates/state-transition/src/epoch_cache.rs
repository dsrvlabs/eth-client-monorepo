//! `EpochCache` fill / invalidate rules (Architecture §5.4).
//!
//! Holds `total_active_balance`, `base_reward_per_increment`, and active index
//! sets for current / previous / next epochs. Keyed by the state's current
//! epoch; invalidated by any registry or effective-balance change and rebuilt
//! at the epoch boundary. Primary consumer: CC-13b rewards.

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Gwei};
use cc_types::BeaconState;

use crate::error::BlockError;
use crate::helpers::accessors::{
    get_active_validator_indices, get_base_reward_per_increment, get_current_epoch,
    get_previous_epoch, get_total_balance,
};

/// Rebuild [`cc_types::EpochCache`] from `state` for the current epoch.
///
/// Idempotent: always overwrites with fresh values derived from the registry.
pub fn rebuild_epoch_cache<P: Preset>(state: &mut BeaconState<P>) -> Result<(), BlockError> {
    let current = get_current_epoch(state);
    let previous = get_previous_epoch(state);
    let next = Epoch::new(current.as_u64().saturating_add(1));

    let current_active = get_active_validator_indices(state, current);
    let previous_active = get_active_validator_indices(state, previous);
    let next_active = get_active_validator_indices(state, next);

    let total_active_balance = get_total_balance(state, &current_active)?;
    // Base reward per increment matches the accessor (uses current total active balance).
    let base_reward_per_increment = get_base_reward_per_increment(state)?;

    let cache = &mut state.caches_mut().epoch;
    cache.epoch = Some(current);
    cache.total_active_balance = Some(total_active_balance.as_u64());
    cache.base_reward_per_increment = Some(base_reward_per_increment.as_u64());
    cache.current_active_indices = Some(current_active);
    cache.previous_active_indices = Some(previous_active);
    cache.next_active_indices = Some(next_active);
    cache.note_rebuild();
    Ok(())
}

/// Invalidate the epoch cache after a registry or effective-balance change.
pub fn invalidate_epoch_cache<P: Preset>(state: &mut BeaconState<P>) {
    state.caches_mut().epoch.invalidate();
}

/// Note a registry / effective-balance mutation: drop cached epoch values.
///
/// Call sites: deposit processing, effective-balance updates, exits that change
/// the active set. Epoch handlers rebuild via [`rebuild_epoch_cache`].
pub fn note_registry_or_effective_balance_change<P: Preset>(state: &mut BeaconState<P>) {
    invalidate_epoch_cache(state);
}

/// Read total active balance from the cache when valid for the current epoch;
/// otherwise compute (without filling the cache).
pub fn total_active_balance_cached<P: Preset>(
    state: &BeaconState<P>,
) -> Result<Gwei, BlockError> {
    let current = get_current_epoch(state);
    if let Some(epoch) = state.caches().epoch.epoch
        && epoch == current
        && let Some(bal) = state.caches().epoch.total_active_balance
    {
        return Ok(Gwei::new(bal));
    }
    let indices = get_active_validator_indices(state, current);
    get_total_balance(state, &indices)
}

/// Read base reward per increment from the cache when valid; else compute.
pub fn base_reward_per_increment_cached<P: Preset>(
    state: &BeaconState<P>,
) -> Result<Gwei, BlockError> {
    let current = get_current_epoch(state);
    if let Some(epoch) = state.caches().epoch.epoch
        && epoch == current
        && let Some(v) = state.caches().epoch.base_reward_per_increment
    {
        return Ok(Gwei::new(v));
    }
    get_base_reward_per_increment(state)
}
