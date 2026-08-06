//! Spec accessors over [`BeaconState`] (Architecture §5.1 / §5.4).
//!
//! Fulu EIP-7917: `get_beacon_proposer_index` is a `proposer_lookahead` read.

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, ValidatorIndex};
use cc_types::BeaconState;

use crate::error::BlockError;
use crate::helpers::misc::compute_epoch_at_slot;

/// `get_current_epoch(state)`.
#[inline]
pub fn get_current_epoch<P: Preset>(state: &BeaconState<P>) -> Epoch {
    compute_epoch_at_slot::<P>(state.slot())
}

/// `get_beacon_proposer_index(state)` — Fulu EIP-7917.
///
/// ```text
/// state.proposer_lookahead[state.slot % SLOTS_PER_EPOCH]
/// ```
///
/// Does **not** touch the shuffling cache (Architecture §5.4).
pub fn get_beacon_proposer_index<P: Preset>(
    state: &BeaconState<P>,
) -> Result<ValidatorIndex, BlockError> {
    let offset = (state.slot().as_u64() % P::SLOTS_PER_EPOCH) as usize;
    state
        .proposer_lookahead_get(offset)
        .ok_or(BlockError::ArithmeticOverflow)
}

/// Spec `get_randao_mix(state, epoch)`.
#[inline]
pub fn get_randao_mix<P: Preset>(state: &BeaconState<P>, epoch: Epoch) -> Result<Root, BlockError> {
    let i = (epoch.as_u64() % P::EPOCHS_PER_HISTORICAL_VECTOR) as usize;
    state
        .randao_mixes_get(i)
        .ok_or(BlockError::ArithmeticOverflow)
}

/// Spec `compute_time_at_slot(state, slot)` with runtime `seconds_per_slot`.
#[inline]
pub fn compute_time_at_slot(genesis_time: u64, slot: cc_types::primitives::Slot, seconds_per_slot: u64) -> u64 {
    genesis_time.saturating_add(slot.as_u64().saturating_mul(seconds_per_slot))
}
