//! Spec accessors over [`BeaconState`] (Architecture §5.1 / §5.4).
//!
//! Fulu EIP-7917: `get_beacon_proposer_index` is a `proposer_lookahead` read.

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, ValidatorIndex};
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
