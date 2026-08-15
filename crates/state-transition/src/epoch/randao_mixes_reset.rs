//! Spec `process_randao_mixes_reset`.

use cc_types::BeaconState;
use cc_types::preset::Preset;
use cc_types::primitives::Epoch;

use crate::error::EpochError;
use crate::helpers::accessors::{get_current_epoch, get_randao_mix};

use super::block_to_epoch;

/// Spec `process_randao_mixes_reset`.
pub fn process_randao_mixes_reset<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    let current_epoch = get_current_epoch(state);
    let next_epoch = Epoch::new(current_epoch.as_u64().saturating_add(1));
    let mix = get_randao_mix(state, current_epoch).map_err(block_to_epoch)?;
    let i = (next_epoch.as_u64() % P::EPOCHS_PER_HISTORICAL_VECTOR) as usize;
    state.randao_mixes_set(i, mix).map_err(EpochError::from)?;
    Ok(())
}
