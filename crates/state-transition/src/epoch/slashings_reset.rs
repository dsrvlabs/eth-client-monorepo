//! Spec `process_slashings_reset`.

use cc_types::BeaconState;
use cc_types::preset::Preset;
use cc_types::primitives::Gwei;

use crate::error::EpochError;
use crate::helpers::accessors::get_current_epoch;

/// Spec `process_slashings_reset`.
pub fn process_slashings_reset<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    let next_epoch = get_current_epoch(state).as_u64().saturating_add(1);
    let i = (next_epoch % P::EPOCHS_PER_SLASHINGS_VECTOR) as usize;
    state
        .slashings_set(i, Gwei::new(0))
        .map_err(EpochError::from)?;
    Ok(())
}
