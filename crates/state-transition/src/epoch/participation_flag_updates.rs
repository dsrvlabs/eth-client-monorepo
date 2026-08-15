//! Spec `process_participation_flag_updates` (Altair).

use cc_types::BeaconState;
use cc_types::preset::Preset;

use crate::error::EpochError;

/// Spec `process_participation_flag_updates`.
///
/// Rotates current → previous and zeroes current. Dirties both participation
/// list-hash caches (Architecture §3.3 / §3.4).
pub fn process_participation_flag_updates<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    state.participation_flag_rotate();
    Ok(())
}
