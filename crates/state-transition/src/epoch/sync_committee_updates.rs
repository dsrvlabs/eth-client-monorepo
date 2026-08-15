//! Spec `process_sync_committee_updates` (Altair).

use cc_types::BeaconState;
use cc_types::preset::Preset;
use cc_types::primitives::Epoch;

use crate::error::EpochError;
use crate::helpers::accessors::get_current_epoch;
use crate::shuffling::get_next_sync_committee;

use super::block_to_epoch;

/// Spec `process_sync_committee_updates`.
///
/// At sync-committee period boundaries: current ← next, next ← freshly
/// computed committee for the following period.
pub fn process_sync_committee_updates<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    let next_epoch = Epoch::new(get_current_epoch(state).as_u64().saturating_add(1));
    if next_epoch
        .as_u64()
        .is_multiple_of(P::EPOCHS_PER_SYNC_COMMITTEE_PERIOD)
    {
        let new_next = get_next_sync_committee(state).map_err(block_to_epoch)?;
        let current = state.next_sync_committee().clone();
        state.set_current_sync_committee(current);
        state.set_next_sync_committee(new_next);
    }
    Ok(())
}
