//! Spec `process_eth1_data_reset`.

use cc_types::preset::Preset;
use cc_types::BeaconState;

use crate::error::EpochError;
use crate::helpers::accessors::get_current_epoch;

/// Spec `process_eth1_data_reset`.
pub fn process_eth1_data_reset<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    let next_epoch = get_current_epoch(state).as_u64().saturating_add(1);
    if next_epoch.is_multiple_of(P::EPOCHS_PER_ETH1_VOTING_PERIOD) {
        state.eth1_data_votes_clear();
    }
    Ok(())
}
