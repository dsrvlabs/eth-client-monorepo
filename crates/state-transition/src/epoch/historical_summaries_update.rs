//! Spec `process_historical_summaries_update` (Capella).

use cc_types::BeaconState;
use cc_types::containers::HistoricalSummary;
use cc_types::preset::Preset;
use cc_types::primitives::Epoch;

use crate::error::EpochError;
use crate::helpers::accessors::get_current_epoch;

/// Spec `process_historical_summaries_update`.
pub fn process_historical_summaries_update<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    let next_epoch = Epoch::new(get_current_epoch(state).as_u64().saturating_add(1));
    let epochs_per_historical_root = P::SLOTS_PER_HISTORICAL_ROOT / P::SLOTS_PER_EPOCH;
    if next_epoch
        .as_u64()
        .is_multiple_of(epochs_per_historical_root)
    {
        let summary = HistoricalSummary {
            block_summary_root: state.block_roots_tree_hash_root(),
            state_summary_root: state.state_roots_tree_hash_root(),
        };
        state
            .historical_summaries_push(summary)
            .map_err(EpochError::from)?;
    }
    Ok(())
}
