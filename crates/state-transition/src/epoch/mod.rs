//! Epoch processing handlers (Architecture §5.1).
//!
//! Handler bodies: CC-13a–c. `process_epoch` is the flat fifteen-call list
//! in consensus-specs order (CC-13d).

pub mod effective_balance_updates;
pub mod eth1_data_reset;
pub mod historical_summaries_update;
pub mod inactivity_updates;
pub mod justification_and_finalization;
pub mod participation_flag_updates;
pub mod pending_consolidations;
pub mod pending_deposits;
pub mod proposer_lookahead;
pub mod randao_mixes_reset;
pub mod registry_updates;
pub mod rewards_and_penalties;
pub mod slashings;
pub mod slashings_reset;
pub mod sync_committee_updates;

use cc_types::BeaconState;
use cc_types::preset::Preset;

use crate::error::{BlockError, EpochError};

pub use effective_balance_updates::process_effective_balance_updates;
pub use eth1_data_reset::process_eth1_data_reset;
pub use historical_summaries_update::process_historical_summaries_update;
pub use inactivity_updates::process_inactivity_updates;
pub use justification_and_finalization::{
    process_justification_and_finalization, weigh_justification_and_finalization,
};
pub use participation_flag_updates::process_participation_flag_updates;
pub use pending_consolidations::process_pending_consolidations;
pub use pending_deposits::{apply_pending_deposit, process_pending_deposits};
pub use proposer_lookahead::process_proposer_lookahead;
pub use randao_mixes_reset::process_randao_mixes_reset;
pub use registry_updates::process_registry_updates;
pub use rewards_and_penalties::{
    RewardPenalties, get_flag_index_deltas, get_inactivity_penalty_deltas,
    process_rewards_and_penalties,
};
pub use slashings::process_slashings;
pub use slashings_reset::process_slashings_reset;
pub use sync_committee_updates::process_sync_committee_updates;

/// Map common block-path errors into [`EpochError`].
pub(crate) fn block_to_epoch(err: BlockError) -> EpochError {
    match err {
        BlockError::ArithmeticOverflow => EpochError::ArithmeticOverflow,
        BlockError::StateAccess(e) => EpochError::StateAccess(e),
        _ => EpochError::ArithmeticOverflow,
    }
}

/// Spec `process_epoch` — public for `epoch_processing` vectors and
/// `compute_pulled_up_tip` (CC-15b).
///
/// Flat list of fifteen handlers in consensus-specs order; no conditionals.
/// Spec (`fulu/beacon-chain.md` / Electra epoch processing):
/// 1. process_justification_and_finalization
/// 2. process_inactivity_updates
/// 3. process_rewards_and_penalties
/// 4. process_registry_updates
/// 5. process_slashings
/// 6. process_eth1_data_reset
/// 7. process_pending_deposits
/// 8. process_pending_consolidations
/// 9. process_effective_balance_updates
/// 10. process_slashings_reset
/// 11. process_randao_mixes_reset
/// 12. process_historical_summaries_update
/// 13. process_participation_flag_updates
/// 14. process_sync_committee_updates
/// 15. process_proposer_lookahead
pub fn process_epoch<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    process_justification_and_finalization(state)?;
    process_inactivity_updates(state)?;
    process_rewards_and_penalties(state)?;
    process_registry_updates(state)?;
    process_slashings(state)?;
    process_eth1_data_reset(state)?;
    process_pending_deposits(state)?;
    process_pending_consolidations(state)?;
    process_effective_balance_updates(state)?;
    process_slashings_reset(state)?;
    process_randao_mixes_reset(state)?;
    process_historical_summaries_update(state)?;
    process_participation_flag_updates(state)?;
    process_sync_committee_updates(state)?;
    process_proposer_lookahead(state)?;
    Ok(())
}
