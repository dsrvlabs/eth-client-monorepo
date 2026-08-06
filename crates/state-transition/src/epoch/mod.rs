//! Epoch processing handlers (Architecture §5.1).
//!
//! Handler bodies: CC-13a–c. `process_epoch` assembly (fifteen calls in
//! spec order) lands at CC-13d.

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

use cc_types::preset::Preset;
use cc_types::BeaconState;

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
    get_flag_index_deltas, get_inactivity_penalty_deltas, process_rewards_and_penalties,
    RewardPenalties,
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
/// Assembly (fifteen calls in spec order, no conditionals) lands at CC-13d.
pub fn process_epoch<P: Preset>(_state: &mut BeaconState<P>) -> Result<(), EpochError> {
    Err(EpochError::NotYetImplemented("process_epoch"))
}
