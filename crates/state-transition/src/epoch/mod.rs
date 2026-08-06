//! Epoch processing assembly (Architecture §5.1).
//!
//! Handler bodies land in CC-13a–c; `process_epoch` is wired here so
//! `process_slots` can call it at epoch boundaries. Until CC-13d fills the
//! flat call list, this returns [`EpochError::NotYetImplemented`].

pub mod inactivity_updates;
pub mod justification_and_finalization;
pub mod rewards_and_penalties;

use cc_types::preset::Preset;
use cc_types::BeaconState;

use crate::error::EpochError;

pub use inactivity_updates::process_inactivity_updates;
pub use justification_and_finalization::{
    process_justification_and_finalization, weigh_justification_and_finalization,
};
pub use rewards_and_penalties::{
    get_flag_index_deltas, get_inactivity_penalty_deltas, process_rewards_and_penalties,
    RewardPenalties,
};

/// Spec `process_epoch` — public for `epoch_processing` vectors and
/// `compute_pulled_up_tip` (CC-15b).
///
/// Assembly (fifteen calls in spec order, no conditionals) lands at CC-13d.
pub fn process_epoch<P: Preset>(_state: &mut BeaconState<P>) -> Result<(), EpochError> {
    Err(EpochError::NotYetImplemented("process_epoch"))
}
