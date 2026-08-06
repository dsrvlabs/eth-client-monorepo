//! Epoch processing assembly (Architecture §5.1).
//!
//! Handler bodies land in CC-13a–c; `process_epoch` is wired here so
//! `process_slots` can call it at epoch boundaries. Until CC-13d fills the
//! flat call list, this returns [`EpochError::NotYetImplemented`].

use cc_types::preset::Preset;
use cc_types::BeaconState;

use crate::error::EpochError;

/// Spec `process_epoch` — public for `epoch_processing` vectors and
/// `compute_pulled_up_tip` (CC-15b).
///
/// Assembly (fifteen calls in spec order, no conditionals) lands at CC-13d.
pub fn process_epoch<P: Preset>(_state: &mut BeaconState<P>) -> Result<(), EpochError> {
    Err(EpochError::NotYetImplemented("process_epoch"))
}
