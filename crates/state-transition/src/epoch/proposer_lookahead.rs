//! Spec `process_proposer_lookahead` (Fulu EIP-7917).
//!
//! **Sole writer** of `BeaconState.proposer_lookahead` in the transition.

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, ValidatorIndex};
use cc_types::BeaconState;

use crate::error::EpochError;
use crate::helpers::accessors::get_current_epoch;
use crate::shuffling::get_beacon_proposer_indices;

use super::block_to_epoch;

/// Spec `process_proposer_lookahead`.
///
/// Shift the lookahead left by `SLOTS_PER_EPOCH` and append
/// `get_beacon_proposer_indices(state, current_epoch + MIN_SEED_LOOKAHEAD + 1)`.
pub fn process_proposer_lookahead<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    let len = state.proposer_lookahead_len();
    let slots_per_epoch = P::SLOTS_PER_EPOCH as usize;
    if len < slots_per_epoch {
        return Err(EpochError::ArithmeticOverflow);
    }
    let last_epoch_start = len - slots_per_epoch;

    // Snapshot full vector, then write shifted + new tail.
    let mut shifted: Vec<ValidatorIndex> = Vec::with_capacity(len);
    for i in slots_per_epoch..len {
        let v = state
            .proposer_lookahead_get(i)
            .ok_or(EpochError::ArithmeticOverflow)?;
        shifted.push(v);
    }

    let fill_epoch = Epoch::new(
        get_current_epoch(state)
            .as_u64()
            .saturating_add(P::MIN_SEED_LOOKAHEAD)
            .saturating_add(1),
    );
    let last_epoch_proposers =
        get_beacon_proposer_indices(state, fill_epoch).map_err(block_to_epoch)?;
    if last_epoch_proposers.len() != slots_per_epoch {
        return Err(EpochError::ArithmeticOverflow);
    }
    shifted.extend_from_slice(&last_epoch_proposers);

    debug_assert_eq!(shifted.len(), len);
    for (i, v) in shifted.into_iter().enumerate() {
        // Indices below last_epoch_start come from the shift; the rest are new.
        let _ = last_epoch_start;
        state
            .proposer_lookahead_set(i, v)
            .map_err(EpochError::from)?;
    }
    Ok(())
}
