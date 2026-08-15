//! Spec `process_eth1_data` (phase0).

use cc_types::preset::Preset;
use cc_types::{BeaconBlock, BeaconState};

use crate::error::BlockError;

/// Spec `process_eth1_data`.
///
/// Appends the block's eth1 vote; on supermajority
/// `count * 2 > EPOCHS_PER_ETH1_VOTING_PERIOD * SLOTS_PER_EPOCH` replaces
/// `state.eth1_data`.
pub fn process_eth1_data<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
) -> Result<(), BlockError> {
    let vote = block.body.eth1_data;
    state.eth1_data_votes_push(vote)?;

    let threshold = P::EPOCHS_PER_ETH1_VOTING_PERIOD
        .checked_mul(P::SLOTS_PER_EPOCH)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let count = state.eth1_data_votes_count(&vote) as u64;
    // Spec: `count * 2 > EPOCHS_PER_ETH1_VOTING_PERIOD * SLOTS_PER_EPOCH`
    if count.checked_mul(2).ok_or(BlockError::ArithmeticOverflow)? > threshold {
        state.set_eth1_data(vote);
    }
    Ok(())
}
