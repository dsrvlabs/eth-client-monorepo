//! Spec mutators (`increase_balance` / `decrease_balance`).

use cc_types::preset::Preset;
use cc_types::primitives::{Gwei, ValidatorIndex};
use cc_types::BeaconState;

use crate::error::BlockError;

/// Spec `increase_balance(state, index, delta)`.
pub fn increase_balance<P: Preset>(
    state: &mut BeaconState<P>,
    index: ValidatorIndex,
    delta: Gwei,
) -> Result<(), BlockError> {
    let i = index.as_u64() as usize;
    let bal = state
        .balances_get(i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let new = bal
        .checked_add(delta.as_u64())
        .ok_or(BlockError::ArithmeticOverflow)?;
    state.balances_set(i, new)?;
    Ok(())
}

/// Spec `decrease_balance(state, index, delta)` — saturating at zero.
pub fn decrease_balance<P: Preset>(
    state: &mut BeaconState<P>,
    index: ValidatorIndex,
    delta: Gwei,
) -> Result<(), BlockError> {
    let i = index.as_u64() as usize;
    let bal = state
        .balances_get(i)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let new = bal.saturating_sub(delta.as_u64());
    state.balances_set(i, new)?;
    Ok(())
}
