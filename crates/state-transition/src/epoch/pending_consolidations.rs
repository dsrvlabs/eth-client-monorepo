//! Spec `process_pending_consolidations` (Electra).

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Gwei};
use cc_types::BeaconState;

use crate::error::EpochError;
use crate::helpers::accessors::get_current_epoch;
use crate::helpers::mutators::{decrease_balance, increase_balance};

use super::block_to_epoch;

/// Spec `process_pending_consolidations`.
///
/// Drains consolidations whose source is withdrawable (or slashed → skip).
/// Moves the **effective** balance (capped by the current balance), not the
/// full balance; excess remains on the source for withdrawal.
pub fn process_pending_consolidations<P: Preset>(
    state: &mut BeaconState<P>,
) -> Result<(), EpochError> {
    let next_epoch = Epoch::new(get_current_epoch(state).as_u64().saturating_add(1));
    let mut next_pending_consolidation = 0usize;

    let queue_len = state.pending_consolidations_len();
    for i in 0..queue_len {
        let (source_index, target_index) = {
            let pc = state
                .pending_consolidations_get(i)
                .ok_or(EpochError::ArithmeticOverflow)?;
            (pc.source_index, pc.target_index)
        };
        let source_i = source_index.as_u64() as usize;

        let (slashed, withdrawable_epoch, effective_balance) = {
            let v = state
                .validators_get(source_i)
                .ok_or(EpochError::ArithmeticOverflow)?;
            (v.slashed, v.withdrawable_epoch, v.effective_balance)
        };

        if slashed {
            next_pending_consolidation = next_pending_consolidation
                .checked_add(1)
                .ok_or(EpochError::ArithmeticOverflow)?;
            continue;
        }
        if withdrawable_epoch.as_u64() > next_epoch.as_u64() {
            break;
        }

        let balance = state
            .balances_get(source_i)
            .ok_or(EpochError::ArithmeticOverflow)?;
        let source_effective_balance = Gwei::new(balance.as_u64().min(effective_balance.as_u64()));

        decrease_balance(state, source_index, source_effective_balance).map_err(block_to_epoch)?;
        increase_balance(state, target_index, source_effective_balance).map_err(block_to_epoch)?;

        next_pending_consolidation = next_pending_consolidation
            .checked_add(1)
            .ok_or(EpochError::ArithmeticOverflow)?;
    }

    state
        .pending_consolidations_drain_prefix(next_pending_consolidation)
        .map_err(EpochError::from)?;
    Ok(())
}
