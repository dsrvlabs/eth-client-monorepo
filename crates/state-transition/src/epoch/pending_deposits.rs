//! Spec `process_pending_deposits` / `apply_pending_deposit` (Fulu / Electra).

use cc_types::BeaconState;
use cc_types::operations::PendingDeposit;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Gwei, ValidatorIndex};

use crate::block::operations::{add_validator_to_registry, is_valid_deposit_signature};
use crate::error::EpochError;
use crate::helpers::accessors::{get_activation_exit_churn_limit, get_current_epoch};
use crate::helpers::constants::{FAR_FUTURE_EPOCH, MAX_PENDING_DEPOSITS_PER_EPOCH};
use crate::helpers::misc::compute_start_slot_at_epoch;
use crate::helpers::mutators::increase_balance;

use super::block_to_epoch;

/// Spec `apply_pending_deposit` (Electra).
///
/// New validator (valid PoP): append to registry with the deposit amount.
/// Existing: top up balance. Invalid PoP for unknown pubkey: no-op.
pub fn apply_pending_deposit<P: Preset>(
    state: &mut BeaconState<P>,
    deposit: &PendingDeposit,
) -> Result<(), EpochError> {
    let existing = state.caches().pubkeys.get(&deposit.pubkey).or_else(|| {
        state
            .validators_iter()
            .enumerate()
            .find(|(_, v)| v.pubkey == deposit.pubkey)
            .map(|(i, _)| ValidatorIndex::new(i as u64))
    });

    match existing {
        None => {
            if is_valid_deposit_signature::<P>(
                &deposit.pubkey,
                &deposit.withdrawal_credentials,
                deposit.amount,
                &deposit.signature,
            )
            .map_err(block_to_epoch)?
            {
                add_validator_to_registry(
                    state,
                    deposit.pubkey,
                    deposit.withdrawal_credentials,
                    deposit.amount,
                )
                .map_err(block_to_epoch)?;
            }
        }
        Some(idx) => {
            state.caches_mut().pubkeys.insert(deposit.pubkey, idx);
            increase_balance(state, idx, deposit.amount).map_err(block_to_epoch)?;
        }
    }
    Ok(())
}

/// Spec `process_pending_deposits` (Fulu: eth1-bridge gate removed).
///
/// Drains finalized deposits up to `MAX_PENDING_DEPOSITS_PER_EPOCH` and the
/// activation-exit balance churn. Exiting validators' deposits are postponed
/// (kept in the queue) rather than dropped; withdrawn validators receive the
/// balance without consuming churn.
pub fn process_pending_deposits<P: Preset>(state: &mut BeaconState<P>) -> Result<(), EpochError> {
    let next_epoch = Epoch::new(get_current_epoch(state).as_u64().saturating_add(1));
    let available_for_processing = state
        .deposit_balance_to_consume()
        .as_u64()
        .checked_add(
            get_activation_exit_churn_limit(state)
                .map_err(block_to_epoch)?
                .as_u64(),
        )
        .ok_or(EpochError::ArithmeticOverflow)?;

    let mut processed_amount = 0u64;
    let mut next_deposit_index = 0usize;
    let mut deposits_to_postpone: Vec<PendingDeposit> = Vec::new();
    let mut is_churn_limit_reached = false;
    let finalized_slot = compute_start_slot_at_epoch::<P>(state.finalized_checkpoint().epoch);

    // Snapshot the queue; apply/mutate state for each processed entry.
    let queue: Vec<PendingDeposit> = state.pending_deposits_iter().copied().collect();

    for deposit in &queue {
        // Finalized-slot gate: stop (remaining stay in queue).
        if deposit.slot.as_u64() > finalized_slot.as_u64() {
            break;
        }
        if (next_deposit_index as u64) >= MAX_PENDING_DEPOSITS_PER_EPOCH {
            break;
        }

        let (is_validator_exited, is_validator_withdrawn) =
            match state.caches().pubkeys.get(&deposit.pubkey).or_else(|| {
                state
                    .validators_iter()
                    .enumerate()
                    .find(|(_, v)| v.pubkey == deposit.pubkey)
                    .map(|(i, _)| ValidatorIndex::new(i as u64))
            }) {
                Some(idx) => {
                    let v = state
                        .validators_get(idx.as_u64() as usize)
                        .ok_or(EpochError::ArithmeticOverflow)?;
                    (
                        v.exit_epoch.as_u64() < FAR_FUTURE_EPOCH.as_u64(),
                        v.withdrawable_epoch.as_u64() < next_epoch.as_u64(),
                    )
                }
                None => (false, false),
            };

        if is_validator_withdrawn {
            apply_pending_deposit(state, deposit)?;
        } else if is_validator_exited {
            // Postpone until after withdrawable epoch — do not drop.
            deposits_to_postpone.push(*deposit);
        } else {
            is_churn_limit_reached = processed_amount
                .checked_add(deposit.amount.as_u64())
                .ok_or(EpochError::ArithmeticOverflow)?
                > available_for_processing;
            if is_churn_limit_reached {
                break;
            }
            processed_amount = processed_amount
                .checked_add(deposit.amount.as_u64())
                .ok_or(EpochError::ArithmeticOverflow)?;
            apply_pending_deposit(state, deposit)?;
        }

        next_deposit_index = next_deposit_index
            .checked_add(1)
            .ok_or(EpochError::ArithmeticOverflow)?;
    }

    // remaining = queue[next_deposit_index..] + postponed
    let mut new_queue: Vec<PendingDeposit> = queue.into_iter().skip(next_deposit_index).collect();
    new_queue.append(&mut deposits_to_postpone);
    state
        .pending_deposits_replace(new_queue)
        .map_err(EpochError::from)?;

    if is_churn_limit_reached {
        state.set_deposit_balance_to_consume(Gwei::new(
            available_for_processing.saturating_sub(processed_amount),
        ));
    } else {
        state.set_deposit_balance_to_consume(Gwei::new(0));
    }
    Ok(())
}
