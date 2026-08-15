//! Spec `process_deposit_request` (Electra EIP-6110).
//!
//! Queues a [`PendingDeposit`]; application is epoch processing (CC-13c).

use cc_types::BeaconState;
use cc_types::operations::{DepositRequest, PendingDeposit};
use cc_types::preset::Preset;

use crate::error::BlockError;
use crate::helpers::constants::UNSET_DEPOSIT_REQUESTS_START_INDEX;

/// Spec `process_deposit_request`.
///
/// Sets `deposit_requests_start_index` on the first request seen, then appends
/// a pending deposit. Never applies the deposit inline.
pub fn process_deposit_request<P: Preset>(
    state: &mut BeaconState<P>,
    deposit_request: &DepositRequest,
) -> Result<(), BlockError> {
    if state.deposit_requests_start_index() == UNSET_DEPOSIT_REQUESTS_START_INDEX {
        state.set_deposit_requests_start_index(deposit_request.index);
    }

    state.pending_deposits_push(PendingDeposit {
        pubkey: deposit_request.pubkey,
        withdrawal_credentials: deposit_request.withdrawal_credentials,
        amount: deposit_request.amount,
        signature: deposit_request.signature,
        slot: state.slot(),
    })?;
    Ok(())
}
