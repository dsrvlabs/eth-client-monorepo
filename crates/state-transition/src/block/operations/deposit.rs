//! Spec `process_deposit` / `apply_deposit` (Electra).

use std::cell::RefCell;

use cc_crypto::{DOMAIN_DEPOSIT, compute_signing_root, verify};
use cc_types::BeaconState;
use cc_types::PubkeyIndexMap;
use cc_types::config::ChainConfig;
use cc_types::containers::{DepositMessage, Validator};
use cc_types::operations::{Deposit, PendingDeposit};
use cc_types::preset::Preset;
use cc_types::primitives::{Gwei, Root, Slot, ValidatorIndex};
use tree_hash::TreeHash;

use crate::epoch_cache::note_registry_or_effective_balance_change;
use crate::error::{BlockError, OperationError};
use crate::helpers::accessors::get_validator_index_by_pubkey;
use crate::helpers::constants::{
    DEPOSIT_CONTRACT_TREE_DEPTH, EFFECTIVE_BALANCE_INCREMENT, FAR_FUTURE_EPOCH, GENESIS_SLOT,
};
use crate::helpers::misc::is_valid_merkle_branch;
use crate::helpers::predicates::get_max_effective_balance;
use crate::signatures::{decode_block_pubkey, decode_signature};

fn invalid(detail: impl Into<String>) -> BlockError {
    BlockError::InvalidOperation(OperationError::Invalid {
        op: "deposit",
        detail: detail.into(),
    })
}

/// Spec `is_valid_deposit_signature`.
///
/// Domain uses the runtime config's `GENESIS_FORK_VERSION`.
pub fn is_valid_deposit_signature(
    pubkey: &cc_types::primitives::BlsPublicKey,
    withdrawal_credentials: &Root,
    amount: Gwei,
    signature: &cc_types::primitives::BlsSignature,
    config: &ChainConfig,
) -> Result<bool, BlockError> {
    let deposit_message = DepositMessage {
        pubkey: *pubkey,
        withdrawal_credentials: *withdrawal_credentials,
        amount,
    };
    let domain = cc_crypto::compute_domain(DOMAIN_DEPOSIT, Some(config.genesis_fork_version), None);
    let message = *compute_signing_root(&deposit_message, domain).as_array();
    // Block-carried material → Reject on bad encoding.
    let pk = match decode_block_pubkey(pubkey) {
        Ok(pk) => pk,
        Err(_) => return Ok(false),
    };
    let sig = match decode_signature(signature) {
        Ok(sig) => sig,
        Err(_) => return Ok(false),
    };
    Ok(verify(&pk, &message, &sig))
}

/// Spec `get_validator_from_deposit` (Electra).
pub fn get_validator_from_deposit(
    pubkey: cc_types::primitives::BlsPublicKey,
    withdrawal_credentials: Root,
    amount: Gwei,
) -> Validator {
    let mut validator = Validator {
        pubkey,
        withdrawal_credentials,
        effective_balance: Gwei::new(0),
        slashed: false,
        activation_eligibility_epoch: FAR_FUTURE_EPOCH,
        activation_epoch: FAR_FUTURE_EPOCH,
        exit_epoch: FAR_FUTURE_EPOCH,
        withdrawable_epoch: FAR_FUTURE_EPOCH,
    };
    let max_eb = get_max_effective_balance(&validator);
    let amount_u = amount.as_u64();
    validator.effective_balance = Gwei::new(
        (amount_u - (amount_u % EFFECTIVE_BALANCE_INCREMENT.as_u64())).min(max_eb.as_u64()),
    );
    validator
}

/// Spec `add_validator_to_registry` (Electra).
pub fn add_validator_to_registry<P: Preset>(
    state: &mut BeaconState<P>,
    pubkey: cc_types::primitives::BlsPublicKey,
    withdrawal_credentials: Root,
    amount: Gwei,
    cache: &RefCell<PubkeyIndexMap>,
) -> Result<ValidatorIndex, BlockError> {
    let index = ValidatorIndex::new(state.validators_len() as u64);
    // Electra apply_deposit path for new validators uses amount=0 for the
    // registry balance; pending deposit carries the real amount.
    let validator = get_validator_from_deposit(pubkey, withdrawal_credentials, amount);
    state.validators_push(validator)?;
    state.balances_push(amount)?;
    state.previous_epoch_participation_push(0)?;
    state.current_epoch_participation_push(0)?;
    state.inactivity_scores_push(0)?;
    // Extend the context map (S2-A-11). First-wins if the key is already present.
    cache.borrow_mut().insert(pubkey, index);
    // Registry growth invalidates epoch-derived active-set cache (CC-13b).
    note_registry_or_effective_balance_change(state);
    Ok(index)
}

/// Spec `apply_deposit` (Electra).
///
/// New validator: append registry (balance 0) then queue pending deposit.
/// Top-up: queue pending deposit only (not direct balance increase).
pub fn apply_deposit<P: Preset>(
    state: &mut BeaconState<P>,
    pubkey: cc_types::primitives::BlsPublicKey,
    withdrawal_credentials: Root,
    amount: Gwei,
    signature: cc_types::primitives::BlsSignature,
    config: &ChainConfig,
    cache: &RefCell<PubkeyIndexMap>,
) -> Result<(), BlockError> {
    if get_validator_index_by_pubkey(state, &pubkey, cache).is_none() {
        // Proof-of-possession; invalid signature → silently drop (spec).
        if is_valid_deposit_signature(&pubkey, &withdrawal_credentials, amount, &signature, config)?
        {
            // New validator with balance 0; pending deposit carries amount.
            add_validator_to_registry(state, pubkey, withdrawal_credentials, Gwei::new(0), cache)?;
        } else {
            return Ok(());
        }
    }

    // Electra: always queue pending deposit (new or top-up).
    state.pending_deposits_push(PendingDeposit {
        pubkey,
        withdrawal_credentials,
        amount,
        signature,
        slot: Slot::new(GENESIS_SLOT),
    })?;
    Ok(())
}

/// Spec `process_deposit`.
pub fn process_deposit<P: Preset>(
    state: &mut BeaconState<P>,
    deposit: &Deposit,
    config: &ChainConfig,
    cache: &RefCell<PubkeyIndexMap>,
) -> Result<(), BlockError> {
    let leaf = Root::from_hash256(TreeHash::tree_hash_root(&deposit.data));
    let branch: Vec<Root> = deposit.proof.iter().copied().collect();
    if !is_valid_merkle_branch(
        leaf,
        &branch,
        DEPOSIT_CONTRACT_TREE_DEPTH + 1,
        state.eth1_deposit_index(),
        state.eth1_data().deposit_root,
    ) {
        return Err(invalid("invalid deposit merkle branch"));
    }

    let next = state
        .eth1_deposit_index()
        .checked_add(1)
        .ok_or(BlockError::ArithmeticOverflow)?;
    state.set_eth1_deposit_index(next);

    apply_deposit(
        state,
        deposit.data.pubkey,
        deposit.data.withdrawal_credentials,
        deposit.data.amount,
        deposit.data.signature,
        config,
        cache,
    )
}
