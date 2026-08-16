//! Spec `process_operations` and per-operation handlers (CC-12c core + CC-12d
//! execution requests).

mod attestation;
mod attester_slashing;
mod bls_to_execution_change;
mod consolidation_request;
mod deposit;
mod deposit_request;
mod proposer_slashing;
mod voluntary_exit;
mod withdrawal_request;

pub use attestation::{
    ProcessAttestationOpts, get_attesting_indices_for_test, process_attestation,
};
pub use attester_slashing::process_attester_slashing;
pub use bls_to_execution_change::{
    bls_to_execution_change_domain, process_bls_to_execution_change,
};
pub use consolidation_request::process_consolidation_request;
pub use deposit::{
    add_validator_to_registry, apply_deposit, get_validator_from_deposit,
    is_valid_deposit_signature, process_deposit,
};
pub use deposit_request::process_deposit_request;
pub use proposer_slashing::process_proposer_slashing;
pub use voluntary_exit::process_voluntary_exit;
pub use withdrawal_request::process_withdrawal_request;

use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::{BeaconBlock, BeaconState};

use crate::block::TransitionContext;
use crate::error::{BlockError, OperationError};

/// Spec `process_operations` — count assertions + Electra loops (CC-12c + CC-12d).
///
/// Signature verification: when `verify_signatures` is true, each signed handler
/// verifies its own BLS material (operations vectors / `bls_setting` ≠ 0).
/// Full `state_transition` verifies the block signature set first and may
/// pass `false` here.
pub fn process_operations<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
    verify_signatures: bool,
) -> Result<(), BlockError> {
    let body = &block.body;
    let config = ctx.config;

    // Count assertions (list capacities are SSZ-enforced; deposit count is dynamic).
    assert_op_count(
        "proposer_slashings",
        body.proposer_slashings.len(),
        P::MAX_PROPOSER_SLASHINGS,
    )?;
    assert_op_count(
        "attester_slashings",
        body.attester_slashings.len(),
        P::MAX_ATTESTER_SLASHINGS_ELECTRA,
    )?;
    assert_op_count(
        "attestations",
        body.attestations.len(),
        P::MAX_ATTESTATIONS_ELECTRA,
    )?;
    assert_op_count("deposits", body.deposits.len(), P::MAX_DEPOSITS)?;
    assert_op_count(
        "voluntary_exits",
        body.voluntary_exits.len(),
        P::MAX_VOLUNTARY_EXITS,
    )?;
    assert_op_count(
        "bls_to_execution_changes",
        body.bls_to_execution_changes.len(),
        P::MAX_BLS_TO_EXECUTION_CHANGES,
    )?;

    // Electra deposit-count assertion.
    let eth1_deposit_index_limit = state
        .eth1_data()
        .deposit_count
        .min(state.deposit_requests_start_index());
    if state.eth1_deposit_index() < eth1_deposit_index_limit {
        let expected = P::MAX_DEPOSITS.min(eth1_deposit_index_limit - state.eth1_deposit_index());
        if body.deposits.len() as u64 != expected {
            return Err(BlockError::InvalidOperation(OperationError::Invalid {
                op: "deposits",
                detail: format!("expected {expected} deposits, got {}", body.deposits.len()),
            }));
        }
    } else if !body.deposits.is_empty() {
        return Err(BlockError::InvalidOperation(OperationError::Invalid {
            op: "deposits",
            detail: "eth1 deposits disabled after deposit_requests_start_index".into(),
        }));
    }

    for slashing in body.proposer_slashings.iter() {
        process_proposer_slashing(state, slashing, config, verify_signatures)?;
    }
    for slashing in body.attester_slashings.iter() {
        process_attester_slashing(state, slashing, config, verify_signatures)?;
    }
    for attestation in body.attestations.iter() {
        process_attestation(
            state,
            attestation,
            ProcessAttestationOpts { verify_signatures },
        )?;
    }
    for deposit in body.deposits.iter() {
        process_deposit(state, deposit, config, ctx.pubkey_index_map())?;
    }
    for exit in body.voluntary_exits.iter() {
        process_voluntary_exit(state, exit, config, verify_signatures)?;
    }
    for change in body.bls_to_execution_changes.iter() {
        process_bls_to_execution_change(state, change, config, verify_signatures)?;
    }

    // CC-12d — execution requests (Electra).
    for req in body.execution_requests.deposits.iter() {
        process_deposit_request(state, req)?;
    }
    for req in body.execution_requests.withdrawals.iter() {
        process_withdrawal_request(state, req, config, ctx.pubkey_index_map())?;
    }
    for req in body.execution_requests.consolidations.iter() {
        process_consolidation_request(state, req, config, ctx.pubkey_index_map())?;
    }

    Ok(())
}

fn assert_op_count(op: &'static str, count: usize, max: u64) -> Result<(), BlockError> {
    if count as u64 > max {
        return Err(BlockError::OperationCountOverflow { op, count, max });
    }
    Ok(())
}

/// Build a [`TransitionContext`] and run [`process_operations`].
#[inline]
pub fn process_operations_with_config<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    config: &ChainConfig,
    engine: &dyn crate::engine_seam::ExecutionEngine<P>,
    verify_signatures: bool,
) -> Result<(), BlockError> {
    let ctx = TransitionContext::new(config, engine);
    process_operations(state, block, &ctx, verify_signatures)
}
