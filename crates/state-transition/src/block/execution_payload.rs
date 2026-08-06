//! Spec `process_execution_payload` — sole CC-14 engine call site (Architecture §5.5–5.6).

use cc_types::execution::ExecutionPayloadHeader;
use cc_types::preset::Preset;
use cc_types::{BeaconBlock, BeaconState};
use tree_hash::TreeHash;

use crate::block::TransitionContext;
use crate::engine_seam::{NewPayloadRequest, PayloadStatus};
use crate::error::{BlockError, EngineError};
use crate::helpers::accessors::{compute_time_at_slot, get_current_epoch, get_randao_mix};
use crate::helpers::misc::kzg_commitment_to_versioned_hash;

/// Spec `process_execution_payload` (Fulu).
///
/// Local checks (parent hash, prev_randao, timestamp, blob bound), versioned-hash
/// derivation, engine seam call, then cache `latest_execution_payload_header`.
///
/// Blob bound is a **runtime** lookup:
/// `body.blob_kzg_commitments.len() <= ctx.config.get_blob_parameters(epoch).max_blobs_per_block`
/// (Architecture §5.6). Never compared against a compile-time constant.
pub fn process_execution_payload<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
) -> Result<(), BlockError> {
    let body = &block.body;
    let payload = &body.execution_payload;
    let current_epoch = get_current_epoch(state);

    // Verify consistency of the parent hash with respect to the previous header.
    if payload.parent_hash != state.latest_execution_payload_header().block_hash {
        return Err(BlockError::InvalidPayload);
    }

    // Verify prev_randao.
    let expected_randao = get_randao_mix(state, current_epoch)?;
    if payload.prev_randao != expected_randao {
        return Err(BlockError::InvalidPayload);
    }

    // Verify timestamp.
    let expected_ts =
        compute_time_at_slot(state.genesis_time(), state.slot(), ctx.config.seconds_per_slot);
    if payload.timestamp != expected_ts {
        return Err(BlockError::InvalidPayload);
    }

    // Fulu: blob commitments under the runtime schedule bound (§5.6).
    let max_blobs = ctx
        .config
        .get_blob_parameters::<P>(current_epoch)
        .max_blobs_per_block;
    let commitment_count = body.blob_kzg_commitments.len();
    if (commitment_count as u64) > max_blobs {
        return Err(BlockError::BlobBoundExceeded {
            count: commitment_count,
            max: max_blobs,
        });
    }

    // Versioned hashes from blob_kzg_commitments.
    let versioned_hashes: Vec<_> = body
        .blob_kzg_commitments
        .iter()
        .map(kzg_commitment_to_versioned_hash)
        .collect();

    // parent_beacon_block_root = state.latest_block_header.parent_root (Electra/Fulu).
    // After process_block_header this equals block.parent_root; operations tests
    // call this handler alone, so always read from state.
    let request = NewPayloadRequest {
        execution_payload: payload,
        versioned_hashes,
        parent_beacon_block_root: state.latest_block_header().parent_root,
        execution_requests: &body.execution_requests,
    };

    match ctx.engine.verify_and_notify_new_payload(request)? {
        PayloadStatus::Valid | PayloadStatus::Syncing => {}
        PayloadStatus::Invalid { .. } => {
            return Err(BlockError::Engine(EngineError::InvalidPayload));
        }
    }

    // Cache execution payload header.
    let transactions_root =
        cc_types::primitives::Root::from_hash256(TreeHash::tree_hash_root(&payload.transactions));
    let withdrawals_root =
        cc_types::primitives::Root::from_hash256(TreeHash::tree_hash_root(&payload.withdrawals));

    state.set_latest_execution_payload_header(ExecutionPayloadHeader {
        parent_hash: payload.parent_hash,
        fee_recipient: payload.fee_recipient,
        state_root: payload.state_root,
        receipts_root: payload.receipts_root,
        logs_bloom: payload.logs_bloom.clone(),
        prev_randao: payload.prev_randao,
        block_number: payload.block_number,
        gas_limit: payload.gas_limit,
        gas_used: payload.gas_used,
        timestamp: payload.timestamp,
        extra_data: payload.extra_data.clone(),
        base_fee_per_gas: payload.base_fee_per_gas,
        block_hash: payload.block_hash,
        transactions_root,
        withdrawals_root,
        blob_gas_used: payload.blob_gas_used,
        excess_blob_gas: payload.excess_blob_gas,
    });

    Ok(())
}
