//! Spec `process_sync_aggregate` (Altair+).
//!
//! Uses [`eth_fast_aggregate_verify`] (empty participants + infinity → true)
//! and resolves participant indices exclusively through [`PubkeyIndexMap`].

use cc_crypto::{
    DOMAIN_SYNC_COMMITTEE, INFINITY_SIGNATURE, Signature, compute_signing_root,
    eth_fast_aggregate_verify, get_domain,
};
use cc_types::containers::SyncAggregate;
use cc_types::preset::Preset;
use cc_types::primitives::{Gwei, Slot};
use cc_types::{BeaconBlock, BeaconState};

use crate::block::TransitionContext;
use crate::error::{BlockError, SignatureKind};
use crate::helpers::accessors::{
    get_base_reward_per_increment, get_beacon_proposer_index, get_block_root_at_slot,
    get_total_active_balance,
};
use crate::helpers::constants::{
    EFFECTIVE_BALANCE_INCREMENT, PROPOSER_WEIGHT, SYNC_REWARD_WEIGHT, WEIGHT_DENOMINATOR,
};
use crate::helpers::misc::compute_epoch_at_slot;
use crate::helpers::mutators::{decrease_balance, increase_balance};
use crate::signatures::{decode_signature, decode_state_pubkey};

/// Spec `process_sync_aggregate`.
///
/// Verifies the aggregate signature over the **previous slot's** block root
/// under `DOMAIN_SYNC_COMMITTEE`, then applies participant/proposer rewards.
pub fn process_sync_aggregate<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
) -> Result<(), BlockError> {
    process_sync_aggregate_inner(state, &block.body.sync_aggregate, true, ctx)
}

/// Core handler used by the operations vector runner (takes the aggregate
/// directly and honours `verify_signatures` from `meta.yaml` `bls_setting`).
pub fn process_sync_aggregate_with_opts<P: Preset>(
    state: &mut BeaconState<P>,
    sync_aggregate: &SyncAggregate<P>,
    verify_signatures: bool,
    ctx: &TransitionContext<'_, P>,
) -> Result<(), BlockError> {
    process_sync_aggregate_inner(state, sync_aggregate, verify_signatures, ctx)
}

fn process_sync_aggregate_inner<P: Preset>(
    state: &mut BeaconState<P>,
    sync_aggregate: &SyncAggregate<P>,
    verify_signatures: bool,
    ctx: &TransitionContext<'_, P>,
) -> Result<(), BlockError> {
    // Standalone `with_opts` callers (operations vectors) start empty.
    // `process_block` already topped up; skip the second O(V) walk.
    if ctx.pubkeys().is_empty() {
        ctx.top_up_pubkey_cache(state);
    }
    let committee = state.current_sync_committee().clone();
    let bits = &sync_aggregate.sync_committee_bits;

    // Participant pubkeys for BLS (committee order; bit-selected).
    let mut participant_pubkeys = Vec::new();
    let sync_size = bits.len();
    for i in 0..sync_size {
        let bit = bits.get(i).map_err(|_| BlockError::ArithmeticOverflow)?;
        if bit {
            let pk = committee
                .pubkeys
                .get(i)
                .ok_or(BlockError::ArithmeticOverflow)?;
            // State-resident material → StateBlsMaterial on bad encoding.
            participant_pubkeys.push(decode_state_pubkey(pk)?);
        }
    }

    let previous_slot = Slot::new(state.slot().as_u64().max(1).saturating_sub(1));
    let domain = get_domain(
        &state.fork(),
        DOMAIN_SYNC_COMMITTEE,
        Some(compute_epoch_at_slot::<P>(previous_slot)),
        state.genesis_validators_root(),
    );
    let block_root = get_block_root_at_slot(state, previous_slot)?;
    let signing_root = *compute_signing_root(&block_root, domain).as_array();

    if verify_signatures {
        // Infinity is rejected by `Signature::deserialize` (sig_infcheck) but is
        // the required empty-participant aggregate (eth_fast_aggregate_verify).
        let signature = if sync_aggregate.sync_committee_signature.as_array() == &INFINITY_SIGNATURE
        {
            Signature::infinity()
        } else {
            // Block-carried signature → BlsMaterial on bad encoding.
            decode_signature(&sync_aggregate.sync_committee_signature)?
        };
        if !eth_fast_aggregate_verify(&participant_pubkeys, &signing_root, &signature) {
            return Err(BlockError::InvalidSignature {
                which: SignatureKind::SyncAggregate,
            });
        }
    }

    // Participant and proposer rewards.
    let total_active_balance = get_total_active_balance(state)?;
    let total_active_increments =
        total_active_balance.as_u64() / EFFECTIVE_BALANCE_INCREMENT.as_u64();
    let base_reward_per_increment = get_base_reward_per_increment(state)?;
    let total_base_rewards = base_reward_per_increment
        .as_u64()
        .saturating_mul(total_active_increments);
    let max_participant_rewards = total_base_rewards.saturating_mul(SYNC_REWARD_WEIGHT)
        / WEIGHT_DENOMINATOR
        / P::SLOTS_PER_EPOCH;
    let participant_reward = Gwei::new(max_participant_rewards / P::SYNC_COMMITTEE_SIZE);
    let proposer_reward = Gwei::new(
        participant_reward.as_u64() * PROPOSER_WEIGHT / (WEIGHT_DENOMINATOR - PROPOSER_WEIGHT),
    );

    // Resolve committee indices **only** through PubkeyIndexMap (no linear scan).
    let proposer_index = get_beacon_proposer_index(state)?;
    let committee_indices = {
        let pubkeys = ctx.pubkeys();
        let mut committee_indices = Vec::with_capacity(sync_size);
        for i in 0..sync_size {
            let pk = committee
                .pubkeys
                .get(i)
                .ok_or(BlockError::ArithmeticOverflow)?;
            let idx = pubkeys.get(pk).ok_or(BlockError::CachePoisoned)?;
            committee_indices.push(idx);
        }
        committee_indices
    };

    for (participant_index, i) in committee_indices.into_iter().zip(0..sync_size) {
        let bit = bits.get(i).map_err(|_| BlockError::ArithmeticOverflow)?;
        if bit {
            increase_balance(state, participant_index, participant_reward)?;
            increase_balance(state, proposer_index, proposer_reward)?;
        } else {
            decrease_balance(state, participant_index, participant_reward)?;
        }
    }

    Ok(())
}
