//! Committee shuffling, proposer indices, and sync-committee selection (Architecture §5.1 / §5.4).
//!
//! - `compute_shuffled_index` (swap-or-not) lives in [`helpers::misc`] and is re-exported here.
//! - [`get_beacon_committee`] is the sole Phase-1 reader of [`ShufflingCache`].
//! - [`get_beacon_proposer_index`] is a `proposer_lookahead` read and **never** touches the
//!   shuffling cache (EIP-7917). [`get_beacon_proposer_indices`] fills the lookahead vector
//!   at epoch boundaries (consumed by CC-13c's `process_proposer_lookahead`).

use cc_crypto::{
    aggregate_public_keys, hash_fixed, DOMAIN_BEACON_ATTESTER, DOMAIN_BEACON_PROPOSER,
    DOMAIN_SYNC_COMMITTEE,
};
use cc_types::containers::SyncCommittee;
use cc_types::preset::Preset;
use cc_types::primitives::{BlsPublicKey, CommitteeIndex, Epoch, Root, Slot, ValidatorIndex};
use cc_types::{
    BeaconState, ShuffledCommitteeEpoch, ShufflingCacheKey,
};
use ssz_types::FixedVector;

use crate::error::BlockError;
use crate::helpers::accessors::{
    get_active_validator_indices, get_block_root_at_slot, get_current_epoch, get_seed,
};
use crate::helpers::constants::MAX_EFFECTIVE_BALANCE_ELECTRA;
use crate::helpers::misc::{
    compute_epoch_at_slot, compute_shuffled_index, compute_start_slot_at_epoch, u64_to_bytes_le,
};
use crate::signatures::decode_state_pubkey;

pub use crate::helpers::accessors::get_committee_count_per_slot;

/// Electra `MAX_RANDOM_VALUE = 2**16 - 1` (proposer / sync-committee sampling).
const MAX_RANDOM_VALUE: u64 = 0xFFFF;

/// Decision root for an epoch's shuffling: block root at `start_slot(epoch) − 1`.
///
/// At genesis (`start_slot == 0`) the dependent root is the zero root — there is no prior block.
pub fn decision_root_for_epoch<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
) -> Result<Root, BlockError> {
    let start = compute_start_slot_at_epoch::<P>(epoch);
    if start.as_u64() == 0 {
        return Ok(Root::ZERO);
    }
    let dependent_slot = Slot::new(start.as_u64() - 1);
    get_block_root_at_slot(state, dependent_slot)
}

/// Build the full shuffled active-validator list for `epoch` (uncached).
pub fn compute_shuffled_active_indices<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
) -> Result<ShuffledCommitteeEpoch, BlockError> {
    let indices = get_active_validator_indices(state, epoch);
    let seed = get_seed(state, epoch, DOMAIN_BEACON_ATTESTER)?;
    let len = indices.len() as u64;
    if len == 0 {
        return Ok(ShuffledCommitteeEpoch {
            shuffled: Vec::new(),
        });
    }
    let mut shuffled = Vec::with_capacity(indices.len());
    for i in 0..len {
        let si = compute_shuffled_index::<P>(i, len, seed)?;
        let vi = indices
            .get(si as usize)
            .copied()
            .ok_or(BlockError::ArithmeticOverflow)?;
        shuffled.push(vi);
    }
    Ok(ShuffledCommitteeEpoch { shuffled })
}

/// Cached path: ensure the shuffling for `(epoch, decision_root)` is resident and return it.
///
/// Increments the instrumented compute-once counter only on a cold fill.
pub fn get_or_compute_shuffling<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
) -> Result<std::sync::Arc<ShuffledCommitteeEpoch>, BlockError> {
    let decision_root = decision_root_for_epoch(state, epoch)?;
    let key = ShufflingCacheKey {
        epoch,
        decision_root,
    };
    // Clone the Arc result; compute closure captures state by reference for the miss path.
    // We cannot return Result from get_or_insert_with, so pre-validate / fall back carefully.
    if let Some(hit) = state.caches().committees.get(&key) {
        return Ok(hit);
    }
    let computed = compute_shuffled_active_indices(state, epoch)?;
    Ok(state
        .caches()
        .committees
        .get_or_insert_with(key, || computed))
}

/// Spec `get_beacon_committee` — uses [`ShufflingCache`] keyed by `(epoch, decision_root)`.
///
/// Signature matches the uncached helper (CC-12c): `&BeaconState` only. Cache writes use
/// interior mutability on [`cc_types::ShufflingCache`].
pub fn get_beacon_committee<P: Preset>(
    state: &BeaconState<P>,
    slot: Slot,
    index: CommitteeIndex,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    let epoch = compute_epoch_at_slot::<P>(slot);
    let committees_per_slot = get_committee_count_per_slot(state, epoch);
    let shuffling = get_or_compute_shuffling(state, epoch)?;
    let count = committees_per_slot.saturating_mul(P::SLOTS_PER_EPOCH);
    if count == 0 {
        return Err(BlockError::ArithmeticOverflow);
    }
    let committee_index =
        (slot.as_u64() % P::SLOTS_PER_EPOCH).saturating_mul(committees_per_slot) + index.as_u64();
    let len = shuffling.shuffled.len() as u64;
    let start = (len.saturating_mul(committee_index)) / count;
    let end = (len.saturating_mul(committee_index.saturating_add(1))) / count;
    if end < start || end as usize > shuffling.shuffled.len() {
        return Err(BlockError::ArithmeticOverflow);
    }
    Ok(shuffling.shuffled[start as usize..end as usize].to_vec())
}

/// Spec `compute_proposer_index` (Electra: 16-bit random value, `MAX_EFFECTIVE_BALANCE_ELECTRA`).
pub fn compute_proposer_index<P: Preset>(
    state: &BeaconState<P>,
    indices: &[ValidatorIndex],
    seed: Root,
) -> Result<ValidatorIndex, BlockError> {
    if indices.is_empty() {
        return Err(BlockError::ArithmeticOverflow);
    }
    let total = indices.len() as u64;
    let seed_bytes = seed.as_array();
    let mut i = 0u64;
    loop {
        let candidate_shuffled = compute_shuffled_index::<P>(i % total, total, seed)?;
        let candidate_index = indices
            .get(candidate_shuffled as usize)
            .copied()
            .ok_or(BlockError::ArithmeticOverflow)?;
        // random_bytes = hash(seed + uint_to_bytes(i // 16)); offset = (i % 16) * 2
        let mut input = [0u8; 32 + 8];
        input[..32].copy_from_slice(seed_bytes);
        input[32..].copy_from_slice(&u64_to_bytes_le(i / 16));
        let random_bytes = hash_fixed(&input);
        let offset = ((i % 16) * 2) as usize;
        let random_value = u64::from(random_bytes[offset])
            | (u64::from(random_bytes[offset + 1]) << 8);
        let effective_balance = state
            .validators_get(candidate_index.as_u64() as usize)
            .ok_or(BlockError::ArithmeticOverflow)?
            .effective_balance
            .as_u64();
        if effective_balance.saturating_mul(MAX_RANDOM_VALUE)
            >= MAX_EFFECTIVE_BALANCE_ELECTRA
                .as_u64()
                .saturating_mul(random_value)
        {
            return Ok(candidate_index);
        }
        i = i.saturating_add(1);
    }
}

/// Spec `compute_proposer_indices` — one proposer per slot of `epoch`.
pub fn compute_proposer_indices<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
    seed: Root,
    indices: &[ValidatorIndex],
) -> Result<Vec<ValidatorIndex>, BlockError> {
    let start_slot = compute_start_slot_at_epoch::<P>(epoch);
    let seed_bytes = seed.as_array();
    let mut out = Vec::with_capacity(P::SLOTS_PER_EPOCH as usize);
    for i in 0..P::SLOTS_PER_EPOCH {
        let slot = Slot::new(start_slot.as_u64().saturating_add(i));
        let mut input = [0u8; 32 + 8];
        input[..32].copy_from_slice(seed_bytes);
        input[32..].copy_from_slice(&u64_to_bytes_le(slot.as_u64()));
        let slot_seed = Root::from_array(hash_fixed(&input));
        out.push(compute_proposer_index(state, indices, slot_seed)?);
    }
    Ok(out)
}

/// Spec `get_beacon_proposer_indices(state, epoch)` — fills `proposer_lookahead` epochs.
///
/// Does **not** read or write the shuffling cache (proposer path is independent).
pub fn get_beacon_proposer_indices<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    let indices = get_active_validator_indices(state, epoch);
    let seed = get_seed(state, epoch, DOMAIN_BEACON_PROPOSER)?;
    compute_proposer_indices(state, epoch, seed, &indices)
}

/// Spec `get_next_sync_committee_indices` (Electra random-value width).
pub fn get_next_sync_committee_indices<P: Preset>(
    state: &BeaconState<P>,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    let epoch = Epoch::new(get_current_epoch(state).as_u64().saturating_add(1));
    let active = get_active_validator_indices(state, epoch);
    if active.is_empty() {
        return Err(BlockError::ArithmeticOverflow);
    }
    let active_count = active.len() as u64;
    let seed = get_seed(state, epoch, DOMAIN_SYNC_COMMITTEE)?;
    let seed_bytes = seed.as_array();
    let mut sync_committee_indices = Vec::with_capacity(P::SYNC_COMMITTEE_SIZE as usize);
    let mut i = 0u64;
    while (sync_committee_indices.len() as u64) < P::SYNC_COMMITTEE_SIZE {
        let shuffled_index = compute_shuffled_index::<P>(i % active_count, active_count, seed)?;
        let candidate_index = active
            .get(shuffled_index as usize)
            .copied()
            .ok_or(BlockError::ArithmeticOverflow)?;
        let mut input = [0u8; 32 + 8];
        input[..32].copy_from_slice(seed_bytes);
        input[32..].copy_from_slice(&u64_to_bytes_le(i / 16));
        let random_bytes = hash_fixed(&input);
        let offset = ((i % 16) * 2) as usize;
        let random_value = u64::from(random_bytes[offset])
            | (u64::from(random_bytes[offset + 1]) << 8);
        let effective_balance = state
            .validators_get(candidate_index.as_u64() as usize)
            .ok_or(BlockError::ArithmeticOverflow)?
            .effective_balance
            .as_u64();
        if effective_balance.saturating_mul(MAX_RANDOM_VALUE)
            >= MAX_EFFECTIVE_BALANCE_ELECTRA
                .as_u64()
                .saturating_mul(random_value)
        {
            sync_committee_indices.push(candidate_index);
        }
        i = i.saturating_add(1);
    }
    Ok(sync_committee_indices)
}

/// Spec `get_next_sync_committee`.
pub fn get_next_sync_committee<P: Preset>(
    state: &BeaconState<P>,
) -> Result<SyncCommittee<P>, BlockError> {
    let indices = get_next_sync_committee_indices(state)?;
    let mut pubkeys: Vec<BlsPublicKey> = Vec::with_capacity(indices.len());
    let mut crypto_pks = Vec::with_capacity(indices.len());
    for idx in &indices {
        let v = state
            .validators_get(idx.as_u64() as usize)
            .ok_or(BlockError::ArithmeticOverflow)?;
        pubkeys.push(v.pubkey);
        crypto_pks.push(decode_state_pubkey(&v.pubkey)?);
    }
    let refs: Vec<&cc_crypto::PublicKey> = crypto_pks.iter().collect();
    let aggregate = aggregate_public_keys(&refs).map_err(|_| BlockError::ArithmeticOverflow)?;
    let aggregate_pubkey = BlsPublicKey::from_array(aggregate.serialize());
    let pubkeys_fv = FixedVector::new(pubkeys).map_err(|_| BlockError::ArithmeticOverflow)?;
    Ok(SyncCommittee {
        pubkeys: pubkeys_fv,
        aggregate_pubkey,
    })
}

/// Uncached committee slice from a precomputed shuffled list (test / internal).
pub fn committee_from_shuffling(
    shuffled: &[ValidatorIndex],
    slot: Slot,
    index: CommitteeIndex,
    committees_per_slot: u64,
    slots_per_epoch: u64,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    let count = committees_per_slot.saturating_mul(slots_per_epoch);
    if count == 0 {
        return Err(BlockError::ArithmeticOverflow);
    }
    let committee_index =
        (slot.as_u64() % slots_per_epoch).saturating_mul(committees_per_slot) + index.as_u64();
    let len = shuffled.len() as u64;
    let start = (len.saturating_mul(committee_index)) / count;
    let end = (len.saturating_mul(committee_index.saturating_add(1))) / count;
    if end as usize > shuffled.len() {
        return Err(BlockError::ArithmeticOverflow);
    }
    Ok(shuffled[start as usize..end as usize].to_vec())
}
