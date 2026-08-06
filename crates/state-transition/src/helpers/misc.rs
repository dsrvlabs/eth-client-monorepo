//! Miscellaneous pure helpers (`compute_*`, merkle, shuffling).

use cc_crypto::{hash32_concat, hash_fixed};
use cc_types::preset::Preset;
use cc_types::primitives::{
    Epoch, ExecutionAddress, KzgCommitment, Root, Slot, ValidatorIndex,
};

use crate::engine_seam::VersionedHash;
use crate::error::BlockError;
use crate::helpers::constants::{UINT64_MAX_SQRT, VERSIONED_HASH_VERSION_KZG};

/// `compute_epoch_at_slot(slot) = slot // SLOTS_PER_EPOCH`.
#[inline]
pub fn compute_epoch_at_slot<P: Preset>(slot: Slot) -> Epoch {
    slot.epoch(P::SLOTS_PER_EPOCH)
}

/// Spec `compute_start_slot_at_epoch`.
#[inline]
pub fn compute_start_slot_at_epoch<P: Preset>(epoch: Epoch) -> Slot {
    Slot::new(epoch.as_u64().saturating_mul(P::SLOTS_PER_EPOCH))
}

/// Spec `compute_activation_exit_epoch`.
#[inline]
pub fn compute_activation_exit_epoch<P: Preset>(epoch: Epoch) -> Epoch {
    Epoch::new(
        epoch
            .as_u64()
            .saturating_add(1)
            .saturating_add(P::MAX_SEED_LOOKAHEAD),
    )
}

/// Spec `integer_squareroot`.
pub fn integer_squareroot(n: u64) -> u64 {
    if n == u64::MAX {
        return UINT64_MAX_SQRT;
    }
    let mut x = n;
    let mut y = x.saturating_add(1) / 2;
    while y < x {
        x = y;
        y = x.saturating_add(n / x) / 2;
    }
    x
}

/// Spec `kzg_commitment_to_versioned_hash`.
///
/// `VERSIONED_HASH_VERSION_KZG + hash(commitment)[1:]`.
pub fn kzg_commitment_to_versioned_hash(commitment: &KzgCommitment) -> VersionedHash {
    let digest = hash_fixed(commitment.as_slice());
    let mut out = [0u8; 32];
    out[0] = VERSIONED_HASH_VERSION_KZG;
    out[1..].copy_from_slice(&digest[1..]);
    VersionedHash::from(out)
}

/// Execution address from withdrawal credentials bytes `[12..32]`.
#[inline]
pub fn execution_address_from_credentials(credentials: &Root) -> ExecutionAddress {
    let bytes = credentials.as_array();
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&bytes[12..32]);
    ExecutionAddress::from_array(addr)
}

/// XOR two 32-byte roots (RANDAO mix update).
#[inline]
pub fn xor_bytes32(a: Root, b: Root) -> Root {
    let mut out = [0u8; 32];
    for (o, (x, y)) in out
        .iter_mut()
        .zip(a.as_array().iter().zip(b.as_array().iter()))
    {
        *o = x ^ y;
    }
    Root::from_array(out)
}

/// Hash of a BLS signature (96 bytes) as a 32-byte root (RANDAO mix).
#[inline]
pub fn hash_signature_root(sig: &cc_types::primitives::BlsSignature) -> Root {
    Root::from_array(hash_fixed(sig.as_slice()))
}

/// Little-endian `uint_to_bytes` for a `u64` (8 bytes).
#[inline]
pub fn u64_to_bytes_le(n: u64) -> [u8; 8] {
    n.to_le_bytes()
}

/// Little-endian `uint_to_bytes` for a `u32` (4 bytes).
#[inline]
pub fn u32_to_bytes_le(n: u32) -> [u8; 4] {
    n.to_le_bytes()
}

/// Spec `compute_shuffled_index` (swap-or-not).
pub fn compute_shuffled_index<P: Preset>(
    mut index: u64,
    index_count: u64,
    seed: Root,
) -> Result<u64, BlockError> {
    if index >= index_count || index_count == 0 {
        return Err(BlockError::ArithmeticOverflow);
    }
    let seed_bytes = seed.as_array();
    for current_round in 0..P::SHUFFLE_ROUND_COUNT {
        let mut pivot_input = [0u8; 33];
        pivot_input[..32].copy_from_slice(seed_bytes);
        pivot_input[32] = current_round;
        let pivot = u64::from_le_bytes(hash_fixed(&pivot_input)[0..8].try_into().unwrap_or([0; 8]))
            % index_count;

        let flip = (pivot.saturating_add(index_count).saturating_sub(index)) % index_count;
        let position = index.max(flip);

        let mut source_input = [0u8; 37];
        source_input[..32].copy_from_slice(seed_bytes);
        source_input[32] = current_round;
        source_input[33..37].copy_from_slice(&u32_to_bytes_le((position / 256) as u32));
        let source = hash_fixed(&source_input);
        let byte = source[((position % 256) / 8) as usize];
        let bit = (byte >> (position % 8)) & 1;
        if bit == 1 {
            index = flip;
        }
    }
    Ok(index)
}

/// Spec `compute_committee`.
pub fn compute_committee<P: Preset>(
    indices: &[ValidatorIndex],
    seed: Root,
    index: u64,
    count: u64,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    if count == 0 {
        return Err(BlockError::ArithmeticOverflow);
    }
    let len = indices.len() as u64;
    let start = (len.saturating_mul(index)) / count;
    let end = (len.saturating_mul(index.saturating_add(1))) / count;
    let mut out = Vec::with_capacity((end.saturating_sub(start)) as usize);
    for i in start..end {
        let shuffled = compute_shuffled_index::<P>(i, len, seed)?;
        let vi = indices
            .get(shuffled as usize)
            .copied()
            .ok_or(BlockError::ArithmeticOverflow)?;
        out.push(vi);
    }
    Ok(out)
}

/// Spec `is_valid_merkle_branch` / `compute_merkle_branch_root`.
pub fn is_valid_merkle_branch(
    leaf: Root,
    branch: &[Root],
    depth: usize,
    index: u64,
    root: Root,
) -> bool {
    if depth != branch.len() {
        return false;
    }
    let mut value = *leaf.as_array();
    for (i, node) in branch.iter().enumerate().take(depth) {
        let sibling = node.as_array();
        if (index >> i) & 1 == 1 {
            value = hash32_concat(sibling, &value);
        } else {
            value = hash32_concat(&value, sibling);
        }
    }
    Root::from_array(value) == root
}

/// Hash of a BLS public key (used for BLS withdrawal credential check).
#[inline]
pub fn hash_pubkey(pubkey: &cc_types::primitives::BlsPublicKey) -> [u8; 32] {
    hash_fixed(pubkey.as_slice())
}
