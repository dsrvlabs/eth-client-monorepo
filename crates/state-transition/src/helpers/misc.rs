//! Miscellaneous pure helpers (`compute_*`).

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, ExecutionAddress, KzgCommitment, Root, Slot};

use crate::engine_seam::VersionedHash;
use crate::helpers::constants::VERSIONED_HASH_VERSION_KZG;

/// `compute_epoch_at_slot(slot) = slot // SLOTS_PER_EPOCH`.
#[inline]
pub fn compute_epoch_at_slot<P: Preset>(slot: Slot) -> Epoch {
    slot.epoch(P::SLOTS_PER_EPOCH)
}

/// Spec `kzg_commitment_to_versioned_hash`.
///
/// `VERSIONED_HASH_VERSION_KZG + hash(commitment)[1:]`.
pub fn kzg_commitment_to_versioned_hash(commitment: &KzgCommitment) -> VersionedHash {
    let digest = cc_crypto::hash_fixed(commitment.as_slice());
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
    for (o, (x, y)) in out.iter_mut().zip(a.as_array().iter().zip(b.as_array().iter())) {
        *o = x ^ y;
    }
    Root::from_array(out)
}

/// Hash of a BLS signature (96 bytes) as a 32-byte root (RANDAO mix).
#[inline]
pub fn hash_signature_root(sig: &cc_types::primitives::BlsSignature) -> Root {
    Root::from_array(cc_crypto::hash_fixed(sig.as_slice()))
}
