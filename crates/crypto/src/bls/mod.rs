//! Thin owned wrapper over `blst` 0.3.17 `min_pk` (Architecture §4.1).
//!
//! Subgroup and infinity checks run **once** at deserialization (`key_validate` /
//! `sig_validate(true)`). Verify paths pass `pks_validate=false` and
//! `sigs_groupcheck=false` so the cost is not paid again per attestation.

mod batch;

pub use batch::{OsRandom, RandomScalarSource, SignatureSet};

use blst::min_pk::{
    AggregatePublicKey as BlstAggPk, AggregateSignature as BlstAggSig, PublicKey as BlstPk,
    Signature as BlstSig,
};
use blst::BLST_ERROR;

// SecretKey is tooling-only (feature `signing` / unit tests). Keep the blst
// import gated so default-feature service builds stay unused-import free.
#[cfg(any(test, feature = "signing"))]
use blst::min_pk::SecretKey as BlstSk;

/// Ethereum BLS signature domain-separation tag (PoP ciphersuite).
pub const BLS_SIGNATURE_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

/// Compressed G2 point-at-infinity (`0xc0 ‖ 0x00*95`), used by
/// [`eth_fast_aggregate_verify`] for the empty-participant special case.
pub const INFINITY_SIGNATURE: [u8; 96] = {
    let mut b = [0u8; 96];
    b[0] = 0xc0;
    b
};

/// BLS wrapper errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlsError {
    /// Byte encoding is not a valid compressed/serialized point.
    #[error("BLS bad encoding")]
    BadEncoding,
    /// Point is not in the correct subgroup.
    #[error("BLS point not in group")]
    PointNotInGroup,
    /// Public key or signature is the point at infinity (rejected at deserialize).
    #[error("BLS point is infinity")]
    Infinity,
    /// Aggregation type mismatch (e.g. empty input).
    #[error("BLS aggregation type mismatch")]
    AggregateTypeMismatch,
    /// Underlying blst error not covered above.
    #[error("BLS error: {0:?}")]
    Other(BLST_ERROR),
}

impl From<BLST_ERROR> for BlsError {
    fn from(err: BLST_ERROR) -> Self {
        match err {
            BLST_ERROR::BLST_BAD_ENCODING => Self::BadEncoding,
            BLST_ERROR::BLST_POINT_NOT_IN_GROUP => Self::PointNotInGroup,
            BLST_ERROR::BLST_PK_IS_INFINITY => Self::Infinity,
            BLST_ERROR::BLST_AGGR_TYPE_MISMATCH => Self::AggregateTypeMismatch,
            other => Self::Other(other),
        }
    }
}

fn success(err: BLST_ERROR) -> bool {
    err == BLST_ERROR::BLST_SUCCESS
}

/// Validated BLS public key (48-byte compressed G1).
#[derive(Clone, Copy)]
pub struct PublicKey(BlstPk);

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.serialize();
        write!(f, "PublicKey(0x")?;
        for b in bytes.iter().take(4) {
            write!(f, "{b:02x}")?;
        }
        write!(f, "…)")
    }
}

impl PartialEq for PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for PublicKey {}

impl PublicKey {
    /// Deserialize and **validate** a compressed public key (`key_validate`).
    ///
    /// This is the only call site for `key_validate` in the crate — subgroup
    /// and infinity checks happen once here, not at verify time.
    pub fn deserialize(bytes: &[u8; 48]) -> Result<Self, BlsError> {
        let pk = BlstPk::key_validate(bytes.as_slice()).map_err(BlsError::from)?;
        Ok(Self(pk))
    }

    /// Compressed 48-byte serialization.
    pub fn serialize(&self) -> [u8; 48] {
        self.0.compress()
    }

    /// Borrow the inner blst public key (crate-internal).
    pub(crate) fn inner(&self) -> &BlstPk {
        &self.0
    }

    /// Construct from an aggregate public key (already validated constituents).
    pub fn from_aggregate(agg: &AggregatePublicKey) -> Self {
        Self(BlstPk::from_aggregate(&agg.0))
    }
}

/// Validated BLS signature (96-byte compressed G2).
#[derive(Clone, Copy)]
pub struct Signature(BlstSig);

impl std::fmt::Debug for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.serialize();
        write!(f, "Signature(0x")?;
        for b in bytes.iter().take(4) {
            write!(f, "{b:02x}")?;
        }
        write!(f, "…)")
    }
}

impl PartialEq for Signature {
    fn eq(&self, other: &Self) -> bool {
        self.serialize() == other.serialize()
    }
}

impl Eq for Signature {}

impl Signature {
    /// Deserialize and **validate** a compressed signature
    /// (`sig_validate(sig_infcheck = true)`).
    ///
    /// This is the only call site for `sig_validate` in the crate. Infinity is
    /// rejected here; the empty-sync-committee path uses [`Signature::infinity`].
    pub fn deserialize(bytes: &[u8; 96]) -> Result<Self, BlsError> {
        let sig = BlstSig::sig_validate(bytes.as_slice(), true).map_err(BlsError::from)?;
        Ok(Self(sig))
    }

    /// Compressed G2 point-at-infinity (not accepted by [`Self::deserialize`]).
    ///
    /// The fixed [`INFINITY_SIGNATURE`] encoding is part of the BLS12-381
    /// ciphersuite and is always accepted by blst's byte decoder.
    #[allow(clippy::expect_used)] // constant encoding; failure would be a blst regression
    pub fn infinity() -> Self {
        Self(
            BlstSig::from_bytes(INFINITY_SIGNATURE.as_slice())
                .expect("INFINITY_SIGNATURE is a valid compressed G2 infinity encoding"),
        )
    }

    /// Whether this signature is the G2 point at infinity.
    pub fn is_infinity(&self) -> bool {
        self.serialize() == INFINITY_SIGNATURE
    }

    /// Compressed 96-byte serialization.
    pub fn serialize(&self) -> [u8; 96] {
        self.0.compress()
    }

    pub(crate) fn inner(&self) -> &BlstSig {
        &self.0
    }

    /// Construct from an aggregate signature.
    pub fn from_aggregate(agg: &AggregateSignature) -> Self {
        Self(agg.0.to_signature())
    }
}

/// Aggregate public key (projective).
#[derive(Clone, Copy, Debug)]
pub struct AggregatePublicKey(BlstAggPk);

impl AggregatePublicKey {
    /// Aggregate already-validated public keys.
    pub fn aggregate(pks: &[&PublicKey]) -> Result<Self, BlsError> {
        let refs: Vec<&BlstPk> = pks.iter().map(|p| p.inner()).collect();
        let agg = BlstAggPk::aggregate(&refs, false).map_err(BlsError::from)?;
        Ok(Self(agg))
    }

    /// Convert to an affine [`PublicKey`].
    pub fn to_public_key(&self) -> PublicKey {
        PublicKey(self.0.to_public_key())
    }
}

/// Aggregate signature (projective).
#[derive(Clone, Copy, Debug)]
pub struct AggregateSignature(BlstAggSig);

impl AggregateSignature {
    /// Aggregate already-validated signatures (no extra group check).
    pub fn aggregate(sigs: &[&Signature]) -> Result<Self, BlsError> {
        let refs: Vec<&BlstSig> = sigs.iter().map(|s| s.inner()).collect();
        let agg = BlstAggSig::aggregate(&refs, false).map_err(BlsError::from)?;
        Ok(Self(agg))
    }

    /// Start an aggregate from a single signature.
    pub fn from_signature(sig: &Signature) -> Self {
        Self(BlstAggSig::from_signature(sig.inner()))
    }

    /// Add a signature into this aggregate.
    pub fn add_signature(&mut self, sig: &Signature) -> Result<(), BlsError> {
        self.0
            .add_signature(sig.inner(), false)
            .map_err(BlsError::from)
    }

    /// Convert to an affine [`Signature`].
    pub fn to_signature(&self) -> Signature {
        Signature(self.0.to_signature())
    }
}

/// Aggregate public keys into one affine key.
pub fn aggregate_public_keys(pks: &[&PublicKey]) -> Result<PublicKey, BlsError> {
    Ok(AggregatePublicKey::aggregate(pks)?.to_public_key())
}

/// Aggregate signatures into one affine signature.
pub fn aggregate_signatures(sigs: &[&Signature]) -> Result<Signature, BlsError> {
    Ok(AggregateSignature::aggregate(sigs)?.to_signature())
}

/// Single signature verification over a 32-byte message (signing root).
pub fn verify(pk: &PublicKey, msg: &[u8; 32], sig: &Signature) -> bool {
    success(sig.inner().verify(
        false, // already validated at deserialize
        msg.as_slice(),
        BLS_SIGNATURE_DST,
        &[],
        pk.inner(),
        false, // already validated at deserialize
    ))
}

/// Aggregate verification: one signature over distinct `(pk_i, msg_i)` pairs.
pub fn aggregate_verify(pks: &[PublicKey], msgs: &[[u8; 32]], sig: &Signature) -> bool {
    if pks.is_empty() || pks.len() != msgs.len() {
        return false;
    }
    let pk_refs: Vec<&BlstPk> = pks.iter().map(|p| p.inner()).collect();
    let msg_refs: Vec<&[u8]> = msgs.iter().map(|m| m.as_slice()).collect();
    success(sig.inner().aggregate_verify(
        false,
        &msg_refs,
        BLS_SIGNATURE_DST,
        &pk_refs,
        false,
    ))
}

/// Fast aggregate verify: many pubkeys, one shared message, one aggregate sig.
pub fn fast_aggregate_verify(pks: &[PublicKey], msg: &[u8; 32], sig: &Signature) -> bool {
    if pks.is_empty() {
        return false;
    }
    let pk_refs: Vec<&BlstPk> = pks.iter().map(|p| p.inner()).collect();
    success(
        sig.inner()
            .fast_aggregate_verify(false, msg.as_slice(), BLS_SIGNATURE_DST, &pk_refs),
    )
}

/// Ethereum variant of fast aggregate verify.
///
/// Returns `true` for an empty participant set with the infinity signature —
/// plain [`fast_aggregate_verify`] returns `false` in that case. Used by
/// `process_sync_aggregate`.
pub fn eth_fast_aggregate_verify(pks: &[PublicKey], msg: &[u8; 32], sig: &Signature) -> bool {
    if pks.is_empty() {
        return sig.is_infinity();
    }
    fast_aggregate_verify(pks, msg, sig)
}

// ---------------------------------------------------------------------------
// Tooling / generator signing surface (CC-2Ja)
// ---------------------------------------------------------------------------
//
// Production consensus path remains verify-only. Keygen + sign live here for
// offline generators (devnet-gen) and tests. Gated by feature `signing` or
// always available under `cfg(test)`.

/// BLS secret key for offline keygen / signing (tooling surface).
///
/// Not used by the live verify path. Available under feature `signing` or in
/// unit tests.
#[cfg(any(test, feature = "signing"))]
#[derive(Clone)]
pub struct SecretKey(BlstSk);

#[cfg(any(test, feature = "signing"))]
impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(..)")
    }
}

#[cfg(any(test, feature = "signing"))]
impl SecretKey {
    /// Derive a secret key from 32 bytes of IKM (`blst` `key_gen`).
    ///
    /// # Errors
    ///
    /// Returns [`BlsError`] when `blst` rejects the IKM.
    pub fn from_ikm(ikm: &[u8; 32]) -> Result<Self, BlsError> {
        let sk = BlstSk::key_gen(ikm.as_slice(), &[]).map_err(BlsError::from)?;
        Ok(Self(sk))
    }

    /// Deterministic secret key from a seed and validator index (devnet keys).
    ///
    /// Mixes `seed || index_le` through SHA-256-style fixed hash then
    /// [`Self::from_ikm`]. Uses [`crate::hash::hash_fixed`] so the monorepo
    /// keeps one hash stack for key material.
    pub fn from_seed_index(seed: &[u8; 32], index: u64) -> Result<Self, BlsError> {
        let mut material = [0u8; 40];
        material[..32].copy_from_slice(seed);
        material[32..].copy_from_slice(&index.to_le_bytes());
        let ikm = crate::hash::hash_fixed(&material);
        Self::from_ikm(&ikm)
    }

    /// Corresponding public key.
    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.0.sk_to_pk())
    }

    /// Sign a 32-byte message (signing root) under the Ethereum BLS DST.
    pub fn sign(&self, msg: &[u8; 32]) -> Signature {
        Signature(self.0.sign(msg.as_slice(), BLS_SIGNATURE_DST, &[]))
    }

    /// Serialize the secret key to 32 bytes (big-endian scalar).
    pub fn serialize(&self) -> [u8; 32] {
        self.0.to_bytes()
    }

    /// Deserialize a secret key from 32 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`BlsError`] on invalid encoding.
    pub fn deserialize(bytes: &[u8; 32]) -> Result<Self, BlsError> {
        let sk = BlstSk::from_bytes(bytes.as_slice()).map_err(BlsError::from)?;
        Ok(Self(sk))
    }
}

/// Test-only alias kept for existing `pub(crate)` call sites.
#[cfg(test)]
pub(crate) mod test_utils {
    pub(crate) use super::SecretKey;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::SecretKey;
    use super::*;

    #[test]
    fn deserialize_rejects_all_zero_pubkey() {
        let err = PublicKey::deserialize(&[0u8; 48]).unwrap_err();
        assert!(matches!(
            err,
            BlsError::BadEncoding | BlsError::Infinity | BlsError::PointNotInGroup | BlsError::Other(_)
        ));
    }

    #[test]
    fn deserialize_rejects_all_zero_signature() {
        let err = Signature::deserialize(&[0u8; 96]).unwrap_err();
        assert!(matches!(
            err,
            BlsError::BadEncoding | BlsError::Infinity | BlsError::PointNotInGroup | BlsError::Other(_)
        ));
    }

    #[test]
    fn deserialize_rejects_infinity_signature() {
        let err = Signature::deserialize(&INFINITY_SIGNATURE).unwrap_err();
        assert!(matches!(err, BlsError::Infinity | BlsError::Other(_)));
    }

    #[test]
    fn infinity_signature_roundtrip_flag() {
        let sig = Signature::infinity();
        assert!(sig.is_infinity());
        assert_eq!(sig.serialize(), INFINITY_SIGNATURE);
    }

    #[test]
    fn eth_fast_aggregate_empty_infinity_true_plain_false() {
        let msg = [7u8; 32];
        let inf = Signature::infinity();
        assert!(eth_fast_aggregate_verify(&[], &msg, &inf));
        assert!(!fast_aggregate_verify(&[], &msg, &inf));
    }

    #[test]
    fn verify_and_aggregate_roundtrip() {
        let sk1 = SecretKey::from_ikm(&[1u8; 32]).unwrap();
        let sk2 = SecretKey::from_ikm(&[2u8; 32]).unwrap();
        let pk1 = sk1.public_key();
        let pk2 = sk2.public_key();
        let m1 = [10u8; 32];
        let m2 = [20u8; 32];
        let s1 = sk1.sign(&m1);
        let s2 = sk2.sign(&m2);
        assert!(verify(&pk1, &m1, &s1));
        assert!(!verify(&pk1, &m2, &s1));

        let agg = aggregate_signatures(&[&s1, &s2]).unwrap();
        assert!(aggregate_verify(&[pk1, pk2], &[m1, m2], &agg));

        // Fast aggregate: both sign same message.
        let s1b = sk1.sign(&m1);
        let s2b = sk2.sign(&m1);
        let fast = aggregate_signatures(&[&s1b, &s2b]).unwrap();
        assert!(fast_aggregate_verify(&[pk1, pk2], &m1, &fast));
        assert!(eth_fast_aggregate_verify(&[pk1, pk2], &m1, &fast));
    }

    #[test]
    fn altered_byte_fails_verify() {
        let sk = SecretKey::from_ikm(&[3u8; 32]).unwrap();
        let pk = sk.public_key();
        let msg = [9u8; 32];
        let mut bytes = sk.sign(&msg).serialize();
        bytes[10] ^= 0xff;
        // May fail deserialize (not in group) or verify — either is correct.
        if let Ok(sig) = Signature::deserialize(&bytes) {
            assert!(!verify(&pk, &msg, &sig));
        }
    }

    #[test]
    fn seed_index_is_deterministic() {
        let seed = [0x42u8; 32];
        let a = SecretKey::from_seed_index(&seed, 7).unwrap();
        let b = SecretKey::from_seed_index(&seed, 7).unwrap();
        let c = SecretKey::from_seed_index(&seed, 8).unwrap();
        assert_eq!(a.public_key(), b.public_key());
        assert_ne!(a.public_key(), c.public_key());
        assert_eq!(a.serialize(), b.serialize());
    }
}
