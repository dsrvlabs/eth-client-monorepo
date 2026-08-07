//! Batch signature verification with fresh random coefficients (Architecture §4.1).
//!
//! `SignatureSet` accumulates `(pubkeys, message, signature)` triples across a
//! whole block and verifies them in one
//! `verify_multiple_aggregate_signatures` call. Coefficients are 64-bit
//! CSPRNG scalars drawn **fresh per call**; reusing them makes forged batches
//! accept.

use super::{
    aggregate_public_keys, BlsError, PublicKey, Signature, BLS_SIGNATURE_DST,
};
use blst::{blst_scalar, min_pk::Signature as BlstSig, BLST_ERROR};

/// Source of 64-bit batch-verification coefficients.
///
/// Production code uses [`OsRandom`]. Tests inject a capturing or fixed source
/// to assert freshness and the reused-coefficient forgery failure mode.
pub trait RandomScalarSource {
    /// Fill `dest` with cryptographically strong random bytes (8 bytes → one
    /// 64-bit coefficient). Must not return all-zero (blst multiplies by the
    /// scalar; zero would drop a term).
    fn fill_nonzero_u64(&mut self) -> Result<u64, BlsError>;
}

/// Operating-system CSPRNG (`getrandom`).
#[derive(Debug, Default, Clone, Copy)]
pub struct OsRandom;

impl RandomScalarSource for OsRandom {
    fn fill_nonzero_u64(&mut self) -> Result<u64, BlsError> {
        let mut bytes = [0u8; 8];
        // Reject zero so the term is never dropped from the linear combination.
        for _ in 0..16 {
            getrandom::fill(&mut bytes).map_err(|_| BlsError::Other(BLST_ERROR::BLST_BAD_ENCODING))?;
            let v = u64::from_le_bytes(bytes);
            if v != 0 {
                return Ok(v);
            }
        }
        Err(BlsError::Other(BLST_ERROR::BLST_BAD_ENCODING))
    }
}

/// One `(pubkeys, message, signature)` entry in a batch.
#[derive(Clone, Debug)]
struct SignatureSetEntry {
    /// One or more pubkeys; multiple keys are aggregated before the batch call
    /// (attestation committees / sync aggregates).
    pubkeys: Vec<PublicKey>,
    message: [u8; 32],
    signature: Signature,
}

/// Builder for block-level batch verification.
///
/// Accumulates triples across a whole block — block signature, RANDAO reveal,
/// each attestation, slashings, exits, BLS-to-execution changes, sync aggregate
/// — and verifies them in a single pairing-product check.
#[derive(Clone, Debug, Default)]
pub struct SignatureSet {
    entries: Vec<SignatureSetEntry>,
}

impl SignatureSet {
    /// Empty set.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Number of accumulated triples.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Push a single-pubkey signature (block proposal, RANDAO, exit, …).
    pub fn push(&mut self, pubkey: PublicKey, message: [u8; 32], signature: Signature) {
        self.entries.push(SignatureSetEntry {
            pubkeys: vec![pubkey],
            message,
            signature,
        });
    }

    /// Push a multi-pubkey aggregate signature (attestation, sync aggregate).
    pub fn push_aggregate(
        &mut self,
        pubkeys: Vec<PublicKey>,
        message: [u8; 32],
        signature: Signature,
    ) {
        self.entries.push(SignatureSetEntry {
            pubkeys,
            message,
            signature,
        });
    }

    /// Verify the whole set with fresh OS randomness.
    pub fn verify(&self) -> bool {
        self.verify_with_rng(&mut OsRandom)
    }

    /// Verify with an injected coefficient source (tests + deterministic harnesses).
    pub fn verify_with_rng<R: RandomScalarSource>(&self, rng: &mut R) -> bool {
        match self.verify_with_rng_inner(rng) {
            Ok((ok, _)) => ok,
            Err(_) => false,
        }
    }

    /// Like [`Self::verify_with_rng`] but also returns the coefficient vector
    /// drawn for this call (test seam for freshness assertions).
    pub fn verify_with_rng_recorded<R: RandomScalarSource>(
        &self,
        rng: &mut R,
    ) -> Result<(bool, Vec<u64>), BlsError> {
        self.verify_with_rng_inner(rng)
    }

    fn verify_with_rng_inner<R: RandomScalarSource>(
        &self,
        rng: &mut R,
    ) -> Result<(bool, Vec<u64>), BlsError> {
        if self.entries.is_empty() {
            // Empty batch is vacuously valid (no signatures to check).
            return Ok((true, Vec::new()));
        }

        // Aggregate multi-key entries into a single affine pk per triple.
        let mut agg_pks: Vec<PublicKey> = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            if entry.pubkeys.is_empty() {
                return Ok((false, Vec::new()));
            }
            if entry.pubkeys.len() == 1 {
                agg_pks.push(entry.pubkeys[0]);
            } else {
                let refs: Vec<&PublicKey> = entry.pubkeys.iter().collect();
                agg_pks.push(aggregate_public_keys(&refs)?);
            }
        }

        let mut coeffs = Vec::with_capacity(self.entries.len());
        let mut rands: Vec<blst_scalar> = Vec::with_capacity(self.entries.len());
        for _ in 0..self.entries.len() {
            let v = rng.fill_nonzero_u64()?;
            coeffs.push(v);
            // 64-bit coefficient in the low 8 bytes of the scalar (little-endian);
            // remaining bytes stay zero. Matches `blst_scalar_from_uint64([v,0,0,0])`
            // without requiring `unsafe`.
            let mut rand_i = blst_scalar::default();
            rand_i.b[..8].copy_from_slice(&v.to_le_bytes());
            rands.push(rand_i);
        }

        let pk_refs: Vec<&blst::min_pk::PublicKey> =
            agg_pks.iter().map(PublicKey::inner).collect();
        let sig_refs: Vec<&BlstSig> = self
            .entries
            .iter()
            .map(|e| e.signature.inner())
            .collect();
        let msg_refs: Vec<&[u8]> = self
            .entries
            .iter()
            .map(|e| e.message.as_slice())
            .collect();

        // pks_validate=false, sigs_groupcheck=false: deserialization already
        // validated. rand_bits=64 matches the 64-bit coefficients.
        let err = BlstSig::verify_multiple_aggregate_signatures(
            &msg_refs,
            BLS_SIGNATURE_DST,
            &pk_refs,
            false,
            &sig_refs,
            false,
            &rands,
            64,
        );
        Ok((err == BLST_ERROR::BLST_SUCCESS, coeffs))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::super::{AggregateSignature, Signature};
    use super::*;
    use crate::bls::test_utils::SecretKey;

    /// Capturing RNG that yields a predetermined sequence, then records draws.
    struct ScriptedRng {
        script: Vec<u64>,
        idx: usize,
        drawn: Vec<u64>,
    }

    impl ScriptedRng {
        fn new(script: Vec<u64>) -> Self {
            Self {
                script,
                idx: 0,
                drawn: Vec::new(),
            }
        }
    }

    impl RandomScalarSource for ScriptedRng {
        fn fill_nonzero_u64(&mut self) -> Result<u64, BlsError> {
            let v = if self.idx < self.script.len() {
                let v = self.script[self.idx];
                self.idx += 1;
                v
            } else {
                // Fall back to OS randomness when the script is exhausted.
                OsRandom.fill_nonzero_u64()?
            };
            let v = if v == 0 { 1 } else { v };
            self.drawn.push(v);
            Ok(v)
        }
    }

    #[test]
    fn batch_verify_valid_set() {
        let sk1 = SecretKey::from_ikm(&[11u8; 32]).unwrap();
        let sk2 = SecretKey::from_ikm(&[12u8; 32]).unwrap();
        let m1 = [1u8; 32];
        let m2 = [2u8; 32];
        let mut set = SignatureSet::new();
        set.push(sk1.public_key(), m1, sk1.sign(&m1));
        set.push(sk2.public_key(), m2, sk2.sign(&m2));
        assert!(set.verify());
    }

    #[test]
    fn batch_fresh_randomness_differs_across_calls() {
        let sk = SecretKey::from_ikm(&[13u8; 32]).unwrap();
        let msg = [3u8; 32];
        let mut set = SignatureSet::new();
        set.push(sk.public_key(), msg, sk.sign(&msg));

        // Two consecutive OS draws must produce different coefficient vectors
        // (collision probability 2^-64 per entry).
        let (_, c1) = set
            .verify_with_rng_recorded(&mut OsRandom)
            .expect("rng");
        let (_, c2) = set
            .verify_with_rng_recorded(&mut OsRandom)
            .expect("rng");
        assert_ne!(c1, c2, "batch coefficients must be fresh per call");
    }

    #[test]
    fn forged_batch_passes_reused_unit_coeffs_fails_fresh() {
        // Algebraic forgery for coefficients (1, 1):
        //   s1' = s1 + s2,  s2' = infinity
        // verifies under r1 = r2 = 1 but not under independent random r_i.
        let sk1 = SecretKey::from_ikm(&[21u8; 32]).unwrap();
        let sk2 = SecretKey::from_ikm(&[22u8; 32]).unwrap();
        let m1 = [31u8; 32];
        let m2 = [32u8; 32];
        let s1 = sk1.sign(&m1);
        let s2 = sk2.sign(&m2);
        let s1_forged = AggregateSignature::aggregate(&[&s1, &s2])
            .unwrap()
            .to_signature();
        let s2_forged = Signature::infinity();

        let mut forged = SignatureSet::new();
        forged.push(sk1.public_key(), m1, s1_forged);
        forged.push(sk2.public_key(), m2, s2_forged);

        // Reused unit coefficients: forgery accepts.
        let mut unit = ScriptedRng::new(vec![1, 1]);
        let (ok_unit, coeffs) = forged.verify_with_rng_recorded(&mut unit).unwrap();
        assert_eq!(coeffs, vec![1, 1]);
        assert!(
            ok_unit,
            "forgery must verify under reused unit coefficients"
        );

        // Fresh randomness: forgery rejected.
        assert!(
            !forged.verify(),
            "forgery must fail under fresh random coefficients"
        );
    }

    #[test]
    fn altered_entry_fails_batch() {
        let sk1 = SecretKey::from_ikm(&[41u8; 32]).unwrap();
        let sk2 = SecretKey::from_ikm(&[42u8; 32]).unwrap();
        let m1 = [51u8; 32];
        let m2 = [52u8; 32];
        let mut set = SignatureSet::new();
        set.push(sk1.public_key(), m1, sk1.sign(&m1));
        // Wrong signature for the second entry.
        set.push(sk2.public_key(), m2, sk1.sign(&m2));
        assert!(!set.verify());
    }
}
