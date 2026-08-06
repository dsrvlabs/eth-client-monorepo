//! Block-level [`SignatureSet`] assembly and verification strategy
//! (Architecture §5.2).
//!
//! Handlers (RANDAO, operations, sync aggregate) push additional triples as
//! they land in CC-12b–d. This module owns the block-proposer entry and the
//! batch → individual re-attribution path.

use cc_crypto::{
    compute_signing_root, get_domain, verify, PublicKey, Signature, SignatureSet,
    DOMAIN_BEACON_PROPOSER, DOMAIN_RANDAO,
};
use cc_types::preset::Preset;
use cc_types::primitives::BlsPublicKey;
use cc_types::{BeaconState, SignedBeaconBlock};

use crate::error::{BlockError, SignatureKind};
use crate::helpers::accessors::get_beacon_proposer_index;
use crate::helpers::misc::compute_epoch_at_slot;
use crate::BlockSignatureStrategy;

/// One labelled signature triple for individual verification / re-attribution.
#[derive(Clone, Debug)]
pub struct LabelledSignature {
    /// Which signature this is.
    pub kind: SignatureKind,
    /// Validated public key.
    pub pubkey: PublicKey,
    /// 32-byte signing root.
    pub message: [u8; 32],
    /// Validated signature.
    pub signature: Signature,
}

/// Accumulated labelled signatures for a block (proposer first; handlers append).
#[derive(Clone, Debug, Default)]
pub struct BlockSignatureSet {
    entries: Vec<LabelledSignature>,
}

impl BlockSignatureSet {
    /// Empty set.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Number of triples.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Push a labelled triple.
    pub fn push(&mut self, entry: LabelledSignature) {
        self.entries.push(entry);
    }

    /// Borrow entries.
    pub fn entries(&self) -> &[LabelledSignature] {
        &self.entries
    }

    /// Build a crypto [`SignatureSet`] for batch verification.
    pub fn to_signature_set(&self) -> SignatureSet {
        let mut set = SignatureSet::new();
        for e in &self.entries {
            set.push(e.pubkey, e.message, e.signature);
        }
        set
    }

    /// Verify according to `strategy`.
    ///
    /// - [`BlockSignatureStrategy::NoVerification`]: always ok.
    /// - [`BlockSignatureStrategy::VerifyIndividual`]: first failure names `which`.
    /// - [`BlockSignatureStrategy::VerifyBatch`]: one batch check; on failure,
    ///   re-run individually and return [`BlockError::InvalidSignature`].
    pub fn verify(&self, strategy: BlockSignatureStrategy) -> Result<(), BlockError> {
        match strategy {
            BlockSignatureStrategy::NoVerification => Ok(()),
            BlockSignatureStrategy::VerifyIndividual => self.verify_individual(),
            BlockSignatureStrategy::VerifyBatch => {
                if self.to_signature_set().verify() {
                    return Ok(());
                }
                // Re-attribute.
                self.verify_individual()
            }
        }
    }

    fn verify_individual(&self) -> Result<(), BlockError> {
        for e in &self.entries {
            if !verify(&e.pubkey, &e.message, &e.signature) {
                return Err(BlockError::InvalidSignature { which: e.kind });
            }
        }
        Ok(())
    }
}

/// Deserialize and validate a **state-resident** BLS public key (registry).
///
/// Failures classify as [`BlockError::StateBlsMaterial`] →
/// [`crate::GossipClass::Internal`] (SEC-12a-1): do not descore peers for our
/// registry being unusable.
pub fn decode_state_pubkey(pk: &BlsPublicKey) -> Result<PublicKey, BlockError> {
    PublicKey::deserialize(pk.as_array())
        .map_err(|e| BlockError::StateBlsMaterial(format!("pubkey: {e}")))
}

/// Deserialize and validate a **block-carried** BLS public key (deposits, …).
///
/// Failures classify as [`BlockError::BlsMaterial`] →
/// [`crate::GossipClass::Reject`].
pub fn decode_block_pubkey(pk: &BlsPublicKey) -> Result<PublicKey, BlockError> {
    PublicKey::deserialize(pk.as_array())
        .map_err(|e| BlockError::BlsMaterial(format!("pubkey: {e}")))
}

/// Deserialize and validate a **block-carried** BLS signature.
///
/// Failures classify as [`BlockError::BlsMaterial`] →
/// [`crate::GossipClass::Reject`].
pub fn decode_signature(sig: &cc_types::primitives::BlsSignature) -> Result<Signature, BlockError> {
    Signature::deserialize(sig.as_array())
        .map_err(|e| BlockError::BlsMaterial(format!("signature: {e}")))
}

/// Alias kept for call sites that mean “registry key” (same as [`decode_state_pubkey`]).
#[inline]
pub fn decode_pubkey(pk: &BlsPublicKey) -> Result<PublicKey, BlockError> {
    decode_state_pubkey(pk)
}

/// Push the block-proposer signature into `set`.
pub fn push_block_proposer_signature<P: Preset>(
    set: &mut BlockSignatureSet,
    state: &BeaconState<P>,
    signed_block: &SignedBeaconBlock<P>,
) -> Result<(), BlockError> {
    let block = &signed_block.message;
    let idx = block.proposer_index.as_u64() as usize;
    let validator = state
        .validators_get(idx)
        .ok_or(BlockError::ProposerUnknown {
            index: block.proposer_index,
            len: state.validators_len(),
        })?;

    let epoch = compute_epoch_at_slot::<P>(block.slot);
    let domain = get_domain(
        &state.fork(),
        DOMAIN_BEACON_PROPOSER,
        Some(epoch),
        state.genesis_validators_root(),
    );
    let message = *compute_signing_root(block, domain).as_array();
    // Registry key is state-resident → StateBlsMaterial / Internal on failure.
    let pubkey = decode_state_pubkey(&validator.pubkey)?;
    // Signature bytes are block-carried → BlsMaterial / Reject on failure.
    let signature = decode_signature(&signed_block.signature)?;

    set.push(LabelledSignature {
        kind: SignatureKind::BlockProposer,
        pubkey,
        message,
        signature,
    });
    Ok(())
}

/// Push the RANDAO reveal into `set` (CC-12b).
///
/// Signing root is `compute_signing_root(epoch, DOMAIN_RANDAO)`. Pubkey is the
/// proposer from state (state-resident → [`BlockError::StateBlsMaterial`]).
/// Signature bytes are block-carried → [`BlockError::BlsMaterial`].
pub fn push_randao_signature<P: Preset>(
    set: &mut BlockSignatureSet,
    state: &BeaconState<P>,
    signed_block: &SignedBeaconBlock<P>,
) -> Result<(), BlockError> {
    let block = &signed_block.message;
    // Spec uses get_beacon_proposer_index; after header checks this equals block.proposer_index.
    let proposer_index = get_beacon_proposer_index(state).unwrap_or(block.proposer_index);
    let idx = proposer_index.as_u64() as usize;
    let validator = state
        .validators_get(idx)
        .ok_or(BlockError::ProposerUnknown {
            index: proposer_index,
            len: state.validators_len(),
        })?;

    let epoch = compute_epoch_at_slot::<P>(block.slot);
    let domain = get_domain(
        &state.fork(),
        DOMAIN_RANDAO,
        Some(epoch),
        state.genesis_validators_root(),
    );
    let message = *compute_signing_root(&epoch, domain).as_array();
    let pubkey = decode_state_pubkey(&validator.pubkey)?;
    let signature = decode_signature(&block.body.randao_reveal)?;

    set.push(LabelledSignature {
        kind: SignatureKind::Randao,
        pubkey,
        message,
        signature,
    });
    Ok(())
}

/// Assemble the block-level signature set (proposer + RANDAO; more in CC-12c–d)
/// and verify under `strategy`.
pub fn verify_block_signatures<P: Preset>(
    state: &BeaconState<P>,
    signed_block: &SignedBeaconBlock<P>,
    strategy: BlockSignatureStrategy,
) -> Result<(), BlockError> {
    if matches!(strategy, BlockSignatureStrategy::NoVerification) {
        return Ok(());
    }
    let mut set = BlockSignatureSet::new();
    push_block_proposer_signature(&mut set, state, signed_block)?;
    push_randao_signature(&mut set, state, signed_block)?;
    set.verify(strategy)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_crypto::{compute_signing_root, BLS_SIGNATURE_DST};
    use cc_types::primitives::Root;

    /// Local signing helper (cc-crypto keeps `SecretKey` crate-private).
    struct TestSk(blst::min_pk::SecretKey);

    impl TestSk {
        fn from_ikm(ikm: &[u8; 32]) -> Self {
            Self(blst::min_pk::SecretKey::key_gen(ikm.as_slice(), &[]).expect("key_gen"))
        }

        fn public_key(&self) -> PublicKey {
            let bytes = self.0.sk_to_pk().compress();
            PublicKey::deserialize(&bytes).expect("pk")
        }

        fn sign(&self, msg: &[u8; 32]) -> Signature {
            let sig = self.0.sign(msg.as_slice(), BLS_SIGNATURE_DST, &[]);
            Signature::deserialize(&sig.compress()).expect("sig")
        }
    }

    fn labelled(sk: &TestSk, msg: [u8; 32], kind: SignatureKind) -> LabelledSignature {
        LabelledSignature {
            kind,
            pubkey: sk.public_key(),
            message: msg,
            signature: sk.sign(&msg),
        }
    }

    #[test]
    fn no_verification_always_ok() {
        let mut set = BlockSignatureSet::new();
        let sk = TestSk::from_ikm(&[1u8; 32]);
        // Deliberately wrong signature material is irrelevant under NoVerification.
        set.push(LabelledSignature {
            kind: SignatureKind::BlockProposer,
            pubkey: sk.public_key(),
            message: [9u8; 32],
            signature: sk.sign(&[0u8; 32]),
        });
        set.verify(BlockSignatureStrategy::NoVerification)
            .unwrap();
    }

    #[test]
    fn verify_individual_names_which() {
        let sk1 = TestSk::from_ikm(&[11u8; 32]);
        let sk2 = TestSk::from_ikm(&[12u8; 32]);
        let m1 = [1u8; 32];
        let m2 = [2u8; 32];
        let mut set = BlockSignatureSet::new();
        set.push(labelled(&sk1, m1, SignatureKind::BlockProposer));
        // Wrong signature for second entry.
        set.push(LabelledSignature {
            kind: SignatureKind::Randao,
            pubkey: sk2.public_key(),
            message: m2,
            signature: sk1.sign(&m2),
        });
        let err = set
            .verify(BlockSignatureStrategy::VerifyIndividual)
            .unwrap_err();
        match err {
            BlockError::InvalidSignature {
                which: SignatureKind::Randao,
            } => {}
            other => panic!("expected Randao, got {other:?}"),
        }
    }

    #[test]
    fn batch_failure_reruns_individual_and_names_which() {
        let sk1 = TestSk::from_ikm(&[21u8; 32]);
        let sk2 = TestSk::from_ikm(&[22u8; 32]);
        let m1 = [31u8; 32];
        let m2 = [32u8; 32];
        let mut set = BlockSignatureSet::new();
        set.push(labelled(&sk1, m1, SignatureKind::BlockProposer));
        set.push(LabelledSignature {
            kind: SignatureKind::Attestation(0),
            pubkey: sk2.public_key(),
            message: m2,
            signature: sk1.sign(&m2), // wrong
        });
        // Batch must fail, then individual names Attestation(0).
        let err = set
            .verify(BlockSignatureStrategy::VerifyBatch)
            .unwrap_err();
        match err {
            BlockError::InvalidSignature {
                which: SignatureKind::Attestation(0),
            } => {}
            other => panic!("expected Attestation(0), got {other:?}"),
        }
    }

    #[test]
    fn batch_valid_set_ok() {
        let sk1 = TestSk::from_ikm(&[41u8; 32]);
        let sk2 = TestSk::from_ikm(&[42u8; 32]);
        let mut set = BlockSignatureSet::new();
        set.push(labelled(&sk1, [1u8; 32], SignatureKind::BlockProposer));
        set.push(labelled(&sk2, [2u8; 32], SignatureKind::Randao));
        set.verify(BlockSignatureStrategy::VerifyBatch).unwrap();
    }

    #[test]
    fn signing_root_uses_domain() {
        let domain = cc_crypto::compute_domain(DOMAIN_BEACON_PROPOSER, None, None);
        let root = Root::from_array([7u8; 32]);
        let a = compute_signing_root(&root, domain);
        let b = compute_signing_root(&root, domain);
        assert_eq!(a, b);
    }
}
