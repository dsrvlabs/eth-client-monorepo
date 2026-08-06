//! Block-level [`SignatureSet`] assembly and verification strategy
//! (Architecture §5.2).
//!
//! Collects proposer, RANDAO, and CC-12c operation signatures into one set.
//! `process_block` / `process_operations` then run with signature verification
//! skipped (already verified here under VerifyBatch / VerifyIndividual).

use cc_crypto::{
    compute_domain, compute_signing_root, fast_aggregate_verify, get_domain, verify, PublicKey,
    Signature, SignatureSet, DOMAIN_BEACON_ATTESTER, DOMAIN_BEACON_PROPOSER,
    DOMAIN_BLS_TO_EXECUTION_CHANGE, DOMAIN_RANDAO, DOMAIN_VOLUNTARY_EXIT,
};
use cc_types::config::ChainConfig;
use cc_types::operations::IndexedAttestation;
use cc_types::preset::Preset;
use cc_types::primitives::BlsPublicKey;
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};

use crate::error::{BlockError, OperationError, SignatureKind};
use crate::helpers::accessors::{get_beacon_proposer_index, get_indexed_attestation};
use crate::helpers::misc::compute_epoch_at_slot;
use crate::BlockSignatureStrategy;

/// One labelled signature triple for individual verification / re-attribution.
///
/// `pubkeys` is length 1 for single-signer ops (proposer, RANDAO, exit, …) and
/// length N for fast-aggregate ops (attestation, attester slashing).
#[derive(Clone, Debug)]
pub struct LabelledSignature {
    /// Which signature this is.
    pub kind: SignatureKind,
    /// Validated public key(s); aggregated before batch verify when `len > 1`.
    pub pubkeys: Vec<PublicKey>,
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
            if e.pubkeys.len() == 1 {
                set.push(e.pubkeys[0], e.message, e.signature);
            } else {
                set.push_aggregate(e.pubkeys.clone(), e.message, e.signature);
            }
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
            let ok = if e.pubkeys.len() == 1 {
                verify(&e.pubkeys[0], &e.message, &e.signature)
            } else {
                fast_aggregate_verify(&e.pubkeys, &e.message, &e.signature)
            };
            if !ok {
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
        pubkeys: vec![pubkey],
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
        pubkeys: vec![pubkey],
        message,
        signature,
    });
    Ok(())
}

/// Push CC-12c operation signatures from `block.body` into `set`.
///
/// Order: proposer slashings (2 headers each), attester slashings (2 indexed
/// attests each), attestations, voluntary exits, bls_to_execution_changes.
/// Deposits are not block-batch signatures (PoP is optional inside apply_deposit).
pub fn push_operation_signatures<P: Preset>(
    set: &mut BlockSignatureSet,
    state: &BeaconState<P>,
    block: &BeaconBlock<P>,
    config: &ChainConfig,
) -> Result<(), BlockError> {
    let body = &block.body;

    for (i, slashing) in body.proposer_slashings.iter().enumerate() {
        push_proposer_slashing_signatures(set, state, slashing, i)?;
    }
    for (i, slashing) in body.attester_slashings.iter().enumerate() {
        push_indexed_attestation_signature(
            set,
            state,
            &slashing.attestation_1,
            SignatureKind::AttesterSlashing(i),
        )?;
        push_indexed_attestation_signature(
            set,
            state,
            &slashing.attestation_2,
            SignatureKind::AttesterSlashing(i),
        )?;
    }
    for (i, attestation) in body.attestations.iter().enumerate() {
        // Build indexed form for committee-resolved pubkeys (Electra).
        let indexed = get_indexed_attestation(state, attestation)?;
        push_indexed_attestation_signature(
            set,
            state,
            &indexed,
            SignatureKind::Attestation(i),
        )?;
    }
    for (i, exit) in body.voluntary_exits.iter().enumerate() {
        push_voluntary_exit_signature(set, state, exit, config, i)?;
    }
    for (i, change) in body.bls_to_execution_changes.iter().enumerate() {
        push_bls_to_execution_change_signature(set, state, change, config, i)?;
    }
    Ok(())
}

fn push_proposer_slashing_signatures<P: Preset>(
    set: &mut BlockSignatureSet,
    state: &BeaconState<P>,
    slashing: &cc_types::operations::ProposerSlashing,
    op_index: usize,
) -> Result<(), BlockError> {
    let proposer_index = slashing.signed_header_1.message.proposer_index;
    let validator = state
        .validators_get(proposer_index.as_u64() as usize)
        .ok_or_else(|| {
            BlockError::InvalidOperation(OperationError::Invalid {
                op: "proposer_slashing",
                detail: format!("proposer index {} unknown", proposer_index.as_u64()),
            })
        })?;
    let pubkey = decode_state_pubkey(&validator.pubkey)?;

    for signed_header in [&slashing.signed_header_1, &slashing.signed_header_2] {
        let epoch = compute_epoch_at_slot::<P>(signed_header.message.slot);
        let domain = get_domain(
            &state.fork(),
            DOMAIN_BEACON_PROPOSER,
            Some(epoch),
            state.genesis_validators_root(),
        );
        let message = *compute_signing_root(&signed_header.message, domain).as_array();
        let signature = decode_signature(&signed_header.signature)?;
        set.push(LabelledSignature {
            kind: SignatureKind::ProposerSlashing(op_index),
            pubkeys: vec![pubkey],
            message,
            signature,
        });
    }
    Ok(())
}

fn push_indexed_attestation_signature<P: Preset>(
    set: &mut BlockSignatureSet,
    state: &BeaconState<P>,
    indexed: &IndexedAttestation<P>,
    kind: SignatureKind,
) -> Result<(), BlockError> {
    if indexed.attesting_indices.is_empty() {
        // Empty aggregate: nothing to batch-verify; process_* will reject.
        return Ok(());
    }
    let mut pubkeys = Vec::with_capacity(indexed.attesting_indices.len());
    for idx in indexed.attesting_indices.iter() {
        let v = state.validators_get(idx.as_u64() as usize).ok_or_else(|| {
            BlockError::InvalidOperation(OperationError::Invalid {
                op: "attestation",
                detail: format!("attesting index {} out of range", idx.as_u64()),
            })
        })?;
        pubkeys.push(decode_state_pubkey(&v.pubkey)?);
    }
    let domain = get_domain(
        &state.fork(),
        DOMAIN_BEACON_ATTESTER,
        Some(indexed.data.target.epoch),
        state.genesis_validators_root(),
    );
    let message = *compute_signing_root(&indexed.data, domain).as_array();
    let signature = decode_signature(&indexed.signature)?;
    set.push(LabelledSignature {
        kind,
        pubkeys,
        message,
        signature,
    });
    Ok(())
}

fn push_voluntary_exit_signature<P: Preset>(
    set: &mut BlockSignatureSet,
    state: &BeaconState<P>,
    signed: &cc_types::operations::SignedVoluntaryExit,
    config: &ChainConfig,
    op_index: usize,
) -> Result<(), BlockError> {
    let index = signed.message.validator_index;
    let validator = state.validators_get(index.as_u64() as usize).ok_or_else(|| {
        BlockError::InvalidOperation(OperationError::Invalid {
            op: "voluntary_exit",
            detail: format!("validator index {} out of range", index.as_u64()),
        })
    })?;
    let domain = compute_domain(
        DOMAIN_VOLUNTARY_EXIT,
        Some(config.capella_fork_version),
        Some(state.genesis_validators_root()),
    );
    let message = *compute_signing_root(&signed.message, domain).as_array();
    let pubkey = decode_state_pubkey(&validator.pubkey)?;
    let signature = decode_signature(&signed.signature)?;
    set.push(LabelledSignature {
        kind: SignatureKind::VoluntaryExit(op_index),
        pubkeys: vec![pubkey],
        message,
        signature,
    });
    Ok(())
}

fn push_bls_to_execution_change_signature<P: Preset>(
    set: &mut BlockSignatureSet,
    state: &BeaconState<P>,
    signed: &cc_types::operations::SignedBlsToExecutionChange,
    config: &ChainConfig,
    op_index: usize,
) -> Result<(), BlockError> {
    let domain = compute_domain(
        DOMAIN_BLS_TO_EXECUTION_CHANGE,
        Some(config.genesis_fork_version),
        Some(state.genesis_validators_root()),
    );
    let message = *compute_signing_root(&signed.message, domain).as_array();
    // from_bls_pubkey is message-carried → block material.
    let pubkey = decode_block_pubkey(&signed.message.from_bls_pubkey)?;
    let signature = decode_signature(&signed.signature)?;
    set.push(LabelledSignature {
        kind: SignatureKind::BlsToExecutionChange(op_index),
        pubkeys: vec![pubkey],
        message,
        signature,
    });
    Ok(())
}

/// Assemble the block-level signature set (proposer + RANDAO + CC-12c ops)
/// and verify under `strategy`.
///
/// `config` supplies Capella / genesis fork versions for exit and
/// bls-to-execution-change domains.
pub fn verify_block_signatures<P: Preset>(
    state: &BeaconState<P>,
    signed_block: &SignedBeaconBlock<P>,
    config: &ChainConfig,
    strategy: BlockSignatureStrategy,
) -> Result<(), BlockError> {
    if matches!(strategy, BlockSignatureStrategy::NoVerification) {
        return Ok(());
    }
    let mut set = BlockSignatureSet::new();
    push_block_proposer_signature(&mut set, state, signed_block)?;
    push_randao_signature(&mut set, state, signed_block)?;
    push_operation_signatures(&mut set, state, &signed_block.message, config)?;
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
            pubkeys: vec![sk.public_key()],
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
            pubkeys: vec![sk.public_key()],
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
            pubkeys: vec![sk2.public_key()],
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
            pubkeys: vec![sk2.public_key()],
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
    fn forged_operation_signature_fails_verify_batch() {
        // Regression: operation sigs must be in the batch set. A forged
        // voluntary-exit-shaped entry must fail under VerifyBatch.
        let sk_proposer = TestSk::from_ikm(&[51u8; 32]);
        let sk_randao = TestSk::from_ikm(&[52u8; 32]);
        let sk_exit = TestSk::from_ikm(&[53u8; 32]);
        let sk_forger = TestSk::from_ikm(&[54u8; 32]);

        let m_prop = [1u8; 32];
        let m_randao = [2u8; 32];
        let m_exit = [3u8; 32];

        let mut set = BlockSignatureSet::new();
        set.push(labelled(
            &sk_proposer,
            m_prop,
            SignatureKind::BlockProposer,
        ));
        set.push(labelled(&sk_randao, m_randao, SignatureKind::Randao));
        // Forged: wrong signer for the exit message.
        set.push(LabelledSignature {
            kind: SignatureKind::VoluntaryExit(0),
            pubkeys: vec![sk_exit.public_key()],
            message: m_exit,
            signature: sk_forger.sign(&m_exit),
        });

        let err = set
            .verify(BlockSignatureStrategy::VerifyBatch)
            .unwrap_err();
        match err {
            BlockError::InvalidSignature {
                which: SignatureKind::VoluntaryExit(0),
            } => {}
            other => panic!("expected VoluntaryExit(0), got {other:?}"),
        }

        // Honest exit signs correctly → batch ok.
        let mut good = BlockSignatureSet::new();
        good.push(labelled(
            &sk_proposer,
            m_prop,
            SignatureKind::BlockProposer,
        ));
        good.push(labelled(&sk_randao, m_randao, SignatureKind::Randao));
        good.push(labelled(
            &sk_exit,
            m_exit,
            SignatureKind::VoluntaryExit(0),
        ));
        good.verify(BlockSignatureStrategy::VerifyBatch).unwrap();
    }

    #[test]
    fn aggregate_attestation_forged_fails_batch() {
        let sk1 = TestSk::from_ikm(&[61u8; 32]);
        let sk2 = TestSk::from_ikm(&[62u8; 32]);
        let sk_forger = TestSk::from_ikm(&[63u8; 32]);
        let msg = [7u8; 32];

        // Valid aggregate of sk1+sk2.
        let s1 = sk1.sign(&msg);
        let s2 = sk2.sign(&msg);
        let agg = cc_crypto::aggregate_signatures(&[&s1, &s2]).unwrap();

        let mut good = BlockSignatureSet::new();
        good.push(LabelledSignature {
            kind: SignatureKind::Attestation(0),
            pubkeys: vec![sk1.public_key(), sk2.public_key()],
            message: msg,
            signature: agg,
        });
        good.verify(BlockSignatureStrategy::VerifyBatch).unwrap();

        // Forged aggregate (forger alone).
        let mut bad = BlockSignatureSet::new();
        bad.push(LabelledSignature {
            kind: SignatureKind::Attestation(0),
            pubkeys: vec![sk1.public_key(), sk2.public_key()],
            message: msg,
            signature: sk_forger.sign(&msg),
        });
        let err = bad
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
    fn signing_root_uses_domain() {
        let domain = cc_crypto::compute_domain(DOMAIN_BEACON_PROPOSER, None, None);
        let root = Root::from_array([7u8; 32]);
        let a = compute_signing_root(&root, domain);
        let b = compute_signing_root(&root, domain);
        assert_eq!(a, b);
    }
}
