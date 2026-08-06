//! Beacon block containers (Architecture §3.1, Electra/Fulu body shape).
//!
//! `context_deserialize` / fork-context SSZ entry point lands on
//! [`SignedBeaconBlock::from_ssz_bytes_with`] only (BeaconState is CC-10e).

use ssz::DecodeError;
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash;

use crate::containers::{Eth1Data, SyncAggregate};
use crate::execution::ExecutionPayload;
use crate::fork::ForkName;
use crate::operations::{
    Attestation, AttesterSlashing, Deposit, ExecutionRequests, ProposerSlashing,
    SignedBlsToExecutionChange, SignedVoluntaryExit,
};
use crate::preset::Preset;
use crate::primitives::{BlsSignature, Hash256, KzgCommitment, Root, Slot, ValidatorIndex};

/// Spec `BeaconBlockBody` (Electra/Fulu).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct BeaconBlockBody<P: Preset> {
    /// Proposer RANDAO reveal.
    pub randao_reveal: BlsSignature,
    /// Eth1 data vote.
    pub eth1_data: Eth1Data,
    /// 32-byte graffiti.
    pub graffiti: Root,
    /// Proposer slashings.
    pub proposer_slashings: VariableList<ProposerSlashing, P::MaxProposerSlashings>,
    /// Attester slashings (Electra capacity).
    pub attester_slashings: VariableList<AttesterSlashing<P>, P::MaxAttesterSlashingsElectra>,
    /// Attestations (Electra capacity + shape).
    pub attestations: VariableList<Attestation<P>, P::MaxAttestationsElectra>,
    /// Eth1 deposits (empty under Fulu once deposit requests take over).
    pub deposits: VariableList<Deposit, P::MaxDeposits>,
    /// Voluntary exits.
    pub voluntary_exits: VariableList<SignedVoluntaryExit, P::MaxVoluntaryExits>,
    /// Sync committee aggregate.
    pub sync_aggregate: SyncAggregate<P>,
    /// Execution payload.
    pub execution_payload: ExecutionPayload<P>,
    /// BLS-to-execution credential changes.
    pub bls_to_execution_changes:
        VariableList<SignedBlsToExecutionChange, P::MaxBlsToExecutionChanges>,
    /// KZG commitments for blobs.
    pub blob_kzg_commitments: VariableList<KzgCommitment, P::MaxBlobCommitmentsPerBlock>,
    /// Execution-layer requests (Electra).
    pub execution_requests: ExecutionRequests<P>,
}

impl<P: Preset> Default for BeaconBlockBody<P> {
    fn default() -> Self {
        Self {
            randao_reveal: BlsSignature::default(),
            eth1_data: Eth1Data::default(),
            graffiti: Root::default(),
            proposer_slashings: VariableList::default(),
            attester_slashings: VariableList::default(),
            attestations: VariableList::default(),
            deposits: VariableList::default(),
            voluntary_exits: VariableList::default(),
            sync_aggregate: SyncAggregate::default(),
            execution_payload: ExecutionPayload::default(),
            bls_to_execution_changes: VariableList::default(),
            blob_kzg_commitments: VariableList::default(),
            execution_requests: ExecutionRequests::default(),
        }
    }
}

/// Spec `BeaconBlock`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct BeaconBlock<P: Preset> {
    /// Slot of the block.
    pub slot: Slot,
    /// Proposer validator index.
    pub proposer_index: ValidatorIndex,
    /// Parent block root.
    pub parent_root: Root,
    /// Post-state root.
    pub state_root: Root,
    /// Block body.
    pub body: BeaconBlockBody<P>,
}

impl<P: Preset> Default for BeaconBlock<P> {
    fn default() -> Self {
        Self {
            slot: Slot::default(),
            proposer_index: ValidatorIndex::default(),
            parent_root: Root::default(),
            state_root: Root::default(),
            body: BeaconBlockBody::default(),
        }
    }
}

impl<P: Preset> BeaconBlock<P> {
    /// Canonical block root (`hash_tree_root` of the unsigned block).
    pub fn canonical_root(&self) -> Hash256 {
        TreeHash::tree_hash_root(self)
    }
}

/// Spec `SignedBeaconBlock`.
///
/// Fork-context SSZ entry point: [`Self::from_ssz_bytes_with`]. Phase 1 only
/// accepts [`ForkName::Fulu`]; other forks return `Err` (no panic).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SignedBeaconBlock<P: Preset> {
    /// Unsigned block.
    pub message: BeaconBlock<P>,
    /// Proposer signature over `message`.
    pub signature: BlsSignature,
}

impl<P: Preset> Default for SignedBeaconBlock<P> {
    fn default() -> Self {
        Self {
            message: BeaconBlock::default(),
            signature: BlsSignature::default(),
        }
    }
}

impl<P: Preset> SignedBeaconBlock<P> {
    /// Decode SSZ bytes under an explicit fork context.
    ///
    /// Phase 1 supports only [`ForkName::Fulu`]. The method signature is the
    /// long-term boundary used by `p2p`, `beacon-api`, and `storage` so a later
    /// fork does not require a call-site sweep.
    pub fn from_ssz_bytes_with(fork_name: ForkName, bytes: &[u8]) -> Result<Self, DecodeError> {
        match fork_name {
            ForkName::Fulu => <Self as ssz::Decode>::from_ssz_bytes(bytes),
            other => Err(DecodeError::BytesInvalid(format!(
                "unsupported fork for SignedBeaconBlock SSZ decode: {other} (Phase 1 is Fulu-only)"
            ))),
        }
    }

    /// Canonical block root (`hash_tree_root` of [`Self::message`]).
    pub fn canonical_root(&self) -> Hash256 {
        self.message.canonical_root()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::preset::{Mainnet, Minimal};
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;

    #[test]
    fn beacon_block_body_default_roundtrip() {
        let body = BeaconBlockBody::<Mainnet>::default();
        let bytes = body.as_ssz_bytes();
        assert_eq!(
            BeaconBlockBody::<Mainnet>::from_ssz_bytes(&bytes).unwrap(),
            body
        );
        let _ = body.tree_hash_root();
    }

    #[test]
    fn signed_beacon_block_default_encodes_both_presets() {
        let main = SignedBeaconBlock::<Mainnet>::default();
        let min = SignedBeaconBlock::<Minimal>::default();
        let _ = main.as_ssz_bytes();
        let _ = min.as_ssz_bytes();
        let _ = main.tree_hash_root();
        let _ = min.tree_hash_root();
        let _ = main.canonical_root();
    }

    #[test]
    fn from_ssz_bytes_with_fulu_roundtrip() {
        let block = SignedBeaconBlock::<Mainnet>::default();
        let bytes = block.as_ssz_bytes();
        let decoded =
            SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes).unwrap();
        assert_eq!(decoded, block);
    }

    #[test]
    fn from_ssz_bytes_with_rejects_non_fulu() {
        let block = SignedBeaconBlock::<Mainnet>::default();
        let bytes = block.as_ssz_bytes();
        let err =
            SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Electra, &bytes).unwrap_err();
        match err {
            DecodeError::BytesInvalid(msg) => {
                assert!(msg.contains("unsupported fork"), "{msg}");
            }
            other => panic!("expected BytesInvalid, got {other:?}"),
        }
    }

    #[test]
    fn truncated_beacon_block_body_returns_err() {
        let body = BeaconBlockBody::<Mainnet>::default();
        let bytes = body.as_ssz_bytes();
        let truncated = &bytes[..bytes.len().saturating_sub(1)];
        assert!(BeaconBlockBody::<Mainnet>::from_ssz_bytes(truncated).is_err());
    }

    #[test]
    fn overlong_beacon_block_body_returns_err() {
        let body = BeaconBlockBody::<Mainnet>::default();
        let mut bytes = body.as_ssz_bytes();
        bytes.push(0x01);
        assert!(BeaconBlockBody::<Mainnet>::from_ssz_bytes(&bytes).is_err());
    }

    #[test]
    fn no_panic_on_garbage_beacon_block_body() {
        let garbage = [0u8; 32];
        let result = std::panic::catch_unwind(|| {
            let _ = BeaconBlockBody::<Mainnet>::from_ssz_bytes(&garbage);
        });
        assert!(result.is_ok(), "decode must not panic");
        assert!(BeaconBlockBody::<Mainnet>::from_ssz_bytes(&garbage).is_err());
    }

    /// Compile-time / runtime exercise of every container's Encode + TreeHash on Default.
    #[test]
    fn all_container_defaults_encode_and_hash() {
        use crate::containers::*;
        use crate::execution::*;
        use crate::operations::*;

        macro_rules! exercise {
            ($($ty:ty),+ $(,)?) => {
                $(
                    {
                        let v = <$ty>::default();
                        let _bytes = ssz::Encode::as_ssz_bytes(&v);
                        let _root = TreeHash::tree_hash_root(&v);
                    }
                )+
            };
        }

        // containers
        exercise!(
            Checkpoint,
            Validator,
            Eth1Data,
            AttestationData,
            BeaconBlockHeader,
            SignedBeaconBlockHeader,
            SigningData,
            HistoricalSummary,
            DepositData,
            DepositMessage,
            SyncCommittee<Mainnet>,
            SyncAggregate<Mainnet>,
            SyncCommittee<Minimal>,
            SyncAggregate<Minimal>,
        );

        // operations
        exercise!(
            ProposerSlashing,
            IndexedAttestation<Mainnet>,
            AttesterSlashing<Mainnet>,
            Attestation<Mainnet>,
            Deposit,
            VoluntaryExit,
            SignedVoluntaryExit,
            BlsToExecutionChange,
            SignedBlsToExecutionChange,
            Withdrawal,
            DepositRequest,
            WithdrawalRequest,
            ConsolidationRequest,
            ExecutionRequests<Mainnet>,
            PendingDeposit,
            PendingPartialWithdrawal,
            PendingConsolidation,
            IndexedAttestation<Minimal>,
            AttesterSlashing<Minimal>,
            Attestation<Minimal>,
            ExecutionRequests<Minimal>,
        );

        // execution
        exercise!(
            ExecutionPayload<Mainnet>,
            ExecutionPayloadHeader<Mainnet>,
            ExecutionPayload<Minimal>,
            ExecutionPayloadHeader<Minimal>,
        );

        // block
        exercise!(
            BeaconBlockBody<Mainnet>,
            BeaconBlock<Mainnet>,
            SignedBeaconBlock<Mainnet>,
            BeaconBlockBody<Minimal>,
            BeaconBlock<Minimal>,
            SignedBeaconBlock<Minimal>,
        );
    }
}
