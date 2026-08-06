//! Beacon operations and Electra request / pending-queue containers (Architecture §3.1).
//!
//! Electra-shaped `Attestation` / `IndexedAttestation` / `AttesterSlashing` use
//! `P::MaxValidatorsPerSlot` for aggregation / attesting-indices capacity.

use ssz_derive::{Decode, Encode};
use ssz_types::{BitList, BitVector, FixedVector, VariableList};
use tree_hash_derive::TreeHash;
use typenum::U33;

use crate::containers::{
    AttestationData, DepositData, SignedBeaconBlockHeader,
};
use crate::preset::Preset;
use crate::primitives::{
    BlsPublicKey, BlsSignature, Epoch, ExecutionAddress, Gwei, Root, Slot, ValidatorIndex,
};

/// Merkle proof depth for the deposit contract tree (`DEPOSIT_CONTRACT_TREE_DEPTH = 32`).
pub const DEPOSIT_CONTRACT_TREE_DEPTH: usize = 32;

/// Length of a deposit Merkle proof including the mix-in (`DEPOSIT_CONTRACT_TREE_DEPTH + 1`).
pub type DepositProofLen = U33;

/// Spec `ProposerSlashing`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct ProposerSlashing {
    /// First conflicting signed header.
    pub signed_header_1: SignedBeaconBlockHeader,
    /// Second conflicting signed header.
    pub signed_header_2: SignedBeaconBlockHeader,
}

/// Spec `IndexedAttestation` (Electra: indices capacity = validators per slot).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct IndexedAttestation<P: Preset> {
    /// Sorted unique validator indices participating in the attestation.
    pub attesting_indices: VariableList<ValidatorIndex, P::MaxValidatorsPerSlot>,
    /// Attestation data.
    pub data: AttestationData,
    /// Aggregate signature.
    pub signature: BlsSignature,
}

impl<P: Preset> Default for IndexedAttestation<P> {
    fn default() -> Self {
        Self {
            attesting_indices: VariableList::default(),
            data: AttestationData::default(),
            signature: BlsSignature::default(),
        }
    }
}

/// Spec `AttesterSlashing` (Electra-shaped indexed attestations).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct AttesterSlashing<P: Preset> {
    /// First conflicting indexed attestation.
    pub attestation_1: IndexedAttestation<P>,
    /// Second conflicting indexed attestation.
    pub attestation_2: IndexedAttestation<P>,
}

impl<P: Preset> Default for AttesterSlashing<P> {
    fn default() -> Self {
        Self {
            attestation_1: IndexedAttestation::default(),
            attestation_2: IndexedAttestation::default(),
        }
    }
}

/// Spec `Attestation` (Electra: `aggregation_bits` + `committee_bits`).
///
/// `aggregation_bits` capacity is [`Preset::MaxValidatorsPerSlot`]
/// (`MAX_VALIDATORS_PER_COMMITTEE × MAX_COMMITTEES_PER_SLOT`), **not**
/// `MaxValidatorsPerCommittee`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Attestation<P: Preset> {
    /// Participation bits across all selected committees (concatenated).
    pub aggregation_bits: BitList<P::MaxValidatorsPerSlot>,
    /// Attestation data.
    pub data: AttestationData,
    /// Aggregate signature.
    pub signature: BlsSignature,
    /// Bits marking which committees of the slot participate.
    pub committee_bits: BitVector<P::MaxCommitteesPerSlot>,
}

impl<P: Preset> Default for Attestation<P> {
    fn default() -> Self {
        Self {
            // BitList has no Default; zero-length is always within capacity N.
            aggregation_bits: empty_bitlist(),
            data: AttestationData::default(),
            signature: BlsSignature::default(),
            committee_bits: BitVector::default(),
        }
    }
}

/// Empty `BitList<N>`.
///
/// `BitList` has no `Default`. `with_capacity(0)` fails only when `0 > N`, which
/// is impossible for any `typenum::Unsigned` capacity used as a list bound.
#[allow(clippy::expect_used)] // capacity 0 is an SSZ-stack invariant, not input data
fn empty_bitlist<N: typenum::Unsigned + Clone>() -> BitList<N> {
    BitList::with_capacity(0).expect("BitList::with_capacity(0) is infallible for Unsigned N")
}

/// Spec `Deposit`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Deposit {
    /// Merkle proof of `data` in the deposit contract tree.
    pub proof: FixedVector<Root, DepositProofLen>,
    /// Deposit data.
    pub data: DepositData,
}

/// Spec `VoluntaryExit`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct VoluntaryExit {
    /// Earliest epoch when the exit is valid.
    pub epoch: Epoch,
    /// Exiting validator index.
    pub validator_index: ValidatorIndex,
}

/// Spec `SignedVoluntaryExit`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SignedVoluntaryExit {
    /// Unsigned exit message.
    pub message: VoluntaryExit,
    /// Validator signature.
    pub signature: BlsSignature,
}

/// Spec `BLSToExecutionChange` (Capella).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct BlsToExecutionChange {
    /// Validator index.
    pub validator_index: ValidatorIndex,
    /// Previous BLS withdrawal public key.
    pub from_bls_pubkey: BlsPublicKey,
    /// New execution withdrawal address.
    pub to_execution_address: ExecutionAddress,
}

/// Spec `SignedBLSToExecutionChange`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SignedBlsToExecutionChange {
    /// Unsigned change message.
    pub message: BlsToExecutionChange,
    /// Signature from `from_bls_pubkey`.
    pub signature: BlsSignature,
}

/// Spec `Withdrawal` (Capella).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Withdrawal {
    /// Monotonic withdrawal index.
    pub index: u64,
    /// Validator being withdrawn.
    pub validator_index: ValidatorIndex,
    /// Destination execution address.
    pub address: ExecutionAddress,
    /// Amount in Gwei.
    pub amount: Gwei,
}

// ---------------------------------------------------------------------------
// Electra execution-layer request types
// ---------------------------------------------------------------------------

/// Spec `DepositRequest` (EIP-6110).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct DepositRequest {
    /// Validator public key.
    pub pubkey: BlsPublicKey,
    /// Withdrawal credentials.
    pub withdrawal_credentials: Root,
    /// Deposit amount in Gwei.
    pub amount: Gwei,
    /// Deposit signature.
    pub signature: BlsSignature,
    /// Deposit request index from the EL.
    pub index: u64,
}

/// Spec `WithdrawalRequest` (EIP-7002 / EIP-7251).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct WithdrawalRequest {
    /// Source execution address initiating the request.
    pub source_address: ExecutionAddress,
    /// Validator public key.
    pub validator_pubkey: BlsPublicKey,
    /// Amount in Gwei (`0` signals full exit).
    pub amount: Gwei,
}

/// Spec `ConsolidationRequest` (EIP-7251).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct ConsolidationRequest {
    /// Source execution address initiating the request.
    pub source_address: ExecutionAddress,
    /// Source validator public key.
    pub source_pubkey: BlsPublicKey,
    /// Target validator public key.
    pub target_pubkey: BlsPublicKey,
}

/// Spec `ExecutionRequests` (Electra block-body field).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct ExecutionRequests<P: Preset> {
    /// Deposit requests from the EL.
    pub deposits: VariableList<DepositRequest, P::MaxDepositRequestsPerPayload>,
    /// Withdrawal requests from the EL.
    pub withdrawals: VariableList<WithdrawalRequest, P::MaxWithdrawalRequestsPerPayload>,
    /// Consolidation requests from the EL.
    pub consolidations: VariableList<ConsolidationRequest, P::MaxConsolidationRequestsPerPayload>,
}

impl<P: Preset> Default for ExecutionRequests<P> {
    fn default() -> Self {
        Self {
            deposits: VariableList::default(),
            withdrawals: VariableList::default(),
            consolidations: VariableList::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Electra pending queues (BeaconState fields; types owned here per §3.1)
// ---------------------------------------------------------------------------

/// Spec `PendingDeposit`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PendingDeposit {
    /// Validator public key.
    pub pubkey: BlsPublicKey,
    /// Withdrawal credentials.
    pub withdrawal_credentials: Root,
    /// Amount in Gwei.
    pub amount: Gwei,
    /// Deposit signature.
    pub signature: BlsSignature,
    /// Slot at which the deposit was recorded (`GENESIS_SLOT` for eth1-bridge).
    pub slot: Slot,
}

/// Spec `PendingPartialWithdrawal`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PendingPartialWithdrawal {
    /// Validator index.
    pub validator_index: ValidatorIndex,
    /// Amount in Gwei.
    pub amount: Gwei,
    /// Epoch at which the withdrawal becomes withdrawable.
    pub withdrawable_epoch: Epoch,
}

/// Spec `PendingConsolidation`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PendingConsolidation {
    /// Source validator index.
    pub source_index: ValidatorIndex,
    /// Target validator index.
    pub target_index: ValidatorIndex,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::preset::{Mainnet, Minimal};
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;
    use typenum::Unsigned;

    #[test]
    fn attestation_aggregation_bits_capacity_mainnet() {
        // MAX_VALIDATORS_PER_COMMITTEE × MAX_COMMITTEES_PER_SLOT
        let expected = Mainnet::MAX_VALIDATORS_PER_COMMITTEE
            .checked_mul(Mainnet::MAX_COMMITTEES_PER_SLOT)
            .expect("product fits u64");
        assert_eq!(Mainnet::MAX_VALIDATORS_PER_SLOT, expected);
        assert_eq!(
            <Mainnet as Preset>::MaxValidatorsPerSlot::to_u64(),
            expected
        );
        // Type-level capacity on Attestation.aggregation_bits
        assert_eq!(
            <Mainnet as Preset>::MaxValidatorsPerSlot::to_usize(),
            2048 * 64
        );
        let _marker: Attestation<Mainnet> = Attestation::default();
        let _ = _marker.aggregation_bits;
    }

    #[test]
    fn attestation_aggregation_bits_capacity_minimal() {
        let expected = Minimal::MAX_VALIDATORS_PER_COMMITTEE
            .checked_mul(Minimal::MAX_COMMITTEES_PER_SLOT)
            .expect("product fits u64");
        assert_eq!(Minimal::MAX_VALIDATORS_PER_SLOT, expected);
        assert_eq!(
            <Minimal as Preset>::MaxValidatorsPerSlot::to_usize(),
            2048 * 4
        );
    }

    #[test]
    fn deposit_proof_len_is_33() {
        assert_eq!(DEPOSIT_CONTRACT_TREE_DEPTH, 32);
        assert_eq!(DepositProofLen::to_usize(), 33);
    }

    #[test]
    fn attestation_ssz_roundtrip_default() {
        let a = Attestation::<Mainnet>::default();
        let bytes = a.as_ssz_bytes();
        assert_eq!(Attestation::<Mainnet>::from_ssz_bytes(&bytes).unwrap(), a);
        let _ = a.tree_hash_root();
    }

    #[test]
    fn truncated_attestation_returns_err() {
        let a = Attestation::<Mainnet>::default();
        let bytes = a.as_ssz_bytes();
        assert!(!bytes.is_empty());
        let truncated = &bytes[..bytes.len().saturating_sub(1)];
        assert!(Attestation::<Mainnet>::from_ssz_bytes(truncated).is_err());
    }

    #[test]
    fn overlong_attestation_returns_err() {
        let a = Attestation::<Mainnet>::default();
        let mut bytes = a.as_ssz_bytes();
        // Trailing bytes are absorbed into the sole variable field (`aggregation_bits`).
        // A zero trailer has no length-delimiter bit → BitList decode Err (not a panic).
        bytes.push(0x00);
        assert!(Attestation::<Mainnet>::from_ssz_bytes(&bytes).is_err());
        // Also a multi-byte trailer that is not a valid bitlist delimiter pattern.
        let mut bytes = a.as_ssz_bytes();
        bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        assert!(Attestation::<Mainnet>::from_ssz_bytes(&bytes).is_err());
    }

    #[test]
    fn no_panic_on_garbage_attestation_ssz() {
        let garbage = [0u8; 8];
        let result = std::panic::catch_unwind(|| {
            let _ = Attestation::<Mainnet>::from_ssz_bytes(&garbage);
        });
        assert!(result.is_ok(), "decode must not panic");
        assert!(Attestation::<Mainnet>::from_ssz_bytes(&garbage).is_err());
    }
}
