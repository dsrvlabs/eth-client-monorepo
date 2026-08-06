//! Core consensus containers (Architecture §3.1, phase0/altair).
//!
//! Spec containers that do not belong to operations, execution, or block body
//! groupings. Generic over [`Preset`] only where list/vector capacities appear.

use alloy_primitives::U256;
use ssz_derive::{Decode, Encode};
use ssz_types::{BitVector, FixedVector};
use tree_hash_derive::TreeHash;

use crate::preset::Preset;
use crate::primitives::{
    BlsPublicKey, BlsSignature, CommitteeIndex, Domain, Epoch, Gwei, Root, Slot, ValidatorIndex,
};

/// Spec `Checkpoint`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Checkpoint {
    /// Checkpoint epoch.
    pub epoch: Epoch,
    /// Block root at the start of `epoch`.
    pub root: Root,
}

/// Spec `Validator`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Validator {
    /// BLS public key.
    pub pubkey: BlsPublicKey,
    /// Withdrawal credentials (32 bytes).
    pub withdrawal_credentials: Root,
    /// Effective balance in Gwei.
    pub effective_balance: Gwei,
    /// Whether the validator has been slashed.
    pub slashed: bool,
    /// Epoch when eligible for activation.
    pub activation_eligibility_epoch: Epoch,
    /// Epoch when activated.
    pub activation_epoch: Epoch,
    /// Epoch when exited.
    pub exit_epoch: Epoch,
    /// Epoch when withdrawable.
    pub withdrawable_epoch: Epoch,
}

/// Spec `Eth1Data`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Eth1Data {
    /// Root of the deposit tree.
    pub deposit_root: Root,
    /// Total deposits observed.
    pub deposit_count: u64,
    /// Eth1 block hash.
    pub block_hash: Root,
}

/// Spec `AttestationData`.
///
/// Electra keeps `index` in the container for SSZ compatibility; on-chain values
/// must be zero (committee index lives in `Attestation.committee_bits`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct AttestationData {
    /// Slot of the attestation.
    pub slot: Slot,
    /// Committee index (must be 0 post-Electra).
    pub index: CommitteeIndex,
    /// LMD GHOST head block root.
    pub beacon_block_root: Root,
    /// FFG source checkpoint.
    pub source: Checkpoint,
    /// FFG target checkpoint.
    pub target: Checkpoint,
}

/// Spec `BeaconBlockHeader`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct BeaconBlockHeader {
    /// Slot of the block.
    pub slot: Slot,
    /// Proposer validator index.
    pub proposer_index: ValidatorIndex,
    /// Parent block root.
    pub parent_root: Root,
    /// State root after applying the block.
    pub state_root: Root,
    /// Root of the block body.
    pub body_root: Root,
}

/// Spec `SignedBeaconBlockHeader`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SignedBeaconBlockHeader {
    /// Unsigned header.
    pub message: BeaconBlockHeader,
    /// Proposer signature over `message`.
    pub signature: BlsSignature,
}

/// Spec `SigningData` (domain-separated signing root helper).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SigningData {
    /// Hash-tree-root of the signed object.
    pub object_root: Root,
    /// Signing domain.
    pub domain: Domain,
}

/// Spec `HistoricalSummary` (Capella).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct HistoricalSummary {
    /// Root of a window of block roots.
    pub block_summary_root: Root,
    /// Root of a window of state roots.
    pub state_summary_root: Root,
}

/// Spec `Eth1Block` (phase0 deposit-contract helper; present in `ssz_static`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Eth1Block {
    /// Eth1 block timestamp.
    pub timestamp: u64,
    /// Deposit tree root at this block.
    pub deposit_root: Root,
    /// Deposit count at this block.
    pub deposit_count: u64,
}

/// Spec `PowBlock` (Bellatrix merge helper; present in `ssz_static`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PowBlock {
    /// This PoW block's hash.
    pub block_hash: Root,
    /// Parent PoW block hash.
    pub parent_hash: Root,
    /// Cumulative difficulty (`uint256`).
    pub total_difficulty: U256,
}

/// Spec `DepositMessage`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct DepositMessage {
    /// Validator public key.
    pub pubkey: BlsPublicKey,
    /// Withdrawal credentials.
    pub withdrawal_credentials: Root,
    /// Deposit amount in Gwei.
    pub amount: Gwei,
}

/// Spec `DepositData` (`signature` over [`DepositMessage`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct DepositData {
    /// Validator public key.
    pub pubkey: BlsPublicKey,
    /// Withdrawal credentials.
    pub withdrawal_credentials: Root,
    /// Deposit amount in Gwei.
    pub amount: Gwei,
    /// BLS signature over the deposit message.
    pub signature: BlsSignature,
}

/// Spec `SyncCommittee` (Altair+).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SyncCommittee<P: Preset> {
    /// Public keys of committee members.
    pub pubkeys: FixedVector<BlsPublicKey, P::SyncCommitteeSize>,
    /// Aggregate public key of the committee.
    pub aggregate_pubkey: BlsPublicKey,
}

impl<P: Preset> Default for SyncCommittee<P> {
    fn default() -> Self {
        Self {
            pubkeys: FixedVector::default(),
            aggregate_pubkey: BlsPublicKey::default(),
        }
    }
}

/// Spec `SyncAggregate` (Altair+).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SyncAggregate<P: Preset> {
    /// Participation bits of the sync committee.
    pub sync_committee_bits: BitVector<P::SyncCommitteeSize>,
    /// Aggregate signature of participating members.
    pub sync_committee_signature: BlsSignature,
}

impl<P: Preset> Default for SyncAggregate<P> {
    fn default() -> Self {
        Self {
            sync_committee_bits: BitVector::default(),
            sync_committee_signature: BlsSignature::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::preset::Mainnet;
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;

    #[test]
    fn checkpoint_ssz_roundtrip() {
        let c = Checkpoint {
            epoch: Epoch::new(7),
            root: Root::from_array([0x11; 32]),
        };
        let bytes = c.as_ssz_bytes();
        assert_eq!(
            Checkpoint::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}")),
            c
        );
        let _ = c.tree_hash_root();
    }

    #[test]
    fn sync_committee_default_encodes() {
        let sc = SyncCommittee::<Mainnet>::default();
        let _ = sc.as_ssz_bytes();
        let _ = sc.tree_hash_root();
    }
}
