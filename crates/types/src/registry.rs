//! `ssz_static` dispatch table (Architecture §3.1 / CC-10/1).
//!
//! Completeness is mechanical: [`SSZ_STATIC_TYPE_NAMES`] must equal the on-disk
//! `tests/<preset>/fulu/ssz_static/*` listing for both presets. Handlers are
//! generic over [`Preset`] so mainnet and minimal share one table.

use ssz::{Decode, DecodeError, Encode};
use tree_hash::TreeHash;

use crate::block::{BeaconBlock, BeaconBlockBody, SignedBeaconBlock};
use crate::containers::{
    AttestationData, BeaconBlockHeader, Checkpoint, DepositData, DepositMessage, Eth1Block,
    Eth1Data, HistoricalSummary, PowBlock, SignedBeaconBlockHeader, SigningData, SyncAggregate,
    SyncCommittee, Validator,
};
use crate::execution::{ExecutionPayload, ExecutionPayloadHeader};
use crate::fork::{Fork, ForkData};
use crate::light_client::{
    LightClientBootstrap, LightClientFinalityUpdate, LightClientHeader,
    LightClientOptimisticUpdate, LightClientUpdate,
};
use crate::operations::{
    AggregateAndProof, Attestation, AttesterSlashing, BlsToExecutionChange, ConsolidationRequest,
    ContributionAndProof, Deposit, DepositRequest, ExecutionRequests, IndexedAttestation,
    PendingConsolidation, PendingDeposit, PendingPartialWithdrawal, ProposerSlashing,
    SignedAggregateAndProof, SignedBlsToExecutionChange, SignedContributionAndProof,
    SignedVoluntaryExit, SingleAttestation, SyncAggregatorSelectionData, SyncCommitteeContribution,
    SyncCommitteeMessage, VoluntaryExit, Withdrawal, WithdrawalRequest,
};
use crate::preset::Preset;
use crate::primitives::Root;
use crate::sidecar::{
    DataColumnSidecar, DataColumnsByRootIdentifier, MatrixEntry, PartialDataColumnGroupID,
    PartialDataColumnHeader, PartialDataColumnPartsMetadata, PartialDataColumnSidecar,
};
use crate::state::BeaconState;

/// Result of a successful `ssz_static` handler: re-encoded bytes + tree hash root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SszStaticOutput {
    /// SSZ re-encoding of the decoded value (must equal the input bytes).
    pub serialized: Vec<u8>,
    /// `hash_tree_root` of the decoded value.
    pub root: Root,
}

/// Handler function pointer: decode → re-encode → tree-hash.
pub type SszStaticHandler = fn(&[u8]) -> Result<SszStaticOutput, DecodeError>;

/// Ordered names of every type in the Fulu `ssz_static` registry.
///
/// Must match the on-disk directory listing exactly (both directions).
pub const SSZ_STATIC_TYPE_NAMES: &[&str] = &[
    "AggregateAndProof",
    "Attestation",
    "AttestationData",
    "AttesterSlashing",
    "BLSToExecutionChange",
    "BeaconBlock",
    "BeaconBlockBody",
    "BeaconBlockHeader",
    "BeaconState",
    "Checkpoint",
    "ConsolidationRequest",
    "ContributionAndProof",
    "DataColumnSidecar",
    "DataColumnsByRootIdentifier",
    "Deposit",
    "DepositData",
    "DepositMessage",
    "DepositRequest",
    "Eth1Block",
    "Eth1Data",
    "ExecutionPayload",
    "ExecutionPayloadHeader",
    "ExecutionRequests",
    "Fork",
    "ForkData",
    "HistoricalSummary",
    "IndexedAttestation",
    "LightClientBootstrap",
    "LightClientFinalityUpdate",
    "LightClientHeader",
    "LightClientOptimisticUpdate",
    "LightClientUpdate",
    "MatrixEntry",
    "PartialDataColumnGroupID",
    "PartialDataColumnHeader",
    "PartialDataColumnPartsMetadata",
    "PartialDataColumnSidecar",
    "PendingConsolidation",
    "PendingDeposit",
    "PendingPartialWithdrawal",
    "PowBlock",
    "ProposerSlashing",
    "SignedAggregateAndProof",
    "SignedBLSToExecutionChange",
    "SignedBeaconBlock",
    "SignedBeaconBlockHeader",
    "SignedContributionAndProof",
    "SignedVoluntaryExit",
    "SigningData",
    "SingleAttestation",
    "SyncAggregate",
    "SyncAggregatorSelectionData",
    "SyncCommittee",
    "SyncCommitteeContribution",
    "SyncCommitteeMessage",
    "Validator",
    "VoluntaryExit",
    "Withdrawal",
    "WithdrawalRequest",
];

fn handle_type<T: Decode + Encode + TreeHash>(
    bytes: &[u8],
) -> Result<SszStaticOutput, DecodeError> {
    let value = T::from_ssz_bytes(bytes)?;
    let serialized = value.as_ssz_bytes();
    let root = Root::from_hash256(value.tree_hash_root());
    Ok(SszStaticOutput { serialized, root })
}

/// Dispatch table for preset `P`: one handler per [`SSZ_STATIC_TYPE_NAMES`] entry.
///
/// Order matches [`SSZ_STATIC_TYPE_NAMES`]. Both presets share the same name set;
/// handlers are monomorphized per `P`.
pub fn ssz_static_types<P: Preset>() -> &'static [(&'static str, SszStaticHandler)] {
    // Each monomorphization of this function gets its own static table.
    // `handler` pointers are monomorphized concrete `fn` items.
    &[
        ("AggregateAndProof", handle_type::<AggregateAndProof<P>>),
        ("Attestation", handle_type::<Attestation<P>>),
        ("AttestationData", handle_type::<AttestationData>),
        ("AttesterSlashing", handle_type::<AttesterSlashing<P>>),
        ("BLSToExecutionChange", handle_type::<BlsToExecutionChange>),
        ("BeaconBlock", handle_type::<BeaconBlock<P>>),
        ("BeaconBlockBody", handle_type::<BeaconBlockBody<P>>),
        ("BeaconBlockHeader", handle_type::<BeaconBlockHeader>),
        ("BeaconState", handle_type::<BeaconState<P>>),
        ("Checkpoint", handle_type::<Checkpoint>),
        ("ConsolidationRequest", handle_type::<ConsolidationRequest>),
        (
            "ContributionAndProof",
            handle_type::<ContributionAndProof<P>>,
        ),
        ("DataColumnSidecar", handle_type::<DataColumnSidecar<P>>),
        (
            "DataColumnsByRootIdentifier",
            handle_type::<DataColumnsByRootIdentifier>,
        ),
        ("Deposit", handle_type::<Deposit>),
        ("DepositData", handle_type::<DepositData>),
        ("DepositMessage", handle_type::<DepositMessage>),
        ("DepositRequest", handle_type::<DepositRequest>),
        ("Eth1Block", handle_type::<Eth1Block>),
        ("Eth1Data", handle_type::<Eth1Data>),
        ("ExecutionPayload", handle_type::<ExecutionPayload<P>>),
        (
            "ExecutionPayloadHeader",
            handle_type::<ExecutionPayloadHeader<P>>,
        ),
        ("ExecutionRequests", handle_type::<ExecutionRequests<P>>),
        ("Fork", handle_type::<Fork>),
        ("ForkData", handle_type::<ForkData>),
        ("HistoricalSummary", handle_type::<HistoricalSummary>),
        ("IndexedAttestation", handle_type::<IndexedAttestation<P>>),
        (
            "LightClientBootstrap",
            handle_type::<LightClientBootstrap<P>>,
        ),
        (
            "LightClientFinalityUpdate",
            handle_type::<LightClientFinalityUpdate<P>>,
        ),
        ("LightClientHeader", handle_type::<LightClientHeader<P>>),
        (
            "LightClientOptimisticUpdate",
            handle_type::<LightClientOptimisticUpdate<P>>,
        ),
        ("LightClientUpdate", handle_type::<LightClientUpdate<P>>),
        ("MatrixEntry", handle_type::<MatrixEntry>),
        (
            "PartialDataColumnGroupID",
            handle_type::<PartialDataColumnGroupID>,
        ),
        (
            "PartialDataColumnHeader",
            handle_type::<PartialDataColumnHeader<P>>,
        ),
        (
            "PartialDataColumnPartsMetadata",
            handle_type::<PartialDataColumnPartsMetadata<P>>,
        ),
        (
            "PartialDataColumnSidecar",
            handle_type::<PartialDataColumnSidecar<P>>,
        ),
        ("PendingConsolidation", handle_type::<PendingConsolidation>),
        ("PendingDeposit", handle_type::<PendingDeposit>),
        (
            "PendingPartialWithdrawal",
            handle_type::<PendingPartialWithdrawal>,
        ),
        ("PowBlock", handle_type::<PowBlock>),
        ("ProposerSlashing", handle_type::<ProposerSlashing>),
        (
            "SignedAggregateAndProof",
            handle_type::<SignedAggregateAndProof<P>>,
        ),
        (
            "SignedBLSToExecutionChange",
            handle_type::<SignedBlsToExecutionChange>,
        ),
        ("SignedBeaconBlock", handle_type::<SignedBeaconBlock<P>>),
        (
            "SignedBeaconBlockHeader",
            handle_type::<SignedBeaconBlockHeader>,
        ),
        (
            "SignedContributionAndProof",
            handle_type::<SignedContributionAndProof<P>>,
        ),
        ("SignedVoluntaryExit", handle_type::<SignedVoluntaryExit>),
        ("SigningData", handle_type::<SigningData>),
        ("SingleAttestation", handle_type::<SingleAttestation>),
        ("SyncAggregate", handle_type::<SyncAggregate<P>>),
        (
            "SyncAggregatorSelectionData",
            handle_type::<SyncAggregatorSelectionData>,
        ),
        ("SyncCommittee", handle_type::<SyncCommittee<P>>),
        (
            "SyncCommitteeContribution",
            handle_type::<SyncCommitteeContribution<P>>,
        ),
        ("SyncCommitteeMessage", handle_type::<SyncCommitteeMessage>),
        ("Validator", handle_type::<Validator>),
        ("VoluntaryExit", handle_type::<VoluntaryExit>),
        ("Withdrawal", handle_type::<Withdrawal>),
        ("WithdrawalRequest", handle_type::<WithdrawalRequest>),
    ]
}

/// Look up a handler by container name for preset `P`.
pub fn ssz_static_handler<P: Preset>(name: &str) -> Option<SszStaticHandler> {
    ssz_static_types::<P>()
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, h)| *h)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::preset::{Mainnet, Minimal};
    use std::collections::BTreeSet;

    #[test]
    fn table_names_match_const_list() {
        let from_table: Vec<&str> = ssz_static_types::<Mainnet>()
            .iter()
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(from_table, SSZ_STATIC_TYPE_NAMES);
        let from_min: Vec<&str> = ssz_static_types::<Minimal>()
            .iter()
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(from_min, SSZ_STATIC_TYPE_NAMES);
    }

    #[test]
    fn names_are_unique() {
        let set: BTreeSet<&str> = SSZ_STATIC_TYPE_NAMES.iter().copied().collect();
        assert_eq!(set.len(), SSZ_STATIC_TYPE_NAMES.len());
    }

    #[test]
    fn fork_handler_roundtrips() {
        let fork = Fork {
            previous_version: Default::default(),
            current_version: Default::default(),
            epoch: Default::default(),
        };
        let bytes = ssz::Encode::as_ssz_bytes(&fork);
        let handler =
            ssz_static_handler::<Mainnet>("Fork").unwrap_or_else(|| panic!("Fork handler missing"));
        let out = handler(&bytes).unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(out.serialized, bytes);
    }
}
