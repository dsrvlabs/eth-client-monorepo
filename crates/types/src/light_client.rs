//! Light-client sync-protocol containers (Capella/Deneb/Electra shape for Fulu).
//!
//! Branch depths use Electra generalized indices (Architecture: Fulu inherits
//! Electra light-client gindices). Present in `ssz_static` for both presets.

use ssz_derive::{Decode, Encode};
use ssz_types::FixedVector;
use tree_hash_derive::TreeHash;
use typenum::{U4, U6, U7};

use crate::containers::{BeaconBlockHeader, SyncAggregate, SyncCommittee};
use crate::execution::ExecutionPayloadHeader;
use crate::preset::Preset;
use crate::primitives::{Root, Slot};

/// `floorlog2(EXECUTION_PAYLOAD_GINDEX)` with `EXECUTION_PAYLOAD_GINDEX = 25` → 4.
pub type ExecutionBranchDepth = U4;

/// `floorlog2(FINALIZED_ROOT_GINDEX_ELECTRA)` with gindex 169 → 7.
pub type FinalityBranchDepth = U7;

/// `floorlog2(CURRENT_SYNC_COMMITTEE_GINDEX_ELECTRA)` with gindex 86 → 6.
pub type CurrentSyncCommitteeBranchDepth = U6;

/// `floorlog2(NEXT_SYNC_COMMITTEE_GINDEX_ELECTRA)` with gindex 87 → 6.
pub type NextSyncCommitteeBranchDepth = U6;

/// Spec `LightClientHeader` (Capella+: beacon + execution + branch).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct LightClientHeader<P: Preset> {
    /// Beacon block header.
    pub beacon: BeaconBlockHeader,
    /// Execution payload header of the block body.
    pub execution: ExecutionPayloadHeader<P>,
    /// Merkle branch of `execution_payload` in the body.
    pub execution_branch: FixedVector<Root, ExecutionBranchDepth>,
}

impl<P: Preset> Default for LightClientHeader<P> {
    fn default() -> Self {
        Self {
            beacon: BeaconBlockHeader::default(),
            execution: ExecutionPayloadHeader::default(),
            execution_branch: FixedVector::default(),
        }
    }
}

/// Spec `LightClientBootstrap`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct LightClientBootstrap<P: Preset> {
    /// Header matching the requested beacon block root.
    pub header: LightClientHeader<P>,
    /// Current sync committee at `header.beacon.state_root`.
    pub current_sync_committee: SyncCommittee<P>,
    /// Merkle branch of `current_sync_committee` in the state.
    pub current_sync_committee_branch: FixedVector<Root, CurrentSyncCommitteeBranchDepth>,
}

impl<P: Preset> Default for LightClientBootstrap<P> {
    fn default() -> Self {
        Self {
            header: LightClientHeader::default(),
            current_sync_committee: SyncCommittee::default(),
            current_sync_committee_branch: FixedVector::default(),
        }
    }
}

/// Spec `LightClientUpdate`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct LightClientUpdate<P: Preset> {
    /// Header attested by the sync committee.
    pub attested_header: LightClientHeader<P>,
    /// Next sync committee corresponding to `attested_header`.
    pub next_sync_committee: SyncCommittee<P>,
    /// Merkle branch of `next_sync_committee` in the state.
    pub next_sync_committee_branch: FixedVector<Root, NextSyncCommitteeBranchDepth>,
    /// Finalized header corresponding to `attested_header`.
    pub finalized_header: LightClientHeader<P>,
    /// Merkle branch of `finalized_checkpoint.root` in the state.
    pub finality_branch: FixedVector<Root, FinalityBranchDepth>,
    /// Sync committee aggregate signature.
    pub sync_aggregate: SyncAggregate<P>,
    /// Slot at which the aggregate signature was created (untrusted).
    pub signature_slot: Slot,
}

impl<P: Preset> Default for LightClientUpdate<P> {
    fn default() -> Self {
        Self {
            attested_header: LightClientHeader::default(),
            next_sync_committee: SyncCommittee::default(),
            next_sync_committee_branch: FixedVector::default(),
            finalized_header: LightClientHeader::default(),
            finality_branch: FixedVector::default(),
            sync_aggregate: SyncAggregate::default(),
            signature_slot: Slot::default(),
        }
    }
}

/// Spec `LightClientFinalityUpdate`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct LightClientFinalityUpdate<P: Preset> {
    /// Header attested by the sync committee.
    pub attested_header: LightClientHeader<P>,
    /// Finalized header corresponding to `attested_header`.
    pub finalized_header: LightClientHeader<P>,
    /// Merkle branch of `finalized_checkpoint.root` in the state.
    pub finality_branch: FixedVector<Root, FinalityBranchDepth>,
    /// Sync committee aggregate signature.
    pub sync_aggregate: SyncAggregate<P>,
    /// Slot at which the aggregate signature was created (untrusted).
    pub signature_slot: Slot,
}

impl<P: Preset> Default for LightClientFinalityUpdate<P> {
    fn default() -> Self {
        Self {
            attested_header: LightClientHeader::default(),
            finalized_header: LightClientHeader::default(),
            finality_branch: FixedVector::default(),
            sync_aggregate: SyncAggregate::default(),
            signature_slot: Slot::default(),
        }
    }
}

/// Spec `LightClientOptimisticUpdate`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct LightClientOptimisticUpdate<P: Preset> {
    /// Header attested by the sync committee.
    pub attested_header: LightClientHeader<P>,
    /// Sync committee aggregate signature.
    pub sync_aggregate: SyncAggregate<P>,
    /// Slot at which the aggregate signature was created (untrusted).
    pub signature_slot: Slot,
}

impl<P: Preset> Default for LightClientOptimisticUpdate<P> {
    fn default() -> Self {
        Self {
            attested_header: LightClientHeader::default(),
            sync_aggregate: SyncAggregate::default(),
            signature_slot: Slot::default(),
        }
    }
}
