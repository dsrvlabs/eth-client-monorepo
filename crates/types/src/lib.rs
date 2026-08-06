//! Consensus types substrate: preset, config, primitives, fork, containers, operations,
//! execution, block, state, sidecars, light-client, and `ssz_static` registry (CC-10c–CC-10e).

#![allow(missing_docs)] // public re-exports are documented at their definitions

pub mod block;
pub mod config;
pub mod containers;
pub mod execution;
pub mod fork;
pub mod light_client;
pub mod operations;
pub mod preset;
pub mod primitives;
pub mod registry;
pub mod sidecar;
pub mod state;

pub use block::{BeaconBlock, BeaconBlockBody, SignedBeaconBlock};
pub use config::{
    BlobParameters, BlobSchedule, BlobScheduleError, ChainConfig, ConfigError, PresetName,
};
pub use containers::{
    AttestationData, BeaconBlockHeader, Checkpoint, DepositData, DepositMessage, Eth1Block,
    Eth1Data, HistoricalSummary, PowBlock, SignedBeaconBlockHeader, SigningData, SyncAggregate,
    SyncCommittee, Validator,
};
pub use execution::{ExecutionPayload, ExecutionPayloadHeader, Transaction};
pub use fork::{Fork, ForkData, ForkDigest, ForkName, UnknownForkName};
pub use light_client::{
    LightClientBootstrap, LightClientFinalityUpdate, LightClientHeader,
    LightClientOptimisticUpdate, LightClientUpdate,
};
pub use operations::{
    AggregateAndProof, Attestation, AttesterSlashing, BlsToExecutionChange, ConsolidationRequest,
    ContributionAndProof, DEPOSIT_CONTRACT_TREE_DEPTH, Deposit, DepositRequest, ExecutionRequests,
    IndexedAttestation, PendingConsolidation, PendingDeposit, PendingPartialWithdrawal,
    ProposerSlashing, SignedAggregateAndProof, SignedBlsToExecutionChange,
    SignedContributionAndProof, SignedVoluntaryExit, SingleAttestation,
    SyncAggregatorSelectionData, SyncCommitteeContribution, SyncCommitteeMessage, VoluntaryExit,
    Withdrawal, WithdrawalRequest,
};
pub use preset::{Mainnet, Minimal, Preset, PresetUnsigned};
pub use primitives::{
    BlsPublicKey, BlsSignature, Cell, CommitteeIndex, Domain, DomainType, Epoch, ExecutionAddress,
    ForkVersion, Gwei, Hash256, HexParseError, KzgCommitment, KzgProof, Root, Slot, ValidatorIndex,
    parse_hex_bytes,
};
pub use registry::{
    SSZ_STATIC_TYPE_NAMES, SszStaticHandler, SszStaticOutput, ssz_static_handler, ssz_static_types,
};
pub use sidecar::{
    DataColumnSidecar, DataColumnsByRootIdentifier, MatrixEntry, PartialDataColumnGroupID,
    PartialDataColumnHeader, PartialDataColumnPartsMetadata, PartialDataColumnSidecar,
};
pub use state::{BeaconState, List, StateCaches, Vector};

// ---------------------------------------------------------------------------
// KZG / DAS constants (Architecture §4.3, Fulu das-core / polynomial-commitments)
//
// `cc-crypto` asserts its backends agree with these rather than defining its own.
// ---------------------------------------------------------------------------

/// Bytes per BLS12-381 scalar field element.
pub const BYTES_PER_FIELD_ELEMENT: usize = 32;

/// Field elements per cell (`FIELD_ELEMENTS_PER_CELL = 64`).
pub const FIELD_ELEMENTS_PER_CELL: usize = 64;

/// Field elements per (unextended) blob (`FIELD_ELEMENTS_PER_BLOB = 4096`).
pub const FIELD_ELEMENTS_PER_BLOB: usize = 4096;

/// Field elements per Reed-Solomon extended blob (`2 * FIELD_ELEMENTS_PER_BLOB`).
pub const FIELD_ELEMENTS_PER_EXT_BLOB: usize = 2 * FIELD_ELEMENTS_PER_BLOB;

/// Cells per extended blob (`FIELD_ELEMENTS_PER_EXT_BLOB / FIELD_ELEMENTS_PER_CELL` = 128).
pub const CELLS_PER_EXT_BLOB: usize = FIELD_ELEMENTS_PER_EXT_BLOB / FIELD_ELEMENTS_PER_CELL;

/// Byte length of a [`Cell`] (`BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_CELL` = 2048).
pub const BYTES_PER_CELL: usize = BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_CELL;

/// Number of columns in the extended data matrix (`= CELLS_PER_EXT_BLOB` = 128).
pub const NUMBER_OF_COLUMNS: u64 = CELLS_PER_EXT_BLOB as u64;

/// Number of custody groups available (`NUMBER_OF_CUSTODY_GROUPS = 128`).
pub const NUMBER_OF_CUSTODY_GROUPS: u64 = 128;

/// Minimum custody groups an honest node custodies (`CUSTODY_REQUIREMENT = 4`).
pub const CUSTODY_REQUIREMENT: u64 = 4;

/// Minimum samples per slot (`SAMPLES_PER_SLOT = 8`).
pub const SAMPLES_PER_SLOT: u64 = 8;

/// Merkle proof depth for `blob_kzg_commitments` in `BeaconBlockBody` (Fulu preset = 4).
pub const KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH: u64 = 4;

/// Data-column sidecar subnet count (Fulu config = 128).
pub const DATA_COLUMN_SIDECAR_SUBNET_COUNT: u64 = 128;

/// Justification bits length (`JUSTIFICATION_BITS_LENGTH = 4`).
pub const JUSTIFICATION_BITS_LENGTH: usize = 4;

// ---------------------------------------------------------------------------
// Compile-only surface for Stream B (`cc-crypto`) — R-10 guard
// ---------------------------------------------------------------------------

/// Names the types and constants `cc-crypto` is allowed to depend on from this crate.
///
/// Presence of this function is asserted by `tests/crypto_surface.rs`.
#[doc(hidden)]
pub fn __crypto_surface_markers() {
    let _: Root = Root::ZERO;
    let _: ForkVersion = ForkVersion::ZERO;
    let _: KzgCommitment = KzgCommitment::ZERO;
    let _: KzgProof = KzgProof::ZERO;
    let _: Cell = Cell::ZERO;
    let _: usize = CELLS_PER_EXT_BLOB;
    let _: usize = FIELD_ELEMENTS_PER_CELL;
    let _: u64 = NUMBER_OF_COLUMNS;
    let _: usize = BYTES_PER_CELL;
    let _: u64 = KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH;
    let _: u64 = CUSTODY_REQUIREMENT;
    let _: u64 = SAMPLES_PER_SLOT;
    let _: u64 = NUMBER_OF_CUSTODY_GROUPS;
}

#[cfg(test)]
mod constants_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn kzg_das_constants_agree_with_spec() {
        assert_eq!(BYTES_PER_FIELD_ELEMENT, 32);
        assert_eq!(FIELD_ELEMENTS_PER_CELL, 64);
        assert_eq!(FIELD_ELEMENTS_PER_BLOB, 4096);
        assert_eq!(FIELD_ELEMENTS_PER_EXT_BLOB, 8192);
        assert_eq!(CELLS_PER_EXT_BLOB, 128);
        assert_eq!(BYTES_PER_CELL, 2048);
        assert_eq!(NUMBER_OF_COLUMNS, 128);
        assert_eq!(NUMBER_OF_CUSTODY_GROUPS, 128);
        assert_eq!(CUSTODY_REQUIREMENT, 4);
        assert_eq!(SAMPLES_PER_SLOT, 8);
        assert_eq!(KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH, 4);
        assert_eq!(Cell::LEN, BYTES_PER_CELL);
    }
}
