//! Preset parameterisation: const scalars **and** typenum associated types (Architecture §2.1–§2.2).
//!
//! Derived lengths (`MaxValidatorsPerSlot`, `ProposerLookaheadLen`) are **declared** with hand-done
//! arithmetic — never `typenum::Prod` — so `Mul` bounds do not leak into every signature.

use std::fmt::Debug;

use typenum::{
    U1, U2, U4, U8, U16, U32, U64, U128, U256, U512, U2048, U4096, U8192, U65536, U131072, U262144,
    U1048576, U16777216, U134217728, U1073741824, U1099511627776, Unsigned,
};

/// Bound shared by every capacity associated type on [`Preset`].
///
/// `Eq` is required so container `#[derive(Eq)]` does not demand bounds on every
/// `P::Max*` associated type at each use site (Architecture §3.2).
pub trait PresetUnsigned:
    Unsigned + Clone + Sync + Send + Debug + PartialEq + Eq + 'static
{
}
impl<T> PresetUnsigned for T where
    T: Unsigned + Clone + Sync + Send + Debug + PartialEq + Eq + 'static
{
}

/// Compile-time chain preset (mainnet / minimal).
///
/// Scalar `const`s feed state-transition arithmetic; associated typenum types feed
/// `ssz_types::{FixedVector, VariableList, BitList, BitVector}` capacities. A unit test per
/// preset asserts the two agree for every pair.
pub trait Preset:
    'static + Default + Clone + Copy + PartialEq + Eq + Debug + Send + Sync + Unpin
{
    /// `"mainnet"` or `"minimal"`.
    const NAME: &'static str;

    // --- scalar arithmetic (state transition) --------------------------------

    /// `SLOTS_PER_EPOCH`
    const SLOTS_PER_EPOCH: u64;
    /// `MAX_COMMITTEES_PER_SLOT`
    const MAX_COMMITTEES_PER_SLOT: u64;
    /// `TARGET_COMMITTEE_SIZE`
    const TARGET_COMMITTEE_SIZE: u64;
    /// `MAX_VALIDATORS_PER_COMMITTEE`
    const MAX_VALIDATORS_PER_COMMITTEE: u64;
    /// `SHUFFLE_ROUND_COUNT`
    const SHUFFLE_ROUND_COUNT: u8;
    /// `MIN_SEED_LOOKAHEAD`
    const MIN_SEED_LOOKAHEAD: u64;
    /// `MAX_SEED_LOOKAHEAD`
    const MAX_SEED_LOOKAHEAD: u64;
    /// `EPOCHS_PER_ETH1_VOTING_PERIOD`
    const EPOCHS_PER_ETH1_VOTING_PERIOD: u64;
    /// `SLOTS_PER_HISTORICAL_ROOT`
    const SLOTS_PER_HISTORICAL_ROOT: u64;
    /// `EPOCHS_PER_HISTORICAL_VECTOR`
    const EPOCHS_PER_HISTORICAL_VECTOR: u64;
    /// `EPOCHS_PER_SLASHINGS_VECTOR`
    const EPOCHS_PER_SLASHINGS_VECTOR: u64;
    /// `HISTORICAL_ROOTS_LIMIT`
    const HISTORICAL_ROOTS_LIMIT: u64;
    /// `VALIDATOR_REGISTRY_LIMIT`
    const VALIDATOR_REGISTRY_LIMIT: u64;
    /// `SYNC_COMMITTEE_SIZE`
    const SYNC_COMMITTEE_SIZE: u64;
    /// `EPOCHS_PER_SYNC_COMMITTEE_PERIOD`
    const EPOCHS_PER_SYNC_COMMITTEE_PERIOD: u64;
    /// `MAX_BLOB_COMMITMENTS_PER_BLOCK`
    const MAX_BLOB_COMMITMENTS_PER_BLOCK: u64;
    /// `MAX_PROPOSER_SLASHINGS`
    const MAX_PROPOSER_SLASHINGS: u64;
    /// `MAX_ATTESTER_SLASHINGS` (phase0; Electra uses `MAX_ATTESTER_SLASHINGS_ELECTRA`)
    const MAX_ATTESTER_SLASHINGS: u64;
    /// `MAX_ATTESTER_SLASHINGS_ELECTRA`
    const MAX_ATTESTER_SLASHINGS_ELECTRA: u64;
    /// `MAX_ATTESTATIONS` (phase0)
    const MAX_ATTESTATIONS: u64;
    /// `MAX_ATTESTATIONS_ELECTRA`
    const MAX_ATTESTATIONS_ELECTRA: u64;
    /// `MAX_DEPOSITS`
    const MAX_DEPOSITS: u64;
    /// `MAX_VOLUNTARY_EXITS`
    const MAX_VOLUNTARY_EXITS: u64;
    /// `MAX_BLS_TO_EXECUTION_CHANGES`
    const MAX_BLS_TO_EXECUTION_CHANGES: u64;
    /// `MAX_WITHDRAWALS_PER_PAYLOAD`
    const MAX_WITHDRAWALS_PER_PAYLOAD: u64;
    /// `MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP`
    const MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP: u64;
    /// Electra `MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP` (mainnet 8, minimal 2).
    const MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP: u64;
    /// `MAX_BYTES_PER_TRANSACTION`
    const MAX_BYTES_PER_TRANSACTION: u64;
    /// `MAX_TRANSACTIONS_PER_PAYLOAD`
    const MAX_TRANSACTIONS_PER_PAYLOAD: u64;
    /// `BYTES_PER_LOGS_BLOOM`
    const BYTES_PER_LOGS_BLOOM: u64;
    /// `MAX_EXTRA_DATA_BYTES`
    const MAX_EXTRA_DATA_BYTES: u64;
    /// `PENDING_DEPOSITS_LIMIT`
    const PENDING_DEPOSITS_LIMIT: u64;
    /// `PENDING_PARTIAL_WITHDRAWALS_LIMIT`
    const PENDING_PARTIAL_WITHDRAWALS_LIMIT: u64;
    /// `PENDING_CONSOLIDATIONS_LIMIT`
    const PENDING_CONSOLIDATIONS_LIMIT: u64;
    /// `MAX_DEPOSIT_REQUESTS_PER_PAYLOAD`
    const MAX_DEPOSIT_REQUESTS_PER_PAYLOAD: u64;
    /// `MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD`
    const MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD: u64;
    /// `MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD`
    const MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD: u64;
    /// Electra-era base blob bound; sole runtime consumer is the pre-schedule
    /// fallback inside `BlobSchedule::get_blob_parameters` (Architecture §5.6).
    const MAX_BLOBS_PER_BLOCK_BASE: u64;
    /// `SYNC_COMMITTEE_SUBNET_COUNT` (Altair validator; always 4).
    const SYNC_COMMITTEE_SUBNET_COUNT: u64;

    // --- derived scalars (hand-computed; asserted in tests) ------------------

    /// `MAX_VALIDATORS_PER_COMMITTEE × MAX_COMMITTEES_PER_SLOT`
    const MAX_VALIDATORS_PER_SLOT: u64;
    /// `(MIN_SEED_LOOKAHEAD + 1) × SLOTS_PER_EPOCH`
    const PROPOSER_LOOKAHEAD_LEN: u64;
    /// `EPOCHS_PER_ETH1_VOTING_PERIOD × SLOTS_PER_EPOCH`
    const ETH1_DATA_VOTES_LENGTH: u64;
    /// `SYNC_COMMITTEE_SIZE / SYNC_COMMITTEE_SUBNET_COUNT`
    const SYNC_SUBCOMMITTEE_SIZE: u64;

    // --- typenum capacities (ssz_types generics) -----------------------------

    /// typenum of `SLOTS_PER_HISTORICAL_ROOT`
    type SlotsPerHistoricalRoot: PresetUnsigned;
    /// typenum of `EPOCHS_PER_HISTORICAL_VECTOR`
    type EpochsPerHistoricalVector: PresetUnsigned;
    /// typenum of `EPOCHS_PER_SLASHINGS_VECTOR`
    type EpochsPerSlashingsVector: PresetUnsigned;
    /// typenum of `HISTORICAL_ROOTS_LIMIT`
    type HistoricalRootsLimit: PresetUnsigned;
    /// typenum of `VALIDATOR_REGISTRY_LIMIT`
    type ValidatorRegistryLimit: PresetUnsigned;
    /// typenum of `MAX_VALIDATORS_PER_COMMITTEE`
    type MaxValidatorsPerCommittee: PresetUnsigned;
    /// typenum of `MAX_COMMITTEES_PER_SLOT`
    type MaxCommitteesPerSlot: PresetUnsigned;
    /// typenum of `MAX_VALIDATORS_PER_SLOT` (derived, declared)
    type MaxValidatorsPerSlot: PresetUnsigned;
    /// typenum of `SYNC_COMMITTEE_SIZE`
    type SyncCommitteeSize: PresetUnsigned;
    /// typenum of `MAX_BLOB_COMMITMENTS_PER_BLOCK`
    type MaxBlobCommitmentsPerBlock: PresetUnsigned;
    /// typenum of `PROPOSER_LOOKAHEAD_LEN` (derived, declared)
    type ProposerLookaheadLen: PresetUnsigned;
    /// typenum of `MAX_PROPOSER_SLASHINGS`
    type MaxProposerSlashings: PresetUnsigned;
    /// typenum of `MAX_ATTESTER_SLASHINGS_ELECTRA`
    type MaxAttesterSlashingsElectra: PresetUnsigned;
    /// typenum of `MAX_ATTESTATIONS_ELECTRA`
    type MaxAttestationsElectra: PresetUnsigned;
    /// typenum of `MAX_DEPOSITS`
    type MaxDeposits: PresetUnsigned;
    /// typenum of `MAX_VOLUNTARY_EXITS`
    type MaxVoluntaryExits: PresetUnsigned;
    /// typenum of `MAX_BLS_TO_EXECUTION_CHANGES`
    type MaxBlsToExecutionChanges: PresetUnsigned;
    /// typenum of `MAX_WITHDRAWALS_PER_PAYLOAD`
    type MaxWithdrawalsPerPayload: PresetUnsigned;
    /// typenum of `MAX_BYTES_PER_TRANSACTION`
    type MaxBytesPerTransaction: PresetUnsigned;
    /// typenum of `MAX_TRANSACTIONS_PER_PAYLOAD`
    type MaxTransactionsPerPayload: PresetUnsigned;
    /// typenum of `BYTES_PER_LOGS_BLOOM`
    type BytesPerLogsBloom: PresetUnsigned;
    /// typenum of `MAX_EXTRA_DATA_BYTES`
    type MaxExtraDataBytes: PresetUnsigned;
    /// typenum of `PENDING_DEPOSITS_LIMIT`
    type PendingDepositsLimit: PresetUnsigned;
    /// typenum of `PENDING_PARTIAL_WITHDRAWALS_LIMIT`
    type PendingPartialWithdrawalsLimit: PresetUnsigned;
    /// typenum of `PENDING_CONSOLIDATIONS_LIMIT`
    type PendingConsolidationsLimit: PresetUnsigned;
    /// typenum of `MAX_DEPOSIT_REQUESTS_PER_PAYLOAD`
    type MaxDepositRequestsPerPayload: PresetUnsigned;
    /// typenum of `MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD`
    type MaxWithdrawalRequestsPerPayload: PresetUnsigned;
    /// typenum of `MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD`
    type MaxConsolidationRequestsPerPayload: PresetUnsigned;
    /// typenum of `SLOTS_PER_EPOCH`
    type SlotsPerEpoch: PresetUnsigned;
    /// typenum of `EPOCHS_PER_ETH1_VOTING_PERIOD` (eth1 data votes list length uses slots product)
    type EpochsPerEth1VotingPeriod: PresetUnsigned;
    /// typenum of `ETH1_DATA_VOTES_LENGTH` (derived, declared)
    type Eth1DataVotesLength: PresetUnsigned;
    /// typenum of `SYNC_SUBCOMMITTEE_SIZE` (derived, declared)
    type SyncSubcommitteeSize: PresetUnsigned;
}

// ---------------------------------------------------------------------------
// Mainnet
// ---------------------------------------------------------------------------

/// Mainnet (and Hoodi) preset values from consensus-specs `presets/mainnet/*`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Mainnet;

impl Preset for Mainnet {
    const NAME: &'static str = "mainnet";

    const SLOTS_PER_EPOCH: u64 = 32;
    const MAX_COMMITTEES_PER_SLOT: u64 = 64;
    const TARGET_COMMITTEE_SIZE: u64 = 128;
    const MAX_VALIDATORS_PER_COMMITTEE: u64 = 2048;
    const SHUFFLE_ROUND_COUNT: u8 = 90;
    const MIN_SEED_LOOKAHEAD: u64 = 1;
    const MAX_SEED_LOOKAHEAD: u64 = 4;
    const EPOCHS_PER_ETH1_VOTING_PERIOD: u64 = 64;
    const SLOTS_PER_HISTORICAL_ROOT: u64 = 8192;
    const EPOCHS_PER_HISTORICAL_VECTOR: u64 = 65536;
    const EPOCHS_PER_SLASHINGS_VECTOR: u64 = 8192;
    const HISTORICAL_ROOTS_LIMIT: u64 = 16_777_216;
    const VALIDATOR_REGISTRY_LIMIT: u64 = 1_099_511_627_776;
    const SYNC_COMMITTEE_SIZE: u64 = 512;
    const EPOCHS_PER_SYNC_COMMITTEE_PERIOD: u64 = 256;
    const MAX_BLOB_COMMITMENTS_PER_BLOCK: u64 = 4096;
    const MAX_PROPOSER_SLASHINGS: u64 = 16;
    const MAX_ATTESTER_SLASHINGS: u64 = 2;
    const MAX_ATTESTER_SLASHINGS_ELECTRA: u64 = 1;
    const MAX_ATTESTATIONS: u64 = 128;
    const MAX_ATTESTATIONS_ELECTRA: u64 = 8;
    const MAX_DEPOSITS: u64 = 16;
    const MAX_VOLUNTARY_EXITS: u64 = 16;
    const MAX_BLS_TO_EXECUTION_CHANGES: u64 = 16;
    const MAX_WITHDRAWALS_PER_PAYLOAD: u64 = 16;
    const MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP: u64 = 16_384;
    const MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP: u64 = 8;
    const MAX_BYTES_PER_TRANSACTION: u64 = 1_073_741_824;
    const MAX_TRANSACTIONS_PER_PAYLOAD: u64 = 1_048_576;
    const BYTES_PER_LOGS_BLOOM: u64 = 256;
    const MAX_EXTRA_DATA_BYTES: u64 = 32;
    const PENDING_DEPOSITS_LIMIT: u64 = 134_217_728;
    const PENDING_PARTIAL_WITHDRAWALS_LIMIT: u64 = 134_217_728;
    const PENDING_CONSOLIDATIONS_LIMIT: u64 = 262_144;
    const MAX_DEPOSIT_REQUESTS_PER_PAYLOAD: u64 = 8192;
    const MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD: u64 = 16;
    const MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD: u64 = 2;
    // Electra base (`MAX_BLOBS_PER_BLOCK_ELECTRA` in network config.yaml).
    const MAX_BLOBS_PER_BLOCK_BASE: u64 = 9;
    const SYNC_COMMITTEE_SUBNET_COUNT: u64 = 4;

    // Derived by hand (Architecture §2.2).
    const MAX_VALIDATORS_PER_SLOT: u64 = 2048 * 64; // 131_072
    const PROPOSER_LOOKAHEAD_LEN: u64 = (1 + 1) * 32; // 64
    const ETH1_DATA_VOTES_LENGTH: u64 = 64 * 32; // 2048
    const SYNC_SUBCOMMITTEE_SIZE: u64 = 512 / 4; // 128

    type SlotsPerEpoch = U32;
    type EpochsPerEth1VotingPeriod = U64;
    type SlotsPerHistoricalRoot = U8192;
    type EpochsPerHistoricalVector = U65536;
    type EpochsPerSlashingsVector = U8192;
    type HistoricalRootsLimit = U16777216;
    type ValidatorRegistryLimit = U1099511627776;
    type MaxValidatorsPerCommittee = U2048;
    type MaxCommitteesPerSlot = U64;
    type MaxValidatorsPerSlot = U131072;
    type SyncCommitteeSize = U512;
    type MaxBlobCommitmentsPerBlock = U4096;
    type ProposerLookaheadLen = U64;
    type MaxProposerSlashings = U16;
    type MaxAttesterSlashingsElectra = U1;
    type MaxAttestationsElectra = U8;
    type MaxDeposits = U16;
    type MaxVoluntaryExits = U16;
    type MaxBlsToExecutionChanges = U16;
    type MaxWithdrawalsPerPayload = U16;
    type MaxBytesPerTransaction = U1073741824;
    type MaxTransactionsPerPayload = U1048576;
    type BytesPerLogsBloom = U256;
    type MaxExtraDataBytes = U32;
    type PendingDepositsLimit = U134217728;
    type PendingPartialWithdrawalsLimit = U134217728;
    type PendingConsolidationsLimit = U262144;
    type MaxDepositRequestsPerPayload = U8192;
    type MaxWithdrawalRequestsPerPayload = U16;
    type MaxConsolidationRequestsPerPayload = U2;
    type Eth1DataVotesLength = U2048;
    type SyncSubcommitteeSize = U128;
}

// ---------------------------------------------------------------------------
// Minimal
// ---------------------------------------------------------------------------

/// Minimal preset values from consensus-specs `presets/minimal/*`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Minimal;

impl Preset for Minimal {
    const NAME: &'static str = "minimal";

    const SLOTS_PER_EPOCH: u64 = 8;
    const MAX_COMMITTEES_PER_SLOT: u64 = 4;
    const TARGET_COMMITTEE_SIZE: u64 = 4;
    const MAX_VALIDATORS_PER_COMMITTEE: u64 = 2048;
    const SHUFFLE_ROUND_COUNT: u8 = 10;
    const MIN_SEED_LOOKAHEAD: u64 = 1;
    const MAX_SEED_LOOKAHEAD: u64 = 4;
    const EPOCHS_PER_ETH1_VOTING_PERIOD: u64 = 4;
    const SLOTS_PER_HISTORICAL_ROOT: u64 = 64;
    const EPOCHS_PER_HISTORICAL_VECTOR: u64 = 64;
    const EPOCHS_PER_SLASHINGS_VECTOR: u64 = 64;
    const HISTORICAL_ROOTS_LIMIT: u64 = 16_777_216;
    const VALIDATOR_REGISTRY_LIMIT: u64 = 1_099_511_627_776;
    const SYNC_COMMITTEE_SIZE: u64 = 32;
    const EPOCHS_PER_SYNC_COMMITTEE_PERIOD: u64 = 8;
    const MAX_BLOB_COMMITMENTS_PER_BLOCK: u64 = 4096;
    const MAX_PROPOSER_SLASHINGS: u64 = 16;
    const MAX_ATTESTER_SLASHINGS: u64 = 2;
    const MAX_ATTESTER_SLASHINGS_ELECTRA: u64 = 1;
    const MAX_ATTESTATIONS: u64 = 128;
    const MAX_ATTESTATIONS_ELECTRA: u64 = 8;
    const MAX_DEPOSITS: u64 = 16;
    const MAX_VOLUNTARY_EXITS: u64 = 16;
    const MAX_BLS_TO_EXECUTION_CHANGES: u64 = 16;
    const MAX_WITHDRAWALS_PER_PAYLOAD: u64 = 4;
    const MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP: u64 = 16;
    /// Minimal electra.yaml: customized `2**1` (= 2).
    const MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP: u64 = 2;
    const MAX_BYTES_PER_TRANSACTION: u64 = 1_073_741_824;
    const MAX_TRANSACTIONS_PER_PAYLOAD: u64 = 1_048_576;
    const BYTES_PER_LOGS_BLOOM: u64 = 256;
    const MAX_EXTRA_DATA_BYTES: u64 = 32;
    const PENDING_DEPOSITS_LIMIT: u64 = 134_217_728;
    const PENDING_PARTIAL_WITHDRAWALS_LIMIT: u64 = 64;
    const PENDING_CONSOLIDATIONS_LIMIT: u64 = 64;
    const MAX_DEPOSIT_REQUESTS_PER_PAYLOAD: u64 = 8192;
    const MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD: u64 = 16;
    const MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD: u64 = 2;
    const MAX_BLOBS_PER_BLOCK_BASE: u64 = 9;
    const SYNC_COMMITTEE_SUBNET_COUNT: u64 = 4;

    // Derived by hand.
    const MAX_VALIDATORS_PER_SLOT: u64 = 2048 * 4; // 8192
    const PROPOSER_LOOKAHEAD_LEN: u64 = (1 + 1) * 8; // 16
    const ETH1_DATA_VOTES_LENGTH: u64 = 4 * 8; // 32
    const SYNC_SUBCOMMITTEE_SIZE: u64 = 32 / 4; // 8

    type SlotsPerEpoch = U8;
    type EpochsPerEth1VotingPeriod = U4;
    type SlotsPerHistoricalRoot = U64;
    type EpochsPerHistoricalVector = U64;
    type EpochsPerSlashingsVector = U64;
    type HistoricalRootsLimit = U16777216;
    type ValidatorRegistryLimit = U1099511627776;
    type MaxValidatorsPerCommittee = U2048;
    type MaxCommitteesPerSlot = U4;
    type MaxValidatorsPerSlot = U8192;
    type SyncCommitteeSize = U32;
    type MaxBlobCommitmentsPerBlock = U4096;
    type ProposerLookaheadLen = U16;
    type MaxProposerSlashings = U16;
    type MaxAttesterSlashingsElectra = U1;
    type MaxAttestationsElectra = U8;
    type MaxDeposits = U16;
    type MaxVoluntaryExits = U16;
    type MaxBlsToExecutionChanges = U16;
    type MaxWithdrawalsPerPayload = U4;
    type MaxBytesPerTransaction = U1073741824;
    type MaxTransactionsPerPayload = U1048576;
    type BytesPerLogsBloom = U256;
    type MaxExtraDataBytes = U32;
    type PendingDepositsLimit = U134217728;
    type PendingPartialWithdrawalsLimit = U64;
    type PendingConsolidationsLimit = U64;
    type MaxDepositRequestsPerPayload = U8192;
    type MaxWithdrawalRequestsPerPayload = U16;
    type MaxConsolidationRequestsPerPayload = U2;
    type Eth1DataVotesLength = U32;
    type SyncSubcommitteeSize = U8;
}
