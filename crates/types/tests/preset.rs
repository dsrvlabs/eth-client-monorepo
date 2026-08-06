//! Per-preset agreement: every typenum associated type equals its scalar const,
//! and both derived lengths equal their hand-computed products (Architecture §2.1–§2.2).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use cc_types::{Mainnet, Minimal, Preset};
use typenum::Unsigned;

fn assert_preset_pairs<P: Preset>() {
    // Direct scalar ↔ typenum pairs.
    assert_eq!(
        P::SlotsPerEpoch::U64,
        P::SLOTS_PER_EPOCH,
        "{} SlotsPerEpoch",
        P::NAME
    );
    assert_eq!(
        P::EpochsPerEth1VotingPeriod::U64,
        P::EPOCHS_PER_ETH1_VOTING_PERIOD,
        "{} EpochsPerEth1VotingPeriod",
        P::NAME
    );
    assert_eq!(
        P::SlotsPerHistoricalRoot::U64,
        P::SLOTS_PER_HISTORICAL_ROOT,
        "{} SlotsPerHistoricalRoot",
        P::NAME
    );
    assert_eq!(
        P::EpochsPerHistoricalVector::U64,
        P::EPOCHS_PER_HISTORICAL_VECTOR,
        "{} EpochsPerHistoricalVector",
        P::NAME
    );
    assert_eq!(
        P::EpochsPerSlashingsVector::U64,
        P::EPOCHS_PER_SLASHINGS_VECTOR,
        "{} EpochsPerSlashingsVector",
        P::NAME
    );
    assert_eq!(
        P::HistoricalRootsLimit::U64,
        P::HISTORICAL_ROOTS_LIMIT,
        "{} HistoricalRootsLimit",
        P::NAME
    );
    assert_eq!(
        P::ValidatorRegistryLimit::U64,
        P::VALIDATOR_REGISTRY_LIMIT,
        "{} ValidatorRegistryLimit",
        P::NAME
    );
    assert_eq!(
        P::MaxValidatorsPerCommittee::U64,
        P::MAX_VALIDATORS_PER_COMMITTEE,
        "{} MaxValidatorsPerCommittee",
        P::NAME
    );
    assert_eq!(
        P::MaxCommitteesPerSlot::U64,
        P::MAX_COMMITTEES_PER_SLOT,
        "{} MaxCommitteesPerSlot",
        P::NAME
    );
    assert_eq!(
        P::SyncCommitteeSize::U64,
        P::SYNC_COMMITTEE_SIZE,
        "{} SyncCommitteeSize",
        P::NAME
    );
    assert_eq!(
        P::MaxBlobCommitmentsPerBlock::U64,
        P::MAX_BLOB_COMMITMENTS_PER_BLOCK,
        "{} MaxBlobCommitmentsPerBlock",
        P::NAME
    );
    assert_eq!(
        P::MaxProposerSlashings::U64,
        P::MAX_PROPOSER_SLASHINGS,
        "{} MaxProposerSlashings",
        P::NAME
    );
    assert_eq!(
        P::MaxAttesterSlashingsElectra::U64,
        P::MAX_ATTESTER_SLASHINGS_ELECTRA,
        "{} MaxAttesterSlashingsElectra",
        P::NAME
    );
    assert_eq!(
        P::MaxAttestationsElectra::U64,
        P::MAX_ATTESTATIONS_ELECTRA,
        "{} MaxAttestationsElectra",
        P::NAME
    );
    assert_eq!(
        P::MaxDeposits::U64,
        P::MAX_DEPOSITS,
        "{} MaxDeposits",
        P::NAME
    );
    assert_eq!(
        P::MaxVoluntaryExits::U64,
        P::MAX_VOLUNTARY_EXITS,
        "{} MaxVoluntaryExits",
        P::NAME
    );
    assert_eq!(
        P::MaxBlsToExecutionChanges::U64,
        P::MAX_BLS_TO_EXECUTION_CHANGES,
        "{} MaxBlsToExecutionChanges",
        P::NAME
    );
    assert_eq!(
        P::MaxWithdrawalsPerPayload::U64,
        P::MAX_WITHDRAWALS_PER_PAYLOAD,
        "{} MaxWithdrawalsPerPayload",
        P::NAME
    );
    assert_eq!(
        P::MaxBytesPerTransaction::U64,
        P::MAX_BYTES_PER_TRANSACTION,
        "{} MaxBytesPerTransaction",
        P::NAME
    );
    assert_eq!(
        P::MaxTransactionsPerPayload::U64,
        P::MAX_TRANSACTIONS_PER_PAYLOAD,
        "{} MaxTransactionsPerPayload",
        P::NAME
    );
    assert_eq!(
        P::BytesPerLogsBloom::U64,
        P::BYTES_PER_LOGS_BLOOM,
        "{} BytesPerLogsBloom",
        P::NAME
    );
    assert_eq!(
        P::MaxExtraDataBytes::U64,
        P::MAX_EXTRA_DATA_BYTES,
        "{} MaxExtraDataBytes",
        P::NAME
    );
    assert_eq!(
        P::PendingDepositsLimit::U64,
        P::PENDING_DEPOSITS_LIMIT,
        "{} PendingDepositsLimit",
        P::NAME
    );
    assert_eq!(
        P::PendingPartialWithdrawalsLimit::U64,
        P::PENDING_PARTIAL_WITHDRAWALS_LIMIT,
        "{} PendingPartialWithdrawalsLimit",
        P::NAME
    );
    assert_eq!(
        P::PendingConsolidationsLimit::U64,
        P::PENDING_CONSOLIDATIONS_LIMIT,
        "{} PendingConsolidationsLimit",
        P::NAME
    );
    assert_eq!(
        P::MaxDepositRequestsPerPayload::U64,
        P::MAX_DEPOSIT_REQUESTS_PER_PAYLOAD,
        "{} MaxDepositRequestsPerPayload",
        P::NAME
    );
    assert_eq!(
        P::MaxWithdrawalRequestsPerPayload::U64,
        P::MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD,
        "{} MaxWithdrawalRequestsPerPayload",
        P::NAME
    );
    assert_eq!(
        P::MaxConsolidationRequestsPerPayload::U64,
        P::MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD,
        "{} MaxConsolidationRequestsPerPayload",
        P::NAME
    );

    // Derived lengths: typenum ↔ scalar, and scalar ↔ hand product.
    assert_eq!(
        P::MaxValidatorsPerSlot::U64,
        P::MAX_VALIDATORS_PER_SLOT,
        "{} MaxValidatorsPerSlot typenum",
        P::NAME
    );
    assert_eq!(
        P::MAX_VALIDATORS_PER_SLOT,
        P::MAX_VALIDATORS_PER_COMMITTEE * P::MAX_COMMITTEES_PER_SLOT,
        "{} MaxValidatorsPerSlot product",
        P::NAME
    );

    assert_eq!(
        P::ProposerLookaheadLen::U64,
        P::PROPOSER_LOOKAHEAD_LEN,
        "{} ProposerLookaheadLen typenum",
        P::NAME
    );
    assert_eq!(
        P::PROPOSER_LOOKAHEAD_LEN,
        (P::MIN_SEED_LOOKAHEAD + 1) * P::SLOTS_PER_EPOCH,
        "{} ProposerLookaheadLen product",
        P::NAME
    );

    assert_eq!(
        P::Eth1DataVotesLength::U64,
        P::ETH1_DATA_VOTES_LENGTH,
        "{} Eth1DataVotesLength typenum",
        P::NAME
    );
    assert_eq!(
        P::ETH1_DATA_VOTES_LENGTH,
        P::EPOCHS_PER_ETH1_VOTING_PERIOD * P::SLOTS_PER_EPOCH,
        "{} Eth1DataVotesLength product",
        P::NAME
    );

    assert_eq!(
        P::SyncSubcommitteeSize::U64,
        P::SYNC_SUBCOMMITTEE_SIZE,
        "{} SyncSubcommitteeSize typenum",
        P::NAME
    );
    assert_eq!(
        P::SYNC_SUBCOMMITTEE_SIZE,
        P::SYNC_COMMITTEE_SIZE / P::SYNC_COMMITTEE_SUBNET_COUNT,
        "{} SyncSubcommitteeSize product",
        P::NAME
    );
    assert_eq!(P::SYNC_COMMITTEE_SUBNET_COUNT, 4);
}

#[test]
fn mainnet_const_typenum_and_derived_agree() {
    assert_eq!(Mainnet::NAME, "mainnet");
    assert_preset_pairs::<Mainnet>();
}

#[test]
fn minimal_const_typenum_and_derived_agree() {
    assert_eq!(Minimal::NAME, "minimal");
    assert_preset_pairs::<Minimal>();
}
