//! Consensus state transition (CC-12 / CC-13 / CC-14).
//!
//! Module map mirrors `specs/fulu/beacon-chain.md` function names
//! (Architecture §5.1).

#![allow(missing_docs)]

pub mod block;
pub mod engine_seam;
pub mod epoch;
pub mod epoch_cache;
pub mod error;
pub mod helpers;
pub mod root_measure;
pub mod shuffling;
pub mod signatures;
pub mod slots;

pub use block::{
    get_expected_withdrawals, process_attestation, process_attester_slashing, process_block,
    process_block_header, process_bls_to_execution_change, process_consolidation_request,
    process_deposit, process_deposit_request, process_eth1_data, process_execution_payload,
    process_operations, process_proposer_slashing, process_randao, process_sync_aggregate,
    process_sync_aggregate_with_opts, process_voluntary_exit, process_withdrawal_request,
    process_withdrawals, state_transition, ProcessAttestationOpts, TransitionContext,
};
pub use engine_seam::{
    ExecutionEngine, NewPayloadRequest, PayloadStatus, StubOptimisticEngine, VersionedHash,
};
pub use epoch::{
    apply_pending_deposit, get_flag_index_deltas, get_inactivity_penalty_deltas,
    process_effective_balance_updates, process_epoch, process_eth1_data_reset,
    process_historical_summaries_update, process_inactivity_updates,
    process_justification_and_finalization, process_participation_flag_updates,
    process_pending_consolidations, process_pending_deposits, process_proposer_lookahead,
    process_randao_mixes_reset, process_registry_updates, process_rewards_and_penalties,
    process_slashings, process_slashings_reset, process_sync_committee_updates,
    weigh_justification_and_finalization, RewardPenalties,
};
pub use error::{
    BlockError, EngineError, EpochError, GossipClass, OperationError, SignatureKind,
};
pub use epoch_cache::{
    base_reward_per_increment_cached, invalidate_epoch_cache, note_registry_or_effective_balance_change,
    rebuild_epoch_cache, total_active_balance_cached,
};
pub use helpers::{
    compute_epoch_at_slot, compute_time_at_slot, decrease_balance, get_beacon_proposer_index,
    get_current_epoch, get_max_effective_balance, get_randao_mix, increase_balance,
    kzg_commitment_to_versioned_hash,
};
pub use shuffling::{
    compute_proposer_index, compute_proposer_indices, compute_shuffled_active_indices,
    decision_root_for_epoch, get_beacon_committee, get_beacon_proposer_indices,
    get_committee_count_per_slot, get_next_sync_committee, get_next_sync_committee_indices,
    get_or_compute_shuffling,
};
pub use root_measure::{
    canonical_root_call_count, measured_canonical_root, take_canonical_root_call_count,
};
pub use signatures::{
    decode_block_pubkey, decode_pubkey, decode_signature, decode_state_pubkey,
    push_block_proposer_signature, push_operation_signatures, push_randao_signature,
    verify_block_signatures, BlockSignatureSet, LabelledSignature,
};
pub use slots::{process_slot, process_slots};

/// Block-signature verification strategy demanded by the vectors
/// (`meta.yaml` `bls_setting`) and the runtime default (Architecture §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BlockSignatureStrategy {
    /// Spec-shaped; default for vectors that assert a specific failure.
    VerifyIndividual,
    /// One [`cc_crypto::SignatureSet`] for the whole block — runtime default.
    #[default]
    VerifyBatch,
    /// `bls_setting: 0` vector cases, and re-processing a block already verified.
    NoVerification,
}

impl BlockSignatureStrategy {
    /// Map a harness [`BlsSetting`] integer (0/1/2) onto this strategy.
    ///
    /// - `0` ([`cc_spec_tests::BlsSetting::Optional`]) → [`Self::NoVerification`]
    /// - `1` (required) → [`Self::VerifyIndividual`]
    /// - `2` (must-fail / BLS ignored) → [`Self::VerifyIndividual`] so failures attribute
    pub fn from_bls_setting_u8(v: u8) -> Self {
        match v {
            0 => Self::NoVerification,
            // Required and must-fail both run verification; must-fail cases
            // expect `Err` under verification.
            1 | 2 => Self::VerifyIndividual,
            _ => Self::VerifyBatch,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::BlockSignatureStrategy;

    #[test]
    fn from_bls_setting_u8_mapping() {
        assert_eq!(
            BlockSignatureStrategy::from_bls_setting_u8(0),
            BlockSignatureStrategy::NoVerification
        );
        assert_eq!(
            BlockSignatureStrategy::from_bls_setting_u8(1),
            BlockSignatureStrategy::VerifyIndividual
        );
        assert_eq!(
            BlockSignatureStrategy::from_bls_setting_u8(2),
            BlockSignatureStrategy::VerifyIndividual
        );
        // Unknown / future values fall back to the runtime default path.
        assert_eq!(
            BlockSignatureStrategy::from_bls_setting_u8(3),
            BlockSignatureStrategy::VerifyBatch
        );
        assert_eq!(
            BlockSignatureStrategy::from_bls_setting_u8(255),
            BlockSignatureStrategy::VerifyBatch
        );
    }

    #[test]
    fn default_strategy_is_verify_batch() {
        assert_eq!(
            BlockSignatureStrategy::default(),
            BlockSignatureStrategy::VerifyBatch
        );
    }
}
