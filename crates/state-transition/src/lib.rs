//! Consensus state transition (CC-12 / CC-13 / CC-14).
//!
//! Module map mirrors `specs/fulu/beacon-chain.md` function names
//! (Architecture §5.1).

#![allow(missing_docs)]

pub mod block;
pub mod engine_seam;
pub mod epoch;
pub mod error;
pub mod helpers;
pub mod root_measure;
pub mod signatures;
pub mod slots;

pub use block::{
    get_expected_withdrawals, process_block, process_block_header, process_eth1_data,
    process_execution_payload, process_operations, process_randao, process_sync_aggregate,
    process_withdrawals, state_transition, TransitionContext,
};
pub use engine_seam::{
    ExecutionEngine, NewPayloadRequest, PayloadStatus, StubOptimisticEngine, VersionedHash,
};
pub use epoch::process_epoch;
pub use error::{
    BlockError, EngineError, EpochError, GossipClass, OperationError, SignatureKind,
};
pub use helpers::{
    compute_epoch_at_slot, compute_time_at_slot, decrease_balance, get_beacon_proposer_index,
    get_current_epoch, get_max_effective_balance, get_randao_mix, increase_balance,
    kzg_commitment_to_versioned_hash,
};
pub use root_measure::{
    canonical_root_call_count, measured_canonical_root, take_canonical_root_call_count,
};
pub use signatures::{
    decode_block_pubkey, decode_pubkey, decode_signature, decode_state_pubkey,
    push_block_proposer_signature, push_randao_signature, verify_block_signatures,
    BlockSignatureSet, LabelledSignature,
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
