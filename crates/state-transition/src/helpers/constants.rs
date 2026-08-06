//! Spec constants used by block/epoch handlers (Electra / Capella / Deneb).
//!
//! Values match consensus-specs; not network-configurable unless noted as
//! network-config constants keyed by preset name.

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Gwei};

/// `FAR_FUTURE_EPOCH = 2**64 - 1`.
pub const FAR_FUTURE_EPOCH: Epoch = Epoch::new(u64::MAX);

/// `GENESIS_EPOCH = 0`.
pub const GENESIS_EPOCH: Epoch = Epoch::new(0);

/// `GENESIS_SLOT = 0`.
pub const GENESIS_SLOT: u64 = 0;

/// Electra `UNSET_DEPOSIT_REQUESTS_START_INDEX = 2**64 - 1`.
pub const UNSET_DEPOSIT_REQUESTS_START_INDEX: u64 = u64::MAX;

/// Electra `FULL_EXIT_REQUEST_AMOUNT = 0` (signals a full exit, not a partial).
pub const FULL_EXIT_REQUEST_AMOUNT: u64 = 0;

/// `MIN_ACTIVATION_BALANCE` (Electra) = 32 ETH in Gwei.
pub const MIN_ACTIVATION_BALANCE: Gwei = Gwei::new(32_000_000_000);

/// `MAX_EFFECTIVE_BALANCE_ELECTRA` = 2048 ETH in Gwei.
pub const MAX_EFFECTIVE_BALANCE_ELECTRA: Gwei = Gwei::new(2_048_000_000_000);

/// Phase0 `MAX_EFFECTIVE_BALANCE` = 32 ETH (still used as non-compounding max).
pub const MAX_EFFECTIVE_BALANCE: Gwei = Gwei::new(32_000_000_000);

/// `EFFECTIVE_BALANCE_INCREMENT` = 1 ETH in Gwei.
pub const EFFECTIVE_BALANCE_INCREMENT: Gwei = Gwei::new(1_000_000_000);

/// Capella `ETH1_ADDRESS_WITHDRAWAL_PREFIX = 0x01`.
pub const ETH1_ADDRESS_WITHDRAWAL_PREFIX: u8 = 0x01;

/// Electra `COMPOUNDING_WITHDRAWAL_PREFIX = 0x02`.
pub const COMPOUNDING_WITHDRAWAL_PREFIX: u8 = 0x02;

/// Phase0 `BLS_WITHDRAWAL_PREFIX = 0x00`.
pub const BLS_WITHDRAWAL_PREFIX: u8 = 0x00;

/// Deneb `VERSIONED_HASH_VERSION_KZG = 0x01`.
pub const VERSIONED_HASH_VERSION_KZG: u8 = 0x01;

/// `DEPOSIT_CONTRACT_TREE_DEPTH = 32`.
pub const DEPOSIT_CONTRACT_TREE_DEPTH: usize = 32;

/// `MIN_ATTESTATION_INCLUSION_DELAY = 1`.
pub const MIN_ATTESTATION_INCLUSION_DELAY: u64 = 1;

/// Altair participation flag indices.
pub const TIMELY_SOURCE_FLAG_INDEX: usize = 0;
/// Altair participation flag indices.
pub const TIMELY_TARGET_FLAG_INDEX: usize = 1;
/// Altair participation flag indices.
pub const TIMELY_HEAD_FLAG_INDEX: usize = 2;

/// Altair incentivization weights.
pub const TIMELY_SOURCE_WEIGHT: u64 = 14;
/// Altair incentivization weights.
pub const TIMELY_TARGET_WEIGHT: u64 = 26;
/// Altair incentivization weights.
pub const TIMELY_HEAD_WEIGHT: u64 = 14;
/// Altair incentivization weights.
pub const PROPOSER_WEIGHT: u64 = 8;
/// Altair `SYNC_REWARD_WEIGHT = 2`.
pub const SYNC_REWARD_WEIGHT: u64 = 2;
/// Altair incentivization weights.
pub const WEIGHT_DENOMINATOR: u64 = 64;

/// `PARTICIPATION_FLAG_WEIGHTS`.
pub const PARTICIPATION_FLAG_WEIGHTS: [u64; 3] =
    [TIMELY_SOURCE_WEIGHT, TIMELY_TARGET_WEIGHT, TIMELY_HEAD_WEIGHT];

/// `BASE_REWARD_FACTOR = 64`.
pub const BASE_REWARD_FACTOR: u64 = 64;

/// `MIN_EPOCHS_TO_INACTIVITY_PENALTY = 4`.
pub const MIN_EPOCHS_TO_INACTIVITY_PENALTY: u64 = 4;

/// Altair `INACTIVITY_SCORE_BIAS = 4`.
pub const INACTIVITY_SCORE_BIAS: u64 = 4;

/// Altair `INACTIVITY_SCORE_RECOVERY_RATE = 16`.
pub const INACTIVITY_SCORE_RECOVERY_RATE: u64 = 16;

/// Bellatrix `INACTIVITY_PENALTY_QUOTIENT_BELLATRIX = 2**24`.
pub const INACTIVITY_PENALTY_QUOTIENT_BELLATRIX: u64 = 16_777_216;

/// `JUSTIFICATION_BITS_LENGTH = 4`.
pub const JUSTIFICATION_BITS_LENGTH: usize = 4;

/// Electra `MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA = 4096`.
pub const MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA: u64 = 4096;

/// Electra `WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA = 4096`.
pub const WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA: u64 = 4096;

/// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY = 256` (mainnet and minimal).
pub const MIN_VALIDATOR_WITHDRAWABILITY_DELAY: u64 = 256;

/// Network `EJECTION_BALANCE` = 16 ETH in Gwei (mainnet and minimal).
pub const EJECTION_BALANCE: Gwei = Gwei::new(16_000_000_000);

/// Phase0 `HYSTERESIS_QUOTIENT = 4`.
pub const HYSTERESIS_QUOTIENT: u64 = 4;

/// Phase0 `HYSTERESIS_DOWNWARD_MULTIPLIER = 1`.
pub const HYSTERESIS_DOWNWARD_MULTIPLIER: u64 = 1;

/// Phase0 `HYSTERESIS_UPWARD_MULTIPLIER = 5`.
pub const HYSTERESIS_UPWARD_MULTIPLIER: u64 = 5;

/// Bellatrix `PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX = 3`.
pub const PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX: u64 = 3;

/// Electra `MAX_PENDING_DEPOSITS_PER_EPOCH = 16`.
pub const MAX_PENDING_DEPOSITS_PER_EPOCH: u64 = 16;

/// `UINT64_MAX_SQRT` for integer square root of `u64::MAX`.
pub const UINT64_MAX_SQRT: u64 = 4_294_967_295;

/// Network-config constants that differ by preset base (not compile-time preset
/// scalars, but fixed per mainnet/minimal config shipped with the pin).
pub mod network {
    use super::*;

    /// `SHARD_COMMITTEE_PERIOD`.
    pub fn shard_committee_period<P: Preset>() -> Epoch {
        match P::NAME {
            "minimal" => Epoch::new(64),
            _ => Epoch::new(256),
        }
    }

    /// `MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA` (Gwei).
    pub fn min_per_epoch_churn_limit_electra<P: Preset>() -> Gwei {
        match P::NAME {
            "minimal" => Gwei::new(64_000_000_000),
            _ => Gwei::new(128_000_000_000),
        }
    }

    /// `MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT` (Gwei).
    pub fn max_per_epoch_activation_exit_churn_limit<P: Preset>() -> Gwei {
        match P::NAME {
            "minimal" => Gwei::new(128_000_000_000),
            _ => Gwei::new(256_000_000_000),
        }
    }

    /// `CHURN_LIMIT_QUOTIENT`.
    pub fn churn_limit_quotient<P: Preset>() -> u64 {
        match P::NAME {
            "minimal" => 32,
            _ => 65_536,
        }
    }

    /// `GENESIS_FORK_VERSION` (mainnet `0x00000000`, minimal `0x00000001`).
    ///
    /// Used for deposit-domain computation (`compute_domain(DOMAIN_DEPOSIT, …)`).
    pub fn genesis_fork_version<P: Preset>() -> cc_types::primitives::ForkVersion {
        match P::NAME {
            "minimal" => cc_types::primitives::ForkVersion::from_array([0, 0, 0, 1]),
            _ => cc_types::primitives::ForkVersion::from_array([0, 0, 0, 0]),
        }
    }
}
