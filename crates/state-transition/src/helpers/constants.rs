//! Spec constants used by block/epoch handlers (Electra / Capella / Deneb).
//!
//! Values match consensus-specs; not network-configurable.

use cc_types::primitives::{Epoch, Gwei};

/// `FAR_FUTURE_EPOCH = 2**64 - 1`.
pub const FAR_FUTURE_EPOCH: Epoch = Epoch::new(u64::MAX);

/// `MIN_ACTIVATION_BALANCE` (Electra) = 32 ETH in Gwei.
pub const MIN_ACTIVATION_BALANCE: Gwei = Gwei::new(32_000_000_000);

/// `MAX_EFFECTIVE_BALANCE_ELECTRA` = 2048 ETH in Gwei.
pub const MAX_EFFECTIVE_BALANCE_ELECTRA: Gwei = Gwei::new(2_048_000_000_000);

/// Capella `ETH1_ADDRESS_WITHDRAWAL_PREFIX = 0x01`.
pub const ETH1_ADDRESS_WITHDRAWAL_PREFIX: u8 = 0x01;

/// Electra `COMPOUNDING_WITHDRAWAL_PREFIX = 0x02`.
pub const COMPOUNDING_WITHDRAWAL_PREFIX: u8 = 0x02;

/// Deneb `VERSIONED_HASH_VERSION_KZG = 0x01`.
pub const VERSIONED_HASH_VERSION_KZG: u8 = 0x01;
