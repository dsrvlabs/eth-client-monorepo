//! Execution-layer payload containers (Architecture §3.1, Capella/Deneb shape).
//!
//! Fulu does not reshape `ExecutionPayload` / `ExecutionPayloadHeader` beyond
//! Deneb (`blob_gas_used`, `excess_blob_gas`).

use alloy_primitives::U256;
use ssz_derive::{Decode, Encode};
use ssz_types::{FixedVector, VariableList};
use tree_hash_derive::TreeHash;

use crate::operations::Withdrawal;
use crate::preset::Preset;
use crate::primitives::{ExecutionAddress, Root};

/// Opaque execution transaction (`ByteList[MAX_BYTES_PER_TRANSACTION]`).
pub type Transaction<P> = VariableList<u8, <P as Preset>::MaxBytesPerTransaction>;

/// Spec `ExecutionPayload` (Deneb+).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct ExecutionPayload<P: Preset> {
    /// Parent execution block hash.
    pub parent_hash: Root,
    /// Fee recipient.
    pub fee_recipient: ExecutionAddress,
    /// Execution state root.
    pub state_root: Root,
    /// Receipts root.
    pub receipts_root: Root,
    /// Logs bloom filter.
    pub logs_bloom: FixedVector<u8, P::BytesPerLogsBloom>,
    /// Previous RANDAO mix.
    pub prev_randao: Root,
    /// Execution block number.
    pub block_number: u64,
    /// Gas limit.
    pub gas_limit: u64,
    /// Gas used.
    pub gas_used: u64,
    /// Timestamp.
    pub timestamp: u64,
    /// Extra data.
    pub extra_data: VariableList<u8, P::MaxExtraDataBytes>,
    /// Base fee per gas (`uint256`).
    pub base_fee_per_gas: U256,
    /// Execution block hash.
    pub block_hash: Root,
    /// Transactions.
    pub transactions: VariableList<Transaction<P>, P::MaxTransactionsPerPayload>,
    /// Withdrawals (Capella+).
    pub withdrawals: VariableList<Withdrawal, P::MaxWithdrawalsPerPayload>,
    /// Blob gas used (Deneb+).
    pub blob_gas_used: u64,
    /// Excess blob gas (Deneb+).
    pub excess_blob_gas: u64,
}

impl<P: Preset> Default for ExecutionPayload<P> {
    fn default() -> Self {
        Self {
            parent_hash: Root::default(),
            fee_recipient: ExecutionAddress::default(),
            state_root: Root::default(),
            receipts_root: Root::default(),
            logs_bloom: FixedVector::default(),
            prev_randao: Root::default(),
            block_number: 0,
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: VariableList::default(),
            base_fee_per_gas: U256::ZERO,
            block_hash: Root::default(),
            transactions: VariableList::default(),
            withdrawals: VariableList::default(),
            blob_gas_used: 0,
            excess_blob_gas: 0,
        }
    }
}

/// Spec `ExecutionPayloadHeader` (Deneb+).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct ExecutionPayloadHeader<P: Preset> {
    /// Parent execution block hash.
    pub parent_hash: Root,
    /// Fee recipient.
    pub fee_recipient: ExecutionAddress,
    /// Execution state root.
    pub state_root: Root,
    /// Receipts root.
    pub receipts_root: Root,
    /// Logs bloom filter.
    pub logs_bloom: FixedVector<u8, P::BytesPerLogsBloom>,
    /// Previous RANDAO mix.
    pub prev_randao: Root,
    /// Execution block number.
    pub block_number: u64,
    /// Gas limit.
    pub gas_limit: u64,
    /// Gas used.
    pub gas_used: u64,
    /// Timestamp.
    pub timestamp: u64,
    /// Extra data.
    pub extra_data: VariableList<u8, P::MaxExtraDataBytes>,
    /// Base fee per gas (`uint256`).
    pub base_fee_per_gas: U256,
    /// Execution block hash.
    pub block_hash: Root,
    /// Root of the transactions list.
    pub transactions_root: Root,
    /// Root of the withdrawals list (Capella+).
    pub withdrawals_root: Root,
    /// Blob gas used (Deneb+).
    pub blob_gas_used: u64,
    /// Excess blob gas (Deneb+).
    pub excess_blob_gas: u64,
}

impl<P: Preset> Default for ExecutionPayloadHeader<P> {
    fn default() -> Self {
        Self {
            parent_hash: Root::default(),
            fee_recipient: ExecutionAddress::default(),
            state_root: Root::default(),
            receipts_root: Root::default(),
            logs_bloom: FixedVector::default(),
            prev_randao: Root::default(),
            block_number: 0,
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: VariableList::default(),
            base_fee_per_gas: U256::ZERO,
            block_hash: Root::default(),
            transactions_root: Root::default(),
            withdrawals_root: Root::default(),
            blob_gas_used: 0,
            excess_blob_gas: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::preset::Mainnet;
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;

    #[test]
    fn execution_payload_default_roundtrip() {
        let p = ExecutionPayload::<Mainnet>::default();
        let bytes = p.as_ssz_bytes();
        assert_eq!(
            ExecutionPayload::<Mainnet>::from_ssz_bytes(&bytes).unwrap(),
            p
        );
        let _ = p.tree_hash_root();
    }

    #[test]
    fn truncated_execution_payload_returns_err() {
        let p = ExecutionPayload::<Mainnet>::default();
        let bytes = p.as_ssz_bytes();
        let truncated = &bytes[..bytes.len().saturating_sub(1)];
        assert!(ExecutionPayload::<Mainnet>::from_ssz_bytes(truncated).is_err());
    }

    #[test]
    fn overlong_execution_payload_returns_err() {
        let p = ExecutionPayload::<Mainnet>::default();
        let mut bytes = p.as_ssz_bytes();
        bytes.extend_from_slice(&[0xaa, 0xbb]);
        assert!(ExecutionPayload::<Mainnet>::from_ssz_bytes(&bytes).is_err());
    }

    #[test]
    fn no_panic_on_garbage_execution_payload() {
        let garbage = [0u8; 16];
        let result = std::panic::catch_unwind(|| {
            let _ = ExecutionPayload::<Mainnet>::from_ssz_bytes(&garbage);
        });
        assert!(result.is_ok(), "decode must not panic");
        assert!(ExecutionPayload::<Mainnet>::from_ssz_bytes(&garbage).is_err());
    }
}
