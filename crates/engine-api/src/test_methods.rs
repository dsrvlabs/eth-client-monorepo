//! Test-only method surface so moved `transport` / `config` / `state` compile.

use crate::capabilities::CapabilityCache;
use crate::errors::EngineError;
use crate::metrics::EngineMetrics;
use crate::transport::EngineTransport;
use crate::version::ElForkSchedule;
use serde_json::Value;

pub mod names {
    pub const NEW_PAYLOAD_V4: &str = "engine_newPayloadV4";
    pub const FORKCHOICE_UPDATED_V3: &str = "engine_forkchoiceUpdatedV3";
    pub const GET_BLOBS_V2: &str = "engine_getBlobsV2";
    pub const EXCHANGE_CAPABILITIES: &str = "engine_exchangeCapabilities";
    pub const ETH_SYNCING: &str = "eth_syncing";
}

pub mod eth_syncing {
    pub use super::EthSyncingResult;

    #[allow(clippy::unused_async)]
    pub async fn eth_syncing(
        _transport: &super::EngineTransport,
        _metrics: Option<&super::EngineMetrics>,
    ) -> Result<EthSyncingResult, super::EngineError> {
        Ok(EthSyncingResult::NotSyncing)
    }
}

/// Outcome of a single `eth_syncing` probe (shape matches `cc-engine`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EthSyncingResult {
    NotSyncing,
    Syncing(Value),
}

pub mod fcu {
    #[allow(clippy::too_many_arguments, clippy::unused_async)]
    pub async fn forkchoice_updated_v3(
        _transport: &super::EngineTransport,
        _schedule: &super::ElForkSchedule,
        _metrics: Option<&super::EngineMetrics>,
        _head_block_hash: &[u8],
        _safe_block_hash: &[u8],
        _finalized_block_hash: &[u8],
        _head_slot: Option<u64>,
    ) -> Result<(), super::EngineError> {
        Ok(())
    }
}

pub mod capabilities {
    #[allow(clippy::unused_async)]
    pub async fn exchange_capabilities(
        _transport: &super::EngineTransport,
        _cache: &super::CapabilityCache,
        _metrics: Option<&super::EngineMetrics>,
    ) -> Result<(), super::EngineError> {
        Ok(())
    }
}
