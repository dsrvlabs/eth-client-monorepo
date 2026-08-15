//! Test-only wire names so moved `transport` / `config` keep `crate::methods::names`.

pub mod names {
    pub const NEW_PAYLOAD_V4: &str = "engine_newPayloadV4";
    pub const FORKCHOICE_UPDATED_V3: &str = "engine_forkchoiceUpdatedV3";
    pub const GET_BLOBS_V2: &str = "engine_getBlobsV2";
    pub const EXCHANGE_CAPABILITIES: &str = "engine_exchangeCapabilities";
    pub const ETH_SYNCING: &str = "eth_syncing";
}
