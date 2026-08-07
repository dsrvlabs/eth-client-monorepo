//! Engine API method adapters (CC-30a skeleton).
//!
//! Transport lanes and the JSON-RPC client land in this issue; method bodies
//! (`newPayloadV4`, `forkchoiceUpdatedV3`, `getBlobsV2`, `eth_syncing`) and the
//! capability handshake are `CC-31` / `CC-32` / `CC-33` / `CC-36` / `CC-37`.

/// JSON-RPC method name strings used on the wire (version-suffixed per common.md).
pub mod names {
    pub const NEW_PAYLOAD_V4: &str = "engine_newPayloadV4";
    pub const FORKCHOICE_UPDATED_V3: &str = "engine_forkchoiceUpdatedV3";
    pub const GET_BLOBS_V2: &str = "engine_getBlobsV2";
    pub const EXCHANGE_CAPABILITIES: &str = "engine_exchangeCapabilities";
    pub const ETH_SYNCING: &str = "eth_syncing";
}
