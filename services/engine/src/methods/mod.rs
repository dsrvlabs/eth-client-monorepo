//! Engine API method adapters.
//!
//! - **CC-30a**: transport lanes + method name constants
//! - **CC-31**: capability handshake (`exchangeCapabilities`), discovery of
//!   `engine_getBlobsV3` without adoption
//! - **CC-32** / **CC-33** / **CC-36** / **CC-37**: method bodies (`newPayloadV4`,
//!   `forkchoiceUpdatedV3`, `getBlobsV2`, `eth_syncing`)

pub mod capabilities;
pub mod eth_syncing;
pub mod fcu;
pub mod get_blobs;
pub mod new_payload;

/// JSON-RPC method name strings used on the wire (version-suffixed per common.md).
///
/// Phase 3 surface: V4 / V3 / V2 only. Do not add Amsterdam-era method names
/// or getBlobs cell-mode methods here (see CC-31 / Architecture §3.3–§3.4).
pub mod names {
    pub const NEW_PAYLOAD_V4: &str = "engine_newPayloadV4";
    pub const FORKCHOICE_UPDATED_V3: &str = "engine_forkchoiceUpdatedV3";
    pub const GET_BLOBS_V2: &str = "engine_getBlobsV2";
    pub const EXCHANGE_CAPABILITIES: &str = "engine_exchangeCapabilities";
    pub const ETH_SYNCING: &str = "eth_syncing";
}
