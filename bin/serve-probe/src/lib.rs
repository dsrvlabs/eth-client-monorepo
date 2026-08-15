//! Wire-only serve-window prober (CC-4B / ADR P4-12).
//!
//! Own varint + snappy-frame + result-byte codec against `cc-libp2p` transport
//! (Noise + yamux). Does **not** link `services/p2p`.

pub mod client;
pub mod codec;
pub mod probe;
pub mod protocols;
pub mod report;
pub mod sample;

pub use client::{PeerInfo, ProbeClient, ProbeClientError};
pub use codec::{
    MAX_ERROR_MESSAGE, MAX_PAYLOAD_SIZE, MAX_RESPONSE_CHUNKS, MAX_RESPONSE_UNCOMPRESSED,
    ResponseChunk, ResponseCode, SszLimits, decode_response_chunks, decode_response_chunks_bounded,
    encode_error_chunk, encode_request, encode_success_chunk,
};
pub use probe::{ProbeConfig, ProbeError, ProbeOutcome, SideResult, run_probe};
pub use protocols::{
    BlocksByRangeRequest, BlocksByRootRequest, ColumnsByRangeRequest, ColumnsByRootRequest,
    Protocol, STATUS_V2_SSZ_LEN, StatusV2,
};
pub use report::ProbeReport;
pub use sample::{MAX_SAMPLE_COUNT, NEGATIVE_LOOKBACK_SLOTS, sample_slots};
