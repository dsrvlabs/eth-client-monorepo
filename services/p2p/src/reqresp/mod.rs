//! Req/resp — Architecture §7.1–§7.6 / CC-23a + CC-23b.
//!
//! Stream R:
//! - the nine protocol IDs and the exact-set test (CC-23/1)
//! - SSZ+snappy framing helpers ([`codec`]) with per-chunk `ForkDigest` context
//! - inbound token buckets + outbound in-flight caps ([`limits`])
//! - client-side [`RequestScheduler`] ([`client`])
//! - **CC-23b:** [`status`], [`ping`], [`metadata`], [`handshake`] (incl. Goodbye)
//!
//! Timeouts: [`TTFB_TIMEOUT`] 5 s / [`RESP_TIMEOUT`] 10 s (CC-23/6).

pub mod client;
pub mod codec;
pub mod handshake;
pub mod limits;
pub mod metadata;
pub mod ping;
pub mod status;

pub use client::{
    Exhausted, PeerPredicate, Priority, RequestPayload, RequestScheduler, RequestSpec,
    ScheduleError, SchedulerConfig, DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_PEERS,
};
pub use codec::{
    ResponseChunk, ResponseCode, SszLimits, SszSnappyFraming, CONTEXT_BYTES_LEN, MAX_ERROR_MESSAGE,
    MAX_PAYLOAD_SIZE, RESP_TIMEOUT, TTFB_TIMEOUT,
};
pub use handshake::{
    decode_goodbye_ssz, encode_goodbye_ssz, handle_inbound_goodbye, GoodbyeReceipt, HandshakeBook,
    HandshakeDeps, InboundStatusResult, OutboundAction, PeerHandshakeState,
};
pub use limits::{
    InboundRateLimiter, OutboundLimiter, RateLimitKind, RateLimitOutcome, TokenBucket,
    GLOBAL_MULTIPLIER, INBOUND_BLOCKS_CAPACITY, INBOUND_COLUMNS_CAPACITY, INBOUND_WINDOW,
    OUTBOUND_MAX_IN_FLIGHT_PER_PEER, OUTBOUND_MAX_IN_FLIGHT_PER_PROTOCOL,
    RATE_LIMIT_ERROR_MESSAGE,
};
pub use metadata::{
    decode_metadata_response_framed, decode_metadata_ssz, encode_metadata_request,
    encode_metadata_response, evaluate_peer_cgc, CgcEval, CgcPolicy, LocalMetaData, MetaDataV3,
    METADATA_V3_SSZ_LEN,
};
pub use ping::{
    decode_ping_response_framed, decode_ping_ssz, encode_ping_request, encode_ping_response,
    seq_mismatch, Ping, PING_SSZ_LEN,
};
pub use status::{
    build_local_status, decode_status_response_framed, decode_status_ssz, encode_status_request,
    encode_status_response, evaluate_peer_status, StatusEval, StatusV2, STATUS_V2_SSZ_LEN,
};

use cc_libp2p::reexport::{ProtocolSupport, StreamProtocol};

/// The nine Ethereum req/resp protocols (all `ssz_snappy`).
///
/// Exact set — an extra or missing entry fails [`ALL_PROTOCOL_IDS`] tests (CC-23/1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Protocol {
    /// `/eth2/beacon_chain/req/status/2/`
    StatusV2,
    /// `/eth2/beacon_chain/req/goodbye/1/`
    GoodbyeV1,
    /// `/eth2/beacon_chain/req/ping/1/`
    PingV1,
    /// `/eth2/beacon_chain/req/metadata/3/`
    MetaDataV3,
    /// `/eth2/beacon_chain/req/beacon_blocks_by_range/2/`
    BeaconBlocksByRangeV2,
    /// `/eth2/beacon_chain/req/beacon_blocks_by_root/2/`
    BeaconBlocksByRootV2,
    /// `/eth2/beacon_chain/req/beacon_blocks_by_head/1/` — Fulu (spec delta 2).
    BeaconBlocksByHeadV1,
    /// `/eth2/beacon_chain/req/data_column_sidecars_by_range/1/`
    DataColumnSidecarsByRangeV1,
    /// `/eth2/beacon_chain/req/data_column_sidecars_by_root/1/`
    DataColumnSidecarsByRootV1,
}

impl Protocol {
    /// All nine protocols in stable registration order.
    pub const ALL: [Self; 9] = [
        Self::StatusV2,
        Self::GoodbyeV1,
        Self::PingV1,
        Self::MetaDataV3,
        Self::BeaconBlocksByRangeV2,
        Self::BeaconBlocksByRootV2,
        Self::BeaconBlocksByHeadV1,
        Self::DataColumnSidecarsByRangeV1,
        Self::DataColumnSidecarsByRootV1,
    ];

    /// Full libp2p protocol ID including the `ssz_snappy` encoding suffix.
    #[must_use]
    pub const fn protocol_id(self) -> &'static str {
        match self {
            Self::StatusV2 => "/eth2/beacon_chain/req/status/2/ssz_snappy",
            Self::GoodbyeV1 => "/eth2/beacon_chain/req/goodbye/1/ssz_snappy",
            Self::PingV1 => "/eth2/beacon_chain/req/ping/1/ssz_snappy",
            Self::MetaDataV3 => "/eth2/beacon_chain/req/metadata/3/ssz_snappy",
            Self::BeaconBlocksByRangeV2 => {
                "/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy"
            }
            Self::BeaconBlocksByRootV2 => {
                "/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy"
            }
            Self::BeaconBlocksByHeadV1 => {
                "/eth2/beacon_chain/req/beacon_blocks_by_head/1/ssz_snappy"
            }
            Self::DataColumnSidecarsByRangeV1 => {
                "/eth2/beacon_chain/req/data_column_sidecars_by_range/1/ssz_snappy"
            }
            Self::DataColumnSidecarsByRootV1 => {
                "/eth2/beacon_chain/req/data_column_sidecars_by_root/1/ssz_snappy"
            }
        }
    }

    /// Prometheus / scheduler short name (no leading slash).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StatusV2 => "status",
            Self::GoodbyeV1 => "goodbye",
            Self::PingV1 => "ping",
            Self::MetaDataV3 => "metadata",
            Self::BeaconBlocksByRangeV2 => "beacon_blocks_by_range",
            Self::BeaconBlocksByRootV2 => "beacon_blocks_by_root",
            Self::BeaconBlocksByHeadV1 => "beacon_blocks_by_head",
            Self::DataColumnSidecarsByRangeV1 => "data_column_sidecars_by_range",
            Self::DataColumnSidecarsByRootV1 => "data_column_sidecars_by_root",
        }
    }

    /// Whether successful response chunks carry a 4-byte `ForkDigest` context
    /// (Altair+ block/column family). Status/ping/metadata/goodbye do not.
    #[must_use]
    pub const fn has_context_bytes(self) -> bool {
        matches!(
            self,
            Self::BeaconBlocksByRangeV2
                | Self::BeaconBlocksByRootV2
                | Self::BeaconBlocksByHeadV1
                | Self::DataColumnSidecarsByRangeV1
                | Self::DataColumnSidecarsByRootV1
        )
    }

    /// Whether this protocol's response chunks are metered as **blocks**.
    #[must_use]
    pub const fn is_block_protocol(self) -> bool {
        matches!(
            self,
            Self::BeaconBlocksByRangeV2
                | Self::BeaconBlocksByRootV2
                | Self::BeaconBlocksByHeadV1
        )
    }

    /// Whether this protocol's response chunks are metered as **column sidecars**.
    #[must_use]
    pub const fn is_column_protocol(self) -> bool {
        matches!(
            self,
            Self::DataColumnSidecarsByRangeV1 | Self::DataColumnSidecarsByRootV1
        )
    }

    /// Protocol-declared SSZ size bounds for requests (pre-decompress check).
    #[must_use]
    pub const fn request_limits(self) -> SszLimits {
        match self {
            // Status v2: 4+32+8+32+8+8 = 92 fixed.
            Self::StatusV2 => SszLimits { min: 92, max: 92 },
            // Goodbye / Ping: single uint64.
            Self::GoodbyeV1 | Self::PingV1 => SszLimits { min: 8, max: 8 },
            // MetaData request is empty.
            Self::MetaDataV3 => SszLimits { min: 0, max: 0 },
            // (start_slot, count) — two uint64s. Spec also had step historically;
            // v2 is start+count = 16.
            Self::BeaconBlocksByRangeV2 => SszLimits { min: 16, max: 16 },
            // List[Root, 1024] ≈ 4 + 1024×32 (not 10 MiB).
            Self::BeaconBlocksByRootV2 => SszLimits {
                min: 4,
                max: 4 + 1024 * 32,
            },
            // (beacon_root, count) = 32 + 8.
            Self::BeaconBlocksByHeadV1 => SszLimits { min: 40, max: 40 },
            // Column range/root: SSZ list bound still large; keep ≤ MAX_PAYLOAD_SIZE
            // but live codec also take-bounds compressed side (H1).
            Self::DataColumnSidecarsByRangeV1 | Self::DataColumnSidecarsByRootV1 => SszLimits {
                min: 4,
                max: MAX_PAYLOAD_SIZE,
            },
        }
    }

    /// Protocol-declared SSZ size bounds for a single success response chunk.
    #[must_use]
    pub const fn response_limits(self) -> SszLimits {
        match self {
            Self::StatusV2 => SszLimits { min: 92, max: 92 },
            Self::GoodbyeV1 => SszLimits { min: 0, max: 0 },
            Self::PingV1 => SszLimits { min: 8, max: 8 },
            // MetaData v3: seq + attnets(8) + syncnets(1) + cgc ≈ 8+8+1+8 + bitvector packing.
            Self::MetaDataV3 => SszLimits {
                min: 8,
                max: 256,
            },
            Self::BeaconBlocksByRangeV2
            | Self::BeaconBlocksByRootV2
            | Self::BeaconBlocksByHeadV1
            | Self::DataColumnSidecarsByRangeV1
            | Self::DataColumnSidecarsByRootV1 => SszLimits {
                min: 0,
                max: MAX_PAYLOAD_SIZE,
            },
        }
    }

    /// Parse a negotiated protocol ID string.
    #[must_use]
    pub fn from_protocol_id(id: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|p| p.protocol_id() == id)
    }
}

/// Exact ordered list of the nine protocol-ID strings (CC-23/1).
pub const ALL_PROTOCOL_IDS: [&str; 9] = [
    Protocol::StatusV2.protocol_id(),
    Protocol::GoodbyeV1.protocol_id(),
    Protocol::PingV1.protocol_id(),
    Protocol::MetaDataV3.protocol_id(),
    Protocol::BeaconBlocksByRangeV2.protocol_id(),
    Protocol::BeaconBlocksByRootV2.protocol_id(),
    Protocol::BeaconBlocksByHeadV1.protocol_id(),
    Protocol::DataColumnSidecarsByRangeV1.protocol_id(),
    Protocol::DataColumnSidecarsByRootV1.protocol_id(),
];

/// Registration list for the single multi-protocol `request_response` behaviour.
///
/// Most protocols are [`ProtocolSupport::Full`]. `BeaconBlocksByHead` is
/// **Inbound-only** (served, not initiated client-side — OQ-P2-1 / spec delta 2).
#[must_use]
pub fn ethereum_reqresp_protocols() -> Vec<(StreamProtocol, ProtocolSupport)> {
    Protocol::ALL
        .into_iter()
        .map(|p| {
            let support = if matches!(p, Protocol::BeaconBlocksByHeadV1) {
                ProtocolSupport::Inbound
            } else {
                ProtocolSupport::Full
            };
            (StreamProtocol::new(p.protocol_id()), support)
        })
        .collect()
}

/// [`cc_libp2p::BehaviourConfig`] with eth2 message-id, IDONTWANT, the nine
/// req/resp protocols, and the 15 s request timeout (CC-22b/c + CC-23a).
#[must_use]
pub fn ethereum_behaviour_config() -> cc_libp2p::BehaviourConfig {
    crate::gossip::ethereum_behaviour_config()
        .with_reqresp_protocols(ethereum_reqresp_protocols())
        .with_reqresp_request_timeout(TTFB_TIMEOUT + RESP_TIMEOUT)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn exact_nine_protocol_ids() {
        assert_eq!(Protocol::ALL.len(), 9);
        assert_eq!(ALL_PROTOCOL_IDS.len(), 9);
        let set: BTreeSet<&str> = ALL_PROTOCOL_IDS.into_iter().collect();
        assert_eq!(set.len(), 9, "protocol ids must be unique");
        for (i, p) in Protocol::ALL.iter().enumerate() {
            assert_eq!(p.protocol_id(), ALL_PROTOCOL_IDS[i]);
            assert!(
                p.protocol_id().ends_with("/ssz_snappy"),
                "{} must end with /ssz_snappy",
                p.protocol_id()
            );
        }
        // Spec delta 2: BeaconBlocksByHead v1 is present.
        assert!(
            ALL_PROTOCOL_IDS
                .iter()
                .any(|id| id.contains("beacon_blocks_by_head/1/")),
            "BeaconBlocksByHead v1 must be registered"
        );
    }

    #[test]
    fn ethereum_reqresp_protocols_matches_exact_set() {
        let registered: BTreeSet<String> = ethereum_reqresp_protocols()
            .into_iter()
            .map(|(p, _)| p.to_string())
            .collect();
        let expected: BTreeSet<String> = ALL_PROTOCOL_IDS
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert_eq!(
            registered, expected,
            "registered protocol-ID set must exactly equal the nine"
        );
    }

    #[test]
    fn context_bytes_only_on_block_and_column_families() {
        assert!(!Protocol::StatusV2.has_context_bytes());
        assert!(!Protocol::PingV1.has_context_bytes());
        assert!(!Protocol::MetaDataV3.has_context_bytes());
        assert!(!Protocol::GoodbyeV1.has_context_bytes());
        assert!(Protocol::BeaconBlocksByRangeV2.has_context_bytes());
        assert!(Protocol::BeaconBlocksByHeadV1.has_context_bytes());
        assert!(Protocol::DataColumnSidecarsByRootV1.has_context_bytes());
    }
}
