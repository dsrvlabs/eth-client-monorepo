//! Git-pinned libp2p edge: transport, `CcBehaviour`, `SnappyTransform`, scoring
//! translation, and the narrow re-export set (CC-20a / Architecture §3).
//!
//! **Frozen after this issue (D-9):** further changes are cross-stream requests
//! against the CC-20a owner, not local edits by consumers.
//!
//! **Zero workspace dependencies permanently** (Architecture §1.2). Downstream
//! crates take libp2p surfaces through this crate only — never via a direct
//! `libp2p*` Cargo dependency.

mod behaviour;
mod limits;
mod scoring;
mod snappy;
mod ssz_snappy_codec;
mod swarm;
mod transport;

pub mod reexport;

pub use behaviour::{
    is_control_reqresp_protocol, BehaviourBuildError, BehaviourConfig, CcBehaviour,
    CcBehaviourEvent, MessageIdFn, MAX_RESPONSE_STREAM_BYTES, REQRESP_MAX_PAYLOAD_SIZE,
    RESP_TIMEOUT, ReqRespRequest, ReqRespResponse, SszSnappyCodec, TTFB_TIMEOUT,
    decode_ssz_snappy_payload, default_eth2_message_id, encode_ssz_snappy_payload, request_limits,
    response_stream_cap,
};
pub use limits::{
    DEFAULT_CONNECTION_TIMEOUT, DEFAULT_IDLE_CONNECTION_TIMEOUT, DEFAULT_MAX_ESTABLISHED_PER_PEER,
    DEFAULT_MAX_INBOUND, DEFAULT_MAX_OUTBOUND, DEFAULT_MAX_PEERS, DEFAULT_MAX_PENDING_INCOMING,
    DEFAULT_MAX_PENDING_OUTGOING, DEFAULT_YAMUX_MAX_NUM_STREAMS, IDENTIFY_AGENT, IDENTIFY_PROTOCOL,
    connection_limits, default_connection_limits,
};
pub use scoring::{ScoringConfig, TopicScoringConfig, build_peer_score_params};
pub use snappy::{GOSSIP_MAX_SIZE, SnappyTransform};
pub use swarm::{SwarmBuildError, SwarmConfig, build_swarm};
pub use transport::{
    QuicConfig, TransportBuildError, TransportConfig, build_transport, quic_feature_is_compiled,
};

/// 40-hex git rev of `https://github.com/libp2p/rust-libp2p` from root
/// `[workspace.dependencies]`. Greppable OQ-7 compensating control (CC-20/4):
/// must match `Cargo.toml`, `docs/p2p-dependencies.md`, and the supply-chain
/// Phase 2 section. Enforced by `scripts/check-crate-dag.sh`.
pub const LIBP2P_GIT_REV: &str = "6348a0be4aeb5b48eecf17a5d0aae15ff8239984";

/// `libp2p-metrics` [`Metrics`] type for the P2P service sub-registry
/// (CC-29a / §3.4). Minimal reexport so `services/p2p` never depends on
/// `libp2p*` crates directly (full `libp2p::metrics` module is not reexported).
pub use libp2p::metrics::Metrics;

// Convenient root reexports of the hottest identity / multiaddr types.
pub use reexport::{Keypair, Multiaddr, PeerId, Swarm, SwarmEvent};

#[cfg(test)]
mod tests {
    use super::LIBP2P_GIT_REV;

    #[test]
    fn libp2p_git_rev_is_40_lowercase_hex() {
        assert_eq!(LIBP2P_GIT_REV.len(), 40);
        assert!(
            LIBP2P_GIT_REV
                .chars()
                .all(|c| matches!(c, '0'..='9' | 'a'..='f')),
            "LIBP2P_GIT_REV must be lowercase 40-hex, got {LIBP2P_GIT_REV:?}"
        );
    }
}
