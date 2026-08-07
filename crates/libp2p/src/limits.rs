//! Connection-limit helpers for [`connection_limits::Behaviour`] (Architecture §3.6).
//!
//! Policy (who/when) lives in the peer manager (CC-20c); hard enforcement is the
//! swarm-level behaviour constructed from these defaults.
//!
//! Pending and per-peer caps are freeze-load-bearing: without them a flood of
//! half-open handshakes can bypass the established caps (SEC H1 / M1).

use std::time::Duration;

use libp2p::connection_limits::ConnectionLimits;

/// Default max established peers (inbound + outbound combined soft-target ceiling).
pub const DEFAULT_MAX_PEERS: u32 = 100;
/// Default max established inbound connections.
pub const DEFAULT_MAX_INBOUND: u32 = 60;
/// Default max established outbound connections.
pub const DEFAULT_MAX_OUTBOUND: u32 = 60;
/// Default max concurrent pending (not yet established) inbound connections.
///
/// Aligned with [`DEFAULT_MAX_PEERS`]: absorb a burst of dials without letting
/// handshake state grow unbounded past the established ceiling.
pub const DEFAULT_MAX_PENDING_INCOMING: u32 = 100;
/// Default max concurrent pending outbound connections.
///
/// Aligned with peer-manager max concurrent dials (§3.6: 8).
pub const DEFAULT_MAX_PENDING_OUTGOING: u32 = 8;
/// Default max established connections **per remote peer** (eth2: typically 1).
pub const DEFAULT_MAX_ESTABLISHED_PER_PEER: u32 = 1;

/// Build [`ConnectionLimits`] with Architecture §3.6 defaults plus pending/per-peer caps.
///
/// - `max_established` = 100
/// - `max_established_incoming` = 60
/// - `max_established_outgoing` = 60
/// - `max_pending_incoming` = 100
/// - `max_pending_outgoing` = 8
/// - `max_established_per_peer` = 1
pub fn default_connection_limits() -> ConnectionLimits {
    ConnectionLimits::default()
        .with_max_established(Some(DEFAULT_MAX_PEERS))
        .with_max_established_incoming(Some(DEFAULT_MAX_INBOUND))
        .with_max_established_outgoing(Some(DEFAULT_MAX_OUTBOUND))
        .with_max_pending_incoming(Some(DEFAULT_MAX_PENDING_INCOMING))
        .with_max_pending_outgoing(Some(DEFAULT_MAX_PENDING_OUTGOING))
        .with_max_established_per_peer(Some(DEFAULT_MAX_ESTABLISHED_PER_PEER))
}

/// Build limits from explicit knobs (peer-manager policy feeds these).
///
/// Pending and per-peer caps use the crate defaults so callers that only
/// override established counts still get flood protection.
pub fn connection_limits(
    max_established: Option<u32>,
    max_incoming: Option<u32>,
    max_outgoing: Option<u32>,
) -> ConnectionLimits {
    ConnectionLimits::default()
        .with_max_established(max_established)
        .with_max_established_incoming(max_incoming)
        .with_max_established_outgoing(max_outgoing)
        .with_max_pending_incoming(Some(DEFAULT_MAX_PENDING_INCOMING))
        .with_max_pending_outgoing(Some(DEFAULT_MAX_PENDING_OUTGOING))
        .with_max_established_per_peer(Some(DEFAULT_MAX_ESTABLISHED_PER_PEER))
}

/// Identify protocol / agent string defaults used when building [`crate::CcBehaviour`].
pub const IDENTIFY_PROTOCOL: &str = "/cc/identify/1.0.0";
pub const IDENTIFY_AGENT: &str = "cc-libp2p/0.1.0";

/// Default idle connection timeout applied in [`crate::build_swarm`] (30 s).
pub const DEFAULT_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Default transport connection / upgrade timeout (15 s).
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// Default yamux max concurrent streams per connection.
pub const DEFAULT_YAMUX_MAX_NUM_STREAMS: usize = 512;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limits_set_pending_and_per_peer() {
        // `ConnectionLimits` fields are private; construct via defaults and assert
        // the builder chain is exercised by re-applying the same knobs (no panic)
        // and documenting the constants are the freeze values.
        assert_eq!(DEFAULT_MAX_PENDING_INCOMING, DEFAULT_MAX_PEERS);
        assert_eq!(DEFAULT_MAX_PENDING_OUTGOING, 8);
        assert_eq!(DEFAULT_MAX_ESTABLISHED_PER_PEER, 1);
        let _ = default_connection_limits();
        let _ = connection_limits(Some(50), Some(30), Some(30));
    }
}
