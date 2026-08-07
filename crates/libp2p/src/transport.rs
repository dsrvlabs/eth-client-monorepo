//! TCP + Noise (XX) + yamux transport with DNS; QUIC compiled but config-off
//! (Architecture §3.3 / CC-2F readiness).

use std::time::Duration;

use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::core::upgrade::Version;
use libp2p::identity::Keypair;
use libp2p::{PeerId, Transport, dns, noise, tcp, yamux};

use crate::limits::{DEFAULT_CONNECTION_TIMEOUT, DEFAULT_YAMUX_MAX_NUM_STREAMS};

/// QUIC sub-config. Feature is compiled; runtime enable is CC-2F.
#[derive(Debug, Clone, Default)]
pub struct QuicConfig {
    /// When `false` (default), [`build_transport`] is TCP-only.
    pub enabled: bool,
}

/// Transport construction knobs (Architecture §3.3 defaults).
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// TCP `TCP_NODELAY` — default **on** (disable Nagle).
    pub nodelay: bool,
    /// Yamux max concurrent streams per connection (default 512).
    ///
    /// Note: at pin `6348a0be…`, `libp2p_yamux::Config` only exposes
    /// [`yamux::Config::set_max_num_streams`]. The architecture's 4 MiB
    /// receive-window is not settable via the public wrapper (underlying
    /// `yamux` defaults apply).
    pub yamux_max_num_streams: usize,
    /// Connection / security / muxer upgrade timeout (default 15 s).
    pub connection_timeout: Duration,
    /// QUIC — compiled, default disabled.
    pub quic: QuicConfig,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            yamux_max_num_streams: DEFAULT_YAMUX_MAX_NUM_STREAMS,
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            quic: QuicConfig::default(),
        }
    }
}

/// Build the authenticated + multiplexed transport.
///
/// Stack: DNS(TCP) → Noise XX → yamux, with [`TransportConfig::connection_timeout`].
/// When `cfg.quic.enabled` is false (default), QUIC is **not** composed in —
/// the built transport is TCP-only. Enabling QUIC is CC-2F (`OrTransport`).
pub fn build_transport(
    keypair: &Keypair,
    cfg: &TransportConfig,
) -> Result<Boxed<(PeerId, StreamMuxerBox)>, TransportBuildError> {
    if cfg.quic.enabled {
        // Feature is compiled so CC-2F is a config flip + OrTransport line.
        // Wiring the QUIC upgrade path is intentionally deferred.
        let _quic_feature_compiled = std::any::type_name::<libp2p::quic::Config>();
        let _ = _quic_feature_compiled;
        return Err(TransportBuildError::QuicNotYetWired);
    }

    let noise_config =
        noise::Config::new(keypair).map_err(|e| TransportBuildError::Noise(e.to_string()))?;

    let mut yamux_config = yamux::Config::default();
    yamux_config.set_max_num_streams(cfg.yamux_max_num_streams);

    let tcp_config = tcp::Config::default().nodelay(cfg.nodelay);
    let tcp = tcp::tokio::Transport::new(tcp_config);

    // DNS for bootnode multiaddrs (`/dns4/…`, `/dns6/…`).
    let transport =
        dns::tokio::Transport::system(tcp).map_err(|e| TransportBuildError::Dns(e.to_string()))?;

    let transport = transport
        .upgrade(Version::V1Lazy)
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(cfg.connection_timeout)
        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
        .boxed();

    Ok(transport)
}

/// Errors from [`build_transport`].
#[derive(Debug)]
pub enum TransportBuildError {
    /// Noise XX key setup failed.
    Noise(String),
    /// DNS transport system resolver failed.
    Dns(String),
    /// `transport.quic.enabled = true` but QUIC composition is CC-2F.
    QuicNotYetWired,
}

impl std::fmt::Display for TransportBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noise(e) => write!(f, "noise config: {e}"),
            Self::Dns(e) => write!(f, "dns transport: {e}"),
            Self::QuicNotYetWired => write!(
                f,
                "quic enabled in config but transport wiring is CC-2F (feature is compiled)"
            ),
        }
    }
}

impl std::error::Error for TransportBuildError {}

/// Ensure the `quic` feature path is referenced so the crate fails to compile
/// if the workspace pin drops `quic`.
#[inline]
pub fn quic_feature_is_compiled() -> bool {
    // Touch a quic type without constructing a runtime transport.
    let name = std::any::type_name::<libp2p::quic::Config>();
    name.contains("quic")
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn default_is_tcp_only_quic_disabled() {
        let cfg = TransportConfig::default();
        assert!(!cfg.quic.enabled);
        assert!(cfg.nodelay);
        assert_eq!(cfg.yamux_max_num_streams, 512);
        assert_eq!(cfg.connection_timeout, Duration::from_secs(15));
        assert!(quic_feature_is_compiled());
    }

    #[test]
    fn build_transport_succeeds_with_defaults() {
        let kp = Keypair::generate_secp256k1();
        let t = build_transport(&kp, &TransportConfig::default());
        assert!(t.is_ok());
    }

    #[test]
    fn quic_enabled_returns_not_yet_wired() {
        let kp = Keypair::generate_secp256k1();
        let mut cfg = TransportConfig::default();
        cfg.quic.enabled = true;
        match build_transport(&kp, &cfg) {
            Err(TransportBuildError::QuicNotYetWired) => {}
            other => panic!("expected QuicNotYetWired, got {other:?}"),
        }
    }
}
