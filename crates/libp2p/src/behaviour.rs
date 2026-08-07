//! [`CcBehaviour`] composition (Architecture §3.2).
//!
//! One `request_response::Behaviour` for all nine Ethereum protocols (codec
//! body is CC-23a — type stub only here). libp2p `ping` is kept alongside
//! Ethereum `/eth2/…/ping/1/` (different jobs); see `docs/p2p-dependencies.md`
//! §Deviations.
//!
//! ## Message-id (CC-22b / SEC C1)
//!
//! Gossipsub's default id is **not** eth2-compatible and causes cross-client
//! duplicate-suppression failure. [`BehaviourConfig`] always installs a
//! message-id function: the eth2 Altair+ preimage by default, overridable so
//! `services/p2p` can inject its scaffold-owned implementation from
//! `gossip/topics.rs`.
//!
//! With [`SnappyTransform`], gossipsub computes the id **after** successful
//! decompression, so the production path always uses
//! `MESSAGE_DOMAIN_VALID_SNAPPY` over the decompressed payload. Transform
//! failures drop the message before the id function runs — the invalid-snappy
//! domain is for offline / hostile fixtures, not the live transform path.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures::AsyncRead as FuturesAsyncRead;
use futures::AsyncWrite as FuturesAsyncWrite;
use libp2p::allow_block_list::BlockedPeers;
use libp2p::connection_limits::{self, ConnectionLimits};
use libp2p::gossipsub::{
    self, AllowAllSubscriptionFilter, ConfigBuilder as GossipsubConfigBuilder, Message,
    MessageAuthenticity, MessageId, ValidationMode,
};
use libp2p::identity::Keypair;
use libp2p::request_response::{self, Codec, ProtocolSupport};
use libp2p::swarm::{NetworkBehaviour, StreamProtocol};
use libp2p::{allow_block_list, identify, ping};
use sha2::{Digest, Sha256};

use crate::limits::{IDENTIFY_AGENT, IDENTIFY_PROTOCOL, default_connection_limits};
use crate::snappy::{GOSSIP_MAX_SIZE, SnappyTransform};

/// Gossipsub message-id callback installed on [`CcBehaviour`].
///
/// Signature matches `libp2p::gossipsub::ConfigBuilder::message_id_fn`.
pub type MessageIdFn =
    Arc<dyn Fn(&Message) -> MessageId + Send + Sync + 'static>;

/// Eth2 Altair+ message-id over a **successfully snappy-decoded** payload.
///
/// Preimage: `SHA256(MESSAGE_DOMAIN_VALID_SNAPPY ‖ le64(len(topic)) ‖ topic ‖ data)[:20]`.
/// Used as the secure default when no override is supplied (SEC C1).
///
/// # Note on invalid-snappy
///
/// [`SnappyTransform::inbound_transform`] rejects failed decompressions before
/// gossipsub calls this function, so the live path never needs
/// `MESSAGE_DOMAIN_INVALID_SNAPPY`. That domain remains for offline fixtures
/// and any future path that bypasses the transform.
#[must_use]
pub fn default_eth2_message_id(message: &Message) -> MessageId {
    // DomainType('0x01000000') little-endian — specs/phase0/p2p-interface.md.
    const MESSAGE_DOMAIN_VALID_SNAPPY: [u8; 4] = [0x01, 0x00, 0x00, 0x00];
    let topic = message.topic.as_str().as_bytes();
    let mut hasher = Sha256::new();
    hasher.update(MESSAGE_DOMAIN_VALID_SNAPPY);
    hasher.update((topic.len() as u64).to_le_bytes());
    hasher.update(topic);
    hasher.update(&message.data);
    let digest = hasher.finalize();
    MessageId::new(&digest[..20])
}

/// Placeholder codec type for the single multi-protocol `request_response`
/// behaviour. Wire body (SSZ+snappy framing, nine protocol names) is **CC-23a**.
#[derive(Debug, Clone, Default)]
pub struct SszSnappyCodec;

impl Codec for SszSnappyCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        _io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: FuturesAsyncRead + Unpin + Send,
    {
        Err(io::Error::other(
            "SszSnappyCodec::read_request body is CC-23a",
        ))
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        _io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: FuturesAsyncRead + Unpin + Send,
    {
        Err(io::Error::other(
            "SszSnappyCodec::read_response body is CC-23a",
        ))
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        _io: &mut T,
        _req: Self::Request,
    ) -> io::Result<()>
    where
        T: FuturesAsyncWrite + Unpin + Send,
    {
        Err(io::Error::other(
            "SszSnappyCodec::write_request body is CC-23a",
        ))
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        _io: &mut T,
        _res: Self::Response,
    ) -> io::Result<()>
    where
        T: FuturesAsyncWrite + Unpin + Send,
    {
        Err(io::Error::other(
            "SszSnappyCodec::write_response body is CC-23a",
        ))
    }
}

/// Composite network behaviour for the consensus client (Architecture §3.2).
///
/// No kad / mdns / autonat / relay / upnp.
#[derive(NetworkBehaviour)]
pub struct CcBehaviour {
    pub gossipsub: gossipsub::Behaviour<SnappyTransform, AllowAllSubscriptionFilter>,
    pub reqresp: request_response::Behaviour<SszSnappyCodec>,
    pub identify: identify::Behaviour,
    /// libp2p ping (RTT / liveness) — **not** Ethereum `/eth2/beacon_chain/req/ping/1/`.
    pub ping: ping::Behaviour,
    pub limits: connection_limits::Behaviour,
    pub allow_block: allow_block_list::Behaviour<BlockedPeers>,
}

impl std::fmt::Debug for CcBehaviour {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Child behaviours do not uniformly implement Debug; name the composition only.
        f.debug_struct("CcBehaviour")
            .field("gossipsub", &"gossipsub::Behaviour<SnappyTransform>")
            .field("reqresp", &"request_response::Behaviour<SszSnappyCodec>")
            .field("identify", &"identify::Behaviour")
            .field("ping", &"ping::Behaviour")
            .field("limits", &"connection_limits::Behaviour")
            .field("allow_block", &"allow_block_list::Behaviour<BlockedPeers>")
            .finish()
    }
}

/// Options for constructing [`CcBehaviour`].
#[derive(Clone)]
pub struct BehaviourConfig {
    /// Gossipsub max transmit size (compressed). Default [`GOSSIP_MAX_SIZE`].
    pub max_transmit_size: usize,
    /// Snappy uncompressed ceiling. Default [`GOSSIP_MAX_SIZE`].
    pub max_uncompressed: usize,
    /// Gossipsub heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Duplicate message cache TTL.
    pub duplicate_cache_time: Duration,
    /// Identify protocol version string.
    pub identify_protocol: String,
    /// Identify agent version string.
    pub identify_agent: String,
    /// Connection limits (swarm-level hard caps).
    pub connection_limits: ConnectionLimits,
    /// Protocols registered on the single `request_response` behaviour.
    /// Empty until CC-23a registers the nine Ethereum protocols.
    pub reqresp_protocols: Vec<(StreamProtocol, ProtocolSupport)>,
    /// Gossipsub message-id function (SEC C1 / CC-22b).
    ///
    /// Defaults to [`default_eth2_message_id`]. `services/p2p` should override
    /// with its scaffold-owned implementation so the committed fixture and the
    /// live path share one source of truth.
    pub message_id_fn: MessageIdFn,
}

impl std::fmt::Debug for BehaviourConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BehaviourConfig")
            .field("max_transmit_size", &self.max_transmit_size)
            .field("max_uncompressed", &self.max_uncompressed)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("duplicate_cache_time", &self.duplicate_cache_time)
            .field("identify_protocol", &self.identify_protocol)
            .field("identify_agent", &self.identify_agent)
            .field("connection_limits", &"ConnectionLimits{…}")
            .field("reqresp_protocols", &self.reqresp_protocols)
            .field("message_id_fn", &"<MessageIdFn>")
            .finish()
    }
}

impl Default for BehaviourConfig {
    fn default() -> Self {
        Self {
            max_transmit_size: GOSSIP_MAX_SIZE,
            max_uncompressed: GOSSIP_MAX_SIZE,
            heartbeat_interval: Duration::from_secs(1),
            duplicate_cache_time: Duration::from_secs(60),
            identify_protocol: IDENTIFY_PROTOCOL.to_string(),
            identify_agent: IDENTIFY_AGENT.to_string(),
            connection_limits: default_connection_limits(),
            reqresp_protocols: Vec::new(),
            // Secure default: eth2 Altair+ id (never libp2p's seqno/from hash).
            message_id_fn: Arc::new(default_eth2_message_id),
        }
    }
}

impl BehaviourConfig {
    /// Override the gossipsub message-id function (typically from `services/p2p`).
    #[must_use]
    pub fn with_message_id_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&Message) -> MessageId + Send + Sync + 'static,
    {
        self.message_id_fn = Arc::new(f);
        self
    }
}

/// Errors from [`CcBehaviour::new`].
#[derive(Debug)]
pub enum BehaviourBuildError {
    GossipsubConfig(String),
    GossipsubBehaviour(String),
}

impl std::fmt::Display for BehaviourBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GossipsubConfig(e) => write!(f, "gossipsub config: {e}"),
            Self::GossipsubBehaviour(e) => write!(f, "gossipsub behaviour: {e}"),
        }
    }
}

impl std::error::Error for BehaviourBuildError {}

impl CcBehaviour {
    /// Build the composite behaviour with Architecture defaults.
    ///
    /// Gossipsub uses:
    /// - [`SnappyTransform`] as the `DataTransform` type parameter
    /// - [`AllowAllSubscriptionFilter`]
    /// - `ValidationMode::Anonymous` + `MessageAuthenticity::Anonymous` (Ethereum
    ///   does not libp2p-sign gossip; application validation is manual via
    ///   `validate_messages()` — Architecture §5.3 "Strict" means application
    ///   strictness, not libp2p's signed `ValidationMode::Strict`)
    /// - `max_transmit_size`, `heartbeat_interval`, `duplicate_cache_time`,
    ///   `validate_messages`, **`message_id_fn`** (SEC C1 / CC-22b — eth2
    ///   Altair+ preimage; never the libp2p default)
    /// - resolved method names at pin — see `docs/p2p-dependencies.md` §14
    pub fn new(keypair: &Keypair, cfg: BehaviourConfig) -> Result<Self, BehaviourBuildError> {
        let message_id_fn = cfg.message_id_fn;
        let gossipsub_config = GossipsubConfigBuilder::default()
            .validation_mode(ValidationMode::Anonymous)
            .validate_messages()
            .max_transmit_size(cfg.max_transmit_size)
            .heartbeat_interval(cfg.heartbeat_interval)
            .duplicate_cache_time(cfg.duplicate_cache_time)
            .message_id_fn(move |message: &Message| message_id_fn(message))
            .build()
            .map_err(|e| BehaviourBuildError::GossipsubConfig(e.to_string()))?;

        let snappy = SnappyTransform::new(cfg.max_uncompressed);
        let gossipsub = gossipsub::Behaviour::new_with_subscription_filter_and_transform(
            MessageAuthenticity::Anonymous,
            gossipsub_config,
            AllowAllSubscriptionFilter {},
            snappy,
        )
        .map_err(|e| BehaviourBuildError::GossipsubBehaviour(e.to_string()))?;

        let reqresp = request_response::Behaviour::with_codec(
            SszSnappyCodec,
            cfg.reqresp_protocols,
            request_response::Config::default(),
        );

        let identify = identify::Behaviour::new(
            identify::Config::new(cfg.identify_protocol, keypair.public())
                .with_agent_version(cfg.identify_agent),
        );

        let ping = ping::Behaviour::new(ping::Config::new());
        let limits = connection_limits::Behaviour::new(cfg.connection_limits);
        let allow_block = allow_block_list::Behaviour::<BlockedPeers>::default();

        Ok(Self {
            gossipsub,
            reqresp,
            identify,
            ping,
            limits,
            allow_block,
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use libp2p::gossipsub::TopicHash;

    fn sample_message(topic: &str, data: &[u8]) -> Message {
        Message {
            source: None,
            data: data.to_vec(),
            sequence_number: Some(1),
            topic: TopicHash::from_raw(topic),
        }
    }

    #[test]
    fn default_eth2_message_id_is_20_bytes_and_topic_aware() {
        let a = default_eth2_message_id(&sample_message(
            "/eth2/aabbccdd/beacon_block/ssz_snappy",
            b"payload",
        ));
        let b = default_eth2_message_id(&sample_message(
            "/eth2/aabbccdd/voluntary_exit/ssz_snappy",
            b"payload",
        ));
        let c = default_eth2_message_id(&sample_message(
            "/eth2/aabbccdd/beacon_block/ssz_snappy",
            b"other",
        ));
        assert_eq!(a.0.len(), 20);
        assert_ne!(a.0, b.0, "different topics must not collide");
        assert_ne!(a.0, c.0, "different payloads must not collide");
    }

    #[test]
    fn behaviour_config_always_has_message_id_fn() {
        let cfg = BehaviourConfig::default();
        let msg = sample_message("/eth2/00000000/beacon_block/ssz_snappy", b"x");
        let id = (cfg.message_id_fn)(&msg);
        assert_eq!(id.0.len(), 20);
        assert_eq!(id.0, default_eth2_message_id(&msg).0);
    }

    #[test]
    fn with_message_id_fn_overrides_default() {
        let cfg = BehaviourConfig::default().with_message_id_fn(|m: &Message| {
            MessageId::new(format!("custom-{}", m.data.len()).as_bytes())
        });
        let msg = sample_message("t", b"hello");
        let id = (cfg.message_id_fn)(&msg);
        assert_eq!(id.0, b"custom-5");
    }

    #[test]
    fn cc_behaviour_new_accepts_default_config() {
        let keypair = Keypair::generate_secp256k1();
        let _ = CcBehaviour::new(&keypair, BehaviourConfig::default()).expect("build");
    }
}
