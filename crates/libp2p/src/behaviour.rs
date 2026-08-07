//! [`CcBehaviour`] composition (Architecture §3.2).
//!
//! One `request_response::Behaviour` for all nine Ethereum protocols (codec
//! body is CC-23a — type stub only here). libp2p `ping` is kept alongside
//! Ethereum `/eth2/…/ping/1/` (different jobs); see `docs/p2p-dependencies.md`
//! §Deviations.

use std::io;
use std::time::Duration;

use futures::AsyncRead as FuturesAsyncRead;
use futures::AsyncWrite as FuturesAsyncWrite;
use libp2p::allow_block_list::BlockedPeers;
use libp2p::connection_limits::{self, ConnectionLimits};
use libp2p::gossipsub::{
    self, AllowAllSubscriptionFilter, ConfigBuilder as GossipsubConfigBuilder, MessageAuthenticity,
    ValidationMode,
};
use libp2p::identity::Keypair;
use libp2p::request_response::{self, Codec, ProtocolSupport};
use libp2p::swarm::{NetworkBehaviour, StreamProtocol};
use libp2p::{allow_block_list, identify, ping};

use crate::limits::{IDENTIFY_AGENT, IDENTIFY_PROTOCOL, default_connection_limits};
use crate::snappy::{GOSSIP_MAX_SIZE, SnappyTransform};

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
#[derive(Debug, Clone)]
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
        }
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
    ///   `validate_messages` (resolved method names at pin — see
    ///   `docs/p2p-dependencies.md` §14)
    pub fn new(keypair: &Keypair, cfg: BehaviourConfig) -> Result<Self, BehaviourBuildError> {
        let gossipsub_config = GossipsubConfigBuilder::default()
            .validation_mode(ValidationMode::Anonymous)
            .validate_messages()
            .max_transmit_size(cfg.max_transmit_size)
            .heartbeat_interval(cfg.heartbeat_interval)
            .duplicate_cache_time(cfg.duplicate_cache_time)
            // message_id_fn is topic-aware and lands with CC-22b; default id is fine here.
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
