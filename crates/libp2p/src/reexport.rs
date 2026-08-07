//! Narrow re-export set for downstream crates (Architecture §1.1 / §3.1).
//!
//! Downstream services take libp2p surfaces **only** through this module (or the
//! crate-root construction API). They never declare a `libp2p*` Cargo dependency.

pub use libp2p::futures;
pub use libp2p::identity::{self, Keypair, PeerId};
pub use libp2p::multiaddr::{self, Multiaddr, Protocol};
pub use libp2p::swarm::dial_opts::DialOpts;
pub use libp2p::swarm::{self, DialError, NetworkBehaviour, StreamProtocol, Swarm, SwarmEvent};
pub use libp2p::{
    allow_block_list, connection_limits, gossipsub, identify, noise, ping, quic, request_response,
    tcp, yamux,
};

pub use libp2p::allow_block_list::BlockedPeers;
pub use libp2p::connection_limits::ConnectionLimits;

// Hot gossipsub types used at every publish/validate / message-id call site.
pub use libp2p::gossipsub::{
    IdentTopic, Message, MessageAcceptance, MessageId, TopicHash,
};

// request_response surfaces for the nine Ethereum protocols (codec body: CC-23a).
pub use libp2p::request_response::{
    Codec as RequestResponseCodec, Event as RequestResponseEvent,
    InboundFailure, Message as RequestResponseMessage, OutboundFailure, OutboundRequestId,
    ProtocolSupport, ResponseChannel,
};
