//! Dial + Status handshake + outbound req/resp over `cc-libp2p` transport.
//!
//! # Dial trust boundary (SEC-4B-4)
//!
//! This module dials the multiaddr it is given. The multiaddr is a **trusted
//! operator input**: there is no private-IP deny list or SSRF guard beyond
//! requiring a TCP-over-IP/DNS multiaddr shape. Do not feed untrusted peer
//! tables into [`ProbeClient::dial`] / CLI `--peer`.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use cc_libp2p::reexport::{
    Multiaddr, PeerId, ProtocolSupport, StreamProtocol, Swarm, SwarmEvent, identify,
    multiaddr::Protocol as MultiaddrProtocol, request_response,
};
use cc_libp2p::{
    BehaviourConfig, CcBehaviour, CcBehaviourEvent, Keypair, ReqRespRequest, ReqRespResponse,
    SwarmConfig, build_swarm,
};
use cc_types::ForkDigest;
use futures::StreamExt;
use tokio::time::timeout;

use crate::codec::{ResponseChunk, decode_response_chunks};
use crate::protocols::{Protocol, StatusV2};

/// Default overall dial / handshake / request timeout budget.
const DEFAULT_OP_TIMEOUT: Duration = Duration::from_secs(30);

/// Errors from the probe client.
#[derive(Debug)]
pub enum ProbeClientError {
    /// Multiaddr parse failure.
    InvalidMultiaddr(String),
    /// Transport / dial failure with a named reason.
    Transport(String),
    /// Status handshake failure.
    Status(String),
    /// Outbound request failed.
    Request(String),
    /// Response framing / SSZ decode failure.
    Codec(String),
    /// Timed out waiting for an event.
    Timeout(String),
    /// Swarm / behaviour construction failure.
    Build(String),
}

impl fmt::Display for ProbeClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMultiaddr(m) => write!(f, "invalid multiaddr: {m}"),
            Self::Transport(m) => write!(f, "transport error: {m}"),
            Self::Status(m) => write!(f, "status handshake error: {m}"),
            Self::Request(m) => write!(f, "request error: {m}"),
            Self::Codec(m) => write!(f, "codec error: {m}"),
            Self::Timeout(m) => write!(f, "timeout: {m}"),
            Self::Build(m) => write!(f, "swarm build error: {m}"),
        }
    }
}

impl std::error::Error for ProbeClientError {}

/// Peer info collected during dial / identify / status.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Remote peer id.
    pub peer_id: PeerId,
    /// Identify agent version (empty if identify not observed).
    pub agent_version: String,
    /// Peer's Status v2.
    pub status: StatusV2,
}

/// Connected probe client holding a live swarm session with one peer.
pub struct ProbeClient {
    swarm: Swarm<CcBehaviour>,
    peer_id: PeerId,
    agent_version: String,
    status: StatusV2,
    /// Pending outbound request id → protocol.
    pending: HashMap<request_response::OutboundRequestId, Protocol>,
    /// Completed framed responses.
    completed: HashMap<request_response::OutboundRequestId, Result<Vec<u8>, String>>,
    /// Local status used to answer inbound Status requests.
    local_status: StatusV2,
    /// Multi-protocol data plane protocol this client may outbound.
    ///
    /// `cc-libp2p`'s multi `request_response` proposes **all** Full protocols
    /// on every outbound stream; multistream then settles on the first mutual
    /// ID. A probe client therefore registers **exactly one** Full data
    /// protocol (plus Status on its dedicated behaviour) so ByRange blocks and
    /// ByRange columns do not collide.
    data_protocol: Protocol,
}

impl fmt::Debug for ProbeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbeClient")
            .field("peer_id", &self.peer_id)
            .field("agent_version", &self.agent_version)
            .field("status", &self.status)
            .field("data_protocol", &self.data_protocol.protocol_id())
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl ProbeClient {
    /// Dial `multiaddr` with a blocks-by-range multi protocol (default path).
    pub async fn dial(multiaddr: &str, fork_digest: ForkDigest) -> Result<Self, ProbeClientError> {
        Self::dial_for(multiaddr, fork_digest, Protocol::BeaconBlocksByRangeV2).await
    }

    /// Dial for column-by-range outbound (separate multi-protocol selection).
    pub async fn dial_columns(
        multiaddr: &str,
        fork_digest: ForkDigest,
    ) -> Result<Self, ProbeClientError> {
        Self::dial_for(
            multiaddr,
            fork_digest,
            Protocol::DataColumnSidecarsByRangeV1,
        )
        .await
    }

    /// Dial for blocks-by-root outbound.
    pub async fn dial_blocks_by_root(
        multiaddr: &str,
        fork_digest: ForkDigest,
    ) -> Result<Self, ProbeClientError> {
        Self::dial_for(multiaddr, fork_digest, Protocol::BeaconBlocksByRootV2).await
    }

    /// Dial for columns-by-root outbound.
    pub async fn dial_columns_by_root(
        multiaddr: &str,
        fork_digest: ForkDigest,
    ) -> Result<Self, ProbeClientError> {
        Self::dial_for(multiaddr, fork_digest, Protocol::DataColumnSidecarsByRootV1).await
    }

    /// Dial `multiaddr`, complete Status v2 handshake, return a live client.
    ///
    /// `data_protocol` is the sole Full multi-protocol registered for outbound
    /// block/column traffic (see [`ProbeClient::data_protocol`]).
    pub async fn dial_for(
        multiaddr: &str,
        fork_digest: ForkDigest,
        data_protocol: Protocol,
    ) -> Result<Self, ProbeClientError> {
        if matches!(data_protocol, Protocol::StatusV2) {
            return Err(ProbeClientError::Build(
                "data_protocol must be a block or column protocol".to_owned(),
            ));
        }

        let addr: Multiaddr = multiaddr
            .parse()
            .map_err(|e| ProbeClientError::InvalidMultiaddr(format!("{e}")))?;
        // SEC-4B-4: refuse non-IP/DNS multiaddrs (cheap shape check). Full SSRF
        // guard (private ranges) is intentionally out of scope for this CLI.
        validate_dial_multiaddr(&addr)?;

        let keypair = Keypair::generate_secp256k1();
        // Status → dedicated single-protocol behaviour (correct multistream).
        // Exactly one Full data protocol on the multi behaviour.
        // Remaining probe protocols as Inbound so the peer can still negotiate
        // them if it opens streams at us, without polluting our outbound set.
        let mut protocols: Vec<(StreamProtocol, ProtocolSupport)> = vec![(
            StreamProtocol::new(Protocol::StatusV2.protocol_id()),
            ProtocolSupport::Full,
        )];
        for p in Protocol::ALL {
            if matches!(p, Protocol::StatusV2) {
                continue;
            }
            let support = if p == data_protocol {
                ProtocolSupport::Full
            } else {
                ProtocolSupport::Inbound
            };
            protocols.push((StreamProtocol::new(p.protocol_id()), support));
        }
        let cfg = BehaviourConfig::default()
            .with_reqresp_protocols(protocols)
            .with_reqresp_request_timeout(Duration::from_secs(15));
        let behaviour =
            CcBehaviour::new(&keypair, cfg).map_err(|e| ProbeClientError::Build(e.to_string()))?;
        let mut swarm = build_swarm(keypair, behaviour, &SwarmConfig::default())
            .map_err(|e| ProbeClientError::Build(e.to_string()))?;

        swarm
            .dial(addr.clone())
            .map_err(|e| ProbeClientError::Transport(format!("dial {addr}: {e}")))?;

        let local_status = StatusV2::for_probe(fork_digest);
        let mut agent_version = String::new();
        let mut peer_id: Option<PeerId> = None;
        let mut remote_status: Option<StatusV2> = None;
        let mut status_req_id: Option<request_response::OutboundRequestId> = None;

        let dial_result = timeout(DEFAULT_OP_TIMEOUT, async {
            loop {
                let ev = swarm.select_next_some().await;
                match ev {
                    SwarmEvent::ConnectionEstablished { peer_id: pid, .. } => {
                        peer_id = Some(pid);
                        // Outbound Status on the dedicated behaviour.
                        let req = ReqRespRequest {
                            protocol: StreamProtocol::new(Protocol::StatusV2.protocol_id()),
                            ssz: local_status.to_ssz_bytes().to_vec(),
                        };
                        let id = swarm.behaviour_mut().send_reqresp(&pid, req);
                        status_req_id = Some(id);
                    }
                    SwarmEvent::OutgoingConnectionError { error, .. } => {
                        return Err(ProbeClientError::Transport(format!(
                            "unreachable multiaddr {addr}: {error}"
                        )));
                    }
                    SwarmEvent::Behaviour(CcBehaviourEvent::Identify(
                        identify::Event::Received { info, .. },
                    )) => {
                        agent_version = info.agent_version;
                    }
                    SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespStatus(ev)) => {
                        handle_status_event(
                            &mut swarm,
                            ev,
                            &local_status,
                            status_req_id,
                            &mut remote_status,
                        )?;
                    }
                    // Block/column behaviours unused during handshake.
                    SwarmEvent::Behaviour(_) => {}
                    _ => {}
                }
                if peer_id.is_some() && remote_status.is_some() {
                    break;
                }
            }
            Ok(())
        })
        .await;

        match dial_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(ProbeClientError::Timeout(format!(
                    "dial/status handshake to {addr} exceeded {}s",
                    DEFAULT_OP_TIMEOUT.as_secs()
                )));
            }
        }

        let peer_id = peer_id.ok_or_else(|| {
            ProbeClientError::Transport(format!("no ConnectionEstablished for {addr}"))
        })?;
        let status = remote_status.ok_or_else(|| {
            ProbeClientError::Status("Status v2 response not received".to_owned())
        })?;

        Ok(Self {
            swarm,
            peer_id,
            agent_version,
            status,
            pending: HashMap::new(),
            completed: HashMap::new(),
            local_status,
            data_protocol,
        })
    }

    /// Peer info after handshake.
    #[must_use]
    pub fn peer_info(&self) -> PeerInfo {
        PeerInfo {
            peer_id: self.peer_id,
            agent_version: self.agent_version.clone(),
            status: self.status,
        }
    }

    /// The data-plane protocol this client can outbound.
    #[must_use]
    pub const fn data_protocol(&self) -> Protocol {
        self.data_protocol
    }

    /// Send an uncompressed-SSZ request and return the framed response body.
    pub async fn request(
        &mut self,
        protocol: Protocol,
        ssz: Vec<u8>,
    ) -> Result<Vec<u8>, ProbeClientError> {
        if !matches!(protocol, Protocol::StatusV2) && protocol != self.data_protocol {
            return Err(ProbeClientError::Request(format!(
                "client dialed for {} cannot outbound {}",
                self.data_protocol.protocol_id(),
                protocol.protocol_id()
            )));
        }
        let req = ReqRespRequest {
            protocol: StreamProtocol::new(protocol.protocol_id()),
            ssz,
        };
        let id = self.swarm.behaviour_mut().send_reqresp(&self.peer_id, req);
        self.pending.insert(id, protocol);

        let result = timeout(DEFAULT_OP_TIMEOUT, async {
            loop {
                if let Some(res) = self.completed.remove(&id) {
                    return res.map_err(ProbeClientError::Request);
                }
                let ev = self.swarm.select_next_some().await;
                self.drive_event(ev)?;
            }
        })
        .await;

        match result {
            Ok(r) => r,
            Err(_) => Err(ProbeClientError::Timeout(format!(
                "{} request to {} timed out",
                protocol.protocol_id(),
                self.peer_id
            ))),
        }
    }

    /// Request and decode response chunks for `protocol`.
    pub async fn request_chunks(
        &mut self,
        protocol: Protocol,
        ssz: Vec<u8>,
    ) -> Result<Vec<ResponseChunk>, ProbeClientError> {
        let framed = self.request(protocol, ssz).await?;
        decode_response_chunks(
            &framed,
            protocol.has_context_bytes(),
            protocol.response_limits(),
        )
        .map_err(|e| ProbeClientError::Codec(e.to_string()))
    }

    /// Timed request returning `(chunks, elapsed)`.
    pub async fn request_chunks_timed(
        &mut self,
        protocol: Protocol,
        ssz: Vec<u8>,
    ) -> Result<(Vec<ResponseChunk>, Duration), ProbeClientError> {
        let start = Instant::now();
        let chunks = self.request_chunks(protocol, ssz).await?;
        Ok((chunks, start.elapsed()))
    }

    fn drive_event(&mut self, ev: SwarmEvent<CcBehaviourEvent>) -> Result<(), ProbeClientError> {
        match ev {
            SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespStatus(ev)) => {
                handle_status_event_drive(
                    &mut self.swarm,
                    ev,
                    &self.local_status,
                    &mut self.pending,
                    &mut self.completed,
                );
            }
            SwarmEvent::Behaviour(CcBehaviourEvent::Reqresp(ev)) => {
                handle_data_event(&mut self.swarm, ev, &mut self.pending, &mut self.completed);
            }
            SwarmEvent::Behaviour(CcBehaviourEvent::Identify(identify::Event::Received {
                info,
                ..
            })) => {
                if self.agent_version.is_empty() {
                    self.agent_version = info.agent_version;
                }
            }
            SwarmEvent::ConnectionClosed {
                cause: Some(cause), ..
            } => {
                return Err(ProbeClientError::Transport(format!(
                    "connection closed: {cause}"
                )));
            }
            _ => {}
        }
        Ok(())
    }
}

fn handle_status_event(
    swarm: &mut Swarm<CcBehaviour>,
    ev: request_response::Event<ReqRespRequest, ReqRespResponse>,
    local_status: &StatusV2,
    status_req_id: Option<request_response::OutboundRequestId>,
    remote_status: &mut Option<StatusV2>,
) -> Result<(), ProbeClientError> {
    use request_response::{Event as RREvent, Message as RRMessage};

    match ev {
        RREvent::Message { message, .. } => match message {
            RRMessage::Request {
                request, channel, ..
            } => {
                // Answer inbound Status with our local probe status.
                let _ = request; // SSZ already decoded by cc-libp2p codec
                let framed = encode_status_response_framed(local_status)
                    .map_err(|e| ProbeClientError::Codec(e.to_string()))?;
                let _ = swarm
                    .behaviour_mut()
                    .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
            }
            RRMessage::Response {
                request_id,
                response,
            } => {
                if status_req_id == Some(request_id) {
                    let chunks = decode_response_chunks(
                        &response.framed,
                        Protocol::StatusV2.has_context_bytes(),
                        Protocol::StatusV2.response_limits(),
                    )
                    .map_err(|e| ProbeClientError::Codec(e.to_string()))?;
                    let Some(ResponseChunk::Success { ssz, .. }) = chunks.into_iter().next() else {
                        return Err(ProbeClientError::Status(
                            "status response missing success chunk".to_owned(),
                        ));
                    };
                    let status = StatusV2::from_ssz_bytes(&ssz)
                        .map_err(|e| ProbeClientError::Status(e.to_string()))?;
                    *remote_status = Some(status);
                }
            }
        },
        RREvent::OutboundFailure { error, .. } => {
            return Err(ProbeClientError::Status(format!(
                "status outbound failure: {error}"
            )));
        }
        RREvent::InboundFailure { .. } | RREvent::ResponseSent { .. } => {}
    }
    Ok(())
}

fn handle_status_event_drive(
    swarm: &mut Swarm<CcBehaviour>,
    ev: request_response::Event<ReqRespRequest, ReqRespResponse>,
    local_status: &StatusV2,
    pending: &mut HashMap<request_response::OutboundRequestId, Protocol>,
    completed: &mut HashMap<request_response::OutboundRequestId, Result<Vec<u8>, String>>,
) {
    use request_response::{Event as RREvent, Message as RRMessage};

    match ev {
        RREvent::Message { message, .. } => match message {
            RRMessage::Request { channel, .. } => {
                if let Ok(framed) = encode_status_response_framed(local_status) {
                    let _ = swarm
                        .behaviour_mut()
                        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
                }
            }
            RRMessage::Response {
                request_id,
                response,
            } => {
                pending.remove(&request_id);
                completed.insert(request_id, Ok(response.framed));
            }
        },
        RREvent::OutboundFailure {
            request_id, error, ..
        } => {
            pending.remove(&request_id);
            completed.insert(request_id, Err(format!("outbound failure: {error}")));
        }
        RREvent::InboundFailure { .. } | RREvent::ResponseSent { .. } => {}
    }
}

fn handle_data_event(
    swarm: &mut Swarm<CcBehaviour>,
    ev: request_response::Event<ReqRespRequest, ReqRespResponse>,
    pending: &mut HashMap<request_response::OutboundRequestId, Protocol>,
    completed: &mut HashMap<request_response::OutboundRequestId, Result<Vec<u8>, String>>,
) {
    use request_response::{Event as RREvent, Message as RRMessage};

    match ev {
        RREvent::Message { message, .. } => match message {
            RRMessage::Request { channel, .. } => {
                // Probe is client-only for block/column; refuse inbound with code 3.
                if let Ok(framed) = crate::codec::encode_error_chunk(3, b"probe does not serve") {
                    let _ = swarm
                        .behaviour_mut()
                        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
                }
            }
            RRMessage::Response {
                request_id,
                response,
            } => {
                pending.remove(&request_id);
                completed.insert(request_id, Ok(response.framed));
            }
        },
        RREvent::OutboundFailure {
            request_id, error, ..
        } => {
            pending.remove(&request_id);
            completed.insert(request_id, Err(format!("outbound failure: {error}")));
        }
        RREvent::InboundFailure { .. } | RREvent::ResponseSent { .. } => {}
    }
}

/// Require TCP over IP4/IP6/DNS so we do not dial exotic multiaddrs (SEC-4B-4).
fn validate_dial_multiaddr(addr: &Multiaddr) -> Result<(), ProbeClientError> {
    let mut has_net = false;
    let mut has_tcp = false;
    for p in addr.iter() {
        match p {
            MultiaddrProtocol::Ip4(_)
            | MultiaddrProtocol::Ip6(_)
            | MultiaddrProtocol::Dns(_)
            | MultiaddrProtocol::Dns4(_)
            | MultiaddrProtocol::Dns6(_)
            | MultiaddrProtocol::Dnsaddr(_) => has_net = true,
            MultiaddrProtocol::Tcp(_) => has_tcp = true,
            // Explicitly reject known non-TCP transports we never want.
            MultiaddrProtocol::Unix(_)
            | MultiaddrProtocol::QuicV1
            | MultiaddrProtocol::WebRTCDirect
            | MultiaddrProtocol::Ws(_)
            | MultiaddrProtocol::Wss(_) => {
                return Err(ProbeClientError::InvalidMultiaddr(format!(
                    "unsupported multiaddr protocol component (need /ip4|ip6|dns…/tcp/…): {addr}"
                )));
            }
            _ => {}
        }
    }
    if has_net && has_tcp {
        Ok(())
    } else {
        Err(ProbeClientError::InvalidMultiaddr(format!(
            "multiaddr must include IP/DNS and TCP components (SEC-4B-4): {addr}"
        )))
    }
}

/// Encode Status as a framed success response (no context bytes) using **our** codec.
fn encode_status_response_framed(status: &StatusV2) -> std::io::Result<Vec<u8>> {
    let ssz = status.to_ssz_bytes();
    crate::codec::encode_success_chunk(&ssz, false, None, Protocol::StatusV2.response_limits())
}

/// Encode a ResourceUnavailable framed body using our codec.
#[must_use]
pub fn encode_resource_unavailable_framed() -> Vec<u8> {
    crate::codec::encode_error_chunk(3, b"resource unavailable").unwrap_or_else(|_| vec![3])
}

/// Encode an empty success stream (zero chunks) — the negative-side failure shape.
#[must_use]
pub fn encode_empty_success_framed() -> Vec<u8> {
    Vec::new()
}
