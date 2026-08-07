//! §2.2 channel map — bounds land verbatim.
//!
//! Fan-out edges from the swarm task and the publish/cmd path into it. Each
//! bound is a design value. Depth is reported on `cc_p2p_queue_depth{q=…}`;
//! chain-stream edges report depth via `outstanding` and
//! `cc_p2p_chain_stream_saturation_ratio` (CC-27b).

use cc_libp2p::{Multiaddr, PeerId};
use cc_proto::p2p::{GossipObject, Verdict};
use tokio::sync::{mpsc, oneshot};

use crate::metrics::{P2pMetrics, QueueName};

/// Resolution of a chain-bound object (verdict or local timeout IGNORE).
#[derive(Debug, Clone)]
pub enum VerdictResolution {
    /// Chain answered with a `Verdict`.
    FromChain(Verdict),
    /// Timed out locally — resolved as IGNORE so gossipsub is released.
    Timeout,
}

// ── §2.2 bounds (verbatim) ──────────────────────────────────────────────────

/// swarm → gossip validation.
pub const GOSSIP_BOUND: usize = 1024;
/// swarm → req/resp server.
pub const REQRESP_IN_BOUND: usize = 256;
/// swarm → peer manager (connection events).
pub const CONN_BOUND: usize = 256;
/// gossip validation → KZG pool.
pub const KZG_BOUND: usize = 256;
/// any → chain-stream outbound.
pub const CHAIN_OUT_BOUND: usize = 1024;
/// chain-stream inbound → dispatch.
pub const CHAIN_IN_BOUND: usize = 1024;
/// publish queue → swarm.
pub const PUBLISH_BOUND: usize = 256;
/// peer manager → swarm (`cmd_tx`).
pub const CMD_BOUND: usize = 512;

/// Placeholder gossip-validation work item (claimed by CC-22*).
#[derive(Debug, Clone)]
pub struct GossipWork {
    /// Opaque payload placeholder.
    pub bytes: Vec<u8>,
}

/// Placeholder inbound req/resp request (claimed by CC-23*).
#[derive(Debug, Clone)]
pub struct ReqRespInbound {
    /// Opaque payload placeholder.
    pub bytes: Vec<u8>,
}

/// Connection direction relative to the local node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionDirection {
    /// Remote dialed us.
    Inbound,
    /// We dialed the remote.
    Outbound,
}

/// Ethereum `Goodbye` reason codes (p2p-interface).
///
/// Wire framing is CC-23b; the peer manager emits the command with a reason
/// before every intentional disconnect (CC-20c).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u64)]
pub enum GoodbyeReason {
    /// Client shut down.
    ClientShutdown = 1,
    /// Irrelevant network.
    IrrelevantNetwork = 2,
    /// Fault / error.
    FaultOrError = 3,
    /// Unable to verify network.
    UnableToVerifyNetwork = 4,
    /// Too many peers.
    TooManyPeers = 5,
    /// Duplicate peer.
    DuplicatePeer = 6,
}

impl GoodbyeReason {
    /// Numeric reason code on the wire.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self as u64
    }
}

/// Connection events swarm → peer manager (CC-20c).
#[derive(Debug, Clone)]
pub enum ConnEvent {
    /// A new listen address was reported.
    NewListenAddr {
        /// Listen multiaddr.
        address: Multiaddr,
    },
    /// Connection fully established.
    ConnectionEstablished {
        /// Remote peer.
        peer_id: PeerId,
        /// Inbound vs outbound.
        direction: ConnectionDirection,
        /// Remote endpoint multiaddr.
        endpoint: Multiaddr,
    },
    /// Connection closed.
    ConnectionClosed {
        /// Remote peer.
        peer_id: PeerId,
    },
    /// Outbound dial failed (before or after partial progress).
    DialFailure {
        /// Target peer if known.
        peer_id: Option<PeerId>,
        /// Error display string (no libp2p type leak into consumers).
        error: String,
    },
}

/// Placeholder KZG verify job (claimed by CC-22d / DA path).
#[derive(Debug, Clone)]
pub struct KzgJob {
    /// Opaque payload placeholder.
    pub bytes: Vec<u8>,
}

/// Object destined for the chain stream (p2p → chain).
///
/// Produced by gossip validation (CC-22*) or synthetic tests. The stream
/// client assigns `seq`, tracks `outstanding`, and fills [`Self::reply`] when
/// a verdict (or local timeout IGNORE) resolves.
#[derive(Debug)]
pub struct ChainOutbound {
    /// Consensus object (GossipObject reuses ImportBlockRequest shape).
    pub object: GossipObject,
    /// Optional completion for the producer (gossip hold / tests).
    pub reply: Option<oneshot::Sender<VerdictResolution>>,
}

/// Verdict dispatched from the stream client → gossip validation / scoring.
#[derive(Debug, Clone)]
pub struct ChainInbound {
    /// Chain (or local-timeout) verdict.
    pub verdict: Verdict,
    /// Wire latency when known (zero for synthetic local IGNORE).
    pub latency: std::time::Duration,
}

/// Local publish request → swarm (claimed by CC-27b / gossip publish path).
#[derive(Debug, Clone)]
pub struct PublishRequest {
    /// Topic string placeholder.
    pub topic: String,
    /// Payload placeholder.
    pub data: Vec<u8>,
}

/// Commands the swarm task accepts — **only** path that mutates `Swarm`.
///
/// Dial / disconnect / ban policy is decided by the peer manager (CC-20c);
/// discovery content lands in CC-21c. Goodbye wire body is CC-23b — until then
/// the swarm drops `Goodbye` with a counter after asserting the command path.
#[derive(Debug, Clone)]
pub enum SwarmCommand {
    /// No-op (used by tests / keep-alive).
    Noop,
    /// Local publish (routed from the publish queue).
    Publish(PublishRequest),
    /// Subscribe to a gossip topic string (topic registry lands later).
    Subscribe {
        /// Full topic string.
        topic: String,
    },
    /// Dial a peer at `addr` (peer manager / static peers).
    Dial {
        /// Target peer.
        peer_id: PeerId,
        /// Multiaddr to dial.
        addr: Multiaddr,
    },
    /// Send Ethereum `Goodbye` (standalone; prefer [`ClosePeer`] for intentional closes).
    ///
    /// Wire handler is CC-23b; swarm currently records and continues.
    Goodbye {
        /// Peer to notify.
        peer_id: PeerId,
        /// Reason code.
        reason: GoodbyeReason,
    },
    /// Disconnect a peer (standalone; prefer [`ClosePeer`] for intentional closes).
    Disconnect {
        /// Peer to disconnect.
        peer_id: PeerId,
    },
    /// **Atomic policy close** (H2): Goodbye → disconnect → optional ban, one cmd slot.
    ///
    /// Peer manager uses this for every intentional close so a full queue cannot
    /// accept Goodbye and drop Disconnect/BlockPeer independently.
    ClosePeer {
        /// Peer to close.
        peer_id: PeerId,
        /// Goodbye reason code.
        reason: GoodbyeReason,
        /// When true, also `allow_block_list.block_peer` (ban enforcement).
        ban: bool,
    },
    /// Add peer to `allow_block_list` (ban enforcement) without disconnecting.
    BlockPeer {
        /// Peer to block.
        peer_id: PeerId,
    },
    /// Remove peer from `allow_block_list`.
    UnblockPeer {
        /// Peer to unblock.
        peer_id: PeerId,
    },
}

/// All §2.2 edges: senders retained by producers, receivers by consumers.
#[derive(Debug)]
pub struct ChannelMap {
    /// swarm → gossip validation.
    pub gossip_tx: mpsc::Sender<GossipWork>,
    /// gossip validation receiver (stub consumer / CC-22*).
    pub gossip_rx: mpsc::Receiver<GossipWork>,
    /// swarm → req/resp server.
    pub reqresp_in_tx: mpsc::Sender<ReqRespInbound>,
    /// req/resp inbound receiver.
    pub reqresp_in_rx: mpsc::Receiver<ReqRespInbound>,
    /// swarm → peer manager.
    pub conn_tx: mpsc::Sender<ConnEvent>,
    /// connection-event receiver.
    pub conn_rx: mpsc::Receiver<ConnEvent>,
    /// gossip validation → KZG pool.
    pub kzg_tx: mpsc::Sender<KzgJob>,
    /// KZG pool receiver.
    pub kzg_rx: mpsc::Receiver<KzgJob>,
    /// any → chain-stream outbound.
    pub chain_out_tx: mpsc::Sender<ChainOutbound>,
    /// chain-stream outbound receiver.
    pub chain_out_rx: mpsc::Receiver<ChainOutbound>,
    /// chain-stream inbound → dispatch.
    pub chain_in_tx: mpsc::Sender<ChainInbound>,
    /// chain-stream inbound receiver.
    pub chain_in_rx: mpsc::Receiver<ChainInbound>,
    /// publish queue → swarm (feeds cmd path; separate bound for depth metric).
    pub publish_tx: mpsc::Sender<PublishRequest>,
    /// publish queue receiver (bridged into cmd by a stub).
    pub publish_rx: mpsc::Receiver<PublishRequest>,
    /// peer manager → swarm.
    pub cmd_tx: mpsc::Sender<SwarmCommand>,
    /// swarm command receiver (owned by the swarm task).
    pub cmd_rx: mpsc::Receiver<SwarmCommand>,
}

impl ChannelMap {
    /// Allocate every §2.2 channel at its design bound and seed depth gauges to 0.
    #[must_use]
    pub fn new(metrics: &P2pMetrics) -> Self {
        let (gossip_tx, gossip_rx) = mpsc::channel(GOSSIP_BOUND);
        let (reqresp_in_tx, reqresp_in_rx) = mpsc::channel(REQRESP_IN_BOUND);
        let (conn_tx, conn_rx) = mpsc::channel(CONN_BOUND);
        let (kzg_tx, kzg_rx) = mpsc::channel(KZG_BOUND);
        let (chain_out_tx, chain_out_rx) = mpsc::channel(CHAIN_OUT_BOUND);
        let (chain_in_tx, chain_in_rx) = mpsc::channel(CHAIN_IN_BOUND);
        let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_BOUND);
        let (cmd_tx, cmd_rx) = mpsc::channel(CMD_BOUND);

        // Seed the six labelled depths that map to this map (chain stream uses
        // saturation_ratio later; still create the channels here).
        for q in [
            QueueName::Gossip,
            QueueName::ReqrespIn,
            QueueName::Conn,
            QueueName::Kzg,
            QueueName::Publish,
            QueueName::Cmd,
        ] {
            metrics.set_queue_depth(q, 0);
        }

        Self {
            gossip_tx,
            gossip_rx,
            reqresp_in_tx,
            reqresp_in_rx,
            conn_tx,
            conn_rx,
            kzg_tx,
            kzg_rx,
            chain_out_tx,
            chain_out_rx,
            chain_in_tx,
            chain_in_rx,
            publish_tx,
            publish_rx,
            cmd_tx,
            cmd_rx,
        }
    }

    /// Design bounds as `(QueueName, bound)` for the six depth-labelled edges.
    #[must_use]
    pub const fn labelled_bounds() -> [(QueueName, usize); 6] {
        [
            (QueueName::Gossip, GOSSIP_BOUND),
            (QueueName::ReqrespIn, REQRESP_IN_BOUND),
            (QueueName::Conn, CONN_BOUND),
            (QueueName::Kzg, KZG_BOUND),
            (QueueName::Publish, PUBLISH_BOUND),
            (QueueName::Cmd, CMD_BOUND),
        ]
    }
}

/// Drain a receiver forever, counting and dropping (stub consumer for later issues).
pub async fn stub_consumer<T: Send + 'static>(
    name: &'static str,
    mut rx: mpsc::Receiver<T>,
    metrics: P2pMetrics,
    queue: Option<QueueName>,
) {
    let _ = name;
    while rx.recv().await.is_some() {
        if let Some(q) = queue {
            // Approximate: after a successful recv the depth is at most bound-1;
            // producers own precise depth updates. Keep gauge non-negative.
            let depth = metrics.queue_depth(q);
            if depth > 0 {
                metrics.set_queue_depth(q, depth - 1);
            }
        }
    }
}
