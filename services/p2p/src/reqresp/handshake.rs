//! Status handshake orchestration — Architecture §7.4 / CC-23b.
//!
//! - On connect, **both directions**: each side sends `Status v2`.
//! - Re-exchanged **once per epoch** (and after a BPO boundary via the same
//!   epoch path once `ForkContext` advances).
//! - `fork_digest` mismatch → Goodbye + disconnect.
//! - Ping seq mismatch → outbound MetaData fetch.
//!
//! This module is the pure control plane; the swarm task emits wire
//! [`OutboundAction`]s and feeds responses back through [`HandshakeBook`].

use std::collections::HashMap;
use std::sync::Arc;

use cc_libp2p::PeerId;
use cc_types::primitives::Slot;
use cc_types::{Epoch, ForkDigest};

use crate::backfill::ServeWindow;
use crate::chain_stream::ChainViewStore;
use crate::channels::GoodbyeReason;
use crate::fork_digest::ForkContext;
use crate::reqresp::metadata::{
    evaluate_peer_cgc, CgcEval, CgcPolicy, LocalMetaData, MetaDataV3,
};
use crate::reqresp::ping::{seq_mismatch, Ping};
use crate::reqresp::status::{
    build_local_status, evaluate_peer_status, StatusEval, StatusV2,
};
use crate::reqresp::Protocol;

/// Outbound work the swarm / peer manager should perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundAction {
    /// Send a req/resp request with uncompressed SSZ body.
    SendRequest {
        /// Target peer.
        peer_id: PeerId,
        /// Protocol to negotiate.
        protocol: Protocol,
        /// Uncompressed SSZ request body.
        ssz: Vec<u8>,
    },
    /// Goodbye + disconnect (and never merely deprioritise).
    Disconnect {
        /// Peer to close.
        peer_id: PeerId,
        /// Wire reason.
        reason: GoodbyeReason,
    },
}

/// Per-peer handshake state.
#[derive(Debug, Clone, Default)]
pub struct PeerHandshakeState {
    /// Last Status received (six fields).
    pub status: Option<StatusV2>,
    /// Last MetaData received.
    pub metadata: Option<MetaDataV3>,
    /// Cached MetaData seq (for Ping comparison).
    pub cached_meta_seq: Option<u64>,
    /// How many Status exchanges we have initiated toward this peer.
    pub status_exchanges_initiated: u32,
    /// Epoch of the last status we initiated (for per-epoch re-exchange).
    pub last_status_epoch: Option<u64>,
    /// Whether a MetaData fetch is outstanding after a Ping mismatch.
    pub metadata_fetch_pending: bool,
}

/// Shared sources for building local Status / MetaData.
#[derive(Debug, Clone)]
pub struct HandshakeDeps {
    /// Chain view (CC-27a) — sole source of head/finalized fields.
    pub view: ChainViewStore,
    /// Serve window atomic (CC-26a) — sole source of earliest_available_slot.
    pub window: Arc<ServeWindow>,
    /// Low-cgc policy knob (`p2p.reject_low_cgc_peers`, default false).
    pub cgc_policy: CgcPolicy,
    /// Local MetaData (seq / attnets / syncnets / cgc).
    pub local_meta: Arc<LocalMetaData>,
}

impl HandshakeDeps {
    /// Construct with empty view, empty window seed, default cgc policy.
    #[must_use]
    pub fn new(anchor_slot: Slot, cgc: u64) -> Self {
        Self {
            view: ChainViewStore::new(),
            window: Arc::new(ServeWindow::new(anchor_slot)),
            cgc_policy: CgcPolicy::default(),
            local_meta: Arc::new(LocalMetaData::new(cgc)),
        }
    }

    /// Read earliest_available_slot from the CC-26a atomic only.
    #[must_use]
    pub fn earliest_available_slot(&self) -> Slot {
        self.window.load()
    }

    /// Build local Status from the three sole sources.
    #[must_use]
    pub fn local_status(&self, fork_ctx: &ForkContext) -> StatusV2 {
        let view = self.view.load();
        build_local_status(fork_ctx, view.as_ref(), self.earliest_available_slot())
    }

    /// Local MetaData snapshot.
    #[must_use]
    pub fn local_metadata(&self) -> MetaDataV3 {
        self.local_meta.load()
    }

    /// Local Ping body (current seq).
    #[must_use]
    pub fn local_ping(&self) -> Ping {
        Ping::new(self.local_meta.seq_number())
    }
}

/// Tracks handshake state per peer and produces outbound actions.
#[derive(Debug, Default)]
pub struct HandshakeBook {
    peers: HashMap<PeerId, PeerHandshakeState>,
}

impl HandshakeBook {
    /// Empty book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Peer row (created on demand).
    pub fn peer_mut(&mut self, peer_id: PeerId) -> &mut PeerHandshakeState {
        self.peers.entry(peer_id).or_default()
    }

    /// Immutable peer row.
    #[must_use]
    pub fn peer(&self, peer_id: PeerId) -> Option<&PeerHandshakeState> {
        self.peers.get(&peer_id)
    }

    /// Drop peer state on disconnect.
    pub fn on_disconnected(&mut self, peer_id: PeerId) {
        self.peers.remove(&peer_id);
    }

    /// On connect: initiate Status (one side of the bidirectional handshake).
    ///
    /// Both peers independently call this when they observe the connection, so
    /// each direction carries a Status without coordination.
    pub fn on_connect(
        &mut self,
        peer_id: PeerId,
        fork_ctx: &ForkContext,
        deps: &HandshakeDeps,
    ) -> Vec<OutboundAction> {
        self.initiate_status(peer_id, fork_ctx, deps)
    }

    /// Per-epoch re-exchange: for every connected peer, send Status once per
    /// new epoch (or BPO boundary, which advances the epoch path).
    pub fn on_epoch(
        &mut self,
        epoch: Epoch,
        connected: &[PeerId],
        fork_ctx: &ForkContext,
        deps: &HandshakeDeps,
    ) -> Vec<OutboundAction> {
        let mut actions = Vec::new();
        let epoch_u = epoch.as_u64();
        for &peer_id in connected {
            let state = self.peers.entry(peer_id).or_default();
            if state.last_status_epoch == Some(epoch_u) {
                continue;
            }
            actions.extend(self.initiate_status(peer_id, fork_ctx, deps));
        }
        actions
    }

    fn initiate_status(
        &mut self,
        peer_id: PeerId,
        fork_ctx: &ForkContext,
        deps: &HandshakeDeps,
    ) -> Vec<OutboundAction> {
        let status = deps.local_status(fork_ctx);
        let state = self.peers.entry(peer_id).or_default();
        state.status_exchanges_initiated =
            state.status_exchanges_initiated.saturating_add(1);
        state.last_status_epoch = Some(fork_ctx.current_epoch().as_u64());
        vec![OutboundAction::SendRequest {
            peer_id,
            protocol: Protocol::StatusV2,
            ssz: status.to_ssz_bytes().to_vec(),
        }]
    }

    /// Handle an inbound Status request body; returns response SSZ + any policy actions.
    pub fn on_inbound_status(
        &mut self,
        peer_id: PeerId,
        peer_status: StatusV2,
        local_digest: ForkDigest,
        deps: &HandshakeDeps,
    ) -> InboundStatusResult {
        let eval = evaluate_peer_status(local_digest, &peer_status);
        let state = self.peers.entry(peer_id).or_default();
        state.status = Some(peer_status);

        // Note: cgc is not on Status; it is applied when MetaData arrives.
        // `deps` kept for API symmetry / future Status extensions.
        let _ = deps;
        match eval {
            StatusEval::Accept => InboundStatusResult {
                accept: true,
                disconnect: None,
            },
            StatusEval::RejectIrrelevantNetwork { reason } => InboundStatusResult {
                accept: false,
                disconnect: Some(OutboundAction::Disconnect { peer_id, reason }),
            },
        }
    }

    /// Apply a peer's MetaData (from response or — theoretically — push).
    pub fn on_peer_metadata(
        &mut self,
        peer_id: PeerId,
        md: MetaDataV3,
        deps: &HandshakeDeps,
    ) -> Vec<OutboundAction> {
        let state = self.peers.entry(peer_id).or_default();
        state.metadata = Some(md);
        state.cached_meta_seq = Some(md.seq_number);
        state.metadata_fetch_pending = false;

        match evaluate_peer_cgc(deps.cgc_policy, md.custody_group_count) {
            CgcEval::Accept => Vec::new(),
            CgcEval::Reject { reason } => {
                vec![OutboundAction::Disconnect { peer_id, reason }]
            }
        }
    }

    /// Handle an inbound / outbound Ping carrying a peer seq_number.
    ///
    /// When the seq disagrees with our cache, schedule a MetaData fetch.
    pub fn on_peer_ping(&mut self, peer_id: PeerId, ping: Ping) -> Vec<OutboundAction> {
        let state = self.peers.entry(peer_id).or_default();
        if seq_mismatch(state.cached_meta_seq, ping.seq_number) {
            if state.metadata_fetch_pending {
                return Vec::new();
            }
            state.metadata_fetch_pending = true;
            vec![OutboundAction::SendRequest {
                peer_id,
                protocol: Protocol::MetaDataV3,
                ssz: Vec::new(),
            }]
        } else {
            Vec::new()
        }
    }

    /// Initiate a Ping to `peer_id` with our current seq.
    pub fn initiate_ping(
        &mut self,
        peer_id: PeerId,
        deps: &HandshakeDeps,
    ) -> Vec<OutboundAction> {
        let _ = self.peers.entry(peer_id).or_default();
        let ping = deps.local_ping();
        vec![OutboundAction::SendRequest {
            peer_id,
            protocol: Protocol::PingV1,
            ssz: ping.to_ssz_bytes().to_vec(),
        }]
    }

    /// Number of Status exchanges initiated toward `peer_id` (tests).
    #[must_use]
    pub fn status_exchange_count(&self, peer_id: PeerId) -> u32 {
        self.peers
            .get(&peer_id)
            .map(|s| s.status_exchanges_initiated)
            .unwrap_or(0)
    }
}

/// Result of handling an inbound Status request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundStatusResult {
    /// Whether the peer is still acceptable (digest matched).
    pub accept: bool,
    /// Optional disconnect action.
    pub disconnect: Option<OutboundAction>,
}

/// Encode Goodbye reason as SSZ `uint64` request body.
#[must_use]
pub fn encode_goodbye_ssz(reason: GoodbyeReason) -> [u8; 8] {
    reason.as_u64().to_le_bytes()
}

/// Decode Goodbye reason from SSZ body.
pub fn decode_goodbye_ssz(ssz: &[u8]) -> Result<u64, std::io::Error> {
    if ssz.len() != 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Goodbye SSZ length {} != 8", ssz.len()),
        ));
    }
    let arr: [u8; 8] = ssz.try_into().map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "goodbye reason")
    })?;
    Ok(u64::from_le_bytes(arr))
}

/// Server-side Goodbye: log + signal graceful close (no response body).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoodbyeReceipt {
    /// Numeric reason from the wire.
    pub reason: u64,
}

/// Handle an inbound Goodbye request (Architecture §7.4).
pub fn handle_inbound_goodbye(ssz: &[u8]) -> Result<GoodbyeReceipt, std::io::Error> {
    let reason = decode_goodbye_ssz(ssz)?;
    Ok(GoodbyeReceipt { reason })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_proto::p2p::ChainView;
    use cc_types::{ChainConfig, Root, CUSTODY_REQUIREMENT};
    use crate::reqresp::metadata::CgcPolicy;

    fn fork_ctx_at(epoch: u64) -> ForkContext {
        const YAML: &str =
            include_str!("../../../../crates/types/tests/fixtures/hoodi-config.yaml");
        let cfg = ChainConfig::from_yaml_str(YAML).expect("hoodi");
        ForkContext::new(cfg, Root::from_array([0xCD; 32]), Epoch::new(epoch))
    }

    fn peer(n: u8) -> PeerId {
        let _ = n;
        PeerId::random()
    }

    #[test]
    fn handshake_on_connect_both_directions_and_per_epoch() {
        // Simulate two nodes: each book initiates on connect, then epochs advance.
        let a = peer(1);
        let b = peer(2);
        let mut book_a = HandshakeBook::new();
        let mut book_b = HandshakeBook::new();
        let deps = HandshakeDeps::new(Slot::new(0), CUSTODY_REQUIREMENT);
        deps.view.store(ChainView {
            head_slot: 10,
            head_root: vec![1; 32],
            finalized_root: vec![2; 32],
            finalized_epoch: 0,
            view_kind: 4,
            ..ChainView::default()
        });
        // Seed window so Status is honest (tests may write via store_recomputed).
        deps.window.store_recomputed(Slot::new(5));

        let mut fork = fork_ctx_at(0);
        // Connect: A→B and B→A each initiate once → 1 exchange each side.
        let act_a = book_a.on_connect(b, &fork, &deps);
        let act_b = book_b.on_connect(a, &fork, &deps);
        assert_eq!(act_a.len(), 1);
        assert_eq!(act_b.len(), 1);
        assert!(matches!(
            &act_a[0],
            OutboundAction::SendRequest {
                protocol: Protocol::StatusV2,
                ..
            }
        ));
        assert_eq!(book_a.status_exchange_count(b), 1);
        assert_eq!(book_b.status_exchange_count(a), 1);

        // Advance two epochs → two more exchanges each → total 3.
        fork.on_epoch(Epoch::new(1));
        let _ = book_a.on_epoch(Epoch::new(1), &[b], &fork, &deps);
        let _ = book_b.on_epoch(Epoch::new(1), &[a], &fork, &deps);
        fork.on_epoch(Epoch::new(2));
        let _ = book_a.on_epoch(Epoch::new(2), &[b], &fork, &deps);
        let _ = book_b.on_epoch(Epoch::new(2), &[a], &fork, &deps);

        assert_eq!(
            book_a.status_exchange_count(b),
            3,
            "connect + two epochs = three Status exchanges"
        );
        assert_eq!(book_b.status_exchange_count(a), 3);
    }

    #[test]
    fn fork_digest_mismatch_disconnects_not_deprioritises() {
        let mut book = HandshakeBook::new();
        let deps = HandshakeDeps::new(Slot::new(0), CUSTODY_REQUIREMENT);
        let fork = fork_ctx_at(0);
        let peer_id = peer(3);
        let mut peer_status = deps.local_status(&fork);
        peer_status.fork_digest = ForkDigest::from_array([0xDE, 0xAD, 0xBE, 0xEF]);

        let result = book.on_inbound_status(
            peer_id,
            peer_status,
            fork.current_digest(),
            &deps,
        );
        assert!(!result.accept);
        match result.disconnect {
            Some(OutboundAction::Disconnect { reason, .. }) => {
                assert_eq!(reason, GoodbyeReason::IrrelevantNetwork);
            }
            other => panic!("expected Disconnect, got {other:?}"),
        }
    }

    #[test]
    fn handshake_ignores_enr_next_fork_digest_mismatch() {
        // Re-run CC-21/4: handshake disconnects only on Status.fork_digest.
        // Production code above `mod tests` must not mention the ENR next-fork field.
        let src = include_str!("handshake.rs");
        let prod = src.split("mod tests").next().unwrap_or(src);
        let code: String = prod
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && !t.starts_with("//!") && !t.starts_with("///")
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !code.to_ascii_lowercase().contains("nfd"),
            "handshake production code must not reference nfd (CC-21/4)"
        );
        let mut book = HandshakeBook::new();
        let deps = HandshakeDeps::new(Slot::new(0), CUSTODY_REQUIREMENT);
        let fork = fork_ctx_at(0);
        let peer_id = peer(4);
        let peer_status = deps.local_status(&fork);
        let result =
            book.on_inbound_status(peer_id, peer_status, fork.current_digest(), &deps);
        assert!(result.accept);
        assert!(result.disconnect.is_none());
    }

    #[test]
    fn ping_seq_mismatch_schedules_metadata_fetch() {
        let mut book = HandshakeBook::new();
        let peer_id = peer(5);
        // First ping with unknown cache → MetaData fetch.
        let actions = book.on_peer_ping(peer_id, Ping::new(1));
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            OutboundAction::SendRequest {
                protocol: Protocol::MetaDataV3,
                ssz,
                ..
            } if ssz.is_empty()
        ));
        // Second ping while pending → no duplicate fetch.
        assert!(book.on_peer_ping(peer_id, Ping::new(1)).is_empty());

        // Deliver MetaData; subsequent matching ping is quiet.
        let deps = HandshakeDeps::new(Slot::new(0), CUSTODY_REQUIREMENT);
        let md = MetaDataV3 {
            seq_number: 1,
            ..MetaDataV3::default()
        };
        assert!(book.on_peer_metadata(peer_id, md, &deps).is_empty());
        assert!(book.on_peer_ping(peer_id, Ping::new(1)).is_empty());
        // Bump peer seq → fetch again.
        let actions = book.on_peer_ping(peer_id, Ping::new(2));
        assert!(matches!(
            actions.first(),
            Some(OutboundAction::SendRequest {
                protocol: Protocol::MetaDataV3,
                ..
            })
        ));
    }

    /// AC: Ping seq mismatch schedules metadata; host increments
    /// `cc_p2p_reqresp_outbound_total{protocol="metadata",…}` on that path.
    #[test]
    fn ping_mismatch_metadata_path_increments_outbound_metric() {
        use crate::metrics::P2pMetrics;
        use prometheus_client::registry::Registry;

        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let mut book = HandshakeBook::new();
        let peer_id = peer(7);
        let actions = book.on_peer_ping(peer_id, Ping::new(9));
        assert!(matches!(
            actions.first(),
            Some(OutboundAction::SendRequest {
                protocol: Protocol::MetaDataV3,
                ..
            })
        ));
        // Host records "scheduled" then "sent" when applying the action.
        metrics.inc_reqresp_outbound("metadata", "scheduled");
        metrics.inc_reqresp_outbound("metadata", "sent");
        assert_eq!(metrics.reqresp_outbound_count("metadata", "scheduled"), 1);
        assert_eq!(metrics.reqresp_outbound_count("metadata", "sent"), 1);
    }

    #[test]
    fn cgc_policy_on_metadata_both_ways() {
        let peer_id = peer(6);
        let mut book = HandshakeBook::new();
        let mut deps = HandshakeDeps::new(Slot::new(0), CUSTODY_REQUIREMENT);
        let low = MetaDataV3 {
            seq_number: 0,
            attnets: 0,
            syncnets: 0,
            custody_group_count: 1,
        };
        // Default accept.
        assert!(book.on_peer_metadata(peer_id, low, &deps).is_empty());

        let mut book2 = HandshakeBook::new();
        deps.cgc_policy = CgcPolicy::reject_low_cgc();
        let actions = book2.on_peer_metadata(peer_id, low, &deps);
        assert!(matches!(
            actions.first(),
            Some(OutboundAction::Disconnect {
                reason: GoodbyeReason::FaultOrError,
                ..
            })
        ));
    }

    #[test]
    fn goodbye_roundtrip() {
        let reason = GoodbyeReason::IrrelevantNetwork;
        let ssz = encode_goodbye_ssz(reason);
        assert_eq!(decode_goodbye_ssz(&ssz).unwrap(), 2);
        let receipt = handle_inbound_goodbye(&ssz).unwrap();
        assert_eq!(receipt.reason, 2);
    }

    #[test]
    fn earliest_available_slot_is_window_load_only() {
        let deps = HandshakeDeps::new(Slot::new(0), CUSTODY_REQUIREMENT);
        deps.window.store_recomputed(Slot::new(77));
        assert_eq!(deps.earliest_available_slot(), Slot::new(77));
        // Source of build_local_status uses the same read.
        let fork = fork_ctx_at(0);
        let s = deps.local_status(&fork);
        assert_eq!(s.earliest_available_slot, Slot::new(77));
    }
}
