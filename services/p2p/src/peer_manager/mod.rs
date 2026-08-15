//! Peer manager — `PeerTable`, dial scheduler, limits policy, backoff/ban,
//! eviction, and `Goodbye`-on-disconnect (CC-20c / Architecture §3.6).
//!
//! One task, one table, **no locks**. Hard caps live in
//! `connection_limits::Behaviour` / `allow_block_list::Behaviour` on the swarm;
//! this module owns *which* peers and *when*.
//!
//! File ownership: Stream H owns `{mod,dial,ban}.rs`; Stream G owns `score.rs`
//! (fields defined here; semantics filled by CC-22c).

pub mod ban;
pub mod dial;
pub mod score;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cc_libp2p::{Multiaddr, PeerId};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use crate::channels::{ConnEvent, ConnectionDirection, GoodbyeReason, SwarmCommand};
use crate::metrics::{Direction as MetricDirection, P2pMetrics};

use self::ban::{BanList, DialBackoff};
use self::dial::{DialRequest, schedule_dials};
pub use self::score::{
    APP_SCORE_BAN, APP_SCORE_DECAY, APP_SCORE_DISCONNECT, APP_SCORE_MAX, APP_SCORE_MIN,
    DEFAULT_APP_SCORE, DEFAULT_GOSSIP_SCORE, GOSSIP_THRESHOLD, GossipClass, apply_gossip_coupling,
    apply_penalty, apply_penalty_with_metrics, apply_useful_delivery, decay_app_score,
    gossip_coupling_delta, is_mesh_protected, observe_score_snapshot, penalty_delta,
    penalty_for_chain_class, penalty_for_gossip_reject, sanitize_app_score, sanitize_gossip_score,
    should_ban, should_disconnect,
};

/// Application-score decay / gossip-coupling interval (1 slot = 12 s, §5.6).
pub const SCORE_DECAY_INTERVAL: Duration = Duration::from_secs(12);

// ── knobs (CC-20/3 defaults) ────────────────────────────────────────────────

/// Default target peer count (dial while `connected < target`).
pub const DEFAULT_TARGET_PEERS: usize = 50;
/// Default max established peers (hard: connection_limits; soft: scheduler).
pub const DEFAULT_MAX_PEERS: usize = 100;
/// Default inbound established cap.
pub const DEFAULT_MAX_INBOUND: usize = 60;
/// Default outbound established cap.
pub const DEFAULT_MAX_OUTBOUND: usize = 60;
/// Default max concurrent dials.
pub const DEFAULT_MAX_CONCURRENT_DIALS: usize = 8;
/// Scheduler / score-check tick.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);

// ── config ──────────────────────────────────────────────────────────────────

/// A config-supplied static peer (the only dial source until CC-21c).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticPeer {
    /// Peer id (when known from ENR / config).
    pub peer_id: PeerId,
    /// Multiaddr to dial.
    pub addr: Multiaddr,
}

/// Peer-manager knobs (all overridable; defaults match Architecture §3.6).
#[derive(Debug, Clone)]
pub struct PeerManagerConfig {
    /// Dial while `connected < target`.
    pub target_peers: usize,
    /// Soft max; hard max is connection_limits.
    pub max_peers: usize,
    /// Inbound established soft intent (hard: connection_limits).
    pub max_inbound: usize,
    /// Outbound established soft intent (hard: connection_limits).
    pub max_outbound: usize,
    /// Max in-flight dials.
    pub max_concurrent_dials: usize,
    /// Static peers only (discovery is CC-21c).
    pub static_peers: Vec<StaticPeer>,
    /// Tick interval (default 1 s).
    pub tick_interval: Duration,
}

impl Default for PeerManagerConfig {
    fn default() -> Self {
        Self {
            target_peers: DEFAULT_TARGET_PEERS,
            max_peers: DEFAULT_MAX_PEERS,
            max_inbound: DEFAULT_MAX_INBOUND,
            max_outbound: DEFAULT_MAX_OUTBOUND,
            max_concurrent_dials: DEFAULT_MAX_CONCURRENT_DIALS,
            static_peers: Vec::new(),
            tick_interval: DEFAULT_TICK_INTERVAL,
        }
    }
}

// ── peer record ─────────────────────────────────────────────────────────────

/// Connection lifecycle state for one peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected (and not dialing).
    Disconnected,
    /// Outbound dial in flight.
    Dialing,
    /// Established.
    Connected,
}

/// Peer MetaData v3 (CC-23b wire type, mirrored into the table).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetaDataV3 {
    /// Sequence number.
    pub seq_number: u64,
    /// Attestation subnet bitfield (`BitVector[64]` as `u64`).
    pub attnets: u64,
    /// Sync-committee subnet bitfield (`BitVector[4]` as low nibble).
    pub syncnets: u8,
    /// Custody group count.
    pub cgc: u64,
}

impl From<crate::reqresp::MetaDataV3> for MetaDataV3 {
    fn from(m: crate::reqresp::MetaDataV3) -> Self {
        Self {
            seq_number: m.seq_number,
            attnets: m.attnets,
            syncnets: m.syncnets,
            cgc: m.custody_group_count,
        }
    }
}

/// Peer `Status v2` six fields (CC-23b).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerStatus {
    /// Fork digest from the peer's Status.
    pub fork_digest: [u8; 4],
    /// Finalized root.
    pub finalized_root: [u8; 32],
    /// Finalized epoch.
    pub finalized_epoch: u64,
    /// Head root.
    pub head_root: [u8; 32],
    /// Head slot.
    pub head_slot: u64,
    /// Peer-advertised earliest available slot.
    pub earliest_available_slot: u64,
}

impl From<crate::reqresp::StatusV2> for PeerStatus {
    fn from(s: crate::reqresp::StatusV2) -> Self {
        let mut fork_digest = [0u8; 4];
        fork_digest.copy_from_slice(s.fork_digest.as_slice());
        Self {
            fork_digest,
            finalized_root: *s.finalized_root.as_array(),
            finalized_epoch: s.finalized_epoch.as_u64(),
            head_root: *s.head_root.as_array(),
            head_slot: s.head_slot.as_u64(),
            earliest_available_slot: s.earliest_available_slot.as_u64(),
        }
    }
}

/// Snapshot of a peer's advertised ENR fields relevant to dial policy.
///
/// Full ENR retention is not required for Phase 2; we keep the digests and
/// bitfields discovery already decoded. **`nfd` is intentionally informational
/// only** — an `nfd` mismatch MUST NOT cause a disconnect before the fork
/// boundary (CC-21/4, spec delta 11). There is no disconnect condition on
/// `nfd` anywhere in this module (grep guard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEnrInfo {
    /// Advertised `eth2.fork_digest` when present.
    pub fork_digest: Option<[u8; 4]>,
    /// Advertised `nfd` when present (not a disconnect input).
    pub nfd: Option<[u8; 4]>,
    /// Advertised `cgc` when present.
    pub cgc: Option<u64>,
    /// Advertised attestation subnet bitfield (`BitVector[64]` as `u64`).
    pub attnets: Option<u64>,
    /// Advertised sync-committee subnet bitfield (`BitVector[4]` as low nibble).
    pub syncnets: Option<u8>,
}

/// One peer's row in the table (Architecture §3.6).
#[derive(Debug, Clone)]
pub struct PeerRecord {
    /// libp2p peer id.
    pub peer_id: PeerId,
    /// Known multiaddrs.
    pub addrs: Vec<Multiaddr>,
    /// Connection state.
    pub state: ConnectionState,
    /// Direction of the established connection (`None` if not connected).
    pub direction: Option<ConnectionDirection>,
    /// ENR-derived fields from discovery (CC-21c). Not a disconnect input for `nfd`.
    pub enr: Option<PeerEnrInfo>,
    /// MetaData v3 (CC-23b) — `None` until received.
    pub metadata: Option<MetaDataV3>,
    /// Last Status (CC-23b).
    pub status: Option<PeerStatus>,
    /// RTT from libp2p ping (if known).
    pub rtt: Option<Duration>,
    /// GossipSub score field (≈ −16000…+30). Semantics: CC-22c.
    pub gossip_score: f64,
    /// Application score field (−100…+100). Semantics: CC-22c.
    pub app_score: f64,
    /// How many of our sampled columns this peer covers (0 until CC-24a).
    /// Discovery may stash dial priority here until CC-24a computes real usefulness.
    pub custody_usefulness: u32,
    /// Dial backoff state.
    pub dial_backoff: DialBackoff,
    /// When the current connection was established.
    pub connected_at: Option<Instant>,
    /// Peer is in a gossipsub mesh for a topic we depend on.
    pub in_mesh: bool,
}

impl PeerRecord {
    /// New disconnected peer with default scores.
    #[must_use]
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            addrs: Vec::new(),
            state: ConnectionState::Disconnected,
            direction: None,
            enr: None,
            metadata: None,
            status: None,
            rtt: None,
            gossip_score: DEFAULT_GOSSIP_SCORE,
            app_score: DEFAULT_APP_SCORE,
            custody_usefulness: 0,
            dial_backoff: DialBackoff::default(),
            connected_at: None,
            in_mesh: false,
        }
    }
}

// ── table ───────────────────────────────────────────────────────────────────

/// Sole peer state table — owned by the peer-manager task (no locks).
#[derive(Debug, Default)]
pub struct PeerTable {
    peers: HashMap<PeerId, PeerRecord>,
}

impl PeerTable {
    /// Lookup.
    #[must_use]
    pub fn get(&self, id: &PeerId) -> Option<&PeerRecord> {
        self.peers.get(id)
    }

    /// Mutable lookup.
    pub fn get_mut(&mut self, id: &PeerId) -> Option<&mut PeerRecord> {
        self.peers.get_mut(id)
    }

    /// Insert or replace a full record.
    pub fn upsert(&mut self, rec: PeerRecord) {
        self.peers.insert(rec.peer_id, rec);
    }

    /// Ensure a row exists; return mutable ref.
    pub fn entry_mut(&mut self, id: PeerId) -> &mut PeerRecord {
        self.peers.entry(id).or_insert_with(|| PeerRecord::new(id))
    }

    /// Mark dialing.
    pub fn insert_dialing(&mut self, id: PeerId, addr: Multiaddr) {
        let rec = self.entry_mut(id);
        if !rec.addrs.contains(&addr) {
            rec.addrs.push(addr);
        }
        rec.state = ConnectionState::Dialing;
        rec.direction = Some(ConnectionDirection::Outbound);
    }

    /// Mark connected.
    pub fn insert_connected(&mut self, id: PeerId, direction: ConnectionDirection, now: Instant) {
        let rec = self.entry_mut(id);
        rec.state = ConnectionState::Connected;
        rec.direction = Some(direction);
        rec.connected_at = Some(now);
        rec.dial_backoff.on_success();
    }

    /// Mark disconnected (keeps the row for backoff / scores).
    pub fn on_disconnected(&mut self, id: &PeerId) {
        if let Some(rec) = self.peers.get_mut(id) {
            rec.state = ConnectionState::Disconnected;
            rec.direction = None;
            rec.connected_at = None;
            rec.in_mesh = false;
        }
    }

    /// Established peer count.
    #[must_use]
    pub fn connected_count(&self) -> usize {
        self.peers
            .values()
            .filter(|p| p.state == ConnectionState::Connected)
            .count()
    }

    /// In-flight dials.
    #[must_use]
    pub fn dialing_count(&self) -> usize {
        self.peers
            .values()
            .filter(|p| p.state == ConnectionState::Dialing)
            .count()
    }

    /// Connected inbound count.
    #[must_use]
    pub fn connected_inbound(&self) -> usize {
        self.peers
            .values()
            .filter(|p| {
                p.state == ConnectionState::Connected
                    && p.direction == Some(ConnectionDirection::Inbound)
            })
            .count()
    }

    /// Connected outbound count.
    #[must_use]
    pub fn connected_outbound(&self) -> usize {
        self.peers
            .values()
            .filter(|p| {
                p.state == ConnectionState::Connected
                    && p.direction == Some(ConnectionDirection::Outbound)
            })
            .count()
    }

    /// Iterator over connected peer ids.
    pub fn connected_peer_ids(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers
            .values()
            .filter(|p| p.state == ConnectionState::Connected)
            .map(|p| p.peer_id)
    }

    /// All records (tests / eviction).
    pub fn iter(&self) -> impl Iterator<Item = &PeerRecord> {
        self.peers.values()
    }

    /// Mutable records (score pipeline).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut PeerRecord> {
        self.peers.values_mut()
    }

    /// Number of rows (connected or not).
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Empty table?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

// ── eviction ────────────────────────────────────────────────────────────────

/// Pick the peer to evict under Architecture §3.6 order.
///
/// Order among **non-protected** connected peers: ascending `app_score`, then
/// ascending `custody_usefulness`, then youngest connection first.
/// Mesh members above [`score::GOSSIP_THRESHOLD`] are skipped.
#[must_use]
pub fn select_eviction_victim(table: &PeerTable) -> Option<PeerId> {
    let mut candidates: Vec<&PeerRecord> = table
        .iter()
        .filter(|p| p.state == ConnectionState::Connected)
        .filter(|p| !is_mesh_protected(p.in_mesh, p.gossip_score))
        .collect();

    if candidates.is_empty() {
        return None;
    }

    candidates.sort_by(|a, b| {
        a.app_score
            .partial_cmp(&b.app_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.custody_usefulness.cmp(&b.custody_usefulness))
            .then_with(|| {
                // Youngest first: larger connected_at first in sort → reverse Instant order.
                match (a.connected_at, b.connected_at) {
                    (Some(ta), Some(tb)) => tb.cmp(&ta),
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            })
            .then_with(|| a.peer_id.cmp(&b.peer_id))
    });

    candidates.first().map(|p| p.peer_id)
}

// ── manager ─────────────────────────────────────────────────────────────────

/// Peer manager state machine (runs as one supervised task).
#[derive(Debug)]
pub struct PeerManager {
    /// Peer table.
    pub table: PeerTable,
    /// Ban set (policy); swarm block-list is the enforcement.
    pub bans: BanList,
    /// Knobs.
    pub config: PeerManagerConfig,
    /// Commands → swarm.
    cmd_tx: mpsc::Sender<SwarmCommand>,
    /// Metrics.
    metrics: P2pMetrics,
    /// Captured commands when `capture` is set (tests).
    capture: Option<mpsc::UnboundedSender<SwarmCommand>>,
    /// Last time we applied gossip coupling + app decay (1 slot).
    last_score_decay_at: Option<Instant>,
}

impl PeerManager {
    /// Construct with live cmd channel + metrics.
    #[must_use]
    pub fn new(
        config: PeerManagerConfig,
        cmd_tx: mpsc::Sender<SwarmCommand>,
        metrics: P2pMetrics,
    ) -> Self {
        Self {
            table: PeerTable::default(),
            bans: BanList::default(),
            config,
            cmd_tx,
            metrics,
            capture: None,
            last_score_decay_at: None,
        }
    }

    /// Test helper: also mirror every emitted command onto `capture`.
    #[must_use]
    pub fn with_capture(mut self, capture: mpsc::UnboundedSender<SwarmCommand>) -> Self {
        self.capture = Some(capture);
        self
    }

    /// Handle a connection event from the swarm task.
    pub async fn handle_conn_event(&mut self, event: ConnEvent, now: Instant) {
        match event {
            ConnEvent::ConnectionEstablished {
                peer_id, direction, ..
            } => {
                self.on_connected(peer_id, direction, now).await;
            }
            ConnEvent::ConnectionClosed { peer_id, .. } => {
                self.on_closed(peer_id);
            }
            ConnEvent::DialFailure { peer_id, .. } => {
                if let Some(id) = peer_id {
                    self.on_dial_failure(id, now);
                }
            }
            ConnEvent::NewListenAddr { .. } => {
                // Informational only.
            }
            ConnEvent::PeerPenalty { peer_id, reason } => {
                let reason = match reason.as_str() {
                    "rate_limit" => crate::metrics::PeerPenaltyReason::RateLimit,
                    "reqresp_fault" => crate::metrics::PeerPenaltyReason::ReqrespFault,
                    _ => crate::metrics::PeerPenaltyReason::ReqrespFault,
                };
                if let Some(rec) = self.table.get_mut(&peer_id) {
                    let _ = score::apply_penalty_with_metrics(
                        &mut rec.app_score,
                        reason,
                        &self.metrics,
                    );
                } else {
                    // Peer already gone — still count the metric.
                    self.metrics.inc_peer_penalty(reason);
                }
            }
        }
        // Scheduler also runs on every connection event.
        self.run_scheduler(now).await;
        self.enforce_scores(now).await;
    }

    /// Periodic tick (1 s): scheduler + score decay/coupling (slot) + enforce.
    pub async fn on_tick(&mut self, now: Instant) {
        self.run_scheduler(now).await;
        self.tick_scores(now).await;
    }

    /// Manual disconnect path (always Goodbye then close).
    pub async fn disconnect(&mut self, peer_id: PeerId, reason: GoodbyeReason) {
        self.emit_disconnect(peer_id, reason, false).await;
    }

    /// Apply an external app_score update (tests / producers).
    ///
    /// Non-finite values are sanitized to 0 and clamped to [−100, +100] (H2).
    pub async fn set_app_score(&mut self, peer_id: PeerId, score: f64, now: Instant) {
        let rec = self.table.entry_mut(peer_id);
        rec.app_score = sanitize_app_score(score);
        self.enforce_scores(now).await;
    }

    /// Apply a named penalty to a peer and emit metrics (CC-22c / §3.7).
    pub async fn apply_peer_penalty(
        &mut self,
        peer_id: PeerId,
        reason: crate::metrics::PeerPenaltyReason,
        now: Instant,
    ) {
        let rec = self.table.entry_mut(peer_id);
        apply_penalty_with_metrics(&mut rec.app_score, reason, &self.metrics);
        self.enforce_scores(now).await;
    }

    /// Mark mesh membership (gossip path / tests).
    pub fn set_in_mesh(&mut self, peer_id: PeerId, in_mesh: bool) {
        self.table.entry_mut(peer_id).in_mesh = in_mesh;
    }

    /// Set custody usefulness (CC-24a will compute; tests set directly).
    pub fn set_custody_usefulness(&mut self, peer_id: PeerId, n: u32) {
        self.table.entry_mut(peer_id).custody_usefulness = n;
    }

    /// Set gossip_score field (not a disconnect input).
    ///
    /// Non-finite values are sanitized to 0 (H2). Coupling reads this on the
    /// next score-decay tick; does not disconnect by itself (ADR P2-09).
    pub fn set_gossip_score(&mut self, peer_id: PeerId, score: f64) {
        self.table.entry_mut(peer_id).gossip_score = sanitize_gossip_score(score);
    }

    /// Offer a discovery-sourced dial candidate (CC-21c).
    ///
    /// Inserts/updates the table row with the multiaddr and priority. Does **not**
    /// inspect `nfd` and never disconnects on ENR field mismatch. Actual dial
    /// emission is left to [`Self::run_scheduler`].
    pub fn offer_discovered(
        &mut self,
        peer_id: PeerId,
        addr: Multiaddr,
        priority: u32,
        enr_info: Option<PeerEnrInfo>,
    ) {
        if self.bans.is_banned(&peer_id) {
            return;
        }
        let rec = self.table.entry_mut(peer_id);
        if !rec.addrs.contains(&addr) {
            rec.addrs.push(addr);
        }
        // Stash discovery priority until CC-24a computes real custody usefulness.
        if priority > rec.custody_usefulness {
            rec.custody_usefulness = priority;
        }
        if enr_info.is_some() {
            rec.enr = enr_info;
        }
    }

    /// Active peer ids (connected or dialing) for the discovery filter.
    #[must_use]
    pub fn active_peer_ids(&self) -> Vec<PeerId> {
        self.table
            .iter()
            .filter(|p| {
                matches!(
                    p.state,
                    ConnectionState::Connected | ConnectionState::Dialing
                )
            })
            .map(|p| p.peer_id)
            .collect()
    }

    async fn on_connected(
        &mut self,
        peer_id: PeerId,
        direction: ConnectionDirection,
        now: Instant,
    ) {
        if self.bans.is_banned(&peer_id) {
            // Swarm should have refused; still eject if we observe it.
            warn!(%peer_id, "banned peer connected; emitting goodbye + block");
            self.emit_disconnect(peer_id, GoodbyeReason::FaultOrError, true)
                .await;
            return;
        }
        self.table.insert_connected(peer_id, direction, now);
        self.sync_peer_metrics();
        debug!(%peer_id, ?direction, "peer connected");
    }

    fn on_closed(&mut self, peer_id: PeerId) {
        self.table.on_disconnected(&peer_id);
        self.sync_peer_metrics();
        debug!(%peer_id, "peer disconnected");
    }

    fn on_dial_failure(&mut self, peer_id: PeerId, now: Instant) {
        let rec = self.table.entry_mut(peer_id);
        if rec.state == ConnectionState::Dialing {
            rec.state = ConnectionState::Disconnected;
            rec.direction = None;
        }
        let delay = rec.dial_backoff.on_failure(now);
        debug!(%peer_id, ?delay, failures = rec.dial_backoff.failures, "dial failure backoff");
    }

    async fn run_scheduler(&mut self, now: Instant) {
        // If at max and static peers remain undialed that look better, evict first.
        if self.table.connected_count() >= self.config.max_peers
            && let Some(candidate) = self.better_static_candidate(now)
            && let Some(victim) = select_eviction_victim(&self.table)
            && self.is_better_than(&candidate, &victim)
        {
            debug!(%victim, "evicting for better peer");
            self.emit_disconnect(victim, GoodbyeReason::TooManyPeers, false)
                .await;
            // Victim still counted until ConnectionClosed; soft-mark so
            // subsequent dials in this pass can proceed under max.
            self.table.on_disconnected(&victim);
            self.sync_peer_metrics();
        }

        let requests = schedule_dials(&self.table, &self.bans, &self.config, now);
        for DialRequest { peer_id, addr } in requests {
            self.table.insert_dialing(peer_id, addr.clone());
            // Dial is best-effort; hard pending-outgoing cap is swarm-level.
            self.emit_best_effort(SwarmCommand::Dial { peer_id, addr });
        }
    }

    /// Ordered score pipeline (CC-22c / M1): sanitize → couple+decay (slot) →
    /// observe metrics → enforce disconnect/ban.
    async fn tick_scores(&mut self, now: Instant) {
        // Always strip non-finite poison so enforce never sees NaN (H2).
        for rec in self.table.iter_mut() {
            rec.app_score = sanitize_app_score(rec.app_score);
            rec.gossip_score = sanitize_gossip_score(rec.gossip_score);
        }

        let due = match self.last_score_decay_at {
            None => true,
            Some(t) => now.saturating_duration_since(t) >= SCORE_DECAY_INTERVAL,
        };
        if due {
            for rec in self.table.iter_mut() {
                // Coupling first (one-way damped), then decay toward 0.
                apply_gossip_coupling(&mut rec.app_score, rec.gossip_score);
                decay_app_score(&mut rec.app_score);
            }
            self.last_score_decay_at = Some(now);
        }

        // R-3 early warning: left tail of peer_score + below GossipThreshold.
        let gossip: Vec<f64> = self.table.iter().map(|r| r.gossip_score).collect();
        let apps: Vec<f64> = self.table.iter().map(|r| r.app_score).collect();
        observe_score_snapshot(&self.metrics, gossip, apps);

        self.enforce_scores(now).await;
    }

    /// Score-driven disconnect / ban — **app_score only** (ADR P2-09).
    async fn enforce_scores(&mut self, _now: Instant) {
        let mut to_disconnect: Vec<PeerId> = Vec::new();
        let mut to_ban: Vec<PeerId> = Vec::new();

        for rec in self.table.iter() {
            if rec.state != ConnectionState::Connected {
                continue;
            }
            // Defensive sanitize in case a raw field write bypassed setters.
            let app = sanitize_app_score(rec.app_score);
            if should_ban(app) {
                to_ban.push(rec.peer_id);
            } else if should_disconnect(app) {
                to_disconnect.push(rec.peer_id);
            }
        }

        for peer_id in to_ban {
            self.ban_peer(peer_id, GoodbyeReason::FaultOrError).await;
        }
        for peer_id in to_disconnect {
            self.emit_disconnect(peer_id, GoodbyeReason::FaultOrError, false)
                .await;
            self.table.on_disconnected(&peer_id);
            self.sync_peer_metrics();
        }
    }

    /// Ban: atomic ClosePeer(ban=true) so Disconnect and BlockPeer cannot split (H2).
    pub async fn ban_peer(&mut self, peer_id: PeerId, reason: GoodbyeReason) {
        let newly = self.bans.ban(peer_id);
        if newly {
            debug!(%peer_id, "peer banned");
        }
        if self
            .table
            .get(&peer_id)
            .is_some_and(|r| r.state == ConnectionState::Connected)
        {
            self.emit_disconnect(peer_id, reason, true).await;
            self.table.on_disconnected(&peer_id);
            self.sync_peer_metrics();
        } else {
            // Not connected — still enforce swarm block list (await, never drop).
            self.emit_policy(SwarmCommand::BlockPeer { peer_id }).await;
        }
    }

    /// Atomic Goodbye + Disconnect (+ optional ban) — every intentional close path (H2).
    async fn emit_disconnect(&mut self, peer_id: PeerId, reason: GoodbyeReason, ban: bool) {
        // Capture expands to the logical multi-step sequence for harnesses.
        if let Some(cap) = &self.capture {
            let _ = cap.send(SwarmCommand::Goodbye { peer_id, reason });
            let _ = cap.send(SwarmCommand::Disconnect { peer_id });
            if ban {
                let _ = cap.send(SwarmCommand::BlockPeer { peer_id });
            }
        }
        self.emit_policy(SwarmCommand::ClosePeer {
            peer_id,
            reason,
            ban,
        })
        .await;
    }

    /// Policy cmds: **await** capacity — never drop ban/close (H2). Architecture
    /// §2.2: peer manager may block on `cmd_tx`; swarm may not.
    async fn emit_policy(&mut self, cmd: SwarmCommand) {
        // Capture for ClosePeer is handled by emit_disconnect; other policy cmds
        // (standalone BlockPeer) still mirror here when not pre-captured.
        if !matches!(cmd, SwarmCommand::ClosePeer { .. })
            && let Some(cap) = &self.capture
        {
            let _ = cap.send(cmd.clone());
        }
        match self.cmd_tx.send(cmd).await {
            Ok(()) => {
                let d = self.metrics.queue_depth(crate::metrics::QueueName::Cmd);
                self.metrics.set_queue_depth(
                    crate::metrics::QueueName::Cmd,
                    (d + 1).min(crate::channels::CMD_BOUND as i64),
                );
            }
            Err(_) => {
                warn!("cmd channel closed; policy command not delivered");
            }
        }
    }

    /// Best-effort cmds (Dial): try_send; under pressure skip rather than stall.
    fn emit_best_effort(&mut self, cmd: SwarmCommand) {
        if let Some(cap) = &self.capture {
            let _ = cap.send(cmd.clone());
        }
        match self.cmd_tx.try_send(cmd) {
            Ok(()) => {
                let d = self.metrics.queue_depth(crate::metrics::QueueName::Cmd);
                self.metrics.set_queue_depth(
                    crate::metrics::QueueName::Cmd,
                    (d + 1).min(crate::channels::CMD_BOUND as i64),
                );
            }
            Err(mpsc::error::TrySendError::Full(cmd)) => {
                // Roll back dialing state if we failed to enqueue Dial.
                if let SwarmCommand::Dial { peer_id, .. } = &cmd
                    && let Some(rec) = self.table.get_mut(peer_id)
                    && rec.state == ConnectionState::Dialing
                {
                    rec.state = ConnectionState::Disconnected;
                    rec.direction = None;
                }
                warn!(?cmd, "cmd queue full; best-effort command dropped");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                trace!("cmd channel closed");
            }
        }
    }

    fn sync_peer_metrics(&self) {
        self.metrics.set_peers(
            MetricDirection::Inbound,
            self.table.connected_inbound() as i64,
        );
        self.metrics.set_peers(
            MetricDirection::Outbound,
            self.table.connected_outbound() as i64,
        );
        // Pure intersection math lives in `das::custody` (`count_custody_compatible_peers`
        // / `is_peer_custody_compatible`). Until peer rows carry a discv5 `NodeId`
        // for the ENR path, keep the gauge as count of peers with usefulness > 0
        // (discovery stashes priority there; real coverage uses `CustodyManager::peer_coverage`).
        let compatible = self
            .table
            .iter()
            .filter(|p| p.state == ConnectionState::Connected && p.custody_usefulness > 0)
            .count();
        self.metrics.set_peers_custody_compatible(compatible as i64);
    }

    fn better_static_candidate(&self, now: Instant) -> Option<PeerId> {
        for sp in &self.config.static_peers {
            if self.bans.is_banned(&sp.peer_id) {
                continue;
            }
            let connected_or_dialing = self.table.get(&sp.peer_id).is_some_and(|r| {
                matches!(
                    r.state,
                    ConnectionState::Connected | ConnectionState::Dialing
                )
            });
            if connected_or_dialing {
                continue;
            }
            if let Some(rec) = self.table.get(&sp.peer_id)
                && !rec.dial_backoff.ready(now)
            {
                continue;
            }
            return Some(sp.peer_id);
        }
        None
    }

    fn is_better_than(&self, candidate: &PeerId, victim: &PeerId) -> bool {
        let c_score = self
            .table
            .get(candidate)
            .map(|r| r.app_score)
            .unwrap_or(0.0);
        let v_score = self.table.get(victim).map(|r| r.app_score).unwrap_or(0.0);
        let c_c = self
            .table
            .get(candidate)
            .map(|r| r.custody_usefulness)
            .unwrap_or(0);
        let v_c = self
            .table
            .get(victim)
            .map(|r| r.custody_usefulness)
            .unwrap_or(0);
        c_score > v_score || (c_score == v_score && c_c > v_c)
    }
}

/// Run the peer-manager task until `conn_rx` closes or shutdown.
///
/// `discovery_rx` is the discovery → dial path (CC-21c); when `None`, only
/// static peers are dialed. `penalty_rx` applies app-score penalties from the
/// gossip validation path (CC-22d).
pub async fn run_peer_manager(
    mut manager: PeerManager,
    mut conn_rx: mpsc::Receiver<ConnEvent>,
    mut discovery_rx: Option<mpsc::Receiver<crate::discovery::DiscoveredPeer>>,
    mut penalty_rx: Option<mpsc::Receiver<crate::channels::PeerPenaltyCmd>>,
    peer_view_tx: Option<tokio::sync::watch::Sender<crate::discovery::DiscoveryPeerView>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let tick = manager.config.tick_interval;
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Seed static peers into the table so backoff rows exist.
    for sp in manager.config.static_peers.clone() {
        let rec = manager.table.entry_mut(sp.peer_id);
        if !rec.addrs.contains(&sp.addr) {
            rec.addrs.push(sp.addr.clone());
        }
    }
    // Initial schedule.
    manager.on_tick(Instant::now()).await;
    publish_peer_view(&manager, &peer_view_tx);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("peer manager shutdown");
                    break;
                }
            }
            _ = interval.tick() => {
                manager.on_tick(Instant::now()).await;
                publish_peer_view(&manager, &peer_view_tx);
            }
            event = conn_rx.recv() => {
                match event {
                    Some(ev) => {
                        let depth = manager.metrics.queue_depth(crate::metrics::QueueName::Conn);
                        if depth > 0 {
                            manager.metrics.set_queue_depth(
                                crate::metrics::QueueName::Conn,
                                depth - 1,
                            );
                        }
                        manager.handle_conn_event(ev, Instant::now()).await;
                        publish_peer_view(&manager, &peer_view_tx);
                    }
                    None => {
                        debug!("conn channel closed; peer manager exiting");
                        break;
                    }
                }
            }
            discovered = async {
                match discovery_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match discovered {
                    Some(d) => {
                        manager.offer_discovered(d.peer_id, d.addr, d.priority, d.enr_info);
                        manager.on_tick(Instant::now()).await;
                        publish_peer_view(&manager, &peer_view_tx);
                    }
                    None => {
                        // Discovery task exited; keep PM alive on conn/tick only.
                        discovery_rx = None;
                    }
                }
            }
            penalty = async {
                match penalty_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match penalty {
                    Some(cmd) => {
                        manager
                            .apply_peer_penalty(cmd.peer_id, cmd.reason, Instant::now())
                            .await;
                        publish_peer_view(&manager, &peer_view_tx);
                    }
                    None => {
                        penalty_rx = None;
                    }
                }
            }
        }
    }
}

fn publish_peer_view(
    manager: &PeerManager,
    peer_view_tx: &Option<tokio::sync::watch::Sender<crate::discovery::DiscoveryPeerView>>,
) {
    if let Some(tx) = peer_view_tx {
        let mut attnet_peer_counts = [0u16; crate::discovery::ATTNETS_BIT_LEN];
        let mut syncnet_peer_counts = [0u16; crate::discovery::SYNCNETS_BIT_LEN];
        for rec in manager.table.iter() {
            if rec.state != ConnectionState::Connected {
                continue;
            }
            if let Some(info) = &rec.enr {
                if let Some(bits) = info.attnets {
                    for (s, count) in attnet_peer_counts.iter_mut().enumerate() {
                        if bits & (1u64 << s) != 0 {
                            *count = count.saturating_add(1);
                        }
                    }
                }
                if let Some(bits) = info.syncnets {
                    for (s, count) in syncnet_peer_counts.iter_mut().enumerate() {
                        if bits & (1u8 << s) != 0 {
                            *count = count.saturating_add(1);
                        }
                    }
                }
            }
        }
        let view = crate::discovery::DiscoveryPeerView {
            connected: manager.table.connected_count(),
            active: manager.active_peer_ids(),
            attnet_peer_counts,
            syncnet_peer_counts,
        };
        let _ = tx.send(view);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::channels::GoodbyeReason;
    use cc_libp2p::reexport::Keypair;
    use prometheus_client::registry::Registry;

    fn metrics() -> P2pMetrics {
        let mut reg = Registry::default();
        P2pMetrics::register(&mut reg)
    }

    fn pid() -> PeerId {
        PeerId::from_public_key(&Keypair::generate_secp256k1().public())
    }

    fn manager(
        cfg: PeerManagerConfig,
    ) -> (
        PeerManager,
        mpsc::UnboundedReceiver<SwarmCommand>,
        mpsc::Receiver<SwarmCommand>,
    ) {
        // Keep cmd_rx alive so policy `send().await` (H2) does not see a closed channel.
        let (cmd_tx, cmd_rx) = mpsc::channel(512);
        let (cap_tx, cap_rx) = mpsc::unbounded_channel();
        let m = PeerManager::new(cfg, cmd_tx, metrics()).with_capture(cap_tx);
        (m, cap_rx, cmd_rx)
    }

    fn drain(rx: &mut mpsc::UnboundedReceiver<SwarmCommand>) -> Vec<SwarmCommand> {
        let mut v = Vec::new();
        while let Ok(c) = rx.try_recv() {
            v.push(c);
        }
        v
    }

    #[tokio::test]
    async fn past_max_stops_dialling_below_target_resumes_120_dummy() {
        // 120 dummy static peers; max 100; target 50.
        let static_peers: Vec<_> = (0..120)
            .map(|_| StaticPeer {
                peer_id: pid(),
                addr: "/ip4/127.0.0.1/tcp/1".parse().unwrap(),
            })
            .collect();
        let cfg = PeerManagerConfig {
            target_peers: 50,
            max_peers: 100,
            max_inbound: 60,
            max_outbound: 60,
            max_concurrent_dials: 8,
            static_peers: static_peers.clone(),
            tick_interval: Duration::from_secs(1),
        };
        let (mut mgr, mut cap, _cmd_rx) = manager(cfg);
        let now = Instant::now();

        // Drive 100 connected (dummy hosts) — past/at max.
        for sp in static_peers.iter().take(100) {
            mgr.handle_conn_event(
                ConnEvent::ConnectionEstablished {
                    peer_id: sp.peer_id,
                    direction: ConnectionDirection::Outbound,
                    endpoint: sp.addr.clone(),
                },
                now,
            )
            .await;
        }
        let _ = drain(&mut cap);
        assert_eq!(mgr.table.connected_count(), 100);

        // Tick at max → no dials.
        mgr.on_tick(now).await;
        let cmds = drain(&mut cap);
        assert!(
            !cmds.iter().any(|c| matches!(c, SwarmCommand::Dial { .. })),
            "must not dial at max; got {cmds:?}"
        );

        // Drop 60 peers → connected = 40 < target 50 → dialling resumes.
        for sp in static_peers.iter().take(60) {
            mgr.handle_conn_event(
                ConnEvent::ConnectionClosed {
                    peer_id: sp.peer_id,
                },
                now,
            )
            .await;
        }
        assert_eq!(mgr.table.connected_count(), 40);
        let cmds = drain(&mut cap);
        let dials: Vec<_> = cmds
            .iter()
            .filter(|c| matches!(c, SwarmCommand::Dial { .. }))
            .collect();
        assert!(
            !dials.is_empty(),
            "dialling must resume below target; cmds={cmds:?}"
        );
        assert!(
            dials.len() <= 8,
            "concurrent dials ≤ 8, got {}",
            dials.len()
        );
    }

    #[tokio::test]
    async fn concurrent_dials_never_exceed_8() {
        let static_peers: Vec<_> = (0..50)
            .map(|_| StaticPeer {
                peer_id: pid(),
                addr: "/ip4/127.0.0.1/tcp/1".parse().unwrap(),
            })
            .collect();
        let cfg = PeerManagerConfig {
            target_peers: 50,
            max_peers: 100,
            max_inbound: 60,
            max_outbound: 60,
            max_concurrent_dials: 8,
            static_peers,
            tick_interval: Duration::from_secs(1),
        };
        let (mut mgr, mut cap, _cmd_rx) = manager(cfg);
        let now = Instant::now();

        // Stall every dial: schedule once, leave them Dialing.
        mgr.on_tick(now).await;
        let cmds = drain(&mut cap);
        let dials: Vec<_> = cmds
            .into_iter()
            .filter(|c| matches!(c, SwarmCommand::Dial { .. }))
            .collect();
        assert_eq!(dials.len(), 8);
        assert_eq!(mgr.table.dialing_count(), 8);

        // Further ticks while stalled still ≤ 8 concurrent.
        mgr.on_tick(now + Duration::from_secs(1)).await;
        let more = drain(&mut cap);
        assert!(
            !more.iter().any(|c| matches!(c, SwarmCommand::Dial { .. })),
            "no additional dials while 8 in flight"
        );
        assert_eq!(mgr.table.dialing_count(), 8);
    }

    #[tokio::test]
    async fn dial_backoff_fourth_failure_and_reset() {
        let id = pid();
        let cfg = PeerManagerConfig {
            target_peers: 50,
            max_peers: 100,
            static_peers: vec![StaticPeer {
                peer_id: id,
                addr: "/ip4/127.0.0.1/tcp/9".parse().unwrap(),
            }],
            max_concurrent_dials: 8,
            ..PeerManagerConfig::default()
        };
        let (mut mgr, mut cap, _cmd_rx) = manager(cfg);
        let t0 = Instant::now();

        for i in 0..4 {
            mgr.table
                .insert_dialing(id, "/ip4/127.0.0.1/tcp/9".parse().unwrap());
            mgr.handle_conn_event(
                ConnEvent::DialFailure {
                    peer_id: Some(id),
                    error: "timeout".into(),
                },
                t0 + Duration::from_secs(i * 1000),
            )
            .await;
        }
        let rec = mgr.table.get(&id).unwrap();
        assert_eq!(rec.dial_backoff.failures, 4);
        assert_eq!(
            rec.dial_backoff.last_delay,
            Duration::from_secs(240),
            "fourth failure delay"
        );

        // Success resets.
        mgr.handle_conn_event(
            ConnEvent::ConnectionEstablished {
                peer_id: id,
                direction: ConnectionDirection::Outbound,
                endpoint: "/ip4/127.0.0.1/tcp/9".parse().unwrap(),
            },
            t0 + Duration::from_secs(10_000),
        )
        .await;
        let rec = mgr.table.get(&id).unwrap();
        assert_eq!(rec.dial_backoff.failures, 0);
        let _ = drain(&mut cap);
    }

    #[test]
    fn eviction_order_app_score_custody_age_mesh_protect() {
        let mut table = PeerTable::default();
        let now = Instant::now();

        let low = pid();
        let mid = pid();
        let high = pid();
        let mesh = pid();
        let young = pid();

        table.insert_connected(
            low,
            ConnectionDirection::Inbound,
            now - Duration::from_secs(100),
        );
        table.get_mut(&low).unwrap().app_score = -10.0;
        table.get_mut(&low).unwrap().custody_usefulness = 5;

        table.insert_connected(
            mid,
            ConnectionDirection::Inbound,
            now - Duration::from_secs(100),
        );
        table.get_mut(&mid).unwrap().app_score = 0.0;
        table.get_mut(&mid).unwrap().custody_usefulness = 0;

        table.insert_connected(
            high,
            ConnectionDirection::Outbound,
            now - Duration::from_secs(100),
        );
        table.get_mut(&high).unwrap().app_score = 10.0;

        // Mesh-protected with high score — must not be chosen while others exist.
        table.insert_connected(
            mesh,
            ConnectionDirection::Outbound,
            now - Duration::from_secs(50),
        );
        table.get_mut(&mesh).unwrap().app_score = -15.0; // worse score but protected
        table.get_mut(&mesh).unwrap().in_mesh = true;
        table.get_mut(&mesh).unwrap().gossip_score = 0.0; // above GossipThreshold

        // Youngest with same score as mid — preferred over older mid when scores equal...
        // Actually low has worst score so low should win overall.
        table.insert_connected(young, ConnectionDirection::Outbound, now);
        table.get_mut(&young).unwrap().app_score = -10.0;
        table.get_mut(&young).unwrap().custody_usefulness = 5;

        // Lowest app_score is -15 mesh (protected) and -10 (low, young).
        // Between low and young (same score/custody), youngest first → young.
        let victim = select_eviction_victim(&table).unwrap();
        assert_eq!(victim, young, "youngest among lowest app_score");

        // Remove young; low next.
        table.on_disconnected(&young);
        assert_eq!(select_eviction_victim(&table), Some(low));

        // Mesh member not selected while non-members remain.
        table.on_disconnected(&low);
        table.on_disconnected(&mid);
        table.on_disconnected(&high);
        // Only mesh left among connected — protected, so None.
        assert_eq!(select_eviction_victim(&table), None);

        // Drop mesh below GossipThreshold → loses protection.
        table.get_mut(&mesh).unwrap().gossip_score = -5000.0;
        assert_eq!(select_eviction_victim(&table), Some(mesh));
    }

    #[test]
    fn eviction_prefers_lower_custody_on_score_tie() {
        let mut table = PeerTable::default();
        let now = Instant::now();
        let a = pid();
        let b = pid();
        table.insert_connected(
            a,
            ConnectionDirection::Outbound,
            now - Duration::from_secs(10),
        );
        table.insert_connected(
            b,
            ConnectionDirection::Outbound,
            now - Duration::from_secs(10),
        );
        table.get_mut(&a).unwrap().app_score = 1.0;
        table.get_mut(&b).unwrap().app_score = 1.0;
        table.get_mut(&a).unwrap().custody_usefulness = 0;
        table.get_mut(&b).unwrap().custody_usefulness = 3;
        assert_eq!(select_eviction_victim(&table), Some(a));
    }

    #[tokio::test]
    async fn goodbye_on_manual_evict_and_ban() {
        let id = pid();
        let (mut mgr, mut cap, _cmd_rx) = manager(PeerManagerConfig::default());
        let now = Instant::now();
        mgr.table
            .insert_connected(id, ConnectionDirection::Inbound, now);

        // Manual disconnect.
        mgr.disconnect(id, GoodbyeReason::ClientShutdown).await;
        let cmds = drain(&mut cap);
        assert!(
            matches!(
                cmds.as_slice(),
                [
                    SwarmCommand::Goodbye {
                        reason: GoodbyeReason::ClientShutdown,
                        ..
                    },
                    SwarmCommand::Disconnect { .. }
                ]
            ),
            "manual: {cmds:?}"
        );

        // Ban path.
        let id2 = pid();
        mgr.table
            .insert_connected(id2, ConnectionDirection::Outbound, now);
        mgr.ban_peer(id2, GoodbyeReason::FaultOrError).await;
        let cmds = drain(&mut cap);
        assert!(
            cmds.iter().any(|c| matches!(
                c,
                SwarmCommand::Goodbye {
                    reason: GoodbyeReason::FaultOrError,
                    ..
                }
            )),
            "ban goodbye: {cmds:?}"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SwarmCommand::Disconnect { .. })),
            "ban disconnect: {cmds:?}"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SwarmCommand::BlockPeer { .. })),
            "ban block: {cmds:?}"
        );
        assert!(mgr.bans.is_banned(&id2));

        // Eviction path.
        let id3 = pid();
        mgr.table
            .insert_connected(id3, ConnectionDirection::Outbound, now);
        let victim = select_eviction_victim(&mgr.table).unwrap();
        mgr.emit_disconnect(victim, GoodbyeReason::TooManyPeers, false)
            .await;
        let cmds = drain(&mut cap);
        assert!(
            matches!(
                &cmds[0],
                SwarmCommand::Goodbye {
                    reason: GoodbyeReason::TooManyPeers,
                    ..
                }
            ),
            "evict goodbye: {cmds:?}"
        );
    }

    #[tokio::test]
    async fn app_score_disconnect_and_ban_thresholds() {
        let id = pid();
        let (mut mgr, mut cap, _cmd_rx) = manager(PeerManagerConfig::default());
        let now = Instant::now();
        mgr.table
            .insert_connected(id, ConnectionDirection::Inbound, now);

        // -20 exactly does not disconnect (strictly below).
        mgr.set_app_score(id, -20.0, now).await;
        let cmds = drain(&mut cap);
        assert!(
            !cmds
                .iter()
                .any(|c| matches!(c, SwarmCommand::Disconnect { .. })),
            "at -20 must stay: {cmds:?}"
        );

        // -20.1 disconnects, no ban.
        mgr.set_app_score(id, -20.1, now).await;
        let cmds = drain(&mut cap);
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SwarmCommand::Disconnect { .. }))
        );
        assert!(
            !cmds
                .iter()
                .any(|c| matches!(c, SwarmCommand::BlockPeer { .. }))
        );
        assert!(!mgr.bans.is_banned(&id));

        // Reconnect and ban at < -50.
        mgr.table
            .insert_connected(id, ConnectionDirection::Inbound, now);
        let _ = drain(&mut cap);
        mgr.set_app_score(id, -50.1, now).await;
        let cmds = drain(&mut cap);
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SwarmCommand::BlockPeer { .. })),
            "ban at <-50: {cmds:?}"
        );
        assert!(mgr.bans.is_banned(&id));
    }

    #[tokio::test]
    async fn no_disconnect_on_gossip_score() {
        let id = pid();
        let (mut mgr, mut cap, _cmd_rx) = manager(PeerManagerConfig::default());
        let now = Instant::now();
        mgr.table
            .insert_connected(id, ConnectionDirection::Outbound, now);
        mgr.set_gossip_score(id, crate::gossip::scoring::GRAYLIST_THRESHOLD);
        mgr.on_tick(now).await;
        let cmds = drain(&mut cap);
        assert!(
            !cmds.iter().any(|c| matches!(
                c,
                SwarmCommand::Disconnect { .. } | SwarmCommand::Goodbye { .. }
            )),
            "gossip_score must not disconnect: {cmds:?}"
        );
        // Coupling applied once: 0 + (−5) then ×0.98 → finite, still above disconnect.
        let app = mgr.table.get(&id).unwrap().app_score;
        assert!(app.is_finite() && app > APP_SCORE_DISCONNECT, "app={app}");
    }

    #[tokio::test]
    async fn nan_app_score_is_sanitized_not_poisoned() {
        let id = pid();
        let (mut mgr, mut cap, _cmd_rx) = manager(PeerManagerConfig::default());
        let now = Instant::now();
        mgr.table
            .insert_connected(id, ConnectionDirection::Inbound, now);
        // NaN must not freeze disconnect forever (H2).
        mgr.set_app_score(id, f64::NAN, now).await;
        let app = mgr.table.get(&id).unwrap().app_score;
        assert_eq!(app, 0.0, "NaN sanitized to 0");
        let cmds = drain(&mut cap);
        assert!(
            !cmds
                .iter()
                .any(|c| matches!(c, SwarmCommand::Disconnect { .. })),
            "sanitized 0 must not disconnect: {cmds:?}"
        );

        mgr.set_gossip_score(id, f64::NAN);
        assert_eq!(mgr.table.get(&id).unwrap().gossip_score, 0.0);
    }

    #[tokio::test]
    async fn score_tick_couples_and_decays_once_per_slot() {
        let id = pid();
        let (mut mgr, mut cap, _cmd_rx) = manager(PeerManagerConfig::default());
        let now = Instant::now();
        mgr.table
            .insert_connected(id, ConnectionDirection::Outbound, now);
        mgr.set_gossip_score(id, -2500.0); // coupling −2.5
        mgr.set_app_score(id, 10.0, now).await;
        let _ = drain(&mut cap);

        mgr.on_tick(now).await;
        let app1 = mgr.table.get(&id).unwrap().app_score;
        // 10 + (−2.5) = 7.5; ×0.98 = 7.35
        assert!((app1 - 7.35).abs() < 1e-9, "app1={app1}");

        // Second tick inside the same slot: no extra couple/decay.
        mgr.on_tick(now + Duration::from_secs(1)).await;
        let app2 = mgr.table.get(&id).unwrap().app_score;
        assert!((app2 - app1).abs() < 1e-12, "app2={app2} app1={app1}");

        // After SCORE_DECAY_INTERVAL, another step.
        mgr.on_tick(now + SCORE_DECAY_INTERVAL).await;
        let app3 = mgr.table.get(&id).unwrap().app_score;
        // 7.35 + (−2.5) = 4.85; ×0.98 = 4.753
        assert!((app3 - 4.753).abs() < 1e-9, "app3={app3}");
    }

    #[tokio::test]
    async fn peers_metric_direction_labels_track_connect_disconnect() {
        let (mut mgr, _cap, _cmd_rx) = manager(PeerManagerConfig::default());
        let now = Instant::now();
        let mut inbound = 0i64;
        let mut outbound = 0i64;

        // 50-event sequence alternating in/out and closes.
        let mut ids = Vec::new();
        for i in 0..25 {
            let id = pid();
            ids.push(id);
            let dir = if i % 2 == 0 {
                inbound += 1;
                ConnectionDirection::Inbound
            } else {
                outbound += 1;
                ConnectionDirection::Outbound
            };
            mgr.handle_conn_event(
                ConnEvent::ConnectionEstablished {
                    peer_id: id,
                    direction: dir,
                    endpoint: Multiaddr::empty(),
                },
                now,
            )
            .await;
        }
        assert_eq!(mgr.metrics.peers(MetricDirection::Inbound), inbound);
        assert_eq!(mgr.metrics.peers(MetricDirection::Outbound), outbound);
        assert_eq!(
            mgr.metrics.peers(MetricDirection::Inbound)
                + mgr.metrics.peers(MetricDirection::Outbound),
            mgr.table.connected_count() as i64
        );

        // 25 disconnects.
        for id in ids.iter().take(25) {
            mgr.handle_conn_event(ConnEvent::ConnectionClosed { peer_id: *id }, now)
                .await;
        }
        assert_eq!(mgr.metrics.peers(MetricDirection::Inbound), 0);
        assert_eq!(mgr.metrics.peers(MetricDirection::Outbound), 0);
        assert_eq!(mgr.table.connected_count(), 0);
    }

    /// H2: policy close uses a single `ClosePeer` on the cmd channel (no partial drop).
    #[tokio::test]
    async fn policy_close_is_atomic_on_cmd_channel() {
        let (mut mgr, mut cap, mut cmd_rx) = manager(PeerManagerConfig::default());
        let id = pid();
        mgr.table
            .insert_connected(id, ConnectionDirection::Inbound, Instant::now());
        mgr.ban_peer(id, GoodbyeReason::FaultOrError).await;

        // Capture expands to Goodbye+Disconnect+Block for harness.
        let captured = drain(&mut cap);
        assert_eq!(captured.len(), 3);

        // Live cmd channel receives exactly one atomic ClosePeer { ban: true }.
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(
            matches!(
                cmd,
                SwarmCommand::ClosePeer {
                    ban: true,
                    reason: GoodbyeReason::FaultOrError,
                    ..
                }
            ),
            "got {cmd:?}"
        );
        assert!(cmd_rx.try_recv().is_err(), "no second policy cmd");
    }
}
