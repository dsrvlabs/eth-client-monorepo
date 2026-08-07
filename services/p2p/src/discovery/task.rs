//! Discovery task — discv5 handle, query scheduler, ENR manager, dial queue.
//!
//! Architecture §6.1 / §6.3, CC-21c. **Never** touches the swarm: dial
//! candidates are pushed to the peer manager via [`DiscoveredPeer`] messages.
//!
//! # Bootnode trust (M3 residual)
//!
//! Configured bootnode ENRs are a **root of trust** analogous to genesis: their
//! signatures are verified by the `enr` crate on parse, but there is no further
//! multi-source majority or table-level `eth2` filter at DHT seed time. A
//! wrong-network or replaced bootnode string in config can seed the routing
//! table and influence FINDNODE walks even when dial filtering rejects the peer.
//! Operators must pin bootnodes from a trusted source (V-4 Hoodi list /
//! `devnet/out/bootnodes.txt`). Digest filtering applies on the **dial** path
//! only (`enqueue_discovered`).

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use cc_libp2p::reexport::{Multiaddr, Protocol, identity};
use cc_libp2p::{Multiaddr as Libp2pMultiaddr, PeerId};
use cc_types::ForkDigest;
use discv5::enr::{CombinedKey, NodeId};
use discv5::{Discv5, Enr, Event};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::discovery::dial_queue::{DIAL_QUEUE_BOUND, DialCandidate, DialQueue};
use crate::discovery::enr::{
    ATTNETS_BIT_LEN, EnrApplyError, EnrManager, EnrSeqStrategy, SYNCNETS_BIT_LEN, attnets_has,
    enr_secp256k1_pubkey_bytes, parse_bootnodes, read_attnets, read_cgc, read_eth2, read_nfd,
    read_syncnets, syncnets_has,
};
use crate::discovery::predicate::{
    attestation_subnet_predicate, generic_peer_predicate,
};
use crate::fork_digest::ForkContext;
use crate::metrics::P2pMetrics;
use crate::peer_manager::PeerEnrInfo;

/// Query every 5 s while below target peers.
pub const QUERY_INTERVAL_BELOW_TARGET: Duration = Duration::from_secs(5);
/// Query every 30 s once at/above target.
pub const QUERY_INTERVAL_AT_TARGET: Duration = Duration::from_secs(30);
/// Default peers requested per `find_node_predicate`.
pub const QUERY_TARGET_PEER_NO: usize = 16;
/// Default min peers per subnet before issuing a targeted query.
pub const DEFAULT_MIN_PEERS_PER_SUBNET: usize = 3;
/// Subnet-targeted query cool-down (§6.3: at most one query per subnet / 30 s).
pub const SUBNET_QUERY_COOLDOWN: Duration = Duration::from_secs(30);
/// Base dial priority for a digest-matching peer with no deficit coverage.
pub const PRIORITY_BASE: u32 = 1;

/// A peer the discovery pipeline wants the peer manager to dial.
///
/// Bound on the discovery → peer-manager channel is [`DIAL_QUEUE_BOUND`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPeer {
    /// libp2p peer id.
    pub peer_id: PeerId,
    /// TCP multiaddr.
    pub addr: Multiaddr,
    /// Priority hint (subnet/column coverage of our deficits).
    pub priority: u32,
    /// ENR snapshot from the same verified record used for PeerId/addr (M2).
    pub enr_info: Option<PeerEnrInfo>,
}

/// Discovery configuration (bootnodes + listen + query knobs).
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// UDP listen IP (discv5).
    pub listen_ip: Ipv4Addr,
    /// UDP listen port (discv5).
    pub listen_udp: u16,
    /// TCP port advertised in the local ENR (libp2p).
    pub tcp_port: u16,
    /// Bootnode ENR strings (`enr:…` or bare base64).
    ///
    /// **Trust root:** see module docs. Signatures verified on parse; no digest
    /// gate at DHT insert.
    pub boot_nodes: Vec<String>,
    /// Optional path to a bootnodes file (self-devnet: `devnet/out/bootnodes.txt`).
    pub boot_nodes_file: Option<PathBuf>,
    /// Target peer count (from peer manager) for query cadence.
    pub target_peers: usize,
    /// Min peers per subnet for targeted queries / deficit scoring.
    pub min_peers_per_subnet: usize,
    /// Attestation subnets we want peers for (bitmask). `0` = no subnet-targeted
    /// queries until CC-2C sets the backbone; dial priority still scores sparse
    /// global attnet coverage when counts are known.
    pub interested_attnets: u64,
    /// ENR seq strategy.
    pub enr_strategy: EnrSeqStrategy,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            listen_ip: Ipv4Addr::UNSPECIFIED,
            listen_udp: 9000,
            tcp_port: 9000,
            boot_nodes: Vec::new(),
            boot_nodes_file: None,
            target_peers: crate::peer_manager::DEFAULT_TARGET_PEERS,
            min_peers_per_subnet: DEFAULT_MIN_PEERS_PER_SUBNET,
            interested_attnets: 0,
            enr_strategy: EnrSeqStrategy::EnrInsert,
        }
    }
}

/// Snapshot the peer manager exposes for query cadence / filter decisions.
#[derive(Debug, Clone)]
pub struct DiscoveryPeerView {
    /// Connected peer count.
    pub connected: usize,
    /// Peer ids currently connected or dialing (skip re-dial).
    pub active: Vec<PeerId>,
    /// Connected peers advertising each attestation subnet bit (from ENR).
    pub attnet_peer_counts: [u16; ATTNETS_BIT_LEN],
    /// Connected peers advertising each sync subnet bit (from ENR).
    pub syncnet_peer_counts: [u16; SYNCNETS_BIT_LEN],
}

impl Default for DiscoveryPeerView {
    fn default() -> Self {
        Self {
            connected: 0,
            active: Vec::new(),
            attnet_peer_counts: [0; ATTNETS_BIT_LEN],
            syncnet_peer_counts: [0; SYNCNETS_BIT_LEN],
        }
    }
}

/// Bridge ENR secp256k1 pubkey → libp2p [`PeerId`] (byte-level; dual identity).
///
/// discv5 0.11 pins `libp2p-identity` 0.2 while `cc-libp2p` uses 0.3 — types do
/// not unify; we re-decode compressed secp256k1 bytes (docs/p2p-dependencies.md).
#[must_use]
pub fn peer_id_from_enr(enr: &Enr) -> Option<PeerId> {
    let bytes = enr_secp256k1_pubkey_bytes(enr)?;
    let pk = identity::secp256k1::PublicKey::try_from_bytes(&bytes).ok()?;
    Some(PeerId::from_public_key(&identity::PublicKey::from(pk)))
}

/// TCP multiaddr from ENR ip/tcp fields (v4 preferred, then v6).
#[must_use]
pub fn multiaddr_from_enr(enr: &Enr) -> Option<Libp2pMultiaddr> {
    if let (Some(ip), Some(tcp)) = (enr.ip4(), enr.tcp4()) {
        let mut addr = Multiaddr::empty();
        addr.push(Protocol::Ip4(ip));
        addr.push(Protocol::Tcp(tcp));
        return Some(addr);
    }
    if let (Some(ip), Some(tcp)) = (enr.ip6(), enr.tcp6()) {
        let mut addr = Multiaddr::empty();
        addr.push(Protocol::Ip6(ip));
        addr.push(Protocol::Tcp(tcp));
        return Some(addr);
    }
    None
}

/// Decode [`PeerEnrInfo`] from a signature-verified ENR (same record as dial).
#[must_use]
pub fn peer_enr_info_from_enr(enr: &Enr) -> PeerEnrInfo {
    let fork_digest = read_eth2(enr).map(|id| {
        let s = id.fork_digest.as_slice();
        [s[0], s[1], s[2], s[3]]
    });
    let nfd = read_nfd(enr).map(|d| {
        let s = d.as_slice();
        [s[0], s[1], s[2], s[3]]
    });
    PeerEnrInfo {
        fork_digest,
        nfd,
        cgc: read_cgc(enr),
        attnets: read_attnets(enr),
        syncnets: read_syncnets(enr),
    }
}

/// Dial priority from ENR coverage of current subnet deficits (§6.3).
///
/// `PRIORITY_BASE` plus one per deficit attnet/syncnet the ENR claims, so peers
/// covering sparse subnets sort ahead of generic matches.
#[must_use]
pub fn dial_priority_for_enr(
    enr: &Enr,
    view: &DiscoveryPeerView,
    min_peers_per_subnet: usize,
    interested_attnets: u64,
) -> u32 {
    let min = min_peers_per_subnet as u16;
    let mut p = PRIORITY_BASE;

    // Score coverage of interested deficits first; if none configured, score
    // any sparse attnet the peer advertises (network-fill heuristic).
    for s in 0..ATTNETS_BIT_LEN {
        let bit = 1u64 << s;
        let interested = interested_attnets == 0 || (interested_attnets & bit) != 0;
        if !interested {
            continue;
        }
        if view.attnet_peer_counts[s] < min && attnets_has(enr, s as u8) {
            p = p.saturating_add(1);
        }
    }
    for s in 0..SYNCNETS_BIT_LEN {
        if view.syncnet_peer_counts[s] < min && syncnets_has(enr, s as u8) {
            p = p.saturating_add(1);
        }
    }
    // Mild boost for non-default cgc (custody coverage signal until CC-24a).
    if read_cgc(enr).is_some_and(|c| c > cc_types::CUSTODY_REQUIREMENT) {
        p = p.saturating_add(1);
    }
    p
}

/// Attestation subnets that are below `min_peers_per_subnet` among those we
/// care about (`interested_attnets`; when 0, no subnet is deficit-tracked for
/// **queries** — only generic FINDNODE runs).
#[must_use]
pub fn deficit_attnets(
    view: &DiscoveryPeerView,
    min_peers_per_subnet: usize,
    interested_attnets: u64,
) -> Vec<u8> {
    if interested_attnets == 0 {
        return Vec::new();
    }
    let min = min_peers_per_subnet as u16;
    let mut out = Vec::new();
    for s in 0..ATTNETS_BIT_LEN {
        if (interested_attnets & (1u64 << s)) != 0 && view.attnet_peer_counts[s] < min {
            out.push(s as u8);
        }
    }
    out
}

/// Load bootnode ENRs from config strings + optional file.
pub fn load_bootnodes(cfg: &DiscoveryConfig) -> Result<Vec<Enr>, EnrApplyError> {
    let mut lines: Vec<String> = cfg.boot_nodes.clone();
    if let Some(path) = &cfg.boot_nodes_file {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                for line in text.lines() {
                    lines.push(line.to_owned());
                }
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "boot_nodes_file unreadable");
            }
        }
    }
    parse_bootnodes(lines)
}

/// Build [`EnrManager`] for the discovery task (not started).
pub fn build_enr_manager(
    enr_key: CombinedKey,
    cfg: &DiscoveryConfig,
) -> Result<EnrManager, EnrApplyError> {
    EnrManager::from_key_and_listen(
        enr_key,
        cfg.listen_ip,
        cfg.listen_udp,
        cfg.tcp_port,
        cfg.enr_strategy,
    )
}

/// Filter + enqueue discovered ENRs into the dial queue.
///
/// Priority is computed per ENR from deficit coverage (not a constant).
/// Drops: missing digests outside `allowed`, missing TCP addr, peers already
/// active, and peers we cannot bridge to a PeerId.
pub fn enqueue_discovered(
    queue: &mut DialQueue,
    enrs: impl IntoIterator<Item = Enr>,
    allowed: &[ForkDigest],
    view: &DiscoveryPeerView,
    min_peers_per_subnet: usize,
    interested_attnets: u64,
) -> usize {
    let mut n = 0;
    for enr in enrs {
        let Some(digest) = read_eth2(&enr).map(|id| id.fork_digest) else {
            // No eth2: only allow when caller explicitly passes empty allowed
            // (tests). Production always passes `[current]`.
            if !allowed.is_empty() {
                continue;
            }
            if let Some(added) =
                try_enqueue_one(queue, &enr, view, min_peers_per_subnet, interested_attnets)
            {
                n += usize::from(added);
            }
            continue;
        };
        if !allowed.is_empty() && !allowed.contains(&digest) {
            continue;
        }
        if let Some(added) =
            try_enqueue_one(queue, &enr, view, min_peers_per_subnet, interested_attnets)
        {
            n += usize::from(added);
        }
    }
    n
}

fn try_enqueue_one(
    queue: &mut DialQueue,
    enr: &Enr,
    view: &DiscoveryPeerView,
    min_peers_per_subnet: usize,
    interested_attnets: u64,
) -> Option<bool> {
    let peer_id = peer_id_from_enr(enr)?;
    if view.active.contains(&peer_id) {
        return Some(false);
    }
    let addr = multiaddr_from_enr(enr)?;
    let priority =
        dial_priority_for_enr(enr, view, min_peers_per_subnet, interested_attnets);
    let enr_info = peer_enr_info_from_enr(enr);
    Some(queue.push(DialCandidate {
        peer_id,
        addr,
        priority,
        enr_info: Some(enr_info),
    }))
}

/// Drain the dial queue onto the peer-manager channel (non-blocking).
pub fn flush_dial_queue(
    queue: &mut DialQueue,
    dial_tx: &mpsc::Sender<DiscoveredPeer>,
    max: usize,
    _metrics: &P2pMetrics,
) -> usize {
    let batch = queue.drain(max);
    let n = batch.len();
    for c in batch {
        let msg = DiscoveredPeer {
            peer_id: c.peer_id,
            addr: c.addr,
            priority: c.priority,
            enr_info: c.enr_info,
        };
        match dial_tx.try_send(msg) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("discovery→peer_manager dial channel full; dropping candidate");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("discovery dial channel closed");
                break;
            }
        }
    }
    let _ = queue.len();
    n
}

/// Run one generic predicate query against a started [`Discv5`] handle.
pub async fn run_generic_query(
    discv5: &Discv5,
    allowed: Vec<ForkDigest>,
) -> Result<Vec<Enr>, discv5::QueryError> {
    let target = NodeId::random();
    let predicate = generic_peer_predicate(allowed);
    discv5
        .find_node_predicate(target, predicate, QUERY_TARGET_PEER_NO)
        .await
}

/// Run one attestation-subnet-targeted predicate query.
pub async fn run_attnet_query(
    discv5: &Discv5,
    allowed: Vec<ForkDigest>,
    subnet: u8,
) -> Result<Vec<Enr>, discv5::QueryError> {
    let target = NodeId::random();
    let predicate = attestation_subnet_predicate(allowed, subnet);
    discv5
        .find_node_predicate(target, predicate, QUERY_TARGET_PEER_NO)
        .await
}

/// Inputs owned by the discovery driver after spawn.
#[allow(missing_debug_implementations)] // holds Discv5 / channel types without Debug
pub struct DiscoveryTask {
    /// ENR manager / discv5 handle.
    pub manager: EnrManager,
    /// Discovery knobs + bootnodes.
    pub cfg: DiscoveryConfig,
    /// Epoch-aware fork digests for ENR `eth2`/`nfd`.
    pub fork_ctx: ForkContext,
    /// Epoch ticks from the slot clock.
    pub epoch_rx: tokio::sync::watch::Receiver<u64>,
    /// Connected/active peer snapshot from the peer manager.
    pub peer_view_rx: tokio::sync::watch::Receiver<DiscoveryPeerView>,
    /// Dial candidates → peer manager.
    pub dial_tx: mpsc::Sender<DiscoveredPeer>,
    /// Metrics (queue depths / panics live elsewhere).
    pub metrics: P2pMetrics,
    /// Shutdown signal.
    pub shutdown: tokio::sync::watch::Receiver<bool>,
}

/// Discovery driver loop.
///
/// Owns the [`EnrManager`] / discv5 handle, schedules queries, and flushes the
/// dial queue to `dial_tx`. Epoch ticks update `eth2`/`nfd` via `epoch_rx`.
///
/// The query interval is a **stable** timer: it is only rebuilt when the
/// below-target / at-target cadence actually flips (B1).
pub async fn run_discovery_task(task: DiscoveryTask) {
    let DiscoveryTask {
        mut manager,
        cfg,
        mut fork_ctx,
        mut epoch_rx,
        mut peer_view_rx,
        dial_tx,
        metrics,
        mut shutdown,
    } = task;

    // Seed ENR fields (phase-2 defaults + fork view).
    if let Err(e) = manager.apply_phase2_defaults() {
        warn!(error = %e, "failed to apply phase-2 ENR defaults");
    }
    if let Err(e) = manager.apply_fork_context(&fork_ctx) {
        warn!(error = %e, "failed to apply fork ENR fields");
    }

    match load_bootnodes(&cfg) {
        Ok(boot) => {
            let n = manager.add_bootnodes(&boot);
            info!(added = n, total = boot.len(), "bootnodes loaded into discv5");
        }
        Err(e) => warn!(error = %e, "bootnode load failed"),
    }

    if let Err(e) = manager.discv5_mut().start().await {
        // Fatal for this task instance — supervisor restart policy applies.
        warn!(error = %e, "discv5 start failed");
        return;
    }
    info!(enr = %manager.local_enr(), "discv5 started");

    let mut event_stream = match manager.discv5_mut().event_stream().await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "discv5 event_stream failed");
            return;
        }
    };

    let mut dial_queue = DialQueue::new();
    // Stable query timer (B1): start at below-target cadence; rebuild only on flip.
    let mut at_target = false;
    let mut query_interval = tokio::time::interval(QUERY_INTERVAL_BELOW_TARGET);
    query_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so we do not query before bootnode flush.
    query_interval.tick().await;

    // Last subnet-targeted query time per subnet index.
    let mut subnet_last_query: HashMap<u8, Instant> = HashMap::new();

    // Initial bootnode dials (contactable TCP + digest filter).
    if let Ok(boot) = load_bootnodes(&cfg) {
        let view = peer_view_rx.borrow().clone();
        let allowed = allowed_digests(&fork_ctx);
        enqueue_discovered(
            &mut dial_queue,
            boot,
            &allowed,
            &view,
            cfg.min_peers_per_subnet,
            cfg.interested_attnets,
        );
        flush_dial_queue(&mut dial_queue, &dial_tx, DIAL_QUEUE_BOUND, &metrics);
    }

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    manager.discv5_mut().shutdown();
                    info!("discovery task shutdown");
                    break;
                }
            }
            _ = epoch_rx.changed() => {
                let epoch = *epoch_rx.borrow();
                fork_ctx.on_epoch(cc_types::Epoch::new(epoch));
                if let Err(e) = manager.apply_fork_context(&fork_ctx) {
                    warn!(error = %e, epoch, "ENR fork apply failed");
                } else {
                    debug!(epoch, digest = ?fork_ctx.current_digest(), "ENR eth2/nfd updated");
                }
            }
            _ = peer_view_rx.changed() => {
                // Cadence flip is checked on the query tick (stable timer).
            }
            event = event_stream.recv() => {
                match event {
                    Some(Event::Discovered(enr)) | Some(Event::SessionEstablished(enr, _)) => {
                        let allowed = allowed_digests(&fork_ctx);
                        let view = peer_view_rx.borrow().clone();
                        enqueue_discovered(
                            &mut dial_queue,
                            std::iter::once(enr),
                            &allowed,
                            &view,
                            cfg.min_peers_per_subnet,
                            cfg.interested_attnets,
                        );
                        flush_dial_queue(&mut dial_queue, &dial_tx, 32, &metrics);
                    }
                    Some(_) => {}
                    None => {
                        warn!("discv5 event stream closed");
                        break;
                    }
                }
            }
            _ = query_interval.tick() => {
                let view = peer_view_rx.borrow().clone();
                let now_at_target = view.connected >= cfg.target_peers;
                if now_at_target != at_target {
                    at_target = now_at_target;
                    let cadence = if at_target {
                        QUERY_INTERVAL_AT_TARGET
                    } else {
                        QUERY_INTERVAL_BELOW_TARGET
                    };
                    query_interval = tokio::time::interval(cadence);
                    query_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    // Consume the immediate tick from the new interval so the
                    // next real fire is one full period away.
                    query_interval.tick().await;
                }

                let allowed = allowed_digests(&fork_ctx);
                // Generic random-target query (§6.3).
                match run_generic_query(manager.discv5(), allowed.clone()).await {
                    Ok(enrs) => {
                        debug!(found = enrs.len(), "find_node_predicate (generic) completed");
                        enqueue_discovered(
                            &mut dial_queue,
                            enrs,
                            &allowed,
                            &view,
                            cfg.min_peers_per_subnet,
                            cfg.interested_attnets,
                        );
                    }
                    Err(e) => {
                        debug!(error = %e, "find_node_predicate (generic) error");
                    }
                }

                // Subnet-targeted queries for deficits (§6.3 / B3).
                let now = Instant::now();
                let deficits = deficit_attnets(
                    &view,
                    cfg.min_peers_per_subnet,
                    cfg.interested_attnets,
                );
                for subnet in deficits {
                    let cooled = subnet_last_query
                        .get(&subnet)
                        .is_none_or(|t| now.duration_since(*t) >= SUBNET_QUERY_COOLDOWN);
                    if !cooled {
                        continue;
                    }
                    match run_attnet_query(manager.discv5(), allowed.clone(), subnet).await {
                        Ok(enrs) => {
                            debug!(
                                subnet,
                                found = enrs.len(),
                                "find_node_predicate (attnet) completed"
                            );
                            enqueue_discovered(
                                &mut dial_queue,
                                enrs,
                                &allowed,
                                &view,
                                cfg.min_peers_per_subnet,
                                cfg.interested_attnets,
                            );
                            subnet_last_query.insert(subnet, Instant::now());
                        }
                        Err(e) => {
                            debug!(subnet, error = %e, "find_node_predicate (attnet) error");
                            // Still mark cooldown to avoid storming a failing query.
                            subnet_last_query.insert(subnet, Instant::now());
                        }
                    }
                }

                flush_dial_queue(&mut dial_queue, &dial_tx, 64, &metrics);
            }
        }
    }
}

/// Digests accepted by discv5 predicates (CC-2A Overlap-aware).
///
/// Steady: `{current}`. Overlap window (boundary − 1 … boundary):
/// `{current, next}` / `{previous, current}` via schedule lookahead.
fn allowed_digests(ctx: &ForkContext) -> Vec<ForkDigest> {
    ctx.discovery_allowed_digests()
}

/// Parse a multiaddr string without panicking.
pub fn parse_multiaddr(s: &str) -> Result<Multiaddr, String> {
    Multiaddr::from_str(s).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::discovery::enr::{
        ENR_KEY_ATTNETS, ENR_KEY_ETH2, EnrManager, EnrSeqStrategy, encode_attnets, encode_eth2,
    };
    use crate::fork_digest::EnrForkId;
    use cc_types::{Epoch, ForkVersion};
    use discv5::enr::CombinedKey;
    use std::net::Ipv4Addr;

    fn enr_with_digest_and_attnets(digest: [u8; 4], attnets: u64) -> Enr {
        let key = CombinedKey::generate_secp256k1();
        let mut enr = discv5::enr::Enr::builder()
            .ip4(Ipv4Addr::LOCALHOST)
            .tcp4(9000)
            .udp4(9000)
            .build(&key)
            .unwrap();
        let eth2 = EnrForkId {
            fork_digest: ForkDigest::from_array(digest),
            next_fork_version: ForkVersion::ZERO,
            next_fork_epoch: Epoch::new(u64::MAX),
        };
        enr.insert(ENR_KEY_ETH2, &encode_eth2(eth2).as_slice(), &key)
            .unwrap();
        enr.insert(ENR_KEY_ATTNETS, &encode_attnets(attnets).as_slice(), &key)
            .unwrap();
        enr
    }

    #[test]
    fn peer_id_and_multiaddr_from_constructed_enr() {
        let key = CombinedKey::generate_secp256k1();
        let enr = discv5::enr::Enr::builder()
            .ip4(Ipv4Addr::new(1, 2, 3, 4))
            .tcp4(9000)
            .udp4(9001)
            .build(&key)
            .unwrap();
        let pid = peer_id_from_enr(&enr).expect("peer id");
        let addr = multiaddr_from_enr(&enr).expect("addr");
        assert!(addr.to_string().contains("1.2.3.4"));
        assert!(addr.to_string().contains("9000"));
        let pid2 = peer_id_from_enr(&enr).unwrap();
        assert_eq!(pid, pid2);
    }

    #[test]
    fn enqueue_filters_wrong_digest() {
        let enr = enr_with_digest_and_attnets([1, 1, 1, 1], 0);
        let mut q = DialQueue::new();
        let view = DiscoveryPeerView::default();
        let n = enqueue_discovered(
            &mut q,
            vec![enr.clone()],
            &[ForkDigest::from_array([2, 2, 2, 2])],
            &view,
            3,
            0,
        );
        assert_eq!(n, 0);
        let n = enqueue_discovered(
            &mut q,
            vec![enr],
            &[ForkDigest::from_array([1, 1, 1, 1])],
            &view,
            3,
            0,
        );
        assert_eq!(n, 1);
        assert_eq!(q.len(), 1);
        // enr_info is attached.
        let c = q.pop().unwrap();
        assert!(c.enr_info.is_some());
        assert_eq!(c.enr_info.unwrap().fork_digest, Some([1, 1, 1, 1]));
    }

    #[test]
    fn dial_priority_higher_when_covering_deficit_subnet() {
        let mut view = DiscoveryPeerView::default();
        // Subnet 3 is sparse (0 peers); subnet 5 is full.
        view.attnet_peer_counts[3] = 0;
        view.attnet_peer_counts[5] = 10;
        let min = 3;
        let interested = (1u64 << 3) | (1u64 << 5);

        let covers_deficit = enr_with_digest_and_attnets([1, 1, 1, 1], 1u64 << 3);
        let covers_full = enr_with_digest_and_attnets([1, 1, 1, 1], 1u64 << 5);
        let covers_none = enr_with_digest_and_attnets([1, 1, 1, 1], 0);

        let p_def = dial_priority_for_enr(&covers_deficit, &view, min, interested);
        let p_full = dial_priority_for_enr(&covers_full, &view, min, interested);
        let p_none = dial_priority_for_enr(&covers_none, &view, min, interested);

        assert!(p_def > p_full, "deficit coverage must outrank full subnet");
        assert!(p_def > p_none);
        assert_eq!(p_full, PRIORITY_BASE); // subnet 5 not a deficit
        assert_eq!(p_none, PRIORITY_BASE);
    }

    #[test]
    fn deficit_attnets_respects_interested_mask() {
        let mut view = DiscoveryPeerView::default();
        view.attnet_peer_counts[1] = 0;
        view.attnet_peer_counts[2] = 0;
        // interested only bit 1
        let d = deficit_attnets(&view, 3, 1u64 << 1);
        assert_eq!(d, vec![1]);
        // interested none → no query deficits
        assert!(deficit_attnets(&view, 3, 0).is_empty());
    }

    #[test]
    fn peer_enr_info_roundtrip_fields() {
        let enr = enr_with_digest_and_attnets([0xaa, 0xbb, 0xcc, 0xdd], 1u64 << 7);
        let info = peer_enr_info_from_enr(&enr);
        assert_eq!(info.fork_digest, Some([0xaa, 0xbb, 0xcc, 0xdd]));
        assert_eq!(info.attnets, Some(1u64 << 7));
        let _ = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert);
    }
}
