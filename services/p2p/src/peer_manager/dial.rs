//! Dial scheduler: target / max / concurrent dial caps (Architecture §3.6).
//!
//! Policy lives here; hard ceilings are `connection_limits::Behaviour` on the
//! swarm. Dial sources: config **static peers** and table rows with known
//! multiaddrs (discovery-fed via CC-21c). Discovery never calls the swarm.

use std::collections::HashSet;
use std::time::Instant;

use cc_libp2p::{Multiaddr, PeerId};

use super::ban::BanList;
use super::{ConnectionState, PeerManagerConfig, PeerTable};

/// A dial the scheduler wants the swarm to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DialRequest {
    /// Target peer.
    pub peer_id: PeerId,
    /// Address to dial (static peer list / discovery).
    pub addr: Multiaddr,
}

/// Pure scheduler: given table + config + now, produce dial requests (≤ concurrent cap).
///
/// Does **not** dial when `connected >= max_peers`. Dials while
/// `connected < target_peers` and concurrent dials < `max_concurrent_dials`.
///
/// Preference order: static peers first, then any disconnected table peer that
/// has at least one multiaddr (discovery candidates).
#[must_use]
pub fn schedule_dials(
    table: &PeerTable,
    bans: &BanList,
    config: &PeerManagerConfig,
    now: Instant,
) -> Vec<DialRequest> {
    let connected = table.connected_count();
    if connected >= config.max_peers {
        return Vec::new();
    }
    if connected >= config.target_peers {
        return Vec::new();
    }

    let in_flight = table.dialing_count();
    let slots = config.max_concurrent_dials.saturating_sub(in_flight);
    if slots == 0 {
        return Vec::new();
    }

    // Room under max — also respect outbound soft intent (hard cap is swarm-level).
    let outbound = table.connected_outbound();
    let outbound_room = config.max_outbound.saturating_sub(outbound);
    if outbound_room == 0 {
        return Vec::new();
    }

    let want = (config.target_peers - connected)
        .min(slots)
        .min(outbound_room)
        .min(config.max_peers - connected);

    let mut out = Vec::with_capacity(want);
    let mut scheduled: HashSet<PeerId> = HashSet::new();

    for sp in &config.static_peers {
        if out.len() >= want {
            break;
        }
        if bans.is_banned(&sp.peer_id) {
            continue;
        }
        if let Some(rec) = table.get(&sp.peer_id) {
            match rec.state {
                ConnectionState::Connected | ConnectionState::Dialing => continue,
                ConnectionState::Disconnected => {
                    if !rec.dial_backoff.ready(now) {
                        continue;
                    }
                }
            }
        }
        // Not yet in the table, or disconnected + backoff ready.
        scheduled.insert(sp.peer_id);
        out.push(DialRequest {
            peer_id: sp.peer_id,
            addr: sp.addr.clone(),
        });
    }

    // Discovery-fed peers: disconnected table rows with a multiaddr.
    if out.len() < want {
        let mut discovered: Vec<DialRequest> = table
            .iter()
            .filter(|rec| {
                !scheduled.contains(&rec.peer_id)
                    && !bans.is_banned(&rec.peer_id)
                    && rec.state == ConnectionState::Disconnected
                    && rec.dial_backoff.ready(now)
                    && !rec.addrs.is_empty()
            })
            .map(|rec| DialRequest {
                peer_id: rec.peer_id,
                addr: rec.addrs[0].clone(),
            })
            .collect();
        // Prefer higher custody_usefulness (discovery priority is stored there
        // until a dedicated field lands).
        discovered.sort_by(|a, b| {
            let ca = table
                .get(&a.peer_id)
                .map(|r| r.custody_usefulness)
                .unwrap_or(0);
            let cb = table
                .get(&b.peer_id)
                .map(|r| r.custody_usefulness)
                .unwrap_or(0);
            cb.cmp(&ca).then_with(|| a.peer_id.cmp(&b.peer_id))
        });
        for d in discovered {
            if out.len() >= want {
                break;
            }
            out.push(d);
        }
    }
    out
}

/// Whether the scheduler would dial at all (used by past-max / below-target tests).
#[must_use]
pub fn would_dial(
    table: &PeerTable,
    bans: &BanList,
    config: &PeerManagerConfig,
    now: Instant,
) -> bool {
    !schedule_dials(table, bans, config, now).is_empty()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::peer_manager::{ConnectionDirection, PeerRecord, StaticPeer};
    use cc_libp2p::reexport::Keypair;

    fn peer() -> PeerId {
        PeerId::from_public_key(&Keypair::generate_secp256k1().public())
    }

    fn addr() -> Multiaddr {
        "/ip4/127.0.0.1/tcp/9000".parse().unwrap()
    }

    fn cfg(static_n: usize, target: usize, max: usize, concurrent: usize) -> PeerManagerConfig {
        let static_peers = (0..static_n)
            .map(|_| StaticPeer {
                peer_id: peer(),
                addr: addr(),
            })
            .collect();
        PeerManagerConfig {
            target_peers: target,
            max_peers: max,
            max_inbound: 60,
            max_outbound: 60,
            max_concurrent_dials: concurrent,
            static_peers,
            tick_interval: std::time::Duration::from_secs(1),
        }
    }

    #[test]
    fn stops_at_max_and_respects_concurrent() {
        let mut config = cfg(20, 10, 10, 3);
        let bans = BanList::default();
        let now = Instant::now();
        let mut table = PeerTable::default();

        // Fill to max with connected peers (not from static list).
        for _ in 0..10 {
            let id = peer();
            table.insert_connected(id, ConnectionDirection::Outbound, now);
        }
        assert!(schedule_dials(&table, &bans, &config, now).is_empty());

        // Drop below target.
        let ids: Vec<_> = table.connected_peer_ids().collect();
        for id in ids.iter().take(5) {
            table.on_disconnected(id);
        }
        assert_eq!(table.connected_count(), 5);

        let dials = schedule_dials(&table, &bans, &config, now);
        assert_eq!(dials.len(), 3, "concurrent dials capped at 3");
        assert!(dials.len() <= config.max_concurrent_dials);

        // Mark them dialing → no more room.
        for d in &dials {
            table.insert_dialing(d.peer_id, d.addr.clone());
        }
        assert!(schedule_dials(&table, &bans, &config, now).is_empty());

        // Prevent unused-mut warning if config later mutates.
        let _ = &mut config;
    }

    #[test]
    fn skips_banned_and_backoff() {
        let config = cfg(3, 50, 100, 8);
        let mut bans = BanList::default();
        let now = Instant::now();
        let mut table = PeerTable::default();

        let banned = config.static_peers[0].peer_id;
        bans.ban(banned);

        let delayed = config.static_peers[1].peer_id;
        let mut rec = PeerRecord::new(delayed);
        rec.dial_backoff.on_failure(now);
        table.upsert(rec);

        let dials = schedule_dials(&table, &bans, &config, now);
        assert_eq!(dials.len(), 1);
        assert_eq!(dials[0].peer_id, config.static_peers[2].peer_id);
    }
}
