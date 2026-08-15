//! CC-21c acceptance tests: cgc round-trip, ENRForkID BPO semantics, nfd
//! non-disconnect, predicates, dial queue bound, discovery restart budget.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use cc_libp2p::PeerId;
use cc_libp2p::reexport::Keypair;
use cc_p2p::discovery::{
    DIAL_QUEUE_BOUND, DialCandidate, DialQueue, DiscoveryPeerView, ENR_KEY_CGC, ENR_KEY_ETH2,
    EnrFieldChange, EnrManager, EnrSeqStrategy, PRIORITY_BASE, attestation_subnet_predicate,
    column_predicate, decode_cgc, dial_priority_for_enr, encode_attnets, encode_cgc, encode_eth2,
    encode_syncnets, enr_custody_groups, enr_field_payload, generic_peer_predicate, parse_bootnode,
    parse_bootnodes, read_cgc, read_eth2, read_nfd, sync_subnet_predicate,
};
use cc_p2p::fork_digest::{FAR_FUTURE_EPOCH, ForkContext, enr_fork_id, next_fork_digest};
use cc_p2p::metrics::P2pMetrics;
use cc_p2p::peer_manager::{
    ConnectionState, PeerEnrInfo, PeerManager, PeerManagerConfig, PeerRecord, PeerTable,
};
use cc_p2p::supervisor::{
    SupervisedTask, SupervisorOutcome, TaskPolicy, factory_from_future, run_supervisor,
};
use cc_types::{ChainConfig, Epoch, ForkDigest, ForkVersion, Root, parse_hex_bytes};
use prometheus_client::registry::Registry;
use tokio::sync::watch;

// ── Hoodi fixtures ──────────────────────────────────────────────────────────

const HOODI_GVR_HEX: &str = "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f";
const EPOCH_FULU_FALLBACK: u64 = 51_000;
const EPOCH_BPO1: u64 = 52_480;

fn hoodi_config() -> ChainConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
    ChainConfig::from_yaml_file(&path).unwrap_or_else(|e| panic!("hoodi-config.yaml: {e}"))
}

fn hoodi_gvr() -> Root {
    Root::from_array(parse_hex_bytes::<32>(HOODI_GVR_HEX).unwrap())
}

fn metrics() -> P2pMetrics {
    let mut reg = Registry::default();
    P2pMetrics::register(&mut reg)
}

// ── CC-21/5 cgc round-trip through a real ENR ───────────────────────────────

#[test]
fn cgc_roundtrip_through_real_enr_4_and_128() {
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
    for v in [4u64, 128, 0] {
        manager
            .apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(v))])
            .unwrap();
        let enr = manager.local_enr();
        assert!(enr.verify(), "ENR must verify after cgc={v}");
        let payload = enr_field_payload(&enr, ENR_KEY_CGC).expect("cgc payload");
        assert_eq!(decode_cgc(&payload), Some(v));
        assert_eq!(read_cgc(&enr), Some(v));
        // Same encoder/decoder both directions.
        assert_eq!(encode_cgc(v), payload);
        // Fixed-width 8-byte encoding must fail equality for small values.
        if v > 0 && v < 256 {
            assert_ne!(payload, v.to_be_bytes().to_vec());
        }
    }
}

// ── CC-21/3 ENRForkID across BPO / none scheduled ───────────────────────────

#[test]
fn enr_fork_id_bpo_version_unchanged_epoch_tracks() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    // Inside Fulu fallback: next is BPO1.
    let enr = enr_fork_id(&cfg, gvr, Epoch::new(EPOCH_FULU_FALLBACK));
    assert_eq!(enr.next_fork_epoch, Epoch::new(EPOCH_BPO1));
    // next_fork_version is regular-fork only — still Fulu across BPO.
    assert_eq!(enr.next_fork_version, cfg.fulu_fork_version);
    assert_eq!(
        enr.next_fork_version,
        cc_p2p::fork_digest::compute_fork_version(&cfg, Epoch::new(EPOCH_FULU_FALLBACK))
    );

    // Nothing scheduled past last BPO.
    let enr_none = enr_fork_id(&cfg, gvr, Epoch::new(100_000));
    assert_eq!(enr_none.next_fork_epoch, FAR_FUTURE_EPOCH);
    assert_eq!(enr_none.next_fork_version, cfg.fulu_fork_version);
    assert_eq!(
        next_fork_digest(&cfg, gvr, Epoch::new(100_000)),
        ForkDigest::ZERO
    );
}

#[test]
fn eth2_nfd_apply_coalesces_to_one_seq_bump() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let ctx = ForkContext::new(cfg, gvr, Epoch::new(EPOCH_FULU_FALLBACK));
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
    let seq_before = manager.local_enr().seq();
    manager.apply_fork_context(&ctx).unwrap();
    let enr = manager.local_enr();
    assert_eq!(enr.seq(), seq_before + 1);
    assert!(enr.verify());
    assert_eq!(read_eth2(&enr).unwrap().fork_digest, ctx.current_digest());
    assert_eq!(read_nfd(&enr).unwrap(), ctx.nfd());
}

// ── CC-21/4 nfd mismatch does not disconnect ───────────────────────────────

#[test]
fn nfd_mismatch_does_not_disconnect() {
    // Peer manager has no nfd-based disconnect path.
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel(64);
    let mut pm = PeerManager::new(PeerManagerConfig::default(), cmd_tx, metrics());
    let peer = PeerId::from_public_key(&Keypair::generate_secp256k1().public());
    pm.table.insert_connected(
        peer,
        cc_p2p::channels::ConnectionDirection::Outbound,
        Instant::now(),
    );
    // Store an nfd that differs from "ours".
    pm.offer_discovered(
        peer,
        "/ip4/127.0.0.1/tcp/9000".parse().unwrap(),
        1,
        Some(PeerEnrInfo {
            fork_digest: Some([1, 2, 3, 4]),
            nfd: Some([9, 9, 9, 9]),
            cgc: Some(4),
            attnets: None,
            syncnets: None,
        }),
    );
    // Tick / score enforcement must leave the peer connected.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        pm.on_tick(Instant::now()).await;
    });
    assert_eq!(
        pm.table.get(&peer).unwrap().state,
        ConnectionState::Connected
    );
    // No ClosePeer / Disconnect emitted solely due to nfd.
    while let Ok(cmd) = cmd_rx.try_recv() {
        match cmd {
            cc_p2p::channels::SwarmCommand::ClosePeer { .. }
            | cc_p2p::channels::SwarmCommand::Disconnect { .. }
            | cc_p2p::channels::SwarmCommand::Goodbye { .. } => {
                panic!("nfd must not trigger disconnect: {cmd:?}");
            }
            _ => {}
        }
    }
}

#[test]
fn peer_manager_source_has_no_nfd_disconnect() {
    // Static grep-equivalent: the peer_manager module must not contain an nfd
    // disconnect condition. We assert the symbol `nfd` only appears in
    // PeerEnrInfo docs / field storage, never as a disconnect predicate.
    let src = include_str!("../src/peer_manager/mod.rs");
    let ban = include_str!("../src/peer_manager/ban.rs");
    let dial = include_str!("../src/peer_manager/dial.rs");
    let score = include_str!("../src/peer_manager/score.rs");
    for (name, text) in [
        ("mod.rs", src),
        ("ban.rs", ban),
        ("dial.rs", dial),
        ("score.rs", score),
    ] {
        for line in text.lines() {
            let lower = line.to_ascii_lowercase();
            if lower.contains("nfd")
                && (lower.contains("disconnect")
                    || lower.contains("ban")
                    || lower.contains("goodbye")
                    || lower.contains("close_peer")
                    || lower.contains("closepeer"))
            {
                // Allow documentation lines that say nfd is *not* a disconnect input.
                if lower.contains("must not")
                    || lower.contains("not a disconnect")
                    || lower.contains("never disconnect")
                    || lower.contains("informational")
                {
                    continue;
                }
                panic!("peer_manager/{name} has nfd disconnect condition: {line}");
            }
        }
    }
}

// ── Predicates ──────────────────────────────────────────────────────────────

#[test]
fn predicates_unit_against_constructed_enrs() {
    use cc_p2p::discovery::{ENR_KEY_ATTNETS, ENR_KEY_SYNCNETS};

    let dig_ok = ForkDigest::from_array([0xaa, 0xbb, 0xcc, 0xdd]);
    let dig_bad = ForkDigest::from_array([0x11, 0x22, 0x33, 0x44]);
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
    let eth2 = cc_p2p::fork_digest::EnrForkId {
        fork_digest: dig_ok,
        next_fork_version: ForkVersion::ZERO,
        next_fork_epoch: Epoch::new(u64::MAX),
    };
    manager
        .apply([
            EnrFieldChange::new(ENR_KEY_ETH2, encode_eth2(eth2)),
            EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(1u64 << 7)),
            EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(1u8 << 1)),
            EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(cc_types::NUMBER_OF_CUSTODY_GROUPS)),
        ])
        .unwrap();
    let enr = manager.local_enr();

    let g = generic_peer_predicate(vec![dig_ok]);
    assert!(g(&enr));
    let g_bad = generic_peer_predicate(vec![dig_bad]);
    assert!(!g_bad(&enr));

    let a = attestation_subnet_predicate(vec![dig_ok], 7);
    assert!(a(&enr));
    let a_clear = attestation_subnet_predicate(vec![dig_ok], 0);
    assert!(!a_clear(&enr));

    let s = sync_subnet_predicate(vec![dig_ok], 1);
    assert!(s(&enr));
    let s_clear = sync_subnet_predicate(vec![dig_ok], 0);
    assert!(!s_clear(&enr));

    let col = *enr_custody_groups(&enr).iter().next().unwrap();
    let c = column_predicate(vec![dig_ok], col);
    assert!(c(&enr));
}

// ── Dial queue bound + no swarm in discovery ────────────────────────────────

#[test]
fn dial_queue_bound_256_depth_exported() {
    let mut q = DialQueue::new();
    assert_eq!(DialQueue::capacity(), DIAL_QUEUE_BOUND);
    for _ in 0..DIAL_QUEUE_BOUND {
        let peer_id = PeerId::from_public_key(&Keypair::generate_secp256k1().public());
        assert!(q.push(DialCandidate {
            peer_id,
            addr: "/ip4/127.0.0.1/tcp/9000".parse().unwrap(),
            priority: 1,
            enr_info: None,
        }));
    }
    assert_eq!(q.len(), DIAL_QUEUE_BOUND);
    let peer_id = PeerId::from_public_key(&Keypair::generate_secp256k1().public());
    assert!(!q.push(DialCandidate {
        peer_id,
        addr: "/ip4/127.0.0.1/tcp/9000".parse().unwrap(),
        priority: 1,
        enr_info: None,
    }));
}

#[test]
fn dial_priority_not_always_base() {
    // B3: peers covering deficit subnets score above PRIORITY_BASE.
    use cc_p2p::discovery::{ENR_KEY_ATTNETS, encode_attnets};
    use discv5::enr::CombinedKey;
    use std::net::Ipv4Addr;

    let key = CombinedKey::generate_secp256k1();
    let mut enr = discv5::Enr::builder()
        .ip4(Ipv4Addr::LOCALHOST)
        .tcp4(9000)
        .udp4(9000)
        .build(&key)
        .unwrap();
    enr.insert(ENR_KEY_ATTNETS, &encode_attnets(1u64 << 2).as_slice(), &key)
        .unwrap();

    let mut view = DiscoveryPeerView::default();
    view.attnet_peer_counts[2] = 0; // deficit
    let interested = 1u64 << 2;
    let p = dial_priority_for_enr(&enr, &view, 3, interested);
    assert!(p > PRIORITY_BASE);
}

#[test]
fn discovery_module_does_not_reference_swarm() {
    // Acceptance: `grep -rn "swarm" services/p2p/src/discovery/` returns nothing
    // that dials the swarm. Allow crate docs mentioning the policy.
    for (name, src) in [
        ("mod.rs", include_str!("../src/discovery/mod.rs")),
        ("enr.rs", include_str!("../src/discovery/enr.rs")),
        (
            "predicate.rs",
            include_str!("../src/discovery/predicate.rs"),
        ),
        (
            "dial_queue.rs",
            include_str!("../src/discovery/dial_queue.rs"),
        ),
        ("task.rs", include_str!("../src/discovery/task.rs")),
    ] {
        for line in src.lines() {
            let lower = line.to_ascii_lowercase();
            if !lower.contains("swarm") {
                continue;
            }
            // Allowed: comments / docs that say discovery never touches the swarm.
            if lower.contains("never")
                || lower.contains("does not")
                || lower.contains("don't")
                || lower.contains("must not")
                || lower.contains("not touch")
                || line.trim_start().starts_with("//")
                || line.trim_start().starts_with("//!")
                || line.trim_start().starts_with("///")
            {
                continue;
            }
            panic!("discovery/{name} references swarm outside docs: {line}");
        }
    }
}

// ── Bootnodes parse (V-4 list) ──────────────────────────────────────────────

#[test]
fn hoodi_bootnodes_from_config_parse() {
    // Spot-check a few ENRs from the V-4 list in config/p2p.toml.
    let sample = "enr:-Ku4QLVumWTwyOUVS4ajqq8ZuZz2ik6t3Gtq0Ozxqecj0qNZWpMnudcvTs-4jrlwYRQMQwBS8Pvtmu4ZPP2Lx3i2t7YBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpBd9cEGEAAJEP__________gmlkgnY0gmlwhNEmfKCJc2VjcDI1NmsxoQLdRlI8aCa_ELwTJhVN8k7km7IDc3pYu-FMYBs5_FiigIN1ZHCCIyk";
    let enr = parse_bootnode(sample).expect("parse hoodi bootnode");
    assert!(enr.udp4().is_some() || enr.tcp4().is_some());
    let list = parse_bootnodes(["# comment", "", sample, &format!("enr:{sample}")]).unwrap();
    assert_eq!(list.len(), 2);
}

// ── Discovery restart budget (5 in 5 minutes → fatal) ───────────────────────

#[tokio::test]
async fn discovery_restart_budget_exhausted_is_fatal() {
    let metrics = metrics();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Factory always panics → supervisor counts restarts.
    let factory = factory_from_future("discovery", || async {
        panic!("induced discovery panic");
    });
    let tasks = vec![SupervisedTask {
        name: "discovery",
        policy: TaskPolicy::discovery(),
        factory,
    }];

    let handle = tokio::spawn(async move { run_supervisor(tasks, metrics, shutdown_rx).await });

    // Budget: 5 restarts in 5 minutes → 6th panic is fatal (len > max_restarts).
    // Policy counts panics in the window; first panic is restart 1, …
    // After max_restarts+1 panics we get Fatal.
    let outcome = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("supervisor finished")
        .expect("join");

    match outcome {
        SupervisorOutcome::Fatal { task, payload } => {
            assert_eq!(task, "discovery");
            assert!(
                payload.contains("restart budget") || payload.contains("panic"),
                "payload={payload}"
            );
        }
        other => panic!("expected Fatal, got {other:?}"),
    }
    let _ = shutdown_tx; // keep alive until end
}

// Keep PeerTable import honest if we expand table-level tests.
#[test]
fn peer_table_default_empty() {
    let t = PeerTable::default();
    assert!(t.is_empty());
    let _ = PeerRecord::new(PeerId::from_public_key(
        &Keypair::generate_secp256k1().public(),
    ));
}
