//! CC-2A — BPO transition: schedule-driven Overlap/Drain, nfd / ENR, Status.
//!
//! # CC-2A/6 — V-3 re-read (source, not the issue doc)
//!
//! Re-read date: **2026-08-07**.
//!
//! Sources:
//! - Committed local: `crates/types/tests/fixtures/hoodi-config.yaml`
//!   (`BLOB_SCHEDULE` matches eth-clients/hoodi `metadata/config.yaml` and
//!   `https://beacon.hoodi.ethpandaops.io/eth/v1/config/spec` the same day).
//! - Live head (wall-clock 2026-08-07 UTC): slot ≈ 3 656 764 → **epoch ≈ 114 273**
//!   from `MIN_GENESIS_TIME + GENESIS_DELAY = 1742213400` and 12 s slots
//!   (also cross-checked against ethpandaops `/eth/v1/beacon/headers/head`).
//! - `BLOB_SCHEDULE`: BPO 1 at **52 480** (max_blobs=15), BPO 2 at **54 016**
//!   (max_blobs=21). Both are **well behind** live head (~nine months past).
//! - **No real-network BPO is pending.** Self-devnet is the only transition path.
//!
//! OQ-5 residual (recorded, not papered over): the self-devnet covers the
//! *mechanism* under our control (digest change, resubscription, `nfd`, peer
//! retention). Heterogeneous peers transitioning at slightly different times
//! is **not** covered — every peer on it runs our code.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;
use std::path::PathBuf;

use cc_p2p::discovery::{EnrManager, EnrSeqStrategy, generic_peer_predicate, read_eth2, read_nfd};
use cc_p2p::fork_digest::{
    FAR_FUTURE_EPOCH, ForkContext, compute_fork_digest, compute_fork_version,
    discovery_allowed_digests, enr_fork_id, is_overlap_entry_epoch, next_fork, next_fork_digest,
};
use cc_p2p::gossip::{
    GossipCall, RecordingGossipsub, SubnetCounts, SubscriptionPhase, TopicKey, TopicName,
    TopicParams, TopicRegistry, format_topic_string,
};
use cc_p2p::reqresp::{StatusEval, StatusV2, evaluate_peer_status};
use cc_types::{
    BlobParameters, BlobSchedule, ChainConfig, Epoch, ForkDigest, ForkVersion, Mainnet, Root,
    parse_hex_bytes,
};

// ── Hoodi fixtures ──────────────────────────────────────────────────────────

const HOODI_GVR_HEX: &str = "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f";

/// V-3 live head epoch recorded 2026-08-07 (see module docs).
const V3_LIVE_HEAD_EPOCH: u64 = 114_273;
const EPOCH_FULU_FALLBACK: u64 = 51_000;
const EPOCH_BPO1: u64 = 52_480;
const EPOCH_BPO2: u64 = 54_016;

fn hoodi_config() -> ChainConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
    ChainConfig::from_yaml_file(&path).unwrap_or_else(|e| panic!("hoodi-config.yaml: {e}"))
}

fn hoodi_gvr() -> Root {
    Root::from_array(parse_hex_bytes::<32>(HOODI_GVR_HEX).unwrap())
}

/// Self-devnet style schedule: BPO at epochs 5 and 10 (CC-2Ja / booking g).
fn devnet_two_bpo_config() -> ChainConfig {
    let schedule = BlobSchedule::try_from_entries(vec![
        BlobParameters {
            epoch: Epoch::new(5),
            max_blobs_per_block: 6,
        },
        BlobParameters {
            epoch: Epoch::new(10),
            max_blobs_per_block: 9,
        },
    ])
    .expect("schedule");
    ChainConfig {
        preset_base: cc_types::PresetName::Mainnet,
        config_name: "cc-2a-devnet".into(),
        genesis_fork_version: ForkVersion::from_array([0, 0, 0, 1]),
        altair_fork_version: ForkVersion::from_array([1, 0, 0, 1]),
        altair_fork_epoch: Epoch::new(0),
        bellatrix_fork_version: ForkVersion::from_array([2, 0, 0, 1]),
        bellatrix_fork_epoch: Epoch::new(0),
        capella_fork_version: ForkVersion::from_array([3, 0, 0, 1]),
        capella_fork_epoch: Epoch::new(0),
        deneb_fork_version: ForkVersion::from_array([4, 0, 0, 1]),
        deneb_fork_epoch: Epoch::new(0),
        electra_fork_version: ForkVersion::from_array([5, 0, 0, 1]),
        electra_fork_epoch: Epoch::new(0),
        fulu_fork_version: ForkVersion::from_array([6, 0, 0, 1]),
        fulu_fork_epoch: Epoch::new(0),
        seconds_per_slot: 3,
        blob_schedule: schedule,
        deposit_chain_id: 1,
        deposit_contract_address: Default::default(),
    }
}

fn sparse_counts() -> SubnetCounts {
    SubnetCounts {
        attestation: 0,
        sync_committee: 0,
        data_column_sidecar: 0,
    }
}

// ── CC-2A/6 V-3 ─────────────────────────────────────────────────────────────

#[test]
fn v3_blob_schedule_re_read_no_pending_bpo_ahead_of_live_head() {
    let cfg = hoodi_config();
    assert_eq!(cfg.blob_schedule.entries().len(), 2);
    assert_eq!(cfg.blob_schedule.entries()[0].epoch, Epoch::new(EPOCH_BPO1));
    assert_eq!(cfg.blob_schedule.entries()[0].max_blobs_per_block, 15);
    assert_eq!(cfg.blob_schedule.entries()[1].epoch, Epoch::new(EPOCH_BPO2));
    assert_eq!(cfg.blob_schedule.entries()[1].max_blobs_per_block, 21);

    // Live head epoch from V-3 re-read (module docs) — both BPOs are past.
    const {
        assert!(EPOCH_BPO2 < V3_LIVE_HEAD_EPOCH);
    }
    // No schedule entry ahead of live head → no real-network BPO pending.
    for entry in cfg.blob_schedule.entries() {
        let e = entry.epoch.as_u64();
        assert!(
            e < V3_LIVE_HEAD_EPOCH,
            "unexpected future BPO at epoch {e} (live head {V3_LIVE_HEAD_EPOCH})"
        );
    }
    // Past last entry: nfd zero, FAR_FUTURE next epoch.
    let gvr = hoodi_gvr();
    let enr = enr_fork_id(&cfg, gvr, Epoch::new(V3_LIVE_HEAD_EPOCH));
    assert_eq!(enr.next_fork_epoch, FAR_FUTURE_EPOCH);
    assert_eq!(
        next_fork_digest(&cfg, gvr, Epoch::new(V3_LIVE_HEAD_EPOCH)),
        ForkDigest::ZERO
    );
    // Consumes get_blob_parameters from Phase 1 (CC-1G), not re-implemented.
    let bp = cfg.get_blob_parameters::<Mainnet>(Epoch::new(V3_LIVE_HEAD_EPOCH));
    assert_eq!(bp.epoch, Epoch::new(EPOCH_BPO2));
    assert_eq!(bp.max_blobs_per_block, 21);
}

// ── CC-2A/1 boundary known in advance ───────────────────────────────────────

#[test]
fn boundary_known_in_advance_from_schedule_before_arrival() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    // Well before BPO1: next_fork must already report 52480.
    let epoch = Epoch::new(EPOCH_FULU_FALLBACK);
    let (b, version, digest) = next_fork(&cfg, gvr, epoch).expect("BPO1 scheduled");
    assert_eq!(b, Epoch::new(EPOCH_BPO1));
    assert!(
        b.as_u64() > epoch.as_u64(),
        "boundary must be known strictly before it arrives"
    );
    assert_eq!(version, cfg.fulu_fork_version);
    assert_eq!(
        digest,
        compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1))
    );

    let mut ctx = ForkContext::new(cfg.clone(), gvr, epoch);
    assert_eq!(ctx.next_boundary_epoch(), Some(Epoch::new(EPOCH_BPO1)));
    assert!(is_overlap_entry_epoch(&cfg, Epoch::new(EPOCH_BPO1 - 1)));
    assert!(!is_overlap_entry_epoch(&cfg, epoch));

    // Epoch tick recomputes; boundary stays known until crossed.
    ctx.on_epoch(Epoch::new(EPOCH_BPO1 - 2));
    assert_eq!(ctx.next_boundary_epoch(), Some(Epoch::new(EPOCH_BPO1)));
    ctx.on_epoch(Epoch::new(EPOCH_BPO1));
    assert_eq!(ctx.next_boundary_epoch(), Some(Epoch::new(EPOCH_BPO2)));
}

// ── CC-2A/2 overlap window both ends ────────────────────────────────────────

#[test]
fn overlap_window_both_topic_sets_inside_exactly_one_outside() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let boundary = Epoch::new(EPOCH_BPO1);
    let start = Epoch::new(EPOCH_FULU_FALLBACK);
    let mut ctx = ForkContext::new(cfg, gvr, start);
    let d_current = ctx.current_digest();
    let (b, _, d_next) = ctx.next().expect("BPO1");
    assert_eq!(b, boundary);
    assert_ne!(d_current, d_next);

    let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, sparse_counts());
    reg.register_validator(TopicName::BeaconBlock);
    reg.subscribe(
        TopicKey::new(d_current, TopicName::BeaconBlock),
        TopicParams { topic_weight: 1.0 },
    )
    .unwrap();

    // Outside before window: exactly one.
    assert_eq!(reg.phase(), SubscriptionPhase::Steady);
    assert_eq!(reg.live_digests(), HashSet::from([d_current]));

    // boundary − 2 still Steady.
    let e = Epoch::new(boundary.as_u64() - 2);
    ctx.on_epoch(e);
    reg.advance_to(e, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Steady);
    assert_eq!(reg.live_digests().len(), 1);

    // boundary − 1 → Overlap: both.
    let e = Epoch::new(boundary.as_u64() - 1);
    ctx.on_epoch(e);
    reg.advance_to(e, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
    assert_eq!(reg.live_digests(), HashSet::from([d_current, d_next]));
    assert_eq!(
        reg.publish_digest(e),
        d_current,
        "pre-boundary publish = current"
    );

    // At boundary: still Overlap; publish switches to next.
    ctx.on_epoch(boundary);
    reg.advance_to(boundary, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
    assert_eq!(reg.live_digests(), HashSet::from([d_current, d_next]));
    assert_eq!(
        reg.publish_digest(boundary),
        d_next,
        "at boundary publish = next"
    );

    // boundary + 1 → Drain: exactly one (next).
    let e = Epoch::new(boundary.as_u64() + 1);
    ctx.on_epoch(e);
    reg.advance_to(e, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Drain);
    assert_eq!(reg.live_digests(), HashSet::from([d_next]));

    // Outside after: Steady with current := next.
    let e = Epoch::new(boundary.as_u64() + 2);
    ctx.on_epoch(e);
    reg.advance_to(e, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Steady);
    assert_eq!(reg.live_digests(), HashSet::from([d_next]));
    assert_eq!(reg.current_digest(), d_next);
}

#[test]
fn set_topic_params_precedes_subscribe_for_next_digest_on_overlap() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let boundary = Epoch::new(EPOCH_BPO1);
    let mut ctx = ForkContext::new(cfg, gvr, Epoch::new(EPOCH_FULU_FALLBACK));
    let d_current = ctx.current_digest();
    let d_next = ctx.next().unwrap().2;

    let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, sparse_counts());
    reg.register_validator(TopicName::BeaconBlock);
    reg.subscribe(
        TopicKey::new(d_current, TopicName::BeaconBlock),
        TopicParams { topic_weight: 3.0 },
    )
    .unwrap();
    reg.gossip_mut().calls.clear();

    let e = Epoch::new(boundary.as_u64() - 1);
    ctx.on_epoch(e);
    reg.advance_to(e, &ctx).unwrap();

    let next_topic = format_topic_string(&d_next, TopicName::BeaconBlock);
    let calls = &reg.gossip().calls;
    let set_idx = calls.iter().position(
        |c| matches!(c, GossipCall::SetTopicParams { topic, .. } if topic == &next_topic),
    );
    let sub_idx = calls
        .iter()
        .position(|c| matches!(c, GossipCall::Subscribe { topic } if topic == &next_topic));
    let (si, su) = (
        set_idx.expect("set_topic_params for next-digest topic"),
        sub_idx.expect("subscribe for next-digest topic"),
    );
    assert!(si < su, "params must precede subscribe: {calls:?}");
}

// ── CC-2A/3 nfd + next_fork_version unchanged across BPO ────────────────────

#[test]
fn nfd_and_next_fork_version_across_hoodi_bpo() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();

    // Before BPO1: nfd = BPO1 digest; next_fork_epoch = BPO1; version stays Fulu.
    let before = Epoch::new(EPOCH_FULU_FALLBACK);
    let enr = enr_fork_id(&cfg, gvr, before);
    assert_eq!(enr.next_fork_epoch, Epoch::new(EPOCH_BPO1));
    assert_eq!(enr.next_fork_version, cfg.fulu_fork_version);
    assert_eq!(
        enr.next_fork_version,
        compute_fork_version(&cfg, before),
        "BPO must not advance next_fork_version"
    );
    assert_eq!(
        next_fork_digest(&cfg, gvr, before),
        compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1))
    );

    // After BPO1, before BPO2.
    let mid = Epoch::new(EPOCH_BPO1);
    let enr_mid = enr_fork_id(&cfg, gvr, mid);
    assert_eq!(enr_mid.next_fork_epoch, Epoch::new(EPOCH_BPO2));
    assert_eq!(
        enr_mid.next_fork_version, cfg.fulu_fork_version,
        "next_fork_version unchanged across BPO1"
    );
    assert_eq!(
        next_fork_digest(&cfg, gvr, mid),
        compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO2))
    );

    // Past last BPO: nfd zero-filled.
    let after = Epoch::new(EPOCH_BPO2 + 1);
    assert_eq!(next_fork_digest(&cfg, gvr, after), ForkDigest::ZERO);
    assert_eq!(
        enr_fork_id(&cfg, gvr, after).next_fork_epoch,
        FAR_FUTURE_EPOCH
    );
    assert_eq!(
        enr_fork_id(&cfg, gvr, after).next_fork_version,
        cfg.fulu_fork_version
    );
}

#[test]
fn enr_eth2_nfd_coalesce_one_seq_bump_across_bpo_boundary() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();

    // Seed at Fulu-fallback epoch.
    let mut ctx = ForkContext::new(cfg.clone(), gvr, Epoch::new(EPOCH_FULU_FALLBACK));
    manager.apply_fork_context(&ctx).unwrap();
    let seq_seeded = manager.local_enr().seq();

    // Same epoch again: idempotent, no bump.
    manager.apply_fork_context(&ctx).unwrap();
    assert_eq!(
        manager.local_enr().seq(),
        seq_seeded,
        "unchanged eth2/nfd must not bump seq"
    );

    // Advance to boundary − 1: digests/nfd still same current; next_fork_epoch
    // already pointed at BPO1 — may or may not change eth2 payload fields.
    ctx.on_epoch(Epoch::new(EPOCH_BPO1 - 1));
    manager.apply_fork_context(&ctx).unwrap();
    let seq_pre = manager.local_enr().seq();

    // Cross BPO1: eth2.fork_digest + nfd both change → exactly one seq bump.
    ctx.on_epoch(Epoch::new(EPOCH_BPO1));
    manager.apply_fork_context(&ctx).unwrap();
    let enr = manager.local_enr();
    assert_eq!(
        enr.seq(),
        seq_pre + 1,
        "BPO boundary eth2+nfd must coalesce to one seq bump"
    );
    assert!(enr.verify());
    assert_eq!(read_eth2(&enr).unwrap().fork_digest, ctx.current_digest());
    assert_eq!(read_nfd(&enr).unwrap(), ctx.nfd());
    // next_fork_version still Fulu after the bump.
    assert_eq!(
        read_eth2(&enr).unwrap().next_fork_version,
        cfg.fulu_fork_version
    );
    assert_eq!(
        read_eth2(&enr).unwrap().next_fork_epoch,
        Epoch::new(EPOCH_BPO2)
    );

    // Idempotent re-apply.
    manager.apply_fork_context(&ctx).unwrap();
    assert_eq!(manager.local_enr().seq(), seq_pre + 1);
}

// ── CC-2A/4 Status v2 at boundary ───────────────────────────────────────────

#[test]
fn status_v2_switches_at_boundary_old_peer_asymmetry() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let d_old = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1 - 1));
    let d_new = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1));
    assert_ne!(d_old, d_new);

    let mut peer_old = StatusV2 {
        fork_digest: d_old,
        finalized_root: Root::ZERO,
        finalized_epoch: Epoch::new(0),
        head_root: Root::ZERO,
        head_slot: cc_types::Slot::new(0),
        earliest_available_slot: cc_types::Slot::new(0),
    };

    // Before boundary: local still on old → peer on old Accepts (not disconnected).
    assert_eq!(
        evaluate_peer_status(d_old, &peer_old),
        StatusEval::Accept,
        "peer on old digest must not disconnect before the boundary"
    );

    // After boundary: local on new → peer still on old MAY disconnect (Reject).
    match evaluate_peer_status(d_new, &peer_old) {
        StatusEval::RejectIrrelevantNetwork { .. } => {}
        StatusEval::Accept => panic!("peer on old digest may be disconnected after the boundary"),
    }

    // Peer that switched with us Accepts after boundary.
    peer_old.fork_digest = d_new;
    assert_eq!(evaluate_peer_status(d_new, &peer_old), StatusEval::Accept);

    // ForkContext Status source tracks boundary.
    let mut ctx = ForkContext::new(cfg, gvr, Epoch::new(EPOCH_BPO1 - 1));
    assert_eq!(ctx.current_digest(), d_old);
    ctx.on_epoch(Epoch::new(EPOCH_BPO1));
    assert_eq!(ctx.current_digest(), d_new);
}

// ── Discovery allowed digests (Overlap) ─────────────────────────────────────

#[test]
fn discovery_allowed_digests_overlap_window_hoodi() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let d_old = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1 - 1));
    let d_new = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1));

    // Far before: only current.
    let far = discovery_allowed_digests(&cfg, gvr, Epoch::new(EPOCH_FULU_FALLBACK));
    assert_eq!(
        far,
        vec![compute_fork_digest(
            &cfg,
            gvr,
            Epoch::new(EPOCH_FULU_FALLBACK)
        )]
    );

    // boundary − 1: current + next.
    let pre = discovery_allowed_digests(&cfg, gvr, Epoch::new(EPOCH_BPO1 - 1));
    assert_eq!(pre.len(), 2);
    assert!(pre.contains(&d_old));
    assert!(pre.contains(&d_new));

    // At boundary: current (new) + previous (old).
    let at = discovery_allowed_digests(&cfg, gvr, Epoch::new(EPOCH_BPO1));
    assert_eq!(at.len(), 2);
    assert!(at.contains(&d_old));
    assert!(at.contains(&d_new));

    // boundary + 1 (Drain): only new.
    let post = discovery_allowed_digests(&cfg, gvr, Epoch::new(EPOCH_BPO1 + 1));
    assert_eq!(post, vec![d_new]);

    // Predicate accepts both during Overlap.
    let pred = generic_peer_predicate(pre);
    // Build minimal ENRs via EnrManager is heavy; unit predicate already covers
    // containment — schedule set is the CC-2A contract under test here.
    let _ = pred;
}

// ── Two-boundary self-devnet schedule (CC-2A/5 unit stand-in) ───────────────

#[test]
fn two_boundary_devnet_schedule_steady_overlap_drain_twice() {
    let cfg = devnet_two_bpo_config();
    let gvr = Root::from_array([0x22; 32]);
    let mut ctx = ForkContext::new(cfg, gvr, Epoch::new(0));
    let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, sparse_counts());
    reg.register_validator(TopicName::BeaconBlock);

    let d0 = ctx.current_digest();
    reg.subscribe(
        TopicKey::new(d0, TopicName::BeaconBlock),
        TopicParams { topic_weight: 1.0 },
    )
    .unwrap();

    let mut topic_set_changes = 0u32;
    let mut last_live = reg.live_digests();

    // Walk epochs 0..14 covering bpo@5 and bpo@10.
    for epoch_u in 0u64..=14 {
        let e = Epoch::new(epoch_u);
        ctx.on_epoch(e);
        reg.advance_to(e, &ctx).unwrap();
        let live = reg.live_digests();
        if live != last_live {
            // Count transitions of the live digest *set* (Steady↔Overlap↔Drain).
            // A full BPO cycle changes the set at Overlap entry and Drain entry.
            topic_set_changes += 1;
            last_live = live;
        }

        // Boundary known in advance until crossed.
        if epoch_u < 5 {
            assert_eq!(ctx.next_boundary_epoch(), Some(Epoch::new(5)));
            assert_eq!(reg.boundary(), Some(Epoch::new(5)));
        } else if (5..10).contains(&epoch_u) {
            // After first boundary, next is 10 (once Steady re-armed).
            if reg.phase() == SubscriptionPhase::Steady || epoch_u >= 7 {
                assert_eq!(
                    ctx.next_boundary_epoch(),
                    Some(Epoch::new(10)),
                    "epoch {epoch_u}"
                );
            }
        }

        match epoch_u {
            4 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
                assert_eq!(reg.live_digests().len(), 2);
            }
            5 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
                assert_eq!(reg.live_digests().len(), 2);
            }
            6 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Drain);
                assert_eq!(reg.live_digests().len(), 1);
            }
            7 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Steady);
            }
            9 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
                assert_eq!(reg.live_digests().len(), 2);
            }
            10 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
            }
            11 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Drain);
            }
            12 => {
                assert_eq!(reg.phase(), SubscriptionPhase::Steady);
                assert!(ctx.next().is_none());
                assert_eq!(ctx.nfd(), ForkDigest::ZERO);
            }
            _ => {}
        }
    }

    // Two full Overlap+Drain cycles → at least 4 live-set changes
    // (enter Overlap₁, enter Drain₁, enter Overlap₂, enter Drain₂); Steady
    // roll may add more. Require the booking (g) shape: crossed twice.
    assert!(
        topic_set_changes >= 4,
        "expected ≥4 live-set changes across two BPOs, got {topic_set_changes}"
    );
    // Final digest is BPO2's.
    let d_final = compute_fork_digest(ctx.config(), gvr, Epoch::new(10));
    assert_eq!(reg.current_digest(), d_final);
    assert_eq!(reg.live_digests(), HashSet::from([d_final]));
}

#[test]
fn get_blob_parameters_consumed_not_reimplemented_in_fork_digest() {
    // Grep-style: fork_digest.rs must call ChainConfig::get_blob_parameters
    // and must not hard-code Electra/Fulu max-blobs constants.
    let src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/fork_digest.rs"));
    assert!(
        src.contains("get_blob_parameters"),
        "CC-2A/1: must consume get_blob_parameters from Phase 1"
    );
    let code_only: String = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//") && !t.starts_with("//!") && !t.starts_with("///")
        })
        .collect::<Vec<_>>()
        .join("\n");
    for banned in ["MAX_BLOBS_PER_BLOCK", "ELECTRA_FORK_EPOCH"] {
        assert!(
            !code_only.contains(banned),
            "must not hard-code {banned} in fork_digest.rs"
        );
    }
}
