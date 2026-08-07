//! CC-22a / CC-22/1 — Hoodi topic-string fixture and registry acceptance.
//!
//! The committed fixture embeds the CC-21b BPO2 digest; a wrong digest fails
//! here as a string diff rather than as zero peers on Hoodi.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;
use std::path::PathBuf;

use cc_p2p::fork_digest::{ForkContext, compute_fork_digest};
use cc_p2p::gossip::{
    ATTESTATION_SUBNET_COUNT, RecordingGossipsub, RegistryError, SubnetCounts, SubscriptionPhase,
    TopicKey, TopicName, TopicParams, TopicRegistry, expand_fulu_topic_names,
    expand_fulu_topic_strings, format_topic_string,
};
use cc_types::{
    DATA_COLUMN_SIDECAR_SUBNET_COUNT, ChainConfig, Epoch, ForkDigest, Mainnet, Preset, Root,
    parse_hex_bytes,
};

/// Hoodi GVR — same as CC-21b fixture.
const HOODI_GVR_HEX: &str = "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f";

/// Fixture epoch: BPO 2 activation (CC-21b EXPECT_BPO2 era).
const FIXTURE_EPOCH: u64 = 54_016;

/// CC-21b committed BPO2 digest.
const EXPECT_BPO2: [u8; 4] = [0xc6, 0xec, 0xb7, 0x6c];

fn hoodi_config() -> ChainConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
    ChainConfig::from_yaml_file(&path).unwrap_or_else(|e| panic!("hoodi-config.yaml: {e}"))
}

fn hoodi_gvr() -> Root {
    let bytes = parse_hex_bytes::<32>(HOODI_GVR_HEX).expect("gvr hex");
    Root::from_array(bytes)
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/hoodi-topics.txt")
}

fn load_fixture_topics() -> Vec<String> {
    let text = std::fs::read_to_string(fixture_path()).expect("hoodi-topics.txt");
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

// ── CC-22/1 fixture equality ────────────────────────────────────────────────

#[test]
fn hoodi_topic_strings_match_committed_fixture() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let epoch = Epoch::new(FIXTURE_EPOCH);

    let digest = compute_fork_digest(&cfg, gvr, epoch);
    assert_eq!(
        digest.as_slice(),
        &EXPECT_BPO2,
        "fixture epoch must yield CC-21b BPO2 digest"
    );

    // BLOB_SCHEDULE entry that produced this digest (BPO 2).
    let bp = cfg.get_blob_parameters::<Mainnet>(epoch);
    assert_eq!(bp.epoch, Epoch::new(54_016));
    assert_eq!(bp.max_blobs_per_block, 21);

    let counts = SubnetCounts::mainnet();
    assert_eq!(counts.attestation, ATTESTATION_SUBNET_COUNT);
    assert_eq!(
        counts.sync_committee,
        Mainnet::SYNC_COMMITTEE_SUBNET_COUNT
    );
    assert_eq!(
        counts.data_column_sidecar,
        DATA_COLUMN_SIDECAR_SUBNET_COUNT
    );

    let constructed = expand_fulu_topic_strings(digest, &counts);
    let expected = load_fixture_topics();

    assert_eq!(
        constructed.len(),
        expected.len(),
        "topic count mismatch (constructed {} vs fixture {})",
        constructed.len(),
        expected.len()
    );
    for (i, (got, want)) in constructed.iter().zip(expected.iter()).enumerate() {
        assert_eq!(got, want, "topic string mismatch at index {i}");
    }
    assert_eq!(
        constructed, expected,
        "full fixture equality (CC-22/1)"
    );
}

#[test]
fn fixture_header_records_epoch_digest_and_bpo_entry() {
    let text = std::fs::read_to_string(fixture_path()).expect("hoodi-topics.txt");
    let header: String = text
        .lines()
        .take_while(|l| l.starts_with('#') || l.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        header.contains("54016") || header.contains("54_016") || header.contains("54 016"),
        "header must record fixture epoch: {header}"
    );
    assert!(
        header.contains("c6ecb76c"),
        "header must record fork digest (cross-ref CC-21b): {header}"
    );
    assert!(
        header.contains("BLOB_SCHEDULE") || header.contains("max_blobs"),
        "header must record BLOB_SCHEDULE entry: {header}"
    );
    assert!(
        header.contains("2026-08-07"),
        "header must record derivation date: {header}"
    );
    assert!(
        header.contains("CC-21b") || header.contains("EXPECT_BPO2"),
        "header must cross-reference CC-21b: {header}"
    );
}

// ── blob_sidecar absence ────────────────────────────────────────────────────

#[test]
fn blob_sidecar_absent_from_all_constructible_topics() {
    let names = expand_fulu_topic_names(&SubnetCounts::mainnet());
    for name in names {
        let seg = name.path_segment();
        assert!(
            !seg.starts_with("blob_sidecar") && !seg.contains("blob_sidecar_"),
            "deprecated blob_sidecar must not appear: {seg}"
        );
    }

    let digest = ForkDigest::from_array(EXPECT_BPO2);
    for s in expand_fulu_topic_strings(digest, &SubnetCounts::mainnet()) {
        assert!(
            !s.contains("blob_sidecar_"),
            "topic string must not contain blob_sidecar_: {s}"
        );
    }
}

// ── subnet counts from config/types, not inlined in topics.rs ───────────────

#[test]
fn subnet_counts_sourced_from_types_constants() {
    let c = SubnetCounts::mainnet();
    assert_eq!(c.attestation, ATTESTATION_SUBNET_COUNT);
    assert_eq!(c.sync_committee, Mainnet::SYNC_COMMITTEE_SUBNET_COUNT);
    assert_eq!(c.data_column_sidecar, DATA_COLUMN_SIDECAR_SUBNET_COUNT);

    // Expansion cardinality tracks the supplied counts.
    let n = expand_fulu_topic_names(&c).len() as u64;
    let expected = 2 + c.attestation + c.data_column_sidecar + 1 + c.sync_committee + 4;
    assert_eq!(n, expected);
}

// ── registry: no-validator guard + params-before-subscribe ──────────────────

#[test]
fn subscribe_without_validator_is_refused() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let ctx = ForkContext::new(cfg, gvr, Epoch::new(FIXTURE_EPOCH));
    let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, SubnetCounts::mainnet());

    let key = TopicKey::new(ctx.current_digest(), TopicName::BeaconBlock);
    let err = reg
        .subscribe(key, TopicParams::default())
        .expect_err("must refuse");
    assert!(matches!(err, RegistryError::NoValidator(_)));
    assert!(
        reg.gossip().calls.is_empty(),
        "refused subscribe must not touch gossipsub"
    );
}

#[test]
fn params_applied_before_subscribe_on_recording_stub() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let ctx = ForkContext::new(cfg, gvr, Epoch::new(FIXTURE_EPOCH));
    let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, SubnetCounts::mainnet());
    reg.register_validator(TopicName::VoluntaryExit);

    let key = TopicKey::new(ctx.current_digest(), TopicName::VoluntaryExit);
    let params = TopicParams { topic_weight: 99 };
    reg.subscribe(key, params.clone()).unwrap();

    let calls = &reg.gossip().calls;
    assert!(
        calls.len() >= 2,
        "expected set_topic_params then subscribe: {calls:?}"
    );
    let topic = format_topic_string(&key.digest, key.name);
    assert!(
        matches!(&calls[0], cc_p2p::gossip::GossipCall::SetTopicParams { topic: t, params: p } if t == &topic && p == &params)
    );
    assert!(
        matches!(&calls[1], cc_p2p::gossip::GossipCall::Subscribe { topic: t } if t == &topic)
    );
}

// ── state machine live set ──────────────────────────────────────────────────

#[test]
fn state_machine_live_set_across_bpo_with_hoodi_schedule() {
    // Use Hoodi's real BPO1 boundary (52480) so two digests are distinct.
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let boundary = Epoch::new(52_480);
    let start = Epoch::new(52_000); // Fulu fallback window

    let mut ctx = ForkContext::new(cfg, gvr, start);
    let d_current = ctx.current_digest();
    let (b, _, d_next) = ctx.next().expect("BPO1 scheduled");
    assert_eq!(b, boundary);
    assert_ne!(d_current, d_next);

    let mut reg = TopicRegistry::new(
        RecordingGossipsub::default(),
        &ctx,
        SubnetCounts {
            attestation: 0,
            sync_committee: 0,
            data_column_sidecar: 0,
        },
    );
    reg.register_validator(TopicName::BeaconBlock);
    reg.subscribe(
        TopicKey::new(d_current, TopicName::BeaconBlock),
        TopicParams { topic_weight: 1 },
    )
    .unwrap();

    assert_eq!(reg.phase(), SubscriptionPhase::Steady);
    assert_eq!(reg.live_digests(), HashSet::from([d_current]));

    // Overlap at boundary − 1
    let e = Epoch::new(boundary.as_u64() - 1);
    ctx.on_epoch(e);
    reg.advance_to(e, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
    assert_eq!(reg.live_digests(), HashSet::from([d_current, d_next]));

    // Still Overlap at boundary
    ctx.on_epoch(boundary);
    reg.advance_to(boundary, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
    assert_eq!(reg.live_digests(), HashSet::from([d_current, d_next]));

    // Drain at boundary + 1
    let e = Epoch::new(boundary.as_u64() + 1);
    ctx.on_epoch(e);
    reg.advance_to(e, &ctx).unwrap();
    assert_eq!(reg.phase(), SubscriptionPhase::Drain);
    assert_eq!(reg.live_digests(), HashSet::from([d_next]));
}
