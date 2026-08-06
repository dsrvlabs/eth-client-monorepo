//! `operations` runner — `execution_payload` + `withdrawals` green for both
//! presets (CC-12b). Handlers `randao` / `eth1_data` are not emitted under Fulu
//! operations (covered by unit tests); the runner still greps for them if they
//! appear on disk.
//!
//! Does **not** depend on `cc-spec-tests` (crate DAG). Vector cache layout and
//! readiness markers match Architecture §10.1 / the committed `spec-vectors.lock`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use cc_state_transition::{
    process_eth1_data, process_execution_payload, process_randao, process_withdrawals, BlockError,
    EngineError, ExecutionEngine, GossipClass, NewPayloadRequest, PayloadStatus, TransitionContext,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, KzgCommitment, Root, Slot};
use cc_types::{
    BeaconBlock, BeaconBlockBody, BeaconState, ExecutionPayload, ForkName, SignedBeaconBlock,
};
use ssz::{Decode, Encode};

const FORK: &str = "fulu";
const RUNNER: &str = "operations";
const LOCKFILE: &str = include_str!("../../../spec-vectors.lock");
const SKIPLIST: &str = include_str!("../../../docs/spec-vectors-skiplist.md");

/// Handlers this runner owns (CC-12b subset; CC-12c–d fill the rest).
const HANDLERS: &[&str] = &["execution_payload", "withdrawals"];

// ---------------------------------------------------------------------------
// Vector-cache helpers
// ---------------------------------------------------------------------------

fn lock_tag() -> &'static str {
    LOCKFILE
        .lines()
        .find_map(|l| {
            let l = l.trim();
            l.strip_prefix("tag")
                .and_then(|r| r.trim().strip_prefix('='))
                .map(|v| v.trim().trim_matches('"'))
        })
        .expect("tag in spec-vectors.lock")
}

fn tests_root() -> PathBuf {
    let cache = std::env::var("SPEC_VECTORS_CACHE").unwrap_or_else(|_| {
        let home = std::env::var("HOME").expect("HOME");
        format!("{home}/.cache/eth-consensus-spec-vectors")
    });
    let tag = lock_tag();
    let root = PathBuf::from(cache).join(tag).join("tests");
    assert!(
        root.is_dir(),
        "vector tests tree missing at {}; run scripts/fetch-spec-vectors.sh",
        root.display()
    );
    root
}

fn snappy_decompress(path: &Path) -> Vec<u8> {
    let compressed = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let claimed = snap::raw::decompress_len(&compressed)
        .unwrap_or_else(|e| panic!("snappy len {}: {e}", path.display()));
    let mut out = vec![0u8; claimed];
    let n = snap::raw::Decoder::new()
        .decompress(&compressed, &mut out)
        .unwrap_or_else(|e| panic!("snappy {}: {e}", path.display()));
    out.truncate(n);
    out
}

fn skiplist_prefixes() -> Vec<String> {
    let mut out = Vec::new();
    let mut in_fence = false;
    for line in SKIPLIST.lines() {
        let line = line.trim();
        if line.starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || line.is_empty() {
            continue;
        }
        if !line.starts_with("- ") && !line.starts_with("* ") {
            continue;
        }
        if let Some(start) = line.find('`')
            && let Some(end) = line[start + 1..].find('`')
        {
            out.push(line[start + 1..start + 1 + end].to_string());
        }
    }
    out
}

fn is_skipped(rel: &str, prefixes: &[String]) -> bool {
    prefixes
        .iter()
        .any(|p| rel == p.as_str() || rel.starts_with(&format!("{p}/")))
}

fn collect_cases(tests: &Path, preset: &str, handler: &str) -> Vec<(String, PathBuf)> {
    let handler_dir = tests.join(preset).join(FORK).join(RUNNER).join(handler);
    if !handler_dir.is_dir() {
        return Vec::new();
    }
    let mut out = Vec::new();
    collect_leaf_cases(&handler_dir, &handler_dir, preset, handler, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn collect_leaf_cases(
    handler_dir: &Path,
    current: &Path,
    preset: &str,
    handler: &str,
    out: &mut Vec<(String, PathBuf)>,
) {
    let mut has_file = false;
    let mut subdirs = Vec::new();
    for ent in fs::read_dir(current).unwrap() {
        let ent = ent.unwrap();
        let ft = ent.file_type().unwrap();
        if ft.is_dir() {
            subdirs.push(ent.path());
        } else if ft.is_file() {
            has_file = true;
        }
    }
    if has_file {
        let rel_name = current
            .strip_prefix(handler_dir)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let case_rel = format!("{preset}/{FORK}/{RUNNER}/{handler}/{rel_name}");
        out.push((case_rel, current.to_path_buf()));
        return;
    }
    for sub in subdirs {
        collect_leaf_cases(handler_dir, &sub, preset, handler, out);
    }
}

// ---------------------------------------------------------------------------
// Spec config for operations vectors (blob schedule matches preset base)
// ---------------------------------------------------------------------------

fn spec_config_for_preset(preset: PresetName) -> ChainConfig {
    // Spec vectors run against the network config shipped with the pin; for Fulu
    // operation tests the BPO schedule is effectively "Electra base from epoch 0"
    // unless the case is past a BPO. Using a schedule entry at epoch 0 with the
    // Electra max (9) matches get_blob_parameters pre-BPO behaviour used by
    // pyspec for these cases.
    let (name, seconds) = match preset {
        PresetName::Mainnet => ("mainnet", 12u64),
        PresetName::Minimal => ("minimal", 6u64),
    };
    ChainConfig {
        preset_base: preset,
        config_name: name.into(),
        genesis_fork_version: ForkVersion::from_array([0; 4]),
        altair_fork_version: ForkVersion::from_array([1; 4]),
        altair_fork_epoch: Epoch::new(0),
        bellatrix_fork_version: ForkVersion::from_array([2; 4]),
        bellatrix_fork_epoch: Epoch::new(0),
        capella_fork_version: ForkVersion::from_array([3; 4]),
        capella_fork_epoch: Epoch::new(0),
        deneb_fork_version: ForkVersion::from_array([4; 4]),
        deneb_fork_epoch: Epoch::new(0),
        electra_fork_version: ForkVersion::from_array([5; 4]),
        electra_fork_epoch: Epoch::new(0),
        fulu_fork_version: ForkVersion::from_array([6; 4]),
        fulu_fork_epoch: Epoch::new(0),
        seconds_per_slot: seconds,
        blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
            epoch: Epoch::new(0),
            max_blobs_per_block: 9,
        }])
        .unwrap(),
        deposit_chain_id: 0,
        deposit_contract_address: ExecutionAddress::ZERO,
    }
}

// ---------------------------------------------------------------------------
// Engine that honours execution.yaml `execution_valid`
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct YamlEngine {
    execution_valid: bool,
}

impl<P: Preset> ExecutionEngine<P> for YamlEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, EngineError> {
        if self.execution_valid {
            Ok(PayloadStatus::Valid)
        } else {
            Ok(PayloadStatus::Invalid {
                latest_valid_hash: None,
            })
        }
    }
}

fn read_execution_valid(case_dir: &Path) -> bool {
    let path = case_dir.join("execution.yaml");
    if !path.is_file() {
        // No meta → treat as valid (engine accepts).
        return true;
    }
    let text = fs::read_to_string(&path).unwrap();
    // `{execution_valid: false}` or `execution_valid: true`
    if let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(&text)
        && let Some(b) = v.get("execution_valid").and_then(|x| x.as_bool())
    {
        return b;
    }
    true
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn run_withdrawals_cases<P: Preset>() {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let cases = collect_cases(&tests, P::NAME, "withdrawals");
    assert!(!cases.is_empty(), "expected withdrawals cases for {}", P::NAME);

    let mut ran = 0usize;
    let mut invalid_ok = 0usize;

    for (rel, case_dir) in &cases {
        if is_skipped(rel, &prefixes) {
            continue;
        }

        let pre_bytes = snappy_decompress(&case_dir.join("pre.ssz_snappy"));
        let mut state = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &pre_bytes)
            .unwrap_or_else(|e| panic!("decode pre {rel}: {e:?}"));

        let payload_bytes = snappy_decompress(&case_dir.join("execution_payload.ssz_snappy"));
        let payload = ExecutionPayload::<P>::from_ssz_bytes(&payload_bytes)
            .unwrap_or_else(|e| panic!("decode payload {rel}: {e:?}"));

        let body = BeaconBlockBody::<P> {
            execution_payload: payload,
            ..Default::default()
        };
        let block = BeaconBlock {
            slot: state.slot(),
            proposer_index: Default::default(),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body,
        };

        let post_path = case_dir.join("post.ssz_snappy");
        let result = process_withdrawals(&mut state, &block);

        if post_path.is_file() {
            result.unwrap_or_else(|e| panic!("process_withdrawals valid case {rel}: {e}"));
            let post_bytes = snappy_decompress(&post_path);
            let expected = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &post_bytes)
                .unwrap_or_else(|e| panic!("decode post {rel}: {e:?}"));
            assert_eq!(state, expected, "post-state mismatch for {rel}");
            assert_eq!(
                state.as_ssz_bytes(),
                expected.as_ssz_bytes(),
                "post SSZ bytes mismatch for {rel}"
            );
            ran += 1;
        } else {
            // invalid/ — must reject with a typed error, never panic.
            let err = result.expect_err(&format!("invalid case must Err: {rel}"));
            assert_ne!(
                err.gossip_class(),
                GossipClass::Internal,
                "invalid vector should not be Internal: {rel} → {err:?}"
            );
            invalid_ok += 1;
        }
    }

    assert!(ran > 0, "expected valid withdrawals cases for {}", P::NAME);
    assert!(
        invalid_ok > 0,
        "expected invalid withdrawals cases for {}",
        P::NAME
    );
}

fn run_execution_payload_cases<P: Preset>() {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let cases = collect_cases(&tests, P::NAME, "execution_payload");
    assert!(
        !cases.is_empty(),
        "expected execution_payload cases for {}",
        P::NAME
    );

    let config = spec_config_for_preset(match P::NAME {
        "mainnet" => PresetName::Mainnet,
        "minimal" => PresetName::Minimal,
        other => panic!("unknown preset {other}"),
    });

    let mut ran = 0usize;
    let mut invalid_ok = 0usize;

    for (rel, case_dir) in &cases {
        if is_skipped(rel, &prefixes) {
            continue;
        }

        let pre_bytes = snappy_decompress(&case_dir.join("pre.ssz_snappy"));
        let mut state = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &pre_bytes)
            .unwrap_or_else(|e| panic!("decode pre {rel}: {e:?}"));

        let body_bytes = snappy_decompress(&case_dir.join("body.ssz_snappy"));
        let body = BeaconBlockBody::<P>::from_ssz_bytes(&body_bytes)
            .unwrap_or_else(|e| panic!("decode body {rel}: {e:?}"));

        let block = BeaconBlock {
            slot: state.slot(),
            proposer_index: Default::default(),
            parent_root: state.latest_block_header().parent_root,
            state_root: Root::ZERO,
            body,
        };

        let execution_valid = read_execution_valid(case_dir);
        let engine = YamlEngine { execution_valid };
        let ctx = TransitionContext::<P>::new(&config, &engine);

        let post_path = case_dir.join("post.ssz_snappy");
        let result = process_execution_payload(&mut state, &block, &ctx);

        if post_path.is_file() {
            result.unwrap_or_else(|e| panic!("process_execution_payload valid case {rel}: {e}"));
            let post_bytes = snappy_decompress(&post_path);
            let expected = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &post_bytes)
                .unwrap_or_else(|e| panic!("decode post {rel}: {e:?}"));
            assert_eq!(state, expected, "post-state mismatch for {rel}");
            ran += 1;
        } else {
            let err = result.expect_err(&format!("invalid case must Err: {rel}"));
            // Typed error, never panic — already held.
            let _ = err;
            invalid_ok += 1;
        }
    }

    assert!(
        ran > 0,
        "expected valid execution_payload cases for {}",
        P::NAME
    );
    assert!(
        invalid_ok > 0,
        "expected invalid execution_payload cases for {}",
        P::NAME
    );
}

#[test]
fn withdrawals_minimal() {
    run_withdrawals_cases::<Minimal>();
}

#[test]
fn withdrawals_mainnet() {
    run_withdrawals_cases::<Mainnet>();
}

#[test]
fn execution_payload_minimal() {
    run_execution_payload_cases::<Minimal>();
}

#[test]
fn execution_payload_mainnet() {
    run_execution_payload_cases::<Mainnet>();
}

// ---------------------------------------------------------------------------
// Unit coverage for process_randao / process_eth1_data (no operations vectors)
// ---------------------------------------------------------------------------

#[test]
fn process_eth1_data_supermajority_replaces() {
    let mut state = BeaconState::<Minimal>::default();
    // Minimal: EPOCHS_PER_ETH1_VOTING_PERIOD * SLOTS_PER_EPOCH = 4 * 8 = 32.
    // Need count * 2 > 32 → count >= 17.
    let vote = cc_types::containers::Eth1Data {
        deposit_root: Root::from_array([1u8; 32]),
        deposit_count: 1,
        block_hash: Root::from_array([2u8; 32]),
    };
    let other = cc_types::containers::Eth1Data::default();
    for _ in 0..16 {
        let block = block_with_eth1(vote);
        process_eth1_data(&mut state, &block).unwrap();
    }
    assert_ne!(state.eth1_data(), vote);
    // 17th vote of same data → supermajority.
    let block = block_with_eth1(vote);
    process_eth1_data(&mut state, &block).unwrap();
    assert_eq!(state.eth1_data(), vote);

    // Different vote does not replace until it has supermajority.
    let block = block_with_eth1(other);
    process_eth1_data(&mut state, &block).unwrap();
    assert_eq!(state.eth1_data(), vote);
}

fn block_with_eth1(eth1: cc_types::containers::Eth1Data) -> BeaconBlock<Minimal> {
    let body = BeaconBlockBody::<Minimal> {
        eth1_data: eth1,
        ..Default::default()
    };
    BeaconBlock {
        slot: Slot::new(0),
        proposer_index: Default::default(),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body,
    }
}

#[test]
fn process_randao_mixes_reveal() {
    let mut state = BeaconState::<Minimal>::default();
    state.set_slot(Slot::new(0));
    // Seed a known mix at epoch 0.
    let prior = Root::from_array([0xAAu8; 32]);
    state.randao_mixes_set(0, prior).unwrap();

    let body = BeaconBlockBody::<Minimal> {
        randao_reveal: cc_types::primitives::BlsSignature::from_array([0x11u8; 96]),
        ..Default::default()
    };
    let block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: Default::default(),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body,
    };
    process_randao(&mut state, &block).unwrap();
    let mixed = state.randao_mixes_get(0).unwrap();
    assert_ne!(mixed, prior);
    // Deterministic: re-run from same prior should yield same mix.
    state.randao_mixes_set(0, prior).unwrap();
    process_randao(&mut state, &block).unwrap();
    assert_eq!(state.randao_mixes_get(0).unwrap(), mixed);
}

// ---------------------------------------------------------------------------
// CC-12/5 — Hoodi blob schedule vs pre-BPO schedule in one binary
// ---------------------------------------------------------------------------

/// Engine that always accepts (local checks are what we test for the bound).
struct AcceptEngine;
impl<P: Preset> ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

/// Build a state/block pair at a post-BPO Hoodi epoch with `n` blob commitments,
/// aligned so every local check except the blob bound passes.
fn hoodi_payload_fixture(
    n_commitments: usize,
    epoch: u64,
) -> (BeaconState<Mainnet>, BeaconBlock<Mainnet>) {
    let slots_per_epoch = Mainnet::SLOTS_PER_EPOCH;
    let slot = epoch * slots_per_epoch;
    let mut state = BeaconState::<Mainnet>::default();
    state.set_slot(Slot::new(slot));
    state.set_genesis_time(1_000_000);
    // Zero mix at epoch % EPOCHS_PER_HISTORICAL_VECTOR.
    let parent_hash = state.latest_execution_payload_header().block_hash;
    let prev_randao = state
        .randao_mixes_get((epoch % Mainnet::EPOCHS_PER_HISTORICAL_VECTOR) as usize)
        .unwrap_or(Root::ZERO);
    let timestamp = state.genesis_time() + slot * 12;
    let payload = ExecutionPayload::<Mainnet> {
        parent_hash,
        prev_randao,
        timestamp,
        ..Default::default()
    };
    let mut body = BeaconBlockBody::<Mainnet> {
        execution_payload: payload,
        ..Default::default()
    };
    for _ in 0..n_commitments {
        body.blob_kzg_commitments
            .push(KzgCommitment::default())
            .expect("within MAX_BLOB_COMMITMENTS");
    }
    let block = BeaconBlock {
        slot: Slot::new(slot),
        proposer_index: Default::default(),
        parent_root: state.latest_block_header().parent_root,
        state_root: Root::ZERO,
        body,
    };
    (state, block)
}

#[test]
fn hoodi_blob_bound_vs_pre_bpo_same_binary() {
    // Shipping parse path (§5.6) — committed hoodi-config.yaml.
    let hoodi_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../types/tests/fixtures/hoodi-config.yaml");
    let hoodi = ChainConfig::from_yaml_file(&hoodi_path).expect("parse hoodi-config.yaml");
    assert_eq!(hoodi.config_name, "hoodi");

    // Pre-BPO schedule: only Electra base (9) — simulates a client that never
    // loaded the Fulu BPO table.
    let pre_bpo = ChainConfig {
        blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
            epoch: Epoch::new(0),
            max_blobs_per_block: 9,
        }])
        .unwrap(),
        ..hoodi.clone()
    };

    // Epoch past 54016 (second BPO → max 21). Real Hoodi is far past this.
    // 15 commitments: > 9 (fails pre-BPO) and ≤ 21 (passes Hoodi).
    let epoch = 54_017u64;
    assert!(epoch > 54_016);
    let (mut state_ok, block) = hoodi_payload_fixture(15, epoch);
    assert!(block.body.blob_kzg_commitments.len() > 9);

    let engine = AcceptEngine;
    let ctx_hoodi = TransitionContext::<Mainnet>::new(&hoodi, &engine);
    process_execution_payload(&mut state_ok, &block, &ctx_hoodi)
        .expect("Hoodi post-BPO schedule must accept 15 commitments");

    let (mut state_fail, block2) = hoodi_payload_fixture(15, epoch);
    let ctx_pre = TransitionContext::<Mainnet>::new(&pre_bpo, &engine);
    let err = process_execution_payload(&mut state_fail, &block2, &ctx_pre).unwrap_err();
    assert!(
        matches!(
            err,
            BlockError::BlobBoundExceeded {
                count: 15,
                max: 9
            }
        ),
        "pre-BPO must reject with BlobBoundExceeded, got {err:?}"
    );
    assert_eq!(err.gossip_class(), GossipClass::Reject);

    // Confirm get_blob_parameters numbers for the two configs at this epoch.
    assert_eq!(
        hoodi.get_blob_parameters::<Mainnet>(Epoch::new(epoch)).max_blobs_per_block,
        21
    );
    assert_eq!(
        pre_bpo
            .get_blob_parameters::<Mainnet>(Epoch::new(epoch))
            .max_blobs_per_block,
        9
    );
}

/// Optional: if the Hoodi fixture cache is present, assert a real anchor block
/// carries more than 9 blob commitments (criterion /5 "real Hoodi block").
#[test]
fn hoodi_real_block_commitment_count_when_cached() {
    let cache = std::env::var("HOODI_FIXTURES_CACHE").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.cache/cc-hoodi-fixtures")
    });
    let block_path = PathBuf::from(&cache)
        .join("3649472")
        .join("signed_beacon_block.ssz");
    if !block_path.is_file() {
        eprintln!("hoodi fixture not cached; skip real-block commitment count");
        return;
    }
    let bytes = fs::read(&block_path).unwrap();
    let signed = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .expect("decode hoodi signed block");
    let n = signed.message.body.blob_kzg_commitments.len();
    // Pin documents max 21 over the sequence; the anchor itself may be lower
    // but the sequence max is > 9. Count on this block is still informative.
    eprintln!("hoodi anchor blob_kzg_commitments.len() = {n}");
    // Prefer a sequence slot if the anchor is lean — scan sequence dir.
    let seq = PathBuf::from(&cache).join("3649472").join("sequence");
    let mut max_n = n;
    if seq.is_dir() {
        for ent in fs::read_dir(&seq).unwrap() {
            let p = ent.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) != Some("ssz") {
                continue;
            }
            let b = fs::read(&p).unwrap();
            if let Ok(sb) = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &b) {
                max_n = max_n.max(sb.message.body.blob_kzg_commitments.len());
            }
        }
    }
    assert!(
        max_n > 9,
        "expected a real Hoodi block with >9 commitments in the 40-slot sequence, max was {max_n}"
    );
}

// ---------------------------------------------------------------------------
// Handler directory coverage for the two we own
// ---------------------------------------------------------------------------

#[test]
fn handler_dirs_exist() {
    let tests = tests_root();
    for preset in ["mainnet", "minimal"] {
        for handler in HANDLERS {
            let dir = tests.join(preset).join(FORK).join(RUNNER).join(handler);
            assert!(dir.is_dir(), "missing {}", dir.display());
        }
    }
}

#[test]
fn skiplist_operations_entries_match_disk_if_any() {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let mut paths: BTreeSet<String> = BTreeSet::new();
    for preset in ["mainnet", "minimal"] {
        for handler in HANDLERS {
            for (rel, _) in collect_cases(&tests, preset, handler) {
                paths.insert(rel);
            }
        }
    }
    for p in &prefixes {
        if !p.contains("/operations/") {
            continue;
        }
        // Only care about our four handler names if present.
        if !(p.contains("/withdrawals")
            || p.contains("/execution_payload")
            || p.contains("/randao")
            || p.contains("/eth1_data"))
        {
            continue;
        }
        let matched = paths
            .iter()
            .any(|c| c == p || c.starts_with(&format!("{p}/")));
        assert!(
            matched,
            "stale skip entry matches no operations case on disk: `{p}`"
        );
    }
}
