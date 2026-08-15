//! `random` runner — adversarially generated multi-block cases (CC-12e).
//!
//! Same shape as `sanity/blocks`: `pre` + `blocks_N` sequence + optional `post`,
//! driven by [`state_transition`]. Handler on disk is the suite name `random`
//! under `tests/<preset>/fulu/random/random/`.
//!
//! Epoch-crossing cases run through assembled `process_epoch` (CC-13d).
//! Does **not** depend on `cc-spec-tests` (crate DAG).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use cc_state_transition::{
    BlockError, BlockSignatureStrategy, EngineError, ExecutionEngine, GossipClass,
    NewPayloadRequest, PayloadStatus, TransitionContext, state_transition,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion};
use cc_types::{BeaconState, ForkName, SignedBeaconBlock};
use ssz::Encode;

const FORK: &str = "fulu";
const RUNNER: &str = "random";
const LOCKFILE: &str = include_str!("../../../spec-vectors.lock");
const SKIPLIST: &str = include_str!("../../../docs/spec-vectors-skiplist.md");

/// Handlers this runner owns (`tests/<preset>/fulu/random/<handler>/`).
const HANDLERS: &[&str] = &["random"];

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
    assert!(
        handler_dir.is_dir(),
        "missing handler dir {}",
        handler_dir.display()
    );
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

fn list_handlers(tests: &Path, preset: &str) -> BTreeSet<String> {
    let dir = tests.join(preset).join(FORK).join(RUNNER);
    let mut set = BTreeSet::new();
    for ent in fs::read_dir(&dir).unwrap() {
        let ent = ent.unwrap();
        if ent.file_type().unwrap().is_dir() {
            set.insert(ent.file_name().to_string_lossy().into_owned());
        }
    }
    set
}

fn rebuild_pubkey_cache<P: Preset>(state: &mut BeaconState<P>) {
    let pk_entries: Vec<_> = state
        .validators_iter()
        .enumerate()
        .map(|(i, v)| {
            (
                v.pubkey,
                cc_types::primitives::ValidatorIndex::new(i as u64),
            )
        })
        .collect();
    for (pk, idx) in pk_entries {
        state.caches_mut().pubkeys.insert(pk, idx);
    }
}

fn spec_config_for_preset(preset: PresetName) -> ChainConfig {
    let (name, seconds, genesis, altair, bellatrix, capella, deneb, electra, fulu) = match preset {
        PresetName::Mainnet => (
            "mainnet",
            12u64,
            [0x00, 0x00, 0x00, 0x00],
            [0x01, 0x00, 0x00, 0x00],
            [0x02, 0x00, 0x00, 0x00],
            [0x03, 0x00, 0x00, 0x00],
            [0x04, 0x00, 0x00, 0x00],
            [0x05, 0x00, 0x00, 0x00],
            [0x06, 0x00, 0x00, 0x00],
        ),
        PresetName::Minimal => (
            "minimal",
            6u64,
            [0x00, 0x00, 0x00, 0x01],
            [0x01, 0x00, 0x00, 0x01],
            [0x02, 0x00, 0x00, 0x01],
            [0x03, 0x00, 0x00, 0x01],
            [0x04, 0x00, 0x00, 0x01],
            [0x05, 0x00, 0x00, 0x01],
            [0x06, 0x00, 0x00, 0x01],
        ),
    };
    ChainConfig {
        preset_base: preset,
        config_name: name.into(),
        genesis_fork_version: ForkVersion::from_array(genesis),
        altair_fork_version: ForkVersion::from_array(altair),
        altair_fork_epoch: Epoch::new(0),
        bellatrix_fork_version: ForkVersion::from_array(bellatrix),
        bellatrix_fork_epoch: Epoch::new(0),
        capella_fork_version: ForkVersion::from_array(capella),
        capella_fork_epoch: Epoch::new(0),
        deneb_fork_version: ForkVersion::from_array(deneb),
        deneb_fork_epoch: Epoch::new(0),
        electra_fork_version: ForkVersion::from_array(electra),
        electra_fork_epoch: Epoch::new(0),
        fulu_fork_version: ForkVersion::from_array(fulu),
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

fn read_meta_u64(case_dir: &Path, key: &str, default: u64) -> u64 {
    let path = case_dir.join("meta.yaml");
    if !path.is_file() {
        return default;
    }
    let text = fs::read_to_string(&path).unwrap();
    if let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(&text)
        && let Some(n) = v.get(key).and_then(|x| x.as_u64())
    {
        return n;
    }
    default
}

fn assert_typed_reject(err: &BlockError, rel: &str) {
    assert_ne!(
        err.gossip_class(),
        GossipClass::Internal,
        "invalid vector should not be Internal: {rel} → {err:?}"
    );
    assert!(!err.to_string().is_empty(), "empty error for {rel}");
}

#[derive(Debug, Clone, Copy)]
struct AcceptEngine;

impl<P: Preset> ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

// ---------------------------------------------------------------------------
// random handler (blocks sequence)
// ---------------------------------------------------------------------------

fn run_random_cases<P: Preset>() {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let cases = collect_cases(&tests, P::NAME, "random");
    assert!(!cases.is_empty(), "expected random cases for {}", P::NAME);

    let config = spec_config_for_preset(match P::NAME {
        "mainnet" => PresetName::Mainnet,
        "minimal" => PresetName::Minimal,
        other => panic!("unknown preset {other}"),
    });
    let engine = AcceptEngine;
    let ctx = TransitionContext::<P>::new(&config, &engine);

    let mut ran = 0usize;
    let mut skipped = 0usize;

    for (rel, case_dir) in &cases {
        if is_skipped(rel, &prefixes) {
            skipped += 1;
            continue;
        }

        let pre_bytes = snappy_decompress(&case_dir.join("pre.ssz_snappy"));
        let mut state = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &pre_bytes)
            .unwrap_or_else(|e| panic!("decode pre {rel}: {e:?}"));
        rebuild_pubkey_cache(&mut state);

        let blocks_count = read_meta_u64(case_dir, "blocks_count", 1);
        let bls_setting = read_meta_u64(case_dir, "bls_setting", 1) as u8;
        let strategy = BlockSignatureStrategy::from_bls_setting_u8(bls_setting);

        let post_path = case_dir.join("post.ssz_snappy");
        let mut first_err: Option<BlockError> = None;

        for i in 0..blocks_count {
            let block_path = case_dir.join(format!("blocks_{i}.ssz_snappy"));
            let block_bytes = snappy_decompress(&block_path);
            let signed = SignedBeaconBlock::<P>::from_ssz_bytes_with(ForkName::Fulu, &block_bytes)
                .unwrap_or_else(|e| panic!("decode blocks_{i} {rel}: {e:?}"));

            match state_transition(&mut state, &signed, &ctx, strategy) {
                Ok(()) => {}
                Err(e) => {
                    first_err = Some(e);
                    break;
                }
            }
            rebuild_pubkey_cache(&mut state);
        }

        if post_path.is_file() {
            if let Some(e) = first_err {
                panic!("random valid case {rel}: {e}");
            }
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
            let err = first_err.unwrap_or_else(|| {
                panic!("invalid random case must Err: {rel}");
            });
            assert_typed_reject(&err, rel);
            ran += 1;
        }
    }

    assert!(
        ran > 0,
        "expected to execute random cases for {} (ran={ran}, skipped={skipped})",
        P::NAME
    );
}

#[test]
fn random_minimal() {
    run_random_cases::<Minimal>();
}

#[test]
fn random_mainnet() {
    run_random_cases::<Mainnet>();
}

#[test]
fn handler_coverage() {
    let tests = tests_root();
    for preset in ["mainnet", "minimal"] {
        let on_disk = list_handlers(&tests, preset);
        let declared: BTreeSet<&str> = HANDLERS.iter().copied().collect();
        let on_disk_refs: BTreeSet<&str> = on_disk.iter().map(String::as_str).collect();
        assert_eq!(
            declared, on_disk_refs,
            "handler coverage mismatch for {preset}: declared={declared:?} on_disk={on_disk_refs:?}"
        );
    }
}

/// Negative control: empty HANDLERS fails coverage equality.
#[test]
fn handler_coverage_negative_missing_handler_fails() {
    let on_disk: BTreeSet<String> = HANDLERS.iter().map(|s| (*s).to_string()).collect();
    let partial: BTreeSet<&str> = BTreeSet::new();
    let on_disk_refs: BTreeSet<&str> = on_disk.iter().map(String::as_str).collect();
    assert_ne!(
        partial, on_disk_refs,
        "empty HANDLERS must differ from full set"
    );
}

#[test]
fn skiplist_random_entries_match_disk() {
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
        if !p.contains("/random/") {
            continue;
        }
        let matched = paths
            .iter()
            .any(|c| c == p || c.starts_with(&format!("{p}/")));
        assert!(
            matched,
            "stale skip entry matches no random case on disk: `{p}`"
        );
    }
}
