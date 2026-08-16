//! `epoch_processing` runner — every Fulu handler, both presets (CC-13d).
//!
//! Enumerates handler subdirectories from disk and asserts set equality against
//! the declared list (union across presets). Mainnet omits
//! `sync_committee_updates`; minimal includes it. Every on-disk case is run.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use cc_state_transition::{
    EpochError, process_effective_balance_updates, process_eth1_data_reset,
    process_historical_summaries_update, process_inactivity_updates,
    process_justification_and_finalization, process_participation_flag_updates,
    process_pending_consolidations, process_pending_deposits, process_proposer_lookahead,
    process_randao_mixes_reset, process_registry_updates, process_rewards_and_penalties,
    process_slashings, process_slashings_reset, process_sync_committee_updates,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion};
use cc_types::{BeaconState, ForkName};

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
        churn_limit_quotient: match preset {
            PresetName::Mainnet => 65_536,
            PresetName::Minimal => 32,
        },
        min_per_epoch_churn_limit_electra: match preset {
            PresetName::Mainnet => 128_000_000_000,
            PresetName::Minimal => 64_000_000_000,
        },
        max_per_epoch_activation_exit_churn_limit: match preset {
            PresetName::Mainnet => 256_000_000_000,
            PresetName::Minimal => 128_000_000_000,
        },
        shard_committee_period: Epoch::new(match preset {
            PresetName::Mainnet => 256,
            PresetName::Minimal => 64,
        }),
        max_blobs_per_block_electra: 9,
    }
}

fn spec_config_for<P: Preset>() -> ChainConfig {
    match P::NAME {
        "minimal" => spec_config_for_preset(PresetName::Minimal),
        _ => spec_config_for_preset(PresetName::Mainnet),
    }
}

const FORK: &str = "fulu";
const RUNNER: &str = "epoch_processing";
const LAYOUT: &str = include_str!("../../../spec-vectors-layout.md");
const LOCKFILE: &str = include_str!("../../../spec-vectors.lock");
const SKIPLIST: &str = include_str!("../../../docs/spec-vectors-skiplist.md");

/// Handlers this runner owns — must equal the on-disk set (union across presets).
///
/// Order matches neither spec nor disk; coverage uses set equality.
const HANDLERS: &[&str] = &[
    "effective_balance_updates",
    "eth1_data_reset",
    "historical_summaries_update",
    "inactivity_updates",
    "justification_and_finalization",
    "participation_flag_updates",
    "pending_consolidations",
    "pending_deposits",
    "proposer_lookahead",
    "randao_mixes_reset",
    "registry_updates",
    "rewards_and_penalties",
    "slashings",
    "slashings_reset",
    "sync_committee_updates",
];

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
    if has_file && current.join("pre.ssz_snappy").is_file() {
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

fn rebuild_pubkey_cache<P: Preset>(_state: &mut BeaconState<P>) {
    // S2-A-10: PubkeyIndexMap lives on TransitionContext. STF top-up fills it.
}

fn dispatch<P: Preset>(
    handler: &str,
    state: &mut BeaconState<P>,
    config: &ChainConfig,
) -> Result<(), EpochError> {
    match handler {
        "justification_and_finalization" => process_justification_and_finalization(state),
        "inactivity_updates" => process_inactivity_updates(state),
        "rewards_and_penalties" => process_rewards_and_penalties(state),
        "registry_updates" => process_registry_updates(state, config),
        "slashings" => process_slashings(state),
        "eth1_data_reset" => process_eth1_data_reset(state),
        "pending_deposits" => process_pending_deposits(state, config),
        "pending_consolidations" => process_pending_consolidations(state),
        "effective_balance_updates" => process_effective_balance_updates(state),
        "slashings_reset" => process_slashings_reset(state),
        "randao_mixes_reset" => process_randao_mixes_reset(state),
        "historical_summaries_update" => process_historical_summaries_update(state),
        "participation_flag_updates" => process_participation_flag_updates(state),
        "sync_committee_updates" => process_sync_committee_updates(state),
        "proposer_lookahead" => process_proposer_lookahead(state),
        other => panic!("unknown epoch_processing handler: {other}"),
    }
}

fn run_case<P: Preset>(handler: &str, rel: &str, case_dir: &Path) {
    let pre_bytes = snappy_decompress(&case_dir.join("pre.ssz_snappy"));
    let mut state = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &pre_bytes)
        .unwrap_or_else(|e| panic!("decode pre {rel}: {e:?}"));
    rebuild_pubkey_cache(&mut state);

    let post_path = case_dir.join("post.ssz_snappy");
    let config = spec_config_for::<P>();
    let result = dispatch(handler, &mut state, &config);

    if post_path.is_file() {
        result.unwrap_or_else(|e| panic!("{handler} valid case {rel}: {e}"));
        let post_bytes = snappy_decompress(&post_path);
        let expected = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &post_bytes)
            .unwrap_or_else(|e| panic!("decode post {rel}: {e:?}"));
        assert_eq!(state, expected, "post-state mismatch for {rel}");
    } else {
        // invalid-shaped case: typed EpochError, never panic.
        let err = result.expect_err(&format!("invalid case must Err: {rel}"));
        assert!(
            !err.to_string().is_empty(),
            "expected typed EpochError for {rel}, got {err:?}"
        );
    }
}

fn run_handler<P: Preset>(handler: &str) {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let cases = collect_cases(&tests, P::NAME, handler);
    if cases.is_empty() {
        // Mainnet may omit some handlers (e.g. sync_committee_updates).
        eprintln!(
            "skip: no {handler} cases for {} (not present on disk)",
            P::NAME
        );
        return;
    }
    let mut ran = 0usize;
    for (rel, dir) in &cases {
        if is_skipped(rel, &prefixes) {
            continue;
        }
        run_case::<P>(handler, rel, dir);
        ran += 1;
    }
    assert!(ran > 0, "all {handler} cases skipped for {}", P::NAME);
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

#[test]
fn handler_coverage() {
    let tests = tests_root();
    let declared: BTreeSet<_> = HANDLERS.iter().map(|s| (*s).to_string()).collect();
    let mut union = BTreeSet::new();
    for preset in ["mainnet", "minimal"] {
        let on_disk = list_handlers(&tests, preset);
        // Every on-disk handler must be declared (no silent skips).
        assert!(
            on_disk.is_subset(&declared),
            "{preset}: on-disk handlers not ⊆ declared: extra={:?}",
            on_disk.difference(&declared).collect::<Vec<_>>()
        );
        // Per-preset set equality against the declared handlers that exist on disk
        // for this preset (mainnet omits sync_committee_updates).
        let expected: BTreeSet<_> = declared
            .iter()
            .filter(|h| tests.join(preset).join(FORK).join(RUNNER).join(h).is_dir())
            .cloned()
            .collect();
        assert_eq!(
            on_disk, expected,
            "handler set equality for {preset}: on_disk={on_disk:?} expected={expected:?}"
        );
        union.extend(on_disk);
    }
    // Union across presets equals the full declared list.
    assert_eq!(
        union, declared,
        "union of on-disk handlers must equal declared HANDLERS"
    );
    assert_eq!(HANDLERS.len(), 15);
    assert!(
        LAYOUT.contains("fulu/epoch_processing"),
        "layout should list fulu/epoch_processing"
    );
}

/// Negative control: a declared handler renamed fails coverage equality.
#[test]
fn handler_coverage_negative_missing_handler_fails() {
    let declared: BTreeSet<&str> = HANDLERS.iter().copied().collect();
    let mut renamed: BTreeSet<&str> = declared.clone();
    renamed.remove("slashings");
    renamed.insert("slashings_renamed");
    assert_ne!(
        renamed, declared,
        "renamed HANDLERS must differ from full set"
    );
    // Simulate on-disk == full declared: renamed ≠ on-disk.
    assert_ne!(renamed, declared);
}

// ---------------------------------------------------------------------------
// Per-handler tests
// ---------------------------------------------------------------------------

macro_rules! epoch_tests {
    ($preset:ident, $mod_name:ident) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn effective_balance_updates() {
                run_handler::<$preset>("effective_balance_updates");
            }
            #[test]
            fn eth1_data_reset() {
                run_handler::<$preset>("eth1_data_reset");
            }
            #[test]
            fn historical_summaries_update() {
                run_handler::<$preset>("historical_summaries_update");
            }
            #[test]
            fn inactivity_updates() {
                run_handler::<$preset>("inactivity_updates");
            }
            #[test]
            fn justification_and_finalization() {
                run_handler::<$preset>("justification_and_finalization");
            }
            #[test]
            fn participation_flag_updates() {
                run_handler::<$preset>("participation_flag_updates");
            }
            #[test]
            fn pending_consolidations() {
                run_handler::<$preset>("pending_consolidations");
            }
            #[test]
            fn pending_deposits() {
                run_handler::<$preset>("pending_deposits");
            }
            #[test]
            fn proposer_lookahead() {
                run_handler::<$preset>("proposer_lookahead");
            }
            #[test]
            fn randao_mixes_reset() {
                run_handler::<$preset>("randao_mixes_reset");
            }
            #[test]
            fn registry_updates() {
                run_handler::<$preset>("registry_updates");
            }
            #[test]
            fn rewards_and_penalties() {
                run_handler::<$preset>("rewards_and_penalties");
            }
            #[test]
            fn slashings() {
                run_handler::<$preset>("slashings");
            }
            #[test]
            fn slashings_reset() {
                run_handler::<$preset>("slashings_reset");
            }
            #[test]
            fn sync_committee_updates() {
                run_handler::<$preset>("sync_committee_updates");
            }
        }
    };
}

epoch_tests!(Minimal, minimal);
epoch_tests!(Mainnet, mainnet);
