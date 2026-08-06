//! `sanity` runner — **slots** handler green for both presets (CC-12a).
//!
//! Does **not** depend on `cc-spec-tests` (crate DAG: state-transition may only
//! edge to `{cc-types, cc-crypto}`). Vector cache layout and readiness markers
//! match Architecture §10.1 / the committed `spec-vectors.lock`.
//!
//! The `blocks` handler is skiplisted until CC-12b–d (emptied at CC-12e).
//! Epoch-crossing `slots` cases are skiplisted under CC-13 until `process_epoch`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use cc_state_transition::process_slots;
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::Slot;
use cc_types::{BeaconState, ForkName};
use ssz::Encode;

const FORK: &str = "fulu";
const RUNNER: &str = "sanity";
const LOCKFILE: &str = include_str!("../../../spec-vectors.lock");
const SKIPLIST: &str = include_str!("../../../docs/spec-vectors-skiplist.md");

/// Handlers this runner owns.
const HANDLERS: &[&str] = &["blocks", "slots"];

// ---------------------------------------------------------------------------
// Minimal vector-cache helpers (mirrors cc-spec-tests readiness, no workspace edge)
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

/// Paths relative to `tests/` that are skiplisted (prefix match).
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
        // `- `path` -- reason -- CC-XXy`
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

/// Collect case directories under `tests/<preset>/fulu/sanity/<handler>/`.
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

// ---------------------------------------------------------------------------
// slots runner
// ---------------------------------------------------------------------------

fn run_slots_cases<P: Preset>() {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let cases = collect_cases(&tests, P::NAME, "slots");
    assert!(
        !cases.is_empty(),
        "expected slots cases for {}",
        P::NAME
    );

    let mut ran = 0usize;
    let mut skipped = 0usize;

    for (rel, case_dir) in &cases {
        if is_skipped(rel, &prefixes) {
            skipped += 1;
            continue;
        }

        let pre_path = case_dir.join("pre.ssz_snappy");
        let pre_bytes = snappy_decompress(&pre_path);
        let mut state = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &pre_bytes)
            .unwrap_or_else(|e| panic!("decode pre {rel}: {e:?}"));

        let slots_text = fs::read_to_string(case_dir.join("slots.yaml"))
            .unwrap_or_else(|e| panic!("slots.yaml {rel}: {e}"));
        let slots: u64 = serde_yaml::from_str(&slots_text)
            .unwrap_or_else(|e| panic!("parse slots.yaml {rel}: {e}"));
        let target = state
            .slot()
            .checked_add(slots)
            .unwrap_or_else(|| panic!("slot overflow {rel}"));

        process_slots(&mut state, Slot::new(target.as_u64()))
            .unwrap_or_else(|e| panic!("process_slots {rel}: {e}"));

        let post_path = case_dir.join("post.ssz_snappy");
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
    }

    assert!(
        ran > 0,
        "expected to execute at least one non-skipped slots case for {}",
        P::NAME
    );
    assert!(
        skipped > 0,
        "expected some slots cases skiplisted under CC-13 for {}",
        P::NAME
    );
}

#[test]
fn slots_minimal() {
    run_slots_cases::<Minimal>();
}

#[test]
fn slots_mainnet() {
    run_slots_cases::<Mainnet>();
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

#[test]
fn skiplist_sanity_entries_match_disk() {
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
        if !p.contains("/sanity/") {
            continue;
        }
        let matched = paths
            .iter()
            .any(|c| c == p || c.starts_with(&format!("{p}/")));
        assert!(
            matched,
            "stale skip entry matches no sanity case on disk: `{p}`"
        );
    }
}
