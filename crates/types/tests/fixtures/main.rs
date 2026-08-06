//! Hoodi fixture rig tests (CC-10b).
//!
//! - Parent-link assertion over committed manifests alone (no network, no cache).
//! - Helper absent / corrupt-SHA behaviour.
//! - Cache-dependent open is **skipped** when `HOODI_FIXTURES_CACHE` is unset
//!   so CI without the fixture cache stays green.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "mod.rs"]
mod fixtures;

use std::fs;
use std::path::PathBuf;

use cc_types::{ForkName, Mainnet, SignedBeaconBlock};

use fixtures::{
    cache_env_is_set, ci_cache_key, load_anchor, load_sequence, resolve_cache_root, Error,
    HoodiFixtures, FETCH_HINT,
};

/// Parent-linked sequence over the committed manifest alone — no network, no cache.
#[test]
fn sequence_parent_linked_manifest_only() {
    let seq = load_sequence().expect("load hoodi-sequence.toml");
    assert_eq!(seq.slots.len(), 40, "sequence must be exactly 40 slots");
    assert_eq!(
        seq.slots.last().map(|s| s.slot),
        Some(seq.anchor_slot),
        "last slot must be the anchor"
    );
    assert_eq!(
        seq.slots.first().map(|s| s.slot),
        Some(seq.start_slot),
        "first slot must equal start_slot"
    );

    // Consecutive slot numbers.
    for w in seq.slots.windows(2) {
        assert_eq!(
            w[1].slot,
            w[0].slot + 1,
            "slots must be consecutive: {} then {}",
            w[0].slot,
            w[1].slot
        );
    }

    // Parent-link over non-empty slots; empty slots explicitly marked.
    let mut prev_root: Option<&str> = None;
    let mut saw_empty = false;
    let mut max_blobs = 0u64;
    for entry in &seq.slots {
        if entry.empty {
            saw_empty = true;
            assert!(
                entry.root.is_empty(),
                "empty slot {} must have empty root",
                entry.slot
            );
            assert_eq!(
                entry.blob_commitment_count, 0,
                "empty slot {} blob count",
                entry.slot
            );
            continue;
        }
        assert!(
            !entry.root.is_empty(),
            "non-empty slot {} must have a root",
            entry.slot
        );
        assert!(
            !entry.parent_root.is_empty(),
            "non-empty slot {} must have parent_root",
            entry.slot
        );
        if let Some(prev) = prev_root {
            assert_eq!(
                entry.parent_root, prev,
                "parent-link broken at slot {}: parent_root {} != previous root {}",
                entry.slot, entry.parent_root, prev
            );
        }
        prev_root = Some(entry.root.as_str());
        max_blobs = max_blobs.max(entry.blob_commitment_count);
    }

    // Empty slots are expected on a live chain; if the pin has none, still ok
    // but record for operators.
    let _ = saw_empty;

    let anchor = load_anchor().expect("load hoodi-anchor.toml");
    assert!(
        anchor.epoch > 54016,
        "anchor epoch {} must be > 54016",
        anchor.epoch
    );
    assert!(
        max_blobs > 9,
        "max blob_commitment_count {max_blobs} must be > 9"
    );
    assert_eq!(
        seq.max_blob_commitment_count, max_blobs,
        "manifest max_blob_commitment_count must match scan"
    );
    assert_eq!(anchor.slot, seq.anchor_slot);
    assert_eq!(anchor.max_blob_commitment_count, max_blobs);
    assert_eq!(anchor.sequence_len, 40);
    assert!(
        anchor.state_size >= 150 * 1024 * 1024,
        "recorded state_size {} < 150 MB",
        anchor.state_size
    );

    // CI cache key shape.
    let key = ci_cache_key(&anchor);
    assert!(
        key.starts_with(&format!("hoodi-fixtures-{}-", anchor.slot)),
        "cache key shape: {key}"
    );
    assert!(
        key.ends_with(&anchor.block_sha256),
        "cache key must end with full block sha: {key}"
    );
}

#[test]
fn helper_absent_cache_contains_fetch_hint() {
    let dir = std::env::temp_dir().join(format!(
        "cc-hoodi-fixtures-absent-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");

    let err = HoodiFixtures::open_in(&dir).expect_err("empty cache must fail");
    let msg = err.to_string();
    assert!(
        msg.contains(FETCH_HINT),
        "Display must contain `{FETCH_HINT}`, got: {msg}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn helper_corrupt_artifact_names_expected_and_actual_sha256() {
    let anchor = load_anchor().expect("anchor");
    let sequence = load_sequence().expect("sequence");

    let dir = std::env::temp_dir().join(format!(
        "cc-hoodi-fixtures-corrupt-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    let slot_dir = dir.join(anchor.slot.to_string());
    fs::create_dir_all(slot_dir.join("sequence")).expect("mkdir");

    // Write a block with known wrong contents; state file present but also wrong.
    let bad_block = b"not-a-real-signed-beacon-block";
    fs::write(slot_dir.join("signed_beacon_block.ssz"), bad_block).expect("write block");
    // State must exist for the helper to reach the block check first — write
    // anything; the block SHA mismatch is what we assert.
    fs::write(slot_dir.join("beacon_state.ssz"), b"tiny-state").expect("write state");

    let err = HoodiFixtures::open_with_manifests(&dir, anchor.clone(), sequence)
        .expect_err("corrupt block must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("signed_beacon_block.ssz") || msg.contains("SHA256"),
        "must name the artifact, got: {msg}"
    );
    assert!(
        msg.contains(&anchor.block_sha256),
        "must name expected sha {}, got: {msg}",
        anchor.block_sha256
    );
    // actual digest of bad_block
    let actual = {
        use sha2::{Digest, Sha256};
        let d = Sha256::digest(bad_block);
        d.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    assert!(
        msg.contains(&actual),
        "must name actual sha {actual}, got: {msg}"
    );
    assert!(
        msg.contains(FETCH_HINT),
        "must contain fetch hint, got: {msg}"
    );

    // Also exercise the typed variant.
    match err {
        Error::Sha256Mismatch {
            expected, actual: a, ..
        } => {
            assert_eq!(expected, anchor.block_sha256);
            assert_eq!(a, actual);
        }
        other => panic!("expected Sha256Mismatch, got {other}"),
    }

    let _ = fs::remove_dir_all(&dir);
}

/// CC-10d: decode a committed Hoodi `SignedBeaconBlock` via
/// `from_ssz_bytes_with(ForkName::Fulu, …)` and check the block root.
///
/// Skips when `HOODI_FIXTURES_CACHE` is unset (same contract as other cache tests).
#[test]
fn signed_beacon_block_fulu_decode_matches_anchor_root() {
    if !cache_env_is_set() {
        eprintln!(
            "skip: {env} unset — Hoodi SSZ cache not required for this run \
             (see crates/types/tests/fixtures/README.md)",
            env = fixtures::CACHE_ENV
        );
        return;
    }

    let root = resolve_cache_root().expect("resolve cache root");
    let fixtures = HoodiFixtures::open_in(&root).unwrap_or_else(|e| {
        panic!("{e}");
    });

    let bytes = fs::read(&fixtures.block_ssz).expect("read signed_beacon_block.ssz");
    let signed = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("SSZ decode failed: {e:?}"));

    assert_eq!(
        signed.message.slot.as_u64(),
        fixtures.anchor.slot,
        "decoded slot must match anchor"
    );

    let block_root = signed.canonical_root();
    let expected = fixtures.anchor.block_root.trim_start_matches("0x");
    let actual = format!("{block_root:x}");
    // Hash256 Display/Debug may vary; compare lower-hex of 32 bytes.
    let actual_hex: String = block_root
        .as_slice()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        actual_hex, expected,
        "canonical block root mismatch (display was {actual})"
    );
}

/// Opens the real cache when `HOODI_FIXTURES_CACHE` is set; otherwise skips.
///
/// CI without the restored cache leaves the env unset so this test does not
/// fail the suite. Locally: `export HOODI_FIXTURES_CACHE=$HOME/.cache/cc-hoodi-fixtures`
/// after `bash scripts/fetch-hoodi-fixtures.sh`.
#[test]
fn open_real_cache_when_env_set() {
    if !cache_env_is_set() {
        eprintln!(
            "skip: {env} unset — Hoodi SSZ cache not required for this run \
             (see crates/types/tests/fixtures/README.md)",
            env = fixtures::CACHE_ENV
        );
        return;
    }

    let root = resolve_cache_root().expect("resolve cache root");
    let fixtures = HoodiFixtures::open_in(&root).unwrap_or_else(|e| {
        panic!("{e}");
    });

    assert!(fixtures.block_ssz.is_file());
    assert!(fixtures.state_ssz.is_file());
    let state_len = fs::metadata(&fixtures.state_ssz).unwrap().len();
    assert!(
        state_len >= 150 * 1024 * 1024,
        "state size {state_len} < 150 MB"
    );

    // At least one non-empty sequence SSZ on disk.
    let nonempty: Vec<_> = fixtures
        .sequence
        .slots
        .iter()
        .filter(|s| !s.empty)
        .collect();
    assert!(!nonempty.is_empty());
    let path = fixtures
        .sequence_block_ssz(nonempty[0].slot)
        .expect("path for nonempty slot");
    assert!(
        path.is_file(),
        "sequence SSZ missing at {}",
        path.display()
    );
}

/// Manifests directory contains no SSZ / large blobs (git hygiene).
#[test]
fn fixtures_dir_has_no_large_files() {
    let dir = fixtures::manifests_dir();
    let mut large = Vec::new();
    walk_check(&dir, &mut large);
    assert!(
        large.is_empty(),
        "fixtures dir must not contain files > 64 KiB (no SSZ in git): {large:?}"
    );
}

fn walk_check(dir: &PathBuf, large: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for ent in entries.flatten() {
        let path = ent.path();
        if path.is_dir() {
            // Skip the module source tree's target-like noise; only data files.
            if path.file_name().and_then(|s| s.to_str()) == Some("mod.rs") {
                continue;
            }
            walk_check(&path, large);
            continue;
        }
        // Ignore Rust sources next to the manifests.
        if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) == Some("md") {
            continue;
        }
        if let Ok(meta) = ent.metadata()
            && meta.len() > 64 * 1024
        {
            large.push(path);
        }
    }
}
