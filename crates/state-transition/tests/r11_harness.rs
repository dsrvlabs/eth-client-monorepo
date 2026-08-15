//! R-11 falsifier harness skeleton (S0a-A-01).
//!
//! Loads the committed Hoodi pin's SSZ pair without filling any cache.
//! No production code. Does not run `process_block` (that is S0-A-30,
//! `r11_process_block.rs`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor.rs"]
mod support;

use std::fs;

use support::{
    cache_env_is_set, hoodi_config_path, load_anchor, load_hoodi_config, load_pin,
    resolve_anchor_paths, resolve_anchor_paths_in, CACHE_ENV, FETCH_HINT,
};

/// A missing cache tree must fail with the fetch hint — never invent bytes.
#[test]
fn missing_anchor_ssz_fails_clearly() {
    let dir = std::env::temp_dir().join(format!(
        "cc-r11-hoodi-absent-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");

    let err = resolve_anchor_paths_in(&dir).expect_err("empty cache must fail");
    let msg = err.to_string();
    assert!(
        msg.contains(FETCH_HINT),
        "missing fixture must name `{FETCH_HINT}`, got: {msg}"
    );
    assert!(
        msg.contains("beacon_state.ssz")
            || msg.contains("signed_beacon_block.ssz")
            || msg.contains("slot directory missing"),
        "missing fixture must name the path, got: {msg}"
    );

    let _ = fs::remove_dir_all(&dir);
}

/// This rig must not become a ninth hand-fill of `caches.pubkeys`.
#[test]
fn harness_does_not_hand_fill_pubkey_cache() {
    let src = include_str!("support/anchor.rs");
    for needle in [
        "rebuild_pubkey_cache",
        "top_up_pubkey_cache",
        "pubkeys.insert",
        "caches_mut",
        "from_ssz_bytes_hydrated",
    ] {
        assert!(
            !src.contains(needle),
            "R-11 harness must not contain `{needle}` (eight existing harnesses already hand-fill)"
        );
    }
    assert!(
        src.contains("from_ssz_bytes_with"),
        "decode must use the raw fork-context constructor"
    );
    assert!(
        src.contains("fn pubkey_cache_len"),
        "harness must expose pubkey_cache_len"
    );
}

/// Config side of the fixture is committed and always loadable.
#[test]
fn hoodi_config_yaml_loads() {
    let path = hoodi_config_path();
    assert!(
        path.is_file(),
        "committed hoodi-config.yaml missing at {}",
        path.display()
    );
    let cfg = load_hoodi_config().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(cfg.config_name, "hoodi");
    assert_eq!(cfg.preset_base, cc_types::PresetName::Mainnet);
}

/// Decode the real Hoodi pair when the cache is present; skip when the env is unset
/// (no BeaconState SSZ is committed — see crates/types/tests/fixtures/README.md).
#[test]
fn decoded_hoodi_anchor_exposes_empty_pubkey_cache() {
    if !cache_env_is_set() {
        eprintln!(
            "skip: {CACHE_ENV} unset — Hoodi BeaconState SSZ is not in git; \
             {FETCH_HINT} (see crates/types/tests/fixtures/README.md)"
        );
        return;
    }

    let pin = load_pin().unwrap_or_else(|e| panic!("{e}"));
    let paths = resolve_anchor_paths().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(paths.pin.slot, pin.slot);
    assert_eq!(paths.config_yaml, hoodi_config_path());
    let state_len = fs::metadata(&paths.state_ssz)
        .unwrap_or_else(|e| panic!("stat {}: {e}", paths.state_ssz.display()))
        .len();
    assert!(
        state_len >= pin.state_size.min(150 * 1024 * 1024),
        "on-disk state {state_len} smaller than pin state_size {}",
        pin.state_size
    );

    let loaded = load_anchor().unwrap_or_else(|e| panic!("{e}"));
    let block_root = loaded.block.canonical_root();
    let expected_block = pin.block_root.trim_start_matches("0x");
    let actual_block: String = block_root
        .as_slice()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(actual_block, expected_block, "decoded block root");
    assert!(
        pin.state_root.starts_with("0x") && pin.state_root.len() == 66,
        "committed state_root must be 0x + 32-byte hex"
    );
    assert_eq!(
        loaded.block.message.slot.as_u64(),
        pin.slot,
        "decoded block slot must match the committed pin"
    );
    assert_eq!(
        loaded.state.slot().as_u64(),
        pin.slot,
        "decoded state slot must match the committed pin"
    );
    assert!(
        loaded.validators_len() > 0,
        "Hoodi registry must be non-empty"
    );
    assert_eq!(
        loaded.pubkey_cache_len(),
        0,
        "SSZ decode must leave caches.pubkeys empty; got {}",
        loaded.pubkey_cache_len()
    );
}
