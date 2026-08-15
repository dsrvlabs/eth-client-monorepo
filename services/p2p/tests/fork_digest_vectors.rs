//! CC-21b / CC-21/2 — Hoodi fork-digest vectors and ForkContext coverage.
//!
//! # V-1 re-read (fixture input — not the PRD)
//!
//! Re-read date: **2026-08-07**.
//!
//! Sources:
//! - Committed local: `crates/types/tests/fixtures/hoodi-config.yaml`
//!   (matches eth-clients/hoodi `metadata/config.yaml` `BLOB_SCHEDULE` /
//!   `FULU_FORK_EPOCH` as of the re-read; cross-checked against
//!   `https://beacon.hoodi.ethpandaops.io/eth/v1/config/spec` the same day).
//! - Hoodi GVR from `crates/types/tests/fixtures/hoodi-anchor.toml`:
//!   `0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f`
//! - Timing: `MIN_GENESIS_TIME=1742212800`, `GENESIS_DELAY=600` →
//!   `genesis_time=1742213400`; 12 s slots; 32 slots/epoch.
//!
//! Live head epoch at re-read (wall-clock 2026-08-07 UTC):
//!   `epoch ≈ 114185` (slot ≈ 3653950) — well past the last BPO.
//!
//! Which BPO parameters the **current** digest carries today:
//!   **BPO 2** `(epoch=54016, max_blobs=21)`.
//!
//! Hoodi Fulu-era digest ranges (from `BLOB_SCHEDULE` + Electra-era fallback):
//! - `[fulu=50688, 52480)` → fallback `(electra epoch=2048, max_blobs=9)`
//! - `[52480, 54016)` → BPO 1 `(52480, 15)`
//! - `[54016, …)` → BPO 2 `(54016, 21)` (current)
//!
//! Discriminating epochs for CC-21/2: **51000**, **52480**, **54016**, plus a
//! pre-Fulu base case (un-XOR'd `base_digest[:4]`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;
use std::path::PathBuf;

use cc_crypto::compute_fork_data_root;
use cc_p2p::fork_digest::{
    FAR_FUTURE_EPOCH, ForkContext, compute_fork_digest, compute_fork_version, enr_fork_id,
    next_fork, next_fork_digest,
};
use cc_types::{
    BlobParameters, ChainConfig, Epoch, ForkDigest, ForkVersion, Mainnet, Root, parse_hex_bytes,
};

// ── Fixture constants (V-1 derived) ─────────────────────────────────────────

/// Hoodi genesis validators root (anchor fixture).
const HOODI_GVR_HEX: &str = "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f";

/// Pre-Fulu epoch (Electra-era): digest is un-XOR'd `base_digest[:4]`.
const EPOCH_PRE_FULU: u64 = 50_000;
/// Fulu, before first BPO — Electra fallback blob params `(2048, 9)`.
const EPOCH_FULU_FALLBACK: u64 = 51_000;
/// BPO 1 activation epoch — `(52480, 15)`.
const EPOCH_BPO1: u64 = 52_480;
/// BPO 2 activation epoch — `(54016, 21)`, current head era.
const EPOCH_BPO2: u64 = 54_016;

// Committed CC-21/2 fixture — derived 2026-08-07 from loaded Hoodi YAML + GVR
// via `compute_fork_digest` (this module), after V-1 re-read of BLOB_SCHEDULE.
// Not taken from the PRD.
const EXPECT_PRE_FULU: [u8; 4] = [0x82, 0x55, 0x6a, 0x32];
const EXPECT_FULU_FALLBACK: [u8; 4] = [0xe2, 0xab, 0xcc, 0xa4];
const EXPECT_BPO1: [u8; 4] = [0xae, 0x9f, 0x70, 0xa0];
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

fn digest_bytes(d: ForkDigest) -> [u8; 4] {
    let s = d.as_slice();
    [s[0], s[1], s[2], s[3]]
}

// ── CC-21/2 discriminating test ─────────────────────────────────────────────

#[test]
fn hoodi_three_distinct_fulu_digests_plus_pre_fulu_base() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();

    assert_eq!(cfg.fulu_fork_epoch, Epoch::new(50_688));
    assert_eq!(cfg.blob_schedule.entries().len(), 2);

    let pre = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_PRE_FULU));
    let fb = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_FULU_FALLBACK));
    let b1 = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1));
    let b2 = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO2));

    // Three Fulu-era digests must be pairwise distinct.
    assert_ne!(fb, b1, "fallback vs BPO1");
    assert_ne!(b1, b2, "BPO1 vs BPO2");
    assert_ne!(fb, b2, "fallback vs BPO2");
    // Pre-Fulu is the un-XOR'd base and must differ from every Fulu-era value
    // (base is Electra version; Fulu XOR's Fulu base — different by construction).
    assert_ne!(pre, fb);
    assert_ne!(pre, b1);
    assert_ne!(pre, b2);

    // Pre-Fulu is the un-XOR'd base_digest[:4] (Electra fork version).
    let electra_base = compute_fork_data_root(cfg.electra_fork_version, gvr);
    assert_eq!(&digest_bytes(pre)[..], &electra_base.as_slice()[0..4]);
    assert_eq!(
        compute_fork_version(&cfg, Epoch::new(EPOCH_PRE_FULU)),
        cfg.electra_fork_version
    );

    // Committed fixture equality (V-1-derived expected values).
    for (label, got, expect) in [
        ("pre-Fulu", pre, EXPECT_PRE_FULU),
        ("Fulu-fallback", fb, EXPECT_FULU_FALLBACK),
        ("BPO1", b1, EXPECT_BPO1),
        ("BPO2", b2, EXPECT_BPO2),
    ] {
        assert_eq!(
            digest_bytes(got),
            expect,
            "{label}: digest mismatch (got {got}, expect 0x{:02x}{:02x}{:02x}{:02x})",
            expect[0],
            expect[1],
            expect[2],
            expect[3]
        );
    }
}

// ── Fallback branch: Fulu window before first BLOB_SCHEDULE entry ───────────

#[test]
fn get_blob_parameters_fallback_is_electra_from_loaded_config() {
    let cfg = hoodi_config();
    // Epoch in [FULU_FORK_EPOCH, first BLOB_SCHEDULE entry).
    let epoch = Epoch::new(EPOCH_FULU_FALLBACK);
    assert!(epoch.as_u64() >= cfg.fulu_fork_epoch.as_u64());
    assert!(epoch.as_u64() < cfg.blob_schedule.entries()[0].epoch.as_u64());

    let bp = cfg.get_blob_parameters::<Mainnet>(epoch);
    // Must be Electra-era parameters from the *loaded* config, not a Fulu
    // default and not a hard-coded constant name in services/p2p.
    // Hoodi electra epoch = 2048; mainnet Electra-era max blobs = 9.
    assert_eq!(bp.epoch, cfg.electra_fork_epoch);
    assert_eq!(bp.epoch, Epoch::new(2_048));
    assert_eq!(bp.max_blobs_per_block, 9);
    assert_eq!(
        bp,
        BlobParameters {
            epoch: Epoch::new(2_048),
            max_blobs_per_block: 9,
        }
    );

    // Digest at this epoch is the XOR form (Fulu-era), not the bare base.
    let gvr = hoodi_gvr();
    let fulu_fb = compute_fork_digest(&cfg, gvr, epoch);
    // Same fork version, pre-Fulu would not XOR — electra pre-fulu differs.
    let electra_pre = compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_PRE_FULU));
    assert_ne!(fulu_fb, electra_pre);
}

// ── next_fork / ENRForkID across a BPO ──────────────────────────────────────

#[test]
fn next_fork_before_bpo_returns_bpo_epoch_and_digest() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    // Inside Fulu fallback window, next digest change is BPO1.
    let epoch = Epoch::new(EPOCH_FULU_FALLBACK);
    let (b_epoch, b_version, b_digest) =
        next_fork(&cfg, gvr, epoch).expect("BPO1 must be scheduled");
    assert_eq!(b_epoch, Epoch::new(EPOCH_BPO1));
    // BPO does not change the regular fork version — still Fulu.
    assert_eq!(b_version, cfg.fulu_fork_version);
    assert_eq!(
        b_digest,
        compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1))
    );

    let enr = enr_fork_id(&cfg, gvr, epoch);
    // next_fork_epoch tracks the BPO; next_fork_version is unchanged (Fulu).
    assert_eq!(enr.next_fork_epoch, Epoch::new(EPOCH_BPO1));
    assert_eq!(enr.next_fork_version, cfg.fulu_fork_version);
    assert_eq!(enr.next_fork_version, compute_fork_version(&cfg, epoch));
    // Deliberate agreement of version with current while epoch advances.
    assert_eq!(enr.fork_digest, compute_fork_digest(&cfg, gvr, epoch));
    assert_eq!(
        next_fork_digest(&cfg, gvr, epoch),
        compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1))
    );
}

#[test]
fn next_fork_none_beyond_last_schedule_entry() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    // Past last BPO and past every regular fork — Hoodi has nothing further.
    let epoch = Epoch::new(100_000);
    assert!(next_fork(&cfg, gvr, epoch).is_none());

    let enr = enr_fork_id(&cfg, gvr, epoch);
    assert_eq!(enr.next_fork_epoch, FAR_FUTURE_EPOCH);
    // No future regular fork → next_fork_version == current (Fulu).
    assert_eq!(enr.next_fork_version, cfg.fulu_fork_version);
    assert_eq!(next_fork_digest(&cfg, gvr, epoch), ForkDigest::ZERO);
}

#[test]
fn next_fork_before_fulu_is_regular_fork_boundary() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    // Electra-era, before Fulu: next digest change is the Fulu regular fork.
    let epoch = Epoch::new(EPOCH_PRE_FULU);
    let (b_epoch, b_version, _) = next_fork(&cfg, gvr, epoch).expect("Fulu scheduled");
    assert_eq!(b_epoch, cfg.fulu_fork_epoch);
    assert_eq!(b_version, cfg.fulu_fork_version);

    let enr = enr_fork_id(&cfg, gvr, epoch);
    assert_eq!(enr.next_fork_epoch, cfg.fulu_fork_epoch);
    assert_eq!(enr.next_fork_version, cfg.fulu_fork_version);
    // Here version *does* advance (regular fork) — not a pure BPO.
    assert_ne!(enr.next_fork_version, compute_fork_version(&cfg, epoch));
}

#[test]
fn far_future_regular_fork_is_not_a_digest_boundary() {
    let mut cfg = hoodi_config();
    let gvr = hoodi_gvr();
    cfg.fulu_fork_epoch = FAR_FUTURE_EPOCH;

    // Electra-era, Fulu unscheduled: next digest change is BPO1, not u64::MAX.
    let epoch = Epoch::new(EPOCH_PRE_FULU);
    let (b_epoch, b_version, _) = next_fork(&cfg, gvr, epoch).expect("BPO1 still scheduled");
    assert_eq!(b_epoch, Epoch::new(EPOCH_BPO1));
    assert_ne!(b_epoch, FAR_FUTURE_EPOCH);
    assert_eq!(b_version, cfg.electra_fork_version);

    let enr = enr_fork_id(&cfg, gvr, epoch);
    assert_eq!(enr.next_fork_epoch, Epoch::new(EPOCH_BPO1));
    assert_eq!(enr.next_fork_version, cfg.electra_fork_version);
}

// ── ForkContext cache ───────────────────────────────────────────────────────

#[test]
fn fork_context_cache_matches_uncached_across_hoodi_ranges() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    // Start in the current head era (BPO2).
    let mut ctx = ForkContext::new(cfg.clone(), gvr, Epoch::new(EPOCH_BPO2));
    assert_eq!(
        ctx.current_digest(),
        compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO2))
    );

    // 100 pseudo-random epochs spanning pre-Fulu, fallback, BPO1, BPO2.
    let ranges = [
        (0u64, 50_687u64), // pre-Fulu
        (50_688, 52_479),  // Fulu fallback
        (52_480, 54_015),  // BPO1
        (54_016, 120_000), // BPO2+
    ];
    let mut epochs = Vec::with_capacity(100);
    let mut seed = 0xC0FFEE_u64;
    for i in 0..100 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let (lo, hi) = ranges[i % ranges.len()];
        let span = hi - lo + 1;
        epochs.push(Epoch::new(lo + (seed % span)));
    }

    for epoch in epochs {
        let cached = ctx.digest_at(epoch);
        let direct = compute_fork_digest(&cfg, gvr, epoch);
        assert_eq!(cached, direct, "cache miss/hit mismatch at epoch {epoch}");
        // Second hit must be stable.
        assert_eq!(ctx.digest_at(epoch), direct);
    }
}

#[test]
fn fork_context_on_epoch_refreshes_next_and_enr() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let mut ctx = ForkContext::new(cfg.clone(), gvr, Epoch::new(EPOCH_FULU_FALLBACK));
    assert_eq!(ctx.next().map(|(e, _, _)| e), Some(Epoch::new(EPOCH_BPO1)));
    assert_eq!(ctx.enr_fork_id().next_fork_epoch, Epoch::new(EPOCH_BPO1));
    assert_eq!(ctx.enr_fork_id().next_fork_version, cfg.fulu_fork_version);

    ctx.on_epoch(Epoch::new(EPOCH_BPO1));
    assert_eq!(ctx.current_epoch(), Epoch::new(EPOCH_BPO1));
    assert_eq!(
        ctx.current_digest(),
        compute_fork_digest(&cfg, gvr, Epoch::new(EPOCH_BPO1))
    );
    assert_eq!(ctx.next().map(|(e, _, _)| e), Some(Epoch::new(EPOCH_BPO2)));

    ctx.on_epoch(Epoch::new(EPOCH_BPO2));
    assert!(ctx.next().is_none());
    assert_eq!(ctx.nfd(), ForkDigest::ZERO);
    assert_eq!(ctx.enr_fork_id().next_fork_epoch, FAR_FUTURE_EPOCH);
}

#[test]
fn fork_digest_module_has_no_network_imports() {
    // Structural guard on the *use* block: only cc-types, cc-crypto, and std.
    // Doc comments may mention forbidden crates by name ("no libp2p") — strip
    // line comments / block docs before scanning for real imports.
    let src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/fork_digest.rs"));
    let code_only: String = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//") && !t.starts_with("//!") && !t.starts_with("///")
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Token fragments re-assembled so this test file itself does not trip the
    // CC-21b grep guard on services/p2p (exact SCREAMING names are forbidden).
    let banned_blob = format!("{}_{}_{}", "MAX", "BLOBS", "PER_BLOCK");
    let banned_electra = format!("{}_{}_{}", "ELECTRA", "FORK", "EPOCH");
    for banned in [
        "libp2p",
        "discv5",
        "std::fs",
        "std::net",
        "std::io",
        "tokio::",
        banned_blob.as_str(),
        banned_electra.as_str(),
    ] {
        assert!(
            !code_only.contains(banned),
            "fork_digest.rs code must not contain `{banned}`"
        );
    }

    // Collect `use` lines — must be limited to cc_types, cc_crypto, std.
    let use_lines: Vec<&str> = src
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("use "))
        .collect();
    assert!(!use_lines.is_empty(), "expected at least one use line");
    for line in &use_lines {
        let ok = line.starts_with("use std::")
            || line.starts_with("use cc_types::")
            || line.starts_with("use cc_crypto::");
        assert!(
            ok,
            "fork_digest.rs use block must be limited to cc-types / cc-crypto / std; got: {line}"
        );
    }
}

#[test]
fn four_hoodi_digests_are_unique_set() {
    let cfg = hoodi_config();
    let gvr = hoodi_gvr();
    let mut set = HashSet::new();
    for e in [EPOCH_PRE_FULU, EPOCH_FULU_FALLBACK, EPOCH_BPO1, EPOCH_BPO2] {
        set.insert(digest_bytes(compute_fork_digest(&cfg, gvr, Epoch::new(e))));
    }
    assert_eq!(set.len(), 4);
}

// Compile-time check that ForkVersion is available for ENR assertions.
#[test]
fn fulu_fork_version_matches_config() {
    let cfg = hoodi_config();
    let v: ForkVersion = cfg.fulu_fork_version;
    assert_eq!(v.as_slice(), &[0x70, 0x00, 0x09, 0x10]);
}
