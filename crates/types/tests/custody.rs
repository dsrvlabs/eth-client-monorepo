//! Fulu `networking` custody-helper runner (CC-1B / Architecture §3.6).
//!
//! **Mainnet only** — there is no minimal-preset `networking` tree under this
//! pin. The missing minimal directory is therefore not a coverage gap.
//!
//! Handlers exercised here are the pure custody helpers:
//! - `get_custody_groups`
//! - `compute_columns_for_custody_group`
//!
//! Gossip handlers under the same runner (`gossip_*`) require state-transition
//! and fork-choice machinery (Phase 2 / later milestones) and are excluded from
//! this suite's coverage set via [`OUT_OF_SCOPE_HANDLERS`].

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::str::FromStr;

use alloy_primitives::U256;
use cc_spec_tests::{Vectors, assert_handler_coverage};
use cc_types::{
    CUSTODY_REQUIREMENT, NUMBER_OF_COLUMNS, NUMBER_OF_CUSTODY_GROUPS, SAMPLES_PER_SLOT,
    compute_columns_for_custody_group, get_custody_groups, sampling_size,
};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Runner constants
// ---------------------------------------------------------------------------

/// Mainnet only — no minimal-preset `networking` tree exists for this pin.
const PRESET: &str = "mainnet";
const FORK: &str = "fulu";
const RUNNER: &str = "networking";

/// Handlers this suite implements (CC-1B AC: at minimum these two).
const HANDLERS: &[&str] = &["compute_columns_for_custody_group", "get_custody_groups"];

/// Gossip handlers share the `networking` runner on disk but are out of scope
/// for CC-1B (Phase 2 / fork-choice milestones consume them).
const OUT_OF_SCOPE_HANDLERS: &[&str] = &[
    "gossip_attester_slashing",
    "gossip_beacon_aggregate_and_proof",
    "gossip_beacon_attestation",
    "gossip_beacon_block",
    "gossip_bls_to_execution_change",
    "gossip_data_column_sidecar",
    "gossip_partial_data_column_sidecar",
    "gossip_proposer_slashing",
    "gossip_sync_committee_contribution_and_proof",
    "gossip_sync_committee_message",
    "gossip_voluntary_exit",
];

// ---------------------------------------------------------------------------
// Vector case YAML shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GetCustodyGroupsMeta {
    node_id: U256Yaml,
    custody_group_count: u64,
    result: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct ComputeColumnsMeta {
    custody_group: u64,
    result: Vec<u64>,
}

/// YAML may encode U256 as an integer or a (quoted) decimal string.
///
/// `serde_yaml` only preserves integers up to `u64`; larger values become
/// lossy `f64`. The loader quotes 19+ digit decimals as strings before
/// parse so full `NodeID` precision is retained.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum U256Yaml {
    Int(u64),
    Str(String),
}

impl U256Yaml {
    fn into_u256(self) -> U256 {
        match self {
            Self::Int(v) => U256::from(v),
            Self::Str(s) => {
                let s = s.trim();
                U256::from_str(s).unwrap_or_else(|e| panic!("U256 parse {s:?}: {e}"))
            }
        }
    }
}

/// Quote decimal integer tokens of 19+ digits so `serde_yaml` keeps them as
/// strings (avoids u64 overflow → lossy f64).
fn quote_large_decimal_integers(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 32);
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            let slice = &input[start..i];
            if i - start >= 19 {
                out.push('"');
                out.push_str(slice);
                out.push('"');
            } else {
                out.push_str(slice);
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn load_get_custody_groups_meta(case: &cc_spec_tests::Case) -> GetCustodyGroupsMeta {
    let path = case.path.join("meta.yaml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let quoted = quote_large_decimal_integers(&text);
    serde_yaml::from_str(&quoted).unwrap_or_else(|e| panic!("meta.yaml {}: {e}", case.rel_path()))
}

// ---------------------------------------------------------------------------
// Spec-vector runners
// ---------------------------------------------------------------------------

fn open_vectors() -> Vectors {
    Vectors::open().expect("spec vector cache must be present; run scripts/fetch-spec-vectors.sh")
}

#[test]
fn get_custody_groups_vectors() {
    let vectors = open_vectors();
    let cases = vectors
        .cases(PRESET, FORK, RUNNER, "get_custody_groups")
        .unwrap_or_else(|e| panic!("enumerate get_custody_groups: {e}"));
    assert!(
        !cases.is_empty(),
        "expected at least one get_custody_groups case under {PRESET}/{FORK}/{RUNNER}"
    );

    for case in &cases {
        let meta = load_get_custody_groups_meta(case);
        let node_id = meta.node_id.into_u256();
        let got = get_custody_groups(node_id, meta.custody_group_count);
        let expected: BTreeSet<u64> = meta.result.into_iter().collect();
        assert_eq!(
            got,
            expected,
            "get_custody_groups mismatch for {} (node_id={node_id}, count={})",
            case.rel_path(),
            meta.custody_group_count,
        );
    }
}

#[test]
fn compute_columns_for_custody_group_vectors() {
    let vectors = open_vectors();
    let cases = vectors
        .cases(PRESET, FORK, RUNNER, "compute_columns_for_custody_group")
        .unwrap_or_else(|e| panic!("enumerate compute_columns_for_custody_group: {e}"));
    assert!(
        !cases.is_empty(),
        "expected at least one compute_columns_for_custody_group case under {PRESET}/{FORK}/{RUNNER}"
    );

    for case in &cases {
        let meta: ComputeColumnsMeta = case
            .yaml("meta.yaml")
            .unwrap_or_else(|e| panic!("meta.yaml {}: {e}", case.rel_path()));
        let got = compute_columns_for_custody_group(meta.custody_group);
        assert_eq!(
            got,
            meta.result,
            "compute_columns_for_custody_group mismatch for {} (group={})",
            case.rel_path(),
            meta.custody_group,
        );
    }
}

/// Declared handlers equal on-disk listing minus out-of-scope gossip handlers.
///
/// An unimplemented (non-gossip) handler fails coverage rather than being
/// skipped (Clause 1 / CC-1B AC).
#[test]
fn handler_coverage() {
    let vectors = open_vectors();
    let on_disk = vectors
        .handlers(PRESET, FORK, RUNNER)
        .unwrap_or_else(|e| panic!("handlers: {e}"));
    let relevant: BTreeSet<String> = on_disk
        .into_iter()
        .filter(|h| !OUT_OF_SCOPE_HANDLERS.contains(&h.as_str()))
        .collect();
    assert_handler_coverage(HANDLERS, &relevant)
        .unwrap_or_else(|e| panic!("handler coverage mismatch for {PRESET}/{FORK}/{RUNNER}: {e}"));
}

/// Negative coverage proof: dropping a declared handler fails loudly.
///
/// Documents Clause 1 behaviour (unimplemented handler → hard failure).
#[test]
fn handler_coverage_negative_missing_handler_fails() {
    let on_disk: BTreeSet<String> = HANDLERS.iter().map(|s| (*s).to_string()).collect();
    // Declare only one of the two → missing the other.
    let err = assert_handler_coverage(&["get_custody_groups"], &on_disk)
        .expect_err("partial HANDLERS must fail coverage");
    let msg = err.to_string();
    assert!(
        msg.contains("extra") || msg.contains("missing") || msg.contains("compute_columns"),
        "expected coverage failure naming the gap, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Unit tests (CC-1B/2–3 + partition)
// ---------------------------------------------------------------------------

#[test]
fn sampling_size_minimum_custody_exceeds_custody() {
    // CC-1B/2: at minimum custody, sampling exceeds custody.
    assert_eq!(sampling_size(CUSTODY_REQUIREMENT), 8);
    assert_eq!(sampling_size(4), 8);
    assert_eq!(sampling_size(12), 12);
    assert_eq!(sampling_size(SAMPLES_PER_SLOT), SAMPLES_PER_SLOT);
}

#[test]
fn get_custody_groups_deterministic_btreeset() {
    // CC-1B/3: fixed node_id → exact size, stable, BTreeSet sorted+deduped.
    let node_id = U256::from(42u64);
    let count = 4u64;
    let a = get_custody_groups(node_id, count);
    let b = get_custody_groups(node_id, count);
    assert_eq!(a.len() as u64, count);
    assert_eq!(a, b);
    let v: Vec<_> = a.iter().copied().collect();
    let mut sorted = v.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(v, sorted);
}

#[test]
fn compute_columns_partition_check() {
    // Over all 128 groups, every column index appears exactly the expected
    // number of times (catches off-by-one the vectors might not).
    let expected_hits = NUMBER_OF_COLUMNS / NUMBER_OF_CUSTODY_GROUPS;
    let mut hits = vec![0u64; NUMBER_OF_COLUMNS as usize];
    for group in 0..NUMBER_OF_CUSTODY_GROUPS {
        for col in compute_columns_for_custody_group(group) {
            assert!(col < NUMBER_OF_COLUMNS, "column {col} out of range");
            hits[col as usize] += 1;
        }
    }
    for (col, n) in hits.iter().enumerate() {
        assert_eq!(
            *n, expected_hits,
            "column {col} hit {n} times, expected {expected_hits}"
        );
    }
}

/// Mainnet-only suite (CC-1B AC / vector artifact `mainnet.tar.gz`).
///
/// A `minimal/fulu/networking` tree may exist on disk for this pin, but this
/// runner intentionally exercises **mainnet only** — the Phase 1 green list
/// and the issue's vector artifact are mainnet. Missing (or unused) minimal
/// coverage is therefore not a gap for CC-1B.
#[test]
fn mainnet_only_suite() {
    let vectors = open_vectors();
    let mainnet_handlers = vectors
        .handlers(PRESET, FORK, RUNNER)
        .unwrap_or_else(|e| panic!("mainnet handlers: {e}"));
    assert!(
        !mainnet_handlers.is_empty(),
        "mainnet/{FORK}/{RUNNER} must expose handlers"
    );
    for h in HANDLERS {
        assert!(
            mainnet_handlers.contains(*h),
            "mainnet handler {h} missing from on-disk listing {mainnet_handlers:?}"
        );
    }
}
