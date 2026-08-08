//! CC-4G /1 — custody subset property for the cgc 4 → 8 raise.
//!
//! OQ-6 resolution: `get_custody_groups(node_id, count)` walks discovery order
//! and only sorts at the end (`BTreeSet`), so the cgc=4 set is always a subset
//! of the cgc=8 set. A raise therefore never invalidates stored columns for the
//! old indices — only four new indices need backfill.
//!
//! # Corpus sizes (record for commit body)
//!
//! - **Spec vectors:** every mainnet Fulu `networking` / `get_custody_groups`
//!   case under the vector cache (11 cases on pin `v1.7.0-alpha.13`).
//! - **Random node ids:** 2 000 deterministic ids from a fixed-seed LCG
//!   expanded to `alloy_primitives::U256` (CI-stable; no `rand` dep).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::str::FromStr;

use alloy_primitives::U256;
use cc_spec_tests::Vectors;
use cc_types::get_custody_groups;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Runner constants (same as crates/types/tests/custody.rs)
// ---------------------------------------------------------------------------

/// Mainnet only — matches the types custody suite / Phase 1 green list.
const PRESET: &str = "mainnet";
const FORK: &str = "fulu";
const RUNNER: &str = "networking";
const HANDLER: &str = "get_custody_groups";

/// Random-id half corpus size (CC-4G /1).
const RANDOM_NODE_COUNT: usize = 2_000;
/// Fixed LCG seed for reproducible CI (not cryptographic).
/// Fixed seed for the 2 000-id half (`CC-4G` mnemonic as hex digits only).
const RANDOM_SEED: u64 = 0x0000_CC46_C64B_4001;

// ---------------------------------------------------------------------------
// Vector case YAML (mirrors types/tests/custody.rs loader)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GetCustodyGroupsMeta {
    node_id: U256Yaml,
    /// Present in every vector; unused for the 4⊆8 property (we recompute both).
    #[allow(dead_code)]
    custody_group_count: u64,
    #[allow(dead_code)]
    result: Vec<u64>,
}

/// YAML may encode U256 as an integer or a (quoted) decimal string.
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
/// strings (avoids u64 overflow → lossy f64). Same helper as types custody.
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

fn open_vectors() -> Vectors {
    Vectors::open().expect("spec vector cache must be present; run scripts/fetch-spec-vectors.sh")
}

/// Assert `get_custody_groups(id, 4) ⊆ get_custody_groups(id, 8)`.
///
/// On failure, names the failing `node_id`.
fn assert_cgc4_subset_of_cgc8(node_id: U256, context: &str) {
    let four = get_custody_groups(node_id, 4);
    let eight = get_custody_groups(node_id, 8);
    assert_eq!(four.len(), 4, "cgc=4 size for {context} node_id={node_id}");
    assert_eq!(eight.len(), 8, "cgc=8 size for {context} node_id={node_id}");
    assert!(
        four.is_subset(&eight),
        "CC-4G subset failed for {context}: get_custody_groups({node_id}, 4)={four:?} \
         is not ⊆ get_custody_groups({node_id}, 8)={eight:?}"
    );
}

// ---------------------------------------------------------------------------
// Deterministic random node ids (fixed-seed LCG → 32-byte U256)
// ---------------------------------------------------------------------------

/// Minimal LCG (`state = state * 6364136223846793005 + 1`) — no extra deps.
struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0
    }

    fn next_u256(&mut self) -> U256 {
        let mut bytes = [0u8; 32];
        for chunk in bytes.chunks_exact_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        U256::from_le_bytes(bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// CC-4G /1 — every Fulu networking `get_custody_groups` vector node id.
#[test]
fn custody_groups_4_subset_8_spec_vectors() {
    let vectors = open_vectors();
    let cases = vectors
        .cases(PRESET, FORK, RUNNER, HANDLER)
        .unwrap_or_else(|e| panic!("enumerate {HANDLER}: {e}"));
    assert!(
        !cases.is_empty(),
        "expected at least one {HANDLER} case under {PRESET}/{FORK}/{RUNNER}"
    );

    // Corpus size for soak / commit body (mainnet fulu networking pin).
    let vector_count = cases.len();
    eprintln!("CC-4G subset corpus: {vector_count} spec-vector node ids");

    for case in &cases {
        let meta = load_get_custody_groups_meta(case);
        let node_id = meta.node_id.into_u256();
        assert_cgc4_subset_of_cgc8(node_id, &format!("vector {}", case.rel_path()));
    }
}

/// CC-4G /1 — 2 000 random node ids with fixed seed.
#[test]
fn custody_groups_4_subset_8_random_2000() {
    let mut rng = Lcg(RANDOM_SEED);
    // Track uniqueness so the corpus is honest about coverage density.
    let mut seen: BTreeSet<U256> = BTreeSet::new();
    for i in 0..RANDOM_NODE_COUNT {
        let node_id = rng.next_u256();
        seen.insert(node_id);
        assert_cgc4_subset_of_cgc8(node_id, &format!("random[{i}]"));
    }
    assert_eq!(
        seen.len(),
        RANDOM_NODE_COUNT,
        "LCG collision in {RANDOM_NODE_COUNT}-id sample (reseed if this fires)"
    );
    eprintln!("CC-4G subset corpus: {RANDOM_NODE_COUNT} random node ids (seed={RANDOM_SEED:#x})");
}
