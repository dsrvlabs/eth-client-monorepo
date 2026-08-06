//! CC-11b / CC-11c CellKzg backend unit tests.
//!
//! # Spec vectors
//!
//! Pin `v1.7.0-alpha.13` has **no** `tests/general/**/kzg` suite (see
//! `spec-vectors-layout.md` OQ-2). Acceptance criteria that require the general
//! KZG walker are therefore **not claimed green** on this pin — unit tests
//! cover the trait contract, trusted-setup load, constant agreement, recover
//! path, dual-backend commitment identity, and `Ok(false)` vs `Err` verdicts
//! **per backend** instead.
//!
//! When a future pin ships `tests/general/fulu/kzg/**`, add a handler walker
//! here (and remove any CC-11 skip-list entries). Do not invent false green.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use cc_crypto::kzg::setup::{TrustedSetupBytes, NUM_G1_POINTS, NUM_G2_POINTS};
use cc_crypto::{Blob, CellKzg, KzgError, BYTES_PER_BLOB};
use cc_types::{
    Cell, KzgCommitment, KzgProof, CELLS_PER_EXT_BLOB, FIELD_ELEMENTS_PER_CELL, NUMBER_OF_COLUMNS,
};

/// Known-blob commitment (CC-11/5). Both backends must produce this identical
/// value from the same committed trusted setup.
///
/// Blob: every byte `0x01`. Commitment recorded from c-kzg 2.1.8 + committed
/// `trusted_setup.json` at CC-11b; re-checked under rust_eth_kzg 0.10.0 at CC-11c.
fn known_blob() -> Blob {
    Blob::filled(0x01)
}

/// Commitment for [`known_blob`] under the committed mainnet setup.
const KNOWN_BLOB_COMMITMENT_HEX: &str =
    "aa1a1c26055a329817a5759d877a2795f9499b97d6056edde0eea39512f24e8bc874b4471f0501127abb1ea0d9f68ac1";

fn parse_commitment_hex(hex: &str) -> KzgCommitment {
    let mut out = [0u8; 48];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).unwrap();
        out[i] = u8::from_str_radix(s, 16).unwrap();
    }
    KzgCommitment::from_array(out)
}

// ---------------------------------------------------------------------------
// Backend constructors (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "kzg-c-kzg")]
fn c_kzg_backend() -> cc_crypto::CKzgBackend {
    cc_crypto::CKzgBackend::load_default().expect("load c-kzg trusted setup")
}

#[cfg(feature = "kzg-rust-eth-kzg")]
fn rust_eth_kzg_backend() -> cc_crypto::RustEthKzgBackend {
    cc_crypto::RustEthKzgBackend::load_default().expect("load rust_eth_kzg trusted setup")
}

// ---------------------------------------------------------------------------
// CC-11/5 — trusted setup loads; known blob commitment
// ---------------------------------------------------------------------------

#[test]
fn trusted_setup_loads_from_committed_json() {
    let bytes = TrustedSetupBytes::from_committed().expect("parse");
    assert_eq!(bytes.g1_monomial.len(), NUM_G1_POINTS * 48);
    assert_eq!(bytes.g2_monomial.len(), NUM_G2_POINTS * 96);
}

#[cfg(feature = "kzg-c-kzg")]
#[test]
fn c_kzg_known_blob_commitment_is_stable() {
    let b = c_kzg_backend();
    let expected = parse_commitment_hex(KNOWN_BLOB_COMMITMENT_HEX);
    let got = b.blob_to_kzg_commitment(&known_blob()).unwrap();
    assert_eq!(got, expected, "CC-11/5 known-blob commitment drift (c-kzg)");
    assert_ne!(got, KzgCommitment::ZERO);
}

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_known_blob_commitment_is_stable() {
    let b = rust_eth_kzg_backend();
    let expected = parse_commitment_hex(KNOWN_BLOB_COMMITMENT_HEX);
    let got = b.blob_to_kzg_commitment(&known_blob()).unwrap();
    assert_eq!(
        got, expected,
        "CC-11/5 known-blob commitment drift (rust_eth_kzg)"
    );
    assert_ne!(got, KzgCommitment::ZERO);
}

/// CC-11/5 across backends: both produce an **identical** commitment for the
/// same blob from the same committed trusted setup.
#[cfg(all(feature = "kzg-c-kzg", feature = "kzg-rust-eth-kzg"))]
#[test]
fn both_backends_identical_known_blob_commitment() {
    let a = c_kzg_backend();
    let b = rust_eth_kzg_backend();
    let blob = known_blob();
    let ca = a.blob_to_kzg_commitment(&blob).unwrap();
    let cb = b.blob_to_kzg_commitment(&blob).unwrap();
    assert_eq!(ca, cb, "backends disagree on known-blob commitment");
    assert_eq!(ca, parse_commitment_hex(KNOWN_BLOB_COMMITMENT_HEX));
}

// ---------------------------------------------------------------------------
// Constants agreement with cc-types (Architecture §4.3)
// ---------------------------------------------------------------------------

#[cfg(feature = "kzg-c-kzg")]
#[test]
fn c_kzg_constants_agree_with_cc_types() {
    assert_eq!(c_kzg::CELLS_PER_EXT_BLOB, CELLS_PER_EXT_BLOB);
    assert_eq!(c_kzg::FIELD_ELEMENTS_PER_CELL, FIELD_ELEMENTS_PER_CELL);
    // c-kzg has no NUMBER_OF_COLUMNS; it equals CELLS_PER_EXT_BLOB by spec.
    assert_eq!(c_kzg::CELLS_PER_EXT_BLOB as u64, NUMBER_OF_COLUMNS);
    assert_eq!(c_kzg::BYTES_PER_BLOB, BYTES_PER_BLOB);
    assert_eq!(c_kzg::BYTES_PER_CELL, cc_types::BYTES_PER_CELL);
}

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_constants_agree_with_cc_types() {
    assert_eq!(
        rust_eth_kzg::constants::CELLS_PER_EXT_BLOB,
        CELLS_PER_EXT_BLOB
    );
    assert_eq!(
        rust_eth_kzg::constants::FIELD_ELEMENTS_PER_CELL,
        FIELD_ELEMENTS_PER_CELL
    );
    // rust_eth_kzg has no NUMBER_OF_COLUMNS; equals CELLS_PER_EXT_BLOB by spec.
    assert_eq!(
        rust_eth_kzg::constants::CELLS_PER_EXT_BLOB as u64,
        NUMBER_OF_COLUMNS
    );
    assert_eq!(rust_eth_kzg::constants::BYTES_PER_BLOB, BYTES_PER_BLOB);
    assert_eq!(
        rust_eth_kzg::constants::BYTES_PER_CELL,
        cc_types::BYTES_PER_CELL
    );
}

// ---------------------------------------------------------------------------
// Shared trait-contract helpers (work over any CellKzg)
// ---------------------------------------------------------------------------

fn assert_compute_and_verify_ok(b: &impl CellKzg) {
    let blob = known_blob();
    let commitment = b.blob_to_kzg_commitment(&blob).unwrap();
    let (cells, proofs) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    let ok = b
        .verify_cell_kzg_proof_batch(&[commitment], &[0], &[cells[0]], &[proofs[0]])
        .unwrap();
    assert!(ok, "valid cell proof must return Ok(true)");

    let indices: Vec<u64> = (0..8).collect();
    let batch_cells: Vec<Cell> = indices.iter().map(|&i| cells[i as usize]).collect();
    let batch_proofs: Vec<KzgProof> = indices.iter().map(|&i| proofs[i as usize]).collect();
    let commitments = vec![commitment; indices.len()];
    let ok = b
        .verify_cell_kzg_proof_batch(&commitments, &indices, &batch_cells, &batch_proofs)
        .unwrap();
    assert!(ok);
}

fn assert_recover_roundtrip(b: &impl CellKzg) {
    let blob = known_blob();
    let (cells, proofs) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    let half = CELLS_PER_EXT_BLOB / 2;
    let indices: Vec<u64> = (0..half as u64).collect();
    let subset: Vec<Cell> = indices.iter().map(|&i| cells[i as usize]).collect();

    let (recovered_cells, recovered_proofs) = b
        .recover_cells_and_kzg_proofs(&indices, &subset)
        .expect("recover");

    assert_eq!(&recovered_cells[..], &cells[..]);
    assert_eq!(&recovered_proofs[..], &proofs[..]);
}

/// Invalid-but-well-formed batch → `Ok(false)` (CC-11/2).
fn assert_invalid_batch_ok_false(b: &impl CellKzg) {
    let blob = known_blob();
    let commitment = b.blob_to_kzg_commitment(&blob).unwrap();
    let (cells, proofs) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    let other_commitment = b
        .blob_to_kzg_commitment(&Blob::filled(0x02))
        .expect("other blob commitment");
    assert_ne!(commitment, other_commitment);

    let verdict = b
        .verify_cell_kzg_proof_batch(&[other_commitment], &[0], &[cells[0]], &[proofs[0]])
        .expect("well-formed batch must not Err");
    assert!(
        !verdict,
        "proof under a different commitment must return Ok(false)"
    );
}

/// Malformed input (slice length mismatch) → `Err` (CC-11/2).
fn assert_mismatched_lengths_err(b: &impl CellKzg) {
    let blob = known_blob();
    let commitment = b.blob_to_kzg_commitment(&blob).unwrap();
    let (cells, proofs) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    let err = b
        .verify_cell_kzg_proof_batch(
            &[commitment],
            &[0, 1], // mismatched indices length
            &[cells[0]],
            &[proofs[0]],
        )
        .unwrap_err();
    assert!(
        matches!(err, KzgError::MismatchLength(_)),
        "expected MismatchLength, got {err:?}"
    );
}

fn assert_recover_mismatched_lengths_err(b: &impl CellKzg) {
    let blob = known_blob();
    let (cells, _) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    let err = b
        .recover_cells_and_kzg_proofs(&[0, 1], &[cells[0]])
        .unwrap_err();
    assert!(matches!(err, KzgError::MismatchLength(_)));
}

// ---------------------------------------------------------------------------
// Backend A (c-kzg) — unit suite
// ---------------------------------------------------------------------------

#[cfg(feature = "kzg-c-kzg")]
#[test]
fn c_kzg_compute_cells_and_verify_batch_ok_true() {
    assert_compute_and_verify_ok(&c_kzg_backend());
}

#[cfg(feature = "kzg-c-kzg")]
#[test]
fn c_kzg_recover_cells_and_kzg_proofs_roundtrip() {
    assert_recover_roundtrip(&c_kzg_backend());
}

#[cfg(feature = "kzg-c-kzg")]
#[test]
fn c_kzg_invalid_batch_returns_ok_false() {
    assert_invalid_batch_ok_false(&c_kzg_backend());
}

#[cfg(feature = "kzg-c-kzg")]
#[test]
fn c_kzg_mismatched_slice_lengths_return_err() {
    assert_mismatched_lengths_err(&c_kzg_backend());
}

#[cfg(feature = "kzg-c-kzg")]
#[test]
fn c_kzg_recover_mismatched_lengths_return_err() {
    assert_recover_mismatched_lengths_err(&c_kzg_backend());
}

// ---------------------------------------------------------------------------
// Backend B (rust_eth_kzg) — unit suite
// ---------------------------------------------------------------------------

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_compute_cells_and_verify_batch_ok_true() {
    assert_compute_and_verify_ok(&rust_eth_kzg_backend());
}

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_recover_cells_and_kzg_proofs_roundtrip() {
    assert_recover_roundtrip(&rust_eth_kzg_backend());
}

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_invalid_batch_returns_ok_false() {
    assert_invalid_batch_ok_false(&rust_eth_kzg_backend());
}

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_mismatched_slice_lengths_return_err() {
    assert_mismatched_lengths_err(&rust_eth_kzg_backend());
}

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_recover_mismatched_lengths_return_err() {
    assert_recover_mismatched_lengths_err(&rust_eth_kzg_backend());
}

/// CC-11/2 asserted **per backend, not once**: four assertions in one test
/// so a single backend mis-mapping fails the whole case.
#[cfg(all(feature = "kzg-c-kzg", feature = "kzg-rust-eth-kzg"))]
#[test]
fn both_backends_ok_false_vs_err_verdicts() {
    let a = c_kzg_backend();
    let b = rust_eth_kzg_backend();
    // 1–2: invalid proof → Ok(false) on each backend
    assert_invalid_batch_ok_false(&a);
    assert_invalid_batch_ok_false(&b);
    // 3–4: malformed input → Err on each backend
    assert_mismatched_lengths_err(&a);
    assert_mismatched_lengths_err(&b);
}

// ---------------------------------------------------------------------------
// Vector suite absence (pin documentation — not a false green)
// ---------------------------------------------------------------------------

#[test]
fn general_kzg_vector_suite_absent_on_this_pin() {
    // Mirrors spec-vectors-layout.md: no tests/general/**/kzg under v1.7.0-alpha.13.
    // If the cache is present, assert the path is missing; if absent, the layout
    // doc is the authority and this test still documents the pin state.
    let cache = std::env::var_os("SPEC_VECTORS_CACHE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let mut home = dirs_home();
            home.push(".cache/eth-consensus-spec-vectors");
            home
        });
    let tag = "v1.7.0-alpha.13";
    let general = cache.join(tag).join("tests/general");
    if general.is_dir() {
        let kzg_paths = walk_for_kzg(&general);
        assert!(
            kzg_paths.is_empty(),
            "unexpected KZG vector paths on pin {tag}: {kzg_paths:?}"
        );
    } else {
        eprintln!(
            "spec-vector cache not present at {}; layout.md records no general/**/kzg for {tag}",
            general.display()
        );
    }
}

fn dirs_home() -> std::path::PathBuf {
    std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .expect("HOME")
}

fn walk_for_kzg(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|n| n.to_str()) == Some("kzg") {
                    found.push(path);
                } else {
                    stack.push(path);
                }
            }
        }
    }
    found
}
