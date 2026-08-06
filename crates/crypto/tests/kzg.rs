//! CC-11b CellKzg + c-kzg backend unit tests.
//!
//! # Spec vectors
//!
//! Pin `v1.7.0-alpha.13` has **no** `tests/general/**/kzg` suite (see
//! `spec-vectors-layout.md` OQ-2). Acceptance criteria that require the general
//! KZG walker are therefore **not claimed green** on this pin — unit tests
//! cover the trait contract, trusted-setup load, constant agreement, recover
//! path, and `Ok(false)` vs `Err` verdicts instead.
//!
//! When a future pin ships `tests/general/fulu/kzg/**`, add a handler walker
//! here (and remove any CC-11 skip-list entries). Do not invent false green.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use cc_crypto::kzg::setup::{TrustedSetupBytes, NUM_G1_POINTS, NUM_G2_POINTS};
use cc_crypto::{Blob, CellKzg, CKzgBackend, KzgError, BYTES_PER_BLOB};
use cc_types::{
    Cell, KzgCommitment, KzgProof, CELLS_PER_EXT_BLOB, FIELD_ELEMENTS_PER_CELL, NUMBER_OF_COLUMNS,
};

/// Known-blob commitment (CC-11/5). Structured so CC-11c can assert the
/// **identical** commitment from backend B against the same setup.
///
/// Blob: every byte `0x01`. Commitment recorded from c-kzg 2.1.8 + committed
/// `trusted_setup.json` at CC-11b.
fn known_blob() -> Blob {
    Blob::filled(0x01)
}

/// Commitment for [`known_blob`] under the committed mainnet setup (c-kzg 2.1.8).
///
/// CC-11c must produce the **identical** value from backend B.
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

fn backend() -> CKzgBackend {
    CKzgBackend::load_default().expect("load c-kzg trusted setup")
}

// ---------------------------------------------------------------------------
// CC-11/5 — trusted setup loads; known blob commitment
// ---------------------------------------------------------------------------

#[test]
fn trusted_setup_loads_from_committed_json() {
    let bytes = TrustedSetupBytes::from_committed().expect("parse");
    assert_eq!(bytes.g1_monomial.len(), NUM_G1_POINTS * 48);
    assert_eq!(bytes.g2_monomial.len(), NUM_G2_POINTS * 96);
    let _ = backend();
}

#[test]
fn known_blob_commitment_is_stable() {
    let b = backend();
    let expected = parse_commitment_hex(KNOWN_BLOB_COMMITMENT_HEX);
    let got = b.blob_to_kzg_commitment(&known_blob()).unwrap();
    assert_eq!(got, expected, "CC-11/5 known-blob commitment drift");
    assert_ne!(got, KzgCommitment::ZERO);
}

// ---------------------------------------------------------------------------
// Constants agreement with cc-types (Architecture §4.3)
// ---------------------------------------------------------------------------

#[test]
fn c_kzg_constants_agree_with_cc_types() {
    assert_eq!(c_kzg::CELLS_PER_EXT_BLOB, CELLS_PER_EXT_BLOB);
    assert_eq!(c_kzg::FIELD_ELEMENTS_PER_CELL, FIELD_ELEMENTS_PER_CELL);
    // c-kzg has no NUMBER_OF_COLUMNS; it equals CELLS_PER_EXT_BLOB by spec.
    assert_eq!(c_kzg::CELLS_PER_EXT_BLOB as u64, NUMBER_OF_COLUMNS);
    assert_eq!(c_kzg::BYTES_PER_BLOB, BYTES_PER_BLOB);
    assert_eq!(c_kzg::BYTES_PER_CELL, cc_types::BYTES_PER_CELL);
}

// ---------------------------------------------------------------------------
// Cell compute / recover / verify round-trip
// ---------------------------------------------------------------------------

#[test]
fn compute_cells_and_verify_batch_ok_true() {
    let b = backend();
    let blob = known_blob();
    let commitment = b.blob_to_kzg_commitment(&blob).unwrap();
    let (cells, proofs) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    // Verify a single cell (column 0).
    let ok = b
        .verify_cell_kzg_proof_batch(&[commitment], &[0], &[cells[0]], &[proofs[0]])
        .unwrap();
    assert!(ok, "valid cell proof must return Ok(true)");

    // Verify a small multi-cell batch.
    let indices: Vec<u64> = (0..8).collect();
    let batch_cells: Vec<Cell> = indices.iter().map(|&i| cells[i as usize]).collect();
    let batch_proofs: Vec<KzgProof> = indices.iter().map(|&i| proofs[i as usize]).collect();
    let commitments = vec![commitment; indices.len()];
    let ok = b
        .verify_cell_kzg_proof_batch(&commitments, &indices, &batch_cells, &batch_proofs)
        .unwrap();
    assert!(ok);
}

#[test]
fn recover_cells_and_kzg_proofs_roundtrip() {
    let b = backend();
    let blob = known_blob();
    let (cells, proofs) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    // Any CELLS_PER_EXT_BLOB/2 cells suffice for Reed-Solomon recovery.
    let half = CELLS_PER_EXT_BLOB / 2;
    let indices: Vec<u64> = (0..half as u64).collect();
    let subset: Vec<Cell> = indices.iter().map(|&i| cells[i as usize]).collect();

    let (recovered_cells, recovered_proofs) = b
        .recover_cells_and_kzg_proofs(&indices, &subset)
        .expect("recover");

    assert_eq!(&recovered_cells[..], &cells[..]);
    assert_eq!(&recovered_proofs[..], &proofs[..]);
}

// ---------------------------------------------------------------------------
// Verdict contract: Ok(false) vs Err
// ---------------------------------------------------------------------------

#[test]
fn invalid_batch_returns_ok_false() {
    let b = backend();
    let blob = known_blob();
    let commitment = b.blob_to_kzg_commitment(&blob).unwrap();
    let (cells, proofs) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    // Valid-but-wrong: correct cell/proof for column 0, commitment of a
    // different blob. Encoding stays well-formed so the backend returns
    // Ok(false) rather than Err (malformed).
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

#[test]
fn mismatched_slice_lengths_return_err() {
    let b = backend();
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

#[test]
fn recover_mismatched_lengths_return_err() {
    let b = backend();
    let blob = known_blob();
    let (cells, _) = b.compute_cells_and_kzg_proofs(&blob).unwrap();

    let err = b
        .recover_cells_and_kzg_proofs(&[0, 1], &[cells[0]])
        .unwrap_err();
    assert!(matches!(err, KzgError::MismatchLength(_)));
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

// ---------------------------------------------------------------------------
// Both features compile (smoke for stub backend B)
// ---------------------------------------------------------------------------

#[cfg(feature = "kzg-rust-eth-kzg")]
#[test]
fn rust_eth_kzg_stub_returns_unavailable() {
    use cc_crypto::RustEthKzgBackend;
    let stub = RustEthKzgBackend::new();
    let err = stub.blob_to_kzg_commitment(&known_blob()).unwrap_err();
    assert!(matches!(err, KzgError::BackendUnavailable(_)));
}
