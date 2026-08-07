//! Cell-KZG column material for fixture sidecars.

use anyhow::{Context, Result};
use cc_crypto::{Blob, CellKzg, CKzgBackend, BYTES_PER_BLOB};
use cc_types::primitives::{Cell, KzgCommitment, KzgProof};
use cc_types::NUMBER_OF_COLUMNS;

/// Per-blob cells + proofs + commitment.
#[derive(Debug, Clone)]
pub struct BlobColumnMaterial {
    /// KZG commitment for the blob.
    pub commitment: KzgCommitment,
    /// 128 cells (one per column).
    pub cells: Vec<Cell>,
    /// 128 cell proofs.
    pub proofs: Vec<KzgProof>,
}

/// Load the default c-kzg backend once.
pub fn load_kzg() -> Result<CKzgBackend> {
    CKzgBackend::load_default().context("load c-kzg trusted setup")
}

/// Deterministic blob for `(slot, blob_index)`.
pub fn deterministic_blob(seed: &[u8; 32], slot: u64, blob_index: u64) -> Blob {
    let mut bytes = vec![0u8; BYTES_PER_BLOB];
    // Keep each 32-byte field element small so it is a valid BLS scalar.
    for (i, chunk) in bytes.chunks_mut(32).enumerate() {
        chunk[0] = 0;
        chunk[1] = seed[i % 32];
        chunk[2] = (slot as u8).wrapping_add(blob_index as u8);
        chunk[3] = (i as u8).wrapping_mul(3).wrapping_add(1);
        chunk[4] = ((slot >> 8) as u8).wrapping_add(blob_index as u8);
        // remaining zeros
    }
    // Length is always BYTES_PER_BLOB by construction above.
    match Blob::from_slice(&bytes) {
        Ok(b) => b,
        Err(_) => Blob::zero(),
    }
}

/// Compute commitments + cell matrix for `blob_count` blobs at `slot`.
pub fn compute_blob_materials(
    kzg: &impl CellKzg,
    seed: &[u8; 32],
    slot: u64,
    blob_count: u64,
) -> Result<Vec<BlobColumnMaterial>> {
    let mut out = Vec::with_capacity(blob_count as usize);
    for i in 0..blob_count {
        let blob = deterministic_blob(seed, slot, i);
        let commitment = kzg
            .blob_to_kzg_commitment(&blob)
            .with_context(|| format!("commitment slot={slot} blob={i}"))?;
        let (cells, proofs) = kzg
            .compute_cells_and_kzg_proofs(&blob)
            .with_context(|| format!("cells slot={slot} blob={i}"))?;
        out.push(BlobColumnMaterial {
            commitment,
            cells: cells.to_vec(),
            proofs: proofs.to_vec(),
        });
    }
    Ok(out)
}

/// Transposed column view: for each column index, cells/proofs across blobs.
#[derive(Debug, Clone)]
pub struct ColumnBundle {
    /// Column index `0..128`.
    pub index: u64,
    /// One cell per blob.
    pub cells: Vec<Cell>,
    /// Matching commitments (same order as block body).
    pub commitments: Vec<KzgCommitment>,
    /// Matching cell proofs.
    pub proofs: Vec<KzgProof>,
}

/// Build 128 column bundles from per-blob materials.
pub fn columns_from_materials(materials: &[BlobColumnMaterial]) -> Vec<ColumnBundle> {
    let n_cols = NUMBER_OF_COLUMNS as usize;
    let mut cols = Vec::with_capacity(n_cols);
    for col in 0..n_cols {
        let mut cells = Vec::with_capacity(materials.len());
        let mut commitments = Vec::with_capacity(materials.len());
        let mut proofs = Vec::with_capacity(materials.len());
        for m in materials {
            cells.push(m.cells[col]);
            commitments.push(m.commitment);
            proofs.push(m.proofs[col]);
        }
        cols.push(ColumnBundle {
            index: col as u64,
            cells,
            commitments,
            proofs,
        });
    }
    cols
}

/// Verify all cells of a column bundle (AC three-step cell path).
pub fn verify_column_cells(
    kzg: &impl CellKzg,
    bundle: &ColumnBundle,
) -> Result<bool> {
    if bundle.cells.is_empty() {
        return Ok(true);
    }
    // Each cell is at column index `bundle.index` of its blob (row = blob index).
    // verify_cell_kzg_proof_batch expects parallel slices of equal length.
    let n = bundle.cells.len();
    let mut commitments = Vec::with_capacity(n);
    let mut indices = Vec::with_capacity(n);
    let mut cells = Vec::with_capacity(n);
    let mut proofs = Vec::with_capacity(n);
    for i in 0..n {
        commitments.push(bundle.commitments[i]);
        indices.push(bundle.index);
        cells.push(bundle.cells[i]);
        proofs.push(bundle.proofs[i]);
    }
    kzg.verify_cell_kzg_proof_batch(&commitments, &indices, &cells, &proofs)
        .context("verify_cell_kzg_proof_batch")
}
