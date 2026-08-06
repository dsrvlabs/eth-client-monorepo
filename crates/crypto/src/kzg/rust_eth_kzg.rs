//! `rust_eth_kzg` 0.10.0 backend stub (CC-11c).
//!
//! Present so both feature flags compile together (`kzg-c-kzg` +
//! `kzg-rust-eth-kzg`). Real adapter work is CC-11c.

use super::{Blob, CellKzg, Cells, CellsAndProofs, KzgError};
use cc_types::{Cell, KzgCommitment, KzgProof};

/// Stub backend B — methods return [`KzgError::BackendUnavailable`] until CC-11c.
#[derive(Debug, Default, Clone, Copy)]
pub struct RustEthKzgBackend;

impl RustEthKzgBackend {
    /// Construct the stub (no trusted-setup load until CC-11c).
    pub fn new() -> Self {
        Self
    }
}

fn unavailable<T>() -> Result<T, KzgError> {
    Err(KzgError::BackendUnavailable(
        "rust_eth_kzg backend lands in CC-11c".into(),
    ))
}

impl CellKzg for RustEthKzgBackend {
    fn blob_to_kzg_commitment(&self, _blob: &Blob) -> Result<KzgCommitment, KzgError> {
        unavailable()
    }

    fn compute_cells(&self, _blob: &Blob) -> Result<Cells, KzgError> {
        unavailable()
    }

    fn compute_cells_and_kzg_proofs(&self, _blob: &Blob) -> Result<CellsAndProofs, KzgError> {
        unavailable()
    }

    fn recover_cells_and_kzg_proofs(
        &self,
        _cell_indices: &[u64],
        _cells: &[Cell],
    ) -> Result<CellsAndProofs, KzgError> {
        unavailable()
    }

    fn verify_cell_kzg_proof_batch(
        &self,
        _commitments: &[KzgCommitment],
        _cell_indices: &[u64],
        _cells: &[Cell],
        _proofs: &[KzgProof],
    ) -> Result<bool, KzgError> {
        unavailable()
    }
}
