//! Column step-12 KZG verification (CC-22d / H1).
//!
//! Production installs a real [`CellKzg`] backend when the trusted setup loads.
//! If the backend is unavailable, verification is **fail-closed** (never ACCEPT).
//! Tests may install [`AlwaysValidKzg`] only under explicit test control — never
//! the default production path.

use std::sync::Arc;

use cc_crypto::CellKzg;
use cc_types::primitives::{Cell, KzgCommitment, KzgProof};
use tracing::error;

/// Dyn-compatible KZG seam for column gossip validation.
pub trait KzgVerify: Send + Sync {
    /// Verify cell KZG proofs for one data-column sidecar.
    ///
    /// Returns `true` only when proofs are cryptographically valid.
    /// Unavailable backend / malformed input / invalid proof → `false` (REJECT).
    fn verify_column_kzg(
        &self,
        column_index: u64,
        commitments: &[KzgCommitment],
        cells: &[Cell],
        proofs: &[KzgProof],
    ) -> bool;
}

/// Fail-closed verifier: always returns `false` (REJECT at step 12).
///
/// Used when the trusted setup cannot be loaded so invalid columns never ACCEPT.
#[derive(Debug, Default, Clone, Copy)]
pub struct FailClosedKzg;

impl KzgVerify for FailClosedKzg {
    fn verify_column_kzg(
        &self,
        _column_index: u64,
        _commitments: &[KzgCommitment],
        _cells: &[Cell],
        _proofs: &[KzgProof],
    ) -> bool {
        false
    }
}

/// Real [`CellKzg`] backend wrapper.
#[derive(Clone)]
pub struct CellKzgVerifier {
    backend: Arc<dyn CellKzg>,
}

impl std::fmt::Debug for CellKzgVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CellKzgVerifier")
    }
}

impl CellKzgVerifier {
    /// Wrap an existing backend.
    #[must_use]
    pub fn new(backend: Arc<dyn CellKzg>) -> Self {
        Self { backend }
    }
}

impl KzgVerify for CellKzgVerifier {
    fn verify_column_kzg(
        &self,
        column_index: u64,
        commitments: &[KzgCommitment],
        cells: &[Cell],
        proofs: &[KzgProof],
    ) -> bool {
        if commitments.is_empty()
            || commitments.len() != cells.len()
            || commitments.len() != proofs.len()
        {
            return false;
        }
        // Spec: one cell index per row, all equal to the column index.
        let cell_indices: Vec<u64> = vec![column_index; cells.len()];
        match self
            .backend
            .verify_cell_kzg_proof_batch(commitments, &cell_indices, cells, proofs)
        {
            Ok(valid) => valid,
            Err(e) => {
                // Malformed / internal → fail-closed REJECT (not ACCEPT).
                error!(error = %e, "column KZG verify error; rejecting");
                false
            }
        }
    }
}

/// Test-only always-valid verifier. **Never** install on the production pool.
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysValidKzg;

impl KzgVerify for AlwaysValidKzg {
    fn verify_column_kzg(
        &self,
        _column_index: u64,
        _commitments: &[KzgCommitment],
        _cells: &[Cell],
        _proofs: &[KzgProof],
    ) -> bool {
        true
    }
}

/// Load the default CellKzg backend for production column validation.
///
/// On setup failure returns [`FailClosedKzg`] so step 12 never ACCEPTS.
#[must_use]
pub fn production_kzg_verify() -> Arc<dyn KzgVerify> {
    match load_default_backend() {
        Some(backend) => Arc::new(CellKzgVerifier::new(backend)),
        None => {
            error!(
                "KZG trusted setup unavailable; column step-12 is fail-closed (REJECT all KZG)"
            );
            Arc::new(FailClosedKzg)
        }
    }
}

fn load_default_backend() -> Option<Arc<dyn CellKzg>> {
    // cc-crypto default feature is kzg-c-kzg (CC-11d).
    match cc_crypto::CKzgBackend::load_default() {
        Ok(b) => Some(Arc::new(b)),
        Err(e) => {
            error!(error = %e, "CKzgBackend::load_default failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn fail_closed_never_accepts() {
        let v = FailClosedKzg;
        assert!(!v.verify_column_kzg(0, &[], &[], &[]));
    }

    #[test]
    fn always_valid_test_only() {
        assert!(AlwaysValidKzg.verify_column_kzg(0, &[], &[], &[]));
    }
}
