//! `rust_eth_kzg` 0.10.0 backend (Architecture §4.2–4.3, CC-11c).
//!
//! # Normalisations
//!
//! 1. **Verdict** — `rust_eth_kzg` returns `Result<(), Error>` (crypto failure
//!    folded into `Err`). The trait contract is `c-kzg`'s `Result<bool, _>`:
//!    proof-verification-failure → `Ok(false)`; everything else → `Err`.
//! 2. **Ownership** — the trait takes slices; this adapter builds owned
//!    `Vec<Bytes48Ref>` / `Vec<CellRef>` per call for the backend.

use super::setup::TRUSTED_SETUP_JSON;
use super::{Blob, CellKzg, CellProofs, Cells, CellsAndProofs, KzgError};
use cc_types::{Cell, KzgCommitment, KzgProof, BYTES_PER_CELL, CELLS_PER_EXT_BLOB};
use rust_eth_kzg::{
    constants::{self, CELLS_PER_EXT_BLOB as REK_CELLS},
    DASContext, Error as RekError, TrustedSetup,
};

// KZG-VARIANT: rust_eth_kzg 0.10.0 proof-verification-failure =
//   Error::Verifier(VerifierError::FK20(_))  [cell path]
//   Error::EIP4844(...::InvalidProof)        [4844 path]
// Detected via Error::is_proof_invalid(). Nested VerifierError is not
// re-exported at the crate root; a rename that breaks is_proof_invalid is a
// reviewable bump. Catch-all for any other Err → KzgError, never Ok(false).

/// Re-export so consumers can thread precompute choice without depending on
/// `rust_eth_kzg` directly.
pub use rust_eth_kzg::UsePrecomp;

/// Default `UsePrecomp` (no precompute tables) — mirrors c-kzg
/// [`DEFAULT_PRECOMPUTE = 0`](super::setup::DEFAULT_PRECOMPUTE). CC-11d may
/// change the production default after measurement.
pub const DEFAULT_USE_PRECOMP: UsePrecomp = UsePrecomp::No;

/// `rust_eth_kzg` cell-KZG backend loaded from the committed trusted setup.
pub struct RustEthKzgBackend {
    ctx: DASContext,
}

impl std::fmt::Debug for RustEthKzgBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustEthKzgBackend").finish_non_exhaustive()
    }
}

impl RustEthKzgBackend {
    /// Load from the committed `trusted_setup.json` with the given
    /// [`UsePrecomp`] setting.
    pub fn load(use_precomp: UsePrecomp) -> Result<Self, KzgError> {
        let setup = load_trusted_setup()?;
        Ok(Self {
            ctx: DASContext::new(&setup, use_precomp),
        })
    }

    /// Load with [`DEFAULT_USE_PRECOMP`].
    pub fn load_default() -> Result<Self, KzgError> {
        Self::load(DEFAULT_USE_PRECOMP)
    }

    /// Wrap an already-built [`DASContext`].
    pub fn from_context(ctx: DASContext) -> Self {
        Self { ctx }
    }

    /// Borrow the underlying `DASContext`.
    pub fn context(&self) -> &DASContext {
        &self.ctx
    }
}

/// Parse the committed trusted-setup JSON into a `rust_eth_kzg` [`TrustedSetup`].
fn load_trusted_setup() -> Result<TrustedSetup, KzgError> {
    // `TrustedSetup::from_json` panics on malformed JSON; the file is committed
    // and already validated by the c-kzg path. Catch panics so the trait surface
    // stays `Result`-based.
    std::panic::catch_unwind(|| TrustedSetup::from_json(TRUSTED_SETUP_JSON)).map_err(|_| {
        KzgError::TrustedSetup(
            "rust_eth_kzg TrustedSetup::from_json panicked on committed JSON".into(),
        )
    })
}

fn map_err(e: RekError) -> KzgError {
    // Length-mismatch shapes → MismatchLength; serialization / bad encodings →
    // MalformedInput; everything else → Backend.
    let msg = format!("{e:?}");
    if msg.contains("BatchVerificationInputsMustHaveSameLength")
        || msg.contains("NumCellIndicesNotEqualToNumCells")
    {
        return KzgError::MismatchLength(msg);
    }
    if msg.contains("Serialization")
        || msg.contains("CellIndexOutOfRange")
        || msg.contains("InvalidCommitmentIndex")
        || msg.contains("PolynomialHasInvalidLength")
    {
        return KzgError::MalformedInput(msg);
    }
    KzgError::Backend(msg)
}

/// Map a verify `Result<(), Error>` onto the trait verdict contract.
///
/// - `Ok(())` → `Ok(true)`
/// - `Err` with `is_proof_invalid()` → `Ok(false)`  (**only** this path)
/// - any other `Err` → `Err(KzgError::…)`
fn map_verify_result(result: Result<(), RekError>) -> Result<bool, KzgError> {
    match result {
        Ok(()) => Ok(true),
        Err(e) if e.is_proof_invalid() => Ok(false),
        Err(e) => Err(map_err(e)),
    }
}

fn to_types_commitment(c: rust_eth_kzg::KZGCommitment) -> KzgCommitment {
    KzgCommitment::from_array(c)
}

fn to_types_proof(p: rust_eth_kzg::KZGProof) -> KzgProof {
    KzgProof::from_array(p)
}

fn to_types_cell(c: rust_eth_kzg::Cell) -> Cell {
    Cell::from_array(*c)
}

fn map_cells(cells: [rust_eth_kzg::Cell; REK_CELLS]) -> Cells {
    Box::new(cells.map(to_types_cell))
}

fn map_proofs(proofs: [rust_eth_kzg::KZGProof; REK_CELLS]) -> CellProofs {
    Box::new(proofs.map(to_types_proof))
}

fn cell_array_ref(cell: &Cell) -> &[u8; BYTES_PER_CELL] {
    // `Cell` is a transparent newtype over `[u8; BYTES_PER_CELL]`.
    &cell.0
}

impl CellKzg for RustEthKzgBackend {
    fn blob_to_kzg_commitment(&self, blob: &Blob) -> Result<KzgCommitment, KzgError> {
        let commitment = self
            .ctx
            .blob_to_kzg_commitment(blob.as_array())
            .map_err(map_err)?;
        Ok(to_types_commitment(commitment))
    }

    fn compute_cells(&self, blob: &Blob) -> Result<Cells, KzgError> {
        let cells = self.ctx.compute_cells(blob.as_array()).map_err(map_err)?;
        Ok(map_cells(cells))
    }

    fn compute_cells_and_kzg_proofs(&self, blob: &Blob) -> Result<CellsAndProofs, KzgError> {
        let (cells, proofs) = self
            .ctx
            .compute_cells_and_kzg_proofs(blob.as_array())
            .map_err(map_err)?;
        Ok((map_cells(cells), map_proofs(proofs)))
    }

    fn recover_cells_and_kzg_proofs(
        &self,
        cell_indices: &[u64],
        cells: &[Cell],
    ) -> Result<CellsAndProofs, KzgError> {
        if cell_indices.len() != cells.len() {
            return Err(KzgError::MismatchLength(format!(
                "There are {} cell indices and {} cells",
                cell_indices.len(),
                cells.len()
            )));
        }
        // Normalisation 2: trait takes slices; build owned ref Vecs for backend.
        let indices: Vec<u64> = cell_indices.to_vec();
        let cell_refs: Vec<&[u8; BYTES_PER_CELL]> = cells.iter().map(cell_array_ref).collect();
        let (recovered_cells, recovered_proofs) = self
            .ctx
            .recover_cells_and_kzg_proofs(indices, cell_refs)
            .map_err(map_err)?;
        Ok((map_cells(recovered_cells), map_proofs(recovered_proofs)))
    }

    fn verify_cell_kzg_proof_batch(
        &self,
        commitments: &[KzgCommitment],
        cell_indices: &[u64],
        cells: &[Cell],
        proofs: &[KzgProof],
    ) -> Result<bool, KzgError> {
        // Pre-check length mismatches so the trait contract is independent of
        // whether the backend checks them first (same as c-kzg adapter).
        if cells.len() != commitments.len() {
            return Err(KzgError::MismatchLength(format!(
                "There are {} cells and {} commitments",
                cells.len(),
                commitments.len()
            )));
        }
        if cells.len() != cell_indices.len() {
            return Err(KzgError::MismatchLength(format!(
                "There are {} cells and {} cell indices",
                cells.len(),
                cell_indices.len()
            )));
        }
        if cells.len() != proofs.len() {
            return Err(KzgError::MismatchLength(format!(
                "There are {} cells and {} proofs",
                cells.len(),
                proofs.len()
            )));
        }

        // Normalisation 2: build owned reference vectors for rust_eth_kzg.
        let commitment_refs: Vec<&[u8; 48]> = commitments.iter().map(|c| c.as_array()).collect();
        let proof_refs: Vec<&[u8; 48]> = proofs.iter().map(|p| p.as_array()).collect();
        let cell_refs: Vec<&[u8; BYTES_PER_CELL]> = cells.iter().map(cell_array_ref).collect();

        // Normalisation 1: map proof-verification-failure → Ok(false).
        map_verify_result(self.ctx.verify_cell_kzg_proof_batch(
            commitment_refs,
            cell_indices,
            cell_refs,
            proof_refs,
        ))
    }
}

/// Compile-time cross-check against `cc-types` constants (Architecture §4.3).
const _: () = {
    assert!(constants::CELLS_PER_EXT_BLOB == CELLS_PER_EXT_BLOB);
    assert!(constants::FIELD_ELEMENTS_PER_CELL == cc_types::FIELD_ELEMENTS_PER_CELL);
    assert!(constants::BYTES_PER_CELL == BYTES_PER_CELL);
    assert!(constants::BYTES_PER_BLOB == super::BYTES_PER_BLOB);
};
