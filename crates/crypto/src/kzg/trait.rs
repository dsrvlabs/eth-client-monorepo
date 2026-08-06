//! [`CellKzg`] trait — EIP-7594 cell API (Architecture §4.2).
//!
//! # Verdict contract
//!
//! `verify_cell_kzg_proof_batch` returns:
//!
//! - **`Ok(true)`** — the batch is cryptographically valid
//! - **`Ok(false)`** — the proof is **invalid** (not an error)
//! - **`Err`** — malformed input or an internal fault
//!
//! The two must never be confused. This normalises to `c-kzg`'s
//! `Result<bool, _>` shape (ADR-P1-05). CC-11c's `rust_eth_kzg` adapter must
//! map the proof-verification-failure variant to `Ok(false)` and everything
//! else to `Err`.

use cc_types::{Cell, KzgCommitment, KzgProof, CELLS_PER_EXT_BLOB};

/// Byte length of a Deneb/Fulu blob (`BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_BLOB`).
pub const BYTES_PER_BLOB: usize =
    cc_types::BYTES_PER_FIELD_ELEMENT * cc_types::FIELD_ELEMENTS_PER_BLOB;

/// All cells for one extended blob.
pub type Cells = Box<[Cell; CELLS_PER_EXT_BLOB]>;
/// All cell proofs for one extended blob.
pub type CellProofs = Box<[KzgProof; CELLS_PER_EXT_BLOB]>;
/// Cells and matching proofs from compute / recover.
pub type CellsAndProofs = (Cells, CellProofs);

/// A blob: 4096 field elements × 32 bytes = 131_072 bytes.
///
/// Heap-backed so construction does not risk stack overflow.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Blob(Box<[u8; BYTES_PER_BLOB]>);

impl Blob {
    /// All-zero blob.
    #[allow(clippy::expect_used)] // allocation of a fixed size cannot fail in practice
    pub fn zero() -> Self {
        Self(vec![0u8; BYTES_PER_BLOB]
            .into_boxed_slice()
            .try_into()
            .expect("BYTES_PER_BLOB length"))
    }

    /// Construct from a fixed-size array (moved onto the heap).
    pub fn from_array(bytes: [u8; BYTES_PER_BLOB]) -> Self {
        Self(Box::new(bytes))
    }

    /// Construct from a byte slice; length must be [`BYTES_PER_BLOB`].
    pub fn from_slice(bytes: &[u8]) -> Result<Self, KzgError> {
        if bytes.len() != BYTES_PER_BLOB {
            return Err(KzgError::InvalidBytesLength {
                expected: BYTES_PER_BLOB,
                got: bytes.len(),
            });
        }
        let mut owned = Self::zero();
        owned.0.copy_from_slice(bytes);
        Ok(owned)
    }

    /// Fill every byte with `value` (useful for deterministic unit tests).
    pub fn filled(value: u8) -> Self {
        let mut b = Self::zero();
        b.0.fill(value);
        b
    }

    /// Borrow the underlying bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// Borrow the fixed array.
    pub fn as_array(&self) -> &[u8; BYTES_PER_BLOB] {
        &self.0
    }
}

impl Default for Blob {
    fn default() -> Self {
        Self::zero()
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::fmt::Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Blob(len={}, head={:02x}{:02x}{:02x}{:02x}…)",
            BYTES_PER_BLOB, self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

/// Cell-KZG errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KzgError {
    /// Byte encoding has the wrong length.
    #[error("KZG invalid bytes length: expected {expected}, got {got}")]
    InvalidBytesLength {
        /// Expected length in bytes.
        expected: usize,
        /// Actual length in bytes.
        got: usize,
    },
    /// Paired argument slices have mismatched lengths.
    #[error("KZG length mismatch: {0}")]
    MismatchLength(String),
    /// Input bytes are malformed (not a valid field element / point, etc.).
    #[error("KZG malformed input: {0}")]
    MalformedInput(String),
    /// Trusted setup failed to load or parse.
    #[error("KZG trusted setup error: {0}")]
    TrustedSetup(String),
    /// Backend is not available (feature disabled at compile time).
    #[error("KZG backend unavailable: {0}")]
    BackendUnavailable(String),
    /// Underlying backend error not covered above.
    #[error("KZG backend error: {0}")]
    Backend(String),
}

/// EIP-7594 cell KZG API (Architecture §4.2).
///
/// Slice arguments in, owned results out. Verdict contract for
/// [`verify_cell_kzg_proof_batch`](CellKzg::verify_cell_kzg_proof_batch):
/// `Ok(true)` = valid, **`Ok(false)` = invalid proof**, `Err` = malformed /
/// internal fault.
pub trait CellKzg: Send + Sync + 'static {
    /// Compute the KZG commitment for a blob.
    fn blob_to_kzg_commitment(&self, blob: &Blob) -> Result<KzgCommitment, KzgError>;

    /// Compute the cells for a blob (no proofs).
    fn compute_cells(&self, blob: &Blob) -> Result<Cells, KzgError>;

    /// Compute cells and their KZG proofs for a blob.
    fn compute_cells_and_kzg_proofs(&self, blob: &Blob) -> Result<CellsAndProofs, KzgError>;

    /// Recover all cells and proofs from a sufficient subset.
    ///
    /// Present in the trait even though its Phase-1 consumer is absent — the
    /// general-preset vectors (when available) cover it.
    fn recover_cells_and_kzg_proofs(
        &self,
        cell_indices: &[u64],
        cells: &[Cell],
    ) -> Result<CellsAndProofs, KzgError>;

    /// Batch-verify cell KZG proofs.
    ///
    /// # Verdict
    ///
    /// - `Ok(true)` — valid
    /// - `Ok(false)` — **invalid proof** (not an error)
    /// - `Err` — malformed input or internal fault
    fn verify_cell_kzg_proof_batch(
        &self,
        commitments: &[KzgCommitment],
        cell_indices: &[u64],
        cells: &[Cell],
        proofs: &[KzgProof],
    ) -> Result<bool, KzgError>;
}
