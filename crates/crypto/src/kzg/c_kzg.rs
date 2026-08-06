//! `c-kzg` 2.1.8 backend (Architecture §4.3, CC-11b backend A).

use super::setup::{load_c_kzg_settings, DEFAULT_PRECOMPUTE};
use super::{Blob, CellKzg, CellProofs, Cells, CellsAndProofs, KzgError};
use cc_types::{Cell, KzgCommitment, KzgProof, CELLS_PER_EXT_BLOB};
use c_kzg::{Bytes48, Cell as CkzgCell, KzgSettings};

/// `c-kzg` cell-KZG backend loaded from the committed trusted setup.
pub struct CKzgBackend {
    settings: KzgSettings,
}

impl std::fmt::Debug for CKzgBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CKzgBackend").finish_non_exhaustive()
    }
}

impl CKzgBackend {
    /// Load settings from the committed `trusted_setup.json` with the given
    /// `precompute` parameter (`0..=15`).
    pub fn load(precompute: u64) -> Result<Self, KzgError> {
        Ok(Self {
            settings: load_c_kzg_settings(precompute)?,
        })
    }

    /// Load with [`DEFAULT_PRECOMPUTE`].
    pub fn load_default() -> Result<Self, KzgError> {
        Self::load(DEFAULT_PRECOMPUTE)
    }

    /// Wrap an already-loaded [`KzgSettings`].
    pub fn from_settings(settings: KzgSettings) -> Self {
        Self { settings }
    }

    /// Borrow the underlying `c-kzg` settings.
    pub fn settings(&self) -> &KzgSettings {
        &self.settings
    }
}

fn map_err(e: c_kzg::Error) -> KzgError {
    match &e {
        c_kzg::Error::MismatchLength(s) => KzgError::MismatchLength(s.clone()),
        c_kzg::Error::InvalidBytesLength(s)
        | c_kzg::Error::InvalidHexFormat(s)
        | c_kzg::Error::InvalidKzgProof(s)
        | c_kzg::Error::InvalidKzgCommitment(s) => KzgError::MalformedInput(s.clone()),
        c_kzg::Error::InvalidTrustedSetup(s) => KzgError::TrustedSetup(s.clone()),
        other => KzgError::Backend(other.to_string()),
    }
}

fn to_ckzg_blob(blob: &Blob) -> c_kzg::Blob {
    c_kzg::Blob::new(*blob.as_array())
}

fn to_types_commitment(c: c_kzg::KzgCommitment) -> KzgCommitment {
    KzgCommitment::from_array(*c)
}

fn to_types_proof(p: c_kzg::KzgProof) -> KzgProof {
    KzgProof::from_array(*p)
}

fn to_types_cell(c: CkzgCell) -> Cell {
    Cell::from_array(c.to_bytes())
}

fn to_ckzg_cell(c: &Cell) -> Result<CkzgCell, KzgError> {
    CkzgCell::from_bytes(c.as_slice()).map_err(map_err)
}

fn to_ckzg_bytes48(bytes: &[u8; 48]) -> Bytes48 {
    Bytes48::from(*bytes)
}

fn map_cells(cells: Box<[CkzgCell; CELLS_PER_EXT_BLOB]>) -> Cells {
    Box::new(std::array::from_fn(|i| to_types_cell(cells[i])))
}

fn map_proofs(proofs: Box<[c_kzg::KzgProof; CELLS_PER_EXT_BLOB]>) -> CellProofs {
    Box::new(std::array::from_fn(|i| to_types_proof(proofs[i])))
}

impl CellKzg for CKzgBackend {
    fn blob_to_kzg_commitment(&self, blob: &Blob) -> Result<KzgCommitment, KzgError> {
        let ckzg_blob = to_ckzg_blob(blob);
        let commitment = self
            .settings
            .blob_to_kzg_commitment(&ckzg_blob)
            .map_err(map_err)?;
        Ok(to_types_commitment(commitment))
    }

    fn compute_cells(&self, blob: &Blob) -> Result<Cells, KzgError> {
        let ckzg_blob = to_ckzg_blob(blob);
        let cells = self.settings.compute_cells(&ckzg_blob).map_err(map_err)?;
        Ok(map_cells(cells))
    }

    fn compute_cells_and_kzg_proofs(&self, blob: &Blob) -> Result<CellsAndProofs, KzgError> {
        let ckzg_blob = to_ckzg_blob(blob);
        let (cells, proofs) = self
            .settings
            .compute_cells_and_kzg_proofs(&ckzg_blob)
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
        let ckzg_cells: Result<Vec<CkzgCell>, KzgError> =
            cells.iter().map(to_ckzg_cell).collect();
        let ckzg_cells = ckzg_cells?;
        let (recovered_cells, recovered_proofs) = self
            .settings
            .recover_cells_and_kzg_proofs(cell_indices, &ckzg_cells)
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
        // whether the backend checks them first.
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

        let ckzg_commitments: Vec<Bytes48> = commitments
            .iter()
            .map(|c| to_ckzg_bytes48(c.as_array()))
            .collect();
        let ckzg_proofs: Vec<Bytes48> = proofs
            .iter()
            .map(|p| to_ckzg_bytes48(p.as_array()))
            .collect();
        let ckzg_cells: Result<Vec<CkzgCell>, KzgError> =
            cells.iter().map(to_ckzg_cell).collect();
        let ckzg_cells = ckzg_cells?;

        // c-kzg already returns Result<bool, Error> with Ok(false) = invalid.
        self.settings
            .verify_cell_kzg_proof_batch(
                &ckzg_commitments,
                cell_indices,
                &ckzg_cells,
                &ckzg_proofs,
            )
            .map_err(map_err)
    }
}
