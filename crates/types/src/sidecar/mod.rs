//! Fulu data-column sidecar and partial-column containers (Architecture §3.5).
//!
//! Completeness is governed by the on-disk `ssz_static` listing (CC-10/1), not
//! this module's prose.

use ssz_derive::{Decode, Encode};
use ssz_types::{BitList, FixedVector, VariableList};
use tree_hash_derive::TreeHash;
use typenum::{U1, U4, U128};

use crate::containers::SignedBeaconBlockHeader;
use crate::preset::Preset;
use crate::primitives::{Cell, KzgCommitment, KzgProof, Root};

/// Merkle proof depth for `blob_kzg_commitments` in `BeaconBlockBody`
/// (`KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH = 4`).
pub type KzgCommitmentsInclusionProofDepth = U4;

/// `NUMBER_OF_COLUMNS` as a typenum capacity (128).
pub type NumberOfColumns = U128;

/// Spec `DataColumnSidecar` (Fulu DAS).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct DataColumnSidecar<P: Preset> {
    /// Column index in the extended matrix.
    pub index: u64,
    /// Cells for this column (one per blob commitment).
    pub column: VariableList<Cell, P::MaxBlobCommitmentsPerBlock>,
    /// KZG commitments matching the column cells.
    pub kzg_commitments: VariableList<KzgCommitment, P::MaxBlobCommitmentsPerBlock>,
    /// KZG proofs for the cells.
    pub kzg_proofs: VariableList<KzgProof, P::MaxBlobCommitmentsPerBlock>,
    /// Signed header of the block that produced the column.
    pub signed_block_header: SignedBeaconBlockHeader,
    /// Merkle inclusion proof of `kzg_commitments` in the block body.
    pub kzg_commitments_inclusion_proof: FixedVector<Root, KzgCommitmentsInclusionProofDepth>,
}

impl<P: Preset> Default for DataColumnSidecar<P> {
    fn default() -> Self {
        Self {
            index: 0,
            column: VariableList::default(),
            kzg_commitments: VariableList::default(),
            kzg_proofs: VariableList::default(),
            signed_block_header: SignedBeaconBlockHeader::default(),
            kzg_commitments_inclusion_proof: FixedVector::default(),
        }
    }
}

/// Spec `MatrixEntry` (Fulu DAS helper).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct MatrixEntry {
    /// Cell payload (2048 bytes). Manual `Debug` via [`Cell`].
    pub cell: Cell,
    /// KZG proof for the cell.
    pub kzg_proof: KzgProof,
    /// Column index.
    pub column_index: u64,
    /// Row index.
    pub row_index: u64,
}

impl Default for MatrixEntry {
    fn default() -> Self {
        Self {
            cell: Cell::ZERO,
            kzg_proof: KzgProof::default(),
            column_index: 0,
            row_index: 0,
        }
    }
}

/// Spec `DataColumnsByRootIdentifier` (Fulu p2p).
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct DataColumnsByRootIdentifier {
    /// Beacon block root.
    pub block_root: Root,
    /// Requested column indices.
    pub columns: VariableList<u64, NumberOfColumns>,
}

/// Spec `PartialDataColumnHeader` (Fulu partial-columns).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PartialDataColumnHeader<P: Preset> {
    /// Blob KZG commitments for the block.
    pub kzg_commitments: VariableList<KzgCommitment, P::MaxBlobCommitmentsPerBlock>,
    /// Signed header of the producing block.
    pub signed_block_header: SignedBeaconBlockHeader,
    /// Inclusion proof of `kzg_commitments` in the body root.
    pub kzg_commitments_inclusion_proof: FixedVector<Root, KzgCommitmentsInclusionProofDepth>,
}

impl<P: Preset> Default for PartialDataColumnHeader<P> {
    fn default() -> Self {
        Self {
            kzg_commitments: VariableList::default(),
            signed_block_header: SignedBeaconBlockHeader::default(),
            kzg_commitments_inclusion_proof: FixedVector::default(),
        }
    }
}

/// Spec `PartialDataColumnGroupID` (Fulu partial-columns).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PartialDataColumnGroupID {
    /// Beacon block root identifying the partial-message group.
    pub beacon_block_root: Root,
}

/// Spec `PartialDataColumnPartsMetadata` (Fulu partial-columns).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PartialDataColumnPartsMetadata<P: Preset> {
    /// Bitmap of cells the peer holds.
    pub available: BitList<P::MaxBlobCommitmentsPerBlock>,
    /// Bitmap of cells the peer wants / is willing to provide.
    pub requests: BitList<P::MaxBlobCommitmentsPerBlock>,
}

impl<P: Preset> Default for PartialDataColumnPartsMetadata<P> {
    fn default() -> Self {
        Self {
            available: empty_bitlist(),
            requests: empty_bitlist(),
        }
    }
}

/// Spec `PartialDataColumnSidecar` (Fulu partial-columns).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PartialDataColumnSidecar<P: Preset> {
    /// Bitmap of which cells are present in `partial_column`.
    pub cells_present_bitmap: BitList<P::MaxBlobCommitmentsPerBlock>,
    /// Present cells (order matches set bits of the bitmap).
    pub partial_column: VariableList<Cell, P::MaxBlobCommitmentsPerBlock>,
    /// KZG proofs for the present cells.
    pub kzg_proofs: VariableList<KzgProof, P::MaxBlobCommitmentsPerBlock>,
    /// Optional header (eager push only); length 0 or 1.
    pub header: VariableList<PartialDataColumnHeader<P>, U1>,
}

impl<P: Preset> Default for PartialDataColumnSidecar<P> {
    fn default() -> Self {
        Self {
            cells_present_bitmap: empty_bitlist(),
            partial_column: VariableList::default(),
            kzg_proofs: VariableList::default(),
            header: VariableList::default(),
        }
    }
}

fn empty_bitlist<N: typenum::Unsigned + Clone>() -> BitList<N> {
    match BitList::with_capacity(0) {
        Ok(b) => b,
        Err(_) => unreachable!("BitList::with_capacity(0) is infallible for Unsigned N"),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::preset::Mainnet;
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;

    #[test]
    fn data_column_sidecar_default_roundtrip() {
        let s = DataColumnSidecar::<Mainnet>::default();
        let bytes = s.as_ssz_bytes();
        assert_eq!(
            DataColumnSidecar::<Mainnet>::from_ssz_bytes(&bytes)
                .unwrap_or_else(|e| panic!("{e:?}")),
            s
        );
        let _ = s.tree_hash_root();
    }

    #[test]
    fn matrix_entry_cell_debug_is_compact() {
        let e = MatrixEntry::default();
        let dbg = format!("{e:?}");
        assert!(dbg.contains("Cell(len=2048"));
        assert!(!dbg.contains(&"00".repeat(100)));
    }
}
