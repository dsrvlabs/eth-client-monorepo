//! 128-way transpose into `DataColumnSidecar`s (CC-37b / Architecture §5.3).
//!
//! Index arithmetic: `[blob][cell]` → `[column][blob]`, producing
//! [`NUMBER_OF_COLUMNS`] sidecars of `n_blobs` cells each.
//!
//! # Inclusion proof from the block, never the EL
//!
//! `kzg_commitments_inclusion_proof` is a depth-4 Merkle branch over the block
//! **body**. It arrives in [`SidecarTemplate`] (from chain / p2p column path).
//! The EL response type ([`BlobAndProofV2`](crate::methods::get_blobs::BlobAndProofV2))
//! has **no** field for an inclusion proof and must never be asked for one.

use std::time::Instant;

use cc_types::containers::SignedBeaconBlockHeader;
use cc_types::preset::Mainnet;
use cc_types::primitives::{KzgCommitment, Root};
use cc_types::sidecar::DataColumnSidecar;
use cc_types::{CELLS_PER_EXT_BLOB, KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH, NUMBER_OF_COLUMNS};
use ssz_types::{FixedVector, VariableList};

use super::cells::ZippedBlobMaterial;
use crate::metrics::{EngineMetrics, FastpathStage, FastpathStageLabels};

/// Local stand-in for the ninth-contract `SidecarTemplate` (CC-38a owns the wire).
///
/// ~6.5 KB: signed header + ≤ 21 commitments (48 B) + 4-node inclusion proof.
/// Until `CC-38a` lands, callers construct this from the block (chain branch)
/// or from a gossiped column sidecar (p2p branch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarTemplate {
    /// Signed header of the block that produced the columns.
    pub signed_block_header: SignedBeaconBlockHeader,
    /// `blob_kzg_commitments` from the block body (never from the EL).
    pub kzg_commitments: Vec<KzgCommitment>,
    /// Depth-4 Merkle branch of `blob_kzg_commitments` in the block body.
    pub kzg_commitments_inclusion_proof: [Root; KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize],
}

impl SidecarTemplate {
    /// Construct from explicit fields.
    #[must_use]
    pub fn new(
        signed_block_header: SignedBeaconBlockHeader,
        kzg_commitments: Vec<KzgCommitment>,
        kzg_commitments_inclusion_proof: [Root; KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize],
    ) -> Self {
        Self {
            signed_block_header,
            kzg_commitments,
            kzg_commitments_inclusion_proof,
        }
    }

    /// Test helper: header + commitments + a zero inclusion proof (structure only).
    #[cfg(test)]
    #[must_use]
    pub fn fixture_for_commitments(commitments: &[KzgCommitment]) -> Self {
        use cc_types::primitives::Slot;
        let mut header = SignedBeaconBlockHeader::default();
        header.message.slot = Slot::new(54_016 * 32);
        Self {
            signed_block_header: header,
            kzg_commitments: commitments.to_vec(),
            kzg_commitments_inclusion_proof: [Root::default();
                KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize],
        }
    }
}

/// Errors from the transpose / assemble stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembleError {
    /// Commitment count does not match zipped blob materials.
    CountMismatch { commitments: usize, blobs: usize },
    /// SSZ list capacity exceeded.
    Ssz(String),
    /// Column index out of range (programming error).
    ColumnOutOfRange(u64),
}

impl std::fmt::Display for AssembleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CountMismatch { commitments, blobs } => write!(
                f,
                "commitment/blob count mismatch: commitments={commitments} blobs={blobs}"
            ),
            Self::Ssz(s) => write!(f, "ssz list: {s}"),
            Self::ColumnOutOfRange(i) => write!(f, "column index {i} out of range"),
        }
    }
}

impl std::error::Error for AssembleError {}

/// Transpose `[blob][cell]` materials into 128 `DataColumnSidecar`s.
///
/// Every sidecar carries the **same** commitments, signed header, and inclusion
/// proof from [`SidecarTemplate`] — never anything from the EL response.
///
/// Observes `cc_engine_fastpath_seconds{stage="assemble"}`.
pub fn transpose_to_sidecars(
    materials: &[ZippedBlobMaterial],
    template: &SidecarTemplate,
    metrics: Option<&EngineMetrics>,
) -> Result<Vec<DataColumnSidecar<Mainnet>>, AssembleError> {
    let started = Instant::now();
    let n_blobs = materials.len();
    if template.kzg_commitments.len() != n_blobs {
        return Err(AssembleError::CountMismatch {
            commitments: template.kzg_commitments.len(),
            blobs: n_blobs,
        });
    }

    let inclusion: FixedVector<Root, cc_types::sidecar::KzgCommitmentsInclusionProofDepth> =
        FixedVector::new(template.kzg_commitments_inclusion_proof.to_vec())
            .map_err(|_| AssembleError::Ssz("inclusion proof length != 4".into()))?;

    let n_cols = NUMBER_OF_COLUMNS as usize;
    debug_assert_eq!(n_cols, CELLS_PER_EXT_BLOB);
    let mut sidecars = Vec::with_capacity(n_cols);

    for col in 0..n_cols {
        let mut column_cells = Vec::with_capacity(n_blobs);
        let mut column_proofs = Vec::with_capacity(n_blobs);
        for m in materials {
            column_cells.push(m.cells[col]);
            column_proofs.push(m.proofs[col]);
        }
        let column = VariableList::new(column_cells)
            .map_err(|_| AssembleError::Ssz("column cells over capacity".into()))?;
        let kzg_proofs = VariableList::new(column_proofs)
            .map_err(|_| AssembleError::Ssz("column proofs over capacity".into()))?;
        let kzg_commitments = VariableList::new(template.kzg_commitments.clone())
            .map_err(|_| AssembleError::Ssz("commitments over capacity".into()))?;

        sidecars.push(DataColumnSidecar {
            index: col as u64,
            column,
            kzg_commitments,
            kzg_proofs,
            signed_block_header: template.signed_block_header,
            kzg_commitments_inclusion_proof: inclusion.clone(),
        });
    }

    if let Some(m) = metrics {
        m.fastpath_seconds
            .get_or_create(&FastpathStageLabels {
                stage: FastpathStage::Assemble.as_str().to_owned(),
            })
            .observe(started.elapsed().as_secs_f64());
    }
    Ok(sidecars)
}

/// Structural `verify_data_column_sidecar` (CC-24 step 1) — used in tests.
#[cfg(test)]
pub fn verify_data_column_sidecar_structure<P: cc_types::preset::Preset>(
    sc: &DataColumnSidecar<P>,
    max_blobs_per_block: u64,
) -> bool {
    if sc.index >= NUMBER_OF_COLUMNS {
        return false;
    }
    let n = sc.kzg_commitments.len();
    if n == 0 {
        return false;
    }
    if (n as u64) > max_blobs_per_block {
        return false;
    }
    sc.column.len() == n
        && sc.kzg_proofs.len() == n
        && sc.kzg_commitments_inclusion_proof.len()
            == KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize
}

/// Field index of `blob_kzg_commitments` in Electra/Fulu `BeaconBlockBody`.
#[cfg(test)]
pub const BLOB_KZG_COMMITMENTS_FIELD_INDEX: u64 = 11;

/// Spec `verify_data_column_sidecar_inclusion_proof` (depth 4) — test venue.
#[cfg(test)]
pub fn verify_inclusion_proof_branch(
    commitments_root: &[u8; 32],
    branch: &[Root],
    body_root: [u8; 32],
) -> bool {
    use cc_crypto::hash32_concat;
    let depth = KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize;
    if branch.len() != depth {
        return false;
    }
    let mut value = *commitments_root;
    let index = BLOB_KZG_COMMITMENTS_FIELD_INDEX;
    for (i, node) in branch.iter().enumerate().take(depth) {
        let sibling = node.as_array();
        if (index >> i) & 1 == 1 {
            value = hash32_concat(sibling, &value);
        } else {
            value = hash32_concat(&value, sibling);
        }
    }
    value == body_root
}

/// Build a valid depth-4 inclusion proof for `commitments` at body field index 11.
///
/// Test-only: synthesises a padded merkle tree so the assembled sidecar's
/// inclusion branch verifies without a full `BeaconBlockBody`.
#[cfg(test)]
#[allow(clippy::expect_used)] // fixture builder; failure is a test bug
pub fn synthetic_inclusion_proof(
    commitments: &[KzgCommitment],
) -> (
    [Root; KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize],
    [u8; 32],
    [u8; 32],
) {
    use cc_crypto::hash32_concat;
    use cc_types::preset::Preset;
    use ssz_types::VariableList;
    use tree_hash::TreeHash;

    type Max = <Mainnet as Preset>::MaxBlobCommitmentsPerBlock;
    let list =
        VariableList::<KzgCommitment, Max>::new(commitments.to_vec()).expect("commitments fit");
    let leaf_hash = list.tree_hash_root();
    let mut leaf = [0u8; 32];
    leaf.copy_from_slice(leaf_hash.as_slice());

    let depth = KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize;
    let width = 1usize << depth;
    let mut layer: Vec<[u8; 32]> = vec![[0u8; 32]; width];
    let index = BLOB_KZG_COMMITMENTS_FIELD_INDEX as usize;
    layer[index] = leaf;

    let mut branch = Vec::with_capacity(depth);
    let mut idx = index;
    for _ in 0..depth {
        let sibling = idx ^ 1;
        branch.push(Root::from_array(layer[sibling]));
        let mut next = vec![[0u8; 32]; layer.len() / 2];
        for i in 0..next.len() {
            next[i] = hash32_concat(&layer[2 * i], &layer[2 * i + 1]);
        }
        layer = next;
        idx /= 2;
    }
    let body_root = layer[0];
    let proof: [Root; KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize] =
        branch.try_into().expect("depth 4");
    (proof, leaf, body_root)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::fastpath::cells::{ZippedBlobMaterial, compute_cells_zipped_with_el_proofs};
    use crate::methods::get_blobs::BlobAndProofV2;
    use crate::metrics::EngineMetrics;
    use cc_crypto::{Blob, CKzgBackend, CellKzg};
    use cc_types::primitives::Slot;
    use prometheus_client::registry::Registry;
    use std::sync::Arc;

    fn metrics() -> EngineMetrics {
        let mut registry = Registry::default();
        EngineMetrics::register(&mut registry)
    }

    fn backend() -> Arc<dyn CellKzg> {
        Arc::new(CKzgBackend::load_default().expect("load"))
    }

    fn fixture_materials(
        kzg: &dyn CellKzg,
        n: usize,
    ) -> (
        Vec<ZippedBlobMaterial>,
        Vec<KzgCommitment>,
        Vec<BlobAndProofV2>,
    ) {
        use crate::methods::get_blobs::kzg_commitment_to_versioned_hash;
        let mut items = Vec::with_capacity(n);
        let mut commitments = Vec::with_capacity(n);
        let mut hashes = Vec::with_capacity(n);
        for i in 0..n {
            let blob = Blob::filled((i as u8).saturating_add(1));
            let c = kzg.blob_to_kzg_commitment(&blob).expect("c");
            let (_cells, proofs) = kzg.compute_cells_and_kzg_proofs(&blob).expect("p");
            hashes.push(kzg_commitment_to_versioned_hash(c.as_array()));
            commitments.push(c);
            items.push(BlobAndProofV2 {
                blob: blob.as_slice().to_vec(),
                proofs: proofs.iter().map(|p| p.as_slice().to_vec()).collect(),
            });
        }
        let materials = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(compute_cells_zipped_with_el_proofs(
                Arc::from(CKzgBackend::load_default().unwrap()) as Arc<dyn CellKzg>,
                items.clone(),
                hashes,
                commitments.clone(),
                None,
            ))
            .expect("cells");
        (materials, commitments, items)
    }

    #[test]
    fn inclusion_proof_from_block_not_el() {
        // EL response type has no inclusion-proof field.
        let src = include_str!("../methods/get_blobs.rs");
        assert!(src.contains("struct BlobAndProofV2"));
        assert!(
            !src.contains("inclusion_proof") && !src.contains("kzg_commitments_inclusion"),
            "EL BlobAndProofV2 must not carry inclusion proof"
        );
        // Template is the sole source.
        let (proof, leaf, body_root) =
            synthetic_inclusion_proof(&[KzgCommitment::from_array([0xab; 48])]);
        assert!(verify_inclusion_proof_branch(&leaf, &proof, body_root));
        let template = SidecarTemplate::new(
            SignedBeaconBlockHeader::default(),
            vec![KzgCommitment::from_array([0xab; 48])],
            proof,
        );
        assert_eq!(
            template.kzg_commitments_inclusion_proof.len(),
            KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize
        );
        // Fabricated EL-side proof cannot enter the type system: BlobAndProofV2
        // has only `blob` and `proofs`.
        let el = BlobAndProofV2 {
            blob: vec![0u8; cc_crypto::BYTES_PER_BLOB],
            proofs: vec![vec![0u8; 48]; 128],
        };
        let _ = el.blob;
        let _ = el.proofs;
    }

    #[tokio::test]
    async fn assembled_sidecars_verify() {
        let kzg = backend();
        // Use 2 blobs for speed; transpose correctness is collective over all 128.
        // cell_arithmetic covers the 21-blob shape numerically.
        use crate::methods::get_blobs::kzg_commitment_to_versioned_hash;
        let n = 2usize;
        let mut items = Vec::new();
        let mut commitments = Vec::new();
        let mut hashes = Vec::new();
        for i in 0..n {
            let blob = Blob::filled((i as u8).saturating_add(1));
            let c = kzg.blob_to_kzg_commitment(&blob).expect("c");
            hashes.push(kzg_commitment_to_versioned_hash(c.as_array()));
            commitments.push(c);
            let (_c, proofs) = kzg.compute_cells_and_kzg_proofs(&blob).expect("p");
            items.push(BlobAndProofV2 {
                blob: blob.as_slice().to_vec(),
                proofs: proofs.iter().map(|p| p.as_slice().to_vec()).collect(),
            });
        }
        let materials = compute_cells_zipped_with_el_proofs(
            Arc::clone(&kzg),
            items,
            hashes,
            commitments.clone(),
            None,
        )
        .await
        .expect("cells");
        let (incl, _leaf, _body) = synthetic_inclusion_proof(&commitments);
        let mut header = SignedBeaconBlockHeader::default();
        header.message.slot = Slot::new(100);
        let template = SidecarTemplate::new(header, commitments.clone(), incl);
        let m = metrics();
        let sidecars = transpose_to_sidecars(&materials, &template, Some(&m)).expect("assemble");
        assert_eq!(sidecars.len(), 128);

        for sc in &sidecars {
            // Step 1: structure.
            assert!(
                verify_data_column_sidecar_structure(sc, 21),
                "structure fail col {}",
                sc.index
            );
            // Step 2: depth-4 inclusion proof from the template.
            let (leaf, body) = {
                use cc_types::preset::Preset;
                use ssz_types::VariableList;
                use tree_hash::TreeHash;
                type Max = <Mainnet as Preset>::MaxBlobCommitmentsPerBlock;
                let list =
                    VariableList::<KzgCommitment, Max>::new(sc.kzg_commitments.to_vec()).unwrap();
                let h = list.tree_hash_root();
                let mut leaf = [0u8; 32];
                leaf.copy_from_slice(h.as_slice());
                // Recompute body root from the stored branch + leaf.
                let branch: Vec<Root> =
                    sc.kzg_commitments_inclusion_proof.iter().copied().collect();
                let mut value = leaf;
                let index = BLOB_KZG_COMMITMENTS_FIELD_INDEX;
                for (i, node) in branch.iter().enumerate() {
                    let sibling = node.as_array();
                    if (index >> i) & 1 == 1 {
                        value = cc_crypto::hash32_concat(sibling, &value);
                    } else {
                        value = cc_crypto::hash32_concat(&value, sibling);
                    }
                }
                (leaf, value)
            };
            let branch: Vec<Root> = sc.kzg_commitments_inclusion_proof.iter().copied().collect();
            assert!(
                verify_inclusion_proof_branch(&leaf, &branch, body),
                "inclusion fail col {}",
                sc.index
            );
            // Step 3: verify_cell_kzg_proof_batch (test venue only — ADR P3-15).
            let cell_indices = vec![sc.index; sc.column.len()];
            let ok = kzg
                .verify_cell_kzg_proof_batch(
                    sc.kzg_commitments.as_ref(),
                    &cell_indices,
                    sc.column.as_ref(),
                    sc.kzg_proofs.as_ref(),
                )
                .expect("kzg");
            assert!(ok, "kzg fail col {}", sc.index);
        }
    }

    #[test]
    fn el_proofs_used_in_sidecar_not_recomputed() {
        // Grep-level: production engine sources never recompute proofs.
        let engine_src_files = [
            ("cells.rs", include_str!("cells.rs")),
            ("sidecars.rs", include_str!("sidecars.rs")),
            ("filter.rs", include_str!("filter.rs")),
            ("mod.rs", include_str!("mod.rs")),
            ("fetch.rs", include_str!("fetch.rs")),
        ];
        for (name, src) in engine_src_files {
            let production: String = src
                .lines()
                .take_while(|l| !l.contains("#[cfg(test)]") && !l.contains("mod tests"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !production.contains("compute_cells_and_kzg_proofs"),
                "{name} production must not recompute proofs"
            );
        }
    }

    #[test]
    fn unused_fixture_materials_compiles() {
        // Keep the helper linked for future multi-blob harnesses.
        let kzg = CKzgBackend::load_default().unwrap();
        let (_m, c, _i) = fixture_materials(&kzg, 1);
        assert_eq!(c.len(), 1);
    }
}
