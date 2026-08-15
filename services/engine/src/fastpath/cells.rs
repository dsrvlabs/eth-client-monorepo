//! Cell extension on the blocking pool (CC-37b / Architecture §5.3, ADR P3-12).
//!
//! The EL returns blobs plus **exactly** [`CELLS_PER_EXT_BLOB`] cell **proofs**
//! per blob. The CL computes the **cells** itself with
//! [`CellKzg::compute_cells`] and zips them index-wise with the EL proofs.
//!
//! # ADR P3-12
//!
//! Every `compute_cells` call runs inside `tokio::task::spawn_blocking` — never
//! on a tokio worker. Up to 21 × 128 = 2 688 cell computations is head-of-line
//! blocking material; the ordered lane (attestation deadline) must stay free.
//!
//! # OQ-P3-3 closed
//!
//! `CellKzg::compute_cells` is declared at `crates/crypto/src/kzg/trait.rs:142`
//! and implemented at `c_kzg.rs:98` via the extension direction alone. This
//! path **never** recomputes proofs the EL already supplied (R-14 does not fire).
//!
//! # ADR P3-15 (production)
//!
//! Spec SHOULD: when clients use the local execution layer to retrieve blobs,
//! they SHOULD skip verification of those blobs (`fulu/p2p-interface.md`).
//! Production skips cell-proof batch verification here — the blobs came from a
//! trusted local process. Verification still runs **in the test** over every
//! assembled sidecar (see `sidecars` / filter tests).

use std::sync::Arc;
use std::time::Instant;

use cc_crypto::{BYTES_PER_BLOB, Blob, CellKzg, Cells, KzgError};
use cc_types::primitives::KzgCommitment;
use cc_types::{CELLS_PER_EXT_BLOB, Cell, KzgProof};

use crate::methods::get_blobs::{
    BYTES_PER_KZG_PROOF, BlobAndProofV2, CELL_PROOFS_PER_BLOB, kzg_commitment_to_versioned_hash,
};
use crate::metrics::{EngineMetrics, FastpathStage, FastpathStageLabels};

/// Per-blob cells (ours) zipped with EL-supplied proofs.
#[derive(Debug, Clone)]
pub struct ZippedBlobMaterial {
    /// 128 cells from [`CellKzg::compute_cells`].
    pub cells: Cells,
    /// 128 cell proofs from the EL (`BlobAndProofV2.proofs`).
    pub proofs: Box<[KzgProof; CELLS_PER_EXT_BLOB]>,
    /// Commitment of the blob (bound to request hash / template).
    pub commitment: KzgCommitment,
}

/// Errors from the cell-extension stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellsError {
    /// Blob bytes were the wrong length or otherwise unusable.
    InvalidBlob(String),
    /// EL proofs were the wrong count or length.
    InvalidProofs(String),
    /// Blob did not match the requested versioned hash or template commitment.
    BindingMismatch(String),
    /// Count mismatch between blobs and request hashes / commitments.
    CountMismatch(String),
    /// KZG backend failure.
    Kzg(String),
    /// Blocking-pool join failure.
    Join(String),
}

impl std::fmt::Display for CellsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBlob(s) => write!(f, "invalid blob: {s}"),
            Self::InvalidProofs(s) => write!(f, "invalid EL proofs: {s}"),
            Self::BindingMismatch(s) => write!(f, "blob binding: {s}"),
            Self::CountMismatch(s) => write!(f, "count mismatch: {s}"),
            Self::Kzg(s) => write!(f, "kzg: {s}"),
            Self::Join(s) => write!(f, "spawn_blocking join: {s}"),
        }
    }
}

impl std::error::Error for CellsError {}

impl From<KzgError> for CellsError {
    fn from(e: KzgError) -> Self {
        Self::Kzg(e.to_string())
    }
}

/// Parse EL-supplied cell proofs (exactly 128 × 48 B) into typed proofs.
pub fn parse_el_proofs(
    proofs: &[Vec<u8>],
) -> Result<Box<[KzgProof; CELLS_PER_EXT_BLOB]>, CellsError> {
    if proofs.len() != CELL_PROOFS_PER_BLOB {
        return Err(CellsError::InvalidProofs(format!(
            "expected {CELL_PROOFS_PER_BLOB} proofs, got {}",
            proofs.len()
        )));
    }
    let mut out = Box::new([KzgProof::default(); CELLS_PER_EXT_BLOB]);
    for (i, p) in proofs.iter().enumerate() {
        if p.len() != BYTES_PER_KZG_PROOF {
            return Err(CellsError::InvalidProofs(format!(
                "proof[{i}] length {} != {BYTES_PER_KZG_PROOF}",
                p.len()
            )));
        }
        let mut arr = [0u8; 48];
        arr.copy_from_slice(p);
        out[i] = KzgProof::from_array(arr);
    }
    Ok(out)
}

/// Synchronous bind + cell computation + zip (blocking pool only).
///
/// **Integrity bind (security F1, cheap under ADR P3-15):** for each blob,
/// `blob_to_kzg_commitment` → versioned hash must equal the request hash, and
/// the commitment must equal the template commitment. Full cell-proof batch
/// verification remains skipped in production (ADR P3-15).
///
/// Callers outside tests must go through [`compute_cells_zipped_with_el_proofs`].
fn compute_cells_zipped_sync(
    kzg: &dyn CellKzg,
    blobs_and_proofs: &[BlobAndProofV2],
    versioned_hashes: &[[u8; 32]],
    template_commitments: &[KzgCommitment],
) -> Result<Vec<ZippedBlobMaterial>, CellsError> {
    if blobs_and_proofs.len() != versioned_hashes.len() {
        return Err(CellsError::CountMismatch(format!(
            "blobs {} != versioned_hashes {}",
            blobs_and_proofs.len(),
            versioned_hashes.len()
        )));
    }
    if blobs_and_proofs.len() != template_commitments.len() {
        return Err(CellsError::CountMismatch(format!(
            "blobs {} != template commitments {}",
            blobs_and_proofs.len(),
            template_commitments.len()
        )));
    }

    let mut out = Vec::with_capacity(blobs_and_proofs.len());
    for (i, item) in blobs_and_proofs.iter().enumerate() {
        if item.blob.len() != BYTES_PER_BLOB {
            return Err(CellsError::InvalidBlob(format!(
                "blob[{i}] length {} != {BYTES_PER_BLOB}",
                item.blob.len()
            )));
        }
        let blob = Blob::from_slice(&item.blob)
            .map_err(|e| CellsError::InvalidBlob(format!("blob[{i}]: {e}")))?;

        // Cheap integrity bind before extension (not full proof batch verify).
        let commitment = kzg.blob_to_kzg_commitment(&blob)?;
        let vh = kzg_commitment_to_versioned_hash(commitment.as_array());
        if vh != versioned_hashes[i] {
            return Err(CellsError::BindingMismatch(format!(
                "blob[{i}] versioned hash mismatch vs request"
            )));
        }
        if commitment != template_commitments[i] {
            return Err(CellsError::BindingMismatch(format!(
                "blob[{i}] commitment mismatch vs SidecarTemplate"
            )));
        }

        // ADR P3-12: this is the sole production `compute_cells` call site in
        // `services/engine` and it lives inside a `spawn_blocking` closure
        // (see `compute_cells_zipped_with_el_proofs` below). Extension only —
        // proofs come from the EL; we never recompute proofs on this path.
        let cells = kzg.compute_cells(&blob)?;
        let proofs = parse_el_proofs(&item.proofs)?;
        debug_assert_eq!(cells.len(), CELLS_PER_EXT_BLOB);
        debug_assert_eq!(proofs.len(), CELLS_PER_EXT_BLOB);
        let _ = Cell::LEN; // keep cell size visible to readers of the zip.
        out.push(ZippedBlobMaterial {
            cells,
            proofs,
            commitment,
        });
    }
    Ok(out)
}

/// Compute cells for every blob on the **blocking pool**, zip with EL proofs.
///
/// Binds each blob to `versioned_hashes[i]` and `template_commitments[i]` via
/// `blob_to_kzg_commitment` before extension (security F1).
///
/// Observes:
/// - `cc_engine_cells_computed_total` += `n_blobs × 128`
/// - `cc_engine_fastpath_seconds{stage="compute_cells"}`
///
/// # ADR P3-15
///
/// Production path does **not** run cell-proof batch verification. Spec SHOULD
/// (`fulu/p2p-interface.md`): when clients use the local execution layer to
/// retrieve blobs, they SHOULD skip verification of those blobs — they came
/// from a trusted local process. The bind above is **not** that batch verify;
/// it is a cheap integrity check that ADR P3-15 does not forbid.
pub async fn compute_cells_zipped_with_el_proofs(
    kzg: Arc<dyn CellKzg>,
    blobs_and_proofs: Vec<BlobAndProofV2>,
    versioned_hashes: Vec<[u8; 32]>,
    template_commitments: Vec<KzgCommitment>,
    metrics: Option<&EngineMetrics>,
) -> Result<Vec<ZippedBlobMaterial>, CellsError> {
    let started = Instant::now();
    let n_blobs = blobs_and_proofs.len();

    let material = tokio::task::spawn_blocking(move || {
        compute_cells_zipped_sync(
            kzg.as_ref(),
            &blobs_and_proofs,
            &versioned_hashes,
            &template_commitments,
        )
    })
    .await
    .map_err(|e| CellsError::Join(e.to_string()))??;

    if let Some(m) = metrics {
        let cells = (n_blobs as u64).saturating_mul(CELLS_PER_EXT_BLOB as u64);
        m.cells_computed.inc_by(cells);
        m.fastpath_seconds
            .get_or_create(&FastpathStageLabels {
                stage: FastpathStage::ComputeCells.as_str().to_owned(),
            })
            .observe(started.elapsed().as_secs_f64());
    }
    Ok(material)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_crypto::CKzgBackend;
    use prometheus_client::registry::Registry;

    fn metrics() -> EngineMetrics {
        let mut registry = Registry::default();
        EngineMetrics::register(&mut registry)
    }

    fn backend() -> Arc<dyn CellKzg> {
        Arc::new(CKzgBackend::load_default().expect("load c-kzg"))
    }

    /// Valid field-element blob (not `Blob::filled` — constant bytes can make
    /// cell proofs collide across seeds and weaken the "foreign proof" check).
    fn test_blob(seed: u8) -> Blob {
        let mut bytes = vec![0u8; BYTES_PER_BLOB];
        for (i, chunk) in bytes.chunks_mut(32).enumerate() {
            chunk[0] = 0;
            chunk[1] = seed;
            chunk[2] = (i as u8).wrapping_mul(3).wrapping_add(1);
            chunk[3] = seed.wrapping_mul(7).wrapping_add(i as u8);
        }
        Blob::from_slice(&bytes).expect("BYTES_PER_BLOB")
    }

    fn el_item(kzg: &dyn CellKzg, seed: u8) -> BlobAndProofV2 {
        let blob = test_blob(seed);
        // Simulate the EL: it supplies proofs (we use the full path only to
        // *generate* the fixture; the production path under test never does).
        let (_cells, proofs) = kzg
            .compute_cells_and_kzg_proofs(&blob)
            .expect("fixture proofs");
        BlobAndProofV2 {
            blob: blob.as_slice().to_vec(),
            proofs: proofs.iter().map(|p| p.as_slice().to_vec()).collect(),
        }
    }

    /// Bind inputs for `n` fixture seeds: items, versioned hashes, commitments.
    fn fixture_batch(
        kzg: &dyn CellKzg,
        seeds: impl IntoIterator<Item = u8>,
    ) -> (Vec<BlobAndProofV2>, Vec<[u8; 32]>, Vec<KzgCommitment>) {
        let mut items = Vec::new();
        let mut hashes = Vec::new();
        let mut commits = Vec::new();
        for seed in seeds {
            let blob = test_blob(seed);
            let c = kzg.blob_to_kzg_commitment(&blob).expect("c");
            hashes.push(kzg_commitment_to_versioned_hash(c.as_array()));
            commits.push(c);
            items.push(el_item(kzg, seed));
        }
        (items, hashes, commits)
    }

    #[tokio::test]
    async fn compute_cells_does_not_block_ordered_lane() {
        use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
        use crate::jwt::JwtSecret;
        use crate::methods::names;
        use crate::metrics::EngineMethod;
        use crate::transport::{EngineTransport, Lane, SharedTransport};
        use serde_json::json;
        use std::time::{Duration, Instant};
        use wiremock::matchers::method as http_method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc":"2.0","id":1,
                "result":{"status":"VALID","latestValidHash":null,"validationError":null}
            })))
            .mount(&server)
            .await;

        let t: SharedTransport = Arc::new(EngineTransport::from_parts(
            server.uri(),
            JwtSecret::from_bytes([0x37; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs {
                new_payload_ms: 2_000,
                forkchoice_updated_ms: 2_000,
                get_blobs_ms: 1_000,
                exchange_capabilities_ms: 1_000,
                eth_syncing_ms: 1_000,
                multiplier: 1.0,
            }),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            None,
        ));

        let kzg = backend();
        let (items, hashes, commits) = fixture_batch(kzg.as_ref(), 0..21);
        let cells_fut =
            compute_cells_zipped_with_el_proofs(Arc::clone(&kzg), items, hashes, commits, None);
        let np_fut = async {
            let started = Instant::now();
            let r = t
                .call(
                    Lane::Ordered,
                    EngineMethod::NewPayloadV4,
                    names::NEW_PAYLOAD_V4,
                    json!([{}]),
                )
                .await;
            (r, started.elapsed())
        };

        let (cells_res, (np_res, np_elapsed)) = tokio::join!(cells_fut, np_fut);
        assert!(cells_res.is_ok(), "cells: {cells_res:?}");
        assert!(np_res.is_ok(), "newPayload: {np_res:?}");
        assert!(
            np_elapsed < Duration::from_secs(2),
            "newPayload delayed by compute_cells: {np_elapsed:?}"
        );
    }

    #[tokio::test]
    async fn cell_arithmetic_21_blobs() {
        let m = metrics();
        let kzg = backend();
        let (items, hashes, commits) = fixture_batch(kzg.as_ref(), 0..21);
        let before = m.cells_computed.get();
        let material = compute_cells_zipped_with_el_proofs(
            Arc::clone(&kzg),
            items,
            hashes,
            commits.clone(),
            Some(&m),
        )
        .await
        .expect("cells");
        assert_eq!(material.len(), 21);
        for z in &material {
            assert_eq!(z.cells.len(), 128);
            assert_eq!(z.proofs.len(), 128);
        }
        let after = m.cells_computed.get();
        assert_eq!(
            after - before,
            2_688,
            "cc_engine_cells_computed_total must +2688 for 21-blob block"
        );
        let assembled = crate::fastpath::sidecars::transpose_to_sidecars(
            &material,
            &crate::fastpath::sidecars::SidecarTemplate::fixture_for_commitments(&commits),
            Some(&m),
        )
        .expect("transpose");
        assert_eq!(assembled.len(), 128);
        for sc in &assembled {
            assert_eq!(sc.column.len(), 21);
        }
    }

    #[tokio::test]
    async fn binding_rejects_mismatched_versioned_hash() {
        let kzg = backend();
        let (items, mut hashes, commits) = fixture_batch(kzg.as_ref(), [1u8]);
        hashes[0][31] ^= 0xff;
        let err =
            compute_cells_zipped_with_el_proofs(Arc::clone(&kzg), items, hashes, commits, None)
                .await
                .expect_err("must bind");
        assert!(matches!(err, CellsError::BindingMismatch(_)), "got {err:?}");
    }

    #[test]
    fn compute_cells_only_inside_spawn_blocking_source() {
        let src = include_str!("cells.rs");
        // Every `compute_cells` invocation must sit under spawn_blocking.
        assert!(src.contains("spawn_blocking"));
        assert!(src.contains("kzg.compute_cells("));
        // Production region: no proof-recompute API, no cell-proof batch verify.
        let production: String = src
            .lines()
            .take_while(|l| !l.contains("mod tests"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !production.contains("compute_cells_and_kzg_proofs"),
            "production cells.rs must not recompute proofs"
        );
        assert!(
            !production.contains("verify_cell_kzg_proof_batch"),
            "production cells.rs must not verify EL-supplied cells (ADR P3-15)"
        );
        assert!(
            production.contains("SHOULD skip verification")
                || production.contains("skip verification"),
            "production path must name the spec's SHOULD"
        );
    }

    #[tokio::test]
    async fn proofs_come_from_el_not_computed() {
        let kzg = backend();
        let (items, hashes, commits) = fixture_batch(kzg.as_ref(), [7u8]);
        let item = items[0].clone();
        let material = compute_cells_zipped_with_el_proofs(
            Arc::clone(&kzg),
            items,
            hashes.clone(),
            commits.clone(),
            None,
        )
        .await
        .expect("cells");
        let commitment = commits[0];
        // Honest EL proofs verify.
        let ok = kzg
            .verify_cell_kzg_proof_batch(
                &[commitment],
                &[0],
                &[material[0].cells[0]],
                &[material[0].proofs[0]],
            )
            .expect("honest");
        assert!(ok, "honest EL proof must verify");

        // Replace proof[0] with a different blob's proof[0] (well-formed, wrong).
        let mut item2 = item;
        let other = el_item(kzg.as_ref(), 3);
        assert_ne!(
            other.proofs[0], item2.proofs[0],
            "fixture seeds must yield distinct EL proofs"
        );
        item2.proofs[0] = other.proofs[0].clone();
        let material2 = compute_cells_zipped_with_el_proofs(
            Arc::clone(&kzg),
            vec![item2],
            hashes,
            commits,
            None,
        )
        .await
        .expect("cells with foreign EL proof");
        assert_eq!(
            material2[0].cells[0].as_slice(),
            material[0].cells[0].as_slice(),
            "cells are computed from the blob, independent of EL proofs"
        );
        assert_ne!(
            material2[0].proofs[0].as_slice(),
            material[0].proofs[0].as_slice(),
            "pipeline must carry the (mutated) EL proof bytes through"
        );
        let verdict = kzg
            .verify_cell_kzg_proof_batch(
                &[commitment],
                &[0],
                &[material2[0].cells[0]],
                &[material2[0].proofs[0]],
            )
            .expect("well-formed foreign proof");
        assert!(
            !verdict,
            "foreign EL proof must fail in-test verification — proves proofs are the EL's"
        );
        // Grep-level: production engine sources never call the recompute API.
        for path in ["cells.rs", "sidecars.rs", "filter.rs", "mod.rs", "fetch.rs"] {
            let src = match path {
                "cells.rs" => include_str!("cells.rs"),
                "sidecars.rs" => include_str!("sidecars.rs"),
                "filter.rs" => include_str!("filter.rs"),
                "mod.rs" => include_str!("mod.rs"),
                "fetch.rs" => include_str!("fetch.rs"),
                _ => unreachable!(),
            };
            let production: String = src
                .lines()
                .take_while(|l| !l.contains("#[cfg(test)]") && !l.contains("mod tests"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !production.contains("compute_cells_and_kzg_proofs"),
                "{path} production must not call compute_cells_and_kzg_proofs"
            );
        }
    }
}
