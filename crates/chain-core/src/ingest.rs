//! Direct typed column ingest (S2-A-05 / `[ARCH]` §4.3).
//!
//! `chain-core` decodes a sidecar only to name [`ColumnBatch`] fields, then
//! calls [`ArchiveWrite::ingest_columns`]. `index` is a field, not a
//! byte-offset guess. A malformed sidecar is rejected. Column bytes do not
//! enter the event ring.

use cc_seam::{ArchiveWrite, Bytes, ColumnBatch, Root, SeamError};
use cc_types::NUMBER_OF_COLUMNS;
use cc_types::preset::Mainnet;
use cc_types::sidecar::DataColumnSidecar;
use ssz::Decode;

/// Decode sidecar SSZ into a typed [`ColumnBatch`].
///
/// Rejects empty, undecodable, or out-of-range sidecars. Never defaults
/// `index` to 0.
pub fn decode_column_batch(ssz: Bytes, block_root: Root) -> Result<ColumnBatch, SeamError> {
    if ssz.is_empty() {
        return Err(SeamError::InvalidArgument(
            "empty column sidecar".to_owned(),
        ));
    }
    let sidecar = DataColumnSidecar::<Mainnet>::from_ssz_bytes(ssz.as_ref())
        .map_err(|e| SeamError::InvalidArgument(format!("malformed column sidecar: {e:?}")))?;
    if sidecar.index >= NUMBER_OF_COLUMNS {
        return Err(SeamError::InvalidArgument(format!(
            "column index {} ≥ NUMBER_OF_COLUMNS ({NUMBER_OF_COLUMNS})",
            sidecar.index
        )));
    }
    Ok(ColumnBatch {
        parent_root: *sidecar.signed_block_header.message.parent_root.as_array(),
        slot: sidecar.signed_block_header.message.slot.as_u64(),
        block_root,
        index: sidecar.index,
        ssz,
    })
}

/// Decode then ingest. Call site for [`ArchiveWrite::ingest_columns`].
pub async fn ingest_column_ssz(
    archive: &dyn ArchiveWrite,
    ssz: Bytes,
    block_root: Root,
) -> Result<ColumnBatch, SeamError> {
    let batch = decode_column_batch(ssz, block_root)?;
    archive.ingest_columns(batch.clone()).await?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use async_trait::async_trait;
    use cc_types::primitives::Slot;
    use cc_types::sidecar::DataColumnSidecar;
    use ssz::Encode;
    use std::sync::{Arc, Mutex};

    fn sidecar_ssz(index: u64, slot: u64) -> Bytes {
        sidecar_ssz_with_parent(index, slot, [0u8; 32])
    }

    fn sidecar_ssz_with_parent(index: u64, slot: u64, parent: [u8; 32]) -> Bytes {
        let mut sc = DataColumnSidecar::<Mainnet> {
            index,
            ..Default::default()
        };
        sc.signed_block_header.message.slot = Slot::new(slot);
        sc.signed_block_header.message.parent_root = cc_types::primitives::Root::from_array(parent);
        Bytes::from(sc.as_ssz_bytes())
    }

    #[derive(Default)]
    struct RecordingArchive {
        batches: Mutex<Vec<ColumnBatch>>,
        fail: Mutex<Option<SeamError>>,
    }

    #[async_trait]
    impl ArchiveWrite for RecordingArchive {
        async fn ingest_columns(&self, batch: ColumnBatch) -> Result<(), SeamError> {
            if let Some(err) = self.fail.lock().unwrap().clone() {
                return Err(err);
            }
            self.batches.lock().unwrap().push(batch);
            Ok(())
        }
    }

    #[test]
    fn decode_names_index_as_a_field() {
        let parent = [0xAA; 32];
        let ssz = sidecar_ssz_with_parent(7, 42, parent);
        let batch = decode_column_batch(ssz.clone(), [0x11; 32]).unwrap();
        assert_eq!(batch.index, 7);
        assert_eq!(batch.parent_root, parent);
        assert_eq!(batch.slot, 42);
        assert_eq!(batch.block_root, [0x11; 32]);
        assert_eq!(batch.ssz, ssz);
    }

    #[test]
    fn empty_sidecar_is_rejected() {
        let err = decode_column_batch(Bytes::new(), [0; 32]).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
    }

    #[test]
    fn opaque_bytes_are_rejected_not_index_zero() {
        let err = decode_column_batch(Bytes::from_static(b"not-a-sidecar"), [0; 32]).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
    }

    #[test]
    fn index_at_column_count_is_rejected() {
        let ssz = sidecar_ssz(NUMBER_OF_COLUMNS, 1);
        let err = decode_column_batch(ssz, [0; 32]).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn ingest_forwards_typed_batch() {
        let archive = RecordingArchive::default();
        let ssz = sidecar_ssz(3, 9);
        let got = ingest_column_ssz(&archive, ssz.clone(), [0x22; 32])
            .await
            .unwrap();
        assert_eq!(got.index, 3);
        let stored = archive.batches.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].index, 3);
        assert_eq!(stored[0].slot, 9);
        assert_eq!(stored[0].ssz, ssz);
    }

    #[tokio::test]
    async fn ingest_does_not_call_archive_on_malformed() {
        let archive = RecordingArchive::default();
        let err = ingest_column_ssz(&archive, Bytes::from_static(b"short"), [0; 32])
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(archive.batches.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ingest_forwards_backpressure() {
        let archive = RecordingArchive::default();
        *archive.fail.lock().unwrap() = Some(SeamError::Backpressure {
            bound: 32,
            waited_ms: 0,
        });
        let err = ingest_column_ssz(&archive, sidecar_ssz(1, 1), [0; 32])
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::Backpressure { bound: 32, .. }),
            "ingest must forward Backpressure, not swallow it as success, got {err:?}"
        );
        assert!(archive.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn recording_archive_is_dyn() {
        let _: Arc<dyn ArchiveWrite> = Arc::new(RecordingArchive::default());
    }
}
