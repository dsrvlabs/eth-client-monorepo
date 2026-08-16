//! [`ArchiveWrite`] over the live P0 writer mailbox (S2-A-05).
//!
//! Uses [`ColumnBatch::index`] as the durable key. Does not peek a sidecar
//! byte offset and does not fall back to index 0. Continuity bind is S2-A-06.

use std::sync::Arc;

use async_trait::async_trait;
use cc_seam::{ArchiveWrite, ColumnBatch, SeamError};
use cc_store::columns::{MIN_COLUMN_SSZ_LEN, NUMBER_OF_COLUMNS};
use cc_store::engine::{Engine, StoreError};
use cc_store::{Root, Slot};

use crate::writer::{CommitUnit, StagedColumn, WriterError, WriterHandle, load_write_cursor};

/// Typed ingest adapter. Holds the live writer handle — no second mailbox.
#[derive(Debug, Clone)]
pub(crate) struct ArchiveWriter {
    writer: WriterHandle,
    engine: Arc<Engine>,
}

impl ArchiveWriter {
    pub(crate) fn new(writer: WriterHandle, engine: Arc<Engine>) -> Self {
        Self { writer, engine }
    }
}

fn staged_from_batch(batch: ColumnBatch) -> Result<StagedColumn, SeamError> {
    let index = u16::try_from(batch.index).map_err(|_| {
        SeamError::InvalidArgument(format!("column index {} exceeds u16 domain", batch.index))
    })?;
    if index >= NUMBER_OF_COLUMNS {
        return Err(SeamError::InvalidArgument(format!(
            "column index {index} ≥ NUMBER_OF_COLUMNS ({NUMBER_OF_COLUMNS})"
        )));
    }
    if batch.ssz.len() < MIN_COLUMN_SSZ_LEN {
        return Err(SeamError::InvalidArgument(format!(
            "malformed column sidecar: len {} below MIN_COLUMN_SSZ_LEN ({MIN_COLUMN_SSZ_LEN})",
            batch.ssz.len()
        )));
    }
    Ok(StagedColumn {
        slot: Slot::new(batch.slot),
        root: Root::from_array(batch.block_root),
        index,
        ssz: batch.ssz.to_vec(),
    })
}

fn map_writer_err(err: WriterError) -> SeamError {
    match err {
        WriterError::ShutDown => SeamError::Unavailable("writer shut down".into()),
        WriterError::Store(StoreError::Codec(msg) | StoreError::Limit(msg)) => {
            SeamError::InvalidArgument(msg)
        }
        WriterError::Store(e) => SeamError::Unavailable(e.to_string()),
        WriterError::InjectedFailure => SeamError::Unavailable("injected commit failure".into()),
    }
}

#[async_trait]
impl ArchiveWrite for ArchiveWriter {
    async fn ingest_columns(&self, batch: ColumnBatch) -> Result<(), SeamError> {
        let column = staged_from_batch(batch)?;
        // CommitUnit always restamps KEY_WRITE_CURSOR (D-4). Never invent
        // session_id=0/seq=0 — that rewinds a missing cursor to zeros.
        // A-06 is the (parent_root, slot) bind, not this restamp.
        let cursor = load_write_cursor(&self.engine)
            .map_err(|e| SeamError::Unavailable(e.to_string()))?
            .ok_or_else(|| {
                SeamError::Unavailable(
                    "no durable write cursor; refuse to invent a zero cursor".into(),
                )
            })?;
        let unit = CommitUnit {
            blocks: Vec::new(),
            columns: vec![column],
            fork_choice: None,
            cursor,
            done: None,
        };
        self.writer
            .submit_p0_committed(unit)
            .await
            .map_err(map_writer_err)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use crate::writer::{WriterBounds, WriterFaults, spawn_writer};
    use cc_seam::Bytes;
    use cc_store::columns::{
        COLUMN_HEADER_SLOT_SSZ_OFFSET, COLUMN_INDEX_SSZ_OFFSET, DATA_COLUMN_SIDECAR_FIXED_BYTES,
        get_column_by_root,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::BlockRegion;
    use cc_store::meta::WriteCursor;
    use prometheus_client::registry::Registry;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::watch;

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-archive-write-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn eng(label: &str) -> (PathBuf, Arc<Engine>) {
        let dir = tmp_dir(label);
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        (dir, Arc::new(eng))
    }

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn synth_sidecar(index: u16, slot: u64) -> Vec<u8> {
        let mut v = vec![0u8; DATA_COLUMN_SIDECAR_FIXED_BYTES];
        v[COLUMN_INDEX_SSZ_OFFSET..COLUMN_INDEX_SSZ_OFFSET + 8]
            .copy_from_slice(&u64::from(index).to_le_bytes());
        v[COLUMN_HEADER_SLOT_SSZ_OFFSET..COLUMN_HEADER_SLOT_SSZ_OFFSET + 8]
            .copy_from_slice(&slot.to_le_bytes());
        v
    }

    fn batch(index: u64, slot: u64, ssz: Vec<u8>) -> ColumnBatch {
        ColumnBatch {
            slot,
            block_root: [0xAB; 32],
            index,
            ssz: Bytes::from(ssz),
        }
    }

    #[test]
    fn staged_uses_batch_index_not_ssz_guess() {
        let ssz = synth_sidecar(7, 11);
        let col = staged_from_batch(batch(7, 11, ssz.clone())).unwrap();
        assert_eq!(col.index, 7);
        assert_eq!(col.slot, Slot::new(11));
        assert_eq!(col.ssz, ssz);
    }

    #[test]
    fn short_sidecar_is_rejected() {
        let err = staged_from_batch(batch(0, 1, vec![0u8; 2])).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
    }

    #[test]
    fn index_over_u16_is_rejected() {
        let ssz = synth_sidecar(0, 1);
        let err = staged_from_batch(batch(u64::from(u16::MAX) + 1, 1, ssz)).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn ingest_persists_typed_index() {
        let (dir, engine) = eng("persist");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle.clone(), Arc::clone(&engine));
        let seed = WriteCursor {
            session_id: 7,
            seq: 11,
            slot: Slot::new(19),
            root: Root::from_array([0xCD; 32]),
        };
        handle
            .submit_p0_committed(CommitUnit::cursor_only(seed))
            .await
            .unwrap();
        let ssz = synth_sidecar(5, 20);
        archive
            .ingest_columns(batch(5, 20, ssz.clone()))
            .await
            .unwrap();

        let rt = engine.read().unwrap();
        let got = get_column_by_root(
            &rt,
            &Root::from_array([0xAB; 32]),
            5,
            Some(BlockRegion::Hot),
        )
        .unwrap()
        .unwrap();
        assert_eq!(got, ssz);
        let cursor = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(cursor.session_id, 7);
        assert_eq!(cursor.seq, 11, "ingest must not rewind KEY_WRITE_CURSOR");
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ingest_rejects_index_ssz_mismatch() {
        let (dir, engine) = eng("mismatch");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle.clone(), Arc::clone(&engine));
        handle
            .submit_p0_committed(CommitUnit::cursor_only(WriteCursor {
                session_id: 1,
                seq: 1,
                slot: Slot::new(1),
                root: Root::from_array([0x11; 32]),
            }))
            .await
            .unwrap();
        // Caller names index 3; SSZ body is column 1 — reject, do not store as 0.
        let err = archive
            .ingest_columns(batch(3, 20, synth_sidecar(1, 20)))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        let rt = engine.read().unwrap();
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0xAB; 32]),
                0,
                Some(BlockRegion::Hot)
            )
            .unwrap()
            .is_none()
        );
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0xAB; 32]),
                3,
                Some(BlockRegion::Hot)
            )
            .unwrap()
            .is_none()
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ingest_without_durable_cursor_is_rejected() {
        let (dir, engine) = eng("no-cursor");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle, Arc::clone(&engine));
        let err = archive
            .ingest_columns(batch(5, 20, synth_sidecar(5, 20)))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)));
        assert!(load_write_cursor(&engine).unwrap().is_none());
        let rt = engine.read().unwrap();
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0xAB; 32]),
                5,
                Some(BlockRegion::Hot)
            )
            .unwrap()
            .is_none()
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
