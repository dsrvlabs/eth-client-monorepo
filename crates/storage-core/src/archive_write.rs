//! [`ArchiveWrite`] over the live P0 writer mailbox (S2-A-05 / S2-A-06).
//!
//! Uses [`ColumnBatch::index`] as the durable key. Does not peek a sidecar
//! byte offset and does not fall back to index 0.
//!
//! Every batch submitted to the writer carries, at its head, the
//! `(parent_root, slot)` of the block the batch's columns and canonical
//! rows attach to. The writer rejects the batch unless that parent is
//! already durable, or is the first row of the same batch. There is no
//! "progress optional" path and no empty-progress bypass.
//!
//! A batch may only extend the durable frontier, never jump it.

use std::sync::Arc;

use async_trait::async_trait;
use cc_seam::{ArchiveWrite, ColumnBatch, SeamError};
use cc_store::columns::{
    COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, MIN_COLUMN_SSZ_LEN, NUMBER_OF_COLUMNS,
    column_parent_root_at_offset,
};
use cc_store::engine::{Engine, StoreError};
use cc_store::{Root, Slot};

use crate::writer::{
    CommitUnit, StagedBlock, StagedColumn, WriterError, WriterHandle, block_present,
    load_write_cursor,
};

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
    let ssz_parent = column_parent_root_at_offset(batch.ssz.as_ref())
        .map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    if ssz_parent.as_slice() != batch.parent_root.as_slice() {
        return Err(SeamError::InvalidArgument(format!(
            "column parent_root mismatch: caller != SSZ header parent_root at offset {COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET}"
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

/// Head of a writer batch: the `(parent_root, slot)` rows attach to.
#[derive(Debug, Clone, Copy)]
struct ContinuityHead {
    parent_root: Root,
    slot: Slot,
}

/// Writer-facing batch. [`None`] head is only the rejected empty-progress encoding.
#[derive(Debug)]
struct WriterBatch {
    head: Option<ContinuityHead>,
    blocks: Vec<StagedBlock>,
    columns: Vec<StagedColumn>,
}

/// A batch may only extend the durable frontier, never jump it.
///
/// Same invariant as `S0-B-10` on `PutBackfillBatch`. The writer rejects
/// the batch unless the named parent is already durable, or is the first
/// row of the same batch. There is no "progress optional" path and no
/// empty-progress bypass.
fn admit_top_of_batch_continuity(engine: &Engine, batch: &WriterBatch) -> Result<(), SeamError> {
    let Some(head) = batch.head else {
        return Err(SeamError::InvalidArgument(
            "continuity bind is required; empty-progress batches are rejected".into(),
        ));
    };
    if batch
        .blocks
        .first()
        .is_some_and(|row| row.root == head.parent_root)
    {
        return Ok(());
    }
    match block_present(engine, &head.parent_root) {
        Ok(true) => Ok(()),
        Ok(false) => Err(SeamError::InvalidArgument(format!(
            "a batch may only extend the durable frontier, never jump it \
             (parent is not durable and is not the first row of the same batch; \
              slot={})",
            head.slot.as_u64()
        ))),
        Err(e) => Err(SeamError::Unavailable(e.to_string())),
    }
}

impl ArchiveWriter {
    async fn submit_writer_batch(&self, batch: WriterBatch) -> Result<(), SeamError> {
        admit_top_of_batch_continuity(&self.engine, &batch)?;
        // CommitUnit always restamps KEY_WRITE_CURSOR (D-4). Never invent
        // session_id=0/seq=0 — that rewinds a missing cursor to zeros.
        let cursor = load_write_cursor(&self.engine)
            .map_err(|e| SeamError::Unavailable(e.to_string()))?
            .ok_or_else(|| {
                SeamError::Unavailable(
                    "no durable write cursor; refuse to invent a zero cursor".into(),
                )
            })?;
        let unit = CommitUnit {
            blocks: batch.blocks,
            columns: batch.columns,
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

#[async_trait]
impl ArchiveWrite for ArchiveWriter {
    async fn ingest_columns(&self, batch: ColumnBatch) -> Result<(), SeamError> {
        let head = ContinuityHead {
            parent_root: Root::from_array(batch.parent_root),
            slot: Slot::new(batch.slot),
        };
        let column = staged_from_batch(batch)?;
        self.submit_writer_batch(WriterBatch {
            head: Some(head),
            blocks: Vec::new(),
            columns: vec![column],
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use crate::writer::{WriterBounds, WriterFaults, WriterHandle, spawn_writer};
    use cc_seam::Bytes;
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
    };
    use cc_store::columns::{
        COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, COLUMN_HEADER_SLOT_SSZ_OFFSET,
        COLUMN_INDEX_SSZ_OFFSET, DATA_COLUMN_SIDECAR_FIXED_BYTES, get_column_by_root,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::BlockRegion;
    use cc_store::meta::WriteCursor;
    use cc_store::{get_block_by_root, put_block};
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
        synth_sidecar_with_parent(index, slot, &parent_root())
    }

    fn synth_sidecar_with_parent(index: u16, slot: u64, parent: &Root) -> Vec<u8> {
        let mut v = vec![0u8; DATA_COLUMN_SIDECAR_FIXED_BYTES];
        v[COLUMN_INDEX_SSZ_OFFSET..COLUMN_INDEX_SSZ_OFFSET + 8]
            .copy_from_slice(&u64::from(index).to_le_bytes());
        v[COLUMN_HEADER_SLOT_SSZ_OFFSET..COLUMN_HEADER_SLOT_SSZ_OFFSET + 8]
            .copy_from_slice(&slot.to_le_bytes());
        v[COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET..COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET + 32]
            .copy_from_slice(parent.as_slice());
        v
    }

    fn parent_root() -> Root {
        Root::from_array([0x11; 32])
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        v
    }

    fn batch(index: u64, slot: u64, ssz: Vec<u8>) -> ColumnBatch {
        ColumnBatch {
            parent_root: parent_root().into_array(),
            slot,
            block_root: [0xAB; 32],
            index,
            ssz: Bytes::from(ssz),
        }
    }

    fn staged_parent_block(root: Root, slot: u64) -> StagedBlock {
        StagedBlock {
            slot: Slot::new(slot),
            root,
            ssz: synth_block(slot, &Root::ZERO, &Root::from_array([0xF0; 32])),
            update_canonical: false,
            write_state_root: false,
            da_status: None,
        }
    }

    async fn seed_cursor_and_parent(handle: &WriterHandle) {
        let parent = parent_root();
        handle
            .submit_p0_committed(CommitUnit {
                blocks: vec![staged_parent_block(parent, 19)],
                columns: vec![],
                fork_choice: None,
                cursor: WriteCursor {
                    session_id: 7,
                    seq: 11,
                    slot: Slot::new(19),
                    root: parent,
                },
                done: None,
            })
            .await
            .unwrap();
    }

    fn seed_parent_direct(engine: &Engine) {
        let parent = parent_root();
        let ssz = synth_block(19, &Root::ZERO, &Root::from_array([0xF0; 32]));
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            Slot::new(19),
            &parent,
            &ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        drop(rt);
        engine.commit(batch).unwrap();
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
    fn staged_rejects_parent_root_ssz_mismatch() {
        let ssz = synth_sidecar_with_parent(7, 11, &Root::from_array([0x22; 32]));
        let err = staged_from_batch(batch(7, 11, ssz)).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(err.to_string().contains("parent_root mismatch"), "{err}");
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
        seed_cursor_and_parent(&handle).await;
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
        seed_cursor_and_parent(&handle).await;
        // Caller names index 3; SSZ body is column 1 — reject, do not store as 0.
        let err = archive
            .ingest_columns(batch(3, 20, synth_sidecar(1, 20)))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("index mismatch"),
            "must reach put_column index bind, not frontier admit: {msg}"
        );
        assert!(
            !msg.contains("durable frontier"),
            "must reach put_column index bind, not frontier admit: {msg}"
        );
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
        seed_parent_direct(&engine);
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

    #[test]
    fn first_row_of_same_batch_is_accepted_without_durable_parent() {
        let (_dir, engine) = eng("first-row");
        let parent = parent_root();
        let batch = WriterBatch {
            head: Some(ContinuityHead {
                parent_root: parent,
                slot: Slot::new(19),
            }),
            blocks: vec![staged_parent_block(parent, 19)],
            columns: vec![],
        };
        assert!(admit_top_of_batch_continuity(&engine, &batch).is_ok());
        assert!(!block_present(&engine, &parent).unwrap());
        let _ = std::fs::remove_dir_all(&_dir);
    }

    // Named identically to S0-B-10 so the pair is greppable.
    #[tokio::test]
    async fn put_backfill_batch_empty_progress_single_block_batch_is_rejected() {
        let (dir, engine) = eng("empty-progress");
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
        let root = Root::from_array([0x42; 32]);
        let err = archive
            .submit_writer_batch(WriterBatch {
                head: None,
                blocks: vec![staged_parent_block(root, 7)],
                columns: vec![],
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::InvalidArgument(_)),
            "empty-progress single-block batch must be rejected, not fast-pathed: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("progress is required") || msg.contains("empty-progress"),
            "empty-progress single-block batch must be rejected, not fast-pathed: {msg}"
        );
        let rt = engine.read().unwrap();
        assert!(
            get_block_by_root(&rt, &root).unwrap().is_none(),
            "empty-progress must not persist the block"
        );

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Named identically to S0-B-10 so the pair is greppable.
    #[tokio::test]
    async fn put_backfill_batch_parent_not_durable_and_not_first_row_is_rejected() {
        let (dir, engine) = eng("frontier-jump");
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
                root: Root::from_array([0xEE; 32]),
            }))
            .await
            .unwrap();
        // Parent 0xFF is not durable; the batch has no first block row.
        let jumped = Root::from_array([0xFF; 32]);
        let err = archive
            .ingest_columns(ColumnBatch {
                parent_root: jumped.into_array(),
                slot: 9,
                block_root: [0x33; 32],
                index: 5,
                ssz: Bytes::from(synth_sidecar_with_parent(5, 9, &jumped)),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::InvalidArgument(_)),
            "must state the S0-B-10 / S2-A-06 invariant: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("durable frontier") && msg.contains("never jump"),
            "must state the S0-B-10 / S2-A-06 invariant: {msg}"
        );
        assert!(
            msg.contains("not durable") && msg.contains("not the first row"),
            "{msg}"
        );
        let rt = engine.read().unwrap();
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0x33; 32]),
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
