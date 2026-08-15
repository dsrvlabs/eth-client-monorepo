//! Hot/cold migration task (CC-41 / Architecture §3.2).
//!
//! Triggered by `FINALIZED_CHECKPOINT` from the write-behind stream at
//! `storage.epochs_per_migration` (default **1**). One **P1** writer batch runs
//! the four steps inside the split **write** lock:
//!
//! 1. re-key canonical hot → cold for `(old, new]`
//! 2. delete unfinalized non-canonical siblings still in hot (**not** a prune tick;
//!    *Deviations* 2 — lives here, not `prune/unfinalized.rs`)
//! 3. write `Split { slot, state_root, block_root }`
//! 4. commit **via the single writer** (never `Engine::commit` from this task)
//!
//! LOCK ORDER (verbatim with `crates/store/src/split.rs`): every read that can
//! race migration takes `split.read_recursive()` before opening its ReadTxn.
//! Migration acquires `split.write()` first and holds it across stage+writer commit.
//!
//! Large targets are chunked at [`MAX_MIGRATION_SLOTS_PER_BATCH`] so each P1 unit
//! stays under `MAX_BATCH_OPS`.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use cc_store::engine::Engine;
use cc_store::meta::Split;
use cc_store::{
    MAX_MIGRATION_SLOTS_PER_BATCH, MigrationStats, Root, SplitLock, epoch_start_slot,
    migration_window_end, plan_migration, should_migrate_on_finalization,
};
use tracing::{debug, info, warn};

use crate::metrics::StorageMetrics;
use crate::writer::{MetaUpdate, WriterError, WriterHandle};

/// Default cadence (Lighthouse `--epochs-per-migration`).
pub(crate) const DEFAULT_EPOCHS_PER_MIGRATION: u64 = cc_store::DEFAULT_EPOCHS_PER_MIGRATION;

/// Migration knobs from `config/storage.toml`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MigrationConfig {
    /// Fire migration every N finalizations / epochs of advance. Default **1**.
    pub epochs_per_migration: u64,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            epochs_per_migration: DEFAULT_EPOCHS_PER_MIGRATION,
        }
    }
}

/// Shared migration state: split lock, writer handle, cadence, counters, metrics.
#[derive(Debug)]
pub(crate) struct Migrator {
    pub split: Arc<SplitLock>,
    /// Read-only: plan migration under a snapshot. Commits go through [`Self::writer`].
    pub engine: Arc<Engine>,
    /// Sole write path — P1 mailbox (Architecture §1.5 / §3.2).
    pub writer: WriterHandle,
    pub cfg: MigrationConfig,
    pub metrics: StorageMetrics,
    /// How many times a non-noop migrate window committed.
    pub invocations: AtomicU64,
    /// How many FINALIZED_CHECKPOINT events were observed.
    pub finalizations_seen: AtomicU64,
}

impl Migrator {
    pub(crate) fn new(
        split: Arc<SplitLock>,
        engine: Arc<Engine>,
        writer: WriterHandle,
        cfg: MigrationConfig,
        metrics: StorageMetrics,
    ) -> Self {
        let slot = split.snapshot().slot.as_u64();
        metrics
            .split_slot
            .set(i64::try_from(slot).unwrap_or(i64::MAX));
        Self {
            split,
            engine,
            writer,
            cfg,
            metrics,
            invocations: AtomicU64::new(0),
            finalizations_seen: AtomicU64::new(0),
        }
    }

    /// Handle a `FINALIZED_CHECKPOINT` (write-behind stream).
    ///
    /// Payload layout from chain (CC-44a): `8 B epoch LE ‖ 32 B state root ‖ …`.
    /// Event `root` is the finalized block root; `slot` is epoch-start tagging.
    ///
    /// Caller (write-behind) must flush the P0 accumulator **before** this so no
    /// uncommitted hot rows for slots ≤ new split land after the advance.
    pub(crate) async fn on_finalized_checkpoint(
        &self,
        finalized_epoch: u64,
        finalized_root: Root,
        state_root: Root,
    ) -> Result<Option<MigrationStats>, MigrateError> {
        self.finalizations_seen.fetch_add(1, Ordering::SeqCst);

        let snapshot = {
            // LOCK ORDER: read_recursive first.
            let g = self.split.read_recursive();
            *g
        };
        if !should_migrate_on_finalization(
            &snapshot,
            finalized_epoch,
            self.cfg.epochs_per_migration,
        ) {
            debug!(
                target: "cc_storage::migrate",
                finalized_epoch,
                split_slot = snapshot.slot.as_u64(),
                cadence = self.cfg.epochs_per_migration,
                "finalization observed; migration cadence not met"
            );
            return Ok(None);
        }

        let new_split = Split {
            slot: epoch_start_slot(finalized_epoch),
            state_root,
            block_root: finalized_root,
        };

        self.run_migration(new_split).await.map(Some)
    }

    /// Run migration to `new_split` under the split write lock, committing each
    /// window via **writer P1** (single-writer invariant).
    ///
    /// Uses blocking P1 submit while holding the split write guard so in-memory
    /// and durable split advance together (no torn serve-path view). Runs via
    /// `block_in_place` so the multi-thread runtime can schedule other work.
    pub(crate) async fn run_migration(
        &self,
        new_split: Split,
    ) -> Result<MigrationStats, MigrateError> {
        // Requires multi-thread runtime: holds split write + blocking P1 wait.
        // Production `#[tokio::main]` and migrate tests use `flavor = "multi_thread"`.
        tokio::task::block_in_place(|| self.run_migration_blocking(new_split))
    }

    /// Blocking migration: split write held across plan + writer P1 commit per window.
    pub(crate) fn run_migration_blocking(
        &self,
        new_split: Split,
    ) -> Result<MigrationStats, MigrateError> {
        let mut total = MigrationStats::default();
        loop {
            // LOCK ORDER: split write FIRST and held through writer commit.
            let mut guard = self.split.write();
            let old = *guard;
            if new_split.slot.as_u64() <= old.slot.as_u64() {
                return Ok(total);
            }
            let window_end = migration_window_end(old.slot, new_split.slot);
            let window_split = Split {
                slot: window_end,
                state_root: if window_end == new_split.slot {
                    new_split.state_root
                } else {
                    Root::ZERO
                },
                block_root: if window_end == new_split.slot {
                    new_split.block_root
                } else {
                    Root::ZERO
                },
            };
            let done = window_end == new_split.slot;

            let plan = plan_migration(&self.engine, &old, &window_split)?;
            let window_stats = plan.stats;
            if plan.stats.split_written || !plan.puts.is_empty() || !plan.deletes.is_empty() {
                let update = MetaUpdate {
                    puts: plan.puts,
                    deletes: plan.deletes,
                    done: None,
                };
                self.writer
                    .blocking_submit_p1_committed(update)
                    .map_err(MigrateError::Writer)?;
            }

            *guard = window_split;
            drop(guard);

            let slot = window_split.slot.as_u64();
            self.metrics
                .split_slot
                .set(i64::try_from(slot).unwrap_or(i64::MAX));
            self.invocations.fetch_add(1, Ordering::SeqCst);
            total.blocks_moved = total.blocks_moved.saturating_add(window_stats.blocks_moved);
            total.columns_moved = total
                .columns_moved
                .saturating_add(window_stats.columns_moved);
            total.blocks_unfinalized_deleted = total
                .blocks_unfinalized_deleted
                .saturating_add(window_stats.blocks_unfinalized_deleted);
            total.columns_unfinalized_deleted = total
                .columns_unfinalized_deleted
                .saturating_add(window_stats.columns_unfinalized_deleted);
            total.split_written = total.split_written || window_stats.split_written;

            info!(
                target: "cc_storage::migrate",
                split_slot = slot,
                blocks_moved = window_stats.blocks_moved,
                columns_moved = window_stats.columns_moved,
                unfinalized_blocks = window_stats.blocks_unfinalized_deleted,
                window_cap = MAX_MIGRATION_SLOTS_PER_BATCH,
                "hot/cold migration window committed via writer P1"
            );

            if done {
                break;
            }
        }
        Ok(total)
    }

    #[must_use]
    pub(crate) fn invocation_count(&self) -> u64 {
        self.invocations.load(Ordering::SeqCst)
    }

    #[must_use]
    pub(crate) fn split_slot_gauge(&self) -> i64 {
        self.metrics.split_slot.get()
    }
}

/// Migration-layer error (store plan or writer commit).
#[derive(Debug, thiserror::Error)]
pub(crate) enum MigrateError {
    #[error("store: {0}")]
    Store(#[from] cc_store::StoreError),
    #[error("writer: {0}")]
    Writer(WriterError),
}

/// Parse FINALIZED_CHECKPOINT payload head: epoch LE u64 + state root 32 B.
#[must_use]
pub(crate) fn parse_finalized_payload(payload: &[u8]) -> Option<(u64, Root)> {
    if payload.len() < 40 {
        return None;
    }
    let mut epoch_le = [0u8; 8];
    epoch_le.copy_from_slice(&payload[..8]);
    let epoch = u64::from_le_bytes(epoch_le);
    let mut root = [0u8; 32];
    root.copy_from_slice(&payload[8..40]);
    Some((epoch, Root::from_array(root)))
}

/// Decode finalized root from event root bytes.
#[must_use]
pub(crate) fn root_from_event(bytes: &[u8]) -> Root {
    let mut arr = [0u8; 32];
    let n = bytes.len().min(32);
    arr[..n].copy_from_slice(&bytes[..n]);
    Root::from_array(arr)
}

/// Async notify helper used by write-behind: best-effort migrate, log on error.
pub(crate) async fn maybe_migrate_on_finalized(
    migrator: &Migrator,
    epoch: u64,
    finalized_root: Root,
    state_root: Root,
) {
    match migrator
        .on_finalized_checkpoint(epoch, finalized_root, state_root)
        .await
    {
        Ok(Some(stats)) => {
            debug!(
                target: "cc_storage::migrate",
                epoch,
                split_written = stats.split_written,
                "migration ok (writer P1)"
            );
        }
        Ok(None) => {}
        Err(e) => {
            warn!(
                target: "cc_storage::migrate",
                error = %e,
                epoch,
                "migration failed; will retry on next finalization"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::writer::{WriterBounds, WriterFaults, spawn_writer};
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
        measure_class_stats, put_block,
    };
    use cc_store::canonical::put_canonical;
    use cc_store::columns::{
        COLUMN_HEADER_SLOT_SSZ_OFFSET, COLUMN_INDEX_SSZ_OFFSET, MIN_COLUMN_SSZ_LEN,
        measure_column_class_stats, put_column,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::BlockRegion;
    use cc_store::{Slot, SplitLock, epoch_of_slot, load_split};
    use prometheus_client::registry::Registry;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::watch;

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-storage-migrate-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn eng(label: &str) -> Arc<Engine> {
        let dir = tmp_dir(label);
        Arc::new(
            Engine::open(
                &dir,
                EngineOptions::default().with_durability(Durability::None),
            )
            .unwrap(),
        )
    }

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn root_n(n: u8) -> Root {
        Root::from_array([n; 32])
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        v
    }

    fn synth_column(slot: u64, index: u16) -> Vec<u8> {
        let mut v = vec![0u8; MIN_COLUMN_SSZ_LEN];
        v[COLUMN_INDEX_SSZ_OFFSET..COLUMN_INDEX_SSZ_OFFSET + 8]
            .copy_from_slice(&(index as u64).to_le_bytes());
        v[COLUMN_HEADER_SLOT_SSZ_OFFSET..COLUMN_HEADER_SLOT_SSZ_OFFSET + 8]
            .copy_from_slice(&slot.to_le_bytes());
        v
    }

    /// Migrator + live writer task (P1 path).
    fn migrator_with_writer(
        label: &str,
        cfg: MigrationConfig,
    ) -> (Arc<Migrator>, watch::Sender<bool>) {
        let engine = eng(label);
        let split = Arc::new(SplitLock::new(Split::default()));
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = spawn_writer(
            Arc::clone(&engine),
            m.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false, // not process-fatal in tests
        );
        let mig = Arc::new(Migrator::new(split, engine, writer, cfg, m));
        (mig, shutdown_tx)
    }

    /// CC-41/2: at epochs_per_migration = 4, migration fires once per four finalizations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn epochs_per_migration_four_fires_once_per_four() {
        let (m, shutdown) = migrator_with_writer(
            "cadence",
            MigrationConfig {
                epochs_per_migration: 4,
            },
        );

        for epoch in 1u64..=8 {
            let _ = m
                .on_finalized_checkpoint(epoch, root_n(epoch as u8), root_n(0x10 + epoch as u8))
                .await
                .unwrap();
        }
        assert_eq!(
            m.invocation_count(),
            2,
            "epochs_per_migration=4 → migrate at epoch 4 and 8 only"
        );
        assert_eq!(m.finalizations_seen.load(Ordering::SeqCst), 8);
        let _ = shutdown.send(true);
    }

    /// CC-41/3: two-branch fork — non-descendant blocks **and** columns gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_branch_fork_deletes_unfinalized_siblings() {
        let (m, shutdown) = migrator_with_writer("fork", MigrationConfig::default());

        {
            let mut batch = m.engine.batch();
            let rt = m.engine.read().unwrap();
            for s in 1u64..=32 {
                let slot = Slot::new(s);
                let root = root_n(s as u8);
                let parent = if s == 1 {
                    Root::ZERO
                } else {
                    root_n((s - 1) as u8)
                };
                let ssz = synth_block(s, &parent, &root_n(0x80));
                put_block(&rt, &mut batch, slot, &root, &ssz, BlockRegion::Hot, true).unwrap();
                put_canonical(&rt, &mut batch, slot, &root).unwrap();
                let col = synth_column(s, 0);
                put_column(&rt, &mut batch, slot, &root, 0, &col, BlockRegion::Hot).unwrap();
            }
            for s in 10u64..=12 {
                let slot = Slot::new(s);
                let sib = root_n(0xA0 + (s - 10) as u8);
                let ssz = synth_block(s, &root_n((s - 1) as u8), &root_n(0xFE));
                put_block(&rt, &mut batch, slot, &sib, &ssz, BlockRegion::Hot, false).unwrap();
                let col = synth_column(s, 0);
                put_column(&rt, &mut batch, slot, &sib, 0, &col, BlockRegion::Hot).unwrap();
            }
            drop(rt);
            m.engine.commit(batch).unwrap();
        }

        let before_blocks = measure_class_stats(&m.engine).unwrap();
        let before_cols = measure_column_class_stats(&m.engine).unwrap();
        assert_eq!(before_blocks.blocks_rows, 35);
        assert_eq!(before_cols.columns_rows, 35);

        let stats = m
            .on_finalized_checkpoint(1, root_n(32), root_n(0xB2))
            .await
            .unwrap()
            .expect("migration should fire at cadence=1");
        assert!(stats.split_written);

        let after_blocks = measure_class_stats(&m.engine).unwrap();
        let after_cols = measure_column_class_stats(&m.engine).unwrap();
        assert_eq!(
            after_blocks.blocks_rows, 32,
            "non-descendant branch blocks deleted; canonical retained"
        );
        assert_eq!(
            after_cols.columns_rows, 32,
            "non-descendant branch columns deleted; canonical retained"
        );

        let rt = m.engine.read().unwrap();
        let split = load_split(&rt).unwrap().unwrap();
        assert_eq!(split.slot, Slot::new(32));
        drop(rt);
        assert_eq!(m.split_slot_gauge(), 32);
        let _ = shutdown.send(true);
    }

    /// CC-41/6: `cc_storage_split_slot` advances across two finalizations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn split_slot_gauge_advances_across_two_finalizations() {
        let (m, shutdown) = migrator_with_writer("gauge", MigrationConfig::default());
        {
            let mut batch = m.engine.batch();
            let rt = m.engine.read().unwrap();
            for s in 1u64..=64 {
                let slot = Slot::new(s);
                let root = root_n((s % 200 + 1) as u8);
                let ssz = synth_block(s, &Root::ZERO, &root_n(0x11));
                put_block(&rt, &mut batch, slot, &root, &ssz, BlockRegion::Hot, true).unwrap();
                put_canonical(&rt, &mut batch, slot, &root).unwrap();
            }
            drop(rt);
            m.engine.commit(batch).unwrap();
        }

        assert_eq!(m.split_slot_gauge(), 0);
        m.on_finalized_checkpoint(1, root_n(32), root_n(0x01))
            .await
            .unwrap();
        let v1 = m.split_slot_gauge();
        assert_eq!(v1, 32);
        m.on_finalized_checkpoint(2, root_n(64), root_n(0x02))
            .await
            .unwrap();
        let v2 = m.split_slot_gauge();
        assert_eq!(v2, 64);
        assert!(v2 > v1);
        let _ = shutdown.send(true);
    }

    /// Migration commits only through writer P1 (plan has puts/deletes; no direct eng commit path in Migrator).
    #[test]
    fn migrator_source_uses_writer_p1_not_engine_commit() {
        let src = include_str!("migrate.rs");
        // Production path names.
        assert!(
            src.contains("submit_p1_committed") || src.contains("blocking_submit_p1_committed")
        );
        assert!(src.contains("plan_migration"));
        // Migrator must not call Engine::commit for the migration batch.
        // (engine.commit may appear in tests for seeding only.)
        let prod = src.split("mod tests").next().unwrap();
        assert!(
            !prod.contains("engine.commit") && !prod.contains(".commit(batch"),
            "production migrate.rs must submit via writer, not Engine::commit"
        );
        assert!(src.contains("read_recursive"));
        assert!(
            src.contains("MAX_MIGRATION_SLOTS_PER_BATCH") || src.contains("migration_window_end")
        );
    }

    #[test]
    fn parse_finalized_payload_roundtrip() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7u64.to_le_bytes());
        payload.extend_from_slice(&[0xAB; 32]);
        payload.extend_from_slice(&[0; 8]);
        let (e, sr) = parse_finalized_payload(&payload).unwrap();
        assert_eq!(e, 7);
        assert_eq!(sr, Root::from_array([0xAB; 32]));
    }

    #[test]
    fn no_unfinalized_rs_in_prune_dir() {
        let prune = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/prune");
        if prune.is_dir() {
            assert!(!prune.join("unfinalized.rs").exists());
        }
        let src = include_str!("migrate.rs");
        assert!(src.contains("unfinalized"));
        assert!(src.contains("read_recursive"));
        assert!(src.contains("epochs_per_migration"));
    }

    #[test]
    fn epoch_helpers_match_store() {
        assert_eq!(epoch_of_slot(Slot::new(32)), 1);
        assert_eq!(epoch_start_slot(2).as_u64(), 64);
    }
}
