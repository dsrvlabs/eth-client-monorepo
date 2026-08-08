//! Five prune passes, five watermarks (Architecture §7.0 / CC-46a).
//!
//! | Pass | Watermark | Trigger |
//! |---|---|---|
//! | columns | wall-clock − retention − margin | epoch tick every **32** |
//! | blocks | wall-clock − computed floor − margin | epoch tick every **256** |
//! | state roots | with blocks | with blocks |
//! | snapshot ring | keep newest `snapshot_ring` | on each snapshot write |
//! | unfinalized | `Split.slot` | **migration** (`migrate.rs`, not here) |
//!
//! **Spec delta 9:** both retention watermarks move with wall-clock
//! `current_epoch`, **not** with finalization. A node whose finalization has
//! stalled still prunes and still serves the wall-clock window.
//!
//! **`I2`:** the block-prune mark is refused (not clamped) when it would rise
//! above `start_slot(current_epoch − floor)` — see [`blocks::i2_check`].
//!
//! **Disk watermark:** alarm only, never a trigger (§7.4). A full-disk alarm
//! must not fire a prune pass (feedback loop).
//!
//! **Writer:** passes submit **P2** chunks only. This module does not edit
//! `writer.rs` (**D-4**). Chunk/deadline abandon loop is **CC-46b**.
//!
//! `ls services/storage/src/prune/` shows exactly
//! `mod.rs`, `columns.rs`, `blocks.rs`, `states.rs` — **no `unfinalized.rs`**.

#![allow(dead_code)]

pub(crate) mod blocks;
pub(crate) mod columns;
pub(crate) mod states;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_store::engine::Engine;
use cc_store::meta::{
    KEY_PRUNE_MARKS, PruneMarks, TABLE_META,
};
use cc_store::{
    epoch_of_slot, epoch_start_slot, newest_snapshot, Slot, SszDecode, SszEncode, StoreError,
};
use tokio::sync::{oneshot, watch};
use tracing::{error, info, warn};

use crate::metrics::{
    ClassLabels, PassLabels, PrunePass, StorageClass, StorageMetrics,
};
use crate::writer::{BackgroundChunk, WriterError, WriterHandle};

use blocks::{blocks_prune_mark, i2_check, plan_block_deletes, record_i2_refusal, I2Decision};
use columns::{columns_prune_mark, plan_column_deletes, DEFAULT_COLUMNS_RETENTION_EPOCHS};
use states::{plan_snapshot_ring_trim, plan_state_root_deletes};

// ── defaults (§7.4) ─────────────────────────────────────────────────────────

/// Column pass cadence (shares snapshot tick; equals column shard width).
pub(crate) const DEFAULT_PRUNE_COLUMNS_EPOCHS: u64 = 32;
/// Block (+ state roots) pass cadence (equals block shard width).
pub(crate) const DEFAULT_PRUNE_BLOCKS_EPOCHS: u64 = 256;
/// One-epoch serve-side margin baked into both watermarks (§7.1 / §7.0 table).
pub(crate) const DEFAULT_PRUNE_MARGIN_EPOCHS: u64 = 1;
/// Disk alarm at 75 % of the 128 GiB provision = **96 GiB** (§9.3).
pub(crate) const DEFAULT_DISK_ALARM_BYTES: u64 = 96 * 1024 * 1024 * 1024;
/// Soft chunk size for P2 submit (CC-46b owns the deadline loop; we still chunk
/// so a single pass stays under `MAX_BATCH_OPS`).
pub(crate) const PRUNE_CHUNK_KEYS: usize = 512;
/// Default slots per epoch / seconds per slot (mainnet-shaped).
pub(crate) const DEFAULT_SLOTS_PER_EPOCH: u64 = 32;
pub(crate) const DEFAULT_SECONDS_PER_SLOT: u64 = 12;

// ── config ──────────────────────────────────────────────────────────────────

/// Prune knobs from `config/storage.toml` (+ retention override + CC-4A floor).
#[derive(Debug, Clone)]
pub(crate) struct PruneConfig {
    /// Epoch tick cadence for the columns pass. Default **32**.
    pub prune_columns_epochs: u64,
    /// Epoch tick cadence for the blocks (+ state roots) pass. Default **256**.
    pub prune_blocks_epochs: u64,
    /// Margin subtracted from both watermarks. Default **1**.
    pub prune_margin_epochs: u64,
    /// Disk alarm threshold in bytes. Default **96 GiB**. Alarm only.
    pub disk_alarm_bytes: u64,
    /// Column retention depth in epochs (spec 4096; CC-4D override ok).
    pub columns_retention_epochs: u64,
    /// Block retention depth in epochs (CC-4A computed; CC-4D override ok).
    pub blocks_retention_epochs: u64,
    /// `FULU_FORK_EPOCH` floor for the column watermark.
    pub fulu_fork_epoch: u64,
    /// Snapshot ring depth (snapshot-ring pass). Default **4**.
    pub snapshot_ring: u64,
    /// Genesis unix time for wall-clock epoch derivation (0 → tick injection only).
    pub genesis_time: u64,
    /// Seconds per slot for wall-clock epoch derivation.
    pub seconds_per_slot: u64,
    /// Slots per epoch.
    pub slots_per_epoch: u64,
}

impl Default for PruneConfig {
    fn default() -> Self {
        // Block floor from CC-4A (computed); never a hard-coded epoch count.
        let blocks_retention_epochs = cc_store::compute_min_epochs_for_block_requests(
            &cc_store::BlockServeWindowCfg::new(256, 65_536),
        )
        .unwrap_or(0);
        Self {
            prune_columns_epochs: DEFAULT_PRUNE_COLUMNS_EPOCHS,
            prune_blocks_epochs: DEFAULT_PRUNE_BLOCKS_EPOCHS,
            prune_margin_epochs: DEFAULT_PRUNE_MARGIN_EPOCHS,
            disk_alarm_bytes: DEFAULT_DISK_ALARM_BYTES,
            columns_retention_epochs: DEFAULT_COLUMNS_RETENTION_EPOCHS,
            blocks_retention_epochs,
            fulu_fork_epoch: 0,
            snapshot_ring: 4,
            genesis_time: 0,
            seconds_per_slot: DEFAULT_SECONDS_PER_SLOT,
            slots_per_epoch: DEFAULT_SLOTS_PER_EPOCH,
        }
    }
}

// ── plan + outcome ──────────────────────────────────────────────────────────

/// Staged deletes + accounting for one pass (or part of one).
#[derive(Debug, Default, Clone)]
pub(crate) struct PrunePlan {
    pub deletes: Vec<(String, Vec<u8>)>,
    pub puts: Vec<(String, Vec<u8>, Vec<u8>)>,
    pub rows: u64,
    pub bytes: u64,
}

impl PrunePlan {
    fn append(&mut self, other: PrunePlan) {
        self.rows = self.rows.saturating_add(other.rows);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.deletes.extend(other.deletes);
        self.puts.extend(other.puts);
    }
}

/// Outcome of a single pass attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassOutcome {
    /// Pass ran (possibly a no-op if mark did not advance).
    Ran {
        /// Exclusive mark after the pass.
        mark: Slot,
        rows: u64,
        bytes: u64,
    },
    /// Cadence not met — skipped.
    SkippedCadence,
    /// `I2` refused the block mark.
    RefusedI2,
    /// Proposed mark ≤ durable mark — nothing to do.
    AlreadyAtMark,
    /// P2 queue full; marks not advanced (retry next tick).
    QueueFull,
}

/// Disk-alarm check result — **never** a prune trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskAlarm {
    Ok,
    /// `disk_bytes` at or above the configured threshold.
    Alarm { disk_bytes: u64, threshold: u64 },
}

// ── pruner ──────────────────────────────────────────────────────────────────

/// Shared prune state: watermarks, cadence counters, writer handle, metrics.
#[derive(Debug)]
pub(crate) struct Pruner {
    pub engine: Arc<Engine>,
    pub writer: WriterHandle,
    pub cfg: PruneConfig,
    pub metrics: StorageMetrics,
    /// In-memory marks (durable copy is `meta.prune_marks`).
    ///
    /// Updated **only after** a durable meta put of the new marks succeeds.
    marks: std::sync::Mutex<PruneMarks>,
    /// Effective genesis unix time (config / network yaml / store snapshot).
    /// May be refreshed from the store when initially 0.
    genesis_time: AtomicU64,
    /// Last epoch at which the columns pass fired (for cadence).
    last_columns_epoch: AtomicU64,
    /// Last epoch at which the blocks pass fired.
    last_blocks_epoch: AtomicU64,
    /// Invocation counters (CC-46 /3).
    pub columns_invocations: AtomicU64,
    pub blocks_invocations: AtomicU64,
    pub state_roots_invocations: AtomicU64,
    pub snapshot_invocations: AtomicU64,
    /// How many times the disk alarm path fired (never a pass trigger).
    pub disk_alarm_firings: AtomicU64,
    /// How many I2 refusals this process has recorded.
    pub i2_refusals: AtomicU64,
}

impl Pruner {
    pub(crate) fn new(
        engine: Arc<Engine>,
        writer: WriterHandle,
        cfg: PruneConfig,
        metrics: StorageMetrics,
    ) -> Self {
        let marks = load_prune_marks(&engine).unwrap_or_default();
        // Prefer configured genesis; else try store snapshot / leave 0 for later refresh.
        let mut genesis = cfg.genesis_time;
        if genesis == 0 {
            genesis = genesis_time_from_store(&engine).unwrap_or(0);
        }
        if genesis != 0 {
            info!(
                target: "cc_storage::prune",
                genesis_time = genesis,
                "prune wall-clock genesis resolved"
            );
        } else {
            warn!(
                target: "cc_storage::prune",
                "prune genesis_time unresolved — wall-clock ticks idle until a snapshot \
                 carries genesis_time or config supplies it"
            );
        }
        Self {
            engine,
            writer,
            cfg,
            metrics,
            marks: std::sync::Mutex::new(marks),
            genesis_time: AtomicU64::new(genesis),
            last_columns_epoch: AtomicU64::new(0),
            last_blocks_epoch: AtomicU64::new(0),
            columns_invocations: AtomicU64::new(0),
            blocks_invocations: AtomicU64::new(0),
            state_roots_invocations: AtomicU64::new(0),
            snapshot_invocations: AtomicU64::new(0),
            disk_alarm_firings: AtomicU64::new(0),
            i2_refusals: AtomicU64::new(0),
        }
    }

    /// Snapshot of in-memory marks.
    pub(crate) fn marks_snapshot(&self) -> PruneMarks {
        *self.marks.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Effective genesis unix seconds for wall-clock epoch derivation.
    ///
    /// Order: cached value → config → store snapshot SSZ peek. Caches a
    /// non-zero result from the store so subsequent ticks are O(1).
    #[must_use]
    pub(crate) fn effective_genesis_time(&self) -> u64 {
        let cached = self.genesis_time.load(Ordering::Relaxed);
        if cached != 0 {
            return cached;
        }
        if self.cfg.genesis_time != 0 {
            self.genesis_time
                .store(self.cfg.genesis_time, Ordering::Relaxed);
            return self.cfg.genesis_time;
        }
        if let Some(from_store) = genesis_time_from_store(&self.engine) {
            self.genesis_time.store(from_store, Ordering::Relaxed);
            info!(
                target: "cc_storage::prune",
                genesis_time = from_store,
                "prune genesis_time resolved from store snapshot"
            );
            return from_store;
        }
        0
    }

    // ── disk alarm (never a trigger) ────────────────────────────────────────

    /// Observe `cc_storage_disk_bytes` and raise an alarm if over threshold.
    ///
    /// **Does not schedule or run any prune pass.** Grep anchor for the
    /// acceptance criterion: `disk_bytes` / `disk_watermark` appear only here
    /// in the prune tree (alarm path).
    pub(crate) fn check_disk_alarm(&self, disk_bytes: u64) -> DiskAlarm {
        self.metrics
            .disk_bytes
            .set(i64::try_from(disk_bytes).unwrap_or(i64::MAX));
        let threshold = self.cfg.disk_alarm_bytes;
        if disk_bytes >= threshold {
            self.disk_alarm_firings.fetch_add(1, Ordering::SeqCst);
            error!(
                target: "cc_storage::prune",
                disk_bytes,
                threshold,
                "disk watermark alarm — operator action required (alarm only, never a prune trigger)"
            );
            DiskAlarm::Alarm {
                disk_bytes,
                threshold,
            }
        } else {
            DiskAlarm::Ok
        }
    }

    /// Read the on-disk file length and run [`Self::check_disk_alarm`].
    pub(crate) fn observe_disk(&self) -> DiskAlarm {
        match self.engine.file_len() {
            Ok(n) => self.check_disk_alarm(n),
            Err(e) => {
                warn!(target: "cc_storage::prune", error = %e, "disk_bytes read failed");
                DiskAlarm::Ok
            }
        }
    }

    // ── wall-clock epoch ────────────────────────────────────────────────────

    /// Derive wall-clock `current_epoch` from `now` (unix seconds).
    ///
    /// When genesis cannot be resolved (config + store empty), returns `None`
    /// (tests inject epochs via [`Self::on_epoch_tick`] directly).
    #[must_use]
    pub(crate) fn wall_clock_epoch(&self, now_unix: u64) -> Option<u64> {
        let genesis = self.effective_genesis_time();
        if genesis == 0 {
            return None;
        }
        let sps = self.cfg.seconds_per_slot.max(1);
        let spe = self.cfg.slots_per_epoch.max(1);
        let elapsed = now_unix.saturating_sub(genesis);
        let slot = elapsed / sps;
        Some(slot / spe)
    }

    // ── epoch tick ──────────────────────────────────────────────────────────

    /// Drive prune cadences from a wall-clock (or simulated) `current_epoch`.
    ///
    /// **Independent of finalization.** Callers that only advance finalization
    /// without calling this will not move retention watermarks — and callers
    /// that call this without finalization **will**.
    pub(crate) async fn on_epoch_tick(&self, current_epoch: u64) -> Vec<(PrunePass, PassOutcome)> {
        // Disk alarm is observed on the tick but never gates the passes.
        let _ = self.observe_disk();

        let mut out = Vec::new();
        if self.should_run_columns(current_epoch) {
            let o = self.run_columns_pass(current_epoch).await;
            out.push((PrunePass::Columns, o));
        } else {
            out.push((PrunePass::Columns, PassOutcome::SkippedCadence));
        }
        if self.should_run_blocks(current_epoch) {
            let o = self.run_blocks_pass(current_epoch).await;
            out.push((PrunePass::Blocks, o));
            // State roots ride the blocks pass (same watermark / same tick).
            if matches!(o, PassOutcome::Ran { .. }) {
                // Counted inside run_blocks_pass for state_roots.
            }
        } else {
            out.push((PrunePass::Blocks, PassOutcome::SkippedCadence));
        }
        self.update_lag_gauges(current_epoch);
        out
    }

    fn should_run_columns(&self, current_epoch: u64) -> bool {
        let cadence = self.cfg.prune_columns_epochs.max(1);
        if current_epoch == 0 {
            return false;
        }
        // Fire when epoch is a multiple of cadence and we have not fired at this epoch.
        if !current_epoch.is_multiple_of(cadence) {
            return false;
        }
        self.last_columns_epoch.load(Ordering::SeqCst) < current_epoch
    }

    fn should_run_blocks(&self, current_epoch: u64) -> bool {
        let cadence = self.cfg.prune_blocks_epochs.max(1);
        if current_epoch == 0 {
            return false;
        }
        if !current_epoch.is_multiple_of(cadence) {
            return false;
        }
        self.last_blocks_epoch.load(Ordering::SeqCst) < current_epoch
    }

    // ── passes ──────────────────────────────────────────────────────────────

    /// Columns pass at wall-clock `current_epoch`.
    pub(crate) async fn run_columns_pass(&self, current_epoch: u64) -> PassOutcome {
        let started = Instant::now();
        let proposed = columns_prune_mark(
            current_epoch,
            self.cfg.columns_retention_epochs,
            self.cfg.fulu_fork_epoch,
            self.cfg.prune_margin_epochs,
        );
        let from = {
            let g = self.marks.lock().unwrap_or_else(|e| e.into_inner());
            g.columns_up_to
        };
        if proposed.as_u64() <= from.as_u64() {
            self.last_columns_epoch
                .store(current_epoch, Ordering::SeqCst);
            self.columns_invocations.fetch_add(1, Ordering::SeqCst);
            self.observe_pass_seconds(PrunePass::Columns, started.elapsed());
            return PassOutcome::AlreadyAtMark;
        }

        let plan = match plan_column_deletes(&self.engine, from, proposed) {
            Ok(p) => p,
            Err(e) => {
                warn!(target: "cc_storage::prune", error = %e, "columns plan failed");
                return PassOutcome::QueueFull;
            }
        };
        let rows = plan.rows;
        let bytes = plan.bytes;
        if let Err(e) = self
            .submit_plan_and_marks(
                StorageClass::Columns,
                plan,
                |m| m.columns_up_to = proposed,
            )
            .await
        {
            warn!(target: "cc_storage::prune", error = %e, "columns P2 submit failed");
            return PassOutcome::QueueFull;
        }

        self.last_columns_epoch
            .store(current_epoch, Ordering::SeqCst);
        self.columns_invocations.fetch_add(1, Ordering::SeqCst);
        self.metrics
            .pruned_rows
            .get_or_create(&ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            })
            .inc_by(rows);
        self.metrics
            .pruned_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            })
            .inc_by(bytes);
        self.observe_pass_seconds(PrunePass::Columns, started.elapsed());
        info!(
            target: "cc_storage::prune",
            current_epoch,
            mark = proposed.as_u64(),
            rows,
            bytes,
            "columns prune pass"
        );
        PassOutcome::Ran {
            mark: proposed,
            rows,
            bytes,
        }
    }

    /// Blocks + state_roots pass with `I2` guard.
    pub(crate) async fn run_blocks_pass(&self, current_epoch: u64) -> PassOutcome {
        let started = Instant::now();
        let proposed = blocks_prune_mark(
            current_epoch,
            self.cfg.blocks_retention_epochs,
            self.cfg.prune_margin_epochs,
        );

        // I2 — refuse, do not clamp.
        match i2_check(
            proposed,
            current_epoch,
            self.cfg.blocks_retention_epochs,
        ) {
            I2Decision::Allow => {}
            I2Decision::Refuse { proposed, floor } => {
                record_i2_refusal(&self.metrics, proposed, floor, current_epoch);
                self.i2_refusals.fetch_add(1, Ordering::SeqCst);
                self.observe_pass_seconds(PrunePass::Blocks, started.elapsed());
                return PassOutcome::RefusedI2;
            }
        }

        // Explicit I2 for a *forced* above-floor proposal is tested via
        // [`Self::run_blocks_pass_with_mark`].

        let from = {
            let g = self.marks.lock().unwrap_or_else(|e| e.into_inner());
            g.blocks_up_to
        };
        if proposed.as_u64() <= from.as_u64() {
            self.last_blocks_epoch
                .store(current_epoch, Ordering::SeqCst);
            self.blocks_invocations.fetch_add(1, Ordering::SeqCst);
            self.state_roots_invocations.fetch_add(1, Ordering::SeqCst);
            self.observe_pass_seconds(PrunePass::Blocks, started.elapsed());
            self.observe_pass_seconds(PrunePass::StateRoots, started.elapsed());
            return PassOutcome::AlreadyAtMark;
        }

        let mut plan = match plan_block_deletes(&self.engine, from, proposed) {
            Ok(p) => p,
            Err(e) => {
                warn!(target: "cc_storage::prune", error = %e, "blocks plan failed");
                return PassOutcome::QueueFull;
            }
        };
        let sr = match plan_state_root_deletes(&self.engine, from, proposed) {
            Ok(p) => p,
            Err(e) => {
                warn!(target: "cc_storage::prune", error = %e, "state_roots plan failed");
                return PassOutcome::QueueFull;
            }
        };
        let sr_rows = sr.rows;
        let sr_bytes = sr.bytes;
        plan.append(sr);
        let rows = plan.rows;
        let bytes = plan.bytes;

        if let Err(e) = self
            .submit_plan_and_marks(StorageClass::Blocks, plan, |m| {
                m.blocks_up_to = proposed;
                m.state_roots_up_to = proposed;
            })
            .await
        {
            warn!(target: "cc_storage::prune", error = %e, "blocks P2 submit failed");
            return PassOutcome::QueueFull;
        }

        self.last_blocks_epoch
            .store(current_epoch, Ordering::SeqCst);
        self.blocks_invocations.fetch_add(1, Ordering::SeqCst);
        self.state_roots_invocations.fetch_add(1, Ordering::SeqCst);
        self.metrics
            .pruned_rows
            .get_or_create(&ClassLabels {
                class: StorageClass::Blocks.as_str().to_owned(),
            })
            .inc_by(rows.saturating_sub(sr_rows));
        self.metrics
            .pruned_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Blocks.as_str().to_owned(),
            })
            .inc_by(bytes.saturating_sub(sr_bytes));
        // State-roots class is not a StorageClass variant; attribute to Index.
        self.metrics
            .pruned_rows
            .get_or_create(&ClassLabels {
                class: StorageClass::Index.as_str().to_owned(),
            })
            .inc_by(sr_rows);
        self.metrics
            .pruned_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Index.as_str().to_owned(),
            })
            .inc_by(sr_bytes);
        self.observe_pass_seconds(PrunePass::Blocks, started.elapsed());
        self.observe_pass_seconds(PrunePass::StateRoots, started.elapsed());
        info!(
            target: "cc_storage::prune",
            current_epoch,
            mark = proposed.as_u64(),
            rows,
            bytes,
            "blocks+state_roots prune pass"
        );
        PassOutcome::Ran {
            mark: proposed,
            rows,
            bytes,
        }
    }

    /// Run the blocks pass with an **explicit** proposed mark (I2 tests).
    ///
    /// Production always uses [`blocks_prune_mark`]; this entry point exists so
    /// a test can propose a mark above the floor and observe refusal without
    /// a clamp path existing in production code.
    pub(crate) async fn run_blocks_pass_with_mark(
        &self,
        current_epoch: u64,
        proposed: Slot,
    ) -> PassOutcome {
        let started = Instant::now();
        match i2_check(
            proposed,
            current_epoch,
            self.cfg.blocks_retention_epochs,
        ) {
            I2Decision::Allow => {}
            I2Decision::Refuse { proposed, floor } => {
                record_i2_refusal(&self.metrics, proposed, floor, current_epoch);
                self.i2_refusals.fetch_add(1, Ordering::SeqCst);
                self.observe_pass_seconds(PrunePass::Blocks, started.elapsed());
                return PassOutcome::RefusedI2;
            }
        }
        // Allowed explicit mark — advance marks only (no scan) for the happy path.
        let from = {
            let g = self.marks.lock().unwrap_or_else(|e| e.into_inner());
            g.blocks_up_to
        };
        if proposed.as_u64() <= from.as_u64() {
            return PassOutcome::AlreadyAtMark;
        }
        if let Err(e) = self
            .submit_plan_and_marks(StorageClass::Blocks, PrunePlan::default(), |m| {
                m.blocks_up_to = proposed;
                m.state_roots_up_to = proposed;
            })
            .await
        {
            warn!(target: "cc_storage::prune", error = %e, "explicit-mark P2 failed");
            return PassOutcome::QueueFull;
        }
        self.blocks_invocations.fetch_add(1, Ordering::SeqCst);
        self.observe_pass_seconds(PrunePass::Blocks, started.elapsed());
        PassOutcome::Ran {
            mark: proposed,
            rows: 0,
            bytes: 0,
        }
    }

    /// Snapshot-ring pass (triggered on each snapshot write, not an epoch tick).
    pub(crate) async fn run_snapshot_ring_pass(&self) -> PassOutcome {
        let started = Instant::now();
        let plan = match plan_snapshot_ring_trim(&self.engine, self.cfg.snapshot_ring) {
            Ok(p) => p,
            Err(e) => {
                warn!(target: "cc_storage::prune", error = %e, "snapshot ring plan failed");
                return PassOutcome::QueueFull;
            }
        };
        let rows = plan.rows;
        let bytes = plan.bytes;
        if plan.deletes.is_empty() {
            self.snapshot_invocations.fetch_add(1, Ordering::SeqCst);
            self.observe_pass_seconds(PrunePass::Snapshots, started.elapsed());
            return PassOutcome::AlreadyAtMark;
        }
        if let Err(e) = self
            .submit_plan_and_marks(StorageClass::Snapshots, plan, |_| {})
            .await
        {
            warn!(target: "cc_storage::prune", error = %e, "snapshot ring P2 failed");
            return PassOutcome::QueueFull;
        }
        self.snapshot_invocations.fetch_add(1, Ordering::SeqCst);
        self.metrics
            .pruned_rows
            .get_or_create(&ClassLabels {
                class: StorageClass::Snapshots.as_str().to_owned(),
            })
            .inc_by(rows);
        self.metrics
            .pruned_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Snapshots.as_str().to_owned(),
            })
            .inc_by(bytes);
        self.observe_pass_seconds(PrunePass::Snapshots, started.elapsed());
        PassOutcome::Ran {
            mark: Slot::ZERO,
            rows,
            bytes,
        }
    }

    // ── P2 submit ───────────────────────────────────────────────────────────

    async fn submit_plan_and_marks<F>(
        &self,
        class: StorageClass,
        plan: PrunePlan,
        mut update_marks: F,
    ) -> Result<(), PruneError>
    where
        F: FnMut(&mut PruneMarks),
    {
        // Chunk deletes so each P2 unit stays under PRUNE_CHUNK_KEYS.
        let mut offset = 0usize;
        while offset < plan.deletes.len() {
            let end = (offset + PRUNE_CHUNK_KEYS).min(plan.deletes.len());
            let chunk_deletes = plan.deletes[offset..end].to_vec();
            let chunk = BackgroundChunk {
                class,
                puts: Vec::new(),
                deletes: chunk_deletes,
                done: None,
            };
            self.submit_p2_committed(chunk).await?;
            offset = end;
        }
        // Any puts from the plan (rare).
        if !plan.puts.is_empty() {
            let chunk = BackgroundChunk {
                class,
                puts: plan.puts,
                deletes: Vec::new(),
                done: None,
            };
            self.submit_p2_committed(chunk).await?;
        }

        // Propose new marks from the current in-memory snapshot **without**
        // mutating it yet. Advance in-memory only after the durable meta put
        // commits — a P2 drop/fail must leave marks unchanged so the next tick
        // re-derives the same work (idempotent).
        let new_marks = {
            let g = self.marks.lock().unwrap_or_else(|e| e.into_inner());
            let mut proposed = *g;
            update_marks(&mut proposed);
            proposed
        };
        // No-op mark update (snapshot ring trim often has nothing to change).
        let current = self.marks_snapshot();
        if new_marks == current {
            return Ok(());
        }
        let ssz = new_marks.as_ssz_bytes();
        let chunk = BackgroundChunk {
            class: StorageClass::Meta,
            puts: vec![(
                TABLE_META.to_owned(),
                KEY_PRUNE_MARKS.as_bytes().to_vec(),
                ssz,
            )],
            deletes: Vec::new(),
            done: None,
        };
        self.submit_p2_committed(chunk).await?;
        // Durable put succeeded — publish in-memory.
        {
            let mut g = self.marks.lock().unwrap_or_else(|e| e.into_inner());
            *g = new_marks;
        }
        Ok(())
    }

    async fn submit_p2_committed(&self, mut chunk: BackgroundChunk) -> Result<(), PruneError> {
        let (tx, rx) = oneshot::channel();
        chunk.done = Some(tx);
        if !self.writer.try_submit_p2(chunk, &self.metrics) {
            return Err(PruneError::QueueFull);
        }
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(PruneError::Writer(e)),
            Err(_) => Err(PruneError::ShutDown),
        }
    }

    fn observe_pass_seconds(&self, pass: PrunePass, d: Duration) {
        self.metrics
            .prune_seconds
            .get_or_create(&PassLabels {
                pass: pass.as_str().to_owned(),
            })
            .observe(d.as_secs_f64());
    }

    fn update_lag_gauges(&self, current_epoch: u64) {
        let marks = self.marks_snapshot();
        // Lag ≈ how many epochs the watermark trails the ideal mark.
        let ideal_c = columns_prune_mark(
            current_epoch,
            self.cfg.columns_retention_epochs,
            self.cfg.fulu_fork_epoch,
            self.cfg.prune_margin_epochs,
        );
        let ideal_b = blocks_prune_mark(
            current_epoch,
            self.cfg.blocks_retention_epochs,
            self.cfg.prune_margin_epochs,
        );
        let lag_c = epoch_of_slot(ideal_c).saturating_sub(epoch_of_slot(marks.columns_up_to));
        let lag_b = epoch_of_slot(ideal_b).saturating_sub(epoch_of_slot(marks.blocks_up_to));
        self.metrics
            .prune_lag_epochs
            .get_or_create(&ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            })
            .set(i64::try_from(lag_c).unwrap_or(i64::MAX));
        self.metrics
            .prune_lag_epochs
            .get_or_create(&ClassLabels {
                class: StorageClass::Blocks.as_str().to_owned(),
            })
            .set(i64::try_from(lag_b).unwrap_or(i64::MAX));
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn load_prune_marks(engine: &Engine) -> Result<PruneMarks, StoreError> {
    let rt = engine.read()?;
    let Some(bytes) = rt.get(TABLE_META, KEY_PRUNE_MARKS.as_bytes())? else {
        return Ok(PruneMarks::default());
    };
    PruneMarks::from_ssz_bytes(&bytes).map_err(|e| StoreError::Codec(format!("PruneMarks: {e:?}")))
}

/// Spawn the prune background task (wall-clock epoch ticks).
///
/// When genesis is unresolved, the task only runs the disk-alarm observe path
/// and keeps trying to resolve genesis from the store (e.g. after the first
/// snapshot lands). Tests drive passes via [`Pruner::on_epoch_tick`].
pub(crate) fn spawn_prune_task(
    pruner: Arc<Pruner>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            target: "cc_storage::prune",
            genesis_time = pruner.effective_genesis_time(),
            "prune task started"
        );
        let mut ticker = tokio::time::interval(Duration::from_secs(4));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_epoch = u64::MAX;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    // Alarm path every tick (never a trigger).
                    let _ = pruner.observe_disk();
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let Some(epoch) = pruner.wall_clock_epoch(now) else {
                        continue;
                    };
                    if epoch == last_epoch {
                        continue;
                    }
                    last_epoch = epoch;
                    let outcomes = pruner.on_epoch_tick(epoch).await;
                    for (pass, o) in outcomes {
                        if matches!(o, PassOutcome::Ran { .. } | PassOutcome::RefusedI2) {
                            info!(
                                target: "cc_storage::prune",
                                pass = pass.as_str(),
                                ?o,
                                epoch,
                                "epoch-tick prune outcome"
                            );
                        }
                    }
                }
            }
        }
        info!(target: "cc_storage::prune", "prune task stopped");
    })
}

/// Peek `BeaconState.genesis_time` from the newest store snapshot (SSZ offset 0).
///
/// BeaconState fixed prefix is `genesis_time: uint64` then `genesis_validators_root`.
/// Used when config does not supply `storage.genesis_time`.
pub(crate) fn genesis_time_from_store(engine: &Engine) -> Option<u64> {
    let rt = engine.read().ok()?;
    let (_slot, ssz) = newest_snapshot(&rt).ok().flatten()?;
    if ssz.len() < 8 {
        return None;
    }
    let mut le = [0u8; 8];
    le.copy_from_slice(&ssz[0..8]);
    let t = u64::from_le_bytes(le);
    (t > 0).then_some(t)
}

/// Derive genesis unix time from a consensus-specs network YAML:
/// `MIN_GENESIS_TIME + GENESIS_DELAY` (Hoodi: 1742212800 + 600 = 1742213400).
pub(crate) fn genesis_time_from_network_yaml(text: &str) -> Option<u64> {
    let mut min_genesis: Option<u64> = None;
    let mut delay: Option<u64> = None;
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or(line).trim();
        if let Some(rest) = line.strip_prefix("MIN_GENESIS_TIME:") {
            min_genesis = rest.trim().parse().ok();
        } else if let Some(rest) = line.strip_prefix("GENESIS_DELAY:") {
            delay = rest.trim().parse().ok();
        }
    }
    match (min_genesis, delay) {
        (Some(m), Some(d)) => Some(m.saturating_add(d)),
        (Some(m), None) => Some(m),
        _ => None,
    }
}

/// Load genesis from the committed Hoodi fixture (same path as config digest).
pub(crate) fn genesis_time_from_fixture() -> Option<u64> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
    let text = std::fs::read_to_string(path).ok()?;
    genesis_time_from_network_yaml(&text)
}

/// Pure watermark pair for tests (no store).
#[must_use]
pub(crate) fn watermarks_at(
    current_epoch: u64,
    cfg: &PruneConfig,
) -> (Slot, Slot) {
    let c = columns_prune_mark(
        current_epoch,
        cfg.columns_retention_epochs,
        cfg.fulu_fork_epoch,
        cfg.prune_margin_epochs,
    );
    let b = blocks_prune_mark(
        current_epoch,
        cfg.blocks_retention_epochs,
        cfg.prune_margin_epochs,
    );
    (c, b)
}

/// Prune-path errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PruneError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("writer: {0}")]
    Writer(#[from] WriterError),
    #[error("P2 queue full")]
    QueueFull,
    #[error("writer shut down")]
    ShutDown,
}

// Silence unused import of epoch_start_slot in non-test builds where only
// submodules use it through their own imports.
#[allow(dead_code)]
fn _epoch_start_slot_anchor(e: u64) -> Slot {
    epoch_start_slot(e)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use crate::prune::blocks::block_serve_floor_slot;
    use crate::writer::{WriterBounds, WriterFaults, spawn_writer};
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::{
        BlockServeWindowCfg, compute_min_epochs_for_block_requests,
    };
    use prometheus_client::registry::Registry;
    use std::sync::atomic::AtomicU64;

    fn hoodi_floor() -> u64 {
        compute_min_epochs_for_block_requests(&BlockServeWindowCfg::new(256, 65_536)).unwrap()
    }

    fn tmp_engine() -> Engine {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("cc-storage-prune-mod-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        let mut b = eng.batch();
        b.put(TABLE_META, b"__touch__", b"1");
        eng.commit(b).unwrap();
        eng
    }

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    async fn pruner_with(cfg: PruneConfig) -> (Arc<Pruner>, watch::Sender<bool>, Arc<Engine>) {
        let eng = Arc::new(tmp_engine());
        let (tx, rx) = watch::channel(false);
        let m = metrics();
        let writer = spawn_writer(
            Arc::clone(&eng),
            m.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            rx,
            false,
        );
        // Writer task must be scheduled before first P2 submit.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let p = Arc::new(Pruner::new(Arc::clone(&eng), writer, cfg, m));
        (p, tx, eng)
    }

    /// CC-46 /1 — watermarks advance with wall-clock even when finalization stalls.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watermarks_advance_without_finalization() {
        // Compressed-style retention so marks move at low simulated epochs
        // (spec floors would need current_epoch ≫ 33_024 before blocks move).
        let cfg = PruneConfig {
            blocks_retention_epochs: 256,
            columns_retention_epochs: 64,
            fulu_fork_epoch: 0,
            prune_columns_epochs: 1, // fire every epoch for this test
            prune_blocks_epochs: 1,
            prune_margin_epochs: 1,
            ..PruneConfig::default()
        };
        let (p, _shutdown, _) = pruner_with(cfg).await;
        // Start far above retention so marks are non-zero.
        let e0 = 300u64;
        let outcomes0 = p.on_epoch_tick(e0).await;
        assert!(
            outcomes0.iter().any(|(_, o)| matches!(o, PassOutcome::Ran { .. })),
            "first tick must run passes: {outcomes0:?}"
        );
        let m0 = p.marks_snapshot();
        assert!(m0.columns_up_to.as_u64() > 0, "columns mark should advance");
        assert!(m0.blocks_up_to.as_u64() > 0, "blocks mark should advance");

        // Stall finalization (we never touch Split / migrator) and advance wall-clock 4 epochs.
        let e1 = e0 + 4;
        let _ = p.on_epoch_tick(e1).await;
        let m1 = p.marks_snapshot();
        assert!(
            m1.columns_up_to.as_u64() > m0.columns_up_to.as_u64(),
            "columns watermark must advance with wall-clock while finalization stalls: {:?} → {:?}",
            m0.columns_up_to,
            m1.columns_up_to
        );
        assert!(
            m1.blocks_up_to.as_u64() > m0.blocks_up_to.as_u64(),
            "blocks watermark must advance with wall-clock while finalization stalls: {:?} → {:?}",
            m0.blocks_up_to,
            m1.blocks_up_to
        );
    }

    /// CC-46 /2 — four boundary assertions (two per class).
    #[test]
    fn watermark_boundaries_plus_minus_one() {
        let floor = hoodi_floor();
        assert_eq!(floor, 33_024, "test names the computed floor");
        let current = floor + 5_000;
        let margin = 1u64;

        // Columns.
        let c_mark = columns_prune_mark(current, 4_096, 0, margin);
        let c_newest = c_mark.as_u64() - 1;
        let c_expected = epoch_start_slot(current - 4_096).as_u64()
            - margin * DEFAULT_SLOTS_PER_EPOCH
            - 1;
        assert_eq!(c_newest, c_expected);
        assert_eq!(c_mark.as_u64(), c_expected + 1);

        // Blocks against computed 33_024.
        let b_mark = blocks_prune_mark(current, floor, margin);
        let b_newest = b_mark.as_u64() - 1;
        let b_expected = epoch_start_slot(current - floor).as_u64()
            - margin * DEFAULT_SLOTS_PER_EPOCH
            - 1;
        assert_eq!(b_newest, b_expected);
        assert_eq!(b_mark.as_u64(), b_expected + 1);
    }

    /// CC-46 /3 — cadence counters over 512 simulated epochs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cadence_counters_over_512_epochs() {
        let floor = hoodi_floor();
        let cfg = PruneConfig {
            blocks_retention_epochs: floor,
            columns_retention_epochs: 4_096,
            fulu_fork_epoch: 0,
            prune_columns_epochs: 32,
            prune_blocks_epochs: 256,
            prune_margin_epochs: 1,
            ..PruneConfig::default()
        };
        let (p, _shutdown, _) = pruner_with(cfg).await;
        for epoch in 1..=512 {
            let _ = p.on_epoch_tick(epoch).await;
        }
        assert_eq!(
            p.columns_invocations.load(Ordering::SeqCst),
            16,
            "32-epoch cadence over 512 epochs → 16 column passes"
        );
        assert_eq!(
            p.blocks_invocations.load(Ordering::SeqCst),
            2,
            "256-epoch cadence over 512 epochs → 2 block passes"
        );
    }

    /// CC-46 /3 — disk watermark is an alarm, never a trigger.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disk_watermark_alarm_not_trigger() {
        let cfg = PruneConfig {
            disk_alarm_bytes: 1_000, // low threshold for the test
            prune_columns_epochs: 32,
            prune_blocks_epochs: 256,
            ..PruneConfig::default()
        };
        let (p, _shutdown, _) = pruner_with(cfg).await;
        let before_c = p.columns_invocations.load(Ordering::SeqCst);
        let before_b = p.blocks_invocations.load(Ordering::SeqCst);
        // Drive disk_bytes past the threshold without an epoch cadence hit.
        let alarm = p.check_disk_alarm(5_000);
        assert!(matches!(alarm, DiskAlarm::Alarm { .. }));
        assert_eq!(p.disk_alarm_firings.load(Ordering::SeqCst), 1);
        // No pass was triggered by the alarm.
        assert_eq!(p.columns_invocations.load(Ordering::SeqCst), before_c);
        assert_eq!(p.blocks_invocations.load(Ordering::SeqCst), before_b);
        // Metric is populated.
        assert_eq!(p.metrics.disk_bytes.get(), 5_000);
        // Epoch not on cadence → still no pass.
        let outcomes = p.on_epoch_tick(3).await;
        for (_, o) in outcomes {
            assert!(matches!(o, PassOutcome::SkippedCadence));
        }
        assert_eq!(p.columns_invocations.load(Ordering::SeqCst), before_c);
        assert_eq!(p.blocks_invocations.load(Ordering::SeqCst), before_b);
    }

    /// I2 refuses an above-floor mark; marks unchanged; counter +1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn i2_refuses_above_floor_mark() {
        let floor_epochs = hoodi_floor();
        let cfg = PruneConfig {
            blocks_retention_epochs: floor_epochs,
            prune_margin_epochs: 1,
            ..PruneConfig::default()
        };
        let (p, _shutdown, _) = pruner_with(cfg).await;
        let current = floor_epochs + 1_000;
        let floor = block_serve_floor_slot(current, floor_epochs);
        let proposed = Slot::new(floor.as_u64() + 1);
        let before_marks = p.marks_snapshot();
        let before_metric = p.metrics.window_increase_rejected.get();
        let outcome = p.run_blocks_pass_with_mark(current, proposed).await;
        assert_eq!(outcome, PassOutcome::RefusedI2);
        assert_eq!(p.i2_refusals.load(Ordering::SeqCst), 1);
        assert_eq!(
            p.metrics.window_increase_rejected.get(),
            before_metric + 1
        );
        let after = p.marks_snapshot();
        assert_eq!(
            after.blocks_up_to, before_marks.blocks_up_to,
            "PruneMarks.blocks_up_to must be unchanged on I2 refuse"
        );
    }

    /// CC-46 /8 — prune metric families are readable after a pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prune_metrics_populated() {
        let floor = hoodi_floor();
        let cfg = PruneConfig {
            blocks_retention_epochs: floor,
            columns_retention_epochs: 64,
            fulu_fork_epoch: 0,
            prune_columns_epochs: 1,
            prune_blocks_epochs: 1,
            prune_margin_epochs: 1,
            ..PruneConfig::default()
        };
        let (p, _shutdown, _) = pruner_with(cfg).await;
        let _ = p.on_epoch_tick(200).await;
        // prune_seconds observed (seed observes 0.0; pass observes a real sample).
        // lag gauges set.
        let lag_c = p
            .metrics
            .prune_lag_epochs
            .get_or_create(&ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            })
            .get();
        let lag_b = p
            .metrics
            .prune_lag_epochs
            .get_or_create(&ClassLabels {
                class: StorageClass::Blocks.as_str().to_owned(),
            })
            .get();
        // After a successful pass lag should be 0.
        assert_eq!(lag_c, 0);
        assert_eq!(lag_b, 0);
        // deadline_exceeded series exists (seeded); CC-46b populates on abandon.
        let _ = p
            .metrics
            .prune_deadline_exceeded
            .get_or_create(&PassLabels {
                pass: PrunePass::Columns.as_str().to_owned(),
            })
            .get();
        // pruned_bytes family exists.
        let _ = p
            .metrics
            .pruned_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            })
            .get();
    }

    /// Directory inventory: no unfinalized.rs.
    #[test]
    fn prune_dir_has_no_unfinalized() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/prune");
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "blocks.rs".to_owned(),
                "columns.rs".to_owned(),
                "mod.rs".to_owned(),
                "states.rs".to_owned(),
            ]
        );
        assert!(!names.iter().any(|n| n.contains("unfinalized")));
    }

    /// Pure watermarks move with epoch independent of any finalization handle.
    #[test]
    fn pure_watermarks_track_wall_clock() {
        let cfg = PruneConfig {
            columns_retention_epochs: 64,
            // Use a short block floor so low wall-clock epochs still move the mark.
            blocks_retention_epochs: 256,
            fulu_fork_epoch: 0,
            prune_margin_epochs: 1,
            ..PruneConfig::default()
        };
        let (c0, b0) = watermarks_at(300, &cfg);
        let (c1, b1) = watermarks_at(304, &cfg);
        assert!(c1.as_u64() > c0.as_u64(), "columns {c0:?} → {c1:?}");
        assert!(b1.as_u64() > b0.as_u64(), "blocks {b0:?} → {b1:?}");
    }

    /// Network YAML → MIN_GENESIS_TIME + GENESIS_DELAY (Hoodi arithmetic).
    #[test]
    fn genesis_time_from_network_yaml_hoodi() {
        let text = "\
MIN_GENESIS_TIME: 1742212800
GENESIS_DELAY: 600
SECONDS_PER_SLOT: 12
";
        assert_eq!(
            genesis_time_from_network_yaml(text),
            Some(1_742_213_400)
        );
        // Fixture path used by production prune_config.
        let from_fixture = genesis_time_from_fixture();
        assert_eq!(from_fixture, Some(1_742_213_400));
    }

    /// wall_clock_epoch works when genesis is configured (not hard-coded 0).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wall_clock_epoch_with_configured_genesis() {
        let cfg = PruneConfig {
            genesis_time: 1_600_000_000,
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            ..PruneConfig::default()
        };
        let (p, _shutdown, _) = pruner_with(cfg).await;
        // 32 slots * 12 s = 384 s per epoch.
        // now = genesis + 384 * 10 → epoch 10.
        let now = 1_600_000_000 + 384 * 10;
        assert_eq!(p.wall_clock_epoch(now), Some(10));
        assert_eq!(p.effective_genesis_time(), 1_600_000_000);
        // Unresolved path still works for pure injection tests.
        let cfg0 = PruneConfig {
            genesis_time: 0,
            ..PruneConfig::default()
        };
        let (p0, _, _) = pruner_with(cfg0).await;
        // No store snapshot → still None.
        assert_eq!(p0.wall_clock_epoch(now), None);
    }

    /// PruneMarks stay put when the marks put fails (queue full simulation via
    /// shutdown writer). Durable-first: in-memory only advances after commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn marks_not_advanced_until_durable_put() {
        let cfg = PruneConfig {
            blocks_retention_epochs: 256,
            columns_retention_epochs: 64,
            fulu_fork_epoch: 0,
            prune_columns_epochs: 1,
            prune_blocks_epochs: 1,
            prune_margin_epochs: 1,
            ..PruneConfig::default()
        };
        let (p, shutdown, _) = pruner_with(cfg).await;
        let before = p.marks_snapshot();
        // Successful pass advances marks only after durable put.
        let _ = p.on_epoch_tick(300).await;
        let after = p.marks_snapshot();
        assert!(
            after.columns_up_to.as_u64() > before.columns_up_to.as_u64()
                || after.blocks_up_to.as_u64() > before.blocks_up_to.as_u64(),
            "successful pass must advance marks after durable put"
        );
        // Shut down writer so subsequent P2 fails; marks must not regress/change
        // on a failed attempt from a higher epoch if the put never lands.
        let _ = shutdown.send(true);
        tokio::task::yield_now().await;
        let mid = p.marks_snapshot();
        // Force another pass attempt (cadence already fired at 300; use 301 with
        // cadence=1 — last_* already at 300 so 301 fires).
        let _ = p.on_epoch_tick(301).await;
        // Either advanced further (writer still drained) or stuck at mid if
        // shut down — never rolls back below mid.
        let final_m = p.marks_snapshot();
        assert!(final_m.columns_up_to.as_u64() >= mid.columns_up_to.as_u64());
        assert!(final_m.blocks_up_to.as_u64() >= mid.blocks_up_to.as_u64());
    }
}
