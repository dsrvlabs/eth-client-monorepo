//! Backfill progress helpers — Architecture §6.5 / CC-47a.
//!
//! [`BackfillProgress`] lives in [`crate::meta`]; this module owns the pure
//! rules that keep it honest:
//!
//! | Rule | Why |
//! |---|---|
//! | `per_index_oldest: List<Slot, 128>` | `cgc` 4→8 (CC-4G) backfills new indices independently |
//! | `C = min(custodied per_index_oldest)` | column floor is the **minimum over the custodied set** |
//! | one contiguous frontier | progress only moves **non-increasing** (descending backfill) |
//! | resume within one batch | in-flight batch at kill is lost; 64 slots is the honest bar |
//! | column completion | `oldest_custodied ≤ start_slot(current_epoch − 4096)` |
//!
//! Load / store of the meta singleton rides the same transaction as the batch
//! (`PutBackfillBatch`); helpers here never open their own writer.

use ssz::{Decode, Encode};
use ssz_types::VariableList;
use typenum::U128;

use cc_types::Slot;

use crate::engine::{Batch, Engine, ReadTxn, StoreError};
use crate::meta::{BackfillProgress, KEY_BACKFILL_PROG, TABLE_META};
use crate::split::{epoch_start_slot, SLOTS_PER_EPOCH};

/// Spec `MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS` (≈ 18 days).
pub const COLUMN_BACKFILL_EPOCHS: u64 = 4_096;

/// Max slots per by-range batch (same as p2p planner / Architecture §6.2).
pub const BACKFILL_BATCH_SLOT_LIMIT: u64 = 64;

/// Number of column indices (`NUMBER_OF_COLUMNS`).
pub const COLUMN_INDEX_COUNT: usize = 128;

// ── Load / put ──────────────────────────────────────────────────────────────

/// Read [`BackfillProgress`] from the meta table, if present.
pub fn load_backfill_progress(engine: &Engine) -> Result<Option<BackfillProgress>, StoreError> {
    let rt = engine.read()?;
    load_backfill_progress_txn(&rt)
}

/// Read under an existing read txn.
pub fn load_backfill_progress_txn(rt: &ReadTxn) -> Result<Option<BackfillProgress>, StoreError> {
    match rt.get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())? {
        Some(bytes) => {
            let p = BackfillProgress::from_ssz_bytes(&bytes).map_err(|e| {
                StoreError::Codec(format!("BackfillProgress SSZ decode: {e:?}"))
            })?;
            Ok(Some(p))
        }
        None => Ok(None),
    }
}

/// Stage a full progress record into `batch` (same-transaction rule).
pub fn put_backfill_progress(batch: &mut Batch, progress: &BackfillProgress) {
    batch.put(
        TABLE_META,
        KEY_BACKFILL_PROG.as_bytes(),
        &progress.as_ssz_bytes(),
    );
}

// ── Per-index / C ───────────────────────────────────────────────────────────

/// Build a 128-entry `per_index_oldest` list, defaulting missing entries to
/// `default_slot` (typically the current frontier / head).
#[must_use]
pub fn ensure_per_index_len(
    list: &[Slot],
    default_slot: Slot,
) -> VariableList<Slot, U128> {
    let mut v: Vec<Slot> = list.to_vec();
    if v.len() > COLUMN_INDEX_COUNT {
        v.truncate(COLUMN_INDEX_COUNT);
    }
    while v.len() < COLUMN_INDEX_COUNT {
        v.push(default_slot);
    }
    // Length is clamped to COLUMN_INDEX_COUNT == 128, which is U128 capacity.
    VariableList::new(v).unwrap_or_default()
}

/// `C` = minimum of `per_index_oldest[i]` over the **custodied** indices.
///
/// Empty custodied set → `columns_oldest` scalar (fallback).
#[must_use]
pub fn oldest_custodied_column_slot(
    progress: &BackfillProgress,
    custodied_indices: &[u64],
) -> Slot {
    if custodied_indices.is_empty() {
        return progress.columns_oldest;
    }
    let entries = progress.per_index_oldest.as_ref();
    let mut min = Slot::new(u64::MAX);
    let mut any = false;
    for &idx in custodied_indices {
        let i = idx as usize;
        let slot = entries.get(i).copied().unwrap_or(progress.columns_oldest);
        if slot.as_u64() < min.as_u64() {
            min = slot;
            any = true;
        }
    }
    if any {
        min
    } else {
        progress.columns_oldest
    }
}

/// Advance one index's oldest slot **only if non-increasing** (descending frontier).
///
/// Returns `true` when the value changed. A non-monotone (increasing) write is
/// refused — that is R-7's early warning for out-of-order commits.
pub fn advance_per_index_oldest(
    progress: &mut BackfillProgress,
    index: u64,
    new_oldest: Slot,
) -> Result<bool, ProgressError> {
    let i = index as usize;
    if i >= COLUMN_INDEX_COUNT {
        return Err(ProgressError::IndexOutOfRange(index));
    }
    // Ensure capacity.
    if progress.per_index_oldest.len() <= i {
        let default = progress.columns_oldest;
        let filled = ensure_per_index_len(progress.per_index_oldest.as_ref(), default);
        progress.per_index_oldest = filled;
    }
    let cur = progress.per_index_oldest[i];
    if new_oldest.as_u64() > cur.as_u64() && cur.as_u64() != 0 {
        // Non-monotone: refuse. (Zero means "unset" at genesis-style seed.)
        return Err(ProgressError::NonMonotone {
            index,
            current: cur.as_u64(),
            attempted: new_oldest.as_u64(),
        });
    }
    if new_oldest.as_u64() == cur.as_u64() {
        return Ok(false);
    }
    // VariableList does not expose &mut [T] directly on all versions; rebuild.
    let mut v: Vec<Slot> = progress.per_index_oldest.iter().copied().collect();
    while v.len() <= i {
        v.push(progress.columns_oldest);
    }
    v[i] = new_oldest;
    progress.per_index_oldest =
        VariableList::new(v).map_err(|_| ProgressError::IndexOutOfRange(index))?;
    Ok(true)
}

/// After a successful column batch covering `[batch_start, batch_end]`
/// (inclusive) for `indices`, move each index's frontier to `batch_start`
/// (the new oldest held), then recompute `columns_oldest = C`.
pub fn apply_column_batch_progress(
    progress: &mut BackfillProgress,
    custodied_indices: &[u64],
    batch_start: Slot,
) -> Result<(), ProgressError> {
    for &idx in custodied_indices {
        // Descending: new oldest is the start of the committed batch.
        let _ = advance_per_index_oldest(progress, idx, batch_start)?;
    }
    progress.columns_oldest = oldest_custodied_column_slot(progress, custodied_indices);
    Ok(())
}

/// After a successful block batch, move `blocks_oldest` downward (non-increasing).
pub fn apply_block_batch_progress(
    progress: &mut BackfillProgress,
    batch_start: Slot,
    batch_start_parent: cc_types::Root,
) -> Result<(), ProgressError> {
    let cur = progress.blocks_oldest.as_u64();
    let next = batch_start.as_u64();
    if next > cur && cur != 0 {
        return Err(ProgressError::NonMonotone {
            index: u64::MAX, // blocks class
            current: cur,
            attempted: next,
        });
    }
    progress.blocks_oldest = batch_start;
    progress.blocks_oldest_parent = batch_start_parent;
    Ok(())
}

// ── Completion / target ─────────────────────────────────────────────────────

/// Column backfill target slot: `start_slot(current_epoch − 4096)`.
#[must_use]
pub fn column_backfill_target_slot(current_epoch: u64) -> Slot {
    let target_epoch = current_epoch.saturating_sub(COLUMN_BACKFILL_EPOCHS);
    epoch_start_slot(target_epoch)
}

/// Column completion predicate (CC-47 /7) — **what triggers CC-49's flip**.
///
/// `oldest_custodied_column_slot ≤ start_slot(current_epoch − 4 096)`.
#[must_use]
pub fn column_backfill_complete(
    oldest_custodied: Slot,
    current_epoch: u64,
) -> bool {
    let target = column_backfill_target_slot(current_epoch);
    oldest_custodied.as_u64() <= target.as_u64()
}

/// Whether the serve window is above its column target (fifth-trigger half).
#[must_use]
pub fn serve_window_above_column_target(
    earliest_available_slot: Slot,
    current_epoch: u64,
) -> bool {
    earliest_available_slot.as_u64() > column_backfill_target_slot(current_epoch).as_u64()
}

// ── Resume ──────────────────────────────────────────────────────────────────

/// Resume frontier for columns: `C` from durable progress (or `fallback`).
#[must_use]
pub fn resume_column_frontier(
    progress: Option<&BackfillProgress>,
    custodied_indices: &[u64],
    fallback: Slot,
) -> Slot {
    match progress {
        Some(p) => oldest_custodied_column_slot(p, custodied_indices),
        None => fallback,
    }
}

/// Honest resume bar: `|prev − resumed| ≤ BACKFILL_BATCH_SLOT_LIMIT`.
///
/// The in-flight batch at kill time is lost and refetched — 64 slots of work,
/// the price of never committing a partial batch.
#[must_use]
pub fn resume_within_one_batch(prev_oldest: Slot, resumed_oldest: Slot) -> bool {
    let a = prev_oldest.as_u64();
    let b = resumed_oldest.as_u64();
    let delta = a.abs_diff(b);
    delta <= BACKFILL_BATCH_SLOT_LIMIT
}

/// Slots per epoch used by progress arithmetic (mainnet / store constant).
#[must_use]
pub const fn slots_per_epoch() -> u64 {
    SLOTS_PER_EPOCH
}

// ── Errors ──────────────────────────────────────────────────────────────────

/// Progress-mutation failures (non-monotone writes, bad indices).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProgressError {
    /// Column index ≥ 128.
    #[error("column index {0} out of range (NUMBER_OF_COLUMNS = 128)")]
    IndexOutOfRange(u64),
    /// Attempted to move a frontier **upward** (out-of-order commit / R-7).
    #[error(
        "non-monotone backfill progress for index {index}: current={current}, attempted={attempted}"
    )]
    NonMonotone {
        /// Column index, or `u64::MAX` for the blocks class.
        index: u64,
        /// Durable oldest before the write.
        current: u64,
        /// Rejected new value.
        attempted: u64,
    },
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::engine::{Durability, EngineOptions};
    use cc_types::Root;

    fn custodied_four() -> Vec<u64> {
        vec![0, 1, 2, 3]
    }

    fn progress_at(columns_oldest: u64, per: &[u64]) -> BackfillProgress {
        let slots: Vec<Slot> = per.iter().map(|&s| Slot::new(s)).collect();
        BackfillProgress {
            blocks_oldest: Slot::new(columns_oldest),
            blocks_oldest_parent: Root::default(),
            columns_oldest: Slot::new(columns_oldest),
            per_index_oldest: ensure_per_index_len(&slots, Slot::new(columns_oldest)),
        }
    }

    #[test]
    fn c_is_minimum_over_custodied_not_all_128() {
        // Indices 0..3 at 100; index 7 (sampled-not-custodied) at 10.
        let mut per = vec![100u64; 8];
        per[7] = 10;
        let p = progress_at(100, &per);
        let c = oldest_custodied_column_slot(&p, &custodied_four());
        assert_eq!(c, Slot::new(100), "sampled-not-custodied must not move C");
        // Raise one custodied index lagging:
        let mut per2 = vec![100u64; 4];
        per2[2] = 40;
        let p2 = progress_at(100, &per2);
        assert_eq!(
            oldest_custodied_column_slot(&p2, &custodied_four()),
            Slot::new(40)
        );
    }

    #[test]
    fn advance_refuses_non_monotone() {
        let mut p = progress_at(50, &[50, 50, 50, 50]);
        // Descending 50 → 40 is fine.
        assert!(advance_per_index_oldest(&mut p, 0, Slot::new(40)).unwrap());
        assert_eq!(p.per_index_oldest[0], Slot::new(40));
        // Upward 40 → 45 is R-7.
        let err = advance_per_index_oldest(&mut p, 0, Slot::new(45)).unwrap_err();
        assert!(matches!(err, ProgressError::NonMonotone { .. }));
    }

    #[test]
    fn apply_column_batch_updates_c() {
        let mut p = progress_at(200, &[200, 200, 200, 200]);
        apply_column_batch_progress(&mut p, &custodied_four(), Slot::new(136)).unwrap();
        assert_eq!(p.columns_oldest, Slot::new(136));
        for i in 0..4 {
            assert_eq!(p.per_index_oldest[i], Slot::new(136));
        }
    }

    #[test]
    fn column_completion_predicate_not_a_timer() {
        // current_epoch = 5000 → target = start_slot(5000 − 4096) = start_slot(904)
        let current = 5_000u64;
        let target = column_backfill_target_slot(current);
        assert_eq!(target, epoch_start_slot(current - COLUMN_BACKFILL_EPOCHS));
        assert_eq!(target.as_u64(), 904 * SLOTS_PER_EPOCH);

        // Above target → incomplete.
        assert!(!column_backfill_complete(Slot::new(target.as_u64() + 1), current));
        // At target → complete.
        assert!(column_backfill_complete(target, current));
        // Below target → complete.
        assert!(column_backfill_complete(Slot::new(target.as_u64().saturating_sub(1)), current));
    }

    #[test]
    fn resume_within_one_batch_is_the_honest_bar() {
        let prev = Slot::new(10_000);
        // Lost the in-flight batch (64 slots higher = less progressed).
        let resumed = Slot::new(10_000 + BACKFILL_BATCH_SLOT_LIMIT);
        assert!(resume_within_one_batch(prev, resumed));
        // Two batches away fails the bar.
        let too_far = Slot::new(10_000 + BACKFILL_BATCH_SLOT_LIMIT + 1);
        assert!(!resume_within_one_batch(prev, too_far));
        // Exact resume also fine.
        assert!(resume_within_one_batch(prev, prev));
    }

    #[test]
    fn resume_column_frontier_reads_durable_c() {
        let p = progress_at(777, &[777, 800, 777, 900]);
        // min custodied = 777
        let frontier = resume_column_frontier(Some(&p), &custodied_four(), Slot::new(0));
        assert_eq!(frontier, Slot::new(777));
        let none = resume_column_frontier(None, &custodied_four(), Slot::new(42));
        assert_eq!(none, Slot::new(42));
    }

    #[test]
    fn serve_window_above_target_detects_fifth_trigger_condition() {
        let current = 5_000u64;
        let target = column_backfill_target_slot(current);
        assert!(serve_window_above_column_target(
            Slot::new(target.as_u64() + 32),
            current
        ));
        assert!(!serve_window_above_column_target(target, current));
    }

    #[test]
    fn put_and_load_roundtrip_same_transaction_key() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-bf-prog-{n}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .expect("open engine");

        let mut p = progress_at(64, &[64, 64, 64, 64]);
        apply_column_batch_progress(&mut p, &custodied_four(), Slot::new(0)).unwrap();

        let mut batch = engine.batch();
        put_backfill_progress(&mut batch, &p);
        engine.commit(batch).unwrap();

        let loaded = load_backfill_progress(&engine).unwrap().unwrap();
        assert_eq!(loaded.columns_oldest, Slot::new(0));
        assert_eq!(loaded.per_index_oldest[0], Slot::new(0));
        assert_eq!(
            oldest_custodied_column_slot(&loaded, &custodied_four()),
            Slot::new(0)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_block_batch_monotone() {
        let mut p = progress_at(100, &[100]);
        p.blocks_oldest = Slot::new(100);
        apply_block_batch_progress(&mut p, Slot::new(36), Root::default()).unwrap();
        assert_eq!(p.blocks_oldest, Slot::new(36));
        let err = apply_block_batch_progress(&mut p, Slot::new(50), Root::default()).unwrap_err();
        assert!(matches!(err, ProgressError::NonMonotone { .. }));
    }
}
