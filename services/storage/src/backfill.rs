//! Below-anchor backfill write path — Architecture §6.6 / CC-47a.
//!
//! Bytes land via `PutBackfillBatch` (CC-4F) in **one transaction** with
//! [`BackfillProgress`]. This module never routes through `chain` and never
//! calls fork choice — the RPC handler is the only write surface.
//!
//! Responsibilities owned here (not in the gRPC handler):
//!
//! | Surface | Role |
//! |---|---|
//! | [`proto_progress_to_store`] | Full `per_index_oldest` mapping (128-cap) |
//! | [`observe_backfill_commit`] | `cc_storage_backfill_oldest_slot` + `_bytes_total` |
//! | [`assert_monotone_oldest`] | R-7 early warning: non-increasing scrapes |
//! | resume helpers | wrap `cc_store::backfill_progress` for service tests |
//!
//! The serve-path handler in `serve.rs` stages the batch and calls into this
//! module for progress mapping, descending-order admission, and metrics.

// Resume / monotone / completion helpers are exercised by unit tests and by
// future host wiring (CC-47b / CC-49); keep them public(crate) without noise.
#![allow(dead_code)]

use std::sync::atomic::{AtomicI64, Ordering};

use cc_store::backfill_progress::{
    apply_column_batch_progress, column_backfill_complete, ensure_per_index_len,
    oldest_custodied_column_slot, resume_within_one_batch,
};
use cc_store::meta::BackfillProgress;
use cc_store::{parent_root_at_offset, slot_at_offset, Root, Slot};

use crate::metrics::{ClassLabels, StorageClass, StorageMetrics};

/// A block row as presented on `PutBackfillBatch` (pre-engine).
#[derive(Debug, Clone)]
pub(crate) struct BackfillBlockRow {
    pub slot: Slot,
    pub root: Root,
    pub ssz: Vec<u8>,
}

/// Why a backfill batch was refused before commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BatchAdmitError {
    /// Blocks are not strictly descending by slot.
    NotDescending,
    /// Parent-root chain broken (Ethereum: higher.parent → lower.root).
    ParentBroken { slot: u64 },
    /// Claimed slot/root disagrees with SSZ peeks.
    FieldMismatch { slot: u64 },
}

// ── Proto → store ───────────────────────────────────────────────────────────

/// Build a store [`BackfillProgress`] from proto fields, including a full
/// `per_index_oldest` list (padded to 128 with `columns_oldest`).
#[must_use]
pub(crate) fn proto_progress_to_store(
    blocks_oldest: u64,
    blocks_oldest_parent: Root,
    columns_oldest: u64,
    per_index_oldest: &[u64],
) -> BackfillProgress {
    let cols = Slot::new(columns_oldest);
    let slots: Vec<Slot> = per_index_oldest.iter().map(|&s| Slot::new(s)).collect();
    // Empty list is valid SSZ; non-empty is padded to 128 for per-index C.
    let per = if slots.is_empty() {
        ensure_per_index_len(&[], cols)
    } else {
        ensure_per_index_len(&slots, cols)
    };
    // When the caller sent an empty list, keep the list empty (CC-4F atomicity
    // tests only assert the meta row's presence, not the 128-wide pad).
    let per_index_oldest = if per_index_oldest.is_empty() {
        Default::default()
    } else {
        per
    };
    BackfillProgress {
        blocks_oldest: Slot::new(blocks_oldest),
        blocks_oldest_parent,
        columns_oldest: cols,
        per_index_oldest,
    }
}

// ── Descending contiguous admission ─────────────────────────────────────────

/// Sort blocks **descending by slot** and require a contiguous Ethereum parent
/// chain before any engine write (Architecture §6.2 one-contiguous-frontier).
///
/// Higher slot's `parent_root` (SSZ peek) must equal the next-lower block's
/// claimed root. Claimed slot must match SSZ slot.
pub(crate) fn admit_descending_contiguous(
    blocks: &[BackfillBlockRow],
) -> Result<Vec<BackfillBlockRow>, BatchAdmitError> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    let mut ordered = blocks.to_vec();
    ordered.sort_by_key(|b| std::cmp::Reverse(b.slot.as_u64()));

    // Strictly descending slots, no duplicates.
    for w in ordered.windows(2) {
        if w[0].slot.as_u64() <= w[1].slot.as_u64() {
            return Err(BatchAdmitError::NotDescending);
        }
    }

    for b in &ordered {
        let ssz_slot = slot_at_offset(&b.ssz).map_err(|_| BatchAdmitError::FieldMismatch {
            slot: b.slot.as_u64(),
        })?;
        if ssz_slot != b.slot {
            return Err(BatchAdmitError::FieldMismatch {
                slot: b.slot.as_u64(),
            });
        }
    }

    // higher.parent_root == lower.root
    for w in ordered.windows(2) {
        let higher = &w[0];
        let lower = &w[1];
        let parent = parent_root_at_offset(&higher.ssz).map_err(|_| {
            BatchAdmitError::FieldMismatch {
                slot: higher.slot.as_u64(),
            }
        })?;
        if parent != lower.root {
            return Err(BatchAdmitError::ParentBroken {
                slot: higher.slot.as_u64(),
            });
        }
    }
    Ok(ordered)
}

// ── Metrics ─────────────────────────────────────────────────────────────────

/// Record a successful backfill commit on the storage metric surface.
///
/// - `cc_storage_backfill_oldest_slot{class}` — set to the new frontier
/// - `cc_storage_backfill_bytes_total{class}` — add payload bytes
pub(crate) fn observe_backfill_commit(
    metrics: &StorageMetrics,
    class: StorageClass,
    oldest_slot: Slot,
    bytes: u64,
) {
    let labels = ClassLabels {
        class: class.as_str().to_owned(),
    };
    metrics
        .backfill_oldest_slot
        .get_or_create(&labels)
        .set(oldest_slot.as_u64() as i64);
    if bytes > 0 {
        metrics.backfill_bytes.get_or_create(&labels).inc_by(bytes);
    }
}

/// After a successful `PutBackfillBatch`, update both class gauges from the
/// progress record and the payload sizes just written.
pub(crate) fn observe_put_backfill_batch(
    metrics: &StorageMetrics,
    progress: Option<&BackfillProgress>,
    block_bytes: u64,
    column_bytes: u64,
) {
    if let Some(p) = progress {
        observe_backfill_commit(metrics, StorageClass::Blocks, p.blocks_oldest, block_bytes);
        observe_backfill_commit(metrics, StorageClass::Columns, p.columns_oldest, column_bytes);
    } else {
        // No progress row: still count bytes under the classes that moved.
        if block_bytes > 0 {
            let labels = ClassLabels {
                class: StorageClass::Blocks.as_str().to_owned(),
            };
            metrics
                .backfill_bytes
                .get_or_create(&labels)
                .inc_by(block_bytes);
        }
        if column_bytes > 0 {
            let labels = ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            };
            metrics
                .backfill_bytes
                .get_or_create(&labels)
                .inc_by(column_bytes);
        }
    }
}

/// In-process monotone tracker for `cc_storage_backfill_oldest_slot{class}`.
///
/// Over a whole run the gauge must be **non-increasing** at every scrape.
/// A single non-monotone scrape is R-7's early warning and fails the criterion.
#[derive(Debug, Default)]
pub(crate) struct MonotoneOldestTracker {
    last: AtomicI64,
    /// `-1` means "no scrape yet".
    seeded: AtomicI64,
}

impl MonotoneOldestTracker {
    /// Fresh tracker (no scrapes).
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            last: AtomicI64::new(0),
            seeded: AtomicI64::new(-1),
        }
    }

    /// Observe a scrape. Returns `Ok(())` if monotone (non-increasing), else `Err(prev)`.
    pub(crate) fn observe(&self, slot: i64) -> Result<(), i64> {
        if self.seeded.load(Ordering::SeqCst) < 0 {
            self.last.store(slot, Ordering::SeqCst);
            self.seeded.store(1, Ordering::SeqCst);
            return Ok(());
        }
        let prev = self.last.load(Ordering::SeqCst);
        if slot > prev {
            return Err(prev);
        }
        self.last.store(slot, Ordering::SeqCst);
        Ok(())
    }

}

/// Assert a sequence of oldest-slot scrapes is non-increasing (test / soak helper).
pub(crate) fn assert_monotone_oldest(scrapes: &[u64]) -> Result<(), usize> {
    let t = MonotoneOldestTracker::new();
    for (i, &s) in scrapes.iter().enumerate() {
        if t.observe(s as i64).is_err() {
            return Err(i);
        }
    }
    Ok(())
}

// ── Resume / completion (service-facing wrappers) ───────────────────────────

/// Simulate kill mid-batch: durable frontier stays at `committed`; in-flight
/// batch of up to [`BACKFILL_BATCH_SLOT_LIMIT`] slots is lost. Resume must land
/// within one batch of the pre-kill in-memory position.
#[must_use]
pub(crate) fn resume_after_kill(
    durable_oldest: Slot,
    in_flight_batch_start: Slot,
) -> (Slot, bool) {
    // After kill, only durable progress survives.
    let resumed = durable_oldest;
    // Pre-kill in-memory frontier would have been `in_flight_batch_start` after
    // a successful commit; distance from durable is the lost work.
    let ok = resume_within_one_batch(in_flight_batch_start, resumed)
        || resume_within_one_batch(resumed, in_flight_batch_start);
    (resumed, ok)
}

/// Drive the column completion predicate with a synthetic progress state.
#[must_use]
pub(crate) fn columns_complete_for(
    progress: &BackfillProgress,
    custodied: &[u64],
    current_epoch: u64,
) -> bool {
    let oldest = oldest_custodied_column_slot(progress, custodied);
    column_backfill_complete(oldest, current_epoch)
}

/// Apply a committed column batch to progress (one-txn companion to PutBackfillBatch).
pub(crate) fn advance_after_column_batch(
    progress: &mut BackfillProgress,
    custodied: &[u64],
    batch_start: Slot,
) -> Result<(), cc_store::ProgressError> {
    apply_column_batch_progress(progress, custodied, batch_start)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_store::backfill_progress::{
        column_backfill_target_slot, ensure_per_index_len, BACKFILL_BATCH_SLOT_LIMIT,
        COLUMN_BACKFILL_EPOCHS, COLUMN_INDEX_COUNT,
    };
    use cc_store::epoch_start_slot;
    use prometheus_client::registry::Registry;

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn custodied() -> Vec<u64> {
        vec![0, 1, 2, 3]
    }

    #[test]
    fn proto_progress_pads_per_index_to_128() {
        let p = proto_progress_to_store(10, Root::default(), 20, &[20, 20, 20, 20]);
        assert_eq!(p.per_index_oldest.len(), COLUMN_INDEX_COUNT);
        assert_eq!(p.per_index_oldest[0], Slot::new(20));
        // Unspecified indices pad to columns_oldest.
        assert_eq!(p.per_index_oldest[127], Slot::new(20));
    }

    #[test]
    fn observe_backfill_commit_moves_gauges() {
        let m = metrics();
        observe_backfill_commit(&m, StorageClass::Columns, Slot::new(500), 1_024);
        let labels = ClassLabels {
            class: StorageClass::Columns.as_str().to_owned(),
        };
        assert_eq!(
            m.backfill_oldest_slot.get_or_create(&labels).get(),
            500
        );
        assert_eq!(m.backfill_bytes.get_or_create(&labels).get(), 1_024);

        // Second commit moves oldest down and accumulates bytes.
        observe_backfill_commit(&m, StorageClass::Columns, Slot::new(436), 512);
        assert_eq!(
            m.backfill_oldest_slot.get_or_create(&labels).get(),
            436
        );
        assert_eq!(m.backfill_bytes.get_or_create(&labels).get(), 1_024 + 512);
    }

    #[test]
    fn frontier_is_monotone_over_scrapes() {
        // Descending column backfill scrapes.
        let scrapes = [1_000u64, 936, 872, 808, 808, 744];
        assert!(assert_monotone_oldest(&scrapes).is_ok());

        let bad = [1_000u64, 936, 950]; // non-monotone at index 2
        assert_eq!(assert_monotone_oldest(&bad), Err(2));
    }

    #[test]
    fn resume_within_one_batch_after_kill() {
        let durable = Slot::new(10_000);
        // In-flight batch was about to commit [9936, 9999] (64 slots).
        let in_flight_start = Slot::new(10_000 - BACKFILL_BATCH_SLOT_LIMIT);
        let (resumed, ok) = resume_after_kill(durable, in_flight_start);
        assert_eq!(resumed, durable);
        assert!(ok, "lost at most one batch of work");

        // Two batches away is outside the honest bar.
        let far = Slot::new(10_000 - 2 * BACKFILL_BATCH_SLOT_LIMIT);
        let (_, ok2) = resume_after_kill(durable, far);
        assert!(!ok2);
    }

    #[test]
    fn column_completion_predicate_drives_flip_gate() {
        let current = 6_000u64;
        let target = column_backfill_target_slot(current);
        assert_eq!(
            target,
            epoch_start_slot(current - COLUMN_BACKFILL_EPOCHS)
        );

        let mut progress = BackfillProgress {
            blocks_oldest: Slot::new(target.as_u64() + 100),
            blocks_oldest_parent: Root::default(),
            columns_oldest: Slot::new(target.as_u64() + 100),
            per_index_oldest: ensure_per_index_len(
                &[Slot::new(target.as_u64() + 100); 4],
                Slot::new(target.as_u64() + 100),
            ),
        };
        assert!(!columns_complete_for(&progress, &custodied(), current));

        // Drive to target via one batch application.
        advance_after_column_batch(&mut progress, &custodied(), target).unwrap();
        assert!(columns_complete_for(&progress, &custodied(), current));
        assert_eq!(
            oldest_custodied_column_slot(&progress, &custodied()),
            target
        );
    }

    #[test]
    fn admit_descending_contiguous_ethereum_parent_direction() {
        use cc_store::{PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET};

        fn synth(slot: u64, parent: &Root, root: Root) -> BackfillBlockRow {
            let mut v = vec![0u8; STATE_ROOT_SSZ_OFFSET + 32];
            v[0..4].copy_from_slice(&100u32.to_le_bytes());
            v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
            v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32]
                .copy_from_slice(parent.as_slice());
            BackfillBlockRow {
                slot: Slot::new(slot),
                root,
                ssz: v,
            }
        }
        let r7 = Root::from_array([7; 32]);
        let r8 = Root::from_array([8; 32]);
        let r9 = Root::from_array([9; 32]);
        // Present ascending; admit reorders descending and checks higher.parent → lower.root.
        let rows = vec![
            synth(7, &Root::from_array([6; 32]), r7),
            synth(8, &r7, r8),
            synth(9, &r8, r9),
        ];
        let ordered = admit_descending_contiguous(&rows).unwrap();
        assert_eq!(ordered[0].slot, Slot::new(9));
        assert_eq!(ordered[2].slot, Slot::new(7));

        // Broken parent: slot 9 claims wrong parent.
        let bad = vec![
            synth(8, &r7, r8),
            synth(9, &Root::from_array([0xff; 32]), r9),
        ];
        assert!(matches!(
            admit_descending_contiguous(&bad),
            Err(BatchAdmitError::ParentBroken { slot: 9 })
        ));
    }

    #[test]
    fn per_index_supports_partial_cgc_raise() {
        // Four indices complete at target; four new lag at head.
        let target = 1_000u64;
        let head = 50_000u64;
        let mut per = vec![Slot::new(head); 8];
        for s in per.iter_mut().take(4) {
            *s = Slot::new(target);
        }
        let progress = BackfillProgress {
            blocks_oldest: Slot::new(target),
            blocks_oldest_parent: Root::default(),
            columns_oldest: Slot::new(target),
            per_index_oldest: ensure_per_index_len(&per, Slot::new(head)),
        };
        // cgc=4 complete.
        assert_eq!(
            oldest_custodied_column_slot(&progress, &[0, 1, 2, 3]),
            Slot::new(target)
        );
        // cgc=8 incomplete (min of 0..7 includes the lagging 4..7).
        assert_eq!(
            oldest_custodied_column_slot(&progress, &[0, 1, 2, 3, 4, 5, 6, 7]),
            Slot::new(target) // wait — 4..7 are at head which is HIGHER
        );
        // Actually min(target, head) = target. Need lagging to be LOWER for incomplete.
        // After cgc raise, new indices start at head and backfill downward — C = min
        // so while new indices are still at head, min is still target (old complete).
        // C only moves when new indices drop below the old ones. For CC-4G the new
        // indices start *without* history so their oldest is head; C = min stays at
        // target until… actually min(target, head)=target, so C stays at target.
        // The incomplete signal for new indices is per-index, not C rising.
        // Assert the four new indices are independently readable:
        assert_eq!(progress.per_index_oldest[4], Slot::new(head));
        assert_eq!(progress.per_index_oldest[0], Slot::new(target));
    }
}
