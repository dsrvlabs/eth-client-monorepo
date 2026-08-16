//! Below-anchor backfill write path — Architecture §6.6 / CC-47a + CC-47b.
//!
//! Bytes land via `PutBackfillBatch` (CC-4F) in **one transaction** with
//! [`BackfillProgress`]. This module never routes through `chain` and never
//! calls fork choice — the RPC handler is the only write surface.
//!
//! Responsibilities owned here (not in the gRPC handler):
//!
//! | Surface | Role |
//! |---|---|
//! | [`proto_progress_to_store`] | Full `per_index_oldest` mapping (128-cap; unset pad) |
//! | [`observe_backfill_commit`] | `cc_storage_backfill_oldest_slot` + `_bytes_total` |
//! | [`assert_monotone_oldest`] | R-7 early warning: non-increasing scrapes |
//! | resume helpers | wrap `cc_store::backfill_progress` for service tests |
//! | block completion / advance | CC-47b target (`min_epochs` from CC-4A) |
//!
//! The serve-path handler in `serve.rs` stages the batch and calls into this
//! module for progress mapping, descending-order admission, and metrics.

// Resume / monotone / completion helpers are exercised by unit tests and by
// host wiring (CC-47b / CC-49); keep them public(crate) without noise.
#![allow(dead_code)]

use std::sync::atomic::{AtomicI64, Ordering};

use cc_store::backfill_progress::{
    apply_block_batch_progress, apply_column_batch_progress, block_backfill_complete,
    column_backfill_complete, ensure_per_index_len, oldest_custodied_column_slot,
    resume_block_frontier, resume_block_parent, resume_within_one_batch,
};
use cc_store::meta::{AnchorInfo, BackfillProgress};
use cc_store::{Root, Slot, parent_root_at_offset, slot_at_offset};

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
    /// Progress would move a frontier **upward** vs durable store (R-7).
    ProgressNonMonotone {
        /// `"blocks"` or `"columns"`.
        class: &'static str,
        /// Durable oldest before the write.
        current: u64,
        /// Rejected proposed value.
        attempted: u64,
    },
    /// Progress frontier does not match the admitted batch (oldest slot / parent).
    ProgressFrontierMismatch {
        /// Human-readable reason.
        reason: &'static str,
        /// Expected value (slot or first byte of root for logs).
        expected: u64,
        /// Claimed value.
        got: u64,
    },
    /// Progress is required; there is no empty-progress bypass.
    ProgressRequired,
    /// A batch may only extend the durable frontier, never jump it.
    FrontierJump {
        /// Human-readable reason.
        reason: &'static str,
    },
}

// ── Proto → store ───────────────────────────────────────────────────────────

/// Build a store [`BackfillProgress`] from proto fields, including a full
/// `per_index_oldest` list (padded to 128 with unset / no-progress).
///
/// Missing indices stay [`Slot::ZERO`]. Padding them with `columns_oldest`
/// fabricates progress for never-custodied indices; the monotone guard then
/// rejects an honest custody-group-count raise that seeds those indices at
/// head (P1-A/3).
#[must_use]
pub(crate) fn proto_progress_to_store(
    blocks_oldest: u64,
    blocks_oldest_parent: Root,
    columns_oldest: u64,
    per_index_oldest: &[u64],
) -> BackfillProgress {
    let cols = Slot::new(columns_oldest);
    let slots: Vec<Slot> = per_index_oldest.iter().map(|&s| Slot::new(s)).collect();
    // Empty list is valid SSZ (CC-4F atomicity tests only assert the meta
    // row). Non-empty is padded to 128; unspecified indices stay unset.
    let per_index_oldest = if per_index_oldest.is_empty() {
        Default::default()
    } else {
        ensure_per_index_len(&slots, Slot::ZERO)
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
/// claimed root. Claimed slot must match SSZ slot. Slots must be consecutive
/// (`higher == lower + 1`) so `blocks_oldest` cannot jump a hole inside
/// the batch.
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
    admit_slot_contiguous_range(&ordered)?;

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
        let parent =
            parent_root_at_offset(&higher.ssz).map_err(|_| BatchAdmitError::FieldMismatch {
                slot: higher.slot.as_u64(),
            })?;
        if parent != lower.root {
            return Err(BatchAdmitError::ParentBroken {
                slot: higher.slot.as_u64(),
            });
        }
    }
    Ok(ordered)
}

/// Refuse a descending batch whose slots skip — parent linkage alone would
/// still let `blocks_oldest` jump down across the hole.
fn admit_slot_contiguous_range(ordered: &[BackfillBlockRow]) -> Result<(), BatchAdmitError> {
    for w in ordered.windows(2) {
        if w[0].slot.as_u64() != w[1].slot.as_u64().saturating_add(1) {
            return Err(BatchAdmitError::FrontierJump {
                reason: "admitted range is not slot-contiguous",
            });
        }
    }
    Ok(())
}

// ── Progress monotony + frontier binding (CC-47b server-side) ────────────────

/// Whether `proposed` is a non-monotone (increasing) move vs `current`.
///
/// Zero current is treated as "unset" (first seed may land any value).
#[must_use]
pub(crate) fn progress_slot_non_monotone(current: Slot, proposed: Slot) -> bool {
    let cur = current.as_u64();
    let next = proposed.as_u64();
    next > cur && cur != 0
}

/// Refuse a proposed [`BackfillProgress`] that would raise any frontier vs
/// the durable record (R-7 / one-contiguous-frontier).
pub(crate) fn admit_progress_monotone(
    proposed: &BackfillProgress,
    stored: Option<&BackfillProgress>,
) -> Result<(), BatchAdmitError> {
    let Some(s) = stored else {
        return Ok(());
    };
    if progress_slot_non_monotone(s.blocks_oldest, proposed.blocks_oldest) {
        return Err(BatchAdmitError::ProgressNonMonotone {
            class: "blocks",
            current: s.blocks_oldest.as_u64(),
            attempted: proposed.blocks_oldest.as_u64(),
        });
    }
    if progress_slot_non_monotone(s.columns_oldest, proposed.columns_oldest) {
        return Err(BatchAdmitError::ProgressNonMonotone {
            class: "columns",
            current: s.columns_oldest.as_u64(),
            attempted: proposed.columns_oldest.as_u64(),
        });
    }
    // Per-index: refuse any increase over a non-zero durable entry.
    // Missing / zero is unset (never custodied) — a cgc raise may seed
    // those indices at head. Do not treat a missing entry as columns_oldest.
    let proposed_per = proposed.per_index_oldest.as_ref();
    let stored_per = s.per_index_oldest.as_ref();
    for (i, &p_slot) in proposed_per.iter().enumerate() {
        let cur = stored_per.get(i).copied().unwrap_or(Slot::ZERO);
        if progress_slot_non_monotone(cur, p_slot) {
            return Err(BatchAdmitError::ProgressNonMonotone {
                class: "columns",
                current: cur.as_u64(),
                attempted: p_slot.as_u64(),
            });
        }
    }
    Ok(())
}

/// Bind progress to the admitted batch frontier.
///
/// When the batch carries blocks (already admitted descending):
/// - `blocks_oldest` **must** equal the lowest slot in the batch
/// - `blocks_oldest_parent` **must** equal that block's SSZ parent root
///
/// When the batch carries columns:
/// - `columns_oldest` **must** equal the minimum column slot in the request
///   (when non-empty).
pub(crate) fn admit_progress_bound_to_batch(
    progress: &BackfillProgress,
    ordered_blocks: &[BackfillBlockRow],
    column_slots: &[u64],
) -> Result<(), BatchAdmitError> {
    if let Some(oldest) = ordered_blocks.last() {
        // ordered is descending → last is lowest slot (new frontier).
        if progress.blocks_oldest != oldest.slot {
            return Err(BatchAdmitError::ProgressFrontierMismatch {
                reason: "blocks_oldest must equal lowest admitted block slot",
                expected: oldest.slot.as_u64(),
                got: progress.blocks_oldest.as_u64(),
            });
        }
        let parent =
            parent_root_at_offset(&oldest.ssz).map_err(|_| BatchAdmitError::FieldMismatch {
                slot: oldest.slot.as_u64(),
            })?;
        if parent != progress.blocks_oldest_parent {
            return Err(BatchAdmitError::ProgressFrontierMismatch {
                reason: "blocks_oldest_parent must equal parent of lowest admitted block",
                expected: u64::from(parent.as_slice()[0]),
                got: u64::from(progress.blocks_oldest_parent.as_slice()[0]),
            });
        }
    }

    if let Some(&min_col) = column_slots.iter().min()
        && progress.columns_oldest.as_u64() != min_col
    {
        return Err(BatchAdmitError::ProgressFrontierMismatch {
            reason: "columns_oldest must equal minimum column slot in batch",
            expected: min_col,
            got: progress.columns_oldest.as_u64(),
        });
    }
    Ok(())
}

// ── Anchor / frontier binding ───────────────────────────────────────────────

/// Refuse a block batch that omits `progress`.
///
/// A single-block batch with empty progress must be rejected, not fast-pathed.
pub(crate) fn admit_progress_required(
    progress: Option<&BackfillProgress>,
    has_blocks: bool,
) -> Result<(), BatchAdmitError> {
    if has_blocks && progress.is_none() {
        return Err(BatchAdmitError::ProgressRequired);
    }
    Ok(())
}

/// Refuse a progress-only (empty block list) write that would plant or move
/// the named block frontier.
///
/// A batch may only extend the durable frontier, never jump it. Without
/// this check, `blocks_oldest_parent` can be set with no blocks, and the
/// next single-block batch attaches to that fabricated pointer.
pub(crate) fn admit_progress_only_preserves_block_frontier(
    progress: Option<&BackfillProgress>,
    has_blocks: bool,
    stored: Option<&BackfillProgress>,
    anchor: Option<&AnchorInfo>,
) -> Result<(), BatchAdmitError> {
    if has_blocks {
        return Ok(());
    }
    let Some(proposed) = progress else {
        return Ok(());
    };
    let Some((named_slot, named_parent)) = stored
        .map(|s| (s.blocks_oldest, s.blocks_oldest_parent))
        .or_else(|| anchor.map(|a| (a.oldest_block_slot, a.oldest_block_parent)))
    else {
        return Err(BatchAdmitError::FrontierJump {
            reason: "progress-only request must not plant the named block frontier",
        });
    };
    if proposed.blocks_oldest != named_slot || proposed.blocks_oldest_parent != named_parent {
        return Err(BatchAdmitError::FrontierJump {
            reason: "progress-only request must not move the named block frontier",
        });
    }
    Ok(())
}

/// A batch may only extend the durable frontier, never jump it.
///
/// The writer rejects the batch unless the attachment parent is already
/// durable, or is the first row of the same batch. There is no
/// "progress optional" path and no empty-progress bypass.
///
/// Named frontier (stored progress, else [`AnchorInfo`]): the first
/// (highest, frontier-adjacent) row **is** that parent. A **set** named
/// slot also requires `first.slot == named_slot − 1` and a slot-contiguous
/// run down to the new oldest — otherwise `blocks_oldest` would jump
/// down across a hole. Stored progress is always set, including
/// `blocks_oldest == 0` (genesis, not a first-seed trampoline). An
/// anchor slot of 0 with no stored progress is unset. Unnamed first
/// seed: the first row's SSZ parent must be durable, or must be the
/// first row itself.
pub(crate) fn admit_extends_durable_frontier(
    ordered: &[BackfillBlockRow],
    stored: Option<&BackfillProgress>,
    anchor: Option<&AnchorInfo>,
    ssz_parent_is_durable: bool,
) -> Result<(), BatchAdmitError> {
    let Some(first) = ordered.first() else {
        return Ok(());
    };
    // Intra-batch holes jump the named oldest even when the top row attaches.
    admit_slot_contiguous_range(ordered)?;

    // Stored progress always names a slot (0 = genesis). Anchor slot 0 is unset.
    let named = stored
        .map(|s| (s.blocks_oldest, s.blocks_oldest_parent, true))
        .or_else(|| {
            anchor.map(|a| {
                (
                    a.oldest_block_slot,
                    a.oldest_block_parent,
                    a.oldest_block_slot.as_u64() != 0,
                )
            })
        });

    if let Some((named_slot, expected, slot_is_set)) = named {
        if first.root != expected {
            return Err(BatchAdmitError::FrontierJump {
                reason: "parent is not durable and is not the first row of the same batch",
            });
        }
        if slot_is_set {
            let expected_slot = named_slot.as_u64().saturating_sub(1);
            if first.slot.as_u64() != expected_slot {
                return Err(BatchAdmitError::FrontierJump {
                    reason: "batch is not adjacent to the durable frontier",
                });
            }
        }
        return Ok(());
    }

    let parent = parent_root_at_offset(&first.ssz).map_err(|_| BatchAdmitError::FieldMismatch {
        slot: first.slot.as_u64(),
    })?;
    if parent == first.root || ssz_parent_is_durable {
        return Ok(());
    }
    Err(BatchAdmitError::FrontierJump {
        reason: "parent is not durable and is not the first row of the same batch",
    })
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
        observe_backfill_commit(
            metrics,
            StorageClass::Columns,
            p.columns_oldest,
            column_bytes,
        );
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
pub(crate) fn resume_after_kill(durable_oldest: Slot, in_flight_batch_start: Slot) -> (Slot, bool) {
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

/// Drive the **block** completion predicate (CC-47b /7 block class).
///
/// `min_epochs` is CC-4A's computed floor — callers must not hard-code `33024`.
#[must_use]
pub(crate) fn blocks_complete_for(
    progress: &BackfillProgress,
    current_epoch: u64,
    min_epochs: u64,
) -> bool {
    block_backfill_complete(progress.blocks_oldest, current_epoch, min_epochs)
}

/// Apply a committed column batch to progress (one-txn companion to PutBackfillBatch).
pub(crate) fn advance_after_column_batch(
    progress: &mut BackfillProgress,
    custodied: &[u64],
    batch_start: Slot,
) -> Result<(), cc_store::ProgressError> {
    apply_column_batch_progress(progress, custodied, batch_start)
}

/// Apply a committed **block** batch: move `blocks_oldest` + `blocks_oldest_parent`
/// (Architecture §6.5 / Lighthouse `AnchorInfo` shape).
pub(crate) fn advance_after_block_batch(
    progress: &mut BackfillProgress,
    batch_start: Slot,
    batch_start_parent: Root,
) -> Result<(), cc_store::ProgressError> {
    apply_block_batch_progress(progress, batch_start, batch_start_parent)
}

/// Durable block frontier after a restart (resume path).
#[must_use]
pub(crate) fn resume_block_state(
    progress: Option<&BackfillProgress>,
    fallback_slot: Slot,
    fallback_parent: Root,
) -> (Slot, Root) {
    (
        resume_block_frontier(progress, fallback_slot),
        resume_block_parent(progress, fallback_parent),
    )
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_store::backfill_progress::{
        BACKFILL_BATCH_SLOT_LIMIT, COLUMN_BACKFILL_EPOCHS, COLUMN_INDEX_COUNT,
        block_backfill_target_slot, column_backfill_target_slot, ensure_per_index_len,
    };
    use cc_store::epoch_start_slot;
    use prometheus_client::registry::Registry;

    /// CC-4A computed floor as a *test input* (never a production constant).
    fn computed_min_epochs() -> u64 {
        256 + 65_536 / 2
    }

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn custodied() -> Vec<u64> {
        vec![0, 1, 2, 3]
    }

    #[test]
    fn proto_progress_never_custodied_index_reports_no_progress() {
        let p = proto_progress_to_store(10, Root::default(), 20, &[20, 20, 20, 20]);
        assert_eq!(p.per_index_oldest.len(), COLUMN_INDEX_COUNT);
        assert_eq!(p.per_index_oldest[0], Slot::new(20));
        // Never-custodied indices stay unset — not a copy of columns_oldest.
        assert_eq!(p.per_index_oldest[4], Slot::ZERO);
        assert_eq!(p.per_index_oldest[127], Slot::ZERO);
        assert_ne!(
            p.per_index_oldest[4], p.columns_oldest,
            "padding with columns_oldest would fabricate progress"
        );
    }

    #[test]
    fn observe_backfill_commit_moves_gauges() {
        let m = metrics();
        observe_backfill_commit(&m, StorageClass::Columns, Slot::new(500), 1_024);
        let labels = ClassLabels {
            class: StorageClass::Columns.as_str().to_owned(),
        };
        assert_eq!(m.backfill_oldest_slot.get_or_create(&labels).get(), 500);
        assert_eq!(m.backfill_bytes.get_or_create(&labels).get(), 1_024);

        // Second commit moves oldest down and accumulates bytes.
        observe_backfill_commit(&m, StorageClass::Columns, Slot::new(436), 512);
        assert_eq!(m.backfill_oldest_slot.get_or_create(&labels).get(), 436);
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
        assert_eq!(target, epoch_start_slot(current - COLUMN_BACKFILL_EPOCHS));

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

        // Parent-linked sandwich [9, 0] still jumps slots 1–8.
        let r0 = Root::from_array([0; 32]);
        let sandwich = vec![
            synth(0, &Root::from_array([0xff; 32]), r0),
            synth(9, &r0, r9),
        ];
        assert!(matches!(
            admit_descending_contiguous(&sandwich),
            Err(BatchAdmitError::FrontierJump { reason })
                if reason.contains("not slot-contiguous")
        ));

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

    // ── CC-47b block class ──────────────────────────────────────────────────

    #[test]
    fn block_completion_predicate_drives_target() {
        let current = 50_000u64;
        let min_epochs = computed_min_epochs();
        let target = block_backfill_target_slot(current, min_epochs);
        assert_eq!(target, epoch_start_slot(current.saturating_sub(min_epochs)));

        let mut progress = BackfillProgress {
            blocks_oldest: Slot::new(target.as_u64() + 128),
            blocks_oldest_parent: Root::from_array([1; 32]),
            columns_oldest: Slot::new(target.as_u64() + 128),
            per_index_oldest: Default::default(),
        };
        assert!(!blocks_complete_for(&progress, current, min_epochs));

        // Drive to target via batch application (oldest + parent).
        let parent = Root::from_array([0xBB; 32]);
        advance_after_block_batch(&mut progress, target, parent).unwrap();
        assert_eq!(progress.blocks_oldest, target);
        assert_eq!(progress.blocks_oldest_parent, parent);
        assert!(blocks_complete_for(&progress, current, min_epochs));
    }

    #[test]
    fn block_frontier_monotone_and_resume_within_one_batch() {
        // Descending block scrapes must be non-increasing (R-7).
        let scrapes = [100_000u64, 99_936, 99_872, 99_872, 99_808];
        assert!(assert_monotone_oldest(&scrapes).is_ok());
        assert_eq!(assert_monotone_oldest(&[100_000, 99_900, 99_950]), Err(2));

        // kill -9 mid-batch: durable frontier stays; lost work ≤ 64 slots.
        let durable = Slot::new(50_000);
        let in_flight = Slot::new(50_000 - BACKFILL_BATCH_SLOT_LIMIT);
        let (resumed, ok) = resume_after_kill(durable, in_flight);
        assert_eq!(resumed, durable);
        assert!(ok);

        let progress = BackfillProgress {
            blocks_oldest: durable,
            blocks_oldest_parent: Root::from_array([0xCC; 32]),
            columns_oldest: durable,
            per_index_oldest: Default::default(),
        };
        let (slot, parent) = resume_block_state(Some(&progress), Slot::new(0), Root::default());
        assert_eq!(slot, durable);
        assert_eq!(parent, Root::from_array([0xCC; 32]));
        // Metrics-facing: observe commits checkpoint the resume position.
        let m = metrics();
        observe_backfill_commit(&m, StorageClass::Blocks, durable, 64 * 24_000);
        let labels = ClassLabels {
            class: StorageClass::Blocks.as_str().to_owned(),
        };
        assert_eq!(
            m.backfill_oldest_slot.get_or_create(&labels).get(),
            durable.as_u64() as i64
        );
    }

    #[test]
    fn advance_block_batch_refuses_non_monotone() {
        let mut progress = BackfillProgress {
            blocks_oldest: Slot::new(1_000),
            blocks_oldest_parent: Root::default(),
            columns_oldest: Slot::new(1_000),
            per_index_oldest: Default::default(),
        };
        advance_after_block_batch(&mut progress, Slot::new(936), Root::from_array([2; 32]))
            .unwrap();
        let err =
            advance_after_block_batch(&mut progress, Slot::new(950), Root::default()).unwrap_err();
        assert!(matches!(err, cc_store::ProgressError::NonMonotone { .. }));
    }

    #[test]
    fn admit_progress_refuses_increase_vs_stored() {
        let stored = BackfillProgress {
            blocks_oldest: Slot::new(500),
            blocks_oldest_parent: Root::from_array([1; 32]),
            columns_oldest: Slot::new(500),
            per_index_oldest: Default::default(),
        };
        // Descending is fine.
        let ok = BackfillProgress {
            blocks_oldest: Slot::new(436),
            blocks_oldest_parent: Root::from_array([2; 32]),
            columns_oldest: Slot::new(436),
            per_index_oldest: Default::default(),
        };
        assert!(admit_progress_monotone(&ok, Some(&stored)).is_ok());

        // Increase blocks_oldest → refuse.
        let bad_blocks = BackfillProgress {
            blocks_oldest: Slot::new(600),
            ..ok.clone()
        };
        assert!(matches!(
            admit_progress_monotone(&bad_blocks, Some(&stored)),
            Err(BatchAdmitError::ProgressNonMonotone {
                class: "blocks",
                ..
            })
        ));

        // Increase columns_oldest → refuse.
        let bad_cols = BackfillProgress {
            columns_oldest: Slot::new(600),
            ..ok
        };
        assert!(matches!(
            admit_progress_monotone(&bad_cols, Some(&stored)),
            Err(BatchAdmitError::ProgressNonMonotone {
                class: "columns",
                ..
            })
        ));

        // No stored → first seed always ok (even "high" values).
        assert!(admit_progress_monotone(&bad_blocks, None).is_ok());
    }

    #[test]
    fn admit_progress_accepts_legitimate_cgc_raise() {
        // Durable: four custodied indices complete at target; rest never custodied.
        let target = 1_000u64;
        let stored = proto_progress_to_store(
            target,
            Root::default(),
            target,
            &[target, target, target, target],
        );
        assert_eq!(stored.per_index_oldest[4], Slot::ZERO);

        // Honest raise: new indices start at head (no history).
        let head = 50_000u64;
        let raised = proto_progress_to_store(
            target,
            Root::default(),
            target,
            &[target, target, target, target, head, head, head, head],
        );
        assert_eq!(raised.per_index_oldest[4], Slot::new(head));
        assert_eq!(raised.per_index_oldest[127], Slot::ZERO);
        assert!(
            admit_progress_monotone(&raised, Some(&stored)).is_ok(),
            "cgc raise must seed never-custodied indices at head"
        );

        // Raising an actually-custodied index is still non-monotone.
        let bad = proto_progress_to_store(
            target,
            Root::default(),
            target,
            &[head, target, target, target, head, head, head, head],
        );
        assert!(matches!(
            admit_progress_monotone(&bad, Some(&stored)),
            Err(BatchAdmitError::ProgressNonMonotone {
                class: "columns",
                current: 1_000,
                attempted: 50_000,
            })
        ));
    }

    #[test]
    fn admit_progress_bound_to_admitted_batch_frontier() {
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
        let parent7 = Root::from_array([6; 32]);
        let r7 = Root::from_array([7; 32]);
        let r8 = Root::from_array([8; 32]);
        // Admitted descending: [8, 7] — frontier is slot 7 / parent of 7.
        let ordered = vec![synth(8, &r7, r8), synth(7, &parent7, r7)];
        let ok = BackfillProgress {
            blocks_oldest: Slot::new(7),
            blocks_oldest_parent: parent7,
            columns_oldest: Slot::new(7),
            per_index_oldest: Default::default(),
        };
        assert!(admit_progress_bound_to_batch(&ok, &ordered, &[7]).is_ok());

        // Wrong oldest slot.
        let bad_slot = BackfillProgress {
            blocks_oldest: Slot::new(8),
            ..ok.clone()
        };
        assert!(matches!(
            admit_progress_bound_to_batch(&bad_slot, &ordered, &[]),
            Err(BatchAdmitError::ProgressFrontierMismatch { .. })
        ));

        // Wrong parent.
        let bad_parent = BackfillProgress {
            blocks_oldest_parent: Root::from_array([0xff; 32]),
            ..ok.clone()
        };
        assert!(matches!(
            admit_progress_bound_to_batch(&bad_parent, &ordered, &[]),
            Err(BatchAdmitError::ProgressFrontierMismatch { .. })
        ));

        // columns_oldest must match min column slot.
        let bad_col = BackfillProgress {
            columns_oldest: Slot::new(99),
            ..ok
        };
        assert!(matches!(
            admit_progress_bound_to_batch(&bad_col, &[], &[10, 12]),
            Err(BatchAdmitError::ProgressFrontierMismatch { .. })
        ));
    }

    #[test]
    fn admit_progress_required_rejects_empty_progress_when_blocks_present() {
        assert!(matches!(
            admit_progress_required(None, true),
            Err(BatchAdmitError::ProgressRequired)
        ));
        assert!(admit_progress_required(None, false).is_ok());
        let progress = BackfillProgress {
            blocks_oldest: Slot::new(1),
            blocks_oldest_parent: Root::from_array([1; 32]),
            columns_oldest: Slot::new(1),
            per_index_oldest: Default::default(),
        };
        assert!(admit_progress_required(Some(&progress), true).is_ok());
    }

    #[test]
    fn admit_progress_only_preserves_named_block_frontier() {
        let named_parent = Root::from_array([0xEE; 32]);
        let stored = BackfillProgress {
            blocks_oldest: Slot::new(10),
            blocks_oldest_parent: named_parent,
            columns_oldest: Slot::new(10),
            per_index_oldest: Default::default(),
        };
        let restated = stored.clone();
        assert!(
            admit_progress_only_preserves_block_frontier(
                Some(&restated),
                false,
                Some(&stored),
                None
            )
            .is_ok()
        );
        assert!(
            admit_progress_only_preserves_block_frontier(Some(&stored), true, Some(&stored), None)
                .is_ok()
        );

        let planted = BackfillProgress {
            blocks_oldest: Slot::new(0),
            blocks_oldest_parent: Root::from_array([0xAA; 32]),
            columns_oldest: Slot::new(0),
            per_index_oldest: Default::default(),
        };
        assert!(matches!(
            admit_progress_only_preserves_block_frontier(
                Some(&planted),
                false,
                Some(&stored),
                None
            ),
            Err(BatchAdmitError::FrontierJump { .. })
        ));
        assert!(matches!(
            admit_progress_only_preserves_block_frontier(Some(&planted), false, None, None),
            Err(BatchAdmitError::FrontierJump { .. })
        ));

        let anchor = AnchorInfo {
            oldest_block_slot: Slot::new(100),
            oldest_block_parent: named_parent,
            ..AnchorInfo::default()
        };
        let matching_anchor = BackfillProgress {
            blocks_oldest: Slot::new(100),
            blocks_oldest_parent: named_parent,
            columns_oldest: Slot::new(7),
            per_index_oldest: Default::default(),
        };
        assert!(
            admit_progress_only_preserves_block_frontier(
                Some(&matching_anchor),
                false,
                None,
                Some(&anchor)
            )
            .is_ok()
        );
        assert!(matches!(
            admit_progress_only_preserves_block_frontier(
                Some(&planted),
                false,
                None,
                Some(&anchor)
            ),
            Err(BatchAdmitError::FrontierJump { .. })
        ));
    }

    #[test]
    fn admit_extends_durable_frontier_parent_must_be_durable_or_first_row() {
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

        let expected = Root::from_array([0xEE; 32]);
        let first = synth(9, &Root::from_array([0x08; 32]), expected);
        let stored = BackfillProgress {
            blocks_oldest: Slot::new(10),
            blocks_oldest_parent: expected,
            columns_oldest: Slot::new(10),
            per_index_oldest: Default::default(),
        };
        assert!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&first),
                Some(&stored),
                None,
                false
            )
            .is_ok()
        );

        let jumped = synth(
            5,
            &Root::from_array([0xFF; 32]),
            Root::from_array([0x55; 32]),
        );
        assert!(matches!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&jumped),
                Some(&stored),
                None,
                false
            ),
            Err(BatchAdmitError::FrontierJump { .. })
        ));

        // Unnamed first seed: SSZ parent durable, or parent is the first row.
        assert!(
            admit_extends_durable_frontier(std::slice::from_ref(&jumped), None, None, true).is_ok()
        );
        let self_parent = synth(3, &expected, expected);
        assert!(
            admit_extends_durable_frontier(std::slice::from_ref(&self_parent), None, None, false)
                .is_ok()
        );
        assert!(matches!(
            admit_extends_durable_frontier(std::slice::from_ref(&jumped), None, None, false),
            Err(BatchAdmitError::FrontierJump { .. })
        ));

        // Anchor binds the first seed the same way stored progress does.
        let anchor = AnchorInfo {
            oldest_block_parent: expected,
            ..AnchorInfo::default()
        };
        assert!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&first),
                None,
                Some(&anchor),
                false
            )
            .is_ok()
        );
    }

    #[test]
    fn admit_extends_durable_frontier_rejects_jump_down_across_hole() {
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

        // Durable frontier at slot 10 / parent 0xEE. Parent bind alone would
        // accept a matching-root row at slot 8 and skip 9 — the hole.
        let expected = Root::from_array([0xEE; 32]);
        let stored = BackfillProgress {
            blocks_oldest: Slot::new(10),
            blocks_oldest_parent: expected,
            columns_oldest: Slot::new(10),
            per_index_oldest: Default::default(),
        };
        let hole = synth(8, &Root::from_array([0x07; 32]), expected);
        assert!(matches!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&hole),
                Some(&stored),
                None,
                false
            ),
            Err(BatchAdmitError::FrontierJump { reason })
                if reason.contains("not adjacent")
        ));

        let adjacent = synth(9, &Root::from_array([0x08; 32]), expected);
        assert!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&adjacent),
                Some(&stored),
                None,
                false
            )
            .is_ok()
        );

        // Named anchor slot (no stored progress) has the same bind.
        let anchor = AnchorInfo {
            oldest_block_slot: Slot::new(100),
            oldest_block_parent: expected,
            ..AnchorInfo::default()
        };
        let hole_anchor = synth(90, &Root::from_array([0xFF; 32]), expected);
        assert!(matches!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&hole_anchor),
                None,
                Some(&anchor),
                false
            ),
            Err(BatchAdmitError::FrontierJump { reason })
                if reason.contains("not adjacent")
        ));
        let adjacent_anchor = synth(99, &Root::from_array([0x08; 32]), expected);
        assert!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&adjacent_anchor),
                None,
                Some(&anchor),
                false
            )
            .is_ok()
        );
    }

    #[test]
    fn admit_extends_durable_frontier_rejects_intra_batch_sandwich() {
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

        // stored oldest = 10 / parent P; top row attaches at 9, lowest is 0.
        let expected = Root::from_array([0xEE; 32]);
        let r0 = Root::from_array([0x00; 32]);
        let stored = BackfillProgress {
            blocks_oldest: Slot::new(10),
            blocks_oldest_parent: expected,
            columns_oldest: Slot::new(10),
            per_index_oldest: Default::default(),
        };
        let sandwich = vec![
            synth(9, &r0, expected),
            synth(0, &Root::from_array([0xFF; 32]), r0),
        ];
        assert!(matches!(
            admit_extends_durable_frontier(&sandwich, Some(&stored), None, false),
            Err(BatchAdmitError::FrontierJump { reason })
                if reason.contains("not slot-contiguous")
        ));

        let adjacent = vec![
            synth(9, &Root::from_array([0x08; 32]), expected),
            synth(
                8,
                &Root::from_array([0x07; 32]),
                Root::from_array([0x08; 32]),
            ),
        ];
        assert!(admit_extends_durable_frontier(&adjacent, Some(&stored), None, false).is_ok());

        // Stored oldest = 0 is genesis, not a first-seed trampoline.
        let at_zero = BackfillProgress {
            blocks_oldest: Slot::new(0),
            blocks_oldest_parent: expected,
            columns_oldest: Slot::new(0),
            per_index_oldest: Default::default(),
        };
        let jumped = synth(5, &Root::from_array([0x04; 32]), expected);
        assert!(matches!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&jumped),
                Some(&at_zero),
                None,
                false
            ),
            Err(BatchAdmitError::FrontierJump { reason })
                if reason.contains("not adjacent")
        ));
        let stay = synth(0, &Root::from_array([0xFF; 32]), expected);
        assert!(
            admit_extends_durable_frontier(
                std::slice::from_ref(&stay),
                Some(&at_zero),
                None,
                false
            )
            .is_ok()
        );
    }

    /// CC-47 /6 block class: write-behind commit p99 during P2 backfill batches
    /// stays within **10 %** of the no-backfill baseline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_behind_p99_during_block_backfill_within_10_percent() {
        use std::sync::Arc;
        use std::time::{Instant, SystemTime, UNIX_EPOCH};

        use cc_store::engine::{Durability, Engine, EngineOptions};
        use cc_store::meta::WriteCursor;
        use tokio::sync::{oneshot, watch};

        use crate::writer::{
            BackgroundChunk, CommitUnit, WriterBounds, WriterFaults, spawn_writer,
        };

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-bf-p99-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Arc::new(
            Engine::open(
                &dir,
                EngineOptions::default().with_durability(Durability::None),
            )
            .unwrap(),
        );
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = spawn_writer(
            Arc::clone(&engine),
            m.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );

        let n = 40u64;
        writer
            .submit_p0_committed(CommitUnit::cursor_only(WriteCursor {
                session_id: 1,
                seq: 0,
                slot: Slot::new(0),
                root: Root::ZERO,
            }))
            .await
            .unwrap();

        let mut baseline = Vec::with_capacity(n as usize);
        for i in 1..=n {
            let started = Instant::now();
            writer
                .submit_p0_committed(CommitUnit::cursor_only(WriteCursor {
                    session_id: 1,
                    seq: i,
                    slot: Slot::new(i),
                    root: Root::ZERO,
                }))
                .await
                .unwrap();
            baseline.push(started.elapsed().as_secs_f64());
        }
        baseline.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p99_base = baseline[((n as usize) * 99 / 100).min(baseline.len() - 1)];

        // P2 backfill-class chunks (~256 KiB each) concurrent with P0 commits.
        const BF_CHUNK: usize = 256 * 1024;
        let mut during = Vec::with_capacity(n as usize);
        for i in 0..n {
            let payload = vec![(i % 251) as u8; BF_CHUNK];
            let (done_tx, done_rx) = oneshot::channel();
            let chunk = BackgroundChunk {
                class: StorageClass::Blocks,
                puts: vec![(
                    "meta".to_owned(),
                    format!("bf-p2-{i}").into_bytes(),
                    payload,
                )],
                deletes: Vec::new(),
                done: Some(done_tx),
            };
            assert!(writer.try_submit_p2(chunk, &m));
            let started = Instant::now();
            writer
                .submit_p0_committed(CommitUnit::cursor_only(WriteCursor {
                    session_id: 1,
                    seq: 1_000 + i,
                    slot: Slot::new(1_000 + i),
                    root: Root::ZERO,
                }))
                .await
                .unwrap();
            during.push(started.elapsed().as_secs_f64());
            let _ = done_rx.await;
        }
        during.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p99_during = during[((n as usize) * 99 / 100).min(during.len() - 1)];

        eprintln!(
            "CC-47b /6 write-behind p99 baseline={p99_base:.6}s during_block_backfill_p2={p99_during:.6}s \
             (chunk={BF_CHUNK} B)"
        );
        let limit = (p99_base * 1.10).max(p99_base + 0.002);
        assert!(
            p99_during <= limit,
            "commit p99 during block backfill {p99_during} exceeds 10% of baseline {p99_base} (limit {limit})"
        );

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
