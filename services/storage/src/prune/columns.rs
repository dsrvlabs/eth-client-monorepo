//! Column prune pass watermark (Architecture §7.0 / CC-46a).
//!
//! Watermark (exclusive upper bound of the deleted range):
//! ```text
//! start_slot(max(current_epoch − columns_retention, FULU_FORK_EPOCH))
//!   − prune_margin_epochs × SLOTS_PER_EPOCH
//! ```
//!
//! Spec retention is `MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS = 4_096`
//! (Fulu networking config). Compressed-retention venues override via
//! `storage.retention_override.columns_epochs` (CC-4D).

use cc_store::keys::{
    cold_column_slot_range, column_shard_id, columns_shard_table, decode_column_slot_by_root_value,
    decode_hot_column_key, encode_column_slot_by_root_key, encode_hot_column_key, SLOTS_PER_EPOCH,
};
use cc_store::{
    epoch_start_slot, Engine, Root, Slot, StoreError, TABLE_COLUMNS_HOT, TABLE_COLUMN_SLOT_BY_ROOT,
};

use super::PrunePlan;

/// Spec default: `MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS` (Fulu).
pub(crate) const DEFAULT_COLUMNS_RETENTION_EPOCHS: u64 = 4_096;

/// Compute the exclusive column prune mark for wall-clock `current_epoch`.
///
/// Newest slot deleted = `mark − 1` =
/// `start_slot(current_epoch − retention) − margin × SPE − 1` when the Fulu
/// floor does not bind (CC-46 /2 boundary assertion).
#[must_use]
pub(crate) fn columns_prune_mark(
    current_epoch: u64,
    columns_retention_epochs: u64,
    fulu_fork_epoch: u64,
    margin_epochs: u64,
) -> Slot {
    let retention = columns_retention_epochs.max(1);
    let floor_epoch = current_epoch
        .saturating_sub(retention)
        .max(fulu_fork_epoch);
    let floor_slot = epoch_start_slot(floor_epoch);
    let margin_slots = margin_epochs.saturating_mul(SLOTS_PER_EPOCH);
    floor_slot.saturating_sub(margin_slots)
}

/// Newest slot a columns pass deletes for the given mark (mark is exclusive).
#[must_use]
pub(crate) fn newest_deleted_slot(mark: Slot) -> Option<Slot> {
    mark.checked_sub(1)
}

/// Stage cold + hot column deletes for slots in `[from, to)` plus reverse-index rows.
///
/// Does **not** drop whole shards (`drop_table` is CC-46b §7.5). Keys are staged
/// as discrete deletes for the single-writer P2 path.
pub(crate) fn plan_column_deletes(
    engine: &Engine,
    from: Slot,
    to: Slot,
) -> Result<PrunePlan, StoreError> {
    if to.as_u64() <= from.as_u64() {
        return Ok(PrunePlan::default());
    }
    let rt = engine.read()?;
    let mut plan = PrunePlan::default();

    // Cold sharded tables that intersect [from, to).
    let start_shard = column_shard_id(from);
    let end_shard = column_shard_id(Slot::new(to.as_u64().saturating_sub(1)));
    for shard in start_shard..=end_shard {
        let table = columns_shard_table(shard);
        let (lo, hi) = cold_column_slot_range(from, to);
        let iter = match rt.range(&table, &lo, &hi) {
            Ok(it) => it,
            Err(_) => continue,
        };
        for item in iter {
            let (key, value) = item?;
            plan.bytes = plan.bytes.saturating_add(key.len() as u64 + value.len() as u64);
            plan.rows = plan.rows.saturating_add(1);
            plan.deletes.push((table.clone(), key));
        }
    }

    // Hot columns in the same slot span.
    let lo = encode_hot_column_key(from, &Root::ZERO, 0);
    let hi = encode_hot_column_key(to, &Root::ZERO, 0);
    if let Ok(iter) = rt.range(TABLE_COLUMNS_HOT, &lo, &hi) {
        for item in iter {
            let (key, value) = item?;
            plan.bytes = plan.bytes.saturating_add(key.len() as u64 + value.len() as u64);
            plan.rows = plan.rows.saturating_add(1);
            if let Some((_slot, root, index)) = decode_hot_column_key(&key) {
                let idx_key = encode_column_slot_by_root_key(&root, index);
                plan.deletes
                    .push((TABLE_COLUMN_SLOT_BY_ROOT.to_owned(), idx_key.to_vec()));
            }
            plan.deletes.push((TABLE_COLUMNS_HOT.to_owned(), key));
        }
    }

    // Reverse-index sweep whose value slot falls in [from, to).
    let idx_lo = [0u8; 34];
    let idx_hi = [0xffu8; 34];
    if let Ok(iter) = rt.range(TABLE_COLUMN_SLOT_BY_ROOT, &idx_lo, &idx_hi) {
        for item in iter {
            let (key, value) = item?;
            let Some(slot) = decode_column_slot_by_root_value(&value) else {
                continue;
            };
            if slot.as_u64() >= from.as_u64()
                && slot.as_u64() < to.as_u64()
                && !plan
                    .deletes
                    .iter()
                    .any(|(t, k)| t == TABLE_COLUMN_SLOT_BY_ROOT && k.as_slice() == key.as_slice())
            {
                plan.deletes
                    .push((TABLE_COLUMN_SLOT_BY_ROOT.to_owned(), key));
                plan.rows = plan.rows.saturating_add(1);
            }
        }
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    /// CC-46 /2 — columns boundary ±1 slot (Fulu floor unbound).
    #[test]
    fn column_mark_boundary_plus_minus_one() {
        let current = 5_000u64;
        let retention = 4_096u64;
        let margin = 1u64;
        let mark = columns_prune_mark(current, retention, 0, margin);
        let expected_newest = epoch_start_slot(current - retention)
            .as_u64()
            .saturating_sub(SLOTS_PER_EPOCH)
            .saturating_sub(1);
        assert_eq!(newest_deleted_slot(mark).unwrap().as_u64(), expected_newest);
        assert_eq!(mark.as_u64(), expected_newest + 1);
        assert!(expected_newest < mark.as_u64());
        assert_eq!(mark.as_u64() - 1, expected_newest);
    }

    #[test]
    fn fulu_fork_floor_binds() {
        let mark = columns_prune_mark(100, 4_096, 50, 1);
        let expected = epoch_start_slot(50)
            .as_u64()
            .saturating_sub(SLOTS_PER_EPOCH);
        assert_eq!(mark.as_u64(), expected);
    }
}
