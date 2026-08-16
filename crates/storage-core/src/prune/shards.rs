//! Shard retirement via `drop_table` (Architecture §7.5 / CC-46b / ADR P4-10).
//!
//! With shard width equal to prune cadence, **exactly one shard becomes
//! retirable per tick** and retiring it is one `drop_table` — metadata work
//! rather than per-key B-tree rebalancing.
//!
//! The one-epoch margin composes cleanly: a shard is dropped once its newest
//! slot is below `watermark − 1 epoch` (the exclusive prune mark already
//! subtracts `prune_margin_epochs`, so a shard whose exclusive end is `≤ mark`
//! is fully below the margined watermark). At a 32-epoch shard and a 32-epoch
//! tick this is a one-tick lag and nothing more.
//!
//! ## Two effects, reported separately (§7.5)
//!
//! - **Delete latency** (falsifier #1): `drop_table` replaces ~8 200 B-tree
//!   deletions with one metadata op — the lever against redb's ~3.9× removal
//!   cost vs fjall.
//! - **File growth** (falsifier #2): whether freed pages return to the
//!   allocator is an **engine property**, unaffected by how the delete was
//!   expressed. Measured by `cc_storage_disk_bytes / cc_storage_live_set_bytes`,
//!   not by this module.
//!
//! If the engine collapsed shards to key prefixes (`R-14`), `drop_table`
//! degrades to `delete_range` and the chunk loop carries the whole pass —
//! nothing else changes.

use cc_store::keys::{
    BLOCK_SHARD_EPOCHS, COLUMN_SHARD_EPOCHS, SLOTS_PER_EPOCH, block_shard_id,
    block_shard_start_slot, blocks_shard_table, column_shard_id, column_shard_start_slot,
    columns_shard_table,
};
use cc_store::{Engine, Slot, StoreError};
use tracing::{info, warn};

use crate::metrics::{ClassLabels, StorageClass, StorageMetrics};

/// Exclusive end slot of column shard `id` (first slot of the next shard).
#[must_use]
pub(crate) fn column_shard_end_slot(shard_id: u64) -> Slot {
    column_shard_start_slot(shard_id.saturating_add(1))
}

/// Exclusive end slot of block shard `id`.
#[must_use]
pub(crate) fn block_shard_end_slot(shard_id: u64) -> Slot {
    block_shard_start_slot(shard_id.saturating_add(1))
}

/// Newest slot stored in column shard `id`.
#[must_use]
pub(crate) fn column_shard_newest_slot(shard_id: u64) -> Slot {
    Slot::new(column_shard_end_slot(shard_id).as_u64().saturating_sub(1))
}

/// Newest slot stored in block shard `id`.
#[must_use]
pub(crate) fn block_shard_newest_slot(shard_id: u64) -> Slot {
    Slot::new(block_shard_end_slot(shard_id).as_u64().saturating_sub(1))
}

/// Whether column shard `id` is fully covered by exclusive prune mark `mark`.
///
/// Newest slot of the shard must be `< mark` (i.e. exclusive end `≤ mark`).
/// Equivalently: newest is below the margined watermark.
#[must_use]
pub(crate) fn column_shard_retirable(shard_id: u64, mark: Slot) -> bool {
    column_shard_end_slot(shard_id).as_u64() <= mark.as_u64()
}

/// Whether block shard `id` is fully covered by exclusive prune mark `mark`.
#[must_use]
pub(crate) fn block_shard_retirable(shard_id: u64, mark: Slot) -> bool {
    block_shard_end_slot(shard_id).as_u64() <= mark.as_u64()
}

/// Lowest column-shard id that intersects `[from, mark)` and is fully retirable.
///
/// At most one id is returned per call — one `drop_table` per tick (§7.5).
#[must_use]
pub(crate) fn next_retirable_column_shard(from: Slot, mark: Slot) -> Option<u64> {
    if mark.as_u64() <= from.as_u64() {
        return None;
    }
    // First shard that may still hold keys ≥ from.
    let mut id = column_shard_id(from);
    // Walk until we pass the mark's shard.
    let last_candidate = column_shard_id(Slot::new(mark.as_u64().saturating_sub(1)));
    while id <= last_candidate {
        if column_shard_retirable(id, mark) {
            // Prefer shards that still intersect the work range.
            let end = column_shard_end_slot(id).as_u64();
            let start = column_shard_start_slot(id).as_u64();
            if end > from.as_u64() && start < mark.as_u64() {
                return Some(id);
            }
        }
        id = id.saturating_add(1);
        if id == 0 {
            break; // overflow guard
        }
    }
    None
}

/// Lowest block-shard id that intersects `[from, mark)` and is fully retirable.
#[must_use]
pub(crate) fn next_retirable_block_shard(from: Slot, mark: Slot) -> Option<u64> {
    if mark.as_u64() <= from.as_u64() {
        return None;
    }
    let mut id = block_shard_id(from);
    let last_candidate = block_shard_id(Slot::new(mark.as_u64().saturating_sub(1)));
    while id <= last_candidate {
        if block_shard_retirable(id, mark) {
            let end = block_shard_end_slot(id).as_u64();
            let start = block_shard_start_slot(id).as_u64();
            if end > from.as_u64() && start < mark.as_u64() {
                return Some(id);
            }
        }
        id = id.saturating_add(1);
        if id == 0 {
            break;
        }
    }
    None
}

/// Drop one retirable column shard if present. Returns `true` when a table was dropped.
///
/// Uses `Engine::drop_table` directly (metadata op). The single-writer task owns
/// ordinary put/delete commits; table drop is a separate redb write that the
/// engine mutex serialises against the writer (no `writer.rs` change — **D-4**).
pub(crate) fn retire_one_column_shard(
    engine: &Engine,
    metrics: &StorageMetrics,
    from: Slot,
    mark: Slot,
) -> Result<bool, StoreError> {
    let Some(id) = next_retirable_column_shard(from, mark) else {
        return Ok(false);
    };
    let name = columns_shard_table(id);
    let names = engine.table_names()?;
    if !names.iter().any(|n| n == &name) {
        return Ok(false);
    }
    let newest = column_shard_newest_slot(id);
    // Compose with margin: newest must be below mark (already required by retirable).
    debug_assert!(newest.as_u64() < mark.as_u64());
    match engine.drop_table(&name) {
        Ok(()) => {
            metrics
                .shard_dropped
                .get_or_create(&ClassLabels {
                    class: StorageClass::Columns.as_str().to_owned(),
                })
                .inc();
            info!(
                target: "cc_storage::prune",
                shard = id,
                table = %name,
                newest = newest.as_u64(),
                mark = mark.as_u64(),
                "column shard retired via drop_table (§7.5 delete-latency lever; \
                 file growth remains an engine property)"
            );
            Ok(true)
        }
        Err(e) => {
            warn!(
                target: "cc_storage::prune",
                shard = id,
                table = %name,
                error = %e,
                "column shard drop_table failed"
            );
            Err(e)
        }
    }
}

/// Drop one retirable block shard if present.
pub(crate) fn retire_one_block_shard(
    engine: &Engine,
    metrics: &StorageMetrics,
    from: Slot,
    mark: Slot,
) -> Result<bool, StoreError> {
    let Some(id) = next_retirable_block_shard(from, mark) else {
        return Ok(false);
    };
    let name = blocks_shard_table(id);
    let names = engine.table_names()?;
    if !names.iter().any(|n| n == &name) {
        return Ok(false);
    }
    let newest = block_shard_newest_slot(id);
    debug_assert!(newest.as_u64() < mark.as_u64());
    match engine.drop_table(&name) {
        Ok(()) => {
            metrics
                .shard_dropped
                .get_or_create(&ClassLabels {
                    class: StorageClass::Blocks.as_str().to_owned(),
                })
                .inc();
            info!(
                target: "cc_storage::prune",
                shard = id,
                table = %name,
                newest = newest.as_u64(),
                mark = mark.as_u64(),
                "block shard retired via drop_table (§7.5)"
            );
            Ok(true)
        }
        Err(e) => {
            warn!(
                target: "cc_storage::prune",
                shard = id,
                table = %name,
                error = %e,
                "block shard drop_table failed"
            );
            Err(e)
        }
    }
}

/// Document shard widths (equals prune cadences — ADR P4-10).
#[allow(dead_code)]
pub(crate) const COLUMN_SHARD_WIDTH_EPOCHS: u64 = COLUMN_SHARD_EPOCHS;
#[allow(dead_code)]
pub(crate) const BLOCK_SHARD_WIDTH_EPOCHS: u64 = BLOCK_SHARD_EPOCHS;
#[allow(dead_code)]
pub(crate) const SLOTS_PER_COLUMN_SHARD: u64 = COLUMN_SHARD_EPOCHS * SLOTS_PER_EPOCH;
#[allow(dead_code)]
pub(crate) const SLOTS_PER_BLOCK_SHARD: u64 = BLOCK_SHARD_EPOCHS * SLOTS_PER_EPOCH;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::{columns_shard_table, encode_cold_column_key};
    use prometheus_client::registry::Registry;

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn tmp_engine() -> Engine {
        let dir = crate::test_tmpdir::unique_temp_dir("cc-storage-prune-shards");
        std::fs::create_dir_all(&dir).unwrap();
        Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap()
    }

    #[test]
    fn retirable_when_end_at_or_below_mark() {
        // Shard 0: slots [0, 1024). Retirable at mark=1024.
        assert!(column_shard_retirable(0, Slot::new(1024)));
        assert!(!column_shard_retirable(0, Slot::new(1023)));
        assert_eq!(column_shard_newest_slot(0).as_u64(), 1023);
    }

    #[test]
    fn next_retirable_one_per_call() {
        // from=0, mark covers shards 0 and 1 fully (2048).
        let a = next_retirable_column_shard(Slot::new(0), Slot::new(2048));
        assert_eq!(a, Some(0));
        // After "retiring" 0, caller advances from past shard 0.
        let b = next_retirable_column_shard(Slot::new(1024), Slot::new(2048));
        assert_eq!(b, Some(1));
    }

    #[test]
    fn drop_table_increments_metric_and_composes_with_margin() {
        let eng = tmp_engine();
        let m = metrics();
        // Create columns_00000 with one key.
        let table = columns_shard_table(0);
        {
            let mut b = eng.batch();
            let key = encode_cold_column_key(Slot::new(0), 0);
            b.put(&table, &key, b"sidecar");
            eng.commit(b).unwrap();
        }
        assert!(eng.table_names().unwrap().iter().any(|n| n == &table));

        // mark = 1024 retires shard 0; newest (1023) < mark.
        let dropped = retire_one_column_shard(&eng, &m, Slot::new(0), Slot::new(1024)).unwrap();
        assert!(dropped);
        let count = m
            .shard_dropped
            .get_or_create(&ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            })
            .get();
        assert_eq!(count, 1);
        // Table gone.
        assert!(!eng.table_names().unwrap().iter().any(|n| n == &table));
        // Second call: no further retirable present → 0.
        let dropped2 = retire_one_column_shard(&eng, &m, Slot::new(0), Slot::new(1024)).unwrap();
        assert!(!dropped2);
        assert_eq!(
            m.shard_dropped
                .get_or_create(&ClassLabels {
                    class: StorageClass::Columns.as_str().to_owned(),
                })
                .get(),
            1
        );
    }

    #[test]
    fn margin_composition_newest_below_mark_minus_zero() {
        // With mark already margined, newest of a retirable shard is always < mark.
        let mark = Slot::new(1024);
        let newest = column_shard_newest_slot(0);
        assert!(newest.as_u64() < mark.as_u64());
        // "below watermark − 1 epoch" when watermark is the un-margined floor
        // at mark + 32: newest < floor − 32 ≡ newest < mark.
        let floor = mark.as_u64() + SLOTS_PER_EPOCH;
        assert!(newest.as_u64() < floor - SLOTS_PER_EPOCH);
    }
}
