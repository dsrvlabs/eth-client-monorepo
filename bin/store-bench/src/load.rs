//! Write-behind load + per-epoch prune for the CC-40 falsifier (§8.2).

use std::time::Instant;

use anyhow::Result;
use cc_store::keys::{
    BLOCK_SHARD_EPOCHS, COLUMN_SHARD_EPOCHS, SLOTS_PER_EPOCH, blocks_shard_table,
    cold_block_epoch_range, cold_column_epoch_range, columns_shard_table, encode_flat_column_key,
};
use cc_store::{Engine, Slot, StoreError};

use crate::measure::LatencyHist;
use crate::synth::{WRITE_BEHIND_BYTES, value_pattern};
use crate::{BenchConfig, Layout};

/// Run 64 (or configured) prune-under-load epochs.
///
/// Each simulated epoch:
/// 1. For each of 32 slots: one write-behind commit (~400 KB).
/// 2. Once per epoch: delete the oldest epoch's columns + blocks
///    (flat: `delete_range`; sharded: range-delete inside the shard table, or
///    `drop_table` when the retired epoch completes a shard).
///
/// Returns prune-commit latencies (commits that included a delete) and final
/// theoretical live-set bytes.
pub(crate) fn run_prune_under_load(
    eng: &Engine,
    cfg: &BenchConfig,
    mut live_bytes: u64,
) -> Result<LoadResult> {
    let mut hist = LatencyHist::new();
    let wb = value_pattern(WRITE_BEHIND_BYTES, 99);
    let mut oldest_epoch: u64 = 0;
    // Head advances one epoch per outer loop iteration.
    let mut head_epoch = cfg.retention_epochs;

    for cycle in 0..cfg.prune_epochs {
        // Write-behind: one txn per slot for the new epoch.
        let epoch_start_slot = head_epoch * SLOTS_PER_EPOCH;
        for s in 0..SLOTS_PER_EPOCH {
            let slot_u = epoch_start_slot + s;
            let mut batch = eng.batch();
            // Opaque write-behind blob under a dedicated table (not pruned by column/block ranges).
            let key = slot_u.to_be_bytes();
            batch.put("write_behind", &key, &wb);
            let t0 = Instant::now();
            eng.commit(batch)
                .map_err(|e: StoreError| anyhow::anyhow!(e))?;
            let dt = t0.elapsed();
            hist.record(dt);
            live_bytes = live_bytes.saturating_add(WRITE_BEHIND_BYTES as u64);
        }

        // Prune oldest epoch (columns + blocks).
        let prune_t0 = Instant::now();
        let pruned = prune_one_epoch(eng, cfg.layout, oldest_epoch, cfg.cgc)?;
        let prune_dt = prune_t0.elapsed();
        hist.record_prune(prune_dt);
        live_bytes = live_bytes.saturating_sub(pruned);

        oldest_epoch = oldest_epoch.saturating_add(1);
        head_epoch = head_epoch.saturating_add(1);

        if cycle % 8 == 0 || cycle + 1 == cfg.prune_epochs {
            eprintln!(
                "  cycle {}/{} oldest_epoch={} file={} live≈{} prune_ms={:.2}",
                cycle + 1,
                cfg.prune_epochs,
                oldest_epoch,
                eng.file_len().unwrap_or(0),
                live_bytes,
                prune_dt.as_secs_f64() * 1000.0
            );
        }
    }

    Ok(LoadResult { hist, live_bytes })
}

fn prune_one_epoch(eng: &Engine, layout: Layout, epoch: u64, cgc: u16) -> Result<u64> {
    use crate::synth::{BLOCK_VALUE_BYTES, MEAN_VALUE_BYTES};

    let col_bytes = SLOTS_PER_EPOCH
        .saturating_mul(u64::from(cgc))
        .saturating_mul(MEAN_VALUE_BYTES as u64);
    let blk_bytes = SLOTS_PER_EPOCH.saturating_mul(BLOCK_VALUE_BYTES as u64);
    let pruned = col_bytes.saturating_add(blk_bytes);

    match layout {
        Layout::Flat => {
            let mut batch = eng.batch();
            // Flat columns use shard-prefix keys; delete the epoch's slot span across all shards
            // by scanning the epoch's slot range without shard prefix discrimination via
            // per-slot deletes... Better: delete by reconstructing keys for this epoch.
            // encode_flat_column_key includes shard; for an epoch all slots share one column shard.
            let shard = (epoch / COLUMN_SHARD_EPOCHS) as u16;
            let start_slot = epoch * SLOTS_PER_EPOCH;
            let end_slot = (epoch + 1) * SLOTS_PER_EPOCH;
            let lo = encode_flat_column_key(shard, Slot::new(start_slot), 0);
            // hi = first key of end_slot at index 0
            let hi = encode_flat_column_key(shard, Slot::new(end_slot), 0);
            batch.delete_range("columns", &lo, &hi);
            let (blo, bhi) = cold_block_epoch_range(epoch);
            batch.delete_range("blocks", &blo, &bhi);
            eng.commit(batch)
                .map_err(|e: StoreError| anyhow::anyhow!(e))?;
        }
        Layout::Sharded => {
            let col_shard = epoch / COLUMN_SHARD_EPOCHS;
            let col_table = columns_shard_table(col_shard);
            // If this epoch is the last in its column shard, drop the whole table
            // (the falsifier's sharding payoff). Otherwise range-delete the epoch.
            let last_in_col_shard = (epoch + 1).is_multiple_of(COLUMN_SHARD_EPOCHS);
            if last_in_col_shard {
                // Drop only after deleting? At boundary the whole shard is retired:
                // epochs [col_shard*32, (col_shard+1)*32) are all gone when epoch == last.
                // We prune one epoch at a time, so on the last epoch of the shard drop.
                eng.drop_table(&col_table)
                    .map_err(|e: StoreError| anyhow::anyhow!(e))?;
            } else {
                let mut batch = eng.batch();
                let (lo, hi) = cold_column_epoch_range(epoch);
                batch.delete_range(&col_table, &lo, &hi);
                eng.commit(batch)
                    .map_err(|e: StoreError| anyhow::anyhow!(e))?;
            }

            let blk_shard = epoch / BLOCK_SHARD_EPOCHS;
            let blk_table = blocks_shard_table(blk_shard);
            let last_in_blk_shard = (epoch + 1).is_multiple_of(BLOCK_SHARD_EPOCHS);
            if last_in_blk_shard {
                eng.drop_table(&blk_table)
                    .map_err(|e: StoreError| anyhow::anyhow!(e))?;
            } else {
                let mut batch = eng.batch();
                let (lo, hi) = cold_block_epoch_range(epoch);
                batch.delete_range(&blk_table, &lo, &hi);
                eng.commit(batch)
                    .map_err(|e: StoreError| anyhow::anyhow!(e))?;
            }
        }
    }

    Ok(pruned)
}

#[derive(Debug)]
pub(crate) struct LoadResult {
    pub hist: LatencyHist,
    pub live_bytes: u64,
}
