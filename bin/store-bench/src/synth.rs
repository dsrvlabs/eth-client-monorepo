//! Synthetic store builder for the CC-40 falsifier (§8.2).

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use cc_store::keys::{
    BLOCK_SHARD_EPOCHS, COLUMN_SHARD_EPOCHS, SLOTS_PER_EPOCH, blocks_shard_table,
    columns_shard_table, encode_cold_block_key, encode_cold_column_key, encode_flat_column_key,
};
use cc_store::{Durability, Engine, EngineOptions, Slot, StoreError};

use crate::{BenchConfig, Layout};

/// Mean synthetic sidecar size (~30 KB per Architecture §8.2).
pub(crate) const MEAN_VALUE_BYTES: usize = 30_000;

/// Approximate block body size for the falsifier's block keys.
pub(crate) const BLOCK_VALUE_BYTES: usize = 24_000;

/// Write-behind payload per slot during the prune phase (~400 KB).
pub(crate) const WRITE_BEHIND_BYTES: usize = 400 * 1024;

/// Custody group count at full window (`cgc = 8`).
pub(crate) const DEFAULT_CGC: u16 = 8;

/// Full column retention window in epochs at `cgc = 8`.
pub(crate) const FULL_RETENTION_EPOCHS: u64 = 4096;

pub(crate) fn value_pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x = seed ^ 0xC0FFEE_u64;
    for (i, b) in v.iter_mut().enumerate() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = (x as u8).wrapping_add(i as u8);
    }
    v
}

pub(crate) fn open_engine(dir: &Path, durability: Durability) -> Result<Engine> {
    Engine::open(dir, EngineOptions::default().with_durability(durability))
        .map_err(|e| anyhow::anyhow!(e))
}

/// Build the synthetic column (+ light block) store for one layout.
pub(crate) fn build_store(eng: &Engine, cfg: &BenchConfig) -> Result<BuildStats> {
    let t0 = Instant::now();
    let cgc = cfg.cgc;
    let epochs = cfg.retention_epochs;
    let slots = epochs.saturating_mul(SLOTS_PER_EPOCH);
    let mut puts = 0u64;
    let mut live_bytes = 0u64;

    let col_val = value_pattern(MEAN_VALUE_BYTES, 1);
    let blk_val = value_pattern(BLOCK_VALUE_BYTES, 2);

    // Batch aggressively for the initial fill; production path is 1-txn/slot.
    const BATCH_KEYS: usize = 2048;
    let mut batch = eng.batch();
    let mut batch_n = 0usize;

    let flush = |eng: &Engine, batch: &mut cc_store::Batch, batch_n: &mut usize| -> Result<()> {
        if *batch_n > 0 {
            eng.commit(std::mem::take(batch))
                .map_err(|e: StoreError| anyhow::anyhow!(e))?;
            *batch = eng.batch();
            *batch_n = 0;
        }
        Ok(())
    };

    for slot_u in 0..slots {
        for idx in 0..cgc {
            let (table, key) = column_put_target(cfg.layout, slot_u, idx);
            batch.put(&table, &key, &col_val);
            batch_n += 1;
            puts += 1;
            live_bytes += MEAN_VALUE_BYTES as u64;
            if batch_n >= BATCH_KEYS {
                flush(eng, &mut batch, &mut batch_n)?;
            }
        }
        let (table, key) = block_put_target(cfg.layout, slot_u);
        batch.put(&table, &key, &blk_val);
        batch_n += 1;
        puts += 1;
        live_bytes += BLOCK_VALUE_BYTES as u64;
        if batch_n >= BATCH_KEYS {
            flush(eng, &mut batch, &mut batch_n)?;
        }
    }
    flush(eng, &mut batch, &mut batch_n)?;

    Ok(BuildStats {
        puts,
        live_bytes,
        elapsed_secs: t0.elapsed().as_secs_f64(),
        slots,
        epochs,
        cgc,
    })
}

fn column_put_target(layout: Layout, slot_u: u64, idx: u16) -> (String, Vec<u8>) {
    let slot = Slot::new(slot_u);
    match layout {
        Layout::Flat => {
            let shard = (slot_u / SLOTS_PER_EPOCH / COLUMN_SHARD_EPOCHS) as u16;
            let key = encode_flat_column_key(shard, slot, idx);
            ("columns".to_owned(), key.to_vec())
        }
        Layout::Sharded => {
            let shard = slot_u / SLOTS_PER_EPOCH / COLUMN_SHARD_EPOCHS;
            let table = columns_shard_table(shard);
            let key = encode_cold_column_key(slot, idx);
            (table, key.to_vec())
        }
    }
}

fn block_put_target(layout: Layout, slot_u: u64) -> (String, Vec<u8>) {
    let slot = Slot::new(slot_u);
    match layout {
        Layout::Flat => {
            let key = encode_cold_block_key(slot);
            ("blocks".to_owned(), key.to_vec())
        }
        Layout::Sharded => {
            let shard = slot_u / SLOTS_PER_EPOCH / BLOCK_SHARD_EPOCHS;
            let table = blocks_shard_table(shard);
            let key = encode_cold_block_key(slot);
            (table, key.to_vec())
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BuildStats {
    pub(crate) puts: u64,
    pub(crate) live_bytes: u64,
    #[allow(dead_code)]
    pub(crate) elapsed_secs: f64,
    #[allow(dead_code)]
    pub(crate) slots: u64,
    #[allow(dead_code)]
    pub(crate) epochs: u64,
    #[allow(dead_code)]
    pub(crate) cgc: u16,
}
