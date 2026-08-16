//! State-roots prune (with blocks) and snapshot-ring pass (Architecture §7.0 / CC-46a).
//!
//! | Pass | Trigger |
//! |---|---|
//! | state roots | **with blocks** (same watermark, same epoch tick) |
//! | snapshot ring | keep the newest `storage.snapshot_ring` (4) — **on each snapshot write** |
//!
//! Unfinalized-branch deletion is **not** here — it lives in `migrate.rs`
//! (*Deviations* 2). There is deliberately **no** `unfinalized.rs`.

use cc_store::keys::encode_cold_block_key;
use cc_store::{
    Engine, Slot, StoreError, TABLE_SNAPSHOTS, TABLE_STATE_ROOTS, list_snapshot_slots,
    plan_snapshot_put,
};

use super::PrunePlan;

/// Stage `state_roots` deletes for slots in `[from, to)` (pruned with blocks).
pub(crate) fn plan_state_root_deletes(
    engine: &Engine,
    from: Slot,
    to: Slot,
) -> Result<PrunePlan, StoreError> {
    if to.as_u64() <= from.as_u64() {
        return Ok(PrunePlan::default());
    }
    let rt = engine.read()?;
    let mut plan = PrunePlan::default();
    let lo = encode_cold_block_key(from);
    let hi = encode_cold_block_key(to);
    if let Ok(iter) = rt.range(TABLE_STATE_ROOTS, &lo, &hi) {
        for item in iter {
            let (key, value) = item?;
            plan.bytes = plan
                .bytes
                .saturating_add(key.len() as u64 + value.len() as u64);
            plan.rows = plan.rows.saturating_add(1);
            plan.deletes.push((TABLE_STATE_ROOTS.to_owned(), key));
        }
    }
    Ok(plan)
}

/// Snapshot-ring pass: after a snapshot write, ensure depth ≤ `ring`.
///
/// Production snapshot put already stages ring eviction via
/// [`plan_snapshot_put`] (CC-42). This helper re-derives the same plan shape
/// for an explicit prune-pass invocation (metrics / tests) without writing a
/// new snapshot body — it only stages oldest-first deletes when depth > ring.
pub(crate) fn plan_snapshot_ring_trim(engine: &Engine, ring: u64) -> Result<PrunePlan, StoreError> {
    let ring = ring.max(1);
    let rt = engine.read()?;
    let slots = list_snapshot_slots(&rt)?;
    let mut plan = PrunePlan::default();
    if (slots.len() as u64) <= ring {
        return Ok(plan);
    }
    let excess = slots.len() as u64 - ring;
    for slot in slots.into_iter().take(excess as usize) {
        let key = encode_cold_block_key(slot);
        // Best-effort byte accounting.
        if let Ok(Some(v)) = rt.get(TABLE_SNAPSHOTS, &key) {
            plan.bytes = plan.bytes.saturating_add(key.len() as u64 + v.len() as u64);
        }
        plan.rows = plan.rows.saturating_add(1);
        plan.deletes
            .push((TABLE_SNAPSHOTS.to_owned(), key.to_vec()));
    }
    Ok(plan)
}

/// Re-export the CC-42 put planner so the snapshot write path and the prune
/// pass share one authority for ring eviction.
#[allow(dead_code)] // used when prune owns the write-side ring metrics
pub(crate) fn plan_snapshot_write(
    engine: &Engine,
    slot: Slot,
    ssz: &[u8],
    ring: u64,
) -> Result<cc_store::SnapshotPlan, StoreError> {
    let rt = engine.read()?;
    plan_snapshot_put(&rt, slot, ssz, ring)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::{Engine, plan_snapshot_put};

    fn tmp_engine() -> Engine {
        let dir = crate::test_tmpdir::unique_temp_dir("cc-storage-prune-states");
        std::fs::create_dir_all(&dir).unwrap();
        Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::Immediate),
        )
        .unwrap()
    }

    #[test]
    fn ring_trim_keeps_newest_n() {
        let eng = tmp_engine();
        // Write 6 snapshots, ring=4 → trim plans 2 oldest deletes.
        for i in 0..6u64 {
            let slot = Slot::new(i * 32);
            let ssz = vec![i as u8; 64];
            let rt = eng.read().unwrap();
            let plan = plan_snapshot_put(&rt, slot, &ssz, 4).unwrap();
            drop(rt);
            let mut batch = eng.batch();
            for (t, k, v) in &plan.puts {
                batch.put(t, k, v);
            }
            for (t, k) in &plan.deletes {
                batch.delete(t, k);
            }
            eng.commit(batch).unwrap();
        }
        // After CC-42 planner, depth should already be ≤ 4. Explicit trim is no-op.
        let trim = plan_snapshot_ring_trim(&eng, 4).unwrap();
        assert!(trim.deletes.is_empty());
        let rt = eng.read().unwrap();
        assert_eq!(list_snapshot_slots(&rt).unwrap().len(), 4);
    }

    #[test]
    fn state_root_empty_range() {
        let eng = tmp_engine();
        let plan = plan_state_root_deletes(&eng, Slot::new(10), Slot::new(10)).unwrap();
        assert!(plan.deletes.is_empty());
    }
}
