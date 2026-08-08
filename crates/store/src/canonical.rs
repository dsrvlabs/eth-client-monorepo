//! Canonical chain index: `slot → root` maintained by a parent-root walk (CC-43a).
//!
//! The `canonical` table has **no counterpart in the reference clients**. It is
//! what the serve path consults above the hot/cold split, what `CC-44b`'s
//! `CURSOR_TOO_OLD` fallback queries, and what `CC-4H`'s `I-contig` walks.
//!
//! ## Walk rule
//!
//! On a `HEAD` or `CHAIN_REORG` event, walk `parent_root` backwards from the new
//! head until the walk meets an already-canonical row, rewriting
//! `canonical[slot]` for each slot on the path. The walk's writes go into the
//! **same** [`Batch`] as the event's block write, so the index can never disagree
//! with the blocks it indexes.
//!
//! `parent_root` is read at byte offset [`crate::blocks::PARENT_ROOT_SSZ_OFFSET`]
//! (**116**) without a full SSZ decode. The offset is asserted against a real
//! Hoodi fixture in `tests/parent_root_offset.rs` (*Values Deliberately Not
//! Invented* 5 — do not ship the arithmetic without the test).

use std::collections::HashMap;

use cc_types::{Root, Slot};

use crate::blocks::{
    PendingBlocks, parent_root_at_offset, resolve_block_ssz, slot_at_offset, stage_pending,
};
use crate::engine::{Batch, ReadTxn, StoreError};
use crate::keys::{BlockRegion, decode_root_value, encode_cold_block_key, encode_root_value};

/// Canonical index table (`slot:u64be` → `root:32`).
pub const TABLE_CANONICAL: &str = "canonical";

/// Hard cap on walk steps (reorg depth bound; also DoS guard).
///
/// Full serve window is ~985k slots; a walk longer than that is corruption or a
/// missing fork-point row, not a legitimate reorg.
pub const MAX_CANONICAL_WALK_STEPS: u64 = 1_048_576;

/// Outcome of a canonical rewrite walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalWalkResult {
    /// Number of `canonical[slot]` rows written (including no-ops that match).
    pub rewritten: u64,
    /// Slot where the walk stopped because the existing row already matched
    /// (`None` if the walk exhausted parents / hit genesis).
    pub fork_point: Option<Slot>,
    /// Slots whose canonical root was changed (old ≠ new). Used by reorg tests.
    pub changed_slots: Vec<Slot>,
}

/// Read `canonical[slot]`.
pub fn get_canonical(rt: &ReadTxn, slot: Slot) -> Result<Option<Root>, StoreError> {
    let Some(v) = rt.get(TABLE_CANONICAL, &encode_cold_block_key(slot))? else {
        return Ok(None);
    };
    decode_root_value(&v)
        .ok_or_else(|| StoreError::Codec(format!("canonical value len {} (want 32)", v.len())))
        .map(Some)
}

/// Stage `canonical[slot] = root` with key-level idempotency.
///
/// Same root → idempotent. **Different** root under the same slot is allowed and
/// **overwrites** — that is a reorg, not a key collision. (Key collision applies
/// to content-addressed block bodies, not to the mutable head index.)
pub fn put_canonical(
    _rt: &ReadTxn,
    batch: &mut Batch,
    slot: Slot,
    root: &Root,
) -> Result<(), StoreError> {
    batch.put(
        TABLE_CANONICAL,
        &encode_cold_block_key(slot),
        &encode_root_value(root),
    );
    Ok(())
}

/// Walk `parent_root` from `head_root` and stage canonical rewrites into `batch`.
///
/// `pending` supplies block bodies staged in the same batch but not yet
/// committed (so the new head is visible to the walk). Committed store is used
/// for parents and for the "already-canonical" stop condition.
///
/// Stop conditions:
/// 1. Existing `canonical[slot] == current_root` (fork point / already indexed).
/// 2. Parent root missing from store and pending.
/// 3. Parent root is zero.
/// 4. [`MAX_CANONICAL_WALK_STEPS`] exceeded → [`StoreError::Limit`].
pub fn rewrite_from_head(
    rt: &ReadTxn,
    batch: &mut Batch,
    pending: &PendingBlocks,
    head_root: &Root,
) -> Result<CanonicalWalkResult, StoreError> {
    let mut current = *head_root;
    let mut rewritten = 0u64;
    let mut changed_slots = Vec::new();
    let mut fork_point = None;
    let mut steps = 0u64;

    loop {
        if steps >= MAX_CANONICAL_WALK_STEPS {
            return Err(StoreError::limit(format!(
                "canonical walk exceeded MAX_CANONICAL_WALK_STEPS ({MAX_CANONICAL_WALK_STEPS})"
            )));
        }
        steps += 1;

        let Some((slot, _region, ssz)) = resolve_block_ssz(rt, pending, &current)? else {
            // Head or parent not in store — stop without error (partial history).
            break;
        };

        // Prefer slot from reverse index / pending; cross-check fixed offset when long enough.
        let slot = if ssz.len() >= crate::blocks::SLOT_SSZ_OFFSET + 8 {
            slot_at_offset(&ssz).unwrap_or(slot)
        } else {
            slot
        };

        match get_canonical(rt, slot)? {
            Some(existing) if existing == current => {
                // Already canonical at this slot — fork point. Do not rewrite.
                fork_point = Some(slot);
                break;
            }
            Some(_) => {
                // Reorg: different root at this slot.
                put_canonical(rt, batch, slot, &current)?;
                changed_slots.push(slot);
                rewritten += 1;
            }
            None => {
                put_canonical(rt, batch, slot, &current)?;
                changed_slots.push(slot);
                rewritten += 1;
            }
        }

        // Walk to parent via fixed offset.
        let parent = parent_root_at_offset(&ssz)?;
        if parent == Root::ZERO || parent == current {
            break;
        }
        current = parent;
    }

    // changed_slots is head→parent order; reverse for ascending-slot consumers.
    changed_slots.reverse();

    Ok(CanonicalWalkResult {
        rewritten,
        fork_point,
        changed_slots,
    })
}

/// Convenience: stage a hot block + rewrite canonical from that head in one batch.
///
/// The block body is put first (idempotent), then the walk runs against an
/// overlay that includes the new body. Caller commits the batch.
pub fn put_block_and_update_head(
    rt: &ReadTxn,
    batch: &mut Batch,
    slot: Slot,
    root: &Root,
    ssz: &[u8],
    region: BlockRegion,
    write_state_root: bool,
) -> Result<(crate::blocks::PutBlockOutcome, CanonicalWalkResult), StoreError> {
    let outcome = crate::blocks::put_block(rt, batch, slot, root, ssz, region, write_state_root)?;
    let mut pending = HashMap::new();
    stage_pending(&mut pending, slot, *root, region, ssz.to_vec());
    let walk = rewrite_from_head(rt, batch, &pending, root)?;
    Ok((outcome, walk))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
        get_block_by_root, put_block,
    };
    use crate::engine::{Durability, Engine, EngineOptions};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-canon-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn eng(label: &str) -> (PathBuf, Engine) {
        let dir = tmp_dir(label);
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        (dir, eng)
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

    /// Build a linear chain of `n` blocks at slots 1..=n; roots = root_n(slot as u8).
    fn commit_linear_chain(eng: &Engine, n: u8) {
        let mut parent = Root::ZERO;
        for i in 1..=n {
            let slot = Slot::new(u64::from(i));
            let root = root_n(i);
            let ssz = synth_block(u64::from(i), &parent, &root_n(0xF0));
            let mut b = eng.batch();
            {
                let rt = eng.read().unwrap();
                put_block_and_update_head(&rt, &mut b, slot, &root, &ssz, BlockRegion::Hot, false)
                    .unwrap();
            }
            eng.commit(b).unwrap();
            parent = root;
        }
    }

    #[test]
    fn walk_extends_linear_chain() {
        let (dir, eng) = eng("linear");
        commit_linear_chain(&eng, 5);
        let rt = eng.read().unwrap();
        for i in 1u8..=5 {
            assert_eq!(
                get_canonical(&rt, Slot::new(u64::from(i)))
                    .unwrap()
                    .unwrap(),
                root_n(i)
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reorg_rewrites_above_fork_point_only() {
        // Two-branch fork: common 1-2-3, then A: 4a-5a vs B: 4b-5b. Switch head A→B.
        let (dir, eng) = eng("reorg");

        // Common prefix slots 1,2,3.
        commit_linear_chain(&eng, 3);

        // Branch A: slot 4 root=0xA4, slot 5 root=0xA5
        let r3 = root_n(3);
        let r4a = Root::from_array([0xA4; 32]);
        let r5a = Root::from_array([0xA5; 32]);
        let ssz4a = synth_block(4, &r3, &root_n(0xF0));
        let ssz5a = synth_block(5, &r4a, &root_n(0xF0));
        for (slot, root, ssz) in [(Slot::new(4), r4a, ssz4a), (Slot::new(5), r5a, ssz5a)] {
            let mut b = eng.batch();
            {
                let rt = eng.read().unwrap();
                put_block_and_update_head(&rt, &mut b, slot, &root, &ssz, BlockRegion::Hot, false)
                    .unwrap();
            }
            eng.commit(b).unwrap();
        }

        // Snapshot canonical under A.
        {
            let rt = eng.read().unwrap();
            assert_eq!(get_canonical(&rt, Slot::new(4)).unwrap().unwrap(), r4a);
            assert_eq!(get_canonical(&rt, Slot::new(5)).unwrap().unwrap(), r5a);
            assert_eq!(get_canonical(&rt, Slot::new(3)).unwrap().unwrap(), r3);
        }

        // Branch B from slot 3: 4b, 5b (siblings of A).
        let r4b = Root::from_array([0xB4; 32]);
        let r5b = Root::from_array([0xB5; 32]);
        let ssz4b = synth_block(4, &r3, &root_n(0xF1));
        let ssz5b = synth_block(5, &r4b, &root_n(0xF1));

        // Write B blocks without updating head yet.
        {
            let mut b = eng.batch();
            let rt = eng.read().unwrap();
            put_block(
                &rt,
                &mut b,
                Slot::new(4),
                &r4b,
                &ssz4b,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            put_block(
                &rt,
                &mut b,
                Slot::new(5),
                &r5b,
                &ssz5b,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            eng.commit(b).unwrap();
        }

        // Switch head to B: walk should rewrite 5 and 4, stop at 3.
        let mut b = eng.batch();
        let walk = {
            let rt = eng.read().unwrap();
            let pending: PendingBlocks = HashMap::new();
            rewrite_from_head(&rt, &mut b, &pending, &r5b).unwrap()
        };
        eng.commit(b).unwrap();

        assert_eq!(walk.fork_point, Some(Slot::new(3)));
        let changed: Vec<u64> = walk.changed_slots.iter().map(|s| s.as_u64()).collect();
        assert_eq!(changed, vec![4, 5], "exactly slots above fork point");

        let rt = eng.read().unwrap();
        // Above fork: B.
        assert_eq!(get_canonical(&rt, Slot::new(4)).unwrap().unwrap(), r4b);
        assert_eq!(get_canonical(&rt, Slot::new(5)).unwrap().unwrap(), r5b);
        // At and below fork: unchanged.
        assert_eq!(get_canonical(&rt, Slot::new(3)).unwrap().unwrap(), r3);
        assert_eq!(
            get_canonical(&rt, Slot::new(2)).unwrap().unwrap(),
            root_n(2)
        );
        assert_eq!(
            get_canonical(&rt, Slot::new(1)).unwrap().unwrap(),
            root_n(1)
        );
        // Sibling A bodies still present (hot keeps non-canonical).
        assert!(get_block_by_root(&rt, &r5a).unwrap().is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn walk_and_block_same_batch_discard_lands_neither() {
        // AC: batch-commit failure (discard) after walk → neither block nor canonical.
        let (dir, eng) = eng("atomic-discard");
        // Seed parent so walk has somewhere to stop.
        commit_linear_chain(&eng, 2);

        let r2 = root_n(2);
        let r3 = root_n(3);
        let ssz = synth_block(3, &r2, &root_n(0xF0));

        // Build batch with block + walk; drop without commit.
        {
            let mut b = eng.batch();
            let rt = eng.read().unwrap();
            put_block_and_update_head(
                &rt,
                &mut b,
                Slot::new(3),
                &r3,
                &ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            assert!(!b.is_empty());
            // Injected "commit failure": discard batch.
            drop(b);
        }

        let rt = eng.read().unwrap();
        assert!(
            get_block_by_root(&rt, &r3).unwrap().is_none(),
            "block must not land without commit"
        );
        assert!(
            get_canonical(&rt, Slot::new(3)).unwrap().is_none(),
            "canonical must not land without commit"
        );
        // Parent chain intact.
        assert_eq!(get_canonical(&rt, Slot::new(2)).unwrap().unwrap(), r2);

        // Positive control: same ops with commit land both atomically.
        {
            let mut b = eng.batch();
            let rt = eng.read().unwrap();
            put_block_and_update_head(
                &rt,
                &mut b,
                Slot::new(3),
                &r3,
                &ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            eng.commit(b).unwrap();
        }
        let rt = eng.read().unwrap();
        assert!(get_block_by_root(&rt, &r3).unwrap().is_some());
        assert_eq!(get_canonical(&rt, Slot::new(3)).unwrap().unwrap(), r3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn commit_failure_via_overflow_lands_neither() {
        // Stronger atomicity: force Engine::commit to fail (batch overflow) after
        // staging block + canonical walk — neither row may be visible.
        let (dir, eng) = eng("atomic-overflow");
        commit_linear_chain(&eng, 1);

        let r1 = root_n(1);
        let r2 = root_n(2);
        let ssz = synth_block(2, &r1, &root_n(0xF0));

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_block_and_update_head(
                &rt,
                &mut b,
                Slot::new(2),
                &r2,
                &ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
        }
        // Overflow the batch after the walk so commit fails closed.
        for i in 0..crate::engine::MAX_BATCH_OPS {
            b.put("meta", &i.to_be_bytes(), b"x");
        }
        assert!(b.overflowed());
        let err = eng.commit(b).unwrap_err();
        assert!(
            matches!(err, StoreError::Limit(_)),
            "overflow must fail commit: {err:?}"
        );

        let rt = eng.read().unwrap();
        assert!(get_block_by_root(&rt, &r2).unwrap().is_none());
        assert!(get_canonical(&rt, Slot::new(2)).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
