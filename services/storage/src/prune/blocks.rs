//! Block prune pass watermark and the `I2` guard (Architecture §5.2 / §7.0 / CC-46a).
//!
//! Watermark (exclusive upper bound of the deleted range):
//! ```text
//! start_slot(max(GENESIS_EPOCH, current_epoch − compute_min_epochs_for_block_requests()))
//!   − prune_margin_epochs × SLOTS_PER_EPOCH
//! ```
//!
//! The block floor is **CC-4A's computed constant**, never a config field of the
//! serve-window formula. Compressed-retention venues may override the *retention
//! depth* via `storage.retention_override.blocks_epochs` (CC-4D) for the
//! discharging venue only.
//!
//! ## `I2` (ADR P4-08)
//!
//! `earliest_available_slot` is *derived, never assigned*. A voluntary increase
//! can only originate in the block-prune watermark. The guard therefore sits
//! **one level below the window**, on the proposed prune mark:
//!
//! - If `proposed > start_slot(current_epoch − floor_epochs)` → **refuse the pass**
//! - Increment `cc_storage_window_increase_rejected_total`
//! - Log at **`error`**
//! - Leave `PruneMarks.blocks_up_to` **unchanged**
//!
//! This is **not a clamp**: there is no `min` / `clamp` path that silently
//! lowers the proposed mark to the floor.

use cc_store::keys::{
    block_shard_id, blocks_shard_table, decode_block_slot_by_root_value, decode_hot_block_key,
    encode_cold_block_key, encode_hot_block_key, encode_root_key, SLOTS_PER_EPOCH,
};
use cc_store::{
    epoch_start_slot, Engine, Root, Slot, StoreError, TABLE_BLOCKS_HOT, TABLE_BLOCK_SLOT_BY_ROOT,
    TABLE_CANONICAL,
};
use tracing::error;

use super::PrunePlan;

/// Genesis epoch (phase0).
pub(crate) const GENESIS_EPOCH: u64 = 0;

/// Result of the `I2` check on a proposed block-prune mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum I2Decision {
    /// Proposed mark is within the serve floor; pass may proceed.
    Allow,
    /// Proposed mark is strictly above the serve floor; **refuse** the pass.
    Refuse {
        /// Proposed exclusive prune mark.
        proposed: Slot,
        /// Spec floor: `start_slot(current_epoch − floor_epochs)`.
        floor: Slot,
    },
}

/// Serve-window floor slot for blocks at wall-clock `current_epoch`.
///
/// `floor_epochs` is [`cc_store::compute_min_epochs_for_block_requests`] (or a
/// CC-4D retention override on the self-devnet). **Not** clamped.
#[must_use]
pub(crate) fn block_serve_floor_slot(current_epoch: u64, floor_epochs: u64) -> Slot {
    // GENESIS_EPOCH is 0; saturating_sub already floors at zero.
    let _genesis = GENESIS_EPOCH;
    let floor_epoch = current_epoch.saturating_sub(floor_epochs.max(1));
    epoch_start_slot(floor_epoch)
}

/// Compute the exclusive block prune mark (with margin) for wall-clock `current_epoch`.
///
/// Newest slot deleted = `mark − 1` =
/// `start_slot(current_epoch − floor) − margin × SPE − 1` (CC-46 /2).
#[must_use]
pub(crate) fn blocks_prune_mark(
    current_epoch: u64,
    floor_epochs: u64,
    margin_epochs: u64,
) -> Slot {
    let floor = block_serve_floor_slot(current_epoch, floor_epochs);
    let margin_slots = margin_epochs.saturating_mul(SLOTS_PER_EPOCH);
    floor.saturating_sub(margin_slots)
}

/// `I2` guard: refuse a proposed mark strictly above the serve floor.
///
/// **Not a clamp** — the only actions are Allow or Refuse. Callers must not
/// lower `proposed` to `floor` on Refuse.
#[must_use]
pub(crate) fn i2_check(
    proposed: Slot,
    current_epoch: u64,
    floor_epochs: u64,
) -> I2Decision {
    let floor = block_serve_floor_slot(current_epoch, floor_epochs);
    if proposed.as_u64() > floor.as_u64() {
        I2Decision::Refuse { proposed, floor }
    } else {
        I2Decision::Allow
    }
}

/// Log + metric side-effect helper for an `I2` refusal (CC-48 /2 criterion implemented here).
pub(crate) fn record_i2_refusal(
    metrics: &crate::metrics::StorageMetrics,
    proposed: Slot,
    floor: Slot,
    current_epoch: u64,
) {
    error!(
        target: "cc_storage::prune",
        proposed = proposed.as_u64(),
        floor = floor.as_u64(),
        current_epoch,
        "I2: block prune mark refused — would delete data inside the mandatory serve window \
         (Prysm: do not voluntarily refuse to serve mandatory block data)"
    );
    metrics.window_increase_rejected.inc();
}

/// Newest slot a blocks pass deletes for the given exclusive mark.
#[must_use]
pub(crate) fn newest_deleted_slot(mark: Slot) -> Option<Slot> {
    mark.checked_sub(1)
}

/// Stage cold + hot block deletes for slots in `[from, to)` plus reverse-index /
/// canonical rows for those slots.
pub(crate) fn plan_block_deletes(
    engine: &Engine,
    from: Slot,
    to: Slot,
) -> Result<PrunePlan, StoreError> {
    if to.as_u64() <= from.as_u64() {
        return Ok(PrunePlan::default());
    }
    let rt = engine.read()?;
    let mut plan = PrunePlan::default();

    let start_shard = block_shard_id(from);
    let end_shard = block_shard_id(Slot::new(to.as_u64().saturating_sub(1)));
    for shard in start_shard..=end_shard {
        let table = blocks_shard_table(shard);
        let lo = encode_cold_block_key(from);
        let hi = encode_cold_block_key(to);
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

    // Hot blocks.
    let lo = encode_hot_block_key(from, &Root::ZERO);
    let hi = encode_hot_block_key(to, &Root::ZERO);
    if let Ok(iter) = rt.range(TABLE_BLOCKS_HOT, &lo, &hi) {
        for item in iter {
            let (key, value) = item?;
            plan.bytes = plan.bytes.saturating_add(key.len() as u64 + value.len() as u64);
            plan.rows = plan.rows.saturating_add(1);
            if let Some((_slot, root)) = decode_hot_block_key(&key) {
                plan.deletes
                    .push((TABLE_BLOCK_SLOT_BY_ROOT.to_owned(), encode_root_key(&root).to_vec()));
            }
            plan.deletes.push((TABLE_BLOCKS_HOT.to_owned(), key));
        }
    }

    // Canonical index rows for the slot span (slot → root).
    let clo = encode_cold_block_key(from);
    let chi = encode_cold_block_key(to);
    if let Ok(iter) = rt.range(TABLE_CANONICAL, &clo, &chi) {
        for item in iter {
            let (key, value) = item?;
            plan.bytes = plan.bytes.saturating_add(key.len() as u64 + value.len() as u64);
            plan.rows = plan.rows.saturating_add(1);
            plan.deletes.push((TABLE_CANONICAL.to_owned(), key));
        }
    }

    // Reverse-index sweep by value slot.
    let idx_lo = [0u8; 32];
    let idx_hi = [0xffu8; 32];
    if let Ok(iter) = rt.range(TABLE_BLOCK_SLOT_BY_ROOT, &idx_lo, &idx_hi) {
        for item in iter {
            let (key, value) = item?;
            let Some((slot, _region)) = decode_block_slot_by_root_value(&value) else {
                continue;
            };
            if slot.as_u64() >= from.as_u64()
                && slot.as_u64() < to.as_u64()
                && !plan
                    .deletes
                    .iter()
                    .any(|(t, k)| t == TABLE_BLOCK_SLOT_BY_ROOT && k.as_slice() == key.as_slice())
            {
                plan.deletes
                    .push((TABLE_BLOCK_SLOT_BY_ROOT.to_owned(), key));
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
    use cc_store::{BlockServeWindowCfg, compute_min_epochs_for_block_requests};

    /// Hoodi / mainnet floor (tests may name the number; production must not).
    fn hoodi_floor() -> u64 {
        let cfg = BlockServeWindowCfg::new(256, 65_536);
        compute_min_epochs_for_block_requests(&cfg).unwrap()
    }

    /// CC-46 /2 — blocks boundary ±1 slot against the computed floor.
    #[test]
    fn block_mark_boundary_plus_minus_one() {
        let floor_epochs = hoodi_floor();
        let current = floor_epochs + 1_000;
        let margin = 1u64;
        let mark = blocks_prune_mark(current, floor_epochs, margin);
        let expected_newest = epoch_start_slot(current - floor_epochs)
            .as_u64()
            .saturating_sub(SLOTS_PER_EPOCH)
            .saturating_sub(1);
        assert_eq!(newest_deleted_slot(mark).unwrap().as_u64(), expected_newest);
        assert_eq!(mark.as_u64(), expected_newest + 1);
    }

    /// I2 allows a correctly-margined mark.
    #[test]
    fn i2_allows_mark_at_or_below_floor() {
        let floor_epochs = hoodi_floor();
        let current = floor_epochs + 500;
        let mark = blocks_prune_mark(current, floor_epochs, 1);
        assert_eq!(i2_check(mark, current, floor_epochs), I2Decision::Allow);
        let floor = block_serve_floor_slot(current, floor_epochs);
        assert_eq!(i2_check(floor, current, floor_epochs), I2Decision::Allow);
    }

    /// CC-46a / CC-48 /2 — proposed mark above the floor is **refused**, not clamped.
    #[test]
    fn i2_refuses_mark_above_floor() {
        let floor_epochs = hoodi_floor();
        let current = floor_epochs + 500;
        let floor = block_serve_floor_slot(current, floor_epochs);
        let proposed = Slot::new(floor.as_u64().saturating_add(1));
        match i2_check(proposed, current, floor_epochs) {
            I2Decision::Refuse {
                proposed: p,
                floor: f,
            } => {
                assert_eq!(p, proposed);
                assert_eq!(f, floor);
            }
            I2Decision::Allow => panic!("must refuse mark above floor"),
        }
        assert!(proposed.as_u64() > floor.as_u64());
    }

    /// The acceptance criterion requires
    /// `grep -rn "min(\|clamp\|saturating_sub" services/storage/src/prune/blocks.rs`
    /// show no path that silently lowers the proposed mark to the floor.
    /// Margin subtraction uses `saturating_sub` on the *floor* when building the
    /// watermark — that is construction, not a clamp of an over-high proposal.
    /// `i2_check` itself has neither `min` nor `clamp`.
    #[test]
    fn i2_source_has_no_clamp_of_proposed() {
        let src = include_str!("blocks.rs");
        let i2_start = src.find("pub(crate) fn i2_check").expect("i2_check present");
        let i2_body = &src[i2_start..];
        let i2_end = i2_body
            .find("pub(crate) fn record_i2_refusal")
            .unwrap_or(i2_body.len());
        let i2_fn = &i2_body[..i2_end];
        assert!(
            !i2_fn.contains("clamp") && !i2_fn.contains(".min("),
            "i2_check must not clamp the proposed mark"
        );
    }
}
