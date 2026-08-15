//! Serve window: `earliest_available_slot` as one `AtomicU64` (ADR P2-14 / §5.3).
//!
//! **One source of truth across a process boundary (CC-48).** The atomic has a
//! **single production `.store` site**: the `WatchServeWindow` stream handler in
//! [`crate::storage_client`]. Cache eviction / insert **must not** write this
//! value — Phase 2's eviction-path write is **deleted**. `Status v2` and the
//! four serve handlers only **read**.
//!
//! Pure [`compute_earliest_available_slot`] remains for the in-memory **cache
//! floor** used by §5.5 fail-closed collapse (not the advertised atomic). The
//! **advertised** value is storage's two-branch derivation (CC-49); this module
//! no longer owns a Phase-2 `anchor`-clamped advertisement formula.
//!
//! # Seed (honesty)
//!
//! Construction seeds `u64::MAX` — an empty serve window — not `anchor`.
//! Advertising `anchor` with an empty cache would free-ride (CC-26a F2).

use std::sync::atomic::{AtomicU64, Ordering};

use cc_types::primitives::Slot;

/// Sentinel: no slots available (empty serve window).
pub const EMPTY_WINDOW_SLOT: u64 = u64::MAX;

/// Pure recompute of the in-memory **cache floor** over a contiguous head range.
///
/// Used only for §5.5 fail-closed collapse when `WatchServeWindow` is stale —
/// **not** the Status-advertised window (that is storage / CC-49).
///
/// Walks the contiguous complete suffix from `head` down to `walk_floor`, then
/// clamps the result so it never sits below `anchor`:
///
/// ```text
/// cache_floor = clamp_at_or_above_anchor(
///     oldest slot s such that for all t in [s, head]: slot t is complete
/// )
/// ```
///
/// Completeness is defined by the caller (block + all **custodied** columns, or
/// an explicit empty slot). If `head` itself is incomplete the window is empty
/// and this returns `head + 1` (still clamped to `≥ anchor`).
///
/// `walk_floor` caps how far back the walk may go (O(window depth), not O(head)):
/// typically the higher of `anchor` and `head.saturating_sub(max_depth)`.
#[must_use]
pub fn compute_earliest_available_slot(
    anchor: Slot,
    head: Slot,
    walk_floor: Slot,
    mut is_complete: impl FnMut(Slot) -> bool,
) -> Slot {
    let anchor_u = anchor.as_u64();
    let head_u = head.as_u64();
    let floor_u = walk_floor.as_u64().max(anchor_u);

    // Empty window: nothing complete at head.
    if !is_complete(head) {
        let empty = head_u.saturating_add(1);
        return Slot::new(empty.max(anchor_u));
    }

    let mut earliest = head_u;
    let mut t = head_u;
    while t > floor_u {
        let prev = t - 1;
        if is_complete(Slot::new(prev)) {
            earliest = prev;
            t = prev;
        } else {
            break;
        }
    }

    Slot::new(earliest.max(anchor_u))
}

/// The single serve-window number advertised by `Status v2` and enforced by
/// by-range / by-root handlers.
///
/// # Write discipline (ADR P2-14 / CC-48 §5.3)
///
/// Construction seeds via [`AtomicU64::new`] (`EMPTY_WINDOW_SLOT`). Production
/// writes go **only** through [`ServeWindow::store_recomputed`] from the
/// `WatchServeWindow` handler (and §5.5 collapse in the same module). Cache
/// eviction must not call this.
#[derive(Debug)]
pub struct ServeWindow {
    /// Earliest slot we can honestly serve. **Sole `AtomicU64` for this value.**
    earliest_available_slot: AtomicU64,
    /// Checkpoint / genesis floor; window never advertises below this.
    anchor_slot: Slot,
    /// How many times [`Self::store_recomputed`] ran (tests / diagnostics).
    recompute_invocations: AtomicU64,
}

impl ServeWindow {
    /// Seed the atomic to [`EMPTY_WINDOW_SLOT`] (construction only — not a store site).
    #[must_use]
    pub fn new(anchor_slot: Slot) -> Self {
        Self {
            earliest_available_slot: AtomicU64::new(EMPTY_WINDOW_SLOT),
            anchor_slot,
            recompute_invocations: AtomicU64::new(0),
        }
    }

    /// Configured anchor (floor for the advertised window).
    #[must_use]
    pub const fn anchor_slot(&self) -> Slot {
        self.anchor_slot
    }

    /// Read the current earliest available slot.
    ///
    /// Returns [`Slot::new`]`(EMPTY_WINDOW_SLOT)` until the first recompute.
    #[must_use]
    pub fn load(&self) -> Slot {
        Slot::new(self.earliest_available_slot.load(Ordering::Acquire))
    }

    /// Whether the atomic still holds the empty-window seed (no recompute yet).
    #[must_use]
    pub fn is_empty_window_seed(&self) -> bool {
        self.earliest_available_slot.load(Ordering::Acquire) == EMPTY_WINDOW_SLOT
    }

    /// Number of recompute writes.
    #[must_use]
    pub fn recompute_invocations(&self) -> u64 {
        self.recompute_invocations.load(Ordering::Relaxed)
    }

    /// Write site for `earliest_available_slot` after construction.
    ///
    /// **Sealed `pub(crate)`:** production callers are only the `WatchServeWindow`
    /// stream handler and §5.5 collapse in `storage_client` (same crate). Cache
    /// eviction / insert must not call this (CC-48 /5 — eviction-path write deleted).
    /// Outside the `cc-p2p` crate this method is not visible.
    pub(crate) fn store_recomputed(&self, slot: Slot) {
        self.recompute_invocations.fetch_add(1, Ordering::Relaxed);
        self.earliest_available_slot
            .store(slot.as_u64(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn empty_head_yields_head_plus_one_clamped_to_anchor() {
        let complete: BTreeSet<u64> = BTreeSet::new();
        let s = compute_earliest_available_slot(Slot::new(10), Slot::new(20), Slot::new(10), |t| {
            complete.contains(&t.as_u64())
        });
        assert_eq!(s, Slot::new(21));
    }

    #[test]
    fn contiguous_suffix_from_head() {
        // Complete: 5,6,7,8,9,10 — missing 4. head=10, floor=0 → earliest=5.
        let complete: BTreeSet<u64> = (5..=10).collect();
        let s = compute_earliest_available_slot(Slot::new(0), Slot::new(10), Slot::new(0), |t| {
            complete.contains(&t.as_u64())
        });
        assert_eq!(s, Slot::new(5));
    }

    #[test]
    fn walk_floor_caps_backward_scan() {
        // All complete down to 0, but floor=8 → earliest cannot go below 8.
        let complete: BTreeSet<u64> = (0..=10).collect();
        let s = compute_earliest_available_slot(Slot::new(0), Slot::new(10), Slot::new(8), |t| {
            complete.contains(&t.as_u64())
        });
        assert_eq!(s, Slot::new(8));
    }

    #[test]
    fn anchor_floors_the_window() {
        let complete: BTreeSet<u64> = (0..=10).collect();
        let s = compute_earliest_available_slot(Slot::new(7), Slot::new(10), Slot::new(0), |t| {
            complete.contains(&t.as_u64())
        });
        assert_eq!(s, Slot::new(7));
    }

    #[test]
    fn construction_seeds_empty_window_not_anchor() {
        let w = ServeWindow::new(Slot::new(42));
        assert!(w.is_empty_window_seed());
        assert_eq!(w.load().as_u64(), EMPTY_WINDOW_SLOT);
        assert_eq!(w.recompute_invocations(), 0);
        w.store_recomputed(Slot::new(100));
        assert_eq!(w.load(), Slot::new(100));
        assert_eq!(w.recompute_invocations(), 1);
        assert!(!w.is_empty_window_seed());
    }
}
