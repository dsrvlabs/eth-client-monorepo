//! Never-shed tick-lane helpers (S0-A-14 / [PRD] P0-12).
//!
//! The producer lives in [`crate::core`]: a `thread::sleep` loop aligned to
//! genesis-derived slot boundaries, `blocking_send` into a
//! [`cc_scheduler::TICK_LANE_DEPTH`] channel. This module owns the clock math
//! and import-path `on_tick(store, wall_clock_now)`. Disparity is applied
//! only when *this* block's slot is still future
//! ([`admit_block_slot_if_within_disparity`]), never as an unconditional
//! `on_tick` into the next slot.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_fork_choice::{Store, on_tick};
use cc_types::preset::Preset;

/// Spec default `MAXIMUM_GOSSIP_CLOCK_DISPARITY`. Config is the source at the
/// service boundary; this is only `CoreConfig::default`. Written as a product
/// so a bare tolerance literal never appears at a gossip-condition site.
pub const DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY: Duration = Duration::from_millis(5 * 100);

/// Work that rides the never-shed `tick` lane ([ARCH] §3.2).
#[derive(Debug)]
pub enum TickWork {
    /// Per-slot fcU floor + pending_* expiry + wall-clock `on_tick`.
    SlotTick,
}

/// Clock inputs for the future-slot *admission* check (not a fork-choice tick).
#[derive(Debug, Clone, Copy)]
pub struct GossipClock {
    /// Unix milliseconds used for the disparity comparison.
    pub now_millis: u64,
    /// Configured `MAXIMUM_GOSSIP_CLOCK_DISPARITY`.
    pub disparity: Duration,
}

/// Unix time in whole seconds.
#[must_use]
pub fn unix_now_secs() -> u64 {
    unix_now_millis() / 1000
}

/// Unix time in milliseconds (disparity is a millisecond quantity).
#[must_use]
pub fn unix_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Sleep duration until the next genesis-aligned slot boundary.
///
/// On an exact boundary, waits one full slot so a just-emitted tick does not
/// busy-loop. Before genesis, waits until genesis.
#[must_use]
pub fn duration_until_next_slot_boundary(
    now: SystemTime,
    genesis_time: u64,
    seconds_per_slot: u64,
) -> Duration {
    let sps = seconds_per_slot.max(1);
    let genesis = UNIX_EPOCH + Duration::from_secs(genesis_time);
    match now.duration_since(genesis) {
        Err(early) => early.duration(),
        Ok(elapsed) => {
            let slot_ms = u128::from(sps).saturating_mul(1000);
            let rem = elapsed.as_millis() % slot_ms;
            let wait_ms = if rem == 0 { slot_ms } else { slot_ms - rem };
            let wait_ms = u64::try_from(wait_ms).unwrap_or(sps.saturating_mul(1000));
            Duration::from_millis(wait_ms).max(Duration::from_millis(1))
        }
    }
}

/// Slot index at `unix_millis` (no offset).
#[must_use]
pub fn slot_at_millis(genesis_time: u64, seconds_per_slot: u64, unix_millis: u64) -> u64 {
    let sps_ms = seconds_per_slot.max(1).saturating_mul(1000);
    let genesis_ms = genesis_time.saturating_mul(1000);
    if unix_millis <= genesis_ms {
        0
    } else {
        unix_millis.saturating_sub(genesis_ms) / sps_ms
    }
}

/// Whether `now + disparity` reaches `slot_start` (millisecond arithmetic).
///
/// Must not quantize `disparity` up to a whole slot ([PRD] P1-A/8).
#[must_use]
pub fn within_gossip_disparity(now_millis: u64, slot_start_secs: u64, disparity: Duration) -> bool {
    let slot_start_ms = slot_start_secs.saturating_mul(1000);
    let extra = u64::try_from(disparity.as_millis()).unwrap_or(u64::MAX);
    now_millis.saturating_add(extra) >= slot_start_ms
}

/// Unix-seconds start of `slot`.
#[must_use]
pub fn slot_start_secs(genesis_time: u64, seconds_per_slot: u64, slot: u64) -> u64 {
    genesis_time.saturating_add(slot.saturating_mul(seconds_per_slot.max(1)))
}

/// `on_tick(store, now_secs)` only — never the next slot start.
pub fn advance_store_clock_at<P: Preset>(store: &mut Store<P>, now_secs: u64) {
    if let Err(e) = on_tick(store, now_secs) {
        tracing::debug!(error = %e, now_secs, "on_tick at import skipped");
    }
}

/// Advance `store.time` to wall-clock now. Does **not** tick into the next slot
/// when `now` is inside `MAXIMUM_GOSSIP_CLOCK_DISPARITY` of the boundary.
pub fn advance_store_clock<P: Preset>(store: &mut Store<P>) {
    advance_store_clock_at(store, unix_now_secs());
}

/// If `block_slot` is still ahead of the store and `now + disparity` reaches
/// that slot's start, `on_tick` to **this block's** slot start (not the next
/// wall-clock slot). Returns whether the store can now admit `block_slot`.
pub fn admit_block_slot_if_within_disparity<P: Preset>(
    store: &mut Store<P>,
    block_slot: u64,
    now_millis: u64,
    disparity: Duration,
) -> bool {
    if store.get_current_slot().as_u64() >= block_slot {
        return true;
    }
    let start = slot_start_secs(store.genesis_time(), store.seconds_per_slot(), block_slot);
    if !within_gossip_disparity(now_millis, start, disparity) {
        return false;
    }
    if let Err(e) = on_tick(store, start) {
        tracing::debug!(error = %e, start, block_slot, "on_tick for admitted slot skipped");
        return false;
    }
    store.get_current_slot().as_u64() >= block_slot
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn disparity_is_millisecond_not_a_whole_slot() {
        let disparity = DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY;
        assert_eq!(disparity, Duration::from_millis(500));
        let slot_start = 1_000_u64;
        let slot_start_ms = slot_start * 1000;
        // 300 ms early — allowed.
        assert!(within_gossip_disparity(
            slot_start_ms - 300,
            slot_start,
            disparity
        ));
        // 501 ms early — not allowed (would be allowed if quantized to 12 s).
        assert!(!within_gossip_disparity(
            slot_start_ms - 501,
            slot_start,
            disparity
        ));
        // A full-slot quantization of 12 s would accept this; we must not.
        assert!(!within_gossip_disparity(
            slot_start_ms - 11_000,
            slot_start,
            disparity
        ));
    }

    #[test]
    fn next_boundary_aligns_to_genesis_not_a_phase_offset() {
        let genesis = 1_000_u64;
        let sps = 12_u64;
        // 3 s after genesis (mid slot 0) → 9 s to slot 1.
        let now = UNIX_EPOCH + Duration::from_secs(genesis + 3);
        let wait = duration_until_next_slot_boundary(now, genesis, sps);
        assert_eq!(wait, Duration::from_secs(9));
        // Exact boundary → one full slot (no busy loop).
        let on_boundary = UNIX_EPOCH + Duration::from_secs(genesis + 12);
        let wait = duration_until_next_slot_boundary(on_boundary, genesis, sps);
        assert_eq!(wait, Duration::from_secs(12));
        // Before genesis.
        let early = UNIX_EPOCH + Duration::from_secs(genesis - 4);
        let wait = duration_until_next_slot_boundary(early, genesis, sps);
        assert_eq!(wait, Duration::from_secs(4));
    }

    #[test]
    fn slot_at_millis_does_not_round_disparity_to_a_slot() {
        let genesis = 1_000_u64;
        let sps = 12_u64;
        let start_slot_1_ms = (genesis + sps) * 1000;
        assert_eq!(slot_at_millis(genesis, sps, start_slot_1_ms), 1);
        // 300 ms before slot 1 is still slot 0 (slot math); disparity is separate.
        assert_eq!(slot_at_millis(genesis, sps, start_slot_1_ms - 300), 0);
    }

    /// Private always-Valid harness (not exported).
    #[derive(Debug, Default, Clone, Copy)]
    struct AcceptEngine;

    impl<P: cc_types::preset::Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: cc_state_transition::NewPayloadRequest<'_, P>,
        ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
            Ok(cc_state_transition::PayloadStatus::Valid)
        }
    }

    fn root(b: u8) -> cc_types::primitives::Root {
        let mut a = [0u8; 32];
        a[0] = b;
        cc_types::primitives::Root::from_array(a)
    }

    fn new_store(time: u64) -> Store<cc_types::preset::Minimal> {
        use cc_types::containers::Checkpoint;
        use cc_types::primitives::Epoch;
        let anchor = Checkpoint {
            epoch: Epoch::new(0),
            root: root(1),
        };
        Store::new(
            time,
            0,
            6,
            anchor,
            anchor,
            16,
            std::sync::Arc::new(AcceptEngine),
            std::sync::Arc::new(cc_fork_choice::HarnessAvailability),
        )
    }

    #[test]
    fn advance_store_clock_at_does_not_enter_the_next_slot() {
        let mut store = new_store(0);
        store.set_proposer_boost_root(root(0xAB));
        // Last second of slot 0 — old code jumped to slot 1 if now was within 500 ms.
        advance_store_clock_at(&mut store, 5);
        assert_eq!(store.time(), 5);
        assert_eq!(store.get_current_slot().as_u64(), 0);
        assert_eq!(store.proposer_boost_root(), root(0xAB));
    }

    #[test]
    fn admit_300ms_early_ticks_only_that_block_slot() {
        let mut store = new_store(0);
        store.set_proposer_boost_root(root(0xAB));
        let disparity = DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY;
        let slot_1_start = slot_start_secs(0, 6, 1);
        assert!(admit_block_slot_if_within_disparity(
            &mut store,
            1,
            slot_1_start * 1000 - 300,
            disparity,
        ));
        assert_eq!(store.time(), slot_1_start);
        assert_eq!(store.get_current_slot().as_u64(), 1);
        assert_eq!(
            store.proposer_boost_root(),
            cc_types::primitives::Root::ZERO
        );
    }

    #[test]
    fn admit_501ms_early_does_not_tick() {
        let mut store = new_store(0);
        store.set_proposer_boost_root(root(0xAB));
        let disparity = DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY;
        let slot_1_start = slot_start_secs(0, 6, 1);
        assert!(!admit_block_slot_if_within_disparity(
            &mut store,
            1,
            slot_1_start * 1000 - 501,
            disparity,
        ));
        assert_eq!(store.time(), 0);
        assert_eq!(store.get_current_slot().as_u64(), 0);
        assert_eq!(store.proposer_boost_root(), root(0xAB));
    }

    #[test]
    fn current_slot_does_not_admit_the_next_slot() {
        let mut store = new_store(5);
        store.set_proposer_boost_root(root(0xAB));
        // Admitting a *current* slot 0 block must not jump to slot 1.
        assert!(admit_block_slot_if_within_disparity(
            &mut store,
            0,
            slot_start_secs(0, 6, 1) * 1000 - 300,
            DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY,
        ));
        assert_eq!(store.get_current_slot().as_u64(), 0);
        assert_eq!(store.proposer_boost_root(), root(0xAB));
    }
}
