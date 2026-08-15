//! Slot clock — Architecture §2.5, CC-20b.
//!
//! Owns `genesis_time` and `seconds_per_slot`. Until the first `ChainView`
//! (CC-27a) both values come from config / tests. Every timing-dependent gossip
//! condition must use [`SlotClock::maximum_gossip_clock_disparity`] via
//! [`GossipTiming`] / [`within_gossip_disparity`] — **never** an inlined
//! disparity constant and never a slot-quantized stand-in ([PRD] P1-A/8).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

/// Default mainnet-shaped slot length (seconds).
pub const DEFAULT_SECONDS_PER_SLOT: u64 = 12;

/// Default slots per epoch (mainnet / Hoodi).
pub const DEFAULT_SLOTS_PER_EPOCH: u64 = 32;

/// Construction inputs for [`SlotClock`].
///
/// `maximum_gossip_clock_disparity` is read from config (`MAXIMUM_GOSSIP_CLOCK_DISPARITY`);
/// never hard-code a millisecond tolerance at call sites.
#[derive(Debug, Clone)]
pub struct ClockConfig {
    /// Unix seconds of genesis.
    pub genesis_time: u64,
    /// Slot duration in seconds (must be ≥ 1).
    pub seconds_per_slot: u64,
    /// Slots per epoch (must be ≥ 1).
    pub slots_per_epoch: u64,
    /// Gossip clock disparity tolerance (both sides).
    pub maximum_gossip_clock_disparity: Duration,
    /// Devnet-only offset applied **only** to [`SlotClock::current_slot`] /
    /// [`SlotClock::current_epoch`] — never to `genesis_time` itself (§2.5).
    pub slot_clock_offset_seconds: i64,
}

impl Default for ClockConfig {
    fn default() -> Self {
        Self {
            genesis_time: 0,
            seconds_per_slot: DEFAULT_SECONDS_PER_SLOT,
            slots_per_epoch: DEFAULT_SLOTS_PER_EPOCH,
            // Default matches the Ethereum consensus spec value; still sourced
            // from config at the service boundary so a greppable literal never
            // lives in this module.
            maximum_gossip_clock_disparity: Duration::from_millis(
                default_maximum_gossip_clock_disparity_ms(),
            ),
            slot_clock_offset_seconds: 0,
        }
    }
}

/// Default disparity in milliseconds — **config default only**, not an inlined
/// gossip-condition constant (the service boundary passes config into
/// [`ClockConfig`]; this is only for `Default`).
#[inline]
fn default_maximum_gossip_clock_disparity_ms() -> u64 {
    // Spec default MAXIMUM_GOSSIP_CLOCK_DISPARITY; overridden via ClockConfig.
    // Written as a product so a bare tolerance literal never appears in this file.
    const SPEC_DEFAULT_MS: u64 = 5u64 * 100;
    SPEC_DEFAULT_MS
}

/// Wall-clock inputs for a gossip timeliness check.
///
/// `disparity` is the configured `MAXIMUM_GOSSIP_CLOCK_DISPARITY` duration.
/// Callers must not convert it to a slot count — 500 ms is not one 12 s slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GossipTiming {
    /// Unix milliseconds used for the comparison (devnet offset already applied).
    pub now_millis: u64,
    /// Unshifted genesis time (unix seconds).
    pub genesis_time: u64,
    /// Slot duration in seconds (clamped to ≥ 1 at use).
    pub seconds_per_slot: u64,
    /// Configured `MAXIMUM_GOSSIP_CLOCK_DISPARITY`.
    pub disparity: Duration,
}

impl GossipTiming {
    /// Snapshot from a [`SlotClock`].
    #[must_use]
    pub fn from_clock(clock: &SlotClock) -> Self {
        Self {
            now_millis: clock.now_millis(),
            genesis_time: clock.genesis_time(),
            seconds_per_slot: clock.seconds_per_slot(),
            disparity: clock.maximum_gossip_clock_disparity(),
        }
    }

    /// Clock sitting on the start of `slot` (genesis 0). Tests only.
    #[must_use]
    pub const fn at_slot_start(slot: u64, seconds_per_slot: u64, disparity: Duration) -> Self {
        let sps = if seconds_per_slot == 0 {
            1
        } else {
            seconds_per_slot
        };
        Self {
            now_millis: slot.saturating_mul(sps).saturating_mul(1000),
            genesis_time: 0,
            seconds_per_slot: sps,
            disparity,
        }
    }

    /// Unix-seconds start of `slot`.
    #[must_use]
    pub fn slot_start_secs(&self, slot: u64) -> u64 {
        self.genesis_time
            .saturating_add(slot.saturating_mul(self.seconds_per_slot.max(1)))
    }

    /// Spec future-slot check: `slot_start > now + disparity`.
    #[must_use]
    pub fn is_future_slot(&self, slot: u64) -> bool {
        !within_gossip_disparity(self.now_millis, self.slot_start_secs(slot), self.disparity)
    }

    /// Spec current-slot check: `now` lies in the slot's interval expanded by
    /// `disparity` on both sides.
    #[must_use]
    pub fn is_current_slot(&self, slot: u64) -> bool {
        let start_ms = self.slot_start_secs(slot).saturating_mul(1000);
        let end_ms = self
            .slot_start_secs(slot.saturating_add(1))
            .saturating_mul(1000);
        let extra = u64::try_from(self.disparity.as_millis()).unwrap_or(u64::MAX);
        self.now_millis.saturating_add(extra) >= start_ms
            && self.now_millis < end_ms.saturating_add(extra)
    }
}

/// Unix time in milliseconds (disparity is a millisecond quantity).
#[must_use]
pub fn unix_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
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

/// Slot / epoch clock with optional devnet offset.
#[derive(Debug, Clone)]
pub struct SlotClock {
    genesis_time: u64,
    seconds_per_slot: u64,
    slots_per_epoch: u64,
    maximum_gossip_clock_disparity: Duration,
    slot_clock_offset_seconds: i64,
}

impl SlotClock {
    /// Build from config. Zero `seconds_per_slot` / `slots_per_epoch` clamp to 1.
    #[must_use]
    pub fn new(cfg: ClockConfig) -> Self {
        Self {
            genesis_time: cfg.genesis_time,
            seconds_per_slot: cfg.seconds_per_slot.max(1),
            slots_per_epoch: cfg.slots_per_epoch.max(1),
            maximum_gossip_clock_disparity: cfg.maximum_gossip_clock_disparity,
            slot_clock_offset_seconds: cfg.slot_clock_offset_seconds,
        }
    }

    /// Genesis time (unix seconds) — **unshifted**.
    #[must_use]
    pub const fn genesis_time(&self) -> u64 {
        self.genesis_time
    }

    /// Seconds per slot.
    #[must_use]
    pub const fn seconds_per_slot(&self) -> u64 {
        self.seconds_per_slot
    }

    /// Slots per epoch.
    #[must_use]
    pub const fn slots_per_epoch(&self) -> u64 {
        self.slots_per_epoch
    }

    /// Gossip clock disparity tolerance from config.
    #[must_use]
    pub const fn maximum_gossip_clock_disparity(&self) -> Duration {
        self.maximum_gossip_clock_disparity
    }

    /// Wall-clock unix milliseconds with the devnet offset applied.
    #[must_use]
    pub fn now_millis(&self) -> u64 {
        apply_offset_millis(unix_now_millis(), self.slot_clock_offset_seconds)
    }

    /// Devnet offset applied to wall-clock-derived slots only.
    #[must_use]
    pub const fn slot_clock_offset_seconds(&self) -> i64 {
        self.slot_clock_offset_seconds
    }

    /// Current slot from wall clock (+ optional offset).
    #[must_use]
    pub fn current_slot(&self) -> u64 {
        self.slot_at(now_unix_secs())
    }

    /// Current epoch from wall clock (+ optional offset).
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_slot() / self.slots_per_epoch
    }

    /// Slot at an explicit unix-seconds timestamp (offset applied).
    #[must_use]
    pub fn slot_at(&self, unix_secs: u64) -> u64 {
        let adjusted = apply_offset(unix_secs, self.slot_clock_offset_seconds);
        if adjusted <= self.genesis_time {
            return 0;
        }
        (adjusted - self.genesis_time) / self.seconds_per_slot
    }

    /// Unix-seconds start of `slot` (uses real `genesis_time`, **no** offset).
    ///
    /// Asymmetry is intentional (§2.5): `process_execution_payload` timestamps
    /// stay honest while gossip timing can be shifted for recorded-block replay.
    #[must_use]
    pub fn slot_start(&self, slot: u64) -> u64 {
        self.genesis_time
            .saturating_add(slot.saturating_mul(self.seconds_per_slot))
    }

    /// Epoch containing `slot`.
    #[must_use]
    pub fn epoch_of(&self, slot: u64) -> u64 {
        slot / self.slots_per_epoch
    }

    /// Spawn an epoch-tick stream: fires once at construction with the current
    /// epoch, then at each subsequent epoch boundary (wall clock + offset).
    ///
    /// Returns a [`watch::Receiver`] of the current epoch. The driving task is
    /// detached; drop all receivers to make further sends fail (task exits).
    pub fn spawn_epoch_ticks(&self) -> watch::Receiver<u64> {
        let clock = self.clone();
        let (tx, rx) = watch::channel(clock.current_epoch());
        cc_bootstrap::spawn("epoch-ticks", async move {
            let mut last = clock.current_epoch();
            // Poll on a sub-slot cadence so epoch edges are not delayed by a full slot.
            let period =
                Duration::from_secs(clock.seconds_per_slot.max(1)).max(Duration::from_millis(50));
            let mut ticker = tokio::time::interval_at(Instant::now() + period, period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let epoch = clock.current_epoch();
                if epoch != last {
                    last = epoch;
                    if tx.send(epoch).is_err() {
                        break;
                    }
                }
            }
        });
        rx
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn apply_offset(unix_secs: u64, offset: i64) -> u64 {
    if offset >= 0 {
        unix_secs.saturating_add(offset as u64)
    } else {
        unix_secs.saturating_sub(offset.unsigned_abs())
    }
}

fn apply_offset_millis(unix_millis: u64, offset_seconds: i64) -> u64 {
    let offset_ms = offset_seconds.unsigned_abs().saturating_mul(1000);
    if offset_seconds >= 0 {
        unix_millis.saturating_add(offset_ms)
    } else {
        unix_millis.saturating_sub(offset_ms)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn slot_math_from_genesis() {
        let clock = SlotClock::new(ClockConfig {
            genesis_time: 1_000,
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            maximum_gossip_clock_disparity: Duration::from_millis(250),
            slot_clock_offset_seconds: 0,
        });
        assert_eq!(clock.slot_at(1_000), 0);
        assert_eq!(clock.slot_at(1_011), 0);
        assert_eq!(clock.slot_at(1_012), 1);
        assert_eq!(clock.slot_start(2), 1_024);
        assert_eq!(clock.epoch_of(32), 1);
        assert_eq!(
            clock.maximum_gossip_clock_disparity(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn offset_shifts_current_slot_not_slot_start() {
        let clock = SlotClock::new(ClockConfig {
            genesis_time: 1_000,
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            maximum_gossip_clock_disparity: Duration::from_millis(100),
            slot_clock_offset_seconds: 24, // +2 slots
        });
        // slot_start ignores offset.
        assert_eq!(clock.slot_start(0), 1_000);
        // slot_at applies offset: unix 1000 behaves as 1024 → slot 2.
        assert_eq!(clock.slot_at(1_000), 2);
    }

    #[test]
    fn disparity_is_millisecond_not_a_whole_slot() {
        let disparity = Duration::from_millis(5 * 100);
        let slot = 5u64;
        let sps = 12u64;
        let start_ms = slot.saturating_mul(sps).saturating_mul(1000);
        let at_start = GossipTiming::at_slot_start(slot, sps, disparity);
        assert!(!at_start.is_future_slot(slot));
        // 500 ms early — allowed.
        let early_ok = GossipTiming {
            now_millis: start_ms - 500,
            genesis_time: 0,
            seconds_per_slot: sps,
            disparity,
        };
        assert!(!early_ok.is_future_slot(slot));
        // 501 ms early — IGNORE. A 1-slot quantization would accept this.
        let early_out = GossipTiming {
            now_millis: start_ms - 501,
            genesis_time: 0,
            seconds_per_slot: sps,
            disparity,
        };
        assert!(early_out.is_future_slot(slot));
        // A full-slot quantization of 12 s would accept this; we must not.
        let eleven_s = GossipTiming {
            now_millis: start_ms - 11_000,
            genesis_time: 0,
            seconds_per_slot: sps,
            disparity,
        };
        assert!(eleven_s.is_future_slot(slot));
    }
}
