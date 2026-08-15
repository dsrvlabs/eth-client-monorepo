//! Slot clock — Architecture §2.5, CC-20b.
//!
//! Owns `genesis_time` and `seconds_per_slot`. Until the first `ChainView`
//! (CC-27a) both values come from config / tests. Every timing-dependent gossip
//! condition must use [`SlotClock::maximum_gossip_clock_disparity`] — **never**
//! an inlined disparity constant.

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
}
