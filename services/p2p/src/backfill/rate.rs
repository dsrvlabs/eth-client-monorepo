//! Outbound block self-limit for backfill — Architecture §6.3 / CC-47b /5.
//!
//! Peers advertise comparable inbound caps (**128 blocks / 10 s**). We must not
//! point the whole queue at the one peer with the deepest window and get
//! descored while claiming compliance. This module:
//!
//! | Surface | Role |
//! |---|---|
//! | [`OUTBOUND_BLOCKS_CAPACITY`] | 128 blocks per peer per window |
//! | [`OUTBOUND_BLOCKS_WINDOW`] | 10 s sliding window |
//! | [`OutboundBlockBudget`] | per-peer hard sliding window + request counters |
//!
//! Cost is on **blocks requested** (batch `count`), not on response chunks.
//! Enforcement is a **hard sliding window** (not continuous-refill token bucket)
//! so "≤ 128 / 10 s" is the measured bound against our own counters — the
//! measurement that would catch a planner concentrating on one deep peer.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use cc_libp2p::PeerId;

/// Per-peer outbound block budget (Architecture §6.3 / Phase 2 §7.3 mirror).
pub const OUTBOUND_BLOCKS_CAPACITY: u64 = 128;
/// Sliding window for the outbound block budget.
pub const OUTBOUND_BLOCKS_WINDOW: Duration = Duration::from_secs(10);

/// One recorded reservation.
#[derive(Debug, Clone, Copy)]
struct Sample {
    at: Instant,
    count: u64,
}

/// Per-peer hard sliding-window budget.
#[derive(Debug, Default)]
struct PeerWindow {
    /// Chronological samples still inside / relevant to the window.
    events: VecDeque<Sample>,
    /// Sum of `events[*].count`.
    sum: u64,
}

impl PeerWindow {
    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.events.front() {
            if now.saturating_duration_since(front.at) > OUTBOUND_BLOCKS_WINDOW {
                self.sum = self.sum.saturating_sub(front.count);
                self.events.pop_front();
            } else {
                break;
            }
        }
    }

    fn used(&mut self, now: Instant) -> u64 {
        self.prune(now);
        self.sum
    }

    fn try_add(&mut self, count: u64, now: Instant) -> bool {
        self.prune(now);
        if self.sum.saturating_add(count) > OUTBOUND_BLOCKS_CAPACITY {
            return false;
        }
        self.events.push_back(Sample { at: now, count });
        self.sum = self.sum.saturating_add(count);
        true
    }

    /// Remove the most recent sample of exact `count` (local cancel / refund).
    fn refund_last(&mut self, count: u64, now: Instant) -> bool {
        self.prune(now);
        if let Some(idx) = self.events.iter().rposition(|s| s.count == count)
            && let Some(s) = self.events.remove(idx)
        {
            self.sum = self.sum.saturating_sub(s.count);
            return true;
        }
        false
    }
}

/// Per-peer outbound block self-limit + counters for CC-47 /5.
///
/// The planner consults [`Self::try_reserve`] before assigning a blocks-by-range
/// batch to a peer. Over any observation window the recorded counters must show
/// ≤ [`OUTBOUND_BLOCKS_CAPACITY`] blocks per peer per [`OUTBOUND_BLOCKS_WINDOW`].
#[derive(Debug)]
pub struct OutboundBlockBudget {
    peers: HashMap<PeerId, PeerWindow>,
    /// Cumulative blocks requested per peer (never reset by window expiry).
    peer_requested: HashMap<PeerId, u64>,
    /// Flat sample log for distribution reporting / max-window scans.
    samples: Vec<(PeerId, Instant, u64)>,
    max_samples: usize,
}

impl Default for OutboundBlockBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl OutboundBlockBudget {
    /// Fresh budget with Architecture capacities.
    #[must_use]
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            peer_requested: HashMap::new(),
            samples: Vec::new(),
            // ~1 h of 64-slot batches at 4 concurrent ≈ 225 samples; 16k is ample.
            max_samples: 16_384,
        }
    }

    /// Tokens (blocks) still available for `peer` right now.
    pub fn available(&mut self, peer: PeerId, now: Instant) -> u64 {
        let used = self.peers.entry(peer).or_default().used(now);
        OUTBOUND_BLOCKS_CAPACITY.saturating_sub(used)
    }

    /// Try to reserve `count` outbound blocks for `peer`.
    ///
    /// On success records counters. On failure the planner must pick another
    /// peer or wait for the window to slide.
    pub fn try_reserve(&mut self, peer: PeerId, count: u64, now: Instant) -> bool {
        if count == 0 {
            return true;
        }
        if count > OUTBOUND_BLOCKS_CAPACITY {
            return false;
        }
        if !self.peers.entry(peer).or_default().try_add(count, now) {
            return false;
        }
        *self.peer_requested.entry(peer).or_insert(0) = self
            .peer_requested
            .get(&peer)
            .copied()
            .unwrap_or(0)
            .saturating_add(count);
        if self.samples.len() >= self.max_samples {
            let drop_n = self.max_samples / 2;
            self.samples.drain(0..drop_n);
        }
        self.samples.push((peer, now, count));
        true
    }

    /// Refund a prior reservation that never left the machine (local schedule
    /// cancel). Cheap: pops the most recent matching sample for `peer`.
    ///
    /// Cumulative `total_requested` is **not** reduced — that counter is
    /// "attempts scheduled", while the sliding window is "in-flight budget".
    pub fn refund(&mut self, peer: PeerId, count: u64, now: Instant) -> bool {
        if count == 0 {
            return true;
        }
        let ok = self.peers.entry(peer).or_default().refund_last(count, now);
        if ok {
            // Drop the newest matching flat sample so max_in_any_window agrees.
            if let Some(i) = self
                .samples
                .iter()
                .rposition(|&(p, _, c)| p == peer && c == count)
            {
                self.samples.remove(i);
            }
        }
        ok
    }

    /// Cumulative blocks requested of `peer` this run.
    #[must_use]
    pub fn total_requested(&self, peer: PeerId) -> u64 {
        self.peer_requested.get(&peer).copied().unwrap_or(0)
    }

    /// All peers that have been reserved against, with cumulative counts
    /// (highest first — concentration is visible at a glance).
    #[must_use]
    pub fn per_peer_totals(&self) -> Vec<(PeerId, u64)> {
        let mut v: Vec<(PeerId, u64)> = self.peer_requested.iter().map(|(&p, &c)| (p, c)).collect();
        v.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
        v
    }

    /// Max blocks requested of any single peer inside any
    /// [`OUTBOUND_BLOCKS_WINDOW`]-length window over the recorded samples.
    ///
    /// Returns `(max_count, peer)` — the measurement CC-47 /5 asserts against
    /// our own counters (≤ [`OUTBOUND_BLOCKS_CAPACITY`]).
    #[must_use]
    pub fn max_in_any_window(&self) -> (u64, Option<PeerId>) {
        if self.samples.is_empty() {
            return (0, None);
        }
        let mut by_peer: HashMap<PeerId, Vec<(Instant, u64)>> = HashMap::new();
        for &(peer, t, c) in &self.samples {
            by_peer.entry(peer).or_default().push((t, c));
        }
        let mut max_count = 0u64;
        let mut max_peer = None;
        for (peer, mut events) in by_peer {
            events.sort_by_key(|(t, _)| *t);
            let mut left = 0usize;
            let mut sum = 0u64;
            for right in 0..events.len() {
                sum = sum.saturating_add(events[right].1);
                while left <= right
                    && events[right]
                        .0
                        .saturating_duration_since(events[left].0)
                        > OUTBOUND_BLOCKS_WINDOW
                {
                    sum = sum.saturating_sub(events[left].1);
                    left = left.saturating_add(1);
                }
                if sum > max_count {
                    max_count = sum;
                    max_peer = Some(peer);
                }
            }
        }
        (max_count, max_peer)
    }

    /// Whether every peer stayed inside the outbound bound over recorded samples.
    #[must_use]
    pub fn within_bound(&self) -> bool {
        self.max_in_any_window().0 <= OUTBOUND_BLOCKS_CAPACITY
    }

    /// Drop a peer's live window on disconnect (counters retained for post-run).
    pub fn on_peer_disconnected(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn peer(n: u8) -> PeerId {
        static MAP: OnceLock<Mutex<HashMap<u8, PeerId>>> = OnceLock::new();
        let map = MAP.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock().unwrap();
        *guard.entry(n).or_insert_with(PeerId::random)
    }

    #[test]
    fn refuses_above_128_blocks_in_window() {
        let mut budget = OutboundBlockBudget::new();
        let p = peer(1);
        let t0 = Instant::now();
        // Two 64-slot batches = 128 — exact capacity.
        assert!(budget.try_reserve(p, 64, t0));
        assert!(budget.try_reserve(p, 64, t0));
        // Third would exceed.
        assert!(!budget.try_reserve(p, 1, t0));
        assert_eq!(budget.total_requested(p), 128);
        assert!(budget.within_bound());
    }

    #[test]
    fn max_in_any_window_detects_concentration() {
        let mut budget = OutboundBlockBudget::new();
        let deep = peer(1);
        let other = peer(2);
        let t0 = Instant::now();
        // Spread across peers: each under the cap.
        assert!(budget.try_reserve(deep, 64, t0));
        assert!(budget.try_reserve(other, 64, t0));
        assert!(budget.try_reserve(deep, 64, t0)); // deep = 128
        let (max, who) = budget.max_in_any_window();
        assert_eq!(max, 128);
        assert_eq!(who, Some(deep));
        assert!(budget.within_bound());

        // After the window slides, more budget is available.
        let t1 = t0 + OUTBOUND_BLOCKS_WINDOW + Duration::from_millis(1);
        assert!(budget.try_reserve(deep, 64, t1));
        // Across the whole run deep has 192, but no 10 s window exceeds 128.
        assert!(
            budget.within_bound(),
            "max in window = {}",
            budget.max_in_any_window().0
        );
        assert_eq!(budget.total_requested(deep), 192);
    }

    #[test]
    fn simulated_10_minute_run_stays_inside_bound() {
        // CC-47 /5 unit form: drive the budget at full rate for 10 minutes of
        // simulated time against our own counters. The limiter itself is the
        // enforcement; counters prove no window exceeded 128.
        let mut budget = OutboundBlockBudget::new();
        let peers = [peer(10), peer(11), peer(12), peer(13)];
        let t0 = Instant::now();
        let run = Duration::from_secs(600); // 10 minutes
        let step = Duration::from_secs(1);
        let mut t = t0;
        let mut peer_idx = 0usize;
        while t.duration_since(t0) < run {
            // Attempt a 64-slot batch on a rotating peer every second.
            let p = peers[peer_idx % peers.len()];
            peer_idx = peer_idx.saturating_add(1);
            let _ = budget.try_reserve(p, 64, t);
            t += step;
        }
        let (max, _) = budget.max_in_any_window();
        assert!(
            max <= OUTBOUND_BLOCKS_CAPACITY,
            "outbound max-in-window {max} exceeds {OUTBOUND_BLOCKS_CAPACITY}"
        );
        assert!(budget.within_bound());
        // Distribution recorded (non-empty per-peer totals).
        let totals = budget.per_peer_totals();
        assert_eq!(totals.len(), 4);
        let sum: u64 = totals.iter().map(|(_, c)| c).sum();
        assert!(sum > 0);
        // No single peer took the entire sum (rotation works).
        assert!(totals[0].1 < sum);
    }

    #[test]
    fn empty_budget_is_within_bound() {
        let budget = OutboundBlockBudget::new();
        assert!(budget.within_bound());
        assert_eq!(budget.max_in_any_window(), (0, None));
    }

    #[test]
    fn refund_restores_window_budget_on_local_cancel() {
        let mut budget = OutboundBlockBudget::new();
        let p = peer(7);
        let t0 = Instant::now();
        assert!(budget.try_reserve(p, 64, t0));
        assert!(budget.try_reserve(p, 64, t0));
        assert_eq!(budget.available(p, t0), 0);
        // Local schedule failure: request never went out.
        assert!(budget.refund(p, 64, t0));
        assert_eq!(budget.available(p, t0), 64);
        // Can re-reserve the refunded slice.
        assert!(budget.try_reserve(p, 64, t0));
        assert!(!budget.try_reserve(p, 1, t0));
        // Cumulative attempts still count both reserves (not reduced by refund).
        assert_eq!(budget.total_requested(p), 192);
        assert!(budget.within_bound());
    }
}
