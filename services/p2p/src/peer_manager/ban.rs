//! Per-peer dial backoff and ban bookkeeping (Architecture §3.6).
//!
//! Backoff schedule: **30 s → 2× → 10 min cap**, reset on successful connect.
//! Ban decision is driven by `app_score < APP_SCORE_BAN` (wired in the manager);
//! this module owns the state and the allow-block-list *policy hooks* (which
//! peer / when). Swarm-level enforcement is `allow_block_list::Behaviour`.

use std::time::{Duration, Instant};

/// Initial dial backoff after the first failure.
pub const DIAL_BACKOFF_INITIAL: Duration = Duration::from_secs(30);
/// Multiplier applied after each successive failure.
pub const DIAL_BACKOFF_FACTOR: u32 = 2;
/// Cap on dial backoff.
pub const DIAL_BACKOFF_MAX: Duration = Duration::from_secs(10 * 60);

/// Per-peer dial backoff state.
#[derive(Debug, Clone)]
pub struct DialBackoff {
    /// Consecutive dial failures since last success (0 = clean).
    pub failures: u32,
    /// Earliest instant a re-dial is allowed (`None` = free to dial).
    pub next_attempt: Option<Instant>,
    /// Last computed delay (for tests / metrics).
    pub last_delay: Duration,
}

impl Default for DialBackoff {
    fn default() -> Self {
        Self {
            failures: 0,
            next_attempt: None,
            last_delay: Duration::ZERO,
        }
    }
}

impl DialBackoff {
    /// Whether a dial is allowed at `now`.
    #[must_use]
    pub fn ready(&self, now: Instant) -> bool {
        match self.next_attempt {
            None => true,
            Some(t) => now >= t,
        }
    }

    /// Record a dial failure and advance backoff. Returns the delay applied.
    pub fn on_failure(&mut self, now: Instant) -> Duration {
        self.failures = self.failures.saturating_add(1);
        let delay = delay_for_failures(self.failures);
        self.last_delay = delay;
        self.next_attempt = Some(now + delay);
        delay
    }

    /// Successful connection resets backoff.
    pub fn on_success(&mut self) {
        self.failures = 0;
        self.next_attempt = None;
        self.last_delay = Duration::ZERO;
    }
}

/// Delay after `failures` consecutive failures (1-based count after the call).
///
/// - 1st failure → 30 s
/// - 2nd → 60 s
/// - 3rd → 120 s
/// - 4th → 240 s
/// - … capped at 10 min
#[must_use]
pub fn delay_for_failures(failures: u32) -> Duration {
    if failures == 0 {
        return Duration::ZERO;
    }
    let shift = failures.saturating_sub(1).min(16);
    let secs = DIAL_BACKOFF_INITIAL
        .as_secs()
        .saturating_mul(u64::from(DIAL_BACKOFF_FACTOR).saturating_pow(shift));
    Duration::from_secs(secs).min(DIAL_BACKOFF_MAX)
}

/// Ban bookkeeping for peers the manager has decided to refuse.
///
/// The manager owns *which* peers land here; the swarm's
/// `allow_block_list::Behaviour` is the hard enforcement surface.
#[derive(Debug, Default, Clone)]
pub struct BanList {
    peers: std::collections::HashSet<cc_libp2p::PeerId>,
}

impl BanList {
    /// Insert a peer into the ban set. Returns `true` if newly banned.
    pub fn ban(&mut self, peer: cc_libp2p::PeerId) -> bool {
        self.peers.insert(peer)
    }

    /// Whether `peer` is currently banned.
    #[must_use]
    pub fn is_banned(&self, peer: &cc_libp2p::PeerId) -> bool {
        self.peers.contains(peer)
    }

    /// Number of banned peers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Empty ban set?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Iterate banned peer ids.
    pub fn iter(&self) -> impl Iterator<Item = &cc_libp2p::PeerId> {
        self.peers.iter()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn backoff_schedule_30s_double_10min_cap() {
        assert_eq!(delay_for_failures(0), Duration::ZERO);
        assert_eq!(delay_for_failures(1), Duration::from_secs(30));
        assert_eq!(delay_for_failures(2), Duration::from_secs(60));
        assert_eq!(delay_for_failures(3), Duration::from_secs(120));
        // Fourth failure → 240 s
        assert_eq!(delay_for_failures(4), Duration::from_secs(240));
        // Keep doubling until cap
        assert_eq!(delay_for_failures(5), Duration::from_secs(480));
        assert_eq!(delay_for_failures(6), Duration::from_secs(600)); // 960 capped
        assert_eq!(delay_for_failures(10), DIAL_BACKOFF_MAX);
    }

    #[test]
    fn failure_advances_and_success_resets() {
        let t0 = Instant::now();
        let mut b = DialBackoff::default();
        assert!(b.ready(t0));

        let d1 = b.on_failure(t0);
        assert_eq!(d1, Duration::from_secs(30));
        assert!(!b.ready(t0 + Duration::from_secs(29)));
        assert!(b.ready(t0 + Duration::from_secs(30)));

        let d2 = b.on_failure(t0 + Duration::from_secs(30));
        assert_eq!(d2, Duration::from_secs(60));
        assert_eq!(b.failures, 2);

        b.on_success();
        assert_eq!(b.failures, 0);
        assert!(b.ready(t0));
        assert!(b.next_attempt.is_none());
    }

    #[test]
    fn fourth_failure_delay_is_240s() {
        let t0 = Instant::now();
        let mut b = DialBackoff::default();
        for _ in 0..4 {
            b.on_failure(t0);
        }
        assert_eq!(b.failures, 4);
        assert_eq!(b.last_delay, Duration::from_secs(240));
    }
}
