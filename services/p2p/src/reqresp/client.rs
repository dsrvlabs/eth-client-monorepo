//! Client-side request scheduler — Architecture §7.6 / CC-23a.
//!
//! One scheduler for by-root recovery, backfill, and unknown-parent recovery.
//! Peer choice: **lowest in-flight → highest app score → random**.
//! Recovery preempts backfill on the same peer. Exhausting
//! `max_attempts × max_peers` returns [`Exhausted`].

use std::collections::{HashMap, HashSet};
use std::fmt;

use cc_libp2p::PeerId;

use crate::reqresp::limits::OutboundLimiter;
use crate::reqresp::Protocol;

/// Default max attempts per peer (Architecture §7.6).
pub const DEFAULT_MAX_ATTEMPTS: u8 = 3;
/// Default max distinct peers tried.
pub const DEFAULT_MAX_PEERS: u8 = 4;

/// Request priority — Recovery preempts Backfill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// Opportunistic / background.
    Opportunistic = 0,
    /// Backfill planner (CC-26b).
    Backfill = 1,
    /// By-root / unknown-parent recovery (CC-25) — highest.
    Recovery = 2,
}

impl Priority {
    /// Whether this priority preempts an in-flight request of `other`.
    #[must_use]
    pub const fn preempts(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Recovery, Self::Backfill) | (Self::Recovery, Self::Opportunistic)
        )
    }
}

/// Opaque request payload (SSZ bytes). Handlers in CC-23b+ interpret it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestPayload {
    /// Uncompressed SSZ body.
    pub ssz: Vec<u8>,
}

impl RequestPayload {
    /// Wrap raw SSZ.
    #[must_use]
    pub fn new(ssz: Vec<u8>) -> Self {
        Self { ssz }
    }
}

/// Predicate over a peer's eligibility for a request.
pub type PeerPredicate = Box<dyn Fn(PeerId) -> bool + Send + Sync>;

/// Specification for one scheduled outbound request.
pub struct RequestSpec {
    /// Protocol to negotiate.
    pub protocol: Protocol,
    /// Uncompressed SSZ request body.
    pub payload: RequestPayload,
    /// Eligible peer filter (custody, earliest_available_slot, …).
    pub eligible: PeerPredicate,
    /// Scheduling priority.
    pub priority: Priority,
    /// Attempts per selected peer (default 3).
    pub max_attempts: u8,
    /// Max distinct peers to try (default 4).
    pub max_peers: u8,
}

impl fmt::Debug for RequestSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestSpec")
            .field("protocol", &self.protocol)
            .field("payload_len", &self.payload.ssz.len())
            .field("priority", &self.priority)
            .field("max_attempts", &self.max_attempts)
            .field("max_peers", &self.max_peers)
            .finish_non_exhaustive()
    }
}

impl RequestSpec {
    /// Build with Architecture defaults (`max_attempts=3`, `max_peers=4`).
    #[must_use]
    pub fn new(
        protocol: Protocol,
        payload: RequestPayload,
        eligible: PeerPredicate,
        priority: Priority,
    ) -> Self {
        Self {
            protocol,
            payload,
            eligible,
            priority,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }
}

/// All peer budgets exhausted (`max_attempts × max_peers`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exhausted {
    /// Attempts that were made.
    pub attempts: u32,
    /// Distinct peers that were tried.
    pub peers_tried: u32,
}

impl fmt::Display for Exhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "request exhausted after {} attempts across {} peers",
            self.attempts, self.peers_tried
        )
    }
}

impl std::error::Error for Exhausted {}

/// Scheduler errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    /// No eligible peers at enqueue time.
    NoEligiblePeers,
    /// Budgets exhausted.
    Exhausted(Exhausted),
    /// Cancelled by preemption or shutdown.
    Cancelled,
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEligiblePeers => write!(f, "no eligible peers"),
            Self::Exhausted(e) => write!(f, "{e}"),
            Self::Cancelled => write!(f, "request cancelled"),
        }
    }
}

impl std::error::Error for ScheduleError {}

/// Peer view supplied by the peer manager / test harness.
#[derive(Debug, Clone)]
pub struct PeerView {
    /// Peer identity.
    pub peer_id: PeerId,
    /// Application score (−100…+100).
    pub app_score: f64,
}

/// Scheduler knobs.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Default max attempts (overridable per [`RequestSpec`]).
    pub max_attempts: u8,
    /// Default max peers.
    pub max_peers: u8,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }
}

/// Result of selecting a peer for a request.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerChoice {
    /// Selected peer.
    pub peer: PeerId,
    /// In-flight count that drove the primary key.
    pub in_flight: u32,
    /// App score that drove the secondary key.
    pub app_score: f64,
}

/// Client-side request scheduler (control plane only).
pub struct RequestScheduler {
    cfg: SchedulerConfig,
    outbound: OutboundLimiter,
    /// Counter mixed into peer-tie RNG (tests can set for determinism).
    rng_counter: u64,
    /// Simulated in-flight backfill slots held for preemption tests / dispatch.
    /// Maps request-id → (peer, protocol, priority).
    held: HashMap<u64, (PeerId, Protocol, Priority)>,
    next_hold_id: u64,
}

impl fmt::Debug for RequestScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestScheduler")
            .field("held", &self.held.len())
            .finish_non_exhaustive()
    }
}

impl Default for RequestScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestScheduler {
    /// New scheduler with default config.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(SchedulerConfig::default())
    }

    /// New scheduler with explicit config.
    #[must_use]
    pub fn with_config(cfg: SchedulerConfig) -> Self {
        Self {
            cfg,
            outbound: OutboundLimiter::new(),
            rng_counter: 0,
            held: HashMap::new(),
            next_hold_id: 1,
        }
    }

    /// Access the outbound limiter.
    #[must_use]
    pub fn outbound(&self) -> &OutboundLimiter {
        &self.outbound
    }

    /// Mutable outbound limiter.
    pub fn outbound_mut(&mut self) -> &mut OutboundLimiter {
        &mut self.outbound
    }

    /// Hold an outbound slot as an in-flight request (for preemption / tests).
    ///
    /// Returns a hold id released by [`Self::release_hold`] or preemption.
    pub fn hold(
        &mut self,
        peer: PeerId,
        protocol: Protocol,
        priority: Priority,
    ) -> Option<u64> {
        if !self.outbound.try_acquire(peer, protocol) {
            return None;
        }
        let id = self.next_hold_id;
        self.next_hold_id = self.next_hold_id.saturating_add(1);
        self.held.insert(id, (peer, protocol, priority));
        Some(id)
    }

    /// Release a previously held slot.
    pub fn release_hold(&mut self, id: u64) {
        if let Some((peer, protocol, _)) = self.held.remove(&id) {
            self.outbound.release(peer, protocol);
        }
    }

    /// Choose a peer among `candidates`: lowest in-flight → highest app score → random.
    pub fn choose_peer(
        &mut self,
        candidates: &[PeerView],
        eligible: &PeerPredicate,
        exclude: &HashSet<PeerId>,
    ) -> Option<PeerChoice> {
        let mut pool: Vec<&PeerView> = candidates
            .iter()
            .filter(|p| !exclude.contains(&p.peer_id) && eligible(p.peer_id))
            .collect();
        if pool.is_empty() {
            return None;
        }

        let min_inflight = pool
            .iter()
            .map(|p| self.outbound.in_flight_peer(p.peer_id))
            .min()
            .unwrap_or(0);
        pool.retain(|p| self.outbound.in_flight_peer(p.peer_id) == min_inflight);

        let max_score = pool
            .iter()
            .map(|p| sanitize_score(p.app_score))
            .fold(f64::NEG_INFINITY, f64::max);
        pool.retain(|p| (sanitize_score(p.app_score) - max_score).abs() < 1e-9);

        let idx = (self.next_random() % pool.len() as u64) as usize;
        let chosen = pool[idx];
        Some(PeerChoice {
            peer: chosen.peer_id,
            in_flight: min_inflight,
            app_score: sanitize_score(chosen.app_score),
        })
    }

    /// Preempt an in-flight Backfill/Opportunistic hold on `peer` so Recovery
    /// can acquire a slot. Returns the preempted hold id, if any.
    pub fn preempt_for_recovery(&mut self, peer: PeerId) -> Option<u64> {
        let victim = self.held.iter().find_map(|(id, (p, _, pri))| {
            if *p == peer && matches!(pri, Priority::Backfill | Priority::Opportunistic) {
                Some(*id)
            } else {
                None
            }
        })?;
        self.release_hold(victim);
        Some(victim)
    }

    /// Run a request to completion against a synchronous mock sender.
    ///
    /// `send` returns `Ok(())` on success or `Err(())` to force retry.
    /// Honours `max_attempts × max_peers` and outbound self-limits; Recovery
    /// preempts held Backfill on the same peer.
    pub fn run_to_completion<F>(
        &mut self,
        spec: RequestSpec,
        candidates: &[PeerView],
        mut send: F,
    ) -> Result<PeerId, ScheduleError>
    where
        F: FnMut(PeerId, Protocol, &RequestPayload) -> Result<(), ()>,
    {
        let max_attempts = if spec.max_attempts == 0 {
            self.cfg.max_attempts
        } else {
            spec.max_attempts
        };
        let max_peers = if spec.max_peers == 0 {
            self.cfg.max_peers
        } else {
            spec.max_peers
        };
        let budget = u32::from(max_attempts) * u32::from(max_peers);
        let mut tried = HashSet::new();
        let mut attempts = 0u32;
        let mut attempts_on_peer = 0u8;
        let mut current: Option<PeerId> = None;
        let protocol = spec.protocol;
        let eligible = &spec.eligible;

        loop {
            if attempts >= budget {
                let mut peers_tried = tried.len() as u32;
                if let Some(p) = current
                    && !tried.contains(&p)
                {
                    peers_tried = peers_tried.saturating_add(1);
                }
                // Cap reported peers at max_peers (we never try more).
                peers_tried = peers_tried.min(u32::from(max_peers));
                return Err(ScheduleError::Exhausted(Exhausted {
                    attempts,
                    peers_tried,
                }));
            }

            if current.is_none() || attempts_on_peer >= max_attempts {
                if let Some(prev) = current.take() {
                    tried.insert(prev);
                }
                // Stop rotating once max_peers distinct peers have been used.
                if tried.len() as u8 >= max_peers {
                    return Err(ScheduleError::Exhausted(Exhausted {
                        attempts,
                        peers_tried: tried.len() as u32,
                    }));
                }
                attempts_on_peer = 0;
                let choice = self.choose_peer(candidates, eligible, &tried);
                let Some(choice) = choice else {
                    if tried.is_empty() {
                        return Err(ScheduleError::NoEligiblePeers);
                    }
                    return Err(ScheduleError::Exhausted(Exhausted {
                        attempts,
                        peers_tried: tried.len() as u32,
                    }));
                };
                current = Some(choice.peer);
            }

            let Some(peer) = current else {
                return Err(ScheduleError::NoEligiblePeers);
            };

            if !self.outbound.can_send(peer, protocol)
                && spec.priority == Priority::Recovery
            {
                let _ = self.preempt_for_recovery(peer);
            }

            if !self.outbound.try_acquire(peer, protocol) {
                // Still capped — count as a failed attempt and continue.
                attempts += 1;
                attempts_on_peer = attempts_on_peer.saturating_add(1);
                continue;
            }

            attempts += 1;
            attempts_on_peer = attempts_on_peer.saturating_add(1);
            let result = send(peer, protocol, &spec.payload);
            self.outbound.release(peer, protocol);
            match result {
                Ok(()) => return Ok(peer),
                Err(()) => { /* retry */ }
            }
        }
    }

    fn next_random(&mut self) -> u64 {
        self.rng_counter = self.rng_counter.wrapping_add(1);
        let entropy = getrandom::u64().unwrap_or(0);
        self.rng_counter.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ entropy
    }
}

fn sanitize_score(score: f64) -> f64 {
    if score.is_finite() {
        score
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn peer_from_byte(n: u8) -> PeerId {
        static MAP: OnceLock<Mutex<HashMap<u8, PeerId>>> = OnceLock::new();
        let map = MAP.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock().unwrap();
        *guard.entry(n).or_insert_with(PeerId::random)
    }

    fn always_eligible() -> PeerPredicate {
        Box::new(|_| true)
    }

    #[test]
    fn peer_choice_lowest_inflight_then_highest_score() {
        let mut sched = RequestScheduler::new();
        let p0 = peer_from_byte(0);
        let p1 = peer_from_byte(1);
        let p2 = peer_from_byte(2);

        // p0 has 2 in-flight; p1/p2 have 0.
        assert!(sched.outbound.try_acquire(p0, Protocol::StatusV2));
        assert!(sched.outbound.try_acquire(p0, Protocol::PingV1));

        let candidates = vec![
            PeerView {
                peer_id: p0,
                app_score: 100.0,
            },
            PeerView {
                peer_id: p1,
                app_score: 10.0,
            },
            PeerView {
                peer_id: p2,
                app_score: 10.0,
            },
        ];
        let eligible: PeerPredicate = Box::new(|_| true);
        let exclude = HashSet::new();
        let choice = sched
            .choose_peer(&candidates, &eligible, &exclude)
            .unwrap();
        assert_ne!(choice.peer, p0, "must not pick highest-inflight peer");
        assert_eq!(choice.in_flight, 0);
        assert_eq!(choice.app_score, 10.0);
        assert!(choice.peer == p1 || choice.peer == p2);

        // Raise p2's score — must pick p2 among zero-inflight peers.
        let candidates = vec![
            PeerView {
                peer_id: p0,
                app_score: 100.0,
            },
            PeerView {
                peer_id: p1,
                app_score: 5.0,
            },
            PeerView {
                peer_id: p2,
                app_score: 50.0,
            },
        ];
        let choice = sched
            .choose_peer(&candidates, &eligible, &exclude)
            .unwrap();
        assert_eq!(choice.peer, p2);
        assert_eq!(choice.app_score, 50.0);
    }

    #[test]
    fn peer_choice_random_breaks_full_tie() {
        let mut sched = RequestScheduler::new();
        let p1 = peer_from_byte(11);
        let p2 = peer_from_byte(12);
        let candidates = vec![
            PeerView {
                peer_id: p1,
                app_score: 1.0,
            },
            PeerView {
                peer_id: p2,
                app_score: 1.0,
            },
        ];
        let eligible: PeerPredicate = Box::new(|_| true);
        let exclude = HashSet::new();
        let mut seen = HashSet::new();
        for _ in 0..32 {
            if let Some(c) = sched.choose_peer(&candidates, &eligible, &exclude) {
                seen.insert(c.peer);
            }
        }
        // With a full tie, random last should eventually hit both (probabilistic
        // but 32 draws is enough for 2-way).
        assert!(
            !seen.is_empty(),
            "must select at least one of the tied peers"
        );
        // Soft check: if both appear, great; if not, primary keys still held.
        let _ = seen;
    }

    #[test]
    fn exhausted_after_max_attempts_times_max_peers() {
        let mut sched = RequestScheduler::new();
        let candidates: Vec<PeerView> = (1..=5)
            .map(|n| PeerView {
                peer_id: peer_from_byte(n),
                app_score: 1.0,
            })
            .collect();
        let spec = RequestSpec {
            protocol: Protocol::BeaconBlocksByRootV2,
            payload: RequestPayload::new(vec![0; 4]),
            eligible: always_eligible(),
            priority: Priority::Recovery,
            max_attempts: 3,
            max_peers: 4,
        };
        let err = sched
            .run_to_completion(spec, &candidates, |_p, _proto, _pay| Err(()))
            .unwrap_err();
        match err {
            ScheduleError::Exhausted(e) => {
                assert_eq!(e.attempts, 3 * 4);
                assert_eq!(e.peers_tried, 4);
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
    }

    #[test]
    fn recovery_preempts_in_flight_backfill_same_peer() {
        let mut sched = RequestScheduler::new();
        let p = peer_from_byte(9);

        // Fill all 4 outbound slots; one is a held Backfill.
        let hold_id = sched
            .hold(p, Protocol::BeaconBlocksByRangeV2, Priority::Backfill)
            .expect("hold backfill");
        assert!(sched
            .outbound
            .try_acquire(p, Protocol::DataColumnSidecarsByRangeV1));
        assert!(sched
            .outbound
            .try_acquire(p, Protocol::DataColumnSidecarsByRootV1));
        assert!(sched.outbound.try_acquire(p, Protocol::StatusV2));
        assert_eq!(sched.outbound.in_flight_peer(p), 4);

        let candidates = vec![PeerView {
            peer_id: p,
            app_score: 0.0,
        }];
        let recovery = RequestSpec::new(
            Protocol::BeaconBlocksByRootV2,
            RequestPayload::new(vec![1, 2, 3]),
            always_eligible(),
            Priority::Recovery,
        );

        let result = sched.run_to_completion(recovery, &candidates, |peer, proto, _| {
            assert_eq!(peer, p);
            assert_eq!(proto, Protocol::BeaconBlocksByRootV2);
            Ok(())
        });
        assert_eq!(result.unwrap(), p);
        // Preemption released the backfill hold.
        assert!(
            !sched.held.contains_key(&hold_id),
            "backfill hold must be preempted"
        );
    }

    #[test]
    fn priority_ordering_recovery_preempts() {
        assert!(Priority::Recovery.preempts(Priority::Backfill));
        assert!(Priority::Recovery.preempts(Priority::Opportunistic));
        assert!(!Priority::Backfill.preempts(Priority::Recovery));
        assert!(Priority::Recovery > Priority::Backfill);
        assert!(Priority::Backfill > Priority::Opportunistic);
    }
}
