//! Req/resp rate limiting — Architecture §7.3 / CC-23/5.
//!
//! | Direction | Bucket | On violation |
//! |---|---|---|
//! | inbound, per peer | 128 blocks / 10 s; 1 024 columns / 10 s | error response (code 2), metric, −5 score |
//! | inbound, global | 10× per-peer | same, `peer_kind="global"` |
//! | outbound, per peer | 1 in-flight / protocol / peer; 4 across all | queued in scheduler |
//!
//! Cost is on **chunks served**, not requests. When the bucket empties mid-stream
//! the response is **truncated** (valid short success stream), never errored mid-chunk.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cc_libp2p::PeerId;

use crate::metrics::{P2pMetrics, PeerPenaltyReason};
use crate::peer_manager::score::apply_penalty_with_metrics;
use crate::reqresp::Protocol;
use crate::reqresp::codec::{ResponseChunk, ResponseCode};

/// Per-peer inbound block budget (Architecture §7.3).
pub const INBOUND_BLOCKS_CAPACITY: u64 = 128;
/// Per-peer inbound column-sidecar budget.
pub const INBOUND_COLUMNS_CAPACITY: u64 = 1_024;
/// Refill window for both inbound buckets.
pub const INBOUND_WINDOW: Duration = Duration::from_secs(10);
/// Global inbound multiplier over the per-peer capacity.
pub const GLOBAL_MULTIPLIER: u64 = 10;
/// Outbound: at most one in-flight request per protocol per peer.
pub const OUTBOUND_MAX_IN_FLIGHT_PER_PROTOCOL: u32 = 1;
/// Outbound: at most four in-flight requests across all protocols to one peer.
pub const OUTBOUND_MAX_IN_FLIGHT_PER_PEER: u32 = 4;

/// Error message body for rate-limit responses (code 2 `ServerError`).
pub const RATE_LIMIT_ERROR_MESSAGE: &[u8] = b"rate limit exceeded";

/// Which inbound resource was limited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitKind {
    /// Beacon block chunks.
    Blocks,
    /// Data-column sidecar chunks.
    Columns,
}

impl RateLimitKind {
    /// Prometheus protocol label fragment.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Columns => "columns",
        }
    }

    /// Capacity for the per-peer bucket.
    #[must_use]
    pub const fn per_peer_capacity(self) -> u64 {
        match self {
            Self::Blocks => INBOUND_BLOCKS_CAPACITY,
            Self::Columns => INBOUND_COLUMNS_CAPACITY,
        }
    }

    /// Capacity for the global bucket.
    #[must_use]
    pub const fn global_capacity(self) -> u64 {
        self.per_peer_capacity().saturating_mul(GLOBAL_MULTIPLIER)
    }
}

/// Continuous-refill token bucket.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    /// Tokens added per second.
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    /// Build a bucket that holds `capacity` tokens and refills fully every `window`.
    #[must_use]
    pub fn new(capacity: u64, window: Duration) -> Self {
        let capacity = capacity as f64;
        let secs = window.as_secs_f64().max(f64::EPSILON);
        Self {
            capacity,
            tokens: capacity,
            refill_per_sec: capacity / secs,
            last: Instant::now(),
        }
    }

    /// Refill based on elapsed time (continuous).
    pub fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last = now;
        }
    }

    /// Current whole tokens available (after refill to `now`).
    pub fn available(&mut self, now: Instant) -> u64 {
        self.refill(now);
        self.tokens.max(0.0).floor() as u64
    }

    /// Try to debit `n` tokens. Returns `true` if granted.
    pub fn try_acquire(&mut self, n: u64, now: Instant) -> bool {
        self.refill(now);
        let need = n as f64;
        if self.tokens + f64::EPSILON >= need {
            self.tokens -= need;
            true
        } else {
            false
        }
    }

    /// Debit up to `n` tokens; returns how many were actually granted.
    pub fn acquire_up_to(&mut self, n: u64, now: Instant) -> u64 {
        self.refill(now);
        let have = self.tokens.max(0.0).floor() as u64;
        let take = have.min(n);
        self.tokens -= take as f64;
        take
    }

    /// Force-empty the bucket (tests).
    pub fn drain(&mut self) {
        self.tokens = 0.0;
    }

    /// Inspect remaining fractional tokens (tests).
    #[must_use]
    pub fn tokens(&self) -> f64 {
        self.tokens
    }
}

/// Outcome of an inbound rate-limit check for one chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitOutcome {
    /// Chunk may be served; tokens debited.
    Allow,
    /// Per-peer bucket empty — return error response (first chunk) or truncate.
    PeerLimited,
    /// Global bucket empty.
    GlobalLimited,
}

impl RateLimitOutcome {
    /// Whether the chunk is denied.
    #[must_use]
    pub const fn is_limited(self) -> bool {
        !matches!(self, Self::Allow)
    }

    /// `peer_kind` label for metrics.
    #[must_use]
    pub const fn peer_kind(self) -> &'static str {
        match self {
            Self::Allow => "none",
            Self::PeerLimited => "peer",
            Self::GlobalLimited => "global",
        }
    }
}

/// Inbound per-peer + global token buckets for blocks and columns.
#[derive(Debug)]
pub struct InboundRateLimiter {
    peer_blocks: HashMap<PeerId, TokenBucket>,
    peer_columns: HashMap<PeerId, TokenBucket>,
    global_blocks: TokenBucket,
    global_columns: TokenBucket,
}

impl Default for InboundRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl InboundRateLimiter {
    /// Build with Architecture §7.3 capacities.
    #[must_use]
    pub fn new() -> Self {
        Self {
            peer_blocks: HashMap::new(),
            peer_columns: HashMap::new(),
            global_blocks: TokenBucket::new(
                RateLimitKind::Blocks.global_capacity(),
                INBOUND_WINDOW,
            ),
            global_columns: TokenBucket::new(
                RateLimitKind::Columns.global_capacity(),
                INBOUND_WINDOW,
            ),
        }
    }

    fn peer_bucket(&mut self, peer: PeerId, kind: RateLimitKind) -> &mut TokenBucket {
        let map = match kind {
            RateLimitKind::Blocks => &mut self.peer_blocks,
            RateLimitKind::Columns => &mut self.peer_columns,
        };
        map.entry(peer)
            .or_insert_with(|| TokenBucket::new(kind.per_peer_capacity(), INBOUND_WINDOW))
    }

    fn global_bucket(&mut self, kind: RateLimitKind) -> &mut TokenBucket {
        match kind {
            RateLimitKind::Blocks => &mut self.global_blocks,
            RateLimitKind::Columns => &mut self.global_columns,
        }
    }

    /// Try to serve one chunk of `kind` for `peer`. Debits both buckets on allow.
    pub fn check_chunk(
        &mut self,
        peer: PeerId,
        kind: RateLimitKind,
        now: Instant,
    ) -> RateLimitOutcome {
        // Peer first — report the more specific limit.
        if !self.peer_bucket(peer, kind).try_acquire(1, now) {
            return RateLimitOutcome::PeerLimited;
        }
        if !self.global_bucket(kind).try_acquire(1, now) {
            // Refund peer token so a global limit does not also drain peers.
            let b = self.peer_bucket(peer, kind);
            b.tokens = (b.tokens + 1.0).min(b.capacity);
            return RateLimitOutcome::GlobalLimited;
        }
        RateLimitOutcome::Allow
    }

    /// How many more chunks of `kind` can be served for `peer` right now
    /// (min of peer and global availability). Used for truncation.
    pub fn remaining_chunks(&mut self, peer: PeerId, kind: RateLimitKind, now: Instant) -> u64 {
        let peer_avail = self.peer_bucket(peer, kind).available(now);
        let global_avail = self.global_bucket(kind).available(now);
        peer_avail.min(global_avail)
    }

    /// Truncate a planned success-chunk list to what the bucket allows, debiting
    /// as we go. Never inserts an error mid-stream — an empty start yields a
    /// rate-limit **error** response instead. Truncation surfaces
    /// [`ChunkBudgetResult::Serve::limited`] so callers always record the
    /// penalty/metric (M4).
    pub fn apply_chunk_budget(
        &mut self,
        peer: PeerId,
        kind: RateLimitKind,
        planned: Vec<ResponseChunk>,
        now: Instant,
    ) -> ChunkBudgetResult {
        if planned.is_empty() {
            return ChunkBudgetResult::Serve {
                chunks: Vec::new(),
                limited: None,
            };
        }
        let mut out = Vec::with_capacity(planned.len());
        for chunk in planned {
            match self.check_chunk(peer, kind, now) {
                RateLimitOutcome::Allow => out.push(chunk),
                limited => {
                    if out.is_empty() {
                        return ChunkBudgetResult::Error {
                            outcome: limited,
                            chunk: ResponseChunk::Error {
                                code: ResponseCode::ServerError.as_u8(),
                                message: RATE_LIMIT_ERROR_MESSAGE.to_vec(),
                            },
                        };
                    }
                    // Truncate: valid short success response + limited outcome.
                    return ChunkBudgetResult::Serve {
                        chunks: out,
                        limited: Some(limited),
                    };
                }
            }
        }
        ChunkBudgetResult::Serve {
            chunks: out,
            limited: None,
        }
    }

    /// Drop per-peer buckets when a peer disconnects (M3 — no unbounded growth).
    pub fn on_peer_disconnected(&mut self, peer: PeerId) {
        self.peer_blocks.remove(&peer);
        self.peer_columns.remove(&peer);
    }

    /// Admit one **request** for a metered protocol before any chunk is produced.
    ///
    /// Returns a rate-limit error chunk when the peer or global bucket is empty
    /// (fail-closed serve path until CC-23b/c/d handlers produce real bodies).
    pub fn admit_request(
        &mut self,
        peer: PeerId,
        kind: RateLimitKind,
        now: Instant,
    ) -> Result<(), (RateLimitOutcome, ResponseChunk)> {
        // Require at least one token available for the first chunk of a response.
        match self.check_chunk(peer, kind, now) {
            RateLimitOutcome::Allow => {
                // Refund: admission peeks capacity; real debit is per chunk served.
                let b = self.peer_bucket(peer, kind);
                b.tokens = (b.tokens + 1.0).min(b.capacity);
                let g = self.global_bucket(kind);
                g.tokens = (g.tokens + 1.0).min(g.capacity);
                Ok(())
            }
            limited => Err((
                limited,
                ResponseChunk::Error {
                    code: ResponseCode::ServerError.as_u8(),
                    message: RATE_LIMIT_ERROR_MESSAGE.to_vec(),
                },
            )),
        }
    }

    /// Record a rate-limit hit: metric + −5 application score.
    pub fn record_violation(
        metrics: &P2pMetrics,
        app_score: &mut f64,
        outcome: RateLimitOutcome,
        protocol: Protocol,
    ) {
        if matches!(outcome, RateLimitOutcome::Allow) {
            return;
        }
        metrics.inc_reqresp_ratelimit(outcome.peer_kind(), protocol.as_str());
        apply_penalty_with_metrics(app_score, PeerPenaltyReason::RateLimit, metrics);
    }
}

/// Result of applying the inbound chunk budget to a planned response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkBudgetResult {
    /// Serve these success chunks (may be truncated).
    Serve {
        /// Chunks allowed under the current budget.
        chunks: Vec<ResponseChunk>,
        /// Set when the bucket emptied mid-stream (callers must record violation).
        limited: Option<RateLimitOutcome>,
    },
    /// Entire request denied before any chunk — send one error response.
    Error {
        /// Which bucket fired.
        outcome: RateLimitOutcome,
        /// Error chunk (code 2).
        chunk: ResponseChunk,
    },
}

/// Outbound self-limiting: 1 in-flight per protocol per peer, 4 across all.
#[derive(Debug, Default)]
pub struct OutboundLimiter {
    /// `(peer, protocol) -> in-flight count` (0 or 1 in practice).
    per_protocol: HashMap<(PeerId, Protocol), u32>,
    /// `peer -> total in-flight across protocols`.
    per_peer: HashMap<PeerId, u32>,
}

impl OutboundLimiter {
    /// Empty limiter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// In-flight count for one protocol on one peer.
    #[must_use]
    pub fn in_flight_protocol(&self, peer: PeerId, protocol: Protocol) -> u32 {
        self.per_protocol
            .get(&(peer, protocol))
            .copied()
            .unwrap_or(0)
    }

    /// Total in-flight across all protocols for one peer.
    #[must_use]
    pub fn in_flight_peer(&self, peer: PeerId) -> u32 {
        self.per_peer.get(&peer).copied().unwrap_or(0)
    }

    /// Whether a new request to `(peer, protocol)` is allowed under the caps.
    #[must_use]
    pub fn can_send(&self, peer: PeerId, protocol: Protocol) -> bool {
        self.in_flight_protocol(peer, protocol) < OUTBOUND_MAX_IN_FLIGHT_PER_PROTOCOL
            && self.in_flight_peer(peer) < OUTBOUND_MAX_IN_FLIGHT_PER_PEER
    }

    /// Reserve an in-flight slot. Returns `false` if caps would be exceeded.
    pub fn try_acquire(&mut self, peer: PeerId, protocol: Protocol) -> bool {
        if !self.can_send(peer, protocol) {
            return false;
        }
        *self.per_protocol.entry((peer, protocol)).or_insert(0) += 1;
        *self.per_peer.entry(peer).or_insert(0) += 1;
        true
    }

    /// Release a previously acquired slot.
    pub fn release(&mut self, peer: PeerId, protocol: Protocol) {
        if let Some(c) = self.per_protocol.get_mut(&(peer, protocol)) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.per_protocol.remove(&(peer, protocol));
            }
        }
        if let Some(c) = self.per_peer.get_mut(&peer) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.per_peer.remove(&peer);
            }
        }
    }
}

/// Map a protocol to its inbound metering kind, if any.
#[must_use]
pub fn rate_limit_kind(protocol: Protocol) -> Option<RateLimitKind> {
    if protocol.is_block_protocol() {
        Some(RateLimitKind::Blocks)
    } else if protocol.is_column_protocol() {
        Some(RateLimitKind::Columns)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_libp2p::PeerId;
    use prometheus_client::registry::Registry;

    fn peer(n: u8) -> PeerId {
        // Deterministic peer id from a keypair seed via generate + not available;
        // use random for tests — identity only needs inequality.
        let _ = n;
        PeerId::random()
    }

    #[test]
    fn per_peer_block_bucket_fires_at_128() {
        let mut lim = InboundRateLimiter::new();
        let p = peer(1);
        let now = Instant::now();
        for i in 0..INBOUND_BLOCKS_CAPACITY {
            let o = lim.check_chunk(p, RateLimitKind::Blocks, now);
            assert_eq!(o, RateLimitOutcome::Allow, "chunk {i} should allow");
        }
        assert_eq!(
            lim.check_chunk(p, RateLimitKind::Blocks, now),
            RateLimitOutcome::PeerLimited
        );
    }

    #[test]
    fn per_peer_column_bucket_fires_at_1024() {
        let mut lim = InboundRateLimiter::new();
        let p = peer(2);
        let now = Instant::now();
        for i in 0..INBOUND_COLUMNS_CAPACITY {
            assert_eq!(
                lim.check_chunk(p, RateLimitKind::Columns, now),
                RateLimitOutcome::Allow,
                "chunk {i}"
            );
        }
        assert_eq!(
            lim.check_chunk(p, RateLimitKind::Columns, now),
            RateLimitOutcome::PeerLimited
        );
    }

    #[test]
    fn global_bucket_is_10x_per_peer() {
        let mut lim = InboundRateLimiter::new();
        let now = Instant::now();
        // Drain global blocks using many peers so per-peer does not trip first.
        let mut served = 0u64;
        let mut peer_idx = 0u64;
        let target = RateLimitKind::Blocks.global_capacity();
        while served < target {
            let p = PeerId::random();
            peer_idx += 1;
            let _ = peer_idx;
            // Each peer can serve up to 128; use a fresh peer each 128.
            for _ in 0..INBOUND_BLOCKS_CAPACITY {
                match lim.check_chunk(p, RateLimitKind::Blocks, now) {
                    RateLimitOutcome::Allow => served += 1,
                    RateLimitOutcome::GlobalLimited => {
                        assert_eq!(served, target);
                        return;
                    }
                    RateLimitOutcome::PeerLimited => break,
                }
                if served >= target {
                    break;
                }
            }
        }
        // Next any peer must hit global.
        let o = lim.check_chunk(PeerId::random(), RateLimitKind::Blocks, now);
        assert_eq!(o, RateLimitOutcome::GlobalLimited);
        assert_eq!(target, INBOUND_BLOCKS_CAPACITY * GLOBAL_MULTIPLIER);
    }

    #[test]
    fn over_limit_returns_error_response_not_drop() {
        let mut lim = InboundRateLimiter::new();
        let p = peer(3);
        let now = Instant::now();
        lim.peer_bucket(p, RateLimitKind::Blocks).drain();
        let planned = vec![ResponseChunk::success_with_context(
            [0; 4],
            b"block".to_vec(),
        )];
        match lim.apply_chunk_budget(p, RateLimitKind::Blocks, planned, now) {
            ChunkBudgetResult::Error { outcome, chunk } => {
                assert_eq!(outcome, RateLimitOutcome::PeerLimited);
                match chunk {
                    ResponseChunk::Error { code, message } => {
                        assert_eq!(code, ResponseCode::ServerError.as_u8());
                        assert_eq!(message, RATE_LIMIT_ERROR_MESSAGE);
                    }
                    ResponseChunk::Success { .. } => panic!("must be error response"),
                }
            }
            ChunkBudgetResult::Serve { .. } => panic!("must not silently drop"),
        }
    }

    #[test]
    fn chunk_budget_truncates_when_bucket_empties() {
        let mut lim = InboundRateLimiter::new();
        let p = peer(4);
        let now = Instant::now();
        // Leave exactly 2 tokens.
        for _ in 0..INBOUND_BLOCKS_CAPACITY - 2 {
            assert_eq!(
                lim.check_chunk(p, RateLimitKind::Blocks, now),
                RateLimitOutcome::Allow
            );
        }
        // Request 16_384 "sidecars"/blocks — only 2 tokens left.
        let planned: Vec<_> = (0..16_384)
            .map(|i| ResponseChunk::success_with_context([i as u8; 4], vec![i as u8]))
            .collect();
        match lim.apply_chunk_budget(p, RateLimitKind::Blocks, planned, now) {
            ChunkBudgetResult::Serve { chunks, limited } => {
                assert_eq!(chunks.len(), 2, "must truncate to remaining tokens");
                assert_eq!(limited, Some(RateLimitOutcome::PeerLimited));
            }
            ChunkBudgetResult::Error { .. } => panic!("should truncate, not error mid-stream"),
        }
    }

    #[test]
    fn peer_disconnect_prunes_buckets() {
        let mut lim = InboundRateLimiter::new();
        let p = peer(6);
        let now = Instant::now();
        // Drain peer bucket fully.
        for _ in 0..INBOUND_BLOCKS_CAPACITY {
            assert_eq!(
                lim.check_chunk(p, RateLimitKind::Blocks, now),
                RateLimitOutcome::Allow
            );
        }
        assert_eq!(
            lim.check_chunk(p, RateLimitKind::Blocks, now),
            RateLimitOutcome::PeerLimited
        );
        lim.on_peer_disconnected(p);
        // Fresh peer row after prune → full capacity again.
        assert_eq!(
            lim.check_chunk(p, RateLimitKind::Blocks, now),
            RateLimitOutcome::Allow
        );
    }

    #[test]
    fn rate_limit_increments_metric_and_penalty() {
        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let mut score = 0.0_f64;
        InboundRateLimiter::record_violation(
            &metrics,
            &mut score,
            RateLimitOutcome::PeerLimited,
            Protocol::BeaconBlocksByRangeV2,
        );
        assert_eq!(
            metrics.reqresp_ratelimit_count("peer", "beacon_blocks_by_range"),
            1
        );
        assert_eq!(metrics.peer_penalty_count(PeerPenaltyReason::RateLimit), 1);
        assert!((score - (-5.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn outbound_caps_one_per_protocol_and_four_overall() {
        let mut out = OutboundLimiter::new();
        let p = peer(5);
        let protocols = [
            Protocol::StatusV2,
            Protocol::PingV1,
            Protocol::BeaconBlocksByRangeV2,
            Protocol::BeaconBlocksByRootV2,
            Protocol::DataColumnSidecarsByRangeV1,
        ];
        // First four distinct protocols OK.
        for proto in &protocols[..4] {
            assert!(out.try_acquire(p, *proto), "{proto:?}");
        }
        // Fifth protocol on same peer blocked by overall cap 4.
        assert!(!out.try_acquire(p, protocols[4]));
        // Same protocol again blocked by per-protocol cap 1.
        assert!(!out.try_acquire(p, protocols[0]));
        // Release one and retry.
        out.release(p, protocols[0]);
        assert!(out.try_acquire(p, protocols[4]));
        assert_eq!(out.in_flight_peer(p), 4);
    }
}
