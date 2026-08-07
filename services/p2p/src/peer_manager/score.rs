//! Score *fields* and thresholds used by the peer manager (Architecture §3.7).
//!
//! **Stream G (CC-22c) owns this file's semantics** — the two score spaces'
//! meaning, the damped one-way coupling, the penalty table, and
//! `cc_p2p_peer_penalty_total{reason}` producers. This module only holds the
//! field defaults and the thresholds the mechanics (disconnect / ban /
//! mesh-protection) need so Stream H does not invent numbers locally.
//!
//! Coupling term and penalty producers land in CC-22c; do not expand this
//! beyond constants + pure helpers without coordinating Stream G.

/// Default GossipSub peer score (libp2p range ≈ −16000…+30).
pub const DEFAULT_GOSSIP_SCORE: f64 = 0.0;

/// Default application score (−100…+100).
pub const DEFAULT_APP_SCORE: f64 = 0.0;

/// Disconnect when `app_score` is strictly below this (Architecture §3.7).
pub const APP_SCORE_DISCONNECT: f64 = -20.0;

/// Ban (and block-list) when `app_score` is strictly below this.
pub const APP_SCORE_BAN: f64 = -50.0;

/// GossipSub `GossipThreshold` — mesh members below this lose eviction protection.
///
/// Verbatim from Architecture §5.6 / scoring research note.
pub const GOSSIP_THRESHOLD: f64 = -4000.0;

/// Whether a mesh member is protected from eviction.
///
/// Mesh members are protected **unless** their gossip score is below
/// [`GOSSIP_THRESHOLD`]. This is **not** a disconnect condition — it only
/// gates eviction candidate selection. No path disconnects on `gossip_score`.
#[must_use]
pub fn is_mesh_protected(in_mesh: bool, gossip_score: f64) -> bool {
    in_mesh && gossip_score >= GOSSIP_THRESHOLD
}

// ── CC-22c will append: coupling term, penalty table, producers ─────────────
