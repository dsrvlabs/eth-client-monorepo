//! Application score space, damped GossipSub coupling, and penalty table
//! (Architecture §3.7 / CC-22c).
//!
//! **Stream G owns this file's semantics** inside Stream H's peer-manager
//! directory: the two score spaces' meaning, the never-positive damped
//! coupling, the penalty table, and `cc_p2p_peer_penalty_total{reason}`
//! producers.
//!
//! | Space | Range | Owner | Drives |
//! |---|---|---|---|
//! | GossipSub score | ≈ −16000 … +30 | `libp2p-gossipsub` | mesh / gossip / graylist |
//! | Application score | −100 … +100 | **this module** | disconnect (`< −20`), ban (`< −50`), eviction, P5 |
//!
//! Coupling is **one-way and damped**:
//! `app += clamp(gossip_score / 1000, −5, 0)` per decay interval — **never
//! positive**. Disconnecting on the raw GossipSub score is the death spiral
//! (ADR P2-09).

use crate::gossip::scoring::GOSSIP_THRESHOLD as SCORING_GOSSIP_THRESHOLD;
use crate::metrics::{P2pMetrics, PeerPenaltyReason};

// ── field defaults ──────────────────────────────────────────────────────────

/// Default GossipSub peer score (libp2p range ≈ −16000…+30).
pub const DEFAULT_GOSSIP_SCORE: f64 = 0.0;

/// Default application score (−100…+100).
pub const DEFAULT_APP_SCORE: f64 = 0.0;

/// Application-score floor.
pub const APP_SCORE_MIN: f64 = -100.0;
/// Application-score ceiling.
pub const APP_SCORE_MAX: f64 = 100.0;

/// Disconnect when `app_score` is strictly below this (Architecture §3.7).
pub const APP_SCORE_DISCONNECT: f64 = -20.0;

/// Ban (and block-list) when `app_score` is strictly below this.
pub const APP_SCORE_BAN: f64 = -50.0;

/// GossipSub `GossipThreshold` — mesh members below this lose eviction
/// protection. Value is owned by [`crate::gossip::scoring`] (CC-22/2).
pub const GOSSIP_THRESHOLD: f64 = SCORING_GOSSIP_THRESHOLD;

/// Per-slot application-score decay toward 0 (`×0.98`).
pub const APP_SCORE_DECAY: f64 = 0.98;

/// Useful delivery reward (+1, capped at [`APP_SCORE_MAX`]).
pub const USEFUL_DELIVERY_DELTA: f64 = 1.0;

/// Coupling divisor: `clamp(gossip / COUPLING_DIVISOR, COUPLING_MIN, 0)`.
pub const COUPLING_DIVISOR: f64 = 1000.0;
/// Most negative coupling contribution per decay interval.
pub const COUPLING_MIN: f64 = -5.0;

// ── penalty table (§3.7 / CC-29/3) ───────────────────────────────────────────

/// Score delta for a [`PeerPenaltyReason`].
#[must_use]
pub const fn penalty_delta(reason: PeerPenaltyReason) -> f64 {
    match reason {
        PeerPenaltyReason::GossipInvalid => -10.0,
        PeerPenaltyReason::ImportInvalid => -25.0,
        PeerPenaltyReason::ReqrespFault => -5.0,
        PeerPenaltyReason::CustodyUnserved => -15.0,
        PeerPenaltyReason::Behavioural => -5.0,
        PeerPenaltyReason::RateLimit => -5.0,
    }
}

/// Three-way gossip classification (Phase 1 §5.3 / Architecture §5.3).
///
/// Mirrors chain's `gossip_class()` so the mapping function can keep the
/// promise that **Internal never descratches an honest peer**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GossipClass {
    /// Message is fine / accepted — no penalty.
    Accept,
    /// Not processable right now — no penalty, do not propagate.
    Ignore,
    /// Provably invalid — peer fault.
    Reject,
    /// Our bug / limit / transport — **never** a peer penalty.
    Internal,
}

/// Late import path: chain `Reject` after we already ACCEPTed on gossip.
///
/// Returns the application penalty to apply, or `None` for Internal / Ignore /
/// Accept (no penalty, ever, for Internal).
#[must_use]
pub fn penalty_for_chain_class(class: GossipClass) -> Option<(PeerPenaltyReason, f64)> {
    match class {
        GossipClass::Reject => Some((
            PeerPenaltyReason::ImportInvalid,
            penalty_delta(PeerPenaltyReason::ImportInvalid),
        )),
        GossipClass::Internal | GossipClass::Ignore | GossipClass::Accept => None,
    }
}

/// Gossip-validation REJECT attributed to a peer → `gossip_invalid` (−10).
#[must_use]
pub fn penalty_for_gossip_reject() -> (PeerPenaltyReason, f64) {
    (
        PeerPenaltyReason::GossipInvalid,
        penalty_delta(PeerPenaltyReason::GossipInvalid),
    )
}

// ── pure score ops ──────────────────────────────────────────────────────────

/// Sanitize an application score: non-finite → [`DEFAULT_APP_SCORE`], then clamp.
///
/// NaN must never enter coupling / disconnect (H2): `NaN < −20` is false, so a
/// poisoned peer would be un-ejectable forever.
#[must_use]
pub fn sanitize_app_score(score: f64) -> f64 {
    if score.is_finite() {
        score.clamp(APP_SCORE_MIN, APP_SCORE_MAX)
    } else {
        DEFAULT_APP_SCORE
    }
}

/// Sanitize a GossipSub score: non-finite → [`DEFAULT_GOSSIP_SCORE`].
#[must_use]
pub fn sanitize_gossip_score(score: f64) -> f64 {
    if score.is_finite() {
        score
    } else {
        DEFAULT_GOSSIP_SCORE
    }
}

/// Clamp application score into [−100, +100]; non-finite → default 0.
#[must_use]
pub fn clamp_app_score(score: f64) -> f64 {
    sanitize_app_score(score)
}

/// Damped one-way coupling term for one decay interval.
///
/// `clamp(gossip_score / 1000, −5, 0)` — **never positive**.
/// Non-finite gossip contributes **0** (H2).
#[must_use]
pub fn gossip_coupling_delta(gossip_score: f64) -> f64 {
    if !gossip_score.is_finite() {
        return 0.0;
    }
    (gossip_score / COUPLING_DIVISOR).clamp(COUPLING_MIN, 0.0)
}

/// Apply the coupling term to `app_score` (mutates in place).
pub fn apply_gossip_coupling(app_score: &mut f64, gossip_score: f64) {
    *app_score = sanitize_app_score(*app_score);
    *app_score = sanitize_app_score(*app_score + gossip_coupling_delta(gossip_score));
}

/// Apply a named penalty; returns the new score.
pub fn apply_penalty(app_score: &mut f64, reason: PeerPenaltyReason) -> f64 {
    *app_score = sanitize_app_score(*app_score + penalty_delta(reason));
    *app_score
}

/// Useful delivery reward (+1, cap +100).
pub fn apply_useful_delivery(app_score: &mut f64) -> f64 {
    *app_score = sanitize_app_score(*app_score + USEFUL_DELIVERY_DELTA);
    *app_score
}

/// Decay toward 0 by [`APP_SCORE_DECAY`] per slot.
///
/// Non-finite input is reset to 0 (H2).
pub fn decay_app_score(app_score: &mut f64) -> f64 {
    if !app_score.is_finite() {
        *app_score = DEFAULT_APP_SCORE;
        return *app_score;
    }
    *app_score *= APP_SCORE_DECAY;
    // Snap tiny residuals so we do not float forever.
    if app_score.abs() < 1e-12 {
        *app_score = 0.0;
    }
    *app_score
}

/// Whether a mesh member is protected from eviction.
///
/// Mesh members are protected **unless** their gossip score is below
/// [`GOSSIP_THRESHOLD`]. This is **not** a disconnect condition — it only
/// gates eviction candidate selection. No path disconnects on `gossip_score`.
/// Non-finite gossip is treated as unprotected.
#[must_use]
pub fn is_mesh_protected(in_mesh: bool, gossip_score: f64) -> bool {
    in_mesh && gossip_score.is_finite() && gossip_score >= GOSSIP_THRESHOLD
}

/// Disconnect decision — **app_score only**.
///
/// Non-finite app scores are treated as disconnect (H2 fail-closed).
#[must_use]
pub fn should_disconnect(app_score: f64) -> bool {
    if !app_score.is_finite() {
        return true;
    }
    app_score < APP_SCORE_DISCONNECT
}

/// Ban decision — **app_score only**.
///
/// Non-finite app scores are **not** auto-banned (reset path should run first);
/// callers must sanitize before enforce. Fail-closed disconnect still applies
/// via [`should_disconnect`].
#[must_use]
pub fn should_ban(app_score: f64) -> bool {
    app_score.is_finite() && app_score < APP_SCORE_BAN
}

// ── metric producers (CC-29/3 label set) ────────────────────────────────────

/// Apply a penalty and emit `cc_p2p_peer_penalty_total{reason}` + app_score.
pub fn apply_penalty_with_metrics(
    app_score: &mut f64,
    reason: PeerPenaltyReason,
    metrics: &P2pMetrics,
) -> f64 {
    let new = apply_penalty(app_score, reason);
    metrics.inc_peer_penalty(reason);
    metrics.observe_app_score(new);
    new
}

/// Observe GossipSub + application scores and update R-3 early-warning gauges.
///
/// Populates the left tail of `cc_p2p_peer_score` and
/// `cc_p2p_peers_below_threshold{threshold="gossip"}`.
pub fn observe_score_snapshot(
    metrics: &P2pMetrics,
    gossip_scores: impl IntoIterator<Item = f64>,
    app_scores: impl IntoIterator<Item = f64>,
) {
    let mut below_gossip = 0i64;
    for g in gossip_scores {
        metrics.observe_peer_score(g);
        if g < GOSSIP_THRESHOLD {
            below_gossip += 1;
        }
    }
    for a in app_scores {
        metrics.observe_app_score(a);
    }
    metrics.set_peers_below_threshold("gossip", below_gossip);
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;
    use crate::metrics::P2pMetrics;
    use prometheus_client::encoding::text::encode;
    use prometheus_client::registry::Registry;

    fn metrics() -> P2pMetrics {
        let mut reg = Registry::default();
        P2pMetrics::register(&mut reg)
    }

    #[test]
    fn coupling_is_one_way_and_damped() {
        use crate::gossip::scoring::GRAYLIST_THRESHOLD;
        // gossip at graylist floor → at most −5 per interval.
        let d = gossip_coupling_delta(GRAYLIST_THRESHOLD);
        assert!((d - (-5.0)).abs() < 1e-12, "delta={d}");

        // never positive from a positive GossipSub score.
        assert_eq!(gossip_coupling_delta(30.0), 0.0);
        assert_eq!(gossip_coupling_delta(1000.0), 0.0);
        assert_eq!(gossip_coupling_delta(0.0), 0.0);

        // mid-range: −2500 / 1000 = −2.5
        assert!((gossip_coupling_delta(-2500.0) - (-2.5)).abs() < 1e-12);

        let mut app = 10.0;
        apply_gossip_coupling(&mut app, 5000.0);
        assert_eq!(app, 10.0, "positive gossip must not raise app score");

        apply_gossip_coupling(&mut app, GRAYLIST_THRESHOLD);
        assert!((app - 5.0).abs() < 1e-12, "10 + (−5) = 5, got {app}");
    }

    #[test]
    fn no_disconnect_reads_gossip_score_decision() {
        use crate::gossip::scoring::GRAYLIST_THRESHOLD;
        // Pure decision helpers: gossip alone never disconnects.
        assert!(!should_disconnect(0.0));
        assert!(!should_ban(0.0));
        // Peer at gossip = GraylistThreshold, app = 0 stays connected.
        let app = 0.0;
        let _gossip = GRAYLIST_THRESHOLD;
        assert!(!should_disconnect(app));
        // Peer at app = −21 is disconnected.
        assert!(should_disconnect(-21.0));
        assert!(!should_ban(-21.0));
        assert!(should_ban(-50.1));
    }

    #[test]
    fn penalty_table_deltas_and_metrics() {
        let m = metrics();
        let cases = [
            (PeerPenaltyReason::GossipInvalid, -10.0),
            (PeerPenaltyReason::ImportInvalid, -25.0),
            (PeerPenaltyReason::ReqrespFault, -5.0),
            (PeerPenaltyReason::CustodyUnserved, -15.0),
            (PeerPenaltyReason::Behavioural, -5.0),
            (PeerPenaltyReason::RateLimit, -5.0),
        ];
        for (reason, delta) in cases {
            assert_eq!(penalty_delta(reason), delta, "{reason:?}");
            let mut score = 0.0;
            let before = score;
            let after = apply_penalty_with_metrics(&mut score, reason, &m);
            assert!((after - (before + delta)).abs() < 1e-12);
            assert_eq!(m.peer_penalty_count(reason), 1);
        }

        let mut reg = Registry::default();
        let m2 = P2pMetrics::register(&mut reg);
        for reason in PeerPenaltyReason::ALL {
            let mut s = 0.0;
            apply_penalty_with_metrics(&mut s, reason, &m2);
        }
        let mut buf = String::new();
        encode(&mut buf, &reg).expect("encode");
        for reason in PeerPenaltyReason::ALL {
            let needle = format!(
                "cc_p2p_peer_penalty_total{{reason=\"{}\"}} 1",
                reason.as_str()
            );
            assert!(
                buf.contains(&needle),
                "missing series for {:?}:\n{buf}",
                reason
            );
        }
    }

    #[test]
    fn internal_chain_class_produces_no_penalty() {
        assert_eq!(penalty_for_chain_class(GossipClass::Internal), None);
        assert_eq!(penalty_for_chain_class(GossipClass::Ignore), None);
        assert_eq!(penalty_for_chain_class(GossipClass::Accept), None);
        let reject = penalty_for_chain_class(GossipClass::Reject).expect("reject");
        assert_eq!(reject.0, PeerPenaltyReason::ImportInvalid);
        assert_eq!(reject.1, -25.0);

        // Driving Internal through the mapping must not touch the score or counter.
        let m = metrics();
        let mut app = 5.0;
        if let Some((reason, _)) = penalty_for_chain_class(GossipClass::Internal) {
            apply_penalty_with_metrics(&mut app, reason, &m);
        }
        assert_eq!(app, 5.0);
        for reason in PeerPenaltyReason::ALL {
            assert_eq!(m.peer_penalty_count(reason), 0, "{reason:?}");
        }
    }

    #[test]
    fn useful_delivery_and_decay() {
        let mut app = 0.0;
        apply_useful_delivery(&mut app);
        assert_eq!(app, 1.0);
        app = 100.0;
        apply_useful_delivery(&mut app);
        assert_eq!(app, 100.0, "cap at +100");

        app = -10.0;
        decay_app_score(&mut app);
        assert!((app - (-9.8)).abs() < 1e-12);

        app = 10.0;
        decay_app_score(&mut app);
        assert!((app - 9.8).abs() < 1e-12);
    }

    #[test]
    fn r3_early_warning_metrics_populated() {
        let mut reg = Registry::default();
        let m = P2pMetrics::register(&mut reg);
        observe_score_snapshot(
            &m,
            [-5000.0, -100.0, 5.0],
            [-1.0, 0.0, 2.0],
        );
        // One peer below GossipThreshold (−4000).
        assert_eq!(m.peers_below_threshold("gossip"), 1);

        let mut buf = String::new();
        encode(&mut buf, &reg).expect("encode");
        assert!(
            buf.contains("cc_p2p_peers_below_threshold{threshold=\"gossip\"} 1"),
            "{buf}"
        );
        assert!(buf.contains("cc_p2p_peer_score"), "{buf}");
        assert!(buf.contains("cc_p2p_app_score"), "{buf}");
    }

    #[test]
    fn mesh_protection_uses_scoring_threshold() {
        assert!(is_mesh_protected(true, 0.0));
        assert!(is_mesh_protected(true, GOSSIP_THRESHOLD));
        assert!(!is_mesh_protected(true, GOSSIP_THRESHOLD - 1.0));
        assert!(!is_mesh_protected(false, 0.0));
    }

    #[test]
    fn non_finite_scores_never_poison_coupling_or_decisions() {
        // Coupling ignores all non-finite gossip (NaN / ±∞ → 0).
        assert_eq!(gossip_coupling_delta(f64::NAN), 0.0);
        assert_eq!(gossip_coupling_delta(f64::INFINITY), 0.0);
        assert_eq!(gossip_coupling_delta(f64::NEG_INFINITY), 0.0);

        let mut app = 10.0;
        apply_gossip_coupling(&mut app, f64::NAN);
        assert_eq!(app, 10.0);

        // Sanitize resets NaN app; out-of-range finite clamps.
        assert_eq!(sanitize_app_score(f64::NAN), 0.0);
        assert_eq!(sanitize_app_score(f64::INFINITY), 0.0);
        assert_eq!(sanitize_app_score(-150.0), APP_SCORE_MIN);
        assert_eq!(sanitize_gossip_score(f64::NAN), 0.0);

        // Fail-closed disconnect on residual NaN; ban only on finite < −50.
        assert!(should_disconnect(f64::NAN));
        assert!(!should_ban(f64::NAN));

        let mut poisoned = f64::NAN;
        decay_app_score(&mut poisoned);
        assert_eq!(poisoned, 0.0);
    }
}
