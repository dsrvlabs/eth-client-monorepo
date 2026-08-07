//! Translate a plain [`ScoringConfig`] into libp2p gossipsub peer-score types
//! (Architecture §5.6). Values are owned by CC-22c; this module owns the mapping.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::time::Duration;

use libp2p::gossipsub::{PeerScoreParams, PeerScoreThresholds, TopicHash, TopicScoreParams};

/// Plain scoring parameters with no consensus knowledge — `f64`s, `Duration`s,
/// topic strings, and optional IP whitelist entries.
///
/// Field names mirror libp2p's [`PeerScoreParams`] / [`PeerScoreThresholds`] /
/// [`TopicScoreParams`] so the mapping is field-for-field.
#[derive(Debug, Clone)]
pub struct ScoringConfig {
    // --- PeerScoreThresholds ---
    pub gossip_threshold: f64,
    pub publish_threshold: f64,
    pub graylist_threshold: f64,
    pub accept_px_threshold: f64,
    pub opportunistic_graft_threshold: f64,

    // --- PeerScoreParams (global) ---
    pub topic_score_cap: f64,
    pub app_specific_weight: f64,
    pub ip_colocation_factor_weight: f64,
    pub ip_colocation_factor_threshold: f64,
    pub ip_colocation_factor_whitelist: Vec<IpAddr>,
    pub behaviour_penalty_weight: f64,
    pub behaviour_penalty_threshold: f64,
    pub behaviour_penalty_decay: f64,
    pub decay_interval: Duration,
    pub decay_to_zero: f64,
    pub retain_score: Duration,
    pub slow_peer_weight: f64,
    pub slow_peer_threshold: f64,
    pub slow_peer_decay: f64,

    /// Per-topic score parameters keyed by topic string.
    ///
    /// Keys are converted via [`TopicHash::from_raw`], which is the identity
    /// hash used by [`libp2p::gossipsub::IdentTopic`]. Pass the **full** eth
    /// gossip topic string (the same string given to `IdentTopic::new`), not a
    /// short display name — otherwise score params never attach to the mesh.
    pub topics: Vec<(String, TopicScoringConfig)>,
}

/// Per-topic score parameters (P1–P4), plain form of [`TopicScoreParams`].
#[derive(Debug, Clone)]
pub struct TopicScoringConfig {
    pub topic_weight: f64,
    pub time_in_mesh_weight: f64,
    pub time_in_mesh_quantum: Duration,
    pub time_in_mesh_cap: f64,
    pub first_message_deliveries_weight: f64,
    pub first_message_deliveries_decay: f64,
    pub first_message_deliveries_cap: f64,
    pub mesh_message_deliveries_weight: f64,
    pub mesh_message_deliveries_decay: f64,
    pub mesh_message_deliveries_cap: f64,
    pub mesh_message_deliveries_threshold: f64,
    pub mesh_message_deliveries_window: Duration,
    pub mesh_message_deliveries_activation: Duration,
    pub mesh_failure_penalty_weight: f64,
    pub mesh_failure_penalty_decay: f64,
    pub invalid_message_deliveries_weight: f64,
    pub invalid_message_deliveries_decay: f64,
}

/// Field-for-field translation into libp2p score types.
pub fn build_peer_score_params(cfg: &ScoringConfig) -> (PeerScoreParams, PeerScoreThresholds) {
    let thresholds = PeerScoreThresholds {
        gossip_threshold: cfg.gossip_threshold,
        publish_threshold: cfg.publish_threshold,
        graylist_threshold: cfg.graylist_threshold,
        accept_px_threshold: cfg.accept_px_threshold,
        opportunistic_graft_threshold: cfg.opportunistic_graft_threshold,
    };

    let mut topics: HashMap<TopicHash, TopicScoreParams> = HashMap::new();
    for (name, t) in &cfg.topics {
        topics.insert(
            TopicHash::from_raw(name),
            TopicScoreParams {
                topic_weight: t.topic_weight,
                time_in_mesh_weight: t.time_in_mesh_weight,
                time_in_mesh_quantum: t.time_in_mesh_quantum,
                time_in_mesh_cap: t.time_in_mesh_cap,
                first_message_deliveries_weight: t.first_message_deliveries_weight,
                first_message_deliveries_decay: t.first_message_deliveries_decay,
                first_message_deliveries_cap: t.first_message_deliveries_cap,
                mesh_message_deliveries_weight: t.mesh_message_deliveries_weight,
                mesh_message_deliveries_decay: t.mesh_message_deliveries_decay,
                mesh_message_deliveries_cap: t.mesh_message_deliveries_cap,
                mesh_message_deliveries_threshold: t.mesh_message_deliveries_threshold,
                mesh_message_deliveries_window: t.mesh_message_deliveries_window,
                mesh_message_deliveries_activation: t.mesh_message_deliveries_activation,
                mesh_failure_penalty_weight: t.mesh_failure_penalty_weight,
                mesh_failure_penalty_decay: t.mesh_failure_penalty_decay,
                invalid_message_deliveries_weight: t.invalid_message_deliveries_weight,
                invalid_message_deliveries_decay: t.invalid_message_deliveries_decay,
            },
        );
    }

    let params = PeerScoreParams {
        topics,
        topic_score_cap: cfg.topic_score_cap,
        app_specific_weight: cfg.app_specific_weight,
        ip_colocation_factor_weight: cfg.ip_colocation_factor_weight,
        ip_colocation_factor_threshold: cfg.ip_colocation_factor_threshold,
        ip_colocation_factor_whitelist: cfg
            .ip_colocation_factor_whitelist
            .iter()
            .copied()
            .collect::<HashSet<_>>(),
        behaviour_penalty_weight: cfg.behaviour_penalty_weight,
        behaviour_penalty_threshold: cfg.behaviour_penalty_threshold,
        behaviour_penalty_decay: cfg.behaviour_penalty_decay,
        decay_interval: cfg.decay_interval,
        decay_to_zero: cfg.decay_to_zero,
        retain_score: cfg.retain_score,
        slow_peer_weight: cfg.slow_peer_weight,
        slow_peer_threshold: cfg.slow_peer_threshold,
        slow_peer_decay: cfg.slow_peer_decay,
    };

    (params, thresholds)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn non_default_config() -> ScoringConfig {
        ScoringConfig {
            gossip_threshold: -4000.0,
            publish_threshold: -8000.0,
            graylist_threshold: -16000.0,
            accept_px_threshold: 100.0,
            opportunistic_graft_threshold: 5.0,
            topic_score_cap: 12.5,
            app_specific_weight: 1.0,
            ip_colocation_factor_weight: -15.0,
            ip_colocation_factor_threshold: 10.0,
            ip_colocation_factor_whitelist: vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))],
            behaviour_penalty_weight: -15.92,
            behaviour_penalty_threshold: 6.0,
            behaviour_penalty_decay: 0.9857,
            decay_interval: Duration::from_secs(12),
            decay_to_zero: 0.01,
            retain_score: Duration::from_secs(100 * 12 * 32),
            slow_peer_weight: -0.5,
            slow_peer_threshold: 1.0,
            slow_peer_decay: 0.3,
            topics: vec![(
                "beacon_block".to_string(),
                TopicScoringConfig {
                    topic_weight: 0.5,
                    time_in_mesh_weight: 0.0333,
                    time_in_mesh_quantum: Duration::from_secs(12),
                    time_in_mesh_cap: 300.0,
                    first_message_deliveries_weight: 1.0,
                    first_message_deliveries_decay: 0.9928,
                    first_message_deliveries_cap: 23.0,
                    mesh_message_deliveries_weight: 0.0, // P3 disabled
                    mesh_message_deliveries_decay: 0.5,
                    mesh_message_deliveries_cap: 1.0,
                    mesh_message_deliveries_threshold: 1.0,
                    mesh_message_deliveries_window: Duration::from_secs(2),
                    mesh_message_deliveries_activation: Duration::from_secs(12),
                    mesh_failure_penalty_weight: 0.0, // P3b disabled
                    mesh_failure_penalty_decay: 0.5,
                    invalid_message_deliveries_weight: -25.0,
                    invalid_message_deliveries_decay: 0.9971,
                },
            )],
        }
    }

    #[test]
    fn build_peer_score_params_field_for_field() {
        let cfg = non_default_config();
        let (params, thresholds) = build_peer_score_params(&cfg);

        assert_eq!(thresholds.gossip_threshold, cfg.gossip_threshold);
        assert_eq!(thresholds.publish_threshold, cfg.publish_threshold);
        assert_eq!(thresholds.graylist_threshold, cfg.graylist_threshold);
        assert_eq!(thresholds.accept_px_threshold, cfg.accept_px_threshold);
        assert_eq!(
            thresholds.opportunistic_graft_threshold,
            cfg.opportunistic_graft_threshold
        );

        assert_eq!(params.topic_score_cap, cfg.topic_score_cap);
        assert_eq!(params.app_specific_weight, cfg.app_specific_weight);
        assert_eq!(
            params.ip_colocation_factor_weight,
            cfg.ip_colocation_factor_weight
        );
        assert_eq!(
            params.ip_colocation_factor_threshold,
            cfg.ip_colocation_factor_threshold
        );
        assert_eq!(
            params.ip_colocation_factor_whitelist,
            cfg.ip_colocation_factor_whitelist
                .iter()
                .copied()
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            params.behaviour_penalty_weight,
            cfg.behaviour_penalty_weight
        );
        assert_eq!(
            params.behaviour_penalty_threshold,
            cfg.behaviour_penalty_threshold
        );
        assert_eq!(params.behaviour_penalty_decay, cfg.behaviour_penalty_decay);
        assert_eq!(params.decay_interval, cfg.decay_interval);
        assert_eq!(params.decay_to_zero, cfg.decay_to_zero);
        assert_eq!(params.retain_score, cfg.retain_score);
        assert_eq!(params.slow_peer_weight, cfg.slow_peer_weight);
        assert_eq!(params.slow_peer_threshold, cfg.slow_peer_threshold);
        assert_eq!(params.slow_peer_decay, cfg.slow_peer_decay);

        let (name, tcfg) = &cfg.topics[0];
        let t = params
            .topics
            .get(&TopicHash::from_raw(name))
            .expect("topic present");
        assert_eq!(t.topic_weight, tcfg.topic_weight);
        assert_eq!(t.time_in_mesh_weight, tcfg.time_in_mesh_weight);
        assert_eq!(t.time_in_mesh_quantum, tcfg.time_in_mesh_quantum);
        assert_eq!(t.time_in_mesh_cap, tcfg.time_in_mesh_cap);
        assert_eq!(
            t.first_message_deliveries_weight,
            tcfg.first_message_deliveries_weight
        );
        assert_eq!(
            t.first_message_deliveries_decay,
            tcfg.first_message_deliveries_decay
        );
        assert_eq!(
            t.first_message_deliveries_cap,
            tcfg.first_message_deliveries_cap
        );
        assert_eq!(
            t.mesh_message_deliveries_weight,
            tcfg.mesh_message_deliveries_weight
        );
        assert_eq!(
            t.mesh_message_deliveries_decay,
            tcfg.mesh_message_deliveries_decay
        );
        assert_eq!(
            t.mesh_message_deliveries_cap,
            tcfg.mesh_message_deliveries_cap
        );
        assert_eq!(
            t.mesh_message_deliveries_threshold,
            tcfg.mesh_message_deliveries_threshold
        );
        assert_eq!(
            t.mesh_message_deliveries_window,
            tcfg.mesh_message_deliveries_window
        );
        assert_eq!(
            t.mesh_message_deliveries_activation,
            tcfg.mesh_message_deliveries_activation
        );
        assert_eq!(
            t.mesh_failure_penalty_weight,
            tcfg.mesh_failure_penalty_weight
        );
        assert_eq!(
            t.mesh_failure_penalty_decay,
            tcfg.mesh_failure_penalty_decay
        );
        assert_eq!(
            t.invalid_message_deliveries_weight,
            tcfg.invalid_message_deliveries_weight
        );
        assert_eq!(
            t.invalid_message_deliveries_decay,
            tcfg.invalid_message_deliveries_decay
        );

        // Defaults would fail at least one of these non-default probes.
        assert_ne!(
            thresholds.gossip_threshold,
            PeerScoreThresholds::default().gossip_threshold
        );
        assert_ne!(
            params.app_specific_weight,
            PeerScoreParams::default().app_specific_weight
        );
    }
}
