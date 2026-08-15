//! GossipSub scoring parameters — Architecture §5.6 / CC-22c.
//!
//! **One** [`ScoringConfig`] is built by **one** function at startup
//! ([`build_scoring_config`]). All §5.6 numbers live here so a family cannot
//! end up unscored because a call site forgot a weight. P3 / P3b ship at
//! weight **0** on every topic (ADR P2-10 / OQ-P2-3 deferred).
//!
//! `docs/p2p-scoring.md` is generated from this struct
//! ([`render_scoring_doc`]); a test regenerates and asserts the committed file
//! matches.
//!
//! The plain [`cc_libp2p::ScoringConfig`] translation is field-for-field via
//! [`to_libp2p_scoring_config`] — `cc-libp2p` owns mapping, this module owns
//! the shipped numbers.

use std::time::Duration;

use cc_libp2p::{ScoringConfig as Libp2pScoringConfig, TopicScoringConfig};
use cc_types::{Mainnet, Preset};

use super::topics::TopicName;
use super::{ATTESTATION_SUBNET_COUNT, SubnetCounts};

// ── time base (mainnet / Hoodi) ─────────────────────────────────────────────

/// Slot duration in seconds (mainnet / Hoodi).
pub const SLOT_SECONDS: u64 = 12;
/// Slots per epoch.
pub const SLOTS_PER_EPOCH: u64 = Mainnet::SLOTS_PER_EPOCH;
/// Mesh degree `D` used in P2 cap derivation.
pub const MESH_DEGREE_D: f64 = 8.0;
/// Max contribution of P1 (time-in-mesh) toward max positive score.
pub const MAX_IN_MESH_SCORE: f64 = 10.0;
/// Max contribution of P2 (first deliveries) toward max positive score.
pub const MAX_FIRST_MESSAGE_DELIVERIES_SCORE: f64 = 40.0;
/// Decay-to-zero floor (global).
pub const DECAY_TO_ZERO: f64 = 0.01;
/// Default active-validator count for rate-derived P2 caps (snapshot / startup
/// before a live count is available). Documented; not a network observation.
pub const DEFAULT_ACTIVE_VALIDATORS: u64 = 1_000_000;

// ── global thresholds (verbatim §5.6 — the *only* scoring definition site) ─

/// `GossipThreshold` — mesh / gossip emission floor.
pub const GOSSIP_THRESHOLD: f64 = -4000.0;
/// `PublishThreshold`.
pub const PUBLISH_THRESHOLD: f64 = -8000.0;
/// `GraylistThreshold`.
pub const GRAYLIST_THRESHOLD: f64 = -16000.0;
/// `AcceptPXThreshold`.
pub const ACCEPT_PX_THRESHOLD: f64 = 100.0;
/// `OpportunisticGraftThreshold`.
pub const OPPORTUNISTIC_GRAFT_THRESHOLD: f64 = 5.0;

// ── per-topic family weights (§5.6) ─────────────────────────────────────────

/// `beacon_block` topic weight.
pub const WEIGHT_BEACON_BLOCK: f64 = 0.5;
/// `beacon_aggregate_and_proof` topic weight.
pub const WEIGHT_BEACON_AGGREGATE: f64 = 0.5;
/// `beacon_attestation_{id}` per-subnet weight (`1/64`).
pub const WEIGHT_BEACON_ATTESTATION: f64 = 1.0 / 64.0;
/// Rare operation topics (`voluntary_exit`, slashings, `bls_to_execution_change`).
pub const WEIGHT_OPERATION: f64 = 0.05;
/// `sync_committee_contribution_and_proof` topic weight *(derived)*.
pub const WEIGHT_SYNC_CONTRIBUTION: f64 = 0.05;
/// `sync_committee_{id}` per-subnet weight *(derived — 4 subnets, family 0.05)*.
pub const WEIGHT_SYNC_COMMITTEE: f64 = 0.0125;
/// Column family total weight (spread as `0.5 / sampling_size` per topic).
pub const WEIGHT_COLUMN_FAMILY: f64 = 0.5;

// ── helpers ─────────────────────────────────────────────────────────────────

/// `scoreDecay(d) = 0.01^(1 / (d / slot))` with `d` a wall-clock duration.
#[must_use]
pub fn score_decay(duration: Duration) -> f64 {
    let ticks = duration.as_secs_f64() / f64::from(SLOT_SECONDS as u32);
    // Guard against zero-length: decay of 0 is invalid for counters.
    if ticks <= 0.0 {
        return DECAY_TO_ZERO;
    }
    DECAY_TO_ZERO.powf(1.0 / ticks)
}

/// `scoreDecay` for an epoch count.
#[must_use]
pub fn score_decay_epochs(epochs: u64) -> f64 {
    score_decay(Duration::from_secs(epochs * SLOTS_PER_EPOCH * SLOT_SECONDS))
}

/// Steady-state counter: `rate / (1 − decay)`.
#[must_use]
pub fn decay_convergence(decay: f64, rate: f64) -> f64 {
    rate / (1.0 - decay)
}

/// Sum of per-family topic weights (column family counted once as 0.5).
///
/// Independent of `sampling_size` — that is the invariant ADR P2-07 protects.
#[must_use]
pub fn family_weight_sum() -> f64 {
    WEIGHT_BEACON_BLOCK
        + WEIGHT_COLUMN_FAMILY
        + WEIGHT_BEACON_AGGREGATE
        + WEIGHT_BEACON_ATTESTATION * ATTESTATION_SUBNET_COUNT as f64
        + WEIGHT_OPERATION * 3.0 // exit, proposer_slashing, attester_slashing
        + WEIGHT_OPERATION // bls_to_execution_change (derived)
        + WEIGHT_SYNC_CONTRIBUTION
        + WEIGHT_SYNC_COMMITTEE * Mainnet::SYNC_COMMITTEE_SUBNET_COUNT as f64
}

/// `max_positive_score = (P1_cap + P2_cap) × Σ topicWeights` (Lighthouse shape).
#[must_use]
pub fn max_positive_score() -> f64 {
    (MAX_IN_MESH_SCORE + MAX_FIRST_MESSAGE_DELIVERIES_SCORE) * family_weight_sum()
}

/// `TopicScoreCap = max_positive_score × 0.5`.
#[must_use]
pub fn topic_score_cap() -> f64 {
    max_positive_score() * 0.5
}

/// Column per-topic weight: `0.5 / sampling_size`.
#[must_use]
pub fn column_topic_weight(sampling_size: u64) -> f64 {
    let n = sampling_size.max(1) as f64;
    WEIGHT_COLUMN_FAMILY / n
}

// ── ten topic families ──────────────────────────────────────────────────────

/// The ten Fulu gossip topic families (subnet families counted once).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicFamily {
    /// `beacon_block`
    BeaconBlock,
    /// `data_column_sidecar_{id}`
    DataColumnSidecar,
    /// `beacon_aggregate_and_proof`
    BeaconAggregateAndProof,
    /// `beacon_attestation_{id}`
    BeaconAttestation,
    /// `voluntary_exit`
    VoluntaryExit,
    /// `proposer_slashing`
    ProposerSlashing,
    /// `attester_slashing`
    AttesterSlashing,
    /// `bls_to_execution_change`
    BlsToExecutionChange,
    /// `sync_committee_contribution_and_proof`
    SyncCommitteeContributionAndProof,
    /// `sync_committee_{id}`
    SyncCommittee,
}

impl TopicFamily {
    /// All ten families in Architecture table order.
    pub const ALL: [Self; 10] = [
        Self::BeaconBlock,
        Self::DataColumnSidecar,
        Self::BeaconAggregateAndProof,
        Self::BeaconAttestation,
        Self::VoluntaryExit,
        Self::ProposerSlashing,
        Self::AttesterSlashing,
        Self::BlsToExecutionChange,
        Self::SyncCommitteeContributionAndProof,
        Self::SyncCommittee,
    ];

    /// Stable display name for docs / snapshot keys.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeaconBlock => "beacon_block",
            Self::DataColumnSidecar => "data_column_sidecar_{id}",
            Self::BeaconAggregateAndProof => "beacon_aggregate_and_proof",
            Self::BeaconAttestation => "beacon_attestation_{id}",
            Self::VoluntaryExit => "voluntary_exit",
            Self::ProposerSlashing => "proposer_slashing",
            Self::AttesterSlashing => "attester_slashing",
            Self::BlsToExecutionChange => "bls_to_execution_change",
            Self::SyncCommitteeContributionAndProof => "sync_committee_contribution_and_proof",
            Self::SyncCommittee => "sync_committee_{id}",
        }
    }

    /// Map a concrete [`TopicName`] onto its family.
    #[must_use]
    pub const fn from_topic_name(name: TopicName) -> Self {
        match name {
            TopicName::BeaconBlock => Self::BeaconBlock,
            TopicName::DataColumnSidecar(_) => Self::DataColumnSidecar,
            TopicName::BeaconAggregateAndProof => Self::BeaconAggregateAndProof,
            TopicName::BeaconAttestation(_) => Self::BeaconAttestation,
            TopicName::VoluntaryExit => Self::VoluntaryExit,
            TopicName::ProposerSlashing => Self::ProposerSlashing,
            TopicName::AttesterSlashing => Self::AttesterSlashing,
            TopicName::BlsToExecutionChange => Self::BlsToExecutionChange,
            TopicName::SyncCommitteeContributionAndProof => Self::SyncCommitteeContributionAndProof,
            TopicName::SyncCommittee(_) => Self::SyncCommittee,
        }
    }
}

// ── ScoringConfig ───────────────────────────────────────────────────────────

/// Inputs for [`build_scoring_config`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScoringInputs {
    /// `sampling_size = max(8, cgc)` — drives column per-topic weight.
    pub sampling_size: u64,
    /// Active validator count for aggregate / attestation rate estimates.
    pub active_validators: u64,
}

impl Default for ScoringInputs {
    fn default() -> Self {
        Self {
            sampling_size: 8,
            active_validators: DEFAULT_ACTIVE_VALIDATORS,
        }
    }
}

/// Per-topic score parameters as shipped (P1–P4; P3/P3b weight 0).
#[derive(Debug, Clone, PartialEq)]
pub struct TopicScoreConfig {
    /// Family this row describes.
    pub family: TopicFamily,
    /// Per-topic weight (for subnet families: weight of one subnet topic).
    pub topic_weight: f64,
    /// Family total weight (subnet families: weight × count / sampling_size).
    pub family_total_weight: f64,
    /// Whether the weight is derived (not in the research note's fixed table).
    pub derived: bool,
    /// P1
    pub time_in_mesh_weight: f64,
    pub time_in_mesh_quantum: Duration,
    pub time_in_mesh_cap: f64,
    /// P2
    pub first_message_deliveries_weight: f64,
    pub first_message_deliveries_decay: f64,
    pub first_message_deliveries_cap: f64,
    /// P3 — **always 0** (shipped disabled).
    pub mesh_message_deliveries_weight: f64,
    pub mesh_message_deliveries_decay: f64,
    pub mesh_message_deliveries_cap: f64,
    pub mesh_message_deliveries_threshold: f64,
    pub mesh_message_deliveries_window: Duration,
    pub mesh_message_deliveries_activation: Duration,
    /// P3b — **always 0**.
    pub mesh_failure_penalty_weight: f64,
    pub mesh_failure_penalty_decay: f64,
    /// P4
    pub invalid_message_deliveries_weight: f64,
    pub invalid_message_deliveries_decay: f64,
    /// Expected messages per slot (documentation / derivation).
    pub expected_rate_per_slot: f64,
}

/// The single shipped scoring parameter set (CC-22/2).
///
/// Built only by [`build_scoring_config`]. Threshold literals appear **only**
/// in that constructor (and the named constants it uses).
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringConfig {
    /// Build inputs (sampling size, active validators).
    pub inputs: ScoringInputs,
    // thresholds
    pub gossip_threshold: f64,
    pub publish_threshold: f64,
    pub graylist_threshold: f64,
    pub accept_px_threshold: f64,
    pub opportunistic_graft_threshold: f64,
    // global params
    pub topic_score_cap: f64,
    pub max_positive_score: f64,
    pub app_specific_weight: f64,
    pub ip_colocation_factor_weight: f64,
    pub ip_colocation_factor_threshold: f64,
    pub behaviour_penalty_weight: f64,
    pub behaviour_penalty_threshold: f64,
    pub behaviour_penalty_decay: f64,
    pub decay_interval: Duration,
    pub decay_to_zero: f64,
    pub retain_score: Duration,
    /// Slow-peer term disabled (not in §5.6 table).
    pub slow_peer_weight: f64,
    pub slow_peer_threshold: f64,
    pub slow_peer_decay: f64,
    /// One row per topic family (ten).
    pub topics: Vec<TopicScoreConfig>,
}

/// Build the shipped [`ScoringConfig`] for `inputs` (one function at startup).
///
/// Recompute whenever `set_custody_group_count` changes `sampling_size`
/// (column weight = `0.5 / sampling_size`; family total stays 0.5).
#[must_use]
pub fn build_scoring_config(inputs: ScoringInputs) -> ScoringConfig {
    let sampling_size = inputs.sampling_size.max(1);
    let active = inputs.active_validators.max(1) as f64;

    let max_pos = max_positive_score();
    let cap = topic_score_cap();

    let decay_interval = Duration::from_secs(SLOT_SECONDS);
    let retain_score = Duration::from_secs(100 * SLOTS_PER_EPOCH * SLOT_SECONDS);

    // P7 weight −15.92 (research note / Architecture §5.6).
    let behaviour_penalty_decay = score_decay_epochs(10);
    let behaviour_penalty_weight = -15.92;
    let behaviour_penalty_threshold = 6.0;

    // P1 shared shape: weight 10/cap, quantum 1 slot, cap 3600/12 = 300.
    let time_in_mesh_quantum = Duration::from_secs(SLOT_SECONDS);
    let time_in_mesh_cap = 3600.0 / SLOT_SECONDS as f64;
    let time_in_mesh_weight = MAX_IN_MESH_SCORE / time_in_mesh_cap;

    // P4 decay scoreDecay(50 epochs) = 0.9971 (note table).
    let invalid_decay = score_decay_epochs(50);

    let col_w = column_topic_weight(sampling_size);

    // Expected rates (messages / slot).
    let att_rate = active / SLOTS_PER_EPOCH as f64 / ATTESTATION_SUBNET_COUNT as f64;
    // Aggregators: committees ≈ active / (slots × target_committee), then × target aggregators.
    // Use a stable Lighthouse-shaped floor: at least 1.
    let agg_rate = (active / SLOTS_PER_EPOCH as f64 / 128.0).max(1.0);
    let exit_rate = 4.0 / SLOTS_PER_EPOCH as f64;
    let slash_rate = 1.0 / 5.0 / SLOTS_PER_EPOCH as f64;
    let sync_contrib_rate = 4.0; // one contrib-stream across 4 subnets (aggregate-shaped)
    let sync_subnet_rate = Mainnet::SYNC_COMMITTEE_SIZE as f64
        / Mainnet::SYNC_COMMITTEE_SUBNET_COUNT as f64
        / SLOTS_PER_EPOCH as f64;

    let topic = |family: TopicFamily,
                 topic_weight: f64,
                 family_total: f64,
                 derived: bool,
                 p2_decay_epochs: u64,
                 expected_rate: f64|
     -> TopicScoreConfig {
        let p2_decay = score_decay_epochs(p2_decay_epochs);
        let p2_cap = decay_convergence(p2_decay, 2.0 * expected_rate / MESH_DEGREE_D).max(1e-9);
        let p2_weight = MAX_FIRST_MESSAGE_DELIVERIES_SCORE / p2_cap;
        let p4_weight = -max_pos / topic_weight;

        TopicScoreConfig {
            family,
            topic_weight,
            family_total_weight: family_total,
            derived,
            time_in_mesh_weight,
            time_in_mesh_quantum,
            time_in_mesh_cap,
            first_message_deliveries_weight: p2_weight,
            first_message_deliveries_decay: p2_decay,
            first_message_deliveries_cap: p2_cap,
            // P3 / P3b disabled (weight 0) on every topic — CC-22/2.
            mesh_message_deliveries_weight: 0.0,
            mesh_message_deliveries_decay: 0.0,
            mesh_message_deliveries_cap: 0.0,
            mesh_message_deliveries_threshold: 0.0,
            mesh_message_deliveries_window: Duration::from_secs(0),
            mesh_message_deliveries_activation: Duration::from_secs(0),
            mesh_failure_penalty_weight: 0.0,
            mesh_failure_penalty_decay: 0.0,
            invalid_message_deliveries_weight: p4_weight,
            invalid_message_deliveries_decay: invalid_decay,
            expected_rate_per_slot: expected_rate,
        }
    };

    let topics = vec![
        topic(
            TopicFamily::BeaconBlock,
            WEIGHT_BEACON_BLOCK,
            WEIGHT_BEACON_BLOCK,
            false,
            20,
            1.0,
        ),
        topic(
            TopicFamily::DataColumnSidecar,
            col_w,
            WEIGHT_COLUMN_FAMILY,
            true,
            20,
            1.0,
        ),
        topic(
            TopicFamily::BeaconAggregateAndProof,
            WEIGHT_BEACON_AGGREGATE,
            WEIGHT_BEACON_AGGREGATE,
            false,
            1,
            agg_rate,
        ),
        topic(
            TopicFamily::BeaconAttestation,
            WEIGHT_BEACON_ATTESTATION,
            WEIGHT_BEACON_ATTESTATION * ATTESTATION_SUBNET_COUNT as f64,
            false,
            1,
            att_rate,
        ),
        topic(
            TopicFamily::VoluntaryExit,
            WEIGHT_OPERATION,
            WEIGHT_OPERATION,
            false,
            100,
            exit_rate,
        ),
        topic(
            TopicFamily::ProposerSlashing,
            WEIGHT_OPERATION,
            WEIGHT_OPERATION,
            false,
            100,
            slash_rate,
        ),
        topic(
            TopicFamily::AttesterSlashing,
            WEIGHT_OPERATION,
            WEIGHT_OPERATION,
            false,
            100,
            slash_rate,
        ),
        topic(
            TopicFamily::BlsToExecutionChange,
            WEIGHT_OPERATION,
            WEIGHT_OPERATION,
            true,
            100,
            exit_rate,
        ),
        topic(
            TopicFamily::SyncCommitteeContributionAndProof,
            WEIGHT_SYNC_CONTRIBUTION,
            WEIGHT_SYNC_CONTRIBUTION,
            true,
            1,
            sync_contrib_rate,
        ),
        topic(
            TopicFamily::SyncCommittee,
            WEIGHT_SYNC_COMMITTEE,
            WEIGHT_SYNC_COMMITTEE * Mainnet::SYNC_COMMITTEE_SUBNET_COUNT as f64,
            true,
            1,
            sync_subnet_rate,
        ),
    ];

    ScoringConfig {
        inputs: ScoringInputs {
            sampling_size,
            active_validators: inputs.active_validators.max(1),
        },
        gossip_threshold: GOSSIP_THRESHOLD,
        publish_threshold: PUBLISH_THRESHOLD,
        graylist_threshold: GRAYLIST_THRESHOLD,
        accept_px_threshold: ACCEPT_PX_THRESHOLD,
        opportunistic_graft_threshold: OPPORTUNISTIC_GRAFT_THRESHOLD,
        topic_score_cap: cap,
        max_positive_score: max_pos,
        app_specific_weight: 1.0,             // P5
        ip_colocation_factor_weight: -cap,    // P6 weight = −TopicScoreCap
        ip_colocation_factor_threshold: 10.0, // note recommendation 7 / Architecture
        behaviour_penalty_weight,
        behaviour_penalty_threshold,
        behaviour_penalty_decay,
        decay_interval,
        decay_to_zero: DECAY_TO_ZERO,
        retain_score,
        slow_peer_weight: 0.0,
        slow_peer_threshold: 0.0,
        slow_peer_decay: 0.2,
        topics,
    }
}

/// Look up the topic-score row for a family.
///
/// # Panics
///
/// Never panics for a config produced by [`build_scoring_config`] (always ten
/// families). Returns the first topic row as a defensive fallback if a hand-
/// built config is incomplete — production code only uses `build_scoring_config`.
#[must_use]
pub fn topic_for_family(cfg: &ScoringConfig, family: TopicFamily) -> &TopicScoreConfig {
    if let Some(t) = cfg.topics.iter().find(|t| t.family == family) {
        return t;
    }
    // `build_scoring_config` always emits all ten; keep production unwrap-free.
    &cfg.topics[0]
}

/// Convert to the plain libp2p translation struct for a concrete topic string.
#[must_use]
pub fn topic_to_libp2p(t: &TopicScoreConfig) -> TopicScoringConfig {
    TopicScoringConfig {
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
    }
}

/// Expand family rows onto concrete topic strings and produce a libp2p config.
///
/// `topics` is `(full_topic_string, family)` — typically from the topic
/// registry after digest expansion. Column family rows must use the
/// `sampling_size` this config was built with.
#[must_use]
pub fn to_libp2p_scoring_config(
    cfg: &ScoringConfig,
    topics: &[(String, TopicFamily)],
) -> Libp2pScoringConfig {
    let mut out_topics = Vec::with_capacity(topics.len());
    for (name, family) in topics {
        let row = topic_for_family(cfg, *family);
        out_topics.push((name.clone(), topic_to_libp2p(row)));
    }
    Libp2pScoringConfig {
        gossip_threshold: cfg.gossip_threshold,
        publish_threshold: cfg.publish_threshold,
        graylist_threshold: cfg.graylist_threshold,
        accept_px_threshold: cfg.accept_px_threshold,
        opportunistic_graft_threshold: cfg.opportunistic_graft_threshold,
        topic_score_cap: cfg.topic_score_cap,
        app_specific_weight: cfg.app_specific_weight,
        ip_colocation_factor_weight: cfg.ip_colocation_factor_weight,
        ip_colocation_factor_threshold: cfg.ip_colocation_factor_threshold,
        ip_colocation_factor_whitelist: Vec::new(),
        behaviour_penalty_weight: cfg.behaviour_penalty_weight,
        behaviour_penalty_threshold: cfg.behaviour_penalty_threshold,
        behaviour_penalty_decay: cfg.behaviour_penalty_decay,
        decay_interval: cfg.decay_interval,
        decay_to_zero: cfg.decay_to_zero,
        retain_score: cfg.retain_score,
        slow_peer_weight: cfg.slow_peer_weight,
        slow_peer_threshold: cfg.slow_peer_threshold,
        slow_peer_decay: cfg.slow_peer_decay,
        topics: out_topics,
    }
}

/// Expand Fulu topic names under `digest` for scoring attach.
///
/// Column topics are taken from **`sampled_column_ids`** — the sparse custody /
/// sample set (`get_custody_groups(node_id, sampling_size)`), **not** the dense
/// prefix `0..sampling_size`. Per-topic weight remains `0.5 / sampling_size`
/// from [`build_scoring_config`]; this helper only chooses which concrete
/// topics receive those params (ADR P2-07 / H1).
///
/// IDs outside `[0, counts.data_column_sidecar)` are skipped. Duplicates are
/// ignored (first occurrence wins).
#[must_use]
pub fn scoring_topic_keys(
    digest: &cc_types::ForkDigest,
    counts: SubnetCounts,
    sampled_column_ids: impl IntoIterator<Item = u64>,
) -> Vec<(String, TopicFamily)> {
    use super::topics::{TopicName, format_topic_string};
    use std::collections::BTreeSet;

    let mut out = Vec::new();
    let push = |out: &mut Vec<_>, name: TopicName| {
        let family = TopicFamily::from_topic_name(name);
        out.push((format_topic_string(digest, name), family));
    };

    push(&mut out, TopicName::BeaconBlock);
    push(&mut out, TopicName::BeaconAggregateAndProof);
    push(&mut out, TopicName::VoluntaryExit);
    push(&mut out, TopicName::ProposerSlashing);
    push(&mut out, TopicName::AttesterSlashing);
    push(&mut out, TopicName::BlsToExecutionChange);
    push(&mut out, TopicName::SyncCommitteeContributionAndProof);

    for id in 0..counts.attestation {
        push(&mut out, TopicName::BeaconAttestation(id));
    }
    for id in 0..counts.sync_committee {
        push(&mut out, TopicName::SyncCommittee(id));
    }

    // Sparse sample/custody set — never assume 0..n (H1).
    let mut seen = BTreeSet::new();
    for id in sampled_column_ids {
        if id >= counts.data_column_sidecar {
            continue;
        }
        if !seen.insert(id) {
            continue;
        }
        push(&mut out, TopicName::DataColumnSidecar(id));
    }
    out
}

// ── docs generation ─────────────────────────────────────────────────────────

/// Render `docs/p2p-scoring.md` from a [`ScoringConfig`].
#[must_use]
pub fn render_scoring_doc(cfg: &ScoringConfig) -> String {
    let mut s = String::new();
    s.push_str("# P2P GossipSub scoring parameters\n\n");
    s.push_str(
        "<!-- GENERATED by `cc_p2p::gossip::scoring::render_scoring_doc` — do not edit by hand.\n",
    );
    s.push_str(
        "     Source of truth: `services/p2p/src/gossip/scoring.rs` (`build_scoring_config`).\n",
    );
    s.push_str("     Regenerated by unit test `docs_p2p_scoring_md_matches_struct`. -->\n\n");
    s.push_str("Architecture §5.6 / CC-22c. P3/P3b weight **0** on every topic.\n\n");

    s.push_str("## Build inputs\n\n");
    s.push_str(&format!(
        "| Input | Value |\n|---|---|\n| `sampling_size` | {} |\n| `active_validators` (rate estimate) | {} |\n\n",
        cfg.inputs.sampling_size, cfg.inputs.active_validators
    ));

    s.push_str("## Global thresholds and decay\n\n");
    s.push_str("| Parameter | Value |\n|---|---|\n");
    s.push_str(&format!(
        "| `GossipThreshold` | {} |\n",
        cfg.gossip_threshold
    ));
    s.push_str(&format!(
        "| `PublishThreshold` | {} |\n",
        cfg.publish_threshold
    ));
    s.push_str(&format!(
        "| `GraylistThreshold` | {} |\n",
        cfg.graylist_threshold
    ));
    s.push_str(&format!(
        "| `AcceptPXThreshold` | {} |\n",
        cfg.accept_px_threshold
    ));
    s.push_str(&format!(
        "| `OpportunisticGraftThreshold` | {} |\n",
        cfg.opportunistic_graft_threshold
    ));
    s.push_str(&format!(
        "| `DecayInterval` | {} s |\n",
        cfg.decay_interval.as_secs()
    ));
    s.push_str(&format!("| `DecayToZero` | {} |\n", cfg.decay_to_zero));
    s.push_str(&format!(
        "| `RetainScore` | {} s (100 epochs) |\n",
        cfg.retain_score.as_secs()
    ));
    s.push_str(&format!(
        "| `max_positive_score` | {:.6} |\n",
        cfg.max_positive_score
    ));
    s.push_str(&format!(
        "| `TopicScoreCap` | {:.6} (`max_positive_score × 0.5`) |\n",
        cfg.topic_score_cap
    ));
    s.push_str(&format!(
        "| P5 `app_specific_weight` | {} |\n",
        cfg.app_specific_weight
    ));
    s.push_str(&format!(
        "| P6 `ip_colocation_factor_threshold` | {} |\n",
        cfg.ip_colocation_factor_threshold
    ));
    s.push_str(&format!(
        "| P6 `ip_colocation_factor_weight` | {:.6} (`−TopicScoreCap`) |\n",
        cfg.ip_colocation_factor_weight
    ));
    s.push_str(&format!(
        "| P7 `behaviour_penalty_threshold` | {} |\n",
        cfg.behaviour_penalty_threshold
    ));
    s.push_str(&format!(
        "| P7 `behaviour_penalty_decay` | {:.6} |\n",
        cfg.behaviour_penalty_decay
    ));
    s.push_str(&format!(
        "| P7 `behaviour_penalty_weight` | {} |\n\n",
        cfg.behaviour_penalty_weight
    ));

    s.push_str("## Per-topic family parameters\n\n");
    s.push_str("| Family | Weight | Family total | Derived | P2 decay | P2 cap | P2 weight | P3 | P3b | P4 weight | P4 decay | Rate/slot |\n");
    s.push_str("|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for t in &cfg.topics {
        s.push_str(&format!(
            "| `{}` | {:.6} | {:.6} | {} | {:.6} | {:.6} | {:.6} | {} | {} | {:.6} | {:.6} | {:.6} |\n",
            t.family.as_str(),
            t.topic_weight,
            t.family_total_weight,
            if t.derived { "yes" } else { "no" },
            t.first_message_deliveries_decay,
            t.first_message_deliveries_cap,
            t.first_message_deliveries_weight,
            t.mesh_message_deliveries_weight,
            t.mesh_failure_penalty_weight,
            t.invalid_message_deliveries_weight,
            t.invalid_message_deliveries_decay,
            t.expected_rate_per_slot,
        ));
    }
    s.push_str("\nP1 is uniform: weight `10/300 ≈ 0.033333`, quantum 1 slot (12 s), cap 300.\n");
    s.push_str("\nColumn weight = `0.5 / sampling_size` so the family total is always 0.5\n");
    s.push_str("regardless of `cgc` (ADR P2-07).\n");
    s
}

/// Path of the committed scoring document relative to the workspace root.
pub const SCORING_DOC_REL_PATH: &str = "docs/p2p-scoring.md";

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn default_cfg() -> ScoringConfig {
        build_scoring_config(ScoringInputs::default())
    }

    /// CC-22/2: one snapshot of shipped numbers (default sampling_size = 8).
    #[test]
    fn scoring_config_snapshot_shipped_numbers() {
        let cfg = default_cfg();

        assert_eq!(cfg.gossip_threshold, GOSSIP_THRESHOLD);
        assert_eq!(cfg.publish_threshold, PUBLISH_THRESHOLD);
        assert_eq!(cfg.graylist_threshold, GRAYLIST_THRESHOLD);
        assert_eq!(cfg.accept_px_threshold, 100.0);
        assert_eq!(cfg.opportunistic_graft_threshold, 5.0);
        assert_eq!(cfg.decay_interval, Duration::from_secs(12));
        assert_eq!(cfg.decay_to_zero, 0.01);
        assert_eq!(cfg.retain_score, Duration::from_secs(100 * 32 * 12));
        assert_eq!(cfg.app_specific_weight, 1.0);
        assert_eq!(cfg.ip_colocation_factor_threshold, 10.0);
        assert_eq!(cfg.behaviour_penalty_threshold, 6.0);
        assert!((cfg.behaviour_penalty_weight - (-15.92)).abs() < 1e-12);
        // scoreDecay(10 epochs) ≈ 0.9857
        assert!(
            (cfg.behaviour_penalty_decay - 0.9857).abs() < 5e-4,
            "P7 decay {}",
            cfg.behaviour_penalty_decay
        );

        // max_positive and cap are derived from family sum 2.8 → 140 / 70.
        assert!((family_weight_sum() - 2.8).abs() < 1e-12);
        assert!((cfg.max_positive_score - 140.0).abs() < 1e-9);
        assert!((cfg.topic_score_cap - 70.0).abs() < 1e-9);
        assert!((cfg.ip_colocation_factor_weight - (-70.0)).abs() < 1e-9);

        assert_eq!(cfg.topics.len(), 10);

        let block = topic_for_family(&cfg, TopicFamily::BeaconBlock);
        assert_eq!(block.topic_weight, 0.5);
        assert!((block.time_in_mesh_cap - 300.0).abs() < 1e-12);
        assert!((block.time_in_mesh_weight - (10.0 / 300.0)).abs() < 1e-12);
        // P2 decay 20 epochs ≈ 0.9928
        assert!(
            (block.first_message_deliveries_decay - 0.9928).abs() < 5e-4,
            "P2 decay {}",
            block.first_message_deliveries_decay
        );
        // P4 decay 50 epochs ≈ 0.9971
        assert!(
            (block.invalid_message_deliveries_decay - 0.9971).abs() < 5e-4,
            "P4 decay {}",
            block.invalid_message_deliveries_decay
        );
        assert!((block.invalid_message_deliveries_weight - (-140.0 / 0.5)).abs() < 1e-9);

        let att = topic_for_family(&cfg, TopicFamily::BeaconAttestation);
        assert!((att.topic_weight - 0.015625).abs() < 1e-12);

        let col = topic_for_family(&cfg, TopicFamily::DataColumnSidecar);
        assert!((col.topic_weight - (0.5 / 8.0)).abs() < 1e-12);
        assert!((col.family_total_weight - 0.5).abs() < 1e-12);
        assert!(col.derived);
    }

    #[test]
    fn p3_and_p3b_weight_zero_on_every_family() {
        let cfg = default_cfg();
        assert_eq!(TopicFamily::ALL.len(), 10);
        for family in TopicFamily::ALL {
            let t = topic_for_family(&cfg, family);
            assert_eq!(
                t.mesh_message_deliveries_weight, 0.0,
                "P3 weight for {:?}",
                family
            );
            assert_eq!(
                t.mesh_failure_penalty_weight, 0.0,
                "P3b weight for {:?}",
                family
            );
        }
    }

    #[test]
    fn column_family_total_half_for_sampling_sizes() {
        for sampling_size in [8_u64, 16, 128] {
            let cfg = build_scoring_config(ScoringInputs {
                sampling_size,
                active_validators: DEFAULT_ACTIVE_VALIDATORS,
            });
            let col = topic_for_family(&cfg, TopicFamily::DataColumnSidecar);
            let expected_w = 0.5 / sampling_size as f64;
            assert!(
                (col.topic_weight - expected_w).abs() < 1e-12,
                "weight at {sampling_size}: {}",
                col.topic_weight
            );
            assert!(
                (col.family_total_weight - 0.5).abs() < 1e-12,
                "family total at {sampling_size}"
            );
            // Per-topic × count recovers family total.
            assert!((col.topic_weight * sampling_size as f64 - 0.5).abs() < 1e-12);
            // max_positive independent of sampling_size.
            assert!((cfg.max_positive_score - 140.0).abs() < 1e-9);
        }
    }

    #[test]
    fn score_decay_table_matches_note() {
        // note: 1→0.8659, 5→0.9715, 10→0.9857, 20→0.9928, 50→0.9971, 100→0.9986
        let cases = [
            (1_u64, 0.8659),
            (5, 0.9715),
            (10, 0.9857),
            (20, 0.9928),
            (50, 0.9971),
            (100, 0.9986),
        ];
        for (epochs, expected) in cases {
            let d = score_decay_epochs(epochs);
            assert!(
                (d - expected).abs() < 5e-4,
                "epochs={epochs}: got {d}, expected ~{expected}"
            );
        }
    }

    #[test]
    fn docs_p2p_scoring_md_matches_struct() {
        let cfg = default_cfg();
        let rendered = render_scoring_doc(&cfg);

        // Resolve docs path relative to this crate's manifest (…/services/p2p)
        // → workspace root is ../..
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop(); // services
        path.pop(); // workspace root
        path.push(SCORING_DOC_REL_PATH);

        if std::env::var_os("UPDATE_SCORING_DOC").is_some() {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            std::fs::write(&path, &rendered).expect("write docs/p2p-scoring.md");
        }

        let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing {path:?}: {e}\nSet UPDATE_SCORING_DOC=1 to write, then re-run.\n--- rendered ---\n{rendered}"
            )
        });
        assert_eq!(
            committed, rendered,
            "docs/p2p-scoring.md drifted from ScoringConfig; UPDATE_SCORING_DOC=1 to regenerate"
        );
    }

    #[test]
    fn libp2p_translation_preserves_thresholds() {
        let cfg = default_cfg();
        let topics = vec![
            ("beacon_block".into(), TopicFamily::BeaconBlock),
            (
                "data_column_sidecar_0".into(),
                TopicFamily::DataColumnSidecar,
            ),
        ];
        let lib = to_libp2p_scoring_config(&cfg, &topics);
        assert_eq!(lib.gossip_threshold, cfg.gossip_threshold);
        assert_eq!(lib.publish_threshold, cfg.publish_threshold);
        assert_eq!(lib.graylist_threshold, cfg.graylist_threshold);
        assert_eq!(lib.topics.len(), 2);
        assert_eq!(lib.topics[1].1.mesh_message_deliveries_weight, 0.0);
        assert_eq!(lib.topics[1].1.mesh_failure_penalty_weight, 0.0);
    }

    #[test]
    fn scoring_topic_keys_uses_sparse_column_ids_not_dense_prefix() {
        use cc_types::ForkDigest;
        let digest = ForkDigest::from([0xaa, 0xbb, 0xcc, 0xdd]);
        let counts = SubnetCounts::mainnet();
        // Sparse custody-like set: high indices, not 0..8.
        let sampled = [3_u64, 17, 42, 99, 100, 3]; // trailing dup + out-of-range 100 if count=128
        let keys = scoring_topic_keys(&digest, counts, sampled);
        let cols: Vec<_> = keys
            .iter()
            .filter(|(_, f)| *f == TopicFamily::DataColumnSidecar)
            .map(|(s, _)| s.clone())
            .collect();
        // Exactly unique in-range IDs from the sample.
        assert_eq!(cols.len(), 5, "cols={cols:?}");
        assert!(cols.iter().any(|s| s.contains("data_column_sidecar_3")));
        assert!(cols.iter().any(|s| s.contains("data_column_sidecar_17")));
        assert!(cols.iter().any(|s| s.contains("data_column_sidecar_42")));
        assert!(cols.iter().any(|s| s.contains("data_column_sidecar_99")));
        // Must not invent dense prefix 0,1,2 when they were not sampled.
        assert!(
            !cols
                .iter()
                .any(|s| s.ends_with("/data_column_sidecar_0/ssz_snappy"))
        );
        assert!(
            !cols
                .iter()
                .any(|s| s.ends_with("/data_column_sidecar_1/ssz_snappy"))
        );
        // Out of range skipped if any.
        assert!(!cols.iter().any(|s| s.contains("data_column_sidecar_128")));
    }
}
