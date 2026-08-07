//! Gossip pipeline surface — Architecture §5 / CC-22.
//!
//! | Module | Issue | Role |
//! |--------|-------|------|
//! | [`topics`] | CC-22a/b | `(digest, name)` keys, Fulu name expansion, topic strings, message-id |
//! | [`registry`] | CC-22a | sole `subscribe` / `unsubscribe` / `set_topic_params` owner; Steady→Overlap→Drain skeleton |
//! | [`validate`] | CC-22b | per-container SSZ maximum table and pre-decode size check |
//! | [`scoring`] | CC-22c | one `ScoringConfig`, P3/P3b weight 0, column `0.5/sampling_size`, docs gen |
//!
//! Later: per-topic validators (CC-22d / CC-2B/C/D).
//!
//! ## Spec delta 13 — `blob_sidecar_{subnet_id}`
//!
//! Deprecated in Fulu and **MUST NOT** be subscribed. Hoodi forked to Fulu at
//! epoch 50 688; we hold no pre-Fulu data. The deprecation is enforced by the
//! absence of a `TopicName` variant and by tests that assert no constructible
//! name matches `blob_sidecar_`. This comment is the only intentional
//! `blob_sidecar` mention under `services/p2p/src/`.

use cc_types::{DATA_COLUMN_SIDECAR_SUBNET_COUNT, Mainnet, Preset};

pub mod registry;
pub mod scoring;
pub mod topics;
pub mod validate;

pub use registry::{
    GossipCall, GossipControlError, GossipsubControl, RecordingGossipsub, RegistryError,
    SubscriptionPhase, TopicParams, TopicRegistry,
};
pub use scoring::{
    ScoringConfig, ScoringInputs, TopicFamily, TopicScoreConfig, build_scoring_config,
    column_topic_weight, render_scoring_doc, to_libp2p_scoring_config,
};
pub use topics::{
    MESSAGE_DOMAIN_INVALID_SNAPPY, MESSAGE_DOMAIN_VALID_SNAPPY, MESSAGE_ID_SIZE, SubnetCounts,
    TopicKey, TopicName, compute_message_id, ethereum_behaviour_config, expand_fulu_topic_names,
    expand_fulu_topic_strings, expand_fulu_topics, format_topic_string, gossipsub_message_id,
    message_id_invalid_snappy, message_id_valid_snappy,
};
pub use validate::{
    DecodeCounter, SizeError, check_payload_len, check_payload_len_counted, max_container_bytes,
    ssz_max,
};

/// Mainnet / Hoodi `ATTESTATION_SUBNET_COUNT` (phase0 networking config).
///
/// Supplied into [`SubnetCounts`] — never inlined inside `topics.rs` expansion.
pub const ATTESTATION_SUBNET_COUNT: u64 = 64;

impl SubnetCounts {
    /// Mainnet / Hoodi counts from `cc_types` / preset constants.
    #[must_use]
    pub const fn mainnet() -> Self {
        Self {
            attestation: ATTESTATION_SUBNET_COUNT,
            sync_committee: Mainnet::SYNC_COMMITTEE_SUBNET_COUNT,
            data_column_sidecar: DATA_COLUMN_SIDECAR_SUBNET_COUNT,
        }
    }
}
