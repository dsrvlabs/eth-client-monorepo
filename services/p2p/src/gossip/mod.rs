//! Gossip pipeline surface — Architecture §5 / CC-22.
//!
//! | Module | Issue | Role |
//! |--------|-------|------|
//! | [`topics`] | CC-22a/b | `(digest, name)` keys, Fulu name expansion, topic strings, message-id |
//! | [`registry`] | CC-22a | sole `subscribe` / `unsubscribe` / `set_topic_params` owner; Steady→Overlap→Drain skeleton |
//! | [`validate`] | CC-22b/d | SSZ max table, block/column validators, pipeline |
//! | [`scoring`] | CC-22c | one `ScoringConfig`, P3/P3b weight 0, column `0.5/sampling_size`, docs gen |
//! | [`seen`] | CC-22d | bounded column/block seen sets |
//! | [`pending`] | CC-22d | pending-sidecar (256) / pending-block (64) queues |
//!
//! Operation / attestation / sync validators are IGNORE stubs until CC-2B/C/D.
//!
//! ## Spec delta 13 — `blob_sidecar_{subnet_id}`
//!
//! Deprecated in Fulu and **MUST NOT** be subscribed. Hoodi forked to Fulu at
//! epoch 50 688; we hold no pre-Fulu data. The deprecation is enforced by the
//! absence of a `TopicName` variant and by tests that assert no constructible
//! name matches `blob_sidecar_`. This comment is the only intentional
//! `blob_sidecar` mention under `services/p2p/src/`.

use cc_types::{DATA_COLUMN_SIDECAR_SUBNET_COUNT, Mainnet, Preset};

pub mod pending;
pub mod registry;
pub mod scoring;
pub mod seen;
pub mod topics;
pub mod validate;

pub use pending::{
    BoundedQueue, PendingBlock, PendingQueues, PendingSidecar, PendingSidecarReason,
    PENDING_BLOCK_BOUND, PENDING_SIDECAR_BOUND,
};
pub use registry::{
    GossipCall, GossipControlError, GossipsubControl, RecordingGossipsub, RegistryError,
    SubscriptionPhase, TopicParams, TopicRegistry,
};
pub use scoring::{
    ScoringConfig, ScoringInputs, TopicFamily, TopicScoreConfig, build_scoring_config,
    column_topic_weight, render_scoring_doc, to_libp2p_scoring_config,
};
pub use seen::{
    BlockSeenKey, BoundedSeenSet, ColumnSeenKey, SeenSets, BLOCK_SEEN_BOUND, COLUMN_SEEN_BOUND,
};
pub use topics::{
    MESSAGE_DOMAIN_INVALID_SNAPPY, MESSAGE_DOMAIN_VALID_SNAPPY, MESSAGE_ID_SIZE, SubnetCounts,
    TopicKey, TopicName, compute_message_id, ethereum_behaviour_config, expand_fulu_topic_names,
    expand_fulu_topic_strings, expand_fulu_topics, format_topic_string, gossipsub_message_id,
    message_id_invalid_snappy, message_id_valid_snappy,
};
pub use validate::{
    all_topics_have_validators, check_payload_len, check_payload_len_counted, max_container_bytes,
    parse_topic_name, production_kzg_verify, run_chain_in_late_verdicts, run_validation_pool,
    ssz_max, validator_kind, DecodeCounter, SizeError, ValidationPool, ValidatorKind,
    IN_FLIGHT_VALIDATION_CAP,
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
