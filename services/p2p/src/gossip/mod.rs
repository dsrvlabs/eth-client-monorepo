//! Gossip pipeline surface — Architecture §5 / CC-22.
//!
//! | Module | Issue | Role |
//! |--------|-------|------|
//! | [`topics`] | CC-22a | `(digest, name)` keys, Fulu name expansion, topic strings |
//! | [`registry`] | CC-22a | sole `subscribe` / `unsubscribe` / `set_topic_params` owner; Steady→Overlap→Drain skeleton |
//!
//! Later: `scoring` (CC-22c), validators (CC-22d), snappy transform (CC-22b).
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
pub mod topics;

pub use registry::{
    GossipCall, GossipControlError, GossipsubControl, RecordingGossipsub, RegistryError,
    SubscriptionPhase, TopicParams, TopicRegistry,
};
pub use topics::{
    SubnetCounts, TopicKey, TopicName, expand_fulu_topic_names, expand_fulu_topic_strings,
    expand_fulu_topics, format_topic_string,
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
