//! Fulu gossip topic names, string construction, and message-id — §5.1–§5.2 / CC-22a–b.
//!
//! A topic is `(digest, name)` with the wire form
//! `/eth2/{fork_digest_hex}/{name}/ssz_snappy`. Subnet families expand from
//! **caller-supplied counts** ([`SubnetCounts`]) — this file never inlines
//! attestation / column / sync subnet cardinalities (devnets set them
//! differently; mainnet/Hoodi values come from `cc_types` / preset constants
//! via [`SubnetCounts::mainnet`]).
//!
//! **Message id** (Altair+ / Fulu preimage, §5.2):
//! `SHA256(domain ‖ uint64_le(len(topic)) ‖ topic ‖ payload)[..20]`.
//! With [`cc_libp2p::SnappyTransform`] installed, gossipsub runs the id function
//! on **decompressed** bytes and the production path always uses
//! [`MESSAGE_DOMAIN_VALID_SNAPPY`] via [`gossipsub_message_id`] /
//! [`ethereum_behaviour_config`]. The invalid-snappy domain is retained for
//! offline fixtures: transform failures drop the message **before** the id
//! function runs, so the live path never hashes raw wire bytes.
//!
//! **Deprecated:** `blob_sidecar_{subnet_id}` is gone in Fulu (spec delta 13).
//! There is no [`TopicName`] variant for it; enumeration tests assert absence.

use sha2::{Digest, Sha256};

use cc_libp2p::reexport::gossipsub::{Message, MessageId};
use cc_libp2p::BehaviourConfig;
use cc_types::ForkDigest;

// ── Message-id domains (§14/1 scaffold read from specs/phase0/p2p-interface.md
//    at v1.7.0-alpha.13; Altair extends the preimage with topic length + topic) ─

/// `MESSAGE_DOMAIN_VALID_SNAPPY` = `DomainType('0x01000000')` (little-endian).
///
/// Used when snappy decompression succeeded; `payload` is the decompressed
/// SSZ bytes (what gossipsub hands the id function after `SnappyTransform`).
pub const MESSAGE_DOMAIN_VALID_SNAPPY: [u8; 4] = [0x01, 0x00, 0x00, 0x00];

/// `MESSAGE_DOMAIN_INVALID_SNAPPY` = `DomainType('0x00000000')` (little-endian).
///
/// Used when snappy decompression failed; `payload` is the raw wire bytes.
pub const MESSAGE_DOMAIN_INVALID_SNAPPY: [u8; 4] = [0x00, 0x00, 0x00, 0x00];

/// Length of the gossipsub message-id (spec: first 20 bytes of SHA256).
pub const MESSAGE_ID_SIZE: usize = 20;

/// Compute the Altair+ gossipsub message-id for `(topic, payload, domain)`.
///
/// Preimage (specs/altair/p2p-interface.md, pin `v1.7.0-alpha.13`):
/// `SHA256(domain ‖ uint_to_bytes(uint64(len(topic))) ‖ topic ‖ payload)[..20]`
/// with little-endian length encoding.
///
/// When the inbound snappy transform succeeded, callers pass
/// [`MESSAGE_DOMAIN_VALID_SNAPPY`] and the **decompressed** payload. When it
/// failed, pass [`MESSAGE_DOMAIN_INVALID_SNAPPY`] and the raw bytes.
#[must_use]
pub fn compute_message_id(topic: &str, payload: &[u8], domain: [u8; 4]) -> [u8; MESSAGE_ID_SIZE] {
    let topic_bytes = topic.as_bytes();
    // uint_to_bytes(uint64(len(topic))) — little-endian, always 8 bytes.
    let topic_len = (topic_bytes.len() as u64).to_le_bytes();

    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(topic_len);
    hasher.update(topic_bytes);
    hasher.update(payload);
    let digest = hasher.finalize();

    let mut id = [0u8; MESSAGE_ID_SIZE];
    id.copy_from_slice(&digest[..MESSAGE_ID_SIZE]);
    id
}

/// Message-id for a successfully snappy-decoded gossip payload (production path).
///
/// `payload` must already be the **decompressed** SSZ bytes.
#[must_use]
pub fn message_id_valid_snappy(topic: &str, decompressed_payload: &[u8]) -> [u8; MESSAGE_ID_SIZE] {
    compute_message_id(
        topic,
        decompressed_payload,
        MESSAGE_DOMAIN_VALID_SNAPPY,
    )
}

/// Message-id for a payload that failed snappy decompression (hostile / raw).
///
/// `raw_payload` is the undecoded wire bytes.
///
/// **Live path:** with `SnappyTransform` installed, gossipsub never calls the
/// message-id function on a failed decompress (the transform returns `Err` and
/// the message is dropped). This helper exists for the committed fixture and
/// for any future path that evaluates raw wire bytes offline.
#[must_use]
pub fn message_id_invalid_snappy(topic: &str, raw_payload: &[u8]) -> [u8; MESSAGE_ID_SIZE] {
    compute_message_id(topic, raw_payload, MESSAGE_DOMAIN_INVALID_SNAPPY)
}

/// Gossipsub `message_id_fn` for the production path (SEC C1 / CC-22b).
///
/// Reads `message.topic` + **decompressed** `message.data` (post-transform)
/// under [`MESSAGE_DOMAIN_VALID_SNAPPY`]. Install via
/// [`ethereum_behaviour_config`] or
/// [`BehaviourConfig::with_message_id_fn`].
#[must_use]
pub fn gossipsub_message_id(message: &Message) -> MessageId {
    let id = message_id_valid_snappy(message.topic.as_str(), &message.data);
    MessageId::new(&id)
}

/// [`BehaviourConfig`] with the scaffold-owned eth2 message-id installed.
///
/// Prefer this over bare [`BehaviourConfig::default`] at every production and
/// integration build site so the live path and the committed fixture share
/// one implementation (`compute_message_id` in this module). The libp2p crate
/// also defaults to the same preimage as a safety net.
#[must_use]
pub fn ethereum_behaviour_config() -> BehaviourConfig {
    BehaviourConfig::default().with_message_id_fn(gossipsub_message_id)
}

/// Subnet-family cardinalities used to expand the three Fulu subnet topics.
///
/// All three fields are **caller-supplied** (see [`crate::gossip::SubnetCounts::mainnet`]
/// for the mainnet/Hoodi source of each constant). Expansion loops read only
/// these fields — no attestation/column/sync cardinalities are inlined here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubnetCounts {
    /// `ATTESTATION_SUBNET_COUNT` — `beacon_attestation_{subnet_id}` range.
    pub attestation: u64,
    /// `SYNC_COMMITTEE_SUBNET_COUNT` — `sync_committee_{subnet_id}` range.
    pub sync_committee: u64,
    /// `DATA_COLUMN_SIDECAR_SUBNET_COUNT` — `data_column_sidecar_{subnet_id}` range.
    pub data_column_sidecar: u64,
}

/// One of the ten Fulu gossip topic names (subnet families carry an id).
///
/// Constructed names map 1:1 onto the path segment between digest and
/// `ssz_snappy`. There is deliberately **no** `BlobSidecar` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TopicName {
    /// `beacon_block`
    BeaconBlock,
    /// `beacon_aggregate_and_proof`
    BeaconAggregateAndProof,
    /// `beacon_attestation_{subnet_id}`
    BeaconAttestation(u64),
    /// `data_column_sidecar_{subnet_id}`
    DataColumnSidecar(u64),
    /// `sync_committee_contribution_and_proof`
    SyncCommitteeContributionAndProof,
    /// `sync_committee_{subnet_id}`
    SyncCommittee(u64),
    /// `voluntary_exit`
    VoluntaryExit,
    /// `proposer_slashing`
    ProposerSlashing,
    /// `attester_slashing`
    AttesterSlashing,
    /// `bls_to_execution_change`
    BlsToExecutionChange,
}

impl TopicName {
    /// Path-segment form used inside the topic string (no leading slash).
    #[must_use]
    pub fn path_segment(self) -> String {
        match self {
            Self::BeaconBlock => "beacon_block".to_owned(),
            Self::BeaconAggregateAndProof => "beacon_aggregate_and_proof".to_owned(),
            Self::BeaconAttestation(id) => format!("beacon_attestation_{id}"),
            Self::DataColumnSidecar(id) => format!("data_column_sidecar_{id}"),
            Self::SyncCommitteeContributionAndProof => {
                "sync_committee_contribution_and_proof".to_owned()
            }
            Self::SyncCommittee(id) => format!("sync_committee_{id}"),
            Self::VoluntaryExit => "voluntary_exit".to_owned(),
            Self::ProposerSlashing => "proposer_slashing".to_owned(),
            Self::AttesterSlashing => "attester_slashing".to_owned(),
            Self::BlsToExecutionChange => "bls_to_execution_change".to_owned(),
        }
    }
}

/// Registry key: `(fork_digest, topic_name)`.
///
/// Both digests' topics coexist during a BPO transition without special-casing
/// — the pair is the unit of subscription bookkeeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicKey {
    /// Fork digest embedded in the topic string.
    pub digest: ForkDigest,
    /// Fulu topic name (possibly subnet-expanded).
    pub name: TopicName,
}

impl TopicKey {
    /// Construct a key from digest + name.
    #[must_use]
    pub const fn new(digest: ForkDigest, name: TopicName) -> Self {
        Self { digest, name }
    }

    /// Full gossip topic string.
    #[must_use]
    pub fn topic_string(&self) -> String {
        format_topic_string(&self.digest, self.name)
    }
}

/// Build `/eth2/{fork_digest_hex}/{name}/ssz_snappy`.
///
/// Digest is lowercase hex **without** a `0x` prefix (libp2p / eth2 convention).
#[must_use]
pub fn format_topic_string(digest: &ForkDigest, name: TopicName) -> String {
    let d = digest.as_slice();
    format!(
        "/eth2/{:02x}{:02x}{:02x}{:02x}/{}/ssz_snappy",
        d[0],
        d[1],
        d[2],
        d[3],
        name.path_segment()
    )
}

/// Expand the ten Fulu names over the three subnet families using `counts`.
///
/// Order is stable and matches the committed Hoodi fixture:
/// block, aggregate, attestation×N, column×N, sync contribution, sync×N,
/// voluntary_exit, proposer_slashing, attester_slashing, bls_to_execution_change.
#[must_use]
pub fn expand_fulu_topic_names(counts: &SubnetCounts) -> Vec<TopicName> {
    let mut names = Vec::new();
    names.push(TopicName::BeaconBlock);
    names.push(TopicName::BeaconAggregateAndProof);
    for id in 0..counts.attestation {
        names.push(TopicName::BeaconAttestation(id));
    }
    for id in 0..counts.data_column_sidecar {
        names.push(TopicName::DataColumnSidecar(id));
    }
    names.push(TopicName::SyncCommitteeContributionAndProof);
    for id in 0..counts.sync_committee {
        names.push(TopicName::SyncCommittee(id));
    }
    names.push(TopicName::VoluntaryExit);
    names.push(TopicName::ProposerSlashing);
    names.push(TopicName::AttesterSlashing);
    names.push(TopicName::BlsToExecutionChange);
    names
}

/// Expand every Fulu topic name under `digest` into [`TopicKey`]s.
#[must_use]
pub fn expand_fulu_topics(digest: ForkDigest, counts: &SubnetCounts) -> Vec<TopicKey> {
    expand_fulu_topic_names(counts)
        .into_iter()
        .map(|name| TopicKey::new(digest, name))
        .collect()
}

/// All topic strings for `digest` under `counts`, in fixture order.
#[must_use]
pub fn expand_fulu_topic_strings(digest: ForkDigest, counts: &SubnetCounts) -> Vec<String> {
    expand_fulu_topics(digest, counts)
        .into_iter()
        .map(|k| k.topic_string())
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn message_id_domains_match_spec_v1_7_0_alpha_13() {
        // Scaffold-time read (§14/1): DomainType little-endian encoding.
        assert_eq!(MESSAGE_DOMAIN_VALID_SNAPPY, [0x01, 0x00, 0x00, 0x00]);
        assert_eq!(MESSAGE_DOMAIN_INVALID_SNAPPY, [0x00, 0x00, 0x00, 0x00]);
        assert_eq!(MESSAGE_ID_SIZE, 20);
    }

    #[test]
    fn message_id_uses_decompressed_payload_not_compressed() {
        // Compressed and decompressed bytes differ; id must match the
        // decompressed preimage (what SnappyTransform hands gossipsub).
        let topic = "/eth2/c6ecb76c/beacon_block/ssz_snappy";
        let decompressed = b"ssz-payload-bytes";
        let compressed = b"\xffsnappy-looking-but-different";
        assert_ne!(
            decompressed.as_slice(),
            compressed.as_slice(),
            "fixture assumes distinct byte strings"
        );

        let id_from_decompressed = message_id_valid_snappy(topic, decompressed);
        let id_if_wrongly_used_compressed = message_id_valid_snappy(topic, compressed);
        assert_ne!(
            id_from_decompressed, id_if_wrongly_used_compressed,
            "id must change when payload bytes change"
        );

        // Explicit preimage: domain ‖ le64(len(topic)) ‖ topic ‖ decompressed.
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&MESSAGE_DOMAIN_VALID_SNAPPY);
        preimage.extend_from_slice(&(topic.len() as u64).to_le_bytes());
        preimage.extend_from_slice(topic.as_bytes());
        preimage.extend_from_slice(decompressed);
        let expected = {
            use sha2::{Digest, Sha256};
            let d = Sha256::digest(&preimage);
            let mut id = [0u8; 20];
            id.copy_from_slice(&d[..20]);
            id
        };
        assert_eq!(id_from_decompressed, expected);
    }

    #[test]
    fn message_id_invalid_snappy_uses_raw_bytes() {
        let topic = "/eth2/c6ecb76c/beacon_block/ssz_snappy";
        let raw = b"\xffnot-snappy";
        let id = message_id_invalid_snappy(topic, raw);
        let via_compute = compute_message_id(topic, raw, MESSAGE_DOMAIN_INVALID_SNAPPY);
        assert_eq!(id, via_compute);
        // Distinct from the valid-domain id over the same bytes.
        assert_ne!(id, message_id_valid_snappy(topic, raw));
    }

    #[test]
    fn gossipsub_message_id_matches_valid_snappy_helper() {
        use cc_libp2p::reexport::gossipsub::TopicHash;

        let topic = "/eth2/c6ecb76c/beacon_block/ssz_snappy";
        let data = b"decompressed-ssz";
        let message = Message {
            source: None,
            data: data.to_vec(),
            sequence_number: None,
            topic: TopicHash::from_raw(topic),
        };
        let mid = gossipsub_message_id(&message);
        let expected = message_id_valid_snappy(topic, data);
        assert_eq!(mid.0.as_slice(), expected.as_slice());
        // Agrees with the libp2p default eth2 id (same preimage).
        let via_libp2p = cc_libp2p::default_eth2_message_id(&message);
        assert_eq!(mid.0, via_libp2p.0);
    }

    #[test]
    fn ethereum_behaviour_config_installs_p2p_message_id_fn() {
        // SEC C1: production config carries a message_id_fn that matches the
        // fixture-owned preimage (not libp2p's default seqno/from hash).
        let cfg = ethereum_behaviour_config();
        let topic = "/eth2/aabbccdd/voluntary_exit/ssz_snappy";
        let data = b"payload";
        let message = Message {
            source: None,
            data: data.to_vec(),
            sequence_number: Some(99),
            topic: cc_libp2p::reexport::gossipsub::TopicHash::from_raw(topic),
        };
        let id = (cfg.message_id_fn)(&message);
        assert_eq!(id.0.len(), MESSAGE_ID_SIZE);
        assert_eq!(
            id.0.as_slice(),
            message_id_valid_snappy(topic, data).as_slice()
        );
        // Sequence number must NOT enter the preimage (content-addressed).
        let message2 = Message {
            sequence_number: Some(1),
            ..message
        };
        assert_eq!(id.0, (cfg.message_id_fn)(&message2).0);
    }

    #[test]
    fn format_topic_string_matches_eth2_shape() {
        let digest = ForkDigest::from_array([0xc6, 0xec, 0xb7, 0x6c]);
        assert_eq!(
            format_topic_string(&digest, TopicName::BeaconBlock),
            "/eth2/c6ecb76c/beacon_block/ssz_snappy"
        );
        assert_eq!(
            format_topic_string(&digest, TopicName::BeaconAttestation(7)),
            "/eth2/c6ecb76c/beacon_attestation_7/ssz_snappy"
        );
        assert_eq!(
            format_topic_string(&digest, TopicName::DataColumnSidecar(127)),
            "/eth2/c6ecb76c/data_column_sidecar_127/ssz_snappy"
        );
    }

    #[test]
    fn expand_uses_supplied_counts_not_literals() {
        let counts = SubnetCounts {
            attestation: 2,
            sync_committee: 1,
            data_column_sidecar: 3,
        };
        let names = expand_fulu_topic_names(&counts);
        // 2 fixed + 2 att + 3 col + 1 sync-contrib + 1 sync + 4 ops = 13
        assert_eq!(names.len(), 2 + 2 + 3 + 1 + 1 + 4);
        assert!(names.contains(&TopicName::BeaconAttestation(0)));
        assert!(names.contains(&TopicName::BeaconAttestation(1)));
        assert!(!names.contains(&TopicName::BeaconAttestation(2)));
        assert!(names.contains(&TopicName::DataColumnSidecar(2)));
        assert!(!names.contains(&TopicName::DataColumnSidecar(3)));
        assert!(names.contains(&TopicName::SyncCommittee(0)));
        assert!(!names.contains(&TopicName::SyncCommittee(1)));
    }

    #[test]
    fn every_constructible_name_is_one_of_the_ten_fulu_families() {
        // Structural half of the deprecation guard: no extra family appears in
        // the expansion (the deprecated Deneb name has no `TopicName` variant).
        // String-level absence is asserted in `tests/topic_registry.rs` so that
        // the banned path segment does not appear under `services/p2p/src/`.
        let counts = SubnetCounts {
            attestation: 2,
            sync_committee: 2,
            data_column_sidecar: 2,
        };
        for name in expand_fulu_topic_names(&counts) {
            let seg = name.path_segment();
            let ok = seg == "beacon_block"
                || seg == "beacon_aggregate_and_proof"
                || seg.starts_with("beacon_attestation_")
                || seg.starts_with("data_column_sidecar_")
                || seg == "sync_committee_contribution_and_proof"
                || (seg.starts_with("sync_committee_") && !seg.contains("contribution"))
                || seg == "voluntary_exit"
                || seg == "proposer_slashing"
                || seg == "attester_slashing"
                || seg == "bls_to_execution_change";
            assert!(ok, "unexpected topic family: {seg}");
        }
    }

    #[test]
    fn topic_key_pairs_differ_by_digest() {
        let d0 = ForkDigest::from_array([0x00, 0x00, 0x00, 0x01]);
        let d1 = ForkDigest::from_array([0x00, 0x00, 0x00, 0x02]);
        let a = TopicKey::new(d0, TopicName::BeaconBlock);
        let b = TopicKey::new(d1, TopicName::BeaconBlock);
        assert_ne!(a, b);
        assert!(a.topic_string().contains("00000001"));
    }
}
