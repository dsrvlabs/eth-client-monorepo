//! Fulu gossip topic names and string construction — Architecture §5.1 / CC-22a.
//!
//! A topic is `(digest, name)` with the wire form
//! `/eth2/{fork_digest_hex}/{name}/ssz_snappy`. Subnet families expand from
//! **caller-supplied counts** ([`SubnetCounts`]) — this file never inlines
//! attestation / column / sync subnet cardinalities (devnets set them
//! differently; mainnet/Hoodi values come from `cc_types` / preset constants
//! via [`SubnetCounts::mainnet`]).
//!
//! **Deprecated:** `blob_sidecar_{subnet_id}` is gone in Fulu (spec delta 13).
//! There is no [`TopicName`] variant for it; enumeration tests assert absence.

use cc_types::ForkDigest;

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
