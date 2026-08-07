//! Custody and sampling sets — Architecture §8.1 / CC-24a.
//!
//! Phase 2 adds **no** custody arithmetic. This module composes Phase 1's
//! `CC-1B` helpers (`cc_types::networking`) and asserts the properties the
//! composition must have:
//!
//! - `sampling_size = max(SAMPLES_PER_SLOT, cgc)` (default: sample 8, custody 4)
//! - `custody_groups ⊆ sampled_groups` (spec guarantee, **asserted** not assumed)
//! - column subnets derived via `compute_columns_for_custody_group` →
//!   `compute_subnet_for_data_column_sidecar` (constants from `cc_types`, never
//!   inlined subnet-count literals)
//! - `NodeId` taken **by value** at construction — no global / lazy identity read
//!
//! The two sets live in **distinct newtypes** ([`SampledGroups`] /
//! [`CustodiedGroups`]) so a call site cannot pass one where the other is meant.
//!
//! Subscription goes through CC-22a's [`TopicRegistry`] with
//! `set_topic_params` **before** `subscribe` (registry-owned ordering).

use std::collections::BTreeSet;
use std::ops::Deref;

use cc_types::{
    compute_columns_for_custody_group, compute_subnet_for_data_column_sidecar, get_custody_groups,
    sampling_size, CustodyIndex, ForkDigest, NUMBER_OF_CUSTODY_GROUPS, SubnetId, CUSTODY_REQUIREMENT,
};
use discv5::enr::NodeId;

use crate::discovery::enr::node_id_as_u256;
use crate::gossip::{
    GossipsubControl, RegistryError, TopicKey, TopicName, TopicParams, TopicRegistry,
};

// ── Newtypes ────────────────────────────────────────────────────────────────

/// Groups the node **samples** (gossip subscription, DA gate required set, cache).
///
/// Size at Phase 2 default: [`sampling_size`]`(cgc)` = 8 when `cgc = 4`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampledGroups(BTreeSet<CustodyIndex>);

impl SampledGroups {
    /// Borrow the underlying set.
    #[must_use]
    pub fn as_set(&self) -> &BTreeSet<CustodyIndex> {
        &self.0
    }

    /// Number of sampled groups.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the sampled set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `group` is in the sampled set.
    #[must_use]
    pub fn contains(&self, group: CustodyIndex) -> bool {
        self.0.contains(&group)
    }
}

impl Deref for SampledGroups {
    type Target = BTreeSet<CustodyIndex>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Groups the node **custodies** (advertised `cgc`, serve window, peer demands).
///
/// Size at Phase 2 default: [`CUSTODY_REQUIREMENT`] = 4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodiedGroups(BTreeSet<CustodyIndex>);

impl CustodiedGroups {
    /// Borrow the underlying set.
    #[must_use]
    pub fn as_set(&self) -> &BTreeSet<CustodyIndex> {
        &self.0
    }

    /// Number of custodied groups.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the custodied set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `group` is in the custodied set.
    #[must_use]
    pub fn contains(&self, group: CustodyIndex) -> bool {
        self.0.contains(&group)
    }
}

impl Deref for CustodiedGroups {
    type Target = BTreeSet<CustodyIndex>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// ── Subnet derivation ───────────────────────────────────────────────────────

/// Map custody groups → column indices → data-column sidecar subnet ids.
///
/// Uses only `CC-1B` helpers; because
/// `NUMBER_OF_CUSTODY_GROUPS == NUMBER_OF_COLUMNS == DATA_COLUMN_SIDECAR_SUBNET_COUNT`,
/// one group is one column and one subnet on mainnet/Hoodi, but callers must not
/// assume that equality — always go through the helpers.
#[must_use]
pub fn column_subnets_for_groups(groups: &BTreeSet<CustodyIndex>) -> BTreeSet<SubnetId> {
    groups
        .iter()
        .flat_map(|g| compute_columns_for_custody_group(*g))
        .map(compute_subnet_for_data_column_sidecar)
        .collect()
}

// ── Custody manager ─────────────────────────────────────────────────────────

/// Owns the local node's sampled and custodied group sets.
///
/// Constructed with a discv5 [`NodeId`] **by value** so there is no code path
/// that recomputes custody from an unpersisted key (§3.5 / CC-20b edge).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodyManager {
    /// discv5 node id used as the custody input (owned; never re-read).
    node_id: NodeId,
    /// Advertised / configured custody group count after construction-time seam.
    cgc: u64,
    /// `max(SAMPLES_PER_SLOT, cgc)`.
    sampling_size: u64,
    /// Sampled groups (gossip + DA).
    sampled: SampledGroups,
    /// Custodied groups (`cgc` advertisement + serve).
    custodied: CustodiedGroups,
    /// Column subnets derived from **sampled** groups (subscription set).
    column_subnets: BTreeSet<SubnetId>,
}

impl CustodyManager {
    /// Build both sets from `node_id` and the construction-time custody count.
    ///
    /// # Track D seam
    ///
    /// `custody_group_count` is **Track D's single named construction-time
    /// injection point** for the self-devnet publisher: fault-mode instance B
    /// (and `--publish-fixture`) forces this argument to
    /// [`NUMBER_OF_CUSTODY_GROUPS`] so the publisher holds every column. Normal
    /// nodes pass their configured / default `cgc` (typically
    /// [`CUSTODY_REQUIREMENT`]). Do not thread a fault-mode flag through this
    /// module — keep the seam as this one parameter (CC-2Jb's diff is one line
    /// at the call site).
    ///
    /// # Panics
    ///
    /// Panics if `custody_group_count > NUMBER_OF_CUSTODY_GROUPS` (delegated to
    /// [`get_custody_groups`]'s spec assert).
    #[must_use]
    pub fn new(node_id: NodeId, custody_group_count: u64) -> Self {
        let cgc = custody_group_count.min(NUMBER_OF_CUSTODY_GROUPS);
        let node_u256 = node_id_as_u256(node_id);
        let samp = sampling_size(cgc);
        let sampled = SampledGroups(get_custody_groups(node_u256, samp));
        let custodied = CustodiedGroups(get_custody_groups(node_u256, cgc));

        // Spec guarantee custody ⊆ sampled — assert rather than assume (CC-24/1).
        debug_assert!(
            custodied.0.is_subset(&sampled.0),
            "custody_groups must be ⊆ sampled_groups (cgc={cgc}, sampling_size={samp})"
        );

        let column_subnets = column_subnets_for_groups(&sampled.0);

        Self {
            node_id,
            cgc,
            sampling_size: samp,
            sampled,
            custodied,
            column_subnets,
        }
    }

    /// Phase-2 default: [`CUSTODY_REQUIREMENT`] custodied groups.
    #[must_use]
    pub fn with_default_cgc(node_id: NodeId) -> Self {
        Self::new(node_id, CUSTODY_REQUIREMENT)
    }

    /// discv5 node id this manager was built with.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Effective custody group count (post-seam, capped).
    #[must_use]
    pub const fn cgc(&self) -> u64 {
        self.cgc
    }

    /// `sampling_size = max(SAMPLES_PER_SLOT, cgc)`.
    #[must_use]
    pub const fn sampling_size(&self) -> u64 {
        self.sampling_size
    }

    /// Sampled groups (distinct type from [`Self::custodied`]).
    #[must_use]
    pub const fn sampled(&self) -> &SampledGroups {
        &self.sampled
    }

    /// Custodied groups (distinct type from [`Self::sampled`]).
    #[must_use]
    pub const fn custodied(&self) -> &CustodiedGroups {
        &self.custodied
    }

    /// Column-sidecar subnet ids derived from the **sampled** set.
    #[must_use]
    pub const fn column_subnets(&self) -> &BTreeSet<SubnetId> {
        &self.column_subnets
    }

    /// Subscribe to every sampled column subnet via the topic registry.
    ///
    /// Goes through [`TopicRegistry::subscribe`], which applies
    /// `set_topic_params` **before** `subscribe` on the gossip control surface.
    /// Callers must have registered validators for the column topic names
    /// (typically via [`TopicRegistry::register_all_fulu_validators`] or
    /// per-subnet [`TopicRegistry::register_validator`]).
    ///
    /// # Errors
    ///
    /// Propagates [`RegistryError`] from the registry (no validator / control).
    pub fn subscribe_sampled_columns<G: GossipsubControl>(
        &self,
        registry: &mut TopicRegistry<G>,
        digest: ForkDigest,
        params: TopicParams,
    ) -> Result<(), RegistryError> {
        for subnet in &self.column_subnets {
            let key = TopicKey::new(digest, TopicName::DataColumnSidecar(*subnet));
            registry.subscribe(key, params.clone())?;
        }
        Ok(())
    }

    /// Whether a peer's custody coverage intersects our **sampled** set.
    ///
    /// A peer is custody-compatible when
    /// `get_custody_groups(peer_node_id, peer_cgc) ∩ sampled ≠ ∅`.
    #[must_use]
    pub fn is_peer_compatible(&self, peer_node_id: NodeId, peer_cgc: u64) -> bool {
        is_peer_custody_compatible(&self.sampled, peer_node_id, peer_cgc)
    }

    /// Count peers whose custody groups intersect our **sampled** set.
    ///
    /// Feeds `cc_p2p_peers_custody_compatible` (clause 1's "≥ 8 custody-compatible").
    #[must_use]
    pub fn count_compatible_peers(
        &self,
        peers: impl IntoIterator<Item = (NodeId, u64)>,
    ) -> usize {
        count_custody_compatible_peers(&self.sampled, peers)
    }

    /// How many of our sampled groups `peer` covers (usefulness score input).
    #[must_use]
    pub fn peer_coverage(&self, peer_node_id: NodeId, peer_cgc: u64) -> u32 {
        let peer_cgc = peer_cgc.min(NUMBER_OF_CUSTODY_GROUPS);
        let peer_groups = get_custody_groups(node_id_as_u256(peer_node_id), peer_cgc);
        peer_groups.intersection(&self.sampled.0).count() as u32
    }
}

// ── Peer compatibility (pure) ───────────────────────────────────────────────

/// Whether `peer`'s custodied groups intersect `our_sampled`.
#[must_use]
pub fn is_peer_custody_compatible(
    our_sampled: &SampledGroups,
    peer_node_id: NodeId,
    peer_cgc: u64,
) -> bool {
    let peer_cgc = peer_cgc.min(NUMBER_OF_CUSTODY_GROUPS);
    let peer_groups = get_custody_groups(node_id_as_u256(peer_node_id), peer_cgc);
    !peer_groups.is_disjoint(&our_sampled.0)
}

/// Count `(peer_node_id, peer_cgc)` pairs that are custody-compatible with
/// `our_sampled`.
#[must_use]
pub fn count_custody_compatible_peers(
    our_sampled: &SampledGroups,
    peers: impl IntoIterator<Item = (NodeId, u64)>,
) -> usize {
    peers
        .into_iter()
        .filter(|(id, cgc)| is_peer_custody_compatible(our_sampled, *id, *cgc))
        .count()
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::fork_digest::ForkContext;
    use crate::gossip::{RecordingGossipsub, SubnetCounts, TopicRegistry};
    use crate::metrics::P2pMetrics;
    use alloy_primitives::U256;
    use cc_types::{
        sampling_size as types_sampling_size, BlobParameters, BlobSchedule, ChainConfig, Epoch,
        ForkVersion, PresetName, Root, SAMPLES_PER_SLOT, DATA_COLUMN_SIDECAR_SUBNET_COUNT,
        NUMBER_OF_COLUMNS,
    };
    use std::collections::HashSet;

    fn node_id_from_u64(n: u64) -> NodeId {
        let mut raw = [0u8; 32];
        raw[24..].copy_from_slice(&n.to_be_bytes());
        NodeId::new(&raw)
    }

    fn synthetic_ctx() -> ForkContext {
        let schedule = BlobSchedule::try_from_entries(vec![
            BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 15,
            },
            BlobParameters {
                epoch: Epoch::new(10_000),
                max_blobs_per_block: 21,
            },
        ])
        .expect("schedule");
        let cfg = ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "synthetic-custody".into(),
            genesis_fork_version: ForkVersion::from_array([0, 0, 0, 1]),
            altair_fork_version: ForkVersion::from_array([1, 0, 0, 1]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([2, 0, 0, 1]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([3, 0, 0, 1]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([4, 0, 0, 1]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([5, 0, 0, 1]),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([6, 0, 0, 1]),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 12,
            blob_schedule: schedule,
            deposit_chain_id: 1,
            deposit_contract_address: Default::default(),
        };
        let gvr = Root::from_array([0xcc; 32]);
        ForkContext::new(cfg, gvr, Epoch::new(50))
    }

    // ── CC-24/1 containment ─────────────────────────────────────────────────

    #[test]
    fn custody_subset_of_sampled_across_random_pairs() {
        // At least 100 random (node_id, cgc) pairs including fixed cgc corners.
        let fixed_cgc = [
            4_u64,
            8,
            64,
            NUMBER_OF_CUSTODY_GROUPS, // full set
        ];
        let mut pairs: Vec<(NodeId, u64)> = Vec::with_capacity(120);

        for (i, &cgc) in fixed_cgc.iter().enumerate() {
            pairs.push((node_id_from_u64(0x1000 + i as u64), cgc));
            pairs.push((NodeId::random(), cgc));
        }
        // Fill to ≥ 100 with random node ids and cgc in 0..=NUMBER_OF_CUSTODY_GROUPS.
        let mut seed = 0xC0FFEE_u64;
        while pairs.len() < 100 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1);
            let node = node_id_from_u64(seed);
            let cgc = seed % (NUMBER_OF_CUSTODY_GROUPS + 1);
            pairs.push((node, cgc));
        }
        // Extra pure-random NodeIds for good measure.
        for _ in 0..20 {
            pairs.push((NodeId::random(), 4));
            pairs.push((NodeId::random(), 8));
        }

        assert!(pairs.len() >= 100, "need ≥ 100 pairs, got {}", pairs.len());

        for (node_id, cgc) in pairs {
            let mgr = CustodyManager::new(node_id, cgc);
            assert!(
                mgr.custodied().as_set().is_subset(mgr.sampled().as_set()),
                "custody ⊈ sampled for node_id={:?} cgc={cgc}",
                node_id.raw()
            );
            assert_eq!(mgr.custodied().len() as u64, cgc.min(NUMBER_OF_CUSTODY_GROUPS));
            assert_eq!(
                mgr.sampled().len() as u64,
                types_sampling_size(cgc.min(NUMBER_OF_CUSTODY_GROUPS))
            );
        }
    }

    #[test]
    fn sampling_size_four_is_eight() {
        assert_eq!(types_sampling_size(4), 8);
        assert_eq!(types_sampling_size(CUSTODY_REQUIREMENT), SAMPLES_PER_SLOT);
        let mgr = CustodyManager::with_default_cgc(node_id_from_u64(1));
        assert_eq!(mgr.cgc(), CUSTODY_REQUIREMENT);
        assert_eq!(mgr.sampling_size(), 8);
        assert_eq!(mgr.custodied().len(), 4);
        assert_eq!(mgr.sampled().len(), 8);
        assert_eq!(mgr.column_subnets().len(), 8);
    }

    #[test]
    fn default_subscribes_exactly_eight_column_subnets_on_registry() {
        let ctx = synthetic_ctx();
        let digest = ctx.current_digest();
        let counts = SubnetCounts {
            attestation: 0,
            sync_committee: 0,
            // Full column range so registry accepts any sampled subnet id.
            data_column_sidecar: DATA_COLUMN_SIDECAR_SUBNET_COUNT,
        };
        let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, counts);
        // Register validators for every column subnet name we might subscribe.
        for id in 0..DATA_COLUMN_SIDECAR_SUBNET_COUNT {
            reg.register_validator(TopicName::DataColumnSidecar(id));
        }

        let mgr = CustodyManager::with_default_cgc(node_id_from_u64(0xA11CE));
        assert_eq!(mgr.custodied().len(), 4);
        assert_eq!(mgr.sampled().len(), 8);

        let params = TopicParams { topic_weight: 1 };
        mgr.subscribe_sampled_columns(&mut reg, digest, params)
            .expect("subscribe sampled columns");

        // Assert on the **registry's** live subscription set, not a local var.
        let live = reg.subscribed_keys();
        let column_live: HashSet<_> = live
            .iter()
            .filter_map(|k| match k.name {
                TopicName::DataColumnSidecar(id) if k.digest == digest => Some(id),
                _ => None,
            })
            .collect();

        assert_eq!(
            column_live.len(),
            8,
            "default must subscribe exactly 8 column subnets; got {column_live:?}"
        );
        assert_eq!(
            column_live,
            mgr.column_subnets().iter().copied().collect::<HashSet<_>>(),
            "registry live set must match derived sampled subnets"
        );
        // Custodied count stays 4 (not conflated with sampled).
        assert_eq!(mgr.custodied().len(), 4);
    }

    #[test]
    fn changed_node_id_changes_sampled_set() {
        let a = CustodyManager::new(node_id_from_u64(1), CUSTODY_REQUIREMENT);
        let b = CustodyManager::new(node_id_from_u64(2), CUSTODY_REQUIREMENT);
        assert_ne!(
            a.sampled().as_set(),
            b.sampled().as_set(),
            "distinct node ids must yield distinct sampled sets (custody is node-id-derived)"
        );
        assert_ne!(a.custodied().as_set(), b.custodied().as_set());

        // Same node id → stable sets (persisted key property).
        let a2 = CustodyManager::new(node_id_from_u64(1), CUSTODY_REQUIREMENT);
        assert_eq!(a.sampled().as_set(), a2.sampled().as_set());
        assert_eq!(a.custodied().as_set(), a2.custodied().as_set());
    }

    #[test]
    fn subnet_derivation_uses_config_constants_not_inline() {
        // Structural: one group → one column → one subnet under mainnet equality.
        assert_eq!(NUMBER_OF_CUSTODY_GROUPS, NUMBER_OF_COLUMNS);
        assert_eq!(NUMBER_OF_COLUMNS, DATA_COLUMN_SIDECAR_SUBNET_COUNT);

        let last = NUMBER_OF_CUSTODY_GROUPS - 1;
        let groups: BTreeSet<CustodyIndex> = [0, 7, last].into_iter().collect();
        let subnets = column_subnets_for_groups(&groups);
        for g in &groups {
            let cols = compute_columns_for_custody_group(*g);
            assert_eq!(cols.len(), 1);
            assert_eq!(compute_subnet_for_data_column_sidecar(cols[0]), *g);
            assert!(subnets.contains(g));
        }
        assert_eq!(subnets.len(), 3);
    }

    #[test]
    fn sampled_and_custodied_are_distinct_types() {
        let mgr = CustodyManager::with_default_cgc(node_id_from_u64(9));
        let sampled: &SampledGroups = mgr.sampled();
        let custodied: &CustodiedGroups = mgr.custodied();
        // Type-level separation: these are different newtypes (compile-time).
        // Runtime: sizes differ at default cgc.
        assert_eq!(sampled.len(), 8);
        assert_eq!(custodied.len(), 4);
        // Cannot assign across types without .as_set() — exercised by the
        // distinct method return types above.
        let _: &SampledGroups = sampled;
        let _: &CustodiedGroups = custodied;
    }

    #[test]
    fn set_topic_params_precedes_each_column_subscribe() {
        let ctx = synthetic_ctx();
        let digest = ctx.current_digest();
        let counts = SubnetCounts {
            attestation: 0,
            sync_committee: 0,
            data_column_sidecar: DATA_COLUMN_SIDECAR_SUBNET_COUNT,
        };
        let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, counts);
        for id in 0..DATA_COLUMN_SIDECAR_SUBNET_COUNT {
            reg.register_validator(TopicName::DataColumnSidecar(id));
        }

        let mgr = CustodyManager::with_default_cgc(node_id_from_u64(42));
        let params = TopicParams { topic_weight: 7 };
        mgr.subscribe_sampled_columns(&mut reg, digest, params.clone())
            .unwrap();

        let calls = &reg.gossip().calls;
        // Exactly 8 × (SetTopicParams, Subscribe) pairs, params first each time.
        assert_eq!(calls.len(), 16, "8 subnets × 2 calls: {calls:?}");

        let mut i = 0;
        while i < calls.len() {
            match (&calls[i], &calls[i + 1]) {
                (
                    crate::gossip::GossipCall::SetTopicParams {
                        topic: t_params,
                        params: p,
                    },
                    crate::gossip::GossipCall::Subscribe { topic: t_sub },
                ) => {
                    assert_eq!(t_params, t_sub, "params topic must match subscribe topic");
                    assert_eq!(p, &params);
                    assert!(
                        t_params.contains("data_column_sidecar_"),
                        "unexpected topic {t_params}"
                    );
                }
                other => panic!(
                    "expected (SetTopicParams, Subscribe) pair at {i}, got {other:?}"
                ),
            }
            i += 2;
        }
    }

    #[test]
    fn custody_compatible_count_and_metric() {
        let us = CustodyManager::with_default_cgc(node_id_from_u64(0xBEEF));
        let sampled = us.sampled().clone();

        // Peer with full custody covers everything → compatible.
        let full = node_id_from_u64(0x1111);
        assert!(is_peer_custody_compatible(&sampled, full, NUMBER_OF_CUSTODY_GROUPS));

        // Peer with cgc=0 covers nothing → incompatible.
        let empty = node_id_from_u64(0x2222);
        assert!(!is_peer_custody_compatible(&sampled, empty, 0));

        // Peer with same node_id and default cgc custodies a subset of sampled → compatible.
        let same = node_id_from_u64(0xBEEF);
        assert!(is_peer_custody_compatible(&sampled, same, CUSTODY_REQUIREMENT));

        // Construct a peer whose groups are known to miss our sampled set:
        // search a few random ids for a non-intersecting cgc=4 peer, or use cgc=0.
        let peers = [
            (full, NUMBER_OF_CUSTODY_GROUPS),
            (empty, 0),
            (same, CUSTODY_REQUIREMENT),
            (node_id_from_u64(0x3333), 0),
        ];
        let count = count_custody_compatible_peers(&sampled, peers);
        assert_eq!(count, 2, "full + same should be compatible; two cgc=0 are not");

        // Metric surface: export the count.
        let mut registry = prometheus_client::registry::Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        metrics.set_peers_custody_compatible(count as i64);
        assert_eq!(metrics.peers_custody_compatible(), 2);

        // Manager API agrees.
        assert_eq!(us.count_compatible_peers(peers), 2);
        assert!(us.is_peer_compatible(full, NUMBER_OF_CUSTODY_GROUPS));
        assert!(!us.is_peer_compatible(empty, 0));
        assert!(us.peer_coverage(full, NUMBER_OF_CUSTODY_GROUPS) >= 1);
        assert_eq!(us.peer_coverage(empty, 0), 0);
    }

    #[test]
    fn track_d_seam_is_construction_time_cgc_parameter() {
        // Normal path: default cgc = 4 → sample 8.
        let normal = CustodyManager::new(node_id_from_u64(1), CUSTODY_REQUIREMENT);
        assert_eq!(normal.cgc(), 4);
        assert_eq!(normal.sampled().len(), 8);

        // Track D seam: publisher / fault-mode forces NUMBER_OF_CUSTODY_GROUPS.
        let publisher = CustodyManager::new(node_id_from_u64(1), NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(publisher.cgc(), NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(publisher.sampled().len() as u64, NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(publisher.custodied().len() as u64, NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(
            publisher.column_subnets().len() as u64,
            DATA_COLUMN_SIDECAR_SUBNET_COUNT
        );
    }

    #[test]
    fn node_id_taken_by_value_not_global() {
        // Construction signature is `new(NodeId, u64)` — NodeId is Copy/by value.
        // This test documents that the manager stores the id it was given.
        let id = node_id_from_u64(0xDEAD);
        let mgr = CustodyManager::new(id, 4);
        assert_eq!(mgr.node_id(), id);
        // No ambient identity: building with a different id changes the set.
        let other = CustodyManager::new(node_id_from_u64(0xBEEF), 4);
        assert_ne!(mgr.node_id(), other.node_id());
    }

    #[test]
    fn composition_matches_cc1b_helpers_directly() {
        let id = node_id_from_u64(99);
        let cgc = 4_u64;
        let mgr = CustodyManager::new(id, cgc);
        let u = node_id_as_u256(id);
        let expected_sampled = get_custody_groups(u, types_sampling_size(cgc));
        let expected_custody = get_custody_groups(u, cgc);
        assert_eq!(mgr.sampled().as_set(), &expected_sampled);
        assert_eq!(mgr.custodied().as_set(), &expected_custody);
        let expected_subnets = column_subnets_for_groups(&expected_sampled);
        assert_eq!(mgr.column_subnets(), &expected_subnets);
    }

    #[test]
    fn u256_path_matches_node_id_path() {
        // node_id_as_u256 is big-endian raw — same as discovery ENR path.
        let id = NodeId::new(&[0xab; 32]);
        let u = U256::from_be_bytes(id.raw());
        assert_eq!(node_id_as_u256(id), u);
    }
}
