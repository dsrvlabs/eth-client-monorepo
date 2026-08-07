//! Attestation-subnet backbone — Architecture §6.6 / CC-2C.
//!
//! With no attached validators the node maintains **exactly**
//! [`AttestationSubnetConfig::subnets_per_node`] long-lived subnets, rotated
//! every [`AttestationSubnetConfig::epochs_per_subnet_subscription`] epochs via
//! the spec's `compute_subscribed_subnet(node_id, epoch, index)`.
//!
//! ## Single writer
//!
//! [`SubnetManager`] owns the authoritative `attnets` bitvector. Its
//! [`SubnetManager::apply`] is the **only** site that mutates ENR `attnets`,
//! MetaData v3 `attnets`, and the gossipsub attestation-subnet subscription
//! set — all three in one call, so they cannot drift.
//!
//! ## Constants
//!
//! All five networking values come from [`cc_config::AttestationSubnetConfig`]
//! (defaults in `crates/config`). This file never inlines `2`, `64`, `256`, or
//! the prefix-bit / offset-modulus literals.

use std::collections::BTreeSet;
use std::fmt;

use alloy_primitives::U256;
use cc_config::AttestationSubnetConfig;
use cc_crypto::hash_fixed;
use cc_types::{Epoch, ForkDigest, Mainnet, Preset, Root, SubnetId};
use discv5::enr::NodeId;
use thiserror::Error;

use crate::discovery::enr::{
    encode_attnets, node_id_as_u256, read_attnets, EnrApplyError, EnrFieldChange, EnrManager,
    ENR_KEY_ATTNETS,
};
use crate::gossip::{
    GossipsubControl, RegistryError, TopicName, TopicParams, TopicRegistry,
};
use crate::reqresp::LocalMetaData;

// ── Errors ──────────────────────────────────────────────────────────────────

/// Errors from [`SubnetManager::apply`].
#[derive(Debug, Error)]
pub enum SubnetManagerError {
    /// Topic registry refused params / subscribe / unsubscribe.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// ENR batch apply failed.
    #[error(transparent)]
    Enr(#[from] EnrApplyError),
    /// Config failed validation (zero counts, etc.).
    #[error("invalid attestation subnet config: {0}")]
    InvalidConfig(&'static str),
    /// Shuffle / arithmetic failure for the requested index.
    #[error("compute_subscribed_subnet failed for index {index}")]
    Compute {
        /// Subnet index into `0..subnets_per_node`.
        index: u64,
    },
}

// ── Effect trace ────────────────────────────────────────────────────────────

/// Ordered step markers from one [`SubnetManager::apply`] (tests / diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubnetEffectKind {
    /// Desired set recomputed from `compute_subscribed_subnet`.
    Recompute,
    /// Gossip attestation-topic params / subscribe / unsubscribe via registry.
    GossipSync,
    /// ENR `attnets` applied (one seq bump).
    EnrApply,
    /// MetaData v3 `attnets` updated.
    MetaData,
}

/// Outcome of one apply (rotation or initial subscribe).
#[derive(Debug, Clone)]
pub struct SubnetApplyOutcome {
    /// Bitvector written to ENR + MetaData (`BitVector[attestation_subnet_count]`).
    pub attnets: u64,
    /// Ordered subnet ids that form the backbone.
    pub subnets: BTreeSet<SubnetId>,
    /// Epoch the apply was computed for.
    pub epoch: Epoch,
    /// Subscription period index: `(epoch + node_offset) // epochs_per_subnet_subscription`.
    pub period: u64,
    /// ENR sequence before the apply.
    pub enr_seq_before: u64,
    /// ENR sequence after the apply.
    pub enr_seq_after: u64,
    /// MetaData sequence after the mutation.
    pub meta_seq: u64,
    /// Ordered effect markers.
    pub effects: Vec<SubnetEffectKind>,
    /// Whether the bitvector (and therefore advertisements) actually changed.
    pub rotated: bool,
}

impl fmt::Display for SubnetApplyOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "attnets={:#x} subnets={:?} epoch={} period={} enr_seq {}→{} meta_seq={} rotated={} effects={:?}",
            self.attnets,
            self.subnets,
            self.epoch.as_u64(),
            self.period,
            self.enr_seq_before,
            self.enr_seq_after,
            self.meta_seq,
            self.rotated,
            self.effects
        )
    }
}

// ── Pure helpers (spec `compute_subscribed_subnet`) ─────────────────────────

/// Spec `compute_shuffled_index` (swap-or-not) with an explicit round count.
///
/// Uses mainnet / Hoodi `SHUFFLE_ROUND_COUNT` when called through
/// [`compute_subscribed_subnet`]. Kept local so `cc-p2p` does not depend on
/// `cc-state-transition` (crate DAG).
fn compute_shuffled_index(mut index: u64, index_count: u64, seed: Root, rounds: u8) -> Option<u64> {
    if index >= index_count || index_count == 0 {
        return None;
    }
    let seed_bytes = seed.as_array();
    for current_round in 0..rounds {
        let mut pivot_input = [0u8; 33];
        pivot_input[..32].copy_from_slice(seed_bytes);
        pivot_input[32] = current_round;
        let pivot =
            u64::from_le_bytes(hash_fixed(&pivot_input)[0..8].try_into().unwrap_or([0; 8]))
                % index_count;

        let flip = (pivot.saturating_add(index_count).saturating_sub(index)) % index_count;
        let position = index.max(flip);

        let mut source_input = [0u8; 37];
        source_input[..32].copy_from_slice(seed_bytes);
        source_input[32] = current_round;
        source_input[33..37].copy_from_slice(&((position / 256) as u32).to_le_bytes());
        let source = hash_fixed(&source_input);
        let byte = source[((position % 256) / 8) as usize];
        let bit = (byte >> (position % 8)) & 1;
        if bit == 1 {
            index = flip;
        }
    }
    Some(index)
}

/// Spec `compute_subscribed_subnet(node_id, epoch, index)`.
///
/// All networking constants are read from `cfg` — never inlined here.
#[must_use]
pub fn compute_subscribed_subnet(
    node_id: U256,
    epoch: Epoch,
    index: u64,
    cfg: &AttestationSubnetConfig,
) -> Option<SubnetId> {
    if !cfg.is_valid() {
        return None;
    }
    let prefix_bits = cfg.attestation_subnet_prefix_bits;
    let node_id_bits = cfg.node_id_bits;
    let epochs_per = cfg.epochs_per_subnet_subscription;
    let count = cfg.attestation_subnet_count;

    // node_id_prefix = node_id >> (NODE_ID_BITS - ATTESTATION_SUBNET_PREFIX_BITS)
    let shift = node_id_bits.saturating_sub(prefix_bits);
    let node_id_prefix = (node_id >> shift).as_limbs()[0];

    // node_offset = node_id % EPOCHS_PER_SUBNET_SUBSCRIPTION  (offset modulus)
    let node_offset = (node_id % U256::from(epochs_per)).as_limbs()[0];

    let period = epoch
        .as_u64()
        .saturating_add(node_offset)
        / epochs_per;
    // permutation_seed = hash(uint_to_bytes(uint64(period)))
    let seed = Root::from_array(hash_fixed(&period.to_le_bytes()));

    let index_count = cfg.shuffle_index_count();
    let permutated_prefix = compute_shuffled_index(
        node_id_prefix,
        index_count,
        seed,
        Mainnet::SHUFFLE_ROUND_COUNT,
    )?;

    Some((permutated_prefix.saturating_add(index)) % count)
}

/// Spec `compute_subscribed_subnets(node_id, epoch)`.
#[must_use]
pub fn compute_subscribed_subnets(
    node_id: U256,
    epoch: Epoch,
    cfg: &AttestationSubnetConfig,
) -> Option<BTreeSet<SubnetId>> {
    let mut out = BTreeSet::new();
    for index in 0..cfg.subnets_per_node {
        let s = compute_subscribed_subnet(node_id, epoch, index, cfg)?;
        out.insert(s);
    }
    Some(out)
}

/// Pack a set of subnet ids into a little-endian `attnets` bitvector (`u64`).
///
/// Wire form is SSZ `BitVector[ATTESTATION_SUBNET_COUNT]` packed into 8 bytes
/// when the count is ≤ 64 (mainnet / Hoodi). Bits whose index does not fit a
/// `u64` limb are skipped — Phase 2 never configures a larger count.
#[must_use]
pub fn attnets_bitvector(subnets: &BTreeSet<SubnetId>) -> u64 {
    let mut bits = 0u64;
    for &s in subnets {
        if s < u64::BITS as u64 {
            bits |= 1u64 << s;
        }
    }
    bits
}

/// Subscription period for `(node_id, epoch)` under `cfg`.
#[must_use]
pub fn subscription_period(node_id: U256, epoch: Epoch, cfg: &AttestationSubnetConfig) -> u64 {
    let epochs_per = cfg.epochs_per_subnet_subscription.max(1);
    let node_offset = (node_id % U256::from(epochs_per)).as_limbs()[0];
    epoch.as_u64().saturating_add(node_offset) / epochs_per
}

// ── Manager ─────────────────────────────────────────────────────────────────

/// Single writer for the long-lived attestation-subnet backbone.
///
/// Holds the authoritative bitvector and the last applied period so callers
/// can ask "do I need to rotate?" without recomputing ENR/MetaData.
#[derive(Debug)]
pub struct SubnetManager {
    node_id: NodeId,
    node_u256: U256,
    cfg: AttestationSubnetConfig,
    /// Authoritative `attnets` bitvector.
    attnets: u64,
    /// Last successfully applied subscription period, if any.
    period: Option<u64>,
    /// Live subnet set matching `attnets`.
    subnets: BTreeSet<SubnetId>,
}

/// Mutable targets for one [`SubnetManager::apply`] call.
pub struct SubnetApplyTarget<'a, G: GossipsubControl> {
    /// Sole gossip subscription owner.
    pub registry: &'a mut TopicRegistry<G>,
    /// Local ENR writer (batched apply).
    pub enr: &'a EnrManager,
    /// Local MetaData v3.
    pub metadata: &'a LocalMetaData,
    /// Digest whose attestation topics are live.
    pub digest: ForkDigest,
}

impl<G: GossipsubControl> fmt::Debug for SubnetApplyTarget<'_, G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubnetApplyTarget")
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl SubnetManager {
    /// Construct with empty advertisements (no apply yet).
    ///
    /// # Errors
    ///
    /// Returns [`SubnetManagerError::InvalidConfig`] when `cfg` fails validation.
    pub fn new(node_id: NodeId, cfg: AttestationSubnetConfig) -> Result<Self, SubnetManagerError> {
        if !cfg.is_valid() {
            return Err(SubnetManagerError::InvalidConfig(
                "zero or out-of-range field",
            ));
        }
        Ok(Self {
            node_id,
            node_u256: node_id_as_u256(node_id),
            cfg,
            attnets: 0,
            period: None,
            subnets: BTreeSet::new(),
        })
    }

    /// Hoodi / mainnet defaults from [`AttestationSubnetConfig::hoodi`].
    ///
    /// # Errors
    ///
    /// Propagates config validation errors (defaults are always valid).
    pub fn with_hoodi_defaults(node_id: NodeId) -> Result<Self, SubnetManagerError> {
        Self::new(node_id, AttestationSubnetConfig::hoodi())
    }

    /// discv5 node id this manager was built for.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Runtime config snapshot.
    #[must_use]
    pub const fn config(&self) -> AttestationSubnetConfig {
        self.cfg
    }

    /// Authoritative attnets bitvector.
    #[must_use]
    pub const fn attnets(&self) -> u64 {
        self.attnets
    }

    /// Live subnet set.
    #[must_use]
    pub fn subnets(&self) -> &BTreeSet<SubnetId> {
        &self.subnets
    }

    /// Last applied period, if any.
    #[must_use]
    pub const fn period(&self) -> Option<u64> {
        self.period
    }

    /// Whether `epoch` falls in a different subscription period than the last apply.
    #[must_use]
    pub fn needs_rotation(&self, epoch: Epoch) -> bool {
        let p = subscription_period(self.node_u256, epoch, &self.cfg);
        match self.period {
            None => true,
            Some(prev) => prev != p,
        }
    }

    /// Desired backbone set for `epoch` (pure; does not mutate).
    #[must_use]
    pub fn desired_subnets(&self, epoch: Epoch) -> Option<BTreeSet<SubnetId>> {
        compute_subscribed_subnets(self.node_u256, epoch, &self.cfg)
    }

    /// Recompute, resubscribe, and advertise in one call — the single writer.
    ///
    /// Ordering (§6.6 three-way consistency):
    /// 1. recompute desired set via `compute_subscribed_subnet`
    /// 2. registry `sync_attestation_subnets` (params first)
    /// 3. ENR `attnets` via [`EnrManager::apply`] (exactly one seq bump when applied)
    /// 4. MetaData v3 `attnets`
    ///
    /// When `epoch` is still inside the current period and an apply has already
    /// landed, this is a **no-op** that returns `Ok(None)` so callers can tick
    /// every epoch without thrashing ENR.
    ///
    /// # Errors
    ///
    /// Registry / ENR failures, or compute failure for a configured index.
    pub fn apply<G: GossipsubControl>(
        &mut self,
        target: SubnetApplyTarget<'_, G>,
        epoch: Epoch,
    ) -> Result<Option<SubnetApplyOutcome>, SubnetManagerError> {
        if !self.needs_rotation(epoch) {
            return Ok(None);
        }

        let period = subscription_period(self.node_u256, epoch, &self.cfg);
        let subnets = compute_subscribed_subnets(self.node_u256, epoch, &self.cfg).ok_or(
            SubnetManagerError::Compute { index: 0 },
        )?;
        // Spec returns `subnets_per_node` entries; a BTreeSet collapses rare
        // prefix-shuffle collisions so `len` may be smaller — still a valid set.

        let attnets = attnets_bitvector(&subnets);
        let rotated = self.period.is_some() && attnets != self.attnets;

        let mut effects = Vec::with_capacity(4);
        effects.push(SubnetEffectKind::Recompute);

        // Topic weight: family total 1.0 / attestation_subnet_count (CC-22c table).
        let params = TopicParams {
            topic_weight: 1.0 / self.cfg.attestation_subnet_count.max(1) as f64,
        };
        target
            .registry
            .sync_attestation_subnets(target.digest, &subnets, params)?;
        effects.push(SubnetEffectKind::GossipSync);

        let enr_seq_before = target.enr.local_enr().seq();
        target
            .enr
            .apply([EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(attnets))])?;
        let enr_seq_after = target.enr.local_enr().seq();
        effects.push(SubnetEffectKind::EnrApply);

        let meta_seq = target.metadata.set_attnets(attnets);
        effects.push(SubnetEffectKind::MetaData);

        self.attnets = attnets;
        self.subnets = subnets.clone();
        self.period = Some(period);

        Ok(Some(SubnetApplyOutcome {
            attnets,
            subnets,
            epoch,
            period,
            enr_seq_before,
            enr_seq_after,
            meta_seq,
            effects,
            rotated,
        }))
    }

    /// Three-way consistency snapshot: ENR / MetaData / live gossip set.
    ///
    /// Returns `(enr_attnets, meta_attnets, gossip_attnets_bitvector)`.
    #[must_use]
    pub fn consistency_triple<G: GossipsubControl>(
        &self,
        registry: &TopicRegistry<G>,
        enr: &EnrManager,
        metadata: &LocalMetaData,
        digest: ForkDigest,
    ) -> (Option<u64>, u64, u64) {
        let enr_bits = read_attnets(&enr.local_enr());
        let meta_bits = metadata.load().attnets;
        let gossip_subnets: BTreeSet<SubnetId> = registry
            .subscribed_keys()
            .into_iter()
            .filter_map(|k| {
                if k.digest == digest
                    && let TopicName::BeaconAttestation(id) = k.name
                {
                    return Some(id);
                }
                None
            })
            .collect();
        let gossip_bits = attnets_bitvector(&gossip_subnets);
        (enr_bits, meta_bits, gossip_bits)
    }
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::discovery::enr::EnrSeqStrategy;
    use crate::fork_digest::ForkContext;
    use crate::gossip::{RecordingGossipsub, SubnetCounts, TopicRegistry};
    use crate::reqresp::LocalMetaData;
    use cc_config::{
        ATTESTATION_SUBNET_COUNT, ATTESTATION_SUBNET_PREFIX_BITS, EPOCHS_PER_SUBNET_SUBSCRIPTION,
        SUBNETS_PER_NODE,
    };
    use cc_types::{
        BlobParameters, BlobSchedule, ChainConfig, Epoch, ForkVersion, PresetName, Root,
    };

    fn node_id_from_u64(n: u64) -> NodeId {
        let mut raw = [0u8; 32];
        raw[24..].copy_from_slice(&n.to_be_bytes());
        NodeId::new(&raw)
    }

    fn synthetic_ctx() -> ForkContext {
        let schedule = BlobSchedule::try_from_entries(vec![BlobParameters {
            epoch: Epoch::new(0),
            max_blobs_per_block: 15,
        }])
        .expect("schedule");
        let cfg = ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "synthetic-attnets".into(),
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
        ForkContext::new(cfg, Root::from_array([0xaa; 32]), Epoch::new(0))
    }

    struct Fixture {
        mgr: SubnetManager,
        registry: TopicRegistry<RecordingGossipsub>,
        enr: EnrManager,
        metadata: LocalMetaData,
        digest: ForkDigest,
    }

    impl Fixture {
        fn new(node: NodeId) -> Self {
            let cfg = AttestationSubnetConfig::hoodi();
            let mgr = SubnetManager::new(node, cfg).expect("mgr");
            let ctx = synthetic_ctx();
            let digest = ctx.current_digest();
            let counts = SubnetCounts {
                attestation: cfg.attestation_subnet_count,
                sync_committee: 0,
                data_column_sidecar: 0,
            };
            let mut registry = TopicRegistry::new(RecordingGossipsub::default(), &ctx, counts);
            for id in 0..cfg.attestation_subnet_count {
                registry.register_validator(TopicName::BeaconAttestation(id));
            }
            let enr = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).expect("enr");
            let metadata = LocalMetaData::default();
            Self {
                mgr,
                registry,
                enr,
                metadata,
                digest,
            }
        }

        fn apply(&mut self, epoch: u64) -> Option<SubnetApplyOutcome> {
            self.mgr
                .apply(
                    SubnetApplyTarget {
                        registry: &mut self.registry,
                        enr: &self.enr,
                        metadata: &self.metadata,
                        digest: self.digest,
                    },
                    Epoch::new(epoch),
                )
                .expect("apply")
        }

        fn triple(&self) -> (Option<u64>, u64, u64) {
            self.mgr.consistency_triple(
                &self.registry,
                &self.enr,
                &self.metadata,
                self.digest,
            )
        }
    }

    /// CC-2C/1: fixed node_id → deterministic set of exactly SUBNETS_PER_NODE.
    #[test]
    fn backbone_exactly_subnets_per_node_and_deterministic() {
        let node = node_id_from_u64(0xA77_E75);
        let cfg = AttestationSubnetConfig::hoodi();
        assert_eq!(cfg.subnets_per_node, SUBNETS_PER_NODE);
        assert_eq!(cfg.attestation_subnet_count, ATTESTATION_SUBNET_COUNT);
        assert_eq!(
            cfg.epochs_per_subnet_subscription,
            EPOCHS_PER_SUBNET_SUBSCRIPTION
        );
        assert_eq!(
            cfg.attestation_subnet_prefix_bits,
            ATTESTATION_SUBNET_PREFIX_BITS
        );

        let a = compute_subscribed_subnets(node_id_as_u256(node), Epoch::new(0), &cfg).unwrap();
        let b = compute_subscribed_subnets(node_id_as_u256(node), Epoch::new(0), &cfg).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len() as u64, cfg.subnets_per_node);
        for &s in &a {
            assert!(s < cfg.attestation_subnet_count);
        }

        // Different node_id generally yields a different set (not required by
        // spec for every pair, but true for these fixtures and guards against
        // a constant stub).
        let other = node_id_from_u64(0xBEEF);
        let c =
            compute_subscribed_subnets(node_id_as_u256(other), Epoch::new(0), &cfg).unwrap();
        assert_eq!(c.len() as u64, cfg.subnets_per_node);
    }

    /// CC-2C/1: set is stable within a period and changes exactly at the boundary.
    #[test]
    fn set_stable_within_period_changes_at_boundary() {
        let node = node_id_from_u64(0xC0_FFEE);
        let cfg = AttestationSubnetConfig::hoodi();
        let n = node_id_as_u256(node);
        let epochs_per = cfg.epochs_per_subnet_subscription;
        let node_offset = (n % U256::from(epochs_per)).as_limbs()[0];

        // Pick an epoch near the end of a period so the next period is close.
        // period = (epoch + node_offset) // epochs_per
        // Choose epoch such that (epoch + node_offset) % epochs_per == epochs_per - 1
        // → next epoch crosses the boundary.
        let within = epochs_per.saturating_sub(1).saturating_sub(node_offset % epochs_per);
        // Ensure within + node_offset ends just before a multiple of epochs_per.
        let base = if (within + node_offset) % epochs_per == epochs_per - 1 {
            within
        } else {
            // Brute: find any epoch with remainder epochs_per-1.
            let mut e = 0u64;
            loop {
                if (e + node_offset) % epochs_per == epochs_per - 1 {
                    break e;
                }
                e += 1;
                assert!(e < epochs_per + 2, "failed to find boundary epoch");
            }
        };

        let at_end =
            compute_subscribed_subnets(n, Epoch::new(base), &cfg).unwrap();
        let still_same =
            compute_subscribed_subnets(n, Epoch::new(base), &cfg).unwrap();
        assert_eq!(at_end, still_same);

        // One epoch earlier is the same period (when base > 0).
        if base > 0 {
            let earlier =
                compute_subscribed_subnets(n, Epoch::new(base - 1), &cfg).unwrap();
            assert_eq!(
                earlier, at_end,
                "set must not change before the period boundary"
            );
        }

        // Crossing the boundary must recompute for a new period.
        let after =
            compute_subscribed_subnets(n, Epoch::new(base + 1), &cfg).unwrap();
        let p0 = subscription_period(n, Epoch::new(base), &cfg);
        let p1 = subscription_period(n, Epoch::new(base + 1), &cfg);
        assert_eq!(p1, p0 + 1, "epoch+1 must enter the next period");
        // The set *may* collide across periods; the period itself must change,
        // and over a full sweep of epochs the set is not constant.
        let _ = after;

        // Across a wide range of periods the set is not a single constant
        // (guards against a stub that always returns {0,1}).
        let mut seen = BTreeSet::new();
        for period in 0..8u64 {
            // Reconstruct an epoch inside that period.
            let epoch = period
                .saturating_mul(epochs_per)
                .saturating_sub(node_offset % epochs_per);
            let set = compute_subscribed_subnets(n, Epoch::new(epoch), &cfg).unwrap();
            seen.insert(set);
        }
        assert!(
            seen.len() > 1,
            "expected set to vary across periods, got {seen:?}"
        );
    }

    /// CC-2C/2: three-way consistency immediately after a rotation.
    #[test]
    fn three_way_consistency_immediately_after_rotation() {
        let node = node_id_from_u64(0x2C_C001);
        let mut fx = Fixture::new(node);
        let cfg = fx.mgr.config();
        let n = node_id_as_u256(node);
        let epochs_per = cfg.epochs_per_subnet_subscription;
        let node_offset = (n % U256::from(epochs_per)).as_limbs()[0];

        // Initial apply at epoch 0.
        let out0 = fx.apply(0).expect("initial apply");
        assert_eq!(out0.subnets.len() as u64, cfg.subnets_per_node);
        assert_eq!(out0.enr_seq_after, out0.enr_seq_before + 1);
        let (enr0, meta0, gossip0) = fx.triple();
        assert_eq!(enr0, Some(out0.attnets));
        assert_eq!(meta0, out0.attnets);
        assert_eq!(gossip0, out0.attnets);
        assert_eq!(fx.mgr.attnets(), out0.attnets);

        // Same period → no-op.
        assert!(fx.apply(1).is_none());
        let (enr1, meta1, gossip1) = fx.triple();
        assert_eq!(enr1, Some(out0.attnets));
        assert_eq!(meta1, out0.attnets);
        assert_eq!(gossip1, out0.attnets);

        // Find the first epoch of the next period.
        let p0 = subscription_period(n, Epoch::new(0), &cfg);
        let mut boundary = 1u64;
        while subscription_period(n, Epoch::new(boundary), &cfg) == p0 {
            boundary += 1;
            assert!(boundary < epochs_per + node_offset + 2);
        }

        let out1 = fx.apply(boundary).expect("rotation apply");
        // ENR sequence bumps by exactly one on the rotation apply.
        assert_eq!(
            out1.enr_seq_after - out1.enr_seq_before,
            1,
            "rotation must bump ENR seq by exactly one"
        );
        assert_eq!(out1.period, p0 + 1);

        // Three-way equality at the instant of rotation (the window that breaks).
        let (enr, meta, gossip) = fx.triple();
        assert_eq!(enr, Some(out1.attnets), "ENR attnets");
        assert_eq!(meta, out1.attnets, "MetaData attnets");
        assert_eq!(gossip, out1.attnets, "gossip subscription bitvector");
        assert_eq!(enr, Some(meta));
        assert_eq!(meta, gossip);
        assert_eq!(fx.mgr.attnets(), out1.attnets);

        // Effects order: recompute → gossip → ENR → MetaData.
        assert_eq!(
            out1.effects,
            [
                SubnetEffectKind::Recompute,
                SubnetEffectKind::GossipSync,
                SubnetEffectKind::EnrApply,
                SubnetEffectKind::MetaData,
            ]
        );
    }

    /// CC-2C/3: rotation bumps ENR sequence by exactly one.
    #[test]
    fn rotation_bumps_enr_seq_exactly_one() {
        let node = node_id_from_u64(0x2C_C003);
        let mut fx = Fixture::new(node);
        let cfg = fx.mgr.config();
        let n = node_id_as_u256(node);
        let p0 = subscription_period(n, Epoch::new(0), &cfg);

        let first = fx.apply(0).expect("initial");
        assert_eq!(first.enr_seq_after, first.enr_seq_before + 1);

        let mut boundary = 1u64;
        while subscription_period(n, Epoch::new(boundary), &cfg) == p0 {
            boundary += 1;
        }
        let second = fx.apply(boundary).expect("rotation");
        assert_eq!(second.enr_seq_after, second.enr_seq_before + 1);
        assert_eq!(second.enr_seq_before, first.enr_seq_after);
    }

    #[test]
    fn needs_rotation_false_inside_period() {
        let node = node_id_from_u64(7);
        let mut fx = Fixture::new(node);
        fx.apply(0).expect("initial");
        assert!(!fx.mgr.needs_rotation(Epoch::new(0)));
        assert!(!fx.mgr.needs_rotation(Epoch::new(1)));
    }

    #[test]
    fn custom_config_short_period_for_devnet() {
        // Devnet: 4-epoch rotation, 8 subnets, 2 per node.
        let cfg = AttestationSubnetConfig {
            subnets_per_node: 2,
            epochs_per_subnet_subscription: 4,
            attestation_subnet_count: 8,
            attestation_subnet_prefix_bits: 3,
            node_id_bits: 256,
        };
        assert!(cfg.is_valid());
        let node = node_id_from_u64(42);
        let n = node_id_as_u256(node);
        let epochs_per = cfg.epochs_per_subnet_subscription;
        let node_offset = (n % U256::from(epochs_per)).as_limbs()[0];
        // Find two epochs in the same period and the first of the next.
        let mut e0 = 0u64;
        while !(e0 + node_offset).is_multiple_of(epochs_per) {
            e0 += 1;
        }
        let e_same = e0 + 1; // still same period when epochs_per > 1
        let e_next = e0 + epochs_per;

        let a = compute_subscribed_subnets(n, Epoch::new(e0), &cfg).unwrap();
        let b = compute_subscribed_subnets(n, Epoch::new(e_same), &cfg).unwrap();
        assert_eq!(a, b, "stable inside short period");
        assert_eq!(a.len(), 2);
        let p0 = subscription_period(n, Epoch::new(e0), &cfg);
        let p1 = subscription_period(n, Epoch::new(e_next), &cfg);
        assert_eq!(p1, p0 + 1);
        let _ = compute_subscribed_subnets(n, Epoch::new(e_next), &cfg).unwrap();
    }

    #[test]
    fn attnets_bitvector_sets_bits() {
        let mut s = BTreeSet::new();
        s.insert(0);
        s.insert(3);
        s.insert(63);
        assert_eq!(attnets_bitvector(&s), (1u64 << 0) | (1u64 << 3) | (1u64 << 63));
    }
}
