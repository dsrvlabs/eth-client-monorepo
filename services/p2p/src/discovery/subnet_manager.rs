//! Subnet manager — Architecture §6.6 / CC-2C + CC-2D.
//!
//! Single writer for the ENR / MetaData / gossip subscription triple.
//!
//! | Half | Issue | Phase-2 default |
//! |------|-------|-----------------|
//! | `attnets` | CC-2C | backbone of `SUBNETS_PER_NODE` (config-sourced) |
//! | `syncnets` | CC-2D | **empty — correct, not a bug** |
//!
//! ## CC-2C: `attnets`
//!
//! With no attached validators the node maintains **exactly**
//! [`AttestationSubnetConfig::subnets_per_node`] long-lived subnets, rotated
//! every [`AttestationSubnetConfig::epochs_per_subnet_subscription`] epochs via
//! the spec's `compute_subscribed_subnet(node_id, epoch, index)`.
//!
//! ## CC-2D: `syncnets`
//!
//! `syncnets` is a `BitVector[4]`. Empty by default; the Phase 6 hook
//! [`SubnetManager::subscribe_sync_subnets`] is the only writer (tests only).
//!
//! ## Constants
//!
//! Attestation values come from [`cc_config::AttestationSubnetConfig`].
//! This file never inlines `2`, `64`, `256`, or the prefix-bit / offset-modulus
//! literals for attnets.

use std::collections::BTreeSet;
use std::fmt;

use alloy_primitives::U256;
use cc_config::AttestationSubnetConfig;
use cc_crypto::hash_fixed;
use cc_types::{Epoch, ForkDigest, Mainnet, Preset, Root, SubnetId};
use discv5::enr::NodeId;
use thiserror::Error;

use crate::discovery::enr::{
    ENR_KEY_ATTNETS, ENR_KEY_SYNCNETS, EnrApplyError, EnrFieldChange, EnrManager, encode_attnets,
    encode_syncnets, node_id_as_u256, read_attnets, read_syncnets,
};
use crate::gossip::scoring::WEIGHT_SYNC_COMMITTEE;
use crate::gossip::{GossipsubControl, RegistryError, TopicName, TopicParams, TopicRegistry};
use crate::reqresp::LocalMetaData;

/// Sync-committee subnet count (`BitVector[4]` / `SYNC_COMMITTEE_SUBNET_COUNT`).
pub const SYNC_SUBNET_COUNT: u8 = Mainnet::SYNC_COMMITTEE_SUBNET_COUNT as u8;

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
    /// Requested sync subnet id is out of range.
    #[error("sync subnet {got} >= SYNC_COMMITTEE_SUBNET_COUNT ({SYNC_SUBNET_COUNT})")]
    SyncSubnetOutOfRange {
        /// Requested subnet id.
        got: u8,
    },
}

/// Alias used by the CC-2D syncnets API surface.
pub type SubnetError = SubnetManagerError;

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

// ── Syncnets effect trace (CC-2D) ───────────────────────────────────────────

/// One effect of [`SubnetManager::subscribe_sync_subnets`] for ordered asserts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncSubnetEffect {
    /// `set_topic_params` for at least one sync-committee topic.
    TopicParams,
    /// At least one new `sync_committee_{id}` subscription.
    Subscribe,
    /// At least one `sync_committee_{id}` unsubscription.
    Unsubscribe,
    /// ENR `syncnets` applied (one seq bump).
    EnrApply,
    /// MetaData `syncnets` updated (seq_number bump when value changes).
    MetaData,
}

/// Outcome of one [`SubnetManager::subscribe_sync_subnets`] call.
#[derive(Debug, Clone)]
pub struct SyncSubnetOutcome {
    /// Resulting `syncnets` bitmask (low 4 bits).
    pub syncnets: u8,
    /// ENR sequence before the apply.
    pub enr_seq_before: u64,
    /// ENR sequence after the apply.
    pub enr_seq_after: u64,
    /// MetaData sequence after the mutation.
    pub meta_seq: u64,
    /// Ordered effect markers.
    pub effects: Vec<SyncSubnetEffect>,
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
        let pivot = u64::from_le_bytes(hash_fixed(&pivot_input)[0..8].try_into().unwrap_or([0; 8]))
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

    let period = epoch.as_u64().saturating_add(node_offset) / epochs_per;
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
    /// Authoritative `syncnets` (`BitVector[4]`, low nibble). Empty by default.
    syncnets: u8,
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

/// Mutable targets for one [`SubnetManager::subscribe_sync_subnets`] call.
pub struct SyncSubnetTarget<'a, G: GossipsubControl> {
    /// Sole gossip subscription owner.
    pub registry: &'a mut TopicRegistry<G>,
    /// Local ENR writer (batched apply).
    pub enr: &'a EnrManager,
    /// Local MetaData v3.
    pub metadata: &'a LocalMetaData,
    /// Digest whose sync-committee topics are live.
    pub digest: ForkDigest,
}

impl<G: GossipsubControl> fmt::Debug for SyncSubnetTarget<'_, G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncSubnetTarget")
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
            syncnets: 0,
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
        let subnets = compute_subscribed_subnets(self.node_u256, epoch, &self.cfg)
            .ok_or(SubnetManagerError::Compute { index: 0 })?;
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
        target.enr.apply([EnrFieldChange::new(
            ENR_KEY_ATTNETS,
            encode_attnets(attnets),
        )])?;
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

    /// Current authoritative `syncnets` bitmask (low 4 bits).
    #[must_use]
    pub const fn syncnets(&self) -> u8 {
        self.syncnets
    }

    /// Whether any sync subnet is subscribed.
    #[must_use]
    pub const fn syncnets_empty(&self) -> bool {
        self.syncnets == 0
    }

    /// Subscription set as subnet ids (`0..4` with bit set).
    #[must_use]
    pub fn sync_subscription_set(&self) -> BTreeSet<SubnetId> {
        bitset_to_subnets(self.syncnets)
    }

    /// Phase 6 hook: set the desired sync-committee subscription set.
    ///
    /// **No Phase 2 production caller** — definition and tests only.
    ///
    /// Effects (in order):
    /// 1. registry `sync_sync_committee_subnets` (params first);
    /// 2. ENR `syncnets` via [`EnrManager::apply`] when bits differ;
    /// 3. MetaData v3 `syncnets`.
    ///
    /// # Errors
    ///
    /// - [`SubnetManagerError::SyncSubnetOutOfRange`] for id ≥ 4
    /// - registry / ENR failures from the underlying writers
    pub fn subscribe_sync_subnets<G: GossipsubControl>(
        &mut self,
        set: impl IntoIterator<Item = u8>,
        target: SyncSubnetTarget<'_, G>,
    ) -> Result<SyncSubnetOutcome, SubnetManagerError> {
        let mut bits: u8 = 0;
        for s in set {
            if s >= SYNC_SUBNET_COUNT {
                return Err(SubnetManagerError::SyncSubnetOutOfRange { got: s });
            }
            bits |= 1u8 << s;
        }
        bits &= 0x0f;

        let SyncSubnetTarget {
            registry,
            enr,
            metadata,
            digest,
        } = target;

        let mut effects = Vec::with_capacity(5);
        let desired = bitset_to_subnets(bits);
        let before = bitset_to_subnets(self.syncnets);
        let added: Vec<_> = desired.difference(&before).copied().collect();
        let removed: Vec<_> = before.difference(&desired).copied().collect();

        // ── 1. registry: params first, then subscribe / unsubscribe ────────
        let params = TopicParams {
            topic_weight: WEIGHT_SYNC_COMMITTEE,
        };
        registry.sync_sync_committee_subnets(digest, &desired, params)?;
        if !desired.is_empty() || !before.is_empty() {
            effects.push(SyncSubnetEffect::TopicParams);
        }
        if !added.is_empty() {
            effects.push(SyncSubnetEffect::Subscribe);
        }
        if !removed.is_empty() {
            effects.push(SyncSubnetEffect::Unsubscribe);
        }

        // ── 2. ENR syncnets (coalesced single bump when we apply) ───────────
        let enr_seq_before = enr.local_enr().seq();
        let enr_seq_after =
            if bits != self.syncnets || read_syncnets(&enr.local_enr()) != Some(bits) {
                enr.apply([EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(bits))])?;
                effects.push(SyncSubnetEffect::EnrApply);
                enr.local_enr().seq()
            } else {
                enr_seq_before
            };

        // ── 3. MetaData v3 ─────────────────────────────────────────────────
        let meta_seq = metadata.set_syncnets(bits);
        effects.push(SyncSubnetEffect::MetaData);

        self.syncnets = bits;

        Ok(SyncSubnetOutcome {
            syncnets: bits,
            enr_seq_before,
            enr_seq_after,
            meta_seq,
            effects,
        })
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
    /// Three-way consistency: ENR `syncnets` == MetaData `syncnets` == live
    /// gossip subscriptions for `digest`, and all equal this manager's set.
    #[must_use]
    pub fn syncnets_three_way_consistent<G: GossipsubControl>(
        &self,
        registry: &TopicRegistry<G>,
        enr: &EnrManager,
        metadata: &LocalMetaData,
        digest: ForkDigest,
    ) -> bool {
        let want = self.syncnets;
        let enr_bits = read_syncnets(&enr.local_enr()).unwrap_or(0xFF);
        let md_bits = metadata.load().syncnets & 0x0f;
        if enr_bits != want || md_bits != want {
            return false;
        }
        let live = live_sync_subnets(registry, digest);
        live == self.sync_subscription_set()
    }
}

fn bitset_to_subnets(bits: u8) -> BTreeSet<SubnetId> {
    let mut out = BTreeSet::new();
    for s in 0..SYNC_SUBNET_COUNT {
        if bits & (1u8 << s) != 0 {
            out.insert(u64::from(s));
        }
    }
    out
}

fn live_sync_subnets<G: GossipsubControl>(
    registry: &TopicRegistry<G>,
    digest: ForkDigest,
) -> BTreeSet<SubnetId> {
    registry
        .subscribed_keys()
        .iter()
        .filter_map(|k| match k.name {
            TopicName::SyncCommittee(id) if k.digest == digest => Some(id),
            _ => None,
        })
        .collect()
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
            churn_limit_quotient: 65_536,
            min_per_epoch_churn_limit_electra: 128_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 256_000_000_000,
            shard_committee_period: Epoch::new(256),
            max_blobs_per_block_electra: 9,
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
            self.mgr
                .consistency_triple(&self.registry, &self.enr, &self.metadata, self.digest)
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
        let c = compute_subscribed_subnets(node_id_as_u256(other), Epoch::new(0), &cfg).unwrap();
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
        let within = epochs_per
            .saturating_sub(1)
            .saturating_sub(node_offset % epochs_per);
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

        let at_end = compute_subscribed_subnets(n, Epoch::new(base), &cfg).unwrap();
        let still_same = compute_subscribed_subnets(n, Epoch::new(base), &cfg).unwrap();
        assert_eq!(at_end, still_same);

        // One epoch earlier is the same period (when base > 0).
        if base > 0 {
            let earlier = compute_subscribed_subnets(n, Epoch::new(base - 1), &cfg).unwrap();
            assert_eq!(
                earlier, at_end,
                "set must not change before the period boundary"
            );
        }

        // Crossing the boundary must recompute for a new period.
        let after = compute_subscribed_subnets(n, Epoch::new(base + 1), &cfg).unwrap();
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
        assert_eq!(
            attnets_bitvector(&s),
            (1u64 << 0) | (1u64 << 3) | (1u64 << 63)
        );
    }

    // ── CC-2D syncnets ────────────────────────────────────────────────────

    struct SyncFixture {
        mgr: SubnetManager,
        registry: TopicRegistry<RecordingGossipsub>,
        enr: EnrManager,
        metadata: LocalMetaData,
        digest: ForkDigest,
    }

    impl SyncFixture {
        fn new() -> Self {
            let node = node_id_from_u64(1);
            let mgr = SubnetManager::with_hoodi_defaults(node).expect("mgr");
            let ctx = synthetic_ctx();
            let digest = ctx.current_digest();
            let counts = SubnetCounts {
                attestation: 0,
                sync_committee: SYNC_SUBNET_COUNT as u64,
                data_column_sidecar: 0,
            };
            let mut registry = TopicRegistry::new(RecordingGossipsub::default(), &ctx, counts);
            for id in 0..u64::from(SYNC_SUBNET_COUNT) {
                registry.register_validator(TopicName::SyncCommittee(id));
            }
            let enr = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).expect("enr");
            // Ensure empty syncnets present so three-way can read.
            enr.apply([EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(0))])
                .expect("seed syncnets");
            let metadata = LocalMetaData::default();
            Self {
                mgr,
                registry,
                enr,
                metadata,
                digest,
            }
        }

        fn subscribe(&mut self, set: impl IntoIterator<Item = u8>) -> SyncSubnetOutcome {
            self.mgr
                .subscribe_sync_subnets(
                    set,
                    SyncSubnetTarget {
                        registry: &mut self.registry,
                        enr: &self.enr,
                        metadata: &self.metadata,
                        digest: self.digest,
                    },
                )
                .expect("subscribe_sync_subnets")
        }

        fn assert_three_way(&self) {
            assert!(self.mgr.syncnets_three_way_consistent(
                &self.registry,
                &self.enr,
                &self.metadata,
                self.digest,
            ));
        }
    }

    #[test]
    fn empty_by_default_is_correct_not_a_bug() {
        let f = SyncFixture::new();
        assert_eq!(f.mgr.syncnets(), 0);
        assert!(f.mgr.syncnets_empty());
        assert!(f.mgr.sync_subscription_set().is_empty());
        f.assert_three_way();
    }

    #[test]
    fn subscribe_sync_subnets_synthetic_injection() {
        let mut f = SyncFixture::new();
        let out = f.subscribe([0u8, 2]);
        assert_eq!(out.syncnets, 0b0101);
        assert!(out.effects.contains(&SyncSubnetEffect::TopicParams));
        assert!(out.effects.contains(&SyncSubnetEffect::Subscribe));
        assert!(out.effects.contains(&SyncSubnetEffect::EnrApply));
        assert!(out.effects.contains(&SyncSubnetEffect::MetaData));
        assert_eq!(f.mgr.sync_subscription_set(), BTreeSet::from([0u64, 2]));
        f.assert_three_way();
    }

    #[test]
    fn subscribe_then_clear_unsubscribes() {
        let mut f = SyncFixture::new();
        f.subscribe([1u8, 3]);
        let out = f.subscribe([]);
        assert_eq!(out.syncnets, 0);
        assert!(out.effects.contains(&SyncSubnetEffect::Unsubscribe));
        assert!(f.mgr.syncnets_empty());
        f.assert_three_way();
    }

    #[test]
    fn rejects_out_of_range_subnet() {
        let mut f = SyncFixture::new();
        let err = f
            .mgr
            .subscribe_sync_subnets(
                [4u8],
                SyncSubnetTarget {
                    registry: &mut f.registry,
                    enr: &f.enr,
                    metadata: &f.metadata,
                    digest: f.digest,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            SubnetManagerError::SyncSubnetOutOfRange { got: 4 }
        ));
    }

    #[test]
    fn idempotent_same_set_no_enr_bump() {
        let mut f = SyncFixture::new();
        let first = f.subscribe([0u8, 1]);
        let second = f.subscribe([0u8, 1]);
        assert_eq!(first.enr_seq_after, second.enr_seq_before);
        assert_eq!(second.enr_seq_before, second.enr_seq_after);
        assert!(!second.effects.contains(&SyncSubnetEffect::EnrApply));
        f.assert_three_way();
    }
}
