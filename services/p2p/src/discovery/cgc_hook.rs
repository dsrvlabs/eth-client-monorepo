//! Runtime `cgc` hook — Architecture §6.4 / CC-21d.
//!
//! `set_custody_group_count(n)` performs **all five** of §6.4's effects in order:
//!
//! 1. recompute `sampling_size` + custodied/sampled sets via [`CustodyManager`];
//! 2. recompute column topic weight `0.5 / sampling_size` and
//!    `set_topic_params` for every live/to-be-live column topic;
//! 3. subscribe newly sampled subnets (params first) / unsubscribe dropped;
//! 4. update ENR `cgc` through [`EnrManager::apply`] (exactly one seq bump);
//! 5. update `MetaData v3.custody_group_count` (seq_number bump).
//!
//! **No Phase 2 caller** beyond the Rust method and the
//! `SetCustodyGroupCount` RPC — Phase 6's attached-validator tracker is the
//! production caller. Ceiling is 128 (supernode obligation, spec delta 15).
//!
//! Steps 2–3 go through the topic registry (sole
//! `subscribe`/`unsubscribe`/`set_topic_params` owner) — same mechanism the
//! BPO state machine uses (§5.1).

use std::fmt;

use cc_types::{ForkDigest, NUMBER_OF_CUSTODY_GROUPS};
use thiserror::Error;

use crate::das::CustodyManager;
use crate::discovery::enr::{
    encode_cgc, EnrApplyError, EnrFieldChange, EnrManager, ENR_KEY_CGC,
};
use crate::gossip::{
    column_topic_weight, GossipsubControl, RegistryError, TopicParams, TopicRegistry,
};
use crate::reqresp::LocalMetaData;

/// Column-family total weight (ADR P2-07 / scoring `WEIGHT_COLUMN_FAMILY`).
const COLUMN_FAMILY_TOTAL: f64 = 0.5;

// ── Errors ──────────────────────────────────────────────────────────────────

/// Errors from [`set_custody_group_count`].
#[derive(Debug, Error)]
pub enum CgcHookError {
    /// Topic registry refused params / subscribe / unsubscribe.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// ENR batch apply failed.
    #[error(transparent)]
    Enr(#[from] EnrApplyError),
    /// `cgc` exceeds [`NUMBER_OF_CUSTODY_GROUPS`].
    #[error("cgc {got} exceeds NUMBER_OF_CUSTODY_GROUPS ({NUMBER_OF_CUSTODY_GROUPS})")]
    CgcOutOfRange {
        /// Requested value.
        got: u64,
    },
    /// gRPC / service surface has no live invoker attached (Phase 2 default).
    #[error("cgc hook not attached (Phase 2 — no production caller)")]
    NotAttached,
}

// ── Effect trace (integration-test ordering) ────────────────────────────────

/// One §6.4 step marker for sequence-recording assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CgcEffectKind {
    /// Step 1: custody / sampling sets recomputed.
    Recompute,
    /// Step 2: column `set_topic_params` (at least one call recorded).
    ColumnParams,
    /// Step 3a: a new column topic was subscribed (params-first).
    ColumnSubscribe,
    /// Step 3b: a column topic was unsubscribed.
    ColumnUnsubscribe,
    /// Step 4: ENR `cgc` applied.
    EnrApply,
    /// Step 5: MetaData `custody_group_count` updated.
    MetaData,
}

/// Outcome of one hook invocation (for tests / diagnostics).
#[derive(Debug, Clone)]
pub struct CgcHookOutcome {
    /// Effective cgc after capping.
    pub cgc: u64,
    /// `max(SAMPLES_PER_SLOT, cgc)`.
    pub sampling_size: u64,
    /// Per-column topic weight `0.5 / sampling_size`.
    pub column_weight: f64,
    /// ENR sequence before the apply.
    pub enr_seq_before: u64,
    /// ENR sequence after the apply.
    pub enr_seq_after: u64,
    /// MetaData sequence after the mutation.
    pub meta_seq: u64,
    /// Ordered effect markers (subset of the five steps; subscribe/unsubscribe
    /// appear only when the sampled set actually grows/shrinks).
    pub effects: Vec<CgcEffectKind>,
}

impl CgcHookOutcome {
    /// Whether the effect list respects §6.4 order (no advertise-before-subscribe).
    #[must_use]
    pub fn order_is_valid(&self) -> bool {
        let mut phase = 0u8;
        for step in &self.effects {
            let p = match step {
                CgcEffectKind::Recompute => 1,
                CgcEffectKind::ColumnParams => 2,
                CgcEffectKind::ColumnSubscribe | CgcEffectKind::ColumnUnsubscribe => 3,
                CgcEffectKind::EnrApply => 4,
                CgcEffectKind::MetaData => 5,
            };
            if p < phase {
                return false;
            }
            phase = p;
        }
        // ENR and MetaData must not precede any subscribe-side work when both
        // appear — catch "advertise first" reordering.
        let enr_i = self
            .effects
            .iter()
            .position(|e| *e == CgcEffectKind::EnrApply);
        let sub_i = self
            .effects
            .iter()
            .position(|e| *e == CgcEffectKind::ColumnSubscribe);
        if let (Some(e), Some(s)) = (enr_i, sub_i)
            && e < s
        {
            return false;
        }
        true
    }
}

impl fmt::Display for CgcHookOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cgc={} sampling_size={} col_w={} enr_seq {}→{} meta_seq={} effects={:?}",
            self.cgc,
            self.sampling_size,
            self.column_weight,
            self.enr_seq_before,
            self.enr_seq_after,
            self.meta_seq,
            self.effects
        )
    }
}

// ── Hook ────────────────────────────────────────────────────────────────────

/// Mutable targets for one [`set_custody_group_count`] call.
pub struct CgcHookTarget<'a, G: GossipsubControl> {
    /// Local custody / sampling sets.
    pub custody: &'a mut CustodyManager,
    /// Sole gossip subscription owner.
    pub registry: &'a mut TopicRegistry<G>,
    /// Local ENR writer (batched apply).
    pub enr: &'a EnrManager,
    /// Local MetaData v3.
    pub metadata: &'a LocalMetaData,
    /// Digest whose column topics are live.
    pub digest: ForkDigest,
}

impl<G: GossipsubControl> fmt::Debug for CgcHookTarget<'_, G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CgcHookTarget")
            .field("cgc", &self.custody.cgc())
            .field("sampling_size", &self.custody.sampling_size())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Perform the five §6.4 effects for a new custody group count.
///
/// # Errors
///
/// - [`CgcHookError::CgcOutOfRange`] when `n > NUMBER_OF_CUSTODY_GROUPS`
/// - registry / ENR failures from the underlying writers
pub fn set_custody_group_count<G: GossipsubControl>(
    target: CgcHookTarget<'_, G>,
    n: u64,
) -> Result<CgcHookOutcome, CgcHookError> {
    if n > NUMBER_OF_CUSTODY_GROUPS {
        return Err(CgcHookError::CgcOutOfRange { got: n });
    }

    let CgcHookTarget {
        custody,
        registry,
        enr,
        metadata,
        digest,
    } = target;

    let mut effects = Vec::with_capacity(8);

    // ── 1. recompute custody + sampling ────────────────────────────────────
    let before_subnets = custody.column_subnets().clone();
    custody.set_cgc(n);
    let sampling_size = custody.sampling_size();
    let cgc = custody.cgc();
    effects.push(CgcEffectKind::Recompute);

    // ── 2 + 3. column weight + subscribe/unsubscribe via registry ──────────
    let column_weight = column_topic_weight(sampling_size);
    let params = TopicParams {
        topic_weight: column_weight,
    };
    let desired = custody.column_subnets().clone();
    let added: Vec<_> = desired.difference(&before_subnets).copied().collect();
    let removed: Vec<_> = before_subnets.difference(&desired).copied().collect();

    registry.sync_column_subnets(digest, &desired, params)?;

    // Effect markers from set diffs (independent of the concrete GossipsubControl).
    if !desired.is_empty() || !before_subnets.is_empty() {
        effects.push(CgcEffectKind::ColumnParams);
    }
    if !added.is_empty() {
        effects.push(CgcEffectKind::ColumnSubscribe);
    }
    if !removed.is_empty() {
        effects.push(CgcEffectKind::ColumnUnsubscribe);
    }

    // ── 4. ENR cgc (one seq bump via apply) ────────────────────────────────
    let enr_seq_before = enr.local_enr().seq();
    enr.apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(cgc))])?;
    let enr_seq_after = enr.local_enr().seq();
    effects.push(CgcEffectKind::EnrApply);

    // ── 5. MetaData v3 ─────────────────────────────────────────────────────
    let meta_seq = metadata.set_custody_group_count(cgc);
    effects.push(CgcEffectKind::MetaData);

    Ok(CgcHookOutcome {
        cgc,
        sampling_size,
        column_weight,
        enr_seq_before,
        enr_seq_after,
        meta_seq,
        effects,
    })
}

/// Column-family total weight invariant (ADR P2-07): always 0.5.
#[must_use]
pub fn column_family_total(sampling_size: u64) -> f64 {
    column_topic_weight(sampling_size) * sampling_size.max(1) as f64
}

/// Assert ADR P2-07 within a loose epsilon.
#[must_use]
pub fn column_family_total_is_half(sampling_size: u64) -> bool {
    (column_family_total(sampling_size) - COLUMN_FAMILY_TOTAL).abs() < 1e-12
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::discovery::enr::{read_cgc, EnrSeqStrategy};
    use crate::fork_digest::ForkContext;
    use crate::gossip::{
        format_topic_string, GossipCall, RecordingGossipsub, SubnetCounts, TopicName, TopicRegistry,
    };
    use crate::reqresp::LocalMetaData;
    use cc_types::{
        get_custody_groups, sampling_size as types_sampling_size, BlobParameters, BlobSchedule,
        ChainConfig, Epoch, ForkVersion, PresetName, Root, CUSTODY_REQUIREMENT,
        DATA_COLUMN_SIDECAR_SUBNET_COUNT, NUMBER_OF_CUSTODY_GROUPS, SAMPLES_PER_SLOT,
    };
    use discv5::enr::NodeId;
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
            config_name: "synthetic-cgc-hook".into(),
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

    struct Fixture {
        custody: CustodyManager,
        registry: TopicRegistry<RecordingGossipsub>,
        enr: EnrManager,
        metadata: LocalMetaData,
        digest: ForkDigest,
    }

    impl Fixture {
        fn new(node: NodeId, cgc: u64) -> Self {
            let ctx = synthetic_ctx();
            let digest = ctx.current_digest();
            let counts = SubnetCounts {
                attestation: 0,
                sync_committee: 0,
                data_column_sidecar: DATA_COLUMN_SIDECAR_SUBNET_COUNT,
            };
            let mut registry = TopicRegistry::new(RecordingGossipsub::default(), &ctx, counts);
            for id in 0..DATA_COLUMN_SIDECAR_SUBNET_COUNT {
                registry.register_validator(TopicName::DataColumnSidecar(id));
            }

            let custody = CustodyManager::new(node, cgc);
            let weight = column_topic_weight(custody.sampling_size());
            let params = TopicParams {
                topic_weight: weight,
            };
            custody
                .subscribe_sampled_columns(&mut registry, digest, params)
                .expect("initial subscribe");
            // Clear the initial subscribe log so hook assertions only see the
            // mutation under test.
            registry.gossip_mut().calls.clear();

            let enr = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).expect("enr");
            enr.apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(cgc))])
                .expect("seed cgc");
            let metadata = LocalMetaData::new(cgc);

            Self {
                custody,
                registry,
                enr,
                metadata,
                digest,
            }
        }

        fn run(&mut self, n: u64) -> CgcHookOutcome {
            set_custody_group_count(
                CgcHookTarget {
                    custody: &mut self.custody,
                    registry: &mut self.registry,
                    enr: &self.enr,
                    metadata: &self.metadata,
                    digest: self.digest,
                },
                n,
            )
            .expect("hook")
        }

        fn live_column_subnets(&self) -> HashSet<u64> {
            self.registry
                .subscribed_keys()
                .iter()
                .filter_map(|k| match k.name {
                    TopicName::DataColumnSidecar(id) if k.digest == self.digest => Some(id),
                    _ => None,
                })
                .collect()
        }
    }

    /// CC-21/6 + D-7: 4 → 8 asserts all five effects in one run.
    #[test]
    fn set_custody_group_count_4_to_8_five_effects() {
        let node = node_id_from_u64(0xC6C_0004);
        let mut fx = Fixture::new(node, CUSTODY_REQUIREMENT);
        assert_eq!(fx.custody.cgc(), 4);
        assert_eq!(fx.custody.sampling_size(), 8);
        assert_eq!(fx.custody.custodied().len(), 4);
        assert_eq!(fx.custody.sampled().len(), 8);

        let before_custody = fx.custody.custodied().as_set().clone();
        let before_sampled = fx.custody.sampled().as_set().clone();
        let enr_seq0 = fx.enr.local_enr().seq();
        let meta_seq0 = fx.metadata.seq_number();

        let out = fx.run(8);

        // 1. both sets re-derived; custodied grows 4 → 8; sampled stays size 8
        //    (sampling_size floor) but is re-derived from helpers.
        assert_eq!(fx.custody.cgc(), 8);
        assert_eq!(fx.custody.sampling_size(), 8);
        assert_eq!(fx.custody.custodied().len(), 8);
        assert_eq!(fx.custody.sampled().len(), 8);
        assert_ne!(
            fx.custody.custodied().as_set(),
            &before_custody,
            "custodied must change 4 → 8"
        );
        // Sampled re-derived via get_custody_groups(node, 8) — same size as
        // before (also sampling_size 8); membership is the helper's set.
        let expected_sampled =
            get_custody_groups(crate::discovery::enr::node_id_as_u256(node), 8);
        assert_eq!(fx.custody.sampled().as_set(), &expected_sampled);
        let expected_custody =
            get_custody_groups(crate::discovery::enr::node_id_as_u256(node), 8);
        assert_eq!(fx.custody.custodied().as_set(), &expected_custody);
        // Document whether sampled membership moved (node-dependent).
        let _ = before_sampled;

        // 2. column weight = 0.5/8; set_topic_params for every column topic;
        //    family total stays 0.5.
        let expected_w = 0.5 / 8.0;
        assert!((out.column_weight - expected_w).abs() < 1e-12);
        assert!(column_family_total_is_half(8));
        assert!((column_family_total(8) - 0.5).abs() < 1e-12);

        let param_calls: Vec<_> = fx
            .registry
            .gossip()
            .calls
            .iter()
            .filter_map(|c| match c {
                GossipCall::SetTopicParams { topic, params } => {
                    Some((topic.clone(), params.topic_weight))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            param_calls.len(),
            8,
            "set_topic_params for every column topic: {param_calls:?}"
        );
        for (topic, w) in &param_calls {
            assert!(
                topic.contains("data_column_sidecar_"),
                "non-column topic {topic}"
            );
            assert!((w - expected_w).abs() < 1e-12, "weight {w} on {topic}");
        }

        // 3. subscribed set matches sampled subnets; params precede each new
        //    subscribe (none expected when sampling_size stays 8, but order
        //    still holds for any that fire).
        assert_eq!(
            fx.live_column_subnets(),
            fx.custody
                .column_subnets()
                .iter()
                .copied()
                .collect::<HashSet<_>>()
        );
        assert_params_before_subscribe(&fx.registry.gossip().calls);

        // 4. ENR cgc changes; seq + exactly one.
        assert_eq!(read_cgc(&fx.enr.local_enr()), Some(8));
        assert_eq!(out.enr_seq_after, enr_seq0 + 1);
        assert_eq!(out.enr_seq_after - out.enr_seq_before, 1);

        // 5. MetaData v3.custody_group_count + seq bump.
        let md = fx.metadata.load();
        assert_eq!(md.custody_group_count, 8);
        assert_eq!(md.seq_number, meta_seq0 + 1);
        assert_eq!(out.meta_seq, meta_seq0 + 1);

        // Ordering of the five steps.
        assert!(
            out.order_is_valid(),
            "§6.4 order violated: {:?}",
            out.effects
        );
        assert_eq!(out.effects.first(), Some(&CgcEffectKind::Recompute));
        assert!(out.effects.contains(&CgcEffectKind::ColumnParams));
        assert_eq!(out.effects.last(), Some(&CgcEffectKind::MetaData));
        let enr_pos = out
            .effects
            .iter()
            .position(|e| *e == CgcEffectKind::EnrApply)
            .unwrap();
        let meta_pos = out
            .effects
            .iter()
            .position(|e| *e == CgcEffectKind::MetaData)
            .unwrap();
        let params_pos = out
            .effects
            .iter()
            .position(|e| *e == CgcEffectKind::ColumnParams)
            .unwrap();
        assert!(params_pos < enr_pos, "params before ENR advertise");
        assert!(enr_pos < meta_pos, "ENR before MetaData");
    }

    /// Supernode ceiling 4 → 128: 128 subnets, family total still 0.5.
    #[test]
    fn set_custody_group_count_4_to_128_supernode_ceiling() {
        let node = node_id_from_u64(0xC6C_0128);
        let mut fx = Fixture::new(node, CUSTODY_REQUIREMENT);
        let before_sampled = fx.custody.sampled().as_set().clone();
        let before_custody = fx.custody.custodied().as_set().clone();
        let enr_seq0 = fx.enr.local_enr().seq();

        let out = fx.run(NUMBER_OF_CUSTODY_GROUPS);

        assert_eq!(fx.custody.cgc(), NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(fx.custody.sampling_size(), NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(fx.custody.custodied().len() as u64, NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(fx.custody.sampled().len() as u64, NUMBER_OF_CUSTODY_GROUPS);
        assert_ne!(fx.custody.custodied().as_set(), &before_custody);
        assert_ne!(fx.custody.sampled().as_set(), &before_sampled);

        let expected_w = 0.5 / NUMBER_OF_CUSTODY_GROUPS as f64;
        assert!((out.column_weight - expected_w).abs() < 1e-12);
        assert!(column_family_total_is_half(NUMBER_OF_CUSTODY_GROUPS));
        assert!((column_family_total(NUMBER_OF_CUSTODY_GROUPS) - 0.5).abs() < 1e-12);

        let live = fx.live_column_subnets();
        assert_eq!(
            live.len() as u64,
            DATA_COLUMN_SIDECAR_SUBNET_COUNT,
            "128 column subnets subscribed"
        );
        assert_eq!(
            live,
            fx.custody
                .column_subnets()
                .iter()
                .copied()
                .collect::<HashSet<_>>()
        );

        // New subnets: set_topic_params before each subscribe.
        assert_params_before_subscribe(&fx.registry.gossip().calls);
        assert!(out.effects.contains(&CgcEffectKind::ColumnSubscribe));
        assert!(out.order_is_valid(), "{:?}", out.effects);

        assert_eq!(read_cgc(&fx.enr.local_enr()), Some(NUMBER_OF_CUSTODY_GROUPS));
        assert_eq!(out.enr_seq_after, enr_seq0 + 1);
        assert_eq!(fx.metadata.load().custody_group_count, NUMBER_OF_CUSTODY_GROUPS);
    }

    /// Shrink 8 → 4: sampling_size floors at 8; sampled/custodied diverge.
    #[test]
    fn set_custody_group_count_shrink_8_to_4_floors_sampling() {
        let node = node_id_from_u64(0xC6C_0008);
        // Start at cgc=16 so sampling_size=16 and a shrink to 4 drops subnets.
        // AC names 8→4 for the floor case; also cover 16→4 for real unsubscribes.
        let mut fx = Fixture::new(node, 8);
        assert_eq!(fx.custody.sampling_size(), SAMPLES_PER_SLOT);
        assert_eq!(fx.custody.custodied().len(), 8);
        assert_eq!(fx.custody.sampled().len(), 8);

        let out = fx.run(4);

        assert_eq!(fx.custody.cgc(), 4);
        assert_eq!(
            fx.custody.sampling_size(),
            SAMPLES_PER_SLOT,
            "sampling_size floors at SAMPLES_PER_SLOT when cgc shrinks below it"
        );
        assert_eq!(fx.custody.custodied().len(), 4);
        assert_eq!(fx.custody.sampled().len(), 8);
        assert!(
            fx.custody
                .custodied()
                .as_set()
                .is_subset(fx.custody.sampled().as_set()),
            "custody ⊆ sampled after shrink"
        );
        // Sampled vs custodied visibly diverge.
        assert_ne!(
            fx.custody.custodied().as_set(),
            fx.custody.sampled().as_set()
        );
        // Subscriptions follow **sampled** (still 8), not custodied (4).
        assert_eq!(fx.live_column_subnets().len(), 8);
        assert_eq!(types_sampling_size(4), 8);
        assert!((out.column_weight - 0.5 / 8.0).abs() < 1e-12);
        assert!(out.order_is_valid(), "{:?}", out.effects);
        assert_eq!(read_cgc(&fx.enr.local_enr()), Some(4));
        assert_eq!(fx.metadata.load().custody_group_count, 4);
    }

    /// Shrink that actually drops subnets: 16 → 4 unsubscribes and floors at 8.
    #[test]
    fn set_custody_group_count_shrink_16_to_4_unsubscribes() {
        let node = node_id_from_u64(0xC6C_0016);
        let mut fx = Fixture::new(node, 16);
        assert_eq!(fx.custody.sampling_size(), 16);
        assert_eq!(fx.live_column_subnets().len(), 16);

        let out = fx.run(4);

        assert_eq!(fx.custody.sampling_size(), 8);
        assert_eq!(fx.custody.custodied().len(), 4);
        assert_eq!(fx.custody.sampled().len(), 8);
        assert_eq!(fx.live_column_subnets().len(), 8);
        assert!(
            out.effects.contains(&CgcEffectKind::ColumnUnsubscribe),
            "must unsubscribe dropped subnets: {:?}",
            out.effects
        );
        assert!(out.order_is_valid(), "{:?}", out.effects);
        // Unsubscribes must not precede params/recompute, and ENR after.
        let unsub = out
            .effects
            .iter()
            .position(|e| *e == CgcEffectKind::ColumnUnsubscribe)
            .unwrap();
        let enr = out
            .effects
            .iter()
            .position(|e| *e == CgcEffectKind::EnrApply)
            .unwrap();
        assert!(unsub < enr, "unsubscribe before ENR advertise");
    }

    /// Advertising before subscribing would fail the order check.
    #[test]
    fn order_rejects_advertise_before_subscribe() {
        let mut bad = CgcHookOutcome {
            cgc: 8,
            sampling_size: 8,
            column_weight: 0.0625,
            enr_seq_before: 0,
            enr_seq_after: 1,
            meta_seq: 1,
            effects: vec![
                CgcEffectKind::Recompute,
                CgcEffectKind::EnrApply, // advertised too early
                CgcEffectKind::ColumnParams,
                CgcEffectKind::ColumnSubscribe,
                CgcEffectKind::MetaData,
            ],
        };
        assert!(!bad.order_is_valid());
        // Fix order.
        bad.effects = vec![
            CgcEffectKind::Recompute,
            CgcEffectKind::ColumnParams,
            CgcEffectKind::ColumnSubscribe,
            CgcEffectKind::EnrApply,
            CgcEffectKind::MetaData,
        ];
        assert!(bad.order_is_valid());
    }

    #[test]
    fn rejects_cgc_above_number_of_custody_groups() {
        let node = node_id_from_u64(1);
        let mut fx = Fixture::new(node, 4);
        let err = set_custody_group_count(
            CgcHookTarget {
                custody: &mut fx.custody,
                registry: &mut fx.registry,
                enr: &fx.enr,
                metadata: &fx.metadata,
                digest: fx.digest,
            },
            NUMBER_OF_CUSTODY_GROUPS + 1,
        )
        .unwrap_err();
        assert!(matches!(err, CgcHookError::CgcOutOfRange { .. }));
    }

    #[test]
    fn params_before_subscribe_on_growth_path() {
        // 4 → 16 grows sampling_size 8 → 16.
        let node = node_id_from_u64(0xC6C_0010);
        let mut fx = Fixture::new(node, 4);
        let _ = fx.run(16);
        assert_params_before_subscribe(&fx.registry.gossip().calls);

        // Every Subscribe for a column topic has a preceding SetTopicParams
        // for the same topic string in the call log.
        let calls = &fx.registry.gossip().calls;
        for (i, c) in calls.iter().enumerate() {
            if let GossipCall::Subscribe { topic } = c {
                if !topic.contains("data_column_sidecar_") {
                    continue;
                }
                let prior_params = calls[..i].iter().any(|p| {
                    matches!(
                        p,
                        GossipCall::SetTopicParams { topic: t, .. } if t == topic
                    )
                });
                assert!(
                    prior_params,
                    "subscribe to {topic} without prior set_topic_params at {i}: {calls:?}"
                );
            }
        }
    }

    fn assert_params_before_subscribe(calls: &[GossipCall]) {
        // Walk pairs: never see Subscribe for a topic that has not yet had
        // SetTopicParams in this batch. Also, immediately before a Subscribe
        // that is part of the (params, subscribe) pair, prefer params.
        let mut params_seen: HashSet<String> = HashSet::new();
        for c in calls {
            match c {
                GossipCall::SetTopicParams { topic, .. } => {
                    params_seen.insert(topic.clone());
                }
                GossipCall::Subscribe { topic } => {
                    assert!(
                        params_seen.contains(topic),
                        "subscribe without prior set_topic_params for {topic}: {calls:?}"
                    );
                }
                GossipCall::Unsubscribe { .. } => {}
            }
        }
    }

    #[test]
    fn topic_string_shape_stable() {
        // Sanity: format matches what RecordingGossipsub records.
        let ctx = synthetic_ctx();
        let d = ctx.current_digest();
        let s = format_topic_string(&d, TopicName::DataColumnSidecar(3));
        assert!(s.contains("data_column_sidecar_3"));
    }
}
