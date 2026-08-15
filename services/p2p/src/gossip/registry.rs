//! Topic registry and BPO resubscription state machine — §5.1 / CC-22a.
//!
//! **Single owner** of `subscribe` / `unsubscribe` / `set_topic_params` against
//! the gossipsub control surface. Call sites of those three operations live
//! only in this file (acceptance grep).
//!
//! ## Ordering rule
//!
//! [`TopicRegistry::subscribe`] always calls `set_topic_params` **before**
//! `subscribe` on the underlying control handle. There is no parameterless
//! subscribe — scoring defaults would otherwise apply invisibly (CC-22c owns
//! the numeric weights; this issue owns the ordering).
//!
//! ## Validator guard
//!
//! A topic with no registered validator is refused. Phase 2's first real
//! subscription lands in CC-22d; until then the registry only bookkeeps.
//!
//! ## State machine (CC-2A — schedule-driven epoch tick)
//!
//! Boundary detection is **read from the schedule in advance** via
//! [`ForkContext::next`] / [`crate::fork_digest::next_fork`] (CC-21b lookahead;
//! CC-2A supplies the epoch-tick trigger). Never discovered on failure.
//!
//! ```text
//! Steady   live = {current}
//!   │  epoch >= boundary − 1   (a missed tick still enters)
//! Overlap  live = {current, next}   ← subscribe(next) here (params first)
//!   │  epoch >= boundary + 1
//! Drain    live = {next}; unsubscribe(current)
//!   │  next advance_to (or same call when epoch >= boundary + 2)
//! Steady with current := next
//! ```
//!
//! Call [`TopicRegistry::advance_to`] once per epoch tick **after**
//! [`ForkContext::on_epoch`]. Publishing uses [`TopicRegistry::publish_digest`]
//! (switches at the boundary); Status/ENR use [`ForkContext::current_digest`].

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

use cc_types::{Epoch, ForkDigest, SubnetId};

use crate::fork_digest::ForkContext;

use super::topics::{SubnetCounts, TopicKey, TopicName, format_topic_string};

// ── Gossipsub control surface ───────────────────────────────────────────────

/// Opaque per-topic scoring parameters.
///
/// Column weight is `0.5 / sampling_size` ([`super::column_topic_weight`]) —
/// recomputed by the CC-21d custody hook. Other families use the CC-22c table.
/// `f64` so the column formula is expressible (not a placeholder integer).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicParams {
    /// Topic weight (column family: `0.5 / sampling_size`).
    pub topic_weight: f64,
}

/// Errors from the gossipsub control adapter (not registry policy errors).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GossipControlError {
    /// Underlying gossipsub rejected the operation.
    #[error("gossipsub control error: {0}")]
    Failed(&'static str),
}

/// Minimal control surface the registry drives.
///
/// Real rust-libp2p `Gossipsub` is wired in CC-20a / CC-22b; tests use
/// [`RecordingGossipsub`]. Method names match the libp2p API so the
/// ownership grep stays meaningful once the real backend lands.
pub trait GossipsubControl {
    /// Register scoring parameters for `topic` **before** subscribing.
    ///
    /// # Errors
    ///
    /// Returns [`GossipControlError`] when the backend rejects the params.
    fn set_topic_params(
        &mut self,
        topic: &str,
        params: &TopicParams,
    ) -> Result<(), GossipControlError>;

    /// Subscribe to `topic` (params must already be set).
    ///
    /// # Errors
    ///
    /// Returns [`GossipControlError`] when the backend rejects the subscribe.
    fn subscribe(&mut self, topic: &str) -> Result<bool, GossipControlError>;

    /// Unsubscribe from `topic`.
    ///
    /// # Errors
    ///
    /// Returns [`GossipControlError`] when the backend rejects the unsubscribe.
    fn unsubscribe(&mut self, topic: &str) -> Result<bool, GossipControlError>;
}

/// Recorded control call for ordering / ownership tests.
#[derive(Debug, Clone, PartialEq)]
pub enum GossipCall {
    /// `set_topic_params` was invoked.
    SetTopicParams {
        /// Topic string.
        topic: String,
        /// Params snapshot.
        params: TopicParams,
    },
    /// `subscribe` was invoked.
    Subscribe {
        /// Topic string.
        topic: String,
    },
    /// `unsubscribe` was invoked.
    Unsubscribe {
        /// Topic string.
        topic: String,
    },
}

/// In-memory gossipsub stub that records call order.
#[derive(Debug, Default, Clone)]
pub struct RecordingGossipsub {
    /// Ordered call log.
    pub calls: Vec<GossipCall>,
    /// Currently subscribed topic strings.
    pub subscribed: HashSet<String>,
    /// Topics that have had params set at least once.
    pub params_set: HashSet<String>,
}

impl GossipsubControl for RecordingGossipsub {
    fn set_topic_params(
        &mut self,
        topic: &str,
        params: &TopicParams,
    ) -> Result<(), GossipControlError> {
        self.params_set.insert(topic.to_owned());
        self.calls.push(GossipCall::SetTopicParams {
            topic: topic.to_owned(),
            params: params.clone(),
        });
        Ok(())
    }

    fn subscribe(&mut self, topic: &str) -> Result<bool, GossipControlError> {
        self.calls.push(GossipCall::Subscribe {
            topic: topic.to_owned(),
        });
        Ok(self.subscribed.insert(topic.to_owned()))
    }

    fn unsubscribe(&mut self, topic: &str) -> Result<bool, GossipControlError> {
        self.calls.push(GossipCall::Unsubscribe {
            topic: topic.to_owned(),
        });
        Ok(self.subscribed.remove(topic))
    }
}

// ── Registry ────────────────────────────────────────────────────────────────

/// BPO resubscription phase (§5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionPhase {
    /// Only `current` digest topics are live.
    Steady,
    /// `current` and `next` coexist (subscribed one epoch early).
    Overlap,
    /// Only `next` is live; `current` has been unsubscribed.
    Drain,
}

/// Policy / bookkeeping errors from [`TopicRegistry`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    /// Subscribe refused: no validator registered for this topic name (CC-22/4).
    #[error("no validator registered for topic {}", .0.path_segment())]
    NoValidator(TopicName),
    /// Underlying gossipsub control failed.
    #[error(transparent)]
    Control(#[from] GossipControlError),
}

/// Owns live subscriptions and is the only caller of gossipsub subscribe /
/// unsubscribe / set_topic_params.
pub struct TopicRegistry<G: GossipsubControl> {
    gossip: G,
    counts: SubnetCounts,
    phase: SubscriptionPhase,
    /// Primary digest (Steady publishing digest).
    current: ForkDigest,
    /// Next digest during Overlap / Drain.
    next_digest: Option<ForkDigest>,
    /// Epoch at which `next_digest` activates (digest change boundary).
    boundary: Option<Epoch>,
    /// Digests whose topics are considered live.
    live: HashSet<ForkDigest>,
    /// Topic names that have a validator registered (presence-only until CC-22d).
    validators: HashSet<TopicName>,
    /// Active subscriptions with the params used at subscribe time.
    subscribed: HashMap<TopicKey, TopicParams>,
}

impl<G: GossipsubControl> fmt::Debug for TopicRegistry<G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicRegistry")
            .field("phase", &self.phase)
            .field("current", &self.current)
            .field("next_digest", &self.next_digest)
            .field("boundary", &self.boundary)
            .field("live_count", &self.live.len())
            .field("validators", &self.validators.len())
            .field("subscribed", &self.subscribed.len())
            .field("counts", &self.counts)
            .finish_non_exhaustive()
    }
}

impl<G: GossipsubControl> TopicRegistry<G> {
    /// Construct in [`SubscriptionPhase::Steady`] for `ctx`'s current digest.
    #[must_use]
    pub fn new(gossip: G, ctx: &ForkContext, counts: SubnetCounts) -> Self {
        let current = ctx.current_digest();
        let mut live = HashSet::new();
        live.insert(current);
        let (boundary, next_digest) = match ctx.next() {
            Some((epoch, _, digest)) => (Some(epoch), Some(digest)),
            None => (None, None),
        };
        Self {
            gossip,
            counts,
            phase: SubscriptionPhase::Steady,
            current,
            next_digest,
            boundary,
            live,
            validators: HashSet::new(),
            subscribed: HashMap::new(),
        }
    }

    /// Borrow the control handle (tests / diagnostics).
    #[must_use]
    pub fn gossip(&self) -> &G {
        &self.gossip
    }

    /// Mutable control handle (tests).
    pub fn gossip_mut(&mut self) -> &mut G {
        &mut self.gossip
    }

    /// Current phase.
    #[must_use]
    pub fn phase(&self) -> SubscriptionPhase {
        self.phase
    }

    /// Primary digest.
    #[must_use]
    pub fn current_digest(&self) -> ForkDigest {
        self.current
    }

    /// Live digest set (copy).
    #[must_use]
    pub fn live_digests(&self) -> HashSet<ForkDigest> {
        self.live.clone()
    }

    /// Scheduled digest-change boundary, if known from the schedule.
    #[must_use]
    pub fn boundary(&self) -> Option<Epoch> {
        self.boundary
    }

    /// Next-digest twin during Overlap / Drain.
    #[must_use]
    pub fn next_digest(&self) -> Option<ForkDigest> {
        self.next_digest
    }

    /// Digest used for **publish / validate** at `epoch`.
    ///
    /// Steady and pre-boundary Overlap publish on `current`; from the boundary
    /// epoch inclusive (and through Drain) publish on `next` (CC-2A / §5.1 step 3).
    #[must_use]
    pub fn publish_digest(&self, epoch: Epoch) -> ForkDigest {
        match self.phase {
            SubscriptionPhase::Steady => self.current,
            SubscriptionPhase::Overlap => {
                if self.boundary.is_some_and(|b| epoch.as_u64() >= b.as_u64()) {
                    self.next_digest.unwrap_or(self.current)
                } else {
                    self.current
                }
            }
            SubscriptionPhase::Drain => self.next_digest.unwrap_or(self.current),
        }
    }

    /// Subnet counts used for expansion.
    #[must_use]
    pub fn counts(&self) -> SubnetCounts {
        self.counts
    }

    /// Whether `name` has a validator registered.
    #[must_use]
    pub fn has_validator(&self, name: TopicName) -> bool {
        self.validators.contains(&name)
    }

    /// Keys currently subscribed.
    #[must_use]
    pub fn subscribed_keys(&self) -> HashSet<TopicKey> {
        self.subscribed.keys().copied().collect()
    }

    /// Mark that a validator exists for `name`.
    ///
    /// Real validators land in CC-22d / CC-2B / CC-2C / CC-2D; this is the
    /// structural slot the subscribe guard checks.
    pub fn register_validator(&mut self, name: TopicName) {
        self.validators.insert(name);
    }

    /// Register validators for every expanded Fulu name under `counts`.
    pub fn register_all_fulu_validators(&mut self) {
        for name in super::topics::expand_fulu_topic_names(&self.counts) {
            self.validators.insert(name);
        }
    }

    /// Subscribe to `key` with `params`.
    ///
    /// **Ordering:** `set_topic_params` then `subscribe` on the control surface.
    /// **Guard:** refuses if no validator is registered for `key.name`.
    ///
    /// # Errors
    ///
    /// - [`RegistryError::NoValidator`] when no validator is registered
    /// - [`RegistryError::Control`] on backend failure
    pub fn subscribe(&mut self, key: TopicKey, params: TopicParams) -> Result<(), RegistryError> {
        if !self.validators.contains(&key.name) {
            return Err(RegistryError::NoValidator(key.name));
        }
        self.subscribe_unchecked(key, params)
    }

    /// Apply scoring params for `key` **without** (un)subscribing.
    ///
    /// Sole external path for mid-lifetime weight updates (CC-21d column weight
    /// recompute). Updates the bookkeeping map when the topic is already live.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Control`] on backend failure.
    pub fn set_topic_params(
        &mut self,
        key: &TopicKey,
        params: TopicParams,
    ) -> Result<(), RegistryError> {
        let topic = key.topic_string();
        self.gossip.set_topic_params(&topic, &params)?;
        if let Some(stored) = self.subscribed.get_mut(key) {
            *stored = params;
        }
        Ok(())
    }

    /// Resync column-sidecar subscriptions for `digest` to `desired` subnets.
    ///
    /// §6.4 / §5.1 ordering (one mechanism, two triggers with the BPO machine):
    /// 1. `set_topic_params` for **every** topic that remains or will be
    ///    subscribed (new weight first — never score at GossipSub defaults);
    /// 2. `subscribe` newly desired subnets (params-first via
    ///    [`Self::subscribe`]);
    /// 3. `unsubscribe` subnets no longer desired.
    ///
    /// # Errors
    ///
    /// Propagates [`RegistryError`] from params / subscribe / unsubscribe.
    pub fn sync_column_subnets(
        &mut self,
        digest: ForkDigest,
        desired: &BTreeSet<SubnetId>,
        params: TopicParams,
    ) -> Result<(), RegistryError> {
        self.resync_subnet_family(
            digest,
            desired,
            params,
            TopicName::DataColumnSidecar,
            |name| match name {
                TopicName::DataColumnSidecar(id) => Some(id),
                _ => None,
            },
        )
    }

    /// Resync `sync_committee_{id}` subscriptions for `digest` to `desired`.
    ///
    /// Same params-first ordering as [`Self::sync_column_subnets`] (CC-2D /
    /// §6.6). Called only from [`crate::discovery::SubnetManager`].
    ///
    /// # Errors
    ///
    /// Propagates [`RegistryError`] from params / subscribe / unsubscribe.
    pub fn sync_sync_committee_subnets(
        &mut self,
        digest: ForkDigest,
        desired: &BTreeSet<SubnetId>,
        params: TopicParams,
    ) -> Result<(), RegistryError> {
        self.resync_subnet_family(digest, desired, params, TopicName::SyncCommittee, |name| {
            match name {
                TopicName::SyncCommittee(id) => Some(id),
                _ => None,
            }
        })
    }

    /// Resync `beacon_attestation_{subnet_id}` subscriptions for `digest`.
    ///
    /// Same params-first ordering as [`Self::sync_column_subnets`] — the third
    /// subscription trigger (subnet rotation / CC-2C) shares the mechanism with
    /// BPO boundary and custody change.
    ///
    /// # Errors
    ///
    /// Propagates [`RegistryError`] from params / subscribe / unsubscribe.
    pub fn sync_attestation_subnets(
        &mut self,
        digest: ForkDigest,
        desired: &BTreeSet<SubnetId>,
        params: TopicParams,
    ) -> Result<(), RegistryError> {
        self.resync_subnet_family(
            digest,
            desired,
            params,
            TopicName::BeaconAttestation,
            |name| match name {
                TopicName::BeaconAttestation(id) => Some(id),
                _ => None,
            },
        )
    }

    fn resync_subnet_family(
        &mut self,
        digest: ForkDigest,
        desired: &BTreeSet<SubnetId>,
        params: TopicParams,
        name_of: fn(SubnetId) -> TopicName,
        extract: impl Fn(TopicName) -> Option<SubnetId>,
    ) -> Result<(), RegistryError> {
        let current: BTreeSet<SubnetId> = self
            .subscribed
            .keys()
            .filter_map(|k| {
                if k.digest == digest {
                    return extract(k.name);
                }
                None
            })
            .collect();
        self.sync_subnet_diff(digest, &current, desired, params, name_of)
    }

    /// Params → subscribe new → unsubscribe dropped for one subnet family.
    fn sync_subnet_diff(
        &mut self,
        digest: ForkDigest,
        current: &BTreeSet<SubnetId>,
        desired: &BTreeSet<SubnetId>,
        params: TopicParams,
        name_of: fn(SubnetId) -> TopicName,
    ) -> Result<(), RegistryError> {
        // Params for every topic that remains live (weight first).
        for &subnet in desired {
            let key = TopicKey::new(digest, name_of(subnet));
            if current.contains(&subnet) {
                self.set_topic_params(&key, params.clone())?;
            }
            // New topics get params inside `subscribe` (params-first).
        }

        for &subnet in desired {
            if !current.contains(&subnet) {
                let key = TopicKey::new(digest, name_of(subnet));
                self.subscribe(key, params.clone())?;
            }
        }
        for &subnet in current {
            if !desired.contains(&subnet) {
                let key = TopicKey::new(digest, name_of(subnet));
                self.unsubscribe(&key)?;
            }
        }
        Ok(())
    }

    /// Unsubscribe `key` if present.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Control`] on backend failure.
    pub fn unsubscribe(&mut self, key: &TopicKey) -> Result<(), RegistryError> {
        if !self.subscribed.contains_key(key) {
            return Ok(());
        }
        let topic = key.topic_string();
        // Only call site of gossipsub.unsubscribe (via trait) outside tests.
        self.gossip.unsubscribe(&topic)?;
        self.subscribed.remove(key);
        Ok(())
    }

    /// Drive Steady → Overlap → Drain → Steady from the epoch tick (CC-2A).
    ///
    /// **Trigger wiring:** call once per epoch **after** [`ForkContext::on_epoch`].
    /// The boundary is taken from `ctx.next()` (schedule lookahead — regular
    /// fork **or** BPO), never discovered on subscribe failure.
    ///
    /// Ordering inside transitions (the bug surface, §5.1):
    /// 1. `set_topic_params` for every next-digest topic **before** `subscribe`
    /// 2. subscribe one full epoch before the boundary (Overlap entry)
    /// 3. publish/validate switch at the boundary ([`Self::publish_digest`])
    /// 4. unsubscribe old one epoch after (Drain); ENR eth2/nfd coalesce elsewhere
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Control`] if subscribe/unsubscribe during a
    /// phase transition fails.
    pub fn advance_to(&mut self, epoch: Epoch, ctx: &ForkContext) -> Result<(), RegistryError> {
        // Complete a prior Drain → Steady roll on the subsequent tick.
        if self.phase == SubscriptionPhase::Drain {
            self.complete_drain(ctx);
        }

        // When Steady, refresh current + scheduled next from ForkContext —
        // unless this tick has already reached a previously scheduled overlap
        // window. `on_epoch` then reports the *next* BPO, which would clobber
        // the transition we still owe and wedge the live set.
        if self.phase == SubscriptionPhase::Steady {
            let reached_scheduled = self
                .boundary
                .is_some_and(|b| epoch.as_u64().saturating_add(1) >= b.as_u64());
            if !reached_scheduled {
                self.current = ctx.current_digest();
                match ctx.next() {
                    Some((b, _, d)) => {
                        self.boundary = Some(b);
                        self.next_digest = Some(d);
                    }
                    None => {
                        self.boundary = None;
                        self.next_digest = None;
                    }
                }
                self.rebuild_live();
            }
        }

        let Some(boundary) = self.boundary else {
            return Ok(());
        };
        let Some(next_d) = self.next_digest else {
            return Ok(());
        };

        let e = epoch.as_u64();
        let b = boundary.as_u64();
        // Overlap window is [b−1, b]. Drain at b+1. Catch up if a tick was missed.
        let overlap_start = b.saturating_sub(1);

        match self.phase {
            SubscriptionPhase::Steady if e < overlap_start => {
                self.rebuild_live();
            }
            SubscriptionPhase::Steady if e <= b => {
                self.enter_overlap(next_d)?;
            }
            SubscriptionPhase::Steady => {
                // e >= b + 1: skipped the overlap window entirely.
                self.enter_overlap(next_d)?;
                self.enter_drain()?;
                if e >= b.saturating_add(2) {
                    self.complete_drain(ctx);
                }
            }
            SubscriptionPhase::Overlap if e <= b => {
                self.rebuild_live();
            }
            SubscriptionPhase::Overlap => {
                self.enter_drain()?;
                if e >= b.saturating_add(2) {
                    self.complete_drain(ctx);
                }
            }
            SubscriptionPhase::Drain => {
                self.rebuild_live();
            }
        }
        Ok(())
    }

    // ── internals ───────────────────────────────────────────────────────────

    fn subscribe_unchecked(
        &mut self,
        key: TopicKey,
        params: TopicParams,
    ) -> Result<(), RegistryError> {
        let topic = format_topic_string(&key.digest, key.name);
        // Ordering rule: params first, then subscribe. Both call sites only here.
        self.gossip.set_topic_params(&topic, &params)?;
        self.gossip.subscribe(&topic)?;
        self.subscribed.insert(key, params);
        Ok(())
    }

    fn enter_overlap(&mut self, next_d: ForkDigest) -> Result<(), RegistryError> {
        self.phase = SubscriptionPhase::Overlap;
        self.next_digest = Some(next_d);
        // Mirror every current-digest subscription onto the next digest
        // (params-first via subscribe_unchecked).
        let to_add: Vec<(TopicKey, TopicParams)> = self
            .subscribed
            .iter()
            .filter(|(k, _)| k.digest == self.current)
            .map(|(k, p)| (TopicKey::new(next_d, k.name), p.clone()))
            .filter(|(k, _)| !self.subscribed.contains_key(k))
            .collect();
        for (key, params) in to_add {
            // Validators already required for the current-digest twin.
            self.subscribe_unchecked(key, params)?;
        }
        self.rebuild_live();
        Ok(())
    }

    fn enter_drain(&mut self) -> Result<(), RegistryError> {
        self.phase = SubscriptionPhase::Drain;
        let current = self.current;
        let to_drop: Vec<TopicKey> = self
            .subscribed
            .keys()
            .filter(|k| k.digest == current)
            .copied()
            .collect();
        for key in to_drop {
            let topic = key.topic_string();
            self.gossip.unsubscribe(&topic)?;
            self.subscribed.remove(&key);
        }
        self.rebuild_live();
        Ok(())
    }

    fn complete_drain(&mut self, ctx: &ForkContext) {
        // current := next; return to Steady. Live already {next}.
        if let Some(next) = self.next_digest.take() {
            self.current = next;
        } else {
            self.current = ctx.current_digest();
        }
        self.phase = SubscriptionPhase::Steady;
        self.boundary = None;
        // Re-arm next transition from context at the new epoch view.
        match ctx.next() {
            Some((b, _, d)) => {
                self.boundary = Some(b);
                self.next_digest = Some(d);
            }
            None => {
                self.next_digest = None;
            }
        }
        self.rebuild_live();
    }

    fn rebuild_live(&mut self) {
        self.live.clear();
        match self.phase {
            SubscriptionPhase::Steady => {
                self.live.insert(self.current);
            }
            SubscriptionPhase::Overlap => {
                self.live.insert(self.current);
                if let Some(d) = self.next_digest {
                    self.live.insert(d);
                }
            }
            SubscriptionPhase::Drain => {
                if let Some(d) = self.next_digest {
                    self.live.insert(d);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::fork_digest::{ForkContext, compute_fork_digest};
    use cc_types::{
        BlobParameters, BlobSchedule, ChainConfig, Epoch, ForkDigest, ForkVersion, PresetName, Root,
    };

    fn digest(b0: u8) -> ForkDigest {
        ForkDigest::from_array([b0, 0, 0, 0])
    }

    /// Synthetic config with a single BPO at `boundary` so two digests exist.
    fn synthetic_ctx(epoch: Epoch, boundary: Epoch) -> ForkContext {
        let schedule = BlobSchedule::try_from_entries(vec![
            BlobParameters {
                epoch: boundary,
                max_blobs_per_block: 15,
            },
            BlobParameters {
                epoch: Epoch::new(boundary.as_u64() + 10_000),
                max_blobs_per_block: 21,
            },
        ])
        .expect("schedule");
        let cfg = ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "synthetic".into(),
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
        let gvr = Root::from_array([0x11; 32]);
        ForkContext::new(cfg, gvr, epoch)
    }

    #[test]
    fn subscribe_refuses_without_validator() {
        let ctx = synthetic_ctx(Epoch::new(50), Epoch::new(100));
        let mut reg = TopicRegistry::new(
            RecordingGossipsub::default(),
            &ctx,
            SubnetCounts {
                attestation: 1,
                sync_committee: 1,
                data_column_sidecar: 1,
            },
        );
        let key = TopicKey::new(ctx.current_digest(), TopicName::BeaconBlock);
        let err = reg.subscribe(key, TopicParams::default()).unwrap_err();
        assert!(matches!(
            err,
            RegistryError::NoValidator(TopicName::BeaconBlock)
        ));
        assert!(reg.gossip().calls.is_empty());
    }

    #[test]
    fn set_topic_params_precedes_subscribe_on_recording_stub() {
        let ctx = synthetic_ctx(Epoch::new(50), Epoch::new(100));
        let mut reg = TopicRegistry::new(
            RecordingGossipsub::default(),
            &ctx,
            SubnetCounts {
                attestation: 1,
                sync_committee: 1,
                data_column_sidecar: 1,
            },
        );
        reg.register_validator(TopicName::BeaconBlock);
        let key = TopicKey::new(ctx.current_digest(), TopicName::BeaconBlock);
        let params = TopicParams { topic_weight: 42.0 };
        reg.subscribe(key, params.clone()).unwrap();

        let calls = &reg.gossip().calls;
        assert_eq!(calls.len(), 2, "exactly params then subscribe: {calls:?}");
        match &calls[0] {
            GossipCall::SetTopicParams { topic, params: p } => {
                assert_eq!(topic, &key.topic_string());
                assert_eq!(p, &params);
            }
            other => panic!("first call must be SetTopicParams, got {other:?}"),
        }
        match &calls[1] {
            GossipCall::Subscribe { topic } => assert_eq!(topic, &key.topic_string()),
            other => panic!("second call must be Subscribe, got {other:?}"),
        }
        // No parameterless path exists — signature requires TopicParams.
    }

    #[test]
    fn state_machine_live_set_steady_overlap_drain() {
        let boundary = Epoch::new(100);
        let epoch_steady = Epoch::new(50);
        let mut ctx = synthetic_ctx(epoch_steady, boundary);
        let counts = SubnetCounts {
            attestation: 0,
            sync_committee: 0,
            data_column_sidecar: 0,
        };
        let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, counts);
        reg.register_validator(TopicName::BeaconBlock);

        let d_current = ctx.current_digest();
        let (_b, _, d_next) = ctx.next().expect("BPO scheduled");
        assert_ne!(d_current, d_next);

        // Subscribe one topic so Overlap mirrors it.
        reg.subscribe(
            TopicKey::new(d_current, TopicName::BeaconBlock),
            TopicParams { topic_weight: 1.0 },
        )
        .unwrap();

        // Steady: live = {current}
        assert_eq!(reg.phase(), SubscriptionPhase::Steady);
        assert_eq!(reg.live_digests(), HashSet::from([d_current]));

        // boundary − 1 → Overlap
        let e_overlap = Epoch::new(boundary.as_u64() - 1);
        ctx.on_epoch(e_overlap);
        reg.advance_to(e_overlap, &ctx).unwrap();
        assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
        assert_eq!(
            reg.live_digests(),
            HashSet::from([d_current, d_next]),
            "Overlap live set"
        );
        // Next-digest twin subscribed with params-first.
        assert!(
            reg.subscribed_keys()
                .contains(&TopicKey::new(d_next, TopicName::BeaconBlock))
        );

        // At boundary: still Overlap
        ctx.on_epoch(boundary);
        reg.advance_to(boundary, &ctx).unwrap();
        assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
        assert_eq!(reg.live_digests(), HashSet::from([d_current, d_next]));

        // boundary + 1 → Drain
        let e_drain = Epoch::new(boundary.as_u64() + 1);
        ctx.on_epoch(e_drain);
        reg.advance_to(e_drain, &ctx).unwrap();
        assert_eq!(reg.phase(), SubscriptionPhase::Drain);
        assert_eq!(
            reg.live_digests(),
            HashSet::from([d_next]),
            "Drain live set"
        );
        assert!(
            !reg.subscribed_keys()
                .contains(&TopicKey::new(d_current, TopicName::BeaconBlock)),
            "old digest unsubscribed"
        );
        assert!(
            reg.subscribed_keys()
                .contains(&TopicKey::new(d_next, TopicName::BeaconBlock))
        );

        // Next tick completes Drain → Steady with current := next
        let e_after = Epoch::new(boundary.as_u64() + 2);
        ctx.on_epoch(e_after);
        reg.advance_to(e_after, &ctx).unwrap();
        assert_eq!(reg.phase(), SubscriptionPhase::Steady);
        assert_eq!(reg.current_digest(), d_next);
        assert_eq!(reg.live_digests(), HashSet::from([d_next]));
    }

    #[test]
    fn overlap_subscribe_orders_params_before_subscribe_for_next_digest() {
        let boundary = Epoch::new(100);
        let mut ctx = synthetic_ctx(Epoch::new(50), boundary);
        let mut reg = TopicRegistry::new(
            RecordingGossipsub::default(),
            &ctx,
            SubnetCounts {
                attestation: 0,
                sync_committee: 0,
                data_column_sidecar: 0,
            },
        );
        reg.register_validator(TopicName::BeaconBlock);
        let d_current = ctx.current_digest();
        let d_next = ctx.next().unwrap().2;
        reg.subscribe(
            TopicKey::new(d_current, TopicName::BeaconBlock),
            TopicParams { topic_weight: 7.0 },
        )
        .unwrap();
        reg.gossip_mut().calls.clear();

        let e = Epoch::new(boundary.as_u64() - 1);
        ctx.on_epoch(e);
        reg.advance_to(e, &ctx).unwrap();

        let next_topic = format_topic_string(&d_next, TopicName::BeaconBlock);
        let calls = &reg.gossip().calls;
        let set_idx = calls.iter().position(
            |c| matches!(c, GossipCall::SetTopicParams { topic, .. } if topic == &next_topic),
        );
        let sub_idx = calls
            .iter()
            .position(|c| matches!(c, GossipCall::Subscribe { topic } if topic == &next_topic));
        let (si, su) = (
            set_idx.expect("set_topic_params for next"),
            sub_idx.expect("subscribe for next"),
        );
        assert!(si < su, "params must precede subscribe: {calls:?}");
    }

    #[test]
    fn skipped_epoch_tick_still_converges() {
        let boundary = Epoch::new(100);
        let mut ctx = synthetic_ctx(Epoch::new(50), boundary);
        let counts = SubnetCounts {
            attestation: 0,
            sync_committee: 0,
            data_column_sidecar: 0,
        };
        let mut reg = TopicRegistry::new(RecordingGossipsub::default(), &ctx, counts);
        reg.register_validator(TopicName::BeaconBlock);
        let d_current = ctx.current_digest();
        let d_next = ctx.next().expect("BPO scheduled").2;
        reg.subscribe(
            TopicKey::new(d_current, TopicName::BeaconBlock),
            TopicParams { topic_weight: 1.0 },
        )
        .unwrap();

        // Miss boundary − 1: jump 50 → 100. Must still enter Overlap.
        ctx.on_epoch(boundary);
        reg.advance_to(boundary, &ctx).unwrap();
        assert_eq!(reg.phase(), SubscriptionPhase::Overlap);
        assert_eq!(reg.live_digests(), HashSet::from([d_current, d_next]));
        assert!(
            reg.subscribed_keys()
                .contains(&TopicKey::new(d_next, TopicName::BeaconBlock))
        );

        // Miss boundary + 1: jump 100 → 102. Subscriptions must be {next} only.
        let skipped = Epoch::new(boundary.as_u64() + 2);
        ctx.on_epoch(skipped);
        reg.advance_to(skipped, &ctx).unwrap();
        assert_eq!(reg.live_digests(), HashSet::from([d_next]));
        assert!(
            !reg.subscribed_keys()
                .contains(&TopicKey::new(d_current, TopicName::BeaconBlock))
        );
        assert!(
            reg.subscribed_keys()
                .contains(&TopicKey::new(d_next, TopicName::BeaconBlock))
        );
    }

    #[test]
    fn unused_digest_helper_smoke() {
        // Keep compute_fork_digest import honest if synthetic digests ever need it.
        let _ = digest(1);
        let ctx = synthetic_ctx(Epoch::new(1), Epoch::new(10));
        let d = compute_fork_digest(ctx.config(), ctx.genesis_validators_root(), Epoch::new(1));
        assert_eq!(d, ctx.current_digest());
    }
}
