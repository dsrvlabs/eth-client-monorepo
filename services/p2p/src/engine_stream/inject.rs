//! §5.5 four-step injection — Architecture / CC-38b.
//!
//! ```text
//! InjectColumns → for each sidecar:
//!   1. anti-equivocation `seen` set (slot, proposer_index, column_index)
//!      already seen → duplicate; do NOT publish, do NOT re-insert
//!   2. sampling tracker: on_column(..., ColumnSource::Engine)
//!      (same entry point gossip uses; BTreeSet insert is idempotent)
//!   3. tracker emits DataAvailable when verified == required (CC-24c)
//!   4. if subscribed(column_index) && step 1 said "new":
//!      publish on data_column_sidecar_{subnet_id}
//! ```
//!
//! # Security (S-38a-1 / S-38b-1)
//!
//! - **KZG:** never skip solely on `trusted_local` without [`AuthMode::Authenticated`].
//! - **Inclusion multiproof:** **always** re-verified — never skipped, even when a
//!   future auth path allows KZG skip (S-38b-1).
//!
//! # AuthMode footgun (ops)
//!
//! [`AuthMode::Authenticated`] is a **software knob** (`with_auth`), not proof of
//! mTLS / allowlist / token. Production hosts **must** leave the default
//! [`AuthMode::Unauthenticated`] until real mutual auth is wired. A mistaken
//! `with_auth(Authenticated)` plus engine's always-`trusted_local=true` would
//! silently skip **KZG only** (inclusion still always runs).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cc_proto::p2p::InjectColumns;
use cc_types::preset::Mainnet;
use cc_types::primitives::Root;
use cc_types::sidecar::DataColumnSidecar;
use cc_types::{NUMBER_OF_COLUMNS, compute_subnet_for_data_column_sidecar};
use ssz::Decode;
use tracing::{debug, warn};
use tree_hash::TreeHash;

use crate::channels::PublishRequest;
use crate::das::SamplingHandle;
use crate::gossip::seen::{ColumnSeenKey, SeenSets};
use crate::gossip::validate::{
    AlwaysValidKzg, FailClosedKzg, KzgVerify, production_kzg_verify, verify_inclusion_proof,
};
use crate::metrics::{ColumnSource, P2pMetrics};

use super::subscription::{LocalSubscription, SubscriptionHandle};

// ── Auth / KZG policy ───────────────────────────────────────────────────────

/// Mutual-auth state for the inject stream (S-38a-1).
///
/// Until mTLS / allowlist / shared token exists, production is
/// [`AuthMode::Unauthenticated`] and **always re-verifies KZG**.
///
/// # Footgun
///
/// This is **not** bound to transport auth. Only set [`Self::Authenticated`]
/// when the process has established mutual auth out-of-band. Mis-setting it
/// skips KZG when `trusted_local` is also true — inclusion multiproof is
/// still always verified (S-38b-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMode {
    /// No mutual auth — never trust `trusted_local` alone for KZG skip.
    #[default]
    Unauthenticated,
    /// Mutual auth established (future / ops-gated). KZG skip may key on
    /// `trusted_local`. **Does not** skip inclusion multiproof.
    Authenticated,
}

impl AuthMode {
    /// Whether mutual auth is present.
    #[must_use]
    pub const fn is_authenticated(self) -> bool {
        matches!(self, Self::Authenticated)
    }
}

/// Whether production may skip KZG re-verification for this inject.
///
/// **MUST NOT** return true solely because `trusted_local` is true.
/// Requires both the wire hint **and** [`AuthMode::Authenticated`].
#[must_use]
pub fn should_skip_kzg(trusted_local: bool, auth: AuthMode) -> bool {
    trusted_local && auth.is_authenticated()
}

/// How KZG is applied on the inject path.
///
/// Inclusion multiproof is **not** governed by this policy — it always runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KzgPolicy {
    /// Always re-verify KZG (default production without auth).
    AlwaysVerify,
    /// Skip KZG when `should_skip_kzg` allows (auth + trusted_local).
    /// Inclusion multiproof is still always verified (S-38b-1).
    SkipWhenAuthenticatedTrusted,
}

// ── Inclusion multiproof (S-38b-1: never skip) ──────────────────────────────

/// Inclusion-proof verifier for inject (gossip step 11 / `verify_inclusion_proof`).
///
/// Production always uses real multiproof verification. Tests may install
/// [`AlwaysValidInclusion`] only under explicit test control — never as a
/// production default.
pub trait InclusionVerify: Send + Sync {
    /// Verify depth-4 inclusion of `commitments_root` in `body_root` via `branch`.
    fn verify_inclusion(
        &self,
        commitments_root: &[u8; 32],
        branch: &[Root],
        body_root: [u8; 32],
    ) -> bool;
}

/// Production inclusion: always calls [`verify_inclusion_proof`].
#[derive(Debug, Default, Clone, Copy)]
pub struct ProductionInclusion;

impl InclusionVerify for ProductionInclusion {
    fn verify_inclusion(
        &self,
        commitments_root: &[u8; 32],
        branch: &[Root],
        body_root: [u8; 32],
    ) -> bool {
        verify_inclusion_proof(commitments_root, branch, body_root)
    }
}

/// Test-only: always accepts inclusion (fixtures with zero multiproofs).
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysValidInclusion;

impl InclusionVerify for AlwaysValidInclusion {
    fn verify_inclusion(
        &self,
        _commitments_root: &[u8; 32],
        _branch: &[Root],
        _body_root: [u8; 32],
    ) -> bool {
        true
    }
}

// ── Outcomes / counters ─────────────────────────────────────────────────────

/// Per-sidecar inject outcome (mirrors engine's `InjectOutcome` labels).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InjectOutcome {
    /// Newly accepted into seen + tracker; eligible for publish.
    New,
    /// Already in anti-equivocation seen set (racing gossip, or re-inject).
    Duplicate,
    /// SSZ / structure / KZG rejection.
    Rejected,
}

impl InjectOutcome {
    /// Prometheus-style label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Duplicate => "duplicate",
            Self::Rejected => "rejected",
        }
    }
}

/// Observable counters for inject / publish (p2p-side; engine has its own).
///
/// Label semantics match `cc_engine_sidecars_{injected,published}_total` so
/// tests and operators share one vocabulary across the ninth contract.
#[derive(Debug, Default)]
pub struct InjectCounters {
    /// `outcome=new|duplicate|rejected`.
    pub injected_new: AtomicU64,
    pub injected_duplicate: AtomicU64,
    pub injected_rejected: AtomicU64,
    /// Publish attempts with `subscribed=true|false`.
    pub published_subscribed_true: AtomicU64,
    pub published_subscribed_false: AtomicU64,
    /// Entry-point instrumentation: calls into the gossip sampling entry.
    pub gossip_entry_point_hits: AtomicU64,
}

impl InjectCounters {
    /// Fresh zeros.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read inject counter for `outcome`.
    #[must_use]
    pub fn injected(&self, outcome: InjectOutcome) -> u64 {
        match outcome {
            InjectOutcome::New => self.injected_new.load(Ordering::Relaxed),
            InjectOutcome::Duplicate => self.injected_duplicate.load(Ordering::Relaxed),
            InjectOutcome::Rejected => self.injected_rejected.load(Ordering::Relaxed),
        }
    }

    /// Read publish counter for `subscribed`.
    #[must_use]
    pub fn published(&self, subscribed: bool) -> u64 {
        if subscribed {
            self.published_subscribed_true.load(Ordering::Relaxed)
        } else {
            self.published_subscribed_false.load(Ordering::Relaxed)
        }
    }

    fn bump_injected(&self, outcome: InjectOutcome) {
        match outcome {
            InjectOutcome::New => {
                self.injected_new.fetch_add(1, Ordering::Relaxed);
            }
            InjectOutcome::Duplicate => {
                self.injected_duplicate.fetch_add(1, Ordering::Relaxed);
            }
            InjectOutcome::Rejected => {
                self.injected_rejected.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn bump_published(&self, subscribed: bool) {
        if subscribed {
            self.published_subscribed_true
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.published_subscribed_false
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ── Sampling / publish seams ────────────────────────────────────────────────

/// Named call site into the sampling tracker (gossip entry point).
///
/// Production uses [`SamplingHandle`]; tests may install a mock that records
/// without completing real DA (severed-edge mock tracker).
pub trait SamplingSink: Send + Sync {
    /// Same signature family as gossip's [`crate::gossip::validate::SamplingFeed`],
    /// with an explicit [`ColumnSource`] (engine path uses [`ColumnSource::Engine`]).
    fn on_column(&self, root: [u8; 32], slot: u64, column_index: u64, source: ColumnSource);
}

impl SamplingSink for SamplingHandle {
    fn on_column(&self, root: [u8; 32], slot: u64, column_index: u64, source: ColumnSource) {
        self.lock().on_column(root, slot, column_index, source);
    }
}

/// One recorded sampling sink call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MockSampleCall {
    /// Beacon block root.
    pub root: [u8; 32],
    /// Slot.
    pub slot: u64,
    /// Column index.
    pub column_index: u64,
    /// Source label.
    pub source: ColumnSource,
}

/// Recording sink for severed-edge / unit tests (does not complete DA).
#[derive(Debug, Default)]
pub struct MockSamplingSink {
    /// Recorded column admissions.
    pub calls: Mutex<Vec<MockSampleCall>>,
}

impl SamplingSink for MockSamplingSink {
    fn on_column(&self, root: [u8; 32], slot: u64, column_index: u64, source: ColumnSource) {
        self.calls
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(MockSampleCall {
                root,
                slot,
                column_index,
                source,
            });
    }
}

/// Outward publish of a reconstructed column sidecar.
pub trait ColumnPublisher: Send + Sync {
    /// Publish SSZ bytes on the data-column topic for `column_index`.
    fn publish_column(&self, column_index: u64, ssz: Vec<u8>, topic: String);
}

/// Records publishes without a live swarm (tests).
#[derive(Debug, Default)]
pub struct RecordingPublisher {
    /// `(column_index, topic, ssz_len)`.
    pub published: Mutex<Vec<(u64, String, usize)>>,
}

impl ColumnPublisher for RecordingPublisher {
    fn publish_column(&self, column_index: u64, ssz: Vec<u8>, topic: String) {
        self.published
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((column_index, topic, ssz.len()));
    }
}

/// Channel-backed publisher → swarm publish queue.
#[derive(Debug)]
pub struct ChannelPublisher {
    tx: tokio::sync::mpsc::Sender<PublishRequest>,
}

impl ChannelPublisher {
    /// Wrap a publish queue sender.
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::Sender<PublishRequest>) -> Self {
        Self { tx }
    }
}

impl ColumnPublisher for ChannelPublisher {
    fn publish_column(&self, _column_index: u64, ssz: Vec<u8>, topic: String) {
        let req = PublishRequest { topic, data: ssz };
        // Drop-oldest semantics live on the swarm side; here we best-effort try.
        if self.tx.try_send(req).is_err() {
            warn!("engine inject publish queue full/closed; column publish dropped");
        }
    }
}

/// No-op publisher for minimal host attach (swarm publish path not co-owned yet).
///
/// Inject still updates seen + sampling tracker; gossip republish is deferred
/// until the gRPC task shares `publish_tx` with the swarm ChannelMap.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopPublisher;

impl ColumnPublisher for NoopPublisher {
    fn publish_column(&self, column_index: u64, _ssz: Vec<u8>, topic: String) {
        debug!(
            column_index,
            %topic,
            "engine inject: publish deferred (NoopPublisher — swarm path not co-owned)"
        );
    }
}

// ── Per-sidecar result ──────────────────────────────────────────────────────

/// Outcome of one sidecar in an `InjectColumns` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectSidecarResult {
    /// Column index (if decoded).
    pub column_index: Option<u64>,
    /// Outcome class.
    pub outcome: InjectOutcome,
    /// Whether a gossip publish was issued.
    pub published: bool,
}

// ── Pipeline ────────────────────────────────────────────────────────────────

/// Stateful inject path: seen + sampling + publish + subscription filter.
pub struct InjectPipeline {
    /// Anti-equivocation cache — **the** component where the cache lives (CC-22/5).
    /// Named call site only; no second `seen` under `engine_stream/`.
    seen: Arc<Mutex<SeenSets>>,
    /// Sampling tracker (or mock).
    sampling: Arc<dyn SamplingSink>,
    /// Outward gossip publish.
    publisher: Arc<dyn ColumnPublisher>,
    /// Subscribed column set (publish-iff-subscribed).
    subscription: SubscriptionHandle,
    /// KZG verifier (production: real / fail-closed; tests: AlwaysValid).
    kzg: Arc<dyn KzgVerify>,
    /// Inclusion multiproof verifier — **always** invoked (S-38b-1).
    inclusion: Arc<dyn InclusionVerify>,
    /// Mutual-auth state for KZG skip-reverify gate (S-38a-1).
    ///
    /// Default [`AuthMode::Unauthenticated`]. See type-level footgun docs.
    auth: AuthMode,
    /// KZG application policy (does **not** govern inclusion).
    kzg_policy: KzgPolicy,
    /// Optional p2p metrics (`cc_p2p_columns_received_total{source="engine"}`).
    ///
    /// Pass `None` when the sampling sink is a real [`SamplingHandle`] whose
    /// tracker already owns the counter (avoids double-count).
    metrics: Option<P2pMetrics>,
    /// Inject/publish counters.
    counters: Arc<InjectCounters>,
}

impl std::fmt::Debug for InjectPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InjectPipeline")
            .field("auth", &self.auth)
            .field("kzg_policy", &self.kzg_policy)
            .finish_non_exhaustive()
    }
}

impl InjectPipeline {
    /// Build a production-oriented pipeline.
    ///
    /// - Auth defaults to [`AuthMode::Unauthenticated`] (always re-verify KZG).
    /// - Inclusion defaults to [`ProductionInclusion`] (**always** verified).
    /// - Do **not** call [`Self::with_auth`]`(Authenticated)` without real mutual auth.
    #[must_use]
    pub fn new(
        seen: Arc<Mutex<SeenSets>>,
        sampling: Arc<dyn SamplingSink>,
        publisher: Arc<dyn ColumnPublisher>,
        subscription: SubscriptionHandle,
        kzg: Arc<dyn KzgVerify>,
        metrics: Option<P2pMetrics>,
    ) -> Self {
        Self {
            seen,
            sampling,
            publisher,
            subscription,
            kzg,
            inclusion: Arc::new(ProductionInclusion),
            auth: AuthMode::Unauthenticated,
            kzg_policy: KzgPolicy::SkipWhenAuthenticatedTrusted,
            metrics,
            counters: Arc::new(InjectCounters::new()),
        }
    }

    /// Production helper: real KZG installer + production inclusion + unauthenticated.
    #[must_use]
    pub fn production(
        seen: Arc<Mutex<SeenSets>>,
        sampling: Arc<dyn SamplingSink>,
        publisher: Arc<dyn ColumnPublisher>,
        subscription: SubscriptionHandle,
        metrics: Option<P2pMetrics>,
    ) -> Self {
        Self::new(
            seen,
            sampling,
            publisher,
            subscription,
            production_kzg_verify(),
            metrics,
        )
    }

    /// Test helper: AlwaysValid KZG + AlwaysValid inclusion + unauthenticated.
    ///
    /// Inclusion always-valid is test-only so structural fixtures can exercise
    /// the tracker path; production never uses it.
    #[must_use]
    pub fn for_tests(
        seen: Arc<Mutex<SeenSets>>,
        sampling: Arc<dyn SamplingSink>,
        publisher: Arc<dyn ColumnPublisher>,
        subscription: SubscriptionHandle,
        metrics: Option<P2pMetrics>,
    ) -> Self {
        Self::new(
            seen,
            sampling,
            publisher,
            subscription,
            Arc::new(AlwaysValidKzg),
            metrics,
        )
        .with_inclusion(Arc::new(AlwaysValidInclusion))
    }

    /// Fail-closed KZG (rejects unless KZG skip-reverify applies).
    #[must_use]
    pub fn with_fail_closed_kzg(mut self) -> Self {
        self.kzg = Arc::new(FailClosedKzg);
        self
    }

    /// Override inclusion verifier (tests only should install AlwaysValid).
    #[must_use]
    pub fn with_inclusion(mut self, inclusion: Arc<dyn InclusionVerify>) -> Self {
        self.inclusion = inclusion;
        self
    }

    /// Override auth mode.
    ///
    /// **Footgun:** only use [`AuthMode::Authenticated`] when mutual auth is
    /// real. Software-only flip skips KZG under `trusted_local=true`.
    #[must_use]
    pub fn with_auth(mut self, auth: AuthMode) -> Self {
        self.auth = auth;
        self
    }

    /// Override KZG policy (inclusion remains always-on).
    #[must_use]
    pub fn with_kzg_policy(mut self, policy: KzgPolicy) -> Self {
        self.kzg_policy = policy;
        self
    }

    /// Shared counters (tests / metrics scrape).
    #[must_use]
    pub fn counters(&self) -> Arc<InjectCounters> {
        Arc::clone(&self.counters)
    }

    /// Borrow the shared seen sets (tests assert no second structure elsewhere).
    #[must_use]
    pub fn seen(&self) -> Arc<Mutex<SeenSets>> {
        Arc::clone(&self.seen)
    }

    /// Current subscription snapshot.
    #[must_use]
    pub fn subscription(&self) -> LocalSubscription {
        self.subscription.current()
    }

    /// §5.5 four-step injection for one `InjectColumns` message.
    pub fn inject(&self, msg: &InjectColumns) -> Vec<InjectSidecarResult> {
        let root = root_from_bytes(&msg.beacon_block_root);
        let slot_hint = msg.slot;
        let trusted_local = msg.trusted_local;
        let skip_kzg = match self.kzg_policy {
            KzgPolicy::AlwaysVerify => false,
            KzgPolicy::SkipWhenAuthenticatedTrusted => should_skip_kzg(trusted_local, self.auth),
        };

        let sub = self.subscription.current();
        let mut out = Vec::with_capacity(msg.sidecar_ssz.len());

        for ssz in &msg.sidecar_ssz {
            out.push(self.inject_one(ssz, root, slot_hint, skip_kzg, &sub));
        }
        out
    }

    fn inject_one(
        &self,
        ssz: &[u8],
        root_hint: [u8; 32],
        slot_hint: u64,
        skip_kzg: bool,
        sub: &LocalSubscription,
    ) -> InjectSidecarResult {
        // Decode SSZ — never re-model (CC-3K /7).
        let sidecar = match DataColumnSidecar::<Mainnet>::from_ssz_bytes(ssz) {
            Ok(s) => s,
            Err(_) => {
                self.counters.bump_injected(InjectOutcome::Rejected);
                return InjectSidecarResult {
                    column_index: None,
                    outcome: InjectOutcome::Rejected,
                    published: false,
                };
            }
        };

        let column_index = sidecar.index;
        if column_index >= NUMBER_OF_COLUMNS {
            self.counters.bump_injected(InjectOutcome::Rejected);
            return InjectSidecarResult {
                column_index: Some(column_index),
                outcome: InjectOutcome::Rejected,
                published: false,
            };
        }

        // Structural lengths (gossip step 6 subset).
        let n = sidecar.kzg_commitments.len();
        if n == 0 || sidecar.column.len() != n || sidecar.kzg_proofs.len() != n {
            self.counters.bump_injected(InjectOutcome::Rejected);
            return InjectSidecarResult {
                column_index: Some(column_index),
                outcome: InjectOutcome::Rejected,
                published: false,
            };
        }

        let header = &sidecar.signed_block_header.message;
        let slot = header.slot.as_u64();
        let proposer_index = header.proposer_index.as_u64();
        let block_root = {
            let h = header.tree_hash_root();
            let mut a = [0u8; 32];
            a.copy_from_slice(h.as_slice());
            a
        };
        // Prefer header-derived root; fall back to wire hint if header hashes to zero
        // only in pathological fixtures — still prefer header.
        let root = if block_root != [0u8; 32] {
            block_root
        } else if root_hint != [0u8; 32] {
            root_hint
        } else {
            block_root
        };
        let slot = if slot != 0 { slot } else { slot_hint };

        // ── Inclusion multiproof (S-38b-1): ALWAYS verified, never skipped ──
        // Even when KZG skip is allowed under Authenticated + trusted_local.
        let commitments_root = {
            let h = sidecar.kzg_commitments.tree_hash_root();
            let mut a = [0u8; 32];
            a.copy_from_slice(h.as_slice());
            a
        };
        let body_root = *header.body_root.as_array();
        if !self.inclusion.verify_inclusion(
            &commitments_root,
            sidecar.kzg_commitments_inclusion_proof.as_ref(),
            body_root,
        ) {
            self.counters.bump_injected(InjectOutcome::Rejected);
            debug!(
                column_index,
                "engine inject: inclusion multiproof rejected (never skipped on trusted_local)"
            );
            return InjectSidecarResult {
                column_index: Some(column_index),
                outcome: InjectOutcome::Rejected,
                published: false,
            };
        }

        // ── KZG (S-38a-1): never skip solely on trusted_local ───────────────
        if !skip_kzg
            && !self.kzg.verify_column_kzg(
                column_index,
                sidecar.kzg_commitments.as_ref(),
                sidecar.column.as_ref(),
                sidecar.kzg_proofs.as_ref(),
            )
        {
            self.counters.bump_injected(InjectOutcome::Rejected);
            debug!(
                column_index,
                "engine inject: KZG rejected (trusted_local alone cannot skip)"
            );
            return InjectSidecarResult {
                column_index: Some(column_index),
                outcome: InjectOutcome::Rejected,
                published: false,
            };
        }

        // ── Step 1: anti-equivocation seen (where the cache lives) ──────────
        let seen_key = ColumnSeenKey {
            slot,
            proposer_index,
            column_index,
        };
        {
            let mut seen = self.seen.lock().unwrap_or_else(|p| p.into_inner());
            if seen.columns.contains(&seen_key) {
                self.counters.bump_injected(InjectOutcome::Duplicate);
                return InjectSidecarResult {
                    column_index: Some(column_index),
                    outcome: InjectOutcome::Duplicate,
                    published: false,
                };
            }
            // Insert before tracker so a concurrent gossip path races cleanly.
            let _ = seen.columns.insert(seen_key);
        }

        // ── Step 2: same entry point gossip columns use ─────────────────────
        self.counters
            .gossip_entry_point_hits
            .fetch_add(1, Ordering::Relaxed);
        self.sampling
            .on_column(root, slot, column_index, ColumnSource::Engine);
        // `SamplingTracker::on_column` owns `cc_p2p_columns_received_total` when
        // the real handle is wired. For mock sinks, mirror the counter here so
        // the fourth label is still observable in unit tests.
        if let Some(m) = &self.metrics {
            // Real SamplingHandle already increments; double-count would be
            // wrong. Only increment when the sink is *not* a SamplingHandle —
            // we cannot type-erase that cheaply, so production wires metrics
            // only on the tracker and leaves `metrics: None` here, OR tests
            // that use MockSamplingSink pass metrics. Documented contract:
            // pass metrics iff the sink does not increment columns_received.
            m.inc_columns_received(ColumnSource::Engine);
        }

        self.counters.bump_injected(InjectOutcome::New);

        // ── Step 4: publish iff subscribed (ADR P3-07, p2p half) ────────────
        // (Step 3 DataAvailable is inside the tracker on set equality.)
        let subscribed = sub.is_subscribed(column_index);
        if !subscribed {
            // Engine should never send unsubscribed indices; count and drop.
            self.counters.bump_published(false);
            warn!(
                column_index,
                "engine inject: unsubscribed column not published (filter closed)"
            );
            return InjectSidecarResult {
                column_index: Some(column_index),
                outcome: InjectOutcome::New,
                published: false,
            };
        }

        let subnet = compute_subnet_for_data_column_sidecar(column_index);
        let topic = format!("data_column_sidecar_{subnet}");
        self.publisher
            .publish_column(column_index, ssz.to_vec(), topic);
        self.counters.bump_published(true);

        InjectSidecarResult {
            column_index: Some(column_index),
            outcome: InjectOutcome::New,
            published: true,
        }
    }
}

fn root_from_bytes(b: &[u8]) -> [u8; 32] {
    let mut a = [0u8; 32];
    if b.len() >= 32 {
        a.copy_from_slice(&b[..32]);
    } else {
        a[..b.len()].copy_from_slice(b);
    }
    a
}

/// Encode a minimal test sidecar (structure-valid for inject path).
#[cfg(test)]
#[allow(clippy::expect_used, clippy::field_reassign_with_default)]
pub fn minimal_sidecar_ssz(index: u64, slot: u64, proposer: u64) -> Vec<u8> {
    use cc_types::BeaconBlockHeader;
    use cc_types::containers::SignedBeaconBlockHeader;
    use cc_types::primitives::{
        BlsSignature, Cell, KzgCommitment, KzgProof, Root, Slot, ValidatorIndex,
    };
    use ssz::Encode;
    use ssz_types::{FixedVector, VariableList};

    let mut sc = DataColumnSidecar::<Mainnet>::default();
    sc.index = index;
    sc.signed_block_header = SignedBeaconBlockHeader {
        message: BeaconBlockHeader {
            slot: Slot::new(slot),
            proposer_index: ValidatorIndex::new(proposer),
            parent_root: Root::from_array([1u8; 32]),
            state_root: Root::ZERO,
            body_root: Root::ZERO,
        },
        signature: BlsSignature::default(),
    };
    let commitment = KzgCommitment::default();
    sc.kzg_commitments = VariableList::new(vec![commitment]).expect("1 commitment");
    sc.kzg_proofs = VariableList::new(vec![KzgProof::default()]).expect("1 proof");
    sc.column = VariableList::new(vec![Cell::ZERO]).expect("1 cell");
    sc.kzg_commitments_inclusion_proof = FixedVector::default();
    sc.as_ssz_bytes()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::das::{FixedDeadline, SamplingTracker};
    use crate::metrics::P2pMetrics;
    use cc_proto::p2p::DataAvailable;
    use prometheus_client::registry::Registry;
    use std::collections::BTreeSet;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    fn root(n: u8) -> [u8; 32] {
        let mut r = [0u8; 32];
        r[0] = n;
        r
    }

    fn required_eight() -> BTreeSet<u64> {
        (0..8).collect()
    }

    fn pipeline_with_real_tracker(
        required: BTreeSet<u64>,
        sub_indices: impl IntoIterator<Item = u64>,
        cgc: u64,
    ) -> (
        InjectPipeline,
        mpsc::UnboundedReceiver<DataAvailable>,
        P2pMetrics,
        Arc<RecordingPublisher>,
        Arc<InjectCounters>,
    ) {
        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let (tx, rx) = mpsc::unbounded_channel();
        let far = Instant::now() + Duration::from_secs(3600);
        let tracker = SamplingTracker::new(
            required,
            Arc::new(FixedDeadline(far)),
            Some(metrics.clone()),
            Some(tx),
        );
        let handle = crate::das::SamplingHandle::new(tracker);
        let seen = Arc::new(Mutex::new(SeenSets::new()));
        let publisher = Arc::new(RecordingPublisher::default());
        let sub = SubscriptionHandle::new(LocalSubscription::from_indices(sub_indices, cgc));
        // metrics: None — SamplingHandle increments columns_received itself.
        let pipeline = InjectPipeline::for_tests(
            seen,
            Arc::new(handle),
            Arc::clone(&publisher) as Arc<dyn ColumnPublisher>,
            sub,
            None,
        );
        let counters = pipeline.counters();
        (pipeline, rx, metrics, publisher, counters)
    }

    #[test]
    fn trusted_local_without_auth_does_not_skip_kzg() {
        // SECURITY residual S-38a-1: trusted_local alone is insufficient.
        assert!(
            !should_skip_kzg(true, AuthMode::Unauthenticated),
            "must not skip KZG solely on trusted_local without auth"
        );
        assert!(!should_skip_kzg(false, AuthMode::Unauthenticated));
        assert!(!should_skip_kzg(false, AuthMode::Authenticated));
        assert!(
            should_skip_kzg(true, AuthMode::Authenticated),
            "skip only with auth + trusted_local"
        );
    }

    #[test]
    fn trusted_local_unauthenticated_still_runs_kzg() {
        // FailClosed KZG + unauthenticated + trusted_local=true → Rejected.
        let seen = Arc::new(Mutex::new(SeenSets::new()));
        let sink = Arc::new(MockSamplingSink::default());
        let publisher = Arc::new(RecordingPublisher::default());
        let sub = SubscriptionHandle::new(LocalSubscription::from_indices([0u64], 4));
        let pipeline = InjectPipeline::new(
            seen,
            Arc::clone(&sink) as Arc<dyn SamplingSink>,
            Arc::clone(&publisher) as Arc<dyn ColumnPublisher>,
            sub,
            Arc::new(FailClosedKzg),
            None,
        )
        .with_auth(AuthMode::Unauthenticated)
        .with_kzg_policy(KzgPolicy::SkipWhenAuthenticatedTrusted);

        let msg = InjectColumns {
            beacon_block_root: root(1).to_vec(),
            slot: 10,
            sidecar_ssz: vec![minimal_sidecar_ssz(0, 10, 1)],
            trusted_local: true, // wire claims trust — MUST NOT skip without auth
        };
        let results = pipeline.inject(&msg);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, InjectOutcome::Rejected);
        assert!(
            sink.calls.lock().unwrap().is_empty(),
            "rejected columns must not enter the tracker"
        );
    }

    #[test]
    fn injected_column_uses_gossip_entry_point() {
        let (pipeline, _rx, metrics, _pub, counters) =
            pipeline_with_real_tracker(required_eight(), 0..8, 4);
        let msg = InjectColumns {
            beacon_block_root: root(2).to_vec(),
            slot: 20,
            sidecar_ssz: vec![minimal_sidecar_ssz(0, 20, 3)],
            trusted_local: true,
        };
        let results = pipeline.inject(&msg);
        assert_eq!(results[0].outcome, InjectOutcome::New);
        assert_eq!(
            counters.gossip_entry_point_hits.load(Ordering::Relaxed),
            1,
            "must hit the gossip sampling entry point"
        );
        assert_eq!(
            metrics.columns_received(ColumnSource::Engine),
            1,
            "cc_p2p_columns_received_total{{source=\"engine\"}} += 1"
        );
        assert_eq!(
            metrics.columns_received(ColumnSource::Gossip),
            0,
            "gossip label must not move on inject"
        );
    }

    #[test]
    fn injection_updates_seen_set() {
        let (pipeline, _rx, metrics, _pub, counters) =
            pipeline_with_real_tracker(required_eight(), 0..8, 4);
        let ssz = minimal_sidecar_ssz(1, 30, 7);
        let msg = InjectColumns {
            beacon_block_root: root(3).to_vec(),
            slot: 30,
            sidecar_ssz: vec![ssz.clone()],
            trusted_local: true,
        };
        assert_eq!(pipeline.inject(&msg)[0].outcome, InjectOutcome::New);
        // Subsequent inject of the same column → duplicate (seen set).
        assert_eq!(pipeline.inject(&msg)[0].outcome, InjectOutcome::Duplicate);
        assert_eq!(counters.injected(InjectOutcome::Duplicate), 1);
        // Tracker only saw one Engine receipt (duplicate did not re-insert).
        assert_eq!(metrics.columns_received(ColumnSource::Engine), 1);
        // No second seen structure under engine_stream/ — asserted by grep in
        // acceptance; here we confirm the shared SeenSets has the key.
        let seen = pipeline.seen();
        let g = seen.lock().unwrap();
        assert!(g.columns.contains(&ColumnSeenKey {
            slot: 30,
            proposer_index: 7,
            column_index: 1,
        }));
    }

    #[test]
    fn publish_iff_subscribed_p2p_side() {
        // Subscribed 0..4 only; inject 0 and 5.
        let (pipeline, _rx, _m, publisher, counters) =
            pipeline_with_real_tracker(required_eight(), 0..4u64, 4);
        let msg = InjectColumns {
            beacon_block_root: root(4).to_vec(),
            slot: 40,
            sidecar_ssz: vec![minimal_sidecar_ssz(0, 40, 1), minimal_sidecar_ssz(5, 40, 1)],
            trusted_local: true,
        };
        let results = pipeline.inject(&msg);
        assert!(results[0].published, "subscribed index publishes");
        assert!(!results[1].published, "unsubscribed index does not publish");
        let pubs = publisher.published.lock().unwrap();
        assert_eq!(pubs.len(), 1);
        assert_eq!(pubs[0].0, 0);
        assert_eq!(
            counters.published(false),
            1,
            "unsubscribed attempt recorded"
        );
        // Production engine already filters: subscribed=false on the *engine*
        // side stays 0. P2p records the defensive drop; AC asserts the
        // engine-side series at 0. Here we assert p2p never publishes false.
        assert_eq!(pubs.iter().filter(|p| p.0 >= 4).count(), 0);
    }

    #[test]
    fn fastpath_to_data_available() {
        let (pipeline, mut rx, metrics, _pub, _c) =
            pipeline_with_real_tracker(required_eight(), 0..8, 4);
        // Use a fixed root by setting body/state so tree_hash is stable — we
        // feed on_block first so the task exists, then inject all 8 columns.
        // For inject we use the header-derived root; call on_block with that.
        let ssz0 = minimal_sidecar_ssz(0, 50, 2);
        let sc0 = DataColumnSidecar::<Mainnet>::from_ssz_bytes(&ssz0).unwrap();
        let block_root = {
            let h = sc0.signed_block_header.message.tree_hash_root();
            let mut a = [0u8; 32];
            a.copy_from_slice(h.as_slice());
            a
        };

        // Seed the tracker with the block (non-zero commitment count).
        // Access via SamplingSink is column-only; re-obtain handle through
        // a direct tracker path: inject alone creates the task on first column.
        let mut sidecar_ssz = Vec::with_capacity(8);
        for col in 0..8u64 {
            sidecar_ssz.push(minimal_sidecar_ssz(col, 50, 2));
        }
        let msg = InjectColumns {
            beacon_block_root: block_root.to_vec(),
            slot: 50,
            sidecar_ssz,
            trusted_local: true,
        };
        let results = pipeline.inject(&msg);
        assert!(results.iter().all(|r| r.outcome == InjectOutcome::New));
        assert_eq!(metrics.columns_received(ColumnSource::Engine), 8);

        let da = rx
            .try_recv()
            .expect("DataAvailable after full sample set via engine inject");
        assert_eq!(da.root, block_root.to_vec());
        assert_eq!(da.slot, 50);
    }

    #[test]
    fn tracker_completion_is_btreeset_equality() {
        // Re-assert CC-24c: 7-of-8 with one duplicate does NOT complete.
        let (pipeline, mut rx, _m, _p, _c) = pipeline_with_real_tracker(required_eight(), 0..8, 4);
        let mut sidecar_ssz = Vec::new();
        for col in 0..7u64 {
            sidecar_ssz.push(minimal_sidecar_ssz(col, 60, 1));
        }
        // Duplicate of column 0 instead of column 7.
        sidecar_ssz.push(minimal_sidecar_ssz(0, 60, 1));
        let msg = InjectColumns {
            beacon_block_root: root(6).to_vec(),
            slot: 60,
            sidecar_ssz,
            trusted_local: true,
        };
        let results = pipeline.inject(&msg);
        let news = results
            .iter()
            .filter(|r| r.outcome == InjectOutcome::New)
            .count();
        let dups = results
            .iter()
            .filter(|r| r.outcome == InjectOutcome::Duplicate)
            .count();
        assert_eq!(news, 7);
        assert_eq!(dups, 1);
        assert!(
            rx.try_recv().is_err(),
            "7-of-8 with a duplicate must not emit DataAvailable (BTreeSet equality, never a counter)"
        );
    }

    #[test]
    fn severed_engine_edge_mock_tracker() {
        // Prove the edge is load-bearing even when the tracker is faked (D-13).
        // Fast path "succeeds" (inject accepts) but mock sink does not complete DA.
        let seen = Arc::new(Mutex::new(SeenSets::new()));
        let sink = Arc::new(MockSamplingSink::default());
        let publisher = Arc::new(RecordingPublisher::default());
        let sub = SubscriptionHandle::new(LocalSubscription::from_indices(0..8u64, 4));
        let pipeline = InjectPipeline::for_tests(
            seen,
            Arc::clone(&sink) as Arc<dyn SamplingSink>,
            Arc::clone(&publisher) as Arc<dyn ColumnPublisher>,
            sub,
            None,
        );
        let mut sidecar_ssz = Vec::new();
        for col in 0..8u64 {
            sidecar_ssz.push(minimal_sidecar_ssz(col, 70, 1));
        }
        let msg = InjectColumns {
            beacon_block_root: root(7).to_vec(),
            slot: 70,
            sidecar_ssz,
            trusted_local: true,
        };
        let results = pipeline.inject(&msg);
        assert!(
            results.iter().all(|r| r.outcome == InjectOutcome::New),
            "fast path inject succeeds"
        );
        assert_eq!(sink.calls.lock().unwrap().len(), 8);
        // Mock tracker never emits DataAvailable — edge to real tracker is
        // what makes DA true. Severing it leaves the block deferred.
        // (No DA channel here by construction.)
    }

    #[test]
    fn severed_engine_edge_leaves_block_deferred() {
        // Job: prove the engine→p2p edge is **load-bearing**, so a refactor
        // that removes inject→tracker while still returning New fails here
        // rather than on Hoodi (R-8).
        //
        // Setup: a *live* sampling tracker with a DA channel, and a *separate*
        // inject pipeline whose sampling sink is a MockSamplingSink (edge
        // severed — inject does not call the real tracker). Fast path runs to
        // success (all New); real tracker never receives columns → no
        // DataAvailable → DA stays deferred.
        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let (tx, mut rx) = mpsc::unbounded_channel::<DataAvailable>();
        let far = Instant::now() + Duration::from_secs(3600);
        let tracker = SamplingTracker::new(
            required_eight(),
            Arc::new(FixedDeadline(far)),
            Some(metrics),
            Some(tx),
        );
        let real_handle = crate::das::SamplingHandle::new(tracker);

        // Severed edge: inject uses mock sink, not `real_handle`.
        let mock_sink = Arc::new(MockSamplingSink::default());
        let seen = Arc::new(Mutex::new(SeenSets::new()));
        let publisher = Arc::new(RecordingPublisher::default());
        let sub = SubscriptionHandle::new(LocalSubscription::from_indices(0..8u64, 4));
        let pipeline = InjectPipeline::for_tests(
            seen,
            Arc::clone(&mock_sink) as Arc<dyn SamplingSink>,
            Arc::clone(&publisher) as Arc<dyn ColumnPublisher>,
            sub,
            None,
        );

        let mut sidecar_ssz = Vec::with_capacity(8);
        for col in 0..8u64 {
            sidecar_ssz.push(minimal_sidecar_ssz(col, 80, 1));
        }
        let msg = InjectColumns {
            beacon_block_root: root(9).to_vec(),
            slot: 80,
            sidecar_ssz,
            trusted_local: true,
        };
        let results = pipeline.inject(&msg);
        assert!(
            results.iter().all(|r| r.outcome == InjectOutcome::New),
            "fast path inject succeeds with edge severed"
        );
        assert_eq!(
            mock_sink.calls.lock().unwrap().len(),
            8,
            "mock sink saw all columns (inject path ran)"
        );
        // Real tracker was never fed → no DataAvailable.
        assert!(
            rx.try_recv().is_err(),
            "severed edge: no DataAvailable; is_data_available stays false"
        );
        let guard = real_handle.lock();
        assert!(
            guard.get(&root(9)).is_none(),
            "real tracker has no task — inject did not cross the edge"
        );
        // Fork-choice head does not advance past a deferred block — that check
        // lives on chain/FC; here we prove the DA signal never fired despite
        // inject success.
    }

    #[test]
    fn inclusion_always_verified_even_when_kzg_would_skip() {
        // S-38b-1: Authenticated + trusted_local would skip KZG, but inclusion
        // multiproof is still always verified. Production inclusion rejects
        // the zero multiproof fixture.
        let seen = Arc::new(Mutex::new(SeenSets::new()));
        let sink = Arc::new(MockSamplingSink::default());
        let publisher = Arc::new(RecordingPublisher::default());
        let sub = SubscriptionHandle::new(LocalSubscription::from_indices([0u64], 4));
        let pipeline = InjectPipeline::new(
            seen,
            Arc::clone(&sink) as Arc<dyn SamplingSink>,
            Arc::clone(&publisher) as Arc<dyn ColumnPublisher>,
            sub,
            Arc::new(FailClosedKzg), // would reject KZG if it ran
            None,
        )
        .with_auth(AuthMode::Authenticated)
        .with_kzg_policy(KzgPolicy::SkipWhenAuthenticatedTrusted)
        .with_inclusion(Arc::new(ProductionInclusion)); // real multiproof

        let msg = InjectColumns {
            beacon_block_root: root(11).to_vec(),
            slot: 11,
            sidecar_ssz: vec![minimal_sidecar_ssz(0, 11, 1)],
            trusted_local: true, // KZG skip allowed under auth
        };
        let results = pipeline.inject(&msg);
        assert_eq!(
            results[0].outcome,
            InjectOutcome::Rejected,
            "zero multiproof must fail inclusion even when KZG skip is allowed"
        );
        assert!(
            sink.calls.lock().unwrap().is_empty(),
            "rejected columns must not enter the tracker"
        );
    }

    #[test]
    fn fourth_label_value_engine_exists() {
        assert_eq!(ColumnSource::ALL.len(), 4);
        let labels: BTreeSet<&str> = ColumnSource::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            labels,
            BTreeSet::from(["gossip", "byroot", "byrange", "engine"])
        );
    }

    #[test]
    fn cc24d_timeout_ordering_re_run_unchanged() {
        // CC-38 /7: re-run CC-24d's timeout-ordering check unchanged.
        // Single-variable because ADR P3-05 put engine requeue in a separate
        // map (pending_engine 64/8) vs pending_da (64/4).
        use crate::das::recovery::DEFAULT_SECONDS_PER_SLOT;
        use crate::das::{
            CHAIN_PENDING_DA_TIMEOUT_SLOTS, RECOVERY_MAX_ATTEMPTS, RECOVERY_MAX_PEERS,
            RECOVERY_RESP_SECS, RECOVERY_TTFB_SECS, default_ladder_under_chain_timeout,
            recovery_ladder_worst_case_secs,
        };
        assert!(
            default_ladder_under_chain_timeout(),
            "4×12s must exceed 3×(5+10)s — pending_da timeout still outlasts recovery"
        );
        assert_eq!(
            recovery_ladder_worst_case_secs(
                RECOVERY_MAX_ATTEMPTS,
                RECOVERY_MAX_PEERS,
                RECOVERY_TTFB_SECS,
                RECOVERY_RESP_SECS
            ),
            45
        );
        assert_eq!(
            CHAIN_PENDING_DA_TIMEOUT_SLOTS * DEFAULT_SECONDS_PER_SLOT,
            48
        );
    }
}
