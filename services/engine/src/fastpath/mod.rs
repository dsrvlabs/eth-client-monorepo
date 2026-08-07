//! Fast-path lane: triggers, single-flight, worker, cells / transpose / filter
//! (CC-37a + CC-37b / Architecture §2.2, §5.1, §5.3, §5.4).
//!
//! # Two triggers, two owners
//!
//! | Branch | Owner | Call site (when wired) |
//! |---|---|---|
//! | valid `beacon_block` | **chain** | `services/chain/src/da.rs` → [`FastpathLane::trigger_from_block`] |
//! | `data_column_sidecar` | **p2p** | reverse direction of the ninth contract (CC-38) → [`FastpathLane::trigger_from_column_sidecar`] |
//!
//! Both collapse to one `getBlobsV2` via single-flight on `beacon_block_root`.
//!
//! # CC-37b pipeline (live on the worker)
//!
//! On `getBlobsV2` Complete the worker runs:
//! [`cells`] (bind + `compute_cells` on `spawn_blocking` + EL proof zip) →
//! [`sidecars`] (128-way transpose) → [`filter`] (subscribe-only, before any
//! process boundary). Inject of filtered sidecars is **CC-38**.
//!
//! # Out of scope
//!
//! - Ninth contract stream / inject → **CC-38a/b**
//! - p2p stream client is **stubbed** here: column triggers are accepted on the
//!   same [`FastpathLane`] API with [`TriggerOwner::P2pColumn`]; CC-38 wires the
//!   gRPC edge.

pub mod cells;
pub mod fetch;
pub mod filter;
pub mod sidecars;

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use cc_crypto::CellKzg;
use cc_types::primitives::{KzgCommitment, Root};
use tokio::sync::{Mutex, Notify, mpsc};

use crate::methods::get_blobs::{
    GetBlobsOutcome, NullContext, versioned_hashes_from_commitments,
};
use crate::metrics::EngineMetrics;
use crate::transport::SharedTransport;

use self::cells::compute_cells_zipped_with_el_proofs;
use self::fetch::{
    BlobBound, FetchRequest, FetchResult, SamplingTrackerProbe, assert_request_length_within_bound,
    epoch_at_slot, fetch_blobs,
};
use self::filter::{SubscriptionSet, filter_subscribed};
use self::sidecars::{SidecarTemplate, transpose_to_sidecars};
use cc_types::preset::Mainnet;
use cc_types::sidecar::DataColumnSidecar;
use ssz::Encode;

/// Bound on the trigger queue (Architecture §2.3).
pub const FASTPATH_QUEUE_BOUND: usize = 32;

/// One block's filtered sidecars ready for `InjectColumns` (CC-38a).
///
/// Lives here (not in `inject`) so the fastpath worker can emit without a
/// circular module edge; the stream client consumes these on `inject_tx`.
#[derive(Debug, Clone)]
pub struct InjectItem {
    pub beacon_block_root: [u8; 32],
    pub slot: u64,
    /// SSZ-encoded `DataColumnSidecar`s (subscribed only).
    pub sidecar_ssz: Vec<Vec<u8>>,
}

impl InjectItem {
    /// Build from assembled + filtered sidecars (CC-37b → CC-38).
    #[must_use]
    pub fn from_sidecars(
        beacon_block_root: [u8; 32],
        slot: u64,
        sidecars: &[DataColumnSidecar<Mainnet>],
    ) -> Self {
        let sidecar_ssz = sidecars.iter().map(|s| s.as_ssz_bytes()).collect();
        Self {
            beacon_block_root,
            slot,
            sidecar_ssz,
        }
    }
}

/// Ring bound for the in-memory completion log (tests / local observers).
///
/// Prevents unbounded growth once the lane is process-global (CC-37a review F3).
pub const COMPLETED_LOG_BOUND: usize = 32;

/// Hoodi-like blob schedule for tests and local wiring.
///
/// Entry epochs/maxima mirror the committed Hoodi `BLOB_SCHEDULE` (CC-1G). Kept
/// out of `fetch.rs` so that file stays free of blob-count literals (CC-37a AC).
#[must_use]
#[allow(clippy::expect_used)] // static fixture table; validation failure is a test bug
pub fn hoodi_blob_bound() -> BlobBound {
    use cc_types::config::{BlobParameters, BlobSchedule};
    use cc_types::primitives::Epoch;
    let entries = vec![
        BlobParameters {
            epoch: Epoch::new(52_480),
            max_blobs_per_block: 15,
        },
        BlobParameters {
            epoch: Epoch::new(54_016),
            max_blobs_per_block: 21,
        },
    ];
    let schedule = BlobSchedule::try_from_entries(entries).expect("hoodi fixture schedule");
    BlobBound::new(schedule, Epoch::new(2_048))
}

/// Completion notification payload for the fastpath worker (tests / observers).
type FastpathCompletion = ([u8; 32], FetchResult);

/// Who produced a fetch trigger (ownership is structural, not just a label).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerOwner {
    /// Chain is authoritative for `beacon_block` validity (Phase 2 ADR-P2-04).
    /// Production call site: `services/chain/src/da.rs`.
    ChainBlock,
    /// p2p alone sees `data_column_sidecar` bytes. Production path travels down
    /// the reverse direction of the ninth contract (CC-38); until then tests
    /// and stubs call [`FastpathLane::trigger_from_column_sidecar`] directly.
    P2pColumn,
}

/// Enqueued work item (pre single-flight).
#[derive(Debug, Clone)]
pub struct Trigger {
    pub beacon_block_root: [u8; 32],
    pub slot: u64,
    pub versioned_hashes: Vec<[u8; 32]>,
    /// Block/gossip template: commitments + inclusion proof + header.
    /// Inclusion proof comes from the block (or gossiped column), never the EL.
    pub template: SidecarTemplate,
    pub owner: TriggerOwner,
    pub null_ctx: NullContext,
}

/// Build a [`SidecarTemplate`] from raw 48-byte commitments (zero inclusion until
/// chain/p2p supplies the real depth-4 branch — required for live reconstruction
/// bind against template commitments).
#[must_use]
pub fn template_from_commitments(blob_kzg_commitments: &[[u8; 48]]) -> SidecarTemplate {
    let kzg_commitments: Vec<KzgCommitment> = blob_kzg_commitments
        .iter()
        .map(|c| KzgCommitment::from_array(*c))
        .collect();
    SidecarTemplate::new(
        cc_types::containers::SignedBeaconBlockHeader::default(),
        kzg_commitments,
        [Root::default(); 4],
    )
}

/// Outcome of attempting to enqueue a trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// Accepted into the queue (will fetch, subject to worker).
    ///
    /// Also returned when the queue was full and **drop-oldest** evicted a prior
    /// entry to make room — the **caller's** trigger is kept (Architecture §2.3).
    /// Evictions are observed only via `cc_engine_fastpath_dropped_total`.
    Enqueued,
    /// Root already in flight or already queued — dropped + counted.
    DroppedSingleFlight,
    /// Zero commitments — no query issued (by design).
    SkippedNoCommitments,
    /// Hash count exceeds runtime `max_blobs_per_block` / EL ceiling — no query.
    SkippedExceedsBound,
}

/// Shared fast-path lane handle.
///
/// Clone is cheap (Arc). Triggers are fire-and-forget from the caller's
/// perspective: the worker owns the EL call off the state-transition thread.
#[derive(Debug, Clone)]
pub struct FastpathLane {
    inner: Arc<FastpathInner>,
}

struct FastpathInner {
    queue: Mutex<VecDeque<Trigger>>,
    /// Roots currently queued or in-flight (single-flight set).
    inflight: Mutex<HashSet<[u8; 32]>>,
    notify: Notify,
    transport: SharedTransport,
    metrics: Option<EngineMetrics>,
    bound: BlobBound,
    /// Optional sampling-tracker probe (tests; production is CC-38).
    tracker: Option<Arc<dyn SamplingTrackerProbe>>,
    /// Cell-KZG backend for CC-37b reconstruction. `None` = fetch-only (tests).
    kzg: Option<Arc<dyn CellKzg>>,
    /// Subscribe-only publish set (custody-sampled indices from p2p / config).
    /// Never defaulted to `0..8` — empty until the operator or CC-38 sets it.
    subscription: Mutex<SubscriptionSet>,
    /// Shutdown flag.
    closed: Mutex<bool>,
    /// Test hook: completed fetch results (bounded).
    completed: Mutex<Vec<FastpathCompletion>>,
    completed_tx: Mutex<Option<mpsc::UnboundedSender<FastpathCompletion>>>,
    /// Ninth-contract inject channel (CC-38a). `None` until the stream client
    /// is wired; Assembled results are dropped rather than queued unboundedly.
    inject_tx: Mutex<Option<mpsc::Sender<InjectItem>>>,
}

impl std::fmt::Debug for FastpathInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastpathInner").finish_non_exhaustive()
    }
}

impl FastpathLane {
    /// Construct a lane. Spawn the worker with [`Self::spawn_worker`].
    ///
    /// - `kzg`: when `Some`, Complete fetches run bind + cells + transpose + filter.
    /// - `subscription`: column indices to publish (custody-sampled). Use
    ///   [`SubscriptionSet::empty`] until p2p supplies the real set — **never**
    ///   invent `0..8`.
    #[must_use]
    pub fn new(
        transport: SharedTransport,
        metrics: Option<EngineMetrics>,
        bound: BlobBound,
        tracker: Option<Arc<dyn SamplingTrackerProbe>>,
        kzg: Option<Arc<dyn CellKzg>>,
        subscription: SubscriptionSet,
    ) -> Self {
        Self {
            inner: Arc::new(FastpathInner {
                queue: Mutex::new(VecDeque::with_capacity(FASTPATH_QUEUE_BOUND)),
                inflight: Mutex::new(HashSet::new()),
                notify: Notify::new(),
                transport,
                metrics,
                bound,
                tracker,
                kzg,
                subscription: Mutex::new(subscription),
                closed: Mutex::new(false),
                completed: Mutex::new(Vec::new()),
                completed_tx: Mutex::new(None),
                inject_tx: Mutex::new(None),
            }),
        }
    }

    /// Attach the ninth-contract inject channel (worker → stream client).
    pub async fn set_inject_tx(&self, tx: mpsc::Sender<InjectItem>) {
        *self.inner.inject_tx.lock().await = Some(tx);
    }

    /// Replace the subscribe-only set (CC-38 / config). Read before every
    /// outbound filter — never invent indices here.
    pub async fn set_subscription(&self, subscription: SubscriptionSet) {
        *self.inner.subscription.lock().await = subscription;
    }

    /// Snapshot of the current subscription set.
    pub async fn subscription(&self) -> SubscriptionSet {
        self.inner.subscription.lock().await.clone()
    }

    /// Subscribe to fetch completions (tests).
    pub async fn subscribe_completions(&self) -> mpsc::UnboundedReceiver<FastpathCompletion> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.inner.completed_tx.lock().await = Some(tx);
        rx
    }

    /// Snapshot of completed results (tests).
    pub async fn completed(&self) -> Vec<FastpathCompletion> {
        self.inner.completed.lock().await.clone()
    }

    /// Spawn the background worker (one task). Returns a join handle.
    pub fn spawn_worker(&self) -> tokio::task::JoinHandle<()> {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move { worker_loop(inner).await })
    }

    /// **Chain-owned** trigger: valid `beacon_block` with `blob_kzg_commitments`.
    ///
    /// Versioned hashes are derived from the commitments. A zero-commitment
    /// block issues **no** request.
    pub async fn trigger_from_block(
        &self,
        beacon_block_root: [u8; 32],
        slot: u64,
        blob_kzg_commitments: &[[u8; 48]],
        null_ctx: NullContext,
    ) -> EnqueueOutcome {
        self.trigger_from_block_with_template(
            beacon_block_root,
            slot,
            template_from_commitments(blob_kzg_commitments),
            null_ctx,
        )
        .await
    }

    /// Chain-owned trigger with a full [`SidecarTemplate`] (inclusion proof from
    /// the block body — preferred production entry once chain wires it).
    pub async fn trigger_from_block_with_template(
        &self,
        beacon_block_root: [u8; 32],
        slot: u64,
        template: SidecarTemplate,
        null_ctx: NullContext,
    ) -> EnqueueOutcome {
        if template.kzg_commitments.is_empty() {
            return EnqueueOutcome::SkippedNoCommitments;
        }
        let raw: Vec<[u8; 48]> = template
            .kzg_commitments
            .iter()
            .map(|c| *c.as_array())
            .collect();
        let versioned_hashes = versioned_hashes_from_commitments(&raw);
        self.enqueue(Trigger {
            beacon_block_root,
            slot,
            versioned_hashes,
            template,
            owner: TriggerOwner::ChainBlock,
            null_ctx,
        })
        .await
    }

    /// **P2P-owned** trigger: `data_column_sidecar` from gossip.
    ///
    /// Rebuilds versioned hashes from the sidecars's `kzg_commitments` — the
    /// inputs `get_data_column_sidecars_from_column_sidecar` needs — **without
    /// any beacon block present** (Architecture §5.1).
    ///
    /// Ninth-contract stream injection of this trigger is **CC-38**; this
    /// method is the engine-side admission point the stream will call.
    pub async fn trigger_from_column_sidecar(
        &self,
        beacon_block_root: [u8; 32],
        slot: u64,
        kzg_commitments: &[[u8; 48]],
        null_ctx: NullContext,
    ) -> EnqueueOutcome {
        self.trigger_from_column_with_template(
            beacon_block_root,
            slot,
            template_from_commitments(kzg_commitments),
            null_ctx,
        )
        .await
    }

    /// P2P-owned trigger with template fields from a gossiped column sidecar.
    pub async fn trigger_from_column_with_template(
        &self,
        beacon_block_root: [u8; 32],
        slot: u64,
        template: SidecarTemplate,
        null_ctx: NullContext,
    ) -> EnqueueOutcome {
        if template.kzg_commitments.is_empty() {
            return EnqueueOutcome::SkippedNoCommitments;
        }
        let raw: Vec<[u8; 48]> = template
            .kzg_commitments
            .iter()
            .map(|c| *c.as_array())
            .collect();
        let versioned_hashes = versioned_hashes_from_commitments(&raw);
        self.enqueue(Trigger {
            beacon_block_root,
            slot,
            versioned_hashes,
            template,
            owner: TriggerOwner::P2pColumn,
            null_ctx,
        })
        .await
    }

    /// Enqueue with bound gate + single-flight + drop-oldest overflow.
    ///
    /// The CC-1G hash-count gate runs **here** (admission), not only at fetch, so
    /// an oversized trigger never occupies the single-flight slot or queue.
    pub async fn enqueue(&self, trigger: Trigger) -> EnqueueOutcome {
        if trigger.versioned_hashes.is_empty() {
            return EnqueueOutcome::SkippedNoCommitments;
        }
        let epoch = epoch_at_slot(trigger.slot);
        if assert_request_length_within_bound(
            &trigger.versioned_hashes,
            &self.inner.bound,
            epoch,
        )
        .is_err()
        {
            tracing::warn!(
                n = trigger.versioned_hashes.len(),
                slot = trigger.slot,
                epoch = epoch.as_u64(),
                "fastpath trigger rejected: versioned_hashes exceed runtime max_blobs_per_block"
            );
            return EnqueueOutcome::SkippedExceedsBound;
        }

        let mut inflight = self.inner.inflight.lock().await;
        if inflight.contains(&trigger.beacon_block_root) {
            if let Some(m) = &self.inner.metrics {
                m.fastpath_dropped.inc();
            }
            return EnqueueOutcome::DroppedSingleFlight;
        }

        let mut queue = self.inner.queue.lock().await;
        // Drop-oldest when at capacity (Architecture §2.3): newest is valuable.
        // The caller's trigger is enqueued; return Enqueued (not "dropped") so
        // callers do not mis-handle a successful admission (CC-37a review F2).
        while queue.len() >= FASTPATH_QUEUE_BOUND {
            if let Some(old) = queue.pop_front() {
                inflight.remove(&old.beacon_block_root);
                if let Some(m) = &self.inner.metrics {
                    m.fastpath_dropped.inc();
                }
            } else {
                break;
            }
        }
        inflight.insert(trigger.beacon_block_root);
        queue.push_back(trigger);
        drop(queue);
        drop(inflight);
        self.inner.notify.notify_one();
        EnqueueOutcome::Enqueued
    }

    /// Graceful close (tests).
    pub async fn close(&self) {
        *self.inner.closed.lock().await = true;
        self.inner.notify.notify_waiters();
    }
}

/// Live CC-37b composition: bind → cells → transpose → filter.
///
/// Called from the worker on every `getBlobsV2` Complete when a KZG backend is
/// configured. Filtered sidecars are returned; inject is CC-38.
pub async fn reconstruct_and_filter(
    kzg: Arc<dyn CellKzg>,
    blobs: Vec<crate::methods::get_blobs::BlobAndProofV2>,
    versioned_hashes: Vec<[u8; 32]>,
    template: &SidecarTemplate,
    subscription: &SubscriptionSet,
    metrics: Option<&EngineMetrics>,
) -> Result<crate::fastpath::filter::FilterOutcome, String> {
    let n_blobs = blobs.len();
    let material = compute_cells_zipped_with_el_proofs(
        kzg,
        blobs,
        versioned_hashes,
        template.kzg_commitments.clone(),
        metrics,
    )
    .await
    .map_err(|e| e.to_string())?;
    let assembled = transpose_to_sidecars(&material, template, metrics).map_err(|e| e.to_string())?;
    debug_assert_eq!(assembled.len(), 128);
    let _ = n_blobs;
    // Filter reads subscription **before** outbound construction (ADR P3-07).
    Ok(filter_subscribed(assembled, subscription, metrics))
}

async fn worker_loop(inner: Arc<FastpathInner>) {
    loop {
        if *inner.closed.lock().await {
            break;
        }
        let trigger = {
            let mut q = inner.queue.lock().await;
            q.pop_front()
        };
        let Some(trigger) = trigger else {
            inner.notify.notified().await;
            continue;
        };

        let request = FetchRequest {
            beacon_block_root: trigger.beacon_block_root,
            slot: trigger.slot,
            versioned_hashes: trigger.versioned_hashes.clone(),
            null_ctx: trigger.null_ctx,
        };
        let tracker = inner.tracker.as_deref();
        let wire = fetch_blobs(
            inner.transport.as_ref(),
            inner.metrics.as_ref(),
            &inner.bound,
            tracker,
            &request,
        )
        .await;

        // CC-37b: compose reconstruction on the live Complete path when KZG is
        // configured. Inject of published sidecars remains CC-38.
        let result = match wire {
            FetchResult::Complete(GetBlobsOutcome::Complete(blobs)) => {
                if let Some(kzg) = inner.kzg.clone() {
                    let subscription = inner.subscription.lock().await.clone();
                    let n_blobs = blobs.len();
                    match reconstruct_and_filter(
                        kzg,
                        blobs,
                        trigger.versioned_hashes.clone(),
                        &trigger.template,
                        &subscription,
                        inner.metrics.as_ref(),
                    )
                    .await
                    {
                        Ok(outcome) => {
                            // CC-38a: push filtered sidecars to the inject stream.
                            // Drop when the channel is full / absent — never block
                            // the worker on p2p (Architecture §2.3 inject_tx).
                            if !outcome.published.is_empty() {
                                let item = InjectItem::from_sidecars(
                                    trigger.beacon_block_root,
                                    trigger.slot,
                                    &outcome.published,
                                );
                                if let Some(tx) = inner.inject_tx.lock().await.as_ref() {
                                    match tx.try_send(item) {
                                        Ok(()) => {
                                            if let Some(m) = &inner.metrics {
                                                use crate::metrics::{
                                                    InjectOutcome, InjectOutcomeLabels,
                                                };
                                                for _ in 0..outcome.published.len() {
                                                    m.sidecars_injected
                                                        .get_or_create(&InjectOutcomeLabels {
                                                            outcome: InjectOutcome::New
                                                                .as_str()
                                                                .to_owned(),
                                                        })
                                                        .inc();
                                                }
                                            }
                                        }
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            tracing::debug!(
                                                root = ?trigger.beacon_block_root,
                                                "inject_tx full; dropping assembled sidecars"
                                            );
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            tracing::debug!(
                                                root = ?trigger.beacon_block_root,
                                                "inject_tx closed; dropping assembled sidecars"
                                            );
                                        }
                                    }
                                }
                            }
                            FetchResult::Assembled {
                                n_blobs,
                                published: outcome.published,
                                dropped: outcome.dropped,
                                published_bytes: outcome.published_bytes,
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                root = ?trigger.beacon_block_root,
                                "fastpath reconstruction failed"
                            );
                            FetchResult::Error(e)
                        }
                    }
                } else {
                    // Fetch-only mode (tests without KZG): wire outcome only.
                    FetchResult::Complete(GetBlobsOutcome::Complete(blobs))
                }
            }
            other => other,
        };

        // Release single-flight slot.
        {
            let mut inflight = inner.inflight.lock().await;
            inflight.remove(&trigger.beacon_block_root);
        }

        let completion = (trigger.beacon_block_root, result);
        {
            // Ring buffer — drop-oldest once past COMPLETED_LOG_BOUND (F3).
            let mut completed = inner.completed.lock().await;
            if completed.len() >= COMPLETED_LOG_BOUND {
                let overflow = completed.len() + 1 - COMPLETED_LOG_BOUND;
                completed.drain(0..overflow);
            }
            completed.push(completion.clone());
        }
        if let Some(tx) = inner.completed_tx.lock().await.as_ref() {
            let _ = tx.send(completion);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
    use crate::jwt::JwtSecret;
    use crate::methods::get_blobs::VERSIONED_HASH_VERSION_KZG;
    use crate::methods::names;
    use crate::metrics::{EngineMetrics, GetBlobsResultLabels, MethodLabels};
    use crate::transport::{EngineTransport, Lane};
    use crate::metrics::EngineMethod;
    use prometheus_client::registry::Registry;
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    fn metrics() -> EngineMetrics {
        let mut registry = Registry::default();
        EngineMetrics::register(&mut registry)
    }

    fn transport(url: &str, m: Option<EngineMetrics>) -> SharedTransport {
        Arc::new(EngineTransport::from_parts(
            url,
            JwtSecret::from_bytes([0x44; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs {
                new_payload_ms: 2_000,
                forkchoice_updated_ms: 2_000,
                get_blobs_ms: 1_000,
                exchange_capabilities_ms: 1_000,
                eth_syncing_ms: 1_000,
                multiplier: 1.0,
            }),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            m,
        ))
    }

    fn commitment(n: u8) -> [u8; 48] {
        [n; 48]
    }

    /// Counting mock: records how many getBlobsV2 calls arrive.
    struct CountingGetBlobs {
        hits: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl Respond for CountingGetBlobs {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
            let method = body.get("method").and_then(|m| m.as_str()).unwrap_or("");
            if method == names::GET_BLOBS_V2 {
                self.hits.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200)
                    .set_delay(self.delay)
                    .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":null}))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc":"2.0","id":1,
                    "result":{"status":"VALID","latestValidHash":null,"validationError":null}
                }))
            }
        }
    }

    /// CC-37 /1: block branch fires the fetch.
    #[tokio::test]
    async fn trigger_from_block() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::clone(&hits),
                delay: Duration::from_millis(10),
            })
            .mount(&server)
            .await;

        let m = metrics();
        let lane = FastpathLane::new(
            transport(&server.uri(), Some(m.clone())),
            Some(m),
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        );
        let mut rx = lane.subscribe_completions().await;
        let worker = lane.spawn_worker();

        let root = [0xabu8; 32];
        let outcome = lane
            .trigger_from_block(
                root,
                54_016 * 32,
                &[commitment(1)],
                NullContext::PrunedPool,
            )
            .await;
        assert_eq!(outcome, EnqueueOutcome::Enqueued);

        let (got_root, result) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timeout waiting for fetch")
            .expect("channel closed");
        assert_eq!(got_root, root);
        assert!(matches!(result, FetchResult::Miss { .. }));
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        lane.close().await;
        let _ = worker.await;
    }

    /// CC-37 /1: column branch fires **without any block present**.
    #[tokio::test]
    async fn trigger_from_column_sidecar() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::clone(&hits),
                delay: Duration::from_millis(10),
            })
            .mount(&server)
            .await;

        let m = metrics();
        let lane = FastpathLane::new(
            transport(&server.uri(), Some(m.clone())),
            Some(m),
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        );
        let mut rx = lane.subscribe_completions().await;
        let worker = lane.spawn_worker();

        // No beacon block in hand — only sidecar commitments + root from header.
        let root = [0xcd_u8; 32];
        let outcome = lane
            .trigger_from_column_sidecar(
                root,
                54_016 * 32,
                &[commitment(9), commitment(10)],
                NullContext::PrunedPool,
            )
            .await;
        assert_eq!(outcome, EnqueueOutcome::Enqueued);
        assert_eq!(
            outcome,
            EnqueueOutcome::Enqueued,
            "column branch must not require a block"
        );

        let (got_root, _) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timeout")
            .expect("closed");
        assert_eq!(got_root, root);
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // Ownership tag is structural: column path uses P2pColumn.
        // (Verified by enqueue path; Trigger is private after enqueue.)
        lane.close().await;
        let _ = worker.await;
    }

    /// Single-flight collapses both triggers to one EL call.
    #[tokio::test]
    async fn single_flight_by_block_root() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::clone(&hits),
                delay: Duration::from_millis(200),
            })
            .mount(&server)
            .await;

        let m = metrics();
        let dropped_before = m.fastpath_dropped.get();
        let lane = FastpathLane::new(
            transport(&server.uri(), Some(m.clone())),
            Some(m.clone()),
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        );
        let mut rx = lane.subscribe_completions().await;
        let worker = lane.spawn_worker();

        let root = [0x11_u8; 32];
        let a = lane
            .trigger_from_block(root, 100, &[commitment(1)], NullContext::PrunedPool)
            .await;
        let b = lane
            .trigger_from_column_sidecar(root, 100, &[commitment(1)], NullContext::PrunedPool)
            .await;
        assert_eq!(a, EnqueueOutcome::Enqueued);
        assert_eq!(b, EnqueueOutcome::DroppedSingleFlight);

        let _ = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timeout")
            .expect("closed");

        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "mock EL must see exactly one engine_getBlobsV2"
        );
        let dropped_after = m.fastpath_dropped.get();
        assert_eq!(
            dropped_after - dropped_before,
            1,
            "cc_engine_fastpath_dropped_total must increment by 1"
        );

        lane.close().await;
        let _ = worker.await;
    }

    /// Zero-commitment block issues no request.
    #[tokio::test]
    async fn zero_commitment_block_issues_no_request() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::clone(&hits),
                delay: Duration::ZERO,
            })
            .mount(&server)
            .await;

        let lane = FastpathLane::new(
            transport(&server.uri(), None),
            None,
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        );
        let worker = lane.spawn_worker();

        let outcome = lane
            .trigger_from_block([0u8; 32], 1, &[], NullContext::PrunedPool)
            .await;
        assert_eq!(outcome, EnqueueOutcome::SkippedNoCommitments);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        lane.close().await;
        let _ = worker.await;
    }

    /// 1 s timeout is the fastpath lane's get_blobs knob.
    #[tokio::test]
    async fn get_blobs_timeout_is_one_second() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":null})),
            )
            .mount(&server)
            .await;

        let m = metrics();
        // Explicit 1 s get_blobs timeout (the production default).
        let timeouts = TransportTimeouts::from_knobs(&TimeoutKnobs {
            new_payload_ms: 8_000,
            forkchoice_updated_ms: 8_000,
            get_blobs_ms: 1_000,
            exchange_capabilities_ms: 1_000,
            eth_syncing_ms: 1_000,
            multiplier: 1.0,
        });
        assert_eq!(timeouts.get_blobs, Duration::from_millis(1_000));
        let t = Arc::new(EngineTransport::from_parts(
            server.uri(),
            JwtSecret::from_bytes([0x55; 32]),
            timeouts,
            Duration::from_secs(60),
            Some(m.clone()),
        ));
        let lane = FastpathLane::new(t, Some(m.clone()), hoodi_blob_bound(), None, None, SubscriptionSet::empty());
        let mut rx = lane.subscribe_completions().await;
        let worker = lane.spawn_worker();

        let started = Instant::now();
        let _ = lane
            .trigger_from_block([7u8; 32], 100, &[commitment(1)], NullContext::PrunedPool)
            .await;
        let (_, result) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout waiting for abort")
            .expect("closed");
        let elapsed = started.elapsed();
        // Abort around 1 s (not the 5 s mock delay, not the 8 s newPayload budget).
        assert!(
            elapsed >= Duration::from_millis(800) && elapsed < Duration::from_secs(3),
            "expected ~1 s abort, got {elapsed:?}"
        );
        assert!(
            matches!(result, FetchResult::Error(_)),
            "timeout is an error, not a miss: {result:?}"
        );
        let to = m
            .transport_timeout
            .get_or_create(&MethodLabels {
                method: "getBlobsV2".into(),
            })
            .get();
        assert!(to >= 1, "cc_engine_transport_timeout_total{{method=getBlobsV2}}");
        let err = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "error".into(),
            })
            .get();
        assert!(err >= 1, "timeout must count as result=error");
        let miss = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "miss".into(),
            })
            .get();
        assert_eq!(miss, 0, "timeout must not count as miss");

        lane.close().await;
        let _ = worker.await;
    }

    /// Stalled getBlobs must not delay newPayload (CC-37 /10) — via the lane.
    #[tokio::test]
    async fn stalled_get_blobs_does_not_delay_new_payload() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::clone(&hits),
                delay: Duration::from_secs(5),
            })
            .mount(&server)
            .await;

        let t = transport(&server.uri(), None);
        let lane = FastpathLane::new(Arc::clone(&t), None, hoodi_blob_bound(), None, None, SubscriptionSet::empty());
        let worker = lane.spawn_worker();
        let _ = lane
            .trigger_from_block([9u8; 32], 100, &[commitment(1)], NullContext::PrunedPool)
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let started = Instant::now();
        let np = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([{}]),
            )
            .await;
        let elapsed = started.elapsed();
        assert!(np.is_ok(), "{np:?}");
        assert!(
            elapsed < Duration::from_secs(2),
            "newPayload delayed by getBlobs: {elapsed:?}"
        );

        lane.close().await;
        let _ = worker.await;
    }

    #[test]
    fn versioned_hash_version_byte_present() {
        assert_eq!(VERSIONED_HASH_VERSION_KZG, 0x01);
    }

    /// Bound gate at enqueue: oversized hash list never reaches the EL.
    #[tokio::test]
    async fn enqueue_rejects_hashes_over_runtime_max_blobs() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::clone(&hits),
                delay: Duration::ZERO,
            })
            .mount(&server)
            .await;

        let lane = FastpathLane::new(
            transport(&server.uri(), None),
            None,
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        );
        let worker = lane.spawn_worker();

        // Electra-window slot → max 9; 10 commitments must be rejected at enqueue.
        let slot_electra = 50_688 * 32;
        let commits: Vec<[u8; 48]> = (0..10).map(|i| commitment(i as u8)).collect();
        let outcome = lane
            .trigger_from_block([0xee_u8; 32], slot_electra, &commits, NullContext::PrunedPool)
            .await;
        assert_eq!(outcome, EnqueueOutcome::SkippedExceedsBound);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0, "no EL call for oversize");

        lane.close().await;
        let _ = worker.await;
    }

    /// Drop-oldest keeps the **newest** trigger and returns Enqueued to that caller (F2).
    #[tokio::test]
    async fn drop_oldest_returns_enqueued_for_newest() {
        let server = MockServer::start().await;
        // Stall so the queue fills without draining.
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_secs(30),
            })
            .mount(&server)
            .await;

        let m = metrics();
        let dropped_before = m.fastpath_dropped.get();
        let lane = FastpathLane::new(
            transport(&server.uri(), Some(m.clone())),
            Some(m.clone()),
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        );
        let worker = lane.spawn_worker();

        // Fill queue + one in-flight: worker takes first; remaining fill to bound.
        for i in 0..FASTPATH_QUEUE_BOUND + 2 {
            let mut root = [0u8; 32];
            root[0] = i as u8;
            let outcome = lane
                .trigger_from_block(root, 54_016 * 32, &[commitment(1)], NullContext::PrunedPool)
                .await;
            // Every admission of a *new* root that we keep is Enqueued — including
            // after drop-oldest evictions. Never DroppedQueueOverflow (removed).
            assert!(
                matches!(
                    outcome,
                    EnqueueOutcome::Enqueued | EnqueueOutcome::DroppedSingleFlight
                ),
                "unexpected outcome {outcome:?} at i={i}"
            );
        }
        // At least one eviction must have incremented the drop counter.
        assert!(
            m.fastpath_dropped.get() > dropped_before,
            "drop-oldest must count evictions on fastpath_dropped"
        );

        // One more newest root: must be Enqueued (kept), not a "dropped" outcome.
        let newest = [0xff_u8; 32];
        let outcome = lane
            .trigger_from_block(newest, 54_016 * 32, &[commitment(2)], NullContext::PrunedPool)
            .await;
        assert_eq!(
            outcome,
            EnqueueOutcome::Enqueued,
            "newest trigger must report Enqueued after drop-oldest"
        );

        lane.close().await;
        worker.abort();
    }

    /// Completion log is a ring of COMPLETED_LOG_BOUND (F3).
    #[tokio::test]
    async fn completed_log_is_bounded() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(CountingGetBlobs {
                hits: Arc::new(AtomicUsize::new(0)),
                delay: Duration::ZERO,
            })
            .mount(&server)
            .await;

        let lane = FastpathLane::new(
            transport(&server.uri(), None),
            None,
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        );
        let worker = lane.spawn_worker();

        let n = COMPLETED_LOG_BOUND + 10;
        for i in 0..n {
            let mut root = [0u8; 32];
            root[0] = (i % 250) as u8;
            root[1] = (i / 250) as u8;
            let _ = lane
                .trigger_from_block(root, 54_016 * 32, &[commitment(1)], NullContext::PrunedPool)
                .await;
        }
        // Drain worker.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let log = lane.completed().await;
        assert!(
            log.len() <= COMPLETED_LOG_BOUND,
            "completed log grew past bound: {}",
            log.len()
        );

        lane.close().await;
        let _ = worker.await;
    }

    // ── fetch.rs acceptance tests (live here so fetch.rs stays free of 9/21) ─

    /// CC-37 /8: same binary, two epochs — bound follows CC-1G.
    #[test]
    fn request_length_within_runtime_max_blobs() {
        use crate::methods::get_blobs::GET_BLOBS_V2_MAX_HASHES;
        use cc_types::primitives::Epoch;
        use fetch::assert_request_length_within_bound;

        let bound = hoodi_blob_bound();
        // Pre-first-entry (Electra base): max = 9.
        let epoch_electra = Epoch::new(50_688);
        let p_electra = bound.get_blob_parameters(epoch_electra);
        assert_eq!(p_electra.max_blobs_per_block, 9);
        let hashes_electra: Vec<[u8; 32]> = (0..9).map(|i| {
            let mut h = [0u8; 32];
            h[0] = VERSIONED_HASH_VERSION_KZG;
            h[31] = i as u8;
            h
        }).collect();
        assert!(assert_request_length_within_bound(&hashes_electra, &bound, epoch_electra).is_ok());

        // Post-BPO2: max = 21.
        let epoch_bpo2 = Epoch::new(54_016);
        let p_bpo2 = bound.get_blob_parameters(epoch_bpo2);
        assert_eq!(p_bpo2.max_blobs_per_block, 21);
        let hashes_bpo2: Vec<[u8; 32]> = (0..21).map(|i| {
            let mut h = [0u8; 32];
            h[0] = VERSIONED_HASH_VERSION_KZG;
            h[31] = i as u8;
            h
        }).collect();
        assert!(assert_request_length_within_bound(&hashes_bpo2, &bound, epoch_bpo2).is_ok());

        // One over the electra max must fail the soft gate.
        let too_many: Vec<[u8; 32]> = (0..10).map(|i| {
            let mut h = [0u8; 32];
            h[0] = VERSIONED_HASH_VERSION_KZG;
            h[31] = i as u8;
            h
        }).collect();
        assert!(assert_request_length_within_bound(&too_many, &bound, epoch_electra).is_err());

        assert_eq!(GET_BLOBS_V2_MAX_HASHES, 128);
        assert!(p_bpo2.max_blobs_per_block <= GET_BLOBS_V2_MAX_HASHES as u64);
        assert!(p_electra.max_blobs_per_block <= GET_BLOBS_V2_MAX_HASHES as u64);
    }

    /// Bound is read at runtime via `get_blob_parameters`; fetch.rs has no 9/21.
    #[test]
    fn bound_uses_runtime_lookup_not_literals() {
        let src = include_str!("fetch.rs");
        assert!(
            src.contains("get_blob_parameters"),
            "fetch.rs must call get_blob_parameters (CC-1G)"
        );
        assert!(
            src.contains("debug_assert"),
            "fetch.rs must debug_assert the bound"
        );
        // AC greps: no "chunk" / "split" implementation in fetch.rs.
        let lower = src.to_ascii_lowercase();
        assert!(!lower.contains("chunk") && !lower.contains("split"));
        // AC greps: no blob-count literals 9 or 21 in fetch.rs.
        assert!(
            !regex_word_boundary_digit(src, 9) && !regex_word_boundary_digit(src, 21),
            "fetch.rs must not embed blob-count maxima 9/21"
        );
    }

    fn regex_word_boundary_digit(src: &str, n: u64) -> bool {
        // Approximate `\bN\b`: digit sequence equal to n not part of a larger number.
        let needle = n.to_string();
        let bytes = src.as_bytes();
        let nb = needle.as_bytes();
        let mut i = 0;
        while i + nb.len() <= bytes.len() {
            if &bytes[i..i + nb.len()] == nb {
                let before_ok = i == 0 || !bytes[i - 1].is_ascii_digit();
                let after_ok = i + nb.len() == bytes.len() || !bytes[i + nb.len()].is_ascii_digit();
                if before_ok && after_ok {
                    return true;
                }
            }
            i += 1;
        }
        false
    }

    /// Null-cause match in fetch.rs has no wildcard arm.
    #[test]
    fn null_cause_match_has_no_wildcard() {
        let src = include_str!("fetch.rs");
        assert!(src.contains("NullCause::PartialHit"));
        assert!(src.contains("NullCause::PrunedPool"));
        assert!(src.contains("NullCause::ElHeadPreOsaka"));
        assert!(
            !src.contains("_ =>"),
            "fetch.rs must not contain a wildcard match arm"
        );
    }

    /// CC-37 /2: partial hit is miss; sampling tracker untouched.
    #[tokio::test]
    async fn partial_hit_is_miss_not_error() {
        use crate::methods::get_blobs::NullCause;
        use fetch::{CountingSamplingTracker, FetchRequest, fetch_blobs};

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": 1, "result": null
            })))
            .mount(&server)
            .await;

        let m = metrics();
        let t = transport(&server.uri(), Some(m.clone()));
        let bound = hoodi_blob_bound();
        let tracker = CountingSamplingTracker::default();
        let mut h = [0u8; 32];
        h[0] = VERSIONED_HASH_VERSION_KZG;
        h[31] = 1;
        let req = FetchRequest {
            beacon_block_root: [1u8; 32],
            slot: 54_016 * 32,
            versioned_hashes: vec![h],
            null_ctx: NullContext::PartialHit,
        };
        let result = fetch_blobs(t.as_ref(), Some(&m), &bound, Some(&tracker), &req).await;
        assert_eq!(
            result,
            FetchResult::Miss {
                cause: NullCause::PartialHit
            }
        );
        let miss = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "miss".into(),
            })
            .get();
        let err = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "error".into(),
            })
            .get();
        assert!(miss >= 1);
        assert_eq!(err, 0);
        assert_eq!(tracker.interactions(), 0);
    }

    /// CC-37 /3: pre-Osaka null is miss and does not perturb state machine.
    #[tokio::test]
    async fn pre_osaka_null_is_miss() {
        use crate::capabilities::CapabilityCache;
        use crate::methods::eth_syncing::EthSyncingResult;
        use crate::methods::get_blobs::NullCause;
        use crate::state::{EngineStateHandle, EngineStateInternal, UpcheckOutcome};
        use fetch::{FetchRequest, fetch_blobs};

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": 1, "result": null
            })))
            .mount(&server)
            .await;

        let m = metrics();
        let t = transport(&server.uri(), Some(m.clone()));
        let bound = hoodi_blob_bound();
        let state = EngineStateHandle::new(
            Arc::new(CapabilityCache::new()),
            Some(m.clone()),
            Duration::from_secs(12),
        );
        let _ = state
            .apply(UpcheckOutcome::Ok(EthSyncingResult::NotSyncing))
            .await;
        assert_eq!(state.internal().await, EngineStateInternal::Synced);
        let synced_before = m
            .state
            .get_or_create(&crate::metrics::EngineStateLabels {
                state: "synced".into(),
            })
            .get();

        let mut h = [0u8; 32];
        h[0] = VERSIONED_HASH_VERSION_KZG;
        h[31] = 2;
        let req = FetchRequest {
            beacon_block_root: [2u8; 32],
            slot: 100,
            versioned_hashes: vec![h],
            null_ctx: NullContext::ElHeadPreOsaka,
        };
        let result = fetch_blobs(t.as_ref(), Some(&m), &bound, None, &req).await;
        assert_eq!(
            result,
            FetchResult::Miss {
                cause: NullCause::ElHeadPreOsaka
            }
        );

        let miss = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "miss".into(),
            })
            .get();
        let err = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "error".into(),
            })
            .get();
        assert!(miss >= 1);
        assert_eq!(err, 0);
        assert_eq!(state.internal().await, EngineStateInternal::Synced);
        let synced_after = m
            .state
            .get_or_create(&crate::metrics::EngineStateLabels {
                state: "synced".into(),
            })
            .get();
        assert_eq!(synced_before, synced_after);
    }

    /// Pruned-pool null is its own arm.
    #[tokio::test]
    async fn pruned_pool_null_is_miss() {
        use crate::methods::get_blobs::NullCause;
        use fetch::{FetchRequest, fetch_blobs};

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": 1, "result": null
            })))
            .mount(&server)
            .await;

        let m = metrics();
        let t = transport(&server.uri(), Some(m.clone()));
        let bound = hoodi_blob_bound();
        let mut h = [0u8; 32];
        h[0] = VERSIONED_HASH_VERSION_KZG;
        h[31] = 3;
        let req = FetchRequest {
            beacon_block_root: [3u8; 32],
            slot: 54_016 * 32,
            versioned_hashes: vec![h],
            null_ctx: NullContext::PrunedPool,
        };
        let result = fetch_blobs(t.as_ref(), Some(&m), &bound, None, &req).await;
        assert_eq!(
            result,
            FetchResult::Miss {
                cause: NullCause::PrunedPool
            }
        );
        let miss = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "miss".into(),
            })
            .get();
        assert!(miss >= 1);
    }

    #[test]
    fn column_sidecar_commitments_rebuild_hashes_without_block() {
        use crate::methods::get_blobs::versioned_hashes_from_commitments;
        let c1 = [0x11_u8; 48];
        let c2 = [0x22_u8; 48];
        let hashes = versioned_hashes_from_commitments(&[c1, c2]);
        assert_eq!(hashes.len(), 2);
        assert_eq!(hashes[0][0], VERSIONED_HASH_VERSION_KZG);
        assert_ne!(hashes[0], hashes[1]);
    }

    /// CC-37b live path: Complete → bind → cells → transpose → filter on the worker.
    #[tokio::test]
    async fn worker_composes_reconstruction_on_complete() {
        use crate::methods::get_blobs::{BYTES_PER_BLOB, CELL_PROOFS_PER_BLOB};
        use cc_crypto::{Blob, CellKzg, CKzgBackend};
        use cc_types::primitives::KzgCommitment;
        use hex;

        let kzg: Arc<dyn CellKzg> = Arc::new(CKzgBackend::load_default().expect("kzg"));
        // One real blob + proofs as the EL would return.
        let mut blob_bytes = vec![0u8; BYTES_PER_BLOB];
        for (i, chunk) in blob_bytes.chunks_mut(32).enumerate() {
            chunk[1] = 9;
            chunk[2] = (i as u8).wrapping_mul(3).wrapping_add(1);
        }
        let blob = Blob::from_slice(&blob_bytes).expect("blob");
        let commitment = kzg.blob_to_kzg_commitment(&blob).expect("c");
        let (_cells, proofs) = kzg.compute_cells_and_kzg_proofs(&blob).expect("p");
        let vh = crate::methods::get_blobs::kzg_commitment_to_versioned_hash(commitment.as_array());

        // Mock EL returns Complete with that blob+proofs.
        let server = MockServer::start().await;
        let blob_hex = format!("0x{}", hex::encode(blob.as_slice()));
        let proof_hexes: Vec<String> = proofs
            .iter()
            .map(|p| format!("0x{}", hex::encode(p.as_slice())))
            .collect();
        assert_eq!(proof_hexes.len(), CELL_PROOFS_PER_BLOB);
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [{
                    "blob": blob_hex,
                    "proofs": proof_hexes,
                }]
            })))
            .mount(&server)
            .await;

        let m = metrics();
        // Explicit subscription — not 0..8 default; first three columns only.
        let sub = SubscriptionSet::from_indices([0u64, 1, 2], 4);
        let lane = FastpathLane::new(
            transport(&server.uri(), Some(m.clone())),
            Some(m.clone()),
            hoodi_blob_bound(),
            None,
            Some(Arc::clone(&kzg)),
            sub,
        );
        let mut rx = lane.subscribe_completions().await;
        let worker = lane.spawn_worker();

        let raw = [*commitment.as_array()];
        let outcome = lane
            .trigger_from_block([0xaa; 32], 54_016 * 32, &raw, NullContext::PrunedPool)
            .await;
        assert_eq!(outcome, EnqueueOutcome::Enqueued);

        let (root, result) = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("timeout")
            .expect("closed");
        assert_eq!(root, [0xaa; 32]);
        match result {
            FetchResult::Assembled {
                n_blobs,
                published,
                dropped,
                published_bytes,
            } => {
                assert_eq!(n_blobs, 1);
                assert_eq!(published.len(), 3, "exactly subscribed columns");
                assert_eq!(dropped, 125);
                assert!(published_bytes > 0);
                for sc in &published {
                    assert!(sc.index < 3);
                    assert_eq!(sc.column.len(), 1);
                    assert_eq!(
                        sc.kzg_commitments[0],
                        KzgCommitment::from_array(*commitment.as_array())
                    );
                }
                // Filter before boundary: no unsubscribed columns present.
                assert!(published.iter().all(|s| s.index < 3));
            }
            other => panic!("expected Assembled, got {other:?}"),
        }
        // Hash bind used the request versioned hash (implicit in success path).
        let _ = vh;

        lane.close().await;
        let _ = worker.await;
    }

    /// Empty subscription on Complete → Assembled with zero published (fail-closed).
    #[tokio::test]
    async fn empty_subscription_publishes_nothing() {
        use crate::methods::get_blobs::{BYTES_PER_BLOB, CELL_PROOFS_PER_BLOB};
        use cc_crypto::{Blob, CellKzg, CKzgBackend};

        let kzg: Arc<dyn CellKzg> = Arc::new(CKzgBackend::load_default().expect("kzg"));
        let mut blob_bytes = vec![0u8; BYTES_PER_BLOB];
        for (i, chunk) in blob_bytes.chunks_mut(32).enumerate() {
            chunk[1] = 3;
            chunk[2] = (i as u8).wrapping_add(1);
        }
        let blob = Blob::from_slice(&blob_bytes).expect("blob");
        let commitment = kzg.blob_to_kzg_commitment(&blob).expect("c");
        let (_cells, proofs) = kzg.compute_cells_and_kzg_proofs(&blob).expect("p");

        let server = MockServer::start().await;
        let blob_hex = format!("0x{}", hex::encode(blob.as_slice()));
        let proof_hexes: Vec<String> = proofs
            .iter()
            .map(|p| format!("0x{}", hex::encode(p.as_slice())))
            .collect();
        assert_eq!(proof_hexes.len(), CELL_PROOFS_PER_BLOB);
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [{"blob": blob_hex, "proofs": proof_hexes}]
            })))
            .mount(&server)
            .await;

        let lane = FastpathLane::new(
            transport(&server.uri(), None),
            None,
            hoodi_blob_bound(),
            None,
            Some(kzg),
            SubscriptionSet::empty(), // production-safe default
        );
        let mut rx = lane.subscribe_completions().await;
        let worker = lane.spawn_worker();
        let _ = lane
            .trigger_from_block(
                [0xbb; 32],
                54_016 * 32,
                &[*commitment.as_array()],
                NullContext::PrunedPool,
            )
            .await;
        let (_, result) = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("timeout")
            .expect("closed");
        match result {
            FetchResult::Assembled {
                published,
                dropped,
                ..
            } => {
                assert!(published.is_empty());
                assert_eq!(dropped, 128);
            }
            other => panic!("expected Assembled empty, got {other:?}"),
        }
        lane.close().await;
        let _ = worker.await;
    }
}
