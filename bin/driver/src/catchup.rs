//! Catch-up: anchor+1 → head with 8-deep prefetch and strictly sequential import
//! (Architecture §9.2, CC-1Aa).
//!
//! Fetches are pipelined up to `prefetch_depth` while **exactly one**
//! `ImportBlock` is in flight. Empty slots (HTTP 404) are skipped.
//! Termination re-polls head *after* the last import so a live chain that
//! advanced during catch-up still converges.
//!
//! # Offline acceptance (CC-1Aa AC)
//!
//! Unit tests in this module drive catch-up against a **stub HTTP beacon** and a
//! process-local [`BlockImporter`] (`MockImporter`), not a live `cc-chain` gRPC
//! core. That exercises empty-slot skip, strict slot order, one-in-flight
//! import, prefetch overlap, moving head, and backpressure without SSZ decode
//! or a full store. **Real-chain** import of fixture / live SSZ remains:
//! `services/chain` offline replay + live Hoodi soak (CC-1Ad). The issue’s
//! “stub → real chain” wording is intentionally split: driver logic here,
//! chain integrity there.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_proto::chain::{ImportBlockRequest, ImportBlockResponse, ImportBlockVerdict};
use cc_proto::common::Source;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::api::{ApiError, BeaconApiClient, FetchedBlock};

/// Default prefetch window (§9.2).
pub(crate) const DEFAULT_PREFETCH_DEPTH: usize = 8;

/// Backoff base when `chain` returns `RESOURCE_EXHAUSTED`.
const BACKPRESSURE_BASE: Duration = Duration::from_millis(50);
/// Cap for backpressure retry sleep.
const BACKPRESSURE_CAP: Duration = Duration::from_secs(2);
/// Max retries on `RESOURCE_EXHAUSTED` before surfacing the error.
const BACKPRESSURE_MAX_RETRIES: u32 = 64;

/// Outcome of one catch-up run (for logs / soak record).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatchupReport {
    /// Slot the walk started from (anchor).
    pub anchor_slot: u64,
    /// Highest head slot observed at termination.
    pub head_slot_reached: u64,
    /// Blocks actually imported (non-empty slots).
    pub blocks_imported: u64,
    /// Empty slots skipped (404).
    pub empty_slots_skipped: u64,
    /// Wall time for the whole catch-up.
    pub elapsed: Duration,
    /// `unknown_parent` count at end (must be 0 for CC-1A/1).
    pub unknown_parent: u64,
}

/// Import-result counters the catch-up loop updates (and metrics mirror).
#[derive(Debug, Default, Clone)]
pub(crate) struct ImportResultCounts {
    pub imported: u64,
    pub duplicate: u64,
    pub deferred: u64,
    pub unknown_parent: u64,
    pub invalid: u64,
    pub unspecified: u64,
}

impl ImportResultCounts {
    /// Total `unknown_parent` verdicts.
    pub(crate) fn unknown_parent(&self) -> u64 {
        self.unknown_parent
    }

    /// Record a verdict from an `ImportBlockResponse`.
    pub(crate) fn record(&mut self, verdict: ImportBlockVerdict) {
        match verdict {
            ImportBlockVerdict::Imported => self.imported += 1,
            ImportBlockVerdict::Duplicate => self.duplicate += 1,
            ImportBlockVerdict::DeferredDa => self.deferred += 1,
            ImportBlockVerdict::UnknownParent => self.unknown_parent += 1,
            ImportBlockVerdict::Invalid => self.invalid += 1,
            ImportBlockVerdict::Unspecified => self.unspecified += 1,
        }
    }
}

/// Optional instrumentation for offline tests (concurrency + ordering).
#[derive(Debug, Default)]
pub(crate) struct CatchupProbe {
    /// Peak concurrent in-flight fetches.
    pub peak_fetches: AtomicUsize,
    /// Peak concurrent in-flight imports (must stay ≤ 1).
    pub peak_imports: AtomicUsize,
    /// Current concurrent fetches.
    inflight_fetches: AtomicUsize,
    /// Current concurrent imports.
    inflight_imports: AtomicUsize,
    /// Slots imported in order (non-empty only).
    pub imported_slots: std::sync::Mutex<Vec<u64>>,
}

impl CatchupProbe {
    /// Snapshot of imported slots (test helper).
    #[cfg(test)]
    fn imported_slots(&self) -> Vec<u64> {
        self.imported_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn enter_fetch(&self) {
        let cur = self.inflight_fetches.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_fetches.fetch_max(cur, Ordering::SeqCst);
    }

    fn leave_fetch(&self) {
        self.inflight_fetches.fetch_sub(1, Ordering::SeqCst);
    }

    fn enter_import(&self) {
        let cur = self.inflight_imports.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_imports.fetch_max(cur, Ordering::SeqCst);
    }

    fn leave_import(&self) {
        self.inflight_imports.fetch_sub(1, Ordering::SeqCst);
    }

    fn push_imported_slot(&self, slot: u64) {
        self.imported_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(slot);
    }
}

/// Abstraction over `chain.ImportBlock` so offline tests can inject a stub.
pub(crate) trait BlockImporter: Send {
    /// Unary `ImportBlock`. Implementations must not re-order concurrent calls
    /// (the catch-up loop already serialises them).
    fn import_block(
        &mut self,
        request: ImportBlockRequest,
    ) -> impl Future<Output = Result<ImportBlockResponse, tonic::Status>> + Send;
}

/// gRPC client wrapper.
#[derive(Debug, Clone)]
pub(crate) struct ChainGrpcImporter {
    client: cc_proto::chain::chain_service_client::ChainServiceClient<tonic::transport::Channel>,
}

impl ChainGrpcImporter {
    /// Connect to `uri` (e.g. `http://127.0.0.1:9001`).
    pub(crate) async fn connect(uri: &str) -> Result<Self, tonic::transport::Error> {
        let channel = tonic::transport::Endpoint::from_shared(uri.to_owned())?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .connect()
            .await?;
        Ok(Self {
            client: cc_proto::chain::chain_service_client::ChainServiceClient::new(channel),
        })
    }

    /// Poll `GetHead` until the core is bootstrapped (not `FAILED_PRECONDITION`).
    pub(crate) async fn wait_bootstrapped(
        &mut self,
        timeout: Duration,
    ) -> Result<(u64, Vec<u8>), tonic::Status> {
        let deadline = Instant::now() + timeout;
        loop {
            match self
                .client
                .get_head(cc_proto::chain::GetHeadRequest {})
                .await
            {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    return Ok((inner.head_slot, inner.head_root));
                }
                Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                    if Instant::now() >= deadline {
                        return Err(status);
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(status) => return Err(status),
            }
        }
    }

    /// One-shot `GetHead` (steady-state / walk-back will use this in CC-1Ab).
    #[allow(dead_code)]
    pub(crate) async fn get_head(&mut self) -> Result<(u64, Vec<u8>), tonic::Status> {
        let resp = self
            .client
            .get_head(cc_proto::chain::GetHeadRequest {})
            .await?;
        let inner = resp.into_inner();
        Ok((inner.head_slot, inner.head_root))
    }
}

impl BlockImporter for ChainGrpcImporter {
    async fn import_block(
        &mut self,
        request: ImportBlockRequest,
    ) -> Result<ImportBlockResponse, tonic::Status> {
        let resp = self.client.import_block(request).await?;
        Ok(resp.into_inner())
    }
}

/// Callback when an import verdict is recorded (metrics hook).
pub(crate) type OnImportResult = Arc<dyn Fn(ImportBlockVerdict) + Send + Sync>;

/// Catch-up parameters.
#[derive(Debug, Clone)]
pub(crate) struct CatchupConfig {
    /// First known slot (checkpoint anchor). Walk starts at `anchor_slot + 1`.
    pub anchor_slot: u64,
    /// Prefetch window depth (default 8).
    pub prefetch_depth: usize,
}

/// Run catch-up until the imported tip reaches a head polled *after* the last import.
pub(crate) async fn run_catchup<I: BlockImporter>(
    api: &BeaconApiClient,
    importer: &mut I,
    cfg: CatchupConfig,
    on_result: Option<OnImportResult>,
    probe: Option<Arc<CatchupProbe>>,
) -> Result<(CatchupReport, ImportResultCounts), CatchupError> {
    let start = Instant::now();
    let prefetch = cfg.prefetch_depth.max(1);
    let mut next_slot = cfg.anchor_slot.saturating_add(1);
    let mut counts = ImportResultCounts::default();
    let mut blocks_imported = 0u64;
    let mut empty_slots_skipped = 0u64;
    // Last head observed on a post-import poll (or the initial poll if already caught up).
    let mut head_slot_reached;

    info!(
        anchor_slot = cfg.anchor_slot,
        prefetch_depth = prefetch,
        "catch-up starting"
    );

    loop {
        let head = api
            .get_head_header()
            .await
            .map_err(CatchupError::Api)?;

        if next_slot > head.slot {
            // Already at/ past the head we just polled → complete.
            head_slot_reached = head.slot;
            break;
        }

        let window_end = head.slot;
        let (win_blocks, win_empties) = catchup_window(
            api,
            importer,
            next_slot,
            window_end,
            prefetch,
            &mut counts,
            on_result.as_ref(),
            probe.as_ref(),
        )
        .await?;
        blocks_imported += win_blocks;
        empty_slots_skipped += win_empties;
        next_slot = window_end.saturating_add(1);

        // Re-poll head *after* the last import of this window (§9.2 termination).
        let head_after = api
            .get_head_header()
            .await
            .map_err(CatchupError::Api)?;
        head_slot_reached = head_after.slot;
        if next_slot > head_after.slot {
            break;
        }
        // Network advanced during the window — continue from next_slot.
        debug!(
            next_slot,
            new_head = head_after.slot,
            "head advanced during catch-up; continuing"
        );
    }

    let elapsed = start.elapsed();
    let report = CatchupReport {
        anchor_slot: cfg.anchor_slot,
        head_slot_reached,
        blocks_imported,
        empty_slots_skipped,
        elapsed,
        unknown_parent: counts.unknown_parent(),
    };
    info!(
        anchor_slot = report.anchor_slot,
        head_slot = report.head_slot_reached,
        blocks = report.blocks_imported,
        empty = report.empty_slots_skipped,
        unknown_parent = report.unknown_parent,
        elapsed_ms = elapsed.as_millis() as u64,
        "catch-up complete"
    );
    Ok((report, counts))
}

/// Publish catch-up complete as unix seconds (CC-1C/3 boundary).
pub(crate) fn unix_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Error type for catch-up.
#[derive(Debug)]
pub(crate) enum CatchupError {
    /// Beacon API failure.
    Api(ApiError),
    /// Chain gRPC failure (after backpressure retries exhausted).
    Chain(tonic::Status),
    /// Prefetch task join failure.
    Join(tokio::task::JoinError),
}

impl std::fmt::Display for CatchupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "api: {e}"),
            Self::Chain(e) => write!(f, "chain: {e}"),
            Self::Join(e) => write!(f, "join: {e}"),
        }
    }
}

impl std::error::Error for CatchupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Api(e) => Some(e),
            Self::Chain(e) => Some(e),
            Self::Join(e) => Some(e),
        }
    }
}

type PrefetchItem = (u64, JoinHandle<Result<Option<FetchedBlock>, ApiError>>);

#[allow(clippy::too_many_arguments)]
async fn catchup_window<I: BlockImporter>(
    api: &BeaconApiClient,
    importer: &mut I,
    start_slot: u64,
    end_slot: u64,
    prefetch: usize,
    counts: &mut ImportResultCounts,
    on_result: Option<&OnImportResult>,
    probe: Option<&Arc<CatchupProbe>>,
) -> Result<(u64, u64), CatchupError> {
    let mut next_fetch = start_slot;
    let mut next_import = start_slot;
    let mut inflight: VecDeque<PrefetchItem> = VecDeque::new();
    let mut blocks = 0u64;
    let mut empties = 0u64;

    while next_import <= end_slot {
        // Fill the prefetch window (bounded).
        while inflight.len() < prefetch && next_fetch <= end_slot {
            let slot = next_fetch;
            next_fetch += 1;
            let client = api.clone();
            let probe_fetch = probe.cloned();
            let handle = tokio::spawn(async move {
                if let Some(p) = probe_fetch.as_ref() {
                    p.enter_fetch();
                }
                // Artificial yield so tests can observe overlapping fetches.
                tokio::task::yield_now().await;
                let result = client.fetch_slot(slot).await;
                if let Some(p) = probe_fetch.as_ref() {
                    p.leave_fetch();
                }
                result
            });
            inflight.push_back((slot, handle));
        }

        let Some((slot, handle)) = inflight.pop_front() else {
            return Err(CatchupError::Chain(tonic::Status::internal(
                "catch-up prefetch queue empty while slots remain",
            )));
        };
        debug_assert_eq!(slot, next_import);

        let fetched = handle.await.map_err(CatchupError::Join)?.map_err(CatchupError::Api)?;
        match fetched {
            None => {
                empties += 1;
                debug!(slot, "empty slot (404); skip");
            }
            Some(block) => {
                let verdict = import_with_backpressure(importer, &block, probe).await?;
                counts.record(verdict);
                if let Some(cb) = on_result {
                    cb(verdict);
                }
                if let Some(p) = probe {
                    p.push_imported_slot(block.slot);
                }
                match verdict {
                    ImportBlockVerdict::Imported | ImportBlockVerdict::Duplicate => {
                        blocks += 1;
                    }
                    ImportBlockVerdict::UnknownParent => {
                        warn!(
                            slot = block.slot,
                            "import UNKNOWN_PARENT during catch-up (ordering bug?)"
                        );
                        blocks += 1;
                    }
                    other => {
                        warn!(slot = block.slot, ?other, "import non-success verdict");
                        blocks += 1;
                    }
                }
            }
        }
        next_import += 1;
    }

    Ok((blocks, empties))
}

async fn import_with_backpressure<I: BlockImporter>(
    importer: &mut I,
    block: &FetchedBlock,
    probe: Option<&Arc<CatchupProbe>>,
) -> Result<ImportBlockVerdict, CatchupError> {
    let request = ImportBlockRequest {
        ssz: block.ssz.to_vec(),
        fork: block.fork,
        root: block.root.clone(),
        source: Source::Api as i32,
    };

    let mut attempt = 0u32;
    loop {
        if let Some(p) = probe {
            p.enter_import();
        }
        let result = importer.import_block(request.clone()).await;
        if let Some(p) = probe {
            p.leave_import();
        }

        match result {
            Ok(resp) => {
                let verdict = match resp.verdict {
                    v if v == ImportBlockVerdict::Imported as i32 => ImportBlockVerdict::Imported,
                    v if v == ImportBlockVerdict::Duplicate as i32 => ImportBlockVerdict::Duplicate,
                    v if v == ImportBlockVerdict::DeferredDa as i32 => ImportBlockVerdict::DeferredDa,
                    v if v == ImportBlockVerdict::UnknownParent as i32 => {
                        ImportBlockVerdict::UnknownParent
                    }
                    v if v == ImportBlockVerdict::Invalid as i32 => ImportBlockVerdict::Invalid,
                    _ => ImportBlockVerdict::Unspecified,
                };
                return Ok(verdict);
            }
            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                attempt += 1;
                if attempt > BACKPRESSURE_MAX_RETRIES {
                    return Err(CatchupError::Chain(status));
                }
                let shift = (attempt - 1).min(5);
                let sleep = (BACKPRESSURE_BASE * 2u32.pow(shift)).min(BACKPRESSURE_CAP);
                debug!(?sleep, attempt, "chain RESOURCE_EXHAUSTED; retry");
                tokio::time::sleep(sleep).await;
            }
            Err(status) => return Err(CatchupError::Chain(status)),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::api::parse_root_hex;
    use http_body_util::Full;
    use hyper::body::Bytes as HyperBytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;
    use tokio::net::TcpListener;

    /// Minimal in-memory sequence served over HTTP (headers + SSZ).
    #[derive(Debug, Clone)]
    struct StubSlot {
        root: Vec<u8>,
        parent_root: Vec<u8>,
        ssz: Vec<u8>,
        empty: bool,
    }

    struct StubState {
        slots: HashMap<u64, StubSlot>,
        /// Mutable head slot (moving-head tests); `Arc` so the importer can advance it.
        head_slot: Arc<AtomicU64>,
        /// Artificial fetch delay for overlap tests.
        fetch_delay: Duration,
    }

    async fn spawn_stub(state: Arc<StubState>) -> SocketAddr {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let io = TokioIo::new(stream);
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let svc = service_fn(move |req| {
                        let state = Arc::clone(&state);
                        async move { handle_stub(req, state).await }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        // Brief pause so the listener is ready.
        tokio::task::yield_now().await;
        addr
    }

    async fn handle_stub(
        req: Request<hyper::body::Incoming>,
        state: Arc<StubState>,
    ) -> Result<Response<Full<HyperBytes>>, Infallible> {
        if state.fetch_delay > Duration::ZERO {
            tokio::time::sleep(state.fetch_delay).await;
        }
        let path = req.uri().path().to_owned();
        if req.method() != Method::GET {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(HyperBytes::new()))
                .unwrap());
        }

        if let Some(id) = path.strip_prefix("/eth/v1/beacon/headers/") {
            let slot = if id == "head" {
                state.head_slot.load(Ordering::SeqCst)
            } else {
                match id.parse::<u64>() {
                    Ok(s) => s,
                    Err(_) => {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Full::new(HyperBytes::new()))
                            .unwrap());
                    }
                }
            };
            return Ok(header_response(&state, slot));
        }

        if let Some(id) = path.strip_prefix("/eth/v2/beacon/blocks/") {
            let slot = if id == "head" {
                state.head_slot.load(Ordering::SeqCst)
            } else {
                match id.parse::<u64>() {
                    Ok(s) => s,
                    Err(_) => {
                        // Root-based lookup: scan.
                        if let Ok(root) = parse_root_hex(id) {
                            if let Some((&s, _)) =
                                state.slots.iter().find(|(_, v)| v.root == root)
                            {
                                s
                            } else {
                                return Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(HyperBytes::new()))
                                    .unwrap());
                            }
                        } else {
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Full::new(HyperBytes::new()))
                                .unwrap());
                        }
                    }
                }
            };
            return Ok(block_response(&state, slot));
        }

        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(HyperBytes::new()))
            .unwrap())
    }

    fn header_response(state: &StubState, slot: u64) -> Response<Full<HyperBytes>> {
        let Some(s) = state.slots.get(&slot) else {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(HyperBytes::new()))
                .unwrap();
        };
        if s.empty {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(HyperBytes::new()))
                .unwrap();
        }
        let root = format!("0x{}", hex::encode(&s.root));
        let parent = format!("0x{}", hex::encode(&s.parent_root));
        let body = format!(
            r#"{{"data":{{"root":"{root}","canonical":true,"header":{{"message":{{"slot":"{slot}","proposer_index":"0","parent_root":"{parent}","state_root":"0x{}","body_root":"0x{}"}},"signature":"0x00"}}}}}}"#,
            "00".repeat(32),
            "00".repeat(32),
        );
        Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(HyperBytes::from(body)))
            .unwrap()
    }

    fn block_response(state: &StubState, slot: u64) -> Response<Full<HyperBytes>> {
        let Some(s) = state.slots.get(&slot) else {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(HyperBytes::new()))
                .unwrap();
        };
        if s.empty {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(HyperBytes::new()))
                .unwrap();
        }
        Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
            .header("eth-consensus-version", "fulu")
            .body(Full::new(HyperBytes::from(s.ssz.clone())))
            .unwrap()
    }

    /// Local hex helper so tests do not pull an extra crate.
    mod hex {
        pub(super) fn encode(bytes: &[u8]) -> String {
            let mut s = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                s.push_str(&format!("{b:02x}"));
            }
            s
        }
    }

    /// Mock chain: tracks parent linkage and import concurrency.
    struct MockImporter {
        known: std::collections::HashSet<Vec<u8>>,
        order: Vec<u64>,
        /// ssz body → (slot, root, parent)
        roots_by_ssz: HashMap<Vec<u8>, (u64, Vec<u8>, Vec<u8>)>,
        import_delay: Duration,
        inflight: AtomicUsize,
        peak: AtomicUsize,
        backpressure_left: u32,
        /// When set, bump stub head to this value after importing `advance_after_slot`.
        advance_head_to: Option<u64>,
        advance_after_slot: Option<u64>,
        stub_head: Option<Arc<AtomicU64>>,
    }

    impl MockImporter {
        fn new(anchor_root: Vec<u8>) -> Self {
            let mut known = std::collections::HashSet::new();
            known.insert(anchor_root);
            Self {
                known,
                order: Vec::new(),
                roots_by_ssz: HashMap::new(),
                import_delay: Duration::ZERO,
                inflight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                backpressure_left: 0,
                advance_head_to: None,
                advance_after_slot: None,
                stub_head: None,
            }
        }

        fn register(&mut self, slot: u64, root: Vec<u8>, parent: Vec<u8>, ssz: Vec<u8>) {
            self.roots_by_ssz.insert(ssz, (slot, root, parent));
        }
    }

    impl BlockImporter for MockImporter {
        async fn import_block(
            &mut self,
            request: ImportBlockRequest,
        ) -> Result<ImportBlockResponse, tonic::Status> {
            if self.backpressure_left > 0 {
                self.backpressure_left -= 1;
                return Err(tonic::Status::resource_exhausted("queue full"));
            }
            let cur = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(cur, Ordering::SeqCst);
            if self.import_delay > Duration::ZERO {
                tokio::time::sleep(self.import_delay).await;
            }
            let (slot, root, parent) = self
                .roots_by_ssz
                .get(&request.ssz)
                .cloned()
                .unwrap_or((0, request.root.clone(), vec![]));
            assert_eq!(request.source, Source::Api as i32);
            let verdict = if self.known.contains(&root) {
                ImportBlockVerdict::Duplicate
            } else if !parent.is_empty() && !self.known.contains(&parent) {
                ImportBlockVerdict::UnknownParent
            } else {
                self.known.insert(root);
                self.order.push(slot);
                ImportBlockVerdict::Imported
            };
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            if let (Some(after), Some(to), Some(head)) = (
                self.advance_after_slot,
                self.advance_head_to,
                self.stub_head.as_ref(),
            ) && slot == after
                && matches!(
                    verdict,
                    ImportBlockVerdict::Imported | ImportBlockVerdict::Duplicate
                )
            {
                head.store(to, Ordering::SeqCst);
            }
            Ok(ImportBlockResponse {
                verdict: verdict as i32,
                reason: String::new(),
            })
        }
    }

    fn root(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    /// Build a linear parent-linked sequence with optional empty slots.
    fn linear_sequence(
        start: u64,
        n: u64,
        empty: &[u64],
        anchor_root: Vec<u8>,
    ) -> (HashMap<u64, StubSlot>, MockImporter) {
        let mut slots = HashMap::new();
        let mut importer = MockImporter::new(anchor_root.clone());
        let mut parent = anchor_root;
        for i in 0..n {
            let slot = start + i;
            if empty.contains(&slot) {
                slots.insert(
                    slot,
                    StubSlot {
                        root: root(0),
                        parent_root: parent.clone(),
                        ssz: vec![],
                        empty: true,
                    },
                );
                continue;
            }
            let r = root((i + 1) as u8);
            let ssz = vec![0xAB, i as u8, 0xCD];
            importer.register(slot, r.clone(), parent.clone(), ssz.clone());
            slots.insert(
                slot,
                StubSlot {
                    root: r.clone(),
                    parent_root: parent,
                    ssz,
                    empty: false,
                },
            );
            parent = r;
        }
        (slots, importer)
    }

    #[tokio::test]
    async fn catchup_skips_empty_slots_and_zero_unknown_parent() {
        let anchor_slot = 100;
        let start = 101;
        let empties = [103u64, 107];
        let anchor_root = root(0xAA);
        let (slots, mut importer) = linear_sequence(start, 10, &empties, anchor_root);
        let head = start + 9;
        let state = Arc::new(StubState {
            slots,
            head_slot: Arc::new(AtomicU64::new(head)),
            fetch_delay: Duration::from_millis(5),
        });
        let addr = spawn_stub(Arc::clone(&state)).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();
        let probe = Arc::new(CatchupProbe::default());
        let (report, counts) = run_catchup(
            &api,
            &mut importer,
            CatchupConfig {
                anchor_slot,
                prefetch_depth: 8,
            },
            None,
            Some(Arc::clone(&probe)),
        )
        .await
        .unwrap();

        assert_eq!(counts.unknown_parent, 0, "zero unknown_parent");
        assert_eq!(report.empty_slots_skipped, 2);
        assert_eq!(report.blocks_imported, 8);
        assert_eq!(report.head_slot_reached, head);
        let imported = probe.imported_slots();
        assert!(imported.windows(2).all(|w| w[0] < w[1]), "strict slot order");
        for e in empties {
            assert!(!imported.contains(&e), "empty slot {e} must not import");
        }
        // Sequential import: peak imports ≤ 1.
        assert!(
            probe.peak_imports.load(Ordering::SeqCst) <= 1,
            "at most one ImportBlock in flight"
        );
        // Prefetch: with delay, ≥2 fetches should overlap.
        assert!(
            probe.peak_fetches.load(Ordering::SeqCst) >= 2,
            "expected overlapping fetches, peak={}",
            probe.peak_fetches.load(Ordering::SeqCst)
        );
        assert_eq!(importer.peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn catchup_converges_when_head_moves() {
        let anchor_slot = 50;
        let start = 51;
        let anchor_root = root(0xBB);
        // Slots 51..=60 exist. Head starts at 55; after importing 55, bump to 58.
        let (slots, mut importer) = linear_sequence(start, 10, &[], anchor_root);
        let head_slot = Arc::new(AtomicU64::new(55));
        let state = Arc::new(StubState {
            slots,
            head_slot: Arc::clone(&head_slot),
            fetch_delay: Duration::ZERO,
        });
        importer.advance_after_slot = Some(55);
        importer.advance_head_to = Some(58);
        importer.stub_head = Some(head_slot);
        let addr = spawn_stub(Arc::clone(&state)).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();

        let (report, counts) = run_catchup(
            &api,
            &mut importer,
            CatchupConfig {
                anchor_slot,
                prefetch_depth: 4,
            },
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(counts.unknown_parent, 0);
        assert!(
            report.head_slot_reached >= 58,
            "must converge past moved head, got {}",
            report.head_slot_reached
        );
        // 51..=58 non-empty = 8 blocks.
        assert_eq!(report.blocks_imported, 8);
    }

    #[tokio::test]
    async fn backpressure_retries_then_succeeds() {
        let anchor_slot = 0;
        let anchor_root = root(0x01);
        let (slots, mut importer) = linear_sequence(1, 1, &[], anchor_root);
        importer.backpressure_left = 2;
        let state = Arc::new(StubState {
            slots,
            head_slot: Arc::new(AtomicU64::new(1)),
            fetch_delay: Duration::ZERO,
        });
        let addr = spawn_stub(state).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();
        let (report, counts) = run_catchup(
            &api,
            &mut importer,
            CatchupConfig {
                anchor_slot,
                prefetch_depth: 2,
            },
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(counts.unknown_parent, 0);
        assert_eq!(report.blocks_imported, 1);
        assert_eq!(importer.backpressure_left, 0);
    }

    #[tokio::test]
    async fn hoodi_sequence_manifest_empty_slots_skipped() {
        // Replay CC-10b slot *topology* (roots + empty marks) through the stub.
        // SSZ bodies are opaque placeholders — driver never decodes them.
        // Parent of start_slot is synthetic anchor so the mock linkage holds.
        let manifest = include_str!("../../../crates/types/tests/fixtures/hoodi-sequence.toml");
        let (anchor_slot, start_slot, entries) = parse_sequence_manifest(manifest);
        assert_eq!(anchor_slot, 3_649_472);
        assert_eq!(start_slot, 3_649_433);

        // Catch-up walks anchor+1 → head. The CC-10b window *ends* at the
        // anchor, so we re-base it as a synthetic post-anchor walk: treat
        // (start_slot - 1) as the anchor and import start_slot..anchor_slot.
        let synthetic_anchor = start_slot - 1;
        let first_parent = entries[0].parent_root.clone();
        let mut importer = MockImporter::new(first_parent.clone());
        let mut slots = HashMap::new();
        let mut empty_slots = Vec::new();
        for e in &entries {
            if e.empty {
                empty_slots.push(e.slot);
                slots.insert(
                    e.slot,
                    StubSlot {
                        root: e.root.clone(),
                        parent_root: e.parent_root.clone(),
                        ssz: vec![],
                        empty: true,
                    },
                );
            } else {
                let ssz = e.root.clone(); // unique opaque body
                importer.register(e.slot, e.root.clone(), e.parent_root.clone(), ssz.clone());
                slots.insert(
                    e.slot,
                    StubSlot {
                        root: e.root.clone(),
                        parent_root: e.parent_root.clone(),
                        ssz,
                        empty: false,
                    },
                );
            }
        }
        let head = entries.last().unwrap().slot;
        let state = Arc::new(StubState {
            slots,
            head_slot: Arc::new(AtomicU64::new(head)),
            fetch_delay: Duration::from_millis(2),
        });
        let addr = spawn_stub(state).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();
        let probe = Arc::new(CatchupProbe::default());
        let (report, counts) = run_catchup(
            &api,
            &mut importer,
            CatchupConfig {
                anchor_slot: synthetic_anchor,
                prefetch_depth: 8,
            },
            None,
            Some(Arc::clone(&probe)),
        )
        .await
        .unwrap();

        assert_eq!(counts.unknown_parent, 0);
        assert_eq!(report.empty_slots_skipped, empty_slots.len() as u64);
        for e in empty_slots {
            assert!(!probe.imported_slots().contains(&e));
        }
        let imported = probe.imported_slots();
        assert!(imported.windows(2).all(|w| w[0] < w[1]));
        assert!(probe.peak_imports.load(Ordering::SeqCst) <= 1);
        assert!(probe.peak_fetches.load(Ordering::SeqCst) >= 2);
    }

    struct SeqEntry {
        slot: u64,
        root: Vec<u8>,
        parent_root: Vec<u8>,
        empty: bool,
    }

    fn parse_sequence_manifest(text: &str) -> (u64, u64, Vec<SeqEntry>) {
        let mut anchor_slot = 0u64;
        let mut start_slot = 0u64;
        let mut entries = Vec::new();
        let mut cur_slot: Option<u64> = None;
        let mut cur_root = String::new();
        let mut cur_parent = String::new();
        let mut cur_empty = false;

        let flush = |slot: Option<u64>,
                     root: &str,
                     parent: &str,
                     empty: bool,
                     out: &mut Vec<SeqEntry>| {
            if let Some(s) = slot {
                let (root_b, parent_b) = if empty {
                    (vec![0u8; 32], vec![0u8; 32])
                } else {
                    (
                        parse_root_hex(root).unwrap(),
                        parse_root_hex(parent).unwrap(),
                    )
                };
                out.push(SeqEntry {
                    slot: s,
                    root: root_b,
                    parent_root: parent_b,
                    empty,
                });
            }
        };

        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if line == "[[slots]]" {
                flush(cur_slot, &cur_root, &cur_parent, cur_empty, &mut entries);
                cur_slot = None;
                cur_root.clear();
                cur_parent.clear();
                cur_empty = false;
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                let k = k.trim();
                let v = v.trim().trim_matches('"');
                match k {
                    "anchor_slot" => anchor_slot = v.parse().unwrap(),
                    "start_slot" => start_slot = v.parse().unwrap(),
                    "slot" => cur_slot = Some(v.parse().unwrap()),
                    "root" => cur_root = v.to_owned(),
                    "parent_root" => cur_parent = v.to_owned(),
                    "empty" => cur_empty = v == "true",
                    _ => {}
                }
            }
        }
        flush(cur_slot, &cur_root, &cur_parent, cur_empty, &mut entries);
        (anchor_slot, start_slot, entries)
    }
}
