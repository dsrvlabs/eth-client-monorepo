//! Root-chain walk-back for gaps and reorgs (Architecture §9.4, CC-1Ab).
//!
//! One mechanism covers missed polls, reorgs, and restarted providers:
//!
//! ```text
//! push the block onto a stack
//! loop: fetch header/block by parent_root
//!       if chain already has it (ImportBlock → DUPLICATE) → stop
//!       else push and continue, up to max_walkback_slots
//! then pop the stack, importing oldest-first
//! ```
//!
//! There is **no separate reorg path** — a reorg is a head whose parent we lack.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cc_proto::chain::ImportBlockVerdict;
use tracing::{debug, error, info, warn};

use crate::api::{ApiError, FetchedBlock, encode_root_hex};
use crate::catchup::{
    BlockImporter, CatchupError, CatchupProbe, ImportResultCounts, OnImportResult,
    import_with_backpressure,
};
use crate::ratelimit::ProviderPool;

/// Default `max_walkback_slots` (§9.4).
pub(crate) const DEFAULT_MAX_WALKBACK_SLOTS: u64 = 64;

/// Mainnet-shaped default for escalation (`4 × SLOTS_PER_EPOCH`).
pub(crate) const DEFAULT_SLOTS_PER_EPOCH: u64 = 32;

/// Walk-back configuration.
#[derive(Debug, Clone)]
pub(crate) struct WalkbackConfig {
    /// First-attempt depth limit.
    pub max_walkback_slots: u64,
    /// Used for the escalated second attempt: `4 × slots_per_epoch`.
    pub slots_per_epoch: u64,
}

impl Default for WalkbackConfig {
    fn default() -> Self {
        Self {
            max_walkback_slots: DEFAULT_MAX_WALKBACK_SLOTS,
            slots_per_epoch: DEFAULT_SLOTS_PER_EPOCH,
        }
    }
}

/// Outcome of a walk-back attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalkbackOutcome {
    /// Stack imported oldest-first; chain is contiguous to the tip.
    Filled {
        /// Blocks newly imported (excluding the original tip if it was re-imported).
        imported: u64,
        /// Depth walked (parents fetched).
        depth: u64,
    },
    /// Hit both limits without finding a known ancestor.
    Abandoned {
        /// How deep we walked on the last attempt.
        depth: u64,
        /// Tip root that could not be connected.
        tip_root: Vec<u8>,
        tip_slot: u64,
    },
}

/// Optional concurrency / order probe for offline tests.
#[derive(Debug, Default)]
pub(crate) struct WalkbackProbe {
    /// Peak concurrent `ImportBlock` calls (must stay ≤ 1).
    pub peak_imports: AtomicUsize,
    inflight_imports: AtomicUsize,
    /// Roots imported during walk-back, oldest-first order.
    pub import_order: std::sync::Mutex<Vec<Vec<u8>>>,
}

impl WalkbackProbe {
    fn enter_import(&self) {
        let cur = self.inflight_imports.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_imports.fetch_max(cur, Ordering::SeqCst);
    }

    fn leave_import(&self) {
        self.inflight_imports.fetch_sub(1, Ordering::SeqCst);
    }

    fn push_root(&self, root: Vec<u8>) {
        self.import_order
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(root);
    }

    #[cfg(test)]
    fn import_order(&self) -> Vec<Vec<u8>> {
        self.import_order
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// Error from walk-back (API / chain / join).
#[derive(Debug)]
pub(crate) enum WalkbackError {
    Api(ApiError),
    Chain(tonic::Status),
}

impl std::fmt::Display for WalkbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "walk-back api: {e}"),
            Self::Chain(e) => write!(f, "walk-back chain: {e}"),
        }
    }
}

impl std::error::Error for WalkbackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Api(e) => Some(e),
            Self::Chain(e) => Some(e),
        }
    }
}

impl From<CatchupError> for WalkbackError {
    fn from(e: CatchupError) -> Self {
        match e {
            CatchupError::Api(a) => Self::Api(a),
            CatchupError::Chain(c) => Self::Chain(c),
            CatchupError::Join(j) => Self::Chain(tonic::Status::internal(format!("join: {j}"))),
        }
    }
}

/// Walk back from `tip` (already known to have returned `UNKNOWN_PARENT` or
/// about to be imported) and import oldest-first.
///
/// Tries `max_walkback_slots` first, then escalates to `4 × slots_per_epoch`
/// on failure before abandoning (§9.4).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn walk_back_and_import<I: BlockImporter>(
    pool: &mut ProviderPool,
    importer: &mut I,
    tip: FetchedBlock,
    cfg: &WalkbackConfig,
    counts: &mut ImportResultCounts,
    on_result: Option<&OnImportResult>,
    catchup_probe: Option<&Arc<CatchupProbe>>,
    walk_probe: Option<&Arc<WalkbackProbe>>,
) -> Result<WalkbackOutcome, WalkbackError> {
    let escalated = cfg.slots_per_epoch.saturating_mul(4);
    let mut limits = vec![cfg.max_walkback_slots.max(1)];
    if escalated > limits[0] {
        limits.push(escalated);
    }

    let tip_root = tip.root.clone();
    let tip_slot = tip.slot;
    let mut last_depth = 0u64;

    for (attempt, &limit) in limits.iter().enumerate() {
        match walk_once(
            pool,
            importer,
            tip.clone(),
            limit,
            counts,
            on_result,
            catchup_probe,
            walk_probe,
        )
        .await?
        {
            OnceResult::Filled { imported, depth } => {
                info!(
                    tip_slot,
                    depth,
                    imported,
                    attempt,
                    limit,
                    "walk-back filled gap"
                );
                return Ok(WalkbackOutcome::Filled { imported, depth });
            }
            OnceResult::HitLimit { depth } => {
                last_depth = depth;
                warn!(
                    tip_slot,
                    depth,
                    limit,
                    attempt,
                    "walk-back hit depth limit"
                );
            }
        }
    }

    error!(
        tip_slot,
        tip_root = %encode_root_hex(&tip_root),
        depth = last_depth,
        "walk-back abandoned; continuing forward polls"
    );
    Ok(WalkbackOutcome::Abandoned {
        depth: last_depth,
        tip_root,
        tip_slot,
    })
}

enum OnceResult {
    Filled { imported: u64, depth: u64 },
    HitLimit { depth: u64 },
}

#[allow(clippy::too_many_arguments)]
async fn walk_once<I: BlockImporter>(
    pool: &mut ProviderPool,
    importer: &mut I,
    tip: FetchedBlock,
    limit: u64,
    counts: &mut ImportResultCounts,
    on_result: Option<&OnImportResult>,
    catchup_probe: Option<&Arc<CatchupProbe>>,
    walk_probe: Option<&Arc<WalkbackProbe>>,
) -> Result<OnceResult, WalkbackError> {
    // Stack of blocks to import oldest-first (tip is deepest child).
    let mut stack: Vec<FetchedBlock> = vec![tip];
    let mut depth = 0u64;
    let mut found_anchor = false;

    while depth < limit {
        let Some(top) = stack.last() else {
            break;
        };
        let parent_root = top.parent_root.clone();
        if parent_root.iter().all(|&b| b == 0) {
            // Genesis parent — treat as known boundary.
            found_anchor = true;
            break;
        }

        let id = encode_root_hex(&parent_root);
        let parent = match pool
            .call(|c| {
                let id = id.clone();
                async move { c.fetch_by_id(&id).await }
            })
            .await
        {
            Ok(p) => p,
            // Terminal 404/4xx: parent is gone for good — stop climb so abandon
            // can run (SEC-1Ab-1). Do not convert into a fatal steady exit.
            Err(e) if e.is_terminal_http() => {
                warn!(
                    parent = %id,
                    depth,
                    error = %e,
                    "walk-back parent fetch terminal; treating as depth limit"
                );
                return Ok(OnceResult::HitLimit { depth });
            }
            Err(e) => return Err(WalkbackError::Api(e)),
        };

        depth += 1;
        debug!(
            slot = parent.slot,
            root = %encode_root_hex(&parent.root),
            depth,
            "walk-back fetched parent"
        );

        // Probe: does chain already have this parent?
        let verdict = import_block_tracked(importer, &parent, catchup_probe, walk_probe).await?;
        counts.record(verdict);
        if let Some(cb) = on_result {
            cb(verdict);
        }

        match verdict {
            ImportBlockVerdict::Duplicate => {
                // Known ancestor — stop climbing; do not push (already in chain).
                found_anchor = true;
                break;
            }
            ImportBlockVerdict::Imported => {
                // Parent landed (its parent was known). Still stop climbing —
                // the stack below can now attach.
                if let Some(p) = walk_probe {
                    p.push_root(parent.root.clone());
                }
                found_anchor = true;
                break;
            }
            ImportBlockVerdict::UnknownParent => {
                // Need to climb further.
                stack.push(parent);
            }
            other => {
                warn!(?other, "walk-back parent import unexpected verdict");
                stack.push(parent);
            }
        }
    }

    if !found_anchor {
        return Ok(OnceResult::HitLimit { depth });
    }

    // Pop stack oldest-first: reverse order of push (tip was first, parents on top).
    let mut imported = 0u64;
    while let Some(block) = stack.pop() {
        let verdict = import_block_tracked(importer, &block, catchup_probe, walk_probe).await?;
        counts.record(verdict);
        if let Some(cb) = on_result {
            cb(verdict);
        }
        match verdict {
            ImportBlockVerdict::Imported => {
                if let Some(p) = walk_probe {
                    p.push_root(block.root.clone());
                }
                imported += 1;
            }
            ImportBlockVerdict::Duplicate => {
                // Already present (e.g. tip retried after partial fill).
            }
            ImportBlockVerdict::UnknownParent => {
                // Should not happen after a successful climb — treat as failure.
                warn!(
                    slot = block.slot,
                    "walk-back oldest-first still UNKNOWN_PARENT"
                );
                return Ok(OnceResult::HitLimit { depth });
            }
            other => {
                warn!(slot = block.slot, ?other, "walk-back import non-success");
            }
        }
    }

    Ok(OnceResult::Filled { imported, depth })
}

async fn import_block_tracked<I: BlockImporter>(
    importer: &mut I,
    block: &FetchedBlock,
    catchup_probe: Option<&Arc<CatchupProbe>>,
    walk_probe: Option<&Arc<WalkbackProbe>>,
) -> Result<ImportBlockVerdict, WalkbackError> {
    if let Some(p) = walk_probe {
        p.enter_import();
    }
    let result = import_with_backpressure(importer, block, catchup_probe).await;
    if let Some(p) = walk_probe {
        p.leave_import();
    }
    result.map_err(WalkbackError::from)
}

/// Convenience: walk-back using a single client (wraps a one-entry pool).
#[cfg(test)]
pub(crate) async fn walk_back_single<I: BlockImporter>(
    api: &crate::api::BeaconApiClient,
    importer: &mut I,
    tip: FetchedBlock,
    cfg: &WalkbackConfig,
    counts: &mut ImportResultCounts,
    walk_probe: Option<&Arc<WalkbackProbe>>,
) -> Result<WalkbackOutcome, WalkbackError> {
    let mut pool = ProviderPool::new(
        &[api.base().to_owned()],
        6,
        3,
        crate::ratelimit::RateLimitMetrics::default(),
    )
    .map_err(WalkbackError::Api)?;
    walk_back_and_import(
        &mut pool,
        importer,
        tip,
        cfg,
        counts,
        None,
        None,
        walk_probe,
    )
    .await
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::api::{BeaconApiClient, parse_root_hex};
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::body::Bytes as HyperBytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::collections::{HashMap, HashSet};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct StubBlock {
        slot: u64,
        root: Vec<u8>,
        parent_root: Vec<u8>,
        ssz: Vec<u8>,
    }

    struct StubState {
        /// Indexed by root and by slot.
        by_root: HashMap<Vec<u8>, StubBlock>,
        by_slot: HashMap<u64, StubBlock>,
        head_root: std::sync::Mutex<Vec<u8>>,
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
                        async move { handle(req, state).await }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        tokio::task::yield_now().await;
        addr
    }

    async fn handle(
        req: Request<hyper::body::Incoming>,
        state: Arc<StubState>,
    ) -> Result<Response<Full<HyperBytes>>, Infallible> {
        if req.method() != Method::GET {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(HyperBytes::new()))
                .unwrap());
        }
        let path = req.uri().path().to_owned();

        if let Some(id) = path.strip_prefix("/eth/v1/beacon/headers/") {
            let block = lookup(&state, id);
            return Ok(match block {
                Some(b) => header_json(&b),
                None => not_found(),
            });
        }
        if let Some(id) = path.strip_prefix("/eth/v2/beacon/blocks/") {
            let block = lookup(&state, id);
            return Ok(match block {
                Some(b) => block_ssz(&b),
                None => not_found(),
            });
        }
        Ok(not_found())
    }

    fn lookup(state: &StubState, id: &str) -> Option<StubBlock> {
        if id == "head" {
            let head = state.head_root.lock().unwrap_or_else(|e| e.into_inner());
            return state.by_root.get(&*head).cloned();
        }
        if let Ok(slot) = id.parse::<u64>() {
            return state.by_slot.get(&slot).cloned();
        }
        if let Ok(root) = parse_root_hex(id) {
            return state.by_root.get(&root).cloned();
        }
        None
    }

    fn header_json(b: &StubBlock) -> Response<Full<HyperBytes>> {
        let root = encode_root_hex(&b.root);
        let parent = encode_root_hex(&b.parent_root);
        let body = format!(
            r#"{{"data":{{"root":"{root}","canonical":true,"header":{{"message":{{"slot":"{}","proposer_index":"0","parent_root":"{parent}","state_root":"0x{}","body_root":"0x{}"}},"signature":"0x00"}}}}}}"#,
            b.slot,
            "00".repeat(32),
            "00".repeat(32),
        );
        Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(HyperBytes::from(body)))
            .unwrap()
    }

    fn block_ssz(b: &StubBlock) -> Response<Full<HyperBytes>> {
        Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
            .header("eth-consensus-version", "fulu")
            .body(Full::new(HyperBytes::from(b.ssz.clone())))
            .unwrap()
    }

    fn not_found() -> Response<Full<HyperBytes>> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(HyperBytes::new()))
            .unwrap()
    }

    struct MockImporter {
        known: HashSet<Vec<u8>>,
        /// ssz → (slot, root, parent)
        by_ssz: HashMap<Vec<u8>, (u64, Vec<u8>, Vec<u8>)>,
        order: Vec<u64>,
        peak: AtomicUsize,
        inflight: AtomicUsize,
    }

    impl MockImporter {
        fn new(anchor: Vec<u8>) -> Self {
            let mut known = HashSet::new();
            known.insert(anchor);
            Self {
                known,
                by_ssz: HashMap::new(),
                order: Vec::new(),
                peak: AtomicUsize::new(0),
                inflight: AtomicUsize::new(0),
            }
        }

        fn register(&mut self, b: &StubBlock) {
            self.by_ssz
                .insert(b.ssz.clone(), (b.slot, b.root.clone(), b.parent_root.clone()));
        }
    }

    impl BlockImporter for MockImporter {
        async fn import_block(
            &mut self,
            request: cc_proto::chain::ImportBlockRequest,
        ) -> Result<cc_proto::chain::ImportBlockResponse, tonic::Status> {
            let cur = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(cur, Ordering::SeqCst);
            // Yield so overlapping calls (if any) can race.
            tokio::task::yield_now().await;
            let (slot, root, parent) = self
                .by_ssz
                .get(&request.ssz)
                .cloned()
                .unwrap_or((0, request.root.clone(), vec![]));
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
            Ok(cc_proto::chain::ImportBlockResponse {
                verdict: verdict as i32,
                reason: String::new(),
            })
        }
    }

    fn root(b: u8) -> Vec<u8> {
        vec![b; 32]
    }

    fn mk_block(slot: u64, r: u8, parent: Vec<u8>) -> StubBlock {
        let root = root(r);
        StubBlock {
            slot,
            root: root.clone(),
            parent_root: parent,
            ssz: vec![0xAB, r, slot as u8],
        }
    }

    fn fetched(b: &StubBlock) -> FetchedBlock {
        FetchedBlock {
            slot: b.slot,
            root: b.root.clone(),
            parent_root: b.parent_root.clone(),
            ssz: Bytes::from(b.ssz.clone()),
            fork: 6,
        }
    }

    /// Linear chain A(anchor)→B→C→D→E→F; importer only knows A..C; tip=F.
    /// Simulates a 3-slot pause (missed D,E before seeing F).
    #[tokio::test]
    async fn paused_three_slots_recovered_no_gap() {
        let anchor = root(0xAA);
        let b = mk_block(1, 0x01, anchor.clone());
        let c = mk_block(2, 0x02, b.root.clone());
        let d = mk_block(3, 0x03, c.root.clone());
        let e = mk_block(4, 0x04, d.root.clone());
        let f = mk_block(5, 0x05, e.root.clone());

        let mut by_root = HashMap::new();
        let mut by_slot = HashMap::new();
        for blk in [&b, &c, &d, &e, &f] {
            by_root.insert(blk.root.clone(), blk.clone());
            by_slot.insert(blk.slot, blk.clone());
        }
        let state = Arc::new(StubState {
            by_root,
            by_slot,
            head_root: std::sync::Mutex::new(f.root.clone()),
        });
        let addr = spawn_stub(state).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();

        let mut importer = MockImporter::new(anchor);
        // Already imported B and C (pre-pause).
        for blk in [&b, &c] {
            importer.register(blk);
            let v = import_with_backpressure(&mut importer, &fetched(blk), None)
                .await
                .unwrap();
            assert_eq!(v, ImportBlockVerdict::Imported);
        }
        for blk in [&d, &e, &f] {
            importer.register(blk);
        }

        // Tip F → UNKNOWN_PARENT
        let tip = fetched(&f);
        let v = import_with_backpressure(&mut importer, &tip, None)
            .await
            .unwrap();
        assert_eq!(v, ImportBlockVerdict::UnknownParent);

        let probe = Arc::new(WalkbackProbe::default());
        let mut counts = ImportResultCounts::default();
        let outcome = walk_back_single(
            &api,
            &mut importer,
            tip,
            &WalkbackConfig::default(),
            &mut counts,
            Some(&probe),
        )
        .await
        .unwrap();

        match outcome {
            WalkbackOutcome::Filled { imported, .. } => {
                // D may land during the climb (parent C known → IMPORTED) and is
                // then not re-counted on the stack pop; E+F still pop-import.
                assert!(imported >= 2, "E,F must import via stack, got {imported}");
            }
            WalkbackOutcome::Abandoned { .. } => panic!("must not abandon"),
        }

        // Parent-linkage walk: known set contains full chain B..F (no gap).
        for blk in [&b, &c, &d, &e, &f] {
            assert!(
                importer.known.contains(&blk.root),
                "missing root slot {}",
                blk.slot
            );
        }
        // Imported order among walk-back fills is oldest-first: D then E then F.
        let order = &importer.order;
        assert_eq!(&order[order.len().saturating_sub(3)..], &[3, 4, 5]);
        assert!(probe.peak_imports.load(Ordering::SeqCst) <= 1);
    }

    /// Reorg: A→B→C imported; head switches to A→B'→C'. Same walk-back path.
    #[tokio::test]
    async fn reorg_sibling_branch_oldest_first() {
        let anchor = root(0x00);
        let b = mk_block(1, 0x10, anchor.clone());
        let c = mk_block(2, 0x11, b.root.clone());
        // Sibling branch off A:
        let b2 = mk_block(1, 0x20, anchor.clone());
        let c2 = mk_block(2, 0x21, b2.root.clone());

        let mut by_root = HashMap::new();
        let mut by_slot = HashMap::new();
        for blk in [&b, &c, &b2, &c2] {
            by_root.insert(blk.root.clone(), blk.clone());
            // slot index: last write wins — head lookup uses root, not slot.
            by_slot.insert(blk.slot, blk.clone());
        }
        let state = Arc::new(StubState {
            by_root,
            by_slot,
            head_root: std::sync::Mutex::new(c2.root.clone()),
        });
        let addr = spawn_stub(state).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();

        let mut importer = MockImporter::new(anchor);
        for blk in [&b, &c, &b2, &c2] {
            importer.register(blk);
        }
        for blk in [&b, &c] {
            let v = import_with_backpressure(&mut importer, &fetched(blk), None)
                .await
                .unwrap();
            assert_eq!(v, ImportBlockVerdict::Imported);
        }

        // New head C' — parent B' unknown.
        let tip = fetched(&c2);
        let v = import_with_backpressure(&mut importer, &tip, None)
            .await
            .unwrap();
        assert_eq!(v, ImportBlockVerdict::UnknownParent);

        let probe = Arc::new(WalkbackProbe::default());
        let mut counts = ImportResultCounts::default();
        let outcome = walk_back_single(
            &api,
            &mut importer,
            tip,
            &WalkbackConfig::default(),
            &mut counts,
            Some(&probe),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, WalkbackOutcome::Filled { .. }));
        assert!(importer.known.contains(&b2.root));
        assert!(importer.known.contains(&c2.root));
        // Oldest-first: B' before C' (slots collide, so order by root probe).
        let roots = probe.import_order();
        let i_b2 = roots.iter().position(|r| r == &b2.root);
        let i_c2 = roots.iter().position(|r| r == &c2.root);
        assert!(i_b2.is_some() && i_c2.is_some());
        assert!(i_b2 < i_c2, "B' must import before C'");
        assert!(probe.peak_imports.load(Ordering::SeqCst) <= 1);
    }

    #[tokio::test]
    async fn abandon_when_limit_too_shallow() {
        let anchor = root(0xFF);
        // Chain of 10 blocks; importer only knows anchor.
        // max=2 then escalated 4×1=4 — both < 10 parents → abandon.
        let mut blocks = Vec::new();
        let mut parent = anchor.clone();
        for i in 1..=10u8 {
            let b = mk_block(i as u64, i, parent.clone());
            parent = b.root.clone();
            blocks.push(b);
        }
        let tip_blk = blocks.last().unwrap().clone();

        let mut by_root = HashMap::new();
        let mut by_slot = HashMap::new();
        for blk in &blocks {
            by_root.insert(blk.root.clone(), blk.clone());
            by_slot.insert(blk.slot, blk.clone());
        }
        let state = Arc::new(StubState {
            by_root,
            by_slot,
            head_root: std::sync::Mutex::new(tip_blk.root.clone()),
        });
        let addr = spawn_stub(state).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();
        let mut importer = MockImporter::new(anchor);
        for blk in &blocks {
            importer.register(blk);
        }

        let mut counts = ImportResultCounts::default();
        let outcome = walk_back_single(
            &api,
            &mut importer,
            fetched(&tip_blk),
            &WalkbackConfig {
                max_walkback_slots: 2,
                slots_per_epoch: 1, // escalated = 4; still < 10
            },
            &mut counts,
            None,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, WalkbackOutcome::Abandoned { .. }),
            "got {outcome:?}"
        );
    }

    /// Parent root 404s permanently — pool must not infinite-retry; walk-back
    /// abandons so steady can keep polling forward (SEC-1Ab-1).
    #[tokio::test]
    async fn parent_404_abandons_without_hang() {
        let anchor = root(0xAA);
        // Tip's parent is a synthetic root that the stub does not serve → 404.
        let missing_parent = root(0xDE);
        let tip = mk_block(5, 0x05, missing_parent);

        let mut by_root = HashMap::new();
        let mut by_slot = HashMap::new();
        by_root.insert(tip.root.clone(), tip.clone());
        by_slot.insert(tip.slot, tip.clone());
        let state = Arc::new(StubState {
            by_root,
            by_slot,
            head_root: std::sync::Mutex::new(tip.root.clone()),
        });
        let addr = spawn_stub(state).await;
        let api = BeaconApiClient::new(format!("http://{addr}"), 6).unwrap();
        let mut importer = MockImporter::new(anchor);
        importer.register(&tip);

        let mut counts = ImportResultCounts::default();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            walk_back_single(
                &api,
                &mut importer,
                fetched(&tip),
                &WalkbackConfig {
                    max_walkback_slots: 8,
                    slots_per_epoch: 8,
                },
                &mut counts,
                None,
            ),
        )
        .await
        .expect("must not hang on parent 404")
        .unwrap();

        assert!(
            matches!(outcome, WalkbackOutcome::Abandoned { .. }),
            "parent 404 must abandon, got {outcome:?}"
        );
        // Tip must not have been force-imported without its parent.
        assert!(!importer.known.contains(&tip.root));
    }
}
