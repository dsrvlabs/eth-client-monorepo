//! Steady-state head following (Architecture §9.3, CC-1Ab).
//!
//! Slot boundaries come from `genesis_time` (fetched once). Within each slot the
//! driver polls `/eth/v1/beacon/headers/head` at **+4 s, +8 s, and +11 s**,
//! stopping early once that slot's block is seen. Exactly one `ImportBlock` is
//! in flight at a time — the single-task loop is the sequencing guarantee (R-6).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_proto::chain::ImportBlockVerdict;
use tracing::{debug, info, warn};

use crate::api::{BeaconApiClient, FetchedBlock, encode_root_hex};
use crate::catchup::{
    BlockImporter, CatchupProbe, ImportResultCounts, OnImportResult, import_with_backpressure,
};
use crate::ratelimit::ProviderPool;
use crate::walkback::{WalkbackConfig, WalkbackOutcome, WalkbackProbe, walk_back_and_import};

/// Mainnet-shaped defaults.
pub(crate) const DEFAULT_SECONDS_PER_SLOT: u64 = 12;
/// Poll offsets into the slot (seconds), §9.3.
pub(crate) const DEFAULT_POLL_OFFSETS_SECS: [u64; 3] = [4, 8, 11];

/// Steady-state configuration.
#[derive(Debug, Clone)]
pub(crate) struct SteadyConfig {
    /// Unix genesis time (seconds), from `/eth/v1/beacon/genesis`.
    pub genesis_time: u64,
    /// Slot duration in seconds (default 12).
    pub seconds_per_slot: u64,
    /// Poll offsets into each slot (default +4 / +8 / +11).
    pub poll_offsets_secs: Vec<u64>,
    /// Walk-back limits.
    pub walkback: WalkbackConfig,
    /// Optional: stop after processing this many wall-clock slots (tests).
    pub max_slots: Option<u64>,
    /// Optional: absolute unix deadline (tests / bounded runs).
    pub deadline: Option<SystemTime>,
}

/// Per-slot record for soak reporting (CC-1A/2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotRecord {
    pub slot: u64,
    /// True if a head block for this slot was imported (or duplicate) in-slot.
    pub seen_in_slot: bool,
    /// Wall time from slot start to first successful import of this slot's root.
    pub import_delay: Option<Duration>,
}

/// Aggregate steady-state report.
#[derive(Debug, Clone, Default)]
pub(crate) struct SteadyReport {
    pub slots: Vec<SlotRecord>,
    pub blocks_imported: u64,
    pub gaps_abandoned: u64,
    pub unknown_parent: u64,
}

impl SteadyReport {
    /// Fraction of recorded slots that saw their block in-slot.
    pub(crate) fn in_slot_ratio(&self) -> f64 {
        if self.slots.is_empty() {
            return 1.0;
        }
        let ok = self.slots.iter().filter(|s| s.seen_in_slot).count();
        ok as f64 / self.slots.len() as f64
    }

    /// Slots that missed in-slot import.
    pub(crate) fn miss_list(&self) -> Vec<u64> {
        self.slots
            .iter()
            .filter(|s| !s.seen_in_slot)
            .map(|s| s.slot)
            .collect()
    }
}

/// Instrumentation for offline tests (single in-flight, pause, ordering).
#[derive(Debug, Default)]
pub(crate) struct SteadyProbe {
    pub peak_imports: AtomicUsize,
    inflight_imports: AtomicUsize,
    /// When true, the loop sleeps without polling (simulated pause).
    pub paused: AtomicBool,
    /// Roots imported in order.
    pub imported_roots: std::sync::Mutex<Vec<Vec<u8>>>,
    /// Number of head polls performed.
    pub head_polls: AtomicUsize,
}

impl SteadyProbe {
    fn enter_import(&self) {
        let cur = self.inflight_imports.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_imports.fetch_max(cur, Ordering::SeqCst);
    }

    fn leave_import(&self) {
        self.inflight_imports.fetch_sub(1, Ordering::SeqCst);
    }

    fn push_root(&self, root: Vec<u8>) {
        self.imported_roots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(root);
    }
}

/// Callback when a gap is abandoned (`cc_driver_gap_abandoned_total`).
pub(crate) type OnGapAbandoned = Arc<dyn Fn() + Send + Sync>;

/// Run the steady-state poll loop until `max_slots`, `deadline`, or forever.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_steady_state<I: BlockImporter>(
    pool: &mut ProviderPool,
    importer: &mut I,
    cfg: SteadyConfig,
    counts: &mut ImportResultCounts,
    on_result: Option<OnImportResult>,
    on_gap_abandoned: Option<OnGapAbandoned>,
    catchup_probe: Option<Arc<CatchupProbe>>,
    walk_probe: Option<Arc<WalkbackProbe>>,
    steady_probe: Option<Arc<SteadyProbe>>,
) -> Result<SteadyReport, SteadyError> {
    let mut report = SteadyReport::default();
    let mut last_root: Option<Vec<u8>> = None;
    let seconds_per_slot = cfg.seconds_per_slot.max(1);
    let offsets = if cfg.poll_offsets_secs.is_empty() {
        DEFAULT_POLL_OFFSETS_SECS.to_vec()
    } else {
        cfg.poll_offsets_secs.clone()
    };

    // Align to the current wall-clock slot.
    let mut slot = current_slot(cfg.genesis_time, seconds_per_slot);
    let start_slot = slot;
    info!(
        genesis_time = cfg.genesis_time,
        seconds_per_slot,
        start_slot,
        offsets = ?offsets,
        "steady state starting"
    );

    loop {
        if let Some(max) = cfg.max_slots
            && slot.saturating_sub(start_slot) >= max
        {
            break;
        }
        if let Some(dl) = cfg.deadline
            && SystemTime::now() >= dl
        {
            break;
        }

        // Honour test pause: advance wall time without polling.
        if steady_probe
            .as_ref()
            .is_some_and(|p| p.paused.load(Ordering::SeqCst))
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
            slot = current_slot(cfg.genesis_time, seconds_per_slot);
            continue;
        }

        let slot_start = slot_start_instant(cfg.genesis_time, slot, seconds_per_slot);
        let mut seen_in_slot = false;
        let mut import_delay = None;

        for &offset in &offsets {
            // Sleep until poll time (or skip if already past).
            let target = slot_start + Duration::from_secs(offset.min(seconds_per_slot.saturating_sub(1)));
            sleep_until(target).await;

            // Slot rolled over while we waited.
            let now_slot = current_slot(cfg.genesis_time, seconds_per_slot);
            if now_slot > slot {
                break;
            }

            if steady_probe
                .as_ref()
                .is_some_and(|p| p.paused.load(Ordering::SeqCst))
            {
                break;
            }

            let head = match pool
                .call(|c| async move { c.get_head_header().await })
                .await
            {
                Ok(h) => h,
                Err(e) => {
                    warn!(error = %e, "steady head poll failed");
                    continue;
                }
            };
            if let Some(p) = steady_probe.as_ref() {
                p.head_polls.fetch_add(1, Ordering::SeqCst);
            }

            if last_root.as_ref().is_some_and(|r| r == &head.root) {
                // Already handled this head (imported, duplicate, or sticky abandon).
                if head.slot == slot {
                    seen_in_slot = true;
                }
                continue;
            }

            // New head root — fetch full block and import (single in-flight).
            let id = encode_root_hex(&head.root);
            let block = match pool
                .call(|c| {
                    let id = id.clone();
                    async move { c.fetch_by_id(&id).await }
                })
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, root = %id, "steady block fetch failed");
                    continue;
                }
            };

            let outcome = import_head(
                pool,
                importer,
                block,
                &cfg.walkback,
                counts,
                on_result.as_ref(),
                on_gap_abandoned.as_ref(),
                catchup_probe.as_ref(),
                walk_probe.as_ref(),
                steady_probe.as_ref(),
            )
            .await?;

            match outcome {
                HeadImport::Applied { root, slot: s } => {
                    last_root = Some(root.clone());
                    report.blocks_imported += 1;
                    if let Some(p) = steady_probe.as_ref() {
                        p.push_root(root);
                    }
                    if s == slot {
                        seen_in_slot = true;
                        let delay = unix_now()
                            .duration_since(slot_start)
                            .unwrap_or(Duration::ZERO);
                        import_delay = Some(delay);
                    }
                }
                HeadImport::Duplicate { root, slot: s } => {
                    last_root = Some(root);
                    if s == slot {
                        seen_in_slot = true;
                    }
                }
                HeadImport::Abandoned { root } => {
                    // Sticky tip: do not re-walk / re-increment on every poll.
                    last_root = Some(root);
                    report.gaps_abandoned += 1;
                }
                HeadImport::Ignored => {}
            }

            if seen_in_slot {
                break; // stop early once this slot's block is seen
            }
        }

        report.slots.push(SlotRecord {
            slot,
            seen_in_slot,
            import_delay,
        });
        report.unknown_parent = counts.unknown_parent;

        // Sleep until next slot boundary if we still have time.
        let next_start =
            slot_start_instant(cfg.genesis_time, slot.saturating_add(1), seconds_per_slot);
        sleep_until(next_start).await;
        slot = current_slot(cfg.genesis_time, seconds_per_slot).max(slot.saturating_add(1));
    }

    info!(
        slots = report.slots.len(),
        in_slot_ratio = report.in_slot_ratio(),
        misses = ?report.miss_list(),
        blocks = report.blocks_imported,
        abandoned = report.gaps_abandoned,
        "steady state finished"
    );
    Ok(report)
}

enum HeadImport {
    Applied { root: Vec<u8>, slot: u64 },
    Duplicate { root: Vec<u8>, slot: u64 },
    /// Walk-back abandoned; `root` is sticky so the same tip is not re-entered.
    Abandoned { root: Vec<u8> },
    Ignored,
}

#[allow(clippy::too_many_arguments)]
async fn import_head<I: BlockImporter>(
    pool: &mut ProviderPool,
    importer: &mut I,
    block: FetchedBlock,
    walkback_cfg: &WalkbackConfig,
    counts: &mut ImportResultCounts,
    on_result: Option<&OnImportResult>,
    on_gap_abandoned: Option<&OnGapAbandoned>,
    catchup_probe: Option<&Arc<CatchupProbe>>,
    walk_probe: Option<&Arc<WalkbackProbe>>,
    steady_probe: Option<&Arc<SteadyProbe>>,
) -> Result<HeadImport, SteadyError> {
    if let Some(p) = steady_probe {
        p.enter_import();
    }
    let verdict = import_with_backpressure(importer, &block, catchup_probe)
        .await
        .map_err(SteadyError::from)?;
    if let Some(p) = steady_probe {
        p.leave_import();
    }
    counts.record(verdict);
    if let Some(cb) = on_result {
        cb(verdict);
    }

    match verdict {
        ImportBlockVerdict::Imported => Ok(HeadImport::Applied {
            root: block.root,
            slot: block.slot,
        }),
        ImportBlockVerdict::Duplicate => Ok(HeadImport::Duplicate {
            root: block.root,
            slot: block.slot,
        }),
        ImportBlockVerdict::UnknownParent => {
            debug!(
                slot = block.slot,
                root = %encode_root_hex(&block.root),
                "UNKNOWN_PARENT — entering walk-back"
            );
            let outcome = walk_back_and_import(
                pool,
                importer,
                block.clone(),
                walkback_cfg,
                counts,
                on_result,
                catchup_probe,
                walk_probe,
            )
            .await
            .map_err(SteadyError::from)?;
            match outcome {
                WalkbackOutcome::Filled { .. } => {
                    // Tip should now be in chain (imported oldest-first).
                    Ok(HeadImport::Applied {
                        root: block.root,
                        slot: block.slot,
                    })
                }
                WalkbackOutcome::Abandoned { tip_root, .. } => {
                    if let Some(cb) = on_gap_abandoned {
                        cb();
                    }
                    Ok(HeadImport::Abandoned { root: tip_root })
                }
            }
        }
        other => {
            warn!(?other, slot = block.slot, "steady import non-success");
            Ok(HeadImport::Ignored)
        }
    }
}

/// Current slot from wall clock.
pub(crate) fn current_slot(genesis_time: u64, seconds_per_slot: u64) -> u64 {
    let now = unix_now_secs();
    if now <= genesis_time {
        return 0;
    }
    (now - genesis_time) / seconds_per_slot.max(1)
}

fn slot_start_instant(genesis_time: u64, slot: u64, seconds_per_slot: u64) -> SystemTime {
    let secs = genesis_time.saturating_add(slot.saturating_mul(seconds_per_slot));
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn unix_now() -> SystemTime {
    SystemTime::now()
}

fn unix_now_secs() -> u64 {
    unix_now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn sleep_until(target: SystemTime) {
    let now = SystemTime::now();
    if let Ok(dur) = target.duration_since(now)
        && dur > Duration::ZERO
    {
        tokio::time::sleep(dur).await;
    }
}

/// Errors from the steady loop.
#[derive(Debug)]
pub(crate) enum SteadyError {
    Walkback(crate::walkback::WalkbackError),
    Catchup(crate::catchup::CatchupError),
}

impl std::fmt::Display for SteadyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Walkback(e) => write!(f, "steady walkback: {e}"),
            Self::Catchup(e) => write!(f, "steady catchup: {e}"),
        }
    }
}

impl std::error::Error for SteadyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Walkback(e) => Some(e),
            Self::Catchup(e) => Some(e),
        }
    }
}

impl From<crate::catchup::CatchupError> for SteadyError {
    fn from(e: crate::catchup::CatchupError) -> Self {
        Self::Catchup(e)
    }
}

impl From<crate::walkback::WalkbackError> for SteadyError {
    fn from(e: crate::walkback::WalkbackError) -> Self {
        Self::Walkback(e)
    }
}

/// Fetch genesis once via the provider pool.
pub(crate) async fn fetch_genesis_time(pool: &mut ProviderPool) -> Result<u64, crate::api::ApiError> {
    let g = pool
        .call(|c: BeaconApiClient| async move { c.get_genesis().await })
        .await?;
    Ok(g.genesis_time)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::api::parse_root_hex;
    use crate::ratelimit::RateLimitMetrics;
    use crate::walkback::WalkbackConfig;
    use http_body_util::Full;
    use hyper::body::Bytes as HyperBytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::collections::{HashMap, HashSet};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct StubBlock {
        slot: u64,
        root: Vec<u8>,
        parent_root: Vec<u8>,
        ssz: Vec<u8>,
    }

    struct StubState {
        by_root: HashMap<Vec<u8>, StubBlock>,
        by_slot: HashMap<u64, StubBlock>,
        head_slot: AtomicU64,
        genesis_time: u64,
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
        let path = req.uri().path().to_owned();
        if path == "/eth/v1/beacon/genesis" {
            let body = format!(
                r#"{{"data":{{"genesis_time":"{}","genesis_validators_root":"0x{}","genesis_fork_version":"0x00000000"}}}}"#,
                state.genesis_time,
                "11".repeat(32),
            );
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Full::new(HyperBytes::from(body)))
                .unwrap());
        }
        if let Some(id) = path.strip_prefix("/eth/v1/beacon/headers/") {
            let b = lookup(&state, id);
            return Ok(match b {
                Some(b) => header_json(&b),
                None => not_found(),
            });
        }
        if let Some(id) = path.strip_prefix("/eth/v2/beacon/blocks/") {
            let b = lookup(&state, id);
            return Ok(match b {
                Some(b) => block_ssz(&b),
                None => not_found(),
            });
        }
        Ok(not_found())
    }

    fn lookup(state: &StubState, id: &str) -> Option<StubBlock> {
        if id == "head" {
            let slot = state.head_slot.load(Ordering::SeqCst);
            return state.by_slot.get(&slot).cloned();
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
        by_ssz: HashMap<Vec<u8>, (u64, Vec<u8>, Vec<u8>)>,
        peak: AtomicUsize,
        inflight: AtomicUsize,
        order: Vec<u64>,
    }

    impl MockImporter {
        fn new(anchor: Vec<u8>) -> Self {
            let mut known = HashSet::new();
            known.insert(anchor);
            Self {
                known,
                by_ssz: HashMap::new(),
                peak: AtomicUsize::new(0),
                inflight: AtomicUsize::new(0),
                order: Vec::new(),
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

    fn mk(slot: u64, r: u8, parent: Vec<u8>) -> StubBlock {
        StubBlock {
            slot,
            root: root(r),
            parent_root: parent,
            ssz: vec![0xCD, r, slot as u8],
        }
    }

    /// Build a chain of `n` blocks starting at slot 0; returns (blocks, anchor_root).
    fn linear(n: u64) -> (Vec<StubBlock>, Vec<u8>) {
        let anchor = root(0xAA);
        let mut blocks = Vec::new();
        let mut parent = anchor.clone();
        for i in 0..n {
            let b = mk(i, (i + 1) as u8, parent.clone());
            parent = b.root.clone();
            blocks.push(b);
        }
        (blocks, anchor)
    }

    /// Real-time smoke: short slots, poll at 0, max 6 slots, assert peak imports ≤ 1
    /// and recovery after a simulated multi-slot pause.
    #[tokio::test]
    async fn steady_short_slots_single_inflight_and_gap_recovery() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // genesis so current slot is 0
        let genesis = now;
        let seconds_per_slot = 1u64;

        let (blocks, anchor) = linear(6);
        let mut by_root = HashMap::new();
        let mut by_slot = HashMap::new();
        for b in &blocks {
            by_root.insert(b.root.clone(), b.clone());
            by_slot.insert(b.slot, b.clone());
        }
        let state = Arc::new(StubState {
            by_root,
            by_slot,
            head_slot: AtomicU64::new(0),
            genesis_time: genesis,
        });
        // Advance head every second.
        let state_head = Arc::clone(&state);
        tokio::spawn(async move {
            for s in 0..6u64 {
                state_head.head_slot.store(s, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        });

        let addr = spawn_stub(Arc::clone(&state)).await;
        let mut pool = ProviderPool::new(
            &[format!("http://{addr}")],
            6,
            3,
            RateLimitMetrics::default(),
        )
        .unwrap();
        let mut importer = MockImporter::new(anchor);
        for b in &blocks {
            importer.register(b);
        }

        let probe = Arc::new(SteadyProbe::default());
        // Pause for ~3 slots after the first import window to simulate miss.
        let pause_flag = Arc::clone(&probe);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(800)).await;
            pause_flag.paused.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(3200)).await; // ~3 slots
            pause_flag.paused.store(false, Ordering::SeqCst);
        });

        let mut counts = ImportResultCounts::default();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let abandoned_cb = {
            let a = Arc::clone(&abandoned);
            Arc::new(move || {
                a.fetch_add(1, Ordering::SeqCst);
            }) as OnGapAbandoned
        };

        let report = run_steady_state(
            &mut pool,
            &mut importer,
            SteadyConfig {
                genesis_time: genesis,
                seconds_per_slot,
                poll_offsets_secs: vec![0],
                walkback: WalkbackConfig::default(),
                max_slots: Some(6),
                deadline: Some(SystemTime::now() + Duration::from_secs(12)),
            },
            &mut counts,
            None,
            Some(abandoned_cb),
            None,
            None,
            Some(Arc::clone(&probe)),
        )
        .await
        .unwrap();

        assert!(
            probe.peak_imports.load(Ordering::SeqCst) <= 1,
            "exactly one ImportBlock in flight, peak={}",
            probe.peak_imports.load(Ordering::SeqCst)
        );
        assert_eq!(importer.peak.load(Ordering::SeqCst), 1);

        // After pause recovery, parent linkage should be contiguous for imported set.
        // At least some blocks beyond the pause must be known.
        let known_slots: Vec<u64> = importer.order.clone();
        assert!(
            known_slots.len() >= 2,
            "expected imports after recovery, got {known_slots:?}"
        );
        // Parent-linkage: every consecutive pair in import order differs by ≥0 and
        // each imported root's parent is known — mock already enforced this.
        assert!(
            known_slots.windows(2).all(|w| w[0] < w[1]),
            "imports oldest-first by slot: {known_slots:?}"
        );
        assert_eq!(abandoned.load(Ordering::SeqCst), 0);
        assert!(report.gaps_abandoned == 0);
        let _ = report;
    }

    /// Abandoned tip is sticky: same head re-polled across multiple slots
    /// increments gap_abandoned once (not every poll).
    #[tokio::test]
    async fn abandoned_tip_sticky_no_rewalk() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let genesis = now;
        let seconds_per_slot = 1u64;

        // Tip with missing parent (404 on walk-back) — always head.
        let missing_parent = root(0xDE);
        let tip = mk(0, 0x05, missing_parent);
        let mut by_root = HashMap::new();
        let mut by_slot = HashMap::new();
        by_root.insert(tip.root.clone(), tip.clone());
        by_slot.insert(tip.slot, tip.clone());
        let state = Arc::new(StubState {
            by_root,
            by_slot,
            head_slot: AtomicU64::new(0),
            genesis_time: genesis,
        });
        // Head stays on tip for the whole run (slot 0 block remains head).
        let addr = spawn_stub(Arc::clone(&state)).await;
        let mut pool = ProviderPool::new(
            &[format!("http://{addr}")],
            6,
            3,
            RateLimitMetrics::default(),
        )
        .unwrap();
        let mut importer = MockImporter::new(root(0xAA));
        importer.register(&tip);

        let abandoned = Arc::new(AtomicUsize::new(0));
        let abandoned_cb = {
            let a = Arc::clone(&abandoned);
            Arc::new(move || {
                a.fetch_add(1, Ordering::SeqCst);
            }) as OnGapAbandoned
        };

        let report = run_steady_state(
            &mut pool,
            &mut importer,
            SteadyConfig {
                genesis_time: genesis,
                seconds_per_slot,
                poll_offsets_secs: vec![0],
                walkback: WalkbackConfig {
                    max_walkback_slots: 4,
                    slots_per_epoch: 4,
                },
                max_slots: Some(4),
                deadline: Some(SystemTime::now() + Duration::from_secs(8)),
            },
            &mut ImportResultCounts::default(),
            None,
            Some(abandoned_cb),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            abandoned.load(Ordering::SeqCst),
            1,
            "gap_abandoned callback must fire once for a sticky tip"
        );
        assert_eq!(
            report.gaps_abandoned, 1,
            "report must count one abandon, not one per poll/slot"
        );
    }

    #[test]
    fn current_slot_math() {
        let now = unix_now_secs();
        // Far-future genesis → still at slot 0.
        assert_eq!(current_slot(now.saturating_add(10_000), 12), 0);
        // 120s after genesis at 12s/slot → slot 10.
        let genesis = now.saturating_sub(120);
        assert_eq!(current_slot(genesis, 12), 10);
        // 1s slots: 5s after genesis → slot 5.
        let genesis = now.saturating_sub(5);
        assert_eq!(current_slot(genesis, 1), 5);
    }
}
