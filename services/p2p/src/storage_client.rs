//! p2p side of the ninth contract `eth.storage.v1` (CC-4F / Architecture §1.6, §5.5).
//!
//! ## Degradation: `ResourceUnavailable`, never empty success
//!
//! When `storage` is unreachable the serve path **must** answer
//! `3: ResourceUnavailable` for the duration — never an empty success. An empty
//! success when the backend is down is free-riding peers descore for
//! (CC-4F /7).
//!
//! ## `WatchServeWindow` → sole AtomicU64 writer (CC-48 / §5.3)
//!
//! The stream handler is the **only** production site that writes
//! [`ServeWindow::store_recomputed`]. Cache eviction / insert must not.
//! On disconnect, the last value is held for [`StorageClientConfig::window_stale_grace`]
//! then **fail-closed collapsed** to the in-memory cache floor
//! (`cc_p2p_window_collapsed_total`). Reconnect reverses the collapse
//! automatically when the next stream message arrives.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cc_proto::storage::storage_service_client::StorageServiceClient;
use cc_proto::storage::{
    GetBlocksByRangeRequest, GetBlocksByRootRequest, GetBlocksResponse, GetColumnsByRangeRequest,
    GetColumnsByRootRequest, GetColumnsResponse, WatchServeWindowRequest,
};
use cc_types::primitives::Slot;
use futures::StreamExt;
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};
use tracing::{debug, info, warn};

use crate::backfill::{ServeWindow, EMPTY_WINDOW_SLOT};
use crate::metrics::P2pMetrics;

/// Initial reconnect backoff for `WatchServeWindow`.
pub const STORAGE_BACKOFF_INITIAL: Duration = Duration::from_millis(250);
/// Hard cap on reconnect backoff.
pub const STORAGE_BACKOFF_CAP: Duration = Duration::from_secs(10);
/// Default dial timeout.
pub const STORAGE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Default fail-closed grace after stream disconnect (Architecture §5.5).
pub const DEFAULT_WINDOW_STALE_GRACE: Duration = Duration::from_secs(60);

/// Configuration for the storage gRPC client.
#[derive(Debug, Clone)]
pub struct StorageClientConfig {
    /// gRPC URI for `storage` (e.g. `http://127.0.0.1:9006`).
    pub storage_uri: String,
    /// Initial reconnect backoff.
    pub backoff_initial: Duration,
    /// Backoff hard cap.
    pub backoff_cap: Duration,
    /// Connect timeout per dial.
    pub connect_timeout: Duration,
    /// How long to keep the last advertised window after disconnect before
    /// collapsing to the cache floor (`p2p.window_stale_grace`, default 60 s).
    pub window_stale_grace: Duration,
}

impl Default for StorageClientConfig {
    fn default() -> Self {
        Self {
            storage_uri: "http://127.0.0.1:9006".to_owned(),
            backoff_initial: STORAGE_BACKOFF_INITIAL,
            backoff_cap: STORAGE_BACKOFF_CAP,
            connect_timeout: STORAGE_CONNECT_TIMEOUT,
            window_stale_grace: DEFAULT_WINDOW_STALE_GRACE,
        }
    }
}

/// Shared handle: available flag + serve window + cache floor for collapse.
///
/// `available == false` means storage is down / stream disconnected past grace
/// — all serve reads must map to ResourceUnavailable (never empty success).
#[derive(Debug)]
pub struct StorageClientHandle {
    /// Whether the storage backend is currently reachable for serve reads.
    available: AtomicBool,
    /// CC-26a / CC-48 serve window (sole production write site: stream handler + collapse).
    window: Arc<ServeWindow>,
    /// In-memory cache floor for §5.5 collapse (updated by the backfill cache).
    cache_floor: Arc<AtomicU64>,
    /// Whether the advertised window has been collapsed after grace expiry.
    collapsed: AtomicBool,
}

impl StorageClientHandle {
    /// Construct with a shared [`ServeWindow`] (from the handshake / Status path).
    #[must_use]
    pub fn new(window: Arc<ServeWindow>) -> Self {
        Self::with_cache_floor(window, Arc::new(AtomicU64::new(EMPTY_WINDOW_SLOT)))
    }

    /// Construct with an explicit cache-floor atomic (shared with [`crate::backfill::BackfillCache`]).
    #[must_use]
    pub fn with_cache_floor(window: Arc<ServeWindow>, cache_floor: Arc<AtomicU64>) -> Self {
        Self {
            // Start unavailable until the first successful dial / stream msg.
            available: AtomicBool::new(false),
            window,
            cache_floor,
            collapsed: AtomicBool::new(false),
        }
    }

    /// Whether storage is currently reachable.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    /// Mark storage up/down (tests + reconnect loop).
    pub fn set_available(&self, up: bool) {
        self.available.store(up, Ordering::Release);
    }

    /// Shared serve window.
    #[must_use]
    pub fn window(&self) -> &Arc<ServeWindow> {
        &self.window
    }

    /// Shared cache-floor atomic (for the backfill cache to publish into).
    #[must_use]
    pub fn cache_floor(&self) -> &Arc<AtomicU64> {
        &self.cache_floor
    }

    /// Publish a new cache floor (called by the backfill cache on mutation).
    pub fn set_cache_floor(&self, slot: Slot) {
        self.cache_floor.store(slot.as_u64(), Ordering::Release);
    }

    /// Whether the window is currently collapsed to the cache floor.
    #[must_use]
    pub fn is_collapsed(&self) -> bool {
        self.collapsed.load(Ordering::Acquire)
    }

    /// Map a down backend to ResourceUnavailable — **never** an empty success.
    ///
    /// Callers of the four serve reads must invoke this before treating a
    /// transport error as "no data".
    pub fn refuse_if_unavailable(&self) -> Result<(), Status> {
        if self.is_available() {
            Ok(())
        } else {
            Err(Status::unavailable(
                "storage backend unreachable; ResourceUnavailable (never empty success)",
            ))
        }
    }

    /// Apply a stream-derived window update (sole production write path with collapse).
    fn apply_stream_window(&self, slot: Slot, metrics: Option<&P2pMetrics>) {
        self.window.store_recomputed(slot);
        self.collapsed.store(false, Ordering::Release);
        if let Some(m) = metrics {
            m.set_earliest_available_slot(slot.as_u64() as i64);
        }
    }

    /// Fail-closed collapse to the cache floor after grace expiry.
    fn collapse_to_cache_floor(&self, metrics: Option<&P2pMetrics>) {
        let floor = self.cache_floor.load(Ordering::Acquire);
        self.window.store_recomputed(Slot::new(floor));
        self.collapsed.store(true, Ordering::Release);
        if let Some(m) = metrics {
            m.set_earliest_available_slot(floor as i64);
            m.inc_window_collapsed();
        }
        warn!(
            target: "cc_p2p::storage_client",
            cache_floor = floor,
            "WatchServeWindow stale past window_stale_grace; collapsed advertised window to cache floor"
        );
    }
}

/// Thin client for the four serve reads + PutBackfillBatch.
///
/// Holds a lazily-connected channel. On transport failure the handle is marked
/// unavailable so the req/resp layer answers ResourceUnavailable.
#[derive(Debug, Clone)]
pub struct StorageClient {
    cfg: StorageClientConfig,
    handle: Arc<StorageClientHandle>,
    channel: Arc<tokio::sync::RwLock<Option<Channel>>>,
}

impl StorageClient {
    /// Build a client bound to `handle`.
    #[must_use]
    pub fn new(cfg: StorageClientConfig, handle: Arc<StorageClientHandle>) -> Self {
        Self {
            cfg,
            handle,
            channel: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Shared handle (availability + window).
    #[must_use]
    pub fn handle(&self) -> &Arc<StorageClientHandle> {
        &self.handle
    }

    /// Ensure a live channel or mark unavailable.
    async fn client(&self) -> Result<StorageServiceClient<Channel>, Status> {
        self.handle.refuse_if_unavailable()?;
        {
            let guard = self.channel.read().await;
            if let Some(ch) = guard.as_ref() {
                return Ok(StorageServiceClient::new(ch.clone()));
            }
        }
        match dial(&self.cfg).await {
            Ok(ch) => {
                let mut guard = self.channel.write().await;
                *guard = Some(ch.clone());
                self.handle.set_available(true);
                Ok(StorageServiceClient::new(ch))
            }
            Err(e) => {
                self.handle.set_available(false);
                Err(Status::unavailable(format!(
                    "storage dial failed: {e} (ResourceUnavailable, never empty success)"
                )))
            }
        }
    }

    /// Drop the cached channel (forces re-dial on next call).
    pub async fn invalidate(&self) {
        let mut guard = self.channel.write().await;
        *guard = None;
        self.handle.set_available(false);
    }

    /// `GetBlocksByRange` — empty response from a live server is still a
    /// ResourceUnavailable at the p2p layer when no blocks land; transport
    /// failure is always ResourceUnavailable.
    pub async fn get_blocks_by_range(
        &self,
        start_slot: u64,
        count: u64,
    ) -> Result<GetBlocksResponse, Status> {
        let mut c = self.client().await?;
        match c
            .get_blocks_by_range(GetBlocksByRangeRequest { start_slot, count })
            .await
        {
            Ok(r) => Ok(r.into_inner()),
            Err(status) => {
                if is_transport_down(&status) {
                    self.invalidate().await;
                    Err(Status::unavailable(format!(
                        "storage down mid-serve: {status} (ResourceUnavailable, never empty success)"
                    )))
                } else {
                    Err(status)
                }
            }
        }
    }

    /// `GetBlocksByRoot`.
    pub async fn get_blocks_by_root(&self, roots: Vec<Vec<u8>>) -> Result<GetBlocksResponse, Status> {
        let mut c = self.client().await?;
        match c
            .get_blocks_by_root(GetBlocksByRootRequest { roots })
            .await
        {
            Ok(r) => Ok(r.into_inner()),
            Err(status) => {
                if is_transport_down(&status) {
                    self.invalidate().await;
                    Err(Status::unavailable(format!(
                        "storage down mid-serve: {status} (ResourceUnavailable, never empty success)"
                    )))
                } else {
                    Err(status)
                }
            }
        }
    }

    /// `GetColumnsByRange`.
    pub async fn get_columns_by_range(
        &self,
        start_slot: u64,
        count: u64,
        column_indices: Vec<u32>,
    ) -> Result<GetColumnsResponse, Status> {
        let mut c = self.client().await?;
        match c
            .get_columns_by_range(GetColumnsByRangeRequest {
                start_slot,
                count,
                column_indices,
            })
            .await
        {
            Ok(r) => Ok(r.into_inner()),
            Err(status) => {
                if is_transport_down(&status) {
                    self.invalidate().await;
                    Err(Status::unavailable(format!(
                        "storage down mid-serve: {status} (ResourceUnavailable, never empty success)"
                    )))
                } else {
                    Err(status)
                }
            }
        }
    }

    /// `GetColumnsByRoot`.
    pub async fn get_columns_by_root(
        &self,
        identifiers: Vec<cc_proto::storage::ColumnsByRootIdentifier>,
    ) -> Result<GetColumnsResponse, Status> {
        let mut c = self.client().await?;
        match c
            .get_columns_by_root(GetColumnsByRootRequest { identifiers })
            .await
        {
            Ok(r) => Ok(r.into_inner()),
            Err(status) => {
                if is_transport_down(&status) {
                    self.invalidate().await;
                    Err(Status::unavailable(format!(
                        "storage down mid-serve: {status} (ResourceUnavailable, never empty success)"
                    )))
                } else {
                    Err(status)
                }
            }
        }
    }
}

/// Spawn the `WatchServeWindow` reconnect loop.
///
/// Writes [`ServeWindow::store_recomputed`] **only** from:
/// 1. the stream message handler, and
/// 2. §5.5 fail-closed collapse after `window_stale_grace`.
///
/// Bounded exponential backoff on disconnect.
pub fn spawn_watch_serve_window(
    cfg: StorageClientConfig,
    handle: Arc<StorageClientHandle>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    spawn_watch_serve_window_with_metrics(cfg, handle, None, shutdown)
}

/// Like [`spawn_watch_serve_window`] with optional metrics for collapse / gauge.
pub fn spawn_watch_serve_window_with_metrics(
    cfg: StorageClientConfig,
    handle: Arc<StorageClientHandle>,
    metrics: Option<Arc<P2pMetrics>>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = cfg.backoff_initial;
        // When `Some`, we are in the post-disconnect grace window.
        let mut grace_deadline: Option<tokio::time::Instant> = None;
        loop {
            if *shutdown.borrow() {
                break;
            }
            match run_watch_once(&cfg, &handle, metrics.as_deref(), &mut shutdown).await {
                WatchOutcome::Shutdown => break,
                outcome @ (WatchOutcome::Disconnected | WatchOutcome::Connected) => {
                    handle.set_available(false);
                    // A successful stream session (Connected) resets grace: we
                    // just held a fresh value and now start the hold timer again.
                    if matches!(outcome, WatchOutcome::Connected) {
                        grace_deadline = None;
                        backoff = cfg.backoff_initial;
                    }
                    let now = tokio::time::Instant::now();
                    let deadline = match grace_deadline {
                        Some(d) => d,
                        None => {
                            let d = now + cfg.window_stale_grace;
                            grace_deadline = Some(d);
                            warn!(
                                target: "cc_p2p::storage_client",
                                grace_ms = cfg.window_stale_grace.as_millis() as u64,
                                "WatchServeWindow disconnected; holding advertised window for grace"
                            );
                            d
                        }
                    };
                    if now >= deadline {
                        if !handle.is_collapsed() {
                            handle.collapse_to_cache_floor(metrics.as_deref());
                        }
                        // After collapse, keep reconnecting with backoff.
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.changed() => {
                                if *shutdown.borrow() {
                                    break;
                                }
                            }
                        }
                        backoff = next_backoff(backoff, cfg.backoff_cap);
                    } else {
                        // Still inside grace: retry soon, keep last advertised value.
                        let wait = backoff.min(deadline.saturating_duration_since(now));
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => {}
                            _ = shutdown.changed() => {
                                if *shutdown.borrow() {
                                    break;
                                }
                            }
                        }
                        backoff = next_backoff(backoff, cfg.backoff_cap);
                    }
                }
            }
        }
        info!(target: "cc_p2p::storage_client", "WatchServeWindow task exit");
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchOutcome {
    Shutdown,
    Disconnected,
    /// Stream opened successfully (may have received messages) then ended.
    Connected,
}

async fn run_watch_once(
    cfg: &StorageClientConfig,
    handle: &StorageClientHandle,
    metrics: Option<&P2pMetrics>,
    shutdown: &mut watch::Receiver<bool>,
) -> WatchOutcome {
    let channel = match dial(cfg).await {
        Ok(ch) => ch,
        Err(e) => {
            debug!(target: "cc_p2p::storage_client", error = %e, "storage dial failed");
            return WatchOutcome::Disconnected;
        }
    };
    let mut client = StorageServiceClient::new(channel);
    let mut stream = match client
        .watch_serve_window(WatchServeWindowRequest {})
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            debug!(target: "cc_p2p::storage_client", error = %e, "WatchServeWindow open failed");
            return WatchOutcome::Disconnected;
        }
    };
    handle.set_available(true);
    info!(target: "cc_p2p::storage_client", "WatchServeWindow connected");
    let mut saw_message = false;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return WatchOutcome::Shutdown;
                }
            }
            item = stream.next() => {
                match item {
                    Some(Ok(msg)) => {
                        // Sole production write site for earliest_available_slot
                        // from storage (CC-48 / §5.3). Also reverses collapse.
                        handle.apply_stream_window(
                            Slot::new(msg.earliest_available_slot),
                            metrics,
                        );
                        handle.set_available(true);
                        saw_message = true;
                    }
                    Some(Err(e)) => {
                        warn!(target: "cc_p2p::storage_client", error = %e, "WatchServeWindow stream error");
                        return if saw_message {
                            WatchOutcome::Connected
                        } else {
                            WatchOutcome::Disconnected
                        };
                    }
                    None => {
                        return if saw_message {
                            WatchOutcome::Connected
                        } else {
                            WatchOutcome::Disconnected
                        };
                    }
                }
            }
        }
    }
}

async fn dial(cfg: &StorageClientConfig) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_shared(cfg.storage_uri.clone())?
        .connect_timeout(cfg.connect_timeout)
        .connect()
        .await
}

fn next_backoff(current: Duration, cap: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > cap {
        cap
    } else {
        doubled
    }
}

fn is_transport_down(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::DeadlineExceeded | Code::Cancelled
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use prometheus_client::registry::Registry;

    #[test]
    fn refuse_if_unavailable_never_empty_success() {
        let window = Arc::new(ServeWindow::new(Slot::new(0)));
        let handle = StorageClientHandle::new(window);
        assert!(!handle.is_available());
        let err = handle.refuse_if_unavailable().unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert!(err.message().contains("never empty success"));
        // Not an empty success: the Result is Err, not Ok with empty payload.
    }

    #[test]
    fn available_flag_allows_calls() {
        let window = Arc::new(ServeWindow::new(Slot::new(0)));
        let handle = StorageClientHandle::new(window);
        handle.set_available(true);
        assert!(handle.refuse_if_unavailable().is_ok());
    }

    #[test]
    fn backoff_caps() {
        let mut b = STORAGE_BACKOFF_INITIAL;
        for _ in 0..20 {
            b = next_backoff(b, STORAGE_BACKOFF_CAP);
        }
        assert_eq!(b, STORAGE_BACKOFF_CAP);
    }

    #[test]
    fn stream_handler_is_sole_store_site_in_this_module() {
        // Grep-equivalent: store_recomputed appears only via apply_stream_window
        // and collapse_to_cache_floor (both WatchServeWindow lifecycle).
        let src = include_str!("storage_client.rs");
        let prod = src.split("#[cfg(test)]").next().expect("prod half");
        let count = prod.matches("store_recomputed").count();
        // Comments + two call sites (stream apply + collapse).
        assert!(
            count >= 2,
            "storage_client must write AtomicU64 via store_recomputed (stream + collapse)"
        );
        assert!(prod.contains("apply_stream_window") || prod.contains("store_recomputed"));
        assert!(prod.contains("collapse_to_cache_floor"));
    }

    #[test]
    fn window_seed_empty_until_stream() {
        let window = Arc::new(ServeWindow::new(Slot::new(10)));
        assert_eq!(window.load().as_u64(), EMPTY_WINDOW_SLOT);
        window.store_recomputed(Slot::new(42));
        assert_eq!(window.load().as_u64(), 42);
    }

    #[test]
    fn collapse_writes_cache_floor_and_increments_metric() {
        let mut reg = Registry::default();
        let metrics = P2pMetrics::register(&mut reg);
        let window = Arc::new(ServeWindow::new(Slot::new(0)));
        let handle = StorageClientHandle::new(Arc::clone(&window));
        // Last advertised value from a prior stream message.
        handle.apply_stream_window(Slot::new(1_000), Some(&metrics));
        assert_eq!(window.load().as_u64(), 1_000);
        assert!(!handle.is_collapsed());

        // Cache floor is the Phase 2 in-memory floor.
        handle.set_cache_floor(Slot::new(50));
        handle.collapse_to_cache_floor(Some(&metrics));

        assert_eq!(window.load().as_u64(), 50);
        assert!(handle.is_collapsed());
        assert_eq!(metrics.window_collapsed(), 1);
        assert_eq!(metrics.earliest_available_slot(), 50);

        // Reconnect reverses collapse without operator action.
        handle.apply_stream_window(Slot::new(900), Some(&metrics));
        assert!(!handle.is_collapsed());
        assert_eq!(window.load().as_u64(), 900);
        assert_eq!(metrics.earliest_available_slot(), 900);
        // Counter does not go backwards.
        assert_eq!(metrics.window_collapsed(), 1);
    }

    #[test]
    fn default_window_stale_grace_is_60s() {
        assert_eq!(DEFAULT_WINDOW_STALE_GRACE, Duration::from_secs(60));
        assert_eq!(
            StorageClientConfig::default().window_stale_grace,
            Duration::from_secs(60)
        );
    }

    /// `config/p2p.toml` must place `window_stale_grace_secs` at the **root**,
    /// not under `[peers]` (URI map) or `[clock]` (typed clock fields).
    #[test]
    fn p2p_toml_window_stale_grace_is_root_key() {
        let text = include_str!("../../../config/p2p.toml");
        let mut table = String::new(); // "" == root
        let mut found_in: Option<String> = None;
        for line in text.lines() {
            let t = line.trim();
            if t.starts_with('[') && t.ends_with(']') && !t.starts_with("[[") {
                table = t.trim_matches(|c| c == '[' || c == ']').to_owned();
                continue;
            }
            if t.starts_with("window_stale_grace_secs") {
                found_in = Some(table.clone());
            }
        }
        assert_eq!(
            found_in.as_deref(),
            Some(""),
            "window_stale_grace_secs must be a root key, found under [{:?}]",
            found_in
        );
        assert!(
            text.contains("window_stale_grace_secs = 60"),
            "fixture must set the 60s default"
        );
    }

    /// Shared cache-floor Arc updates collapse target without a second copy.
    #[test]
    fn shared_cache_floor_feeds_collapse() {
        use crate::backfill::BackfillCache;
        use cc_types::Mainnet;

        let window = Arc::new(ServeWindow::new(Slot::new(0)));
        let floor = Arc::new(AtomicU64::new(EMPTY_WINDOW_SLOT));
        let handle = StorageClientHandle::with_cache_floor(Arc::clone(&window), Arc::clone(&floor));

        let mut cache = BackfillCache::<Mainnet>::with_bounds(
            Slot::new(0),
            0u64..8,
            0u64..4,
            50_000,
            2048,
            2048 * 8,
        );
        cache.bind_cache_floor(Arc::clone(&floor));
        cache.set_head_slot(Slot::new(5));
        // Incomplete head → floor 6.
        assert_eq!(cache.cache_floor().as_u64(), 6);
        assert_eq!(floor.load(Ordering::Acquire), 6);

        handle.apply_stream_window(Slot::new(1_000), None);
        handle.collapse_to_cache_floor(None);
        assert_eq!(window.load().as_u64(), 6, "collapse must read shared floor");
        assert!(handle.is_collapsed());
    }

    #[tokio::test]
    async fn client_get_blocks_refuses_when_down() {
        let window = Arc::new(ServeWindow::new(Slot::new(0)));
        let handle = Arc::new(StorageClientHandle::new(window));
        // available=false by default
        let client = StorageClient::new(
            StorageClientConfig {
                storage_uri: "http://127.0.0.1:1".to_owned(),
                ..StorageClientConfig::default()
            },
            handle,
        );
        let err = client.get_blocks_by_range(0, 1).await.unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert!(
            err.message().contains("ResourceUnavailable")
                || err.message().contains("never empty success")
                || err.message().contains("unreachable"),
            "msg={}",
            err.message()
        );
    }

    /// Three-directory grep hygiene for services/p2p: exactly one production
    /// write path family (stream apply + collapse) in this module.
    #[test]
    fn grep_hygiene_sole_write_in_storage_client() {
        let cache_src = include_str!("backfill/cache.rs");
        let cache_prod = cache_src.split("#[cfg(test)]").next().unwrap();
        assert!(
            !cache_prod.contains("store_recomputed"),
            "cache production half must not call store_recomputed"
        );
        let sc = include_str!("storage_client.rs");
        let sc_prod = sc.split("#[cfg(test)]").next().unwrap();
        assert!(
            sc_prod.contains("store_recomputed"),
            "storage_client must own the AtomicU64 write"
        );
        // Exactly the apply + collapse call sites (not counting comments).
        let call_sites = sc_prod
            .lines()
            .filter(|l| {
                let t = l.trim();
                t.contains("store_recomputed") && !t.starts_with("//") && !t.starts_with("///")
            })
            .count();
        assert_eq!(
            call_sites, 2,
            "expected stream apply + collapse only, got {call_sites}"
        );
    }
}
