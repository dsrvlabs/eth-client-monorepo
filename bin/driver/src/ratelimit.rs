//! Provider politeness: `429`/`503` backoff and rotation (Architecture §9.5, CC-1Ab).
//!
//! - Honour `Retry-After` when present; otherwise exponential full jitter in
//!   `[1 s, 60 s]`.
//! - After **3 consecutive failures** on the active provider, rotate to the
//!   next and log at `warn`.
//! - Backoff state is **per provider** — rotation does not reset the penalty
//!   on a rate-limiting peer.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;
use tracing::{debug, warn};

use crate::api::{ApiError, BeaconApiClient};

/// Consecutive provider failures before rotation (§9.5).
pub(crate) const DEFAULT_ROTATE_AFTER: u32 = 3;

/// Floor for jittered backoff.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
/// Cap for jittered backoff.
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Per-provider backoff bookkeeping.
#[derive(Debug, Clone, Default)]
struct ProviderState {
    consecutive_failures: u32,
    /// Tokio instant after which this provider may be contacted again.
    next_ready: Option<Instant>,
    /// Last computed backoff (for tests / observability).
    last_backoff: Option<Duration>,
}

type OnProviderError = std::sync::Arc<dyn Fn(&str, u16) + Send + Sync>;
type OnActiveProvider = std::sync::Arc<dyn Fn(u64) + Send + Sync>;

/// Metrics / instrumentation hooks for the pool (optional).
#[derive(Clone, Default)]
pub(crate) struct RateLimitMetrics {
    /// `cc_driver_provider_errors_total{provider,code}` — increment callback.
    pub on_error: Option<OnProviderError>,
    /// `cc_driver_active_provider` — set to the active index.
    pub on_active: Option<OnActiveProvider>,
    /// Total HTTP attempts issued through the pool (rate-ceiling tests).
    pub request_attempts: std::sync::Arc<AtomicU64>,
}

impl std::fmt::Debug for RateLimitMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitMetrics")
            .field("on_error", &self.on_error.as_ref().map(|_| "<fn>"))
            .field("on_active", &self.on_active.as_ref().map(|_| "<fn>"))
            .field(
                "request_attempts",
                &self.request_attempts.load(Ordering::Relaxed),
            )
            .finish()
    }
}

/// Multi-provider client with per-provider backoff and rotation.
#[derive(Debug)]
pub(crate) struct ProviderPool {
    clients: Vec<BeaconApiClient>,
    states: Vec<ProviderState>,
    active: usize,
    rotate_after: u32,
    metrics: RateLimitMetrics,
}

impl ProviderPool {
    /// Build a pool from ordered base URLs (first is preferred).
    pub(crate) fn new(
        bases: &[String],
        default_fork: u32,
        rotate_after: u32,
        metrics: RateLimitMetrics,
    ) -> Result<Self, ApiError> {
        if bases.is_empty() {
            return Err(ApiError::Provider {
                provider: "<none>".into(),
                reason: "provider pool requires at least one base URL".into(),
            });
        }
        let mut clients = Vec::with_capacity(bases.len());
        for base in bases {
            clients.push(BeaconApiClient::new(base.clone(), default_fork)?);
        }
        let n = clients.len();
        let pool = Self {
            clients,
            states: vec![ProviderState::default(); n],
            active: 0,
            rotate_after: rotate_after.max(1),
            metrics,
        };
        if let Some(cb) = pool.metrics.on_active.as_ref() {
            cb(0);
        }
        Ok(pool)
    }

    /// Active provider index (0-based).
    #[cfg(test)]
    pub(crate) fn active_index(&self) -> usize {
        self.active
    }

    /// Active provider base URL.
    pub(crate) fn active_base(&self) -> &str {
        self.clients[self.active].base()
    }

    /// Snapshot of consecutive failures for provider `idx` (tests).
    #[cfg(test)]
    pub(crate) fn consecutive_failures(&self, idx: usize) -> u32 {
        self.states.get(idx).map(|s| s.consecutive_failures).unwrap_or(0)
    }

    /// Last backoff applied to provider `idx` (tests).
    #[cfg(test)]
    pub(crate) fn last_backoff(&self, idx: usize) -> Option<Duration> {
        self.states.get(idx).and_then(|s| s.last_backoff)
    }

    /// Run `op` against the active client, waiting out backoff and rotating on
    /// consecutive **retryable** failures (429/503, transport, 5xx).
    ///
    /// Terminal HTTP (404/4xx) and decode errors return immediately so walk-back
    /// can abandon and the steady loop can keep polling forward (SEC-1Ab-1).
    pub(crate) async fn call<F, Fut, T>(&mut self, mut op: F) -> Result<T, ApiError>
    where
        F: FnMut(BeaconApiClient) -> Fut,
        Fut: Future<Output = Result<T, ApiError>>,
    {
        loop {
            self.wait_active_ready().await;
            let client = self.clients[self.active].clone();
            self.metrics
                .request_attempts
                .fetch_add(1, Ordering::SeqCst);
            match op(client).await {
                Ok(v) => {
                    self.on_success();
                    return Ok(v);
                }
                Err(e) if e.is_retryable_provider() => {
                    self.on_failure(&e);
                    // Continue — never hot-loop: on_failure always schedules a wait.
                }
                Err(e) => {
                    // Terminal / non-retryable: surface once, do not spin.
                    debug!(error = %e, "provider call terminal error (no retry)");
                    return Err(e);
                }
            }
        }
    }

    /// Like [`Self::call`] but gives up after `max_attempts` retryable failures
    /// (used by unit tests that assert rotation without hanging).
    #[cfg(test)]
    pub(crate) async fn call_with_limit<F, Fut, T>(
        &mut self,
        max_attempts: u32,
        mut op: F,
    ) -> Result<T, ApiError>
    where
        F: FnMut(BeaconApiClient) -> Fut,
        Fut: Future<Output = Result<T, ApiError>>,
    {
        let mut attempts = 0u32;
        loop {
            self.wait_active_ready().await;
            let client = self.clients[self.active].clone();
            self.metrics
                .request_attempts
                .fetch_add(1, Ordering::SeqCst);
            match op(client).await {
                Ok(v) => {
                    self.on_success();
                    return Ok(v);
                }
                Err(e) if e.is_retryable_provider() => {
                    attempts += 1;
                    self.on_failure(&e);
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn wait_active_ready(&mut self) {
        // Rotation is **only** after `rotate_after` consecutive failures
        // (`on_failure`). While the active provider is cooling down we wait —
        // we do not opportunistically hop to a ready peer (that would bypass
        // the 3-strike rule and reset observability of the penalty).
        let idx = self.active;
        if let Some(ready_at) = self.states[idx].next_ready {
            let now = Instant::now();
            if ready_at > now {
                let wait = ready_at.saturating_duration_since(now);
                debug!(
                    provider = self.clients[idx].base(),
                    wait_ms = wait.as_millis() as u64,
                    "provider backoff wait"
                );
                tokio::time::sleep(wait).await;
            }
            self.states[idx].next_ready = None;
        }
    }

    fn on_success(&mut self) {
        let s = &mut self.states[self.active];
        s.consecutive_failures = 0;
        s.next_ready = None;
    }

    fn on_failure(&mut self, err: &ApiError) {
        let provider = self.clients[self.active].base().to_owned();
        let code = err.status_code().unwrap_or(0);
        if let Some(cb) = self.metrics.on_error.as_ref() {
            cb(&provider, code);
        }

        let delay = compute_backoff_delay(
            self.states[self.active].consecutive_failures,
            err.retry_after(),
        );
        let s = &mut self.states[self.active];
        s.consecutive_failures = s.consecutive_failures.saturating_add(1);
        s.last_backoff = Some(delay);
        s.next_ready = Some(Instant::now() + delay);

        let failures = s.consecutive_failures;
        debug!(
            %provider,
            code,
            failures,
            backoff_ms = delay.as_millis() as u64,
            "provider failure recorded"
        );

        if failures >= self.rotate_after && self.clients.len() > 1 {
            let next = (self.active + 1) % self.clients.len();
            warn!(
                from = %provider,
                to = %self.clients[next].base(),
                failures,
                "rotating beacon provider after consecutive failures"
            );
            self.switch_active(next);
        }
    }

    fn switch_active(&mut self, idx: usize) {
        self.active = idx;
        if let Some(cb) = self.metrics.on_active.as_ref() {
            cb(idx as u64);
        }
    }
}

/// Compute the sleep before the next attempt.
///
/// Prefers `Retry-After` when present (clamped to [`BACKOFF_MAX`]); otherwise
/// exponential full jitter from [`BACKOFF_MIN`] to [`BACKOFF_MAX`].
pub(crate) fn compute_backoff_delay(
    consecutive_failures_before: u32,
    retry_after: Option<Duration>,
) -> Duration {
    if let Some(ra) = retry_after {
        let secs = ra.as_secs().max(1).min(BACKOFF_MAX.as_secs());
        return Duration::from_secs(secs);
    }
    // attempt index after this failure: 0,1,2,... → cap 2^n growth
    let exp = consecutive_failures_before.min(6);
    let ceiling_ms = (BACKOFF_MIN.as_millis() as u64)
        .saturating_mul(1u64 << exp)
        .min(BACKOFF_MAX.as_millis() as u64);
    full_jitter(BACKOFF_MIN.as_millis() as u64, ceiling_ms)
}

/// Full jitter in `[min_ms, ceiling_ms]` (inclusive lower, exclusive-ish upper).
fn full_jitter(min_ms: u64, ceiling_ms: u64) -> Duration {
    let ceiling = ceiling_ms.max(min_ms);
    let span = ceiling.saturating_sub(min_ms).max(1);
    let r = weak_rand_u64();
    let ms = min_ms + (r % span);
    Duration::from_millis(ms.min(BACKOFF_MAX.as_millis() as u64))
}

/// Cheap non-crypto jitter source (no extra crate edge on the driver).
fn weak_rand_u64() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(1);
    // Mix with a simple LCG step so consecutive draws in the same ns differ.
    nanos
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0x6A09_E667_F3BC_C909)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use http_body_util::Full;
    use hyper::body::Bytes as HyperBytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use tokio::net::TcpListener;

    #[test]
    fn retry_after_preferred_over_jitter() {
        let d = compute_backoff_delay(0, Some(Duration::from_secs(7)));
        assert_eq!(d, Duration::from_secs(7));
        let d = compute_backoff_delay(5, Some(Duration::from_secs(120)));
        assert_eq!(d, BACKOFF_MAX, "Retry-After clamped to 60s");
    }

    #[test]
    fn jitter_stays_within_bounds() {
        for fails in 0..10 {
            for _ in 0..20 {
                let d = compute_backoff_delay(fails, None);
                assert!(d >= BACKOFF_MIN, "{d:?}");
                assert!(d <= BACKOFF_MAX, "{d:?}");
            }
        }
    }

    #[derive(Debug, Default)]
    struct StubCtl {
        /// Remaining 429 responses for provider A (path prefix /a/).
        a_remain_429: AtomicU32,
        /// Retry-After seconds on those 429s.
        retry_after_secs: u64,
        /// Total requests observed per mount.
        a_hits: AtomicU64,
        b_hits: AtomicU64,
    }

    async fn spawn_dual_stub(ctl: Arc<StubCtl>) -> (SocketAddr, SocketAddr) {
        // One listener; route by host path? Simpler: two listeners with own state
        // via Arc — share ctl, different "label" via closure.
        let a = spawn_one(Arc::clone(&ctl), true).await;
        let b = spawn_one(Arc::clone(&ctl), false).await;
        (a, b)
    }

    async fn spawn_one(ctl: Arc<StubCtl>, is_a: bool) -> SocketAddr {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let io = TokioIo::new(stream);
                let ctl = Arc::clone(&ctl);
                tokio::spawn(async move {
                    let svc = service_fn(move |req| {
                        let ctl = Arc::clone(&ctl);
                        async move { handle(req, ctl, is_a).await }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        // Block until the accept loop is live (avoids connection-refused hot loops
        // under `start_paused`, which would otherwise skew attempt counts).
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
        addr
    }

    async fn handle(
        req: Request<hyper::body::Incoming>,
        ctl: Arc<StubCtl>,
        is_a: bool,
    ) -> Result<Response<Full<HyperBytes>>, Infallible> {
        if req.method() != Method::GET {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(HyperBytes::new()))
                .unwrap());
        }
        if is_a {
            ctl.a_hits.fetch_add(1, Ordering::SeqCst);
            let left = ctl.a_remain_429.load(Ordering::SeqCst);
            if left > 0 {
                ctl.a_remain_429.fetch_sub(1, Ordering::SeqCst);
                return Ok(Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header(hyper::header::RETRY_AFTER, ctl.retry_after_secs.to_string())
                    .body(Full::new(HyperBytes::from("slow down")))
                    .unwrap());
            }
        } else {
            ctl.b_hits.fetch_add(1, Ordering::SeqCst);
        }
        // Successful head header (minimal).
        let body = r#"{"data":{"root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","canonical":true,"header":{"message":{"slot":"1","proposer_index":"0","parent_root":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","state_root":"0x0000000000000000000000000000000000000000000000000000000000000001","body_root":"0x0000000000000000000000000000000000000000000000000000000000000002"},"signature":"0x00"}}}"#;
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(HyperBytes::from(body)))
            .unwrap())
    }

    /// Pure unit: `Retry-After` is preferred and clamped (no HTTP).
    #[test]
    fn retry_after_delay_is_exact_and_clamped() {
        assert_eq!(
            compute_backoff_delay(0, Some(Duration::from_secs(5))),
            Duration::from_secs(5)
        );
        assert_eq!(
            compute_backoff_delay(99, Some(Duration::from_secs(1))),
            Duration::from_secs(1)
        );
    }

    /// Real-time HTTP: 2×429 with Retry-After=1s then success — no hot loop.
    #[tokio::test]
    async fn honour_retry_after_and_no_hot_loop() {
        let ctl = Arc::new(StubCtl {
            a_remain_429: AtomicU32::new(2),
            retry_after_secs: 1,
            ..StubCtl::default()
        });
        let (addr_a, _) = spawn_dual_stub(Arc::clone(&ctl)).await;
        let attempts = Arc::new(AtomicU64::new(0));
        let mut pool = ProviderPool::new(
            &[format!("http://{addr_a}")],
            6,
            3,
            RateLimitMetrics {
                request_attempts: Arc::clone(&attempts),
                ..Default::default()
            },
        )
        .unwrap();

        let start = std::time::Instant::now();
        let header = pool
            .call_with_limit(8, |c| async move { c.get_head_header().await })
            .await
            .expect("eventual success after Retry-After backoff");
        assert_eq!(header.slot, 1);
        let elapsed = start.elapsed();
        // Two 429s with Retry-After 1s each → at least ~2s wall time.
        assert!(
            elapsed >= Duration::from_secs(2),
            "expected ≥2s backoff, got {elapsed:?}"
        );
        let n = attempts.load(Ordering::SeqCst);
        assert!(
            (3..=6).contains(&n),
            "expected ~3 attempts (2×429 + ok), got {n}"
        );
        assert_eq!(
            pool.last_backoff(0),
            Some(Duration::from_secs(1)),
            "Retry-After must drive backoff, not jitter"
        );
    }

    #[tokio::test]
    async fn rotate_after_three_consecutive_failures() {
        let ctl = Arc::new(StubCtl {
            a_remain_429: AtomicU32::new(100), // A never recovers in this test
            retry_after_secs: 1,
            ..StubCtl::default()
        });
        let (addr_a, addr_b) = spawn_dual_stub(Arc::clone(&ctl)).await;
        let active = Arc::new(AtomicU64::new(0));
        let active_log = Arc::clone(&active);
        let mut pool = ProviderPool::new(
            &[format!("http://{addr_a}"), format!("http://{addr_b}")],
            6,
            3,
            RateLimitMetrics {
                on_active: Some(Arc::new(move |idx| {
                    active_log.store(idx, Ordering::SeqCst);
                })),
                ..Default::default()
            },
        )
        .unwrap();

        let header = pool
            .call_with_limit(12, |c| async move { c.get_head_header().await })
            .await
            .expect("B should succeed after A is rotated out");
        assert_eq!(header.slot, 1);
        assert_eq!(pool.active_index(), 1, "must rotate to provider B");
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert!(
            ctl.b_hits.load(Ordering::SeqCst) >= 1,
            "B must have served the success"
        );
        // A kept its failure count — rotation must not reset it.
        assert!(
            pool.consecutive_failures(0) >= 3,
            "A penalty retained, got {}",
            pool.consecutive_failures(0)
        );
    }

    #[tokio::test]
    async fn backoff_state_retained_across_rotation_roundtrip() {
        let ctl = Arc::new(StubCtl {
            a_remain_429: AtomicU32::new(3),
            retry_after_secs: 1,
            ..StubCtl::default()
        });
        let (addr_a, addr_b) = spawn_dual_stub(Arc::clone(&ctl)).await;
        let mut pool = ProviderPool::new(
            &[format!("http://{addr_a}"), format!("http://{addr_b}")],
            6,
            3,
            RateLimitMetrics::default(),
        )
        .unwrap();

        // First call: 3×429 on A → rotate to B → success on B.
        let _ = pool
            .call_with_limit(12, |c| async move { c.get_head_header().await })
            .await
            .expect("rotate to B");
        assert_eq!(pool.active_index(), 1);
        let a_fails = pool.consecutive_failures(0);
        assert!(a_fails >= 3, "A failures={a_fails}");
        let a_backoff = pool.last_backoff(0).expect("A must have a recorded backoff");
        assert_eq!(
            a_backoff,
            Duration::from_secs(1),
            "per-provider Retry-After backoff must be retained"
        );

        // Force active back to A — penalty and last_backoff retained.
        pool.switch_active(0);
        assert_eq!(pool.consecutive_failures(0), a_fails);
        assert_eq!(pool.last_backoff(0), Some(a_backoff));
    }

    #[tokio::test]
    async fn request_rate_during_backoff_respects_retry_after_ceiling() {
        // Ceiling: with Retry-After=1s, 3 attempts need ≥2s of sleeps → ≤ 1.5 rps.
        let ctl = Arc::new(StubCtl {
            a_remain_429: AtomicU32::new(100),
            retry_after_secs: 1,
            ..StubCtl::default()
        });
        let (addr_a, _) = spawn_dual_stub(Arc::clone(&ctl)).await;
        let attempts = Arc::new(AtomicU64::new(0));
        let mut pool = ProviderPool::new(
            &[format!("http://{addr_a}")],
            6,
            99, // never rotate
            RateLimitMetrics {
                request_attempts: Arc::clone(&attempts),
                ..Default::default()
            },
        )
        .unwrap();

        let start = std::time::Instant::now();
        let result = pool
            .call_with_limit(3, |c| async move { c.get_head_header().await })
            .await;
        assert!(result.is_err(), "always-429 provider must not succeed");
        let elapsed = start.elapsed();
        let n = attempts.load(Ordering::SeqCst);
        assert_eq!(n, 3);
        // 3 attempts with 1s Retry-After between failures → ≥2s for two sleeps.
        assert!(elapsed >= Duration::from_secs(2), "elapsed={elapsed:?}");
        let rate = n as f64 / elapsed.as_secs_f64().max(0.001);
        assert!(
            rate <= 2.0,
            "request rate {rate} rps exceeded ceiling 2.0 rps"
        );
    }

    /// Always-404 provider: `call` returns immediately (no infinite retry).
    #[tokio::test]
    async fn call_does_not_infinite_retry_404() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let svc = service_fn(|_req| async {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Full::new(HyperBytes::from("gone")))
                                .unwrap(),
                        )
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }

        let attempts = Arc::new(AtomicU64::new(0));
        let mut pool = ProviderPool::new(
            &[format!("http://{addr}")],
            6,
            3,
            RateLimitMetrics {
                request_attempts: Arc::clone(&attempts),
                ..Default::default()
            },
        )
        .unwrap();

        let start = std::time::Instant::now();
        let err = pool
            .call(|c| async move { c.get_head_header().await })
            .await
            .expect_err("404 must surface");
        assert!(err.is_terminal_http(), "got {err}");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "terminal 404 must not be retried"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "must return promptly, elapsed {:?}",
            start.elapsed()
        );
    }
}
