//! Deadline-bounded core-liveness probe (S1-A-15 / S1-A-16 / [ARCH] §7.2).
//!
//! Issues a no-op through the consensus core's never-shed `tick` lane
//! (`TickWork::Ping`). This is **not** a `grpc-health-probe` of the process:
//! the health service answers from a tokio task while the core is a separate
//! OS thread ([ARCH] §7.1). `GetHead` is a snapshot load and never touches
//! that thread (ADR-P1-09).
//!
//! Deadline is the attestation soft deadline
//! `ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000` (ADR-P3-13). Formula is
//! duplicated here so `cc-chain` does not depend on `cc-engine-api` (JWT
//! invariant).
//!
//! [`probe_core_liveness`] is one sample. [`run_core_liveness_loop`] applies
//! the consecutive-miss policy and drives aggregate `local_ready` (ADR-R-04).

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

use crate::metrics::ChainMetrics;

/// Hoodi / engine-config default `SLOT_DURATION_MS` (ADR-P3-13).
pub const DEFAULT_SLOT_DURATION_MS: u64 = 12_000;
/// Hoodi / engine-config default `ATTESTATION_DUE_BPS` (ADR-P3-13).
pub const DEFAULT_ATTESTATION_DUE_BPS: u64 = 3_333;

/// Something that can round-trip a no-op through the consensus core.
///
/// Production: [`crate::core::CoreHandle`]. Tests: a core that never answers.
pub trait CoreLiveness: Send + Sync {
    /// Enqueue the no-op and wait for the core thread to reply.
    ///
    /// Must not apply the deadline — [`probe_core_liveness`] owns that.
    fn ping(&self) -> impl Future<Output = Result<(), LivenessError>> + Send;
}

/// Why a probe sample failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivenessError {
    /// The no-op did not complete within the soft deadline.
    DeadlineExceeded { deadline: Duration },
    /// The core thread is gone or dropped the reply (not a deadline miss).
    Unavailable { reason: String },
}

impl std::fmt::Display for LivenessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeadlineExceeded { deadline } => {
                write!(f, "core liveness ping exceeded deadline {deadline:?}")
            }
            Self::Unavailable { reason } => write!(f, "core unavailable: {reason}"),
        }
    }
}

impl std::error::Error for LivenessError {}

/// Soft-deadline milliseconds: `ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000`.
///
/// Same substitution as `cc_engine_api::config::soft_deadline_ms` (ADR-P3-13).
/// Uses `SLOT_DURATION_MS`, never `SECONDS_PER_SLOT`.
#[must_use]
pub fn soft_deadline_ms(attestation_due_bps: u64, slot_duration_ms: u64) -> f64 {
    (attestation_due_bps as f64) * (slot_duration_ms as f64) / 10_000.0
}

/// Soft deadline as [`Duration`] (sub-millisecond via `from_secs_f64`).
#[must_use]
pub fn liveness_deadline(attestation_due_bps: u64, slot_duration_ms: u64) -> Duration {
    Duration::from_secs_f64(soft_deadline_ms(attestation_due_bps, slot_duration_ms) / 1_000.0)
}

/// Hoodi default probe deadline (`3333 × 12000 / 10000` ms).
#[must_use]
pub fn default_liveness_deadline() -> Duration {
    liveness_deadline(DEFAULT_ATTESTATION_DUE_BPS, DEFAULT_SLOT_DURATION_MS)
}

/// Issue one no-op through `core` and fail if it does not complete in `deadline`.
///
/// Returns the observed RTT on success. Consecutive-miss accounting and the
/// health flip live in [`run_core_liveness_loop`].
pub async fn probe_core_liveness<C: CoreLiveness + ?Sized>(
    core: &C,
    deadline: Duration,
) -> Result<Duration, LivenessError> {
    let start = tokio::time::Instant::now();
    match tokio::time::timeout(deadline, core.ping()).await {
        Ok(Ok(())) => Ok(start.elapsed()),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(LivenessError::DeadlineExceeded { deadline }),
    }
}

/// Samples per slot ([ARCH] §7.2 — 4×/slot majority without a one-hiccup alarm).
pub const SAMPLES_PER_SLOT: u32 = 4;

/// Consecutive samples required to flip SERVING ↔ NOT_SERVING (ADR-R-04).
///
/// ADR-P3-13 (~4 s) is the **per-sample** deadline, not the miss budget.
/// The core thread legally `block_on`s Engine RPCs for 8 s
/// (`DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT` / fcU). N=2 at slot/4 cadence
/// let two in-flight 8 s units park the DAG (S-A16-1). N=3 sequential
/// horizon is 3×~4 s + 2×3 s ≈ 18 s, so one 8 s `newPayload` (and two
/// back-to-back) cannot accumulate a flip.
///
/// Recover uses the **same N** (S-A16-2). One success must not restore
/// SERVING after a park — that inverted hysteresis flaps compose/peers.
pub const CONSECUTIVE_MISS_THRESHOLD: u32 = 3;

/// Same N as [`CONSECUTIVE_MISS_THRESHOLD`] — recover is not cheaper than fail.
pub const CONSECUTIVE_SUCCESS_THRESHOLD: u32 = CONSECUTIVE_MISS_THRESHOLD;

/// Probe cadence: `slot_duration / 4`.
#[must_use]
pub fn sample_interval(slot_duration_ms: u64) -> Duration {
    Duration::from_millis(slot_duration_ms / u64::from(SAMPLES_PER_SLOT))
}

/// Sink that receives the published liveness verdict.
///
/// Production: [`cc_bootstrap::LocalReadyHandle`] (ANDed into aggregate `""`).
pub trait LivenessSink: Send + Sync {
    /// Publish whether the core is considered live enough for SERVING.
    fn set_serving(&self, serving: bool) -> impl Future<Output = ()> + Send;
}

impl LivenessSink for cc_bootstrap::LocalReadyHandle {
    fn set_serving(&self, serving: bool) -> impl Future<Output = ()> + Send {
        self.set_ready(serving)
    }
}

/// Symmetric consecutive-sample counter that owns the SERVING flip.
///
/// Starts SERVING (restore handshake already marked `local_ready`). N
/// consecutive misses → NOT_SERVING. N consecutive successes → SERVING.
/// A lone miss or success resets the opposite streak. A parked core
/// cannot stay SERVING, and a single later ping cannot flap it back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsecutiveMissPolicy {
    misses: u32,
    successes: u32,
    serving: bool,
}

impl ConsecutiveMissPolicy {
    /// Fail and recover share this N (S-A16-2).
    pub const THRESHOLD: u32 = CONSECUTIVE_MISS_THRESHOLD;

    /// Start SERVING with clean streaks (core just installed).
    #[must_use]
    pub fn new_serving() -> Self {
        Self {
            misses: 0,
            successes: 0,
            serving: true,
        }
    }

    /// Current published verdict.
    #[must_use]
    pub fn serving(&self) -> bool {
        self.serving
    }

    /// Consecutive misses since the last success.
    #[must_use]
    pub fn consecutive_misses(&self) -> u32 {
        self.misses
    }

    /// Consecutive successes since the last miss (only counted while parked).
    #[must_use]
    pub fn consecutive_successes(&self) -> u32 {
        self.successes
    }

    /// Record one sample. `Some(verdict)` means the published bit changed.
    pub fn observe(&mut self, success: bool) -> Option<bool> {
        if success {
            self.misses = 0;
            if !self.serving {
                self.successes = self.successes.saturating_add(1);
                if self.successes >= CONSECUTIVE_SUCCESS_THRESHOLD {
                    self.serving = true;
                    self.successes = 0;
                    return Some(true);
                }
            }
            None
        } else {
            self.successes = 0;
            self.misses = self.misses.saturating_add(1);
            if self.serving && self.misses >= Self::THRESHOLD {
                self.serving = false;
                self.misses = 0;
                return Some(false);
            }
            None
        }
    }
}

/// Sample [`probe_core_liveness`] until `cancel` is true.
///
/// Feeds `sink` (production: `local_ready`) after N consecutive misses or
/// N consecutive successes. Does not touch engine health.
pub async fn run_core_liveness_loop<C, S>(
    core: C,
    sink: S,
    deadline: Duration,
    interval: Duration,
    mut cancel: watch::Receiver<bool>,
    metrics: Option<ChainMetrics>,
) where
    C: CoreLiveness,
    S: LivenessSink,
{
    if let Some(m) = metrics.as_ref() {
        m.set_core_liveness_deadline(deadline);
        m.set_core_liveness_parked(false);
    }
    let mut policy = ConsecutiveMissPolicy::new_serving();
    loop {
        if *cancel.borrow() {
            return;
        }
        let success = match probe_core_liveness(&core, deadline).await {
            Ok(rtt) => {
                if let Some(m) = metrics.as_ref() {
                    m.observe_core_liveness_rtt(rtt);
                }
                true
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    consecutive_misses = policy.consecutive_misses().saturating_add(1),
                    threshold = CONSECUTIVE_MISS_THRESHOLD,
                    "core liveness probe missed"
                );
                false
            }
        };
        if let Some(serving) = policy.observe(success) {
            if let Some(m) = metrics.as_ref() {
                m.set_core_liveness_parked(!serving);
            }
            if serving {
                tracing::info!(
                    threshold = CONSECUTIVE_SUCCESS_THRESHOLD,
                    "core liveness restored after consecutive successful probes"
                );
            } else {
                tracing::warn!(
                    threshold = CONSECUTIVE_MISS_THRESHOLD,
                    "core parked: consecutive probe misses; aggregate NOT_SERVING"
                );
            }
            sink.set_serving(serving).await;
        }
        tokio::select! {
            _ = cancel.wait_for(|&v| v) => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Core that never completes a ping — parked thread / black-holed engine.
    #[derive(Debug, Default)]
    struct SilentCore;

    impl CoreLiveness for SilentCore {
        fn ping(&self) -> impl Future<Output = Result<(), LivenessError>> + Send {
            std::future::pending()
        }
    }

    /// Core that replies immediately (healthy tick-lane path).
    #[derive(Debug, Default)]
    struct PromptCore;

    impl CoreLiveness for PromptCore {
        async fn ping(&self) -> Result<(), LivenessError> {
            Ok(())
        }
    }

    /// Counts pings so the probe is observably invoking the core path.
    #[derive(Debug, Default)]
    struct CountingCore {
        hits: AtomicUsize,
    }

    impl CoreLiveness for CountingCore {
        async fn ping(&self) -> Result<(), LivenessError> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn deadline_is_attestation_due_bps_times_slot_duration() {
        // Hoodi: 3333 bps × 12000 ms / 10000 = 3999.6 ms.
        let hoodi = soft_deadline_ms(DEFAULT_ATTESTATION_DUE_BPS, DEFAULT_SLOT_DURATION_MS);
        assert!((hoodi - 3999.6).abs() < 1e-9, "hoodi deadline = {hoodi}");

        // Gloas-like: 2500 bps → 3000 ms.
        let gloas = soft_deadline_ms(2_500, 12_000);
        assert!((gloas - 3000.0).abs() < 1e-9, "gloas deadline = {gloas}");

        let d = default_liveness_deadline();
        assert_eq!(d, Duration::from_secs_f64(3.999_6));
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_miss_is_observable() {
        let deadline = Duration::from_millis(50);
        let start = tokio::time::Instant::now();
        let err = probe_core_liveness(&SilentCore, deadline)
            .await
            .expect_err("silent core must miss the deadline");
        assert_eq!(err, LivenessError::DeadlineExceeded { deadline });
        // Virtual time advances to the timeout (tokio test-util).
        assert!(
            start.elapsed() >= deadline,
            "timeout must consume at least the deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn prompt_core_is_live_within_deadline() {
        let deadline = default_liveness_deadline();
        let rtt = probe_core_liveness(&PromptCore, deadline).await.unwrap();
        assert!(rtt < deadline);
    }

    #[tokio::test]
    async fn probe_invokes_the_core_ping() {
        let core = CountingCore {
            hits: AtomicUsize::new(0),
        };
        probe_core_liveness(&core, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(core.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unavailable_is_not_a_deadline_miss() {
        struct DeadCore;
        impl CoreLiveness for DeadCore {
            async fn ping(&self) -> Result<(), LivenessError> {
                Err(LivenessError::Unavailable {
                    reason: "core thread dropped ping reply".into(),
                })
            }
        }
        let err = probe_core_liveness(&DeadCore, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, LivenessError::Unavailable { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn arc_dyn_is_not_required_generic_handle_is_enough() {
        // A-16 holds a CoreHandle (or any CoreLiveness), not grpc-health-probe.
        let core = Arc::new(SilentCore);
        let err = probe_core_liveness(core.as_ref(), Duration::from_millis(10))
            .await
            .unwrap_err();
        assert!(matches!(err, LivenessError::DeadlineExceeded { .. }));
    }

    #[test]
    fn sample_interval_is_a_quarter_slot() {
        assert_eq!(
            sample_interval(DEFAULT_SLOT_DURATION_MS),
            Duration::from_secs(3)
        );
        assert_eq!(sample_interval(3_000), Duration::from_millis(750));
    }

    #[test]
    fn flip_horizon_exceeds_one_engine_rpc_deadline() {
        // Sequential miss wall-clock: N × probe + (N−1) × interval.
        // Must outlast one 8 s newPayload / fcU (S-A16-1) and two back-to-back.
        let n = CONSECUTIVE_MISS_THRESHOLD;
        let horizon = default_liveness_deadline()
            .saturating_mul(n)
            .saturating_add(sample_interval(DEFAULT_SLOT_DURATION_MS).saturating_mul(n - 1));
        assert!(
            horizon > Duration::from_millis(8_000),
            "horizon {horizon:?} must exceed one Engine RPC deadline"
        );
        assert!(
            horizon > Duration::from_millis(16_000),
            "horizon {horizon:?} must exceed two back-to-back Engine RPC deadlines"
        );
    }

    #[test]
    fn one_or_two_misses_do_not_leave_serving() {
        let mut p = ConsecutiveMissPolicy::new_serving();
        assert!(p.serving());
        assert_eq!(p.observe(false), None);
        assert!(p.serving());
        assert_eq!(p.consecutive_misses(), 1);
        assert_eq!(p.observe(false), None);
        assert!(p.serving());
        assert_eq!(p.consecutive_misses(), 2);
    }

    #[test]
    fn three_consecutive_misses_leave_serving() {
        let mut p = ConsecutiveMissPolicy::new_serving();
        assert_eq!(p.observe(false), None);
        assert_eq!(p.observe(false), None);
        assert_eq!(p.observe(false), Some(false));
        assert!(!p.serving());
        // Further misses do not re-publish.
        assert_eq!(p.observe(false), None);
        assert!(!p.serving());
    }

    #[test]
    fn success_after_one_miss_resets_counter() {
        let mut p = ConsecutiveMissPolicy::new_serving();
        assert_eq!(p.observe(false), None);
        assert_eq!(p.observe(true), None);
        assert!(p.serving());
        assert_eq!(p.consecutive_misses(), 0);
        // Still need three fresh misses to flip.
        assert_eq!(p.observe(false), None);
        assert_eq!(p.observe(false), None);
        assert!(p.serving());
    }

    #[test]
    fn recover_requires_the_same_n_successes() {
        let mut p = ConsecutiveMissPolicy::new_serving();
        assert_eq!(p.observe(false), None);
        assert_eq!(p.observe(false), None);
        assert_eq!(p.observe(false), Some(false));
        // One or two successes must not flap back (S-A16-2).
        assert_eq!(p.observe(true), None);
        assert!(!p.serving());
        assert_eq!(p.consecutive_successes(), 1);
        assert_eq!(p.observe(true), None);
        assert!(!p.serving());
        assert_eq!(p.observe(true), Some(true));
        assert!(p.serving());
    }

    #[test]
    fn miss_during_recover_resets_success_streak() {
        let mut p = ConsecutiveMissPolicy::new_serving();
        for _ in 0..CONSECUTIVE_MISS_THRESHOLD - 1 {
            assert_eq!(p.observe(false), None);
        }
        assert_eq!(p.observe(false), Some(false));
        assert_eq!(p.observe(true), None);
        assert_eq!(p.observe(true), None);
        assert_eq!(p.observe(false), None);
        assert_eq!(p.consecutive_successes(), 0);
        assert!(!p.serving());
        // Need a full N again.
        assert_eq!(p.observe(true), None);
        assert_eq!(p.observe(true), None);
        assert!(!p.serving());
        assert_eq!(p.observe(true), Some(true));
        assert!(p.serving());
    }

    #[derive(Clone, Default)]
    struct RecordingSink {
        serving: Arc<std::sync::Mutex<Option<bool>>>,
    }

    impl RecordingSink {
        fn last(&self) -> Option<bool> {
            *self.serving.lock().unwrap()
        }
    }

    impl LivenessSink for RecordingSink {
        async fn set_serving(&self, serving: bool) {
            *self.serving.lock().unwrap() = Some(serving);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn loop_three_misses_publish_not_serving() {
        let sink = RecordingSink::default();
        let (tx, rx) = watch::channel(false);
        let loop_sink = sink.clone();
        let task = tokio::spawn(async move {
            run_core_liveness_loop(
                SilentCore,
                loop_sink,
                Duration::from_millis(10),
                Duration::from_millis(5),
                rx,
                None,
            )
            .await;
        });

        // Two sequential misses (10 + 5 + 10) stay SERVING.
        tokio::time::sleep(Duration::from_millis(25)).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.last(), None, "two misses must not flip SERVING");

        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.last(), Some(false));

        let _ = tx.send(true);
        task.await.unwrap();
    }

    /// Fails `fail_n` times, then answers.
    struct FailThenLive {
        remaining: AtomicUsize,
    }

    impl CoreLiveness for FailThenLive {
        fn ping(&self) -> impl Future<Output = Result<(), LivenessError>> + Send {
            let left = self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    Some(n.saturating_sub(1))
                });
            async move {
                if left.unwrap_or(0) > 0 {
                    std::future::pending().await
                } else {
                    Ok(())
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn loop_recover_requires_n_successes() {
        let sink = RecordingSink::default();
        let (tx, rx) = watch::channel(false);
        let loop_sink = sink.clone();
        // 3 misses to park, then live — recover needs 3 successes.
        // Interval 20 ms so the 2-success window is not a timing sliver.
        let core = FailThenLive {
            remaining: AtomicUsize::new(3),
        };
        let task = tokio::spawn(async move {
            run_core_liveness_loop(
                core,
                loop_sink,
                Duration::from_millis(10),
                Duration::from_millis(20),
                rx,
                None,
            )
            .await;
        });

        // Misses at ~10, 40, 70 ms; successes at ~90, 110, 130 ms.
        tokio::time::sleep(Duration::from_millis(80)).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.last(), Some(false));

        tokio::time::sleep(Duration::from_millis(80)).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.last(), Some(true));

        let _ = tx.send(true);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn loop_prompt_core_does_not_flip() {
        let sink = RecordingSink::default();
        let (tx, rx) = watch::channel(false);
        let loop_sink = sink.clone();
        let task = tokio::spawn(async move {
            run_core_liveness_loop(
                PromptCore,
                loop_sink,
                Duration::from_millis(10),
                Duration::from_millis(5),
                rx,
                None,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(40)).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.last(), None);
        let _ = tx.send(true);
        task.await.unwrap();
    }
}
