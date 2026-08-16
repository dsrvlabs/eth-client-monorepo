//! Deadline-bounded core-liveness probe (S1-A-15 / [ARCH] §7.2).
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
//! Callable from a later healthcheck (`S1-A-16`). This module does **not**
//! flip tonic aggregate health, `local_ready`, or compose.

use std::future::Future;
use std::time::Duration;

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
/// Returns the observed RTT on success. Does not record consecutive misses or
/// touch health — that is `S1-A-16`.
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
        // A-16 will hold a CoreHandle (or any CoreLiveness), not grpc-health-probe.
        let core = Arc::new(SilentCore);
        let err = probe_core_liveness(core.as_ref(), Duration::from_millis(10))
            .await
            .unwrap_err();
        assert!(matches!(err, LivenessError::DeadlineExceeded { .. }));
    }
}
