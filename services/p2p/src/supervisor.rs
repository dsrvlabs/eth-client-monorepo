//! Per-task panic policy — Architecture §2.4, ADR P2-13.
//!
//! - **swarm**: process-fatal (health NOT_SERVING + non-zero exit at the service edge)
//! - **workers**: respawn immediately; `cc_p2p_worker_panics_total` is **cumulative**
//! - **discovery**: respawn with budget (fatal after 5 restarts in 5 minutes) — consumed by CC-21c
//!
//! Spawn always goes through [`cc_bootstrap::spawn`] so the root tracing span survives.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tracing::error;

use crate::metrics::P2pMetrics;

/// Discovery-driver budget: 5 restarts inside a 5-minute window (§2.4).
pub const DISCOVERY_MAX_RESTARTS: u32 = 5;
/// Window for the discovery-driver restart budget.
pub const DISCOVERY_RESTART_WINDOW: Duration = Duration::from_secs(5 * 60);

/// Policy applied when a supervised task's join handle completes with a panic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPolicy {
    /// Swarm: process-fatal (ADR P2-13).
    ProcessFatal,
    /// Default worker: respawn immediately.
    Respawn,
    /// Discovery driver: respawn until budget exhausted, then fatal.
    RespawnBudget {
        /// Max restarts inside `window`.
        max_restarts: u32,
        /// Sliding window for counting restarts.
        window: Duration,
    },
}

impl TaskPolicy {
    /// Discovery-driver policy (§2.4) — encoded here, consumed by CC-21c.
    #[must_use]
    pub const fn discovery() -> Self {
        Self::RespawnBudget {
            max_restarts: DISCOVERY_MAX_RESTARTS,
            window: DISCOVERY_RESTART_WINDOW,
        }
    }
}

/// Factory that (re)spawns a named worker. Must not reset metric counters.
pub type TaskFactory =
    Box<dyn FnMut() -> JoinHandle<()> + Send + 'static>;

/// One supervised task.
#[allow(missing_debug_implementations)] // holds `TaskFactory` closure
pub struct SupervisedTask {
    /// Metric / log label (`"swarm"`, `"gossip"`, …).
    pub name: &'static str,
    /// Panic policy.
    pub policy: TaskPolicy,
    /// (Re)spawn factory.
    pub factory: TaskFactory,
}

/// Outcome of [`run_supervisor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorOutcome {
    /// Shutdown signal observed; tasks aborted.
    Shutdown,
    /// A process-fatal task panicked (or discovery budget exhausted).
    Fatal {
        /// Task name.
        task: &'static str,
        /// Panic payload string (best-effort).
        payload: String,
    },
}

/// Run the supervisor until shutdown or a process-fatal panic.
///
/// `shutdown` resolves when the service is draining (SIGTERM path). On
/// [`SupervisorOutcome::Fatal`] the caller must set aggregate health NOT_SERVING
/// and exit non-zero.
pub async fn run_supervisor(
    mut tasks: Vec<SupervisedTask>,
    metrics: P2pMetrics,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> SupervisorOutcome {
    // Live handles parallel to `tasks`.
    let mut handles: Vec<Option<JoinHandle<()>>> =
        tasks.iter_mut().map(|t| Some((t.factory)())).collect();
    // Restart timestamps for budgeted tasks (index → deque of Instant).
    let mut restart_log: Vec<VecDeque<Instant>> = tasks.iter().map(|_| VecDeque::new()).collect();

    loop {
        if *shutdown.borrow() {
            for h in handles.iter_mut().flatten() {
                h.abort();
            }
            for h in handles.iter_mut().flatten() {
                let _ = h.await;
            }
            return SupervisorOutcome::Shutdown;
        }

        // Wait for any handle to finish, or shutdown.
        let finished = {
            let wait_shutdown = shutdown.changed();
            // Build a select over all live handles via FutureAny pattern.
            let wait_task = wait_any_join(&mut handles);
            tokio::select! {
                biased;
                changed = wait_shutdown => {
                    if changed.is_err() || *shutdown.borrow() {
                        for h in handles.iter_mut().flatten() {
                            h.abort();
                        }
                        for h in handles.iter_mut().flatten() {
                            let _ = h.await;
                        }
                        return SupervisorOutcome::Shutdown;
                    }
                    None
                }
                idx = wait_task => Some(idx),
            }
        };

        let Some(idx) = finished else {
            continue;
        };

        let handle = match handles[idx].take() {
            Some(h) => h,
            None => continue,
        };

        match handle.await {
            Ok(()) => {
                // Clean exit — do not respawn (worker finished its work).
                // Swarm exiting cleanly is unusual; treat as fatal so we do not
                // run deaf.
                let name = tasks[idx].name;
                if matches!(tasks[idx].policy, TaskPolicy::ProcessFatal) {
                    error!(task = name, "process-fatal task exited cleanly; treating as fatal");
                    return SupervisorOutcome::Fatal {
                        task: name,
                        payload: "clean exit".to_owned(),
                    };
                }
            }
            Err(join_err) => {
                let name = tasks[idx].name;
                let payload = panic_payload(join_err);
                // Invariant: log **once** at error with task name (§2.4).
                error!(task = name, payload = %payload, "worker task panicked");
                // Invariant: counter is cumulative across respawns.
                metrics.inc_worker_panics(name);

                match tasks[idx].policy {
                    TaskPolicy::ProcessFatal => {
                        return SupervisorOutcome::Fatal {
                            task: name,
                            payload,
                        };
                    }
                    TaskPolicy::Respawn => {
                        handles[idx] = Some((tasks[idx].factory)());
                    }
                    TaskPolicy::RespawnBudget {
                        max_restarts,
                        window,
                    } => {
                        let now = Instant::now();
                        let log = &mut restart_log[idx];
                        log.push_back(now);
                        while log
                            .front()
                            .is_some_and(|t| now.duration_since(*t) > window)
                        {
                            log.pop_front();
                        }
                        if log.len() as u32 > max_restarts {
                            error!(
                                task = name,
                                restarts = log.len(),
                                "restart budget exhausted; process-fatal"
                            );
                            return SupervisorOutcome::Fatal {
                                task: name,
                                payload: format!(
                                    "restart budget exhausted after panic: {payload}"
                                ),
                            };
                        }
                        handles[idx] = Some((tasks[idx].factory)());
                    }
                }
            }
        }
    }
}

/// Wait until any `Some` handle completes; returns its index.
///
/// Uses `futures::future::select_all` over pinned join futures.
async fn wait_any_join(handles: &mut [Option<JoinHandle<()>>]) -> usize {
    // Collect indices of live handles.
    let live: Vec<usize> = handles
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.as_ref().map(|_| i))
        .collect();

    if live.is_empty() {
        // Park forever — caller will observe shutdown.
        std::future::pending::<()>().await;
        unreachable!();
    }

    // Poll each handle via JoinHandle::is_finished in a loop with yield —
    // avoids re-borrowing JoinHandles into select_all (which takes ownership).
    loop {
        for &i in &live {
            if handles[i].as_ref().is_some_and(|h| h.is_finished()) {
                return i;
            }
        }
        tokio::task::yield_now().await;
        // Also sleep briefly so we do not spin.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn panic_payload(err: tokio::task::JoinError) -> String {
    if err.is_cancelled() {
        return "cancelled".to_owned();
    }
    match err.try_into_panic() {
        Ok(payload) => payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic payload".to_owned()),
        Err(_) => "unknown join error".to_owned(),
    }
}

/// Helper to box a spawn factory from an async thunk.
pub fn factory_from_future<F, Fut>(name: &'static str, mut f: F) -> TaskFactory
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move || cc_bootstrap::spawn(name, f()))
}

/// Type alias for boxed async thunks used in tests.
pub type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
