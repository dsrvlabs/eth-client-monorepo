//! Serve half of bootstrap — bind, health, reflection, prober, drain (Architecture §4.2, §4.5).
//!
//! **R-8 (tonic 0.14.6):** `tonic::service::Routes` implements `Default`; construct via
//! `Routes::default()`, `Routes::new(svc)`, or `Routes::builder()`; chain services with
//! consuming `Routes::add_service(self, svc) -> Self`. Hand the finished routes to
//! `Server::builder().layer(…).add_routes(routes).serve_with_shutdown(addr, signal)`.
//!
//! Phase 1 `chain` extends aggregate health with a **local readiness** gate (CC-19b /
//! §7.4 / §15/5): `""` additionally requires bootstrap complete. Optional
//! [`ServeOptions::on_pre_drain`] joins the core thread after NOT_SERVING and before
//! the drain release.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tonic::service::Routes;
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::{info, warn};

use crate::error::Error;
use crate::metrics_server::serve_metrics;
use crate::prober::{PeerHealthState, spawn_probers};
use crate::{Bootstrap, spawn};

/// Drain budget after the shutdown signal (Architecture §4.5).
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Pause after marking aggregate NOT_SERVING and before stopping accept.
///
/// Gives peers (and local probes) a window to observe the drain status over a
/// still-open listener rather than racing a connection reset (§4.5 step 2).
pub(crate) const NOT_SERVING_NOTIFY_PAUSE: Duration = Duration::from_millis(150);

/// Aggregate health service name (empty string). Read by `grpc-health-probe` with no `-service`.
pub const AGGREGATE_HEALTH: &str = "";

/// Boxed async hook run after aggregate NOT_SERVING and before the drain release.
pub type PreDrainHook =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

/// One required peer for the aggregate health prober (Architecture §4.2).
#[derive(Debug, Clone)]
pub struct PeerSpec {
    /// Peer process name (`"chain"`, …) — label for `cc_peer_health` and logs.
    pub name: String,
    /// gRPC URI (`http://chain:9001`).
    pub uri: http::Uri,
}

/// Construction inputs for [`serve`] (Architecture §4.1).
///
/// **D-1:** constructed by the service crate (L3), never returned by `cc-config` (L0).
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    /// Process name (`"chain"`). Must match the name passed to [`crate::init`].
    pub name: &'static str,
    /// Fully-qualified self health name (`"eth.chain.v1.ChainService"`).
    pub health_service_name: &'static str,
    /// gRPC listen address.
    pub grpc_addr: SocketAddr,
    /// Prometheus `/metrics` listen address.
    pub metrics_addr: SocketAddr,
    /// Required peers for aggregate health.
    pub peers: Vec<PeerSpec>,
    /// Encoded `FileDescriptorSet` (`cc_proto::FILE_DESCRIPTOR_SET`).
    pub descriptor_set: &'static [u8],
    /// Full gRPC paths for metric label normalisation
    /// (e.g. `"/eth.chain.v1.ChainService/GetInfo"`). Empty → all methods record as
    /// `"unknown"` (still increments `cc_grpc_requests_total`).
    pub known_methods: Vec<String>,
}

/// Optional Phase 1 extensions to [`serve`] (CC-19b lifecycle).
///
/// Defaults preserve Phase 0 behaviour: aggregate flips with peers only, no
/// pre-drain hook, no local-ready gate.
#[derive(Default)]
#[allow(missing_debug_implementations)] // holds non-Debug `PreDrainHook` / oneshot
pub struct ServeOptions {
    /// When true, aggregate `""` stays **NOT_SERVING** until
    /// [`LocalReadyHandle::mark_ready`] even if every peer is up (and even
    /// with an empty peer set). Used by `chain` so `wait-healthy.sh` waits
    /// for checkpoint bootstrap to finish (§7.4 / §15/5).
    pub require_local_ready: bool,
    /// Receives the [`LocalReadyHandle`] once health is initialised (before
    /// bind). Dropped if `None`.
    pub local_ready_tx: Option<tokio::sync::oneshot::Sender<LocalReadyHandle>>,
    /// Invoked after aggregate goes NOT_SERVING and before the drain release
    /// (core-thread join for `chain`).
    pub on_pre_drain: Option<PreDrainHook>,
}

/// Handle that flips the local-readiness bit on the aggregate health gate.
///
/// Cloned freely; the first `mark_ready` wins, subsequent calls recompute.
#[derive(Clone)]
#[allow(missing_debug_implementations)] // wraps non-Debug health reporter internals
pub struct LocalReadyHandle {
    peer_state: PeerHealthState,
}

impl LocalReadyHandle {
    /// Mark local work (e.g. checkpoint bootstrap) complete and recompute
    /// aggregate health. Idempotent.
    pub async fn mark_ready(&self) {
        self.peer_state.set_local_ready(true).await;
    }
}

/// Bind, serve health + reflection + user routes, run the peer prober, drain on signal.
///
/// Shutdown sequence (§4.5):
/// 1. Install SIGTERM/SIGINT handlers **before** binding.
/// 2. On signal: set aggregate `""` to **NOT_SERVING** first; log at `info`.
/// 3. Optional pre-drain hook (CC-19b: core `Shutdown` + 2 s join).
/// 4. Drain in-flight requests under a **3 s** timeout; warn and proceed if it fires.
/// 5. Stop prober, flush stdout, return `Ok(())`.
pub async fn serve(bs: Bootstrap, spec: ServiceSpec, routes: Routes) -> Result<(), Error> {
    serve_with_options(
        bs,
        spec,
        routes,
        ServeOptions::default(),
        SignalTrigger::UnixSignals,
    )
    .await
}

/// Like [`serve`], but completes the shutdown sequence when `trigger` resolves.
///
/// Used by integration tests that must stop a service without signalling the whole
/// process (multi-service in-process fixtures). The production path is [`serve`].
pub async fn serve_with_shutdown<F>(
    bs: Bootstrap,
    spec: ServiceSpec,
    routes: Routes,
    trigger: F,
) -> Result<(), Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_options(
        bs,
        spec,
        routes,
        ServeOptions::default(),
        SignalTrigger::External(Box::pin(trigger)),
    )
    .await
}

/// Convenience: external cancel trigger + [`ServeOptions`] (integration tests / chain).
pub async fn serve_with_shutdown_options<F>(
    bs: Bootstrap,
    spec: ServiceSpec,
    routes: Routes,
    options: ServeOptions,
    trigger: F,
) -> Result<(), Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_options(
        bs,
        spec,
        routes,
        options,
        SignalTrigger::External(Box::pin(trigger)),
    )
    .await
}

/// Trigger that ends the serve loop (production signals or an external future).
#[allow(missing_debug_implementations)] // External holds an opaque future
pub enum SignalTrigger {
    /// Production: wait for SIGTERM/SIGINT.
    UnixSignals,
    /// Tests / fixtures: complete when the future resolves.
    External(Pin<Box<dyn Future<Output = ()> + Send>>),
}

/// [`serve`] with optional local-ready gate and pre-drain hook.
pub async fn serve_with_options(
    bs: Bootstrap,
    spec: ServiceSpec,
    routes: Routes,
    options: ServeOptions,
    trigger: SignalTrigger,
) -> Result<(), Error> {
    serve_with_options_inner(bs, spec, routes, options, trigger).await
}

async fn serve_with_options_inner(
    bs: Bootstrap,
    spec: ServiceSpec,
    mut routes: Routes,
    mut options: ServeOptions,
    trigger: SignalTrigger,
) -> Result<(), Error> {
    // Extract everything from Bootstrap before moving `registry`.
    let service = bs.service();
    let metrics = bs.metrics().clone();
    let root_span = bs.root_span().clone();
    let metrics_layer = bs.grpc_metrics_layer(spec.known_methods.clone());
    let registry = Arc::new(bs.registry);
    let _enter = root_span.enter();

    // ── 1. Signal / cancel plumbing BEFORE bind (§4.5) ─────────────────────
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let cancel_tx_signal = cancel_tx.clone();
    let mut cancel_for_drain = cancel_tx.subscribe();
    let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();

    // Health reporter lives for the process lifetime of this serve call.
    let (health_reporter, health_service) = health_reporter();

    // Self-only health: bound and own RPCs servable (set before bind; probes
    // that race the accept loop still see SERVING once the port is open).
    health_reporter
        .set_service_status(spec.health_service_name, ServingStatus::Serving)
        .await;

    // Local-ready gate: when required, start not-ready so aggregate stays
    // NOT_SERVING through bootstrap (CC-19b).
    let initial_local_ready = !options.require_local_ready;
    let peer_state =
        PeerHealthState::new(&spec.peers, health_reporter.clone(), initial_local_ready);

    // Aggregate: SERVING only when every required peer is up **and** local-ready.
    let initial_aggregate = if initial_local_ready && spec.peers.is_empty() {
        ServingStatus::Serving
    } else {
        ServingStatus::NotServing
    };
    health_reporter
        .set_service_status(AGGREGATE_HEALTH, initial_aggregate)
        .await;

    // Hand the local-ready handle to the caller before bind so bootstrap can
    // race the accept loop (bind-before-bootstrap-complete, §7.4).
    if let Some(tx) = options.local_ready_tx.take() {
        let _ = tx.send(LocalReadyHandle {
            peer_state: peer_state.clone(),
        });
    }

    // Signal watcher: sets aggregate NOT_SERVING first, optional pre-drain, then unblocks.
    let health_for_signal = health_reporter.clone();
    let peer_state_for_signal = peer_state.clone();
    let on_pre_drain = options.on_pre_drain.take();
    match trigger {
        SignalTrigger::UnixSignals => {
            spawn("shutdown-signal", async move {
                let name = wait_unix_signal().await;
                begin_shutdown(
                    name,
                    &peer_state_for_signal,
                    &health_for_signal,
                    on_pre_drain,
                    drain_tx,
                    cancel_tx_signal,
                )
                .await;
            });
        }
        SignalTrigger::External(fut) => {
            spawn("shutdown-signal", async move {
                fut.await;
                begin_shutdown(
                    "external",
                    &peer_state_for_signal,
                    &health_for_signal,
                    on_pre_drain,
                    drain_tx,
                    cancel_tx_signal,
                )
                .await;
            });
        }
    }

    // ── Reflection ─────────────────────────────────────────────────────────
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(spec.descriptor_set)
        // Also expose the health service descriptor so grpcurl can see it.
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    // ── Assemble routes: user services + health + reflection ───────────────
    // R-8: Routes::add_service consumes self and returns Self.
    routes = routes.add_service(health_service).add_service(reflection);

    // ── Spawn metrics HTTP server ──────────────────────────────────────────
    let metrics_addr = spec.metrics_addr;
    let metrics_handle = spawn("metrics-http", async move {
        if let Err(e) = serve_metrics(metrics_addr, registry).await {
            warn!(error = %e, "metrics server exited");
        }
    });

    // ── Spawn peer probers ─────────────────────────────────────────────────
    let prober_handles = spawn_probers(spec.peers.clone(), peer_state, metrics, service, cancel_rx);

    // ── Bind + serve_with_shutdown ─────────────────────────────────────────
    info!(
        grpc = %spec.grpc_addr,
        metrics = %spec.metrics_addr,
        health = spec.health_service_name,
        peers = spec.peers.len(),
        require_local_ready = options.require_local_ready,
        "serving"
    );

    let server = Server::builder()
        .layer(metrics_layer)
        .add_routes(routes)
        .serve_with_shutdown(spec.grpc_addr, async {
            let _ = drain_rx.await;
        });

    // Drain budget: if graceful shutdown hangs past DRAIN_TIMEOUT after the
    // signal, log and drop the serve future (connections aborted).
    let serve_result = {
        let mut server = std::pin::pin!(server);
        tokio::select! {
            result = &mut server => {
                result.map_err(Error::from)
            }
            _ = async {
                // Wait until cancel is true, then start the drain timer.
                let _ = cancel_for_drain.wait_for(|&v| v).await;
                tokio::time::sleep(DRAIN_TIMEOUT).await;
            } => {
                warn!(
                    in_flight = "unknown",
                    timeout_secs = DRAIN_TIMEOUT.as_secs(),
                    "drain timeout exceeded; forcing exit"
                );
                Ok(())
            }
        }
    };

    // ── 4. Stop prober, flush, return ──────────────────────────────────────
    for h in prober_handles {
        h.abort();
        let _ = h.await;
    }
    metrics_handle.abort();
    let _ = metrics_handle.await;

    // Best-effort flush of the tracing/log writer (stdout).
    use std::io::Write;
    let _ = std::io::stdout().flush();

    serve_result?;
    info!("serve shutdown complete");
    Ok(())
}

/// Mark aggregate NOT_SERVING, run optional pre-drain hook, pause, then release drain.
async fn begin_shutdown(
    signal_name: &'static str,
    peer_state: &PeerHealthState,
    health: &tonic_health::server::HealthReporter,
    on_pre_drain: Option<PreDrainHook>,
    drain_tx: tokio::sync::oneshot::Sender<()>,
    cancel_tx: watch::Sender<bool>,
) {
    info!(signal = signal_name, "shutdown signal received");
    peer_state.mark_draining();
    // §4.5 step 2: NOT_SERVING first so peers observe a drain, not a reset.
    health
        .set_service_status(AGGREGATE_HEALTH, ServingStatus::NotServing)
        .await;
    // CC-19b: join core after NOT_SERVING and before the drain release (§7.4).
    if let Some(hook) = on_pre_drain {
        hook().await;
    }
    tokio::time::sleep(NOT_SERVING_NOTIFY_PAUSE).await;
    let _ = drain_tx.send(());
    let _ = cancel_tx.send(true);
}

/// Block until SIGTERM or SIGINT. Returns the signal name for logging.
async fn wait_unix_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to install SIGTERM handler; falling back to ctrl_c");
                let _ = tokio::signal::ctrl_c().await;
                return "SIGINT";
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to install SIGINT handler; waiting on SIGTERM only");
                let _ = sigterm.recv().await;
                return "SIGTERM";
            }
        };
        tokio::select! {
            _ = sigterm.recv() => "SIGTERM",
            _ = sigint.recv() => "SIGINT",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}
