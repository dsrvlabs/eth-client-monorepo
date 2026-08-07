//! P2P runtime wiring — CC-20b.
//!
//! Starts the swarm task (sole `Swarm` owner), supervisor, clock, and §2.2
//! channel map with stub consumers. **No dialling.** Keeps the Phase 0
//! health / metrics surface startable via [`serve`] / [`run_process`].

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use cc_bootstrap::{
    Bootstrap, PeerSpec, ServeOptions, ServiceSpec, SignalTrigger, serve_with_options,
};
use cc_libp2p::reexport::{Multiaddr, Protocol};
use cc_libp2p::PeerId;
use discv5::enr::NodeId;
use tokio::sync::{mpsc, oneshot, watch};
use tonic::service::Routes;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;
use tracing::{error, info};

use crate::channels::{self, ChannelMap, SwarmCommand, stub_consumer};
use crate::clock::{ClockConfig, SlotClock};
use crate::host::{SwarmTask, build_host_swarm, run_swarm_task};
use crate::identity::{self, IdentityError};
use crate::metrics::{P2pMetrics, QueueName};
use crate::peer_manager::{PeerManager, PeerManagerConfig, run_peer_manager};
use crate::supervisor::{
    SupervisedTask, SupervisorOutcome, TaskPolicy, factory_from_future, run_supervisor,
};

/// Process name / config slug.
pub const SERVICE: &str = "p2p";

/// Fully-qualified gRPC service name for self-only health.
pub const HEALTH_SERVICE_NAME: &str = "eth.p2p.v1.P2pService";

/// Default libp2p listen multiaddr.
pub const DEFAULT_LISTEN_MULTIADDR: &str = "/ip4/0.0.0.0/tcp/9000";

/// Runtime configuration for the P2P host (beyond shared [`cc_config::ServiceConfig`]).
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Path to the 32-byte secp256k1 node key (default `./data/node_key`).
    pub node_key_path: PathBuf,
    /// libp2p listen multiaddr.
    pub listen_multiaddr: Multiaddr,
    /// Slot clock inputs (from config until first `ChainView`).
    pub clock: ClockConfig,
    /// Peer manager knobs (target/max/static peers). Defaults match §3.6.
    pub peer_manager: PeerManagerConfig,
    /// **Test-only:** swarm task panics immediately so the process-fatal path
    /// can be exercised through [`run_process`] without a real host crash.
    /// Production always leaves this `false`.
    pub test_swarm_panic: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            node_key_path: PathBuf::from(identity::DEFAULT_NODE_KEY_PATH),
            listen_multiaddr: default_listen_multiaddr(),
            clock: ClockConfig::default(),
            peer_manager: PeerManagerConfig::default(),
            test_swarm_panic: false,
        }
    }
}

/// Build the default listen multiaddr without parsing (no expect/unwrap).
fn default_listen_multiaddr() -> Multiaddr {
    let mut addr = Multiaddr::empty();
    addr.push(Protocol::Ip4(Ipv4Addr::UNSPECIFIED));
    addr.push(Protocol::Tcp(9000));
    addr
}

/// Snapshot published when the runtime is up (identity loaded + swarm spawned).
#[derive(Debug, Clone)]
pub struct IdentitySnapshot {
    /// libp2p peer id.
    pub peer_id: PeerId,
    /// discv5 node id.
    pub node_id: NodeId,
    /// Key file path.
    pub node_key_path: PathBuf,
}

/// Errors from [`serve`] / runtime start.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Identity load/create failed (including permissions-too-broad).
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// Swarm / behaviour construction failed.
    #[error(transparent)]
    Host(#[from] crate::host::HostBuildError),
    /// Listen multiaddr parse error.
    #[error("listen multiaddr: {0}")]
    ListenAddr(String),
    /// Swarm task panicked — process-fatal (ADR P2-13).
    #[error("swarm task panicked ({task}): {payload}")]
    SwarmPanic {
        /// Task name.
        task: &'static str,
        /// Panic payload.
        payload: String,
    },
    /// Bootstrap / gRPC serve error.
    #[error(transparent)]
    Bootstrap(#[from] cc_bootstrap::Error),
}

/// Optional hooks for tests (ready signal, aggregate health reporter, external shutdown).
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct RuntimeHooks {
    /// Fires once identity is loaded and the swarm task is spawned.
    pub on_ready: Option<oneshot::Sender<IdentitySnapshot>>,
    /// When set, swarm-fatal path marks aggregate `""` NOT_SERVING on this reporter.
    pub aggregate_health: Option<HealthReporter>,
    /// External cancel (tests). When `None`, blocks until supervisor returns.
    pub shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

/// Start the P2P runtime (swarm + supervisor + stubs + clock) and run until
/// shutdown or a process-fatal panic.
///
/// **Load order:** identity is loaded **before** any other runtime work.
///
/// Returns [`Ok`] with the identity snapshot on clean shutdown, or
/// [`RuntimeError::SwarmPanic`] so the process can exit non-zero.
pub async fn serve(
    cfg: RuntimeConfig,
    metrics: P2pMetrics,
    mut hooks: RuntimeHooks,
) -> Result<IdentitySnapshot, RuntimeError> {
    // ── 1. Identity first (CC-20/2 structural) ─────────────────────────────
    let identity = identity::load_or_create(&cfg.node_key_path)?;
    let snapshot = IdentitySnapshot {
        peer_id: identity.peer_id(),
        node_id: identity.node_id(),
        node_key_path: cfg.node_key_path.clone(),
    };
    info!(
        peer_id = %snapshot.peer_id,
        node_id = ?snapshot.node_id,
        path = %cfg.node_key_path.display(),
        "node identity loaded"
    );

    // ── 2. Clock (config-sourced until ChainView) ──────────────────────────
    let clock = SlotClock::new(cfg.clock.clone());
    let _epoch_rx = clock.spawn_epoch_ticks();

    // ── 3. Channel map (§2.2 bounds) ───────────────────────────────────────
    let mut channels = ChannelMap::new(&metrics);

    // ── 4. Swarm (sole owner lives in host task) ───────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Take cmd_rx for the swarm; leave a placeholder receiver in the map.
    let cmd_rx = std::mem::replace(&mut channels.cmd_rx, mpsc::channel(1).1);
    let gossip_tx = channels.gossip_tx.clone();
    let reqresp_in_tx = channels.reqresp_in_tx.clone();
    let conn_tx = channels.conn_tx.clone();
    // Peer manager owns conn_rx + a clone of cmd_tx.
    let conn_rx = std::mem::replace(&mut channels.conn_rx, mpsc::channel(1).1);
    let peer_cmd_tx = channels.cmd_tx.clone();
    let peer_cfg = cfg.peer_manager.clone();

    // ── 5. Stub consumers (count + drop) ───────────────────────────────────
    // Owned edges claimed by later issues; stubs are fire-and-forget scaffolding
    // until CC-22*/23*/27* install supervised workers (§2.4 note). Peer manager
    // (CC-20c) consumes `conn_rx` itself — not the stub.
    spawn_stub_consumers(channels, metrics.clone());

    // ── 6. Supervisor ──────────────────────────────────────────────────────
    let (swarm_factory, peer_factory) = if cfg.test_swarm_panic {
        // Induced panic for process-fatal production-path tests.
        drop(cmd_rx);
        drop(conn_rx);
        drop(peer_cmd_tx);
        let swarm_factory = factory_from_future("swarm", || async {
            #[allow(clippy::panic)] // test-only RuntimeConfig.test_swarm_panic
            {
                panic!("induced swarm panic");
            }
        });
        let peer_factory = factory_from_future("peer_manager", || async {
            std::future::pending::<()>().await;
        });
        (swarm_factory, peer_factory)
    } else {
        let swarm = build_host_swarm(identity.keypair().clone())?;
        assert_eq!(snapshot.peer_id, *swarm.local_peer_id());
        let swarm_task = SwarmTask::new(
            swarm,
            cmd_rx,
            gossip_tx,
            reqresp_in_tx,
            conn_tx,
            metrics.clone(),
        );
        let listen_for_swarm = cfg.listen_multiaddr.clone();
        let swarm_cell = Mutex::new(Some(swarm_task));
        let swarm_factory = factory_from_future("swarm", move || {
            let task = swarm_cell
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let listen = listen_for_swarm.clone();
            async move {
                if let Some(task) = task {
                    run_swarm_task(task, listen).await;
                } else {
                    // Process-fatal tasks are never respawned; park if re-entered.
                    std::future::pending::<()>().await;
                }
            }
        });

        // Peer manager: respawnable; table is rebuilt from connection events.
        let peer_metrics = metrics.clone();
        let peer_shutdown = shutdown_rx.clone();
        let peer_cell = Mutex::new(Some((conn_rx, peer_cmd_tx, peer_cfg, peer_metrics)));
        let peer_factory = factory_from_future("peer_manager", move || {
            let taken = peer_cell
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let mut shutdown = peer_shutdown.clone();
            async move {
                if let Some((conn_rx, cmd_tx, config, metrics)) = taken {
                    let manager = PeerManager::new(config, cmd_tx, metrics);
                    run_peer_manager(manager, conn_rx, shutdown).await;
                } else {
                    // After first exit, park (respawn rebuilds from empty table
                    // only if we re-install; for now stay idle post-channel-close).
                    let _ = shutdown.changed().await;
                }
            }
        });
        (swarm_factory, peer_factory)
    };

    // Idle respawnable worker so the respawn policy is live before CC-22*.
    let idle_factory = factory_from_future("idle_worker", || async {
        std::future::pending::<()>().await;
    });

    let tasks = vec![
        SupervisedTask {
            name: "swarm",
            policy: TaskPolicy::ProcessFatal,
            factory: swarm_factory,
        },
        SupervisedTask {
            name: "peer_manager",
            policy: TaskPolicy::Respawn,
            factory: peer_factory,
        },
        SupervisedTask {
            name: "idle_worker",
            policy: TaskPolicy::Respawn,
            factory: idle_factory,
        },
    ];

    if let Some(tx) = hooks.on_ready.take() {
        let _ = tx.send(snapshot.clone());
    }

    let sup_metrics = metrics.clone();
    let sup_rx = shutdown_rx.clone();
    let supervisor = cc_bootstrap::spawn("supervisor", async move {
        run_supervisor(tasks, sup_metrics, sup_rx).await
    });

    let outcome = if let Some(ext) = hooks.shutdown.take() {
        let mut supervisor = supervisor;
        let early = tokio::select! {
            _ = ext => None,
            join = &mut supervisor => Some(join),
        };
        match early {
            Some(join) => map_supervisor_join(join),
            None => {
                let _ = shutdown_tx.send(true);
                match tokio::time::timeout(Duration::from_secs(2), supervisor).await {
                    Ok(join) => map_supervisor_join(join),
                    Err(_) => SupervisorOutcome::Shutdown,
                }
            }
        }
    } else {
        map_supervisor_join(supervisor.await)
    };

    match outcome {
        SupervisorOutcome::Shutdown => Ok(snapshot),
        SupervisorOutcome::Fatal { task, payload } => {
            error!(task, payload = %payload, "process-fatal runtime failure");
            if let Some(reporter) = hooks.aggregate_health.take() {
                reporter
                    .set_service_status(cc_bootstrap::AGGREGATE_HEALTH, ServingStatus::NotServing)
                    .await;
            }
            Err(RuntimeError::SwarmPanic { task, payload })
        }
    }
}

fn map_supervisor_join(
    join: Result<SupervisorOutcome, tokio::task::JoinError>,
) -> SupervisorOutcome {
    match join {
        Ok(o) => o,
        Err(e) => SupervisorOutcome::Fatal {
            task: "supervisor",
            payload: format!("supervisor join: {e}"),
        },
    }
}

fn spawn_stub_consumers(channels: ChannelMap, metrics: P2pMetrics) {
    let ChannelMap {
        gossip_rx,
        reqresp_in_rx,
        conn_rx: _, // owned by peer manager (CC-20c)
        kzg_rx,
        chain_out_rx,
        chain_in_rx,
        publish_rx,
        cmd_tx,
        publish_tx: _,
        gossip_tx: _,
        reqresp_in_tx: _,
        conn_tx: _,
        kzg_tx: _,
        chain_out_tx: _,
        chain_in_tx: _,
        cmd_rx: _,
    } = channels;

    let m = metrics.clone();
    cc_bootstrap::spawn(
        "stub-gossip",
        stub_consumer("gossip", gossip_rx, m, Some(QueueName::Gossip)),
    );
    let m = metrics.clone();
    cc_bootstrap::spawn(
        "stub-reqresp",
        stub_consumer(
            "reqresp_in",
            reqresp_in_rx,
            m,
            Some(QueueName::ReqrespIn),
        ),
    );
    let m = metrics.clone();
    cc_bootstrap::spawn(
        "stub-kzg",
        stub_consumer("kzg", kzg_rx, m, Some(QueueName::Kzg)),
    );
    let m = metrics.clone();
    cc_bootstrap::spawn(
        "stub-chain-out",
        stub_consumer("chain_out", chain_out_rx, m, None),
    );
    let m = metrics.clone();
    cc_bootstrap::spawn(
        "stub-chain-in",
        stub_consumer("chain_in", chain_in_rx, m, None),
    );

    // Publish queue → cmd bridge.
    let m = metrics.clone();
    cc_bootstrap::spawn("stub-publish-bridge", async move {
        let mut publish_rx = publish_rx;
        while let Some(req) = publish_rx.recv().await {
            let depth = m.queue_depth(QueueName::Publish);
            if depth > 0 {
                m.set_queue_depth(QueueName::Publish, depth - 1);
            }
            match cmd_tx.try_send(SwarmCommand::Publish(req)) {
                Ok(()) => {
                    let d = m.queue_depth(QueueName::Cmd);
                    m.set_queue_depth(QueueName::Cmd, (d + 1).min(channels::CMD_BOUND as i64));
                }
                Err(mpsc::error::TrySendError::Full(cmd)) => {
                    error!("cmd queue full; dropping local publish");
                    let _ = cmd;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
    });
}

/// Process entry: identity → runtime + gRPC health/metrics/SIGTERM.
///
/// **Process-fatal swarm (ADR P2-13):** `select!`s the runtime against gRPC
/// serve. On [`RuntimeError::SwarmPanic`] the gRPC drain is triggered so
/// aggregate health `""` flips to NOT_SERVING, then this function returns the
/// error so `main` can `process::exit(1)` — without waiting for an external
/// SIGTERM.
pub async fn run_process(
    bs: Bootstrap,
    spec: ServiceSpec,
    routes: Routes,
    runtime_cfg: RuntimeConfig,
    metrics: P2pMetrics,
    signal: SignalTrigger,
) -> Result<(), RuntimeError> {
    let (ready_tx, ready_rx) = oneshot::channel::<IdentitySnapshot>();
    let (stop_rt_tx, stop_rt_rx) = oneshot::channel::<()>();
    let (stop_grpc_tx, stop_grpc_rx) = oneshot::channel::<()>();

    // Health reporter is owned by bootstrap `serve_with_options`. On swarm
    // fatal we fire `stop_grpc_tx`, which completes the external signal and
    // runs `begin_shutdown` → aggregate NOT_SERVING before drain returns.
    let combined_signal = signal_or_fatal(signal, stop_grpc_rx);

    let runtime = cc_bootstrap::spawn("p2p-runtime", {
        let metrics = metrics.clone();
        async move {
            serve(
                runtime_cfg,
                metrics,
                RuntimeHooks {
                    on_ready: Some(ready_tx),
                    // Bootstrap owns the live reporter; fatal path uses signal
                    // drain for NOT_SERVING (see `signal_or_fatal` + select).
                    aggregate_health: None,
                    shutdown: Some(Box::pin(async move {
                        let _ = stop_rt_rx.await;
                    })),
                },
            )
            .await
        }
    });

    // Identity is loaded synchronously at the top of `serve`; ready arrives
    // before listen completes. Fail-fast if identity is bad.
    let ready = tokio::time::timeout(Duration::from_secs(10), ready_rx).await;
    match ready {
        Ok(Ok(snap)) => {
            info!(peer_id = %snap.peer_id, "p2p runtime ready");
        }
        Ok(Err(_)) | Err(_) => {
            let _ = stop_rt_tx.send(());
            let _ = stop_grpc_tx.send(());
            return map_runtime_join(runtime.await);
        }
    }

    let mut grpc = cc_bootstrap::spawn("grpc-serve", async move {
        serve_with_options(
            bs,
            spec,
            routes,
            ServeOptions::default(),
            combined_signal,
        )
        .await
    });
    let mut runtime = runtime;

    // Race: external SIGTERM drains cleanly; swarm panic must not leave us
    // SERVING and blocked on the serve future (ADR P2-13).
    tokio::select! {
        grpc_join = &mut grpc => {
            let _ = stop_rt_tx.send(());
            let rt = map_runtime_join(runtime.await);
            match grpc_join {
                Ok(Ok(())) => rt,
                Ok(Err(e)) => {
                    // Prefer swarm-fatal if both failed.
                    if matches!(rt, Err(RuntimeError::SwarmPanic { .. })) {
                        rt
                    } else {
                        Err(RuntimeError::Bootstrap(e))
                    }
                }
                Err(e) if e.is_cancelled() => rt,
                Err(e) => Err(RuntimeError::SwarmPanic {
                    task: "grpc-serve",
                    payload: format!("grpc join: {e}"),
                }),
            }
        }
        rt_join = &mut runtime => {
            let rt = map_runtime_join(rt_join);
            // Always stop gRPC so drain sets aggregate NOT_SERVING (fatal or not).
            let _ = stop_grpc_tx.send(());
            match tokio::time::timeout(Duration::from_secs(5), grpc).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => {
                    if !matches!(rt, Err(RuntimeError::SwarmPanic { .. })) {
                        return Err(RuntimeError::Bootstrap(e));
                    }
                }
                Ok(Err(_)) | Err(_) => {}
            }
            rt
        }
    }
}

/// Map a runtime task join into [`RuntimeError`].
fn map_runtime_join(
    join: Result<Result<IdentitySnapshot, RuntimeError>, tokio::task::JoinError>,
) -> Result<(), RuntimeError> {
    match join {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) if e.is_cancelled() => Ok(()),
        Err(e) => Err(RuntimeError::SwarmPanic {
            task: "p2p-runtime",
            payload: format!("runtime join: {e}"),
        }),
    }
}

/// Combine the production/test shutdown trigger with a fatal oneshot so a
/// swarm panic can complete gRPC drain (NOT_SERVING) without SIGTERM.
fn signal_or_fatal(
    signal: SignalTrigger,
    fatal_rx: oneshot::Receiver<()>,
) -> SignalTrigger {
    SignalTrigger::External(Box::pin(async move {
        match signal {
            SignalTrigger::UnixSignals => {
                tokio::select! {
                    _ = wait_unix_signal() => {}
                    _ = fatal_rx => {}
                }
            }
            SignalTrigger::External(ext) => {
                tokio::select! {
                    _ = ext => {}
                    _ = fatal_rx => {}
                }
            }
        }
    }))
}

async fn wait_unix_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(_) => {
                let _ = sigterm.recv().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Build a [`ServiceSpec`] for the p2p process (D-1: lives in L3).
#[must_use]
pub fn service_spec(
    grpc_addr: SocketAddr,
    metrics_addr: SocketAddr,
    peers: Vec<PeerSpec>,
    known_methods: Vec<String>,
) -> ServiceSpec {
    ServiceSpec {
        name: SERVICE,
        health_service_name: HEALTH_SERVICE_NAME,
        grpc_addr,
        metrics_addr,
        peers,
        descriptor_set: cc_proto::FILE_DESCRIPTOR_SET,
        known_methods,
    }
}

/// Fill a bounded cmd sender to capacity and publish the depth gauge (tests).
pub fn fill_cmd_queue_for_test(
    cmd_tx: &mpsc::Sender<SwarmCommand>,
    metrics: &P2pMetrics,
) -> usize {
    let mut n = 0;
    while cmd_tx.try_send(SwarmCommand::Noop).is_ok() {
        n += 1;
    }
    metrics.set_queue_depth(QueueName::Cmd, n as i64);
    n
}
