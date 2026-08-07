//! P2P runtime wiring — CC-20b + CC-21d gRPC surface.
//!
//! Starts the swarm task (sole `Swarm` owner), supervisor, clock, and §2.2
//! channel map with stub consumers. **No dialling.** Keeps the Phase 0
//! health / metrics surface startable via [`serve`] / [`run_process`].
//!
//! **CC-21d:** [`P2pGrpcService`] exposes `SetCustodyGroupCount` and a Rust
//! [`P2pGrpcService::set_custody_group_count`] method. Phase 2 has **no
//! production caller** — Phase 6 attaches a real invoker; until then the
//! default invoker returns `failed_precondition`.

use std::fmt;
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
use cc_proto::common::BuildInfo;
use cc_proto::p2p::p2p_service_server::P2pService;
use cc_proto::p2p::{
    GetInfoRequest, GetInfoResponse, SetCustodyGroupCountRequest, SetCustodyGroupCountResponse,
};
use discv5::enr::NodeId;
use tokio::sync::{mpsc, oneshot, watch};
use tonic::service::Routes;
use tonic::{Request, Response, Status};
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;
use tracing::{error, info};

use crate::chain_stream::{
    ChainStreamConfig, ChainStreamHandle, run_chain_stream_client, run_publish_dispatch,
};
use crate::channels::{self, ChannelMap, SwarmCommand, stub_consumer};
use crate::clock::{ClockConfig, SlotClock};
use cc_types::ChainConfig;
use std::sync::Arc;
use crate::discovery::cgc_hook::CgcHookError;
use crate::discovery::{
    DIAL_QUEUE_BOUND, DiscoveryConfig, DiscoveryPeerView, DiscoveryTask, build_enr_manager,
    run_discovery_task,
};
use crate::fork_digest::ForkContext;
use crate::host::{build_host_swarm, run_swarm_task, HandshakeRuntime, SwarmTask};
use crate::reqresp::{CgcPolicy, HandshakeDeps};
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
    /// Discovery / discv5 knobs (CC-21c). Empty bootnodes → discovery still
    /// starts but finds nobody until peers are configured.
    pub discovery: DiscoveryConfig,
    /// Optional path to consensus-specs network YAML for [`ForkContext`].
    pub network_config_path: Option<PathBuf>,
    /// Optional genesis validators root (32 bytes). Required with network config
    /// for a correct `eth2` ENR field.
    pub genesis_validators_root: Option<[u8; 32]>,
    /// When false, discovery task is not spawned (tests / offline).
    pub enable_discovery: bool,
    /// gRPC URI for the chain service (`peers.chain` / `CC_P2P_PEERS__CHAIN`).
    /// Empty disables the chain-stream client (tests / offline).
    pub chain_uri: String,
    /// When false, do not spawn the chain-stream client (unit tests).
    pub enable_chain_stream: bool,
    /// Gossipsub heartbeat interval — stall bound is derived from this (§2.3).
    pub heartbeat_interval: Duration,
    /// **Test-only:** swarm task panics immediately so the process-fatal path
    /// can be exercised through [`run_process`] without a real host crash.
    /// Production always leaves this `false`.
    pub test_swarm_panic: bool,
    /// CC-23b: when true, peers advertising `cgc < CUSTODY_REQUIREMENT` are
    /// Goodbye'd. Default **false** (accept) — see `docs/p2p-dependencies.md`.
    pub reject_low_cgc_peers: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            node_key_path: PathBuf::from(identity::DEFAULT_NODE_KEY_PATH),
            listen_multiaddr: default_listen_multiaddr(),
            clock: ClockConfig::default(),
            peer_manager: PeerManagerConfig::default(),
            discovery: DiscoveryConfig::default(),
            network_config_path: None,
            genesis_validators_root: None,
            enable_discovery: true,
            chain_uri: String::new(),
            enable_chain_stream: false,
            // Match `cc_libp2p::BehaviourConfig::default().heartbeat_interval`.
            heartbeat_interval: Duration::from_secs(1),
            test_swarm_panic: false,
            reject_low_cgc_peers: false,
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

/// Extract the first TCP port from a multiaddr, if any.
fn tcp_port_from_multiaddr(addr: &Multiaddr) -> Option<u16> {
    for p in addr.iter() {
        if let Protocol::Tcp(port) = p {
            return Some(port);
        }
    }
    None
}

/// Build a [`ForkContext`] from optional network config + GVR.
///
/// Falls back to the committed Hoodi fixture embedded via `include_str!` so
/// discovery can still start when operators omit `network_config`.
fn build_fork_context(cfg: &RuntimeConfig, epoch: u64) -> ForkContext {
    use cc_types::{ChainConfig, Epoch, Root};

    let epoch = Epoch::new(epoch);
    let gvr = cfg
        .genesis_validators_root
        .map(Root::from_array)
        .unwrap_or(Root::ZERO);

    if let Some(path) = &cfg.network_config_path {
        match ChainConfig::from_yaml_file(path) {
            Ok(chain) => return ForkContext::new(chain, gvr, epoch),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "network_config load failed; falling back to embedded Hoodi"
                );
            }
        }
    }

    // Embedded at compile time — always available without filesystem layout.
    // The fixture is a workspace invariant; parse failure means ChainConfig
    // schema drift and is treated as unreachable for a well-formed tree.
    const EMBEDDED_HOODI: &str =
        include_str!("../../../crates/types/tests/fixtures/hoodi-config.yaml");
    let chain = match ChainConfig::from_yaml_str(EMBEDDED_HOODI) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "embedded hoodi-config.yaml rejected");
            unreachable!("embedded hoodi-config.yaml must parse as ChainConfig");
        }
    };
    ForkContext::new(chain, gvr, epoch)
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
    let epoch_rx = clock.spawn_epoch_ticks();
    let initial_epoch = clock.current_epoch();

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
    // CC-23b: epoch → Status re-exchange (same cmd path as peer manager).
    let status_epoch_cmd_tx = channels.cmd_tx.clone();
    let peer_cfg = cfg.peer_manager.clone();

    // Discovery → peer manager dial channel (bound = dial queue capacity).
    let (dial_tx, dial_rx) = mpsc::channel(DIAL_QUEUE_BOUND);
    let (peer_view_tx, peer_view_rx) = watch::channel(DiscoveryPeerView::default());
    // Second receiver for StatusEpoch worker (same watch).
    let status_epoch_peer_view = peer_view_rx.clone();

    // Chain-stream handle (view ArcSwap + publish drop counter) — shared.
    let chain_stream_handle = ChainStreamHandle::new();
    let chain_out_tx = channels.chain_out_tx.clone();

    // Validation → peer-manager app-score penalties (CC-22d).
    let (penalty_tx, penalty_rx) =
        mpsc::channel::<crate::channels::PeerPenaltyCmd>(crate::channels::PENALTY_BOUND);

    // ── 5. Workers / stub consumers ────────────────────────────────────────
    // Gossip validation pool is CC-22d. Reqresp/kzg remain stubs until
    // CC-23*/24*. Chain stream (CC-27b) replaces chain_out/chain_in stubs
    // when enabled. Peer manager owns `conn_rx`.
    spawn_edge_workers(
        channels,
        metrics.clone(),
        &cfg,
        clock.clone(),
        chain_stream_handle.clone(),
        penalty_tx,
        shutdown_rx.clone(),
    );

    // Fork context for ENR eth2/nfd (best-effort when network config is present).
    let fork_ctx = build_fork_context(&cfg, initial_epoch);

    // ── 6. Supervisor ──────────────────────────────────────────────────────
    let (swarm_factory, peer_factory, discovery_factory) = if cfg.test_swarm_panic {
        // Induced panic for process-fatal production-path tests.
        drop(cmd_rx);
        drop(conn_rx);
        drop(peer_cmd_tx);
        drop(dial_tx);
        drop(dial_rx);
        drop(chain_out_tx);
        let swarm_factory = factory_from_future("swarm", || async {
            #[allow(clippy::panic)] // test-only RuntimeConfig.test_swarm_panic
            {
                panic!("induced swarm panic");
            }
        });
        let peer_factory = factory_from_future("peer_manager", || async {
            std::future::pending::<()>().await;
        });
        let discovery_factory = factory_from_future("discovery", || async {
            std::future::pending::<()>().await;
        });
        (swarm_factory, peer_factory, discovery_factory)
    } else {
        let swarm = build_host_swarm(identity.keypair().clone())?;
        assert_eq!(snapshot.peer_id, *swarm.local_peer_id());
        // CC-23b handshake: ChainView from stream client, serve window seed empty,
        // local MetaData at CUSTODY_REQUIREMENT, cgc policy from config.
        let mut handshake_deps = HandshakeDeps::new(
            cc_types::Slot::new(0),
            cc_types::CUSTODY_REQUIREMENT,
        );
        handshake_deps.view = chain_stream_handle.view.clone();
        handshake_deps.cgc_policy = if cfg.reject_low_cgc_peers {
            CgcPolicy::reject_low_cgc()
        } else {
            CgcPolicy::accept_low_cgc()
        };
        let handshake = HandshakeRuntime::new(fork_ctx.clone(), handshake_deps);
        let swarm_task = SwarmTask::new(
            swarm,
            cmd_rx,
            gossip_tx,
            reqresp_in_tx,
            conn_tx,
            chain_out_tx,
            cfg.heartbeat_interval,
            metrics.clone(),
        )
        .with_handshake(handshake);
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
        let peer_view_tx_pm = peer_view_tx;
        let peer_cell = Mutex::new(Some((
            conn_rx,
            peer_cmd_tx,
            peer_cfg,
            peer_metrics,
            dial_rx,
            peer_view_tx_pm,
            penalty_rx,
        )));
        let peer_factory = factory_from_future("peer_manager", move || {
            let taken = peer_cell
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let mut shutdown = peer_shutdown.clone();
            async move {
                if let Some((
                    conn_rx,
                    cmd_tx,
                    config,
                    metrics,
                    dial_rx,
                    peer_view_tx,
                    penalty_rx,
                )) = taken
                {
                    let manager = PeerManager::new(config, cmd_tx, metrics);
                    run_peer_manager(
                        manager,
                        conn_rx,
                        Some(dial_rx),
                        Some(penalty_rx),
                        Some(peer_view_tx),
                        shutdown,
                    )
                    .await;
                } else {
                    let _ = shutdown.changed().await;
                }
            }
        });

        // Discovery driver: RespawnBudget (fatal after 5 restarts / 5 min).
        // Factory **rebuilds** EnrManager from the retained identity secret on
        // every spawn (B2) — no one-shot take that parks forever after first run.
        let discovery_factory = if cfg.enable_discovery {
            let disc_cfg = {
                let mut d = cfg.discovery.clone();
                d.target_peers = cfg.peer_manager.target_peers;
                // Align UDP with TCP port from listen multiaddr when unset.
                if let Some(tcp) = tcp_port_from_multiaddr(&cfg.listen_multiaddr) {
                    d.tcp_port = tcp;
                    if d.listen_udp == 0 || d.listen_udp == 9000 {
                        d.listen_udp = tcp;
                    }
                }
                d
            };
            // Validate key once at start so we fail before bind on bad identity.
            let _ = identity.combined_key().map_err(RuntimeError::Identity)?;
            let identity_for_disc = identity.clone();
            let disc_metrics = metrics.clone();
            let disc_shutdown = shutdown_rx.clone();
            let disc_epoch = epoch_rx.clone();
            let disc_peer_view = peer_view_rx.clone();
            let dial_tx_factory = dial_tx;
            let fork_ctx_seed = fork_ctx;
            factory_from_future("discovery", move || {
                let identity = identity_for_disc.clone();
                let disc_cfg = disc_cfg.clone();
                let metrics = disc_metrics.clone();
                let shutdown = disc_shutdown.clone();
                let epoch_rx = disc_epoch.clone();
                let peer_view_rx = disc_peer_view.clone();
                let dial_tx = dial_tx_factory.clone();
                let mut fork_ctx = fork_ctx_seed.clone();
                // Refresh epoch from the clock watch so respawns are not stuck
                // on the pre-start epoch.
                let epoch_now = *epoch_rx.borrow();
                fork_ctx.on_epoch(cc_types::Epoch::new(epoch_now));
                async move {
                    // Panics here are intentional: RespawnBudget counts them and
                    // re-invokes this factory, which rebuilds EnrManager (B2).
                    #[allow(clippy::panic)]
                    let run = async {
                        let enr_key = identity
                            .combined_key()
                            .unwrap_or_else(|e| panic!("discovery identity CombinedKey: {e}"));
                        let manager = build_enr_manager(enr_key, &disc_cfg)
                            .unwrap_or_else(|e| panic!("discovery EnrManager build failed: {e}"));
                        run_discovery_task(DiscoveryTask {
                            manager,
                            cfg: disc_cfg,
                            fork_ctx,
                            epoch_rx,
                            peer_view_rx,
                            dial_tx,
                            metrics,
                            shutdown: shutdown.clone(),
                        })
                        .await;
                        // Unexpected clean exit → panic so budget applies.
                        if !*shutdown.borrow() {
                            panic!("discovery task exited unexpectedly");
                        }
                    };
                    run.await;
                }
            })
        } else {
            drop(dial_tx);
            factory_from_future("discovery", || async {
                std::future::pending::<()>().await;
            })
        };

        (swarm_factory, peer_factory, discovery_factory)
    };

    // Idle respawnable worker so the respawn policy is live before CC-22*.
    let idle_factory = factory_from_future("idle_worker", || async {
        std::future::pending::<()>().await;
    });

    // CC-23b: live per-epoch Status re-exchange + handshake ForkContext advance.
    // Driven by the same epoch_rx discovery uses; connected set from peer view.
    {
        let mut epoch_rx = epoch_rx.clone();
        let peer_view_rx = status_epoch_peer_view;
        let cmd_tx = status_epoch_cmd_tx;
        let mut shutdown = shutdown_rx.clone();
        cc_bootstrap::spawn("status-epoch", async move {
            let mut last_epoch = *epoch_rx.borrow();
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                    result = epoch_rx.changed() => {
                        if result.is_err() {
                            break;
                        }
                        let epoch = *epoch_rx.borrow();
                        if epoch == last_epoch {
                            continue;
                        }
                        last_epoch = epoch;
                        let connected = peer_view_rx.borrow().active.clone();
                        if cmd_tx
                            .send(SwarmCommand::StatusEpoch { epoch, connected })
                            .await
                            .is_err()
                        {
                            break;
                        }
                        info!(epoch, "status epoch re-exchange scheduled");
                    }
                }
            }
        });
    }

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
            name: "discovery",
            policy: TaskPolicy::discovery(),
            factory: discovery_factory,
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

/// Spawn edge consumers: gossip validation pool, stubs for unclaimed edges,
/// chain-stream client when enabled.
fn spawn_edge_workers(
    channels: ChannelMap,
    metrics: P2pMetrics,
    cfg: &RuntimeConfig,
    clock: SlotClock,
    handle: ChainStreamHandle,
    penalty_tx: mpsc::Sender<crate::channels::PeerPenaltyCmd>,
    shutdown: watch::Receiver<bool>,
) {
    let ChannelMap {
        gossip_rx,
        reqresp_in_rx,
        conn_rx: _, // owned by peer manager (CC-20c)
        kzg_rx,
        chain_out_rx,
        chain_in_rx,
        publish_rx,
        cmd_tx,
        publish_tx,
        gossip_tx: _,
        reqresp_in_tx: _,
        conn_tx: _,
        kzg_tx: _,
        chain_out_tx,
        chain_in_tx,
        cmd_rx: _,
    } = channels;

    // CC-22d validation pool (replaces gossip stub).
    let chain_cfg = load_chain_config_for_validation(cfg);
    let view_store = handle.view.clone();
    let view_fn: std::sync::Arc<dyn Fn() -> cc_proto::p2p::ChainView + Send + Sync> =
        std::sync::Arc::new(move || (*view_store.load()).clone());
    let pool = crate::gossip::validate::ValidationPool::with_chain_uri(
        std::sync::Arc::new(chain_cfg),
        clock,
        view_fn,
        chain_out_tx.clone(),
        cmd_tx.clone(),
        penalty_tx.clone(),
        metrics.clone(),
        cfg.chain_uri.clone(),
    );
    // CC-24b: share CC-22d's single inclusion-proof LRU with the KZG pool
    // (do not create a second cache).
    let shared_inclusion_cache = {
        let guard = match pool.state.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        std::sync::Arc::clone(&guard.column.inclusion_cache)
    };
    let pool_state = Arc::clone(&pool.state);
    cc_bootstrap::spawn(
        "gossip-validate",
        crate::gossip::validate::run_validation_pool(pool, gossip_rx),
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
    // CC-24b: dedicated OS-thread KZG verify pool (ADR P2-02).
    let kzg_backend: std::sync::Arc<dyn cc_crypto::CellKzg> =
        match cc_crypto::CKzgBackend::load_default() {
            Ok(b) => std::sync::Arc::new(b),
            Err(e) => {
                error!(error = %e, "KZG trusted setup unavailable; verify pool fail-closed");
                std::sync::Arc::new(crate::das::verify_pool::FailClosedCellKzg)
            }
        };
    let verify_pool = std::sync::Arc::new(crate::das::VerifyPool::start_with_cache(
        kzg_backend,
        Some(metrics.clone()),
        shared_inclusion_cache,
    ));
    let m = metrics.clone();
    let pool_for_bridge = std::sync::Arc::clone(&verify_pool);
    cc_bootstrap::spawn("kzg-verify-pool", async move {
        crate::das::run_verify_pool_bridge(kzg_rx, pool_for_bridge, m).await;
        // Hold the pool Arc until the bridge task ends so workers stay alive.
        drop(verify_pool);
    });

    // Publish queue → cmd bridge (always on).
    let m = metrics.clone();
    cc_bootstrap::spawn("publish-bridge", async move {
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

    let enable_stream = cfg.enable_chain_stream && !cfg.chain_uri.is_empty();
    if enable_stream {
        // Proto PublishRequest fan-in from the stream client → publish_tx.
        let (proto_pub_tx, proto_pub_rx) = mpsc::channel(channels::PUBLISH_BOUND);
        let pub_metrics = metrics.clone();
        let drops = handle.publish_drops.clone();
        let pub_tx = publish_tx;
        cc_bootstrap::spawn("chain-publish-dispatch", async move {
            run_publish_dispatch(proto_pub_rx, pub_tx, pub_metrics, drops).await;
        });

        // Late chain verdicts (import_invalid after ACCEPT) — no re-report.
        let m = metrics.clone();
        let pen = penalty_tx;
        cc_bootstrap::spawn(
            "chain-in-late-verdicts",
            crate::gossip::validate::run_chain_in_late_verdicts(pool_state, chain_in_rx, pen, m),
        );

        let stream_cfg = ChainStreamConfig {
            chain_uri: cfg.chain_uri.clone(),
            ..ChainStreamConfig::default()
        };
        let m = metrics.clone();
        let h = handle;
        cc_bootstrap::spawn("chain-stream-client", async move {
            // Never fatal (§2.4): the client is already a reconnect loop.
            run_chain_stream_client(
                stream_cfg,
                chain_out_rx,
                chain_in_tx,
                proto_pub_tx,
                h,
                m,
                shutdown,
            )
            .await;
        });
        info!(uri = %cfg.chain_uri, "chain-stream client spawned");
    } else {
        // Offline / tests: drain chain edges so producers never hang.
        let m = metrics.clone();
        cc_bootstrap::spawn(
            "stub-chain-out",
            stub_consumer("chain_out", chain_out_rx, m, None),
        );
        // Still run late-verdict consumer so chain_in is never blocked if wired.
        let m = metrics.clone();
        let pen = penalty_tx;
        cc_bootstrap::spawn(
            "chain-in-late-verdicts",
            crate::gossip::validate::run_chain_in_late_verdicts(pool_state, chain_in_rx, pen, m),
        );
        drop(chain_in_tx);
        drop(publish_tx);
        drop(handle);
        drop(shutdown);
    }
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

/// Chain config for gossip validators (`get_blob_parameters`, fork versions).
fn load_chain_config_for_validation(cfg: &RuntimeConfig) -> ChainConfig {
    if let Some(path) = &cfg.network_config_path
        && let Ok(c) = ChainConfig::from_yaml_file(path)
    {
        return c;
    }
    const EMBEDDED_HOODI: &str =
        include_str!("../../../crates/types/tests/fixtures/hoodi-config.yaml");
    match ChainConfig::from_yaml_str(EMBEDDED_HOODI) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "embedded hoodi-config.yaml rejected");
            // Unreachable for a well-formed tree; keep process alive with a
            // default-empty schedule only as last resort (validators will be
            // strict on blob bounds).
            unreachable!("embedded hoodi-config.yaml must parse as ChainConfig");
        }
    }
}

// ── CC-21d: gRPC + Rust `set_custody_group_count` surface ───────────────────

/// Invoker behind [`P2pGrpcService::set_custody_group_count`].
///
/// Production Phase 2 leaves the default unattached invoker; tests and Phase 6
/// install a real one that drives [`crate::discovery::set_custody_group_count`].
pub trait CgcHookInvoker: Send + Sync {
    /// Apply a new custody group count (five §6.4 effects).
    ///
    /// # Errors
    ///
    /// Propagates [`CgcHookError`] from the underlying hook.
    fn set_custody_group_count(&self, n: u64) -> Result<(), CgcHookError>;
}

/// Default invoker: hook not attached to live EnrManager/registry yet.
#[derive(Debug, Default)]
pub struct UnattachedCgcHook;

impl CgcHookInvoker for UnattachedCgcHook {
    fn set_custody_group_count(&self, _n: u64) -> Result<(), CgcHookError> {
        // Deliberately does **not** call the five-effect free function — there
        // is no production caller in Phase 2 and the live registry/ENR are not
        // co-owned by the gRPC task yet. Phase 6 attaches a real invoker.
        Err(CgcHookError::NotAttached)
    }
}

/// gRPC `eth.p2p.v1.P2pService` implementation (GetInfo + SetCustodyGroupCount).
#[derive(Clone)]
pub struct P2pGrpcService {
    cgc: Arc<dyn CgcHookInvoker>,
}

impl fmt::Debug for P2pGrpcService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("P2pGrpcService").finish_non_exhaustive()
    }
}

impl Default for P2pGrpcService {
    fn default() -> Self {
        Self::new()
    }
}

impl P2pGrpcService {
    /// Construct with the unattached Phase-2 default invoker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cgc: Arc::new(UnattachedCgcHook),
        }
    }

    /// Construct with a custom invoker (tests / Phase 6 wiring).
    #[must_use]
    pub fn with_invoker(invoker: Arc<dyn CgcHookInvoker>) -> Self {
        Self { cgc: invoker }
    }

    /// Rust method: same entry point as the `SetCustodyGroupCount` RPC.
    ///
    /// # Errors
    ///
    /// Propagates [`CgcHookError`] from the attached invoker.
    pub fn set_custody_group_count(&self, n: u64) -> Result<(), CgcHookError> {
        self.cgc.set_custody_group_count(n)
    }
}

#[tonic::async_trait]
impl P2pService for P2pGrpcService {
    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse {
            build_info: Some(BuildInfo {
                service: SERVICE.to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                git_sha: cc_bootstrap::GIT_SHA.to_owned(),
                rustc: cc_bootstrap::RUSTC.to_owned(),
            }),
        }))
    }

    async fn set_custody_group_count(
        &self,
        request: Request<SetCustodyGroupCountRequest>,
    ) -> Result<Response<SetCustodyGroupCountResponse>, Status> {
        let cgc = request.into_inner().cgc;
        // Handler calls the Rust method of the same name (CC-21d).
        match self.set_custody_group_count(cgc) {
            Ok(()) => Ok(Response::new(SetCustodyGroupCountResponse {})),
            Err(CgcHookError::NotAttached) => Err(Status::failed_precondition(
                "set_custody_group_count: cgc hook not attached (Phase 2 — no production caller)",
            )),
            Err(CgcHookError::CgcOutOfRange { got }) => Err(Status::invalid_argument(format!(
                "cgc {got} out of range"
            ))),
            Err(CgcHookError::Registry(e)) => {
                Err(Status::internal(format!("cgc hook registry: {e}")))
            }
            Err(CgcHookError::Enr(e)) => Err(Status::internal(format!("cgc hook enr: {e}"))),
        }
    }
}
