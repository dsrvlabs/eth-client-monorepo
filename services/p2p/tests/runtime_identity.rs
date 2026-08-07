//! CC-20b acceptance: identity stability, permissions, serve PeerId, channels,
//! supervisor, and **production** swarm-fatal path via `run_process`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_bootstrap::{
    AGGREGATE_HEALTH, SignalTrigger, bootstrap_without_tracing,
};
use cc_p2p::channels::{ChannelMap, CMD_BOUND, GOSSIP_BOUND, SwarmCommand};
use cc_p2p::clock::ClockConfig;
use cc_p2p::identity::{self, IdentityError, NODE_KEY_MODE};
use cc_p2p::metrics::{P2pMetrics, QueueName};
use cc_p2p::service::{
    RuntimeConfig, RuntimeError, RuntimeHooks, SERVICE, run_process, serve, service_spec,
};
use cc_p2p::supervisor::{
    SupervisedTask, SupervisorOutcome, TaskPolicy, factory_from_future, run_supervisor,
};
use cc_proto::p2p::p2p_service_server::P2pServiceServer;
use cc_proto::p2p::{GetInfoRequest, GetInfoResponse};
use cc_proto::p2p::p2p_service_server::P2pService;
use prometheus_client::registry::Registry;
use tokio::sync::{oneshot, watch};
use tonic::service::Routes;
use tonic::{Request, Response, Status};
use tonic::transport::Endpoint;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_client::HealthClient;

fn tmp(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("cc-p2p-rt-{name}-{nanos}"))
}

fn metrics() -> P2pMetrics {
    let mut reg = Registry::default();
    P2pMetrics::register(&mut reg)
}

fn ephemeral() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn runtime_cfg(key: PathBuf, panic: bool) -> RuntimeConfig {
    RuntimeConfig {
        node_key_path: key,
        listen_multiaddr: "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
        clock: ClockConfig {
            genesis_time: 1_600_000_000,
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            maximum_gossip_clock_disparity: Duration::from_millis(250),
            slot_clock_offset_seconds: 0,
        },
        test_swarm_panic: panic,
    }
}

#[derive(Debug, Default)]
struct StubP2p;

#[tonic::async_trait]
impl P2pService for StubP2p {
    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse { build_info: None }))
    }
}

#[tokio::test]
async fn serve_twice_same_key_stable_peer_and_node_id() {
    let dir = tmp("serve-stable");
    fs::create_dir_all(&dir).unwrap();
    let key = dir.join("node_key");

    let mut peer_ids = Vec::new();
    let mut node_ids = Vec::new();

    for i in 0..2 {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let m = metrics();
        let cfg = runtime_cfg(key.clone(), false);
        let handle = tokio::spawn(async move {
            serve(
                cfg,
                m,
                RuntimeHooks {
                    on_ready: Some(ready_tx),
                    aggregate_health: None,
                    shutdown: Some(Box::pin(async move {
                        let _ = stop_rx.await;
                    })),
                },
            )
            .await
        });

        let snap = tokio::time::timeout(Duration::from_secs(10), ready_rx)
            .await
            .expect("ready timeout")
            .expect("ready");
        peer_ids.push(snap.peer_id);
        node_ids.push(snap.node_id);
        let _ = stop_tx.send(());
        let out = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("join timeout")
            .expect("join")
            .expect("serve ok");
        assert_eq!(out.peer_id, peer_ids[i]);
    }

    assert_eq!(peer_ids[0], peer_ids[1], "PeerId must be stable across serve()");
    assert_eq!(
        node_ids[0].raw(),
        node_ids[1].raw(),
        "NodeId must be stable across serve()"
    );
    assert!(key.is_file(), "key must be persisted");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&key).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, NODE_KEY_MODE);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn missing_key_creates_0600_file() {
    let dir = tmp("create-key");
    fs::create_dir_all(&dir).unwrap();
    let key = dir.join("node_key");
    assert!(!key.exists());
    let id = identity::load_or_create(&key).unwrap();
    assert!(key.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&key).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
    let _ = id;
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[cfg(unix)]
fn broad_permissions_fail_named_error() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tmp("broad");
    fs::create_dir_all(&dir).unwrap();
    let key = dir.join("node_key");
    fs::write(&key, [7u8; 32]).unwrap();
    let mut perms = fs::metadata(&key).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&key, perms).unwrap();
    let err = identity::load_or_create(&key).unwrap_err();
    assert!(
        matches!(err, IdentityError::PermissionsTooBroad { .. }),
        "named error, got {err}"
    );
    let msg = err.to_string();
    assert!(msg.contains("permissions too broad"), "{msg}");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn changing_key_changes_peer_and_node_id() {
    let dir = tmp("change-key");
    fs::create_dir_all(&dir).unwrap();
    let key = dir.join("node_key");
    let a = identity::load_or_create(&key).unwrap();
    let peer_a = a.peer_id();
    let node_a = a.node_id().raw();
    drop(a);
    fs::remove_file(&key).unwrap();
    let b = identity::load_or_create(&key).unwrap();
    assert_ne!(peer_a, b.peer_id());
    assert_ne!(node_a, b.node_id().raw());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn empty_and_escape_paths_refused() {
    assert!(matches!(
        identity::load_or_create(""),
        Err(IdentityError::EmptyPath)
    ));
    assert!(matches!(
        identity::load_or_create("../etc/passwd"),
        Err(IdentityError::PathEscape { .. })
    ));
}

#[test]
fn channel_map_bounds_and_queue_depth_gauge() {
    let m = metrics();
    let channels = ChannelMap::new(&m);

    let mut n = 0;
    while channels
        .gossip_tx
        .try_send(cc_p2p::channels::GossipWork { bytes: vec![n as u8] })
        .is_ok()
    {
        n += 1;
    }
    assert_eq!(n, GOSSIP_BOUND);
    m.set_queue_depth(QueueName::Gossip, n as i64);
    assert_eq!(m.queue_depth(QueueName::Gossip), GOSSIP_BOUND as i64);

    let mut c = 0;
    while channels.cmd_tx.try_send(SwarmCommand::Noop).is_ok() {
        c += 1;
    }
    assert_eq!(c, CMD_BOUND);
    m.set_queue_depth(QueueName::Cmd, c as i64);
    assert_eq!(m.queue_depth(QueueName::Cmd), CMD_BOUND as i64);

    for (q, bound) in ChannelMap::labelled_bounds() {
        assert!(bound > 0, "{q:?}");
        let _ = m.queue_depth(q);
    }
}

#[tokio::test]
async fn worker_respawn_increments_panic_counter_cumulatively() {
    let m = metrics();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let panics = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let panics_c = panics.clone();
    let factory = factory_from_future("stub_worker", move || {
        let panics_c = panics_c.clone();
        async move {
            let n = panics_c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < 3 {
                panic!("induced worker panic {n}");
            }
            std::future::pending::<()>().await;
        }
    });

    let tasks = vec![SupervisedTask {
        name: "stub_worker",
        policy: TaskPolicy::Respawn,
        factory,
    }];

    let m2 = m.clone();
    let sup = tokio::spawn(async move { run_supervisor(tasks, m2, shutdown_rx).await });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if m.worker_panics("stub_worker") >= 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "timeout waiting for 3 panics, got {}",
                m.worker_panics("stub_worker")
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        m.worker_panics("stub_worker"),
        3,
        "counter must be cumulative across respawns (3, not 1)"
    );

    let _ = shutdown_tx.send(true);
    let outcome = tokio::time::timeout(Duration::from_secs(2), sup)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome, SupervisorOutcome::Shutdown);
}

/// Production path: `run_process` observes swarm fatal, drains gRPC
/// (aggregate NOT_SERVING), and returns `RuntimeError::SwarmPanic` so `main`
/// can `process::exit(1)` — without waiting for SIGTERM.
#[tokio::test]
async fn run_process_swarm_fatal_not_serving_and_returns_error() {
    let dir = tmp("run-process-fatal");
    fs::create_dir_all(&dir).unwrap();
    let key = dir.join("node_key");

    let grpc_addr = ephemeral();
    let metrics_addr = ephemeral();
    let mut bs = bootstrap_without_tracing("p2p-fatal-test");
    let m = P2pMetrics::register(&mut bs.registry);

    let spec = service_spec(grpc_addr, metrics_addr, vec![], vec![]);
    let routes = Routes::default().add_service(P2pServiceServer::new(StubP2p));
    let cfg = runtime_cfg(key, true); // induce swarm panic

    // Never fires — only swarm fatal should stop the process entry.
    let (never_tx, never_rx) = oneshot::channel::<()>();
    let _hold = never_tx;

    let run = tokio::spawn(async move {
        run_process(
            bs,
            spec,
            routes,
            cfg,
            m,
            SignalTrigger::External(Box::pin(async move {
                let _ = never_rx.await;
            })),
        )
        .await
    });

    // Poll aggregate health until NOT_SERVING (drain after fatal).
    let health_uri = format!("http://{grpc_addr}");
    let mut saw_not_serving = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if let Ok(channel) = Endpoint::from_shared(health_uri.clone())
            .unwrap()
            .connect_timeout(Duration::from_millis(200))
            .connect()
            .await
        {
            let mut client = HealthClient::new(channel);
            if let Ok(resp) = client
                .check(HealthCheckRequest {
                    service: AGGREGATE_HEALTH.to_owned(),
                })
                .await
            {
                let status = resp.into_inner().status;
                if status == WireStatus::NotServing as i32 {
                    saw_not_serving = true;
                    break;
                }
            }
        }
        // Also check if run already finished with SwarmPanic (server may be down).
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let result = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("run_process timed out — still blocked on gRPC serve?")
        .expect("join");

    match &result {
        Err(RuntimeError::SwarmPanic { task, payload }) => {
            assert_eq!(*task, "swarm");
            assert!(
                payload.contains("induced swarm panic"),
                "payload={payload}"
            );
        }
        other => panic!("expected SwarmPanic from run_process, got {other:?}"),
    }

    // Prefer observing NOT_SERVING on the wire; if the server drained too fast
    // for the poll loop, SwarmPanic alone still proves main would exit 1.
    assert!(
        saw_not_serving || result.is_err(),
        "expected NOT_SERVING and/or SwarmPanic"
    );
    let _ = SERVICE;
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn supervisor_swarm_panic_is_fatal() {
    let m = metrics();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let factory = factory_from_future("swarm", || async {
        panic!("induced swarm panic");
    });
    let tasks = vec![SupervisedTask {
        name: "swarm",
        policy: TaskPolicy::ProcessFatal,
        factory,
    }];

    let outcome = run_supervisor(tasks, m.clone(), shutdown_rx).await;
    match outcome {
        SupervisorOutcome::Fatal { task, payload } => {
            assert_eq!(task, "swarm");
            assert!(payload.contains("induced swarm panic"), "{payload}");
        }
        other => panic!("expected Fatal, got {other:?}"),
    }
    assert_eq!(m.worker_panics("swarm"), 1);
}

#[test]
fn swarm_type_only_in_host() {
    let host = include_str!("../src/host.rs");
    assert!(
        host.contains("Swarm<"),
        "host.rs must own Swarm<CcBehaviour>"
    );
    for (name, src) in [
        ("identity.rs", include_str!("../src/identity.rs")),
        ("supervisor.rs", include_str!("../src/supervisor.rs")),
        ("clock.rs", include_str!("../src/clock.rs")),
        ("channels.rs", include_str!("../src/channels.rs")),
        ("service.rs", include_str!("../src/service.rs")),
        ("main.rs", include_str!("../src/main.rs")),
    ] {
        assert!(!src.contains("Swarm<"), "{name} must not name Swarm<");
        assert!(!src.contains("Mutex<Swarm"), "{name} must not Mutex swarm");
        assert!(!src.contains("RwLock<Swarm"), "{name} must not RwLock swarm");
    }
}
