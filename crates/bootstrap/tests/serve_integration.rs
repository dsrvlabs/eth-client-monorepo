//! CC-05b acceptance: health model, peer prober, reflection, SIGTERM, metrics layer.
// `kill(2)` for the SIGTERM self-signal test; exclusive to this nextest process.
#![allow(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::process::Command;
use std::sync::Once;
use std::time::Duration;

use cc_bootstrap::{
    AGGREGATE_HEALTH, PeerSpec, ServiceSpec, TelemetrySettings, bootstrap_without_tracing, init,
    serve, serve_with_shutdown,
};
use cc_proto::chain::chain_service_server::{ChainService, ChainServiceServer};
use cc_proto::chain::{GetInfoRequest, GetInfoResponse};
use cc_proto::common::BuildInfo;
use tokio::sync::oneshot;
use tonic::service::Routes;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_client::HealthClient;

static INIT: Once = Once::new();

fn ensure_tracing() {
    INIT.call_once(|| {
        // If subscriber already installed, continue without the registry from init.
        let _ = init(
            "test-bootstrap",
            TelemetrySettings {
                log_format: "json".into(),
                log_filter: "info".into(),
            },
        );
    });
}

fn ephemeral() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn http_uri(addr: SocketAddr) -> http::Uri {
    format!("http://{addr}").parse().unwrap()
}

#[derive(Debug, Default)]
struct StubChain;

#[tonic::async_trait]
impl ChainService for StubChain {
    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse {
            build_info: Some(BuildInfo {
                service: "a".into(),
                version: "0.1.0".into(),
                git_sha: "test".into(),
                rustc: "test".into(),
            }),
        }))
    }
}

fn chain_routes() -> Routes {
    Routes::default().add_service(ChainServiceServer::new(StubChain))
}

fn chain_spec(
    name: &'static str,
    health: &'static str,
    grpc: SocketAddr,
    metrics: SocketAddr,
    peers: Vec<PeerSpec>,
) -> ServiceSpec {
    ServiceSpec {
        name,
        health_service_name: health,
        grpc_addr: grpc,
        metrics_addr: metrics,
        peers,
        descriptor_set: cc_proto::FILE_DESCRIPTOR_SET,
        known_methods: vec!["/eth.chain.v1.ChainService/GetInfo".into()],
    }
}

async fn health_status(addr: SocketAddr, service: &str) -> Result<i32, tonic::Status> {
    let channel = Endpoint::from_shared(http_uri(addr).to_string())
        .unwrap()
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: service.to_owned(),
        })
        .await?;
    Ok(resp.into_inner().status)
}

async fn wait_health(addr: SocketAddr, service: &str, want: WireStatus, budget: Duration) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        match health_status(addr, service).await {
            Ok(status) if status == want as i32 => return,
            Ok(status) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "health {service:?} at {addr} still {status}, want {:?} within {budget:?}",
                        want
                    );
                }
            }
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("health {service:?} at {addr} unreachable: {e} within {budget:?}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn fetch_metrics(addr: SocketAddr) -> String {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = http::Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();
    let res = sender.send_request(req).await.unwrap();
    assert_eq!(res.status(), hyper::StatusCode::OK);
    let body = http_body_util::BodyExt::collect(res.into_body())
        .await
        .unwrap()
        .to_bytes();
    String::from_utf8(body.to_vec()).unwrap()
}

/// Two in-process services: B declares A as peer.
/// Asserts aggregate SERVING ↔ peer up/down within 15 s, and `cc_peer_health` tracks it.
/// Also asserts FQ self-health stays SERVING while aggregate is NOT_SERVING.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_prober_aggregate_and_self_health() {
    ensure_tracing();

    let a_grpc = ephemeral();
    let a_metrics = ephemeral();
    let b_grpc = ephemeral();
    let b_metrics = ephemeral();

    let (stop_a_tx, stop_a_rx) = oneshot::channel::<()>();
    let (stop_b_tx, stop_b_rx) = oneshot::channel::<()>();

    let a_bs = bootstrap_without_tracing("a");
    let a_spec = chain_spec("a", "eth.chain.v1.ChainService", a_grpc, a_metrics, vec![]);
    let a_handle = tokio::spawn(async move {
        serve_with_shutdown(a_bs, a_spec, chain_routes(), async move {
            let _ = stop_a_rx.await;
        })
        .await
    });

    // Wait for A aggregate SERVING before starting B (B will probe A).
    wait_health(
        a_grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    let b_bs = bootstrap_without_tracing("b");
    let b_spec = chain_spec(
        "b",
        "eth.chain.v1.ChainService",
        b_grpc,
        b_metrics,
        vec![PeerSpec {
            name: "a".into(),
            uri: http_uri(a_grpc),
        }],
    );
    let b_handle = tokio::spawn(async move {
        serve_with_shutdown(b_bs, b_spec, chain_routes(), async move {
            let _ = stop_b_rx.await;
        })
        .await
    });

    // Both report SERVING on ""; self FQ is SERVING.
    wait_health(
        b_grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(15),
    )
    .await;
    wait_health(
        b_grpc,
        "eth.chain.v1.ChainService",
        WireStatus::Serving,
        Duration::from_secs(2),
    )
    .await;

    // cc_peer_health{peer="a"} == 1 on B.
    let text = fetch_metrics(b_metrics).await;
    assert!(
        text.lines().any(|l| {
            l.contains("cc_peer_health") && l.contains("peer=\"a\"") && l.ends_with("} 1")
        }),
        "expected cc_peer_health peer=a 1, got:\n{text}"
    );

    // Stop A → B's aggregate flips NOT_SERVING within 15 s; self stays SERVING.
    let _ = stop_a_tx.send(());
    let a_result = a_handle.await.unwrap();
    assert!(a_result.is_ok(), "A serve: {a_result:?}");

    wait_health(
        b_grpc,
        AGGREGATE_HEALTH,
        WireStatus::NotServing,
        Duration::from_secs(15),
    )
    .await;
    wait_health(
        b_grpc,
        "eth.chain.v1.ChainService",
        WireStatus::Serving,
        Duration::from_secs(2),
    )
    .await;

    let text = fetch_metrics(b_metrics).await;
    assert!(
        text.lines().any(|l| {
            l.contains("cc_peer_health") && l.contains("peer=\"a\"") && l.ends_with("} 0")
        }),
        "expected cc_peer_health peer=a 0, got:\n{text}"
    );

    // Restart A → B recovers to SERVING; peer health back to 1.
    let (stop_a2_tx, stop_a2_rx) = oneshot::channel::<()>();
    let a2_grpc = a_grpc; // reuse same address so B's prober finds it
    let a2_metrics = ephemeral();
    let a2_bs = bootstrap_without_tracing("a");
    let a2_spec = chain_spec(
        "a",
        "eth.chain.v1.ChainService",
        a2_grpc,
        a2_metrics,
        vec![],
    );
    let a2_handle = tokio::spawn(async move {
        serve_with_shutdown(a2_bs, a2_spec, chain_routes(), async move {
            let _ = stop_a2_rx.await;
        })
        .await
    });

    wait_health(
        a2_grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;
    wait_health(
        b_grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(15),
    )
    .await;

    let text = fetch_metrics(b_metrics).await;
    assert!(
        text.lines().any(|l| {
            l.contains("cc_peer_health") && l.contains("peer=\"a\"") && l.ends_with("} 1")
        }),
        "expected cc_peer_health peer=a 1 after restart, got:\n{text}"
    );

    let _ = stop_a2_tx.send(());
    let _ = stop_b_tx.send(());
    let _ = a2_handle.await;
    let _ = b_handle.await;
}

/// `grpcurl -plaintext <addr> list` lists the service via reflection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reflection_lists_service_via_grpcurl() {
    ensure_tracing();

    let grpc = ephemeral();
    let metrics = ephemeral();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let bs = bootstrap_without_tracing("reflect");
    let spec = chain_spec(
        "reflect",
        "eth.chain.v1.ChainService",
        grpc,
        metrics,
        vec![],
    );
    let handle = tokio::spawn(async move {
        serve_with_shutdown(bs, spec, chain_routes(), async move {
            let _ = stop_rx.await;
        })
        .await
    });

    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    let output = Command::new("grpcurl")
        .args(["-plaintext", &grpc.to_string(), "list"])
        .output()
        .expect("grpcurl must be installed for this test");
    assert!(
        output.status.success(),
        "grpcurl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("eth.chain.v1.ChainService"),
        "grpcurl list missing ChainService:\n{stdout}"
    );
    assert!(
        stdout.contains("grpc.health.v1.Health"),
        "grpcurl list missing Health:\n{stdout}"
    );

    let _ = stop_tx.send(());
    let _ = handle.await;
}

/// Metrics layer applied by serve: gRPC call increments `cc_grpc_requests_total`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_applies_grpc_metrics_layer() {
    ensure_tracing();

    let grpc = ephemeral();
    let metrics = ephemeral();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let bs = bootstrap_without_tracing("metrics-svc");
    let spec = chain_spec(
        "metrics-svc",
        "eth.chain.v1.ChainService",
        grpc,
        metrics,
        vec![],
    );
    let handle = tokio::spawn(async move {
        serve_with_shutdown(bs, spec, chain_routes(), async move {
            let _ = stop_rx.await;
        })
        .await
    });

    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    // Call GetInfo through the served instance.
    let channel = Endpoint::from_shared(http_uri(grpc).to_string())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = cc_proto::chain::chain_service_client::ChainServiceClient::new(channel);
    let _ = client.get_info(GetInfoRequest {}).await.expect("GetInfo");

    // Allow the metrics body path to record.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let text = fetch_metrics(metrics).await;
    assert!(
        text.contains("cc_grpc_requests_total"),
        "missing counter family:\n{text}"
    );
    assert!(
        text.contains("method=\"/eth.chain.v1.ChainService/GetInfo\"")
            || text.contains("method=\"unknown\""),
        "expected GetInfo or unknown method label:\n{text}"
    );
    // At least one series with a non-zero count.
    let saw_inc = text.lines().any(|l| {
        l.starts_with("cc_grpc_requests_total{")
            && (l.ends_with("} 1") || l.contains("} 1.") || l.ends_with(" 1"))
    });
    assert!(saw_inc, "counter did not increment:\n{text}");

    let _ = stop_tx.send(());
    let _ = handle.await;
}

/// SIGTERM → aggregate NOT_SERVING before listener closes; serve returns Ok(()).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_sets_not_serving_then_exits_ok() {
    ensure_tracing();

    let grpc = ephemeral();
    let metrics = ephemeral();
    let bs = bootstrap_without_tracing("sigterm-svc");
    let spec = chain_spec(
        "sigterm-svc",
        "eth.chain.v1.ChainService",
        grpc,
        metrics,
        vec![],
    );

    // Production path: real Unix signal handlers.
    let handle = tokio::spawn(async move { serve(bs, spec, chain_routes()).await });

    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    // Race: send SIGTERM, then poll health — should observe NOT_SERVING while
    // still able to connect (listener not yet closed), then serve completes Ok.
    #[cfg(unix)]
    {
        // SAFETY: kill(getpid, SIGTERM) is the intended self-signal for this test;
        // nextest runs each test in its own process so we do not disturb siblings.
        let pid = std::process::id() as i32;
        let rc = unsafe { libc_kill(pid, 15) }; // SIGTERM = 15
        assert_eq!(rc, 0, "kill(SIGTERM) failed");
    }
    #[cfg(not(unix))]
    {
        // Non-unix: fall back to external cancel is not available on `serve`;
        // skip signal assertion.
        return;
    }

    // Poll aggressively for NOT_SERVING while the listener is still open
    // (serve pauses ~150 ms after marking NOT_SERVING before stopping accept).
    let mut saw_not_serving = false;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < deadline {
        match health_status(grpc, AGGREGATE_HEALTH).await {
            Ok(status) if status == WireStatus::NotServing as i32 => {
                saw_not_serving = true;
                break;
            }
            Ok(_) | Err(_) => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
    assert!(
        saw_not_serving,
        "expected aggregate NOT_SERVING after SIGTERM before exit"
    );

    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("serve did not finish within 5 s")
        .expect("serve task join");
    assert!(result.is_ok(), "serve must return Ok(()): {result:?}");
}

/// Minimal libc kill binding so we do not pull the `libc` crate.
#[cfg(unix)]
unsafe fn libc_kill(pid: i32, sig: i32) -> i32 {
    // SAFETY: caller passes a valid pid/signal; kill is async-signal-safe.
    unsafe { extern_kill(pid, sig) }
}

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn extern_kill(pid: i32, sig: i32) -> i32;
}
