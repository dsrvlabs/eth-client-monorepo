//! CC-27b acceptance: stream client, outstanding, timeout, re-send, reconnect.
//!
//! In-process tonic stubs stand in for `chain` so reconnect / timeout / re-send
//! paths are real gRPC without pulling the full chain core into p2p tests.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cc_p2p::chain_stream::{
    ChainStreamConfig, ChainStreamHandle, OUTSTANDING_CAP, OutstandingMap, PublishDropCounter,
    run_chain_stream_client, run_publish_dispatch, stall_max_from_heartbeat,
    wait_reconnect_backoff,
};
use cc_p2p::channels::{CHAIN_OUT_BOUND, ChainOutbound, VerdictResolution};
use cc_p2p::metrics::P2pMetrics;
use cc_proto::chain::chain_service_server::{ChainService, ChainServiceServer};
use cc_proto::common::Source;
use cc_proto::p2p::{
    Acceptance, ChainToP2p, ChainView, GossipObject, ImportResult, ObjectKind, PublishRequest,
    Reason, Verdict, chain_to_p2p, p2p_to_chain,
};
use futures::Stream;
use futures::StreamExt;
use prometheus_client::registry::Registry;
use tokio::sync::{mpsc, oneshot, watch};
use tonic::transport::Server;

type BoxStreamChainToP2p =
    Pin<Box<dyn Stream<Item = Result<ChainToP2p, tonic::Status>> + Send + 'static>>;

fn metrics() -> P2pMetrics {
    let mut reg = Registry::default();
    P2pMetrics::register(&mut reg)
}

fn gossip_obj(seed: u64) -> GossipObject {
    let mut root = vec![0u8; 32];
    root[..8].copy_from_slice(&seed.to_le_bytes());
    GossipObject {
        ssz: root.clone(),
        fork: 0,
        root,
        source: Source::Gossip as i32,
        kind: ObjectKind::Block as i32,
        subnet_id: 0,
    }
}

fn full_view(slot: u64) -> ChainView {
    ChainView {
        slot,
        head_slot: slot,
        genesis_time: 1_600_000_000,
        view_kind: 4,
        ..Default::default()
    }
}

// ── grep ACs ────────────────────────────────────────────────────────────────

#[test]
fn no_get_head_poller_in_p2p_src() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    for entry in walkdir_rs(&root) {
        let text = std::fs::read_to_string(&entry).unwrap_or_default();
        for (i, line) in text.lines().enumerate() {
            if line.contains("GetHead") {
                hits.push(format!("{}:{}: {line}", entry.display(), i + 1));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "GetHead must not appear under services/p2p/src (use ChainView):\n{}",
        hits.join("\n")
    );
}

#[test]
fn no_inlined_stall_bound_in_chain_stream() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/chain_stream");
    let mut hits = Vec::new();
    for entry in walkdir_rs(&root) {
        let text = std::fs::read_to_string(&entry).unwrap_or_default();
        for (i, line) in text.lines().enumerate() {
            if line.contains("700") || line.contains("0.7") {
                hits.push(format!("{}:{}: {line}", entry.display(), i + 1));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "inlined stall bound in chain_stream:\n{}",
        hits.join("\n")
    );
    assert_eq!(
        stall_max_from_heartbeat(Duration::from_secs(1)),
        Duration::from_millis(500)
    );
}

fn walkdir_rs(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walkdir_rs(&p));
            } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
                out.push(p);
            }
        }
    }
    out
}

// ── silent stub (never answers objects) ─────────────────────────────────────

#[derive(Debug, Default)]
struct SilentChain;

#[tonic::async_trait]
impl ChainService for SilentChain {
    async fn p2p_stream(
        &self,
        request: tonic::Request<tonic::Streaming<cc_proto::p2p::P2pToChain>>,
    ) -> Result<tonic::Response<BoxStreamChainToP2p>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(Ok(msg)) = inbound.next().await {
                if let Some(p2p_to_chain::Msg::Hello(_)) = msg.msg {
                    let _ = tx
                        .send(Ok(ChainToP2p {
                            seq: 1,
                            msg: Some(chain_to_p2p::Msg::View(full_view(1))),
                        }))
                        .await;
                }
                // Objects intentionally unanswered → client timeout path.
            }
        });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

async fn spawn_silent_grpc() -> (SocketAddr, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = Server::builder()
            .add_service(ChainServiceServer::new(SilentChain))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    (addr, shutdown_tx)
}

async fn wait_view(handle: &ChainStreamHandle) {
    for _ in 0..100 {
        if handle.view.has_view() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("expected ChainView after StreamHello");
}

#[tokio::test]
async fn verdict_timeout_resolves_local_ignore() {
    let (addr, stop) = spawn_silent_grpc().await;
    let m = metrics();
    let handle = ChainStreamHandle::new();
    let (out_tx, out_rx) = mpsc::channel(16);
    let (in_tx, _in_rx) = mpsc::channel(16);
    let (pub_tx, _pub_rx) = mpsc::channel(16);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cfg = ChainStreamConfig {
        chain_uri: format!("http://{addr}"),
        verdict_timeout: Duration::from_millis(200),
        ..ChainStreamConfig::default()
    };
    let client = tokio::spawn(run_chain_stream_client(
        cfg,
        out_rx,
        in_tx,
        pub_tx,
        handle.clone(),
        m.clone(),
        shutdown_rx,
    ));

    wait_view(&handle).await;

    let (reply_tx, reply_rx) = oneshot::channel();
    out_tx
        .send(ChainOutbound {
            object: gossip_obj(42),
            reply: Some(reply_tx),
        })
        .await
        .unwrap();

    let resolution = tokio::time::timeout(Duration::from_secs(3), reply_rx)
        .await
        .expect("timeout waiting for local resolution")
        .unwrap();
    assert!(
        matches!(resolution, VerdictResolution::Backpressure { .. }),
        "enqueue-clock overflow is Policy A, not Timeout IGNORE"
    );
    assert!(m.verdict_timeout() >= 1);
    assert_eq!(m.chain_objects_sent(), 1);
    assert_eq!(
        m.chain_objects_sent(),
        m.chain_verdicts_received() + m.verdict_timeout()
    );

    let _ = shutdown_tx.send(true);
    let _ = stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

// ── counting stub: kill after first object, answer on re-send ───────────────

#[derive(Debug, Default)]
struct CountingChain {
    objects: Arc<AtomicU64>,
    hellos: Arc<AtomicU64>,
    roots: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    /// Shared across sessions: kill only the first object ever (mid-stream tear).
    objects_seen_total: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl ChainService for CountingChain {
    async fn p2p_stream(
        &self,
        request: tonic::Request<tonic::Streaming<cc_proto::p2p::P2pToChain>>,
    ) -> Result<tonic::Response<BoxStreamChainToP2p>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        let objects = Arc::clone(&self.objects);
        let hellos = Arc::clone(&self.hellos);
        let roots = Arc::clone(&self.roots);
        let objects_seen_total = Arc::clone(&self.objects_seen_total);
        tokio::spawn(async move {
            let mut out_seq = 0u64;
            while let Some(Ok(msg)) = inbound.next().await {
                match msg.msg {
                    Some(p2p_to_chain::Msg::Hello(_)) => {
                        hellos.fetch_add(1, Ordering::SeqCst);
                        out_seq += 1;
                        let _ = tx
                            .send(Ok(ChainToP2p {
                                seq: out_seq,
                                msg: Some(chain_to_p2p::Msg::View(full_view(2))),
                            }))
                            .await;
                    }
                    Some(p2p_to_chain::Msg::Object(obj)) => {
                        objects.fetch_add(1, Ordering::SeqCst);
                        roots.lock().unwrap().push(obj.root.clone());
                        let n = objects_seen_total.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            // Tear the stream with outstanding still populated.
                            break;
                        }
                        out_seq += 1;
                        let _ = tx
                            .send(Ok(ChainToP2p {
                                seq: out_seq,
                                msg: Some(chain_to_p2p::Msg::Verdict(Verdict {
                                    correlation_id: obj.root,
                                    acceptance: Acceptance::Ignore as i32,
                                    reason: Reason::Duplicate as i32,
                                    import: ImportResult::Duplicate as i32,
                                })),
                            }))
                            .await;
                    }
                    _ => {}
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

#[tokio::test]
async fn mid_stream_disconnect_resends_outstanding() {
    let objects = Arc::new(AtomicU64::new(0));
    let hellos = Arc::new(AtomicU64::new(0));
    let roots = Arc::new(std::sync::Mutex::new(Vec::new()));
    let objects_seen_total = Arc::new(AtomicU64::new(0));
    let svc = CountingChain {
        objects: Arc::clone(&objects),
        hellos: Arc::clone(&hellos),
        roots: Arc::clone(&roots),
        objects_seen_total,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = Server::builder()
            .add_service(ChainServiceServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop_rx.await;
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let m = metrics();
    let handle = ChainStreamHandle::new();
    let (out_tx, out_rx) = mpsc::channel(16);
    let (in_tx, _in_rx) = mpsc::channel(16);
    let (pub_tx, _pub_rx) = mpsc::channel(16);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cfg = ChainStreamConfig {
        chain_uri: format!("http://{addr}"),
        backoff_initial: Duration::from_millis(50),
        backoff_cap: Duration::from_millis(200),
        verdict_timeout: Duration::from_secs(30),
        ..ChainStreamConfig::default()
    };
    let client = tokio::spawn(run_chain_stream_client(
        cfg,
        out_rx,
        in_tx,
        pub_tx,
        handle.clone(),
        m.clone(),
        shutdown_rx,
    ));

    wait_view(&handle).await;

    let (reply_tx, reply_rx) = oneshot::channel();
    let root = gossip_obj(7).root.clone();
    out_tx
        .send(ChainOutbound {
            object: gossip_obj(7),
            reply: Some(reply_tx),
        })
        .await
        .unwrap();

    for _ in 0..100 {
        if objects.load(Ordering::SeqCst) >= 2 && hellos.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        hellos.load(Ordering::SeqCst) >= 2,
        "expected reconnect StreamHello, hellos={}",
        hellos.load(Ordering::SeqCst)
    );
    assert!(
        objects.load(Ordering::SeqCst) >= 2,
        "expected re-send of outstanding, objects={}",
        objects.load(Ordering::SeqCst)
    );
    let seen = roots.lock().unwrap().clone();
    let count_root = seen.iter().filter(|r| **r == root).count();
    assert!(
        count_root >= 2,
        "same root re-sent after reconnect; count={count_root}"
    );

    let resolution = tokio::time::timeout(Duration::from_secs(3), reply_rx)
        .await
        .expect("resolution after re-send")
        .unwrap();
    assert!(matches!(resolution, VerdictResolution::FromChain(_)));

    assert_eq!(
        m.chain_objects_sent(),
        m.chain_verdicts_received() + m.verdict_timeout()
    );

    let _ = shutdown_tx.send(true);
    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

// ── chain killed: client keeps reconnecting (CC-27/6) ───────────────────────

#[tokio::test]
async fn chain_killed_client_reconnects_without_operator() {
    let (addr, stop) = spawn_silent_grpc().await;
    let m = metrics();
    let handle = ChainStreamHandle::new();
    let (out_tx, out_rx) = mpsc::channel(8);
    let (in_tx, _in_rx) = mpsc::channel(8);
    let (pub_tx, _pub_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cfg = ChainStreamConfig {
        chain_uri: format!("http://{addr}"),
        backoff_initial: Duration::from_millis(50),
        backoff_cap: Duration::from_secs(1),
        ..ChainStreamConfig::default()
    };
    let client = tokio::spawn(run_chain_stream_client(
        cfg,
        out_rx,
        in_tx,
        pub_tx,
        handle.clone(),
        m,
        shutdown_rx,
    ));

    wait_view(&handle).await;

    // Kill chain mid-stream.
    let _ = stop.send(());

    let send = out_tx
        .send(ChainOutbound {
            object: gossip_obj(99),
            reply: None,
        })
        .await;
    assert!(send.is_ok(), "client accepts outbound while disconnected");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !client.is_finished(),
        "chain-stream client must keep reconnecting (never fatal)"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

// ── outstanding cap + saturation ────────────────────────────────────────────

#[tokio::test]
async fn outstanding_depth_metric_and_cap() {
    assert_eq!(OUTSTANDING_CAP, CHAIN_OUT_BOUND);
    assert_eq!(OUTSTANDING_CAP, 1024);
    let m = metrics();
    m.set_queue_depth(cc_p2p::metrics::QueueName::Outstanding, 1024);
    assert_eq!(m.queue_depth(cc_p2p::metrics::QueueName::Outstanding), 1024);
    m.set_saturation_ratio(1.0);
    assert_eq!(m.saturation_ratio_milli(), 1000);
}

#[tokio::test]
async fn load_saturation_without_unbounded_growth() {
    let (addr, stop) = spawn_silent_grpc().await;
    let m = metrics();
    let handle = ChainStreamHandle::new();
    let (out_tx, out_rx) = mpsc::channel(CHAIN_OUT_BOUND);
    let (in_tx, _in_rx) = mpsc::channel(CHAIN_OUT_BOUND);
    let (pub_tx, _pub_rx) = mpsc::channel(16);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cfg = ChainStreamConfig {
        chain_uri: format!("http://{addr}"),
        verdict_timeout: Duration::from_secs(60),
        ..ChainStreamConfig::default()
    };
    let client = tokio::spawn(run_chain_stream_client(
        cfg,
        out_rx,
        in_tx,
        pub_tx,
        handle.clone(),
        m.clone(),
        shutdown_rx,
    ));

    wait_view(&handle).await;

    let mut sent = 0u64;
    for i in 0..(OUTSTANDING_CAP as u64 + 64) {
        match out_tx.try_send(ChainOutbound {
            object: gossip_obj(i + 1_000),
            reply: None,
        }) {
            Ok(()) => sent += 1,
            Err(mpsc::error::TrySendError::Full(_)) => break,
            Err(e) => panic!("send failed: {e}"),
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let depth = m.queue_depth(cc_p2p::metrics::QueueName::Outstanding);
    assert!(
        depth <= OUTSTANDING_CAP as i64,
        "outstanding must be capped at {OUTSTANDING_CAP}, got {depth}"
    );
    assert!(sent > 0);
    assert!(
        m.saturation_ratio_milli() > 0 || depth > 0,
        "expected saturation under load"
    );
    // In-flight outstanding means sent > resolved; that is not a violation of
    // CC-27/4 (equality is over a closed interval / after timeout). Assert the
    // inequality direction and the hard cap instead.
    assert!(m.chain_objects_sent() >= m.chain_verdicts_received() + m.verdict_timeout());
    assert!(m.chain_objects_sent() as i64 >= depth);

    let _ = shutdown_tx.send(true);
    let _ = stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

// ── outward publish dispatch ────────────────────────────────────────────────

#[tokio::test]
async fn publish_request_reaches_publish_queue() {
    let m = metrics();
    let (proto_tx, proto_rx) = mpsc::channel(8);
    let (pub_tx, mut pub_rx) = mpsc::channel(8);
    let drops = PublishDropCounter::new();
    let dispatch = tokio::spawn(run_publish_dispatch(proto_rx, pub_tx, m, drops));

    proto_tx
        .send(PublishRequest {
            ssz: vec![0xab; 32],
            kind: ObjectKind::Block as i32,
            topic: "beacon_block".into(),
            subnet_id: 0,
        })
        .await
        .unwrap();

    let got = tokio::time::timeout(Duration::from_secs(1), pub_rx.recv())
        .await
        .expect("publish")
        .expect("closed");
    assert_eq!(got.topic, "beacon_block");
    assert_eq!(got.data, vec![0xab; 32]);

    drop(proto_tx);
    let _ = tokio::time::timeout(Duration::from_secs(1), dispatch).await;
}

// ── ChainView single writer ─────────────────────────────────────────────────

#[tokio::test]
async fn chain_view_published_to_arcswap() {
    let (addr, stop) = spawn_silent_grpc().await;
    let m = metrics();
    let handle = ChainStreamHandle::new();
    let (_out_tx, out_rx) = mpsc::channel(4);
    let (in_tx, _in_rx) = mpsc::channel(4);
    let (pub_tx, _pub_rx) = mpsc::channel(4);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cfg = ChainStreamConfig {
        chain_uri: format!("http://{addr}"),
        ..ChainStreamConfig::default()
    };
    let client = tokio::spawn(run_chain_stream_client(
        cfg,
        out_rx,
        in_tx,
        pub_tx,
        handle.clone(),
        m,
        shutdown_rx,
    ));

    wait_view(&handle).await;
    let v = handle.view.load();
    assert_eq!(v.view_kind, 4);
    assert_eq!(v.genesis_time, 1_600_000_000);

    let _ = shutdown_tx.send(true);
    let _ = stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

// ── F1: late after timeout must not break CC-27/4 equality ──────────────────

/// Answers Hello, ignores the first object long enough to timeout, then emits
/// a late Verdict for that root (post-timeout).
#[derive(Debug, Default)]
struct LateAfterTimeoutChain {
    answered: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl ChainService for LateAfterTimeoutChain {
    async fn p2p_stream(
        &self,
        request: tonic::Request<tonic::Streaming<cc_proto::p2p::P2pToChain>>,
    ) -> Result<tonic::Response<BoxStreamChainToP2p>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let answered = Arc::clone(&self.answered);
        tokio::spawn(async move {
            let mut out_seq = 0u64;
            while let Some(Ok(msg)) = inbound.next().await {
                match msg.msg {
                    Some(p2p_to_chain::Msg::Hello(_)) => {
                        out_seq += 1;
                        let _ = tx
                            .send(Ok(ChainToP2p {
                                seq: out_seq,
                                msg: Some(chain_to_p2p::Msg::View(full_view(1))),
                            }))
                            .await;
                    }
                    Some(p2p_to_chain::Msg::Object(obj)) => {
                        // Answer after the client has timed out (local IGNORE first).
                        let root = obj.root;
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        out_seq += 1;
                        answered.fetch_add(1, Ordering::SeqCst);
                        let _ = tx
                            .send(Ok(ChainToP2p {
                                seq: out_seq,
                                msg: Some(chain_to_p2p::Msg::Verdict(Verdict {
                                    correlation_id: root,
                                    acceptance: Acceptance::Accept as i32,
                                    reason: Reason::Valid as i32,
                                    import: ImportResult::Imported as i32,
                                })),
                            }))
                            .await;
                    }
                    _ => {}
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

#[tokio::test]
async fn late_after_timeout_preserves_counter_equality() {
    let answered = Arc::new(AtomicU64::new(0));
    let svc = LateAfterTimeoutChain {
        answered: Arc::clone(&answered),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = Server::builder()
            .add_service(ChainServiceServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop_rx.await;
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let m = metrics();
    let handle = ChainStreamHandle::new();
    let (out_tx, out_rx) = mpsc::channel(8);
    let (in_tx, _in_rx) = mpsc::channel(8);
    let (pub_tx, _pub_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cfg = ChainStreamConfig {
        chain_uri: format!("http://{addr}"),
        verdict_timeout: Duration::from_millis(100),
        ..ChainStreamConfig::default()
    };
    let client = tokio::spawn(run_chain_stream_client(
        cfg,
        out_rx,
        in_tx,
        pub_tx,
        handle.clone(),
        m.clone(),
        shutdown_rx,
    ));

    wait_view(&handle).await;

    let (reply_tx, reply_rx) = oneshot::channel();
    out_tx
        .send(ChainOutbound {
            object: gossip_obj(77),
            reply: Some(reply_tx),
        })
        .await
        .unwrap();

    // Local timeout first.
    let resolution = tokio::time::timeout(Duration::from_secs(2), reply_rx)
        .await
        .expect("local timeout")
        .unwrap();
    assert!(
        matches!(resolution, VerdictResolution::Backpressure { .. }),
        "verdict wait past enqueue clock is Backpressure"
    );
    assert!(m.verdict_timeout() >= 1);

    // Wait for the late chain answer to arrive.
    for _ in 0..50 {
        if answered.load(Ordering::SeqCst) >= 1 && m.verdict_late() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        m.verdict_late() >= 1,
        "late-after-timeout must increment late counter"
    );
    // Equality: late answer must NOT also count as verdicts_received.
    assert_eq!(
        m.chain_objects_sent(),
        m.chain_verdicts_received() + m.verdict_timeout(),
        "sent={} verdicts={} timeouts={} late={}",
        m.chain_objects_sent(),
        m.chain_verdicts_received(),
        m.verdict_timeout(),
        m.verdict_late()
    );

    let _ = shutdown_tx.send(true);
    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

// ── H1: backoff not cancelled by continuous chain_out ───────────────────────

#[tokio::test]
async fn reconnect_backoff_completes_under_outbound_load() {
    let m = metrics();
    let (out_tx, mut out_rx) = mpsc::channel(64);
    let mut pending = Vec::new();
    let mut outstanding = OutstandingMap::new();
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Producer floods with try_send so it never blocks the test on a full queue.
    let flood = tokio::spawn(async move {
        for i in 0..200u64 {
            let _ = out_tx.try_send(ChainOutbound {
                object: gossip_obj(i + 50_000),
                reply: None,
            });
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    let sleep = Duration::from_millis(150);
    let start = std::time::Instant::now();
    let shut = wait_reconnect_backoff(
        sleep,
        &mut out_rx,
        &mut pending,
        &mut outstanding,
        &m,
        Duration::from_secs(2),
        &mut shutdown_rx,
    )
    .await;
    let elapsed = start.elapsed();
    assert!(!shut);
    // Must not return early because of recv wins — allow a small scheduler slack.
    assert!(
        elapsed >= sleep - Duration::from_millis(20),
        "backoff aborted early under load: elapsed={elapsed:?} sleep={sleep:?}"
    );
    assert!(
        !pending.is_empty(),
        "outbound traffic should have been drained into pending"
    );

    // Drop the receiver so the flood task can finish if it still holds the sender.
    drop(out_rx);
    let _ = tokio::time::timeout(Duration::from_secs(2), flood).await;
}

// ── M2: duplicate root does not double-count sent ───────────────────────────

#[tokio::test]
async fn duplicate_root_does_not_double_count_or_orphan() {
    let (addr, stop) = spawn_silent_grpc().await;
    let m = metrics();
    let handle = ChainStreamHandle::new();
    let (out_tx, out_rx) = mpsc::channel(16);
    let (in_tx, _in_rx) = mpsc::channel(16);
    let (pub_tx, _pub_rx) = mpsc::channel(16);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cfg = ChainStreamConfig {
        chain_uri: format!("http://{addr}"),
        verdict_timeout: Duration::from_millis(300),
        ..ChainStreamConfig::default()
    };
    let client = tokio::spawn(run_chain_stream_client(
        cfg,
        out_rx,
        in_tx,
        pub_tx,
        handle.clone(),
        m.clone(),
        shutdown_rx,
    ));

    wait_view(&handle).await;

    let (r1, rx1) = oneshot::channel();
    let (r2, rx2) = oneshot::channel();
    let obj = gossip_obj(1234);
    out_tx
        .send(ChainOutbound {
            object: obj.clone(),
            reply: Some(r1),
        })
        .await
        .unwrap();
    // Same root while first is outstanding.
    out_tx
        .send(ChainOutbound {
            object: obj,
            reply: Some(r2),
        })
        .await
        .unwrap();

    // Duplicate is resolved immediately (local release) without counting sent.
    let dup = tokio::time::timeout(Duration::from_secs(1), rx2)
        .await
        .expect("duplicate reply")
        .unwrap();
    assert!(matches!(dup, VerdictResolution::Timeout));

    // First still expires later (enqueue clock → Backpressure).
    let first = tokio::time::timeout(Duration::from_secs(2), rx1)
        .await
        .expect("first timeout")
        .unwrap();
    assert!(matches!(first, VerdictResolution::Backpressure { .. }));

    // Only one send counted; one timeout for the real outstanding entry.
    assert_eq!(m.chain_objects_sent(), 1);
    assert_eq!(
        m.chain_objects_sent(),
        m.chain_verdicts_received() + m.verdict_timeout()
    );

    let _ = shutdown_tx.send(true);
    let _ = stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}
