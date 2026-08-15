//! gRPC `EngineService` client implementing [`ExecutionEngine`] (CC-32b).
//!
//! # Sync→async bridge (Architecture §2.4)
//!
//! Three facts from the live tree settle the bridge:
//! 1. The fork-choice core runs on a **plain OS thread** (`chain-core`), not a
//!    tokio worker — `blocking_recv` panics inside an async context.
//! 2. `services/chain/src/main.rs` is `#[tokio::main]` with **no `flavor`
//!    argument**, so the runtime is **multi-threaded**.
//! 3. `tokio::task::block-in-place` requires the caller to *be* a runtime
//!    worker and **would panic** on `chain-core`.
//!
//! Design: hold a captured [`tokio::runtime::Handle`] and call
//! `handle.block_on` from that OS thread. Other runtime workers continue
//! driving IO while this thread parks. **`block-in-place` must not appear.**
//!
//! Every `block_on` carries an explicit [`Duration`] (P0-15 / S0-A-27). A
//! timeout is [`EngineError::Transport`], which `on_block` maps to
//! `Deferred(ExecutionEngineUnavailable)` and the import path parks in
//! [`crate::pending_engine`] (ADR-P3-05). Without the deadline a black-holed
//! engine parks the whole consensus core.
//!
//! ADR P3-16: no HTTP client or JWT signer here — that surface lives only in
//! `services/engine`.

use std::sync::Mutex;
use std::time::Duration;

use cc_proto::engine::FetchBlobsRequest;
use cc_proto::engine::NewPayloadRequest as ProtoNewPayloadRequest;
use cc_proto::engine::engine_service_client::EngineServiceClient;
use cc_state_transition::error::EngineError;
use cc_state_transition::{
    ExecutionEngine, NewPayloadRequest, PayloadStatus, get_execution_requests_list,
};
use cc_types::preset::Preset;
use cc_types::primitives::Hash256;
use ssz::Encode;
use tokio::runtime::Handle;
use tonic::transport::Channel;

/// Default `engine` gRPC endpoint (local compose / `config/chain.toml`).
pub const DEFAULT_ENGINE_URI: &str = "http://127.0.0.1:9004";

/// Default gRPC connect deadline on the chain→engine hop (not the EL).
pub const DEFAULT_ENGINE_CONNECT_TIMEOUT: Duration = Duration::from_millis(1_000);

/// Default `NewPayload` deadline. Matches engine `TransportTimeouts::new_payload` (8 s).
pub const DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT: Duration = Duration::from_millis(8_000);

/// Default `GetEngineState` / poll deadline.
pub const DEFAULT_ENGINE_GET_STATE_TIMEOUT: Duration = Duration::from_millis(1_000);

/// Default `FetchBlobs` deadline (accelerator; matches engine getBlobs 1 s).
pub const DEFAULT_ENGINE_FETCH_BLOBS_TIMEOUT: Duration = Duration::from_millis(1_000);

/// Default `ForkchoiceUpdated` deadline. Matches engine `TransportTimeouts`.
pub const DEFAULT_ENGINE_FORKCHOICE_UPDATED_TIMEOUT: Duration = Duration::from_millis(8_000);

/// Hard deadlines on every chain→engine `block_on` (P0-15 / S0-A-27).
///
/// Defaults match the engine-side EL transport knobs so a live engine can still
/// return its own timeout. A black-holed *engine* unparks the core via these
/// caps and surfaces [`EngineError::Transport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineRpcDeadlines {
    /// `EngineServiceClient::connect`.
    pub connect: Duration,
    /// Unary `NewPayload`.
    pub new_payload: Duration,
    /// Unary `GetEngineState` (and the one-shot poll helper).
    pub get_engine_state: Duration,
    /// Unary `FetchBlobs` (fire-and-forget accelerator).
    pub fetch_blobs: Duration,
    /// Unary `ForkchoiceUpdated` (`GrpcFcuSink`).
    pub forkchoice_updated: Duration,
}

impl Default for EngineRpcDeadlines {
    fn default() -> Self {
        Self {
            connect: DEFAULT_ENGINE_CONNECT_TIMEOUT,
            new_payload: DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT,
            get_engine_state: DEFAULT_ENGINE_GET_STATE_TIMEOUT,
            fetch_blobs: DEFAULT_ENGINE_FETCH_BLOBS_TIMEOUT,
            forkchoice_updated: DEFAULT_ENGINE_FORKCHOICE_UPDATED_TIMEOUT,
        }
    }
}

impl EngineRpcDeadlines {
    /// Sub-second deadlines for injected-timeout tests. Must not wait the 8 s EL bar.
    #[must_use]
    pub const fn for_test() -> Self {
        Self {
            connect: Duration::from_millis(80),
            new_payload: Duration::from_millis(80),
            get_engine_state: Duration::from_millis(80),
            fetch_blobs: Duration::from_millis(80),
            forkchoice_updated: Duration::from_millis(80),
        }
    }
}

fn timeout_err(timeout: Duration) -> EngineError {
    EngineError::Transport(format!(
        "engine RPC timed out after {}ms",
        timeout.as_millis()
    ))
}

/// Drive `fut` on `handle` and fail closed as [`EngineError::Transport`] at `timeout`.
///
/// Every chain→engine `block_on` goes through here so a missing `Duration` is a
/// compile error. `GrpcFcuSink` uses the same helper (P0-15 Low 1).
pub(crate) fn block_on_deadline<F, T>(
    handle: &Handle,
    timeout: Duration,
    fut: F,
) -> Result<T, EngineError>
where
    F: std::future::Future<Output = Result<T, EngineError>>,
{
    handle.block_on(async move {
        match tokio::time::timeout(timeout, fut).await {
            Ok(inner) => inner,
            Err(_) => Err(timeout_err(timeout)),
        }
    })
}

/// gRPC client implementing the CC-14 [`ExecutionEngine`] seam.
///
/// Clone is cheap on the channel; the runtime handle is shared.
#[derive(Debug)]
pub struct EngineApiClient {
    /// Captured multi-threaded runtime handle (§2.4). Used exclusively via
    /// `handle.block_on` from the plain OS thread `chain-core`. Never
    /// `block-in-place`.
    handle: Handle,
    uri: String,
    /// Lazily connected client. Mutex so `verify_and_notify_new_payload` can
    /// take `&self` while reconnecting once.
    ///
    /// Held across connect `block_on` only; safe under the intended single
    /// consumer (`chain-core`). Must not be locked by a future async caller on
    /// the same handle while another thread parks on connect.
    inner: Mutex<Option<EngineServiceClient<Channel>>>,
    deadlines: EngineRpcDeadlines,
}

impl EngineApiClient {
    /// Build a client for `uri` using the current tokio runtime handle.
    ///
    /// Must be constructed from a tokio context (bootstrap / main) so
    /// [`Handle::try_current`] succeeds; the handle is then used from
    /// `chain-core` via `block_on`.
    pub fn new(uri: impl Into<String>) -> Result<Self, EngineError> {
        let handle = Handle::try_current().map_err(|e| {
            EngineError::Transport(format!("no tokio runtime handle for engine client: {e}"))
        })?;
        Ok(Self {
            handle,
            uri: uri.into(),
            inner: Mutex::new(None),
            deadlines: EngineRpcDeadlines::default(),
        })
    }

    /// Like [`Self::new`] with [`DEFAULT_ENGINE_URI`].
    pub fn with_default_uri() -> Result<Self, EngineError> {
        Self::new(DEFAULT_ENGINE_URI)
    }

    /// Construct with an explicit handle (tests that capture a runtime).
    #[must_use]
    pub fn from_handle(handle: Handle, uri: impl Into<String>) -> Self {
        Self::from_handle_with_deadlines(handle, uri, EngineRpcDeadlines::default())
    }

    /// Construct with an explicit handle and RPC deadlines (injected-timeout tests).
    #[must_use]
    pub fn from_handle_with_deadlines(
        handle: Handle,
        uri: impl Into<String>,
        deadlines: EngineRpcDeadlines,
    ) -> Self {
        Self {
            handle,
            uri: uri.into(),
            inner: Mutex::new(None),
            deadlines,
        }
    }

    /// Deadlines applied to every `block_on` on this client.
    #[must_use]
    pub fn deadlines(&self) -> EngineRpcDeadlines {
        self.deadlines
    }

    fn connect_blocking(&self) -> Result<EngineServiceClient<Channel>, EngineError> {
        let uri = self.uri.clone();
        let timeout = self.deadlines.connect;
        block_on_deadline(&self.handle, timeout, async {
            EngineServiceClient::connect(uri)
                .await
                .map_err(|e| EngineError::Transport(format!("engine connect: {e}")))
        })
    }

    fn client(&self) -> Result<EngineServiceClient<Channel>, EngineError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| EngineError::Transport("engine client mutex poisoned".into()))?;
        if guard.is_none() {
            *guard = Some(self.connect_blocking()?);
        }
        match guard.as_ref() {
            Some(c) => Ok(c.clone()),
            None => Err(EngineError::Transport(
                "engine client missing after connect".into(),
            )),
        }
    }

    /// Issue `NewPayload` over gRPC (async body for `block_on`).
    async fn new_payload_async(
        &self,
        mut client: EngineServiceClient<Channel>,
        req: ProtoNewPayloadRequest,
    ) -> Result<PayloadStatus, EngineError> {
        let resp = client
            .new_payload(req)
            .await
            .map_err(|e| EngineError::Transport(format!("engine NewPayload: {e}")))?
            .into_inner();
        let status = resp.payload_status.ok_or_else(|| {
            EngineError::Transport("engine NewPayload missing payload_status".into())
        })?;
        map_proto_status(&status)
    }

    /// Poll `GetEngineState` (CC-36a: Offline→Online redrive of `pending_engine`).
    ///
    /// Returns `true` when the engine reports online (`el_offline == false`).
    /// Transport failures are treated as offline (fail-closed).
    #[must_use]
    pub fn is_engine_online(&self) -> bool {
        let client = match self.client() {
            Ok(c) => c,
            Err(_) => return false,
        };
        let timeout = self.deadlines.get_engine_state;
        block_on_deadline(&self.handle, timeout, async move {
            let mut client = client;
            match client
                .get_engine_state(cc_proto::engine::GetEngineStateRequest {})
                .await
            {
                Ok(resp) => Ok(!resp.into_inner().el_offline),
                Err(_) => Ok(false),
            }
        })
        .unwrap_or(false)
    }

    /// Fire-and-forget CC-38a block-branch `FetchBlobs` (template-sized only).
    ///
    /// Enqueues on engine's fastpath and returns immediately on the server.
    /// Transport errors are logged and swallowed — DA recovery still runs via
    /// gossip / pending_da; the fast path is an accelerator.
    pub fn fetch_blobs(&self, req: FetchBlobsRequest) {
        let client = match self.client() {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "FetchBlobs: no engine client");
                return;
            }
        };
        let timeout = self.deadlines.fetch_blobs;
        let _ = block_on_deadline(&self.handle, timeout, async move {
            let mut client = client;
            if let Err(e) = client.fetch_blobs(req).await {
                tracing::debug!(error = %e, "FetchBlobs transport failed (accelerator)");
            }
            Ok(())
        });
    }
}

/// Poll engine online via a one-shot client (core SlotTick; no shared mutex).
#[must_use]
pub fn poll_engine_online(handle: &Handle, uri: &str) -> bool {
    poll_engine_online_with(handle, uri, DEFAULT_ENGINE_GET_STATE_TIMEOUT)
}

/// [`poll_engine_online`] with an explicit deadline (tests / SlotTick).
#[must_use]
pub fn poll_engine_online_with(handle: &Handle, uri: &str, timeout: Duration) -> bool {
    block_on_deadline(handle, timeout, async {
        match EngineServiceClient::connect(uri.to_owned()).await {
            Ok(mut client) => match client
                .get_engine_state(cc_proto::engine::GetEngineStateRequest {})
                .await
            {
                Ok(resp) => Ok(!resp.into_inner().el_offline),
                Err(_) => Ok(false),
            },
            Err(_) => Ok(false),
        }
    })
    .unwrap_or(false)
}

/// Fire unary `FetchBlobs` (CC-38a block branch) via a one-shot connect.
///
/// Template-sized only — never cells. Best-effort; errors are debug-logged.
pub fn fire_fetch_blobs(handle: &Handle, uri: &str, req: FetchBlobsRequest) {
    fire_fetch_blobs_with(handle, uri, req, DEFAULT_ENGINE_FETCH_BLOBS_TIMEOUT);
}

/// [`fire_fetch_blobs`] with an explicit deadline (tests).
pub fn fire_fetch_blobs_with(
    handle: &Handle,
    uri: &str,
    req: FetchBlobsRequest,
    timeout: Duration,
) {
    let _ = block_on_deadline(handle, timeout, async {
        match EngineServiceClient::connect(uri.to_owned()).await {
            Ok(mut client) => {
                if let Err(e) = client.fetch_blobs(req).await {
                    tracing::debug!(error = %e, "FetchBlobs transport failed (accelerator)");
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "FetchBlobs: engine dial failed (accelerator)");
            }
        }
        Ok(())
    });
}

impl<P: Preset> ExecutionEngine<P> for EngineApiClient {
    fn verify_and_notify_new_payload(
        &self,
        request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, EngineError> {
        let ssz = request.execution_payload.as_ssz_bytes();
        let versioned_hashes: Vec<Vec<u8>> = request
            .versioned_hashes
            .iter()
            .map(|h| h.as_slice().to_vec())
            .collect();
        let parent_beacon_block_root = request.parent_beacon_block_root.as_slice().to_vec();
        let execution_requests = get_execution_requests_list(request.execution_requests);

        let proto = ProtoNewPayloadRequest {
            ssz,
            versioned_hashes,
            parent_beacon_block_root,
            execution_requests,
        };

        // §2.4 bridge: park this OS thread on the multi-threaded runtime
        // for at most `deadlines.new_payload`. Do NOT use block-in-place.
        let client = self.client()?;
        let timeout = self.deadlines.new_payload;
        block_on_deadline(&self.handle, timeout, self.new_payload_async(client, proto))
    }
}

fn map_proto_status(
    status: &cc_proto::engine::PayloadStatusV1,
) -> Result<PayloadStatus, EngineError> {
    let latest = match &status.latest_valid_hash {
        None => None,
        Some(bytes) if bytes.len() == 32 => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(bytes);
            Some(Hash256::from(arr))
        }
        Some(bytes) => {
            return Err(EngineError::Transport(format!(
                "latest_valid_hash length {} (want 32 or absent)",
                bytes.len()
            )));
        }
    };
    match status.status.as_str() {
        "VALID" => Ok(PayloadStatus::Valid),
        "INVALID" => Ok(PayloadStatus::Invalid {
            latest_valid_hash: latest,
        }),
        "SYNCING" => Ok(PayloadStatus::Syncing),
        "ACCEPTED" => Ok(PayloadStatus::Accepted),
        "INVALID_BLOCK_HASH" => Ok(PayloadStatus::InvalidBlockHash),
        other => Err(EngineError::Transport(format!(
            "unknown payload status {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_proto::engine::engine_service_server::{EngineService, EngineServiceServer};
    use cc_proto::engine::{
        FetchBlobsRequest, FetchBlobsResponse, ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse,
        GetEngineStateRequest, GetEngineStateResponse, GetInfoRequest, GetInfoResponse,
        NewPayloadResponse, PayloadStatusV1,
    };
    use cc_types::execution::ExecutionPayload;
    use cc_types::operations::ExecutionRequests;
    use cc_types::preset::Mainnet;
    use cc_types::primitives::Root;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    /// Mock engine that always returns VALID and counts calls.
    #[derive(Debug, Default)]
    struct MockEngine {
        calls: AtomicU64,
    }

    #[tonic::async_trait]
    impl EngineService for MockEngine {
        async fn get_info(
            &self,
            _: Request<GetInfoRequest>,
        ) -> Result<Response<GetInfoResponse>, Status> {
            Ok(Response::new(GetInfoResponse { build_info: None }))
        }

        async fn new_payload(
            &self,
            _: Request<ProtoNewPayloadRequest>,
        ) -> Result<Response<NewPayloadResponse>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(NewPayloadResponse {
                payload_status: Some(PayloadStatusV1 {
                    status: "VALID".into(),
                    latest_valid_hash: None,
                    validation_error: None,
                }),
            }))
        }

        async fn forkchoice_updated(
            &self,
            _: Request<ForkchoiceUpdatedRequest>,
        ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
            Ok(Response::new(ForkchoiceUpdatedResponse {
                payload_status: Some(PayloadStatusV1 {
                    status: "VALID".into(),
                    latest_valid_hash: None,
                    validation_error: None,
                }),
                payload_id: None,
            }))
        }

        async fn get_engine_state(
            &self,
            _: Request<GetEngineStateRequest>,
        ) -> Result<Response<GetEngineStateResponse>, Status> {
            Ok(Response::new(GetEngineStateResponse {
                el_offline: false,
                internal_state: "synced".into(),
            }))
        }

        async fn fetch_blobs(
            &self,
            _: Request<FetchBlobsRequest>,
        ) -> Result<Response<FetchBlobsResponse>, Status> {
            Ok(Response::new(FetchBlobsResponse {}))
        }
    }

    /// Engine that accepts TCP and never answers any RPC (P0-15 black hole).
    #[derive(Debug, Default)]
    struct BlackHoleEngine;

    #[tonic::async_trait]
    impl EngineService for BlackHoleEngine {
        async fn get_info(
            &self,
            _: Request<GetInfoRequest>,
        ) -> Result<Response<GetInfoResponse>, Status> {
            std::future::pending().await
        }

        async fn new_payload(
            &self,
            _: Request<ProtoNewPayloadRequest>,
        ) -> Result<Response<NewPayloadResponse>, Status> {
            std::future::pending().await
        }

        async fn forkchoice_updated(
            &self,
            _: Request<ForkchoiceUpdatedRequest>,
        ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
            std::future::pending().await
        }

        async fn get_engine_state(
            &self,
            _: Request<GetEngineStateRequest>,
        ) -> Result<Response<GetEngineStateResponse>, Status> {
            std::future::pending().await
        }

        async fn fetch_blobs(
            &self,
            _: Request<FetchBlobsRequest>,
        ) -> Result<Response<FetchBlobsResponse>, Status> {
            std::future::pending().await
        }
    }

    async fn spawn_mock() -> (SocketAddr, oneshot::Sender<()>, Arc<MockEngine>) {
        let mock = Arc::new(MockEngine::default());
        let svc = EngineServiceServer::from_arc(Arc::clone(&mock));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async move {
                        let _ = rx.await;
                    },
                )
                .await
                .unwrap();
        });
        // Give the server a moment to accept.
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, tx, mock)
    }

    async fn spawn_black_hole() -> (SocketAddr, oneshot::Sender<()>) {
        let svc = EngineServiceServer::new(BlackHoleEngine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async move {
                        let _ = rx.await;
                    },
                )
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, tx)
    }

    /// CC-32 /8: 1 000 sequential newPayload calls from a plain OS thread via
    /// `handle.block_on`, with a concurrent tokio task making progress.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engine_client_thousand_sequential_calls() {
        let (addr, shutdown, mock) = spawn_mock().await;
        let uri = format!("http://{addr}");
        let handle = Handle::current();
        let client = Arc::new(EngineApiClient::from_handle(handle.clone(), uri));

        // Concurrent task must make progress while chain-core blocks on engine.
        let progress = Arc::new(AtomicU64::new(0));
        let progress_task = {
            let progress = Arc::clone(&progress);
            tokio::spawn(async move {
                for _ in 0..10_000 {
                    progress.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                }
            })
        };

        let client_thread = Arc::clone(&client);
        let join = std::thread::Builder::new()
            .name("chain-core".into())
            .spawn(move || {
                let payload = ExecutionPayload::<Mainnet>::default();
                let requests = ExecutionRequests::<Mainnet>::default();
                for _ in 0..1_000 {
                    let req = NewPayloadRequest {
                        execution_payload: &payload,
                        versioned_hashes: vec![],
                        parent_beacon_block_root: Root::ZERO,
                        execution_requests: &requests,
                    };
                    let status = ExecutionEngine::<Mainnet>::verify_and_notify_new_payload(
                        client_thread.as_ref(),
                        req,
                    )
                    .expect("newPayload");
                    assert_eq!(status, PayloadStatus::Valid);
                }
            })
            .expect("spawn chain-core");

        join.join().expect("chain-core join");
        let _ = progress_task.await;
        assert!(
            progress.load(Ordering::SeqCst) > 0,
            "concurrent tokio task must make progress during block_on bridge"
        );
        assert_eq!(mock.calls.load(Ordering::SeqCst), 1_000);
        let _ = shutdown.send(());
    }

    /// P0-15: a black-holed NewPayload unparks as `EngineError::Transport`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn black_holed_new_payload_times_out_as_transport() {
        let (addr, shutdown) = spawn_black_hole().await;
        let uri = format!("http://{addr}");
        let handle = Handle::current();
        let client = Arc::new(EngineApiClient::from_handle_with_deadlines(
            handle,
            uri,
            EngineRpcDeadlines::for_test(),
        ));
        let start = std::time::Instant::now();
        let client_thread = Arc::clone(&client);
        let err = std::thread::Builder::new()
            .name("chain-core".into())
            .spawn(move || {
                let payload = ExecutionPayload::<Mainnet>::default();
                let requests = ExecutionRequests::<Mainnet>::default();
                let req = NewPayloadRequest {
                    execution_payload: &payload,
                    versioned_hashes: vec![],
                    parent_beacon_block_root: Root::ZERO,
                    execution_requests: &requests,
                };
                ExecutionEngine::<Mainnet>::verify_and_notify_new_payload(
                    client_thread.as_ref(),
                    req,
                )
            })
            .expect("spawn chain-core")
            .join()
            .expect("chain-core join")
            .expect_err("black hole must not return a payload status");
        let elapsed = start.elapsed();
        assert!(
            matches!(err, EngineError::Transport(ref m) if m.contains("timed out")),
            "expected Transport timeout, got {err:?}"
        );
        assert!(
            elapsed < Duration::from_millis(800),
            "core parked on engine: {elapsed:?}"
        );
        let _ = shutdown.send(());
    }

    /// Poll / FetchBlobs must also unpark (false / swallow), not park the core.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn black_holed_poll_and_fetch_unpark() {
        let (addr, shutdown) = spawn_black_hole().await;
        let uri = format!("http://{addr}");
        let handle = Handle::current();
        let client = Arc::new(EngineApiClient::from_handle_with_deadlines(
            handle.clone(),
            uri.clone(),
            EngineRpcDeadlines::for_test(),
        ));
        let start = std::time::Instant::now();
        let client_thread = Arc::clone(&client);
        let handle_thread = handle.clone();
        let uri_thread = uri;
        let online = std::thread::Builder::new()
            .name("chain-core".into())
            .spawn(move || {
                let online = client_thread.is_engine_online();
                client_thread.fetch_blobs(FetchBlobsRequest {
                    beacon_block_root: vec![0; 32],
                    slot: 1,
                    versioned_hashes: vec![],
                    template: None,
                });
                let poll = poll_engine_online_with(
                    &handle_thread,
                    &uri_thread,
                    EngineRpcDeadlines::for_test().get_engine_state,
                );
                fire_fetch_blobs_with(
                    &handle_thread,
                    &uri_thread,
                    FetchBlobsRequest {
                        beacon_block_root: vec![0; 32],
                        slot: 1,
                        versioned_hashes: vec![],
                        template: None,
                    },
                    EngineRpcDeadlines::for_test().fetch_blobs,
                );
                assert!(!poll);
                online
            })
            .expect("spawn")
            .join()
            .expect("join");
        assert!(!online);
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "poll/fetch parked: {:?}",
            start.elapsed()
        );
        let _ = shutdown.send(());
    }

    #[test]
    fn bridge_uses_handle_block_on_only() {
        // §2.4: must not invoke the park-on-worker API from chain-core.
        let src = include_str!("engine_client.rs");
        assert!(
            src.contains("handle.block_on") || src.contains("self.handle.block_on"),
            "engine client must use Handle::block_on"
        );
        // Forbidden token (underscore form) must not appear in this file.
        let forbidden = ["block", "in", "place"].join("_");
        assert!(
            !src.contains(&forbidden),
            "must not reference the park-on-worker API by its rustc name"
        );
    }

    /// P0-15 / S0-A-27: every production `block_on` is the deadline helper.
    #[test]
    fn every_block_on_carries_an_explicit_duration() {
        let src = include_str!("engine_client.rs");
        let prod = src
            .split("#[cfg(test)]")
            .next()
            .expect("production half before tests");
        let helper_hits = prod.matches("fn block_on_deadline").count();
        assert_eq!(helper_hits, 1, "one deadline helper");
        let raw = prod
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && !t.starts_with("//!") && l.contains(".block_on")
            })
            .count();
        assert_eq!(
            raw, 1,
            "only block_on_deadline may call Handle::block_on; found {raw}"
        );
        assert!(
            prod.contains("tokio::time::timeout(timeout, fut)"),
            "helper must wrap the future in tokio::time::timeout"
        );
        let sites = prod.matches("block_on_deadline(").count();
        // connect, get_state, fetch_blobs, poll, fire, newPayload
        assert_eq!(
            sites, 6,
            "six production RPC sites must use the helper, got {sites}"
        );

        let fcu = include_str!("fcu_driver.rs");
        let fcu_prod = fcu.split("#[cfg(test)]").next().unwrap_or(fcu);
        let raw_fcu = fcu_prod
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && !t.starts_with("//!") && l.contains(".block_on")
            })
            .count();
        assert_eq!(
            raw_fcu, 0,
            "fcu_driver must route through block_on_deadline, not Handle::block_on"
        );
        let fcu_sites = fcu_prod.matches("block_on_deadline(").count();
        assert_eq!(
            fcu_sites, 2,
            "fcu connect + ForkchoiceUpdated must use the helper, got {fcu_sites}"
        );
        assert!(
            fcu_prod.contains("deadlines.forkchoice_updated"),
            "EngineRpcDeadlines must carry the FCU timeout"
        );
    }
}
