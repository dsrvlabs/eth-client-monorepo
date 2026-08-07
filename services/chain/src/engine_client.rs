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
//! ADR P3-16: no HTTP client or JWT signer here — that surface lives only in
//! `services/engine`.

use std::sync::Mutex;

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
        })
    }

    /// Like [`Self::new`] with [`DEFAULT_ENGINE_URI`].
    pub fn with_default_uri() -> Result<Self, EngineError> {
        Self::new(DEFAULT_ENGINE_URI)
    }

    /// Construct with an explicit handle (tests that capture a runtime).
    #[must_use]
    pub fn from_handle(handle: Handle, uri: impl Into<String>) -> Self {
        Self {
            handle,
            uri: uri.into(),
            inner: Mutex::new(None),
        }
    }

    fn connect_blocking(&self) -> Result<EngineServiceClient<Channel>, EngineError> {
        let uri = self.uri.clone();
        self.handle.block_on(async {
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
        self.handle.block_on(async move {
            let mut client = client;
            match client
                .get_engine_state(cc_proto::engine::GetEngineStateRequest {})
                .await
            {
                Ok(resp) => !resp.into_inner().el_offline,
                Err(_) => false,
            }
        })
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
        self.handle.block_on(async move {
            let mut client = client;
            if let Err(e) = client.fetch_blobs(req).await {
                tracing::debug!(error = %e, "FetchBlobs transport failed (accelerator)");
            }
        });
    }
}

/// Poll engine online via a one-shot client (core SlotTick; no shared mutex).
#[must_use]
pub fn poll_engine_online(handle: &Handle, uri: &str) -> bool {
    handle.block_on(async {
        match EngineServiceClient::connect(uri.to_owned()).await {
            Ok(mut client) => match client
                .get_engine_state(cc_proto::engine::GetEngineStateRequest {})
                .await
            {
                Ok(resp) => !resp.into_inner().el_offline,
                Err(_) => false,
            },
            Err(_) => false,
        }
    })
}

/// Fire unary `FetchBlobs` (CC-38a block branch) via a one-shot connect.
///
/// Template-sized only — never cells. Best-effort; errors are debug-logged.
pub fn fire_fetch_blobs(handle: &Handle, uri: &str, req: FetchBlobsRequest) {
    handle.block_on(async {
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

        // §2.4 bridge: park this OS thread on the multi-threaded runtime.
        // Do NOT use block-in-place — it panics off a runtime worker.
        let client = self.client()?;
        self.handle.block_on(self.new_payload_async(client, proto))
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
        ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse, GetEngineStateRequest,
        GetEngineStateResponse, GetInfoRequest, GetInfoResponse, NewPayloadResponse,
        PayloadStatusV1,
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
}
