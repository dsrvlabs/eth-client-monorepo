//! gRPC `ChainService` implementation (CC-18b).
//!
//! - `ImportBlock` → core command channel (`send_timeout` 2 s)
//! - `GetHead` → [`HeadSnapshotStore`] pointer load (no core interaction)
//! - `SubscribeEvents` → events task (CC-18c)
//!
//! Until CC-19 checkpoint bootstrap lands, a service constructed without a
//! [`CoreHandle`] returns `FAILED_PRECONDITION` / `NOT_BOOTSTRAPPED` for
//! `ImportBlock` (and optionally `GetHead` when no snapshot has been published).

use std::pin::Pin;

use bytes::Bytes;
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{
    Checkpoint as ProtoCheckpoint, GetHeadRequest, GetHeadResponse, GetInfoRequest,
    GetInfoResponse, ImportBlockRequest, ImportBlockResponse, SubscribeEventsRequest,
};
use cc_proto::common::BuildInfo;
use cc_proto::status_with_error_info;
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

use crate::core::CoreHandle;
use crate::events::EventsHandle;
use crate::head::HeadSnapshotStore;
use crate::metrics::ChainMetrics;

/// gRPC `ErrorInfo.reason` before checkpoint bootstrap (CC-19; Architecture §7.4).
pub const REASON_NOT_BOOTSTRAPPED: &str = "NOT_BOOTSTRAPPED";

/// Domain for chain error details.
pub const ERROR_DOMAIN: &str = "eth.chain.v1";

/// Process name for `GetInfo`.
const SERVICE: &str = "chain";

/// Fully wired chain service.
#[derive(Debug, Clone)]
pub struct ChainServiceImpl {
    core: Option<CoreHandle>,
    head: HeadSnapshotStore,
    events: EventsHandle,
    #[allow(dead_code)]
    metrics: ChainMetrics,
}

impl ChainServiceImpl {
    /// Construct with an optional core (None → not bootstrapped).
    pub fn new(
        core: Option<CoreHandle>,
        head: HeadSnapshotStore,
        events: EventsHandle,
        metrics: ChainMetrics,
    ) -> Self {
        Self {
            core,
            head,
            events,
            metrics,
        }
    }

    /// Shared head snapshot store.
    pub fn head(&self) -> &HeadSnapshotStore {
        &self.head
    }

    /// Events handle.
    pub fn events(&self) -> &EventsHandle {
        &self.events
    }

    /// Core handle if bootstrapped.
    pub fn core(&self) -> Option<&CoreHandle> {
        self.core.as_ref()
    }

    fn not_bootstrapped(message: &str) -> Status {
        status_with_error_info(
            Code::FailedPrecondition,
            message,
            REASON_NOT_BOOTSTRAPPED,
            ERROR_DOMAIN,
        )
    }
}

#[tonic::async_trait]
impl ChainService for ChainServiceImpl {
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

    async fn import_block(
        &self,
        request: Request<ImportBlockRequest>,
    ) -> Result<Response<ImportBlockResponse>, Status> {
        let Some(core) = self.core.as_ref() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; ImportBlock unavailable until checkpoint sync",
            ));
        };
        let response = core.import_block(request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn get_head(
        &self,
        _request: Request<GetHeadRequest>,
    ) -> Result<Response<GetHeadResponse>, Status> {
        // Pointer load — never touches the core thread (ADR-P1-09 / §7.1).
        // When no core is present and the snapshot is still the zero default,
        // surface NOT_BOOTSTRAPPED so clients do not treat zeros as a real head.
        let snap = self.head.load();
        if self.core.is_none()
            && snap.sequence == 0
            && snap.head_root == cc_types::primitives::Root::ZERO
        {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; GetHead unavailable until checkpoint sync",
            ));
        }
        Ok(Response::new(GetHeadResponse {
            head_root: snap.head_root.as_slice().to_vec(),
            head_slot: snap.head_slot.as_u64(),
            justified: Some(ProtoCheckpoint {
                epoch: snap.justified.epoch.as_u64(),
                root: snap.justified.root.as_slice().to_vec(),
            }),
            finalized: Some(ProtoCheckpoint {
                epoch: snap.finalized.epoch.as_u64(),
                root: snap.finalized.root.as_slice().to_vec(),
            }),
        }))
    }

    async fn subscribe_events(
        &self,
        request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<BoxStreamEvent>, Status> {
        let cursor = request.into_inner().cursor;
        let mut sub = self.events.subscribe(cursor).await?;

        // Bridge EventSubscription → async Stream for tonic.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<cc_proto::chain::Event, Status>>(16);
        tokio::spawn(async move {
            loop {
                match sub.recv().await {
                    Ok(Some(ev)) => {
                        if tx.send(Ok(ev)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as BoxStreamEvent))
    }
}

/// Server-streaming response type matching the generated trait (`BoxStream`).
pub type BoxStreamEvent =
    Pin<Box<dyn Stream<Item = Result<cc_proto::chain::Event, Status>> + Send + 'static>>;

/// Build a resume cursor helper for tests.
pub fn root_bytes(root: &cc_types::primitives::Root) -> Bytes {
    Bytes::copy_from_slice(root.as_slice())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_proto::error_info_from_status;
    use prometheus_client::registry::Registry;

    use crate::events::EventsConfig;
    use crate::metrics::ChainMetrics;

    #[tokio::test]
    async fn import_without_core_is_not_bootstrapped() {
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let svc = ChainServiceImpl::new(None, HeadSnapshotStore::new(), events, metrics);
        let err = svc
            .import_block(Request::new(ImportBlockRequest {
                ssz: vec![],
                fork: 0,
                root: vec![0u8; 32],
                source: 0,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        let info = error_info_from_status(&err).unwrap().unwrap();
        assert_eq!(info.reason, REASON_NOT_BOOTSTRAPPED);
    }
}
