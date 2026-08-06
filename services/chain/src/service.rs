//! gRPC `ChainService` implementation (CC-18b / CC-1E / CC-1F / CC-19b).
//!
//! - `ImportBlock` → core command channel (`send_timeout` 2 s)
//! - `ApplyAttestations` → core command channel (batched `on_attestation`, CC-1E)
//! - `GetHead` → [`HeadSnapshotStore`] pointer load (no core interaction)
//! - `SubscribeEvents` → events task (CC-18c)
//! - `GetCommitteeShuffling` / `GetValidatorPubkeys` → core [`Query`] (CC-1F)
//!
//! Before checkpoint bootstrap completes the core slot is empty and RPCs return
//! `FAILED_PRECONDITION` / `NOT_BOOTSTRAPPED` (§7.4). [`Self::install_core`] is
//! called from the bootstrap task after the gRPC server has already bound
//! (bind-before-bootstrap; CC-19b). All `ErrorInfo` construction goes through
//! the shared helper below so call sites never hand-assemble trailers (§7.6).

use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{
    ApplyAttestationsRequest, ApplyAttestationsResponse, Checkpoint as ProtoCheckpoint,
    GetCommitteeShufflingRequest, GetCommitteeShufflingResponse, GetHeadRequest, GetHeadResponse,
    GetInfoRequest, GetInfoResponse, GetValidatorPubkeysRequest, GetValidatorPubkeysResponse,
    ImportBlockRequest, ImportBlockResponse, SubscribeEventsRequest,
};
use cc_proto::common::BuildInfo;
use cc_proto::status_with_error_info;
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

use crate::apply_attestations::MAX_APPLY_ATTESTATIONS;
use crate::core::{CoreHandle, MAX_VALIDATOR_PUBKEYS_PER_REQUEST, QueryReply, QueryRequest};
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
///
/// The core handle is behind [`RwLock`] so the bootstrap task can install it
/// after bind without rebuilding the tonic service.
#[derive(Debug, Clone)]
pub struct ChainServiceImpl {
    core: Arc<RwLock<Option<CoreHandle>>>,
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
            core: Arc::new(RwLock::new(core)),
            head,
            events,
            metrics,
        }
    }

    /// Install the core handle after checkpoint bootstrap (CC-19b).
    ///
    /// Idempotent replace: later installs overwrite (tests only; production
    /// installs once).
    pub fn install_core(&self, handle: CoreHandle) {
        match self.core.write() {
            Ok(mut guard) => {
                *guard = Some(handle);
            }
            Err(poisoned) => {
                *poisoned.into_inner() = Some(handle);
            }
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

    /// Whether a core handle has been installed (bootstrap complete).
    pub fn is_bootstrapped(&self) -> bool {
        self.core
            .read()
            .map(|g| g.is_some())
            .unwrap_or_else(|p| p.into_inner().is_some())
    }

    /// Clone the core handle if bootstrapped.
    pub fn core_handle(&self) -> Option<CoreHandle> {
        self.core
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|p| p.into_inner().clone())
    }

    /// Core handle if bootstrapped (borrow via clone — handle is cheap).
    pub fn core(&self) -> Option<CoreHandle> {
        self.core_handle()
    }

    /// Shared `ErrorInfo` constructor for pre-bootstrap RPCs (§7.6).
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
        let Some(core) = self.core_handle() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; ImportBlock unavailable until checkpoint sync",
            ));
        };
        let response = core.import_block(request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn apply_attestations(
        &self,
        request: Request<ApplyAttestationsRequest>,
    ) -> Result<Response<ApplyAttestationsResponse>, Status> {
        // SEC-1E-1: trusted internal RPC until Phase 5 validates signatures —
        // see apply_attestations module docs / contracts.md.
        let inner = request.into_inner();
        // Fail fast on the gRPC worker so an oversized batch never queues work
        // on the core thread (bound is also enforced on the core path).
        let n = inner.attestations_ssz.len();
        if n > MAX_APPLY_ATTESTATIONS {
            return Err(Status::invalid_argument(format!(
                "ApplyAttestations batch size {n} exceeds bound of {MAX_APPLY_ATTESTATIONS}"
            )));
        }
        let Some(core) = self.core_handle() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; ApplyAttestations unavailable until checkpoint sync",
            ));
        };
        let response = core.apply_attestations(inner).await?;
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
        if !self.is_bootstrapped()
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

    async fn get_committee_shuffling(
        &self,
        request: Request<GetCommitteeShufflingRequest>,
    ) -> Result<Response<GetCommitteeShufflingResponse>, Status> {
        let Some(core) = self.core_handle() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; GetCommitteeShuffling unavailable until checkpoint sync",
            ));
        };
        let epoch = request.into_inner().epoch;
        let reply = core
            .query(QueryRequest::CommitteeShuffling { epoch })
            .await?;
        match reply {
            QueryReply::CommitteeShuffling {
                shuffled_indices,
                dependent_root,
                epoch,
                committees_per_slot,
            } => Ok(Response::new(GetCommitteeShufflingResponse {
                shuffled_indices,
                dependent_root: dependent_root.as_slice().to_vec(),
                epoch,
                committees_per_slot,
            })),
            other => Err(Status::internal(format!(
                "unexpected query reply for GetCommitteeShuffling: {other:?}"
            ))),
        }
    }

    async fn get_validator_pubkeys(
        &self,
        request: Request<GetValidatorPubkeysRequest>,
    ) -> Result<Response<GetValidatorPubkeysResponse>, Status> {
        let Some(core) = self.core_handle() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; GetValidatorPubkeys unavailable until checkpoint sync",
            ));
        };
        let req = request.into_inner();
        let indices = resolve_pubkey_indices(&req)?;
        let reply = core
            .query(QueryRequest::ValidatorPubkeys { indices })
            .await?;
        match reply {
            QueryReply::ValidatorPubkeys { indices, pubkeys } => {
                Ok(Response::new(GetValidatorPubkeysResponse {
                    indices,
                    pubkeys,
                }))
            }
            other => Err(Status::internal(format!(
                "unexpected query reply for GetValidatorPubkeys: {other:?}"
            ))),
        }
    }
}

/// Resolve the index list for `GetValidatorPubkeys`, enforcing the 256 bound
/// **before** allocating the expanded index `Vec` (SEC-1F-1).
///
/// `indices` non-empty wins over `start_index`/`count`. Empty / zero-count /
/// over-sized requests are `INVALID_ARGUMENT` (never a truncated response).
fn resolve_pubkey_indices(req: &GetValidatorPubkeysRequest) -> Result<Vec<u64>, Status> {
    if !req.indices.is_empty() {
        // Bound check before clone (indices already allocated by prost decode,
        // but reject before any further expansion / core work).
        if req.indices.len() as u64 > MAX_VALIDATOR_PUBKEYS_PER_REQUEST {
            return Err(Status::invalid_argument(format!(
                "GetValidatorPubkeys bound is {MAX_VALIDATOR_PUBKEYS_PER_REQUEST} indices per request; \
                 got {}",
                req.indices.len()
            )));
        }
        return Ok(req.indices.clone());
    }
    if req.count > 0 {
        // SEC-1F-1: reject oversize *before* `(start..end).collect()`.
        if req.count > MAX_VALIDATOR_PUBKEYS_PER_REQUEST {
            return Err(Status::invalid_argument(format!(
                "GetValidatorPubkeys bound is {MAX_VALIDATOR_PUBKEYS_PER_REQUEST} indices per request; \
                 got {}",
                req.count
            )));
        }
        let end = req
            .start_index
            .checked_add(req.count)
            .ok_or_else(|| Status::invalid_argument("GetValidatorPubkeys range overflows u64"))?;
        return Ok((req.start_index..end).collect());
    }
    Err(Status::invalid_argument(
        "GetValidatorPubkeys requires a non-empty indices list or count > 0",
    ))
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
