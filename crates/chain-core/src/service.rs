//! gRPC `ChainService` implementation (CC-18b / CC-1E / CC-1F / CC-19b / CC-27a / CC-3B).
//!
//! - `ImportBlock` → import lane (`send_timeout` 2 s)
//! - `ApplyAttestations` → attestation LIFO lane (batched `on_attestation`, CC-1E)
//! - `GetHead` → [`HeadSnapshotStore`] pointer load (no core interaction)
//! - `SubscribeEvents` → events task (CC-18c)
//! - `GetCommitteeShuffling` / `GetValidatorPubkeys` → core [`Query`] (CC-1F)
//! - `P2pStream` / `GetValidatorRecords` → CC-27a chain-side stream contract
//! - `IsOptimistic` → core [`Query`] over fork-choice only (CC-3B; no Phase 3 caller)
//! - `GetCanonicalRoots` → core [`Query`] for storage gap fill (CC-44a /3)
//! - `RestoreFromStore` → [`crate::restore`] (CC-45b / §3.5); available during
//!   `AwaitingRestore` before the core is installed
//!
//! Before restore or checkpoint bootstrap completes the core slot is empty and
//! RPCs return `FAILED_PRECONDITION` / `NOT_BOOTSTRAPPED` (§7.4).
//! [`Self::install_core`] is called after restore or checkpoint bootstrap once
//! the gRPC server has already bound (bind-before-bootstrap; CC-19b). All
//! `ErrorInfo` construction goes through the shared helper below so call sites
//! never hand-assemble trailers (§7.6).

use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{
    ApplyAttestationsRequest, ApplyAttestationsResponse, Checkpoint as ProtoCheckpoint,
    GetCanonicalRootsRequest, GetCanonicalRootsResponse, GetCommitteeShufflingRequest,
    GetCommitteeShufflingResponse, GetHeadRequest, GetHeadResponse, GetInfoRequest,
    GetInfoResponse, GetValidatorPubkeysRequest, GetValidatorPubkeysResponse,
    GetValidatorRecordsRequest, GetValidatorRecordsResponse, ImportBlockRequest,
    ImportBlockResponse, IsOptimisticRequest, IsOptimisticResponse, RestoreChunk, RestoreResponse,
    SubscribeEventsRequest,
};
use cc_proto::common::BuildInfo;
use cc_proto::p2p::{ChainToP2p, P2pToChain, PublishRequest};
use cc_proto::status_with_error_info;
use cc_types::config::ChainConfig;
use cc_types::preset::Mainnet;
use cc_types::primitives::Root;
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status, Streaming};

use crate::apply_attestations::MAX_APPLY_ATTESTATIONS;
use crate::core::{
    CoreConfig, CoreHandle, MAX_VALIDATOR_PUBKEYS_PER_REQUEST, MAX_VALIDATOR_RECORDS_PER_REQUEST,
    QueryReply, QueryRequest,
};
use crate::epoch_context::EpochContextStore;
use crate::events::EventsHandle;
use crate::head::HeadSnapshotStore;
use crate::metrics::ChainMetrics;
use crate::p2p_stream::{P2pStreamDeps, serve_p2p_stream};
use crate::restore::{RestoreGate, RestoreHandlerDeps, handle_restore_from_store};

/// gRPC `ErrorInfo.reason` before checkpoint bootstrap (CC-19; Architecture §7.4).
pub const REASON_NOT_BOOTSTRAPPED: &str = "NOT_BOOTSTRAPPED";

/// gRPC `ErrorInfo.reason` when `GetCanonicalRoots` starts below finalized retention (CC-44a).
pub const REASON_BELOW_FINALIZED_RETENTION: &str = "BELOW_FINALIZED_RETENTION";

/// Domain for chain error details.
pub const ERROR_DOMAIN: &str = "eth.chain.v1";

/// Process name for `GetInfo`.
const SERVICE: &str = "chain";

/// Fully wired chain service.
///
/// The core handle is behind a shared [`Arc`]<[`RwLock`]> so bootstrap
/// [`Self::install_core`] is visible to live `P2pStream` sessions (F2).
/// The epoch store identity is fixed at construction and shared with the core
/// via [`crate::core::spawn_core_thread_with_epoch`].
#[derive(Debug, Clone)]
pub struct ChainServiceImpl {
    core: Arc<RwLock<Option<CoreHandle>>>,
    head: HeadSnapshotStore,
    /// Immutable identity after construction (clone shares ArcSwap + tick bus).
    stream_deps: P2pStreamDeps,
    events: EventsHandle,
    metrics: ChainMetrics,
    /// CC-45b: restore gate (None when restore is disabled / tests without it).
    restore_gate: Option<Arc<RestoreGate>>,
    /// Network config + core knobs for the restore spawn path.
    restore_chain_config: Option<ChainConfig>,
    restore_core_cfg: Option<CoreConfig>,
}

impl ChainServiceImpl {
    /// Construct with an optional core (None → not bootstrapped).
    pub fn new(
        core: Option<CoreHandle>,
        head: HeadSnapshotStore,
        events: EventsHandle,
        metrics: ChainMetrics,
    ) -> Self {
        Self::with_epoch(core, head, EpochContextStore::new(), events, metrics)
    }

    /// Construct with an explicit shared [`EpochContextStore`] (tests / bootstrap).
    ///
    /// Prefer the core's epoch store when a core is already present so service
    /// and core share one ArcSwap identity.
    pub fn with_epoch(
        core: Option<CoreHandle>,
        head: HeadSnapshotStore,
        epoch: EpochContextStore,
        events: EventsHandle,
        metrics: ChainMetrics,
    ) -> Self {
        let epoch = core
            .as_ref()
            .map(|c| c.epoch_context().clone())
            .unwrap_or(epoch);
        let core_slot = Arc::new(RwLock::new(core));
        // S2-A-05: events producer stays for observers. Columns ingest
        // via ArchiveWrite and do not enter the ring. Missing handle
        // fail-closes (Internal), never ACK AlreadyKnown after a drop.
        let stream_deps = P2pStreamDeps::with_events(
            head.clone(),
            epoch,
            Arc::clone(&core_slot),
            Some(events.event_sender()),
        );
        Self {
            core: core_slot,
            head,
            stream_deps,
            events,
            metrics,
            restore_gate: None,
            restore_chain_config: None,
            restore_core_cfg: None,
        }
    }

    /// Attach the restore gate and network config (CC-45b production wiring).
    #[must_use]
    pub fn with_restore(
        mut self,
        gate: Arc<RestoreGate>,
        chain_config: ChainConfig,
        core_cfg: CoreConfig,
    ) -> Self {
        self.restore_gate = Some(gate);
        self.restore_chain_config = Some(chain_config);
        self.restore_core_cfg = Some(core_cfg);
        self
    }

    /// Restore gate, if attached.
    pub fn restore_gate(&self) -> Option<Arc<RestoreGate>> {
        self.restore_gate.clone()
    }

    /// Install the core handle after checkpoint bootstrap (CC-19b).
    ///
    /// Idempotent replace: later installs overwrite (tests only; production
    /// installs once). Live `P2pStream` sessions re-read this slot on every
    /// gossip object, so no reconnect is required (F2).
    ///
    /// **Epoch store identity:** callers must spawn the core with the same
    /// [`EpochContextStore`] already held by this service
    /// ([`crate::core::spawn_core_thread_with_epoch`] / bootstrap with epoch).
    /// If the core carries a different store, sessions keep reading the service
    /// store; prefer sharing at construction.
    pub fn install_core(&self, handle: CoreHandle) {
        self.stream_deps.set_core(Some(handle));
    }

    /// Shared head snapshot store.
    pub fn head(&self) -> &HeadSnapshotStore {
        &self.head
    }

    /// Shared epoch context store (`ChainView` source).
    pub fn epoch_context(&self) -> EpochContextStore {
        self.stream_deps.epoch.clone()
    }

    /// Events handle.
    pub fn events(&self) -> &EventsHandle {
        &self.events
    }

    /// Clone of stream deps (cheap: Arc handles + broadcast senders).
    pub fn stream_deps(&self) -> P2pStreamDeps {
        self.stream_deps.clone()
    }

    /// Validate and fan-out a publish request onto live `P2pStream` sessions.
    ///
    /// Unknown topic → `INVALID_ARGUMENT` / `UNKNOWN_TOPIC` (CC-27a §10.5).
    pub fn request_publish(&self, req: PublishRequest) -> Result<(), Status> {
        self.stream_deps.request_publish(req)
    }

    /// Open a `P2pStream` session against an arbitrary inbound stream (tests +
    /// the tonic handler). Avoids needing a real `tonic::Streaming` transport.
    pub async fn open_p2p_stream(
        &self,
        inbound: impl Stream<Item = Result<P2pToChain, Status>> + Send + Unpin + 'static,
    ) -> Result<Response<BoxStreamChainToP2p>, Status> {
        let outbound = serve_p2p_stream(self.stream_deps.clone(), inbound).await?;
        Ok(Response::new(outbound))
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

/// `FAILED_PRECONDITION` / `BELOW_FINALIZED_RETENTION` for GetCanonicalRoots (CC-44a).
pub fn status_below_finalized(start_slot: u64, finalized_slot: u64) -> Status {
    status_with_error_info(
        Code::FailedPrecondition,
        format!(
            "GetCanonicalRoots start_slot {start_slot} is below chain finalized retention \
             (finalized epoch start slot {finalized_slot})"
        ),
        REASON_BELOW_FINALIZED_RETENTION,
        ERROR_DOMAIN,
    )
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
        // CC-44b: storage write-behind needs the live session_id to stamp
        // durable WriteCursor (Event has no session field). Advertise it on
        // the response so a live-from-tip subscribe can resume later without
        // spamming CURSOR_UNKNOWN_SESSION.
        let session_id = sub.session_id();

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
        let mut response = Response::new(Box::pin(stream) as BoxStreamEvent);
        // ASCII digits only — always a valid metadata value.
        if let Ok(val) = session_id.to_string().parse() {
            response
                .metadata_mut()
                .insert(crate::events::SESSION_ID_METADATA_KEY, val);
        }
        Ok(response)
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

    async fn p2p_stream(
        &self,
        request: Request<Streaming<P2pToChain>>,
    ) -> Result<Response<BoxStreamChainToP2p>, Status> {
        // Stream is available pre-bootstrap for ChainView (snapshot loads);
        // block import on the stream still requires a core (verdicts → IGNORE).
        self.open_p2p_stream(request.into_inner()).await
    }

    async fn get_validator_records(
        &self,
        request: Request<GetValidatorRecordsRequest>,
    ) -> Result<Response<GetValidatorRecordsResponse>, Status> {
        let Some(core) = self.core_handle() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; GetValidatorRecords unavailable until checkpoint sync",
            ));
        };
        let req = request.into_inner();
        let indices = resolve_record_indices(&req)?;
        let reply = core
            .query(QueryRequest::ValidatorRecords { indices })
            .await?;
        match reply {
            QueryReply::ValidatorRecords { ssz, slot } => {
                Ok(Response::new(GetValidatorRecordsResponse { ssz, slot }))
            }
            other => Err(Status::internal(format!(
                "unexpected query reply for GetValidatorRecords: {other:?}"
            ))),
        }
    }

    /// CC-3B: optimistic status from **fork choice** (proto-array), never from
    /// engine liveness. `known=false` for a root the store has never seen so
    /// Phase 6 cannot invent `is_optimistic: false` for an unknown block.
    ///
    /// Root-less request → node-level predicate (`is_optimistic_node`, both
    /// CC-34c branches). Root present → per-root derivation.
    async fn is_optimistic(
        &self,
        request: Request<IsOptimisticRequest>,
    ) -> Result<Response<IsOptimisticResponse>, Status> {
        let Some(core) = self.core_handle() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; IsOptimistic unavailable until checkpoint sync",
            ));
        };
        let req = request.into_inner();
        let root = match req.root {
            None => None,
            Some(bytes) if bytes.is_empty() => None,
            Some(bytes) => {
                let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                    Status::invalid_argument(format!(
                        "IsOptimistic root must be 32 bytes; got {}",
                        bytes.len()
                    ))
                })?;
                Some(Root::from_array(arr))
            }
        };
        // Answer is read from the proto-array via core Query (fork choice only).
        let reply = core.query(QueryRequest::IsOptimistic { root }).await?;
        match reply {
            QueryReply::IsOptimistic {
                is_optimistic,
                known,
            } => Ok(Response::new(IsOptimisticResponse {
                is_optimistic,
                known,
            })),
            other => Err(Status::internal(format!(
                "unexpected query reply for IsOptimistic: {other:?}"
            ))),
        }
    }

    /// CC-44a /3: one root per canonical slot in `[start_slot, end_slot]`.
    ///
    /// Below finalized retention → `FAILED_PRECONDITION` /
    /// [`REASON_BELOW_FINALIZED_RETENTION`].
    async fn get_canonical_roots(
        &self,
        request: Request<GetCanonicalRootsRequest>,
    ) -> Result<Response<GetCanonicalRootsResponse>, Status> {
        let Some(core) = self.core_handle() else {
            return Err(Self::not_bootstrapped(
                "chain core not bootstrapped; GetCanonicalRoots unavailable until checkpoint sync",
            ));
        };
        let req = request.into_inner();
        if req.end_slot < req.start_slot {
            return Err(Status::invalid_argument(format!(
                "GetCanonicalRoots end_slot ({}) < start_slot ({})",
                req.end_slot, req.start_slot
            )));
        }
        // Bound the range so a miswired client cannot force a multi-million-slot walk.
        const MAX_RANGE: u64 = 4096;
        let span = req
            .end_slot
            .saturating_sub(req.start_slot)
            .saturating_add(1);
        if span > MAX_RANGE {
            return Err(Status::invalid_argument(format!(
                "GetCanonicalRoots range {span} exceeds bound of {MAX_RANGE}"
            )));
        }
        let reply = core
            .query(QueryRequest::CanonicalRoots {
                start_slot: req.start_slot,
                end_slot: req.end_slot,
            })
            .await?;
        match reply {
            QueryReply::CanonicalRoots { roots } => Ok(Response::new(GetCanonicalRootsResponse {
                roots: roots.into_iter().map(|r| r.as_slice().to_vec()).collect(),
            })),
            other => Err(Status::internal(format!(
                "unexpected query reply for GetCanonicalRoots: {other:?}"
            ))),
        }
    }

    /// CC-45b / §3.5: storage-pushed restore stream.
    ///
    /// Available during `AwaitingRestore` (core may still be absent). After the
    /// gate is sealed, further calls return `FAILED_PRECONDITION`.
    async fn restore_from_store(
        &self,
        request: Request<Streaming<RestoreChunk>>,
    ) -> Result<Response<RestoreResponse>, Status> {
        let Some(gate) = self.restore_gate.clone() else {
            return Err(Status::failed_precondition(
                "RestoreFromStore: restore gate not configured on this chain process",
            ));
        };
        let chain_config = self.restore_chain_config.clone().ok_or_else(|| {
            Status::failed_precondition("RestoreFromStore: chain_config not configured")
        })?;
        let core_cfg = self.restore_core_cfg.clone().unwrap_or_default();
        let deps = RestoreHandlerDeps {
            gate,
            head: self.head.clone(),
            epoch: self.epoch_context(),
            events: self.events.event_sender(),
            metrics: self.metrics.clone(),
            chain_config,
            core_cfg,
            _marker: (),
        };
        // Production is Mainnet/Hoodi-shaped (same as checkpoint bootstrap).
        handle_restore_from_store::<Mainnet>(deps, request).await
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

/// Resolve indices for `GetValidatorRecords` (explicit list only; bound 256).
fn resolve_record_indices(req: &GetValidatorRecordsRequest) -> Result<Vec<u64>, Status> {
    if req.indices.is_empty() {
        return Err(Status::invalid_argument(
            "GetValidatorRecords requires a non-empty indices list",
        ));
    }
    if req.indices.len() as u64 > MAX_VALIDATOR_RECORDS_PER_REQUEST {
        return Err(Status::invalid_argument(format!(
            "GetValidatorRecords bound is {MAX_VALIDATOR_RECORDS_PER_REQUEST} indices per request; \
             got {}",
            req.indices.len()
        )));
    }
    Ok(req.indices.clone())
}

/// Server-streaming response type matching the generated trait (`BoxStream`).
pub type BoxStreamEvent =
    Pin<Box<dyn Stream<Item = Result<cc_proto::chain::Event, Status>> + Send + 'static>>;

/// Bidirectional stream outbound half for `P2pStream`.
pub type BoxStreamChainToP2p =
    Pin<Box<dyn Stream<Item = Result<ChainToP2p, Status>> + Send + 'static>>;

// Re-export so call sites can validate without depending on p2p_stream directly.
pub use crate::p2p_stream::validate_publish_topic;

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
