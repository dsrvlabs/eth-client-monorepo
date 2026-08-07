//! `EngineService` gRPC server (CC-32b / Architecture §2.1, §1.3 Decision A).
//!
//! `NewPayload` / `ForkchoiceUpdated` ride the ordered lane to the EL.
//! `GetEngineState` reads a local snapshot and never blocks on the EL
//! (el_offline answer is CC-3B / CC-36a; declared here so the field set is stable).
//! `FetchBlobs` (CC-38a) enqueues on the fastpath lane and returns immediately.

use std::sync::Arc;

use cc_proto::common::BuildInfo;
use cc_proto::engine::engine_service_server::EngineService;
use cc_proto::engine::{
    FetchBlobsRequest, FetchBlobsResponse, ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse,
    GetEngineStateRequest, GetEngineStateResponse, GetInfoRequest, GetInfoResponse,
    NewPayloadRequest, NewPayloadResponse, PayloadStatusV1,
};
use tonic::{Request, Response, Status};

use crate::config::EngineTransportConfig;
use crate::errors::EngineError;
use crate::fastpath::FastpathLane;
use crate::inject::decode_fetch_blobs_request;
use crate::methods::fcu::{FcuGatedError, FcuSequenceGate, forkchoice_updated_v3_gated};
use crate::methods::get_blobs::NullContext;
use crate::methods::new_payload::{DecodedPayloadStatus, new_payload_v4};
use crate::metrics::EngineMetrics;
use crate::state::{CachedForkchoiceState, EngineStateHandle, UpcheckOutcome};
use crate::transport::SharedTransport;
use crate::version::ElForkSchedule;

/// Process name for `GetInfo` (matches binary).
const SERVICE: &str = "engine";

/// gRPC `ErrorInfo.reason` when an fcU sequence is dropped as stale.
pub const REASON_FCU_DROPPED_STALE: &str = "FCU_DROPPED_STALE";

/// gRPC implementation of [`EngineService`].
#[derive(Debug, Clone)]
pub struct EngineServiceImpl {
    transport: SharedTransport,
    schedule: ElForkSchedule,
    metrics: Option<EngineMetrics>,
    /// fcU sequence high-water; resets on session change / reconnect (§3.8/2, CC-33).
    fcu_gate: Arc<FcuSequenceGate>,
    /// Four-state engine machine (CC-36a / §3.7).
    state: Option<EngineStateHandle>,
    /// Fast-path lane for `FetchBlobs` (CC-38a block branch). `None` ⇒ RPC
    /// returns `UNAVAILABLE` (lane not wired — tests / early bootstrap).
    fastpath: Option<FastpathLane>,
}

impl EngineServiceImpl {
    /// Construct from the shared transport + fork schedule.
    ///
    /// When `[el_forks]` is absent, uses a zero Osaka time so Prague/Osaka V4
    /// remains selected (tests / partial fixtures). Production config always
    /// supplies the table (`config/engine.toml`).
    #[must_use]
    pub fn new(
        transport: SharedTransport,
        cfg: &EngineTransportConfig,
        metrics: Option<EngineMetrics>,
    ) -> Self {
        Self::new_with_state(transport, cfg, metrics, None)
    }

    /// Construct with an optional shared engine-state handle (CC-36a).
    #[must_use]
    pub fn new_with_state(
        transport: SharedTransport,
        cfg: &EngineTransportConfig,
        metrics: Option<EngineMetrics>,
        state: Option<EngineStateHandle>,
    ) -> Self {
        Self::new_with_fastpath(transport, cfg, metrics, state, None)
    }

    /// Construct with optional state + fastpath lane (CC-38a).
    #[must_use]
    pub fn new_with_fastpath(
        transport: SharedTransport,
        cfg: &EngineTransportConfig,
        metrics: Option<EngineMetrics>,
        state: Option<EngineStateHandle>,
        fastpath: Option<FastpathLane>,
    ) -> Self {
        let schedule = cfg.el_fork_schedule().unwrap_or(ElForkSchedule {
            osaka_time: 0,
            bpo1_time: None,
            bpo2_time: None,
            amsterdam_time: None,
        });
        Self {
            transport,
            schedule,
            metrics,
            fcu_gate: Arc::new(FcuSequenceGate::new()),
            state,
            fastpath,
        }
    }

    /// Attach / replace the fastpath lane after construction (bootstrap order).
    pub fn set_fastpath(&mut self, lane: FastpathLane) {
        self.fastpath = Some(lane);
    }

    /// Reset the fcU sequence high-water mark (reconnect / new session, §3.8/2).
    ///
    /// Production path also resets automatically when
    /// `ForkchoiceUpdatedRequest.session_id` changes (chain restart).
    pub fn reset_fcu_sequence(&self) {
        self.fcu_gate.reset();
    }

    /// Shared sequence gate (tests / session wiring).
    #[must_use]
    pub fn fcu_gate(&self) -> Arc<FcuSequenceGate> {
        Arc::clone(&self.fcu_gate)
    }

    /// Fail-closed gate: Offline / AuthFailed must not hit the EL (CC-36a review).
    async fn ensure_el_admitted(&self) -> Result<(), Status> {
        let Some(state) = &self.state else {
            return Ok(());
        };
        if state.admits_el_call().await {
            return Ok(());
        }
        let (el_offline, internal) = state.get_engine_state_fields().await;
        Err(Status::unavailable(format!(
            "execution engine unavailable (el_offline={el_offline}, state={internal})"
        )))
    }

    /// Feed ordered-lane auth errors into the state machine so a wrong JWT on
    /// `newPayload`/`fcU` becomes terminal `AuthFailed` rather than an endless
    /// soft deferral. Transient Offline is still owned by the upcheck loop.
    async fn note_ordered_lane_error(&self, err: &EngineError) {
        let Some(state) = &self.state else {
            return;
        };
        if let EngineError::Http401 { body } | EngineError::Http403 { body } = err {
            let _ = state
                .apply(UpcheckOutcome::AuthRejected {
                    body: body.clone(),
                })
                .await;
        }
    }
}

#[tonic::async_trait]
impl EngineService for EngineServiceImpl {
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

    async fn new_payload(
        &self,
        request: Request<NewPayloadRequest>,
    ) -> Result<Response<NewPayloadResponse>, Status> {
        self.ensure_el_admitted().await?;
        let req = request.into_inner();
        match new_payload_v4(
            self.transport.as_ref(),
            &self.schedule,
            self.metrics.as_ref(),
            &req.ssz,
            &req.versioned_hashes,
            &req.parent_beacon_block_root,
            &req.execution_requests,
        )
        .await
        {
            Ok(status) => Ok(Response::new(NewPayloadResponse {
                payload_status: Some(to_proto_status(&status)),
            })),
            Err(e) => {
                self.note_ordered_lane_error(&e).await;
                Err(engine_err_to_status(e))
            }
        }
    }

    async fn forkchoice_updated(
        &self,
        request: Request<ForkchoiceUpdatedRequest>,
    ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
        self.ensure_el_admitted().await?;
        let req = request.into_inner();
        let head_slot = if req.head_slot == 0 {
            None
        } else {
            Some(req.head_slot)
        };
        // Admit under ordered lane + session reset + EL call (CC-33 F1/F2).
        // Stale sequences return gRPC Aborted — never spoofed VALID.
        match forkchoice_updated_v3_gated(
            self.transport.as_ref(),
            self.fcu_gate.as_ref(),
            &self.schedule,
            self.metrics.as_ref(),
            req.sequence,
            req.session_id,
            &req.head_block_hash,
            &req.safe_block_hash,
            &req.finalized_block_hash,
            head_slot,
        )
        .await
        {
            Ok(status) => {
                // Cache triple for not-Synced → Synced re-send (CC-36 /4).
                if let Some(state) = &self.state
                    && let (Ok(head), Ok(safe), Ok(finalized)) = (
                        as_32(&req.head_block_hash),
                        as_32(&req.safe_block_hash),
                        as_32(&req.finalized_block_hash),
                    )
                {
                    state
                        .cache_forkchoice(CachedForkchoiceState {
                            head_block_hash: head,
                            safe_block_hash: safe,
                            finalized_block_hash: finalized,
                        })
                        .await;
                }
                Ok(Response::new(ForkchoiceUpdatedResponse {
                    payload_status: Some(to_proto_status(&status)),
                    payload_id: None,
                }))
            }
            Err(FcuGatedError::DroppedStale(d)) => Err(Status::aborted(format!(
                "{REASON_FCU_DROPPED_STALE}: {d}"
            ))),
            Err(FcuGatedError::Engine(e)) => {
                self.note_ordered_lane_error(&e).await;
                Err(engine_err_to_status(e))
            }
        }
    }

    async fn get_engine_state(
        &self,
        _request: Request<GetEngineStateRequest>,
    ) -> Result<Response<GetEngineStateResponse>, Status> {
        if let Some(state) = &self.state {
            let (el_offline, internal_state) = state.get_engine_state_fields().await;
            return Ok(Response::new(GetEngineStateResponse {
                el_offline,
                internal_state,
            }));
        }
        // No state machine wired (unit tests): default online/synced.
        Ok(Response::new(GetEngineStateResponse {
            el_offline: false,
            internal_state: "synced".into(),
        }))
    }

    async fn fetch_blobs(
        &self,
        request: Request<FetchBlobsRequest>,
    ) -> Result<Response<FetchBlobsResponse>, Status> {
        // Architecture §2.1: enqueue on fastpath_tx, return immediately.
        // Never awaits the EL. Column branch uses EngineStream instead.
        let Some(lane) = &self.fastpath else {
            return Err(Status::unavailable(
                "FetchBlobs: fastpath lane not configured",
            ));
        };
        let req = request.into_inner();
        let Some(decoded) = decode_fetch_blobs_request(&req) else {
            return Err(Status::invalid_argument(
                "FetchBlobs: malformed template or empty commitments",
            ));
        };
        let _outcome = lane
            .trigger_from_block_with_template(
                decoded.beacon_block_root,
                decoded.slot,
                decoded.template,
                NullContext::PrunedPool,
            )
            .await;
        Ok(Response::new(FetchBlobsResponse {}))
    }
}

fn as_32(bytes: &[u8]) -> Result<[u8; 32], ()> {
    <[u8; 32]>::try_from(bytes).map_err(|_| ())
}

fn to_proto_status(status: &DecodedPayloadStatus) -> PayloadStatusV1 {
    PayloadStatusV1 {
        status: status.status_str().to_owned(),
        latest_valid_hash: status.latest_valid_hash.map(|h| h.to_vec()),
        validation_error: status.validation_error.clone(),
    }
}

fn engine_err_to_status(err: crate::errors::EngineError) -> Status {
    use crate::errors::EngineError;
    match &err {
        EngineError::Decode { reason } => Status::invalid_argument(reason.clone()),
        EngineError::InvalidParams { message } => Status::invalid_argument(message.clone()),
        EngineError::UnsupportedFork { message } => Status::failed_precondition(message.clone()),
        EngineError::Timeout { method } => {
            Status::deadline_exceeded(format!("engine timeout on {method}"))
        }
        EngineError::Http401 { .. } | EngineError::Http403 { .. } => {
            Status::unavailable(err.to_string())
        }
        other => Status::unavailable(other.to_string()),
    }
}

/// Shared handle type for the service.
pub type SharedEngineService = Arc<EngineServiceImpl>;
