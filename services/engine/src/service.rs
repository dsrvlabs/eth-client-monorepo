//! `EngineService` gRPC server (CC-32b / Architecture §2.1, §1.3 Decision A).
//!
//! `NewPayload` / `ForkchoiceUpdated` ride the ordered lane to the EL.
//! `GetEngineState` reads a local snapshot and never blocks on the EL
//! (el_offline answer is CC-3B / CC-36a; declared here so the field set is stable).

use std::sync::Arc;

use cc_proto::common::BuildInfo;
use cc_proto::engine::engine_service_server::EngineService;
use cc_proto::engine::{
    ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse, GetEngineStateRequest,
    GetEngineStateResponse, GetInfoRequest, GetInfoResponse, NewPayloadRequest, NewPayloadResponse,
    PayloadStatusV1,
};
use tonic::{Request, Response, Status};

use crate::config::EngineTransportConfig;
use crate::methods::new_payload::{DecodedPayloadStatus, forkchoice_updated_v3, new_payload_v4};
use crate::metrics::EngineMetrics;
use crate::transport::SharedTransport;
use crate::version::ElForkSchedule;

/// Process name for `GetInfo` (matches binary).
const SERVICE: &str = "engine";

/// gRPC implementation of [`EngineService`].
#[derive(Debug, Clone)]
pub struct EngineServiceImpl {
    transport: SharedTransport,
    schedule: ElForkSchedule,
    metrics: Option<EngineMetrics>,
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
        let req = request.into_inner();
        let status = new_payload_v4(
            self.transport.as_ref(),
            &self.schedule,
            self.metrics.as_ref(),
            &req.ssz,
            &req.versioned_hashes,
            &req.parent_beacon_block_root,
            &req.execution_requests,
        )
        .await
        .map_err(engine_err_to_status)?;
        Ok(Response::new(NewPayloadResponse {
            payload_status: Some(to_proto_status(&status)),
        }))
    }

    async fn forkchoice_updated(
        &self,
        request: Request<ForkchoiceUpdatedRequest>,
    ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
        let req = request.into_inner();
        // Sequence drop is CC-33; this RPC still forwards to the EL so the
        // chain→engine contract is live. Stale-drop lands with the driver.
        let _sequence = req.sequence;
        let status = forkchoice_updated_v3(
            self.transport.as_ref(),
            &self.schedule,
            self.metrics.as_ref(),
            &req.head_block_hash,
            &req.safe_block_hash,
            &req.finalized_block_hash,
        )
        .await
        .map_err(engine_err_to_status)?;
        Ok(Response::new(ForkchoiceUpdatedResponse {
            payload_status: Some(to_proto_status(&status)),
            payload_id: None,
        }))
    }

    async fn get_engine_state(
        &self,
        _request: Request<GetEngineStateRequest>,
    ) -> Result<Response<GetEngineStateResponse>, Status> {
        // Placeholder until CC-36a / CC-3B wire the state machine. Declared so
        // the field set is additive-stable (`buf breaking` with no label).
        Ok(Response::new(GetEngineStateResponse {
            el_offline: false,
            internal_state: "synced".into(),
        }))
    }
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
