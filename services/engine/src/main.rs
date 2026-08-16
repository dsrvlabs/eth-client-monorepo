//! `engine` service — thin constructor over [`cc_engine_api`] (S1-A-06).
//!
//! Fail-before-bind: JWT + sandboxed `network_config` + `[el_forks]` + KZG
//! run inside [`cc_engine_api::EngineApi::prepare`] before `init` / serve.
//! `services/engine` stays a workspace member so the 4-container topology
//! can still be run for A/B (`[ARCH]` §9.1). EngineStream's engine half is gone.

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_config::ServiceConfig;
use cc_engine::EngineApi;
use cc_engine::config::EngineTransportConfig;
use cc_engine::errors::EngineError;
use cc_engine::fastpath::sidecars::SidecarTemplate;
use cc_engine::methods::new_payload::DecodedPayloadStatus;
use cc_engine::metrics::EngineMetrics;
use cc_proto::common::BuildInfo;
use cc_proto::engine::engine_service_server::EngineService;
use cc_proto::engine::{
    FetchBlobsRequest, FetchBlobsResponse, ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse,
    GetEngineStateRequest, GetEngineStateResponse, GetInfoRequest, GetInfoResponse,
    NewPayloadRequest, NewPayloadResponse, PayloadStatusV1, SidecarTemplate as WireSidecarTemplate,
};
use cc_types::KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH;
use cc_types::containers::SignedBeaconBlockHeader;
use cc_types::preset::Mainnet;
use cc_types::primitives::{KzgCommitment, Root};
use serde::Deserialize;
use ssz::Decode;
use std::path::PathBuf;
use tonic::service::Routes;
use tonic::{Request, Response, Status};

/// Process name and config slug (`config/engine.toml`, `CC_ENGINE_*`).
const SERVICE: &str = "engine";

/// Fully-qualified gRPC service name for self-only health.
const HEALTH_SERVICE_NAME: &str = "eth.engine.v1.EngineService";

/// Full gRPC paths for metrics label normalisation.
const GET_INFO_METHOD: &str = "/eth.engine.v1.EngineService/GetInfo";
const NEW_PAYLOAD_METHOD: &str = "/eth.engine.v1.EngineService/NewPayload";
const FORKCHOICE_UPDATED_METHOD: &str = "/eth.engine.v1.EngineService/ForkchoiceUpdated";
const GET_ENGINE_STATE_METHOD: &str = "/eth.engine.v1.EngineService/GetEngineState";
const FETCH_BLOBS_METHOD: &str = "/eth.engine.v1.EngineService/FetchBlobs";

/// Per-service config: shared [`ServiceConfig`] plus engine transport (CC-30a).
#[derive(Debug, Deserialize)]
struct EngineConfig {
    /// Consensus-specs YAML for the getBlobs blob-count gate (S1-B-02).
    ///
    /// Required: a missing path must not fall back to a compiled test fixture.
    /// Override: `CC_ENGINE_NETWORK_CONFIG`.
    network_config: PathBuf,
    #[serde(flatten)]
    service: ServiceConfig,
    #[serde(flatten)]
    transport: EngineTransportConfig,
}

impl EngineConfig {
    /// Build the bootstrap [`ServiceSpec`] (D-1: lives in L3, never in `cc-config`).
    fn service_spec(&self) -> ServiceSpec {
        ServiceSpec {
            name: SERVICE,
            health_service_name: HEALTH_SERVICE_NAME,
            grpc_addr: self.service.grpc_addr,
            metrics_addr: self.service.metrics_addr,
            peers: self
                .service
                .peers
                .iter()
                .map(|(name, uri)| PeerSpec {
                    name: name.clone(),
                    uri: uri.clone(),
                })
                .collect(),
            descriptor_set: cc_proto::FILE_DESCRIPTOR_SET,
            known_methods: vec![
                GET_INFO_METHOD.to_owned(),
                NEW_PAYLOAD_METHOD.to_owned(),
                FORKCHOICE_UPDATED_METHOD.to_owned(),
                GET_ENGINE_STATE_METHOD.to_owned(),
                FETCH_BLOBS_METHOD.to_owned(),
            ],
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fail before any bind (CC-09/2 / CC-30/1): load config, then JWT +
    // network_config + forks + KZG, then telemetry, then serve.
    let cfg = cc_config::load::<EngineConfig>(SERVICE)?;
    let prepared = EngineApi::prepare(&cfg.transport, &cfg.network_config)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;

    let engine_metrics = EngineMetrics::register(&mut bs.registry);
    let api = prepared
        .finish(Some(engine_metrics))
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Preset pin: Mainnet — matches chain main.rs (≠13/5).
    let _preset_pin: std::marker::PhantomData<Mainnet> = std::marker::PhantomData;
    let svc = EngineGrpc(api);

    // gRPC decode budget: tonic's default max_decoding_message_size is **4 MiB**.
    let routes = Routes::default()
        .add_service(cc_proto::engine::engine_service_server::EngineServiceServer::new(svc));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}

/// Leftover gRPC shell so the 4-container topology still binds EngineService.
struct EngineGrpc(EngineApi);

#[tonic::async_trait]
impl EngineService for EngineGrpc {
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
        match self
            .0
            .new_payload_async(
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
            Err(e) => Err(engine_err_to_status(e)),
        }
    }

    async fn forkchoice_updated(
        &self,
        request: Request<ForkchoiceUpdatedRequest>,
    ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
        let req = request.into_inner();
        let head_slot = if req.head_slot == 0 {
            None
        } else {
            Some(req.head_slot)
        };
        match self
            .0
            .forkchoice_updated_async(
                req.sequence,
                req.session_id,
                &req.head_block_hash,
                &req.safe_block_hash,
                &req.finalized_block_hash,
                head_slot,
            )
            .await
        {
            Ok(status) => Ok(Response::new(ForkchoiceUpdatedResponse {
                payload_status: Some(to_proto_status(&status)),
                payload_id: None,
            })),
            Err(cc_engine::FcuGatedError::DroppedStale(d)) => {
                Err(cc_proto::EngineRpcReason::FcuDroppedStale
                    .to_status(tonic::Code::Aborted, d.to_string()))
            }
            Err(cc_engine::FcuGatedError::Engine(e)) => Err(engine_err_to_status(e)),
        }
    }

    async fn get_engine_state(
        &self,
        _request: Request<GetEngineStateRequest>,
    ) -> Result<Response<GetEngineStateResponse>, Status> {
        let (el_offline, internal_state) = self.0.state().get_engine_state_fields().await;
        Ok(Response::new(GetEngineStateResponse {
            el_offline,
            internal_state,
        }))
    }

    async fn fetch_blobs(
        &self,
        request: Request<FetchBlobsRequest>,
    ) -> Result<Response<FetchBlobsResponse>, Status> {
        let req = request.into_inner();
        let Some((root, slot, template)) = decode_fetch_blobs_request(&req) else {
            return Err(Status::invalid_argument(
                "FetchBlobs: malformed template or empty commitments",
            ));
        };
        self.0.fetch_blobs_async(template, root, slot).await;
        Ok(Response::new(FetchBlobsResponse {}))
    }
}

fn decode_fetch_blobs_request(req: &FetchBlobsRequest) -> Option<([u8; 32], u64, SidecarTemplate)> {
    let root = <[u8; 32]>::try_from(req.beacon_block_root.as_slice()).ok()?;
    let template = decode_wire_template(req.template.as_ref()?)?;
    if template.kzg_commitments.is_empty() {
        return None;
    }
    Some((root, req.slot, template))
}

fn decode_wire_template(wire: &WireSidecarTemplate) -> Option<SidecarTemplate> {
    let header = SignedBeaconBlockHeader::from_ssz_bytes(&wire.signed_block_header_ssz).ok()?;
    let mut kzg_commitments = Vec::with_capacity(wire.kzg_commitments.len());
    for c in &wire.kzg_commitments {
        let arr = <[u8; 48]>::try_from(c.as_slice()).ok()?;
        kzg_commitments.push(KzgCommitment::from_array(arr));
    }
    if wire.kzg_commitments_inclusion_proof.len() != KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize
    {
        return None;
    }
    let mut proof = [Root::default(); KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize];
    for (i, p) in wire.kzg_commitments_inclusion_proof.iter().enumerate() {
        proof[i] = Root::from_array(<[u8; 32]>::try_from(p.as_slice()).ok()?);
    }
    Some(SidecarTemplate::new(header, kzg_commitments, proof))
}

fn to_proto_status(status: &DecodedPayloadStatus) -> PayloadStatusV1 {
    PayloadStatusV1 {
        status: status.status_str().to_owned(),
        latest_valid_hash: status.latest_valid_hash.map(|h| h.to_vec()),
        validation_error: status.validation_error.clone(),
    }
}

fn engine_err_to_status(err: EngineError) -> Status {
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
