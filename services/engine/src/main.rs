//! `engine` service stub — Architecture §4.1, CC-01b.
//!
//! Phase 0 surface: health + reflection + `GetInfo`. Real RPCs land in Phase 3+.
//! Health peer: `chain` (§6.3).
//!
//! CC-3Aa: §9.1 metric families are registered between `init` and `serve`.

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_config::ServiceConfig;
use cc_engine::config::EngineTransportConfig;
use cc_engine::jwt::JwtSecret;
use cc_engine::metrics::EngineMetrics;
use cc_engine::transport::EngineTransport;
use cc_proto::common::BuildInfo;
use cc_proto::engine::engine_service_server::{EngineService, EngineServiceServer};
use cc_proto::engine::{GetInfoRequest, GetInfoResponse};
use serde::Deserialize;
use tonic::service::Routes;
use tonic::{Request, Response, Status};

/// Process name and config slug (`config/engine.toml`, `CC_ENGINE_*`).
const SERVICE: &str = "engine";

/// Fully-qualified gRPC service name for self-only health.
const HEALTH_SERVICE_NAME: &str = "eth.engine.v1.EngineService";

/// Full gRPC path for the Phase 0 RPC (metrics label normalisation).
const GET_INFO_METHOD: &str = "/eth.engine.v1.EngineService/GetInfo";

/// Per-service config: shared [`ServiceConfig`] plus engine transport (CC-30a).
#[derive(Debug, Deserialize)]
struct EngineConfig {
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
            known_methods: vec![GET_INFO_METHOD.to_owned()],
        }
    }
}

/// Phase 0 stub: only `GetInfo` is implemented.
#[derive(Debug, Default)]
struct EngineStub;

#[tonic::async_trait]
impl EngineService for EngineStub {
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fail before any bind (CC-09/2 / CC-30/1): load config, then JWT secret,
    // then telemetry, then serve. A mis-mounted secret must not leave us
    // listening while the EL 401s forever.
    let cfg = cc_config::load::<EngineConfig>(SERVICE)?;
    // JWT secret: abort before any port bind (CC-30/1, §7). Load before init so
    // a bad secret never opens metrics/gRPC listeners.
    let jwt = JwtSecret::load(&cfg.transport.jwt_secret_path)
        .map_err(|e| anyhow::anyhow!("JWT secret: {e}"))?;
    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;

    // CC-3Aa: register §9.1 engine families between init and serve.
    let engine_metrics = EngineMetrics::register(&mut bs.registry);

    // CC-30a: transport constructed before gRPC serve.
    let _transport = EngineTransport::new(&cfg.transport, jwt, Some(engine_metrics))
        .map_err(|e| anyhow::anyhow!(e))?;

    let routes = Routes::default().add_service(EngineServiceServer::new(EngineStub));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
