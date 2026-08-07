//! `p2p` service stub — Architecture §4.1, CC-01b.
//!
//! Phase 0 surface: health + reflection + `GetInfo`. Real RPCs land in Phase 2+.
//! Health peer: `chain` (§6.3).
//!
//! CC-29a: §12 metric families registered into `bs.registry` between `init` and
//! `serve` (same seam as `services/chain`).

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_config::ServiceConfig;
use cc_p2p::metrics::P2pMetrics;
use cc_proto::common::BuildInfo;
use cc_proto::p2p::p2p_service_server::{P2pService, P2pServiceServer};
use cc_proto::p2p::{GetInfoRequest, GetInfoResponse};
use serde::Deserialize;
use tonic::service::Routes;
use tonic::{Request, Response, Status};

/// Process name and config slug (`config/p2p.toml`, `CC_P2P_*`).
const SERVICE: &str = "p2p";

/// Fully-qualified gRPC service name for self-only health.
const HEALTH_SERVICE_NAME: &str = "eth.p2p.v1.P2pService";

/// Full gRPC path for the Phase 0 RPC (metrics label normalisation).
const GET_INFO_METHOD: &str = "/eth.p2p.v1.P2pService/GetInfo";

/// Per-service config: shared [`ServiceConfig`] plus future p2p-only fields (D-1).
#[derive(Debug, Deserialize)]
struct P2pConfig {
    #[serde(flatten)]
    service: ServiceConfig,
}

impl P2pConfig {
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
struct P2pStub;

#[tonic::async_trait]
impl P2pService for P2pStub {
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
    // Fail before any bind (CC-09/2): load config, then telemetry, then serve.
    let cfg = cc_config::load::<P2pConfig>(SERVICE)?;
    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;

    // CC-29a: register §12 families + libp2p-metrics sub-registry between init and serve.
    let _p2p_metrics = P2pMetrics::register(&mut bs.registry);

    let routes = Routes::default().add_service(P2pServiceServer::new(P2pStub));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
