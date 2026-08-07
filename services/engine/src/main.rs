//! `engine` service — Architecture §4.1, CC-01b / CC-32b.
//!
//! Phase 0 surface: health + reflection + `GetInfo`.
//! Phase 3 (CC-32b): `NewPayload`, `ForkchoiceUpdated`, `GetEngineState`.
//! Health peer: `chain` (§6.3).
//!
//! CC-3Aa: §9.1 metric families are registered between `init` and `serve`.
//!
//! **Preset (≠13/5):** hard-wired [`cc_types::preset::Mainnet`], matching
//! `services/chain/src/main.rs:240` (`bootstrap_core_from_providers::<Mainnet>`).
//! `ExecutionPayload<P>` is generic over `Preset` and SSZ decoding needs a
//! concrete `P`; this is the existing chain decision applied consistently.

use std::sync::Arc;

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_config::ServiceConfig;
use cc_engine::config::EngineTransportConfig;
use cc_engine::jwt::JwtSecret;
use cc_engine::metrics::EngineMetrics;
use cc_engine::service::EngineServiceImpl;
use cc_engine::transport::EngineTransport;
// Mainnet preset hard-wired (≠13/5) — matches chain main.rs:240.
use cc_types::preset::Mainnet;
use serde::Deserialize;
use tonic::service::Routes;

/// Process name and config slug (`config/engine.toml`, `CC_ENGINE_*`).
const SERVICE: &str = "engine";

/// Fully-qualified gRPC service name for self-only health.
const HEALTH_SERVICE_NAME: &str = "eth.engine.v1.EngineService";

/// Full gRPC paths for metrics label normalisation.
const GET_INFO_METHOD: &str = "/eth.engine.v1.EngineService/GetInfo";
const NEW_PAYLOAD_METHOD: &str = "/eth.engine.v1.EngineService/NewPayload";
const FORKCHOICE_UPDATED_METHOD: &str = "/eth.engine.v1.EngineService/ForkchoiceUpdated";
const GET_ENGINE_STATE_METHOD: &str = "/eth.engine.v1.EngineService/GetEngineState";

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
            known_methods: vec![
                GET_INFO_METHOD.to_owned(),
                NEW_PAYLOAD_METHOD.to_owned(),
                FORKCHOICE_UPDATED_METHOD.to_owned(),
                GET_ENGINE_STATE_METHOD.to_owned(),
            ],
        }
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
    let transport = EngineTransport::new(&cfg.transport, jwt, Some(engine_metrics.clone()))
        .map_err(|e| anyhow::anyhow!(e))?;
    let transport = Arc::new(transport);

    // CC-32b: real EngineService (NewPayload / ForkchoiceUpdated / GetEngineState).
    // Preset pin: Mainnet — see module docs and chain main.rs:240 (≠13/5).
    let _preset_pin: std::marker::PhantomData<Mainnet> = std::marker::PhantomData;
    let svc = EngineServiceImpl::new(transport, &cfg.transport, Some(engine_metrics));

    // gRPC decode budget: tonic's default max_decoding_message_size is **4 MiB**.
    // That is the load-bearing upper bound on inbound NewPayload SSZ until we
    // pin an explicit constant (honest mainnet payloads can approach P2P body
    // size; raise carefully, never remove). See security note S-1 / CC-32b.
    let routes = Routes::default()
        .add_service(cc_proto::engine::engine_service_server::EngineServiceServer::new(svc));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
