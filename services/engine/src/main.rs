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
use cc_engine::SubscriptionSet;
use cc_engine::capabilities::CapabilityCache;
use cc_engine::config::EngineTransportConfig;
use cc_engine::fastpath::{FastpathLane, hoodi_blob_bound, production_cell_kzg};
use cc_engine::inject::{INJECT_QUEUE_BOUND, InjectStreamConfig, run_inject_stream_client};
use cc_engine::jwt::JwtSecret;
use cc_engine::metrics::EngineMetrics;
use cc_engine::service::EngineServiceImpl;
use cc_engine::state::{EngineStateHandle, spawn_upcheck_driver};
use cc_engine::transport::EngineTransport;
// Mainnet preset hard-wired (≠13/5) — matches chain main.rs:240.
use cc_types::preset::Mainnet;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
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
const FETCH_BLOBS_METHOD: &str = "/eth.engine.v1.EngineService/FetchBlobs";

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
                FETCH_BLOBS_METHOD.to_owned(),
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
    // JwtSecret is crate-private on cc-engine-api (ADR-R-03); pass the
    // already-loaded bytes so the file is read and the crc32 line fires once.
    let transport = EngineTransport::from_config_secret_bytes(
        &cfg.transport,
        jwt.as_bytes(),
        Some(engine_metrics.clone()),
    )
    .map_err(|e| anyhow::anyhow!(e))?;
    let transport = Arc::new(transport);

    // CC-36a: four-state machine + detached/floored upcheck driver.
    let slot_duration = Duration::from_millis(cfg.transport.slot_duration_ms.max(1));
    let state = EngineStateHandle::new(
        Arc::new(CapabilityCache::new()),
        Some(engine_metrics.clone()),
        slot_duration,
    );
    let schedule = cfg
        .transport
        .el_fork_schedule()
        .unwrap_or(cc_engine::version::ElForkSchedule {
            osaka_time: 0,
            bpo1_time: None,
            bpo2_time: None,
            amsterdam_time: None,
        });
    let _upcheck = spawn_upcheck_driver(
        state.clone(),
        Arc::clone(&transport),
        Some(engine_metrics.clone()),
        schedule,
        slot_duration,
    );

    // CC-37a/b + CC-38a: fastpath lane + ninth-contract inject stream.
    // Subscription starts empty (fail-closed) until p2p pushes SubscriptionSet.
    // Abort before serve if the committed trusted setup cannot load (P1-A/25).
    let kzg = Some(production_cell_kzg().map_err(|e| anyhow::anyhow!("KZG trusted setup: {e}"))?);
    let lane = FastpathLane::new(
        Arc::clone(&transport),
        Some(engine_metrics.clone()),
        hoodi_blob_bound(),
        None,
        kzg,
        SubscriptionSet::empty(),
    );
    let _fastpath_worker = lane.spawn_worker();
    let (inject_tx, inject_rx) = mpsc::channel(INJECT_QUEUE_BOUND);
    lane.set_inject_tx(inject_tx).await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let inject_cfg = InjectStreamConfig {
        p2p_uri: cfg.transport.p2p_uri.clone(),
        ..InjectStreamConfig::default()
    };
    let _inject_client = tokio::spawn(run_inject_stream_client(
        inject_cfg,
        inject_rx,
        lane.clone(),
        Some(engine_metrics.clone()),
        shutdown_rx,
    ));
    // Keep shutdown sender alive for process lifetime (drop → client exits).
    let _shutdown_tx = shutdown_tx;

    // CC-32b + CC-38a: EngineService (NewPayload / fcU / GetEngineState / FetchBlobs).
    // Preset pin: Mainnet — see module docs and chain main.rs:240 (≠13/5).
    let _preset_pin: std::marker::PhantomData<Mainnet> = std::marker::PhantomData;
    let svc = EngineServiceImpl::new_with_fastpath(
        transport,
        &cfg.transport,
        Some(engine_metrics),
        Some(state),
        Some(lane),
    );

    // gRPC decode budget: tonic's default max_decoding_message_size is **4 MiB**.
    // That is the load-bearing upper bound on inbound NewPayload SSZ until we
    // pin an explicit constant (honest mainnet payloads can approach P2P body
    // size; raise carefully, never remove). See security note S-1 / CC-32b.
    let routes = Routes::default()
        .add_service(cc_proto::engine::engine_service_server::EngineServiceServer::new(svc));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
