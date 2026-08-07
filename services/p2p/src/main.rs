//! `p2p` service — Architecture §4.1, CC-01b / CC-20b.
//!
//! Phase 0 surface: health + reflection + `GetInfo`.
//! CC-20b: persisted identity (load **before** bind), swarm task, supervisor,
//! clock, §2.2 channel map. Listens; does **not** dial (CC-21c).
//!
//! CC-29a: §12 metric families registered into `bs.registry` between `init` and
//! serve.

use std::path::PathBuf;
use std::time::Duration;

use cc_bootstrap::{PeerSpec, SignalTrigger, TelemetrySettings};
use cc_config::ServiceConfig;
use cc_p2p::clock::ClockConfig;
use cc_p2p::identity::{self, DEFAULT_NODE_KEY_PATH};
use cc_p2p::metrics::P2pMetrics;
use cc_p2p::service::{
    DEFAULT_LISTEN_MULTIADDR, RuntimeConfig, RuntimeError, SERVICE, run_process, service_spec,
};
use cc_proto::common::BuildInfo;
use cc_proto::p2p::p2p_service_server::{P2pService, P2pServiceServer};
use cc_proto::p2p::{GetInfoRequest, GetInfoResponse};
use serde::Deserialize;
use tonic::service::Routes;
use tonic::{Request, Response, Status};

/// Full gRPC path for the Phase 0 RPC (metrics label normalisation).
const GET_INFO_METHOD: &str = "/eth.p2p.v1.P2pService/GetInfo";

/// Per-service config: shared [`ServiceConfig`] plus p2p runtime fields.
#[derive(Debug, Deserialize)]
struct P2pConfig {
    #[serde(flatten)]
    service: ServiceConfig,
    /// Path to the 32-byte secp256k1 node key.
    #[serde(default = "default_node_key_path")]
    node_key_path: PathBuf,
    /// libp2p listen multiaddr.
    #[serde(default = "default_listen_multiaddr")]
    listen_multiaddr: String,
    /// Slot clock (config-sourced until first `ChainView`).
    #[serde(default)]
    clock: ClockFileConfig,
}

#[derive(Debug, Deserialize)]
struct ClockFileConfig {
    #[serde(default)]
    genesis_time: u64,
    #[serde(default = "default_seconds_per_slot")]
    seconds_per_slot: u64,
    #[serde(default = "default_slots_per_epoch")]
    slots_per_epoch: u64,
    /// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` in milliseconds (config, never inlined in clock logic).
    #[serde(default = "default_disparity_ms")]
    maximum_gossip_clock_disparity_ms: u64,
    #[serde(default)]
    slot_clock_offset_seconds: i64,
}

impl Default for ClockFileConfig {
    fn default() -> Self {
        Self {
            genesis_time: 0,
            seconds_per_slot: default_seconds_per_slot(),
            slots_per_epoch: default_slots_per_epoch(),
            maximum_gossip_clock_disparity_ms: default_disparity_ms(),
            slot_clock_offset_seconds: 0,
        }
    }
}

fn default_node_key_path() -> PathBuf {
    PathBuf::from(DEFAULT_NODE_KEY_PATH)
}

fn default_listen_multiaddr() -> String {
    DEFAULT_LISTEN_MULTIADDR.to_owned()
}

fn default_seconds_per_slot() -> u64 {
    12
}

fn default_slots_per_epoch() -> u64 {
    32
}

fn default_disparity_ms() -> u64 {
    // Spec default MAXIMUM_GOSSIP_CLOCK_DISPARITY; config is the sole source for clock.rs.
    500
}

impl P2pConfig {
    fn runtime_config(&self) -> Result<RuntimeConfig, RuntimeError> {
        let listen_multiaddr = self
            .listen_multiaddr
            .parse()
            .map_err(|e| RuntimeError::ListenAddr(format!("{}: {e}", self.listen_multiaddr)))?;
        Ok(RuntimeConfig {
            node_key_path: self.node_key_path.clone(),
            listen_multiaddr,
            clock: ClockConfig {
                genesis_time: self.clock.genesis_time,
                seconds_per_slot: self.clock.seconds_per_slot,
                slots_per_epoch: self.clock.slots_per_epoch,
                maximum_gossip_clock_disparity: Duration::from_millis(
                    self.clock.maximum_gossip_clock_disparity_ms,
                ),
                slot_clock_offset_seconds: self.clock.slot_clock_offset_seconds,
            },
            test_swarm_panic: false,
        })
    }

    fn service_spec(&self) -> cc_bootstrap::ServiceSpec {
        service_spec(
            self.service.grpc_addr,
            self.service.metrics_addr,
            self.service
                .peers
                .iter()
                .map(|(name, uri)| PeerSpec {
                    name: name.clone(),
                    uri: uri.clone(),
                })
                .collect(),
            vec![GET_INFO_METHOD.to_owned()],
        )
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
    // Fail before any bind (CC-09/2): load config, then identity, then telemetry.
    let cfg = cc_config::load::<P2pConfig>(SERVICE)?;

    // CC-20/2 structural load order: identity **before** anything else that
    // could bind or compute custody. Refuse broad permissions here so we never
    // open ports with an unusable key.
    let _identity = identity::load_or_create(&cfg.node_key_path)?;

    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;

    // CC-29a: register §12 families + libp2p-metrics sub-registry between init and serve.
    let p2p_metrics = P2pMetrics::register(&mut bs.registry);

    let runtime_cfg = cfg.runtime_config()?;
    let routes = Routes::default().add_service(P2pServiceServer::new(P2pStub));

    match run_process(
        bs,
        cfg.service_spec(),
        routes,
        runtime_cfg,
        p2p_metrics,
        SignalTrigger::UnixSignals,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(RuntimeError::SwarmPanic { task, payload }) => {
            // ADR P2-13: swarm is process-fatal — non-zero exit for compose restart.
            tracing::error!(task, payload = %payload, "exiting non-zero after swarm panic");
            std::process::exit(1);
        }
        Err(e) => Err(e.into()),
    }
}
