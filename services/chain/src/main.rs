//! `chain` service — Architecture §4.1 / §7.1, CC-18b.
//!
//! Wires the events task (CC-18c), timing metrics (CC-1C), and the core-thread
//! import path (CC-18b). Checkpoint bootstrap (CC-19) is out of scope: until
//! it lands the process serves `GetInfo` / `SubscribeEvents` and returns
//! `NOT_BOOTSTRAPPED` for `ImportBlock` / unseeded `GetHead`.

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_chain::service::ChainServiceImpl;
use cc_chain::{ChainMetrics, EventsConfig, EventsHandle, HeadSnapshotStore};
use cc_config::ServiceConfig;
use cc_proto::chain::chain_service_server::ChainServiceServer;
use serde::Deserialize;
use tonic::service::Routes;

/// Process name and config slug (`config/chain.toml`, `CC_CHAIN_*`).
const SERVICE: &str = "chain";

/// Fully-qualified gRPC service name for self-only health.
const HEALTH_SERVICE_NAME: &str = "eth.chain.v1.ChainService";

/// Full gRPC paths for metrics label normalisation.
const GET_INFO_METHOD: &str = "/eth.chain.v1.ChainService/GetInfo";
const IMPORT_BLOCK_METHOD: &str = "/eth.chain.v1.ChainService/ImportBlock";
const GET_HEAD_METHOD: &str = "/eth.chain.v1.ChainService/GetHead";
const SUBSCRIBE_EVENTS_METHOD: &str = "/eth.chain.v1.ChainService/SubscribeEvents";
const APPLY_ATTESTATIONS_METHOD: &str = "/eth.chain.v1.ChainService/ApplyAttestations";

/// Per-service config: shared [`ServiceConfig`] plus chain-only fields.
#[derive(Debug, Deserialize)]
struct ChainConfig {
    #[serde(flatten)]
    service: ServiceConfig,
    /// Max resident BeaconState values (Architecture §7.5). Default 4.
    #[serde(default = "default_max_resident_states")]
    max_resident_states: usize,
    /// Body ring capacity for shallow-reorg replay. Default 64.
    #[serde(default = "default_body_ring_capacity")]
    body_ring_capacity: usize,
    /// Event ring capacity (CC-18c). Default 1024.
    #[serde(default = "default_event_ring_capacity")]
    event_ring_capacity: usize,
    /// Per-subscriber queue capacity (CC-18c). Default 256.
    #[serde(default = "default_subscriber_queue_capacity")]
    subscriber_queue_capacity: usize,
}

fn default_max_resident_states() -> usize {
    cc_chain::residency::DEFAULT_MAX_RESIDENT_STATES
}
fn default_body_ring_capacity() -> usize {
    cc_chain::residency::DEFAULT_BODY_RING_CAPACITY
}
fn default_event_ring_capacity() -> usize {
    cc_chain::events::DEFAULT_RING_CAPACITY
}
fn default_subscriber_queue_capacity() -> usize {
    cc_chain::events::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
}

impl ChainConfig {
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
                IMPORT_BLOCK_METHOD.to_owned(),
                GET_HEAD_METHOD.to_owned(),
                SUBSCRIBE_EVENTS_METHOD.to_owned(),
                APPLY_ATTESTATIONS_METHOD.to_owned(),
            ],
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fail before any bind (CC-09/2): load config, then telemetry, then serve.
    let cfg = cc_config::load::<ChainConfig>(SERVICE)?;
    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;

    // CC-1C: register chain metrics into bs.registry between init and serve.
    let chain_metrics = ChainMetrics::register(&mut bs.registry);

    // CC-18c: events task (no fork-choice dependency on the events path).
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: cfg.event_ring_capacity,
        subscriber_queue_capacity: cfg.subscriber_queue_capacity,
        session_id: None,
    });

    // CC-18b: head snapshot store. Core thread starts after CC-19 bootstrap;
    // until then ImportBlock / unseeded GetHead return NOT_BOOTSTRAPPED.
    // Residency config is loaded here so CC-19 can pass it into spawn_core_thread
    // without a config shape change (`max_resident_states` / `body_ring_capacity`).
    let head = HeadSnapshotStore::new();
    tracing::debug!(
        max_resident_states = cfg.max_resident_states,
        body_ring_capacity = cfg.body_ring_capacity,
        "residency config loaded (core spawn deferred to CC-19)"
    );

    // Optional: select KZG backend from CC-11d's default when crypto is linked.
    // Held for future DA / blob paths; Phase 1 DA is AlwaysAvailable.
    let _kzg_kind = cc_crypto::KzgBackendKind::default();
    tracing::info!(kzg_backend = %_kzg_kind, "chain KZG backend selection (CC-11d default)");

    let svc = ChainServiceImpl::new(None, head, events, chain_metrics);
    let routes = Routes::default().add_service(ChainServiceServer::new(svc));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
