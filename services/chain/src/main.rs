//! `chain` service — Architecture §4.1 / §7.1, CC-18b / CC-19a.
//!
//! Wires the events task (CC-18c), timing metrics (CC-1C), the core-thread
//! import path (CC-18b), and optional checkpoint bootstrap (CC-19a).
//!
//! When `checkpoint_providers` is non-empty and `network_config` points at a
//! chain YAML, the process fetches a verified finalized anchor and spawns the
//! core before serve. Full lifecycle / aggregate health during bootstrap is
//! CC-19b — this path is the partial wire so the core *can* be spawned from a
//! fetched anchor when config enables it.

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_chain::checkpoint_sync::{
    CheckpointBootstrapConfig, bootstrap_core_from_providers, parse_optional_root,
};
use cc_chain::core::CoreConfig;
use cc_chain::service::ChainServiceImpl;
use cc_chain::{ChainMetrics, EventsConfig, EventsHandle, HeadSnapshotStore};
use cc_config::ServiceConfig;
use cc_proto::chain::chain_service_server::ChainServiceServer;
use cc_types::config::ChainConfig as NetworkChainConfig;
use cc_types::preset::Mainnet;
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
    /// Ordered checkpoint provider base URLs (CC-19a). Empty → no bootstrap.
    #[serde(default)]
    checkpoint_providers: Vec<String>,
    /// Optional operator-supplied finalized checkpoint root (`0x…`).
    #[serde(default)]
    checkpoint_root: Option<String>,
    /// Path to consensus-specs YAML for `/eth/v1/config/spec` cross-check.
    /// Required when `checkpoint_providers` is non-empty.
    #[serde(default)]
    network_config: Option<String>,
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

    let head = HeadSnapshotStore::new();
    tracing::debug!(
        max_resident_states = cfg.max_resident_states,
        body_ring_capacity = cfg.body_ring_capacity,
        checkpoint_providers = cfg.checkpoint_providers.len(),
        "residency + checkpoint config loaded"
    );

    // Optional: select KZG backend from CC-11d's default when crypto is linked.
    let _kzg_kind = cc_crypto::KzgBackendKind::default();
    tracing::info!(kzg_backend = %_kzg_kind, "chain KZG backend selection (CC-11d default)");

    // CC-19a: optional checkpoint bootstrap → spawn core from verified anchor.
    // Empty providers keep the Phase-0/1 NOT_BOOTSTRAPPED surface (compose default).
    // Full bind-before-bootstrap health lifecycle is CC-19b.
    let core_handle = if cfg.checkpoint_providers.is_empty() {
        tracing::info!("checkpoint_providers empty; core spawn deferred (NOT_BOOTSTRAPPED)");
        None
    } else {
        let network_path = cfg.network_config.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "network_config is required when checkpoint_providers is non-empty \
                 (path to hoodi/mainnet consensus YAML for /eth/v1/config/spec cross-check)"
            )
        })?;
        let network = NetworkChainConfig::from_yaml_file(network_path).map_err(|e| {
            anyhow::anyhow!("failed to load network_config {network_path}: {e}")
        })?;
        let expected = parse_optional_root(cfg.checkpoint_root.as_deref())
            .map_err(|e| anyhow::anyhow!("checkpoint_root: {e}"))?;
        let boot_cfg = CheckpointBootstrapConfig {
            providers: cfg.checkpoint_providers.clone(),
            expected_checkpoint_root: expected,
            chain_config: network,
            connect_timeout: cc_chain::PROVIDER_CONNECT_TIMEOUT,
            total_timeout: cc_chain::PROVIDER_TOTAL_TIMEOUT,
            network_retries: cc_chain::NETWORK_RETRIES,
            triple_attempts: cc_chain::TRIPLE_ATTEMPTS,
        };
        let core_cfg = CoreConfig {
            max_resident_states: cfg.max_resident_states,
            body_ring_capacity: cfg.body_ring_capacity,
            ..CoreConfig::default()
        };
        tracing::info!(
            providers = boot_cfg.providers.len(),
            "starting checkpoint bootstrap (CC-19a)"
        );
        let (core, summary) = bootstrap_core_from_providers::<Mainnet>(
            &boot_cfg,
            head.clone(),
            events.event_sender(),
            chain_metrics.clone(),
            core_cfg,
        )
        .await
        .map_err(|e| anyhow::anyhow!("checkpoint bootstrap failed: {e}"))?;
        tracing::info!(
            provider = %summary.provider,
            block_root = %summary.block_root,
            slot = summary.slot,
            genesis_time = summary.genesis.genesis_time,
            genesis_validators_root = %summary.genesis.genesis_validators_root,
            "checkpoint bootstrap complete; core thread running"
        );
        // CoreThread must live for the process; leak the join handle into a
        // static-ish owner. CC-19b will join on shutdown.
        let handle = core.handle.clone();
        std::mem::forget(core);
        Some(handle)
    };

    let svc = ChainServiceImpl::new(core_handle, head, events, chain_metrics);
    let routes = Routes::default().add_service(ChainServiceServer::new(svc));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
