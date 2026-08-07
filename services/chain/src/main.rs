//! `chain` service — Architecture §4.1 / §7.1 / §7.4, CC-18b / CC-19a / CC-19b.
//!
//! Lifecycle (CC-19b):
//! 1. Bind gRPC (`eth.chain.v1.ChainService` → SERVING immediately).
//! 2. Aggregate `""` stays NOT_SERVING while checkpoint bootstrap runs.
//! 3. Bootstrap completes → install core → mark local ready → aggregate SERVING
//!    (also requires peers SERVING when configured).
//! 4. Shutdown: aggregate NOT_SERVING → core `Shutdown` + join under a single
//!    2 s envelope → drain (total SIGTERM budget remains 5 s with Phase 0 drain).
//!
//! Empty `checkpoint_providers` keeps Phase 0 compose healthy: no local-ready
//! gate, core absent, RPCs return `NOT_BOOTSTRAPPED`.

use std::sync::{Arc, Mutex};

use cc_bootstrap::{
    LocalReadyHandle, PeerSpec, ServeOptions, ServiceSpec, SignalTrigger, TelemetrySettings,
    serve_with_options,
};
use cc_chain::checkpoint_sync::{
    CheckpointBootstrapConfig, bootstrap_core_from_providers_with_epoch, parse_optional_root,
};
use cc_chain::core::{CoreConfig, CoreThread};
use cc_chain::service::ChainServiceImpl;
use cc_chain::{
    ChainMetrics, EpochContextStore, EventsConfig, EventsHandle, HeadSnapshotStore,
};
use cc_config::ServiceConfig;
use cc_proto::chain::chain_service_server::ChainServiceServer;
use cc_types::config::ChainConfig as NetworkChainConfig;
use cc_types::preset::Mainnet;
use serde::Deserialize;
use tonic::service::Routes;

/// Owns the core OS join handle and coordinates bootstrap install vs pre-drain
/// (SEC-19b-2). Lock order: this mutex first, then `ChainServiceImpl` core
/// `RwLock` inside `install_core` — never the reverse while holding service.
#[derive(Debug, Default)]
struct CoreJoinOwner {
    thread: Option<CoreThread>,
    /// Set by pre-drain before `take`; bootstrap must not install without
    /// joining locally when this is true.
    shutting_down: bool,
}

impl CoreJoinOwner {
    /// Install core into the service and take join ownership.
    ///
    /// Returns the `core` unchanged if drain already started (caller joins it).
    /// Holds `self` only for the synchronous install/store step (no await).
    ///
    /// `Option` rather than `Result` so the large `CoreThread` is not an Err variant.
    fn try_install(
        &mut self,
        svc: &ChainServiceImpl,
        core: CoreThread,
    ) -> Option<CoreThread> {
        if self.shutting_down {
            return Some(core);
        }
        // install_core before store so RPCs never see a core we cannot join
        // unless we also own the JoinHandle.
        svc.install_core(core.handle.clone());
        self.thread = Some(core);
        None
    }

    /// Begin drain: seal further installs and take the join handle if present.
    fn take_for_shutdown(&mut self) -> Option<CoreThread> {
        self.shutting_down = true;
        self.thread.take()
    }
}

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
    // Shared with core at spawn so pre-bootstrap P2pStream sessions keep the
    // same EpochContext ArcSwap after install_core (CC-27a F2).
    let epoch = EpochContextStore::new();
    let needs_bootstrap = !cfg.checkpoint_providers.is_empty();
    tracing::debug!(
        max_resident_states = cfg.max_resident_states,
        body_ring_capacity = cfg.body_ring_capacity,
        checkpoint_providers = cfg.checkpoint_providers.len(),
        needs_bootstrap,
        "residency + checkpoint config loaded"
    );

    // Optional: select KZG backend from CC-11d's default when crypto is linked.
    let _kzg_kind = cc_crypto::KzgBackendKind::default();
    tracing::info!(kzg_backend = %_kzg_kind, "chain KZG backend selection (CC-11d default)");

    // Core starts absent; bootstrap task installs it after bind (CC-19b).
    let svc = ChainServiceImpl::with_epoch(
        None,
        head.clone(),
        epoch.clone(),
        events.clone(),
        chain_metrics.clone(),
    );
    let core_owner: Arc<Mutex<CoreJoinOwner>> = Arc::new(Mutex::new(CoreJoinOwner::default()));

    // Local-ready channel: serve hands us the handle once health is initialised.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    if needs_bootstrap {
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
        let svc_boot = svc.clone();
        let head_boot = head;
        let epoch_boot = epoch;
        let events_boot = events.event_sender();
        let metrics_boot = chain_metrics;
        let core_owner_boot = Arc::clone(&core_owner);

        // Concurrent with serve: wait for LocalReadyHandle (health up), then
        // fetch+spawn, install core, mark aggregate ready. Fail-fast on error.
        tokio::spawn(async move {
            let gate: LocalReadyHandle = match ready_rx.await {
                Ok(g) => g,
                Err(_) => {
                    tracing::error!("local-ready handle dropped before bootstrap; aborting");
                    std::process::exit(1);
                }
            };
            tracing::info!(
                providers = boot_cfg.providers.len(),
                "starting checkpoint bootstrap after health init (CC-19b; bind races multi-minute fetch)"
            );
            match bootstrap_core_from_providers_with_epoch::<Mainnet>(
                &boot_cfg,
                head_boot,
                epoch_boot,
                events_boot,
                metrics_boot,
                core_cfg,
            )
            .await
            {
                Ok((core, summary)) => {
                    tracing::info!(
                        provider = %summary.provider,
                        block_root = %summary.block_root,
                        slot = summary.slot,
                        genesis_time = summary.genesis.genesis_time,
                        genesis_validators_root = %summary.genesis.genesis_validators_root,
                        "checkpoint bootstrap complete; installing core"
                    );
                    // SEC-19b-2: under core_owner lock — install + store, or
                    // join locally if pre-drain already sealed installs.
                    let orphan = {
                        let mut guard = core_owner_boot
                            .lock()
                            .unwrap_or_else(|p| p.into_inner());
                        guard.try_install(&svc_boot, core)
                    };
                    if let Some(core) = orphan {
                        tracing::warn!(
                            "pre-drain already active; shutting down late-spawned core without mark_ready"
                        );
                        core.shutdown_and_join().await;
                        return;
                    }
                    gate.mark_ready().await;
                    tracing::info!("aggregate local-ready set; bootstrap lifecycle complete");
                }
                Err(e) => {
                    tracing::error!(error = %e, "checkpoint bootstrap failed");
                    std::process::exit(1);
                }
            }
        });
    } else {
        tracing::info!(
            "checkpoint_providers empty; core absent (NOT_BOOTSTRAPPED); aggregate ready without gate"
        );
        // Drop unused receiver so serve does not need to send when gate off.
        drop(ready_rx);
    }

    let core_owner_shutdown = Arc::clone(&core_owner);
    let options = ServeOptions {
        // When bootstrapping: aggregate stays NOT_SERVING until mark_ready.
        // Empty providers: Phase 0 compose — aggregate SERVING as soon as bound.
        require_local_ready: needs_bootstrap,
        local_ready_tx: if needs_bootstrap {
            Some(ready_tx)
        } else {
            // Avoid hanging if someone still holds the sender.
            drop(ready_tx);
            None
        },
        on_pre_drain: Some(Box::new(move || {
            Box::pin(async move {
                // Seal installs then take join ownership (SEC-19b-2).
                let core = {
                    let mut guard = core_owner_shutdown
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    guard.take_for_shutdown()
                };
                if let Some(core) = core {
                    tracing::info!(
                        "pre-drain: shutting down chain-core (single {}s Shutdown+join budget)",
                        cc_chain::SHUTDOWN_JOIN_TIMEOUT.as_secs()
                    );
                    // Single 2 s envelope for oneshot + OS join; Phase 0 drain
                    // (3 s) + notify pause still fit under the 5 s SIGTERM budget
                    // when the core stops promptly (Architecture §7.4).
                    core.shutdown_and_join().await;
                }
            })
        })),
    };

    let routes = Routes::default().add_service(ChainServiceServer::new(svc));
    // Production path: Unix signals + lifecycle options (local-ready + core join).
    serve_with_options(
        bs,
        cfg.service_spec(),
        routes,
        options,
        SignalTrigger::UnixSignals,
    )
    .await?;
    Ok(())
}
