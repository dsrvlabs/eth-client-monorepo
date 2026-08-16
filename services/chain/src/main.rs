//! `chain` service — Architecture §4.1 / §7.1 / §7.4, CC-18b / CC-19 / CC-45b.
//!
//! Lifecycle (CC-45b primary restore, CC-19 demoted to fallback):
//! 1. Bind gRPC (`eth.chain.v1.ChainService` → SERVING immediately).
//! 2. Enter `AwaitingRestore` for `restore_grace_seconds` (default 30 s).
//! 3. Storage pushes `RestoreFromStore` (or `EMPTY`, which collapses grace
//!    immediately). Full restore → install core. EMPTY / timeout → fall back
//!    to checkpoint sync when `checkpoint_providers` is configured.
//! 4. Aggregate `""` stays NOT_SERVING until local-ready (restore handshake
//!    marks ready so storage can push). After a core is installed, SERVING
//!    additionally requires a recent `probe_core_liveness` (N=3 consecutive
//!    misses → NOT_SERVING, same N successes to restore; ADR-R-04).
//! 5. Shutdown: aggregate NOT_SERVING → core `Shutdown` + join under a single
//!    2 s envelope → drain (total SIGTERM budget remains 5 s with Phase 0 drain).
//!
//! Empty `checkpoint_providers` and no restore keeps Phase 0 compose healthy:
//! no local-ready gate, core absent, RPCs return `NOT_BOOTSTRAPPED`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_bootstrap::{
    LocalReadyHandle, PeerSpec, ServeOptions, ServiceSpec, SignalTrigger, TelemetrySettings,
    serve_with_options,
};
use cc_chain::checkpoint_sync::{
    CheckpointBootstrapConfig, bootstrap_core_from_providers_with_epoch, parse_optional_root,
};
use cc_chain::core::{CoreConfig, CoreThread};
use cc_chain::restore::{DEFAULT_RESTORE_GRACE_SECONDS, RestoreGate, RestoreGateOutcome};
use cc_chain::service::ChainServiceImpl;
use cc_chain::{ChainMetrics, EpochContextStore, EventsConfig, EventsHandle, HeadSnapshotStore};
use cc_config::ServiceConfig;
use cc_proto::chain::chain_service_server::ChainServiceServer;
use cc_types::config::ChainConfig as NetworkChainConfig;
use cc_types::preset::Mainnet;
use serde::Deserialize;
use tokio::sync::watch;
use tonic::service::Routes;

/// Owns the core OS join handle and coordinates bootstrap install vs pre-drain
/// (SEC-19b-2). Lock order: this mutex first, then `ChainServiceImpl` core
/// `RwLock` inside `install_core` — never the reverse while holding service.
#[derive(Debug, Default)]
struct CoreJoinOwner {
    thread: Option<CoreThread>,
    /// Stops the S1-A-16 sampler before the core join.
    liveness_cancel: Option<watch::Sender<bool>>,
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
    fn try_install(&mut self, svc: &ChainServiceImpl, core: CoreThread) -> Option<CoreThread> {
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
        if let Some(tx) = self.liveness_cancel.take() {
            let _ = tx.send(true);
        }
        self.thread.take()
    }

    /// Drive aggregate `local_ready` from [`cc_chain::probe_core_liveness`].
    fn spawn_liveness(
        &mut self,
        local_ready: LocalReadyHandle,
        metrics: cc_chain::ChainMetrics,
        deadline: Duration,
        interval: Duration,
    ) {
        let Some(core) = self.thread.as_ref() else {
            return;
        };
        if self.shutting_down {
            return;
        }
        let (tx, rx) = watch::channel(false);
        if let Some(prev) = self.liveness_cancel.replace(tx) {
            let _ = prev.send(true);
        }
        let handle = core.handle.clone();
        tokio::spawn(async move {
            cc_chain::run_core_liveness_loop(
                handle,
                local_ready,
                deadline,
                interval,
                rx,
                Some(metrics),
            )
            .await;
        });
    }
}

/// Per-sample deadline + 4×/slot cadence from the network slot length.
fn liveness_timing(seconds_per_slot: u64) -> (Duration, Duration) {
    let slot_ms = seconds_per_slot.max(1).saturating_mul(1_000);
    (
        cc_chain::liveness_deadline(cc_chain::DEFAULT_ATTESTATION_DUE_BPS, slot_ms),
        cc_chain::sample_interval(slot_ms),
    )
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
/// CC-3B surface (hook-without-caller); listed so gRPC metrics do not bucket as `"unknown"`.
const IS_OPTIMISTIC_METHOD: &str = "/eth.chain.v1.ChainService/IsOptimistic";
/// CC-44a additive unary for storage gap fill.
const GET_CANONICAL_ROOTS_METHOD: &str = "/eth.chain.v1.ChainService/GetCanonicalRoots";
/// CC-45b client-streaming restore push from storage.
const RESTORE_FROM_STORE_METHOD: &str = "/eth.chain.v1.ChainService/RestoreFromStore";

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
    /// Event ring entry capacity (CC-44a / `chain.event_ring_events`). Default 4096.
    #[serde(default = "default_event_ring_events", alias = "event_ring_capacity")]
    event_ring_events: usize,
    /// Event ring hard byte ceiling (CC-44a / `chain.event_ring_bytes`). Default 64 MiB.
    #[serde(default = "default_event_ring_bytes")]
    event_ring_bytes: usize,
    /// Per-subscriber queue capacity (CC-18c). Default 256.
    #[serde(default = "default_subscriber_queue_capacity")]
    subscriber_queue_capacity: usize,
    /// Ordered checkpoint provider base URLs (CC-19 demoted to **fallback**).
    ///
    /// Local store restore (CC-45b) is the primary load strategy. Checkpoint
    /// sync runs only when restore sends EMPTY or the grace timer elapses
    /// (Grandine `StateLoadStrategy::Auto` pattern). Empty → no fallback either.
    #[serde(default)]
    checkpoint_providers: Vec<String>,
    /// Optional operator-supplied finalized checkpoint root (`0x…`).
    #[serde(default)]
    checkpoint_root: Option<String>,
    /// Path to consensus-specs YAML for `/eth/v1/config/spec` cross-check.
    /// Required when `checkpoint_providers` is non-empty.
    #[serde(default)]
    network_config: Option<String>,
    /// Spec-mandated `SAFE_SLOTS_TO_IMPORT_OPTIMISTICALLY` override (CC-34c).
    ///
    /// Default **128**. Spec requires a user-configurable
    /// `--safe-slots-to-import-optimistically` flag for disaster recovery of
    /// fork-choice poisoning; wire is `CC_CHAIN_SAFE_SLOTS_TO_IMPORT_OPTIMISTICALLY`
    /// via `cc-config` only (CC-3K /4 — no ad-hoc process-env reads here).
    ///
    /// **Loaded and logged; not yet applied** to
    /// `is_optimistic_candidate_block` at the import gate (hook-without-caller
    /// until optimistic-import / disaster-recovery wiring — same family as
    /// **CC-3B** consume). Changing this value today does not change behaviour.
    #[serde(default = "default_safe_slots_to_import_optimistically")]
    safe_slots_to_import_optimistically: u64,
    /// gRPC URI for `EngineService` (CC-32b). Plain config key — not under
    /// `[peers]` (ADR P3-02 / health DAG must stay acyclic).
    #[serde(default = "default_engine_uri")]
    engine_uri: String,
    /// Network identity for the CC-4D dangerous-knob guard.
    ///
    /// Required when `event_ring_bytes` is shrunk below the production default
    /// (64 MiB); must be neither Hoodi's nor mainnet's.
    #[serde(default)]
    genesis_validators_root: Option<String>,
    /// Seconds to wait in `AwaitingRestore` before falling back to checkpoint
    /// sync (CC-45b / §3.5). Default **30**. Collapsed immediately by
    /// `RestoreFromStore{ kind: EMPTY }`.
    #[serde(default = "default_restore_grace_seconds")]
    restore_grace_seconds: u64,
    /// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` in milliseconds (P0-12). Config is the
    /// sole source; never inlined at the future-slot check.
    #[serde(default = "default_maximum_gossip_clock_disparity_ms")]
    maximum_gossip_clock_disparity_ms: u64,
}

fn default_engine_uri() -> String {
    cc_chain::engine_client::DEFAULT_ENGINE_URI.to_owned()
}

fn default_max_resident_states() -> usize {
    cc_chain::residency::DEFAULT_MAX_RESIDENT_STATES
}
fn default_body_ring_capacity() -> usize {
    cc_chain::residency::DEFAULT_BODY_RING_CAPACITY
}
fn default_event_ring_events() -> usize {
    cc_chain::events::DEFAULT_RING_CAPACITY
}
fn default_event_ring_bytes() -> usize {
    cc_chain::events::DEFAULT_RING_BYTES
}
fn default_subscriber_queue_capacity() -> usize {
    cc_chain::events::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
}
fn default_safe_slots_to_import_optimistically() -> u64 {
    cc_fork_choice::SAFE_SLOTS_TO_IMPORT_OPTIMISTICALLY
}
fn default_restore_grace_seconds() -> u64 {
    DEFAULT_RESTORE_GRACE_SECONDS
}
fn default_maximum_gossip_clock_disparity_ms() -> u64 {
    u64::try_from(cc_chain::tick::DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY.as_millis())
        .unwrap_or(5 * 100)
}

impl ChainConfig {
    /// CC-4D: refuse `event_ring_bytes` shrink unless GVR is devnet-like.
    fn check_dangerous_knobs(&self) -> Result<(), cc_config::DangerousKnobError> {
        cc_config::check_dangerous_knobs(
            self.genesis_validators_root.as_deref(),
            false,
            None,
            Some(self.event_ring_bytes),
        )
    }

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
                IS_OPTIMISTIC_METHOD.to_owned(),
                GET_CANONICAL_ROOTS_METHOD.to_owned(),
                RESTORE_FROM_STORE_METHOD.to_owned(),
            ],
        }
    }
}

// Explicit multi-thread runtime: §2.4 engine bridge parks `chain-core` via
// `Handle::block_on`; a `current_thread` runtime would deadlock that path.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Fail before any bind (CC-09/2): load config, then telemetry, then serve.
    let cfg = cc_config::load::<ChainConfig>(SERVICE)?;
    // CC-4D: ring-shrinking override is a dangerous knob (devnet-only).
    cfg.check_dangerous_knobs()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;
    if cc_config::is_event_ring_bytes_shrink(cfg.event_ring_bytes) {
        tracing::warn!(
            event_ring_bytes = cfg.event_ring_bytes,
            "chain.event_ring_bytes shrink active (devnet-only; CC-4D)"
        );
    }

    // CC-1C: register chain metrics into bs.registry between init and serve.
    let chain_metrics = ChainMetrics::register(&mut bs.registry);

    // CC-18c / CC-44a: events task (no fork-choice dependency on the events path).
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: cfg.event_ring_events,
        ring_bytes: cfg.event_ring_bytes,
        subscriber_queue_capacity: cfg.subscriber_queue_capacity,
        session_id: None,
    });
    chain_metrics.set_event_buffer_bytes_bound(cfg.event_ring_bytes as u64);

    let head = HeadSnapshotStore::new();
    // Shared with core at spawn so pre-bootstrap P2pStream sessions keep the
    // same EpochContext ArcSwap after install_core (CC-27a F2).
    let epoch = EpochContextStore::new();
    let has_checkpoint_fallback = !cfg.checkpoint_providers.is_empty();
    let restore_grace = Duration::from_secs(cfg.restore_grace_seconds);
    // Always enter AwaitingRestore so storage can push EMPTY / snapshot; when
    // grace is 0 the gate times out immediately (tests / no-restore profiles).
    let restore_gate = RestoreGate::new(restore_grace);
    // Local-ready is required when we expect a core (restore or checkpoint).
    // Phase 0 compose (no providers, no restore push expected soon) still
    // enters AwaitingRestore briefly so EMPTY collapses it.
    let needs_core = true;
    tracing::debug!(
        max_resident_states = cfg.max_resident_states,
        body_ring_capacity = cfg.body_ring_capacity,
        checkpoint_providers = cfg.checkpoint_providers.len(),
        restore_grace_seconds = cfg.restore_grace_seconds,
        safe_slots_to_import_optimistically = cfg.safe_slots_to_import_optimistically,
        has_checkpoint_fallback,
        "residency + restore + checkpoint-fallback config loaded"
    );

    // Optional: select KZG backend from CC-11d's default when crypto is linked.
    let _kzg_kind = cc_crypto::KzgBackendKind::default();
    tracing::info!(kzg_backend = %_kzg_kind, "chain KZG backend selection (CC-11d default)");

    // Network config for restore spawn / checkpoint fallback.
    // When neither providers nor a network_config path is set, use a
    // mainnet-like skeleton so restore can still seed a store in devnet.
    let network_for_restore = if let Some(path) = cfg.network_config.as_deref() {
        NetworkChainConfig::from_yaml_file(path)
            .map_err(|e| anyhow::anyhow!("failed to load network_config {path}: {e}"))?
    } else if has_checkpoint_fallback {
        return Err(anyhow::anyhow!(
            "network_config is required when checkpoint_providers is non-empty \
             (path to hoodi/mainnet consensus YAML for /eth/v1/config/spec cross-check)"
        ));
    } else {
        // Devnet / local: fixture when present, else process continues and
        // restore EMPTY → no core (NOT_BOOTSTRAPPED).
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        match NetworkChainConfig::from_yaml_file(&fixture) {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "no network_config; hoodi fixture load failed — trying bundled YAML"
                );
                NetworkChainConfig::from_yaml_str(include_str!(
                    "../../../crates/types/tests/fixtures/hoodi-config.yaml"
                ))
                .map_err(|e2| anyhow::anyhow!("bundled hoodi-config.yaml: {e2}"))?
            }
        }
    };

    let core_cfg = CoreConfig {
        max_resident_states: cfg.max_resident_states,
        body_ring_capacity: cfg.body_ring_capacity,
        engine_uri: cfg.engine_uri.clone(),
        // Production: wall-clock SlotTick for fcU floor + pending_* expiry.
        slot_tick_enabled: true,
        maximum_gossip_clock_disparity: Duration::from_millis(
            cfg.maximum_gossip_clock_disparity_ms,
        ),
        ..CoreConfig::default()
    };

    // Core starts absent; restore (primary) or checkpoint (fallback) installs it.
    let svc = ChainServiceImpl::with_epoch(
        None,
        head.clone(),
        epoch.clone(),
        events.clone(),
        chain_metrics.clone(),
    )
    .with_restore(
        Arc::clone(&restore_gate),
        network_for_restore.clone(),
        core_cfg.clone(),
    );
    let core_owner: Arc<Mutex<CoreJoinOwner>> = Arc::new(Mutex::new(CoreJoinOwner::default()));

    // Local-ready channel: serve hands us the handle once health is initialised.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    {
        let svc_boot = svc.clone();
        let head_boot = head;
        let epoch_boot = epoch;
        let events_boot = events.event_sender();
        let metrics_boot = chain_metrics;
        let core_owner_boot = Arc::clone(&core_owner);
        let gate_wait = Arc::clone(&restore_gate);
        let providers = cfg.checkpoint_providers.clone();
        let checkpoint_root = cfg.checkpoint_root.clone();
        let network_boot = network_for_restore;
        let core_cfg_boot = core_cfg;
        let has_fallback = has_checkpoint_fallback;

        // Concurrent with serve: wait for LocalReadyHandle, then AwaitingRestore
        // outcome, then install core (restore) or fall back to checkpoint.
        tokio::spawn(async move {
            let local_ready: LocalReadyHandle = match ready_rx.await {
                Ok(g) => g,
                Err(_) => {
                    tracing::error!("local-ready handle dropped before restore wait; aborting");
                    std::process::exit(1);
                }
            };
            // ── Health DAG choice (CC-45b) ──────────────────────────────────
            // Aggregate `""` is what `grpc-health-probe -addr=:9001` (compose)
            // and storage's `depends_on: chain: service_healthy` observe.
            // Require local-ready so we control the flip, then mark ready
            // **immediately at AwaitingRestore entry** so storage can start
            // and push RestoreFromStore. Self `eth.chain.v1.ChainService` is
            // already SERVING at bind. Fork-choice RPCs still return
            // NOT_BOOTSTRAPPED until a core is installed. After install,
            // the liveness sampler owns this bit (3 consecutive misses →
            // NOT_SERVING; 3 successes to restore). Engine health is not
            // flipped (ADR-P3-02 / S1-B-12).
            // (Alternative rejected: storage `service_started` — races bind;
            // dual health — compose only probes aggregate.)
            local_ready.mark_ready().await;
            let (deadline, interval) = liveness_timing(network_boot.seconds_per_slot);
            tracing::info!(
                grace_secs = gate_wait.grace().as_secs(),
                deadline_ms = deadline.as_secs_f64() * 1_000.0,
                interval_ms = interval.as_millis(),
                "AwaitingRestore: aggregate healthy; waiting for storage RestoreFromStore (or EMPTY / timeout)"
            );
            let outcome = gate_wait.wait().await;
            match outcome {
                RestoreGateOutcome::Restored(install) => {
                    tracing::info!(
                        head_root = %install.head_root,
                        head_slot = install.head_slot,
                        matched_expected = install.matched_expected,
                        "restore complete; installing core (primary path, CC-45b)"
                    );
                    let orphan = {
                        let mut guard = core_owner_boot.lock().unwrap_or_else(|p| p.into_inner());
                        let orphan = guard.try_install(&svc_boot, install.core);
                        if orphan.is_none() {
                            guard.spawn_liveness(
                                local_ready.clone(),
                                metrics_boot.clone(),
                                deadline,
                                interval,
                            );
                        }
                        orphan
                    };
                    if let Some(core) = orphan {
                        tracing::warn!(
                            "pre-drain already active; shutting down late-restored core"
                        );
                        core.shutdown_and_join().await;
                        return;
                    }
                    tracing::info!(
                        deadline_ms = deadline.as_secs_f64() * 1_000.0,
                        interval_ms = interval.as_millis(),
                        "restore lifecycle complete (core installed; liveness sampler started)"
                    );
                }
                empty_or_timeout @ (RestoreGateOutcome::Empty | RestoreGateOutcome::TimedOut) => {
                    let reason = match empty_or_timeout {
                        RestoreGateOutcome::Empty => "EMPTY",
                        RestoreGateOutcome::TimedOut => "grace timeout",
                        RestoreGateOutcome::Restored(_) => unreachable!(),
                    };
                    if !has_fallback {
                        tracing::info!(
                            reason,
                            "no checkpoint_providers; core remains absent (NOT_BOOTSTRAPPED); health already SERVING"
                        );
                        return;
                    }
                    tracing::info!(
                        reason,
                        providers = providers.len(),
                        "falling back to checkpoint sync (CC-19 demoted; primary was restore)"
                    );
                    let expected = match parse_optional_root(checkpoint_root.as_deref()) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(error = %e, "checkpoint_root");
                            std::process::exit(1);
                        }
                    };
                    let boot_cfg = CheckpointBootstrapConfig {
                        providers,
                        expected_checkpoint_root: expected,
                        chain_config: network_boot,
                        connect_timeout: cc_chain::PROVIDER_CONNECT_TIMEOUT,
                        total_timeout: cc_chain::PROVIDER_TOTAL_TIMEOUT,
                        network_retries: cc_chain::NETWORK_RETRIES,
                        triple_attempts: cc_chain::TRIPLE_ATTEMPTS,
                    };
                    match bootstrap_core_from_providers_with_epoch::<Mainnet>(
                        &boot_cfg,
                        head_boot,
                        epoch_boot,
                        events_boot,
                        metrics_boot.clone(),
                        core_cfg_boot,
                    )
                    .await
                    {
                        Ok((core, summary)) => {
                            tracing::info!(
                                provider = %summary.provider,
                                block_root = %summary.block_root,
                                slot = summary.slot,
                                "checkpoint fallback complete; installing core"
                            );
                            let orphan = {
                                let mut guard =
                                    core_owner_boot.lock().unwrap_or_else(|p| p.into_inner());
                                let orphan = guard.try_install(&svc_boot, core);
                                if orphan.is_none() {
                                    guard.spawn_liveness(
                                        local_ready.clone(),
                                        metrics_boot.clone(),
                                        deadline,
                                        interval,
                                    );
                                }
                                orphan
                            };
                            if let Some(core) = orphan {
                                tracing::warn!(
                                    "pre-drain already active; shutting down late-spawned core"
                                );
                                core.shutdown_and_join().await;
                                return;
                            }
                            tracing::info!(
                                deadline_ms = deadline.as_secs_f64() * 1_000.0,
                                interval_ms = interval.as_millis(),
                                "checkpoint-fallback lifecycle complete (core installed; liveness sampler started)"
                            );
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "checkpoint fallback failed");
                            std::process::exit(1);
                        }
                    }
                }
            }
        });
    }

    let core_owner_shutdown = Arc::clone(&core_owner);
    let options = ServeOptions {
        // Always wait for restore outcome (or EMPTY→no-core mark_ready).
        require_local_ready: needs_core,
        local_ready_tx: Some(ready_tx),
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
