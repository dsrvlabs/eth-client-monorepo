//! `storage` service — Architecture §4.1, CC-01b / CC-4Ca / CC-44b.
//!
//! Phase 0 surface: health + reflection + `GetInfo`. Real serve RPCs land later.
//! Health peer: `chain` (§6.3).
//!
//! CC-4Ca: §10.1 metric families are registered between `init` and `serve`.
//! CC-44b: single writer + write-behind task spawns (append-only here).

mod metrics;
mod migrate;
mod write_behind;
mod writer;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_config::ServiceConfig;
use cc_proto::common::BuildInfo;
use cc_proto::storage::storage_service_server::{StorageService, StorageServiceServer};
use cc_proto::storage::{GetInfoRequest, GetInfoResponse};
use cc_store::engine::{Durability, EngineOptions};
use cc_store::{ConfigDigestInput, Store, StoreOpenOptions};
use cc_types::{ChainConfig, Root};
use metrics::StorageMetrics;
use serde::Deserialize;
use tokio::sync::watch;
use tonic::service::Routes;
use tonic::{Request, Response, Status};
use migrate::{MigrationConfig, Migrator};
use write_behind::{WriteBehindConfig, spawn_write_behind};
use writer::{WriterBounds, WriterFaults, load_write_cursor, spawn_writer};
use cc_store::SplitLock;

/// Process name and config slug (`config/storage.toml`, `CC_STORAGE_*`).
const SERVICE: &str = "storage";

/// Fully-qualified gRPC service name for self-only health.
const HEALTH_SERVICE_NAME: &str = "eth.storage.v1.StorageService";

/// Full gRPC path for the Phase 0 RPC (metrics label normalisation).
const GET_INFO_METHOD: &str = "/eth.storage.v1.StorageService/GetInfo";

/// Per-service config: shared [`ServiceConfig`] plus storage-only fields (D-1).
#[derive(Debug, Deserialize)]
struct StorageConfig {
    #[serde(flatten)]
    service: ServiceConfig,
    /// §2.7 / CC-4H: run store invariants at open and after migration/prune.
    ///
    /// Devnet/local default in `config/storage.toml` is `true`. Hoodi soak should
    /// set `CC_STORAGE_CHECK_INVARIANTS=false` (no multi-file profiles yet).
    #[serde(default = "default_check_invariants")]
    check_invariants: bool,
    /// On-disk store directory (CC-44b).
    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,
    /// Engine durability token (`immediate` | `paranoid`).
    #[serde(default = "default_durability")]
    durability: String,
    /// Network identity for the CC-4D dangerous-knob guard / config digest.
    ///
    /// Required when [`Self::retention_override`] or [`StorageDebug::crash_point`]
    /// is set; must be neither Hoodi's nor mainnet's.
    #[serde(default)]
    genesis_validators_root: Option<String>,
    /// Compressed-retention venue override (CC-4D / Architecture §10.4).
    #[serde(default)]
    retention_override: Option<RetentionOverride>,
    /// Fault-injection knobs (CC-4D / Architecture §2.5). Config, not env.
    #[serde(default)]
    debug: StorageDebug,
    // ── CC-44b write-behind / writer ────────────────────────────────────────
    /// One commit per N slots — **the loss bound** (§4.4). Default 1.
    #[serde(default = "default_commit_slots")]
    commit_slots: u64,
    /// Flush after this many events without a slot boundary. Default 64.
    #[serde(default = "default_commit_max_events")]
    commit_max_events: usize,
    /// Flush after this many milliseconds. Default 4000.
    #[serde(default = "default_commit_max_latency_ms")]
    commit_max_latency_ms: u64,
    /// P0 channel bound (slots' commit units). Default 32; on full **block**.
    #[serde(default = "default_writer_p0_bound")]
    writer_p0_bound: usize,
    /// P1 channel bound. Default 64; on full **block**.
    #[serde(default = "default_writer_p1_bound")]
    writer_p1_bound: usize,
    /// P2 channel bound. Default 256; on full **drop newest**.
    #[serde(default = "default_writer_p2_bound")]
    writer_p2_bound: usize,
    /// When false, skip opening the store / spawning writer + write-behind
    /// (Phase 0 compose without a data volume). Default **true**.
    #[serde(default = "default_enable_write_path")]
    enable_write_path: bool,
    /// CC-41: hot/cold migration cadence in epochs (default **1**).
    #[serde(default = "default_epochs_per_migration")]
    epochs_per_migration: u64,
}

/// `storage.retention_override` — non-spec retention windows for the discharging
/// prune venue (columns 64 / blocks 256 on the compressed profile).
#[derive(Debug, Clone, Deserialize)]
struct RetentionOverride {
    #[allow(dead_code)] // consumed by prune path when CC-46a lands
    columns_epochs: u64,
    #[allow(dead_code)] // consumed by prune path when CC-46a lands
    blocks_epochs: u64,
}

/// `storage.debug.*` — fault injection (CC-48 /3 uses `crash_point`).
#[derive(Debug, Clone, Default, Deserialize)]
struct StorageDebug {
    /// e.g. `"after_put_before_commit"`. Absent / empty → inactive.
    #[serde(default)]
    #[allow(dead_code)] // engine seam abort when CC-48 /3 lands
    crash_point: Option<String>,
}

fn default_check_invariants() -> bool {
    true
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("data/storage")
}
fn default_durability() -> String {
    "immediate".to_owned()
}
fn default_commit_slots() -> u64 {
    write_behind::DEFAULT_COMMIT_SLOTS
}
fn default_commit_max_events() -> usize {
    write_behind::DEFAULT_COMMIT_MAX_EVENTS
}
fn default_commit_max_latency_ms() -> u64 {
    4_000
}
fn default_writer_p0_bound() -> usize {
    writer::WRITER_P0_BOUND
}
fn default_writer_p1_bound() -> usize {
    writer::WRITER_P1_BOUND
}
fn default_writer_p2_bound() -> usize {
    writer::WRITER_P2_BOUND
}
fn default_enable_write_path() -> bool {
    true
}
fn default_epochs_per_migration() -> u64 {
    migrate::DEFAULT_EPOCHS_PER_MIGRATION
}

impl StorageConfig {
    /// CC-4D: refuse retention override / crash_point unless GVR is devnet-like.
    fn check_dangerous_knobs(&self) -> Result<(), cc_config::DangerousKnobError> {
        cc_config::check_dangerous_knobs(
            self.genesis_validators_root.as_deref(),
            self.retention_override.is_some(),
            self.debug.crash_point.as_deref(),
            None,
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
            known_methods: vec![GET_INFO_METHOD.to_owned()],
        }
    }

    fn writer_bounds(&self) -> WriterBounds {
        WriterBounds {
            p0: self.writer_p0_bound.max(1),
            p1: self.writer_p1_bound.max(1),
            p2: self.writer_p2_bound.max(1),
        }
    }

    fn write_behind_config(&self) -> WriteBehindConfig {
        let chain_uri = self
            .service
            .peers
            .get("chain")
            .map(|u| u.to_string())
            .unwrap_or_else(|| "http://127.0.0.1:9001".to_owned());
        WriteBehindConfig {
            chain_uri,
            commit_slots: self.commit_slots.max(1),
            commit_max_events: self.commit_max_events.max(1),
            commit_max_latency: Duration::from_millis(self.commit_max_latency_ms.max(1)),
            ..WriteBehindConfig::default()
        }
    }

    fn migration_config(&self) -> MigrationConfig {
        MigrationConfig {
            epochs_per_migration: self.epochs_per_migration.max(1),
        }
    }
}

#[cfg(test)]
mod config_tests {
    // Edition 2024: env mutation is unsafe and must be serialised.
    #![allow(unsafe_code)]
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn storage_toml_path() -> std::path::PathBuf {
        // services/storage → repo root `config/storage.toml` (CWD-independent).
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/storage.toml")
    }

    #[test]
    fn check_invariants_true_in_storage_toml() {
        // AC: config-loading test (not file-string grep). Devnet/local profile polarity.
        let _g = env_lock();
        // Ensure no stale override from a parallel env test.
        // SAFETY: exclusive via env_lock.
        unsafe { std::env::remove_var("CC_STORAGE_CHECK_INVARIANTS") };
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert!(
            cfg.check_invariants,
            "devnet/local storage.toml must default check_invariants = true"
        );
    }

    #[test]
    fn check_invariants_env_override_false_for_hoodi_soak() {
        // Documented Hoodi soak polarity: CC_STORAGE_CHECK_INVARIANTS=false.
        let _g = env_lock();
        let key = "CC_STORAGE_CHECK_INVARIANTS";
        let prev = std::env::var(key).ok();
        // SAFETY: exclusive via env_lock; restored below.
        unsafe { std::env::set_var(key, "false") };
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
        assert!(
            !cfg.check_invariants,
            "Hoodi soak override CC_STORAGE_CHECK_INVARIANTS=false must load as false"
        );
    }

    /// CC-4D /1: committed storage.toml has no dangerous knobs active, so the
    /// guard is a no-op even without a GVR.
    #[test]
    fn committed_storage_toml_passes_dangerous_knob_guard() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_CHECK_INVARIANTS");
            std::env::remove_var("CC_STORAGE_GENESIS_VALIDATORS_ROOT");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert!(cfg.retention_override.is_none());
        assert!(
            cfg.debug
                .crash_point
                .as_deref()
                .unwrap_or("")
                .is_empty()
        );
        cfg.check_dangerous_knobs()
            .expect("default storage.toml must start without GVR");
    }

    /// CC-44 /5: `commit_slots = 1` with the loss-bound comment in storage.toml.
    #[test]
    fn commit_slots_default_is_one_loss_bound() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_COMMIT_SLOTS");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(cfg.commit_slots, 1, "commit_slots IS the loss bound (§4.4)");
        assert_eq!(cfg.commit_max_events, 64);
        assert_eq!(cfg.commit_max_latency_ms, 4_000);
        assert_eq!(cfg.writer_p0_bound, 32);
        assert_eq!(cfg.writer_p1_bound, 64);
        assert_eq!(cfg.writer_p2_bound, 256);
        // Comment presence (loss-bound wording from §4.4).
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            text.contains("this value IS the loss bound"),
            "storage.toml must carry the §4.4 loss-bound comment verbatim-ish"
        );
    }

    /// CC-41: `epochs_per_migration = 1` default in storage.toml.
    #[test]
    fn epochs_per_migration_default_is_one() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_EPOCHS_PER_MIGRATION");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(
            cfg.epochs_per_migration, 1,
            "Lighthouse --epochs-per-migration default"
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("epochs_per_migration"));
    }
}

/// Phase 0 stub: only `GetInfo` is implemented.
#[derive(Debug, Default)]
struct StorageStub;

#[tonic::async_trait]
impl StorageService for StorageStub {
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

/// Open the store under `data_dir` with durability + digest from config.
fn open_store(cfg: &StorageConfig) -> anyhow::Result<Store> {
    let durability = Durability::parse(&cfg.durability)
        .map_err(|e| anyhow::anyhow!("durability: {e}"))?;
    let gvr = parse_gvr(cfg.genesis_validators_root.as_deref())?;
    // Digest inputs: use mainnet-scalar defaults until a network_config path lands.
    // Fork epochs / BLOB_SCHEDULE come from a minimal mainnet-shaped ChainConfig
    // so a fresh store opens without a YAML fixture dependency.
    let chain = ChainConfig::mainnet_like_for_digest();
    let digest_input = ConfigDigestInput::with_mainnet_scalars(chain, gvr);
    let opts = StoreOpenOptions::from_config(
        EngineOptions::default().with_durability(durability),
        &digest_input,
    )?
    .with_check_invariants(cfg.check_invariants);
    Store::open(&cfg.data_dir, opts).map_err(|e| anyhow::anyhow!("store open: {e}"))
}

fn parse_gvr(s: Option<&str>) -> anyhow::Result<Root> {
    let Some(raw) = s.filter(|s| !s.is_empty()) else {
        // Devnet default: zero root when unset (local write-path bring-up).
        return Ok(Root::ZERO);
    };
    let hex = raw.strip_prefix("0x").unwrap_or(raw);
    if hex.len() != 64 {
        anyhow::bail!("genesis_validators_root must be 32-byte hex, got len {}", hex.len());
    }
    let mut arr = [0u8; 32];
    for i in 0..32 {
        arr[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|e| anyhow::anyhow!("genesis_validators_root hex: {e}"))?;
    }
    Ok(Root::from_array(arr))
}

/// Minimal chain config for the config-digest input when no network YAML is set.
trait MainnetLikeDigest {
    fn mainnet_like_for_digest() -> Self;
}

impl MainnetLikeDigest for ChainConfig {
    fn mainnet_like_for_digest() -> Self {
        // Prefer loading the committed Hoodi fixture when present; fall back to
        // a compile-time skeleton so unit tests / bare binaries still open.
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        if fixture.is_file()
            && let Ok(cfg) = ChainConfig::from_yaml_file(&fixture)
        {
            return cfg;
        }
        // Last-resort skeleton (digest is still well-defined).
        match ChainConfig::from_yaml_str(include_str!(
            "../../../crates/types/tests/fixtures/hoodi-config.yaml"
        )) {
            Ok(cfg) => cfg,
            Err(e) => {
                // Bundle is compile-time; a parse failure is a shipping bug.
                tracing::error!(error = %e, "bundled hoodi-config.yaml failed to parse");
                // Return a zeroed-epoch skeleton via re-parse of empty is impossible;
                // panic is process-fatal at startup before bind (same as open fail).
                std::process::exit(1);
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fail before any bind (CC-09/2): load config, then telemetry, then serve.
    let cfg = cc_config::load::<StorageConfig>(SERVICE)?;
    // CC-4D: one guard, three knobs — retention_override + crash_point here.
    cfg.check_dangerous_knobs()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;
    if let Some(ref ro) = cfg.retention_override {
        tracing::warn!(
            columns_epochs = ro.columns_epochs,
            blocks_epochs = ro.blocks_epochs,
            "storage.retention_override active (devnet-only compressed retention)"
        );
    }
    if let Some(ref cp) = cfg.debug.crash_point
        && !cp.is_empty()
    {
        tracing::warn!(crash_point = %cp, "storage.debug.crash_point active (devnet-only)");
    }
    // CC-4Ca: §10.1 families into bs.registry between init and serve (Phase 0 seam).
    let storage_metrics = StorageMetrics::register(&mut bs.registry);

    // CC-44b: open store + spawn writer (process-fatal) + write-behind.
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    // Keep shutdown sender alive for process lifetime (SIGTERM path is serve's).
    let _shutdown_keep = _shutdown_tx;
    if cfg.enable_write_path {
        match open_store(&cfg) {
            Ok(store) => {
                let engine = Arc::new(store.into_engine());
                // Only a non-zero session_id is resume-valid (O1 / write_behind::is_resumable).
                let initial_cursor = load_write_cursor(&engine)
                    .ok()
                    .flatten()
                    .filter(write_behind::is_resumable);
                let faults = WriterFaults::default();
                let writer = spawn_writer(
                    Arc::clone(&engine),
                    storage_metrics.clone(),
                    cfg.writer_bounds(),
                    faults,
                    shutdown_rx.clone(),
                    true, // process-fatal on panic (§1.5); write-behind is not
                );
                // CC-41: split lock + migrator (FINALIZED_CHECKPOINT cadence).
                let split = Arc::new(
                    SplitLock::load(&engine).unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "split load failed; defaulting to zero");
                        SplitLock::new(cc_store::Split::default())
                    }),
                );
                let migrator = Arc::new(Migrator::new(
                    Arc::clone(&split),
                    Arc::clone(&engine),
                    writer.clone(),
                    cfg.migration_config(),
                    storage_metrics.clone(),
                ));
                // Write-behind: respawn-on-panic with backoff (counter-example to writer).
                let (_wb, _wb_respawns) = spawn_write_behind(
                    cfg.write_behind_config(),
                    writer,
                    storage_metrics.clone(),
                    initial_cursor,
                    shutdown_rx,
                    Some(migrator),
                );
                tracing::info!(
                    data_dir = %cfg.data_dir.display(),
                    commit_slots = cfg.commit_slots,
                    epochs_per_migration = cfg.epochs_per_migration,
                    "writer + write-behind + migrator spawned"
                );
                // Hold engine / split for process lifetime (writer holds Arc).
                std::mem::forget(engine);
                std::mem::forget(split);
            }
            Err(e) => {
                // Fail closed on open errors when write path is enabled.
                return Err(e);
            }
        }
    } else {
        tracing::warn!("enable_write_path=false — writer/write-behind not started");
    }

    let routes = Routes::default().add_service(StorageServiceServer::new(StorageStub));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
