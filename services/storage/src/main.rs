//! `storage` service stub — Architecture §4.1, CC-01b / CC-4Ca.
//!
//! Phase 0 surface: health + reflection + `GetInfo`. Real RPCs land in Phase 4+.
//! Health peer: `chain` (§6.3).
//!
//! CC-4Ca: §10.1 metric families are registered between `init` and `serve`.
//! This file is append-only thereafter (one task-spawn append per issue).

mod metrics;

use cc_bootstrap::{PeerSpec, ServiceSpec, TelemetrySettings};
use cc_config::ServiceConfig;
use cc_proto::common::BuildInfo;
use cc_proto::storage::storage_service_server::{StorageService, StorageServiceServer};
use cc_proto::storage::{GetInfoRequest, GetInfoResponse};
use metrics::StorageMetrics;
use serde::Deserialize;
use tonic::service::Routes;
use tonic::{Request, Response, Status};

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
    ///
    /// Read when `Store::open` is wired into this process (still a Phase 0 stub).
    #[serde(default = "default_check_invariants")]
    #[allow(dead_code)]
    check_invariants: bool,
    /// Network identity for the CC-4D dangerous-knob guard.
    ///
    /// Required when [`Self::retention_override`] or [`StorageDebug::crash_point`]
    /// is set; must be neither Hoodi's nor mainnet's.
    #[serde(default)]
    genesis_validators_root: Option<String>,
    /// Compressed-retention venue override (CC-4D / Architecture §10.4).
    ///
    /// Devnet-only; refused at startup on Hoodi/mainnet GVR.
    #[serde(default)]
    retention_override: Option<RetentionOverride>,
    /// Fault-injection knobs (CC-4D / Architecture §2.5). Config, not env.
    #[serde(default)]
    debug: StorageDebug,
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
    // Prefer explicit TOML; this default is the local-dev / devnet polarity.
    true
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
    if let Some(ref cp) = cfg.debug.crash_point {
        if !cp.is_empty() {
            tracing::warn!(crash_point = %cp, "storage.debug.crash_point active (devnet-only)");
        }
    }
    // CC-4Ca: §10.1 families into bs.registry between init and serve (Phase 0 seam).
    let _storage_metrics = StorageMetrics::register(&mut bs.registry);
    let routes = Routes::default().add_service(StorageServiceServer::new(StorageStub));
    cc_bootstrap::serve(bs, cfg.service_spec(), routes).await?;
    Ok(())
}
