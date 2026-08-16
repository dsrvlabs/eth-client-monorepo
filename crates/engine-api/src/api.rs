//! In-process Engine API host (S1-A-06).
//!
//! Production construction is fail-before-bind: JWT load, sandboxed
//! `network_config`, [`crate::config::EngineTransportConfig::require_el_fork_schedule`],
//! and [`crate::fastpath::production_cell_kzg`] all run before any listener.
//!
//! E3 is a direct call from the consensus-core OS thread with an explicit
//! [`Duration`]. The EL's HTTP timeouts stay in [`crate::config::TransportTimeouts`];
//! the argument is the caller-side cap so a black-holed EL cannot park the core.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::capabilities::CapabilityCache;
use crate::config::EngineTransportConfig;
use crate::errors::EngineError;
use crate::fastpath::fetch::BlobBound;
use crate::fastpath::filter::SubscriptionSet;
use crate::fastpath::sidecars::SidecarTemplate;
use crate::fastpath::{FastpathLane, production_cell_kzg};
use crate::jwt::JwtSecret;
use crate::methods::fcu::{FcuGatedError, FcuSequenceGate, forkchoice_updated_v3_gated};
use crate::methods::get_blobs::NullContext;
use crate::methods::new_payload::{DecodedPayloadStatus, new_payload_v4};
use crate::metrics::EngineMetrics;
use crate::network_config::{NetworkConfigError, load_network_chain_config};
use crate::state::{
    CachedForkchoiceState, EngineStateHandle, UpcheckOutcome, spawn_upcheck_driver,
};
use crate::transport::{EngineTransport, SharedTransport};
use crate::version::ElForkSchedule;

/// Fail-before-bind construction error (kind-only; no JWT path/body).
#[derive(Debug)]
pub enum EngineBuildError {
    /// JWT secret failed to load (abort before bind).
    Jwt(String),
    /// Sandboxed `network_config` load failed.
    NetworkConfig(NetworkConfigError),
    /// Missing `[el_forks]` (P2-D/19) or other fork-schedule refusal.
    ForkSchedule(&'static str),
    /// Trusted-setup / `production_cell_kzg` failure (P1-A/25).
    Kzg(String),
    /// Blob-count bound from the loaded chain config.
    BlobBound(&'static str),
    /// HTTP client / transport construction.
    Transport(EngineError),
    /// No tokio runtime to drive EL calls from the core OS thread.
    Runtime(String),
}

impl std::fmt::Display for EngineBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Jwt(s) => write!(f, "JWT secret: {s}"),
            Self::NetworkConfig(e) => write!(f, "{e}"),
            Self::ForkSchedule(s) => write!(f, "{s}"),
            Self::Kzg(s) => write!(f, "KZG trusted setup: {s}"),
            Self::BlobBound(s) => write!(f, "network_config blob bound: {s}"),
            Self::Transport(e) => write!(f, "{e}"),
            Self::Runtime(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for EngineBuildError {}

/// JWT + YAML + fork schedule + KZG, loaded before any port bind.
pub struct PreparedEngine {
    jwt_bytes: [u8; 32],
    cfg: EngineTransportConfig,
    chain: cc_types::ChainConfig,
    kzg: Arc<dyn cc_crypto::CellKzg>,
    schedule: ElForkSchedule,
}

impl std::fmt::Debug for PreparedEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedEngine").finish_non_exhaustive()
    }
}

/// In-process engine host: transport, health machine, fastpath, Duration-capped calls.
#[derive(Clone)]
pub struct EngineApi {
    transport: SharedTransport,
    schedule: ElForkSchedule,
    metrics: Option<EngineMetrics>,
    state: EngineStateHandle,
    fastpath: FastpathLane,
    fcu_gate: Arc<FcuSequenceGate>,
    /// Captured at construction (tokio main). Drives EL futures from `chain-core`.
    runtime: tokio::runtime::Handle,
    /// Keep the detached upcheck + fastpath worker alive for the process lifetime.
    _upcheck: Arc<tokio::task::JoinHandle<()>>,
    _fastpath_worker: Arc<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for EngineApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineApi").finish_non_exhaustive()
    }
}

impl EngineApi {
    /// Fail-before-bind: JWT, sandboxed `network_config`, `[el_forks]`, KZG.
    ///
    /// Must run before `cc_bootstrap::init` / any listener. `JwtSecret` stays
    /// crate-private (ADR-R-03).
    pub fn prepare(
        cfg: &EngineTransportConfig,
        network_config: &Path,
    ) -> Result<PreparedEngine, EngineBuildError> {
        let chain = load_network_chain_config(network_config, Some(cfg.jwt_secret_path.as_path()))
            .map_err(EngineBuildError::NetworkConfig)?;
        let jwt = JwtSecret::load(&cfg.jwt_secret_path)
            .map_err(|e| EngineBuildError::Jwt(e.to_string()))?;
        let schedule = cfg
            .require_el_fork_schedule()
            .map_err(EngineBuildError::ForkSchedule)?;
        // Abort before serve if the committed trusted setup cannot load (P1-A/25).
        let kzg = production_cell_kzg().map_err(|e| EngineBuildError::Kzg(e.to_string()))?;
        Ok(PreparedEngine {
            jwt_bytes: jwt.as_bytes(),
            cfg: cfg.clone(),
            chain,
            kzg,
            schedule,
        })
    }

    /// Construct from an already-loaded [`cc_types::ChainConfig`] (chain host).
    ///
    /// JWT + `[el_forks]` + KZG still run fail-before-bind. Blob bound comes
    /// from `chain` (`BlobBound::from_chain_config`) — not a test fixture.
    pub fn prepare_with_chain_config(
        cfg: &EngineTransportConfig,
        chain: cc_types::ChainConfig,
    ) -> Result<PreparedEngine, EngineBuildError> {
        let jwt = JwtSecret::load(&cfg.jwt_secret_path)
            .map_err(|e| EngineBuildError::Jwt(e.to_string()))?;
        let schedule = cfg
            .require_el_fork_schedule()
            .map_err(EngineBuildError::ForkSchedule)?;
        let kzg = production_cell_kzg().map_err(|e| EngineBuildError::Kzg(e.to_string()))?;
        Ok(PreparedEngine {
            jwt_bytes: jwt.as_bytes(),
            cfg: cfg.clone(),
            chain,
            kzg,
            schedule,
        })
    }
}

impl PreparedEngine {
    /// Wire transport, state machine, and fastpath. Call after metrics register.
    pub fn finish(self, metrics: Option<EngineMetrics>) -> Result<EngineApi, EngineBuildError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|e| {
            EngineBuildError::Runtime(format!("no tokio runtime for engine host: {e}"))
        })?;
        let transport =
            EngineTransport::from_config_secret_bytes(&self.cfg, self.jwt_bytes, metrics.clone())
                .map_err(EngineBuildError::Transport)?;
        let transport = Arc::new(transport);

        let slot_duration = Duration::from_millis(self.cfg.slot_duration_ms.max(1));
        let state = EngineStateHandle::new(
            Arc::new(CapabilityCache::new()),
            metrics.clone(),
            slot_duration,
        );
        let upcheck = spawn_upcheck_driver(
            state.clone(),
            Arc::clone(&transport),
            metrics.clone(),
            self.schedule.clone(),
            slot_duration,
        );

        let bound =
            BlobBound::from_chain_config(&self.chain).map_err(EngineBuildError::BlobBound)?;
        // Positional 5th arg after tracker `None` is the named `kzg` binding.
        let kzg = Some(self.kzg);
        let lane = FastpathLane::new(
            Arc::clone(&transport),
            metrics.clone(),
            bound,
            None,
            kzg,
            SubscriptionSet::empty(),
        );
        let fastpath_worker = lane.spawn_worker();

        Ok(EngineApi {
            transport,
            schedule: self.schedule,
            metrics,
            state,
            fastpath: lane,
            fcu_gate: Arc::new(FcuSequenceGate::new()),
            runtime,
            _upcheck: Arc::new(upcheck),
            _fastpath_worker: Arc::new(fastpath_worker),
        })
    }
}

impl EngineApi {
    /// Harness constructor: caller supplies transport, state, and fastpath.
    ///
    /// Production uses [`Self::prepare`] / [`PreparedEngine::finish`].
    pub fn from_parts(
        transport: SharedTransport,
        schedule: ElForkSchedule,
        runtime: tokio::runtime::Handle,
        metrics: Option<EngineMetrics>,
        state: EngineStateHandle,
        fastpath: FastpathLane,
    ) -> Self {
        let slot_duration = Duration::from_millis(1_000);
        let upcheck = spawn_upcheck_driver(
            state.clone(),
            Arc::clone(&transport),
            metrics.clone(),
            schedule.clone(),
            slot_duration,
        );
        let worker = fastpath.spawn_worker();
        Self {
            transport,
            schedule,
            metrics,
            state,
            fastpath,
            fcu_gate: Arc::new(FcuSequenceGate::new()),
            runtime,
            _upcheck: Arc::new(upcheck),
            _fastpath_worker: Arc::new(worker),
        }
    }

    /// Shared transport (gRPC adapter / tests).
    #[must_use]
    pub fn transport(&self) -> SharedTransport {
        Arc::clone(&self.transport)
    }

    /// EL fork schedule used by the version gate.
    #[must_use]
    pub fn schedule(&self) -> &ElForkSchedule {
        &self.schedule
    }

    /// Four-state machine (GetEngineState / admit).
    #[must_use]
    pub fn state(&self) -> &EngineStateHandle {
        &self.state
    }

    /// Fastpath lane (FetchBlobs / block-branch).
    #[must_use]
    pub fn fastpath(&self) -> &FastpathLane {
        &self.fastpath
    }

    /// fcU sequence high-water.
    #[must_use]
    pub fn fcu_gate(&self) -> Arc<FcuSequenceGate> {
        Arc::clone(&self.fcu_gate)
    }

    /// Drive `fut` for at most `timeout`. Missing `Duration` is a compile error.
    fn call_with_deadline<F, T>(
        &self,
        timeout: Duration,
        method: &'static str,
        fut: F,
    ) -> Result<T, EngineError>
    where
        F: std::future::Future<Output = Result<T, EngineError>>,
    {
        match self
            .runtime
            .block_on(async move { tokio::time::timeout(timeout, fut).await })
        {
            Ok(inner) => inner,
            Err(_) => Err(EngineError::Timeout {
                method: method.to_owned(),
            }),
        }
    }

    /// `engine_newPayloadV4` from the core OS thread.
    pub fn new_payload(
        &self,
        ssz: &[u8],
        versioned_hashes: &[Vec<u8>],
        parent_beacon_block_root: &[u8],
        execution_requests: &[Vec<u8>],
        timeout: Duration,
    ) -> Result<DecodedPayloadStatus, EngineError> {
        self.runtime.block_on(self.ensure_el_admitted())?;
        self.call_with_deadline(timeout, "engine_newPayloadV4", async {
            match new_payload_v4(
                self.transport.as_ref(),
                &self.schedule,
                self.metrics.as_ref(),
                ssz,
                versioned_hashes,
                parent_beacon_block_root,
                execution_requests,
            )
            .await
            {
                Ok(status) => Ok(status),
                Err(e) => {
                    self.note_ordered_lane_error(&e).await;
                    Err(e)
                }
            }
        })
    }

    /// Async `newPayload` for the leftover engine gRPC adapter (4-container A/B).
    pub async fn new_payload_async(
        &self,
        ssz: &[u8],
        versioned_hashes: &[Vec<u8>],
        parent_beacon_block_root: &[u8],
        execution_requests: &[Vec<u8>],
    ) -> Result<DecodedPayloadStatus, EngineError> {
        self.ensure_el_admitted().await?;
        match new_payload_v4(
            self.transport.as_ref(),
            &self.schedule,
            self.metrics.as_ref(),
            ssz,
            versioned_hashes,
            parent_beacon_block_root,
            execution_requests,
        )
        .await
        {
            Ok(status) => Ok(status),
            Err(e) => {
                self.note_ordered_lane_error(&e).await;
                Err(e)
            }
        }
    }

    /// `engine_forkchoiceUpdatedV3` from the core OS thread.
    #[allow(clippy::too_many_arguments)]
    pub fn forkchoice_updated(
        &self,
        sequence: u64,
        session_id: u64,
        head_block_hash: &[u8],
        safe_block_hash: &[u8],
        finalized_block_hash: &[u8],
        head_slot: Option<u64>,
        timeout: Duration,
    ) -> Result<DecodedPayloadStatus, FcuGatedError> {
        self.runtime
            .block_on(self.ensure_el_admitted())
            .map_err(FcuGatedError::Engine)?;
        match self.runtime.block_on(async {
            tokio::time::timeout(
                timeout,
                self.forkchoice_updated_async(
                    sequence,
                    session_id,
                    head_block_hash,
                    safe_block_hash,
                    finalized_block_hash,
                    head_slot,
                ),
            )
            .await
        }) {
            Ok(inner) => inner,
            Err(_) => Err(FcuGatedError::Engine(EngineError::Timeout {
                method: "engine_forkchoiceUpdatedV3".into(),
            })),
        }
    }

    /// Async fcU for the leftover engine gRPC adapter.
    #[allow(clippy::too_many_arguments)]
    pub async fn forkchoice_updated_async(
        &self,
        sequence: u64,
        session_id: u64,
        head_block_hash: &[u8],
        safe_block_hash: &[u8],
        finalized_block_hash: &[u8],
        head_slot: Option<u64>,
    ) -> Result<DecodedPayloadStatus, FcuGatedError> {
        self.ensure_el_admitted()
            .await
            .map_err(FcuGatedError::Engine)?;
        match forkchoice_updated_v3_gated(
            self.transport.as_ref(),
            self.fcu_gate.as_ref(),
            &self.schedule,
            self.metrics.as_ref(),
            sequence,
            session_id,
            head_block_hash,
            safe_block_hash,
            finalized_block_hash,
            head_slot,
        )
        .await
        {
            Ok(status) => {
                if let (Ok(head), Ok(safe), Ok(finalized)) = (
                    as_32(head_block_hash),
                    as_32(safe_block_hash),
                    as_32(finalized_block_hash),
                ) {
                    self.state
                        .cache_forkchoice(CachedForkchoiceState {
                            head_block_hash: head,
                            safe_block_hash: safe,
                            finalized_block_hash: finalized,
                        })
                        .await;
                }
                Ok(status)
            }
            Err(e) => {
                if let FcuGatedError::Engine(ref err) = e {
                    self.note_ordered_lane_error(err).await;
                }
                Err(e)
            }
        }
    }

    /// Local health snapshot (`el_offline`, internal state).
    pub fn engine_state(&self, timeout: Duration) -> Result<(bool, String), EngineError> {
        self.call_with_deadline(timeout, "GetEngineState", async {
            Ok(self.state.get_engine_state_fields().await)
        })
    }

    /// `true` when the local machine reports Online.
    #[must_use]
    pub fn is_online(&self, timeout: Duration) -> bool {
        self.engine_state(timeout)
            .map(|(el_offline, _)| !el_offline)
            .unwrap_or(false)
    }

    /// Enqueue block-branch `getBlobs` (accelerator; never cells).
    pub fn fetch_blobs(
        &self,
        template: SidecarTemplate,
        root: [u8; 32],
        slot: u64,
        timeout: Duration,
    ) {
        let _ = self.call_with_deadline(timeout, "engine_getBlobsV2", async {
            let _ = self
                .fastpath
                .trigger_from_block_with_template(root, slot, template, NullContext::PrunedPool)
                .await;
            Ok(())
        });
    }

    /// Async FetchBlobs enqueue for the leftover gRPC adapter.
    pub async fn fetch_blobs_async(&self, template: SidecarTemplate, root: [u8; 32], slot: u64) {
        let _ = self
            .fastpath
            .trigger_from_block_with_template(root, slot, template, NullContext::PrunedPool)
            .await;
    }

    async fn ensure_el_admitted(&self) -> Result<(), EngineError> {
        if self.state.admits_el_call().await {
            return Ok(());
        }
        let (el_offline, internal) = self.state.get_engine_state_fields().await;
        Err(EngineError::Transport {
            detail: format!(
                "execution engine unavailable (el_offline={el_offline}, state={internal})"
            ),
        })
    }

    async fn note_ordered_lane_error(&self, err: &EngineError) {
        if let EngineError::Http401 { body } | EngineError::Http403 { body } = err {
            let _ = self
                .state
                .apply(UpcheckOutcome::AuthRejected { body: body.clone() })
                .await;
        }
    }
}

fn as_32(bytes: &[u8]) -> Result<[u8; 32], ()> {
    <[u8; 32]>::try_from(bytes).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::{ElForksConfig, EngineTransportConfig};

    #[test]
    fn missing_el_forks_does_not_invent_osaka_at_genesis() {
        let cfg = EngineTransportConfig::default();
        assert!(cfg.el_fork_schedule().is_none());
        let err = cfg.require_el_fork_schedule().unwrap_err();
        assert!(
            err.contains("el_forks") && err.contains("osaka_time=0"),
            "constructor must refuse the fail-open default: {err}"
        );
    }

    #[test]
    fn explicit_el_forks_is_required_for_prepare() {
        let cfg = EngineTransportConfig {
            el_forks: Some(ElForksConfig {
                osaka_time: 1_761_677_592,
                bpo1_time: None,
                bpo2_time: None,
                amsterdam_time: None,
            }),
            ..EngineTransportConfig::default()
        };
        assert!(cfg.require_el_fork_schedule().is_ok());
    }

    /// Test / harness constructor (no JWT file, no trusted setup).
    impl EngineApi {
        pub fn from_transport(
            transport: SharedTransport,
            schedule: ElForkSchedule,
            runtime: tokio::runtime::Handle,
            metrics: Option<EngineMetrics>,
            state: Option<EngineStateHandle>,
            fastpath: Option<FastpathLane>,
        ) -> Self {
            let slot_duration = Duration::from_millis(1_000);
            let state = state.unwrap_or_else(|| {
                EngineStateHandle::new(
                    Arc::new(CapabilityCache::new()),
                    metrics.clone(),
                    slot_duration,
                )
            });
            let fastpath = fastpath.unwrap_or_else(|| {
                let cfg = cc_types::ChainConfig::from_yaml_str(include_str!(
                    "../../types/tests/fixtures/hoodi-config.yaml"
                ))
                .expect("hoodi fixture");
                let bound = BlobBound::from_chain_config(&cfg).expect("hoodi fixture bound");
                FastpathLane::new(
                    Arc::clone(&transport),
                    metrics.clone(),
                    bound,
                    None,
                    None,
                    SubscriptionSet::empty(),
                )
            });
            let upcheck = spawn_upcheck_driver(
                state.clone(),
                Arc::clone(&transport),
                metrics.clone(),
                schedule.clone(),
                slot_duration,
            );
            let worker = fastpath.spawn_worker();
            Self {
                transport,
                schedule,
                metrics,
                state,
                fastpath,
                fcu_gate: Arc::new(FcuSequenceGate::new()),
                runtime,
                _upcheck: Arc::new(upcheck),
                _fastpath_worker: Arc::new(worker),
            }
        }
    }

    #[test]
    fn production_constructor_wires_kzg_and_blob_bound() {
        // Source-shape pin: finish() must keep the landed production wiring.
        let src = include_str!("api.rs");
        let production = src.split("#[cfg(test)]").next().expect("production half");
        let compact: String = production.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            compact.contains("production_cell_kzg()"),
            "production constructor must load CellKzg via production_cell_kzg"
        );
        assert!(
            compact.contains("letkzg=Some(self.kzg)") || compact.contains("letkzg=Some("),
            "production FastpathLane kzg argument must be Some(...)"
        );
        assert!(
            !compact.contains("kzg:None") && !compact.contains("kzg,None,SubscriptionSet"),
            "production must not pass kzg: None"
        );
        assert!(
            compact.contains("bound,None,kzg,SubscriptionSet::empty()"),
            "production FastpathLane::new must pass the Some-wrapped kzg binding"
        );
        assert!(
            compact.contains("load_network_chain_config"),
            "production constructor must sandbox-load ChainConfig from network_config"
        );
        assert!(
            compact.contains("BlobBound::from_chain_config(&self.chain)")
                || compact.contains("BlobBound::from_chain_config(&chain)"),
            "production FastpathLane bound must come from ChainConfig"
        );
        assert!(
            compact.contains("require_el_fork_schedule()"),
            "production must refuse a missing [el_forks] table"
        );
    }
}
