//! Integration driver — beacon-API → chain `ImportBlock` pipe (CC-1A / §9).
//!
//! Populated in Phase 1; removed as a directory in CC-28. Dependency DAG is
//! strictly `{cc-proto, cc-config}` plus HTTP/tokio (ADR-P1-13). **Never**
//! depends on consensus types or crypto crates — SSZ is forwarded, never decoded.
//!
//! CC-1Aa: API client + catch-up. CC-1Ab: steady state (+4/+8/+11), walk-back,
//! 429 backoff and provider rotation.
//!
//! Offline unit tests use a stub beacon HTTP server + `MockImporter`. Live
//! binary path: real chain gRPC + configured beacon provider (HTTPS / loopback
//! HTTP only — SEC-1Aa-3).

mod api;
mod catchup;
mod ratelimit;
mod steady;
mod walkback;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bytes::Bytes;
use catchup::{
    CatchupConfig, ChainGrpcImporter, DEFAULT_PREFETCH_DEPTH, ImportResultCounts, run_catchup,
    unix_now_secs,
};
use cc_config::ServiceConfig;
use cc_proto::chain::ImportBlockVerdict;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use ratelimit::{DEFAULT_ROTATE_AFTER, ProviderPool, RateLimitMetrics};
use serde::Deserialize;
use steady::{SteadyConfig, fetch_genesis_time, run_steady_state};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use walkback::{DEFAULT_MAX_WALKBACK_SLOTS, DEFAULT_SLOTS_PER_EPOCH, WalkbackConfig};

use api::{map_consensus_version, validate_provider_base};

/// Process name and config slug (`config/driver.toml`, `CC_DRIVER_*`).
const SERVICE: &str = "driver";

/// Shared service fields plus driver-only config.
#[derive(Debug, Deserialize)]
struct DriverConfig {
    #[serde(flatten)]
    service: ServiceConfig,
    /// Ordered beacon-API provider base URLs.
    #[serde(default)]
    beacon_providers: Vec<String>,
    /// Default `Eth-Consensus-Version` when the block response omits the header.
    #[serde(default = "default_fork_name")]
    default_fork: String,
    /// Prefetch window depth (§9.2). Default 8.
    #[serde(default = "default_prefetch_depth")]
    prefetch_depth: usize,
    /// Seconds per slot for the steady-state clock (§9.3). Default 12.
    #[serde(default = "default_seconds_per_slot")]
    seconds_per_slot: u64,
    /// Slots per epoch (walk-back escalation). Default 32.
    #[serde(default = "default_slots_per_epoch")]
    slots_per_epoch: u64,
    /// First-attempt walk-back depth (§9.4). Default 64.
    #[serde(default = "default_max_walkback_slots")]
    max_walkback_slots: u64,
    /// Consecutive provider failures before rotation (§9.5). Default 3.
    #[serde(default = "default_rotate_after")]
    provider_rotate_after: u32,
}

fn default_fork_name() -> String {
    "fulu".to_owned()
}

fn default_prefetch_depth() -> usize {
    DEFAULT_PREFETCH_DEPTH
}

fn default_seconds_per_slot() -> u64 {
    steady::DEFAULT_SECONDS_PER_SLOT
}

fn default_slots_per_epoch() -> u64 {
    DEFAULT_SLOTS_PER_EPOCH
}

fn default_max_walkback_slots() -> u64 {
    DEFAULT_MAX_WALKBACK_SLOTS
}

fn default_rotate_after() -> u32 {
    DEFAULT_ROTATE_AFTER
}

/// Labels for `cc_driver_import_result_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ImportResultLabels {
    result: String,
}

/// Labels for `cc_driver_provider_errors_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ProviderErrorLabels {
    provider: String,
    code: String,
}

/// Driver metrics registered into the process registry.
#[derive(Debug, Clone)]
struct DriverMetrics {
    import_result_total: Family<ImportResultLabels, Counter>,
    /// Unix timestamp (seconds) when catch-up completed; 0 until then.
    catchup_complete_timestamp: Gauge,
    /// Gaps abandoned after walk-back exhausted both limits (§9.4).
    gap_abandoned_total: Counter,
    /// Provider HTTP/transport errors by base URL and status code.
    provider_errors_total: Family<ProviderErrorLabels, Counter>,
    /// Index of the active beacon provider (0-based).
    active_provider: Gauge,
}

impl DriverMetrics {
    fn register(registry: &mut Registry) -> Self {
        let import_result_total = Family::<ImportResultLabels, Counter>::default();
        for result in [
            "imported",
            "duplicate",
            "deferred",
            "unknown_parent",
            "invalid",
            "unspecified",
        ] {
            let _ = import_result_total
                .get_or_create(&ImportResultLabels {
                    result: result.to_owned(),
                })
                .get();
        }
        let catchup_complete_timestamp = Gauge::default();
        catchup_complete_timestamp.set(0);

        let gap_abandoned_total = Counter::default();
        let provider_errors_total = Family::<ProviderErrorLabels, Counter>::default();
        let active_provider = Gauge::default();
        active_provider.set(0);

        registry.register(
            "cc_driver_import_result",
            "ImportBlock verdicts observed by the driver (total)",
            import_result_total.clone(),
        );
        registry.register(
            "cc_driver_catchup_complete_timestamp",
            "Unix seconds when catch-up completed (CC-1C/3 boundary); 0 until complete",
            catchup_complete_timestamp.clone(),
        );
        registry.register(
            "cc_driver_gap_abandoned",
            "Walk-back gaps abandoned after depth limits (total)",
            gap_abandoned_total.clone(),
        );
        registry.register(
            "cc_driver_provider_errors",
            "Beacon provider errors by provider base and HTTP/status code (total)",
            provider_errors_total.clone(),
        );
        registry.register(
            "cc_driver_active_provider",
            "Index of the active beacon-API provider (0-based)",
            active_provider.clone(),
        );

        Self {
            import_result_total,
            catchup_complete_timestamp,
            gap_abandoned_total,
            provider_errors_total,
            active_provider,
        }
    }

    fn inc_result(&self, verdict: ImportBlockVerdict) {
        let result = match verdict {
            ImportBlockVerdict::Imported => "imported",
            ImportBlockVerdict::Duplicate => "duplicate",
            ImportBlockVerdict::DeferredDa => "deferred",
            ImportBlockVerdict::UnknownParent => "unknown_parent",
            ImportBlockVerdict::Invalid => "invalid",
            ImportBlockVerdict::Unspecified => "unspecified",
        };
        self.import_result_total
            .get_or_create(&ImportResultLabels {
                result: result.to_owned(),
            })
            .inc();
    }

    fn mark_catchup_complete(&self) {
        self.catchup_complete_timestamp.set(unix_now_secs());
    }

    fn inc_gap_abandoned(&self) {
        self.gap_abandoned_total.inc();
    }

    fn inc_provider_error(&self, provider: &str, code: u16) {
        self.provider_errors_total
            .get_or_create(&ProviderErrorLabels {
                provider: provider.to_owned(),
                code: code.to_string(),
            })
            .inc();
    }

    fn set_active_provider(&self, idx: u64) {
        self.active_provider.set(idx as i64);
    }

    fn unknown_parent_count(&self) -> u64 {
        self.import_result_total
            .get_or_create(&ImportResultLabels {
                result: "unknown_parent".to_owned(),
            })
            .get()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = cc_config::load::<DriverConfig>(SERVICE)?;
    install_tracing(&cfg.service.log_filter, &cfg.service.log_format)?;

    let mut registry = Registry::default();
    let metrics = DriverMetrics::register(&mut registry);
    let registry = Arc::new(registry);

    // Metrics exposition (no cc-bootstrap — DAG forbids it).
    let metrics_addr = cfg.service.metrics_addr;
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr, registry).await {
            error!(error = %e, "metrics server exited");
        }
    });

    let chain_uri = cfg
        .service
        .peers
        .get("chain")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing peers.chain (set in config/driver.toml or CC_DRIVER_PEERS__CHAIN)"
            )
        })?
        .to_string();

    if cfg.beacon_providers.is_empty() {
        anyhow::bail!(
            "beacon_providers is empty (set at least one URL in config/driver.toml \
             or CC_DRIVER_BEACON_PROVIDERS)"
        );
    }
    // Fail closed on misconfigured bases before any bind/work (SEC-1Aa-3).
    for base in &cfg.beacon_providers {
        validate_provider_base(base).map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    let default_fork = map_consensus_version(&cfg.default_fork).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown default_fork {:?}: expected phase0|altair|…|fulu",
            cfg.default_fork
        )
    })?;

    let prefetch_depth = if cfg.prefetch_depth == 0 {
        DEFAULT_PREFETCH_DEPTH
    } else {
        cfg.prefetch_depth
    };

    let rotate_after = if cfg.provider_rotate_after == 0 {
        DEFAULT_ROTATE_AFTER
    } else {
        cfg.provider_rotate_after
    };

    info!(
        %chain_uri,
        providers = ?cfg.beacon_providers,
        default_fork,
        prefetch_depth,
        seconds_per_slot = cfg.seconds_per_slot,
        max_walkback_slots = cfg.max_walkback_slots,
        metrics = %metrics_addr,
        "driver starting (CC-1A catch-up + steady)"
    );

    // Provider pool for steady / walk-back / politeness; catch-up uses index 0.
    let metrics_err = metrics.clone();
    let metrics_active = metrics.clone();
    let rate_metrics = RateLimitMetrics {
        on_error: Some(Arc::new(move |provider, code| {
            metrics_err.inc_provider_error(provider, code);
        })),
        on_active: Some(Arc::new(move |idx| {
            metrics_active.set_active_provider(idx);
        })),
        request_attempts: Arc::new(AtomicU64::new(0)),
    };
    let mut pool = ProviderPool::new(
        &cfg.beacon_providers,
        default_fork,
        rotate_after,
        rate_metrics,
    )
    .map_err(|e| anyhow::anyhow!("provider pool: {e}"))?;

    // Catch-up still uses a dedicated client on the first provider (CC-1Aa path).
    let api = api::BeaconApiClient::new(cfg.beacon_providers[0].clone(), default_fork)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut importer = ChainGrpcImporter::connect(&chain_uri).await?;

    // Wait for chain checkpoint bootstrap; head_slot at that moment is the anchor.
    info!("waiting for chain bootstrap (GetHead)");
    let (anchor_slot, anchor_root) = importer
        .wait_bootstrapped(Duration::from_secs(600))
        .await
        .map_err(|e| anyhow::anyhow!("chain not bootstrapped: {e}"))?;
    info!(
        anchor_slot,
        anchor_root = %hex_root(&anchor_root),
        "chain bootstrapped; beginning catch-up"
    );

    let metrics_cb = metrics.clone();
    let on_result: catchup::OnImportResult = Arc::new(move |v| metrics_cb.inc_result(v));

    let (report, counts) = run_catchup(
        &api,
        &mut importer,
        CatchupConfig {
            anchor_slot,
            prefetch_depth,
        },
        Some(Arc::clone(&on_result)),
        None,
    )
    .await
    .map_err(|e| anyhow::anyhow!("catch-up failed: {e}"))?;

    metrics.mark_catchup_complete();

    info!(
        anchor_slot = report.anchor_slot,
        head_slot = report.head_slot_reached,
        blocks = report.blocks_imported,
        empty_skipped = report.empty_slots_skipped,
        unknown_parent = counts.unknown_parent,
        metric_unknown_parent = metrics.unknown_parent_count(),
        elapsed_ms = report.elapsed.as_millis() as u64,
        catchup_ts = metrics.catchup_complete_timestamp.get(),
        "CC-1A/1 catch-up finished"
    );

    if counts.unknown_parent != 0 {
        warn!(
            unknown_parent = counts.unknown_parent,
            "catch-up completed with non-zero unknown_parent (CC-1A/1 failed)"
        );
    }

    // ── Steady state (CC-1Ab) ────────────────────────────────────────────
    let genesis_time = fetch_genesis_time(&mut pool)
        .await
        .map_err(|e| anyhow::anyhow!("genesis fetch: {e}"))?;
    info!(genesis_time, "genesis_time loaded for slot clock");

    let metrics_gap = metrics.clone();
    let on_gap: steady::OnGapAbandoned = Arc::new(move || {
        metrics_gap.inc_gap_abandoned();
    });

    let mut steady_counts = ImportResultCounts::default();
    let steady_cfg = SteadyConfig {
        genesis_time,
        seconds_per_slot: cfg.seconds_per_slot.max(1),
        poll_offsets_secs: steady::DEFAULT_POLL_OFFSETS_SECS.to_vec(),
        walkback: WalkbackConfig {
            max_walkback_slots: cfg.max_walkback_slots.max(1),
            slots_per_epoch: cfg.slots_per_epoch.max(1),
        },
        max_slots: None,
        deadline: None,
    };

    // Run steady until ctrl-c. Spawn the loop and select on signal so metrics
    // remain scrapable and shutdown is clean.
    info!(
        active_provider = pool.active_base(),
        "entering steady state (+4/+8/+11 polls)"
    );

    tokio::select! {
        result = run_steady_state(
            &mut pool,
            &mut importer,
            steady_cfg,
            &mut steady_counts,
            Some(on_result),
            Some(on_gap),
            None,
            None,
            None,
        ) => {
            match result {
                Ok(rep) => {
                    info!(
                        slots = rep.slots.len(),
                        in_slot_ratio = rep.in_slot_ratio(),
                        misses = ?rep.miss_list(),
                        abandoned = rep.gaps_abandoned,
                        "steady state exited"
                    );
                }
                Err(e) => {
                    error!(error = %e, "steady state failed");
                    return Err(anyhow::anyhow!("steady state: {e}"));
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
        }
    }

    Ok(())
}

fn install_tracing(filter: &str, format: &str) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry().with(env_filter);
    match format {
        "pretty" => {
            registry
                .with(tracing_subscriber::fmt::layer().pretty())
                .try_init()
                .map_err(|e| anyhow::anyhow!("tracing init: {e}"))?;
        }
        _ => {
            registry
                .with(tracing_subscriber::fmt::layer().json())
                .try_init()
                .map_err(|e| anyhow::anyhow!("tracing init: {e}"))?;
        }
    }
    Ok(())
}

fn hex_root(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Minimal Prometheus text exposition (mirrors `cc-bootstrap` metrics_server
/// without taking that workspace edge).
async fn serve_metrics(addr: SocketAddr, registry: Arc<Registry>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "metrics listening");
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let registry = Arc::clone(&registry);
                async move { handle_metrics(req, registry).await }
            });
            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!(error = %err, "metrics connection closed");
            }
        });
    }
}

async fn handle_metrics(
    req: Request<Incoming>,
    registry: Arc<Registry>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    if req.method() == Method::GET && req.uri().path() == "/metrics" {
        let mut buf = String::new();
        if encode(&mut buf, &registry).is_err() {
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(full(Bytes::from_static(b"encode error")))
                .unwrap_or_else(|_| Response::new(full(Bytes::new()))));
        }
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(
                hyper::header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .body(full(Bytes::from(buf)))
            .unwrap_or_else(|_| Response::new(full(Bytes::new()))))
    } else {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(full(Bytes::from_static(b"not found")))
            .unwrap_or_else(|_| Response::new(empty())))
    }
}

fn full(body: Bytes) -> BoxBody<Bytes, Infallible> {
    Full::new(body).map_err(|never| match never {}).boxed()
}

fn empty() -> BoxBody<Bytes, Infallible> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
