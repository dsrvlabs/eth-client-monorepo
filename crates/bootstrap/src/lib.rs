//! Shared service runtime — telemetry half (Architecture §4.1, §4.3, §4.4; CC-05a).
//!
//! Phase 1 of 2: `init` installs tracing, builds the metric registry, and returns
//! a [`Bootstrap`] whose `registry` services can extend before CC-05b's `serve`.
//!
//! **D-2:** `init` takes resolved [`TelemetrySettings`]; it never reads the
//! environment. `RUST_LOG` / `LOG_FORMAT` are resolved by `cc-config`.

#![allow(missing_docs)]

mod error;
mod grpc_metrics;
mod metrics;
mod metrics_server;
mod process;

pub use error::Error;
pub use grpc_metrics::{GrpcMetricsLayer, GrpcMetricsService};
pub use metrics::{
    BuildInfoLabels, GRPC_DURATION_BUCKETS, GrpcMethodLabels, GrpcRequestLabels, Metrics,
    PeerHealthLabels,
};
pub use metrics_server::{serve_metrics, spawn_metrics_server};

use std::collections::HashSet;
use std::future::Future;
use std::sync::OnceLock;

use prometheus_client::registry::Registry;
use tracing::{Instrument, Span, info_span};
use tracing_subscriber::Registry as TracingRegistry;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Short git SHA at build time (from `build.rs`; env-var-first for Docker).
pub const GIT_SHA: &str = env!("CC_GIT_SHA");

/// `rustc --version` string at build time.
pub const RUSTC: &str = env!("CC_RUSTC");

/// Process package version, matching `eth.common.v1.BuildInfo.version`.
///
/// Workspace members share `[workspace.package] version`, so this equals each
/// service binary's `CARGO_PKG_VERSION`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Root `service` span held for process lifetime; `spawn` instruments with it.
static ROOT_SPAN: OnceLock<Span> = OnceLock::new();

/// Resolved telemetry settings from `cc-config` (D-2).
///
/// Construct from [`cc_config::ServiceConfig`] via [`TelemetrySettings::from`]
/// or [`TelemetrySettings::from_config`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySettings {
    /// `"json"` (default) or `"pretty"`.
    pub log_format: String,
    /// `tracing_subscriber::EnvFilter` directive string (e.g. `"info"`).
    pub log_filter: String,
}

impl TelemetrySettings {
    /// Borrow the telemetry fields from a loaded [`cc_config::ServiceConfig`].
    pub fn from_config(cfg: &cc_config::ServiceConfig) -> Self {
        Self {
            log_format: cfg.log_format.clone(),
            log_filter: cfg.log_filter.clone(),
        }
    }
}

impl From<&cc_config::ServiceConfig> for TelemetrySettings {
    fn from(cfg: &cc_config::ServiceConfig) -> Self {
        Self::from_config(cfg)
    }
}

impl From<cc_config::ServiceConfig> for TelemetrySettings {
    fn from(cfg: cc_config::ServiceConfig) -> Self {
        Self::from_config(&cfg)
    }
}

/// Result of phase-1 bootstrap: tracing is live and the metric registry is ready.
///
/// Services register Phase 1+ metrics into [`Bootstrap::registry`] before
/// `serve` (CC-05b). The gRPC metrics layer is available via
/// [`Bootstrap::grpc_metrics_layer`].
#[derive(Debug)]
pub struct Bootstrap {
    /// Services register their own Phase 1+ metrics here before `serve`.
    pub registry: Registry,
    /// Process name (`"chain"`, …).
    service: &'static str,
    /// Shared metric handles (gRPC layer, peer prober).
    metrics: Metrics,
    /// Root span clone (also in [`ROOT_SPAN`]).
    root_span: Span,
}

impl Bootstrap {
    /// Process name passed to [`init`].
    pub fn service(&self) -> &'static str {
        self.service
    }

    /// Shared metric families (for the prober and ad-hoc recording).
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Root `service` span — instrument long-lived futures that cannot use
    /// [`spawn`] (e.g. the main `serve` task in CC-05b).
    pub fn root_span(&self) -> &Span {
        &self.root_span
    }

    /// Build the gRPC metrics tower layer for the known route set.
    ///
    /// `known_methods` is the closed set of full gRPC paths
    /// (e.g. `"/eth.chain.v1.ChainService/GetInfo"`). Anything else is recorded
    /// as `method="unknown"`.
    pub fn grpc_metrics_layer(
        &self,
        known_methods: impl IntoIterator<Item = String>,
    ) -> GrpcMetricsLayer {
        GrpcMetricsLayer::new(self.metrics.clone(), self.service, known_methods)
    }

    /// Convenience: known methods from `&str` paths.
    pub fn grpc_metrics_layer_from(
        &self,
        known_methods: impl IntoIterator<Item = impl Into<String>>,
    ) -> GrpcMetricsLayer {
        self.grpc_metrics_layer(
            known_methods
                .into_iter()
                .map(Into::into)
                .collect::<HashSet<_>>(),
        )
    }
}

/// Install the global tracing subscriber and build the metric registry.
///
/// **D-2:** `telemetry` is already resolved by `cc-config`; this function does
/// not read process environment variables.
///
/// Opens a root `info_span!("service", service = …)` held for process lifetime.
/// Use [`spawn`] so tasks inherit it.
pub fn init(service: &'static str, telemetry: TelemetrySettings) -> Result<Bootstrap, Error> {
    install_tracing(&telemetry)?;

    let root_span = info_span!("service", service);
    // Best-effort store for `spawn`. Ignore if a prior init already set it
    // (should not happen in production; tests may re-enter carefully).
    let _ = ROOT_SPAN.set(root_span.clone());

    let mut registry = Registry::default();
    let metrics = Metrics::register(&mut registry, service, VERSION, GIT_SHA, RUSTC);

    Ok(Bootstrap {
        registry,
        service,
        metrics,
        root_span,
    })
}

/// Spawn a Tokio task that inherits the service's root tracing span.
///
/// **This is why `spawn` is public** — without it, log correlation dies at the
/// first `tokio::spawn` in Phase 1.
pub fn spawn<F>(name: &'static str, fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let root = ROOT_SPAN
        .get()
        .cloned()
        .unwrap_or_else(|| info_span!("service", service = "unknown"));
    let task_span = tracing::info_span!(parent: root, "task", name);
    tokio::spawn(fut.instrument(task_span))
}

fn install_tracing(telemetry: &TelemetrySettings) -> Result<(), Error> {
    let filter = EnvFilter::try_new(telemetry.log_filter.as_str())?;

    match telemetry.log_format.as_str() {
        "json" => {
            let layer = fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_target(true)
                .with_writer(std::io::stdout);
            TracingRegistry::default()
                .with(filter)
                .with(layer)
                .try_init()?;
        }
        "pretty" => {
            let layer = fmt::layer()
                .pretty()
                .with_target(true)
                .with_writer(std::io::stdout);
            TracingRegistry::default()
                .with(filter)
                .with(layer)
                .try_init()?;
        }
        other => return Err(Error::InvalidLogFormat(other.to_owned())),
    }

    Ok(())
}

/// Build a tracing subscriber layer stack for tests (does **not** install global).
///
/// Returns events written to the provided buffer. Used to assert JSON / pretty
/// shapes without contending for the process-global subscriber.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    pub(crate) struct BufferWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl BufferWriter {
        pub(crate) fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let inner = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    inner: Arc::clone(&inner),
                },
                inner,
            )
        }
    }

    impl io::Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut g = self
                .inner
                .lock()
                .map_err(|e| io::Error::other(e.to_string()))?;
            g.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufferWriter {
        type Writer = BufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    pub(crate) fn with_json_subscriber<R>(
        filter: &str,
        writer: BufferWriter,
        f: impl FnOnce() -> R,
    ) -> R {
        let subscriber = TracingRegistry::default()
            .with(EnvFilter::new(filter))
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_writer(writer),
            );
        tracing::subscriber::with_default(subscriber, f)
    }

    pub(crate) fn with_pretty_subscriber<R>(
        filter: &str,
        writer: BufferWriter,
        f: impl FnOnce() -> R,
    ) -> R {
        let subscriber = TracingRegistry::default()
            .with(EnvFilter::new(filter))
            .with(fmt::layer().pretty().with_writer(writer));
        tracing::subscriber::with_default(subscriber, f)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use prometheus_client::encoding::text::encode;
    use std::sync::Arc;

    #[test]
    fn registry_exposes_required_families() {
        let mut registry = Registry::default();
        let metrics = Metrics::register(&mut registry, "chain", "0.1.0", "abc1234", "rustc 1.97.1");
        // prometheus-client skips empty families; seed one series per family so
        // HELP/TYPE lines are emitted (CC-05/3).
        metrics.record_grpc("chain", "/eth.chain.v1.ChainService/GetInfo", "0", 0.001);
        metrics.set_peer_health("chain", "p2p", true);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        for needle in [
            "cc_build_info",
            "cc_grpc_requests_total",
            "cc_grpc_request_duration_seconds",
            "cc_peer_health",
        ] {
            assert!(buf.contains(needle), "exposition missing {needle}:\n{buf}");
        }

        // Build info labels match eth.common.v1.BuildInfo one-for-one.
        assert!(buf.contains("service=\"chain\""));
        assert!(buf.contains("version=\"0.1.0\""));
        assert!(buf.contains("git_sha=\"abc1234\""));
        assert!(buf.contains("rustc=\"rustc 1.97.1\""));
        // Gauge always 1.
        assert!(
            buf.lines()
                .any(|l| l.starts_with("cc_build_info{") && l.ends_with("} 1")),
            "cc_build_info must be gauge 1:\n{buf}"
        );
    }

    #[test]
    fn json_log_lines_are_json_and_carry_service() {
        let (writer, buf) = test_support::BufferWriter::new();
        test_support::with_json_subscriber("info", writer, || {
            let span = info_span!("service", service = "chain");
            let _g = span.enter();
            tracing::info!("telemetry smoke");
        });

        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(!text.trim().is_empty(), "expected log output");
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let v: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"));
            // service field lives on the current span (Architecture §4.3).
            let service = v
                .pointer("/span/service")
                .or_else(|| v.get("service"))
                .or_else(|| v.pointer("/spans/0/service"));
            assert!(
                service.and_then(|s| s.as_str()) == Some("chain"),
                "line missing service field: {line}"
            );
        }
    }

    #[test]
    fn pretty_log_is_not_json() {
        let (writer, buf) = test_support::BufferWriter::new();
        test_support::with_pretty_subscriber("info", writer, || {
            let span = info_span!("service", service = "chain");
            let _g = span.enter();
            tracing::info!("pretty smoke");
        });

        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(!text.trim().is_empty(), "expected pretty log output");
        // At least one non-empty line must fail JSON parse (human format).
        let mut saw_non_json = false;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            if serde_json::from_str::<serde_json::Value>(line).is_err() {
                saw_non_json = true;
                break;
            }
        }
        assert!(
            saw_non_json,
            "pretty format must produce non-JSON human output, got:\n{text}"
        );
    }

    #[test]
    fn invalid_log_format_rejected() {
        // Fails before installing the global subscriber.
        let err = init(
            "chain",
            TelemetrySettings {
                log_format: "xml".into(),
                log_filter: "info".into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidLogFormat(_)));
        assert!(err.to_string().contains("xml"));
    }

    #[test]
    fn telemetry_settings_from_service_config() {
        let cfg = cc_config::ServiceConfig {
            grpc_addr: "127.0.0.1:9001".parse().unwrap(),
            metrics_addr: "127.0.0.1:9101".parse().unwrap(),
            peers: Default::default(),
            log_format: "pretty".into(),
            log_filter: "cc_chain=debug,info".into(),
        };
        let t = TelemetrySettings::from(&cfg);
        assert_eq!(t.log_format, "pretty");
        assert_eq!(t.log_filter, "cc_chain=debug,info");
    }

    #[tokio::test]
    async fn metrics_http_exposes_families() {
        let mut registry = Registry::default();
        let metrics = Metrics::register(&mut registry, "chain", VERSION, GIT_SHA, RUSTC);
        metrics.record_grpc("chain", "/eth.chain.v1.ChainService/GetInfo", "0", 0.001);
        metrics.set_peer_health("chain", "p2p", true);
        let registry = Arc::new(registry);
        let (addr, handle) = spawn_metrics_server(Arc::clone(&registry)).await.unwrap();

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let io = hyper_util::rt::TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = http::Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        assert_eq!(res.status(), hyper::StatusCode::OK);
        let body = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();
        for needle in [
            "cc_build_info",
            "cc_grpc_requests_total",
            "cc_grpc_request_duration_seconds",
            "cc_peer_health",
        ] {
            assert!(text.contains(needle), "missing {needle} in:\n{text}");
        }

        handle.abort();
        let _ = handle.await;
    }

    #[test]
    fn build_identity_constants_nonempty() {
        assert!(!GIT_SHA.is_empty());
        assert!(!RUSTC.is_empty());
        assert!(RUSTC.contains("rustc") || RUSTC == "unknown");
        assert!(!VERSION.is_empty());
    }
}
