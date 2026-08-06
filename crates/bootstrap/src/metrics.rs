//! Shared metric families registered by every service (Architecture §4.4, CC-05/3).

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};

use crate::process::ProcessCollector;

/// Histogram buckets for gRPC latency (Architecture §4.4).
///
/// Sub-millisecond for Phase 0 stubs; past 4 s for Fulu's attestation deadline.
pub const GRPC_DURATION_BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Labels for `cc_build_info` — exactly `eth.common.v1.BuildInfo`'s four fields.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BuildInfoLabels {
    pub service: String,
    pub version: String,
    pub git_sha: String,
    pub rustc: String,
}

/// Labels for `cc_grpc_requests_total`.
///
/// `service` is the **process** name (`"chain"`), never the gRPC service name.
/// `method` is the full gRPC path (`/eth.chain.v1.ChainService/GetInfo`), or
/// `"unknown"` when the path is not in the known route set.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GrpcRequestLabels {
    pub service: String,
    pub method: String,
    pub code: String,
}

/// Labels for `cc_grpc_request_duration_seconds`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GrpcMethodLabels {
    pub service: String,
    pub method: String,
}

/// Labels for `cc_peer_health` (declared here; driven by CC-05b's prober).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PeerHealthLabels {
    pub service: String,
    pub peer: String,
}

/// Metric handles shared by the registry, the gRPC layer, and (later) the prober.
#[derive(Debug, Clone)]
pub struct Metrics {
    pub build_info: Family<BuildInfoLabels, Gauge>,
    pub grpc_requests: Family<GrpcRequestLabels, Counter>,
    pub grpc_duration: Family<GrpcMethodLabels, Histogram>,
    pub peer_health: Family<PeerHealthLabels, Gauge>,
}

impl Metrics {
    /// Create the four families and register them (plus the process collector) on `registry`.
    pub fn register(
        registry: &mut Registry,
        service: &str,
        version: &str,
        git_sha: &str,
        rustc: &str,
    ) -> Self {
        let build_info = Family::<BuildInfoLabels, Gauge>::default();
        build_info
            .get_or_create(&BuildInfoLabels {
                service: service.to_owned(),
                version: version.to_owned(),
                git_sha: git_sha.to_owned(),
                rustc: rustc.to_owned(),
            })
            .set(1);

        let grpc_requests = Family::<GrpcRequestLabels, Counter>::default();
        let grpc_duration = Family::<GrpcMethodLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(GRPC_DURATION_BUCKETS)
        });
        let peer_health = Family::<PeerHealthLabels, Gauge>::default();

        registry.register(
            "cc_build_info",
            "Build identity (always 1); labels match eth.common.v1.BuildInfo",
            build_info.clone(),
        );
        registry.register(
            "cc_grpc_requests",
            "Total gRPC requests handled by this process",
            grpc_requests.clone(),
        );
        registry.register_with_unit(
            "cc_grpc_request_duration",
            "gRPC request latency",
            Unit::Seconds,
            grpc_duration.clone(),
        );
        registry.register(
            "cc_peer_health",
            "Peer aggregate health (1=up, 0=down); driven by the peer prober",
            peer_health.clone(),
        );
        registry.register_collector(Box::new(ProcessCollector));

        Self {
            build_info,
            grpc_requests,
            grpc_duration,
            peer_health,
        }
    }

    /// Record one completed gRPC call.
    pub fn record_grpc(&self, service: &str, method: &str, code: &str, duration_secs: f64) {
        self.grpc_requests
            .get_or_create(&GrpcRequestLabels {
                service: service.to_owned(),
                method: method.to_owned(),
                code: code.to_owned(),
            })
            .inc();
        self.grpc_duration
            .get_or_create(&GrpcMethodLabels {
                service: service.to_owned(),
                method: method.to_owned(),
            })
            .observe(duration_secs);
    }

    /// Set `cc_peer_health{service,peer}` (used by CC-05b).
    pub fn set_peer_health(&self, service: &str, peer: &str, up: bool) {
        self.peer_health
            .get_or_create(&PeerHealthLabels {
                service: service.to_owned(),
                peer: peer.to_owned(),
            })
            .set(i64::from(up));
    }
}
