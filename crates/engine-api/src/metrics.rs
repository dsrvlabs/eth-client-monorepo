//! Engine service Prometheus metrics (CC-3Aa / Architecture §9).
//!
//! Registered into [`cc_bootstrap::Bootstrap::registry`] between `init` and
//! `serve` — the Phase 0 §4.1 seam, exactly as chain/p2p already do. **Most
//! families are declared empty** in this issue; individual observations fill
//! in with their owning requirements (same pattern as Phase 2 `CC-29a`).
//!
//! Soft-deadline substitution (ADR P3-13 / §9.2): the attestation deadline is
//! derived at runtime from `ATTESTATION_DUE_BPS × SLOT_DURATION_MS` (3 999.6 ms
//! on Hoodi, ≈ 3 000 ms under Gloas). A compile-time bucket boundary cannot
//! track that value, so `cc_engine_soft_deadline_exceeded_total` is a
//! **counter** — not a `4.0` boundary on `cc_engine_request_seconds`. Baking
//! `4.0` into the request ladder would silently become wrong at the next fork.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};

// ── bucket boundaries (§9.2, verbatim) ──────────────────────────────────────

/// `cc_engine_request_seconds` — `decimal_buckets(-2, 1)`, copied from the
/// reference client so dashboards are directly comparable (CC-3A/1).
///
/// Soft deadline is **not** a 4.0 bucket here: see module docs and
/// `ATTESTATION_DUE_BPS` (ADR P3-13).
pub const REQUEST_SECONDS_BUCKETS: [f64; 10] =
    [0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0];

/// `cc_engine_encode_seconds` — millisecond-scale (a ~3 MB hex encode would
/// otherwise sit entirely in the decimal ladder's first bucket).
pub const ENCODE_SECONDS_BUCKETS: [f64; 10] =
    [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

/// `cc_engine_upcheck_seconds` — exact boundary at 1.0 (the upcheck timeout).
pub const UPCHECK_SECONDS_BUCKETS: [f64; 9] = [0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0];

/// `cc_engine_fastpath_seconds` — exact at 1.0 (`getBlobsV2` transport timeout)
/// and 4.0 (attestation deadline).
pub const FASTPATH_SECONDS_BUCKETS: [f64; 10] =
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0];

/// `cc_engine_fastpath_to_available_seconds` — exact at 4.0, 12.0 (one slot),
/// and 48.0 (CC-24d's 4-slot `pending_da` timeout).
pub const FASTPATH_TO_AVAILABLE_SECONDS_BUCKETS: [f64; 10] =
    [0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 12.0, 24.0, 48.0];

// ── label sets (§9.3 closed sets) ───────────────────────────────────────────

/// Labels for method-scoped engine families.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MethodLabels {
    pub method: String,
}

/// Labels for `cc_engine_payload_status_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PayloadStatusLabels {
    pub method: String,
    pub status: String,
}

/// Labels for `cc_engine_errors_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ErrorCodeLabels {
    pub code: String,
}

/// Labels for `cc_engine_state`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct EngineStateLabels {
    pub state: String,
}

/// Labels for `cc_engine_getblobs_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GetBlobsResultLabels {
    pub result: String,
}

/// Labels for `cc_engine_sidecars_published_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SubscribedLabels {
    pub subscribed: String,
}

/// Labels for `cc_engine_sidecars_injected_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct InjectOutcomeLabels {
    pub outcome: String,
}

/// Labels for `cc_engine_fastpath_seconds`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FastpathStageLabels {
    pub stage: String,
}

// ── fixed label enums (closed; no unbounded external strings) ───────────────

/// `method` label values (§9.3) — cardinality 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineMethod {
    NewPayloadV4,
    ForkchoiceUpdatedV3,
    GetBlobsV2,
    ExchangeCapabilities,
    EthSyncing,
}

impl EngineMethod {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NewPayloadV4 => "newPayloadV4",
            Self::ForkchoiceUpdatedV3 => "forkchoiceUpdatedV3",
            Self::GetBlobsV2 => "getBlobsV2",
            Self::ExchangeCapabilities => "exchangeCapabilities",
            Self::EthSyncing => "eth_syncing",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 5] = [
        Self::NewPayloadV4,
        Self::ForkchoiceUpdatedV3,
        Self::GetBlobsV2,
        Self::ExchangeCapabilities,
        Self::EthSyncing,
    ];
}

/// `status` label values for `cc_engine_payload_status_total` (§9.3) — cardinality 5.
///
/// Asymmetry (`newPayloadV4` → five, `forkchoiceUpdatedV3` → three) is asserted
/// by CC-32b / CC-33, not here; this issue only declares the closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PayloadStatus {
    Valid,
    Invalid,
    Syncing,
    Accepted,
    InvalidBlockHash,
}

impl PayloadStatus {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "VALID",
            Self::Invalid => "INVALID",
            Self::Syncing => "SYNCING",
            Self::Accepted => "ACCEPTED",
            Self::InvalidBlockHash => "INVALID_BLOCK_HASH",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 5] = [
        Self::Valid,
        Self::Invalid,
        Self::Syncing,
        Self::Accepted,
        Self::InvalidBlockHash,
    ];
}

/// `state` label values for `cc_engine_state` (§9.3) — cardinality 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineStateLabel {
    Synced,
    Syncing,
    Offline,
    AuthFailed,
}

impl EngineStateLabel {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::Syncing => "syncing",
            Self::Offline => "offline",
            Self::AuthFailed => "auth_failed",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 4] = [Self::Synced, Self::Syncing, Self::Offline, Self::AuthFailed];
}

/// `direction` label values for optimistic transitions (§9.3) — cardinality 2.
///
/// Declared here so `label_sets_are_closed` owns all nine §9.3 sets in one crate.
/// Chain's `OptimisticDirection` is the observation-side source of truth (no
/// crate edge); keep label strings identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptimisticDirection {
    Validated,
    Invalidated,
}

impl OptimisticDirection {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validated => "validated",
            Self::Invalidated => "invalidated",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 2] = [Self::Validated, Self::Invalidated];
}

/// `result` label values for `cc_engine_getblobs_total` (§9.3) — cardinality 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GetBlobsResult {
    Complete,
    Miss,
    Error,
}

impl GetBlobsResult {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Miss => "miss",
            Self::Error => "error",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 3] = [Self::Complete, Self::Miss, Self::Error];
}

/// `subscribed` label values (§9.3) — cardinality 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Subscribed {
    True,
    False,
}

impl Subscribed {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::True => "true",
            Self::False => "false",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 2] = [Self::True, Self::False];
}

/// `outcome` label values for injected sidecars (§9.3) — cardinality 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InjectOutcome {
    New,
    Duplicate,
    Rejected,
}

impl InjectOutcome {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Duplicate => "duplicate",
            Self::Rejected => "rejected",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 3] = [Self::New, Self::Duplicate, Self::Rejected];
}

/// `stage` label values for `cc_engine_fastpath_seconds` (§9.3) — cardinality 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FastpathStage {
    Fetch,
    ComputeCells,
    Assemble,
    Inject,
}

impl FastpathStage {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::ComputeCells => "compute_cells",
            Self::Assemble => "assemble",
            Self::Inject => "inject",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 4] = [
        Self::Fetch,
        Self::ComputeCells,
        Self::Assemble,
        Self::Inject,
    ];
}

/// `code` label values for `cc_engine_errors_total` (§9.3).
///
/// Closed set generated by our own code, except `Other` which is the
/// cardinality guard for unrecognised EL-supplied JSON-RPC codes. Unknown
/// codes are logged with the numeric value and counted under `other`.
///
/// Enumerated cardinality is **20** (the §9.3 table's "21" is an off-by-one
/// against this explicit list).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    ParseError,
    InvalidRequest,
    MethodNotFound,
    InvalidParams,
    InternalError,
    ServerError,
    UnknownPayload,
    InvalidForkchoiceState,
    InvalidPayloadAttributes,
    TooLargeRequest,
    UnsupportedFork,
    InvalidRange,
    Http401,
    Http403,
    Http4xx,
    Http5xx,
    Timeout,
    Transport,
    Decode,
    /// Cardinality guard — unrecognised EL-supplied codes collapse here.
    Other,
}

impl ErrorCode {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParseError => "-32700",
            Self::InvalidRequest => "-32600",
            Self::MethodNotFound => "-32601",
            Self::InvalidParams => "-32602",
            Self::InternalError => "-32603",
            Self::ServerError => "-32000",
            Self::UnknownPayload => "-38001",
            Self::InvalidForkchoiceState => "-38002",
            Self::InvalidPayloadAttributes => "-38003",
            Self::TooLargeRequest => "-38004",
            Self::UnsupportedFork => "-38005",
            Self::InvalidRange => "-38006",
            Self::Http401 => "http_401",
            Self::Http403 => "http_403",
            Self::Http4xx => "http_4xx",
            Self::Http5xx => "http_5xx",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Decode => "decode",
            Self::Other => "other",
        }
    }

    /// All closed label values (seed + tests). Length is the §9.3 cardinality
    /// for `code` as enumerated (20; see enum docs).
    pub const ALL: [Self; 20] = [
        Self::ParseError,
        Self::InvalidRequest,
        Self::MethodNotFound,
        Self::InvalidParams,
        Self::InternalError,
        Self::ServerError,
        Self::UnknownPayload,
        Self::InvalidForkchoiceState,
        Self::InvalidPayloadAttributes,
        Self::TooLargeRequest,
        Self::UnsupportedFork,
        Self::InvalidRange,
        Self::Http401,
        Self::Http403,
        Self::Http4xx,
        Self::Http5xx,
        Self::Timeout,
        Self::Transport,
        Self::Decode,
        Self::Other,
    ];

    /// Map a JSON-RPC numeric error code to a closed label.
    ///
    /// Unrecognised codes return [`None`] so the caller can log the numeric
    /// value and count under [`Self::Other`] (cardinality guard).
    #[must_use]
    pub const fn from_jsonrpc_code(code: i64) -> Option<Self> {
        match code {
            -32700 => Some(Self::ParseError),
            -32600 => Some(Self::InvalidRequest),
            -32601 => Some(Self::MethodNotFound),
            -32602 => Some(Self::InvalidParams),
            -32603 => Some(Self::InternalError),
            -32000 => Some(Self::ServerError),
            -38001 => Some(Self::UnknownPayload),
            -38002 => Some(Self::InvalidForkchoiceState),
            -38003 => Some(Self::InvalidPayloadAttributes),
            -38004 => Some(Self::TooLargeRequest),
            -38005 => Some(Self::UnsupportedFork),
            -38006 => Some(Self::InvalidRange),
            _ => None,
        }
    }
}

// ── metric handles ──────────────────────────────────────────────────────────

/// All engine §9.1 metric families (CC-3Aa).
///
/// Cheap to clone (each field is a handle into shared series storage).
/// Producers for each family land in the requirement that owns them — this
/// issue only declares and seeds (except the `code="other"` guard helper).
#[derive(Debug, Clone)]
pub struct EngineMetrics {
    // Latency
    pub request_seconds: Family<MethodLabels, Histogram>,
    pub encode_seconds: Family<MethodLabels, Histogram>,
    pub upcheck_seconds: Histogram,
    // Status
    pub payload_status: Family<PayloadStatusLabels, Counter>,
    // Budget
    // Soft deadline: counter, not a 4.0 bucket — see ATTESTATION_DUE_BPS (ADR P3-13 / §9.2).
    pub soft_deadline_exceeded: Family<MethodLabels, Counter>,
    pub transport_timeout: Family<MethodLabels, Counter>,
    // Errors
    pub errors_total: Family<ErrorCodeLabels, Counter>,
    pub unsupported_fork: Counter,
    pub capability_missing: Family<MethodLabels, Gauge>,
    // Engine state
    pub state: Family<EngineStateLabels, Gauge>,
    pub el_offline: Gauge,
    pub fcu_dropped_stale: Counter,
    pub inject_stream_state: Gauge,
    // Fast path
    pub getblobs_total: Family<GetBlobsResultLabels, Counter>,
    pub cells_computed: Counter,
    pub fastpath_seconds: Family<FastpathStageLabels, Histogram>,
    pub sidecars_published: Family<SubscribedLabels, Counter>,
    pub sidecars_injected: Family<InjectOutcomeLabels, Counter>,
    pub fastpath_dropped: Counter,
    pub fastpath_to_available: Histogram,
}

impl EngineMetrics {
    /// Create and register every §9.1 engine family on `registry`.
    ///
    /// Call between [`cc_bootstrap::init`] and [`cc_bootstrap::serve`]. Seeds
    /// labelled series so exposition always emits HELP/TYPE (prometheus-client
    /// omits empty families).
    pub fn register(registry: &mut Registry) -> Self {
        // ── construct ───────────────────────────────────────────────────────
        let request_seconds = Family::<MethodLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(REQUEST_SECONDS_BUCKETS)
        });
        let encode_seconds = Family::<MethodLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(ENCODE_SECONDS_BUCKETS)
        });
        let upcheck_seconds = Histogram::new(UPCHECK_SECONDS_BUCKETS);

        let payload_status = Family::<PayloadStatusLabels, Counter>::default();

        // Soft deadline is a counter (not a 4.0 bucket on request_seconds):
        // ATTESTATION_DUE_BPS × SLOT_DURATION_MS is runtime-derived (ADR P3-13).
        let soft_deadline_exceeded = Family::<MethodLabels, Counter>::default();
        let transport_timeout = Family::<MethodLabels, Counter>::default();

        let errors_total = Family::<ErrorCodeLabels, Counter>::default();
        let unsupported_fork = Counter::default();
        let capability_missing = Family::<MethodLabels, Gauge>::default();

        let state = Family::<EngineStateLabels, Gauge>::default();
        let el_offline = Gauge::default();
        let fcu_dropped_stale = Counter::default();
        let inject_stream_state = Gauge::default();

        let getblobs_total = Family::<GetBlobsResultLabels, Counter>::default();
        let cells_computed = Counter::default();
        let fastpath_seconds =
            Family::<FastpathStageLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(FASTPATH_SECONDS_BUCKETS)
            });
        let sidecars_published = Family::<SubscribedLabels, Counter>::default();
        let sidecars_injected = Family::<InjectOutcomeLabels, Counter>::default();
        let fastpath_dropped = Counter::default();
        let fastpath_to_available = Histogram::new(FASTPATH_TO_AVAILABLE_SECONDS_BUCKETS);

        // ── register (OpenMetrics appends `_total` for counters) ────────────
        registry.register_with_unit(
            "cc_engine_request",
            "Wall time of Engine API requests (method; decimal_buckets(-2,1); soft deadline is a separate counter — ATTESTATION_DUE_BPS, not a 4.0 bucket)",
            Unit::Seconds,
            request_seconds.clone(),
        );
        registry.register_with_unit(
            "cc_engine_encode",
            "Wall time of SSZ-decode + JSON-hex-encode (method; millisecond ladder)",
            Unit::Seconds,
            encode_seconds.clone(),
        );
        registry.register_with_unit(
            "cc_engine_upcheck",
            "Wall time of eth_syncing upcheck (boundary at 1.0 s)",
            Unit::Seconds,
            upcheck_seconds.clone(),
        );
        registry.register(
            "cc_engine_payload_status",
            "PayloadStatusV1 outcomes (method, status=VALID|INVALID|SYNCING|ACCEPTED|INVALID_BLOCK_HASH)",
            payload_status.clone(),
        );
        // Soft deadline: counter, not histogram bucket — ATTESTATION_DUE_BPS (ADR P3-13).
        registry.register(
            "cc_engine_soft_deadline_exceeded",
            "Engine API calls that exceeded the runtime-derived attestation soft deadline (ATTESTATION_DUE_BPS × SLOT_DURATION_MS); never aborts",
            soft_deadline_exceeded.clone(),
        );
        registry.register(
            "cc_engine_transport_timeout",
            "Engine API calls that hit the per-method transport timeout (method)",
            transport_timeout.clone(),
        );
        registry.register(
            "cc_engine_errors",
            "Engine transport/JSON-RPC errors (code; unknown numeric codes collapse to other)",
            errors_total.clone(),
        );
        registry.register(
            "cc_engine_unsupported_fork",
            "Unsupported-fork errors (EL -38005 or our version-gate alarm)",
            unsupported_fork.clone(),
        );
        registry.register(
            "cc_engine_capability_missing",
            "Advertised capability missing on the connected EL (method; 0/1 gauge)",
            capability_missing.clone(),
        );
        registry.register(
            "cc_engine_state",
            "Engine internal state (state=synced|syncing|offline|auth_failed; 0/1 gauge)",
            state.clone(),
        );
        registry.register(
            "cc_engine_el_offline",
            "Execution layer offline from the engine's perspective (0/1)",
            el_offline.clone(),
        );
        registry.register(
            "cc_engine_fcu_dropped_stale",
            "forkchoiceUpdated calls dropped because a newer sequence superseded them",
            fcu_dropped_stale.clone(),
        );
        registry.register(
            "cc_engine_inject_stream_state",
            "Engine↔p2p inject stream connected (0/1)",
            inject_stream_state.clone(),
        );
        registry.register(
            "cc_engine_getblobs",
            "getBlobsV2 outcomes (result=complete|miss|error)",
            getblobs_total.clone(),
        );
        registry.register(
            "cc_engine_cells_computed",
            "KZG cells computed on the getBlobsV2 fast path",
            cells_computed.clone(),
        );
        registry.register_with_unit(
            "cc_engine_fastpath",
            "Wall time of fast-path stages (stage=fetch|compute_cells|assemble|inject)",
            Unit::Seconds,
            fastpath_seconds.clone(),
        );
        registry.register(
            "cc_engine_sidecars_published",
            "Sidecars published to gossip (subscribed=true|false; false must stay zero)",
            sidecars_published.clone(),
        );
        registry.register(
            "cc_engine_sidecars_injected",
            "Sidecars injected into p2p (outcome=new|duplicate|rejected)",
            sidecars_injected.clone(),
        );
        registry.register(
            "cc_engine_fastpath_dropped",
            "Fast-path work dropped under load or disconnect",
            fastpath_dropped.clone(),
        );
        registry.register_with_unit(
            "cc_engine_fastpath_to_available",
            "Wall time from fast-path start to DA available (boundaries at 4/12/48 s)",
            Unit::Seconds,
            fastpath_to_available.clone(),
        );

        let metrics = Self {
            request_seconds,
            encode_seconds,
            upcheck_seconds,
            payload_status,
            soft_deadline_exceeded,
            transport_timeout,
            errors_total,
            unsupported_fork,
            capability_missing,
            state,
            el_offline,
            fcu_dropped_stale,
            inject_stream_state,
            getblobs_total,
            cells_computed,
            fastpath_seconds,
            sidecars_published,
            sidecars_injected,
            fastpath_dropped,
            fastpath_to_available,
        };
        metrics.seed_exposition();
        metrics
    }

    /// Ensure every labelled family has at least one series so HELP/TYPE appear.
    fn seed_exposition(&self) {
        for method in EngineMethod::ALL {
            let labels = MethodLabels {
                method: method.as_str().to_owned(),
            };
            self.request_seconds.get_or_create(&labels).observe(0.0);
            self.encode_seconds.get_or_create(&labels).observe(0.0);
            let _ = self.soft_deadline_exceeded.get_or_create(&labels).get();
            let _ = self.transport_timeout.get_or_create(&labels).get();
            self.capability_missing.get_or_create(&labels).set(0);
        }
        self.upcheck_seconds.observe(0.0);

        for method in [
            EngineMethod::NewPayloadV4,
            EngineMethod::ForkchoiceUpdatedV3,
        ] {
            for status in PayloadStatus::ALL {
                let _ = self
                    .payload_status
                    .get_or_create(&PayloadStatusLabels {
                        method: method.as_str().to_owned(),
                        status: status.as_str().to_owned(),
                    })
                    .get();
            }
        }

        for code in ErrorCode::ALL {
            let _ = self
                .errors_total
                .get_or_create(&ErrorCodeLabels {
                    code: code.as_str().to_owned(),
                })
                .get();
        }
        let _ = self.unsupported_fork.get();

        for state in EngineStateLabel::ALL {
            self.state
                .get_or_create(&EngineStateLabels {
                    state: state.as_str().to_owned(),
                })
                .set(0);
        }
        self.el_offline.set(0);
        let _ = self.fcu_dropped_stale.get();
        self.inject_stream_state.set(0);

        for result in GetBlobsResult::ALL {
            let _ = self
                .getblobs_total
                .get_or_create(&GetBlobsResultLabels {
                    result: result.as_str().to_owned(),
                })
                .get();
        }
        let _ = self.cells_computed.get();
        for stage in FastpathStage::ALL {
            self.fastpath_seconds
                .get_or_create(&FastpathStageLabels {
                    stage: stage.as_str().to_owned(),
                })
                .observe(0.0);
        }
        for subscribed in Subscribed::ALL {
            let _ = self
                .sidecars_published
                .get_or_create(&SubscribedLabels {
                    subscribed: subscribed.as_str().to_owned(),
                })
                .get();
        }
        for outcome in InjectOutcome::ALL {
            let _ = self
                .sidecars_injected
                .get_or_create(&InjectOutcomeLabels {
                    outcome: outcome.as_str().to_owned(),
                })
                .get();
        }
        let _ = self.fastpath_dropped.get();
        self.fastpath_to_available.observe(0.0);
    }

    /// Increment `cc_engine_errors_total` for a JSON-RPC numeric code.
    ///
    /// Unrecognised codes are logged with the numeric value and counted under
    /// `code="other"` — the cardinality guard (§9.3).
    pub fn observe_jsonrpc_error_code(&self, code: i64) {
        let label = match ErrorCode::from_jsonrpc_code(code) {
            Some(c) => c,
            None => {
                tracing::warn!(
                    code,
                    "unrecognised JSON-RPC error code; counting under code=other"
                );
                ErrorCode::Other
            }
        };
        self.errors_total
            .get_or_create(&ErrorCodeLabels {
                code: label.as_str().to_owned(),
            })
            .inc();
    }

    /// Read `cc_engine_errors_total{code}` (tests).
    pub fn errors_total_count(&self, code: ErrorCode) -> u64 {
        self.errors_total
            .get_or_create(&ErrorCodeLabels {
                code: code.as_str().to_owned(),
            })
            .get()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use prometheus_client::encoding::text::encode;
    use std::collections::BTreeSet;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::Registry as TracingRegistry;
    use tracing_subscriber::fmt;
    use tracing_subscriber::layer::SubscriberExt;

    /// Every §9.1 engine family name as it appears on OpenMetrics `# TYPE` lines
    /// after register+seed.
    const EXPECTED_FAMILIES: &[&str] = &[
        "cc_engine_request_seconds",
        "cc_engine_encode_seconds",
        "cc_engine_upcheck_seconds",
        "cc_engine_payload_status",
        "cc_engine_soft_deadline_exceeded",
        "cc_engine_transport_timeout",
        "cc_engine_errors",
        "cc_engine_unsupported_fork",
        "cc_engine_capability_missing",
        "cc_engine_state",
        "cc_engine_el_offline",
        "cc_engine_fcu_dropped_stale",
        "cc_engine_inject_stream_state",
        "cc_engine_getblobs",
        "cc_engine_cells_computed",
        "cc_engine_fastpath_seconds",
        "cc_engine_sidecars_published",
        "cc_engine_sidecars_injected",
        "cc_engine_fastpath_dropped",
        "cc_engine_fastpath_to_available_seconds",
    ];

    fn parse_cc_engine_family_names(buf: &str, kind: &str) -> BTreeSet<String> {
        let prefix = match kind {
            "TYPE" => "# TYPE ",
            "HELP" => "# HELP ",
            other => panic!("unknown openmetrics meta kind: {other}"),
        };
        let mut set = BTreeSet::new();
        for line in buf.lines() {
            let Some(rest) = line.strip_prefix(prefix) else {
                continue;
            };
            let Some(name) = rest.split_whitespace().next() else {
                continue;
            };
            if name.starts_with("cc_engine_") {
                set.insert(name.to_owned());
            }
        }
        set
    }

    #[derive(Clone, Debug)]
    struct BufferWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl BufferWriter {
        fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let inner = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    inner: Arc::clone(&inner),
                },
                inner,
            )
        }
    }

    impl Write for BufferWriter {
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

    fn with_json_subscriber<R>(writer: BufferWriter, f: impl FnOnce() -> R) -> R {
        let subscriber = TracingRegistry::default()
            .with(EnvFilter::new("warn"))
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_writer(writer),
            );
        tracing::subscriber::with_default(subscriber, f)
    }

    #[test]
    fn bucket_ladders_match_section_9_2_element_for_element() {
        // Five explicit ladders from §9.2 (chain PROCESS_BLOCK_BUCKETS is
        // asserted on the chain side).
        assert_eq!(
            REQUEST_SECONDS_BUCKETS,
            [0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0]
        );
        assert_eq!(REQUEST_SECONDS_BUCKETS.len(), 10);

        assert_eq!(
            ENCODE_SECONDS_BUCKETS,
            [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        );
        assert_eq!(ENCODE_SECONDS_BUCKETS.len(), 10);

        assert_eq!(
            UPCHECK_SECONDS_BUCKETS,
            [0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]
        );
        assert_eq!(UPCHECK_SECONDS_BUCKETS.len(), 9);
        assert!(UPCHECK_SECONDS_BUCKETS.contains(&1.0));

        assert_eq!(
            FASTPATH_SECONDS_BUCKETS,
            [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0]
        );
        assert_eq!(FASTPATH_SECONDS_BUCKETS.len(), 10);
        assert!(FASTPATH_SECONDS_BUCKETS.contains(&1.0));
        assert!(FASTPATH_SECONDS_BUCKETS.contains(&4.0));

        assert_eq!(
            FASTPATH_TO_AVAILABLE_SECONDS_BUCKETS,
            [0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 12.0, 24.0, 48.0]
        );
        assert_eq!(FASTPATH_TO_AVAILABLE_SECONDS_BUCKETS.len(), 10);
        assert!(FASTPATH_TO_AVAILABLE_SECONDS_BUCKETS.contains(&4.0));
        assert!(FASTPATH_TO_AVAILABLE_SECONDS_BUCKETS.contains(&12.0));
        assert!(FASTPATH_TO_AVAILABLE_SECONDS_BUCKETS.contains(&48.0));
    }

    #[test]
    fn label_sets_are_closed() {
        // §9.3's nine label value sets with documented cardinalities.
        assert_eq!(EngineMethod::ALL.len(), 5);
        assert_eq!(PayloadStatus::ALL.len(), 5);
        assert_eq!(EngineStateLabel::ALL.len(), 4);
        assert_eq!(OptimisticDirection::ALL.len(), 2);
        assert_eq!(GetBlobsResult::ALL.len(), 3);
        assert_eq!(Subscribed::ALL.len(), 2);
        assert_eq!(InjectOutcome::ALL.len(), 3);
        assert_eq!(FastpathStage::ALL.len(), 4);
        // Enumerated closed set is 20 (architecture table lists "21" off-by-one).
        assert_eq!(ErrorCode::ALL.len(), 20);

        let methods: BTreeSet<&str> = EngineMethod::ALL.iter().map(|m| m.as_str()).collect();
        assert_eq!(
            methods,
            BTreeSet::from([
                "newPayloadV4",
                "forkchoiceUpdatedV3",
                "getBlobsV2",
                "exchangeCapabilities",
                "eth_syncing",
            ])
        );

        let statuses: BTreeSet<&str> = PayloadStatus::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            statuses,
            BTreeSet::from([
                "VALID",
                "INVALID",
                "SYNCING",
                "ACCEPTED",
                "INVALID_BLOCK_HASH",
            ])
        );

        let states: BTreeSet<&str> = EngineStateLabel::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            states,
            BTreeSet::from(["synced", "syncing", "offline", "auth_failed"])
        );

        let directions: BTreeSet<&str> = OptimisticDirection::ALL
            .iter()
            .map(|d| d.as_str())
            .collect();
        assert_eq!(directions, BTreeSet::from(["validated", "invalidated"]));

        let results: BTreeSet<&str> = GetBlobsResult::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(results, BTreeSet::from(["complete", "miss", "error"]));

        let subscribed: BTreeSet<&str> = Subscribed::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(subscribed, BTreeSet::from(["true", "false"]));

        let outcomes: BTreeSet<&str> = InjectOutcome::ALL.iter().map(|o| o.as_str()).collect();
        assert_eq!(outcomes, BTreeSet::from(["new", "duplicate", "rejected"]));

        let stages: BTreeSet<&str> = FastpathStage::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            stages,
            BTreeSet::from(["fetch", "compute_cells", "assemble", "inject"])
        );

        let codes: BTreeSet<&str> = ErrorCode::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(
            codes,
            BTreeSet::from([
                "-32700",
                "-32600",
                "-32601",
                "-32602",
                "-32603",
                "-32000",
                "-38001",
                "-38002",
                "-38003",
                "-38004",
                "-38005",
                "-38006",
                "http_401",
                "http_403",
                "http_4xx",
                "http_5xx",
                "timeout",
                "transport",
                "decode",
                "other",
            ])
        );
        assert!(codes.contains("other"));
    }

    #[test]
    fn unknown_code_collapses_to_other() {
        let mut registry = Registry::default();
        let m = EngineMetrics::register(&mut registry);
        let before_other = m.errors_total_count(ErrorCode::Other);

        // Snapshot label values present before the unrecognised code.
        let mut before_buf = String::new();
        encode(&mut before_buf, &registry).unwrap();
        let before_codes = extract_error_code_labels(&before_buf);

        let unknown = -99999_i64;
        let (writer, log_buf) = BufferWriter::new();
        with_json_subscriber(writer, || {
            m.observe_jsonrpc_error_code(unknown);
        });

        assert_eq!(
            m.errors_total_count(ErrorCode::Other),
            before_other + 1,
            "unknown code must increment code=other"
        );

        let mut after_buf = String::new();
        encode(&mut after_buf, &registry).unwrap();
        let after_codes = extract_error_code_labels(&after_buf);
        assert_eq!(
            before_codes, after_codes,
            "no new code label value may appear; before={before_codes:?} after={after_codes:?}"
        );
        assert!(
            !after_buf.contains(&format!("code=\"{unknown}\"")),
            "raw unknown code must not become a label:\n{after_buf}"
        );

        let text = String::from_utf8(log_buf.lock().unwrap().clone()).unwrap();
        assert!(
            text.contains(&unknown.to_string())
                || text.contains("unrecognised JSON-RPC error code"),
            "log line must contain the numeric code; got:\n{text}"
        );
        // Prefer the numeric value actually present.
        assert!(
            text.contains(&unknown.to_string()),
            "log line must contain the numeric code {unknown}; got:\n{text}"
        );
    }

    fn extract_error_code_labels(buf: &str) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        for line in buf.lines() {
            if !line.starts_with("cc_engine_errors_total{") {
                continue;
            }
            if let Some(start) = line.find("code=\"") {
                let rest = &line[start + 6..];
                if let Some(end) = rest.find('"') {
                    set.insert(rest[..end].to_owned());
                }
            }
        }
        set
    }

    #[test]
    fn family_name_fixture_matches_exposition() {
        let mut registry = Registry::default();
        let _m = EngineMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        let expected: BTreeSet<&str> = EXPECTED_FAMILIES.iter().copied().collect();
        let type_names = parse_cc_engine_family_names(&buf, "TYPE");
        let help_names = parse_cc_engine_family_names(&buf, "HELP");
        let type_as_str: BTreeSet<&str> = type_names.iter().map(String::as_str).collect();
        let help_as_str: BTreeSet<&str> = help_names.iter().map(String::as_str).collect();

        assert_eq!(
            type_as_str,
            expected,
            "TYPE family set must equal EXPECTED_FAMILIES\nmissing: {:?}\nextra: {:?}",
            expected.difference(&type_as_str).collect::<Vec<_>>(),
            type_as_str.difference(&expected).collect::<Vec<_>>(),
        );
        assert_eq!(
            help_as_str,
            expected,
            "HELP family set must equal EXPECTED_FAMILIES\nmissing: {:?}\nextra: {:?}",
            expected.difference(&help_as_str).collect::<Vec<_>>(),
            help_as_str.difference(&expected).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn soft_deadline_is_counter_not_request_bucket() {
        // Source-level guarantee: request ladder has no 4.0; soft deadline is a counter.
        assert!(
            !REQUEST_SECONDS_BUCKETS.contains(&4.0),
            "4.0 must not be a request_seconds bucket (ATTESTATION_DUE_BPS is runtime-derived)"
        );
        let mut registry = Registry::default();
        let _m = EngineMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("# TYPE cc_engine_soft_deadline_exceeded counter")
                || buf.lines().any(
                    |l| l.contains("cc_engine_soft_deadline_exceeded") && l.contains("counter")
                ),
            "soft_deadline_exceeded must be a counter:\n{buf}"
        );
        assert!(
            !buf.contains("cc_engine_request_seconds_bucket{le=\"4.0\"}"),
            "request_seconds must not have le=4.0:\n{buf}"
        );
    }

    #[test]
    fn known_jsonrpc_code_maps_without_other() {
        let mut registry = Registry::default();
        let m = EngineMetrics::register(&mut registry);
        let before_other = m.errors_total_count(ErrorCode::Other);
        let before_params = m.errors_total_count(ErrorCode::InvalidParams);
        m.observe_jsonrpc_error_code(-32602);
        assert_eq!(
            m.errors_total_count(ErrorCode::InvalidParams),
            before_params + 1
        );
        assert_eq!(m.errors_total_count(ErrorCode::Other), before_other);
    }
}
