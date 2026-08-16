//! Test-only metrics surface used by moved `transport.rs` / `state.rs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use prometheus_client::registry::Registry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineMethod {
    NewPayloadV4,
    ForkchoiceUpdatedV3,
    GetBlobsV2,
    ExchangeCapabilities,
    EthSyncing,
}

impl EngineMethod {
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
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct MethodLabels {
    pub method: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ErrorCodeLabels {
    pub code: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct EngineStateLabels {
    pub state: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineStateLabel {
    Synced,
    Syncing,
    Offline,
    AuthFailed,
}

impl EngineStateLabel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::Syncing => "syncing",
            Self::Offline => "offline",
            Self::AuthFailed => "auth_failed",
        }
    }
}

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
    Other,
}

impl ErrorCode {
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
}

#[derive(Clone, Debug, Default)]
pub struct HistHandle;

impl HistHandle {
    pub fn observe(&self, _value: f64) {}
}

#[derive(Clone, Debug)]
pub struct CounterHandle {
    n: Arc<AtomicU64>,
}

impl CounterHandle {
    pub fn inc(&self) {
        self.n.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn get(&self) -> u64 {
        self.n.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug, Default)]
pub struct HistFamily;

impl HistFamily {
    #[must_use]
    pub fn get_or_create(&self, _labels: &MethodLabels) -> HistHandle {
        HistHandle
    }
}

#[derive(Clone, Debug, Default)]
pub struct CounterFamily {
    inner: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
}

impl CounterFamily {
    fn handle(&self, key: String) -> CounterHandle {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let n = map
            .entry(key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        CounterHandle { n }
    }

    #[must_use]
    pub fn get_or_create(&self, labels: &impl CounterKey) -> CounterHandle {
        self.handle(labels.key())
    }
}

pub trait CounterKey {
    fn key(&self) -> String;
}

impl CounterKey for MethodLabels {
    fn key(&self) -> String {
        self.method.clone()
    }
}

impl CounterKey for ErrorCodeLabels {
    fn key(&self) -> String {
        self.code.clone()
    }
}

#[derive(Clone, Debug, Default)]
pub struct GaugeHandle {
    n: Arc<AtomicI64>,
}

impl GaugeHandle {
    pub fn set(&self, value: i64) {
        self.n.store(value, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Default)]
pub struct GaugeFamily {
    inner: Arc<Mutex<HashMap<String, Arc<AtomicI64>>>>,
}

impl GaugeFamily {
    #[must_use]
    pub fn get_or_create(&self, labels: &EngineStateLabels) -> GaugeHandle {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let n = map
            .entry(labels.state.clone())
            .or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .clone();
        GaugeHandle { n }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EngineMetrics {
    pub request_seconds: HistFamily,
    pub soft_deadline_exceeded: CounterFamily,
    pub transport_timeout: CounterFamily,
    pub errors_total: CounterFamily,
    pub state: GaugeFamily,
    pub el_offline: GaugeHandle,
}

impl EngineMetrics {
    #[must_use]
    pub fn register(_registry: &mut Registry) -> Self {
        Self::default()
    }
}
