//! Bootstrap error type.

use std::fmt;

/// Errors from telemetry init and the metrics exposition server.
#[derive(Debug)]
pub enum Error {
    /// Log format was neither `"json"` nor `"pretty"`.
    InvalidLogFormat(String),
    /// `EnvFilter` could not parse the resolved filter string.
    LogFilter(tracing_subscriber::filter::ParseError),
    /// Global tracing subscriber was already installed.
    TracingInit(tracing_subscriber::util::TryInitError),
    /// Metrics HTTP server failed to bind or accept.
    MetricsServer(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLogFormat(fmt) => {
                write!(
                    f,
                    "invalid log format {fmt:?}: expected \"json\" or \"pretty\""
                )
            }
            Self::LogFilter(e) => write!(f, "invalid log filter: {e}"),
            Self::TracingInit(e) => write!(f, "tracing subscriber init failed: {e}"),
            Self::MetricsServer(e) => write!(f, "metrics server: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidLogFormat(_) => None,
            Self::LogFilter(e) => Some(e),
            Self::TracingInit(e) => Some(e),
            Self::MetricsServer(e) => Some(e),
        }
    }
}

impl From<tracing_subscriber::filter::ParseError> for Error {
    fn from(value: tracing_subscriber::filter::ParseError) -> Self {
        Self::LogFilter(value)
    }
}

impl From<tracing_subscriber::util::TryInitError> for Error {
    fn from(value: tracing_subscriber::util::TryInitError) -> Self {
        Self::TracingInit(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::MetricsServer(value)
    }
}
