//! Engine API transport error taxonomy (CC-30a / Architecture §3.5).
//!
//! One typed enum, one classification function, one [`RetryClass`] per code.
//! **Only `-32603` and `-32000` are retryable** inside the transport sense of
//! "the EL is unwell"; `newPayload` is **never** retried inside `services/engine`
//! (ADR P3-09) — chain's `pending_engine` is the retry.

use std::fmt;

use crate::metrics::ErrorCode;

/// Where the failure lives for upstream policy (Architecture §3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryClass {
    /// Our bug or a non-recoverable protocol error. Never retried.
    Fatal,
    /// The EL is unwell. Chain requeues; transport does not nest a second retry
    /// for `newPayload` (ADR P3-09).
    Transient,
    /// Operator error (wrong JWT, host rejection). Terminal engine state.
    Auth,
}

/// Typed Engine API / transport failure.
///
/// Bodies of HTTP error responses are captured **verbatim** (after the 1 MiB
/// cap) so the el-runbook table can be grepped against the log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    /// JSON-RPC `-32700`.
    ParseError { message: String },
    /// JSON-RPC `-32600`.
    InvalidRequest { message: String },
    /// JSON-RPC `-32601`.
    MethodNotFound { message: String },
    /// JSON-RPC `-32602`.
    InvalidParams { message: String },
    /// JSON-RPC `-32603` — **retryable**.
    InternalError { message: String },
    /// JSON-RPC `-32000` — **retryable**; `data.err` is logged when present.
    ServerError {
        message: String,
        data_err: Option<String>,
    },
    /// JSON-RPC `-38001`.
    UnknownPayload { message: String },
    /// JSON-RPC `-38002`.
    InvalidForkchoiceState { message: String },
    /// JSON-RPC `-38003`.
    InvalidPayloadAttributes { message: String },
    /// JSON-RPC `-38004`.
    TooLargeRequest { message: String },
    /// JSON-RPC `-38005`.
    UnsupportedFork { message: String },
    /// JSON-RPC `-38006`.
    InvalidRange { message: String },
    /// HTTP 401 with plain-text body (geth auth failures: `stale token`, …).
    ///
    /// Distinct from [`Self::Http403`] at the type level so a CL cannot treat
    /// vhost rejection as a JWT problem (CC-30b / PRD delta 15).
    Http401 { body: String },
    /// HTTP 403 with plain-text body (vhost misconfiguration: `invalid host specified`).
    ///
    /// Distinct from [`Self::Http401`]: the token may be valid; the `Host:` is not.
    Http403 { body: String },
    /// Other HTTP 4xx.
    HttpClient { code: u16, body: String },
    /// HTTP 5xx.
    HttpServer { code: u16, body: String },
    /// Per-call transport timeout.
    Timeout { method: String },
    /// Lower-level transport failure (connect reset, DNS, …).
    Transport { detail: String },
    /// Body cap exceeded, invalid JSON, or envelope shape failure.
    Decode { reason: String },
    /// Unrecognised JSON-RPC numeric code (cardinality guard → metric `other`).
    Other { code: i64, message: String },
}

impl EngineError {
    /// Metric label for `cc_engine_errors_total{code}`.
    #[must_use]
    pub fn metric_code(&self) -> ErrorCode {
        match self {
            Self::ParseError { .. } => ErrorCode::ParseError,
            Self::InvalidRequest { .. } => ErrorCode::InvalidRequest,
            Self::MethodNotFound { .. } => ErrorCode::MethodNotFound,
            Self::InvalidParams { .. } => ErrorCode::InvalidParams,
            Self::InternalError { .. } => ErrorCode::InternalError,
            Self::ServerError { .. } => ErrorCode::ServerError,
            Self::UnknownPayload { .. } => ErrorCode::UnknownPayload,
            Self::InvalidForkchoiceState { .. } => ErrorCode::InvalidForkchoiceState,
            Self::InvalidPayloadAttributes { .. } => ErrorCode::InvalidPayloadAttributes,
            Self::TooLargeRequest { .. } => ErrorCode::TooLargeRequest,
            Self::UnsupportedFork { .. } => ErrorCode::UnsupportedFork,
            Self::InvalidRange { .. } => ErrorCode::InvalidRange,
            Self::Http401 { .. } => ErrorCode::Http401,
            Self::Http403 { .. } => ErrorCode::Http403,
            Self::HttpClient { .. } => ErrorCode::Http4xx,
            Self::HttpServer { .. } => ErrorCode::Http5xx,
            Self::Timeout { .. } => ErrorCode::Timeout,
            Self::Transport { .. } => ErrorCode::Transport,
            Self::Decode { .. } => ErrorCode::Decode,
            Self::Other { .. } => ErrorCode::Other,
        }
    }

    /// Retry class for this error (Architecture §3.5).
    ///
    /// Exactly two codes are [`RetryClass::Transient`]: `-32603` and `-32000`.
    #[must_use]
    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::InternalError { .. } | Self::ServerError { .. } => RetryClass::Transient,
            Self::Http401 { .. } | Self::Http403 { .. } => RetryClass::Auth,
            _ => RetryClass::Fatal,
        }
    }

    /// Classify a JSON-RPC error object (`code`, `message`, optional `data.err`).
    #[must_use]
    pub fn from_jsonrpc(code: i64, message: String, data_err: Option<String>) -> Self {
        match code {
            -32700 => Self::ParseError { message },
            -32600 => Self::InvalidRequest { message },
            -32601 => Self::MethodNotFound { message },
            -32602 => Self::InvalidParams { message },
            -32603 => Self::InternalError { message },
            -32000 => Self::ServerError { message, data_err },
            -38001 => Self::UnknownPayload { message },
            -38002 => Self::InvalidForkchoiceState { message },
            -38003 => Self::InvalidPayloadAttributes { message },
            -38004 => Self::TooLargeRequest { message },
            -38005 => Self::UnsupportedFork { message },
            -38006 => Self::InvalidRange { message },
            other => Self::Other {
                code: other,
                message,
            },
        }
    }

    /// Map an HTTP status + capped body into the taxonomy (§3.1).
    #[must_use]
    pub fn from_http_status(status: u16, body: String) -> Self {
        match status {
            401 => Self::Http401 { body },
            403 => Self::Http403 { body },
            400..=499 => Self::HttpClient { code: status, body },
            500..=599 => Self::HttpServer { code: status, body },
            _ => Self::Transport {
                detail: format!("unexpected HTTP status {status}: {body}"),
            },
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError { message } => write!(f, "JSON-RPC -32700 parse error: {message}"),
            Self::InvalidRequest { message } => {
                write!(f, "JSON-RPC -32600 invalid request: {message}")
            }
            Self::MethodNotFound { message } => {
                write!(f, "JSON-RPC -32601 method not found: {message}")
            }
            Self::InvalidParams { message } => {
                write!(f, "JSON-RPC -32602 invalid params: {message}")
            }
            Self::InternalError { message } => {
                write!(f, "JSON-RPC -32603 internal error: {message}")
            }
            Self::ServerError { message, data_err } => match data_err {
                Some(err) => write!(
                    f,
                    "JSON-RPC -32000 server error: {message} (data.err={err})"
                ),
                None => write!(f, "JSON-RPC -32000 server error: {message}"),
            },
            Self::UnknownPayload { message } => {
                write!(f, "JSON-RPC -38001 unknown payload: {message}")
            }
            Self::InvalidForkchoiceState { message } => {
                write!(f, "JSON-RPC -38002 invalid forkchoice state: {message}")
            }
            Self::InvalidPayloadAttributes { message } => {
                write!(f, "JSON-RPC -38003 invalid payload attributes: {message}")
            }
            Self::TooLargeRequest { message } => {
                write!(f, "JSON-RPC -38004 too large request: {message}")
            }
            Self::UnsupportedFork { message } => {
                write!(f, "JSON-RPC -38005 unsupported fork: {message}")
            }
            Self::InvalidRange { message } => {
                write!(f, "JSON-RPC -38006 invalid range: {message}")
            }
            Self::Http401 { body } => write!(f, "HTTP 401 auth rejected: {body}"),
            Self::Http403 { body } => write!(f, "HTTP 403 host rejected: {body}"),
            Self::HttpClient { code, body } => write!(f, "HTTP {code} client error: {body}"),
            Self::HttpServer { code, body } => write!(f, "HTTP {code} server error: {body}"),
            Self::Timeout { method } => write!(f, "transport timeout on {method}"),
            Self::Transport { detail } => write!(f, "transport error: {detail}"),
            Self::Decode { reason } => write!(f, "decode error: {reason}"),
            Self::Other { code, message } => {
                write!(f, "JSON-RPC {code} (other): {message}")
            }
        }
    }
}

impl std::error::Error for EngineError {}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::ErrorCode;

    /// One case per closed `code` label: typed variant, metric label, retry class.
    /// Exactly two codes are retryable (`-32603`, `-32000`).
    #[test]
    fn error_taxonomy_table() {
        let cases: Vec<(EngineError, ErrorCode, RetryClass)> = vec![
            (
                EngineError::ParseError {
                    message: "p".into(),
                },
                ErrorCode::ParseError,
                RetryClass::Fatal,
            ),
            (
                EngineError::InvalidRequest {
                    message: "p".into(),
                },
                ErrorCode::InvalidRequest,
                RetryClass::Fatal,
            ),
            (
                EngineError::MethodNotFound {
                    message: "p".into(),
                },
                ErrorCode::MethodNotFound,
                RetryClass::Fatal,
            ),
            (
                EngineError::InvalidParams {
                    message: "p".into(),
                },
                ErrorCode::InvalidParams,
                RetryClass::Fatal,
            ),
            (
                EngineError::InternalError {
                    message: "p".into(),
                },
                ErrorCode::InternalError,
                RetryClass::Transient,
            ),
            (
                EngineError::ServerError {
                    message: "p".into(),
                    data_err: Some("boom".into()),
                },
                ErrorCode::ServerError,
                RetryClass::Transient,
            ),
            (
                EngineError::UnknownPayload {
                    message: "p".into(),
                },
                ErrorCode::UnknownPayload,
                RetryClass::Fatal,
            ),
            (
                EngineError::InvalidForkchoiceState {
                    message: "p".into(),
                },
                ErrorCode::InvalidForkchoiceState,
                RetryClass::Fatal,
            ),
            (
                EngineError::InvalidPayloadAttributes {
                    message: "p".into(),
                },
                ErrorCode::InvalidPayloadAttributes,
                RetryClass::Fatal,
            ),
            (
                EngineError::TooLargeRequest {
                    message: "p".into(),
                },
                ErrorCode::TooLargeRequest,
                RetryClass::Fatal,
            ),
            (
                EngineError::UnsupportedFork {
                    message: "p".into(),
                },
                ErrorCode::UnsupportedFork,
                RetryClass::Fatal,
            ),
            (
                EngineError::InvalidRange {
                    message: "p".into(),
                },
                ErrorCode::InvalidRange,
                RetryClass::Fatal,
            ),
            (
                EngineError::Http401 {
                    body: "missing token".into(),
                },
                ErrorCode::Http401,
                RetryClass::Auth,
            ),
            (
                EngineError::Http403 {
                    body: "invalid host specified".into(),
                },
                ErrorCode::Http403,
                RetryClass::Auth,
            ),
            (
                EngineError::HttpClient {
                    code: 404,
                    body: "no".into(),
                },
                ErrorCode::Http4xx,
                RetryClass::Fatal,
            ),
            (
                EngineError::HttpServer {
                    code: 503,
                    body: "no".into(),
                },
                ErrorCode::Http5xx,
                RetryClass::Fatal,
            ),
            (
                EngineError::Timeout {
                    method: "newPayloadV4".into(),
                },
                ErrorCode::Timeout,
                RetryClass::Fatal,
            ),
            (
                EngineError::Transport {
                    detail: "reset".into(),
                },
                ErrorCode::Transport,
                RetryClass::Fatal,
            ),
            (
                EngineError::Decode {
                    reason: "cap".into(),
                },
                ErrorCode::Decode,
                RetryClass::Fatal,
            ),
            (
                EngineError::Other {
                    code: -99999,
                    message: "x".into(),
                },
                ErrorCode::Other,
                RetryClass::Fatal,
            ),
        ];

        assert_eq!(cases.len(), ErrorCode::ALL.len());

        let mut transient = 0usize;
        for (err, code, class) in &cases {
            assert_eq!(err.metric_code(), *code, "metric for {err:?}");
            assert_eq!(err.retry_class(), *class, "retry for {err:?}");
            if *class == RetryClass::Transient {
                transient += 1;
            }
        }
        assert_eq!(
            transient, 2,
            "exactly two codes are retryable (-32603, -32000)"
        );

        // Round-trip from_jsonrpc for the JSON-RPC numeric set.
        for (code, expected) in [
            (-32700, ErrorCode::ParseError),
            (-32600, ErrorCode::InvalidRequest),
            (-32601, ErrorCode::MethodNotFound),
            (-32602, ErrorCode::InvalidParams),
            (-32603, ErrorCode::InternalError),
            (-32000, ErrorCode::ServerError),
            (-38001, ErrorCode::UnknownPayload),
            (-38002, ErrorCode::InvalidForkchoiceState),
            (-38003, ErrorCode::InvalidPayloadAttributes),
            (-38004, ErrorCode::TooLargeRequest),
            (-38005, ErrorCode::UnsupportedFork),
            (-38006, ErrorCode::InvalidRange),
            (-42, ErrorCode::Other),
        ] {
            let err = EngineError::from_jsonrpc(code, "m".into(), None);
            assert_eq!(err.metric_code(), expected);
        }

        // -32000 data.err appears in Display (and therefore in emitted logs).
        let with_err = EngineError::ServerError {
            message: "server".into(),
            data_err: Some("geth said no".into()),
        };
        let rendered = with_err.to_string();
        assert!(
            rendered.contains("geth said no"),
            "data.err must appear in log-facing Display: {rendered}"
        );
    }

    /// 401 and 403 are distinct variants at the type level, not just metric labels
    /// (CC-30b). A match arm on one must not accept the other.
    #[test]
    fn auth_errors_are_distinct_variants() {
        let e401 = EngineError::Http401 {
            body: "stale token".into(),
        };
        let e403 = EngineError::Http403 {
            body: "invalid host specified".into(),
        };

        assert_ne!(e401, e403);
        assert_eq!(e401.metric_code(), ErrorCode::Http401);
        assert_eq!(e403.metric_code(), ErrorCode::Http403);
        assert_eq!(e401.retry_class(), RetryClass::Auth);
        assert_eq!(e403.retry_class(), RetryClass::Auth);

        let is_401_only = matches!(e401, EngineError::Http401 { .. });
        let is_403_only = matches!(e403, EngineError::Http403 { .. });
        assert!(is_401_only);
        assert!(is_403_only);
        // Cross-match: 401 does not accept 403 and vice versa.
        assert!(!matches!(e401, EngineError::Http403 { .. }));
        assert!(!matches!(e403, EngineError::Http401 { .. }));

        // from_http_status maps to the right variant.
        assert!(matches!(
            EngineError::from_http_status(401, "stale token".into()),
            EngineError::Http401 { body } if body == "stale token"
        ));
        assert!(matches!(
            EngineError::from_http_status(403, "invalid host specified".into()),
            EngineError::Http403 { body } if body == "invalid host specified"
        ));
    }
}
