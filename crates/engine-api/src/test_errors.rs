//! Test-only error taxonomy matching the transport's call sites.

use std::fmt;

use crate::metrics::ErrorCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryClass {
    Fatal,
    Transient,
    Auth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    ParseError { message: String },
    InvalidRequest { message: String },
    MethodNotFound { message: String },
    InvalidParams { message: String },
    InternalError { message: String },
    ServerError {
        message: String,
        data_err: Option<String>,
    },
    UnknownPayload { message: String },
    InvalidForkchoiceState { message: String },
    InvalidPayloadAttributes { message: String },
    TooLargeRequest { message: String },
    UnsupportedFork { message: String },
    InvalidRange { message: String },
    Http401 { body: String },
    Http403 { body: String },
    HttpClient { code: u16, body: String },
    HttpServer { code: u16, body: String },
    Timeout { method: String },
    Transport { detail: String },
    Decode { reason: String },
    Other { code: i64, message: String },
}

impl EngineError {
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

    #[must_use]
    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::InternalError { .. } | Self::ServerError { .. } => RetryClass::Transient,
            Self::Http401 { .. } | Self::Http403 { .. } => RetryClass::Auth,
            _ => RetryClass::Fatal,
        }
    }

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
