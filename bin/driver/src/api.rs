//! Beacon HTTP API client (Architecture §9.1, CC-1Aa).
//!
//! The driver never decodes SSZ: block bodies are forwarded as opaque bytes.
//! Roots and parent roots come from JSON header metadata only.
//!
//! # Provider URL policy (SEC-1Aa-3)
//!
//! Mirrors chain checkpoint client hardening: production bases must be
//! `https://`; `http://` is allowed only to loopback hosts (offline tests).
//! Redirects that leave that scheme/host set are refused.

use std::time::Duration;

use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;

/// Connect timeout for beacon-API requests.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Total request timeout for a single block/header fetch (not a state).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Hard cap on a single signed-block SSZ body (aligned with chain checkpoint).
const MAX_BLOCK_BYTES: usize = 8 * 1024 * 1024;

/// Hard cap on JSON header responses.
const MAX_JSON_BYTES: usize = 2 * 1024 * 1024;

/// Beacon-API client errors.
#[derive(Debug)]
pub(crate) enum ApiError {
    /// Transport / HTTP / decode failure.
    Provider { provider: String, reason: String },
    /// HTTP 429 / 503 — politeness path (CC-1Ab §9.5).
    RateLimited {
        provider: String,
        /// HTTP status (429 or 503).
        status: u16,
        /// Parsed `Retry-After` (seconds form), when present.
        retry_after: Option<Duration>,
    },
    /// Hex root could not be parsed.
    BadHex { field: String, value: String },
    /// Unexpected JSON shape.
    Json { reason: String },
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider { provider, reason } => {
                write!(f, "provider {provider}: {reason}")
            }
            Self::RateLimited {
                provider,
                status,
                retry_after,
            } => match retry_after {
                Some(d) => write!(
                    f,
                    "provider {provider}: HTTP {status} rate-limited (Retry-After {}s)",
                    d.as_secs()
                ),
                None => write!(f, "provider {provider}: HTTP {status} rate-limited"),
            },
            Self::BadHex { field, value } => {
                write!(f, "bad hex for {field}: {value}")
            }
            Self::Json { reason } => write!(f, "json: {reason}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    /// True for failures that should drive backoff + rotation in [`crate::ratelimit::ProviderPool`].
    ///
    /// Retryable: `429`/`503` ([`Self::RateLimited`]), transport errors, and other
    /// 5xx. **Not** retryable: terminal 4xx (404/400/…) so walk-back parent misses
    /// surface to abandon instead of stalling the steady loop (SEC-1Ab-1).
    pub(crate) fn is_retryable_provider(&self) -> bool {
        match self {
            Self::RateLimited { .. } => true,
            Self::Provider { reason, .. } => match parse_http_status_from_reason(reason) {
                // Permanent client errors — return to caller (walk-back / steady).
                Some(code) if (400..500).contains(&code) => false,
                // 5xx (except 503, which is RateLimited) and transport (no code).
                Some(_) | None => true,
            },
            Self::BadHex { .. } | Self::Json { .. } => false,
        }
    }

    /// True when the error is a terminal HTTP client error (e.g. 404) that must
    /// not be infinite-retried by the provider pool.
    pub(crate) fn is_terminal_http(&self) -> bool {
        match self {
            Self::Provider { reason, .. } => matches!(
                parse_http_status_from_reason(reason),
                Some(code) if (400..500).contains(&code)
            ),
            _ => false,
        }
    }

    /// HTTP status code for metrics labels, when known.
    pub(crate) fn status_code(&self) -> Option<u16> {
        match self {
            Self::RateLimited { status, .. } => Some(*status),
            Self::Provider { reason, .. } => parse_http_status_from_reason(reason),
            _ => None,
        }
    }

    /// `Retry-After` when this is a rate-limit response.
    pub(crate) fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

fn parse_http_status_from_reason(reason: &str) -> Option<u16> {
    // Reasons look like `GET {url}: HTTP 502` or `GET {url}: HTTP 404`.
    let idx = reason.rfind("HTTP ")?;
    let rest = &reason[idx + 5..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Header metadata from `/eth/v1/beacon/headers/{id}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockHeader {
    /// `hash_tree_root(block.message)` as 32 bytes.
    pub root: Vec<u8>,
    /// Slot from the header message.
    pub slot: u64,
    /// Parent block root as 32 bytes.
    pub parent_root: Vec<u8>,
}

/// A block ready for `ImportBlock` — SSZ body + API metadata, no SSZ decode.
#[derive(Debug, Clone)]
pub(crate) struct FetchedBlock {
    /// Slot the fetch targeted (and header reported).
    pub slot: u64,
    /// Block root from the header response.
    pub root: Vec<u8>,
    /// Parent root from the header response (walk-back entry).
    pub parent_root: Vec<u8>,
    /// `SignedBeaconBlock` SSZ bytes, forwarded verbatim.
    pub ssz: Bytes,
    /// Mapped `Eth-Consensus-Version` (or config default).
    pub fork: u32,
}

/// Genesis payload from `/eth/v1/beacon/genesis` (slot clock only; no SSZ).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GenesisInfo {
    /// Unix genesis time (seconds).
    pub genesis_time: u64,
}

/// HTTP client against one beacon-API base URL.
#[derive(Debug, Clone)]
pub(crate) struct BeaconApiClient {
    http: reqwest::Client,
    base: String,
    /// Fallback fork tag when `Eth-Consensus-Version` is absent.
    default_fork: u32,
}

impl BeaconApiClient {
    /// Build a client for `base` (e.g. `https://beacon.example`).
    ///
    /// Rejects non-HTTPS non-loopback bases and installs a redirect policy that
    /// only follows targets still allowed by [`validate_provider_base`]
    /// (SEC-1Aa-3 / checkpoint_sync parity).
    pub(crate) fn new(base: impl Into<String>, default_fork: u32) -> Result<Self, ApiError> {
        let base = base.into();
        validate_provider_base(&base)?;
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if provider_scheme_allowed(attempt.url()) {
                    attempt.follow()
                } else {
                    let target = attempt.url().to_string();
                    attempt.error(format!(
                        "refusing redirect to disallowed scheme/host {target}"
                    ))
                }
            }))
            .build()
            .map_err(|e| ApiError::Provider {
                provider: base.clone(),
                reason: format!("http client: {e}"),
            })?;
        Ok(Self {
            http,
            base,
            default_fork,
        })
    }

    /// Base URL this client targets (for metrics / rotation logs).
    pub(crate) fn base(&self) -> &str {
        &self.base
    }

    /// `GET /eth/v1/beacon/headers/head`.
    pub(crate) async fn get_head_header(&self) -> Result<BlockHeader, ApiError> {
        self.get_header("head").await
    }

    /// `GET /eth/v1/beacon/headers/{block_id}` (slot, root, or `head`).
    ///
    /// Returns [`ApiError`] with reason containing `HTTP 404` on empty slots so
    /// callers can treat 404 as skip without special-casing the type.
    pub(crate) async fn get_header(&self, block_id: &str) -> Result<BlockHeader, ApiError> {
        let url = join_url(&self.base, &format!("/eth/v1/beacon/headers/{block_id}"));
        let text = self.get_json(&url).await?;
        parse_header_json(&text)
    }

    /// `GET /eth/v1/beacon/genesis` — `genesis_time` for the slot clock (§9.3).
    pub(crate) async fn get_genesis(&self) -> Result<GenesisInfo, ApiError> {
        let url = join_url(&self.base, "/eth/v1/beacon/genesis");
        let text = self.get_json(&url).await?;
        parse_genesis_json(&text)
    }

    /// Fetch header + SSZ body for a slot.
    ///
    /// `Ok(None)` means the slot is empty (HTTP 404 on the header or block).
    /// Rejects a header whose `message.slot` does not match the requested slot
    /// (cheap provider-fidelity check; no SSZ decode).
    pub(crate) async fn fetch_slot(&self, slot: u64) -> Result<Option<FetchedBlock>, ApiError> {
        let header = match self.get_header(&slot.to_string()).await {
            Ok(h) => h,
            Err(ApiError::Provider { reason, .. }) if reason.contains("HTTP 404") => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        if header.slot != slot {
            return Err(ApiError::Provider {
                provider: self.base.clone(),
                reason: format!(
                    "header slot mismatch: requested {slot}, header.message.slot={}",
                    header.slot
                ),
            });
        }

        let (version, ssz) = match self.get_block_ssz(&slot.to_string()).await {
            Ok(v) => v,
            Err(ApiError::Provider { reason, .. }) if reason.contains("HTTP 404") => {
                // Header present but block 404 — treat as empty (provider race).
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let fork = map_consensus_version(&version).unwrap_or(self.default_fork);
        Ok(Some(FetchedBlock {
            slot,
            root: header.root,
            parent_root: header.parent_root,
            ssz,
            fork,
        }))
    }

    /// Fetch header + SSZ body by block id (slot string, `0x` root, or `head`).
    ///
    /// Used by walk-back (`parent_root`) and steady-state head import. Unlike
    /// [`Self::fetch_slot`], does not require the id to be a slot number.
    pub(crate) async fn fetch_by_id(&self, block_id: &str) -> Result<FetchedBlock, ApiError> {
        let header = self.get_header(block_id).await?;
        let (version, ssz) = self.get_block_ssz(block_id).await?;
        let fork = map_consensus_version(&version).unwrap_or(self.default_fork);
        Ok(FetchedBlock {
            slot: header.slot,
            root: header.root,
            parent_root: header.parent_root,
            ssz,
            fork,
        })
    }

    /// `GET /eth/v2/beacon/blocks/{id}` as `application/octet-stream`.
    ///
    /// Returns `(Eth-Consensus-Version, body)`. Empty version string if the
    /// header is absent (caller applies the config default).
    pub(crate) async fn get_block_ssz(&self, block_id: &str) -> Result<(String, Bytes), ApiError> {
        let url = join_url(&self.base, &format!("/eth/v2/beacon/blocks/{block_id}"));
        let resp = self
            .http
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/octet-stream")
            .send()
            .await
            .map_err(|e| ApiError::Provider {
                provider: self.base.clone(),
                reason: format!("GET {url}: {e}"),
            })?;
        let status = resp.status();
        let version = resp
            .headers()
            .get("eth-consensus-version")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        if let Some(err) = classify_http_status(status, resp.headers(), &self.base, &url) {
            return Err(err);
        }
        let bytes = read_body_capped(resp, MAX_BLOCK_BYTES, &self.base, &url).await?;
        Ok((version, bytes))
    }

    async fn get_json(&self, url: &str) -> Result<String, ApiError> {
        let resp = self.http.get(url).send().await.map_err(|e| ApiError::Provider {
            provider: self.base.clone(),
            reason: format!("GET {url}: {e}"),
        })?;
        let status = resp.status();
        if let Some(err) = classify_http_status(status, resp.headers(), &self.base, url) {
            return Err(err);
        }
        let bytes = read_body_capped(resp, MAX_JSON_BYTES, &self.base, url).await?;
        String::from_utf8(bytes.to_vec()).map_err(|e| ApiError::Provider {
            provider: self.base.clone(),
            reason: format!("GET {url} utf8: {e}"),
        })
    }
}

/// Map non-success HTTP status into [`ApiError`], honouring `Retry-After` on 429/503.
fn classify_http_status(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    provider: &str,
    url: &str,
) -> Option<ApiError> {
    if status.is_success() {
        return None;
    }
    let code = status.as_u16();
    if code == 429 || code == 503 {
        return Some(ApiError::RateLimited {
            provider: provider.to_owned(),
            status: code,
            retry_after: parse_retry_after(headers),
        });
    }
    Some(ApiError::Provider {
        provider: provider.to_owned(),
        reason: format!("GET {url}: HTTP {status}"),
    })
}

/// Parse `Retry-After` as delta-seconds (HTTP-date form is ignored → `None`).
pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)
        .or_else(|| headers.get("retry-after"))?
        .to_str()
        .ok()?
        .trim();
    let secs: u64 = raw.parse().ok()?;
    Some(Duration::from_secs(secs))
}

/// Encode a 32-byte root as `0x` + lowercase hex (block_id for walk-back).
pub(crate) fn encode_root_hex(root: &[u8]) -> String {
    let mut s = String::with_capacity(2 + root.len() * 2);
    s.push_str("0x");
    for b in root {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn parse_genesis_json(text: &str) -> Result<GenesisInfo, ApiError> {
    let v: Value = serde_json::from_str(text).map_err(|e| ApiError::Json {
        reason: format!("genesis decode: {e}"),
    })?;
    let data = v.get("data").ok_or_else(|| ApiError::Json {
        reason: "genesis missing data".into(),
    })?;
    #[derive(Deserialize)]
    struct GenesisWire {
        #[serde(deserialize_with = "deserialize_u64_flexible")]
        genesis_time: u64,
    }
    let wire: GenesisWire = serde_json::from_value(data.clone()).map_err(|e| ApiError::Json {
        reason: format!("genesis fields: {e}"),
    })?;
    Ok(GenesisInfo {
        genesis_time: wire.genesis_time,
    })
}

/// Production providers must be HTTPS. Loopback HTTP is allowed for offline tests.
///
/// Parity with `cc_chain::checkpoint_sync::validate_provider_base` (SEC-1Aa-3 /
/// SEC-19a-3). Kept local so the driver does not take a `cc-chain` edge.
pub(crate) fn validate_provider_base(base: &str) -> Result<(), ApiError> {
    let parsed = reqwest::Url::parse(base).map_err(|e| ApiError::Provider {
        provider: base.to_owned(),
        reason: format!("invalid provider URL: {e}"),
    })?;
    if provider_scheme_allowed(&parsed) {
        Ok(())
    } else {
        Err(ApiError::Provider {
            provider: base.to_owned(),
            reason: format!(
                "provider URL must be https:// (or http:// to loopback for tests); got {}",
                parsed.scheme()
            ),
        })
    }
}

fn provider_scheme_allowed(url: &reqwest::Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => is_loopback_host(url.host_str()),
        _ => false,
    }
}

fn is_loopback_host(host: Option<&str>) -> bool {
    match host {
        Some("localhost") | Some("127.0.0.1") | Some("::1") => true,
        Some(h) => h.starts_with("127."),
        None => false,
    }
}

/// Map `Eth-Consensus-Version` header / config string to `ImportBlockRequest.fork`.
///
/// Phase 1 chain always decodes as Fulu; the table is still complete so a
/// missing/odd header does not invent a silent fork.
pub(crate) fn map_consensus_version(version: &str) -> Option<u32> {
    match version.trim().to_ascii_lowercase().as_str() {
        "phase0" | "base" | "genesis" => Some(0),
        "altair" => Some(1),
        "bellatrix" => Some(2),
        "capella" => Some(3),
        "deneb" => Some(4),
        "electra" => Some(5),
        "fulu" => Some(6),
        "" => None,
        _ => None,
    }
}

/// Parse a 0x-prefixed or bare hex root into 32 bytes.
pub(crate) fn parse_root_hex(s: &str) -> Result<Vec<u8>, ApiError> {
    let raw = s.trim();
    let hex = raw.strip_prefix("0x").unwrap_or(raw);
    if hex.len() != 64 {
        return Err(ApiError::BadHex {
            field: "root".into(),
            value: s.to_owned(),
        });
    }
    let mut out = vec![0u8; 32];
    for i in 0..32 {
        let byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| {
            ApiError::BadHex {
                field: "root".into(),
                value: s.to_owned(),
            }
        })?;
        out[i] = byte;
    }
    Ok(out)
}

fn join_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

async fn read_body_capped(
    resp: reqwest::Response,
    max_bytes: usize,
    provider: &str,
    url: &str,
) -> Result<Bytes, ApiError> {
    use futures::StreamExt;

    if let Some(cl) = resp.content_length()
        && cl as usize > max_bytes
    {
        return Err(ApiError::Provider {
            provider: provider.to_owned(),
            reason: format!("GET {url}: Content-Length {cl} exceeds max {max_bytes} bytes"),
        });
    }

    let mut out = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| ApiError::Provider {
            provider: provider.to_owned(),
            reason: format!("GET {url} body: {e}"),
        })?;
        let next = out.len().saturating_add(chunk.len());
        if next > max_bytes {
            return Err(ApiError::Provider {
                provider: provider.to_owned(),
                reason: format!(
                    "GET {url}: body exceeds max {max_bytes} bytes (got at least {next})"
                ),
            });
        }
        out.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(out))
}

fn parse_header_json(text: &str) -> Result<BlockHeader, ApiError> {
    let v: Value = serde_json::from_str(text).map_err(|e| ApiError::Json {
        reason: format!("header decode: {e}"),
    })?;
    // Spec: single object under data. Tolerate accidental array (one element).
    let data = match v.get("data") {
        Some(Value::Array(arr)) => arr
            .first()
            .ok_or_else(|| ApiError::Json {
                reason: "headers data array empty".into(),
            })?,
        Some(obj) => obj,
        None => {
            return Err(ApiError::Json {
                reason: "missing data field".into(),
            });
        }
    };

    #[derive(Deserialize)]
    struct HeaderWire {
        root: String,
        header: HeaderInner,
    }
    #[derive(Deserialize)]
    struct HeaderInner {
        message: HeaderMessage,
    }
    #[derive(Deserialize)]
    struct HeaderMessage {
        #[serde(deserialize_with = "deserialize_u64_flexible")]
        slot: u64,
        parent_root: String,
    }

    let wire: HeaderWire = serde_json::from_value(data.clone()).map_err(|e| ApiError::Json {
        reason: format!("header fields: {e}"),
    })?;
    Ok(BlockHeader {
        root: parse_root_hex(&wire.root)?,
        slot: wire.header.message.slot,
        parent_root: parse_root_hex(&wire.header.message.parent_root)?,
    })
}

fn deserialize_u64_flexible<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Value::deserialize(deserializer)?;
    match v {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("slot not u64")),
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|e| serde::de::Error::custom(format!("slot string: {e}"))),
        other => Err(serde::de::Error::custom(format!(
            "slot must be number or string, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn map_fork_table() {
        assert_eq!(map_consensus_version("fulu"), Some(6));
        assert_eq!(map_consensus_version("FULU"), Some(6));
        assert_eq!(map_consensus_version("deneb"), Some(4));
        assert_eq!(map_consensus_version(""), None);
        assert_eq!(map_consensus_version("unknown"), None);
    }

    #[test]
    fn parse_root_roundtrip() {
        let hex = "0x710c1a2cd5c0d0e62531aa577b160455a90e3a2fd6ded68c8f62b62da4728242";
        let bytes = parse_root_hex(hex).unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes[0], 0x71);
        assert_eq!(parse_root_hex(&hex[2..]).unwrap(), bytes);
    }

    #[test]
    fn parse_header_object_shape() {
        let json = r#"{
          "data": {
            "root": "0x710c1a2cd5c0d0e62531aa577b160455a90e3a2fd6ded68c8f62b62da4728242",
            "canonical": true,
            "header": {
              "message": {
                "slot": "3649433",
                "proposer_index": "1",
                "parent_root": "0xbdbaee55cb59ed2b1452c485523af4014a9e77ca7a494c6d947b2ffa798b24fa",
                "state_root": "0x0000000000000000000000000000000000000000000000000000000000000001",
                "body_root": "0x0000000000000000000000000000000000000000000000000000000000000002"
              },
              "signature": "0x00"
            }
          }
        }"#;
        let h = parse_header_json(json).unwrap();
        assert_eq!(h.slot, 3_649_433);
        assert_eq!(h.root[0], 0x71);
        assert_eq!(h.parent_root[0], 0xbd);
    }

    #[test]
    fn provider_url_https_and_loopback_http_allowed() {
        validate_provider_base("https://checkpoint-sync.hoodi.ethpandaops.io").unwrap();
        validate_provider_base("http://127.0.0.1:9").unwrap();
        validate_provider_base("http://localhost:8080").unwrap();
        validate_provider_base("http://127.1.2.3:1").unwrap();
    }

    #[test]
    fn provider_url_rejects_non_loopback_http_and_odd_schemes() {
        let err = validate_provider_base("http://example.com").unwrap_err();
        assert!(
            err.to_string().contains("https"),
            "expected https rejection, got {err}"
        );
        let err = validate_provider_base("ftp://127.0.0.1").unwrap_err();
        assert!(
            err.to_string().contains("https") || err.to_string().contains("scheme"),
            "got {err}"
        );
        // Client construction must apply the same gate.
        let err = BeaconApiClient::new("http://metadata.internal", 6).unwrap_err();
        assert!(err.to_string().contains("https"), "got {err}");
    }

    #[test]
    fn retryable_classification_terminal_4xx_vs_transient() {
        let not_found = ApiError::Provider {
            provider: "http://127.0.0.1".into(),
            reason: "GET http://x/headers/0xab: HTTP 404".into(),
        };
        assert!(!not_found.is_retryable_provider());
        assert!(not_found.is_terminal_http());

        let bad_req = ApiError::Provider {
            provider: "http://127.0.0.1".into(),
            reason: "GET http://x: HTTP 400".into(),
        };
        assert!(!bad_req.is_retryable_provider());

        let bad_gateway = ApiError::Provider {
            provider: "http://127.0.0.1".into(),
            reason: "GET http://x: HTTP 502".into(),
        };
        assert!(bad_gateway.is_retryable_provider());
        assert!(!bad_gateway.is_terminal_http());

        let transport = ApiError::Provider {
            provider: "http://127.0.0.1".into(),
            reason: "GET http://x: connection refused".into(),
        };
        assert!(transport.is_retryable_provider());

        let rate = ApiError::RateLimited {
            provider: "http://127.0.0.1".into(),
            status: 429,
            retry_after: Some(Duration::from_secs(1)),
        };
        assert!(rate.is_retryable_provider());
        assert!(!rate.is_terminal_http());
    }
}
