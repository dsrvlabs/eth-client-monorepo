//! CC-30b — transport against a **real** geth auth endpoint.
//!
//! Venue: the compose `el` service (`ethereum/client-go`, same auth flags as
//! `docker-compose.yml`). These tests are `#[ignore]` by default so the
//! 10-minute suite stays offline; run with:
//!
//! ```text
//! cargo nextest run -p cc-engine -E 'test(jwt_iat_stale)' --run-ignored all
//! cargo nextest run -p cc-engine -E 'test(jwt_iat_future)' --run-ignored all
//! cargo nextest run -p cc-engine -E 'test(vhost_rejected_is_403_not_401)' --run-ignored all
//! ```
//!
//! **Host reachability.** Port 8551 is intentionally **not** published on the
//! compose `el` service (CC-39a). For host-side IT, publish it only for the
//! test window, e.g.:
//!
//! ```text
//! docker run -d --rm --name cc30b-auth-proxy \
//!   --network <project>_cc -p 127.0.0.1:8551:8551 \
//!   alpine/socat TCP-LISTEN:8551,fork,reuseaddr TCP:el:8551
//! ```
//!
//! Or run a throwaway geth with the same `--authrpc.*` flags and the same
//! `secrets/jwt.hex`. Endpoint defaults to `http://127.0.0.1:8551`; JWT path
//! defaults to `secrets/jwt.hex` (repo root relative to cargo CWD).
//!
//! No process-environment reads here (CC-09/3 / check-no-env-reads).
//! The gate is `#[ignore]` + `--run-ignored all` (CC-30b / CC-3K /4).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_engine::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
use cc_engine::errors::EngineError;
use cc_engine::jwt::JwtSecret;
use cc_engine::methods::names;
use cc_engine::metrics::{EngineMethod, EngineMetrics, ErrorCode};
use cc_engine::transport::{EngineTransport, Lane, RequestOverrides};
use prometheus_client::registry::Registry;
use serde_json::json;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry as TracingRegistry;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;

/// Host-side Engine API endpoint used by ignored container tests.
const DEFAULT_EL_ENDPOINT: &str = "http://127.0.0.1:8551";
/// Compose default vhosts value — IP Hosts are accepted; this hostname is not.
const REJECTED_VHOST: &str = "not-in-vhosts.invalid";

fn jwt_path() -> PathBuf {
    // nextest/cargo may not use the repo root as CWD. Resolve via the crate
    // manifest dir (compile-time `env!`, not a runtime process-environment read).
    // Canonicalise so the path has no `..` components — JwtSecret::load rejects them.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent() // services/
        .and_then(|p| p.parent()) // repo root
        .expect("services/engine has a two-level parent");
    let path = repo_root.join("secrets/jwt.hex");
    path.canonicalize().unwrap_or(path)
}

fn load_jwt() -> JwtSecret {
    let path = jwt_path();
    JwtSecret::load(&path).unwrap_or_else(|e| {
        panic!(
            "JWT secret load failed at {}: {e}\n\
             Generate with: mkdir -p secrets && openssl rand -hex 32 > secrets/jwt.hex && chmod 0600 secrets/jwt.hex\n\
             Mount the same file into geth as --authrpc.jwtsecret=/jwt/jwt.hex",
            path.display()
        )
    })
}

fn test_timeouts() -> TransportTimeouts {
    TransportTimeouts::from_knobs(&TimeoutKnobs {
        new_payload_ms: 5_000,
        forkchoice_updated_ms: 5_000,
        get_blobs_ms: 2_000,
        exchange_capabilities_ms: 2_000,
        eth_syncing_ms: 2_000,
        multiplier: 1.0,
    })
}

fn transport_with_metrics() -> (EngineTransport, EngineMetrics) {
    let jwt = load_jwt();
    let mut registry = Registry::default();
    let metrics = EngineMetrics::register(&mut registry);
    let t = EngineTransport::from_secret_bytes(
        DEFAULT_EL_ENDPOINT,
        jwt.as_bytes(),
        test_timeouts(),
        Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
        Some(metrics.clone()),
    );
    (t, metrics)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

/// Minimal Engine API method already available without CC-31 method adapters.
async fn upcheck_call(
    t: &EngineTransport,
    overrides: RequestOverrides,
) -> Result<serde_json::Value, EngineError> {
    t.call_with(
        Lane::Upcheck,
        EngineMethod::ExchangeCapabilities,
        names::EXCHANGE_CAPABILITIES,
        json!([["engine_newPayloadV4"]]),
        overrides,
    )
    .await
}

fn error_count(m: &EngineMetrics, code: ErrorCode) -> u64 {
    m.errors_total_count(code)
}

// ── Buffer writer for tracing capture (403 body verbatim) ───────────────────

#[derive(Clone, Debug)]
struct BufferWriter {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().extend_from_slice(buf);
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

/// CC-30 /2 — `iat = now − 120 s` ⇒ HTTP 401 body `stale token`.
///
/// geth applies `jwtExpiryTimeout = 60 s` **symmetrically**; a client that only
/// tests the future direction discovers the past during a clock-drift incident.
#[tokio::test]
#[ignore = "requires real geth auth endpoint on 127.0.0.1:8551 (compose el)"]
async fn jwt_iat_stale() {
    let (t, metrics) = transport_with_metrics();
    let before_401 = error_count(&metrics, ErrorCode::Http401);
    let before_403 = error_count(&metrics, ErrorCode::Http403);

    let iat = now_secs().saturating_sub(120);
    let err = upcheck_call(
        &t,
        RequestOverrides {
            iat: Some(iat),
            host: None,
        },
    )
    .await
    .expect_err("stale iat must 401");

    match &err {
        EngineError::Http401 { body } => {
            assert_eq!(
                body.trim(),
                "stale token",
                "geth body must be asserted literally, not paraphrased; got {body:?}"
            );
        }
        other => panic!("expected Http401, got {other:?}"),
    }

    assert_eq!(
        error_count(&metrics, ErrorCode::Http401),
        before_401 + 1,
        "http_401 must increment by exactly 1"
    );
    assert_eq!(
        error_count(&metrics, ErrorCode::Http403),
        before_403,
        "http_403 must be unchanged on a 401 path"
    );
}

/// CC-30 /2 — `iat = now + 120 s` ⇒ HTTP 401 body `future token`.
#[tokio::test]
#[ignore = "requires real geth auth endpoint on 127.0.0.1:8551 (compose el)"]
async fn jwt_iat_future() {
    let (t, metrics) = transport_with_metrics();
    let before_401 = error_count(&metrics, ErrorCode::Http401);
    let before_403 = error_count(&metrics, ErrorCode::Http403);

    let iat = now_secs().saturating_add(120);
    let err = upcheck_call(
        &t,
        RequestOverrides {
            iat: Some(iat),
            host: None,
        },
    )
    .await
    .expect_err("future iat must 401");

    match &err {
        EngineError::Http401 { body } => {
            assert_eq!(
                body.trim(),
                "future token",
                "geth body must be asserted literally, not paraphrased; got {body:?}"
            );
        }
        other => panic!("expected Http401, got {other:?}"),
    }

    assert_eq!(
        error_count(&metrics, ErrorCode::Http401),
        before_401 + 1,
        "http_401 must increment by exactly 1"
    );
    assert_eq!(
        error_count(&metrics, ErrorCode::Http403),
        before_403,
        "http_403 must be unchanged on a 401 path"
    );
}

/// CC-30 /3 + CC-39 /2 — valid token + unknown Host ⇒ 403, not 401.
///
/// The token is **valid**; only the Host is wrong. That is the property that
/// stops operators from chasing JWT for an hour (PRD *Container-Auth Trap*).
#[tokio::test]
#[ignore = "requires real geth auth endpoint on 127.0.0.1:8551 (compose el)"]
async fn vhost_rejected_is_403_not_401() {
    let (t, metrics) = transport_with_metrics();
    let before_401 = error_count(&metrics, ErrorCode::Http401);
    let before_403 = error_count(&metrics, ErrorCode::Http403);

    let buf = Arc::new(Mutex::new(Vec::new()));
    let writer = BufferWriter {
        inner: Arc::clone(&buf),
    };
    let subscriber = TracingRegistry::default()
        .with(EnvFilter::new("error"))
        .with(fmt::layer().with_writer(writer).with_ansi(false));

    let err = {
        let _guard = tracing::subscriber::set_default(subscriber);
        upcheck_call(
            &t,
            RequestOverrides {
                iat: None, // valid now
                host: Some(REJECTED_VHOST.to_owned()),
            },
        )
        .await
        .expect_err("unknown Host must 403")
    };

    match &err {
        EngineError::Http403 { body } => {
            assert_eq!(
                body.trim(),
                "invalid host specified",
                "geth body must be asserted literally; got {body:?}"
            );
        }
        EngineError::Http401 { body } => {
            panic!(
                "vhost rejection must be Http403, not Http401 (body={body:?}); \
                 treating 403 as auth failed is the Container-Auth Trap"
            );
        }
        other => panic!("expected Http403, got {other:?}"),
    }

    assert_eq!(
        error_count(&metrics, ErrorCode::Http403),
        before_403 + 1,
        "http_403 must increment by exactly 1"
    );
    assert_eq!(
        error_count(&metrics, ErrorCode::Http401),
        before_401,
        "http_401 must be unchanged on a 403 path"
    );

    // geth's plain-text body is logged verbatim at error! (not paraphrased).
    let captured = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
    assert!(
        captured.contains("invalid host specified"),
        "error! log must contain geth body verbatim; capture was:\n{captured}"
    );
    // S-2: 401/403 error! path must not leak compact JWS or Authorization material.
    assert!(
        !captured.contains("eyJ"),
        "JWS compact form must not appear in auth-failure logs:\n{captured}"
    );
    assert!(
        !captured.to_ascii_lowercase().contains("authorization"),
        "Authorization header must not appear in auth-failure logs:\n{captured}"
    );
}

/// Offline half of the type-level distinction (also covered in errors unit tests).
/// Named so nextest `-E 'test(auth_errors_are_distinct_variants)'` hits either site.
#[test]
fn auth_errors_are_distinct_variants() {
    let e401 = EngineError::Http401 {
        body: "stale token".into(),
    };
    let e403 = EngineError::Http403 {
        body: "invalid host specified".into(),
    };
    assert!(!matches!(e401, EngineError::Http403 { .. }));
    assert!(!matches!(e403, EngineError::Http401 { .. }));
    assert_eq!(e401.metric_code(), ErrorCode::Http401);
    assert_eq!(e403.metric_code(), ErrorCode::Http403);
}

/// Sanity: with a fresh token and no Host override, a live EL answers 2xx
/// (or a JSON-RPC method error — not HTTP 401/403). Proves the secret matches.
#[tokio::test]
#[ignore = "requires real geth auth endpoint on 127.0.0.1:8551 (compose el)"]
async fn valid_token_is_not_auth_rejected() {
    let (t, _metrics) = transport_with_metrics();
    let result = upcheck_call(&t, RequestOverrides::default()).await;
    match result {
        Ok(_) => {}
        Err(EngineError::Http401 { body }) | Err(EngineError::Http403 { body }) => {
            panic!("valid token must not HTTP-auth-fail; body={body:?}");
        }
        Err(EngineError::Transport { detail }) => {
            panic!(
                "transport error talking to {DEFAULT_EL_ENDPOINT}: {detail}\n\
                 Is geth up with --authrpc.addr=0.0.0.0 and 8551 reachable from the host?"
            );
        }
        Err(other) => {
            // JSON-RPC errors (method not found etc.) still prove auth passed.
            eprintln!("auth ok; RPC-level error (acceptable for this probe): {other}");
        }
    }
}
