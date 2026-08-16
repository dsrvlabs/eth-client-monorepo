//! CC-32b /3 — three `-32602 Invalid params` negative tests against a real geth.
//!
//! Offline unit equivalents live in `methods/new_payload.rs` (`requests_*_is_32602`).
//! These container tests are `#[ignore]` (same gate as CC-30b auth tests) and
//! require a reachable Engine API + JWT:
//!
//! ```text
//! cargo nextest run -p cc-engine -E 'test(requests_misordered_is_32602)' --run-ignored all
//! cargo nextest run -p cc-engine -E 'test(requests_one_byte_element_is_32602)' --run-ignored all
//! cargo nextest run -p cc-engine -E 'test(requests_duplicate_type_is_32602)' --run-ignored all
//! ```
//!
//! Host reachability: 8551 is not published on compose `el` by default (CC-39a).
//! Publish a proxy or run throwaway geth as documented in `auth_container.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::time::Duration;

use cc_engine::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
use cc_engine::errors::EngineError;
use cc_engine::jwt::JwtSecret;
use cc_engine::methods::new_payload::new_payload_v4;
use cc_engine::metrics::{EngineMetrics, ErrorCode};
use cc_engine::transport::EngineTransport;
use cc_engine::version::ElForkSchedule;
use cc_types::execution::ExecutionPayload;
use cc_types::preset::Mainnet;
use prometheus_client::registry::Registry;
use ssz::Encode;

const DEFAULT_EL_ENDPOINT: &str = "http://127.0.0.1:8551";

fn jwt_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("services/engine has a two-level parent");
    let path = repo_root.join("secrets/jwt.hex");
    path.canonicalize().unwrap_or(path)
}

fn transport_with_metrics() -> (EngineTransport, EngineMetrics) {
    let mut registry = Registry::default();
    let metrics = EngineMetrics::register(&mut registry);
    let jwt = JwtSecret::load(&jwt_path()).expect("JWT secret");
    let t = EngineTransport::from_secret_bytes(
        DEFAULT_EL_ENDPOINT,
        jwt.as_bytes(),
        TransportTimeouts::from_knobs(&TimeoutKnobs {
            new_payload_ms: 5_000,
            forkchoice_updated_ms: 5_000,
            get_blobs_ms: 2_000,
            exchange_capabilities_ms: 2_000,
            eth_syncing_ms: 2_000,
            multiplier: 1.0,
        }),
        Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
        Some(metrics.clone()),
    );
    (t, metrics)
}

fn schedule() -> ElForkSchedule {
    // Osaka already active on Hoodi; use zero so V4 is selected for any timestamp.
    ElForkSchedule {
        osaka_time: 0,
        bpo1_time: None,
        bpo2_time: None,
        amsterdam_time: None,
    }
}

fn empty_payload_ssz() -> Vec<u8> {
    ExecutionPayload::<Mainnet>::default().as_ssz_bytes()
}

async fn expect_32602(requests: Vec<Vec<u8>>, label: &str) {
    let (t, metrics) = transport_with_metrics();
    let before = metrics.errors_total_count(ErrorCode::InvalidParams);
    let err = new_payload_v4(
        &t,
        &schedule(),
        Some(&metrics),
        &empty_payload_ssz(),
        &[],
        &[0u8; 32],
        &requests,
    )
    .await
    .expect_err(label);
    assert!(
        matches!(err, EngineError::InvalidParams { .. }),
        "{label}: expected InvalidParams (-32602), got {err:?}"
    );
    let after = metrics.errors_total_count(ErrorCode::InvalidParams);
    assert!(
        after > before,
        "{label}: cc_engine_errors_total{{code=\"-32602\"}} must +1 (before={before} after={after})"
    );
}

/// Mis-ordered executionRequests (type 0x02 before 0x00) → geth -32602.
#[tokio::test]
#[ignore = "requires real geth Engine API on 127.0.0.1:8551"]
async fn requests_misordered_is_32602() {
    // consolidation (0x02) before deposit (0x00) — descending type order.
    let requests = vec![vec![0x02, 0x00], vec![0x00, 0x00]];
    expect_32602(requests, "misordered").await;
}

/// A ≤1-byte element (type only, no SSZ body) → geth -32602.
#[tokio::test]
#[ignore = "requires real geth Engine API on 127.0.0.1:8551"]
async fn requests_one_byte_element_is_32602() {
    let requests = vec![vec![0x00]]; // type byte only
    expect_32602(requests, "one_byte_element").await;
}

/// Duplicated request type → geth -32602.
#[tokio::test]
#[ignore = "requires real geth Engine API on 127.0.0.1:8551"]
async fn requests_duplicate_type_is_32602() {
    let requests = vec![vec![0x00, 0xaa], vec![0x00, 0xbb]];
    expect_32602(requests, "duplicate_type").await;
}
