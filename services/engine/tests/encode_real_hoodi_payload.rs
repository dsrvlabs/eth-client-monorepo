//! CC-3C /1 — SSZ→JSON hex encode wall clock on a **real Hoodi** payload.
//!
//! Loads a captured `SignedBeaconBlock` from the Hoodi fixture cache (or
//! `CC_3C_HOODI_BLOCK_SSZ` / `/tmp/hoodi-head-block.ssz`), extracts the
//! `ExecutionPayload`, and prints encode timing + byte sizes for
//! `docs/engine-latency.md`.
//!
//! ```text
//! cargo nextest run -p cc-engine -E 'test(encode_real_hoodi_payload)' --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::time::Instant;

use cc_engine::methods::new_payload::{build_new_payload_v4_params, encode_execution_payload_v3};
use cc_types::execution::ExecutionPayload;
use cc_types::preset::Mainnet;
use cc_types::{ForkName, SignedBeaconBlock};
use ssz::Decode;
use ssz::Encode;

const FETCH_HINT: &str = "run scripts/fetch-hoodi-fixtures.sh";
const ANCHOR_SLOT: u64 = 3_649_472;
/// Largest non-anchor sequence SSZ in the committed pin (byte size on disk).
const PREFERRED_SEQUENCE_SLOT: u64 = 3_649_446;

fn cache_root() -> PathBuf {
    if let Ok(p) = std::env::var("HOODI_FIXTURES_CACHE") {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").expect("HOME must be set for Hoodi fixture cache");
    PathBuf::from(home).join(".cache/cc-hoodi-fixtures")
}

/// True when `bytes` look like raw SSZ (not JSON / HTML error pages).
fn looks_like_ssz(bytes: &[u8]) -> bool {
    bytes.len() > 200 && bytes[0] != b'{' && bytes[0] != b'<'
}

fn load_block_bytes() -> (Vec<u8>, String) {
    if let Ok(p) = std::env::var("CC_3C_HOODI_BLOCK_SSZ") {
        let bytes = std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}"));
        return (bytes, format!("CC_3C_HOODI_BLOCK_SSZ={p}"));
    }
    // Optional live capture (must be raw SSZ — publicnode often returns JSON).
    for (path, label) in [
        (
            PathBuf::from("/tmp/hoodi-head-ethpandaops.ssz"),
            "beacon.hoodi.ethpandaops.io/eth/v2/beacon/blocks/head → /tmp/hoodi-head-ethpandaops.ssz",
        ),
        (
            PathBuf::from("/tmp/hoodi-head-block.ssz"),
            "/tmp/hoodi-head-block.ssz",
        ),
    ] {
        if path.is_file()
            && let Ok(bytes) = std::fs::read(&path)
            && looks_like_ssz(&bytes)
        {
            return (bytes, label.into());
        }
    }
    // Committed pin sequence — largest non-empty block in the 40-slot window.
    let path = cache_root()
        .join(ANCHOR_SLOT.to_string())
        .join("sequence")
        .join(format!("{PREFERRED_SEQUENCE_SLOT}.ssz"));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "Hoodi sequence block missing at {}: {e}; {FETCH_HINT}",
            path.display()
        )
    });
    (
        bytes,
        format!(
            "HOODI fixture cache sequence/{PREFERRED_SEQUENCE_SLOT}.ssz (pin slot {ANCHOR_SLOT})"
        ),
    )
}

fn tx_list_bytes(payload: &ExecutionPayload<Mainnet>) -> usize {
    payload.transactions.iter().map(|t| t.len()).sum()
}

/// CC-3C /1: time SSZ→JSON hex encode of a real Hoodi ExecutionPayloadV3.
#[test]
fn encode_real_hoodi_payload() {
    let (block_ssz, source) = load_block_bytes();
    let signed = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &block_ssz)
        .unwrap_or_else(|e| panic!("decode SignedBeaconBlock: {e:?}; source={source}"));
    let payload = &signed.message.body.execution_payload;
    let block_number = payload.block_number;
    let slot = signed.message.slot.as_u64();
    let payload_ssz = payload.as_ssz_bytes();
    let payload_ssz_len = payload_ssz.len();
    let tx_bytes = tx_list_bytes(payload);
    let n_tx = payload.transactions.len();

    // Warm: one encode discarded so the measured sample is steady-state.
    let _ = encode_execution_payload_v3(payload).expect("warm encode");

    const SAMPLES: usize = 25;
    let mut times_ns = Vec::with_capacity(SAMPLES);
    let mut json_bytes = 0usize;
    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        let v = encode_execution_payload_v3(payload).expect("encode ExecutionPayloadV3");
        let elapsed = t0.elapsed();
        let encoded = serde_json::to_vec(&v).expect("serialize JSON value");
        json_bytes = encoded.len();
        times_ns.push(elapsed.as_nanos() as u64);
    }
    times_ns.sort_unstable();
    let p50 = times_ns[SAMPLES / 2];
    let p95 = times_ns[(SAMPLES as f64 * 0.95) as usize];
    let min = times_ns[0];
    let max = times_ns[SAMPLES - 1];
    let mean: f64 = times_ns.iter().map(|&n| n as f64).sum::<f64>() / SAMPLES as f64;

    // Full newPayloadV4 params build (payload + empty hashes/root/requests) —
    // the span `cc_engine_encode_seconds` wraps in production.
    let parent = [0u8; 32];
    let t0 = Instant::now();
    let params = build_new_payload_v4_params(payload, &[], &parent, &[]).expect("params");
    let params_encode_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let params_json_len = serde_json::to_vec(&params).expect("params json").len();

    // Also time SSZ decode + encode (the production encode_seconds window).
    let mut decode_encode_ns = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        let decoded =
            ExecutionPayload::<Mainnet>::from_ssz_bytes(&payload_ssz).expect("SSZ decode payload");
        let _ = build_new_payload_v4_params(&decoded, &[], &parent, &[]).expect("params");
        decode_encode_ns.push(t0.elapsed().as_nanos() as u64);
    }
    decode_encode_ns.sort_unstable();
    let de_p50 = decode_encode_ns[SAMPLES / 2];
    let de_p95 = decode_encode_ns[(SAMPLES as f64 * 0.95) as usize];

    println!("cc3c_encode_real_hoodi_payload");
    println!("source={source}");
    println!("beacon_slot={slot}");
    println!("el_block_number={block_number}");
    println!("signed_block_ssz_bytes={}", block_ssz.len());
    println!("payload_ssz_bytes={payload_ssz_len}");
    println!("transaction_count={n_tx}");
    println!("transaction_list_bytes={tx_bytes}");
    println!("execution_payload_v3_json_bytes={json_bytes}");
    println!("newpayload_v4_params_json_bytes={params_json_len}");
    println!(
        "encode_execution_payload_v3_ns min={min} p50={p50} mean={mean:.0} p95={p95} max={max} samples={SAMPLES}"
    );
    println!(
        "encode_execution_payload_v3_ms p50={:.4} p95={:.4} mean={:.4}",
        p50 as f64 / 1_000_000.0,
        p95 as f64 / 1_000_000.0,
        mean / 1_000_000.0
    );
    println!(
        "ssz_decode_plus_params_build_ms p50={:.4} p95={:.4} (matches cc_engine_encode_seconds window)",
        de_p50 as f64 / 1_000_000.0,
        de_p95 as f64 / 1_000_000.0
    );
    println!("single_params_build_ms={params_encode_ms:.4}");

    // Sanity: real payload, not the empty default.
    assert!(
        block_number > 0 || n_tx > 0 || payload_ssz_len > 200,
        "expected a non-trivial Hoodi payload (block_number={block_number} n_tx={n_tx} ssz={payload_ssz_len})"
    );
    assert!(json_bytes > 0, "JSON encode produced empty body");
}

/// CC-3C /2 offline span arithmetic: encode + mock HTTP request; hop = (1)−(2)−(3).
///
/// Uses wiremock for the EL so request is measurable without a live geth.
/// The chain→engine gRPC hop is **not** present in this process-local path
/// (same binary); it is reported as ~0 here and as derived-from-live-metrics
/// in the document when histograms have real samples.
#[tokio::test]
async fn latency_spans_wiremock_new_payload() {
    use cc_engine::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
    use cc_engine::methods::new_payload::new_payload_v4;
    use cc_engine::metrics::EngineMethod;
    use cc_engine::metrics::EngineMetrics;
    use cc_engine::transport::{EngineTransport, Lane};
    use cc_engine::version::ElForkSchedule;
    use prometheus_client::registry::Registry;
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let (block_ssz, source) = load_block_bytes();
    let signed = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &block_ssz)
        .unwrap_or_else(|e| panic!("decode: {e:?}"));
    let payload = &signed.message.body.execution_payload;
    let payload_ssz = payload.as_ssz_bytes();
    let block_number = payload.block_number;

    let server = MockServer::start().await;
    Mock::given(http_method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "status": "VALID",
                "latestValidHash": null,
                "validationError": null
            }
        })))
        .mount(&server)
        .await;

    let mut registry = Registry::default();
    let metrics = EngineMetrics::register(&mut registry);
    let transport = EngineTransport::from_secret_bytes(
        server.uri(),
        [0x42; 32],
        TransportTimeouts::from_knobs(&TimeoutKnobs {
            new_payload_ms: 2_000,
            forkchoice_updated_ms: 2_000,
            get_blobs_ms: 1_000,
            exchange_capabilities_ms: 1_000,
            eth_syncing_ms: 1_000,
            multiplier: 1.0,
        }),
        Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
        Some(metrics.clone()),
    );
    let schedule = ElForkSchedule {
        osaka_time: 0,
        bpo1_time: None,
        bpo2_time: None,
        amsterdam_time: None,
    };

    // Warm one call.
    new_payload_v4(
        &transport,
        &schedule,
        Some(&metrics),
        &payload_ssz,
        &[],
        &[0u8; 32],
        &[],
    )
    .await
    .expect("warm new_payload_v4");

    const N: usize = 20;
    let mut inclusive_ms = Vec::with_capacity(N);
    for _ in 0..N {
        let t0 = Instant::now();
        new_payload_v4(
            &transport,
            &schedule,
            Some(&metrics),
            &payload_ssz,
            &[],
            &[0u8; 32],
            &[],
        )
        .await
        .expect("new_payload_v4");
        inclusive_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    inclusive_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let inc_p50 = inclusive_ms[N / 2];
    let inc_p95 = inclusive_ms[(N as f64 * 0.95) as usize];

    // Standalone encode samples (same payload).
    let mut enc_ms = Vec::with_capacity(N);
    for _ in 0..N {
        let t0 = Instant::now();
        let decoded = ExecutionPayload::<Mainnet>::from_ssz_bytes(&payload_ssz).expect("decode");
        let _ = build_new_payload_v4_params(&decoded, &[], &[0u8; 32], &[]).expect("params");
        enc_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    enc_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let enc_p50 = enc_ms[N / 2];
    let enc_p95 = enc_ms[(N as f64 * 0.95) as usize];

    // HTTP-only: empty params so encode is out of band; still hits ordered lane.
    let mut req_ms = Vec::with_capacity(N);
    for _ in 0..N {
        let t0 = Instant::now();
        let _ = transport
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                cc_engine::methods::names::NEW_PAYLOAD_V4,
                json!([{}, [], "0x00", []]),
            )
            .await;
        req_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    req_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let req_p50 = req_ms[N / 2];
    let req_p95 = req_ms[(N as f64 * 0.95) as usize];

    // Process-local path has no gRPC hop: (1) ≈ (2)+(3). Residual is setup noise.
    let hop_p50 = (inc_p50 - req_p50 - enc_p50).max(0.0);
    let hop_p95 = (inc_p95 - req_p95 - enc_p95).max(0.0);

    println!("cc3c_latency_spans_wiremock");
    println!("source={source}");
    println!("el_block_number={block_number}");
    println!("(1)_inclusive_new_payload_v4_ms p50={inc_p50:.4} p95={inc_p95:.4}");
    println!("(2)_http_request_ms p50={req_p50:.4} p95={req_p95:.4}");
    println!("(3)_encode_ms p50={enc_p50:.4} p95={enc_p95:.4}");
    println!(
        "hop_(1)-(2)-(3)_ms p50={hop_p50:.4} p95={hop_p95:.4} (process-local; gRPC hop absent)"
    );
    println!(
        "note=chain→engine gRPC hop requires live chain metrics; this residual is local setup noise only"
    );

    assert!(inc_p50 > 0.0, "inclusive span must be positive");
    assert!(enc_p50 >= 0.0);
}
