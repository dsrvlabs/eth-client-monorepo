//! CC-1C timing instrumentation tests.
//!
//! - Bucket boundaries at exactly 0.4 / 1.0 (CC-1C/1).
//! - Full §11.1 metric set on a test-spawned `/metrics` exposition.
//! - Structured epoch log line (CC-1C/2).
//! - CI 2× loose ceiling over the Hoodi fixture (block < 800 ms, epoch < 2000 ms).
//! - Budget-exceeded warn + counter per `op`.
//! - `hash_tree_root` records both `path=cached` and `path=cold`.
//!
//! Run: `cargo nextest run -p cc-chain --test timing`
//! (issue text says `-p chain`; the package name is `cc-chain`.)

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_bootstrap::spawn_metrics_server;
use cc_chain::{
    BLOCK_BUDGET_SECS, BUFFER_RING, BUFFER_SUBSCRIBER, BudgetOp, CI_BLOCK_CEILING_SECS,
    CI_EPOCH_CEILING_SECS, ChainMetrics, EPOCH_BUDGET_SECS, EventInput, EventsConfig, EventsHandle,
    HashPath, ImportResult, ImportStage, PROCESS_BLOCK_BUCKETS, PROCESS_EPOCH_BUCKETS,
};
use cc_types::{BeaconState, ForkName, Mainnet};
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry as TracingRegistry;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tree_hash::TreeHash;

// ── log capture ─────────────────────────────────────────────────────────────

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
        .with(EnvFilter::new("info"))
        .with(
            fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_writer(writer),
        );
    tracing::subscriber::with_default(subscriber, f)
}

fn field_from_json<'a>(v: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    v.get(key)
        .or_else(|| v.pointer(&format!("/fields/{key}")))
        .or_else(|| v.pointer(&format!("/span/{key}")))
}

// ── Hoodi fixture (cache-optional; same contract as CC-10b) ─────────────────

const CACHE_ENV: &str = "HOODI_FIXTURES_CACHE";
const ANCHOR_SLOT: u64 = 3649472;
const ANCHOR_EPOCH: u64 = 114046;

fn cache_env_is_set() -> bool {
    std::env::var_os(CACHE_ENV).is_some()
}

fn cache_root() -> Option<PathBuf> {
    std::env::var_os(CACHE_ENV).map(PathBuf::from)
}

fn hoodi_state_path(root: &Path) -> PathBuf {
    root.join(ANCHOR_SLOT.to_string()).join("beacon_state.ssz")
}

/// Load the committed Hoodi `BeaconState` when the fixture cache is available.
///
/// Returns `None` when `HOODI_FIXTURES_CACHE` is unset so CI without the restored
/// cache stays green (CC-10b skip contract).
fn try_load_hoodi_state() -> Option<BeaconState<Mainnet>> {
    if !cache_env_is_set() {
        eprintln!(
            "skip: {CACHE_ENV} unset — Hoodi SSZ cache not required for this run \
             (see crates/types/tests/fixtures/README.md)"
        );
        return None;
    }
    let root = cache_root().expect("CACHE_ENV set");
    let path = hoodi_state_path(&root);
    if !path.is_file() {
        panic!(
            "HOODI_FIXTURES_CACHE set but {} missing; run scripts/fetch-hoodi-fixtures.sh",
            path.display()
        );
    }
    let bytes = fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert!(
        bytes.len() as u64 >= 150 * 1024 * 1024,
        "Hoodi state must be ≥ 150 MB, got {} bytes",
        bytes.len()
    );
    let state = BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("BeaconState SSZ decode failed: {e:?}"));
    assert_eq!(state.slot().as_u64(), ANCHOR_SLOT);
    Some(state)
}

// ── CC-1C/1 bucket boundaries ───────────────────────────────────────────────

#[test]
fn cc1c_1_block_histogram_bucket_boundary_exactly_0_4() {
    assert_eq!(
        PROCESS_BLOCK_BUCKETS.to_vec(),
        vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0
        ]
    );
    assert!(
        PROCESS_BLOCK_BUCKETS.contains(&0.4),
        "block buckets must include exact 0.4"
    );

    let mut registry = Registry::default();
    let _m = ChainMetrics::register(&mut registry);
    let mut buf = String::new();
    encode(&mut buf, &registry).unwrap();

    assert!(
        buf.contains("cc_chain_process_block_seconds_bucket{le=\"0.4\"}"),
        "exposition must have le=\"0.4\" on process_block:\n{buf}"
    );
    // OpenMetrics encodes 1.0 as "1.0" (not Display's "1") — use literal labels.
    for le in [
        "0.005", "0.01", "0.025", "0.05", "0.1", "0.2", "0.3", "0.4", "0.5", "0.75", "1.0", "2.0",
        "5.0",
    ] {
        let needle = format!("cc_chain_process_block_seconds_bucket{{le=\"{le}\"}}");
        assert!(buf.contains(&needle), "missing {needle}");
    }
}

#[test]
fn cc1c_1_epoch_histogram_bucket_boundary_exactly_1_0() {
    assert_eq!(
        PROCESS_EPOCH_BUCKETS.to_vec(),
        vec![
            0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0
        ]
    );
    assert!(
        PROCESS_EPOCH_BUCKETS.contains(&1.0),
        "epoch buckets must include exact 1.0"
    );

    let mut registry = Registry::default();
    let _m = ChainMetrics::register(&mut registry);
    let mut buf = String::new();
    encode(&mut buf, &registry).unwrap();

    assert!(
        buf.contains("cc_chain_process_epoch_seconds_bucket{le=\"1.0\"}"),
        "exposition must have le=\"1.0\" on process_epoch:\n{buf}"
    );
    for le in [
        "0.05", "0.1", "0.25", "0.5", "0.75", "1.0", "1.25", "1.5", "2.0", "3.0", "5.0", "10.0",
    ] {
        let needle = format!("cc_chain_process_epoch_seconds_bucket{{le=\"{le}\"}}");
        assert!(buf.contains(&needle), "missing {needle}");
    }
}

// ── full metric set on exposition server ────────────────────────────────────

#[tokio::test]
async fn metrics_exposition_server_lists_section_11_1_families() {
    let mut registry = Registry::default();
    let m = ChainMetrics::register(&mut registry);

    // Drive labelled series that seed already created; also exercise occupancy sync.
    m.observe_process_block(0.01, 1, 0, 1);
    m.observe_process_epoch(0.1, 32, 1, 1);
    m.observe_process_slots(0.001);
    m.observe_state_hash_tree_root(HashPath::Cached, 0.001);
    m.observe_state_hash_tree_root(HashPath::Cold, 0.01);
    m.observe_import_stage(ImportStage::Decode, 0.001);
    m.inc_import_result(ImportResult::Imported);
    m.set_head(100, 2, 3);
    m.set_import_queue_depth(0);
    m.set_resident_states(1);

    let registry = Arc::new(registry);
    let (addr, handle) = spawn_metrics_server(Arc::clone(&registry)).await.unwrap();

    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = http::Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();
    let res = sender.send_request(req).await.unwrap();
    assert_eq!(res.status(), hyper::StatusCode::OK);
    let body = http_body_util::BodyExt::collect(res.into_body())
        .await
        .unwrap()
        .to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();

    // §11.1 metric names (with unit suffixes where registered with Unit::Seconds).
    for needle in [
        "cc_chain_process_block_seconds",
        "cc_chain_process_epoch_seconds",
        "cc_chain_process_slots_seconds",
        "cc_chain_state_hash_tree_root_seconds",
        "cc_chain_import_seconds",
        "cc_chain_head_slot",
        "cc_chain_head_lag_slots",
        "cc_chain_finalized_epoch",
        "cc_chain_import_total",
        "cc_chain_import_queue_depth",
        "cc_chain_event_buffer_occupancy",
        "cc_chain_resident_states",
        "cc_chain_subscribers",
        "cc_chain_budget_exceeded_total",
    ] {
        assert!(text.contains(needle), "missing {needle} in:\n{text}");
    }

    // Label sets.
    assert!(text.contains("path=\"cached\""));
    assert!(text.contains("path=\"cold\""));
    assert!(text.contains("stage=\"decode\""));
    assert!(text.contains("stage=\"transition\""));
    assert!(text.contains("stage=\"fork_choice\""));
    assert!(text.contains("stage=\"publish\""));
    assert!(text.contains("result=\"imported\""));
    assert!(text.contains(&format!("buffer=\"{BUFFER_RING}\"")));
    assert!(text.contains(&format!("buffer=\"{BUFFER_SUBSCRIBER}\"")));
    assert!(text.contains("op=\"block\""));
    assert!(text.contains("op=\"epoch\""));

    handle.abort();
    let _ = handle.await;
}

// ── CC-1C/2 structured epoch log ────────────────────────────────────────────

#[test]
fn cc1c_2_epoch_transition_structured_log_line() {
    let mut registry = Registry::default();
    let m = ChainMetrics::register(&mut registry);
    let (writer, buf) = BufferWriter::new();
    with_json_subscriber(writer, || {
        m.observe_process_epoch(0.25, ANCHOR_SLOT, ANCHOR_EPOCH, 999_999);
    });

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let epoch_lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("epoch transition"))
        .collect();
    assert_eq!(
        epoch_lines.len(),
        1,
        "expected exactly one epoch transition line:\n{text}"
    );
    let v: serde_json::Value = serde_json::from_str(epoch_lines[0])
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", epoch_lines[0]));
    for key in ["slot", "epoch", "duration", "validator_count"] {
        assert!(
            field_from_json(&v, key).is_some(),
            "missing field {key} in {v}"
        );
    }
}

// ── budget exceeded (one test per op) ───────────────────────────────────────

#[test]
fn budget_exceeded_op_block_warns_and_increments() {
    let mut registry = Registry::default();
    let m = ChainMetrics::register(&mut registry);
    let before = m.budget_exceeded_count(BudgetOp::Block);
    let (writer, buf) = BufferWriter::new();
    with_json_subscriber(writer, || {
        // Just over production budget; never fatal.
        m.observe_process_block(BLOCK_BUDGET_SECS + 0.01, 1, 0, 10);
    });
    assert_eq!(m.budget_exceeded_count(BudgetOp::Block), before + 1);
    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("exceeded budget"), "warn missing:\n{text}");
    // Confirm counter series in exposition.
    let mut expo = String::new();
    encode(&mut expo, &registry).unwrap();
    assert!(expo.contains("cc_chain_budget_exceeded_total"));
    assert!(expo.contains("op=\"block\""));
}

#[test]
fn budget_exceeded_op_epoch_warns_and_increments() {
    let mut registry = Registry::default();
    let m = ChainMetrics::register(&mut registry);
    let before = m.budget_exceeded_count(BudgetOp::Epoch);
    let (writer, buf) = BufferWriter::new();
    with_json_subscriber(writer, || {
        m.observe_process_epoch(EPOCH_BUDGET_SECS + 0.05, 32, 1, 10);
    });
    assert_eq!(m.budget_exceeded_count(BudgetOp::Epoch), before + 1);
    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("exceeded budget"), "warn missing:\n{text}");
    let mut expo = String::new();
    encode(&mut expo, &registry).unwrap();
    assert!(expo.contains("op=\"epoch\""));
}

// ── hash_tree_root path labels ──────────────────────────────────────────────

#[test]
fn state_hash_tree_root_records_cached_and_cold_paths() {
    let mut registry = Registry::default();
    let m = ChainMetrics::register(&mut registry);

    m.time_state_hash_tree_root(HashPath::Cached, || {
        // Tiny stand-in; Hoodi exercise is in the ceiling test when cache is present.
        std::thread::sleep(std::time::Duration::from_micros(10));
    });
    m.time_state_hash_tree_root(HashPath::Cold, || {
        std::thread::sleep(std::time::Duration::from_micros(10));
    });

    let mut buf = String::new();
    encode(&mut buf, &registry).unwrap();
    assert!(buf.contains("path=\"cached\""));
    assert!(buf.contains("path=\"cold\""));
    assert!(buf.contains("cc_chain_state_hash_tree_root_seconds_count"));
}

// ── occupancy wiring from events bus ────────────────────────────────────────

#[tokio::test]
async fn occupancy_syncs_into_event_buffer_gauges() {
    let mut registry = Registry::default();
    let m = ChainMetrics::register(&mut registry);

    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 8,
        subscriber_queue_capacity: 4,
        session_id: Some(7),
    });
    let _sub = h.subscribe(None).await.unwrap();
    h.publish(EventInput::block_imported(
        1,
        bytes::Bytes::from_static(b"r"),
    ))
    .await
    .unwrap();
    // Let the events task process publish + occupancy atomics.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    m.sync_from_occupancy(h.occupancy());

    let mut buf = String::new();
    encode(&mut buf, &registry).unwrap();
    // Ring should be non-zero after one event.
    assert!(
        h.occupancy().ring() >= 1,
        "expected ring occupancy ≥ 1, got {}",
        h.occupancy().ring()
    );
    assert!(buf.contains("cc_chain_event_buffer_occupancy"));
    assert!(buf.contains(&format!("buffer=\"{BUFFER_RING}\"")));
    assert!(buf.contains("cc_chain_subscribers"));

    h.shutdown().await;
}

// ── CI 2× loose ceiling (Hoodi fixture) ─────────────────────────────────────

/// CI 2× loose ceiling over the committed Hoodi state fixture.
///
/// Asserts **block < 800 ms** and **epoch < 2000 ms**. This is a **2× loose
/// ceiling that is not the budget** (production budget is block p95 ≤ 400 ms /
/// epoch p95 ≤ 1000 ms on the soak machine — ADR-P1-15 / §11.3).
///
/// Full `process_block` / `process_epoch` on Hoodi are not wired yet (CC-12 /
/// CC-13 / CC-18b). Until those land, this times the measurable work that
/// dominates those paths on the fixture: one **cached** `canonical_root`
/// (block-scale dirty set) and one **cold** `tree_hash_root` (epoch-scale
/// hashing). Both are recorded through [`ChainMetrics`] so histogram + budget
/// paths stay live. When import/transition hooks exist, replace the closures
/// with the real process calls without changing the ceiling constants.
///
/// Skips when `HOODI_FIXTURES_CACHE` is unset (CC-10b skip contract).
#[test]
fn ci_two_x_ceiling_block_and_epoch_over_hoodi_fixture() {
    let Some(mut state) = try_load_hoodi_state() else {
        return;
    };

    let mut registry = Registry::default();
    let m = ChainMetrics::register(&mut registry);
    let validator_count = state.validators_len() as u64;
    let slot = state.slot().as_u64();
    let epoch = ANCHOR_EPOCH;

    // Cold hash first (epoch-scale hashing cost; also records path=cold).
    let cold_start = Instant::now();
    let _cold_root = TreeHash::tree_hash_root(&state);
    let cold_secs = cold_start.elapsed().as_secs_f64();
    m.observe_state_hash_tree_root(HashPath::Cold, cold_secs);

    // Cached path: warm then measure (block-scale dirty set; path=cached).
    let _ = state.canonical_root();
    let cached_start = Instant::now();
    let _cached_root = state.canonical_root();
    let cached_secs = cached_start.elapsed().as_secs_f64();
    m.observe_state_hash_tree_root(HashPath::Cached, cached_secs);

    // Record as process_block / process_epoch stand-ins (see doc comment).
    // Use the measured durations so the ceiling assertion and the histograms agree.
    m.observe_process_block(cached_secs, slot, epoch, validator_count);
    m.observe_process_epoch(cold_secs, slot, epoch, validator_count);

    assert!(
        cached_secs < CI_BLOCK_CEILING_SECS,
        "block-path stand-in (cached canonical_root) took {cached_secs:.3}s ≥ \
         CI 2× ceiling {CI_BLOCK_CEILING_SECS}s — not the production budget \
         ({BLOCK_BUDGET_SECS}s); investigate order-of-magnitude regression"
    );
    assert!(
        cold_secs < CI_EPOCH_CEILING_SECS,
        "epoch-path stand-in (cold tree_hash_root) took {cold_secs:.3}s ≥ \
         CI 2× ceiling {CI_EPOCH_CEILING_SECS}s — not the production budget \
         ({EPOCH_BUDGET_SECS}s); investigate order-of-magnitude regression"
    );

    // Exposition still carries both hash paths after the fixture exercise.
    let mut buf = String::new();
    encode(&mut buf, &registry).unwrap();
    assert!(buf.contains("path=\"cached\""));
    assert!(buf.contains("path=\"cold\""));
}

// ── no home-grown telemetry in chain service ────────────────────────────────

#[test]
fn chain_service_does_not_reinvent_phase0_telemetry() {
    // Acceptance: grep for tracing_subscriber:: / Registry::default re-init
    // in production sources (not tests) returns nothing that re-invents Phase 0.
    // This test locks the production sources we own.
    let main = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"));
    let metrics = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/metrics.rs"));
    let lib = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"));
    for (name, src) in [("main.rs", main), ("metrics.rs", metrics), ("lib.rs", lib)] {
        assert!(
            !src.contains("tracing_subscriber::fmt().init")
                && !src.contains("tracing_subscriber::registry()")
                && !src.contains("Registry::default().with("),
            "{name} must not install a tracing subscriber (use cc_bootstrap::init)"
        );
        // metrics.rs may mention Registry for prometheus — that is fine.
        // It must not construct a *tracing* Registry for production init.
    }
    // Production registration goes through bootstrap's registry.
    assert!(
        main.contains("ChainMetrics::register") && main.contains("bs.registry"),
        "main must register chain metrics into bs.registry between init and serve"
    );
}
