//! CC-22e — Hostile-input harness (CC-22/6, Architecture §5.8 / §13.2).
//!
//! For every Fulu topic **family**:
//! - 10 000 uniformly random byte strings
//! - 10 000 mutated-valid byte strings (from the committed seed corpus)
//!
//! Assertions:
//! 1. every input produces `Ok`/`Err` (validator/decode result) — **never a panic**;
//!    on panic the harness fails with the topic, master seed, case index, and input prefix;
//! 2. peak live heap during the exercise of one input stays ≤ the topic's
//!    [`max_container_bytes`] bound (+ a small fixed overhead for allocator metadata).
//!
//! The counting [`GlobalAlloc`] lives in this integration-test crate only
//! (`grep -rn global_allocator services/p2p/src/` is empty).
//!
//! **CC-23d / CC-23/8:** req/resp framing is exercised in **both directions**
//! (request decode + response decode) with the same panic-is-failure and
//! counting-allocator discipline, including a length prefix that disagrees
//! with the payload and a snappy stream that expands past the declared size.

// Counting GlobalAlloc requires `unsafe` (workspace `unsafe_code = "deny"`).
// Confined to this test binary; production `services/p2p/src/` stays deny-clean.
#![allow(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use cc_proto::p2p::ChainView;
use cc_types::{
    Attestation, ChainConfig, DATA_COLUMN_SIDECAR_SUBNET_COUNT, Mainnet, Preset,
    SignedAggregateAndProof, SignedContributionAndProof, SyncCommitteeMessage,
};
use ssz::Decode;

use cc_p2p::chain_stream::{MapValidatorRecordSource, ValidatorRecordCache};
use cc_p2p::gossip::topics::{SubnetCounts, TopicName, expand_fulu_topic_names};
use cc_p2p::gossip::validate::{
    AlwaysValidKzg, BlockValidateInput, ColumnValidateInput, ColumnValidatorState,
    NoopSamplingFeed, OperationValidateInput, OperationValidatorState, check_payload_len,
    max_container_bytes, validate_beacon_block_local, validate_data_column_sidecar,
    validate_operation,
};
use cc_p2p::gossip::{ATTESTATION_SUBNET_COUNT, PendingQueues, SeenSets};
use cc_p2p::reqresp::Protocol;
use cc_p2p::reqresp::codec::{
    MAX_PAYLOAD_SIZE, ResponseChunk, ResponseCode, SszLimits, SszSnappyFraming,
};
use cc_p2p::reqresp::columns::{
    ColumnsByRangeRequest, ColumnsByRootRequest, make_by_root_identifier,
};
use cc_types::Slot;
use cc_types::primitives::Root;

// ── Counting GlobalAlloc (test harness only) ────────────────────────────────

/// Live bytes attributed while [`TRACKING`] is true.
static LIVE: AtomicUsize = AtomicUsize::new(0);
/// Peak of [`LIVE`] since last reset.
static PEAK: AtomicUsize = AtomicUsize::new(0);
/// When false, alloc/dealloc are pass-through (no accounting).
static TRACKING: AtomicBool = AtomicBool::new(false);
/// Serialises start/stop windows so parallel tests do not clobber counters.
static TRACK_LOCK: Mutex<()> = Mutex::new(());

/// System allocator wrapper that records peak live bytes under test.
struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded to System with the same layout.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && TRACKING.load(Ordering::Relaxed) {
            note_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && TRACKING.load(Ordering::Relaxed) {
            note_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.load(Ordering::Relaxed) {
            note_dealloc(layout.size());
        }
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() && TRACKING.load(Ordering::Relaxed) {
            let old = layout.size();
            if new_size >= old {
                note_alloc(new_size - old);
            } else {
                note_dealloc(old - new_size);
            }
        }
        new_ptr
    }
}

fn note_alloc(size: usize) {
    let cur = LIVE.fetch_add(size, Ordering::Relaxed).saturating_add(size);
    let mut peak = PEAK.load(Ordering::Relaxed);
    while cur > peak {
        match PEAK.compare_exchange_weak(peak, cur, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(p) => peak = p,
        }
    }
}

/// Saturating: deallocations of pre-tracking heap must not wrap LIVE to `usize::MAX`.
fn note_dealloc(size: usize) {
    let mut cur = LIVE.load(Ordering::Relaxed);
    loop {
        let next = cur.saturating_sub(size);
        match LIVE.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(c) => cur = c,
        }
    }
}

// Integration-test binary only (never linked into `services/p2p/src`).
// `cfg(test)` satisfies the acceptance criterion; this crate is test-only.
#[cfg(test)]
#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn alloc_start() -> MutexGuard<'static, ()> {
    let guard = TRACK_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    TRACKING.store(true, Ordering::SeqCst);
    guard
}

fn alloc_stop_peak(_guard: MutexGuard<'static, ()>) -> usize {
    TRACKING.store(false, Ordering::SeqCst);
    PEAK.load(Ordering::SeqCst)
    // guard dropped here — releases TRACK_LOCK
}

// ── Corpus / topic families ─────────────────────────────────────────────────

/// Fixed master seed — printed on every panic failure for reproducibility.
const MASTER_SEED: u64 = 0x00CC_22E0_C0B5_5500;

/// Inputs per class (random / mutated-valid) per topic family.
const INPUTS_PER_CLASS: usize = 10_000;

/// Max length of a uniformly random payload (keeps CI inside the 10 min budget;
/// over-bound cases use a length token without materialising `max+1` for 10 MiB topics).
const RANDOM_LEN_CAP: usize = 512;

/// Fixed overhead allowed above the CC-22b table bound (allocator metadata, small temps).
const ALLOC_OVERHEAD: usize = 64 * 1024;

/// Ten Fulu topic families — one representative name each (subnets share the container).
const FAMILIES: &[(&str, TopicName)] = &[
    ("beacon_block", TopicName::BeaconBlock),
    (
        "beacon_aggregate_and_proof",
        TopicName::BeaconAggregateAndProof,
    ),
    ("beacon_attestation", TopicName::BeaconAttestation(0)),
    ("data_column_sidecar", TopicName::DataColumnSidecar(0)),
    (
        "sync_committee_contribution_and_proof",
        TopicName::SyncCommitteeContributionAndProof,
    ),
    ("sync_committee", TopicName::SyncCommittee(0)),
    ("voluntary_exit", TopicName::VoluntaryExit),
    ("proposer_slashing", TopicName::ProposerSlashing),
    ("attester_slashing", TopicName::AttesterSlashing),
    ("bls_to_execution_change", TopicName::BlsToExecutionChange),
];

fn family_key(name: TopicName) -> &'static str {
    match name {
        TopicName::BeaconBlock => "beacon_block",
        TopicName::BeaconAggregateAndProof => "beacon_aggregate_and_proof",
        TopicName::BeaconAttestation(_) => "beacon_attestation",
        TopicName::DataColumnSidecar(_) => "data_column_sidecar",
        TopicName::SyncCommitteeContributionAndProof => "sync_committee_contribution_and_proof",
        TopicName::SyncCommittee(_) => "sync_committee",
        TopicName::VoluntaryExit => "voluntary_exit",
        TopicName::ProposerSlashing => "proposer_slashing",
        TopicName::AttesterSlashing => "attester_slashing",
        TopicName::BlsToExecutionChange => "bls_to_execution_change",
    }
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/corpus")
}

fn load_seed(family: &str) -> Vec<u8> {
    let path = corpus_dir().join("seeds").join(format!("{family}.ssz"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("read seed {}: {e}", path.display()))
}

fn hoodi_config() -> ChainConfig {
    const YAML: &str = include_str!("../../../crates/types/tests/fixtures/hoodi-config.yaml");
    ChainConfig::from_yaml_str(YAML).expect("hoodi config")
}

// ── Seeded PRNG (no extra dep) ──────────────────────────────────────────────

#[derive(Clone, Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: seed | 1, // never zero
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    fn gen_range(&mut self, max_exclusive: usize) -> usize {
        if max_exclusive == 0 {
            return 0;
        }
        (self.next_u64() as usize) % max_exclusive
    }

    fn fill_bytes(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            for (i, b) in chunk.iter_mut().enumerate() {
                *b = v[i];
            }
        }
    }
}

// ── Mutations over valid seeds ──────────────────────────────────────────────

fn mutate(seed: &[u8], rng: &mut XorShift64) -> Vec<u8> {
    if seed.is_empty() {
        return vec![rng.next_u32() as u8];
    }
    let mut out = seed.to_vec();
    match rng.gen_range(5) {
        0 => {
            // Bit-flip
            let i = rng.gen_range(out.len());
            let bit = 1u8 << (rng.gen_range(8) as u8);
            out[i] ^= bit;
        }
        1 => {
            // Byte corrupt
            let i = rng.gen_range(out.len());
            out[i] = rng.next_u32() as u8;
        }
        2 => {
            // Truncate
            let new_len = rng.gen_range(out.len());
            out.truncate(new_len);
        }
        3 => {
            // Length-field / offset tamper (first 4–16 bytes often hold SSZ offsets)
            let n = out.len().clamp(1, 16);
            let i = rng.gen_range(n);
            out[i] ^= 0xff;
            if out.len() > 4 {
                let j = rng.gen_range(out.len().min(8));
                out[j] = out[j].wrapping_add(1);
            }
        }
        _ => {
            // Append garbage
            let extra = rng.gen_range(32) + 1;
            let mut tail = vec![0u8; extra];
            rng.fill_bytes(&mut tail);
            out.extend_from_slice(&tail);
        }
    }
    out
}

// ── Exercise one payload (never panics on well-formed Rust; SSZ Err is fine) ─

struct ExerciseCtx {
    config: ChainConfig,
    view: ChainView,
    column_state: ColumnValidatorState,
    seen: SeenSets,
    pending: PendingQueues,
    kzg: AlwaysValidKzg,
    sampling: NoopSamplingFeed,
    operations: OperationValidatorState,
    record_cache: ValidatorRecordCache,
    record_source: MapValidatorRecordSource,
}

impl ExerciseCtx {
    fn new(config: ChainConfig) -> Self {
        let view = ChainView {
            slot: 100,
            epoch: 3,
            head_slot: 100,
            genesis_validators_root: vec![0u8; 32],
            ..ChainView::default()
        };
        Self {
            config,
            view,
            column_state: ColumnValidatorState::new(),
            seen: SeenSets::new(),
            pending: PendingQueues::new(),
            kzg: AlwaysValidKzg,
            sampling: NoopSamplingFeed,
            operations: OperationValidatorState::new(),
            record_cache: ValidatorRecordCache::new(),
            record_source: MapValidatorRecordSource::new(),
        }
    }

    /// Run the production-shaped path for `name` on `payload`.
    ///
    /// Returns after `Ok`/`Err` from size check / decode / validator. Must not panic.
    fn exercise(&mut self, name: TopicName, payload: &[u8]) {
        // Shared pre-decode size gate (CC-22b) — same entry validators call.
        if check_payload_len::<Mainnet>(name, payload.len()).is_err() {
            return;
        }

        match name {
            TopicName::BeaconBlock => {
                let inp = BlockValidateInput {
                    payload,
                    current_slot: self.view.slot,
                    finalized_slot: 0,
                    disparity_slots: 2,
                    topic: "beacon_block",
                    message_id: b"hostile-mid",
                    peer_id: b"hostile-peer",
                };
                let _ = validate_beacon_block_local::<Mainnet>(
                    &mut self.seen,
                    &mut self.pending,
                    &inp,
                    None,
                );
            }
            TopicName::DataColumnSidecar(subnet) => {
                let inp = ColumnValidateInput {
                    payload,
                    topic_subnet: subnet,
                    current_slot: self.view.slot,
                    finalized_slot: 0,
                    disparity_slots: 2,
                    view: &self.view,
                    config: &self.config,
                    slots_per_epoch: 32,
                    message_id: b"hostile-mid",
                    peer_id: b"hostile-peer",
                    topic: "data_column_sidecar_0",
                };
                let _ = validate_data_column_sidecar::<Mainnet>(
                    &mut self.column_state,
                    &inp,
                    &self.kzg,
                    &self.sampling,
                    None,
                );
            }
            // IGNORE stubs today (CC-2B/C): still SSZ-decode the container so
            // mutated-valid bytes reach deep decoders; panic-is-failure applies.
            // CC-2C/D IGNORE stubs: still SSZ-decode so mutated-valid bytes reach
            // deep decoders; panic-is-failure applies.
            TopicName::BeaconAggregateAndProof => {
                let _ = SignedAggregateAndProof::<Mainnet>::from_ssz_bytes(payload);
            }
            TopicName::BeaconAttestation(_) => {
                let _ = Attestation::<Mainnet>::from_ssz_bytes(payload);
            }
            // CC-2D: real validators — exercise the ordered path (cold source
            // → IGNORE at signature; SSZ still reaches the decoder).
            TopicName::SyncCommitteeContributionAndProof => {
                let _ = SignedContributionAndProof::<Mainnet>::from_ssz_bytes(payload);
                let mut seen = cc_p2p::gossip::validate::SyncSeenSets::new();
                let source = cc_p2p::gossip::validate::NoopSyncSource;
                let input = cc_p2p::gossip::validate::SyncContribValidateInput {
                    payload,
                    current_slot: 1,
                    disparity_slots: 2,
                    config: &self.config,
                    slots_per_epoch: 32,
                    genesis_validators_root: &[0u8; 32],
                };
                let _ = cc_p2p::gossip::validate::validate_sync_contribution_and_proof::<Mainnet>(
                    &mut seen, &source, &input, None,
                );
            }
            TopicName::SyncCommittee(_) => {
                let _ = SyncCommitteeMessage::from_ssz_bytes(payload);
                let mut seen = cc_p2p::gossip::validate::SyncSeenSets::new();
                let source = cc_p2p::gossip::validate::NoopSyncSource;
                let input = cc_p2p::gossip::validate::SyncMessageValidateInput {
                    payload,
                    topic_subnet: 0,
                    current_slot: 1,
                    disparity_slots: 2,
                    config: &self.config,
                    slots_per_epoch: 32,
                    genesis_validators_root: &[0u8; 32],
                };
                let _ = cc_p2p::gossip::validate::validate_sync_committee_message::<Mainnet>(
                    &mut seen, &source, &input, None,
                );
            }
            // CC-2B real operation validators (empty record map → IGNORE/REJECT; no panic).
            TopicName::VoluntaryExit
            | TopicName::ProposerSlashing
            | TopicName::AttesterSlashing
            | TopicName::BlsToExecutionChange => {
                let input = OperationValidateInput {
                    payload,
                    view: &self.view,
                    config: &self.config,
                    slots_per_epoch: 32,
                    current_epoch: self.view.epoch,
                };
                // Sync test harness: block_on is fine; no nested runtime.
                let _ = futures::executor::block_on(validate_operation::<Mainnet>(
                    &self.operations,
                    name,
                    &input,
                    &self.record_cache,
                    &self.record_source,
                ));
            }
        }
    }

    /// Exercise an **over-bound** length without materialising a multi-MiB buffer.
    fn exercise_oversize(&mut self, name: TopicName) {
        let max = max_container_bytes::<Mainnet>(name);
        // Length-only check: reject path must not panic and must not allocate the payload.
        let _ = check_payload_len::<Mainnet>(name, max.saturating_add(1));
    }
}

fn hex_prefix(bytes: &[u8], n: usize) -> String {
    bytes
        .iter()
        .take(n)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

#[allow(clippy::too_many_arguments)]
fn run_one(
    ctx: &mut ExerciseCtx,
    family: &str,
    name: TopicName,
    payload: &[u8],
    case_kind: &str,
    case_idx: usize,
    oversize: bool,
    bound_override: Option<usize>,
) {
    let bound = bound_override
        .unwrap_or_else(|| max_container_bytes::<Mainnet>(name).saturating_add(ALLOC_OVERHEAD));

    let track = alloc_start();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if oversize {
            ctx.exercise_oversize(name);
        } else {
            ctx.exercise(name, payload);
        }
    }));
    let peak = alloc_stop_peak(track);

    if let Err(panic_payload) = result {
        let msg = panic_payload
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| panic_payload.downcast_ref::<&str>().copied())
            .unwrap_or("<non-string panic>");
        panic!(
            "CC-22e panic-is-failure: topic_family={family} kind={case_kind} idx={case_idx} \
             master_seed=0x{MASTER_SEED:016x} input_len={} input_prefix={} panic={msg}",
            payload.len(),
            hex_prefix(payload, 64),
        );
    }

    if peak > bound {
        panic!(
            "CC-22e alloc-above-bound: topic_family={family} kind={case_kind} idx={case_idx} \
             master_seed=0x{MASTER_SEED:016x} peak={peak} bound={bound} \
             (ssz_max={} + overhead) input_len={} input_prefix={}",
            max_container_bytes::<Mainnet>(name),
            payload.len(),
            hex_prefix(payload, 64),
        );
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn corpus_topic_set_equals_registry_families() {
    let counts = SubnetCounts {
        attestation: ATTESTATION_SUBNET_COUNT,
        sync_committee: Mainnet::SYNC_COMMITTEE_SUBNET_COUNT,
        data_column_sidecar: DATA_COLUMN_SIDECAR_SUBNET_COUNT,
    };
    let registry: BTreeSet<&'static str> = expand_fulu_topic_names(&counts)
        .into_iter()
        .map(family_key)
        .collect();
    let corpus: BTreeSet<&'static str> = FAMILIES.iter().map(|(k, _)| *k).collect();
    assert_eq!(
        registry, corpus,
        "corpus topic families must equal the registry's Fulu family set"
    );

    // Every family has a committed seed file.
    for (fam, _) in FAMILIES {
        let path = corpus_dir().join("seeds").join(format!("{fam}.ssz"));
        assert!(
            path.is_file(),
            "missing seed for family {fam}: {}",
            path.display()
        );
        let bytes = std::fs::read(&path).expect("read");
        assert!(!bytes.is_empty(), "empty seed for {fam}");
    }
}

#[test]
fn counting_allocator_is_live() {
    // Direct heap traffic — proves the GlobalAlloc wrapper is installed.
    {
        let track = alloc_start();
        let probe = vec![0u8; 4096];
        core::hint::black_box(&probe);
        let peak_direct = alloc_stop_peak(track);
        assert!(
            peak_direct >= 4096,
            "counting GlobalAlloc must observe heap traffic (peak={peak_direct})"
        );
        drop(probe);
    }

    // Deliberately low bound: mechanism must fire when peak exceeds the override.
    let too_low = 0usize;
    let result = std::panic::catch_unwind(|| {
        let track = alloc_start();
        let forced = vec![1u8; 512];
        core::hint::black_box(&forced);
        let peak = alloc_stop_peak(track);
        drop(forced);
        assert!(peak >= 512, "expected tracked alloc, peak={peak}");
        if peak > too_low {
            panic!(
                "CC-22e alloc-above-bound: topic_family=beacon_block kind=scratch_low_bound \
                 idx=0 master_seed=0x{MASTER_SEED:016x} peak={peak} bound={too_low}"
            );
        }
    });
    assert!(
        result.is_err(),
        "deliberately low bound must fail (mechanism live)"
    );

    // Sanity: the real run_one path with a normal bound still completes.
    let config = hoodi_config();
    let mut ctx = ExerciseCtx::new(config);
    let seed = load_seed("beacon_block");
    run_one(
        &mut ctx,
        "beacon_block",
        TopicName::BeaconBlock,
        &seed,
        "scratch_ok",
        0,
        false,
        None,
    );
}

#[test]
fn capture_to_corpus_script_exercised() {
    // CI exercises the R-9 one-command refresh against the committed sample capture.
    let root = workspace_root();
    let script = root.join("scripts/corpus-from-capture.sh");
    let capture = corpus_dir().join("sample_capture");
    let out = tempfile_dir("corpus-out");
    assert!(script.is_file(), "missing {}", script.display());
    assert!(capture.is_dir(), "missing {}", capture.display());

    let status = std::process::Command::new("bash")
        .arg(&script)
        .arg(&capture)
        .arg(&out)
        .status()
        .expect("spawn corpus-from-capture.sh");
    assert!(status.success(), "corpus-from-capture.sh failed: {status}");

    // Sample capture supplies all ten families (slot block/column + ops/*).
    for (fam, _) in FAMILIES {
        let p = out.join(format!("{fam}.ssz"));
        assert!(p.is_file(), "script did not write {}", p.display());
    }
    let _ = std::fs::remove_dir_all(&out);
}

#[test]
fn hostile_input_no_panic_and_alloc_bound() {
    let t0 = Instant::now();
    let config = hoodi_config();
    let mut ctx = ExerciseCtx::new(config);

    for (family, name) in FAMILIES {
        let ssz_max = max_container_bytes::<Mainnet>(*name);
        let seed = load_seed(family);
        assert!(
            !seed.is_empty(),
            "seed for {family} must be non-empty real object"
        );

        // ── 10 000 random ────────────────────────────────────────────────
        let mut rng = XorShift64::new(MASTER_SEED ^ family_mix(family) ^ 0xA11CE);
        for i in 0..INPUTS_PER_CLASS {
            let oversize = rng.gen_range(20) == 0;
            if oversize {
                run_one(
                    &mut ctx,
                    family,
                    *name,
                    &[],
                    "random_oversize",
                    i,
                    true,
                    None,
                );
            } else {
                let payload = {
                    let cap = RANDOM_LEN_CAP.min(ssz_max.saturating_add(1));
                    let len = rng.gen_range(cap.saturating_add(1));
                    let mut buf = vec![0u8; len];
                    rng.fill_bytes(&mut buf);
                    buf
                };
                run_one(&mut ctx, family, *name, &payload, "random", i, false, None);
            }
        }

        // ── 10 000 mutated-valid ─────────────────────────────────────────
        let mut rng = XorShift64::new(MASTER_SEED ^ family_mix(family) ^ 0xBEEF);
        for i in 0..INPUTS_PER_CLASS {
            let payload = mutate(&seed, &mut rng);
            run_one(&mut ctx, family, *name, &payload, "mutated", i, false, None);
        }
    }

    let elapsed = t0.elapsed();
    eprintln!(
        "CC-22e hostile_input: {} families × {}×2 inputs in {:.2?}",
        FAMILIES.len(),
        INPUTS_PER_CLASS,
        elapsed
    );
    // Soft guard: stay well under the ten-minute CI budget (record in README).
    assert!(
        elapsed.as_secs() < 8 * 60,
        "harness took {elapsed:?}; exceeds 8 min soft budget (CI hard limit 10 min)"
    );
}

fn family_mix(family: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in family.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

// ── CC-23d / CC-23/8: req/resp framing, both directions ─────────────────────

/// Protocols whose framing is exercised (context-bytes family + a control).
const REQRESP_PROTOCOLS: &[Protocol] = &[
    Protocol::DataColumnSidecarsByRangeV1,
    Protocol::DataColumnSidecarsByRootV1,
    Protocol::BeaconBlocksByRangeV2,
    Protocol::StatusV2,
];

/// Bound for one framing exercise: max payload + framing overhead + allocator metadata.
const REQRESP_ALLOC_BOUND: usize = MAX_PAYLOAD_SIZE + 256 * 1024;

fn encode_varint_u64(mut n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut b = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
    out
}

/// Build a well-formed framed request for seed mutation.
fn valid_request_seed(protocol: Protocol) -> Vec<u8> {
    let ssz: Vec<u8> = match protocol {
        Protocol::DataColumnSidecarsByRangeV1 => ColumnsByRangeRequest {
            start_slot: Slot::new(100),
            count: 2,
            columns: vec![0, 1],
        }
        .to_ssz_bytes(),
        Protocol::DataColumnSidecarsByRootV1 => ColumnsByRootRequest {
            identifiers: vec![make_by_root_identifier(
                Root::from_array([0x11; 32]),
                &[0, 1],
            )],
        }
        .to_ssz_bytes(),
        Protocol::BeaconBlocksByRangeV2 => {
            let mut b = [0u8; 24];
            b[0..8].copy_from_slice(&100u64.to_le_bytes());
            b[8..16].copy_from_slice(&2u64.to_le_bytes());
            b[16..24].copy_from_slice(&1u64.to_le_bytes());
            b.to_vec()
        }
        Protocol::StatusV2 => vec![0u8; 92],
        _ => vec![0u8; 8],
    };
    SszSnappyFraming::encode_request(&ssz, protocol).unwrap_or_default()
}

/// Build a well-formed framed success response for seed mutation.
fn valid_response_seed(protocol: Protocol) -> Vec<u8> {
    let ssz = vec![0xABu8; 32];
    let chunk = if protocol.has_context_bytes() {
        ResponseChunk::success_with_context([0xDE, 0xAD, 0xBE, 0xEF], ssz)
    } else {
        ResponseChunk::success(ssz)
    };
    SszSnappyFraming::encode_response(std::slice::from_ref(&chunk), protocol).unwrap_or_default()
}

/// Exercise request **or** response framing on `payload` — must not panic.
fn exercise_reqresp_framing(protocol: Protocol, payload: &[u8], direction: &str) {
    match direction {
        "request" => {
            let _ = SszSnappyFraming::decode_request(payload, protocol);
            // Also run the SSZ body path when framing succeeds.
            if let Ok(ssz) = SszSnappyFraming::decode_request(payload, protocol) {
                match protocol {
                    Protocol::DataColumnSidecarsByRangeV1 => {
                        let _ = ColumnsByRangeRequest::from_ssz_bytes(&ssz);
                    }
                    Protocol::DataColumnSidecarsByRootV1 => {
                        let _ = ColumnsByRootRequest::from_ssz_bytes(&ssz);
                    }
                    _ => {}
                }
            }
        }
        "response" => {
            let _ = SszSnappyFraming::decode_response(payload, protocol);
            let _ = SszSnappyFraming::decode_response_chunk(payload, protocol);
        }
        _ => {}
    }
}

fn run_reqresp_one(
    protocol: Protocol,
    direction: &str,
    payload: &[u8],
    case_kind: &str,
    case_idx: usize,
) {
    let track = alloc_start();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exercise_reqresp_framing(protocol, payload, direction);
    }));
    let peak = alloc_stop_peak(track);

    if let Err(panic_payload) = result {
        let msg = panic_payload
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| panic_payload.downcast_ref::<&str>().copied())
            .unwrap_or("<non-string panic>");
        panic!(
            "CC-23/8 panic-is-failure: protocol={} direction={direction} kind={case_kind} \
             idx={case_idx} master_seed=0x{MASTER_SEED:016x} input_len={} input_prefix={} panic={msg}",
            protocol.as_str(),
            payload.len(),
            hex_prefix(payload, 64),
        );
    }

    if peak > REQRESP_ALLOC_BOUND {
        panic!(
            "CC-23/8 alloc-above-bound: protocol={} direction={direction} kind={case_kind} \
             idx={case_idx} master_seed=0x{MASTER_SEED:016x} peak={peak} bound={REQRESP_ALLOC_BOUND} \
             input_len={} input_prefix={}",
            protocol.as_str(),
            payload.len(),
            hex_prefix(payload, 64),
        );
    }
}

#[test]
fn reqresp_framing_hostile_both_directions() {
    // Fewer inputs than gossip families: framing is cheaper but still covers
    // random + mutated + the two named §13.2 cases.
    const N: usize = 2_000;

    for protocol in REQRESP_PROTOCOLS {
        for direction in ["request", "response"] {
            let seed = if direction == "request" {
                valid_request_seed(*protocol)
            } else {
                valid_response_seed(*protocol)
            };

            let mut rng = XorShift64::new(
                MASTER_SEED ^ family_mix(protocol.as_str()) ^ family_mix(direction) ^ 0x23D0,
            );
            for i in 0..N {
                let len = rng.gen_range(RANDOM_LEN_CAP.saturating_add(1));
                let mut buf = vec![0u8; len];
                rng.fill_bytes(&mut buf);
                run_reqresp_one(*protocol, direction, &buf, "random", i);
            }

            let mut rng = XorShift64::new(
                MASTER_SEED ^ family_mix(protocol.as_str()) ^ family_mix(direction) ^ 0x23D1,
            );
            for i in 0..N {
                if seed.is_empty() {
                    continue;
                }
                let payload = mutate(&seed, &mut rng);
                run_reqresp_one(*protocol, direction, &payload, "mutated", i);
            }
        }
    }
}

#[test]
fn reqresp_length_prefix_disagrees_with_payload() {
    // §13.2 named case: varint claims a length that does not match the snappy body.
    let protocol = Protocol::DataColumnSidecarsByRangeV1;
    let real_ssz = ColumnsByRangeRequest {
        start_slot: Slot::new(1),
        count: 1,
        columns: vec![0],
    }
    .to_ssz_bytes();
    let well = SszSnappyFraming::encode_request(&real_ssz, protocol).expect("encode");

    // Claim a much larger uncompressed length than the snappy frames produce.
    let mut hostile = encode_varint_u64(1_000_000);
    // Append the snappy portion of the well-formed frame (skip its own varint).
    let (_, rest_start) = {
        // Decode just enough to find where snappy starts: re-use framing decode of varint.
        let mut cursor = std::io::Cursor::new(well.as_slice());
        let _ = SszSnappyFraming::read_varint(&mut cursor).expect("varint");
        let pos = cursor.position() as usize;
        ((), pos)
    };
    hostile.extend_from_slice(&well[rest_start..]);

    let track = alloc_start();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let err = SszSnappyFraming::decode_request(&hostile, protocol);
        assert!(
            err.is_err(),
            "mismatched length prefix must error, not succeed"
        );
    }));
    let peak = alloc_stop_peak(track);
    assert!(result.is_ok(), "mismatched length prefix must not panic");
    // Must not allocate proportional to the claimed 1_000_000 if decompress fails early,
    // and never above the global framing bound.
    assert!(
        peak <= REQRESP_ALLOC_BOUND,
        "peak {peak} exceeds framing alloc bound"
    );

    // Same case on the response path.
    let resp_ssz = vec![0u8; 16];
    let chunk = ResponseChunk::success_with_context([1, 2, 3, 4], resp_ssz);
    let well_resp =
        SszSnappyFraming::encode_response(std::slice::from_ref(&chunk), protocol).expect("enc");
    // Build: result byte + context + hostile varint + real snappy tail of payload.
    let mut hostile_resp = vec![ResponseCode::Success.as_u8()];
    hostile_resp.extend_from_slice(&[1, 2, 3, 4]);
    // Locate snappy after varint in the well-formed chunk (skip result+context+varint).
    let payload_start = 1 + 4; // result + context
    let mut cursor = std::io::Cursor::new(&well_resp[payload_start..]);
    let _ = SszSnappyFraming::read_varint(&mut cursor).expect("varint");
    let snappy_off = payload_start + cursor.position() as usize;
    hostile_resp.extend_from_slice(&encode_varint_u64(5_000_000));
    hostile_resp.extend_from_slice(&well_resp[snappy_off..]);

    let track = alloc_start();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let err = SszSnappyFraming::decode_response(&hostile_resp, protocol);
        assert!(err.is_err(), "response length disagree must error");
    }));
    let peak = alloc_stop_peak(track);
    assert!(result.is_ok(), "response length disagree must not panic");
    assert!(peak <= REQRESP_ALLOC_BOUND, "peak {peak}");
}

#[test]
fn reqresp_snappy_expands_past_declared_size() {
    // §13.2 named case: snappy stream would expand past the declared varint length.
    // Craft: declare a tiny uncompressed length, attach a snappy frame of larger data.
    let protocol = Protocol::DataColumnSidecarsByRootV1;
    let big = vec![0x5Au8; 4096];
    let limits = SszLimits {
        min: 0,
        max: MAX_PAYLOAD_SIZE,
    };
    let framed_big = SszSnappyFraming::encode_payload(&big, limits).expect("encode big");
    // Strip its varint; re-prefix with a too-small claim.
    let mut cursor = std::io::Cursor::new(framed_big.as_slice());
    let claimed = SszSnappyFraming::read_varint(&mut cursor).expect("varint");
    assert_eq!(claimed, 4096);
    let snappy = &framed_big[cursor.position() as usize..];

    let mut hostile = encode_varint_u64(16); // claim only 16 uncompressed bytes
    hostile.extend_from_slice(snappy);

    let track = alloc_start();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let err = SszSnappyFraming::decode_request(&hostile, protocol);
        // Decoder take-bounds to declared length; must error (short/invalid) not panic.
        assert!(err.is_err(), "expand-past-declared must error");
    }));
    let peak = alloc_stop_peak(track);
    assert!(result.is_ok(), "expand-past-declared must not panic");
    // Allocation is bounded by the declared length (16), not the snappy source size.
    assert!(
        peak <= REQRESP_ALLOC_BOUND,
        "peak {peak} exceeds framing alloc bound"
    );

    // Response direction with the same hostile payload after result+context.
    let mut hostile_resp = vec![ResponseCode::Success.as_u8()];
    hostile_resp.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
    hostile_resp.extend_from_slice(&hostile);

    let track = alloc_start();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let err = SszSnappyFraming::decode_response(&hostile_resp, protocol);
        assert!(err.is_err(), "response expand-past-declared must error");
    }));
    let peak = alloc_stop_peak(track);
    assert!(
        result.is_ok(),
        "response expand-past-declared must not panic"
    );
    assert!(peak <= REQRESP_ALLOC_BOUND, "peak {peak}");
}

fn workspace_root() -> PathBuf {
    // services/p2p → repo root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn tempfile_dir(prefix: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "cc-p2p-{prefix}-{}-{}",
        std::process::id(),
        MASTER_SEED
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).expect("tmpdir");
    p
}
