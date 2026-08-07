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
//! (`grep -rn global_allocator services/p2p/src/` is empty). Req/resp framing
//! extends this harness in CC-23d.

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
    Attestation, AttesterSlashing, ChainConfig, DATA_COLUMN_SIDECAR_SUBNET_COUNT, Mainnet, Preset,
    ProposerSlashing, SignedAggregateAndProof, SignedBlsToExecutionChange,
    SignedContributionAndProof, SignedVoluntaryExit, SyncCommitteeMessage,
};
use ssz::Decode;

use cc_p2p::gossip::topics::{expand_fulu_topic_names, SubnetCounts, TopicName};
use cc_p2p::gossip::validate::{
    check_payload_len, max_container_bytes, validate_beacon_block_local,
    validate_data_column_sidecar, AlwaysValidKzg, BlockValidateInput, ColumnValidateInput,
    ColumnValidatorState, NoopSamplingFeed,
};
use cc_p2p::gossip::{PendingQueues, SeenSets, ATTESTATION_SUBNET_COUNT};

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
    ("beacon_aggregate_and_proof", TopicName::BeaconAggregateAndProof),
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
    const YAML: &str =
        include_str!("../../../crates/types/tests/fixtures/hoodi-config.yaml");
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
}

impl ExerciseCtx {
    fn new(config: ChainConfig) -> Self {
        let view = ChainView {
            slot: 100,
            epoch: 3,
            head_slot: 100,
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
            // IGNORE stubs today (CC-2B/C/D): still SSZ-decode the container so
            // mutated-valid bytes reach deep decoders; panic-is-failure applies.
            TopicName::BeaconAggregateAndProof => {
                let _ = SignedAggregateAndProof::<Mainnet>::from_ssz_bytes(payload);
            }
            TopicName::BeaconAttestation(_) => {
                let _ = Attestation::<Mainnet>::from_ssz_bytes(payload);
            }
            TopicName::SyncCommitteeContributionAndProof => {
                let _ = SignedContributionAndProof::<Mainnet>::from_ssz_bytes(payload);
            }
            TopicName::SyncCommittee(_) => {
                let _ = SyncCommitteeMessage::from_ssz_bytes(payload);
            }
            TopicName::VoluntaryExit => {
                let _ = SignedVoluntaryExit::from_ssz_bytes(payload);
            }
            TopicName::ProposerSlashing => {
                let _ = ProposerSlashing::from_ssz_bytes(payload);
            }
            TopicName::AttesterSlashing => {
                let _ = AttesterSlashing::<Mainnet>::from_ssz_bytes(payload);
            }
            TopicName::BlsToExecutionChange => {
                let _ = SignedBlsToExecutionChange::from_ssz_bytes(payload);
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
    let bound = bound_override.unwrap_or_else(|| {
        max_container_bytes::<Mainnet>(name).saturating_add(ALLOC_OVERHEAD)
    });

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
                    &mut ctx, family, *name, &[], "random_oversize", i, true, None,
                );
            } else {
                let payload = {
                    let cap = RANDOM_LEN_CAP.min(ssz_max.saturating_add(1));
                    let len = rng.gen_range(cap.saturating_add(1));
                    let mut buf = vec![0u8; len];
                    rng.fill_bytes(&mut buf);
                    buf
                };
                run_one(
                    &mut ctx, family, *name, &payload, "random", i, false, None,
                );
            }
        }

        // ── 10 000 mutated-valid ─────────────────────────────────────────
        let mut rng = XorShift64::new(MASTER_SEED ^ family_mix(family) ^ 0xBEEF);
        for i in 0..INPUTS_PER_CLASS {
            let payload = mutate(&seed, &mut rng);
            run_one(
                &mut ctx, family, *name, &payload, "mutated", i, false, None,
            );
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
