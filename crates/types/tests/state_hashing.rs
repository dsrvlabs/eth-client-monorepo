//! Cached state hashing: differential test + Hoodi root check (CC-10g).
//!
//! - Randomised mutations assert `canonical_root() == tree_hash_root()` after every step.
//! - Fixed-seed regression case is committed.
//! - Hoodi fixture (when cache present) asserts state root == anchor `state_root`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "fixtures/mod.rs"]
mod fixtures;

use std::fs;
use std::time::Instant;

use cc_types::containers::Validator;
use cc_types::primitives::{Epoch, Gwei, Root, Slot};
use cc_types::{BeaconState, ForkName, Mainnet, Minimal};
use tree_hash::TreeHash;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Very small deterministic PRNG (xorshift64) so the test has no extra deps.
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

    fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        (self.next_u64() as usize) % max
    }

    fn next_u8(&mut self) -> u8 {
        self.next_u64() as u8
    }
}

fn sample_validator(rng: &mut XorShift64) -> Validator {
    let mut pk = [0u8; 48];
    let mut wc = [0u8; 32];
    for b in &mut pk {
        *b = rng.next_u8();
    }
    for b in &mut wc {
        *b = rng.next_u8();
    }
    Validator {
        pubkey: pk.into(),
        withdrawal_credentials: Root::from(wc),
        effective_balance: Gwei::new(32_000_000_000),
        slashed: false,
        activation_eligibility_epoch: Epoch::new(rng.next_u64() % 100),
        activation_epoch: Epoch::new(0),
        exit_epoch: Epoch::new(u64::MAX),
        withdrawable_epoch: Epoch::new(u64::MAX),
    }
}

/// Seed the state with a handful of registry entries so mutations have targets.
fn seed_registry(state: &mut BeaconState<Minimal>, n: usize, rng: &mut XorShift64) {
    for _ in 0..n {
        state.validators_push(sample_validator(rng)).unwrap();
        state.balances_push(Gwei::new(32_000_000_000)).unwrap();
        state.previous_epoch_participation_push(0).unwrap();
        state.current_epoch_participation_push(0).unwrap();
        state.inactivity_scores_push(0).unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum MutationKind {
    SetValidatorField,
    PushValidator,
    BalanceWrite,
    ParticipationWrite,
    InactivityWrite,
    RandaoRotation,
}

const MUTATION_KINDS: [MutationKind; 6] = [
    MutationKind::SetValidatorField,
    MutationKind::PushValidator,
    MutationKind::BalanceWrite,
    MutationKind::ParticipationWrite,
    MutationKind::InactivityWrite,
    MutationKind::RandaoRotation,
];

fn apply_mutation(state: &mut BeaconState<Minimal>, kind: MutationKind, rng: &mut XorShift64) {
    match kind {
        MutationKind::SetValidatorField => {
            let n = state.validators_len();
            if n == 0 {
                return;
            }
            let i = rng.next_usize(n);
            if let Some(v) = state.validators_get_mut(i) {
                v.effective_balance = Gwei::new(rng.next_u64() % 32_000_000_000);
                v.slashed = rng.next_u64().is_multiple_of(2);
            }
        }
        MutationKind::PushValidator => {
            // Keep the registry modest for cold tree_hash in the differential loop.
            if state.validators_len() >= 64 {
                return;
            }
            state.validators_push(sample_validator(rng)).unwrap();
            state.balances_push(Gwei::new(32_000_000_000)).unwrap();
            state.previous_epoch_participation_push(0).unwrap();
            state.current_epoch_participation_push(0).unwrap();
            state.inactivity_scores_push(0).unwrap();
        }
        MutationKind::BalanceWrite => {
            let n = state.balances_len();
            if n == 0 {
                return;
            }
            let i = rng.next_usize(n);
            state
                .balances_set(i, Gwei::new(rng.next_u64() % 40_000_000_000))
                .unwrap();
        }
        MutationKind::ParticipationWrite => {
            let n = state.current_epoch_participation_len();
            if n == 0 {
                return;
            }
            let i = rng.next_usize(n);
            let flags = rng.next_u8() & 0b111;
            if rng.next_u64().is_multiple_of(2) {
                state.current_epoch_participation_set(i, flags).unwrap();
            } else {
                state.previous_epoch_participation_set(i, flags).unwrap();
            }
        }
        MutationKind::InactivityWrite => {
            let n = state.inactivity_scores_len();
            if n == 0 {
                return;
            }
            let i = rng.next_usize(n);
            state
                .inactivity_scores_set(i, rng.next_u64() % 1_000)
                .unwrap();
        }
        MutationKind::RandaoRotation => {
            let n = state.randao_mixes_len();
            if n == 0 {
                return;
            }
            let i = rng.next_usize(n);
            let mut bytes = [0u8; 32];
            for b in &mut bytes {
                *b = rng.next_u8();
            }
            state.randao_mixes_set(i, Root::from(bytes)).unwrap();
        }
    }
}

fn assert_roots_agree(state: &mut BeaconState<Minimal>, seed: u64, step: usize, kind: MutationKind) {
    let cached = state.canonical_root();
    let cold = TreeHash::tree_hash_root(state);
    assert_eq!(
        cached.to_hash256(),
        cold,
        "canonical_root != tree_hash_root after step {step} kind={kind:?} seed={seed}"
    );
}

// ---------------------------------------------------------------------------
// Differential tests
// ---------------------------------------------------------------------------

/// Fixed-seed regression (committed). Covers all six mutation kinds.
#[test]
fn differential_fixed_seed_regression() {
    const SEED: u64 = 0x000C_1010_C10A; // committed CC-10g regression seed
    const STEPS: usize = 256;

    let mut rng = XorShift64::new(SEED);
    let mut state = BeaconState::<Minimal>::default();
    seed_registry(&mut state, 8, &mut rng);

    // Ensure every kind appears at least once in the first 6 steps.
    for (step, &kind) in MUTATION_KINDS.iter().enumerate() {
        apply_mutation(&mut state, kind, &mut rng);
        assert_roots_agree(&mut state, SEED, step, kind);
    }

    for step in MUTATION_KINDS.len()..STEPS {
        let kind = MUTATION_KINDS[rng.next_usize(MUTATION_KINDS.len())];
        apply_mutation(&mut state, kind, &mut rng);
        assert_roots_agree(&mut state, SEED, step, kind);
    }
}

/// Randomised differential (≥ 200 steps); seed printed on failure.
#[test]
fn differential_randomised_200_steps() {
    // Derive a seed from a fixed base XOR a compile-time constant so runs are
    // reproducible in CI while still exercising a second trajectory.
    const SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;
    const STEPS: usize = 200;

    let mut rng = XorShift64::new(SEED);
    let mut state = BeaconState::<Minimal>::default();
    seed_registry(&mut state, 16, &mut rng);

    let mut seen = [false; 6];
    for step in 0..STEPS {
        let kind_idx = rng.next_usize(MUTATION_KINDS.len());
        let kind = MUTATION_KINDS[kind_idx];
        seen[kind_idx] = true;
        apply_mutation(&mut state, kind, &mut rng);
        assert_roots_agree(&mut state, SEED, step, kind);
    }
    assert!(
        seen.iter().all(|&s| s),
        "not all mutation kinds exercised; seed={SEED}"
    );
}

/// Mutation + `canonical_root` without an external `commit()` still matches cold hash.
#[test]
fn canonical_root_commits_internally() {
    let mut state = BeaconState::<Minimal>::default();
    let mut rng = XorShift64::new(1);
    seed_registry(&mut state, 4, &mut rng);
    state.balances_set(0, Gwei::new(1)).unwrap();
    // Deliberately no state.commit() here.
    let cached = state.canonical_root();
    let cold = TreeHash::tree_hash_root(&state);
    assert_eq!(cached.to_hash256(), cold);
}

#[test]
fn default_state_roots_agree() {
    let mut state = BeaconState::<Minimal>::default();
    let cached = state.canonical_root();
    let cold = TreeHash::tree_hash_root(&state);
    assert_eq!(cached.to_hash256(), cold);
}

// ---------------------------------------------------------------------------
// Hoodi (CC-10/4)
// ---------------------------------------------------------------------------

/// `canonical_root()` of the committed Hoodi finalized state equals the anchor
/// block's `state_root`, and `tree_hash_root()` returns the same value.
///
/// Skips when `HOODI_FIXTURES_CACHE` is unset (same contract as other fixture tests).
#[test]
fn hoodi_state_root_matches_anchor() {
    if !fixtures::cache_env_is_set() {
        eprintln!(
            "skip: {env} unset — Hoodi SSZ cache not required for this run",
            env = fixtures::CACHE_ENV
        );
        return;
    }

    let root = fixtures::resolve_cache_root().expect("resolve cache root");
    let fixtures = fixtures::HoodiFixtures::open_in(&root).unwrap_or_else(|e| {
        panic!("{e}");
    });

    let state_bytes = fs::read(&fixtures.state_ssz).expect("read beacon_state.ssz");
    assert!(
        state_bytes.len() as u64 >= 150 * 1024 * 1024,
        "state must be ≥ 150 MB, got {}",
        state_bytes.len()
    );

    let mut state = BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &state_bytes)
        .unwrap_or_else(|e| panic!("BeaconState SSZ decode failed: {e:?}"));
    assert_eq!(state.slot().as_u64(), fixtures.anchor.slot);

    // Cold path (TreeHash) — also the R-3 early-warning measurement.
    let t0 = Instant::now();
    let cold = TreeHash::tree_hash_root(&state);
    let cold_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Cached path, first call (builds caches = effectively cold for caches).
    let t1 = Instant::now();
    let cached = state.canonical_root();
    let cached_build_ms = t1.elapsed().as_secs_f64() * 1000.0;

    // Warm path.
    let t2 = Instant::now();
    let warm = state.canonical_root();
    let warm_ms = t2.elapsed().as_secs_f64() * 1000.0;

    let expected_hex = fixtures.anchor.state_root.trim_start_matches("0x");
    let cold_hex: String = cold.as_slice().iter().map(|b| format!("{b:02x}")).collect();
    let cached_hex: String = cached
        .to_hash256()
        .as_slice()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let warm_hex: String = warm
        .to_hash256()
        .as_slice()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    eprintln!(
        "CC-10g Hoodi timings (slot {}): tree_hash_root cold={cold_ms:.1} ms, \
         canonical_root build={cached_build_ms:.1} ms, warm={warm_ms:.1} ms; \
         cold_exceeds_300ms={}",
        fixtures.anchor.slot,
        cold_ms > 300.0
    );
    eprintln!(
        "machine: {} / {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );

    assert_eq!(cold_hex, expected_hex, "tree_hash_root != anchor state_root");
    assert_eq!(
        cached_hex, expected_hex,
        "canonical_root != anchor state_root"
    );
    assert_eq!(warm_hex, expected_hex, "warm canonical_root mismatch");
    assert_eq!(cached.to_hash256(), cold);
    assert_eq!(warm.to_hash256(), cold);

    // Slot smoke.
    let _ = Slot::new(fixtures.anchor.slot);
}
