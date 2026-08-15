//! `shuffling` suite + ShufflingCache / EpochCache unit tests (CC-13a).
//!
//! Vector path: `tests/<preset>/phase0/shuffling/core/shuffle/*` (consensus-spec
//! vectors pin shuffling under phase0; SHUFFLE_ROUND_COUNT comes from the preset).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use cc_state_transition::helpers::constants::{FAR_FUTURE_EPOCH, MAX_EFFECTIVE_BALANCE};
use cc_state_transition::helpers::misc::compute_shuffled_index;
use cc_state_transition::{
    decision_root_for_epoch, get_beacon_committee, get_beacon_proposer_index,
    get_or_compute_shuffling, invalidate_epoch_cache, rebuild_epoch_cache,
};
use cc_types::containers::Validator;
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{BlsPublicKey, CommitteeIndex, Epoch, Gwei, Root, Slot, ValidatorIndex};
use cc_types::{
    BeaconState, SHUFFLING_CACHE_DEFAULT_CAPACITY, ShuffledCommitteeEpoch, ShufflingCache,
    ShufflingCacheKey,
};
use serde::Deserialize;

const LOCKFILE: &str = include_str!("../../../spec-vectors.lock");
const RUNNER: &str = "shuffling";
const FORK: &str = "phase0";
const HANDLER: &str = "core";
const SUITE: &str = "shuffle";

// ---------------------------------------------------------------------------
// Vector-cache helpers
// ---------------------------------------------------------------------------

fn lock_tag() -> &'static str {
    LOCKFILE
        .lines()
        .find_map(|l| {
            let l = l.trim();
            l.strip_prefix("tag")
                .and_then(|r| r.trim().strip_prefix('='))
                .map(|v| v.trim().trim_matches('"'))
        })
        .expect("tag in spec-vectors.lock")
}

fn tests_root() -> PathBuf {
    let cache = std::env::var("SPEC_VECTORS_CACHE").unwrap_or_else(|_| {
        let home = std::env::var("HOME").expect("HOME");
        format!("{home}/.cache/eth-consensus-spec-vectors")
    });
    let tag = lock_tag();
    let root = PathBuf::from(cache).join(tag).join("tests");
    assert!(
        root.is_dir(),
        "vector tests tree missing at {}; run scripts/fetch-spec-vectors.sh",
        root.display()
    );
    root
}

fn collect_shuffle_cases(tests: &Path, preset: &str) -> Vec<(String, PathBuf)> {
    let dir = tests
        .join(preset)
        .join(FORK)
        .join(RUNNER)
        .join(HANDLER)
        .join(SUITE);
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for ent in fs::read_dir(&dir).unwrap() {
        let ent = ent.unwrap();
        if !ent.file_type().unwrap().is_dir() {
            continue;
        }
        let case_dir = ent.path();
        let mapping = case_dir.join("mapping.yaml");
        if mapping.is_file() {
            let name = ent.file_name().to_string_lossy().into_owned();
            let rel = format!("{preset}/{FORK}/{RUNNER}/{HANDLER}/{SUITE}/{name}");
            out.push((rel, case_dir));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[derive(Debug, Deserialize)]
struct MappingCase {
    seed: String,
    count: u64,
    mapping: Vec<u64>,
}

fn parse_seed_hex(s: &str) -> Root {
    let hex = s.trim().trim_start_matches("0x");
    let bytes = decode_hex(hex).unwrap_or_else(|e| panic!("seed hex {s}: {e}"));
    assert_eq!(bytes.len(), 32, "seed must be 32 bytes");
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Root::from_array(arr)
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd hex length".into());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = from_hex_digit(bytes[i])?;
        let lo = from_hex_digit(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn from_hex_digit(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("bad hex digit {}", b as char)),
    }
}

fn run_shuffle_suite<P: Preset>(preset_name: &str) {
    let tests = tests_root();
    let cases = collect_shuffle_cases(&tests, preset_name);
    assert!(
        !cases.is_empty(),
        "no shuffling cases for {preset_name} under {}/{FORK}/{RUNNER}",
        tests.display()
    );
    let mut passed = 0u32;
    for (rel, dir) in &cases {
        let yaml = fs::read_to_string(dir.join("mapping.yaml")).unwrap();
        let case: MappingCase = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("parse mapping.yaml for {rel}: {e}"));
        assert_eq!(
            case.mapping.len() as u64,
            case.count,
            "{rel}: mapping length != count"
        );
        let seed = parse_seed_hex(&case.seed);
        for (i, &expected) in case.mapping.iter().enumerate() {
            let got = compute_shuffled_index::<P>(i as u64, case.count, seed)
                .unwrap_or_else(|e| panic!("{rel} index {i}: {e:?}"));
            assert_eq!(
                got, expected,
                "{rel}: compute_shuffled_index({i}, {}, seed) = {got}, expected {expected}",
                case.count
            );
        }
        passed += 1;
    }
    eprintln!("shuffling/{preset_name}: {passed} cases green");
}

#[test]
fn shuffling_suite_minimal() {
    run_shuffle_suite::<Minimal>("minimal");
}

#[test]
fn shuffling_suite_mainnet() {
    run_shuffle_suite::<Mainnet>("mainnet");
}

// ---------------------------------------------------------------------------
// Fixture helpers for cache tests
// ---------------------------------------------------------------------------

fn active_validator(i: u64) -> Validator {
    Validator {
        pubkey: BlsPublicKey::from_array({
            let mut pk = [0u8; 48];
            pk[0..8].copy_from_slice(&i.to_le_bytes());
            // Make it look non-zero; decode not required for committee tests.
            pk[47] = 0x01;
            pk
        }),
        withdrawal_credentials: Root::from_array({
            let mut c = [0u8; 32];
            c[0] = 0x01;
            c
        }),
        effective_balance: MAX_EFFECTIVE_BALANCE,
        slashed: false,
        activation_eligibility_epoch: Epoch::new(0),
        activation_epoch: Epoch::new(0),
        exit_epoch: FAR_FUTURE_EPOCH,
        withdrawable_epoch: FAR_FUTURE_EPOCH,
    }
}

/// Minimal state with `n` active validators, slot in epoch 1 so decision roots resolve.
fn state_with_validators<P: Preset>(n: usize, slot: Slot) -> BeaconState<P> {
    let mut state = BeaconState::<P>::default();
    state.set_slot(slot);
    for i in 0..n {
        state.validators_push(active_validator(i as u64)).unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    }
    // Seed RANDAO mixes so get_seed is deterministic.
    for i in 0..state.randao_mixes_len() {
        let mut mix = [0u8; 32];
        mix[0] = (i as u8).wrapping_add(1);
        mix[1] = 0xab;
        state.randao_mixes_set(i, Root::from_array(mix)).unwrap();
    }
    // Fill block roots so decision_root_for_epoch works for epoch >= 1.
    for i in 0..state
        .block_roots_len()
        .min(P::SLOTS_PER_HISTORICAL_ROOT as usize)
    {
        let mut r = [0u8; 32];
        r[0] = 0xde;
        r[1] = 0xad;
        r[8..16].copy_from_slice(&(i as u64).to_le_bytes());
        state.block_roots_set(i, Root::from_array(r)).unwrap();
    }
    // Proposer lookahead so get_beacon_proposer_index is defined.
    for i in 0..state.proposer_lookahead_len() {
        state
            .proposer_lookahead_set(i, ValidatorIndex::new((i as u64) % n as u64))
            .unwrap();
    }
    state
}

// ---------------------------------------------------------------------------
// CC-13/6: compute-once + eviction
// ---------------------------------------------------------------------------

#[test]
fn shuffling_cache_compute_once_for_same_key() {
    // Slot in epoch 1 (minimal: 8 slots/epoch → slot 8).
    let slot = Slot::new(P_MIN_SLOT_EPOCH1);
    let state = state_with_validators::<Minimal>(64, slot);
    let epoch = Epoch::new(1);

    let _ = state.caches().committees.take_compute_count();
    let c1 = get_beacon_committee(&state, slot, CommitteeIndex::new(0)).unwrap();
    let after_first = state.caches().committees.compute_count();
    assert_eq!(
        after_first, 1,
        "first committee lookup fills the cache once"
    );

    let c2 = get_beacon_committee(&state, slot, CommitteeIndex::new(0)).unwrap();
    assert_eq!(c1, c2);
    assert_eq!(
        state.caches().committees.compute_count(),
        1,
        "second lookup for same (epoch, decision_root) must not recompute"
    );

    // Another committee index in the same epoch reuses the same shuffling.
    let _ =
        get_beacon_committee(&state, Slot::new(slot.as_u64() + 1), CommitteeIndex::new(0)).unwrap();
    assert_eq!(
        state.caches().committees.compute_count(),
        1,
        "same-epoch committee uses the same cached shuffling"
    );

    let _ = get_or_compute_shuffling(&state, epoch).unwrap();
    assert_eq!(state.caches().committees.compute_count(), 1);
    assert_eq!(state.caches().committees.len(), 1);
}

const P_MIN_SLOT_EPOCH1: u64 = 8; // Minimal SLOTS_PER_EPOCH = 8

#[test]
fn shuffling_cache_lru_eviction_at_capacity() {
    let cap = SHUFFLING_CACHE_DEFAULT_CAPACITY;
    let cache = ShufflingCache::with_capacity(cap);
    assert_eq!(cache.capacity(), cap);

    let epoch = Epoch::new(0);
    for i in 0..(cap + 4) {
        let mut root = [0u8; 32];
        root[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        let key = ShufflingCacheKey {
            epoch,
            decision_root: Root::from_array(root),
        };
        cache.get_or_insert_with(key, || ShuffledCommitteeEpoch {
            shuffled: vec![ValidatorIndex::new(i as u64)],
        });
    }
    assert_eq!(cache.len(), cap, "cache must stay at configured capacity");
    assert_eq!(cache.compute_count(), (cap + 4) as u64);

    // First keys should have been evicted.
    let first_key = ShufflingCacheKey {
        epoch,
        decision_root: Root::from_array({
            let mut r = [0u8; 32];
            r[0..8].copy_from_slice(&0u64.to_le_bytes());
            r
        }),
    };
    assert!(
        !cache.contains(&first_key),
        "oldest entry must be LRU-evicted under pressure"
    );

    // Most recent key must still be present.
    let last_key = ShufflingCacheKey {
        epoch,
        decision_root: Root::from_array({
            let mut r = [0u8; 32];
            r[0..8].copy_from_slice(&((cap + 3) as u64).to_le_bytes());
            r
        }),
    };
    assert!(cache.contains(&last_key));
}

// ---------------------------------------------------------------------------
// Composite key: different decision roots → two entries, different committees
// ---------------------------------------------------------------------------

#[test]
fn shuffling_cache_different_decision_roots_same_epoch() {
    let slot = Slot::new(P_MIN_SLOT_EPOCH1);
    let mut state_a = state_with_validators::<Minimal>(64, slot);
    let mut state_b = state_with_validators::<Minimal>(64, slot);
    let epoch = Epoch::new(1);

    // Distinct decision roots (block root at start_slot(1) - 1 = slot 7).
    let dep_slot = Slot::new(7);
    let idx = (dep_slot.as_u64() % Minimal::SLOTS_PER_HISTORICAL_ROOT) as usize;
    let root_a = Root::from_array([0xaa; 32]);
    let root_b = Root::from_array([0xbb; 32]);
    state_a.block_roots_set(idx, root_a).unwrap();
    state_b.block_roots_set(idx, root_b).unwrap();

    // Different RANDAO mixes → different seeds → different committee assignments.
    for i in 0..state_b.randao_mixes_len() {
        let mut mix = [0xff; 32];
        mix[0] = i as u8;
        state_b.randao_mixes_set(i, Root::from_array(mix)).unwrap();
    }

    assert_eq!(decision_root_for_epoch(&state_a, epoch).unwrap(), root_a);
    assert_eq!(decision_root_for_epoch(&state_b, epoch).unwrap(), root_b);

    // Drive both through a single shared cache to show the composite key.
    let shared = ShufflingCache::with_capacity(16);
    let key_a = ShufflingCacheKey {
        epoch,
        decision_root: root_a,
    };
    let key_b = ShufflingCacheKey {
        epoch,
        decision_root: root_b,
    };
    let shuffle_a = cc_state_transition::compute_shuffled_active_indices(&state_a, epoch).unwrap();
    let shuffle_b = cc_state_transition::compute_shuffled_active_indices(&state_b, epoch).unwrap();
    assert_ne!(
        shuffle_a.shuffled, shuffle_b.shuffled,
        "different seeds must yield different shuffles"
    );
    shared.insert(key_a, shuffle_a.clone());
    shared.insert(key_b, shuffle_b.clone());
    assert_eq!(
        shared.len(),
        2,
        "same epoch + different decision roots → 2 entries"
    );

    let c_a = get_beacon_committee(&state_a, slot, CommitteeIndex::new(0)).unwrap();
    let c_b = get_beacon_committee(&state_b, slot, CommitteeIndex::new(0)).unwrap();
    assert_ne!(
        c_a, c_b,
        "different decision roots / seeds → different committee assignments"
    );
    // Each state cache has its own entry.
    assert_eq!(state_a.caches().committees.len(), 1);
    assert_eq!(state_b.caches().committees.len(), 1);
    assert_ne!(
        decision_root_for_epoch(&state_a, epoch).unwrap(),
        decision_root_for_epoch(&state_b, epoch).unwrap()
    );
}

// ---------------------------------------------------------------------------
// EIP-7917: proposer path does not touch the shuffling cache
// ---------------------------------------------------------------------------

#[test]
fn get_beacon_proposer_index_does_not_touch_shuffling_cache() {
    let slot = Slot::new(P_MIN_SLOT_EPOCH1);
    let state = state_with_validators::<Minimal>(32, slot);
    let _ = state.caches().committees.take_compute_count();
    assert_eq!(state.caches().committees.len(), 0);

    let proposer = get_beacon_proposer_index(&state).unwrap();
    assert_eq!(proposer, ValidatorIndex::new(0)); // lookahead[slot % SPE] seeded as i % n

    assert_eq!(
        state.caches().committees.compute_count(),
        0,
        "proposer lookup must not compute a shuffling"
    );
    assert!(
        state.caches().committees.is_empty(),
        "proposer lookup must not insert into ShufflingCache"
    );
}

// ---------------------------------------------------------------------------
// EpochCache rebuild / invalidate
// ---------------------------------------------------------------------------

#[test]
fn epoch_cache_rebuild_at_epoch_boundary() {
    let slot = Slot::new(P_MIN_SLOT_EPOCH1);
    let mut state = state_with_validators::<Minimal>(16, slot);
    assert!(state.caches().epoch.is_empty());

    rebuild_epoch_cache(&mut state).unwrap();
    let epoch = Epoch::new(1);
    assert!(state.caches().epoch.is_valid_for(epoch));
    assert_eq!(state.caches().epoch.epoch, Some(epoch));
    let tab = state.caches().epoch.total_active_balance.unwrap();
    assert_eq!(tab, 16 * MAX_EFFECTIVE_BALANCE.as_u64());
    assert!(state.caches().epoch.base_reward_per_increment.unwrap() > 0);
    assert_eq!(
        state
            .caches()
            .epoch
            .current_active_indices
            .as_ref()
            .unwrap()
            .len(),
        16
    );
}

#[test]
fn epoch_cache_invalidated_by_effective_balance_change() {
    let slot = Slot::new(P_MIN_SLOT_EPOCH1);
    let mut state = state_with_validators::<Minimal>(8, slot);
    rebuild_epoch_cache(&mut state).unwrap();
    assert!(state.caches().epoch.is_valid_for(Epoch::new(1)));

    // Effective-balance change → invalidate.
    let mut v = *state.validators_get(0).unwrap();
    v.effective_balance = Gwei::new(31_000_000_000);
    state.validators_set(0, v).unwrap();
    invalidate_epoch_cache(&mut state);
    assert!(
        state.caches().epoch.is_empty(),
        "effective-balance change must drop EpochCache"
    );

    // Rebuild after the change reflects the new total.
    rebuild_epoch_cache(&mut state).unwrap();
    let expected = 7 * MAX_EFFECTIVE_BALANCE.as_u64() + 31_000_000_000;
    assert_eq!(state.caches().epoch.total_active_balance, Some(expected));
}

// ---------------------------------------------------------------------------
// OQ-P1-1 input: cold shuffle cost at Hoodi-scale active set (synthetic)
// ---------------------------------------------------------------------------

#[test]
fn cold_shuffling_cost_hoodi_scale_mainnet_rounds() {
    // Hoodi fixture is ~200 MB of state; we measure a synthetic active set sized
    // to a representative Hoodi order of magnitude without loading the SSZ blob.
    // Mainnet SHUFFLE_ROUND_COUNT = 90.
    const ACTIVE: usize = 50_000;
    let slot = Slot::new(Mainnet::SLOTS_PER_EPOCH); // epoch 1
    let state = state_with_validators::<Mainnet>(ACTIVE, slot);
    let epoch = Epoch::new(1);

    let _ = state.caches().committees.take_compute_count();
    let t0 = Instant::now();
    let shuffling = get_or_compute_shuffling(&state, epoch).unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(shuffling.shuffled.len(), ACTIVE);
    assert_eq!(state.caches().committees.compute_count(), 1);

    // Always print so OQ-P1-1 has a measured input in CI logs / commit notes.
    eprintln!(
        "OQ-P1-1 cold shuffling: active={ACTIVE} preset=mainnet rounds={} elapsed={elapsed:?} ({:.3} ms)",
        Mainnet::SHUFFLE_ROUND_COUNT,
        elapsed.as_secs_f64() * 1000.0
    );
}
