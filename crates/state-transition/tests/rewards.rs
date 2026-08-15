//! `rewards` runner — CC-13b: flag + inactivity deltas vs vector files.
//!
//! Compares per-validator `{source,target,head,inactivity_penalty}_deltas`
//! against `get_flag_index_deltas` / `get_inactivity_penalty_deltas`. Handlers:
//! `basic`, `leak`, `random`, `inactivity_scores`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use cc_state_transition::helpers::constants::{
    FAR_FUTURE_EPOCH, MAX_EFFECTIVE_BALANCE, TIMELY_HEAD_FLAG_INDEX, TIMELY_SOURCE_FLAG_INDEX,
    TIMELY_TARGET_FLAG_INDEX,
};
use cc_state_transition::{
    EpochError, decrease_balance, get_flag_index_deltas, get_inactivity_penalty_deltas,
    increase_balance, process_justification_and_finalization, process_rewards_and_penalties,
    rebuild_epoch_cache,
};
use cc_types::containers::{Checkpoint, Validator};
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{BlsPublicKey, Epoch, Gwei, Root, Slot, ValidatorIndex};
use cc_types::{BeaconState, ForkName};
const FORK: &str = "fulu";
const RUNNER: &str = "rewards";
const LAYOUT: &str = include_str!("../../../spec-vectors-layout.md");
const LOCKFILE: &str = include_str!("../../../spec-vectors.lock");
const SKIPLIST: &str = include_str!("../../../docs/spec-vectors-skiplist.md");

/// Handlers this runner owns — must equal the on-disk set (CC-13b coverage).
const HANDLERS: &[&str] = &["basic", "inactivity_scores", "leak", "random"];

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

fn snappy_decompress(path: &Path) -> Vec<u8> {
    let compressed = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let claimed = snap::raw::decompress_len(&compressed)
        .unwrap_or_else(|e| panic!("snappy len {}: {e}", path.display()));
    let mut out = vec![0u8; claimed];
    let n = snap::raw::Decoder::new()
        .decompress(&compressed, &mut out)
        .unwrap_or_else(|e| panic!("snappy {}: {e}", path.display()));
    out.truncate(n);
    out
}

fn skiplist_prefixes() -> Vec<String> {
    let mut out = Vec::new();
    let mut in_fence = false;
    for line in SKIPLIST.lines() {
        let line = line.trim();
        if line.starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || line.is_empty() {
            continue;
        }
        if !line.starts_with("- ") && !line.starts_with("* ") {
            continue;
        }
        if let Some(start) = line.find('`')
            && let Some(end) = line[start + 1..].find('`')
        {
            out.push(line[start + 1..start + 1 + end].to_string());
        }
    }
    out
}

fn is_skipped(rel: &str, prefixes: &[String]) -> bool {
    prefixes
        .iter()
        .any(|p| rel == p.as_str() || rel.starts_with(&format!("{p}/")))
}

/// Decode SSZ `Container { rewards: List[uint64], penalties: List[uint64] }`.
fn decode_deltas(bytes: &[u8]) -> (Vec<u64>, Vec<u64>) {
    assert!(bytes.len() >= 8, "deltas too short");
    let off0 = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let off1 = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    assert!(off0 <= off1 && off1 <= bytes.len(), "bad deltas offsets");
    assert_eq!((off1 - off0) % 8, 0, "rewards misaligned");
    assert_eq!((bytes.len() - off1) % 8, 0, "penalties misaligned");
    let n_rewards = (off1 - off0) / 8;
    let n_penalties = (bytes.len() - off1) / 8;
    let mut rewards = Vec::with_capacity(n_rewards);
    let mut penalties = Vec::with_capacity(n_penalties);
    for i in 0..n_rewards {
        let start = off0 + i * 8;
        rewards.push(u64::from_le_bytes(
            bytes[start..start + 8].try_into().unwrap(),
        ));
    }
    for i in 0..n_penalties {
        let start = off1 + i * 8;
        penalties.push(u64::from_le_bytes(
            bytes[start..start + 8].try_into().unwrap(),
        ));
    }
    (rewards, penalties)
}

fn collect_cases(tests: &Path, preset: &str, handler: &str) -> Vec<(String, PathBuf)> {
    let handler_dir = tests.join(preset).join(FORK).join(RUNNER).join(handler);
    if !handler_dir.is_dir() {
        return Vec::new();
    }
    let mut out = Vec::new();
    collect_leaf_cases(&handler_dir, &handler_dir, preset, handler, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn collect_leaf_cases(
    handler_dir: &Path,
    current: &Path,
    preset: &str,
    handler: &str,
    out: &mut Vec<(String, PathBuf)>,
) {
    let mut has_file = false;
    let mut subdirs = Vec::new();
    for ent in fs::read_dir(current).unwrap() {
        let ent = ent.unwrap();
        let ft = ent.file_type().unwrap();
        if ft.is_dir() {
            subdirs.push(ent.path());
        } else if ft.is_file() {
            has_file = true;
        }
    }
    if has_file && current.join("pre.ssz_snappy").is_file() {
        let rel_name = current
            .strip_prefix(handler_dir)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let case_rel = format!("{preset}/{FORK}/{RUNNER}/{handler}/{rel_name}");
        out.push((case_rel, current.to_path_buf()));
        return;
    }
    for sub in subdirs {
        collect_leaf_cases(handler_dir, &sub, preset, handler, out);
    }
}

fn list_handlers(tests: &Path, preset: &str) -> BTreeSet<String> {
    let dir = tests.join(preset).join(FORK).join(RUNNER);
    let mut set = BTreeSet::new();
    for ent in fs::read_dir(&dir).unwrap() {
        let ent = ent.unwrap();
        if ent.file_type().unwrap().is_dir() {
            set.insert(ent.file_name().to_string_lossy().into_owned());
        }
    }
    set
}

fn assert_delta_pair(got: &(Vec<Gwei>, Vec<Gwei>), expected: &(Vec<u64>, Vec<u64>), label: &str) {
    assert_eq!(
        got.0.len(),
        expected.0.len(),
        "{label}: rewards len {} vs {}",
        got.0.len(),
        expected.0.len()
    );
    assert_eq!(
        got.1.len(),
        expected.1.len(),
        "{label}: penalties len {} vs {}",
        got.1.len(),
        expected.1.len()
    );
    for i in 0..got.0.len() {
        assert_eq!(
            got.0[i].as_u64(),
            expected.0[i],
            "{label}: rewards[{i}] got {} expected {}",
            got.0[i].as_u64(),
            expected.0[i]
        );
        assert_eq!(
            got.1[i].as_u64(),
            expected.1[i],
            "{label}: penalties[{i}] got {} expected {}",
            got.1[i].as_u64(),
            expected.1[i]
        );
    }
}

fn run_case<P: Preset>(rel: &str, case_dir: &Path) {
    let pre_bytes = snappy_decompress(&case_dir.join("pre.ssz_snappy"));
    let mut state = BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &pre_bytes)
        .unwrap_or_else(|e| panic!("decode pre {rel}: {e:?}"));

    // Warm epoch cache once (mirrors process_rewards_and_penalties).
    rebuild_epoch_cache(&mut state).unwrap_or_else(|e| panic!("epoch cache {rel}: {e:?}"));

    let source_exp = decode_deltas(&snappy_decompress(
        &case_dir.join("source_deltas.ssz_snappy"),
    ));
    let target_exp = decode_deltas(&snappy_decompress(
        &case_dir.join("target_deltas.ssz_snappy"),
    ));
    let head_exp = decode_deltas(&snappy_decompress(&case_dir.join("head_deltas.ssz_snappy")));
    let inactivity_exp = decode_deltas(&snappy_decompress(
        &case_dir.join("inactivity_penalty_deltas.ssz_snappy"),
    ));

    let source = get_flag_index_deltas(&state, TIMELY_SOURCE_FLAG_INDEX)
        .unwrap_or_else(|e| panic!("source deltas {rel}: {e:?}"));
    let target = get_flag_index_deltas(&state, TIMELY_TARGET_FLAG_INDEX)
        .unwrap_or_else(|e| panic!("target deltas {rel}: {e:?}"));
    let head = get_flag_index_deltas(&state, TIMELY_HEAD_FLAG_INDEX)
        .unwrap_or_else(|e| panic!("head deltas {rel}: {e:?}"));
    let inactivity = get_inactivity_penalty_deltas(&state)
        .unwrap_or_else(|e| panic!("inactivity deltas {rel}: {e:?}"));

    assert_delta_pair(&source, &source_exp, &format!("{rel} source"));
    assert_delta_pair(&target, &target_exp, &format!("{rel} target"));
    assert_delta_pair(&head, &head_exp, &format!("{rel} head"));
    assert_delta_pair(&inactivity, &inactivity_exp, &format!("{rel} inactivity"));
}

fn run_handler<P: Preset>(handler: &str) {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let cases = collect_cases(&tests, P::NAME, handler);
    assert!(
        !cases.is_empty(),
        "expected {handler} cases for {}",
        P::NAME
    );
    let mut ran = 0usize;
    for (rel, dir) in &cases {
        if is_skipped(rel, &prefixes) {
            continue;
        }
        run_case::<P>(rel, dir);
        ran += 1;
    }
    assert!(ran > 0, "all {handler} cases skipped for {}", P::NAME);
}

// ---------------------------------------------------------------------------
// Coverage + handlers
// ---------------------------------------------------------------------------

#[test]
fn handler_coverage() {
    let tests = tests_root();
    for preset in ["mainnet", "minimal"] {
        let on_disk = list_handlers(&tests, preset);
        let declared: BTreeSet<_> = HANDLERS.iter().map(|s| (*s).to_string()).collect();
        assert_eq!(
            on_disk, declared,
            "handler set mismatch for {preset}: on_disk={on_disk:?} declared={declared:?}"
        );
    }
    // Layout mentions rewards suite.
    assert!(
        LAYOUT.contains("fulu/rewards"),
        "layout should list fulu/rewards"
    );
}

macro_rules! rewards_tests {
    ($preset:ident, $mod_name:ident) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn basic() {
                run_handler::<$preset>("basic");
            }
            #[test]
            fn leak() {
                run_handler::<$preset>("leak");
            }
            #[test]
            fn random() {
                run_handler::<$preset>("random");
            }
            #[test]
            fn inactivity_scores() {
                run_handler::<$preset>("inactivity_scores");
            }
        }
    };
}

rewards_tests!(Minimal, minimal);
rewards_tests!(Mainnet, mainnet);

// ---------------------------------------------------------------------------
// Unit: process_justification standalone on cloned state (CC-15b call shape)
// ---------------------------------------------------------------------------

fn synthetic_state<P: Preset>(n: usize, epoch: u64) -> BeaconState<P> {
    let mut state = BeaconState::<P>::default();
    // Epoch processing runs on the last slot of the epoch (`process_slots`).
    // `get_block_root(current_epoch)` needs start_slot < state.slot.
    let slot = Slot::new(
        epoch
            .saturating_mul(P::SLOTS_PER_EPOCH)
            .saturating_add(P::SLOTS_PER_EPOCH.saturating_sub(1)),
    );
    state.set_slot(slot);
    for i in 0..n {
        let mut pk = [0u8; 48];
        pk[0] = (i % 250) as u8;
        pk[1] = (i / 250) as u8;
        state
            .validators_push(Validator {
                pubkey: BlsPublicKey::from_array(pk),
                withdrawal_credentials: Root::ZERO,
                effective_balance: MAX_EFFECTIVE_BALANCE,
                slashed: false,
                activation_eligibility_epoch: Epoch::new(0),
                activation_epoch: Epoch::new(0),
                exit_epoch: FAR_FUTURE_EPOCH,
                withdrawable_epoch: FAR_FUTURE_EPOCH,
            })
            .unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
        // Full target participation previous + current.
        let flags = (1u8 << TIMELY_TARGET_FLAG_INDEX)
            | (1u8 << TIMELY_SOURCE_FLAG_INDEX)
            | (1u8 << TIMELY_HEAD_FLAG_INDEX);
        state.previous_epoch_participation_push(flags).unwrap();
        state.current_epoch_participation_push(flags).unwrap();
        state.inactivity_scores_push(0).unwrap();
    }
    // Seed block roots so get_block_root works for current/previous epochs.
    let start_prev = epoch.saturating_sub(1).saturating_mul(P::SLOTS_PER_EPOCH);
    let start_curr = epoch.saturating_mul(P::SLOTS_PER_EPOCH);
    for s in [start_prev, start_curr] {
        let idx = (s % P::SLOTS_PER_HISTORICAL_ROOT) as usize;
        let mut root = [0u8; 32];
        root[0] = (s & 0xff) as u8;
        root[1] = ((s >> 8) & 0xff) as u8;
        state.block_roots_set(idx, Root::from_array(root)).unwrap();
    }
    // Non-genesis justified checkpoints so FFG rules can fire.
    state.set_previous_justified_checkpoint(Checkpoint {
        epoch: Epoch::new(epoch.saturating_sub(2)),
        root: Root::from_array([1u8; 32]),
    });
    state.set_current_justified_checkpoint(Checkpoint {
        epoch: Epoch::new(epoch.saturating_sub(1)),
        root: Root::from_array([2u8; 32]),
    });
    state.set_finalized_checkpoint(Checkpoint {
        epoch: Epoch::new(epoch.saturating_sub(3)),
        root: Root::from_array([3u8; 32]),
    });
    // justification_bits all set so finalization rules are exercisable.
    let mut bits = state.justification_bits().clone();
    for i in 0..4 {
        bits.set(i, true).unwrap();
    }
    state.set_justification_bits(bits);
    state
}

/// Callable standalone on a clone — only checkpoint / bits fields change.
#[test]
fn justification_standalone_on_cloned_state() {
    // Prefer Hoodi fixture when cached; else synthetic mainnet-scale small set.
    let hoodi_root = std::env::var("HOODI_FIXTURES_CACHE").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.cache/cc-hoodi-fixtures")
    });
    let mut used_hoodi = false;
    if let Ok(entries) = fs::read_dir(&hoodi_root) {
        // Look for beacon_state.ssz under slot dirs.
        for ent in entries.flatten() {
            let state_path = ent.path().join("beacon_state.ssz");
            if state_path.is_file() {
                let bytes = fs::read(&state_path).expect("read hoodi state");
                if let Ok(state) =
                    BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
                {
                    run_justification_clone_check(state);
                    used_hoodi = true;
                    break;
                }
            }
        }
    }
    if !used_hoodi {
        // Synthetic stand-in: same call shape (clone + process, no TransitionContext).
        let state = synthetic_state::<Minimal>(16, 4);
        run_justification_clone_check(state);
    }
}

fn run_justification_clone_check<P: Preset>(state: BeaconState<P>) {
    let before_slot = state.slot();
    let before_balances: Vec<_> = (0..state.balances_len())
        .map(|i| state.balances_get(i).unwrap())
        .collect();
    let before_validators_len = state.validators_len();
    let before_prev_part: Vec<_> = (0..state.previous_epoch_participation_len())
        .map(|i| state.previous_epoch_participation_get(i).unwrap())
        .collect();

    let mut cloned = state.clone();
    process_justification_and_finalization(&mut cloned).expect("standalone justification");

    // Slot / registry / balances / participation unchanged.
    assert_eq!(cloned.slot(), before_slot);
    assert_eq!(cloned.validators_len(), before_validators_len);
    for (i, b) in before_balances.iter().enumerate() {
        assert_eq!(cloned.balances_get(i).unwrap(), *b);
    }
    for (i, f) in before_prev_part.iter().enumerate() {
        assert_eq!(cloned.previous_epoch_participation_get(i).unwrap(), *f);
    }

    // Checkpoint fields are allowed to change (and usually do after epoch 1).
    // At minimum the function is callable and returns Ok without TransitionContext.
    let _ = (
        cloned.previous_justified_checkpoint(),
        cloned.current_justified_checkpoint(),
        cloned.finalized_checkpoint(),
        cloned.justification_bits(),
    );
}

// ---------------------------------------------------------------------------
// EpochCache once-per-epoch (not per-validator)
// ---------------------------------------------------------------------------

#[test]
fn epoch_cache_rebuilt_once_in_process_rewards() {
    let mut state = synthetic_state::<Minimal>(32, 3);
    let _ = state.caches_mut().epoch.take_rebuild_count();
    process_rewards_and_penalties(&mut state).expect("rewards");
    assert_eq!(
        state.caches().epoch.rebuild_count(),
        1,
        "total_active_balance / base_reward_per_increment computed once per epoch transition"
    );
    // Cache remains valid for the current epoch.
    assert!(state.caches().epoch.is_valid_for(Epoch::new(3)));
}

// ---------------------------------------------------------------------------
// Penalty saturates to zero; genuine overflow → Internal EpochError
// ---------------------------------------------------------------------------

#[test]
fn decrease_balance_saturates_at_zero() {
    let mut state = synthetic_state::<Minimal>(4, 2);
    // Drive balance to a small value then penalize more than remaining.
    state.balances_set(0, Gwei::new(100)).unwrap();
    decrease_balance(&mut state, ValidatorIndex::new(0), Gwei::new(1_000)).unwrap();
    assert_eq!(state.balances_get(0).unwrap().as_u64(), 0);
}

#[test]
fn increase_balance_overflow_is_internal() {
    let mut state = synthetic_state::<Minimal>(2, 2);
    state.balances_set(0, Gwei::new(u64::MAX)).unwrap();
    let err = increase_balance(&mut state, ValidatorIndex::new(0), Gwei::new(1)).unwrap_err();
    // Surfaces as BlockError::ArithmeticOverflow (Internal gossip class).
    assert!(matches!(
        err,
        cc_state_transition::BlockError::ArithmeticOverflow
    ));
    assert_eq!(
        err.gossip_class(),
        cc_state_transition::GossipClass::Internal
    );
}

#[test]
fn epoch_arithmetic_overflow_is_internal() {
    // Direct EpochError variant used by handlers.
    let err = EpochError::ArithmeticOverflow;
    let be = cc_state_transition::BlockError::Epoch(err);
    assert_eq!(
        be.gossip_class(),
        cc_state_transition::GossipClass::Internal
    );
}

// ---------------------------------------------------------------------------
// Finalization rules exercised via weigh_justification (synthetic)
// ---------------------------------------------------------------------------

#[test]
fn finalization_rules_fire_on_full_bits() {
    use cc_state_transition::weigh_justification_and_finalization;

    // Rule 4: bits[0:2] set and old_current + 1 == current → finalize old_current.
    let mut state = synthetic_state::<Minimal>(8, 5);
    // After shift, we control balances so both epochs supermajority-justify.
    let total = 8 * MAX_EFFECTIVE_BALANCE.as_u64();
    let target = total; // 100% participation
    // old_current justified at epoch 4 (= current 5 - 1) so rule 4 matches.
    state.set_current_justified_checkpoint(Checkpoint {
        epoch: Epoch::new(4),
        root: Root::from_array([9u8; 32]),
    });
    state.set_previous_justified_checkpoint(Checkpoint {
        epoch: Epoch::new(3),
        root: Root::from_array([8u8; 32]),
    });
    let before_finalized = state.finalized_checkpoint();
    weigh_justification_and_finalization(&mut state, total, target, target).unwrap();
    // With full participation both epochs justify; bits[0] and bits[1] set after
    // shift+set → rule 4 finalizes old_current (epoch 4).
    assert_eq!(state.finalized_checkpoint().epoch, Epoch::new(4));
    assert_ne!(
        state.finalized_checkpoint().root,
        before_finalized.root,
        "finalized root should update under rule 4"
    );
}
