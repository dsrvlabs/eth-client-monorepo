//! CC-4G /4 — atomic cgc+window raise and unit model of the four effects.
//!
//! `ServeWindow` holds `earliest_available_slot` + `cgc` in **one** container
//! (Architecture §2.5 / I3). A raise is one `put_serve_window` / one
//! `write_derived_serve_window` commit — never independent half-writes.
//!
//! # Atomicity
//!
//! Mid-raise abort (`fail_commit = true`, same injection as
//! `fault_injection_after_put_before_commit_lands_nothing` in
//! `crates/store/src/window.rs`) leaves **either** the prior `(cgc, window)`
//! **or** the new pair — never a mixture of old cgc with new eas (or vice
//! versa). With the whole-container put, the durable outcome on abort is the
//! pre-raise record unchanged.
//!
//! # Four effects (synthetic floors)
//!
//! 1. New indices = `custody(8) \ custody(4)` — size 4, exact set.
//! 2. Raise with `C_new > R` → branch 2, eas narrows (correct, §5.4).
//! 3. Row counts for the four *old* column indices unchanged.
//! 4. After backfill extends `C ≤ R`, branch returns to 1.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::U256;
use cc_store::meta::ServeWindow;
use cc_store::split::SLOTS_PER_EPOCH;
use cc_store::{
    earliest_available_slot, load_serve_window, measure_column_class_stats, put_column,
    put_serve_window, write_derived_serve_window, BlockRegion, Durability, Engine, EngineOptions,
    WindowBranch, COLUMN_HEADER_SLOT_SSZ_OFFSET, COLUMN_INDEX_SSZ_OFFSET,
    DATA_COLUMN_SIDECAR_FIXED_BYTES, MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS,
};
use cc_types::{get_custody_groups, Root, Slot};

// ── Engine fixture ──────────────────────────────────────────────────────────

fn temp_engine(label: &str) -> (PathBuf, Engine) {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("cc-storage-cgc-raise-{label}-{n}-{nanos}"));
    let _ = std::fs::remove_dir_all(&dir);
    let eng = Engine::open(
        &dir,
        EngineOptions::default().with_durability(Durability::None),
    )
    .expect("open engine");
    (dir, eng)
}

/// Minimal opaque sidecar body with `index` @ 0 and header `slot` @ 20.
fn synth_sidecar(index: u16, slot: u64) -> Vec<u8> {
    let mut v = vec![0u8; DATA_COLUMN_SIDECAR_FIXED_BYTES];
    v[COLUMN_INDEX_SSZ_OFFSET..COLUMN_INDEX_SSZ_OFFSET + 8]
        .copy_from_slice(&u64::from(index).to_le_bytes());
    v[COLUMN_HEADER_SLOT_SSZ_OFFSET..COLUMN_HEADER_SLOT_SSZ_OFFSET + 8]
        .copy_from_slice(&slot.to_le_bytes());
    v
}

fn root_n(n: u8) -> Root {
    Root::from_array([n; 32])
}

// ── Atomicity ───────────────────────────────────────────────────────────────

/// CC-4G /4 — abort mid-raise leaves old cgc + old window (no mixture).
#[test]
fn cgc_raise_fail_commit_preserves_old_pair() {
    let (_dir, eng) = temp_engine("fail-commit");

    // Pre-raise: cgc=4, branch 1 shape (C deep relative to a high head).
    let current_epoch = 6_000u64;
    let current_slot = Slot::new(current_epoch * SLOTS_PER_EPOCH);
    let r = current_slot.as_u64().saturating_sub(
        MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS.saturating_mul(SLOTS_PER_EPOCH),
    );
    let b = Slot::new(r + 10_000);
    let c_old = Slot::new(r); // C ≤ R → branch 1
    let old = write_derived_serve_window(&eng, b, c_old, 4, &[], current_slot, false)
        .expect("write pre-raise cgc=4 window");
    assert_eq!(old.cgc, 4);
    assert_eq!(old.branch, WindowBranch::One.as_u8());
    assert_eq!(old.earliest_available_slot, b);

    // Raise attempt: cgc=8 with C head-ish (new indices never custodied) — would
    // land branch 2 if committed. Commit is injected to fail after put.
    let c_new = Slot::new(r + 50_000); // C > R → branch 2
    let err = write_derived_serve_window(&eng, b, c_new, 8, &[], current_slot, true)
        .expect_err("fail_commit must abort");
    assert!(
        err.to_string().contains("after_put_before_commit")
            || err.to_string().contains("injected commit"),
        "err={err}"
    );

    // Durable state: still the old pair — never mixed (old cgc + new eas, etc.).
    let loaded = load_serve_window(&eng)
        .expect("load")
        .expect("pre-raise window must still be present");
    assert_eq!(
        loaded, old,
        "abort must leave the pre-raise ServeWindow intact"
    );
    assert_eq!(loaded.cgc, 4, "cgc must remain 4 after aborted raise");
    assert_eq!(
        loaded.earliest_available_slot, old.earliest_available_slot,
        "eas must remain the pre-raise value"
    );
    assert_eq!(loaded.branch, WindowBranch::One.as_u8());
    // Explicit non-mixture: not (cgc=8 with old eas) and not (cgc=4 with new eas).
    assert!(
        !(loaded.cgc == 8 && loaded.earliest_available_slot == old.earliest_available_slot),
        "mixed: new cgc with old eas"
    );
    let would_be_new_eas = b.as_u64().max(c_new.as_u64());
    assert!(
        !(loaded.cgc == 4 && loaded.earliest_available_slot.as_u64() == would_be_new_eas),
        "mixed: old cgc with new eas"
    );
}

/// Successful raise writes cgc=8 with recomputed window in one put.
#[test]
fn cgc_raise_success_atomic_new_pair() {
    let (_dir, eng) = temp_engine("success");

    let current_epoch = 6_000u64;
    let current_slot = Slot::new(current_epoch * SLOTS_PER_EPOCH);
    let r = current_slot.as_u64().saturating_sub(
        MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS.saturating_mul(SLOTS_PER_EPOCH),
    );
    let b = Slot::new(r + 10_000);
    let c_old = Slot::new(r);
    let old =
        write_derived_serve_window(&eng, b, c_old, 4, &[], current_slot, false).expect("pre-raise");
    assert_eq!(old.branch, WindowBranch::One.as_u8());

    // Raise: new column floor head-ish → branch 2, eas narrows to max(B, C).
    let c_new = Slot::new(r + 50_000);
    let raised = write_derived_serve_window(&eng, b, c_new, 8, &[], current_slot, false)
        .expect("raise commit");
    assert_eq!(raised.cgc, 8);
    assert_eq!(raised.branch, WindowBranch::Two.as_u8());
    assert_eq!(
        raised.earliest_available_slot.as_u64(),
        b.as_u64().max(c_new.as_u64()),
        "branch 2 advertises max(B, C)"
    );
    assert!(
        raised.earliest_available_slot.as_u64() > old.earliest_available_slot.as_u64(),
        "raise with incomplete new indices must narrow (raise) the advertised floor"
    );

    let loaded = load_serve_window(&eng).expect("load").expect("present");
    assert_eq!(
        loaded, raised,
        "load must equal the one-put raised container"
    );
}

/// Whole-container put is the only mutator path used by a raise (no half fields).
#[test]
fn cgc_and_window_are_one_container_put() {
    let (_dir, eng) = temp_engine("container");
    let mut batch = eng.batch();
    let window = ServeWindow {
        earliest_available_slot: Slot::new(42),
        cgc: 8,
        branch: WindowBranch::Two.as_u8(),
        block_floor: Slot::new(10),
        column_floor: Slot::new(42),
        holes: Default::default(),
    };
    put_serve_window(&mut batch, &window);
    eng.commit(batch).expect("commit");
    let loaded = load_serve_window(&eng).expect("load").expect("present");
    assert_eq!(loaded.cgc, 8);
    assert_eq!(loaded.earliest_available_slot, Slot::new(42));
    // Both halves arrived together from a single staged put.
    assert_eq!(loaded, window);
}

// ── Four effects (unit / synthetic) ─────────────────────────────────────────

/// Fixed node id for the four-effects model (deterministic custody sets).
const MODEL_NODE: u64 = 0xCC46_0004;

/// Effect 1 — new indices = custody(8) \ custody(4); size 4 and exact set.
#[test]
fn effect1_new_indices_are_custody8_minus_custody4() {
    let node = U256::from(MODEL_NODE);
    let four: BTreeSet<u64> = get_custody_groups(node, 4);
    let eight: BTreeSet<u64> = get_custody_groups(node, 8);
    assert!(four.is_subset(&eight), "subset property is a precondition");
    let new_indices: BTreeSet<u64> = eight.difference(&four).copied().collect();
    assert_eq!(
        new_indices.len(),
        4,
        "cgc 4→8 must add exactly four custody groups; new={new_indices:?} four={four:?} eight={eight:?}"
    );
    // Exact set: recomputing difference is stable.
    let again: BTreeSet<u64> = get_custody_groups(node, 8)
        .difference(&get_custody_groups(node, 4))
        .copied()
        .collect();
    assert_eq!(new_indices, again);
}

/// Effect 2 — after raise with C_new > R, branch 2 and eas narrows.
#[test]
fn effect2_raise_narrows_window_branch_two() {
    let current_epoch = 6_000u64;
    let current_slot = Slot::new(current_epoch * SLOTS_PER_EPOCH);
    let r = current_slot.as_u64().saturating_sub(
        MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS.saturating_mul(SLOTS_PER_EPOCH),
    );
    // Old indices complete over sidecar retention (C ≤ R) → branch 1 advertises B.
    let b = Slot::new(r.saturating_sub(1_000).max(1));
    let c_old = Slot::new(r);
    let (eas_old, branch_old) = earliest_available_slot(b, c_old, current_slot);
    assert_eq!(branch_old, WindowBranch::One);
    assert_eq!(eas_old, b);

    // New indices never custodied → C head-ish > R → branch 2 / sidecar floor.
    let c_new = Slot::new(r + 100_000);
    let (eas_new, branch_new) = earliest_available_slot(b, c_new, current_slot);
    assert_eq!(branch_new, WindowBranch::Two);
    assert_eq!(eas_new.as_u64(), b.as_u64().max(c_new.as_u64()));
    assert!(
        eas_new.as_u64() > eas_old.as_u64(),
        "narrowing: advertised eas rises from {} to {} (correct, not a regression; §5.4)",
        eas_old.as_u64(),
        eas_new.as_u64()
    );
}

/// Effect 3 — row counts for the four *old* indices unchanged across a raise put.
#[test]
fn effect3_old_column_rows_survive_cgc_raise() {
    let (_dir, eng) = temp_engine("rows");
    let node = U256::from(MODEL_NODE);
    let four: Vec<u64> = get_custody_groups(node, 4).into_iter().collect();
    assert_eq!(four.len(), 4);

    // Synthetic rows for the old custodied indices only.
    let slot = Slot::new(1_000);
    let root = root_n(0x41);
    let mut batch = eng.batch();
    {
        let rt = eng.read().unwrap();
        for &idx in &four {
            let index = idx as u16;
            let ssz = synth_sidecar(index, slot.as_u64());
            put_column(&rt, &mut batch, slot, &root, index, &ssz, BlockRegion::Hot).unwrap();
        }
    }
    eng.commit(batch).unwrap();
    let before = measure_column_class_stats(&eng).unwrap();
    assert_eq!(before.columns_rows, 4, "four old-index rows pre-raise");

    // Raise ServeWindow cgc 4→8 (window only; columns untouched).
    let current_slot = Slot::new(6_000 * SLOTS_PER_EPOCH);
    let r = current_slot.as_u64().saturating_sub(
        MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS.saturating_mul(SLOTS_PER_EPOCH),
    );
    write_derived_serve_window(
        &eng,
        Slot::new(r + 10_000),
        Slot::new(r + 50_000),
        8,
        &[],
        current_slot,
        false,
    )
    .expect("raise window");

    let after = measure_column_class_stats(&eng).unwrap();
    assert_eq!(
        after.columns_rows, before.columns_rows,
        "old column rows must be unchanged by a cgc raise put"
    );
    assert_eq!(after.columns_bytes, before.columns_bytes);
}

/// Effect 4 — after simulating backfill of new indices (C ≤ R), branch returns to 1.
#[test]
fn effect4_backfill_returns_branch_one() {
    let current_epoch = 6_000u64;
    let current_slot = Slot::new(current_epoch * SLOTS_PER_EPOCH);
    let r = current_slot.as_u64().saturating_sub(
        MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS.saturating_mul(SLOTS_PER_EPOCH),
    );
    let b = Slot::new(r.saturating_sub(1_000).max(1));

    // Post-raise incomplete: C > R → branch 2.
    let c_raised = Slot::new(r + 50_000);
    let (eas2, br2) = earliest_available_slot(b, c_raised, current_slot);
    assert_eq!(br2, WindowBranch::Two);
    assert_eq!(eas2.as_u64(), b.as_u64().max(c_raised.as_u64()));

    // Simulate backfill of new indices: extend C down to ≤ R.
    let c_backfilled = Slot::new(r);
    let (eas1, br1) = earliest_available_slot(b, c_backfilled, current_slot);
    assert_eq!(br1, WindowBranch::One);
    assert_eq!(eas1, b, "branch 1 re-advertises the block floor B");
    assert!(
        eas1.as_u64() < eas2.as_u64(),
        "backfill widens the advertised window again"
    );
}
