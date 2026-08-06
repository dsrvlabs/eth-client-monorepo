//! CC-1H early gate (CC-13d / M1.2 exit).
//!
//! Measurement: `process_slots` across ≥ 5 epoch boundaries from the committed
//! Hoodi anchor state, reporting per-epoch wall time and the `canonical_root`
//! share. Not part of the default suite — run with:
//!
//! ```text
//! cargo test -p cc-state-transition --test cc1h_early_gate -- --ignored --nocapture
//! ```
//!
//! Results are recorded in `docs/phase-1-soak.md` § CC-1H early gate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use cc_state_transition::{
    process_slots, take_canonical_root_call_count, take_canonical_root_elapsed_ns,
};
use cc_types::preset::{Mainnet, Preset};
use cc_types::primitives::Slot;
use cc_types::{BeaconState, ForkName};

const ANCHOR_SLOT: u64 = 3_649_472;
const ANCHOR_STATE_ROOT: &str =
    "0x2d4f2b8d81bcb72c3556846a532fca7679e9349b0dc37c9c0812f383daa41c55";
const EPOCHS_TO_CROSS: u64 = 5;

fn cache_root() -> PathBuf {
    if let Ok(p) = std::env::var("HOODI_FIXTURES_CACHE") {
        let p = p.trim();
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    let home = std::env::var("HOME").expect("HOME");
    PathBuf::from(home).join(".cache/cc-hoodi-fixtures")
}

fn load_hoodi_state() -> BeaconState<Mainnet> {
    let path = cache_root()
        .join(ANCHOR_SLOT.to_string())
        .join("beacon_state.ssz");
    assert!(
        path.is_file(),
        "Hoodi state missing at {}; run scripts/fetch-hoodi-fixtures.sh",
        path.display()
    );
    let bytes = fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert!(
        bytes.len() as u64 >= 150 * 1024 * 1024,
        "state must be ≥ 150 MB, got {}",
        bytes.len()
    );

    let state = BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("decode Hoodi BeaconState: {e:?}"));
    assert_eq!(state.slot().as_u64(), ANCHOR_SLOT);
    state
}

/// Advance empty slots across ≥ 5 epoch boundaries, measuring per-epoch wall
/// time and `canonical_root` share.
#[test]
#[ignore = "CC-1H early gate — run manually, record in docs/phase-1-soak.md"]
fn process_slots_five_epoch_boundaries_from_hoodi() {
    let mut state = load_hoodi_state();

    // Warm caches once so the first measured epoch is not cold-decode noise.
    let warm_root = state.canonical_root();
    let warm_hex: String = warm_root
        .to_hash256()
        .as_slice()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let expected = ANCHOR_STATE_ROOT.trim_start_matches("0x");
    assert_eq!(warm_hex, expected, "anchor state_root mismatch after warm");

    let slots_per_epoch = Mainnet::SLOTS_PER_EPOCH;
    let start_slot = state.slot().as_u64();
    // Next epoch boundary strictly after current slot.
    let mut next_boundary = ((start_slot / slots_per_epoch) + 1) * slots_per_epoch;

    println!(
        "machine: {} / {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    if let Ok(brand) = std::process::Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
    {
        println!("cpu: {}", String::from_utf8_lossy(&brand.stdout).trim());
    }
    if let Ok(mem) = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        && let Ok(s) = String::from_utf8(mem.stdout)
        && let Ok(bytes) = s.trim().parse::<u64>()
    {
        println!("mem_gb: {:.1}", bytes as f64 / (1024.0 * 1024.0 * 1024.0));
    }
    println!(
        "anchor_slot={start_slot} slots_per_epoch={slots_per_epoch} epochs={EPOCHS_TO_CROSS}"
    );
    println!("epoch_idx,from_slot,to_slot,wall_ms,hash_ms,hash_share_pct,root_calls");

    let mut walls_ms = Vec::new();
    let mut hash_shares = Vec::new();

    for i in 0..EPOCHS_TO_CROSS {
        let from = state.slot().as_u64();
        let to = next_boundary;
        assert!(to > from, "target slot must advance");

        let _ = take_canonical_root_call_count();
        let _ = take_canonical_root_elapsed_ns();
        let t0 = Instant::now();
        process_slots(&mut state, Slot::new(to))
            .unwrap_or_else(|e| panic!("process_slots {from}->{to}: {e}"));
        let wall = t0.elapsed();
        let root_calls = take_canonical_root_call_count();
        let hash_ns = take_canonical_root_elapsed_ns();

        let wall_ms = wall.as_secs_f64() * 1000.0;
        let hash_ms = (hash_ns as f64) / 1_000_000.0;
        let share = if wall_ms > 0.0 {
            (hash_ms / wall_ms) * 100.0
        } else {
            0.0
        };

        println!("{i},{from},{to},{wall_ms:.2},{hash_ms:.2},{share:.1},{root_calls}");
        walls_ms.push(wall_ms);
        hash_shares.push(share);

        assert_eq!(state.slot().as_u64(), to);
        next_boundary = next_boundary.saturating_add(slots_per_epoch);
    }

    let max_wall = walls_ms.iter().cloned().fold(0.0_f64, f64::max);
    let mean_wall = walls_ms.iter().sum::<f64>() / walls_ms.len() as f64;
    let mean_share = hash_shares.iter().sum::<f64>() / hash_shares.len() as f64;
    let max_share = hash_shares.iter().cloned().fold(0.0_f64, f64::max);

    let threshold = if max_wall < 700.0 {
        "<700"
    } else if max_wall < 1500.0 {
        "700-1500"
    } else {
        ">=1500"
    };
    let attribution = if mean_share > 50.0 {
        ">50% hashing → milhouse is the right fix"
    } else if mean_share < 25.0 {
        "<25% hashing → milhouse will not help; transition-side cost"
    } else {
        "25-50% hashing → mixed; re-measure at mid gate"
    };

    println!("summary_max_wall_ms={max_wall:.2}");
    println!("summary_mean_wall_ms={mean_wall:.2}");
    println!("summary_mean_hash_share_pct={mean_share:.1}");
    println!("summary_max_hash_share_pct={max_share:.1}");
    println!("threshold_band={threshold}");
    println!("attribution={attribution}");
    println!(
        "verdict={}",
        match threshold {
            "<700" => "proceed (CC-1H remains P2 contingency; closed for early gate)",
            "700-1500" => "flagged for mid gate (CC-18d)",
            _ => "promote CC-1H to P0 before M1.3",
        }
    );
}
