//! Assert `parent_root` at byte offset 116 against a real Hoodi signed block.
//!
//! Lives outside `src/` so the CC-4M grep
//! (`SignedBeaconBlock|BeaconState|DataColumnSidecar` in `crates/store/src`)
//! stays empty while still discharging *Values Deliberately Not Invented* 5.
//!
//! Skips when the Hoodi fixture cache is absent (same contract as other
//! `HOODI_FIXTURES_CACHE` consumers).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

use cc_store::{
    BlockRegion, Durability, Engine, EngineOptions, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET,
    STATE_ROOT_SSZ_OFFSET, get_block_by_root, parent_root_at_offset, put_block, slot_at_offset,
    state_root_at_offset,
};
use cc_types::preset::Mainnet;
use cc_types::{ForkName, Root, SignedBeaconBlock, Slot};

const CACHE_ENV: &str = "HOODI_FIXTURES_CACHE";
const DEFAULT_CACHE_DIR: &str = "cc-hoodi-fixtures";
const ANCHOR_SLOT: u64 = 3_649_472;
const FETCH_HINT: &str = "run scripts/fetch-hoodi-fixtures.sh";

fn cache_root() -> PathBuf {
    if let Ok(p) = std::env::var(CACHE_ENV) {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").expect("HOME");
    PathBuf::from(home).join(".cache").join(DEFAULT_CACHE_DIR)
}

fn load_hoodi_block() -> Option<(Vec<u8>, String)> {
    // Prefer explicit override.
    if let Ok(p) = std::env::var("CC_43A_HOODI_BLOCK_SSZ") {
        let bytes = std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}"));
        return Some((bytes, format!("CC_43A_HOODI_BLOCK_SSZ={p}")));
    }
    // When CACHE_ENV is unset, still try the default cache path (local dev).
    // Skip only when the file is truly missing so CI without fixtures stays green.
    let path = cache_root()
        .join(ANCHOR_SLOT.to_string())
        .join("signed_beacon_block.ssz");
    if !path.is_file() {
        eprintln!(
            "skip parent_root_offset: Hoodi fixture missing at {} ({FETCH_HINT})",
            path.display()
        );
        return None;
    }
    let bytes = std::fs::read(&path).expect("read fixture");
    Some((
        bytes,
        format!("cache {}/signed_beacon_block.ssz", ANCHOR_SLOT),
    ))
}

#[test]
fn parent_root_offset_116_matches_full_ssz_decode() {
    let Some((bytes, src)) = load_hoodi_block() else {
        return;
    };
    assert!(
        bytes.len() > PARENT_ROOT_SSZ_OFFSET + 32,
        "fixture too short from {src}: {}",
        bytes.len()
    );

    let decoded = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("full SSZ decode failed ({src}): {e:?}"));

    let from_offset = parent_root_at_offset(&bytes).expect("offset peek");
    assert_eq!(
        from_offset, decoded.message.parent_root,
        "offset {PARENT_ROOT_SSZ_OFFSET} must equal full-decode parent_root ({src})"
    );

    // Cross-check slot + state_root offsets on the same fixture.
    assert_eq!(
        slot_at_offset(&bytes).unwrap(),
        decoded.message.slot,
        "slot offset {SLOT_SSZ_OFFSET}"
    );
    assert_eq!(
        state_root_at_offset(&bytes).unwrap(),
        decoded.message.state_root,
        "state_root offset {STATE_ROOT_SSZ_OFFSET}"
    );

    // Document the arithmetic the architecture states.
    assert_eq!(PARENT_ROOT_SSZ_OFFSET, 116);
    assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 100);
}

#[test]
fn real_hoodi_block_byte_identical_store_roundtrip() {
    // CC-43 /5 on a real fixture (not synthetic).
    let Some((bytes, src)) = load_hoodi_block() else {
        return;
    };
    let decoded = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("decode ({src}): {e:?}"));
    let root = Root::from(decoded.canonical_root());
    let slot = decoded.message.slot;

    let dir = std::env::temp_dir().join(format!(
        "cc-store-hoodi-rt-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let eng = Engine::open(
        &dir,
        EngineOptions::default().with_durability(Durability::None),
    )
    .unwrap();
    let mut b = eng.batch();
    {
        let rt = eng.read().unwrap();
        put_block(&rt, &mut b, slot, &root, &bytes, BlockRegion::Hot, true).unwrap();
    }
    eng.commit(b).unwrap();
    let got = get_block_by_root(&eng.read().unwrap(), &root)
        .unwrap()
        .expect("stored");
    assert_eq!(got, bytes, "wire-identical round-trip ({src})");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn offset_constants_documented() {
    assert_eq!(PARENT_ROOT_SSZ_OFFSET, 116);
    assert_eq!(SLOT_SSZ_OFFSET, 100);
    assert_eq!(STATE_ROOT_SSZ_OFFSET, 148);
    // Slot type used so the integration crate links Slot.
    let _ = Slot::new(0);
}
