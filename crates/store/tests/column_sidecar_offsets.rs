//! Assert `DataColumnSidecar` fixed SSZ offsets and the size formula against a
//! real serialized sidecar (CC-43b / *Values Deliberately Not Invented* 5).
//!
//! Lives outside `src/` so the CC-4M grep
//! (`SignedBeaconBlock|BeaconState|DataColumnSidecar` in `crates/store/src`)
//! stays empty while still discharging the offset + formula assertions.
//!
//! Recorded offsets (commit description anchors):
//! - `index` at byte **0**
//! - header `slot` at byte **20**
//! - size formula: `356 + n × 2144` (45 380 B at n = 21)

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cc_store::{
    BYTES_PER_BLOB_IN_SIDECAR, BlockRegion, COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET,
    COLUMN_HEADER_SLOT_SSZ_OFFSET, COLUMN_INDEX_SSZ_OFFSET, DATA_COLUMN_SIDECAR_FIXED_BYTES,
    Durability, Engine, EngineOptions, column_index_at_offset, column_parent_root_at_offset,
    column_slot_at_offset, columns_for_block, data_column_sidecar_size, get_column_by_root,
    put_column,
};
use cc_types::NUMBER_OF_COLUMNS;
use cc_types::containers::{BeaconBlockHeader, SignedBeaconBlockHeader};
use cc_types::preset::Mainnet;
use cc_types::primitives::{Cell, KzgCommitment, KzgProof, Root, Slot, ValidatorIndex};
use cc_types::sidecar::DataColumnSidecar;
use ssz::{Decode, Encode};
use ssz_types::{FixedVector, VariableList};

/// Build a fully-encoded `DataColumnSidecar` with `n` blobs (real SSZ wire bytes).
fn real_sidecar(index: u64, slot: u64, n_blobs: usize) -> (DataColumnSidecar<Mainnet>, Vec<u8>) {
    let header = SignedBeaconBlockHeader {
        message: BeaconBlockHeader {
            slot: Slot::new(slot),
            proposer_index: ValidatorIndex::new(1),
            parent_root: Root::from_array([0x11; 32]),
            state_root: Root::from_array([0x22; 32]),
            body_root: Root::from_array([0x33; 32]),
        },
        signature: Default::default(),
    };
    let mut cells = Vec::with_capacity(n_blobs);
    let mut commits = Vec::with_capacity(n_blobs);
    let mut proofs = Vec::with_capacity(n_blobs);
    for i in 0..n_blobs {
        // Distinct cell payload so length is driven by n, not zeros collapsing.
        let mut arr = [0u8; Cell::LEN];
        arr[0] = (i as u8).wrapping_add(1);
        cells.push(Cell::from_array(arr));
        commits.push(KzgCommitment::ZERO);
        proofs.push(KzgProof::ZERO);
    }
    let sc = DataColumnSidecar {
        index,
        column: VariableList::new(cells).expect("cells fit MaxBlobCommitmentsPerBlock"),
        kzg_commitments: VariableList::new(commits).expect("commits"),
        kzg_proofs: VariableList::new(proofs).expect("proofs"),
        signed_block_header: header,
        kzg_commitments_inclusion_proof: FixedVector::default(),
    };
    let bytes = sc.as_ssz_bytes();
    (sc, bytes)
}

#[test]
fn size_formula_against_real_sidecar_two_blob_counts() {
    // AC: serialized.len() == 356 + n * 2144 at two different blob counts,
    // with n taken from the fixture rather than from the assertion.
    for n in [0usize, 1, 2, 7, 21] {
        let (sc, bytes) = real_sidecar(0, 3_649_472, n);
        // n from the fixture (list length), not hard-coded in the equality alone.
        let n_from_fixture = sc.column.len();
        assert_eq!(n_from_fixture, n);
        let expected = data_column_sidecar_size(n_from_fixture);
        assert_eq!(
            bytes.len(),
            expected,
            "n={n_from_fixture}: len {} != 356 + n*2144 = {expected}",
            bytes.len()
        );
        assert_eq!(
            expected,
            DATA_COLUMN_SIDECAR_FIXED_BYTES + n_from_fixture * BYTES_PER_BLOB_IN_SIDECAR
        );
    }
    // Explicit 21-blob anchor from the issue (45 380 B).
    let (_, bytes21) = real_sidecar(5, 100, 21);
    assert_eq!(bytes21.len(), 45_380);
    assert_eq!(NUMBER_OF_COLUMNS, 128);
}

#[test]
fn index_at_0_and_header_slot_at_20_match_full_ssz_decode() {
    // Recorded: index @ 0, header slot @ 20.
    let index = 42u64;
    let slot = 3_649_472u64;
    let n = 3usize;
    let (sc, bytes) = real_sidecar(index, slot, n);

    assert!(bytes.len() >= COLUMN_HEADER_SLOT_SSZ_OFFSET + 8);

    // Offset peeks (store path — no container type).
    let from_offset_index = column_index_at_offset(&bytes).expect("index peek");
    let from_offset_slot = column_slot_at_offset(&bytes).expect("slot peek");
    assert_eq!(u64::from(from_offset_index), index);
    assert_eq!(from_offset_slot, Slot::new(slot));

    // Full SSZ decode cross-check.
    let decoded = DataColumnSidecar::<Mainnet>::from_ssz_bytes(&bytes)
        .unwrap_or_else(|e| panic!("full SSZ decode failed: {e:?}"));
    assert_eq!(decoded.index, sc.index);
    assert_eq!(
        decoded.signed_block_header.message.slot,
        sc.signed_block_header.message.slot
    );
    assert_eq!(from_offset_index as u64, decoded.index);
    assert_eq!(from_offset_slot, decoded.signed_block_header.message.slot);
    let from_offset_parent = column_parent_root_at_offset(&bytes).expect("parent peek");
    assert_eq!(
        from_offset_parent,
        decoded.signed_block_header.message.parent_root
    );

    // Constants as documented.
    assert_eq!(COLUMN_INDEX_SSZ_OFFSET, 0);
    assert_eq!(COLUMN_HEADER_SLOT_SSZ_OFFSET, 20);
    assert_eq!(COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, 36);

    // Raw LE reads at the same offsets.
    let raw_index = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let raw_slot = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    assert_eq!(raw_index, index, "index at byte 0");
    assert_eq!(raw_slot, slot, "header slot at byte 20");
}

#[test]
fn real_sidecar_byte_identical_store_roundtrip() {
    let index = 7u16;
    let slot = Slot::new(1_000);
    let root = Root::from_array([0xAB; 32]);
    let (_, bytes) = real_sidecar(u64::from(index), slot.as_u64(), 2);

    let dir = std::env::temp_dir().join(format!(
        "cc-store-col-rt-{}",
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
        put_column(&rt, &mut b, slot, &root, index, &bytes, BlockRegion::Hot).unwrap();
    }
    eng.commit(b).unwrap();
    let got = get_column_by_root(&eng.read().unwrap(), &root, index, Some(BlockRegion::Hot))
        .unwrap()
        .expect("stored");
    assert_eq!(got, bytes, "wire-identical round-trip");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn zero_blob_sidecar_size_and_empty_block_helper() {
    // Zero-blob sidecar is still a valid container (n=0 → 356 B).
    let (sc, bytes) = real_sidecar(0, 3_649_445, 0);
    assert_eq!(sc.column.len(), 0);
    assert_eq!(bytes.len(), data_column_sidecar_size(0));
    assert_eq!(bytes.len(), 356);

    // Store has no column rows for a zero-blob block → empty held set.
    let dir = std::env::temp_dir().join(format!(
        "cc-store-col-zero-{}",
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
    let rt = eng.read().unwrap();
    let res = columns_for_block(
        &rt,
        Slot::new(3_649_445),
        &Root::from_array([0x01; 32]),
        &[0, 1],
        BlockRegion::Hot,
    )
    .unwrap();
    assert!(res.held.is_empty());
    assert!(res.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}
