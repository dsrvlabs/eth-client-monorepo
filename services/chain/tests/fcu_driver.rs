//! CC-33 acceptance tests for the forkchoiceUpdated driver.
//!
//! Lives outside `src/fcu_driver.rs` so that file's only mentions of
//! `execution_block_hash` are the three MP-4 reads (head / safe / finalized).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::thread;

use cc_chain::{FcuDriver, RecordingFcuSink, build_forkchoice_state, safe_is_ancestor_of_head};
use cc_fork_choice::{ExecutionStatus, ProtoArray, ProtoNodeBlock};
use cc_types::containers::Checkpoint;
use cc_types::primitives::{Epoch, Hash256, Root, Slot};

fn root_of(i: u64) -> Root {
    let mut a = [0u8; 32];
    a[..8].copy_from_slice(&i.to_le_bytes());
    Root::from(a)
}

fn exec_of(i: u64) -> Hash256 {
    // Distinct from beacon root so a header-field mistake fails the triple test.
    let mut a = [0u8; 32];
    a[..8].copy_from_slice(&(i.wrapping_add(0xE100)).to_le_bytes());
    a[31] = 0xEE;
    Hash256::from(a)
}

fn cp(epoch: u64, r: Root) -> Checkpoint {
    Checkpoint {
        epoch: Epoch::new(epoch),
        root: r,
    }
}

fn insert(pa: &mut ProtoArray, slot: u64, r: Root, parent: Option<Root>, exec: Hash256) {
    let anchor = cp(0, root_of(1));
    pa.on_block(ProtoNodeBlock {
        slot: Slot::new(slot),
        root: r,
        parent_root: parent,
        state_root: r,
        target_root: r,
        justified_checkpoint: anchor,
        finalized_checkpoint: anchor,
        unrealized_justified_checkpoint: anchor,
        unrealized_finalized_checkpoint: anchor,
        execution_status: ExecutionStatus::Valid,
        execution_block_hash: exec,
    })
    .unwrap();
}

/// Linear chain of `n` blocks; returns (array, expected exec hashes by 1-based index).
fn linear_chain(n: usize) -> (ProtoArray, Vec<Hash256>) {
    let anchor = cp(0, root_of(1));
    let mut pa = ProtoArray::new(anchor, anchor);
    let mut expected = Vec::with_capacity(n + 1);
    expected.push(Hash256::ZERO); // 1-based
    for i in 1..=n as u64 {
        let r = root_of(i);
        let parent = if i == 1 { None } else { Some(root_of(i - 1)) };
        let exec = exec_of(i);
        insert(&mut pa, i, r, parent, exec);
        expected.push(exec);
    }
    (pa, expected)
}

/// CC-33 /1: 200-block replay; safe is always ancestor of head; hashes from
/// the proto-array payload-hash field only (not beacon roots).
#[test]
fn fcu_triple_safe_is_ancestor() {
    const N: usize = 200;
    let (pa, expected) = linear_chain(N);
    let sink = Arc::new(RecordingFcuSink::default());
    let driver = FcuDriver::new(Arc::clone(&sink));

    let justified = root_of(1);
    let finalized = root_of(1);

    for head_i in 1..=N as u64 {
        let head = root_of(head_i);
        let state = driver
            .build_from_proto_array(&pa, head, justified, finalized)
            .unwrap();

        assert_eq!(state.head_block_hash, expected[head_i as usize]);
        assert_eq!(state.safe_block_hash, expected[1]);
        assert_eq!(state.finalized_block_hash, expected[1]);
        // Not the beacon root (would mean we read the wrong field).
        assert_ne!(state.head_block_hash, Hash256::from(head));

        assert!(
            safe_is_ancestor_of_head(&pa, state.safe_root, state.head_root),
            "safe must be ancestor of head at emission head={head_i}"
        );
        assert!(driver.try_emit(state).unwrap());
    }
    assert_eq!(sink.request_count(), N);
}

/// CC-33 /2 both halves: 50 concurrent updates → ordered, and drops (< 50).
#[test]
fn fcu_fifty_concurrent_updates() {
    let (pa, _) = linear_chain(50);
    let sink = Arc::new(RecordingFcuSink::default());
    let driver = Arc::new(FcuDriver::new(Arc::clone(&sink)));

    let justified = root_of(1);
    let finalized = root_of(1);

    let mut states = Vec::with_capacity(50);
    for i in 1..=50u64 {
        let s = driver
            .build_from_proto_array(&pa, root_of(i), justified, finalized)
            .unwrap();
        states.push(s);
    }

    let mut handles = Vec::new();
    for state in states {
        let d = Arc::clone(&driver);
        handles.push(thread::spawn(move || d.try_emit(state)));
    }
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let observed = sink.snapshot();
    let n = observed.len();
    assert!(
        n < 50,
        "superseded calls must be dropped, not delayed (got {n} emissions)"
    );
    assert!(n >= 1, "at least the latest emission must land");
    assert!(
        driver.dropped_stale_total() > 0,
        "dropped-stale counter must increment"
    );

    for w in observed.windows(2) {
        assert!(
            w[0].sequence < w[1].sequence,
            "emissions must be in sequence order (no interleaving)"
        );
        assert!(
            w[0].head_slot.as_u64() <= w[1].head_slot.as_u64(),
            "emissions must respect fork-choice (head slot) order"
        );
    }
}

/// CC-33 /7: exactly one fcU per slot over 5 slots when the block feed stops.
#[test]
fn fcu_per_slot_floor() {
    let (pa, _) = linear_chain(3);
    let sink = Arc::new(RecordingFcuSink::default());
    let driver = FcuDriver::new(Arc::clone(&sink));

    let state = driver
        .build_from_proto_array(&pa, root_of(3), root_of(1), root_of(1))
        .unwrap();
    assert!(driver.try_emit(state).unwrap());
    let after_head = sink.request_count();
    assert_eq!(after_head, 1);

    for slot in 10..15 {
        assert!(
            driver.on_slot(Slot::new(slot)).unwrap(),
            "slot {slot} must emit the floor"
        );
        assert!(!driver.on_slot(Slot::new(slot)).unwrap());
    }
    assert_eq!(
        sink.request_count(),
        after_head + 5,
        "exactly one fcU per slot over 5 slots"
    );
}

#[test]
fn build_reads_three_payload_hashes() {
    let (pa, expected) = linear_chain(3);
    let state = build_forkchoice_state(&pa, root_of(3), root_of(1), root_of(1), 1).unwrap();
    assert_eq!(state.head_block_hash, expected[3]);
    assert_eq!(state.safe_block_hash, expected[1]);
    assert_eq!(state.finalized_block_hash, expected[1]);
}

#[test]
fn fcu_driver_source_has_exactly_three_payload_hash_reads() {
    // AC: grep -n 'execution_block_hash' crates/chain-core/src/fcu_driver.rs
    // returns three uses — head, safe, finalized.
    let src = include_str!("../../../crates/chain-core/src/fcu_driver.rs");
    let count = src.matches("execution_block_hash").count();
    assert_eq!(
        count, 3,
        "fcu_driver.rs must contain exactly three execution_block_hash uses (got {count})"
    );
}
