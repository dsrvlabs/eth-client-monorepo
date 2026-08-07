//! 100 k-node proto-array head computation benchmark (CC-15/5).
//!
//! Target: p95 < 10 ms over a contiguous ≥ 100 k-node tree. Catches accidental
//! O(n²) (typically `get_ancestor` inside the weight pass).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_fork_choice::{HarnessAvailability, ProtoArray, ProtoNodeBlock, Store, get_head};
use cc_state_transition::StubOptimisticEngine;
use cc_types::containers::Checkpoint;
use cc_types::preset::{Minimal, Preset};
use cc_types::primitives::{Epoch, Root, Slot};

const N_NODES: usize = 100_000;
const SAMPLES: usize = 50;

fn root_from_u64(i: u64) -> Root {
    let mut a = [0u8; 32];
    a[..8].copy_from_slice(&i.to_le_bytes());
    Root::from_array(a)
}

fn build_linear_proto_array(n: usize) -> ProtoArray {
    let anchor = Checkpoint {
        epoch: Epoch::new(0),
        root: root_from_u64(0),
    };
    let mut pa = ProtoArray::new(anchor, anchor);
    for i in 0..n {
        let parent = if i == 0 {
            None
        } else {
            Some(root_from_u64((i as u64) - 1))
        };
        pa.on_block(ProtoNodeBlock {
            slot: Slot::new(i as u64),
            root: root_from_u64(i as u64),
            parent_root: parent,
            state_root: root_from_u64(i as u64),
            target_root: root_from_u64(i as u64),
            justified_checkpoint: anchor,
            finalized_checkpoint: anchor,
            unrealized_justified_checkpoint: anchor,
            unrealized_finalized_checkpoint: anchor,
        })
        .expect("insert");
    }
    pa
}

fn main() {
    println!("cc-fork-choice head bench: {N_NODES} nodes, {SAMPLES} samples");

    // --- Pure proto-array score + find_head path (the O(N) kernel) ----------
    let mut pa = build_linear_proto_array(N_NODES);
    let justified = Checkpoint {
        epoch: Epoch::new(0),
        root: root_from_u64(0),
    };
    let deltas = vec![0i64; N_NODES];
    let mut times = Vec::with_capacity(SAMPLES);

    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        pa.apply_score_changes(
            black_box(deltas.clone()),
            justified,
            justified,
            Root::ZERO,
            0,
            Epoch::new(0),
            Minimal::SLOTS_PER_EPOCH,
        )
        .expect("score");
        let head = pa
            .find_head(justified.root, Epoch::new(0), Minimal::SLOTS_PER_EPOCH)
            .expect("head");
        black_box(head);
        times.push(t0.elapsed());
    }

    times.sort();
    let p50 = times[times.len() / 2];
    let p95 = times[(times.len() * 95) / 100];
    let max = *times.last().expect("samples");
    println!("proto_array apply_score_changes+find_head: p50={p50:?} p95={p95:?} max={max:?}");

    // --- Store get_head smoke (cache path) ----------------------------------
    let anchor = Checkpoint {
        epoch: Epoch::new(0),
        root: root_from_u64(0),
    };
    let mut store: Store<Minimal> = Store::new(
        0,
        0,
        6,
        anchor,
        anchor,
        0,
        Arc::new(StubOptimisticEngine),
        Arc::new(HarnessAvailability),
    );
    let _ = get_head(&mut store);

    let budget = Duration::from_millis(10);
    assert!(
        p95 < budget,
        "CC-15/5: head p95 {p95:?} must be < {budget:?} over {N_NODES} nodes"
    );
    println!("CC-15/5 PASS: p95 {p95:?} < 10ms on {N_NODES}-node proto-array");
}
