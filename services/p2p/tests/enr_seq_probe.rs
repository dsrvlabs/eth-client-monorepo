//! CC-21a / CC-21/1 — A-P2-4 ENR sequence probe.
//!
//! Against a real `Discv5` handle with **no network** (`start` never called):
//! 1. `enr_insert("cgc", &v)` strictly increases `local_enr().seq()` and the
//!    resulting ENR verifies against its own public key.
//! 2. `EnrManager::apply` with two field changes bumps `seq` by **exactly one**.
//! 3. The rebuild-and-replace strategy satisfies the same three properties.
//!
//! Outcome is recorded in `docs/phase-2-soak.md` §`CC-21/1 ENR sequence`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cc_p2p::discovery::{EnrFieldChange, EnrManager, EnrSeqStrategy};

/// Probe payload for `cgc` — arbitrary bytes; encoding fidelity is CC-21c.
const CGC_PROBE: &[u8] = &[4];
const NFD_PROBE: &[u8] = &[0, 0, 0, 0];

/// A-P2-4: `Discv5::enr_insert` bumps `seq` and re-signs.
///
/// Constructs a real handle on loopback config, never starts the service, never
/// dials, never writes `./data/node_key`.
#[test]
fn enr_insert_bumps_seq_and_resigns() {
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).expect("ephemeral discv5");
    let discv5 = manager.discv5();

    let seq_before = discv5.local_enr().seq();
    discv5
        .enr_insert("cgc", &CGC_PROBE)
        .expect("enr_insert(cgc) must succeed");

    let enr_after = discv5.local_enr();
    assert!(
        enr_after.seq() > seq_before,
        "enr_insert must strictly increase seq (before={seq_before}, after={})",
        enr_after.seq()
    );
    assert!(
        enr_after.verify(),
        "enr_insert must re-sign: resulting ENR must verify against its public key"
    );
    // Field present as RLP string payload (raw RLP includes the string header).
    let raw = enr_after.get_raw_rlp("cgc").expect("cgc key present");
    assert!(
        !raw.is_empty(),
        "cgc RLP payload must be non-empty after insert"
    );
}

/// `EnrManager::apply` batching contract (§6.2): two field changes → one bump.
#[test]
fn apply_batch_bumps_seq_exactly_once() {
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).expect("ephemeral discv5");
    let seq_before = manager.local_enr().seq();

    manager
        .apply([
            EnrFieldChange::new("cgc", CGC_PROBE),
            EnrFieldChange::new("nfd", NFD_PROBE),
        ])
        .expect("apply batch");

    let enr_after = manager.local_enr();
    assert_eq!(
        enr_after.seq(),
        seq_before + 1,
        "batch apply must bump seq by exactly one (before={seq_before}, after={})",
        enr_after.seq()
    );
    assert!(
        enr_after.verify(),
        "batch apply must leave a signature that verifies"
    );

    assert!(
        enr_after.get_raw_rlp("cgc").is_some(),
        "cgc must be present after batch apply"
    );
    assert!(
        enr_after.get_raw_rlp("nfd").is_some(),
        "nfd must be present after batch apply"
    );
}

/// Single-field `apply` via the `EnrInsert` strategy (one `enr_insert` call).
#[test]
fn apply_single_field_via_enr_insert() {
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).expect("ephemeral discv5");
    let seq_before = manager.local_enr().seq();

    manager
        .apply([EnrFieldChange::new("cgc", CGC_PROBE)])
        .expect("apply single");

    let enr_after = manager.local_enr();
    assert_eq!(enr_after.seq(), seq_before + 1);
    assert!(enr_after.verify());
}

/// Rebuild-and-replace fallback: same three properties as the probe.
///
/// Kept green even when the default strategy is `EnrInsert`, so a strategy
/// flip is a one-line change with a pre-proven path.
#[test]
fn rebuild_and_replace_bumps_seq_and_resigns() {
    let manager =
        EnrManager::new_ephemeral(EnrSeqStrategy::RebuildAndReplace).expect("ephemeral discv5");
    let seq_before = manager.local_enr().seq();

    manager
        .apply([EnrFieldChange::new("cgc", CGC_PROBE)])
        .expect("rebuild apply");

    let enr_after = manager.local_enr();
    assert!(
        enr_after.seq() > seq_before,
        "rebuild-and-replace must strictly increase seq"
    );
    assert_eq!(enr_after.seq(), seq_before + 1);
    assert!(
        enr_after.verify(),
        "rebuild-and-replace must re-sign: ENR must verify"
    );

    // Batch under rebuild strategy also coalesces to one bump.
    let seq_mid = enr_after.seq();
    manager
        .apply([
            EnrFieldChange::new("cgc", [8]),
            EnrFieldChange::new("nfd", NFD_PROBE),
        ])
        .expect("rebuild batch");
    let enr_batch = manager.local_enr();
    assert_eq!(enr_batch.seq(), seq_mid + 1);
    assert!(enr_batch.verify());
}

/// Empty batch is a no-op (no sequence bump).
#[test]
fn apply_empty_batch_is_noop() {
    let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).expect("ephemeral discv5");
    let seq_before = manager.local_enr().seq();
    manager.apply([]).expect("empty apply");
    assert_eq!(manager.local_enr().seq(), seq_before);
}
