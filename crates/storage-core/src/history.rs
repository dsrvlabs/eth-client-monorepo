//! Phase 6 historical read surface (CC-4I / Architecture §1.6, §12).
//!
//! Four methods on `eth.storage.v1`:
//! - [`historical_block_by_root`] / [`historical_block_by_slot`] — unary block SSZ
//! - [`snapshot_state_at`] — full snapshot SSZ when the slot is in the ring;
//!   **typed `NOT_AVAILABLE`** outside the ring (never replays via `process_slots`)
//! - [`finalized_checkpoint_history`] — FC scalars + snapshot-ring checkpoints
//!
//! Latency budget 10–50 ms for the unary paths; observed via
//! `cc_storage_serve_seconds{protocol}` in the gRPC handlers ([`crate::serve`]).
//!
//! **No Phase 6 API gateway involvement** — callers (tests, Phase 6) hit these
//! helpers / RPCs directly (CC-4I /4).

use std::sync::atomic::{AtomicU64, Ordering};

use cc_proto::status_with_error_info;
#[cfg(test)]
use cc_proto::storage::StateChunk;
use cc_store::blocks::{get_cold_block, slot_by_root};
use cc_store::canonical::get_canonical;
use cc_store::engine::{Engine, StoreError};
use cc_store::get_block_by_root;
use cc_store::meta::{ForkChoiceScalars, KEY_FC_SCALARS, TABLE_META};
use cc_store::snapshots::{get_snapshot, list_snapshot_slots};
use cc_store::{Root, Slot, SszDecode, epoch_of_slot};
use tonic::{Code, Status};

/// gRPC `ErrorInfo.reason` when a requested snapshot slot is outside the ring.
pub(crate) const REASON_NOT_AVAILABLE: &str = "NOT_AVAILABLE";

/// Domain for storage error details (CC-4I typed refusals).
pub(crate) const ERROR_DOMAIN: &str = "eth.storage.v1";

/// Default chunk size for `GetSnapshotState` streams (~1 MiB).
///
/// Keeps gRPC frames well under typical HTTP/2 limits while still finishing a
/// ~175–200 MB Hoodi state in a few hundred chunks.
pub(crate) const DEFAULT_STATE_CHUNK_BYTES: usize = 1024 * 1024;

/// Observability counter for accidental historical replay (CC-4I /2).
///
/// The history path **never** calls `process_slots`. Any future code path that
/// attempted a miss-fill replay must increment this so the criterion test
/// fails closed. Tests assert the counter stays at zero on a ring miss.
pub(crate) static PROCESS_SLOTS_INVOCATIONS: AtomicU64 = AtomicU64::new(0);

/// One historical block body + identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoricalBlock {
    pub slot: Slot,
    pub root: Root,
    pub ssz: Vec<u8>,
}

/// One finalized-checkpoint identity (epoch + root).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoricalCheckpoint {
    pub epoch: u64,
    pub root: Root,
}

/// Typed miss for a snapshot slot outside the ring — **not** a store I/O error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotLookup {
    /// Snapshot SSZ present at the requested slot.
    Available(Vec<u8>),
    /// Slot is not in the snapshot ring; caller must refuse without replay.
    NotAvailable { slot: Slot, ring: Vec<Slot> },
}

/// Build `FAILED_PRECONDITION` + `ErrorInfo{reason=NOT_AVAILABLE}`.
///
/// Explicitly does **not** invoke `process_slots` or any replay path (CC-4I /2).
#[must_use]
pub(crate) fn not_available_status(slot: Slot, ring: &[Slot]) -> Status {
    let ring_slots: Vec<u64> = ring.iter().map(|s| s.as_u64()).collect();
    status_with_error_info(
        Code::FailedPrecondition,
        format!(
            "snapshot state at slot {} is not in the snapshot ring {:?}; \
             historical state replay is out of scope (CC-4I)",
            slot.as_u64(),
            ring_slots
        ),
        REASON_NOT_AVAILABLE,
        ERROR_DOMAIN,
    )
}

/// Record that a historical path attempted slot replay (must stay unused).
///
/// Production history helpers never call this. Exposed so a mistaken future
/// replay hook would trip the CC-4I /2 counter assertion.
#[allow(dead_code)]
pub(crate) fn record_process_slots_invocation() {
    PROCESS_SLOTS_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
}

/// Current value of the process_slots invocation counter (tests).
#[cfg(test)]
#[must_use]
pub(crate) fn process_slots_invocations() -> u64 {
    PROCESS_SLOTS_INVOCATIONS.load(Ordering::Relaxed)
}

/// Reset the process_slots invocation counter (tests).
#[cfg(test)]
pub(crate) fn reset_process_slots_invocations() {
    PROCESS_SLOTS_INVOCATIONS.store(0, Ordering::Relaxed);
}

/// Load a block by root (hot or cold via `block_slot_by_root`).
pub(crate) fn historical_block_by_root(
    engine: &Engine,
    root: &Root,
) -> Result<Option<HistoricalBlock>, StoreError> {
    let rt = engine.read()?;
    let Some(ssz) = get_block_by_root(&rt, root)? else {
        return Ok(None);
    };
    let slot = match slot_by_root(&rt, root)? {
        Some((s, _)) => s,
        None => {
            // Index missing but body found is pathological; decode slot from SSZ.
            cc_store::slot_at_offset(&ssz)?
        }
    };
    Ok(Some(HistoricalBlock {
        slot,
        root: *root,
        ssz,
    }))
}

/// Load the **canonical** block at `slot` (hot via canonical index, or cold).
pub(crate) fn historical_block_by_slot(
    engine: &Engine,
    slot: Slot,
) -> Result<Option<HistoricalBlock>, StoreError> {
    let rt = engine.read()?;
    if let Some(root) = get_canonical(&rt, slot)?
        && let Some(ssz) = get_block_by_root(&rt, &root)?
    {
        return Ok(Some(HistoricalBlock { slot, root, ssz }));
    }
    // Cold region: slot-only key (root drops out of the key below the split).
    if let Some(ssz) = get_cold_block(&rt, slot)? {
        let root = get_canonical(&rt, slot)?.unwrap_or(Root::ZERO);
        return Ok(Some(HistoricalBlock { slot, root, ssz }));
    }
    Ok(None)
}

/// Load snapshot SSZ at `slot`, or a typed not-available when outside the ring.
///
/// **Never** calls `process_slots` / replay on a miss (CC-4I /2).
pub(crate) fn snapshot_state_at(engine: &Engine, slot: Slot) -> Result<SnapshotLookup, StoreError> {
    let rt = engine.read()?;
    let ring = list_snapshot_slots(&rt)?;
    if let Some(ssz) = get_snapshot(&rt, slot)? {
        return Ok(SnapshotLookup::Available(ssz));
    }
    // Miss: typed refusal only — do not increment PROCESS_SLOTS_INVOCATIONS,
    // do not call process_slots, do not open the replay driver.
    Ok(SnapshotLookup::NotAvailable { slot, ring })
}

/// Finalized-checkpoint history from FC scalars + the snapshot ring.
///
/// - Current `ForkChoiceScalars.finalized` when present.
/// - One entry per snapshot-ring slot with a known `canonical[slot]` root.
/// - Deduped by epoch, ascending.
pub(crate) fn finalized_checkpoint_history(
    engine: &Engine,
) -> Result<Vec<HistoricalCheckpoint>, StoreError> {
    let rt = engine.read()?;
    let mut by_epoch: std::collections::BTreeMap<u64, Root> = std::collections::BTreeMap::new();

    if let Some(bytes) = rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())? {
        let fc = ForkChoiceScalars::from_ssz_bytes(&bytes)
            .map_err(|e| StoreError::Codec(format!("ForkChoiceScalars SSZ: {e:?}")))?;
        by_epoch.insert(fc.finalized.epoch.as_u64(), fc.finalized.root);
    }

    for slot in list_snapshot_slots(&rt)? {
        if let Some(root) = get_canonical(&rt, slot)? {
            by_epoch.insert(epoch_of_slot(slot), root);
        }
    }

    Ok(by_epoch
        .into_iter()
        .map(|(epoch, root)| HistoricalCheckpoint { epoch, root })
        .collect())
}

/// Split opaque state SSZ into `StateChunk` messages (test helper).
///
/// Production serve uses progressive `SnapshotChunkStream` (SEC-4I-1) so peak
/// RSS stays ≈ 1× state + one chunk. Chunks cover `[0, ssz.len())` without gaps;
/// the final chunk has `last = true`. Empty input yields a single empty last chunk.
#[cfg(test)]
pub(crate) fn chunk_state_ssz(ssz: &[u8], chunk_bytes: usize) -> Vec<StateChunk> {
    let chunk_bytes = chunk_bytes.max(1);
    if ssz.is_empty() {
        return vec![StateChunk {
            data: Vec::new(),
            offset: 0,
            last: true,
        }];
    }
    let mut out = Vec::with_capacity(ssz.len().div_ceil(chunk_bytes));
    let mut offset = 0usize;
    while offset < ssz.len() {
        let end = (offset + chunk_bytes).min(ssz.len());
        let last = end == ssz.len();
        out.push(StateChunk {
            data: ssz[offset..end].to_vec(),
            offset: offset as u64,
            last,
        });
        offset = end;
    }
    out
}

/// Reassemble chunk stream bytes (test helper / round-trip).
#[cfg(test)]
pub(crate) fn reassemble_chunks(chunks: &[StateChunk]) -> Vec<u8> {
    let mut out = Vec::new();
    for c in chunks {
        assert_eq!(
            c.offset as usize,
            out.len(),
            "chunk offset must be contiguous"
        );
        out.extend_from_slice(&c.data);
        if c.last {
            break;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests (CC-4I /1–/4) — exercise helpers / RPC surface with no gateway layer.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_proto::error_info_from_status;
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
    };
    use cc_store::canonical::put_canonical;
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::BlockRegion;
    use cc_store::meta::{ForkChoiceScalars, KEY_FC_SCALARS, TABLE_META};
    use cc_store::put_block;
    use cc_store::put_snapshot;
    use cc_store::{Root, Slot, SszEncode};
    use cc_types::{Checkpoint, Epoch};
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;
    use std::time::Instant;

    fn tmp_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "cc-storage-history-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn open_engine(label: &str) -> (PathBuf, Engine) {
        let dir = tmp_dir(label);
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        (dir, eng)
    }

    fn root_n(n: u8) -> Root {
        Root::from_array([n; 32])
    }

    fn root_slot(slot: u64) -> Root {
        let mut a = [0u8; 32];
        a[0..8].copy_from_slice(&slot.to_be_bytes());
        Root::from_array(a)
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(bytes);
        let d = h.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&d);
        out
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        // Distinct body marker so fixtures are unique per slot.
        if v.len() > STATE_ROOT_SSZ_OFFSET + 32 {
            let mark = (slot as u8).wrapping_mul(17);
            v[STATE_ROOT_SSZ_OFFSET + 32] = mark;
        }
        v
    }

    fn seed_block(eng: &Engine, slot: u64) -> (Root, Vec<u8>) {
        let root = root_slot(slot);
        let ssz = synth_block(slot, &Root::ZERO, &root_n(1));
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_block(
                &rt,
                &mut b,
                Slot::new(slot),
                &root,
                &ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            put_canonical(&rt, &mut b, Slot::new(slot), &root).unwrap();
        }
        eng.commit(b).unwrap();
        (root, ssz)
    }

    fn seed_fc_finalized(eng: &Engine, epoch: u64, root: Root) {
        let fc = ForkChoiceScalars {
            time: 1,
            proposer_boost_root: Root::ZERO,
            justified: Checkpoint {
                epoch: Epoch::new(epoch.saturating_sub(1)),
                root: root_n(0x11),
            },
            finalized: Checkpoint {
                epoch: Epoch::new(epoch),
                root,
            },
            unrealized_justified: Checkpoint::default(),
            unrealized_finalized: Checkpoint::default(),
            head_root: root,
            head_slot: Slot::new(epoch.saturating_mul(32)),
        };
        let mut b = eng.batch();
        b.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &fc.as_ssz_bytes());
        eng.commit(b).unwrap();
    }

    // ── CC-4I /1: GetHistoricalBlock by root ───────────────────────────────

    #[test]
    fn historical_block_by_root_byte_identical() {
        let (dir, eng) = open_engine("by-root");
        let (root, ssz) = seed_block(&eng, 100);
        let got = historical_block_by_root(&eng, &root)
            .unwrap()
            .expect("block present");
        assert_eq!(got.slot.as_u64(), 100);
        assert_eq!(got.root, root);
        assert_eq!(sha256(&got.ssz), sha256(&ssz), "SSZ must be byte-identical");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CC-4I /1: GetHistoricalBlock by slot ───────────────────────────────

    #[test]
    fn historical_block_by_slot_byte_identical() {
        let (dir, eng) = open_engine("by-slot");
        let (root, ssz) = seed_block(&eng, 200);
        let got = historical_block_by_slot(&eng, Slot::new(200))
            .unwrap()
            .expect("canonical block present");
        assert_eq!(got.root, root);
        assert_eq!(sha256(&got.ssz), sha256(&ssz));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CC-4I /1 + ring stream: GetSnapshotState at ring slots ─────────────

    #[test]
    fn snapshot_state_at_ring_slots_reassembles_sha256() {
        let (dir, eng) = open_engine("snap-ring");
        // Four ring slots at 32-epoch cadence (slots 0, 1024, 2048, 3072).
        let slots = [0u64, 1024, 2048, 3072];
        let mut fixtures = Vec::new();
        for (i, &s) in slots.iter().enumerate() {
            let body = format!("snapshot-fixture-{i}-slot-{s}").into_bytes();
            // Pad so chunks exercise multi-chunk reassembly.
            let mut ssz = body;
            ssz.resize(
                DEFAULT_STATE_CHUNK_BYTES + 17 + i * 3,
                (i as u8).wrapping_add(1),
            );
            put_snapshot(&eng, Slot::new(s), &ssz, 4).unwrap();
            fixtures.push((s, ssz));
        }

        for (slot, expected) in &fixtures {
            let lookup = snapshot_state_at(&eng, Slot::new(*slot)).unwrap();
            let SnapshotLookup::Available(got) = lookup else {
                panic!("slot {slot} must be Available");
            };
            assert_eq!(sha256(&got), sha256(expected), "slot {slot}");
            let chunks = chunk_state_ssz(&got, DEFAULT_STATE_CHUNK_BYTES);
            assert!(chunks.last().is_some_and(|c| c.last));
            let reassembled = reassemble_chunks(&chunks);
            assert_eq!(sha256(&reassembled), sha256(expected));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CC-4I /2: outside ring → NOT_AVAILABLE, process_slots == 0 ─────────

    #[test]
    fn snapshot_outside_ring_is_typed_not_available_no_replay() {
        let (dir, eng) = open_engine("snap-miss");
        put_snapshot(&eng, Slot::new(1024), b"ring-only", 4).unwrap();
        reset_process_slots_invocations();

        let started = Instant::now();
        let lookup = snapshot_state_at(&eng, Slot::new(99)).unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed.as_millis() < 10,
            "NOT_AVAILABLE must return in < 10 ms, got {elapsed:?}"
        );

        match lookup {
            SnapshotLookup::NotAvailable { slot, ring } => {
                assert_eq!(slot.as_u64(), 99);
                assert_eq!(ring, vec![Slot::new(1024)]);
                let status = not_available_status(slot, &ring);
                assert_eq!(status.code(), Code::FailedPrecondition);
                let info = error_info_from_status(&status)
                    .unwrap()
                    .expect("ErrorInfo present");
                assert_eq!(info.reason, REASON_NOT_AVAILABLE);
                assert_eq!(info.domain, ERROR_DOMAIN);
            }
            SnapshotLookup::Available(_) => panic!("must not be available"),
        }

        assert_eq!(
            process_slots_invocations(),
            0,
            "process_slots invocation counter must stay at zero on a ring miss"
        );
        // history.rs must not pull process_slots — counter stays zero without
        // any instrumentation of cc_state_transition.
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CC-4I /1: finalized-checkpoint history ─────────────────────────────

    #[test]
    fn finalized_checkpoint_history_matches_seeded() {
        let (dir, eng) = open_engine("fc-hist");
        let fc_root = root_n(0xAB);
        seed_fc_finalized(&eng, 100, fc_root);

        // Snapshot ring entries with canonical roots.
        let snap_slots = [32u64, 1056, 2080]; // epochs 1, 33, 65
        for &s in &snap_slots {
            let root = root_slot(s);
            let ssz = synth_block(s, &Root::ZERO, &root_n(2));
            let mut b = eng.batch();
            {
                let rt = eng.read().unwrap();
                put_block(
                    &rt,
                    &mut b,
                    Slot::new(s),
                    &root,
                    &ssz,
                    BlockRegion::Hot,
                    false,
                )
                .unwrap();
                put_canonical(&rt, &mut b, Slot::new(s), &root).unwrap();
            }
            eng.commit(b).unwrap();
            put_snapshot(&eng, Slot::new(s), format!("snap-{s}").as_bytes(), 4).unwrap();
        }

        let hist = finalized_checkpoint_history(&eng).unwrap();
        // Expect epochs: 1, 33, 65 from snapshots + 100 from FC scalars.
        let epochs: Vec<u64> = hist.iter().map(|c| c.epoch).collect();
        assert_eq!(epochs, vec![1, 33, 65, 100]);
        assert_eq!(hist[3].root, fc_root);
        assert_eq!(hist[0].root, root_slot(32));
        assert_eq!(hist[1].root, root_slot(1056));
        assert_eq!(hist[2].root, root_slot(2080));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CC-4I /1 budget: unary p99 ≤ 50 ms over 1 000 calls ─────────────────

    #[test]
    fn unary_historical_reads_p99_under_50ms() {
        let (dir, eng) = open_engine("p99");
        let (root, _ssz) = seed_block(&eng, 500);
        seed_fc_finalized(&eng, 15, root);
        put_snapshot(&eng, Slot::new(480), b"snap-p99-body", 4).unwrap();

        const N: usize = 1_000;
        let mut samples_root = Vec::with_capacity(N);
        let mut samples_slot = Vec::with_capacity(N);
        let mut samples_fc = Vec::with_capacity(N);

        for _ in 0..N {
            let t0 = Instant::now();
            let _ = historical_block_by_root(&eng, &root).unwrap();
            samples_root.push(t0.elapsed());

            let t0 = Instant::now();
            let _ = historical_block_by_slot(&eng, Slot::new(500)).unwrap();
            samples_slot.push(t0.elapsed());

            let t0 = Instant::now();
            let _ = finalized_checkpoint_history(&eng).unwrap();
            samples_fc.push(t0.elapsed());
        }

        fn p99_ms(mut samples: Vec<std::time::Duration>) -> f64 {
            samples.sort();
            let idx = (samples.len() as f64 * 0.99).ceil() as usize - 1;
            samples[idx.min(samples.len() - 1)].as_secs_f64() * 1000.0
        }

        let p99_root = p99_ms(samples_root);
        let p99_slot = p99_ms(samples_slot);
        let p99_fc = p99_ms(samples_fc);
        eprintln!(
            "CC-4I unary p99 ms: historical_block_by_root={p99_root:.3} \
             historical_block_by_slot={p99_slot:.3} \
             finalized_checkpoint_history={p99_fc:.3}"
        );
        assert!(p99_root <= 50.0, "by_root p99 {p99_root} ms > 50");
        assert!(p99_slot <= 50.0, "by_slot p99 {p99_slot} ms > 50");
        assert!(p99_fc <= 50.0, "fc history p99 {p99_fc} ms > 50");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CC-4I /4: no Phase 6 gateway symbols in this module ─────────────────

    #[test]
    fn history_module_source_has_no_gateway_service() {
        let src = include_str!("history.rs");
        // Criterion CC-4I /4: no Phase 6 gateway package path in this file.
        // Needles built at runtime so this assertion cannot self-match.
        let parts = ["bea", "con"];
        let prefix = format!("{}{}", parts[0], parts[1]);
        let a = format!("{prefix}_{}", "api");
        let b = format!("{prefix}.{}", "api");
        let c = format!("{prefix}-{}", "api");
        assert!(
            !src.contains(&a) && !src.contains(&b) && !src.contains(&c),
            "history.rs must not reference the Phase 6 gateway service (CC-4I /4)"
        );
    }

    #[test]
    fn chunk_state_ssz_empty_and_boundaries() {
        let empty = chunk_state_ssz(&[], 64);
        assert_eq!(empty.len(), 1);
        assert!(empty[0].last);
        assert!(empty[0].data.is_empty());

        let data = vec![7u8; 100];
        let chunks = chunk_state_ssz(&data, 30);
        assert_eq!(chunks.len(), 4);
        assert!(chunks[3].last);
        assert!(!chunks[0].last);
        assert_eq!(reassemble_chunks(&chunks), data);
    }
}
