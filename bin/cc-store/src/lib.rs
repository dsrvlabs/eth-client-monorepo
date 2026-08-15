//! Offline store tool library (CC-4J).
//!
//! Three subcommands against a store a **stopped** node left behind:
//! - [`verify`] — run the eight §2.7 invariants read-only
//! - [`compact`] — on-demand engine compaction with before/after lengths
//! - [`dump`] — one opaque value as length + hex prefix (no consensus decode)
//!
//! There is intentionally **no** repair path.

#![allow(missing_docs)]

use std::path::Path;

use anyhow::{Context, Result, bail};
use cc_store::{
    DEFAULT_SNAPSHOT_RING, Engine, EngineOptions, InvariantCheckMode, InvariantContext, StoreError,
    StoreInvariant, check_invariants, db_file_path,
};

/// Bytes of value payload shown as hex prefix by [`dump`].
pub const DUMP_HEX_PREFIX_BYTES: usize = 64;

/// Open a store directory read-only for offline inspection.
///
/// Maps a live writer lock to a named error ([`StoreError::DatabaseLocked`]).
pub fn open_readonly(path: &Path) -> Result<Engine> {
    Engine::open_read_only(path).map_err(map_store_err)
}

/// Open an existing store read-write (compaction only).
pub fn open_existing_rw(path: &Path) -> Result<Engine> {
    Engine::open_existing(path, EngineOptions::default()).map_err(map_store_err)
}

fn map_store_err(err: StoreError) -> anyhow::Error {
    anyhow::Error::new(err)
}

/// Run all eight CC-4H invariants in open (fatal-first) mode.
///
/// Opens **read-only**. Refuses a store a live process holds
/// ([`StoreError::DatabaseLocked`]).
///
/// On success prints a one-line ok report. On the first violation returns `Err`
/// whose display names the invariant label.
pub fn verify(path: &Path) -> Result<()> {
    let engine = open_readonly(path)?;
    let ctx = InvariantContext {
        expected_node_id: None,
        snapshot_ring: DEFAULT_SNAPSHOT_RING,
        invocation_counter: None,
    };
    match check_invariants(&engine, InvariantCheckMode::Open, &ctx, None) {
        Ok(0) => {
            println!("verify: ok ({} invariants)", StoreInvariant::ALL.len());
            Ok(())
        }
        Ok(n) => {
            // Open mode returns Ok only when n == 0; defensive.
            bail!("verify: unexpected Ok({n}) from open-mode invariant check");
        }
        Err(StoreError::InvariantViolation { invariant, detail }) => {
            bail!("store invariant {invariant} violated: {detail}");
        }
        Err(StoreError::DatabaseLocked) => {
            bail!("{}", StoreError::DatabaseLocked);
        }
        Err(e) => Err(map_store_err(e)).context("verify failed"),
    }
}

/// Compact the on-disk store and report before/after file lengths and ratio.
pub fn compact(path: &Path) -> Result<()> {
    let engine = open_existing_rw(path)?;
    let before = engine.file_len().map_err(map_store_err)?;
    let did_work = engine.compact().map_err(map_store_err)?;
    let after = engine.file_len().map_err(map_store_err)?;
    let ratio = if before == 0 {
        0.0
    } else {
        (after as f64) / (before as f64)
    };
    println!("compact: path={}", db_file_path(path).display());
    println!("compact: before_bytes={before}");
    println!("compact: after_bytes={after}");
    println!("compact: ratio={ratio:.6}");
    println!("compact: did_work={did_work}");
    Ok(())
}

/// Dump one opaque value: length + hex prefix. No SSZ / consensus decode.
pub fn dump(path: &Path, table: &str, key_hex: &str) -> Result<()> {
    if table.is_empty() {
        bail!("table name must be non-empty");
    }
    let key = parse_hex_key(key_hex).context("key hex")?;
    let engine = open_readonly(path)?;
    let rt = engine.read().map_err(map_store_err)?;
    let value = rt
        .get(table, &key)
        .map_err(map_store_err)?
        .ok_or_else(|| anyhow::anyhow!("key not found in table {table}"))?;

    let prefix_len = value.len().min(DUMP_HEX_PREFIX_BYTES);
    let prefix = &value[..prefix_len];
    println!("table={table}");
    println!("key=0x{}", hex::encode(&key));
    println!("length={}", value.len());
    println!("hex_prefix=0x{}", hex::encode(prefix));
    if value.len() > DUMP_HEX_PREFIX_BYTES {
        println!("hex_prefix_truncated=true");
    } else {
        println!("hex_prefix_truncated=false");
    }
    Ok(())
}

/// Parse a key hex string (`0x` optional).
pub fn parse_hex_key(s: &str) -> Result<Vec<u8>> {
    let hex = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    if hex.is_empty() {
        bail!("key hex must be non-empty");
    }
    hex::decode(hex).context("invalid hex")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_store::keys::{encode_cold_block_key, encode_hot_block_key, encode_hot_column_key};
    use cc_store::meta::{
        AnchorInfo, ForkChoiceScalars, KEY_ANCHOR_INFO, KEY_FC_SCALARS, KEY_PRUNE_MARKS,
        KEY_SERVE_WINDOW, KEY_SPLIT, KEY_WRITE_CURSOR, PruneMarks, ServeWindow, Split, TABLE_META,
        WriteCursor,
    };
    use cc_store::{Durability, EngineOptions, Root, Slot, SszEncode, TABLE_BLOCKS_HOT};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-tool-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn root(b: u8) -> Root {
        Root::from_array([b; 32])
    }

    fn open_fresh(dir: &Path) -> Engine {
        Engine::open(
            dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap()
    }

    /// Healthy baseline (passes all eight invariants).
    ///
    /// Uses `Split.slot = 0` so `I-split-fin` holds with default finalized epoch 0
    /// (avoids naming `cc_types::Checkpoint`, which is outside the tool's DAG).
    fn write_healthy(engine: &Engine, node_id: Root) {
        let anchor = AnchorInfo {
            anchor_slot: Slot::new(10),
            anchor_root: root(0x10),
            anchor_state_root: root(0x11),
            node_id,
            oldest_block_slot: Slot::new(10),
            oldest_block_parent: root(0x09),
        };
        let split = Split {
            slot: Slot::ZERO,
            state_root: root(0x81),
            block_root: root(0x82),
        };
        let window = ServeWindow {
            earliest_available_slot: Slot::new(10),
            cgc: 4,
            block_floor: Slot::new(10),
            column_floor: Slot::new(10),
            ..Default::default()
        };

        let cursor = WriteCursor {
            session_id: 1,
            seq: 1,
            slot: Slot::new(12),
            root: root(0x12),
        };
        let fc = ForkChoiceScalars {
            time: 0,
            proposer_boost_root: Root::ZERO,
            justified: Default::default(),
            finalized: Default::default(),
            unrealized_justified: Default::default(),
            unrealized_finalized: Default::default(),
            head_root: root(0x12),
            head_slot: Slot::new(12),
        };
        let marks = PruneMarks {
            columns_up_to: Slot::ZERO,
            blocks_up_to: Slot::ZERO,
            states_up_to: Slot::ZERO,
            state_roots_up_to: Slot::ZERO,
        };

        let mut b = engine.batch();
        b.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        b.put(TABLE_META, KEY_SPLIT.as_bytes(), &split.as_ssz_bytes());
        b.put(
            TABLE_META,
            KEY_SERVE_WINDOW.as_bytes(),
            &window.as_ssz_bytes(),
        );
        b.put(
            TABLE_META,
            KEY_WRITE_CURSOR.as_bytes(),
            &cursor.as_ssz_bytes(),
        );
        b.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &fc.as_ssz_bytes());
        b.put(
            TABLE_META,
            KEY_PRUNE_MARKS.as_bytes(),
            &marks.as_ssz_bytes(),
        );
        for (slot, r) in [(10u64, root(0x10)), (11, root(0x11)), (12, root(0x12))] {
            let s = Slot::new(slot);
            b.put("canonical", &encode_cold_block_key(s), r.as_slice());
            b.put(TABLE_BLOCKS_HOT, &encode_hot_block_key(s, &r), b"block-ssz");
        }
        // Snapshot at split (slot 0).
        b.put(
            "snapshots",
            &encode_cold_block_key(Slot::ZERO),
            b"state-ssz",
        );
        engine.commit(b).unwrap();
    }

    #[test]
    fn verify_clean_store_ok() {
        let dir = tmp_dir("verify-ok");
        {
            let eng = open_fresh(&dir);
            write_healthy(&eng, root(0xAA));
        }
        verify(&dir).expect("clean store must pass");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn verify_col_block_names_invariant() {
        let dir = tmp_dir("verify-col");
        {
            let eng = open_fresh(&dir);
            write_healthy(&eng, root(0xAA));
            let orphan = root(0xEE);
            let key = encode_hot_column_key(Slot::new(10), &orphan, 0);
            let mut b = eng.batch();
            b.put("columns_hot", &key, b"col-ssz");
            eng.commit(b).unwrap();
        }
        let err = verify(&dir).expect_err("orphan column must fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("col_block"), "must name I-col-block: {msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn verify_refuses_live_lock() {
        let dir = tmp_dir("verify-lock");
        let writer = open_fresh(&dir);
        write_healthy(&writer, root(0xAA));
        // Keep writer open so redb exclusive lock is held.
        let err = verify(&dir).expect_err("live lock must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("locked") || msg.contains("DatabaseLocked"),
            "must name lock error: {msg}"
        );
        drop(writer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dump_length_and_hex_prefix() {
        let dir = tmp_dir("dump-ok");
        let key_bytes;
        {
            let eng = open_fresh(&dir);
            write_healthy(&eng, root(0xAA));
            key_bytes = encode_hot_block_key(Slot::new(10), &root(0x10));
        }
        dump(&dir, TABLE_BLOCKS_HOT, &hex::encode(key_bytes)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dump_missing_key_named_error() {
        let dir = tmp_dir("dump-miss");
        {
            let eng = open_fresh(&dir);
            write_healthy(&eng, root(0xAA));
        }
        let missing = encode_hot_block_key(Slot::new(99), &root(0xFF));
        let err = dump(&dir, TABLE_BLOCKS_HOT, &hex::encode(missing)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("key not found") && msg.contains(TABLE_BLOCKS_HOT),
            "{msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_reports_lengths() {
        let dir = tmp_dir("compact");
        {
            let eng = open_fresh(&dir);
            write_healthy(&eng, root(0xAA));
            let mut b = eng.batch();
            for i in 0..64u8 {
                b.put("meta", &[0xF0, i], &vec![i; 1024]);
            }
            eng.commit(b).unwrap();
        }
        compact(&dir).expect("compact must succeed offline");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_hex_key_accepts_0x_prefix() {
        let a = parse_hex_key("0xdead").unwrap();
        let b = parse_hex_key("dead").unwrap();
        assert_eq!(a, b);
        assert_eq!(a, vec![0xde, 0xad]);
    }
}
