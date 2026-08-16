//! Column store: hot + 32-epoch cold shards, by-root index, DA status (CC-43b).
//!
//! ## Tables (§2.2 / §2.3 / ADR P4-09 / D-P4-5)
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | [`TABLE_COLUMNS_HOT`] | `(slot, root, idx:u16be)` 42 B | sidecar SSZ (opaque) |
//! | `columns_{shard}` | `(slot, idx:u16be)` 10 B | sidecar SSZ — **cold key is the
//!   spec's mandated `(slot, column_index)` response order** |
//! | [`TABLE_COLUMN_SLOT_BY_ROOT`] | `(root, idx:u16be)` 34 B | `slot` 8 B |
//! | [`TABLE_DA_STATUS`] | `root` 32 B | `status:u8 ‖ slot:u64be` 9 B |
//!
//! Values are **SSZ wire bytes and nothing else**. This crate never re-serializes
//! and never fully decodes a sidecar — fixed-offset field peeks only
//! ([`COLUMN_INDEX_SSZ_OFFSET`], [`COLUMN_HEADER_SLOT_SSZ_OFFSET`],
//! [`COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET`]).
//!
//! ## 32-epoch shards (Deviation 1 / ADR P4-10)
//!
//! Column shard width equals the column prune cadence (**32 epochs**). A prune
//! tick is one `drop_table` of ~8 200 keys rather than 8 200 B-tree deletions.
//!
//! ## Size formula (`column sidecar (n blobs)`)
//!
//! ```text
//! serialized_len = 356 + n × 2144
//! ```
//!
//! Fixed part **356 B** = 8 (`index`) + 12 (three list offsets) + 208
//! (`signed block header`) + 128 (4×32 inclusion-proof roots). Per blob
//! **2144 B** = 2048 (`Cell`) + 48 (commitment) + 48 (proof). At 21 blobs:
//! **45 380 B**. Asserted against a real serialized sidecar in
//! `tests/column_sidecar_offsets.rs`.
//!
//! ## Fixed SSZ offsets (*Values Deliberately Not Invented* 5)
//!
//! Wire layout of `column sidecar` fixed part:
//!   `index`                @ 0   (u64 LE)
//!   offset `column`        @ 8
//!   offset `kzg_commitments` @ 12
//!   offset `kzg_proofs`    @ 16
//!   `signed_block_header.message.slot` @ **20** (u64 LE)
//!   `signed_block_header.message.parent_root` @ **36** (32 B)
//!
//! Asserted against a real serialized sidecar in `tests/column_sidecar_offsets.rs`.
//!
//! ## Per-block atomic emit (CC-43 /3 store half)
//!
//! [`columns_for_block`] materialises the **whole** `requested ∩ held` set for
//! one block and names the missing indices. The wire anti-truncation rule
//! (drop the last whole block under the response cap) is `CC-4F`'s and composes
//! with this helper — it is not enforced here.

use cc_types::{Root, Slot};

use crate::engine::{Batch, Engine, ReadTxn, StoreError};
use crate::keys::{
    BlockRegion, COLUMN_SHARD_EPOCHS, SLOTS_PER_EPOCH, cold_column_slot_range, column_shard_id,
    columns_shard_table, decode_cold_column_key, decode_column_slot_by_root_value,
    decode_hot_column_key, encode_cold_column_key, encode_column_slot_by_root_key,
    encode_column_slot_by_root_value, encode_hot_column_key, encode_root_key, hot_column_root_end,
};

// ---------------------------------------------------------------------------
// Table names
// ---------------------------------------------------------------------------

/// Hot column table (`slot ‖ root ‖ idx` → sidecar SSZ).
pub const TABLE_COLUMNS_HOT: &str = "columns_hot";
/// `root ‖ idx` → `slot` reverse index for `DataColumnsByRoot`.
pub const TABLE_COLUMN_SLOT_BY_ROOT: &str = "column_slot_by_root";
/// `root` → `status ‖ slot` (Available \| Deferred); read, never re-derived.
pub const TABLE_DA_STATUS: &str = "da_status";

// ---------------------------------------------------------------------------
// Fixed SSZ offsets + size formula
// ---------------------------------------------------------------------------

/// Absolute byte offset of `column sidecar.index` (SSZ little-endian u64).
pub const COLUMN_INDEX_SSZ_OFFSET: usize = 0;

/// Absolute byte offset of the header `slot` inside a serialized sidecar.
///
/// Layout: 8 B index + 3 × 4 B list offsets + start of `signed block header`.
pub const COLUMN_HEADER_SLOT_SSZ_OFFSET: usize = 20;

/// Absolute byte offset of header `parent_root` inside a serialized sidecar.
///
/// `signed_block_header` starts at 20; `parent_root` is 16 B into the header
/// (`slot` 8 + `proposer_index` 8).
pub const COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET: usize = 36;

/// Fixed-part byte count of `column sidecar` (independent of blob count).
pub const DATA_COLUMN_SIDECAR_FIXED_BYTES: usize = 356;

/// Bytes contributed per blob (`Cell` 2048 + commitment 48 + proof 48).
pub const BYTES_PER_BLOB_IN_SIDECAR: usize = 2144;

/// Minimum SSZ length that contains `index` and header `slot`.
pub const MIN_COLUMN_SSZ_LEN: usize = COLUMN_HEADER_SLOT_SSZ_OFFSET + 8;

/// Hard cap on [`columns_by_range`] slot `count` (aligns with `MAX_REQUEST_BLOCKS_DENEB`).
pub const MAX_COLUMNS_BY_RANGE_SLOTS: u64 = 128;

/// Hard cap on sidecars materialised by one range call
/// (`MAX_REQUEST_BLOCKS_DENEB × NUMBER_OF_COLUMNS` = 16 384).
pub const MAX_COLUMNS_BY_RANGE_SIDECARS: usize = 16_384;

/// Spec `NUMBER_OF_COLUMNS` (column index domain).
pub const NUMBER_OF_COLUMNS: u16 = 128;

/// Max cells (blobs) per sidecar value accepted on put.
///
/// Matches the SSZ list capacity `MAX_BLOB_COMMITMENTS_PER_BLOCK` (4096). A
/// larger value cannot be a well-formed mainnet/minimal sidecar; store refuses
/// it fail-closed rather than materialising multi-MiB hostile rows.
pub const MAX_BLOBS_PER_COLUMN_SIDECAR: usize = 4096;

/// Hard cap on sidecar SSZ value bytes: `356 + 4096 × 2144` = 8 782 052.
pub const MAX_COLUMN_SIDECAR_BYTES: usize =
    DATA_COLUMN_SIDECAR_FIXED_BYTES + MAX_BLOBS_PER_COLUMN_SIDECAR * BYTES_PER_BLOB_IN_SIDECAR;

/// Serialized length of a `column sidecar` with `n` blobs.
///
/// Formula: `356 + n × 2144`. At 21 blobs → 45 380 B.
#[must_use]
pub const fn data_column_sidecar_size(n_blobs: usize) -> usize {
    DATA_COLUMN_SIDECAR_FIXED_BYTES + n_blobs.saturating_mul(BYTES_PER_BLOB_IN_SIDECAR)
}

/// Peek `index` at [`COLUMN_INDEX_SSZ_OFFSET`] (SSZ little-endian u64 → `u16`).
pub fn column_index_at_offset(sidecar_ssz: &[u8]) -> Result<u16, StoreError> {
    if sidecar_ssz.len() < COLUMN_INDEX_SSZ_OFFSET + 8 {
        return Err(StoreError::Codec(format!(
            "column SSZ too short for index at {COLUMN_INDEX_SSZ_OFFSET}: len {}",
            sidecar_ssz.len()
        )));
    }
    let mut le = [0u8; 8];
    le.copy_from_slice(&sidecar_ssz[COLUMN_INDEX_SSZ_OFFSET..COLUMN_INDEX_SSZ_OFFSET + 8]);
    let v = u64::from_le_bytes(le);
    if v > u64::from(u16::MAX) {
        return Err(StoreError::Codec(format!(
            "column index {v} exceeds u16 domain"
        )));
    }
    let idx = v as u16;
    if idx >= NUMBER_OF_COLUMNS {
        return Err(StoreError::Codec(format!(
            "column index {idx} ≥ NUMBER_OF_COLUMNS ({NUMBER_OF_COLUMNS})"
        )));
    }
    Ok(idx)
}

/// Peek header `slot` at [`COLUMN_HEADER_SLOT_SSZ_OFFSET`] (SSZ little-endian u64).
pub fn column_slot_at_offset(sidecar_ssz: &[u8]) -> Result<Slot, StoreError> {
    if sidecar_ssz.len() < COLUMN_HEADER_SLOT_SSZ_OFFSET + 8 {
        return Err(StoreError::Codec(format!(
            "column SSZ too short for header slot at {COLUMN_HEADER_SLOT_SSZ_OFFSET}: len {}",
            sidecar_ssz.len()
        )));
    }
    let mut le = [0u8; 8];
    le.copy_from_slice(
        &sidecar_ssz[COLUMN_HEADER_SLOT_SSZ_OFFSET..COLUMN_HEADER_SLOT_SSZ_OFFSET + 8],
    );
    Ok(Slot::new(u64::from_le_bytes(le)))
}

/// Peek header `parent_root` at [`COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET`].
pub fn column_parent_root_at_offset(sidecar_ssz: &[u8]) -> Result<Root, StoreError> {
    if sidecar_ssz.len() < COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET + 32 {
        return Err(StoreError::Codec(format!(
            "column SSZ too short for header parent_root at {COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET}: len {}",
            sidecar_ssz.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(
        &sidecar_ssz
            [COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET..COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET + 32],
    );
    Ok(Root::from_array(arr))
}

// ---------------------------------------------------------------------------
// DA status
// ---------------------------------------------------------------------------

/// On-disk DA verdict for a block root (`da_status` table).
///
/// Written with the block; **read, never re-derived**, on replay (`CC-45` /4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DaStatus {
    /// All custodied columns verified / DA gate passed.
    Available = 0,
    /// Block accepted pending DA (fork-choice deferred path).
    Deferred = 1,
}

impl DaStatus {
    /// Parse the status byte; unknown values yield `None`.
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Available),
            1 => Some(Self::Deferred),
            _ => None,
        }
    }

    /// Wire / stored byte.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Encode `da_status` value: `status:u8 ‖ slot:u64be` (9 B).
pub fn encode_da_status_value(status: DaStatus, slot: Slot) -> [u8; 9] {
    let mut out = [0u8; 9];
    out[0] = status.as_u8();
    out[1..].copy_from_slice(&slot.as_u64().to_be_bytes());
    out
}

/// Decode `da_status` value.
pub fn decode_da_status_value(value: &[u8]) -> Option<(DaStatus, Slot)> {
    if value.len() != 9 {
        return None;
    }
    let status = DaStatus::from_u8(value[0])?;
    let mut slot_be = [0u8; 8];
    slot_be.copy_from_slice(&value[1..9]);
    Some((status, Slot::new(u64::from_be_bytes(slot_be))))
}

// ---------------------------------------------------------------------------
// Outcomes / row types
// ---------------------------------------------------------------------------

/// Result of an idempotent column put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutColumnOutcome {
    /// Key was absent; row inserted.
    Inserted,
    /// Same key, same bytes; no-op (one row, no error).
    Idempotent,
}

/// One sidecar returned by range / block helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeColumn {
    /// Slot of the producing block.
    pub slot: Slot,
    /// Block root (from the key in hot; from reverse index / canonical in cold).
    pub root: Root,
    /// Column index.
    pub index: u16,
    /// Opaque sidecar SSZ (byte-identical to what was stored).
    pub ssz: Vec<u8>,
}

/// Per-block atomic-emit result for the anti-truncation rule's store half.
///
/// Materialises the **whole** `requested ∩ held` set as one unit and names the
/// missing requested indices. The caller (serve path) decides whether to emit
/// the block; this helper never silently drops held sidecars or invents missing
/// ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnsForBlock {
    /// Requested indices present in the store, ascending by index.
    pub held: Vec<(u16, Vec<u8>)>,
    /// Requested indices not present (caller decides).
    pub missing: Vec<u16>,
}

impl ColumnsForBlock {
    /// True when no requested index is held (includes the zero-blob case).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

/// In-memory accounting for `cc_storage_*{class="columns"}` (CC-43b).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColumnClassStats {
    /// Rows under the columns class (hot + every cold shard).
    pub columns_rows: u64,
    /// Value+key bytes under the columns class.
    pub columns_bytes: u64,
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// Stage a column sidecar into `batch` with key-level idempotency.
///
/// - **Absent key** → insert body + `column_slot_by_root`.
/// - **Same bytes** → [`PutColumnOutcome::Idempotent`], no error.
/// - **Different bytes** → [`StoreError::KeyCollision`].
/// - **Caller slot / index ≠ SSZ peeks** → [`StoreError::Codec`].
/// - **Index ≥ [`NUMBER_OF_COLUMNS`]** → [`StoreError::Codec`].
/// - **Value longer than [`MAX_COLUMN_SIDECAR_BYTES`]** → [`StoreError::Limit`]
///   (fail-closed; hostile / non-container-shaped rows never land).
pub fn put_column(
    rt: &ReadTxn,
    batch: &mut Batch,
    slot: Slot,
    root: &Root,
    index: u16,
    ssz: &[u8],
    region: BlockRegion,
) -> Result<PutColumnOutcome, StoreError> {
    if index >= NUMBER_OF_COLUMNS {
        return Err(StoreError::Codec(format!(
            "put_column index {index} ≥ NUMBER_OF_COLUMNS ({NUMBER_OF_COLUMNS})"
        )));
    }
    if ssz.len() > MAX_COLUMN_SIDECAR_BYTES {
        return Err(StoreError::limit(format!(
            "put_column value len {} exceeds MAX_COLUMN_SIDECAR_BYTES ({MAX_COLUMN_SIDECAR_BYTES}; \
             356 + {MAX_BLOBS_PER_COLUMN_SIDECAR}×2144)",
            ssz.len()
        )));
    }
    if ssz.len() < MIN_COLUMN_SSZ_LEN {
        return Err(StoreError::Codec(format!(
            "put_column value len {} below MIN_COLUMN_SSZ_LEN ({MIN_COLUMN_SSZ_LEN})",
            ssz.len()
        )));
    }

    let ssz_index = column_index_at_offset(ssz)?;
    if ssz_index != index {
        return Err(StoreError::Codec(format!(
            "put_column index mismatch: caller {index} != SSZ index {ssz_index} at offset {COLUMN_INDEX_SSZ_OFFSET}"
        )));
    }
    let ssz_slot = column_slot_at_offset(ssz)?;
    if ssz_slot != slot {
        return Err(StoreError::Codec(format!(
            "put_column slot mismatch: caller {} != SSZ header slot {} at offset {COLUMN_HEADER_SLOT_SSZ_OFFSET}",
            slot.as_u64(),
            ssz_slot.as_u64()
        )));
    }

    let (table, key) = column_table_and_key(slot, root, index, region);
    match rt.get(&table, &key)? {
        Some(existing) if existing.as_slice() == ssz => {
            return Ok(PutColumnOutcome::Idempotent);
        }
        Some(_) => {
            return Err(StoreError::KeyCollision { table });
        }
        None => {}
    }

    // Reverse index: same (root, idx) → different slot is fatal.
    let idx_key = encode_column_slot_by_root_key(root, index);
    let idx_val = encode_column_slot_by_root_value(slot);
    if let Some(existing) = rt.get(TABLE_COLUMN_SLOT_BY_ROOT, &idx_key)?
        && existing.as_slice() != idx_val.as_slice()
    {
        return Err(StoreError::KeyCollision {
            table: TABLE_COLUMN_SLOT_BY_ROOT.to_owned(),
        });
    }

    batch.put(&table, &key, ssz);
    batch.put(TABLE_COLUMN_SLOT_BY_ROOT, &idx_key, &idx_val);
    Ok(PutColumnOutcome::Inserted)
}

/// Stage `da_status[root] = (status, slot)`.
///
/// - **Absent / same status** → write (slot may be refreshed).
/// - **`Deferred → Available`** → allowed (deliberate DA promotion write; never
///   an implicit side effect of a read).
/// - **`Available → Deferred`** → **refused** ([`StoreError::Codec`]): demotion
///   is fail-closed. Once a block is recorded Available it stays Available; a
///   wrong Deferred write after the fact would silently re-open the DA gate on
///   replay (`CC-45` /4).
pub fn put_da_status(
    rt: &ReadTxn,
    batch: &mut Batch,
    root: &Root,
    status: DaStatus,
    slot: Slot,
) -> Result<(), StoreError> {
    if let Some((existing, _)) = get_da_status(rt, root)?
        && existing == DaStatus::Available
        && status == DaStatus::Deferred
    {
        return Err(StoreError::Codec(format!(
            "da_status demotion refused: root already Available, cannot write Deferred (slot {})",
            slot.as_u64()
        )));
    }
    batch.put(
        TABLE_DA_STATUS,
        &encode_root_key(root),
        &encode_da_status_value(status, slot),
    );
    Ok(())
}

fn column_table_and_key(
    slot: Slot,
    root: &Root,
    index: u16,
    region: BlockRegion,
) -> (String, Vec<u8>) {
    match region {
        BlockRegion::Hot => (
            TABLE_COLUMNS_HOT.to_owned(),
            encode_hot_column_key(slot, root, index).to_vec(),
        ),
        BlockRegion::Cold => {
            let shard = column_shard_id(slot);
            (
                columns_shard_table(shard),
                encode_cold_column_key(slot, index).to_vec(),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Look up slot for `(block_root, column_index)`.
pub fn slot_by_column_root(
    rt: &ReadTxn,
    root: &Root,
    index: u16,
) -> Result<Option<Slot>, StoreError> {
    let Some(v) = rt.get(
        TABLE_COLUMN_SLOT_BY_ROOT,
        &encode_column_slot_by_root_key(root, index),
    )?
    else {
        return Ok(None);
    };
    let slot = decode_column_slot_by_root_value(&v).ok_or_else(|| {
        StoreError::Codec(format!(
            "column_slot_by_root value len {} (want 8)",
            v.len()
        ))
    })?;
    Ok(Some(slot))
}

/// Load sidecar SSZ by `(root, index)` via the reverse index.
pub fn get_column_by_root(
    rt: &ReadTxn,
    root: &Root,
    index: u16,
    region_hint: Option<BlockRegion>,
) -> Result<Option<Vec<u8>>, StoreError> {
    let Some(slot) = slot_by_column_root(rt, root, index)? else {
        return Ok(None);
    };
    // Prefer the hinted region; fall back to the other.
    match region_hint {
        Some(region) => {
            if let Some(ssz) = get_column(rt, slot, root, index, region)? {
                return Ok(Some(ssz));
            }
            let other = match region {
                BlockRegion::Hot => BlockRegion::Cold,
                BlockRegion::Cold => BlockRegion::Hot,
            };
            get_column(rt, slot, root, index, other)
        }
        None => {
            if let Some(ssz) = get_column(rt, slot, root, index, BlockRegion::Hot)? {
                return Ok(Some(ssz));
            }
            get_column(rt, slot, root, index, BlockRegion::Cold)
        }
    }
}

/// Load sidecar SSZ given slot, root, index, and region.
pub fn get_column(
    rt: &ReadTxn,
    slot: Slot,
    root: &Root,
    index: u16,
    region: BlockRegion,
) -> Result<Option<Vec<u8>>, StoreError> {
    let (table, key) = column_table_and_key(slot, root, index, region);
    rt.get(&table, &key)
}

/// Load a cold-region column by `(slot, index)` alone (root not in the key).
pub fn get_cold_column(
    rt: &ReadTxn,
    slot: Slot,
    index: u16,
) -> Result<Option<Vec<u8>>, StoreError> {
    let table = columns_shard_table(column_shard_id(slot));
    rt.get(&table, &encode_cold_column_key(slot, index))
}

/// Read `da_status[root]`. **Never promotes** `Deferred` → `Available`.
pub fn get_da_status(rt: &ReadTxn, root: &Root) -> Result<Option<(DaStatus, Slot)>, StoreError> {
    let Some(v) = rt.get(TABLE_DA_STATUS, &encode_root_key(root))? else {
        return Ok(None);
    };
    decode_da_status_value(&v)
        .ok_or_else(|| StoreError::Codec(format!("da_status value len {} (want 9)", v.len())))
        .map(Some)
}

/// Per-block atomic emit: whole `requested ∩ held` or empty, with missing named.
///
/// - **Hot** (`BlockRegion::Hot`): indices are looked up under `(slot, root, idx)`.
/// - **Cold** (`BlockRegion::Cold`): **root-blind** by design (ADR P4-09) —
///   cold keys are `(slot, idx)` only because below the split there is exactly
///   one block per slot and the root has already dropped out of the key. The
///   `root` argument is accepted for API symmetry with the hot path and for the
///   serve layer's bookkeeping, but **is not consulted** on cold reads. The
///   serve layer (`CC-4F`) is responsible for selecting the canonical slot /
///   root before calling; this helper does not re-join root → cold row.
/// - Output `held` is ascending by index (iteration order, **no sort**).
/// - A zero-blob / no-column block yields `held = []` (not an error).
/// - `requested` may be empty → empty result.
///
/// Duplicate entries in `requested` are ignored after the first occurrence.
pub fn columns_for_block(
    rt: &ReadTxn,
    slot: Slot,
    root: &Root,
    requested: &[u16],
    region: BlockRegion,
) -> Result<ColumnsForBlock, StoreError> {
    let mut held = Vec::new();
    let mut missing = Vec::new();
    let mut seen = [false; NUMBER_OF_COLUMNS as usize];

    // Walk requested in caller order but emit held ascending by collecting then
    // — AC: "no sort on the response path". We therefore require callers who
    // care about order to pass ascending indices, and we iterate `0..128`
    // filtered by the requested set so emission order is index-ascending
    // without calling sort.
    let mut want = [false; NUMBER_OF_COLUMNS as usize];
    for &idx in requested {
        if idx >= NUMBER_OF_COLUMNS {
            return Err(StoreError::Codec(format!(
                "columns_for_block index {idx} ≥ NUMBER_OF_COLUMNS ({NUMBER_OF_COLUMNS})"
            )));
        }
        want[idx as usize] = true;
    }

    for idx in 0..NUMBER_OF_COLUMNS {
        if !want[idx as usize] || seen[idx as usize] {
            continue;
        }
        seen[idx as usize] = true;
        let row = match region {
            BlockRegion::Hot => get_column(rt, slot, root, idx, BlockRegion::Hot)?,
            // ADR P4-09: cold is slot‖idx only — `root` intentionally unused
            // (serve layer owns canonical root selection; see fn docs).
            BlockRegion::Cold => get_cold_column(rt, slot, idx)?,
        };
        match row {
            Some(ssz) => held.push((idx, ssz)),
            None => missing.push(idx),
        }
    }

    Ok(ColumnsForBlock { held, missing })
}

/// Column sidecars-by-range shaped read: `count` slots from `start_slot`.
///
/// Emits sidecars in ascending `(slot, column_index)` order **straight off the
/// primary index** — no sort on this path.
///
/// - Slots `≤ split` (when `split` is `Some`) are read from cold shards
///   via a forward key-range scan (`(slot, idx)` key order **is** the response
///   order).
/// - Slots above the split use `canonical[slot]` then `columns_hot[(slot, root, idx)]`,
///   iterating indices ascending.
/// - When `columns` is `Some`, only those indices are emitted; when `None`, every
///   held index at each slot is emitted (cold: full slot-prefix scan).
///
/// Caps (fail closed with [`StoreError::Limit`]):
/// - `count` ≤ [`MAX_COLUMNS_BY_RANGE_SLOTS`] (128)
/// - materialised rows ≤ [`MAX_COLUMNS_BY_RANGE_SIDECARS`] (16 384)
pub fn columns_by_range(
    rt: &ReadTxn,
    start_slot: Slot,
    count: u64,
    split: Option<Slot>,
    columns: Option<&[u16]>,
) -> Result<Vec<RangeColumn>, StoreError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if count > MAX_COLUMNS_BY_RANGE_SLOTS {
        return Err(StoreError::limit(format!(
            "columns_by_range count {count} exceeds MAX_COLUMNS_BY_RANGE_SLOTS ({MAX_COLUMNS_BY_RANGE_SLOTS})"
        )));
    }

    // Bitmap of requested indices (all when `columns` is None).
    let mut want_all = columns.is_none();
    let mut want = [false; NUMBER_OF_COLUMNS as usize];
    if let Some(list) = columns {
        if list.is_empty() {
            return Ok(Vec::new());
        }
        for &idx in list {
            if idx >= NUMBER_OF_COLUMNS {
                return Err(StoreError::Codec(format!(
                    "columns_by_range index {idx} ≥ NUMBER_OF_COLUMNS ({NUMBER_OF_COLUMNS})"
                )));
            }
            want[idx as usize] = true;
        }
    } else {
        want_all = true;
    }

    let mut out = Vec::new();
    let start = start_slot.as_u64();
    let end = start.saturating_add(count); // exclusive

    for s in start..end {
        let slot = Slot::new(s);
        let in_cold = match split {
            Some(sp) => s <= sp.as_u64(),
            None => false,
        };

        if in_cold {
            append_cold_slot(rt, slot, want_all, &want, &mut out)?;
        } else {
            append_hot_slot(rt, slot, want_all, &want, &mut out)?;
        }

        if out.len() > MAX_COLUMNS_BY_RANGE_SIDECARS {
            return Err(StoreError::limit(format!(
                "columns_by_range materialised {} sidecars exceeds MAX_COLUMNS_BY_RANGE_SIDECARS ({MAX_COLUMNS_BY_RANGE_SIDECARS})",
                out.len()
            )));
        }
    }

    Ok(out)
}

/// Cold path: forward range scan over `(slot, idx)` keys — **no sort**.
fn append_cold_slot(
    rt: &ReadTxn,
    slot: Slot,
    want_all: bool,
    want: &[bool; NUMBER_OF_COLUMNS as usize],
    out: &mut Vec<RangeColumn>,
) -> Result<(), StoreError> {
    let table = columns_shard_table(column_shard_id(slot));
    let (lo, hi) = cold_column_slot_range(slot, Slot::new(slot.as_u64().saturating_add(1)));
    // Prefer canonical root when present for the returned row; cold key has no root.
    let root = crate::canonical::get_canonical(rt, slot)?.unwrap_or(Root::ZERO);

    for item in rt.range(&table, &lo, &hi)? {
        let (k, v) = item?;
        let Some((k_slot, idx)) = decode_cold_column_key(&k) else {
            continue;
        };
        if k_slot != slot {
            continue;
        }
        if !want_all && !want[idx as usize] {
            continue;
        }
        out.push(RangeColumn {
            slot,
            root,
            index: idx,
            ssz: v,
        });
        if out.len() > MAX_COLUMNS_BY_RANGE_SIDECARS {
            return Err(StoreError::limit(format!(
                "columns_by_range materialised {} sidecars exceeds MAX_COLUMNS_BY_RANGE_SIDECARS ({MAX_COLUMNS_BY_RANGE_SIDECARS})",
                out.len()
            )));
        }
    }
    Ok(())
}

/// Hot path: canonical root + ascending index walk — **no sort**.
fn append_hot_slot(
    rt: &ReadTxn,
    slot: Slot,
    want_all: bool,
    want: &[bool; NUMBER_OF_COLUMNS as usize],
    out: &mut Vec<RangeColumn>,
) -> Result<(), StoreError> {
    let Some(root) = crate::canonical::get_canonical(rt, slot)? else {
        return Ok(());
    };

    if want_all {
        // Prefix scan on hot keys for this (slot, root):
        // lo = (slot, root, 0), hi = (slot, root+ε, 0) — use next-root upper bound
        // via index past last column under the same root.
        let lo = encode_hot_column_key(slot, &root, 0);
        // End key: same slot/root, index just past last legal (or next root prefix).
        // Hot key is slot‖root‖idx; exclusive end for all indices of this root is
        // slot‖root‖0xFFFF+1 which does not fit u16 — use next root with idx 0
        // when root is not all-0xff, else next slot.
        let hi = hot_column_root_end(slot, &root);
        for item in rt.range(TABLE_COLUMNS_HOT, &lo, &hi)? {
            let (k, v) = item?;
            let Some((k_slot, k_root, idx)) = decode_hot_column_key(&k) else {
                continue;
            };
            if k_slot != slot || k_root != root {
                continue;
            }
            out.push(RangeColumn {
                slot,
                root,
                index: idx,
                ssz: v,
            });
            if out.len() > MAX_COLUMNS_BY_RANGE_SIDECARS {
                return Err(StoreError::limit(format!(
                    "columns_by_range materialised {} sidecars exceeds MAX_COLUMNS_BY_RANGE_SIDECARS ({MAX_COLUMNS_BY_RANGE_SIDECARS})",
                    out.len()
                )));
            }
        }
    } else {
        for idx in 0..NUMBER_OF_COLUMNS {
            if !want[idx as usize] {
                continue;
            }
            if let Some(ssz) = get_column(rt, slot, &root, idx, BlockRegion::Hot)? {
                out.push(RangeColumn {
                    slot,
                    root,
                    index: idx,
                    ssz,
                });
                if out.len() > MAX_COLUMNS_BY_RANGE_SIDECARS {
                    return Err(StoreError::limit(format!(
                        "columns_by_range materialised {} sidecars exceeds MAX_COLUMNS_BY_RANGE_SIDECARS ({MAX_COLUMNS_BY_RANGE_SIDECARS})",
                        out.len()
                    )));
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Scan column tables and return row/byte totals for metrics class `columns`.
pub fn measure_column_class_stats(engine: &Engine) -> Result<ColumnClassStats, StoreError> {
    let rt = engine.read()?;
    let mut stats = ColumnClassStats::default();

    accumulate_table(
        &rt,
        TABLE_COLUMNS_HOT,
        &mut stats.columns_rows,
        &mut stats.columns_bytes,
    )?;

    let names = engine.table_names()?;
    for (name, class, _) in crate::schema::iter_shard_tables(&names) {
        if class == "columns" {
            accumulate_table(&rt, name, &mut stats.columns_rows, &mut stats.columns_bytes)?;
        }
    }

    Ok(stats)
}

fn accumulate_table(
    rt: &ReadTxn,
    table: &str,
    rows: &mut u64,
    bytes: &mut u64,
) -> Result<(), StoreError> {
    let lo = [0u8; 0];
    let hi = [0xffu8; 64];
    for item in rt.range(table, &lo, &hi)? {
        let (k, v) = item?;
        *rows = rows.saturating_add(1);
        *bytes = bytes
            .saturating_add(k.len() as u64)
            .saturating_add(v.len() as u64);
    }
    Ok(())
}

/// Re-export shard helpers under the AC names.
pub use crate::keys::{column_shard_of, column_slots_in};

/// Documented constants for external greps / docs.
pub mod docs {
    /// Column shard width in epochs (must equal prune cadence).
    pub const COLUMN_SHARD_WIDTH_EPOCHS: u64 = super::COLUMN_SHARD_EPOCHS;
    /// Slots per epoch used by shard arithmetic.
    pub const SLOTS_PER_EPOCH: u64 = super::SLOTS_PER_EPOCH;
    /// Slots per column shard = 32 × 32 = 1024.
    pub const SLOTS_PER_COLUMN_SHARD: u64 = COLUMN_SHARD_WIDTH_EPOCHS * SLOTS_PER_EPOCH;
    /// Sidecar fixed-part size.
    pub const SIDECAR_FIXED: usize = super::DATA_COLUMN_SIDECAR_FIXED_BYTES;
    /// Bytes per blob in a sidecar.
    pub const BYTES_PER_BLOB: usize = super::BYTES_PER_BLOB_IN_SIDECAR;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::canonical;
    use crate::engine::{Durability, EngineOptions};
    use crate::keys::{
        column_shard_of, column_shard_start_slot, column_slots_in, decode_hot_column_key,
        encode_cold_column_key, encode_hot_column_key, hot_column_root_end,
    };
    use proptest::prelude::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-columns-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn eng(label: &str) -> (PathBuf, Engine) {
        let dir = tmp_dir(label);
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        (dir, eng)
    }

    /// Minimal opaque sidecar body with `index` @ 0 and header `slot` @ 20.
    fn synth_sidecar(index: u16, slot: u64) -> Vec<u8> {
        // Pad to fixed-part size so formula helpers that check min length pass;
        // production rows are full SSZ; unit tests only need the two peeks.
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

    #[test]
    fn size_formula_constants() {
        assert_eq!(data_column_sidecar_size(0), 356);
        assert_eq!(data_column_sidecar_size(1), 356 + 2144);
        assert_eq!(data_column_sidecar_size(21), 45_380);
        assert_eq!(COLUMN_INDEX_SSZ_OFFSET, 0);
        assert_eq!(COLUMN_HEADER_SLOT_SSZ_OFFSET, 20);
        assert_eq!(COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, 36);
    }

    #[test]
    fn fixed_offsets_match_synth() {
        let ssz = synth_sidecar(7, 99);
        assert_eq!(column_index_at_offset(&ssz).unwrap(), 7);
        assert_eq!(column_slot_at_offset(&ssz).unwrap(), Slot::new(99));
        assert_eq!(
            column_parent_root_at_offset(&ssz).unwrap(),
            Root::from_array([0; 32])
        );
    }

    #[test]
    fn byte_identical_roundtrip_hot() {
        let (dir, eng) = eng("roundtrip");
        let slot = Slot::new(100);
        let root = root_n(0xAB);
        let index = 3u16;
        let ssz = synth_sidecar(index, 100);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            assert_eq!(
                put_column(&rt, &mut b, slot, &root, index, &ssz, BlockRegion::Hot).unwrap(),
                PutColumnOutcome::Inserted
            );
        }
        eng.commit(b).unwrap();
        let rt = eng.read().unwrap();
        let got = get_column_by_root(&rt, &root, index, Some(BlockRegion::Hot))
            .unwrap()
            .unwrap();
        assert_eq!(got, ssz, "stored bytes must be wire-identical");
        assert_eq!(slot_by_column_root(&rt, &root, index).unwrap(), Some(slot));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn idempotent_same_bytes_no_error() {
        let (dir, eng) = eng("idem");
        let slot = Slot::new(5);
        let root = root_n(0x11);
        let index = 1u16;
        let ssz = synth_sidecar(index, 5);
        for _ in 0..2 {
            let mut b = eng.batch();
            let rt = eng.read().unwrap();
            put_column(&rt, &mut b, slot, &root, index, &ssz, BlockRegion::Hot).unwrap();
            eng.commit(b).unwrap();
        }
        let stats = measure_column_class_stats(&eng).unwrap();
        assert_eq!(stats.columns_rows, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn different_bytes_same_key_is_key_collision() {
        let (dir, eng) = eng("collision");
        let slot = Slot::new(5);
        let root = root_n(0x11);
        let index = 2u16;
        let ssz_a = synth_sidecar(index, 5);
        let mut ssz_b = ssz_a.clone();
        ssz_b[DATA_COLUMN_SIDECAR_FIXED_BYTES - 1] ^= 0xFF;
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_column(&rt, &mut b, slot, &root, index, &ssz_a, BlockRegion::Hot).unwrap();
        }
        eng.commit(b).unwrap();

        let mut b = eng.batch();
        let rt = eng.read().unwrap();
        let err =
            put_column(&rt, &mut b, slot, &root, index, &ssz_b, BlockRegion::Hot).unwrap_err();
        assert!(
            matches!(err, StoreError::KeyCollision { ref table } if table == TABLE_COLUMNS_HOT),
            "err={err:?}"
        );
        let got = get_column_by_root(&eng.read().unwrap(), &root, index, Some(BlockRegion::Hot))
            .unwrap()
            .unwrap();
        assert_eq!(got, ssz_a);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cold_region_keys_by_slot_and_index() {
        let (dir, eng) = eng("cold");
        let slot = Slot::new(42);
        let root = root_n(0x42);
        let index = 5u16;
        let ssz = synth_sidecar(index, 42);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_column(&rt, &mut b, slot, &root, index, &ssz, BlockRegion::Cold).unwrap();
        }
        eng.commit(b).unwrap();
        let table = columns_shard_table(column_shard_id(slot));
        let rt = eng.read().unwrap();
        let by_key = rt
            .get(&table, &encode_cold_column_key(slot, index))
            .unwrap()
            .unwrap();
        assert_eq!(by_key, ssz);
        assert!(
            rt.get(
                TABLE_COLUMNS_HOT,
                &encode_hot_column_key(slot, &root, index)
            )
            .unwrap()
            .is_none()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cold_forward_scan_is_slot_index_order_no_sort() {
        // AC: write a shard's worth of cold columns in scrambled insertion order;
        // forward range scan emits them in (slot, index) order with no post-processing.
        let (dir, eng) = eng("cold-scan");
        // Use a handful of (slot, idx) pairs inside shard 0, inserted out of order.
        let pairs: Vec<(u64, u16)> = vec![
            (10, 3),
            (5, 127),
            (10, 0),
            (5, 0),
            (7, 1),
            (10, 2),
            (5, 1),
            (7, 0),
        ];
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for &(s, idx) in &pairs {
                let slot = Slot::new(s);
                let root = root_n((s as u8).wrapping_add(1));
                let ssz = synth_sidecar(idx, s);
                put_column(&rt, &mut b, slot, &root, idx, &ssz, BlockRegion::Cold).unwrap();
            }
        }
        eng.commit(b).unwrap();

        // Pure forward scan of the cold table (no sort helper used).
        let table = columns_shard_table(0);
        let rt = eng.read().unwrap();
        let lo = encode_cold_column_key(Slot::new(0), 0);
        let hi = encode_cold_column_key(Slot::new(u64::MAX), 0);
        let mut emitted: Vec<(u64, u16)> = Vec::new();
        for item in rt.range(&table, &lo, &hi).unwrap() {
            let (k, _) = item.unwrap();
            let (slot, idx) = decode_cold_column_key(&k).unwrap();
            emitted.push((slot.as_u64(), idx));
        }
        let mut expected = pairs.clone();
        expected.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        assert_eq!(
            emitted, expected,
            "forward scan must be (slot, index) order"
        );

        // Range API over the same set also emits ascending order.
        let rows = columns_by_range(
            &rt,
            Slot::new(5),
            6, // slots 5..11
            Some(Slot::new(u64::MAX)),
            None,
        )
        .unwrap();
        let seq: Vec<(u64, u16)> = rows.iter().map(|r| (r.slot.as_u64(), r.index)).collect();
        assert!(
            seq.windows(2)
                .all(|w| w[0].0 < w[1].0 || (w[0].0 == w[1].0 && w[0].1 <= w[1].1)),
            "range must be non-decreasing (slot, index): {seq:?}"
        );
        // Source must not call sort on the range path (grep AC).
        let src = include_str!("columns.rs");
        // Restrict to the production half (before `mod tests`).
        let prod = src.split("mod tests").next().unwrap();
        assert!(
            !prod.contains(".sort") && !prod.contains("sort_by") && !prod.contains("sort_unstable"),
            "columns.rs production path must not sort"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn by_range_128_slots_ascending_slot_index() {
        // CC-43 /2 — 128 slots, a few indices each, order straight off the index.
        let (dir, eng) = eng("range-128");
        let start = 100u64;
        let count = 128u64;
        let indices = [0u16, 1, 7, 127];

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0..count {
                let slot = Slot::new(start + i);
                let root = Root::from_array({
                    let mut a = [0u8; 32];
                    a[0..8].copy_from_slice(&(start + i).to_be_bytes());
                    a
                });
                for &idx in &indices {
                    let ssz = synth_sidecar(idx, start + i);
                    put_column(&rt, &mut b, slot, &root, idx, &ssz, BlockRegion::Cold).unwrap();
                }
                canonical::put_canonical(&rt, &mut b, slot, &root).unwrap();
            }
        }
        eng.commit(b).unwrap();

        let rt = eng.read().unwrap();
        let rows = columns_by_range(
            &rt,
            Slot::new(start),
            count,
            Some(Slot::new(u64::MAX)),
            Some(&indices),
        )
        .unwrap();
        assert_eq!(rows.len(), (count as usize) * indices.len());
        let seq: Vec<(u64, u16)> = rows.iter().map(|r| (r.slot.as_u64(), r.index)).collect();
        let mut expected = Vec::new();
        for i in 0..count {
            for &idx in &indices {
                expected.push((start + i, idx));
            }
        }
        assert_eq!(seq, expected, "must be ascending (slot, column_index)");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn columns_for_block_whole_or_nothing_with_missing_named() {
        // AC: request {0,1,2,3} where only {0,1} held → held subset + missing named.
        let (dir, eng) = eng("atomic");
        let slot = Slot::new(50);
        let root = root_n(0x50);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for idx in [0u16, 1] {
                let ssz = synth_sidecar(idx, 50);
                put_column(&rt, &mut b, slot, &root, idx, &ssz, BlockRegion::Hot).unwrap();
            }
        }
        eng.commit(b).unwrap();

        let rt = eng.read().unwrap();
        let res = columns_for_block(&rt, slot, &root, &[0, 1, 2, 3], BlockRegion::Hot).unwrap();
        assert_eq!(res.held.len(), 2);
        assert_eq!(res.held[0].0, 0);
        assert_eq!(res.held[1].0, 1);
        assert_eq!(res.missing, vec![2, 3]);
        // Helper never returns a half-block without naming the gap.
        assert!(!res.missing.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_blob_block_yields_empty_set_not_error() {
        // AC: zero-blob / no column rows → empty set, not an error.
        let (dir, eng) = eng("zero-blob");
        let slot = Slot::new(3_649_445); // Hoodi empty-slot shape
        let root = root_n(0x01);
        let rt = eng.read().unwrap();
        let res = columns_for_block(&rt, slot, &root, &[0, 1, 2, 3], BlockRegion::Hot).unwrap();
        assert!(res.held.is_empty());
        assert!(res.is_empty());
        // All requested are missing when nothing is stored.
        assert_eq!(res.missing, vec![0, 1, 2, 3]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn da_status_deferred_roundtrips_and_is_not_promoted_by_read() {
        let (dir, eng) = eng("da");
        let root = root_n(0xDE);
        let slot = Slot::new(77);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_da_status(&rt, &mut b, &root, DaStatus::Deferred, slot).unwrap();
        }
        eng.commit(b).unwrap();

        let rt = eng.read().unwrap();
        // Multiple reads must not promote.
        for _ in 0..3 {
            let (st, s) = get_da_status(&rt, &root).unwrap().unwrap();
            assert_eq!(st, DaStatus::Deferred);
            assert_eq!(s, slot);
        }
        // Explicit write is the only path to Available.
        let mut b = eng.batch();
        put_da_status(&rt, &mut b, &root, DaStatus::Available, slot).unwrap();
        eng.commit(b).unwrap();
        let (st, _) = get_da_status(&eng.read().unwrap(), &root).unwrap().unwrap();
        assert_eq!(st, DaStatus::Available);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn da_status_refuses_available_to_deferred_demotion() {
        let (dir, eng) = eng("da-demote");
        let root = root_n(0xAD);
        let slot = Slot::new(88);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_da_status(&rt, &mut b, &root, DaStatus::Available, slot).unwrap();
        }
        eng.commit(b).unwrap();

        let mut b = eng.batch();
        let rt = eng.read().unwrap();
        let err = put_da_status(&rt, &mut b, &root, DaStatus::Deferred, slot).unwrap_err();
        assert!(
            matches!(err, StoreError::Codec(_)),
            "demotion must fail closed: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("demotion refused") && msg.contains("Available"),
            "{msg}"
        );
        // Row unchanged.
        let (st, s) = get_da_status(&eng.read().unwrap(), &root).unwrap().unwrap();
        assert_eq!(st, DaStatus::Available);
        assert_eq!(s, slot);
        assert!(b.is_empty(), "failed demotion must not stage a put");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_column_rejects_oversized_sidecar_value() {
        let (dir, eng) = eng("oversized");
        let slot = Slot::new(1);
        let root = root_n(0x0F);
        // Minimal valid peeks, then pad past the hard cap.
        let mut ssz = synth_sidecar(0, 1);
        ssz.resize(MAX_COLUMN_SIDECAR_BYTES + 1, 0xAB);
        assert!(ssz.len() > MAX_COLUMN_SIDECAR_BYTES);

        let mut b = eng.batch();
        let rt = eng.read().unwrap();
        let err = put_column(&rt, &mut b, slot, &root, 0, &ssz, BlockRegion::Hot).unwrap_err();
        assert!(
            matches!(err, StoreError::Limit(_)),
            "oversized value must Limit, got {err:?}"
        );
        assert!(
            err.to_string().contains("MAX_COLUMN_SIDECAR_BYTES"),
            "{err}"
        );
        assert!(b.is_empty());
        // Boundary: exactly at the cap with valid peeks is accepted.
        let mut at_cap = synth_sidecar(0, 1);
        at_cap.resize(MAX_COLUMN_SIDECAR_BYTES, 0);
        let mut b = eng.batch();
        put_column(&rt, &mut b, slot, &root, 0, &at_cap, BlockRegion::Hot).unwrap();
        eng.commit(b).unwrap();
        assert_eq!(measure_column_class_stats(&eng).unwrap().columns_rows, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cold_columns_for_block_is_root_blind() {
        // ADR P4-09: cold lookup is (slot, idx); a mismatched root still returns
        // held rows — serve layer owns canonical root selection.
        let (dir, eng) = eng("cold-root-blind");
        let slot = Slot::new(42);
        let stored_root = root_n(0x42);
        let wrong_root = root_n(0x99);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for idx in [0u16, 2] {
                let ssz = synth_sidecar(idx, 42);
                put_column(
                    &rt,
                    &mut b,
                    slot,
                    &stored_root,
                    idx,
                    &ssz,
                    BlockRegion::Cold,
                )
                .unwrap();
            }
        }
        eng.commit(b).unwrap();

        let rt = eng.read().unwrap();
        let res = columns_for_block(&rt, slot, &wrong_root, &[0, 1, 2], BlockRegion::Cold).unwrap();
        assert_eq!(
            res.held.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(res.missing, vec![1]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn max_sidecar_bytes_matches_formula() {
        assert_eq!(
            MAX_COLUMN_SIDECAR_BYTES,
            data_column_sidecar_size(MAX_BLOBS_PER_COLUMN_SIDECAR)
        );
        assert_eq!(MAX_COLUMN_SIDECAR_BYTES, 356 + 4096 * 2144);
    }

    #[test]
    fn class_stats_move_under_synthetic_write_load() {
        let (dir, eng) = eng("stats");
        let before = measure_column_class_stats(&eng).unwrap();
        assert_eq!(before.columns_rows, 0);
        assert_eq!(before.columns_bytes, 0);

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0u8..10 {
                let slot = Slot::new(u64::from(i));
                let root = root_n(i.wrapping_add(1));
                let idx = u16::from(i % 8);
                let ssz = synth_sidecar(idx, u64::from(i));
                put_column(&rt, &mut b, slot, &root, idx, &ssz, BlockRegion::Hot).unwrap();
            }
        }
        eng.commit(b).unwrap();

        let after = measure_column_class_stats(&eng).unwrap();
        assert_eq!(after.columns_rows, 10);
        assert!(after.columns_bytes > 0);
        // Prometheus names the storage service exposes (documentation anchor):
        let _ = ("cc_storage_bytes_total", "cc_storage_rows_total", "columns");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn columns_by_range_count_over_cap_fails_closed() {
        let (dir, eng) = eng("range-cap");
        let rt = eng.read().unwrap();
        let err = columns_by_range(
            &rt,
            Slot::new(0),
            MAX_COLUMNS_BY_RANGE_SLOTS + 1,
            None,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, StoreError::Limit(_)), "got {err:?}");
        assert!(
            columns_by_range(&rt, Slot::new(0), MAX_COLUMNS_BY_RANGE_SLOTS, None, None).is_ok()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_column_rejects_slot_or_index_mismatch() {
        let (dir, eng) = eng("mismatch");
        let ssz = synth_sidecar(3, 10);
        let mut b = eng.batch();
        let rt = eng.read().unwrap();
        let err = put_column(
            &rt,
            &mut b,
            Slot::new(99),
            &root_n(1),
            3,
            &ssz,
            BlockRegion::Hot,
        )
        .unwrap_err();
        assert!(matches!(err, StoreError::Codec(_)));
        let err = put_column(
            &rt,
            &mut b,
            Slot::new(10),
            &root_n(1),
            4,
            &ssz,
            BlockRegion::Hot,
        )
        .unwrap_err();
        assert!(matches!(err, StoreError::Codec(_)));
        assert!(b.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn range_straddling_column_shard_boundary() {
        let (dir, eng) = eng("shard-boundary");
        let boundary = column_shard_start_slot(1).as_u64(); // 1024
        assert_eq!(boundary, 32 * 32);
        let start = boundary - 4;
        let count = 8u64;
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0..count {
                let slot = Slot::new(start + i);
                let root = root_n((i as u8).wrapping_add(1));
                let ssz = synth_sidecar(0, start + i);
                put_column(&rt, &mut b, slot, &root, 0, &ssz, BlockRegion::Cold).unwrap();
                canonical::put_canonical(&rt, &mut b, slot, &root).unwrap();
            }
        }
        eng.commit(b).unwrap();
        let names = eng.table_names().unwrap();
        assert!(names.iter().any(|n| n == &columns_shard_table(0)));
        assert!(names.iter().any(|n| n == &columns_shard_table(1)));

        let rt = eng.read().unwrap();
        let rows = columns_by_range(
            &rt,
            Slot::new(start),
            count,
            Some(Slot::new(u64::MAX)),
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 8);
        assert_eq!(column_shard_of(rows[0].slot), 0);
        assert_eq!(column_shard_of(rows[7].slot), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_000))]

        /// Key ordering across the index field: encode(s, r, i) orders by
        /// (slot, root, index) with i ∈ [0, 128) — including 0 and 127 so a
        /// little-endian index mistake is visible.
        #[test]
        fn encode_orders_by_slot_root_index(
            s1 in 0u64..100_000u64,
            s2 in 0u64..100_000u64,
            r1 in prop::array::uniform32(any::<u8>()),
            r2 in prop::array::uniform32(any::<u8>()),
            i1 in 0u16..128u16,
            i2 in 0u16..128u16,
        ) {
            let a = encode_hot_column_key(Slot::new(s1), &Root::from_array(r1), i1);
            let b = encode_hot_column_key(Slot::new(s2), &Root::from_array(r2), i2);
            let byte_ord = a.as_slice().cmp(b.as_slice());
            let triple_ord = (s1, r1.as_slice(), i1).cmp(&(s2, r2.as_slice(), i2));
            prop_assert_eq!(byte_ord, triple_ord);

            // Cold key is pure (slot, index) response order.
            let ca = encode_cold_column_key(Slot::new(s1), i1);
            let cb = encode_cold_column_key(Slot::new(s2), i2);
            let cold_byte = ca.as_slice().cmp(cb.as_slice());
            let cold_pair = (s1, i1).cmp(&(s2, i2));
            prop_assert_eq!(cold_byte, cold_pair);
        }

        #[test]
        fn column_shard_of_slots_in_inverse(slot in 0u64..1_000_000u64) {
            let s = Slot::new(slot);
            let id = column_shard_of(s);
            let (lo, hi) = column_slots_in(id);
            prop_assert!(s.as_u64() >= lo.as_u64());
            prop_assert!(s.as_u64() < hi.as_u64());
            let name = columns_shard_table(id);
            let parsed = crate::schema::parse_shard_table(&name);
            prop_assert_eq!(parsed, Some(("columns", id)));
            prop_assert_eq!(hi.as_u64() - lo.as_u64(), COLUMN_SHARD_EPOCHS * SLOTS_PER_EPOCH);
        }
    }

    #[test]
    fn hot_column_root_end_range_stops_before_next_root() {
        let (dir, eng) = eng("root-end");
        let slot = Slot::new(9);
        let mut root_bytes = [0u8; 32];
        root_bytes[31] = 0x10;
        let root = Root::from_array(root_bytes);
        let mut next_bytes = [0u8; 32];
        next_bytes[31] = 0x11;
        let next = Root::from_array(next_bytes);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for (r, idx) in [(&root, 0u16), (&root, 3), (&next, 0)] {
                let ssz = synth_sidecar(idx, 9);
                put_column(&rt, &mut b, slot, r, idx, &ssz, BlockRegion::Hot).unwrap();
            }
        }
        eng.commit(b).unwrap();

        let lo = encode_hot_column_key(slot, &root, 0);
        let hi = hot_column_root_end(slot, &root);
        assert_eq!(hi, encode_hot_column_key(slot, &next, 0));
        let rt = eng.read().unwrap();
        let mut got = Vec::new();
        for item in rt.range(TABLE_COLUMNS_HOT, &lo, &hi).unwrap() {
            let (k, _) = item.unwrap();
            let decoded = decode_hot_column_key(&k).unwrap();
            got.push(decoded);
        }
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|(s, r, _)| *s == slot && *r == root));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn hot_column_root_end_all_0xff_range_stops_before_next_slot() {
        let (dir, eng) = eng("root-end-ff");
        let slot = Slot::new(4);
        let root = Root::from_array([0xff; 32]);
        let next_slot = Slot::new(5);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            let ssz = synth_sidecar(1, 4);
            put_column(&rt, &mut b, slot, &root, 1, &ssz, BlockRegion::Hot).unwrap();
            let next_ssz = synth_sidecar(0, 5);
            put_column(
                &rt,
                &mut b,
                next_slot,
                &Root::ZERO,
                0,
                &next_ssz,
                BlockRegion::Hot,
            )
            .unwrap();
        }
        eng.commit(b).unwrap();

        let lo = encode_hot_column_key(slot, &root, 0);
        let hi = hot_column_root_end(slot, &root);
        assert_eq!(hi, encode_hot_column_key(next_slot, &Root::ZERO, 0));
        let rt = eng.read().unwrap();
        let mut got = Vec::new();
        for item in rt.range(TABLE_COLUMNS_HOT, &lo, &hi).unwrap() {
            let (k, _) = item.unwrap();
            got.push(decode_hot_column_key(&k).unwrap());
        }
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, slot);
        assert_eq!(got[0].1, root);
        assert_eq!(got[0].2, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn index_0_and_127_big_endian_key_order() {
        // Little-endian would put 127 before 0 in the second key byte only when
        // the low byte wraps; with u16 BE, 0 < 127 always as a full field.
        let slot = Slot::new(1);
        let root = Root::ZERO;
        let k0 = encode_hot_column_key(slot, &root, 0);
        let k127 = encode_hot_column_key(slot, &root, 127);
        assert!(k0 < k127);
        let c0 = encode_cold_column_key(slot, 0);
        let c127 = encode_cold_column_key(slot, 127);
        assert!(c0 < c127);
        // And 255 would sort after 127 under BE; LE would put 255 before 1 if
        // only the low byte were compared first — we store u16 BE either way.
        let c255 = encode_cold_column_key(slot, 255);
        assert!(c127 < c255);
    }
}
