//! Big-endian fixed-width key codecs and shard arithmetic (Architecture §2.1 / §2.4).
//!
//! Byte order **is** key order: `encode(a) < encode(b) ⟺ a < b`. Values stay opaque.

use cc_types::{Root, Slot};

/// Spec mainnet slots per epoch (also minimal for these codecs' arithmetic unit).
pub const SLOTS_PER_EPOCH: u64 = 32;

/// Column shard width in epochs (Deviation 1 / ADR P4-10).
pub const COLUMN_SHARD_EPOCHS: u64 = 32;

/// Block shard width in epochs (Deviation 1 / ADR P4-10).
pub const BLOCK_SHARD_EPOCHS: u64 = 256;

/// Epoch of a slot (`slot // SLOTS_PER_EPOCH`).
pub fn epoch_of(slot: Slot) -> u64 {
    slot.as_u64() / SLOTS_PER_EPOCH
}

/// Column-class shard id for `slot` (32-epoch shards).
pub fn column_shard_id(slot: Slot) -> u64 {
    epoch_of(slot) / COLUMN_SHARD_EPOCHS
}

/// Block-class shard id for `slot` (256-epoch shards).
pub fn block_shard_id(slot: Slot) -> u64 {
    epoch_of(slot) / BLOCK_SHARD_EPOCHS
}

/// First slot of column shard `id`.
pub fn column_shard_start_slot(shard_id: u64) -> Slot {
    Slot::new(
        shard_id
            .saturating_mul(COLUMN_SHARD_EPOCHS)
            .saturating_mul(SLOTS_PER_EPOCH),
    )
}

/// First slot of block shard `id`.
pub fn block_shard_start_slot(shard_id: u64) -> Slot {
    Slot::new(
        shard_id
            .saturating_mul(BLOCK_SHARD_EPOCHS)
            .saturating_mul(SLOTS_PER_EPOCH),
    )
}

/// Prefix for cold block shard tables (`blocks_{suffix}`).
pub const BLOCKS_SHARD_PREFIX: &str = "blocks_";
/// Prefix for cold column shard tables (`columns_{suffix}`).
pub const COLUMNS_SHARD_PREFIX: &str = "columns_";

/// Canonical zero-pad width for shard-id suffixes (ADR-P4-10).
///
/// IDs below `10^{width}` stay fixed-width (`00042`). Larger ids emit more
/// digits (`100000`) so the suffix remains a lossless encoding of the logical
/// shard id — the intern pool retires dropped names; it does not wrap ids.
pub const SHARD_TABLE_SUFFIX_WIDTH: usize = 5;

/// Zero-padded shard table suffix (`00042`, or `100000` once the pad overflows).
///
/// Inverse of [`parse_shard_suffix`]. The suffix **is** the logical shard id,
/// not a slot in a wrap-around table-name ring (I-shards reads the id back).
pub fn format_shard_suffix(shard_id: u64) -> String {
    format!("{shard_id:05}")
}

/// Parse a suffix produced by [`format_shard_suffix`].
///
/// Rejects unpadded (`42`), over-padded (`000042`), and non-digit strings so
/// `blocks_42` / `blocks_000042` cannot alias `blocks_00042`.
pub fn parse_shard_suffix(suffix: &str) -> Option<u64> {
    if suffix.len() < SHARD_TABLE_SUFFIX_WIDTH {
        return None;
    }
    if !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let id = suffix.parse::<u64>().ok()?;
    if format_shard_suffix(id) != suffix {
        return None;
    }
    Some(id)
}

/// `columns_{shard}` table name.
pub fn columns_shard_table(shard_id: u64) -> String {
    format!("{COLUMNS_SHARD_PREFIX}{}", format_shard_suffix(shard_id))
}

/// `blocks_{shard}` table name.
pub fn blocks_shard_table(shard_id: u64) -> String {
    format!("{BLOCKS_SHARD_PREFIX}{}", format_shard_suffix(shard_id))
}

// ---------------------------------------------------------------------------
// Encodings — fixed-width BE concatenations
// ---------------------------------------------------------------------------

/// Hot block key: `slot:u64be ‖ root:32` (40 B).
pub fn encode_hot_block_key(slot: Slot, root: &Root) -> [u8; 40] {
    let mut out = [0u8; 40];
    out[..8].copy_from_slice(&slot.as_u64().to_be_bytes());
    out[8..].copy_from_slice(root.as_slice());
    out
}

/// Cold block key: `slot:u64be` (8 B).
pub fn encode_cold_block_key(slot: Slot) -> [u8; 8] {
    slot.as_u64().to_be_bytes()
}

/// Hot column key: `slot:u64be ‖ root:32 ‖ idx:u16be` (42 B).
pub fn encode_hot_column_key(slot: Slot, root: &Root, index: u16) -> [u8; 42] {
    let mut out = [0u8; 42];
    out[..8].copy_from_slice(&slot.as_u64().to_be_bytes());
    out[8..40].copy_from_slice(root.as_slice());
    out[40..].copy_from_slice(&index.to_be_bytes());
    out
}

/// Exclusive end key for all hot columns of `(slot, root)`.
///
/// Hot keys sort `slot ‖ root ‖ idx`. The first key that is not this root is
/// the next root at index 0, or the next slot at `Root::ZERO` when `root` is
/// all `0xff` (no successor in the 32-byte field).
pub fn hot_column_root_end(slot: Slot, root: &Root) -> [u8; 42] {
    let mut next_root = [0u8; 32];
    next_root.copy_from_slice(root.as_slice());
    let mut carry = true;
    for b in next_root.iter_mut().rev() {
        if !carry {
            break;
        }
        let (n, c) = b.overflowing_add(1);
        *b = n;
        carry = c;
    }
    if carry {
        encode_hot_column_key(Slot::new(slot.as_u64().saturating_add(1)), &Root::ZERO, 0)
    } else {
        encode_hot_column_key(slot, &Root::from_array(next_root), 0)
    }
}

/// Cold column key: `slot:u64be ‖ idx:u16be` (10 B).
pub fn encode_cold_column_key(slot: Slot, index: u16) -> [u8; 10] {
    let mut out = [0u8; 10];
    out[..8].copy_from_slice(&slot.as_u64().to_be_bytes());
    out[8..].copy_from_slice(&index.to_be_bytes());
    out
}

/// Flat-layout key with shard prefix: `shard:u16be ‖ slot:u64be ‖ idx:u16be` (12 B).
///
/// Used when shards are key prefixes inside one table (Architecture §2.4 fallback).
pub fn encode_flat_column_key(shard_id: u16, slot: Slot, index: u16) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[..2].copy_from_slice(&shard_id.to_be_bytes());
    out[2..10].copy_from_slice(&slot.as_u64().to_be_bytes());
    out[10..].copy_from_slice(&index.to_be_bytes());
    out
}

/// Exclusive end key for all cold columns at `slot` (prefix successor).
pub fn cold_column_slot_end(slot: Slot) -> [u8; 10] {
    // Next slot's zero index — half-open range over this slot's columns.
    encode_cold_column_key(Slot::new(slot.as_u64().saturating_add(1)), 0)
}

/// Half-open range covering every cold column key in `[start_slot, end_slot)`.
pub fn cold_column_slot_range(start_slot: Slot, end_slot: Slot) -> ([u8; 10], [u8; 10]) {
    (
        encode_cold_column_key(start_slot, 0),
        encode_cold_column_key(end_slot, 0),
    )
}

/// Half-open range for one epoch of cold columns (32 slots).
pub fn cold_column_epoch_range(epoch: u64) -> ([u8; 10], [u8; 10]) {
    let start = Slot::new(epoch.saturating_mul(SLOTS_PER_EPOCH));
    let end = Slot::new(epoch.saturating_add(1).saturating_mul(SLOTS_PER_EPOCH));
    cold_column_slot_range(start, end)
}

/// Half-open range for one epoch of cold blocks.
pub fn cold_block_epoch_range(epoch: u64) -> ([u8; 8], [u8; 8]) {
    let start = epoch.saturating_mul(SLOTS_PER_EPOCH);
    let end = epoch.saturating_add(1).saturating_mul(SLOTS_PER_EPOCH);
    (start.to_be_bytes(), end.to_be_bytes())
}

/// Decode slot prefix from a hot block key (`slot ‖ root`, 40 B).
pub fn decode_hot_block_key(key: &[u8]) -> Option<(Slot, Root)> {
    if key.len() != 40 {
        return None;
    }
    let mut slot_be = [0u8; 8];
    slot_be.copy_from_slice(&key[..8]);
    let mut root_arr = [0u8; 32];
    root_arr.copy_from_slice(&key[8..40]);
    Some((
        Slot::new(u64::from_be_bytes(slot_be)),
        Root::from_array(root_arr),
    ))
}

/// Decode cold block key (`slot`, 8 B).
pub fn decode_cold_block_key(key: &[u8]) -> Option<Slot> {
    if key.len() != 8 {
        return None;
    }
    let mut slot_be = [0u8; 8];
    slot_be.copy_from_slice(key);
    Some(Slot::new(u64::from_be_bytes(slot_be)))
}

/// Decode hot column key (`slot ‖ root ‖ idx`, 42 B).
pub fn decode_hot_column_key(key: &[u8]) -> Option<(Slot, Root, u16)> {
    if key.len() != 42 {
        return None;
    }
    let mut slot_be = [0u8; 8];
    slot_be.copy_from_slice(&key[..8]);
    let mut root_arr = [0u8; 32];
    root_arr.copy_from_slice(&key[8..40]);
    let mut idx_be = [0u8; 2];
    idx_be.copy_from_slice(&key[40..42]);
    Some((
        Slot::new(u64::from_be_bytes(slot_be)),
        Root::from_array(root_arr),
        u16::from_be_bytes(idx_be),
    ))
}

/// Decode cold column key (`slot ‖ idx`, 10 B).
pub fn decode_cold_column_key(key: &[u8]) -> Option<(Slot, u16)> {
    if key.len() != 10 {
        return None;
    }
    let mut slot_be = [0u8; 8];
    slot_be.copy_from_slice(&key[..8]);
    let mut idx_be = [0u8; 2];
    idx_be.copy_from_slice(&key[8..10]);
    Some((
        Slot::new(u64::from_be_bytes(slot_be)),
        u16::from_be_bytes(idx_be),
    ))
}

/// `column_slot_by_root` key: `root:32 ‖ idx:u16be` (34 B).
pub fn encode_column_slot_by_root_key(root: &Root, index: u16) -> [u8; 34] {
    let mut out = [0u8; 34];
    out[..32].copy_from_slice(root.as_slice());
    out[32..].copy_from_slice(&index.to_be_bytes());
    out
}

/// Decode `column_slot_by_root` key.
pub fn decode_column_slot_by_root_key(key: &[u8]) -> Option<(Root, u16)> {
    if key.len() != 34 {
        return None;
    }
    let mut root_arr = [0u8; 32];
    root_arr.copy_from_slice(&key[..32]);
    let mut idx_be = [0u8; 2];
    idx_be.copy_from_slice(&key[32..34]);
    Some((Root::from_array(root_arr), u16::from_be_bytes(idx_be)))
}

/// `column_slot_by_root` value: `slot:u64be` (8 B).
pub fn encode_column_slot_by_root_value(slot: Slot) -> [u8; 8] {
    slot.as_u64().to_be_bytes()
}

/// Decode `column_slot_by_root` value.
pub fn decode_column_slot_by_root_value(value: &[u8]) -> Option<Slot> {
    decode_cold_block_key(value)
}

/// Exclusive upper bound for hot column rows with `slot ≤ max_slot`.
pub fn hot_column_slot_upper_bound(max_slot_inclusive: Slot) -> [u8; 42] {
    let next = Slot::new(max_slot_inclusive.as_u64().saturating_add(1));
    encode_hot_column_key(next, &Root::ZERO, 0)
}

/// Half-open key range for all hot columns at a single slot (any root, any index).
pub fn hot_column_slot_range(slot: Slot) -> ([u8; 42], [u8; 42]) {
    (
        encode_hot_column_key(slot, &Root::ZERO, 0),
        hot_column_slot_upper_bound(slot),
    )
}

/// Half-open slot range of column shard `id` (32-epoch width).
pub fn column_shard_slot_range(shard_id: u64) -> (Slot, Slot) {
    let start = column_shard_start_slot(shard_id);
    let end = column_shard_start_slot(shard_id.saturating_add(1));
    (start, end)
}

/// Column-class shard id for `slot` (AC alias of [`column_shard_id`]).
pub fn column_shard_of(slot: Slot) -> u64 {
    column_shard_id(slot)
}

/// Half-open `[start, end)` slot range for column shard `id` (AC name).
pub fn column_slots_in(shard_id: u64) -> (Slot, Slot) {
    column_shard_slot_range(shard_id)
}

/// Decode canonical key (`slot`, 8 B) and value (`root`, 32 B).
pub fn decode_canonical_entry(key: &[u8], value: &[u8]) -> Option<(Slot, Root)> {
    let slot = decode_cold_block_key(key)?;
    if value.len() != 32 {
        return None;
    }
    let mut root_arr = [0u8; 32];
    root_arr.copy_from_slice(value);
    Some((slot, Root::from_array(root_arr)))
}

/// Encode a 32-byte root key (`block_slot_by_root`, `state_roots` value side, …).
pub fn encode_root_key(root: &Root) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(root.as_slice());
    out
}

/// Decode a 32-byte root key.
pub fn decode_root_key(key: &[u8]) -> Option<Root> {
    if key.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(key);
    Some(Root::from_array(arr))
}

/// Hot/cold region tag stored in `block_slot_by_root` values (§2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BlockRegion {
    /// Above the split (`blocks_hot`).
    Hot = 0,
    /// At or below the split (`blocks_{shard}`).
    Cold = 1,
}

impl BlockRegion {
    /// Parse the region byte; unknown values yield `None`.
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Hot),
            1 => Some(Self::Cold),
            _ => None,
        }
    }

    /// Wire / stored byte.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// `block_slot_by_root` value: `slot:u64be ‖ region:u8` (9 B).
pub fn encode_block_slot_by_root_value(slot: Slot, region: BlockRegion) -> [u8; 9] {
    let mut out = [0u8; 9];
    out[..8].copy_from_slice(&slot.as_u64().to_be_bytes());
    out[8] = region.as_u8();
    out
}

/// Decode `block_slot_by_root` value.
pub fn decode_block_slot_by_root_value(value: &[u8]) -> Option<(Slot, BlockRegion)> {
    if value.len() != 9 {
        return None;
    }
    let mut slot_be = [0u8; 8];
    slot_be.copy_from_slice(&value[..8]);
    let region = BlockRegion::from_u8(value[8])?;
    Some((Slot::new(u64::from_be_bytes(slot_be)), region))
}

/// Encode a root as a 32-byte table value (`canonical`, `state_roots`).
pub fn encode_root_value(root: &Root) -> [u8; 32] {
    encode_root_key(root)
}

/// Decode a 32-byte root table value.
pub fn decode_root_value(value: &[u8]) -> Option<Root> {
    decode_root_key(value)
}

/// Half-open slot range of block shard `id` (256-epoch width).
///
/// `shard_of` / `slots_in` inverses: every slot `s` satisfies
/// `slots_in(shard_of(s)).0 ≤ s < slots_in(shard_of(s)).1`.
pub fn block_shard_slot_range(shard_id: u64) -> (Slot, Slot) {
    let start = block_shard_start_slot(shard_id);
    let end = block_shard_start_slot(shard_id.saturating_add(1));
    (start, end)
}

/// Block-class shard id for `slot` (alias of [`block_shard_id`] for the AC name).
pub fn shard_of(slot: Slot) -> u64 {
    block_shard_id(slot)
}

/// Half-open `[start, end)` slot range for block shard `id` (AC name).
pub fn slots_in(shard_id: u64) -> (Slot, Slot) {
    block_shard_slot_range(shard_id)
}

/// Decode snapshot key (`slot`, 8 B).
pub fn decode_snapshot_key(key: &[u8]) -> Option<Slot> {
    decode_cold_block_key(key)
}

/// Exclusive upper bound key for hot block rows with `slot ≤ max_slot` (slot-prefix order).
pub fn hot_block_slot_upper_bound(max_slot_inclusive: Slot) -> [u8; 40] {
    let next = Slot::new(max_slot_inclusive.as_u64().saturating_add(1));
    encode_hot_block_key(next, &Root::ZERO)
}

/// Exclusive upper bound for cold block rows with `slot ≤ max_slot`.
pub fn cold_block_slot_upper_bound(max_slot_inclusive: Slot) -> [u8; 8] {
    encode_cold_block_key(Slot::new(max_slot_inclusive.as_u64().saturating_add(1)))
}

/// Ordering key used by the §2.1 property test: `(slot, root, index)` as cold/hot composite.
///
/// Encodes the hot column key so the property covers all three components.
pub fn encode_order_key(slot: Slot, root: &Root, index: u16) -> [u8; 42] {
    encode_hot_column_key(slot, root, index)
}

/// Lexicographic compare of the triple as the schema orders it.
pub fn order_triple(a: (Slot, Root, u16), b: (Slot, Root, u16)) -> std::cmp::Ordering {
    a.0.as_u64()
        .cmp(&b.0.as_u64())
        .then_with(|| a.1.as_slice().cmp(b.1.as_slice()))
        .then_with(|| a.2.cmp(&b.2))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use proptest::prelude::*;

    #[test]
    fn column_shard_boundaries() {
        // slot 0 → shard 0
        assert_eq!(column_shard_id(Slot::new(0)), 0);
        // last slot of shard 0: epochs 0..31 → slots 0..1023
        let last_of_0 = Slot::new(COLUMN_SHARD_EPOCHS * SLOTS_PER_EPOCH - 1);
        assert_eq!(column_shard_id(last_of_0), 0);
        // first slot of shard 1
        let first_of_1 = Slot::new(COLUMN_SHARD_EPOCHS * SLOTS_PER_EPOCH);
        assert_eq!(column_shard_id(first_of_1), 1);
        // last of shard n, first of n+1 for n=3
        let n = 3u64;
        let last_n = Slot::new((n + 1) * COLUMN_SHARD_EPOCHS * SLOTS_PER_EPOCH - 1);
        let first_np1 = Slot::new((n + 1) * COLUMN_SHARD_EPOCHS * SLOTS_PER_EPOCH);
        assert_eq!(column_shard_id(last_n), n);
        assert_eq!(column_shard_id(first_np1), n + 1);
        assert_eq!(column_shard_start_slot(n + 1).as_u64(), first_np1.as_u64());
    }

    #[test]
    fn block_shard_boundaries() {
        assert_eq!(block_shard_id(Slot::new(0)), 0);
        let last_of_0 = Slot::new(BLOCK_SHARD_EPOCHS * SLOTS_PER_EPOCH - 1);
        assert_eq!(block_shard_id(last_of_0), 0);
        let first_of_1 = Slot::new(BLOCK_SHARD_EPOCHS * SLOTS_PER_EPOCH);
        assert_eq!(block_shard_id(first_of_1), 1);
        let n = 2u64;
        let last_n = Slot::new((n + 1) * BLOCK_SHARD_EPOCHS * SLOTS_PER_EPOCH - 1);
        let first_np1 = Slot::new((n + 1) * BLOCK_SHARD_EPOCHS * SLOTS_PER_EPOCH);
        assert_eq!(block_shard_id(last_n), n);
        assert_eq!(block_shard_id(first_np1), n + 1);
    }

    #[test]
    fn shard_suffix_roundtrip_and_rejects_aliases() {
        assert_eq!(format_shard_suffix(0), "00000");
        assert_eq!(format_shard_suffix(42), "00042");
        assert_eq!(format_shard_suffix(99_999), "99999");
        assert_eq!(format_shard_suffix(100_000), "100000");
        assert_eq!(parse_shard_suffix("00042"), Some(42));
        assert_eq!(parse_shard_suffix("100000"), Some(100_000));
        assert_eq!(parse_shard_suffix("42"), None);
        assert_eq!(parse_shard_suffix("000042"), None);
        assert_eq!(parse_shard_suffix("00a42"), None);
        assert_eq!(columns_shard_table(42), "columns_00042");
        assert_eq!(blocks_shard_table(100_000), "blocks_100000");
    }

    #[test]
    fn cold_column_keys_order_by_slot_then_index() {
        let a = encode_cold_column_key(Slot::new(1), 0);
        let b = encode_cold_column_key(Slot::new(1), 1);
        let c = encode_cold_column_key(Slot::new(2), 0);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn hot_column_root_end_is_first_key_of_next_root() {
        let slot = Slot::new(42);
        let mut root = [0u8; 32];
        root[31] = 0x10;
        let mut next = [0u8; 32];
        next[31] = 0x11;
        let end = hot_column_root_end(slot, &Root::from_array(root));
        assert_eq!(end, encode_hot_column_key(slot, &Root::from_array(next), 0));
        assert!(encode_hot_column_key(slot, &Root::from_array(root), 0) < end);
        assert!(encode_hot_column_key(slot, &Root::from_array(root), u16::MAX) < end);

        // Carry through the last byte into the previous one.
        let mut root_ff = [0u8; 32];
        root_ff[30] = 0x01;
        root_ff[31] = 0xff;
        let mut next_carry = [0u8; 32];
        next_carry[30] = 0x02;
        assert_eq!(
            hot_column_root_end(slot, &Root::from_array(root_ff)),
            encode_hot_column_key(slot, &Root::from_array(next_carry), 0)
        );
    }

    #[test]
    fn hot_column_root_end_all_0xff_is_next_slot() {
        let slot = Slot::new(42);
        let root = Root::from_array([0xff; 32]);
        let end = hot_column_root_end(slot, &root);
        assert_eq!(end, encode_hot_column_key(Slot::new(43), &Root::ZERO, 0));
        assert!(encode_hot_column_key(slot, &root, u16::MAX) < end);
    }

    #[test]
    fn hot_column_root_end_defined_once() {
        let keys = include_str!("keys.rs");
        let prod = keys.split("#[cfg(test)]").next().unwrap_or(keys);
        assert_eq!(
            prod.matches("pub fn hot_column_root_end").count(),
            1,
            "exclusive end must live next to encode_hot_column_key"
        );
        for (name, src) in [
            ("columns.rs", include_str!("columns.rs")),
            ("split.rs", include_str!("split.rs")),
        ] {
            let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
            assert!(
                !prod.contains("fn hot_column_root_end"),
                "{name} must call keys::hot_column_root_end, not define it"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn encode_order_is_triple_order(
            s1 in 0u64..1_000_000,
            s2 in 0u64..1_000_000,
            r1 in prop::array::uniform32(any::<u8>()),
            r2 in prop::array::uniform32(any::<u8>()),
            i1 in any::<u16>(),
            i2 in any::<u16>(),
        ) {
            let a = (Slot::new(s1), Root::from_array(r1), i1);
            let b = (Slot::new(s2), Root::from_array(r2), i2);
            let ea = encode_order_key(a.0, &a.1, a.2);
            let eb = encode_order_key(b.0, &b.1, b.2);
            let byte_ord = ea.as_slice().cmp(eb.as_slice());
            let triple_ord = order_triple(a, b);
            prop_assert_eq!(byte_ord, triple_ord);
        }
    }
}
