//! Shared bounded backfill cache (§9.5 / CC-26a).
//!
//! One cache, two maps, **one byte ceiling that overrides both count bounds**.
//! Under pressure the cache **evicts oldest data and raises
//! `earliest_available_slot`** rather than allocating past the ceiling.
//!
//! # Accounted occupancy
//!
//! Production inserts derive accounted size from the SSZ encoding of the stored
//! container ([`ssz::Encode::as_ssz_bytes`]). Callers that already know the wire
//! payload length (gossip / reqresp) should use the `*_accounted` variants to
//! avoid a second encode. CC-26/2 (a) enforces the **accounted** ceiling (not
//! process RSS); realistic inserts must pass true payload sizes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use cc_types::primitives::{Root, Slot};
use cc_types::sidecar::DataColumnSidecar;
use cc_types::{Mainnet, Preset, SignedBeaconBlock, SAMPLES_PER_SLOT};
use smallvec::SmallVec;
use ssz::Encode;

use super::window::{compute_earliest_available_slot, ServeWindow};
use crate::metrics::P2pMetrics;

/// Hard byte ceiling across blocks **and** columns (**1 GiB**).
pub const CACHE_BOUND_BYTES: usize = 1 << 30;

/// Block **entry** soft bound: 2 048 (≈ 64 epochs of one block/slot).
///
/// Counts `SmallVec` lengths (multi-root slots count multiple entries).
pub const CACHE_BLOCK_COUNT_BOUND: usize = 2_048;

/// Max sampled columns stored per `(slot, root)` — [`SAMPLES_PER_SLOT`].
pub const MAX_SAMPLED: usize = SAMPLES_PER_SLOT as usize;

/// Column entry count bound: 2 048 slots × 8 sampled.
pub const CACHE_COLUMN_COUNT_BOUND: usize = CACHE_BLOCK_COUNT_BOUND * MAX_SAMPLED;

/// Max empty-slot markers retained (same order as the block entry bound).
pub const CACHE_EMPTY_SLOT_BOUND: usize = CACHE_BLOCK_COUNT_BOUND;

/// Max slots the serve-window walk inspects back from head (O(cache depth)).
pub const CACHE_WINDOW_WALK_DEPTH: u64 = CACHE_BLOCK_COUNT_BOUND as u64;

/// Column-map key: `(slot, root_bytes)`.
type ColumnKey = (Slot, [u8; 32]);

/// One block row entry: `(root, arc, accounted_bytes)`.
type BlockEntry<P> = (Root, Arc<SignedBeaconBlock<P>>, usize);

/// One sampled-column cell: `(arc, accounted_bytes)`.
type ColumnCell<P> = (Arc<DataColumnSidecar<P>>, usize);

/// Per-`(slot, root)` sampled-column array.
type ColumnRow<P> = [Option<ColumnCell<P>>; MAX_SAMPLED];

fn column_key(slot: Slot, root: &Root) -> ColumnKey {
    (slot, *root.as_array())
}

/// Outcome of an insert attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// Newly stored (may have triggered eviction first).
    Inserted {
        /// Data slots fully removed to make room (oldest-first).
        evicted_slots: u64,
    },
    /// Already present; occupancy unchanged.
    Duplicate,
    /// Rejected: even after emptying the cache the item would exceed the ceiling.
    RejectedTooLarge,
    /// Rejected: column index is outside the configured sampled set.
    RejectedUnknownColumn,
}

/// Shared backfill cache: blocks + columns under a single byte ceiling.
///
/// # Bounds
///
/// | Bound | Value | Note |
/// |---|---|---|
/// | blocks, entries | [`CACHE_BLOCK_COUNT_BOUND`] (2 048) | soft |
/// | columns, entries | [`CACHE_COLUMN_COUNT_BOUND`] (2 048 × 8) | soft |
/// | empty markers | [`CACHE_EMPTY_SLOT_BOUND`] | soft; pruned oldest-first |
/// | **bytes, hard** | [`CACHE_BOUND_BYTES`] (**1 GiB**) | **the ceiling wins** |
///
/// # `earliest_available_slot`
///
/// Owned by [`ServeWindow`]; sole `.store` is [`ServeWindow::store_recomputed`],
/// invoked after every completeness-affecting mutation (insert / head / empty /
/// eviction). Seeded to empty-window until the first recompute.
///
/// Column-map key is `(slot, root_bytes)`: `Root` is not `Ord` in `cc_types`.
#[derive(Debug)]
pub struct BackfillCache<P: Preset = Mainnet> {
    blocks: BTreeMap<Slot, SmallVec<[BlockEntry<P>; 1]>>,
    columns: BTreeMap<ColumnKey, ColumnRow<P>>,
    /// Accounted occupancy across both maps.
    bytes: usize,
    /// Soft count of block entries (sum of SmallVec lengths).
    block_entries: usize,
    /// Soft count of column entries (`Some` slots in the arrays).
    column_entries: usize,
    /// Hard byte ceiling (production: [`CACHE_BOUND_BYTES`]; tests may lower).
    bound_bytes: usize,
    /// Soft block entry bound (production: [`CACHE_BLOCK_COUNT_BOUND`]).
    block_count_bound: usize,
    /// Soft column entry bound (production: [`CACHE_COLUMN_COUNT_BOUND`]).
    column_count_bound: usize,
    /// Ordered sampled column indices → array position 0..MAX_SAMPLED.
    sampled_columns: Vec<u64>,
    /// Custodied column indices — completeness is defined over this set only.
    custodied_columns: BTreeSet<u64>,
    /// Slots explicitly marked empty (no block expected). Capped.
    empty_slots: BTreeSet<Slot>,
    /// Current head; drives the `[s, head]` range in the window recompute.
    head_slot: Option<Slot>,
    /// Serve window atomic + recompute counter.
    window: ServeWindow,
    /// Optional metrics export.
    metrics: Option<Arc<P2pMetrics>>,
}

impl<P: Preset> BackfillCache<P> {
    /// Production constructor: 1 GiB ceiling, default count bounds.
    ///
    /// `sampled_columns` must be sorted unique and `len ≤ MAX_SAMPLED`.
    /// `custodied_columns` must be a subset of the sampled set (enforced by
    /// filtering — callers should pass custody-manager output).
    #[must_use]
    pub fn new(
        anchor_slot: Slot,
        sampled_columns: impl IntoIterator<Item = u64>,
        custodied_columns: impl IntoIterator<Item = u64>,
    ) -> Self {
        Self::with_bounds(
            anchor_slot,
            sampled_columns,
            custodied_columns,
            CACHE_BOUND_BYTES,
            CACHE_BLOCK_COUNT_BOUND,
            CACHE_COLUMN_COUNT_BOUND,
        )
    }

    /// Constructor with explicit bounds (unit tests, reduced ceilings).
    #[must_use]
    pub fn with_bounds(
        anchor_slot: Slot,
        sampled_columns: impl IntoIterator<Item = u64>,
        custodied_columns: impl IntoIterator<Item = u64>,
        bound_bytes: usize,
        block_count_bound: usize,
        column_count_bound: usize,
    ) -> Self {
        let mut sampled: Vec<u64> = sampled_columns.into_iter().collect();
        sampled.sort_unstable();
        sampled.dedup();
        if sampled.len() > MAX_SAMPLED {
            sampled.truncate(MAX_SAMPLED);
        }
        let sampled_set: BTreeSet<u64> = sampled.iter().copied().collect();
        let custodied: BTreeSet<u64> = custodied_columns
            .into_iter()
            .filter(|c| sampled_set.contains(c))
            .collect();

        Self {
            blocks: BTreeMap::new(),
            columns: BTreeMap::new(),
            bytes: 0,
            block_entries: 0,
            column_entries: 0,
            bound_bytes: bound_bytes.max(1),
            block_count_bound: block_count_bound.max(1),
            column_count_bound: column_count_bound.max(1),
            sampled_columns: sampled,
            custodied_columns: custodied,
            empty_slots: BTreeSet::new(),
            head_slot: None,
            window: ServeWindow::new(anchor_slot),
            metrics: None,
        }
    }

    /// Attach metrics; seeds `cc_p2p_cache_bound_bytes` and occupancy gauges.
    pub fn set_metrics(&mut self, metrics: Arc<P2pMetrics>) {
        metrics.set_cache_bound_bytes(self.bound_bytes as i64);
        metrics.set_cache_occupancy_bytes(self.bytes as i64);
        metrics.set_earliest_available_slot(self.window.load().as_u64() as i64);
        self.metrics = Some(metrics);
    }

    /// Hard byte ceiling.
    #[must_use]
    pub const fn bound_bytes(&self) -> usize {
        self.bound_bytes
    }

    /// Current **accounted** occupancy (SSZ/wire lengths, not process RSS).
    #[must_use]
    pub const fn occupancy_bytes(&self) -> usize {
        self.bytes
    }

    /// Soft block entry count.
    #[must_use]
    pub const fn block_entries(&self) -> usize {
        self.block_entries
    }

    /// Soft column entry count.
    #[must_use]
    pub const fn column_entries(&self) -> usize {
        self.column_entries
    }

    /// Number of empty-slot markers currently held.
    #[must_use]
    pub fn empty_slot_count(&self) -> usize {
        self.empty_slots.len()
    }

    /// Borrow the serve window (atomic is read-only via [`ServeWindow::load`]).
    #[must_use]
    pub const fn window(&self) -> &ServeWindow {
        &self.window
    }

    /// Read `earliest_available_slot` from the sole atomic.
    #[must_use]
    pub fn earliest_available_slot(&self) -> Slot {
        self.window.load()
    }

    /// Ordered sampled column indices (array positions).
    #[must_use]
    pub fn sampled_columns(&self) -> &[u64] {
        &self.sampled_columns
    }

    /// Custodied column indices (window completeness set).
    #[must_use]
    pub fn custodied_columns(&self) -> &BTreeSet<u64> {
        &self.custodied_columns
    }

    /// Publish a new head slot and recompute the serve window immediately.
    pub fn set_head_slot(&mut self, head: Slot) {
        self.head_slot = Some(head);
        self.prune_empty_slots_outside_window();
        self.recompute_and_store_earliest();
    }

    /// Current head, if set.
    #[must_use]
    pub const fn head_slot(&self) -> Option<Slot> {
        self.head_slot
    }

    /// Mark `slot` as empty (no block expected) and recompute the window.
    ///
    /// Empty markers are capped at [`CACHE_EMPTY_SLOT_BOUND`]; oldest markers
    /// outside the active window are dropped first.
    pub fn mark_empty(&mut self, slot: Slot) {
        self.empty_slots.insert(slot);
        self.prune_empty_slots_outside_window();
        while self.empty_slots.len() > CACHE_EMPTY_SLOT_BOUND {
            if let Some(oldest) = self.empty_slots.iter().next().copied() {
                self.empty_slots.remove(&oldest);
            } else {
                break;
            }
        }
        self.recompute_and_store_earliest();
    }

    /// Whether the cache holds any block at `slot`.
    #[must_use]
    pub fn has_block_at(&self, slot: Slot) -> bool {
        self.blocks.get(&slot).is_some_and(|v| !v.is_empty())
    }

    /// Whether the cache holds block `root` at `slot`.
    #[must_use]
    pub fn contains_block(&self, slot: Slot, root: &Root) -> bool {
        self.blocks
            .get(&slot)
            .is_some_and(|v| v.iter().any(|(r, _, _)| r == root))
    }

    /// Whether a sampled column is present for `(slot, root)`.
    #[must_use]
    pub fn contains_column(&self, slot: Slot, root: &Root, column_index: u64) -> bool {
        let Some(pos) = self.sampled_pos(column_index) else {
            return false;
        };
        self.columns
            .get(&column_key(slot, root))
            .is_some_and(|arr| arr[pos].is_some())
    }

    /// Insert a block; accounted size is the SSZ encoding length of `block`.
    pub fn insert_block(
        &mut self,
        slot: Slot,
        root: Root,
        block: Arc<SignedBeaconBlock<P>>,
    ) -> InsertOutcome {
        let bytes = block.as_ssz_bytes().len();
        self.insert_block_accounted(slot, root, block, bytes)
    }

    /// Insert a block with an explicit accounted size (wire payload length).
    ///
    /// Prefer [`Self::insert_block`] unless the caller already measured the
    /// SSZ/wire buffer (avoids a second encode).
    pub fn insert_block_accounted(
        &mut self,
        slot: Slot,
        root: Root,
        block: Arc<SignedBeaconBlock<P>>,
        bytes: usize,
    ) -> InsertOutcome {
        if self.contains_block(slot, &root) {
            return InsertOutcome::Duplicate;
        }
        if bytes > self.bound_bytes {
            return InsertOutcome::RejectedTooLarge;
        }

        let evicted = self.make_room(bytes, /*add_block=*/ 1, /*add_column=*/ 0);
        if self.bytes.saturating_add(bytes) > self.bound_bytes
            || self.block_entries.saturating_add(1) > self.block_count_bound
        {
            return InsertOutcome::RejectedTooLarge;
        }

        self.blocks
            .entry(slot)
            .or_default()
            .push((root, block, bytes));
        self.block_entries = self.block_entries.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        self.empty_slots.remove(&slot);
        self.export_occupancy();
        self.recompute_and_store_earliest();
        InsertOutcome::Inserted {
            evicted_slots: evicted,
        }
    }

    /// Insert a column; accounted size is the SSZ encoding length of `column`.
    pub fn insert_column(
        &mut self,
        slot: Slot,
        root: Root,
        column_index: u64,
        column: Arc<DataColumnSidecar<P>>,
    ) -> InsertOutcome {
        let bytes = column.as_ssz_bytes().len();
        self.insert_column_accounted(slot, root, column_index, column, bytes)
    }

    /// Insert a column with an explicit accounted size (wire payload length).
    ///
    /// Completeness for the serve window only requires **custodied** indices;
    /// sampled-but-not-custodied columns are stored and served when present but
    /// do not gate `earliest_available_slot`.
    pub fn insert_column_accounted(
        &mut self,
        slot: Slot,
        root: Root,
        column_index: u64,
        column: Arc<DataColumnSidecar<P>>,
        bytes: usize,
    ) -> InsertOutcome {
        let Some(pos) = self.sampled_pos(column_index) else {
            return InsertOutcome::RejectedUnknownColumn;
        };
        if self.contains_column(slot, &root, column_index) {
            return InsertOutcome::Duplicate;
        }
        if bytes > self.bound_bytes {
            return InsertOutcome::RejectedTooLarge;
        }

        let evicted = self.make_room(bytes, /*add_block=*/ 0, /*add_column=*/ 1);
        if self.bytes.saturating_add(bytes) > self.bound_bytes
            || self.column_entries.saturating_add(1) > self.column_count_bound
        {
            return InsertOutcome::RejectedTooLarge;
        }

        let arr = self
            .columns
            .entry(column_key(slot, &root))
            .or_insert_with(|| std::array::from_fn(|_| None));
        debug_assert!(arr[pos].is_none());
        arr[pos] = Some((column, bytes));
        self.column_entries = self.column_entries.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        self.export_occupancy();
        self.recompute_and_store_earliest();
        InsertOutcome::Inserted {
            evicted_slots: evicted,
        }
    }

    /// Evict the oldest **data** slot (blocks and/or columns), not empty markers.
    ///
    /// Empty markers never free bytes; they are pruned separately and never
    /// compete with data for byte-pressure eviction (CC-26a F5).
    pub fn evict_oldest_slot(&mut self) -> Option<Slot> {
        let oldest = self.oldest_data_slot()?;
        self.remove_data_slot(oldest);
        self.recompute_and_store_earliest();
        self.export_occupancy();
        Some(oldest)
    }

    /// Explicit recompute (same sole store path). Prefer mutation APIs, which
    /// recompute automatically; Status/runtime may call this after bulk load.
    pub fn publish_window(&mut self) {
        self.recompute_and_store_earliest();
        self.export_occupancy();
    }

    // ── internals ───────────────────────────────────────────────────────────

    fn sampled_pos(&self, column_index: u64) -> Option<usize> {
        self.sampled_columns.iter().position(|&c| c == column_index)
    }

    /// Oldest slot that holds blocks or columns (excludes empty markers).
    fn oldest_data_slot(&self) -> Option<Slot> {
        let b = self.blocks.keys().next().copied();
        let c = self.columns.keys().next().map(|(s, _)| *s);
        [b, c].into_iter().flatten().min()
    }

    /// Evict oldest **data** slots until room exists for the pending insert.
    fn make_room(&mut self, add_bytes: usize, add_block: usize, add_column: usize) -> u64 {
        let mut evicted = 0_u64;
        while self.bytes.saturating_add(add_bytes) > self.bound_bytes
            || self.block_entries.saturating_add(add_block) > self.block_count_bound
            || self.column_entries.saturating_add(add_column) > self.column_count_bound
        {
            if self.evict_oldest_slot().is_none() {
                break;
            }
            evicted = evicted.saturating_add(1);
        }
        evicted
    }

    fn remove_data_slot(&mut self, slot: Slot) {
        if let Some(entries) = self.blocks.remove(&slot) {
            for (_, _, b) in &entries {
                self.bytes = self.bytes.saturating_sub(*b);
            }
            self.block_entries = self.block_entries.saturating_sub(entries.len());
        }
        // Drain every column row for this slot (range-style: collect keys then remove).
        let keys: Vec<ColumnKey> = self
            .columns
            .keys()
            .filter(|(s, _)| *s == slot)
            .copied()
            .collect();
        for key in keys {
            if let Some(arr) = self.columns.remove(&key) {
                for (_, b) in arr.into_iter().flatten() {
                    self.bytes = self.bytes.saturating_sub(b);
                    self.column_entries = self.column_entries.saturating_sub(1);
                }
            }
        }
        // Empty marker at the same slot is no longer meaningful once data is gone.
        self.empty_slots.remove(&slot);
    }

    /// Drop empty markers outside `[head − walk_depth, head]` (or all if no head).
    fn prune_empty_slots_outside_window(&mut self) {
        let Some(head) = self.head_slot else {
            return;
        };
        let floor = head
            .as_u64()
            .saturating_sub(CACHE_WINDOW_WALK_DEPTH)
            .max(self.window.anchor_slot().as_u64());
        let head_u = head.as_u64();
        self.empty_slots
            .retain(|s| s.as_u64() >= floor && s.as_u64() <= head_u);
    }

    /// Completeness for the serve window: empty **or** (block + all custodied cols).
    fn slot_is_complete(&self, slot: Slot) -> bool {
        if self.empty_slots.contains(&slot) {
            return true;
        }
        let Some(entries) = self.blocks.get(&slot) else {
            return false;
        };
        for (root, _, _) in entries {
            if self.has_all_custodied_columns(slot, root) {
                return true;
            }
        }
        false
    }

    fn has_all_custodied_columns(&self, slot: Slot, root: &Root) -> bool {
        if self.custodied_columns.is_empty() {
            return true;
        }
        let Some(arr) = self.columns.get(&column_key(slot, root)) else {
            return false;
        };
        for col in &self.custodied_columns {
            let Some(pos) = self.sampled_pos(*col) else {
                return false;
            };
            if arr[pos].is_none() {
                return false;
            }
        }
        true
    }

    /// Recompute over custodied completeness and write the atomic (sole store path).
    fn recompute_and_store_earliest(&mut self) {
        let anchor = self.window.anchor_slot();
        let head = self
            .head_slot
            .or_else(|| self.blocks.keys().next_back().copied())
            .or_else(|| self.columns.keys().next_back().map(|(s, _)| *s))
            .unwrap_or(anchor);

        // Cap walk to cache depth so recompute is O(window), not O(head − genesis).
        let walk_floor = Slot::new(
            head.as_u64()
                .saturating_sub(CACHE_WINDOW_WALK_DEPTH)
                .max(anchor.as_u64()),
        );

        let earliest =
            compute_earliest_available_slot(anchor, head, walk_floor, |s| self.slot_is_complete(s));

        self.window.store_recomputed(earliest);
        if let Some(m) = &self.metrics {
            m.set_earliest_available_slot(earliest.as_u64() as i64);
        }
    }

    fn export_occupancy(&self) {
        if let Some(m) = &self.metrics {
            m.set_cache_occupancy_bytes(self.bytes as i64);
            m.set_cache_bound_bytes(self.bound_bytes as i64);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_types::Mainnet;
    use prometheus_client::registry::Registry;

    /// Sampled {0,1,2,3,4,5,6,7}; custodied {0,1,2,3}.
    fn test_cache(bound: usize) -> BackfillCache<Mainnet> {
        BackfillCache::with_bounds(
            Slot::new(0),
            0u64..8,
            0u64..4,
            bound,
            CACHE_BLOCK_COUNT_BOUND,
            CACHE_COLUMN_COUNT_BOUND,
        )
    }

    fn dummy_block() -> Arc<SignedBeaconBlock<Mainnet>> {
        Arc::new(SignedBeaconBlock::<Mainnet>::default())
    }

    fn dummy_column(index: u64) -> Arc<DataColumnSidecar<Mainnet>> {
        Arc::new(DataColumnSidecar::<Mainnet> {
            index,
            ..Default::default()
        })
    }

    fn root_for(slot: u64) -> Root {
        let mut a = [0u8; 32];
        a[0..8].copy_from_slice(&slot.to_le_bytes());
        Root::from_array(a)
    }

    /// Insert a complete slot with explicit accounted sizes (ceiling tests).
    fn insert_complete(cache: &mut BackfillCache<Mainnet>, slot: u64, bytes_each: usize) {
        let s = Slot::new(slot);
        let r = root_for(slot);
        let o = cache.insert_block_accounted(s, r, dummy_block(), bytes_each);
        assert!(
            matches!(o, InsertOutcome::Inserted { .. }),
            "block slot {slot}: {o:?}"
        );
        for col in 0u64..4 {
            let o =
                cache.insert_column_accounted(s, r, col, dummy_column(col), bytes_each);
            assert!(
                matches!(o, InsertOutcome::Inserted { .. }),
                "col {col} slot {slot}: {o:?}"
            );
        }
    }

    #[test]
    fn byte_ceiling_binds_before_count_and_evicts_oldest() {
        let mut cache = test_cache(1_000);
        cache.set_head_slot(Slot::new(50));

        for slot in 1u64..=20 {
            insert_complete(&mut cache, slot, 100);
        }

        assert!(
            cache.occupancy_bytes() <= cache.bound_bytes(),
            "occupancy {} exceeded bound {}",
            cache.occupancy_bytes(),
            cache.bound_bytes()
        );
        assert!(
            cache.block_entries() < CACHE_BLOCK_COUNT_BOUND,
            "count bound must not be the binder"
        );
        assert!(
            !cache.has_block_at(Slot::new(1)),
            "slot 1 must have been evicted under byte pressure"
        );
        assert!(cache.has_block_at(Slot::new(20)));

        let ratio = cache.occupancy_bytes() as f64 / cache.bound_bytes() as f64;
        assert!(
            ratio > 0.5 && ratio <= 1.0,
            "occupancy/bound ratio {ratio} should sit at or under the ceiling"
        );
    }

    #[test]
    fn one_gib_ceiling_constant_and_no_alloc_past_ceiling() {
        assert_eq!(CACHE_BOUND_BYTES, 1 << 30);
        // Accounted occupancy proxy for CC-26/2 (a) — not process RSS.
        let mut cache = test_cache(CACHE_BOUND_BYTES);
        cache.set_head_slot(Slot::new(10_000));

        let chunk = 50 * 1024 * 1024;
        for slot in 1u64..=10 {
            insert_complete(&mut cache, slot, chunk);
            assert!(
                cache.occupancy_bytes() <= CACHE_BOUND_BYTES,
                "slot {slot}: occupancy {} > 1 GiB",
                cache.occupancy_bytes()
            );
        }
        assert!(cache.occupancy_bytes() <= CACHE_BOUND_BYTES);
        assert!(cache.block_entries() < CACHE_BLOCK_COUNT_BOUND);
        assert!(cache.column_entries() < CACHE_COLUMN_COUNT_BOUND);

        let huge = cache.insert_block_accounted(
            Slot::new(99_999),
            root_for(99_999),
            dummy_block(),
            CACHE_BOUND_BYTES + 1,
        );
        assert_eq!(huge, InsertOutcome::RejectedTooLarge);
    }

    #[test]
    fn ssz_insert_derives_accounted_size() {
        let mut cache = test_cache(50_000);
        cache.set_head_slot(Slot::new(1));
        let block = dummy_block();
        let expected = block.as_ssz_bytes().len();
        let o = cache.insert_block(Slot::new(1), root_for(1), block);
        assert!(matches!(o, InsertOutcome::Inserted { .. }));
        assert_eq!(cache.occupancy_bytes(), expected);

        let col = dummy_column(0);
        let col_len = col.as_ssz_bytes().len();
        let o = cache.insert_column(Slot::new(1), root_for(1), 0, col);
        assert!(matches!(o, InsertOutcome::Inserted { .. }));
        assert_eq!(cache.occupancy_bytes(), expected + col_len);
    }

    #[test]
    fn metrics_ratio_at_ceiling() {
        let mut reg = Registry::default();
        let metrics = Arc::new(P2pMetrics::register(&mut reg));
        let mut cache = test_cache(500);
        cache.set_metrics(Arc::clone(&metrics));
        cache.set_head_slot(Slot::new(3));
        insert_complete(&mut cache, 1, 100);
        assert_eq!(cache.occupancy_bytes(), 500);
        assert_eq!(metrics.cache_occupancy_bytes(), 500);
        assert_eq!(metrics.cache_bound_bytes(), 500);
        let ratio = metrics.cache_occupancy_bytes() as f64 / metrics.cache_bound_bytes() as f64;
        assert!((ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn earliest_moves_forward_on_eviction() {
        let mut cache = test_cache(10_000);
        cache.set_head_slot(Slot::new(10));
        for slot in 1u64..=10 {
            insert_complete(&mut cache, slot, 10);
        }
        // Inserts recompute automatically — no publish_window required.
        assert_eq!(cache.earliest_available_slot(), Slot::new(1));

        let before = cache.window().recompute_invocations();
        let evicted = cache.evict_oldest_slot();
        assert_eq!(evicted, Some(Slot::new(1)));
        assert_eq!(cache.earliest_available_slot(), Slot::new(2));
        assert_eq!(cache.window().recompute_invocations(), before + 1);
    }

    #[test]
    fn insert_without_eviction_is_honest_not_anchor_seed() {
        // F2: after fill, atomic must not still advertise empty/anchor free-ride.
        let mut cache = test_cache(50_000);
        assert!(cache.window().is_empty_window_seed());
        cache.set_head_slot(Slot::new(5));
        // Head set with empty cache → incomplete head → earliest = 6.
        assert_eq!(cache.earliest_available_slot(), Slot::new(6));
        for slot in 1u64..=5 {
            insert_complete(&mut cache, slot, 10);
        }
        assert_eq!(cache.earliest_available_slot(), Slot::new(1));
        assert!(!cache.window().is_empty_window_seed());
    }

    #[test]
    fn window_over_custodied_not_sampled() {
        let mut cache = test_cache(50_000);
        cache.set_head_slot(Slot::new(5));
        for slot in 1u64..=5 {
            let s = Slot::new(slot);
            let r = root_for(slot);
            assert!(matches!(
                cache.insert_block_accounted(s, r, dummy_block(), 10),
                InsertOutcome::Inserted { .. }
            ));
            for col in 0u64..4 {
                assert!(matches!(
                    cache.insert_column_accounted(s, r, col, dummy_column(col), 10),
                    InsertOutcome::Inserted { .. }
                ));
            }
        }
        assert_eq!(
            cache.earliest_available_slot(),
            Slot::new(1),
            "missing sampled-only columns must not shrink the window"
        );

        let mut cache2 = test_cache(50_000);
        cache2.set_head_slot(Slot::new(3));
        for slot in 1u64..=2 {
            insert_complete(&mut cache2, slot, 10);
        }
        let s = Slot::new(3);
        let r = root_for(3);
        cache2.insert_block_accounted(s, r, dummy_block(), 10);
        for col in 1u64..4 {
            cache2.insert_column_accounted(s, r, col, dummy_column(col), 10);
        }
        // head=3 incomplete → earliest = 4
        assert_eq!(cache2.earliest_available_slot(), Slot::new(4));

        let mut cache3 = test_cache(50_000);
        cache3.set_head_slot(Slot::new(2));
        for slot in 1u64..=2 {
            let s = Slot::new(slot);
            let r = root_for(slot);
            cache3.insert_block_accounted(s, r, dummy_block(), 10);
            for col in 0u64..4 {
                cache3.insert_column_accounted(s, r, col, dummy_column(col), 10);
            }
        }
        assert_eq!(cache3.earliest_available_slot(), Slot::new(1));
    }

    #[test]
    fn recompute_on_every_eviction_counter() {
        let mut cache = test_cache(1_000_000);
        cache.set_head_slot(Slot::new(600));
        for slot in 1u64..=520 {
            insert_complete(&mut cache, slot, 10);
        }
        let start = cache.window().recompute_invocations();
        for i in 0..500 {
            let evicted = cache.evict_oldest_slot().expect("expected eviction");
            assert_eq!(evicted, Slot::new(1 + i));
        }
        assert_eq!(
            cache.window().recompute_invocations(),
            start + 500,
            "recompute must run exactly once per eviction"
        );
    }

    #[test]
    fn recompute_is_o_cache_depth_not_total_slots() {
        let mut cache = test_cache(100_000);
        let head = 1_000_000u64;
        cache.set_head_slot(Slot::new(head));
        for slot in (head - 63)..=head {
            insert_complete(&mut cache, slot, 10);
        }
        let before = cache.window().recompute_invocations();
        cache.evict_oldest_slot();
        assert_eq!(cache.window().recompute_invocations(), before + 1);
        let earliest = cache.earliest_available_slot().as_u64();
        assert!(earliest >= head - 63);
        assert!(earliest <= head);
    }

    #[test]
    fn empty_slots_capped_and_not_byte_eviction_victims() {
        let mut cache = test_cache(1_000);
        cache.set_head_slot(Slot::new(100));
        // Flood empty markers beyond the cap.
        for s in 0u64..5_000 {
            cache.mark_empty(Slot::new(s));
        }
        assert!(cache.empty_slot_count() <= CACHE_EMPTY_SLOT_BOUND);
        // Insert real data under byte pressure — empties must not block relief.
        for slot in 200u64..220 {
            insert_complete(&mut cache, slot, 100);
        }
        assert!(cache.occupancy_bytes() <= cache.bound_bytes());
        assert!(cache.has_block_at(Slot::new(219)));
    }

    #[test]
    fn metrics_earliest_matches_atomic() {
        let mut reg = Registry::default();
        let metrics = Arc::new(P2pMetrics::register(&mut reg));
        let mut cache = test_cache(50_000);
        cache.set_metrics(Arc::clone(&metrics));
        cache.set_head_slot(Slot::new(5));
        for slot in 1u64..=5 {
            insert_complete(&mut cache, slot, 10);
        }
        assert_eq!(
            metrics.earliest_available_slot() as u64,
            cache.earliest_available_slot().as_u64()
        );
        cache.evict_oldest_slot();
        assert_eq!(
            metrics.earliest_available_slot() as u64,
            cache.earliest_available_slot().as_u64()
        );
    }

    #[test]
    fn unknown_column_rejected() {
        let mut cache = test_cache(1_000);
        let o = cache.insert_column_accounted(
            Slot::new(1),
            root_for(1),
            99,
            dummy_column(99),
            10,
        );
        assert_eq!(o, InsertOutcome::RejectedUnknownColumn);
    }

    #[test]
    fn oldest_first_eviction_order() {
        let mut cache = test_cache(150);
        cache.set_head_slot(Slot::new(10));
        for slot in 1u64..=6 {
            insert_complete(&mut cache, slot, 10);
        }
        let e = cache.evict_oldest_slot().expect("something to evict");
        assert!(!cache.has_block_at(e));
        if let Some(min) = cache.blocks.keys().next() {
            assert!(min.as_u64() > e.as_u64() || cache.blocks.len() == 1);
        }
    }
}
