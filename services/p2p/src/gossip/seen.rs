//! Bounded gossip `seen` sets (§5.4 / CC-22/5).
//!
//! | Set | Key | Bound |
//! |-----|-----|-------|
//! | column | `(slot, proposer_index, column_index)` | **16 384** |
//! | block  | `(slot, proposer_index)` | **1 024** |
//!
//! Oldest-first eviction; pruned at finalization. Occupancy is exported via
//! [`SeenSets::occupancy`] for gauge producers.

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

/// Column seen-set capacity: 64 slots × 128 columns × 2 (two epochs of
/// full-width traffic with equivocation headroom).
pub const COLUMN_SEEN_BOUND: usize = 16_384;

/// Block seen-set capacity: 32 epochs × 1 block/slot with headroom.
pub const BLOCK_SEEN_BOUND: usize = 1_024;

/// Key for the column sidecar seen set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnSeenKey {
    /// Sidecar slot.
    pub slot: u64,
    /// Proposer index from the signed header.
    pub proposer_index: u64,
    /// Column index.
    pub column_index: u64,
}

/// Key for the beacon block seen set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockSeenKey {
    /// Block slot.
    pub slot: u64,
    /// Proposer index.
    pub proposer_index: u64,
}

/// Generic oldest-first bounded set.
#[derive(Debug, Clone)]
pub struct BoundedSeenSet<K: Eq + Hash + Clone> {
    bound: usize,
    order: VecDeque<K>,
    set: HashSet<K>,
    /// Cumulative oldest-first evictions (bound pressure).
    pub evictions: u64,
    /// Entries removed by finalization prune.
    pub pruned: u64,
}

impl<K: Eq + Hash + Clone> BoundedSeenSet<K> {
    /// Create with a fixed bound (clamped to ≥ 1).
    #[must_use]
    pub fn new(bound: usize) -> Self {
        Self {
            bound: bound.max(1),
            order: VecDeque::new(),
            set: HashSet::new(),
            evictions: 0,
            pruned: 0,
        }
    }

    /// Configured capacity.
    #[must_use]
    pub const fn bound(&self) -> usize {
        self.bound
    }

    /// Current occupancy.
    #[must_use]
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// True if `key` is present.
    #[must_use]
    pub fn contains(&self, key: &K) -> bool {
        self.set.contains(key)
    }

    /// Insert `key`. Returns `true` if it was **newly** inserted.
    ///
    /// When at capacity, the oldest key is evicted first.
    pub fn insert(&mut self, key: K) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        while self.set.len() >= self.bound {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
                self.evictions = self.evictions.saturating_add(1);
            } else {
                break;
            }
        }
        self.order.push_back(key.clone());
        self.set.insert(key);
        true
    }

    /// Drop every key for which `pred` returns true (finalization prune).
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: FnMut(&K) -> bool,
    {
        let before = self.set.len();
        self.order.retain(|k| pred(k));
        self.set.retain(|k| pred(k));
        let removed = before.saturating_sub(self.set.len());
        self.pruned = self.pruned.saturating_add(removed as u64);
    }
}

/// Column + block seen sets with finalization pruning.
#[derive(Debug, Clone)]
pub struct SeenSets {
    /// Column `(slot, proposer, column_index)`.
    pub columns: BoundedSeenSet<ColumnSeenKey>,
    /// Block `(slot, proposer)`.
    pub blocks: BoundedSeenSet<BlockSeenKey>,
    /// Roots of ACCEPTed blocks (parent-seen check for columns).
    ///
    /// Bounded with the block seen set's capacity (same lifecycle).
    pub block_roots: BoundedSeenSet<[u8; 32]>,
}

impl SeenSets {
    /// Default production bounds.
    #[must_use]
    pub fn new() -> Self {
        Self {
            columns: BoundedSeenSet::new(COLUMN_SEEN_BOUND),
            blocks: BoundedSeenSet::new(BLOCK_SEEN_BOUND),
            block_roots: BoundedSeenSet::new(BLOCK_SEEN_BOUND),
        }
    }

    /// Occupancy snapshot `(columns, blocks)` for gauges.
    #[must_use]
    pub fn occupancy(&self) -> (usize, usize) {
        (self.columns.len(), self.blocks.len())
    }

    /// Prune entries with `slot < finalized_slot` (and their roots).
    pub fn prune_at_finalization(&mut self, finalized_slot: u64) {
        self.columns.retain(|k| k.slot >= finalized_slot);
        self.blocks.retain(|k| k.slot >= finalized_slot);
        // Roots are not slot-keyed; keep them until block-set pressure evicts.
        // Parent-seen only needs recent roots; bound already caps memory.
        let _ = finalized_slot;
    }

    /// Record an ACCEPTed block root as a known parent candidate.
    pub fn note_block_root(&mut self, root: [u8; 32]) {
        let _ = self.block_roots.insert(root);
    }

    /// Whether `root` has been observed as a valid/accepted block.
    #[must_use]
    pub fn parent_known(&self, root: &[u8; 32]) -> bool {
        self.block_roots.contains(root)
    }
}

impl Default for SeenSets {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn column_bound_is_16384() {
        assert_eq!(COLUMN_SEEN_BOUND, 16_384);
        let s = SeenSets::new();
        assert_eq!(s.columns.bound(), COLUMN_SEEN_BOUND);
        assert_eq!(s.blocks.bound(), BLOCK_SEEN_BOUND);
    }

    #[test]
    fn oldest_first_eviction() {
        let mut set = BoundedSeenSet::new(2);
        assert!(set.insert(1u64));
        assert!(set.insert(2u64));
        assert!(set.insert(3u64));
        assert_eq!(set.len(), 2);
        assert!(!set.contains(&1));
        assert!(set.contains(&2));
        assert!(set.contains(&3));
        assert_eq!(set.evictions, 1);
    }

    #[test]
    fn duplicate_insert_is_false() {
        let mut set = BoundedSeenSet::new(4);
        assert!(set.insert(ColumnSeenKey {
            slot: 1,
            proposer_index: 0,
            column_index: 3,
        }));
        assert!(!set.insert(ColumnSeenKey {
            slot: 1,
            proposer_index: 0,
            column_index: 3,
        }));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn prune_at_finalization_drops_old_slots() {
        let mut seen = SeenSets::new();
        let _ = seen.columns.insert(ColumnSeenKey {
            slot: 10,
            proposer_index: 0,
            column_index: 0,
        });
        let _ = seen.columns.insert(ColumnSeenKey {
            slot: 100,
            proposer_index: 0,
            column_index: 1,
        });
        let _ = seen.blocks.insert(BlockSeenKey {
            slot: 10,
            proposer_index: 1,
        });
        seen.prune_at_finalization(50);
        assert!(!seen.columns.contains(&ColumnSeenKey {
            slot: 10,
            proposer_index: 0,
            column_index: 0,
        }));
        assert!(seen.columns.contains(&ColumnSeenKey {
            slot: 100,
            proposer_index: 0,
            column_index: 1,
        }));
        assert!(!seen.blocks.contains(&BlockSeenKey {
            slot: 10,
            proposer_index: 1,
        }));
    }
}
