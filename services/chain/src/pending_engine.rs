//! Engine-unavailable requeue (CC-36a / Architecture §4.9, ADR P3-05).
//!
//! ```text
//! on_block → Deferred(ExecutionEngineUnavailable)  →  park in pending_engine
//! Offline → Online (engine state watch)            →  re-drive pending_engine
//! ```
//!
//! **Separate map** from the DA requeue (closes OQ-P3-10):
//! - this map: **64** entries / **8**-slot timeout
//! - DA map:   **64** entries / **4**-slot timeout (untouched here)
//!
//! An EL restart is a container restart, not a network fetch — the longer
//! timeout covers a clean `docker compose restart el` (~96 s). Separate maps
//! keep CC-38 /7's timeout-ordering test a single-variable check.
//!
//! Bounds: count + slot-bounded timeout + drop counter (R-15).

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use cc_types::primitives::Root;

// ── Config defaults (Architecture §4.9) ─────────────────────────────────────

/// Concurrent blocks waiting on the execution engine (Architecture §4.9).
pub const PENDING_ENGINE_BOUND: usize = 64;

/// Slots a deferred block may wait for the engine to return (8 slots = 96 s).
pub const DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS: u64 = 8;

// ── Pending entry ───────────────────────────────────────────────────────────

/// A block parked because `on_block` returned `Deferred(ExecutionEngineUnavailable)`.
#[derive(Debug, Clone)]
pub struct PendingEngineEntry {
    /// Beacon block root (dedup key).
    pub root: Root,
    /// Raw SSZ of the signed beacon block (re-import without re-gossip).
    pub ssz: Bytes,
    /// Fork discriminant from the original import request.
    pub fork: u32,
    /// Import source (`Source` proto enum as i32).
    pub source: i32,
    /// Slot of the block (timeout / metrics).
    pub slot: u64,
    /// Store slot when the block was parked (timeout base).
    pub parked_at_slot: u64,
}

// ── Pending map ─────────────────────────────────────────────────────────────

/// Bounded map of blocks waiting on the execution engine (Architecture §4.9).
///
/// Capacity [`PENDING_ENGINE_BOUND`]; oldest-evicted on overflow. Per-entry
/// slot-bounded timeout via [`Self::expire`].
#[derive(Debug, Default)]
pub struct PendingEngine {
    entries: HashMap<Root, PendingEngineEntry>,
    /// Insertion order (front = oldest).
    order: VecDeque<Root>,
    bound: usize,
    /// Cumulative drops (timeout + capacity eviction); tests / metrics.
    dropped: u64,
}

impl PendingEngine {
    /// Empty map with the default bound of 64.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(PENDING_ENGINE_BOUND)
    }

    /// Empty map with a custom bound (tests).
    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bound: bound.max(1),
            dropped: 0,
        }
    }

    /// Number of resident pending blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Cumulative drops (timeout + capacity eviction).
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.dropped
    }

    /// Park a deferred block. Returns the evicted entry when at capacity.
    pub fn insert(&mut self, entry: PendingEngineEntry) -> Option<PendingEngineEntry> {
        let root = entry.root;
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(root) {
            // Refresh: replace payload, keep position.
            e.insert(entry);
            return None;
        }
        let mut evicted = None;
        while self.entries.len() >= self.bound {
            if let Some(old_root) = self.order.pop_front() {
                if let Some(old) = self.entries.remove(&old_root) {
                    self.dropped = self.dropped.saturating_add(1);
                    evicted = Some(old);
                }
            } else {
                break;
            }
        }
        self.entries.insert(root, entry);
        self.order.push_back(root);
        evicted
    }

    /// Remove and return a parked block (re-drive when the engine returns).
    pub fn take(&mut self, root: &Root) -> Option<PendingEngineEntry> {
        let entry = self.entries.remove(root)?;
        self.order.retain(|r| r != root);
        Some(entry)
    }

    /// Peek without removing.
    #[must_use]
    pub fn get(&self, root: &Root) -> Option<&PendingEngineEntry> {
        self.entries.get(root)
    }

    /// Whether `root` is parked.
    #[must_use]
    pub fn contains(&self, root: &Root) -> bool {
        self.entries.contains_key(root)
    }

    /// Drain all entries oldest-first (re-drive on Offline → Online).
    pub fn drain_oldest_first(&mut self) -> Vec<PendingEngineEntry> {
        let mut out = Vec::with_capacity(self.entries.len());
        while let Some(root) = self.order.pop_front() {
            if let Some(e) = self.entries.remove(&root) {
                out.push(e);
            }
        }
        out
    }

    /// Drop entries whose age exceeds `timeout_slots` relative to `current_slot`.
    ///
    /// Returns the dropped entries (caller increments metrics).
    pub fn expire(&mut self, current_slot: u64, timeout_slots: u64) -> Vec<PendingEngineEntry> {
        let timeout = timeout_slots.max(1);
        let mut dropped = Vec::new();
        let mut keep = VecDeque::new();
        while let Some(root) = self.order.pop_front() {
            let Some(entry) = self.entries.get(&root) else {
                continue;
            };
            let age = current_slot.saturating_sub(entry.parked_at_slot);
            if age > timeout {
                if let Some(e) = self.entries.remove(&root) {
                    self.dropped = self.dropped.saturating_add(1);
                    dropped.push(e);
                }
            } else {
                keep.push_back(root);
            }
        }
        self.order = keep;
        dropped
    }

    /// Iterate roots in oldest-first order (tests).
    pub fn roots_oldest_first(&self) -> impl Iterator<Item = &Root> {
        self.order.iter()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn root_byte(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn entry(b: u8, parked_at: u64) -> PendingEngineEntry {
        PendingEngineEntry {
            root: root_byte(b),
            ssz: Bytes::from(vec![b]),
            fork: 0,
            source: 0,
            slot: parked_at,
            parked_at_slot: parked_at,
        }
    }

    /// Bound 64 / timeout 8; 65th entry evicts oldest and increments drop counter.
    #[test]
    fn pending_engine_bounds() {
        assert_eq!(PENDING_ENGINE_BOUND, 64);
        assert_eq!(DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS, 8);

        let mut pending = PendingEngine::new();
        // Park 65 blocks → 64 resident, 1 capacity-evicted (oldest).
        for i in 0u8..65 {
            let _ = pending.insert(entry(i, u64::from(i)));
        }
        assert_eq!(pending.len(), 64);
        assert_eq!(pending.dropped_total(), 1);
        assert!(!pending.contains(&root_byte(0)), "oldest must be evicted");
        assert!(pending.contains(&root_byte(1)));
        assert!(pending.contains(&root_byte(64)));

        // Occupancy tracks len (metrics mirror this).
        let occupancy = pending.len() as u64;
        assert_eq!(occupancy, 64);

        // 8-slot timeout: parked_at=10, current=19 → age 9 > 8 → drop.
        let mut timed = PendingEngine::new();
        timed.insert(entry(1, 10));
        timed.insert(entry(2, 12));
        let dropped = timed.expire(19, DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS);
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].root, root_byte(1));
        assert_eq!(timed.len(), 1);
        assert!(timed.contains(&root_byte(2)));
        assert_eq!(timed.dropped_total(), 1);

        // 65th-from-empty path already covered; re-assert counter name semantics
        // (cc_chain_pending_engine_dropped_total observed by metrics layer).
        assert!(pending.dropped_total() >= 1);
    }

    #[test]
    fn pending_engine_take_redrives() {
        let mut pending = PendingEngine::new();
        pending.insert(entry(7, 0));
        assert!(pending.take(&root_byte(7)).is_some());
        assert!(pending.is_empty());
        assert!(pending.take(&root_byte(7)).is_none());
    }

    #[test]
    fn pending_engine_drain_oldest_first() {
        let mut pending = PendingEngine::new();
        pending.insert(entry(1, 0));
        pending.insert(entry(2, 1));
        pending.insert(entry(3, 2));
        let drained = pending.drain_oldest_first();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].root, root_byte(1));
        assert_eq!(drained[2].root, root_byte(3));
        assert!(pending.is_empty());
    }

    /// CC-36a /5: engine unavailable is a success-path deferral (not invalidate),
    /// parked in `pending_engine`, and re-driven when the engine returns.
    ///
    /// Store-unmutated on_block mapping is asserted in
    /// `cc-fork-choice::on_block::tests::engine_transport_defers_not_invalidates`.
    #[test]
    fn engine_error_defers_not_invalidates() {
        use cc_fork_choice::{BlockImport, DeferralReason};
        use cc_state_transition::GossipClass;
        use prometheus_client::registry::Registry;

        // Success-path deferral — Ok(Deferred), never Err; Ignore gossip class.
        let deferred = BlockImport::Deferred(DeferralReason::ExecutionEngineUnavailable);
        assert!(matches!(
            deferred,
            BlockImport::Deferred(DeferralReason::ExecutionEngineUnavailable)
        ));
        assert_eq!(deferred.gossip_class(), Some(GossipClass::Ignore));
        assert_ne!(deferred.gossip_class(), Some(GossipClass::Reject));

        // Park → drain on Offline→Online (production redrive path).
        let mut pending = PendingEngine::new();
        pending.insert(entry(0xEE, 0));
        pending.insert(entry(0xEF, 1));
        assert_eq!(pending.len(), 2);
        let drained = pending.drain_oldest_first();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].root, root_byte(0xEE));
        assert!(pending.is_empty(), "Online edge must empty the map");

        // Metric label `deferred_engine` is populated by import (seeded at register).
        let mut registry = Registry::default();
        let metrics = crate::metrics::ChainMetrics::register(&mut registry);
        metrics.inc_import_result(crate::metrics::ImportResult::DeferredEngine);
        assert_eq!(
            metrics.import_result_count(crate::metrics::ImportResult::DeferredEngine),
            1
        );
        assert_eq!(
            crate::metrics::ImportResult::DeferredEngine.as_str(),
            "deferred_engine"
        );

        // Bounds differ from the DA map (64 entries / 8 slots vs 64 / 4).
        assert_eq!(PENDING_ENGINE_BOUND, 64);
        assert_eq!(DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS, 8);
        assert_ne!(
            DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS,
            crate::da::DEFAULT_DA_PENDING_TIMEOUT_SLOTS
        );
    }

    /// 8-slot timeout ages relative to `current_slot` (SlotTick must advance time).
    #[test]
    fn pending_engine_eight_slot_expiry_fires() {
        let mut pending = PendingEngine::new();
        pending.insert(entry(1, 100));
        // age = 8 → keep (strict > timeout).
        assert!(pending.expire(108, 8).is_empty());
        assert_eq!(pending.len(), 1);
        // age = 9 → drop.
        let dropped = pending.expire(109, 8);
        assert_eq!(dropped.len(), 1);
        assert!(pending.is_empty());
        assert_eq!(pending.dropped_total(), 1);
    }
}
