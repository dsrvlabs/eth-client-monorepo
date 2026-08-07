//! Bounded pending queues for arrival races (CC-22/5, spec delta 12).
//!
//! | Queue | Bound | When used |
//! |-------|-------|-----------|
//! | pending-sidecar | **256** | parent unseen or proposer lookahead unknown |
//! | pending-block | **64** | block parent unknown (local hold before chain) |
//!
//! Oldest-evicted with eviction counters. Occupancy feeds
//! `cc_p2p_queue_depth{q=pending_sidecar|pending_block}`.
//!
//! ## Redrive vs gossip report
//!
//! Entries are parked **after** gossip already reported `IGNORE` for the held
//! message. Redrive therefore performs **local-only** re-validation (seen insert,
//! sampling feed, chain forward for blocks) — it does **not** re-call the
//! gossipsub report helper for the original message id (hold already released).

use std::collections::VecDeque;
use std::sync::Arc;

/// Pending data-column sidecar queue bound (CC-22/5).
pub const PENDING_SIDECAR_BOUND: usize = 256;

/// Pending beacon-block queue bound.
pub const PENDING_BLOCK_BOUND: usize = 64;

/// Why a sidecar was parked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PendingSidecarReason {
    /// Parent block root not yet seen/valid.
    UnknownParent,
    /// Proposer lookahead not available for the slot.
    UnknownProposer,
}

/// Parked column sidecar awaiting a parent or lookahead.
///
/// Payload is `Arc` so park clones are cheap (H3 memory).
#[derive(Debug, Clone)]
pub struct PendingSidecar {
    /// SSZ payload (size-checked before park; shared via Arc).
    pub ssz: Arc<[u8]>,
    /// Topic subnet id.
    pub topic_subnet: u64,
    /// Full topic string.
    pub topic: String,
    /// Gossip message id bytes (original; already reported IGNORE).
    pub message_id: Vec<u8>,
    /// Propagation source peer bytes.
    pub peer_id: Vec<u8>,
    /// Parent block root that must become known.
    pub parent_root: [u8; 32],
    /// Slot (for proposer redrive).
    pub slot: u64,
    /// Proposer index claimed by the sidecar.
    pub proposer_index: u64,
    /// Column index.
    pub column_index: u64,
    /// Park reason.
    pub reason: PendingSidecarReason,
}

/// Parked block awaiting local parent knowledge (before chain).
#[derive(Debug, Clone)]
pub struct PendingBlock {
    /// SSZ payload (size-checked; shared via Arc).
    pub ssz: Arc<[u8]>,
    /// Full topic string.
    pub topic: String,
    /// Gossip message id bytes.
    pub message_id: Vec<u8>,
    /// Propagation source peer bytes.
    pub peer_id: Vec<u8>,
    /// Parent root.
    pub parent_root: [u8; 32],
    /// Slot.
    pub slot: u64,
    /// Proposer index.
    pub proposer_index: u64,
}

/// Oldest-first bounded queue.
#[derive(Debug, Clone)]
pub struct BoundedQueue<T> {
    bound: usize,
    inner: VecDeque<T>,
    /// Entries dropped because the queue was full.
    pub evictions: u64,
}

impl<T> BoundedQueue<T> {
    /// Create with bound (clamped to ≥ 1).
    #[must_use]
    pub fn new(bound: usize) -> Self {
        Self {
            bound: bound.max(1),
            inner: VecDeque::new(),
            evictions: 0,
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
        self.inner.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Push, evicting the oldest entry if at capacity. Returns the evicted item.
    pub fn push(&mut self, item: T) -> Option<T> {
        let mut evicted = None;
        if self.inner.len() >= self.bound {
            evicted = self.inner.pop_front();
            self.evictions = self.evictions.saturating_add(1);
        }
        self.inner.push_back(item);
        evicted
    }

    /// Pop the oldest entry.
    pub fn pop_front(&mut self) -> Option<T> {
        self.inner.pop_front()
    }

    /// Drain entries matching `pred` (in original order). Remaining stay.
    pub fn drain_if<F>(&mut self, mut pred: F) -> Vec<T>
    where
        F: FnMut(&T) -> bool,
    {
        let mut kept = VecDeque::new();
        let mut matched = Vec::new();
        while let Some(item) = self.inner.pop_front() {
            if pred(&item) {
                matched.push(item);
            } else {
                kept.push_back(item);
            }
        }
        self.inner = kept;
        matched
    }

    /// Iterate immutably.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner.iter()
    }
}

/// Both pending queues.
#[derive(Debug, Clone)]
pub struct PendingQueues {
    /// Column sidecars waiting on parent / proposer.
    pub sidecars: BoundedQueue<PendingSidecar>,
    /// Blocks waiting on local parent knowledge.
    pub blocks: BoundedQueue<PendingBlock>,
}

impl PendingQueues {
    /// Production bounds.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sidecars: BoundedQueue::new(PENDING_SIDECAR_BOUND),
            blocks: BoundedQueue::new(PENDING_BLOCK_BOUND),
        }
    }

    /// Occupancy `(sidecars, blocks)`.
    #[must_use]
    pub fn occupancy(&self) -> (usize, usize) {
        (self.sidecars.len(), self.blocks.len())
    }

    /// Park a sidecar (IGNORE path). Returns evicted entry if any.
    pub fn park_sidecar(&mut self, item: PendingSidecar) -> Option<PendingSidecar> {
        self.sidecars.push(item)
    }

    /// Park a block. Returns evicted entry if any.
    pub fn park_block(&mut self, item: PendingBlock) -> Option<PendingBlock> {
        self.blocks.push(item)
    }

    /// Redrive sidecars whose parent is now known.
    pub fn redrive_sidecars_for_parent(&mut self, parent_root: &[u8; 32]) -> Vec<PendingSidecar> {
        self.sidecars
            .drain_if(|s| s.parent_root == *parent_root)
    }

    /// Redrive sidecars parked for unknown proposer (caller re-checks lookahead).
    pub fn redrive_sidecars_unknown_proposer(&mut self) -> Vec<PendingSidecar> {
        self.sidecars
            .drain_if(|s| s.reason == PendingSidecarReason::UnknownProposer)
    }

    /// Redrive blocks whose parent is now known.
    pub fn redrive_blocks_for_parent(&mut self, parent_root: &[u8; 32]) -> Vec<PendingBlock> {
        self.blocks.drain_if(|b| b.parent_root == *parent_root)
    }

    /// Approximate resident payload bytes (for occupancy metrics).
    #[must_use]
    pub fn payload_bytes(&self) -> usize {
        let s: usize = self.sidecars.iter().map(|s| s.ssz.len()).sum();
        let b: usize = self.blocks.iter().map(|b| b.ssz.len()).sum();
        s.saturating_add(b)
    }
}

impl Default for PendingQueues {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn sidecar(n: u8) -> PendingSidecar {
        PendingSidecar {
            ssz: Arc::from([n].as_slice()),
            topic_subnet: 0,
            topic: "t".into(),
            message_id: vec![n],
            peer_id: vec![n],
            parent_root: [n; 32],
            slot: u64::from(n),
            proposer_index: 0,
            column_index: 0,
            reason: PendingSidecarReason::UnknownParent,
        }
    }

    #[test]
    fn bounds_match_spec() {
        assert_eq!(PENDING_SIDECAR_BOUND, 256);
        assert_eq!(PENDING_BLOCK_BOUND, 64);
        let q = PendingQueues::new();
        assert_eq!(q.sidecars.bound(), 256);
        assert_eq!(q.blocks.bound(), 64);
    }

    #[test]
    fn oldest_evicted_with_counter() {
        let mut q = BoundedQueue::new(2);
        assert!(q.push(1u32).is_none());
        assert!(q.push(2u32).is_none());
        let evicted = q.push(3u32);
        assert_eq!(evicted, Some(1));
        assert_eq!(q.evictions, 1);
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn redrive_by_parent() {
        let mut pending = PendingQueues::new();
        let _ = pending.park_sidecar(sidecar(1));
        let _ = pending.park_sidecar(sidecar(2));
        let ready = pending.redrive_sidecars_for_parent(&[1u8; 32]);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].ssz.as_ref(), &[1]);
        assert_eq!(pending.sidecars.len(), 1);
    }
}
