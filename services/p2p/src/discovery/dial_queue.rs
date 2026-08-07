//! Bounded discovery → dial queue — Architecture §6.3, CC-21c.
//!
//! Capacity **256**. Priority = subnet/column coverage of our deficits (higher
//! first). Discovery never dials the swarm; it only enqueues candidates that
//! the peer manager later drains.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use cc_libp2p::{Multiaddr, PeerId};

use crate::peer_manager::PeerEnrInfo;

/// Hard bound on the discovery dial queue (§6.3).
pub const DIAL_QUEUE_BOUND: usize = 256;

/// A discovered peer ready for the peer manager to dial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DialCandidate {
    /// libp2p peer id (bridged from ENR secp256k1 pubkey).
    pub peer_id: PeerId,
    /// TCP multiaddr derived from the ENR (`/ip4/…/tcp/…` or `/ip6/…/tcp/…`).
    pub addr: Multiaddr,
    /// Higher = better match for our current subnet/column deficits.
    pub priority: u32,
    /// ENR field snapshot from the same verified record (M2).
    pub enr_info: Option<PeerEnrInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct HeapEntry {
    priority: u32,
    seq: u64,
    candidate: DialCandidate,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first; tie-break older (lower seq) first for fairness.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Bounded priority queue of dial candidates (depth exported via [`DialQueue::len`]).
#[derive(Debug, Default)]
pub struct DialQueue {
    heap: BinaryHeap<HeapEntry>,
    /// Monotonic insertion counter for stable ordering.
    next_seq: u64,
}

impl DialQueue {
    /// Empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current depth (exported for metrics / tests).
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Empty?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Hard capacity.
    #[must_use]
    pub const fn capacity() -> usize {
        DIAL_QUEUE_BOUND
    }

    /// Push a candidate. Returns `false` if the queue is full (candidate dropped).
    ///
    /// If the same `peer_id` is already queued, keeps the higher-priority entry.
    pub fn push(&mut self, candidate: DialCandidate) -> bool {
        if let Some(pos) = self
            .heap
            .iter()
            .position(|e| e.candidate.peer_id == candidate.peer_id)
        {
            // Rebuild without that entry if the new one is better.
            let mut all: Vec<HeapEntry> = self.heap.drain().collect();
            let existing = all.swap_remove(pos);
            if existing.priority >= candidate.priority {
                all.push(existing);
                self.heap = all.into_iter().collect();
                return true;
            }
            // Fall through to insert the better candidate (slot freed).
            self.heap = all.into_iter().collect();
        } else if self.heap.len() >= DIAL_QUEUE_BOUND {
            return false;
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.heap.push(HeapEntry {
            priority: candidate.priority,
            seq,
            candidate,
        });
        true
    }

    /// Pop the highest-priority candidate.
    pub fn pop(&mut self) -> Option<DialCandidate> {
        self.heap.pop().map(|e| e.candidate)
    }

    /// Drain up to `n` candidates (highest priority first).
    pub fn drain(&mut self, n: usize) -> Vec<DialCandidate> {
        let mut out = Vec::with_capacity(n.min(self.heap.len()));
        for _ in 0..n {
            match self.pop() {
                Some(c) => out.push(c),
                None => break,
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_libp2p::reexport::Keypair;

    fn cand(prio: u32) -> DialCandidate {
        let peer_id = PeerId::from_public_key(&Keypair::generate_secp256k1().public());
        DialCandidate {
            peer_id,
            addr: "/ip4/127.0.0.1/tcp/9000".parse().unwrap(),
            priority: prio,
            enr_info: None,
        }
    }

    #[test]
    fn bound_at_256_and_depth_exported() {
        let mut q = DialQueue::new();
        assert_eq!(DialQueue::capacity(), 256);
        for _ in 0..DIAL_QUEUE_BOUND {
            assert!(q.push(cand(1)));
        }
        assert_eq!(q.len(), DIAL_QUEUE_BOUND);
        assert!(!q.push(cand(1)), "257th must be rejected");
        assert_eq!(q.len(), DIAL_QUEUE_BOUND);
    }

    #[test]
    fn higher_priority_pops_first() {
        let mut q = DialQueue::new();
        let low = cand(1);
        let high = cand(100);
        let low_id = low.peer_id;
        let high_id = high.peer_id;
        q.push(low);
        q.push(high);
        assert_eq!(q.pop().unwrap().peer_id, high_id);
        assert_eq!(q.pop().unwrap().peer_id, low_id);
    }
}
