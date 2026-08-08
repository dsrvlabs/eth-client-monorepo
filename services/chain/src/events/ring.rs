//! Bounded event ring buffer with per-process `session_id` and monotonic sequence.
//!
//! # Bounds (CC-44a / Architecture §4.3 — closes `OQ-P1-2`)
//!
//! Two bounds, and the **byte ceiling binds first**:
//!
//! | Bound | Phase 1 | Phase 4 | Binds at (`cgc = 8`) |
//! |---|---|---|---|
//! | events, count (`chain.event_ring_events`) | 1 024 | **4 096** | 409 slots / 12.8 epochs |
//! | **bytes, hard (`chain.event_ring_bytes`)** | — | **64 MiB** | **269 slots (measured mean) / 163 slots (headroom)** |
//!
//! Arithmetic at `cgc = 8`: a slot's events are one block (mean ≈ 24 827 B) plus 8
//! sidecars at the measured mean blob count (`356 + 13.99 × 2144` ≈ 30 346 B each)
//! plus a head event ≈ **267 KB/slot**; headroom case (every slot filled at 21
//! blobs, block at p95) ≈ **410 KB/slot**. 64 MiB is therefore **33–55 minutes** of
//! resume window — the quantity that matters: a `storage` restart shorter than that
//! never reaches `CURSOR_TOO_OLD` (§4.6), and one longer is a fault we want to
//! exercise. The count cap rises to 4 096 so the byte ceiling binds first at every
//! plausible `cgc`.

use std::collections::VecDeque;

use bytes::Bytes;
use cc_proto::chain::{Event, EventKind};

use super::EventInput;

/// Fixed per-event overhead counted toward the byte ceiling (seq + slot + kind).
///
/// Payload and root lengths are added on top. Matches the accounted-bytes
/// discipline of the p2p backfill cache (CC-26a): not process RSS.
const EVENT_OVERHEAD_BYTES: usize = 8 + 8 + 4;

/// One retained event after the task assigns `seq`.
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub seq: u64,
    pub slot: u64,
    pub root: Bytes,
    pub kind: EventKind,
    pub payload: Bytes,
}

impl StoredEvent {
    pub fn to_event(&self) -> Event {
        Event {
            seq: self.seq,
            slot: self.slot,
            root: self.root.to_vec(),
            kind: self.kind as i32,
            payload: self.payload.to_vec(),
        }
    }

    /// Accounted size toward the hard byte ceiling.
    pub fn accounted_bytes(&self) -> usize {
        EVENT_OVERHEAD_BYTES
            .saturating_add(self.root.len())
            .saturating_add(self.payload.len())
    }
}

/// Ring of recent events. Oldest entries are evicted when either the **count**
/// cap or the **byte** ceiling is exceeded — the ceiling wins at production
/// defaults (see module docs / OQ-P1-2).
///
/// ```text
/// EventRing { buf, cap /*4096*/, max_bytes /*64 MiB*/, next_seq, session_id, bytes }
/// ```
#[derive(Debug)]
pub struct EventRing {
    buf: VecDeque<StoredEvent>,
    cap: usize,
    max_bytes: usize,
    /// Accounted occupancy across retained events.
    bytes: usize,
    next_seq: u64,
    session_id: u64,
}

impl EventRing {
    /// Construct with count and byte bounds. Both are clamped to at least 1.
    pub fn new(cap: usize, max_bytes: usize, session_id: u64) -> Self {
        Self {
            buf: VecDeque::with_capacity(cap.min(4096)),
            cap: cap.max(1),
            max_bytes: max_bytes.max(1),
            bytes: 0,
            next_seq: 0,
            session_id,
        }
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Hard byte ceiling (`chain.event_ring_bytes`).
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Current accounted occupancy in bytes.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn front(&self) -> Option<&StoredEvent> {
        self.buf.front()
    }

    /// Look up the ring entry with exact `seq`, if still retained.
    pub fn get(&self, seq: u64) -> Option<&StoredEvent> {
        let front = self.buf.front()?;
        if seq < front.seq {
            return None;
        }
        let idx = (seq - front.seq) as usize;
        self.buf.get(idx).filter(|e| e.seq == seq)
    }

    /// Assign the next sequence number, push, evict while over either bound;
    /// return the stored event.
    ///
    /// Callers **must** enforce [`super::MAX_EVENT_PAYLOAD_BYTES`] before push
    /// (SEC-44a-2). The events task rejects oversize inputs before calling this.
    pub fn push(&mut self, input: EventInput) -> StoredEvent {
        debug_assert!(
            input.payload.len() <= super::MAX_EVENT_PAYLOAD_BYTES,
            "payload {} exceeds MAX_EVENT_PAYLOAD_BYTES",
            input.payload.len()
        );
        let stored = StoredEvent {
            seq: self.next_seq,
            slot: input.slot,
            root: input.root,
            kind: input.kind,
            payload: input.payload,
        };
        self.next_seq = self.next_seq.saturating_add(1);
        self.bytes = self.bytes.saturating_add(stored.accounted_bytes());
        self.buf.push_back(stored.clone());
        self.evict_overflow();
        stored
    }

    /// Pop oldest until both count and byte bounds are satisfied.
    fn evict_overflow(&mut self) {
        while self.buf.len() > self.cap || self.bytes > self.max_bytes {
            let Some(old) = self.buf.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(old.accounted_bytes());
        }
    }

    /// Iterate retained events with `seq >= from`, in order.
    pub fn iter_from(&self, from: u64) -> impl Iterator<Item = &StoredEvent> {
        self.buf.iter().filter(move |e| e.seq >= from)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn input(slot: u64, root: &'static [u8]) -> EventInput {
        EventInput::block_imported(slot, Bytes::from_static(root))
    }

    fn heavy(slot: u64, payload_len: usize) -> EventInput {
        EventInput {
            slot,
            root: Bytes::from(vec![slot as u8; 32]),
            kind: EventKind::BlockImported,
            payload: Bytes::from(vec![0u8; payload_len]),
        }
    }

    #[test]
    fn assigns_monotonic_seq_and_evicts() {
        let mut ring = EventRing::new(3, usize::MAX, 7);
        assert_eq!(ring.session_id(), 7);
        for i in 0..5 {
            let s = ring.push(input(i, b"r"));
            assert_eq!(s.seq, i);
        }
        assert_eq!(ring.len(), 3);
        assert_eq!(ring.front().unwrap().seq, 2);
        assert!(ring.get(1).is_none());
        assert_eq!(ring.get(2).unwrap().seq, 2);
        assert_eq!(ring.get(4).unwrap().seq, 4);
        let from: Vec<u64> = ring.iter_from(3).map(|e| e.seq).collect();
        assert_eq!(from, vec![3, 4]);
    }

    #[test]
    fn byte_ceiling_binds_before_count_cap() {
        // Count cap huge; byte ceiling forces eviction well under it.
        let payload = 1_000;
        let mut ring = EventRing::new(4_096, 10_000, 1);
        let mut n = 0u64;
        while ring.bytes() + EVENT_OVERHEAD_BYTES + 32 + payload <= ring.max_bytes() {
            ring.push(heavy(n, payload));
            n += 1;
            assert!(
                ring.len() < 4_096,
                "byte ceiling must bind before count cap; len={}",
                ring.len()
            );
        }
        // One more must evict.
        let before_len = ring.len();
        let before_front = ring.front().map(|e| e.seq);
        ring.push(heavy(n, payload));
        assert!(ring.bytes() <= ring.max_bytes());
        assert!(ring.len() <= before_len);
        assert!(ring.len() < 4_096);
        // Front advanced (oldest evicted) or stayed if already tight.
        if let (Some(prev), Some(now)) = (before_front, ring.front().map(|e| e.seq)) {
            assert!(now >= prev);
        }
    }

    #[test]
    fn count_cap_still_evicts_when_bytes_slack() {
        let mut ring = EventRing::new(3, 1 << 30, 1);
        for i in 0..5 {
            ring.push(input(i, b"r"));
        }
        assert_eq!(ring.len(), 3);
        assert!(ring.bytes() < ring.max_bytes());
    }
}
