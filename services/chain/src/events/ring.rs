//! Bounded event ring buffer with per-process `session_id` and monotonic sequence.

use std::collections::VecDeque;

use bytes::Bytes;
use cc_proto::chain::{Event, EventKind};

use super::EventInput;

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
}

/// Ring of recent events. Oldest entries are evicted when `cap` is exceeded.
///
/// ```text
/// EventRing { buf, cap /*1024*/, next_seq, session_id }
/// ```
#[derive(Debug)]
pub struct EventRing {
    buf: VecDeque<StoredEvent>,
    cap: usize,
    next_seq: u64,
    session_id: u64,
}

impl EventRing {
    pub fn new(cap: usize, session_id: u64) -> Self {
        Self {
            buf: VecDeque::with_capacity(cap),
            cap: cap.max(1),
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

    /// Assign the next sequence number, push, evict if over capacity; return the stored event.
    pub fn push(&mut self, input: EventInput) -> StoredEvent {
        let stored = StoredEvent {
            seq: self.next_seq,
            slot: input.slot,
            root: input.root,
            kind: input.kind,
            payload: input.payload,
        };
        self.next_seq = self.next_seq.saturating_add(1);
        self.buf.push_back(stored.clone());
        while self.buf.len() > self.cap {
            self.buf.pop_front();
        }
        stored
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

    #[test]
    fn assigns_monotonic_seq_and_evicts() {
        let mut ring = EventRing::new(3, 7);
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
}
