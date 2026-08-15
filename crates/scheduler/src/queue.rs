//! Overflow policy lives in the type ([ARCH] §3.1 / [q1] §1.4).

use std::collections::VecDeque;
use std::fmt;

/// Overflow: drop the NEW item and count it.
///
/// For work where order is correctness (blocks import sequentially) or where
/// eviction is an attack (an adversary must not flush slashings with junk).
pub struct FifoQueue<T> {
    queue: VecDeque<T>,
    max_length: usize,
    dropped: u64,
}

impl<T> FifoQueue<T> {
    /// `max_length` must be ≥ 1. A zero-length queue would silently refuse
    /// every item of that work type ([q1] §1.5).
    #[must_use]
    pub fn new(max_length: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(max_length),
            max_length: max_length.max(1),
            dropped: 0,
        }
    }

    /// Push at the back. Full: return the new item and increment [`Self::dropped`].
    pub fn push(&mut self, item: T) -> Option<T> {
        if self.queue.len() >= self.max_length {
            self.dropped = self.dropped.saturating_add(1);
            return Some(item);
        }
        self.queue.push_back(item);
        None
    }

    /// Pop the oldest item.
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.max_length
    }

    #[must_use]
    pub fn max_length(&self) -> usize {
        self.max_length
    }

    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Raise or lower the bound. Existing items are kept; overflow applies on
    /// the next [`Self::push`].
    pub fn set_max_length(&mut self, max_length: usize) {
        self.max_length = max_length.max(1);
    }
}

impl<T> fmt::Debug for FifoQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FifoQueue")
            .field("len", &self.queue.len())
            .field("max_length", &self.max_length)
            .field("dropped", &self.dropped)
            .finish()
    }
}

/// Overflow: evict the OLDEST and keep the new.
///
/// For work where later is strictly better information (a fresher attestation
/// carries more).
pub struct LifoQueue<T> {
    queue: VecDeque<T>,
    max_length: usize,
    evicted: u64,
}

impl<T> LifoQueue<T> {
    /// `max_length` must be ≥ 1. See [`FifoQueue::new`].
    #[must_use]
    pub fn new(max_length: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(max_length),
            max_length: max_length.max(1),
            evicted: 0,
        }
    }

    /// Push at the front. Full: evict the oldest (back) and return it.
    pub fn push(&mut self, item: T) -> Option<T> {
        let evicted = if self.queue.len() >= self.max_length {
            self.evicted = self.evicted.saturating_add(1);
            self.queue.pop_back()
        } else {
            None
        };
        self.queue.push_front(item);
        evicted
    }

    /// Pop the newest item.
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.max_length
    }

    #[must_use]
    pub fn max_length(&self) -> usize {
        self.max_length
    }

    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    /// Raise or lower the bound. Existing items are kept; overflow applies on
    /// the next [`Self::push`].
    pub fn set_max_length(&mut self, max_length: usize) {
        self.max_length = max_length.max(1);
    }
}

impl<T> fmt::Debug for LifoQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LifoQueue")
            .field("len", &self.queue.len())
            .field("max_length", &self.max_length)
            .field("evicted", &self.evicted)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn fifo_drops_new_keeps_oldest() {
        let mut q = FifoQueue::new(2);
        assert!(q.push(1).is_none());
        assert!(q.push(2).is_none());
        assert_eq!(q.push(3), Some(3));
        assert_eq!(q.dropped(), 1);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert!(q.pop().is_none());
    }

    #[test]
    fn lifo_evicts_oldest_keeps_new() {
        let mut q = LifoQueue::new(2);
        assert!(q.push(1).is_none());
        assert!(q.push(2).is_none());
        assert_eq!(q.push(3), Some(1));
        assert_eq!(q.evicted(), 1);
        assert_eq!(q.pop(), Some(3));
        assert_eq!(q.pop(), Some(2));
        assert!(q.pop().is_none());
    }

    #[test]
    fn zero_max_length_becomes_one() {
        let mut fifo = FifoQueue::new(0);
        assert_eq!(fifo.max_length(), 1);
        assert!(fifo.push(1).is_none());
        assert_eq!(fifo.push(2), Some(2));

        let mut lifo = LifoQueue::new(0);
        assert_eq!(lifo.max_length(), 1);
        assert!(lifo.push(1).is_none());
        assert_eq!(lifo.push(2), Some(1));
    }
}
