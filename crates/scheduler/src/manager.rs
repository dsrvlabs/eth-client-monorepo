//! Manager: lane registry + first-match-wins select. No worker threads.

use std::fmt;

use crate::chain::{ChainLane, LOOP_B_LANES};
use crate::config::{
    DEFAULT_MAX_WORKERS, INBOUND_POLL_CHAIN, InboundClass, LaneKey, LaneSpec, MIN_QUEUE_LEN,
    QueueKind, QueueSizes,
};
use crate::queue::{FifoQueue, LifoQueue};

/// Construction failure. Programming error in the caller's spec slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    EmptyChain,
    DuplicateLane,
    UnknownLane,
    ZeroMaxWorkers,
    ZeroDepth,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyChain => f.write_str("selection chain must contain at least one lane"),
            Self::DuplicateLane => f.write_str("selection chain contains a duplicate lane id"),
            Self::UnknownLane => f.write_str("lane is not in the selection chain"),
            Self::ZeroMaxWorkers => f.write_str("max_workers must be at least 1"),
            Self::ZeroDepth => f.write_str("queue depth must be at least 1"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Caps. `max_workers` is a count, not a pool — this crate starts no threads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagerConfig {
    pub max_workers: usize,
    pub deferred_depth: usize,
}

impl ManagerConfig {
    #[must_use]
    pub const fn loop_b() -> Self {
        Self {
            max_workers: DEFAULT_MAX_WORKERS,
            deferred_depth: MIN_QUEUE_LEN,
        }
    }
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self::loop_b()
    }
}

/// Outcome of [`Manager::push`]. The item is returned when it was not queued.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum Enqueue<T> {
    Accepted,
    /// FIFO overflow: the new item was dropped.
    DroppedNew(T),
    /// LIFO overflow: the oldest item was evicted.
    EvictedOldest(T),
    /// `never_shed` lane is full; the item was not taken.
    WouldShed(T),
    /// `lane` is not in this manager's selection chain.
    UnknownLane(T),
}

impl<T> Enqueue<T> {
    #[must_use]
    pub const fn accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }
}

/// Where [`Manager::select`] took the item from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkSource<K> {
    Deferred,
    Lane(K),
}

/// One unit of selected work.
#[derive(Debug)]
pub struct Selected<K, T> {
    pub source: WorkSource<K>,
    pub item: T,
}

/// Per-lane counters for metric derivation (labels from [`LaneKey::as_str`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneSnapshot<K> {
    pub id: K,
    pub kind: QueueKind,
    pub depth: usize,
    pub len: usize,
    pub dropped: u64,
    pub evicted: u64,
    pub never_shed: bool,
}

#[derive(Debug)]
enum TypedQueue<T> {
    Fifo(FifoQueue<T>),
    Lifo(LifoQueue<T>),
}

impl<T> TypedQueue<T> {
    fn new(kind: QueueKind, depth: usize) -> Self {
        match kind {
            QueueKind::Fifo => Self::Fifo(FifoQueue::new(depth)),
            QueueKind::Lifo => Self::Lifo(LifoQueue::new(depth)),
        }
    }

    fn push(&mut self, item: T) -> Enqueue<T> {
        match self {
            Self::Fifo(q) => match q.push(item) {
                None => Enqueue::Accepted,
                Some(dropped) => Enqueue::DroppedNew(dropped),
            },
            Self::Lifo(q) => match q.push(item) {
                None => Enqueue::Accepted,
                Some(evicted) => Enqueue::EvictedOldest(evicted),
            },
        }
    }

    fn pop(&mut self) -> Option<T> {
        match self {
            Self::Fifo(q) => q.pop(),
            Self::Lifo(q) => q.pop(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Fifo(q) => q.len(),
            Self::Lifo(q) => q.len(),
        }
    }

    fn is_full(&self) -> bool {
        match self {
            Self::Fifo(q) => q.is_full(),
            Self::Lifo(q) => q.is_full(),
        }
    }

    fn dropped(&self) -> u64 {
        match self {
            Self::Fifo(q) => q.dropped(),
            Self::Lifo(_) => 0,
        }
    }

    fn evicted(&self) -> u64 {
        match self {
            Self::Fifo(_) => 0,
            Self::Lifo(q) => q.evicted(),
        }
    }

    fn set_max_length(&mut self, depth: usize) {
        match self {
            Self::Fifo(q) => q.set_max_length(depth),
            Self::Lifo(q) => q.set_max_length(depth),
        }
    }
}

struct Lane<K, T> {
    spec: LaneSpec<K>,
    depth: usize,
    queue: TypedQueue<T>,
}

/// Owns the typed queues. [`Manager::select`] walks [`INBOUND_POLL_CHAIN`] then
/// the lane slice; it never spawns a thread.
pub struct Manager<K, T> {
    lanes: Vec<Lane<K, T>>,
    deferred: FifoQueue<T>,
    max_workers: usize,
    busy_workers: usize,
}

impl<K: LaneKey, T> Manager<K, T> {
    /// `specs` order is the New-inbound selection chain.
    pub fn new(
        specs: &[LaneSpec<K>],
        sizes: QueueSizes,
        config: ManagerConfig,
    ) -> Result<Self, ConfigError> {
        if specs.is_empty() {
            return Err(ConfigError::EmptyChain);
        }
        if config.max_workers == 0 {
            return Err(ConfigError::ZeroMaxWorkers);
        }
        if config.deferred_depth == 0 {
            return Err(ConfigError::ZeroDepth);
        }
        let mut lanes = Vec::with_capacity(specs.len());
        for (i, spec) in specs.iter().enumerate() {
            if specs[..i].iter().any(|s| s.id == spec.id) {
                return Err(ConfigError::DuplicateLane);
            }
            let depth = sizes.resolve(spec.depth);
            if depth == 0 {
                return Err(ConfigError::ZeroDepth);
            }
            lanes.push(Lane {
                spec: *spec,
                depth,
                queue: TypedQueue::new(spec.queue, depth),
            });
        }
        Ok(Self {
            lanes,
            deferred: FifoQueue::new(config.deferred_depth),
            max_workers: config.max_workers,
            busy_workers: 0,
        })
    }

    pub fn push(&mut self, lane: K, item: T) -> Enqueue<T> {
        let Some(slot) = self.lanes.iter_mut().find(|l| l.spec.id == lane) else {
            return Enqueue::UnknownLane(item);
        };
        if slot.spec.never_shed && slot.queue.is_full() {
            return Enqueue::WouldShed(item);
        }
        slot.queue.push(item)
    }

    pub fn push_deferred(&mut self, item: T) -> Enqueue<T> {
        match self.deferred.push(item) {
            None => Enqueue::Accepted,
            Some(dropped) => Enqueue::DroppedNew(dropped),
        }
    }

    /// First-match-wins over [`INBOUND_POLL_CHAIN`] then the lane slice.
    pub fn select(&mut self) -> Option<Selected<K, T>> {
        for class in INBOUND_POLL_CHAIN {
            match class {
                InboundClass::WorkerIdle => {
                    if self.busy_workers >= self.max_workers {
                        return None;
                    }
                }
                InboundClass::Deferred => {
                    if let Some(item) = self.deferred.pop() {
                        self.busy_workers = self.busy_workers.saturating_add(1);
                        return Some(Selected {
                            source: WorkSource::Deferred,
                            item,
                        });
                    }
                }
                InboundClass::New => {
                    for lane in &mut self.lanes {
                        if let Some(item) = lane.queue.pop() {
                            self.busy_workers = self.busy_workers.saturating_add(1);
                            return Some(Selected {
                                source: WorkSource::Lane(lane.spec.id),
                                item,
                            });
                        }
                    }
                }
            }
        }
        None
    }

    pub fn note_idle(&mut self) {
        self.busy_workers = self.busy_workers.saturating_sub(1);
    }

    #[must_use]
    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    #[must_use]
    pub fn busy_workers(&self) -> usize {
        self.busy_workers
    }

    #[must_use]
    pub fn spec(&self, id: K) -> Option<&LaneSpec<K>> {
        self.lanes.iter().find(|l| l.spec.id == id).map(|l| &l.spec)
    }

    #[must_use]
    pub fn len(&self, id: K) -> Option<usize> {
        self.lanes
            .iter()
            .find(|l| l.spec.id == id)
            .map(|l| l.queue.len())
    }

    #[must_use]
    pub fn depth(&self, id: K) -> Option<usize> {
        self.lanes.iter().find(|l| l.spec.id == id).map(|l| l.depth)
    }

    #[must_use]
    pub fn deferred_len(&self) -> usize {
        self.deferred.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.deferred.is_empty() && self.lanes.iter().all(|l| l.queue.len() == 0)
    }

    /// Pop every queued item (including deferred). Resets the worker-idle gate.
    pub fn drain(&mut self) -> Vec<T> {
        self.busy_workers = 0;
        let mut out = Vec::new();
        while let Some(sel) = self.select() {
            self.note_idle();
            out.push(sel.item);
        }
        out
    }

    /// Resolved depth for a [`Depth::FromValidators`] lane after a validator-set change.
    pub fn set_depth(&mut self, id: K, depth: usize) -> Result<(), ConfigError> {
        if depth == 0 {
            return Err(ConfigError::ZeroDepth);
        }
        let Some(lane) = self.lanes.iter_mut().find(|l| l.spec.id == id) else {
            return Err(ConfigError::UnknownLane);
        };
        lane.depth = depth;
        lane.queue.set_max_length(depth);
        Ok(())
    }

    #[must_use]
    pub fn snapshots(&self) -> Vec<LaneSnapshot<K>> {
        self.lanes
            .iter()
            .map(|lane| LaneSnapshot {
                id: lane.spec.id,
                kind: lane.spec.queue,
                depth: lane.depth,
                len: lane.queue.len(),
                dropped: lane.queue.dropped(),
                evicted: lane.queue.evicted(),
                never_shed: lane.spec.never_shed,
            })
            .collect()
    }

    /// Selection-chain order (the policy).
    pub fn selection_chain(&self) -> impl Iterator<Item = K> + '_ {
        self.lanes.iter().map(|l| l.spec.id)
    }
}

impl<T> Manager<ChainLane, T> {
    /// Loop B manager: [`LOOP_B_LANES`] + `max_workers = 1`.
    pub fn loop_b(sizes: QueueSizes) -> Result<Self, ConfigError> {
        Self::new(LOOP_B_LANES, sizes, ManagerConfig::loop_b())
    }
}

impl<K: LaneKey, T> fmt::Debug for Manager<K, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manager")
            .field("max_workers", &self.max_workers)
            .field("busy_workers", &self.busy_workers)
            .field("deferred_len", &self.deferred.len())
            .field("lanes", &self.snapshots())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::config::{
        Depth, IMPORT_LANE_DEPTH, QUERY_P0_LANE_DEPTH, QUERY_P1_LANE_DEPTH, TICK_LANE_DEPTH,
    };

    fn mgr() -> Manager<ChainLane, &'static str> {
        Manager::loop_b(QueueSizes::new(30, 32)).unwrap()
    }

    #[test]
    fn first_match_wins_walks_the_data_chain() {
        let mut m = mgr();
        assert!(m.push(ChainLane::QueryP1, "p1").accepted());
        assert!(m.push(ChainLane::Attestation, "att").accepted());
        assert!(m.push(ChainLane::Import, "imp").accepted());

        let a = m.select().unwrap();
        assert_eq!(a.source, WorkSource::Lane(ChainLane::Import));
        assert_eq!(a.item, "imp");
        m.note_idle();

        let b = m.select().unwrap();
        assert_eq!(b.source, WorkSource::Lane(ChainLane::Attestation));
        assert_eq!(b.item, "att");
        m.note_idle();

        let c = m.select().unwrap();
        assert_eq!(c.source, WorkSource::Lane(ChainLane::QueryP1));
        assert_eq!(c.item, "p1");
    }

    #[test]
    fn empty_higher_lane_falls_through() {
        let mut m = mgr();
        assert!(m.push(ChainLane::QueryP0, "head").accepted());
        let got = m.select().unwrap();
        assert_eq!(got.source, WorkSource::Lane(ChainLane::QueryP0));
        assert_eq!(got.item, "head");
    }

    #[test]
    fn deferred_outranks_new_inbound() {
        let mut m = mgr();
        assert!(m.push(ChainLane::Tick, "tick").accepted());
        assert!(m.push_deferred("reprocess").accepted());
        let got = m.select().unwrap();
        assert_eq!(got.source, WorkSource::Deferred);
        assert_eq!(got.item, "reprocess");
        m.note_idle();
        let got = m.select().unwrap();
        assert_eq!(got.source, WorkSource::Lane(ChainLane::Tick));
    }

    #[test]
    fn worker_idle_gate_blocks_select_at_max_workers() {
        let mut m = mgr();
        assert_eq!(m.max_workers(), 1);
        assert!(m.push(ChainLane::Import, "a").accepted());
        assert!(m.push(ChainLane::Import, "b").accepted());
        assert!(m.select().is_some());
        assert!(m.select().is_none());
        m.note_idle();
        let got = m.select().unwrap();
        assert_eq!(got.item, "b");
    }

    #[test]
    fn never_shed_refuses_instead_of_dropping() {
        let mut m = mgr();
        for i in 0..TICK_LANE_DEPTH {
            // Distinct addresses so Enqueue::Accepted is the only success path.
            let item: &'static str = ["t0", "t1", "t2", "t3"][i];
            assert!(m.push(ChainLane::Tick, item).accepted());
        }
        assert!(matches!(
            m.push(ChainLane::Tick, "overflow"),
            Enqueue::WouldShed("overflow")
        ));
        assert_eq!(m.len(ChainLane::Tick), Some(TICK_LANE_DEPTH));
        let tick = m.snapshots().into_iter().find(|s| s.id == ChainLane::Tick);
        assert_eq!(tick.unwrap().dropped, 0);
    }

    #[test]
    fn import_fifo_drops_new() {
        let mut m = mgr();
        for _ in 0..IMPORT_LANE_DEPTH {
            assert!(m.push(ChainLane::Import, "ok").accepted());
        }
        assert!(matches!(
            m.push(ChainLane::Import, "new"),
            Enqueue::DroppedNew("new")
        ));
        assert_eq!(m.len(ChainLane::Import), Some(IMPORT_LANE_DEPTH));
    }

    #[test]
    fn attestation_lifo_evicts_oldest() {
        let mut m = Manager::loop_b(QueueSizes::new(30, 32)).unwrap();
        let att = m
            .snapshots()
            .into_iter()
            .find(|s| s.id == ChainLane::Attestation);
        let depth = att.unwrap().depth;
        assert!(depth >= MIN_QUEUE_LEN);
        for _ in 0..depth {
            assert!(m.push(ChainLane::Attestation, "old").accepted());
        }
        assert!(matches!(
            m.push(ChainLane::Attestation, "fresh"),
            Enqueue::EvictedOldest("old")
        ));
        // Newest is selected first among attestations (and attestation beats query_p1).
        let got = m.select().unwrap();
        assert_eq!(got.source, WorkSource::Lane(ChainLane::Attestation));
        assert_eq!(got.item, "fresh");
    }

    #[test]
    fn drain_empties_every_lane() {
        let mut m = mgr();
        assert!(m.is_empty());
        assert!(m.push(ChainLane::Import, "imp").accepted());
        assert!(m.push(ChainLane::QueryP1, "p1").accepted());
        assert!(!m.is_empty());
        assert_eq!(m.depth(ChainLane::Import), Some(IMPORT_LANE_DEPTH));
        let drained = m.drain();
        assert_eq!(drained, ["imp", "p1"]);
        assert!(m.is_empty());
    }

    #[test]
    fn loop_b_depths_and_worker_cap() {
        let m = mgr();
        let snaps = m.snapshots();
        assert_eq!(snaps.len(), 5);
        assert_eq!(snaps[0].id, ChainLane::Tick);
        assert_eq!(snaps[0].depth, TICK_LANE_DEPTH);
        assert_eq!(snaps[1].depth, IMPORT_LANE_DEPTH);
        assert_eq!(snaps[2].id, ChainLane::QueryP0);
        assert_eq!(snaps[2].depth, QUERY_P0_LANE_DEPTH);
        assert_eq!(snaps[4].depth, QUERY_P1_LANE_DEPTH);
        assert_eq!(snaps[3].kind, QueueKind::Lifo);
        assert_eq!(m.max_workers(), 1);
        let chain: Vec<ChainLane> = m.selection_chain().collect();
        assert_eq!(chain, ChainLane::ALL);
    }

    #[test]
    fn rejects_empty_and_duplicate_and_zero_caps() {
        assert!(matches!(
            Manager::<ChainLane, u8>::new(&[], QueueSizes::new(0, 32), ManagerConfig::loop_b()),
            Err(ConfigError::EmptyChain)
        ));
        let dup = [LOOP_B_LANES[0], LOOP_B_LANES[0]];
        assert!(matches!(
            Manager::<ChainLane, u8>::new(&dup, QueueSizes::new(0, 32), ManagerConfig::loop_b()),
            Err(ConfigError::DuplicateLane)
        ));
        let cfg = ManagerConfig {
            max_workers: 0,
            ..ManagerConfig::loop_b()
        };
        assert!(matches!(
            Manager::<ChainLane, u8>::new(LOOP_B_LANES, QueueSizes::new(0, 32), cfg),
            Err(ConfigError::ZeroMaxWorkers)
        ));
        let zero = [LaneSpec {
            id: ChainLane::Tick,
            queue: QueueKind::Fifo,
            depth: Depth::Fixed(0),
            never_shed: true,
            why: "test",
        }];
        assert!(matches!(
            Manager::<ChainLane, u8>::new(&zero, QueueSizes::new(0, 32), ManagerConfig::loop_b()),
            Err(ConfigError::ZeroDepth)
        ));
    }
}
