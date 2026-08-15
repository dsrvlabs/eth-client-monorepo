//! Queue-type and depth configuration, plus the inbound poll-order chain.
//!
//! Both arrays below are the policy. Reviewers diff the data, not a `match`.

/// Metric / registry key for a lane. One implementation per loop.
pub trait LaneKey: Copy + Eq + std::fmt::Debug {
    fn as_str(self) -> &'static str;
}

/// Overflow policy. Lives on the queue type, chosen per lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueKind {
    /// Drop the new item when full.
    Fifo,
    /// Evict the oldest item when full.
    Lifo,
}

impl QueueKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fifo => "fifo",
            Self::Lifo => "lifo",
        }
    }
}

/// Queue length as a formula, not a compile-time constant ([q1] §1.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Depth {
    Fixed(usize),
    /// [`sized_from_validators`] at manager construction (and on resize).
    FromValidators,
}

/// Inputs for [`Depth::FromValidators`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueueSizes {
    pub active_validators: u64,
    pub slots_per_epoch: u64,
}

impl QueueSizes {
    #[must_use]
    pub const fn new(active_validators: u64, slots_per_epoch: u64) -> Self {
        Self {
            active_validators,
            slots_per_epoch,
        }
    }

    #[must_use]
    pub const fn resolve(self, depth: Depth) -> usize {
        match depth {
            Depth::Fixed(n) => n,
            Depth::FromValidators => {
                sized_from_validators(self.active_validators, self.slots_per_epoch)
            }
        }
    }
}

/// Floor so integer division cannot produce a zero-length queue ([q1] §1.5).
pub const MIN_QUEUE_LEN: usize = 128;

/// Over-provision active validators when sizing attestation-shaped lanes.
pub const OVERPROVISION_PCT: u64 = 110;

/// Loop B `tick` depth ([ARCH] §3.2).
pub const TICK_LANE_DEPTH: usize = 4;

/// Loop B `import` depth ([ARCH] §3.2). Matches today's single-channel bound.
pub const IMPORT_LANE_DEPTH: usize = 64;

/// Loop B `query_p0` depth ([ARCH] §3.2).
pub const QUERY_P0_LANE_DEPTH: usize = 64;

/// Loop B `query_p1` depth ([ARCH] §3.2).
pub const QUERY_P1_LANE_DEPTH: usize = 64;

/// Conceptual worker cap. Loop B stays 1 (ADR P1-09): one `Store`, one thread.
pub const DEFAULT_MAX_WORKERS: usize = 1;

/// `max(active * 110% / slots_per_epoch, MIN_QUEUE_LEN)`.
#[must_use]
pub const fn sized_from_validators(active: u64, slots_per_epoch: u64) -> usize {
    if slots_per_epoch == 0 {
        return MIN_QUEUE_LEN;
    }
    let overprovisioned = active.saturating_mul(OVERPROVISION_PCT) / 100;
    let per_slot = overprovisioned / slots_per_epoch;
    if per_slot < MIN_QUEUE_LEN as u64 {
        MIN_QUEUE_LEN
    } else {
        per_slot as usize
    }
}

/// One lane in a selection chain. Slice order is priority (first match wins).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneSpec<K> {
    pub id: K,
    pub queue: QueueKind,
    pub depth: Depth,
    /// When set, a full lane refuses the item instead of applying overflow.
    /// The tick lane is never-shed: producers block rather than drop (S0-A-14).
    pub never_shed: bool,
    /// One-line adversarial rationale for this lane's overflow policy.
    pub why: &'static str,
}

/// Inbound classes the manager merges. Drain order is [`INBOUND_POLL_CHAIN`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InboundClass {
    /// Worker-freed / capacity check. A high inbound rate must not starve this.
    WorkerIdle,
    /// Already-deferred / reprocessing work.
    Deferred,
    /// New inbound, selected by the lane chain.
    New,
}

impl InboundClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkerIdle => "worker_idle",
            Self::Deferred => "deferred",
            Self::New => "new",
        }
    }
}

/// Poll-priority inversion ([ARCH] §3.1 / [q1] §2.4). This array is the policy.
pub const INBOUND_POLL_CHAIN: &[InboundClass] = &[
    InboundClass::WorkerIdle,
    InboundClass::Deferred,
    InboundClass::New,
];

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn sized_from_validators_floors_small_sets() {
        assert_eq!(sized_from_validators(0, 32), MIN_QUEUE_LEN);
        assert_eq!(sized_from_validators(30, 32), MIN_QUEUE_LEN);
        assert_eq!(sized_from_validators(30, 0), MIN_QUEUE_LEN);
    }

    #[test]
    fn sized_from_validators_mainnet_scale() {
        assert_eq!(sized_from_validators(1_000_000, 32), 34_375);
    }

    #[test]
    fn inbound_poll_chain_is_the_inversion() {
        assert_eq!(
            INBOUND_POLL_CHAIN,
            &[
                InboundClass::WorkerIdle,
                InboundClass::Deferred,
                InboundClass::New,
            ]
        );
    }
}
