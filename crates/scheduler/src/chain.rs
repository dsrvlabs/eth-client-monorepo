//! Loop B lane registry and work-type labels ([ARCH] §3.2).
//!
//! [`LOOP_B_LANES`] is the first-match-wins selection chain. Later issues map
//! `CoreCommand` variants onto [`ChainWork`]; this issue does not wire them.

use crate::config::{
    Depth, IMPORT_LANE_DEPTH, LaneKey, LaneSpec, QUERY_P0_LANE_DEPTH, QUERY_P1_LANE_DEPTH,
    QueueKind, TICK_LANE_DEPTH,
};

/// Loop B lanes, in selection order. First match wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChainLane {
    Tick,
    Import,
    QueryP0,
    Attestation,
    QueryP1,
}

impl ChainLane {
    pub const ALL: [Self; 5] = [
        Self::Tick,
        Self::Import,
        Self::QueryP0,
        Self::Attestation,
        Self::QueryP1,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Import => "import",
            Self::QueryP0 => "query_p0",
            Self::Attestation => "attestation",
            Self::QueryP1 => "query_p1",
        }
    }
}

impl LaneKey for ChainLane {
    fn as_str(self) -> &'static str {
        Self::as_str(self)
    }
}

/// Per-work-type metric labels for Loop B. Parallel to today's `CoreCommand`
/// variants that will ride these lanes; producers are not wired here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChainWork {
    SlotTick,
    Shutdown,
    ImportBlock,
    ImportBlockGossip,
    DataAvailable,
    QueryHead,
    QueryIsOptimistic,
    ApplyAttestations,
    QueryCommitteeShuffling,
    QueryValidatorPubkeys,
    QueryValidatorRecords,
    QueryCanonicalRoots,
}

impl ChainWork {
    pub const ALL: [Self; 12] = [
        Self::SlotTick,
        Self::Shutdown,
        Self::ImportBlock,
        Self::ImportBlockGossip,
        Self::DataAvailable,
        Self::QueryHead,
        Self::QueryIsOptimistic,
        Self::ApplyAttestations,
        Self::QueryCommitteeShuffling,
        Self::QueryValidatorPubkeys,
        Self::QueryValidatorRecords,
        Self::QueryCanonicalRoots,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SlotTick => "slot_tick",
            Self::Shutdown => "shutdown",
            Self::ImportBlock => "import_block",
            Self::ImportBlockGossip => "import_block_gossip",
            Self::DataAvailable => "data_available",
            Self::QueryHead => "query_head",
            Self::QueryIsOptimistic => "query_is_optimistic",
            Self::ApplyAttestations => "apply_attestations",
            Self::QueryCommitteeShuffling => "query_committee_shuffling",
            Self::QueryValidatorPubkeys => "query_validator_pubkeys",
            Self::QueryValidatorRecords => "query_validator_records",
            Self::QueryCanonicalRoots => "query_canonical_roots",
        }
    }

    /// Lane this work type belongs to. `DataAvailable` stays in `import`.
    #[must_use]
    pub const fn lane(self) -> ChainLane {
        match self {
            Self::SlotTick | Self::Shutdown => ChainLane::Tick,
            Self::ImportBlock | Self::ImportBlockGossip | Self::DataAvailable => ChainLane::Import,
            Self::QueryHead | Self::QueryIsOptimistic => ChainLane::QueryP0,
            Self::ApplyAttestations => ChainLane::Attestation,
            Self::QueryCommitteeShuffling
            | Self::QueryValidatorPubkeys
            | Self::QueryValidatorRecords
            | Self::QueryCanonicalRoots => ChainLane::QueryP1,
        }
    }
}

/// Loop B selection chain. Slice order is the policy ([ARCH] §3.2).
pub const LOOP_B_LANES: &[LaneSpec<ChainLane>] = &[
    LaneSpec {
        id: ChainLane::Tick,
        queue: QueueKind::Fifo,
        depth: Depth::Fixed(TICK_LANE_DEPTH),
        never_shed: true,
        why: "store.time advances only via SlotTick; a dropped tick IGNOREs valid blocks as future_slot",
    },
    LaneSpec {
        id: ChainLane::Import,
        queue: QueueKind::Fifo,
        depth: Depth::Fixed(IMPORT_LANE_DEPTH),
        never_shed: false,
        why: "blocks import sequentially; drop new rather than evict an in-flight import. DataAvailable stays here — it re-drives a parked block",
    },
    LaneSpec {
        id: ChainLane::QueryP0,
        queue: QueueKind::Fifo,
        depth: Depth::Fixed(QUERY_P0_LANE_DEPTH),
        never_shed: false,
        why: "head probes must not wait behind a block import or an attestation flood",
    },
    LaneSpec {
        id: ChainLane::Attestation,
        queue: QueueKind::Lifo,
        depth: Depth::FromValidators,
        never_shed: false,
        why: "a later attestation is strictly better information; evict oldest under load",
    },
    LaneSpec {
        id: ChainLane::QueryP1,
        queue: QueueKind::Fifo,
        depth: Depth::Fixed(QUERY_P1_LANE_DEPTH),
        never_shed: false,
        why: "serving reads must not be evicted; drop new under load rather than flush a request",
    },
];

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn loop_b_chain_order_is_the_policy_table() {
        let ids: Vec<ChainLane> = LOOP_B_LANES.iter().map(|s| s.id).collect();
        assert_eq!(ids, ChainLane::ALL);
        assert_eq!(
            ids,
            [
                ChainLane::Tick,
                ChainLane::Import,
                ChainLane::QueryP0,
                ChainLane::Attestation,
                ChainLane::QueryP1,
            ]
        );
    }

    #[test]
    fn loop_b_queue_kinds_and_never_shed() {
        let by_id = |id| LOOP_B_LANES.iter().find(|s| s.id == id).unwrap();
        assert_eq!(by_id(ChainLane::Tick).queue, QueueKind::Fifo);
        assert!(by_id(ChainLane::Tick).never_shed);
        assert_eq!(by_id(ChainLane::Import).queue, QueueKind::Fifo);
        assert!(!by_id(ChainLane::Import).never_shed);
        assert_eq!(
            by_id(ChainLane::Import).depth,
            Depth::Fixed(IMPORT_LANE_DEPTH)
        );
        assert_eq!(by_id(ChainLane::QueryP0).queue, QueueKind::Fifo);
        assert!(!by_id(ChainLane::QueryP0).never_shed);
        assert_eq!(
            by_id(ChainLane::QueryP0).depth,
            Depth::Fixed(QUERY_P0_LANE_DEPTH)
        );
        assert_eq!(by_id(ChainLane::Attestation).queue, QueueKind::Lifo);
        assert_eq!(by_id(ChainLane::Attestation).depth, Depth::FromValidators);
        assert_eq!(by_id(ChainLane::QueryP1).queue, QueueKind::Fifo);
    }

    #[test]
    fn data_available_stays_in_import() {
        assert_eq!(ChainWork::DataAvailable.lane(), ChainLane::Import);
    }

    #[test]
    fn every_work_type_maps_to_a_registered_lane() {
        for work in ChainWork::ALL {
            assert!(
                LOOP_B_LANES.iter().any(|s| s.id == work.lane()),
                "{} maps to unregistered {}",
                work.as_str(),
                work.lane().as_str()
            );
        }
    }

    #[test]
    fn work_type_labels_are_unique() {
        let mut labels: Vec<&str> = ChainWork::ALL.iter().map(|w| w.as_str()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), ChainWork::ALL.len());
    }
}
