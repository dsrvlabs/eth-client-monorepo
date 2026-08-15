//! `beacon_block` validator — chain-authoritative (ADR P2-04 / CC-22d).
//!
//! Local stage: size, SSZ decode, timing, `(slot, proposer)` dedup.
//! Consensus stage (parent, proposer index, finalized descent, transition) is
//! `chain`'s over the CC-27 stream. This module returns either a local
//! terminal [`Verdict`] or a [`BlockForward`] for the stream client.

use cc_proto::common::Source;
use cc_proto::p2p::{GossipObject, ObjectKind, Reason};
use cc_types::preset::Preset;
use cc_types::{ForkName, SignedBeaconBlock};
use ssz::Encode;
use tree_hash::TreeHash;

use std::sync::Arc;

use super::check_payload_len;
use crate::gossip::pending::{PendingBlock, PendingQueues};
use crate::gossip::seen::{BlockSeenKey, SeenSets};
use crate::gossip::topics::TopicName;
use crate::metrics::{P2pMetrics, QueueName};
use crate::verdict::Verdict;

/// Local-stage inputs for one beacon block.
#[derive(Debug)]
pub struct BlockValidateInput<'a> {
    /// Decompressed SSZ payload.
    pub payload: &'a [u8],
    /// Current slot (clock / view).
    pub current_slot: u64,
    /// Finalized slot lower bound.
    pub finalized_slot: u64,
    /// Gossip clock disparity in slots.
    pub disparity_slots: u64,
    /// Topic string.
    pub topic: &'a str,
    /// Message id bytes (pending).
    pub message_id: &'a [u8],
    /// Peer id bytes (pending).
    pub peer_id: &'a [u8],
}

/// When local checks pass, forward to chain.
#[derive(Debug, Clone)]
pub struct BlockForward {
    /// Object for the chain stream.
    pub object: GossipObject,
    /// Slot / proposer for seen-set insert on ACCEPT.
    pub slot: u64,
    /// Proposer index.
    pub proposer_index: u64,
    /// Canonical block root.
    pub block_root: [u8; 32],
    /// Parent root (for pending-block redrive).
    pub parent_root: [u8; 32],
}

/// Local validation outcome.
#[derive(Debug)]
pub enum BlockOutcome {
    /// Terminal local verdict (do not send to chain).
    Done(Verdict),
    /// Local checks passed — send to chain and report chain's verdict.
    Forward(BlockForward),
    /// IGNORE + parked (unknown parent locally). Rare for blocks; chain also
    /// handles UnknownParent. Used when we want local redrive before stream.
    Pending(Verdict),
}

/// Run local beacon_block checks. Chain does the rest.
pub fn validate_beacon_block_local<P: Preset>(
    seen: &mut SeenSets,
    pending: &mut PendingQueues,
    input: &BlockValidateInput<'_>,
    metrics: Option<&P2pMetrics>,
) -> BlockOutcome {
    // Size
    if check_payload_len::<P>(TopicName::BeaconBlock, input.payload.len()).is_err() {
        return BlockOutcome::Done(Verdict::reject(Reason::Invalid, vec![]));
    }

    // SSZ decode (Fulu-only)
    let signed = match SignedBeaconBlock::<P>::from_ssz_bytes_with(ForkName::Fulu, input.payload) {
        Ok(b) => b,
        Err(_) => return BlockOutcome::Done(Verdict::reject(Reason::Invalid, vec![])),
    };

    let slot = signed.message.slot.as_u64();
    let proposer_index = signed.message.proposer_index.as_u64();
    let parent_root = *signed.message.parent_root.as_array();
    let block_root = {
        let h = signed.message.tree_hash_root();
        let mut a = [0u8; 32];
        a.copy_from_slice(h.as_slice());
        a
    };
    let corr = block_root.to_vec();

    // Timing
    let upper = input.current_slot.saturating_add(input.disparity_slots);
    if slot < input.finalized_slot || slot > upper {
        let reason = if slot > upper {
            Reason::FutureSlot
        } else {
            Reason::AlreadyKnown
        };
        return BlockOutcome::Done(Verdict::ignore(reason, corr));
    }

    // Dedup (slot, proposer)
    let key = BlockSeenKey {
        slot,
        proposer_index,
    };
    if seen.blocks.contains(&key) {
        return BlockOutcome::Done(Verdict::ignore(Reason::Duplicate, corr));
    }

    // Optional local parent gate → IGNORE + pending-block (spec delta 12 style).
    // Only park when we have *some* known roots but not this parent (warm state).
    // Cold start (no roots) always forwards to chain (UnknownParent is chain's).
    if !seen.block_roots.is_empty() && !seen.parent_known(&parent_root) {
        let item = PendingBlock {
            ssz: Arc::from(input.payload),
            topic: input.topic.to_owned(),
            message_id: input.message_id.to_vec(),
            peer_id: input.peer_id.to_vec(),
            parent_root,
            slot,
            proposer_index,
        };
        let _ = pending.park_block(item);
        if let Some(m) = metrics {
            m.set_queue_depth(QueueName::PendingBlock, pending.blocks.len() as i64);
        }
        return BlockOutcome::Pending(Verdict::ignore(Reason::UnknownParent, corr));
    }

    let object = GossipObject {
        ssz: input.payload.to_vec(),
        // Phase 1 is Fulu-only; numeric fork tag is informational (chain re-decodes).
        fork: 0,
        root: block_root.to_vec(),
        source: Source::Gossip as i32,
        kind: ObjectKind::Block as i32,
        subnet_id: 0,
    };

    BlockOutcome::Forward(BlockForward {
        object,
        slot,
        proposer_index,
        block_root,
        parent_root,
    })
}

/// After chain ACCEPT, insert into block seen set and note root for columns.
pub fn note_accepted_block(seen: &mut SeenSets, slot: u64, proposer_index: u64, root: [u8; 32]) {
    let _ = seen.blocks.insert(BlockSeenKey {
        slot,
        proposer_index,
    });
    seen.note_block_root(root);
}

/// Re-encode helper for tests.
#[must_use]
pub fn encode_block<P: Preset>(block: &SignedBeaconBlock<P>) -> Vec<u8> {
    block.as_ssz_bytes()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::gossip::pending::PendingQueues;
    use crate::gossip::seen::SeenSets;
    use cc_types::SignedBeaconBlock;
    use cc_types::preset::Mainnet;

    #[test]
    fn oversize_rejects() {
        let mut seen = SeenSets::new();
        let mut pending = PendingQueues::new();
        let max = crate::gossip::validate::max_container_bytes::<Mainnet>(TopicName::BeaconBlock);
        let payload = vec![0u8; max + 1];
        let inp = BlockValidateInput {
            payload: &payload,
            current_slot: 10,
            finalized_slot: 0,
            disparity_slots: 1,
            topic: "beacon_block",
            message_id: b"m",
            peer_id: b"p",
        };
        let out = validate_beacon_block_local::<Mainnet>(&mut seen, &mut pending, &inp, None);
        assert!(
            matches!(out, BlockOutcome::Done(ref v) if matches!(v.acceptance, cc_proto::p2p::Acceptance::Reject))
        );
    }

    #[test]
    fn bad_ssz_rejects() {
        let mut seen = SeenSets::new();
        let mut pending = PendingQueues::new();
        let payload = vec![1, 2, 3, 4];
        let inp = BlockValidateInput {
            payload: &payload,
            current_slot: 10,
            finalized_slot: 0,
            disparity_slots: 1,
            topic: "beacon_block",
            message_id: b"m",
            peer_id: b"p",
        };
        let out = validate_beacon_block_local::<Mainnet>(&mut seen, &mut pending, &inp, None);
        assert!(
            matches!(out, BlockOutcome::Done(ref v) if matches!(v.acceptance, cc_proto::p2p::Acceptance::Reject))
        );
    }

    #[test]
    fn default_block_forwards_when_cold() {
        let mut seen = SeenSets::new();
        let mut pending = PendingQueues::new();
        let block = SignedBeaconBlock::<Mainnet>::default();
        // slot 0, current 0 — within window
        let payload = block.as_ssz_bytes();
        let inp = BlockValidateInput {
            payload: &payload,
            current_slot: 10,
            finalized_slot: 0,
            disparity_slots: 1,
            topic: "beacon_block",
            message_id: b"m",
            peer_id: b"p",
        };
        let out = validate_beacon_block_local::<Mainnet>(&mut seen, &mut pending, &inp, None);
        assert!(matches!(out, BlockOutcome::Forward(_)), "got {out:?}");
    }

    #[test]
    fn duplicate_slot_proposer_ignores() {
        let mut seen = SeenSets::new();
        let mut pending = PendingQueues::new();
        let block = SignedBeaconBlock::<Mainnet>::default();
        let payload = block.as_ssz_bytes();
        let _ = seen.blocks.insert(BlockSeenKey {
            slot: 0,
            proposer_index: 0,
        });
        let inp = BlockValidateInput {
            payload: &payload,
            current_slot: 10,
            finalized_slot: 0,
            disparity_slots: 1,
            topic: "beacon_block",
            message_id: b"m",
            peer_id: b"p",
        };
        let out = validate_beacon_block_local::<Mainnet>(&mut seen, &mut pending, &inp, None);
        assert!(matches!(
            out,
            BlockOutcome::Done(ref v) if v.reason == Reason::Duplicate
        ));
    }
}
