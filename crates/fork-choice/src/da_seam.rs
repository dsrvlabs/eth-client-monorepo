//! Data-availability seam (Architecture §6.5, CC-17).
//!
//! The whole Phase 1 DA surface lives in this file so Phase 2 (CC-24) can
//! substitute real PeerDAS sampling by changing one body, not `on_block` or
//! other fork-choice logic. Symmetrical with the CC-14 engine seam.
//!
//! # Fulu signature
//!
//! Spec delta 6 / Fulu dropped `blob_kzg_commitments` from this predicate.
//! The trait takes **one** argument — `beacon_block_root: Root` — so the
//! Phase 2 substitution does not change the call-site shape.
//!
//! # Spec deviation: defer, do not assert
//!
//! The consensus-spec writes DA as an `assert` inside `on_block`, collapsing
//! "unavailable" into "invalid". A real client must not treat a block whose
//! columns have not arrived yet as a proposer fault. Phase 1 therefore returns
//! [`BlockImport::Deferred`] with [`DeferralReason::DataUnavailable`] — the
//! shape CC-24's requeue needs.
//!
//! # Call site
//!
//! **Intended** sole production call site (CC-15b): top of `on_block`, before
//! the state transition. This module defines the seam and enums only; the
//! production call lands with `on_block`. It does **not** call the CC-14
//! engine seam (that call site stays sole inside `process_execution_payload`).

use cc_state_transition::GossipClass;
use cc_types::primitives::Root;

/// Spec-shaped data-availability seam (Architecture §6.5).
///
/// Object-safe: `dyn DataAvailability` is the store field type so CC-24 can
/// swap `PeerDasAvailability` without touching fork-choice logic.
///
/// # Fulu
///
/// One argument only — Fulu dropped `blob_kzg_commitments` from this predicate.
pub trait DataAvailability: Send + Sync {
    /// Whether the data for `beacon_block_root` is available for import.
    ///
    /// Fulu signature: ONE argument (`beacon_block_root`). Fulu dropped
    /// `blob_kzg_commitments` from this predicate; a two-argument stub would
    /// force a call-site rewrite at CC-24.
    fn is_data_available(&self, beacon_block_root: Root) -> bool;
}

/// Phase 1 optimistic stub: every root is data-available.
///
/// Phase 2 (CC-24) **deletes** this type and substitutes `PeerDasAvailability`
/// against the sampling tracker. Not feature-gated — a gated stub is a path
/// that can be re-enabled by accident.
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysAvailable;

impl DataAvailability for AlwaysAvailable {
    fn is_data_available(&self, _beacon_block_root: Root) -> bool {
        true
    }
}

/// Outcome of `on_block` (Architecture §6.5).
///
/// Import either succeeds or is deferred for a non-fault reason. Provably
/// invalid blocks return an error instead (e.g. not descended from finalized).
///
/// # Handoff (CC-15b)
///
/// DA / unknown-parent / future-slot gates **must** return
/// `Ok(BlockImport::Deferred(...))`. Do **not** map those cases to
/// `Err(BlockError::{DataNotAvailable, UnknownParent, FutureSlot})` — those
/// pre-existing error variants are a parallel taxonomy that would fold to
/// `INVALID` at the ImportBlock verdict boundary instead of `DEFERRED_*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockImport {
    /// Block entered the store and proto-array.
    Imported(ImportedBlock),
    /// Block is not yet importable; sender is not at fault.
    Deferred(DeferralReason),
}

/// Successful import payload from `on_block`.
///
/// Fields expand under CC-15b once the real import path produces more context
/// (post-state root, slot, …). The root alone is enough for the DA-seam
/// substitution test and the verdict boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedBlock {
    /// Canonical tree-hash root of the imported beacon block.
    pub root: Root,
}

/// Why `on_block` deferred rather than imported (Architecture §6.5).
///
/// All variants classify as [`GossipClass::Ignore`] — the peer is not at fault.
///
/// # Handoff (CC-15b)
///
/// These are **success-path deferrals**, not transition errors. `on_block`
/// must return `Ok(Deferred(reason))` for each variant — never
/// `Err(BlockError::DataNotAvailable)` / `UnknownParent` / `FutureSlot`
/// (naming note: error taxonomy uses `DataNotAvailable`; this enum uses
/// `DataUnavailable`). Misrouting to `Err` loses the first-class
/// `DEFERRED_DA` / `UNKNOWN_PARENT` ImportBlock verdicts that CC-24 requeue
/// and driver walk-back need.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeferralReason {
    /// Sampled columns / blobs for this root are not yet verified available.
    DataUnavailable,
    /// Parent block is not in the store.
    UnknownParent,
    /// Block slot is still in the future relative to store time.
    FutureSlot,
}

impl DeferralReason {
    /// Phase 2 maps this to the p2p ACCEPT/REJECT/IGNORE verdict.
    ///
    /// Every deferral is [`GossipClass::Ignore`]: data not yet available, an
    /// unknown parent, or a future slot is never a peer descore. Exhaustive —
    /// no `_ =>` arm so a new variant fails to compile until classified.
    pub const fn gossip_class(self) -> GossipClass {
        match self {
            Self::DataUnavailable | Self::UnknownParent | Self::FutureSlot => GossipClass::Ignore,
        }
    }
}

impl BlockImport {
    /// Gossip classification when the outcome is a deferral.
    ///
    /// `Imported` has no gossip class (the message was already accepted into
    /// the store). Callers map only deferred outcomes at the verdict boundary.
    pub const fn gossip_class(&self) -> Option<GossipClass> {
        match self {
            Self::Imported(_) => None,
            Self::Deferred(reason) => Some(reason.gossip_class()),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Test-only DA implementation that always reports unavailable.
    ///
    /// Lives here so the substitution test is a pure trait-object swap
    /// (CC-17/2): no production branch on "is stubbed".
    #[derive(Debug, Default, Clone, Copy)]
    struct NeverAvailable;

    impl DataAvailability for NeverAvailable {
        fn is_data_available(&self, _beacon_block_root: Root) -> bool {
            false
        }
    }

    /// Minimal stand-in for `on_block`'s DA gate + post-gate path.
    ///
    /// Mirrors Architecture §6.5 / CC-15b order: DA first, then state
    /// transition, then store write. Real `on_block` lands in CC-15b and
    /// re-runs the same assertions against the production function.
    fn on_block_with_da(
        da: &dyn DataAvailability,
        root: Root,
        transition_counter: &AtomicU32,
        blocks: &mut HashMap<Root, ()>,
    ) -> BlockImport {
        // --- single intended call-site shape (production: top of on_block) ---
        if !da.is_data_available(root) {
            return BlockImport::Deferred(DeferralReason::DataUnavailable);
        }
        // State transition would run here (counter proves it did / did not).
        transition_counter.fetch_add(1, Ordering::SeqCst);
        blocks.insert(root, ());
        BlockImport::Imported(ImportedBlock { root })
    }

    /// CC-17/2: substituting `NeverAvailable` yields `Deferred(DataUnavailable)`
    /// — not an error, not an import — and no transition / store write occurs.
    #[test]
    fn never_available_defers_without_transition_or_store_write() {
        // Object-safety assertion: this type must compile (CC-17/2).
        let da: &dyn DataAvailability = &NeverAvailable;
        let root = Root::from_array([0xab; 32]);
        let transition_counter = AtomicU32::new(0);
        let mut blocks: HashMap<Root, ()> = HashMap::new();

        let outcome = on_block_with_da(da, root, &transition_counter, &mut blocks);

        assert!(
            matches!(
                outcome,
                BlockImport::Deferred(DeferralReason::DataUnavailable)
            ),
            "expected Deferred(DataUnavailable), got {outcome:?}"
        );
        assert_eq!(
            transition_counter.load(Ordering::SeqCst),
            0,
            "state transition must not run when DA fails"
        );
        assert!(
            !blocks.contains_key(&root),
            "block must be absent from store.blocks when deferred"
        );
        // Verdict boundary: Ignore, never Reject.
        assert_eq!(
            outcome.gossip_class(),
            Some(GossipClass::Ignore),
            "Deferred(DataUnavailable) must classify as Ignore, never Reject"
        );
    }

    /// AlwaysAvailable admits the root; the post-gate path runs once.
    #[test]
    fn always_available_imports() {
        let da: &dyn DataAvailability = &AlwaysAvailable;
        let root = Root::from_array([0xcd; 32]);
        let transition_counter = AtomicU32::new(0);
        let mut blocks: HashMap<Root, ()> = HashMap::new();

        let outcome = on_block_with_da(da, root, &transition_counter, &mut blocks);

        assert_eq!(outcome, BlockImport::Imported(ImportedBlock { root }));
        assert_eq!(transition_counter.load(Ordering::SeqCst), 1);
        assert!(blocks.contains_key(&root));
        assert_eq!(outcome.gossip_class(), None);
    }

    /// Direct AlwaysAvailable predicate — Phase 1 stub is unconditional true.
    #[test]
    fn always_available_returns_true_for_any_root() {
        let da = AlwaysAvailable;
        assert!(da.is_data_available(Root::ZERO));
        assert!(da.is_data_available(Root::from_array([0xff; 32])));
    }

    /// `Deferred(DataUnavailable)` → `GossipClass::Ignore`, never `Reject`.
    ///
    /// All [`DeferralReason`] variants are Ignore (exhaustive).
    #[test]
    fn deferred_data_unavailable_is_gossip_ignore_not_reject() {
        assert_eq!(
            DeferralReason::DataUnavailable.gossip_class(),
            GossipClass::Ignore
        );
        assert_ne!(
            DeferralReason::DataUnavailable.gossip_class(),
            GossipClass::Reject
        );
        // Exhaustive classification of every deferral reason.
        assert_eq!(
            DeferralReason::UnknownParent.gossip_class(),
            GossipClass::Ignore
        );
        assert_eq!(
            DeferralReason::FutureSlot.gossip_class(),
            GossipClass::Ignore
        );
        assert_eq!(
            BlockImport::Deferred(DeferralReason::DataUnavailable).gossip_class(),
            Some(GossipClass::Ignore)
        );
    }

    /// Trait signature is one-arg: a two-arg call must not compile.
    ///
    /// Enforced by the type system; this test exercises the one-arg form via
    /// `dyn DataAvailability` so a signature drift fails at compile time.
    #[test]
    fn fulu_signature_is_one_argument() {
        fn assert_one_arg(da: &dyn DataAvailability, root: Root) -> bool {
            da.is_data_available(root)
        }
        assert!(assert_one_arg(&AlwaysAvailable, Root::ZERO));
        assert!(!assert_one_arg(&NeverAvailable, Root::ZERO));
    }
}
