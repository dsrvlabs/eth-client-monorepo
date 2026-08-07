//! Data-availability seam (Architecture §6.5 / §8.3, CC-17 → CC-24d).
//!
//! The whole DA surface lives in this file so Phase 2 substitutes real PeerDAS
//! sampling by changing one body, not `on_block` or other fork-choice logic.
//! Symmetrical with the CC-14 engine seam.
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
//! **Sole production call site** (CC-15b / CC-24/4): top of `on_block`, before
//! the state transition. **Sole production implementation**: [`PeerDasAvailability`]
//! — a set-membership test against roots signalled available by the sampling
//! tracker (`DataAvailable` on the p2p stream). Phase 1's optimistic DA stub is
//! **deleted** (not feature-gated).
//!
//! # Two-process shape (Architecture §8.3)
//!
//! ```text
//! services/p2p                          │  services/chain
//! ──────────────────────────────────────┼───────────────────────────────────────────
//! sampling tracker completes root R     │
//!   → DataAvailable{root, slot} ────────┼──► p2p_stream → core command
//!                                       │      → PeerDasAvailability.mark_available
//!                                       │      → re-drive pending_da
//!                                       │
//!                                       │  is_data_available(r) // set lookup only
//! ```

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use cc_state_transition::GossipClass;
use cc_types::primitives::Root;

/// Spec-shaped data-availability seam (Architecture §6.5).
///
/// Object-safe: `dyn DataAvailability` is the store field type so CC-24 can
/// swap [`PeerDasAvailability`] without touching fork-choice logic.
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

/// Bound on the available-root set (Architecture §8.3 — peer-supplied roots).
///
/// Headroom for two epochs of slots plus sampling reordering; oldest-evicted
/// when full. Finalization pruning is the primary reclaim path.
pub const AVAILABLE_ROOTS_BOUND: usize = 256;

/// PeerDAS set-membership data-availability (Architecture §8.3 / CC-24d).
///
/// The **sole production** [`DataAvailability`] implementation. `is_data_available`
/// is a pure set lookup — no I/O, no consensus-structure lock, no network.
/// Roots enter the set when sampling completes (`DataAvailable` on the p2p
/// stream); the chain core calls [`Self::mark_available`].
///
/// Cheap to clone: all clones share the same bounded set via [`Arc`].
#[derive(Debug, Clone)]
pub struct PeerDasAvailability {
    inner: Arc<Mutex<AvailableSet>>,
}

#[derive(Debug)]
struct AvailableSet {
    /// Membership set (O(1) lookup).
    roots: HashSet<Root>,
    /// Insertion order for oldest-eviction when at capacity.
    order: VecDeque<Root>,
    /// Hard cap ([`AVAILABLE_ROOTS_BOUND`] by default).
    bound: usize,
}

impl PeerDasAvailability {
    /// Empty available set with the default bound.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(AVAILABLE_ROOTS_BOUND)
    }

    /// Empty available set with a custom bound (tests).
    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AvailableSet {
                roots: HashSet::new(),
                order: VecDeque::new(),
                bound: bound.max(1),
            })),
        }
    }

    /// Mark `root` data-available (sampling complete).
    ///
    /// Idempotent. When at capacity, the oldest root is evicted first.
    /// Returns `true` if this call newly inserted the root.
    pub fn mark_available(&self, root: Root) -> bool {
        let Ok(mut g) = self.inner.lock() else {
            return false;
        };
        if g.roots.contains(&root) {
            return false;
        }
        while g.roots.len() >= g.bound {
            if let Some(old) = g.order.pop_front() {
                g.roots.remove(&old);
            } else {
                break;
            }
        }
        g.roots.insert(root);
        g.order.push_back(root);
        true
    }

    /// Current occupancy of the available set (for gauges / tests).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map(|g| g.roots.len()).unwrap_or(0)
    }

    /// Whether the available set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `root` is currently marked available (same as the trait method).
    #[must_use]
    pub fn contains(&self, root: Root) -> bool {
        self.inner
            .lock()
            .map(|g| g.roots.contains(&root))
            .unwrap_or(false)
    }

    /// Drop roots that are no longer needed after finalization.
    ///
    /// `keep` is the set of roots that must remain (e.g. finalized checkpoint
    /// root and any still-pending imports). All other available roots are
    /// removed. Returns the number of roots pruned.
    pub fn prune_except(&self, keep: &HashSet<Root>) -> usize {
        let Ok(mut g) = self.inner.lock() else {
            return 0;
        };
        let before = g.roots.len();
        g.roots.retain(|r| keep.contains(r));
        let still: HashSet<Root> = g.roots.iter().copied().collect();
        g.order.retain(|r| still.contains(r));
        before.saturating_sub(g.roots.len())
    }

    /// Remove a single root (tests / explicit forget).
    pub fn remove(&self, root: Root) -> bool {
        let Ok(mut g) = self.inner.lock() else {
            return false;
        };
        if g.roots.remove(&root) {
            g.order.retain(|r| *r != root);
            true
        } else {
            false
        }
    }
}

impl Default for PeerDasAvailability {
    fn default() -> Self {
        Self::new()
    }
}

impl DataAvailability for PeerDasAvailability {
    fn is_data_available(&self, beacon_block_root: Root) -> bool {
        self.contains(beacon_block_root)
    }
}

/// Harness DA that admits every root.
///
/// **Not production.** Phase 1's optimistic production stub is deleted;
/// production wiring must construct [`PeerDasAvailability`] only. This type
/// exists so unit tests that do not exercise the PeerDAS gate can seed a
/// [`crate::store::Store`] without a sampling tracker (vector suite, head
/// cache, residency, …).
#[derive(Debug, Default, Clone, Copy)]
pub struct HarnessAvailability;

impl DataAvailability for HarnessAvailability {
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
    /// transition, then store write. Real `on_block` re-runs the same
    /// assertions against the production function.
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

    /// PeerDasAvailability admits a root only after `mark_available`.
    #[test]
    fn peer_das_imports_only_after_mark_available() {
        let da = PeerDasAvailability::new();
        let root = Root::from_array([0xcd; 32]);
        let transition_counter = AtomicU32::new(0);
        let mut blocks: HashMap<Root, ()> = HashMap::new();

        let outcome = on_block_with_da(&da, root, &transition_counter, &mut blocks);
        assert!(matches!(
            outcome,
            BlockImport::Deferred(DeferralReason::DataUnavailable)
        ));
        assert_eq!(transition_counter.load(Ordering::SeqCst), 0);

        assert!(da.mark_available(root));
        let outcome = on_block_with_da(&da, root, &transition_counter, &mut blocks);
        assert_eq!(outcome, BlockImport::Imported(ImportedBlock { root }));
        assert_eq!(transition_counter.load(Ordering::SeqCst), 1);
        assert!(blocks.contains_key(&root));
        assert_eq!(outcome.gossip_class(), None);
    }

    /// Set membership is independent of root bit patterns (empty → false).
    #[test]
    fn peer_das_empty_returns_false_for_any_root() {
        let da = PeerDasAvailability::new();
        assert!(!da.is_data_available(Root::ZERO));
        assert!(!da.is_data_available(Root::from_array([0xff; 32])));
    }

    /// Available set is bounded; oldest root is evicted.
    #[test]
    fn peer_das_available_set_is_bounded_oldest_evicted() {
        let da = PeerDasAvailability::with_bound(2);
        let r0 = Root::from_array([1; 32]);
        let r1 = Root::from_array([2; 32]);
        let r2 = Root::from_array([3; 32]);
        da.mark_available(r0);
        da.mark_available(r1);
        assert_eq!(da.len(), 2);
        da.mark_available(r2);
        assert_eq!(da.len(), 2);
        assert!(!da.contains(r0), "oldest must be evicted");
        assert!(da.contains(r1));
        assert!(da.contains(r2));
    }

    /// Finalization prune drops roots not in the keep set.
    #[test]
    fn peer_das_prune_except_keeps_only_named_roots() {
        let da = PeerDasAvailability::new();
        let keep_root = Root::from_array([9; 32]);
        let drop_root = Root::from_array([8; 32]);
        da.mark_available(keep_root);
        da.mark_available(drop_root);
        let mut keep = HashSet::new();
        keep.insert(keep_root);
        let pruned = da.prune_except(&keep);
        assert_eq!(pruned, 1);
        assert!(da.contains(keep_root));
        assert!(!da.contains(drop_root));
    }

    /// Clone shares the same set (core + store share one Arc).
    #[test]
    fn peer_das_clone_shares_set() {
        let a = PeerDasAvailability::new();
        let b = a.clone();
        let root = Root::from_array([0x11; 32]);
        a.mark_available(root);
        assert!(b.is_data_available(root));
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
        let da = PeerDasAvailability::new();
        da.mark_available(Root::ZERO);
        assert!(assert_one_arg(&da, Root::ZERO));
        assert!(!assert_one_arg(&NeverAvailable, Root::ZERO));
    }

    /// No I/O / no network: mark + lookup complete without any async runtime.
    #[test]
    fn is_data_available_is_set_membership_no_io() {
        let da = PeerDasAvailability::new();
        let root = Root::from_array([0x22; 32]);
        assert!(!da.is_data_available(root));
        da.mark_available(root);
        assert!(da.is_data_available(root));
    }
}
