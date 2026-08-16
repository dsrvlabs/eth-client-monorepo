//! Epoch-scoped snapshot published by the core thread (Architecture §16/4, ADR-P1-09).
//!
//! Phase 1's [`crate::head::HeadSnapshot`] carries checkpoints and roots but **not**
//! `proposer_lookahead` or index→pubkey (its `PubkeyIndexMap` is the wrong
//! direction). Both change per *epoch*, not per import, so they ride a second
//! `ArcSwap` rather than widening the per-import head snapshot.
//!
//! The [`crate::p2p_stream`] `ChainView` producer reads this store **without
//! touching the command channel** — the same lock-free property as `GetHead`.

use std::sync::Arc;

use arc_swap::ArcSwap;
use cc_types::primitives::{Epoch, Root};

/// Immutable per-epoch view published at each epoch boundary (and at bootstrap).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochContext {
    /// Epoch this context describes.
    pub epoch: Epoch,
    /// `(MIN_SEED_LOOKAHEAD + 1) × SLOTS_PER_EPOCH` proposer indices.
    pub proposer_lookahead: Vec<u64>,
    /// BLS pubkeys parallel to [`Self::proposer_lookahead`].
    pub proposer_pubkeys: Vec<Vec<u8>>,
    /// Active validator count at `epoch` (for P2 rate derivations, §5.6).
    pub active_validator_count: u64,
    /// Genesis time (unix seconds) — stable, carried so `ChainView` is self-contained.
    pub genesis_time: u64,
    /// Genesis validators root.
    pub genesis_validators_root: Root,
    /// Slot duration in seconds (from [`cc_types::config::ChainConfig`]).
    pub seconds_per_slot: u64,
    /// Slots per epoch (preset constant, for epoch derivation from head slot).
    pub slots_per_epoch: u64,
    /// Monotonic publish sequence (core-thread producer only).
    pub sequence: u64,
}

impl Default for EpochContext {
    fn default() -> Self {
        Self {
            epoch: Epoch::new(0),
            proposer_lookahead: Vec::new(),
            proposer_pubkeys: Vec::new(),
            active_validator_count: 0,
            genesis_time: 0,
            genesis_validators_root: Root::ZERO,
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            sequence: 0,
        }
    }
}

/// Shared epoch context: core thread writes, `ChainView` producer reads.
#[derive(Debug, Clone)]
pub struct EpochContextStore {
    inner: Arc<ArcSwap<EpochContext>>,
}

impl EpochContextStore {
    /// Empty context (pre-bootstrap / before first publish).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(EpochContext::default())),
        }
    }

    /// Seed from an initial context (tests / post-bootstrap).
    pub fn with_context(ctx: EpochContext) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(ctx)),
        }
    }

    /// Clone of the `ArcSwap` handle.
    pub fn arc_swap(&self) -> Arc<ArcSwap<EpochContext>> {
        Arc::clone(&self.inner)
    }

    /// Pointer load — no core-thread interaction.
    pub fn load(&self) -> Arc<EpochContext> {
        self.inner.load_full()
    }

    /// Publish a new context (core thread only).
    pub fn store(&self, ctx: EpochContext) {
        self.inner.store(Arc::new(ctx));
    }
}

impl Default for EpochContextStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn load_returns_published_context() {
        let store = EpochContextStore::new();
        assert_eq!(store.load().sequence, 0);
        store.store(EpochContext {
            epoch: Epoch::new(3),
            active_validator_count: 64,
            sequence: 1,
            ..EpochContext::default()
        });
        let ctx = store.load();
        assert_eq!(ctx.epoch.as_u64(), 3);
        assert_eq!(ctx.active_validator_count, 64);
        assert_eq!(ctx.sequence, 1);
    }
}
