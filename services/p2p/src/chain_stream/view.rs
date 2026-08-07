//! In-process `ChainView` publication (Architecture §10.2, ADR P2-05).
//!
//! One writer (the stream client), many readers (slot clock, Status, column
//! validator, gap detector). Do not add a second head poller — §16/3.

use std::sync::Arc;

use arc_swap::ArcSwap;
use cc_proto::p2p::ChainView;

/// View-kind: slot tick (fields 1–8).
pub const VIEW_KIND_SLOT_TICK: u64 = 1;
/// View-kind: epoch tick (fields 1–8 + 11–13).
pub const VIEW_KIND_EPOCH_TICK: u64 = 2;
/// View-kind: head change (fields 1–8).
pub const VIEW_KIND_HEAD_CHANGE: u64 = 3;
/// View-kind: full snapshot on `StreamHello` / reconnect (fields 1–13).
pub const VIEW_KIND_FULL: u64 = 4;

/// Shared chain view: stream client writes, consumers pointer-load.
#[derive(Debug, Clone)]
pub struct ChainViewStore {
    inner: Arc<ArcSwap<ChainView>>,
}

impl ChainViewStore {
    /// Empty view (pre-connect / before first `ChainView`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(ChainView::default())),
        }
    }

    /// Seed from an initial view (tests).
    #[must_use]
    pub fn with_view(view: ChainView) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(view)),
        }
    }

    /// Clone of the `ArcSwap` handle (shares identity).
    #[must_use]
    pub fn arc_swap(&self) -> Arc<ArcSwap<ChainView>> {
        Arc::clone(&self.inner)
    }

    /// Pointer load — never dials chain.
    #[must_use]
    pub fn load(&self) -> Arc<ChainView> {
        self.inner.load_full()
    }

    /// Publish a new view (stream client only — single writer).
    pub fn store(&self, view: ChainView) {
        self.inner.store(Arc::new(view));
    }

    /// True when at least one non-default view has been published.
    #[must_use]
    pub fn has_view(&self) -> bool {
        let v = self.load();
        v.view_kind != 0 || v.genesis_time != 0 || !v.head_root.is_empty()
    }
}

impl Default for ChainViewStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn single_writer_readers_see_update() {
        let store = ChainViewStore::new();
        assert!(!store.has_view());
        store.store(ChainView {
            slot: 42,
            head_slot: 42,
            genesis_time: 1_600_000_000,
            view_kind: VIEW_KIND_FULL,
            ..ChainView::default()
        });
        let a = store.load();
        let b = store.load();
        assert_eq!(a.slot, 42);
        assert_eq!(b.head_slot, 42);
        assert!(store.has_view());
    }
}
