//! `SubscriptionSet` producer — first consumer of CC-21 /6 `set_custody_group_count`.
//!
//! Sends on `EngineHello` and on **every** cgc change so engine's filter
//! (ADR P3-07) tracks a subscription set that actually moves in production.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use cc_proto::p2p::SubscriptionSet as WireSubscriptionSet;
use tokio::sync::watch;

/// Local custody-sampled column indices this node publishes (ADR P3-07).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocalSubscription {
    /// Column indices (sorted unique).
    pub column_indices: BTreeSet<u64>,
    /// Custody group count that produced this set.
    pub cgc: u64,
}

impl LocalSubscription {
    /// Empty set (fail-closed: publish nothing).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// From an iterator of column indices + cgc.
    #[must_use]
    pub fn from_indices(indices: impl IntoIterator<Item = u64>, cgc: u64) -> Self {
        Self {
            column_indices: indices.into_iter().collect(),
            cgc,
        }
    }

    /// Number of subscribed column indices.
    #[must_use]
    pub fn len(&self) -> usize {
        self.column_indices.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.column_indices.is_empty()
    }

    /// Whether `column_index` is subscribed.
    #[must_use]
    pub fn is_subscribed(&self, column_index: u64) -> bool {
        self.column_indices.contains(&column_index)
    }
}

/// Shared subscription state + watch channel for stream sessions.
#[derive(Debug, Clone)]
pub struct SubscriptionHandle {
    inner: Arc<Mutex<LocalSubscription>>,
    tx: watch::Sender<LocalSubscription>,
}

impl SubscriptionHandle {
    /// Construct with an initial set (usually empty until custody is known).
    #[must_use]
    pub fn new(initial: LocalSubscription) -> Self {
        let (tx, _rx) = watch::channel(initial.clone());
        Self {
            inner: Arc::new(Mutex::new(initial)),
            tx,
        }
    }

    /// Current subscription snapshot.
    #[must_use]
    pub fn current(&self) -> LocalSubscription {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Replace the set and notify every stream session (cgc change / hello rebuild).
    pub fn set(&self, next: LocalSubscription) {
        {
            let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            *g = next.clone();
        }
        let _ = self.tx.send(next);
    }

    /// Subscribe to changes (each `EngineStream` session holds one receiver).
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<LocalSubscription> {
        self.tx.subscribe()
    }
}

/// Build a wire [`WireSubscriptionSet`] from column indices + cgc.
#[must_use]
pub fn subscription_to_wire(local: &LocalSubscription) -> WireSubscriptionSet {
    WireSubscriptionSet {
        column_indices: local.column_indices.iter().copied().collect(),
        cgc: local.cgc,
    }
}

/// Convenience: column indices from a sampled set of group/column ids.
#[must_use]
pub fn subscription_set_from_columns(
    columns: impl IntoIterator<Item = u64>,
    cgc: u64,
) -> LocalSubscription {
    LocalSubscription::from_indices(columns, cgc)
}

/// Wraps a [`crate::service::CgcHookInvoker`] so every successful
/// `set_custody_group_count` also refreshes the engine `SubscriptionSet`
/// (CC-38b — first consumer of CC-21 /6).
///
/// Callers supply a `rebuild` closure that maps the new `cgc` to the
/// sampled column indices (typically via [`crate::das::CustodyManager`]).
pub struct CgcSubscriptionBridge<I, F>
where
    I: crate::service::CgcHookInvoker,
    F: Fn(u64) -> LocalSubscription + Send + Sync,
{
    /// Underlying five-effect hook invoker.
    pub inner: I,
    /// Subscription handle notified after each successful cgc change.
    pub subscription: SubscriptionHandle,
    /// Rebuild the subscription from the new cgc.
    pub rebuild: F,
}

impl<I, F> std::fmt::Debug for CgcSubscriptionBridge<I, F>
where
    I: crate::service::CgcHookInvoker + std::fmt::Debug,
    F: Fn(u64) -> LocalSubscription + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CgcSubscriptionBridge")
            .field("inner", &self.inner)
            .field("subscription", &self.subscription.current())
            .finish_non_exhaustive()
    }
}

impl<I, F> crate::service::CgcHookInvoker for CgcSubscriptionBridge<I, F>
where
    I: crate::service::CgcHookInvoker,
    F: Fn(u64) -> LocalSubscription + Send + Sync,
{
    fn set_custody_group_count(
        &self,
        n: u64,
    ) -> Result<(), crate::discovery::cgc_hook::CgcHookError> {
        self.inner.set_custody_group_count(n)?;
        let next = (self.rebuild)(n);
        self.subscription.set(next);
        Ok(())
    }
}
