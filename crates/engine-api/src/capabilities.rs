//! Advertised capability list and response cache (CC-31 / Architecture §3.3).
//!
//! The list is a five-entry `const` asserted element-for-element in tests.
//! Never advertise a method we do not implement: geth's `ExchangeCapabilities`
//! has a side effect on `engine_getBlobsV4` — it calls
//! `BlobCache().SetCellMode(true)`, documented in-source as a one-directional
//! toggle that assumes the CL will not fall back to getBlobsV3 again.
//! Advertising V4 without implementing it would put the EL blob cache in a
//! mode our V2 client does not expect; the symptom is a fast path that
//! mysteriously misses three milestones later (CC-37).

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Methods this client actually implements, and therefore the entire content of
/// our `engine_exchangeCapabilities` request. Version-suffixed per common.md;
/// `engine_exchangeCapabilities` itself MUST NOT appear (it is unversioned).
///
/// DO NOT ADD A METHOD HERE BEFORE IT IS IMPLEMENTED. geth reads *our* list for
/// `engine_getBlobsV4` and calls `BlobCache().SetCellMode(true)` on a hit — a
/// toggle its own source documents as one-directional. Advertising a method we
/// do not implement changes the EL's blob cache behaviour for the lifetime of
/// the process, and the symptom is a fast path that mysteriously misses.
pub const ADVERTISED_CAPABILITIES: [&str; 5] = [
    "engine_newPayloadV4",
    "engine_forkchoiceUpdatedV3",
    "engine_getBlobsV2",
    "eth_syncing",
    "eth_chainId",
];

/// JSON-RPC name of the method we **discover** but never dispatch (OQ-P3-2 / CC-3E).
pub const GET_BLOBS_V3: &str = "engine_getBlobsV3";

/// Capability names that must be present on a live EL for Phase 3 operation.
pub const REQUIRED_CAPABILITIES: [&str; 3] = [
    "engine_newPayloadV4",
    "engine_forkchoiceUpdatedV3",
    "engine_getBlobsV2",
];

/// Snapshot of the EL's half of `engine_exchangeCapabilities`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilitySnapshot {
    /// Methods the EL reported supporting.
    pub methods: BTreeSet<String>,
    /// Whether `engine_getBlobsV3` was present (recorded, never used).
    pub get_blobs_v3_present: bool,
    /// Instant the handshake completed (for cache age).
    pub obtained_at: Instant,
}

impl CapabilitySnapshot {
    /// Build from the EL's capability array.
    #[must_use]
    pub fn from_el_methods(methods: impl IntoIterator<Item = String>) -> Self {
        let methods: BTreeSet<String> = methods.into_iter().collect();
        let get_blobs_v3_present = methods.iter().any(|m| m == GET_BLOBS_V3);
        Self {
            methods,
            get_blobs_v3_present,
            obtained_at: Instant::now(),
        }
    }

    /// Age of this snapshot.
    #[must_use]
    pub fn age(&self) -> Duration {
        self.obtained_at.elapsed()
    }

    /// Whether the EL advertised `method`.
    #[must_use]
    pub fn supports(&self, method: &str) -> bool {
        self.methods.iter().any(|m| m == method)
    }
}

/// Cached EL capability response.
///
/// Refreshed at startup and on the not-Synced → Synced edge. **Cleared** on
/// auth failure and on the transition to Offline — *"as it is likely the
/// engine is being updated to a newer version which might also have new
/// capabilities."*
#[derive(Debug, Default)]
pub struct CapabilityCache {
    inner: Mutex<Option<CapabilitySnapshot>>,
}

impl CapabilityCache {
    /// Empty cache (startup default).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    /// Replace the cache with a fresh snapshot.
    ///
    /// On mutex poison (prior panic while held), recovers the inner state and
    /// still stores — fail-forward so a transient poison cannot leave a stale
    /// snapshot after a successful handshake.
    pub fn store(&self, snapshot: CapabilitySnapshot) {
        match self.inner.lock() {
            Ok(mut guard) => *guard = Some(snapshot),
            Err(poisoned) => {
                *poisoned.into_inner() = Some(snapshot);
            }
        }
    }

    /// Clear the cache (auth failure / Offline edges).
    ///
    /// On mutex poison, still forces `None` via `into_inner` — clear must not
    /// silently no-op after an incident (auth/offline edges).
    pub fn clear(&self) {
        match self.inner.lock() {
            Ok(mut guard) => *guard = None,
            Err(poisoned) => {
                *poisoned.into_inner() = None;
            }
        }
    }

    /// Whether the cache currently holds a snapshot.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self.inner.lock() {
            Ok(g) => g.is_none(),
            // Poisoned + non-empty would be a lie if we reported empty; recover
            // and read so emptiness tracks the real value after poison.
            Err(poisoned) => poisoned.into_inner().is_none(),
        }
    }

    /// Clone the current snapshot, if any.
    #[must_use]
    pub fn get(&self) -> Option<CapabilitySnapshot> {
        match self.inner.lock() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Whether the EL advertised `engine_getBlobsV3` (discovery record).
    #[must_use]
    pub fn get_blobs_v3_discovered(&self) -> Option<bool> {
        self.get().map(|s| s.get_blobs_v3_present)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    /// `CC-31` /1: five entries, exact element order and length; exchangeCapabilities absent.
    #[test]
    fn advertised_capabilities_exact() {
        assert_eq!(ADVERTISED_CAPABILITIES.len(), 5);
        assert_eq!(
            ADVERTISED_CAPABILITIES,
            [
                "engine_newPayloadV4",
                "engine_forkchoiceUpdatedV3",
                "engine_getBlobsV2",
                "eth_syncing",
                "eth_chainId",
            ]
        );
        // Element-for-element (not merely contains).
        assert_eq!(ADVERTISED_CAPABILITIES[0], "engine_newPayloadV4");
        assert_eq!(ADVERTISED_CAPABILITIES[1], "engine_forkchoiceUpdatedV3");
        assert_eq!(ADVERTISED_CAPABILITIES[2], "engine_getBlobsV2");
        assert_eq!(ADVERTISED_CAPABILITIES[3], "eth_syncing");
        assert_eq!(ADVERTISED_CAPABILITIES[4], "eth_chainId");

        assert!(
            !ADVERTISED_CAPABILITIES.contains(&"engine_exchangeCapabilities"),
            "engine_exchangeCapabilities is unversioned and MUST NOT appear"
        );
    }

    /// Negative assertion: `engine_getBlobsV4` must never be advertised.
    ///
    /// Failure message quotes geth's SetCellMode(true) one-directional toggle
    /// comment so the person who trips it reads the reason.
    #[test]
    fn never_advertise_get_blobs_v4() {
        // Built at runtime so static greps for the forbidden wire name stay
        // limited to comments + this assertion's diagnostic (CC-31 /1).
        let forbidden = format!("engine_getBlobsV{}", 4);
        let has_v4 = ADVERTISED_CAPABILITIES.contains(&forbidden.as_str());
        assert!(
            !has_v4,
            "must not advertise {forbidden}: geth calls BlobCache().SetCellMode(true) \
             on a hit — \"a one-directional toggle, which assumes that once the CL supports \
             getBlobsV4, it will not fall back to getBlobsV3 again\". Advertising V4 without \
             implementing it changes EL blob-cache mode for the process lifetime."
        );
    }

    #[test]
    fn capability_cache_clear_empties() {
        let cache = CapabilityCache::new();
        assert!(cache.is_empty());
        cache.store(CapabilitySnapshot::from_el_methods([
            "engine_newPayloadV4".into(),
            GET_BLOBS_V3.into(),
        ]));
        assert!(!cache.is_empty());
        assert_eq!(cache.get_blobs_v3_discovered(), Some(true));
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.get_blobs_v3_discovered(), None);
    }
}
