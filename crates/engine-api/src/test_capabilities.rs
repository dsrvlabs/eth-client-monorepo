//! Test-only capability cache so moved `state.rs` keeps `crate::capabilities`.
//!
//! Not `services/engine/src/capabilities.rs` (S1-A-03).

use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilitySnapshot {
    methods: Vec<String>,
}

impl CapabilitySnapshot {
    #[must_use]
    pub fn from_el_methods(methods: impl IntoIterator<Item = String>) -> Self {
        Self {
            methods: methods.into_iter().collect(),
        }
    }
}

#[derive(Debug, Default)]
pub struct CapabilityCache {
    inner: Mutex<Option<CapabilitySnapshot>>,
}

impl CapabilityCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    pub fn store(&self, snapshot: CapabilitySnapshot) {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner()) = Some(snapshot);
    }

    pub fn clear(&self) {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
    }
}
