//! Consensus types (SSZ containers, identifiers). Populated in Phase 1 (CC-10).

/// Deliberate clippy violation (needless_clone) for CC-07b gate proof.
pub fn demo_clippy_needless_clone(s: String) -> String {
    s.clone()
}
