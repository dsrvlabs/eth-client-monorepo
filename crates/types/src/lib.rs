//! Consensus types (SSZ containers, identifiers). Populated in Phase 1 (CC-10).

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    /// Deliberate failing test for CC-07b gate proof only (R-9: not for main/develop).
    #[test]
    fn demo_failing_test_cc07b() {
        assert_eq!(2 + 2, 5, "throwaway: deliberate failure for gate proof");
    }
}
