//! Git-pinned libp2p edge and transport/behaviour composition.
//!
//! Skeleton only (CC-2K). Content lands in CC-20a.
//!
//! **libp2p reexports:** only this crate may declare `libp2p*` dependencies
//! (CC-20/1). Downstream crates (e.g. `cc-p2p`) take libp2p surfaces through
//! these reexports — never via a direct `libp2p*` Cargo dependency.

/// 40-hex git rev of `https://github.com/libp2p/rust-libp2p` from root
/// `[workspace.dependencies]`. Greppable OQ-7 compensating control (CC-20/4):
/// must match `Cargo.toml`, `docs/p2p-dependencies.md`, and the supply-chain
/// Phase 2 section. Enforced by `scripts/check-crate-dag.sh`.
pub const LIBP2P_GIT_REV: &str = "6348a0be4aeb5b48eecf17a5d0aae15ff8239984";

/// `libp2p-metrics` [`Metrics`] type for the P2P service sub-registry
/// (CC-29a / §3.4). Minimal reexport so `services/p2p` never depends on
/// `libp2p*` crates directly (full `libp2p::metrics` module is not reexported).
pub use libp2p::metrics::Metrics;

#[cfg(test)]
mod tests {
    use super::LIBP2P_GIT_REV;

    #[test]
    fn libp2p_git_rev_is_40_lowercase_hex() {
        assert_eq!(LIBP2P_GIT_REV.len(), 40);
        assert!(
            LIBP2P_GIT_REV
                .chars()
                .all(|c| matches!(c, '0'..='9' | 'a'..='f')),
            "LIBP2P_GIT_REV must be lowercase 40-hex, got {LIBP2P_GIT_REV:?}"
        );
    }
}
