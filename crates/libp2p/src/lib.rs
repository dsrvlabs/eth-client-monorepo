//! Git-pinned libp2p edge and transport/behaviour composition.
//!
//! Skeleton only (CC-2K). Content lands in CC-20a.

/// 40-hex git rev of `https://github.com/libp2p/rust-libp2p` from root
/// `[workspace.dependencies]`. Greppable OQ-7 compensating control (CC-20/4):
/// must match `Cargo.toml`, `docs/p2p-dependencies.md`, and the supply-chain
/// Phase 2 section. Enforced by `scripts/check-crate-dag.sh`.
pub const LIBP2P_GIT_REV: &str = "6348a0be4aeb5b48eecf17a5d0aae15ff8239984";

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
