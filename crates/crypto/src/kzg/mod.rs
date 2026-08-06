//! Cell-KZG trait and backends (Architecture §4.2–4.3, CC-11b / CC-11c / CC-11d).
//!
//! - [`CellKzg`]: EIP-7594 cell API normalised to `c-kzg`'s contract
//! - [`setup`]: loads the committed `trusted_setup.json` for both backends
//! - [`c_kzg`]: backend A (`c-kzg` 2.1.8)
//! - [`rust_eth_kzg`]: backend B (`rust_eth_kzg` 0.10.0) with both normalisations
//! - [`KzgBackendKind`]: config enum whose [`Default`] is the CC-11d measurement choice

mod r#trait;

pub use r#trait::{Blob, CellKzg, CellProofs, Cells, CellsAndProofs, KzgError, BYTES_PER_BLOB};

pub mod setup;

#[cfg(feature = "kzg-c-kzg")]
pub mod c_kzg;

#[cfg(feature = "kzg-c-kzg")]
pub use c_kzg::CKzgBackend;

#[cfg(feature = "kzg-rust-eth-kzg")]
pub mod rust_eth_kzg;

#[cfg(feature = "kzg-rust-eth-kzg")]
pub use rust_eth_kzg::{RustEthKzgBackend, UsePrecomp, DEFAULT_USE_PRECOMP};

// ---------------------------------------------------------------------------
// KzgBackendKind — config selection (Architecture §4.3, CC-11d)
// ---------------------------------------------------------------------------

/// Which cell-KZG backend to construct when both features are compiled.
///
/// Consumed by `services/chain` at CC-18b (`Arc<dyn CellKzg>`). The
/// [`Default`] value **is** the CC-11d measurement choice and must match the
/// crate's `default` feature — see `docs/kzg-benchmark.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KzgBackendKind {
    /// `c-kzg` 2.1.8 (feature `kzg-c-kzg`).
    CKzg,
    /// `rust_eth_kzg` 0.10.0 (feature `kzg-rust-eth-kzg`).
    RustEthKzg,
}

impl KzgBackendKind {
    /// Stable string used in docs and config.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CKzg => "c-kzg",
            Self::RustEthKzg => "rust_eth_kzg",
        }
    }
}

impl Default for KzgBackendKind {
    /// Chosen default from CC-11d (`docs/kzg-benchmark.md`). Keep in lockstep
    /// with `Cargo.toml` `[features] default` and the unit test in
    /// `tests/kzg.rs`.
    fn default() -> Self {
        // CHOSEN_DEFAULT: updated by CC-11d after the benchmark matrix.
        Self::CKzg
    }
}

impl std::fmt::Display for KzgBackendKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// DefaultKzg — static dispatch when exactly one backend feature is on
// ---------------------------------------------------------------------------

/// Default KZG backend when exactly one of `kzg-c-kzg` / `kzg-rust-eth-kzg` is
/// enabled (Architecture §4.3). When both features are on, consumers hold
/// `Arc<dyn CellKzg>` chosen by [`KzgBackendKind`]; this alias is not defined.
#[cfg(all(feature = "kzg-c-kzg", not(feature = "kzg-rust-eth-kzg")))]
pub type DefaultKzg = CKzgBackend;

/// Default KZG backend when only `kzg-rust-eth-kzg` is enabled.
#[cfg(all(feature = "kzg-rust-eth-kzg", not(feature = "kzg-c-kzg")))]
pub type DefaultKzg = RustEthKzgBackend;
