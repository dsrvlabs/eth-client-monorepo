//! Cell-KZG trait and backends (Architecture §4.2–4.3, CC-11b).
//!
//! - [`CellKzg`]: EIP-7594 cell API normalised to `c-kzg`'s contract
//! - [`setup`]: loads the committed `trusted_setup.json` for both backends
//! - [`c_kzg`]: backend A (`c-kzg` 2.1.8)
//! - [`rust_eth_kzg`]: backend B stub until CC-11c

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
pub use rust_eth_kzg::RustEthKzgBackend;

// ---------------------------------------------------------------------------
// DefaultKzg — static dispatch when exactly one backend feature is on
// ---------------------------------------------------------------------------

/// Default KZG backend when exactly one of `kzg-c-kzg` / `kzg-rust-eth-kzg` is
/// enabled (Architecture §4.3). When both features are on, consumers hold
/// `Arc<dyn CellKzg>` chosen by config; this alias is not defined.
#[cfg(all(feature = "kzg-c-kzg", not(feature = "kzg-rust-eth-kzg")))]
pub type DefaultKzg = CKzgBackend;

/// Default KZG backend when only `kzg-rust-eth-kzg` is enabled.
#[cfg(all(feature = "kzg-rust-eth-kzg", not(feature = "kzg-c-kzg")))]
pub type DefaultKzg = RustEthKzgBackend;
