//! S2-A-13 proto-free in-process boot of `bin/beacon-core`'s boot path.
//!
//! Same sequence as `bin/beacon-core/src/boot.rs` `boot_in_process`
//! (`open` → `durable_set`) against one `TempDir` / one redb. No gRPC, no
//! `tonic`, no `cc-proto`. Writer start stays on `cc-storage-core` (S2-J-01).
//! S2-A-14 owns import → durable; this crate does not import a block.

pub mod boot;

pub use boot::{BootConfig, BootPhase, Booted, boot_in_process};
