//! Fork choice. Populated in Phase 1 (CC-15–CC-17).
//!
//! CC-17 lands the data-availability seam in [`da_seam`]. Store, proto-array,
//! and `on_block` are owned by CC-15a / CC-15b.

pub mod da_seam;

pub use da_seam::{AlwaysAvailable, BlockImport, DataAvailability, DeferralReason, ImportedBlock};
