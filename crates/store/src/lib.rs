//! Persistent store library (Phase 4 store DAG).
//!
//! - [`engine`] — twelve-method concrete `Engine` (CC-40b / Architecture §1.1)
//! - [`keys`] — big-endian fixed-width codecs + shard arithmetic (§2.1 / §2.4)
//! - [`buckets`] — histogram boundaries shared with metrics and `bin/store-bench` (CC-4Ca)
//!
//! Schema / meta records land in CC-40a on top of this seam. This crate depends
//! only on `cc-types` among workspace members; consensus containers must not
//! appear here (opaque bytes under typed keys).

#![allow(missing_docs)]

/// Histogram bucket boundaries for the storage metric surface (§10.2 / CC-4Ca).
pub mod buckets;
/// Concrete engine seam (CC-40/3: engine crate name only under `engine/`).
pub mod engine;
/// Key codecs and shard arithmetic.
pub mod keys;

pub use engine::{
    Batch, Durability, Engine, EngineOptions, MAX_BATCH_OPS, MAX_INTERNED_TABLE_NAMES,
    MAX_RANGE_BYTES, MAX_RANGE_ENTRIES, RangeIter, ReadTxn, StoreError, db_file_path,
};

// Re-export key primitives so `bin/store-bench` (DAG: cc-store only) can build keys.
pub use cc_types::{Epoch, Root, Slot};
