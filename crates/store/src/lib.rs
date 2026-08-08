//! Persistent store library (Phase 4 store DAG).
//!
//! - [`engine`] — twelve-method concrete `Engine` (CC-40b / Architecture §1.1)
//! - [`keys`] — big-endian fixed-width codecs + shard arithmetic (§2.1 / §2.4)
//! - [`buckets`] — histogram boundaries shared with metrics and `bin/store-bench` (CC-4Ca)
//! - [`meta`] — ten SSZ singleton records (§2.5 / CC-40a)
//! - [`schema`] — schema version, config digest, table registry, open-or-refuse (CC-40a)
<<<<<<< Updated upstream
//! - [`window`] — computed block serve-window floor (CC-4A / Architecture §5.1)
=======
//! - [`invariants`] — §2.7 eight named checks at open and after passes (CC-4H)
>>>>>>> Stashed changes
//!
//! This crate depends only on `cc-types` among workspace members; consensus
//! containers must not appear here (opaque bytes under typed keys).

#![allow(missing_docs)]

/// Histogram bucket boundaries for the storage metric surface (§10.2 / CC-4Ca).
pub mod buckets;
/// Concrete engine seam (CC-40/3: engine crate name only under `engine/`).
pub mod engine;
/// §2.7 store invariants (CC-4H).
pub mod invariants;
/// Key codecs and shard arithmetic.
pub mod keys;
/// SSZ meta singleton records (Architecture §2.5).
pub mod meta;
/// Schema version, config digest, table registry, [`schema::Store::open`].
pub mod schema;
/// Computed block serve-window floor and vestigial-field cross-check (CC-4A).
pub mod window;

pub use engine::{
    Batch, Durability, Engine, EngineOptions, MAX_BATCH_OPS, MAX_INTERNED_TABLE_NAMES,
    MAX_RANGE_BYTES, MAX_RANGE_ENTRIES, RangeIter, ReadTxn, StoreError, db_file_path,
};
pub use invariants::{
    CountingSink, DEFAULT_SNAPSHOT_RING, FanoutSink, InvariantCheckMode, InvariantContext,
    InvariantSink, InvariantViolation, MAX_CONTIG_WALK_SLOTS, MAX_RING_SCAN_ROWS, StoreInvariant,
    TracingSink, check_invariants, run_invariant_checks_if_enabled,
};
pub use schema::{
    BLOCK_SHARD_WIDTH_EPOCHS, COLUMN_SHARD_WIDTH_EPOCHS, ConfigDigestInput, FIXED_TABLES,
    SCHEMA_VERSION, Store, StoreOpenOptions, compute_config_digest, is_registered_table,
    parse_shard_table,
};
pub use window::{
    BlockServeWindowCfg, WindowConfigError, check_min_epochs_for_block_requests,
    compute_min_epochs_for_block_requests,
};

// Re-export key primitives so `bin/store-bench` (DAG: cc-store only) can build keys.
pub use cc_types::{Epoch, Root, Slot};

// Meta SSZ encode surface for integration tests / services writing singletons.
pub use ssz::{Decode as SszDecode, Encode as SszEncode};
