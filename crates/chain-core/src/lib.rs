//! Consensus core: dedicated OS thread, import path, DA / engine requeue,
//! residency, event bus, and the S2-A-03 host surfaces.
//!
//! S2-A-01: [`core.rs`](core.rs), [`import.rs`](import.rs), and
//! [`apply_attestations.rs`](apply_attestations.rs).
//! S2-A-02: [`da.rs`](da.rs), [`pending_engine.rs`](pending_engine.rs),
//! [`residency.rs`](residency.rs), and [`events/`](events/mod.rs).
//! S2-A-03: remaining crate siblings live here. `services/chain` is a thin
//! shim over this crate so the previous topology stays runnable for A/B
//! (`[ARCH]` §9.1). Restore stays (S2-J-02 deletes it).
//! `checkpoint_sync` stays in `cc-chain` (HTTP grandfather; this crate is
//! not HTTP- or JWT-grandfathered).
//!
//! S2-A-04: this crate names [`ArchiveWriteHandle`] (`Arc<dyn ArchiveWrite>`).
//! It never names a storage type.
//! S2-A-05: [`ingest.rs`](ingest.rs) decodes a sidecar into a typed
//! `ColumnBatch` and calls `ArchiveWrite::ingest_columns`. Column bytes
//! do not enter the ring.
//! S2-A-06: the batch head is `(parent_root, slot)`. A batch may only
//! extend the durable frontier, never jump it.
//! S2-A-07: archive ingest Backpressure is Policy A (ADR-R-02). The
//! `p2p_stream` caller must not map it to `Acceptance::Ignore`.
//! S2-A-09: the events ring is demoted to API/observer (`SubscribeEvents`).
//! Bounds, cursor semantics, and policy B are unchanged.
//!
//! ADR-P1-09, ADR-P1-12, and ADR-P3-05 ride the moved sources unchanged —
//! `pending_engine` stays a separate map from `pending_da`.

#![cfg_attr(test, allow(dead_code, unreachable_pub, unused_imports))]

/// Archive ingest handle. Named here so chain-core never holds a storage type.
pub type ArchiveWriteHandle = std::sync::Arc<dyn cc_seam::ArchiveWrite>;

mod ingest;
pub use ingest::{decode_column_batch, ingest_column_ssz};

pub mod apply_attestations;
pub mod core;
pub mod da;
pub mod engine;
pub mod epoch_context;
pub mod events;
pub mod fcu_driver;
pub mod head;
pub mod import;
pub mod invalidation;
pub mod liveness;
pub mod metrics;
pub mod p2p_stream;
pub mod pending_engine;
pub mod residency;
pub mod restore;
pub mod service;
pub mod tick;

#[cfg(test)]
mod archive_write_name {
    use super::ArchiveWriteHandle;

    #[test]
    fn names_arc_dyn_archive_write() {
        let _: Option<ArchiveWriteHandle> = None;
    }
}
