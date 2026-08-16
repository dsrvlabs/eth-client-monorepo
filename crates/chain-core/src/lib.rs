//! Consensus core: dedicated OS thread, import path, DA / engine requeue,
//! residency, event bus.
//!
//! S2-A-01: [`core.rs`](core.rs), [`import.rs`](import.rs), and
//! [`apply_attestations.rs`](apply_attestations.rs) live here (verbatim).
//! S2-A-02: [`da.rs`](da.rs), [`pending_engine.rs`](pending_engine.rs),
//! [`residency.rs`](residency.rs), and [`events/`](events/mod.rs) live here
//! (verbatim). `cc-chain` compiles them via `#[path]`. `main.rs` stays the
//! service binary until S2-A-03.
//!
//! S2-A-04: this crate names [`ArchiveWriteHandle`] (`Arc<dyn ArchiveWrite>`).
//! It never names a storage type. Ingest is S2-A-05.
//!
//! Unit tests compile the moved files against still-service-owned siblings
//! (not a rewrite of `crate::` paths). ADR-P1-09, ADR-P1-12, and ADR-P3-05
//! ride the moved sources unchanged — `pending_engine` stays a separate map.

#![cfg_attr(test, allow(dead_code, unreachable_pub, unused_imports))]

/// Archive ingest handle. Named here so chain-core never holds a storage type.
pub type ArchiveWriteHandle = std::sync::Arc<dyn cc_seam::ArchiveWrite>;

#[cfg(test)]
#[path = "apply_attestations.rs"]
mod apply_attestations;
#[cfg(test)]
#[path = "core.rs"]
mod core;
#[cfg(test)]
#[path = "da.rs"]
mod da;
#[cfg(test)]
#[path = "events/mod.rs"]
mod events;
#[cfg(test)]
#[path = "import.rs"]
mod import;
#[cfg(test)]
#[path = "pending_engine.rs"]
mod pending_engine;
#[cfg(test)]
#[path = "residency.rs"]
mod residency;

// Unmoved siblings — compiled here so `crate::` resolves. Files stay in
// `services/chain` until S2-A-03. Not a move.
#[cfg(test)]
#[path = "../../../services/chain/src/engine.rs"]
mod engine;
#[cfg(test)]
#[path = "../../../services/chain/src/epoch_context.rs"]
mod epoch_context;
#[cfg(test)]
#[path = "../../../services/chain/src/fcu_driver.rs"]
mod fcu_driver;
#[cfg(test)]
#[path = "../../../services/chain/src/head.rs"]
mod head;
#[cfg(test)]
#[path = "../../../services/chain/src/liveness.rs"]
mod liveness;
#[cfg(test)]
#[path = "../../../services/chain/src/metrics.rs"]
mod metrics;
#[cfg(test)]
#[path = "../../../services/chain/src/p2p_stream.rs"]
mod p2p_stream;
#[cfg(test)]
#[path = "../../../services/chain/src/restore.rs"]
mod restore;
#[cfg(test)]
#[path = "../../../services/chain/src/service.rs"]
mod service;
#[cfg(test)]
#[path = "../../../services/chain/src/tick.rs"]
mod tick;

#[cfg(test)]
mod archive_write_name {
    use super::ArchiveWriteHandle;

    #[test]
    fn names_arc_dyn_archive_write() {
        let _: Option<ArchiveWriteHandle> = None;
    }
}
