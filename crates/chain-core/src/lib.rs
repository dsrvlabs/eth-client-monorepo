//! Consensus core: dedicated OS thread, import path, `ApplyAttestations`.
//!
//! S2-A-01: [`core.rs`](core.rs), [`import.rs`](import.rs), and
//! [`apply_attestations.rs`](apply_attestations.rs) live here (verbatim).
//! `cc-chain` compiles them via `#[path]` so DA / pending_engine / residency /
//! events stay owned by `cc-chain` until S2-A-02.
//!
//! Unit tests under this crate compile those three files against the still-
//! service-owned siblings (not a rewrite of `crate::` paths). ADR-P1-09,
//! ADR-P1-12, and ADR-P3-05 ride the moved sources unchanged.

#![cfg_attr(test, allow(dead_code, unreachable_pub, unused_imports))]

#[cfg(test)]
#[path = "apply_attestations.rs"]
mod apply_attestations;
#[cfg(test)]
#[path = "core.rs"]
mod core;
#[cfg(test)]
#[path = "import.rs"]
mod import;

// Unmoved siblings — compiled here so `crate::` resolves. Files stay in
// `services/chain` until S2-A-02 / A-03. Not a move.
#[cfg(test)]
#[path = "../../../services/chain/src/da.rs"]
mod da;
#[cfg(test)]
#[path = "../../../services/chain/src/engine_client.rs"]
mod engine_client;
#[cfg(test)]
#[path = "../../../services/chain/src/epoch_context.rs"]
mod epoch_context;
#[cfg(test)]
#[path = "../../../services/chain/src/events/mod.rs"]
mod events;
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
#[path = "../../../services/chain/src/pending_engine.rs"]
mod pending_engine;
#[cfg(test)]
#[path = "../../../services/chain/src/residency.rs"]
mod residency;
#[cfg(test)]
#[path = "../../../services/chain/src/restore.rs"]
mod restore;
#[cfg(test)]
#[path = "../../../services/chain/src/service.rs"]
mod service;
#[cfg(test)]
#[path = "../../../services/chain/src/tick.rs"]
mod tick;
