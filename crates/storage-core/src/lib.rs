//! Storage core: single-writer mailbox and serve read path.
//!
//! S2-B-01: [`writer.rs`](writer.rs) and [`serve.rs`](serve.rs) live here
//! (verbatim). `cc-storage` compiles them via `#[path]` so backfill / prune /
//! metrics / history stay owned by `cc-storage` until S2-B-02 / B-03.
//!
//! Unit tests compile the moved files against the still-in-place companions.
//! `backfill.rs` is included production-only (no `#[cfg(test)]` module) so this
//! crate does not run S2-B-02 tests.

#![cfg_attr(test, allow(dead_code, unreachable_pub))]

#[cfg(test)]
#[path = "../../../services/storage/src/metrics.rs"]
mod metrics;
#[cfg(test)]
#[path = "writer.rs"]
mod writer;
#[cfg(test)]
#[allow(dead_code)]
mod backfill {
    include!(concat!(env!("OUT_DIR"), "/backfill_prod.rs"));
}
#[cfg(test)]
#[path = "../../../services/storage/src/history.rs"]
mod history;
#[cfg(test)]
#[path = "serve.rs"]
mod serve;
