//! Storage core: single-writer mailbox, serve, backfill, prune, resume, and boot.
//!
//! S2-B-01: [`writer.rs`](writer.rs) and [`serve.rs`](serve.rs).
//! S2-B-02: [`backfill.rs`](backfill.rs), [`prune/`](prune/mod.rs),
//! [`durable_set.rs`](durable_set.rs), and [`resume.rs`](resume.rs).
//! S2-B-03: remaining companions and the process host ([`boot.rs`](boot.rs))
//! are production items here. `services/storage` is a thin shim
//! (`[ARCH]` §9.1).
//!
//! Replay is the one decoder (CC-42); writer/serve/backfill/prune stay
//! opaque-bytes (`[ARCH]` §1.5). Not JWT/HTTP-grandfathered.

#![cfg_attr(test, allow(dead_code, unreachable_pub))]

mod archive_write;
mod backfill;
mod boot;
mod durable_set;
mod history;
mod metrics;
mod migrate;
mod prune;
mod replay;
mod restore_client;
mod resume;
mod serve;
#[cfg(test)]
mod test_tmpdir;
mod write_behind;
mod writer;

pub use boot::run;
