//! Storage core: single-writer mailbox, serve, backfill, prune, and resume.
//!
//! S2-B-01: [`writer.rs`](writer.rs) and [`serve.rs`](serve.rs).
//! S2-B-02: [`backfill.rs`](backfill.rs), [`prune/`](prune/mod.rs),
//! [`durable_set.rs`](durable_set.rs), and [`resume.rs`](resume.rs) live here
//! (verbatim). `cc-storage` compiles them via `#[path]` so metrics / history /
//! restore stay owned by `cc-storage` until S2-B-03.
//!
//! Unit tests compile the moved files against the still-in-place companions.

#![cfg_attr(test, allow(dead_code, unreachable_pub))]

#[cfg(test)]
#[path = "backfill.rs"]
mod backfill;
#[cfg(test)]
#[path = "durable_set.rs"]
mod durable_set;
#[cfg(test)]
#[path = "../../../services/storage/src/history.rs"]
mod history;
#[cfg(test)]
#[path = "../../../services/storage/src/metrics.rs"]
mod metrics;
#[cfg(test)]
#[path = "prune/mod.rs"]
mod prune;
#[cfg(test)]
#[path = "../../../services/storage/src/restore_client.rs"]
mod restore_client;
#[cfg(test)]
#[path = "resume.rs"]
mod resume;
#[cfg(test)]
#[path = "serve.rs"]
mod serve;
#[cfg(test)]
#[path = "../../../services/storage/src/test_tmpdir.rs"]
mod test_tmpdir;
#[cfg(test)]
#[path = "writer.rs"]
mod writer;
