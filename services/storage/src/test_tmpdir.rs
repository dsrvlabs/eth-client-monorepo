//! Unique per-process temp directories for tests.
//!
//! `cargo-nextest` runs **each test in its own process**, so a `static` counter
//! alone yields the *same* path in every process: concurrent tests then race on
//! one redb file and `Engine::open` fails with `DatabaseLocked` or
//! `I/O error: Invalid argument (os error 22)`.
//!
//! The three components each cover one execution mode:
//! - `pid` separates concurrent nextest processes;
//! - the counter separates tests inside one process (`cargo test` threads);
//! - `nanos` separates repeat runs after the OS reuses a pid.
//!
//! One `static N` shared by every caller is deliberate — a single sequence can
//! only make names more distinct across modules, never less.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A temp-dir path unique across processes, threads and runs.
///
/// The path is *not* created; callers do their own `create_dir_all` (or let
/// `Engine::open` create it). No `remove_dir_all` is needed — and none should
/// be added, since that is what let one test process delete a peer's open
/// database.
pub(crate) fn unique_temp_dir(prefix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("{prefix}-{pid}-{n}-{nanos}"))
}
