//! Unique per-process temp directories for tests.
//!
//! `cargo-nextest` runs **each test in its own process**, so a `static` counter
//! alone yields the *same* path in every process: concurrent tests then race on
//! one redb file and `Engine::open` fails with `DatabaseLocked` or
//! `I/O error: Invalid argument (os error 22)`.
//!
//! The three components each cover one execution mode:
//! - `pid` separates concurrent nextest processes;
//! - `seq` separates tests inside one process (`cargo test` threads);
//! - `nanos` separates repeat runs after the OS reuses a pid.
//!
//! One `static N` shared by every caller is deliberate — a single sequence can
//! only make names more distinct across modules, never less.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Pure naming rule, split out from [`unique_temp_dir`] so the uniqueness
/// contract is testable without observing the clock, the pid or the counter.
fn compose_name(prefix: &str, pid: u32, seq: u64, nanos: u128) -> String {
    format!("{prefix}-{pid}-{seq}-{nanos}")
}

/// A temp-dir path unique across processes, threads and runs.
///
/// The path is *not* created; callers do their own `create_dir_all` (or let
/// `Engine::open` create it). No `remove_dir_all` is needed — and none should
/// be added, since that is what let one test process delete a peer's open
/// database.
pub(crate) fn unique_temp_dir(prefix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let seq = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(compose_name(prefix, std::process::id(), seq, nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The regression that broke `prune::*::tests`: nextest gives each test its
    /// own process, so two processes both take `seq == 0`. Only the pid can
    /// separate them.
    #[test]
    fn test_compose_name_differing_pid_same_seq_yields_distinct_names() {
        let a = compose_name("p", 1234, 0, 42);
        let b = compose_name("p", 5678, 0, 42);
        assert_ne!(a, b, "two processes at seq 0 must not collide");
    }

    #[test]
    fn test_compose_name_differing_seq_yields_distinct_names() {
        let a = compose_name("p", 1234, 0, 42);
        let b = compose_name("p", 1234, 1, 42);
        assert_ne!(a, b, "two tests in one process must not collide");
    }

    #[test]
    fn test_compose_name_differing_nanos_yields_distinct_names() {
        let a = compose_name("p", 1234, 0, 42);
        let b = compose_name("p", 1234, 0, 43);
        assert_ne!(a, b, "a reused pid on a later run must not collide");
    }

    #[test]
    fn test_compose_name_starts_with_prefix() {
        assert!(
            compose_name("cc-storage-prune-chunk", 1, 2, 3).starts_with("cc-storage-prune-chunk-")
        );
    }

    #[test]
    fn test_unique_temp_dir_repeated_calls_yield_distinct_paths() {
        let a = unique_temp_dir("cc-storage-tmpdir-unit");
        let b = unique_temp_dir("cc-storage-tmpdir-unit");
        assert_ne!(a, b);
    }

    #[test]
    fn test_unique_temp_dir_is_absent_child_of_temp_dir() {
        let p = unique_temp_dir("cc-storage-tmpdir-unit");
        assert_eq!(p.parent(), Some(std::env::temp_dir().as_path()));
        assert!(
            !p.exists(),
            "caller owns creation; helper must not create it"
        );
    }
}
