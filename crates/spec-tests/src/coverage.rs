//! Handler coverage assertion (Architecture §10.2 / Clause 1).
//!
//! Runners declare `const HANDLERS: &[&str]` and compare against the on-disk
//! set from [`crate::Vectors::handlers`]. Both directions of the set difference
//! are reported by name — a newly emitted handler fails loudly rather than
//! being skipped.

use std::collections::BTreeSet;

use crate::error::Error;

/// Assert `declared` and `on_disk` name the same handler set.
///
/// # Errors
///
/// [`Error::Coverage`] listing every missing name (declared but not on disk)
/// and every extra name (on disk but not declared).
pub fn assert_handler_coverage(
    declared: &[&str],
    on_disk: &BTreeSet<String>,
) -> Result<(), Error> {
    let declared_set: BTreeSet<&str> = declared.iter().copied().collect();
    let on_disk_set: BTreeSet<&str> = on_disk.iter().map(String::as_str).collect();

    let missing: Vec<String> = declared_set
        .difference(&on_disk_set)
        .map(|s| (*s).to_string())
        .collect();
    let extra: Vec<String> = on_disk_set
        .difference(&declared_set)
        .map(|s| (*s).to_string())
        .collect();

    if missing.is_empty() && extra.is_empty() {
        Ok(())
    } else {
        Err(Error::Coverage { missing, extra })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn coverage_ok_when_equal() {
        let on_disk: BTreeSet<String> = ["a", "b"].into_iter().map(str::to_string).collect();
        assert_handler_coverage(&["a", "b"], &on_disk).expect("equal");
    }

    #[test]
    fn coverage_reports_missing() {
        let on_disk: BTreeSet<String> = ["a"].into_iter().map(str::to_string).collect();
        let err = assert_handler_coverage(&["a", "b"], &on_disk).expect_err("missing b");
        let msg = err.to_string();
        assert!(
            msg.contains("missing") && msg.contains('b'),
            "expected missing b in {msg}"
        );
        match err {
            Error::Coverage { missing, extra } => {
                assert_eq!(missing, vec!["b".to_string()]);
                assert!(extra.is_empty());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn coverage_reports_extra() {
        let on_disk: BTreeSet<String> = ["a", "c"].into_iter().map(str::to_string).collect();
        let err = assert_handler_coverage(&["a"], &on_disk).expect_err("extra c");
        let msg = err.to_string();
        assert!(
            msg.contains("extra") && msg.contains('c'),
            "expected extra c in {msg}"
        );
        match err {
            Error::Coverage { missing, extra } => {
                assert!(missing.is_empty());
                assert_eq!(extra, vec!["c".to_string()]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
