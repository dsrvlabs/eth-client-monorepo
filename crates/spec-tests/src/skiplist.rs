//! Parser for `docs/spec-vectors-skiplist.md` (D-3 / R-9).
//!
//! Format under each `## CC-XX` section:
//!
//! ```text
//! - `preset/fork/runner/handler[/…]` -- reason text -- CC-XXy
//! ```
//!
//! An entry missing a reason **or** a removal issue number is rejected at parse
//! time. An entry matching no on-disk case is a hard failure at validation time.

use std::collections::BTreeSet;
use std::path::Path;

use crate::error::Error;

/// One skip-list entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkipEntry {
    /// Section heading without `## `, e.g. `"CC-10"`.
    pub section: String,
    /// Path relative to `tests/` (may be a prefix of a full case path).
    pub path: String,
    /// Human-readable reason the case is skipped.
    pub reason: String,
    /// Issue number that will remove this entry, e.g. `"CC-12e"`.
    pub removes: String,
}

/// Parsed skip list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkipList {
    pub entries: Vec<SkipEntry>,
}

impl SkipList {
    /// Parse skip-list markdown from a string.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let mut entries = Vec::new();
        let mut section: Option<String> = None;
        let mut in_fence = false;

        for (lineno, raw) in text.lines().enumerate() {
            let line_no = lineno + 1;
            let line = raw.trim();
            if line.starts_with("```") {
                in_fence = !in_fence;
                continue;
            }
            if in_fence || line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("## ") {
                section = Some(rest.trim().to_string());
                continue;
            }
            // Only list items with a backticked path are entries.
            if !line.starts_with("- ") && !line.starts_with("* ") {
                continue;
            }
            if !line.contains('`') {
                continue;
            }
            let sec = section.as_deref().ok_or_else(|| Error::Skiplist {
                detail: format!("line {line_no}: entry outside any `##` section"),
            })?;
            entries.push(parse_entry_line(line, sec, line_no)?);
        }

        Ok(Self { entries })
    }

    /// Load and parse a skiplist file from disk.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path).map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&text)
    }

    /// Default path to the committed skiplist, relative to this crate's manifest.
    pub fn default_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/spec-vectors-skiplist.md")
    }

    /// Fail if any entry matches no case path (exact or prefix).
    ///
    /// `case_paths` are paths relative to `tests/` (e.g.
    /// `mainnet/fulu/operations/attestation/pyspec_tests/one_basic_attestation`).
    pub fn assert_all_match_cases(&self, case_paths: &BTreeSet<String>) -> Result<(), Error> {
        for entry in &self.entries {
            let matched = case_paths.iter().any(|c| path_matches(&entry.path, c));
            if !matched {
                return Err(Error::Skiplist {
                    detail: format!(
                        "stale skip entry matches no case on disk: `{}` (section {}, removes {})",
                        entry.path, entry.section, entry.removes
                    ),
                });
            }
        }
        Ok(())
    }

    /// Entries whose path matches `case_path` (exact or prefix).
    pub fn matching(&self, case_path: &str) -> Vec<&SkipEntry> {
        self.entries
            .iter()
            .filter(|e| path_matches(&e.path, case_path))
            .collect()
    }
}

/// `entry` matches `case` when equal or when `case` is under `entry/` .
fn path_matches(entry: &str, case: &str) -> bool {
    case == entry || case.starts_with(&format!("{entry}/"))
}

fn parse_entry_line(line: &str, section: &str, line_no: usize) -> Result<SkipEntry, Error> {
    // Strip list marker.
    let body = line
        .trim_start_matches("- ")
        .trim_start_matches("* ")
        .trim();

    // Expect `path` -- reason -- removes  (also accept em-dash / en-dash).
    let path_end = body.find('`').ok_or_else(|| Error::Skiplist {
        detail: format!("line {line_no}: entry path must be wrapped in backticks"),
    })?;
    if path_end != 0 {
        return Err(Error::Skiplist {
            detail: format!("line {line_no}: entry must start with a backticked path"),
        });
    }
    let after_first = &body[1..];
    let path_close = after_first.find('`').ok_or_else(|| Error::Skiplist {
        detail: format!("line {line_no}: unclosed path backtick"),
    })?;
    let path = after_first[..path_close].trim().to_string();
    if path.is_empty() {
        return Err(Error::Skiplist {
            detail: format!("line {line_no}: empty path"),
        });
    }
    let rest = after_first[path_close + 1..].trim();

    // Split remaining on `--` / `—` / `–` into reason and removes.
    let parts = split_dashes(rest);
    if parts.len() < 2 {
        return Err(Error::Skiplist {
            detail: format!(
                "line {line_no}: entry must have reason and issue number \
                 (`path` -- reason -- CC-XXy); missing reason or issue number"
            ),
        });
    }
    // Last part is removes; everything between path and last is reason.
    let removes = parts[parts.len() - 1].trim().to_string();
    let reason = parts[..parts.len() - 1]
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(" -- ")
        .trim()
        .to_string();

    if reason.is_empty() {
        return Err(Error::Skiplist {
            detail: format!("line {line_no}: entry missing reason"),
        });
    }
    if removes.is_empty() {
        return Err(Error::Skiplist {
            detail: format!("line {line_no}: entry missing issue number that removes it"),
        });
    }
    // Require something that looks like an issue id (CC-… or similar).
    if !looks_like_issue(&removes) {
        return Err(Error::Skiplist {
            detail: format!(
                "line {line_no}: removal issue must look like `CC-12e` (got {removes:?})"
            ),
        });
    }

    Ok(SkipEntry {
        section: section.to_string(),
        path,
        reason,
        removes,
    })
}

fn split_dashes(s: &str) -> Vec<String> {
    // Normalize em/en dashes to `--` then split.
    let normalized = s.replace(['—', '–'], "--");
    normalized
        .split("--")
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect()
}

fn looks_like_issue(s: &str) -> bool {
    // CC-10, CC-12e, CC-1H, …
    let s = s.trim();
    let Some(rest) = s.strip_prefix("CC-") else {
        return false;
    };
    !rest.is_empty()
        && rest
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    const HEADER: &str = "\
# Spec-vectors skip list

Append-only within named sections.

## CC-10

## CC-12
";

    #[test]
    fn well_formed_entry_parses() {
        let text = format!(
            "{HEADER}\n- `mainnet/fulu/operations/deposit` -- not yet implemented -- CC-12c\n"
        );
        let list = SkipList::parse(&text).expect("parse");
        assert_eq!(list.entries.len(), 1);
        let e = &list.entries[0];
        assert_eq!(e.section, "CC-12");
        assert_eq!(e.path, "mainnet/fulu/operations/deposit");
        assert_eq!(e.reason, "not yet implemented");
        assert_eq!(e.removes, "CC-12c");
    }

    #[test]
    fn missing_reason_rejected() {
        let text = format!("{HEADER}\n- `mainnet/fulu/x` -- CC-12c\n");
        // Only two dash-separated parts after path: interpreted as reason=CC-12c, removes missing
        // Actually split gives ["CC-12c"] only if one segment — we need < 2 parts after path.
        // With `-- CC-12c` only one remaining part after split → error.
        let err = SkipList::parse(&text).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("reason") || msg.contains("issue"),
            "unexpected {msg}"
        );
    }

    #[test]
    fn missing_issue_rejected() {
        // Two empty removes: path -- reason only with trailing incomplete
        let text = format!("{HEADER}\n- `mainnet/fulu/x` -- some reason only\n");
        let err = SkipList::parse(&text).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("reason") || msg.contains("issue") || msg.contains("CC-"),
            "unexpected {msg}"
        );
    }

    #[test]
    fn stale_entry_fails_validation() {
        let text = format!(
            "{HEADER}\n- `mainnet/fulu/no/such/case` -- deferred -- CC-12c\n"
        );
        let list = SkipList::parse(&text).expect("parse");
        let cases: BTreeSet<String> = ["mainnet/fulu/operations/attestation/pyspec_tests/a"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let err = list
            .assert_all_match_cases(&cases)
            .expect_err("stale must fail");
        assert!(err.to_string().contains("stale") || err.to_string().contains("no case"));
    }

    #[test]
    fn prefix_match_is_not_stale() {
        let text = format!(
            "{HEADER}\n- `mainnet/fulu/operations/attestation` -- wip -- CC-12c\n"
        );
        let list = SkipList::parse(&text).expect("parse");
        let cases: BTreeSet<String> =
            ["mainnet/fulu/operations/attestation/pyspec_tests/one_basic"]
                .into_iter()
                .map(str::to_string)
                .collect();
        list.assert_all_match_cases(&cases).expect("prefix ok");
    }

    #[test]
    fn committed_skiplist_parses_empty() {
        let path = SkipList::default_path();
        let list = SkipList::load(&path).expect("load committed skiplist");
        assert!(
            list.entries.is_empty(),
            "committed skiplist must start empty, got {:?}",
            list.entries
        );
    }
}
