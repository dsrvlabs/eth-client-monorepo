//! Compile-time lockfile (`spec-vectors.lock`) parsing.

use crate::error::Error;

/// Pin lockfile embedded at compile time so a tag bump forces a rebuild
/// (Phase 0 §7.4 / Architecture §10.1).
///
/// Path is from `crates/spec-tests/src/` → repo root.
pub const LOCKFILE_SRC: &str = include_str!("../../../spec-vectors.lock");

/// Artifact names whose digests live under `[sha256]` in the lockfile.
pub const ARTIFACTS: [&str; 4] = [
    "general.tar.gz",
    "mainnet.tar.gz",
    "minimal.tar.gz",
    "comptests.tar.gz",
];

/// Parsed lockfile fields needed by the harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lockfile {
    pub tag: String,
    /// Parallel to [`ARTIFACTS`]: digest for each artifact.
    pub digests: [String; 4],
}

impl Lockfile {
    /// Parse the embedded lockfile text.
    pub fn parse(src: &str) -> Result<Self, Error> {
        let tag = parse_scalar(src, "tag").ok_or_else(|| {
            Error::Lockfile("missing `tag` field in spec-vectors.lock".into())
        })?;
        // Mirror scripts/fetch-spec-vectors.sh: tag is a single path segment.
        if !is_safe_tag(tag) {
            return Err(Error::Lockfile(format!(
                "tag contains disallowed characters (allow [A-Za-z0-9._-]): {tag}"
            )));
        }

        let mut digests = [String::new(), String::new(), String::new(), String::new()];
        for (i, art) in ARTIFACTS.iter().enumerate() {
            let digest = parse_sha256(src, art).ok_or_else(|| {
                Error::Lockfile(format!(
                    "missing sha256 for `{art}` in spec-vectors.lock"
                ))
            })?;
            if !is_sha256_hex(digest) {
                return Err(Error::Lockfile(format!(
                    "sha256 for `{art}` must be 64 lowercase/uppercase hex chars"
                )));
            }
            digests[i] = digest.to_string();
        }

        Ok(Self {
            tag: tag.to_string(),
            digests,
        })
    }

    /// Digest for a named artifact.
    pub fn digest_for(&self, artifact: &str) -> Option<&str> {
        ARTIFACTS
            .iter()
            .position(|&a| a == artifact)
            .map(|i| self.digests[i].as_str())
    }
}

fn parse_scalar<'a>(src: &'a str, key: &str) -> Option<&'a str> {
    for line in src.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        if k.trim() == key {
            return Some(unquote(v.trim()));
        }
    }
    None
}

fn parse_sha256<'a>(src: &'a str, artifact: &str) -> Option<&'a str> {
    let mut in_sha = false;
    for line in src.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if trimmed == "[sha256]" {
            in_sha = true;
            continue;
        }
        if in_sha && trimmed.starts_with('[') {
            break;
        }
        if !in_sha {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        let k = unquote(k.trim());
        if k == artifact {
            return Some(unquote(v.trim()));
        }
    }
    None
}

fn unquote(s: &str) -> &str {
    s.trim()
        .trim_matches('"')
        .trim()
}

/// Same charset as `scripts/fetch-spec-vectors.sh` tag sanitisation.
fn is_safe_tag(tag: &str) -> bool {
    !tag.is_empty()
        && tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn parses_committed_lockfile() {
        let lf = Lockfile::parse(LOCKFILE_SRC).expect("lockfile");
        assert_eq!(lf.tag, "v1.7.0-alpha.13");
        assert_eq!(
            lf.digest_for("general.tar.gz"),
            Some("b98e5328037a84993254085700b8d9eae304beca81d0944fe05274ca3523ed8b")
        );
        assert_eq!(
            lf.digest_for("mainnet.tar.gz"),
            Some("bf69706c9cc2c423e3bbaeb5ae7eb53a9e5adfdef84e3731296bb646fe2e8149")
        );
        assert_eq!(
            lf.digest_for("minimal.tar.gz"),
            Some("94bafd73514385645404007e6ec8b47aed13ca443b8e75621f40fce5ce1f47a2")
        );
        assert_eq!(
            lf.digest_for("comptests.tar.gz"),
            Some("a4319cd2e0253433022b3261ed53ee20c3bccb33517454ddce1a895280ac4e4e")
        );
    }

    #[test]
    fn rejects_traversal_tag() {
        let src = r#"
tag = "../../evil"
[sha256]
"general.tar.gz"   = "b98e5328037a84993254085700b8d9eae304beca81d0944fe05274ca3523ed8b"
"mainnet.tar.gz"   = "bf69706c9cc2c423e3bbaeb5ae7eb53a9e5adfdef84e3731296bb646fe2e8149"
"minimal.tar.gz"   = "94bafd73514385645404007e6ec8b47aed13ca443b8e75621f40fce5ce1f47a2"
"comptests.tar.gz" = "a4319cd2e0253433022b3261ed53ee20c3bccb33517454ddce1a895280ac4e4e"
"#;
        let err = Lockfile::parse(src).expect_err("bad tag");
        assert!(err.to_string().contains("disallowed"), "{err}");
    }
}
