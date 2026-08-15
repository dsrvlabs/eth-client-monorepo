//! Cache root resolution and readiness markers (Phase 0 §7.4).

use std::path::{Path, PathBuf};

use crate::error::Error;
use crate::lockfile::{ARTIFACTS, LOCKFILE_SRC, Lockfile};

/// Default cache root when `SPEC_VECTORS_CACHE` is unset.
pub(crate) const DEFAULT_CACHE_DIR_NAME: &str = "eth-consensus-spec-vectors";

/// Resolve `${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}`.
pub fn resolve_cache_root() -> Result<PathBuf, Error> {
    if let Ok(p) = std::env::var("SPEC_VECTORS_CACHE") {
        let p = p.trim();
        if p.is_empty() {
            return Err(Error::Env("SPEC_VECTORS_CACHE is set but empty".into()));
        }
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("HOME")
        .map_err(|_| Error::Env("HOME is unset and SPEC_VECTORS_CACHE was not provided".into()))?;
    Ok(PathBuf::from(home)
        .join(".cache")
        .join(DEFAULT_CACHE_DIR_NAME))
}

/// Verify all four `.complete-*` markers and the `tests/` tree under
/// `<cache_root>/<tag>/`.
///
/// Returns the absolute path to the `tests/` tree root on success.
pub(crate) fn verify_and_tests_root(cache_root: &Path, lock: &Lockfile) -> Result<PathBuf, Error> {
    let tag_dir = cache_root.join(&lock.tag);
    if !tag_dir.is_dir() {
        return Err(Error::cache_not_ready(format!(
            "tag directory missing: {}",
            tag_dir.display()
        )));
    }

    for (i, art) in ARTIFACTS.iter().enumerate() {
        let marker_name = format!(".complete-{art}");
        let marker_path = tag_dir.join(&marker_name);
        if !marker_path.is_file() {
            return Err(Error::MarkerMissing {
                artifact: (*art).to_string(),
                path: marker_path,
            });
        }
        let found = std::fs::read_to_string(&marker_path)
            .map_err(|source| {
                // Readiness-path I/O must surface FETCH_HINT for operators.
                Error::cache_not_ready(format!(
                    "failed to read marker {} for {art}: {source}",
                    marker_path.display()
                ))
            })?
            .trim()
            .to_string();
        let expected = lock.digests[i].as_str();
        if found != expected {
            return Err(Error::MarkerMismatch {
                artifact: (*art).to_string(),
                expected: expected.to_string(),
                found,
            });
        }
    }

    let tests = tag_dir.join("tests");
    if !tests.is_dir() {
        return Err(Error::TestsTreeMissing { path: tests });
    }
    Ok(tests)
}

/// Parse the compile-time lockfile.
pub(crate) fn embedded_lockfile() -> Result<Lockfile, Error> {
    Lockfile::parse(LOCKFILE_SRC)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::error::FETCH_HINT;
    use std::fs;

    #[test]
    fn empty_cache_errors_with_fetch_hint() {
        let dir = std::env::temp_dir().join(format!("cc-spec-tests-empty-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        let lock = embedded_lockfile().expect("lock");
        let err = verify_and_tests_root(&dir, &lock).expect_err("empty");
        assert!(
            err.to_string().contains(FETCH_HINT),
            "error must contain fetch hint, got: {err}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_marker_names_artifact_and_digests() {
        let lock = embedded_lockfile().expect("lock");
        let dir =
            std::env::temp_dir().join(format!("cc-spec-tests-corrupt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let tag_dir = dir.join(&lock.tag);
        fs::create_dir_all(tag_dir.join("tests")).expect("mkdir");
        for (i, art) in ARTIFACTS.iter().enumerate() {
            let digest = if *art == "mainnet.tar.gz" {
                "0".repeat(64)
            } else {
                lock.digests[i].clone()
            };
            fs::write(
                tag_dir.join(format!(".complete-{art}")),
                format!("{digest}\n"),
            )
            .expect("write marker");
        }
        let err = verify_and_tests_root(&dir, &lock).expect_err("corrupt");
        let msg = err.to_string();
        assert!(msg.contains("mainnet.tar.gz"), "artifact in {msg}");
        assert!(msg.contains(&lock.digests[1]), "expected digest in {msg}");
        assert!(msg.contains(&"0".repeat(64)), "found digest in {msg}");
        assert!(msg.contains(FETCH_HINT), "fetch hint in {msg}");
        let _ = fs::remove_dir_all(&dir);
    }
}
