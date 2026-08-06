//! Case model and tree enumeration.
//!
//! Layout (consensus-specs test format):
//! ```text
//! tests/<preset>/<fork>/<runner>/<handler>/<suite>/<case>/…
//! ```
//! The harness embeds no assumed suite names: it discovers directories from disk.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use ssz::Decode;

use crate::error::Error;
use crate::meta::Meta;
use crate::snappy;
use crate::steps;

/// One vector case directory.
#[derive(Debug, Clone)]
pub struct Case {
    /// Absolute path to the case directory.
    pub path: PathBuf,
    /// Path relative to the handler directory (e.g. `pyspec_tests/one_basic_attestation`).
    pub name: String,
    /// Preset (`mainnet`, `minimal`, `general`, …).
    pub preset: String,
    /// Fork / phase (`fulu`, `phase0`, …).
    pub fork: String,
    /// Runner name (`ssz_static`, `operations`, …).
    pub runner: String,
    /// Handler name (`BeaconBlock`, `attestation`, …).
    pub handler: String,
}

impl Case {
    /// Full path relative to the `tests/` tree root.
    pub fn rel_path(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.preset, self.fork, self.runner, self.handler, self.name
        )
    }

    /// Read and snappy-block-decompress a `.ssz_snappy` (or bare) file in this case.
    ///
    /// `file` is a file name relative to the case directory. If it has no
    /// extension, `.ssz_snappy` is appended.
    pub fn ssz_bytes(&self, file: &str) -> Result<Vec<u8>, Error> {
        let path = self.resolve_file(file)?;
        let compressed = std::fs::read(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        snappy::decompress_block(&compressed, &path)
    }

    /// Snappy-block-decompress then SSZ-decode into `T`.
    pub fn ssz<T: Decode>(&self, file: &str) -> Result<T, Error> {
        let path = self.resolve_file(file)?;
        let bytes = self.ssz_bytes(file)?;
        T::from_ssz_bytes(&bytes).map_err(|e| Error::Ssz {
            path,
            detail: format!("{e:?}"),
        })
    }

    /// Deserialize a YAML file in this case directory.
    pub fn yaml<T: DeserializeOwned>(&self, file: &str) -> Result<T, Error> {
        let path = self.file_path(file)?;
        let text = std::fs::read_to_string(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        serde_yaml::from_str(&text).map_err(|e| Error::Yaml {
            path,
            detail: e.to_string(),
        })
    }

    /// Case metadata. When `meta.yaml` is absent, returns [`Meta::default`]
    /// (`bls_setting` = [`crate::BlsSetting::Optional`] / `0`) rather than an
    /// error.
    ///
    /// # Errors
    ///
    /// Returns an error only when `meta.yaml` exists but cannot be read or
    /// parsed. Callers that want a pure non-`Result` path can match on
    /// absence themselves via [`Case::path`].
    pub fn meta(&self) -> Result<Meta, Error> {
        let path = self.path.join("meta.yaml");
        if !path.is_file() {
            return Ok(Meta::default());
        }
        let text = std::fs::read_to_string(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        serde_yaml::from_str(&text).map_err(|e| Error::Yaml {
            path,
            detail: e.to_string(),
        })
    }

    /// Parse `steps.yaml` as type `T`.
    pub fn steps<T: DeserializeOwned>(&self) -> Result<T, Error> {
        steps::load_steps(&self.path)
    }

    /// Parse `steps.yaml` as a list of YAML values.
    pub fn steps_values(&self) -> Result<Vec<serde_yaml::Value>, Error> {
        steps::load_steps_values(&self.path)
    }

    fn file_path(&self, file: &str) -> Result<PathBuf, Error> {
        reject_traversal(file)?;
        Ok(self.path.join(file))
    }

    fn resolve_file(&self, file: &str) -> Result<PathBuf, Error> {
        reject_traversal(file)?;
        let direct = self.path.join(file);
        if direct.is_file() {
            return Ok(direct);
        }
        // Allow callers to pass `pre` for `pre.ssz_snappy`.
        if !file.contains('.') {
            let with_ext = self.path.join(format!("{file}.ssz_snappy"));
            if with_ext.is_file() {
                return Ok(with_ext);
            }
        }
        Ok(direct)
    }
}

/// Reject absolute paths and `..` components (used for case-relative file names,
/// which may intentionally contain nested relative segments like `suite/case`).
fn reject_traversal(file: &str) -> Result<(), Error> {
    let p = Path::new(file);
    if p.is_absolute()
        || p.components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(Error::InvalidPath {
            detail: format!("refusing path {file:?}"),
        });
    }
    Ok(())
}

/// Reject a single tree segment (`preset` / `fork` / `runner` / `handler`).
///
/// Segments must be a single non-empty component: no separators, no `..`, no
/// absolute forms. Prevents `Path::join("..")` from escaping `tests/`.
fn reject_path_segment(seg: &str) -> Result<(), Error> {
    if seg.is_empty() {
        return Err(Error::InvalidPath {
            detail: "empty path segment".into(),
        });
    }
    if seg.contains('/') || seg.contains('\\') {
        return Err(Error::InvalidPath {
            detail: format!("path segment must not contain separators: {seg:?}"),
        });
    }
    if seg == ".." || seg == "." {
        return Err(Error::InvalidPath {
            detail: format!("refusing path segment {seg:?}"),
        });
    }
    reject_traversal(seg)
}

fn join_segments(root: &Path, segments: &[&str]) -> Result<PathBuf, Error> {
    let mut path = root.to_path_buf();
    for seg in segments {
        reject_path_segment(seg)?;
        path.push(seg);
    }
    Ok(path)
}

/// Shared vector tree handle. `root` is `<cache>/<tag>/tests`.
#[derive(Debug, Clone)]
pub struct Vectors {
    root: PathBuf,
}

impl Vectors {
    /// Open the vector tree using `SPEC_VECTORS_CACHE` / default cache root and
    /// the compile-time lockfile pin. Does not fetch remote artifacts.
    pub fn open() -> Result<Self, Error> {
        let cache_root = crate::cache::resolve_cache_root()?;
        Self::open_in(&cache_root)
    }

    /// Open against an explicit cache root (`SPEC_VECTORS_CACHE` equivalent).
    /// Useful for tests that must not race on process environment.
    pub fn open_in(cache_root: impl AsRef<Path>) -> Result<Self, Error> {
        let lock = crate::cache::embedded_lockfile()?;
        let root = crate::cache::verify_and_tests_root(cache_root.as_ref(), &lock)?;
        Ok(Self { root })
    }

    /// Absolute path to the `tests/` tree root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Runner names under `tests/<preset>/<fork>/`.
    ///
    /// Returns an empty set when the directory is absent (typo preset/fork look
    /// like "no runners" rather than a hard error — intentional for coverage
    /// probes against optional forks).
    pub fn runners(&self, preset: &str, fork: &str) -> Result<BTreeSet<String>, Error> {
        let dir = join_segments(&self.root, &[preset, fork])?;
        list_dir_names(&dir)
    }

    /// Handler names under `tests/<preset>/<fork>/<runner>/`.
    ///
    /// Empty set when the path is missing (see [`Self::runners`]).
    pub fn handlers(
        &self,
        preset: &str,
        fork: &str,
        runner: &str,
    ) -> Result<BTreeSet<String>, Error> {
        let dir = join_segments(&self.root, &[preset, fork, runner])?;
        list_dir_names(&dir)
    }

    /// Iterate cases under `tests/<preset>/<fork>/<runner>/<handler>/`.
    ///
    /// Case directories are leaves that contain at least one file (as opposed to
    /// intermediate suite directories that only contain subdirectories). Intermediate
    /// suite dirs must be file-free; a directory with both files and subdirs is
    /// treated as a leaf and is not descended further.
    pub fn cases(
        &self,
        preset: &str,
        fork: &str,
        runner: &str,
        handler: &str,
    ) -> Result<Vec<Case>, Error> {
        let handler_dir = join_segments(&self.root, &[preset, fork, runner, handler])?;
        if !handler_dir.is_dir() {
            return Err(Error::Io {
                path: handler_dir,
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "handler directory not found",
                ),
            });
        }
        let mut out = Vec::new();
        collect_cases(
            &handler_dir,
            &handler_dir,
            preset,
            fork,
            runner,
            handler,
            &mut out,
        )?;
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Collect every case path relative to `tests/` under an optional prefix
    /// filter (used by skiplist validation).
    pub fn all_case_rel_paths(&self) -> Result<BTreeSet<String>, Error> {
        let mut set = BTreeSet::new();
        for preset_ent in read_dirs(&self.root)? {
            let preset = preset_ent.file_name().to_string_lossy().into_owned();
            let preset_path = preset_ent.path();
            for fork_ent in read_dirs(&preset_path)? {
                let fork = fork_ent.file_name().to_string_lossy().into_owned();
                let fork_path = fork_ent.path();
                for runner_ent in read_dirs(&fork_path)? {
                    let runner = runner_ent.file_name().to_string_lossy().into_owned();
                    let runner_path = runner_ent.path();
                    for handler_ent in read_dirs(&runner_path)? {
                        let handler = handler_ent.file_name().to_string_lossy().into_owned();
                        let cases =
                            self.cases(&preset, &fork, &runner, &handler)?;
                        for c in cases {
                            set.insert(c.rel_path());
                        }
                    }
                }
            }
        }
        Ok(set)
    }
}

fn list_dir_names(dir: &Path) -> Result<BTreeSet<String>, Error> {
    if !dir.is_dir() {
        return Ok(BTreeSet::new());
    }
    let mut set = BTreeSet::new();
    for ent in read_dirs(dir)? {
        set.insert(ent.file_name().to_string_lossy().into_owned());
    }
    Ok(set)
}

fn read_dirs(dir: &Path) -> Result<Vec<std::fs::DirEntry>, Error> {
    let rd = std::fs::read_dir(dir).map_err(|source| Error::Io {
        path: dir.to_path_buf(),
        source,
    })?;
    let mut out = Vec::new();
    for ent in rd {
        let ent = ent.map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let ft = ent.file_type().map_err(|source| Error::Io {
            path: ent.path(),
            source,
        })?;
        if ft.is_dir() {
            out.push(ent);
        }
    }
    out.sort_by_key(|e| e.file_name());
    Ok(out)
}

fn collect_cases(
    handler_dir: &Path,
    current: &Path,
    preset: &str,
    fork: &str,
    runner: &str,
    handler: &str,
    out: &mut Vec<Case>,
) -> Result<(), Error> {
    let mut has_file = false;
    let mut subdirs = Vec::new();
    let rd = std::fs::read_dir(current).map_err(|source| Error::Io {
        path: current.to_path_buf(),
        source,
    })?;
    for ent in rd {
        let ent = ent.map_err(|source| Error::Io {
            path: current.to_path_buf(),
            source,
        })?;
        let ft = ent.file_type().map_err(|source| Error::Io {
            path: ent.path(),
            source,
        })?;
        if ft.is_dir() {
            subdirs.push(ent.path());
        } else if ft.is_file() {
            has_file = true;
        }
    }

    // A leaf case directory contains files (meta.yaml, *.ssz_snappy, …).
    // Intermediate suite dirs only contain subdirectories.
    if has_file && current != handler_dir {
        let name = current
            .strip_prefix(handler_dir)
            .unwrap_or(current)
            .to_string_lossy()
            .replace('\\', "/");
        out.push(Case {
            path: current.to_path_buf(),
            name,
            preset: preset.to_string(),
            fork: fork.to_string(),
            runner: runner.to_string(),
            handler: handler.to_string(),
        });
        // Do not descend into case directories (they should not nest cases).
        return Ok(());
    }

    subdirs.sort();
    for sub in subdirs {
        collect_cases(handler_dir, &sub, preset, fork, runner, handler, out)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn rejects_parent_dir_segments() {
        let v = Vectors {
            root: PathBuf::from("/tmp/tests-root-does-not-need-to-exist"),
        };
        let err = v.runners("..", "fulu").expect_err("..");
        assert!(matches!(err, Error::InvalidPath { .. }), "{err}");
        let err = v.handlers("mainnet", "fulu", "../x").expect_err("slash");
        assert!(matches!(err, Error::InvalidPath { .. }), "{err}");
        let err = v
            .cases("mainnet", "fulu", "ssz_static", "..")
            .expect_err("handler ..");
        assert!(matches!(err, Error::InvalidPath { .. }), "{err}");
    }

    #[test]
    fn rejects_absolute_file_names() {
        let case = Case {
            path: PathBuf::from("/tmp/case"),
            name: "c".into(),
            preset: "mainnet".into(),
            fork: "fulu".into(),
            runner: "ssz_static".into(),
            handler: "Fork".into(),
        };
        let err = case.ssz_bytes("/etc/passwd").expect_err("absolute");
        assert!(matches!(err, Error::InvalidPath { .. }), "{err}");
    }
}
