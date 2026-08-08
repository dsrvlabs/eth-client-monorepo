//! ENR manager — Architecture §6.2, CC-21a / CC-21c / **CC-4E**.
//!
//! All ENR field mutations go through [`EnrManager::apply`], which takes a
//! **batch** and produces **exactly one** sequence bump. Field encoders for
//! `eth2`, `attnets`, `syncnets`, `cgc`, and `nfd` live here beside the matching
//! decoders (same wire shape both directions).
//!
//! # Persisted sequence (CC-4E / A-P4-4)
//!
//! The ENR `seq` and `MetaData v3.seq_number` share one counter at
//! `<node_key_path>.seq` (8 bytes LE `u64`, mode `0600`, write-then-rename).
//! The p2p identity is one unit; splitting half of it into a database that a
//! schema bump can refuse to open turns "the node restarted" into "the node
//! has a new identity".
//!
//! ## Process model (SEC-4E-3)
//!
//! **One process per `node_key_path`.** There is no cross-process file lock
//! (matching `identity.rs` for the node key). Concurrent writers on the same
//! `.seq` / `.seq.tmp` are unsupported and can lose updates; operators must not
//! double-start the same identity.
//!
//! ## Startup ordering (SEC-4E-6)
//!
//! [`EnrManager::enable_persisted_seq`] installs the bumped seq on the local ENR
//! **before** the durable write so the advertised value matches the intended
//! counter if both steps succeed. A failed persist after ENR update returns
//! `Err` (callers must not advertise on that path); a crash in that narrow
//! window can re-issue the same seq on restart — accepted residual risk for the
//! single-writer private data-dir model.

use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::U256;
use cc_types::{CUSTODY_REQUIREMENT, ForkDigest, get_custody_groups};
use discv5::enr::{CombinedKey, EnrPublicKey, Error as EnrError, NodeId};
use discv5::{ConfigBuilder, Discv5, Enr, ListenConfig};

use crate::fork_digest::{EnrForkId, ForkContext};
use crate::identity::{self, IdentityError};

// ── Persisted ENR / MetaData sequence (CC-4E) ───────────────────────────────

/// Required Unix permission bits for the `.seq` file (mirrors node-key rule).
pub const ENR_SEQ_MODE: u32 = 0o600;

/// `O_NOFOLLOW` for open(2) — refuse to open through a trailing symlink.
#[cfg(all(
    unix,
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    )
))]
const O_NOFOLLOW: i32 = 0x0000_0100;
#[cfg(all(unix, target_os = "linux"))]
const O_NOFOLLOW: i32 = 0o400_000;
#[cfg(all(
    unix,
    not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "linux",
    ))
))]
const O_NOFOLLOW: i32 = 0;

/// Derive `<node_key_path>.seq` from the node key path (not a hard-coded string).
///
/// The counter lives beside the key so backup/restore of identity is one directory.
#[must_use]
pub fn enr_seq_path(node_key_path: impl AsRef<Path>) -> PathBuf {
    let mut os = node_key_path.as_ref().as_os_str().to_owned();
    os.push(".seq");
    PathBuf::from(os)
}

/// Temporary path used by write-then-rename: `<node_key_path>.seq.tmp`.
#[must_use]
pub fn enr_seq_tmp_path(seq_path: impl AsRef<Path>) -> PathBuf {
    let mut os = seq_path.as_ref().as_os_str().to_owned();
    os.push(".tmp");
    PathBuf::from(os)
}

/// Errors from reading or persisting `<node_key_path>.seq`.
#[derive(Debug, thiserror::Error)]
pub enum EnrSeqError {
    /// `.seq` exists but mode is not exactly [`ENR_SEQ_MODE`] (refuse before advertise).
    #[error(
        "ENR seq permissions too broad at {}: mode {mode:#o} (require {required:#o})",
        path.display()
    )]
    PermissionsTooBroad {
        path: PathBuf,
        mode: u32,
        required: u32,
    },
    /// `.seq` / `.seq.tmp` is a symlink or other non-regular file (refuse).
    #[error("ENR seq at {} is not a regular file ({kind})", path.display())]
    NotRegularFile { path: PathBuf, kind: &'static str },
    /// `.seq` length is not exactly 8 bytes.
    #[error("ENR seq at {} has length {got}, expected 8", path.display())]
    InvalidLength { path: PathBuf, got: usize },
    /// `node_key_path` failed [`identity::validate_node_key_path`] (empty / `..`).
    #[error("ENR seq node_key_path rejected: {0}")]
    InvalidPath(String),
    /// Filesystem I/O.
    #[error("ENR seq I/O at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl From<IdentityError> for EnrSeqError {
    fn from(e: IdentityError) -> Self {
        Self::InvalidPath(e.to_string())
    }
}

/// Shared ENR / MetaData sequence counter persisted beside the node key.
///
/// Single source of truth for `Enr.seq` and `MetaDataV3.seq_number` (CC-4E /3).
///
/// **Single-process writer:** see module docs (SEC-4E-3). No cross-process lock.
#[derive(Debug, Clone)]
pub struct PersistedEnrSeq {
    path: PathBuf,
    value: u64,
}

impl PersistedEnrSeq {
    /// Load from `<node_key_path>.seq`, or initialise to `0` when missing.
    ///
    /// Missing file is **not** an error (first start). Existing file with
    /// permissions other than [`ENR_SEQ_MODE`], or a non-regular / symlink path,
    /// is refused. `node_key_path` is validated like the node key (no `..`).
    pub fn load_or_init(node_key_path: impl AsRef<Path>) -> Result<Self, EnrSeqError> {
        let key_path = identity::validate_node_key_path(node_key_path)?;
        let path = enr_seq_path(&key_path);
        let value = read_enr_seq_or_zero(&path)?;
        Ok(Self { path, value })
    }

    /// Path of the on-disk `.seq` file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Current counter value (after load, or after the last bump/store).
    #[must_use]
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Bump by one, persist via write-then-rename, return the new value.
    pub fn bump_and_persist(&mut self) -> Result<u64, EnrSeqError> {
        let new = self.value.checked_add(1).ok_or_else(|| EnrSeqError::Io {
            path: self.path.clone(),
            source: io::Error::new(io::ErrorKind::InvalidData, "ENR seq overflow"),
        })?;
        write_enr_seq(&self.path, new)?;
        self.value = new;
        Ok(new)
    }

    /// Store an absolute value (e.g. after `EnrManager::apply` advanced ENR seq) and persist.
    pub fn store_and_persist(&mut self, value: u64) -> Result<(), EnrSeqError> {
        write_enr_seq(&self.path, value)?;
        self.value = value;
        Ok(())
    }
}

/// Read 8 LE bytes from `path`, or `0` if the file does not exist.
///
/// Opens once (Unix: `O_NOFOLLOW`), validates **regular file** + mode
/// [`ENR_SEQ_MODE`] on the fd via `fstat`, then reads — no separate
/// exists→metadata→read chain (SEC-4E-2 / SEC-4E-5).
pub fn read_enr_seq_or_zero(path: &Path) -> Result<u64, EnrSeqError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let mut opts = fs::OpenOptions::new();
        opts.read(true);
        if O_NOFOLLOW != 0 {
            opts.custom_flags(O_NOFOLLOW);
        }
        let mut f = match opts.open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => {
                // Symlink / non-follow: classify via lstat when possible.
                if path_is_symlink(path) {
                    return Err(EnrSeqError::NotRegularFile {
                        path: path.to_path_buf(),
                        kind: "symlink",
                    });
                }
                return Err(EnrSeqError::Io {
                    path: path.to_path_buf(),
                    source: e,
                });
            }
        };

        // metadata() on File is fstat(fd) — describes the opened inode, not a re-walk.
        let meta = f.metadata().map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if !meta.file_type().is_file() {
            return Err(EnrSeqError::NotRegularFile {
                path: path.to_path_buf(),
                kind: "non-regular",
            });
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode != ENR_SEQ_MODE {
            return Err(EnrSeqError::PermissionsTooBroad {
                path: path.to_path_buf(),
                mode,
                required: ENR_SEQ_MODE,
            });
        }

        let mut buf = Vec::new();
        f.read_to_end(&mut buf).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if buf.len() != 8 {
            return Err(EnrSeqError::InvalidLength {
                path: path.to_path_buf(),
                got: buf.len(),
            });
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&buf);
        Ok(u64::from_le_bytes(arr))
    }
    #[cfg(not(unix))]
    {
        // Non-Unix: no mode / NOFOLLOW; missing → 0.
        match fs::read(path) {
            Ok(bytes) => {
                if bytes.len() != 8 {
                    return Err(EnrSeqError::InvalidLength {
                        path: path.to_path_buf(),
                        got: bytes.len(),
                    });
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes);
                Ok(u64::from_le_bytes(arr))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(EnrSeqError::Io {
                path: path.to_path_buf(),
                source: e,
            }),
        }
    }
}

/// Persist `value` as 8 LE bytes via `.seq.tmp` then `rename` over `path`.
///
/// Tmp is opened with `create_new(true)` (O_EXCL) so a pre-planted symlink is
/// not truncated (SEC-4E-1). Stale **regular** tmp from a prior crash is removed
/// first; symlink / non-regular tmp is refused.
pub fn write_enr_seq(path: &Path, value: u64) -> Result<(), EnrSeqError> {
    let tmp = enr_seq_tmp_path(path);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    write_seq_bytes_0600_exclusive(&tmp, &value.to_le_bytes())?;
    fs::rename(&tmp, path).map_err(|source| EnrSeqError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    // Ensure final path is 0600 even if rename preserved unexpected bits.
    set_seq_mode_0600(path)?;
    Ok(())
}

/// Write `value` to `<path>.tmp` only — **no** rename. Test hook for mid-write crash.
pub fn write_enr_seq_tmp_only(path: &Path, value: u64) -> Result<PathBuf, EnrSeqError> {
    let tmp = enr_seq_tmp_path(path);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    write_seq_bytes_0600_exclusive(&tmp, &value.to_le_bytes())?;
    Ok(tmp)
}

/// Whether `path` is a symlink (`lstat`). `false` if missing or on error.
fn path_is_symlink(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|m| m.file_type().is_symlink())
        .unwrap_or(false)
}

/// Remove a crash-leftover **regular** tmp; refuse symlink / non-regular (SEC-4E-1).
fn prepare_exclusive_tmp(tmp: &Path) -> Result<(), EnrSeqError> {
    match fs::symlink_metadata(tmp) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(EnrSeqError::Io {
            path: tmp.to_path_buf(),
            source: e,
        }),
        Ok(meta) if meta.file_type().is_symlink() => Err(EnrSeqError::NotRegularFile {
            path: tmp.to_path_buf(),
            kind: "symlink",
        }),
        Ok(meta) if meta.file_type().is_file() => {
            fs::remove_file(tmp).map_err(|source| EnrSeqError::Io {
                path: tmp.to_path_buf(),
                source,
            })
        }
        Ok(_) => Err(EnrSeqError::NotRegularFile {
            path: tmp.to_path_buf(),
            kind: "non-regular",
        }),
    }
}

fn set_seq_mode_0600(path: &Path) -> Result<(), EnrSeqError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Use lstat-equivalent first: refuse to chmod through a surprise symlink.
        let meta = fs::symlink_metadata(path).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if meta.file_type().is_symlink() {
            return Err(EnrSeqError::NotRegularFile {
                path: path.to_path_buf(),
                kind: "symlink",
            });
        }
        if !meta.file_type().is_file() {
            return Err(EnrSeqError::NotRegularFile {
                path: path.to_path_buf(),
                kind: "non-regular",
            });
        }
        let mut perms = meta.permissions();
        perms.set_mode(ENR_SEQ_MODE);
        fs::set_permissions(path, perms).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Write 8 bytes with `create_new(true)` (O_EXCL) + mode 0600 — never truncate
/// through an existing name (SEC-4E-1; mirrors `identity::write_secret_file`).
fn write_seq_bytes_0600_exclusive(path: &Path, bytes: &[u8; 8]) -> Result<(), EnrSeqError> {
    prepare_exclusive_tmp(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true).mode(ENR_SEQ_MODE);
        if O_NOFOLLOW != 0 {
            // Defense in depth: even if something races a symlink into place,
            // do not open through it (create_new already fails on EEXIST).
            opts.custom_flags(O_NOFOLLOW);
        }
        let mut f = opts.open(path).map_err(|source| {
            if path_is_symlink(path) {
                EnrSeqError::NotRegularFile {
                    path: path.to_path_buf(),
                    kind: "symlink",
                }
            } else {
                EnrSeqError::Io {
                    path: path.to_path_buf(),
                    source,
                }
            }
        })?;
        f.write_all(bytes).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        f.sync_all().map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let mut perms = f
            .metadata()
            .map_err(|source| EnrSeqError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions();
        perms.set_mode(ENR_SEQ_MODE);
        fs::set_permissions(path, perms).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    #[cfg(not(unix))]
    {
        // create_new via OpenOptions without Unix mode bits.
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| EnrSeqError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        f.write_all(bytes).map_err(|source| EnrSeqError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

// ── ENR key names ───────────────────────────────────────────────────────────

/// ENR key for SSZ `ENRForkID`.
pub const ENR_KEY_ETH2: &str = "eth2";
/// ENR key for next-fork digest (`Bytes4`).
pub const ENR_KEY_NFD: &str = "nfd";
/// ENR key for attestation subnet bitfield (`BitVector[64]`).
pub const ENR_KEY_ATTNETS: &str = "attnets";
/// ENR key for sync-committee subnet bitfield (`BitVector[4]`).
pub const ENR_KEY_SYNCNETS: &str = "syncnets";
/// ENR key for custody group count (`Uint64`, BE, minimal-length).
pub const ENR_KEY_CGC: &str = "cgc";

/// Attestation subnet count (SSZ `BitVector[64]` → 8 bytes).
pub const ATTNETS_BIT_LEN: usize = 64;
/// Sync subnet count (SSZ `BitVector[4]` → 1 byte).
pub const SYNCNETS_BIT_LEN: usize = 4;

// ── field encoders / decoders ───────────────────────────────────────────────

/// Encode `cgc` as `Uint64` **big-endian with no leading zero bytes**.
///
/// Spec delta 10: `0` is the **empty** byte string. A fixed-width 8-byte
/// encoding is a *different* ENR value and must not be used.
#[must_use]
pub fn encode_cgc(cgc: u64) -> Vec<u8> {
    if cgc == 0 {
        return Vec::new();
    }
    let be = cgc.to_be_bytes();
    let start = be.iter().position(|&b| b != 0).unwrap_or(7);
    be[start..].to_vec()
}

/// Decode a `cgc` payload produced by [`encode_cgc`] (or a peer's equivalent).
///
/// Rejects payloads longer than 8 bytes and leading-zero (non-minimal) encodings
/// other than the empty string for zero.
#[must_use]
pub fn decode_cgc(payload: &[u8]) -> Option<u64> {
    if payload.is_empty() {
        return Some(0);
    }
    if payload.len() > 8 {
        return None;
    }
    // Minimal-length: leading zero is illegal for non-empty payloads.
    if payload[0] == 0 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf[8 - payload.len()..].copy_from_slice(payload);
    Some(u64::from_be_bytes(buf))
}

/// Encode SSZ `ENRForkID` (16 fixed bytes) for the `eth2` key.
#[must_use]
pub fn encode_eth2(fork_id: EnrForkId) -> Vec<u8> {
    fork_id.to_ssz_bytes().to_vec()
}

/// Decode SSZ `ENRForkID` from an `eth2` payload.
#[must_use]
pub fn decode_eth2(payload: &[u8]) -> Option<EnrForkId> {
    if payload.len() != 16 {
        return None;
    }
    let mut dig = [0u8; 4];
    dig.copy_from_slice(&payload[0..4]);
    let mut ver = [0u8; 4];
    ver.copy_from_slice(&payload[4..8]);
    let mut epoch = [0u8; 8];
    epoch.copy_from_slice(&payload[8..16]);
    Some(EnrForkId {
        fork_digest: ForkDigest::from_array(dig),
        next_fork_version: cc_types::ForkVersion::from_array(ver),
        next_fork_epoch: cc_types::Epoch::new(u64::from_le_bytes(epoch)),
    })
}

/// Encode `nfd` as `Bytes4` (next-fork digest, or zero-filled).
#[must_use]
pub fn encode_nfd(digest: ForkDigest) -> Vec<u8> {
    digest.as_slice().to_vec()
}

/// Decode `nfd` (`Bytes4`).
#[must_use]
pub fn decode_nfd(payload: &[u8]) -> Option<ForkDigest> {
    if payload.len() != 4 {
        return None;
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(payload);
    Some(ForkDigest::from_array(arr))
}

/// Encode attestation subnet bitfield as SSZ `BitVector[64]` (8 bytes, LE bits).
#[must_use]
pub fn encode_attnets(bits: u64) -> Vec<u8> {
    bits.to_le_bytes().to_vec()
}

/// Decode `attnets` SSZ `BitVector[64]`.
#[must_use]
pub fn decode_attnets(payload: &[u8]) -> Option<u64> {
    if payload.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(payload);
    Some(u64::from_le_bytes(arr))
}

/// Encode sync-committee subnet bitfield as SSZ `BitVector[4]` (1 byte).
#[must_use]
pub fn encode_syncnets(bits: u8) -> Vec<u8> {
    // Only the low 4 bits are meaningful; store the full byte (SSZ packs 4 bits
    // into one byte with high bits zero).
    vec![bits & 0x0f]
}

/// Decode `syncnets` SSZ `BitVector[4]`.
#[must_use]
pub fn decode_syncnets(payload: &[u8]) -> Option<u8> {
    if payload.len() != 1 {
        return None;
    }
    Some(payload[0] & 0x0f)
}

/// Strip a short RLP string header and return the payload bytes.
///
/// Used for fields inserted via `enr_insert(key, &payload.as_slice())`.
fn rlp_string_payload(raw: &[u8]) -> Option<&[u8]> {
    if raw.is_empty() {
        return None;
    }
    let first = raw[0];
    // Single-byte value 0x00..=0x7f is its own RLP encoding.
    if first < 0x80 {
        return Some(&raw[0..1]);
    }
    // Short string: 0x80 + len, len < 56.
    if first <= 0xb7 {
        let len = (first - 0x80) as usize;
        if raw.len() == 1 + len {
            return Some(&raw[1..]);
        }
        // Tolerate trailing-only exact payload after header.
        if raw.len() > len {
            return Some(&raw[1..1 + len]);
        }
        return None;
    }
    None
}

/// Read a field's raw **payload** (RLP string contents) from an ENR.
#[must_use]
pub fn enr_field_payload(enr: &Enr, key: &str) -> Option<Vec<u8>> {
    let raw = enr.get_raw_rlp(key)?;
    rlp_string_payload(raw).map(<[u8]>::to_vec)
}

/// Read a peer's (or local) `cgc`, decoding via [`decode_cgc`].
///
/// Missing key → `None` (callers substitute [`CUSTODY_REQUIREMENT`]).
#[must_use]
pub fn read_cgc(enr: &Enr) -> Option<u64> {
    let payload = enr_field_payload(enr, ENR_KEY_CGC)?;
    decode_cgc(&payload)
}

/// Read `eth2` as [`EnrForkId`].
#[must_use]
pub fn read_eth2(enr: &Enr) -> Option<EnrForkId> {
    let payload = enr_field_payload(enr, ENR_KEY_ETH2)?;
    decode_eth2(&payload)
}

/// Read `nfd`.
#[must_use]
pub fn read_nfd(enr: &Enr) -> Option<ForkDigest> {
    let payload = enr_field_payload(enr, ENR_KEY_NFD)?;
    decode_nfd(&payload)
}

/// Read `attnets` bitmask.
#[must_use]
pub fn read_attnets(enr: &Enr) -> Option<u64> {
    let payload = enr_field_payload(enr, ENR_KEY_ATTNETS)?;
    decode_attnets(&payload)
}

/// Read `syncnets` bitmask (low 4 bits).
#[must_use]
pub fn read_syncnets(enr: &Enr) -> Option<u8> {
    let payload = enr_field_payload(enr, ENR_KEY_SYNCNETS)?;
    decode_syncnets(&payload)
}

/// Whether attestation subnet `s` is set in the ENR (`s < 64`).
#[must_use]
pub fn attnets_has(enr: &Enr, s: u8) -> bool {
    if (s as usize) >= ATTNETS_BIT_LEN {
        return false;
    }
    read_attnets(enr).is_some_and(|bits| bits & (1u64 << s) != 0)
}

/// Whether sync subnet `s` is set (`s < 4`).
#[must_use]
pub fn syncnets_has(enr: &Enr, s: u8) -> bool {
    if (s as usize) >= SYNCNETS_BIT_LEN {
        return false;
    }
    read_syncnets(enr).is_some_and(|bits| bits & (1u8 << s) != 0)
}

/// `node_id` as big-endian `U256` for [`get_custody_groups`].
#[must_use]
pub fn node_id_as_u256(node_id: NodeId) -> U256 {
    U256::from_be_bytes(node_id.raw())
}

/// Custody groups advertised by `enr` (missing `cgc` → [`CUSTODY_REQUIREMENT`]).
#[must_use]
pub fn enr_custody_groups(enr: &Enr) -> std::collections::BTreeSet<u64> {
    let cgc = read_cgc(enr).unwrap_or(CUSTODY_REQUIREMENT);
    let cgc = cgc.min(cc_types::NUMBER_OF_CUSTODY_GROUPS);
    get_custody_groups(node_id_as_u256(enr.node_id()), cgc)
}

// ── typed field-change constructors ────────────────────────────────────────

/// Build the `eth2` + `nfd` batch for an epoch tick from [`ForkContext`].
///
/// Always returns **both** fields so a single [`EnrManager::apply`] call
/// coalesces to one `seq` bump.
#[must_use]
pub fn fork_field_changes(ctx: &ForkContext) -> [EnrFieldChange; 2] {
    [
        EnrFieldChange::new(ENR_KEY_ETH2, encode_eth2(ctx.enr_fork_id())),
        EnrFieldChange::new(ENR_KEY_NFD, encode_nfd(ctx.nfd())),
    ]
}

/// Phase-2 default ENR fields: empty attnets/syncnets + fixed `cgc`.
#[must_use]
pub fn phase2_default_field_changes(cgc: u64) -> [EnrFieldChange; 3] {
    [
        EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(0)),
        EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(0)),
        EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(cgc)),
    ]
}

// ── seq strategy / manager ──────────────────────────────────────────────────

/// How [`EnrManager::apply`] advances `seq` and keeps the local ENR signature valid.
///
/// Selected by the A-P2-4 probe (CC-21/1). Outcome recorded in
/// `docs/phase-2-soak.md` §`CC-21/1 ENR sequence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnrSeqStrategy {
    /// Use [`Discv5::enr_insert`] (single field) / insert-then-collapse (batch).
    ///
    /// Probe result for discv5 **0.11.0**: `enr_insert` bumps `seq` and re-signs.
    #[default]
    EnrInsert,
    /// Build the updated field set on a cloned ENR, force `seq = old + 1`, re-sign,
    /// and install via [`Discv5::external_enr`] (local-ENR replacement path).
    RebuildAndReplace,
}

/// One field mutation in an [`EnrManager::apply`] batch.
///
/// `value` is the **payload** (not pre-RLP-encoded). It is RLP-encoded as a
/// byte string by `enr_insert` / `Enr::insert`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrFieldChange {
    /// ENR key (e.g. `"cgc"`, `"eth2"`, `"nfd"`).
    pub key: String,
    /// Raw field payload; RLP-encoded as bytes by the insert path.
    pub value: Vec<u8>,
}

impl EnrFieldChange {
    /// Construct a change from a key and a byte payload.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Errors from [`EnrManager::apply`] or handle construction.
#[derive(Debug, thiserror::Error)]
pub enum EnrApplyError {
    /// Underlying `enr` crate error (size, signing, seq overflow, …).
    #[error("enr error: {0}")]
    Enr(#[source] EnrError),
    /// `Discv5::new` rejected the key/ENR pair.
    #[error("discv5 construction failed: {0}")]
    Discv5(&'static str),
    /// Sequence number would overflow `u64`.
    #[error("ENR sequence number overflow")]
    SeqOverflow,
    /// Bootnode ENR string failed to parse.
    #[error("bootnode ENR parse failed: {0}")]
    BootnodeParse(String),
    /// Persisted `.seq` file I/O or permission failure (CC-4E).
    #[error(transparent)]
    SeqFile(#[from] EnrSeqError),
}

/// Single writer for local ENR field mutations (§6.2).
///
/// Holds a real [`Discv5`] handle and a retained node key for the
/// rebuild-and-replace path. Optional [`Self::enable_persisted_seq`] wires
/// the CC-4E on-disk counter shared with MetaData v3.
pub struct EnrManager {
    discv5: Discv5,
    /// Copy of the node key — `Discv5` takes ownership of its own key and does
    /// not expose it; rebuild/collapse paths need a signer.
    enr_key: CombinedKey,
    strategy: EnrSeqStrategy,
    /// When set, every seq advance is written to this path (write-then-rename).
    seq_path: Option<PathBuf>,
    /// Count of seq advances this process (startup bump + each [`Self::apply`]).
    /// Used by CC-4E/1 invocation-counter assertions.
    seq_bump_count: AtomicU64,
}

impl fmt::Debug for EnrManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrManager")
            .field("local_enr", &self.discv5.local_enr())
            .field("strategy", &self.strategy)
            .field("seq_path", &self.seq_path)
            .field(
                "seq_bump_count",
                &self.seq_bump_count.load(Ordering::Relaxed),
            )
            .field("enr_key", &"<redacted>")
            .finish()
    }
}

impl EnrManager {
    /// Build a manager around an existing (not necessarily started) handle.
    ///
    /// `enr_key` must be the **same** secret that signed `discv5`'s local ENR.
    #[must_use]
    pub fn new(discv5: Discv5, enr_key: CombinedKey, strategy: EnrSeqStrategy) -> Self {
        Self {
            discv5,
            enr_key,
            strategy,
            seq_path: None,
            seq_bump_count: AtomicU64::new(0),
        }
    }

    /// Construct a loopback, **no-network** manager for tests and the A-P2-4 probe.
    ///
    /// - Binds nothing: `Discv5::start` is never called.
    /// - Uses a throwaway secp256k1 key (does **not** write `./data/node_key`).
    /// - Listen config is `127.0.0.1:0` (unused without `start`).
    pub fn new_ephemeral(strategy: EnrSeqStrategy) -> Result<Self, EnrApplyError> {
        let enr_key = CombinedKey::generate_secp256k1();
        let enr_key_copy = duplicate_secp256k1(&enr_key)?;

        let enr = Enr::empty(&enr_key).map_err(EnrApplyError::Enr)?;
        let listen_config = ListenConfig::Ipv4 {
            ip: Ipv4Addr::LOCALHOST,
            port: 0,
        };
        let config = ConfigBuilder::new(listen_config).build();
        let discv5 = Discv5::new(enr, enr_key, config).map_err(EnrApplyError::Discv5)?;

        Ok(Self::new(discv5, enr_key_copy, strategy))
    }

    /// Construct a manager for production discovery from a node key + listen UDP.
    ///
    /// Does **not** call `Discv5::start` — the discovery task owns that.
    pub fn from_key_and_listen(
        enr_key: CombinedKey,
        listen_ip: Ipv4Addr,
        listen_udp: u16,
        tcp_port: u16,
        strategy: EnrSeqStrategy,
    ) -> Result<Self, EnrApplyError> {
        let enr_key_copy = duplicate_secp256k1(&enr_key)?;
        let mut builder = Enr::builder();
        builder.ip4(listen_ip).udp4(listen_udp).tcp4(tcp_port);
        let enr = builder.build(&enr_key).map_err(EnrApplyError::Enr)?;
        let listen_config = ListenConfig::Ipv4 {
            ip: listen_ip,
            port: listen_udp,
        };
        let config = ConfigBuilder::new(listen_config).build();
        let discv5 = Discv5::new(enr, enr_key, config).map_err(EnrApplyError::Discv5)?;
        Ok(Self::new(discv5, enr_key_copy, strategy))
    }

    /// Active sequence strategy (probe outcome).
    #[must_use]
    pub const fn strategy(&self) -> EnrSeqStrategy {
        self.strategy
    }

    /// Borrow the underlying discv5 handle.
    #[must_use]
    pub const fn discv5(&self) -> &Discv5 {
        &self.discv5
    }

    /// Mutable borrow of the discv5 handle (start / event stream).
    pub fn discv5_mut(&mut self) -> &mut Discv5 {
        &mut self.discv5
    }

    /// Snapshot of the local ENR.
    #[must_use]
    pub fn local_enr(&self) -> Enr {
        self.discv5.local_enr()
    }

    /// Path of the persisted `.seq` file, when [`Self::enable_persisted_seq`] was used.
    #[must_use]
    pub fn seq_path(&self) -> Option<&Path> {
        self.seq_path.as_deref()
    }

    /// Number of sequence advances this process (startup + each non-empty apply).
    #[must_use]
    pub fn seq_bump_count(&self) -> u64 {
        self.seq_bump_count.load(Ordering::Relaxed)
    }

    /// Load `<node_key_path>.seq`, **bump once**, persist, install on the local ENR.
    ///
    /// Call **before the first advertisement**. Missing `.seq` initialises at 0
    /// then bumps to 1 (no error). Broader-than-`0600` permissions, symlinks, and
    /// non-regular files refuse start. `node_key_path` is validated like the
    /// node key (no empty / `..` components).
    ///
    /// Returns the post-bump sequence (also equal to `local_enr().seq()`).
    /// MetaData v3 must seed `seq_number` from this same value (CC-4E /3).
    ///
    /// Ordering: ENR is updated then disk is written (SEC-4E-6; see module docs).
    /// On persist failure this returns `Err` after the in-memory bump — callers
    /// must not advertise when this method fails.
    pub fn enable_persisted_seq(
        &mut self,
        node_key_path: impl AsRef<Path>,
    ) -> Result<u64, EnrApplyError> {
        let key_path =
            identity::validate_node_key_path(node_key_path).map_err(EnrSeqError::from)?;
        let path = enr_seq_path(&key_path);
        let loaded = read_enr_seq_or_zero(&path)?;
        let new_seq = loaded.checked_add(1).ok_or(EnrApplyError::SeqOverflow)?;

        let mut enr = self.discv5.local_enr();
        enr.set_seq(new_seq, &self.enr_key)
            .map_err(EnrApplyError::Enr)?;
        *self.discv5.external_enr().write() = enr;

        write_enr_seq(&path, new_seq)?;
        self.seq_path = Some(path);
        self.seq_bump_count.fetch_add(1, Ordering::Relaxed);
        Ok(new_seq)
    }

    /// Construct for production with the CC-4E startup bump already applied.
    pub fn from_key_and_listen_persisted(
        enr_key: CombinedKey,
        listen_ip: Ipv4Addr,
        listen_udp: u16,
        tcp_port: u16,
        strategy: EnrSeqStrategy,
        node_key_path: impl AsRef<Path>,
    ) -> Result<Self, EnrApplyError> {
        let mut manager =
            Self::from_key_and_listen(enr_key, listen_ip, listen_udp, tcp_port, strategy)?;
        manager.enable_persisted_seq(node_key_path)?;
        Ok(manager)
    }

    /// Apply a **batch** of field changes with **exactly one** sequence bump.
    ///
    /// Empty batches are a no-op (no bump). Non-empty batches leave the local
    /// ENR signature verifying against its public key. When a `.seq` path is
    /// attached, the new sequence is persisted immediately (not at shutdown).
    pub fn apply(
        &self,
        changes: impl IntoIterator<Item = EnrFieldChange>,
    ) -> Result<(), EnrApplyError> {
        let changes: Vec<EnrFieldChange> = changes.into_iter().collect();
        if changes.is_empty() {
            return Ok(());
        }

        match self.strategy {
            EnrSeqStrategy::EnrInsert => self.apply_enr_insert(&changes)?,
            EnrSeqStrategy::RebuildAndReplace => self.apply_rebuild_and_replace(&changes)?,
        }
        self.after_seq_bump()?;
        Ok(())
    }

    /// Epoch-tick helper: write `eth2` + `nfd` from [`ForkContext`] in **one** bump.
    ///
    /// **Idempotent:** if both payloads already match the local ENR, this is a
    /// no-op (no `seq` bump). Crossing a BPO that changes both fields therefore
    /// advances `seq` by exactly **one** (CC-2A/3 coalesce), not once per epoch.
    pub fn apply_fork_context(&self, ctx: &ForkContext) -> Result<(), EnrApplyError> {
        let changes = fork_field_changes(ctx);
        let enr = self.local_enr();
        let eth2_same = read_eth2(&enr).is_some_and(|id| id == ctx.enr_fork_id());
        let nfd_same = read_nfd(&enr).is_some_and(|d| d == ctx.nfd());
        if eth2_same && nfd_same {
            return Ok(());
        }
        self.apply(changes)
    }

    /// Install Phase-2 default empty attnets/syncnets + fixed `cgc`.
    pub fn apply_phase2_defaults(&self) -> Result<(), EnrApplyError> {
        self.apply(phase2_default_field_changes(CUSTODY_REQUIREMENT))
    }

    /// Seed bootnode ENRs into the routing table (pre-start or post-start).
    pub fn add_bootnodes(&self, bootnodes: &[Enr]) -> usize {
        let mut added = 0;
        for enr in bootnodes {
            if self.discv5.add_enr(enr.clone()).is_ok() {
                added += 1;
            }
        }
        added
    }

    /// Primary path when A-P2-4 holds: `Discv5::enr_insert` for a single field;
    /// multi-field batches collapse to one bump via insert + `set_seq`.
    fn apply_enr_insert(&self, changes: &[EnrFieldChange]) -> Result<(), EnrApplyError> {
        if let [single] = changes {
            self.discv5
                .enr_insert(single.key.as_str(), &single.value.as_slice())
                .map_err(EnrApplyError::Enr)?;
            return Ok(());
        }
        // discv5 bumps once per enr_insert; coalesce multi-field batches.
        self.apply_rebuild_and_replace(changes)
    }

    /// Rebuild-and-replace fallback (and multi-field coalesce under `EnrInsert`).
    ///
    /// Clones the local ENR, applies every change via `Enr::insert`, forces
    /// `seq = old + 1` (re-signs), and installs the result through
    /// [`Discv5::external_enr`].
    fn apply_rebuild_and_replace(&self, changes: &[EnrFieldChange]) -> Result<(), EnrApplyError> {
        let old_seq = self.discv5.local_enr().seq();
        let new_seq = old_seq.checked_add(1).ok_or(EnrApplyError::SeqOverflow)?;

        let mut enr = self.discv5.local_enr();
        for change in changes {
            enr.insert(change.key.as_str(), &change.value.as_slice(), &self.enr_key)
                .map_err(EnrApplyError::Enr)?;
        }
        enr.set_seq(new_seq, &self.enr_key)
            .map_err(EnrApplyError::Enr)?;

        *self.discv5.external_enr().write() = enr;
        Ok(())
    }

    /// After a successful seq advance: count + optional on-disk persist (CC-4E).
    fn after_seq_bump(&self) -> Result<(), EnrApplyError> {
        self.seq_bump_count.fetch_add(1, Ordering::Relaxed);
        if let Some(ref path) = self.seq_path {
            let seq = self.discv5.local_enr().seq();
            write_enr_seq(path, seq)?;
        }
        Ok(())
    }
}

/// Parse a bootnode string: bare base64 ENR or `enr:` URI.
pub fn parse_bootnode(s: &str) -> Result<Enr, EnrApplyError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(EnrApplyError::BootnodeParse("empty bootnode string".into()));
    }
    let body = trimmed
        .strip_prefix("enr:")
        .or_else(|| trimmed.strip_prefix("ENR:"))
        .unwrap_or(trimmed);
    Enr::from_str(body).map_err(EnrApplyError::BootnodeParse)
}

/// Parse a list of bootnode strings; skip blank / comment lines when present.
pub fn parse_bootnodes(
    lines: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Vec<Enr>, EnrApplyError> {
    let mut out = Vec::new();
    for line in lines {
        let s = line.as_ref().trim();
        if s.is_empty() || s.starts_with('#') {
            continue;
        }
        // YAML-list form: "- enr:…"
        let s = s.strip_prefix("- ").unwrap_or(s).trim();
        out.push(parse_bootnode(s)?);
    }
    Ok(out)
}

/// Duplicate a secp256k1 [`CombinedKey`] via encode → re-import.
///
/// `CombinedKey` is not `Clone`; `Discv5::new` takes ownership of one copy
/// while [`EnrManager`] retains another for rebuild/collapse signing.
fn duplicate_secp256k1(key: &CombinedKey) -> Result<CombinedKey, EnrApplyError> {
    let mut bytes = key.encode();
    CombinedKey::secp256k1_from_bytes(&mut bytes).map_err(|_| {
        EnrApplyError::Discv5("failed to duplicate secp256k1 CombinedKey for EnrManager")
    })
}

/// Compressed secp256k1 public key bytes from an ENR (for PeerId bridging).
#[must_use]
pub fn enr_secp256k1_pubkey_bytes(enr: &Enr) -> Option<Vec<u8>> {
    let pk = enr.public_key();
    match pk {
        discv5::enr::CombinedPublicKey::Secp256k1(_) => Some(pk.encode()),
        discv5::enr::CombinedPublicKey::Ed25519(_) => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn cgc_encode_minimal_be() {
        assert_eq!(encode_cgc(0), Vec::<u8>::new());
        assert_eq!(encode_cgc(4), vec![4]);
        assert_eq!(encode_cgc(128), vec![128]);
        assert_eq!(encode_cgc(256), vec![1, 0]);
        // Fixed-width 8-byte must differ from minimal for 4.
        assert_ne!(encode_cgc(4), 4u64.to_be_bytes().to_vec());
    }

    #[test]
    fn cgc_roundtrip_values() {
        for v in [0u64, 4, 128, 255, 256, u64::MAX] {
            let enc = encode_cgc(v);
            assert_eq!(decode_cgc(&enc), Some(v), "value {v}");
        }
        // Leading zero rejected.
        assert_eq!(decode_cgc(&[0, 4]), None);
        // Too long rejected.
        assert_eq!(decode_cgc(&[1, 2, 3, 4, 5, 6, 7, 8, 9]), None);
    }

    #[test]
    fn cgc_real_enr_roundtrip_4_and_128() {
        let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        for v in [4u64, 128, 0] {
            manager
                .apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(v))])
                .unwrap();
            let enr = manager.local_enr();
            assert!(enr.verify());
            let decoded = read_cgc(&enr).expect("cgc present");
            assert_eq!(decoded, v, "cgc round-trip for {v}");
            // Fixed-width encoding must not equal the stored payload for non-full values.
            if v != 0 && v < (1u64 << 56) {
                let fixed = v.to_be_bytes().to_vec();
                let payload = enr_field_payload(&enr, ENR_KEY_CGC).unwrap();
                assert_ne!(
                    payload, fixed,
                    "minimal encoding must differ from fixed 8-byte for {v}"
                );
            }
        }
    }

    #[test]
    fn eth2_nfd_coalesce_one_seq_bump() {
        use crate::fork_digest::EnrForkId;
        use cc_types::{Epoch, ForkVersion};

        let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let seq_before = manager.local_enr().seq();
        let eth2 = EnrForkId {
            fork_digest: ForkDigest::from_array([1, 2, 3, 4]),
            next_fork_version: ForkVersion::from_array([5, 6, 7, 8]),
            next_fork_epoch: Epoch::new(99),
        };
        let nfd = ForkDigest::from_array([9, 10, 11, 12]);
        manager
            .apply([
                EnrFieldChange::new(ENR_KEY_ETH2, encode_eth2(eth2)),
                EnrFieldChange::new(ENR_KEY_NFD, encode_nfd(nfd)),
            ])
            .unwrap();
        let enr = manager.local_enr();
        assert_eq!(
            enr.seq(),
            seq_before + 1,
            "eth2+nfd must coalesce to one bump"
        );
        assert!(enr.verify());
        assert_eq!(read_eth2(&enr).unwrap(), eth2);
        assert_eq!(read_nfd(&enr).unwrap(), nfd);
    }

    #[test]
    fn attnets_syncnets_bits() {
        let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let att = 1u64 << 3 | 1u64 << 10;
        manager
            .apply([
                EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(att)),
                EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(0b0101)),
            ])
            .unwrap();
        let enr = manager.local_enr();
        assert!(attnets_has(&enr, 3));
        assert!(attnets_has(&enr, 10));
        assert!(!attnets_has(&enr, 0));
        assert!(syncnets_has(&enr, 0));
        assert!(!syncnets_has(&enr, 1));
        assert!(syncnets_has(&enr, 2));
    }

    // ── CC-4E: persisted ENR / MetaData sequence ────────────────────────────

    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::reqresp::MetaDataV3;

    fn tmp_node_key(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("cc-p2p-enr-seq-{name}-{nanos}-node_key"))
    }

    fn cleanup_seq_pair(key: &Path) {
        let seq = enr_seq_path(key);
        let tmp = enr_seq_tmp_path(&seq);
        let _ = fs::remove_file(key);
        let _ = fs::remove_file(&seq);
        let _ = fs::remove_file(&tmp);
    }

    /// CC-4E/1 — exactly one startup bump before first advertisement, even when
    /// cgc/attnets are unchanged (no subsequent `apply`).
    #[test]
    fn cc4e_exactly_one_startup_bump_before_advertisement() {
        let key = tmp_node_key("startup-one");
        cleanup_seq_pair(&key);

        let mut manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        assert_eq!(manager.seq_bump_count(), 0);

        let seq = manager.enable_persisted_seq(&key).expect("startup bump");
        assert_eq!(
            manager.seq_bump_count(),
            1,
            "exactly one bump between process start and first advertisement"
        );
        assert_eq!(seq, 1);
        assert_eq!(manager.local_enr().seq(), 1);
        assert!(manager.local_enr().verify());

        // Unchanged fields: no further apply → still exactly one bump.
        assert_eq!(manager.seq_bump_count(), 1);

        cleanup_seq_pair(&key);
    }

    /// CC-4E/2 — SIGKILL-equivalent restart: drop without graceful shutdown;
    /// reloaded seq is strictly greater. Values recorded for the commit body.
    ///
    /// True `SIGKILL` is not practical inside a unit test without a helper
    /// binary; persistence-on-bump (not on Drop) makes abrupt process death
    /// equivalent — the counter is already on disk before kill.
    #[test]
    fn cc4e_sigkill_equivalent_restart_seq_strictly_greater() {
        let key = tmp_node_key("sigkill");
        cleanup_seq_pair(&key);

        // ── process 1 ──────────────────────────────────────────────────────
        let mut m1 = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let seq_before = m1.enable_persisted_seq(&key).expect("p1 startup");
        // Field change also persists on bump (not deferred to shutdown).
        m1.apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(4))])
            .unwrap();
        let seq_recorded = m1.local_enr().seq();
        assert!(seq_recorded >= seq_before);
        // Abrupt drop = SIGKILL-equivalent (no shutdown hook).
        drop(m1);

        // ── process 2 (restart) ────────────────────────────────────────────
        let mut m2 = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let seq_after = m2.enable_persisted_seq(&key).expect("p2 startup");
        assert!(
            seq_after > seq_recorded,
            "CC-4E/2 SIGKILL-restart: new seq must be strictly greater \
             (recorded={seq_recorded}, after={seq_after})"
        );
        // Recorded pair for commit description / summary:
        //   seq_before_kill = seq_recorded, seq_after_restart = seq_after
        eprintln!(
            "CC-4E/2 SIGKILL-equivalent values: before_kill={seq_recorded} after_restart={seq_after}"
        );

        cleanup_seq_pair(&key);
    }

    /// CC-4E/3 — MetaData v3.seq_number and ENR seq equal before/after restart
    /// when seeded from the same `.seq` file.
    #[test]
    fn cc4e_metadata_and_enr_seq_equal_across_restart() {
        let key = tmp_node_key("meta-eq");
        cleanup_seq_pair(&key);

        let mut m1 = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let seq1 = m1.enable_persisted_seq(&key).unwrap();
        let meta1 = MetaDataV3 {
            seq_number: seq1,
            ..MetaDataV3::default()
        };
        assert_eq!(
            meta1.seq_number,
            m1.local_enr().seq(),
            "same counter before restart"
        );
        drop(m1);

        let mut m2 = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let seq2 = m2.enable_persisted_seq(&key).unwrap();
        let meta2 = MetaDataV3 {
            seq_number: seq2,
            ..MetaDataV3::default()
        };
        assert_eq!(
            meta2.seq_number,
            m2.local_enr().seq(),
            "same counter after restart"
        );
        assert!(seq2 > seq1);

        cleanup_seq_pair(&key);
    }

    /// CC-4E/3 negative — seeding MetaData and ENR from two different sources
    /// diverges (documents why a single `.seq` file is required).
    #[test]
    fn cc4e_dual_source_seed_diverges() {
        let key_a = tmp_node_key("dual-a");
        let key_b = tmp_node_key("dual-b");
        cleanup_seq_pair(&key_a);
        cleanup_seq_pair(&key_b);

        // Two independent files with different absolute values.
        write_enr_seq(&enr_seq_path(&key_a), 5).unwrap();
        write_enr_seq(&enr_seq_path(&key_b), 99).unwrap();

        let mut enr_mgr = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        // ENR seeded from file A (load 5 → startup bump → 6).
        let enr_seq = enr_mgr.enable_persisted_seq(&key_a).unwrap();
        assert_eq!(enr_seq, 6);

        // MetaData deliberately seeded from file B (stale absolute 99).
        let meta_seq = read_enr_seq_or_zero(&enr_seq_path(&key_b)).unwrap();
        let meta = MetaDataV3 {
            seq_number: meta_seq,
            ..MetaDataV3::default()
        };
        assert_ne!(
            meta.seq_number, enr_seq,
            "dual-source seed must diverge (meta={}, enr={enr_seq})",
            meta.seq_number,
        );
        assert_ne!(meta.seq_number, enr_mgr.local_enr().seq());

        cleanup_seq_pair(&key_a);
        cleanup_seq_pair(&key_b);
    }

    /// Write-then-rename: failure between tmp write and rename leaves old 8 bytes.
    #[test]
    fn cc4e_crash_between_tmp_and_rename_keeps_old() {
        let key = tmp_node_key("tmp-crash");
        cleanup_seq_pair(&key);
        let path = enr_seq_path(&key);

        write_enr_seq(&path, 5).unwrap();
        assert_eq!(read_enr_seq_or_zero(&path).unwrap(), 5);

        // Inject mid-write crash: tmp holds 99, rename never runs.
        let tmp = write_enr_seq_tmp_only(&path, 99).unwrap();
        assert!(tmp.is_file());
        assert_eq!(
            read_enr_seq_or_zero(&path).unwrap(),
            5,
            "old .seq must remain readable and equal to prior 8 bytes"
        );
        let on_disk = fs::read(&path).unwrap();
        assert_eq!(on_disk, 5u64.to_le_bytes());

        cleanup_seq_pair(&key);
    }

    /// Mode 0600 after creation; broader permissions refuse start.
    #[test]
    #[cfg(unix)]
    fn cc4e_mode_0600_and_broad_permissions_refuse() {
        use std::os::unix::fs::PermissionsExt;

        let key = tmp_node_key("mode");
        cleanup_seq_pair(&key);

        let mut store = PersistedEnrSeq::load_or_init(&key).unwrap();
        let _ = store.bump_and_persist().unwrap();
        let path = store.path().to_path_buf();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "created .seq must be 0600");

        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&path, perms).unwrap();

        let err = PersistedEnrSeq::load_or_init(&key).expect_err("must refuse");
        assert!(
            matches!(err, EnrSeqError::PermissionsTooBroad { .. }),
            "got {err:?}"
        );

        let mut manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let err = manager
            .enable_persisted_seq(&key)
            .expect_err("refuse start");
        assert!(
            matches!(
                err,
                EnrApplyError::SeqFile(EnrSeqError::PermissionsTooBroad { .. })
            ),
            "got {err:?}"
        );

        cleanup_seq_pair(&key);
    }

    /// CC-4E/4 — path is constructed from `node_key_path`, not a hard-coded string.
    #[test]
    fn cc4e_seq_path_derived_from_node_key_path() {
        let key = PathBuf::from("/var/lib/cc/data/node_key");
        let seq = enr_seq_path(&key);
        assert_eq!(seq, PathBuf::from("/var/lib/cc/data/node_key.seq"));
        // Not a bare hard-coded constant independent of the key path.
        assert!(seq.as_os_str().to_string_lossy().ends_with("node_key.seq"));
        assert_eq!(
            enr_seq_tmp_path(&seq),
            PathBuf::from("/var/lib/cc/data/node_key.seq.tmp")
        );

        // Live manager attaches the constructed path.
        let live_key = tmp_node_key("path-live");
        cleanup_seq_pair(&live_key);
        let mut manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        manager.enable_persisted_seq(&live_key).unwrap();
        assert_eq!(manager.seq_path(), Some(enr_seq_path(&live_key).as_path()));
        cleanup_seq_pair(&live_key);
    }

    /// First start on a fresh tempdir → seq=1, mode 0600, no error.
    #[test]
    fn cc4e_first_start_fresh_tempdir_seq_one() {
        let dir = std::env::temp_dir().join(format!(
            "cc-p2p-enr-seq-fresh-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let key = dir.join("node_key");
        // No .seq present.
        assert!(!enr_seq_path(&key).exists());

        let mut manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let seq = manager.enable_persisted_seq(&key).expect("first start");
        assert_eq!(seq, 1, "first advertisement seq must be 1");
        assert_eq!(manager.local_enr().seq(), 1);
        assert!(enr_seq_path(&key).is_file());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(enr_seq_path(&key))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        // apply persists every bump (not only shutdown).
        manager
            .apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(4))])
            .unwrap();
        let after_apply = manager.local_enr().seq();
        assert_eq!(
            read_enr_seq_or_zero(&enr_seq_path(&key)).unwrap(),
            after_apply
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Persist-on-bump: after apply, on-disk value matches ENR without Drop.
    #[test]
    fn cc4e_persist_on_every_apply_bump() {
        let key = tmp_node_key("persist-apply");
        cleanup_seq_pair(&key);

        let mut manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        manager.enable_persisted_seq(&key).unwrap();
        let path = enr_seq_path(&key);

        manager
            .apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(4))])
            .unwrap();
        let s1 = manager.local_enr().seq();
        assert_eq!(read_enr_seq_or_zero(&path).unwrap(), s1);

        manager
            .apply([
                EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(8)),
                EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(1)),
            ])
            .unwrap();
        let s2 = manager.local_enr().seq();
        assert_eq!(s2, s1 + 1);
        assert_eq!(read_enr_seq_or_zero(&path).unwrap(), s2);

        cleanup_seq_pair(&key);
    }

    // ── SEC-4E hardening ───────────────────────────────────────────────────

    /// SEC-4E-1: planted `.seq.tmp` symlink must not be truncated / followed.
    #[test]
    #[cfg(unix)]
    fn sec4e_tmp_symlink_refused_not_clobbered() {
        let key = tmp_node_key("sec-tmp-symlink");
        cleanup_seq_pair(&key);
        let path = enr_seq_path(&key);
        let tmp = enr_seq_tmp_path(&path);

        // Victim file that a naive create+truncate would clobber.
        let victim = key.with_extension("victim");
        fs::write(&victim, b"do-not-clobber-me-please!!").unwrap();
        let _ = fs::remove_file(&tmp);
        std::os::unix::fs::symlink(&victim, &tmp).unwrap();

        let err = write_enr_seq(&path, 42).expect_err("must refuse symlink tmp");
        assert!(
            matches!(
                err,
                EnrSeqError::NotRegularFile {
                    kind: "symlink",
                    ..
                }
            ),
            "got {err:?}"
        );
        assert_eq!(
            fs::read(&victim).unwrap(),
            b"do-not-clobber-me-please!!",
            "victim must be untouched"
        );

        let _ = fs::remove_file(&victim);
        cleanup_seq_pair(&key);
    }

    /// SEC-4E-1: crash leftover regular `.seq.tmp` is removed then rewritten via create_new.
    #[test]
    fn sec4e_stale_regular_tmp_replaced_safely() {
        let key = tmp_node_key("sec-stale-tmp");
        cleanup_seq_pair(&key);
        let path = enr_seq_path(&key);
        let tmp = enr_seq_tmp_path(&path);
        fs::write(&tmp, b"stale!!!").unwrap();

        write_enr_seq(&path, 7).expect("replace regular tmp");
        assert_eq!(read_enr_seq_or_zero(&path).unwrap(), 7);
        assert!(!tmp.exists(), "tmp consumed by rename");

        cleanup_seq_pair(&key);
    }

    /// SEC-4E-2/5: `.seq` symlink refused on load (no follow).
    #[test]
    #[cfg(unix)]
    fn sec4e_seq_symlink_refused_on_load() {
        let key = tmp_node_key("sec-seq-symlink");
        cleanup_seq_pair(&key);
        let path = enr_seq_path(&key);
        let other = key.with_extension("other-seq");
        write_enr_seq(&other, 99).unwrap();
        // Mode 0600 on other; symlink would previously pass a following metadata check.
        std::os::unix::fs::symlink(&other, &path).unwrap();

        let err = read_enr_seq_or_zero(&path).expect_err("must refuse symlink .seq");
        assert!(
            matches!(
                err,
                EnrSeqError::NotRegularFile {
                    kind: "symlink",
                    ..
                }
            ),
            "got {err:?}"
        );

        let _ = fs::remove_file(&other);
        cleanup_seq_pair(&key);
    }

    /// SEC-4E-4: `..` in node_key_path refused (same rule as identity).
    #[test]
    fn sec4e_parent_dir_path_refused() {
        let err = PersistedEnrSeq::load_or_init("../secrets/node_key").expect_err("escape");
        assert!(matches!(err, EnrSeqError::InvalidPath(_)), "got {err:?}");

        let mut manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let err = manager
            .enable_persisted_seq("../secrets/node_key")
            .expect_err("escape");
        assert!(
            matches!(err, EnrApplyError::SeqFile(EnrSeqError::InvalidPath(_))),
            "got {err:?}"
        );
    }
}
