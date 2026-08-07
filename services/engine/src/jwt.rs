//! JWT HS256 signer and secret loader (CC-30a / Architecture §3.2, §7).
//!
//! - HS256, **`iat` only** — no `exp`, no `id`, no `clv`
//! - Signed **per request** (caller attaches via `bearer_auth`)
//! - Secret file: whitespace-trimmed, optional `0x` prefix, **exactly 32 bytes**
//! - Path validation mirrors p2p node-key discipline: no `..`, max file size,
//!   Unix mode must be `0600` (abort before bind on violation)
//! - Hand-written [`Debug`] prints `Jwt(<redacted>)` so a derived impl can never
//!   leak material into a `{:?}` log line
//! - On successful load, logs geth-compatible `crc32=0x…` for volume diagnosis

use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;

/// Required Unix mode for the JWT secret file (same posture as p2p node key).
pub const JWT_SECRET_MODE: u32 = 0o600;

/// Refuse to read a secret file larger than this (hex is ≤ ~70 bytes with
/// whitespace/`0x`; a few KiB is generous and blocks accidental multi-MB reads).
pub const JWT_SECRET_MAX_FILE_BYTES: u64 = 4_096;

/// 32-byte Engine API JWT secret.
///
/// [`Debug`] is hand-written: `Jwt(<redacted>)`. Never derive `Debug` on a
/// wrapper that holds the raw key material.
#[derive(Clone)]
pub struct JwtSecret {
    bytes: [u8; 32],
}

impl fmt::Debug for JwtSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Jwt(<redacted>)")
    }
}

/// Failure loading or signing with the JWT secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwtError {
    /// Empty configured path.
    EmptyPath,
    /// Path contained a `..` component (config-controlled escape).
    PathEscape { path: String },
    /// Path missing, unreadable, or not a regular file (e.g. empty-dir mount).
    Unreadable { path: String, detail: String },
    /// File larger than [`JWT_SECRET_MAX_FILE_BYTES`] before any body read.
    TooLarge { path: String, size: u64 },
    /// Unix mode broader than [`JWT_SECRET_MODE`] (abort before bind).
    PermissionsTooBroad { path: String, mode: u32 },
    /// Hex decode failed or length ≠ 32 bytes after decode.
    InvalidLength { path: String, got_bytes: usize },
    /// jsonwebtoken encode failure (should be rare for HS256 + iat-only).
    Sign(String),
}

impl fmt::Display for JwtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPath => write!(f, "JWT secret path is empty"),
            Self::PathEscape { path } => {
                write!(f, "JWT secret path must not contain '..': {path}")
            }
            Self::Unreadable { path, detail } => {
                write!(f, "JWT secret unreadable at {path}: {detail}")
            }
            Self::TooLarge { path, size } => {
                write!(
                    f,
                    "JWT secret at {path} is {size} bytes; max {JWT_SECRET_MAX_FILE_BYTES}"
                )
            }
            Self::PermissionsTooBroad { path, mode } => {
                write!(
                    f,
                    "JWT secret permissions too broad at {path}: mode {mode:#o} (require {JWT_SECRET_MODE:#o})"
                )
            }
            Self::InvalidLength { path, got_bytes } => {
                write!(
                    f,
                    "JWT secret at {path} decoded to {got_bytes} bytes; need exactly 32"
                )
            }
            Self::Sign(msg) => write!(f, "JWT sign failed: {msg}"),
        }
    }
}

impl std::error::Error for JwtError {}

#[derive(Debug, Serialize)]
struct Claims {
    /// Issued-at (seconds). The only claim we send (Architecture §3.2).
    iat: u64,
}

/// Validate and normalise a configured JWT secret path (p2p node-key shape).
///
/// - refuses empty paths
/// - refuses `..` components (config-controlled path escape)
/// - normalises away `.` components
pub fn validate_jwt_secret_path(path: &Path) -> Result<PathBuf, JwtError> {
    if path.as_os_str().is_empty() {
        return Err(JwtError::EmptyPath);
    }
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(Component::RootDir.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(JwtError::PathEscape {
                    path: path.display().to_string(),
                });
            }
            Component::Normal(s) => out.push(s),
        }
    }
    if out.as_os_str().is_empty() {
        return Err(JwtError::EmptyPath);
    }
    Ok(out)
}

fn check_permissions(path: &Path) -> Result<(), JwtError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::metadata(path).map_err(|e| JwtError::Unreadable {
            path: path.display().to_string(),
            detail: e.to_string(),
        })?;
        let mode = meta.permissions().mode() & 0o777;
        if mode != JWT_SECRET_MODE {
            return Err(JwtError::PermissionsTooBroad {
                path: path.display().to_string(),
                mode,
            });
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

impl JwtSecret {
    /// Construct from raw 32 bytes (tests / in-memory fixtures).
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// Load from a hex file: trim whitespace, strip optional `0x`, decode.
    ///
    /// **Must** yield exactly 32 bytes or return [`JwtError::InvalidLength`].
    /// Unreadable paths and directories return [`JwtError::Unreadable`].
    /// Path escapes, oversized files, and (on Unix) non-`0600` modes abort
    /// before any bind (`main` loads this before `init`/`serve`).
    ///
    /// On success logs `loaded JWT secret path=… crc32=0x…` (geth-compatible).
    pub fn load(path: &Path) -> Result<Self, JwtError> {
        let path = validate_jwt_secret_path(path)?;
        let path_str = path.display().to_string();
        let meta = fs::metadata(&path).map_err(|e| JwtError::Unreadable {
            path: path_str.clone(),
            detail: e.to_string(),
        })?;
        if !meta.is_file() {
            return Err(JwtError::Unreadable {
                path: path_str,
                detail: "path is not a regular file (empty-directory mount?)".into(),
            });
        }
        let size = meta.len();
        if size > JWT_SECRET_MAX_FILE_BYTES {
            return Err(JwtError::TooLarge {
                path: path_str,
                size,
            });
        }
        check_permissions(&path)?;
        let raw = fs::read_to_string(&path).map_err(|e| JwtError::Unreadable {
            path: path_str.clone(),
            detail: e.to_string(),
        })?;
        let trimmed = raw.trim();
        let hex_str = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
            .unwrap_or(trimmed);
        let decoded = hex::decode(hex_str).map_err(|e| JwtError::Unreadable {
            path: path_str.clone(),
            detail: format!("hex decode: {e}"),
        })?;
        if decoded.len() != 32 {
            return Err(JwtError::InvalidLength {
                path: path_str,
                got_bytes: decoded.len(),
            });
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&decoded);
        let secret = Self { bytes };
        let crc = crc32fast::hash(&secret.bytes);
        tracing::info!(
            path = %path.display(),
            crc32 = format_args!("0x{crc:08x}"),
            "loaded JWT secret"
        );
        Ok(secret)
    }

    /// Sign a compact JWS with HS256 and **`iat` only**.
    pub fn sign_iat(&self, iat: u64) -> Result<String, JwtError> {
        let header = Header::new(Algorithm::HS256);
        let claims = Claims { iat };
        let key = EncodingKey::from_secret(&self.bytes);
        encode(&header, &claims, &key).map_err(|e| JwtError::Sign(e.to_string()))
    }

    /// Sign with `iat = now` (seconds since UNIX epoch).
    pub fn sign_now(&self) -> Result<String, JwtError> {
        let iat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.sign_iat(iat)
    }

    /// Hex encoding of the secret (tests only — never log this).
    #[cfg(test)]
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.bytes)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
    use serde::Deserialize;
    use std::io::Write;

    fn write_secret_file(contents: &str) -> (std::path::PathBuf, String) {
        let dir = std::env::temp_dir().join(format!(
            "cc-engine-jwt-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("jwt.hex");
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&path).unwrap().permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&path, perms).unwrap();
        }
        (path, dir.display().to_string())
    }

    fn cleanup(dir: &str) {
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn jwt_debug_is_redacted() {
        let secret = JwtSecret::from_bytes([0xab; 32]);
        assert_eq!(format!("{secret:?}"), "Jwt(<redacted>)");
        let dbg = format!("{secret:?}");
        assert!(!dbg.contains("ab"));
        assert!(!dbg.contains(&secret.to_hex()));
    }

    #[test]
    fn secret_32_bytes_loads_with_0x_and_whitespace() {
        let hex32 = "aa".repeat(32);
        let (path, dir) = write_secret_file(&format!("  0x{hex32}\n"));
        let secret = JwtSecret::load(&path).expect("32-byte 0x-prefixed should load");
        assert_eq!(secret.to_hex(), hex32);
        cleanup(&dir);

        let (path, dir) = write_secret_file(&format!("\t{hex32}  "));
        let secret = JwtSecret::load(&path).expect("whitespace-padded should load");
        assert_eq!(secret.to_hex(), hex32);
        cleanup(&dir);
    }

    #[test]
    fn secret_31_bytes_aborts() {
        let hex31 = "aa".repeat(31);
        let (path, dir) = write_secret_file(&hex31);
        let err = JwtSecret::load(&path).expect_err("31 bytes must abort");
        match err {
            JwtError::InvalidLength { got_bytes: 31, .. } => {}
            other => panic!("expected InvalidLength(31), got {other:?}"),
        }
        cleanup(&dir);
    }

    #[test]
    fn secret_33_bytes_aborts() {
        let hex33 = "aa".repeat(33);
        let (path, dir) = write_secret_file(&hex33);
        let err = JwtSecret::load(&path).expect_err("33 bytes must abort");
        match err {
            JwtError::InvalidLength { got_bytes: 33, .. } => {}
            other => panic!("expected InvalidLength(33), got {other:?}"),
        }
        cleanup(&dir);
    }

    #[test]
    fn secret_unreadable_aborts() {
        let path = std::env::temp_dir().join(format!(
            "cc-engine-jwt-missing-{}-no-such-file",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let err = JwtSecret::load(&path).expect_err("missing path must abort");
        assert!(
            matches!(err, JwtError::Unreadable { .. }),
            "expected Unreadable, got {err:?}"
        );
    }

    #[test]
    fn secret_empty_dir_aborts() {
        let dir =
            std::env::temp_dir().join(format!("cc-engine-jwt-emptydir-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let err = JwtSecret::load(&dir).expect_err("directory path must abort");
        assert!(
            matches!(err, JwtError::Unreadable { .. }),
            "expected Unreadable for empty dir, got {err:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn secret_path_escape_aborts() {
        let err = JwtSecret::load(Path::new("../secrets/jwt.hex")).expect_err(".. must abort");
        assert!(
            matches!(err, JwtError::PathEscape { .. }),
            "expected PathEscape, got {err:?}"
        );
        let err = JwtSecret::load(Path::new("")).expect_err("empty must abort");
        assert!(matches!(err, JwtError::EmptyPath));
    }

    #[test]
    fn secret_too_large_aborts() {
        let big = "aa".repeat(3_000); // > 4 KiB
        assert!(big.len() as u64 > JWT_SECRET_MAX_FILE_BYTES);
        let (path, dir) = write_secret_file(&big);
        let err = JwtSecret::load(&path).expect_err("oversized file must abort");
        assert!(
            matches!(err, JwtError::TooLarge { .. }),
            "expected TooLarge, got {err:?}"
        );
        cleanup(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn secret_broad_permissions_aborts() {
        use std::os::unix::fs::PermissionsExt;
        let hex32 = "bb".repeat(32);
        let (path, dir) = write_secret_file(&hex32);
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&path, perms).unwrap();
        let err = JwtSecret::load(&path).expect_err("0644 must abort");
        match err {
            JwtError::PermissionsTooBroad { mode: 0o644, .. } => {}
            other => panic!("expected PermissionsTooBroad(0644), got {other:?}"),
        }
        cleanup(&dir);
    }

    #[derive(Debug, Deserialize)]
    struct DecodedClaims {
        iat: u64,
        #[serde(default)]
        exp: Option<u64>,
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        clv: Option<String>,
    }

    #[test]
    fn sign_iat_only_no_exp_id_clv() {
        let secret = JwtSecret::from_bytes([7u8; 32]);
        let token = secret.sign_iat(1_700_000_000).unwrap();
        assert_eq!(token.split('.').count(), 3);
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        validation.required_spec_claims.clear();
        let data =
            decode::<DecodedClaims>(&token, &DecodingKey::from_secret(&[7u8; 32]), &validation)
                .unwrap();
        assert_eq!(data.claims.iat, 1_700_000_000);
        assert!(data.claims.exp.is_none());
        assert!(data.claims.id.is_none());
        assert!(data.claims.clv.is_none());
    }
}
