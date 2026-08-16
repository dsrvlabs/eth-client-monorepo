//! Sandboxed `network_config` loader for the production blob-count gate (S1-B-02).
//!
//! Fail-before-bind: refuse `..`, non-regular files, oversize bodies, and a
//! path that collides with the JWT secret. Display is kind-only so a mis-pointed
//! JWT hex file cannot leak the secret or its path.

use std::path::{Component, Path, PathBuf};

/// Consensus YAML size cap (Hoodi/mainnet fixtures are ~2 KiB).
///
/// Metadata is checked before any body read so `/dev/zero` / FIFOs cannot
/// hang or OOM the fail-before-bind path.
pub const NETWORK_CONFIG_MAX_FILE_BYTES: u64 = 256 * 1024;

/// Fail-before-bind error for `network_config`. Display is path-free and
/// body-free so a mis-pointed JWT hex file cannot leak the secret or its path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkConfigError {
    /// Empty configured path.
    EmptyPath,
    /// Path contained a `..` component.
    PathEscape,
    /// Missing path or metadata/read failure.
    Unreadable,
    /// Not a regular file (`/dev/zero`, FIFO, directory).
    NotRegularFile,
    /// File larger than [`NETWORK_CONFIG_MAX_FILE_BYTES`].
    TooLarge { size: u64 },
    /// Same path as the JWT secret (after `..` / `.` normalisation).
    JwtSecretCollision,
    /// Bytes were not a consensus-specs chain-config map.
    Yaml,
}

impl std::fmt::Display for NetworkConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPath => write!(f, "network_config path is empty"),
            Self::PathEscape => write!(f, "network_config path must not contain '..'"),
            Self::Unreadable => write!(f, "network_config is unreadable"),
            Self::NotRegularFile => write!(f, "network_config is not a regular file"),
            Self::TooLarge { size } => write!(
                f,
                "network_config is {size} bytes; max {NETWORK_CONFIG_MAX_FILE_BYTES}"
            ),
            Self::JwtSecretCollision => {
                write!(f, "network_config must not be the JWT secret file")
            }
            Self::Yaml => write!(f, "network_config is not valid chain-config YAML"),
        }
    }
}

impl std::error::Error for NetworkConfigError {}

/// Refuse empty paths, `..` components, and stray `.` (JWT / node-key shape).
pub fn validate_network_config_path(path: impl AsRef<Path>) -> Result<PathBuf, NetworkConfigError> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(NetworkConfigError::EmptyPath);
    }
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(Component::RootDir.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => return Err(NetworkConfigError::PathEscape),
            Component::Normal(s) => out.push(s),
        }
    }
    if out.as_os_str().is_empty() {
        return Err(NetworkConfigError::EmptyPath);
    }
    Ok(out)
}

/// Load [`cc_types::ChainConfig`] for the production blob-count gate.
///
/// Sandbox: no `..`, regular file only, size-capped before read. Parse errors
/// are kind-only (no serde `Display`, no path) so a JWT hex file cannot leak.
pub fn load_network_chain_config(
    path: impl AsRef<Path>,
    jwt_secret_path: Option<&Path>,
) -> Result<cc_types::ChainConfig, NetworkConfigError> {
    let path = validate_network_config_path(path)?;
    if let Some(jwt) = jwt_secret_path
        && let Ok(jwt) = validate_network_config_path(jwt)
        && path == jwt
    {
        return Err(NetworkConfigError::JwtSecretCollision);
    }
    let meta = std::fs::metadata(&path).map_err(|_| NetworkConfigError::Unreadable)?;
    if !meta.is_file() {
        return Err(NetworkConfigError::NotRegularFile);
    }
    if meta.len() > NETWORK_CONFIG_MAX_FILE_BYTES {
        return Err(NetworkConfigError::TooLarge { size: meta.len() });
    }
    let text = std::fs::read_to_string(&path).map_err(|_| NetworkConfigError::Unreadable)?;
    cc_types::ChainConfig::from_yaml_str(&text).map_err(|_| NetworkConfigError::Yaml)
}
