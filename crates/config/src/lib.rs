//! Typed configuration loading (Architecture §5, CC-09 / CC-09b).
//!
//! Layers `config/<service>.toml` then `CC_<SERVICE>_` environment variables
//! (env wins). Nested keys use double-underscore splitting
//! (`CC_P2P_PEERS__CHAIN=http://chain:9001` → `peers["chain"]`).
//!
//! **D-2:** `RUST_LOG` and `LOG_FORMAT` are resolved here into `log_filter` and
//! `log_format`. Callers (`cc_bootstrap::init`) receive already-resolved values;
//! this is the only crate permitted to read the environment.
//!
//! **D-1:** `ServiceSpec` is *not* constructed here. Per-service types live in
//! `services/<name>/` and build a `ServiceSpec` there (L3 may depend on both
//! `cc-config` and `cc-bootstrap`; L0 must not depend on L2).
//!
//! **CC-09/2:** Invalid or missing required fields return `Err` whose display
//! names the offending key and its provenance (config file path and/or
//! `CC_<SERVICE>_<FIELD>` env var). Callers load before any bind.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use figment::Figment;
use figment::error::Kind as FigmentKind;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};

/// Shared service configuration embedded by every per-service config type via
/// `#[serde(flatten)] pub service: ServiceConfig`.
///
/// Phase 0 carries only these fields; Phase 1 service-specific keys land on the
/// per-service wrapper, not on this struct.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ServiceConfig {
    #[serde(deserialize_with = "deserialize_socket_addr")]
    pub grpc_addr: SocketAddr,
    #[serde(deserialize_with = "deserialize_socket_addr")]
    pub metrics_addr: SocketAddr,
    /// Peer name → gRPC URI (e.g. `"chain"` → `http://chain:9001`).
    #[serde(default, deserialize_with = "deserialize_peers")]
    pub peers: BTreeMap<String, http::Uri>,
    /// Log format: `"json"` (default) or `"pretty"`. Resolved from `LOG_FORMAT`.
    #[serde(deserialize_with = "deserialize_log_format")]
    pub log_format: String,
    /// Tracing filter directive string. Resolved from `RUST_LOG`.
    pub log_filter: String,
}

/// Configuration load error. Display includes the offending key and provenance
/// (CC-09/2): file path and/or `CC_<SERVICE>_<FIELD>` for missing required fields;
/// figment source metadata for malformed values.
///
/// Large variants are boxed so `Result<T, Error>` stays small
/// (`clippy::result_large_err`).
#[derive(Debug)]
pub enum Error {
    /// `service` was not a simple slug (`^[a-z0-9-]+$`).
    InvalidServiceName(String),
    /// A required field was absent from both the TOML file and the env layer.
    MissingField {
        field: String,
        config_path: PathBuf,
        env_var: String,
    },
    /// Figment extract/parse failure (bad type, invalid value, …).
    Figment(Box<figment::Error>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidServiceName(name) => write!(
                f,
                "invalid service name {name:?}: must match ^[a-z0-9-]+$ (no path separators or uppercase)"
            ),
            Self::MissingField {
                field,
                config_path,
                env_var,
            } => write!(
                f,
                "missing field `{field}` (set in {} or {env_var})",
                config_path.display()
            ),
            Self::Figment(inner) => write!(f, "{inner}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidServiceName(_) | Self::MissingField { .. } => None,
            Self::Figment(inner) => Some(inner.as_ref()),
        }
    }
}

/// Load configuration for `service` from `config/<service>.toml`, then
/// `CC_<SERVICE>_` environment variables (env wins), then bare `RUST_LOG` /
/// `LOG_FORMAT` into `log_filter` / `log_format`.
///
/// `service` is the process name (`"chain"`, `"beacon-api"`, …). It must be a
/// simple slug matching `^[a-z0-9-]+$` so it cannot introduce path traversal
/// when joined into `config/<service>.toml`. Hyphens become underscores in the
/// env prefix (`beacon-api` → `CC_BEACON_API_`).
///
/// Returns `Err` before any port is bound when a required field is missing or
/// malformed; the error message names the offending key and its provenance.
pub fn load<T: DeserializeOwned>(service: &str) -> Result<T, Error> {
    validate_service_name(service)?;
    let path = PathBuf::from("config").join(format!("{service}.toml"));
    load_from(service, &path)
}

/// Like [`load`], but reads the TOML file from an explicit path.
///
/// Useful for tests and for layouts that do not use a CWD-relative `config/`.
/// The TOML path is taken exactly as given (`Toml::file_exact`); parent
/// directories are not walked.
pub fn load_from<T: DeserializeOwned>(service: &str, path: &Path) -> Result<T, Error> {
    validate_service_name(service)?;
    let figment = figment_for(service, path);
    // Validate the shared schema *without* going through a per-service
    // `#[serde(flatten)]` wrapper first. Serde flatten erases field paths, so
    // figment would otherwise report bare messages like "invalid socket address
    // syntax" with no key or provenance (CC-09/2). Phase 1 service-specific
    // fields are then extracted on the second pass.
    let _: ServiceConfig = figment
        .extract()
        .map_err(|err| map_extract_error(service, path, err))?;
    figment
        .extract()
        .map_err(|err| map_extract_error(service, path, err))
}

/// Map a figment extract error into [`Error`], enriching missing-field cases
/// with the config path and the corresponding `CC_<SERVICE>_<FIELD>` name.
fn map_extract_error(service: &str, path: &Path, err: figment::Error) -> Error {
    for e in err.clone() {
        if let FigmentKind::MissingField(field) = &e.kind {
            return Error::MissingField {
                field: field.to_string(),
                config_path: path.to_path_buf(),
                env_var: env_var_name(service, field),
            };
        }
    }
    Error::Figment(Box::new(err))
}

/// Full env var name for a dotted field path under `service`.
///
/// `"chain"` + `"grpc_addr"` → `"CC_CHAIN_GRPC_ADDR"`;
/// `"beacon-api"` + `"peers.chain"` → `"CC_BEACON_API_PEERS__CHAIN"`.
pub fn env_var_name(service: &str, field_path: &str) -> String {
    let rest = field_path
        .split('.')
        .map(|s| s.to_ascii_uppercase())
        .collect::<Vec<_>>()
        .join("__");
    format!("CC_{}_{}", env_prefix(service), rest)
}

/// Build the layered [`Figment`] for `service` without extracting.
///
/// Caller must have already validated `service` via [`validate_service_name`].
fn figment_for(service: &str, path: &Path) -> Figment {
    let prefix = format!("CC_{}_", env_prefix(service));

    // Layer order (later wins): telemetry defaults → TOML → CC_* env → bare RUST_LOG/LOG_FORMAT.
    // `file_exact` does not walk parent directories looking for the file name.
    let mut figment = Figment::new()
        .merge(Serialized::defaults(TelemetryDefaults::default()))
        .merge(Toml::file_exact(path))
        .merge(Env::prefixed(&prefix).split("__"));

    // D-2: bare RUST_LOG / LOG_FORMAT are resolved here and only here.
    if let Ok(v) = std::env::var("RUST_LOG") {
        figment = figment.merge(Serialized::defaults(LogFilterOverride { log_filter: v }));
    }
    if let Ok(v) = std::env::var("LOG_FORMAT") {
        figment = figment.merge(Serialized::defaults(LogFormatOverride { log_format: v }));
    }

    figment
}

/// Reject service names that are not a simple lowercase slug.
///
/// Allowed: `chain`, `p2p`, `beacon-api`. Rejected: `../etc/passwd`, `Chain`,
/// empty string, underscores, path separators.
fn validate_service_name(service: &str) -> Result<(), Error> {
    let valid = !service.is_empty()
        && service
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidServiceName(service.to_owned()))
    }
}

/// Convert a service name to the `CC_<PREFIX>_` env segment.
///
/// `"chain"` → `"CHAIN"`, `"beacon-api"` → `"BEACON_API"`.
fn env_prefix(service: &str) -> String {
    service.replace('-', "_").to_ascii_uppercase()
}

#[derive(Debug, Serialize)]
struct TelemetryDefaults {
    log_filter: &'static str,
    log_format: &'static str,
}

impl Default for TelemetryDefaults {
    fn default() -> Self {
        Self {
            log_filter: "info",
            log_format: "json",
        }
    }
}

#[derive(Debug, Serialize)]
struct LogFilterOverride {
    log_filter: String,
}

#[derive(Debug, Serialize)]
struct LogFormatOverride {
    log_format: String,
}

fn deserialize_socket_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    SocketAddr::from_str(&s).map_err(serde::de::Error::custom)
}

fn deserialize_log_format<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "json" | "pretty" => Ok(s),
        other => Err(serde::de::Error::custom(format!(
            "invalid log format {other:?}: expected \"json\" or \"pretty\""
        ))),
    }
}

fn deserialize_peers<'de, D>(deserializer: D) -> Result<BTreeMap<String, http::Uri>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = BTreeMap::<String, String>::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(k, v)| {
            http::Uri::from_str(&v)
                .map(|uri| (k, uri))
                .map_err(serde::de::Error::custom)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    // Edition 2024 made `set_var`/`remove_var` unsafe; tests must mutate env under a lock.
    #![allow(unsafe_code)]
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    /// Serialise tests that mutate process environment.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Owned env key so table-driven tests can use computed `CC_<SERVICE>_*` names.
    struct EnvGuard {
        key: String,
        previous: Option<String>,
    }

    impl EnvGuard {
        /// Set `key=value` for the duration of the guard; restore previous state on drop.
        fn set(key: impl Into<String>, value: &str) -> Self {
            let key = key.into();
            let previous = std::env::var(&key).ok();
            // Tests hold `env_lock` and restore on drop so process-global env stays consistent.
            // SAFETY: exclusive access via `env_lock`; restored in Drop.
            unsafe { std::env::set_var(&key, value) };
            Self { key, previous }
        }

        /// Unset `key` for the duration of the guard; restore previous state on drop.
        fn clear(key: impl Into<String>) -> Self {
            let key = key.into();
            let previous = std::env::var(&key).ok();
            // SAFETY: exclusive access via `env_lock`; restored in Drop.
            unsafe { std::env::remove_var(&key) };
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: same exclusive-access contract as `set`/`clear`.
            match &self.previous {
                Some(v) => unsafe { std::env::set_var(&self.key, v) },
                None => unsafe { std::env::remove_var(&self.key) },
            }
        }
    }

    fn write_toml(dir: &Path, name: &str, body: &str) -> PathBuf {
        fs::create_dir_all(dir).unwrap();
        let path = dir.join(format!("{name}.toml"));
        fs::write(&path, body).unwrap();
        path
    }

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cc-config-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn workspace_config(service: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../config")
            .join(format!("{service}.toml"))
    }

    /// Phase 0 services + Architecture §6.3 default gRPC ports (metrics = gRPC + 100).
    const SERVICES: [(&str, u16); 6] = [
        ("chain", 9001),
        ("p2p", 9002),
        ("attestation", 9003),
        ("engine", 9004),
        ("beacon-api", 9005),
        ("storage", 9006),
    ];

    /// Minimal per-service wrapper matching every service binary (D-1 / CC-09b).
    #[derive(Debug, Deserialize)]
    struct PerServiceConfig {
        #[serde(flatten)]
        service: ServiceConfig,
    }

    #[test]
    fn env_beats_file_for_grpc_addr() {
        let _lock = env_lock();
        let dir = temp_dir("env-beats");
        let path = write_toml(
            &dir,
            "chain",
            r#"
grpc_addr = "127.0.0.1:9001"
metrics_addr = "127.0.0.1:9101"
log_format = "json"
log_filter = "info"
"#,
        );

        let _grpc = EnvGuard::set("CC_CHAIN_GRPC_ADDR", "0.0.0.0:19001");
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");

        let cfg: ServiceConfig = load_from("chain", &path).expect("load");
        assert_eq!(cfg.grpc_addr, "0.0.0.0:19001".parse().unwrap());
        // File value for metrics is unchanged.
        assert_eq!(cfg.metrics_addr, "127.0.0.1:9101".parse().unwrap());

        fs::remove_dir_all(&dir).ok();
    }

    /// CC-09/1 + CC-09/4: file < env for at least one field on every service.
    #[test]
    fn env_beats_file_for_grpc_addr_all_services() {
        let _lock = env_lock();
        let dir = temp_dir("env-beats-all");
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");

        // Hold guards so env restores after the loop.
        let mut guards = Vec::new();

        for (service, default_port) in SERVICES {
            let file_addr = format!("127.0.0.1:{default_port}");
            let env_port = default_port + 10_000;
            let env_addr = format!("0.0.0.0:{env_port}");
            let metrics = format!("127.0.0.1:{}", default_port + 100);

            let body = format!(
                r#"
grpc_addr = "{file_addr}"
metrics_addr = "{metrics}"
log_format = "json"
log_filter = "info"
"#
            );
            let path = write_toml(&dir, service, &body);

            let env_key = env_var_name(service, "grpc_addr");
            guards.push(EnvGuard::set(env_key.clone(), &env_addr));
            // Ensure no stray metrics override.
            guards.push(EnvGuard::clear(env_var_name(service, "metrics_addr")));

            let cfg: PerServiceConfig =
                load_from(service, &path).unwrap_or_else(|e| panic!("load {service}: {e}"));
            assert_eq!(
                cfg.service.grpc_addr,
                env_addr.parse().unwrap(),
                "{service}: env must win for grpc_addr"
            );
            assert_eq!(
                cfg.service.metrics_addr,
                metrics.parse().unwrap(),
                "{service}: file metrics_addr must remain"
            );
        }

        drop(guards);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn peers_double_underscore_nesting() {
        let _lock = env_lock();
        let dir = temp_dir("peers-nest");
        let path = write_toml(
            &dir,
            "p2p",
            r#"
grpc_addr = "127.0.0.1:9002"
metrics_addr = "127.0.0.1:9102"
log_format = "json"
log_filter = "info"
"#,
        );

        let _peer = EnvGuard::set("CC_P2P_PEERS__CHAIN", "http://chain:9001");
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");

        let cfg: ServiceConfig = load_from("p2p", &path).expect("load");
        let chain = cfg.peers.get("chain").expect("peers[chain]");
        // http::Uri normalises an empty path to "/".
        assert_eq!(chain, &"http://chain:9001".parse::<http::Uri>().unwrap());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn malformed_grpc_addr_error_names_key_and_file_provenance() {
        let _lock = env_lock();
        let dir = temp_dir("bad-addr");
        let path = write_toml(
            &dir,
            "chain",
            r#"
grpc_addr = "not-a-socket-addr"
metrics_addr = "127.0.0.1:9101"
log_format = "json"
log_filter = "info"
"#,
        );

        let _grpc = EnvGuard::clear("CC_CHAIN_GRPC_ADDR");
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");

        let err = load_from::<ServiceConfig>("chain", &path).expect_err("must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("grpc_addr"),
            "error must name the offending key, got: {msg}"
        );
        assert!(
            msg.contains(path.file_name().unwrap().to_str().unwrap())
                || msg.contains(&path.display().to_string()),
            "error must name the config file provenance, got: {msg}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn malformed_env_grpc_addr_names_key_and_env_provenance() {
        let _lock = env_lock();
        let dir = temp_dir("bad-env-addr");
        let path = write_toml(
            &dir,
            "chain",
            r#"
grpc_addr = "127.0.0.1:9001"
metrics_addr = "127.0.0.1:9101"
log_format = "json"
log_filter = "info"
"#,
        );

        let _grpc = EnvGuard::set("CC_CHAIN_GRPC_ADDR", "not-a-socket-addr");
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");

        let err = load_from::<ServiceConfig>("chain", &path).expect_err("must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("GRPC_ADDR") || msg.contains("grpc_addr"),
            "error must name the offending key, got: {msg}"
        );
        assert!(
            msg.contains("CC_CHAIN_") || msg.contains("environment"),
            "error must name env provenance, got: {msg}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    /// CC-09/2 + CC-09/4: missing required field names key and both provenances.
    #[test]
    fn missing_required_field_names_key_and_provenance() {
        let _lock = env_lock();
        let dir = temp_dir("missing-field");
        // grpc_addr absent from file; clear env so nothing supplies it.
        let path = write_toml(
            &dir,
            "chain",
            r#"
metrics_addr = "127.0.0.1:9101"
log_format = "json"
log_filter = "info"
"#,
        );

        let _grpc = EnvGuard::clear("CC_CHAIN_GRPC_ADDR");
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");

        let err = load_from::<ServiceConfig>("chain", &path).expect_err("must fail");
        match &err {
            Error::MissingField {
                field,
                config_path,
                env_var,
            } => {
                assert_eq!(field, "grpc_addr");
                assert_eq!(config_path, &path);
                assert_eq!(env_var, "CC_CHAIN_GRPC_ADDR");
            }
            other => panic!("expected MissingField, got {other}"),
        }
        let msg = err.to_string();
        assert!(
            msg.contains("grpc_addr"),
            "display must name the key, got: {msg}"
        );
        assert!(
            msg.contains(&path.display().to_string()),
            "display must name the file path, got: {msg}"
        );
        assert!(
            msg.contains("CC_CHAIN_GRPC_ADDR"),
            "display must name the env var, got: {msg}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn invalid_log_format_rejected_with_key() {
        let _lock = env_lock();
        let dir = temp_dir("bad-log-format");
        let path = write_toml(
            &dir,
            "chain",
            r#"
grpc_addr = "127.0.0.1:9001"
metrics_addr = "127.0.0.1:9101"
log_format = "xml"
log_filter = "info"
"#,
        );

        let _log_format = EnvGuard::clear("LOG_FORMAT");
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _grpc = EnvGuard::clear("CC_CHAIN_GRPC_ADDR");

        let err = load_from::<ServiceConfig>("chain", &path).expect_err("must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("log_format") || msg.contains("log format"),
            "error must name log_format, got: {msg}"
        );
        assert!(
            msg.contains("json") && msg.contains("pretty"),
            "error must state allowed values, got: {msg}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rust_log_and_log_format_resolved_here() {
        let _lock = env_lock();
        let dir = temp_dir("telemetry");
        let path = write_toml(
            &dir,
            "chain",
            r#"
grpc_addr = "127.0.0.1:9001"
metrics_addr = "127.0.0.1:9101"
log_format = "json"
log_filter = "info"
"#,
        );

        let _rust_log = EnvGuard::set("RUST_LOG", "cc_chain=debug,info");
        let _log_format = EnvGuard::set("LOG_FORMAT", "pretty");

        let cfg: ServiceConfig = load_from("chain", &path).expect("load");
        assert_eq!(cfg.log_filter, "cc_chain=debug,info");
        assert_eq!(cfg.log_format, "pretty");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn env_prefix_maps_hyphenated_service_names() {
        assert_eq!(env_prefix("chain"), "CHAIN");
        assert_eq!(env_prefix("p2p"), "P2P");
        assert_eq!(env_prefix("beacon-api"), "BEACON_API");
        assert_eq!(env_var_name("chain", "grpc_addr"), "CC_CHAIN_GRPC_ADDR");
        assert_eq!(
            env_var_name("beacon-api", "peers.chain"),
            "CC_BEACON_API_PEERS__CHAIN"
        );
    }

    #[test]
    fn rejects_non_slug_service_names() {
        for bad in [
            "",
            "Chain",
            "chain_api",
            "../etc/passwd",
            "chain/../p2p",
            "chain.toml",
            "beacon api",
            "p2p\0",
        ] {
            let err = validate_service_name(bad).expect_err("must reject");
            assert!(
                matches!(err, Error::InvalidServiceName(_)),
                "expected InvalidServiceName for {bad:?}, got {err}"
            );
            // load_from must fail before touching the path.
            let err = load_from::<ServiceConfig>(bad, Path::new("/nonexistent.toml"))
                .expect_err("must reject before I/O");
            assert!(matches!(err, Error::InvalidServiceName(_)));
        }

        for good in ["chain", "p2p", "beacon-api", "attestation", "a", "x9"] {
            validate_service_name(good).expect("must accept");
        }
    }

    #[test]
    fn telemetry_defaults_when_bare_env_absent() {
        let _lock = env_lock();
        let dir = temp_dir("defaults");
        // File omits log_* so programmatic defaults apply.
        let path = write_toml(
            &dir,
            "chain",
            r#"
grpc_addr = "127.0.0.1:9001"
metrics_addr = "127.0.0.1:9101"
"#,
        );

        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");

        let cfg: ServiceConfig = load_from("chain", &path).expect("load");
        assert_eq!(cfg.log_filter, "info");
        assert_eq!(cfg.log_format, "json");
        assert!(cfg.peers.is_empty());

        fs::remove_dir_all(&dir).ok();
    }

    /// Every committed `config/<service>.toml` loads and matches the port map.
    #[test]
    fn committed_tomls_load_for_all_services_with_port_map() {
        let _lock = env_lock();
        let _rust_log = EnvGuard::clear("RUST_LOG");
        let _log_format = EnvGuard::clear("LOG_FORMAT");
        let mut guards = Vec::new();

        for (service, port) in SERVICES {
            guards.push(EnvGuard::clear(env_var_name(service, "grpc_addr")));
            guards.push(EnvGuard::clear(env_var_name(service, "metrics_addr")));
            // Clear nested peer overrides that compose may inject in the process env.
            for peer in ["chain", "attestation", "storage", "p2p", "engine"] {
                guards.push(EnvGuard::clear(env_var_name(
                    service,
                    &format!("peers.{peer}"),
                )));
            }

            let path = workspace_config(service);
            let cfg: PerServiceConfig = load_from(service, &path)
                .unwrap_or_else(|e| panic!("load committed {service}.toml: {e}"));
            assert_eq!(cfg.service.grpc_addr.port(), port, "{service} grpc port");
            assert_eq!(
                cfg.service.metrics_addr.port(),
                port + 100,
                "{service} metrics = grpc + 100"
            );
        }

        drop(guards);
    }
}
