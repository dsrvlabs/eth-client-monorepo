//! S1-B-02: production FastpathLane blob-count bound comes from ChainConfig.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

#[test]
fn production_main_wires_blob_bound_from_chain_config() {
    let src = include_str!("../../../crates/engine-api/src/api.rs");
    let production = src.split("#[cfg(test)]").next().expect("production half");
    let compact: String = production.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        compact.contains("load_network_chain_config"),
        "production main must sandbox-load ChainConfig from network_config"
    );
    assert!(
        compact.contains("BlobBound::from_chain_config(&self.chain)")
            || compact.contains("BlobBound::from_chain_config(&chain)"),
        "production FastpathLane bound must come from ChainConfig"
    );
    assert!(
        !compact.contains("from_yaml_file"),
        "production must not call unsandboxed ChainConfig::from_yaml_file"
    );
    assert!(
        !production.contains("include_str!"),
        "production must not embed a compiled YAML fallback"
    );
    let fixture_ident = concat!("hoodi_blob", "_bound");
    assert!(
        !src.contains(fixture_ident),
        "production main must not call the test fixture {fixture_ident}"
    );
    assert!(
        !include_str!("../src/lib.rs").contains(fixture_ident),
        "cc-engine must not re-export the test fixture"
    );

    let api_mod = include_str!("../../../crates/engine-api/src/fastpath/mod.rs");
    let api_prod = api_mod
        .split("#[cfg(test)]")
        .next()
        .expect("fastpath/mod.rs has a test module");
    assert!(
        !api_prod.contains(fixture_ident),
        "cc-engine-api production items must not name the test fixture"
    );
    assert!(
        !include_str!("../../../crates/engine-api/src/fastpath/fetch.rs").contains(fixture_ident),
        "fetch.rs must not name the test fixture"
    );
}

#[test]
fn engine_toml_declares_network_config() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/engine.toml");
    let text = std::fs::read_to_string(path).expect("engine.toml");
    assert!(
        text.contains("network_config"),
        "config/engine.toml must declare network_config for the blob-count gate"
    );
    let value: toml::Value = text.parse().expect("engine.toml parses");
    let yaml = value
        .get("network_config")
        .and_then(|v| v.as_str())
        .expect("root-level network_config");
    assert!(
        yaml.ends_with("hoodi-config.yaml") || yaml.ends_with(".yaml"),
        "network_config must be a consensus-specs YAML path, got {yaml}"
    );

    // Same flatten layout as the binary — the YAML path must not be swallowed.
    #[derive(Debug, serde::Deserialize)]
    struct EngineFile {
        network_config: std::path::PathBuf,
        #[serde(flatten)]
        _service: cc_config::ServiceConfig,
        #[serde(flatten)]
        _transport: cc_engine::config::EngineTransportConfig,
    }
    let parsed: EngineFile = toml::from_str(&text).expect("engine.toml deserialises");
    assert_eq!(parsed.network_config.as_os_str(), yaml);

    let missing = r#"
grpc_addr = "127.0.0.1:1"
metrics_addr = "127.0.0.1:2"
log_format = "json"
log_filter = "info"
el_endpoint = "http://127.0.0.1:8551"
"#;
    let err = toml::from_str::<EngineFile>(missing).expect_err("missing network_config");
    let msg = err.to_string();
    assert!(
        msg.contains("network_config"),
        "missing key must name network_config: {msg}"
    );
}

fn hoodi_yaml_path() -> std::path::PathBuf {
    // Walk up from the crate dir so the path has no `..` components
    // (the sandbox refuses ParentDir).
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p.push("crates/types/tests/fixtures/hoodi-config.yaml");
    p
}

#[test]
fn loaded_network_config_feeds_blob_bound() {
    let chain = cc_engine::load_network_chain_config(hoodi_yaml_path(), None).expect("hoodi yaml");
    let bound = cc_engine::BlobBound::from_chain_config(&chain).expect("hoodi bound");
    let pre = bound.get_blob_parameters(cc_types::Epoch::new(0));
    assert_eq!(
        pre.max_blobs_per_block, chain.max_blobs_per_block_electra,
        "pre-schedule bound is ChainConfig.max_blobs_per_block_electra (S0-A-07)"
    );
    assert_eq!(pre.epoch, chain.electra_fork_epoch);
}

#[test]
fn network_config_load_is_fail_closed_and_sandboxed() {
    let jwt_hex = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";

    let missing = cc_engine::load_network_chain_config(
        Path::new("/no/such/cc-engine-network-config.yaml"),
        None,
    )
    .expect_err("missing file");
    assert_eq!(missing, cc_engine::NetworkConfigError::Unreadable);
    assert!(
        !missing.to_string().contains("jwt"),
        "missing-file error must not mention JWT: {missing}"
    );

    let escape = cc_engine::validate_network_config_path(Path::new("../secrets/jwt.hex"))
        .expect_err(".. must be refused");
    assert_eq!(escape, cc_engine::NetworkConfigError::PathEscape);
    assert!(
        !escape.to_string().contains("jwt") && !escape.to_string().contains("secrets"),
        "path-escape error must not embed the JWT path: {escape}"
    );

    let tmp = std::env::temp_dir().join(format!(
        "cc-engine-netcfg-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&tmp).unwrap();
    let hex_file = tmp.join("looks-like-jwt.hex");
    std::fs::write(&hex_file, jwt_hex).unwrap();
    let yaml_err =
        cc_engine::load_network_chain_config(&hex_file, None).expect_err("hex is not YAML");
    assert_eq!(yaml_err, cc_engine::NetworkConfigError::Yaml);
    let yaml_msg = yaml_err.to_string();
    assert!(
        !yaml_msg.contains(jwt_hex) && !yaml_msg.contains("aabbcc"),
        "YAML error must not embed JWT hex: {yaml_msg}"
    );
    assert!(
        !yaml_msg.contains("jwt") && !yaml_msg.contains(hex_file.to_string_lossy().as_ref()),
        "YAML error must not embed the JWT path: {yaml_msg}"
    );

    let collide = cc_engine::load_network_chain_config(&hex_file, Some(hex_file.as_path()))
        .expect_err("same path as JWT");
    assert_eq!(collide, cc_engine::NetworkConfigError::JwtSecretCollision);
    assert!(
        !collide.to_string().contains("jwt.hex") && !collide.to_string().contains(jwt_hex),
        "collision error must not name the JWT path or hex: {collide}"
    );

    let dir_err = cc_engine::load_network_chain_config(&tmp, None).expect_err("dir");
    assert_eq!(dir_err, cc_engine::NetworkConfigError::NotRegularFile);

    if Path::new("/dev/zero").exists() {
        let zero = cc_engine::load_network_chain_config(Path::new("/dev/zero"), None)
            .expect_err("/dev/zero");
        assert_eq!(zero, cc_engine::NetworkConfigError::NotRegularFile);
    }

    let _ = std::fs::remove_file(&hex_file);
    let _ = std::fs::remove_dir_all(&tmp);
}
