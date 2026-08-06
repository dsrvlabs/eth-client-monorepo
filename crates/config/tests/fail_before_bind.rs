//! CC-09/2 — a service binary with bad config exits non-zero **before** binding
//! its gRPC (or metrics) port, and names the offending key in the error.
//!
//! Spawns the production `cc-chain` binary under a temp CWD with a known free
//! port pair in `config/chain.toml`, then overrides `CC_CHAIN_GRPC_ADDR` with a
//! malformed value so `cc_config::load` fails inside `main` prior to
//! `cc_bootstrap::init` / `serve`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Locate `cc-chain` next to the test profile, or under `target/{debug,release}/`.
fn find_cc_chain(root: &Path) -> Option<PathBuf> {
    if let Ok(mut exe) = std::env::current_exe() {
        // target/<profile>/deps/<test-bin> → target/<profile>/cc-chain
        exe.pop();
        if exe.file_name().is_some_and(|n| n == "deps") {
            exe.pop();
        }
        exe.push("cc-chain");
        if exe.is_file() {
            return Some(exe);
        }
    }
    for profile in ["debug", "release"] {
        let p = root.join("target").join(profile).join("cc-chain");
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root from CARGO_MANIFEST_DIR")
}

fn ensure_cc_chain_bin(root: &Path) -> PathBuf {
    if let Some(p) = find_cc_chain(root) {
        return p;
    }
    let status = Command::new("cargo")
        .args(["build", "-p", "cc-chain", "--locked"])
        .current_dir(root)
        .status()
        .expect("spawn cargo build -p cc-chain");
    assert!(
        status.success(),
        "cargo build -p cc-chain failed with {status}"
    );
    find_cc_chain(root).expect("cc-chain binary missing after cargo build -p cc-chain")
}

fn ephemeral() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local_addr")
}

fn port_is_listening(addr: SocketAddr) -> bool {
    TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok()
}

fn temp_workdir() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "cc-config-fail-before-bind-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(path.join("config")).unwrap();
    path
}

/// CC-09/2: bad config → non-zero exit, no listener on the intended gRPC port.
#[test]
fn bad_grpc_addr_exits_nonzero_without_binding() {
    let root = workspace_root();
    let bin = ensure_cc_chain_bin(&root);

    // Reserve free ports, then drop the listeners so the service *could* bind
    // them if it incorrectly reached serve().
    let grpc = ephemeral();
    let metrics = ephemeral();

    let work = temp_workdir();
    let toml_path = work.join("config/chain.toml");
    fs::write(
        &toml_path,
        format!(
            r#"
grpc_addr = "{grpc}"
metrics_addr = "{metrics}"
log_format = "json"
log_filter = "info"

[peers]
"#
        ),
    )
    .unwrap();

    // Malformed override wins over the file; load must fail before bind.
    let output = Command::new(&bin)
        .current_dir(&work)
        .env("CC_CHAIN_GRPC_ADDR", "not-a-socket-addr")
        .env("CC_CHAIN_METRICS_ADDR", metrics.to_string())
        .env("RUST_LOG", "info")
        .env("LOG_FORMAT", "json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn cc-chain");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stderr}\n{stdout}");

    assert!(
        !output.status.success(),
        "cc-chain must exit non-zero on bad config, got {}; output:\n{combined}",
        output.status
    );

    assert!(
        combined.contains("grpc_addr")
            || combined.contains("GRPC_ADDR")
            || combined.contains("not-a-socket-addr"),
        "error must name the offending key, got:\n{combined}"
    );

    // Process has exited; neither intended port may have a listener.
    assert!(
        !port_is_listening(grpc),
        "gRPC port {grpc} must not be listening after config failure"
    );
    assert!(
        !port_is_listening(metrics),
        "metrics port {metrics} must not be listening after config failure"
    );

    fs::remove_dir_all(&work).ok();
}

/// Missing required field (no file value, no env) also fails before bind.
#[test]
fn missing_grpc_addr_exits_nonzero_without_binding() {
    let root = workspace_root();
    let bin = ensure_cc_chain_bin(&root);

    let grpc = ephemeral();
    let metrics = ephemeral();

    let work = temp_workdir();
    // Omit grpc_addr entirely.
    fs::write(
        work.join("config/chain.toml"),
        format!(
            r#"
metrics_addr = "{metrics}"
log_format = "json"
log_filter = "info"

[peers]
"#
        ),
    )
    .unwrap();

    let output = Command::new(&bin)
        .current_dir(&work)
        // Explicitly clear any inherited override.
        .env_remove("CC_CHAIN_GRPC_ADDR")
        .env("CC_CHAIN_METRICS_ADDR", metrics.to_string())
        .env("RUST_LOG", "info")
        .env("LOG_FORMAT", "json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn cc-chain");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stderr}\n{stdout}");

    assert!(
        !output.status.success(),
        "cc-chain must exit non-zero when grpc_addr is missing, got {}; output:\n{combined}",
        output.status
    );
    assert!(
        combined.contains("grpc_addr"),
        "error must name missing key grpc_addr, got:\n{combined}"
    );
    assert!(
        combined.contains("CC_CHAIN_GRPC_ADDR"),
        "error must name the env provenance, got:\n{combined}"
    );
    // The free grpc port we never wrote into the file also must be idle; check
    // metrics (which *was* in the file) to prove serve never ran.
    assert!(
        !port_is_listening(metrics),
        "metrics port {metrics} must not be listening after missing-field failure"
    );
    assert!(
        !port_is_listening(grpc),
        "unrelated free port {grpc} must not be listening"
    );

    // Give a slow machine a beat so we do not race a late bind (should not happen).
    let deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < deadline {
        if port_is_listening(metrics) {
            panic!("metrics port {metrics} became listening after config failure");
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    fs::remove_dir_all(&work).ok();
}
