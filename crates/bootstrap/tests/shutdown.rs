//! CC-01/4 — spawn a representative service binary, wait for aggregate SERVING,
//! SIGTERM, assert exit 0 within 5 s (Architecture §4.5).
//!
//! Uses `cc-chain` (no health peers → aggregate SERVING as soon as bound).
//! The binary is resolved from the cargo target directory; if missing, this test
//! builds `-p cc-chain` so `cargo nextest run --workspace` is self-contained.
// `kill(2)` for SIGTERM to the child; exclusive to this nextest process.
#![allow(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use cc_bootstrap::AGGREGATE_HEALTH;
use tonic::transport::Endpoint;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_client::HealthClient;

/// Drain + NOT_SERVING pause budget from serve, plus a little slack.
const EXIT_BUDGET: Duration = Duration::from_secs(5);

/// Kill the child on drop so a failed assertion does not leave a stray process.
struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn spawn(mut cmd: Command) -> Self {
        let child = cmd.spawn().expect("spawn cc-chain");
        Self(Some(child))
    }

    fn id(&self) -> u32 {
        self.0.as_ref().expect("child present").id()
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.0.as_mut().expect("child present").try_wait()
    }

    fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.0.as_mut().expect("child present").wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            match child.try_wait() {
                Ok(Some(_)) => {}
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root from CARGO_MANIFEST_DIR")
}

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

/// Ensure the production `cc-chain` binary exists (not the test harness).
fn ensure_cc_chain_bin(root: &Path) -> PathBuf {
    if let Some(p) = find_cc_chain(root) {
        return p;
    }
    // Nested cargo is free once nextest has finished its compile phase.
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
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn health_status(addr: SocketAddr, service: &str) -> Result<i32, tonic::Status> {
    let uri = format!("http://{addr}");
    let channel = Endpoint::from_shared(uri)
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: service.to_owned(),
        })
        .await?;
    Ok(resp.into_inner().status)
}

async fn wait_serving(addr: SocketAddr, budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        match health_status(addr, AGGREGATE_HEALTH).await {
            Ok(status) if status == WireStatus::Serving as i32 => return,
            Ok(status) => {
                if Instant::now() >= deadline {
                    panic!(
                        "aggregate health at {addr} still {status}, want SERVING within {budget:?}"
                    );
                }
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    panic!("aggregate health at {addr} unreachable: {e} within {budget:?}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Send SIGTERM to `pid` (Unix). Returns whether the signal was delivered.
#[cfg(unix)]
fn send_sigterm(pid: u32) -> bool {
    // SAFETY: kill(pid, SIGTERM) on a child we own; async-signal-safe.
    let rc = unsafe { extern_kill(pid as i32, 15) };
    rc == 0
}

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn extern_kill(pid: i32, sig: i32) -> i32;
}

/// CC-01/4: real service binary exits 0 within 5 s of SIGTERM after SERVING.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_binary_sigterm_exits_zero_within_5s() {
    let root = workspace_root();
    let bin = ensure_cc_chain_bin(&root);

    let grpc = ephemeral();
    let metrics = ephemeral();

    let mut child = ChildGuard::spawn({
        let mut cmd = Command::new(&bin);
        cmd.current_dir(&root)
            .env("CC_CHAIN_GRPC_ADDR", grpc.to_string())
            .env("CC_CHAIN_METRICS_ADDR", metrics.to_string())
            .env("RUST_LOG", "info")
            .env("LOG_FORMAT", "json")
            // Quiet child logs in nextest output unless the test fails.
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd
    });

    wait_serving(grpc, Duration::from_secs(10)).await;

    #[cfg(unix)]
    {
        assert!(
            send_sigterm(child.id()),
            "kill(SIGTERM) failed for pid {}",
            child.id()
        );
    }
    #[cfg(not(unix))]
    {
        let _ = child;
        panic!("SIGTERM shutdown test requires Unix");
    }

    let deadline = Instant::now() + EXIT_BUDGET;
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            panic!("cc-chain did not exit within {EXIT_BUDGET:?} of SIGTERM");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert!(
        status.success(),
        "cc-chain must exit 0 on SIGTERM, got {status}"
    );
    // Consume wait so Drop does not kill a reaped child.
    let _ = child.wait();
}
