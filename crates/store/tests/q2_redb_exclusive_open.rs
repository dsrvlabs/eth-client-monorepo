//! S0a-B-10 / Spike Q-2 — cross-process exclusive open of a redb file.
//!
//! A second opener of the slashing-protection DB must **error**, not block.
//! Process 1 holds [`Engine::open`]; process 2 opens the same data directory.
//! Workspace pin is **redb 4.1.0** (`File::try_lock` → `WouldBlock` →
//! `DatabaseAlreadyOpen` → [`StoreError::DatabaseLocked`]).
//!
//! Production `Engine::open` locking policy is unchanged by this file.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_store::{Engine, EngineOptions, StoreError};

/// Env var: child re-exec of this test binary should try `Engine::open` on this dir.
const CHILD_PATH_ENV: &str = "CC_STORE_Q2_OPEN_PATH";
/// Fail-fast budget. Blocking `open` is the Q-2 failure mode; 3 s is far above a
/// `try_lock` miss and far below a stuck validator-client start.
const CHILD_DEADLINE: Duration = Duration::from_secs(3);
/// Workspace `redb` pin (`Cargo.toml` / `Cargo.lock`).
const REDB_VERSION: &str = "4.1.0";

fn tmp_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("cc-store-q2-{}-{}", std::process::id(), nanos))
}

fn child_open_and_exit(path: &Path) -> ! {
    match Engine::open(path, EngineOptions::default()) {
        Ok(_) => {
            eprintln!("Q2_RESULT=opened");
            std::process::exit(0);
        }
        Err(StoreError::DatabaseLocked) => {
            eprintln!("Q2_RESULT=database_locked");
            std::process::exit(2);
        }
        Err(e) => {
            eprintln!("Q2_RESULT=error:{e}");
            std::process::exit(3);
        }
    }
}

fn strip_nextest_env(cmd: &mut Command) {
    // cargo-nextest talks to its children over inherited env; a re-exec of this
    // test binary must look like a bare rustc harness or it can hang.
    let keys: Vec<String> = std::env::vars()
        .map(|(k, _)| k)
        .filter(|k| k == "NEXTEST" || k.starts_with("NEXTEST_") || k.starts_with("__NEXTEST"))
        .collect();
    for k in keys {
        cmd.env_remove(k);
    }
}

/// Two-process Q-2 experiment: second writer must return immediately with
/// [`StoreError::DatabaseLocked`].
#[test]
fn second_process_open_errors_without_blocking() {
    if let Some(path) = std::env::var_os(CHILD_PATH_ENV) {
        child_open_and_exit(Path::new(&path));
    }

    let dir = tmp_dir();
    let holder = Engine::open(&dir, EngineOptions::default()).unwrap();
    let mut batch = holder.batch();
    batch.put("q2", b"k", b"v");
    holder.commit(batch).unwrap();

    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = Command::new(&exe);
    cmd.env(CHILD_PATH_ENV, &dir)
        .args([
            "--exact",
            "--nocapture",
            "second_process_open_errors_without_blocking",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    strip_nextest_env(&mut cmd);

    let mut child = cmd.spawn().expect("spawn Q-2 child");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if started.elapsed() > CHILD_DEADLINE {
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_dir_all(&dir);
            panic!(
                "Q-2: process 2 blocked for {CHILD_DEADLINE:?} holding redb {REDB_VERSION} open \
                 (would need flock(LOCK_EX|LOCK_NB))"
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let elapsed = started.elapsed();

    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }

    drop(holder);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        stderr.contains("Q2_RESULT=database_locked"),
        "Q-2 redb {REDB_VERSION}: process 2 must fail-fast DatabaseLocked, \
         status={status:?} elapsed={elapsed:?} stderr={stderr:?}"
    );
    assert_eq!(
        status.code(),
        Some(2),
        "Q-2 child exit code: status={status:?} stderr={stderr:?}"
    );
    eprintln!("Q-2 redb {REDB_VERSION}: process 2 DatabaseLocked in {elapsed:?} (fail-fast)");
}
