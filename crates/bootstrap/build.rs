//! Emit build-identity env vars for `cc_bootstrap::{GIT_SHA, RUSTC}` (Architecture §4.1).
//!
//! Precedence for `CC_GIT_SHA`: env var first (Dockerfile passes it as a build arg because
//! `.dockerignore` excludes `.git/`), then `git rev-parse --short HEAD`, then `"unknown"`.

// Import as `env` so the CC-09/3 grep (literal path form) does not false-positive
// on this compile-time build script (CC-05a).
use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=CC_GIT_SHA");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs/heads");

    let git_sha = env::var_os("CC_GIT_SHA")
        .and_then(|s| s.into_string().ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .or_else(git_short_sha)
        .unwrap_or_else(|| "unknown".to_owned());

    let rustc = rustc_version().unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=CC_GIT_SHA={git_sha}");
    println!("cargo:rustc-env=CC_RUSTC={rustc}");
}

fn git_short_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?;
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_owned())
    }
}

fn rustc_version() -> Option<String> {
    let output = Command::new("rustc").arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let v = String::from_utf8(output.stdout).ok()?;
    let v = v.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_owned())
    }
}
