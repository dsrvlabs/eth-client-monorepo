//! Emit the production half of `backfill.rs` so serve.rs can compile here
//! without running `S2-B-02` tests (the file itself stays in `services/storage`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

fn main() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../services/storage/src/backfill.rs");
    println!("cargo:rerun-if-changed={}", src.display());
    let text = std::fs::read_to_string(&src).expect("read services/storage/src/backfill.rs");
    let prod = text
        .split("#[cfg(test)]")
        .next()
        .expect("backfill.rs production prefix");
    // `include!` into `mod backfill` — crate-level inner docs/attrs become invalid.
    let mut sanitized = String::new();
    for line in prod.lines() {
        let t = line.trim_start();
        if t.starts_with("//!") || t.starts_with("#![") {
            continue;
        } else {
            sanitized.push_str(line);
        }
        sanitized.push('\n');
    }
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"))
        .join("backfill_prod.rs");
    std::fs::write(&out, sanitized).expect("write backfill_prod.rs");
}
