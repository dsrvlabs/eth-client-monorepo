//! Codegen for `proto/` via pure-Rust `protox` + `tonic-prost-build` (Architecture §3.4, ADR-04).
//!
//! No `protoc` binary is required anywhere: Docker, CI, or dev machines.
//!
//! Include path is `proto/` (workspace packages) plus `proto/third_party/` so the
//! vendored `google/rpc/{status,error_details}.proto` resolve as `google/rpc/…`
//! (CC-18a / ADR-P1-14). Well-known types (`google/protobuf/*`) come from
//! `protox`'s built-in `GoogleFileResolver`.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let third_party = proto_root.join("third_party");
    let files = walk_protos(&proto_root)?;
    let out = PathBuf::from(env::var("OUT_DIR")?);

    // third_party first so vendored google/rpc files are named `google/rpc/…`
    // rather than `third_party/google/rpc/…`.
    let fds = protox::compile(&files, [&third_party, &proto_root])?;

    // `compile_fds` does not honour `file_descriptor_set_path` (that path is for the
    // protoc-driven path). Write the descriptor ourselves for tonic-reflection (CC-05b).
    fs::write(out.join("eth_descriptor.bin"), fds.encode_to_vec())?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true) // peer prober (CC-05b) and Phase 1 both need clients
        // Default method bodies return `UNIMPLEMENTED` so additive RPCs (CC-18a+)
        // do not force every service stub to grow empty handlers. Real logic still
        // overrides the default in the owning issue (CC-18b/c for chain).
        .generate_default_stubs(true)
        .out_dir(&out)
        .compile_fds(fds)?;

    // Rebuild when any proto under the tree changes (or a new file is added).
    println!("cargo:rerun-if-changed={}", proto_root.display());
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }

    Ok(())
}

/// Collect every `.proto` under `root`, sorted for deterministic codegen output (CC-02/1).
fn walk_protos(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    walk_dir(root, &mut files)?;
    files.sort();
    if files.is_empty() {
        return Err(format!("no .proto files found under {}", root.display()).into());
    }
    Ok(files)
}

fn walk_dir(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        // Do not follow symlinks — keeps the walk inside the proto tree.
        if ft.is_symlink() {
            continue;
        }
        let path = entry.path();
        if ft.is_dir() {
            walk_dir(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "proto") {
            out.push(path);
        }
    }
    Ok(())
}
