//! Codegen for `proto/` via pure-Rust `protox` + `tonic-prost-build` (Architecture §3.4, ADR-04).
//!
//! No `protoc` binary is required anywhere: Docker, CI, or dev machines.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files = walk_protos(&proto_root)?;
    let out = PathBuf::from(env::var("OUT_DIR")?);

    let fds = protox::compile(&files, [&proto_root])?;

    // `compile_fds` does not honour `file_descriptor_set_path` (that path is for the
    // protoc-driven path). Write the descriptor ourselves for tonic-reflection (CC-05b).
    fs::write(out.join("eth_descriptor.bin"), fds.encode_to_vec())?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true) // peer prober (CC-05b) and Phase 1 both need clients
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
