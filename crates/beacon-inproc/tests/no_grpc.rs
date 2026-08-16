//! S2-A-13: mechanical assertion — this test binary's tree has no tonic / cc-proto.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::process::Command;

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

#[test]
fn cargo_tree_has_no_tonic_or_cc_proto() {
    let script = workspace_root().join("scripts/check-no-grpc-beacon-inproc.sh");
    let status = Command::new("bash")
        .arg(&script)
        .current_dir(workspace_root())
        .status()
        .expect("spawn scripts/check-no-grpc-beacon-inproc.sh");
    assert!(
        status.success(),
        "S2-A-13: cc-beacon-inproc cargo tree must not contain tonic or cc-proto"
    );
}
