//! CC-43 /5: store source must not name payload reconstruction paths.
//!
//! Integration test (outside `src/`) so the banned tokens can appear in the
//! assertion without violating `grep -rn … crates/store/src/`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

#[test]
fn store_src_has_no_payload_reconstruction_surface() {
    let store_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let needles = ["reconstruct", "blinded", "payload_header"];
    let mut hits = Vec::new();
    walk(&store_src, &mut |path| {
        let text = std::fs::read_to_string(path).unwrap();
        for n in needles {
            if text.contains(n) {
                hits.push(format!("{}:{n}", path.display()));
            }
        }
    });
    assert!(
        hits.is_empty(),
        "crates/store/src must not mention {:?}: {hits:?}",
        needles
    );
}

fn walk(dir: &std::path::Path, f: &mut dyn FnMut(&std::path::Path)) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, f);
            } else if p.extension().and_then(|x| x.to_str()) == Some("rs") {
                f(&p);
            }
        }
    }
}
