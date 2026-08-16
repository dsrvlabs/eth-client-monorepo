//! S2-J-01: one process opens redb before any subsystem; I-node-id on a
//! second opener; one writer.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cc_beacon_core::{BootConfig, BootPhase, boot_in_process};
use cc_storage_core::StorageMetrics;
use cc_types::primitives::Root;
use prometheus_client::registry::Registry;

fn unique_temp_dir(prefix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let seq = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{}-{seq}-{nanos}", std::process::id()))
}

fn metrics() -> StorageMetrics {
    let mut registry = Registry::default();
    StorageMetrics::register(&mut registry)
}

fn boot_cfg(dir: PathBuf, node_key: Option<PathBuf>) -> BootConfig {
    BootConfig {
        data_dir: dir,
        durability: "immediate".to_owned(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: None,
        node_key_path: node_key,
        writer_process_fatal: false,
    }
}

#[tokio::test]
async fn open_completes_before_any_subsystem_starts() {
    let dir = unique_temp_dir("beacon-core-order");
    std::fs::create_dir_all(&dir).unwrap();
    let booted = boot_in_process(&boot_cfg(dir.clone(), None), metrics(), true).unwrap();
    assert_eq!(booted.phases.first().copied(), Some(BootPhase::Open));
    assert!(
        booted.phases.iter().position(|&p| p == BootPhase::Open)
            < booted.phases.iter().position(|&p| p == BootPhase::Writer),
        "writer must start after open: {:?}",
        booted.phases
    );
    assert_eq!(
        booted.storage.as_ref().map(|s| s.writer_count()),
        Some(1),
        "one writer"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn second_opener_of_same_data_dir_fails_inode_id() {
    let dir = unique_temp_dir("beacon-core-inode");
    std::fs::create_dir_all(&dir).unwrap();
    let key_a = dir.join("node_key");
    let id_a = Root::from_array([0x11u8; 32]);
    std::fs::write(&key_a, id_a.as_slice()).unwrap();

    // Production stamp: open_and_stamp persists meta.node_id from the key.
    let first = boot_in_process(&boot_cfg(dir.clone(), Some(key_a)), metrics(), false).unwrap();
    drop(first);

    let key_b = dir.join("node_key_b");
    std::fs::write(&key_b, [0x22u8; 32]).unwrap();
    let err = boot_in_process(&boot_cfg(dir.clone(), Some(key_b)), metrics(), false)
        .expect_err("I-node-id must refuse a second identity");
    let msg = err.to_string();
    assert!(
        msg.contains("node_id") || msg.contains("I-node-id"),
        "second opener must fail I-node-id, got: {msg}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
