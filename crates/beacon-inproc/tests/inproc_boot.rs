//! S2-A-13: in-process boot of the beacon-core boot path against one TempDir,
//! one redb. No gRPC. Import → durable is S2-A-14.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cc_beacon_inproc::{BootConfig, BootPhase, boot_in_process};
use cc_store::Root;

fn unique_temp_dir(prefix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let seq = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{}-{seq}-{nanos}", std::process::id()))
}

fn boot_cfg(dir: PathBuf, node_key: Option<PathBuf>) -> BootConfig {
    BootConfig {
        data_dir: dir,
        durability: "immediate".to_owned(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: None,
        node_key_path: node_key,
    }
}

#[test]
fn boot_in_process_one_tempdir_one_redb() {
    let dir = unique_temp_dir("s2-a-13-boot");
    std::fs::create_dir_all(&dir).unwrap();
    let booted = boot_in_process(&boot_cfg(dir.clone(), None)).expect("boot");
    assert_eq!(booted.phases.first().copied(), Some(BootPhase::Open));
    assert_eq!(
        booted.phases,
        [BootPhase::Open, BootPhase::DurableSet],
        "open before durable_set; no writer/gRPC phase: {:?}",
        booted.phases
    );
    assert!(
        booted.durable_empty,
        "fresh TempDir must be an empty durable set"
    );

    let err = boot_in_process(&boot_cfg(dir.clone(), None))
        .expect_err("second live opener of the same redb must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("locked") || msg.contains("Database") || msg.contains("store open"),
        "one redb exclusive handle, got: {msg}"
    );
    drop(booted);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn second_opener_of_same_data_dir_fails_inode_id() {
    let dir = unique_temp_dir("s2-a-13-inode");
    std::fs::create_dir_all(&dir).unwrap();
    let key_a = dir.join("node_key");
    let id_a = Root::from_array([0x11u8; 32]);
    std::fs::write(&key_a, id_a.as_slice()).unwrap();

    let first = boot_in_process(&boot_cfg(dir.clone(), Some(key_a))).expect("first boot");
    drop(first);

    let key_b = dir.join("node_key_b");
    std::fs::write(&key_b, [0x22u8; 32]).unwrap();
    let err = boot_in_process(&boot_cfg(dir.clone(), Some(key_b)))
        .expect_err("I-node-id must refuse a second identity");
    let msg = err.to_string();
    assert!(
        msg.contains("node_id") || msg.contains("I-node-id"),
        "second opener must fail I-node-id, got: {msg}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
