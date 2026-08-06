//! CC-11d KZG criterion matrix: backend × UsePrecomp × batch shape.
//!
//! Axes (Architecture §4.4):
//! - backend: `c-kzg` 2.1.8, `rust_eth_kzg` 0.10.0
//! - precompute: off / on (c-kzg `precompute=0|8`; rust_eth_kzg `UsePrecomp::No|Yes{width:8}`)
//! - batch: Phase 1 = 1 column × 1 blob; Phase 2 = 8 columns × 21 blobs (Hoodi BPO max)
//!
//! Primary: `verify_cell_kzg_proof_batch`. Also: `compute_cells_and_kzg_proofs`,
//! `recover_cells_and_kzg_proofs` (one size each), and setup-load timing.
//!
//! Compiles under each backend feature alone (empty groups for the missing
//! backend). The full matrix is run with both features by `scripts/bench-kzg.sh`.
//!
//! Sample count is reduced vs criterion defaults so the matrix finishes on a
//! laptop; override with `KZG_BENCH_SAMPLES` (min 10).

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use cc_crypto::{Blob, CellKzg};
use cc_types::{Cell, KzgCommitment, KzgProof, CELLS_PER_EXT_BLOB};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Hoodi current BPO max blobs per slot (Architecture §4.4).
const HOODI_BPO_MAX_BLOBS: usize = 21;
/// Phase 2 sampling columns (CC-24 inherits this shape).
const PHASE2_COLUMNS: usize = 8;
/// Precompute width / c-kzg wbits for the "on" axis (matches rust_eth_kzg examples).
#[allow(dead_code)] // used under kzg-rust-eth-kzg; c-kzg path uses C_KZG_PRECOMP_ON
const PRECOMP_WIDTH: usize = 8;
/// c-kzg precompute parameter for the "on" axis (`0..=15`).
#[allow(dead_code)] // used under kzg-c-kzg
const C_KZG_PRECOMP_ON: u64 = PRECOMP_WIDTH as u64;

/// Criterion sample size (env `KZG_BENCH_SAMPLES`, default 15, min 10).
fn sample_size() -> usize {
    std::env::var("KZG_BENCH_SAMPLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15)
        .max(10)
}

fn measurement_time() -> Duration {
    let secs: u64 = std::env::var("KZG_BENCH_MEASURE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    Duration::from_secs(secs.max(1))
}

fn warm_up_time() -> Duration {
    Duration::from_secs(1)
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    group
        .sample_size(sample_size())
        .measurement_time(measurement_time())
        .warm_up_time(warm_up_time())
        .noise_threshold(0.05);
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn blob_for(seed: u8) -> Blob {
    Blob::filled(seed.saturating_add(1))
}

struct VerifyFixture {
    commitments: Vec<KzgCommitment>,
    cell_indices: Vec<u64>,
    cells: Vec<Cell>,
    proofs: Vec<KzgProof>,
}

impl VerifyFixture {
    fn phase1(backend: &impl CellKzg) -> Self {
        let blob = blob_for(1);
        let commitment = backend.blob_to_kzg_commitment(&blob).expect("commitment");
        let (cells, proofs) = backend
            .compute_cells_and_kzg_proofs(&blob)
            .expect("cells+proofs");
        Self {
            commitments: vec![commitment],
            cell_indices: vec![0],
            cells: vec![cells[0]],
            proofs: vec![proofs[0]],
        }
    }

    /// 8 columns × 21 blobs = 168 cells (Phase 2 / CC-24 sampling shape).
    fn phase2(backend: &impl CellKzg) -> Self {
        let mut commitments = Vec::with_capacity(PHASE2_COLUMNS * HOODI_BPO_MAX_BLOBS);
        let mut cell_indices = Vec::with_capacity(PHASE2_COLUMNS * HOODI_BPO_MAX_BLOBS);
        let mut cells_out = Vec::with_capacity(PHASE2_COLUMNS * HOODI_BPO_MAX_BLOBS);
        let mut proofs_out = Vec::with_capacity(PHASE2_COLUMNS * HOODI_BPO_MAX_BLOBS);

        for b in 0..HOODI_BPO_MAX_BLOBS {
            let blob = blob_for((b as u8).wrapping_mul(3).saturating_add(1));
            let commitment = backend.blob_to_kzg_commitment(&blob).expect("commitment");
            let (cells, proofs) = backend
                .compute_cells_and_kzg_proofs(&blob)
                .expect("cells+proofs");
            for col in 0..PHASE2_COLUMNS {
                commitments.push(commitment);
                cell_indices.push(col as u64);
                cells_out.push(cells[col]);
                proofs_out.push(proofs[col]);
            }
        }
        Self {
            commitments,
            cell_indices,
            cells: cells_out,
            proofs: proofs_out,
        }
    }
}

struct RecoverFixture {
    cell_indices: Vec<u64>,
    cells: Vec<Cell>,
}

impl RecoverFixture {
    /// Half the extended cells (worst-case reconstruction input size).
    fn half(backend: &impl CellKzg) -> Self {
        let blob = blob_for(7);
        let (cells, _) = backend
            .compute_cells_and_kzg_proofs(&blob)
            .expect("cells+proofs");
        let half = CELLS_PER_EXT_BLOB / 2;
        let cell_indices: Vec<u64> = (0..half as u64).collect();
        let subset: Vec<Cell> = cell_indices.iter().map(|&i| cells[i as usize]).collect();
        Self {
            cell_indices,
            cells: subset,
        }
    }
}

fn rss_bytes() -> Option<u64> {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&output.stdout);
    s.trim()
        .parse::<u64>()
        .ok()
        .map(|kb| kb.saturating_mul(1024))
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1 << 30 {
        format!("{:.2} GiB", bytes as f64 / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.2} MiB", bytes as f64 / (1u64 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.2} KiB", bytes as f64 / (1u64 << 10) as f64)
    } else {
        format!("{bytes} B")
    }
}

fn print_setup_memory_line(label: &str, load: impl FnOnce()) {
    let before = rss_bytes();
    let t0 = Instant::now();
    load();
    let elapsed = t0.elapsed();
    std::thread::sleep(Duration::from_millis(20));
    let after = rss_bytes();
    let delta = match (before, after) {
        (Some(b), Some(a)) if a >= b => Some(a - b),
        _ => None,
    };
    let delta_s = delta
        .map(format_bytes)
        .unwrap_or_else(|| "n/a".to_string());
    let before_s = before
        .map(format_bytes)
        .unwrap_or_else(|| "n/a".to_string());
    let after_s = after.map(format_bytes).unwrap_or_else(|| "n/a".to_string());
    println!(
        "kzg-setup-memory\t{label}\twall_ms={:.3}\trss_before={before_s}\trss_after={after_s}\trss_delta={delta_s}",
        elapsed.as_secs_f64() * 1000.0
    );
}

fn measure_setup_memory() {
    println!("kzg-setup-memory-begin");

    #[cfg(feature = "kzg-c-kzg")]
    {
        use cc_crypto::CKzgBackend;
        let _ = CKzgBackend::load(0);
    }
    #[cfg(feature = "kzg-rust-eth-kzg")]
    {
        use cc_crypto::{RustEthKzgBackend, UsePrecomp};
        let _ = RustEthKzgBackend::load(UsePrecomp::No);
    }
    std::thread::sleep(Duration::from_millis(50));

    #[cfg(feature = "kzg-c-kzg")]
    {
        use cc_crypto::CKzgBackend;
        print_setup_memory_line("c-kzg/precompute-0", || {
            let _ = CKzgBackend::load(0);
        });
        print_setup_memory_line("c-kzg/precompute-8", || {
            let _ = CKzgBackend::load(C_KZG_PRECOMP_ON);
        });
    }
    #[cfg(feature = "kzg-rust-eth-kzg")]
    {
        use cc_crypto::{RustEthKzgBackend, UsePrecomp};
        print_setup_memory_line("rust_eth_kzg/precomp-off", || {
            let _ = RustEthKzgBackend::load(UsePrecomp::No);
        });
        print_setup_memory_line("rust_eth_kzg/precomp-on-w8", || {
            let _ = RustEthKzgBackend::load(UsePrecomp::Yes {
                width: PRECOMP_WIDTH,
            });
        });
    }

    println!("kzg-setup-memory-end");
    println!(
        "kzg-setup-constants\tprecomp_width={PRECOMP_WIDTH}\thoodi_bpo_max_blobs={HOODI_BPO_MAX_BLOBS}\tphase2_columns={PHASE2_COLUMNS}"
    );
}

fn bench_verify_for(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>, label_prefix: &str, backend: &impl CellKzg) {
    let p1 = VerifyFixture::phase1(backend);
    let p2 = VerifyFixture::phase2(backend);

    group.throughput(Throughput::Elements(1));
    group.bench_with_input(
        BenchmarkId::new(label_prefix, "phase1-1col-1blob"),
        &p1,
        |b, fix| {
            b.iter(|| {
                backend
                    .verify_cell_kzg_proof_batch(
                        black_box(&fix.commitments),
                        black_box(&fix.cell_indices),
                        black_box(&fix.cells),
                        black_box(&fix.proofs),
                    )
                    .expect("verify")
            })
        },
    );

    group.throughput(Throughput::Elements(
        (PHASE2_COLUMNS * HOODI_BPO_MAX_BLOBS) as u64,
    ));
    group.bench_with_input(
        BenchmarkId::new(label_prefix, "phase2-8col-21blob"),
        &p2,
        |b, fix| {
            b.iter(|| {
                backend
                    .verify_cell_kzg_proof_batch(
                        black_box(&fix.commitments),
                        black_box(&fix.cell_indices),
                        black_box(&fix.cells),
                        black_box(&fix.proofs),
                    )
                    .expect("verify")
            })
        },
    );
}

fn bench_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("verify_cell_kzg_proof_batch");
    configure_group(&mut group);

    #[cfg(feature = "kzg-c-kzg")]
    {
        use cc_crypto::CKzgBackend;
        for (precomp_label, precompute) in
            [("precompute-0", 0u64), ("precompute-8", C_KZG_PRECOMP_ON)]
        {
            let backend = CKzgBackend::load(precompute).expect("load c-kzg");
            bench_verify_for(
                &mut group,
                &format!("c-kzg/{precomp_label}"),
                &backend,
            );
        }
    }

    #[cfg(feature = "kzg-rust-eth-kzg")]
    {
        use cc_crypto::{RustEthKzgBackend, UsePrecomp};
        for (precomp_label, use_precomp) in [
            ("precomp-off", UsePrecomp::No),
            (
                "precomp-on-w8",
                UsePrecomp::Yes {
                    width: PRECOMP_WIDTH,
                },
            ),
        ] {
            let backend = RustEthKzgBackend::load(use_precomp).expect("load rust_eth_kzg");
            bench_verify_for(
                &mut group,
                &format!("rust_eth_kzg/{precomp_label}"),
                &backend,
            );
        }
    }

    group.finish();
}

fn bench_compute(c: &mut Criterion) {
    let mut group = c.benchmark_group("compute_cells_and_kzg_proofs");
    configure_group(&mut group);
    group.throughput(Throughput::Elements(1));

    let blob = blob_for(11);

    #[cfg(feature = "kzg-c-kzg")]
    {
        use cc_crypto::CKzgBackend;
        for (precomp_label, precompute) in
            [("precompute-0", 0u64), ("precompute-8", C_KZG_PRECOMP_ON)]
        {
            let backend = CKzgBackend::load(precompute).expect("load c-kzg");
            group.bench_function(format!("c-kzg/{precomp_label}"), |b| {
                b.iter(|| {
                    backend
                        .compute_cells_and_kzg_proofs(black_box(&blob))
                        .expect("compute")
                })
            });
        }
    }

    #[cfg(feature = "kzg-rust-eth-kzg")]
    {
        use cc_crypto::{RustEthKzgBackend, UsePrecomp};
        for (precomp_label, use_precomp) in [
            ("precomp-off", UsePrecomp::No),
            (
                "precomp-on-w8",
                UsePrecomp::Yes {
                    width: PRECOMP_WIDTH,
                },
            ),
        ] {
            let backend = RustEthKzgBackend::load(use_precomp).expect("load rust_eth_kzg");
            group.bench_function(format!("rust_eth_kzg/{precomp_label}"), |b| {
                b.iter(|| {
                    backend
                        .compute_cells_and_kzg_proofs(black_box(&blob))
                        .expect("compute")
                })
            });
        }
    }

    group.finish();
}

fn bench_recover(c: &mut Criterion) {
    let mut group = c.benchmark_group("recover_cells_and_kzg_proofs");
    configure_group(&mut group);
    group.throughput(Throughput::Elements((CELLS_PER_EXT_BLOB / 2) as u64));

    #[cfg(feature = "kzg-c-kzg")]
    {
        use cc_crypto::CKzgBackend;
        for (precomp_label, precompute) in
            [("precompute-0", 0u64), ("precompute-8", C_KZG_PRECOMP_ON)]
        {
            let backend = CKzgBackend::load(precompute).expect("load c-kzg");
            let fix = RecoverFixture::half(&backend);
            group.bench_function(format!("c-kzg/{precomp_label}"), |b| {
                b.iter(|| {
                    backend
                        .recover_cells_and_kzg_proofs(
                            black_box(&fix.cell_indices),
                            black_box(&fix.cells),
                        )
                        .expect("recover")
                })
            });
        }
    }

    #[cfg(feature = "kzg-rust-eth-kzg")]
    {
        use cc_crypto::{RustEthKzgBackend, UsePrecomp};
        for (precomp_label, use_precomp) in [
            ("precomp-off", UsePrecomp::No),
            (
                "precomp-on-w8",
                UsePrecomp::Yes {
                    width: PRECOMP_WIDTH,
                },
            ),
        ] {
            let backend = RustEthKzgBackend::load(use_precomp).expect("load rust_eth_kzg");
            let fix = RecoverFixture::half(&backend);
            group.bench_function(format!("rust_eth_kzg/{precomp_label}"), |b| {
                b.iter(|| {
                    backend
                        .recover_cells_and_kzg_proofs(
                            black_box(&fix.cell_indices),
                            black_box(&fix.cells),
                        )
                        .expect("recover")
                })
            });
        }
    }

    group.finish();
}

fn bench_setup_load(c: &mut Criterion) {
    measure_setup_memory();

    let mut group = c.benchmark_group("setup_load");
    group
        .sample_size(sample_size().min(12))
        .measurement_time(measurement_time())
        .warm_up_time(Duration::from_millis(500));

    #[cfg(feature = "kzg-c-kzg")]
    {
        use cc_crypto::CKzgBackend;
        group.bench_function("c-kzg/precompute-0", |b| {
            b.iter(|| black_box(CKzgBackend::load(0).expect("load")))
        });
        group.bench_function("c-kzg/precompute-8", |b| {
            b.iter(|| black_box(CKzgBackend::load(C_KZG_PRECOMP_ON).expect("load")))
        });
    }

    #[cfg(feature = "kzg-rust-eth-kzg")]
    {
        use cc_crypto::{RustEthKzgBackend, UsePrecomp};
        group.bench_function("rust_eth_kzg/precomp-off", |b| {
            b.iter(|| black_box(RustEthKzgBackend::load(UsePrecomp::No).expect("load")))
        });
        group.bench_function("rust_eth_kzg/precomp-on-w8", |b| {
            b.iter(|| {
                black_box(
                    RustEthKzgBackend::load(UsePrecomp::Yes {
                        width: PRECOMP_WIDTH,
                    })
                    .expect("load"),
                )
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_setup_load,
    bench_verify,
    bench_compute,
    bench_recover
);
criterion_main!(benches);
