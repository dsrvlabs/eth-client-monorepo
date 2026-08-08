//! `cc-store-bench` — CC-40 falsifier (Architecture §8.2).
//!
//! Builds a synthetic store of one full column retention window at `cgc = 8`
//! (~1.05 M values / ~28 GiB at scale=1), then runs 64 prune-under-load epochs
//! with write-behind at ~400 KB/slot. Reports p99 prune-commit latency and
//! file/live-set ratio for `--layout {sharded|flat}`.
//!
//! Disk rule (D-11): one layout at a time; only the known `store.redb` file is
//! removed between runs (never recursive delete of an arbitrary path).

mod load;
mod measure;
mod synth;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use cc_store::{Durability, db_file_path};
use clap::{Parser, ValueEnum};

use load::run_prune_under_load;
use measure::Verdict;
use synth::{BuildStats, DEFAULT_CGC, FULL_RETENTION_EPOCHS, build_store, open_engine};

/// Known store basename created by the engine (SEC-40b-1 allowlist).
const STORE_BASENAME: &str = "store.redb";

/// SEC-40b-2 caps.
const MAX_SCALE: f64 = 2.0;
const MAX_CGC: u16 = 128;
const MAX_PRUNE_EPOCHS: u64 = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Layout {
    /// One table per class; range-delete at epoch boundaries.
    Flat,
    /// One table per shard; `drop_table` at shard boundaries.
    Sharded,
}

impl std::fmt::Display for Layout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Flat => write!(f, "flat"),
            Self::Sharded => write!(f, "sharded"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub layout: Layout,
    pub cgc: u16,
    pub retention_epochs: u64,
    pub prune_epochs: u64,
    pub scale: f64,
}

#[derive(Parser, Debug)]
#[command(
    name = "cc-store-bench",
    about = "CC-40 falsifier: prune-under-load for flat and sharded layouts"
)]
struct Cli {
    /// Key layout under test.
    #[arg(long, value_enum)]
    layout: Layout,

    /// Custody group count (column indices per slot). Max 128 (SEC-40b-2).
    #[arg(long, default_value_t = DEFAULT_CGC)]
    cgc: u16,

    /// Prune-under-load epochs (default 64; max 4096).
    #[arg(long, default_value_t = 64)]
    epochs: u64,

    /// Scale factor for the retention window (0 < scale ≤ 2.0).
    /// Production falsifier uses 1.0 (~28 GiB). CI harness may use e.g. 0.002.
    #[arg(long, default_value_t = 1.0)]
    scale: f64,

    /// Data directory (created if missing). Only `store.redb` inside is deleted
    /// (SEC-40b-1); the directory itself is never recursively removed.
    #[arg(long, default_value = "/tmp/cc-store-bench")]
    data_dir: PathBuf,

    /// Keep `store.redb` after the run.
    #[arg(long, default_value_t = false)]
    keep: bool,

    /// Durability mode: `immediate` | `paranoid` (production), or `none` for
    /// bulk-load smoke only (not a production config value).
    #[arg(long, default_value = "immediate")]
    durability: String,

    /// Skip the build phase and only print planned sizes (dry-run).
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_cli(&cli)?;

    let retention = ((FULL_RETENTION_EPOCHS as f64) * cli.scale).ceil().max(2.0) as u64;
    let prune_epochs = if cli.scale >= 1.0 {
        cli.epochs
    } else {
        cli.epochs.min(retention.max(2)).max(2)
    };

    let cfg = BenchConfig {
        layout: cli.layout,
        cgc: cli.cgc,
        retention_epochs: retention,
        prune_epochs,
        scale: cli.scale,
    };

    let durability = parse_bench_durability(&cli.durability)?;

    println!("cc-store-bench falsifier");
    println!("  layout            = {}", cfg.layout);
    println!("  cgc               = {}", cfg.cgc);
    println!(
        "  retention_epochs  = {} (scale={})",
        cfg.retention_epochs, cfg.scale
    );
    println!("  prune_epochs      = {}", cfg.prune_epochs);
    println!("  durability        = {:?}", durability);
    println!("  data_dir          = {}", cli.data_dir.display());
    print_machine_idle_note();

    let approx_values = cfg
        .retention_epochs
        .saturating_mul(32)
        .saturating_mul(u64::from(cfg.cgc));
    let approx_gi_b =
        (approx_values as f64) * (synth::MEAN_VALUE_BYTES as f64) / (1024.0 * 1024.0 * 1024.0);
    println!(
        "  planned values    ≈ {approx_values} columns + blocks (~{approx_gi_b:.2} GiB column payload)"
    );

    if cli.dry_run {
        println!("dry-run: exiting before open");
        return Ok(());
    }

    // Prepare data dir: never recursive-delete an arbitrary path (SEC-40b-1).
    prepare_data_dir(&cli.data_dir)?;

    let eng = open_engine(&cli.data_dir, durability)?;
    let t_build = Instant::now();
    println!("building synthetic store…");
    let build: BuildStats = build_store(&eng, &cfg)?;
    println!(
        "  build done: puts={} live_bytes={} ({:.2} GiB) in {:.1}s",
        build.puts,
        build.live_bytes,
        (build.live_bytes as f64) / (1024.0 * 1024.0 * 1024.0),
        t_build.elapsed().as_secs_f64()
    );
    println!(
        "  file_len after build = {} ({:.2} GiB)",
        eng.file_len().unwrap_or(0),
        (eng.file_len().unwrap_or(0) as f64) / (1024.0 * 1024.0 * 1024.0)
    );

    println!("prune-under-load ({} epochs)…", cfg.prune_epochs);
    let load = run_prune_under_load(&eng, &cfg, build.live_bytes)?;
    let file_len = eng.file_len().unwrap_or(0);
    let live = load.live_bytes.max(1);
    let ratio = (file_len as f64) / (live as f64);
    let p99 = load.hist.p99_prune_secs();
    let verdict = Verdict::evaluate(p99, ratio);

    println!();
    println!("=== FALSIFIER RESULT layout={} ===", cfg.layout);
    println!(
        "  p99 prune commit latency = {:.4} s ({:.1} ms)",
        p99,
        p99 * 1000.0
    );
    println!(
        "  p99 all commits          = {:.4} s",
        load.hist.p99_all_secs()
    );
    println!("  prune samples            = {}", load.hist.prune_count());
    println!("  all commit samples       = {}", load.hist.all_count());
    println!(
        "  COMMIT_SECONDS buckets   = {}",
        load.hist.format_buckets()
    );
    println!(
        "  ≤0.5s bucket count        = {}  >0.5s = {}",
        load.hist.count_at_or_below_half_second(),
        load.hist.count_above_half_second()
    );
    println!(
        "  file_len                  = {} ({:.2} GiB)",
        file_len,
        (file_len as f64) / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  theoretical live set      = {} ({:.2} GiB)",
        live,
        (live as f64) / (1024.0 * 1024.0 * 1024.0)
    );
    println!("  file / live set           = {ratio:.3}×  (reject if > 2.0)");
    println!("  verdict                   = {}", verdict.as_str());
    println!("=== END ===");

    // Drop engine before deleting the store file.
    drop(eng);
    if !cli.keep {
        delete_store_file_only(&cli.data_dir)?;
        println!("deleted {}", db_file_path(&cli.data_dir).display());
    }

    if !verdict.is_pass() {
        std::process::exit(2);
    }
    Ok(())
}

fn validate_cli(cli: &Cli) -> Result<()> {
    if !(cli.scale > 0.0 && cli.scale <= MAX_SCALE) {
        bail!("--scale must be in (0, {MAX_SCALE}] (got {})", cli.scale);
    }
    if cli.cgc == 0 || cli.cgc > MAX_CGC {
        bail!("--cgc must be in 1..={MAX_CGC} (got {})", cli.cgc);
    }
    if cli.epochs == 0 || cli.epochs > MAX_PRUNE_EPOCHS {
        bail!(
            "--epochs must be in 1..={MAX_PRUNE_EPOCHS} (got {})",
            cli.epochs
        );
    }
    Ok(())
}

/// Bench-only durability parse: production tokens via `Durability::parse`, plus
/// `none` for bulk-load smoke (not accepted by production config parse).
fn parse_bench_durability(s: &str) -> Result<Durability> {
    match s.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(Durability::None),
        other => Durability::parse(other).map_err(|e| anyhow::anyhow!(e)),
    }
}

/// Ensure `data_dir` is a real directory (not a symlink) and remove only a prior
/// `store.redb` regular file (SEC-40b-1).
fn prepare_data_dir(dir: &Path) -> Result<()> {
    if dir.exists() {
        let meta = fs::symlink_metadata(dir).with_context(|| format!("stat {}", dir.display()))?;
        if meta.file_type().is_symlink() {
            bail!("--data-dir must not be a symlink: {}", dir.display());
        }
        if !meta.is_dir() {
            bail!("--data-dir must be a directory: {}", dir.display());
        }
    } else {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        // Refuse if create followed a symlink race into a symlink path.
        let meta = fs::symlink_metadata(dir)?;
        if meta.file_type().is_symlink() {
            bail!(
                "--data-dir resolved to a symlink after create: {}",
                dir.display()
            );
        }
    }
    delete_store_file_only(dir)?;
    Ok(())
}

/// Delete only the known store basename under `dir`. Never `remove_dir_all`.
fn delete_store_file_only(dir: &Path) -> Result<()> {
    let store = dir.join(STORE_BASENAME);
    // Path policy: basename must match exactly (no traversal via odd joins).
    if store.file_name().and_then(|n| n.to_str()) != Some(STORE_BASENAME) {
        bail!(
            "internal error: refuse unexpected store path {}",
            store.display()
        );
    }
    if !store.exists() {
        return Ok(());
    }
    let meta = fs::symlink_metadata(&store).with_context(|| format!("stat {}", store.display()))?;
    if meta.file_type().is_symlink() {
        bail!("refusing to delete symlink store file {}", store.display());
    }
    if !meta.is_file() {
        bail!("refusing to delete non-file store path {}", store.display());
    }
    fs::remove_file(&store).with_context(|| format!("remove {}", store.display()))?;
    Ok(())
}

fn print_machine_idle_note() {
    let load = Command::new("uptime")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "(uptime unavailable)".into());
    println!("  machine note        = {}", load.trim());
    println!(
        "  concurrent jobs     = none started by this harness; operator confirms no devnet/build"
    );
}
