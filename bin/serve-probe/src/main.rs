//! `cc-serve-probe` — wire-only serve-window prober (CC-4B).
//!
//! ```text
//! cc-serve-probe --peer <multiaddr> --fork-digest <hex>
//!     [--slots N | --full-window] [--below N] [--columns 0,1,2,3] [--json path]
//! ```
//!
//! # Dial trust boundary (SEC-4B-4)
//!
//! `--peer` is treated as a **trusted operator input**. The binary dials whatever
//! TCP/IP (or DNS→IP) multiaddr it is given. Feeding untrusted multiaddrs
//! (scraped peer lists, hostile ENRs) is operator-side SSRF / internal recon
//! risk. Prefer explicit `/ip4/…/tcp/…/p2p/…` targets you control or trust.
//! Non-IP transports (e.g. bare Unix sockets) are rejected.

use std::path::PathBuf;
use std::process::ExitCode;

use cc_serve_probe::sample::MAX_SAMPLE_COUNT;
use cc_serve_probe::{ProbeConfig, ProbeReport, run_probe};
use cc_types::ForkDigest;
use clap::Parser;

/// Maximum column indices on `--columns` (spec `NUMBER_OF_COLUMNS` upper use).
const MAX_COLUMNS: usize = 128;

/// Wire-only serve-window prober against a foreign (or local stub) peer.
///
/// **Trust model:** `--peer` multiaddrs and `--json` paths are operator-chosen
/// trusted inputs (SEC-4B-4 / SEC-4B-5). Sample counts and response decode
/// budgets are hard-capped so a hostile dialed peer cannot unbounded-allocate
/// the probe process (SEC-4B-1 … SEC-4B-3).
#[derive(Debug, Parser)]
#[command(
    name = "cc-serve-probe",
    version,
    about = "Wire-only serve-window prober (Status v2 + ByRange blocks/columns)",
    long_about = "Dials a single multiaddr over Noise+yamux, completes Status v2, \
and samples BeaconBlocksByRange / DataColumnSidecarsByRange above and below the \
peer's advertised earliest_available_slot.\n\n\
Trust boundary: --peer is fully attacker-directed if the multiaddr is untrusted \
(SSRF / internal port-scan class). Only pass multiaddrs you intend to contact. \
--json writes a report to an operator-chosen path (atomic temp+rename)."
)]
struct Cli {
    /// Target peer multiaddr (trusted input — see dial trust boundary).
    ///
    /// Must be dialable TCP over IP or DNS (`/ip4|ip6|dns…/tcp/…`); may include
    /// `/p2p/<peer_id>`.
    #[arg(long)]
    peer: String,

    /// Fork digest as 4-byte hex (`0x` optional). Required — never derived.
    #[arg(long)]
    fork_digest: String,

    /// Positive-side sample count from `[eas, head]` (default 1000, max 10000).
    #[arg(long, default_value_t = 1000, value_parser = parse_sample_count)]
    slots: usize,

    /// Sample up to the hard ceiling across `[eas, head]` instead of `--slots N`.
    ///
    /// Large peer windows are strided, never fully materialised (max 10000).
    #[arg(long, default_value_t = false)]
    full_window: bool,

    /// Negative-side sample count from `[eas − 32000, eas)` (default 100, max 10000).
    #[arg(long, default_value_t = 100, value_parser = parse_sample_count)]
    below: usize,

    /// Column indices to request on the column protocols (comma-separated, max 128).
    /// Omit to probe blocks only.
    #[arg(long, value_delimiter = ',')]
    columns: Vec<u64>,

    /// Write machine-readable JSON report to this path (trusted; atomic write).
    #[arg(long)]
    json: Option<PathBuf>,
}

fn parse_sample_count(s: &str) -> Result<usize, String> {
    let n: usize = s
        .parse()
        .map_err(|e| format!("invalid sample count: {e}"))?;
    if n > MAX_SAMPLE_COUNT {
        return Err(format!(
            "sample count {n} exceeds hard ceiling {MAX_SAMPLE_COUNT} (SEC-4B-3)"
        ));
    }
    Ok(n)
}

fn parse_fork_digest(s: &str) -> Result<ForkDigest, String> {
    let hex = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    let bytes = hex::decode(hex).map_err(|e| format!("fork-digest hex: {e}"))?;
    if bytes.len() != 4 {
        return Err(format!(
            "fork-digest must be exactly 4 bytes (got {})",
            bytes.len()
        ));
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes);
    Ok(ForkDigest::from_array(arr))
}

#[tokio::main]
async fn main() -> ExitCode {
    // Keep the probe quiet unless RUST_LOG is set — stderr is for human summary.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    // Touch cc-config so the allowed workspace edge is a real link (DAG / tree).
    let _cfg_marker = std::any::type_name::<cc_config::ServiceConfig>();

    let cli = Cli::parse();
    let fork_digest = match parse_fork_digest(&cli.fork_digest) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    if cli.columns.len() > MAX_COLUMNS {
        eprintln!(
            "error: --columns list length {} exceeds {MAX_COLUMNS}",
            cli.columns.len()
        );
        return ExitCode::from(2);
    }

    let cfg = ProbeConfig {
        peer: cli.peer.clone(),
        fork_digest,
        slots: cli.slots.min(MAX_SAMPLE_COUNT),
        full_window: cli.full_window,
        below: cli.below.min(MAX_SAMPLE_COUNT),
        columns: cli.columns,
    };

    let outcome = match run_probe(&cfg).await {
        Ok(o) => o,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(1);
        }
    };

    let fd_hex = format!("0x{}", hex::encode(fork_digest.as_slice()));
    let report = ProbeReport::from_outcome(&cli.peer, &outcome.agent_version, &fd_hex, &outcome);

    if let Some(path) = &cli.json {
        if let Err(e) = report.write_json(path) {
            eprintln!("error: writing json {}: {e}", path.display());
            return ExitCode::from(1);
        }
        eprintln!("json written to {}", path.display());
    }

    println!(
        "positive: pass={} failing_slots={:?}",
        report.positive.pass, report.positive.failing_slots
    );
    println!(
        "negative: pass={} failing_slots={:?}",
        report.negative.pass, report.negative.failing_slots
    );

    if !report.positive.pass {
        for (slot, reason) in &report.positive.failing_reasons {
            eprintln!("positive fail slot {slot}: {reason}");
        }
    }
    if !report.negative.pass {
        for (slot, reason) in &report.negative.failing_reasons {
            eprintln!("negative fail slot {slot}: {reason}");
        }
    }

    if report.pass {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}
