//! Machine-readable probe output (CC-4B /4).
//!
//! **SEC-4B-5:** JSON is written via temp file + `rename` (best-effort atomic
//! replace on the same filesystem). `--json` paths remain operator-trusted.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::probe::{ProbeOutcome, SideResult};

/// Top-level JSON report written by `--json <path>`.
#[derive(Debug, Clone, Serialize)]
pub struct ProbeReport {
    /// Target multiaddr.
    pub peer: String,
    /// Identify `agent_version` when observed, else empty.
    pub agent_version: String,
    /// CLI fork digest (hex).
    pub fork_digest: String,
    /// Peer's advertised earliest available slot.
    pub earliest_available_slot: u64,
    /// Peer's advertised head slot.
    pub head_slot: u64,
    /// Positive-side result.
    pub positive: SideReport,
    /// Negative-side result.
    pub negative: SideReport,
    /// Overall pass.
    pub pass: bool,
}

/// Per-side pass/fail, failing slots, and latency histogram.
#[derive(Debug, Clone, Serialize)]
pub struct SideReport {
    /// Whether every sampled slot on this side passed.
    pub pass: bool,
    /// Exact failing slot numbers.
    pub failing_slots: Vec<u64>,
    /// Human-readable failure reasons keyed by slot.
    pub failing_reasons: BTreeMap<String, String>,
    /// Per-slot latency in milliseconds.
    pub latencies_ms: BTreeMap<String, u64>,
    /// Coarse latency histogram buckets (ms upper bound → count).
    pub latency_histogram: BTreeMap<String, u64>,
}

impl SideReport {
    /// Build from a [`SideResult`].
    #[must_use]
    pub fn from_side(side: &SideResult) -> Self {
        let mut failing_reasons = BTreeMap::new();
        for (slot, reason) in &side.failures {
            failing_reasons.insert(slot.to_string(), reason.clone());
        }
        let mut latencies_ms = BTreeMap::new();
        let mut histogram: BTreeMap<String, u64> = BTreeMap::new();
        for (slot, ms) in &side.latencies_ms {
            latencies_ms.insert(slot.to_string(), *ms);
            let bucket = latency_bucket(*ms);
            *histogram.entry(bucket).or_insert(0) += 1;
        }
        Self {
            pass: side.pass,
            failing_slots: side.failing_slots(),
            failing_reasons,
            latencies_ms,
            latency_histogram: histogram,
        }
    }
}

fn latency_bucket(ms: u64) -> String {
    // Fixed upper-bound buckets for a greppable histogram.
    let upper = if ms <= 5 {
        5
    } else if ms <= 10 {
        10
    } else if ms <= 25 {
        25
    } else if ms <= 50 {
        50
    } else if ms <= 100 {
        100
    } else if ms <= 250 {
        250
    } else if ms <= 500 {
        500
    } else if ms <= 1000 {
        1000
    } else if ms <= 5000 {
        5000
    } else {
        10000
    };
    format!("le_{upper}")
}

impl ProbeReport {
    /// Build from a completed probe outcome.
    #[must_use]
    pub fn from_outcome(
        peer: &str,
        agent_version: &str,
        fork_digest_hex: &str,
        outcome: &ProbeOutcome,
    ) -> Self {
        Self {
            peer: peer.to_owned(),
            agent_version: agent_version.to_owned(),
            fork_digest: fork_digest_hex.to_owned(),
            earliest_available_slot: outcome.earliest_available_slot,
            head_slot: outcome.head_slot,
            positive: SideReport::from_side(&outcome.positive),
            negative: SideReport::from_side(&outcome.negative),
            pass: outcome.pass(),
        }
    }

    /// Write pretty JSON to `path` via temp file + rename (SEC-4B-5).
    ///
    /// Parent directory must already exist. Path is operator-trusted; this
    /// only avoids torn writes / partial clobber of the final name.
    pub fn write_json(&self, path: &Path) -> io::Result<()> {
        let body = serde_json::to_vec_pretty(self)
            .map_err(|e| io::Error::other(format!("json encode: {e}")))?;

        let tmp = temp_path_for(path);
        // Remove a stale tmp from a previous crash so create_new can succeed.
        let _ = fs::remove_file(&tmp);

        {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            f.write_all(&body)?;
            f.sync_all()?;
        }

        match fs::rename(&tmp, path) {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                Err(e)
            }
        }
    }
}

fn temp_path_for(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_owned();
    os.push(".tmp");
    PathBuf::from(os)
}
