//! Latency histogram against `cc_store::buckets::COMMIT_SECONDS` (§10.2 / CC-4Ca).

use std::time::Duration;

use cc_store::buckets::COMMIT_SECONDS;

/// Histogram of commit latencies with the production bucket ladder.
#[derive(Debug, Clone)]
pub(crate) struct LatencyHist {
    /// All write-behind commit samples (seconds).
    samples: Vec<f64>,
    /// Commits that included a prune delete.
    prune_samples: Vec<f64>,
    /// Counts per COMMIT_SECONDS bucket upper bound (plus +Inf).
    bucket_counts: Vec<u64>,
}

impl LatencyHist {
    pub(crate) fn new() -> Self {
        Self {
            samples: Vec::new(),
            prune_samples: Vec::new(),
            bucket_counts: vec![0; COMMIT_SECONDS.len() + 1],
        }
    }

    pub(crate) fn record(&mut self, d: Duration) {
        let s = d.as_secs_f64();
        self.samples.push(s);
        self.count_bucket(s);
    }

    pub(crate) fn record_prune(&mut self, d: Duration) {
        let s = d.as_secs_f64();
        self.prune_samples.push(s);
        self.samples.push(s);
        self.count_bucket(s);
    }

    fn count_bucket(&mut self, s: f64) {
        for (i, &bound) in COMMIT_SECONDS.iter().enumerate() {
            if s <= bound {
                self.bucket_counts[i] += 1;
                return;
            }
        }
        if let Some(c) = self.bucket_counts.last_mut() {
            *c += 1;
        }
    }

    /// p99 of prune-pass commit latencies (seconds). Falls back to all samples.
    pub(crate) fn p99_prune_secs(&self) -> f64 {
        percentile(&self.prune_samples)
            .or_else(|| percentile(&self.samples))
            .unwrap_or(0.0)
    }

    pub(crate) fn p99_all_secs(&self) -> f64 {
        percentile(&self.samples).unwrap_or(0.0)
    }

    pub(crate) fn prune_count(&self) -> usize {
        self.prune_samples.len()
    }

    pub(crate) fn all_count(&self) -> usize {
        self.samples.len()
    }

    /// How many samples fell in the bucket with upper bound 0.5 s (exact falsifier boundary).
    pub(crate) fn count_at_or_below_half_second(&self) -> u64 {
        // buckets: indices for bounds ≤ 0.5 inclusive
        let mut n = 0u64;
        for (i, &bound) in COMMIT_SECONDS.iter().enumerate() {
            if bound <= 0.5 {
                n += self.bucket_counts[i];
            }
        }
        n
    }

    pub(crate) fn count_above_half_second(&self) -> u64 {
        let mut n = 0u64;
        for (i, &bound) in COMMIT_SECONDS.iter().enumerate() {
            if bound > 0.5 {
                n += self.bucket_counts[i];
            }
        }
        n + self.bucket_counts.last().copied().unwrap_or(0)
    }

    pub(crate) fn format_buckets(&self) -> String {
        let mut parts = Vec::new();
        for (i, &bound) in COMMIT_SECONDS.iter().enumerate() {
            parts.push(format!("≤{bound}s:{}", self.bucket_counts[i]));
        }
        parts.push(format!(
            "+Inf:{}",
            self.bucket_counts.last().copied().unwrap_or(0)
        ));
        parts.join(" ")
    }
}

impl Default for LatencyHist {
    fn default() -> Self {
        Self::new()
    }
}

fn percentile(samples: &[f64]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut v = samples.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((v.len() as f64) * 0.99).ceil() as usize;
    let idx = idx.saturating_sub(1).min(v.len() - 1);
    Some(v[idx])
}

/// Falsifier verdict against Architecture §8.2 bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Pass,
    FailP99,
    FailSpace,
    FailBoth,
}

impl Verdict {
    pub(crate) fn evaluate(p99_secs: f64, file_over_live: f64) -> Self {
        let p99_bad = p99_secs > 0.5;
        let space_bad = file_over_live > 2.0;
        match (p99_bad, space_bad) {
            (false, false) => Self::Pass,
            (true, false) => Self::FailP99,
            (false, true) => Self::FailSpace,
            (true, true) => Self::FailBoth,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::FailP99 => "FAIL (p99 > 500 ms)",
            Self::FailSpace => "FAIL (file > 2.0× live set)",
            Self::FailBoth => "FAIL (p99 and space)",
        }
    }

    pub(crate) fn is_pass(self) -> bool {
        matches!(self, Self::Pass)
    }
}
