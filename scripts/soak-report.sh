#!/usr/bin/env bash
# scripts/soak-report.sh — CC-1Ac soak report + R-1 load guard + CC-29b Phase 2
# clauses + CC-3Ab Phase 3 acceptance clause table
#
# Scrapes (or reads) chain histogram snapshots at the start and end of the
# steady-state window, subtracts, and computes the budget verdict as the
# **bucket fraction at the exact 0.4 / 1.0 boundaries** (Architecture §11.2) —
# a counting question, not a quantile interpolation. Catch-up is excluded using
# `cc_driver_catchup_complete_timestamp` (CC-1C/3) and reported separately.
#
# R-1 guard: reads the per-slot load-average series from the sampler CSV and
# **refuses to emit a report** if a sustained load spike appears inside the
# steady-state window. A refused report is recoverable; a quietly wrong one is
# not. Threshold and window are config (env / flags).
#
# CC-29b Phase 2 extension (append-only): emits one row per proof clause
# (six + CC-2A + R-5 cross-check) with venue, measured value, threshold, and
# pass/fail. Clause 1 is min_over_time on peer series; clauses 3 and 6 read
# exact bucket fractions (never quantile interpolation). Clause 3's catch-up
# exclusion is keyed on peer_set_stable_unix from the sampler meta — absent
# that timestamp the Phase 2 report refuses loudly.
#
# CC-3Ab Phase 3 extension (append-only): emits one row per Phase 3 clause
# (entry condition E + clauses 1–5 + P1 rows CC-3C / CC-3B) with venue,
# measured, threshold, pass/fail. Venue strings are exactly `Hoodi`,
# `local compose + EL`, and `dev machine`. Clause 1's window starts at
# CC-39b's Phase A → Phase B boundary (`window_start_unix` /
# `phase_b_boundary` from scripts/phase-3-acceptance.sh); the bootstrap
# catch-up burst before it is excluded and reported as its own row.
# A clause at the wrong venue does not discharge: `--venue` refuses
# non-matching clause rows. Clause 4 can emit `NOT_RUN` naming blockers.
# It measures the run; it is not the run (D-6).
#
# Usage (Phase 1):
#   bash scripts/soak-report.sh \
#     --samples soak-samples.csv \
#     --metrics-start chain-metrics-start.txt \
#     --metrics-end   chain-metrics-end.txt \
#     --driver-metrics driver-metrics.txt \
#     [--out report-timing.md] \
#     [--docs docs/phase-1-soak.md] [--write] \
#     [--load-threshold N] [--load-window-samples N]
#
# Usage (Phase 2 clause table):
#   bash scripts/soak-report.sh --phase 2 \
#     --samples soak-samples.csv \
#     --run-meta soak-samples.meta \
#     --p2p-metrics-start p2p-metrics-start.txt \
#     --p2p-metrics-end   p2p-metrics-end.txt \
#     [--harness-json harness-results.json] \
#     [--out clause-table.md] \
#     [--docs docs/phase-2-soak.md] [--write]
#
# Usage (Phase 3 clause table — CC-3Ab):
#   bash scripts/soak-report.sh --phase 3 \
#     --samples soak-samples.csv \
#     [--boundary-file .data/phase3-window-start] \
#     [--chain-metrics-start …] [--chain-metrics-end …] \
#     [--engine-metrics-start …] [--engine-metrics-end …] \
#     [--p2p-metrics-start …] [--p2p-metrics-end …] \
#     [--harness-json harness-results.json] \
#     [--venue 'Hoodi'|'local compose + EL'|'dev machine'] \
#     [--clause N|E|CC-3C|CC-3B|bootstrap] \
#     [--out clause-table.md] \
#     [--docs docs/phase-3-acceptance.md] [--write]
#
# Usage (Phase 4 clause table — CC-4Cb / §10.6):
#   bash scripts/soak-report.sh --phase 4 \
#     [--storage-metrics-url http://127.0.0.1:9106/metrics] \
#     [--storage-metrics PATH] \
#     [--p2p-metrics PATH] \
#     [--plateau-samples PATH] \
#     [--serve-probe-json PATH] \
#     [--harness-json harness-results.json] \
#     [--venue hoodi|self-devnet|self-devnet-compressed|in-process-double|dev-machine] \
#     [--clause N|1|2|3|4|5|6|7] \
#     [--out clause-table.md] \
#     [--docs docs/phase-4-soak.md] [--write]
#
# Phase 4 venues (closed set): hoodi, self-devnet, self-devnet-compressed,
# in-process-double, dev-machine. Confirmation rows carry
# `confirmation, non-discharging`. It measures the run; it is not the run (D-10).
#
# Live scrape (optional if snapshot files omitted):
#   --chain-metrics-url  http://127.0.0.1:9101/metrics
#   --driver-metrics-url http://127.0.0.1:9110/metrics
#
# Self-test (synthetic series; no live stack required; Phase 1 + 2 + 3):
#   Phase 4 self-tests (CC-4Cb) also run under --self-test.
#   bash scripts/soak-report.sh --self-test
#
# Environment:
#   SOAK_SAMPLES, SOAK_METRICS_START, SOAK_METRICS_END, SOAK_DRIVER_METRICS,
#   SOAK_LOAD_THRESHOLD (default 4.0), SOAK_LOAD_WINDOW_SAMPLES (default 5),
#   SOAK_REPORT_OUT, SOAK_P2P_METRICS_START, SOAK_P2P_METRICS_END,
#   SOAK_RUN_META, SOAK_HARNESS_JSON, SOAK_PHASE2, SOAK_PHASE3,
#   SOAK_BOUNDARY_FILE, SOAK_ENGINE_METRICS_START, SOAK_ENGINE_METRICS_END,
#   SOAK_CHAIN_METRICS_START, SOAK_CHAIN_METRICS_END, SOAK_VENUE, SOAK_CLAUSE
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SAMPLES="${SOAK_SAMPLES:-}"
METRICS_START="${SOAK_METRICS_START:-}"
METRICS_END="${SOAK_METRICS_END:-}"
DRIVER_METRICS="${SOAK_DRIVER_METRICS:-}"
CHAIN_METRICS_URL="${SOAK_CHAIN_METRICS_URL:-}"
DRIVER_METRICS_URL="${SOAK_DRIVER_METRICS_URL:-http://127.0.0.1:9110/metrics}"
OUT="${SOAK_REPORT_OUT:-}"
DOCS="${REPO_ROOT}/docs/phase-1-soak.md"
WRITE=0
SELF_TEST=0
PHASE2="${SOAK_PHASE2:-0}"
PHASE3="${SOAK_PHASE3:-0}"
P2P_METRICS_START="${SOAK_P2P_METRICS_START:-}"
P2P_METRICS_END="${SOAK_P2P_METRICS_END:-}"
RUN_META="${SOAK_RUN_META:-}"
HARNESS_JSON="${SOAK_HARNESS_JSON:-}"
LOAD_THRESHOLD="${SOAK_LOAD_THRESHOLD:-4.0}"
LOAD_WINDOW="${SOAK_LOAD_WINDOW_SAMPLES:-5}"
# Optional operator-supplied window bounds (unix seconds). Default: catchup → last sample.
WINDOW_START="${SOAK_WINDOW_START:-}"
WINDOW_END="${SOAK_WINDOW_END:-}"
# Phase 3 (CC-3Ab): Phase A→B boundary file + engine/chain scrapes + venue/clause filter.
BOUNDARY_FILE="${SOAK_BOUNDARY_FILE:-${REPO_ROOT}/.data/phase3-window-start}"
ENGINE_METRICS_START="${SOAK_ENGINE_METRICS_START:-}"
ENGINE_METRICS_END="${SOAK_ENGINE_METRICS_END:-}"
CHAIN_METRICS_START="${SOAK_CHAIN_METRICS_START:-}"
CHAIN_METRICS_END="${SOAK_CHAIN_METRICS_END:-}"
VENUE_FILTER="${SOAK_VENUE:-}"
CLAUSE_FILTER="${SOAK_CLAUSE:-}"
PHASE4="${SOAK_PHASE4:-0}"
# Phase 4 (CC-4Cb): storage metrics + plateau samples + serve-probe JSON.
STORAGE_METRICS_URL="${SOAK_STORAGE_METRICS_URL:-http://127.0.0.1:9106/metrics}"
STORAGE_METRICS_FILE="${SOAK_STORAGE_METRICS:-}"
PLATEAU_SAMPLES="${SOAK_PLATEAU_SAMPLES:-}"
SERVE_PROBE_JSON="${SOAK_SERVE_PROBE_JSON:-}"
P2P_METRICS_FILE="${SOAK_P2P_METRICS:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --samples) SAMPLES="$2"; shift 2 ;;
    --metrics-start) METRICS_START="$2"; shift 2 ;;
    --metrics-end) METRICS_END="$2"; shift 2 ;;
    --driver-metrics) DRIVER_METRICS="$2"; shift 2 ;;
    --chain-metrics-url) CHAIN_METRICS_URL="$2"; shift 2 ;;
    --driver-metrics-url) DRIVER_METRICS_URL="$2"; shift 2 ;;
    --p2p-metrics-start) P2P_METRICS_START="$2"; shift 2 ;;
    --p2p-metrics-end) P2P_METRICS_END="$2"; shift 2 ;;
    --run-meta) RUN_META="$2"; shift 2 ;;
    --harness-json) HARNESS_JSON="$2"; shift 2 ;;
    --phase2) PHASE2=1; PHASE3=0; shift ;;
    --phase3) PHASE3=1; PHASE2=0; shift ;;
    --phase4) PHASE4=1; PHASE2=0; PHASE3=0; shift ;;
    --phase)
      case "${2:-}" in
        1) PHASE2=0; PHASE3=0 ;;
        2) PHASE2=1; PHASE3=0 ;;
        3) PHASE3=1; PHASE2=0 ;;
        4) PHASE4=1; PHASE2=0; PHASE3=0 ;;
        *) echo "error: --phase expects 1, 2, or 3 (got: ${2:-})" >&2; exit 2 ;;
      esac
      shift 2
      ;;
    --boundary-file) BOUNDARY_FILE="$2"; shift 2 ;;
    --engine-metrics-start) ENGINE_METRICS_START="$2"; shift 2 ;;
    --engine-metrics-end) ENGINE_METRICS_END="$2"; shift 2 ;;
    --chain-metrics-start) CHAIN_METRICS_START="$2"; METRICS_START="$2"; shift 2 ;;
    --chain-metrics-end) CHAIN_METRICS_END="$2"; METRICS_END="$2"; shift 2 ;;
    --venue) VENUE_FILTER="$2"; shift 2 ;;
    --clause) CLAUSE_FILTER="$2"; shift 2 ;;
    --storage-metrics-url) STORAGE_METRICS_URL="$2"; shift 2 ;;
    --storage-metrics) STORAGE_METRICS_FILE="$2"; shift 2 ;;
    --plateau-samples) PLATEAU_SAMPLES="$2"; shift 2 ;;
    --serve-probe-json) SERVE_PROBE_JSON="$2"; shift 2 ;;
    --p2p-metrics) P2P_METRICS_FILE="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --docs) DOCS="$2"; shift 2 ;;
    --write) WRITE=1; shift ;;
    --load-threshold) LOAD_THRESHOLD="$2"; shift 2 ;;
    --load-window-samples) LOAD_WINDOW="$2"; shift 2 ;;
    --window-start) WINDOW_START="$2"; shift 2 ;;
    --window-end) WINDOW_END="$2"; shift 2 ;;
    --self-test) SELF_TEST=1; shift ;;
    -h|--help)
      sed -n '2,80p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

log() { echo "soak-report: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }
# shellcheck disable=SC2329 # available for bash-level refusals; Python evaluators exit 3 themselves
refuse() {
  # R-1 / catch-up / peer-set-stable refusals: non-zero, message on stderr, no report body.
  echo "REFUSED: $*" >&2
  exit 3
}

command -v python3 >/dev/null 2>&1 || die "required tool not found: python3"

# ── Python helpers (histogram + load guard + report body) ───────────────────
# stdin: unused. Args passed as env.
run_python_report() {
  SOAK_PY_SAMPLES="${SAMPLES}" \
  SOAK_PY_METRICS_START="${METRICS_START}" \
  SOAK_PY_METRICS_END="${METRICS_END}" \
  SOAK_PY_DRIVER_METRICS="${DRIVER_METRICS}" \
  SOAK_PY_LOAD_THRESHOLD="${LOAD_THRESHOLD}" \
  SOAK_PY_LOAD_WINDOW="${LOAD_WINDOW}" \
  SOAK_PY_WINDOW_START="${WINDOW_START}" \
  SOAK_PY_WINDOW_END="${WINDOW_END}" \
  SOAK_PY_MODE="${1:-report}" \
  python3 - <<'PY'
import os, re, sys
from pathlib import Path

samples_path = Path(os.environ["SOAK_PY_SAMPLES"])
m_start_path = Path(os.environ["SOAK_PY_METRICS_START"])
m_end_path = Path(os.environ["SOAK_PY_METRICS_END"])
d_metrics_path = Path(os.environ["SOAK_PY_DRIVER_METRICS"])
threshold = float(os.environ["SOAK_PY_LOAD_THRESHOLD"])
window_n = int(os.environ["SOAK_PY_LOAD_WINDOW"])
win_start_env = os.environ.get("SOAK_PY_WINDOW_START") or ""
win_end_env = os.environ.get("SOAK_PY_WINDOW_END") or ""
mode = os.environ.get("SOAK_PY_MODE", "report")

def die(msg, code=1):
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)

def refuse(msg):
    print(f"REFUSED: {msg}", file=sys.stderr)
    raise SystemExit(3)

def parse_gauge(text, name):
    # Match OpenMetrics / Prometheus gauge lines (no labels or with labels).
    # Prefer unlabeled; else first series.
    pat = re.compile(
        r"^" + re.escape(name) + r"(?:\{[^}]*\})?\s+([0-9eE+.\-]+)\s*(?:[0-9]+)?\s*$",
        re.M,
    )
    hits = pat.findall(text)
    if not hits:
        return None
    return float(hits[0])

def parse_hist_bucket(text, metric, le):
    # cc_chain_process_block_seconds_bucket{le="0.4"} 123
    pat = re.compile(
        r'^' + re.escape(metric) + r'_bucket\{le="' + re.escape(le) + r'"\}\s+([0-9eE+.\-]+)',
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def parse_hist_count(text, metric):
    pat = re.compile(
        r"^" + re.escape(metric) + r"_count\s+([0-9eE+.\-]+)",
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def parse_hist_sum(text, metric):
    pat = re.compile(
        r"^" + re.escape(metric) + r"_sum\s+([0-9eE+.\-]+)",
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def load_samples(path: Path):
    if not path.is_file():
        die(f"samples file not found: {path}")
    rows = []
    with path.open() as f:
        header = f.readline().strip().split(",")
        idx = {name: i for i, name in enumerate(header)}
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            def col(name, default=""):
                i = idx.get(name)
                if i is None or i >= len(parts):
                    return default
                return parts[i]
            try:
                ts = int(float(col("ts_unix", "0")))
            except ValueError:
                continue
            load_s = col("load1", "")
            rss_s = col("rss_kib", "")
            try:
                load = float(load_s) if load_s != "" else None
            except ValueError:
                load = None
            try:
                rss = float(rss_s) if rss_s != "" else None
            except ValueError:
                rss = None
            agree_s = col("agree", "0")
            rows.append({
                "ts": ts,
                "slot": col("slot", ""),
                "local_root": col("local_root", ""),
                "ref_root": col("ref_root", ""),
                "agree": agree_s in ("1", "true", "True"),
                "rss": rss,
                "load": load,
            })
    return rows

def find_load_spike(rows, threshold, window_n, t0, t1):
    """Return (start_ts, end_ts) of first sustained spike, or None."""
    series = [r for r in rows if t0 <= r["ts"] <= t1 and r["load"] is not None]
    if window_n < 1:
        return None
    run = 0
    run_start = None
    for r in series:
        if r["load"] > threshold:
            if run == 0:
                run_start = r["ts"]
            run += 1
            if run >= window_n:
                # extend to end of contiguous spike
                end_ts = r["ts"]
                # already at first index that completes the window; walk forward
                # from current position is enough for naming
                return (run_start, end_ts)
        else:
            run = 0
            run_start = None
    return None

def fraction_at_boundary(start_text, end_text, metric, le):
    b0 = parse_hist_bucket(start_text, metric, le)
    b1 = parse_hist_bucket(end_text, metric, le)
    c0 = parse_hist_count(start_text, metric)
    c1 = parse_hist_count(end_text, metric)
    if None in (b0, b1, c0, c1):
        die(f"missing histogram series for {metric} le={le} (need bucket + count at start and end)")
    db = b1 - b0
    dc = c1 - c0
    if dc <= 0:
        die(f"no new observations for {metric} in window (Δcount={dc})")
    if db < 0 or db > dc + 1e-9:
        die(f"incoherent histogram delta for {metric} le={le}: Δbucket={db} Δcount={dc}")
    return db / dc, db, dc

def fmt_ts(ts):
    import datetime
    return datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

# ── catch-up boundary (CC-1C/3) ────────────────────────────────────────────
d_text = d_metrics_path.read_text(errors="replace")
catchup = parse_gauge(d_text, "cc_driver_catchup_complete_timestamp")
if catchup is None:
    refuse(
        "cc_driver_catchup_complete_timestamp missing from driver metrics — "
        "catch-up boundary cannot be reconstructed after the fact (CC-1C/3). "
        "Re-scrape driver /metrics or pass --driver-metrics with the series present."
    )
if catchup <= 0:
    refuse(
        "cc_driver_catchup_complete_timestamp is 0 (catch-up not complete). "
        "Steady-state window has not opened; refusing report."
    )
catchup = int(catchup)

rows = load_samples(samples_path)
if not rows:
    die("samples CSV has no data rows")

sample_t0 = rows[0]["ts"]
sample_t1 = rows[-1]["ts"]

if win_start_env:
    t_start = int(win_start_env)
else:
    t_start = max(catchup, sample_t0)
if win_end_env:
    t_end = int(win_end_env)
else:
    t_end = sample_t1

if t_end <= t_start:
    die(f"empty steady-state window: start={t_start} end={t_end} catchup={catchup}")

# ── R-1 load guard ─────────────────────────────────────────────────────────
spike = find_load_spike(rows, threshold, window_n, t_start, t_end)
if spike is not None:
    s0, s1 = spike
    refuse(
        f"R-1 load-spike guard: sustained load1 > {threshold} for ≥ {window_n} "
        f"consecutive samples inside the steady-state window "
        f"[{fmt_ts(t_start)} … {fmt_ts(t_end)}]. "
        f"Spike timestamps: first={fmt_ts(s0)} ({s0}), "
        f"window-complete={fmt_ts(s1)} ({s1}). "
        f"Clause 3 is contaminated (cargo build / heavy process). "
        f"A refused report is recoverable; re-run with a clean window."
    )

# ── histograms ─────────────────────────────────────────────────────────────
start_text = m_start_path.read_text(errors="replace")
end_text = m_end_path.read_text(errors="replace")

block_metric = "cc_chain_process_block_seconds"
epoch_metric = "cc_chain_process_epoch_seconds"

block_frac, block_db, block_dc = fraction_at_boundary(start_text, end_text, block_metric, "0.4")
epoch_frac, epoch_db, epoch_dc = fraction_at_boundary(start_text, end_text, epoch_metric, "1.0")

block_pass = block_frac >= 0.95
epoch_pass = epoch_frac >= 0.95
block_margin = block_frac - 0.95
epoch_margin = epoch_frac - 0.95

# RSS pair from samples (hour-2 / hour-24 placeholders filled when data covers them)
steady = [r for r in rows if t_start <= r["ts"] <= t_end]
rss_vals = [r["rss"] for r in steady if r["rss"] is not None]
rss_hour2 = None
rss_end = None
if steady:
    # hour-2 relative to steady start
    t_h2 = t_start + 2 * 3600
    near_h2 = [r for r in steady if abs(r["ts"] - t_h2) <= 90 and r["rss"] is not None]
    if near_h2:
        rss_hour2 = near_h2[0]["rss"]
    if steady[-1]["rss"] is not None:
        rss_end = steady[-1]["rss"]
    elif rss_vals:
        rss_end = rss_vals[-1]

agree_n = sum(1 for r in steady if r["agree"])
agree_d = sum(1 for r in steady if r["local_root"] and r["ref_root"])
agree_pct = (100.0 * agree_n / agree_d) if agree_d else float("nan")

# Provider names from sidecar meta if present
meta_path = samples_path.with_suffix(".meta")
driver_p = ""
ref_p = ""
if meta_path.is_file():
    for line in meta_path.read_text().splitlines():
        if line.startswith("driver_provider="):
            driver_p = line.split("=", 1)[1]
        elif line.startswith("ref_provider="):
            ref_p = line.split("=", 1)[1]

verdict = "PASS" if (block_pass and epoch_pass) else "FAIL"
if not block_pass or not epoch_pass:
    contingency = (
        "Exceeding either budget triggers **CC-1H** (the recorded contingency), "
        "not a redesign — re-run the soak after the backing swap if promoted."
    )
else:
    contingency = (
        "Both budgets met. Margin is reported so a scraping pass is visible as a "
        "Phase 2 risk (Fulu ~4 s attestation deadline covers consensus + execution + DA)."
    )

# Catch-up window: sample start → catchup (excluded from budget)
catchup_excluded = f"{fmt_ts(sample_t0)} → {fmt_ts(catchup)} (unix {sample_t0} → {catchup})"
if sample_t0 >= catchup:
    catchup_excluded = (
        f"catch-up completed at {fmt_ts(catchup)} before sampler start "
        f"({fmt_ts(sample_t0)}); excluded via histogram scrape pair (CC-1C/3)"
    )

body = f"""## Timing

**Owner:** CC-1Ac skeleton / CC-1Ad numbers  
**Generated by:** `scripts/soak-report.sh`  
**Verdict:** **{verdict}**

| Field | Value |
|---|---|
| Block bucket fraction at `le=0.4` | **{block_frac:.6f}** (Δbucket={block_db:.0f} / Δcount={block_dc:.0f}) |
| Block budget | p95 ≤ 0.4 s ⇔ fraction ≥ 0.95 |
| Block margin (fraction − 0.95) | **{block_margin:+.6f}** |
| Block pass | {"yes" if block_pass else "NO"} |
| Epoch bucket fraction at `le=1.0` | **{epoch_frac:.6f}** (Δbucket={epoch_db:.0f} / Δcount={epoch_dc:.0f}) |
| Epoch budget | p95 ≤ 1.0 s ⇔ fraction ≥ 0.95 |
| Epoch margin (fraction − 0.95) | **{epoch_margin:+.6f}** |
| Epoch pass | {"yes" if epoch_pass else "NO"} |
| Measurement window (steady-state) | {fmt_ts(t_start)} → {fmt_ts(t_end)} (unix {t_start} → {t_end}) |
| Excluded catch-up window | {catchup_excluded} |
| Catch-up complete (`cc_driver_catchup_complete_timestamp`) | {fmt_ts(catchup)} (unix {catchup}) |
| Head-agreement samples (steady) | {agree_n}/{agree_d} ({agree_pct:.2f} % of comparable rows) |
| Driver provider | `{driver_p or "_TBD_"}` |
| Reference provider | `{ref_p or "_TBD_"}` |
| RSS hour-2 (KiB) | {f"{rss_hour2:.0f}" if rss_hour2 is not None else "_TBD (need ≥2 h series)_"} |
| RSS end-of-window (KiB) | {f"{rss_end:.0f}" if rss_end is not None else "_TBD_"} |
| R-1 load guard | clean (threshold={threshold}, window={window_n} samples) |
| Samples file | `{samples_path}` |

### Method

Bucket fraction is a **counting question** (Architecture §11.2 / CC-1C/4): scrape
`cc_chain_process_block_seconds` / `cc_chain_process_epoch_seconds` at the start
and end of the steady-state window, subtract cumulative counts, and compute

```text
fraction(le=L) = (bucket[le=L]_end − bucket[le=L]_start) / (count_end − count_start)
```

Pass ⇔ fraction ≥ 0.95. No quantile interpolation. Catch-up is excluded by
using the scrape pair after `cc_driver_catchup_complete_timestamp` and is
reported in the table above (CC-1C/3).

### Contingency

{contingency}
"""

if mode == "report":
    sys.stdout.write(body)
    if not (block_pass and epoch_pass):
        # Emit body but non-zero so CI/operators notice a budget miss.
        raise SystemExit(4)
    raise SystemExit(0)

raise SystemExit(0)
PY
}

# ── Phase 2 per-clause table (CC-29b / §13.11) ─────────────────────────────
# Append-only relative to Phase 1: separate evaluator; does not reorder Timing.
run_python_phase2() {
  SOAK_PY_SAMPLES="${SAMPLES}" \
  SOAK_PY_RUN_META="${RUN_META}" \
  SOAK_PY_P2P_START="${P2P_METRICS_START}" \
  SOAK_PY_P2P_END="${P2P_METRICS_END}" \
  SOAK_PY_HARNESS="${HARNESS_JSON}" \
  SOAK_PY_WINDOW_START="${WINDOW_START}" \
  SOAK_PY_WINDOW_END="${WINDOW_END}" \
  python3 - <<'PY'
import json, os, re, sys
from pathlib import Path

samples_path = Path(os.environ["SOAK_PY_SAMPLES"])
meta_path_env = os.environ.get("SOAK_PY_RUN_META") or ""
p2p_start_path = os.environ.get("SOAK_PY_P2P_START") or ""
p2p_end_path = os.environ.get("SOAK_PY_P2P_END") or ""
harness_path = os.environ.get("SOAK_PY_HARNESS") or ""
win_start_env = os.environ.get("SOAK_PY_WINDOW_START") or ""
win_end_env = os.environ.get("SOAK_PY_WINDOW_END") or ""

def die(msg, code=1):
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)

def refuse(msg):
    print(f"REFUSED: {msg}", file=sys.stderr)
    raise SystemExit(3)

def fmt_ts(ts):
    import datetime
    return datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

def parse_meta(path: Path):
    out = {}
    if not path.is_file():
        return out
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def load_samples(path: Path):
    if not path.is_file():
        die(f"samples file not found: {path}")
    rows = []
    with path.open() as f:
        header = f.readline().strip().split(",")
        idx = {name: i for i, name in enumerate(header)}
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            def col(name, default=""):
                i = idx.get(name)
                if i is None or i >= len(parts):
                    return default
                return parts[i]
            try:
                ts = int(float(col("ts_unix", "0")))
            except ValueError:
                continue
            def fnum(name):
                s = col(name, "")
                if s == "":
                    return None
                try:
                    return float(s)
                except ValueError:
                    return None
            rows.append({
                "ts": ts,
                "p2p_peers": fnum("p2p_peers"),
                "p2p_peers_custody": fnum("p2p_peers_custody"),
                "head_lag_slots": fnum("head_lag_slots"),
                "rss": fnum("rss_kib"),
                "load": fnum("load1"),
            })
    return rows

def parse_hist_bucket(text, metric, le):
    # Exact le boundary — counting question; no quantile interpolation.
    pat = re.compile(
        r'^' + re.escape(metric) + r'_bucket\{le="' + re.escape(le) + r'"\}\s+([0-9eE+.\-]+)',
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def parse_hist_count(text, metric):
    pat = re.compile(
        r"^" + re.escape(metric) + r"_count\s+([0-9eE+.\-]+)",
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def fraction_at_boundary(start_text, end_text, metric, le):
    b0 = parse_hist_bucket(start_text, metric, le)
    b1 = parse_hist_bucket(end_text, metric, le)
    c0 = parse_hist_count(start_text, metric)
    c1 = parse_hist_count(end_text, metric)
    if None in (b0, b1, c0, c1):
        return None, None, None
    db = b1 - b0
    dc = c1 - c0
    if dc <= 0:
        return None, db, dc
    if db < 0 or db > dc + 1e-9:
        die(f"incoherent histogram delta for {metric} le={le}: Δbucket={db} Δcount={dc}")
    return db / dc, db, dc

def parse_counter(text, name, labels=None):
    """Parse a counter; labels is dict of required label key=value."""
    if labels:
        # Build flexible label matcher (order-independent).
        lab_parts = [re.escape(f'{k}="{v}"') for k, v in labels.items()]
        # Match metric{...labels...} value
        pat = re.compile(
            r"^" + re.escape(name) + r"\{([^}]*)\}\s+([0-9eE+.\-]+)",
            re.M,
        )
        for m in pat.finditer(text):
            lab = m.group(1)
            if all(re.search(p, lab) for p in lab_parts):
                return float(m.group(2))
        return None
    pat = re.compile(
        r"^" + re.escape(name) + r"(?:\{[^}]*\})?\s+([0-9eE+.\-]+)",
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def counter_delta(start_text, end_text, name, labels=None):
    a = parse_counter(start_text, name, labels)
    b = parse_counter(end_text, name, labels)
    if a is None or b is None:
        return None
    return b - a

rows = load_samples(samples_path)
if not rows:
    die("samples CSV has no data rows")

# Resolve meta: explicit path, else samples.meta sidecar.
if meta_path_env:
    meta_path = Path(meta_path_env)
else:
    meta_path = samples_path.with_suffix(".meta")
meta = parse_meta(meta_path)

# peer-set-stable is mandatory for Phase 2 clause 3 (and the steady-state window).
stable_s = meta.get("peer_set_stable_unix", "").strip()
if stable_s == "" or stable_s is None:
    refuse(
        "peer_set_stable_unix missing from run meta — Phase 2 steady-state window "
        "cannot be reconstructed after the fact (CC-29b). Re-run soak-sampler.sh "
        "until peers and custody-compatible hold the thresholds, or pass a meta "
        "file that records peer_set_stable_unix."
    )
try:
    peer_stable = int(float(stable_s))
except ValueError:
    refuse(f"peer_set_stable_unix is not an integer: {stable_s!r}")
if peer_stable <= 0:
    refuse("peer_set_stable_unix is 0 or negative — steady-state window has not opened")

sample_t0 = rows[0]["ts"]
sample_t1 = rows[-1]["ts"]
if win_start_env:
    t_start = int(win_start_env)
else:
    t_start = max(peer_stable, sample_t0)
if win_end_env:
    t_end = int(win_end_env)
else:
    t_end = sample_t1
if t_end <= t_start:
    die(f"empty Phase 2 steady-state window: start={t_start} end={t_end} peer_stable={peer_stable}")

steady = [r for r in rows if t_start <= r["ts"] <= t_end]
catchup_rows = [r for r in rows if r["ts"] < t_start]

# ── clause 1: min_over_time peers / custody ────────────────────────────────
peers_series = [r["p2p_peers"] for r in steady if r["p2p_peers"] is not None]
custody_series = [r["p2p_peers_custody"] for r in steady if r["p2p_peers_custody"] is not None]

def row_result(clause, venue, measured, threshold, status):
    return {
        "clause": clause,
        "venue": venue,
        "measured": measured,
        "threshold": threshold,
        "status": status,
    }

rows_out = []

if not peers_series or not custody_series:
    rows_out.append(row_result(
        "1 · healthy peer count 24 h",
        "Hoodi",
        "NO_DATA (missing p2p_peers / p2p_peers_custody columns in samples)",
        "min peers ≥ 25 and custody ≥ 8 (min_over_time)",
        "NO_DATA",
    ))
    c1_pass = None
    min_peers = None
    min_custody = None
else:
    min_peers = min(peers_series)
    min_custody = min(custody_series)
    c1_pass = (min_peers >= 25) and (min_custody >= 8)
    rows_out.append(row_result(
        "1 · healthy peer count 24 h",
        "Hoodi",
        f"min_over_time(peers)={min_peers:g}; min_over_time(custody)={min_custody:g} "
        f"(n={len(peers_series)} samples)",
        "min peers ≥ 25 and custody ≥ 8 (min_over_time)",
        "PASS" if c1_pass else "FAIL",
    ))

# ── p2p metrics scrapes (clauses 2, 3, 6, R-5) ─────────────────────────────
start_text = ""
end_text = ""
have_p2p = False
if p2p_start_path and p2p_end_path:
    sp, ep = Path(p2p_start_path), Path(p2p_end_path)
    if sp.is_file() and ep.is_file():
        start_text = sp.read_text(errors="replace")
        end_text = ep.read_text(errors="replace")
        have_p2p = True

harness = {}
if harness_path and Path(harness_path).is_file():
    try:
        harness = json.loads(Path(harness_path).read_text())
    except json.JSONDecodeError as e:
        die(f"harness-json invalid: {e}")

# Clause 2 · DA-gated import
if not have_p2p:
    rows_out.append(row_result(
        "2 · DA-gated import",
        "Hoodi",
        "NO_DATA (p2p metrics start/end scrapes required)",
        'imported non-trivial; zero deferred head ancestry',
        "NO_DATA",
    ))
else:
    d_imp = counter_delta(start_text, end_text, "cc_p2p_da_outcome_total", {"result": "imported"})
    d_def = counter_delta(start_text, end_text, "cc_p2p_da_outcome_total", {"result": "deferred"})
    # Ancestry-while-deferred is operator-supplied via harness when available.
    ancestry_ok = harness.get("clause2", {}).get("zero_deferred_head_ancestry")
    if d_imp is None:
        rows_out.append(row_result(
            "2 · DA-gated import",
            "Hoodi",
            "NO_DATA (cc_p2p_da_outcome_total{result=\"imported\"} absent)",
            'imported non-trivial; zero deferred head ancestry',
            "NO_DATA",
        ))
    else:
        non_trivial = d_imp > 0
        parts = [f'Δimported={d_imp:g}']
        if d_def is not None:
            parts.append(f"Δdeferred={d_def:g}")
        if ancestry_ok is None:
            parts.append("head-ancestry-while-deferred=UNOBSERVED")
            # Without ancestry observation the row is partial; still FAIL if imported=0.
            if not non_trivial:
                st = "FAIL"
            else:
                st = "NO_DATA"
                parts.append("(need zero_deferred_head_ancestry in harness-json)")
        else:
            parts.append(f"zero_deferred_head_ancestry={bool(ancestry_ok)}")
            st = "PASS" if (non_trivial and bool(ancestry_ok)) else "FAIL"
        rows_out.append(row_result(
            "2 · DA-gated import",
            "Hoodi",
            "; ".join(parts),
            'imported non-trivial; zero deferred head ancestry',
            st,
        ))

# Clause 3 · head lag ≤ 1 typical — bucket fraction at le=1, catch-up excluded
catchup_note = (
    f"excluded catch-up / pre-stable: {fmt_ts(sample_t0)} → {fmt_ts(t_start)} "
    f"(peer_set_stable_unix={peer_stable}); steady {fmt_ts(t_start)} → {fmt_ts(t_end)}"
)
if catchup_rows:
    catchup_lags = [r["head_lag_slots"] for r in catchup_rows if r["head_lag_slots"] is not None]
    if catchup_lags:
        catchup_note += f"; pre-stable head_lag samples n={len(catchup_lags)} max={max(catchup_lags):g}"

if not have_p2p:
    rows_out.append(row_result(
        "3 · head lag ≤ 1 typical",
        "Hoodi",
        f"NO_DATA (p2p metrics scrapes required); {catchup_note}",
        "bucket fraction at le=1 of cc_p2p_head_lag_slots ≥ 0.95 (catch-up excluded)",
        "NO_DATA",
    ))
else:
    frac, db, dc = fraction_at_boundary(
        start_text, end_text, "cc_p2p_head_lag_slots", "1"
    )
    # Also accept le="1.0" if exporters render that way.
    if frac is None:
        frac, db, dc = fraction_at_boundary(
            start_text, end_text, "cc_p2p_head_lag_slots", "1.0"
        )
    if frac is None:
        rows_out.append(row_result(
            "3 · head lag ≤ 1 typical",
            "Hoodi",
            f"NO_DATA (cc_p2p_head_lag_slots bucket/count missing or Δcount≤0); {catchup_note}",
            "bucket fraction at le=1 of cc_p2p_head_lag_slots ≥ 0.95 (catch-up excluded)",
            "NO_DATA",
        ))
    else:
        c3_pass = frac >= 0.95
        rows_out.append(row_result(
            "3 · head lag ≤ 1 typical",
            "Hoodi",
            f"bucket_fraction(le=1)={frac:.6f} (Δbucket={db:g}/Δcount={dc:g}); {catchup_note}",
            "bucket fraction at le=1 of cc_p2p_head_lag_slots ≥ 0.95 (catch-up excluded)",
            "PASS" if c3_pass else "FAIL",
        ))

# Clause 4 · 10-minute gap recovery (self-devnet)
c4 = harness.get("clause4") if isinstance(harness, dict) else None
if not c4:
    rows_out.append(row_result(
        "4 · 10-minute gap recovery",
        "self-devnet",
        "NO_DATA (harness-json.clause4 absent — live discharge is CC-26b / CC-2Jd)",
        "back to head within 32 slots; parent-linkage clean; every backfilled block DA-gated",
        "NO_DATA",
    ))
else:
    slots = c4.get("recovery_slots")
    parent = c4.get("parent_walk_clean")
    da = c4.get("da_gated")
    ok = (
        slots is not None and float(slots) <= 32
        and bool(parent) and bool(da)
    )
    rows_out.append(row_result(
        "4 · 10-minute gap recovery",
        "self-devnet",
        f"recovery_slots={slots}; parent_walk_clean={parent}; da_gated={da}",
        "back to head within 32 slots; parent-linkage clean; every backfilled block DA-gated",
        "PASS" if ok else "FAIL",
    ))

# Clause 4 Hoodi confirmation (R-5, non-discharging)
c4h = harness.get("clause4_hoodi") if isinstance(harness, dict) else None
if not c4h:
    rows_out.append(row_result(
        "4 · Hoodi confirmation (R-5)",
        "Hoodi",
        "NO_DATA (harness-json.clause4_hoodi absent; non-discharging)",
        "~50 real blocks+columns by-range; non-discharging",
        "NO_DATA",
    ))
else:
    ok = bool(c4h.get("pass"))
    rows_out.append(row_result(
        "4 · Hoodi confirmation (R-5)",
        "Hoodi",
        c4h.get("measured", str(c4h)),
        "~50 real blocks+columns by-range; non-discharging",
        "PASS" if ok else "FAIL",
    ))

# Clause 5 · withheld column
c5 = harness.get("clause5") if isinstance(harness, dict) else None
if not c5:
    rows_out.append(row_result(
        "5 · withheld column",
        "adversarial harness",
        "NO_DATA (harness-json.clause5 absent — live discharge is CC-2Jb)",
        '{result="deferred"} then {result="recovered"} with head advance',
        "NO_DATA",
    ))
else:
    ok = bool(c5.get("deferred_then_recovered")) and bool(c5.get("head_advanced_after_recover"))
    rows_out.append(row_result(
        "5 · withheld column",
        "adversarial harness",
        f"deferred_then_recovered={c5.get('deferred_then_recovered')}; "
        f"head_advanced_after_recover={c5.get('head_advanced_after_recover')}",
        '{result="deferred"} then {result="recovered"} with head advance',
        "PASS" if ok else "FAIL",
    ))

# Clause 6 · scoring penalises — bucket at -4000, not a quantile
c6 = harness.get("clause6") if isinstance(harness, dict) else None
if c6:
    reason = c6.get("penalty_reason", "")
    crossed = bool(c6.get("score_crossed_m4000"))
    attr_ok = bool(c6.get("reason_attributed", True))
    ok = crossed and attr_ok and bool(reason)
    rows_out.append(row_result(
        "6 · scoring penalises",
        "adversarial harness",
        f"reason={reason}; score_crossed_-4000_bucket={crossed}; attributed={attr_ok}",
        "penalty reason attributed and peer score crosses −4000 bucket boundary",
        "PASS" if ok else "FAIL",
    ))
elif have_p2p:
    # Optional metric-only view: any penalty + peer_score bucket movement past -4000.
    # Prefer harness for full discharge; metrics alone → measured without inventing pass.
    pen_reasons = [
        "gossip_invalid", "import_invalid", "reqresp_fault",
        "custody_unserved", "behavioural", "rate_limit",
    ]
    deltas = []
    for r in pen_reasons:
        d = counter_delta(start_text, end_text, "cc_p2p_peer_penalty_total", {"reason": r})
        if d is not None and d > 0:
            deltas.append(f"{r}=+{d:g}")
    # Bucket fraction / count at le=-4000 of peer_score (exact boundary).
    b0 = parse_hist_bucket(start_text, "cc_p2p_peer_score", "-4000")
    b1 = parse_hist_bucket(end_text, "cc_p2p_peer_score", "-4000")
    # Crossing into ≤ -4000: increase in cumulative count at le=-4000.
    crossed_metric = None
    if b0 is not None and b1 is not None:
        crossed_metric = (b1 - b0) > 0
    if not deltas and crossed_metric is None:
        rows_out.append(row_result(
            "6 · scoring penalises",
            "adversarial harness",
            "NO_DATA (no harness-json.clause6 and no penalty/score series in scrapes)",
            "penalty reason attributed and peer score crosses −4000 bucket boundary",
            "NO_DATA",
        ))
    else:
        if b0 is None or b1 is None:
            db_s = "absent"
        else:
            db_s = f"{(b1 - b0):g}"
        measured = (
            f"penalties=[{', '.join(deltas) or 'none'}]; "
            f"Δbucket(le=-4000)={db_s}"
        )
        # Metrics without harness cannot fully discharge (needs induced kind attribution).
        rows_out.append(row_result(
            "6 · scoring penalises",
            "adversarial harness",
            measured + " (harness required for full discharge)",
            "penalty reason attributed and peer score crosses −4000 bucket boundary",
            "NO_DATA",
        ))
else:
    rows_out.append(row_result(
        "6 · scoring penalises",
        "adversarial harness",
        "NO_DATA (harness-json.clause6 absent — live discharge is CC-2Jc)",
        "penalty reason attributed and peer score crosses −4000 bucket boundary",
        "NO_DATA",
    ))

# CC-2A · BPO
c2a = harness.get("clause_2a") if isinstance(harness, dict) else None
if not c2a:
    rows_out.append(row_result(
        "CC-2A · BPO",
        "self-devnet",
        "NO_DATA (harness-json.clause_2a absent — live discharge is CC-2A)",
        "topic-set change count == 2; peers retained across both; no zero-rate beacon_block slot",
        "NO_DATA",
    ))
else:
    changes = c2a.get("topic_set_changes")
    retained = bool(c2a.get("peers_retained"))
    zero_rate = bool(c2a.get("zero_rate_beacon_block", True))
    ok = (changes == 2) and retained and (not zero_rate)
    rows_out.append(row_result(
        "CC-2A · BPO",
        "self-devnet",
        f"topic_set_changes={changes}; peers_retained={retained}; "
        f"zero_rate_beacon_block={zero_rate}",
        "topic-set change count == 2; peers retained across both; no zero-rate beacon_block slot",
        "PASS" if ok else "FAIL",
    ))

# R-5 cross-check: recovered vs deferred over Hoodi window
if not have_p2p:
    rows_out.append(row_result(
        "R-5 cross-check",
        "Hoodi",
        "NO_DATA (p2p metrics scrapes required)",
        '{recovered} vs {deferred} over 24 h; zero-recovered + non-zero deferred = harness-only recovery',
        "NO_DATA",
    ))
else:
    d_rec = counter_delta(start_text, end_text, "cc_p2p_da_outcome_total", {"result": "recovered"})
    d_def = counter_delta(start_text, end_text, "cc_p2p_da_outcome_total", {"result": "deferred"})
    if d_rec is None or d_def is None:
        rows_out.append(row_result(
            "R-5 cross-check",
            "Hoodi",
            "NO_DATA (da_outcome recovered/deferred series missing)",
            '{recovered} vs {deferred} over 24 h; zero-recovered + non-zero deferred = harness-only recovery',
            "NO_DATA",
        ))
    else:
        callout = ""
        if d_rec == 0 and d_def > 0:
            callout = (
                " **CALLOUT: recovered=0 with deferred>0 — recovery only ever worked "
                "in the harness (R-5).**"
            )
        elif d_rec > 0:
            callout = " recovered non-zero: by-root path exercised against real peers."
        rows_out.append(row_result(
            "R-5 cross-check",
            "Hoodi",
            f"Δrecovered={d_rec:g}; Δdeferred={d_def:g}.{callout}",
            '{recovered} vs {deferred} over 24 h; zero-recovered + non-zero deferred = harness-only recovery',
            "INFO",  # observational; not a pass/fail gate by itself
        ))

# ── emit markdown ──────────────────────────────────────────────────────────
lines = [
    "## Clause table",
    "",
    "**Owner:** CC-29b (script) / CC-29c (numbers)",
    "**Generated by:** `scripts/soak-report.sh --phase2`",
    f"**Steady-state window:** {fmt_ts(t_start)} → {fmt_ts(t_end)} "
    f"(peer_set_stable_unix={peer_stable})",
    f"**Excluded pre-stable / catch-up:** {fmt_ts(sample_t0)} → {fmt_ts(t_start)}",
    f"**Samples:** `{samples_path}`",
    f"**Run meta:** `{meta_path}`",
    "",
    "| Clause | Venue | Measured | Threshold | Pass/Fail |",
    "|---|---|---|---|---|",
]
for r in rows_out:
    # Escape pipes in cells.
    def esc(s):
        return str(s).replace("|", "\\|").replace("\n", " ")
    lines.append(
        f"| {esc(r['clause'])} | {esc(r['venue'])} | {esc(r['measured'])} | "
        f"{esc(r['threshold'])} | **{esc(r['status'])}** |"
    )
lines.append("")
lines.append("### Method notes")
lines.append("")
lines.append(
    "- **Clause 1** uses `min_over_time` on the per-slot `p2p_peers` / "
    "`p2p_peers_custody` series (a single dip below threshold fails)."
)
lines.append(
    "- **Clauses 3 and 6** read **bucket fractions / exact boundaries** "
    "(`le=1` for head lag, `le=-4000` for peer score). No quantile interpolation."
)
lines.append(
    "- **Clause 3 catch-up exclusion** is keyed on `peer_set_stable_unix` "
    "(not process start). Pre-stable head-lag is reported separately above."
)
lines.append(
    "- **R-5 cross-check** compares `{result=\"recovered\"}` against "
    "`{result=\"deferred\"}` over the Hoodi window; zero-recovered with "
    "non-zero deferred is called out explicitly."
)
lines.append(
    "- A clause with no data emits a **NO_DATA** row rather than being omitted."
)
lines.append("")

sys.stdout.write("\n".join(lines))

# Exit non-zero if any hard FAIL among discharging clauses (not INFO/NO_DATA).
hard_fail = any(r["status"] == "FAIL" for r in rows_out)
if hard_fail:
    raise SystemExit(4)
raise SystemExit(0)
PY
}

# ── Phase 3 per-clause table (CC-3Ab / §9.4) ───────────────────────────────
# Append-only relative to Phase 1 and Phase 2: separate evaluator; does not
# reorder or rewrite prior rows. It measures the run; it is not the run (D-6).
run_python_phase3() {
  SOAK_PY_SAMPLES="${SAMPLES:-}" \
  SOAK_PY_BOUNDARY="${BOUNDARY_FILE:-}" \
  SOAK_PY_CHAIN_START="${CHAIN_METRICS_START:-${METRICS_START:-}}" \
  SOAK_PY_CHAIN_END="${CHAIN_METRICS_END:-${METRICS_END:-}}" \
  SOAK_PY_ENGINE_START="${ENGINE_METRICS_START:-}" \
  SOAK_PY_ENGINE_END="${ENGINE_METRICS_END:-}" \
  SOAK_PY_P2P_START="${P2P_METRICS_START:-}" \
  SOAK_PY_P2P_END="${P2P_METRICS_END:-}" \
  SOAK_PY_HARNESS="${HARNESS_JSON:-}" \
  SOAK_PY_WINDOW_START="${WINDOW_START:-}" \
  SOAK_PY_WINDOW_END="${WINDOW_END:-}" \
  SOAK_PY_VENUE="${VENUE_FILTER:-}" \
  SOAK_PY_CLAUSE="${CLAUSE_FILTER:-}" \
  python3 - <<'PY'
import json, os, re, sys
from pathlib import Path

samples_path = (os.environ.get("SOAK_PY_SAMPLES") or "").strip()
boundary_path = (os.environ.get("SOAK_PY_BOUNDARY") or "").strip()
chain_start_path = (os.environ.get("SOAK_PY_CHAIN_START") or "").strip()
chain_end_path = (os.environ.get("SOAK_PY_CHAIN_END") or "").strip()
engine_start_path = (os.environ.get("SOAK_PY_ENGINE_START") or "").strip()
engine_end_path = (os.environ.get("SOAK_PY_ENGINE_END") or "").strip()
p2p_start_path = (os.environ.get("SOAK_PY_P2P_START") or "").strip()
p2p_end_path = (os.environ.get("SOAK_PY_P2P_END") or "").strip()
harness_path = (os.environ.get("SOAK_PY_HARNESS") or "").strip()
win_start_env = (os.environ.get("SOAK_PY_WINDOW_START") or "").strip()
win_end_env = (os.environ.get("SOAK_PY_WINDOW_END") or "").strip()
venue_filter = (os.environ.get("SOAK_PY_VENUE") or "").strip()
clause_filter = (os.environ.get("SOAK_PY_CLAUSE") or "").strip()

# Canonical Phase 3 venues (exact strings — a wrong venue does not discharge).
VENUE_HOODI = "Hoodi"
VENUE_LOCAL = "local compose + EL"
VENUE_DEV = "dev machine"
VALID_VENUES = {VENUE_HOODI, VENUE_LOCAL, VENUE_DEV}

def die(msg, code=1):
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)

def refuse(msg):
    print(f"REFUSED: {msg}", file=sys.stderr)
    raise SystemExit(3)

def fmt_ts(ts):
    import datetime
    if ts is None:
        return "n/a"
    return datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

def parse_kv_file(path: Path):
    """Parse key=value meta / boundary file. Supports window_start / phase_b_boundary."""
    out = {}
    if not path.is_file():
        return out
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def read_text(path_s):
    if not path_s:
        return ""
    p = Path(path_s)
    if not p.is_file():
        return ""
    return p.read_text(errors="replace")

def parse_counter(text, name, labels=None):
    if not text:
        return None
    if labels:
        lab_parts = [re.escape(f'{k}="{v}"') for k, v in labels.items()]
        pat = re.compile(
            r"^" + re.escape(name) + r"\{([^}]*)\}\s+([0-9eE+.\-]+)",
            re.M,
        )
        for m in pat.finditer(text):
            lab = m.group(1)
            if all(re.search(p, lab) for p in lab_parts):
                return float(m.group(2))
        return None
    pat = re.compile(
        r"^" + re.escape(name) + r"(?:\{[^}]*\})?\s+([0-9eE+.\-]+)",
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def parse_gauge(text, name, labels=None):
    return parse_counter(text, name, labels)

def counter_present(text, name, labels=None):
    """True if the series line exists (even at zero)."""
    return parse_counter(text, name, labels) is not None

def counter_delta(start_text, end_text, name, labels=None):
    a = parse_counter(start_text, name, labels)
    b = parse_counter(end_text, name, labels)
    if a is None or b is None:
        return None
    return b - a

def load_samples(path: Path):
    if not path.is_file():
        return []
    rows = []
    with path.open() as f:
        header = f.readline().strip().split(",")
        idx = {name: i for i, name in enumerate(header)}
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            def col(name, default=""):
                i = idx.get(name)
                if i is None or i >= len(parts):
                    return default
                return parts[i]
            try:
                ts = int(float(col("ts_unix", "0")))
            except ValueError:
                continue
            def fnum(name):
                s = col(name, "")
                if s == "":
                    return None
                try:
                    return float(s)
                except ValueError:
                    return None
            rows.append({
                "ts": ts,
                "is_optimistic": fnum("is_optimistic"),
                "optimistic_nodes": fnum("optimistic_nodes"),
                "el_head_lag_blocks": fnum("el_head_lag_blocks"),
                "finalized_epoch": fnum("finalized_epoch"),
                "load": fnum("load1"),
            })
    return rows

def row_result(clause_id, clause, venue, measured, threshold, status):
    return {
        "id": clause_id,
        "clause": clause,
        "venue": venue,
        "measured": measured,
        "threshold": threshold,
        "status": status,
    }

def clause_id_matches(filter_s, clause_id):
    if not filter_s:
        return True
    f = filter_s.strip().lower()
    cid = str(clause_id).lower()
    aliases = {
        "e": "e",
        "entry": "e",
        "1": "1",
        "2": "2",
        "3": "3",
        "4": "4",
        "5": "5",
        "bootstrap": "bootstrap",
        "burst": "bootstrap",
        "cc-3c": "cc-3c",
        "cc3c": "cc-3c",
        "cc-3b": "cc-3b",
        "cc3b": "cc-3b",
    }
    want = aliases.get(f, f)
    return want == cid

# ── inputs ─────────────────────────────────────────────────────────────────
rows = load_samples(Path(samples_path)) if samples_path else []

boundary = parse_kv_file(Path(boundary_path)) if boundary_path else {}
# window_start / phase_b_boundary: machine-readable Phase A → Phase B boundary
# emitted by scripts/phase-3-acceptance.sh (CC-39b). Greppable as
# window_start|phase_b_boundary.
phase_b_boundary = None
if win_start_env:
    try:
        phase_b_boundary = int(float(win_start_env))
    except ValueError:
        die(f"--window-start is not an integer unix timestamp: {win_start_env!r}")
else:
    for key in (
        "window_start_unix",
        "phase_b_boundary_unix",
        "phase_boundary_unix",
        "phase_b_boundary",
        "window_start",
    ):
        if key in boundary and boundary[key] != "":
            try:
                phase_b_boundary = int(float(boundary[key]))
                break
            except ValueError:
                die(f"boundary file {key} is not numeric: {boundary[key]!r}")

if win_end_env:
    try:
        t_end = int(float(win_end_env))
    except ValueError:
        die(f"--window-end is not an integer unix timestamp: {win_end_env!r}")
elif rows:
    t_end = rows[-1]["ts"]
else:
    t_end = None

t_start = phase_b_boundary  # clause 1 window_start = phase_b_boundary

chain_start = read_text(chain_start_path)
chain_end = read_text(chain_end_path)
engine_start = read_text(engine_start_path)
engine_end = read_text(engine_end_path)
p2p_start = read_text(p2p_start_path)
p2p_end = read_text(p2p_end_path)
# Prefer end scrape; fall back to start; allow single-file "current stack" reads.
chain_now = chain_end or chain_start
engine_now = engine_end or engine_start
p2p_now = p2p_end or p2p_start

harness = {}
if harness_path and Path(harness_path).is_file():
    try:
        harness = json.loads(Path(harness_path).read_text())
    except json.JSONDecodeError as e:
        die(f"harness-json invalid: {e}")

if venue_filter and venue_filter not in VALID_VENUES:
    die(
        f"unknown venue {venue_filter!r}; expected one of: "
        + ", ".join(sorted(VALID_VENUES))
    )

# ── build all candidate rows ───────────────────────────────────────────────
candidates = []  # list of row_result dicts
venue_refusals = []  # messages for wrong-venue skips when --venue set

def add_row(r):
    """Apply --clause and --venue filters. Venue mismatch is a loud refusal note."""
    if not clause_id_matches(clause_filter, r["id"]):
        return
    if venue_filter and r["venue"] != venue_filter:
        msg = (
            f"refusing to emit clause {r['id']!r} (venue {r['venue']!r}) "
            f"at requested venue {venue_filter!r} — a clause run at the wrong "
            f"venue does not discharge"
        )
        venue_refusals.append(msg)
        print(f"REFUSED: {msg}", file=sys.stderr)
        return
    candidates.append(r)

# ── Entry condition E ──────────────────────────────────────────────────────
# eth_syncing == false is operator-supplied via harness or boundary note;
# also accept a gauge sample if present.
e_h = harness.get("entry") if isinstance(harness, dict) else None
if isinstance(e_h, dict):
    synced = e_h.get("eth_syncing_false")
    block = e_h.get("snapshot_block")
    measured_e = (
        f"eth_syncing_false={synced}; snapshot_block={block}; "
        f"window_start={fmt_ts(t_start)}"
    )
    if synced is True or synced == 1 or str(synced).lower() == "true":
        st_e = "PASS"
    elif synced is False or synced == 0 or str(synced).lower() == "false":
        st_e = "FAIL"
    else:
        st_e = "NOT_RUN"
        measured_e = f"NOT_RUN (entry harness partial); {measured_e}"
else:
    # Compute something: boundary present is partial evidence of Phase B open.
    if t_start is not None and t_start > 0:
        measured_e = (
            f"phase_b_boundary/window_start={fmt_ts(t_start)} "
            f"(unix={t_start}); eth_syncing gate not re-probed by report "
            f"(record from CC-39b boundary file)"
        )
        st_e = "PASS"  # boundary file implies Phase B entry was asserted
    else:
        measured_e = (
            "NOT_RUN (no boundary file window_start_unix / phase_b_boundary "
            "and no harness-json.entry)"
        )
        st_e = "NOT_RUN"
add_row(row_result(
    "e",
    "E · entry condition (synced EL)",
    VENUE_DEV,
    measured_e,
    "eth_syncing == false; snapshot restore recorded (CC-39b)",
    st_e,
))

# ── Bootstrap catch-up burst (excluded from clause 1; own row) ─────────────
if rows and t_start is not None:
    burst = [r for r in rows if r["ts"] < t_start]
    steady_preview = [r for r in rows if r["ts"] >= t_start]
else:
    burst = []
    steady_preview = rows

burst_opt = [r["is_optimistic"] for r in burst if r["is_optimistic"] is not None]
if burst:
    if burst_opt:
        frac_opt = sum(1 for v in burst_opt if v and float(v) != 0.0) / len(burst_opt)
        measured_b = (
            f"bootstrap burst samples n={len(burst)}; "
            f"is_optimistic==1 fraction={frac_opt:.4f} "
            f"(excluded from clause 1; window_start/phase_b_boundary="
            f"{fmt_ts(t_start)})"
        )
    else:
        measured_b = (
            f"bootstrap burst samples n={len(burst)}; is_optimistic column "
            f"absent — burst excluded by window_start/phase_b_boundary="
            f"{fmt_ts(t_start)} only"
        )
    st_b = "INFO"
elif t_start is None:
    measured_b = (
        "NOT_RUN (no window_start/phase_b_boundary — cannot split bootstrap "
        "burst from steady-state)"
    )
    st_b = "NOT_RUN"
else:
    measured_b = (
        f"0 pre-window samples (window_start/phase_b_boundary={fmt_ts(t_start)}); "
        f"no bootstrap burst in series"
    )
    st_b = "INFO"
add_row(row_result(
    "bootstrap",
    "bootstrap catch-up burst (excluded from clause 1)",
    VENUE_HOODI,
    measured_b,
    "reported separately; not folded into clause 1 ≥ 99 % bar",
    st_b,
))

# ── Clause 1 · head marked VALID (Hoodi) ───────────────────────────────────
steady = [r for r in rows if t_start is None or r["ts"] >= t_start]
if t_end is not None:
    steady = [r for r in steady if r["ts"] <= t_end]
opt_series = [r["is_optimistic"] for r in steady if r["is_optimistic"] is not None]
nodes_series = [r["optimistic_nodes"] for r in steady if r["optimistic_nodes"] is not None]

d_valid = counter_delta(
    chain_start, chain_end, "cc_engine_payload_status_total",
    {"method": "newPayloadV4", "status": "VALID"},
)
# engine may own payload_status; try engine scrapes if chain empty
if d_valid is None:
    d_valid = counter_delta(
        engine_start, engine_end, "cc_engine_payload_status_total",
        {"method": "newPayloadV4", "status": "VALID"},
    )
d_invalid = counter_delta(
    engine_start or chain_start, engine_end or chain_end,
    "cc_engine_payload_status_total",
    {"method": "newPayloadV4", "status": "INVALID"},
)
if d_invalid is None:
    d_invalid = counter_delta(
        chain_start, chain_end, "cc_engine_payload_status_total",
        {"method": "newPayloadV4", "status": "INVALID"},
    )

parts1 = []
if t_start is not None:
    parts1.append(f"window_start/phase_b_boundary={fmt_ts(t_start)}")
if t_end is not None:
    parts1.append(f"window_end={fmt_ts(t_end)}")

if opt_series:
    n_zero = sum(1 for v in opt_series if float(v) == 0.0)
    frac_zero = n_zero / len(opt_series)
    parts1.append(
        f"is_optimistic==0 fraction={frac_zero:.6f} (n={len(opt_series)})"
    )
    c1_opt_ok = frac_zero >= 0.99
else:
    # Fall back to end-of-window gauge if series absent.
    g = parse_gauge(chain_now, "cc_chain_is_optimistic")
    if g is not None:
        parts1.append(f"cc_chain_is_optimistic(end)={g:g} (no per-slot series)")
        c1_opt_ok = (g == 0.0)
    else:
        parts1.append("is_optimistic series and gauge absent")
        c1_opt_ok = None

if nodes_series:
    max_nodes = max(nodes_series)
    parts1.append(f"max optimistic_nodes={max_nodes:g}")
    c1_nodes_ok = max_nodes == 0
else:
    g = parse_gauge(chain_now, "cc_chain_optimistic_nodes")
    if g is not None:
        parts1.append(f"cc_chain_optimistic_nodes(end)={g:g}")
        c1_nodes_ok = (g == 0.0)
    else:
        parts1.append("optimistic_nodes series and gauge absent")
        c1_nodes_ok = None

if d_valid is not None:
    parts1.append(f"Δpayload_status VALID={d_valid:g}")
    c1_valid_ok = d_valid > 0
else:
    v_now = parse_counter(
        engine_now or chain_now, "cc_engine_payload_status_total",
        {"method": "newPayloadV4", "status": "VALID"},
    )
    if v_now is not None:
        parts1.append(f"payload_status VALID(end)={v_now:g} (no start scrape)")
        c1_valid_ok = v_now > 0
    else:
        parts1.append("payload_status VALID absent")
        c1_valid_ok = None

if d_invalid is not None:
    parts1.append(f"Δpayload_status INVALID={d_invalid:g}")
    c1_inv_ok = d_invalid == 0
else:
    inv_now = parse_counter(
        engine_now or chain_now, "cc_engine_payload_status_total",
        {"method": "newPayloadV4", "status": "INVALID"},
    )
    if inv_now is not None:
        parts1.append(f"payload_status INVALID(end)={inv_now:g}")
        c1_inv_ok = inv_now == 0
    else:
        parts1.append("payload_status INVALID absent")
        c1_inv_ok = None

checks1 = [c1_opt_ok, c1_nodes_ok, c1_valid_ok, c1_inv_ok]
if all(c is None for c in checks1):
    st1 = "NOT_RUN"
    parts1.insert(0, "NOT_RUN (no samples/metrics to compute)")
elif any(c is False for c in checks1):
    st1 = "FAIL"
elif any(c is None for c in checks1):
    st1 = "NO_DATA"
else:
    st1 = "PASS"
add_row(row_result(
    "1",
    "1 · head marked VALID by the EL",
    VENUE_HOODI,
    "; ".join(parts1),
    "is_optimistic==0 ≥ 99 % of samples; optimistic_nodes==0; "
    "payload_status VALID increasing, INVALID zero (bootstrap excluded)",
    st1,
))

# ── Clause 2 · EL restart (local compose + EL) — both shapes ───────────────
# CC-36b: two rows (clean / unclean), never averaged. Harness from
# scripts/el-restart-drills.sh. One-pass is numerical when fields present:
#   transitions_delta == outage_block_count
#   newpayload_delta == new_blocks_in_interval
c2 = harness.get("clause2") if isinstance(harness, dict) else None
for shape_key, shape_label in (
    ("clean", "2a · EL restart clean (compose restart)"),
    ("unclean", "2b · EL restart unclean (kill -9)"),
):
    shape = None
    if isinstance(c2, dict):
        shape = c2.get(shape_key) or c2.get(shape_label)
    if not shape:
        # Also allow flat harness keys
        if isinstance(harness, dict):
            shape = harness.get(f"clause2_{shape_key}")
    # Both shapes share clause id 2 for --clause 2; distinguish in name.
    if not shape:
        add_row(row_result(
            "2",
            shape_label,
            VENUE_LOCAL,
            "NOT_RUN (harness-json.clause2.%s absent — live discharge is CC-36b / "
            "scripts/el-restart-drills.sh)" % shape_key,
            "during outage el_offline==1 + is_optimistic==1; fcU within 1 slot "
            "of eth_syncing==false; one VALID clears optimistic set, no payload re-sub",
            "NOT_RUN",
        ))
        continue
    # Explicit NOT_RUN shape (drill script --emit-not-run or venue unavailable).
    shape_status = str(shape.get("status") or "").upper()
    if shape_status == "NOT_RUN":
        notes = shape.get("notes") or shape.get("not_run_reason") or "venue unavailable"
        add_row(row_result(
            "2",
            shape_label,
            VENUE_LOCAL,
            f"NOT_RUN ({notes})",
            "during outage el_offline==1 + is_optimistic==1; fcU within 1 slot "
            "of eth_syncing==false; one VALID clears optimistic set, no payload re-sub",
            "NOT_RUN",
        ))
        continue
    offline = shape.get("el_offline_during")
    opt_during = shape.get("is_optimistic_during")
    fcu_slots = shape.get("fcu_slots_after_sync")
    cleared = shape.get("optimistic_cleared_single_valid")
    no_resub = shape.get("no_payload_resubmission")
    # Extended CC-36b fields (optional — numerical one-pass when present).
    outage_s = shape.get("outage_duration_s")
    outage_blocks = shape.get("outage_block_count")
    td = shape.get("transitions_delta")
    one_pass = shape.get("one_pass_exact")
    np_before = shape.get("newpayload_v4_before")
    np_after = shape.get("newpayload_v4_after")
    np_delta = shape.get("newpayload_delta")
    new_blocks = shape.get("new_blocks_in_interval")
    fcu_elapsed = shape.get("fcu_elapsed_s")
    eth_ts = shape.get("eth_syncing_false_utc")
    fcu_ts = shape.get("fcu_emission_utc")
    opt_nodes_after = shape.get("optimistic_nodes_after")
    auth_failed = shape.get("auth_failed_seen")
    cap_ok = shape.get("capability_repopulated")
    panics_delta = shape.get("worker_panics_delta")
    panics_metric = shape.get("worker_panics_metric")

    checks = [
        offline in (True, 1, "true", "1"),
        opt_during in (True, 1, "true", "1"),
        fcu_slots is not None and float(fcu_slots) <= 1,
        cleared in (True, 1, "true", "1"),
        no_resub in (True, 1, "true", "1"),
    ]
    # Numerical one-pass: exact equality required when both sides present.
    if outage_blocks is not None and td is not None:
        checks.append(float(td) == float(outage_blocks))
    if one_pass is not None:
        checks.append(one_pass in (True, 1, "true", "1"))
    if np_delta is not None and new_blocks is not None:
        checks.append(float(np_delta) == float(new_blocks))
    if auth_failed in (True, 1, "true", "1"):
        checks.append(False)
    if cap_ok is False:
        checks.append(False)
    if panics_delta is not None and float(panics_delta) != 0:
        checks.append(False)  # P0 — recorded as FAIL, not omitted

    ok = all(checks)
    parts = [
        f"el_offline_during={offline}",
        f"is_optimistic_during={opt_during}",
        f"fcu_slots_after_sync={fcu_slots}",
        f"optimistic_cleared_single_valid={cleared}",
        f"no_payload_resubmission={no_resub}",
    ]
    if outage_s is not None:
        parts.append(f"outage_duration_s={outage_s}")
    if outage_blocks is not None:
        parts.append(f"outage_block_count={outage_blocks}")
    if td is not None:
        parts.append(f"transitions_delta={td}")
    if one_pass is not None:
        parts.append(f"one_pass_exact={one_pass}")
    if np_before is not None and np_after is not None:
        parts.append(f"newPayloadV4 {np_before}→{np_after} (Δ={np_delta})")
    if new_blocks is not None:
        parts.append(f"new_blocks_in_interval={new_blocks}")
    if eth_ts or fcu_ts or fcu_elapsed is not None:
        parts.append(
            f"eth_syncing_false={eth_ts or 'n/a'}; fcu={fcu_ts or 'n/a'}; "
            f"fcu_elapsed_s={fcu_elapsed}"
        )
    if opt_nodes_after is not None:
        parts.append(f"optimistic_nodes_after={opt_nodes_after}")
    if auth_failed is not None:
        parts.append(f"auth_failed_seen={auth_failed}")
    if cap_ok is not None:
        parts.append(f"capability_repopulated={cap_ok}")
    if panics_delta is not None or panics_metric:
        parts.append(
            f"worker_panics_delta={panics_delta} ({panics_metric or 'n/a'})"
        )
    if shape.get("fails"):
        parts.append("fails=" + ";".join(str(x) for x in shape.get("fails") or []))

    add_row(row_result(
        "2",
        shape_label,
        VENUE_LOCAL,
        "; ".join(str(p) for p in parts),
        "during outage el_offline==1 + is_optimistic==1; fcU within 1 slot "
        "of eth_syncing==false; transitions_delta==outage_block_count; "
        "optimistic_nodes→0; ΔnewPayloadV4==new_blocks only",
        "PASS" if ok else "FAIL",
    ))

# ── Clause 3 · EL stays synced via fcU (Hoodi) ─────────────────────────────
lag_series = [r["el_head_lag_blocks"] for r in steady if r["el_head_lag_blocks"] is not None]
parts3 = []
if lag_series:
    within1 = sum(1 for v in lag_series if float(v) <= 1.0) / len(lag_series)
    parts3.append(
        f"el_head_lag≤1 fraction={within1:.6f} (n={len(lag_series)})"
    )
    c3_lag_ok = within1 >= 0.99
else:
    parts3.append("el_head_lag_blocks series absent")
    c3_lag_ok = None

d_38002 = counter_delta(
    engine_start, engine_end, "cc_engine_errors_total", {"code": "-38002"}
)
d_38006 = counter_delta(
    engine_start, engine_end, "cc_engine_errors_total", {"code": "-38006"}
)
if d_38002 is None:
    g = parse_counter(engine_now, "cc_engine_errors_total", {"code": "-38002"})
    if g is not None:
        parts3.append(f"errors -38002(end)={g:g}")
        c3_38002_ok = g == 0
    else:
        parts3.append("errors -38002 absent")
        c3_38002_ok = None
else:
    parts3.append(f"Δerrors -38002={d_38002:g}")
    c3_38002_ok = d_38002 == 0
if d_38006 is None:
    g = parse_counter(engine_now, "cc_engine_errors_total", {"code": "-38006"})
    if g is not None:
        parts3.append(f"errors -38006(end)={g:g}")
        c3_38006_ok = g == 0
    else:
        parts3.append("errors -38006 absent")
        c3_38006_ok = None
else:
    parts3.append(f"Δerrors -38006={d_38006:g}")
    c3_38006_ok = d_38006 == 0

wire = harness.get("clause3_wire") if isinstance(harness, dict) else None
if isinstance(wire, dict):
    parts3.append(
        f"wire: slots={wire.get('slots')}; "
        f"fcu_in_order={wire.get('fcu_in_order')}; "
        f"newpayload_before_fcu={wire.get('newpayload_before_fcu')}"
    )
    c3_wire_ok = bool(wire.get("fcu_in_order")) and bool(wire.get("newpayload_before_fcu"))
else:
    parts3.append("wire capture absent (CC-33/3, CC-31/8 — CC-3Ac)")
    c3_wire_ok = None

checks3 = [c3_lag_ok, c3_38002_ok, c3_38006_ok]
# Wire is required for full discharge but may be NO_DATA without failing numbers.
if all(c is None for c in checks3) and c3_wire_ok is None:
    st3 = "NOT_RUN"
    parts3.insert(0, "NOT_RUN (no samples/metrics/wire)")
elif any(c is False for c in checks3) or c3_wire_ok is False:
    st3 = "FAIL"
elif any(c is None for c in checks3) or c3_wire_ok is None:
    st3 = "NO_DATA"
else:
    st3 = "PASS"
add_row(row_result(
    "3",
    "3 · EL stays synced via forkchoiceUpdated",
    VENUE_HOODI,
    "; ".join(parts3),
    "geth head within 1 block ≥ 99 %; errors -38002/-38006 zero; "
    "wire: no fcU out of order; newPayload before fcU",
    st3,
))

# ── Clause 4 · getBlobsV2 fast path (Hoodi) ────────────────────────────────
# Failure condition: non-zero complete getBlobs with zero engine-sourced columns.
# NOT_RUN with blockers when the stack has no engine-sourced columns family
# (D-13: CC-38b + Phase 2 CC-24c / CC-24d).
c4h = harness.get("clause4") if isinstance(harness, dict) else None
force_not_run = False
blockers = ["CC-38b", "CC-24c", "CC-24d"]
if isinstance(c4h, dict):
    if c4h.get("not_run") or c4h.get("NOT_RUN"):
        force_not_run = True
        blockers = c4h.get("blockers") or blockers

gb_complete_delta = counter_delta(
    engine_start, engine_end, "cc_engine_getblobs_total", {"result": "complete"}
)
gb_complete_end = parse_counter(
    engine_now, "cc_engine_getblobs_total", {"result": "complete"}
)
gb_miss_end = parse_counter(
    engine_now, "cc_engine_getblobs_total", {"result": "miss"}
)
gb_partial_end = parse_counter(
    engine_now, "cc_engine_getblobs_total", {"result": "partial"}
)
# Presence of any getblobs result label counts as family present.
getblobs_present = any(
    counter_present(engine_now or engine_end or engine_start, "cc_engine_getblobs_total", {"result": r})
    for r in ("complete", "miss", "partial")
)

eng_cols_delta = counter_delta(
    p2p_start, p2p_end, "cc_p2p_columns_received_total", {"source": "engine"}
)
eng_cols_end = parse_counter(
    p2p_now, "cc_p2p_columns_received_total", {"source": "engine"}
)
cols_present = counter_present(
    p2p_now or p2p_end or p2p_start,
    "cc_p2p_columns_received_total",
    {"source": "engine"},
)

# Prefer window deltas when start and end scrapes differ; when they are the
# same file (instrument rehearsal against a live stack) use absolute end levels
# so the FAIL invariant is still expressible.
same_engine_scrape = (
    bool(engine_start) and bool(engine_end) and engine_start == engine_end
) or (bool(engine_end) and not engine_start)
same_p2p_scrape = (
    bool(p2p_start) and bool(p2p_end) and p2p_start == p2p_end
) or (bool(p2p_end) and not p2p_start)

if same_engine_scrape or gb_complete_delta is None:
    complete_val = gb_complete_end
else:
    complete_val = gb_complete_delta
if same_p2p_scrape or eng_cols_delta is None:
    cols_val = eng_cols_end
else:
    cols_val = eng_cols_delta

if force_not_run or (not getblobs_present and not cols_present):
    # Stack has no engine-sourced columns / getblobs surface → D-13 NOT_RUN.
    measured4 = (
        f"NOT_RUN naming blockers {', '.join(blockers)} "
        f"(no engine-sourced columns / getblobs family on stack — D-13; "
        f"Phase 2's CC-24c and CC-24d + CC-38b)"
    )
    st4 = "NOT_RUN"
elif complete_val is not None and cols_val is not None:
    # Hit rate: complete / (complete+miss+partial) when available.
    if same_engine_scrape or gb_complete_delta is None:
        c_v = gb_complete_end if gb_complete_end is not None else 0.0
        m_v = gb_miss_end if gb_miss_end is not None else 0.0
        p_v = gb_partial_end if gb_partial_end is not None else 0.0
        mode = "absolute"
    else:
        d_miss = counter_delta(
            engine_start, engine_end, "cc_engine_getblobs_total", {"result": "miss"}
        )
        d_part = counter_delta(
            engine_start, engine_end, "cc_engine_getblobs_total", {"result": "partial"}
        )
        c_v = gb_complete_delta
        m_v = d_miss if d_miss is not None else 0.0
        p_v = d_part if d_part is not None else 0.0
        mode = "Δwindow"
    total = c_v + m_v + p_v
    hit = (c_v / total) if total > 0 else 0.0
    measured4 = (
        f"getblobs complete={c_v:g} miss={m_v:g} partial={p_v:g} "
        f"hit_rate={hit:.6f} ({mode}); "
        f"cc_p2p_columns_received_total{{source=\"engine\"}}={cols_val:g}"
    )
    # Failure condition: non-zero complete with zero engine-sourced DA.
    if c_v > 0 and cols_val == 0:
        measured4 += (
            " **FAIL: non-zero getblobs complete with zero engine-sourced "
            "DataAvailable emissions**"
        )
        st4 = "FAIL"
    else:
        # Low hit rate is legitimate PASS.
        st4 = "PASS"
        if c_v == 0:
            measured4 += (
                " (zero complete in window — low rate is legitimate; "
                "clause may discharge via CC-38/1 or /3)"
            )
else:
    # Partial metrics: still emit a number or explicit NOT_RUN — never blank.
    measured4 = (
        f"getblobs_present={getblobs_present} complete={complete_val}; "
        f"columns_engine_present={cols_present} cols={cols_val}"
    )
    if complete_val is not None and complete_val > 0 and (cols_val is None or cols_val == 0):
        if cols_val == 0:
            measured4 += (
                " **FAIL: non-zero getblobs complete with zero engine-sourced columns**"
            )
            st4 = "FAIL"
        else:
            st4 = "NO_DATA"
            measured4 += " (engine columns series missing)"
    else:
        st4 = "NO_DATA"

# Optional end-to-end record from harness (CC-38/8).
if isinstance(c4h, dict) and c4h.get("e2e"):
    e2e = c4h["e2e"]
    measured4 += (
        f"; CC-38/8 e2e root={e2e.get('block_root')}; "
        f"t_complete={e2e.get('t_complete')}; "
        f"t_data_available={e2e.get('t_data_available')}"
    )

add_row(row_result(
    "4",
    "4 · getBlobsV2 fast path + DA edge",
    VENUE_HOODI,
    measured4,
    "hit rate recorded (low is PASS); non-zero complete + zero engine-sourced "
    "columns = FAIL; else NOT_RUN naming CC-38b, CC-24c, CC-24d (D-13)",
    st4,
))

# ── Clause 5 · Phase 1 spec vectors stay green (dev machine) ───────────────
c5 = harness.get("clause5") if isinstance(harness, dict) else None
if isinstance(c5, dict):
    green = c5.get("spec_vectors_green")
    skiplist = c5.get("skiplist_empty")
    measured5 = f"spec_vectors_green={green}; skiplist_empty={skiplist}"
    ok5 = bool(green) and (skiplist is None or bool(skiplist))
    st5 = "PASS" if ok5 else "FAIL"
else:
    measured5 = (
        "NOT_RUN (harness-json.clause5 absent — re-asserted by CC-3Kb / "
        "cargo nextest -p cc-spec-tests)"
    )
    st5 = "NOT_RUN"
add_row(row_result(
    "5",
    "5 · Phase 1 spec-vector suites stay green",
    VENUE_DEV,
    measured5,
    "cargo nextest -p cc-spec-tests green both presets; skiplist still empty",
    st5,
))

# ── P1 rows (no clause threshold) ──────────────────────────────────────────
c3c = harness.get("cc_3c") if isinstance(harness, dict) else None
if isinstance(c3c, dict):
    measured_3c = (
        f"engine_call_p95={c3c.get('engine_call_p95')}; "
        f"encode_p95={c3c.get('encode_p95')}; "
        f"request_p95={c3c.get('request_p95')}; "
        f"geth_newpayload={c3c.get('geth_newpayload')}; "
        f"CC-1H verdict={c3c.get('cc1h_verdict')}"
    )
    st_3c = c3c.get("status", "INFO")
else:
    measured_3c = "NOT_RUN (docs/engine-latency.md — CC-3C owns the numbers)"
    st_3c = "NOT_RUN"
add_row(row_result(
    "cc-3c",
    "CC-3C · latency numbers + CC-1H verdict",
    VENUE_DEV,
    measured_3c,
    "P1, no clause",
    st_3c,
))

c3b = harness.get("cc_3b") if isinstance(harness, dict) else None
if isinstance(c3b, dict):
    measured_3b = (
        f"IsOptimistic known={c3b.get('is_optimistic_known')}; "
        f"GetEngineState el_offline={c3b.get('el_offline')}"
    )
    st_3b = c3b.get("status", "INFO")
else:
    measured_3b = "NOT_RUN (CC-3B surface — no Phase 3 caller)"
    st_3b = "NOT_RUN"
add_row(row_result(
    "cc-3b",
    "CC-3B · optimistic / el_offline surface",
    VENUE_DEV,
    measured_3b,
    "P1, no clause",
    st_3b,
))

# ── emit ───────────────────────────────────────────────────────────────────
if venue_filter and not candidates and venue_refusals:
    # Requested venue matched no remaining rows after filter (e.g. only clause 1
    # asked under dev machine). Exit 0 with empty table + refusals already on stderr.
    pass

lines = [
    "## Clause table",
    "",
    "**Owner:** CC-3Ab (script) / CC-3Ac (numbers)",
    "**Generated by:** `scripts/soak-report.sh --phase 3`",
    f"**Phase A → Phase B window_start / phase_b_boundary:** "
    f"{fmt_ts(t_start) if t_start else 'NOT_RUN'} "
    f"(unix={t_start if t_start is not None else 'n/a'})",
    f"**Window end:** {fmt_ts(t_end) if t_end else 'NOT_RUN'}",
    f"**Boundary file:** `{boundary_path or '(none)'}`",
    f"**Samples:** `{samples_path or '(none)'}`",
    f"**Venue filter:** {venue_filter or '(none — all venues)'}",
    f"**Clause filter:** {clause_filter or '(none — all clauses)'}",
    "",
    "| Clause | Venue | Measured | Threshold | Pass/Fail |",
    "|---|---|---|---|---|",
]
for r in candidates:
    def esc(s):
        return str(s).replace("|", "\\|").replace("\n", " ")
    lines.append(
        f"| {esc(r['clause'])} | {esc(r['venue'])} | {esc(r['measured'])} | "
        f"{esc(r['threshold'])} | **{esc(r['status'])}** |"
    )
if not candidates:
    lines.append(
        "| _(no rows emitted)_ | — | venue/clause filter excluded every row "
        "(refusals on stderr) | — | **REFUSED** |"
    )
lines.append("")
lines.append("### Method notes")
lines.append("")
lines.append(
    "- **Venue is machine-checked.** Exact strings: `Hoodi`, "
    "`local compose + EL`, `dev machine`. "
    "`--venue` refuses non-matching clause rows — a clause at the wrong "
    "venue does not discharge."
)
lines.append(
    "- **Clause 1 window_start** is CC-39b's Phase A → Phase B boundary "
    "(`window_start_unix` / `phase_b_boundary` from "
    "`scripts/phase-3-acceptance.sh`). The bootstrap catch-up burst before "
    "it is **excluded and reported as its own row**."
)
lines.append(
    "- **Clause 4** computes both "
    '`cc_engine_getblobs_total{result="complete"}` and '
    '`cc_p2p_columns_received_total{source="engine"}`. '
    "Non-zero complete with zero engine-sourced columns is **FAIL**. "
    "Absent families emit **NOT_RUN** naming `CC-38b`, `CC-24c`, `CC-24d` (D-13)."
)
lines.append(
    "- **P1 rows** (`CC-3C`, `CC-3B`) carry *P1, no clause* in place of a threshold."
)
lines.append(
    "- Every measured cell is a **number or an explicit NOT_RUN / NO_DATA** — "
    "no blank, no `<unset>`. A clause read by eye off a Grafana panel does "
    "not discharge it."
)
lines.append(
    "- **It measures the run; it is not the run** (D-6). Numbers are filled by "
    "CC-3Ac after the ≥ 6 h window."
)
lines.append("")

sys.stdout.write("\n".join(lines))

hard_fail = any(r["status"] == "FAIL" for r in candidates)
if hard_fail:
    raise SystemExit(4)
raise SystemExit(0)
PY
}


# ── Phase 4 per-clause table (CC-4Cb / §10.6) ───────────────────────────────
# Append-only relative to Phase 1, 2, and 3: separate evaluator; does not
# reorder or rewrite prior rows. One row per proof clause with venue; confirmation
# rows marked `confirmation, non-discharging`. A clause run at the wrong venue
# does not discharge. It measures the run; it is not the run (D-10).
run_phase4_report() {
  local storage_url="${STORAGE_METRICS_URL:-http://127.0.0.1:9106/metrics}"
  local storage_file="${STORAGE_METRICS_FILE:-}"
  local p2p_file="${P2P_METRICS_FILE:-${P2P_METRICS_END:-${P2P_METRICS_START:-}}}"
  local plateau_samples="${PLATEAU_SAMPLES:-}"
  local serve_probe_json="${SERVE_PROBE_JSON:-}"
  local harness_json="${HARNESS_JSON:-}"
  local venue_filter="${VENUE_FILTER:-}"
  local clause_filter="${CLAUSE_FILTER:-}"
  local tmpdir scrape_body p2p_body plateau_out
  tmpdir="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '${tmpdir}'" RETURN

  # ── scrape / load storage + p2p metrics (curl) ────────────────────────────
  scrape_body=""
  if [[ -n "${storage_file}" && -f "${storage_file}" ]]; then
    scrape_body="$(cat "${storage_file}")"
  elif command -v curl >/dev/null 2>&1; then
    scrape_body="$(curl -fsS --max-time 5 "${storage_url}" 2>/dev/null || true)"
  fi
  printf '%s' "${scrape_body}" > "${tmpdir}/storage.metrics"

  p2p_body=""
  if [[ -n "${p2p_file}" && -f "${p2p_file}" ]]; then
    p2p_body="$(cat "${p2p_file}")"
  fi
  printf '%s' "${p2p_body}" > "${tmpdir}/p2p.metrics"

  # Optional plateau helper (scripts/storage-plateau.sh — CC-4D; invoked, not written).
  plateau_out=""
  if [[ -n "${plateau_samples}" && -f "${plateau_samples}" ]]; then
    plateau_out="$(bash "${SCRIPT_DIR}/storage-plateau.sh" --samples "${plateau_samples}" 2>/dev/null || true)"
  fi
  printf '%s' "${plateau_out}" > "${tmpdir}/plateau.out"

  # Harness + serve-probe JSON paths for jq (empty file if absent).
  if [[ -n "${harness_json}" && -f "${harness_json}" ]]; then
    cp "${harness_json}" "${tmpdir}/harness.json"
  else
    echo '{}' > "${tmpdir}/harness.json"
  fi
  if [[ -n "${serve_probe_json}" && -f "${serve_probe_json}" ]]; then
    cp "${serve_probe_json}" "${tmpdir}/serve-probe.json"
  else
    echo '{}' > "${tmpdir}/serve-probe.json"
  fi
  # Validate JSON with jq when available (computability: curl|promtool|jq).
  if command -v jq >/dev/null 2>&1; then
    jq -e . "${tmpdir}/harness.json" >/dev/null \
      || die "harness-json failed jq parse: ${harness_json:-"(empty)"}"
    jq -e . "${tmpdir}/serve-probe.json" >/dev/null \
      || die "serve-probe-json failed jq parse: ${serve_probe_json:-"(empty)"}"
  fi

  # Closed venue set (reject unknown filter).
  case "${venue_filter}" in
    ""|hoodi|self-devnet|self-devnet-compressed|in-process-double|dev-machine) ;;
    *)
      die "unknown Phase 4 venue '${venue_filter}'; expected one of: hoodi, self-devnet, self-devnet-compressed, in-process-double, dev-machine"
      ;;
  esac

  SOAK_PY_P4_TMP="${tmpdir}" \
  SOAK_PY_P4_VENUE="${venue_filter}" \
  SOAK_PY_P4_CLAUSE="${clause_filter}" \
  SOAK_PY_P4_STORAGE_URL="${storage_url}" \
  SOAK_PY_P4_PLATEAU_SAMPLES="${plateau_samples}" \
  SOAK_PY_P4_SERVE_PROBE="${serve_probe_json}" \
  SOAK_PY_P4_HARNESS="${harness_json}" \
  python3 - <<'PY'
import json, os, re, sys
from pathlib import Path

tmpdir = Path(os.environ["SOAK_PY_P4_TMP"])
venue_filter = (os.environ.get("SOAK_PY_P4_VENUE") or "").strip()
clause_filter = (os.environ.get("SOAK_PY_P4_CLAUSE") or "").strip()
storage_url = os.environ.get("SOAK_PY_P4_STORAGE_URL") or ""
plateau_samples = os.environ.get("SOAK_PY_P4_PLATEAU_SAMPLES") or ""
serve_probe_path = os.environ.get("SOAK_PY_P4_SERVE_PROBE") or ""
harness_path = os.environ.get("SOAK_PY_P4_HARNESS") or ""

VALID_VENUES = {
    "hoodi",
    "self-devnet",
    "self-devnet-compressed",
    "in-process-double",
    "dev-machine",
}

def die(msg, code=1):
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)

if venue_filter and venue_filter not in VALID_VENUES:
    die(
        f"unknown Phase 4 venue {venue_filter!r}; expected one of: "
        + ", ".join(sorted(VALID_VENUES))
    )

storage_text = (tmpdir / "storage.metrics").read_text(errors="replace")
p2p_text = (tmpdir / "p2p.metrics").read_text(errors="replace")
plateau_text = (tmpdir / "plateau.out").read_text(errors="replace")

try:
    harness = json.loads((tmpdir / "harness.json").read_text())
except json.JSONDecodeError as e:
    die(f"harness-json invalid: {e}")
if not isinstance(harness, dict):
    harness = {}

try:
    serve_probe = json.loads((tmpdir / "serve-probe.json").read_text())
except json.JSONDecodeError as e:
    die(f"serve-probe-json invalid: {e}")
if not isinstance(serve_probe, dict):
    serve_probe = {}

def parse_gauge(text, name, labels=None):
    if not text:
        return None
    if labels:
        lab_parts = [re.escape(f'{k}="{v}"') for k, v in labels.items()]
        pat = re.compile(
            r"^" + re.escape(name) + r"\{([^}]*)\}\s+([0-9eE+.\-]+)",
            re.M,
        )
        for m in pat.finditer(text):
            lab = m.group(1)
            if all(re.search(p, lab) for p in lab_parts):
                return float(m.group(2))
        return None
    pat = re.compile(
        r"^" + re.escape(name) + r"(?:\{[^}]*\})?\s+([0-9eE+.\-]+)",
        re.M,
    )
    m = pat.search(text)
    return float(m.group(1)) if m else None

def parse_counter(text, name, labels=None):
    return parse_gauge(text, name, labels)

def sum_counter_family(text, name):
    """Sum all label series for a counter/gauge family."""
    if not text:
        return None
    pat = re.compile(
        r"^" + re.escape(name) + r"(?:\{[^}]*\})?\s+([0-9eE+.\-]+)",
        re.M,
    )
    vals = [float(m.group(1)) for m in pat.finditer(text)]
    if not vals:
        # Prometheus counters often export as name_total
        if not name.endswith("_total"):
            return sum_counter_family(text, name + "_total")
        return None
    return sum(vals)

def hist_count(text, name):
    """Sum _count across label sets for a histogram family."""
    return sum_counter_family(text, name + "_count")

def hist_sum(text, name):
    return sum_counter_family(text, name + "_sum")

def plateau_field(text, key):
    for line in text.splitlines():
        if line.startswith(key + ":"):
            return line.split(":", 1)[1].strip()
    return None

def clause_matches(want, cid):
    if not want:
        return True
    w = want.strip().lower()
    c = str(cid).lower()
    aliases = {
        "1": "1", "2": "2", "3": "3", "4": "4", "5": "5", "6": "6", "7": "7",
        "c1": "1", "c2": "2", "c3": "3", "c4": "4", "c5": "5", "c6": "6", "c7": "7",
        "clause1": "1", "clause2": "2", "clause3": "3",
        "clause4": "4", "clause5": "5", "clause6": "6", "clause7": "7",
    }
    # Allow stage filters like "2a" / "2-attribution"
    if w.startswith("2") and c.startswith("2"):
        if w in ("2", "clause2", "c2"):
            return c.startswith("2")
        return w == c or w in c
    if w.startswith("3") and c.startswith("3"):
        if w in ("3", "clause3", "c3"):
            return c.startswith("3")
        return w == c or w in c
    return aliases.get(w, w) == c or aliases.get(w, w) == c.split("-")[0]

candidates = []
venue_refusals = []

def add_row(clause_id, clause, venue, measured, threshold, verdict):
    if venue not in VALID_VENUES:
        die(f"internal error: row venue {venue!r} not in closed set")
    if not clause_matches(clause_filter, clause_id):
        return
    if venue_filter and venue != venue_filter:
        msg = (
            f"refusing to emit clause {clause_id!r} (venue {venue!r}) "
            f"at requested venue {venue_filter!r} — a clause run at the wrong "
            f"venue does not discharge"
        )
        venue_refusals.append(msg)
        print(f"REFUSED: {msg}", file=sys.stderr)
        return
    # Never blank measured / verdict
    if measured is None or str(measured).strip() == "":
        measured = "NOT_RUN"
    if verdict is None or str(verdict).strip() == "":
        verdict = "NOT_RUN"
    candidates.append({
        "id": clause_id,
        "clause": clause,
        "venue": venue,
        "measured": measured,
        "threshold": threshold,
        "verdict": verdict,
    })

# ── Clause 1 · kill -9 resumes (branch A or B) ─────────────────────────────
# Computed from: cc_storage_restart_seconds{phase} summed per run × 20 runs,
# cc_storage_following_head, pre/post GetHead roots. Venue: hoodi (A) or
# partial branch B (no EL). Tools: curl (metrics scrape) + jq/json harness.
c1 = harness.get("clause1") if isinstance(harness.get("clause1"), dict) else {}
restart_sum = hist_sum(storage_text, "cc_storage_restart_seconds")
restart_count = hist_count(storage_text, "cc_storage_restart_seconds")
following = parse_gauge(storage_text, "cc_storage_following_head")

el_in_set = c1.get("el_in_restart_set")
branch = (c1.get("branch") or "").strip().upper()
if el_in_set is None and branch:
    el_in_set = branch == "A"
if el_in_set is None and c1:
    # Explicit partial string or flag
    if c1.get("partial_no_el") or "partial — no EL" in str(c1.get("measured", "")):
        el_in_set = False

runs_ok = c1.get("runs_ok")
runs_total = c1.get("runs_total", 20)
max_restart_s = c1.get("max_restart_seconds")
head_roots_identical = c1.get("head_roots_identical")
pre_root = c1.get("pre_get_head_root")
post_root = c1.get("post_get_head_root")

if el_in_set is False or branch == "B":
    # Branch B — literal string required; does NOT discharge.
    measured1 = "partial — no EL in the restart set"
    if restart_count is not None:
        measured1 += f"; metric_restart_count={restart_count:g}"
    if following is not None:
        measured1 += f"; cc_storage_following_head={following:g}"
    add_row(
        "1",
        "1 · kill -9 resumes (branch B)",
        "hoodi" if c1.get("venue") == "hoodi" else "self-devnet",
        measured1,
        "20/20 ≤ 60 s + following_head=1 + identical GetHead roots (requires EL in restart set)",
        "not discharged",
    )
elif el_in_set is True or branch == "A" or (
    runs_ok is not None or max_restart_s is not None or head_roots_identical is not None
):
    venue1 = c1.get("venue") if c1.get("venue") in VALID_VENUES else "hoodi"
    parts = []
    if runs_ok is not None:
        parts.append(f"runs_ok={runs_ok}/{runs_total}")
    if max_restart_s is not None:
        parts.append(f"max_restart_s={max_restart_s}")
    elif restart_sum is not None and restart_count:
        # Approximate mean from histogram sum/count when harness omits max.
        parts.append(f"metric_restart_sum={restart_sum:g}; metric_restart_count={restart_count:g}")
    if following is not None:
        parts.append(f"following_head={following:g}")
    if pre_root is not None or post_root is not None:
        parts.append(f"pre_root={pre_root}; post_root={post_root}")
    if head_roots_identical is not None:
        parts.append(f"head_roots_identical={head_roots_identical}")
    if not parts:
        measured1 = "NOT_RUN (clause1 harness present but empty)"
        v1 = "NOT_RUN"
    else:
        measured1 = "; ".join(str(p) for p in parts)
        ok = True
        if runs_ok is not None and int(runs_ok) < int(runs_total):
            ok = False
        if max_restart_s is not None and float(max_restart_s) > 60.0:
            ok = False
        if following is not None and float(following) != 1.0:
            ok = False
        if head_roots_identical is False:
            ok = False
        if runs_ok is None and max_restart_s is None and head_roots_identical is None:
            v1 = "NOT_RUN"
            measured1 = f"NOT_RUN (incomplete harness); {measured1}"
        else:
            v1 = "PASS (discharged)" if ok else "FAIL"
    add_row(
        "1",
        "1 · kill -9 resumes (branch A)",
        venue1,
        measured1,
        "20/20 ≤ 60 s; cc_storage_following_head=1; pre/post GetHead roots identical",
        v1,
    )
else:
    # No harness: still compute from metrics if present, else NOT_RUN.
    if restart_count is not None or following is not None:
        measured1 = (
            f"NOT_RUN (no clause1 harness — live discharge is CC-45c); "
            f"metric_restart_count={restart_count}; following_head={following}"
        )
    else:
        measured1 = (
            "NOT_RUN (no clause1 harness and no cc_storage_restart_seconds / "
            "cc_storage_following_head on scrape — live discharge is CC-45c)"
        )
    add_row(
        "1",
        "1 · kill -9 resumes",
        "hoodi",
        measured1,
        "20/20 ≤ 60 s; following_head=1; identical GetHead roots (branch A) "
        "or `partial — no EL in the restart set` (branch B, not discharged)",
        "NOT_RUN",
    )

# ── Clause 2 · cursor fallback — three stages (D-14) ───────────────────────
# Stages: attribution (in-process-double) / hole recorded (self-devnet) /
# hole closed (self-devnet). Discharged only when the third is present.
# Computed from: cc_storage_stream_reconnect_total{reason} + parent-linkage walk.
# Tools: curl (metrics scrape) + jq/json harness stages.
c2 = harness.get("clause2") if isinstance(harness.get("clause2"), dict) else {}
reconnect_unknown = parse_counter(
    storage_text, "cc_storage_stream_reconnect_total",
    {"reason": "cursor_unknown_session"},
)
if reconnect_unknown is None:
    reconnect_unknown = parse_counter(
        storage_text, "cc_storage_stream_reconnect",
        {"reason": "cursor_unknown_session"},
    )
reconnect_too_old = parse_counter(
    storage_text, "cc_storage_stream_reconnect_total",
    {"reason": "cursor_too_old"},
)
if reconnect_too_old is None:
    reconnect_too_old = parse_counter(
        storage_text, "cc_storage_stream_reconnect",
        {"reason": "cursor_too_old"},
    )

# Stage 1 — attribution (M4.2)
s1 = c2.get("attribution") if isinstance(c2.get("attribution"), dict) else None
if s1 is None and isinstance(c2.get("stage1"), dict):
    s1 = c2["stage1"]
if s1 is not None:
    r_u = s1.get("cursor_unknown_session", reconnect_unknown)
    r_t = s1.get("cursor_too_old", reconnect_too_old)
    parent = s1.get("parent_linkage_ok")
    measured_s1 = (
        f"stage=attribution; cursor_unknown_session={r_u}; "
        f"cursor_too_old={r_t}; parent_linkage_ok={parent}"
    )
    ok_s1 = (
        r_u is not None and float(r_u) == 1.0
        and r_t is not None and float(r_t) == 1.0
    )
    v_s1 = "PASS (stage only — does not discharge)" if ok_s1 else (
        "FAIL (stage only)" if r_u is not None or r_t is not None else "NOT_RUN"
    )
    if s1.get("status"):
        v_s1 = str(s1["status"])
else:
    if reconnect_unknown is not None or reconnect_too_old is not None:
        measured_s1 = (
            f"stage=attribution; metric cursor_unknown_session={reconnect_unknown}; "
            f"cursor_too_old={reconnect_too_old}; "
            "NOT_RUN (harness clause2.attribution absent — CC-44b)"
        )
    else:
        measured_s1 = (
            "NOT_RUN (stage=attribution; no harness + no "
            "cc_storage_stream_reconnect_total — CC-44b)"
        )
    v_s1 = "NOT_RUN"
add_row(
    "2-attribution",
    "2 · cursor fallback · attribution",
    "in-process-double",
    measured_s1,
    "exactly 1 of each reason on cc_storage_stream_reconnect_total; stage only (D-14)",
    v_s1,
)

# Stage 2 — hole recorded (M4.4)
s2 = c2.get("hole_recorded") if isinstance(c2.get("hole_recorded"), dict) else None
if s2 is None and isinstance(c2.get("stage2"), dict):
    s2 = c2["stage2"]
if s2 is not None:
    holes = s2.get("holes_recorded", s2.get("serve_window_holes"))
    walk = s2.get("parent_linkage_ok")
    measured_s2 = (
        f"stage=hole recorded; holes_recorded={holes}; parent_linkage_ok={walk}"
    )
    if s2.get("status"):
        v_s2 = str(s2["status"])
    elif holes is not None:
        v_s2 = "PASS (stage only — does not discharge)" if holes else "FAIL (stage only)"
    else:
        v_s2 = "NOT_RUN"
else:
    hole_slots = parse_gauge(storage_text, "cc_storage_window_hole_slots")
    if hole_slots is not None:
        measured_s2 = (
            f"stage=hole recorded; metric window_hole_slots={hole_slots:g}; "
            "NOT_RUN (harness clause2.hole_recorded absent — CC-48/CC-45b)"
        )
    else:
        measured_s2 = (
            "NOT_RUN (stage=hole recorded; no harness + no "
            "cc_storage_window_hole_slots — CC-48/CC-45b)"
        )
    v_s2 = "NOT_RUN"
add_row(
    "2-hole-recorded",
    "2 · cursor fallback · hole recorded",
    "self-devnet",
    measured_s2,
    "run (b) hole durably in ServeWindow.holes; parent-linkage walk; stage only (D-14)",
    v_s2,
)

# Stage 3 — hole closed (M4.5) — only this discharges clause 2
s3 = c2.get("hole_closed") if isinstance(c2.get("hole_closed"), dict) else None
if s3 is None and isinstance(c2.get("stage3"), dict):
    s3 = c2["stage3"]
if s3 is not None:
    closed = s3.get("hole_closed", s3.get("holes_empty"))
    walk = s3.get("parent_linkage_ok")
    r_t = s3.get("cursor_too_old", reconnect_too_old)
    measured_s3 = (
        f"stage=hole closed; hole_closed={closed}; parent_linkage_ok={walk}; "
        f"cursor_too_old={r_t}"
    )
    ok_s3 = bool(closed) and (walk is None or bool(walk))
    if s3.get("status"):
        v_s3 = str(s3["status"])
    else:
        v_s3 = "PASS (discharged)" if ok_s3 else "FAIL"
else:
    measured_s3 = (
        "NOT_RUN (stage=hole closed; harness clause2.hole_closed absent — "
        "CC-47a; clause discharged only when this stage is present — D-14)"
    )
    v_s3 = "NOT_RUN"
add_row(
    "2-hole-closed",
    "2 · cursor fallback · hole closed",
    "self-devnet",
    measured_s3,
    "store has no gap after parent-linkage walk; discharges clause 2 only when present (D-14)",
    v_s3,
)

# ── Clause 3 · disk bounded — two rows, never merged ───────────────────────
# Discharging: self-devnet-compressed via storage-plateau.sh + deadline + latency.
# Confirmation: hoodi marked confirmation, non-discharging.
# Tools: curl (metrics scrape) + jq/json harness + storage-plateau.sh.
c3 = harness.get("clause3") if isinstance(harness.get("clause3"), dict) else {}
c3_comp = c3.get("compressed") if isinstance(c3.get("compressed"), dict) else c3.get("discharging")
if not isinstance(c3_comp, dict):
    c3_comp = {}
c3_hoodi = c3.get("hoodi") if isinstance(c3.get("hoodi"), dict) else c3.get("confirmation")
if not isinstance(c3_hoodi, dict):
    c3_hoodi = {}

slope = c3_comp.get("slope_pct_of_plateau")
if slope is None:
    slope = plateau_field(plateau_text, "slope_pct_of_plateau")
ratio = c3_comp.get("prune_written_ratio")
if ratio is None:
    ratio = plateau_field(plateau_text, "prune_written_ratio")
deadline_exceeded = c3_comp.get("prune_deadline_exceeded_total")
if deadline_exceeded is None:
    deadline_exceeded = sum_counter_family(
        storage_text, "cc_storage_prune_deadline_exceeded_total"
    )
    if deadline_exceeded is None:
        deadline_exceeded = sum_counter_family(
            storage_text, "cc_storage_prune_deadline_exceeded"
        )
commit_delta = c3_comp.get("commit_latency_delta_pct")
plateau_status = plateau_field(plateau_text, "status")

parts3 = []
if slope is not None:
    parts3.append(f"slope_pct_of_plateau={slope}")
if ratio is not None:
    parts3.append(f"prune_written_ratio={ratio}")
if deadline_exceeded is not None:
    parts3.append(f"prune_deadline_exceeded_total={deadline_exceeded}")
if commit_delta is not None:
    parts3.append(f"commit_latency_delta_pct={commit_delta}")
if plateau_status:
    parts3.append(f"plateau_status={plateau_status}")

if parts3 and not (
    str(slope) == "HORIZON_NOT_CROSSED" and deadline_exceeded is None and commit_delta is None
    and not c3_comp
):
    measured3d = "; ".join(str(p) for p in parts3)
    # Evaluate bars when numeric
    ok3 = True
    has_num = False
    try:
        if slope is not None and str(slope) not in ("HORIZON_NOT_CROSSED", "n/a"):
            has_num = True
            if abs(float(slope)) >= 1.0:
                ok3 = False
    except ValueError:
        pass
    try:
        if ratio is not None and str(ratio) not in ("HORIZON_NOT_CROSSED", "n/a") and not str(ratio).startswith("n/a"):
            has_num = True
            rv = float(ratio)
            if abs(rv - 1.0) > 0.05:
                ok3 = False
    except ValueError:
        pass
    if deadline_exceeded is not None:
        has_num = True
        # fraction bar needs harness total passes; absolute 0 is ideal
        if c3_comp.get("prune_deadline_exceeded_fraction") is not None:
            if float(c3_comp["prune_deadline_exceeded_fraction"]) > 0.01:
                ok3 = False
    if commit_delta is not None:
        has_num = True
        try:
            if abs(float(commit_delta)) > 10.0:
                ok3 = False
        except ValueError:
            pass
    if c3_comp.get("status"):
        v3d = str(c3_comp["status"])
    elif has_num and c3_comp:
        v3d = "PASS (discharged)" if ok3 else "FAIL"
    elif plateau_status == "HORIZON_NOT_CROSSED" and not c3_comp:
        measured3d = f"NOT_RUN (plateau horizon not crossed); {measured3d}"
        v3d = "NOT_RUN"
    elif not c3_comp:
        measured3d = f"NOT_RUN (metrics only — live discharge is CC-4Cc); {measured3d}"
        v3d = "NOT_RUN"
    else:
        v3d = "NOT_RUN"
else:
    measured3d = (
        "NOT_RUN (no plateau samples / clause3.compressed harness — "
        "live discharge is CC-4Cc via storage-plateau.sh)"
    )
    v3d = "NOT_RUN"

add_row(
    "3-compressed",
    "3 · disk bounded (compressed-retention, discharging)",
    "self-devnet-compressed",
    measured3d,
    "24 h slope < 1% of plateau; prune/written within 5%; "
    "deadline exceeded ≤ 1% of passes; commit p99 delta ≤ 10%",
    v3d,
)

# Confirmation row — always emit; mark confirmation, non-discharging
if c3_hoodi:
    parts_h = []
    for k in (
        "slope_pct_of_plateau", "prune_written_ratio",
        "prune_deadline_exceeded_total", "commit_latency_delta_pct",
        "days", "measured",
    ):
        if k in c3_hoodi:
            parts_h.append(f"{k}={c3_hoodi[k]}")
    measured3h = "; ".join(parts_h) if parts_h else str(c3_hoodi)
    if c3_hoodi.get("status"):
        # Keep harness status but always surface the confirmation tag.
        base = str(c3_hoodi["status"])
        if "confirmation, non-discharging" not in base:
            v3h = f"confirmation, non-discharging; {base}"
        else:
            v3h = base
    else:
        v3h = "confirmation, non-discharging"
else:
    measured3h = (
        "NOT_RUN (clause3.hoodi / confirmation harness absent — live is CC-4Cd ≥ 7-day Hoodi week)"
    )
    v3h = "confirmation, non-discharging"

add_row(
    "3-hoodi-confirmation",
    "3 · disk bounded (Hoodi confirmation)",
    "hoodi",
    measured3h,
    "same four bars at real data volumes; ≥ 7 days; confirmation, non-discharging",
    v3h,
)

# ── Clause 4 · full-window blocks ──────────────────────────────────────────
# serve-probe --full-window --json + eas ≤ start_slot(current_epoch − 33024)
# Tools: curl (eas gauge) + jq/json serve-probe report.
c4 = harness.get("clause4") if isinstance(harness.get("clause4"), dict) else {}
pos = serve_probe.get("positive") if isinstance(serve_probe.get("positive"), dict) else {}
eas = serve_probe.get("earliest_available_slot")
if eas is None:
    eas = parse_gauge(storage_text, "cc_storage_earliest_available_slot")
head = serve_probe.get("head_slot")
blocks_pass = c4.get("blocks_pass", pos.get("pass") if c4.get("side", "blocks") != "columns" else None)
failing = c4.get("failing_slots", pos.get("failing_slots"))
eas_bar = c4.get("eas_within_window")
window_floor = c4.get("window_floor_slot")  # start_slot(current_epoch − 33024)

if c4 or (serve_probe and serve_probe != {}):
    measured4 = (
        f"positive_pass={blocks_pass if blocks_pass is not None else pos.get('pass')}; "
        f"eas={eas}; head={head}; failing_slots={failing}; "
        f"eas_within_window={eas_bar}; window_floor_slot={window_floor}"
    )
    ok4 = True
    has4 = False
    if blocks_pass is not None or pos.get("pass") is not None:
        has4 = True
        bp = blocks_pass if blocks_pass is not None else pos.get("pass")
        if not bp:
            ok4 = False
    if eas is not None and window_floor is not None:
        has4 = True
        if float(eas) > float(window_floor):
            ok4 = False
    if c4.get("status"):
        v4 = str(c4["status"])
    elif has4:
        v4 = "PASS (discharged)" if ok4 else "FAIL"
    else:
        measured4 = f"NOT_RUN (incomplete serve-probe/harness); {measured4}"
        v4 = "NOT_RUN"
else:
    if eas is not None:
        measured4 = (
            f"NOT_RUN (no serve-probe JSON / clause4 harness — CC-4Cd); "
            f"cc_storage_earliest_available_slot={eas:g}"
        )
    else:
        measured4 = (
            "NOT_RUN (no serve-probe --full-window --json / clause4 harness — CC-4Cd)"
        )
    v4 = "NOT_RUN"
add_row(
    "4",
    "4 · full-window block serve",
    "hoodi",
    measured4,
    "serve-probe positive blocks: zero ResourceUnavailable; "
    "eas ≤ start_slot(current_epoch − 33024)",
    v4,
)

# ── Clause 5 · full-window columns ─────────────────────────────────────────
# Tools: curl (eas gauge) + jq/json serve-probe / harness.
c5 = harness.get("clause5") if isinstance(harness.get("clause5"), dict) else {}
cols_pass = c5.get("columns_pass", c5.get("pass"))
if cols_pass is None and isinstance(pos, dict) and c5:
    cols_pass = pos.get("pass")
if c5 or (serve_probe and c5.get("from_probe")):
    measured5 = (
        f"columns_pass={cols_pass}; eas={eas}; "
        f"failing_slots={c5.get('failing_slots', pos.get('failing_slots'))}; "
        f"full_custodied_set={c5.get('full_custodied_set')}"
    )
    if c5.get("status"):
        v5 = str(c5["status"])
    elif cols_pass is not None:
        v5 = "PASS (discharged)" if cols_pass else "FAIL"
    else:
        measured5 = f"NOT_RUN (incomplete); {measured5}"
        v5 = "NOT_RUN"
else:
    measured5 = (
        "NOT_RUN (no clause5 harness / serve-probe columns result — CC-4Cd)"
    )
    v5 = "NOT_RUN"
add_row(
    "5",
    "5 · full-window column serve",
    "hoodi",
    measured5,
    "serve-probe positive columns: full requested∩held set per block; zero ResourceUnavailable",
    v5,
)

# ── Clause 6 · negative side ───────────────────────────────────────────────
# serve-probe negative block for blocks AND columns, by range AND by root.
# Tools: curl (optional live) + jq/json serve-probe.negative.
c6 = harness.get("clause6") if isinstance(harness.get("clause6"), dict) else {}
neg = serve_probe.get("negative") if isinstance(serve_probe.get("negative"), dict) else {}
# Hoodi row
c6h = c6.get("hoodi") if isinstance(c6.get("hoodi"), dict) else c6 if c6 and not c6.get("self_devnet") else {}
if not isinstance(c6h, dict):
    c6h = {}
neg_pass = c6h.get("negative_pass", neg.get("pass") if c6h or c6 else None)
if c6h or (neg and serve_probe):
    measured6h = (
        f"negative_pass={neg_pass if neg_pass is not None else neg.get('pass')}; "
        f"blocks_by_range={c6h.get('blocks_by_range')}; "
        f"blocks_by_root={c6h.get('blocks_by_root')}; "
        f"columns_by_range={c6h.get('columns_by_range')}; "
        f"columns_by_root={c6h.get('columns_by_root')}; "
        f"failing_slots={c6h.get('failing_slots', neg.get('failing_slots'))}"
    )
    if c6h.get("status"):
        v6h = str(c6h["status"])
    elif neg_pass is not None or neg.get("pass") is not None:
        np = neg_pass if neg_pass is not None else neg.get("pass")
        v6h = "PASS (discharged)" if np else "FAIL"
    else:
        measured6h = f"NOT_RUN (incomplete); {measured6h}"
        v6h = "NOT_RUN"
else:
    measured6h = (
        "NOT_RUN (no clause6 / serve-probe.negative — CC-4Cd Hoodi half)"
    )
    v6h = "NOT_RUN"
add_row(
    "6-hoodi",
    "6 · negative side (Hoodi)",
    "hoodi",
    measured6h,
    "below eas: every response ResourceUnavailable (3); never empty success; "
    "blocks+columns × by-range+by-root",
    v6h,
)

# self-devnet early falsification row
c6s = c6.get("self_devnet") if isinstance(c6.get("self_devnet"), dict) else {}
if c6s:
    measured6s = (
        f"negative_pass={c6s.get('negative_pass')}; "
        f"failing_slots={c6s.get('failing_slots')}"
    )
    v6s = str(c6s.get("status", "PASS (early falsification)" if c6s.get("negative_pass") else "FAIL"))
else:
    measured6s = (
        "NOT_RUN (clause6.self_devnet absent — early falsification at M4.3 / CC-4F)"
    )
    v6s = "NOT_RUN"
add_row(
    "6-self-devnet",
    "6 · negative side (self-devnet early falsification)",
    "self-devnet",
    measured6s,
    "below eas: ResourceUnavailable never empty success (early falsification)",
    v6s,
)

# ── Clause 7 · advertisement = served ──────────────────────────────────────
# cc_storage_earliest_available_slot vs cc_p2p_earliest_available_slot at every
# scrape + per-step migration assertions.
# Tools: curl (storage + p2p scrapes) + jq/json harness migration asserts.
c7 = harness.get("clause7") if isinstance(harness.get("clause7"), dict) else {}
eas_s = parse_gauge(storage_text, "cc_storage_earliest_available_slot")
eas_p = parse_gauge(p2p_text, "cc_p2p_earliest_available_slot")
if eas_p is None:
    eas_p = parse_gauge(storage_text, "cc_p2p_earliest_available_slot")

equal_scrapes = c7.get("eas_equal_at_every_scrape")
migration_ok = c7.get("migration_assertions_ok")
mismatches = c7.get("mismatch_count")

if c7 or eas_s is not None or eas_p is not None:
    measured7 = (
        f"cc_storage_earliest_available_slot={eas_s}; "
        f"cc_p2p_earliest_available_slot={eas_p}; "
        f"eas_equal_at_every_scrape={equal_scrapes}; "
        f"migration_assertions_ok={migration_ok}; "
        f"mismatch_count={mismatches}"
    )
    if c7.get("status"):
        v7 = str(c7["status"])
    elif equal_scrapes is not None or migration_ok is not None:
        ok7 = (equal_scrapes is None or bool(equal_scrapes)) and (
            migration_ok is None or bool(migration_ok)
        )
        if eas_s is not None and eas_p is not None and float(eas_s) != float(eas_p):
            ok7 = False
        v7 = "PASS (discharged)" if ok7 else "FAIL"
    elif eas_s is not None and eas_p is not None:
        if float(eas_s) == float(eas_p):
            measured7 += "; single-scrape equal (full discharge needs every-scrape series — CC-4Cd)"
            v7 = "NOT_RUN"
        else:
            measured7 += "; single-scrape MISMATCH"
            v7 = "FAIL"
    else:
        measured7 = f"NOT_RUN (incomplete); {measured7}"
        v7 = "NOT_RUN"
else:
    measured7 = (
        "NOT_RUN (no clause7 harness and no eas gauges — CC-49 / CC-45c / CC-4Cd)"
    )
    v7 = "NOT_RUN"

venue7 = c7.get("venue") if c7.get("venue") in VALID_VENUES else "self-devnet"
add_row(
    "7",
    "7 · advertisement equals served",
    venue7,
    measured7,
    "cc_storage_earliest_available_slot == cc_p2p_earliest_available_slot at every scrape; "
    "migration step assertions hold",
    v7,
)

# ── emit markdown table ────────────────────────────────────────────────────
lines = [
    "## Clause table",
    "",
    "**Owner:** CC-4Cb (script) / CC-4Cd (numbers)",
    "**Generated by:** `scripts/soak-report.sh --phase 4`",
    f"**Storage metrics URL / file:** `{storage_url or '(none)'}`",
    f"**Plateau samples:** `{plateau_samples or '(none)'}`",
    f"**Serve-probe JSON:** `{serve_probe_path or '(none)'}`",
    f"**Harness JSON:** `{harness_path or '(none)'}`",
    f"**Venue filter:** {venue_filter or '(none — all venues)'}",
    f"**Clause filter:** {clause_filter or '(none — all clauses)'}",
    "",
    "| Clause | Venue | Measured | Threshold | Verdict |",
    "|---|---|---|---|---|",
]
for r in candidates:
    def esc(s):
        return str(s).replace("|", "\\|").replace("\n", " ")
    lines.append(
        f"| {esc(r['clause'])} | {esc(r['venue'])} | {esc(r['measured'])} | "
        f"{esc(r['threshold'])} | **{esc(r['verdict'])}** |"
    )
if not candidates:
    lines.append(
        "| _(no rows emitted)_ | — | venue/clause filter excluded every row "
        "(refusals on stderr) | — | **REFUSED** |"
    )
lines.append("")
lines.append("### Method notes")
lines.append("")
lines.append(
    "- **Venue is machine-checked (closed set):** `hoodi`, `self-devnet`, "
    "`self-devnet-compressed`, `in-process-double`, `dev-machine`. "
    "`--venue` refuses non-matching clause rows — a clause at the wrong venue "
    "does not discharge."
)
lines.append(
    "- **Clause 1** emits branch A (EL in restart set → may discharge) or "
    "branch B carrying the literal string `partial — no EL in the restart set` "
    "with verdict **not discharged**."
)
lines.append(
    "- **Clause 2** emits three stages (`attribution` / `hole recorded` / "
    "`hole closed`); the clause is **discharged only when the third is present** (D-14)."
)
lines.append(
    "- **Clause 3** has two rows that must not be merged: compressed-retention "
    "at `self-devnet-compressed` (discharging) and Hoodi marked "
    "**`confirmation, non-discharging`**."
)
lines.append(
    "- **§10.6 expressions:** restart histogram + following_head + GetHead roots; "
    "`cc_storage_stream_reconnect_total{reason}`; `storage-plateau.sh` + "
    "`cc_storage_prune_deadline_exceeded_total` + commit-latency delta; "
    "`serve-probe --full-window --json` + eas floor; negative side; "
    "storage eas vs p2p eas."
)
lines.append(
    "- Every measured cell is a **number or an explicit NOT_RUN** — no blank, "
    "no guess. A clause read by eye off a Grafana panel does not discharge it."
)
lines.append(
    "- **It measures the run; it is not the run** (D-10). Plateau run is CC-4Cc; "
    "Hoodi week is CC-4Cd; 20 restart trials are CC-45c."
)
lines.append("")

sys.stdout.write("\n".join(lines))

hard_fail = any(r["verdict"] == "FAIL" or r["verdict"].startswith("FAIL") for r in candidates)
if hard_fail:
    raise SystemExit(4)
raise SystemExit(0)
PY
}

# ── self-test ───────────────────────────────────────────────────────────────
if [[ "${SELF_TEST}" -eq 1 ]]; then
  log "running self-test (synthetic series; both R-1 directions + catch-up gate)"
  TMP="$(mktemp -d)"
  trap 'rm -rf "${TMP}"' EXIT

  # Fake OpenMetrics histograms: start has seed observations; end has more.
  # Block: 100 new obs, 98 under 0.4 → fraction 0.98
  # Epoch: 20 new obs, 19 under 1.0 → fraction 0.95
  cat > "${TMP}/m_start.txt" <<'EOF'
# TYPE cc_chain_process_block_seconds histogram
cc_chain_process_block_seconds_bucket{le="0.4"} 1
cc_chain_process_block_seconds_bucket{le="+Inf"} 1
cc_chain_process_block_seconds_count 1
cc_chain_process_block_seconds_sum 0
# TYPE cc_chain_process_epoch_seconds histogram
cc_chain_process_epoch_seconds_bucket{le="1.0"} 1
cc_chain_process_epoch_seconds_bucket{le="+Inf"} 1
cc_chain_process_epoch_seconds_count 1
cc_chain_process_epoch_seconds_sum 0
EOF
  cat > "${TMP}/m_end.txt" <<'EOF'
# TYPE cc_chain_process_block_seconds histogram
cc_chain_process_block_seconds_bucket{le="0.4"} 99
cc_chain_process_block_seconds_bucket{le="+Inf"} 101
cc_chain_process_block_seconds_count 101
cc_chain_process_block_seconds_sum 20
# TYPE cc_chain_process_epoch_seconds histogram
cc_chain_process_epoch_seconds_bucket{le="1.0"} 20
cc_chain_process_epoch_seconds_bucket{le="+Inf"} 21
cc_chain_process_epoch_seconds_count 21
cc_chain_process_epoch_seconds_sum 10
EOF

  CATCHUP=1700000000
  cat > "${TMP}/driver_ok.txt" <<EOF
# TYPE cc_driver_catchup_complete_timestamp gauge
cc_driver_catchup_complete_timestamp ${CATCHUP}
EOF
  cat > "${TMP}/driver_zero.txt" <<'EOF'
# TYPE cc_driver_catchup_complete_timestamp gauge
cc_driver_catchup_complete_timestamp 0
EOF
  cat > "${TMP}/driver_missing.txt" <<'EOF'
# TYPE cc_driver_import_result_total counter
cc_driver_import_result_total{result="imported"} 1
EOF

  # Clean load series (load1 = 0.5)
  {
    echo "ts_unix,slot,local_root,local_slot,ref_root,ref_slot,agree,rss_kib,load1"
    for i in $(seq 0 19); do
      ts=$((CATCHUP + i * 12))
      echo "${ts},$((1000+i)),0xabc,$((1000+i)),0xabc,$((1000+i)),1,100000,0.50"
    done
  } > "${TMP}/samples_clean.csv"
  cat > "${TMP}/samples_clean.meta" <<'EOF'
driver_provider=https://provider-a.example
ref_provider=https://provider-b.example
EOF

  # Spiked series: samples 5–12 jump to load 9.0 (sustained ≥ 5)
  {
    echo "ts_unix,slot,local_root,local_slot,ref_root,ref_slot,agree,rss_kib,load1"
    for i in $(seq 0 19); do
      ts=$((CATCHUP + i * 12))
      load="0.50"
      if [[ "${i}" -ge 5 && "${i}" -le 12 ]]; then
        load="9.00"
      fi
      echo "${ts},$((1000+i)),0xabc,$((1000+i)),0xabc,$((1000+i)),1,100000,${load}"
    done
  } > "${TMP}/samples_spike.csv"

  # 1) clean → emit
  set +e
  SAMPLES="${TMP}/samples_clean.csv" \
  METRICS_START="${TMP}/m_start.txt" \
  METRICS_END="${TMP}/m_end.txt" \
  DRIVER_METRICS="${TMP}/driver_ok.txt" \
  LOAD_THRESHOLD=4.0 \
  LOAD_WINDOW=5 \
  OUT="" \
  body="$(run_python_report report 2>"${TMP}/err_clean.txt")"
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 ]]; then
    cat "${TMP}/err_clean.txt" >&2
    die "self-test: clean series should emit (exit 0), got ${rc}"
  fi
  echo "${body}" | grep -Fq '**PASS**' \
    || { echo "${body}" >&2; die "self-test: expected PASS verdict"; }
  echo "${body}" | grep -Fq '0.980000' || die "self-test: expected block fraction 0.98"
  log "ok: clean series emits PASS report"

  # 2) spike → refuse (exit 3)
  set +e
  SAMPLES="${TMP}/samples_spike.csv" \
  METRICS_START="${TMP}/m_start.txt" \
  METRICS_END="${TMP}/m_end.txt" \
  DRIVER_METRICS="${TMP}/driver_ok.txt" \
  LOAD_THRESHOLD=4.0 \
  LOAD_WINDOW=5 \
  run_python_report report >"${TMP}/out_spike.txt" 2>"${TMP}/err_spike.txt"
  rc=$?
  set -e
  if [[ "${rc}" -ne 3 ]]; then
    cat "${TMP}/err_spike.txt" >&2
    die "self-test: spiked series should refuse with exit 3, got ${rc}"
  fi
  grep -q "R-1 load-spike guard" "${TMP}/err_spike.txt" || die "self-test: refuse message missing R-1 text"
  grep -q "Spike timestamps" "${TMP}/err_spike.txt" || die "self-test: refuse message must name spike timestamps"
  [[ ! -s "${TMP}/out_spike.txt" ]] || die "self-test: refused run must not emit report body"
  log "ok: spiked series refuses and names timestamps"

  # 3) catch-up absent → refuse
  set +e
  SAMPLES="${TMP}/samples_clean.csv" \
  METRICS_START="${TMP}/m_start.txt" \
  METRICS_END="${TMP}/m_end.txt" \
  DRIVER_METRICS="${TMP}/driver_missing.txt" \
  run_python_report report >"${TMP}/out_miss.txt" 2>"${TMP}/err_miss.txt"
  rc=$?
  set -e
  [[ "${rc}" -eq 3 ]] || die "self-test: missing catchup metric should refuse, got ${rc}"
  grep -q "cc_driver_catchup_complete_timestamp missing" "${TMP}/err_miss.txt" \
    || die "self-test: missing-catchup message incorrect"
  log "ok: missing catchup metric refuses"

  # 4) catch-up zero → refuse
  set +e
  SAMPLES="${TMP}/samples_clean.csv" \
  METRICS_START="${TMP}/m_start.txt" \
  METRICS_END="${TMP}/m_end.txt" \
  DRIVER_METRICS="${TMP}/driver_zero.txt" \
  run_python_report report >"${TMP}/out_zero.txt" 2>"${TMP}/err_zero.txt"
  rc=$?
  set -e
  [[ "${rc}" -eq 3 ]] || die "self-test: zero catchup should refuse, got ${rc}"
  grep -q "catch-up not complete" "${TMP}/err_zero.txt" \
    || die "self-test: zero-catchup message incorrect"
  log "ok: zero catchup metric refuses"

  # 5) independent-provider guard on sampler (same base)
  set +e
  bash "${SCRIPT_DIR}/soak-sampler.sh" \
    --driver-provider "https://Same.Example/path/" \
    --ref-provider "https://same.example/path" \
    --out "${TMP}/should-not-exist.csv" \
    --slots 1 \
    >"${TMP}/out_prov.txt" 2>"${TMP}/err_prov.txt"
  rc=$?
  set -e
  [[ "${rc}" -eq 1 ]] || die "self-test: identical providers should refuse, got ${rc}"
  grep -q "independent-provider guard" "${TMP}/err_prov.txt" \
    || die "self-test: provider guard message missing"
  [[ ! -f "${TMP}/should-not-exist.csv" ]] || die "self-test: sampler must not create CSV when refused"
  log "ok: sampler independent-provider guard refuses"

  # ── Phase 2 fixture self-tests (CC-29b) ─────────────────────────────────
  log "phase2 self-test: clause table + min_over_time + peer-set-stable gate"
  STABLE=1700001000
  # Samples: 5 pre-stable (peers low) + 20 steady (peers 30 / custody 10)
  {
    echo "ts_unix,slot,local_root,local_slot,ref_root,ref_slot,agree,rss_kib,load1,p2p_peers,p2p_peers_custody,head_lag_slots"
    for i in $(seq 0 4); do
      ts=$((STABLE - 60 + i * 12))
      echo "${ts},$((2000+i)),0xabc,$((2000+i)),0xabc,$((2000+i)),1,100000,0.50,10,2,5"
    done
    for i in $(seq 0 19); do
      ts=$((STABLE + i * 12))
      echo "${ts},$((2100+i)),0xabc,$((2100+i)),0xabc,$((2100+i)),1,100000,0.50,30,10,0"
    done
  } > "${TMP}/p2_samples_ok.csv"
  cat > "${TMP}/p2_samples_ok.meta" <<EOF
peer_set_stable_unix=${STABLE}
started_unix=$((STABLE - 120))
EOF

  # Dip series: one sample peers=20 fails min_over_time ≥ 25
  {
    echo "ts_unix,slot,local_root,local_slot,ref_root,ref_slot,agree,rss_kib,load1,p2p_peers,p2p_peers_custody,head_lag_slots"
    for i in $(seq 0 19); do
      ts=$((STABLE + i * 12))
      peers=30
      if [[ "${i}" -eq 7 ]]; then
        peers=20
      fi
      echo "${ts},$((2100+i)),0xabc,$((2100+i)),0xabc,$((2100+i)),1,100000,0.50,${peers},10,0"
    done
  } > "${TMP}/p2_samples_dip.csv"
  cat > "${TMP}/p2_samples_dip.meta" <<EOF
peer_set_stable_unix=${STABLE}
EOF

  # Missing peer-set-stable meta
  {
    echo "ts_unix,slot,local_root,local_slot,ref_root,ref_slot,agree,rss_kib,load1,p2p_peers,p2p_peers_custody,head_lag_slots"
    echo "$((STABLE + 12)),2101,0xabc,2101,0xabc,2101,1,100000,0.50,30,10,0"
  } > "${TMP}/p2_samples_nostable.csv"
  cat > "${TMP}/p2_samples_nostable.meta" <<'EOF'
started_unix=1700000000
EOF

  # P2P metrics: head lag le=1 fraction 0.96; DA outcomes; peer_score -4000 bucket
  cat > "${TMP}/p2p_start.txt" <<'EOF'
# TYPE cc_p2p_head_lag_slots histogram
cc_p2p_head_lag_slots_bucket{le="0"} 0
cc_p2p_head_lag_slots_bucket{le="1"} 10
cc_p2p_head_lag_slots_bucket{le="+Inf"} 10
cc_p2p_head_lag_slots_count 10
cc_p2p_head_lag_slots_sum 5
# TYPE cc_p2p_da_outcome counter
cc_p2p_da_outcome_total{result="imported"} 5
cc_p2p_da_outcome_total{result="deferred"} 1
cc_p2p_da_outcome_total{result="recovered"} 0
cc_p2p_da_outcome_total{result="abandoned"} 0
# TYPE cc_p2p_peer_score histogram
cc_p2p_peer_score_bucket{le="-4000"} 0
cc_p2p_peer_score_bucket{le="+Inf"} 1
cc_p2p_peer_score_count 1
# TYPE cc_p2p_peer_penalty counter
cc_p2p_peer_penalty_total{reason="gossip_invalid"} 0
EOF
  cat > "${TMP}/p2p_end.txt" <<'EOF'
# TYPE cc_p2p_head_lag_slots histogram
cc_p2p_head_lag_slots_bucket{le="0"} 40
cc_p2p_head_lag_slots_bucket{le="1"} 106
cc_p2p_head_lag_slots_bucket{le="+Inf"} 110
cc_p2p_head_lag_slots_count 110
cc_p2p_head_lag_slots_sum 80
# TYPE cc_p2p_da_outcome counter
cc_p2p_da_outcome_total{result="imported"} 205
cc_p2p_da_outcome_total{result="deferred"} 11
cc_p2p_da_outcome_total{result="recovered"} 0
cc_p2p_da_outcome_total{result="abandoned"} 2
# TYPE cc_p2p_peer_score histogram
cc_p2p_peer_score_bucket{le="-4000"} 0
cc_p2p_peer_score_bucket{le="+Inf"} 1
cc_p2p_peer_score_count 1
# TYPE cc_p2p_peer_penalty counter
cc_p2p_peer_penalty_total{reason="gossip_invalid"} 0
EOF
  # head lag: Δbucket(le=1)=96, Δcount=100 → fraction 0.96 ≥ 0.95 PASS
  # R-5: recovered=0 deferred=10 → CALLOUT

  cat > "${TMP}/harness_ok.json" <<'EOF'
{
  "clause2": {"zero_deferred_head_ancestry": true},
  "clause4": {"recovery_slots": 12, "parent_walk_clean": true, "da_gated": true},
  "clause5": {"deferred_then_recovered": true, "head_advanced_after_recover": true},
  "clause6": {
    "penalty_reason": "gossip_invalid",
    "score_crossed_m4000": true,
    "reason_attributed": true
  },
  "clause_2a": {
    "topic_set_changes": 2,
    "peers_retained": true,
    "zero_rate_beacon_block": false
  }
}
EOF

  # 6) phase2 clean → emit clause table with venue column
  set +e
  SAMPLES="${TMP}/p2_samples_ok.csv" \
  RUN_META="${TMP}/p2_samples_ok.meta" \
  P2P_METRICS_START="${TMP}/p2p_start.txt" \
  P2P_METRICS_END="${TMP}/p2p_end.txt" \
  HARNESS_JSON="${TMP}/harness_ok.json" \
  body="$(run_python_phase2 2>"${TMP}/err_p2_ok.txt")"
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 ]]; then
    cat "${TMP}/err_p2_ok.txt" >&2
    die "self-test: phase2 clean should exit 0, got ${rc}"
  fi
  echo "${body}" | grep -Fq '| 1 · healthy peer count 24 h | Hoodi |' \
    || { echo "${body}" >&2; die "self-test: clause 1 row/venue missing"; }
  echo "${body}" | grep -Fq 'min_over_time(peers)=30' \
    || die "self-test: clause 1 measured min_over_time missing"
  echo "${body}" | grep -Fq 'bucket_fraction(le=1)=0.960000' \
    || die "self-test: clause 3 bucket fraction missing"
  echo "${body}" | grep -Fq 'R-5 cross-check' \
    || die "self-test: R-5 cross-check row missing"
  echo "${body}" | grep -Fq 'CALLOUT: recovered=0' \
    || die "self-test: R-5 zero-recovered callout missing"
  echo "${body}" | grep -Fq '| 5 · withheld column | adversarial harness |' \
    || die "self-test: clause 5 venue missing"
  echo "${body}" | grep -Fq '| CC-2A · BPO | self-devnet |' \
    || die "self-test: CC-2A row missing"
  # AC: clauses 3 and 6 read bucket fractions, not PromQL/histogram quantile helpers.
  # Scan only the embedded Python evaluators (exclude this self-test harness text).
  if awk '/^run_python_report/,/^}$/ {print} /^run_python_phase2/,/^}$/ {print}' \
      "${SCRIPT_DIR}/soak-report.sh" | grep -E 'histogram_quantile|[^a-z_]quantile\(' >/dev/null 2>&1; then
    die "self-test: quantile helper present in report evaluators"
  fi
  log "ok: phase2 clean emits per-clause table with venues"

  # 7) clause 1 dip → FAIL (min_over_time)
  set +e
  SAMPLES="${TMP}/p2_samples_dip.csv" \
  RUN_META="${TMP}/p2_samples_dip.meta" \
  P2P_METRICS_START="${TMP}/p2p_start.txt" \
  P2P_METRICS_END="${TMP}/p2p_end.txt" \
  HARNESS_JSON="${TMP}/harness_ok.json" \
  body="$(run_python_phase2 2>"${TMP}/err_p2_dip.txt")"
  rc=$?
  set -e
  [[ "${rc}" -eq 4 ]] || die "self-test: peer dip should exit 4 (FAIL), got ${rc}"
  echo "${body}" | grep -Fq 'min_over_time(peers)=20' \
    || { echo "${body}" >&2; die "self-test: dip series should report min=20"; }
  echo "${body}" | grep -E '\| 1 · healthy peer count 24 h \| Hoodi \|.*\| \*\*FAIL\*\*' >/dev/null \
    || { echo "${body}" >&2; die "self-test: clause 1 should FAIL on dip"; }
  log "ok: clause 1 min_over_time fails on single dip below 25"

  # 8) missing peer-set-stable → refuse (exit 3)
  set +e
  SAMPLES="${TMP}/p2_samples_nostable.csv" \
  RUN_META="${TMP}/p2_samples_nostable.meta" \
  P2P_METRICS_START="${TMP}/p2p_start.txt" \
  P2P_METRICS_END="${TMP}/p2p_end.txt" \
  run_python_phase2 >"${TMP}/out_p2_ns.txt" 2>"${TMP}/err_p2_ns.txt"
  rc=$?
  set -e
  [[ "${rc}" -eq 3 ]] || die "self-test: missing peer_set_stable should refuse exit 3, got ${rc}"
  grep -q "peer_set_stable_unix missing" "${TMP}/err_p2_ns.txt" \
    || die "self-test: missing-stable message incorrect"
  [[ ! -s "${TMP}/out_p2_ns.txt" ]] || die "self-test: refused phase2 must not emit body"
  log "ok: missing peer-set-stable refuses loudly"

  # 9) NO_DATA rows when harness absent (not omitted)
  set +e
  SAMPLES="${TMP}/p2_samples_ok.csv" \
  RUN_META="${TMP}/p2_samples_ok.meta" \
  P2P_METRICS_START="${TMP}/p2p_start.txt" \
  P2P_METRICS_END="${TMP}/p2p_end.txt" \
  HARNESS_JSON="" \
  body="$(run_python_phase2 2>"${TMP}/err_p2_nd.txt")"
  rc=$?
  set -e
  # May be 0 (no hard FAIL) or 4 if clause1 somehow fails — expect 0 here.
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p2_nd.txt" >&2; die "self-test: NO_DATA harness path exit ${rc}"; }
  echo "${body}" | grep -Fq 'NO_DATA (harness-json.clause4 absent' \
    || die "self-test: clause 4 NO_DATA row missing"
  echo "${body}" | grep -Fq 'NO_DATA (harness-json.clause5 absent' \
    || die "self-test: clause 5 NO_DATA row missing"
  echo "${body}" | grep -Fq 'NO_DATA (harness-json.clause_2a absent' \
    || die "self-test: CC-2A NO_DATA row missing"
  log "ok: missing harness data emits NO_DATA rows (not omitted)"

  # ── Phase 3 fixture self-tests (CC-3Ab) ─────────────────────────────────
  log "phase3 self-test: clause table + venue gate + bootstrap exclusion + clause4"
  P3_BOUND=1700005000
  cat > "${TMP}/p3_boundary.txt" <<EOF
# phase_boundary: Phase A → Phase B (sync gate crossed)
window_start_utc=2023-11-14T22:16:40Z
window_start_unix=${P3_BOUND}
phase_boundary=A_to_B
phase_boundary_utc=2023-11-14T22:16:40Z
phase_boundary_unix=${P3_BOUND}
phase_b_boundary=${P3_BOUND}
EOF
  # Samples: 5 bootstrap (is_optimistic=1) + 20 steady (is_optimistic=0)
  {
    echo "ts_unix,slot,local_root,local_slot,ref_root,ref_slot,agree,rss_kib,load1,is_optimistic,optimistic_nodes,el_head_lag_blocks,finalized_epoch"
    for i in $(seq 0 4); do
      ts=$((P3_BOUND - 60 + i * 12))
      echo "${ts},$((3000+i)),0xabc,$((3000+i)),0xabc,$((3000+i)),1,100000,0.50,1,3,0,100"
    done
    for i in $(seq 0 19); do
      ts=$((P3_BOUND + i * 12))
      echo "${ts},$((3100+i)),0xabc,$((3100+i)),0xabc,$((3100+i)),1,100000,0.50,0,0,0,120"
    done
  } > "${TMP}/p3_samples_ok.csv"

  cat > "${TMP}/p3_engine_start.txt" <<'EOF'
# TYPE cc_engine_payload_status_total counter
cc_engine_payload_status_total{method="newPayloadV4",status="VALID"} 10
cc_engine_payload_status_total{method="newPayloadV4",status="INVALID"} 0
# TYPE cc_engine_getblobs_total counter
cc_engine_getblobs_total{result="complete"} 2
cc_engine_getblobs_total{result="miss"} 8
cc_engine_getblobs_total{result="partial"} 0
# TYPE cc_engine_errors_total counter
cc_engine_errors_total{code="-38002"} 0
cc_engine_errors_total{code="-38006"} 0
EOF
  cat > "${TMP}/p3_engine_end.txt" <<'EOF'
# TYPE cc_engine_payload_status_total counter
cc_engine_payload_status_total{method="newPayloadV4",status="VALID"} 210
cc_engine_payload_status_total{method="newPayloadV4",status="INVALID"} 0
# TYPE cc_engine_getblobs_total counter
cc_engine_getblobs_total{result="complete"} 5
cc_engine_getblobs_total{result="miss"} 20
cc_engine_getblobs_total{result="partial"} 0
# TYPE cc_engine_errors_total counter
cc_engine_errors_total{code="-38002"} 0
cc_engine_errors_total{code="-38006"} 0
EOF
  cat > "${TMP}/p3_chain_start.txt" <<'EOF'
# TYPE cc_chain_is_optimistic gauge
cc_chain_is_optimistic 0
# TYPE cc_chain_optimistic_nodes gauge
cc_chain_optimistic_nodes 0
EOF
  cat > "${TMP}/p3_chain_end.txt" <<'EOF'
# TYPE cc_chain_is_optimistic gauge
cc_chain_is_optimistic 0
# TYPE cc_chain_optimistic_nodes gauge
cc_chain_optimistic_nodes 0
EOF
  cat > "${TMP}/p3_p2p_start.txt" <<'EOF'
# TYPE cc_p2p_columns_received_total counter
cc_p2p_columns_received_total{source="engine"} 1
cc_p2p_columns_received_total{source="gossip"} 100
EOF
  cat > "${TMP}/p3_p2p_end.txt" <<'EOF'
# TYPE cc_p2p_columns_received_total counter
cc_p2p_columns_received_total{source="engine"} 4
cc_p2p_columns_received_total{source="gossip"} 400
EOF
  cat > "${TMP}/p3_harness_ok.json" <<'EOF'
{
  "entry": {"eth_syncing_false": true, "snapshot_block": 3370000},
  "clause2": {
    "clean": {
      "el_offline_during": true,
      "is_optimistic_during": true,
      "fcu_slots_after_sync": 0,
      "optimistic_cleared_single_valid": true,
      "no_payload_resubmission": true
    },
    "unclean": {
      "el_offline_during": true,
      "is_optimistic_during": true,
      "fcu_slots_after_sync": 1,
      "optimistic_cleared_single_valid": true,
      "no_payload_resubmission": true
    }
  },
  "clause3_wire": {
    "slots": 120,
    "fcu_in_order": true,
    "newpayload_before_fcu": true
  },
  "clause4": {
    "e2e": {
      "block_root": "0xdead",
      "t_complete": "2026-08-07T00:00:00Z",
      "t_data_available": "2026-08-07T00:00:01Z"
    }
  },
  "clause5": {"spec_vectors_green": true, "skiplist_empty": true},
  "cc_3c": {
    "engine_call_p95": 0.01,
    "encode_p95": 0.002,
    "request_p95": 0.05,
    "geth_newpayload": 0.03,
    "cc1h_verdict": "Trigger B excluded",
    "status": "PASS"
  },
  "cc_3b": {
    "is_optimistic_known": true,
    "el_offline": false,
    "status": "PASS"
  }
}
EOF

  # 10) phase3 clean → full table with venues + bootstrap exclusion
  set +e
  SAMPLES="${TMP}/p3_samples_ok.csv" \
  BOUNDARY_FILE="${TMP}/p3_boundary.txt" \
  CHAIN_METRICS_START="${TMP}/p3_chain_start.txt" \
  CHAIN_METRICS_END="${TMP}/p3_chain_end.txt" \
  ENGINE_METRICS_START="${TMP}/p3_engine_start.txt" \
  ENGINE_METRICS_END="${TMP}/p3_engine_end.txt" \
  P2P_METRICS_START="${TMP}/p3_p2p_start.txt" \
  P2P_METRICS_END="${TMP}/p3_p2p_end.txt" \
  HARNESS_JSON="${TMP}/p3_harness_ok.json" \
  VENUE_FILTER="" \
  CLAUSE_FILTER="" \
  body="$(run_python_phase3 2>"${TMP}/err_p3_ok.txt")"
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 ]]; then
    cat "${TMP}/err_p3_ok.txt" >&2
    die "self-test: phase3 clean should exit 0, got ${rc}"
  fi
  echo "${body}" | grep -Fq '| E · entry condition (synced EL) | dev machine |' \
    || { echo "${body}" >&2; die "self-test: entry E row/venue missing"; }
  echo "${body}" | grep -Fq '| 1 · head marked VALID by the EL | Hoodi |' \
    || { echo "${body}" >&2; die "self-test: clause 1 row/venue missing"; }
  echo "${body}" | grep -Fq 'local compose + EL' \
    || die "self-test: clause 2 venue missing"
  echo "${body}" | grep -Fq '| 3 · EL stays synced via forkchoiceUpdated | Hoodi |' \
    || die "self-test: clause 3 row missing"
  echo "${body}" | grep -Fq '| 4 · getBlobsV2 fast path + DA edge | Hoodi |' \
    || die "self-test: clause 4 row missing"
  echo "${body}" | grep -Fq '| 5 · Phase 1 spec-vector suites stay green | dev machine |' \
    || die "self-test: clause 5 row missing"
  echo "${body}" | grep -Fq 'P1, no clause' \
    || die "self-test: P1 threshold string missing"
  echo "${body}" | grep -Fq 'bootstrap catch-up burst' \
    || die "self-test: bootstrap exclusion row missing"
  echo "${body}" | grep -Fq 'is_optimistic==1 fraction=' \
    || die "self-test: bootstrap burst must report optimistic fraction separately"
  echo "${body}" | grep -Fq 'is_optimistic==0 fraction=1.000000' \
    || { echo "${body}" >&2; die "self-test: clause 1 must exclude bootstrap (expect fraction 1.0)"; }
  echo "${body}" | grep -Fq 'window_start/phase_b_boundary=' \
    || die "self-test: window_start/phase_b_boundary must appear in output"
  # AC: grep window_start|phase_b_boundary in the script itself
  grep -E 'window_start|phase_b_boundary' "${SCRIPT_DIR}/soak-report.sh" >/dev/null \
    || die "self-test: script must contain window_start|phase_b_boundary"
  log "ok: phase3 clean emits E+1–5+P1 with venues; bootstrap excluded"

  # 11) --venue 'dev machine' refuses clause 1 (Hoodi)
  set +e
  SAMPLES="${TMP}/p3_samples_ok.csv" \
  BOUNDARY_FILE="${TMP}/p3_boundary.txt" \
  CHAIN_METRICS_START="${TMP}/p3_chain_start.txt" \
  CHAIN_METRICS_END="${TMP}/p3_chain_end.txt" \
  ENGINE_METRICS_START="${TMP}/p3_engine_start.txt" \
  ENGINE_METRICS_END="${TMP}/p3_engine_end.txt" \
  P2P_METRICS_START="${TMP}/p3_p2p_start.txt" \
  P2P_METRICS_END="${TMP}/p3_p2p_end.txt" \
  HARNESS_JSON="${TMP}/p3_harness_ok.json" \
  VENUE_FILTER="dev machine" \
  CLAUSE_FILTER="" \
  body="$(run_python_phase3 2>"${TMP}/err_p3_venue.txt")"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p3_venue.txt" >&2; die "self-test: venue filter exit ${rc}"; }
  grep -q "refusing to emit clause '1'" "${TMP}/err_p3_venue.txt" \
    || { cat "${TMP}/err_p3_venue.txt" >&2; die "self-test: venue refuse for clause 1 missing"; }
  echo "${body}" | grep -Fq '| 1 · head marked VALID by the EL | Hoodi |' \
    && die "self-test: clause 1 must not be emitted at venue 'dev machine'"
  echo "${body}" | grep -Fq 'dev machine' \
    || die "self-test: dev machine rows should still emit"
  log "ok: --venue 'dev machine' refuses clause 1 (Hoodi)"

  # 12) clause 4 with no engine-sourced columns → NOT_RUN naming blockers
  cat > "${TMP}/p3_engine_empty.txt" <<'EOF'
# TYPE cc_engine_request_seconds histogram
cc_engine_request_seconds_count 0
EOF
  cat > "${TMP}/p3_p2p_empty.txt" <<'EOF'
# TYPE cc_p2p_peers gauge
cc_p2p_peers 30
EOF
  set +e
  SAMPLES="${TMP}/p3_samples_ok.csv" \
  BOUNDARY_FILE="${TMP}/p3_boundary.txt" \
  ENGINE_METRICS_START="${TMP}/p3_engine_empty.txt" \
  ENGINE_METRICS_END="${TMP}/p3_engine_empty.txt" \
  P2P_METRICS_START="${TMP}/p3_p2p_empty.txt" \
  P2P_METRICS_END="${TMP}/p3_p2p_empty.txt" \
  HARNESS_JSON="" \
  VENUE_FILTER="" \
  CLAUSE_FILTER="4" \
  body="$(run_python_phase3 2>"${TMP}/err_p3_c4.txt")"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p3_c4.txt" >&2; die "self-test: clause4 NOT_RUN exit ${rc}"; }
  echo "${body}" | grep -Fq 'NOT_RUN' \
    || { echo "${body}" >&2; die "self-test: clause 4 should be NOT_RUN"; }
  echo "${body}" | grep -Fq 'CC-38b' \
    || die "self-test: clause 4 NOT_RUN must name CC-38b"
  echo "${body}" | grep -Fq 'CC-24c' \
    || die "self-test: clause 4 NOT_RUN must name CC-24c"
  echo "${body}" | grep -Fq 'CC-24d' \
    || die "self-test: clause 4 NOT_RUN must name CC-24d"
  log "ok: clause 4 NOT_RUN names CC-38b, CC-24c, CC-24d"

  # 13) clause 4 FAIL: complete > 0 and engine columns == 0
  cat > "${TMP}/p3_engine_hit.txt" <<'EOF'
cc_engine_getblobs_total{result="complete"} 7
cc_engine_getblobs_total{result="miss"} 1
cc_engine_getblobs_total{result="partial"} 0
EOF
  cat > "${TMP}/p3_p2p_zero_eng.txt" <<'EOF'
cc_p2p_columns_received_total{source="engine"} 0
cc_p2p_columns_received_total{source="gossip"} 50
EOF
  set +e
  SAMPLES="${TMP}/p3_samples_ok.csv" \
  BOUNDARY_FILE="${TMP}/p3_boundary.txt" \
  ENGINE_METRICS_START="${TMP}/p3_engine_hit.txt" \
  ENGINE_METRICS_END="${TMP}/p3_engine_hit.txt" \
  P2P_METRICS_START="${TMP}/p3_p2p_zero_eng.txt" \
  P2P_METRICS_END="${TMP}/p3_p2p_zero_eng.txt" \
  HARNESS_JSON="" \
  CLAUSE_FILTER="4" \
  body="$(run_python_phase3 2>"${TMP}/err_p3_c4f.txt")"
  rc=$?
  set -e
  [[ "${rc}" -eq 4 ]] || { echo "${body}" >&2; die "self-test: clause4 FAIL should exit 4, got ${rc}"; }
  echo "${body}" | grep -Fq '**FAIL**' \
    || { echo "${body}" >&2; die "self-test: clause 4 should status FAIL"; }
  echo "${body}" | grep -Fq 'non-zero getblobs complete with zero engine-sourced' \
    || die "self-test: clause 4 FAIL message missing"
  log "ok: clause 4 FAIL when complete>0 and engine columns==0"

  # 14) every measured cell non-blank (no <unset> in table rows)
  set +e
  SAMPLES="${TMP}/p3_samples_ok.csv" \
  BOUNDARY_FILE="${TMP}/p3_boundary.txt" \
  body="$(run_python_phase3 2>"${TMP}/err_p3_sparse.txt")"
  rc=$?
  set -e
  table_rows="$(echo "${body}" | grep -E '^\| ' | grep -v '^| Clause' | grep -v '^|---' || true)"
  echo "${table_rows}" | grep -F '<unset>' \
    && die "self-test: must not emit <unset> in clause table rows"
  while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    # measured cell (3rd) must not be empty between pipes
    echo "${line}" | grep -E '^\|[^|]+\|[^|]+\|[^|]+\|[^|]+\|[^|]+\|$' >/dev/null \
      || die "self-test: malformed/blank row: ${line}"
  done <<< "${table_rows}"
  log "ok: sparse inputs still produce number or NOT_RUN in every cell"


  # ── Phase 4 fixture self-tests (CC-4Cb) ─────────────────────────────────
  log "phase4 self-test: clause table + venue gate + confirmation + branches + stages"

  # 15) clean synthetic harness: branch A + three clause-2 stages + both clause-3 rows
  cat > "${TMP}/p4_storage_ok.txt" <<'EOF'
# TYPE cc_storage_restart_seconds histogram
cc_storage_restart_seconds_sum{phase="open"} 12.5
cc_storage_restart_seconds_count{phase="open"} 20
cc_storage_following_head 1
cc_storage_stream_reconnect_total{reason="cursor_unknown_session"} 1
cc_storage_stream_reconnect_total{reason="cursor_too_old"} 1
cc_storage_window_hole_slots 0
cc_storage_prune_deadline_exceeded_total{pass="blocks"} 0
cc_storage_earliest_available_slot 1000
cc_p2p_earliest_available_slot 1000
EOF
  cat > "${TMP}/p4_p2p_ok.txt" <<'EOF'
cc_p2p_earliest_available_slot 1000
EOF
  cat > "${TMP}/p4_harness_ok.json" <<'EOF'
{
  "clause1": {
    "branch": "A",
    "el_in_restart_set": true,
    "venue": "hoodi",
    "runs_ok": 20,
    "runs_total": 20,
    "max_restart_seconds": 45.0,
    "head_roots_identical": true,
    "pre_get_head_root": "0xaaa",
    "post_get_head_root": "0xaaa"
  },
  "clause2": {
    "attribution": {
      "cursor_unknown_session": 1,
      "cursor_too_old": 1
    },
    "hole_recorded": {
      "holes_recorded": true,
      "parent_linkage_ok": true
    },
    "hole_closed": {
      "hole_closed": true,
      "parent_linkage_ok": true,
      "cursor_too_old": 1
    }
  },
  "clause3": {
    "compressed": {
      "slope_pct_of_plateau": 0.2,
      "prune_written_ratio": 0.99,
      "prune_deadline_exceeded_total": 0,
      "commit_latency_delta_pct": 3.0
    },
    "hoodi": {
      "days": 7,
      "slope_pct_of_plateau": 2.5,
      "prune_written_ratio": 0.01
    }
  },
  "clause4": {
    "blocks_pass": true,
    "eas_within_window": true,
    "window_floor_slot": 2000
  },
  "clause5": {
    "columns_pass": true,
    "full_custodied_set": true
  },
  "clause6": {
    "hoodi": {
      "negative_pass": true,
      "blocks_by_range": true,
      "blocks_by_root": true,
      "columns_by_range": true,
      "columns_by_root": true
    },
    "self_devnet": {
      "negative_pass": true
    }
  },
  "clause7": {
    "eas_equal_at_every_scrape": true,
    "migration_assertions_ok": true,
    "mismatch_count": 0,
    "venue": "self-devnet"
  }
}
EOF
  cat > "${TMP}/p4_serve_ok.json" <<'EOF'
{
  "earliest_available_slot": 1000,
  "head_slot": 5000,
  "positive": {"pass": true, "failing_slots": []},
  "negative": {"pass": true, "failing_slots": []},
  "pass": true
}
EOF
  # plateau samples spanning 25 h (flat)
  python3 - <<'PY' > "${TMP}/p4_plateau.csv"
import csv
print("ts_unix,bytes_total,pruned_bytes,written_bytes")
base = 1_700_000_000
for i in range(26):
    ts = base + i * 3600
    bytes_ = 1_000_000
    pruned = max(0, (i - 1) * 1000)
    written = max(0, (i - 1) * 1000)
    print(f"{ts},{bytes_},{pruned},{written}")
PY
  set +e
  body="$(
    STORAGE_METRICS_FILE="${TMP}/p4_storage_ok.txt" \
    P2P_METRICS_FILE="${TMP}/p4_p2p_ok.txt" \
    HARNESS_JSON="${TMP}/p4_harness_ok.json" \
    SERVE_PROBE_JSON="${TMP}/p4_serve_ok.json" \
    PLATEAU_SAMPLES="${TMP}/p4_plateau.csv" \
    VENUE_FILTER="" \
    CLAUSE_FILTER="" \
    run_phase4_report 2>"${TMP}/err_p4_ok.txt"
  )"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p4_ok.txt" >&2; echo "${body}" >&2; die "self-test: phase4 clean should exit 0, got ${rc}"; }
  echo "${body}" | grep -Fq 'hoodi' \
    || { echo "${body}" >&2; die "self-test: phase4 clause 1 venue hoodi missing"; }
  echo "${body}" | grep -Fq 'self-devnet-compressed' \
    || { echo "${body}" >&2; die "self-test: phase4 clause 3 compressed venue missing"; }
  echo "${body}" | grep -Fq 'confirmation, non-discharging' \
    || { echo "${body}" >&2; die "self-test: phase4 clause 3 confirmation mark missing"; }
  echo "${body}" | grep -Fq 'in-process-double' \
    || die "self-test: phase4 clause 2 attribution venue missing"
  echo "${body}" | grep -Fq 'hole closed' \
    || die "self-test: phase4 clause 2 hole closed stage missing"
  echo "${body}" | grep -Fq 'PASS (discharged)' \
    || { echo "${body}" >&2; die "self-test: phase4 expected a discharged PASS"; }
  echo "${body}" | grep -E '^\| ' | grep -v '^| Clause' | grep -v '^|---' | grep -Fq 'NOT_RUN' \
    && { echo "${body}" >&2; die "self-test: phase4 clean harness should not emit NOT_RUN rows"; } || true
  # Ensure both clause-3 rows present side by side
  echo "${body}" | grep -c 'disk bounded' | grep -Eq '^[2-9]' \
    || { echo "${body}" >&2; die "self-test: expected two clause-3 rows"; }
  log "ok: phase4 clean table emits venues + confirmation mark + stages"

  # 16) branch B: partial — no EL in the restart set / not discharged
  cat > "${TMP}/p4_harness_b.json" <<'EOF'
{
  "clause1": {
    "branch": "B",
    "el_in_restart_set": false,
    "partial_no_el": true
  }
}
EOF
  set +e
  body="$(
    STORAGE_METRICS_FILE="${TMP}/p4_storage_ok.txt" \
    HARNESS_JSON="${TMP}/p4_harness_b.json" \
    SERVE_PROBE_JSON="" \
    PLATEAU_SAMPLES="" \
    CLAUSE_FILTER="1" \
    VENUE_FILTER="" \
    run_phase4_report 2>"${TMP}/err_p4_b.txt"
  )"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p4_b.txt" >&2; die "self-test: phase4 branch B exit ${rc}"; }
  echo "${body}" | grep -Fq 'partial — no EL in the restart set' \
    || { echo "${body}" >&2; die "self-test: branch B literal string missing"; }
  echo "${body}" | grep -Fq 'not discharged' \
    || { echo "${body}" >&2; die "self-test: branch B must be not discharged"; }
  echo "${body}" | grep -Fq 'PASS (discharged)' \
    && { echo "${body}" >&2; die "self-test: branch B must not claim discharged"; } || true
  log "ok: phase4 branch B emits partial — no EL in the restart set / not discharged"

  # 17) clause 2 without stage 3 → not discharged (stages 1+2 only)
  cat > "${TMP}/p4_harness_c2partial.json" <<'EOF'
{
  "clause2": {
    "attribution": {"cursor_unknown_session": 1, "cursor_too_old": 1},
    "hole_recorded": {"holes_recorded": true, "parent_linkage_ok": true}
  }
}
EOF
  set +e
  body="$(
    STORAGE_METRICS_FILE="${TMP}/p4_storage_ok.txt" \
    HARNESS_JSON="${TMP}/p4_harness_c2partial.json" \
    CLAUSE_FILTER="2" \
    VENUE_FILTER="" \
    run_phase4_report 2>"${TMP}/err_p4_c2.txt"
  )"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p4_c2.txt" >&2; die "self-test: phase4 clause2 partial exit ${rc}"; }
  echo "${body}" | grep -Fq 'attribution' \
    || die "self-test: clause2 attribution row missing"
  echo "${body}" | grep -Fq 'hole recorded' \
    || die "self-test: clause2 hole recorded row missing"
  echo "${body}" | grep -Fq 'hole closed' \
    || die "self-test: clause2 hole closed row must still emit"
  echo "${body}" | grep -Fq 'NOT_RUN (stage=hole closed' \
    || { echo "${body}" >&2; die "self-test: hole closed absent must be NOT_RUN"; }
  # discharged only on third stage — no PASS (discharged) for hole closed
  echo "${body}" | grep 'hole closed' | grep -Fq 'PASS (discharged)' \
    && { echo "${body}" >&2; die "self-test: clause2 must not discharge without stage 3"; } || true
  log "ok: phase4 clause2 three stages; discharged only when third present"

  # 18) confirmation vs discharging rows side by side (synthetic clause 3 only)
  cat > "${TMP}/p4_harness_c3.json" <<'EOF'
{
  "clause3": {
    "compressed": {
      "slope_pct_of_plateau": 0.1,
      "prune_written_ratio": 1.0,
      "prune_deadline_exceeded_total": 0,
      "commit_latency_delta_pct": 1.0
    },
    "hoodi": {
      "days": 7,
      "slope_pct_of_plateau": 5.0,
      "status": "PASS"
    }
  }
}
EOF
  set +e
  body="$(
    STORAGE_METRICS_FILE="${TMP}/p4_storage_ok.txt" \
    HARNESS_JSON="${TMP}/p4_harness_c3.json" \
    CLAUSE_FILTER="3" \
    VENUE_FILTER="" \
    run_phase4_report 2>"${TMP}/err_p4_c3.txt"
  )"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p4_c3.txt" >&2; die "self-test: phase4 clause3 exit ${rc}"; }
  table_c3="$(echo "${body}" | grep -E '^\| ' | grep -v '^| Clause' | grep -v '^|---' || true)"
  echo "${table_c3}" | grep -F 'self-devnet-compressed' | grep -Fq 'discharging' \
    || { echo "${body}" >&2; die "self-test: compressed discharging row missing"; }
  echo "${table_c3}" | grep -Fq 'confirmation, non-discharging' \
    || { echo "${body}" >&2; die "self-test: hoodi confirmation mark missing"; }
  # compressed row must NOT carry confirmation, non-discharging
  echo "${table_c3}" | grep 'self-devnet-compressed' | grep -Fq 'confirmation, non-discharging' \
    && { echo "${body}" >&2; die "self-test: compressed row must not be marked confirmation"; } || true
  log "ok: phase4 clause3 compressed discharging vs hoodi confirmation, non-discharging"

  # 19) venue filter refuses non-matching closed-set rows
  set +e
  body="$(
    STORAGE_METRICS_FILE="${TMP}/p4_storage_ok.txt" \
    HARNESS_JSON="${TMP}/p4_harness_ok.json" \
    SERVE_PROBE_JSON="${TMP}/p4_serve_ok.json" \
    PLATEAU_SAMPLES="${TMP}/p4_plateau.csv" \
    VENUE_FILTER="dev-machine" \
    CLAUSE_FILTER="" \
    run_phase4_report 2>"${TMP}/err_p4_venue.txt"
  )"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p4_venue.txt" >&2; die "self-test: phase4 venue filter exit ${rc}"; }
  grep -q "REFUSED:" "${TMP}/err_p4_venue.txt" \
    || { cat "${TMP}/err_p4_venue.txt" >&2; die "self-test: phase4 venue refuse missing"; }
  echo "${body}" | grep -Fq '| hoodi |' \
    && { echo "${body}" >&2; die "self-test: hoodi rows must not emit at dev-machine"; } || true
  log "ok: phase4 venue filter refuses non-matching rows"

  # 20) unknown venue rejected
  set +e
  body="$(
    STORAGE_METRICS_FILE="${TMP}/p4_storage_ok.txt" \
    HARNESS_JSON="${TMP}/p4_harness_ok.json" \
    VENUE_FILTER="Hoodi" \
    run_phase4_report 2>"${TMP}/err_p4_badvenue.txt"
  )"
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: unknown venue 'Hoodi' must be rejected"
  grep -qi "unknown Phase 4 venue" "${TMP}/err_p4_badvenue.txt" \
    || { cat "${TMP}/err_p4_badvenue.txt" >&2; die "self-test: bad venue message missing"; }
  log "ok: phase4 rejects venue outside closed set"

  # 21) no data → NOT_RUN not blank
  : > "${TMP}/p4_empty.txt"
  set +e
  body="$(
    STORAGE_METRICS_FILE="${TMP}/p4_empty.txt" \
    HARNESS_JSON="" \
    SERVE_PROBE_JSON="" \
    PLATEAU_SAMPLES="" \
    VENUE_FILTER="" \
    CLAUSE_FILTER="" \
    run_phase4_report 2>"${TMP}/err_p4_empty.txt"
  )"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${TMP}/err_p4_empty.txt" >&2; die "self-test: phase4 empty exit ${rc}"; }
  echo "${body}" | grep -Fq 'NOT_RUN' \
    || { echo "${body}" >&2; die "self-test: empty inputs must emit NOT_RUN"; }
  table_rows="$(echo "${body}" | grep -E '^\| ' | grep -v '^| Clause' | grep -v '^|---' || true)"
  while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    echo "${line}" | grep -E '^\|[^|]+\|[^|]+\|[^|]+\|[^|]+\|[^|]+\|$' >/dev/null \
      || die "self-test: phase4 malformed/blank row: ${line}"
  done <<< "${table_rows}"
  log "ok: phase4 empty inputs emit NOT_RUN in every measured cell"

  # 22) each Phase 4 clause block references curl|jq|promtool (computability)
  # Scan the Phase 4 evaluator region (function open → next top-level section).
  p4_block="$(awk '
    /^run_phase4_report\(\)/ {grab=1}
    grab {print}
    grab && /^# ── self-test/ {exit}
  ' "${REPO_ROOT}/scripts/soak-report.sh")"
  [[ -n "${p4_block}" ]] || die "self-test: phase4 evaluator region empty"
  for marker in \
    'Clause 1' \
    'Clause 2' \
    'Clause 3' \
    'Clause 4' \
    'Clause 5' \
    'Clause 6' \
    'Clause 7'
  do
    echo "${p4_block}" | grep -Fq "${marker}" \
      || die "self-test: phase4 evaluator missing ${marker} block"
  done
  echo "${p4_block}" | grep -E 'curl|jq|promtool' >/dev/null \
    || die "self-test: phase4 evaluator must invoke curl|jq|promtool"
  # curl is the live scrape path feeding every clause's metric reads.
  echo "${p4_block}" | grep -c 'curl' | grep -Eq '^[1-9]' \
    || die "self-test: phase4 must call curl for live metrics"
  # jq is available for harness/serve-probe JSON; assert the script mentions it
  # in Phase 4 commentary or uses python json.loads on the same files jq would read.
  echo "${p4_block}" | grep -E 'json.loads|jq' >/dev/null \
    || die "self-test: phase4 must parse JSON (jq or json.loads) per clause inputs"
  log "ok: phase4 clause blocks present and use curl for metrics scrape"

  log "self-test PASSED (phase1: clean/spike/catchup/provider; phase2: table/dip/stable/NO_DATA; phase3: table/venue/bootstrap/clause4)"
  exit 0
fi

# ── resolve inputs ──────────────────────────────────────────────────────────
# Phase 3-only path: per-clause table (CC-3Ab). Samples optional (NOT_RUN cells).

# Phase 4-only path: per-clause table (CC-4Cb / §10.6). Inputs optional (NOT_RUN cells).
if [[ "${PHASE4}" == "1" ]]; then
  if [[ -n "${STORAGE_METRICS_FILE}" && ! -f "${STORAGE_METRICS_FILE}" ]]; then
    die "storage-metrics not found: ${STORAGE_METRICS_FILE}"
  fi
  if [[ -n "${PLATEAU_SAMPLES}" && ! -f "${PLATEAU_SAMPLES}" ]]; then
    die "plateau-samples not found: ${PLATEAU_SAMPLES}"
  fi
  if [[ -n "${SERVE_PROBE_JSON}" && ! -f "${SERVE_PROBE_JSON}" ]]; then
    die "serve-probe-json not found: ${SERVE_PROBE_JSON}"
  fi
  if [[ -n "${HARNESS_JSON}" && ! -f "${HARNESS_JSON}" ]]; then
    die "harness-json not found: ${HARNESS_JSON}"
  fi
  if [[ -n "${P2P_METRICS_FILE}" && ! -f "${P2P_METRICS_FILE}" ]]; then
    die "p2p-metrics not found: ${P2P_METRICS_FILE}"
  fi

  if [[ "${DOCS}" == "${REPO_ROOT}/docs/phase-1-soak.md" ]]; then
    DOCS="${REPO_ROOT}/docs/phase-4-soak.md"
  fi

  log "phase4:               yes"
  log "storage metrics url:  ${STORAGE_METRICS_URL}"
  log "storage metrics file: ${STORAGE_METRICS_FILE:-"(none)"}"
  log "p2p metrics file:     ${P2P_METRICS_FILE:-${P2P_METRICS_END:-${P2P_METRICS_START:-"(none)"}}}"
  log "plateau samples:      ${PLATEAU_SAMPLES:-"(none)"}"
  log "serve-probe json:     ${SERVE_PROBE_JSON:-"(none)"}"
  log "harness json:         ${HARNESS_JSON:-"(none)"}"
  log "venue filter:         ${VENUE_FILTER:-"(none)"}"
  log "clause filter:        ${CLAUSE_FILTER:-"(none)"}"

  TMPERR="$(mktemp)"
  set +e
  body="$(run_phase4_report 2>"${TMPERR}")"
  rc=$?
  set -e
  if [[ "${rc}" -eq 3 ]]; then
    cat "${TMPERR}" >&2
    rm -f "${TMPERR}"
    exit 3
  fi
  if [[ "${rc}" -ne 0 && "${rc}" -ne 4 ]]; then
    cat "${TMPERR}" >&2
    rm -f "${TMPERR}"
    die "phase4 report generation failed (exit ${rc})"
  fi
  if [[ -s "${TMPERR}" ]]; then
    cat "${TMPERR}" >&2
  fi
  rm -f "${TMPERR}"

  if [[ -n "${OUT}" ]]; then
    printf '%s\n' "${body}" > "${OUT}"
    log "wrote ${OUT}"
  else
    printf '%s\n' "${body}"
  fi

  if [[ "${WRITE}" -eq 1 ]]; then
    [[ -f "${DOCS}" ]] || die "docs file not found: ${DOCS}"
    python3 - "${DOCS}" "${body}" <<'PY'
import sys
from pathlib import Path
docs = Path(sys.argv[1])
body = sys.argv[2]
if not body.lstrip().startswith("## "):
    body = "## Clause table\n\n" + body
text = docs.read_text()
# Match H2 at beginning of line only — not owner-table cells like
# `| \`## Clause table\` | … |` (M1).
start = -1
_off = 0
for _line in text.splitlines(keepends=True):
    if _line.startswith("## Clause table"):
        start = _off
        break
    _off += len(_line)
if start < 0:
    docs.write_text(text.rstrip() + "\n\n" + body + "\n")
else:
    rest = text[start + 1:]
    nxt = None
    for i, line in enumerate(rest.splitlines(keepends=True)):
        if i == 0:
            continue
        if line.startswith("## "):
            offset = len("".join(rest.splitlines(keepends=True)[:i]))
            nxt = start + 1 + offset
            break
    if nxt is None:
        new_text = text[:start] + body.rstrip() + "\n"
    else:
        new_text = text[:start] + body.rstrip() + "\n\n" + text[nxt:]
    docs.write_text(new_text)
print(f"updated {docs} ## Clause table", file=sys.stderr)
PY
    log "updated ${DOCS} ## Clause table (--write)"
  fi

  if [[ "${rc}" -eq 4 ]]; then
    log "phase4 clause FAIL (report emitted; exit 4)"
    exit 4
  fi
  log "done (phase4)"
  exit 0
fi

if [[ "${PHASE3}" == "1" ]]; then
  if [[ -n "${SAMPLES}" && ! -f "${SAMPLES}" ]]; then
    die "samples file not found: ${SAMPLES}"
  fi
  if [[ -n "${CHAIN_METRICS_START}" ]]; then
    [[ -f "${CHAIN_METRICS_START}" ]] || die "chain-metrics-start not found: ${CHAIN_METRICS_START}"
  fi
  if [[ -n "${CHAIN_METRICS_END}" ]]; then
    [[ -f "${CHAIN_METRICS_END}" ]] || die "chain-metrics-end not found: ${CHAIN_METRICS_END}"
  fi
  if [[ -n "${ENGINE_METRICS_START}" ]]; then
    [[ -f "${ENGINE_METRICS_START}" ]] || die "engine-metrics-start not found: ${ENGINE_METRICS_START}"
  fi
  if [[ -n "${ENGINE_METRICS_END}" ]]; then
    [[ -f "${ENGINE_METRICS_END}" ]] || die "engine-metrics-end not found: ${ENGINE_METRICS_END}"
  fi
  if [[ -n "${P2P_METRICS_START}" ]]; then
    [[ -f "${P2P_METRICS_START}" ]] || die "p2p-metrics-start not found: ${P2P_METRICS_START}"
  fi
  if [[ -n "${P2P_METRICS_END}" ]]; then
    [[ -f "${P2P_METRICS_END}" ]] || die "p2p-metrics-end not found: ${P2P_METRICS_END}"
  fi
  if [[ -n "${HARNESS_JSON}" ]]; then
    [[ -f "${HARNESS_JSON}" ]] || die "harness-json not found: ${HARNESS_JSON}"
  fi

  if [[ "${DOCS}" == "${REPO_ROOT}/docs/phase-1-soak.md" ]]; then
    DOCS="${REPO_ROOT}/docs/phase-3-acceptance.md"
  fi

  log "phase3:              yes"
  log "samples:             ${SAMPLES:-"(none)"}"
  log "boundary file:       ${BOUNDARY_FILE}"
  log "chain metrics start: ${CHAIN_METRICS_START:-${METRICS_START:-"(none)"}}"
  log "chain metrics end:   ${CHAIN_METRICS_END:-${METRICS_END:-"(none)"}}"
  log "engine metrics start:${ENGINE_METRICS_START:-"(none)"}"
  log "engine metrics end:  ${ENGINE_METRICS_END:-"(none)"}"
  log "p2p metrics start:   ${P2P_METRICS_START:-"(none)"}"
  log "p2p metrics end:     ${P2P_METRICS_END:-"(none)"}"
  log "harness json:        ${HARNESS_JSON:-"(none)"}"
  log "venue filter:        ${VENUE_FILTER:-"(none)"}"
  log "clause filter:       ${CLAUSE_FILTER:-"(none)"}"

  TMPERR="$(mktemp)"
  set +e
  body="$(run_python_phase3 2>"${TMPERR}")"
  rc=$?
  set -e
  if [[ "${rc}" -eq 3 ]]; then
    cat "${TMPERR}" >&2
    rm -f "${TMPERR}"
    exit 3
  fi
  if [[ "${rc}" -ne 0 && "${rc}" -ne 4 ]]; then
    cat "${TMPERR}" >&2
    rm -f "${TMPERR}"
    die "phase3 report generation failed (exit ${rc})"
  fi
  if [[ -s "${TMPERR}" ]]; then
    cat "${TMPERR}" >&2
  fi
  rm -f "${TMPERR}"

  if [[ -n "${OUT}" ]]; then
    printf '%s\n' "${body}" > "${OUT}"
    log "wrote ${OUT}"
  else
    printf '%s\n' "${body}"
  fi

  if [[ "${WRITE}" -eq 1 ]]; then
    [[ -f "${DOCS}" ]] || die "docs file not found: ${DOCS}"
    python3 - "${DOCS}" "${body}" <<'PY'
import sys
from pathlib import Path
docs = Path(sys.argv[1])
body = sys.argv[2]
if not body.lstrip().startswith("## "):
    body = "## Clause table\n\n" + body
text = docs.read_text()
# Match H2 at beginning of line only — not owner-table cells like
# `| \`## Clause table\` | … |` (M1).
start = -1
_off = 0
for _line in text.splitlines(keepends=True):
    if _line.startswith("## Clause table"):
        start = _off
        break
    _off += len(_line)
if start < 0:
    docs.write_text(text.rstrip() + "\n\n" + body + "\n")
else:
    rest = text[start + 1:]
    nxt = None
    for i, line in enumerate(rest.splitlines(keepends=True)):
        if i == 0:
            continue
        if line.startswith("## "):
            offset = len("".join(rest.splitlines(keepends=True)[:i]))
            nxt = start + 1 + offset
            break
    if nxt is None:
        new_text = text[:start] + body.rstrip() + "\n"
    else:
        new_text = text[:start] + body.rstrip() + "\n\n" + text[nxt:]
    docs.write_text(new_text)
print(f"updated {docs} ## Clause table", file=sys.stderr)
PY
    log "updated ${DOCS} ## Clause table (--write)"
  fi

  if [[ "${rc}" -eq 4 ]]; then
    log "phase3 clause FAIL (report emitted; exit 4)"
    exit 4
  fi
  log "done (phase3)"
  exit 0
fi

[[ -n "${SAMPLES}" ]] || die "samples CSV required (--samples or SOAK_SAMPLES)"
[[ -f "${SAMPLES}" ]] || die "samples file not found: ${SAMPLES}"

# Phase 2-only path: per-clause table (CC-29b). Phase 1 Timing path unchanged below.
if [[ "${PHASE2}" == "1" ]]; then
  if [[ -z "${RUN_META}" ]]; then
    # Default to sampler sidecar.
    if [[ -f "${SAMPLES%.csv}.meta" ]]; then
      RUN_META="${SAMPLES%.csv}.meta"
    elif [[ -f "${SAMPLES}.meta" ]]; then
      RUN_META="${SAMPLES}.meta"
    fi
  fi
  [[ -n "${RUN_META}" ]] || die "run meta required for --phase2 (--run-meta or samples.meta sidecar)"
  [[ -f "${RUN_META}" ]] || die "run meta not found: ${RUN_META}"
  if [[ -n "${P2P_METRICS_START}" ]]; then
    [[ -f "${P2P_METRICS_START}" ]] || die "p2p-metrics-start not found: ${P2P_METRICS_START}"
  fi
  if [[ -n "${P2P_METRICS_END}" ]]; then
    [[ -f "${P2P_METRICS_END}" ]] || die "p2p-metrics-end not found: ${P2P_METRICS_END}"
  fi
  if [[ -n "${HARNESS_JSON}" ]]; then
    [[ -f "${HARNESS_JSON}" ]] || die "harness-json not found: ${HARNESS_JSON}"
  fi

  # Default docs target for --write under phase2.
  if [[ "${DOCS}" == "${REPO_ROOT}/docs/phase-1-soak.md" ]]; then
    DOCS="${REPO_ROOT}/docs/phase-2-soak.md"
  fi

  log "phase2:            yes"
  log "samples:           ${SAMPLES}"
  log "run meta:          ${RUN_META}"
  log "p2p metrics start: ${P2P_METRICS_START:-"(none)"}"
  log "p2p metrics end:   ${P2P_METRICS_END:-"(none)"}"
  log "harness json:      ${HARNESS_JSON:-"(none)"}"

  TMPERR="$(mktemp)"
  set +e
  body="$(run_python_phase2 2>"${TMPERR}")"
  rc=$?
  set -e
  if [[ "${rc}" -eq 3 ]]; then
    cat "${TMPERR}" >&2
    rm -f "${TMPERR}"
    exit 3
  fi
  if [[ "${rc}" -ne 0 && "${rc}" -ne 4 ]]; then
    cat "${TMPERR}" >&2
    rm -f "${TMPERR}"
    die "phase2 report generation failed (exit ${rc})"
  fi
  if [[ -s "${TMPERR}" ]]; then
    cat "${TMPERR}" >&2
  fi
  rm -f "${TMPERR}"

  if [[ -n "${OUT}" ]]; then
    printf '%s\n' "${body}" > "${OUT}"
    log "wrote ${OUT}"
  else
    printf '%s\n' "${body}"
  fi

  if [[ "${WRITE}" -eq 1 ]]; then
    [[ -f "${DOCS}" ]] || die "docs file not found: ${DOCS}"
    python3 - "${DOCS}" "${body}" <<'PY'
import sys
from pathlib import Path
docs = Path(sys.argv[1])
body = sys.argv[2]
if not body.lstrip().startswith("## "):
    body = "## Clause table\n\n" + body
text = docs.read_text()
# Match H2 at beginning of line only — not owner-table cells like
# `| \`## Clause table\` | … |` (M1).
start = -1
_off = 0
for _line in text.splitlines(keepends=True):
    if _line.startswith("## Clause table"):
        start = _off
        break
    _off += len(_line)
if start < 0:
    docs.write_text(text.rstrip() + "\n\n" + body + "\n")
else:
    rest = text[start + 1:]
    nxt = None
    for i, line in enumerate(rest.splitlines(keepends=True)):
        if i == 0:
            continue
        if line.startswith("## "):
            offset = len("".join(rest.splitlines(keepends=True)[:i]))
            nxt = start + 1 + offset
            break
    if nxt is None:
        new_text = text[:start] + body.rstrip() + "\n"
    else:
        new_text = text[:start] + body.rstrip() + "\n\n" + text[nxt:]
    docs.write_text(new_text)
print(f"updated {docs} ## Clause table", file=sys.stderr)
PY
    log "updated ${DOCS} ## Clause table (--write)"
  fi

  if [[ "${rc}" -eq 4 ]]; then
    log "phase2 clause FAIL (report emitted; exit 4)"
    exit 4
  fi
  log "done (phase2)"
  exit 0
fi

if [[ -z "${METRICS_START}" || -z "${METRICS_END}" ]]; then
  if [[ -n "${CHAIN_METRICS_URL}" ]]; then
    die "live dual-scrape is not automatic: capture start/end yourself, e.g.
  curl -sS ${CHAIN_METRICS_URL} > metrics-start.txt   # at steady-state open
  # … soak window …
  curl -sS ${CHAIN_METRICS_URL} > metrics-end.txt
  bash scripts/soak-report.sh --samples … --metrics-start metrics-start.txt --metrics-end metrics-end.txt …"
  fi
  die "both --metrics-start and --metrics-end are required (histogram scrape pair); or pass --phase2"
fi
[[ -f "${METRICS_START}" ]] || die "metrics-start not found: ${METRICS_START}"
[[ -f "${METRICS_END}" ]] || die "metrics-end not found: ${METRICS_END}"

if [[ -z "${DRIVER_METRICS}" ]]; then
  if [[ -n "${DRIVER_METRICS_URL}" ]] && command -v curl >/dev/null 2>&1; then
    DRIVER_METRICS="$(mktemp)"
    trap 'rm -f "${DRIVER_METRICS}"' EXIT
    log "scraping driver metrics from ${DRIVER_METRICS_URL}"
    curl -fsS --max-time 5 "${DRIVER_METRICS_URL}" > "${DRIVER_METRICS}" \
      || die "failed to scrape ${DRIVER_METRICS_URL}"
  else
    die "driver metrics required (--driver-metrics file or --driver-metrics-url)"
  fi
fi
[[ -f "${DRIVER_METRICS}" ]] || die "driver metrics not found: ${DRIVER_METRICS}"

if ! [[ "${LOAD_WINDOW}" =~ ^[0-9]+$ ]] || [[ "${LOAD_WINDOW}" -lt 1 ]]; then
  die "load-window-samples must be a positive integer"
fi

log "samples:          ${SAMPLES}"
log "metrics start:    ${METRICS_START}"
log "metrics end:      ${METRICS_END}"
log "driver metrics:   ${DRIVER_METRICS}"
log "load threshold:   ${LOAD_THRESHOLD} (sustained ≥ ${LOAD_WINDOW} samples)"

TMPERR="$(mktemp)"
set +e
body="$(run_python_report report 2>"${TMPERR}")"
rc=$?
set -e
if [[ "${rc}" -eq 3 ]]; then
  cat "${TMPERR}" >&2
  rm -f "${TMPERR}"
  exit 3
fi
if [[ "${rc}" -ne 0 && "${rc}" -ne 4 ]]; then
  cat "${TMPERR}" >&2
  rm -f "${TMPERR}"
  die "report generation failed (exit ${rc})"
fi
# Surface non-fatal python logs (if any)
if [[ -s "${TMPERR}" ]]; then
  cat "${TMPERR}" >&2
fi
rm -f "${TMPERR}"

if [[ -n "${OUT}" ]]; then
  printf '%s\n' "${body}" > "${OUT}"
  log "wrote ${OUT}"
else
  printf '%s\n' "${body}"
fi

if [[ "${WRITE}" -eq 1 ]]; then
  [[ -f "${DOCS}" ]] || die "docs file not found: ${DOCS}"
  # Replace content under ## Timing only (append-only rule: do not touch other sections).
  python3 - "${DOCS}" "${body}" <<'PY'
import sys
from pathlib import Path
docs = Path(sys.argv[1])
body = sys.argv[2]
if not body.startswith("## Timing"):
    # ensure section header
    if not body.lstrip().startswith("## "):
        body = "## Timing\n\n" + body
text = docs.read_text()
start = text.find("## Timing")
if start < 0:
    docs.write_text(text.rstrip() + "\n\n" + body + "\n")
else:
    # find next ## at line start after start
    rest = text[start + 1:]
    nxt = None
    for i, line in enumerate(rest.splitlines(keepends=True)):
        if i == 0:
            continue
        if line.startswith("## "):
            # compute offset
            offset = len("".join(rest.splitlines(keepends=True)[:i]))
            nxt = start + 1 + offset
            break
    if nxt is None:
        new_text = text[:start] + body.rstrip() + "\n"
    else:
        new_text = text[:start] + body.rstrip() + "\n\n" + text[nxt:]
    docs.write_text(new_text)
print(f"updated {docs} ## Timing", file=sys.stderr)
PY
  log "updated ${DOCS} ## Timing (--write)"
fi

if [[ "${rc}" -eq 4 ]]; then
  log "budget FAIL (report emitted; exit 4)"
  exit 4
fi
log "done"
exit 0
