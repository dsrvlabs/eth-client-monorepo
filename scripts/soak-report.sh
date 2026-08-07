#!/usr/bin/env bash
# scripts/soak-report.sh — CC-1Ac soak report + R-1 load guard + CC-29b Phase 2 clauses
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
#   bash scripts/soak-report.sh --phase2 \
#     --samples soak-samples.csv \
#     --run-meta soak-samples.meta \
#     --p2p-metrics-start p2p-metrics-start.txt \
#     --p2p-metrics-end   p2p-metrics-end.txt \
#     [--harness-json harness-results.json] \
#     [--out clause-table.md] \
#     [--docs docs/phase-2-soak.md] [--write]
#
# Live scrape (optional if snapshot files omitted):
#   --chain-metrics-url  http://127.0.0.1:9101/metrics
#   --driver-metrics-url http://127.0.0.1:9110/metrics
#
# Self-test (synthetic series; no live stack required; Phase 1 + Phase 2):
#   bash scripts/soak-report.sh --self-test
#
# Environment:
#   SOAK_SAMPLES, SOAK_METRICS_START, SOAK_METRICS_END, SOAK_DRIVER_METRICS,
#   SOAK_LOAD_THRESHOLD (default 4.0), SOAK_LOAD_WINDOW_SAMPLES (default 5),
#   SOAK_REPORT_OUT, SOAK_P2P_METRICS_START, SOAK_P2P_METRICS_END,
#   SOAK_RUN_META, SOAK_HARNESS_JSON, SOAK_PHASE2
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
P2P_METRICS_START="${SOAK_P2P_METRICS_START:-}"
P2P_METRICS_END="${SOAK_P2P_METRICS_END:-}"
RUN_META="${SOAK_RUN_META:-}"
HARNESS_JSON="${SOAK_HARNESS_JSON:-}"
LOAD_THRESHOLD="${SOAK_LOAD_THRESHOLD:-4.0}"
LOAD_WINDOW="${SOAK_LOAD_WINDOW_SAMPLES:-5}"
# Optional operator-supplied window bounds (unix seconds). Default: catchup → last sample.
WINDOW_START="${SOAK_WINDOW_START:-}"
WINDOW_END="${SOAK_WINDOW_END:-}"

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
    --phase2) PHASE2=1; shift ;;
    --out) OUT="$2"; shift 2 ;;
    --docs) DOCS="$2"; shift 2 ;;
    --write) WRITE=1; shift ;;
    --load-threshold) LOAD_THRESHOLD="$2"; shift 2 ;;
    --load-window-samples) LOAD_WINDOW="$2"; shift 2 ;;
    --window-start) WINDOW_START="$2"; shift 2 ;;
    --window-end) WINDOW_END="$2"; shift 2 ;;
    --self-test) SELF_TEST=1; shift ;;
    -h|--help)
      sed -n '2,55p' "$0"
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

  log "self-test PASSED (phase1: clean/spike/catchup/provider; phase2: table/dip/stable/NO_DATA)"
  exit 0
fi

# ── resolve inputs ──────────────────────────────────────────────────────────
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
start = text.find("## Clause table")
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
