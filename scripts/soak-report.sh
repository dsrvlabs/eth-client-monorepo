#!/usr/bin/env bash
# scripts/soak-report.sh — CC-1Ac soak report + R-1 load guard
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
# Usage:
#   bash scripts/soak-report.sh \
#     --samples soak-samples.csv \
#     --metrics-start chain-metrics-start.txt \
#     --metrics-end   chain-metrics-end.txt \
#     --driver-metrics driver-metrics.txt \
#     [--out report-timing.md] \
#     [--docs docs/phase-1-soak.md] [--write] \
#     [--load-threshold N] [--load-window-samples N]
#
# Live scrape (optional if snapshot files omitted):
#   --chain-metrics-url  http://127.0.0.1:9101/metrics
#   --driver-metrics-url http://127.0.0.1:9110/metrics
#
# Self-test (synthetic series; no live stack required):
#   bash scripts/soak-report.sh --self-test
#
# Environment:
#   SOAK_SAMPLES, SOAK_METRICS_START, SOAK_METRICS_END, SOAK_DRIVER_METRICS,
#   SOAK_LOAD_THRESHOLD (default 4.0), SOAK_LOAD_WINDOW_SAMPLES (default 5),
#   SOAK_REPORT_OUT
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
    --out) OUT="$2"; shift 2 ;;
    --docs) DOCS="$2"; shift 2 ;;
    --write) WRITE=1; shift ;;
    --load-threshold) LOAD_THRESHOLD="$2"; shift 2 ;;
    --load-window-samples) LOAD_WINDOW="$2"; shift 2 ;;
    --window-start) WINDOW_START="$2"; shift 2 ;;
    --window-end) WINDOW_END="$2"; shift 2 ;;
    --self-test) SELF_TEST=1; shift ;;
    -h|--help)
      sed -n '2,40p' "$0"
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
refuse() {
  # R-1 / catch-up refusals: non-zero, message on stderr, no report body.
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

  log "self-test PASSED (clean emit, spike refuse, catchup present/absent, provider guard)"
  exit 0
fi

# ── resolve inputs ──────────────────────────────────────────────────────────
[[ -n "${SAMPLES}" ]] || die "samples CSV required (--samples or SOAK_SAMPLES)"
[[ -f "${SAMPLES}" ]] || die "samples file not found: ${SAMPLES}"

if [[ -z "${METRICS_START}" || -z "${METRICS_END}" ]]; then
  if [[ -n "${CHAIN_METRICS_URL}" ]]; then
    die "live dual-scrape is not automatic: capture start/end yourself, e.g.
  curl -sS ${CHAIN_METRICS_URL} > metrics-start.txt   # at steady-state open
  # … soak window …
  curl -sS ${CHAIN_METRICS_URL} > metrics-end.txt
  bash scripts/soak-report.sh --samples … --metrics-start metrics-start.txt --metrics-end metrics-end.txt …"
  fi
  die "both --metrics-start and --metrics-end are required (histogram scrape pair)"
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
