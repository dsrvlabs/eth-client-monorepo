#!/usr/bin/env bash
# scripts/storage-plateau.sh — CC-4D /5 plateau measurement (Architecture §10.4).
#
# Computes from metrics (or a samples file):
#   1. 24 h slope of cc_storage_bytes_total as a percentage of the plateau
#   2. prune-bytes ÷ written-bytes ratio over the equivalent interval
#
# The horizon crossing is *in the data* (R-15): on a run shorter than the
# horizon the script prints an explicit HORIZON_NOT_CROSSED rather than a number.
#
# It measures; it is not the run (the plateau run itself is CC-4Cc).
#
# Usage:
#   bash scripts/storage-plateau.sh \
#     [--metrics-url URL] \
#     [--samples PATH] \
#     [--horizon-hours N] \
#     [--self-test]
#
# Samples file format (CSV, header required):
#   ts_unix,bytes_total,pruned_bytes,written_bytes
# One row per scrape; ts_unix is epoch seconds. Series may be summed across
# class labels before write (operator responsibility).
#
# Live scrape (no --samples): two scrapes of the metrics URL cannot span 24 h
# in one invocation, so the script always emits HORIZON_NOT_CROSSED for a
# single-shot live scrape unless --samples supplies a long series.
#
# Environment:
#   STORAGE_PLATEAU_METRICS_URL   default http://127.0.0.1:9106/metrics
#   STORAGE_PLATEAU_SAMPLES       optional samples CSV path
#   STORAGE_PLATEAU_HORIZON_HOURS default 24
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

METRICS_URL="${STORAGE_PLATEAU_METRICS_URL:-http://127.0.0.1:9106/metrics}"
SAMPLES="${STORAGE_PLATEAU_SAMPLES:-}"
HORIZON_HOURS="${STORAGE_PLATEAU_HORIZON_HOURS:-24}"
SELF_TEST=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --metrics-url) METRICS_URL="$2"; shift 2 ;;
    --samples)     SAMPLES="$2"; shift 2 ;;
    --horizon-hours) HORIZON_HOURS="$2"; shift 2 ;;
    --self-test)   SELF_TEST=1; shift ;;
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

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}
need python3

# Sum OpenMetrics counter/gauge values whose metric name starts with $1.
# Counters may appear as name_total; we match exact family prefix.
scrape_metric_sum() {
  local body="$1"
  local name="$2"
  BODY="${body}" NAME="${name}" python3 - <<'PY'
import os, re
name = os.environ["NAME"]
# Match name or name_total; ignore HELP/TYPE and exemplars.
pat = re.compile(
    r"^(?P<m>" + re.escape(name) + r"(?:_total)?)(?:\{[^}]*\})?\s+(?P<v>[-+0-9.eE]+)\s*$"
)
total = 0.0
found = False
for line in os.environ["BODY"].splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    m = pat.match(line)
    if not m:
        continue
    found = True
    total += float(m.group("v"))
print(f"{total if found else ''}")
PY
}

compute_from_samples() {
  local path="$1"
  local horizon_h="$2"
  SAMPLES_PATH="${path}" HORIZON_H="${horizon_h}" python3 - <<'PY'
import csv
import os
import sys

path = os.environ["SAMPLES_PATH"]
horizon_h = float(os.environ["HORIZON_H"])
horizon_s = horizon_h * 3600.0

rows = []
with open(path, newline="") as f:
    r = csv.DictReader(f)
    required = {"ts_unix", "bytes_total", "pruned_bytes", "written_bytes"}
    if not r.fieldnames or not required.issubset(set(r.fieldnames)):
        print(
            f"error: samples CSV must have columns {sorted(required)}; got {r.fieldnames}",
            file=sys.stderr,
        )
        sys.exit(2)
    for row in r:
        try:
            rows.append(
                {
                    "ts": float(row["ts_unix"]),
                    "bytes": float(row["bytes_total"]),
                    "pruned": float(row["pruned_bytes"]),
                    "written": float(row["written_bytes"]),
                }
            )
        except (KeyError, ValueError) as e:
            print(f"error: bad row {row!r}: {e}", file=sys.stderr)
            sys.exit(2)

if len(rows) < 2:
    print("status: HORIZON_NOT_CROSSED")
    print("reason: fewer than 2 samples")
    print("slope_pct_of_plateau: HORIZON_NOT_CROSSED")
    print("prune_written_ratio: HORIZON_NOT_CROSSED")
    sys.exit(0)

rows.sort(key=lambda x: x["ts"])
span = rows[-1]["ts"] - rows[0]["ts"]
if span + 1e-9 < horizon_s:
    print("status: HORIZON_NOT_CROSSED")
    print(f"reason: sample span {span:.0f}s < horizon {horizon_s:.0f}s ({horizon_h:g} h)")
    print("slope_pct_of_plateau: HORIZON_NOT_CROSSED")
    print("prune_written_ratio: HORIZON_NOT_CROSSED")
    print(f"sample_span_seconds: {span:.0f}")
    print(f"sample_count: {len(rows)}")
    sys.exit(0)

# Window: last `horizon_s` of the series (R-15: horizon crossing in the data).
t_end = rows[-1]["ts"]
t_start = t_end - horizon_s
window = [r for r in rows if r["ts"] >= t_start]
if len(window) < 2:
    # Degenerate: cluster of points only at the end.
    window = rows[-2:]

b0, b1 = window[0]["bytes"], window[-1]["bytes"]
plateau = max(abs(b1), abs(b0), 1.0)
# Slope over the window as % of plateau: (delta_bytes / plateau) * 100.
slope_pct = ((b1 - b0) / plateau) * 100.0

p0, p1 = window[0]["pruned"], window[-1]["pruned"]
w0, w1 = window[0]["written"], window[-1]["written"]
d_pruned = p1 - p0
d_written = w1 - w0
if d_written == 0:
    ratio_s = "n/a (written_delta=0)"
else:
    ratio_s = f"{d_pruned / d_written:.6f}"

print("status: OK")
print(f"horizon_hours: {horizon_h:g}")
print(f"window_start_unix: {window[0]['ts']:.0f}")
print(f"window_end_unix: {window[-1]['ts']:.0f}")
print(f"plateau_bytes: {plateau:.0f}")
print(f"slope_pct_of_plateau: {slope_pct:.6f}")
print(f"prune_bytes_delta: {d_pruned:.0f}")
print(f"written_bytes_delta: {d_written:.0f}")
print(f"prune_written_ratio: {ratio_s}")
print(f"sample_count_in_window: {len(window)}")
PY
}

self_test() {
  local tmp
  tmp="$(mktemp -t storage-plateau.XXXXXX)"
  # Short series → HORIZON_NOT_CROSSED
  cat >"${tmp}" <<'CSV'
ts_unix,bytes_total,pruned_bytes,written_bytes
1000,100,0,10
1900,110,0,20
CSV
  echo "==> self-test: short series"
  out="$(compute_from_samples "${tmp}" 24)"
  echo "${out}"
  echo "${out}" | grep -q "HORIZON_NOT_CROSSED" || {
    echo "error: expected HORIZON_NOT_CROSSED for short series" >&2
    rm -f "${tmp}"
    exit 1
  }

  # 25 h of samples, flat-ish plateau, prune ≈ written over last 24 h
  python3 - <<'PY' >"${tmp}"
import csv
print("ts_unix,bytes_total,pruned_bytes,written_bytes")
# 25 hours of hourly samples
base = 1_700_000_000
for i in range(26):
    ts = base + i * 3600
    # grow for 1 h then flat plateau at 1_000_000
    bytes_ = 1_000_000 if i >= 1 else 900_000
    # cumulative pruned/written advance in lockstep after hour 1
    pruned = max(0, (i - 1) * 1000)
    written = max(0, (i - 1) * 1000)
    print(f"{ts},{bytes_},{pruned},{written}")
PY
  echo "==> self-test: 25 h plateau"
  out="$(compute_from_samples "${tmp}" 24)"
  echo "${out}"
  echo "${out}" | grep -q "status: OK" || {
    echo "error: expected status OK for long series" >&2
    rm -f "${tmp}"
    exit 1
  }
  echo "${out}" | grep -q "slope_pct_of_plateau:" || {
    echo "error: missing slope" >&2
    rm -f "${tmp}"
    exit 1
  }
  echo "${out}" | grep -q "prune_written_ratio:" || {
    echo "error: missing ratio" >&2
    rm -f "${tmp}"
    exit 1
  }
  # slope should be ~0 on a flat plateau
  slope="$(echo "${out}" | awk -F': ' '/^slope_pct_of_plateau:/{print $2}')"
  python3 -c "s=float('${slope}'); assert abs(s) < 1.0, s"
  rm -f "${tmp}"
  echo "self-test: PASS"
}

if (( SELF_TEST == 1 )); then
  self_test
  exit 0
fi

if [[ -n "${SAMPLES}" ]]; then
  if [[ ! -f "${SAMPLES}" ]]; then
    echo "error: samples file not found: ${SAMPLES}" >&2
    exit 2
  fi
  compute_from_samples "${SAMPLES}" "${HORIZON_HOURS}"
  exit 0
fi

# Live single scrape cannot cross a 24 h horizon — be honest.
echo "==> live scrape ${METRICS_URL} (single shot cannot span horizon)"
body="$(curl -fsS --max-time 5 "${METRICS_URL}" 2>/dev/null || true)"
if [[ -z "${body}" ]]; then
  echo "status: HORIZON_NOT_CROSSED"
  echo "reason: metrics URL unreachable and no --samples file"
  echo "slope_pct_of_plateau: HORIZON_NOT_CROSSED"
  echo "prune_written_ratio: HORIZON_NOT_CROSSED"
  exit 0
fi

bytes="$(scrape_metric_sum "${body}" "cc_storage_bytes_total")"
pruned="$(scrape_metric_sum "${body}" "cc_storage_pruned_bytes")"
written="$(scrape_metric_sum "${body}" "cc_storage_written_bytes")"

echo "status: HORIZON_NOT_CROSSED"
echo "reason: single live scrape; supply --samples with ≥ ${HORIZON_HOURS}h span (R-15)"
echo "live_cc_storage_bytes_total: ${bytes:-n/a}"
echo "live_cc_storage_pruned_bytes: ${pruned:-n/a}"
echo "live_cc_storage_written_bytes: ${written:-n/a}"
echo "slope_pct_of_plateau: HORIZON_NOT_CROSSED"
echo "prune_written_ratio: HORIZON_NOT_CROSSED"
