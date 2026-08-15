#!/usr/bin/env bash
# scripts/s0-ab-baseline.sh — S0-B-19 / E0.7 §9.0 A/B baseline scrape + record
#
# Fail-closed. Loopback-only HTTP. No secrets. Not wired into make ci / make lint.
# Does not bring stacks up. --wait is explicit (never implicit, never 1 h by default).
#
# Usage:
#   bash scripts/s0-ab-baseline.sh --self-test
#   bash scripts/s0-ab-baseline.sh --check-bins
#   bash scripts/s0-ab-baseline.sh --scrape --label T0 --raw-dir DIR
#   bash scripts/s0-ab-baseline.sh --wait SECONDS
#   bash scripts/s0-ab-baseline.sh --record --t0-dir DIR --t1-dir DIR --out FILE
#
# --scrape writes one raw file per endpoint plus parsed.tsv + endpoints.txt.
# --record emits the three family tables with absolute numbers and the T0 vs T1
# dangerous-case verdict (family 3 nonzero while families 1–2 stay zero).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

MODE=""
LABEL=""
RAW_DIR=""
T0_DIR=""
T1_DIR=""
OUT=""
WAIT_SECS=""
ALLOW_MISSING_DEVNET=0
INVOKED="bash scripts/s0-ab-baseline.sh $*"

SIX_BINS=(cc-chain cc-p2p cc-attestation cc-engine cc-beacon-api cc-storage)
SIX_ENDPOINTS=(
  127.0.0.1:9101
  127.0.0.1:9102
  127.0.0.1:9103
  127.0.0.1:9104
  127.0.0.1:9105
  127.0.0.1:9106
)
DEVNET_ENDPOINTS=(
  127.0.0.1:19102
  127.0.0.1:19112
  127.0.0.1:19122
)

usage() {
  sed -n '8,16p' "$0" | sed 's/^# \{0,1\}//'
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --self-test) MODE="self-test"; shift ;;
    --check-bins) MODE="check-bins"; shift ;;
    --scrape) MODE="scrape"; shift ;;
    --wait)
      [[ $# -ge 2 ]] || die "--wait needs a positive integer (seconds)"
      MODE="wait"
      WAIT_SECS="$2"
      shift 2
      ;;
    --record) MODE="record"; shift ;;
    --label)
      [[ $# -ge 2 ]] || die "--label needs a value"
      LABEL="$2"
      shift 2
      ;;
    --raw-dir)
      [[ $# -ge 2 ]] || die "--raw-dir needs a path"
      RAW_DIR="$2"
      shift 2
      ;;
    --t0-dir)
      [[ $# -ge 2 ]] || die "--t0-dir needs a path"
      T0_DIR="$2"
      shift 2
      ;;
    --t1-dir)
      [[ $# -ge 2 ]] || die "--t1-dir needs a path"
      T1_DIR="$2"
      shift 2
      ;;
    --out)
      [[ $# -ge 2 ]] || die "--out needs a path"
      OUT="$2"
      shift 2
      ;;
    --allow-missing-devnet) ALLOW_MISSING_DEVNET=1; shift ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "${MODE}" ]] || die "specify --self-test, --check-bins, --scrape, --wait, or --record"

need() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

assert_loopback_host() {
  local host="$1"
  case "${host}" in
    127.0.0.1|localhost) return 0 ;;
    *) die "refusing non-loopback scrape host: ${host}" ;;
  esac
}

check_bins() {
  local missing=0 b
  for b in "${SIX_BINS[@]}"; do
    if [[ -x "${ROOT}/target/debug/${b}" ]]; then
      echo "ok: target/debug/${b}"
    else
      echo "missing: target/debug/${b}" >&2
      missing=1
    fi
  done
  [[ "${missing}" -eq 0 ]] || die "make build did not produce all six binaries"
}

wait_secs() {
  [[ "${WAIT_SECS}" =~ ^[0-9]+$ ]] || die "--wait needs a non-negative integer, got: ${WAIT_SECS}"
  [[ "${WAIT_SECS}" -ge 1 ]] || die "--wait must be >= 1 (explicit; no implicit hour)"
  local start now
  start="$(date +%s)"
  echo "waiting ${WAIT_SECS}s (start_unix=${start})" >&2
  sleep "${WAIT_SECS}"
  now="$(date +%s)"
  echo "waited $((now - start))s (end_unix=${now})" >&2
  if (( now - start < WAIT_SECS )); then
    die "wait returned early: elapsed=$((now - start)) required=${WAIT_SECS}"
  fi
}

scrape_one() {
  local ep="$1"
  local dest="$2"
  local host="${ep%%:*}"
  assert_loopback_host "${host}"
  local url="http://${ep}/metrics"
  if ! curl -fsS --max-time 8 --proto '=http' --noproxy '*' "${url}" >"${dest}.tmp"; then
    rm -f "${dest}.tmp"
    return 1
  fi
  if ! grep -q '^cc_' "${dest}.tmp"; then
    rm -f "${dest}.tmp"
    echo "error: ${url} answered but has no cc_* series" >&2
    return 1
  fi
  mv "${dest}.tmp" "${dest}"
  return 0
}

scrape_all() {
  [[ -n "${RAW_DIR}" ]] || die "--scrape needs --raw-dir"
  [[ -n "${LABEL}" ]] || die "--scrape needs --label"
  need curl
  need python3
  mkdir -p "${RAW_DIR}"
  local ep fname missing=()
  : >"${RAW_DIR}/endpoints.txt"
  {
    echo "label=${LABEL}"
    echo "scraped_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "scraped_unix=$(date +%s)"
    echo "command=${INVOKED}"
  } >"${RAW_DIR}/meta.txt"

  for ep in "${SIX_ENDPOINTS[@]}"; do
    fname="${RAW_DIR}/metrics-${ep##*:}.txt"
    if scrape_one "${ep}" "${fname}"; then
      echo "${ep} ok ${fname}" | tee -a "${RAW_DIR}/endpoints.txt"
    else
      echo "${ep} FAIL" | tee -a "${RAW_DIR}/endpoints.txt" >&2
      missing+=("${ep}")
    fi
  done
  [[ ${#missing[@]} -eq 0 ]] || die "six-service scrape failed: ${missing[*]}"

  missing=()
  for ep in "${DEVNET_ENDPOINTS[@]}"; do
    fname="${RAW_DIR}/metrics-${ep##*:}.txt"
    if scrape_one "${ep}" "${fname}"; then
      echo "${ep} ok ${fname}" | tee -a "${RAW_DIR}/endpoints.txt"
    else
      echo "${ep} FAIL" | tee -a "${RAW_DIR}/endpoints.txt" >&2
      missing+=("${ep}")
    fi
  done
  if [[ ${#missing[@]} -ne 0 ]]; then
    if [[ "${ALLOW_MISSING_DEVNET}" -eq 1 ]]; then
      echo "warn: self-devnet scrape missing: ${missing[*]}" >&2
    else
      die "self-devnet scrape failed: ${missing[*]}"
    fi
  fi

  parse_py parse_dir "${RAW_DIR}" >"${RAW_DIR}/parsed.tsv"
  echo "wrote ${RAW_DIR}/parsed.tsv"
}

record_pair() {
  [[ -n "${T0_DIR}" && -d "${T0_DIR}" ]] || die "--record needs --t0-dir"
  [[ -n "${T1_DIR}" && -d "${T1_DIR}" ]] || die "--record needs --t1-dir"
  [[ -n "${OUT}" ]] || die "--record needs --out"
  need python3
  mkdir -p "$(dirname "${OUT}")"
  parse_py record "${T0_DIR}" "${T1_DIR}" >"${OUT}.tmp"
  mv "${OUT}.tmp" "${OUT}"
  echo "wrote ${OUT}"
}

self_test() {
  need python3
  local td
  td="$(mktemp -d "${TMPDIR:-/tmp}/s0b19.XXXXXX")"
  # Trap body must not reference a function-local (set -u on EXIT).
  S0B19_TMP="${td}"
  trap 'rm -rf "${S0B19_TMP:-}"' EXIT

  mkdir -p "${td}/t0" "${td}/t1" "${td}/danger/t0" "${td}/danger/t1"

  cat >"${td}/t0/metrics-9101.txt" <<'EOF'
# TYPE cc_chain_import counter
cc_chain_import_total{result="imported"} 1
cc_chain_import_total{result="invalid"} 0
# TYPE cc_chain_head_lag_slots gauge
cc_chain_head_lag_slots 2
# TYPE cc_chain_import_rejected_backpressure counter
cc_chain_import_rejected_backpressure_total 0
# TYPE cc_chain_subscribers gauge
cc_chain_subscribers 3
# TYPE cc_grpc_requests counter
cc_grpc_requests_total{service="chain",method="/eth.chain.v1.ChainService/SubscribeEvents",code="8"} 0
EOF
  cat >"${td}/t0/metrics-9102.txt" <<'EOF'
# TYPE cc_p2p_head_lag_slots histogram
cc_p2p_head_lag_slots_bucket{le="0.0"} 1
cc_p2p_head_lag_slots_bucket{le="1.0"} 4
cc_p2p_head_lag_slots_bucket{le="+Inf"} 4
cc_p2p_head_lag_slots_count 4
EOF
  cat >"${td}/t0/metrics-19112.txt" <<'EOF'
# TYPE cc_p2p_head_lag_slots histogram
cc_p2p_head_lag_slots_bucket{le="1.0"} 2
cc_p2p_head_lag_slots_bucket{le="+Inf"} 2
EOF
  cp "${td}/t0/metrics-9101.txt" "${td}/t0/metrics-9103.txt"
  : >"${td}/t0/metrics-9104.txt"
  : >"${td}/t0/metrics-9105.txt"
  : >"${td}/t0/metrics-9106.txt"
  : >"${td}/t0/metrics-19102.txt"
  : >"${td}/t0/metrics-19122.txt"

  # T1: family 1 and 2 move (not the dangerous case).
  sed 's/imported"} 1/imported"} 9/' "${td}/t0/metrics-9101.txt" \
    | sed 's/cc_chain_head_lag_slots 2/cc_chain_head_lag_slots 0/' \
    >"${td}/t1/metrics-9101.txt"
  sed 's/le="1.0"} 4/le="1.0"} 10/' "${td}/t0/metrics-9102.txt" >"${td}/t1/metrics-9102.txt"
  cp "${td}/t0/metrics-19112.txt" "${td}/t1/metrics-19112.txt"
  cp "${td}/t0/metrics-9103.txt" "${td}/t1/metrics-9103.txt"
  : >"${td}/t1/metrics-9104.txt"
  : >"${td}/t1/metrics-9105.txt"
  : >"${td}/t1/metrics-9106.txt"
  : >"${td}/t1/metrics-19102.txt"
  : >"${td}/t1/metrics-19122.txt"

  local out
  out="$(parse_py record "${td}/t0" "${td}/t1")"
  echo "${out}" | grep -q 'dangerous_case: no' || {
    echo "${out}" >&2
    die "self-test: expected dangerous_case: no when families 1–2 move"
  }
  echo "${out}" | grep -q 'family1_nonzero_delta_series: 1' || {
    echo "${out}" >&2
    die "self-test: family 1 delta not counted"
  }

  # Dangerous case: only family 3 moves.
  cp "${td}/t0/metrics-9101.txt" "${td}/danger/t0/metrics-9101.txt"
  cp "${td}/t0/metrics-9102.txt" "${td}/danger/t0/metrics-9102.txt"
  cp "${td}/t0/metrics-19112.txt" "${td}/danger/t0/metrics-19112.txt"
  : >"${td}/danger/t0/metrics-9103.txt"
  : >"${td}/danger/t0/metrics-9104.txt"
  : >"${td}/danger/t0/metrics-9105.txt"
  : >"${td}/danger/t0/metrics-9106.txt"
  : >"${td}/danger/t0/metrics-19102.txt"
  : >"${td}/danger/t0/metrics-19122.txt"
  sed 's/rejected_backpressure_total 0/rejected_backpressure_total 4/' \
    "${td}/t0/metrics-9101.txt" >"${td}/danger/t1/metrics-9101.txt"
  cp "${td}/t0/metrics-9102.txt" "${td}/danger/t1/metrics-9102.txt"
  cp "${td}/t0/metrics-19112.txt" "${td}/danger/t1/metrics-19112.txt"
  : >"${td}/danger/t1/metrics-9103.txt"
  : >"${td}/danger/t1/metrics-9104.txt"
  : >"${td}/danger/t1/metrics-9105.txt"
  : >"${td}/danger/t1/metrics-9106.txt"
  : >"${td}/danger/t1/metrics-19102.txt"
  : >"${td}/danger/t1/metrics-19122.txt"

  out="$(parse_py record "${td}/danger/t0" "${td}/danger/t1")"
  echo "${out}" | grep -q 'dangerous_case: yes' || {
    echo "${out}" >&2
    die "self-test: expected dangerous_case: yes when only family 3 moves"
  }
  echo "ok: self-test"
}

# python3 - parse_dir DIR | record T0 T1
parse_py() {
  python3 - "$@" <<'PY'
from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from pathlib import Path

SAMPLE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{[^}]*\})?\s+(?P<value>[-+0-9.eE]+)\s*$"
)

FAMILY1_NAME = re.compile(
    r"^(cc_chain_import_total|cc_chain_import_result)(\{|$)"
)
FAMILY2_P2P = re.compile(r"^cc_p2p_head_lag_slots_bucket(\{|$)")
FAMILY2_CHAIN = re.compile(r"^cc_chain_head_lag_slots(\{|$)")
# Family 3: overflow + subscriber termination surface. Exact names are quoted
# from the scrape; this only selects rows.
FAMILY3 = re.compile(
    r"("
    r"rejected_backpressure"
    r"|_dropped"
    r"|event_publish_dropped"
    r"|da_pending_dropped"
    r"|pending_engine_dropped"
    r"|fastpath_dropped"
    r"|cc_storage_[a-z0-9_]*_dropped"
    r"|cc_chain_subscribers(\{|$)"
    r"|terminated"
    r")"
)
GRPC_RE = re.compile(r"^cc_grpc_requests_total\{")
CODE8 = re.compile(r'(^|,)code="8"(,|})')

PORTS = {
    "9101": "six/chain",
    "9102": "six/p2p",
    "9103": "six/attestation",
    "9104": "six/engine",
    "9105": "six/beacon-api",
    "9106": "six/storage",
    "19102": "devnet/publisher",
    "19112": "devnet/node-a",
    "19122": "devnet/node-b",
}


def parse_file(path: Path) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if not path.is_file() or path.stat().st_size == 0:
        return out
    for raw in path.read_text(errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = SAMPLE_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        labels = m.group("labels") or ""
        series = f"{name}{labels}"
        out.append((series, m.group("value")))
    return out


def port_of(path: Path) -> str:
    # metrics-9101.txt
    stem = path.stem
    if stem.startswith("metrics-"):
        return stem.split("-", 1)[1]
    return stem


def classify(series: str) -> str | None:
    if FAMILY1_NAME.match(series):
        return "1"
    if FAMILY2_P2P.match(series) or FAMILY2_CHAIN.match(series):
        return "2"
    if GRPC_RE.match(series) and CODE8.search(series):
        return "3"
    if FAMILY3.search(series):
        return "3"
    return None


def load_dir(d: Path) -> list[tuple[str, str, str, str]]:
    """(family, endpoint, series, value)"""
    rows: list[tuple[str, str, str, str]] = []
    for p in sorted(d.glob("metrics-*.txt")):
        port = port_of(p)
        ep = f"127.0.0.1:{port}"
        role = PORTS.get(port, port)
        for series, value in parse_file(p):
            fam = classify(series)
            if fam is None:
                continue
            rows.append((fam, f"{ep} ({role})", series, value))
    return rows


def num(v: str) -> float:
    return float(v)


def fmt_num(v: float) -> str:
    if v.is_integer():
        return str(int(v))
    return repr(v)


def emit_tables(rows: list[tuple[str, str, str, str]], title: str) -> list[str]:
    lines = [f"== {title} ==", "family\tendpoint\tseries\tvalue"]
    for fam, ep, series, value in rows:
        lines.append(f"{fam}\t{ep}\t{series}\t{value}")
    if len(rows) == 0:
        lines.append("# (no matching series)")
    return lines


def record(t0: Path, t1: Path) -> str:
    a = load_dir(t0)
    b = load_dir(t1)
    key = lambda r: (r[0], r[1], r[2])
    map_a = {key(r): r[3] for r in a}
    map_b = {key(r): r[3] for r in b}
    keys = sorted(set(map_a) | set(map_b))
    lines: list[str] = []
    lines.append("# S0-B-19 / E0.7 parsed family tables (absolute numbers)")
    lines.append("# Topology A == Topology B == this commit (S0: no structural split).")
    lines.append("# Same-commit A/B pair is T0 vs T>=1h. T>=1h is the S0 baseline.")
    lines.append("")
    lines.extend(emit_tables(a, "T0 snapshot (same-commit topology A)"))
    lines.append("")
    lines.extend(emit_tables(b, "T>=1h snapshot (same-commit topology B; S0 baseline)"))
    lines.append("")
    lines.append("== T0 vs T>=1h diff (same-commit A/B) ==")
    lines.append("family\tendpoint\tseries\tT0\tT1h\tdelta")
    nonzero = defaultdict(int)
    for fam, ep, series in keys:
        va = map_a.get((fam, ep, series))
        vb = map_b.get((fam, ep, series))
        fa = num(va) if va is not None else 0.0
        fb = num(vb) if vb is not None else 0.0
        delta = fb - fa
        sa = va if va is not None else "ABSENT"
        sb = vb if vb is not None else "ABSENT"
        lines.append(f"{fam}\t{ep}\t{series}\t{sa}\t{sb}\t{fmt_num(delta)}")
        if abs(delta) > 0:
            nonzero[fam] += 1
    n1, n2, n3 = nonzero["1"], nonzero["2"], nonzero["3"]
    dangerous = n3 > 0 and n1 == 0 and n2 == 0
    lines.append("")
    lines.append(f"family1_nonzero_delta_series: {n1}")
    lines.append(f"family2_nonzero_delta_series: {n2}")
    lines.append(f"family3_nonzero_delta_series: {n3}")
    lines.append(f"dangerous_case: {'yes' if dangerous else 'no'}")
    if dangerous:
        lines.append(
            "DANGEROUS CASE: family 3 moved while families 1–2 did not. "
            "Behaviour unchanged at test load; contract changed. Stage blocker."
        )
    else:
        lines.append(
            "dangerous-case rule did not fire "
            "(requires family-3 nonzero AND families 1–2 zero)."
        )
    lines.append("")
    # Exact series names observed (so later stages quote what existed at S0).
    names = sorted({r[2] for r in a + b})
    lines.append("== exact series names observed ==")
    for n in names:
        lines.append(n)
    if not names:
        lines.append("# (none)")
    return "\n".join(lines) + "\n"


def main() -> int:
    if len(sys.argv) < 2:
        print("error: parser needs a subcommand", file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    if cmd == "parse_dir":
        d = Path(sys.argv[2])
        rows = load_dir(d)
        sys.stdout.write("\n".join(emit_tables(rows, f"parsed {d}")) + "\n")
        return 0
    if cmd == "record":
        sys.stdout.write(record(Path(sys.argv[2]), Path(sys.argv[3])))
        return 0
    print(f"error: unknown parser command {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
PY
}

case "${MODE}" in
  self-test) self_test ;;
  check-bins) check_bins ;;
  scrape) scrape_all ;;
  wait) wait_secs ;;
  record) record_pair ;;
  *) die "unknown mode: ${MODE}" ;;
esac
