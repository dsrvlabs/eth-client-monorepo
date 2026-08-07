#!/usr/bin/env bash
# scripts/soak-sampler.sh — CC-1Ac / CC-29b per-slot soak sampler
#
# Wakes once per slot and writes one CSV row carrying:
#   timestamp, slot, local head root/slot (chain GetHead),
#   reference provider head root/slot, agreement flag,
#   chain process RSS (KiB), machine 1-minute load average,
#   and (CC-29b) the five Phase 2 series numbers:
#     p2p_peers, p2p_peers_custody, head_lag_slots, rss_kib, load1
#
# When --p2p-metrics-url is set (or default reachable), scrapes
# cc_p2p_peers / cc_p2p_peers_custody_compatible each tick. head_lag_slots is
# derived from max(0, ref_slot − local_slot) so the series is recoverable
# offline; the process histogram (cc_p2p_head_lag_slots) is the clause-3
# counting source in soak-report.sh.
#
# Peer-set-stable boundary (CC-29b): once p2p_peers ≥ --peer-stable-min-peers
# and p2p_peers_custody ≥ --peer-stable-min-custody for --peer-stable-samples
# consecutive ticks, records peer_set_stable_unix in the .meta sidecar. That
# timestamp opens the Phase 2 steady-state window and is unrecoverable from
# histograms afterwards.
#
# Independent-provider guard (Clause 2/2): refuses to start if the reference
# provider base equals the driver's block-feed provider (normalized).
#
# Never invoked by cargo build or cargo nextest. Operators run this for the
# soak window (CC-1Ad / CC-29c) and for short dry runs of the measurement rig.
#
# Usage:
#   bash scripts/soak-sampler.sh \
#     --driver-provider https://provider-a.example \
#     --ref-provider    https://provider-b.example \
#     --out             soak-samples.csv \
#     [--slots N | --duration SECS] \
#     [--chain-grpc HOST:PORT] \
#     [--chain-pid PID | --chain-container NAME] \
#     [--seconds-per-slot SECS] \
#     [--p2p-metrics-url URL] \
#     [--peer-stable-min-peers N] [--peer-stable-min-custody N] \
#     [--peer-stable-samples N]
#
# Environment (flags override):
#   SOAK_DRIVER_PROVIDER, SOAK_REF_PROVIDER, SOAK_OUT,
#   SOAK_CHAIN_GRPC (default 127.0.0.1:9001),
#   SOAK_CHAIN_PID, SOAK_CHAIN_CONTAINER (default chain),
#   SOAK_SECONDS_PER_SLOT (default 12), SOAK_SLOTS, SOAK_DURATION,
#   SOAK_P2P_METRICS_URL (default http://127.0.0.1:9102/metrics),
#   SOAK_PEER_STABLE_MIN_PEERS (default 25),
#   SOAK_PEER_STABLE_MIN_CUSTODY (default 8),
#   SOAK_PEER_STABLE_SAMPLES (default 5)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DRIVER_PROVIDER="${SOAK_DRIVER_PROVIDER:-}"
REF_PROVIDER="${SOAK_REF_PROVIDER:-}"
OUT="${SOAK_OUT:-}"
CHAIN_GRPC="${SOAK_CHAIN_GRPC:-127.0.0.1:9001}"
CHAIN_PID="${SOAK_CHAIN_PID:-}"
CHAIN_CONTAINER="${SOAK_CHAIN_CONTAINER:-chain}"
SECONDS_PER_SLOT="${SOAK_SECONDS_PER_SLOT:-12}"
SLOTS="${SOAK_SLOTS:-}"
DURATION="${SOAK_DURATION:-}"
P2P_METRICS_URL="${SOAK_P2P_METRICS_URL:-http://127.0.0.1:9102/metrics}"
PEER_STABLE_MIN_PEERS="${SOAK_PEER_STABLE_MIN_PEERS:-25}"
PEER_STABLE_MIN_CUSTODY="${SOAK_PEER_STABLE_MIN_CUSTODY:-8}"
PEER_STABLE_SAMPLES="${SOAK_PEER_STABLE_SAMPLES:-5}"
# 0 = run until SIGINT/SIGTERM
MAX_SAMPLES=0

UA="cc-soak-sampler/0.1 (eth-client-monorepo CC-1Ac/CC-29b)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --driver-provider) DRIVER_PROVIDER="$2"; shift 2 ;;
    --ref-provider)    REF_PROVIDER="$2"; shift 2 ;;
    --out)             OUT="$2"; shift 2 ;;
    --chain-grpc)      CHAIN_GRPC="$2"; shift 2 ;;
    --chain-pid)       CHAIN_PID="$2"; shift 2 ;;
    --chain-container) CHAIN_CONTAINER="$2"; shift 2 ;;
    --seconds-per-slot) SECONDS_PER_SLOT="$2"; shift 2 ;;
    --slots)           SLOTS="$2"; shift 2 ;;
    --duration)        DURATION="$2"; shift 2 ;;
    --p2p-metrics-url) P2P_METRICS_URL="$2"; shift 2 ;;
    --peer-stable-min-peers) PEER_STABLE_MIN_PEERS="$2"; shift 2 ;;
    --peer-stable-min-custody) PEER_STABLE_MIN_CUSTODY="$2"; shift 2 ;;
    --peer-stable-samples) PEER_STABLE_SAMPLES="$2"; shift 2 ;;
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

log() { echo "soak-sampler: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }

for tool in curl python3; do
  command -v "${tool}" >/dev/null 2>&1 || die "required tool not found: ${tool}"
done

[[ -n "${DRIVER_PROVIDER}" ]] || die "driver provider required (--driver-provider or SOAK_DRIVER_PROVIDER)"
[[ -n "${REF_PROVIDER}" ]]    || die "reference provider required (--ref-provider or SOAK_REF_PROVIDER)"
[[ -n "${OUT}" ]]             || die "output CSV path required (--out or SOAK_OUT)"

if ! [[ "${SECONDS_PER_SLOT}" =~ ^[0-9]+$ ]] || [[ "${SECONDS_PER_SLOT}" -lt 1 ]]; then
  die "seconds-per-slot must be a positive integer, got: ${SECONDS_PER_SLOT}"
fi

if [[ -n "${SLOTS}" && -n "${DURATION}" ]]; then
  die "pass only one of --slots or --duration"
fi
if [[ -n "${SLOTS}" ]]; then
  [[ "${SLOTS}" =~ ^[0-9]+$ && "${SLOTS}" -ge 1 ]] || die "slots must be a positive integer"
  MAX_SAMPLES="${SLOTS}"
elif [[ -n "${DURATION}" ]]; then
  [[ "${DURATION}" =~ ^[0-9]+$ && "${DURATION}" -ge 1 ]] || die "duration must be a positive integer (seconds)"
  MAX_SAMPLES=$(( (DURATION + SECONDS_PER_SLOT - 1) / SECONDS_PER_SLOT ))
fi

if ! [[ "${PEER_STABLE_MIN_PEERS}" =~ ^[0-9]+$ ]]; then
  die "peer-stable-min-peers must be a non-negative integer"
fi
if ! [[ "${PEER_STABLE_MIN_CUSTODY}" =~ ^[0-9]+$ ]]; then
  die "peer-stable-min-custody must be a non-negative integer"
fi
if ! [[ "${PEER_STABLE_SAMPLES}" =~ ^[0-9]+$ ]] || [[ "${PEER_STABLE_SAMPLES}" -lt 1 ]]; then
  die "peer-stable-samples must be a positive integer"
fi

# ── Clause 2/2 independent-provider guard ───────────────────────────────────
# Normalize: scheme+host+path, strip trailing slash, lowercase host, drop
# default ports. Refuse when the two bases collapse to the same origin/path.
normalize_provider() {
  python3 -c '
import sys
from urllib.parse import urlsplit, urlunsplit
raw = sys.argv[1].strip()
if "://" not in raw:
    raw = "https://" + raw
parts = urlsplit(raw)
host = (parts.hostname or "").lower()
if not host:
    print(raw.rstrip("/").lower())
    raise SystemExit(0)
port = parts.port
netloc = host
if port and not ((parts.scheme == "https" and port == 443) or (parts.scheme == "http" and port == 80)):
    netloc = f"{host}:{port}"
path = parts.path.rstrip("/") or ""
print(urlunsplit((parts.scheme.lower(), netloc, path, "", "")))
' "$1"
}

DRIVER_NORM="$(normalize_provider "${DRIVER_PROVIDER}")"
REF_NORM="$(normalize_provider "${REF_PROVIDER}")"

if [[ "${DRIVER_NORM}" == "${REF_NORM}" ]]; then
  cat >&2 <<EOF
error: independent-provider guard refused start (Clause 2/2)
  driver provider:    ${DRIVER_PROVIDER}
  reference provider: ${REF_PROVIDER}
  normalized both:    ${DRIVER_NORM}
The reference must be a different provider than the one the driver polls for
blocks. Sampling the driver's own feed only proves we imported what we were
handed; the check requires an independently computed head.
EOF
  exit 1
fi

log "driver provider:    ${DRIVER_PROVIDER} (norm=${DRIVER_NORM})"
log "reference provider: ${REF_PROVIDER} (norm=${REF_NORM})"
log "chain gRPC:         ${CHAIN_GRPC}"
log "p2p metrics:        ${P2P_METRICS_URL:-"(disabled)"}"
log "peer-set-stable:    peers≥${PEER_STABLE_MIN_PEERS} custody≥${PEER_STABLE_MIN_CUSTODY} for ${PEER_STABLE_SAMPLES} samples"
log "out:                ${OUT}"
log "seconds_per_slot:   ${SECONDS_PER_SLOT}"
if [[ "${MAX_SAMPLES}" -gt 0 ]]; then
  log "samples planned:    ${MAX_SAMPLES}"
else
  log "samples planned:    until SIGINT/SIGTERM"
fi

# ── local GetHead (grpcurl + proto imports) ─────────────────────────────────
PROTO_ROOT="${REPO_ROOT}/proto"
GETHEAD_METHOD="eth.chain.v1.ChainService/GetHead"

have_grpcurl=0
if command -v grpcurl >/dev/null 2>&1; then
  have_grpcurl=1
fi

# Prints "ROOT_HEX SLOT" or fails.
local_get_head() {
  if [[ "${have_grpcurl}" -ne 1 ]]; then
    die "grpcurl is required to call chain GetHead (install grpcurl or put it on PATH)"
  fi
  local json
  json="$(
    grpcurl -plaintext \
      -import-path "${PROTO_ROOT}" \
      -proto eth/chain/v1/chain.proto \
      -d '{}' \
      "${CHAIN_GRPC}" \
      "${GETHEAD_METHOD}" 2>/dev/null
  )" || return 1
  python3 -c '
import base64, json, sys
raw = sys.stdin.read()
if not raw.strip():
    raise SystemExit(1)
obj = json.loads(raw)
root_b64 = obj.get("headRoot") or obj.get("head_root") or ""
slot = obj.get("headSlot") if "headSlot" in obj else obj.get("head_slot")
if slot is None:
    raise SystemExit(1)
if not root_b64:
    # empty root — not bootstrapped
    print("0x" + ("00" * 32), int(slot))
    raise SystemExit(0)
# grpcurl may emit standard or URL-safe base64; pad if needed.
pad = "=" * (-len(root_b64) % 4)
try:
    data = base64.b64decode(root_b64 + pad)
except Exception:
    data = base64.urlsafe_b64decode(root_b64 + pad)
if len(data) != 32:
    # sometimes already hex without 0x
    s = root_b64
    if s.startswith("0x"):
        s = s[2:]
    if len(s) == 64:
        print("0x" + s.lower(), int(slot))
        raise SystemExit(0)
    raise SystemExit(1)
print("0x" + data.hex(), int(slot))
' <<<"${json}"
}

# Prints "ROOT_HEX SLOT" from a beacon-API provider base.
ref_get_head() {
  local base="$1"
  base="${base%/}"
  local body code
  body="$(mktemp)"
  code="$(
    curl -sS -L \
      --proto '=https' \
      --proto-redir '=https' \
      -A "${UA}" \
      -H "Accept: application/json" \
      --connect-timeout 5 \
      --max-time 15 \
      -o "${body}" \
      -w "%{http_code}" \
      "${base}/eth/v1/beacon/headers/head" 2>/dev/null
  )" || code="000"
  if [[ "${code}" != "200" ]]; then
    rm -f "${body}"
    return 1
  fi
  python3 -c '
import json, sys
from pathlib import Path
obj = json.loads(Path(sys.argv[1]).read_text())
data = obj.get("data") or {}
root = data.get("root") or ""
header = data.get("header") or {}
message = header.get("message") or {}
slot = message.get("slot")
if not root or slot is None:
    raise SystemExit(1)
root = root if root.startswith("0x") else "0x" + root
print(root.lower(), int(slot))
' "${body}"
  rm -f "${body}"
}

# Chain RSS in KiB, or empty on failure.
chain_rss_kib() {
  if [[ -n "${CHAIN_PID}" ]]; then
    # macOS ps: rss is KiB; Linux ps: same with -o rss=
    ps -o rss= -p "${CHAIN_PID}" 2>/dev/null | tr -d ' ' || true
    return 0
  fi
  # Prefer docker container when present (compose stack).
  if command -v docker >/dev/null 2>&1; then
    local cid
    cid="$(docker compose -f "${REPO_ROOT}/docker-compose.yml" ps -q "${CHAIN_CONTAINER}" 2>/dev/null || true)"
    if [[ -z "${cid}" ]]; then
      cid="$(docker ps -q -f "name=${CHAIN_CONTAINER}" 2>/dev/null | head -1 || true)"
    fi
    if [[ -n "${cid}" ]]; then
      # docker stats reports human sizes; use cgroup / docker inspect for bytes.
      local mem
      mem="$(docker stats --no-stream --format '{{.MemUsage}}' "${cid}" 2>/dev/null || true)"
      if [[ -n "${mem}" ]]; then
        python3 -c '
import sys
s = sys.argv[1].split("/")[0].strip()
# e.g. "123.4MiB", "1.2GiB", "456.0KiB"
num = ""
unit = ""
for ch in s:
    if ch.isdigit() or ch == ".":
        num += ch
    else:
        unit += ch
try:
    v = float(num)
except Exception:
    print("")
    raise SystemExit(0)
unit = unit.strip().lower()
mult = {"b": 1/1024, "kib": 1, "kb": 1/1.024, "mib": 1024, "mb": 1000/1.024,
        "gib": 1024*1024, "gb": 1e6/1.024}.get(unit, 1)
print(int(v * mult))
' "${mem}"
        return 0
      fi
    fi
  fi
  # Host process fallback: binary name from package / service.
  local p
  for p in cc-chain chain; do
    local pid
    pid="$(pgrep -x "${p}" 2>/dev/null | head -1 || true)"
    if [[ -n "${pid}" ]]; then
      ps -o rss= -p "${pid}" 2>/dev/null | tr -d ' ' || true
      return 0
    fi
  done
  echo ""
}

# 1-minute load average.
load1() {
  if [[ -r /proc/loadavg ]]; then
    awk '{print $1}' /proc/loadavg
    return 0
  fi
  # macOS: "{ 1.23 2.34 3.45 }"
  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n vm.loadavg 2>/dev/null | python3 -c '
import sys
s = sys.stdin.read().strip().strip("{}").split()
print(s[0] if s else "")
' && return 0
  fi
  echo ""
}

# Scrape p2p gauges from OpenMetrics text. Prints "PEERS CUSTODY" or " ".
# Sums cc_p2p_peers{direction=…}; reads unlabeled cc_p2p_peers_custody_compatible.
scrape_p2p_gauges() {
  local url="$1"
  [[ -n "${url}" ]] || { echo " "; return 0; }
  local body
  body="$(
    curl -sS -L \
      -A "${UA}" \
      --connect-timeout 2 \
      --max-time 5 \
      "${url}" 2>/dev/null
  )" || body=""
  if [[ -z "${body}" ]]; then
    echo " "
    return 0
  fi
  python3 -c '
import sys
text = sys.stdin.read()
# Whole-token names only: sum cc_p2p_peers{direction}; read custody gauge.
peers_sum = 0.0
peers_hit = False
custody = None
for line in text.splitlines():
    if line.startswith("#"):
        continue
    parts = line.split()
    if len(parts) < 2:
        continue
    name = parts[0]
    try:
        val = float(parts[1])
    except ValueError:
        continue
    base = name.split("{", 1)[0]
    if base == "cc_p2p_peers":
        peers_sum += val
        peers_hit = True
    elif base == "cc_p2p_peers_custody_compatible":
        custody = val
if not peers_hit:
    peers_s = ""
else:
    peers_s = str(int(peers_sum)) if peers_sum == int(peers_sum) else str(peers_sum)
if custody is None:
    custody_s = ""
else:
    custody_s = str(int(custody)) if custody == int(custody) else str(custody)
print(f"{peers_s} {custody_s}")
' <<<"${body}"
}

# ── CSV header ──────────────────────────────────────────────────────────────
mkdir -p "$(dirname "${OUT}")"
CSV_HEADER="ts_unix,slot,local_root,local_slot,ref_root,ref_slot,agree,rss_kib,load1,p2p_peers,p2p_peers_custody,head_lag_slots"
if [[ ! -f "${OUT}" ]]; then
  printf '%s\n' "${CSV_HEADER}" > "${OUT}"
fi

# Metadata sidecar (provider names + peer-set-stable for the run record).
META="${OUT%.csv}.meta"
{
  echo "driver_provider=${DRIVER_PROVIDER}"
  echo "driver_provider_norm=${DRIVER_NORM}"
  echo "ref_provider=${REF_PROVIDER}"
  echo "ref_provider_norm=${REF_NORM}"
  echo "chain_grpc=${CHAIN_GRPC}"
  echo "p2p_metrics_url=${P2P_METRICS_URL}"
  echo "peer_stable_min_peers=${PEER_STABLE_MIN_PEERS}"
  echo "peer_stable_min_custody=${PEER_STABLE_MIN_CUSTODY}"
  echo "peer_stable_samples=${PEER_STABLE_SAMPLES}"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "started_unix=$(date +%s)"
} > "${META}"

STOP=0
trap 'STOP=1; log "stop signalled"' INT TERM

n=0
failures=0
stable_run=0
peer_set_stable_unix=""
while [[ "${STOP}" -eq 0 ]]; do
  if [[ "${MAX_SAMPLES}" -gt 0 && "${n}" -ge "${MAX_SAMPLES}" ]]; then
    break
  fi
  tick_start="$(date +%s)"
  ts="${tick_start}"

  local_root=""
  local_slot=""
  ref_root=""
  ref_slot=""
  agree=0

  if local_line="$(local_get_head 2>/dev/null)"; then
    local_root="$(awk '{print $1}' <<<"${local_line}")"
    local_slot="$(awk '{print $2}' <<<"${local_line}")"
  else
    log "warn: GetHead failed at ts=${ts}"
    local_root=""
    local_slot=""
    failures=$((failures + 1))
  fi

  if ref_line="$(ref_get_head "${REF_PROVIDER}" 2>/dev/null)"; then
    ref_root="$(awk '{print $1}' <<<"${ref_line}")"
    ref_slot="$(awk '{print $2}' <<<"${ref_line}")"
  else
    log "warn: reference head failed at ts=${ts}"
    ref_root=""
    ref_slot=""
    failures=$((failures + 1))
  fi

  # Agreement: same root, and slots within 1 of each other (Clause 2/2 lag bound).
  if [[ -n "${local_root}" && -n "${ref_root}" && "${local_root}" == "${ref_root}" ]]; then
    agree=1
  elif [[ -n "${local_slot}" && -n "${ref_slot}" && -n "${local_root}" && -n "${ref_root}" ]]; then
    # Allow 1-slot lag: agree if roots differ only when slots differ by ≤1? No —
    # Clause 2/2 is root equality for the same slot. Compare on min slot by
    # re-fetching is expensive; record root equality only, and slot columns for
    # offline re-check.
    agree=0
  fi

  # Prefer local slot as the row key; fall back to ref, then wall-derived.
  slot="${local_slot:-${ref_slot:-0}}"
  rss="$(chain_rss_kib)"
  load="$(load1)"

  # Phase 2 five-number series (peers, custody, head lag, rss, load).
  p2p_peers=""
  p2p_custody=""
  if [[ -n "${P2P_METRICS_URL}" ]]; then
    p2p_line="$(scrape_p2p_gauges "${P2P_METRICS_URL}" || echo " ")"
    p2p_peers="$(awk '{print $1}' <<<"${p2p_line}")"
    p2p_custody="$(awk '{print $2}' <<<"${p2p_line}")"
  fi
  head_lag=""
  if [[ -n "${local_slot}" && -n "${ref_slot}" ]]; then
    # Diagnostic series only; clause 3 reads the process histogram buckets.
    head_lag="$(python3 -c 'import sys; a=int(sys.argv[1]); b=int(sys.argv[2]); print(max(0, b-a))' \
      "${local_slot}" "${ref_slot}" 2>/dev/null || echo "")"
  fi

  # Peer-set-stable: first sustained hold of both thresholds (opens steady-state).
  if [[ -z "${peer_set_stable_unix}" ]]; then
    if [[ -n "${p2p_peers}" && -n "${p2p_custody}" ]] \
      && python3 -c 'import sys; raise SystemExit(0 if float(sys.argv[1]) >= float(sys.argv[2]) and float(sys.argv[3]) >= float(sys.argv[4]) else 1)' \
        "${p2p_peers}" "${PEER_STABLE_MIN_PEERS}" "${p2p_custody}" "${PEER_STABLE_MIN_CUSTODY}" 2>/dev/null
    then
      stable_run=$((stable_run + 1))
      if [[ "${stable_run}" -ge "${PEER_STABLE_SAMPLES}" ]]; then
        # Boundary = first sample of the sustained run.
        peer_set_stable_unix=$((ts - (PEER_STABLE_SAMPLES - 1) * SECONDS_PER_SLOT))
        {
          echo "peer_set_stable_unix=${peer_set_stable_unix}"
          echo "peer_set_stable_utc=$(python3 -c 'import datetime,sys; print(datetime.datetime.fromtimestamp(int(sys.argv[1]), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))' "${peer_set_stable_unix}")"
        } >> "${META}"
        log "peer-set-stable at unix=${peer_set_stable_unix} (peers≥${PEER_STABLE_MIN_PEERS}, custody≥${PEER_STABLE_MIN_CUSTODY} for ${PEER_STABLE_SAMPLES} samples)"
      fi
    else
      stable_run=0
    fi
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${ts}" \
    "${slot}" \
    "${local_root}" \
    "${local_slot}" \
    "${ref_root}" \
    "${ref_slot}" \
    "${agree}" \
    "${rss}" \
    "${load}" \
    "${p2p_peers}" \
    "${p2p_custody}" \
    "${head_lag}" \
    >> "${OUT}"

  n=$((n + 1))
  if (( n % 10 == 0 )); then
    log "wrote ${n} samples → ${OUT}"
  fi

  # Sleep until next slot boundary (relative to tick start).
  now="$(date +%s)"
  elapsed=$((now - tick_start))
  sleep_for=$((SECONDS_PER_SLOT - elapsed))
  if [[ "${sleep_for}" -gt 0 && "${STOP}" -eq 0 ]]; then
    # Interruptible sleep
    sleep "${sleep_for}" &
    wait $! 2>/dev/null || true
  fi
done

if [[ -z "${peer_set_stable_unix}" ]]; then
  echo "peer_set_stable_unix=" >> "${META}"
  log "warn: peer-set-stable never reached (report will refuse Phase 2 clause 3)"
fi

log "done: ${n} samples written to ${OUT} (transient failures=${failures})"
log "meta: ${META}"
exit 0
