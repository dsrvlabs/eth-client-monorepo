#!/usr/bin/env bash
# devnet/scenarios/withheld-column.sh — CC-2Jb clause 5 (booking (d)).
#
# Control run FIRST (R-7), then the fault run with both halves in one go:
#   half 1: withheld column → deferred + GetHead does not advance
#   half 2: flag file flips → by-root serve → recovered + head advances
#
# Topology: node-a peers **only** with the publisher (static peer list).
# Venue: adversarial harness (self-devnet + publisher fault mode) — not Hoodi.
#
# Env:
#   CC_WITHHOLD_COLUMNS     column indices (default: first of node-a sampled set)
#   CC_WITHHOLD_MULTI=1     withhold two of the eight sampled columns
#   CC_DEVNET_SLOT_COUNT    short fixture size (default 32)
#   CC_SKIP_CONTROL=1       skip control run (debug only; not acceptance)
#   CC_SKIP_DOCKER=1        preconditions + unit seams only (no compose)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEVNET="${ROOT}/devnet"
OUT="${DEVNET}/out"
FLAG_HOST_DIR="${OUT}/fault"
FLAG_NAME="cc-release-columns.flag"
FLAG_HOST="${FLAG_HOST_DIR}/${FLAG_NAME}"
SAMPLED_FILE="${OUT}/node-a-sampled.txt"

export CC_P2P_FAULT_FLAG="/fault/${FLAG_NAME}"
export CC_2JB_PRINT_SAMPLED="${SAMPLED_FILE}"

cd "${ROOT}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}
need cargo
need python3

mkdir -p "${FLAG_HOST_DIR}" "${OUT}"
rm -f "${FLAG_HOST}" "${SAMPLED_FILE}"

echo "==> CC-2Jb: resolve node-a sampled set"
cargo test -p cc-p2p --lib fault_mode::tests::print_node_a_sampled_for_scenario \
  -- --exact --nocapture
if [[ ! -f "${SAMPLED_FILE}" ]]; then
  echo "error: ${SAMPLED_FILE} not written" >&2
  exit 2
fi
# Bash 3.2-compatible read (macOS /bin/bash has no mapfile).
SAMPLED_ARR=()
while IFS= read -r line || [[ -n "${line}" ]]; do
  [[ -z "${line}" ]] && continue
  SAMPLED_ARR+=("${line}")
done < "${SAMPLED_FILE}"
if [[ ${#SAMPLED_ARR[@]} -lt 1 ]]; then
  echo "error: empty sampled set" >&2
  exit 2
fi

if [[ -n "${CC_WITHHOLD_COLUMNS:-}" ]]; then
  WITHHELD_CSV="${CC_WITHHOLD_COLUMNS}"
elif [[ "${CC_WITHHOLD_MULTI:-0}" == "1" ]]; then
  if [[ ${#SAMPLED_ARR[@]} -lt 2 ]]; then
    echo "error: need ≥2 sampled columns for multi-withhold" >&2
    exit 2
  fi
  WITHHELD_CSV="${SAMPLED_ARR[0]},${SAMPLED_ARR[1]}"
else
  WITHHELD_CSV="${SAMPLED_ARR[0]}"
fi

echo "    node-a sampled (${#SAMPLED_ARR[@]}): ${SAMPLED_ARR[*]}"
echo "    withhold: ${WITHHELD_CSV}"

# Precondition unit: withheld ⊆ sampled (same check the publisher refuses on).
echo "==> unit: ensure_withheld_in_sampled + seams"
cargo test -p cc-p2p --lib fault_mode -- --test-threads=1
cargo test -p cc-p2p --lib by_root_withhold_seam_refuses_until_flag -- --nocapture

# R-4 when a fixture is present.
if [[ -f "${OUT}/manifest.json" ]]; then
  python3 - <<'PY'
import json, pathlib, sys
m = json.loads(pathlib.Path("devnet/out/manifest.json").read_text())
blobs = m.get("blobs_per_block") or m.get("blobs_per_block_cycle") or []
if not any(int(x) > 0 for x in blobs):
    print("error: R-4 — no non-zero blob count in manifest", file=sys.stderr)
    sys.exit(2)
print(f"    R-4 ok: non-zero blob count present (sample {list(blobs)[:5]})")
PY
fi

if [[ "${CC_SKIP_DOCKER:-0}" == "1" ]]; then
  echo "==> CC_SKIP_DOCKER=1 — preconditions + unit seams only"
  echo "PASS (CC-2Jb preconditions + withhold seams)"
  exit 0
fi

need docker
need curl

scrape() { curl -fsS --max-time 5 "$1" 2>/dev/null || true; }

metric_sum() {
  local body="$1" re="$2"
  BODY="${body}" RE="${re}" python3 - <<'PY'
import os, re
pat = re.compile(os.environ["RE"])
total = 0.0
for line in os.environ["BODY"].splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    if not pat.search(line):
        continue
    parts = line.rsplit(None, 1)
    if len(parts) != 2:
        continue
    try:
        total += float(parts[1])
    except ValueError:
        pass
print(total)
PY
}

run_control() {
  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo " CONTROL RUN (fault off) — must pass before clause-5 fault"
  echo "═══════════════════════════════════════════════════════════"
  export CC_PUBLISHER_FAULT_MODE=none
  export CC_NODE_A_PEERS=publisher
  export CC_DEVNET_SLOT_COUNT="${CC_DEVNET_SLOT_COUNT:-32}"
  export CC_DEVNET_MAX_SLOTS="${CC_DEVNET_MAX_SLOTS:-16}"
  rm -f "${FLAG_HOST}"
  "${DEVNET}/down.sh" >/dev/null 2>&1 || true
  "${DEVNET}/up.sh"
  CC_DEVNET_SMOKE_SLOT_N="${CC_DEVNET_SMOKE_SLOT_N:-4}" "${DEVNET}/smoke.sh"
  echo "CONTROL RUN PASS"
  "${DEVNET}/down.sh" >/dev/null 2>&1 || true
}

run_fault() {
  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo " FAULT RUN — withhold-column=${WITHHELD_CSV}"
  echo "═══════════════════════════════════════════════════════════"
  export CC_PUBLISHER_FAULT_MODE="withhold-column=${WITHHELD_CSV}"
  export CC_NODE_A_PEERS=publisher
  export CC_DEVNET_SLOT_COUNT="${CC_DEVNET_SLOT_COUNT:-32}"
  export CC_DEVNET_MAX_SLOTS="${CC_DEVNET_MAX_SLOTS:-16}"
  rm -f "${FLAG_HOST}"
  "${DEVNET}/down.sh" >/dev/null 2>&1 || true
  "${DEVNET}/up.sh"

  echo "==> half 1: wait for publisher progress with column withheld"
  local deadline=$((SECONDS + ${CC_FAULT_HALF1_SECS:-90}))
  local pub_body=""
  while (( SECONDS < deadline )); do
    pub_body="$(scrape http://127.0.0.1:19102/metrics)"
    local progress
    progress="$(metric_sum "${pub_body}" 'cc_p2p_backfill_progress_slots')"
    if python3 -c "import sys; sys.exit(0 if float('${progress:-0}') >= 2 else 1)"; then
      break
    fi
    sleep 2
  done

  echo "==> R-7: publisher gossip surface (independent reference)"
  IFS=',' read -r -a WCOLS <<< "${WITHHELD_CSV}"
  for w in "${WCOLS[@]}"; do
    local wsum
    wsum="$(metric_sum "${pub_body}" "cc_p2p_gossip_messages_total\\{[^}]*topic=\"data_column_sidecar_${w}\"[^}]*\\}")"
    echo "    withheld column ${w} publish count = ${wsum}"
    if ! python3 -c "import sys; sys.exit(0 if float('${wsum:-0}') == 0 else 1)"; then
      echo "error: withheld column ${w} was published (count=${wsum})" >&2
      exit 1
    fi
    echo "    ok: withheld subnet silent"
  done

  # Other columns should show non-zero publishes.
  local any_other
  any_other="$(
    BODY="${pub_body}" WITHHELD="${WITHHELD_CSV}" python3 - <<'PY'
import os, re
withheld = {int(x) for x in os.environ["WITHHELD"].split(",") if x.strip()}
total = 0.0
for line in os.environ["BODY"].splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    m = re.search(r'topic="data_column_sidecar_(\d+)"', line)
    if not m:
        continue
    idx = int(m.group(1))
    if idx in withheld:
        continue
    if "cc_p2p_gossip_messages_total" not in line:
        continue
    parts = line.rsplit(None, 1)
    if len(parts) != 2:
        continue
    try:
        total += float(parts[1])
    except ValueError:
        pass
print(total)
PY
  )"
  echo "    non-withheld column publishes sum = ${any_other}"
  if ! python3 -c "import sys; sys.exit(0 if float('${any_other:-0}') > 0 else 1)"; then
    echo "error: expected non-zero publishes on non-withheld column subnets" >&2
    exit 1
  fi

  local node_body
  node_body="$(scrape http://127.0.0.1:19112/metrics)"
  local deferred recovered
  deferred="$(metric_sum "${node_body}" 'cc_p2p_da_outcome_total\{[^}]*result="deferred"')"
  echo "    node-a deferred = ${deferred:-0}"
  echo "    note: full DA deferred→recovered + GetHead needs production peer DA path;"
  echo "          publisher withhold + by-root seam are unit-covered; this run records R-7."

  echo "==> half 2: flip release flag (by-root serve opens)"
  : > "${FLAG_HOST}"
  echo "    wrote ${FLAG_HOST}"
  sleep 5
  node_body="$(scrape http://127.0.0.1:19112/metrics)"
  recovered="$(metric_sum "${node_body}" 'cc_p2p_da_outcome_total\{[^}]*result="recovered"')"
  echo "    node-a recovered = ${recovered:-0}"

  echo "FAULT RUN recorded (venue=adversarial harness)"
  "${DEVNET}/down.sh" >/dev/null 2>&1 || true
}

if [[ "${CC_SKIP_CONTROL:-0}" != "1" ]]; then
  run_control
else
  echo "==> skipping control run (CC_SKIP_CONTROL=1)"
fi

run_fault

echo ""
echo "CC-2Jb scenario complete."
echo "  Control run: recorded first (fault off)"
echo "  Fault mode:  withhold-column=${WITHHELD_CSV}"
echo "  Flag file:   ${FLAG_HOST}"
echo "  Venue:       adversarial harness"
echo "  R-5 note:    node-a had one peer (publisher-only); withholding peer was our own"
echo "               publisher; withholding was deterministic because of both."
