#!/usr/bin/env bash
# scripts/phase-3-acceptance.sh — CC-39b two-phase acceptance skeleton (§8.4)
#
# Phase A and Phase B assert **different** things. Conflating them makes the
# test pass on a broken client and fail on a slow disk (CC-39 /6).
#
#   Phase A — during catch-up (`eth_syncing != false`):
#     assert the **optimistic machinery works**:
#       * blocks import / head advances
#       * cc_chain_is_optimistic == 1
#       * cc_engine_payload_status_total{method="newPayloadV4",status="SYNCING"}
#         is strictly increasing
#     Phase A is by construction entirely is_optimistic==1 (A-P3-9 / geth design).
#
#   Phase B — after the sync gate (`eth_syncing == false`):
#     assert NOT_VALIDATED → VALID fires and optimistic occupancy clears:
#       * cc_chain_optimistic_transitions_total{direction="validated"} increasing
#       * cc_chain_optimistic_nodes reaches 0
#
# The Phase A → Phase B boundary is the machine-readable **window_start** that
# CC-3Ab's soak-report.sh consumes so the bootstrap catch-up burst is excluded
# by construction, not by operator judgement.
#
# Usage:
#   bash scripts/phase-3-acceptance.sh --phase a
#   bash scripts/phase-3-acceptance.sh --phase b
#   bash scripts/phase-3-acceptance.sh --phase boundary   # emit window_start only
#   bash scripts/phase-3-acceptance.sh --self-test        # offline metric fixtures
#   bash scripts/phase-3-acceptance.sh --negative a|b     # expect assertion failure
#
# Environment / flags:
#   --chain-metrics-url   default http://127.0.0.1:9101/metrics
#   --engine-metrics-url  default http://127.0.0.1:9104/metrics
#   --el-http             default http://127.0.0.1:8545
#   --boundary-file       where to write window_start (default: .data/phase3-window-start)
#   --sample-seconds      head/metric re-sample interval (default: 12)
#   --samples             number of samples for "strictly increasing" (default: 3)
#
# Exit:
#   0 pass
#   1 assertion failure
#   2 usage / missing tools
#   3 EL not in the phase's expected sync state (refuse to run wrong phase)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PHASE=""
NEGATIVE=0
SELF_TEST=0
CHAIN_METRICS_URL="${PHASE3_CHAIN_METRICS_URL:-http://127.0.0.1:9101/metrics}"
ENGINE_METRICS_URL="${PHASE3_ENGINE_METRICS_URL:-http://127.0.0.1:9104/metrics}"
EL_HTTP="${PHASE3_EL_HTTP:-http://127.0.0.1:8545}"
BOUNDARY_FILE="${PHASE3_BOUNDARY_FILE:-${REPO_ROOT}/.data/phase3-window-start}"
SAMPLE_SECONDS="${PHASE3_SAMPLE_SECONDS:-12}"
SAMPLES="${PHASE3_SAMPLES:-3}"
FIXTURE_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --phase) PHASE="$2"; shift 2 ;;
    --negative) NEGATIVE=1; PHASE="$2"; shift 2 ;;
    --self-test) SELF_TEST=1; shift ;;
    --chain-metrics-url) CHAIN_METRICS_URL="$2"; shift 2 ;;
    --engine-metrics-url) ENGINE_METRICS_URL="$2"; shift 2 ;;
    --el-http) EL_HTTP="$2"; shift 2 ;;
    --boundary-file) BOUNDARY_FILE="$2"; shift 2 ;;
    --sample-seconds) SAMPLE_SECONDS="$2"; shift 2 ;;
    --samples) SAMPLES="$2"; shift 2 ;;
    --fixture-dir) FIXTURE_DIR="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,50p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

log() { echo "phase3-accept: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }
fail() { echo "ASSERT FAIL: $*" >&2; exit 1; }

utc_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
unix_now() { date -u +%s; }

# ── metric helpers ──────────────────────────────────────────────────────────
scrape() {
  local url="$1"
  if [[ -n "${FIXTURE_DIR}" ]]; then
    # Fixture mode: url is a logical name mapped to files under FIXTURE_DIR.
    local base
    base="$(basename "${url}")"
    if [[ -f "${FIXTURE_DIR}/${base}" ]]; then
      cat "${FIXTURE_DIR}/${base}"
      return 0
    fi
    # Allow chain-metrics-N.txt / engine-metrics-N.txt via PHASE3_SAMPLE_IDX
    local idx="${PHASE3_SAMPLE_IDX:-0}"
    if [[ -f "${FIXTURE_DIR}/${base%.txt}-${idx}.txt" ]]; then
      cat "${FIXTURE_DIR}/${base%.txt}-${idx}.txt"
      return 0
    fi
    if [[ -f "${FIXTURE_DIR}/${base}-${idx}" ]]; then
      cat "${FIXTURE_DIR}/${base}-${idx}"
      return 0
    fi
    die "fixture missing for ${url} under ${FIXTURE_DIR}"
  fi
  curl -fsS --max-time 10 "${url}"
}

# Extract a prometheus gauge/counter value. Labels matched as substring regex.
# Usage: prom_value METRICS_TEXT 'metric_name' 'label_fragment_regex'
prom_value() {
  local text="$1" name="$2" labels="${3:-}"
  PROM_BODY="${text}" PROM_NAME="${name}" PROM_LABELS="${labels}" python3 - <<'PY'
import os, re, sys
name = os.environ["PROM_NAME"]
labels = os.environ.get("PROM_LABELS", "")
body = os.environ["PROM_BODY"]
pat = re.compile(
    r"^" + re.escape(name) + r"(?:\{([^}]*)\})?\s+([0-9.eE+-]+)\s*$",
    re.M,
)
best = None
for m in pat.finditer(body):
    lab = m.group(1) or ""
    if labels and not re.search(labels, lab):
        continue
    best = float(m.group(2))
if best is None:
    sys.exit(2)
print(best)
PY
}

eth_syncing_raw() {
  if [[ -n "${FIXTURE_DIR}" && -f "${FIXTURE_DIR}/eth_syncing.json" ]]; then
    cat "${FIXTURE_DIR}/eth_syncing.json"
    return 0
  fi
  curl -fsS --max-time 10 -X POST "${EL_HTTP}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}'
}

# Returns 0 if eth_syncing is false (synced), 1 if still syncing, 2 on error.
# Intentionally never toggles set -e: a non-zero `return` under `set -e` can
# abort the whole script depending on bash version / call-site state.
eth_is_synced() {
  local raw rc
  raw="$(eth_syncing_raw)" && rc=0 || rc=$?
  if [[ "${rc}" -ne 0 ]]; then
    return 2
  fi
  if printf '%s' "${raw}" | grep -q '"result":false'; then
    return 0
  fi
  # result: true or result: { ... } both mean syncing
  if printf '%s' "${raw}" | grep -qE '"result":(true|\{)'; then
    return 1
  fi
  return 2
}

eth_block_number() {
  if [[ -n "${FIXTURE_DIR}" && -f "${FIXTURE_DIR}/eth_blockNumber.json" ]]; then
    python3 -c 'import json,sys; print(int(json.load(open(sys.argv[1]))["result"],16))' \
      "${FIXTURE_DIR}/eth_blockNumber.json"
    return 0
  fi
  local raw
  raw="$(curl -fsS --max-time 10 -X POST "${EL_HTTP}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}')"
  python3 -c 'import json,sys; print(int(json.loads(sys.argv[1])["result"],16))' "${raw}"
}

# ── phase boundary / window_start ──────────────────────────────────────────
# Machine-readable timestamp consumed by CC-3Ab soak-report.sh.
emit_phase_boundary() {
  local ts_utc ts_unix
  ts_utc="$(utc_now)"
  ts_unix="$(unix_now)"
  mkdir -p "$(dirname "${BOUNDARY_FILE}")"
  # Key names include both spellings greppable as phase.*boundary|window_start
  cat >"${BOUNDARY_FILE}" <<EOF
# phase_boundary: Phase A → Phase B (sync gate crossed)
# window_start is the start of the ≥6 h acceptance window (bootstrap burst excluded)
window_start_utc=${ts_utc}
window_start_unix=${ts_unix}
phase_boundary=A_to_B
phase_boundary_utc=${ts_utc}
phase_boundary_unix=${ts_unix}
EOF
  log "phase boundary / window_start written to ${BOUNDARY_FILE}"
  log "window_start_utc=${ts_utc} window_start_unix=${ts_unix}"
  echo "window_start_utc=${ts_utc}"
  echo "window_start_unix=${ts_unix}"
  echo "phase_boundary=A_to_B"
}

# ── Phase A: optimistic machinery during catch-up ───────────────────────────
run_phase_a() {
  log "Phase A — assert optimistic machinery (eth_syncing must NOT be false)"

  local sync_rc is_opt_rc sync_m_rc
  set +e
  eth_is_synced
  sync_rc=$?
  set -e
  if [[ "${sync_rc}" -eq 0 ]]; then
    # EL already synced — Phase A is the wrong phase (or negative test expects fail).
    fail "Phase A requires eth_syncing != false (EL is already synced). Run --phase b."
  fi
  if [[ "${sync_rc}" -eq 2 ]]; then
    fail "Phase A: cannot reach eth_syncing at ${EL_HTTP}"
  fi

  local head0 head1
  head0="$(eth_block_number)"
  log "Phase A: head at t0 = ${head0}"

  local i syncing_prev="" is_opt eng_text chain_text syncing_now
  for ((i = 1; i <= SAMPLES; i++)); do
    sleep "${SAMPLE_SECONDS}"
    chain_text="$(scrape "${CHAIN_METRICS_URL}")"
    eng_text="$(scrape "${ENGINE_METRICS_URL}")"

    set +e
    is_opt="$(prom_value "${chain_text}" "cc_chain_is_optimistic" "")"
    is_opt_rc=$?
    syncing_now="$(prom_value "${eng_text}" "cc_engine_payload_status_total" 'method="newPayloadV4".*status="SYNCING"|status="SYNCING".*method="newPayloadV4"')"
    sync_m_rc=$?
    set -e

    if [[ "${is_opt_rc}" -ne 0 ]]; then
      fail "Phase A: cc_chain_is_optimistic missing from chain metrics"
    fi
    # Accept 1 or 1.0
    if ! awk -v v="${is_opt}" 'BEGIN{exit !(v+0 == 1)}'; then
      fail "Phase A: cc_chain_is_optimistic == ${is_opt}, want 1 (node must be optimistic during catch-up)"
    fi
    log "Phase A sample ${i}: cc_chain_is_optimistic=${is_opt}"

    if [[ "${sync_m_rc}" -ne 0 ]]; then
      fail "Phase A: cc_engine_payload_status_total{method=\"newPayloadV4\",status=\"SYNCING\"} missing"
    fi
    if [[ -n "${syncing_prev}" ]]; then
      if ! awk -v a="${syncing_prev}" -v b="${syncing_now}" 'BEGIN{exit !(b+0 > a+0)}'; then
        fail "Phase A: SYNCING counter not strictly increasing (${syncing_prev} → ${syncing_now})"
      fi
      log "Phase A sample ${i}: SYNCING ${syncing_prev} → ${syncing_now} (increasing)"
    else
      log "Phase A sample ${i}: SYNCING baseline ${syncing_now}"
    fi
    syncing_prev="${syncing_now}"
  done

  head1="$(eth_block_number)"
  log "Phase A: head at t1 = ${head1}"
  if ! awk -v a="${head0}" -v b="${head1}" 'BEGIN{exit !(b+0 > a+0)}'; then
    fail "Phase A: head did not advance (${head0} → ${head1}) — blocks not importing"
  fi
  log "Phase A: head advanced ${head0} → ${head1}"

  log "Phase A PASS — optimistic machinery live during catch-up"
  return 0
}

# ── Phase B: post-gate validated transition ─────────────────────────────────
run_phase_b() {
  log "Phase B — assert NOT_VALIDATED→VALID and optimistic_nodes→0 (eth_syncing must be false)"

  local sync_rc t0_rc t1_rc nodes_rc
  set +e
  eth_is_synced
  sync_rc=$?
  set -e
  if [[ "${sync_rc}" -eq 1 ]]; then
    fail "Phase B requires eth_syncing == false (EL still syncing). Run --phase a during catch-up."
  fi
  if [[ "${sync_rc}" -eq 2 ]]; then
    fail "Phase B: cannot reach eth_syncing at ${EL_HTTP}"
  fi

  # Emit phase boundary / window_start exactly when Phase B is entered post-gate.
  emit_phase_boundary

  local chain_text t0_val t1_val nodes
  chain_text="$(scrape "${CHAIN_METRICS_URL}")"

  set +e
  t0_val="$(prom_value "${chain_text}" "cc_chain_optimistic_transitions_total" 'direction="validated"')"
  t0_rc=$?
  set -e
  if [[ "${t0_rc}" -ne 0 ]]; then
    fail "Phase B: cc_chain_optimistic_transitions_total{direction=\"validated\"} missing"
  fi
  log "Phase B: validated transitions baseline = ${t0_val}"

  local i
  for ((i = 1; i <= SAMPLES; i++)); do
    sleep "${SAMPLE_SECONDS}"
    chain_text="$(scrape "${CHAIN_METRICS_URL}")"
    set +e
    t1_val="$(prom_value "${chain_text}" "cc_chain_optimistic_transitions_total" 'direction="validated"')"
    t1_rc=$?
    nodes="$(prom_value "${chain_text}" "cc_chain_optimistic_nodes" "")"
    nodes_rc=$?
    set -e
    if [[ "${t1_rc}" -ne 0 ]]; then
      fail "Phase B: validated transitions metric disappeared"
    fi
    if [[ "${nodes_rc}" -ne 0 ]]; then
      fail "Phase B: cc_chain_optimistic_nodes missing"
    fi
    log "Phase B sample ${i}: validated=${t1_val} optimistic_nodes=${nodes}"
  done

  if ! awk -v a="${t0_val}" -v b="${t1_val}" 'BEGIN{exit !(b+0 > a+0)}'; then
    fail "Phase B: validated transitions not increasing (${t0_val} → ${t1_val}) — NOT_VALIDATED→VALID did not fire"
  fi
  if ! awk -v n="${nodes}" 'BEGIN{exit !(n+0 == 0)}'; then
    fail "Phase B: cc_chain_optimistic_nodes == ${nodes}, want 0"
  fi

  log "Phase B PASS — validated transition fired; optimistic_nodes == 0"
  return 0
}

# ── self-test with synthetic fixtures ───────────────────────────────────────
run_self_test() {
  local tmp
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/cc-phase3-accept.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -rf '${tmp}'" EXIT

  write_chain_to() {
    local path="$1" opt="$2" nodes="$3" validated="$4"
    cat >"${path}" <<EOF
# TYPE cc_chain_is_optimistic gauge
cc_chain_is_optimistic ${opt}
# TYPE cc_chain_optimistic_nodes gauge
cc_chain_optimistic_nodes ${nodes}
# TYPE cc_chain_optimistic_transitions_total counter
cc_chain_optimistic_transitions_total{direction="validated"} ${validated}
cc_chain_optimistic_transitions_total{direction="invalidated"} 0
EOF
  }
  write_engine_to() {
    local path="$1" syncing="$2"
    cat >"${path}" <<EOF
# TYPE cc_engine_payload_status_total counter
cc_engine_payload_status_total{method="newPayloadV4",status="SYNCING"} ${syncing}
cc_engine_payload_status_total{method="newPayloadV4",status="VALID"} 0
EOF
  }

  cat >"${tmp}/eth_syncing_syncing.json" <<'EOF'
{"jsonrpc":"2.0","id":1,"result":{"startingBlock":"0x1","currentBlock":"0x100","highestBlock":"0x200"}}
EOF
  cat >"${tmp}/eth_syncing_false.json" <<'EOF'
{"jsonrpc":"2.0","id":1,"result":false}
EOF
  cat >"${tmp}/block0.json" <<'EOF'
{"jsonrpc":"2.0","id":1,"result":"0x1000"}
EOF
  cat >"${tmp}/block1.json" <<'EOF'
{"jsonrpc":"2.0","id":1,"result":"0x1010"}
EOF

  # Phase A positive: background writer advances SYNCING counter + head.
  mkdir -p "${tmp}/live_a"
  cp "${tmp}/eth_syncing_syncing.json" "${tmp}/live_a/eth_syncing.json"
  cp "${tmp}/block0.json" "${tmp}/live_a/eth_blockNumber.json"
  write_chain_to "${tmp}/live_a/chain-metrics.txt" 1 12 0
  write_engine_to "${tmp}/live_a/engine-metrics.txt" 10

  (
    sleep 1
    write_chain_to "${tmp}/live_a/chain-metrics.txt" 1 12 0
    write_engine_to "${tmp}/live_a/engine-metrics.txt" 15
    sleep 1
    write_chain_to "${tmp}/live_a/chain-metrics.txt" 1 12 0
    write_engine_to "${tmp}/live_a/engine-metrics.txt" 22
    cp "${tmp}/block1.json" "${tmp}/live_a/eth_blockNumber.json"
  ) &

  log "self-test: Phase A positive"
  bash "$0" --phase a \
    --fixture-dir "${tmp}/live_a" \
    --chain-metrics-url "chain-metrics.txt" \
    --engine-metrics-url "engine-metrics.txt" \
    --sample-seconds 1 \
    --samples 2

  # Phase B positive: validated transitions climb; optimistic_nodes → 0.
  mkdir -p "${tmp}/live_b"
  cp "${tmp}/eth_syncing_false.json" "${tmp}/live_b/eth_syncing.json"
  write_chain_to "${tmp}/live_b/chain-metrics.txt" 0 0 3
  write_engine_to "${tmp}/live_b/engine-metrics.txt" 0
  (
    sleep 1
    write_chain_to "${tmp}/live_b/chain-metrics.txt" 0 0 5
    sleep 1
    write_chain_to "${tmp}/live_b/chain-metrics.txt" 0 0 7
  ) &

  log "self-test: Phase B positive"
  bash "$0" --phase b \
    --fixture-dir "${tmp}/live_b" \
    --chain-metrics-url "chain-metrics.txt" \
    --engine-metrics-url "engine-metrics.txt" \
    --boundary-file "${tmp}/window_start" \
    --sample-seconds 1 \
    --samples 2

  [[ -f "${tmp}/window_start" ]] || die "self-test: boundary file not written"
  grep -q 'window_start_utc=' "${tmp}/window_start" || die "self-test: window_start missing"
  grep -q 'phase_boundary=' "${tmp}/window_start" || die "self-test: phase_boundary missing"

  # Negative: Phase B during catch-up must fail
  log "self-test: Phase B during catch-up (expect fail)"
  mkdir -p "${tmp}/neg"
  cp "${tmp}/eth_syncing_syncing.json" "${tmp}/neg/eth_syncing.json"
  write_chain_to "${tmp}/neg/chain-metrics.txt" 1 12 0
  set +e
  bash "$0" --phase b \
    --fixture-dir "${tmp}/neg" \
    --chain-metrics-url "chain-metrics.txt" \
    --engine-metrics-url "engine-metrics.txt" \
    --sample-seconds 0 \
    --samples 1
  neg_rc=$?
  set -e
  [[ "${neg_rc}" -ne 0 ]] || die "self-test: Phase B should fail while syncing"

  # Negative: Phase A after gate must fail
  log "self-test: Phase A after gate (expect fail)"
  mkdir -p "${tmp}/neg2"
  cp "${tmp}/eth_syncing_false.json" "${tmp}/neg2/eth_syncing.json"
  write_chain_to "${tmp}/neg2/chain-metrics.txt" 0 0 7
  write_engine_to "${tmp}/neg2/engine-metrics.txt" 100
  set +e
  bash "$0" --phase a \
    --fixture-dir "${tmp}/neg2" \
    --chain-metrics-url "chain-metrics.txt" \
    --engine-metrics-url "engine-metrics.txt" \
    --sample-seconds 0 \
    --samples 1
  neg_rc=$?
  set -e
  [[ "${neg_rc}" -ne 0 ]] || die "self-test: Phase A should fail when synced"

  log "self-test PASS"
  exit 0
}

# ── main ────────────────────────────────────────────────────────────────────
if [[ "${SELF_TEST}" -eq 1 ]]; then
  run_self_test
fi

if [[ -z "${PHASE}" ]]; then
  echo "error: --phase a|b|boundary required (or --self-test)" >&2
  exit 2
fi

case "${PHASE}" in
  a|A)
    if [[ "${NEGATIVE}" -eq 1 ]]; then
      # Negative mode: run Phase A and invert exit (used during post-gate proof).
      set +e
      run_phase_a
      rc=$?
      set -e
      if [[ "${rc}" -eq 0 ]]; then
        fail "negative Phase A: expected failure after gate, but assertions passed"
      fi
      log "negative Phase A: correctly failed (exit ${rc})"
      exit 0
    fi
    run_phase_a
    ;;
  b|B)
    if [[ "${NEGATIVE}" -eq 1 ]]; then
      set +e
      run_phase_b
      rc=$?
      set -e
      if [[ "${rc}" -eq 0 ]]; then
        fail "negative Phase B: expected failure during catch-up, but assertions passed"
      fi
      log "negative Phase B: correctly failed (exit ${rc})"
      exit 0
    fi
    run_phase_b
    ;;
  boundary)
    emit_phase_boundary
    ;;
  *)
    echo "error: --phase must be a, b, or boundary" >&2
    exit 2
    ;;
esac
