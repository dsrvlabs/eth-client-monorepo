#!/usr/bin/env bash
# scripts/el-restart-drills.sh — CC-36b Clause 2: both EL restart shapes
#
# Venue: **local compose + EL** (on Hoodi data). Hoodi will not restart an EL on
# cue; Phase 2's self-devnet has no EL containers. Fault is induced at the
# container level (D-7 / D-11).
#
# Exact commands (issue CC-36b):
#   clean:   docker compose restart el
#   unclean: docker kill -s KILL <el-container> && docker compose up -d el
#
# Three assertions per shape:
#   1. During outage: cc_engine_el_offline==1; imports continue optimistic
#      (cc_chain_is_optimistic==1, cc_chain_optimistic_nodes>0).
#   2. After EL answers: forkchoiceUpdated within one slot (12 s) of
#      eth_syncing==false (both timestamps recorded).
#   3. One VALID clears the whole optimistic set in one ancestor pass with
#      zero payload re-submission:
#        transitions_validated_after − before == outage_block_count (exact)
#        cc_chain_optimistic_nodes → 0
#        Δ newPayloadV4 request count == new blocks in that interval only
#
# Also recorded (not pass criteria of the three, but report-required):
#   - AuthFailed never observed (JWT unchanged) — states pass offline/syncing only
#   - cc_engine_capability_missing returns to pre-outage state
#   - cc_engine_worker_panics_total (any increment = P0 bug, not a void)
#   - D-11: both drills complete before clause 1's window; fresh eth_syncing==false
#     after unclean re-acquire is the timestamp CC-3Ac's window starts from
#
# Usage:
#   bash scripts/el-restart-drills.sh --shape both
#   bash scripts/el-restart-drills.sh --shape clean|unclean
#   bash scripts/el-restart-drills.sh --self-test
#   bash scripts/el-restart-drills.sh --check-prereqs
#   bash scripts/el-restart-drills.sh --emit-not-run [--reason TEXT]
#   bash scripts/el-restart-drills.sh --from-fixture-dir DIR   # offline evaluate
#
# Environment / flags:
#   --compose-dir DIR          default: repo root
#   --el-service NAME          default: el
#   --chain-metrics-url URL    default: http://127.0.0.1:9101/metrics
#   --engine-metrics-url URL   default: http://127.0.0.1:9104/metrics
#   --el-http URL              default: http://127.0.0.1:8545
#   --out PATH                 harness JSON (default: .data/el-restart-drills.json)
#   --poll-seconds N           metric poll interval (default: 1)
#   --outage-wait-seconds N    max wait for el_offline==1 after kill (default: 30)
#   --resync-wait-seconds N    max wait for eth_syncing==false (default: 600)
#   --clear-wait-seconds N     max wait for optimistic clear (default: 120)
#   --min-outage-seconds N     hold kill long enough for ≥1 import (default: 24)
#   --slot-seconds N           Hoodi slot (default: 12)
#   --require-synced           refuse unless eth_syncing==false at start (default on for live)
#   --allow-unsynced           override for debugging only (void for clause 2)
#
# Exit:
#   0  all selected shapes PASS (or --emit-not-run / --self-test ok)
#   1  assertion failure on a live/fixture shape
#   2  usage
#   3  prerequisites missing (live path)
#   4  partial (one shape FAIL) — still writes harness JSON
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SHAPE="both"
SELF_TEST=0
CHECK_PREREQS=0
EMIT_NOT_RUN=0
NOT_RUN_REASON=""
FIXTURE_DIR=""
COMPOSE_DIR="${REPO_ROOT}"
EL_SERVICE="el"
CHAIN_METRICS_URL="${EL_DRILL_CHAIN_METRICS_URL:-http://127.0.0.1:9101/metrics}"
ENGINE_METRICS_URL="${EL_DRILL_ENGINE_METRICS_URL:-http://127.0.0.1:9104/metrics}"
EL_HTTP="${EL_DRILL_EL_HTTP:-http://127.0.0.1:8545}"
OUT="${EL_DRILL_OUT:-${REPO_ROOT}/.data/el-restart-drills.json}"
POLL_SECONDS="${EL_DRILL_POLL_SECONDS:-1}"
OUTAGE_WAIT="${EL_DRILL_OUTAGE_WAIT:-30}"
RESYNC_WAIT="${EL_DRILL_RESYNC_WAIT:-600}"
CLEAR_WAIT="${EL_DRILL_CLEAR_WAIT:-120}"
MIN_OUTAGE="${EL_DRILL_MIN_OUTAGE:-24}"
SLOT_SECONDS="${EL_DRILL_SLOT_SECONDS:-12}"
REQUIRE_SYNCED=1
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --shape) SHAPE="$2"; shift 2 ;;
    --self-test) SELF_TEST=1; shift ;;
    --check-prereqs) CHECK_PREREQS=1; shift ;;
    --emit-not-run) EMIT_NOT_RUN=1; shift ;;
    --reason) NOT_RUN_REASON="$2"; shift 2 ;;
    --from-fixture-dir) FIXTURE_DIR="$2"; shift 2 ;;
    --compose-dir) COMPOSE_DIR="$2"; shift 2 ;;
    --el-service) EL_SERVICE="$2"; shift 2 ;;
    --chain-metrics-url) CHAIN_METRICS_URL="$2"; shift 2 ;;
    --engine-metrics-url) ENGINE_METRICS_URL="$2"; shift 2 ;;
    --el-http) EL_HTTP="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --poll-seconds) POLL_SECONDS="$2"; shift 2 ;;
    --outage-wait-seconds) OUTAGE_WAIT="$2"; shift 2 ;;
    --resync-wait-seconds) RESYNC_WAIT="$2"; shift 2 ;;
    --clear-wait-seconds) CLEAR_WAIT="$2"; shift 2 ;;
    --min-outage-seconds) MIN_OUTAGE="$2"; shift 2 ;;
    --slot-seconds) SLOT_SECONDS="$2"; shift 2 ;;
    --require-synced) REQUIRE_SYNCED=1; shift ;;
    --allow-unsynced) REQUIRE_SYNCED=0; shift ;;
    --compose-project) COMPOSE_PROJECT="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,70p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

log() { echo "el-restart-drills: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }
refuse() { echo "REFUSED: $*" >&2; exit 3; }

utc_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
unix_now() { date -u +%s; }

compose() {
  local args=(docker compose)
  if [[ -n "${COMPOSE_PROJECT}" ]]; then
    args+=(-p "${COMPOSE_PROJECT}")
  fi
  args+=(--project-directory "${COMPOSE_DIR}")
  "${args[@]}" "$@"
}

# ── metric / EL helpers ─────────────────────────────────────────────────────
scrape() {
  local url="$1"
  if [[ -n "${FIXTURE_DIR}" ]]; then
    local base idx
    base="$(basename "${url}")"
    idx="${EL_DRILL_SAMPLE_IDX:-0}"
    if [[ -f "${FIXTURE_DIR}/${base}" ]]; then
      cat "${FIXTURE_DIR}/${base}"
      return 0
    fi
    if [[ -f "${FIXTURE_DIR}/${base%.txt}-${idx}.txt" ]]; then
      cat "${FIXTURE_DIR}/${base%.txt}-${idx}.txt"
      return 0
    fi
    if [[ -f "${FIXTURE_DIR}/${base}-${idx}" ]]; then
      cat "${FIXTURE_DIR}/${base}-${idx}"
      return 0
    fi
    # Logical names used by the drill: chain-metrics / engine-metrics
    if [[ -f "${FIXTURE_DIR}/${url}" ]]; then
      cat "${FIXTURE_DIR}/${url}"
      return 0
    fi
    die "fixture missing for ${url} under ${FIXTURE_DIR}"
  fi
  curl -fsS --max-time 10 "${url}"
}

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

# Sum all series for a counter family (optional label regex). Missing → exit 2.
prom_sum() {
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
total = 0.0
found = False
for m in pat.finditer(body):
    lab = m.group(1) or ""
    if labels and not re.search(labels, lab):
        continue
    total += float(m.group(2))
    found = True
if not found:
    sys.exit(2)
print(total)
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

# 0 = synced (false), 1 = syncing, 2 = error / unreachable
eth_is_synced() {
  local raw rc
  raw="$(eth_syncing_raw)" && rc=0 || rc=$?
  if [[ "${rc}" -ne 0 ]]; then
    return 2
  fi
  if printf '%s' "${raw}" | grep -q '"result":false'; then
    return 0
  fi
  if printf '%s' "${raw}" | grep -qE '"result":(true|\{)'; then
    return 1
  fi
  return 2
}

# Snapshot of metrics used by all three assertions.
read_snapshot() {
  # Sets globals: SNAP_*
  local chain eng rc
  chain="$(scrape "${CHAIN_METRICS_URL}")"
  eng="$(scrape "${ENGINE_METRICS_URL}")"

  set +e
  SNAP_EL_OFFLINE="$(prom_value "${eng}" "cc_engine_el_offline" "")"
  SNAP_IS_OPT="$(prom_value "${chain}" "cc_chain_is_optimistic" "")"
  SNAP_OPT_NODES="$(prom_value "${chain}" "cc_chain_optimistic_nodes" "")"
  SNAP_VALIDATED="$(prom_value "${chain}" "cc_chain_optimistic_transitions_total" 'direction="validated"')"
  SNAP_HEAD_SLOT="$(prom_value "${chain}" "cc_chain_head_slot" "")"
  SNAP_NP_COUNT="$(prom_value "${eng}" "cc_engine_request_seconds_count" 'method="newPayloadV4"')"
  SNAP_FCU_COUNT="$(prom_value "${eng}" "cc_engine_request_seconds_count" 'method="forkchoiceUpdatedV3"')"
  SNAP_FCU_VALID="$(prom_value "${eng}" "cc_engine_payload_status_total" 'method="forkchoiceUpdatedV3".*status="VALID"|status="VALID".*method="forkchoiceUpdatedV3"')"
  SNAP_STATE_OFFLINE="$(prom_value "${eng}" "cc_engine_state" 'state="offline"')"
  SNAP_STATE_SYNCING="$(prom_value "${eng}" "cc_engine_state" 'state="syncing"')"
  SNAP_STATE_SYNCED="$(prom_value "${eng}" "cc_engine_state" 'state="synced"')"
  SNAP_STATE_AUTH="$(prom_value "${eng}" "cc_engine_state" 'state="auth_failed"')"
  SNAP_CAP_NP="$(prom_value "${eng}" "cc_engine_capability_missing" 'method="newPayloadV4"')"
  SNAP_CAP_FCU="$(prom_value "${eng}" "cc_engine_capability_missing" 'method="forkchoiceUpdatedV3"')"
  SNAP_CAP_EXC="$(prom_value "${eng}" "cc_engine_capability_missing" 'method="exchangeCapabilities"')"
  # worker panics: may be absent until engine supervisor lands the family
  SNAP_PANICS="$(prom_sum "${eng}" "cc_engine_worker_panics_total" "")"
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 ]]; then
    SNAP_PANICS=""
  fi

  # Default missing numeric fields to empty for callers to detect.
  : "${SNAP_EL_OFFLINE:=}"
  : "${SNAP_IS_OPT:=}"
  : "${SNAP_OPT_NODES:=}"
  : "${SNAP_VALIDATED:=}"
  : "${SNAP_HEAD_SLOT:=}"
  : "${SNAP_NP_COUNT:=}"
  : "${SNAP_FCU_COUNT:=}"
  : "${SNAP_FCU_VALID:=}"
}

# ── evaluate one shape from a result dict (shared live + fixture + self-test) ─
# Inputs via env-style globals set by caller; prints PASS/FAIL to stdout and
# returns 0/1. Side-effect: fills EVAL_* summary fields for harness emission.
evaluate_shape() {
  local shape="$1"
  # Required caller-provided:
  #   OUTAGE_DURATION, OUTAGE_BLOCK_COUNT
  #   EL_OFFLINE_THROUGHOUT (0/1), IS_OPT_THROUGHOUT (0/1), OPT_NODES_MIN
  #   HEAD_ADVANCED_DURING (0/1)
  #   ETH_SYNC_FALSE_UNIX, FCU_EMIT_UNIX
  #   VALIDATED_BEFORE, VALIDATED_AFTER
  #   OPT_NODES_AFTER
  #   NP_BEFORE, NP_AFTER, NEW_BLOCKS_IN_INTERVAL
  #   AUTH_FAILED_SEEN (0/1)
  #   CAP_PRE_SUM, CAP_POST_SUM
  #   PANICS_BEFORE, PANICS_AFTER (may be empty)
  #   STATES_SEEN (comma-separated)

  EVAL_STATUS="PASS"
  EVAL_FAILS=()

  if [[ "${EL_OFFLINE_THROUGHOUT}" != "1" ]]; then
    EVAL_FAILS+=("el_offline not 1 throughout outage")
  fi
  if [[ "${IS_OPT_THROUGHOUT}" != "1" ]]; then
    EVAL_FAILS+=("is_optimistic not 1 throughout outage")
  fi
  if ! awk -v n="${OPT_NODES_MIN}" 'BEGIN{exit !(n+0 > 0)}'; then
    EVAL_FAILS+=("optimistic_nodes never > 0 during outage (min=${OPT_NODES_MIN})")
  fi
  if [[ "${HEAD_ADVANCED_DURING}" != "1" ]]; then
    EVAL_FAILS+=("head did not advance during outage (imports must continue)")
  fi

  local fcu_elapsed
  if [[ -z "${ETH_SYNC_FALSE_UNIX}" || -z "${FCU_EMIT_UNIX}" ]]; then
    EVAL_FAILS+=("missing eth_syncing==false or fcU emission timestamp")
    fcu_elapsed=""
  else
    fcu_elapsed=$(( FCU_EMIT_UNIX - ETH_SYNC_FALSE_UNIX ))
    if [[ "${fcu_elapsed}" -lt 0 ]]; then
      EVAL_FAILS+=("fcU timestamp before eth_syncing==false (${fcu_elapsed}s)")
    elif ! awk -v e="${fcu_elapsed}" -v s="${SLOT_SECONDS}" 'BEGIN{exit !(e+0 <= s+0)}'; then
      EVAL_FAILS+=("fcU not within one slot: elapsed=${fcu_elapsed}s slot=${SLOT_SECONDS}s")
    fi
  fi
  EVAL_FCU_ELAPSED="${fcu_elapsed}"

  local delta_val
  if [[ -z "${VALIDATED_BEFORE}" || -z "${VALIDATED_AFTER}" || -z "${OUTAGE_BLOCK_COUNT}" ]]; then
    EVAL_FAILS+=("missing validated transitions or outage_block_count")
    delta_val=""
  else
    delta_val="$(awk -v a="${VALIDATED_BEFORE}" -v b="${VALIDATED_AFTER}" 'BEGIN{printf "%.0f", b-a}')"
    if ! awk -v d="${delta_val}" -v o="${OUTAGE_BLOCK_COUNT}" 'BEGIN{exit !(d+0 == o+0)}'; then
      EVAL_FAILS+=("one-pass fail: transitions_validated delta=${delta_val} != outage_block_count=${OUTAGE_BLOCK_COUNT}")
    fi
  fi
  EVAL_TRANSITIONS_DELTA="${delta_val}"

  if ! awk -v n="${OPT_NODES_AFTER}" 'BEGIN{exit !(n+0 == 0)}'; then
    EVAL_FAILS+=("optimistic_nodes after clear = ${OPT_NODES_AFTER}, want 0")
  fi

  local np_delta
  if [[ -z "${NP_BEFORE}" || -z "${NP_AFTER}" || -z "${NEW_BLOCKS_IN_INTERVAL}" ]]; then
    EVAL_FAILS+=("missing newPayloadV4 request counts or new_blocks_in_interval")
    np_delta=""
  else
    np_delta="$(awk -v a="${NP_BEFORE}" -v b="${NP_AFTER}" 'BEGIN{printf "%.0f", b-a}')"
    if ! awk -v d="${np_delta}" -v n="${NEW_BLOCKS_IN_INTERVAL}" 'BEGIN{exit !(d+0 == n+0)}'; then
      EVAL_FAILS+=("re-submission: ΔnewPayloadV4=${np_delta} != new_blocks_in_interval=${NEW_BLOCKS_IN_INTERVAL}")
    fi
  fi
  EVAL_NP_DELTA="${np_delta}"

  if [[ "${AUTH_FAILED_SEEN}" == "1" ]]; then
    EVAL_FAILS+=("cc_engine_state auth_failed observed (JWT must be unchanged)")
  fi

  if [[ -n "${CAP_PRE_SUM}" && -n "${CAP_POST_SUM}" ]]; then
    if ! awk -v a="${CAP_PRE_SUM}" -v b="${CAP_POST_SUM}" 'BEGIN{exit !(a+0 == b+0)}'; then
      EVAL_FAILS+=("capability_missing pre=${CAP_PRE_SUM} post=${CAP_POST_SUM} (must re-populate to pre-outage)")
    fi
  fi

  EVAL_PANICS_DELTA=""
  if [[ -n "${PANICS_BEFORE}" && -n "${PANICS_AFTER}" ]]; then
    EVAL_PANICS_DELTA="$(awk -v a="${PANICS_BEFORE}" -v b="${PANICS_AFTER}" 'BEGIN{printf "%.0f", b-a}')"
    if ! awk -v d="${EVAL_PANICS_DELTA}" 'BEGIN{exit !(d+0 == 0)}'; then
      # Not a void — recorded as P0 against §2.5; still FAIL the drill row.
      EVAL_FAILS+=("P0: cc_engine_worker_panics_total increased by ${EVAL_PANICS_DELTA}")
    fi
  fi

  if [[ "${#EVAL_FAILS[@]}" -gt 0 ]]; then
    EVAL_STATUS="FAIL"
    log "${shape}: FAIL — ${EVAL_FAILS[*]}"
    return 1
  fi
  log "${shape}: PASS — outage=${OUTAGE_DURATION}s blocks=${OUTAGE_BLOCK_COUNT} fcu_elapsed=${fcu_elapsed}s Δvalidated=${delta_val} Δnp=${np_delta}"
  return 0
}

# ── live restart of one shape ───────────────────────────────────────────────
resolve_el_container() {
  local cid
  cid="$(compose ps -q "${EL_SERVICE}" 2>/dev/null || true)"
  if [[ -z "${cid}" ]]; then
    refuse "no running container for compose service '${EL_SERVICE}' in ${COMPOSE_DIR}"
  fi
  printf '%s' "${cid}"
}

induce_outage() {
  local shape="$1"
  case "${shape}" in
    clean)
      log "inducing CLEAN outage: docker compose restart ${EL_SERVICE}"
      compose restart "${EL_SERVICE}"
      ;;
    unclean)
      local cid
      cid="$(resolve_el_container)"
      log "inducing UNCLEAN outage: docker kill -s KILL ${cid} && docker compose up -d ${EL_SERVICE}"
      # Issue wording uses the service name; compose project prefixes the real
      # container id. Resolve via compose ps -q so kill targets the right box.
      docker kill -s KILL "${cid}"
      compose up -d "${EL_SERVICE}"
      ;;
    *)
      die "unknown shape: ${shape}"
      ;;
  esac
}

run_live_shape() {
  local shape="$1"
  log "── live shape: ${shape} ──"

  # Pre-outage baseline
  read_snapshot
  local pre_head="${SNAP_HEAD_SLOT}"
  local pre_cap_np="${SNAP_CAP_NP:-0}"
  local pre_cap_fcu="${SNAP_CAP_FCU:-0}"
  local pre_cap_exc="${SNAP_CAP_EXC:-0}"
  local pre_cap_sum
  pre_cap_sum="$(awk -v a="${pre_cap_np}" -v b="${pre_cap_fcu}" -v c="${pre_cap_exc}" 'BEGIN{print a+b+c}')"
  local pre_panics="${SNAP_PANICS}"

  local t0 t0_unix
  t0="$(utc_now)"
  t0_unix="$(unix_now)"

  induce_outage "${shape}"

  # Wait for el_offline==1 (or offline state)
  local deadline seen_offline=0
  deadline=$(( t0_unix + OUTAGE_WAIT ))
  while [[ "$(unix_now)" -lt "${deadline}" ]]; do
    read_snapshot
    if awk -v v="${SNAP_EL_OFFLINE}" 'BEGIN{exit !(v+0 == 1)}' 2>/dev/null; then
      seen_offline=1
      break
    fi
    if awk -v v="${SNAP_STATE_OFFLINE}" 'BEGIN{exit !(v+0 == 1)}' 2>/dev/null; then
      seen_offline=1
      break
    fi
    sleep "${POLL_SECONDS}"
  done
  if [[ "${seen_offline}" -ne 1 ]]; then
    log "warning: el_offline never observed within ${OUTAGE_WAIT}s — continuing; assertion will FAIL"
  fi

  # Sample throughout the outage until EL is answering again (or min hold).
  local el_offline_throughout=1 is_opt_throughout=1
  local opt_nodes_min=0 head_max="${pre_head:-0}"
  local auth_failed_seen=0
  local states_seen=()
  local outage_end_unix
  local min_end=$(( t0_unix + MIN_OUTAGE ))

  # Phase A of observation: while offline / not synced
  while true; do
    read_snapshot
    local now
    now="$(unix_now)"

    if [[ -n "${SNAP_EL_OFFLINE}" ]] && ! awk -v v="${SNAP_EL_OFFLINE}" 'BEGIN{exit !(v+0 == 1)}'; then
      # EL reported online from engine's perspective — outage window closing.
      if [[ "${now}" -ge "${min_end}" ]]; then
        outage_end_unix="${now}"
        break
      fi
    fi

    if [[ -n "${SNAP_EL_OFFLINE}" ]] && ! awk -v v="${SNAP_EL_OFFLINE}" 'BEGIN{exit !(v+0 == 1)}'; then
      el_offline_throughout=0
    fi
    if [[ -n "${SNAP_IS_OPT}" ]] && ! awk -v v="${SNAP_IS_OPT}" 'BEGIN{exit !(v+0 == 1)}'; then
      # Only require optimistic once head has moved past pre; before first import
      # is_optimistic may still be 0 for a moment.
      if awk -v h="${SNAP_HEAD_SLOT}" -v p="${pre_head:-0}" 'BEGIN{exit !(h+0 > p+0)}'; then
        is_opt_throughout=0
      fi
    fi
    if [[ -n "${SNAP_OPT_NODES}" ]]; then
      if awk -v a="${SNAP_OPT_NODES}" -v b="${opt_nodes_min}" 'BEGIN{exit !(a+0 > b+0)}'; then
        opt_nodes_min="${SNAP_OPT_NODES}"
      fi
    fi
    if [[ -n "${SNAP_HEAD_SLOT}" ]]; then
      if awk -v a="${SNAP_HEAD_SLOT}" -v b="${head_max}" 'BEGIN{exit !(a+0 > b+0)}'; then
        head_max="${SNAP_HEAD_SLOT}"
      fi
    fi
    if awk -v v="${SNAP_STATE_AUTH:-0}" 'BEGIN{exit !(v+0 == 1)}' 2>/dev/null; then
      auth_failed_seen=1
    fi
    if awk -v v="${SNAP_STATE_OFFLINE:-0}" 'BEGIN{exit !(v+0 == 1)}' 2>/dev/null; then
      states_seen+=("offline")
    fi
    if awk -v v="${SNAP_STATE_SYNCING:-0}" 'BEGIN{exit !(v+0 == 1)}' 2>/dev/null; then
      states_seen+=("syncing")
    fi
    if awk -v v="${SNAP_STATE_SYNCED:-0}" 'BEGIN{exit !(v+0 == 1)}' 2>/dev/null; then
      states_seen+=("synced")
    fi

    # Hard stop if outage exceeds resync wait without recovery signal.
    if [[ "${now}" -gt $(( t0_unix + RESYNC_WAIT )) ]]; then
      outage_end_unix="${now}"
      log "warning: resync wait exceeded without clean online edge"
      break
    fi
    sleep "${POLL_SECONDS}"
  done
  : "${outage_end_unix:=$(unix_now)}"

  # Wait for eth_syncing == false
  local eth_sync_false_unix="" eth_sync_false_utc=""
  deadline=$(( $(unix_now) + RESYNC_WAIT ))
  while [[ "$(unix_now)" -lt "${deadline}" ]]; do
    set +e
    eth_is_synced
    local sync_rc=$?
    set -e
    if [[ "${sync_rc}" -eq 0 ]]; then
      eth_sync_false_unix="$(unix_now)"
      eth_sync_false_utc="$(utc_now)"
      log "eth_syncing==false at ${eth_sync_false_utc} (${eth_sync_false_unix})"
      break
    fi
    sleep "${POLL_SECONDS}"
  done
  if [[ -z "${eth_sync_false_unix}" ]]; then
    log "warning: eth_syncing never returned false within ${RESYNC_WAIT}s"
  fi

  # Capture newPayload / fcu / validated immediately before the VALID clear window
  read_snapshot
  local np_before_clear="${SNAP_NP_COUNT}"
  local validated_before_clear="${SNAP_VALIDATED}"
  local head_before_clear="${SNAP_HEAD_SLOT}"
  local fcu_before="${SNAP_FCU_COUNT}"
  local fcu_valid_before="${SNAP_FCU_VALID:-0}"

  # Wait for fcU emission after sync gate (count or VALID status bump)
  local fcu_emit_unix="" fcu_emit_utc=""
  deadline=$(( $(unix_now) + SLOT_SECONDS * 3 ))
  while [[ "$(unix_now)" -lt "${deadline}" ]]; do
    read_snapshot
    if [[ -n "${SNAP_FCU_COUNT}" && -n "${fcu_before}" ]]; then
      if awk -v a="${fcu_before}" -v b="${SNAP_FCU_COUNT}" 'BEGIN{exit !(b+0 > a+0)}'; then
        fcu_emit_unix="$(unix_now)"
        fcu_emit_utc="$(utc_now)"
        log "fcU request count advanced at ${fcu_emit_utc}"
        break
      fi
    fi
    if [[ -n "${SNAP_FCU_VALID}" && -n "${fcu_valid_before}" ]]; then
      if awk -v a="${fcu_valid_before}" -v b="${SNAP_FCU_VALID}" 'BEGIN{exit !(b+0 > a+0)}'; then
        fcu_emit_unix="$(unix_now)"
        fcu_emit_utc="$(utc_now)"
        log "fcU VALID status advanced at ${fcu_emit_utc}"
        break
      fi
    fi
    sleep "${POLL_SECONDS}"
  done

  # Wait for optimistic clear (nodes → 0) after VALID ancestor pass
  local validated_after="" opt_nodes_after="" np_after="" head_after_clear=""
  deadline=$(( $(unix_now) + CLEAR_WAIT ))
  while [[ "$(unix_now)" -lt "${deadline}" ]]; do
    read_snapshot
    validated_after="${SNAP_VALIDATED}"
    opt_nodes_after="${SNAP_OPT_NODES}"
    np_after="${SNAP_NP_COUNT}"
    head_after_clear="${SNAP_HEAD_SLOT}"
    if awk -v n="${opt_nodes_after}" 'BEGIN{exit !(n+0 == 0)}' \
      && awk -v a="${validated_before_clear}" -v b="${validated_after}" 'BEGIN{exit !(b+0 > a+0)}'; then
      break
    fi
    sleep "${POLL_SECONDS}"
  done

  read_snapshot
  validated_after="${SNAP_VALIDATED}"
  opt_nodes_after="${SNAP_OPT_NODES}"
  np_after="${SNAP_NP_COUNT}"
  head_after_clear="${SNAP_HEAD_SLOT}"
  local post_cap_np="${SNAP_CAP_NP:-0}"
  local post_cap_fcu="${SNAP_CAP_FCU:-0}"
  local post_cap_exc="${SNAP_CAP_EXC:-0}"
  local post_cap_sum
  post_cap_sum="$(awk -v a="${post_cap_np}" -v b="${post_cap_fcu}" -v c="${post_cap_exc}" 'BEGIN{print a+b+c}')"
  local post_panics="${SNAP_PANICS}"

  # Outage block count = head advance during outage (optimistic imports).
  # Prefer max optimistic_nodes during outage when head metric is sticky; else Δ head.
  local outage_block_count
  if awk -v n="${opt_nodes_min}" 'BEGIN{exit !(n+0 > 0)}'; then
    # Peak optimistic occupancy is the set cleared by one VALID pass.
    outage_block_count="$(awk -v n="${opt_nodes_min}" 'BEGIN{printf "%.0f", n}')"
  else
    outage_block_count="$(awk -v a="${pre_head:-0}" -v b="${head_max}" 'BEGIN{printf "%.0f", (b>a?b-a:0)}')"
  fi

  local new_blocks_in_interval
  new_blocks_in_interval="$(awk -v a="${head_before_clear:-0}" -v b="${head_after_clear:-0}" 'BEGIN{printf "%.0f", (b>a?b-a:0)}')"

  # Unique states seen
  local states_csv
  states_csv="$(printf '%s\n' "${states_seen[@]:-}" | awk 'NF && !seen[$0]++ {printf sep $0; sep=","}')"

  local outage_duration=$(( outage_end_unix - t0_unix ))
  local head_advanced=0
  if awk -v a="${pre_head:-0}" -v b="${head_max}" 'BEGIN{exit !(b+0 > a+0)}'; then
    head_advanced=1
  fi

  # Bind evaluate_shape inputs
  OUTAGE_DURATION="${outage_duration}"
  OUTAGE_BLOCK_COUNT="${outage_block_count}"
  EL_OFFLINE_THROUGHOUT="${el_offline_throughout}"
  IS_OPT_THROUGHOUT="${is_opt_throughout}"
  OPT_NODES_MIN="${opt_nodes_min}"
  HEAD_ADVANCED_DURING="${head_advanced}"
  ETH_SYNC_FALSE_UNIX="${eth_sync_false_unix}"
  FCU_EMIT_UNIX="${fcu_emit_unix}"
  VALIDATED_BEFORE="${validated_before_clear}"
  VALIDATED_AFTER="${validated_after}"
  OPT_NODES_AFTER="${opt_nodes_after}"
  NP_BEFORE="${np_before_clear}"
  NP_AFTER="${np_after}"
  NEW_BLOCKS_IN_INTERVAL="${new_blocks_in_interval}"
  AUTH_FAILED_SEEN="${auth_failed_seen}"
  CAP_PRE_SUM="${pre_cap_sum}"
  CAP_POST_SUM="${post_cap_sum}"
  PANICS_BEFORE="${pre_panics}"
  PANICS_AFTER="${post_panics}"

  set +e
  evaluate_shape "${shape}"
  local ev_rc=$?
  set -e

  # Build final shape JSON for harness emission.
  SHAPE_RESULT_JSON="$(
    SHAPE="${shape}" \
    STATUS="${EVAL_STATUS}" \
    T0="${t0}" T0U="${t0_unix}" OD="${outage_duration}" OBC="${outage_block_count}" \
    ELO="${el_offline_throughout}" ISO="${is_opt_throughout}" ONMIN="${opt_nodes_min}" \
    HADV="${head_advanced}" \
    ESFU="${eth_sync_false_utc}" ESFX="${eth_sync_false_unix}" \
    FCUU="${fcu_emit_utc}" FCUX="${fcu_emit_unix}" \
    FCUE="${EVAL_FCU_ELAPSED}" SLOT="${SLOT_SECONDS}" \
    VB="${validated_before_clear}" VA="${validated_after}" TD="${EVAL_TRANSITIONS_DELTA}" \
    ONA="${opt_nodes_after}" \
    NPB="${np_before_clear}" NPA="${np_after}" NPD="${EVAL_NP_DELTA}" NBI="${new_blocks_in_interval}" \
    STATES="${states_csv}" AUTH="${auth_failed_seen}" \
    CPRE="${pre_cap_sum}" CPOST="${post_cap_sum}" \
    PB="${pre_panics}" PA="${post_panics}" PD="${EVAL_PANICS_DELTA}" \
    FAILS="$(printf '%s\n' "${EVAL_FAILS[@]:-}")" \
    python3 - <<'PY'
import json, os
def num(k):
    v = os.environ.get(k, "")
    if v == "" or v is None:
        return None
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except ValueError:
        return None
def b01(k):
    return os.environ.get(k, "0") in ("1", "true", "True")
fails = [ln for ln in os.environ.get("FAILS", "").splitlines() if ln.strip()]
slot = float(os.environ.get("SLOT", "12") or 12)
fcu_elapsed = num("FCUE")
shape = os.environ["SHAPE"]
cmd = (
    "docker compose restart el" if shape == "clean"
    else "docker kill -s KILL el && docker compose up -d el"
)
obc = num("OBC") or 0
td = num("TD")
npd = num("NPD")
nbi = num("NBI") or 0
out = {
  "shape": shape,
  "command": cmd,
  "status": os.environ.get("STATUS", "FAIL"),
  "outage_start_utc": os.environ.get("T0"),
  "outage_start_unix": num("T0U"),
  "outage_duration_s": num("OD"),
  "outage_block_count": obc,
  "el_offline_during": b01("ELO"),
  "is_optimistic_during": b01("ISO"),
  "optimistic_nodes_min": num("ONMIN") or 0,
  "head_advanced_during": b01("HADV"),
  "eth_syncing_false_utc": os.environ.get("ESFU") or None,
  "eth_syncing_false_unix": num("ESFX"),
  "fcu_emission_utc": os.environ.get("FCUU") or None,
  "fcu_emission_unix": num("FCUX"),
  "fcu_elapsed_s": fcu_elapsed,
  "fcu_slots_after_sync": (fcu_elapsed / slot) if fcu_elapsed is not None else None,
  "fcu_within_one_slot": (fcu_elapsed is not None and fcu_elapsed <= slot),
  "transitions_validated_before": num("VB"),
  "transitions_validated_after": num("VA"),
  "transitions_delta": td,
  "one_pass_exact": (td is not None and td == obc),
  "optimistic_nodes_after": num("ONA"),
  "optimistic_cleared_single_valid": (
    num("ONA") == 0 and td is not None and td == obc
  ),
  "newpayload_v4_before": num("NPB"),
  "newpayload_v4_after": num("NPA"),
  "newpayload_delta": npd,
  "new_blocks_in_interval": nbi,
  "no_payload_resubmission": (npd is not None and npd == nbi),
  "states_seen": [s for s in os.environ.get("STATES", "").split(",") if s],
  "auth_failed_seen": b01("AUTH"),
  "capability_missing_pre_sum": num("CPRE"),
  "capability_missing_post_sum": num("CPOST"),
  "capability_repopulated": num("CPRE") == num("CPOST"),
  "worker_panics_before": num("PB"),
  "worker_panics_after": num("PA"),
  "worker_panics_delta": num("PD"),
  "worker_panics_metric": ("present" if os.environ.get("PB", "") != "" else "ABSENT"),
  "fails": fails,
  "notes": "",
}
print(json.dumps(out))
PY
  )"

  case "${shape}" in
    clean) RESULT_CLEAN="${SHAPE_RESULT_JSON}" ;;
    unclean) RESULT_UNCLEAN="${SHAPE_RESULT_JSON}" ;;
  esac
  return "${ev_rc}"
}

# ── harness writer ──────────────────────────────────────────────────────────
write_harness() {
  local clean_json="${1:-}"
  local unclean_json="${2:-}"
  local overall="${3:-NOT_RUN}"
  local reason="${4:-}"
  local fresh_utc="${5:-}"
  local fresh_unix="${6:-}"
  mkdir -p "$(dirname "${OUT}")"
  CLEAN_JSON="${clean_json}" UNCLEAN_JSON="${unclean_json}" \
  OVERALL="${overall}" REASON="${reason}" \
  FRESH_UTC="${fresh_utc}" FRESH_UNIX="${fresh_unix}" \
  SLOT="${SLOT_SECONDS}" OUT_PATH="${OUT}" \
  python3 - <<'PY'
import json, os
from pathlib import Path

def load(s):
    s = (s or "").strip()
    if not s:
        return None
    return json.loads(s)

clean = load(os.environ.get("CLEAN_JSON"))
unclean = load(os.environ.get("UNCLEAN_JSON"))
reason = os.environ.get("REASON") or ""
fresh_utc = os.environ.get("FRESH_UTC") or None
fresh_unix = os.environ.get("FRESH_UNIX") or None
if fresh_unix == "":
    fresh_unix = None
elif fresh_unix is not None:
    try:
        fresh_unix = int(fresh_unix)
    except ValueError:
        fresh_unix = None

# Build soak-report clause2 shape (minimal keys + full drill record).
def shape_block(full):
    if full is None:
        return None
    return {
        # soak-report.sh CC-3Ab keys:
        "el_offline_during": full.get("el_offline_during"),
        "is_optimistic_during": full.get("is_optimistic_during"),
        "fcu_slots_after_sync": full.get("fcu_slots_after_sync"),
        "optimistic_cleared_single_valid": full.get("optimistic_cleared_single_valid"),
        "no_payload_resubmission": full.get("no_payload_resubmission"),
        # CC-36b extended measurement (soak-report prints when present):
        **full,
    }

clause2 = {}
if clean is not None:
    clause2["clean"] = shape_block(clean)
if unclean is not None:
    clause2["unclean"] = shape_block(unclean)

doc = {
    "venue": "local compose + EL",
    "clause": 2,
    "slot_seconds": int(float(os.environ.get("SLOT", "12"))),
    "overall_status": os.environ.get("OVERALL", "NOT_RUN"),
    "not_run_reason": reason or None,
    "clause2": clause2,
    "d11": {
        "drills_before_clause1_window": True,
        "fresh_eth_syncing_false_after_unclean_utc": fresh_utc,
        "fresh_eth_syncing_false_after_unclean_unix": fresh_unix,
        "note": (
            "D-11: both drills complete before clause 1's window opens. "
            "The fresh eth_syncing==false timestamp after the unclean shape "
            "re-acquired is what CC-3Ac's window starts from."
        ),
    },
    "generator": "scripts/el-restart-drills.sh",
    "issue": "CC-36b",
}
path = Path(os.environ["OUT_PATH"])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(doc, indent=2) + "\n")
print(f"wrote {path}", flush=True)
PY
  log "harness written to ${OUT}"
}

emit_not_run_harness() {
  local reason="${1:-full EL restart venue unavailable}"
  local clean unclean
  clean="$(python3 - <<PY
import json
print(json.dumps({
  "shape": "clean",
  "command": "docker compose restart el",
  "status": "NOT_RUN",
  "outage_duration_s": None,
  "outage_block_count": None,
  "el_offline_during": None,
  "is_optimistic_during": None,
  "fcu_slots_after_sync": None,
  "optimistic_cleared_single_valid": None,
  "no_payload_resubmission": None,
  "notes": """${reason}""",
  "fails": [],
}))
PY
)"
  unclean="$(python3 - <<PY
import json
print(json.dumps({
  "shape": "unclean",
  "command": "docker kill -s KILL el && docker compose up -d el",
  "status": "NOT_RUN",
  "outage_duration_s": None,
  "outage_block_count": None,
  "el_offline_during": None,
  "is_optimistic_during": None,
  "fcu_slots_after_sync": None,
  "optimistic_cleared_single_valid": None,
  "no_payload_resubmission": None,
  "notes": """${reason}""",
  "fails": [],
}))
PY
)"
  write_harness "${clean}" "${unclean}" "NOT_RUN" "${reason}" "" ""
}

# ── prerequisites ───────────────────────────────────────────────────────────
check_prereqs() {
  local ok=1
  command -v docker >/dev/null || { log "missing: docker"; ok=0; }
  command -v curl >/dev/null || { log "missing: curl"; ok=0; }
  command -v python3 >/dev/null || { log "missing: python3"; ok=0; }

  if ! compose ps --services 2>/dev/null | grep -qx "${EL_SERVICE}"; then
    log "compose service '${EL_SERVICE}' not defined/running under ${COMPOSE_DIR}"
    ok=0
  fi

  set +e
  eth_is_synced
  local sync_rc=$?
  set -e
  case "${sync_rc}" in
    0) log "eth_syncing == false at ${EL_HTTP} (synced)" ;;
    1) log "eth_syncing != false at ${EL_HTTP} (still syncing)"
       if [[ "${REQUIRE_SYNCED}" -eq 1 ]]; then ok=0; fi
       ;;
    2) log "cannot reach eth_syncing at ${EL_HTTP}"
       ok=0
       ;;
  esac

  if ! curl -fsS --max-time 5 "${ENGINE_METRICS_URL}" >/dev/null 2>&1; then
    log "engine metrics unreachable: ${ENGINE_METRICS_URL}"
    ok=0
  else
    log "engine metrics ok: ${ENGINE_METRICS_URL}"
  fi
  if ! curl -fsS --max-time 5 "${CHAIN_METRICS_URL}" >/dev/null 2>&1; then
    log "chain metrics unreachable: ${CHAIN_METRICS_URL}"
    ok=0
  else
    log "chain metrics ok: ${CHAIN_METRICS_URL}"
  fi

  # Foreign stack guard: warn if other compose projects expose the same ports.
  local names
  names="$(docker ps --format '{{.Names}}' 2>/dev/null || true)"
  if printf '%s\n' "${names}" | grep -qvE "^(${COMPOSE_PROJECT:-cc}|$)"; then
    log "docker ps names present: $(printf '%s' "${names}" | tr '\n' ' ')"
    log "note: clause 2 requires machine exclusivity (D-11) for a discharging run"
  fi

  if [[ "${ok}" -ne 1 ]]; then
    refuse "prerequisites not met for live EL restart drills"
  fi
  log "prerequisites OK"
  return 0
}

# ── self-test (offline fixtures; no docker) ─────────────────────────────────
run_self_test() {
  local tmp
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/cc-el-restart-drills.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -rf '${tmp}'" EXIT

  log "self-test: evaluate_shape PASS path (clean)"
  OUTAGE_DURATION=30
  OUTAGE_BLOCK_COUNT=5
  EL_OFFLINE_THROUGHOUT=1
  IS_OPT_THROUGHOUT=1
  OPT_NODES_MIN=5
  HEAD_ADVANCED_DURING=1
  ETH_SYNC_FALSE_UNIX=1000
  FCU_EMIT_UNIX=1004   # 4 s < 12 s slot
  VALIDATED_BEFORE=10
  VALIDATED_AFTER=15   # delta 5 == outage_block_count
  OPT_NODES_AFTER=0
  NP_BEFORE=100
  NP_AFTER=102         # 2 new blocks arrived in clear interval
  NEW_BLOCKS_IN_INTERVAL=2
  AUTH_FAILED_SEEN=0
  CAP_PRE_SUM=0
  CAP_POST_SUM=0
  PANICS_BEFORE=0
  PANICS_AFTER=0
  evaluate_shape clean || die "self-test: expected PASS for clean fixture"

  log "self-test: evaluate_shape FAIL on one-pass mismatch"
  VALIDATED_AFTER=14   # delta 4 != 5
  set +e
  evaluate_shape unclean
  local rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on transitions mismatch"
  VALIDATED_AFTER=15

  log "self-test: evaluate_shape FAIL on fcU late"
  FCU_EMIT_UNIX=1020   # 20 s > 12 s
  set +e
  evaluate_shape unclean
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on late fcU"
  FCU_EMIT_UNIX=1004

  log "self-test: evaluate_shape FAIL on re-submission"
  NP_AFTER=105         # delta 5 != 2 new blocks
  set +e
  evaluate_shape unclean
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on newPayload re-submission"
  NP_AFTER=102

  log "self-test: evaluate_shape FAIL on auth_failed"
  AUTH_FAILED_SEEN=1
  set +e
  evaluate_shape unclean
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL when auth_failed seen"
  AUTH_FAILED_SEEN=0

  log "self-test: emit harness with PASS shapes and feed soak-report"
  local clean_json unclean_json
  clean_json="$(python3 - <<'PY'
import json
print(json.dumps({
  "shape": "clean",
  "command": "docker compose restart el",
  "status": "PASS",
  "outage_duration_s": 30,
  "outage_block_count": 5,
  "el_offline_during": True,
  "is_optimistic_during": True,
  "optimistic_nodes_min": 5,
  "head_advanced_during": True,
  "eth_syncing_false_utc": "2026-08-08T00:00:10Z",
  "eth_syncing_false_unix": 1000,
  "fcu_emission_utc": "2026-08-08T00:00:14Z",
  "fcu_emission_unix": 1004,
  "fcu_elapsed_s": 4,
  "fcu_slots_after_sync": 4/12,
  "fcu_within_one_slot": True,
  "transitions_validated_before": 10,
  "transitions_validated_after": 15,
  "transitions_delta": 5,
  "one_pass_exact": True,
  "optimistic_nodes_after": 0,
  "optimistic_cleared_single_valid": True,
  "newpayload_v4_before": 100,
  "newpayload_v4_after": 102,
  "newpayload_delta": 2,
  "new_blocks_in_interval": 2,
  "no_payload_resubmission": True,
  "states_seen": ["offline", "syncing", "synced"],
  "auth_failed_seen": False,
  "capability_missing_pre_sum": 0,
  "capability_missing_post_sum": 0,
  "capability_repopulated": True,
  "worker_panics_before": 0,
  "worker_panics_after": 0,
  "worker_panics_delta": 0,
  "worker_panics_metric": "ABSENT",
  "fails": [],
  "notes": "self-test fixture",
}))
PY
)"
  unclean_json="$(python3 - <<'PY'
import json
print(json.dumps({
  "shape": "unclean",
  "command": "docker kill -s KILL el && docker compose up -d el",
  "status": "PASS",
  "outage_duration_s": 45,
  "outage_block_count": 7,
  "el_offline_during": True,
  "is_optimistic_during": True,
  "optimistic_nodes_min": 7,
  "head_advanced_during": True,
  "eth_syncing_false_utc": "2026-08-08T00:10:00Z",
  "eth_syncing_false_unix": 1600,
  "fcu_emission_utc": "2026-08-08T00:10:08Z",
  "fcu_emission_unix": 1608,
  "fcu_elapsed_s": 8,
  "fcu_slots_after_sync": 8/12,
  "fcu_within_one_slot": True,
  "transitions_validated_before": 20,
  "transitions_validated_after": 27,
  "transitions_delta": 7,
  "one_pass_exact": True,
  "optimistic_nodes_after": 0,
  "optimistic_cleared_single_valid": True,
  "newpayload_v4_before": 200,
  "newpayload_v4_after": 201,
  "newpayload_delta": 1,
  "new_blocks_in_interval": 1,
  "no_payload_resubmission": True,
  "states_seen": ["offline", "syncing", "synced"],
  "auth_failed_seen": False,
  "capability_missing_pre_sum": 0,
  "capability_missing_post_sum": 0,
  "capability_repopulated": True,
  "worker_panics_before": 0,
  "worker_panics_after": 0,
  "worker_panics_delta": 0,
  "worker_panics_metric": "ABSENT",
  "fails": [],
  "notes": "self-test fixture",
}))
PY
)"
  OUT="${tmp}/harness.json"
  write_harness "${clean_json}" "${unclean_json}" "PASS" "" \
    "2026-08-08T00:10:00Z" "1600"

  [[ -f "${OUT}" ]] || die "self-test: harness not written"
  grep -q '"clean"' "${OUT}" || die "self-test: clean shape missing"
  grep -q '"unclean"' "${OUT}" || die "self-test: unclean shape missing"
  grep -q 'transitions_delta' "${OUT}" || die "self-test: transitions_delta missing"

  log "self-test: soak-report --phase 3 --clause 2 --venue 'local compose + EL'"
  local body
  set +e
  body="$(bash "${SCRIPT_DIR}/soak-report.sh" --phase 3 \
    --clause 2 \
    --venue 'local compose + EL' \
    --harness-json "${OUT}" 2>"${tmp}/sr.err")"
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 ]]; then
    cat "${tmp}/sr.err" >&2
    die "self-test: soak-report exit ${rc}"
  fi
  echo "${body}" | grep -Fq '2a · EL restart clean' \
    || { echo "${body}" >&2; die "self-test: clean row missing"; }
  echo "${body}" | grep -Fq '2b · EL restart unclean' \
    || { echo "${body}" >&2; die "self-test: unclean row missing"; }
  echo "${body}" | grep -Fq 'local compose + EL' \
    || die "self-test: venue missing"
  # Both rows PASS
  local pass_count
  pass_count="$(echo "${body}" | grep -E '2a ·|2b ·' | grep -c 'PASS' || true)"
  [[ "${pass_count}" -ge 2 ]] || { echo "${body}" >&2; die "self-test: expected PASS on both shapes, got ${pass_count}"; }

  log "self-test: --emit-not-run writes NOT_RUN harness"
  OUT="${tmp}/not_run.json"
  bash "$0" --emit-not-run --reason "self-test not_run" --out "${OUT}"
  grep -q 'NOT_RUN' "${OUT}" || die "self-test: NOT_RUN missing in harness"

  log "self-test PASS"
  exit 0
}

# ── main ────────────────────────────────────────────────────────────────────
RESULT_CLEAN=""
RESULT_UNCLEAN=""

if [[ "${SELF_TEST}" -eq 1 ]]; then
  run_self_test
fi

if [[ "${EMIT_NOT_RUN}" -eq 1 ]]; then
  reason="${NOT_RUN_REASON:-full EL restart venue unavailable}"
  emit_not_run_harness "${reason}"
  log "emitted NOT_RUN harness: ${reason}"
  exit 0
fi

if [[ "${CHECK_PREREQS}" -eq 1 ]]; then
  check_prereqs
  exit 0
fi

case "${SHAPE}" in
  clean|unclean|both) ;;
  *)
    echo "error: --shape must be clean, unclean, or both" >&2
    exit 2
    ;;
esac

# Fixture-dir path: not a full live orchestration — operator supplies snapshots
# sequenced by EL_DRILL_SAMPLE_IDX. For CC-36b the self-test covers offline
# assertion math; fixture-dir is a thin escape hatch.
if [[ -n "${FIXTURE_DIR}" ]]; then
  die "--from-fixture-dir live loop is not used; run --self-test for offline proof"
fi

check_prereqs

fail_count=0
case "${SHAPE}" in
  clean)
    set +e
    run_live_shape clean
    rc=$?
    set -e
    [[ "${rc}" -eq 0 ]] || fail_count=$((fail_count + 1))
    ;;
  unclean)
    set +e
    run_live_shape unclean
    rc=$?
    set -e
    [[ "${rc}" -eq 0 ]] || fail_count=$((fail_count + 1))
    ;;
  both)
    set +e
    run_live_shape clean
    rc=$?
    set -e
    [[ "${rc}" -eq 0 ]] || fail_count=$((fail_count + 1))
    # Brief settle between shapes so geth is stable before kill -9.
    sleep 5
    set +e
    run_live_shape unclean
    rc=$?
    set -e
    [[ "${rc}" -eq 0 ]] || fail_count=$((fail_count + 1))
    ;;
esac

# Fresh eth_syncing==false after unclean (D-11 window start for CC-3Ac)
fresh_utc=""
fresh_unix=""
set +e
eth_is_synced
sync_rc=$?
set -e
if [[ "${sync_rc}" -eq 0 ]]; then
  fresh_utc="$(utc_now)"
  fresh_unix="$(unix_now)"
  log "D-11 fresh eth_syncing==false after drills: ${fresh_utc}"
fi

overall="PASS"
if [[ "${fail_count}" -gt 0 ]]; then
  overall="FAIL"
fi
write_harness "${RESULT_CLEAN}" "${RESULT_UNCLEAN}" "${overall}" "" \
  "${fresh_utc}" "${fresh_unix}"

log "done overall=${overall} fail_count=${fail_count} harness=${OUT}"
log "report: bash scripts/soak-report.sh --phase 3 --clause 2 --venue 'local compose + EL' --harness-json ${OUT}"

if [[ "${fail_count}" -gt 0 ]]; then
  exit 4
fi
exit 0
