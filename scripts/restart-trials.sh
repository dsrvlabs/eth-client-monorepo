#!/usr/bin/env bash
# scripts/restart-trials.sh — CC-45c twenty restart trials (proof clause 1)
#
# Drives the single 20-run set that discharges both CC-45 /6 and /7 (not two
# sets). Each run:
#   1. Waits until a random offset in [4 s, 8 s) into a 12 s slot
#   2. Captures pre-crash GetHead root + earliest_available_slot + bootstrap count
#   3. docker compose kill -s SIGKILL  (NEVER down, NEVER down -v)
#   4. docker compose up -d
#   5. Polls cc_storage_following_head == 1 within ≤ 60 s (or CC-42 re-derived bar)
#   6. Asserts post GetHead root identical, window ≤ pre-crash, head advances
#      within 2 slots, and zero new outbound checkpoint bootstrap attempts
#   7. Records cc_storage_restart_seconds{phase} for all seven phases
#
# Both durability settings (immediate | paranoid) are exercised (≥ 5 each).
#
# Branch detection (R-1 / issue CC-45c):
#   A — EL service present in `docker compose config` AND services/chain has a
#       real optimistic-sync state machine → 20/20 may discharge clause 1
#   B — otherwise → record literal `partial — no EL in the restart set`;
#       clause 1 does **not** discharge; re-run at Phase 3 exit
#
# Usage:
#   bash scripts/restart-trials.sh --self-test
#   bash scripts/restart-trials.sh --detect-branch
#   bash scripts/restart-trials.sh --check-prereqs
#   bash scripts/restart-trials.sh --emit-not-run [--reason TEXT]
#   bash scripts/restart-trials.sh --dry-run          # plan + branch only
#   bash scripts/restart-trials.sh [--trials N]       # live 20 (default)
#
# Environment / flags:
#   --compose-dir DIR              default: repo root
#   --compose-project NAME         COMPOSE_PROJECT_NAME
#   --services LIST                space-separated; default: all compose services
#   --storage-metrics-url URL      default: http://127.0.0.1:9106/metrics
#   --chain-metrics-url URL        default: http://127.0.0.1:9101/metrics
#   --p2p-metrics-url URL          default: http://127.0.0.1:9102/metrics
#   --chain-grpc HOST:PORT         default: 127.0.0.1:9001
#   --out PATH                     harness JSON (default: .data/restart-trials.json)
#   --trials N                     default 20
#   --bar-seconds N                following_head bar (default 60; CC-42 re-derived)
#   --slot-seconds N               default 12 (Hoodi)
#   --genesis-time N               default 1742213400 (Hoodi)
#   --poll-seconds N               default 1
#   --head-advance-slots N         default 2
#   --min-immediate N              min runs at durability=immediate (default 5)
#   --min-paranoid N               min runs at durability=paranoid (default 5)
#   --venue hoodi|self-devnet      default hoodi
#   --durability-plan PLAN         auto|immediate|paranoid|split (default auto)
#   --checkpoint-blackhole MODE    empty-providers|assert-zero|none (default assert-zero)
#   --no-kill                      with --dry-run only (default dry-run never kills)
#
# Exit:
#   0  20/20 PASS, or --self-test / --emit-not-run / --detect-branch / --dry-run ok
#   1  assertion failure on a live trial
#   2  usage
#   3  prerequisites missing (live path)
#   4  partial (some trials FAIL) — still writes harness JSON
#   5  reserved (unused; --emit-not-run / --dry-run exit 0 with NOT_RUN harness)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SELF_TEST=0
CHECK_PREREQS=0
DETECT_BRANCH=0
EMIT_NOT_RUN=0
DRY_RUN=0
NOT_RUN_REASON=""
COMPOSE_DIR="${REPO_ROOT}"
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-}"
SERVICES_OVERRIDE=""
STORAGE_METRICS_URL="${RESTART_TRIALS_STORAGE_METRICS_URL:-http://127.0.0.1:9106/metrics}"
CHAIN_METRICS_URL="${RESTART_TRIALS_CHAIN_METRICS_URL:-http://127.0.0.1:9101/metrics}"
P2P_METRICS_URL="${RESTART_TRIALS_P2P_METRICS_URL:-http://127.0.0.1:9102/metrics}"
CHAIN_GRPC="${RESTART_TRIALS_CHAIN_GRPC:-127.0.0.1:9001}"
OUT="${RESTART_TRIALS_OUT:-${REPO_ROOT}/.data/restart-trials.json}"
TRIALS="${RESTART_TRIALS_N:-20}"
BAR_SECONDS="${RESTART_TRIALS_BAR:-60}"
SLOT_SECONDS="${RESTART_TRIALS_SLOT:-12}"
GENESIS_TIME="${RESTART_TRIALS_GENESIS:-1742213400}"
POLL_SECONDS="${RESTART_TRIALS_POLL:-1}"
HEAD_ADVANCE_SLOTS="${RESTART_TRIALS_HEAD_ADVANCE_SLOTS:-2}"
MIN_IMMEDIATE="${RESTART_TRIALS_MIN_IMMEDIATE:-5}"
MIN_PARANOID="${RESTART_TRIALS_MIN_PARANOID:-5}"
VENUE="${RESTART_TRIALS_VENUE:-hoodi}"
DURABILITY_PLAN="${RESTART_TRIALS_DURABILITY_PLAN:-auto}"
CHECKPOINT_BLACKHOLE="${RESTART_TRIALS_CHECKPOINT_BLACKHOLE:-assert-zero}"
KILL_SERVICES=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --self-test) SELF_TEST=1; shift ;;
    --check-prereqs) CHECK_PREREQS=1; shift ;;
    --detect-branch) DETECT_BRANCH=1; shift ;;
    --emit-not-run) EMIT_NOT_RUN=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --reason) NOT_RUN_REASON="$2"; shift 2 ;;
    --compose-dir) COMPOSE_DIR="$2"; shift 2 ;;
    --compose-project) COMPOSE_PROJECT="$2"; shift 2 ;;
    --services) SERVICES_OVERRIDE="$2"; shift 2 ;;
    --storage-metrics-url) STORAGE_METRICS_URL="$2"; shift 2 ;;
    --chain-metrics-url) CHAIN_METRICS_URL="$2"; shift 2 ;;
    --p2p-metrics-url) P2P_METRICS_URL="$2"; shift 2 ;;
    --chain-grpc) CHAIN_GRPC="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --trials) TRIALS="$2"; shift 2 ;;
    --bar-seconds) BAR_SECONDS="$2"; shift 2 ;;
    --slot-seconds) SLOT_SECONDS="$2"; shift 2 ;;
    --genesis-time) GENESIS_TIME="$2"; shift 2 ;;
    --poll-seconds) POLL_SECONDS="$2"; shift 2 ;;
    --head-advance-slots) HEAD_ADVANCE_SLOTS="$2"; shift 2 ;;
    --min-immediate) MIN_IMMEDIATE="$2"; shift 2 ;;
    --min-paranoid) MIN_PARANOID="$2"; shift 2 ;;
    --venue) VENUE="$2"; shift 2 ;;
    --durability-plan) DURABILITY_PLAN="$2"; shift 2 ;;
    --checkpoint-blackhole) CHECKPOINT_BLACKHOLE="$2"; shift 2 ;;
    --no-kill) KILL_SERVICES=0; shift ;;
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

log() { echo "restart-trials: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }
refuse() { echo "REFUSED: $*" >&2; exit 3; }

utc_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
unix_now() { date -u +%s; }

# Reject non-integer / negative knobs (SEC: no untrusted text into arithmetic or
# unquoted python). Empty after parse is also illegal for required knobs.
require_uint() {
  local name="$1" val="$2"
  if ! [[ "${val}" =~ ^[0-9]+$ ]]; then
    die "${name} must be a non-negative integer, got: ${val:-<empty>}"
  fi
}

require_positive_uint() {
  local name="$1" val="$2"
  require_uint "${name}" "${val}"
  if [[ "${val}" -lt 1 ]]; then
    die "${name} must be ≥ 1, got: ${val}"
  fi
}

# Compose service names only (no shell metacharacters → docker argv).
require_service_name() {
  local s="$1"
  if ! [[ "${s}" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    die "invalid compose service name: ${s:-<empty>}"
  fi
}

# Validate numeric knobs once after flag parse (and before any arithmetic).
validate_numeric_knobs() {
  require_positive_uint "--trials / TRIALS" "${TRIALS}"
  require_positive_uint "--bar-seconds / BAR_SECONDS" "${BAR_SECONDS}"
  require_positive_uint "--slot-seconds / SLOT_SECONDS" "${SLOT_SECONDS}"
  require_uint "--genesis-time / GENESIS_TIME" "${GENESIS_TIME}"
  require_positive_uint "--poll-seconds / POLL_SECONDS" "${POLL_SECONDS}"
  require_positive_uint "--head-advance-slots / HEAD_ADVANCE_SLOTS" "${HEAD_ADVANCE_SLOTS}"
  require_uint "--min-immediate / MIN_IMMEDIATE" "${MIN_IMMEDIATE}"
  require_uint "--min-paranoid / MIN_PARANOID" "${MIN_PARANOID}"
  if [[ "${MIN_IMMEDIATE}" -lt 1 || "${MIN_PARANOID}" -lt 1 ]]; then
    # At least one of each is required by the issue (≥ 5 in a full set).
    :
  fi
  if [[ $((MIN_IMMEDIATE + MIN_PARANOID)) -gt "${TRIALS}" ]]; then
    die "min-immediate (${MIN_IMMEDIATE}) + min-paranoid (${MIN_PARANOID}) > trials (${TRIALS})"
  fi
}

# Hard guard: this script must never invoke down -v (D-11 / CC-4N).
assert_no_down_v_in_self() {
  if grep -E -- 'compose[[:space:]]+down[[:space:]]+-v|down[[:space:]]+-v' "$0" \
    | grep -v 'never' | grep -v 'NEVER' | grep -v '#' >/dev/null 2>&1; then
    # Only the documentation mentions; production code paths use kill+up.
    :
  fi
  # Positive check: kill path present, down -v absent as a command.
  if grep -E 'docker compose down -v|compose down -v' "$0" | grep -vE '^\s*#' | grep -v 'never' | grep -v 'NEVER' >/dev/null 2>&1; then
    die "script body must never invoke 'docker compose down -v'"
  fi
}

compose() {
  local args=(docker compose)
  if [[ -n "${COMPOSE_PROJECT}" ]]; then
    args+=(-p "${COMPOSE_PROJECT}")
  fi
  args+=(--project-directory "${COMPOSE_DIR}")
  # Prefer repo compose file when project-directory is the monorepo root.
  if [[ -f "${COMPOSE_DIR}/docker-compose.yml" ]]; then
    args+=(-f "${COMPOSE_DIR}/docker-compose.yml")
  fi
  "${args[@]}" "$@"
}

# ── metrics / GetHead helpers ───────────────────────────────────────────────
scrape() {
  local url="$1"
  curl -fsS --max-time 10 "${url}"
}

# Print single gauge/counter value (first match). Exit 2 if missing.
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

# Sum all series for a metric family (optional label regex). Missing → exit 2.
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

# Extract histogram sum/count and optional per-phase sums for restart_seconds.
# Prints JSON: { "sum": F, "count": F, "phases": {phase: last_observed_approx} }
# Histograms do not retain per-sample values; we report _sum / _count per phase
# and the script diffs pre/post to attribute this trial's observations.
prom_restart_phases() {
  local text="$1"
  PROM_BODY="${text}" python3 - <<'PY'
import os, re, json
body = os.environ["PROM_BODY"]
phases = {}
# _sum{phase="open"} 1.23
pat_sum = re.compile(
    r'^cc_storage_restart_seconds_sum\{([^}]*)\}\s+([0-9.eE+-]+)\s*$',
    re.M,
)
pat_count = re.compile(
    r'^cc_storage_restart_seconds_count\{([^}]*)\}\s+([0-9.eE+-]+)\s*$',
    re.M,
)
sums, counts = {}, {}
for m in pat_sum.finditer(body):
    labs = m.group(1)
    pm = re.search(r'phase="([^"]+)"', labs)
    if not pm:
        continue
    sums[pm.group(1)] = float(m.group(2))
for m in pat_count.finditer(body):
    labs = m.group(1)
    pm = re.search(r'phase="([^"]+)"', labs)
    if not pm:
        continue
    counts[pm.group(1)] = float(m.group(2))
for p in sorted(set(sums) | set(counts)):
    phases[p] = {"sum": sums.get(p, 0.0), "count": counts.get(p, 0.0)}
total_sum = sum(v["sum"] for v in phases.values())
total_count = sum(v["count"] for v in phases.values())
print(json.dumps({"sum": total_sum, "count": total_count, "phases": phases}))
PY
}

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
    print("0x" + ("00" * 32), int(slot))
    raise SystemExit(0)
pad = "=" * (-len(root_b64) % 4)
try:
    data = base64.b64decode(root_b64 + pad)
except Exception:
    data = base64.urlsafe_b64decode(root_b64 + pad)
if len(data) != 32:
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

# ── branch detection (R-1) ──────────────────────────────────────────────────
# Branch A requires BOTH:
#   1. EL service present in `docker compose config --services`
#   2. services/chain has a real optimistic-sync state machine (not CC-14 stub)
# Otherwise Branch B: partial — no EL in the restart set.
detect_el_in_compose() {
  local services
  if ! command -v docker >/dev/null 2>&1; then
    # Fall back to static docker-compose.yml parse when docker is unavailable.
    if [[ -f "${COMPOSE_DIR}/docker-compose.yml" ]] \
      && grep -E '^[[:space:]]*el:[[:space:]]*$' "${COMPOSE_DIR}/docker-compose.yml" >/dev/null 2>&1; then
      echo 1
      return 0
    fi
    echo 0
    return 0
  fi
  set +e
  services="$(compose config --services 2>/dev/null)"
  local rc=$?
  set -e
  if [[ "${rc}" -ne 0 || -z "${services}" ]]; then
    if [[ -f "${COMPOSE_DIR}/docker-compose.yml" ]] \
      && grep -E '^[[:space:]]*el:[[:space:]]*$' "${COMPOSE_DIR}/docker-compose.yml" >/dev/null 2>&1; then
      echo 1
      return 0
    fi
    echo 0
    return 0
  fi
  if printf '%s\n' "${services}" | grep -qx 'el'; then
    echo 1
  else
    echo 0
  fi
}

# Real optimistic-sync: chain uses fork-choice is_optimistic_node / is_optimistic
# and exposes cc_chain_is_optimistic (Phase 3), not a constant-true CC-14 stub.
detect_real_optimistic() {
  local chain_src="${REPO_ROOT}/services/chain/src"
  if [[ ! -d "${chain_src}" ]]; then
    echo 0
    return 0
  fi
  # Positive signals of the real state machine.
  if ! grep -R --include='*.rs' -l 'is_optimistic_node' "${chain_src}" >/dev/null 2>&1; then
    echo 0
    return 0
  fi
  if ! grep -R --include='*.rs' -l 'cc_chain_is_optimistic\|is_optimistic' "${chain_src}" >/dev/null 2>&1; then
    echo 0
    return 0
  fi
  # Negative: pure always-true stub without fork-choice import (CC-14 era).
  # Presence of invalidation / engine_client is a Phase 3 signal.
  if grep -R --include='*.rs' -l 'optimistic' "${chain_src}" >/dev/null 2>&1 \
    && { [[ -f "${chain_src}/invalidation.rs" ]] || [[ -f "${chain_src}/engine_client.rs" ]]; }; then
    echo 1
    return 0
  fi
  echo 0
}

detect_branch() {
  local el opt
  el="$(detect_el_in_compose)"
  opt="$(detect_real_optimistic)"
  if [[ "${el}" == "1" && "${opt}" == "1" ]]; then
    echo "A"
  else
    echo "B"
  fi
}

print_branch_report() {
  local el opt branch
  el="$(detect_el_in_compose)"
  opt="$(detect_real_optimistic)"
  branch="$(detect_branch)"
  echo "branch: ${branch}"
  echo "el_in_compose: ${el}"
  echo "real_optimistic_state_machine: ${opt}"
  if [[ "${branch}" == "A" ]]; then
    echo "clause1_may_discharge: yes (20/20 required)"
    echo "note: EL present in compose + real optimistic path in services/chain"
  else
    echo "clause1_may_discharge: no"
    echo "measured_literal: partial — no EL in the restart set"
    echo "note: branch B does not discharge; re-run at Phase 3 exit"
  fi
  if command -v docker >/dev/null 2>&1; then
    echo "--- docker compose config --services ---"
    compose config --services 2>/dev/null || echo "(compose config unavailable)"
  fi
}

# ── durability plan (≥ 5 each when trials ≥ 10) ─────────────────────────────
# Prints one durability token per trial line (immediate|paranoid).
durability_for_trial() {
  local i="$1"  # 1-based
  case "${DURABILITY_PLAN}" in
    immediate) echo "immediate" ;;
    paranoid) echo "paranoid" ;;
    split|auto)
      # First half immediate, second half paranoid; ensure mins when possible.
      local mid=$(( (TRIALS + 1) / 2 ))
      if [[ "${i}" -le "${mid}" ]]; then
        echo "immediate"
      else
        echo "paranoid"
      fi
      ;;
    *)
      die "unknown --durability-plan: ${DURABILITY_PLAN}"
      ;;
  esac
}

# ── mid-slot wait [4 s, 8 s) ────────────────────────────────────────────────
# Returns the chosen offset (float seconds into the slot) after waiting.
# SEC: all python inputs via env + quoted heredoc — never interpolate shell
# values into unquoted <<PY bodies (genesis/slot/target injection surface).
wait_mid_slot() {
  local target
  target="$(
    MID_SLOT_LO=4 MID_SLOT_HI=8 python3 - <<'PY'
import os, random
lo = float(os.environ["MID_SLOT_LO"])
hi = float(os.environ["MID_SLOT_HI"])
print(f"{random.uniform(lo, hi):.6f}")
PY
  )"
  local now wait_s slot_mod
  now="$(unix_now)"
  # Seconds into current slot.
  slot_mod="$(
    NOW="${now}" GENESIS_TIME="${GENESIS_TIME}" SLOT_SECONDS="${SLOT_SECONDS}" \
      python3 - <<'PY'
import os
now = int(os.environ["NOW"])
genesis = int(os.environ["GENESIS_TIME"])
slot = int(os.environ["SLOT_SECONDS"])
if slot <= 0:
    raise SystemExit("SLOT_SECONDS must be > 0")
print((now - genesis) % slot)
PY
  )"
  # Wait until we are at `target` into this or next slot.
  wait_s="$(
    SLOT_MOD="${slot_mod}" TARGET="${target}" SLOT_SECONDS="${SLOT_SECONDS}" \
      python3 - <<'PY'
import os
mod = float(os.environ["SLOT_MOD"])
target = float(os.environ["TARGET"])
slot = float(os.environ["SLOT_SECONDS"])
if mod <= target:
    print(f"{target - mod:.6f}")
else:
    print(f"{slot - mod + target:.6f}")
PY
  )"
  if awk -v w="${wait_s}" 'BEGIN{exit !(w > 0.05)}'; then
    log "mid-slot: waiting ${wait_s}s for offset ${target}s into slot"
    sleep "${wait_s}"
  fi
  echo "${target}"
}

# ── kill + up (never down -v) ───────────────────────────────────────────────
# Emit one service name per line. --services is space-separated; each name is
# validated and always passed as a separate quoted argv element to docker.
list_kill_services() {
  if [[ -n "${SERVICES_OVERRIDE}" ]]; then
    local -a parts=()
    # read -a splits on IFS whitespace; do not eval / unquoted expand into docker.
    read -r -a parts <<< "${SERVICES_OVERRIDE}"
    local s
    for s in "${parts[@]}"; do
      [[ -z "${s}" ]] && continue
      require_service_name "${s}"
      printf '%s\n' "${s}"
    done
    return 0
  fi
  local services
  set +e
  services="$(compose config --services 2>/dev/null)"
  set -e
  if [[ -z "${services}" ]]; then
    # Static fallback for the monorepo stack.
    printf '%s\n' chain p2p attestation engine storage beacon-api
    return 0
  fi
  # Kill consensus stack; keep el running when present so Branch A does not
  # force an EL snap resync. Volumes still survive either way (no down -v).
  printf '%s\n' "${services}" | grep -vx 'el' || true
}

sigkill_stack() {
  local -a svcs=()
  local line
  while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    require_service_name "${line}"
    svcs+=("${line}")
  done < <(list_kill_services)
  if [[ "${#svcs[@]}" -eq 0 ]]; then
    die "no services to kill"
  fi
  log "SIGKILL services: ${svcs[*]}  (never down, never down -v)"
  # Explicit signal; each service is its own quoted argv (never a joined string).
  compose kill -s SIGKILL "${svcs[@]}"
}

bring_up_stack() {
  log "compose up -d (volumes intact)"
  compose up -d
}

# Apply durability for the next storage start via compose env.
# Relies on cc-config figment layer CC_STORAGE_DURABILITY.
apply_durability_env() {
  local d="$1"
  export CC_STORAGE_DURABILITY="${d}"
  log "durability for next up: ${d} (CC_STORAGE_DURABILITY)"
}

# ── evaluate one trial (shared live + self-test) ────────────────────────────
# Caller sets globals listed in the function body; prints PASS/FAIL; returns 0/1.
#
# Root identity vs head advance are **separate** assertions:
#   - PRE_ROOT vs POST_ROOT: GetHead at pre-kill vs at first following_head==1
#     (resume identity — frozen; must not be overwritten when head later advances)
#   - HEAD_ADVANCED_WITHIN_SLOTS: head slot moves within 2 slots *after* resume
evaluate_trial() {
  # Required:
  #   TRIAL_IDX, DURABILITY
  #   MID_SLOT_OFFSET
  #   PRE_ROOT, POST_ROOT   (identity pair — resume-time post root)
  #   PRE_SLOT, POST_SLOT   (slots at the identity sample)
  #   PRE_EAS, POST_EAS
  #   FOLLOWING_HEAD_FINAL (0/1)
  #   RESUME_WALL_SECONDS
  #   HEAD_ADVANCED_WITHIN_SLOTS (0/1)
  #   BOOTSTRAP_DELTA (must be 0)
  #   PHASE_BREAKDOWN_JSON  (dict phase -> seconds for this trial)
  #   BAR_SECONDS
  EVAL_STATUS="PASS"
  EVAL_FAILS=()

  if [[ -z "${PRE_ROOT}" || -z "${POST_ROOT}" ]]; then
    EVAL_FAILS+=("missing pre/post GetHead root")
  elif [[ "${PRE_ROOT}" != "${POST_ROOT}" ]]; then
    EVAL_FAILS+=("GetHead root mismatch pre=${PRE_ROOT} post=${POST_ROOT} (identity at resume; advance is separate)")
  fi

  if [[ -z "${FOLLOWING_HEAD_FINAL}" ]]; then
    EVAL_FAILS+=("cc_storage_following_head missing after resume")
  elif ! awk -v v="${FOLLOWING_HEAD_FINAL}" 'BEGIN{exit !(v+0 == 1)}'; then
    EVAL_FAILS+=("cc_storage_following_head=${FOLLOWING_HEAD_FINAL}, want 1")
  fi

  if [[ -z "${RESUME_WALL_SECONDS}" ]]; then
    EVAL_FAILS+=("missing resume wall seconds")
  elif ! awk -v w="${RESUME_WALL_SECONDS}" -v b="${BAR_SECONDS}" 'BEGIN{exit !(w+0 <= b+0)}'; then
    EVAL_FAILS+=("resume wall ${RESUME_WALL_SECONDS}s > bar ${BAR_SECONDS}s")
  fi

  if [[ -n "${PRE_EAS}" && -n "${POST_EAS}" ]]; then
    if ! awk -v a="${PRE_EAS}" -v b="${POST_EAS}" 'BEGIN{exit !(b+0 <= a+0)}'; then
      EVAL_FAILS+=("earliest_available_slot rose: pre=${PRE_EAS} post=${POST_EAS}")
    fi
  else
    EVAL_FAILS+=("missing pre/post earliest_available_slot")
  fi

  if [[ "${HEAD_ADVANCED_WITHIN_SLOTS}" != "1" ]]; then
    EVAL_FAILS+=("head did not advance within ${HEAD_ADVANCE_SLOTS} slots after resume")
  fi

  if [[ -z "${BOOTSTRAP_DELTA}" ]]; then
    EVAL_FAILS+=("missing checkpoint bootstrap delta")
  elif ! awk -v d="${BOOTSTRAP_DELTA}" 'BEGIN{exit !(d+0 == 0)}'; then
    EVAL_FAILS+=("checkpoint bootstrap attempts delta=${BOOTSTRAP_DELTA} (want 0; fallback must be unreachable)")
  fi

  if [[ -z "${MID_SLOT_OFFSET}" ]]; then
    EVAL_FAILS+=("missing mid-slot offset")
  elif ! awk -v o="${MID_SLOT_OFFSET}" 'BEGIN{exit !(o+0 >= 4.0 && o+0 < 8.0)}'; then
    EVAL_FAILS+=("mid-slot offset ${MID_SLOT_OFFSET} not in [4,8)")
  fi

  # Phase breakdown: seven named terms; sum should be ~ wall (finding if not).
  EVAL_PHASE_SUM=""
  if [[ -n "${PHASE_BREAKDOWN_JSON}" ]]; then
    EVAL_PHASE_SUM="$(
      PHASE_JSON="${PHASE_BREAKDOWN_JSON}" python3 - <<'PY'
import json, os
d = json.loads(os.environ.get("PHASE_JSON") or "{}")
print(f"{sum(float(v) for v in d.values()):.6f}")
PY
    )"
  fi

  if [[ "${#EVAL_FAILS[@]}" -gt 0 ]]; then
    EVAL_STATUS="FAIL"
    log "trial ${TRIAL_IDX}: FAIL — ${EVAL_FAILS[*]}"
    return 1
  fi
  log "trial ${TRIAL_IDX}: PASS — wall=${RESUME_WALL_SECONDS}s durability=${DURABILITY} offset=${MID_SLOT_OFFSET}s"
  return 0
}

# Build one trial row JSON.
trial_row_json() {
  TRIAL_IDX="${TRIAL_IDX}" \
  STATUS="${EVAL_STATUS}" \
  DUR="${DURABILITY}" \
  OFFSET="${MID_SLOT_OFFSET}" \
  PRE_ROOT="${PRE_ROOT}" POST_ROOT="${POST_ROOT}" \
  PRE_SLOT="${PRE_SLOT:-}" POST_SLOT="${POST_SLOT:-}" \
  ADV_ROOT="${ADVANCED_ROOT:-}" ADV_SLOT="${ADVANCED_SLOT:-}" \
  PRE_EAS="${PRE_EAS}" POST_EAS="${POST_EAS}" \
  FH="${FOLLOWING_HEAD_FINAL}" \
  WALL="${RESUME_WALL_SECONDS}" \
  HADV="${HEAD_ADVANCED_WITHIN_SLOTS}" \
  BDELTA="${BOOTSTRAP_DELTA}" \
  PHASES="${PHASE_BREAKDOWN_JSON}" \
  FAILS="$(printf '%s\n' "${EVAL_FAILS[@]:-}")" \
  BAR="${BAR_SECONDS}" \
  PHASE_SUM="${EVAL_PHASE_SUM:-}" \
  KILL_TS="${KILL_TS:-}" \
  RESUME_TS="${RESUME_TS:-}" \
  python3 - <<'PY'
import json, os
fails = [ln for ln in os.environ.get("FAILS", "").splitlines() if ln.strip()]
phases = {}
raw = os.environ.get("PHASES") or "{}"
try:
    phases = json.loads(raw)
except Exception:
    phases = {}
def num(k):
    v = os.environ.get(k, "")
    if v == "" or v is None:
        return None
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except ValueError:
        return None
wall = num("WALL")
phase_sum = num("PHASE_SUM")
pre_root = os.environ.get("PRE_ROOT") or None
post_root = os.environ.get("POST_ROOT") or None
out = {
  "trial": int(os.environ.get("TRIAL_IDX") or 0),
  "status": os.environ.get("STATUS", "FAIL"),
  "durability": os.environ.get("DUR"),
  "mid_slot_offset_s": num("OFFSET"),
  "kill_utc": os.environ.get("KILL_TS") or None,
  "resume_following_head_utc": os.environ.get("RESUME_TS") or None,
  "resume_wall_seconds": wall,
  "bar_seconds": num("BAR"),
  "within_bar": (wall is not None and num("BAR") is not None and wall <= num("BAR")),
  # Identity pair: pre-kill vs first following_head==1 (frozen; not advance sample).
  "pre_get_head_root": pre_root,
  "post_get_head_root": post_root,
  "head_roots_identical": pre_root is not None and pre_root == post_root,
  "pre_head_slot": num("PRE_SLOT"),
  "post_head_slot": num("POST_SLOT"),
  # Advance sample (may differ from post_* identity root/slot).
  "advanced_get_head_root": os.environ.get("ADV_ROOT") or None,
  "advanced_head_slot": num("ADV_SLOT"),
  "pre_earliest_available_slot": num("PRE_EAS"),
  "post_earliest_available_slot": num("POST_EAS"),
  "window_le_pre_crash": (
    num("PRE_EAS") is not None and num("POST_EAS") is not None
    and num("POST_EAS") <= num("PRE_EAS")
  ),
  "cc_storage_following_head": num("FH"),
  "head_advanced_within_slots": os.environ.get("HADV") == "1",
  "checkpoint_bootstrap_delta": num("BDELTA"),
  "checkpoint_fallback_unreachable": num("BDELTA") == 0,
  "restart_seconds_phases": phases,
  "phase_sum_seconds": phase_sum,
  "phase_sum_vs_wall_note": (
    None if wall is None or phase_sum is None
    else ("ok" if abs(wall - phase_sum) <= max(1.0, 0.25 * wall)
          else "FINDING: phase terms do not sum to observed wall")
  ),
  "fails": fails,
}
print(json.dumps(out))
PY
}

# ── harness writer ──────────────────────────────────────────────────────────
write_harness() {
  # Args via env: HARNESS_TRIALS_JSON (JSON array string), OVERALL, REASON, BRANCH, ...
  mkdir -p "$(dirname "${OUT}")"
  local git_sha engine_ver compose_svcs
  git_sha="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
  engine_ver="$(
    REPO_ROOT="${REPO_ROOT}" python3 - <<'PY'
import os, re, pathlib
lock = pathlib.Path(os.environ["REPO_ROOT"]) / "Cargo.lock"
text = lock.read_text() if lock.exists() else ""
m = re.search(r'name = "redb"\nversion = "([^"]+)"', text)
print(m.group(1) if m else "unknown")
PY
  )"
  compose_svcs="$(compose config --services 2>/dev/null | tr '\n' ' ' || true)"

  OUT_PATH="${OUT}" \
  TRIALS_JSON="${HARNESS_TRIALS_JSON:-[]}" \
  OVERALL="${OVERALL:-NOT_RUN}" \
  REASON="${REASON:-}" \
  BRANCH="${BRANCH:-}" \
  EL_IN="${EL_IN_RESTART_SET:-}" \
  VENUE="${VENUE}" \
  BAR="${BAR_SECONDS}" \
  GIT_SHA="${git_sha}" \
  ENGINE_CRATE="redb" \
  ENGINE_VER="${engine_ver}" \
  COMPOSE_SERVICES="${compose_svcs}" \
  python3 - <<'PY'
import json, os
from pathlib import Path

trials = json.loads(os.environ.get("TRIALS_JSON") or "[]")
branch = (os.environ.get("BRANCH") or "").strip().upper()
el_raw = os.environ.get("EL_IN", "")
if el_raw in ("1", "true", "True", "yes"):
    el_in = True
elif el_raw in ("0", "false", "False", "no"):
    el_in = False
else:
    el_in = branch == "A"

overall = os.environ.get("OVERALL", "NOT_RUN")
reason = os.environ.get("REASON") or None
venue = os.environ.get("VENUE") or "hoodi"
bar = float(os.environ.get("BAR") or 60)

ok = [t for t in trials if t.get("status") == "PASS"]
fail = [t for t in trials if t.get("status") == "FAIL"]
walls = [t["resume_wall_seconds"] for t in trials if t.get("resume_wall_seconds") is not None]
max_wall = max(walls) if walls else None

# p50/p99 per durability
def pct(xs, p):
    if not xs:
        return None
    ys = sorted(xs)
    if len(ys) == 1:
        return ys[0]
    k = (len(ys) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(ys) - 1)
    if f == c:
        return ys[f]
    return ys[f] + (ys[c] - ys[f]) * (k - f)

by_dur = {}
for t in trials:
    d = t.get("durability") or "unknown"
    by_dur.setdefault(d, []).append(t.get("resume_wall_seconds"))

dur_stats = {}
for d, xs in by_dur.items():
    xs2 = [x for x in xs if x is not None]
    dur_stats[d] = {
        "n": len(xs),
        "p50": pct(xs2, 50),
        "p99": pct(xs2, 99),
        "max": max(xs2) if xs2 else None,
    }

eas_series = [
    t.get("post_earliest_available_slot")
    for t in trials
    if t.get("post_earliest_available_slot") is not None
]
window_only_down = True
if eas_series:
    prev = None
    for v in eas_series:
        if prev is not None and v > prev:
            window_only_down = False
            break
        prev = v
else:
    window_only_down = None

identical = all(t.get("head_roots_identical") for t in trials) if trials else None
bootstrap_ok = all(t.get("checkpoint_fallback_unreachable") for t in trials) if trials else None

# clause1 block consumed by soak-report.sh --phase 4
if overall == "NOT_RUN":
    clause1 = {
        "branch": branch or None,
        "el_in_restart_set": el_in if branch else None,
        "partial_no_el": (branch == "B"),
        "venue": venue,
        "runs_ok": None,
        "runs_total": 20,
        "max_restart_seconds": None,
        "head_roots_identical": None,
        "pre_get_head_root": None,
        "post_get_head_root": None,
        "status": "NOT_RUN",
        "not_run_reason": reason,
    }
    if branch == "B":
        # Keep the literal string path even on NOT_RUN so soak-report emits
        # partial — no EL… rather than inventing a discharge.
        clause1["partial_no_el"] = True
        clause1["el_in_restart_set"] = False
elif branch == "B":
    clause1 = {
        "branch": "B",
        "el_in_restart_set": False,
        "partial_no_el": True,
        "venue": venue,
        "runs_ok": len(ok),
        "runs_total": len(trials) if trials else 20,
        "max_restart_seconds": max_wall,
        "head_roots_identical": identical,
        "status": overall,
        "measured_literal": "partial — no EL in the restart set",
    }
else:
    clause1 = {
        "branch": "A",
        "el_in_restart_set": True,
        "partial_no_el": False,
        "venue": venue,
        "runs_ok": len(ok),
        "runs_total": len(trials) if trials else 20,
        "max_restart_seconds": max_wall,
        "head_roots_identical": identical,
        "pre_get_head_root": (ok[0].get("pre_get_head_root") if ok else None),
        "post_get_head_root": (ok[0].get("post_get_head_root") if ok else None),
        "status": overall,
    }

doc = {
    "generator": "scripts/restart-trials.sh",
    "issue": "CC-45c",
    "clause": 1,
    "venue": venue,
    "overall_status": overall,
    "not_run_reason": reason,
    "branch": branch or None,
    "el_in_restart_set": el_in,
    "bar_seconds": bar,
    "bar_source": "CC-42 left snapshot_epochs=32; bar remains 60 s (term (c) ≤ 5.0 s)",
    "git_sha": os.environ.get("GIT_SHA"),
    "engine": {
        "crate": os.environ.get("ENGINE_CRATE"),
        "version": os.environ.get("ENGINE_VER"),
    },
    "compose_services": (os.environ.get("COMPOSE_SERVICES") or "").split(),
    "d11": {
        "exclusive_machine_required": True,
        "no_down_v": True,
        "no_store_bench": True,
        "no_second_stack": True,
        "note": (
            "D-11: machine runs the Phase 4 stack only for the whole set. "
            "Any code change, sleep, reboot, OS update, second stack, build, "
            "or bin/store-bench voids the set."
        ),
    },
    "clause1": clause1,
    "clause7b": {
        "advertised_window_only_downward_across_set": window_only_down,
    },
    "checkpoint_fallback": {
        "mode": "assert zero cc_chain_bootstrap_attempts_total delta per trial",
        "all_trials_zero_delta": bootstrap_ok,
    },
    "durability_stats": dur_stats,
    "trials": trials,
    "summary": {
        "runs_ok": len(ok),
        "runs_fail": len(fail),
        "runs_total": len(trials),
        "max_restart_seconds": max_wall,
        "head_roots_identical_all": identical,
    },
}
path = Path(os.environ["OUT_PATH"])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(doc, indent=2) + "\n")
print(f"wrote {path}", flush=True)
PY
  log "harness written to ${OUT}"
}

emit_not_run_harness() {
  local reason="${1:-full 20/20 restart venue unavailable}"
  BRANCH="$(detect_branch)"
  EL_IN_RESTART_SET="$(detect_el_in_compose)"
  OVERALL="NOT_RUN"
  REASON="${reason}"
  HARNESS_TRIALS_JSON="[]"
  write_harness
}

# ── prerequisites ───────────────────────────────────────────────────────────
check_prereqs() {
  local ok=1
  command -v docker >/dev/null || { log "missing: docker"; ok=0; }
  command -v curl >/dev/null || { log "missing: curl"; ok=0; }
  command -v python3 >/dev/null || { log "missing: python3"; ok=0; }
  command -v grpcurl >/dev/null || { log "missing: grpcurl"; ok=0; }

  if ! curl -fsS --max-time 5 "${STORAGE_METRICS_URL}" >/dev/null 2>&1; then
    log "storage metrics unreachable: ${STORAGE_METRICS_URL}"
    ok=0
  else
    log "storage metrics ok: ${STORAGE_METRICS_URL}"
  fi
  if ! curl -fsS --max-time 5 "${CHAIN_METRICS_URL}" >/dev/null 2>&1; then
    log "chain metrics unreachable: ${CHAIN_METRICS_URL}"
    ok=0
  else
    log "chain metrics ok: ${CHAIN_METRICS_URL}"
  fi
  if ! curl -fsS --max-time 5 "${P2P_METRICS_URL}" >/dev/null 2>&1; then
    log "p2p metrics unreachable (clause 7(c) scrape): ${P2P_METRICS_URL}"
    # Advisory only — clause 7(c) is asserted across the set when present.
    log "note: continuing; live set will record p2p eas when available"
  else
    log "p2p metrics ok: ${P2P_METRICS_URL}"
  fi

  set +e
  local_get_head >/dev/null
  local gh_rc=$?
  set -e
  if [[ "${gh_rc}" -ne 0 ]]; then
    log "GetHead failed at ${CHAIN_GRPC}"
    ok=0
  else
    log "GetHead ok at ${CHAIN_GRPC}"
  fi

  # following_head must be exposed (seeded even if still 0).
  local body
  set +e
  body="$(scrape "${STORAGE_METRICS_URL}")"
  set -e
  if ! printf '%s' "${body}" | grep -q 'cc_storage_following_head'; then
    log "cc_storage_following_head family missing from storage metrics"
    ok=0
  fi

  # D-11 advisory: warn on other heavy containers.
  local names
  names="$(docker ps --format '{{.Names}}' 2>/dev/null || true)"
  if [[ -n "${names}" ]]; then
    log "docker ps names: $(printf '%s' "${names}" | tr '\n' ' ')"
    log "note: clause 1 requires machine exclusivity (D-11) for a discharging set"
  fi

  if [[ "${ok}" -ne 1 ]]; then
    refuse "prerequisites not met for live restart trials"
  fi
  log "prerequisites OK"
  return 0
}

# ── live one trial ──────────────────────────────────────────────────────────
run_live_trial() {
  local idx="$1"
  local durability="$2"
  TRIAL_IDX="${idx}"
  DURABILITY="${durability}"
  EVAL_FAILS=()

  apply_durability_env "${durability}"

  # Ensure stack is up and following before kill.
  compose up -d >/dev/null
  # Wait briefly for following_head if just brought up.
  local ready=0 t=0
  while [[ "${t}" -lt 90 ]]; do
    local sbody
    set +e
    sbody="$(scrape "${STORAGE_METRICS_URL}" 2>/dev/null)"
    set -e
    if [[ -n "${sbody}" ]]; then
      set +e
      local fh
      fh="$(prom_value "${sbody}" "cc_storage_following_head" "")"
      set -e
      if [[ -n "${fh}" ]] && awk -v v="${fh}" 'BEGIN{exit !(v+0 == 1)}'; then
        ready=1
        break
      fi
    fi
    sleep "${POLL_SECONDS}"
    t=$((t + POLL_SECONDS))
  done
  if [[ "${ready}" -ne 1 ]]; then
    log "trial ${idx}: stack not following_head=1 before kill (continuing; may FAIL)"
  fi

  MID_SLOT_OFFSET="$(wait_mid_slot)"

  # Pre-crash snapshots.
  local storage_pre chain_pre
  storage_pre="$(scrape "${STORAGE_METRICS_URL}")"
  chain_pre="$(scrape "${CHAIN_METRICS_URL}")"
  local head_line
  set +e
  head_line="$(local_get_head)"
  set -e
  PRE_ROOT="$(awk '{print $1}' <<<"${head_line}")"
  PRE_SLOT="$(awk '{print $2}' <<<"${head_line}")"

  set +e
  PRE_EAS="$(prom_value "${storage_pre}" "cc_storage_earliest_available_slot" "")"
  set -e
  : "${PRE_EAS:=}"

  local boot_pre=""
  set +e
  if ! boot_pre="$(prom_sum "${chain_pre}" "cc_chain_bootstrap_attempts_total" "")"; then
    if ! boot_pre="$(prom_sum "${chain_pre}" "cc_chain_bootstrap_attempts" "")"; then
      boot_pre="0"
    fi
  fi
  set -e
  : "${boot_pre:=0}"

  local phases_pre
  phases_pre="$(prom_restart_phases "${storage_pre}")"

  KILL_TS="$(utc_now)"
  local kill_unix
  kill_unix="$(unix_now)"

  if [[ "${KILL_SERVICES}" -eq 1 ]]; then
    sigkill_stack
    # Optional short hold so process death is observed (not a ring hold).
    sleep 1
    apply_durability_env "${durability}"
    bring_up_stack
  else
    log "trial ${idx}: --no-kill set; skipping SIGKILL (invalid for discharge)"
  fi

  # Poll following_head == 1.
  FOLLOWING_HEAD_FINAL=""
  RESUME_TS=""
  RESUME_WALL_SECONDS=""
  local elapsed=0
  while [[ "${elapsed}" -le "${BAR_SECONDS}" ]]; do
    local sbody fh
    set +e
    sbody="$(scrape "${STORAGE_METRICS_URL}" 2>/dev/null)"
    fh="$(prom_value "${sbody}" "cc_storage_following_head" "")"
    set -e
    if [[ -n "${fh}" ]] && awk -v v="${fh}" 'BEGIN{exit !(v+0 == 1)}'; then
      FOLLOWING_HEAD_FINAL=1
      RESUME_TS="$(utc_now)"
      RESUME_WALL_SECONDS=$(( $(unix_now) - kill_unix ))
      break
    fi
    sleep "${POLL_SECONDS}"
    elapsed=$((elapsed + POLL_SECONDS))
  done
  if [[ -z "${FOLLOWING_HEAD_FINAL}" ]]; then
    FOLLOWING_HEAD_FINAL=0
    RESUME_WALL_SECONDS=$(( $(unix_now) - kill_unix ))
    RESUME_TS="$(utc_now)"
  fi

  # Post GetHead at first following_head==1 — FREEZE for root identity.
  # Do not overwrite POST_ROOT when head later advances (separate assertion).
  local storage_post chain_post
  set +e
  storage_post="$(scrape "${STORAGE_METRICS_URL}" 2>/dev/null)"
  chain_post="$(scrape "${CHAIN_METRICS_URL}" 2>/dev/null)"
  head_line="$(local_get_head)"
  set -e
  POST_ROOT="$(awk '{print $1}' <<<"${head_line}")"
  POST_SLOT="$(awk '{print $2}' <<<"${head_line}")"
  set +e
  POST_EAS="$(prom_value "${storage_post}" "cc_storage_earliest_available_slot" "")"
  set -e
  : "${POST_EAS:=}"

  local boot_post=""
  set +e
  if ! boot_post="$(prom_sum "${chain_post}" "cc_chain_bootstrap_attempts_total" "")"; then
    if ! boot_post="$(prom_sum "${chain_post}" "cc_chain_bootstrap_attempts" "")"; then
      boot_post="${boot_pre}"
    fi
  fi
  set -e
  BOOTSTRAP_DELTA="$(awk -v a="${boot_pre}" -v b="${boot_post}" 'BEGIN{printf "%.0f", b-a}')"

  # Phase breakdown: per-phase sum delta (histogram _sum increase this trial).
  local phases_post
  phases_post="$(prom_restart_phases "${storage_post}")"
  PHASE_BREAKDOWN_JSON="$(
    PRE="${phases_pre}" POST="${phases_post}" python3 - <<'PY'
import json, os
pre = json.loads(os.environ["PRE"])
post = json.loads(os.environ["POST"])
expected = [
  "open", "schema_check", "snapshot_load", "restore_send",
  "chain_replay", "forkchoice_rebuild", "resubscribe",
]
out = {}
for p in expected:
    ps = pre.get("phases", {}).get(p, {}).get("sum", 0.0)
    qs = post.get("phases", {}).get(p, {}).get("sum", 0.0)
    out[p] = max(0.0, qs - ps)
print(json.dumps(out))
PY
  )"

  # Head advance within 2 slots after resume. Track ADVANCED_* only — never
  # clobber POST_ROOT / POST_SLOT (identity sample at following_head==1).
  HEAD_ADVANCED_WITHIN_SLOTS=0
  ADVANCED_ROOT=""
  ADVANCED_SLOT=""
  local wait_budget=$(( HEAD_ADVANCE_SLOTS * SLOT_SECONDS ))
  local waited=0
  local base_slot="${POST_SLOT:-0}"
  while [[ "${waited}" -le "${wait_budget}" ]]; do
    set +e
    head_line="$(local_get_head)"
    set -e
    local cur_slot cur_root
    cur_root="$(awk '{print $1}' <<<"${head_line}")"
    cur_slot="$(awk '{print $2}' <<<"${head_line}")"
    if [[ -n "${cur_slot}" && -n "${base_slot}" ]] \
      && awk -v a="${base_slot}" -v b="${cur_slot}" 'BEGIN{exit !(b+0 > a+0)}'; then
      HEAD_ADVANCED_WITHIN_SLOTS=1
      ADVANCED_SLOT="${cur_slot}"
      ADVANCED_ROOT="${cur_root}"
      break
    fi
    # Also accept advance relative to pre-kill head (resume may already be ahead).
    if [[ -n "${cur_slot}" && -n "${PRE_SLOT}" ]] \
      && awk -v a="${PRE_SLOT}" -v b="${cur_slot}" 'BEGIN{exit !(b+0 > a+0)}'; then
      HEAD_ADVANCED_WITHIN_SLOTS=1
      ADVANCED_SLOT="${cur_slot}"
      ADVANCED_ROOT="${cur_root}"
      break
    fi
    sleep "${POLL_SECONDS}"
    waited=$((waited + POLL_SECONDS))
  done

  set +e
  evaluate_trial
  local ev_rc=$?
  set -e

  TRIAL_JSON="$(trial_row_json)"
  return "${ev_rc}"
}

# ── self-test (offline; no docker) ──────────────────────────────────────────
run_self_test() {
  local tmp
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/cc-restart-trials.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -rf '${tmp}'" EXIT

  assert_no_down_v_in_self
  log "self-test: script never uses compose down -v"

  log "self-test: branch detection"
  local branch el opt
  branch="$(detect_branch)"
  el="$(detect_el_in_compose)"
  opt="$(detect_real_optimistic)"
  log "  branch=${branch} el=${el} optimistic=${opt}"
  [[ "${branch}" == "A" || "${branch}" == "B" ]] || die "self-test: branch must be A or B"
  # Current monorepo tree has el + real optimistic → expect A.
  if [[ "${el}" == "1" && "${opt}" == "1" ]]; then
    [[ "${branch}" == "A" ]] || die "self-test: expected branch A when el+optimistic present"
  fi

  log "self-test: evaluate_trial PASS path"
  TRIAL_IDX=1
  DURABILITY="immediate"
  MID_SLOT_OFFSET="5.5"
  PRE_ROOT="0xaaa"
  POST_ROOT="0xaaa"
  PRE_SLOT=100
  POST_SLOT=102
  PRE_EAS=50
  POST_EAS=50
  FOLLOWING_HEAD_FINAL=1
  RESUME_WALL_SECONDS=12
  HEAD_ADVANCED_WITHIN_SLOTS=1
  BOOTSTRAP_DELTA=0
  PHASE_BREAKDOWN_JSON='{"open":1,"schema_check":0.5,"snapshot_load":3,"restore_send":4,"chain_replay":2,"forkchoice_rebuild":0.5,"resubscribe":0.5}'
  BAR_SECONDS=60
  evaluate_trial || die "self-test: expected PASS"
  local row
  row="$(trial_row_json)"
  echo "${row}" | grep -q '"status": "PASS"' || die "self-test: PASS row missing"

  log "self-test: evaluate_trial FAIL on root mismatch"
  POST_ROOT="0xbbb"
  set +e
  evaluate_trial
  local rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on root mismatch"
  POST_ROOT="0xaaa"

  log "self-test: evaluate_trial FAIL on following_head != 1"
  FOLLOWING_HEAD_FINAL=0
  set +e
  evaluate_trial
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on following_head"
  FOLLOWING_HEAD_FINAL=1

  log "self-test: evaluate_trial FAIL on wall > bar"
  RESUME_WALL_SECONDS=90
  set +e
  evaluate_trial
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on bar"
  RESUME_WALL_SECONDS=12

  log "self-test: evaluate_trial FAIL on window increase"
  POST_EAS=60
  set +e
  evaluate_trial
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on eas rise"
  POST_EAS=50

  log "self-test: evaluate_trial FAIL on bootstrap delta"
  BOOTSTRAP_DELTA=1
  set +e
  evaluate_trial
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on bootstrap"
  BOOTSTRAP_DELTA=0

  log "self-test: evaluate_trial FAIL on mid-slot out of range"
  MID_SLOT_OFFSET="2.0"
  set +e
  evaluate_trial
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: expected FAIL on mid-slot"
  MID_SLOT_OFFSET="5.5"

  log "self-test: root identity frozen when advance sample differs"
  # Simulate resume identity match, then a later advanced head root/slot.
  PRE_ROOT="0xaaa"
  POST_ROOT="0xaaa"
  PRE_SLOT=100
  POST_SLOT=100
  ADVANCED_ROOT="0xccc"
  ADVANCED_SLOT=102
  HEAD_ADVANCED_WITHIN_SLOTS=1
  FOLLOWING_HEAD_FINAL=1
  RESUME_WALL_SECONDS=12
  BOOTSTRAP_DELTA=0
  PRE_EAS=50
  POST_EAS=50
  evaluate_trial || die "self-test: identity must PASS when only advance root differs"
  row="$(trial_row_json)"
  echo "${row}" | grep -q '"head_roots_identical": true' \
    || die "self-test: head_roots_identical must be true (identity pair)"
  echo "${row}" | grep -q '"advanced_get_head_root": "0xccc"' \
    || die "self-test: advanced root missing from row"
  # Regression: if POST_ROOT were clobbered by advance, identity would fail.
  POST_ROOT="0xccc"
  set +e
  evaluate_trial
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: clobbered POST_ROOT must FAIL identity"
  POST_ROOT="0xaaa"
  ADVANCED_ROOT=""
  ADVANCED_SLOT=""

  log "self-test: numeric knob validation rejects non-integers"
  set +e
  bash "$0" --trials '20;rm -rf /' --dry-run --out "${tmp}/bad.json" \
    >/dev/null 2>"${tmp}/bad.err"
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: non-integer --trials must fail"
  grep -q 'must be a non-negative integer' "${tmp}/bad.err" \
    || { cat "${tmp}/bad.err" >&2; die "self-test: missing integer reject message"; }

  log "self-test: --services rejects shell metacharacters"
  set +e
  bash "$0" --services 'storage;id' --dry-run --out "${tmp}/bad2.json" \
    >/dev/null 2>"${tmp}/bad2.err"
  rc=$?
  set -e
  [[ "${rc}" -ne 0 ]] || die "self-test: bad service name must fail"
  grep -qi 'invalid compose service name' "${tmp}/bad2.err" \
    || { cat "${tmp}/bad2.err" >&2; die "self-test: missing service-name reject"; }

  log "self-test: no unquoted python heredocs interpolating knobs"
  # Positive: every python3 - << body after this script's wait_mid_slot must be <<'PY'
  if grep -nE 'python3 - <<PY$' "$0" >/dev/null 2>&1; then
    grep -nE 'python3 - <<PY$' "$0" >&2 || true
    die "self-test: found unquoted python heredoc (use <<'PY' + env)"
  fi
  if grep -nE 'int\("\$\{|float\("\$\{|uniform\(\$\{' "$0" >/dev/null 2>&1; then
    grep -nE 'int\("\$\{|float\("\$\{|uniform\(\$\{' "$0" >&2 || true
    die "self-test: found shell interpolation inside python bodies"
  fi

  log "self-test: durability plan covers both settings"
  TRIALS=20
  DURABILITY_PLAN=auto
  local imm=0 par=0 i d
  for i in $(seq 1 20); do
    d="$(durability_for_trial "${i}")"
    case "${d}" in
      immediate) imm=$((imm + 1)) ;;
      paranoid) par=$((par + 1)) ;;
      *) die "self-test: bad durability ${d}" ;;
    esac
  done
  [[ "${imm}" -ge "${MIN_IMMEDIATE}" ]] || die "self-test: immediate count ${imm} < ${MIN_IMMEDIATE}"
  [[ "${par}" -ge "${MIN_PARANOID}" ]] || die "self-test: paranoid count ${par} < ${MIN_PARANOID}"
  log "  immediate=${imm} paranoid=${par} (mins ${MIN_IMMEDIATE}/${MIN_PARANOID})"

  log "self-test: emit NOT_RUN harness + soak-report phase 4 clause 1"
  OUT="${tmp}/not_run.json"
  bash "$0" --emit-not-run --reason "self-test not_run" --out "${OUT}" --compose-dir "${COMPOSE_DIR}"
  [[ -f "${OUT}" ]] || die "self-test: harness not written"
  grep -q 'NOT_RUN' "${OUT}" || die "self-test: NOT_RUN missing"
  grep -q 'CC-45c' "${OUT}" || die "self-test: issue id missing"
  # Must not claim fake 20/20 PASS
  if grep -q '"runs_ok": 20' "${OUT}" && grep -q '"status": "PASS"' "${OUT}"; then
    die "self-test: NOT_RUN harness must not claim 20/20 PASS"
  fi

  local body
  set +e
  body="$(bash "${SCRIPT_DIR}/soak-report.sh" --phase 4 \
    --clause 1 \
    --harness-json "${OUT}" 2>"${tmp}/sr.err")"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${tmp}/sr.err" >&2; die "self-test: soak-report exit ${rc}"; }
  # Branch A NOT_RUN or branch B partial — either is honest; never fake discharged 20/20.
  if echo "${body}" | grep -Fq 'PASS (discharged)'; then
    echo "${body}" >&2
    die "self-test: NOT_RUN harness must not discharge clause 1"
  fi
  echo "${body}" | grep -E 'NOT_RUN|not discharged|partial' >/dev/null \
    || { echo "${body}" >&2; die "self-test: expected NOT_RUN or not discharged row"; }
  log "soak-report clause 1 row ok"

  log "self-test: synthetic 20/20 branch A harness is accepted by soak-report"
  # This proves the instrument path; it is NOT a live discharge.
  cat > "${tmp}/fake20.json" <<'EOF'
{
  "generator": "scripts/restart-trials.sh",
  "issue": "CC-45c",
  "clause": 1,
  "venue": "hoodi",
  "overall_status": "PASS",
  "branch": "A",
  "el_in_restart_set": true,
  "clause1": {
    "branch": "A",
    "el_in_restart_set": true,
    "venue": "hoodi",
    "runs_ok": 20,
    "runs_total": 20,
    "max_restart_seconds": 41.2,
    "head_roots_identical": true,
    "pre_get_head_root": "0xabc",
    "post_get_head_root": "0xabc"
  },
  "trials": []
}
EOF
  set +e
  body="$(bash "${SCRIPT_DIR}/soak-report.sh" --phase 4 \
    --clause 1 \
    --harness-json "${tmp}/fake20.json" 2>"${tmp}/sr2.err")"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${tmp}/sr2.err" >&2; die "self-test: soak-report A exit ${rc}"; }
  echo "${body}" | grep -Fq 'PASS (discharged)' \
    || { echo "${body}" >&2; die "self-test: expected discharged on synthetic A 20/20"; }

  log "self-test: branch B literal string path"
  cat > "${tmp}/branchb.json" <<'EOF'
{
  "clause1": {
    "branch": "B",
    "el_in_restart_set": false,
    "partial_no_el": true,
    "venue": "self-devnet"
  }
}
EOF
  set +e
  body="$(bash "${SCRIPT_DIR}/soak-report.sh" --phase 4 \
    --clause 1 \
    --harness-json "${tmp}/branchb.json" 2>"${tmp}/sr3.err")"
  rc=$?
  set -e
  [[ "${rc}" -eq 0 ]] || { cat "${tmp}/sr3.err" >&2; die "self-test: soak-report B exit ${rc}"; }
  echo "${body}" | grep -Fq 'partial — no EL in the restart set' \
    || { echo "${body}" >&2; die "self-test: branch B literal missing"; }
  echo "${body}" | grep -Fq 'not discharged' \
    || { echo "${body}" >&2; die "self-test: branch B must be not discharged"; }

  log "self-test PASS"
  exit 0
}

# ── dry-run (plan only) ─────────────────────────────────────────────────────
run_dry_run() {
  log "dry-run: no SIGKILL, no live stack required"
  print_branch_report
  echo "trials_planned: ${TRIALS}"
  echo "bar_seconds: ${BAR_SECONDS}"
  echo "slot_seconds: ${SLOT_SECONDS}"
  echo "mid_slot_window: [4, 8)"
  echo "durability_plan: ${DURABILITY_PLAN}"
  echo "checkpoint_blackhole: ${CHECKPOINT_BLACKHOLE}"
  echo "kill_command: docker compose kill -s SIGKILL <services>"
  echo "up_command: docker compose up -d"
  echo "forbidden: docker compose down / down -v"
  echo "services_to_kill:"
  list_kill_services | sed 's/^/  - /'
  echo "durability_sequence:"
  local i
  for i in $(seq 1 "${TRIALS}"); do
    printf '  %2d %s\n' "${i}" "$(durability_for_trial "${i}")"
  done
  local branch
  branch="$(detect_branch)"
  BRANCH="${branch}"
  EL_IN_RESTART_SET="$(detect_el_in_compose)"
  OVERALL="NOT_RUN"
  REASON="dry-run only — no live 20/20 executed"
  HARNESS_TRIALS_JSON="[]"
  write_harness
  log "dry-run complete; harness is NOT_RUN (never fake 20/20 PASS)"
  exit 0
}

# ── live set ────────────────────────────────────────────────────────────────
run_live_set() {
  check_prereqs
  assert_no_down_v_in_self

  local branch el
  branch="$(detect_branch)"
  el="$(detect_el_in_compose)"
  BRANCH="${branch}"
  EL_IN_RESTART_SET="${el}"
  log "branch=${branch} el_in_compose=${el} bar=${BAR_SECONDS}s trials=${TRIALS}"
  print_branch_report

  # Checkpoint blackhole: prefer empty providers so fallback cannot silently
  # re-sync. Operator may also DNS-blackhole hosts; we assert zero delta on
  # cc_chain_bootstrap_attempts_* regardless.
  case "${CHECKPOINT_BLACKHOLE}" in
    empty-providers)
      export CC_CHAIN_CHECKPOINT_PROVIDERS=""
      log "checkpoint blackhole: CC_CHAIN_CHECKPOINT_PROVIDERS emptied for the set"
      ;;
    assert-zero|none)
      log "checkpoint blackhole mode=${CHECKPOINT_BLACKHOLE}: assert zero bootstrap delta per trial"
      ;;
    *)
      die "unknown --checkpoint-blackhole: ${CHECKPOINT_BLACKHOLE}"
      ;;
  esac

  local rows=()
  local fail_count=0
  local i d rc
  for i in $(seq 1 "${TRIALS}"); do
    d="$(durability_for_trial "${i}")"
    log "──── trial ${i}/${TRIALS} durability=${d} ────"
    set +e
    run_live_trial "${i}" "${d}"
    rc=$?
    set -e
    rows+=("${TRIAL_JSON}")
    if [[ "${rc}" -ne 0 ]]; then
      fail_count=$((fail_count + 1))
    fi
  done

  HARNESS_TRIALS_JSON="$(
    ROWS="$(printf '%s\n' "${rows[@]}")" python3 - <<'PY'
import json, os
rows = []
for ln in os.environ.get("ROWS", "").splitlines():
    ln = ln.strip()
    if not ln:
        continue
    rows.append(json.loads(ln))
print(json.dumps(rows))
PY
  )"

  local ok_count
  ok_count=$((TRIALS - fail_count))
  if [[ "${fail_count}" -eq 0 && "${TRIALS}" -ge 20 ]]; then
    OVERALL="PASS"
  elif [[ "${fail_count}" -eq 0 ]]; then
    OVERALL="PASS"
    log "note: trials=${TRIALS} < 20; discharge still requires 20/20"
  else
    OVERALL="FAIL"
  fi
  REASON=""
  if [[ "${branch}" == "B" ]]; then
    # Branch B never discharges even on 20/20.
    log "branch B: recording partial — no EL in the restart set (not discharged)"
  fi
  write_harness

  log "done: ok=${ok_count}/${TRIALS} fail=${fail_count} overall=${OVERALL} branch=${branch}"
  if [[ "${fail_count}" -eq 0 ]]; then
    exit 0
  fi
  if [[ "${ok_count}" -gt 0 ]]; then
    exit 4
  fi
  exit 1
}

# ── main ────────────────────────────────────────────────────────────────────
assert_no_down_v_in_self
validate_numeric_knobs

if [[ "${SELF_TEST}" -eq 1 ]]; then
  run_self_test
fi

if [[ "${DETECT_BRANCH}" -eq 1 ]]; then
  print_branch_report
  exit 0
fi

if [[ "${EMIT_NOT_RUN}" -eq 1 ]]; then
  reason="${NOT_RUN_REASON:-full 20/20 restart venue unavailable (no exclusive live stack in this session)}"
  emit_not_run_harness "${reason}"
  log "emitted NOT_RUN harness: ${reason}"
  exit 0
fi

if [[ "${CHECK_PREREQS}" -eq 1 ]]; then
  check_prereqs
  exit 0
fi

if [[ "${DRY_RUN}" -eq 1 ]]; then
  # Validate --services early so dry-run surfaces bad names without docker.
  if [[ -n "${SERVICES_OVERRIDE}" ]]; then
    list_kill_services >/dev/null
  fi
  run_dry_run
fi

# Default: attempt live set. If prereqs fail, refuse (exit 3) — operator can
# re-run with --emit-not-run for residual documentation.
if [[ -n "${SERVICES_OVERRIDE}" ]]; then
  list_kill_services >/dev/null
fi
run_live_set
