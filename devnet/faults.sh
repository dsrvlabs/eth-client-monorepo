#!/usr/bin/env bash
# devnet/faults.sh — docker-level fault primitives (CC-2Jd, CC-4N, CC-4D).
#
# No enclave, no cross-network join recipe — our own compose network only.
#
#   offline-gap <container> <minutes>   docker network disconnect → sleep → connect
#   pause <container> <seconds>         docker pause → sleep → unpause
#   restart <container> [--hold <s>]    kill -s SIGKILL, optional hold, then up -d
#   clock-jump <container> <seconds>    advance container wall clock (CC-4D)
#   engine-blackhole                    replace engine with a newPayload/fcU sink (S1-A-17)
#
# Container names are compose *service* names: main-stack `storage` (CC-4N
# kill-9) or self-devnet `publisher | node-a | node-b | anchor`. Full container
# ids/names from `docker ps` also resolve.
#
# Compose file is a parameter (`-f` / `--compose-file` / `CC_FAULTS_COMPOSE`).
# Default is the main stack the CC-4N kill-9 clause names
# (`docker-compose.yml`), not the self-devnet file. Ambient Docker
# `COMPOSE_FILE` is ignored so `restart storage` stays pinned. Pass
# `-f devnet/compose.yml` for node-a / publisher drills.
#
# CC-4N: restart must never use `docker compose down` (and never the volumes
# flag that deletes named volumes). Named volumes (`cc-store-data`,
# `cc-p2p-identity` on the main stack; identity mounts on the devnet) are one
# backup unit with the store. A mismatched node_key after restart must surface
# I-node-id (AnchorInfo.node_id pairing refusal), not a silent re-backfill —
# wiping volumes would destroy the proof.
#
# CC-4D: clock-jump is exercised once in the landing commit and not inventoried
# as a fault mode. restart --hold makes clause-2 run (b) runnable (SIGKILL,
# wait past ring depth, up).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# CC-4N kill-9 clause (docs/running.md) names the main stack, not the self-devnet.
# Dedicated pin — never Docker Compose's COMPOSE_FILE (ambient exports retarget -f).
DEFAULT_COMPOSE_FILE="${ROOT}/docker-compose.yml"
FAULTS_COMPOSE="${CC_FAULTS_COMPOSE:-${DEFAULT_COMPOSE_FILE}}"
NETWORK_NAME="${CC_DEVNET_NETWORK:-cc-devnet}"

usage() {
  cat <<EOF
Usage:
  $0 [-f FILE|--compose-file FILE] offline-gap <container> <minutes>
  $0 [-f FILE|--compose-file FILE] pause <container> <seconds>
  $0 [-f FILE|--compose-file FILE] restart <container> [--hold <seconds>]
  $0 [-f FILE|--compose-file FILE] clock-jump <container> <seconds>
  $0 [-f FILE|--compose-file FILE] engine-blackhole
  $0 [-f FILE|--compose-file FILE] exercise-once   # each primitive once (acceptance)
  $0 --self-test   # assert effective compose pin matches the CC-4N clause

Environment:
  CC_FAULTS_COMPOSE   compose file (default: <repo>/docker-compose.yml)
                      Ambient COMPOSE_FILE is ignored.
  CC_DEVNET_NETWORK   docker network name (default: cc-devnet)

restart uses: docker compose kill -s SIGKILL <service>
              [sleep <hold>]
              docker compose up -d <service>
  — never docker compose down (CC-4N; named volumes must survive the kill-9 clause).

clock-jump advances the container wall clock by <seconds> (docker exec date -s
or privileged sidecar). Requires CAP_SYS_TIME; may be unavailable on Docker
Desktop — report honestly rather than inventing a watermark.

engine-blackhole rebuilds compose \`engine\` as SERVICE=cc-engine-blackhole
(TCP accept on :9004; newPayload/fcU never answer). Production 8 s RPC
caps + ADR-R-04 N=3 (~18 s) stay SERVING on one (or two) deadlined RPCs.
Idle core stays SERVING (ADR-P3-02). Does not scrape health.
EOF
}

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}

# Compose service names only (no leading '-' → docker CLI flags).
require_service_name() {
  local s="$1"
  if ! [[ "${s}" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    echo "error: invalid compose service name: ${s:-<empty>}" >&2
    exit 2
  fi
}

# Regular file only — `-` is Compose stdin YAML.
require_compose_file() {
  local path="$1"
  if [[ -z "${path}" || "${path}" == "-" ]]; then
    echo "error: compose file must be a regular file, not stdin (-)" >&2
    exit 2
  fi
  if [[ ! -f "${path}" ]]; then
    echo "error: compose file is not a regular file: ${path}" >&2
    exit 2
  fi
}

resolve_container() {
  local name="$1"
  # Prefer compose service name resolution.
  local cid
  cid="$(docker compose -f "${FAULTS_COMPOSE}" ps -q "${name}" 2>/dev/null || true)"
  if [[ -n "${cid}" ]]; then
    echo "${cid}"
    return 0
  fi
  # Fall back to exact container name / id.
  if docker inspect "${name}" >/dev/null 2>&1; then
    echo "${name}"
    return 0
  fi
  echo "error: cannot resolve container/service ${name}" >&2
  return 1
}

peer_count_node_a() {
  local body
  body="$(curl -fsS --max-time 3 http://127.0.0.1:19112/metrics 2>/dev/null || true)"
  BODY="${body}" python3 - <<'PY'
import os, re
total = 0.0
for line in os.environ.get("BODY", "").splitlines():
    if line.startswith("cc_p2p_peers{"):
        try:
            total += float(line.rsplit(None, 1)[1])
        except Exception:
            pass
print(int(total))
PY
}

cmd_offline_gap() {
  local name="$1"
  local minutes="$2"
  require_service_name "${name}"
  if ! [[ "${minutes}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "error: minutes must be a number, got ${minutes}" >&2
    exit 2
  fi
  local cid
  cid="$(resolve_container "${name}")"
  local secs
  secs="$(python3 -c "print(int(float('${minutes}') * 60))")"
  echo "==> offline-gap ${name} (${cid}) for ${minutes} min (${secs}s) on ${NETWORK_NAME}"
  docker network disconnect "${NETWORK_NAME}" "${cid}"
  echo "  disconnected; sleeping ${secs}s"
  sleep "${secs}"
  docker network connect "${NETWORK_NAME}" "${cid}"
  echo "  reconnected"
}

cmd_pause() {
  local name="$1"
  local seconds="$2"
  require_service_name "${name}"
  if ! [[ "${seconds}" =~ ^[0-9]+$ ]]; then
    echo "error: seconds must be an integer, got ${seconds}" >&2
    exit 2
  fi
  local cid
  cid="$(resolve_container "${name}")"
  echo "==> pause ${name} (${cid}) for ${seconds}s"
  docker pause "${cid}"
  sleep "${seconds}"
  docker unpause "${cid}"
  echo "  unpaused"
}

cmd_restart() {
  # CC-4N: SIGKILL + up preserves named volumes. Never compose down (esp. with
  # the volumes flag). Pairing refusal (I-node-id): after a restart the store
  # still holds AnchorInfo.node_id; a replaced node_key must refuse open, not
  # re-backfill.
  # CC-4D: optional --hold <seconds> between kill and up for clause-2 ring depth.
  local name="$1"
  local hold="${2:-0}"
  require_service_name "${name}"
  if ! [[ "${hold}" =~ ^[0-9]+$ ]]; then
    echo "error: --hold seconds must be an integer, got ${hold}" >&2
    exit 2
  fi
  local cid
  cid="$(resolve_container "${name}" 2>/dev/null || true)"
  if [[ -n "${cid}" ]]; then
    echo "==> restart ${name} (${cid}) via kill -s SIGKILL then up -d (hold=${hold}s)"
  else
    echo "==> restart ${name} via kill -s SIGKILL then up -d (hold=${hold}s)"
  fi
  docker compose -f "${FAULTS_COMPOSE}" kill -s SIGKILL "${name}"
  if (( hold > 0 )); then
    echo "  holding ${hold}s before up (CC-4D --hold)"
    sleep "${hold}"
  fi
  docker compose -f "${FAULTS_COMPOSE}" up -d "${name}"
  echo "  restarted (named volumes retained)"
}

# S1-A-17 / E1.2: replace engine with a gRPC sink that accepts TCP and never
# answers newPayload/fcU. Overlay file is next to the main stack compose.
ENGINE_BLACKHOLE_OVERLAY="${ROOT}/docker-compose.engine-blackhole.yml"

cmd_engine_blackhole() {
  if [[ ! -f "${ENGINE_BLACKHOLE_OVERLAY}" ]]; then
    echo "error: missing overlay ${ENGINE_BLACKHOLE_OVERLAY}" >&2
    exit 2
  fi
  echo "==> engine-blackhole: rebuild engine as cc-engine-blackhole via overlay"
  echo "    compose=${FAULTS_COMPOSE}"
  echo "    overlay=${ENGINE_BLACKHOLE_OVERLAY}"
  docker compose -f "${FAULTS_COMPOSE}" -f "${ENGINE_BLACKHOLE_OVERLAY}" \
    up -d --build --no-deps engine
  local running
  running="$(
    docker compose -f "${FAULTS_COMPOSE}" -f "${ENGINE_BLACKHOLE_OVERLAY}" \
      ps --status running --services 2>/dev/null || true
  )"
  if ! grep -qx engine <<<"${running}"; then
    echo "error: engine container is not running after overlay up" >&2
    docker compose -f "${FAULTS_COMPOSE}" -f "${ENGINE_BLACKHOLE_OVERLAY}" \
      logs --tail 40 engine >&2 || true
    exit 1
  fi
  echo "  engine is a newPayload/fcU black hole (TCP accept, no answers)"
  echo
  echo "A-19 probe (do not fake; paste live output):"
  echo "  docker compose -f ${FAULTS_COMPOSE} exec -T chain /usr/local/bin/grpc-health-probe -addr=:9001"
  echo "Production 8 s deadlines + N=3 (~18 s): one RPC stays SERVING."
  echo "NOT_SERVING only if the core stays parked across N=3 production samples."
  echo "E1.2 red is cargo test -p cc-chain --test engine_blackhole_liveness \\"
  echo "  black_holed_new_payload_flips_production_probe_budget"
}

# CC-4D: docker-level wall-clock advance. Not inventoried as a fault mode —
# exercised once in the landing commit when CAP_SYS_TIME is available.
cmd_clock_jump() {
  local name="$1"
  local seconds="$2"
  require_service_name "${name}"
  if ! [[ "${seconds}" =~ ^-?[0-9]+$ ]]; then
    echo "error: seconds must be an integer, got ${seconds}" >&2
    exit 2
  fi
  local cid
  cid="$(resolve_container "${name}")"
  echo "==> clock-jump ${name} (${cid}) by ${seconds}s"

  local before after target
  before="$(docker exec "${cid}" date -u +%s 2>/dev/null || echo "")"

  # Prefer in-container date -s (needs CAP_SYS_TIME / privileged).
  local ok=0
  if [[ -n "${before}" ]]; then
    target=$(( before + seconds ))
    if docker exec -u 0 "${cid}" date -s "@${target}" >/dev/null 2>&1; then
      ok=1
      echo "  advanced via docker exec date -s @${target}"
    fi
  fi
  if (( ok == 0 )); then
    if docker exec -u 0 "${cid}" date -s "+${seconds} seconds" >/dev/null 2>&1; then
      ok=1
      echo "  advanced via docker exec date -s +${seconds} seconds"
    fi
  fi
  if (( ok == 0 )); then
    if docker run --rm --privileged --pid="container:${cid}" alpine:3.20 \
        date -s "+${seconds} seconds" >/dev/null 2>&1; then
      ok=1
      echo "  advanced via privileged alpine --pid=container sidecar"
    fi
  fi

  if (( ok == 0 )); then
    echo "error: clock-jump unsupported on this docker (CAP_SYS_TIME / date -s refused)" >&2
    echo "  honest skip: Docker Desktop and many managed runtimes share host time" >&2
    echo "  and deny per-container clock writes; re-run on a privileged Linux daemon" >&2
    exit 3
  fi

  after="$(docker exec "${cid}" date -u +%s 2>/dev/null || echo "")"
  if [[ -n "${before}" && -n "${after}" ]]; then
    local delta=$(( after - before ))
    echo "  wall-clock before=${before} after=${after} delta=${delta}s (requested ${seconds}s)"
  else
    echo "  wall-clock delta unreadable post-jump (date succeeded; scrape skipped)"
  fi
}

wait_peers_recovered() {
  local label="$1"
  local deadline=$((SECONDS + 90))
  local peers=0
  while (( SECONDS < deadline )); do
    peers="$(peer_count_node_a || echo 0)"
    if (( peers >= 1 )); then
      echo "  ${label}: node-a peers recovered: ${peers}"
      return 0
    fi
    sleep 2
  done
  echo "error: ${label}: node-a peer count did not recover (peers=${peers})" >&2
  return 1
}

# S0-B-03 / P1-A/29: the *effective* compose pin (what kill/up would use)
# must be the stack the CC-4N kill-9 clause names. Offline — no docker.
# Invoked by the clause runner (`scripts/restart-trials.sh --self-test`).
cmd_self_test() {
  local clause="${ROOT}/docs/running.md"
  local want="${DEFAULT_COMPOSE_FILE}"
  local want_base="docker-compose.yml"
  local script="${ROOT}/devnet/faults.sh"
  local pin rc clause_file

  if [[ "$(basename "${want}")" != "${want_base}" ]]; then
    echo "error: DEFAULT_COMPOSE_FILE is ${want}, want …/${want_base}" >&2
    exit 1
  fi
  if [[ ! -f "${want}" ]]; then
    echo "error: default compose file missing: ${want}" >&2
    exit 1
  fi
  if ! grep -Eq '^[[:space:]]*storage:[[:space:]]*$' "${want}"; then
    echo "error: ${want_base} has no storage service (CC-4N kill-9 targets storage)" >&2
    exit 1
  fi

  # Effective pin: documented argv, even with ambient COMPOSE_FILE set.
  pin="$(
    env -u CC_FAULTS_COMPOSE COMPOSE_FILE=/tmp/not-the-stack.yml \
      bash "${script}" --print-compose-file
  )"
  if [[ "${pin}" != "${want}" ]]; then
    echo "error: effective pin is ${pin}, want ${want} (ambient COMPOSE_FILE must be ignored)" >&2
    exit 1
  fi

  pin="$(
    env -u CC_FAULTS_COMPOSE \
      bash "${script}" -f "${ROOT}/devnet/compose.yml" --print-compose-file
  )"
  if [[ "${pin}" != "${ROOT}/devnet/compose.yml" ]]; then
    echo "error: -f did not pin to devnet/compose.yml (got ${pin})" >&2
    exit 1
  fi

  pin="$(
    CC_FAULTS_COMPOSE="${ROOT}/devnet/compose.yml" \
      bash "${script}" --print-compose-file
  )"
  if [[ "${pin}" != "${ROOT}/devnet/compose.yml" ]]; then
    echo "error: CC_FAULTS_COMPOSE did not pin (got ${pin})" >&2
    exit 1
  fi

  pin="$(
    CC_FAULTS_COMPOSE="${ROOT}/devnet/compose.yml" \
      bash "${script}" -f "${want}" --print-compose-file
  )"
  if [[ "${pin}" != "${want}" ]]; then
    echo "error: -f should win over CC_FAULTS_COMPOSE (got ${pin})" >&2
    exit 1
  fi

  rc=0
  env -u CC_FAULTS_COMPOSE bash "${script}" -f - --print-compose-file >/dev/null 2>&1 || rc=$?
  if [[ "${rc}" -eq 0 ]]; then
    echo "error: compose file '-' (stdin) must be rejected" >&2
    exit 1
  fi

  rc=0
  env -u CC_FAULTS_COMPOSE bash "${script}" -f /tmp/cc-faults-no-such-compose.yml \
    --print-compose-file >/dev/null 2>&1 || rc=$?
  if [[ "${rc}" -eq 0 ]]; then
    echo "error: missing compose file must be rejected" >&2
    exit 1
  fi

  # Leading '-' is a docker compose flag, not a service (CC-4N must not
  # `--remove-orphans` the whole project including el).
  rc=0
  ( require_service_name --remove-orphans ) >/dev/null 2>&1 || rc=$?
  if [[ "${rc}" -eq 0 ]]; then
    echo "error: --remove-orphans must be rejected as a service name" >&2
    exit 1
  fi
  rc=0
  ( require_service_name -f ) >/dev/null 2>&1 || rc=$?
  if [[ "${rc}" -eq 0 ]]; then
    echo "error: service name starting with '-' must be rejected" >&2
    exit 1
  fi
  require_service_name storage
  require_service_name node-a

  if [[ ! -f "${ENGINE_BLACKHOLE_OVERLAY}" ]]; then
    echo "error: S1-A-17 overlay missing: ${ENGINE_BLACKHOLE_OVERLAY}" >&2
    exit 1
  fi
  if ! grep -q 'SERVICE: cc-engine-blackhole' "${ENGINE_BLACKHOLE_OVERLAY}"; then
    echo "error: overlay must rebuild engine with SERVICE=cc-engine-blackhole" >&2
    exit 1
  fi
  if [[ ! -f "${ROOT}/services/engine/src/bin/blackhole.rs" ]]; then
    echo "error: cc-engine-blackhole source missing" >&2
    exit 1
  fi
  if ! grep -q 'cc-engine-blackhole' "${ROOT}/Dockerfile"; then
    echo "error: builder must emit cc-engine-blackhole for overlay SERVICE=" >&2
    exit 1
  fi
  if grep -q 'COPY --from=builder /out/cc-engine-blackhole' "${ROOT}/Dockerfile"; then
    echo "error: cc-engine-blackhole must not be copied into every runtime image" >&2
    exit 1
  fi

  clause_file="$(
    awk '
      /^### kill -9 clause/ {p=1; next}
      p && /^### / {exit}
      p && $0 ~ /default compose file:/ {
        sub(/.*default compose file:[[:space:]]*/, "")
        gsub(/[`()]/, "")
        split($0, a, /[[:space:]]+/)
        print a[1]
        exit
      }
    ' "${clause}"
  )"
  if [[ -z "${clause_file}" ]]; then
    echo "error: ${clause} kill-9 clause does not name a default compose file" >&2
    exit 1
  fi
  if [[ "${clause_file}" != "${want_base}" ]]; then
    echo "error: clause names ${clause_file}, script defaults to ${want_base}" >&2
    exit 1
  fi
  if ! awk '
      /^### kill -9 clause/ {p=1; next}
      p && /^### / {exit}
      p && /faults\.sh restart storage/ {found=1}
      END {exit found ? 0 : 1}
    ' "${clause}"; then
    echo "error: kill-9 clause must document faults.sh restart storage" >&2
    exit 1
  fi

  echo "self-test: effective pin ${want_base} agrees with CC-4N clause (COMPOSE_FILE ignored)"
  echo "self-test: PASS"
}

# Acceptance: exercise each primitive once (short durations).
# H2: offline-gap recovery must succeed *without* a process restart.
cmd_exercise_once() {
  local target="${1:-node-a}"
  require_service_name "${target}"
  echo "==> exercise-once against ${target}"

  echo "-- pause 2s --"
  cmd_pause "${target}" 2
  wait_peers_recovered "after-pause"

  echo "-- offline-gap 0.05 min (~3s) --"
  # Sub-minute for CI; clause 4 uses 10 minutes in the real scenario.
  cmd_offline_gap "${target}" 0.05
  # Recovery via continuous static-peer re-dial (fault_mode H2) — not restart.
  wait_peers_recovered "after-offline-gap"

  echo "-- restart --hold 2 --"
  cmd_restart "${target}" 2
  wait_peers_recovered "after-restart"

  echo "-- clock-jump 45 (best-effort; may exit 3 on unsupported docker) --"
  if cmd_clock_jump "${target}" 45; then
    echo "  clock-jump exercised"
  else
    local rc=$?
    if (( rc == 3 )); then
      echo "  clock-jump SKIPPED (exit 3: CAP_SYS_TIME unavailable) — recorded honestly"
    else
      return "${rc}"
    fi
  fi

  echo "exercise-once: PASS"
}

main() {
  local print_compose=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -f|--compose-file)
        [[ $# -ge 2 ]] || { echo "error: $1 needs a value" >&2; exit 2; }
        FAULTS_COMPOSE="$2"
        shift 2
        ;;
      --print-compose-file)
        print_compose=1
        shift
        ;;
      --self-test)
        cmd_self_test
        return
        ;;
      -h|--help|help)
        usage
        exit 0
        ;;
      --)
        shift
        break
        ;;
      -*)
        echo "error: unknown option: $1" >&2
        usage
        exit 2
        ;;
      *)
        break
        ;;
    esac
  done

  require_compose_file "${FAULTS_COMPOSE}"
  if (( print_compose )); then
    echo "${FAULTS_COMPOSE}"
    return
  fi

  if [[ $# -lt 1 ]]; then
    usage
    exit 2
  fi

  need docker

  local op="$1"
  shift
  case "${op}" in
    offline-gap)
      [[ $# -eq 2 ]] || { usage; exit 2; }
      cmd_offline_gap "$1" "$2"
      ;;
    pause)
      [[ $# -eq 2 ]] || { usage; exit 2; }
      cmd_pause "$1" "$2"
      ;;
    restart)
      # restart <container> [--hold <seconds>]
      local svc=""
      local hold=0
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --hold)
            [[ $# -ge 2 ]] || { echo "error: --hold needs a value" >&2; exit 2; }
            hold="$2"
            shift 2
            ;;
          -h|--help)
            usage
            exit 0
            ;;
          *)
            if [[ -n "${svc}" ]]; then
              echo "error: unexpected argument: $1" >&2
              usage
              exit 2
            fi
            svc="$1"
            shift
            ;;
        esac
      done
      [[ -n "${svc}" ]] || { usage; exit 2; }
      cmd_restart "${svc}" "${hold}"
      ;;
    clock-jump)
      [[ $# -eq 2 ]] || { usage; exit 2; }
      cmd_clock_jump "$1" "$2"
      ;;
    engine-blackhole)
      [[ $# -eq 0 ]] || { usage; exit 2; }
      cmd_engine_blackhole
      ;;
    exercise-once)
      cmd_exercise_once "${1:-node-a}"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      echo "error: unknown op ${op}" >&2
      usage
      exit 2
      ;;
  esac
}

main "$@"
