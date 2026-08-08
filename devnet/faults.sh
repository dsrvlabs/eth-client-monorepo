#!/usr/bin/env bash
# devnet/faults.sh — docker-level fault primitives (CC-2Jd, CC-4N, CC-4D).
#
# No enclave, no cross-network join recipe — our own compose network only.
#
#   offline-gap <container> <minutes>   docker network disconnect → sleep → connect
#   pause <container> <seconds>         docker pause → sleep → unpause
#   restart <container> [--hold <s>]    kill -s SIGKILL, optional hold, then up -d
#   clock-jump <container> <seconds>    advance container wall clock (CC-4D)
#
# Container names are compose *service* names (publisher | node-a | node-b | anchor)
# or full container ids/names from `docker ps`.
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
COMPOSE_FILE="${ROOT}/devnet/compose.yml"
NETWORK_NAME="${CC_DEVNET_NETWORK:-cc-devnet}"

usage() {
  cat <<EOF
Usage:
  $0 offline-gap <container> <minutes>
  $0 pause <container> <seconds>
  $0 restart <container> [--hold <seconds>]
  $0 clock-jump <container> <seconds>
  $0 exercise-once   # run each primitive once against node-a (acceptance)

Environment:
  CC_DEVNET_NETWORK   docker network name (default: cc-devnet)

restart uses: docker compose kill -s SIGKILL <service>
              [sleep <hold>]
              docker compose up -d <service>
  — never docker compose down (CC-4N; named volumes must survive the kill-9 clause).

clock-jump advances the container wall clock by <seconds> (docker exec date -s
or privileged sidecar). Requires CAP_SYS_TIME; may be unavailable on Docker
Desktop — report honestly rather than inventing a watermark.
EOF
}

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}
need docker

resolve_container() {
  local name="$1"
  # Prefer compose service name resolution.
  local cid
  cid="$(docker compose -f "${COMPOSE_FILE}" ps -q "${name}" 2>/dev/null || true)"
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
  docker compose -f "${COMPOSE_FILE}" kill -s SIGKILL "${name}"
  if (( hold > 0 )); then
    echo "  holding ${hold}s before up (CC-4D --hold)"
    sleep "${hold}"
  fi
  docker compose -f "${COMPOSE_FILE}" up -d "${name}"
  echo "  restarted (named volumes retained)"
}

# CC-4D: docker-level wall-clock advance. Not inventoried as a fault mode —
# exercised once in the landing commit when CAP_SYS_TIME is available.
cmd_clock_jump() {
  local name="$1"
  local seconds="$2"
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

# Acceptance: exercise each primitive once (short durations).
# H2: offline-gap recovery must succeed *without* a process restart.
cmd_exercise_once() {
  local target="${1:-node-a}"
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
  if [[ $# -lt 1 ]]; then
    usage
    exit 2
  fi
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
