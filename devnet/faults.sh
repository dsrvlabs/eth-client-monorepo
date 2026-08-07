#!/usr/bin/env bash
# devnet/faults.sh — docker-level fault primitives (CC-2Jd).
#
# No enclave, no cross-network join recipe — our own compose network only.
#
#   offline-gap <container> <minutes>   docker network disconnect → sleep → connect
#   pause <container> <seconds>         docker pause → sleep → unpause
#   restart <container>                 docker restart
#
# Container names are compose *service* names (publisher | node-a | node-b | anchor)
# or full container ids/names from `docker ps`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT}/devnet/compose.yml"
NETWORK_NAME="${CC_DEVNET_NETWORK:-cc-devnet}"

usage() {
  cat <<EOF
Usage:
  $0 offline-gap <container> <minutes>
  $0 pause <container> <seconds>
  $0 restart <container>
  $0 exercise-once   # run each primitive once against node-a (acceptance)

Environment:
  CC_DEVNET_NETWORK   docker network name (default: cc-devnet)
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
  local name="$1"
  local cid
  cid="$(resolve_container "${name}")"
  echo "==> restart ${name} (${cid})"
  docker restart "${cid}"
  echo "  restarted"
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

  echo "-- restart --"
  cmd_restart "${target}"
  wait_peers_recovered "after-restart"

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
      [[ $# -eq 1 ]] || { usage; exit 2; }
      cmd_restart "$1"
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
