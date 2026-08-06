#!/usr/bin/env bash
# scripts/prove-mutual-health.sh — CC-04b, Architecture §6.5
#
# Success-metric clause 2 (mutual health is a *test*, not a phrase):
#   1. Assert all six healthy (delegates to wait-healthy.sh).
#   2. docker compose stop chain.
#   3. Within 15 s: all five dependents report aggregate "" = NOT_SERVING via
#      grpc-health-probe from the host, and each dependent's
#      cc_peer_health{peer="chain"} == 0 on /metrics.
#   4. docker compose start chain; within 30 s all six SERVING and every
#      cc_peer_health gauge is 1.
#   5. Non-zero exit names the offending service and its observed state.
#
# Requires: bash, docker compose, curl, jq (via wait-healthy.sh), grpc-health-probe
#           (or grpc_health_probe) on the host PATH.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

# Architecture §6.3 — gRPC / metrics ports (metrics = gRPC + 100).
ALL_SERVICES=(chain p2p attestation engine beacon-api storage)
# Every service that lists chain as a peer in docker-compose.yml.
DEPENDENTS=(p2p attestation engine storage beacon-api)

NOT_SERVING_DEADLINE_S=15
RECOVERY_DEADLINE_S=30

grpc_port() {
  case "$1" in
    chain) echo 9001 ;;
    p2p) echo 9002 ;;
    attestation) echo 9003 ;;
    engine) echo 9004 ;;
    beacon-api) echo 9005 ;;
    storage) echo 9006 ;;
    *)
      echo "error: unknown service for grpc_port: $1" >&2
      return 1
      ;;
  esac
}

metrics_port() {
  case "$1" in
    chain) echo 9101 ;;
    p2p) echo 9102 ;;
    attestation) echo 9103 ;;
    engine) echo 9104 ;;
    beacon-api) echo 9105 ;;
    storage) echo 9106 ;;
    *)
      echo "error: unknown service for metrics_port: $1" >&2
      return 1
      ;;
  esac
}

die() {
  echo "error: $*" >&2
  exit 1
}

# Resolve host-side grpc-health-probe (Dockerfile installs as grpc-health-probe).
resolve_probe() {
  if command -v grpc-health-probe >/dev/null 2>&1; then
    echo "grpc-health-probe"
  elif command -v grpc_health_probe >/dev/null 2>&1; then
    echo "grpc_health_probe"
  else
    die "grpc-health-probe (or grpc_health_probe) is required on PATH (host-side probe per Architecture §6.5)"
  fi
}

PROBE="$(resolve_probe)"

if ! command -v curl >/dev/null 2>&1; then
  die "curl is required"
fi

if ! command -v docker >/dev/null 2>&1; then
  die "docker is required"
fi

# Probe aggregate health "" (no -service flag). Prints SERVING | NOT_SERVING | OTHER:<detail>.
# grpc-health-probe: exit 0 ⇒ SERVING; non-zero with "status: NOT_SERVING" ⇒ NOT_SERVING.
aggregate_health() {
  local svc="$1"
  local port out rc
  port="$(grpc_port "${svc}")"
  set +e
  out="$("${PROBE}" -addr="127.0.0.1:${port}" 2>&1)"
  rc=$?
  set -e
  if [[ "${rc}" -eq 0 ]]; then
    echo "SERVING"
    return 0
  fi
  if grep -qiE 'status:[[:space:]]*NOT_SERVING' <<<"${out}"; then
    echo "NOT_SERVING"
    return 0
  fi
  # Collapse multi-line probe noise into one token for error reports.
  local detail
  detail="$(tr '\n' ' ' <<<"${out}" | sed 's/[[:space:]]\+/ /g' | sed 's/[[:space:]]*$//')"
  echo "OTHER:rc=${rc}:${detail}"
}

# Read cc_peer_health{peer="<peer>"} from a service's /metrics. Prints the gauge
# value, or empty if the series is missing / metrics unreachable.
peer_health_gauge() {
  local svc="$1"
  local peer="$2"
  local port text
  port="$(metrics_port "${svc}")"
  set +e
  text="$(curl -fsS --max-time 2 "http://127.0.0.1:${port}/metrics" 2>/dev/null)"
  set -e
  if [[ -z "${text}" ]]; then
    echo ""
    return 0
  fi
  # prometheus-client text format: cc_peer_health{service="…",peer="…"} 0
  # Label order is not stable — match the peer label and take the last field.
  awk -v peer="${peer}" '
    $0 ~ /^cc_peer_health\{/ && $0 ~ ("peer=\"" peer "\"") {
      print $NF
      exit
    }
  ' <<<"${text}"
}

# Every non-comment cc_peer_health sample on this service must equal want (1 or 0).
# Services with no peers (chain) have no series — treated as vacuously OK.
all_peer_health_equals() {
  local svc="$1"
  local want="$2"
  local port text line val
  port="$(metrics_port "${svc}")"
  set +e
  text="$(curl -fsS --max-time 2 "http://127.0.0.1:${port}/metrics" 2>/dev/null)"
  set -e
  if [[ -z "${text}" ]]; then
    return 1
  fi
  while IFS= read -r line; do
    [[ "${line}" =~ ^cc_peer_health\{ ]] || continue
    val="${line##* }"
    if [[ "${val}" != "${want}" ]]; then
      return 1
    fi
  done <<<"${text}"
  return 0
}

snapshot_dependents() {
  local svc h g
  echo "dependent snapshot:" >&2
  for svc in "${DEPENDENTS[@]}"; do
    h="$(aggregate_health "${svc}")"
    g="$(peer_health_gauge "${svc}" "chain")"
    printf "  %-12s health=%s  cc_peer_health{peer=\"chain\"}=%s\n" \
      "${svc}" "${h}" "${g:-<missing>}" >&2
  done
}

snapshot_all() {
  local svc h
  echo "service health snapshot:" >&2
  for svc in "${ALL_SERVICES[@]}"; do
    h="$(aggregate_health "${svc}")"
    printf "  %-12s %s\n" "${svc}" "${h}" >&2
  done
}

# ── 1. Preconditions ─────────────────────────────────────────────────────
echo "prove-mutual-health: asserting all six healthy via wait-healthy.sh..." >&2
bash "${ROOT}/scripts/wait-healthy.sh"

# ── 2. Stop chain ────────────────────────────────────────────────────────
echo "prove-mutual-health: docker compose stop chain" >&2
docker compose stop chain

# ── 3. Dependents → NOT_SERVING + peer_health{chain}=0 within 15 s ───────
echo "prove-mutual-health: waiting up to ${NOT_SERVING_DEADLINE_S}s for five dependents NOT_SERVING..." >&2
start_ts="$(date +%s)"
deadline_ts=$((start_ts + NOT_SERVING_DEADLINE_S))
not_serving_ok=0

while true; do
  now="$(date +%s)"
  all_down=1
  for svc in "${DEPENDENTS[@]}"; do
    h="$(aggregate_health "${svc}")"
    g="$(peer_health_gauge "${svc}" "chain")"
    if [[ "${h}" != "NOT_SERVING" || "${g}" != "0" ]]; then
      all_down=0
      break
    fi
  done
  if [[ "${all_down}" -eq 1 ]]; then
    elapsed=$((now - start_ts))
    echo "prove-mutual-health: all five dependents NOT_SERVING with peer=chain 0 after ${elapsed}s" >&2
    not_serving_ok=1
    break
  fi
  if (( now >= deadline_ts )); then
    break
  fi
  sleep 0.5
done

if [[ "${not_serving_ok}" -ne 1 ]]; then
  echo "error: deadline of ${NOT_SERVING_DEADLINE_S}s exceeded; dependents did not all flip to NOT_SERVING with cc_peer_health{peer=\"chain\"}=0" >&2
  snapshot_dependents
  for svc in "${DEPENDENTS[@]}"; do
    h="$(aggregate_health "${svc}")"
    g="$(peer_health_gauge "${svc}" "chain")"
    if [[ "${h}" != "NOT_SERVING" || "${g}" != "0" ]]; then
      echo "error: offending service=${svc} observed_health=${h} cc_peer_health{peer=\"chain\"}=${g:-<missing>}" >&2
    fi
  done
  exit 1
fi

# ── 4. Start chain; recover within 30 s ──────────────────────────────────
echo "prove-mutual-health: docker compose start chain" >&2
docker compose start chain

echo "prove-mutual-health: waiting up to ${RECOVERY_DEADLINE_S}s for all six SERVING and peer gauges = 1..." >&2
start_ts="$(date +%s)"
deadline_ts=$((start_ts + RECOVERY_DEADLINE_S))
recovered=0

while true; do
  now="$(date +%s)"
  all_up=1
  for svc in "${ALL_SERVICES[@]}"; do
    h="$(aggregate_health "${svc}")"
    if [[ "${h}" != "SERVING" ]]; then
      all_up=0
      break
    fi
    if ! all_peer_health_equals "${svc}" "1"; then
      all_up=0
      break
    fi
  done
  if [[ "${all_up}" -eq 1 ]]; then
    elapsed=$((now - start_ts))
    echo "prove-mutual-health: all six SERVING with every cc_peer_health=1 after ${elapsed}s" >&2
    recovered=1
    break
  fi
  if (( now >= deadline_ts )); then
    break
  fi
  sleep 0.5
done

if [[ "${recovered}" -ne 1 ]]; then
  echo "error: deadline of ${RECOVERY_DEADLINE_S}s exceeded; stack did not fully recover" >&2
  snapshot_all
  for svc in "${ALL_SERVICES[@]}"; do
    h="$(aggregate_health "${svc}")"
    if [[ "${h}" != "SERVING" ]]; then
      echo "error: offending service=${svc} observed_health=${h}" >&2
      continue
    fi
    if ! all_peer_health_equals "${svc}" "1"; then
      # Report each non-1 gauge for this service.
      port="$(metrics_port "${svc}")"
      set +e
      text="$(curl -fsS --max-time 2 "http://127.0.0.1:${port}/metrics" 2>/dev/null)"
      set -e
      while IFS= read -r line; do
        [[ "${line}" =~ ^cc_peer_health\{ ]] || continue
        val="${line##* }"
        if [[ "${val}" != "1" ]]; then
          echo "error: offending service=${svc} observed_metric=${line}" >&2
        fi
      done <<<"${text}"
    fi
  done
  exit 1
fi

echo "prove-mutual-health: ok" >&2
exit 0
