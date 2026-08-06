#!/usr/bin/env bash
# scripts/wait-healthy.sh — CC-04/2, Architecture §6.4
#
# Usage: wait-healthy.sh [deadline_seconds=90]
#
# Polls `docker compose ps --format json` until all six services report Health
# == healthy, then exits 0. On deadline: prints each service's health state and
# the last 20 log lines of any non-healthy service, then exits non-zero.
#
# Requires: bash, docker compose, jq
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

DEADLINE="${1:-90}"
if ! [[ "${DEADLINE}" =~ ^[0-9]+$ ]]; then
  echo "error: deadline must be a non-negative integer (seconds), got: ${DEADLINE}" >&2
  echo "usage: $0 [deadline_seconds=90]" >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required" >&2
  exit 1
fi

# Architecture §6.3 — the six Phase 0 services (compose service names).
EXPECTED=(chain p2p attestation engine beacon-api storage)

# Emit "service<TAB>health" lines for every compose service currently known.
# Handles both NDJSON (one object per line) and a single JSON array.
ps_health_rows() {
  local raw
  raw="$(docker compose ps --format json 2>/dev/null || true)"
  if [[ -z "${raw}" ]]; then
    return 0
  fi
  # If the first non-whitespace char is '[', treat as a JSON array; else NDJSON.
  if [[ "${raw}" =~ ^[[:space:]]*\[ ]]; then
    jq -r '.[] | "\(.Service // .Name // "unknown")\t\(.Health // .State // "")"' <<<"${raw}"
  else
    jq -r '"\(.Service // .Name // "unknown")\t\(.Health // .State // "")"' <<<"${raw}"
  fi
}

# Look up health for one service name from the current ps snapshot.
health_of() {
  local svc="$1"
  local rows
  rows="$(ps_health_rows)"
  # Prefer exact Service name match; fall back to empty = not running.
  awk -F'\t' -v s="${svc}" '$1 == s { print $2; found=1; exit } END { if (!found) print "" }' <<<"${rows}"
}

all_healthy() {
  local svc h
  for svc in "${EXPECTED[@]}"; do
    h="$(health_of "${svc}")"
    if [[ "${h}" != "healthy" ]]; then
      return 1
    fi
  done
  return 0
}

print_status() {
  local svc h
  echo "service health snapshot:" >&2
  for svc in "${EXPECTED[@]}"; do
    h="$(health_of "${svc}")"
    if [[ -z "${h}" ]]; then
      h="(not running)"
    fi
    printf "  %-12s %s\n" "${svc}" "${h}" >&2
  done
}

print_unhealthy_logs() {
  local svc h
  for svc in "${EXPECTED[@]}"; do
    h="$(health_of "${svc}")"
    if [[ "${h}" != "healthy" ]]; then
      echo "----- last 20 log lines: ${svc} (health=${h:-absent}) -----" >&2
      docker compose logs --tail=20 "${svc}" >&2 || true
    fi
  done
}

echo "waiting up to ${DEADLINE}s for all six services to become healthy..." >&2
start_ts="$(date +%s)"
deadline_ts=$((start_ts + DEADLINE))

while true; do
  now="$(date +%s)"
  if all_healthy; then
    elapsed=$((now - start_ts))
    echo "all six services healthy after ${elapsed}s" >&2
    exit 0
  fi
  if (( now >= deadline_ts )); then
    echo "error: deadline of ${DEADLINE}s exceeded; not all services healthy" >&2
    print_status
    print_unhealthy_logs
    exit 1
  fi
  sleep 1
done
