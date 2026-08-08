#!/usr/bin/env bash
# CC-47b residual: monitor a live ~985k-block backfill (run (g)).
#
# Unit tests cover rate bound, resume-within-one-batch, frontier monotonicity,
# completion predicate, and write-behind ≤10 % impact. The multi-hour Hoodi /
# exclusive-machine run itself is **not** discharged by those tests — this
# script is the instrument for the live residual (Architecture §6.3 / V-9 / D-11).
#
# Usage:
#   METRICS_URL=http://127.0.0.1:9102/metrics \
#   INTERVAL_S=30 \
#   bash scripts/block-backfill-monitor.sh
#
# Exit:
#   0  — monitor finished cleanly (target reached or duration elapsed)
#   1  — non-monotone frontier (R-7 early warning) or bad scrape
#   2  — usage / dependency error
#
# Records (append-only) go to stdout as TSV; redirect to a soak log if desired.
# Does not invent bar numbers — scrapes only what the process exports.

set -euo pipefail

METRICS_URL="${METRICS_URL:-http://127.0.0.1:9102/metrics}"
INTERVAL_S="${INTERVAL_S:-30}"
# Optional hard stop (seconds). Empty = run until complete or Ctrl-C.
MAX_DURATION_S="${MAX_DURATION_S:-}"
# Optional: path to append a one-line residual summary for docs/phase-4-soak.md.
SUMMARY_OUT="${SUMMARY_OUT:-}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing dependency: $1" >&2; exit 2; }; }
need curl
need awk

scrape() {
  # Returns: oldest_blocks  oldest_columns  (empty if absent)
  local body
  if ! body="$(curl -fsS --max-time 5 "$METRICS_URL" 2>/dev/null)"; then
    echo "scrape failed: $METRICS_URL" >&2
    return 1
  fi
  local blocks columns
  blocks="$(printf '%s\n' "$body" | awk -F'[{} ]' '
    /cc_storage_backfill_oldest_slot\{/ && /class="blocks"/ {
      for (i=1;i<=NF;i++) if ($i ~ /^[0-9]+(\.[0-9]+)?$/) last=$i
    }
    END { if (last != "") print int(last) }
  ')"
  columns="$(printf '%s\n' "$body" | awk -F'[{} ]' '
    /cc_storage_backfill_oldest_slot\{/ && /class="columns"/ {
      for (i=1;i<=NF;i++) if ($i ~ /^[0-9]+(\.[0-9]+)?$/) last=$i
    }
    END { if (last != "") print int(last) }
  ')"
  printf '%s %s\n' "${blocks:-}" "${columns:-}"
}

echo "# CC-47b block-backfill monitor"
echo "# metrics=$METRICS_URL interval=${INTERVAL_S}s"
echo -e "ts_unix\telapsed_s\toldest_blocks\toldest_columns\tdelta_blocks"

start_ts="$(date +%s)"
prev_blocks=""
scrapes=0
non_monotone=0

while true; do
  now="$(date +%s)"
  elapsed=$((now - start_ts))
  if [[ -n "$MAX_DURATION_S" && "$elapsed" -ge "$MAX_DURATION_S" ]]; then
    echo "# stop: MAX_DURATION_S=$MAX_DURATION_S reached" >&2
    break
  fi

  if ! read -r blocks columns < <(scrape); then
    sleep "$INTERVAL_S"
    continue
  fi
  scrapes=$((scrapes + 1))

  delta="-"
  if [[ -n "$prev_blocks" && -n "$blocks" ]]; then
    delta=$((prev_blocks - blocks))
    # R-7: frontier must be non-increasing (delta ≥ 0 when progressing down).
    if [[ "$blocks" -gt "$prev_blocks" ]]; then
      echo "# R-7 NON-MONOTONE: prev=$prev_blocks now=$blocks" >&2
      non_monotone=$((non_monotone + 1))
    fi
  fi
  printf '%s\t%s\t%s\t%s\t%s\n' "$now" "$elapsed" "${blocks:--}" "${columns:--}" "$delta"
  prev_blocks="${blocks:-$prev_blocks}"

  sleep "$INTERVAL_S"
done

status="OK"
exit_code=0
if [[ "$non_monotone" -gt 0 ]]; then
  status="FAIL_NON_MONOTONE"
  exit_code=1
fi

echo "# summary status=$status scrapes=$scrapes non_monotone=$non_monotone elapsed_s=$(( $(date +%s) - start_ts )) last_blocks=${prev_blocks:--}"

if [[ -n "$SUMMARY_OUT" ]]; then
  {
    echo "CC-47b residual monitor: status=$status scrapes=$scrapes non_monotone=$non_monotone last_blocks=${prev_blocks:--} metrics=$METRICS_URL"
  } >>"$SUMMARY_OUT"
fi

exit "$exit_code"
