#!/usr/bin/env bash
# corpus-from-capture.sh — CC-22e / R-9
#
# Convert a gossip capture (CC-2Ja chain layout and/or flat topic-family SSZ
# files) into the hostile-input seed corpus.
#
# Usage:
#   ./scripts/corpus-from-capture.sh <capture_dir> [out_seeds_dir]
#
# Defaults:
#   out_seeds_dir = services/p2p/tests/fixtures/corpus/seeds
#
# Accepted capture layouts (any mix):
#   1) slot_*/block.ssz              → beacon_block.ssz
#      slot_*/column_XXX.ssz         → data_column_sidecar.ssz (first wins)
#   2) **/<topic_family>.ssz         → that family seed
#
# Exit 0 on success; non-zero if capture_dir missing or no seeds produced.
# Portable: bash 3.2+ (no associative arrays).

set -euo pipefail

usage() {
  echo "usage: $0 <capture_dir> [out_seeds_dir]" >&2
  exit 2
}

[ $# -ge 1 ] && [ $# -le 2 ] || usage

CAPTURE="$(cd "$1" && pwd)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${2:-$ROOT/services/p2p/tests/fixtures/corpus/seeds}"
mkdir -p "$OUT"

FAMILIES="beacon_block
beacon_aggregate_and_proof
beacon_attestation
data_column_sidecar
sync_committee_contribution_and_proof
sync_committee
voluntary_exit
proposer_slashing
attester_slashing
bls_to_execution_change"

written=0

# Prefer the earliest slot's block / first column as the seed objects.
block="$(find "$CAPTURE" -type f -path '*/slot_*/block.ssz' 2>/dev/null | sort | head -n1 || true)"
if [ -n "$block" ]; then
  cp -f "$block" "$OUT/beacon_block.ssz"
  written=$((written + 1))
  echo "beacon_block ← $block"
fi

col="$(find "$CAPTURE" -type f -path '*/slot_*/column_*.ssz' 2>/dev/null | sort | head -n1 || true)"
if [ -n "$col" ]; then
  cp -f "$col" "$OUT/data_column_sidecar.ssz"
  written=$((written + 1))
  echo "data_column_sidecar ← $col"
fi

# Flat / nested topic-family files (ops/, root, anywhere under capture).
# shellcheck disable=SC2086
echo "$FAMILIES" | while IFS= read -r fam; do
  [ -n "$fam" ] || continue
  f="$(find "$CAPTURE" -type f -name "${fam}.ssz" 2>/dev/null | sort | head -n1 || true)"
  if [ -n "$f" ]; then
    cp -f "$f" "$OUT/${fam}.ssz"
    echo "${fam} ← $f"
  fi
done

# Recount written seeds (subshell above cannot update written).
count=0
for fam in $FAMILIES; do
  if [ -f "$OUT/${fam}.ssz" ]; then
    count=$((count + 1))
  fi
done

if [ "$count" -eq 0 ]; then
  echo "error: no seed objects found under $CAPTURE" >&2
  exit 1
fi

echo "corpus-from-capture: $count seed file(s) present under $OUT"

missing=0
for fam in $FAMILIES; do
  if [ ! -f "$OUT/${fam}.ssz" ]; then
    echo "warn: missing seed for family: $fam" >&2
    missing=$((missing + 1))
  fi
done
if [ "$missing" -gt 0 ]; then
  echo "warn: $missing family seed(s) still missing under $OUT" >&2
fi
exit 0
