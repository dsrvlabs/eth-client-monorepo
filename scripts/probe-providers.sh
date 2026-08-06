#!/usr/bin/env bash
# scripts/probe-providers.sh — CC-19a / R-4 Hoodi checkpoint provider probe
#
# For each enumerated provider, records:
#   - whether it answers (HTTP reachability of genesis + finalized block)
#   - Eth-Consensus-Version on the finalized-block SSZ response
#   - whether by-root state resolve works (GET .../states/{state_root})
#
# Output: appends a dated markdown table to docs/running.md under the named
# section `## Checkpoint providers` (append-only).
#
# Never invoked by cargo build or cargo nextest. Operators run this daily from
# M1.1 onward so soak day starts from a table rather than from discovery.
#
# Usage:
#   bash scripts/probe-providers.sh
#   bash scripts/probe-providers.sh --docs docs/running.md
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCS="${REPO_ROOT}/docs/running.md"
SECTION="## Checkpoint providers"
UA="cc-probe-providers/0.1 (eth-client-monorepo CC-19a)"
CONNECT_TIMEOUT=5
MAX_TIME=60
MAX_BLOCK_BYTES=$((8 * 1024 * 1024))
# State HEAD/GET size probe — only need status + version header for by-root.
MAX_STATE_PROBE_BYTES=$((4 * 1024 * 1024))

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docs)
      DOCS="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

for tool in curl python3; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "error: required tool not found: ${tool}" >&2
    exit 1
  fi
done

# Seven Hoodi checkpoint providers from PRD CC-19 (plus eth-clients community
# list entries that serve checkpoint SSZ). Order matches Architecture §8.1 /
# fetch-hoodi-fixtures checkpoint-oriented endpoints.
PROVIDERS=(
  "https://checkpoint-sync.hoodi.ethpandaops.io"
  "https://hoodi.beaconstate.ethstaker.cc"
  "https://hoodi.checkpoint.sigp.io"
  "https://beaconstate-hoodi.chainsafe.io"
  "https://hoodi.beaconstate.info"
  "https://hoodi-checkpoint-sync.stakely.io"
  "https://hoodi-checkpoint-sync.attestant.io"
)

log() { echo "probe-providers: $*" >&2; }

# http_code body_file [extra curl args...]
# Writes body to body_file; prints "CODE|HEADER_VERSION" on stdout.
probe_ssz() {
  local url="$1" out="$2"
  shift 2
  local hdr
  hdr="$(mktemp)"
  local code
  code=$(curl -sS -L \
    --proto '=https' \
    --proto-redir '=https' \
    -A "${UA}" \
    -H "Accept: application/octet-stream" \
    --connect-timeout "${CONNECT_TIMEOUT}" \
    --max-time "${MAX_TIME}" \
    --max-filesize "${MAX_BLOCK_BYTES}" \
    -D "${hdr}" \
    -o "${out}" \
    -w "%{http_code}" \
    "$@" \
    "${url}" 2>/dev/null) || code="000"
  local version
  version=$(python3 - "${hdr}" <<'PY'
import sys
from pathlib import Path
text = Path(sys.argv[1]).read_text(errors="replace")
ver = ""
for line in text.splitlines():
    if line.lower().startswith("eth-consensus-version:"):
        ver = line.split(":", 1)[1].strip()
        break
print(ver)
PY
)
  rm -f "${hdr}"
  echo "${code}|${version}"
}

probe_json() {
  local url="$1" out="$2"
  local code
  code=$(curl -sS -L \
    --proto '=https' \
    --proto-redir '=https' \
    -A "${UA}" \
    -H "Accept: application/json" \
    --connect-timeout "${CONNECT_TIMEOUT}" \
    --max-time "${MAX_TIME}" \
    --max-filesize "$((2 * 1024 * 1024))" \
    -o "${out}" \
    -w "%{http_code}" \
    "${url}" 2>/dev/null) || code="000"
  echo "${code}"
}

# Extract state_root hex from a SignedBeaconBlock SSZ (minimal offset walk).
# SignedBeaconBlock: offset(message)=4 bytes LE, signature 96, message starts at 100.
# BeaconBlock fixed prefix: slot(8)+proposer(8)+parent(32)+state_root(32)=80 before body offset.
state_root_from_block_ssz() {
  local path="$1"
  python3 - "${path}" <<'PY'
import sys
from pathlib import Path
data = Path(sys.argv[1]).read_bytes()
if len(data) < 100 + 80:
    print("")
    sys.exit(0)
msg_off = int.from_bytes(data[0:4], "little")
if msg_off != 100:
    # Still try absolute if offset is sane
    if msg_off + 80 > len(data):
        print("")
        sys.exit(0)
msg = data[msg_off:]
# BeaconBlock fixed prefix: slot(8) + proposer_index(8) + parent_root(32) + state_root(32)
state_root = msg[48:80]
print("0x" + state_root.hex())
PY
}

TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT

DATE_UTC="$(date -u +%Y-%m-%d)"
TIME_UTC="$(date -u +%H:%M:%SZ)"
ROWS=()

log "probing ${#PROVIDERS[@]} providers (${DATE_UTC} ${TIME_UTC})"

for base in "${PROVIDERS[@]}"; do
  base="${base%/}"
  name="${base#https://}"
  answers="no"
  version="—"
  by_root="no"
  notes=""

  gen_out="${TMPDIR}/genesis.json"
  gen_code=$(probe_json "${base}/eth/v1/beacon/genesis" "${gen_out}")
  if [[ "${gen_code}" != "200" ]]; then
    notes="genesis HTTP ${gen_code}"
    log "  ${name}: answers=no (${notes})"
    ROWS+=("| ${DATE_UTC} | \`${name}\` | ${answers} | ${version} | ${by_root} | ${notes} |")
    continue
  fi

  block_out="${TMPDIR}/block.ssz"
  result=$(probe_ssz "${base}/eth/v2/beacon/blocks/finalized" "${block_out}")
  code="${result%%|*}"
  block_version="${result#*|}"

  if [[ "${code}" != "200" ]]; then
    version="${block_version:-—}"
    notes="finalized block HTTP ${code}"
    log "  ${name}: answers=no (${notes})"
    ROWS+=("| ${DATE_UTC} | \`${name}\` | ${answers} | ${version} | ${by_root} | ${notes} |")
    continue
  fi
  answers="yes"

  state_root=$(state_root_from_block_ssz "${block_out}")
  if [[ -z "${state_root}" ]]; then
    version="${block_version:-—}"
    notes="could not parse state_root from block SSZ"
    log "  ${name}: answers=yes version=${version} by-root=no (${notes})"
    ROWS+=("| ${DATE_UTC} | \`${name}\` | ${answers} | ${version} | ${by_root} | ${notes} |")
    continue
  fi

  # By-root: request only headers + small body bound (many servers stream full state).
  # We accept 200 as "resolves"; 404 as "alias-only".
  state_out="${TMPDIR}/state.ssz"
  state_hdr="${TMPDIR}/state.hdr"
  state_result=$(
    curl -sS -L \
      --proto '=https' \
      --proto-redir '=https' \
      -A "${UA}" \
      -H "Accept: application/octet-stream" \
      --connect-timeout "${CONNECT_TIMEOUT}" \
      --max-time "${MAX_TIME}" \
      --max-filesize "${MAX_STATE_PROBE_BYTES}" \
      -D "${state_hdr}" \
      -o "${state_out}" \
      -w "%{http_code}" \
      "${base}/eth/v2/debug/beacon/states/${state_root}" 2>/dev/null
  ) || state_result="000"

  state_version=$(python3 - "${state_hdr}" <<'PY' 2>/dev/null || true
import sys
from pathlib import Path
p = Path(sys.argv[1])
if not p.is_file():
    print("")
    raise SystemExit(0)
text = p.read_text(errors="replace")
ver = ""
for line in text.splitlines():
    if line.lower().startswith("eth-consensus-version:"):
        ver = line.split(":", 1)[1].strip()
        break
print(ver)
PY
)
  # Prefer block header; fall back to state (checkpoint servers often omit on block).
  if [[ -n "${block_version}" ]]; then
    version="${block_version}"
  elif [[ -n "${state_version}" ]]; then
    version="${state_version}"
  else
    version="—"
  fi

  case "${state_result}" in
    200)
      by_root="yes"
      notes="state_root=${state_root:0:18}…"
      ;;
    404)
      by_root="no"
      notes="by-root 404 (finalized alias only); state_root=${state_root:0:18}…"
      ;;
    000)
      # max-filesize abort or transport — if server started streaming, by-root
      # likely works. Treat connect failure as unknown.
      if [[ -s "${state_out}" ]]; then
        by_root="yes"
        notes="by-root stream started (capped); state_root=${state_root:0:18}…"
      else
        by_root="unknown"
        notes="by-root transport/timeout; state_root=${state_root:0:18}…"
      fi
      ;;
    *)
      by_root="no"
      notes="by-root HTTP ${state_result}; state_root=${state_root:0:18}…"
      ;;
  esac

  log "  ${name}: answers=${answers} version=${version} by-root=${by_root}"
  ROWS+=("| ${DATE_UTC} | \`${name}\` | ${answers} | ${version} | ${by_root} | ${notes} |")
done

# ── append to docs/running.md ────────────────────────────────────────────────
if [[ ! -f "${DOCS}" ]]; then
  echo "error: docs file not found: ${DOCS}" >&2
  exit 1
fi

if ! grep -q "^${SECTION}\$" "${DOCS}"; then
  log "creating section ${SECTION} in ${DOCS}"
  {
    echo ""
    echo "${SECTION}"
    echo ""
    echo "Append-only probe log (R-4 / CC-19a). Generated by"
    echo "\`bash scripts/probe-providers.sh\`. Each run adds a dated table;"
    echo "do not rewrite prior rows."
    echo ""
    echo "Code must **not** assume by-root works everywhere — fallback to the"
    echo "\`finalized\` alias is part of the design (Architecture §8.2)."
    echo ""
  } >> "${DOCS}"
fi

{
  echo ""
  echo "### Probe ${DATE_UTC} ${TIME_UTC}"
  echo ""
  echo "| Date (UTC) | Provider | Answers | Eth-Consensus-Version | By-root | Notes |"
  echo "|---|---|---|---|---|---|"
  for row in "${ROWS[@]}"; do
    echo "${row}"
  done
  echo ""
} >> "${DOCS}"

log "appended ${#ROWS[@]} rows to ${DOCS}"
log "done"
