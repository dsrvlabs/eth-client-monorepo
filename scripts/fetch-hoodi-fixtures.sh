#!/usr/bin/env bash
# scripts/fetch-hoodi-fixtures.sh — CC-10b Hoodi fixture rig
#
# Fetches a finalized SignedBeaconBlock + BeaconState (SSZ) and a 40-slot
# consecutive block sequence from a public Hoodi provider into a cache
# outside the build tree. Commits only expected roots (written into
# crates/types/tests/fixtures/*.toml); SSZ bytes never enter git.
#
# ─────────────────────────────────────────────────────────────────────────────
# Consumption contract (CC-10b):
#
#   Env var        HOODI_FIXTURES_CACHE (optional)
#   Cache root     ${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}
#   Slot dir       <cache root>/<anchor_slot>/
#   Artifacts      signed_beacon_block.ssz, beacon_state.ssz (≥ 150 MB),
#                  sequence/<slot>.ssz (non-empty slots only)
#   Readiness      artifacts present, state ≥ 150 MB, and every committed
#                  SHA-256 matches (block / state / sequence SSZ)
#   Failure mode   panic/error with the literal string:
#                    run scripts/fetch-hoodi-fixtures.sh
#                  — never an implicit download from Rust tests
#
# Modes:
#   default (pin present)   restore the *committed* pin into the cache;
#                           verify digests against hoodi-anchor.toml /
#                           hoodi-sequence.toml; **never rewrite** manifests
#   default (no pin)        bootstrap: fetch current finalized, write manifests
#   --force                 re-pin from current finalized and rewrite manifests
#                           (intentional pin advance; update README cache key)
#
# This script is never invoked by cargo build or cargo nextest.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

FORCE=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $arg" >&2
      echo "usage: $0 [--force]" >&2
      exit 2
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
FIXTURES_DIR="${REPO_ROOT}/crates/types/tests/fixtures"
ANCHOR_TOML="${FIXTURES_DIR}/hoodi-anchor.toml"
SEQUENCE_TOML="${FIXTURES_DIR}/hoodi-sequence.toml"

# ── preflight ────────────────────────────────────────────────────────────────
for tool in curl python3; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "error: required tool not found: ${tool}" >&2
    exit 1
  fi
done

# SHA-256: sha256sum (Linux) or shasum -a 256 (macOS)
if command -v sha256sum >/dev/null 2>&1; then
  sha256_file() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256_file() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  echo "error: need sha256sum or shasum" >&2
  exit 1
fi

CACHE_ROOT="${HOODI_FIXTURES_CACHE:-${HOME}/.cache/cc-hoodi-fixtures}"
# Resolve to absolute for the under-repo guard.
CACHE_ROOT="$(cd / && python3 -c "import os,sys; print(os.path.realpath(os.path.expanduser(sys.argv[1])))" "${CACHE_ROOT}")"
UA="cc-hoodi-fixtures/0.1 (eth-client-monorepo CC-10b)"
MIN_STATE_BYTES=$((150 * 1024 * 1024))
# Hard caps (curl --max-filesize): state ~150–250 MB expected; blocks a few 10s of KB.
MAX_STATE_BYTES=$((400 * 1024 * 1024))
MAX_BLOCK_BYTES=$((8 * 1024 * 1024))
SEQUENCE_LEN=40
SLOTS_PER_EPOCH=32

# Provider list from PRD CC-19 (checkpoint endpoints) plus full beacon APIs that
# serve historical headers/blocks for the 40-slot sequence.
# Order: full beacon APIs first (needed for sequence), then checkpoint servers.
PROVIDERS=(
  "https://beacon.hoodi.ethpandaops.io"
  "https://ethereum-hoodi-beacon-api.publicnode.com"
  "https://rpc.hoodi.ethpandaops.io"
  "https://checkpoint-sync.hoodi.ethpandaops.io"
  "https://hoodi.beaconstate.ethstaker.cc"
  "https://hoodi.checkpoint.sigp.io"
  "https://beaconstate-hoodi.chainsafe.io"
  "https://hoodi-checkpoint-sync.stakely.io"
  "https://hoodi-checkpoint-sync.attestant.io"
)

log() { echo "fetch-hoodi-fixtures: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }

# Refuse to write SSZ under the git worktree (accidental HOODI_FIXTURES_CACHE=.).
case "${CACHE_ROOT}/" in
  "${REPO_ROOT}"/*)
    die "HOODI_FIXTURES_CACHE must be outside the repo (${REPO_ROOT}); got ${CACHE_ROOT}"
    ;;
esac

mkdir -p "${CACHE_ROOT}"
mkdir -p "${FIXTURES_DIR}"

# ── HTTP helpers (HTTPS-only, size-bounded; mirror fetch-spec-vectors.sh) ────
http_get_file() {
  # http_get_file URL OUT_PATH [MAX_BYTES]
  # Returns HTTP status code (or 000 on transport failure). Do not use curl --fail:
  # with -w "%{http_code}", a non-zero exit + `|| echo 000` concatenates (e.g. 404000).
  local url="$1" out="$2" max_bytes="${3:-${MAX_BLOCK_BYTES}}"
  local code
  code=$(curl -sS -L \
    --proto '=https' \
    --proto-redir '=https' \
    -A "${UA}" \
    -H "Accept: application/octet-stream" \
    --connect-timeout 10 \
    --max-time 300 \
    --max-filesize "${max_bytes}" \
    --retry 2 \
    --retry-delay 1 \
    -o "${out}" \
    -w "%{http_code}" \
    "${url}" 2>/dev/null) || true
  if [[ -z "${code}" || "${code}" == "000" ]]; then
    echo "000"
  else
    echo "${code}"
  fi
}

http_get_json() {
  # http_get_json URL OUT_PATH → http code
  local url="$1" out="$2"
  local code
  code=$(curl -sS -L \
    --proto '=https' \
    --proto-redir '=https' \
    -A "${UA}" \
    -H "Accept: application/json" \
    --connect-timeout 10 \
    --max-time 60 \
    --max-filesize "$((2 * 1024 * 1024))" \
    --retry 2 \
    --retry-delay 1 \
    -o "${out}" \
    -w "%{http_code}" \
    "${url}" 2>/dev/null) || true
  if [[ -z "${code}" || "${code}" == "000" ]]; then
    echo "000"
  else
    echo "${code}"
  fi
}

# ── pin parse ────────────────────────────────────────────────────────────────
# Prints key=value lines for shell eval via python (safe scalar subset).
read_pin_scalars() {
  python3 - "${ANCHOR_TOML}" <<'PY'
import re, sys
from pathlib import Path
text = Path(sys.argv[1]).read_text()
keys = (
    "slot", "epoch", "block_root", "state_root",
    "genesis_validators_root", "genesis_time",
    "provider", "retrieval_date",
    "block_size", "state_size", "block_sha256", "state_sha256",
    "max_blob_commitment_count", "sequence_len",
)
for k in keys:
    m = re.search(rf'^{re.escape(k)}\s*=\s*(.+)$', text, re.M)
    if not m:
        raise SystemExit(f"missing {k} in pin")
    v = m.group(1).strip().strip('"')
    print(f"{k}={v}")
PY
}

# ── readiness: presence + size + SHA-256 vs pin ─────────────────────────────
# Args: slot_dir  (anchor + sequence TOML must exist)
cache_matches_pin() {
  local slot_dir="$1"
  python3 - "${slot_dir}" "${ANCHOR_TOML}" "${SEQUENCE_TOML}" "${MIN_STATE_BYTES}" <<'PY'
import hashlib, sys
from pathlib import Path

slot_dir = Path(sys.argv[1])
anchor_path = Path(sys.argv[2])
seq_path = Path(sys.argv[3])
min_state = int(sys.argv[4])

def parse_flat(text: str) -> dict:
    out = {}
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("["):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip().strip('"')
    return out

def parse_sequence(text: str):
    slots = []
    cur = None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("[["):
            if cur is not None:
                slots.append(cur)
            cur = {}
            continue
        if cur is None or "=" not in s or s.startswith("#"):
            continue
        k, v = s.split("=", 1)
        cur[k.strip()] = v.strip().strip('"')
    if cur is not None:
        slots.append(cur)
    return slots

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

if not anchor_path.is_file() or not seq_path.is_file():
    sys.exit(1)

anchor = parse_flat(anchor_path.read_text())
block = slot_dir / "signed_beacon_block.ssz"
state = slot_dir / "beacon_state.ssz"
if not block.is_file() or not state.is_file():
    sys.exit(1)
if state.stat().st_size < min_state:
    sys.exit(1)

got = sha256_file(block)
if got != anchor["block_sha256"]:
    print(f"block sha mismatch: expected {anchor['block_sha256']}, got {got}", file=sys.stderr)
    sys.exit(1)
got = sha256_file(state)
if got != anchor["state_sha256"]:
    print(f"state sha mismatch: expected {anchor['state_sha256']}, got {got}", file=sys.stderr)
    sys.exit(1)

slots = parse_sequence(seq_path.read_text())
if len(slots) != 40:
    print(f"sequence length {len(slots)} != 40", file=sys.stderr)
    sys.exit(1)

seq_dir = slot_dir / "sequence"
for e in slots:
    slot = e["slot"]
    empty = e.get("empty", "false").lower() == "true"
    ssz = seq_dir / f"{slot}.ssz"
    empty_mark = seq_dir / f"{slot}.empty"
    if empty:
        if ssz.is_file() and ssz.stat().st_size > 0:
            print(f"empty slot {slot} has non-empty SSZ", file=sys.stderr)
            sys.exit(1)
        # empty marker preferred but not required if SSZ absent
        continue
    if not ssz.is_file() or ssz.stat().st_size < 1:
        print(f"missing sequence SSZ for slot {slot}", file=sys.stderr)
        sys.exit(1)
    expected = e.get("ssz_sha256", "")
    if not expected:
        print(f"non-empty slot {slot} missing ssz_sha256 in pin", file=sys.stderr)
        sys.exit(1)
    got = sha256_file(ssz)
    if got != expected:
        print(
            f"sequence/{slot}.ssz sha mismatch: expected {expected}, got {got}",
            file=sys.stderr,
        )
        sys.exit(1)

sys.exit(0)
PY
}

write_complete_marker() {
  local slot_dir="$1" slot="$2" block_sha="$3" state_sha="$4" provider="$5" date="$6"
  {
    echo "slot=${slot}"
    echo "block_sha256=${block_sha}"
    echo "state_sha256=${state_sha}"
    echo "provider=${provider}"
    echo "retrieval_date=${date}"
  } > "${slot_dir}/.complete"
}

# ── early no-op when pin present, cache matches digests, not --force ─────────
HAS_PIN=0
if [[ -f "${ANCHOR_TOML}" && -f "${SEQUENCE_TOML}" ]]; then
  HAS_PIN=1
fi

if [[ "${FORCE}" -eq 0 && "${HAS_PIN}" -eq 1 ]]; then
  # shellcheck disable=SC1090
  eval "$(read_pin_scalars | sed 's/^/PIN_/')"
  # PIN_slot etc. — rename for clarity
  PINNED_SLOT="${PIN_slot}"
  PINNED_DIR="${CACHE_ROOT}/${PINNED_SLOT}"
  if cache_matches_pin "${PINNED_DIR}"; then
    log "cache already complete and digests match pin at ${PINNED_DIR} (slot=${PINNED_SLOT}); no-op"
    exit 0
  fi
  log "pin present (slot=${PINNED_SLOT}); restoring cache without rewriting manifests"
  MODE="restore"
else
  if [[ "${FORCE}" -eq 1 ]]; then
    log "--force: re-pin from live finalized (will rewrite manifests)"
    MODE="repin"
  else
    log "no pin yet; bootstrapping from live finalized"
    MODE="bootstrap"
  fi
fi

# ── provider selection ───────────────────────────────────────────────────────
TMPDIR_FETCH="$(mktemp -d "${TMPDIR:-/tmp}/hoodi-fixtures.XXXXXX")"
trap 'rm -rf "${TMPDIR_FETCH}"' EXIT

select_provider_for() {
  # select_provider_for PATH_SUFFIX  (e.g. /eth/v1/beacon/headers/finalized)
  local suffix="$1"
  local p code
  for p in "${PROVIDERS[@]}"; do
    code=$(http_get_json "${p}${suffix}" "${TMPDIR_FETCH}/probe.json")
    if [[ "${code}" == "200" ]]; then
      if python3 -c "import json;d=json.load(open('${TMPDIR_FETCH}/probe.json'));assert 'data' in d" 2>/dev/null; then
        echo "${p}"
        return 0
      fi
    fi
    log "provider ${p}${suffix} => HTTP ${code} (skip)"
  done
  return 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# RESTORE MODE — fill pin only; never rewrite TOML
# ═══════════════════════════════════════════════════════════════════════════════
if [[ "${MODE}" == "restore" ]]; then
  ANCHOR_SLOT="${PIN_slot}"
  BLOCK_ROOT="${PIN_block_root}"
  STATE_ROOT="${PIN_state_root}"
  EXPECT_BLOCK_SHA="${PIN_block_sha256}"
  EXPECT_STATE_SHA="${PIN_state_sha256}"
  EXPECT_BLOCK_SIZE="${PIN_block_size}"
  EXPECT_STATE_SIZE="${PIN_state_size}"
  EPOCH="${PIN_epoch}"
  SLOT_DIR="${CACHE_ROOT}/${ANCHOR_SLOT}"
  SEQ_DIR="${SLOT_DIR}/sequence"
  mkdir -p "${SEQ_DIR}"

  log "restoring pin slot=${ANCHOR_SLOT} epoch=${EPOCH} block_root=${BLOCK_ROOT}"

  # Prefer a full beacon API that can serve historical by-root/by-slot.
  log "probing providers for pin restore…"
  PROVIDER="$(select_provider_for "/eth/v1/beacon/headers/${BLOCK_ROOT}")" \
    || PROVIDER="$(select_provider_for "/eth/v1/beacon/headers/finalized")" \
    || die "no Hoodi provider answered; tried: ${PROVIDERS[*]}"
  log "using provider ${PROVIDER}"

  # ── block by root (never finalized alias — pin is historical) ────────────
  BLOCK_SSZ="${SLOT_DIR}/signed_beacon_block.ssz"
  need_block=1
  if [[ -f "${BLOCK_SSZ}" ]]; then
    got=$(sha256_file "${BLOCK_SSZ}")
    if [[ "${got}" == "${EXPECT_BLOCK_SHA}" ]]; then
      need_block=0
      log "block already present and SHA matches"
    else
      log "block SHA mismatch (got ${got}); re-fetching"
      rm -f "${BLOCK_SSZ}"
    fi
  fi
  if [[ "${need_block}" -eq 1 ]]; then
    log "fetching SignedBeaconBlock SSZ by root…"
    code="000"
    for p in "${PROVIDER}" "${PROVIDERS[@]}"; do
      code=$(http_get_file "${p}/eth/v2/beacon/blocks/${BLOCK_ROOT}" "${TMPDIR_FETCH}/block.ssz" "${MAX_BLOCK_BYTES}")
      if [[ "${code}" == "200" ]]; then
        PROVIDER="${p}"
        break
      fi
    done
    [[ "${code}" == "200" ]] || die "block SSZ fetch by root failed (last HTTP ${code})"
    got=$(sha256_file "${TMPDIR_FETCH}/block.ssz")
    [[ "${got}" == "${EXPECT_BLOCK_SHA}" ]] || die "block SHA256 mismatch: expected ${EXPECT_BLOCK_SHA}, got ${got}"
    bsize=$(wc -c < "${TMPDIR_FETCH}/block.ssz" | tr -d ' ')
    [[ "${bsize}" == "${EXPECT_BLOCK_SIZE}" ]] || log "warn: block size ${bsize} != pin ${EXPECT_BLOCK_SIZE} (digest ok; continuing)"
    mv "${TMPDIR_FETCH}/block.ssz" "${BLOCK_SSZ}"
  fi

  # ── state by state_root only (no finalized fallback in restore) ──────────
  STATE_SSZ="${SLOT_DIR}/beacon_state.ssz"
  need_state=1
  if [[ -f "${STATE_SSZ}" ]]; then
    got=$(sha256_file "${STATE_SSZ}")
    if [[ "${got}" == "${EXPECT_STATE_SHA}" ]]; then
      need_state=0
      log "state already present and SHA matches"
    else
      log "state SHA mismatch (got ${got}); re-fetching"
      rm -f "${STATE_SSZ}"
    fi
  fi
  if [[ "${need_state}" -eq 1 ]]; then
    log "fetching BeaconState SSZ by state_root (may be ~150–200 MB)…"
    code="000"
    for p in "${PROVIDER}" "${PROVIDERS[@]}"; do
      code=$(http_get_file "${p}/eth/v2/debug/beacon/states/${STATE_ROOT}" "${TMPDIR_FETCH}/state.ssz" "${MAX_STATE_BYTES}")
      if [[ "${code}" == "200" ]]; then
        PROVIDER="${p}"
        break
      fi
      log "provider ${p} state-by-root => HTTP ${code}"
    done
    [[ "${code}" == "200" ]] || die "state SSZ fetch by root failed for all providers (pin restore cannot use finalized alias)"
    ssize=$(wc -c < "${TMPDIR_FETCH}/state.ssz" | tr -d ' ')
    [[ "${ssize}" -ge "${MIN_STATE_BYTES}" ]] || die "state SSZ size ${ssize} < ${MIN_STATE_BYTES}"
    got=$(sha256_file "${TMPDIR_FETCH}/state.ssz")
    [[ "${got}" == "${EXPECT_STATE_SHA}" ]] || die "state SHA256 mismatch: expected ${EXPECT_STATE_SHA}, got ${got}"
    mv "${TMPDIR_FETCH}/state.ssz" "${STATE_SSZ}"
  fi

  # ── sequence from committed hoodi-sequence.toml ──────────────────────────
  log "restoring 40-slot sequence from pin…"
  python3 - "${PROVIDER}" "${SEQ_DIR}" "${SEQUENCE_TOML}" "${UA}" \
    "${MAX_BLOCK_BYTES}" "$(IFS='|'; echo "${PROVIDERS[*]}")" <<'PY'
import hashlib, json, ssl, sys, time, urllib.error, urllib.request
from pathlib import Path

base, seq_dir_s, seq_toml, ua, max_block_s, providers_joined = sys.argv[1:7]
seq_dir = Path(seq_dir_s)
seq_dir.mkdir(parents=True, exist_ok=True)
max_block = int(max_block_s)
providers = [base] + [p for p in providers_joined.split("|") if p and p != base]
ctx = ssl.create_default_context()

def parse_sequence(text: str):
    slots = []
    cur = None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("[["):
            if cur is not None:
                slots.append(cur)
            cur = {}
            continue
        if cur is None or "=" not in s or s.startswith("#"):
            continue
        k, v = s.split("=", 1)
        cur[k.strip()] = v.strip().strip('"')
    if cur is not None:
        slots.append(cur)
    return slots

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def get_ssz(url: str):
    req = urllib.request.Request(
        url, headers={"Accept": "application/octet-stream", "User-Agent": ua}
    )
    try:
        with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
            # reject non-https final URL if any
            if r.geturl().startswith("http://"):
                return None
            data = r.read(max_block + 1)
    except Exception:
        return None
    if data is None or len(data) < 100 or len(data) > max_block:
        return None
    return data

slots = parse_sequence(Path(seq_toml).read_text())
if len(slots) != 40:
    raise SystemExit(f"pin sequence length {len(slots)} != 40")

for e in slots:
    slot = int(e["slot"])
    empty = e.get("empty", "false").lower() == "true"
    ssz_path = seq_dir / f"{slot}.ssz"
    empty_mark = seq_dir / f"{slot}.empty"
    if empty:
        if ssz_path.exists():
            ssz_path.unlink()
        empty_mark.write_text("empty\n")
        print(f"  slot {slot} EMPTY (marker)", file=sys.stderr, flush=True)
        continue
    expected = e.get("ssz_sha256", "")
    if not expected:
        raise SystemExit(f"non-empty slot {slot} missing ssz_sha256 in pin")
    root = e["root"]
    if ssz_path.is_file():
        got = sha256_bytes(ssz_path.read_bytes())
        if got == expected:
            print(f"  slot {slot} ok (cached)", file=sys.stderr, flush=True)
            if empty_mark.exists():
                empty_mark.unlink()
            continue
        ssz_path.unlink()
    data = None
    for p in providers:
        for url in (
            f"{p}/eth/v2/beacon/blocks/{root}",
            f"{p}/eth/v2/beacon/blocks/{slot}",
        ):
            data = get_ssz(url)
            if data is not None:
                break
        if data is not None:
            break
        time.sleep(0.05)
    if data is None:
        raise SystemExit(f"failed to fetch sequence SSZ for slot {slot} root {root}")
    got = sha256_bytes(data)
    if got != expected:
        raise SystemExit(
            f"sequence slot {slot} SHA256 mismatch: expected {expected}, got {got}"
        )
    ssz_path.write_bytes(data)
    if empty_mark.exists():
        empty_mark.unlink()
    print(f"  slot {slot} restored", file=sys.stderr, flush=True)
    time.sleep(0.05)

print("sequence restore ok", file=sys.stderr)
PY

  # Final verify (fail closed)
  cache_matches_pin "${SLOT_DIR}" || die "post-restore cache does not match pin digests"
  write_complete_marker "${SLOT_DIR}" "${ANCHOR_SLOT}" "${EXPECT_BLOCK_SHA}" \
    "${EXPECT_STATE_SHA}" "${PIN_provider}" "${PIN_retrieval_date}"

  log "ready at ${SLOT_DIR} (pin restored; manifests untouched)"
  log "CI cache key: hoodi-fixtures-${ANCHOR_SLOT}-${EXPECT_BLOCK_SHA}"
  log "done."
  exit 0
fi

# ═══════════════════════════════════════════════════════════════════════════════
# BOOTSTRAP / REPIN — live finalized → write manifests
# ═══════════════════════════════════════════════════════════════════════════════
log "probing providers…"
PROVIDER="$(select_provider_for "/eth/v1/beacon/headers/finalized")" \
  || die "no Hoodi provider answered headers/finalized; tried: ${PROVIDERS[*]}"
log "using provider ${PROVIDER}"
# keep hdr.json from probe if present
if [[ ! -f "${TMPDIR_FETCH}/hdr.json" ]]; then
  code=$(http_get_json "${PROVIDER}/eth/v1/beacon/headers/finalized" "${TMPDIR_FETCH}/hdr.json")
  [[ "${code}" == "200" ]] || die "finalized header HTTP ${code}"
else
  # probe wrote probe.json; re-fetch into hdr.json
  code=$(http_get_json "${PROVIDER}/eth/v1/beacon/headers/finalized" "${TMPDIR_FETCH}/hdr.json")
  [[ "${code}" == "200" ]] || die "finalized header HTTP ${code}"
fi

# ── genesis ──────────────────────────────────────────────────────────────────
code=$(http_get_json "${PROVIDER}/eth/v1/beacon/genesis" "${TMPDIR_FETCH}/genesis.json")
[[ "${code}" == "200" ]] || die "genesis fetch failed HTTP ${code} from ${PROVIDER}"
read -r GENESIS_TIME GENESIS_VALIDATORS_ROOT < <(python3 -c "
import json
d=json.load(open('${TMPDIR_FETCH}/genesis.json'))['data']
print(d['genesis_time'], d['genesis_validators_root'])
")

# ── finalized header → anchor slot / roots ───────────────────────────────────
python3 -c "
import json
d=json.load(open('${TMPDIR_FETCH}/hdr.json'))
data=d['data']
msg=data['header']['message']
print(msg['slot'])
print(data['root'])
print(msg['state_root'])
print(msg['parent_root'])
" > "${TMPDIR_FETCH}/anchor_meta.txt"
ANCHOR_SLOT=$(sed -n '1p' "${TMPDIR_FETCH}/anchor_meta.txt")
BLOCK_ROOT=$(sed -n '2p' "${TMPDIR_FETCH}/anchor_meta.txt")
STATE_ROOT=$(sed -n '3p' "${TMPDIR_FETCH}/anchor_meta.txt")
EPOCH=$((ANCHOR_SLOT / SLOTS_PER_EPOCH))

if [[ "${EPOCH}" -le 54016 ]]; then
  die "anchor epoch ${EPOCH} (slot ${ANCHOR_SLOT}) is not > 54016; refuse to pin (CC-10b / CC-12/5)"
fi

SLOT_DIR="${CACHE_ROOT}/${ANCHOR_SLOT}"
SEQ_DIR="${SLOT_DIR}/sequence"
mkdir -p "${SEQ_DIR}"

log "anchor slot=${ANCHOR_SLOT} epoch=${EPOCH} block_root=${BLOCK_ROOT}"

# ── fetch SSZ block (prefer by root for pin stability; finalized as fallback) ─
BLOCK_SSZ="${SLOT_DIR}/signed_beacon_block.ssz"
log "fetching SignedBeaconBlock SSZ…"
code=$(http_get_file "${PROVIDER}/eth/v2/beacon/blocks/${BLOCK_ROOT}" "${TMPDIR_FETCH}/block.ssz" "${MAX_BLOCK_BYTES}")
if [[ "${code}" != "200" ]]; then
  code=$(http_get_file "${PROVIDER}/eth/v2/beacon/blocks/finalized" "${TMPDIR_FETCH}/block.ssz" "${MAX_BLOCK_BYTES}")
fi
[[ "${code}" == "200" ]] || die "block SSZ fetch failed HTTP ${code}"
bsize=$(wc -c < "${TMPDIR_FETCH}/block.ssz" | tr -d ' ')
[[ "${bsize}" -gt 100 ]] || die "block SSZ too small (${bsize} bytes)"
mv "${TMPDIR_FETCH}/block.ssz" "${BLOCK_SSZ}"
BLOCK_SIZE=$(wc -c < "${BLOCK_SSZ}" | tr -d ' ')
BLOCK_SHA=$(sha256_file "${BLOCK_SSZ}")
log "block size=${BLOCK_SIZE} sha256=${BLOCK_SHA}"

# ── fetch SSZ state (by state_root, fallback finalized) ──────────────────────
STATE_SSZ="${SLOT_DIR}/beacon_state.ssz"
log "fetching BeaconState SSZ by state_root (may be ~150–200 MB)…"
code=$(http_get_file "${PROVIDER}/eth/v2/debug/beacon/states/${STATE_ROOT}" "${TMPDIR_FETCH}/state.ssz" "${MAX_STATE_BYTES}")
if [[ "${code}" != "200" ]]; then
  log "by-root state failed HTTP ${code}; falling back to finalized alias"
  code=$(http_get_file "${PROVIDER}/eth/v2/debug/beacon/states/finalized" "${TMPDIR_FETCH}/state.ssz" "${MAX_STATE_BYTES}")
fi
[[ "${code}" == "200" ]] || die "state SSZ fetch failed HTTP ${code}"
ssize=$(wc -c < "${TMPDIR_FETCH}/state.ssz" | tr -d ' ')
if [[ "${ssize}" -lt "${MIN_STATE_BYTES}" ]]; then
  die "state SSZ size ${ssize} < ${MIN_STATE_BYTES} (150 MB floor); refusing to pin"
fi
if [[ "${ssize}" -gt "${MAX_STATE_BYTES}" ]]; then
  die "state SSZ size ${ssize} > ${MAX_STATE_BYTES} max"
fi
mv "${TMPDIR_FETCH}/state.ssz" "${STATE_SSZ}"
STATE_SIZE=$(wc -c < "${STATE_SSZ}" | tr -d ' ')
STATE_SHA=$(sha256_file "${STATE_SSZ}")
log "state size=${STATE_SIZE} sha256=${STATE_SHA}"

# ── 40-slot consecutive sequence ending at anchor ────────────────────────────
START_SLOT=$((ANCHOR_SLOT - SEQUENCE_LEN + 1))
log "fetching ${SEQUENCE_LEN}-slot sequence [${START_SLOT}..${ANCHOR_SLOT}]…"

SEQUENCE_PROVIDER="${PROVIDER}"
for p in "${PROVIDERS[@]}"; do
  code=$(http_get_json "${p}/eth/v1/beacon/headers/${START_SLOT}" "${TMPDIR_FETCH}/probe_hdr.json")
  if [[ "${code}" == "200" || "${code}" == "404" ]]; then
    SEQUENCE_PROVIDER="${p}"
    break
  fi
done
log "sequence provider ${SEQUENCE_PROVIDER}"

python3 - "${SEQUENCE_PROVIDER}" "${START_SLOT}" "${ANCHOR_SLOT}" "${SEQ_DIR}" "${UA}" \
  "${TMPDIR_FETCH}/sequence.json" "${MAX_BLOCK_BYTES}" <<'PY'
import json, ssl, sys, time, urllib.error, urllib.request
from pathlib import Path

base, start, end, seq_dir, ua, out_json, max_block_s = sys.argv[1:8]
start, end = int(start), int(end)
max_block = int(max_block_s)
seq_path = Path(seq_dir)
seq_path.mkdir(parents=True, exist_ok=True)
ctx = ssl.create_default_context()

def get_json(url: str):
    req = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": ua}
    )
    with urllib.request.urlopen(req, timeout=45, context=ctx) as r:
        if r.geturl().startswith("http://"):
            raise urllib.error.URLError("refusing http redirect")
        return json.load(r)

def get_ssz(url: str, dest: Path) -> bool:
    req = urllib.request.Request(
        url, headers={"Accept": "application/octet-stream", "User-Agent": ua}
    )
    try:
        with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
            if r.geturl().startswith("http://"):
                return False
            data = r.read(max_block + 1)
    except urllib.error.HTTPError:
        return False
    if data is None or len(data) < 100 or len(data) > max_block:
        return False
    dest.write_bytes(data)
    return True

slots = []
max_blobs = 0
for slot in range(start, end + 1):
    empty_mark = seq_path / f"{slot}.empty"
    ssz_path = seq_path / f"{slot}.ssz"
    try:
        d = get_json(f"{base}/eth/v1/beacon/headers/{slot}")
        root = d["data"]["root"]
        parent = d["data"]["header"]["message"]["parent_root"]
        try:
            bd = get_json(f"{base}/eth/v2/beacon/blocks/{slot}")
            blobs = len(bd["data"]["message"]["body"].get("blob_kzg_commitments", []))
        except Exception:
            blobs = 0
        max_blobs = max(max_blobs, blobs)
        if not ssz_path.is_file() or ssz_path.stat().st_size < 100:
            ok = get_ssz(f"{base}/eth/v2/beacon/blocks/{slot}", ssz_path)
            if not ok:
                ok = get_ssz(f"{base}/eth/v2/beacon/blocks/{root}", ssz_path)
            if not ok:
                print(f"warn: no SSZ for slot {slot}", file=sys.stderr)
        if empty_mark.exists():
            empty_mark.unlink()
        sha = ""
        size = 0
        if ssz_path.is_file():
            import hashlib
            raw = ssz_path.read_bytes()
            sha = hashlib.sha256(raw).hexdigest()
            size = len(raw)
        if not sha:
            raise SystemExit(f"non-empty slot {slot} missing SSZ (required for pin digests)")
        slots.append({
            "slot": slot,
            "root": root,
            "parent_root": parent,
            "blob_commitment_count": blobs,
            "empty": False,
            "ssz_sha256": sha,
            "ssz_size": size,
        })
        print(f"  slot {slot} blobs={blobs} root={root[:18]}…", file=sys.stderr, flush=True)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            if ssz_path.exists():
                ssz_path.unlink()
            empty_mark.write_text("empty\n")
            slots.append({
                "slot": slot,
                "root": "",
                "parent_root": "",
                "blob_commitment_count": 0,
                "empty": True,
                "ssz_sha256": "",
                "ssz_size": 0,
            })
            print(f"  slot {slot} EMPTY", file=sys.stderr, flush=True)
        else:
            time.sleep(1.0)
            try:
                d = get_json(f"{base}/eth/v1/beacon/headers/{slot}")
                root = d["data"]["root"]
                parent = d["data"]["header"]["message"]["parent_root"]
                bd = get_json(f"{base}/eth/v2/beacon/blocks/{slot}")
                blobs = len(bd["data"]["message"]["body"].get("blob_kzg_commitments", []))
                max_blobs = max(max_blobs, blobs)
                get_ssz(f"{base}/eth/v2/beacon/blocks/{slot}", ssz_path)
                import hashlib
                raw = ssz_path.read_bytes()
                sha = hashlib.sha256(raw).hexdigest()
                size = len(raw)
                slots.append({
                    "slot": slot,
                    "root": root,
                    "parent_root": parent,
                    "blob_commitment_count": blobs,
                    "empty": False,
                    "ssz_sha256": sha,
                    "ssz_size": size,
                })
                print(f"  slot {slot} (retry) blobs={blobs}", file=sys.stderr, flush=True)
            except urllib.error.HTTPError as e2:
                if e2.code == 404:
                    empty_mark.write_text("empty\n")
                    slots.append({
                        "slot": slot,
                        "root": "",
                        "parent_root": "",
                        "blob_commitment_count": 0,
                        "empty": True,
                        "ssz_sha256": "",
                        "ssz_size": 0,
                    })
                    print(f"  slot {slot} EMPTY", file=sys.stderr, flush=True)
                else:
                    raise SystemExit(f"sequence slot {slot} failed HTTP {e2.code}")
    time.sleep(0.08)

prev = None
for s in slots:
    if s["empty"]:
        continue
    if prev is not None and s["parent_root"] != prev:
        raise SystemExit(
            f"parent-link broken at slot {s['slot']}: "
            f"parent_root={s['parent_root']} != prev_root={prev}"
        )
    prev = s["root"]

if max_blobs <= 9:
    raise SystemExit(
        f"max blob_commitment_count across sequence is {max_blobs}, need > 9 "
        f"(CC-10b / CC-12/5); pick a later anchor"
    )

if len(slots) != 40:
    raise SystemExit(f"expected 40 slots, got {len(slots)}")

json.dump({"slots": slots, "max_blob_commitment_count": max_blobs}, open(out_json, "w"), indent=2)
PY
MAX_BLOBS=$(python3 -c "import json;print(json.load(open('${TMPDIR_FETCH}/sequence.json'))['max_blob_commitment_count'])")
log "max_blob_commitment_count=${MAX_BLOBS}"

# ── write committed manifests (bootstrap / --force only) ─────────────────────
RETRIEVAL_DATE=$(date -u +%Y-%m-%d)
PROVIDER_HOST="${PROVIDER#https://}"
PROVIDER_HOST="${PROVIDER_HOST#http://}"
PROVIDER_HOST="${PROVIDER_HOST%%/*}"

python3 - "${ANCHOR_TOML}" "${SEQUENCE_TOML}" \
  "${ANCHOR_SLOT}" "${EPOCH}" "${BLOCK_ROOT}" "${STATE_ROOT}" \
  "${GENESIS_VALIDATORS_ROOT}" "${GENESIS_TIME}" \
  "${PROVIDER_HOST}" "${RETRIEVAL_DATE}" \
  "${BLOCK_SIZE}" "${STATE_SIZE}" "${BLOCK_SHA}" "${STATE_SHA}" \
  "${MAX_BLOBS}" "${TMPDIR_FETCH}/sequence.json" <<'PY'
import json, sys
from pathlib import Path

(
    anchor_path, seq_path,
    slot, epoch, block_root, state_root,
    gvr, genesis_time,
    provider, date,
    block_size, state_size, block_sha, state_sha,
    max_blobs, seq_json,
) = sys.argv[1:]

slot = int(slot); epoch = int(epoch)
block_size = int(block_size); state_size = int(state_size)
max_blobs = int(max_blobs)
genesis_time = int(genesis_time)
seq = json.load(open(seq_json))

anchor = f"""# Hoodi fixture anchor — expected roots only (CC-10b).
# SSZ bytes live in ${{HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}}/{slot}/
# Restored by: bash scripts/fetch-hoodi-fixtures.sh  (pin-aware; no rewrite)
# Re-pin with: bash scripts/fetch-hoodi-fixtures.sh --force

slot = {slot}
epoch = {epoch}
block_root = "{block_root}"
state_root = "{state_root}"
genesis_validators_root = "{gvr}"
genesis_time = {genesis_time}
provider = "{provider}"
retrieval_date = "{date}"
block_size = {block_size}
state_size = {state_size}
block_sha256 = "{block_sha}"
state_sha256 = "{state_sha}"
max_blob_commitment_count = {max_blobs}
sequence_len = 40
"""
Path(anchor_path).write_text(anchor)

lines = [
    "# Hoodi 40-slot consecutive sequence ending at the anchor (CC-10b).",
    "# Parent-linked in slot order; empty slots explicitly marked.",
    "# No SSZ payload — roots and counts only.",
    "# Restored by: bash scripts/fetch-hoodi-fixtures.sh  (pin-aware; no rewrite)",
    "# Re-pin with: bash scripts/fetch-hoodi-fixtures.sh --force",
    "",
    f"anchor_slot = {slot}",
    f"start_slot = {slot - 39}",
    f"max_blob_commitment_count = {max_blobs}",
    "",
]
for e in seq["slots"]:
    lines.append("[[slots]]")
    lines.append(f"slot = {e['slot']}")
    if e["empty"]:
        lines.append('root = ""')
        lines.append('parent_root = ""')
        lines.append("blob_commitment_count = 0")
        lines.append("empty = true")
    else:
        lines.append(f'root = "{e["root"]}"')
        lines.append(f'parent_root = "{e["parent_root"]}"')
        lines.append(f"blob_commitment_count = {e['blob_commitment_count']}")
        lines.append("empty = false")
        if not e.get("ssz_sha256"):
            raise SystemExit(f"refusing to pin non-empty slot {e['slot']} without ssz_sha256")
        lines.append(f'ssz_sha256 = "{e["ssz_sha256"]}"')
        lines.append(f"ssz_size = {e.get('ssz_size', 0)}")
    lines.append("")

Path(seq_path).write_text("\n".join(lines))
print(f"wrote {anchor_path}")
print(f"wrote {seq_path}")
PY

write_complete_marker "${SLOT_DIR}" "${ANCHOR_SLOT}" "${BLOCK_SHA}" \
  "${STATE_SHA}" "${PROVIDER_HOST}" "${RETRIEVAL_DATE}"

# Self-check: newly written pin must match cache digests.
cache_matches_pin "${SLOT_DIR}" || die "post-pin cache does not match freshly written digests"

log "ready at ${SLOT_DIR}"
log "manifests written: ${ANCHOR_TOML} , ${SEQUENCE_TOML}"
log "CI cache key: hoodi-fixtures-${ANCHOR_SLOT}-${BLOCK_SHA}"
if [[ "${MODE}" == "repin" ]]; then
  log "NOTE: --force re-pinned; update the actions/cache key in crates/types/tests/fixtures/README.md"
fi
log "done."
exit 0
