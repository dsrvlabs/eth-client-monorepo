#!/usr/bin/env bash
# scripts/fetch-spec-vectors.sh — CC-06a spec-vector fetch harness
#
# Downloads the four consensus-specs release artifacts pinned in
# spec-vectors.lock, verifies each SHA-256 against the lockfile, and unpacks
# them into a cache outside the build tree.
#
# ─────────────────────────────────────────────────────────────────────────────
# §7.4 consumption contract (Phase 0 ships the contract; Phase 1 implements it
# in crates/spec-tests):
#
#   Env var        SPEC_VECTORS_CACHE (optional)
#   Cache root     ${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}
#   Tree root      <cache root>/<tag>/tests
#                  where <tag> is read from spec-vectors.lock
#   Readiness      all four <cache root>/<tag>/.complete-<artifact> markers exist
#                  and their contents equal the lockfile digests, and tests/ exists
#   Failure mode   panic/error with the literal string:
#                    run scripts/fetch-spec-vectors.sh
#                  — never an implicit download (CC-06/4)
#   Tag source     include_str!("…/spec-vectors.lock") at compile time, so a pin
#                  bump forces a rebuild
#
# Layout under <cache root>/<tag>/:
#   _dl/{general,mainnet,minimal,comptests}.tar.gz   retained for re-verification
#   .complete-<artifact>                             marker; contains verified sha256
#   tests/                                           merged unpack root of all four
#
# Flags:
#   --force    re-download every artifact
#   --verify   force a full re-hash (default path already re-hashes all four)
#
# This script is never invoked by cargo build or cargo nextest.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

FORCE=0
VERIFY=0
for arg in "$@"; do
  case "$arg" in
    --force)  FORCE=1 ;;
    --verify) VERIFY=1 ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $arg" >&2
      echo "usage: $0 [--force] [--verify]" >&2
      exit 2
      ;;
  esac
done
# --verify is accepted for the contract; default already re-hashes (ADR-11).
: "${VERIFY}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCKFILE="${REPO_ROOT}/spec-vectors.lock"

if [[ ! -f "${LOCKFILE}" ]]; then
  echo "error: lockfile not found: ${LOCKFILE}" >&2
  exit 1
fi

# ── preflight: required external tools ──────────────────────────────────────
for tool in curl tar; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "error: required tool not found: ${tool}" >&2
    exit 1
  fi
done

# ── lockfile fields (awk; no external TOML dependency) ──────────────────────
read_lock_scalar() {
  local key="$1"
  awk -F'=' -v k="$key" '
    $0 ~ "^[[:space:]]*" k "[[:space:]]*=" {
      v = $0
      sub(/^[^=]*=[[:space:]]*/, "", v)
      gsub(/^[[:space:]]*"?|"?[[:space:]]*$/, "", v)
      print v
      exit
    }
  ' "${LOCKFILE}"
}

read_lock_sha256() {
  local artifact="$1"
  awk -F'=' -v art="$artifact" '
    $0 ~ "\\[sha256\\]" { in_sha = 1; next }
    in_sha && $0 ~ /^\[/ { exit }
    in_sha {
      # match "artifact" = "digest" or artifact = "digest"
      line = $0
      if (match(line, /"[^"]+"/)) {
        key = substr(line, RSTART + 1, RLENGTH - 2)
      } else {
        next
      }
      if (key != art) next
      sub(/^[^=]*=[[:space:]]*/, "", line)
      gsub(/^[[:space:]]*"?|"?[[:space:]]*$/, "", line)
      print line
      exit
    }
  ' "${LOCKFILE}"
}

REPO="$(read_lock_scalar repo)"
TAG="$(read_lock_scalar tag)"
BASE_URL="$(read_lock_scalar base_url)"

if [[ -z "${REPO}" || -z "${TAG}" || -z "${BASE_URL}" ]]; then
  echo "error: failed to parse repo/tag/base_url from ${LOCKFILE}" >&2
  exit 1
fi

# repo is lockfile documentation / future Rust readers; shell builds URL from base_url.
: "${REPO}"

# ── fail-closed URL / tag sanitisation ──────────────────────────────────────
# Allow only ethereum/* GitHub release-asset download URLs (HTTPS, no other hosts).
# Exact pin preferred; broader form still restricted to github.com/ethereum/.../releases/download.
BASE_URL="${BASE_URL%/}"
if [[ "${BASE_URL}" == "https://github.com/ethereum/consensus-specs/releases/download" ]]; then
  :
elif [[ "${BASE_URL}" =~ ^https://github\.com/ethereum/[A-Za-z0-9._-]+/releases/download$ ]]; then
  :
else
  echo "error: base_url must be https://github.com/ethereum/<repo>/releases/download (got: ${BASE_URL})" >&2
  exit 1
fi

# Tag: version-like tokens only — no path separators, no spaces, no shell metachars.
if [[ ! "${TAG}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "error: tag contains disallowed characters (allow [A-Za-z0-9._-]): ${TAG}" >&2
  exit 1
fi

ARTIFACTS=(general.tar.gz mainnet.tar.gz minimal.tar.gz comptests.tar.gz)

CACHE_ROOT="${SPEC_VECTORS_CACHE:-${HOME}/.cache/eth-consensus-spec-vectors}"
TAG_DIR="${CACHE_ROOT}/${TAG}"
DL_DIR="${TAG_DIR}/_dl"

# curl: HTTPS-only, bounded time/size. Artifacts total ~1.74 GB; per-file max ~1 GB
# with headroom at 2 GB.
CURL_OPTS=(
  -fL
  --proto '=https'
  --proto-redir '=https'
  --connect-timeout 30
  --max-time 3600
  --max-filesize 2147483648
  --retry 3
  --retry-delay 2
)

# ── hasher resolution (once): sha256sum → openssl → shasum ──────────────────
HASH_CMD=()
HASHER_NAME=""
if command -v sha256sum >/dev/null 2>&1; then
  HASH_CMD=(sha256sum)
  HASHER_NAME="sha256sum"
elif command -v openssl >/dev/null 2>&1; then
  HASH_CMD=(openssl dgst -sha256)
  HASHER_NAME="openssl"
elif command -v shasum >/dev/null 2>&1; then
  HASH_CMD=(shasum -a 256)
  HASHER_NAME="shasum"
  echo "warning: using shasum -a 256 (~570 MB/s); CC-06/2 no-op budget (<2s for 1.74 GB) may not hold" >&2
else
  echo "error: no SHA-256 hasher found (need sha256sum, openssl, or shasum)" >&2
  exit 1
fi

hash_file() {
  local path="$1"
  local out
  out="$("${HASH_CMD[@]}" "${path}")"
  # sha256sum / shasum: "<hex>  <path>"; openssl: "SHA256(path)= <hex>" or "SHA2-256(path)= <hex>"
  if [[ "${HASHER_NAME}" == "openssl" ]]; then
    echo "${out##*= }" | tr -d '[:space:]'
  else
    echo "${out%% *}" | tr -d '[:space:]'
  fi
}

# Reject archive members that are absolute or contain path traversal.
# Lists the tarball first; extract only if every path is safe.
assert_safe_tarball() {
  local archive="$1"
  local member
  # tar -t lines are member paths; empty archives are invalid for our use.
  local any=0
  while IFS= read -r member; do
    any=1
    # Strip trailing slashes on directory entries for the checks below.
    local p="${member%/}"
    [[ -z "${p}" ]] && continue
    case "${p}" in
      /*|~*)
        echo "error: tarball ${archive} contains absolute path: ${member}" >&2
        return 1
        ;;
    esac
    # Reject any `..` path component (leading, middle, or alone).
    case "/${p}/" in
      */../*)
        echo "error: tarball ${archive} contains path traversal: ${member}" >&2
        return 1
        ;;
    esac
  done < <(tar -tzf "${archive}")
  if [[ "${any}" -eq 0 ]]; then
    echo "error: tarball is empty: ${archive}" >&2
    return 1
  fi
  return 0
}

safe_extract() {
  local archive="$1"
  local dest="$2"
  assert_safe_tarball "${archive}"
  tar -xzf "${archive}" -C "${dest}"
}

mkdir -p "${DL_DIR}"

echo "fetch-spec-vectors: tag=${TAG} cache=${TAG_DIR} hasher=${HASHER_NAME}"

FAILED=0
declare -a ACTUAL_DIGESTS=()

for art in "${ARTIFACTS[@]}"; do
  expected="$(read_lock_sha256 "${art}")"
  if [[ -z "${expected}" ]]; then
    echo "error: no [sha256] entry for ${art} in ${LOCKFILE}" >&2
    exit 1
  fi

  dest="${DL_DIR}/${art}"
  url="${BASE_URL}/${TAG}/${art}"

  if [[ "${FORCE}" -eq 1 ]] || [[ ! -f "${dest}" ]]; then
    echo "  downloading ${art} …"
    tmp="${dest}.partial"
    rm -f "${tmp}"
    curl "${CURL_OPTS[@]}" -o "${tmp}" "${url}"
    mv -f "${tmp}" "${dest}"
  else
    echo "  ${art}: using cached _dl/"
  fi

  echo "  hashing ${art} …"
  actual="$(hash_file "${dest}")"
  ACTUAL_DIGESTS+=("${actual}")

  if [[ "${expected}" == "PLACEHOLDER" || "${expected}" == "<computed at first fetch>" ]]; then
    echo "  ${art}: lockfile digest is a placeholder; computed sha256=${actual}"
    echo "error: expected and actual digests differ for ${art}" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    rm -f "${dest}"
    echo "  removed bad cache file: ${dest}" >&2
    echo "  hint: update the lockfile digests, then re-run scripts/fetch-spec-vectors.sh" >&2
    FAILED=1
    continue
  fi

  if [[ "${actual}" != "${expected}" ]]; then
    echo "error: expected and actual digests differ for ${art}" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    rm -f "${dest}"
    echo "  removed bad cache file: ${dest}" >&2
    echo "  hint: re-run scripts/fetch-spec-vectors.sh (or --force) to re-download" >&2
    FAILED=1
    continue
  fi
  echo "  ${art}: sha256 ok (${actual})"
done

if [[ "${FAILED}" -ne 0 ]]; then
  echo "error: one or more artifacts failed digest verification" >&2
  # When placeholders were present, print a ready-to-paste [sha256] block.
  if [[ "$(read_lock_sha256 general.tar.gz)" == "PLACEHOLDER" ]] \
     || [[ "$(read_lock_sha256 general.tar.gz)" == "<computed at first fetch>" ]]; then
    echo "" >&2
    echo "Computed digests for first-fetch lockfile update:" >&2
    i=0
    for art in "${ARTIFACTS[@]}"; do
      echo "  \"${art}\"   = \"${ACTUAL_DIGESTS[$i]}\"" >&2
      i=$((i + 1))
    done
  fi
  exit 1
fi

# ── unpack + markers (only when marker missing, digest mismatch, or tree gone) ─
TREE_MISSING=0
if [[ ! -d "${TAG_DIR}/tests" ]]; then
  TREE_MISSING=1
  echo "  tests/ tree missing; will re-unpack all artifacts"
fi

for art in "${ARTIFACTS[@]}"; do
  expected="$(read_lock_sha256 "${art}")"
  dest="${DL_DIR}/${art}"
  marker="${TAG_DIR}/.complete-${art}"

  need_unpack=0
  if [[ "${TREE_MISSING}" -eq 1 ]]; then
    need_unpack=1
  elif [[ ! -f "${marker}" ]]; then
    need_unpack=1
  else
    marker_digest="$(tr -d '[:space:]' < "${marker}")"
    if [[ "${marker_digest}" != "${expected}" ]]; then
      need_unpack=1
    fi
  fi

  if [[ "${FORCE}" -eq 1 ]]; then
    need_unpack=1
  fi

  if [[ "${need_unpack}" -eq 1 ]]; then
    echo "  unpacking ${art} → ${TAG_DIR}/"
    safe_extract "${dest}" "${TAG_DIR}"
    printf '%s\n' "${expected}" > "${marker}"
  else
    echo "  ${art}: marker present, skip unpack"
  fi
done

# Final readiness: markers + required tree roots.
if [[ ! -d "${TAG_DIR}/tests" ]]; then
  echo "error: markers written but ${TAG_DIR}/tests is missing; re-run with --force" >&2
  exit 1
fi
for required in tests/mainnet/fulu tests/general tests/minimal; do
  if [[ ! -d "${TAG_DIR}/${required}" ]]; then
    echo "error: required tree missing: ${TAG_DIR}/${required}" >&2
    echo "  hint: re-run scripts/fetch-spec-vectors.sh --force" >&2
    exit 1
  fi
done

echo "fetch-spec-vectors: ready at ${TAG_DIR}/tests"
exit 0
