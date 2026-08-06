#!/usr/bin/env bash
# scripts/record-vector-layout.sh — CC-06b vector-layout manifest recorder
#
# Reads the on-disk layout of the pinned consensus-specs vector cache (never
# assumed) and writes repo-root `spec-vectors-layout.md`. Regenerating must
# produce no diff against the committed copy:
#
#   bash scripts/record-vector-layout.sh && git diff --exit-code spec-vectors-layout.md
#
# Scope (Architecture §7.5 / CC-06/5):
#   find <cache>/<tag>/tests/mainnet/fulu -maxdepth 3 -type d
#   find <cache>/<tag>/tests/general      -maxdepth 3 -type d
#
# Paths are normalised to tree-relative form (tests/…) and sorted for
# determinism. Layout findings (operations vs block_processing; ssz_generic /
# KZG locations) are derived from the download. The OQ-1 Hoodi line is a
# recorded one-shot comparison (live endpoints are not re-queried on regen).
#
# This script is never invoked by cargo build or cargo nextest.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCKFILE="${REPO_ROOT}/spec-vectors.lock"
OUT="${REPO_ROOT}/spec-vectors-layout.md"

if [[ ! -f "${LOCKFILE}" ]]; then
  echo "error: lockfile not found: ${LOCKFILE}" >&2
  exit 1
fi

# ── lockfile tag (same awk style as fetch-spec-vectors.sh) ───────────────────
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

TAG="$(read_lock_scalar tag)"
if [[ -z "${TAG}" ]]; then
  echo "error: failed to parse tag from ${LOCKFILE}" >&2
  exit 1
fi

# Tag allowlist (F1): start with alnum; only [A-Za-z0-9._-] thereafter; reject
# path separators (already), lone "." / "..", and any ".." substring.
if [[ ! "${TAG}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "error: tag contains disallowed characters (want ^[A-Za-z0-9][A-Za-z0-9._-]*\$): ${TAG}" >&2
  exit 1
fi
if [[ "${TAG}" == *..* ]]; then
  echo "error: tag must not contain '..': ${TAG}" >&2
  exit 1
fi

CACHE_ROOT="${SPEC_VECTORS_CACHE:-${HOME}/.cache/eth-consensus-spec-vectors}"
TAG_DIR="${CACHE_ROOT}/${TAG}"
TREE_ROOT="${TAG_DIR}/tests"
FULU_ROOT="${TREE_ROOT}/mainnet/fulu"
GENERAL_ROOT="${TREE_ROOT}/general"

if [[ ! -d "${FULU_ROOT}" || ! -d "${GENERAL_ROOT}" ]]; then
  echo "error: vector tree not ready under ${TAG_DIR}" >&2
  echo "  missing one of: tests/mainnet/fulu, tests/general" >&2
  echo "  run scripts/fetch-spec-vectors.sh" >&2
  exit 1
fi

# Guard find operands that look like options (F5). Absolute paths are fine;
# a relative SPEC_VECTORS_CACHE of "-evil" would otherwise be parsed as flags.
# Prefer `./` prefix (portable) over `find --` (GNU) / `find -f` (BSD).
find_safe() {
  local root="$1"
  shift
  case "${root}" in
    -*) root="./${root}" ;;
  esac
  find "${root}" "$@"
}
# Literal prefix strip of TAG_DIR/ via bash parameter expansion (F2).
# Never sed-interpolates cache paths (avoids BRE / sed-script injection).
# Refuses to emit paths that do not sit under TAG_DIR.
to_tree_rel() {
  local abs="$1"
  local prefix="${TAG_DIR}/"
  if [[ "${abs}" == "${prefix}"* ]]; then
    printf '%s\n' "${abs#"${prefix}"}"
  elif [[ "${abs}" == "${TAG_DIR}" ]]; then
    # Exact tag dir — no tree-relative suffix to emit.
    return 0
  else
    echo "error: path escapes tag dir (refusing to embed): ${abs}" >&2
    return 1
  fi
}

# Read find output lines and print sorted tree-relative paths.
relpaths_from_find() {
  local line rel
  local -a paths=()
  while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    rel="$(to_tree_rel "${line}")" || return 1
    [[ -z "${rel}" ]] && continue
    paths+=("${rel}")
  done
  if ((${#paths[@]} > 0)); then
    printf '%s\n' "${paths[@]}" | LC_ALL=C sort
  fi
}

# ── collect depth-≤3 directory listings (tree-relative, sorted) ──────────────
list_dirs() {
  local abs_root="$1"
  find_safe "${abs_root}" -maxdepth 3 -type d | relpaths_from_find
}

FULU_LIST="$(list_dirs "${FULU_ROOT}")"
GENERAL_LIST="$(list_dirs "${GENERAL_ROOT}")"

# ── derive layout findings from the download (A-P0-3 / OQ-2) ─────────────────
OPS_DIR="${FULU_ROOT}/operations"
BP_DIR="${FULU_ROOT}/block_processing"
if [[ -d "${OPS_DIR}" ]]; then
  OPS_CHILDREN="$(find_safe "${OPS_DIR}" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | LC_ALL=C sort | paste -sd, - | sed 's/,/, /g')"
  OPS_FINDING="\`operations\` **exists as a directory** at \`tests/mainnet/fulu/operations/\` (children: ${OPS_CHILDREN}). There is **no** \`block_processing/\` directory under Fulu. The research note's Gloas-era inference that operations are emitted only from \`block_processing/\` does **not** hold for this Fulu tree (A-P0-3 corrected by the download)."
elif [[ -d "${BP_DIR}" ]]; then
  OPS_FINDING="\`operations\` does **not** exist as a top-level Fulu directory; a \`block_processing/\` tree is present at \`tests/mainnet/fulu/block_processing/\`. Runners must walk that tree rather than globbing \`operations/**\` (A-P0-3)."
else
  OPS_FINDING="Neither \`operations/\` nor \`block_processing/\` exists under \`tests/mainnet/fulu/\` for this pin — unexpected; re-check the download."
fi

SSZ_PATHS="$(find_safe "${GENERAL_ROOT}" -type d -name 'ssz_generic' | relpaths_from_find || true)"
if [[ -n "${SSZ_PATHS}" ]]; then
  SSZ_FINDING="\`ssz_generic\` lives under general at: $(printf '%s\n' "${SSZ_PATHS}" | paste -sd, - | sed 's/,/, /g'). Primary path: \`tests/general/phase0/ssz_generic/\`."
else
  SSZ_FINDING="\`ssz_generic\` was **not** found under \`tests/general/**\` for this pin."
fi

# KZG cell / proof suites historically shipped as tests/general/<fork>/kzg.
# Search the whole tests/ tree so we state where they actually are (or aren't).
KZG_DIRS="$(find_safe "${TREE_ROOT}" -type d -name 'kzg' | relpaths_from_find || true)"
KZG_CELL_HINTS="$(find_safe "${TREE_ROOT}" -type d \( -name '*compute_cells*' -o -name '*verify_cell*' -o -name '*recover_cells*' \) | relpaths_from_find | head -20 || true)"
if [[ -n "${KZG_DIRS}" ]]; then
  KZG_FINDING="A \`kzg/\` suite directory is present at: $(printf '%s\n' "${KZG_DIRS}" | paste -sd, - | sed 's/,/, /g')."
elif [[ -n "${KZG_CELL_HINTS}" ]]; then
  KZG_FINDING="No top-level \`kzg/\` directory; cell-related case dirs found at: $(printf '%s\n' "${KZG_CELL_HINTS}" | paste -sd, - | sed 's/,/, /g')."
else
  KZG_FINDING="**No KZG cell-vector suite** (\`kzg/\`, \`compute_cells*\`, \`verify_cell*\`, \`recover_cells*\`) exists under \`tests/**\` for pin \`${TAG}\`. \`general.tar.gz\` contains only \`tests/general/phase0/ssz_generic/**\` and \`tests/general/altair/bls/**\`. PeerDAS/cell material that *is* present lives under **mainnet/minimal Fulu** as networking and ssz_static cases (e.g. \`tests/mainnet/fulu/networking/gossip_data_column_sidecar/\`, \`…/ssz_static/DataColumnSidecar/\`), not as a general-preset KZG proof suite. **CC-10 / CC-11 must not assume a \`tests/general/**/kzg\` walker for this pin** (OQ-2 half closed by observation)."
fi

# Fulu top-level suites (depth 1 under fulu) for a short index.
FULU_SUITES="$(find_safe "${FULU_ROOT}" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | LC_ALL=C sort | paste -sd, - | sed 's/,/, /g')"
GENERAL_TOP="$(find_safe "${GENERAL_ROOT}" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | LC_ALL=C sort | paste -sd, - | sed 's/,/, /g')"

# ── OQ-1: recorded Hoodi cross-check (one-shot; not re-queried on regen) ─────
# Source: GET https://beacon.hoodi.ethpandaops.io/eth/v1/config/spec (2026-08-06)
# Cross-checked against ethereum/consensus-specs @ v1.7.0-alpha.13
#   configs/mainnet.yaml + presets/mainnet/fulu.yaml
# Protocol/preset Fulu constants (PRESET_BASE=mainnet: columns, cells, custody,
# samples, max-blobs baseline) match Hoodi. Network-identity fields differ by
# design (CONFIG_NAME, fork versions 0xN0000910, FULU_FORK_EPOCH 50688, BLOB_SCHEDULE
# epochs). Standing rule: deployed Hoodi behaviour wins on disagreement.
OQ1_FINDING="OQ-1: pin \`${TAG}\` \`presets/mainnet/fulu\` + mainnet config PeerDAS constants **match** deployed Hoodi protocol values (\`PRESET_BASE=mainnet\`, \`NUMBER_OF_COLUMNS=128\`, \`FIELD_ELEMENTS_PER_CELL=64\`, custody/sample knobs); network-identity fields differ as expected (\`CONFIG_NAME=hoodi\`, \`FULU_FORK_VERSION=0x70000910\`, \`FULU_FORK_EPOCH=50688\`). **Deployed behaviour wins** if they ever disagree. Source: \`https://beacon.hoodi.ethpandaops.io/eth/v1/config/spec\` vs \`ethereum/consensus-specs@${TAG}\` \`configs/mainnet.yaml\` + \`presets/mainnet/fulu.yaml\`."

# ── write manifest (fully deterministic from cache + embedded OQ-1 line) ─────
{
  cat <<EOF
# Spec-vector layout manifest

Recorded by \`scripts/record-vector-layout.sh\` from the on-disk cache for pin
**\`${TAG}\`**. Layout is **read off the download, never assumed** (CC-06/5,
Architecture §7.5). Regenerate with:

\`\`\`bash
bash scripts/record-vector-layout.sh && git diff --exit-code spec-vectors-layout.md
\`\`\`

| Field | Value |
|---|---|
| **Pinned tag** | \`${TAG}\` (from \`spec-vectors.lock\`) |
| **Cache root** | \`\${SPEC_VECTORS_CACHE:-\$HOME/.cache/eth-consensus-spec-vectors}\` |
| **Tree root** | \`<cache root>/${TAG}/tests\` |
| **Trees recorded** | \`tests/mainnet/fulu/**\`, \`tests/general/**\` (first three path levels) |

---

## §7.4 consumption contract

Phase 0 ships this contract; Phase 1 implements it in \`crates/spec-tests\`.

| Element | Value |
|---|---|
| Env var | \`SPEC_VECTORS_CACHE\` (optional) |
| Cache root | \`\${SPEC_VECTORS_CACHE:-\$HOME/.cache/eth-consensus-spec-vectors}\` |
| Tree root | \`<cache root>/<tag>/tests\` where \`<tag>\` is read from \`spec-vectors.lock\` |
| Readiness | all four \`<cache root>/<tag>/.complete-<artifact>\` markers exist and their contents equal the lockfile digests, and \`tests/\` exists |
| Failure mode | panic/error with the literal string \`run scripts/fetch-spec-vectors.sh\` — **never** an implicit download (CC-06/4) |
| Tag source | \`include_str!("…/spec-vectors.lock")\` at compile time, so a pin bump forces a rebuild |

Layout under \`<cache root>/<tag>/\`:

\`\`\`text
_dl/{general,mainnet,minimal,comptests}.tar.gz   # retained for re-verification
.complete-<artifact>                             # marker; contains verified sha256
tests/                                           # merged unpack root of all four
\`\`\`

---

## Layout findings (OQ-2 / A-P0-3)

### \`operations\` vs \`block_processing\` (A-P0-3)

${OPS_FINDING}

### \`ssz_generic\` and KZG cell vectors (OQ-2 — CC-10 / CC-11)

- ${SSZ_FINDING}
- ${KZG_FINDING}

\`general.tar.gz\` remains load-bearing for \`ssz_generic\` (and altair \`bls\`) even when a dedicated KZG suite is absent from this pin.

### Fulu suite index (top-level under \`tests/mainnet/fulu/\`)

${FULU_SUITES}

### General top-level (under \`tests/general/\`)

${GENERAL_TOP}

---

## OQ-1 — Fulu specs vs deployed Hoodi

${OQ1_FINDING}

---

## \`tests/mainnet/fulu\` — directories, maxdepth 3

Paths are tree-relative (prefix \`tests/…\`), sorted with \`LC_ALL=C\`.

\`\`\`text
EOF
  printf '%s\n' "${FULU_LIST}"
  cat <<EOF
\`\`\`

---

## \`tests/general\` — directories, maxdepth 3

\`\`\`text
EOF
  printf '%s\n' "${GENERAL_LIST}"
  cat <<EOF
\`\`\`

---

## Regenerating

\`\`\`bash
# Requires a ready cache (CC-06a):
bash scripts/fetch-spec-vectors.sh
bash scripts/record-vector-layout.sh
git diff --exit-code spec-vectors-layout.md
\`\`\`

This file is owned by CC-06b. Do not hand-edit the directory listings; change
the recorder script and re-run.
EOF
} > "${OUT}"

echo "record-vector-layout: wrote ${OUT} (tag=${TAG})"
exit 0
