#!/usr/bin/env bash
# Enforce Architecture §2.2 crate dependency DAG + workspace lint/rust-version opt-in (R-6).
# Exit non-zero and name the offending crate/edge on failure.
# Portable: no bash-4 associative arrays (macOS /bin/bash is 3.2).
#
# Requires: bash, cargo, jq (and rust-toolchain.toml / Cargo.lock present).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required" >&2
  exit 1
fi

METADATA="$(cargo metadata --no-deps --format-version 1 --locked)"

# Allowed intra-workspace edges (Architecture §2.2).
# Empty list = root with no workspace deps.
# Services may take cc-types/cc-crypto only for chain and p2p (Phase 1 allowance).
allowed_deps() {
  case "$1" in
    cc-types)       echo "" ;;
    cc-config)      echo "" ;;
    cc-proto)       echo "" ;;
    cc-crypto)      echo "cc-types" ;;
    cc-bootstrap)   echo "cc-proto cc-config" ;;
    cc-chain)       echo "cc-bootstrap cc-config cc-proto cc-types cc-crypto" ;;
    cc-p2p)         echo "cc-bootstrap cc-config cc-proto cc-types cc-crypto" ;;
    cc-attestation) echo "cc-bootstrap cc-config cc-proto" ;;
    cc-engine)      echo "cc-bootstrap cc-config cc-proto" ;;
    cc-beacon-api)  echo "cc-bootstrap cc-config cc-proto" ;;
    cc-storage)     echo "cc-bootstrap cc-config cc-proto" ;;
    *)
      echo "error: unknown workspace member: $1" >&2
      return 1
      ;;
  esac
}

is_allowed() {
  # $1 = depender, $2 = dependee
  local allow a
  allow="$(allowed_deps "$1")" || return 1
  for a in $allow; do
    if [[ "$a" == "$2" ]]; then
      return 0
    fi
  done
  return 1
}

# Workspace member package names (path packages under this workspace only).
MEMBERS=()
while IFS= read -r name; do
  MEMBERS+=("$name")
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | .name
' | sort)

if [[ ${#MEMBERS[@]} -eq 0 ]]; then
  echo "error: no workspace members found" >&2
  exit 1
fi

member_list() {
  printf '%s\n' "${MEMBERS[@]}"
}

is_member() {
  member_list | grep -qxF "$1"
}

manifest_for() {
  echo "$METADATA" | jq -r --arg n "$1" '
    .packages[]
    | select(.name == $n and .source == null)
    | .manifest_path
  '
}

# --- (c) rust-version: workspace pin equals rust-toolchain.toml channel ---
TOOLCHAIN_FILE="$ROOT/rust-toolchain.toml"
if [[ ! -f "$TOOLCHAIN_FILE" ]]; then
  echo "error: missing rust-toolchain.toml" >&2
  exit 1
fi
CHANNEL="$(awk -F'"' '/^[[:space:]]*channel[[:space:]]*=/ { print $2; exit }' "$TOOLCHAIN_FILE")"
# Exact 1.XX.Y only — never stable/beta/nightly or partial pins (Architecture §2.4, R-6).
if [[ -z "$CHANNEL" ]] || ! [[ "$CHANNEL" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: rust-toolchain.toml channel must be an exact 1.XX.Y (got: ${CHANNEL:-empty})" >&2
  exit 1
fi

WS_RUST_VERSION="$(awk -F'"' '/^[[:space:]]*rust-version[[:space:]]*=/ { print $2; exit }' "$ROOT/Cargo.toml")"
if [[ "$WS_RUST_VERSION" != "$CHANNEL" ]]; then
  echo "error: [workspace.package] rust-version ($WS_RUST_VERSION) != rust-toolchain.toml channel ($CHANNEL)" >&2
  exit 1
fi

FAILED=0

for pkg in "${MEMBERS[@]}"; do
  # Reject unknown members early via allowed_deps.
  if ! allowed_deps "$pkg" >/dev/null; then
    FAILED=1
    continue
  fi

  manifest="$(manifest_for "$pkg")"
  if [[ -z "$manifest" || ! -f "$manifest" ]]; then
    echo "error: no manifest for $pkg" >&2
    FAILED=1
    continue
  fi

  # --- (b) [lints] workspace = true ---
  if ! grep -qE '^[[:space:]]*\[lints\]' "$manifest"; then
    echo "error: $pkg: missing [lints] table (need workspace = true)" >&2
    FAILED=1
  elif ! awk '
    /^[[:space:]]*\[lints\]/ { in_lints=1; next }
    /^[[:space:]]*\[/ { in_lints=0 }
    in_lints && /^[[:space:]]*workspace[[:space:]]*=[[:space:]]*true([[:space:]]|#|$)/ { found=1 }
    END { exit !found }
  ' "$manifest"; then
    echo "error: $pkg: [lints] must set workspace = true" >&2
    FAILED=1
  fi

  # --- (c) package rust-version inherits from workspace ---
  pkg_rv="$(echo "$METADATA" | jq -r --arg n "$pkg" '
    .packages[] | select(.name == $n and .source == null) | .rust_version // empty
  ')"
  if [[ -z "$pkg_rv" ]]; then
    echo "error: $pkg: rust-version not set (must inherit workspace rust-version = $CHANNEL)" >&2
    FAILED=1
  elif [[ "$pkg_rv" != "$CHANNEL" ]]; then
    echo "error: $pkg: rust-version ($pkg_rv) != workspace/toolchain ($CHANNEL)" >&2
    FAILED=1
  fi
  if grep -qE '^[[:space:]]*rust-version\.workspace[[:space:]]*=[[:space:]]*true([[:space:]]|#|$)' "$manifest"; then
    : # inherits workspace — required form
  elif grep -qE '^[[:space:]]*rust-version[[:space:]]*=' "$manifest"; then
    lit="$(awk -F'"' '/^[[:space:]]*rust-version[[:space:]]*=/ { print $2; exit }' "$manifest")"
    if [[ "$lit" != "$CHANNEL" ]]; then
      echo "error: $pkg: literal rust-version ($lit) != $CHANNEL; use rust-version.workspace = true" >&2
      FAILED=1
    fi
  else
    echo "error: $pkg: missing rust-version.workspace = true in [package]" >&2
    FAILED=1
  fi

  # --- (a) intra-workspace edges must be in the allowed table ---
  deps="$(echo "$METADATA" | jq -r --arg n "$pkg" '
    .packages[]
    | select(.name == $n and .source == null)
    | .dependencies[]?
    | select(.path != null)
    | .name
  ' | sort -u)"

  while IFS= read -r dep; do
    [[ -z "$dep" ]] && continue
    # Only enforce edges to other workspace members.
    if ! is_member "$dep"; then
      continue
    fi
    if ! is_allowed "$pkg" "$dep"; then
      echo "error: $pkg: forbidden workspace dependency on $dep" >&2
      FAILED=1
    fi
  done <<< "$deps"
done

if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi

echo "check-crate-dag: ok (${#MEMBERS[@]} members, channel $CHANNEL)"
