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

# Allowed intra-workspace edges (Architecture §2.2 / Phase 1 §1.2).
# Empty list = root with no workspace deps.
# Services may take cc-types/cc-crypto only for chain and p2p (Phase 1 allowance).
# cc-driver edge rule removed with the crate at CC-28 (was ADR-P1-13: {cc-proto, cc-config}).
# cc-spec-tests has no workspace edges (zero outgoing); cc-types may take it as a
# test-only harness edge for ssz_static / vector runners (dev-dependency).
allowed_deps() {
  case "$1" in
    cc-types)             echo "cc-spec-tests" ;;
    cc-config)            echo "" ;;
    cc-proto)             echo "" ;;
    # Phase 2: zero workspace deps permanently (Architecture §1.2 / CC-2K).
    cc-libp2p)            echo "" ;;
    cc-crypto)            echo "cc-types" ;;
    cc-bootstrap)         echo "cc-proto cc-config" ;;
    cc-state-transition)  echo "cc-types cc-crypto" ;;
    cc-fork-choice)       echo "cc-state-transition cc-types cc-crypto" ;;
    cc-spec-tests)        echo "" ;;
    # Self-devnet generator (CC-2K member; content is CC-2Ja).
    cc-devnet-gen)        echo "cc-types cc-crypto cc-state-transition cc-config" ;;
    cc-chain)             echo "cc-bootstrap cc-config cc-proto cc-types cc-crypto cc-state-transition cc-fork-choice" ;;
    # Phase 2: services/p2p may take cc-libp2p (CC-2K / Architecture §1.2).
    cc-p2p)               echo "cc-bootstrap cc-config cc-proto cc-types cc-crypto cc-libp2p" ;;
    cc-attestation)       echo "cc-bootstrap cc-config cc-proto" ;;
    cc-engine)            echo "cc-bootstrap cc-config cc-proto" ;;
    cc-beacon-api)        echo "cc-bootstrap cc-config cc-proto" ;;
    cc-storage)           echo "cc-bootstrap cc-config cc-proto" ;;
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

# --- CC-20/1: only cc-libp2p may declare a libp2p* dependency ---
# Workspace root may pin libp2p in [workspace.dependencies]; every path package
# other than cc-libp2p is forbidden from naming libp2p* in any dependency table.
# Uses cargo metadata (normal + dev + build) so feature-gated / renamed edges count.
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  pkg="${line%%$'\t'*}"
  dep="${line#*$'\t'}"
  if [[ "$pkg" == "cc-libp2p" ]]; then
    continue
  fi
  echo "error: $pkg: only cc-libp2p may declare a libp2p* dependency (found $dep)" >&2
  FAILED=1
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | . as $p
  | .dependencies[]?
  | select(.name | test("^libp2p"))
  | "\($p.name)\t\(.name)"
')

# Manifest scan backup: any libp2p* key or [dependencies.libp2p*] table outside
# crates/libp2p and the workspace root pin. Process substitution — no /tmp file.
while IFS= read -r manifest; do
  [[ -z "$manifest" ]] && continue
  case "$manifest" in
    */crates/libp2p/Cargo.toml) continue ;;
    "$ROOT/Cargo.toml") continue ;;
  esac
  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    echo "error: ${manifest#"$ROOT"/}: only cc-libp2p may declare a libp2p* dependency ($hit)" >&2
    FAILED=1
  done < <(grep -nE \
    '^[[:space:]]*libp2p[a-zA-Z0-9_-]*[[:space:]]*=|^[[:space:]]*\[dependencies\.libp2p[a-zA-Z0-9_-]*\]' \
    "$manifest" 2>/dev/null || true)
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | .manifest_path
' | sort -u)

# --- CC-20/1 pin integrity: direct libp2p* deps must resolve to the workspace git rev ---
# Parse 40-hex rev from root [workspace.dependencies] libp2p = { git = "...", rev = "..." }.
EXPECTED_LIBP2P_REV="$(
  awk '
    /^[[:space:]]*libp2p[[:space:]]*=[[:space:]]*\{/ {
      line = $0
      # Multi-line table: keep reading until closing brace line if needed.
      while (line !~ /\}/ && (getline nxt) > 0) { line = line " " nxt }
      if (match(line, /rev[[:space:]]*=[[:space:]]*"[0-9a-fA-F]+"/)) {
        s = substr(line, RSTART, RLENGTH)
        sub(/^rev[[:space:]]*=[[:space:]]*"/, "", s)
        sub(/"$/, "", s)
        print s
        exit
      }
    }
  ' "$ROOT/Cargo.toml"
)"
if [[ -z "$EXPECTED_LIBP2P_REV" ]]; then
  echo "error: root Cargo.toml: could not parse libp2p workspace pin rev= (need git pin with 40-hex rev)" >&2
  FAILED=1
elif ! [[ "$EXPECTED_LIBP2P_REV" =~ ^[0-9a-f]{40}$ ]]; then
  echo "error: root Cargo.toml: libp2p rev must be lowercase 40-hex (got: $EXPECTED_LIBP2P_REV)" >&2
  FAILED=1
else
  # Reject branch=/tag= form on the workspace pin (rev-only).
  if awk '
    /^[[:space:]]*libp2p[[:space:]]*=[[:space:]]*\{/ {
      line = $0
      while (line !~ /\}/ && (getline nxt) > 0) { line = line " " nxt }
      if (line ~ /branch[[:space:]]*=/ || line ~ /tag[[:space:]]*=/) { found=1 }
    }
    END { exit !found }
  ' "$ROOT/Cargo.toml"; then
    echo "error: root Cargo.toml: libp2p pin must use rev= only (no branch=/tag=)" >&2
    FAILED=1
  fi
  if ! grep -qE 'libp2p[[:space:]]*=[[:space:]]*\{[^}]*git[[:space:]]*=[[:space:]]*"https://github.com/libp2p/rust-libp2p"' "$ROOT/Cargo.toml" \
    && ! awk '
      /^[[:space:]]*libp2p[[:space:]]*=[[:space:]]*\{/ {
        line = $0
        while (line !~ /\}/ && (getline nxt) > 0) { line = line " " nxt }
        if (line ~ /git[[:space:]]*=[[:space:]]*"https:\/\/github.com\/libp2p\/rust-libp2p"/) { found=1 }
      }
      END { exit !found }
    ' "$ROOT/Cargo.toml"; then
    echo "error: root Cargo.toml: libp2p must be git-pinned to https://github.com/libp2p/rust-libp2p" >&2
    FAILED=1
  fi

  EXPECTED_LIBP2P_SOURCE="git+https://github.com/libp2p/rust-libp2p?rev=${EXPECTED_LIBP2P_REV}"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pkg="${line%%$'\t'*}"
    rest="${line#*$'\t'}"
    dep="${rest%%$'\t'*}"
    src="${rest#*$'\t'}"
    if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE" && "$src" != "${EXPECTED_LIBP2P_SOURCE}#${EXPECTED_LIBP2P_REV}" ]]; then
      # cargo metadata may omit #fragment; accept either form. Reject crates.io / other revs.
      if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE"* ]]; then
        echo "error: $pkg: direct dependency $dep must resolve to workspace git pin" >&2
        echo "error:   expected source prefix: $EXPECTED_LIBP2P_SOURCE" >&2
        echo "error:   got: ${src:-<empty/registry>}" >&2
        FAILED=1
      fi
    fi
  done < <(echo "$METADATA" | jq -r '
    .packages[]
    | select(.source == null)
    | . as $p
    | .dependencies[]?
    | select(.name | test("^libp2p"))
    | "\($p.name)\t\(.name)\t\(.source // "")"
  ')

  # Resolved graph: any package named exactly "libp2p" in the lock must be the pin
  # (catches lockfile drift vs declaration). Hybrid crates.io libp2p-identity is OK.
  FULL_META="$(cargo metadata --format-version 1 --locked)"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    name="${line%%$'\t'*}"
    src="${line#*$'\t'}"
    if [[ "$name" == "libp2p" ]]; then
      if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE"* ]]; then
        echo "error: resolved package libp2p is not the workspace git pin" >&2
        echo "error:   expected source prefix: $EXPECTED_LIBP2P_SOURCE" >&2
        echo "error:   got: $src" >&2
        FAILED=1
      fi
    elif [[ "$name" =~ ^libp2p- ]] && [[ "$src" == git+https://github.com/libp2p/rust-libp2p* ]]; then
      # Protocol crates from the monorepo must share the same rev.
      if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE"* ]]; then
        echo "error: resolved $name is from rust-libp2p but not the pinned rev" >&2
        echo "error:   expected source prefix: $EXPECTED_LIBP2P_SOURCE" >&2
        echo "error:   got: $src" >&2
        FAILED=1
      fi
    fi
  done < <(echo "$FULL_META" | jq -r '
    .packages[]
    | select(.name | test("^libp2p"))
    | "\(.name)\t\(.source // "")"
  ')

  # Docs + greppable constant must match Cargo.toml rev (OQ-7 compensating control).
  for doc in "$ROOT/docs/p2p-dependencies.md" "$ROOT/docs/supply-chain.md"; do
    if [[ ! -f "$doc" ]]; then
      echo "error: missing $doc (must record libp2p rev)" >&2
      FAILED=1
    elif ! grep -qF "$EXPECTED_LIBP2P_REV" "$doc"; then
      echo "error: ${doc#"$ROOT"/}: does not contain libp2p rev $EXPECTED_LIBP2P_REV (OQ-7 / docs drift)" >&2
      FAILED=1
    fi
  done
  LIBRS="$ROOT/crates/libp2p/src/lib.rs"
  if [[ ! -f "$LIBRS" ]]; then
    echo "error: missing crates/libp2p/src/lib.rs (need pub const LIBP2P_GIT_REV)" >&2
    FAILED=1
  elif ! grep -qE "pub const LIBP2P_GIT_REV: &str = \"${EXPECTED_LIBP2P_REV}\"" "$LIBRS"; then
    echo "error: crates/libp2p/src/lib.rs: LIBP2P_GIT_REV must equal Cargo.toml rev $EXPECTED_LIBP2P_REV" >&2
    FAILED=1
  fi
fi

if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi

echo "check-crate-dag: ok (${#MEMBERS[@]} members, channel $CHANNEL, libp2p rev ${EXPECTED_LIBP2P_REV:-n/a})"
