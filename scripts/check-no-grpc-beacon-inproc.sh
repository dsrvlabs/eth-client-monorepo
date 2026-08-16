#!/usr/bin/env bash
# S2-A-13 — the in-process boot harness test binary has no gRPC.
#
# `cargo tree -p cc-beacon-inproc` must not list `tonic` (or tonic-*) or
# `cc-proto`. Assert mechanically; do not read Cargo.toml by eye ([PRD] §7.5).
#
# Usage:
#   bash scripts/check-no-grpc-beacon-inproc.sh              # live cargo tree
#   bash scripts/check-no-grpc-beacon-inproc.sh --self-test  # matcher only
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ARG="${1:-}"
if [[ "$ARG" == "-h" || "$ARG" == "--help" ]]; then
  echo "Usage: bash scripts/check-no-grpc-beacon-inproc.sh [--self-test]"
  echo "Fail if cc-beacon-inproc's cargo tree contains tonic or cc-proto (S2-A-13)."
  exit 0
fi
if [[ -n "$ARG" && "$ARG" != "--self-test" ]]; then
  echo "error: unknown argument: $ARG (want --self-test)" >&2
  exit 1
fi

# Package-name lines in `cargo tree` look like `tonic v0.14.6` / `cc-proto v0.1.0`.
# Do not match comments or path fragments. tonic-* (health/prost/…) counts.
grpc_hits() {
  local tree="$1"
  printf '%s\n' "$tree" | grep -E '(^|[[:space:]])(tonic(-[A-Za-z0-9]+)?|cc-proto) v' || true
}

if [[ "$ARG" == "--self-test" ]]; then
  dirty='cc-beacon-inproc v0.1.0
├── anyhow v1.0.100
└── tonic v0.14.6
    └── cc-proto v0.1.0'
  clean='cc-beacon-inproc v0.1.0
├── anyhow v1.0.100
├── cc-store v0.1.0
│   └── cc-types v0.1.0
└── tracing v0.1.44'
  dirty_hits="$(grpc_hits "$dirty")"
  if [[ -z "$dirty_hits" ]]; then
    echo "error: self-test: matcher must flag tonic / cc-proto" >&2
    exit 1
  fi
  clean_hits="$(grpc_hits "$clean")"
  if [[ -n "$clean_hits" ]]; then
    echo "error: self-test: matcher flagged a clean tree:" >&2
    echo "$clean_hits" >&2
    exit 1
  fi
  echo "ok: check-no-grpc-beacon-inproc matcher (self-test)"
  exit 0
fi

check_pkg() {
  local pkg="$1"
  local tree
  tree="$(cargo tree -p "$pkg" --edges normal,build,dev --locked)" || {
    echo "error: cargo tree -p $pkg --locked failed (S2-A-14: do not fall back to unlocked)" >&2
    exit 1
  }
  local hits
  hits="$(grpc_hits "$tree")"
  if [[ -n "$hits" ]]; then
    echo "error: $pkg pulls tonic or cc-proto (S2-A-13/A-14):" >&2
    echo "$hits" >&2
    echo >&2
    echo "hint: the in-process import path must not depend on gRPC crates." >&2
    exit 1
  fi
  echo "ok: $pkg cargo tree has no tonic / cc-proto"
}

check_pkg cc-beacon-inproc
check_pkg cc-beacon-import
