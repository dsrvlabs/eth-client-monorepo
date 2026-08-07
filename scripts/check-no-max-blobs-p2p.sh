#!/usr/bin/env bash
# CC-21b — Phase 1 Blob-Schedule guard applied one layer up (Architecture §4.1).
#
# `compute_fork_version` / `get_blob_parameters` must read the loaded runtime
# config. No constant named MAX_BLOBS_PER_BLOCK* or ELECTRA_FORK_EPOCH may appear
# in services/p2p or crates/libp2p — the mechanical form of "two ways to get the
# fallback branch wrong".
#
# Exit non-zero and print every hit on failure. Wired into `make deps` and the
# CI `deps` job so it blocks merges.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Restrict to Rust sources under the p2p surface + libp2p shim.
hits="$(
  grep -rnE 'MAX_BLOBS_PER_BLOCK|ELECTRA_FORK_EPOCH' \
    services/p2p crates/libp2p \
    --include='*.rs' \
    || true
)"

if [[ -n "${hits}" ]]; then
  echo "error: MAX_BLOBS_PER_BLOCK / ELECTRA_FORK_EPOCH found in services/p2p or crates/libp2p (CC-21b):" >&2
  echo "${hits}" >&2
  echo >&2
  echo "hint: call ChainConfig::get_blob_parameters / compute_fork_version against the" >&2
  echo "      loaded runtime config; do not hard-code Electra/Fulu blob constants." >&2
  exit 1
fi

echo "ok: no MAX_BLOBS_PER_BLOCK / ELECTRA_FORK_EPOCH in services/p2p or crates/libp2p"
