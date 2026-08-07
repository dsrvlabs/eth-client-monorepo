#!/usr/bin/env bash
# CC-28/2 — no HTTP client is reachable from the block-import path.
#
# Half 1 (grep-shaped): import-path modules under services/chain must not
# construct or name an HTTP client. `reqwest` remains allowed only in
# `checkpoint_sync.rs` (CC-19 one-shot bootstrap before the import path exists).
#
# Half 2 (structural): `services/p2p` has no HTTP client dependency at all —
# `cargo tree -p cc-p2p` must not list `reqwest` (or other common HTTP clients).
#
# Exit non-zero and print every hit on failure. Intended as a blocking CI step
# (alongside check-crate-dag.sh / check-no-env-reads.sh).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ── Half 1: no HTTP client construction on the import path ──────────────────
# Everything under services/chain/src except checkpoint_sync.rs (bootstrap only)
# and main.rs (orchestrates bootstrap; does not construct a client itself, but
# may mention provider URLs — still must not construct reqwest::Client).
IMPORT_PATH_ROOTS=(
  services/chain/src/import.rs
  services/chain/src/core.rs
  services/chain/src/service.rs
  services/chain/src/da.rs
  services/chain/src/p2p_stream.rs
  services/chain/src/apply_attestations.rs
  services/chain/src/head.rs
  services/chain/src/residency.rs
  services/chain/src/epoch_context.rs
  services/chain/src/events
  services/chain/src/metrics.rs
  services/chain/src/lib.rs
)

# Patterns that would reintroduce an HTTP client onto the import path.
# reqwest is the production client; hyper client builders / ureq are also banned.
PATTERN='reqwest|ureq|::Client::builder\(\)|hyper::client|hyper_util::client'

hits=""
for path in "${IMPORT_PATH_ROOTS[@]}"; do
  if [[ ! -e "$path" ]]; then
    echo "error: missing import-path surface: $path" >&2
    exit 1
  fi
  if [[ -d "$path" ]]; then
    found="$(grep -rnE "$PATTERN" "$path" --include='*.rs' || true)"
  else
    found="$(grep -nE "$PATTERN" "$path" || true)"
    if [[ -n "$found" ]]; then
      # Prefix with path so multi-file output is uniform.
      found="$(echo "$found" | sed "s|^|${path}:|")"
    fi
  fi
  if [[ -n "$found" ]]; then
    hits+="${found}"$'\n'
  fi
done

if [[ -n "${hits}" ]]; then
  echo "error: HTTP client reference on the block-import path (CC-28/2):" >&2
  echo "${hits}" >&2
  echo >&2
  echo "hint: reqwest is allowed only in services/chain/src/checkpoint_sync.rs" >&2
  echo "      (CC-19 one-shot bootstrap). ImportBlock must not construct clients." >&2
  exit 1
fi

# Also: reqwest must not appear in any services/chain/src file other than
# checkpoint_sync.rs (main.rs must not construct one either).
extra="$(
  grep -rnE 'reqwest' services/chain/src --include='*.rs' \
    | grep -v 'services/chain/src/checkpoint_sync\.rs' \
    || true
)"
if [[ -n "${extra}" ]]; then
  echo "error: reqwest outside checkpoint_sync.rs (CC-28/2):" >&2
  echo "${extra}" >&2
  echo >&2
  echo "hint: CC-19 bootstrap is the only HTTP entry; keep it in checkpoint_sync.rs." >&2
  exit 1
fi

# ── Half 2: p2p has no HTTP client dependency ───────────────────────────────
# Package name is cc-p2p (workspace). Edge kinds include normal + build; dev
# edges (test-only) are still forbidden for an HTTP client on this surface.
TREE="$(cargo tree -p cc-p2p --edges normal,build,dev --locked 2>/dev/null || cargo tree -p cc-p2p --edges normal,build,dev)"
if echo "$TREE" | grep -qiE 'reqwest|ureq'; then
  echo "error: services/p2p (cc-p2p) pulls an HTTP client (CC-28/2):" >&2
  echo "$TREE" | grep -iE 'reqwest|ureq' >&2
  echo >&2
  echo "hint: p2p must have zero HTTP client dependencies; blocks arrive over P2P." >&2
  exit 1
fi

echo "ok: no HTTP client on import path; cc-p2p tree has no reqwest/ureq"
