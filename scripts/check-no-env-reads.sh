#!/usr/bin/env bash
# CC-09/3 — no ad-hoc std::env::var outside crates/config.
#
# crates/config is the only crate permitted to read the process environment
# (Architecture §5, D-2). services/ and crates/bootstrap/ must not accumulate
# direct reads; compose/local-dev config goes through figment via cc-config.
#
# Exit non-zero and print every hit on failure. Intended as a blocking CI step
# (alongside check-crate-dag.sh in the clippy job).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Restrict to Rust sources so editor swap files etc. cannot false-positive.
# R-5 / CC-45b: exempt #[cfg(test)] modules and integration tests/ trees.
hits="$(find services crates/bootstrap -name '*.rs' ! -path '*/tests/*' -print0 2>/dev/null | xargs -0 awk 'FNR==1{skip=0} /#\[cfg\(test\)\]/{skip=1} skip{next} /std::env::var/{print FILENAME ":" FNR ":" $0}' || true)"

if [[ -n "${hits}" ]]; then
  echo "error: std::env::var found outside crates/config (CC-09/3):" >&2
  echo "${hits}" >&2
  echo >&2
  echo "hint: route configuration through cc_config::load; RUST_LOG/LOG_FORMAT" >&2
  echo "      are resolved inside crates/config (D-2)." >&2
  exit 1
fi

echo "ok: no std::env::var in services/ or crates/bootstrap/"
