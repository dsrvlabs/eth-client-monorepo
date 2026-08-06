#!/usr/bin/env bash
# CC-11d — run the KZG criterion matrix and regenerate docs/kzg-benchmark.md results.
#
# Usage:
#   bash scripts/bench-kzg.sh
#
# Env:
#   KZG_BENCH_SAMPLES       criterion sample size (default 15, min 10)
#   KZG_BENCH_MEASURE_SECS  per-benchmark measurement window seconds (default 3)
#   KZG_BENCH_SKIP_RUN=1    only regenerate the doc from existing criterion data
#
# Exit 0 on success. Regenerates the results section of docs/kzg-benchmark.md
# with no manual editing required.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DOC="$ROOT/docs/kzg-benchmark.md"
CRITERION_ROOT="$ROOT/target/criterion"
BENCH_LOG="$ROOT/target/kzg-bench.log"
FEATURES="kzg-c-kzg,kzg-rust-eth-kzg"

export KZG_BENCH_SAMPLES="${KZG_BENCH_SAMPLES:-15}"
export KZG_BENCH_MEASURE_SECS="${KZG_BENCH_MEASURE_SECS:-3}"

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required to parse criterion estimates" >&2
  exit 1
fi

mkdir -p "$ROOT/target"

echo "==> compile-check under each feature combination (cargo bench --no-run)"
# Acceptance: cargo bench --no-run under each combination the script uses.
cargo bench -p cc-crypto --no-run --no-default-features --features kzg-c-kzg --bench kzg --locked
cargo bench -p cc-crypto --no-run --no-default-features --features kzg-rust-eth-kzg --bench kzg --locked
cargo bench -p cc-crypto --no-run --features "$FEATURES" --bench kzg --locked

if [[ "${KZG_BENCH_SKIP_RUN:-0}" != "1" ]]; then
  echo "==> running criterion matrix (samples=${KZG_BENCH_SAMPLES}, measure=${KZG_BENCH_MEASURE_SECS}s)"
  # Capture stdout (includes kzg-setup-memory lines) and leave criterion JSON under target/criterion.
  set +e
  cargo bench -p cc-crypto --features "$FEATURES" --bench kzg --locked -- --noplot \
    2>&1 | tee "$BENCH_LOG"
  bench_rc=${PIPESTATUS[0]}
  set -e
  if [[ "$bench_rc" -ne 0 ]]; then
    echo "error: cargo bench failed with exit $bench_rc" >&2
    exit "$bench_rc"
  fi
else
  echo "==> KZG_BENCH_SKIP_RUN=1 — reusing existing criterion data + log"
  if [[ ! -d "$CRITERION_ROOT" ]]; then
    echo "error: no criterion data at $CRITERION_ROOT" >&2
    exit 1
  fi
  if [[ ! -f "$BENCH_LOG" ]]; then
    echo "warning: no $BENCH_LOG — setup-memory table will be empty" >&2
  fi
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# mean ns from criterion estimates.json → human string
estimate_mean_ns() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "n/a"
    return
  fi
  local ns
  ns="$(jq -r '.mean.point_estimate // empty' "$path" 2>/dev/null || true)"
  if [[ -z "$ns" || "$ns" == "null" ]]; then
    echo "n/a"
    return
  fi
  # ns → human
  awk -v ns="$ns" 'BEGIN {
    if (ns >= 1e9) printf "%.3f s", ns/1e9;
    else if (ns >= 1e6) printf "%.3f ms", ns/1e6;
    else if (ns >= 1e3) printf "%.3f µs", ns/1e3;
    else printf "%.1f ns", ns;
  }'
}

# raw mean seconds (float) for comparison; empty if missing
estimate_mean_secs() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo ""
    return
  fi
  jq -r 'if .mean.point_estimate then (.mean.point_estimate / 1e9) else empty end' "$path" 2>/dev/null || true
}

# Criterion path layout: target/criterion/<group>/<function>[/<input>]/new/estimates.json
# BenchmarkId::new("a/b", "input") → "a_b/input" (slashes in *function* → underscores).
# bench_function("a/b") → "a_b".
criterion_fn() {
  printf '%s' "$1" | tr '/' '_'
}

# est GROUP FUNCTION [INPUT]
est() {
  local group="$1"
  local fn
  fn="$(criterion_fn "$2")"
  local path
  if [[ $# -ge 3 && -n "${3:-}" ]]; then
    path="$CRITERION_ROOT/$group/$fn/$3/new/estimates.json"
  else
    path="$CRITERION_ROOT/$group/$fn/new/estimates.json"
  fi
  estimate_mean_ns "$path"
}

# est_secs GROUP FUNCTION [INPUT]
est_secs() {
  local group="$1"
  local fn
  fn="$(criterion_fn "$2")"
  local path
  if [[ $# -ge 3 && -n "${3:-}" ]]; then
    path="$CRITERION_ROOT/$group/$fn/$3/new/estimates.json"
  else
    path="$CRITERION_ROOT/$group/$fn/new/estimates.json"
  fi
  estimate_mean_secs "$path"
}

# Machine spec
uname_s="$(uname -s)"
uname_m="$(uname -m)"
cpu="unknown"
cores="unknown"
mem="unknown"
if [[ "$uname_s" == "Darwin" ]]; then
  cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
  cores="$(sysctl -n hw.ncpu 2>/dev/null || echo unknown)"
  mem_bytes="$(sysctl -n hw.memsize 2>/dev/null || echo 0)"
  mem="$(awk -v b="$mem_bytes" 'BEGIN { printf "%.1f GiB", b/1024/1024/1024 }')"
elif [[ -r /proc/cpuinfo ]]; then
  cpu="$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
  cores="$(nproc 2>/dev/null || echo unknown)"
  if [[ -r /proc/meminfo ]]; then
    mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
    mem="$(awk -v k="$mem_kb" 'BEGIN { printf "%.1f GiB", k/1024/1024 }')"
  fi
fi
rustc_v="$(rustc --version 2>/dev/null || echo unknown)"
date_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Setup memory lines from bench log
setup_mem_table_rows() {
  if [[ ! -f "$BENCH_LOG" ]]; then
    echo "| *(no setup-memory lines — re-run without KZG_BENCH_SKIP_RUN)* | | | |"
    return
  fi
  local any=0
  while IFS=$'\t' read -r _tag label wall rss_before rss_after rss_delta; do
    [[ "$_tag" == "kzg-setup-memory" ]] || continue
    any=1
    wall_v="${wall#wall_ms=}"
    before_v="${rss_before#rss_before=}"
    after_v="${rss_after#rss_after=}"
    delta_v="${rss_delta#rss_delta=}"
    printf '| `%s` | %s ms | %s → %s | %s |\n' \
      "$label" "$wall_v" "$before_v" "$after_v" "$delta_v"
  done < <(grep $'^kzg-setup-memory\t' "$BENCH_LOG" || true)
  if [[ "$any" -eq 0 ]]; then
    echo "| *(setup-memory lines missing from log)* | | | |"
  fi
}

# ---------------------------------------------------------------------------
# Choose default from Phase 2 verify times (decisive for CC-24)
# Prefer precomp-on configs when available; fall back to off.
# ---------------------------------------------------------------------------

p2_ckzg0="$(est_secs verify_cell_kzg_proof_batch 'c-kzg/precompute-0' 'phase2-8col-21blob')"
p2_ckzg8="$(est_secs verify_cell_kzg_proof_batch 'c-kzg/precompute-8' 'phase2-8col-21blob')"
p2_rek_off="$(est_secs verify_cell_kzg_proof_batch 'rust_eth_kzg/precomp-off' 'phase2-8col-21blob')"
p2_rek_on="$(est_secs verify_cell_kzg_proof_batch 'rust_eth_kzg/precomp-on-w8' 'phase2-8col-21blob')"

# Pick the faster *backend family* on Phase 2 verify, then decide precompute.
# Precompute is only kept as the default when it beats precomp-off by >5% on
# the Phase 2 verify cell — otherwise the ~100 MiB table is not worth it
# (and precompute can regress compute/recover for c-kzg).
fmt_secs() {
  local s="$1"
  [[ -n "$s" ]] || { echo "n/a"; return; }
  awk -v s="$s" 'BEGIN {
    if (s >= 1) printf "%.3f s", s;
    else if (s >= 1e-3) printf "%.3f ms", s*1e3;
    else printf "%.3f µs", s*1e6;
  }'
}

within_pct() {
  # return 0 if |a-b|/min(a,b) <= pct/100
  local a="$1" b="$2" pct="$3"
  awk -v a="$a" -v b="$b" -v p="$pct" 'BEGIN {
    if (a <= 0 || b <= 0) exit 1;
    m = (a < b) ? a : b;
    d = (a > b) ? a - b : b - a;
    exit !(d / m <= p / 100.0);
  }'
}

faster() {
  # echo the label of the smaller secs; empty inputs lose
  local la="$1" sa="$2" lb="$3" sb="$4"
  if [[ -z "$sa" && -z "$sb" ]]; then echo ""; return; fi
  if [[ -z "$sa" ]]; then echo "$lb"; return; fi
  if [[ -z "$sb" ]]; then echo "$la"; return; fi
  awk -v a="$sa" -v b="$sb" 'BEGIN { exit !(a <= b) }' && echo "$la" || echo "$lb"
}

best_ckzg_secs=""
best_ckzg_label=""
if [[ -n "$p2_ckzg0" || -n "$p2_ckzg8" ]]; then
  # Prefer precompute=0 when within 5% of precompute=8.
  if [[ -n "$p2_ckzg0" && -n "$p2_ckzg8" ]] && within_pct "$p2_ckzg0" "$p2_ckzg8" 5; then
    best_ckzg_label="c-kzg (precompute=0)"
    best_ckzg_secs="$p2_ckzg0"
  else
    win="$(faster "c-kzg (precompute=0)" "$p2_ckzg0" "c-kzg (precompute=8)" "$p2_ckzg8")"
    if [[ "$win" == *"precompute=8"* ]]; then
      best_ckzg_label="c-kzg (precompute=8)"
      best_ckzg_secs="$p2_ckzg8"
    else
      best_ckzg_label="c-kzg (precompute=0)"
      best_ckzg_secs="$p2_ckzg0"
    fi
  fi
fi

best_rek_secs=""
best_rek_label=""
if [[ -n "$p2_rek_off" || -n "$p2_rek_on" ]]; then
  if [[ -n "$p2_rek_off" && -n "$p2_rek_on" ]] && within_pct "$p2_rek_off" "$p2_rek_on" 5; then
    best_rek_label="rust_eth_kzg (UsePrecomp::No)"
    best_rek_secs="$p2_rek_off"
  else
    win="$(faster "rust_eth_kzg (UsePrecomp::No)" "$p2_rek_off" "rust_eth_kzg (UsePrecomp::Yes { width: 8 })" "$p2_rek_on")"
    if [[ "$win" == *"Yes"* ]]; then
      best_rek_label="rust_eth_kzg (UsePrecomp::Yes { width: 8 })"
      best_rek_secs="$p2_rek_on"
    else
      best_rek_label="rust_eth_kzg (UsePrecomp::No)"
      best_rek_secs="$p2_rek_off"
    fi
  fi
fi

best_label=""
best_secs=""
win_family="$(faster "$best_ckzg_label" "$best_ckzg_secs" "$best_rek_label" "$best_rek_secs")"
if [[ -n "$win_family" ]]; then
  if [[ "$win_family" == c-kzg* ]]; then
    best_label="$best_ckzg_label"
    best_secs="$best_ckzg_secs"
  else
    best_label="$best_rek_label"
    best_secs="$best_rek_secs"
  fi
fi

chosen_backend="c-kzg"
chosen_kind="CKzg"
chosen_feature="kzg-c-kzg"
chosen_precomp_note="DEFAULT_PRECOMPUTE = 0; rust_eth_kzg DEFAULT_USE_PRECOMP = No"
chosen_reason="Phase 2 batch (8 columns × 21 blobs) verify_cell_kzg_proof_batch is fastest on this machine under the measured matrix."
best_human="$(fmt_secs "${best_secs:-}")"

if [[ -n "$best_label" ]]; then
  case "$best_label" in
    c-kzg*)
      chosen_backend="c-kzg"
      chosen_kind="CKzg"
      chosen_feature="kzg-c-kzg"
      if [[ "$best_label" == *"precompute=8"* ]]; then
        chosen_precomp_note="DEFAULT_PRECOMPUTE = 8 (precompute table on; ~100 MiB RSS delta); rust_eth_kzg DEFAULT_USE_PRECOMP = No"
      else
        chosen_precomp_note="DEFAULT_PRECOMPUTE = 0 (no precompute table — Phase 2 verify within 5% of precompute=8 while saving ~96 MiB and avoiding compute/recover regression); rust_eth_kzg DEFAULT_USE_PRECOMP = No"
      fi
      ckzg_vs_rek=""
      if [[ -n "$best_ckzg_secs" && -n "$best_rek_secs" ]]; then
        ckzg_vs_rek=" (~$(awk -v a="$best_rek_secs" -v b="$best_ckzg_secs" 'BEGIN { printf "%.1f", a/b }')× faster Phase 2 verify than best rust_eth_kzg)"
      fi
      chosen_reason="On the Phase 2 batch shape (8×21) that CC-24 inherits, **${best_label}** wins \`verify_cell_kzg_proof_batch\` at ${best_human}${ckzg_vs_rek}."
      ;;
    rust_eth_kzg*)
      chosen_backend="rust_eth_kzg"
      chosen_kind="RustEthKzg"
      chosen_feature="kzg-rust-eth-kzg"
      if [[ "$best_label" == *"Yes"* ]]; then
        chosen_precomp_note="DEFAULT_USE_PRECOMP = Yes { width: 8 }; c-kzg DEFAULT_PRECOMPUTE remains 0 for the non-default backend"
      else
        chosen_precomp_note="DEFAULT_USE_PRECOMP = No; c-kzg DEFAULT_PRECOMPUTE remains 0 for the non-default backend"
      fi
      chosen_reason="On the Phase 2 batch shape (8×21) that CC-24 inherits, **${best_label}** wins \`verify_cell_kzg_proof_batch\` at ${best_human}."
      ;;
  esac
else
  chosen_reason="Criterion estimates were unavailable; retaining prior default \`c-kzg\` (feature \`kzg-c-kzg\`). Re-run without KZG_BENCH_SKIP_RUN."
fi

echo "==> chosen default candidate: $best_label (${best_human}) → $chosen_backend / $chosen_kind"

# ---------------------------------------------------------------------------
# Emit document
# ---------------------------------------------------------------------------

{
  cat <<EOF
# KZG backend benchmark (CC-11d)

Measurement artifact for **CC-11/4** and the Phase 1 half of **OQ-4**.
Regenerated by \`scripts/bench-kzg.sh\` — do not hand-edit the results tables.

## rust_eth_kzg proof-verification-failure variant (CC-11c)

// KZG-VARIANT: rust_eth_kzg 0.10.0 cell-path proof failure is
// \`Error::Verifier(VerifierError::FK20(_))\` (FK20 wraps
// \`kzg_multi_open::VerifierError::InvalidProof\`). The 4844 path uses
// \`Error::EIP4844(eip4844::Error::Verifier(eip4844::VerifierError::InvalidProof))\`.
// Public detector: \`Error::is_proof_invalid()\` (nested \`VerifierError\` is not
// re-exported at the crate root). Adapter maps \`is_proof_invalid() == true\` →
// \`Ok(false)\`; every other \`Err\` → \`KzgError\` (never \`Ok(false)\`).

## Machine spec

| Field | Value |
|---|---|
| Captured (UTC) | ${date_utc} |
| OS | ${uname_s} (${uname_m}) |
| CPU | ${cpu} |
| Logical cores | ${cores} |
| Memory | ${mem} |
| rustc | ${rustc_v} |

## Criterion sample configuration

| Parameter | Value |
|---|---|
| Sample size | ${KZG_BENCH_SAMPLES} (env \`KZG_BENCH_SAMPLES\`, min 10) |
| Measurement time | ${KZG_BENCH_MEASURE_SECS}s (env \`KZG_BENCH_MEASURE_SECS\`) |
| Warm-up | 1s (setup group 500ms) |
| Features | \`${FEATURES}\` |
| Backend pins | \`c-kzg\` **2.1.8**, \`rust_eth_kzg\` **0.10.0** |
| Precompute "on" | c-kzg \`precompute=8\`; rust_eth_kzg \`UsePrecomp::Yes { width: 8 }\` |
| Phase 1 batch | 1 column × 1 blob |
| Phase 2 batch | 8 columns × 21 blobs (Hoodi BPO max) = **168** cells |

Times below are criterion **mean** point estimates from \`target/criterion/**/new/estimates.json\`.

## Chosen default (CC-11/4)

| Field | Value |
|---|---|
| **Backend** | \`${chosen_backend}\` |
| **Config enum** | \`KzgBackendKind::${chosen_kind}\` (\`Default\`) |
| **Crate default feature** | \`${chosen_feature}\` |
| **Precompute defaults** | ${chosen_precomp_note} |
| **Winner cell (Phase 2 verify)** | ${best_label:-n/a} @ ${best_human} |

**Reason:** ${chosen_reason}

Context (not a tiebreaker): \`rust_eth_kzg\` is what Lighthouse runs (pinned at 0.9 there) and its release cadence is slower than \`c-kzg\`'s.

The choice is **recorded here and consumed at CC-18b**. No other issue may pick the default by argument. Both backends remain compilable and both stay in the vector job (CC-11/1).

## Phase 2 inheritance (OQ-4)

The **Phase 2 batch shape** (8 columns × 21 blobs) is the one **CC-24 inherits** for sampling verification on the DA hot path. OQ-4's Phase 2 half **re-opens at CC-24** — re-run this matrix under production sampling load before locking DA-path defaults.

<!-- BEGIN RESULTS -->
## Setup-load time and precompute resident memory

Wall time from a single load (bench stdout) plus criterion mean for repeated loads.
RSS is process-level via \`ps -o rss=\` (kilobytes×1024); deltas are coarse on macOS because the process retains prior loads — treat as order-of-magnitude, not a precise table size.

| Config | One-shot wall | RSS before → after | RSS delta |
|---|---|---|---|
$(setup_mem_table_rows)

| Config | Criterion mean (setup_load) |
|---|---|
| \`c-kzg/precompute-0\` | $(est setup_load 'c-kzg/precompute-0') |
| \`c-kzg/precompute-8\` | $(est setup_load 'c-kzg/precompute-8') |
| \`rust_eth_kzg/precomp-off\` | $(est setup_load 'rust_eth_kzg/precomp-off') |
| \`rust_eth_kzg/precomp-on-w8\` | $(est setup_load 'rust_eth_kzg/precomp-on-w8') |

## Results: \`verify_cell_kzg_proof_batch\`

Primary measurement. Every cell of {backend} × {precompute on, off} × {Phase 1, Phase 2}.

| Backend | Precompute | Phase 1 (1×1) | Phase 2 (8×21) |
|---|---|---|---|
| c-kzg 2.1.8 | off (\`precompute=0\`) | $(est verify_cell_kzg_proof_batch 'c-kzg/precompute-0' 'phase1-1col-1blob') | $(est verify_cell_kzg_proof_batch 'c-kzg/precompute-0' 'phase2-8col-21blob') |
| c-kzg 2.1.8 | on (\`precompute=8\`) | $(est verify_cell_kzg_proof_batch 'c-kzg/precompute-8' 'phase1-1col-1blob') | $(est verify_cell_kzg_proof_batch 'c-kzg/precompute-8' 'phase2-8col-21blob') |
| rust_eth_kzg 0.10.0 | off (\`UsePrecomp::No\`) | $(est verify_cell_kzg_proof_batch 'rust_eth_kzg/precomp-off' 'phase1-1col-1blob') | $(est verify_cell_kzg_proof_batch 'rust_eth_kzg/precomp-off' 'phase2-8col-21blob') |
| rust_eth_kzg 0.10.0 | on (\`UsePrecomp::Yes { width: 8 }\`) | $(est verify_cell_kzg_proof_batch 'rust_eth_kzg/precomp-on-w8' 'phase1-1col-1blob') | $(est verify_cell_kzg_proof_batch 'rust_eth_kzg/precomp-on-w8' 'phase2-8col-21blob') |

## Results: \`compute_cells_and_kzg_proofs\` (one blob)

Phase 7 proposer path — one size each.

| Backend | Precompute | Mean |
|---|---|---|
| c-kzg 2.1.8 | off (\`precompute=0\`) | $(est compute_cells_and_kzg_proofs 'c-kzg/precompute-0') |
| c-kzg 2.1.8 | on (\`precompute=8\`) | $(est compute_cells_and_kzg_proofs 'c-kzg/precompute-8') |
| rust_eth_kzg 0.10.0 | off (\`UsePrecomp::No\`) | $(est compute_cells_and_kzg_proofs 'rust_eth_kzg/precomp-off') |
| rust_eth_kzg 0.10.0 | on (\`UsePrecomp::Yes { width: 8 }\`) | $(est compute_cells_and_kzg_proofs 'rust_eth_kzg/precomp-on-w8') |

## Results: \`recover_cells_and_kzg_proofs\` (64 of 128 cells)

Phase 2 reconstruction path — one size each (half the extended cells).

| Backend | Precompute | Mean |
|---|---|---|
| c-kzg 2.1.8 | off (\`precompute=0\`) | $(est recover_cells_and_kzg_proofs 'c-kzg/precompute-0') |
| c-kzg 2.1.8 | on (\`precompute=8\`) | $(est recover_cells_and_kzg_proofs 'c-kzg/precompute-8') |
| rust_eth_kzg 0.10.0 | off (\`UsePrecomp::No\`) | $(est recover_cells_and_kzg_proofs 'rust_eth_kzg/precomp-off') |
| rust_eth_kzg 0.10.0 | on (\`UsePrecomp::Yes { width: 8 }\`) | $(est recover_cells_and_kzg_proofs 'rust_eth_kzg/precomp-on-w8') |
<!-- END RESULTS -->

## Code lockstep

After regenerating this document, ensure:

1. \`crates/crypto/Cargo.toml\` \`default\` feature == \`${chosen_feature}\`
2. \`KzgBackendKind::default()\` == \`KzgBackendKind::${chosen_kind}\`
3. \`tests/kzg.rs::default_backend_kind_matches_chosen_default\` passes

\`scripts/bench-kzg.sh\` prints the candidate; applying feature/\`Default\` changes is a deliberate source edit so the document and the code cannot silently drift without a reviewable diff.
EOF
} >"$DOC"

echo "==> wrote $DOC"
echo "Done. Review Chosen default and apply Cargo.toml / KzgBackendKind::default if the winner changed."
echo "Candidate: backend=${chosen_backend} kind=${chosen_kind} feature=${chosen_feature}"
