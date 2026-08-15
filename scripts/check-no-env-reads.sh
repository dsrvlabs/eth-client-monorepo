#!/usr/bin/env bash
# CC-09/3 — no ad-hoc env::var / var_os / vars / vars_os outside crates/config.
#
# crates/config is the only crate permitted to read the process environment
# (Architecture §5, D-2). services/ and crates/bootstrap/ must not accumulate
# direct reads; compose/local-dev config goes through figment via cc-config.
#
# Exit non-zero and print every hit on failure. Intended as a blocking CI step
# (alongside check-crate-dag.sh in the clippy job).
#
# Fixtures under scripts/fixtures/check-no-env-reads/ run first (S0a-B-02).
# They live outside the production find so they cannot weaken it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# R-5 / CC-45b: exempt only the item that #[cfg(test)] annotates, then resume.
# The previous `skip=1` never reset, so one test attribute silently exempted
# the rest of the file (mod foo; / use x; / the body after `}`).
# A one-line `{…}` body (rustfmt `mod tests {}`) is net-zero braces and has no
# `;` — treat that as end-of-item so the next production item is scanned.
# A `;` inside `//` / `///` between the attribute and the item is not the item.
#
# Brace depth ignores `{` / `}` inside strings, char/byte lits (`'{'`, `b'{'`),
# line comments, block comments, and raw strings. A live witness was
# services/p2p/src/reqresp/status.rs (`split('{')`) leaving skip_depth=1 to EOF.
#
# Pattern is env::var / var_os / vars / vars_os, with or without a std:: prefix.
# (The old /std::env::var/ form missed the short names.)
#
# Match #[cfg(test)] only at the start of a line so comments / strings that
# mention the attribute (transport.rs) do not start an exemption.
#
# build.rs short-form (use std::env; env::var_os): existing CC-05a compile-time
# cargo-env exception, already documented on crates/bootstrap/build.rs. Not a
# new hole — the old /std::env::var/ pattern never saw that short form either.
# Fully-qualified std::env::var* in build.rs is still a hit.
# shellcheck disable=SC2016  # awk program is a literal; $0 etc. are awk, not bash
AWK_SCAN='
function reset_lex() {
  lx = 0
  raw_n = 0
}

# i points at the opening quote. Char lits: quote + char + quote, or a backslash escape.
# Lifetimes (quote + ident, no closer) must not consume the rest of the line.
function skip_char_or_lifetime(s, i,    n1, n2) {
  n1 = substr(s, i + 1, 1)
  n2 = substr(s, i + 2, 1)
  if (n1 == "\\") {
    i += 2
    if (i <= length(s)) i++
    while (i <= length(s)) {
      if (substr(s, i, 1) == SQ) { i++; break }
      i++
    }
    return i
  }
  if (n2 == SQ) return i + 3
  i++
  while (i <= length(s) && substr(s, i, 1) ~ /[[:alnum:]_]/) i++
  return i
}

# Walk one line. Sets saw_brace / saw_semi for code tokens only.
# lx/raw_n persist across lines (block comments, strings, raw strings).
function scan_line(s,    i, c, nxt, n, prev, j, hashes, ok) {
  n = 0
  saw_brace = 0
  saw_semi = 0
  i = 1
  prev = ""
  while (i <= length(s)) {
    c = substr(s, i, 1)
    nxt = substr(s, i + 1, 1)
    if (lx == 3) {
      if (c == "*" && nxt == "/") { lx = 0; i += 2; prev = "/"; continue }
      i++
      prev = c
      continue
    }
    if (lx == 1) {
      if (c == "\\") { i += 2; prev = ""; continue }
      if (c == "\"") { lx = 0; i++; prev = "\""; continue }
      i++
      prev = c
      continue
    }
    if (lx == 2) {
      if (c == "\"") {
        ok = 1
        for (j = 1; j <= raw_n; j++) {
          if (substr(s, i + j, 1) != "#") { ok = 0; break }
        }
        if (ok) { lx = 0; i += 1 + raw_n; prev = "#"; continue }
      }
      i++
      prev = c
      continue
    }
    if (c == "/" && nxt == "/") break
    if (c == "/" && nxt == "*") { lx = 3; i += 2; prev = "*"; continue }
    if (prev !~ /[[:alnum:]_]/) {
      if (c == "b" && nxt == SQ) {
        i = skip_char_or_lifetime(s, i + 1)
        prev = SQ
        continue
      }
      if (c == "b" && nxt == "\"") { lx = 1; i += 2; prev = "\""; continue }
      if (c == "b" && nxt == "r") {
        hashes = 0
        j = i + 2
        while (substr(s, j, 1) == "#") { hashes++; j++ }
        if (substr(s, j, 1) == "\"") {
          lx = 2
          raw_n = hashes
          i = j + 1
          prev = "\""
          continue
        }
      }
      if (c == "r") {
        hashes = 0
        j = i + 1
        while (substr(s, j, 1) == "#") { hashes++; j++ }
        if (substr(s, j, 1) == "\"") {
          lx = 2
          raw_n = hashes
          i = j + 1
          prev = "\""
          continue
        }
      }
      if (c == "\"") { lx = 1; i++; prev = "\""; continue }
      if (c == SQ) {
        i = skip_char_or_lifetime(s, i)
        prev = SQ
        continue
      }
    }
    if (c == "{") { n++; saw_brace = 1 }
    else if (c == "}") { n-- }
    else if (c == ";") { saw_semi = 1 }
    prev = c
    i++
  }
  return n
}

# After #[cfg(test)], consume the annotated item then clear pending.
# d>0: body still open (skip_depth tracks). `{` with d==0: one-line body.
# `;` in code (not // / /// / attributes): semicolon-terminated item.
function finish_pending(s,    d) {
  d = scan_line(s)
  if (d > 0) {
    skip_depth = d
    pending = 0
  } else if (saw_brace) {
    pending = 0
    reset_lex()
  } else if (saw_semi && s !~ /^[[:space:]]*#\[/) {
    pending = 0
    reset_lex()
  }
}

FNR == 1 { skip_depth = 0; pending = 0; reset_lex(); SQ = sprintf("%c", 39) }

{
  if (pending) {
    finish_pending($0)
    next
  }
  if (skip_depth > 0) {
    skip_depth += scan_line($0)
    if (skip_depth <= 0) {
      skip_depth = 0
      reset_lex()
    }
    next
  }
  if ($0 ~ /^[[:space:]]*#\[cfg\(test\)\]/) {
    pending = 1
    reset_lex()
    finish_pending($0)
    next
  }
  if ($0 ~ /(^|[^[:alnum:]_])(std::)?env::(var|var_os|vars_os|vars)([^[:alnum:]_]|$)/) {
    # CC-05a: existing compile-time cargo-env exception on crate-root build.rs
    # (see crates/bootstrap/build.rs). Short form only; std::env::var* still hits.
    if (FILENAME ~ /(^|\/)build\.rs$/ && $0 !~ /std::env::/) next
    print FILENAME ":" FNR ":" $0
  }
}
'

scan_files() {
  if [[ $# -eq 0 ]]; then
    return 0
  fi
  awk "${AWK_SCAN}" "$@"
}

# ── Fixture self-test (P0-08(b) / S0a-B-02) ────────────────────────────────
FIXTURE_ROOT="${ROOT}/scripts/fixtures/check-no-env-reads"
FAIL_DIR="${FIXTURE_ROOT}/expect-fail"
PASS_DIR="${FIXTURE_ROOT}/expect-pass"

if [[ ! -d "${FAIL_DIR}" || ! -d "${PASS_DIR}" ]]; then
  echo "error: missing fixture dirs under ${FIXTURE_ROOT#"$ROOT"/}" >&2
  exit 1
fi

selftest_failed=0

for required in \
  cfg-test-mod-then-prod-var.rs \
  cfg-test-empty-mod-then-prod-var.rs \
  cfg-test-char-brace-then-prod-var.rs \
  env-var-os.rs \
  env-vars.rs \
  env-vars-os.rs \
  std-env-var.rs
do
  if [[ ! -f "${FAIL_DIR}/${required}" ]]; then
    echo "error: self-test: missing negative fixture ${FAIL_DIR#"$ROOT"/}/${required}" >&2
    selftest_failed=1
  fi
done
for required_pass in \
  inside-cfg-test.rs \
  cfg-test-doc-semicolon.rs
do
  if [[ ! -f "${PASS_DIR}/${required_pass}" ]]; then
    echo "error: self-test: missing positive fixture ${PASS_DIR#"$ROOT"/}/${required_pass}" >&2
    selftest_failed=1
  fi
done

n_fail=0
for f in "${FAIL_DIR}"/*.rs; do
  [[ -f "$f" ]] || continue
  n_fail=$((n_fail + 1))
  hits="$(scan_files "$f")"
  if [[ -z "${hits}" ]]; then
    echo "error: self-test: expected env-read hit in ${f#"$ROOT"/}" >&2
    selftest_failed=1
  fi
done

n_pass=0
for f in "${PASS_DIR}"/*.rs; do
  [[ -f "$f" ]] || continue
  n_pass=$((n_pass + 1))
  hits="$(scan_files "$f")"
  if [[ -n "${hits}" ]]; then
    echo "error: self-test: unexpected env-read hit in ${f#"$ROOT"/}:" >&2
    echo "${hits}" >&2
    selftest_failed=1
  fi
done

if [[ "${n_fail}" -lt 7 ]]; then
  echo "error: self-test: need >=7 negative fixtures in ${FAIL_DIR#"$ROOT"/} (found ${n_fail})" >&2
  selftest_failed=1
fi
if [[ "${n_pass}" -lt 2 ]]; then
  echo "error: self-test: need >=2 positive fixtures in ${PASS_DIR#"$ROOT"/} (found ${n_pass})" >&2
  selftest_failed=1
fi

if [[ "${selftest_failed}" -ne 0 ]]; then
  exit 1
fi

# ── Production scan ────────────────────────────────────────────────────────
# Same file set as before S0a-B-02: Rust sources under services/ and
# crates/bootstrap/, minus integration tests/ trees (CC-45b).
files=()
while IFS= read -r -d '' f; do
  files+=("$f")
done < <(find services crates/bootstrap -name '*.rs' ! -path '*/tests/*' -print0 2>/dev/null)

hits=""
if [[ ${#files[@]} -gt 0 ]]; then
  hits="$(scan_files "${files[@]}")"
fi

if [[ -n "${hits}" ]]; then
  echo "error: env::var / var_os / vars / vars_os found outside crates/config (CC-09/3):" >&2
  echo "${hits}" >&2
  echo >&2
  echo "hint: route configuration through cc_config::load; RUST_LOG/LOG_FORMAT" >&2
  echo "      are resolved inside crates/config (D-2)." >&2
  exit 1
fi

echo "ok: no env::var / var_os / vars / vars_os in services/ or crates/bootstrap/"
