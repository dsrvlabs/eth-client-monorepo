# ADR-11 — Spec-vector fetch re-hashes by default; `--verify` is accepted for contract compatibility

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 0 (workspace-wide)
- **Issues:** S1-B-07, CC-06a
- **Citations:** `scripts/fetch-spec-vectors.sh:29-32,54-55,257-284`
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-11**. It records the live fetch harness. It does not change
the lockfile or the cache layout.

## Context

`scripts/fetch-spec-vectors.sh` downloads the four consensus-specs release
artifacts pinned in `spec-vectors.lock`, checks each SHA-256 against the
lockfile, and unpacks them into a cache outside the build tree. Cargo
never invokes it; a missing cache fails with the literal string
`run scripts/fetch-spec-vectors.sh` (CC-06/4).

A fetch that trusts a cached tarball on name alone can serve a swapped
blob. A fetch that only hashes when the operator passes a flag will be
run without the flag. The script's contract comment already says the
default path re-hashes all four, and `--verify` is accepted so older
callers do not break (`fetch-spec-vectors.sh:29-32,54-55`).

## Decision

**Every run re-hashes all four artifacts against `spec-vectors.lock`.**
`--verify` is accepted for contract compatibility. It does not change
the hash path. `: "${VERIFY}"` keeps the flag live so a future tightening
can still see it; the default already does the work.

`--force` is the only flag that changes behaviour: it re-downloads every
artifact. After download or cache hit, `hash_file` runs unconditionally
and a digest mismatch deletes the bad cache file and fails the script
(`:257-284`). Placeholder lockfile digests (`PLACEHOLDER`,
`<computed at first fetch>`) also fail; they are not a first-run
success path.

This script is never invoked by `cargo build` or `cargo nextest`.

## Consequences

What this makes easy:

- A corrupted or substituted cache cannot silently feed `cc-spec-tests`.
- Existing `--verify` invocations stay valid.

What this makes hard:

- A "just unpack, I already hashed last week" fast path does not exist.
  Four artifacts are ~1.74 GB; every run pays the hash.

What this forbids:

- A default path that skips the digest check.
- Treating `--verify` as the only way to get a hash.
- Implicit download from a Rust build (CC-06/4).

## Alternatives considered

**Hash only under `--verify`.** Rejected by the live script: the flag is
accepted, the default already re-hashes. Making the default skip would
re-open the swapped-blob window the contract exists to close.

**None further recorded.** Re-derived.

## Refactor impact

**Survives.** The lockfile / cache contract is independent of the S1–S5
fold. A later fetch rewrite must keep default re-hash or supersede this
file.
