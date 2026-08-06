# Running

This document is **append-only after creation** in `##`-level named sections.
Later issues add new sections; they must not rewrite existing ones (Plan §5).

## Spec vectors

Phase 1 (and later) consensus-spec tests need the pinned `ethereum/consensus-specs`
release artifacts on disk. Phase 0 ships the fetch harness and the consumption
contract; nothing in the Rust build or `cargo nextest` downloads them.

### Fetch

```bash
bash scripts/fetch-spec-vectors.sh
```

Optional flags: `--force` (re-download every artifact), `--verify` (accepted for
the contract; the default path already re-hashes all four).

The script reads `spec-vectors.lock` (tag + four SHA-256 digests), downloads the
four release tarballs if missing, verifies digests, and unpacks into the cache.
A digest mismatch is a hard failure — re-download, do not “fix” the lockfile
unless you intentionally bump the pin.

### Cache location (`SPEC_VECTORS_CACHE`)

| | |
|---|---|
| Env var | `SPEC_VECTORS_CACHE` (optional) |
| Default root | `$HOME/.cache/eth-consensus-spec-vectors` |
| Effective root | `${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}` |
| Tree root | `<cache root>/<tag>/tests` (`<tag>` from `spec-vectors.lock`) |
| Tarball dir | `<cache root>/<tag>/_dl/` (retained for re-verification) |

The cache lives **outside** the workspace (never under `target/`), so
`cargo clean` cannot wipe ~8–10 GB of vectors.

### Disk budget

Expect on the order of **8–10 GB** free for a full ready cache (≈1.74 GB of
tarballs under `_dl/` plus the unpacked tree). CI warms only `_dl/` (see the
`vectors` job in `.github/workflows/ci.yml`); unpacking in-job is cheap.

### Never implicit

No `build.rs`, crate, or workspace script invokes `fetch-spec-vectors.sh`.
Code that needs vectors and does not find a ready cache must fail with the
literal string:

```text
run scripts/fetch-spec-vectors.sh
```

Operators run the harness explicitly (locally or via the non-required `vectors`
CI job). Layout of the on-disk tree is recorded in `spec-vectors-layout.md`
(regenerate with `scripts/record-vector-layout.sh` after a pin bump).
