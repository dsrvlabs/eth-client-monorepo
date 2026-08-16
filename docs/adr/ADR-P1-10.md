# ADR-P1-10 — Import dedup probes the supplied root; the server recomputes on miss

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1
- **Issues:** S1-B-08, CC-27c
- **Citations:** 3 sites — `services/chain/src/import.rs:1,11-14,302-331`; `docs/contracts.md:125`
- **Provenance:** re-derived from code (2026-08-16)

## Context

`ImportBlockRequest.root` is a pre-computed `hash_tree_root`. Decoding a
mainnet `SignedBeaconBlock` to discover a duplicate is wasted work on
the core thread and is a cheap way to burn the import lane. Trusting the
supplied root as identity is a substitution attack: a caller could claim
a known root and smuggle a different body.

SEC-4 also needs a *partial* (header present, proto-array absent) to
fall through so `on_block` can resume — a hit that is not fully imported
must not return `DUPLICATE`.

## Decision

The supplied `root` is a **decode-free dedup probe only**
(`import.rs:11-14,302`; `docs/contracts.md:125`).

1. Parse `request.root`. If that root is **fully imported**
   (`store.blocks` **and** proto-array), return `DUPLICATE` without
   decoding or re-running transition (`import.rs:303-317`).
2. Otherwise decode SSZ and recompute `hash_tree_root` of
   `signed.message`. On mismatch, `INVALID_ARGUMENT` — not a verdict
   (`import.rs:324-331`).
3. A partial (header without proto-array) misses the probe and falls
   through so `on_block` can resume.

The server never treats the client-supplied root as the block's
identity.

## Consequences

What this makes easy:

- Duplicate gossip / RPC retries exit before SSZ decode.
- A lying `root` cannot overwrite or short-circuit a different body.

What this makes hard:

- Every first-seen import pays a true `tree_hash_root` even when the
  client already computed one.

What this forbids:

- Using `request.root` as the store key without recomputing on miss.
- Returning `DUPLICATE` for a header-only partial.
- Skipping the true-root check because "the client is trusted" (API,
  req/resp, or restore).

## Alternatives considered

**Always decode, then dedup.** Rejected: the probe exists so the core
thread does not SSZ-decode a block it already has.

**Trust `request.root` as identity.** Rejected: SEC-4 / the mismatch
status exist specifically so a supplied root cannot substitute a body.

None further recorded.

## Refactor impact

**Survives.** In-process `ChainIngress::submit_gossip` still carries a
probe root and still recomputes on miss. S2 seed-from-durable is not
an exemption.
