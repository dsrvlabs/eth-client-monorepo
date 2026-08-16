# ADR-P3-15 — Production fastpath skips `verify_cell_kzg_proof_batch`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3 (in force now; S1 fold does not replace it)
- **Issues:** S1-B-13, S1-A-05, S1-A-06, CC-38b
- **Citations:** 8 census sites (live after `S1-A-05`) — `crates/engine-api/src/fastpath/filter.rs:347`; `cells.rs:19-25,115-118,194-200,425`; `sidecars.rs:428`. `[ARCH]` §10.4 still names the pre-move `services/engine/src/fastpath/filter.rs:346`.
- **Provenance:** re-derived from code (2026-08-16); S1 in-process trust argument decided here

This is **ADR-P3-15**. `[ARCH]` §10.4 class **(b)**: `verify_cell_kzg_proof_batch`
runs **only under `cfg(test)`** in the fastpath — the `trusted_local` KZG-skip
that [AS] §1 names. S1 makes the caller the process, which changes the
inject-hop trust argument but **does not automatically make skipping
correct**. This file records the live rule and that decision.

`[ARCH]` §10.4 routes a replacement to *"ADR-R-05"*. That id is already the
slashing-protection record (`ADR-R-05` / `S0-B-14`, `[ARCH]` §10.5). **X-6:**
the successor id is **ADR-R-07**. This issue does not replace the skip, so
`docs/adr/ADR-R-07.md` is **not** written.

## Context

The EL path (`engine_getBlobsV2`) returns blobs plus exactly
`CELLS_PER_EXT_BLOB` cell **proofs** per blob. The CL computes the **cells**
with `CellKzg::compute_cells` and zips them index-wise with those proofs
(`cells.rs`). `fulu/p2p-interface.md` says that when clients retrieve blobs
from the **local execution layer**, they **SHOULD skip verification** of
those blobs. The live production path follows that SHOULD: it never calls
`verify_cell_kzg_proof_batch`. Tests still run the batch over every assembled
sidecar (`sidecars.rs:428`; `filter.rs:345-367` locks the production region).

What still runs in production is a **cheap integrity bind**, not the batch
verify (`cells.rs:115-118,153-165,194-200`):

- `blob_to_kzg_commitment` → versioned hash must equal the request hash
- that commitment must equal the sidecar-template commitment

That bind is security F1. It is not `verify_cell_kzg_proof_batch`. ADR-P3-12
is the blocking-pool `compute_cells` rule; this file is the skip.

A **second** surface shares the `trusted_local` name and is not this rule.
`InjectColumns.trusted_local` is a client-asserted bool on an unauthenticated
EngineStream (`p2p.proto:231-241`). Engine always sets it true
(`services/engine/src/inject.rs:562-590`) so the trust assumption is explicit
on the wire. p2p **must not** skip KZG solely on that flag (S-38a-1 /
CC-38b): `should_skip_kzg` requires `AuthMode::Authenticated` as well, and
production default is `Unauthenticated` — inject **re-verifies**. Inclusion
multiproof is never skipped. Gossip / sampling is ADR-P2-08 and is untrusted.

S1 folds the engine *bridge* into the consensus process. The EL (geth)
stays a separate process. `S1-A-06` deletes EngineStream's engine half and
the wire bool — the caller of inject is then the process. That deletes a
**bypass surface** ([AS] §1). It does not change who produced the blobs,
and it does not decide this skip.

## Decision

**Keep the production skip.** `verify_cell_kzg_proof_batch` runs only under
`cfg(test)` in the fastpath. Production does not call it.

**S1 putting the caller in-process is not why this skip is correct.**
In-process is neither necessary nor sufficient:

- The trust root is the **local EL** named by the spec SHOULD, plus the
  cheap bind. That was true when engine was a process and is true when
  engine is a crate. The fold does not add a new reason to skip.
- In-process would equally **fail** to justify the skip if the blobs had
  not come from that local EL (remote `engine_getBlobs`, reconstructed
  columns, gossip). Those paths still verify.
- In-process does **not** license expanding the skip onto inject, gossip,
  or sampling "because the hop is gone."

The wire bool and this skip are not the same rule. `S1-A-06` may delete
`trusted_local` on `InjectColumns`. It **must not** add production
`verify_cell_kzg_proof_batch` on the fastpath, and it **must not** start
skipping inject KZG solely because the caller is now the process. Either
change is a replacement and needs **ADR-R-07**.

Do not write ADR-R-07 in this issue. Do not implement `S1-B-12`. No JWT
change.

## Consequences

What this makes easy:

- The attestation-critical assemble path stays off the ~2.3 ms Phase-2
  batch verify (`docs/kzg-benchmark.md`) for blobs this node just pulled
  from its own EL.
- Tests remain the venue that proves EL proofs zip to valid cells.
- `S1-A-06` can delete the unauthenticated wire hint without silently
  rewriting this rule.
- Reviewers have a committed sentence that "we are in-process now" is
  not a KZG-skip argument.

What this makes hard:

- A buggy or malicious local EL can hand the fastpath invalid cell
  proofs. Peers reject them on gossip. Local inject still re-verifies
  until a recorded replacement says otherwise. The cheap bind still
  rejects a blob that does not match the request / template.
- Anyone who wants production verify on this path, or an inject skip
  keyed on "same process," has to contradict this file and write
  ADR-R-07.

What this forbids:

- Calling `verify_cell_kzg_proof_batch` from production fastpath
  (`cells.rs`, `sidecars.rs`, `filter.rs`, `mod.rs`, `fetch.rs`) while
  this record is in force.
- Deleting the skip in `S1-A-06` or S3 as a side effect of removing
  EngineStream / `trusted_local`.
- Skipping gossip / sampling KZG, or the ADR-P2-08 per-sidecar fallback,
  because the sidecar was "local."
- Skipping inject KZG solely on `trusted_local`, or solely because
  engine and p2p now share a process.
- Treating `[ARCH]` §10.4's *"ADR-R-05"* as this skip's successor.
  ADR-R-05 is slashing protection. The successor id is ADR-R-07.

## Alternatives considered

**Enable production `verify_cell_kzg_proof_batch` because the caller is
now the process.** Rejected. Wrong trust root. The fold does not change
who produced the blobs. Paying the batch on the attestation path to
re-prove an unchanged EL SHOULD is a replacement, not a mechanical
consequence of S1.

**Treat in-process as automatically licensing the skip (or a wider
skip).** Rejected. That is the argument `[ARCH]` §10.4 says is not
automatic. The skip stands on the local-EL SHOULD and the cheap bind.

**Write ADR-R-07 now and mark this id superseded.** Rejected. The rule
is not replaced. Superseding a live skip with a file that does not
exist yet would make the table lie. Write ADR-R-07 in the same change
that introduces production verify or that rewrites the inject skip
after the bool dies.

**Keep the wire `trusted_local` bool after S1 "so the skip has a
flag."** Rejected. The bool is a client-asserted hint on a hop that
S1-A-06 deletes. This skip does not key on that field; the field's
residual is S-38a-1.

## Refactor impact

**Survives.** S1-A-06 deletes the wire bool, not this skip. S3 must not
silently delete it when EngineStream dies. A replacement is ADR-R-07.

| Stage | What happens to this record |
|---|---|
| S1-B-13 | This file. No production code change. X-6: successor id is ADR-R-07. |
| S1-A-05 | Fastpath lives in `cc-engine-api`. Skip and the `cfg(test)` lock moved with the files. **Landed.** |
| S1-A-06 | Delete `InjectColumns.trusted_local` and EngineStream's engine half. **Do not** add production verify. **Do not** skip inject KZG "because in-process." |
| S3 | EngineStream / `P2pEgress` reshape. Fastpath skip stays. Gossip / sampling still verify (ADR-P2-08). Replacement, if any, is ADR-R-07. |
| S3+ | A new production `verify_cell_kzg_proof_batch` on this path, or an inject skip keyed only on process topology, is a defect against this record until ADR-R-07 accepts it. |
