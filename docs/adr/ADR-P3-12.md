# ADR-P3-12 — Cell extension runs on the blocking pool

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-10, S1-A-05, CC-37b
- **Citations:** 3 sites — `services/engine/src/fastpath/cells.rs:1,7,167`
- **Provenance:** re-derived from code (2026-08-16)

## Context

`engine_getBlobsV2` returns blobs plus exactly `CELLS_PER_EXT_BLOB` (128)
cell **proofs** per blob. The CL computes the **cells** itself with
`CellKzg::compute_cells` and zips them index-wise with the EL proofs
(`services/engine/src/fastpath/cells.rs`). Up to 21 × 128 = 2 688 cell
computations is head-of-line blocking on a tokio worker. The ordered lane
(attestation deadline) must stay free. This file is also the sole
production `compute_cells` call site in `services/engine`; proofs are never
recomputed on this path (OQ-P3-3 / R-14). Production still skips cell-proof
batch verification here — that skip is `ADR-P3-15` / `S1-B-13`, not this
record.

## Decision

Run every production `compute_cells` inside
`tokio::task::spawn_blocking`. Never on a tokio worker. Callers outside
tests go through `compute_cells_zipped_with_el_proofs`, which is the
`spawn_blocking` wrapper. Bind each blob to the request versioned hash and
the sidecar-template commitment *before* extension; that bind is not the
P3-15 batch verify.

## Consequences

What this makes easy:

- Cell extension cannot stall the ordered lane or the attestation deadline.
- There is one production call site, so a grep for `compute_cells` outside
  the blocking closure is a review finding.

What this makes hard:

- The fastpath pays a pool hop and a `Join` error class. Tests that want
  the sync helper must stay under `#[cfg(test)]`.

What this forbids:

- Calling `compute_cells` on a tokio worker in production.
- Recomputing EL-supplied proofs on this path.
- Adding a second production `compute_cells` site to skip the pool.

## Alternatives considered

**Run extension on the worker ("it is only a few milliseconds").** Rejected
in the module docs: 2 688 cell computations is head-of-line blocking
material.

**Recompute proofs locally and drop the EL proofs.** Rejected (OQ-P3-3):
the EL already supplied them; R-14 does not fire.

## Refactor impact

**Survives; moves to `cc-engine-api` at S1** (`S1-A-05` moves `fastpath/`).
The blocking-pool requirement moves with the file. Do not "simplify" the
move by inlining `compute_cells` onto the worker.
