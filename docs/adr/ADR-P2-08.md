# ADR-P2-08 — KZG cross-sidecar batching with per-sidecar re-verification before penalising

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-09, CC-24b
- **Citations:** 6 sites — `services/p2p/src/das/verify_pool.rs:1-12,41-42,621-625,667-674,786-827`; `services/p2p/src/das/mod.rs:6`; `services/p2p/src/lib.rs:24`; `services/p2p/src/backfill/below.rs:283-287`
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing**. It is the **accepted pool contract**
`S3a-B-06` must not rebuild when it reconnects `kzg_tx`. It is **not**
the live gossip path: gossip still inlines `kzg.verify_column_kzg` on
the validation task (`column.rs:555`) because `kzg_tx` is dropped
(`service.rs:705`). Pool `penalise_peer` is a diagnostic ring + metric
today (`verify_pool.rs:839-840`; "Real PeerPenaltyCmd is M2") — not
the production disconnect path.

## Context

A Fulu block carries up to 128 column sidecars. Each sidecar's step 3 is
`verify_cell_kzg_proof_batch` over that column's cells. The backend is
amortised across a longer vector: verifying eight same-block sidecars as
one batch is cheaper than eight calls, and the p95 sampling budget is
200 ms (`SAMPLING_P95_BUDGET_SECS`).

A cross-sidecar batch has an attribution problem the per-sidecar path does
not. `verify_cell_kzg_proof_batch` returns one `bool` (or an error) for the
whole vector. If the batch fails and the pool penalises every peer that
contributed a sidecar, one bad column from peer A also descratches peer B.
Below-anchor backfill can batch BLS because **every block in that batch
came from one peer** (`below.rs:283-287`). Gossip / sampling columns do
not have that property.

The pool therefore batches opportunistically and **falls back**. Default is
per-sidecar. When a worker dequeues ≥ `CROSS_SIDECAR_BATCH_MIN` (4)
same-block jobs in one tick, it runs structure + inclusion per sidecar,
then one cross-sidecar KZG batch. On `Ok(true)` every survivor is Valid.
On `Ok(false)` / `Err`, it increments `cross_failures` and **re-verifies
each survivor's KZG alone** before any peer is attributed
(`verify_pool.rs:9-12,621-625,786-827`). Steps 1–2 stay per-sidecar even
on the cross path — they are cheap and already attributed.

That rule lives in `VerifyPool` and is exercised only when jobs enter
the queue. Live gossip does not enqueue; the inline callee has no
cross-sidecar batch and no per-sidecar fallback. Reconnect is what
makes the contract the production path. Do not "simplify" the pool
back to Lighthouse's per-item path (`[ARCH]` §3.5), and do not treat
the inline residual as the design.

## Decision

**Once a sidecar is on the verify pool, KZG is per-sidecar by default,
and opportunistically cross-sidecar for the same block, with mandatory
per-sidecar re-verification before any peer is attributed.**

Concretely:

1. Workers pop same-block batches (`queue.pop_batch_same_block()`).
2. If `batch.len() < CROSS_SIDECAR_BATCH_MIN` (4), run the three §8.2
   steps per sidecar (`process_per_sidecar`).
3. If `batch.len() >= 4`, run structure + inclusion per sidecar; gather
   survivors; call `verify_kzg_cross_batch` once.
4. Cross-batch `Ok(true)` → all survivors Valid. Cross-batch `Ok(false)`
   or `Err` → **do not attribute yet**. Re-run step 3 per sidecar
   (`verify_kzg_one`). Attribute only the sidecar whose own KZG fails.
5. Never treat a failed cross-batch as evidence against every member.

The dedicated OS-thread pool and `K = max(2, parallelism / 2)` are
ADR-P2-02. The queue bound and oldest-drop policy live in the pool;
they are **not** this decision. `[ARCH]` §3.5 may later replace the
ad-hoc queue with `LifoQueue` **without** touching the batching /
attribution rule. Changing the queue is not a P2-08 violation.

## Consequences

What this makes easy:

- After reconnect, same-block sampling hits the backend once when ≥ 4
  columns arrive together (`cgc = 4` / `sampling_size = 8`).
- A single invalid column cannot descratch the peers who sent the other
  seven — once those jobs are on the pool.
- S3 reconnects `kzg_tx` and keeps this file as the contract. A PR that
  deletes the fallback to "save a pass" is a spec change.

**Live residual (not the decision):** gossip KZG is still one inline
`verify_column_kzg` on the validation task. The pool's attribution hook
does not yet drive peer-manager disconnect.

What this makes hard:

- The failure path does more KZG, not less. A poisoned batch of 8 pays
  1 + 8 verifies. That is the cost of attribution.
- Cross-batch membership is same-block, not same-peer. Mixing peers is
  allowed; mixing blocks is not.
- Below-anchor BLS batching must not be cited as a precedent for skipping
  the fallback (`below.rs:287` says the opposite).

What this forbids:

- Penalising every peer in a failed cross-sidecar batch.
- Shipping a "batch-only" path with no per-sidecar fallback.
- Keeping the live inline verify as the long-term path so this rule has
  no home (`[ARCH]` §3.5 / `S3a-B-06`).
- Changing `CROSS_SIDECAR_BATCH_MIN` or dropping the fallback in the same
  PR that reconnects the sender.
- Reading pool `penalise_peer` as today's production disconnect — it is
  a diagnostic ring until the real `PeerPenaltyCmd` lands.

## Alternatives considered

**Always per-sidecar, never cross-batch.** Recorded as the default when
the tick has fewer than 4 same-block jobs. Rejected as the *only* path:
it leaves the 200 ms p95 budget on the table for the common eight-column
case, and the backend is built for long vectors.

**Cross-batch failure penalises every contributor.** Rejected. Attribution
is then false. One attacker sidecar plus seven honest ones would
descratch seven honest peers.

**Cross-batch only within one peer.** Rejected as a requirement. The
fallback already attributes; restricting membership would shrink batches
without shrinking the failure-path cost.

**Skip the fallback when the batch is "trusted_local".** Not this record.
The `trusted_local` KZG-skip is ADR-P3-15 / `S1-B-13`. This pool is the
untrusted gossip / sampling path.

## Refactor impact

**Survives and is load-bearing** (`[ARCH]` §3.5, §10.4; `S3a-B-06`).

| Stage | What happens to this record |
|---|---|
| S1 | This file. No production code change. |
| S3 (`S3a-B-06`) | Reconnect `kzg_tx`. **Do not restructure the pool, drop the fallback, or move KZG onto the validation task.** |
| S3+ (`cc-scheduler`) | The ad-hoc 256 oldest-drop queue may become `LifoQueue`. Batching and the re-verify-before-penalise rule stay in `VerifyPool`. |
