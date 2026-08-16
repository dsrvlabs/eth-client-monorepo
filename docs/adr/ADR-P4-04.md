# ADR-P4-04 — Single writer task and three-class priority mailbox

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-44b
- **Citations:** 5 sites — `services/storage/src/writer.rs:1`; `services/storage/src/prune/chunk.rs:8`; `plan/architecture.md:448,962,985`
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing.** `[ARCH]` §4.3's post-move overflow policy
**is** this mailbox. S2 changes who feeds it (typed `ArchiveWrite` call
instead of `SubscribeEvents`); it does not change the classes, bounds, or
on-full behaviour.

## Context

One `Engine` may have only one writer (`ADR-P4-01`). Write-behind commits
(P0), meta that must ride a commit (P1: window, split, migration), and
background chunks (P2: prune / backfill / snapshot) compete for that
writer. A single FIFO would let a prune pass hold a slot's commit past
the 4 s `commit_max_latency`. A second writer would break key-collision
and cursor-in-the-same-batch discipline. The live mailbox
(`services/storage/src/writer.rs`) is the whole policy:

```
P0  bound 32  on full: block (never drop)   write-behind commit units
P1  bound 64  on full: block                meta that must commit
P2  bound 256 on full: drop newest + count  prune / backfill / snapshot
```

The writer is the sole owner of `Engine`'s write side. It picks the
highest non-empty class, builds **one** `Batch`, commits, yields. A panic
is process-fatal: a dead writer that continues to serve is worse than a
compose restart. P2 submitters chunk and `yield_now()` so a pass cannot
hold the writer regardless of thread priority (`prune/chunk.rs:8`).

## Decision

Keep **one writer task** and the **three-class priority mailbox**. P0 and
P1 block when full; P2 drops newest and increments
`cc_storage_writer_chunk_dropped`. Defaults stay 32 / 64 / 256
(`config/storage.toml:65-71`). Cursor and data of a P0 unit land in the
same batch. After S2, overflow on this mailbox surfaces as
`SeamError::Backpressure` to the import path (policy A). Do not add a
second writer. Do not drop P0 or P1.

## Consequences

What this makes easy:

- Slot commits cannot be dropped under load; background work can.
- S2's backpressure story has a named object: this mailbox, not a new
  queue invented at fold time.
- Migration (CC-41) can require P1-committed so a split does not advance
  on a dropped chunk.

What this makes hard:

- A slow P0 consumer blocks write-behind (and, after S2, block import).
  That is the intended trade for a node whose archive is its own
  (`[ARCH]` §4.3 / ADR-R-02).
- P2 is best-effort. Prune / snapshot / backfill must be idempotent
  across a dropped chunk.

What this forbids:

- A second production writer, or a production path calling
  `Engine::commit` beside the writer. Tests / write-path-disabled still
  use `engine.commit` when `writer` is `None` (`serve.rs:1382-1400`);
  that is not a second writer task.
- Drop-newest (or any drop) on P0 or P1.
- Blocking P2 on full (that would invert the class).
- Changing class bounds or on-full policy in the same PR that moves the
  transport (`[ARCH]` §9.2).
- Letting a P2 pass submit an unchunked whole-table job that holds the
  writer.

## Alternatives considered

**Single FIFO.** Rejected by the class table: prune would head-of-line
block slot commits.

**Drop-newest on P0 under backpressure.** Rejected: a dropped commit unit
is a durability hole; the loss bound is `commit_slots = 1`, not "whatever
the queue shed."

**Two writers (hot vs background).** Rejected: one `Engine` write side;
key-collision and cursor/data atomicity assume a single committer.

## Refactor impact

**Survives and is load-bearing** — `[ARCH]` §4.3. The mailbox moves with
`services/storage` into `crates/storage-core` at S2. Feed changes from
gRPC write-behind to a typed `ArchiveWrite` call; classes, bounds, and
on-full policy do not. ADR-R-02 records the *direction* change
(backpressure onto import). This file records the mailbox itself.
