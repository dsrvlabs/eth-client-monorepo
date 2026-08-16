# ADR-P2-14 — `earliest_available_slot` is one `AtomicU64` with one writer

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-09, CC-48, CC-26a
- **Citations:** 5 sites — `services/p2p/src/backfill/window.rs:1,81`; `services/p2p/src/backfill/mod.rs:6`; `services/p2p/src/reqresp/blocks.rs:15`; `services/p2p/src/reqresp/columns.rs:18` (writer evidence, not a tagged site: `services/p2p/src/storage_client.rs:10-17,184-196`; Status reader: `services/p2p/src/reqresp/status.rs:6`)
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing**. It is the shape that **replaces E6**
(`[ARCH]` §2.3 / §10.4). S2 deletes `WatchServeWindow` as a transport; S3
publishes the window (`S3a-B-08` / `S3a-B-09`, [PRD] P0-17c). Neither
stage may grow a second number or a second writer.

## Context

`Status v2` advertises `earliest_available_slot`. By-range / by-root
handlers refuse below it. If that number is recomputed at each reader —
or written from cache insert, cache eviction, **and** a storage stream —
peers see a window that flaps, and a node can advertise a slot it cannot
serve (free-riding; CC-26a F2).

Phase 2 collapsed the advertised value to **one `AtomicU64`**
(`ServeWindow::earliest_available_slot`) with **one production writer**.
Construction seeds `u64::MAX` (`EMPTY_WINDOW_SLOT`), not `anchor`.
Advertising `anchor` with an empty cache is free-riding (`window.rs:15-17,98-106`).

The single write site after construction is `ServeWindow::store_recomputed`,
`pub(crate)`, called only from the `WatchServeWindow` handler
(`apply_stream_window`) and the §5.5 fail-closed collapse to the in-memory
cache floor when the stream is stale (`storage_client.rs:10-17,184-196`).
Cache eviction / insert **must not** write the atomic — Phase 2's
eviction-path write is deleted (`window.rs:4-6,81-87,134-139`). The
production body of `cache.rs` has no `store_recomputed` (the
`#[cfg(test)]` seeder is not a production writer).

Readers — `Status v2` and every block / column serve handler — **only
load** (`status.rs:6`, `handshake.rs:91-101`, `blocks.rs:14-15`,
`columns.rs:17-18`). They never recompute. Pure
`compute_earliest_available_slot` remains for the cache-floor used by
collapse, not for the advertised value (`window.rs:8-12,47-76`).

**Live advertisement is not storage's two-branch derivation.** E6
(`WatchServeWindow` + `GetBlocks*` / `GetColumns*`) is **DEAD** —
storage never publishes; the stream seed is `u64::MAX` (`[ARCH]` §2.3,
`serve.rs` `empty_window`). So the advertised word stays the construction
seed (`u64::MAX`) until either (1) a stream message arrives — none do
today — or (2) `window_stale_grace` expires and collapse writes the
in-memory `cache_floor` (itself seeded `u64::MAX`, then updated by the
backfill cache). `window.rs:11` *intends* CC-49's two-branch formula to
feed this atomic; that is the S3 publish contract (`S3a-B-09`), not the
live Status value.

After S2 the gRPC edge is gone and the window is still one `AtomicU64`.
After S3 the published value must equal
`cc_storage_earliest_available_slot`.

## Decision

**There is one `AtomicU64` named `earliest_available_slot`, and it has
one writer.**

- Seed `u64::MAX` at construction. That is an empty window, not "from
  genesis" and not `anchor`.
- Production `.store` goes only through `ServeWindow::store_recomputed`.
  Production callers: the serve-window stream handler, and the stale-stream
  collapse in the same module. Cache insert / eviction are not writers.
- Status and every block / column serve handler **read** the atomic.
  They do not recompute, do not clamp locally, and do not keep a shadow
  copy.
- `compute_earliest_available_slot` is the cache-floor helper for
  collapse, not a second advertised window.

When E6's transport is deleted at S2, keep this atomic. The mechanism
*is* the load. Do not replace it with a channel, a gRPC watch, or a
per-handler recompute.

## Consequences

What this makes easy:

- `Status v2` and the serve handlers cannot disagree: they load the same
  word.
- S2's E6 deletion is a transport deletion, not a window redesign.
- S3 acceptance is a numeric equality
  (`cc_storage_earliest_available_slot == cc_p2p_*`).
- Tests can grep the **production body** of `cache.rs` (split on
  `#[cfg(test)]`): no `store_recomputed`. A naive
  `rg store_recomputed cache.rs` also hits the test seeder.

What this makes hard:

- A cache hit does not widen the advertised window. Live honesty is
  "empty (`u64::MAX`) or, after stale-stream collapse, the cache floor"
  — not "we have the bytes" and not CC-49's two-branch derivation.
  Two-branch is what S3 must publish *into* this atomic.
- Disconnect does not immediately rewrite the window: the last value is
  held for `window_stale_grace`, then collapsed to the cache floor
  (`storage_client.rs:14-17`). Collapse is still the same writer.

What this forbids:

- A second `AtomicU64` (or `AtomicU64` + `Mutex<Slot>`, or a
  `watch::Sender`) for the advertised window.
- Writing the advertised value from cache eviction, cache insert, or a
  serve handler.
- Recomputing `earliest_available_slot` in `reqresp/blocks.rs` or
  `reqresp/columns.rs`.
- Seeding construction with `anchor` (or genesis, or 0).
- Re-introducing E6 as a live watch after S2 "because p2p needs a
  stream". p2p needs a load.

## Alternatives considered

**Each serve handler recomputes from the cache.** Rejected in the
handler module docs (`blocks.rs:14-15`, `columns.rs:17-18`). It races
eviction and cannot see storage's durable floor.

**Cache eviction writes the atomic (Phase 2's deleted path).** Rejected
(CC-48 /5). Eviction is a memory decision; advertisement is an honesty
decision. Mixing them advertises a hole as a floor.

**Seed `anchor` so Status looks live before the first recompute.**
Rejected (CC-26a F2). An empty cache cannot serve `anchor`.

**Keep `WatchServeWindow` as the long-term API and treat the atomic as
a cache of the stream.** Rejected for post-S2. E6 is deleted; the
atomic *is* the API (`[ARCH]` §2.3). A stream can feed the atomic until
S2; it is not a second source of truth.

## Refactor impact

**Survives and replaces E6** (`[ARCH]` §2.3, §10.4).

| Stage | What happens to this record |
|---|---|
| S1 | This file. No production code change. |
| S2 | Delete E6 (`WatchServeWindow` RPC + the Get* hop as p2p's window source). The advertised value remains this `AtomicU64`. Storage writes it in-process; p2p (or `beacon-core`) loads it. |
| S3 (`S3a-B-08`, `S3a-B-09`) | Publish the window. Serve answers data at/above `eas`; `ResourceUnavailable` only below. Metrics: storage and p2p advertisements match. |
| S3+ | Do not grow a second window number when backfill or prune moves the floor. They call the same writer. |
