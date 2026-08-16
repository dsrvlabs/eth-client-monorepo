# ADR-P1-09 — Fork-choice `Store` is owned by value on a dedicated OS thread

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1
- **Issues:** S1-B-08, S0-A-13
- **Citations:** 5 sites in the §10.4 census — live: `services/chain/src/core.rs:1,1172-1174,1247-1263`; `services/chain/src/head.rs:1`; `services/chain/src/epoch_context.rs:1`; `services/chain/src/service.rs:310`; `crates/scheduler/src/config.rs:82`; `[ARCH]` §3.2 / §3.7; `plan/issues/s0-correctness-floor.md` S0-A-13; `plan/issues/s2-fold-storage.md` S2-A-01
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing**. Loop B's `max_workers = 1` and the S2
`chain-core` move are written against it. A later stage that puts the
`Store` on a tokio task, a blocking-pool worker, or behind
`Arc<Mutex<Store>>` shared by N workers contradicts this file.

## Context

Fork-choice `Store` is mutated only through exclusive `&mut` on
`chain-core` (`on_block` / `on_tick` / `get_head` / `compute_deltas`).
Every mutation that can move the head goes through methods that bump
`mutation_counter`. Two concurrent callers on one store are a data race
or a lock that re-introduces the scheduling the thread was invented to
avoid. The type is ordinary `Sync`; exclusive `&mut` ownership is the
fact, not a `!Sync` marker.

Reads that must not wait behind a state transition (`GetHead`,
`ChainView`) cannot share that thread. They have to observe a published
snapshot.

The consensus core is therefore a different *kind* of thing from the
tonic workers that serve it. Health probes that only ask the workers
cannot see a parked core (`[ARCH]` Appendix A; `service.rs:310`).

## Decision

**The fork-choice `Store` is owned by value on one dedicated OS thread
named `chain-core`.** Spawn is `std::thread::Builder`, not
`tokio::spawn`, not `spawn_blocking` (`core.rs:1172-1174,1247-1263`):

```text
thread::Builder::new().name("chain-core").spawn(move || core_loop(store, …))
```

`store: Store<P>` moves into that closure. There is no shared lock, no
second owner, no worker pool.

**This is why Loop B `max_workers` stays 1.**
`DEFAULT_MAX_WORKERS = 1` (`crates/scheduler/src/config.rs:82`).
`[ARCH]` §3.2: introducing workers here would be a rewrite, not a
refactor. `[ARCH]` §3.7 explicitly rejects a Lighthouse-style blocking
worker pool on the chain side for the same reason.

Communication is the five-lane `Manager` (tick → import → query_p0 →
attestation → query_p1). The core thread first-match-wins and parks
until a producer notifies. `oneshot` replies. There is no mixed command
channel (`core.rs:1-19`).

**Published snapshots, not borrowed store.** `GetHead` is a pointer load
on `HeadSnapshotStore` and **never touches the core thread**
(`head.rs:1-4`, `service.rs:310`). Epoch-scoped `ChainView` fields ride
a second `ArcSwap` (`epoch_context.rs:1-9`). A health or RPC path that
starts calling into the `Store` from a tokio worker to "see fresher
head" deletes this decision.

`block-in-place` is forbidden on this thread
(`engine_client.rs:10-15`): the caller is not a runtime worker.
`Handle::block_on` from `chain-core` is the sync→async bridge, and it
exists *because* this is a plain OS thread.

## Consequences

What this makes easy:

- Fork-choice stays single-threaded by construction. Do not add a
  `Store: Sync` bound (or a dummy `!Sync` marker) to `cc-fork-choice`
  to "explain" the thread — exclusive `&mut` already does.
- `GetHead` / `ChainView` cannot stall behind `process_block`.
- Loop B can add lanes without adding workers. The selection chain is
  the policy; the worker count is not a throughput knob.

What this makes hard:

- Every store mutation waits its turn on `chain-core`. A slow engine
  call on that thread parks consensus — which is why P0-15 deadlines
  and `pending_engine` exist, not a second worker.
- A parked core is invisible to `grpc-health-probe` on the tonic port.
  `GetHead` is not the detection path. The core-liveness probe is
  `S1-A-15` / `S1-A-16` (record ADR-R-04 when that lands) — it does
  not exist yet.

What this forbids:

- Owning `Store` on a tokio task or a `spawn_blocking` pool worker.
- `max_workers > 1` on Loop B, or a chain-side blocking worker pool.
- `Arc<Mutex<Store>>` / `RwLock<Store>` shared across tasks so "workers
  can import in parallel."
- Serving public `GetHead` / `ChainView` by queueing behind the core
  (or by locking the live `Store`) instead of the published snapshot.
  Per-root `IsOptimistic` is a `query_p0` proto-array read
  (`service.rs:475-499`, `[ARCH]` §3.2) and **must stay that way** —
  the snapshot's `is_optimistic` is node-level only and would drop
  `known=false`. Do not "obey" this file by deleting that lookup.
- Carrying this `Store` across an S2 move onto a different thread
  model. `S2-A-01` must carry this ADR **intact**.

## Alternatives considered

**Tokio task owning the store.** Rejected: `blocking_recv` / engine
`block_on` on a runtime worker panics or starves the runtime. The
citing spawn comment names this alternative and refuses it.

**`spawn_blocking` pool, `max_workers > 1`.** Rejected: one `Store`,
one owner. A pool requires a lock or a sharded store. That is a
rewrite of fork-choice, not a scheduler parameter. `[ARCH]` §3.7.

**Share the store under a mutex and let query tasks read it.** Rejected:
`GetHead` would take the import lock. The snapshot/`ArcSwap` path
exists so it does not.

**Keep the store on the tonic worker that handled `ImportBlock`.**
Rejected: import, tick, and attestation would race; `GetHead` would
depend on which worker last ran.

## Refactor impact

**Survives and is load-bearing.**

| Stage | What happens to this record |
|---|---|
| S0 | Loop B lands with `max_workers = 1` *because of this*. |
| S1 | Engine fold: the call becomes in-process but still runs on `chain-core`. Do not move the store onto the runtime to make `block_on` go away. |
| S2 | `S2-A-01` extracts `crates/chain-core`. The `Store` remains owned by value on the dedicated OS thread. |
| S3+ | Scheduler work-types may grow. Worker count on Loop B does not. |
