# ADR-P2-02 — One task owns `Swarm<CcBehaviour>`; the KZG verify pool gets dedicated OS threads

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-09, CC-20b, CC-24b
- **Citations:** 5 sites — `services/p2p/src/host.rs:1,87-88`; `services/p2p/src/service.rs:289,409-433,705,745`; `services/p2p/src/das/verify_pool.rs:1-3,54-60,931`
- **Provenance:** re-derived from code (2026-08-16)

## Context

Libp2p's `Swarm` is not `Sync`. Sharing it behind a lock would serialise every
dial, publish, and event poll on a mutex the swarm task already owns, and it
would put KZG (tens to hundreds of milliseconds of CPU) on the same reactor
that must drain `swarm.next()` within microseconds. Phase 2 therefore split
ownership: the swarm lives in one task, and column KZG lives on a private
thread pool. The two halves of this record are one decision — **who may touch
which hot object**.

`host.rs` is the sole owner of `Swarm<CcBehaviour>`. There is no
`Mutex<Swarm>` / `RwLock<Swarm>` in `services/p2p`. The swarm task never does
work: events are routed onto bounded channels; mutations arrive only as
`SwarmCommand`s on `cmd_rx` (`host.rs:1-5,87-90`). `service.rs:289` is the
section banner; the one-shot handoff is `SwarmTask::new` + `take`
(`service.rs:409-433`). `Mutex<Option<SwarmTask>>` there is supervisor
take-once, not shared swarm mutation.

The KZG verify pool is the other half — **accepted intent, not the live
gossip callee.** `verify_cell_kzg_proof_batch` is CPU work and must not
share tokio's blocking pool with snapshot restore, engine `block_on`, or
proto decode. The pool is constructed at start on dedicated OS threads,
`K = max(2, available_parallelism / 2)` (`verify_pool.rs:1-3,54-60,931`;
`service.rs:745-765`). Live gossip still verifies **inline** on the
validation task (`column.rs:555` `kzg.verify_column_kzg`); `kzg_tx` is
destructured into `_` (`service.rs:705`), so the bridge has no producer
(`[ARCH]` §3.5 / `S3a-B-06`). The queue lives in the pool
(`VERIFY_QUEUE_BOUND = 256`, oldest-drop); that policy is `[ARCH]` §3.5
(may become `LifoQueue`) and is **not** ADR-P2-08's attribution rule.

## Decision

**One tokio task is the sole owner of the process's `Swarm<CcBehaviour>`.**
Nothing else holds the swarm. The task polls, routes, and applies
`SwarmCommand`s. It does not verify KZG, decode SSZ beyond the cheap
pre-checks, or wait on chain import.

**The KZG verify pool runs on dedicated OS threads, not the shared tokio
blocking pool and not the swarm task.** Worker count is
`pool_worker_count() = max(2, available_parallelism / 2)`. The pool is
constructed at service start (`VerifyPool::start_with_cache`) and held alive
by the `kzg-verify-pool` bridge task.

That is the **accepted home** of column KZG. Reconnect `kzg_tx` so gossip
stops calling `verify_column_kzg` on the validation worker. Do not wrap
the swarm in a lock to "share" it. Do not treat the live inline path as
the design, or move KZG onto `spawn_blocking`.

## Consequences

What this makes easy:

- Swarm ownership is already true in production: one task, command hop,
  no `Mutex<Swarm>`.
- After `S3a-B-06` reconnects `kzg_tx`, a slow KZG batch cannot stall
  `ConnectionEstablished` / `DialFailure` (H1) or the gossip validation
  loop. That isolation is the *intent*; it is not true while gossip
  still inlines KZG.
- Reconnecting the sender does not require a new ownership model. The
  threads and the worker formula stay.
- Tests can assert "host.rs must own `Swarm<CcBehaviour>`"
  (`runtime_identity.rs`) without walking a lock graph.

What this makes hard:

- Every swarm mutation is a `SwarmCommand` hop. A new behaviour that wants
  synchronous swarm access has to become a command or it does not ship.
- Worker count is a function of host parallelism, not a config dial. Changing
  `K` is a decision against this record.

What this forbids:

- A second `Swarm<CcBehaviour>` in `cc-p2p` production paths, or a
  `Mutex`/`RwLock` around the one that exists.
- Running `verify_cell_kzg_proof_batch` on the swarm task, the single gossip
  validation worker, or tokio's shared blocking pool.
- Restructuring the pool into `spawn_blocking` while reconnecting `kzg_tx`
  (`S3a-B-06` must not "simplify" ownership).

## Alternatives considered

**`Mutex<Swarm>` / `RwLock<Swarm>` so other tasks can dial and publish
directly.** Rejected in the module docs (`host.rs:5`). It turns every
peer-manager `send().await` into a lock-order problem and puts KZG and
handshake work on the poller.

**Put KZG on tokio `spawn_blocking`.** Rejected. The shared pool is already
the restore / engine / proto-decode sink. Column verification at `cgc = 4`
is a steady CPU load; it would starve those callers and still not give a
named `K`.

**Inline KZG on the gossip validation task.** Rejected as the design.
It is the **live residual** (`column.rs:555`) because `kzg_tx` is
dropped (`service.rs:705`). The pool exists so that path dies when
`S3a-B-06` reconnects the sender — do not rebuild the pool around the
inline callee.

## Refactor impact

**Survives.** Load-bearing for S3 pool reconnect (`[ARCH]` §3.5, `S3a-B-06`):
do not rebuild the pool or move it onto the shared blocking pool. The swarm
task remains the sole `Swarm` owner after E1 becomes in-process; `P2pEgress`
is a command producer, not a second owner.
