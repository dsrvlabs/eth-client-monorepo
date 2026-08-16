# ADR-P3-03 — Exactly one `verify_and_notify_new_payload` call site

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-08, CC-14/1, CC-32, CC-35
- **Citations:** 2 sites in the §10.4 census — `crates/state-transition/src/block/mod.rs:49`; the sole call at `crates/state-transition/src/block/execution_payload.rs:82-85`; outbox read at `crates/fork-choice/src/on_block.rs:264-265`; trait at `crates/state-transition/src/engine_seam.rs:72-76`; `[ARCH]` §4.2b (the `block_on` trace)
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing**. It is why `[ARCH]` §4.2's `block_on` trace
has a **single terminus**. A second production call to
`verify_and_notify_new_payload` (restore, fcU-driven re-notify, a parallel
"just tell the EL" helper) contradicts this file and splits the deadline
story.

## Context

`verify_and_notify_new_payload` is the spec name for "give this payload
to the EL and hear `PayloadStatusV1`." The production impl
(`EngineApiClient`) reaches the EL through `Handle::block_on`
(`engine_client.rs:347-368`). That `block_on` is the S0/S1 deadline
surface (P0-15) and the restore-path panic terminus (`[ARCH]` §4.2b):

```text
on_block → state_transition → process_execution_payload
         → verify_and_notify_new_payload    ← only call
           → EngineApiClient.block_on       ← only terminus
```

A second call site would be a second place that can park `chain-core`,
a second place that must carry a `Duration`, and a second place that
can return `INVALID` without writing the outbox `latest_valid_hash`
that CC-35's walk needs.

Fork-choice still needs the five-value status after the transition
returns. The way to get it is **not** to call the engine again.

## Decision

**There is exactly one call to `verify_and_notify_new_payload` in the
tree:** `process_execution_payload` (`execution_payload.rs:82`).

`ExecutionEngine::verify_and_notify_new_payload` is the trait method
(`engine_seam.rs:72-76`). Trait **implementations** (production
`EngineApiClient`, test `AcceptEngine` / `RejectAllEngine`, storage
`ReplayAcceptEngine`) are not call sites. Test doubles that implement
the trait do not create a second notify.

The status leaves the transition through
`TransitionContext`'s payload-status **outbox** (`block/mod.rs:46-64`):
written at the sole call site on both the success path and the
`INVALIDATED` path so `latest_valid_hash` still travels for CC-35
(`execution_payload.rs:82-90`). `on_block` **reads the outbox**. It
does not call the engine (`on_block.rs:264-265`).

`NOT_VALIDATED` (`SYNCING` / `ACCEPTED`) completes the transition
(spec: return True). `INVALIDATED` is `BlockError::Engine(InvalidPayload)`
after the outbox write.

## Consequences

What this makes easy:

- The `block_on` / deadline / panic trace has one terminus. P0-15 and
  S0-A-27 instrument one function. ADR-R-06's restore question is
  "does restore go through this same site?" — not "which of N sites
  does restore invent?"
- Invalidation (CC-35) sees the same `PayloadStatus` the transition
  saw. No second notify, no second LVH.

What this makes hard:

- Anyone who wants to notify the EL outside `process_execution_payload`
  (eager fcU-side `newPayload`, a restore-mode shortcut, a "warm the
  EL" helper) has to contradict this file.

What this forbids:

- A second production **call** to
  `verify_and_notify_new_payload` / spec `newPayload` from
  `on_block`, restore, tick, DA, or a new helper.
- Re-notifying the EL to "refresh" optimistic status.
- Reading engine status from anywhere except the outbox after the
  sole call (or a test double that still goes through the trait).

## Alternatives considered

**Call the engine from `on_block` after a successful transition.**
Rejected: that *is* the second call site CC-14/1 forbids; the outbox
exists so `on_block` does not.

**Call from restore / replay with a different client.** Restore still
goes through `on_block` → `process_execution_payload` (ADR-R-06).
Storage's `ReplayAcceptEngine` is a DAG constraint, not a second
notify. Neither is a new call site in the transition.

**Notify from the engine service on `forkchoiceUpdated` as well.**
Rejected for this record: fcU is a different Engine API method. It
must not grow a hidden `newPayload`.

## Refactor impact

**Survives and is load-bearing.**

| Stage | What happens to this record |
|---|---|
| S0 | Deadlines wrap the one impl. Restore still uses the one call (ADR-R-06). |
| S1 | `block_on` is deleted with `engine_client.rs`. The in-process `cc-engine-api` call remains **one** call from `process_execution_payload`. §4.2's trace keeps a single terminus. |
| S2 | `seed_from_durable` must not add a second notify. If it calls the engine at all, it goes through this site. |
| S3+ | Transport retries of `newPayload` are a different ADR (P3-09). They still terminate at this one call. |
