# ADR-P3-09 — The transport never retries `newPayload`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-10, S1-A-03, CC-30a
- **Citations:** 4 sites — `services/engine/src/errors.rs:6,18`; `services/engine/src/methods/eth_syncing.rs:233`; `services/engine/src/version.rs:366`
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing.** S1's deadline design depends on a single
`newPayload` attempt inside the engine transport (`[ARCH]` §9.1 / §10.4).
`S1-A-03` moves `errors.rs` verbatim — do not "improve" it by adding a retry.

## Context

Engine API errors have a typed taxonomy and a `RetryClass`
(`services/engine/src/errors.rs`). Exactly two JSON-RPC codes are
`Transient` (`-32603`, `-32000`): they mean the EL is unwell. Every other
code is `Fatal` or `Auth`. A transport that nested its own retry on
`Transient` would issue a second `newPayload` after the first had already
consumed the per-call timeout and the attestation soft deadline. Chain
does **not** read `RetryClass`. It parks on
`cc_state_transition::EngineError::Transport`
(`crates/fork-choice/src/on_block.rs:256-259`) and the import path puts
that deferral in `pending_engine` (`services/chain/src/import.rs:497`) —
a 64-entry / 8-slot map, not a second HTTP attempt. A `-32603` becomes
`Status::unavailable` via `engine_err_to_status`'s `other` arm
(`services/engine/src/service.rs:324`) and then
`EngineError::Transport` on the gRPC client
(`services/chain/src/engine_client.rs:237`), so it *does* park; so do
`Fatal` timeouts (`engine_client.rs:17-20`). Tests pin one HTTP hit on
`-32603` (`eth_syncing.rs:233-280`) and on `-38005` (`version.rs:366-389`).

## Decision

**Never retry `newPayload` inside the engine transport.** One call, one
HTTP request, one timeout. Classify `-32603` / `-32000` as
`RetryClass::Transient` (EL-unwell at this layer). That class is not a
chain-side permit and is not the park predicate — `services/chain` never
imports it. Do not interpret `Transient` as permission to re-issue from
`services/engine` or `cc-engine-api`. Timeouts, transport resets, and
HTTP 5xx stay `Fatal` here and still park as `EngineError::Transport`
after the gRPC hop. After S1 the same no-retry rule binds the
in-process callee: one `Duration`, one attempt.

## Consequences

What this makes easy:

- A `newPayload` deadline is the timeout on that one call. S1 can put an
  explicit `Duration` on the core-thread call and know it is not multiplied
  by a hidden retry.
- EL unwellness is one `Transient` JSON-RPC class at the engine. After
  the gRPC hop it is `EngineError::Transport`, which `pending_engine`
  parks and re-drives on Online. Timeouts take the same park path.

What this makes hard:

- A flaky EL is not smoothed by the transport. Operators see the park /
  re-drive, not a silent second POST.
- Anyone adding "just retry InternalError once" has to contradict this file
  and the two tests that count hits.

What this forbids:

- A retry loop, backoff, or second `call(` for `newPayload` in
  `services/engine`, `cc-engine-api`, or the S1 in-process path.
- Treating `RetryClass::Transient` as a transport-local retry permit or
  as the chain park predicate (`EngineError::Transport` is).
- "Improving" the move of `errors.rs` at `S1-A-03` by adding retries.

## Alternatives considered

**Retry `-32603` / `-32000` once inside the transport.** Rejected in the
live comments: that nests a second attempt under the ordered-lane timeout
and makes S1's deadline arithmetic a lie. Chain's `pending_engine` is the
retry.

**Retry only on HTTP 5xx / Timeout.** Also rejected by the taxonomy: those
codes are `Fatal` at this layer so they cannot be re-issued here either.

## Refactor impact

**Survives and is load-bearing.** S1's deadline design depends on it
(`[ARCH]` §9.1). The wording moves with `errors.rs` at `S1-A-03` and stays
in force after the engine is in-process. A later stage that adds a
transport retry of `newPayload` is a defect against this record, not a
local optimisation.
