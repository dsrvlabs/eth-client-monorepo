# ADR-R-06 — Does the restore/replay path call the execution engine at all?

- **Status:** proposed · revisit at S2 · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 0 (recorded at S0; the gRPC path is deleted at S2)
- **Issues:** S0-A-29, S0-A-28, S2-J-01, S2-J-02
- **Citations:** `plan/architecture.md` §4.2b / §9.1 / §10.5 / Q-2c; `plan/issues/s0-correctness-floor.md` S0-A-28 / S0-A-29; `plan/issues/s2-fold-storage.md` S2-J-01 / S2-J-02; `services/chain/src/restore.rs:9-11,418-456,528-550,812-822`; `services/storage/src/replay.rs:60-75,526-527`; `services/chain/src/engine_client.rs:17-21,112-119`; `crates/fork-choice/src/on_block.rs:243-261`; `crates/fork-choice/src/da_seam.rs:265-278`; `crates/fork-choice/src/execution_status.rs:54-68`; `crates/state-transition/src/block/execution_payload.rs:82-90`; `crates/state-transition/src/engine_seam.rs:82-85`
- **Provenance:** new — closes the `[ARCH]` §10.5 stub and Q-2c *as a recorded deferral*, not as a consensus ruling

This is **ADR-R-06**. `[ARCH]` §10.5 left it `proposed, undecided` with the instruction to
decide before S0 closes **or** record the deferral. This file is the deferral. It is not
a silent one.

## Context

Two privileged skips on the chain restore path are already written down and are **not**
this decision. `RestoreFromStore` replays with `BlockSignatureStrategy::NoVerification`
and applies stored `da_status` as a verdict (`restore.rs:9-11`, `:418-429`, `:528-533`).
Those are CC-45 properties. Engine notification is **not** on that list.

Production `apply_restore_set` nevertheless constructs a real
`EngineApiClient` (`restore.rs:454-456`) and feeds it to `get_forkchoice_store`. Every
replayed block then goes through `on_block` → `state_transition` →
`process_execution_payload` → `verify_and_notify_new_payload` (`on_block.rs:250-254`,
`execution_payload.rs:82`). That is the sole engine call site in the tree (CC-14/1). A
`Valid` / `NOT_VALIDATED` status is recorded on the block; `INVALIDATED` is a transition
error; a transport failure is `Deferred(ExecutionEngineUnavailable)`
(`on_block.rs:256-259`, `da_seam.rs:272-278`). Restore accepts only `Imported` or a
*DA* deferral that matches stored `DEFERRED`. Every other `BlockImport` — including
engine-unavailable — is `Status::internal` and fails the stream (`restore.rs:547-550`).

Every test of this path substitutes a local always-`Valid` `AcceptEngine`
(`restore.rs:812-822`). The Phase-1 production always-Valid stub was deleted at CC-32b
and is not a library export (`engine_seam.rs:82-85`). So the production semantics of
"replay a block whose payload was already validated at first import" have never been
exercised, and the test double may in fact encode the *correct* semantics. That is why
`[ARCH]` §10.5 called this a consensus decision rather than an implementation choice.

A second replay path already refuses to call the EL: storage's snapshot producer uses
`ReplayAcceptEngine` (`replay.rs:60-75,526-527`). The comment there is a **crate-DAG**
reason — storage must not open a second EL client — not a ruling that previously-accepted
payloads are Valid forever. Do not read it as a precedent for the chain restore path.

The mechanical panic on this path is a separate item and is **already closed**.
**S0-A-28** shipped `tokio::task::spawn_blocking` around `apply_restore_set` (and
covers the lazy connect). Option 1 therefore no longer panics on the tonic worker:
`Handle::block_on` runs on a blocking-pool thread (`EnterRuntime` stays
`NotEntered`). This ADR does not block that fix and is not a reason to revert it.

S0-A-27 put an explicit `Duration` on every chain→engine `block_on`
(`engine_client.rs:17-21,112-119`). Combined with the restore match above, engine
outcomes are not all "failed restore":

- `VALID` → `Imported` + `ExecutionStatus::Valid` → restore **accepts**
- `SYNCING` / `ACCEPTED` → `Imported` + `Optimistic` (`NOT_VALIDATED`,
  `execution_status.rs:54-68`) → restore **accepts**
- `INVALID` / `INVALID_BLOCK_HASH` → `Err(Transition(Engine(InvalidPayload)))` →
  restore **fails**
- `Transport` (deadline, connect, hang→timeout) →
  `Deferred(ExecutionEngineUnavailable)` → restore **fails** (`restore.rs:547-550`)

A still-syncing EL that answers `SYNCING`/`ACCEPTED` is already the safe half of
option 3. Only an unreachable or timed-out engine fails restore. That coupling
is a consequence of keeping the call, not an argument for deleting it as
convenience.

S2 deletes the surface. `S2-J-02` removes `RestoreFromStore` and `restore.rs`;
`S2-J-01` replaces `apply_restore_set` with in-process `chain_core::seed_from_durable`.
The gRPC path this ADR names then no longer exists. `seed_from_durable` is the
successor that will have to answer the same question, or a tighter one.

The argument this record is **not** positioned to make, and that Q-2c named, is
whether an EL that has *itself* been restored from a different snapshot could
legitimately disagree about a payload this node previously accepted.

## Decision

**Do not give `EngineApiClient` a restore mode at S0.** Do not install `AcceptEngine`
(or any always-`Valid` double) on the production restore path. Production
`apply_restore_set` keeps constructing a real client and keeps calling
`verify_and_notify_new_payload` for every replayed execution payload.

**S0-A-28 already shipped.** `spawn_blocking` around `apply_restore_set` is the S0
mechanical fix for the `block_on` panic. Option 1 no longer panics on the tonic
worker. That wrap is not gated on this ADR, and this ADR is not a reason to revert it.

**Revisit at S2.** The consensus question is open until `S2-J-01` writes
`seed_from_durable`. That is the first moment the node seeds fork-choice from durable
data without a gRPC hop, and the last moment the current `apply_restore_set` policy
can be changed without being deleted out from under the change.

**S2 default remains (1):** durable seed still calls the engine (this interim,
restated on the new boot path).

**Option 2 — skip the engine and mark payloads `Valid` without `NewPayload` —
stays `proposed` / revisit.** It is **not adopted at S0** and is not an S2 peer of
(1). Taking it requires a superseding ADR that writes Q-2c: an independently
restored EL `INVALID` on a previously accepted payload is a legitimate
disagreement, and `Valid` must not be invented on a path that also skips BLS
and DA. Do not cite the test `AcceptEngine` or storage `ReplayAcceptEngine` as
that argument.

**Option 3 — skip the call and mark `NOT_VALIDATED` / optimistic — also stays
`proposed` / revisit.** It is the only skip-shaped alternative this record will
entertain, and only with a duty gate (`MUST NOT` attest / propose when
`is_optimistic_node`). Marking Optimistic without that gate is operationally
option 2.

Until that revisit, Q-2c stays open: an independently-restored EL may or may not be
allowed to disagree about a previously-accepted payload. Anyone who wants (2) or (3)
before S2 writes a superseding ADR that makes that argument; they do not flip the
client to a test double in `restore.rs`.

## Consequences

What this makes easy:

- S0-A-28 already shipped; this record does not wait on it and is not a reason
  to revert it.
- Restore's documented privileges stay the two that are written: zero BLS, DA as
  verdict. Engine notification is not silently added to that list.
- Storage's `ReplayAcceptEngine` remains a DAG constraint on `services/storage`, not
  a house rule that "replay means Valid."
- S2 reviewers have a named question on `seed_from_durable` instead of discovering
  that restore-mode `AcceptEngine` shipped as a panic workaround.

What this makes hard:

- Until S2, a successful `RestoreFromStore` of a payload-carrying set still needs a
  reachable engine that answers inside the S0-A-27 deadlines. `SYNCING` / `ACCEPTED`
  is `Imported` + `Optimistic` and restore succeeds. Only `Transport` (timeout /
  connect / hang) becomes `Deferred(ExecutionEngineUnavailable)` and fails the
  stream — not an optimistic store.
- The production restore×engine path remains unexercised by the in-tree
  `AcceptEngine` tests. S0-A-28's acceptance (real client, payload-carrying block, no
  panic) is the only test that is allowed to stand in for it; do not "fix" that test
  by putting the double back.
- The Q-2c argument is still unwritten. S2 cannot treat this file as having closed it.

What this forbids:

- A production `EngineApiClient` "restore mode" that returns `PayloadStatus::Valid`
  without a `NewPayload` (or that never constructs the client) — **including as
  the S2 `seed_from_durable` default**. Option 2 stays `proposed` / revisit.
- Treating `restore.rs`'s test `AcceptEngine` or `replay.rs`'s `ReplayAcceptEngine`
  as the production semantics of chain restore.
- Reverting S0-A-28 because "we decided not to call the engine." That decision has
  not been made.
- Closing Q-2c by implication.

## Alternatives considered

**Option 1 — keep calling the engine; wrap `apply_restore_set` in `spawn_blocking`.**
This is the `[ARCH]` §10.5 option that "ships at S0 regardless." It is the interim
this record adopts. **S0-A-28 already shipped the wrap**, so the call no longer
panics on the tonic worker. It does **not** answer Q-2c.

**Option 2 — restore-mode `AcceptEngine` (always `Valid`, no EL call).** The
alternative `[ARCH]` §9.1 / §10.5 flagged as a consensus decision. The argument in
its favour is that these payloads were validated at first import, so re-validation
is redundant work that also makes boot depend on EL availability — and that the
test double has been encoding those semantics all along. **Not adopted at S0.
Stays `proposed` / revisit; not an S2 default.** Returning `Valid` without asking
is stronger than "do not call the engine": it tells fork-choice the payloads are
fully validated, so the node is not optimistic and will not re-check them. That
is exactly the independently-restored-EL disagreement Q-2c asked for and this
record does not have. A superseding ADR must write that argument before anyone
picks (2).

**Option 3 — skip the call and mark `NOT_VALIDATED` / optimistic.** A real third
shape that the §10.5 stub did not list. Safer than option 2 if the point is "do not
depend on the EL at boot": the node comes up, head is optimistic, live `NewPayload`
/ `forkchoiceUpdated` resolve status. Also a consensus/liveness decision (when is
the node allowed to attest / propose from a restored optimistic head?). **Not taken
at S0. Stays `proposed` / revisit**, and only with a duty gate (`MUST NOT` attest
/ propose when `is_optimistic_node`). Listed so S2 does not collapse "do not call"
into option 2 by accident.

**Treat storage `ReplayAcceptEngine` as the answer.** Rejected. That double exists
so `services/storage` does not grow an engine-service edge. Snapshot replay checks
`hash_tree_root` against the stored `state_roots[slot]`; it does not install
fork-choice execution status for a live node.

**Decide now that S2 deletion makes the question moot, and write nothing.**
Rejected. `[ARCH]` §10.5: an honest deferral is fine; a silent one is not.
`seed_from_durable` will re-ask the question on a path that is not deleted.

## Refactor impact

**Created at S0. Revisit at S2. The gRPC path is deleted at S2; the question is not.**

| Stage | What happens to this record |
|---|---|
| S0 | This file. No production code change. S0-A-28 already shipped on a different issue. |
| S1 | Untouched. S1 deletes `engine_client.rs`'s `block_on` bridge; restore still has to reach whatever replaces it until S2 removes restore. |
| S2 | `S2-J-02` deletes `restore.rs` / `RestoreFromStore`. `S2-J-01` writes `seed_from_durable`. **This ADR is revisited there** — default remains (1). (2) and (3) stay `proposed` / revisit; taking either needs a superseding ADR (Q-2c for (2); duty gate for (3)). After deletion, citations into `restore.rs` in this file are historical. |
| S3+ | No remaining restore/replay engine policy on a gRPC edge. Any leftover always-Valid double on a production boot path is a defect against this record until a superseding ADR exists. |
