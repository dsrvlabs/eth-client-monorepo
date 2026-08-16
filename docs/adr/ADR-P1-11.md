# ADR-P1-11 — `session_id` plus two distinct cursor-rejection reasons

- **Status:** proposed · revisit at S2 · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 1 (event-ring resume; storage is a consumer until S2)
- **Issues:** S1-B-16, S2-A-09
- **Citations:** 4 sites — `services/chain/src/events/cursor.rs:1,9-13,17-24`; `services/chain/src/events/mod.rs:25-30`; `proto/eth/chain/v1/chain.proto:117-125`; `docs/contracts.md:207`
- **Provenance:** re-derived from code (2026-08-16) — records the live cursor contract; S2 rewrites Consequences when storage stops consuming

This is **ADR-P1-11**. `[ARCH]` §10.4 class **(b)**: *"storage stops being a
consumer at S2 (§4.3); the ADR survives for API consumers but its consequences
section is rewritten."* This file is the R-17 placeholder. The cursor rules
stay; the durability story does not.

## Context

`SubscribeEvents` resumes from a `Cursor`: `session_id`, `seq`, `slot`,
`root` (`chain.proto:121-125`). `seq` is the resume point (replay from
`seq + 1`). `slot`/`root` identify the ring entry for idempotent consumers.
`session_id` is a **per-process random u64** so a cursor minted by a
previous incarnation cannot silently match after a restart
(`chain.proto:119-120`, `docs/contracts.md:207`).

Validation is `validate_cursor` (`cursor.rs`). Two
`FAILED_PRECONDITION` + `google.rpc.ErrorInfo` reasons
(`mod.rs:25-30`, `cursor.rs:9-13`):

| `reason` | Condition | Consumer action |
|---|---|---|
| `CURSOR_UNKNOWN_SESSION` | `cursor.session_id != ring.session_id` | `GetHead`, resubscribe with no cursor; drop incarnation assumptions |
| `CURSOR_TOO_OLD` | `cursor.seq + 1 < ring.front().seq` (evicted) | `GetHead`, resubscribe with no cursor |

**Session is rejected before too-old** (`cursor.rs:17-24`). A stale
session that would also be evicted still surfaces as
`CURSOR_UNKNOWN_SESSION`. That keeps the CC-18/2 pin meaningful: the two
reasons are distinguishable, and a restart is not reported as ring
eviction.

Today **storage is a consumer**. Write-behind attributes both reasons on
reconnect (`services/storage/src/write_behind.rs:56-58,1243-1244`) and
treats `CURSOR_TOO_OLD` as a durability event: gap-fill via
`GetCanonicalRoots` (`[ARCH]` §4.3). Ring eviction is therefore an
archive event, not just an API event.

S2 deletes that role (`S2-A-09`, §4.3). Storage gets a typed
`ArchiveWrite` ingest. The ring is demoted to API/observer
(`SubscribeEvents` for external consumers, later REST SSE at S5). Bounds
and cursor semantics stay. Policy B still applies to a slow *external*
consumer. Eviction is no longer a durability event.

## Decision

**Keep `session_id` and the two reasons. Check session before too-old.**

A cursor without a matching session is `CURSOR_UNKNOWN_SESSION`. A
matching session whose resume point has been evicted is
`CURSOR_TOO_OLD`. Do not collapse them. Do not invent a third
`FAILED_PRECONDITION` reason for "storage lagged" vs "API lagged."

**Revisit at S2.** When storage stops being a consumer (`S2-A-09`),
**rewrite Consequences** — this file survives for API consumers. After
the rewrite: eviction must not trigger durable gap-fill; write-behind
must not exist; API/SSE consumers still distinguish the two reasons.
The decision above does not change unless a superseding ADR says so.

This file stays `proposed` until that rewrite. Do not mark it `accepted`
with storage still on the bus and call the S2 work done.

## Consequences

What this makes easy **today** (storage is a consumer):

- A chain restart and a ring eviction are different operator signals and
  different reconnect metrics.
- Write-behind can attribute `ReconnectReason::CursorUnknownSession` vs
  `CursorTooOld` without parsing English.
- API clients have the same contract as the archive.

What this makes hard **today**:

- A slow storage process losing the stream is a durability incident
  (policy B on a data plane). Gap-fill can fabricate canonical roots
  (`[ARCH]` §4.3 link 3). That is why S2 removes this consumer, not why
  the reasons should merge.

What this forbids:

- One reason for both "wrong incarnation" and "evicted."
- Checking too-old before session (a restarted server would lie).
- After S2: treating `CURSOR_TOO_OLD` as an archive-recovery trigger.
- Closing the S2 entry gate by marking this `accepted` without the
  Consequences rewrite.

## Alternatives considered

**Single reason (`CURSOR_STALE`).** Rejected. Restart vs eviction have
different recoveries (re-bootstrap assumptions vs replay-from-head) and
CC-18/2 requires they be distinguishable.

**Drop `session_id`; key only on `seq`.** Rejected. `seq` restarts at 0
in a new process; a persisted cursor would silently replay the wrong
incarnation.

**Delete the cursor contract at S2 with the storage consumer.** Rejected.
`[ARCH]` §4.3: the ring survives for API/observer; bounds and cursor
semantics are unchanged.

## Refactor impact

**Survives for API consumers. Consequences rewritten at S2.**

| Stage | What happens to this record |
|---|---|
| S1 | This file. No cursor-protocol change. |
| S2 | **Revisit.** `S2-A-09` demotes the ring. Rewrite Consequences. Storage is not a consumer. Decision + reasons stay. |
| S5 | REST SSE is another API consumer of the same two reasons. |
