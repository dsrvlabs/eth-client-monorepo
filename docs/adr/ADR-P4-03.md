# ADR-P4-03 — Chain relays column SSZ without decoding

- **Status:** superseded-by ADR-R-02 · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4 (in force now; S2 replacement recorded here)
- **Issues:** S1-B-14, CC-44a, S2-A-04, S2-A-05, S2-A-07
- **Citations:** 1 census site — `services/chain/src/p2p_stream.rs:17-18` (handler: `:615-631`; counter: `:119-121,160-163`; wiring: `services/chain/src/service.rs:117-118`; bus ctor: `services/chain/src/events/mod.rs:214-226`; consumer: `services/storage/src/write_behind.rs:1081-1088`; contract: `services/chain/tests/p2p_stream_contract.rs:821-836`; `services/chain/tests/event_payloads.rs:333-411`)
- **Provenance:** re-derived from code (2026-08-16); S2 typed-ingest replacement recorded here

This is a **history record**. The Phase-4 no-decode relay was **right for
a relay and wrong for an owner.** `[ARCH]` §10.4 / `S1-B-14` required the
S2 change to be written down; `ADR-R-02` (`S2-A-07`) is the file that
supersedes this one. Typed ingest is live; Policy B→A is recorded there.

## Context

Phase 4 made `cc-chain` a pass-through for column bytes. p2p already
verified the sidecar. Storage is the only durable consumer. Decoding
the consensus `DataColumnSidecar` on the stream would couple chain to a
type it does not own, spend CPU on a gRPC hop that is about to forget
the bytes, and invent a second source of `column_index` next to the
payload.

The live path is three links (`[ARCH]` §4.3):

1. **Relay without decode.** `P2pToChain::Column` copies `ssz` and
   `root` onto the event bus as `DATA_COLUMN` (`p2p_stream.rs:615-631`).
   Chain does not name `DataColumnSidecar` and does not call
   `from_ssz_bytes` (`p2p_stream_contract.rs:821-836`).
   `column_decode_attempts` stays 0 by construction
   (`p2p_stream.rs:119-121,618-619`; `event_payloads.rs:408-411`).
   Proto `ColumnSidecar.column_index` (`p2p.proto:124-129`) is ignored.
   Slot is not on the wire message; the bus event is stamped `0`
   (`p2p_stream.rs:629-631`). Oversize is rejected before send
   (SEC-44a-2); a live producer uses `send().await`, not silent
   `try_send` (F1).
2. **The ring is the data plane.** `SubscribeEvents` carries full
   sidecar SSZ through a 4096-entry / 64 MiB ring
   (`events/mod.rs:83,88`). Per-subscriber `try_send` drops a slow
   consumer (`events/mod.rs:34-35`). Eviction is therefore a
   **durability** event for the archive (E7).
3. **The consumer guesses the index.** Write-behind peeks
   `COLUMN_INDEX_SSZ_OFFSET` and falls back to `0`
   (`write_behind.rs:1081-1088` — `column_index_at_offset(&ssz).unwrap_or(0)`,
   else a 2-byte LE read, else `0`). A short or malformed payload is
   durably stored as **column index 0**.

That shape is correct while chain is a relay and storage is another
process. After S2, `beacon-core` **owns** redb. An owner that cannot
name the column it is about to persist, and that can lose the only
copy of those bytes when a ring evicts, is the wrong owner.

S1 does not fold storage. Do not "type" this path here
(`S1-A-14`: the offset parse is deleted at S2, not patched).

## Decision

**Until S2: chain relays column SSZ without decoding.**

On `P2pToChain::Column`, publish `EventInput::data_column` with the
verbatim sidecar bytes and the block root. Do not construct a
consensus sidecar. Do not increment `column_decode_attempts`. Do not
trust proto `column_index` as a substitute for a typed batch — the
relay has no ingest type to put it in.

**At S2: replace the relay with a typed ingest.** This is the change
`S1-B-14` exists to record.

- Column bytes **never enter the ring**. The ring is demoted to
  API / observer use; eviction is no longer a durability event
  (`[ARCH]` §4.3, E7).
- `chain-core` calls `ArchiveWrite::ingest_columns(batch)` on
  `cc-seam` (`S2-A-04`, `S2-A-05`).
- The batch is typed:
  `ColumnBatch { slot, block_root, index: ColumnIndex, ssz: Bytes }`.
  **`index` is a field, not a byte-offset guess.**
- The write-behind `column_index_at_offset(…).unwrap_or(0)` path is
  **deleted**. A short or malformed sidecar is **rejected**, not stored
  as index 0. `column_decode_attempts` is no longer 0-by-construction.
- Every batch carries the top-of-batch `(parent_root, slot)` continuity
  bind (`S2-A-06`): a batch may only extend the durable frontier, never
  jump it.
- Overflow becomes the writer mailbox (ADR-P4-04) surfacing
  `SeamError::Backpressure` to import — policy **A**, not policy **B**.
  That policy flip is `ADR-R-02` (`S2-A-07`), which **supersedes** this
  file.

Do not implement the ingest in this issue. Do not write `ADR-R-02`
here. Do not keep the no-decode rule after the owner fold "because
decode is expensive."

## Consequences

What this makes easy:

- Today: the p2p→chain hop stays a byte copy. Chain's SSZ surface does
  not grow a sidecar type. Tests can assert
  `column_decode_attempts() == 0` and `!contains("DataColumnSidecar")`.
- S2: the owner names `index` at the call site. A malformed sidecar
  cannot become durable column 0. Reviewers have a committed sentence
  that the relay is not the post-fold design.
- `ADR-R-02` has a concrete predecessor to supersede, instead of
  deleting an unwritten id.

What this makes hard:

- Until S2, durability of columns is coupled to ring occupancy and to
  a second process staying subscribed. A `CURSOR_TOO_OLD` reconnect
  can still invent canonical roots on the gap-fill path
  (`[ARCH]` §4.3 link 3). That is accepted residual, not a license to
  patch write-behind in S1.
- Anyone who wants to decode on the stream *now* has to contradict
  this file and the CC-44a contract test.

What this forbids:

- Constructing or SSZ-decoding `DataColumnSidecar` on the chain relay
  path while this record is in force.
- Patching `column_index_at_offset(…).unwrap_or(0)` in S1 as if that
  were the fix (`S1-A-14` / `S2-A-05`).
- Keeping the no-decode relay as the ingest path after S2 folds
  storage into `beacon-core`.
- Treating proto `ColumnSidecar.column_index` as a durable key while
  the payload is still opaque bus bytes.
- Re-using the event ring as a bulk column data plane after S2.

## Alternatives considered

**Keep no-decode after S2 because the sidecar is already verified.**
Rejected. Verification is not ownership. An owner that persists bytes
it cannot name re-creates the index-0 fallback inside the same
process. `[ARCH]` §4.3 deletes the byte-offset parse; it does not
move it.

**Decode on the bus in S1 (type the relay now).** Rejected. S1 does
not fold storage. A half-typed relay still dumps SSZ into an evicting
ring. `S1-A-14` explicitly leaves this instance to `S2-A-05`.

**Use proto `column_index` on the relay and stop peeking SSZ.**
Rejected as a silent half-patch. The proto field is already on the
wire and already ignored. Promoting it without a typed `ColumnBatch`
and without removing the ring leaves two sources of index and the
same eviction-as-durability event.

**Keep the ring, raise `event_ring_events` / `event_ring_bytes`.**
Rejected in `[ARCH]` §10.5 `ADR-R-02`. That moves the eviction
threshold; it does not stop eviction from being a durability event.

**Write `ADR-R-02` in this issue and mark this id superseded now.**
Rejected. The ingest is not built. Superseding a live relay with a
file that does not exist yet would make the table lie. `S2-A-07`
writes `ADR-R-02` in the same stage as the call.

## Refactor impact

**Modified at S2; superseded by ADR-R-02.** The no-decode rule does
not survive the owner fold.

| Stage | What happens to this record |
|---|---|
| S1-B-14 | This file. No production code change. Records the S2 replacement. |
| S1 (rest) | Leave `p2p_stream.rs` and `write_behind.rs` alone. CC-44a tests stay green. |
| S2-A-04 | `ArchiveWrite` + typed `ColumnBatch` on `cc-seam`. `index` is a field. |
| S2-A-05 | Direct `ingest_columns`. Delete the byte-offset parse and its zero fallback. Column bytes never enter the ring. |
| S2-A-06 | Top-of-batch `(parent_root, slot)` continuity bind. |
| S2-A-07 | **Done.** ADR-R-02 landed; this file is `superseded-by ADR-R-02`. Policy B → A is recorded there. |
| S2+ | A new no-decode column path into the archive is a defect against ADR-R-02. |
