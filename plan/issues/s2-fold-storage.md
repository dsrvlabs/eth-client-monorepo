# S2 — fold storage · wk 17–25

**Entry.** **The §4 gate discharged** — ADR corpus (M11) resolvable + P2-E triage (**M12 = 0**), both
completed inside S1 ([`s1-fold-el-bridge.md`](s1-fold-el-bridge.md) `S1-B-05` … `S1-B-20`).
**Ships.** 2 processes · the archive-hole class is gone.

**Moves.** `services/storage/*` → `crates/storage-core`; `services/chain/*` → `crates/chain-core`;
boot into `bin/beacon-core`.
**Adds.** `ArchiveWrite` on the seam; direct typed column ingest with the top-of-batch continuity
bind; typed event structs.
**Deletes.** E4 `RestoreFromStore` (server **and** client), `services/chain/src/restore.rs`
(1,227 lines ✓), `services/storage/src/{write_behind,restore_client}.rs`, E5/E6/E7 transport, the
ring's durability role.

**Discharged by deletion at this stage** — P0-07, P0-13, P1-A/4, P1-A/22, P1-A/23, P1-A/27, P1-B/9,
P1-D/13, P1-D/14 (second half). **None of these is patched here.** Each was either patched at S0 with
its deletion recorded (P0-07, P0-13, P1-A/27, P1-D/14) or carries a `deleted @ S2` disposition that
forbids patching (P1-A/4, P1-A/22, P1-A/23, P1-B/9, P1-D/13).

**Out of scope** (`[PLAN]` §3/S2, `[ARCH]` §9.2/§4.4):

- **Wiring any dead island** — that is S3a (P0-16, P0-17).
- **Selecting the E1/E2 transport** — R-14 moves it to S3b *exit*.
- **Changing the redb on-disk schema** — rollback depends on it being unchanged.
- Any direct `cc-chain` ↔ `cc-p2p` call outside `cc-seam`.
- Merging the moved services in as *modules* rather than crates (⟡ D-1).

Estimate provenance and the points scale are defined in [`s0a-gate-restoration.md`](s0a-gate-restoration.md).

---

## Issue index

### Stream A — consensus core (`chain-core`, ingest, P0-19/3) · **plus the single-owner boot**

| Id | Title | pd | pts | Deps |
|---|---|---:|---:|---|
| `S2-A-01` | `crates/chain-core` skeleton; move `core.rs`, `import.rs`, `apply_attestations.rs` | 2.5–3 | 5 | — |
| `S2-A-02` | Move `da.rs`, `pending_engine.rs`, `residency.rs`, `events/` | 2–3 | 5 | `S2-A-01` |
| `S2-A-03` | `services/chain/main.rs` → thin shim over `chain-core` | 1.5–2 | 3 | `S2-A-02` |
| `S2-A-04` | `ArchiveWrite` on `cc-seam` + the typed `ColumnBatch` | 2–2.5 | 5 | `S2-A-01` |
| `S2-A-05` | Direct column ingest; **delete the byte-offset parse and its zero fallback** | 2–3 | 5 | `S2-A-04` |
| `S2-A-06` | The **top-of-batch continuity bind** in the writer | 2.5–3 | 5 | `S2-A-05` |
| `S2-A-07` | **ADR-R-02** — policy B → policy A is a deliberate change (**separate PR**) | 1–1.5 | 3 | `S2-A-06` |
| `S2-A-08` | P1-D/11 (S2 half) — typed event structs | 2–3 | 5 | `S2-A-02` |
| `S2-A-09` | Demote the ring to API/observer; delete `write_behind.rs` and gap-fill | 2–3 | 5 | `S2-A-08`, `S2-A-06` |
| `S2-A-10` | **P0-19/3** — move the pubkey cache onto `TransitionContext` | 3–4 | 8 | `S2-A-01` |
| `S2-A-11` | P0-19/3 — update call sites and the 8 hand-filling harnesses | 2–3 | 5 | `S2-A-10` |
| `S2-A-12` | **E2.5** — measure that the state clone no longer deep-copies the map | 1.5–2 | 3 | `S2-A-11` |
| `S2-A-13` | S2 testability harness — in-process boot, one TempDir, one redb | 1.5–2.5 | 3 | `S2-J-01` |
| `S2-A-14` | S2 testability assertion — `import → durable`, **no gRPC anywhere** (E2.3) | 1.5–2 | 3 | `S2-A-13` |
| `S2-A-15` | §9.0 A/B run + exit note; **E2.6** M10 on the 8-edge denominator | 2–3 | 5 | all |
| `S2-J-01` | **`bin/beacon-core` boot** — one process opens redb before any subsystem | 3–4 | 8 | `S2-A-03`, `S2-B-03` |
| `S2-J-02` | Delete E4 `RestoreFromStore` (server **and** client) and `restore.rs` | 2–3 | 5 | `S2-J-01` |
| | **Stream A total** | **34–47.5** | **81** | |

`S2-J-*` are **single-owner join work** — see the warning below.

### Stream B — edge & platform (`storage-core`, P0-18, storage rows, rollback)

| Id | Title | pd | pts | Deps |
|---|---|---:|---:|---|
| `S2-B-01` | `crates/storage-core` skeleton; move `writer.rs` + serve read paths | 2–3 | 5 | — |
| `S2-B-02` | Move `backfill.rs`, `prune/`, `durable_set.rs`, resume | 2–3 | 5 | `S2-B-01` |
| `S2-B-03` | `services/storage/main.rs` → thin shim over `storage-core` | 1.5–2 | 3 | `S2-B-02` |
| `S2-B-04` | **P0-18/1** — interned table-name exhaustion at ~30 d uptime | 3–4 | 8 | `S2-B-01` |
| `S2-B-05` | **P0-18/2** — the contig-walk cap sits below the full serve window | 2–3 | 5 | `S2-B-01` |
| `S2-B-06` | **P0-18/3** — multi-GB invariant scans block `open` | 3–4 | 8 | `S2-B-01` |
| `S2-B-07` | P1-A/2 — admission never checks the batch attaches to the durable frontier | 2–2.5 | 5 | `S2-A-06` |
| `S2-B-08` | P1-A/3 — `per_index` padding fabricates progress; the monotone guard misfires | 2–2.5 | 5 | `S2-B-07` |
| `S2-B-09` | P1-A/6 — `drop_table` failure after marks advanced leaks the shard | 1.5–2 | 3 | `S2-B-02` |
| `S2-B-10` | P1-B/2 — unary serve permits released at handler return | 1–1.5 | 3 | `S2-B-01` |
| `S2-B-11` | P1-B/3 — `measure_load_from_store` runs multi-second CPU on the async runtime | 1.5–2 | 3 | `S2-B-02` |
| `S2-B-12` | P2-B/1 + P2-B/2 — the two low-quality storage rows riding along | 1.5–2 | 3 | `S2-B-01` |
| `S2-B-13` | Rollback procedure — the operator document | 1.5–2 | 3 | `S2-J-01` |
| `S2-B-14` | **E2.4** — the restart drill: rehearse the rollback, do not document it | 1.5–2 | 3 | `S2-B-13` |
| `S2-B-15` | **W10 prerequisite** — begin sourcing the five foreign clients (A-5 lead time) | 1–2 | 3 | — |
| `S2-B-16` | M3 ledger — `Discharged by` maintenance for S2 rows and deletions | 0.5 | 1 | — |
| | **Stream B total** | **27.5–38** | **66** | |

**Phase totals.** **61.5–85.5 pd · 147 pts.** Against `[PLAN]`'s **58–87 pd** — consistent.

**Parallel duration.** `[PLAN]` A-2 sets efficiency **0.8 for the first two thirds** and **0.6 for the
last third**, where both streams converge on `bin/beacon-core` boot. Stream A binds:
(34–47.5 − 5–7 pd of join work) at 4 effective pd/wk = 7.25–10.1 wk, plus the join at ~5 pd/wk
single-owner = 1–1.4 wk → **≈ 8.25–11.5 wk**, against `[PLAN]`'s **8–10 wk**.

---

## The join — read this before assigning `S2-J-01` and `S2-J-02`

`[PLAN]` §9 marks S2's last third **"No — this is a join."** `[ARCH]` §4.2: **one** process opens
redb in `bin/beacon-core` before any subsystem starts. **Boot is single-owner.** Adding a second
engineer to `S2-J-01` does not shorten it; it is the reason A-2 drops parallel efficiency to 0.6 for
this third of the stage. The second stream **reviews** and writes the rollback rehearsal
(`S2-B-13`, `S2-B-14`) during this window.

Do not story-point these two issues as though they parallelise.

---

## Stream A issues

### `S2-A-01` … `S2-A-03` · `crates/chain-core` extraction

**Combined** 6–8 pd / **13 pts** (≈) · **Discharges** part of P1-E/S2

Same verbatim-move contract as S1's engine fold: behavioural change in a move PR is a review-stopper.
`services/chain`'s `main.rs` becomes a thin shim for the duration and is deleted at the end of the
stage, so the previous topology stays runnable for A/B (`[ARCH]` §9.1).

**Carry these ADRs intact through the move** — `ADR-P1-09` (the `Store` owned **by value** on a
dedicated OS thread, not a tokio task ✓ `core.rs:1`, `:469`), `ADR-P1-12` (state residency + the
64-block body ring), `ADR-P3-05` (engine-unavailable requeue in a **separate** map from `pending_da`).

**Acceptance (`S2-A-01` only)**
- [x] `crates/chain-core` (`cc-chain-core`) is a workspace member
- [x] `core.rs`, `import.rs`, `apply_attestations.rs` live in `crates/chain-core`, verbatim with tests
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `cargo test -p cc-chain-core` covers the moved unit tests
- [x] `services/chain` compiles the three files via `#[path]` and stays a workspace member
- [x] `allowed_deps` names `cc-chain-core`; `cc-chain` appends `cc-chain-core`
- [x] `cc-chain-core` is **not** JWT-grandfathered (E1.4 shape; fixture `chain-core-jwt`)
- [x] ADR-P1-09, ADR-P1-12, ADR-P3-05 carried intact (files moved, not rewritten)

**Acceptance (`S2-A-02` only)**
- [x] `da.rs`, `pending_engine.rs`, `residency.rs`, `events/` live in `crates/chain-core`, verbatim with tests
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `cargo test -p cc-chain-core` covers the moved unit tests
- [x] `services/chain` compiles the four surfaces via `#[path]` and stays a workspace member
- [x] ADR-P3-05 `pending_engine` stays a separate map from `pending_da` (files moved, not rewritten)
- [x] ADR-P1-12 residency + 64-block body ring carried intact

**Acceptance (`S2-A-03` only)**
- [x] Remaining crate siblings (`engine`, `epoch_context`, `fcu_driver`, `head`, `invalidation`, `liveness`, `metrics`, `p2p_stream`, `restore`, `service`, `tick`) live in `crates/chain-core`, verbatim
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `services/chain` compiles via `pub use` from `cc-chain-core` and stays a workspace member
- [x] `services/chain/main.rs` is a thin shim over `chain-core` (host in `run()`)
- [x] S1-A-06 engine wiring preserved (`DirectEngine`, JWT abort-before-bind)
- [x] S1-A-16 liveness sampler preserved
- [x] `restore.rs` still exists (not S2-J-02)
- [x] `checkpoint_sync.rs` stays in `cc-chain` (HTTP grandfather; `cc-chain-core` is not HTTP-grandfathered)
- [x] `cargo test -p cc-chain-core` covers the moved unit tests

---

### `S2-A-04` · `ArchiveWrite` on `cc-seam` + the typed `ColumnBatch`

**Stream** A · **Est** 2–2.5 pd / **5 pts** · ⌂ `[ARCH]` §4.3

`ColumnBatch { slot, block_root, index: ColumnIndex, ssz: Bytes }` — **`index` is a field, not a
guess.**

**Acceptance** — the trait lives in `cc-seam` with a stated overflow contract (policy **A**, the
writer mailbox's three-class priority admission surfacing `SeamError::Backpressure` to the import
path — ADR-P4-04 ✓ `services/storage/src/writer.rs:1`), and `chain-core` names `Arc<dyn ArchiveWrite>`,
never a storage type.

- [x] `ArchiveWrite` + typed `ColumnBatch` on `cc-seam`. `index` is a field, not a guess
- [x] Overflow contract is policy **A**: writer mailbox (ADR-P4-04) surfaces `SeamError::Backpressure`
- [x] `chain-core` names `Arc<dyn ArchiveWrite>`, never a storage type
- [x] `cc-seam` appended to `cc-chain-core` `allowed_deps` (never re-sort)
- [x] No ingest (`S2-A-05`); no byte-offset parse deletion; no `ADR-R-02`

---

### `S2-A-05` · Direct column ingest — delete the byte-offset parse

**Stream** A · **Est** 2–3 pd / **5 pts** · ⌂ `[ARCH]` §4.3 · **Deps** `S2-A-04` ·
**Discharges** part of P1-D/13, part of P1-D/11

`[ARCH]` §4.3 verifies a three-link chain plus a fourth defect riding it:

```
LINK 1  chain relays column SSZ into the ring WITHOUT DECODING
        services/chain/src/p2p_stream.rs:17-18, :618-631  (column_decode_attempts stays 0 by construction)
LINK 2  the ring is a bounded evicting buffer whose eviction is now a DURABILITY event
        services/chain/src/events/mod.rs:83,88   RING_CAPACITY=4096, RING_BYTES=64 MiB
        services/chain/src/events/mod.rs:34-35   per-subscriber mpsc(256), try_send; on Full the SUBSCRIBER IS DROPPED
LINK 3  recovery from eviction can INVENT canonical history
        services/storage/src/write_behind.rs:892-895   CURSOR_TOO_OLD -> GetCanonicalRoots gap-fill
        services/chain/src/core.rs:772-777             get_ancestor(head, s).unwrap_or(head)
+       services/storage/src/write_behind.rs:763-770   column index read at a FIXED SSZ BYTE OFFSET with unwrap_or(0)
```

**After S2:** column bytes **never enter the ring**. `chain-core` calls
`ArchiveWrite::ingest_columns(batch)` directly. The byte-offset parse and its zero fallback are
**deleted** — a short or malformed payload can no longer be durably stored as column index 0.

**Acceptance** — `column_decode_attempts` is no longer 0-by-construction; a malformed sidecar is
**rejected**, not stored as index 0; a grep for `column_index_at_offset` returns 0.

- [x] `chain-core` calls `ArchiveWrite::ingest_columns` with typed `ColumnBatch.index`
- [x] Column bytes never enter the ring on the p2p column path
- [x] `column_decode_attempts` is no longer 0-by-construction
- [x] Malformed sidecar is rejected, not stored as index 0
- [x] `column_index_at_offset` / `unwrap_or(0)` deleted from write-behind
- [x] No continuity bind (`S2-A-06`); no `ADR-R-02`

---

### `S2-A-06` · The top-of-batch continuity bind

**Stream** A · **Est** 2.5–3 pd / **5 pts** · ⌂ `[ARCH]` §4.3 · **Deps** `S2-A-05`

With the ring gone, the mechanism that guaranteed *"these events belong to a contiguous, attributable
range"* (the `seq` cursor) goes with it. Its replacement:

> Every batch submitted to the writer carries, at its head, the `(parent_root, slot)` of the block
> the batch's columns and canonical rows attach to. The writer **rejects the batch** unless that
> parent is already durable, or is the first row of the same batch. There is **no** "progress
> optional" path and **no** empty-progress bypass.

**This is the same invariant `S0-B-10` (P1-A/1) established server-side on `PutBackfillBatch`.**
`[ARCH]` §4.3 is explicit that the S0 patch and the S2 design must state the **same** invariant, in
the same words, so the S0 work is not thrown away: *a batch may only extend the durable frontier,
never jump it.* Quote it verbatim in both places.

**Acceptance** — a batch whose parent is neither durable nor the first row is rejected; an
empty-progress batch has no fast path; both asserted by test, with the test named identically to
`S0-B-10`'s so the pair is greppable.

---

### `S2-A-07` · **ADR-R-02** — the overflow-policy change

**Stream** A · **Est** 1–1.5 pd / **3 pts** · ⌂ `[ARCH]` §4.3, §10.5 · **Deps** `S2-A-06`

**A deliberate policy change, and `[PRD]` R-1 makes it a spec change requiring an explicit decision:**
today a slow archive **silently loses its subscription and reconnects with a cursor** (policy **B**);
after S2 a slow archive **applies backpressure to block import** (policy **A**). That is the correct
trade for a node whose archive is its own — and it is a change that must be recorded as one.

**PR-boundary constraint (`[ARCH]` §9.2).** *"Changing an overflow policy in the same PR that moves a
transport"* is prohibited — it makes the diff unreviewable. **This issue is a separate PR from
`S2-A-05`, `S2-A-06` and `S2-A-09`.**

ADR-R-02 also carries `beacon-core` owning redb and the event bus ceasing to be a data plane, and is
the supersession target for `ADR-P2-11`, `ADR-P4-07` and (per `S1-B-16`) `ADR-07`'s S3 re-decision
chain. `[PLAN]` R-17 flagged `ADR-R-02` as *"created at S2"* — this is that creation, and `S1-B-16`
carries the `Status: proposed` placeholder that made the S2 entry gate satisfiable.

---

### `S2-A-08` · P1-D/11 (S2 half) — typed event structs · 2–3 pd / **5 pts** (≈)
Hand-rolled event byte-offsets with silent-default fallbacks become typed structs. R-1's failure mode
is silent; this is the last of the untyped cross-service surfaces.
**Acceptance** — no production decode of an event payload reads a fixed byte offset; a grep confirms.

- [x] Typed event payload structs live on `cc-seam` (`BlockImportedPayload`, `HeadPayload`, `ChainReorgPayload`, `FinalizedCheckpointPayload`)
- [x] Production write-behind / migrate decode via those structs; unknown / short payloads fail closed (no silent default)
- [x] No production `ev.payload[` / `payload[..N]` event-payload index (grep / test)
- [x] `write_behind.rs` still exists (not `S2-A-09`)
- [x] Overflow policy unchanged (not `S2-A-07` / `ADR-R-02`)

### `S2-A-09` · Demote the ring; delete `write_behind.rs` and gap-fill · 2–3 pd / **5 pts** (≈)
**Discharges by deletion** P1-D/13, P1-A/4 (`write_behind.rs:214`, panic respawn resubscribes with the
boot-time cursor), P1-B/9 (`storage_client.rs:240`, `invalidate()` with no data-plane restore path),
P0-13's surface, P1-D/14's second half.

**The ring survives, demoted** (`[ARCH]` §4.3): it serves the API/observer surface (`SubscribeEvents`
for external consumers, later the REST SSE endpoint at S5). **Bounds and cursor semantics unchanged;
policy B still applies**, because a slow *external* consumer being dropped is correct. Ring eviction
is no longer a durability event for anything.

**Gap-fill is deleted** — there is no cursor to lose. `GetCanonicalRoots`'s `unwrap_or(head)`
fabrication (P2-E row 1, **promoted P1 `patch @ S0` at `S0-B-17`**) **must still be handled per that
outcome** if the S0 patch has not landed, because the RPC survives for API consumers; only its role
in durable writes ends.

**Acceptance** — `services/storage/src/write_behind.rs` and `restore_client.rs` no longer exist; the
`SubscribeEvents` stream still serves an external consumer end to end, asserted by test.

---

### `S2-A-10` … `S2-A-12` · **P0-19/3** — the pubkey cache off `BeaconState`

**Combined** 6.5–9 pd / **16 pts** · ⌂ `[Q3]` Change 3 = **M** (1–2 wk) · **Hard prerequisite for
S4a** (D4)

`StateCaches` derives `Clone`, so an **~80–100 MB `HashMap` with 48-byte keys deep-copies on each of
the 3–5 state clones per import**. **Landing milhouse first makes its O(1) clone a lie** and the
migration would ship and measure no improvement (`[PLAN]` D4, `[PRD]` P1-D/10).

Independently, this is what both reference clients do: Lighthouse hangs the cache off `BeaconChain`;
Grandine makes `pubkey_cache` its own crate (`[Q3]` §3, research README cross-cutting finding 3).

- `S2-A-10` (3–4 pd) — move `PubkeyIndexMap` from `BeaconState.caches` onto `TransitionContext`.
- `S2-A-11` (2–3 pd) — update all call sites **and the eight test harnesses that hand-fill it**
  (`[PRD]` §5.1.2/3). Those harnesses are why no test caught P0-19; leaving them hand-filling a field
  that has moved is how the class comes back.
- `S2-A-12` (1.5–2 pd) — **E2.5**: a measurement showing the state clone no longer deep-copies the
  pubkey map. **This is what makes S4a's O(1) claim true rather than asserted.** Record the before and
  after clone cost in the exit note with absolute numbers.

**Acceptance (`S2-A-10` only)**
- [x] `PubkeyIndexMap` is a field of `TransitionContext`, not `StateCaches`
- [x] `TransitionContext::new` constructs an empty map
- [x] `TransitionContext::top_up_pubkey_cache` fills from the validator registry (append-only, idempotent)
- [x] `process_block` / `state_transition` top up the context map before `process_sync_aggregate`
- [x] `BeaconState` clone no longer includes the pubkey map (type-level; measurement is `S2-A-12`)
- [x] No harness rewrite (`S2-A-11`); no clone-cost measurement (`S2-A-12`)

**P0-19/4 (persist the cache in `cc-store`, `[Q3]` **S–M**) is `patch @ S2`, optional.** Not scheduled
here. If it is picked up, note the distinction `S0-B-14` wrote into the module docs: the pubkey cache
**is** reconstructible from the state, so losing it is a slow boot, and it **is** safe on the
write-behind path — unlike slashing protection.

---

### `S2-A-13` · `S2-A-14` · The S2 testability test

**Combined** 3–4.5 pd / **6 pts** · ⌂ `[ARCH]` §8.2 · **Discharges** half of **M9**, E2.3

`import → durable`, **one `TempDir`, one redb, no gRPC anywhere.**

- `S2-A-13` — the harness: boot `bin/beacon-core`'s boot path in-process against a `TempDir`.
- `S2-A-14` — the assertion: import a block, assert the durable rows.

**Acceptance (falsifiable)** — the test binary's dependency graph contains **no** `tonic` and no
`cc-proto` for this path. Assert it mechanically (a `cargo tree` check in CI), not by reading the
`Cargo.toml`. "No gRPC anywhere" read by eye is the anti-metric `[PRD]` §7.5 names.

---

### `S2-A-15` · §9.0 A/B run + exit note · 2–3 pd / **5 pts**
Same three families, same blocker rule as E1.1 (E2.1). Also carries **E2.6**: **M10 restated on the
8-edge denominator** (⟡ D-2 — there are eight internal edges, not six) — E3–E7 deleted, E1/E2/E8
remaining. State M10 as *3 of 8 remaining, 0 dead*, not as a fraction of six.

---

### `S2-J-01` · `bin/beacon-core` boot — **single-owner**

**Stream** A (join) · **Est** 3–4 pd / **8 pts** · ⌂ `[ARCH]` §4.2 · **Do not parallelise**

```rust
// bin/beacon-core/src/boot.rs  (shape, from [ARCH] §4.2)
let db      = storage_core::open(&cfg.data_dir, opts)?;   // fail-closed gates unchanged
let durable = storage_core::durable_set(&db)?;            // was the RestoreFromStore payload
let store   = match durable {
    Some(d) => chain_core::seed_from_durable(d, &cfg)?,   // was apply_restore_set
    None    => chain_core::checkpoint_sync(&cfg).await?,  // was the EMPTY fallback
};
```

**What replaces the 30 s grace window: nothing.** It was a synchronisation device for two processes.
`open()`'s existing fail-closed gates and the `I-node-id` check ✓
(`services/storage/src/main.rs:403-430`, ADR-P4-13) **are** the boot policy, and they run before
anything else. `ADR-P4-06`'s persisted fork-choice scalars (≈ 300 B, *not* a 75 MB vote table ✓
`crates/store/src/meta.rs:167`) become the boot seed.

**P0-18 becomes restart-critical-path as of this issue** (`[ARCH]` §4.2) — `S2-B-04..06` must land
before or with it, because a multi-GB invariant scan now blocks the *node's* boot, not a sidecar's.

**Acceptance** — one process opens redb; no subsystem starts before `open()` returns; a second opener
of the same data directory fails the `I-node-id` check.

---

### `S2-J-02` · Delete E4, `restore.rs`, and the restore client

**Stream** A (join) · **Est** 2–3 pd / **5 pts** · **Deps** `S2-J-01` ·
**Discharges by deletion** P1-A/22, P1-A/23, and E4's unauthenticated surface

**Blocked on `S0-A-31` (R-13 observation b).** **Recorded 2026-08-16:** restore **reaches**
`end_stream` (Hoodi snapshot, both hydrated and raw decode). P0-19 is real **and** /22 and /23 are
**independent**. This issue's precondition is their re-disposition by explicit decision — not a
silent deletion. See [`s0-exit-note.md`](s0-exit-note.md) § R-13 (b).

**Deletes** — `RestoreFromStore` server and client, `services/chain/src/restore.rs` (1,227 lines ✓),
`services/storage/src/restore_client.rs`. `ADR-P4-07`'s citations go with the code (`S1-B-17` already
wrote it `Status: superseded-by ADR-R-02` so the history stays legible).

**Acceptance** — `restore.rs` no longer exists; `RestoreFromStore` is absent from
`proto/eth/chain/v1/chain.proto`; a grep for `apply_restore_set` returns 0.

---

## Stream B issues

### `S2-B-01` … `S2-B-03` · `crates/storage-core` extraction · 5.5–8 pd / **13 pts** (≈)
Same verbatim-move contract. `services/storage`'s `main.rs` becomes a thin shim for the duration and
is deleted at the end of the stage (`[ARCH]` §9.1).

**Acceptance (`S2-B-01` only)**
- [x] `crates/storage-core` exists as a workspace member (`cc-storage-core`)
- [x] `writer.rs` lives in `crates/storage-core`, verbatim with tests
- [x] Serve read paths live in `crates/storage-core/src/serve.rs` (git rename; `PutBackfillBatch` stays in the moved file until later deletion)
- [x] `backfill.rs` / `prune/` stay in `services/storage` (`S2-B-02`)
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `cargo test -p cc-storage-core` covers the moved writer + serve tests
- [x] `services/storage` compiles via `#[path]` from `cc-storage-core` and stays a workspace member
- [x] `cc-storage-core` appended to `cc-storage` `allowed_deps`
- [x] **`cc-storage-core` is NOT on the JWT grandfather list** (function self-test + `expect-fail/storage-core-jwt/`)

**Acceptance (`S2-B-02` only)**
- [x] `backfill.rs` lives in `crates/storage-core`, verbatim with tests
- [x] `prune/` lives in `crates/storage-core/src/prune/`, verbatim with tests
- [x] `durable_set.rs` and `resume.rs` live in `crates/storage-core`, verbatim with tests
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `cargo test -p cc-storage-core` covers the moved backfill / prune / durable_set / resume tests
- [x] `services/storage` compiles the four via `#[path]` from `cc-storage-core` and stays a workspace member
- [x] `services/storage` `main.rs` is not a thin shim (`S2-B-03`)

**Acceptance (`S2-B-03` only)**
- [x] `services/storage` `main.rs` is a thin shim over `cc-storage-core`
- [x] `services/storage` stays a workspace member (`[ARCH]` §9.1)
- [x] writer / serve / backfill / prune / durable_set / resume are production items of `cc-storage-core` (not `#[cfg(test)]` / `#[path]`)
- [x] Remaining companions (`history`, `metrics`, `migrate`, `replay`, `restore_client`, `write_behind`, `test_tmpdir`) live in `crates/storage-core` (git rename)
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved companion
- [x] `cargo test -p cc-storage-core` covers the moved unit tests
- [x] `cc-storage-core` is still **not** JWT/HTTP-grandfathered
- [x] P0-18 (`S2-B-04`…`06`) not implemented

---

### `S2-B-04` … `S2-B-06` · **P0-18** — the three storage scale time bombs

**Combined** 8–11 pd / **21 pts** (≈) · **Discharges** P0-18

**`[PRD]` P0-18 cites only `[AS] §4/08; crates/store` — no `file:line`.** Recovered here by grep so
the issues are actionable; write these back into `[PRD]` §5.1.

| Id | Time bomb | Touch points (recovered) |
|---|---|---|
| `S2-B-04` | **Interned table names exhaust at ~30 days uptime.** One redb table per shard, named by `format!` | `crates/store/src/keys.rs:51` (`format!("{shard_id:05}")`), `:56` (`columns_{suffix}`), `:61` (`blocks_{suffix}`) · registry reconciliation at `crates/store/src/schema.rs:382,397` (*"reconcile `table_names()` against the registry; unregistered → error"*) · every consumer that iterates them: `crates/store/src/invariants.rs:507,711`, `blocks.rs:388`, `columns.rs:777` · shard widths in epochs: `crates/store/src/keys.rs:10,13` (ADR-P4-10, which `[ARCH]` §10.4 notes **interacts with P0-18**) |
| `S2-B-05` | **The contig-walk cap sits below the full serve window.** `I-contig` is the expensive check and never walks every integer slot | `crates/store/src/invariants.rs:45-50` — `MAX_CONTIG_WALK_SLOTS: u64 = MAX_RANGE_ENTRIES as u64` (SEC-4H-1) · `:12-18` (the module doc stating the cap's role) · `:367` `check_contig`, `:432` `contig_hole_violation`, `:444` `first_uncovered_slot` · the serve window it must cover: `crates/store/src/backfill_progress.rs:237` |
| `S2-B-06` | **Multi-GB invariant scans block `open` on a supernode.** Becomes **restart-critical-path** at this stage | `crates/store/src/invariants.rs:271` `check_invariants`, `:332` `run_invariant_checks_if_enabled` · `:596-624` `check_ring` and `MAX_RING_SCAN_ROWS` (*"refuse rather than unbounded scan"*) · `:476` `check_col_block`, `:552` `check_split_fin`, `:651` `check_window` |

**Acceptance (`S2-B-04` only)**
- [x] Shard table names still encode the logical shard id (`format_shard_suffix` / `parse_shard_suffix`); 32 / 256 epoch widths unchanged (ADR-P4-10)
- [x] Intern pool is the **live** set: dropped tables leave the pool; `Box::leak` of every unique name is gone
- [x] `MAX_INTERNED_TABLE_NAMES` stays 512 (not raised)
- [x] Registry reconciliation walks `Engine::table_names()` only (`iter_shard_tables` / `find_unregistered_table`) — O(active), not O(all shards ever)
- [x] A test simulating 30+ days of shard rollover (and more unique names than the intern cap) does not exhaust the table namespace

**Acceptance (`S2-B-05` only)**
- [x] `MAX_CONTIG_WALK_SLOTS` is ≥ the CC-4A block serve window (`min_epochs × SLOTS_PER_EPOCH`)
- [x] `I-contig` walks the window in `MAX_RANGE_ENTRIES`-sized slot chunks (a single range call cannot sit below the window)
- [x] A test with a serve window wider than the legacy `MAX_RANGE_ENTRIES` cap completes the check
- [x] P0-18/3 (`S2-B-06` `open()` bound) not implemented
- [x] Interned table names / `MAX_INTERNED_TABLE_NAMES` untouched (`S2-B-04`)

**Acceptance for the group**
1. `S2-B-04` — a test simulating 30+ days of shard rollover does not exhaust the table namespace, and
   the registry reconciliation stays O(active shards), not O(all shards ever). ✓
2. `S2-B-05` — the cap is ≥ the configured serve window, or the walk is chunked; a test with a serve
   window wider than today's cap completes the check. ✓
3. `S2-B-06` — `open()` returns within a stated bound on a multi-GB store; the bound is a config value
   with a named default, and exceeding it is an error rather than an unbounded wait. **Measured on a
   store at supernode scale, not a `TempDir`** — a scan that is fast on 10 MB proves nothing.

---

### `S2-B-07` · P1-A/2 — admission never checks the durable frontier · 2–2.5 pd / **5 pts** (≈)
**Touch** `services/storage/src/backfill.rs:219`. `blocks_oldest` can jump down across a hole.
**Deps `S2-A-06`** — same invariant, stated once. **Acceptance** — a batch that would jump the
frontier is rejected; a test constructs the hole explicitly.

### `S2-B-08` · P1-A/3 — `per_index` padding fabricates progress · 2–2.5 pd / **5 pts** (≈)
**Touch** `services/storage/src/backfill.rs:87`. Padding fabricates progress for never-custodied
indices, and the monotone guard then **rejects honest cgc-raise reports**.
**Acceptance** — a never-custodied index reports no progress rather than padded progress; a legitimate
custody-group-count raise is accepted. Both directions asserted.

### `S2-B-09` · P1-A/6 — `drop_table` failure leaks the shard · 1.5–2 pd / **3 pts** (≈)
**Touch** `services/storage/src/prune/mod.rs:532`. Failure **after** marks durably advanced leaks the
stripped shard permanently. **Acceptance** — marks advance only after the drop commits, or a
recoverable record of the pending drop survives restart.

**Acceptance (`S2-B-09` only)**
- [x] Marks advance only after `drop_table` commits (per-key deletes may land first)
- [x] A failed drop leaves durable marks and cadence unmoved so the next tick retries
- [x] Test: injected drop failure holds marks and the table; retry (and restart) then drop + advance

### `S2-B-10` · P1-B/2 — unary serve permits released at handler return · 1–1.5 pd / **3 pts** (≈)
**Touch** `services/storage/src/serve.rs:484`. The documented 256 MiB ceiling is not enforced.
**Acceptance** — the permit is held for the lifetime of the response body; a test with two concurrent
large unary serves observes the ceiling.

### `S2-B-11` · P1-B/3 — multi-second CPU on the async runtime thread · 1.5–2 pd / **3 pts** (≈)
**Touch** `crates/storage-core/src/replay.rs` (was `services/storage/src/replay.rs:354`). A 200 MB SSZ decode + tree-hash on a runtime worker.
**Acceptance** — moved to `spawn_blocking` or the blocking pool; a test asserts the runtime stays
responsive during a `measure_load_from_store`. Same defect *shape* as `S0-A-28`'s `block_on` — cite it.

- [x] `measure_load_from_store` runs under `spawn_blocking` / the blocking pool (same defect shape as `S0-A-28`'s `block_on`).
- [x] A test asserts the runtime stays responsive during `measure_load_from_store`.

### `S2-B-12` · P2-B/1 + P2-B/2 · 1.5–2 pd / **3 pts** (≈)
P2-B/1: ordering-critical `hot_column_root_end` duplicated verbatim across modules
(`crates/store/src/split.rs:453`). P2-B/2: `get_columns_by_root` reads every column **twice through
two divergent code paths** (`services/storage/src/serve.rs:711`).

---

### `S2-B-13` · `S2-B-14` · Rollback — **rehearsed, not documented**

**Combined** 3–4 pd / **6 pts** · ⌂ `[ARCH]` §9.1 · **Discharges** E2.4

**S2 is the only stage with a data-shape consequence.** The redb schema does not change, but the
*writer's input* does:

- (a) the on-disk format is unchanged, so the previous topology can open the same data directory;
- (b) **the `WriteCursor` semantics change from stream-seq to batch-seq, so a rollback must be
  preceded by a clean shutdown**;
- (c) no migration is required in either direction.

`S2-B-13` writes (a)–(c) as an operator procedure. `S2-B-14` **rehearses it**: a clean shutdown, a
redeploy of the previous topology against the same data directory, and a **successful open**. E2.4 is
worded *"rehearsed, not documented"* for a reason — a procedure that has only been written has the
same evidential status as an X1 counter that has only been merged.

**R-10 note.** Before S2, every restart costs an external checkpoint re-sync and permanently holes the
archive. Budget re-sync time into this drill and record it in the Phase 4 clause-1 trial table — this
is exactly why `S3b-W-05` (20/20 restart trials) is scheduled **after** S2 (D11).

---

### `S2-B-15` · **W10 prerequisite** — begin sourcing the five foreign clients

**Stream** B · **Est** 1–2 pd / **3 pts** · ⌂ `[PLAN]` A-5 / D12 · **Blocks** `S3b-W-10` (OQ-1 + M2e)

**External parties with lead time. Sourcing starts at S2, not at S3b.** If it starts when the window
opens, OQ-1 slips to the end of S3b and blocks M2d.

**Acceptance**
1. All five implementations named, with a contact and a tentative window date each.
2. A named owner on our side.
3. Recorded in the S2 exit note so `S3a-B-26` (E3a.5) can assert *sourced and scheduled*, not
   *contacted*.

`[PLAN]` §12 notes that starting outreach at **S1** instead removes W10 from the critical path — but
buys nothing toward the interop **fixes** that 0/5 → 5/5 may require. If the schedule is tight, pull
this issue into S1.

### `S2-B-16` · M3 ledger maintenance · 0.5 pd / **1 pt**
Every row S2 patched gets a commit SHA; every row **discharged by deletion** gets the stage id `S2`.
The nine deletion-discharged rows are listed at the top of this file — none may be left blank on the
argument that "no commit fixed it".

**Convention.** This is the S2 instance of the ~0.5 pd stage-exit line item `S0-B-18` writes down
(`[PRD]` §5.0 / E0.9). Also append `S2` to `CLAIM_STAGES` in `scripts/check-m3-discharged-by.sh`.
A blank S2-claimed cell is a CI failure. That is the program's *no-P0-vanishes-silently* check.

- [x] `S2` appended to `CLAIM_STAGES` in `scripts/check-m3-discharged-by.sh`.
- [x] `[PRD]` §5.1/§5.2 `Discharged by` filled for every S2-claimed row (owning issue id until SHA lands).
- [x] Fixture self-test includes an S2 case and is green.

---

## S2 exit criteria — and which issue earns each

| # | Criterion | Earned by |
|---|---|---|
| E2.1 | §9.0 A/B clean — same three families, same blocker rule | `S2-A-15` |
| E2.2 | Two processes run on self-devnet | `S2-J-01`, `S2-A-15` |
| E2.3 | The `import → durable` in-process test exists and runs in CI (**half of M9**) | `S2-A-13`, `S2-A-14` |
| E2.4 | The rollback procedure is **rehearsed** — clean shutdown, redeploy of the previous topology against the same data directory, successful open | `S2-B-14` |
| E2.5 | **P0-19/3 landed**, with a measurement showing the state clone no longer deep-copies the pubkey map | `S2-A-12` |
| E2.6 | **M10 restated on the 8-edge denominator** (⟡ D-2): E3–E7 deleted, E1/E2/E8 remaining | `S2-A-15` |

---

## Drift against `[PLAN]` §3/S2 — stated, not smoothed

| # | Observation |
|---|---|
| 1 | **Total.** `[PLAN]` 58–87 pd; decomposed **61.5–85.5 pd** — the one phase whose group sizings survive decomposition almost unchanged. Two items `[PLAN]` does not price are nonetheless inside the range: `S2-B-15` (foreign-client sourcing, named as a §5 prerequisite) and `S2-A-07` (ADR-R-02, required by §4.3). |
| 2 | **Storage `patch @ S2` rows: `[PLAN]` says 7 rows / 8–12 pd; decomposed 10–12.5 pd.** The seven are P1-A/2, P1-A/3, P1-A/6, P1-B/2, P1-B/3, P2-B/1, P2-B/2. Three of them (P1-A/2, P1-A/3, P1-A/6) are correctness rows with test-fixture cost, not one-line fixes. |
| 3 | **P0-18 had no `file:line` in either source.** Recovered by grep in `S2-B-04..06`. `S2-B-06`'s acceptance criterion — *measured at supernode scale, not on a `TempDir`* — is this decomposition's addition; without it the fix is verified on the one input size where the bug does not exist. |
| 4 | **The join is 5–7 pd of genuinely single-owner work** (`S2-J-01` + `S2-J-02`). `[PLAN]` prices boot at 5–8 pd and does not separately price the E4 deletion, which is the other half of the same choreography. |
| 5 | **`S2-J-02` is gated on an S0 issue** (`S0-A-31`, R-13 observation b) that `[PLAN]` §3/S0 does not schedule — it says E0.4 discharges the diagnostic obligation, which is one observation short of `[PRD]` R-13. If `S0-A-31` is dropped, S2's deletion of `restore.rs` proceeds on an untested causal claim. |
