# ADR-R-05 — Slashing protection is a separate, exclusively-locked, synchronous store

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-15
- **Phase:** 5 (decision recorded at S0; crate implemented at S5)
- **Issues:** S0-B-14, S0a-B-10, P1-F/1
- **Citations:** `plan/prd.md` P1-F/1; `plan/architecture.md` §9.1 S5 / §10.5; `plan/research/q4-slashing-db.md`; `plan/issues/spike-notes.md` ## Q-2; `config/storage.toml:46`; `services/storage/src/write_behind.rs:70,653-657`; `crates/store/src/engine/redb.rs:19-24,476-493`; `crates/store/tests/q2_redb_exclusive_open.rs`
- **Provenance:** new — [q4] plus the Q-2 exclusive-open spike (`S0a-B-10`)

This is **ADR-R-05**, not ADR-R-04. ADR-R-04 is the S1 liveness-probe record (*liveness is
proved by a deadline-bounded no-op through the consensus core*). `[PLAN]` X-3 is resolved
here: slashing protection keeps the §10.5 id.

## Context

The one ordering that prevents a slashing is **record → fsync → sign**: the durability
call must *return* before a signature exists. Sign-then-record leaves a crash window in
which the restarted client has no memory of a signature already on the wire; the next
conflicting duty is slashable and the evidence is permanent. Record-then-sign's failure
mode is one missed duty.

`services/storage`'s write-behind path cannot honour that ordering. It acknowledges
before it commits, by design, for up to **4 seconds** (`commit_max_latency_ms = 4000` at
`config/storage.toml:46`; `DEFAULT_COMMIT_MAX_LATENCY` at `write_behind.rs:70`) — two
thirds of a slot. And it has a path that **claims a failed flush durable**: on a P0 HEAD
flush error the unit is logged and dropped, the session keeps consuming, and the next
successful flush commits a later `WriteCursor`, so the failed unit's events are skipped
forever on resume (`write_behind.rs:653-657` = [PRD] P0-13). Either inverts
record-before-sign. After S2 folds storage into `beacon-core` the *accumulator* remains;
the process boundary is not the disqualifying part.

That path is the correct contract for an archive (a bounded loss window). The required
loss window for slashing protection is **zero**. Performance is not an argument for
sharing it: one fsync per signing duty is ~1 attestation per validator per epoch plus
rare blocks.

**Contrast with P0-19/4 — write this into the crate's module docs so the two cases are
never conflated.** The pubkey cache *is* reconstructible from the state's validator
registry, so losing it is a slow boot and it **is** safe on the write-behind path
([q3] Change 4). Slashing-protection history is **not** reconstructible from consensus
state: a lost record *is* the slash. Optional persistence of the pubkey cache in
`cc-store` (`patch @ S2`) must not be read as a precedent for parking slashing records
there.

Phase 5–7 contracts are still stubs (`services/attestation` is an ~85-line stub today).
The decision is therefore cheap now and expensive once someone reaches for the convenient
archive write path. Implementation stays S5; this record is what stops that reach.

## Decision

A dedicated leaf crate **`crates/slashing-protection`** (workspace package
`cc-slashing-protection` when it lands), depending only on **`cc-types`** among
workspace crates — beside `crates/crypto`, not under it; no `cc-store`, no `cc-proto`,
no service crate. Enforce that the way the repo already enforces *"only `cc-libp2p` may
depend on `libp2p*`"* (`scripts/check-crate-dag.sh`). The crate has **its own redb
file**; it does not open the archive database and does not reuse `cc-store::Engine`.
`redb` itself is an allowed third-party dependency of this crate, not a reason to take
`cc-store`.

At S5 the crate **moves with the signer**, not the beacon node. The EIP-3076 DB belongs
to whatever holds the keys (validator client or remote signer).

**Record-then-sign is the only path.** There is no public bare `check()`. Check and
insert are one atomic exclusive operation:

```rust
pub fn check_and_insert_block_proposal(
    &self, pubkey: &BlsPublicKey, slot: Slot, signing_root: Root,
) -> Result<Safe, NotSafe>;

pub fn check_and_insert_attestation(
    &self, pubkey: &BlsPublicKey, source: Epoch, target: Epoch, signing_root: Root,
) -> Result<Safe, NotSafe>;

pub fn export_interchange(&self, gvr: Root) -> Result<Interchange, Error>;
pub fn import_interchange(&self, i: &Interchange, gvr: Root) -> Result<(), Error>;
```

**Store the complete form; export the minimal form.** On disk: every signed block
`(validator_id, slot) → signing_root` and every signed attestation
`(validator_id, target_epoch) → (source_epoch, signing_root)`, plus
`validators(id, public_key UNIQUE, enabled)` and
`metadata(genesis_validators_root, interchange_format_version)`. A minimal EIP-3076
export is the three floors per key — `MAX(slot)`, `MAX(source_epoch)`,
`MAX(target_epoch)` — which the EIP's "take the maximum" rule sanctions and which are
strictly more conservative than `is_slashable_attestation_data`. Starting complete and
exporting minimal is easier than the reverse; a minimal export after import or a long
outage refuses some non-slashable signatures (missed duties, never a slashing).

Five non-negotiables, to be written into the crate's module docs as a contract:

1. **Record → fsync → sign.** The durability call **returns** before the API returns
   `Safe`. `Durability::Immediate`, or `Paranoid` → `Immediate` +
   `set_two_phase_commit(true)` (both already exist at
   `crates/store/src/engine/redb.rs:476-493`). Never `Durability::None`, never an
   accumulator, never a background flusher.
2. **Check and insert are one transaction.** No public bare `check()`.
3. **Fail-fast exclusive file lock** held for the process lifetime. A second opener
   must **error**, not block. This is what catches "operator started two validator
   clients on one key."
4. **`genesis_validators_root` stored and checked** on every open and every import.
   GVR mismatch on import is refused.
5. **Fail-closed.** If it cannot commit, the signer refuses to sign (`FailClosedKzg`
   is the house precedent). It is not in the health DAG's degraded path.

**Backend: redb (Q-2 fail-fast confirmed).** Workspace pin **redb 4.1.0**
(`docs/storage-engine.md` V-4). `S0a-B-10` ran a two-process exclusive-open against
production `Engine::open` (`crates/store/tests/q2_redb_exclusive_open.rs`): the second
opener returned `StoreError::DatabaseLocked` in **12.5 ms** and exited 2; it did not
block. redb 4.1.0's `FileBackend` takes a non-blocking exclusive lock
(`file.try_lock()` → `TryLockError::WouldBlock` → `DatabaseError::DatabaseAlreadyOpen`);
`Engine` maps that to `DatabaseLocked` (`redb.rs:19-24`). An extra
`flock(LOCK_EX | LOCK_NB)` wrapper is not required on this workspace's targets.
The slashing crate will open **its own** redb file the same way; it does not call
`cc-store`.

SQLite (`rusqlite` + `POOL_SIZE = 1` + `locking_mode = EXCLUSIVE`, Lighthouse's
choice) is recorded as a **stated exception only**. It is not chosen. `rusqlite`
pulls `libsqlite3-sys` (C FFI) into a workspace that sets `unsafe_code = "deny"`
(`Cargo.toml:38`) and that picked redb after a documented falsifier exercise
(`docs/storage-engine.md`). Do not take SQLite because operator tooling expects a
`.sqlite` file, and do not take it because `[PRD]` P1-F/1 once named it — that line
cited [q4], and [q4] rejected SQLite as the default (`[PLAN]` R-18 / C-9). Revisit
SQLite only if a future redb major loses fail-fast exclusive open *and* an `flock`
wrapper proves awkward; record that here as the exception, not as a silent swap.

Out of scope of this record: doppelganger detection (complementary, not EIP-3076);
wiring into a validator client (S5); interchange-vector ingestion (S5).

## Consequences

What this makes easy:

- Record-before-sign is an API shape, not an operator procedure. Two racing sign
  requests cannot both pass a check and both sign.
- A second process opening the same DB fails immediately, which is the mechanical
  catch for two validator clients on one keystore.
- EIP-3076 import/export stays on the signer. A GVR-checked interchange is how a
  key moves; the archive is not involved.
- The crate DAG can forbid `cc-slashing-protection` → `cc-store` / any service the
  same way it forbids stray `libp2p*` edges.

What this makes hard:

- A second embedded store and a second file lock, implemented at S5 (~800–1200
  lines with the slashable-pair test matrix; [q4] sizes it **M**).
- Minimal export after import or a long outage refuses some non-slashable
  signatures. Operators pay missed duties, never a slashing.
- Anyone adding a "just persist this on the archive path" helper has to contradict
  a committed ADR, not just a comment in [q4].

What this forbids:

- Riding `services/storage` / `crates/storage-core` / `cc-store` for slashing
  records, including after S2 deletes the gRPC hop. Write-behind, eviction, prune,
  and snapshot-ring rotation must never touch this file.
- A public `check()` that is not also an insert.
- Sign-then-record, `Durability::None`, an accumulator, or a background flusher
  on this file.
- Choosing SQLite as the default backend.
- Conflating P0-19/4 (reconstructible pubkey cache; write-behind-safe) with this
  store (not reconstructible; write-behind-unsafe).

## Alternatives considered

**Reuse `services/storage` (or post-S2 `crates/storage-core`).** Rejected on the two
mechanisms in Context: 4 s acknowledgement-before-commit, and P0-13's
failed-flush-claimed-durable path. After S2 the accumulator is still there. The
archive's loss window is a feature; here it is the slash.

**SQLite, Lighthouse-shaped** (`rusqlite` + `r2d2`, `POOL_SIZE = 1`,
`locking_mode = EXCLUSIVE`, `TransactionBehavior::Exclusive`). A genuinely
reasonable alternative, and the one operator tooling expects. Rejected as the
default because it pulls C FFI into a `unsafe_code = "deny"` workspace that already
falsified redb 4.1.0 for storage. **Stated exception only** — see Decision.

**Minimal-only on-disk store** (three integers per validator, no per-slot /
per-target history). Compatible with the EIP floors and enough to refuse slashable
pairs after a fresh import. Rejected as the *stored* form: complete history is
richer diagnostics and a strictly easier export (max-reduce) without weakening
floors. Minimal remains the interchange *export*.

**redb, but via `cc-store::Engine`.** Rejected. `cc-store` is the archive engine
(pruning, rings, interned table names, write-behind callers). Sharing it couples
slashing records to a crate whose other users have a non-zero loss window. The
leaf crate takes `redb` directly.

**Defer the ADR until S5.** Rejected. The value is stopping the convenient
write-behind reach before any Phase 5 code is written against the existing storage
contracts. Implementation cost is unchanged by writing this now.

## Refactor impact

**Created at S5, decided at S0.** No crate, no workspace member, and no DAG rule
land in this record — only the decision. S5 adds `crates/slashing-protection` on
the validator-client / remote-signer side of the standard BN↔VC boundary, with a
`check-crate-dag.sh` leaf rule (`cc-types` and no other workspace crate). Nothing
in S0–S4 may grow a slashing table on the archive path.

Q-2 is **answered** (`plan/issues/spike-notes.md` ## Q-2): redb fail-fast exclusive
open is confirmed. This ADR does not carry `Status: proposed` and does not say
"revisit when Q-2 lands."
