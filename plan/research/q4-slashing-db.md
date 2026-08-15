# Q4 — EIP-3076 slashing-protection DB shape

## Recommendation

A new leaf crate **`crates/slashing-protection`**, depending only on `cc-types`, with
**its own redb file** — never `services/storage`, never the archive database.

**Where it lives in the DAG.** Beside `crates/crypto`, not under it. No dependency on
`cc-store`, `cc-proto`, or any service crate; enforce that the way the repo already
enforces *"only `cc-libp2p` may depend on `libp2p*`"*. When Stage 5's
validator-client / remote-signer boundary lands, the crate moves with **the signer**,
not the beacon node — the EIP-3076 DB belongs to whatever holds the keys.
`services/attestation` is an 85-line stub today, so this boundary is free to get right
now and expensive later.

**Backend: redb, not SQLite** — despite Lighthouse. The workspace sets
`unsafe_code = "deny"`, ships a `deny.toml` and an exact-pin supply-chain policy
(`docs/supply-chain.md`), and picked redb 4.1.0 after a documented falsifier exercise
(`docs/storage-engine.md`). `rusqlite` pulls `libsqlite3-sys` — C FFI — into a codebase
that deliberately went pure-Rust for storage, to protect three integers per validator.
SQLite is the reasonable **alternative** (it is what operator tooling expects, and
`locking_mode = EXCLUSIVE` gives cross-process exclusion for free); take it as a stated
exception in the ADR, not by default.

**Five non-negotiables, to be written into the crate's module docs as a contract:**

1. **Record → fsync → sign.** The durability call **returns** before the API returns
   `Safe`. `Durability::Immediate`, or `paranoid` → `Immediate` +
   `set_two_phase_commit(true)`; `crates/store/src/engine/redb.rs:476-490` already has
   both. Never `Durability::None`, never an accumulator, never a background flusher.
2. **Check and insert are one transaction.** No public bare `check()` — if it exists,
   someone will call it, then sign, then insert.
3. **Fail-fast exclusive file lock** held for the process lifetime. A second opener must
   **error**, not block. This is what catches "operator started two validator clients on
   one key."
4. **`genesis_validators_root` stored and checked** on every open and every import.
5. **Fail-closed.** If it cannot commit, the signer refuses to sign — matching the house
   style (`FailClosedKzg` as precedent). It is not in the health DAG's degraded path.

**API — fused, one entry point per message kind:**

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

**Schema — store the complete form, export the minimal form.** Tables:
`validators(id, public_key UNIQUE, enabled)`,
`signed_blocks((validator_id, slot) -> signing_root)`,
`signed_attestations((validator_id, target_epoch) -> (source_epoch, signing_root))`,
`metadata(genesis_validators_root, interchange_format_version)`. Under redb each is a
`TableDefinition` with that key/value shape and the `MAX(...)` queries become reverse
range scans. Starting complete and exporting minimal is strictly easier than the
reverse — and a minimal export is just `MAX(slot)`, `MAX(source_epoch)`,
`MAX(target_epoch)` per key (§2).

**Performance is not an argument for sharing the archive path.** One fsync per signing
duty: ~1 attestation per validator per epoch (6.4 min), plus rare blocks. Even at 1,000
keys that is ~2.6 fsyncs/s on batched commits — orders of magnitude below what
write-behind was built to absorb. Worth stating explicitly, because it is the only
argument anyone will make for reusing storage.

---

## 1. Why `services/storage` is disqualified

The write-behind store is well-built. Every property that makes it good at archiving
consensus history makes it wrong here.

**It acknowledges before it commits, by design, for up to 4 seconds.**
`services/storage/src/write_behind.rs:1-8`: *"Accumulate events into a slot commit unit.
Flush on the first of: `HEAD` for slot `S+1`, `commit_max_events`, or
`commit_max_latency`."* Defaults: `commit_max_events = 64`,
`commit_max_latency_ms = **4000**` (`config/storage.toml:44,46`;
`services/storage/src/main.rs:212-215`). Two thirds of a slot in which a fact handed to
storage is not durable. The cursor exists to *bound the loss window* — the correct
contract for an archive; the required loss window for slashing protection is **zero**.

**There is a path where a lost write is claimed durable.**
`services/storage/src/write_behind.rs:653-657`:

```rust
if let Some(unit) = take_flush(&mut acc, session_id)
    && let Err(e) = flush_committed(writer, unit, &mut last_flushed).await
{
    error!(error = %e, "P0 commit failed on HEAD flush");
}
// … loop continues, unit dropped
```

The unit is logged and dropped, the session keeps consuming, and the next successful
flush commits a `WriteCursor` with a later seq — so the failed unit's events are
recorded as durable and skipped forever on resume (branch review, HIGH). For an archive
that is an unrecorded data-loss hole. For a slashing DB it is exactly the "we think we
recorded it, we did not" state that produces a slashing on the next restart.

**Two more.** It is reached over gRPC from another process today, adding a reconnect
state machine and a session-cursor protocol between the signer and the durability point
— and after Stage 2 folds storage into `beacon-core`, the *accumulator* remains, which
is the disqualifying part, not the process boundary. And it accepts eviction, pruning
and ring rotation by intent (`prune/`, `snapshots.rs` ring depth 4, `durable_set.rs`);
none of that may ever touch records that must be retained for the life of a key.

## 2. Why record → fsync → sign is non-negotiable

Two orderings, one asymmetry.

**Sign, then record.** A crash between the signature leaving the process and the record
reaching stable storage leaves the restarted client with **no memory** of that
signature. Its floors are stale; it signs a conflicting message for the same slot/epoch;
both are on the wire. Slashing, and the evidence is permanent and submittable by anyone.

**Record, fsync, then sign.** The same crash means a signature was recorded but never
produced. Exactly one **missed** duty.

With this repo's own constants
(`crates/state-transition/src/helpers/constants.rs:96-118`):

| Outcome | Cost |
|---|---|
| Missed attestation | ~one epoch of attestation reward — order 1e-5 ETH |
| Slashing | `effective_balance / MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA` (4096), **plus** a correlation penalty scaled by `PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX = 3`, **plus** forced exit, **plus** `WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA` to the reporter, **plus** all future rewards |

Three to four orders of magnitude, and one side is irreversible. The correct ordering is
the one whose failure mode is the cheap one — and it must hold across process crash,
host crash and power loss, which is why "written" is insufficient and "fsync returned"
is the requirement.

The corollary usually missed: check and record must be **one atomic step**. Two signing
requests racing between a passing check and a completed record both pass, and both sign.

## 3. What the standard and the reference implementation actually say

### EIP-3076 (<https://eips.ethereum.org/EIPS/eip-3076>, the EIP text, fetched 2026-08-15)

Four things that actually constrain the design:

1. **`interchange_format_version` is `"5"`, all numerics are strings** (the EIP is
   explicit: avoids JS 64-bit float limits), and **`signing_root` is optional** on both
   `signed_blocks` and `signed_attestations` entries. Its presence enables the
   "same data signed twice is not slashable" exemption; its absence forces a repeat to
   be treated as unsafe.
2. **GVR mismatch on import must be refused**, or a key is migrated onto the wrong
   chain's history. Hence non-negotiable #4.
3. **Minimal vs complete is in the EIP.** The construction rule for a minimal export is
   to *"take the **maximum** slot block and **maximum** source and target
   attestations"* — three integers per validator, enforcing: block `slot <= max` refused,
   attestation `source < max` refused, attestation `target <= max` refused. These floors
   are strictly **more conservative** than `is_slashable_attestation_data` (a
   strictly-increasing target cannot double-vote; a non-decreasing source with a
   strictly-increasing target cannot surround), so they cost missed duties after an
   import or long outage, never a slashing.
4. **The EIP contains no fsync or durability requirement.** Its only operational
   statement nearby is that export *"should only [be allowed] when the validator client
   or signer is stopped."* §2 is therefore an engineering argument, not a citation.

### Lighthouse (source tree, fetched 2026-08-15)

`validator_client/slashing_protection/src/{lib.rs,slashing_database.rs}`
(<https://github.com/sigp/lighthouse/blob/stable/validator_client/slashing_protection/src/slashing_database.rs>):

- SQLite (`slashing_protection.sqlite`) via `rusqlite` + `r2d2`, with **`POOL_SIZE = 1`**
  *"to enforce exclusive access across threads and processes."*
- `apply_pragmas` sets **`locking_mode = EXCLUSIVE`** — documented reason: prevent
  *"slashable data being checked and signed in parallel"* and lock out other processes.
- Every operation under **`TransactionBehavior::Exclusive`**.
- Fused API: `check_and_insert_block_proposal(&self, validator_pubkey, block_header,
  domain) -> Result<Safe, NotSafe>`, docstring: *"The checking and inserting happen
  atomically and exclusively. We enforce exclusivity to prevent concurrent checks and
  inserts from resulting in slashable data being inserted."*
- `Safe::{SameData, Valid}` encodes the exemption; `NotSafe::{InvalidBlock,
  InvalidAttestation, UnregisteredValidator, DisabledValidator, ConsistencyError, …}`.

Lighthouse stores the **complete** form, not the three-integer minimal one — richer, at
the cost of an unbounded-ish table. Either choice works; the invariants above do not
change.

---

## 4. Effort

| Piece | Size | Rationale |
|---|---|---|
| `crates/slashing-protection` — schema, `check_and_insert_*`, durability, locking | **M** | ~800–1200 lines with tests. The logic is small; the test matrix is not — every slashable pair (double block, double vote, surround, surrounded) × every boundary (equal slot with matching root, equal target with differing root, import floors). On redb, add the exclusive-open test: second opener must **fail**, not block. |
| EIP-3076 import/export + published interchange vectors | **S–M** | Mechanical, and the main correctness evidence. |
| Crash-safety test (kill -9 between sign and record) | **S** | Model on `devnet/faults.sh` and the existing CC-4N kill-9 clause. |
| Wiring into a validator client | **M** | Blocked on Stage 5; not on the Stage 0–4 critical path. |

**Total M — but the *decision* costs nothing** and should be recorded as an ADR now,
before any Phase 5 code is written against the existing storage contracts.

---

## 5. What I could not determine

- **Whether redb gives a *fail-fast* cross-process exclusive open.**
  `crates/store/src/engine/redb.rs` opens a database and redb takes a file lock, but I
  did not verify whether a second opener errors immediately or blocks — and "blocks" is
  not good enough for a signer. If it blocks, wrap the open in an explicit
  `flock(LOCK_EX | LOCK_NB)`. **This is the one thing to check before choosing redb over
  SQLite**, because it is the guarantee SQLite gives for free.
- **Whether Lighthouse sets `PRAGMA synchronous` explicitly.** The fetched
  `apply_pragmas` shows only `foreign_keys` and `locking_mode`. SQLite's default is
  `synchronous = FULL` in rollback-journal mode, which fsyncs on commit — so the
  behaviour is almost certainly right by default. But I did not see the pragma and did
  not verify `journal_mode` is left at `delete`; **WAL + `synchronous = NORMAL` would
  not fsync per commit.** Verify before citing Lighthouse as the fsync precedent.
- **The exact EIP-3076 wording on "minimal" vs "complete".** The EIP describes both and
  gives the "take the maximum" rule, which I quoted; whether it labels them with those
  terms in a normative section or in Rationale, I could not confirm from the fetched
  rendering.
- **Whether the published interchange test vectors are still hosted.** They were
  historically in `eth-clients/slashing-protection-interchange-tests`; I did not fetch
  that repository, and the effort estimate above assumes they exist.
- **Prysm / Teku / Nimbus / Web3Signer storage choices.** Verified Lighthouse only. Teku
  and Web3Signer are reputed to use a per-validator flat-file scheme rather than SQL,
  which would be a useful contrast for "minimal is enough" — I did not confirm it and am
  not asserting it.
- **Whether doppelganger detection should be in scope.** It is a complementary
  protection every major client ships, but it is not EIP-3076 and I did not research it.
  Flagged so it is a deliberate omission, not a gap.
- **Whether any Phase 5–7 proto contract presumes a storage-service home for slashing
  records.** `proto/` has no `slashing` hits, so probably not — but the stub contracts
  were only skimmed.
