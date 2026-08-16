# S1 — fold the EL bridge · wk 9–16

**Entry.** S0 exit criteria E0.1–E0.7 all met, under `make ci`.
**Ships.** 3 containers · engine-fastpath DA works end to end · real KZG replaces `kzg: None`.

**Moves.** `services/engine/{transport,jwt,state,version,errors,capabilities,config}.rs`, `methods/`,
`fastpath/` → `crates/engine-api`, **verbatim with tests**.
**Adds.** `cc-seam` (`ChainIngress`, `P2pEgress`, `SeamError`, the 11-test conformance suite) · **E1/E2
typed but not moved** (the R-1 discharge order) · the core-liveness probe (M8) · real KZG.
**Deletes.** `services/chain/src/engine_client.rs` (E3 and P0-15's surface), `EngineStream`'s engine
half, the `trusted_local` bool, `services/engine/src/{service,main,inject}.rs`.

**Out of scope** (`[PLAN]` §3/S1, `[ARCH]` §9.2):

- **Moving any transport.** E1/E2 are **typed only** — R-1's discharge order is types-then-transport.
- **Merging moved services into `beacon-core` as *modules*.** That deletes `check-crate-dag.sh`'s five
  named prohibitions (⟡ D-1).
- **Any direct `cc-chain` ↔ `cc-p2p` call not routed through `cc-seam`** — it forecloses D-2.
- **Adding `cc-chain` to `check-crate-dag.sh`'s JWT grandfather list** — that deletes the invariant.
- Storage work.

**The stage carries a second, concurrent deliverable: the S2 entry gate.** The ADR corpus (M11) and
the P2-E triage (M12) gate **S2 entry**, so scheduling them at S2 makes them a serial prefix to the
longest structural stage. They run as stream B's second half **inside S1** (`[PLAN]` §4, D8), and
they are an **S1 exit item** (E1.6), not an S2 start item.

Estimate provenance and the points scale are defined in [`s0a-gate-restoration.md`](s0a-gate-restoration.md).

---

## Issue index

### Stream A — consensus core (`crates/engine-api`, `cc-seam`, M8)

| Id | Title | pd | pts | Deps |
|---|---|---:|---:|---|
| `S1-A-01` | `crates/engine-api` skeleton **+ the `check-crate-dag.sh` JWT re-point (same PR)** | 1.5–2.5 | 3 | — |
| `S1-A-02` | Move `transport.rs` (three-lane) + `config.rs`, verbatim with tests | 2–3 | 5 | `S1-A-01` |
| `S1-A-03` | Move `jwt.rs`, `version.rs`, `errors.rs`, `capabilities.rs` | 1.5–2 | 3 | `S1-A-01` |
| `S1-A-04` | Move `state.rs` (the health machine), verbatim with tests | 1.5–2.5 | 3 | `S1-A-01` |
| `S1-A-05` | Move `methods/` + `fastpath/` | 2–3 | 5 | `S1-A-02..04` |
| `S1-A-06` | `services/engine/main.rs` → thin constructor; delete `service.rs`, `inject.rs` | 1.5–2 | 3 | `S1-A-05` |
| `S1-A-07` | `crates/seam` — `SeamError`, `ChainIngress`, `P2pEgress` with overflow doc contracts | 1.5–2 | 3 | — |
| `S1-A-08` | `impl InProcess` — bounded tokio mpsc + oneshot replies | 2–3 | 5 | `S1-A-07` |
| `S1-A-09` | `impl Ipc` — wrap today's tonic edge; the jittered reconnect loop stays inside it | 2–3 | 5 | `S1-A-07` |
| `S1-A-10` | Conformance 1/3 — policy **A**, `backpressure_surfaces_after_deadline` | 1.5–2 | 3 | `S1-A-08`, `S1-A-09` |
| `S1-A-11` | Conformance 2/3 — policies **B**, **C**, **D** | 1.5–2 | 3 | `S1-A-10` |
| `S1-A-12` | Conformance 3/3 — the remaining tests to 11; both impls in CI | 2–3 | 5 | `S1-A-11` |
| `S1-A-13` | `check-crate-dag.sh` — `cc-p2p ↛ cc-chain` and `cc-chain ↛ cc-p2p` prohibitions | 1.5–2 | 3 | `S1-A-07` |
| `S1-A-14` | P1-D/11 (S1 half) — type the stringly-typed cross-service contracts | 1.5–2.5 | 3 | `S1-A-07` |
| `S1-A-15` | M8 core-liveness probe — design + implementation | 2.5–3 | 5 | `S1-A-05` |
| `S1-A-16` | M8 — wire into the healthcheck; **ADR-R-04** | 1.5–2 | 3 | `S1-A-15` |
| `S1-A-17` | M8 — injected engine black-hole; **demonstrated red** (E1.2) | 1.5–2 | 3 | `S1-A-16` |
| `S1-A-18` | S1 testability test — `import → engine → fork-choice`, timeout ⇒ **deferral** | 2–3 | 5 | `S1-A-06` |
| `S1-A-19` | §9.0 A/B run + exit note (E1.1) | 2–3 | 5 | all |
| | **Stream A total** | **33–47.5** | **73** | |

### Stream B — edge & platform (engine rows) + **the S2 entry gate**

| Id | Title | pd | pts | Deps |
|---|---|---:|---:|---|
| `S1-B-01` | P1-A/25 — real KZG replaces `kzg: None` (**P0-16 engine half**) | 2–3 | 5 | `S1-A-05` |
| `S1-B-02` | P1-A/26 — production lane uses the `hoodi_blob_bound()` **test fixture** | 1–1.5 | 3 | `S1-A-05` |
| `S1-B-03` | P1-A/24 — block-branch `FetchBlobs` carries a zeroed KZG inclusion proof | 1.5–2 | 3 | `S1-B-01` |
| `S1-B-04` | P2-D/19 + P2-B/5 — the engine policy edges | 2–3 | 5 | `S1-A-05` |
| `S1-B-05` | **Gate 1** — the reconciliation mechanism itself (J-16). **First task.** | 1.5–2 | 3 | — |
| `S1-B-06` | **Gate 2** — the CI resolver gate | 1.5–2 | 3 | `S1-B-05` |
| `S1-B-07` | Gate (a) 1/4 — proto / build / CI / supply-chain ids (9) | 1.5–2 | 3 | `S1-B-05` |
| `S1-B-08` | Gate (a) 2/4 — chain and fork-choice ids (13) | 2–2.5 | 5 | `S1-B-05` |
| `S1-B-09` | Gate (a) 3/4 — p2p ids (8) | 1.5–2 | 3 | `S1-B-05` |
| `S1-B-10` | Gate (a) 4/4 — engine and store ids (13) | 2–2.5 | 5 | `S1-B-05` |
| `S1-B-11` | Gate (b) — `ADR-P3-16` → **ADR-R-03**. The highest-priority (b) row | 1–1.5 | 3 | `S1-A-01` |
| `S1-B-12` | Gate (b) — `ADR-P3-02` → ADR-R-03 supersession | 0.75–1 | 2 | `S1-A-16` |
| `S1-B-13` | Gate (b) — `ADR-P3-15`, the `trusted_local` KZG skip | 1–1.5 | 3 | `S1-B-01` |
| `S1-B-14` | Gate (b) — `ADR-P4-03`, chain relays column SSZ without decoding | 0.75–1 | 2 | `S1-B-05` |
| `S1-B-15` | Gate (b) — `ADR-P2-13`, per-task panic policy. **On the X1 path.** | 0.75–1 | 2 | `S1-B-05` |
| `S1-B-16` | Gate (b) — the remaining 7, with `Status: proposed` + *revisit at Sn* | 2.5–3.5 | 5 | `S1-B-05` |
| `S1-B-17` | Gate (c) — 3 stale citations deleted | 0.5 | 1 | `S1-B-05` |
| `S1-B-18` | **ADR-R-01** — typed handles with a stated overflow policy | 0.5–0.75 | 2 | `S1-A-07` |
| `S1-B-19` | **R-P2-triage** 1/2 — rows 2–20 (minus the 5 done at S0) | 2–3 | 5 | — |
| `S1-B-20` | **R-P2-triage** 2/2 — rows 22–35 | 2–3 | 5 | — |
| `S1-B-21` | M3 ledger — `Discharged by` maintenance for S1 rows | 0.5 | 1 | — |
| `S1-B-22` | **Spike Q-1** — `check-crate-dag.sh` allowlist minimality | 0.5–1 | 2 | — |
| | **Stream B total** | **29.25–40.75** | **71** | |

**Phase totals.** **62.25–88.25 pd · 144 pts.** Of which the S2 entry gate is **`S1-B-05` … `S1-B-21`
= 22.25–30.25 pd**, against `[PLAN]` §4's **19–32 pd** and §3/S1's **19–26 pd**.

**Parallel duration, 2 streams.** Stream A binds at 33–47.5 pd → **8.25–11.9 wk** at 4 effective
pd/engineer-week, against `[PLAN]`'s **7–9 wk**. See the drift note.

---

## Stream A — the engine fold

### `S1-A-01` · `crates/engine-api` skeleton + the JWT re-point

**Stream** A · **Est** 1.5–2.5 pd / **3 pts** (≈) · **Discharges** ⟡ D-10, part of P1-E/S1

**This issue cannot be split, and the reason is a security invariant.** `[ARCH]` §6.2: the
`check-crate-dag.sh` JWT rule must name `cc-engine-api` **in the same PR that creates the crate**. A
stage that lands `crates/engine-api` without it has **silently deleted a mechanically-enforced
invariant** — the rule today says *only `cc-engine` may declare an HTTP client or JWT signer*
(ADR-P3-16 ✓ `docs/supply-chain.md:121`, `scripts/check-crate-dag.sh:202`), and a new crate holding
the JWT signer with no rule naming it is an invariant that evaporates rather than one that is
re-decided.

**Touch points**
- `crates/engine-api/Cargo.toml`, `src/lib.rs` (new)
- `scripts/check-crate-dag.sh:202` — the JWT/HTTP-client rule
- `Cargo.toml:3-27` — workspace members (20 today ✓)

**Acceptance (falsifiable)**
1. [x] `check-crate-dag.sh` names `cc-engine-api` in the JWT rule (E1.4).
2. [x] **`cc-chain` is NOT on the grandfather list** (E1.4, and `[ARCH]` §9.2's S1 prohibition).
3. [x] A deliberate test edit adding an HTTP client to a third crate makes the script fail —
   demonstrated by `scripts/fixtures/check-crate-dag/expect-fail/third-crate-reqwest/` (self-test).
4. [x] `S1-B-11` (ADR-R-03) is opened in the same sprint; the rule change without the record is half the
   deliverable. (Same-sprint record; ADR not written in this issue.)

---

### `S1-A-02` … `S1-A-06` · The verbatim move

**Combined** 8.5–12.5 pd / **19 pts** (≈ — a move, but the JWT / health-machine / three-lane transport
is delicate) · **Discharges** P1-E/S1

**"Verbatim with tests" is the review contract.** Any behavioural change in these five issues is a
review-stopper and belongs in a separate PR. `[ARCH]` §2.4/3: *"the diff must show the queue bound
moving, not being re-derived. A new numeric literal in a moved file is a review-stopper."*

| Id | Moves | pd | Note |
|---|---|---:|---|
| `S1-A-02` | `transport.rs`, `config.rs` | 2–3 | the three-lane transport and `TransportTimeouts` ✓ (`crates/engine-api/src/config.rs:37-67`) — the EL's own HTTP timeouts already modelled here become the callee-side overflow story for E3 |
| `S1-A-03` | `jwt.rs`, `version.rs`, `errors.rs`, `capabilities.rs` | 1.5–2 | `errors.rs:6` carries ADR-P3-09 (*the transport never retries `newPayload`*), which S1's deadline design depends on — do not "improve" it |
| `S1-A-04` | `state.rs` (health machine) | 1.5–2.5 | |
| `S1-A-05` | `methods/`, `fastpath/` | 2–3 | `fastpath/cells.rs:1` carries ADR-P3-12 (cell extension on the blocking pool); `fastpath/filter.rs:346` carries ADR-P3-15, the `cfg(test)`-only `verify_cell_kzg_proof_batch` → `S1-B-13` |
| `S1-A-06` | `services/engine/main.rs` → thin constructor over `cc-engine-api`; delete `service.rs`, `inject.rs` | 1.5–2 | **`services/engine` stays a workspace member** so the 4-container topology can still be run for A/B (`[ARCH]` §9.1) |

**Acceptance (`S1-A-04` only)**
- [x] `state.rs` lives in `crates/engine-api`, verbatim with tests
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `cargo test -p cc-engine-api` covers the moved health-machine tests
- [x] `services/engine` compiles via `#[path]` from `cc-engine-api`

**Acceptance (`S1-A-02` only)**
- [x] `transport.rs` and `config.rs` live in `crates/engine-api`, verbatim with tests
- [x] Queue bounds moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `cargo test -p cc-engine-api` covers the moved tests
- [x] `services/engine` compiles via `pub use` from `cc-engine-api`
- [x] `cc-engine-api` appended to `cc-engine` `allowed_deps`

**Acceptance (`S1-A-03` only)**
- [x] `jwt.rs`, `version.rs`, `errors.rs`, `capabilities.rs` live in `crates/engine-api`, verbatim with tests
- [x] Files moved (git rename), not rewritten; no new numeric literal in a moved file; `errors.rs` retry taxonomy unchanged (ADR-P3-09)
- [x] `cargo test -p cc-engine-api` covers the moved tests
- [x] `cc-engine` re-exports `capabilities` via `pub use cc_engine_api::capabilities`; jwt / errors / version stay `#[path]` so `JwtSecret` is not crate-public (ADR-R-03) and metric / method types stay in `cc-engine` until A-05
- [x] `cc-engine-api` declares `jsonwebtoken`; grep asserts no public `JwtSecret` / `pub mod jwt`
- [x] A-02 test-only stubs `test_jwt.rs` / `test_errors.rs` / `test_version.rs` removed

**Acceptance (`S1-A-05` only)**
- [x] `methods/` and `fastpath/` live in `crates/engine-api`, verbatim with tests
- [x] Bounds / literals moved (git rename), not re-derived; no new numeric literal in a moved file
- [x] `cargo test -p cc-engine-api` covers the moved method and fastpath tests
- [x] `cc-engine` re-exports `methods` and `fastpath` via `pub use` from `cc-engine-api`
- [x] `cc-engine-api` exports the real modules (not test-only `#[path]` stubs)
- [x] A-02/A-04 test-only stubs `test_methods.rs` / `test_metrics.rs` / `test_capabilities.rs` removed
- [x] `cc-types` / `cc-crypto` appended to `cc-engine-api` `allowed_deps`

**Also deleted in `S1-A-06`** — `services/chain/src/engine_client.rs` (the whole `block_on` bridge,
E3, P0-15's and P1-B/11's surface) and the `trusted_local` bool. E3 becomes a direct
`cc-engine-api` call from the core thread with an explicit `Duration` argument.

**Acceptance for the group**
1. [x] `cargo test -p cc-engine-api` passes with the tests that moved with the code.
2. [ ] `services/chain/src/engine_client.rs` no longer exists; no `handle.block_on` remains on any
   chain→engine path.
3. [ ] The `trusted_local` bool and its proto comment recording the security residual ✓
   (`p2p.proto:240-249`) are gone.

---

### `S1-A-07` · `crates/seam` — the traits

**Stream** A · **Est** 1.5–2 pd / **3 pts** · ⌂ `[ARCH]` §2.1 specifies the shape ·
**Discharges** part of P1-D/11, the R-1 discharge order

Three properties, all load-bearing:
1. **The methods mirror today's proto `oneof` arms**, so the move is mechanical and reviewable
   against the `.proto` file.
2. **The error type is shared with the transport and names the overflow condition explicitly.** Not
   `Result<T, Box<dyn Error>>`, not `Option<T>`.
3. **The overflow policy is stated in the trait's doc contract.**

`SeamError` has exactly four variants — `Backpressure { bound, waited_ms }`, `Unavailable(String)`,
`InvalidArgument(String)`, `FailedPrecondition { reason }`. **Adding a variant is a contract change
and needs an ADR** (`[ARCH]` §2.1); write that sentence into the enum's doc comment.

**Overflow contracts, quoted into the traits**
- `ChainIngress::submit_gossip` MUST block up to `IMPORT_SEND_TIMEOUT` (2 s) and then return
  `Backpressure`. It **MUST NOT silently drop.** Implementations that cannot block must still surface
  `Backpressure`.
- `P2pEgress::publish` is **lossy by design** and returns `Ok(Published::Dropped)` rather than an
  error when the publish queue is full — preserving today's behaviour ✓
  (`services/p2p/src/service.rs:796`). That looseness is a deliberate, recorded decision (ADR-R-02),
  not an oversight.
- `P2pEgress::update_view` is an `ArcSwap` store — never blocks, never fails.

**Where backpressure lives: exactly one place per edge** — the bound on the receiving queue, named as
a constant, plus the `send_timeout` on the sending side. Both are already the house pattern:
`COMMAND_CHANNEL_CAPACITY = 64` ✓ (`core.rs:54`), `IMPORT_SEND_TIMEOUT = 2s` ✓ (`core.rs:57`), and
the nine named bounds in `services/p2p/src/channels.rs:29-45` ✓. **The handle does not introduce a
second bound**; it names the existing one in the trait doc so a reviewer can diff it.

**Acceptance** — `cc-chain` and `cc-p2p` name `Arc<dyn ChainIngress>` / `Arc<dyn P2pEgress>` and **no
transport type**; `cc-p2p` has zero dependency on `cc-proto` for this edge.

- [x] `crates/seam` (`cc-seam`) with `SeamError`, `ChainIngress`, `P2pEgress`
- [x] Overflow contracts quote existing bounds; no second bound
- [x] Workspace member + `allowed_deps` (`cc-seam` leaf; append on chain/p2p)
- [x] `cc-chain` / `cc-p2p` name `Arc<dyn ChainIngress>` / `Arc<dyn P2pEgress>`
- [x] No `InProcess` / `Ipc`; README records both stay buildable (`[ARCH]` §9.2)

---

### `S1-A-08` · `impl InProcess` · 2–3 pd / **5 pts** (≈)
Bounded tokio mpsc + oneshot replies. The Single Hull candidate.

- [x] `impl InProcess` — bounded tokio mpsc + oneshot replies
- [x] Overflow: `submit_gossip` / `notify_data_available` → `Backpressure` after `IMPORT_SEND_TIMEOUT`; `publish` is lossy `Published::Dropped`; `update_view` never fails
- [x] No new `SeamError` variants; no `Ipc`
- [x] Crate README: both impls stay buildable permanently (`[ARCH]` §9.2)
- [x] Unit tests: Backpressure / Dropped
- [x] Docs: InProcess lanes **are** the live Single Hull queues; do not wrap in front of Loop B / event ring / `publish_fwd`

### `S1-A-09` · `impl Ipc` · 2–3 pd / **5 pts** (≈)
Wraps today's tonic-over-TCP edge; the S3 option is a unix socket + `SO_PEERCRED`. **The jittered
reconnect loop stays inside this impl** (`services/p2p/src/chain_stream/client.rs`) — it is the repo's
best distributed-systems code and the thing X3 asks about.

**`[ARCH]` §9.2 / `[PLAN]` R-14: neither impl is ever deleted.** Both stay buildable permanently; the
losing one is demoted to a test fixture, because it is the only way the conformance suite stays
honest. Write that into the crate's README now, not at S3.

---

### `S1-A-10` … `S1-A-12` · The conformance suite

**Combined** 5–7 pd / **11 pts** · ⌂ `[ARCH]` §2.2 · **Discharges** E1.3

`[ARCH]` §2.2 records **four distinct overflow policies** on internal edges today, one of which is a
correctness bug. They must be preserved **individually**; collapsing them to one policy at the fold
would be exactly the silent change R-1 predicts.

| Policy | Today's trigger | Caller-visible signal today | Post-move signal | Conformance test | Issue |
|---|---|---|---|---|---|
| **A** blocking with deadline | `cmd_tx.send_timeout(cmd, 2s)` on a 64-deep channel ✓ `core.rs:241,278,307,332,365` | gRPC `RESOURCE_EXHAUSTED` + `cc_chain_import_rejected_backpressure` | `SeamError::Backpressure` | `backpressure_surfaces_after_deadline` — fill the queue, assert the variant **and** that it took ≥ 2 s | `S1-A-10` |
| **B** try_send, drop the *subscriber* | per-subscriber `mpsc(256)` ✓ `events/mod.rs:34-35,91,599-601` | stream terminated `RESOURCE_EXHAUSTED`; consumer reconnects with cursor | unchanged for the API/observer bus; **N/A** for storage after S2 | `events::slow_subscriber_is_terminated_not_stalled` (**exists today; keep it**) | `S1-A-11` |
| **C** try_send, drop the *message*, log | publish queue → swarm cmd queue full ✓ `service.rs:794-797` | `error!(...)`, **no caller signal** | `Ok(Published::Dropped)` — **a value, not a log line** | `publish_drop_is_observable` | `S1-A-11` |
| **D** try_send, drop *silently* | `SlotTick` into the full 64-deep channel ✓ `core.rs:527-532` | **none** | **deleted** — the tick has its own never-shed lane | `core::slot_tick_is_never_shed` | `S1-A-11` (assert it, landed at `S0-A-14`) |

**Under-specified — flag in `S1-A-12`.** `[PLAN]` and `[ARCH]` both say **11 tests**. `[ARCH]` §2.2
names **four**. The remaining **seven are unnamed in either source.** Do not invent them silently:
`S1-A-12`'s first deliverable is the enumerated list of 11, reviewed before any is written, with each
test traced to a trait method or an overflow row. If the honest count is not 11, say so and correct
the figure in `[ARCH]` §2.1.

**Acceptance for the group** — `cargo test -p cc-seam` passes against **both** impls (`InProcess` and
`Ipc`), not one, and CI runs both (E1.3).

---

### `S1-A-13` · `check-crate-dag.sh` — the `cc-chain` ↔ `cc-p2p` prohibitions

**Stream** A · **Est** 1.5–2 pd / **3 pts** · ⌂ `[ARCH]` §2.5 · **Discharges** R-7's mitigation

**Constraint stated as a check** (`[ARCH]` §2.5): *no PR in S0–S2 may introduce a call from `cc-chain`
to `cc-p2p` or vice versa that is not routed through a `cc-seam` trait.* This is enforceable today by
adding two explicit named prohibitions in the style of the existing `services/storage ↛
cc-fork-choice` rule ✓ (`scripts/check-crate-dag.sh:48-58`).

**Acceptance**

1. [x] Both directions named (`cc-chain` ↛ `cc-p2p` and `cc-p2p` ↛ `cc-chain`).
2. [x] A deliberate test edit adding either dependency fails — fixture self-test
   (`scripts/fixtures/check-crate-dag/expect-fail/chain-depends-p2p/`,
   `expect-fail/p2p-depends-chain/`).
3. [x] The rule runs locally via existing `make lint` / `check-crate-dag` (already in
   the ci job list). No new CI job id.

---

### `S1-A-14` · P1-D/11 (S1 half) — type the stringly-typed contracts

**Stream** A · **Est** 1.5–2.5 pd / **3 pts** (≈) · **Discharges** P1-D/11 (S1 half; the S2 half is
`S2-A-08`)

R-1's failure mode is **silent**, so there is no report to fix it from (ADR-R-01's rejected
alternative). Type the contracts **before** moving them.

**Targets** — verdict reason strings; `FCU_DROPPED_STALE:` prefixes; hand-rolled event byte-offsets
with silent-default fallbacks. The most consequential instance of the last is
`services/storage/src/write_behind.rs:763-770` (`column_index_at_offset(&ssz).unwrap_or(0)`, then a
2-byte LE read, then `0` — a short or malformed payload is **durably stored as column index 0**), but
**that one is deleted at S2** by the typed ingest path (`S2-A-05`), not typed here. Do not patch it.

**Acceptance** — every cross-service string discriminant on an S1-moved surface is an enum; a grep for
the named prefixes outside test code returns 0.

- [x] Verdict reason strings on the import → p2p-stream seam are `ImportReason` (not ad-hoc equality)
- [x] `FCU_DROPPED_STALE` is `EngineRpcReason::FcuDroppedStale` via `ErrorInfo`; no `FCU_DROPPED_STALE:` message prefix in production
- [x] S1 event payloads (`BLOCK_IMPORTED` disc., `CHAIN_REORG`, `FINALIZED_CHECKPOINT`) encode/decode as typed layouts; unknown discriminants fail closed
- [x] `services/storage/src/write_behind.rs` column_index `unwrap_or(0)` left for `S2-A-05` / `S2-A-08`

---

### `S1-A-15` … `S1-A-17` · M8 — the core-liveness probe

**Combined** 5.5–7 pd / **11 pts** (≈) · ⌂ `[ARCH]` §7.2 · **Discharges** M8, R-4, E1.2

**Why it ships at S1 and not "before S3".** `[ARCH]` §9.1 puts it at S1 — earlier than `[PRD]` R-4
requires. The health DAG is green for the one failure it cannot heal and red for the one compose
already restarts (`[ARCH]` §7.1). A parked consensus core reports healthy **today**.

- `S1-A-15` (2.5–3 pd) — a **deadline-bounded no-op through the consensus core**. The deadline is the
  soft deadline substituted from `ATTESTATION_DUE_BPS` × slot duration (ADR-P3-13 ✓
  `services/engine/src/metrics.rs:8`), which `[ARCH]` §7.2 names as the relevant precedent.
- `S1-A-16` (1.5–2 pd) — wire into the healthcheck; commit **ADR-R-04** (*liveness is proved by a
  deadline-bounded no-op through the consensus core*).
- `S1-A-17` (1.5–2 pd) — the injected engine black-hole harness.

**Acceptance (`S1-A-15` only)**

- [x] Probe issues a no-op through the consensus core (`CoreCommand::Ping` / `TickWork::Ping` on the
      never-shed tick lane — not `grpc-health-probe`, not `GetHead`)
- [x] Deadline is `ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000` (ADR-P3-13); Hoodi = 3999.6 ms
- [x] `probe_core_liveness` is callable from a later healthcheck (`S1-A-16`); compose / tonic
      aggregate / `local_ready` unchanged
- [x] Unit test: deadline miss is observable (`tokio` test-util + a fake core that never answers)

**Acceptance (E1.2, falsifiable)** — **M8 demonstrated red** against an injected engine black-hole,
with the healthcheck output pasted into the S1 exit note. A probe that has only ever been observed
green is the same failure shape as an X1 counter that has only ever returned 0. (`S1-A-17`)

---

### `S1-A-18` · S1 testability test

**Stream** A · **Est** 2–3 pd / **5 pts** · ⌂ `[ARCH]` §8.2 · **Discharges** the direct regression
test for P0-15

`import → engine → fork-choice` in one process, with a `newPayload` timeout asserting **deferral**,
not park. This re-runs `S0-A-27`'s assertion after the move: the deferral machinery exists
(`services/chain/src/pending_engine.rs`, ADR-P3-05's separate map) and the point of P0-15 is that it
could never trigger.

**Acceptance** — the test exists, runs in CI, and fails if `pending_engine` is bypassed.

---

### `S1-A-19` · §9.0 A/B run + exit note

**Stream** A · **Est** 2–3 pd / **5 pts** · ⌂ `[ARCH]` §9.0 · **Discharges** E1.1

Same procedure as `S0-B-19`, diffed against the S0 baseline.

**E1.1's blocker rule, stated as acceptance:** `cc_chain_import_result{*}` distribution and head-lag
buckets match the S0 baseline; **any non-zero diff in the overflow-counter family with zero diff in
the other two is a stage blocker, not a curiosity** (`[ARCH]` §9.0/4). It means behaviour is unchanged
at test load and the *contract* changed — which is precisely the R-1 failure this stage's typing work
exists to prevent.

**Also in the exit note** — E1.5: 3 containers run; engine-fastpath DA works end to end on
self-devnet.

---

## Stream B — engine rows

### `S1-B-01` · P1-A/25 — real KZG replaces `kzg: None`

**Stream** B · **Est** 2–3 pd / **5 pts** (≈) · **Deps** `S1-A-05` · **Discharges** P1-A/25,
**P0-16's engine half**

The production binary passed `kzg: None` (`services/engine/src/main.rs`), so **the entire
CC-37b getBlobs fastpath was dead**. P0-16's disposition is `wire @ S3` with the **engine half landing
at S1** — this issue is that half.

**Acceptance**
1. [x] Production lane constructs a real `CellKzg` backend (`Some(production_cell_kzg()?)`), not `None`
2. [x] Unit/integration assertion: `production_cell_kzg()` is `Some` on a constructed `FastpathLane`
3. [x] Grep-backed assertion: `services/engine/src/main.rs` passes the `Some`-wrapped binding, not `None`
4. [ ] Self-devnet non-zero getBlobsV2 fastpath completions — scrape `cc_engine_getblobs_total{result="complete"}` (and `cc_engine_cells_computed`) after a six-service run. Not executed in this worktree. Note that W4's Phase-3 clause 4 makes *"non-zero complete **and** zero engine-sourced columns"* the pass condition and anything else a **FAIL** — so this issue's success is a precondition for `S3b-W-04`, not a substitute for it.

### `S1-B-02` · P1-A/26 — production lane uses a test fixture · 1–1.5 pd / **3 pts** (≈)
**Touch** `services/engine/src/main.rs:133` — the production lane uses `hoodi_blob_bound()`, a **test
fixture**, for the blob-count gate. Links P1-D/18 (test-harness state in production paths).
**Acceptance** — the bound comes from `ChainConfig` (the field added at `S0-A-07`); a grep for
`hoodi_blob_bound` outside `#[cfg(test)]` returns 0.

### `S1-B-03` · P1-A/24 — zeroed KZG inclusion proof · 1.5–2 pd / **3 pts** (≈)
**Touch** `services/chain/src/da.rs:395` — the block-branch `FetchBlobs` template always carries a
zeroed KZG inclusion proof in production.

**Acceptance**
1. [x] The proof is computed on the production block-branch `FetchBlobs` template (`block_branch_trigger_from_signed`)
2. [x] A test asserts a non-zero proof on the block branch and that verification against it passes

### `S1-B-04` · P2-D/19 + P2-B/5 — the engine policy edges · 2–3 pd / **5 pts** (≈)
`[PRD]` P2-D/19 is **19 engine policy edges**; the three named are: the fail-open fork-schedule
default `osaka_time=0`; the gate-bypassing Synced-edge fcU resend; terminal `AuthFailed` with no
operator escape. P2-B/5 is the documented transient fcU retry that is unreachable on the production
gated path (`crates/engine-api/src/methods/fcu.rs`, was `services/engine/src/methods/fcu.rs:324`).
**Under-specified:** `[PRD]` enumerates 3 of the 19 and gives no `file:line` for the other 16. Scope
this issue to the three named plus P2-B/5, and file the remaining 16 as a triage line item if the
count matters — do not silently claim 19.

**Triage (16 of 19 unscoped).** This issue does **not** claim P2-D/19 complete. Sixteen edges
have no `file:line` in `[PRD]` / `[AS]` §4 and stay a later triage line item. P2-E/15
(`prepare_fcu_params` version gate) rides this id but is not patched here.

**Acceptance**

- [x] Missing `[el_forks]` does not invent `osaka_time=0` (`require_el_fork_schedule`; constructors
      and `main.rs` fail closed). Explicit `osaka_time = 0` in config remains operator intent.
- [x] Synced-edge fcU resend is gated by `admits_el_call`; Offline / AuthFailed do not hit the EL.
- [x] `AuthFailed` stays terminal for `apply`; operator escape is process restart (runbook) or
      `operator_reset_auth_failed` → Offline.
- [x] Documented Transient fcU retry (`-32603`/`-32000`, once after 250 ms) is reachable on the
      production gated path. Same policy; no second retry.
- [x] 16 of 19 P2-D/19 edges remain unscoped (triage line item above). This issue does not claim 19.

---

## Stream B — the S2 entry gate (M11 + M12)

**The gate, restated (⟡ D-13).** *"Every cited id resolves to a committed document and the
reconciliation table has no unclassified rows"* — **not** "write 58 ADRs", which would stall the
stage. `[ARCH]` §10.1 measures the corpus at **207 citations / 58 distinct ids / 541 `Architecture §`
occurrences / 0 ADR files**.

**R-17 — the gate as literally worded is not satisfiable at S2 entry, and this is the resolution.**
At least three (b)-class ids are decided by *later* stages: `ADR-07` is *"decided again at S3"*,
`ADR-P1-04` is *"supersede at S4a"*, and `ADR-R-02` is *"created at S2"*. **A resolving document may
carry `Status: proposed` plus an explicit "revisit at Sn" line** — the MADR format already has the
field. Without stating this, someone either stalls S2 or fudges the gate.

**Bucket counts.** Sized off the **enumerated** §10.4 table: **(a) 43 · (b) 12 · (c) 3** = 58.
`[ARCH]` §10.4's own totals line and ⟡ D-13/§9.1 say **42/12/4**; `[PLAN]` X-1 flags the ±1 and does
not adjudicate it. The enumerated table is the artifact the work is done against, so it is the one
used here.

---

### `S1-B-05` · Gate 1 — the reconciliation mechanism itself

**Stream** B · **Est** 1.5–2 pd / **3 pts** · ⌂ `[PLAN]` §4 (J-16) · **This is the first task, not the
last** · **Blocks** every other gate issue

`[PRD]` J-16: **the proposed mechanism for discharging D-6 does not exist yet.** `[ARCH]` cites its
own §10 ten times — and the document reproduced the exact bug class D-6 describes, one revision after
the class was documented. Three of the ids the corpus now needs (`ADR-R-02/03/04`) were created by the
document that proposes to reconcile them.

**Deliverables**
1. `docs/adr/` with the MADR format from `[ARCH]` §10.2 and the id scheme from §10.3.
2. The reconciliation table committed as a **file**, not a section of a design doc, so the CI gate
   (`S1-B-06`) has something to resolve against.
3. A `docs/adr/README.md` recording the **8 never-cited ids** — `P1-01`, `P1-02`, `P1-03`, `P1-06`,
   `P2-01`, `P2-03`, `P2-12`, `P4-02` ✓. **These need no ADR** — an uncited id is not an unresolvable
   citation — and recording the fact is what stops the next reader hunting for them.
4. Adopt **748 occurrences** as the M11 baseline and state the unit as *occurrences*, not lines
   (`[PLAN]` X-5: 541 `Architecture §` + 207 ADR). This settles `[PRD]` J-15's ±1 id / ∓3 citation
   disagreement as a side effect.

- [x] `docs/adr/` MADR format (`[ARCH]` §10.2) and id scheme (§10.3); house files stay `docs/adr/<id>.md`.
- [x] Reconciliation table file enumerates the 58 ids (43 a / 12 b / 3 c); parseable columns.
- [x] `docs/adr/README.md` records the 8 never-cited ids; they need no ADR.
- [x] M11 baseline is 748 occurrences (541 `Architecture §` + 207 ADR); unit is occurrences, not lines.

---

### `S1-B-06` · Gate 2 — the CI resolver gate

**Stream** B · **Est** 1.5–2 pd / **3 pts** · ⌂ `[PLAN]` §4 · **Deps** `S1-B-05`

**Without it the corpus re-diverges the week after the gate passes.**

**Acceptance (falsifiable)**
1. A script extracts every `ADR[ -]<id>` **and** `Architecture §<n>` occurrence in the tree. It must
   catch the **spaced** spelling (`ADR P3-02`) — `[PRD]` M11 records that revision 1 missed it and
   undercounted the corpus by ~4.5×.
2. The script **fails** when an id does not resolve under `docs/adr/`.
3. Added to the `S0a-B-04` `make ci` job list.
4. A deliberate test citation of a nonexistent id fails CI, demonstrated in the PR.

---

### `S1-B-07` … `S1-B-10` · Gate (a) — the 43 re-derivable ids

**Combined** 7–9 pd / **16 pts** · ⌂ `[ARCH]` §10.4 sizes each at **~1 h** *(that is `[ARCH]`'s
estimate, not a measurement — carried as such)* · **Deps** `S1-B-05`

**These parallelise across writers.** Each is written by reading the citation site; no new decision is
needed. Split by area so one writer holds one subsystem's context.

| Id | Ids | Count |
|---|---|---:|
| `S1-B-07` | `ADR-04`, `ADR-05`, `ADR-06`, `ADR-11`, `ADR-12`, `ADR-P1-14`, `ADR-P3-01`, `ADR-P4-11`, `ADR-P4-12` | 9 |
| `S1-B-08` | `ADR-P1-05`, `P1-07`, `P1-08`, `P1-09`, `P1-10`, `P1-12`, `P1-15`, `P2-04`, `P3-03`, `P3-04`, `P3-05`, `P3-10`, `P3-11` | 13 |
| `S1-B-09` | `ADR-P2-02`, `P2-05`, `P2-06`, `P2-07`, `P2-08`, `P2-09`, `P2-14`, `P3-07` | 8 |
| `S1-B-10` | `ADR-P3-08`, `P3-09`, `P3-12`, `P3-13`, `P4-01`, `P4-04`, `P4-05`, `P4-06`, `P4-08`, `P4-09`, `P4-10`, `P4-13`, `P4-14` | 13 |

**Nine of these are marked "survives and is load-bearing" and must be written so a later stage cannot
quietly contradict them:** `ADR-P1-09` (fork-choice `Store` owned by value on a dedicated OS thread —
it is *why* Loop B keeps `max_workers = 1`), `ADR-P1-15` (latency SLOs off histogram buckets, never
quantile interpolation — §7.3's metric reshape must preserve it), `ADR-P2-04` (only BLOCK is
chain-authoritative — it is why `ChainIngress` is narrow), `ADR-P2-08` (KZG cross-sidecar batching),
`ADR-P2-14` (`earliest_available_slot` as one `AtomicU64` — **it replaces E6**), `ADR-P3-03` (exactly
one `verify_and_notify_new_payload` call site — why §4.2's `block_on` trace has a single terminus),
`ADR-P3-09` (the transport never retries `newPayload`), `ADR-P4-01` (redb behind an engine seam),
`ADR-P4-04` (single writer task + three-class priority mailbox — §4.3's post-move overflow policy *is*
this mailbox), `ADR-P4-12` (the prober's independent codec — why P0-06 exists), `ADR-P4-13`
(`I-node-id` — §4.2's boot policy).

**One (a) row carries a correction:** `ADR-P3-01` cites *"Phase 3 adds no workspace member"* alongside
a member count of **16**; the workspace has **23** today ✓ (`Cargo.toml:3-33`). Record the ADR **and**
fix the count in `docs/phase-3-acceptance.md` (live Member-count / CC-3K /8 row).

**Acceptance** — every id in the bucket resolves under `docs/adr/`; `S1-B-06`'s gate is green for
them.

##### `S1-B-07` · Gate (a) 1/4 — proto / build / CI / supply-chain ids

- [x] `docs/adr/ADR-04.md` house MADR; Status: accepted; protox + `tonic-prost-build`; generated `.rs` not checked in.
- [x] `docs/adr/ADR-05.md` house MADR; Status: accepted; `buf breaking` at `FILE`; messages never move file.
- [x] `docs/adr/ADR-06.md` house MADR; Status: accepted; bootstrap telemetry pin.
- [x] `docs/adr/ADR-11.md` house MADR; Status: accepted; spec-vector fetch re-hashes by default.
- [x] `docs/adr/ADR-12.md` house MADR; Status: accepted; `figment` is the config loader.
- [x] `docs/adr/ADR-P1-14.md` house MADR; Status: accepted; vendored `google.rpc` under `third_party`.
- [x] `docs/adr/ADR-P3-01.md` house MADR; Status: accepted; Phase 3 adds no member; live count **23**.
- [x] `docs/adr/ADR-P4-11.md` house MADR; Status: accepted; soak clause not discharged without the discharging stage.
- [x] `docs/adr/ADR-P4-12.md` house MADR; Status: accepted; **load-bearing** independent probe codec (why P0-06 exists); `cc-wire` forbidden.
- [x] `docs/phase-3-acceptance.md` Member-count row records Phase-3-close **16** and today's **23**.
- [x] `docs/adr/reconciliation.md` nine S1-B-07 rows are `accepted` → `docs/adr/ADR-*.md`.

##### `S1-B-09` · Gate (a) 3/4 — p2p ids

- [x] `docs/adr/ADR-P2-02.md` house-MADR; Status: accepted; swarm sole owner + dedicated OS-thread KZG pool.
- [x] `docs/adr/ADR-P2-05.md` house-MADR; Status: accepted; `ChainView` is chain-owned and pushed.
- [x] `docs/adr/ADR-P2-06.md` house-MADR; Status: accepted; snappy framing as gossipsub `DataTransform`.
- [x] `docs/adr/ADR-P2-07.md` house-MADR; Status: accepted; column-family weight 0.5 regardless of `cgc`.
- [x] `docs/adr/ADR-P2-08.md` house-MADR; Status: accepted; **load-bearing** KZG cross-sidecar batching + per-sidecar re-verify before penalise.
- [x] `docs/adr/ADR-P2-09.md` house-MADR; Status: accepted; score decay ticks; gossip score does not disconnect.
- [x] `docs/adr/ADR-P2-14.md` house-MADR; Status: accepted; **load-bearing** `earliest_available_slot` as one `AtomicU64` (replaces E6).
- [x] `docs/adr/ADR-P3-07.md` house-MADR; Status: accepted; publish/inject only custody-sampled / subscribed indices.
- [x] `docs/adr/reconciliation.md` eight S1-B-09 rows are `accepted` → matching `docs/adr/ADR-P*.md` paths.

##### `S1-B-08` · Gate (a) 2/4 — chain and fork-choice ids (13)

- [x] `docs/adr/ADR-P1-05.md` house MADR; Status: accepted; KZG verify is `Result<bool, _>`.
- [x] `docs/adr/ADR-P1-07.md` house MADR; Status: accepted; exhaustive `gossip_class` map.
- [x] `docs/adr/ADR-P1-08.md` house MADR; Status: accepted; `CheckpointContext` LRU capacity 8.
- [x] `docs/adr/ADR-P1-09.md` house MADR; Status: accepted; **load-bearing** — `Store` owned by value on a dedicated OS thread; Loop B `max_workers = 1`.
- [x] `docs/adr/ADR-P1-10.md` house MADR; Status: accepted; decode-free dedup probe; server recomputes on miss.
- [x] `docs/adr/ADR-P1-12.md` house MADR; Status: accepted; four pinned roles + 64-block body ring.
- [x] `docs/adr/ADR-P1-15.md` house MADR; Status: accepted; **load-bearing** — latency SLOs off histogram buckets, never quantile interpolation.
- [x] `docs/adr/ADR-P2-04.md` house MADR; Status: accepted; **load-bearing** — only BLOCK is chain-authoritative; `ChainIngress` stays narrow.
- [x] `docs/adr/ADR-P3-03.md` house MADR; Status: accepted; **load-bearing** — exactly one `verify_and_notify_new_payload` call site.
- [x] `docs/adr/ADR-P3-04.md` house MADR; Status: accepted; five `PayloadStatus` variants.
- [x] `docs/adr/ADR-P3-05.md` house MADR; Status: accepted; `pending_engine` separate from `pending_da`.
- [x] `docs/adr/ADR-P3-10.md` house MADR; Status: accepted; optimistic status from proto-array only.
- [x] `docs/adr/ADR-P3-11.md` house MADR; Status: accepted; walk reuses `remove_invalidated_subtree_weight`.
- [x] `docs/adr/reconciliation.md` those 13 rows are `accepted` → `docs/adr/ADR-*.md`.

---

### `S1-B-10` · Gate (a) 4/4 — engine and store ids (13)

**Stream** B · **Est** 2–2.5 pd / **5 pts** · ⌂ `[ARCH]` §10.4 · **Deps** `S1-B-05`

This bucket only. `S1-B-07`…`S1-B-09` stay with the combined section above.

Re-derived from the live citation sites. No new decision.

- [x] `docs/adr/ADR-P3-08.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P3-09.md` in house MADR; Status: accepted; transport never retries `newPayload`.
- [x] `docs/adr/ADR-P3-12.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P3-13.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P4-01.md` in house MADR; Status: accepted; redb behind an engine seam.
- [x] `docs/adr/ADR-P4-04.md` in house MADR; Status: accepted; single writer + three-class mailbox.
- [x] `docs/adr/ADR-P4-05.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P4-06.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P4-08.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P4-09.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P4-10.md` in house MADR; Status: accepted.
- [x] `docs/adr/ADR-P4-13.md` in house MADR; Status: accepted; `I-node-id` boot policy.
- [x] `docs/adr/ADR-P4-14.md` in house MADR; Status: accepted.
- [x] Load-bearing bodies cannot be quietly contradicted: P3-09 (no `newPayload` retry), P4-01 (redb seam), P4-04 (writer mailbox), P4-13 (`I-node-id`).
- [x] `docs/adr/reconciliation.md` those 13 rows are `accepted` → `docs/adr/ADR-*.md`.

---

### `S1-B-11` · Gate (b) — `ADR-P3-16` → **ADR-R-03**

**Stream** B · **Est** 1–1.5 pd / **3 pts** · ⌂ `[ARCH]` §6.2 / §10.4 / §10.5 · **Deps** `S1-A-01` ·
**Highest-priority (b) row**

`ADR-P3-16` as written (*only `cc-engine` may declare an HTTP client or JWT signer*) is
**falsified** now that `cc-engine-api` is the named crate. The dag rule landed in `S1-A-01`;
this issue is the record.

**Out of scope.** `ADR-P3-02` (engine is not a health peer) is `S1-B-12` and waits on
`S1-A-16`. This record does not partially supersede it.

- [x] `docs/adr/ADR-R-03.md` in house MADR; Status: accepted; supersedes `ADR-P3-16`.
- [x] Decision matches the landed rule: `cc-engine-api` (and transitional `cc-engine` until
      `S1-A-06`) may declare HTTP/JWT; `cc-chain` is **not** grandfathered for JWT.
- [x] `docs/adr/reconciliation.md` `ADR-P3-16` row is `superseded; superseded-by ADR-R-03` →
      `docs/adr/ADR-R-03.md`.

---

### `S1-B-12` … `S1-B-16` · Gate (b) — the remaining 11 that need a decision recorded

**Combined** 5.75–8 pd / **14 pts** · ⌂ `[ARCH]` §10.4, `[PLAN]` §4 sizes each 0.5–1 d

**These do not parallelise onto one writer.** `[PLAN]` §9: *"They look like 12 documents one writer
can produce. Each records a decision whose owner is the person who made it; they parallelise across
**people who know the subject**, not across writers."* Split below by owner, not by volume.

| Id | ADR | Why it needs a decision | Coupling |
|---|---|---|---|
| `S1-B-12` | `ADR-P3-02` → ADR-R-03 | *engine dials p2p; engine is deliberately not a health peer* — §7.1 shows **this is why a parked core reports green**. Defensible for a separate engine process; **void once the engine is in-process** | **must land with `S1-A-16`'s probe** |
| `S1-B-13` | `ADR-P3-15` | `verify_cell_kzg_proof_batch` runs **only under `cfg(test)`** in the fastpath ✓ (`fastpath/filter.rs:346`) — this is the `trusted_local` KZG-skip. S1 makes the caller the process, which changes the trust argument but **does not automatically make skipping correct** | with `S1-B-01` |
| `S1-B-14` | `ADR-P4-03` | *chain relays column SSZ without decoding* — right for a relay, wrong for an owner. S2 replaces it with a typed ingest | records the change S2 makes |
| `S1-B-15` | `ADR-P2-13` | per-task panic policy: **catch and restart rather than abort the process**. **Directly determines whether X1 is measurable** (⟡ D-11) — the ADR must state that the catch path gains a counter before S3 | **on the X1 critical path**; read by `S3a-B-19` |
| `S1-B-16` | the remaining 7: `ADR-07` (*re-decided at S3*), `ADR-09` (*re-decide at S3*), `ADR-P1-04` (*supersede at S4a* — milhouse changes it), `ADR-P1-11` (*storage stops being a consumer at S2*; survives for API consumers, consequences rewritten), `ADR-P2-10` (gossip topic scoring weight **0** on every topic; **P0-17a wires §5.6 scoring at S3** — the deferral ends, record the new weights), `ADR-P2-11` (`ColumnSidecar` contract-only, no producer; supersede at S2), `ADR-P3-14` (EL dependency is `service_started`, never `service_healthy`; re-decide when compose collapses at S2) | each carries `Status: proposed` + an explicit **"revisit at Sn"** line, per R-17 |

##### `S1-B-16` · Gate (b) — the remaining 7, `Status: proposed` + revisit at Sn

- [x] `docs/adr/ADR-07.md` house-MADR; Status: proposed · revisit at S3; p2p dials chain; health DAG roots at chain.
- [x] `docs/adr/ADR-09.md` house-MADR; Status: proposed · revisit at S3; `debian:bookworm-slim`, not distroless.
- [x] `docs/adr/ADR-P1-04.md` house-MADR; Status: proposed · revisit at S4a; cached state-root path; milhouse supersedes.
- [x] `docs/adr/ADR-P1-11.md` house-MADR; Status: proposed · revisit at S2; `session_id` + two cursor reasons; storage consumer ends.
- [x] `docs/adr/ADR-P2-10.md` house-MADR; Status: proposed · revisit at S3; P3/P3b weight 0; P0-17a records new weights.
- [x] `docs/adr/ADR-P2-11.md` house-MADR; Status: proposed · revisit at S2; `ColumnSidecar` contract-only; supersede with ADR-R-02.
- [x] `docs/adr/ADR-P3-14.md` house-MADR; Status: proposed · revisit at S2; EL `service_started`, never `service_healthy`.
- [x] `docs/adr/reconciliation.md` seven S1-B-16 rows are `proposed; revisit at Sn` → matching `docs/adr/ADR-*.md` paths.

**Id conflict to resolve in `S1-B-13` — flagged by this decomposition (`X-6`).** `[ARCH]` §10.4 routes
`ADR-P3-15`'s replacement decision to *"ADR-R-05"*, but §10.5's **`ADR-R-05` is the
slashing-protection record** (written at S0, `S0-B-14`). One of the two needs a new id. Recommend
`ADR-P3-15` take **`ADR-R-07`**, leaving §10.5's enumeration intact.

##### `S1-B-13` · Gate (b) — `ADR-P3-15`

- [x] `docs/adr/ADR-P3-15.md` in house MADR; Status: accepted; production
      fastpath skips `verify_cell_kzg_proof_batch` (`cfg(test)` only);
      in-process is **not** why the skip is correct (local EL spec SHOULD +
      cheap bind).
- [x] Successor id is `ADR-R-07` (X-6); not `ADR-R-05` (slashing /
      `S0-B-14`). File not written — this issue re-affirms the live rule.
- [x] `S1-A-06` / S3 must not silently delete the skip when the wire
      `trusted_local` bool / EngineStream dies.
- [x] `docs/adr/reconciliation.md` `ADR-P3-15` row is `accepted` →
      `docs/adr/ADR-P3-15.md`.

##### `S1-B-14` · Gate (b) — `ADR-P4-03`

- [x] `docs/adr/ADR-P4-03.md` in house MADR; Status: accepted; chain relays column SSZ
      without decoding (right for a relay, wrong for an owner); records the S2 typed
      ingest (`ColumnBatch` / `ArchiveWrite::ingest_columns`; `ADR-R-02` supersedes at
      `S2-A-07`).
- [x] `docs/adr/reconciliation.md` `ADR-P4-03` row is `accepted` →
      `docs/adr/ADR-P4-03.md`.

---

### `S1-B-15` · Gate (b) — `ADR-P2-13`

**Stream** B · **Est** 0.75–1 pd / **2 pts** · ⌂ `[ARCH]` §10.4 / ⟡ D-11 · **Deps** `S1-B-05` ·
**On the X1 critical path**; read by `S3a-B-19`

Per-task panic policy: **catch and restart rather than abort the process**. Directly determines
whether X1 is measurable (⟡ D-11) — the ADR must state that the catch path gains a counter before
S3.

- [x] `docs/adr/ADR-P2-13.md` house-MADR; Status: accepted; catch and restart rather than abort.
- [x] ADR states the catch path gains a counter before S3 (X1 measurable; ⟡ D-11).
- [x] `docs/adr/reconciliation.md` `ADR-P2-13` row is `accepted` → `docs/adr/ADR-P2-13.md`.

---

### `S1-B-17` · Gate (c) — the three stale citations

**Stream** B · **Est** 0.5 pd / **1 pt** · ⌂ `[ARCH]` §10.4

| Id | Site | Action |
|---|---|---|
| `ADR-P1-13` | `scripts/check-crate-dag.sh:100` | **the crate it governs no longer exists** — `cc-driver` retired at CC-28. Delete the citation; the removal note stays as a plain comment |
| `ADR-P3-06` | `proto/eth/p2p/v1/p2p.proto:23,253` | *"`EngineStream` is bidirectional so the topology stays at nine contracts rather than ten"* — **the premise is deleted.** "Number of contracts" stops being a design constraint when the contracts stop being transports. Delete at S1; **do not write the ADR as if it still binds** |
| `ADR-P4-07` | `services/chain/src/restore.rs:5` | deleted at S2. Write the ADR with `Status: superseded-by ADR-R-02` **so the history is legible**, then delete the citations with the code at S2 |

Note the enumerated table gives 3 (c) rows while §10.4's totals line says 4 — `[PLAN]` X-1. If a
fourth surfaces while doing this issue, record it rather than adjudicating the count silently.

**Extra sites of `ADR-P3-06` (same id, not a fourth (c) row).** `[ARCH]` n=5; the table
names `p2p.proto:23,253`. Also cited at `services/engine/src/inject.rs:3,609` and
`services/chain/src/da.rs:930` (reverse-direction / column-branch, not the "nine rather
than ten" sentence). Those citations deleted with the premise sites. Residual
"ninth contract" nicknames without an ADR id (engine/p2p module docs, `storage.proto`)
are not (c) rows. No fourth (c) **id** surfaced — X-1's 3-vs-4 is unadjudicated.

- [x] `scripts/check-crate-dag.sh` — `ADR-P1-13` citation deleted; cc-driver removal stays a plain comment.
- [x] `proto/eth/p2p/v1/p2p.proto` — `ADR-P3-06` "nine contracts" premise citations deleted; no ADR written.
- [x] Extra `ADR-P3-06` sites (`inject.rs`, `da.rs`) — same id; citations deleted.
- [x] `docs/adr/ADR-P4-07.md` house-MADR; Status: superseded-by ADR-R-02; restore citations left for S2.
- [x] `docs/adr/reconciliation.md` three (c) rows: P1-13 / P3-06 `deleted`; P4-07 `superseded; superseded-by ADR-R-02`.
- [x] No fourth (c) id surfaced.

---

### `S1-B-18` · **ADR-R-01** — typed handles with a stated overflow policy
**Est** 0.5–0.75 pd / **2 pts** · ⌂ `[ARCH]` §10.5 (*created at S1*) · **Deps** `S1-A-07`
Records the R-1 discharge order — types land, transport later — and names the rejected alternative
(*wait for a report of changed backpressure semantics*), whose failure mode is silent.

- [x] `docs/adr/ADR-R-01.md` in house MADR; Status: accepted; records R-1 discharge
      order (types land, transport later).
- [x] Rejected alternative named: wait for a report of changed backpressure
      semantics; failure mode is silent.
- [x] `docs/adr/reconciliation.md` records ADR-R-01 as accepted →
      `docs/adr/ADR-R-01.md` (not a 58-row census id; no supersession).

---

### `S1-B-19` · `S1-B-20` · **R-P2-triage** — M12 → 0

**Combined** 4–6 pd / **10 pts** · ⌂ `[PLAN]` §4 sizes the 35-row pass at 5–7 pd · **Discharges M12**

**One adversarial verification pass over all 35 P2-E rows**, each **promoted with a tier and
disposition** or **dismissed with a recorded reason**. Five (rows 1, 6, 8, 21, 31) are done at
`S0-B-17`; these two issues cover the remaining 30.

- `S1-B-19` — rows 2–5, 7, 9–20 (16 rows)
- `S1-B-20` — rows 22–35 (14 rows)

**Not schedulable as work until promoted** (`[PRD]` §5.3.2). A promoted row becomes a new issue filed
against its owning stage, and its estimate is **not** in this file's totals — that is the point of a
triage gate.

**Acceptance** — **M12 = 0**: zero rows in an unknown state; every promotion carries a disposition
from the `[PRD]` §5.0 vocabulary; every dismissal carries a reason. `[PRD]` §7.5's anti-metric applies
— a row cleared without a recorded judgement is not progress.

**Judged 2026-08-16** in `[PRD]` §5.3.2 (S1-B-19). Line numbers re-verified on
`develop` `4b3eff0`. This issue is a triage write: no code patch. `S1-B-20` still owns rows 22–35.

- [x] Seventeen P2-E rows (2–5, 7, 9–20) judged in PRD §5.3.2; none unknown. Issue header said 16; `9–20` is 12 rows.
- [x] Row 2 promoted P2 `patch @ S1` — `ADVERTISED_CAPABILITIES` still lists unimplemented `eth_chainId` and non-Engine `eth_syncing`; drop both when `S1-A-03` moves the file.
- [x] Row 3 promoted P2 `patch @ S4` — consolidation churn still saturates where the exit twin is checked; not a live Hoodi overflow.
- [x] Row 4 dismissed — `MerkleHasher::finish` cannot fail on 38×32-byte writes; `unwrap_or(ZERO)` is unreachable.
- [x] Row 5 dismissed — every `StateAccessError::from` site is `VariableList`, which only returns `OutOfBounds`.
- [x] Row 7 dismissed — lookahead miss still fails `process_block_header`; `unwrap_or` does not hide an import.
- [x] Row 9 dismissed — `committee_from_shuffling` has zero callers and is not re-exported.
- [x] Row 10 dismissed — private `block_to_epoch` copies are gone; sole helper is P1-B/10 / `S0-A-34`.
- [x] Row 11 dismissed — vestigial `last_epoch_start`; no behavior.
- [x] Row 12 dismissed — leftover `block::operations` export of a `get_attesting_indices` wrap; zero callers; not a private module.
- [x] Row 13 dismissed — identical `clear_proposer_boost_root` arms are the intended same mutation.
- [x] Row 14 promoted P2 `patch @ S1` — `pending_engine` still parks `signed.as_ssz_bytes()`; `pending_da` already uses arrival `request.ssz`.
- [x] Row 15 promoted P2 `patch @ S1` — `forkchoice_method_for(osaka_time)` cannot hit Amsterdam on a legal schedule; rides `S1-B-04`.
- [x] Row 16 promoted P2 `deleted @ S1` — fallback session id is `pid`; `inject.rs` dies at `S1-A-06`.
- [x] Row 17 dismissed — `p0_capacity_hint` and `map_put_error` have zero callers.
- [x] Row 18 promoted P2 `patch @ S2` — snapshot degrade `or_else` can pick a newer ring member while saying next-older.
- [x] Row 19 promoted P2 `patch @ S2` — store/parse `?` paths still skip `record_serve`.
- [x] Row 20 promoted P2 `deleted @ S2` — retry loop does not classify RPC errors; RestoreFromStore dies at S2.

##### `S1-B-20` judged 2026-08-16

Rows 22–30, 32–35 (**13**) judged in `[PRD]` §5.3.2. The work-item's "14" included
row 31, already judged at `S0-B-17` and not re-opened — the 22–35 set is now fully
known. Line numbers re-verified on `develop` `4b3eff0`. This issue is a triage
write: no code patch. Combined with `S1-B-19` and `S0-B-17`, **M12 = 0**.

- [x] Thirteen P2-E rows judged in PRD §5.3.2; none of 22–30, 32–35 left unknown.
- [x] Row 22 promoted P2 `patch @ S2` — file-wide `#![allow(dead_code)]` still on production prune + five children; unexplained (unlike `writer.rs`).
- [x] Row 23 dismissed — `seen` unreachable on the `0..NUMBER_OF_COLUMNS` walk; `services/storage/src/columns.rs` gone.
- [x] Row 24 dismissed — requested/returned vecs discarded; serve already `ResourceUnavailable` on a miss.
- [x] Row 25 promoted P2 `patch @ S3` — comment still claims sub-slot; period is `from_secs(seconds_per_slot)`.
- [x] Row 26 dismissed — outer `#[allow]` on `mod tests` is equivalent lint scope.
- [x] Row 27 promoted P1 `patch @ S3` — `find_node_predicate` still awaited inline; stalls shutdown / ENR / event drain.
- [x] Row 28 promoted P1 `patch @ S3` — `try_send` drop is silent; comment's retry/accounting is not implemented.
- [x] Row 29 dismissed — `# Panics` is stale; `min()` clamp is the live contract.
- [x] Row 30 dismissed — same test-module allow nit as row 26 (`sampling.rs`, `engine_stream/{inject,server}.rs`).
- [x] Row 32 dismissed — verdict is correct; only the success-chunk reason string says "empty success".
- [x] Row 33 promoted P2 `patch @ S1` — only crate inlining `serde_json` instead of `workspace = true`.
- [x] Row 34 dismissed — BLS creds discarded; GVR `unwrap_or_default` unreachable after `validators_push`.
- [x] Row 35 dismissed — verbatim private `Default` helpers; no divergence.

---

### `S1-B-21` · M3 ledger maintenance · 0.5 pd / **1 pt**
Carry the `Discharged by` column (`[PRD]` §5.0 / E0.9) forward for every row S1 patched or deleted,
with the commit SHA or the stage id. Zero blank cells for rows this stage claimed.

**Convention.** This is the S1 instance of the ~0.5 pd stage-exit line item `S0-B-18` writes down.
Also append `S1` to `CLAIM_STAGES` in `scripts/check-m3-discharged-by.sh` so a blank S1-claimed cell
is a CI failure, not a review note. That is the program's *no-P0-vanishes-silently* check.

- [x] `S1` appended to `CLAIM_STAGES` in `scripts/check-m3-discharged-by.sh`.
- [x] `[PRD]` §5.1/§5.2 `Discharged by` filled for every S1-claimed row (owning issue id until SHA lands).
- [x] Fixture self-test includes an S1 case and is green.

### `S1-B-22` · **Spike Q-1** — `check-crate-dag.sh` allowlist minimality
**Est** 0.5–1 pd / **2 pts** · ⌂ `[ARCH]` B.2 = **S** · **Scheduled** S1 open · **Blocks** nothing;
hygiene. Run `--check-unused`; report whether the allowlist carries entries no crate needs.

**Acceptance**
1. [x] `bash scripts/check-crate-dag.sh --check-unused` exists (opt-in; default CI still ceiling-only).
2. [x] Demonstrated 2026-08-16: allowlist is **not** minimal. Unused entries (**not deleted**;
   append-only — do not silently drop live ceilings):
   - `cc-devnet-gen` → `cc-config` — `allowed_deps` ceiling; `bin/devnet-gen/Cargo.toml` omits the
     path dep (`# cc-config is intentionally omitted while unused`). No `cargo metadata` path edge.

Finding: `plan/issues/spike-notes.md` ## Q-1. JWT rule untouched (S1-A-01).

---

## S1 exit criteria — and which issue earns each

| # | Criterion | Earned by |
|---|---|---|
| E1.1 | §9.0 A/B clean; **non-zero overflow-family diff with zero diff in the other two = stage blocker** | `S1-A-19` |
| E1.2 | **M8 demonstrated red** against an injected engine black-hole | `S1-A-17` |
| E1.3 | `cargo test -p cc-seam` passes against **both** impls | `S1-A-12` |
| E1.4 | `check-crate-dag.sh` names `cc-engine-api` in the JWT rule; `cc-chain` **not** grandfathered | `S1-A-01` |
| E1.5 | 3 containers run; engine-fastpath DA works end to end on self-devnet | `S1-A-19`, `S1-B-01` |
| E1.6 | **S2 entry gate discharged** — M11 resolvable, M12 = 0. **An S1 exit item, not an S2 start item** | `S1-B-05` … `S1-B-20` |

---

## Drift against `[PLAN]` §3/S1 — stated, not smoothed

| # | Observation |
|---|---|
| 1 | **The `crates/engine-api` extraction is 8–12 pd in `[PLAN]`; decomposed it is 8.5–12.5 pd** — consistent. **The `cc-seam` group is 10–14 pd in `[PLAN]`; decomposed 11–15 pd** — consistent. The gate is 19–26 pd in §3 and 19–32 pd in §4; decomposed **22.25–30.25 pd**, inside §4's range and above §3's. Use §4's figure. |
| 2 | **Calendar.** Stream A carries 33–47.5 pd → **8.25–11.9 wk** at 4 effective pd/engineer-week, against `[PLAN]`'s 7–9 wk. Stream B carries 29.25–40.75 pd → 7.3–10.2 wk. The two streams are well balanced; the phase simply runs ~1–2 wk long at the top of the range. `[PLAN]` §12's lever — *a dedicated technical writer for the ADR corpus* — buys the (a) bucket (7–9 pd) but **not** the 12 (b) rows, which need the decision owners. |
| 3 | **The 11 conformance tests are named as a count, not a list.** `[ARCH]` §2.2 names four. Seven are unspecified in either source. `S1-A-12` enumerates them before writing them; if the honest number is not 11, the source figure needs correcting rather than the test list padding. |
| 4 | **P2-D/19 is "19 engine policy edges" with three named and no `file:line` for the other 16.** `S1-B-04` scopes to the three named; the rest are unestimatable as written. |
| 5 | **`X-6`, this decomposition's finding:** `[ARCH]` §10.4 assigns `ADR-P3-15`'s replacement to `ADR-R-05`, which §10.5 already uses for the slashing-protection record. Two decisions, one id. Resolved in `S1-B-13` by moving `ADR-P3-15`'s successor to `ADR-R-07`. |
