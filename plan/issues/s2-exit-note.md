# S2 exit note

Living record for the S2 exit criteria. Later issues append; they do not rewrite
prior conclusions.

Recorded 2026-08-16 on `feature/s2-b-15-foreign-client-sourcing`. `S2-B-15`
owns only the W10 prerequisite in this file.

**This file does not discharge E2.1–E2.6.** Those belong to `S2-A-15`,
`S2-J-01`, `S2-A-13`/`S2-A-14`, `S2-B-14`, and `S2-A-12`. No A/B scrape was
run. No E2 family numbers are invented.

---

## W10 prerequisite — five foreign clients sourced (`S2-B-15`)

**Conclusion: sourced and scheduled on our calendar. Not confirmed by the
foreign parties. No outreach was sent from this worktree.**

`[PLAN]` A-5 / D12: the five foreign clients for OQ-1 / M2e need external
coordination with lead time; sourcing starts at S2, not when `S3b-W-10`
opens. `S3a-B-26` (E3a.5) may assert *sourced and scheduled* by citing this
section. It must not upgrade that to *contacted*, *meetings booked*, or
*foreign-confirmed* without new evidence.

**What OQ-1 / M2e will require** (`[PLAN]` §5 W10, `s3b-acceptance-windows.md`
`S3b-W-10`): five production consensus clients, 5–10 peers each, venue
**Hoodi**, codec validation **5/5**. Baseline today is **0/5** and **0 peers
for all five** (`docs/phase-4-soak.md` OQ-1 is `NOT_RUN`). This issue does
not run that probe.

### The five implementations

The production CLs. Not a sixth. Not an EL. Not Grandine.

`docs/phase-4-soak.md` CC-4B listed Grandine as a fifth OQ-1 row. That table
is not rewritten here (W10 fills it). **Lodestar is the fifth** for this
program. Grandine is not a substitute.

| # | Implementation | Maintainer | Official public contact | Cite |
|---|---|---|---|---|
| 1 | **Lighthouse** | Sigma Prime | GitHub `sigp/lighthouse`. Discord named in the README Contact section. | [README Contact](https://github.com/sigp/lighthouse/blob/stable/README.md) — *"The best place for discussion is the [Lighthouse Discord server](https://discord.gg/cyAszAh)."* Repo: https://github.com/sigp/lighthouse |
| 2 | **Prysm** | Prysmatic / Offchain Labs | GitHub `OffchainLabs/prysm`. Discord named in the README. | [README](https://github.com/OffchainLabs/prysm/blob/develop/README.md) — *"Need help? Join our [Discord Community](https://discord.gg/qEZK94mFXP)"* (invite documented as non-expiring). Docs repeat it: https://prysm.offchainlabs.com/docs/contribute/contribution-guidelines/ |
| 3 | **Teku** | Consensys | GitHub `Consensys/teku`. `#teku` on Consensys Discord. | [README](https://github.com/Consensys/teku/blob/master/README.md) — *"get in touch in the #teku channel on [Discord](https://discord.gg/teku)"*. User docs: https://docs.teku.consensys.io/ |
| 4 | **Nimbus** | Status | GitHub `status-im/nimbus-eth2`. Discord + Status `#nimbus-general`. | [nimbus-eth2 README badges](https://github.com/status-im/nimbus-eth2/blob/stable/README.md): Discord https://discord.gg/XRxWahP, Status https://join.status.im/nimbus-general. [The Nimbus Guide — Get in touch](https://nimbus.guide/): Status + Discord. |
| 5 | **Lodestar** | ChainSafe | GitHub `ChainSafe/lodestar`. Discord named in the README. | [README](https://github.com/ChainSafe/lodestar/blob/unstable/README.md) — *"submit an issue or join us on [Discord](https://discord.gg/yjyvFRP)"*. Docs: https://chainsafe.github.io/lodestar/ |

Contacts are **public project channels**, not invented personal emails. This
worktree did **not** post in those Discords, open GitHub issues, or send mail.

### Named owner (our side)

**DSRV Stream B (edge & platform).**

No `CODEOWNERS` and no named individual in README, `[PLAN]`, or `[PRD]`. The
documented owner is the org + the stream this issue is assigned to:

- Org / repo: `dsrvlabs/eth-client-monorepo` (`README.md` scaffold table).
- Copyright: `Copyright 2026 DSRV` (`LICENSE`).
- Stream: `[PLAN]` §9 Stream B — *edge & platform* — owns "docs/ADRs, soak
  operations". `S2-B-15` is a Stream B issue.

Do not invent a person's email.

### Tentative window dates

**All dates below are TENTATIVE. Not confirmed by the foreign party.**

Derived from `[PLAN]` §2 / §5, not from a reply.

| Input | Value |
|---|---|
| Today | 2026-08-16 |
| `[PLAN]` date | 2026-08-15 |
| Program wk 1 | S0a (`[PLAN]` §2) |
| Calendar anchor | ISO week containing the plan date: **2026-W33** = 2026-08-10 … 2026-08-16 |
| S3a | wk 26–35 → 2027-02-01 … **2027-04-11** |
| E3a.5 latest | S3a exit: **2027-04-11**. W10 is after this. |
| S3b | wk 36–47 → 2027-04-12 … **2027-07-04** |
| W10 | Hoodi · 2–3 wk · after W2→W3→W5→W8 on the Hoodi serial path (A-4) · outreach may overlap earlier (`[PLAN]` §5 Σ line) |

High-end Hoodi prefix from 2027-04-12: W2 1.5 wk + W3 2 wk + W5 1 wk + W7 1 wk
+ W8 1 wk, after W0 (self-devnet, not Hoodi), lands ~2027-06-10. W10's 3-week
allowance then occupies S3b wk 45–47.

**Shared W10 window (TENTATIVE):** **2027-06-14 – 2027-07-04** (Hoodi).

One probe of all five, 5–10 peers each, codec 5/5. First-dial dates are
staggered inside that window so each row has an explicit calendar date.

| Implementation | Tentative first-dial (Hoodi) | Window close | Foreign confirmation |
|---|---|---|---|
| Lighthouse | **2027-06-14** | 2027-07-04 | **not confirmed** |
| Prysm | **2027-06-16** | 2027-07-04 | **not confirmed** |
| Teku | **2027-06-18** | 2027-07-04 | **not confirmed** |
| Nimbus | **2027-06-21** | 2027-07-04 | **not confirmed** |
| Lodestar | **2027-06-23** | 2027-07-04 | **not confirmed** |

Re-run cost is high (`[PLAN]` §5): a slip reschedules five parties. If the
S3b Hoodi prefix overruns, slide the shared window, keep the stagger, and
append here. Do not silently edit the dates above.

### What this worktree did and did not do

| Check | Result |
| Five production CLs named | **yes** — Lighthouse, Prysm, Teku, Nimbus, Lodestar |
| Public contact + URL per client | **yes** — table above |
| Tentative Hoodi date per client | **yes** — 2027-06-14 … 2027-06-23, close 2027-07-04, labelled TENTATIVE |
| Named owner on our side | **yes** — DSRV Stream B |
| Emails / Discord posts / GitHub issues sent | **no** |
| Meetings booked | **no** |
| Foreign reply received | **no** |
| OQ-1 / M2e probe run | **no** — still `NOT_RUN` |
| E2.1 A/B | **not this issue** |
| `S3a-B-26` closed | **no** — that issue asserts this record at S3a exit |

### What `S3a-B-26` (E3a.5) may quote

Five named implementations, five named public contacts, five tentative
calendar dates, one named owner. That is *sourced and scheduled* on **our**
books.

That is **not** *contacted* and **not** *foreign-confirmed*. E3a.5 does not
require a reply. If S3a wants confirmation before W0, that is new work and
must be appended, not back-dated into this section.

## E2.5 — P0-19/3 clone-cost measurement (`S2-A-12`)

**Conclusion: `BeaconState` clone no longer deep-copies the pubkey map.**

Type-level: `StateCaches` has no `PubkeyIndexMap` field (exhaustive match in
`state_caches_field_inventory_excludes_pubkey_index_map`). The map is a
sidecar (`PubkeyIndexMap` on `TransitionContext` after `S2-A-10`); the
measurement holds it next to the state and clones each separately.

This is a **scaled fixture** (N=2_000 and N=50_000), not an 80–100 MB / 1 M
validator scrape. Table bytes are `capacity × (key + value + 1 control byte)`,
not an allocator sample. Do **not** treat these numbers as a milhouse result.
**milhouse has not landed.** P0-19/4 (persist the cache) was not implemented.

Recorded 2026-08-16 on `feature/s2-a-12-clone-cost-measurement` (uncommitted)
branched from `develop` `31ffa9a`. Worktree
`/Users/nil/.grok/worktrees/dsrv-eth-client-monorepo/subagent-01a009c3-91a5-74a0-908b-c14c0b4252ff`.
Host Darwin 25.6.0 arm64. `rustc 1.97.1 (8bab26f4f 2026-07-14)`.

Command (same filter, two profiles):

```text
cargo test -p cc-types --lib -- beacon_state_clone_does_not_scale_with_pubkey_map --nocapture
cargo test -p cc-types --release --lib -- beacon_state_clone_does_not_scale_with_pubkey_map --nocapture
```

Test: `crates/types/src/state/caches.rs`
`beacon_state_clone_does_not_scale_with_pubkey_map`. Median of 31 clones after
3 warm-up clones.

### test/dev profile (`debug_assertions`, workspace `opt-level=1`)

| Arm | N | median clone | estimated table bytes |
|---|---:|---:|---:|
| `PubkeyIndexMap` clone (legacy cost, **before**) | 0 | **42 ns** | **0 B** (cap=0) |
| `PubkeyIndexMap` clone (legacy cost, **before**) | 2_000 | **10_083 ns** | **204_288 B** (cap=3584) |
| `PubkeyIndexMap` clone (legacy cost, **before**) | 50_000 | **156_459 ns** | **3_268_608 B** (cap=57344) |
| `BeaconState<Minimal>` clone + empty sidecar (**after**) | 0 | **2_209 ns** | map not on state |
| `BeaconState<Minimal>` clone + 2k sidecar (**after**) | 2_000 | **2_041 ns** | map not on state |
| `BeaconState<Minimal>` clone + 50k sidecar (**after**) | 50_000 | **2_042 ns** | map not on state |

`sizeof BeaconState<Minimal> = 3608`, `StateCaches = 2112`, `PubkeyIndexMap = 56`.

### release profile

| `PubkeyIndexMap` clone (legacy cost, **before**) | 0 | **0 ns** (timer floor) | **0 B** (cap=0) |
| `PubkeyIndexMap` clone (legacy cost, **before**) | 2_000 | **3_083 ns** | **204_288 B** (cap=3584) |
| `PubkeyIndexMap` clone (legacy cost, **before**) | 50_000 | **46_042 ns** | **3_268_608 B** (cap=57344) |
| `BeaconState<Minimal>` clone + empty sidecar (**after**) | 0 | **625 ns** | map not on state |
| `BeaconState<Minimal>` clone + 2k sidecar (**after**) | 2_000 | **542 ns** | map not on state |
| `BeaconState<Minimal>` clone + 50k sidecar (**after**) | 50_000 | **542 ns** | map not on state |

### What the numbers say

- Map clone **scales with N** (test/dev 10_083 ns → 156_459 ns; release 3_083 ns → 46_042 ns). That is the cost that used to ride on `StateCaches::clone`.
- State clone **does not** (test/dev 2_209 / 2_041 / 2_042 ns; release 625 / 542 / 542 ns) while a 0 / 2k / 50k map is held beside it.
- At N=50_000 the leftover map-clone tax is **156_459 ns vs 2_042 ns** (test/dev) and **46_042 ns vs 542 ns** (release) — the state clone is not paying the table copy.

## S2-B-13 — rollback operator procedure

**This issue does not discharge E2.4.** That belongs to `S2-B-14`
(rehearsal). The procedure was **written, not rehearsed**. No A/B
scrape. No soak numbers. No restart-trial row.

Recorded 2026-08-16 on `feature/s2-b-13-rollback-procedure` (uncommitted)
branched from `develop` `3e8d529`. Worktree
`/Users/nil/.grok/worktrees/dsrv-eth-client-monorepo/subagent-01a009f5-9a99-7c01-9299-06f2fc88f289`.

Operator procedure: [`docs/s2-rollback.md`](../../docs/s2-rollback.md).

S2 is the only stage with a data-shape consequence. The redb schema
does not change; the writer's input does:

| | Rule |
|---|---|
| **(a)** | On-disk format unchanged (`SCHEMA_VERSION = 1`, `WriteCursor` SSZ unchanged). The previous topology can open the same `<data_dir>/store.redb`. |
| **(b)** | Rollback **must** be preceded by a clean shutdown. |
| **(c)** | No migration in either direction. |

### Live `WriteCursor` (read from this tree)

`S2-A-09` deleted `write_behind.rs`. **Stream-seq is already gone as the
writer's input on both hosts this HEAD starts.** The `WriteCursor`
record still exists (`session_id`, `seq`, `slot`, `root` at
`meta.write_cursor`).

`ArchiveWriter::submit_writer_batch` **would** restamp that record in
the same P0 batch (refuses a missing cursor; does not invent `0/0`).
Neither composed host calls it: `cc-beacon-core` never builds
`ArchiveWriter`; compose `storage` builds one and drops it; compose
`chain` has `archive: None`. Leftover on-disk `seq` is frozen. This
tree does **not** write a new batch-seq into `WriteCursor.seq`.

`docker compose up` from this HEAD is **not** the pre-S2 writer. Last
commit that still has `write_behind.rs` is `e854b1d` (`78a90e1^`). No
image tag is pinned here.

Clean shutdown is still required (exclusive redb lock; writer mailbox
does not drain on the shutdown watch). Compose `stop storage` fires
that watch via `pre_drain_fire_shutdown`. Host `kill -TERM` on
`cc-beacon-core` does **not** — pre-drain only joins chain-core.

Same-file open: compose default mounts are named volumes
`cc-store-data:/app/data` and `cc-p2p-identity` (`/identity` on
storage, `/app/data` on p2p). They are not host `data/storage` or
`./data/node_key`. See the procedure.

`S2-B-14` is the drill. R-10 here is RestoreFromStore still on the
4-container host (`S2-J-02` not done), not write-behind coming back.
No duration is invented.

## S2-B-14 — rollback rehearsal (E2.4)

**Conclusion: same-file `Store::open` after a clean shutdown was observed.
Compose on a live Hoodi volume was not.**

Recorded 2026-08-16 on `feature/s2-b-14-rollback-rehearsal` (uncommitted)
branched from `develop` `60c6200`. Worktree
`/Users/nil/.grok/worktrees/dsrv-eth-client-monorepo/subagent-01a00a16-b76c-7aa3-b671-e742b90d4d63`.
Host Darwin 25.6.0 arm64. `rustc 1.97.1 (8bab26f4f 2026-07-14)`.
Procedure: [`docs/s2-rollback.md`](../../docs/s2-rollback.md). Last SHA
that still has `write_behind.rs` is `e854b1d`. This HEAD's compose is
**not** that writer.

No soak numbers. No restart-trial table. No re-sync duration.

### What was rehearsed

**1. CI test — current P0 writer, then the `e854b1d` `open_store` gates
on the same inode.**

```text
cargo test -p cc-storage-core --lib -- \
  s2_b_14_current_writer_files_open_via_previous_topology_gates --nocapture
```

Test: `crates/storage-core/src/rollback_rehearsal.rs`
`s2_b_14_current_writer_files_open_via_previous_topology_gates`.

- Writes schema 1 + Hoodi config digest + `meta.node_id` via
  `storage-core::open` / `persist_anchor_node_id`.
- Commits one hot block and a `WriteCursor` (`session_id=7`, `seq=11`,
  `slot=19`) through the live P0 mailbox (`submit_p0_committed`).
- Idle (commit returned) then fires the writer shutdown watch — the
  compose `stop` path; mailbox is not drained.
- Reopens **the same** `<data_dir>/store.redb` (Unix inode asserted)
  with the `e854b1d` `open_store` gates: `Store::open` (schema / digest /
  `I-node-id` / `I-cursor` / scan budget) +
  `refuse_missing_key_if_anchor_present`.
- Then reopens again via `storage-core::open` (what this HEAD's
  4-container `boot.rs` wraps).
- Observed: schema version **1**, cursor unchanged, block present, no
  refuse `Display` from the procedure's success table.

Second passing run (this worktree):

```text
S2-B-14 observed: current P0 writer → clean shutdown →
e854b1d Store::open + storage-core::open on
/var/folders/k3/wt33h85x4pzb20y23mm0byjc0000gn/T/s2-b-14-rollback-82693-0-1786876457278491000/store.redb
inode=209860605 bytes=552960
```

**2. Host `cc-storage` (this HEAD, 4-container process) on the first
fixture inode.**

Fixture left by the first test run:
`/var/folders/k3/wt33h85x4pzb20y23mm0byjc0000gn/T/s2-b-14-rollback-75188-0-1786876209713303000/store.redb`
inode **209840289**. Binary:
`target/debug/cc-storage` at `60c6200`.

- `Store::open` completed: `I-node-id node key loaded`, then
  `writer + migrator + replay + prune + serve pool ready` with
  `data_dir` equal to that directory. No schema / digest / lock /
  invariant refuse string.
- Resume then logged `store empty` — `is_store_empty` is “no
  `fc_scalars` and no snapshot”, not “`Store::open` failed”. The
  fixture is a hot block + cursor, not a durable seed.
- `kill -TERM`: `pre-drain: firing storage shutdown watch`, writer
  stopped, serve shutdown complete. Same inode after exit.

**3. Host `cc-storage` built at `e854b1d` (still contains
`write_behind.rs`) on that same inode.**

Binary: `/tmp/s2-b-14-e854b1d/target/debug/cc-storage` (`e854b1d`).
Same `data_dir` / `node_key` / inode **209840289**.

- `Store::open` completed: `I-node-id node key loaded from node_key_path`
  then `resume:` ran (that function is post-open).
- Resume again classified the fixture empty and retried
  `RestoreFromStore` dial to `http://127.0.0.1:9001/` (no chain
  process). That is not an open failure. E2.4 is `Store::open`.
- `kill -TERM` exited. Same inode.

### What was not rehearsed

| Item | Result |
|---|---|
| `docker compose up` of this HEAD | **not run** |
| TempDir-backed compose project | **not run** — see blockers |
| Live Hoodi / named volume `cc-store-data` | **not present** in this worktree (no `./data/storage`) |
| `wait-healthy.sh` | **not run** |
| Soak / restart-trial / re-sync wall-clock | **none** — not invented |
| Published image for `e854b1d` | **none** — this repo does not pin one |
| Treating this HEAD's compose as the pre-S2 writer | **no** |

Compose blockers actually hit: Docker 29.7.2 / Compose v5.3.1 / daemon
up. Host binaries are Mach-O arm64 and cannot run in a Linux container.
Existing local `*-storage` images are 8 days / 17 hours old and are not
`e854b1d` or this HEAD. A full `Dockerfile` workspace `--release` build
was not started.

### E2.4

Same-file open after clean shutdown **was observed** (CI test + this
HEAD's `cc-storage` + the `e854b1d` storage binary). That is the
criterion. Compose on a live Hoodi volume remains **not done**.

## E2.1 — §9.0 A/B (`S2-A-15`)

**Conclusion: procedure and blocker rule recorded. Loaded A/B not
executed. E2.1 is not discharged.**

Same three families, same blocker rule as E1.1 (`S1-A-19` /
[`s1-exit-note.md`](s1-exit-note.md)). Same procedure as `S0-B-19`
(`[ARCH]` §9.0). Diff S2 T≥1h against the **first honest loaded**
window, not against S0's idle Family 1 zeros.

This worktree did **not** run a loaded six-service / 3-container /
2-process A/B scrape. No `docs/s2-e21-*` scrape is committed. Do not
invent numbers. Do **not** treat S0 idle Family 1 zeros as a loaded
baseline — S0 stacks were **not meshed**.

S0 baseline: [`docs/s0-e07-ab-baseline.txt`](../../docs/s0-e07-ab-baseline.txt)
(commit `e2ad297072d07ab00cb328e5e12549d9c210ba8d`, wall **3632 s**).
Quoted from that record and [`s0-exit-note.md`](s0-exit-note.md) E0.7 /
[`s1-exit-note.md`](s1-exit-note.md) E1.1:

- Family 1 `cc_chain_import_total{result=imported}` T≥1h = **0** (and
  the other five result labels 0). There is **no** series named
  `cc_chain_import_result`.
- Family 2 `cc_p2p_head_lag_slots_count=1` (histogram **seed only**)
  and `cc_chain_head_lag_slots=0`.
- Family 3 overflow counters all **0**. `dangerous_case=no`.
- Six-service `cc_chain_head_slot=0`. The six-service stack was **not**
  on the self-devnet mesh. E1 p2p→chain is dead; the self-devnet is
  p2p-only. S0 Family 1 imported 0 **because the stacks were not
  meshed**.

Matching those idle zeros is **not** E2.1 clean. E1.1 was also not
discharged (`S1-A-19`); there is still no loaded T≥1h pair to diff
against.

Recorded 2026-08-16 on `feature/s2-a-15-s2-exit` (uncommitted)
branched from `develop` `d3d7f6065f82011fbb9e0385e7082f673ed60690`.
Worktree
`/Users/nil/.grok/worktrees/dsrv-eth-client-monorepo/subagent-01a00a5f-6203-71e3-8e37-becd05547893`.
Host Darwin 25.6.0 arm64. `rustc 1.97.1 (8bab26f4f 2026-07-14)`.

### Blocker rule (`[ARCH]` §9.0/4) — stated as acceptance

Diff **three families** (absolute numbers, not "no significant change"):

1. Import verdicts: `cc_chain_import_total{result=*}` on six-service
   `:9101` (OpenMetrics `_total` suffix). The plan name
   `cc_chain_import_result{*}` does not exist on the scrape.
2. Head-lag: `cc_p2p_head_lag_slots_bucket` on six-service `:9102` and
   self-devnet `:19102`/`:19112`/`:19122`, plus gauge
   `cc_chain_head_lag_slots` on `:9101`.
3. §2.2 overflow: `*_rejected_backpressure`, `*_dropped`, subscriber
   terminations, and `cc_grpc_requests_total{code="8"}` if present.
   Exact names S0 found are listed in the baseline file.

**A non-zero overflow-family (3) diff with a zero diff in families 1
and 2 is a stage blocker, not a curiosity.** Behaviour unchanged at
test load and the *contract* changed — the R-1 failure S2's transport
deletes exist to prevent. `scripts/s0-ab-baseline.sh --record` prints
`dangerous_case: yes` for that shape.

A loaded pass requires families 1 and 2 to be **import / head-lag
distributions under mesh**, then compared to a loaded T≥1h. S0 T≥1h
is idle; S1 did not produce a loaded window either. Until that window
exists, E2.1 stays open.

### Procedure (not executed here)

Both topologies from the **same** S2 commit (`[ARCH]` §9.0/1). S2
keeps `services/chain` and `services/storage` as workspace members so
the previous topology still builds (`[ARCH]` §9.1; `S2-J-01`
4-container A/B). `bin/beacon-core` is the new host (`S2-J-01`,
`3e8d529`). Production compose is still the leftover multi-service
host — it does **not** start `cc-beacon-core` (see E2.2). The
3-container self-devnet is still `devnet/compose.yml` (publisher,
node-a, node-b; plus anchor).

Helper: `scripts/s0-ab-baseline.sh` (fail-closed, loopback-only HTTP,
not in `make ci` / `make lint`). `--wait` is explicit; never implicit
1 h.

```text
make build
# six leftover binaries under target/debug/: cc-chain cc-p2p
# cc-attestation cc-engine cc-beacon-api cc-storage
# plus cc-beacon-core (S2-J-01). compose does not run the last one.

COMPOSE_PROJECT_NAME=s2a15-ab docker compose build && docker compose up -d
bash scripts/wait-healthy.sh 180

CC_DEVNET_MAX_SLOTS=2000 ./devnet/up.sh
# 3-container mesh: publisher + node-a + node-b (+ anchor). Reuse the
# same 64-slot fixture S0 used if regenerating; do not change fixtures
# or EL snap between topologies.

bash scripts/s0-ab-baseline.sh --scrape --label T0 --raw-dir docs/s2-e21-run/t0
bash scripts/s0-ab-baseline.sh --wait 3610
bash scripts/s0-ab-baseline.sh --scrape --label T1h --raw-dir docs/s2-e21-run/t1
bash scripts/s0-ab-baseline.sh --record \
  --t0-dir docs/s2-e21-run/t0 \
  --t1-dir docs/s2-e21-run/t1 \
  --out docs/s2-e21-run/families.txt
```

Then:

1. Confirm leftover compose `chain`/`p2p`/`attestation`/`engine`/
   `beacon-api`/`storage` healthy for the whole window. EL gate is
   `service_started` (ADR-P3-14).
2. Confirm self-devnet publisher + node-a + node-b + anchor stayed up.
   Quote mesh gossip (`cc_p2p_gossip_messages_total`) so the run is
   not an idle six-service scrape.
3. If a 2-process host is ever composed, scrape that pair from the
   **same** commit as well. This HEAD has no such compose service.
4. Diff S2 T≥1h against a **loaded** T≥1h (not S0 idle zeros) for the
   three families. Apply the §9.0/4 blocker.
5. Paste absolute numbers into this note. Do not summarise as "no
   significant change".

If leftover-compose `cc_chain_import_total{result="imported"}` is
still 0 after ≥1 h, the stacks were not meshed — same defect S0
recorded. That is **not** a loaded Family 1 match.

### What this worktree demonstrated (not a scrape)

| Check | Result |
|---|---|
| `bash scripts/s0-ab-baseline.sh --self-test` | `ok: self-test` (2026-08-16T11:44:07Z) |
| `docker compose -f docker-compose.yml config --services` | `chain` `p2p` `storage` `attestation` `beacon-api` `el` `engine` |
| `beacon-core` compose service | **absent** |
| `docker compose -f devnet/compose.yml config --services` | `anchor` `publisher` `node-a` `node-b` |
| `bash scripts/s0-ab-baseline.sh --check-bins` | **fail** — empty `target/`; all six `target/debug/cc-*` missing |
| `make build` / six `target/debug/cc-*` + `cc-beacon-core` | **not run** |
| leftover compose `up` + `./devnet/up.sh` + ≥1 h scrape | **not run** |
| 2-process (`cc-p2p` + `cc-beacon-core`) compose scrape | **not run** — no such compose |
| Host leftover images | `s0b19-ab-baseline-*` / `cc-devnet-*` **18 hours** old (S0 commit `e2ad297`, not this tree). 8-day `*-storage` / `*-beacon-api` images are older still. Not started. |

## E2.2 — two processes on self-devnet (`S2-J-01`, `S2-A-15`)

**Conclusion: `cc-beacon-core` exists. Compose still runs the leftover
4-container / six-service host. Two processes were not run on
self-devnet. E2.2 is not discharged.**

`S2-J-01` (`3e8d529544c48e12d13a912216fbd494036b4807`) added
`bin/beacon-core` (`cc-beacon-core`). One process opens redb before
any subsystem (`bin/beacon-core/src/boot.rs`). The Dockerfile copies
`cc-beacon-core` into `/out/` next to the six leftover binaries.

`docker-compose.yml` does **not** start that binary. This HEAD's
`docker compose config --services` is `chain` `p2p` `storage`
`attestation` `beacon-api` `el` `engine`. Makefile `SERVICES` is still
`chain p2p attestation engine beacon-api storage`. `S2-J-01`
acceptance kept `services/chain` + `services/storage` as workspace
members for **4-container A/B**.

Self-devnet (`devnet/compose.yml`) is still the p2p-only 3-container
mesh: publisher, node-a, node-b + anchor. No `cc-beacon-core` service
there either.

`[ARCH]` §9.1 S2 **ships** "2 processes". That host is not what
compose runs on this commit.

## E2.3 — `import → durable` in-process (`S2-A-13`, `S2-A-14`)

**Conclusion: the test crate exists and is in the workspace CI graph.
Not re-run in this worktree (empty `target/`).**

Half of M9. Dedicated proto-free crate **`cc-beacon-import`**
(`crates/beacon-import`). Harness crate `cc-beacon-inproc`
(`crates/beacon-inproc`, `S2-A-13`, `8dffb86254ae5dbce0b76a655b9551c9113974a8`).

Assertion (`S2-A-14`, `d3d7f6065f82011fbb9e0385e7082f673ed60690`):

```text
cargo test -p cc-beacon-import --test import_durable \
  import_on_block_then_ingest_writes_durable_rows
```

Test: `crates/beacon-import/tests/import_durable.rs`
`import_on_block_then_ingest_writes_durable_rows`. After
`boot_in_process` (one TempDir, one redb), `on_block` then
`ArchiveWrite::ingest_block`; durable rows
`canonical[slot] == root`, body present, `WriteCursor` advanced, rows
survive reopen. No gRPC: `crates/beacon-import/tests/no_grpc.rs` plus
`scripts/check-no-grpc-beacon-inproc.sh` (clippy job +
`make check-inproc-grpc`). Workspace `cargo nextest` in the `test` job
includes this member (not in the vectors-job exclude filter).

## E2.4 — rollback rehearsal (`S2-B-14`)

**Not re-rehearsed here.** Cite the section already in this file
(`## S2-B-14 — rollback rehearsal (E2.4)`). Same-file `Store::open`
after a clean shutdown **was observed**. Compose on a live Hoodi
volume **was not**. No soak / restart-trial / re-sync numbers
invented.

## E2.5 — P0-19/3 clone-cost measurement (`S2-A-12`)

**Not rewritten here.** Cite the E2.5 section already in this file
(`S2-A-12`). Those absolute ns / estimated table bytes stand.
milhouse has not landed.

## E2.6 — M10 restated on the 8-edge denominator (`S2-A-15`)

**Conclusion: M10 is 3 of 8 remaining, 0 dead.** Not a fraction of
six. ⟡ D-2: there are eight internal edges, not six (`[ARCH]` §2.0).
`[PRD]` M10's baseline ("4 of 6 dead, 1 of 6 unauthenticated") used
the wrong denominator.

The eight internal edges (`[ARCH]` §2.3). After S2, **E3–E7 deleted**,
**E1 / E2 / E8 remaining**.

| ID | Edge | After S2 | This tree |
|---|---|---|---|
| **E1** | p2p → chain `P2pStream` | **remaining** | `rpc P2pStream` still in `proto/eth/chain/v1/chain.proto`. Transport undecided (S3). |
| **E2** | p2p / engine → chain `DataAvailable` | **remaining** | Still a `P2pToChain` arm. Wired at S3 (P0-16). |
| **E3** | chain → engine gRPC | **deleted** (S1) | `services/chain/src/engine_client.rs` gone. Production is in-process `DirectEngine` (`S1-A-06`). |
| **E4** | storage → chain `RestoreFromStore` | **deleted** (S2) | Absent from `proto/`. `restore.rs` / `restore_client.rs` gone (`S2-J-02`, `60c6200`). |
| **E5** | p2p → storage `PutBackfillBatch` | **deleted** as the internal data-plane RPC | Becomes `storage_core::backfill::admit()` behind `ArchiveWrite`. Proto RPC remains on the leftover 4-container `cc-storage` host (`storage.proto:28`) so that A/B topology still builds. |
| **E6** | p2p ← storage `WatchServeWindow` | **deleted** as the internal data-plane RPC | Serve window is an `AtomicU64` load (`[ARCH]` §2.3). Proto RPC remains on the leftover host (`storage.proto:32`). |
| **E7** | storage → chain `SubscribeEvents` as bulk data plane | **deleted** for storage (S2) | `write_behind.rs` gone (`78a90e1`). `SubscribeEvents` survives for API/observer (`chain.proto:21`). |
| **E8** | engine ↔ p2p `EngineStream` | **remaining** | `rpc EngineStream` still in `proto/eth/p2p/v1/p2p.proto`. Folds into E1's egress half at S3. |

**M10 = 3 of 8 remaining, 0 dead.**

"Remaining" is E1, E2, E8. "Dead" here is leftover *deleted-set*
transports still sitting in the inventory as unwired RPCs of E3–E7.
Those five are deleted (E3 at S1, E4–E7 at S2). E5/E6 proto methods
on the leftover 4-container `cc-storage` binary are the previous
topology, not surviving internal edges on the S2 2-process inventory.

E1/E2 still unwired (P0-16 / P0-17) is **S3 wiring**, not an M10
dead-edge leftover after S2. Do not write M10 as 3/6, 4/6, or "1 of
6 unauthenticated".

## S2 exit criteria (`S2-A-15` reading)

| # | Status |
|---|---|
| **E2.1** | **open — no loaded A/B; blocker rule recorded; do not match S0 idle zeros** |
| **E2.2** | **open — `cc-beacon-core` exists; compose still 4-container / six-service; two processes not run on self-devnet** |
| **E2.3** | **recorded — `cc-beacon-import` `import_on_block_then_ingest_writes_durable_rows`; not re-run here** |
| **E2.4** | **recorded by `S2-B-14` — same-file open observed; compose-on-Hoodi not run** |
| **E2.5** | **recorded by `S2-A-12` — numbers in the E2.5 section above; not rewritten** |
| **E2.6** | **recorded — M10 = 3 of 8 remaining, 0 dead** |
