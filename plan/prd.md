# PRD — Refactoring program: make the node follow the chain, then collapse the hexad

**Repo:** `dsrvlabs/eth-client-monorepo` · **Branch:** `develop` (`4146791`) · **Date:** 2026-08-15
**Status:** proposed · **Scope:** refactoring program, not a greenfield product

**Source documents (normative):**

| Ref | Document | Role |
|---|---|---|
| **[AS]** | `architecture-study-2026-08-12.md` | 25-agent architecture study — §1 recommendation, §3 headline finding, §4 twenty-finding ledger, §7 judge panel, §8 six-stage migration, §9 keep-&-watch |
| **[RV]** | `review-develop-2026-08-09.md` | multi-agent code review of `develop` — 2 HIGH security, 12 HIGH correctness, 29 med + 8 low correctness, 12 med + 8 low quality, 2 convention, 35 unverified |
| **[Q1]–[Q5]** | `plan/research/` | research stage, 2026-08-15 — five open technical questions the two documents above left unanswered. **[Q3]** (pubkey cache) and **[Q5]** (fork seam) each changed the P0 ledger; **[Q4]** added a decision record. |
| **[ARCH]** | `plan/architecture.md` | target-state architecture for `beacon-core`. Cited here for its ADR enumeration (§10.4, §10.5) and its D-13 note on the corpus size. **Its §10 is not yet written** — see §10/J-16. |

**Verification markers.** **✓** = independently re-verified against the working tree by the PRD author
on 2026-08-15 (method in Appendix B.1). **✓ᴸ** = verified against the working tree by the team lead
during the research stage, 2026-08-15 (Appendix B.2). **✓ᴱ** = recovered by the estimator while
decomposing a row into issues, 2026-08-15 (Appendix B.3) — located, not adversarially verified.
Unmarked rows are asserted by [AS], [RV], [ARCH] or a research brief and carried forward on their
authority. The three markers are not interchangeable — a claim's authority chain is part of the claim.

---

## 0. Summary

The repo contains a mainnet-grade consensus core that has never followed a chain. Two problems are
entangled and must be stated apart: **(A)** spec-complete subsystems ship disconnected, so the node
cannot follow the chain and has zero live-network acceptance evidence; **(B)** the six-process gRPC
topology is what hid (A) from the compiler and from every test, and is what will hide the next one.

This program fixes (A) on a schedule that does not wait for (B), then removes (B) in stages that are
individually shippable. It commits to the uncontested part of the architecture recommendation —
fusing `chain` + `storage` + `engine` into one `beacon-core` process — and deliberately **defers** the
one contested decision (whether the `p2p` process survives) to evidence that does not yet exist.

**Requirement counts:** **P0 = 19** (18 from 22 raw high-severity findings after de-duplication, plus
**1 from the research stage present in neither source document**) · **P1 = 58** · **P2 = 54** ·
**total 131**.

> **Priority tier ranks finding severity, not work sequence.** Several P0 rows are discharged by P1
> stage work rather than by a direct patch — the `Disposition` column on every ledger row is
> authoritative for sequencing.

**Revision 1 — 2026-08-15 (research stage).** Three changes to the ledger, all from [Q3]/[Q4]/[Q5]:
**P0-19** is new — an empty pubkey cache on every SSZ-decoded state, which makes a checkpoint-synced or
restored node fail every block import silently. It appears in neither [AS] nor [RV]. **P0-02** widened
from one deposit-domain bug to a **five-instance class** — all five functions in `pub mod network`
resolve config-scoped values from a compile-time preset key, and four have no runtime-config path at
all (§5.1.1). **P1-F/1** is new: record the EIP-3076 slashing-protection decision as an ADR now, six
months before anyone can reach for the write-behind path. Two sequencing traps are now dispositions
rather than notes — P0-19/3 before milhouse (P1-D/10), and milhouse before the Gloas schema edit
(P1-D/15, P1-E/S4).

**Revision 2 — 2026-08-15.** Three corrections, no change to requirement counts:

- **P0-19 is not latent — it is firing today.** Revision 1 said the stall waits for P0-17a to wire
  gossip. That is wrong, and the correction makes it worse: `restore.rs` and `replay.rs` both run the
  state transition on an SSZ-decoded state **now**, so `CachePoisoned` fires on the **second boot of
  any node that has taken a snapshot** — no gossip required. It is a **candidate root cause for why
  the restore choreography has never worked end to end** (P1-A/22, P1-A/23). See §5.1.2.
- **The phantom-corpus census in D-6 and M11 was ~5× under.** It counted only the hyphenated `ADR-`
  spelling and missed the dominant spaced `ADR P3-02` form: **~59 distinct ids / ~204 citations**, not
  13 / 38. That makes D-6 a **work item needing its own estimate**, not a documentation chore riding
  along with S2.
- **Per-fork STF dispatch drops from L to M.** Lighthouse gates handlers with monotone `ForkName`
  capability predicates written **once**, not per-fork modules copied N times (P1-D/16, P1-E/S4).

---

## 1. Problem statement

### 1.A — The node does not work

The most spec-sensitive logic in the repo is spec-complete and well unit-tested, and then severed by
wiring that no compiler saw and no test could drive ([AS] §3). Verified in the tree:

| Severed path | Evidence | Consequence |
|---|---|---|
| Gossip subscription | `services/p2p/src/host.rs:1299` — `SwarmCommand::Subscribe` handler has **zero senders** in production; only the devnet fault harness subscribes **✓** | The node subscribes to no gossip topic. It receives no blocks. |
| PeerDAS data availability | `NoopSamplingFeed` + `da_tx = None` (`services/p2p/src/das/sampling.rs:154,200,538`); `services/engine/src/main.rs:135` passes `kzg: None` **✓** | `DataAvailable` can never reach `chain`. Every blob-carrying block parks and is dropped after 4 slots. |
| KZG verify pool | `services/p2p/src/service.rs:714` — `kzg_tx: _`, sender dropped at destructure **✓** | The OS-thread KZG pool is bypassed; verification falls back inline on the validation worker. |
| Serve window | never published; empty-window seed `earliest_available_slot: u64::MAX` (`services/storage/src/serve.rs:1086-1087`) **✓** (seed constant verified; "never published" is [AS] §3) | Every block/column served to peers answers `ResourceUnavailable`. |
| Backfill upward write | `PutBackfillBatch` named in `services/p2p/src/storage_client.rs:210` and `backfill/below.rs:43`, but the client method does not exist **✓** | ~3,200 lines of backfill have zero call sites ([AS] §4/12). |
| Container peer URIs | `docker-compose.yml:30` omits all three `CC_*` peer-URI overrides **✓** | In-container `chain` dials *itself* for `newPayload`; every healthcheck still reports green. |

**Zero live-network acceptance runs have ever been executed.** Every clause of Phases 1–4 reads
`NOT_RUN`: **294 `NOT_RUN` cells** across `docs/phase-1-soak.md` (36), `docs/phase-2-soak.md` (76),
`docs/phase-3-acceptance.md` (89), `docs/phase-4-soak.md` (93) **✓**. There is no measurement
anywhere in this repo taken against a real network.

This is a bug *class*, not a bug: spec-complete subsystems shipped dead because a cross-process
boundary hid the disconnection.

### 1.B — The topology causes 1.A

Six gRPC microservices — `chain` :9001, `p2p` :9002, `engine` :9004, `storage` :9006, plus two
~85-line stubs `attestation` :9003 and `beacon-api` :9005 — were drawn along **consensus-subsystem
lines rather than trust or failure lines** ([AS] §2).

**No production consensus client does this.** Lighthouse, Prysm, Teku, Nimbus and Grandine all ship
the beacon node as one OS process; networking, fork-choice, storage and the Engine bridge are
separated by in-process module boundaries, never by IPC ([AS] §5). The only real process boundaries
in the ecosystem are the ones with a genuine trust or ownership split (beacon-node ↔ EL over
JWT-authenticated Engine API; beacon-node ↔ validator-client; validator-client ↔ remote signer).
Prysm — the one client that bet on gRPC — removed it; its one experiment splitting a subsystem into a
separate process (the standalone slasher) was deleted and folded back in because it "suffered from
several bugs" ([AS] §5).

The causal link from B to A is direct: **every disconnection in §1.A is either a compile error or
unrepresentable inside a single process.** Six of six internal edges are dead or unguarded ([AS] §2):

| Internal edge | Contract | Status |
|---|---|---|
| p2p → chain | `P2pStream` bidi | **DEAD** — no gossip subscription |
| p2p / engine → chain | `DataAvailable` | **DEAD** — Noop feed, `da_tx=None` |
| chain → engine | `NewPayload` / `ForkchoiceUpdated` | LIVE — but no deadline |
| storage → chain | `RestoreFromStore` boot push | LIVE — boot-only, **unauthenticated** |
| p2p → storage | `PutBackfillBatch`, serve reads | **DEAD** — client method absent |
| p2p (serve window) | `WatchServeWindow` | **DEAD** — never published |

The topology also imposes standing costs that no trust argument pays for: six lockstep-versioned
stateful singletons that cannot scale, upgrade or fail independently; a health DAG that stays green
for the one failure it cannot heal (a hung engine edge parking the core) and red for the one compose
already restarts; and a lone `chain` restart that forces external checkpoint re-sync and permanently
holes the archive ([AS] §6, Reliability lens: *"a monolith paying microservice rent"*).

---

## 2. Stakeholders

| Stakeholder | What they need from this program | How they judge it |
|---|---|---|
| **Node operator** (runs it on Hoodi, then mainnet) | A node that follows head, survives restarts, does not silently corrupt its archive, and is not reachable by an unauthenticated LAN attacker | Head lag, finality progress, restart drills, a health signal that goes red when the core is parked |
| **Consensus-correctness owner** | Blocks accepted by this node are accepted by the network; blocks the network accepts are accepted here | Spec-vector suites green with an empty skiplist; deposit/RANDAO/attestation signature verification actually running; wire encodings that decode against five foreign clients |
| **Engineering team** (maintains and evolves it) | A tree where a disconnected subsystem fails to compile, a green CI gate that means something, and a fork seam that makes Gloas a new module rather than a 44-file retrofit | Gate trustworthiness, whole-node integration test exists and runs, dead-wiring bug class made unrepresentable |

---

## 3. Goals and non-goals

### Goals

| # | Goal | Primary stakeholder |
|---|---|---|
| G1 | The node imports blocks end to end on a live network (Hoodi) and holds head | Operator |
| G2 | Every high-severity correctness and security finding in [AS] §4 and [RV] is closed or explicitly deleted by a topology change | Correctness owner |
| G3 | The dead-wiring bug class is made unrepresentable — a disconnected subsystem becomes a compile error, not a silent no-op | Engineering |
| G4 | The gate is trustworthy: `make test` and every blocking CI check are green on the committed tree and mirror CI exactly | Engineering |
| G5 | Phases 1–4 acceptance clauses are discharged with real measurements, not skeletons | Operator + correctness owner |
| G6 | A fork-evolution seam exists before any Phase 5 code is written | Engineering |
| G7 | The p2p-endpoint decision is made on live evidence, with criteria fixed in advance | All three |

### Non-goals

| # | Non-goal | Why |
|---|---|---|
| N1 | **Rewriting the pure crate DAG** (`types → crypto → state-transition → fork-choice`, plus the embedded `store` on redb) | It is the asset. It maps 1:1 onto Lighthouse and Grandine and makes the fusion mechanical rather than a rewrite. Every critique lens named these crates as the thing worth keeping ([AS] §2, §9). |
| N2 | **Picking the Stage-3 p2p endpoint today** | The panel tied 147–147 ([AS] §7). The crate DAG keeps both endpoints one mechanical stage apart. The deciding evidence — a live soak — does not exist yet. See §9. |
| N3 | Changing the single-writer / `ArcSwap` actor discipline or the bounded named-constant queues | Already in-process patterns; they survive consolidation verbatim ([AS] §1, §9). |
| N4 | Abandoning the opaque-SSZ-over-contract rule | It is accidentally a first-class fork-evolution property — a Gloas block crosses the same contracts untouched ([AS] §5). |
| N5 | Building Phases 5–7 on the existing bespoke gRPC stubs | Deferred to Stage 5, on standard boundaries (REST beacon API, standard BN↔VC line, remote signer) ([AS] §8). |
| N6 | Performance work on the gRPC hop tax | Acquitted: at ~200 KB-scale messages/second the ~100 µs hop tax is three orders of magnitude below budget. Every real deadline-killer is intra-process ([AS] §6, Performance lens). |

---

## 4. Framing decisions carried forward

These are decided. They are requirements, not options.

**D-1 — The shared spine is uncontested: merge `chain` + `storage` + `engine` into one `beacon-core`
process.**
These three services are one trust domain and one failure domain; their contracts already skip
verification for each other. Both front-running proposals agree on this and on ~90% of the moves
([AS] §1, §7). Fusing them **deletes rather than patches** a set of high-severity surfaces. [AS] §1
names five: the unauthenticated state-takeover restore path, the deadline-less engine hop, the
event-ring-as-data-plane coupling, the broken cross-process restart choreography, and the
`trusted_local` KZG-skip bypass. See §10/J-1 — only some of those map onto §4 ledger rows, and the
ledger `Disposition` column, not the §1 bullet list, governs sequencing.

**D-2 — The p2p endpoint remains an open decision.** See §9 for the decision record and exit criteria.
A PRD that hard-commits to one endpoint today is wrong.

**D-3 — Stage 0 correctness fixes are topology-independent and ship-blocking, and must not be folded
into consolidation work.** Specifically: the deposit domain fork version, `CoreConfig::default()`
shipping `NoVerification` on the production import path, and the two interop-breaking
`BeaconBlocksByRange` / `BlocksByRoot` wire encodings. These survive any topology; blocking them on a
multi-month consolidation would be a scheduling error ([AS] §8 Stage 0).

**D-4 — The two HIGH security findings collapse to one change.** Both [RV] Vuln 1 and Vuln 2 share one
root cause: the root `docker-compose.yml` publishes every internal mutating gRPC port on `0.0.0.0`
while the code behind them assumes a trusted private network. The sibling `devnet/compose.yml` already
binds `127.0.0.1` under a `SEC-H1` comment (`devnet/compose.yml:11-12,90-91`) **✓**. Dropping the
`9001–9006` host publishes is a **near-one-liner, not a phase** (P0-01). Server-side hardening of the
same RPCs is real work but is separately tracked (P1-A/1, P1-A/2) and does not gate the port fix.

**D-5 — The baseline is not green.** Any requirement that assumes a green baseline is wrong.
Independently confirmed on the committed tree **✓**:
- `cargo fmt --all -- --check` is **red** — three diffs in `bin/cc-store/src/lib.rs:13,52,138`.
- `scripts/check-no-env-reads.sh` exits **1** with 4 production hits (`services/storage/src/replay.rs:829,832`;
  `services/p2p/src/main.rs:536,537`) — and it is a **blocking** CI gate (`.github/workflows/ci.yml:88`).
- `make test` is the canonical gate; plain `cargo test` and `cargo fmt --all` are red at HEAD.

Restoring a green, trustworthy gate is itself a P0 requirement (P0-08).

**D-6 — The phantom ADR / Architecture corpus is a prerequisite, and it is ~5× larger than first
scoped.** The tree cites an authority corpus that does not exist — `find -iname '*adr*'` returns
nothing **✓**. The census, corrected in revision 2:

| Basis | Revision 1 (this PRD) | **Corrected** | Why the first figure was wrong |
|---|---|---|---|
| `ADR-` hyphenated citations | 38 **✓** | **45** ✓ᴸ | — |
| `ADR ` **spaced** citations (`ADR P3-02`) | **not counted** | **159** ✓ᴸ | The dominant spelling was missed entirely — the regex matched only `ADR-[A-Za-z0-9_-]*` |
| **Total ADR citations** | 38 | **~204** ✓ᴸ | |
| **Distinct ADR ids** | 13 **✓** | **~59** ✓ᴸ | Same cause |
| `Architecture §` | 434 **lines** **✓** | **439 lines** ✓ᴸ / **541 occurrences** | Revision 1 counted lines containing the string, not occurrences of it |
| ADR files in the repo | 0 **✓** | **0** ✓ᴸ | unchanged |

**Occurrences is the basis used from here on**, and it is stated wherever the figure appears:
**~745 unresolvable citations** (541 `Architecture §` + ~204 ADR) against **~59 distinct ADR ids** and
zero files. See §10/J-15 for a ±1-id disagreement between the two sources of the corrected count.

**The consequence changes, not just the number.** [AS] §9 lists "import or re-derive the phantom ADR
corpus" as a Stage-3 prerequisite; this PRD already promotes it to a **Stage-2 entry gate**. At ~59 ids
rather than 13, that gate is **a work item requiring its own estimate — not a documentation chore that
rides along with the storage fold.** It must be sized and staffed before S2's entry gate can be
honestly evaluated, or the gate will be waived under schedule pressure, which is the failure mode the
gate exists to prevent.

**The mechanism is a reconciliation table, and it does not exist yet.** `plan/architecture.md`
proposes classifying every cited id as *re-derivable* / *needs-decision* / *stale-citation*, and
estimates that ~40 of the ids are re-derivable from the code that cites them in under an hour each,
with only a handful needing a decision recorded. That reframes the gate as *"every cited id resolves to
a committed document, and the reconciliation table has no unclassified rows"* rather than *"write 59
ADRs"* — achievable rather than stalling. **Caution:** that architecture document cites its own §10 ten
times, for the reconciliation table and for three new ADRs (`ADR-R-02`, `ADR-R-03`, `ADR-R-04`), but it
**ends at §9.2 with a `<!-- SECTION-10-ANCHOR -->` placeholder — §10 has not been written** ✓. The
mechanism is therefore **proposed, not available**, and the architecture document has reproduced the
exact bug class D-6 describes. See §10/J-16.

---

## 5. Requirement ledger

### 5.0 Disposition vocabulary

Every ledger row carries a disposition. Three of the values are load-bearing and are not
interchangeable:

| Disposition | Meaning |
|---|---|
| `patch @ S0` | Fix in place now. Topology-independent; survives any endpoint choice. |
| `patch @ S0 → deleted @ Sn` | Fix now **and** the surface disappears at stage *n*. Both are true; do the patch, then delete the patched code with the stage. |
| `deleted @ Sn` | **Do not patch.** The consolidation removes the surface. Patching it is waste — the exact waste the consolidation argument exists to avoid. |
| `wire @ S3` | A dead island. The code exists and is unit-tested; the requirement is to connect it. |
| `patch @ Sn` | Fix, scheduled to ride along with stage *n*'s work. |

Stage 4 carries an internal ordering (`S4a` → `S4b` → `S4c`, defined in P1-E/S4) because a step there
is order-dependent in a way that silently degrades the result rather than failing: milhouse before the
Gloas schema edit, and P0-19/3 before milhouse. A disposition of `patch @ S4a` names that sub-stage.

### 5.1 P0 — ship-blocking (19 requirements)

Derived mechanically from [RV] 2 HIGH security + [RV] 12 HIGH correctness + [AS] §4 findings 01–08 =
**22 raw findings → 18 requirements** after de-duplication (four collapses; full map in Appendix A),
**plus P0-19 from the research stage, present in neither source document** = **19**.

> **Read P0-19 first.** It is the last row in the table only because ids are append-only for
> reference stability (§10/J-13). By R-12 it is **the most urgent row here** — the only P0 that is
> both firing on the committed tree today and invisible to every existing signal, log line and
> healthcheck.

| ID | Requirement | Evidence `file:line` | Source | Disposition |
|---|---|---|---|---|
| **P0-01** | Drop the `0.0.0.0` host port publishes for `9001–9006` in the root `docker-compose.yml`; bind metrics `9101–9106` to `127.0.0.1` exactly as `devnet/compose.yml` does under `SEC-H1`. Closes **both** HIGH security findings. | `docker-compose.yml:32,53,73,99,128,150`; pattern at `devnet/compose.yml:11-12,90-91` **✓** | [RV] Vuln 1 + Vuln 2; [AS] 06a | `patch @ S0` |
| **P0-02** | **Delete `pub mod network`; move all five config-scoped constants to `ChainConfig`.** [RV] and [AS] both report this as one deposit-domain bug. [Q5] §1 establishes it is a **five-instance class**: `crates/state-transition/src/helpers/constants.rs:128-172` has exactly five functions, **all** keyed on the compile-time preset `P::NAME`, and **all five are `config`-scoped in the spec's `configs/mainnet.yaml`** — while `crates/types/src/config.rs` parses only one of them (`genesis_fork_version`, `:144`). Four have **no runtime-config path at all**. See the instance table below. Fix: add the four missing fields to `ChainConfig`/`RawChainConfig` (each `#[serde(default = …)]` at the mainnet value so no fixture breaks), thread `&ChainConfig` into `is_valid_deposit_signature` and the ~20 other `constants::network::` call sites, and **delete the module** — so omitting the config becomes a compile error. Same fix in `epoch/pending_deposits.rs`. [Q5] sizes the whole class as **S** (2–4 days) and recommends pulling it forward from S4 to S0; this PRD adopts that. | `crates/state-transition/src/helpers/constants.rs:128-172` **✓ᴸ**; `crates/types/src/config.rs:144` **✓ᴸ**; deposit instance at `.../operations/deposit.rs:42` (call) / `:44` (arg) **✓** — see §10/J-2 | [RV] HIGH; [AS] 04; **[Q5] §1** | `patch @ S0` |
| **P0-03** | `CoreConfig::default()` must not ship `verify: NoVerification`. Production `main.rs` builds `CoreConfig { ..default() }` without raising it, so live import never verifies RANDAO / attestation / exit / slashing / sync-aggregate signatures and skips the proposer signature on the unary path. Default to `VerifyIndividual`; reserve `NoVerification` for the restore-replay path that already overrides it. | `services/chain/src/core.rs:188`; skip at `services/chain/src/import.rs:636` | [RV] HIGH | `patch @ S0` |
| **P0-04** | `BeaconBlocksByRange` v2 must encode 24 bytes. It currently encodes 16 (`start_slot`, `count`), dropping the spec's mandatory `step: uint64` — deprecated but never removed from the SSZ schema. The primary block-sync protocol is fully interop-broken in both directions. Add `step: u64` (=1), set `BY_RANGE_SSZ_LEN = 24`, update the duplicated `(16,16)` limits. | `services/p2p/src/reqresp/blocks.rs:46`; dup limits `services/p2p/src/reqresp/mod.rs:203`, `crates/libp2p/src/ssz_snappy_codec.rs:210` | [RV] HIGH; [AS] 05 | `patch @ S0` |
| **P0-05** | `BlocksByRoot` v2 must encode bare `32*n` roots. It currently prepends a spurious 4-byte offset; the request is a top-level `List[Root]` of fixed-size elements, and offsets only exist for variable-size element lists. Decode on `len % 32 == 0`; fix the duplicated `min:4` limits. | `services/p2p/src/reqresp/blocks.rs:148` | [RV] HIGH; [AS] 05 | `patch @ S0` |
| **P0-06** | Restore the wire prober's independence. It reproduces the *same* bogus 4-byte `BlocksByRoot` offset, so probe and node agree with each other and both diverge from spec — defeating its stated purpose (ADR P4-12: "independent so a shared bug cannot make the probe pass against our own node"). | `bin/serve-probe/src/protocols.rs:258` | [RV] HIGH; [AS] 05 | `patch @ S0` |
| **P0-07** | Add the three missing cross-service URI overrides to compose. Their absence makes localhost TOML defaults apply in-container: `chain` dials its own `9004` (engine unavailable, **import dead**), `engine` has no `CC_ENGINE_P2P_URI`, `p2p` has no `CC_P2P_PEERS__STORAGE` (serve window fail-closes to `NOT_SERVING`). Every *other* peer is overridden — an oversight, not a design. Ship with a block-import smoke test that would have caught it. | `docker-compose.yml:30`; zero occurrences of the three keys **✓** | [RV] HIGH; [AS] 06b | `patch @ S0 → deleted @ S2` |
| **P0-08** | **Restore a green, trustworthy gate.** (a) Route the 4 production `std::env::var` reads through `cc-config` or an annotated allowlist — `scripts/check-no-env-reads.sh` is a *blocking* CI gate (`ci.yml:88`) that exits 1 on the committed tree **✓**; (b) fix the script's `awk` `#[cfg(test)]` exemption, which never resets, so one test attribute silently exempts the rest of the file, and broaden the pattern to `env::var` / `var_os` / `vars`; (c) make `cargo fmt --check` green; (d) make `make ci` mirror the CI gates it claims to. | `scripts/check-no-env-reads.sh:17`; `services/storage/src/replay.rs:829,832`; `services/p2p/src/main.rs:536,537`; `bin/cc-store/src/lib.rs:13,52,138`; `Makefile:193` **✓** | [RV] HIGH + convention; [AS] 17a | `patch @ S0` |
| **P0-09** | Grow the fork-choice vote and balance tables from the trusted post-state in `integrate_block`. They are sized to the anchor registry and never grown; `resize_votes` has no production caller. Any attestation naming a validator activated after checkpoint sync hits non-deferrable `ValidatorIndexOutOfRange` and the aggregate is dropped, and `justified_balances_snapshot` truncates to stale capacity. On mainnet the registry grows every epoch, so **LMD weights diverge within hours**. | `crates/fork-choice/src/on_block.rs:447` | [RV] HIGH; [AS] 07a | `patch @ S0` |
| **P0-10** | Wire `ProtoArray::prune` to the FINALIZED event. It has no callers, so fork-choice memory grows without bound. | `crates/fork-choice/src/proto_array.rs:325` (the uncalled `prune`); `services/chain/src/events/mod.rs:206` (the FINALIZED event to hang it off) — **✓ᴱ** | [AS] 07b | `patch @ S0` |
| **P0-11** | Fix the proposer-lookahead fallback. When a block slot is outside the Fulu lookahead window it reads the head state's *current-epoch* row (`slot % SLOTS_PER_EPOCH`) as the "expected proposer", which almost never matches — producing a Reject verdict **and a peer descore** for any valid block arriving >~2 epochs after the head state's epoch (first block after a long gap, lagging fork branch). Return `None` (unknown) and let `on_block` validate; prefer the parent state. | `services/chain/src/import.rs:697` | [RV] HIGH | `patch @ S0` |
| **P0-12** | Fix the slot clock. `store.time` advances only via a `thread::sleep`-driven `SlotTick` that is phase-unaligned, drifts, and is **silently dropped when the 64-deep command channel is full**. Between ticks `get_current_slot()` lags up to a full slot, so blocks gossiped early in their slot are IGNOREd as `future_slot` and proposer-boost timing is skewed. Call `on_tick(store, wall_clock_now)` at the top of each import or align the ticker to genesis-derived boundaries; allow `MAXIMUM_GOSSIP_CLOCK_DISPARITY`. | `services/chain/src/core.rs:527` | [RV] HIGH | `patch @ S0` |
| **P0-13** | On a P0 flush error, end the write-behind session with `Reconnect { cursor: last_flushed }`. Today the unit is logged and dropped while the session keeps consuming events; the next successful flush commits a `WriteCursor` with a later seq, so the failed unit's events are **claimed durable and skipped forever on resume** — an unrecorded data-loss hole that violates the cursor-bounds-the-loss-window contract. | `services/storage/src/write_behind.rs:657` | [RV] HIGH | `patch @ S0 → deleted @ S2` |
| **P0-14** | `rewrite_from_head` must delete canonical rows for vacated slots. It only ever *writes* rows on the new branch; there is no `TABLE_CANONICAL` delete anywhere in the tree. A reorg onto a branch that skips a slot the old branch occupied (or a shorter head) leaves the stale `canonical[slot]` row forever, violating the module's "can never disagree with the blocks it indexes" contract. Stage `batch.delete` during the walk; add a slot-skipping reorg test. | `crates/store/src/canonical.rs:91` | [RV] HIGH | `patch @ S0` |
| **P0-15** | Put hard deadlines on **every** chain→engine RPC and route timeouts to the existing optimistic-import deferral. Today a black-holed engine parks the whole consensus core; the deferral machinery to handle it exists but can never trigger. Also kill the latent `block_on` panic on the restore path. | `services/chain/src/engine_client.rs:177`; [AS] §4/03 | [AS] 03 | `patch @ S0 → deleted @ S1` |
| **P0-16** | Build a production path for data-availability signaling. `NoopSamplingFeed` and `da_tx = None` sever the PeerDAS loop from both p2p and engine to chain, so **every blob-carrying block parks and is dropped after 4 slots** — the fork's central new mechanism has no production path at all. | `services/p2p/src/das/sampling.rs:154,200,538` **✓**; `services/engine/src/main.rs:135` (`kzg: None`) | [AS] 02 | `wire @ S3` (engine half lands @ S1) |
| **P0-17** | **Wire the dead library islands.** The node runs its most spec-sensitive logic only in tests. Sub-items: (a) gossip subscribe — `SwarmCommand::Subscribe` handler with zero senders **✓** — **blocked on P0-19: gossip does not *cause* the pubkey-cache stall (it fires on boot today) but it widens the blast radius to the live-import path, where the failure names no cause**; (b) the KZG verify pool — `kzg_tx: _` sender dropped at destructure **✓**; (c) serve window publication — currently `ResourceUnavailable` for every block/column served; (d) backfill's client-side storage write method, absent, leaving ~3,200 lines with zero call sites. (DA feed → P0-16; proto-array prune → P0-10.) | `services/p2p/src/host.rs:1299`; `services/p2p/src/service.rs:714`; `services/storage/src/serve.rs:1086-1087`; `services/p2p/src/storage_client.rs:210` **✓** | [AS] 01 | `wire @ S3` |
| **P0-18** | Fix the storage scale time bombs: (a) interned table-names exhaust at **~30 days uptime**; (b) the contig-walk cap sits **below the full serve window**; (c) multi-GB invariant scans block `open` on a supernode. | (a) `crates/store/src/keys.rs:51,56,61`; (b) `crates/store/src/invariants.rs:271,332,596-624`; (c) `crates/store/src/invariants.rs:45-50` — **✓ᴱ** | [AS] 08 | `patch @ S2` |
| **P0-19** | **Fill the pubkey cache after every state decode. Firing today — not latent.** `caches: StateCaches<P>` on `BeaconState` carries `#[ssz(skip_serializing, skip_deserializing)]` — correct for consensus (caches must not affect the state root), **fatal for reachability**: every SSZ-decoded state starts with an empty `PubkeyIndexMap`. `process_sync_aggregate` then resolves committee indices **only** through that map with **no linear-scan fallback**, returning `BlockError::CachePoisoned`, which classifies as `GossipClass::Internal` and so surfaces to nobody. **Both restore and replay run the STF on a decoded state in production today**, so this fires on the **second boot of any node that has taken a snapshot** — no gossip required. Fix (Q3 Change 1, **S**): a `top_up_pubkey_cache` that fills from the registry — append-only, idempotent, O(V) once per decode — called at all three decode sites, and better, made unskippable behind a `BeaconState::from_ssz_bytes_hydrated` chokepoint (Q3 Change 1b). Plus (Q3 Change 2) a round-trip regression test that is **forbidden to hand-fill the cache**, with a negative assertion that omitting the top-up yields `CachePoisoned`. Plus a `pubkey_cache_len` vs `validators_len` gauge (§7.4/M13). **Present in neither [AS] nor [RV].** | `crates/types/src/state/mod.rs:106` **✓ᴸ**; `crates/state-transition/src/block/sync_aggregate.rs:125-129` **✓ᴸ**; **active paths:** `services/chain/src/restore.rs:437` decode → `on_block` at `:525` **✓ᴸ**; `services/storage/src/replay.rs:644` decode → `state_transition(` at `:572` **✓ᴸ**; plus `services/chain/src/checkpoint_sync.rs:1023` | **[Q3]** | `patch @ S0` **+ S2 follow-on** — see below |

**P0 disposition summary (19 rows):** **16** `patch @ S0` — of which **3** are also deleted by a later
stage (P0-07 and P0-13 @ S2, P0-15 @ S1) and **1** (P0-19) has an M-sized S2 follow-on · **1**
`patch @ S2` (P0-18) · **2** `wire @ S3` (P0-17, and P0-16 whose engine half lands @ S1).

#### 5.1.1 P0-02 — the five instances

All five functions in `pub mod network` (`crates/state-transition/src/helpers/constants.rs:128-172`)
resolve a **config**-scoped value from the **compile-time preset** key `P::NAME`. Stated as [Q5] §1.1
does: *a module named `network` that resolves config-scoped values from a preset-scoped key.*

| Function | Spec home | Parsed by `ChainConfig`? | Consensus impact on a non-mainnet-preset network |
|---|---|:--:|---|
| `genesis_fork_version` | `configs/*.yaml` | ✅ `config.rs:144` | **LIVE TODAY.** Hoodi's GVF is `0x10000910`; preset `mainnet` returns `0x00000000`, so every deposit proof-of-possession verifies under the wrong domain and is silently dropped. Same on Sepolia and Holesky. |
| `churn_limit_quotient` | `configs/*.yaml` (`65536`) | ❌ | **Latent, consensus-critical.** Sets the activation/exit churn limit. A network that customises it diverges on validator-set changes **every epoch** — no compile error, no test failure, no log line. |
| `min_per_epoch_churn_limit_electra` | `configs/*.yaml` (`128000000000`) | ❌ | **Latent, consensus-critical.** Electra churn balance floor — same divergence shape. |
| `max_per_epoch_activation_exit_churn_limit` | `configs/*.yaml` (`256000000000`) | ❌ | **Latent, consensus-critical.** Electra churn balance ceiling — same divergence shape. |
| `shard_committee_period` | `configs/*.yaml` (`256`) | ❌ | **Latent, consensus-critical.** Gates voluntary-exit eligibility; a wrong value accepts or rejects exits the network does not. |

**Why the four are latent and not benign:** [Q5] §1.2 verified against
`crates/types/tests/fixtures/hoodi-config.yaml` that Hoodi happens to use mainnet's values for all
four, so nothing fires there today. The spec puts them in the config *precisely so networks can
customise them* — a devnet or future testnet that does produces a silent consensus divergence. This is
also the trap in R-9, generalised: **a naive fix passes on Hoodi and diverges on a customising
devnet.**

**The loader half of the same class.** [Q5] §1.3: `RawChainConfig` (`crates/types/src/config.rs:286-309`)
declares 19 fields with no `#[serde(deny_unknown_fields)]`, so these keys are **read from disk and
discarded**. Both halves are in P0-02's scope — the reader takes the value from the wrong source and
the loader does not take it from the right one. Prefer an explicit captured-and-WARN-logged unknown-key
map over bare `deny_unknown_fields`, which would break on every upstream config that adds a Heze key.

**The precedent is tighter than "some client does it differently."** [Q5] verified from Lighthouse
source that `ChainSpec::fork_name_at_epoch` lives in `consensus/types/src/core/chain_spec.rs` — **the
types crate**, exactly where [AS] wants `ForkSchedule` authority to sit — implemented as a **descending
data table**, so adding a fork is one row rather than an if/else edit. And the precise analogue of this
repo's bug is `voluntary_exit.rs::get_domain`, which resolves a **signature domain** from
`spec.fork_name_at_epoch(epoch)`: the same operation as `deposit.rs:42`, performed against the right
authority source. This is not a stylistic preference — a production client does the identical
computation from runtime config, in the crate this program is already moving authority into.

**Cross-references.** P2-A/8 (`crates/types/src/config.rs:101`, `MAX_BLOBS_PER_BLOCK_ELECTRA` compiled
in) is the **same class by a different mechanism** (`P::MAX_BLOBS_PER_BLOCK_BASE`) and is discharged by
P0-02, which must add `max_blobs_per_block_electra` anyway ([Q5] §1.5 step 1) — see §10/J-11. S4's
`ForkSchedule` authority work (P1-E/S4) is the *rest* of this: P0-02 removes the preset-keying, S4 adds
the ordered accessors — modelled on the descending table above — and deletes the five duplicated
fork-schedule walks.

**Out of scope, flagged for decision:** [Q5] §1.4 reports a *third* live defect on the same file —
today's upstream `configs/mainnet.yaml` has no `SECONDS_PER_SLOT` (it carries `SLOT_DURATION_MS`
instead), while `RawChainConfig.seconds_per_slot` has no `#[serde(default)]`, **so loading the current
upstream mainnet config fails to parse**. It lands in the same edit but it is a loader-defaulting bug,
not a preset-keying one, so it is not folded into P0-02. See §10/J-12 — it may warrant its own P0.

#### 5.1.2 P0-19 — disposition detail and sequencing

| Q3 change | Size | Disposition |
|---|---|---|
| 1 — `top_up_pubkey_cache` + 3 call sites | **S** (~30 lines + 3 calls) | `patch @ S0` |
| 1b — `from_ssz_bytes_hydrated` chokepoint | **S** (~1 day) | `patch @ S0` — makes the class unrepresentable rather than fixed-thrice |
| 2 — round-trip regression test, hand-fill forbidden | **S** (~1 day) | `patch @ S0` |
| 3 — move the cache off `BeaconState` onto `TransitionContext` | **M** (1–2 weeks) | `patch @ S2` — **prerequisite for P1-D/10**, see below |
| 4 — persist in `cc-store` | **S–M** | `patch @ S2`, optional |

**Four things about P0-19 that are not obvious from the row:**

1. **It is firing today, and it is a candidate root cause for the broken restore choreography.**
   Revision 1 of this PRD called it latent, pending P0-17a wiring gossip. That was wrong. Two
   production paths run the state transition on an SSZ-decoded state **right now**:
   - `services/chain/src/restore.rs:437` decodes with raw `BeaconState::<P>::from_ssz_bytes(input.state_ssz)`
     and feeds `on_block` at `:525` **✓ᴸ**
   - `services/storage/src/replay.rs:644` decodes and reaches `state_transition(` at `:572` — which
     appears **exactly once** in that file, i.e. it is the production call, not a test **✓ᴸ**

   So `CachePoisoned` fires on the **second boot of any node that has taken a snapshot**. Restore is
   the path that has never worked end to end, and this is a **plausible common cause** for P1-A/22
   (`RestoreGate::end_stream` never notifies waiters — failed restore hangs bootstrap forever) and
   P1-A/23 (restore silently drops DA-deferred blocks). Those two are dispositioned `deleted @ S2`;
   if P0-19 is their actual cause, **deleting the surface would have hidden the bug rather than fixed
   it**, and it would have reappeared in `beacon-core` — see §10/J-14. Treat P0-19 as a candidate
   explanation to test *before* S2 deletes the evidence.

   **The hypothesis has a competing explanation, and the ledger already contains it.** /22 is a
   notification gap and /23 is a re-drive gap; `CachePoisoned` would make restore fail *earlier and
   differently* than either symptom describes. So the two are equally consistent with P0-19 being real
   **and** /22 and /23 being independent bugs. Distinguishing them takes two observations, not one —
   see R-13. Do not let a passing `CachePoisoned` reproduction be read as a diagnosis of /22 and /23.

   **Carry the caveat with the claim:** this is a **code-path trace, not an executed reproduction**.
   The block-replay branch has **zero test coverage**, which is consistent with the trace but is not
   confirmation. Do not quote the severity outside this repo until the ~30-line check in [Q3] §5 has
   run (R-11).
2. **P0-17a widens the blast radius but is no longer the trigger.** Wiring gossip subscribe adds the
   live-import path to the two boot paths already affected, and the failure mode there is the ugly one:
   *"we wired gossip, the node connects, peers are healthy, and it imports nothing"* — no `Reject`, no
   descore, no invalid-block metric, health DAG **green**. The P0-17a dependency stands; the reason has
   changed from *causes* to *compounds*.
3. **Eight test harnesses hand-fill the cache that production never fills** — the spec-test harnesses,
   the fork-choice tests, the chain offline-replay test, and the devnet generator (which fills it at
   genesis construction and then serialises to SSZ, which drops it). Eight independent authors
   discovered the requirement and each patched around it locally. That is why no test catches it, and it
   is why Q3 Change 2's test must be *forbidden* to hand-fill.
4. **`restore.rs:437` is one edit that closes two findings.** It decodes with the raw
   `BeaconState::from_ssz_bytes`, which both bypasses the fork chokepoint ([Q5] §4.1 — the chokepoint is
   not one while this site exists) **and** never fills the pubkey cache ([Q3] §1.3). Route it through
   `from_ssz_bytes_with` *and* top up, in the same rewrite.

### 5.2 P1 — required, scheduled (58 requirements)

Six registers — 29 medium correctness + 12 medium quality + 1 medium convention + 10 cross-cutting
programs + 5 consolidation stages + 1 decision record. Overlaps between registers are links, not
duplicates: a cross-cutting program and one of its constituent line items are different units of work.

#### P1-A — Medium correctness line items (29) · [RV] §2 Medium

| # | Location | Requirement | Disposition |
|---|---|---|---|
| 1 | `services/storage/src/serve.rs:821` | `PutBackfillBatch` can overwrite the canonical index at any slot: no anchor/frontier binding, progress optional. Server-side validation for [RV] Vuln 2. | `patch @ S0` |
| 2 | `services/storage/src/backfill.rs:219` | Admission never checks the batch attaches to the durable frontier; `blocks_oldest` can jump down across a hole | `patch @ S2` |
| 3 | `services/storage/src/backfill.rs:87` | `per_index` padding fabricates progress for never-custodied indices; monotone guard rejects honest cgc-raise reports | `patch @ S2` |
| 4 | `services/storage/src/write_behind.rs:214` | Panic respawn resubscribes with the boot-time cursor, discarding committed progress | `deleted @ S2` |
| 5 | `services/storage/src/main.rs:352` | CC-4A block floor read from a build-machine fixture path with silent hardcoded fallback + `unwrap_or(0)` | `patch @ S0` |
| 6 | `services/storage/src/prune/mod.rs:532` | `drop_table` failure after marks durably advanced leaks the stripped shard permanently | `patch @ S2` |
| 7 | `crates/libp2p/src/ssz_snappy_codec.rs:35` | 32 MiB response-stream cap silently truncates legitimate column/block responses | `patch @ S3` (with `cc-wire`) |
| 8 | `services/p2p/src/gossip/validate/pipeline.rs:897` | Clock disparity quantized up to a full slot — widens all gossip timeliness checks **24×** | `patch @ S0` |
| 9 | `services/p2p/src/gossip/validate/column.rs:445` | Column validator omits two spec REJECT conditions (slot > parent slot; finalized-ancestor) | `patch @ S0` |
| 10 | `services/p2p/src/gossip/registry.rs:581` | `advance_to` requires exact epoch equality; a missed epoch tick **permanently wedges subscriptions** | `patch @ S0` |
| 11 | `services/p2p/src/service.rs:535` | CC-4E persisted ENR seq never wired: seq resets to ~1 on every restart/discovery respawn | `patch @ S3` |
| 12 | `services/p2p/src/reqresp/blocks.rs:446` | Empty in-window results answered with error code 3 instead of an empty success stream | `patch @ S0` |
| 13 | `services/p2p/src/reqresp/columns.rs:536` | Column by-range chunks served in request order, not ascending `(slot, column_index)`; comment contradicts code | `patch @ S0` |
| 14 | `services/p2p/src/peer_manager/dial.rs:101` | Scheduler re-dials peers just disconnected for bad `app_score` → 1 s connect/Goodbye churn loop | `patch @ S3` |
| 15 | `services/p2p/src/host.rs:1342` | `ClosePeer` sends Goodbye then immediately disconnects — Goodbye almost never reaches the wire | `patch @ S3` |
| 16 | `services/p2p/src/backfill/planner.rs:1158` | `drain_imports` **deadlocks** the oldest-first cursor at any slot without a block | `patch @ S3` |
| 17 | `services/p2p/src/backfill/planner.rs:1114` | Below-anchor batch results leak into the forward-only hold map, permanently blocking completion | `patch @ S3` |
| 18 | `crates/state-transition/src/block/mod.rs:160` | Sync-aggregate signature verified even under `NoVerification` strategy | `patch @ S0` |
| 19 | `crates/fork-choice/src/on_attestation.rs:284` | `update_latest_messages` partially mutates vote trackers on OOB index, skips the counter bump | `patch @ S0` |
| 20 | `crates/fork-choice/src/on_block.rs:452` | Justified `CheckpointContext` overwritten from the importing block's post-state, not the checkpoint state | `patch @ S0` |
| 21 | `crates/fork-choice/src/head_cache.rs:151` | `get_proposer_head` has an invented equivocation branch that bypasses the spec's safety conditions | `patch @ S0` |
| 22 | `services/chain/src/restore.rs:189` | `RestoreGate::end_stream` never notifies waiters — a failed restore after grace **hangs bootstrap forever**. **⚠ P0-19 is a candidate common cause**: restore decodes at `:437` and feeds `on_block` at `:525` on a state with an empty pubkey cache, so every replayed block returns `CachePoisoned` — which is one explanation for *why* the restore fails before this handler's gap matters. **Test that hypothesis before S2 deletes the surface** (§10/J-14). | `deleted @ S2` — **but diagnose first** |
| 23 | `services/chain/src/restore.rs:656` | Restore silently drops DA-deferred blocks — `DataAvailable` can never re-drive them. **⚠ Same candidate common cause as /22.** | `deleted @ S2` — **but diagnose first** |
| 24 | `services/chain/src/da.rs:395` | Block-branch `FetchBlobs` template always carries a zeroed KZG inclusion proof in production | `patch @ S1` |
| 25 | `services/engine/src/main.rs:135` | Production binary passes `kzg: None` — the entire CC-37b getBlobs fastpath is dead | `patch @ S1` (links P0-16) |
| 26 | `services/engine/src/main.rs:133` | Production lane uses the `hoodi_blob_bound()` **test fixture** for the blob-count gate | `patch @ S1` |
| 27 | `docker-compose.yml:127` | Storage can never enforce I-node-id: no identity mount, no `CC_STORAGE_NODE_KEY_PATH` in compose | `patch @ S0 → deleted @ S2` |
| 28 | `.github/workflows/ci.yml:208` | Proto breaking baseline hardcodes `branch=develop` for pushes, but the workflow also runs on push to `main` | `patch @ S0` |
| 29 | `devnet/faults.sh:27` | Pinned to `devnet/compose.yml`, but the CC-4N kill-9 clause documents it against the main stack | `patch @ S0` |

#### P1-B — Medium quality line items (12) · [RV] §3

| # | Location | Requirement | Disposition |
|---|---|---|---|
| 1 | `services/storage/src/write_behind.rs:599` | `observe_lag` runs on the already-reset accumulator, so `write_behind_lag_slots` **never records** | `patch @ S0` |
| 2 | `services/storage/src/serve.rs:484` | Unary serve permits released at handler return, so the documented 256 MiB ceiling isn't enforced | `patch @ S2` |
| 3 | `services/storage/src/replay.rs:354` | `measure_load_from_store` runs multi-second CPU work (200 MB SSZ decode + tree-hash) on the async runtime thread | `patch @ S2` |
| 4 | `services/p2p/src/reqresp/server.rs:111` | `serve_block_protocol`/`serve_column_protocol` are production-dead; `host.rs` re-implements the pipeline and has already drifted | `patch @ S3` |
| 5 | `services/p2p/src/reqresp/limits.rs:329` | `record_violation`'s `&mut f64` score param is always stubbed, double-counting the penalty metric | `patch @ S3` |
| 6 | `crates/libp2p/src/ssz_snappy_codec.rs:204` | `request_limits` duplicates `Protocol::request_limits` and the two **already disagree** — the third codec copy behind P0-04/P0-05 | `patch @ S3` (`cc-wire`) |
| 7 | `services/p2p/src/gossip/validate/pipeline.rs:621` | Single-worker validation loop blocks up to **12 s** on the chain block-import reply, stalling all gossip validation | `patch @ S3` (links P1-D/09) |
| 8 | `services/p2p/src/gossip/validate/column.rs:707` | Fork-version schedule walk hand-duplicated across three validator files | `patch @ S4` (links P1-D/15) |
| 9 | `services/p2p/src/storage_client.rs:240` | `invalidate()` flips `available=false` with no data-plane path to restore it; one transient RPC error refuses serves until the watch stream speaks | `deleted @ S2` |
| 10 | `crates/state-transition/src/epoch/mod.rs:53` | `block_to_epoch` catch-all relabels every unexpected error as `ArithmeticOverflow` | `patch @ S0` |
| 11 | `services/chain/src/engine_client.rs:177` | Core thread blocks on engine gRPC with no connect/RPC deadline | `patch @ S0` (= P0-15) → `deleted @ S1` |
| 12 | `crates/crypto/src/bls/batch.rs:127` | `SignatureSet` batch verification bypasses the `BLS_VERIFY_COUNT` instrumentation contract | `patch @ S0` |

#### P1-C — Convention (1) · [RV] §3

| # | Location | Requirement | Disposition |
|---|---|---|---|
| 1 | `.github/workflows/ci.yml:49` | All GitHub Actions pinned by mutable tag (`@v4`…), contradicting the repo's own exact-pin policy (`docs/supply-chain.md`). Pin to commit SHAs. | `patch @ S0` |

#### P1-D — Cross-cutting programs (10) · [AS] §4 Med findings 09–18

| ID | Program | Subsumes | Disposition |
|---|---|---|---|
| 09 | **Head-of-line blocking on both critical loops ✳** — single-worker validation with inline crypto (dead KZG pool); chain's one `mpsc(64)` FIFO mixes imports, queries and ticks. The production-proven answer is Lighthouse's `beacon_processor`: a priority work-scheduler feeding a blocking worker pool ([AS] §5). | P1-B/7, P0-17b | `patch @ S3` |
| 10 | **Per-import state economics ✳** — ~3–5 full 150–200 MB `BeaconState` clones per import; O(V) `get_head` copies. The milhouse seam is still only a type alias. Orthogonal to topology. **Two corrections from the research stage:** (i) [AS]'s "no pubkey cache, ~1.5–3 s/block" understates it — the cache exists and its absence is a *silent import stall*, not a tax; that is P0-19, not this row. (ii) **Hard prerequisite: P0-19 Change 3** (move the cache off `BeaconState` onto `TransitionContext`). `StateCaches` derives `Clone`, so an ~80–100 MB `HashMap` with 48-byte keys deep-copies on each of the 3–5 state clones per import. **Landing milhouse before that move makes its O(1) clone a lie** — the migration would ship and measure no improvement. | P0-19/3 (prereq) | `patch @ S4a`, **after P0-19/3** |
| 11 | **Stringly-typed cross-service protocols ✳** — verdict reason strings, `FCU_DROPPED_STALE:` prefixes, hand-rolled event byte-offsets with silent-default fallbacks. **Type the contracts before moving them** (see §8/R-1). | — | `patch @ S1–S2` (before transport moves) |
| 12 | **Backfill: unwired, missing its storage write method, deadlocks on empty slots** — ~3,200 lines, zero call sites; the oldest-first cursor cannot represent missed proposals. | P0-17d, P1-A/16, P1-A/17 | `wire @ S3` |
| 13 | **Chain event bus doubles as a bulk data plane** — full column sidecars relayed through a 64 MiB ring couple archive durability to another process's eviction timing; gap-fill can invent canonical roots. | — | `deleted @ S2` |
| 14 | **Graceful shutdown is largely unreachable or lying ✳** — storage's shutdown watch never fires; biased selects hot-spin on `watch` Err; drain-timeout exits 0. | — | `patch @ S0` (fire the watch) → `deleted @ S2` |
| 15 | **Duplicated single-sources-of-truth that diverge at runtime ✳** — `ForkContext` ×3, three slot clocks, peer-state split-brain, `BeaconState` schema maintained in **four hand-synchronised places**. **Sequencing note from the research stage:** one of the four — the `StateField` discriminant order — yields a **wrong state root rather than a compile error** when it drifts from the struct's field order. The milhouse swap (P1-D/10) **deletes two of the four**. Gloas is `−1 / +9` fields on `BeaconState`, so doing milhouse first converts the Gloas schema change from a four-place synchronised edit into a two-place one, at no extra cost. **Order: P0-19/3 → P1-D/10 (milhouse) → Gloas schema work.** The fork-schedule-walk half of this row is duplicated **five** ways, not three ([Q5] §1.5). | P1-B/8, P1-B/6, P0-12, P0-02 | `patch @ S4a` |
| 16 | **No fork-evolution seam; unowned spec-vector coverage holes** — single-shape containers face EPBS; `upgrade_to_fulu`, transition/core and light_client suites are run by no crate. **Dispatch mechanism settled, and it is cheaper than [AS] implies (L → M):** [Q5] verified from Lighthouse source that `per_block_processing` uses neither trait objects nor an exhaustive per-fork enum match, but **monotone capability predicates on `ForkName`** — `if fork_name.gloas_enabled()` — inside **one** function generic over `EthSpec`, with variant-specific fields arriving through superstruct partial getters. Because `X_enabled()` means *"fork X or later"*, each handler is written **once and gated**, never copied per fork. Grandine's per-fork `block_processing.rs` / `epoch_processing.rs` modules are the literal reading of [AS] §8's "per-fork STF dispatch"; that option is more expensive **and reintroduces the same N-copies synchronisation hazard as the four-place `BeaconState` schema** in P1-D/15 — the hazard this program is trying to delete. Adopt the predicate form: **~10 lines of predicates on the existing `ForkName`** (`crates/types/src/fork.rs:17-59`, already an ordered enum). | P1-E/S4 (4b) | `patch @ S4b`, sized **M** (was **L**) |
| 17 | **Governance / QA drift ✳** — (a) the ADR/Architecture corpus cited **~745×** across **~59 distinct ADR ids** does not exist in the repo ✓ᴸ — **import or re-derive it as a gate on Stage 2 entry** (D-6). **At this size it needs its own estimate**, and the reconciliation mechanism that makes it tractable is itself unwritten (§10/J-16); (b) Makefile doesn't mirror CI. (The red-CI-gate half is P0-08.) | P2-B/8 (`Makefile:193`), **promoted to P0-08(d)** | (a) **gate on S2 entry — size it first**; (b) `patch @ S0` |
| 18 | **Test-harness state in production paths** — a 1,900-line `fault_mode` global is consulted by the column validator; devnet fixture configs are compiled into binaries. | P2-C/1, P1-A/26 | `patch @ S3` |

#### P1-E — Consolidation stages (5) · [AS] §8

| ID | Stage | Requirement | Ships |
|---|---|---|---|
| S1 | **Fold the EL bridge into chain** (wk 3–8) | Extract `crates/engine-api` (three-lane transport, JWT, health machine, verbatim with tests); chain calls it directly. Move the blob fastpath in with **real** KZG. Delete `EngineStream` and the `trusted_local` bool — the caller is now the process. | 3 containers · engine-fastpath DA path works end to end |
| S2 | **Fold storage into `beacon-core`** (wk 8–16) | Move the storage modules in; boot opens redb in-process, deleting `RestoreFromStore`'s takeover surface **and** the `block_on` panic. Events become typed structs. Columns leave the ring for a direct ingest path with a top-of-batch continuity bind. cc-store scale fixes (P0-18) ride along. **Gated on D-6.** | 2 processes · the archive-hole class is gone |
| S3 | **Wiring completion + first Hoodi soak** (wk 16–28) | Wire every dead island — gossip subscribe + §5.6 scoring, the `kzg_tx` reconnect, the real DA feed, serve, backfill — and extract `cc-wire`. **This stage also resolves D-2**; see §9. **Exit criteria are the Phase 1–4 acceptance clauses, run for real.** | A node that follows the chain, proven on a live network |
| S4 | **"Phase 4.5": the fork seam, before any Phase 5 code** — **internally ordered 4a → 4b → 4c** | **4a:** milhouse swap on `BeaconState` (P1-D/10), **which requires P0-19/3 first** or its O(1) clone is a lie; this reduces the state schema from four hand-synchronised places to two *before* Gloas edits it (P1-D/15). **4b:** `ForkName::Gloas`; enum-of-forks for the **exactly 6** containers EPBS reshapes ([Q5] §2 — `Attestation`, `IndexedAttestation`, `BeaconBlockBody`, `BeaconState`, `ExecutionPayload`, `ExecutionRequests`; [AS]'s "~6" is exact), behind the two existing decode chokepoints whose five call sites all hardcode `ForkName::Fulu`; per-fork STF dispatch + `upgrade_to_*`. Per-fork STF dispatch is **monotone `ForkName` capability predicates written once and gated** (`fork_name.gloas_enabled()`), **not** per-fork modules copied N times — sized **M**, not L; see P1-D/16 for why the literal reading of [AS] §8 is the more expensive and more hazardous option. **4c:** the 13 new containers, plus `DataColumnSidecar(Fulu, Gloas)` — the PeerDAS sidecar is *also* fork-shaped at Gloas, which [AS]'s list omits. Throughout: the ordered `ChainConfig` fork accessors and deletion of the five duplicated schedule walks (P0-02 removes the preset-keying half in S0); total-coverage enforcement that fails unless every on-disk vector is claimed or skiplisted. | Gloas becomes a new module, not a 44-file retrofit |
| S5 | **Phases 5–7 on standard boundaries** | Replace the planned bespoke gRPC stubs with what the ecosystem standardizes: a REST beacon API, the standard BN↔VC line, and a remote-signer pattern for keys — with a **synchronous** EIP-3076 slashing-protection DB that never rides the write-behind path (**decision recorded now — P1-F/1**). | attestation, REST, block production — interoperable by construction |

#### P1-F — Decision records (1) · [Q4]

| ID | Requirement | Disposition |
|---|---|---|
| 1 | **Record the EIP-3076 slashing-protection DB decision as an ADR now, even though implementation is S5.** [Q4] recommends a dedicated `crates/slashing-protection` (SQLite, `POOL_SIZE=1`, `locking_mode=EXCLUSIVE`, fused `check_and_insert_*`) on the **validator-client** side of the Stage-5 boundary, and establishes why it must **not** ride `services/storage`: that path acknowledges up to **4 seconds** before it commits (`config/storage.toml:46`) — two thirds of a slot — and has a path that claims a failed flush durable (`write_behind.rs:653-657`, the same defect as P0-13). **Either inverts record-before-sign**, the one ordering that prevents a slashable signature. Implementation is **M** and belongs in S5; **the decision costs nothing today**, and the ADR is what stops someone reaching for the convenient write-behind path in six months. Note the contrast with P0-19/4: the pubkey cache *is* reconstructible from the state, so losing it is merely a slow boot and it **is** safe on the write-behind path — write that distinction into the module docs so the two cases are never conflated. Files under D-6's corpus **as `ADR-R-05`** — not `ADR-R-04`; see §10/J-18 for the collision and its resolution. | **write @ S0** (the ADR); implement @ S5 |

### 5.3 P2 — deferred / requires triage (54)

| Register | Count | Content | Disposition |
|---|---|---|---|
| **P2-A** | 8 | [RV] low correctness: `crates/store/src/window.rs:270` (slot-granular vs epoch-aligned retention floor) · `services/storage/src/prune/mod.rs:897` (TOCTOU lets the snapshot ring durably regress `PruneMarks`) · `services/p2p/src/gossip/validate/column.rs:401` (finalized-slot lower bound off by one) · `services/p2p/src/fork_digest.rs:350` (`FAR_FUTURE_EPOCH` fork entries treated as real boundaries) · `services/p2p/src/discovery/predicate.rs:79` (`column_predicate` matches column index against custody group ids without the group→columns mapping) · `services/p2p/src/das/sampling.rs:596` (`expire_task` lacks the M1 empty-required guard, emitting a **false `DataAvailable`**) · `services/engine/src/methods/new_payload.rs:311` (`hex_to_32` panics on non-ASCII `latestValidHash`) · `crates/types/src/config.rs:101` (`MAX_BLOBS_PER_BLOCK_ELECTRA` compiled in; the YAML key is silently ignored — **same class as P0-02 by a different mechanism (`P::MAX_BLOBS_PER_BLOCK_BASE`); discharged by P0-02, which must add the field anyway.** Row and count retained; see §10/J-11) | fold into the owning stage; **/8 → P0-02** |
| **P2-B** | 8 | [RV] low quality — **enumerated in §5.3.1** | opportunistic |
| **P2-C** | 1 | [RV] convention: `services/p2p/src/fault_mode.rs:16` — file-wide `#![allow(clippy::unwrap_used, expect_used)]` exempts ~1,600 lines of **production** code, far beyond the test-module allowance in `docs/dev-conventions.md` | with P1-D/18 |
| **P2-D** | 2 | [AS] §4 Low: **19** engine policy edges (fail-open fork-schedule default `osaka_time=0`; gate-bypassing Synced-edge fcU resend; terminal `AuthFailed` with no operator escape) · **20** brittle self-referential tests and observability smells ✳ (tests that grep their own source; 1,600-line god-metric facades; racy hand-rolled gauges; a lag metric that never emits — that last one is P1-B/1) | 19 `patch @ S1`; 20 opportunistic |
| **P2-E** | 35 | [RV] §4 unverified — **enumerated in §5.3.2** | **triage pass first** — 5 judged at `S0-B-17` (2026-08-16); 30 pending `S1-B-19`/`S1-B-20` |

**Total: 8 + 8 + 1 + 2 + 35 = 54.**

#### 5.3.1 P2-B — Low quality (8) · [RV] §3

| # | Location | Issue | Disposition |
|---|---|---|---|
| 1 | `crates/store/src/split.rs:453` | Ordering-critical `hot_column_root_end` duplicated verbatim across modules | `patch @ S2` |
| 2 | `services/storage/src/serve.rs:711` | `get_columns_by_root` reads every column twice through two divergent code paths | `patch @ S2` |
| 3 | `crates/state-transition/src/block/operations/deposit.rs:119` | `apply_deposit` reimplements `get_validator_index_by_pubkey`, bypassing scan accounting | with P0-02 |
| 4 | `crates/fork-choice/src/on_attestation.rs:526` | `apply_attestation_deltas` is a weaker parallel weight path with a doc that contradicts the code | opportunistic |
| 5 | `services/engine/src/methods/fcu.rs:324` | Documented transient fcU retry is unreachable on the production gated path | `patch @ S1` (with P2-D/19) |
| 6 | `crates/crypto/src/domain.rs:52` | cc-crypto defines its own `SigningData`, duplicating `cc_types::containers::SigningData` | `patch @ S4` (with P1-D/15) |
| 7 | `crates/bootstrap/src/serve.rs:207` | Span `Entered` guard held across every await in `serve_with_options_inner` | opportunistic |
| 8 | `Makefile:193` | `make deps/lint/ci` do not mirror the CI gates they claim to; a guard-script header claims wiring that does not exist | **promoted to P0-08(d)** |

#### 5.3.2 P2-E — Unverified (35) · [RV] §4 — the R-P2-triage checklist

**Not schedulable as work until promoted.** These exceeded [RV]'s verification cap and are reported
as-found; several look real and cheap. Each row below is promoted (with a tier and disposition) or
dismissed (with a reason) by R-P2-triage. `C` = correctness · `Q` = quality · `V` = convention.

| # | Location | Kind | Issue as-found |
|---|---|---|---|
| 1 | `services/chain/src/core.rs:776` | C | `CanonicalRoots` fabricates the head root for slots where `get_ancestor` fails. **`S0-B-17`: promoted P1 `patch @ S0`** (live site `:1031-1034`) |
| 2 | `services/engine/src/capabilities.rs:25` | C | `ADVERTISED_CAPABILITIES` includes unimplemented `eth_chainId` and non-Engine `eth_syncing` |
| 3 | `crates/state-transition/src/helpers/mutators.rs:152` | Q | Consolidation churn uses saturating arithmetic where the exit twin deliberately uses checked |
| 4 | `crates/types/src/state/caches.rs:492` | Q | Container-root computation swallows hasher errors into an all-zero state root |
| 5 | `crates/types/src/state/accessors.rs:65` | Q | `StateAccessError::from` maps unrelated `ssz_types` errors to `OutOfBounds { index: 0, len: 0 }` |
| 6 | `crates/types/src/config.rs:58` | C | Empty `BLOB_SCHEDULE` (spec-legal) rejected at load; `serde(default)` guarantees the confusing error. **`S0-B-17`: promoted P1 `patch @ S0`** |
| 7 | `crates/state-transition/src/signatures.rs:205` | Q | `push_randao_signature` swallows proposer-lookahead failure with `unwrap_or` |
| 8 | `crates/state-transition/src/helpers/accessors.rs:545` | Q | Unused helpers; `deposit_domain()` disagrees with the real deposit-domain computation — **`S0-B-17`: dismissed** (P0-02/`S0-A-09` closed the live path; helper has zero callers) |
| 9 | `crates/state-transition/src/shuffling.rs:271` | Q | `committee_from_shuffling` has no callers; duplicates `get_beacon_committee` with a weaker bounds check |
| 10 | `crates/state-transition/src/epoch/justification_and_finalization.rs:157` | Q | Private `block_to_epoch` copies shadow the shared `pub(crate)` helper |
| 11 | `crates/state-transition/src/epoch/proposer_lookahead.rs:54` | Q | Vestigial `last_epoch_start` suppressed with `let _` in the write loop |
| 12 | `crates/state-transition/src/block/operations/attestation.rs:165` | V | Test-only helpers exported unconditionally from the production API |
| 13 | `crates/fork-choice/src/store.rs:443` | Q | `clear_proposer_boost_root` has an if/else with byte-identical arms |
| 14 | `services/chain/src/import.rs:480` | Q | `pending_engine` parks a re-encoded block, violating the F2 arrival-bytes discipline |
| 15 | `services/engine/src/methods/fcu.rs:252` | Q | `prepare_fcu_params` version gate can never fire |
| 16 | `services/engine/src/inject.rs:157` | Q | `new_session_id` fallback entropy is always ~zero |
| 17 | `services/storage/src/writer.rs:275` | Q | `p0_capacity_hint` always returns 0; `map_put_error` has no callers |
| 18 | `services/storage/src/durable_set.rs:440` | Q | Snapshot degradation fallback can pick a **newer** ring member while reporting next-older |
| 19 | `services/storage/src/serve.rs:557` | Q | Serve error paths bypass `record_serve`, so `serve_total`/`serve_seconds` undercount failures |
| 20 | `services/storage/src/restore_client.rs:96` | Q | `push_restore_with_retry` retries non-retryable RPC failures for the full budget |
| 21 | `services/storage/src/prune/mod.rs:155` | Q | `PruneConfig::default` maps a failed floor computation to 0 — **maximally destructive retention**. **`S0-B-17`: dismissed** (`Default` cannot produce 0 from `(256, 65_536)`; production 0-floor is P1-A/5) |
| 22 | `services/storage/src/prune/mod.rs:57` | V | Blanket `#![allow(dead_code)]` on three production modules masks real dead code |
| 23 | `services/storage/src/columns.rs` / `crates/store/src/columns.rs:517` | Q | `columns_for_block` carries a dead `seen` bitmap |
| 24 | `services/p2p/src/reqresp/columns.rs:549` | Q | Dead requested/returned index tracking with a false comment, plus doc-comment debris |
| 25 | `services/p2p/src/clock.rs:168` | Q | `spawn_epoch_ticks` comment claims sub-slot polling but the period is exactly one full slot |
| 26 | `services/p2p/src/clock.rs:202` | V | Test modules use outer `#[allow]` instead of the prescribed inner `#![allow]` |
| 27 | `services/p2p/src/discovery/task.rs:559` | Q | Discovery loop awaits `find_node_predicate` queries inline, stalling shutdown / epoch ENR updates / event draining |
| 28 | `services/p2p/src/chain_stream/client.rs:715` | Q | Publish forward drop is silent; the comment describes retry/accounting logic that does not exist |
| 29 | `services/p2p/src/das/custody.rs:171` | Q | `CustodyManager::new` `# Panics` doc contradicts the silent `min()` clamp |
| 30 | `services/p2p/src/das/sampling.rs:679` | V | Test modules use outer `#[allow]` instead of inner `#![allow]` (`sampling.rs`, `inject.rs`, `server.rs`) |
| 31 | `bin/serve-probe/src/protocols.rs:429` | C | Empty `ColumnsByRootRequest` encodes 4 bytes instead of zero — **same class as P0-06**. **`S0-B-17`: promoted P0-class, discharged by `S0-B-07` (`5eebbbb`)** |
| 32 | `bin/serve-probe/src/probe.rs:407` | Q | `negative_verdict` labels a non-empty success chunk as "empty success" |
| 33 | `crates/bootstrap/Cargo.toml:34` | V | Dev-dependency `serde_json` pinned inline instead of `workspace = true` |
| 34 | `bin/devnet-gen/src/genesis.rs:167` | Q | GVR helper swallows list-overflow into an empty-list root; dead BLS-creds block above |
| 35 | `crates/types/src/sidecar/mod.rs:155` | Q | `empty_bitlist` helper duplicated verbatim in `operations.rs` and `sidecar/mod.rs` |

**P2 requirement R-P2-triage:** run one adversarial verification pass over all 35 rows above **before
Stage 2 opens**; each is promoted to P0/P1 with a disposition or dismissed with a recorded reason. No
unverified finding is silently dropped and none is scheduled as work until it is promoted. Rows **1,
6, 8, 21, 31** were pulled into S0 (`S0-B-17`) because 8 and 31 bear on P0-02 and P0-06; the remaining
30 stay pending `S1-B-19`/`S1-B-20`.

##### S0-B-17 early triage — 2026-08-16 · M12 35 → 30 unknown

Adversarial pass over the five pulled-forward rows. Line numbers below are the live sites on
`develop` `2fe9d0b` (several have drifted from the [RV] citations). Promoted rows become follow-up
issues against their owning stage and are **not** added to the P0=19 / P1=58 counts here (same rule
as `S1-B-19`/`S1-B-20`). This write does not ship those patches.

| # | Live site | Judgement | Tier | Disposition | Reason |
|---|---|---|---|---|---|
| **1** | `services/chain/src/core.rs:1031-1034` | **promoted** | P1 (C) | `patch @ S0` | Still `get_ancestor(head, s).unwrap_or(head)`. `ProtoArray::get_ancestor` fails only on `UnknownRoot`. `head_root_of` (`:838-843`) can fall through to `justified_checkpoint().root`. A miss therefore answers every unresolvable slot with that root — a silent lie to API consumers. Gap-fill's *durable* write of those roots is **not live today** (`write_behind.rs:1212-1217` does `let _ =` and comments the hole as later work); S2 deletes that path (`[ARCH]` §4.3) but the RPC survives. Replace the fallback with a typed error (or stop at the first unresolvable slot). Not patched in this PR. |
| **6** | `crates/types/src/config.rs:58-62` | **promoted** | P1 (C) | `patch @ S0` | Spec is explicit: *"The blob schedule MAY be empty"* ([fulu/beacon-chain.md](https://github.com/ethereum/consensus-specs/blob/master/specs/fulu/beacon-chain.md#blob-schedule)); `get_blob_parameters` already falls through to `(ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA)` on an empty list. The local CC-1G constructor rejects empty; `RawChainConfig.blob_schedule` is `#[serde(default)]`, so an omitted key and an explicit `[]` both fail with `BlobScheduleError::Empty`. Hoodi/mainnet fixtures are non-empty, so this does not fire on shipping networks, but it rejects a spec-legal config. `S0-A-07`/`S0-A-08` did not change this contract and have already shipped — follow-up loader patch, not this PR. Accept empty and use the existing Electra fallback; distinguish omitted-vs-empty only if a later issue wants a clearer error. |
| **8** | `crates/state-transition/src/helpers/accessors.rs:548-550` | **dismissed** | — | — | Checked against P0-02 after `S0-A-09` (`08e3861`). The live deposit domain is `deposit.rs:43`: `compute_domain(DOMAIN_DEPOSIT, Some(config.genesis_fork_version), None)`. `deposit_domain()` still calls `compute_domain(..., None, None)` (zero fork version) and has **zero callers** (grep). `state_get_domain` is used; only this helper is dead. Not a second P0-02 instance. Residual unused helper is opportunistic dead-code, not scheduled. |
| **21** | `services/storage/src/prune/mod.rs:148-151` | **dismissed** | — | — | Same *shape* as P1-A/5 (`S0-B-11`), not the same live path. `Default` calls `compute_min_epochs_for_block_requests(&BlockServeWindowCfg::new(256, 65_536)).unwrap_or(0)`. That compute cannot return `Err` on those constants: `checked_div(2)` never fails (divisor is 2) and `256 + 32_768` does not overflow `u64`, so the value stored is always **33_024**, never 0. Every `PruneConfig::default()` site is a `#[cfg(test)]` `..Default` fill in this file. Production builds the struct field-wise in `main.rs::prune_config` (that is P1-A/5). Residual `unwrap_or(0)` is opportunistic hygiene, not a 0-floor. |
| **31** | `bin/serve-probe/src/protocols.rs:416-418` | **promoted** | P0-class (P0-06 sibling) | **discharged by `S0-B-07`** (`5eebbbb`) | Confirmed the pre-fix shape: empty `ColumnsByRootRequest` wrote a lone `4u32` offset and returned it. Same class as P0-06 (probe encodes the list wrapper the spec does not). `S0-B-07` already shipped the independent fix — empty returns `Vec::new()`, asserted by `columns_by_root_empty_list_is_zero_bytes`; commit subject even names *"Empty ColumnsByRoot is zero bytes."* The issue said *if it promotes, it lands in `S0-B-07`*; that PR already landed, so **this PR does not re-patch**. P0 stays at 19 (J-11 rule: do not double-count a sibling discharged by an existing P0). |

`S1-B-19` covers rows 2–5, 7, 9–20. `S1-B-20` covers rows 22–30, 32–35.

---

## 6. Program shape

Six binaries stay buildable until the end, so every stage A/B's against the previous topology ([AS]
§8). The services are already thin hosts over library crates, so consolidation is mostly re-hosting
then deleting transport.

| Stage | Weeks | Contains | Ships | Entry gate |
|---|---|---|---|---|
| **S0 — correctness floor** | 1–3 | **16** P0 rows + the `patch @ S0` P1 line items + the P1-F/1 ADR | six services, now safe to operate | — |
| **S1 — fold the EL bridge** | 3–8 | P1-E/S1, P1-A/24–26, P2-D/19 | 3 containers · engine-fastpath DA works end to end | S0 green under `make ci` |
| **S2 — fold storage** | 8–16 | P1-E/S2, P0-18, storage `patch @ S2` rows | 2 processes · archive-hole class gone | **D-6 discharged** (ADR corpus exists) + P2 triage pass done |
| **S3 — wiring + first Hoodi soak** | 16–28 | P0-16, P0-17, P1-D/09, P1-D/12, `cc-wire` | a node that follows the chain, proven live | S2 shipped; D-2 criteria fixed in advance (§9) |
| **S4 — the fork seam** | after S3 | P1-E/S4 (**ordered 4a→4b→4c**), P1-D/10, P1-D/15, P1-D/16 | Gloas is a new module | Phase 1–4 clauses discharged; **P0-19/3 landed** (else milhouse's O(1) clone is a lie) |
| **S5 — Phases 5–7** | after S4 | P1-E/S5 | interoperable by construction | S4 shipped |

**Sequencing rule:** no row with disposition `deleted @ Sn` is patched. If a `deleted @ Sn` row becomes
operationally urgent before stage *n* lands, it is re-dispositioned to `patch @ S0 → deleted @ Sn` by
explicit decision, not by default.

---

## 7. Success metrics

Anchored on [AS] §9: *"Guard against declaring victory before a block actually imports end-to-end on
Hoodi."* Every metric below is falsifiable and has a named instrument.

### 7.1 Primary — the one that outranks everything

| # | Metric | Baseline (2026-08-15) | Target | Instrument | Stakeholder |
|---|---|---|---|---|---|
| **M1** | **A block produced by a foreign peer imports end to end on Hoodi and the node holds head.** Not a healthcheck, not a unit test, not a Grafana panel read by eye. | **never observed** — 0 live-network acceptance runs | ≥ 1 observed, then sustained | `docs/phase-2-soak.md` clause 2 (*DA-gated import*) + clause 3 (*head lag ≤ 1 typical*, bucket `le=1` ≥ 0.95) | Operator |

M1 is the gate on the word "works". No other metric may be reported as success while M1 is unmet.

### 7.2 Acceptance clauses discharged for real

The Phase 1–4 clause tables are fully built and entirely unexecuted. The falsifiable metric is the
count of **undischarged clause rows** going to zero.

**M2 counts clause rows, not `NOT_RUN` strings.** The unit is one row of a phase clause table (or, for
Phase 1, one named clause), which is the operator-meaningful unit and the thing a live window actually
discharges. **M2 reaches zero only by running windows — never by editing the docs.** A row edited to
remove `NOT_RUN` without a run is a §7.5 anti-metric violation, not progress.

| # | Metric | Baseline **✓** | Target | Instrument |
|---|---|---|---|---|
| **M2** | **Undischarged clause rows across Phases 1–4** — Phase 1: 3 named clauses (1 spec-vector suites, 2 ≥ 24 h Hoodi soak with sub-clauses 2/2–2/5, 3 timing budgets) · Phase 2: 9 rows (`phase-2-soak.md:474`) · Phase 3: 10 rows (`phase-3-acceptance.md:492`) · Phase 4: 11 rows (`phase-4-soak.md:1050`) + OQ-1 (`phase-4-soak.md:31`) | **34** — every one `NOT_RUN` | **0** | the four clause tables; `scripts/soak-report.sh --phase N` |
| **M2-scale** | Secondary, for scale only: **lines mentioning `NOT_RUN`** across the four phase docs | **294** (P1 36 · P2 76 · P3 89 · P4 93) | not a target — several lines are prose about the convention itself (`phase-4-soak.md:59`, `:523`) and some carry four cells on one line (`:322-323`), so this figure neither measures cells nor reaches zero | `grep -c NOT_RUN docs/phase-*.md` |
| **M2a** | Phase 1 — clause 1 spec-vector suites green both presets, **skiplist empty**; clause 2 ≥ 24 h continuous Hoodi soak; clause 3 timing budgets (epoch p95 ≤ 1000 ms, `process_block` p95 ≤ 400 ms) | all `NOT_RUN` | discharged | `docs/phase-1-soak.md` |
| **M2b** | Phase 2 — 9 clause rows incl. healthy peer count ≥ 25 **and** custody ≥ 8 over 24 h (`min_over_time`, one dip fails); 10-minute gap recovery within 32 slots; withheld-column deferral+recovery; scoring penalty crossing the −4000 bucket | all `NOT_RUN` | discharged at their named venues | `docs/phase-2-soak.md:474` clause table |
| **M2c** | Phase 3 — 10 clause rows incl. clause 1 `is_optimistic==0` for ≥ 99 % of samples over a ≥ 6 h / ≥ 20-finalized-epoch window with bootstrap excluded; clause 3 geth head within 1 block ≥ 99 % with zero `-38002`/`-38006`; clause 4 getBlobsV2 fastpath (non-zero complete + zero engine-sourced columns = **FAIL**) | all `NOT_RUN`, blockers B1–B5 | discharged on an exclusive machine | `docs/phase-3-acceptance.md:492` clause table |
| **M2d** | Phase 4 — 11 clause rows + OQ-1: clause 1 restart trials **20/20 ≤ 60 s** with `following_head=1` and identical `GetHead` roots; clause 2 cursor fallback in three stages (attribution / hole recorded / **hole closed** — discharged only when the third is present); clause 3 in two rows that must not be merged (compressed-retention plateau at `self-devnet-compressed`, **discharging**; Hoodi week marked `confirmation, non-discharging`) with four bars each (24 h slope < 1 % of plateau; prune/ingest within 5 %; prune deadlines < 1 %; hot-path p99 within 10 %); **clauses 4–7 full-window block and column serve** — the direct falsifier for P0-17c: today every serve answers `ResourceUnavailable`, and clause 6's negative side requires exactly that *only* below `eas`; clause 7 requires `cc_storage_earliest_available_slot == cc_p2p_*` advertisement; **OQ-1** foreign-peer probe against all five clients, 5–10 peers each | all 11 rows `NOT_RUN`; OQ-1 at **0 peers for all five clients** | discharged at their machine-checked venues | `docs/phase-4-soak.md:1050` clause table; `:31` OQ-1 |
| **M2e** | Codec validation against five foreign implementations (ADR P4-12) — the direct falsifier for P0-04/P0-05/P0-06 | 0 / 5 | 5 / 5 decode our requests and we decode theirs | `docs/phase-4-soak.md:68` |

### 7.3 Correctness and security

| # | Metric | Baseline | Target | Stakeholder |
|---|---|---|---|---|
| **M3** | High-severity findings open (P0 ledger) | 19 | 0 — each **closed by patch or deleted by a topology change**, with the discharging commit or stage recorded per row | Correctness owner |
| **M4** | Unauthenticated mutating gRPC ports reachable off-host | 6 (`9001`–`9006`, all `0.0.0.0`) **✓** | 0 | Operator |
| **M5** | Signature verification actually running on the production import path | RANDAO / attestation / exit / slashing / sync-aggregate **not verified**; proposer sig skipped on the unary path | all verified; `NoVerification` reachable only from restore-replay | Correctness owner |
| **M6** | Deposits accepted on a non-zero-GVF network | **0** — every PoP verifies under the wrong domain on Hoodi/Sepolia/Holesky | matches a reference client over the same window | Correctness owner |
| **M6b** | Config-scoped constants resolved from the compile-time preset — `constants::network::` call sites | **5 functions, ~20 call sites** ✓ᴸ | **0** — `pub mod network` deleted; omitting `&ChainConfig` becomes a compile error | Correctness owner |

### 7.4 Gate trustworthiness and the dead-wiring class

| # | Metric | Baseline **✓** | Target | Stakeholder |
|---|---|---|---|---|
| **M7** | Blocking CI gates red on the committed tree | **2** — `scripts/check-no-env-reads.sh` (exit 1, 4 hits) and `cargo fmt --check` (3 diffs) | **0**, and `make ci` mirrors the CI job list exactly | Engineering |
| **M8** | **Core-liveness probe**: a deadline-bounded no-op through the consensus core, so a parked core goes **red instead of green** ([AS] §9) | does not exist — the health DAG stays green for the one failure it cannot heal | exists, wired into the healthcheck, and demonstrated red against an injected engine black-hole | Operator |
| **M9** | Whole-node integration test importing a block from gossip receipt through to durable storage | **not writable** — the path crosses three process boundaries | exists and runs in CI | Engineering |
| **M10** | Dead internal edges | **4 of 6** dead, **1 of 6** unauthenticated **✓** | 0 dead; every surviving edge authenticated on the Engine-API JWT template | Engineering |
| **M11** | Unresolvable authority citations | **~745 occurrences** (541 `Architecture §` + ~204 ADR, the latter being 45 hyphenated + 159 spaced) across **~59 distinct ADR ids**, against **0** ADR files ✓ᴸ. *(Revision 1 reported 472 / 13 ids; it counted lines not occurrences and missed the dominant spaced `ADR P3-02` spelling — D-6.)* | every cited id resolves to a committed document **and the reconciliation table has no unclassified rows** — the achievable form of the gate, not "write 59 ADRs" | Engineering |
| **M12** | Unverified findings in an unknown state | 35 | 0 — each promoted with a disposition or dismissed with a reason. **5 judged at `S0-B-17` (2026-08-16); 30 remain for `S1-B-19`/`S1-B-20`.** | Engineering |
| **M13** | **`pubkey_cache_len` vs `validators_len` gauge** — the honest alarm for P0-19. Same species as M8: a failure the node currently cannot report. Note the existing `linear_scan_count` instrumentation is **silent in exactly this failure mode**, because `process_sync_aggregate` never scans — it errors ([Q3] §5). | does not exist | exists; alerts when `pubkey_cache_len < validators_len` on a state the core is importing against | Operator + correctness owner |

### 7.5 Explicit anti-metrics

These do **not** count as evidence of success, per [AS] §9 and the phase docs' own method sections:

- All six healthchecks green. The compose stack reports green today with a dead import path.
- A clause read by eye off a Grafana panel, or run at the wrong venue. Both fail to discharge.
- Any invented number. `NOT_RUN` with named blockers is the correct value until a real window runs.
- Unit or property tests passing. Every dead island in §1.A is well unit-tested.

---

## 8. Constraints and risks

| ID | Risk | Source | Mitigation |
|---|---|---|---|
| **R-1** | **Transport collapse silently changes backpressure semantics** — a `RESOURCE_EXHAUSTED` becomes a `TrySendError`. A gRPC-level rejection the caller handles becomes an in-process drop it does not. | [AS] §9 | **Type the contracts before moving them** (P1-D/11 lands before S1–S2 transport work). Put the strongest reviewer on every such PR. Treat any change in overflow behaviour as a spec change requiring an explicit decision, not an implementation detail. |
| **R-2** | **One process means one blast radius** — any panic restarts the whole node. | [AS] §9 | This is the posture all five production clients ship. **Accept it explicitly.** Keep the frontend behind a strict handle seam so the sandbox option (D-2) stays open. |
| **R-3** | **The wiring backlog, not the topology, is the long pole.** | [AS] §9 | M1 is the primary metric precisely so the program cannot declare victory on a topology diagram. Stage 3 is the longest stage (wk 16–28) and is scheduled as such. |
| **R-4** | **A parked consensus core reports healthy.** The health DAG is green for the failure it cannot heal and red for the one compose already restarts. | [AS] §6, §9 | M8 core-liveness probe. Ship it before S3, not after. |
| **R-5** | **The baseline is not green**, so "did this change break something?" is currently unanswerable. | D-5 **✓** | P0-08 first. No stage entry gate can be evaluated against a red baseline. |
| **R-6** | **Refactoring against ~745 unresolvable citations across ~59 phantom ADR ids** during the riskiest stage — ~5× the corpus revision 1 scoped. | [AS] §9, D-6 ✓ᴸ | D-6 is a hard gate on S2 entry **and a work item with its own estimate**, not a documentation chore. **Size it before S2 planning closes.** The reconciliation-table mechanism that makes it achievable is itself unwritten (D-6, §10/J-16) — so the first task is writing the mechanism, not classifying rows. |
| **R-7** | **Deleting the process boundary deletes the one real isolation it buys** — quarantining the git-pinned libp2p stack from the fork-choice store. | [AS] §6 Security lens, §7 | This is exactly what D-2 keeps open. Do not foreclose it during S1–S2. |
| **R-8** | **Repairing surfaces that a later stage deletes.** | §5.0 | The `Disposition` column is normative. `deleted @ Sn` rows are not patched without an explicit re-disposition. |
| **R-9** | **P0-02 has a documented-as-intentional trap, and it generalises across all five constants.** The offending call carries a doc comment stating the preset's `GENESIS_FORK_VERSION` is used *"so minimal vectors verify correctly"* (`deposit.rs:29-30`) **✓** — the bug is documented as a feature. A naive fix breaks the minimal-preset spec vectors. **The generalised form is worse:** [Q5] §1.2 verified that Hoodi uses mainnet's values for the other four constants, so a naive fix **passes on Hoodi and diverges on a customising devnet** — the venue where it would be caught is the one nobody runs. | verified in tree; [Q5] §1.2 | The fix must thread runtime config **and** keep both preset vector suites green — config plumbing, not a constant swap. Every new `ChainConfig` field takes `#[serde(default = …)]` at the mainnet value so no fixture breaks. Test against a fixture that *differs* from mainnet on all five, not only against `hoodi-config.yaml`. [Q5] §6 flags that `crates/types/src/preset.rs` was never audited for **other** config-scoped values that landed in the preset by the same mistake — **that audit is the first task of P0-02**, since the class is defined by the mismatch. |
| **R-11** | **P0-19's severity and its reach are both derived, not executed.** Verified ✓ᴸ: the `skip_deserializing` attribute, the missing scan fallback, and the two active call paths (`restore.rs:437`→`:525`, `replay.rs:644`→`:572`). **Derived, not executed:** that this means *every* block fails, and that it is what breaks restore. [Q3] §5 flags the first against itself — *"I read the code and believe it is unconditional, but I did not execute it against a Hoodi state. Run that before quoting me."* The block-replay branch has **zero test coverage**, which is consistent with the trace but is not confirmation. | [Q3] §5; revision-2 path trace | Close it with the ~30-line test [Q3] names: decode the committed Hoodi anchor state from SSZ and call `process_block` on a real block. **Run it before P0-19 is scheduled and before S2 deletes the restore surface** — it either confirms a live ship-blocker and a root cause for P1-A/22–23, or downgrades the claim. The fix is `S` either way, so the test does not gate the work; it gates the *claim* and the *causal attribution*. **Do not quote the severity outside this repo until it has run.** See §10/J-10, J-14. |
| **R-12** | **P0-19 fires on boot, so deferring it does not merely delay a Stage-3 problem — it leaves a live defect in every restart.** Revision 1 framed this as an ordering risk against P0-17a; the corrected framing is that P0-19 is already active on the restore and replay paths, and P0-17a only widens the blast radius to live import. | [Q3] §1.7; revision-2 path trace | Treat P0-19 as **non-deferrable out of S0** — the strongest such claim in the ledger, because it is the only P0 that is both firing today and invisible to every existing signal. If it somehow slips, P0-17a slips with it and M13 is the detector. |
| **R-13** | **S2 may delete the evidence for P0-19's causal claim — and the obvious test does not discharge it.** P1-A/22 and P1-A/23 are dispositioned `deleted @ S2` (the whole `restore.rs` surface goes away when boot opens redb in-process). If P0-19 is their common cause and that is never tested, the deletion **hides the bug rather than fixing it**, and it reappears in `beacon-core`, where restore-from-snapshot still decodes a state. **The sharper risk is a false diagnosis:** R-11's test confirms the *mechanism* (`CachePoisoned` fires on a decoded state) but **cannot** confirm the *attribution*. The two symptoms do not match the mechanism's shape — /22 is a notification gap (`end_stream` never notifies waiters) and /23 is a re-drive gap (DA-deferred blocks dropped), whereas `CachePoisoned` would make restore fail **earlier and differently** than either describes. Someone runs R-11, sees `CachePoisoned`, and marks /22 and /23 diagnosed when they have not been. | revision-2 path trace; §5.0/R-8 | **Two observations, not one, and both before S2.** (a) R-11's test — does `CachePoisoned` fire on a decoded state? Confirms the mechanism. (b) **A real restore run instrumented for where it actually fails** — does it fail at `process_block` before reaching `end_stream` at all? Only (b) confirms the attribution. If (a) passes and (b) shows restore reaching `end_stream`, then P0-19 is real **and** /22 and /23 are independent bugs, and deleting them at S2 without fixing them is the wrong call. Recorded on both rows as "**but diagnose first**"; this is the one place in the ledger where a `deleted @ Sn` disposition carries a diagnostic obligation ahead of it. **⚠ Normative, and the PRD governs where a plan disagrees: running observation (a) alone does NOT discharge the diagnostic obligation on P1-A/22 or P1-A/23, and no plan item that performs only (a) may mark them diagnosed. Observation (b) is a separate, independently-tracked piece of work, and the S2 deletion of both rows is gated on *its* conclusion — not on (a)'s.** |
| **R-10** | **`chain` currently cannot restart without external checkpoint re-sync**, and doing so permanently holes the archive. This makes S0–S2 operational rehearsals expensive. | [AS] §6 | S2 removes the cause. Until then, budget checkpoint re-sync time into every restart drill and record it in the Phase 4 clause-1 trial table. |

---

## 9. Open decision — the Stage 3 p2p endpoint

**Status: OPEN. Do not resolve before the first live Hoodi soak.**

| Option | What it is | Case for |
|---|---|---|
| **Single Hull** *(panel default, 2/3 votes)* | One `cc-node` binary + geth | Wins the panel on operability (9.0) and fork-evolvability (9.0); matches all five production clients; makes the dead-wiring bug class **unrepresentable** and the whole-node integration test finally writable ([AS] §7) |
| **Gatehouse & Keep** *(1/3 votes)* | Keep `p2p` as a separate process over a unix socket, sandboxed | Highest security score (9.0); quarantines the git-pinned libp2p stack from the fork-choice store; preserves the repo's best distributed-systems code — the jittered reconnect loop — verbatim ([AS] §7) |

The three-judge panel scored these **147–147** and voted 2–1 for Single Hull. Read the two plans and
they are the **same plan for their first three stages**; they diverge on exactly one question: *does
the p2p process survive?* Because the shared spine (D-1) is uncontested and the crate DAG keeps both
endpoints one mechanical stage apart, [AS] §1 and §7 both state the honest recommendation is **not to
pick today**.

### Exit criteria

[AS] identifies the first Hoodi soak as the decider but does not name what evidence decides it. The
following criteria are supplied by this PRD (see §10/J-4) and must be **fixed before S3 opens**, so
the decision is read off data rather than argued after it:

| # | Evidence to collect during the first live soak | Decides toward |
|---|---|---|
| **X1** | Count of libp2p-attributable panics/aborts observed in the `p2p` process over the soak window. Non-zero ⇒ each one would have taken down the fork-choice store under Single Hull. | > 0 → **Gatehouse & Keep** |
| **X2** | Whether the p2p↔chain seam exhibited backpressure-loss incidents once modeled as an in-process channel (R-1). Measure by instrumenting the existing gRPC edge for `RESOURCE_EXHAUSTED` frequency under real peer load. | frequent ⇒ typed in-process contracts must land first regardless of endpoint |
| **X3** | Whether the jittered reconnect loop survives an in-process port **unchanged**. If porting it requires behavioural change, the sandbox option preserves more value. | change required → **Gatehouse & Keep** |
| **X4** | Whether any dead-wiring regression recurs at the p2p seam during S3 despite the compile-time coupling of S1–S2. | recurrence → **Single Hull** |
| **X5** | Operator-observed cost of the split at S3: restart choreography incidents, lockstep-version incidents, and split-brain peer-state incidents attributable to the boundary. | material → **Single Hull** |

**Decision rule:** X1 and X3 are the only criteria that can carry Gatehouse & Keep on their own. If
both come back clean, the panel's 2–1 default (Single Hull) stands. Absent evidence is **not** a vote
for the default — an S3 that produces no measurement on X1–X5 does not discharge this decision.

**Constraint on S1–S2:** keep the frontend behind a strict handle seam (R-2, R-7) so the choice stays
one mechanical stage apart throughout.

---

## 10. Source disagreements, evidence gaps, and judgment calls

| ID | Item | Resolution |
|---|---|---|
| **J-1** | **[AS] §1 contradicts [AS] §4.** §1 claims fusing chain+storage+engine "deletes rather than patches **five of the eight** high-severity findings" and names them: the unauthenticated state-takeover restore path, the deadline-less engine hop, the event-ring-as-data-plane coupling, the broken cross-process restart choreography, and the `trusted_local` KZG-skip bypass. Mapping those onto the §4 ledger: only the engine hop is a §4 **High** row (03). Event-ring-as-data-plane is §4 **Med** (13). Restart choreography and `trusted_local` are **not named in the §4 ledger at all**. The restore-path takeover is a §6 Security-lens finding, folded into §4/06. | **The claim as stated does not hold against the §4 ledger.** This PRD does not write "five P0 rows are deleted at Stage 2." Instead each ledger row carries its own `Disposition`, and the §1 bullet list is carried in D-1 as an §1-only assertion. By ledger disposition, S1–S2 delete: P0-15 (partially — patched at S0 first), P1-A/4, P1-A/22, P1-A/23, P1-B/9, P1-D/13, P1-D/14, P0-07 and P1-A/27. That is a real and substantial deletion set — it is simply not the five §1 names. **This is the strongest internal disagreement between the sources and it is load-bearing for the central recommendation**, so it is flagged rather than smoothed. |
| **J-2** | **[RV] cites `deposit.rs:42`, [AS] cites `deposit.rs:44`** for the same bug. | **Not a disagreement.** Read in tree **✓**: `:42` is the opening of the `cc_crypto::compute_domain(` call; `:44` is the offending argument `network::genesis_fork_version::<P>()`. Both cite the same three-line expression. P0-02 records both. **Extended after the research stage:** this resolution stands, but both source documents undercount the *scope* — [Q5] §1 establishes that the deposit domain is one of **five** instances of the same preset-vs-config keying error, four of which have no runtime-config path at all. P0-02 is now a class requirement (§5.1.1), not a single-line fix. The line-number resolution above is unaffected. |
| **J-3** | **[AS] §3 says the serve window is "never published"; the tree shows an empty-window seed.** | Independently verified only the seed constant: `earliest_available_slot: u64::MAX` at `services/storage/src/serve.rs:1086-1087`, matching `p2p`'s `EMPTY_WINDOW_SLOT = u64::MAX` **✓**. That is *consistent with* but not proof of "never published". P0-17c cites the seed as verified and the never-published claim on [AS] §3's authority. **A verification pass should close this specific gap before S3.** |
| **J-4** | **Neither source names p2p-endpoint exit criteria.** [AS] says the first Hoodi soak decides it but never states what evidence would decide. | X1–X5 in §9 are **authored by this PRD**, not carried from a source. They are the largest piece of the document with no upstream authority and should be reviewed as such. |
| **J-5** | **[AS] §4/01 is an umbrella, not a finding.** Treating it as one P0 row would be unactionable; treating its six islands as six P0 rows would double-count against P0-16 and P0-10. | Decomposed into four sub-items under P0-17 (gossip subscribe, KZG pool, serve window, backfill client method), with the DA feed routed to P0-16 and proto-array prune to P0-10. Keeps the 22 → 18 arithmetic exact. |
| **J-6** | **Priority tier vs work sequence.** A mechanical P0/P1/P2 derivation from severity produces rows where a P0 is discharged by P1 stage work. | Accepted deliberately, and made explicit: severity sets the tier, the `Disposition` column sets the sequence. The alternative — re-ranking by schedule — would have hidden the ship-blocking status of the S3-scheduled dead islands (P0-16, P0-17), which are the two findings with the largest operator impact in the entire ledger. |
| **J-7** | **[RV]'s "highest-priority actions" list vs the study's emphasis.** [RV] ranks the compose port fix, `CoreConfig` default, deposit domain, wire encodings and fork-choice table growth as its top five. [AS] treats the dead islands and the DA loop as the headline. | Both are kept. The [RV] five are all `patch @ S0` and are the first work. The [AS] headline findings outrank them on operator impact but are Stage-3-scheduled because they cannot be honestly fixed before the spine exists. This is exactly the problem-A / problem-B split of §1. |
| **J-8** | **[RV] Vuln 1 and Vuln 2 recommend both a port fix and server-side hardening.** The brief's framing is that the two findings collapse to one near-one-liner. | Both are true and are separated: **P0-01** is the port fix (closes both HIGH findings, one line each in compose); **P1-A/1** and **P1-A/2** are the server-side validation (`root == hash_tree_root(block)` in `put_block`, KZG proof verification, anchor/frontier binding). The port fix does **not** wait on the hardening. |
| **J-9** | **Metrics ports `9101–9106` are also published unbound**, which neither source lists among the six bus ports. | [RV]'s own recommendation is that "only beacon-api and metrics scrape targets should be host-reachable, **and those on loopback**". P0-01 therefore covers `9101–9106` → `127.0.0.1` as well, matching `devnet/compose.yml:90-91`'s `SEC-H1` pattern **✓**. |
| **J-10** | **P0-19's severity claim is derived, not executed** — the same species of gap as J-3. Verified ✓ᴸ: the `#[ssz(skip_serializing, skip_deserializing)]` attribute at `state/mod.rs:106`, the `.ok_or(BlockError::CachePoisoned)?` with no scan fallback at `sync_aggregate.rs:125-129`, and (revision 2) the two active call paths. **Not** verified: that this means *every* block fails. [Q3] §5 flags this against itself — the loop resolves all `sync_size` indices before checking participation bits, which [Q3] read as unconditional but did not execute. | **Recorded as asserted-pending-execution, not as verified.** The code facts carry ✓ᴸ; the severity carries [Q3]'s authority with the gap named. R-11 schedules the ~30-line closing test **before** P0-19 is scheduled. This does not change the disposition — the fix is `S` regardless — it changes what may be claimed about it. **Revision 2 raised the stakes without raising the confidence:** the finding moved from latent to active, but the evidence is still a code-path trace. Both moved together, deliberately. |
| **J-14** | **Revision 1 of this PRD stated P0-19 was latent. That was wrong, and it was this document's error, not a source's.** [Q3] §1.7 says the finding is latent *because gossip is unwired* — true for the **live-import** path, and this PRD generalised it to the whole finding without checking the boot paths. `restore.rs:437`→`:525` and `replay.rs:644`→`:572` both run the STF on a decoded state in production today ✓ᴸ. | **Corrected in place, with the error recorded rather than quietly overwritten** (§0 revision 2, §5.1.2/1). Two consequences follow that a silent fix would have lost: (a) P0-19 is the **only P0 that is both firing today and invisible to every existing signal**, which makes it non-deferrable in a way no other row is (R-12); (b) it becomes a **candidate common cause for P1-A/22 and P1-A/23**, the restore findings — and those are dispositioned `deleted @ S2`, so the surface that would prove or disprove it is scheduled for deletion. R-13 and the "**but diagnose first**" note on both rows exist to stop that. **The causal claim is a hypothesis, not a finding** — it is worth testing precisely because it is cheap to test and expensive to be wrong about. |
| **J-15** | **The two sources of the corrected ADR census disagree by ±1 id and ∓3 citations.** The team lead's verified count is **~59 distinct ids / ~204 citations** (45 hyphenated + 159 spaced); `plan/architecture.md:1494` states **58 ids / 207 citations**. | **Both recorded; neither adjudicated.** The figures are stated as "~59" and "~204" with the disagreement noted here, because the conclusion is identical under either: the corpus is ~4.5× the id count and ~5× the citation count that revision 1 scoped, and the S2 entry gate needs its own estimate. Adjudicating ±1 id would be false precision on a number whose only load-bearing property is its order of magnitude. If the reconciliation table is ever built, it settles this as a side effect — it must enumerate every id to classify them. |
| **J-16** | **The proposed mechanism for discharging D-6 does not exist yet.** `plan/architecture.md` cites its own **§10** ten times — for the reconciliation table (*re-derivable* / *needs-decision* / *stale-citation*) and for three new ADRs `ADR-R-02`, `ADR-R-03`, `ADR-R-04` — but the document ends at §9.2 with a `<!-- SECTION-10-ANCHOR -->` placeholder ✓. | **Cited as proposed, not as available**, in D-6 and R-6. Noted plainly because it is not a clerical gap: the architecture document has **reproduced the exact bug class D-6 describes**, one revision after the class was documented, which is the strongest available evidence that the citation habit is systemic rather than historical. The first task under D-6 is therefore writing the mechanism, not classifying rows against it. Three of the ids the corpus now needs (`ADR-R-02/03/04`) were created by the document that proposes to reconcile them. |
| **J-18** | **Two ADR id collisions in the new `ADR-R-*` series, found by the estimator.** (a) The slashing-protection record (P1-F/1) is cited as both `ADR-R-04` and `ADR-R-05`. (b) **`ADR-R-05` is itself double-booked**: `[ARCH]` §10.4 routes `ADR-P3-15`'s replacement decision to it while §10.5 already assigns it to slashing protection. (b) is the estimator's own discovery and **appears nowhere upstream**. | **Resolved here so the ids stop drifting:** the slashing-protection record is **`ADR-R-05`**, per `[ARCH]` §10.5's enumeration — P1-F/1 is updated accordingly. `ADR-P3-15`'s successor becomes **`ADR-R-07`**. Stated plainly because of what it means, not because renumbering is interesting: **these are brand-new ids joining the phantom corpus that the entire D-6 gate exists to eliminate**, and they collided before a single one of them was written to a file. A corpus of ~59 unresolvable ids is not a historical accident being cleaned up — it is a live, still-growing habit. D-6's first task is the mechanism (§10/J-16); this is the second piece of evidence in two revisions that the mechanism is overdue. |
| **J-17** | **[AS] §8's "per-fork STF dispatch" reads literally as Grandine's per-fork modules, which is the more expensive option and is self-defeating here.** [Q5] verified Lighthouse uses monotone `ForkName` capability predicates in one generic function instead. | **This PRD adopts the predicate form and re-sizes the line L → M** (P1-D/16, P1-E/S4). The deciding argument is not cost: per-fork modules copied N times **reintroduce the same N-copies synchronisation hazard as the four-place `BeaconState` schema** in P1-D/15 — a hazard this program exists to delete, and one where drift yields a wrong state root rather than a compile error. Adopting the literal reading of [AS] §8 would have made S4 both more expensive and less safe. Recorded as a deliberate departure from the source's phrasing. |
| **J-11** | **P2-A/8 (`crates/types/src/config.rs:101`) is the same class as P0-02 but reaches the preset by a different mechanism** (`P::MAX_BLOBS_PER_BLOCK_BASE` rather than a `pub mod network` function), and [Q5] §1.5 step 1 lists `max_blobs_per_block_electra` among the fields P0-02 must add anyway. | **Kept as a P2-A row, marked discharged by P0-02; P2-A stays at 8 and P2 stays at 54.** It remains a distinct [RV] finding with its own `file:line`, so deleting it would break the mechanical derivation from the source documents. Re-tiering it to P0 would double-count the same work. Recording the linkage is the honest option. |
| **J-12** | **[Q5] §1.4 reports a third live defect that is *not* in the class P0-02 was widened to cover.** Today's upstream `configs/mainnet.yaml` carries `SLOT_DURATION_MS: 12000` and **no `SECONDS_PER_SLOT`**, while `RawChainConfig.seconds_per_slot` has no `#[serde(default)]` — so **loading the current upstream mainnet config fails to parse**. The repo's own Hoodi fixture carries both keys, which is why nothing has caught it. The same file also declares `GLOAS_FORK_*` and `HEZE_FORK_*`, which `ChainConfig` has no fields for and silently ignores. | **Noted under P0-02 as landing in the same edit, but deliberately not folded into it, and not tiered.** P0-02's class is "the reader takes a config value from a preset key" (plus [Q5] §1.3's loader half, which *is* in scope). A missing `serde` default is a third thing. **This may warrant its own P0 — that is the team lead's call, not this document's.** Flagged rather than silently absorbed or dropped. |
| **J-13** | **P0-19 is numbered 19 rather than inserted near the other state-transition rows**, which reads out of severity order. | Deliberate. The 18 existing P0 ids are cross-referenced from §5.1.1, §6, §7, §8, §10 and Appendix A. Renumbering to place P0-19 "correctly" would silently invalidate every one of those references for a cosmetic gain. Appending keeps the derivation auditable: ids 01–18 are exactly the [AS]/[RV] set, and anything ≥ 19 came from a later stage. |

---

## Appendix A — P0 de-duplication map (22 raw → 18, +1 = 19)

| Raw finding | Source | → Requirement |
|---|---|---|
| Vuln 1 auth_bypass `services/chain/src/service.rs:286` | [RV] sec | **P0-01** |
| Vuln 2 validation_bypass `services/storage/src/serve.rs:767` | [RV] sec | **P0-01** *(collapse 1 of 4)* |
| 06 trust boundary is "the compose network" (a: `0.0.0.0`) | [AS] High | **P0-01** |
| 06 trust boundary (b: three dead edges from missing env overrides) | [AS] High | **P0-07** |
| HIGH `docker-compose.yml:30` three missing overrides | [RV] corr | **P0-07** *(collapse 2)* |
| HIGH `deposit.rs:42` deposit PoP domain | [RV] corr | **P0-02** |
| 04 deposit domain preset-keyed | [AS] High | **P0-02** *(collapse 3)* |
| HIGH `core.rs:188` `NoVerification` default | [RV] corr | **P0-03** |
| HIGH `blocks.rs:46` `BeaconBlocksByRange` | [RV] corr | **P0-04** |
| HIGH `blocks.rs:148` `BlocksByRoot` | [RV] corr | **P0-05** |
| HIGH `protocols.rs:258` probe reproduces the bug | [RV] corr | **P0-06** |
| 05 interop-breaking encodings + triplicated codec | [AS] High | **P0-04/05/06** *(collapse 4)*; codec-unification tail → **P1-B/6** (`cc-wire`, S3) |
| HIGH `check-no-env-reads.sh:17` red blocking gate | [RV] corr | **P0-08** (+ [RV] convention `cargo fmt --check`; + [AS] 17a) |
| HIGH `on_block.rs:447` vote/balance tables | [RV] corr | **P0-09** |
| 07 unbounded fork-choice growth + validator drift | [AS] High | **P0-09** (drift half) + **P0-10** (`prune` half) |
| HIGH `import.rs:697` proposer lookahead | [RV] corr | **P0-11** |
| HIGH `core.rs:527` slot clock | [RV] corr | **P0-12** |
| HIGH `write_behind.rs:657` flush-error data loss | [RV] corr | **P0-13** |
| HIGH `canonical.rs:91` no canonical delete | [RV] corr | **P0-14** |
| 03 no engine deadline + `block_on` panic | [AS] High | **P0-15** |
| 02 DA signaling has no production path | [AS] High | **P0-16** |
| 01 dead library islands | [AS] High | **P0-17** (4 sub-items; DA → P0-16, prune → P0-10 — see J-5) |
| 08 storage scale time bombs | [AS] High | **P0-18** |
| — *(no [AS]/[RV] source)* — pubkey cache empty on every decoded state | **[Q3]**, research stage | **P0-19** |

**Arithmetic, in two lines so the derivation stays auditable:**

1. 2 security + 12 HIGH correctness + 8 [AS] High = **22 raw findings** → 4 collapses → **18 P0
   requirements**. (The table above has 23 [AS]/[RV] rows because [AS] finding **06** is shown twice —
   once for its `0.0.0.0` half and once for its missing-env-override half — and [AS] **07** maps to two
   requirements from one row. Distinct raw findings remain 22.)
2. **+1 from the research stage**, present in neither source document (P0-19, from [Q3]) = **19 P0
   requirements**.

**Scope and severity changed without changing the count.** Three ledger entries grew after their
derivation. Recorded here so the ledger is not read as unchanged:

| Requirement | Count | What changed |
|---|:--:|---|
| **P0-02** | unchanged | Derived from two raw findings that each describe a single deposit-domain bug. [Q5] §1 establishes it is a **five-instance class** (§5.1.1) — one requirement, five instances, larger work than the [AS]/[RV] derivation implies. §10/J-2. |
| **P0-19** | unchanged | Revision 1 recorded it as latent pending P0-17a. Revision 2 corrects it to **firing today** on the restore and replay boot paths, and adds a candidate causal link to P1-A/22–23. Same requirement, materially higher severity and urgency. §10/J-14. |
| **P1-D/17a** (D-6) | unchanged | The phantom corpus is **~59 ids / ~745 citations**, not 13 / 472 — ~5× the scoped size, promoting it from a documentation chore to **a work item needing its own estimate**. §10/J-15. |

**No requirement was added, removed, or re-tiered in revision 2.** Counts remain **P0 19 · P1 58 ·
P2 54 · total 131**.

## Appendix B — Verification method (2026-08-15)

Two verification passes, by two different parties, at the same tree revision `4146791`. They are
recorded separately and marked differently on purpose: **✓** and **✓ᴸ** are not interchangeable.

### B.1 — Verified by the PRD author (marker **✓**)

Re-checked against the working tree at `4146791`:

| Claim | Command / read | Result |
|---|---|---|
| fmt gate red | `cargo fmt --all -- --check` | 3 diffs, `bin/cc-store/src/lib.rs:13,52,138` |
| env gate red **and blocking** | `bash scripts/check-no-env-reads.sh; echo $?` | exit **1**, 4 production hits |
| bus ports unbound | `grep -n 'ports:' docker-compose.yml` | `9001`–`9006` + `9101`–`9106`, all unbound |
| `SEC-H1` precedent exists | `devnet/compose.yml:11-12,90-91` | `127.0.0.1` publishes, documented |
| gossip subscribe has zero senders | `grep -rn 'SwarmCommand::Subscribe' services/ crates/` | exactly one occurrence — the handler at `host.rs:1299` |
| KZG sender dropped | read `services/p2p/src/service.rs:714` | `kzg_tx: _` |
| `da_tx` optional and unset in production | `grep -rn 'da_tx'` | `das/sampling.rs:154,200,538`; `engine_stream/server.rs:61` documents the gap |
| backfill client method absent | `grep -rn 'PutBackfillBatch\|put_backfill_batch' services/p2p/` | only doc/comment references, no call site |
| serve-window empty seed | read `services/storage/src/serve.rs:1086-1087` | `earliest_available_slot: u64::MAX` — see J-3 |
| phantom ADR corpus — **superseded, see B.2** | `grep -rc 'Architecture §'`, `grep -rho 'ADR-[A-Za-z0-9_-]*' \| sort -u`, `find -iname '*adr*'` | 434 + 38 citations across 13 ids, **0** ADR files. **⚠ This row undercounted by ~5×.** The regex matched only the hyphenated `ADR-` spelling and missed the dominant spaced `ADR P3-02` form, and `grep -rc` counted *lines containing* the string rather than *occurrences* of it. The zero-ADR-files half stands. Corrected in B.2. |
| `architecture.md` §10 is unwritten | `grep -c '§10' plan/architecture.md`; read tail | **10** citations of §10 (reconciliation table, `ADR-R-02/03/04`); document ends at §9.2 with `<!-- SECTION-10-ANCHOR -->` — §10 absent (§10/J-16) |
| deposit line numbers | read `crates/state-transition/src/block/operations/deposit.rs:27-47` | `:42` call, `:44` argument; doc comment at `:29-30` documents the bug as intentional (R-9) |
| `NOT_RUN` census | `grep -c NOT_RUN docs/phase-*.md` | P1 36 · P2 76 · P3 89 · P4 93 = **294** |

### B.2 — Verified by the team lead during the research stage (marker **✓ᴸ**)

Verified against the working tree by the team lead and handed to this document as established fact.
**Not re-checked by the PRD author** — the marker records whose verification stands behind each claim.

| Claim | Evidence `file:line` | Feeds |
|---|---|---|
| `caches: StateCaches<P>` carries `#[ssz(skip_serializing, skip_deserializing)]`, so every SSZ-decoded `BeaconState` has an empty pubkey cache | `crates/types/src/state/mod.rs:106` | P0-19 |
| `process_sync_aggregate` resolves committee indices only through the map, with **no scan fallback** — `state.caches().pubkeys.get(pk).ok_or(BlockError::CachePoisoned)?` | `crates/state-transition/src/block/sync_aggregate.rs:125-129` | P0-19 |
| `pub mod network` has exactly five functions, **all** keyed on the compile-time `P::NAME` preset | `crates/state-transition/src/helpers/constants.rs:128-172` | P0-02, §5.1.1, M6b |
| `ChainConfig` parses only **one** of the five (`genesis_fork_version`); the other four have no runtime-config path | `crates/types/src/config.rs:144` | P0-02, §5.1.1 |

**Revision 2 additions:**

| Claim | Evidence `file:line` | Feeds |
|---|---|---|
| Restore decodes with raw `BeaconState::<P>::from_ssz_bytes(input.state_ssz)` and **feeds `on_block`** — so the STF runs on a cache-empty state in production today | `services/chain/src/restore.rs:437` → `:525` | P0-19, R-12, R-13, §10/J-14 |
| Storage replay decodes and reaches `state_transition(`, which appears **exactly once** in that file (i.e. the production call, not a test) | `services/storage/src/replay.rs:644` → `:572` | P0-19, R-12 |
| ADR citations: **45** hyphenated (`ADR-`) + **159** spaced (`ADR P3-02`) = **~204**, across **~59 distinct ids**, against **0** ADR files | repo-wide | D-6, M11, R-6, P1-D/17, §10/J-15 |
| `Architecture §`: **439 lines** contain it; **541 occurrences** — occurrences is the basis used in D-6/M11 | repo-wide | D-6, M11 |

**Not verified, and stated as such:** that the pubkey-cache stall is *the* cause of the restore
failures (P1-A/22, P1-A/23). That is a hypothesis derived from the two path traces above — see R-11,
R-13 and §10/J-14. The block-replay branch has **zero test coverage**, which is consistent with the
trace but is not confirmation.

### B.3 — Recovered by the estimator during decomposition (marker **✓ᴱ**)

P0-10 and P0-18 were the only two rows in the P0 ledger carrying no `file:line` — both cited a source
section plus a crate name, which made them unestimatable. The estimator located the evidence by grep
while breaking them into issues and handed it back. **These were located, not adversarially verified,
and were not re-read by the PRD author** — the marker is deliberately weaker than **✓** and **✓ᴸ**.

| Claim | Evidence `file:line` | Feeds |
|---|---|---|
| `ProtoArray::prune` exists and has no production caller | `crates/fork-choice/src/proto_array.rs:325` | P0-10 |
| The FINALIZED event to drive it from | `services/chain/src/events/mod.rs:206` | P0-10 |
| Interned table-name exhaustion (~30 days uptime) | `crates/store/src/keys.rs:51,56,61` | P0-18a |
| Contig-walk cap below the full serve window | `crates/store/src/invariants.rs:271,332,596-624` | P0-18b |
| Multi-GB invariant scan blocking `open` | `crates/store/src/invariants.rs:45-50` | P0-18c |

### B.4 — Asserted, not verified

Everything not marked **✓** or **✓ᴸ** is carried on the authority of [AS], [RV] or a research brief and
was not independently re-derived for this PRD. Three such claims are load-bearing enough to be named
with their closing action:

| Claim | Authority | Gap | Closing action |
|---|---|---|---|
| The serve window is "never published" | [AS] §3 | Only the empty-window seed constant was verified ✓ | §10/J-3 — verification pass before S3 |
| P0-19 fails **every** block, not merely some | [Q3] §1.4, §5 | Derived from reading the loop; never executed | §10/J-10, R-11 — ~30-line test: decode the committed Hoodi anchor state, call `process_block` on a real block |
| P0-19 is **the cause** of the restore choreography never working (P1-A/22, P1-A/23) | revision-2 path trace | Two call paths verified ✓ᴸ; the causal attribution is inferred from them, not observed. Block-replay branch has zero test coverage | §10/J-14, R-11, R-13 — same test, run **before S2 deletes the restore surface** |
| Fusing the spine deletes "five of the eight" high-severity findings | [AS] §1 | Does not survive contact with [AS]'s own §4 ledger | §10/J-1 — superseded by per-row dispositions |
