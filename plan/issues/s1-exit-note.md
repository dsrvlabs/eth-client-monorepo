# S1 exit note

Living record for the S1 exit criteria. Later issues append; they do not rewrite
prior conclusions.

Recorded 2026-08-16 on `feature/s1-a-19-s1-exit-ab` at
`be1f2d8097b5ab9f32a9074374fa0f1096ef8790` (plus this note). `S1-A-19` owns
E1.1 and the E1.5 line in this file.

**This worktree did not run a loaded six-service / 3-container A/B.** No
`docs/s1-e11-*` scrape is committed. Do not invent numbers. Do not treat S0's
idle Family 1 zeros / Family 2 histogram seed as a loaded distribution to
match.

---

## E1.1 — §9.0 A/B (`S1-A-19`)

**Conclusion: procedure and blocker rule recorded. Loaded A/B not executed.
E1.1 is not discharged.**

Same procedure as `S0-B-19` (`[ARCH]` §9.0), then **diff S1 T≥1h against the
S0 T≥1h absolute numbers**, not against S0's same-commit T0→T≥1h delta.

S0 baseline: [`docs/s0-e07-ab-baseline.txt`](../../docs/s0-e07-ab-baseline.txt)
(commit `e2ad297072d07ab00cb328e5e12549d9c210ba8d`, wall **3632 s**). Quoted
from that record and [`s0-exit-note.md`](s0-exit-note.md) E0.7:

- Family 1 `cc_chain_import_total{result=imported}` T≥1h = **0** (and the
  other five result labels 0). There is **no** series named
  `cc_chain_import_result`.
- Family 2 `cc_p2p_head_lag_slots_count=1` (histogram **seed only**) and
  `cc_chain_head_lag_slots=0`.
- Family 3 overflow counters all **0**. `dangerous_case=no`.
- Six-service `cc_chain_head_slot=0`. The six-service stack was **not** on
  the self-devnet mesh. E1 p2p→chain is dead; the self-devnet is p2p-only.
  S0 Family 1 imported 0 **because the stacks were not meshed**.

Matching those idle zeros is **not** E1.1 clean.

### Blocker rule (`[ARCH]` §9.0/4) — stated as acceptance

Diff **three families** (absolute numbers, not "no significant change"):

1. Import verdicts: `cc_chain_import_total{result=*}` on six-service `:9101`
   (OpenMetrics `_total` suffix). The plan name `cc_chain_import_result{*}`
   does not exist on the scrape.
2. Head-lag: `cc_p2p_head_lag_slots_bucket` on six-service `:9102` and
   self-devnet `:19102`/`:19112`/`:19122`, plus gauge
   `cc_chain_head_lag_slots` on `:9101`.
3. §2.2 overflow: `*_rejected_backpressure`, `*_dropped`, subscriber
   terminations, and `cc_grpc_requests_total{code="8"}` if present. Exact
   names S0 found are listed in the baseline file.

**A non-zero overflow-family (3) diff with a zero diff in families 1 and 2
is a stage blocker, not a curiosity.** Behaviour unchanged at test load and
the *contract* changed — the R-1 failure S1's typing work exists to prevent.
`scripts/s0-ab-baseline.sh --record` prints `dangerous_case: yes` for that
shape.

A loaded pass requires families 1 and 2 to be **import / head-lag
distributions under mesh**, then compared to S0 T≥1h. S0 T≥1h is idle;
the first honest loaded window is the new baseline later stages (E2.1 …)
diff against. Until that window exists, E1.1 stays open.

### Procedure (not executed here)

Both topologies from the **same** S1 commit (`[ARCH]` §9.0/1). S1 keeps
`services/engine` as a workspace member so the previous engine-container
topology still builds (`[ARCH]` §9.1; leftover gRPC shell in
`services/engine/src/main.rs`). After `S1-A-06`, production E3 is
in-process `DirectEngine` (`services/chain/src/engine.rs`). The six-service
compose is still the A/B host. The 3-container self-devnet is
`devnet/compose.yml` (publisher, node-a, node-b; plus anchor).

Helper: `scripts/s0-ab-baseline.sh` (fail-closed, loopback-only HTTP, not
in `make ci` / `make lint`). `--wait` is explicit; never implicit 1 h.

```text
make build
# six binaries under target/debug/: cc-chain cc-p2p cc-attestation
# cc-engine cc-beacon-api cc-storage

COMPOSE_PROJECT_NAME=s1a19-ab docker compose build && docker compose up -d
bash scripts/wait-healthy.sh 180

CC_DEVNET_MAX_SLOTS=2000 ./devnet/up.sh
# 3-container mesh: publisher + node-a + node-b (+ anchor). Reuse the same
# 64-slot fixture S0 used if regenerating; do not change fixtures or EL snap
# between topologies.

bash scripts/s0-ab-baseline.sh --scrape --label T0 --raw-dir docs/s1-e11-run/t0
bash scripts/s0-ab-baseline.sh --wait 3610
bash scripts/s0-ab-baseline.sh --scrape --label T1h --raw-dir docs/s1-e11-run/t1
bash scripts/s0-ab-baseline.sh --record \
  --t0-dir docs/s1-e11-run/t0 \
  --t1-dir docs/s1-e11-run/t1 \
  --out docs/s1-e11-run/families.txt
```

Then:

1. Confirm six-service `chain`/`p2p`/`attestation`/`engine`/`beacon-api`/`storage`
   healthy for the whole window. EL gate is `service_started` (ADR-P3-14).
2. Confirm self-devnet publisher + node-a + node-b + anchor stayed up.
   Quote mesh gossip (`cc_p2p_gossip_messages_total`) so the run is not an
   idle six-service scrape.
3. Diff S1 T≥1h against S0 T≥1h (`docs/s0-e07-ab-baseline.txt`) for the
   three families. Apply the §9.0/4 blocker.
4. Paste absolute numbers into this note. Do not summarise as "no
   significant change".

If six-service `cc_chain_import_total{result="imported"}` is still 0 after
≥1 h, the stacks were not meshed — same defect S0 recorded. That is **not**
a loaded Family 1 match.

### What this worktree demonstrated (not a scrape)

| Check | Result |
|---|---|
| `bash scripts/s0-ab-baseline.sh --self-test` | `ok: self-test` (dangerous-case yes when only family 3 moves; no when 1–2 move) |
| `docker compose -f docker-compose.yml config --services` | `chain` `p2p` `attestation` `engine` `beacon-api` `storage` `el` |
| `docker compose -f devnet/compose.yml config --services` | `publisher` `node-a` `node-b` `anchor` |
| `make build` / six `target/debug/cc-*` | **not run** (empty `target/`) |
| six-service `up` + `./devnet/up.sh` + ≥1 h scrape | **not run** |
| Host has leftover `s0b19-ab-baseline-*` / `cc-devnet-*` images | S0 commit `e2ad297`, not this tree. Not started. |

---

## E1.2 — M8 demonstrated red (`S1-A-17`)

**Conclusion: landed.** Production-budget integration test; no live compose
scrape (do not fake one; do not paste the CI `eprintln` as a scrape).

Issue: `S1-A-17`. Test:
`services/chain/tests/engine_blackhole_liveness.rs`,
`black_holed_new_payload_flips_production_probe_budget`. Commit
`f083af958d0d602cd767a4c0c485db4f4a43309d`.

```text
cargo test -p cc-chain --test engine_blackhole_liveness \
  black_holed_new_payload_flips_production_probe_budget -- --nocapture
```

Budget is production ADR-R-04 (N=3, Hoodi deadline ~3999.6 ms, interval 3 s)
plus hang `> 8 s × N` (30 s). When it flips: tonic aggregate `""` =
`NOT_SERVING` (same bit `grpc-health-probe -addr=:9001` reads). FQ
`eth.chain.v1.ChainService` stays SERVING. GetHead still answers
(ADR-P1-09). Idle core + black-holed engine stays SERVING (ADR-P3-02).
Production `DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT` / `TransportTimeouts` stay
8 s (P0-15).

Compose overlay exists (`docker-compose.engine-blackhole.yml`;
`devnet/faults.sh engine-blackhole`). Not executed here.

```bash
docker compose up -d --build
bash scripts/wait-healthy.sh
# Core must be installed (restore / checkpoint). Then inject the sink:
docker compose -f docker-compose.yml -f docker-compose.engine-blackhole.yml \
  up -d --build --no-deps engine
docker compose exec -T chain /usr/local/bin/grpc-health-probe -addr=:9001
```

Live production stack (8 s RPC caps + N=3 ≈ 18 s): **one** in-flight
newPayload/fcU stays `SERVING`. `NOT_SERVING` only if the core stays off
the tick lane across N=3 production samples.

---

## E1.3 — `cargo test -p cc-seam` both impls (`S1-A-12`)

**Conclusion: landed.** Eleven named cases in
`crates/seam/src/conformance.rs` (`[ARCH]` §2.1). Cases 1–8 share one
assertion on `InProcess` and `Ipc` / `IpcEgress`. Case 9 is an impl split
(`column_does_not_ride_import_lane_in_process` /
`column_admits_on_live_session_ipc`). Cases 10–11 stay in `cc-chain`
(policy B `events::slow_subscriber_is_terminated_not_stalled`, policy D
`core::slot_tick_is_never_shed`).

CI: `.github/workflows/ci.yml` job `test` runs
`cargo test -p cc-seam --locked`. Local: `make test-seam`.

Not re-compiled in this worktree (no `target/`). Evidence is the committed
suite + CI wiring, issue `S1-A-12` (commit
`be1f2d8097b5ab9f32a9074374fa0f1096ef8790`).

---

## E1.4 — JWT rule names `cc-engine-api`; `cc-chain` not grandfathered (`S1-A-01`)

**Conclusion: landed.** Re-checked this worktree.

`scripts/check-crate-dag.sh`: only `cc-engine-api` (and transitional
`cc-engine`) may declare JWT. `cc-chain` is HTTP-grandfathered, **never**
JWT. Fixture `scripts/fixtures/check-crate-dag/expect-fail/chain-jwt/` is
red; `expect-pass/engine-api-reqwest/` is green.

```text
bash scripts/check-crate-dag.sh --self-test
# 2026-08-16T07:00:30Z this worktree: ok: check-crate-dag JWT/HTTP and chain↔p2p fixtures
```

Record: `docs/adr/ADR-R-03.md` (supersedes ADR-P3-16). Commit that named
the crate: `d6730fb8a1b44f8e0144214e1ffa30293f2077d0`.

---

## E1.5 — 3 containers run; engine-fastpath DA e2e on self-devnet (`S1-A-19`, `S1-B-01`)

**Conclusion: compose config exists. 3 containers were not run on this
commit. Fastpath e2e scrape was not taken. E1.5 is not discharged.**

3-container self-devnet is `devnet/compose.yml` (CC-2Jd):

```text
publisher   cc-p2p --publish-fixture    :19102
node-a      peer under test             :19112
node-b      mesh peer                   :19122
anchor      nginx genesis + CC-19       :18080
```

`docker compose -f devnet/compose.yml config --services` this worktree:
`anchor` `publisher` `node-a` `node-b`. Bring-up is `./devnet/up.sh` (not
raw compose). S0 already ran this mesh for ≥1 h on `e2ad297`; that is not
an S1 run.

Production `docker-compose.yml` is still the **six-service** topology
(chain, p2p, attestation, engine, beacon-api, storage + el).
`services/engine` remains a member so that topology still binds
(`[ARCH]` §9.1). S1 did not land a collapsed 3-process production compose.

Engine-fastpath DA **precondition** landed at `S1-B-01`
(`92e71bb9eadafc75063f11cf132d6c5cf5462da2`): production
`FastpathLane` takes `Some(production_cell_kzg()?)`
(`crates/engine-api/src/api.rs`); tests
`services/engine/tests/production_kzg.rs`. That issue's self-devnet scrape
box is **unchecked**: `cc_engine_getblobs_total{result="complete"}` (and
`cc_engine_cells_computed`) was not collected. W4 / `S3b-W-04` still owns
"non-zero complete **and** zero engine-sourced columns".

---

## E1.6 — S2 entry gate (`S1-B-05` … `S1-B-20`)

**Conclusion: landed.** M11 resolvable; M12 = 0. An S1 exit item, not an
S2 start item.

| Piece | Evidence |
|---|---|
| M11 mechanism | `docs/adr/` MADR + `docs/adr/reconciliation.md` (58 ids; 43 a / 12 b / 3 c). Baseline 748 occurrences. `S1-B-05` (`66fe5a809e333eb8080a9c6041615baa525369d6`) |
| M11 CI | `scripts/check-adr-resolver.sh` + `make check-adr-resolver` + ci.yml lint job. `S1-B-06` (`a56c5e883383321e5bb90a0d29181901fb299dd2`) |
| Resolver this worktree | `ok: ADR resolver — 801 ADR citations (58 ids) ok via table id or ADR-*.md (63 resolvable); 531 Architecture § extracted (census only)` |
| Gate (a) | `S1-B-07`…`S1-B-10` bodies under `docs/adr/` |
| Gate (b) | `S1-B-11`…`S1-B-16` (incl. ADR-R-03, ADR-R-04, ADR-R-01, ADR-P3-15 → ADR-R-07) |
| Gate (c) | `S1-B-17` stale citations |
| M12 | `[PRD]` §5.3.2: 35 judged (`S0-B-17` × 5 + `S1-B-19` × 17 + `S1-B-20` × 13, 2026-08-16). **M12 = 0** |
| M3 ratchet (not E1.6, rides the stage) | `S1` is in `CLAIM_STAGES`; `check-m3-discharged-by.sh` this worktree: `59 claimed rows filled` |

---

## Other S1 exit criteria

| # | Status |
|---|---|
| **E1.1** | **open — no loaded A/B; blocker rule recorded; do not match S0 idle zeros** |
| **E1.2** | **recorded — `S1-A-17` production-budget test; no live compose scrape** |
| **E1.3** | **recorded — `S1-A-12`; `cargo test -p cc-seam` in CI / `make test-seam`** |
| **E1.4** | **recorded — `S1-A-01`; `--self-test` green this worktree** |
| **E1.5** | **open — 3-container compose exists; containers not run; no getBlobs scrape** |
| **E1.6** | **recorded — `S1-B-05`…`S1-B-20`; resolver green; M12 = 0** |
