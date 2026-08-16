# ADR-R-04 — Liveness is proved by a deadline-bounded no-op through the consensus core

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 1 (probe at `S1-A-15`; health wiring at `S1-A-16`)
- **Issues:** S1-A-16, S1-A-15
- **Citations:** `plan/architecture.md` §7.1 / §7.2 / §10.5; `plan/prd.md` R-4 / M8; `plan/issues/s1-fold-el-bridge.md` S1-A-15 / S1-A-16 / S1-A-17; `services/chain/src/liveness.rs`; `crates/bootstrap/src/prober.rs`; `docker-compose.yml` chain healthcheck
- **Provenance:** new — records the S1-A-16 health wiring; the per-sample probe landed at `S1-A-15`

This is **ADR-R-04**, not the slashing-protection record (`ADR-R-05`). `[PLAN]` X-3 is
already closed on that split. This file does **not** supersede `ADR-P3-02` (engine is
deliberately not a health peer). That half is `S1-B-12` and waits on this probe.
It does **not** add `cc-chain` to the JWT grandfather list (`ADR-R-03`).

## Context

Every compose healthcheck is `grpc-health-probe -addr=:900X` against the tonic port.
The health service answers from a tokio task. The consensus core is a separate OS
thread (`services/chain/src/core.rs`). Parking that thread — the failure the topology
cannot heal — had **no effect** on what the health service reported ([ARCH] §7.1,
[PRD] R-4 / M8).

`GetHead` cannot detect it: it is a snapshot load that never touches the core
thread (ADR-P1-09). A process-up probe is the same shape.

`S1-A-15` landed `probe_core_liveness`: `TickWork::Ping` on the never-shed tick
lane, deadline `ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000` (ADR-P3-13;
Hoodi = 3999.6 ms). That function did not flip tonic aggregate health. This
record is the wiring.

The restore handshake (CC-45b) marks `local_ready` at `AwaitingRestore` entry so
storage can start and push `RestoreFromStore`. That handshake is **not** a parked
core. The sampler starts only after a core is installed.

## Decision

**After a consensus core is installed, aggregate SERVING requires a recent
successful `probe_core_liveness`.** Compose `grpc-health-probe` on `""` therefore
reflects core liveness, not just process up.

**Consecutive-sample policy** (S-A16-1 / S-A16-2; supersedes the [ARCH] §7.2
"2 misses / 1 success" stub):

| Property | Value |
|---|---|
| Sample | `probe_core_liveness` (`TickWork::Ping`, never-shed tick lane) |
| Per-sample deadline | `ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000` (A-15 / ADR-P3-13) |
| Cadence | `slot_duration / 4` (4 samples per slot) |
| Flip to NOT_SERVING | **3 consecutive** misses (deadline or `Unavailable`) |
| Restore SERVING | **3 consecutive** successes (same N as fail) |
| 1 or 2 misses | stay SERVING |
| 1 or 2 successes while parked | stay NOT_SERVING |
| Health bit | existing `local_ready` (`prober.rs`), already ANDed into aggregate `""` |

ADR-P3-13 is the **probe** deadline (~3999.6 ms Hoodi), not the miss budget.
The consensus OS thread legally `block_on`s Engine RPCs for **8 s**
(`DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT` / `DEFAULT_ENGINE_FORKCHOICE_UPDATED_TIMEOUT`).
Ping is a no-op but cannot run until the current `CoreCommand` returns. N=2
at slot/4 let two in-flight 8 s units (redrive, gossip+fcU) accumulate a
flip in ~11 s — a live-but-busy core went red (S-A16-1).

N=3 sequential horizon is 3×~4 s + 2×3 s ≈ **18 s**. One 8 s `newPayload`,
and two back-to-back 8 s units, cannot park the DAG. The `[ARCH]` §7.2 stub's
"deadline one slot" / "2 consecutive" is the rejected fail-side; this file
owns the miss budget.

Recover is **not** cheaper than fail (S-A16-2). One later success restoring
SERVING after two misses inverted the hysteresis: a mostly-stuck thread that
answered once every ~14 s stayed in the healthy set, and S-A16-1's false-red
flapped back on the next fast Ping. Same N both ways.

The sampler starts SERVING (the restore handshake already marked ready). It
cannot *stay* SERVING on a parked core: the third consecutive miss clears
`local_ready`. An absent core (Phase 0 / EMPTY restore, `NOT_BOOTSTRAPPED`)
does not start the sampler and is not a park.

**Not decided here.** Engine's place in the health DAG stays `ADR-P3-02` until
`S1-B-12`. Do not flip EL health. Do not add `cc-chain` to the JWT list. The
injected engine black-hole that **demonstrates** this red is `S1-A-17`.

## Consequences

What this makes easy:

- A parked core makes `beacon-core` report `NOT_SERVING`. Compose restarts it.
  After S2 a restart no longer forces checkpoint re-sync ([PRD] R-10), which is
  what makes the restart an acceptable response.
- `grpc-health-probe -addr=:9001` (no `-service`) is the same instrument
  operators and compose already use.

What this makes hard:

- One extra tick-lane ping 4× per slot, plus three metric series
  (`cc_core_liveness_rtt_seconds`, `cc_core_liveness_parked`,
  `cc_core_liveness_deadline_seconds`).
- A single long epoch transition, and one (or two back-to-back) 8 s Engine
  RPCs, are tolerated. Three consecutive missed soft deadlines are not.
- Recover takes the same three consecutive successes, so compose `retries: 12`
  and peer `FAIL_THRESHOLD=2` see a stable bit, not a 1-success flap.

What this forbids:

- Staying SERVING on a parked core.
- Treating `GetHead` or a tonic-port open as core liveness.
- Folding engine health or the JWT grandfather list into this change.

## Alternatives considered

**Probe `GetHead`.** Rejected. It is a pointer load that never touches the core
thread (ADR-P1-09). It measures the snapshot, not the worker.

**Flip on the first miss.** Rejected. One long epoch transition would restart
the container.

**[ARCH] §7.2's 2 consecutive misses / 1 success.** Rejected. ADR-P3-13 is
the per-sample deadline, not the miss budget. Two 4 s misses at slot/4
(~11 s) is shorter than two legitimate 8 s Engine `block_on`s, and
1-success recover flaps the DAG (S-A16-1 / S-A16-2). N=3 both ways.

**Cadence = one slot, keep N=2.** Considered. Also outlasts one 8 s RPC.
Rejected in favour of keeping 4×/slot sampling (the [ARCH] majority
cadence) and raising N, so a park is still visible inside ~1.5 Hoodi slots.

**A new health service name instead of `local_ready`.** Rejected. Aggregate
`""` is what compose and `grpc-health-probe` already read. `local_ready` is
already ANDed in. A second name would leave compose green.

**Start the sampler before a core exists.** Rejected. The restore handshake
needs SERVING so storage can push. An absent core is `NOT_BOOTSTRAPPED`, not a
park.

**Demonstrate red against an injected engine black-hole in this issue.**
Rejected. That harness is `S1-A-17`. This record is the wiring and the policy.

## Refactor impact

**Created at S1. Probe landed at `S1-A-15`. Wiring landed at `S1-A-16`. This
file is the record.**

| Stage | What happens to this record |
|---|---|
| S1-A-15 | `probe_core_liveness` + `TickWork::Ping`. **Landed.** |
| S1-A-16 | Sampler feeds `local_ready`; this file. **Landed.** |
| S1-A-17 | Injected engine black-hole; M8 demonstrated red. Not this file. |
| S1-B-12 | `ADR-P3-02` / engine-as-health-peer supersession. Not a silent amendment. |
| S2 | Restart must not force checkpoint re-sync ([PRD] R-10), or the remedy is worse than the fault. |
| S3+ | The probe stays the liveness contract for `beacon-core`. |
