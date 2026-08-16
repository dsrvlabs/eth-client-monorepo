# ADR-P2-13 — Per-task panic policy: catch and restart rather than abort

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-15, S3a-B-19, S3a-B-20
- **Citations:** 7 sites — `services/p2p/src/supervisor.rs:1,27`; `services/p2p/src/service.rs:230,852-856,914`; `services/p2p/src/main.rs:525`; `docs/phase-2-soak.md:433`
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing for [PRD] §9 X1** (⟡ D-11). The policy
catches panics, so a libp2p panic may never become a process abort.
**The catch path gains a task-labelled counter before S3**, or X1
returns 0 for the wrong reason. `S3a-B-19` hangs that instrument on
this path; `S3a-B-20` validates it. This file does not close X1.

## Context

A p2p process runs several tokio tasks beside the libp2p swarm.
`JoinHandle` panic is the default Rust behaviour: the task dies and,
without a supervisor, the rest of the process keeps serving while that
work is gone — or a `panic = abort` profile takes the whole process
down, including (under Single Hull) the fork-choice store. Phase 2
chose neither default. `run_supervisor` owns the join handles
(`supervisor.rs:1-7,84-200`).

Every supervised panic enters the same `JoinError` arm
(`supervisor.rs:155-196`). The arm logs once at error with the task
name, then applies a **per-task** policy:

| Task | Policy | After the catch |
|---|---|---|
| `swarm` | `TaskPolicy::ProcessFatal` | `SupervisorOutcome::Fatal` → `RuntimeError::SwarmPanic` → aggregate health NOT_SERVING → `process::exit(1)` (`service.rs:230,602-606,852-856,914`; `main.rs:525`) |
| `peer_manager`, `idle_worker` | `TaskPolicy::Respawn` | respawn immediately (`service.rs:608-621`) |
| `discovery` | `TaskPolicy::discovery()` — 5 restarts in 5 minutes | respawn until the budget; then fatal (`supervisor.rs:19-22,32-47,173-195`; soak void at `phase-2-soak.md:433`) |

Workers that panic are therefore **caught and restarted** rather than
aborting the process. Swarm (and discovery over budget) still aborts,
but only **after** the same catch arm. A soak that counts process
exits therefore cannot see worker panics, and cannot tell a libp2p
panic from silence.

X1 counts *libp2p-attributable* panics/aborts in the `p2p` process
(`[PRD]` §9). Non-zero carries Gatehouse & Keep: each such panic would
have taken down fork-choice under Single Hull. Absent evidence is
**not** a vote for the default. A counter wired to `exit(1)` — or
missing entirely — returns 0 while the catch path is restarting
libp2p work, and D-2 defaults by accident (⟡ D-11).

The increment already lives on the catch path:
`metrics.inc_worker_panics(name)` →
`cc_p2p_worker_panics_total{worker}`, cumulative across respawns
(`supervisor.rs:4,160-161`; `metrics.rs:121-125,634-637,812-818`).
`S3a-B-19` must keep X1 on **that** increment, labelled by task, and
export it to the soak scraper. It must not add a second series on the
abort path.

## Decision

**A supervised task that panics is caught. Default workers restart.
The process does not abort unless the task's policy is fatal.**

- Spawn through `cc_bootstrap::spawn` so the root tracing span
  survives (`supervisor.rs:7`).
- On `JoinError`: log once with `task` + payload; increment the
  **task-labelled** panic counter; then apply `TaskPolicy`.
- `Respawn` respawns immediately. The factory must not reset counters.
- `RespawnBudget` (discovery: 5 / 5 min) respawns until the window
  overflows, then `Fatal`.
- `ProcessFatal` (swarm) returns `Fatal`. `run_process` drains gRPC
  so aggregate health is NOT_SERVING, then `main` exits 1. Do not
  wait for SIGTERM (`service.rs:852-856,913-914`; `main.rs:525`).
- A `ProcessFatal` task that exits **cleanly** is also `Fatal` — the
  process must not run deaf (`supervisor.rs:139-152`).

**The catch path gains a counter before S3 (X1 measurable).** The
counter is incremented in `run_supervisor`'s `JoinError` arm, labelled
by task, cumulative across respawns. X1 reads that series. A counter
on the process-abort / `exit(1)` path is the wrong instrument: workers
never reach it. `S3a-B-19` is the issue that treats this increment as
the X1 instrument; `S3a-B-20` demonstrates it against an injected
panic. Do not ship S3 soak without it.

## Consequences

What this makes easy:

- A peer-manager / idle-worker panic does not take the process
  (or, under Single Hull, the fork-choice store) with it.
- Swarm death is a compose restart, not a hung SERVING process.
- X1 has a single, named hook: the catch-path increment labelled by
  task. `S3a-B-19` does not invent a second supervisor.

What this makes hard:

- A flapping worker is silent at the process edge. Operators and the
  soak see it only if they scrape the labelled counter. That is why
  the counter is not optional instrumentation.
- X1 cannot be read off `restart=always` counts or non-zero exits.
  Swarm fatal voids a Phase-2 soak (`phase-2-soak.md:432-436`);
  worker panics must not.

What this forbids:

- `panic = abort` for supervised p2p tasks, or letting a worker
  `JoinHandle` complete unobserved.
- Putting the X1 series on `RuntimeError::SwarmPanic` / `exit(1)`
  instead of the catch arm.
- An unlabelled total as the X1 reading — it cannot separate a
  libp2p panic from an unrelated task.
- Resetting `cc_p2p_worker_panics_total` on respawn.
- Treating a clean swarm exit as success.

## Alternatives considered

**Abort the process on any supervised panic.** Rejected. It makes
every worker panic a compose restart and, under Single Hull, a
fork-choice restart. Blast radius is then the default rather than
the X1 finding.

**Catch and restart with no counter.** Rejected (⟡ D-11). It is the
live failure mode X1 would have: the soak reports 0 because panics
never become aborts. `[PRD]` §9 forbids reading that 0 as a vote for
Single Hull.

**Count only `process::exit(1)` / swarm fatal.** Rejected as the X1
instrument. Swarm is already process-fatal; the interesting series is
the workers that *do not* abort. The catch arm is the only place that
sees both.

**Supervise by letting tokio's runtime catch panics and continue.**
Rejected. There is no task name, no policy split, no budget, and no
place to hang a labelled counter.

## Refactor impact

**Survives.** Load-bearing for X1 (`[ARCH]` §6.4 ⟡ D-11, §10.4;
`S3a-B-19`). Catch-and-restart stays the worker policy after S2/S3
fold. The increment stays on this catch path.

| Stage | What happens to this record |
|---|---|
| S1 (`S1-B-15`) | This file. No production code change. |
| S3a (`S3a-B-19`) | Treat `inc_worker_panics` as the X1 instrument: labelled by task, exported, scraped. Do not move it onto the abort path. |
| S3a (`S3a-B-20`) | Demonstrate increment against an injected p2p-task panic on the self-devnet. That closes X1, not this file. |
| S3b soak | Read X1 off the catch-path series. A reading of 0 is valid only after `S3a-B-20`. |
| S3+ / Single Hull | The same supervisor (or its in-process successor) still catches workers. Do not drop the policy because the process boundary went away — that is the blast-radius control §6.4 names. |
