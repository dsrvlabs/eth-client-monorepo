# Q1 — Lighthouse `beacon_processor` as a concrete template

**Verdict:** Adopt the *shape* (one manager task → N named priority queues → bounded
blocking worker pool), not the taxonomy. This repo already owns half of it
(`das/verify_pool.rs` is a textbook Lighthouse-style blocking pool, merely unwired);
the missing half is the scheduler in front of it. **But the dominant stall on the
gossip loop is the 12 s cross-process reply wait, not the scheduler** — a priority
queue in front of a gRPC hop reorders the stall, it does not remove it. Sequence
this after Stage 3.

**Question.** Study finding 09 names head-of-line blocking on both critical loops:
single-worker gossip validation with inline crypto, and chain's one `mpsc(64)` FIFO
mixing imports, queries and ticks. Lighthouse's `beacon_processor` is cited as the
production-proven in-process answer. What is its actual queue taxonomy and priority
ordering? How does it separate the async reactor from blocking crypto? How are
backpressure and shedding expressed? What does `WorkEvent`/`Work` look like — and
which parts map onto this repo's two loops?

---

## 1. What Lighthouse actually built

All claims in this section are from the **source tree**, `sigp/lighthouse` branch
`stable`, fetched 2026-08-15. Files:

- `beacon_node/beacon_processor/src/lib.rs`
  (<https://github.com/sigp/lighthouse/blob/stable/beacon_node/beacon_processor/src/lib.rs>)
- `beacon_node/beacon_processor/src/scheduler/work_queue.rs`
  (<https://github.com/sigp/lighthouse/blob/stable/beacon_node/beacon_processor/src/scheduler/work_queue.rs>)
- `beacon_node/beacon_processor/src/scheduler/work_reprocessing_queue.rs` (~85 KB)
  (<https://github.com/sigp/lighthouse/blob/stable/beacon_node/beacon_processor/src/scheduler/work_reprocessing_queue.rs>)
- `beacon_node/beacon_processor/src/metrics.rs`
  (<https://github.com/sigp/lighthouse/blob/stable/beacon_node/beacon_processor/src/metrics.rs>)

Note the directory itself: the processor is its **own crate**, extracted out of the
`network` crate. That extraction is the first thing to copy — it makes the scheduler
testable without a libp2p swarm.

### 1.1 The core loop

One **manager** task owns all queues. It never does work. It listens on three
channels, merged into a single stream with a **strict poll priority**:

```rust
enum InboundEvent<E: EthSpec> {
    WorkerIdle,
    WorkEvent((WorkEvent<E>, Instant)),
    ReprocessingWork((WorkEvent<E>, Instant)),
}
```

Poll order is **`WorkerIdle` → `ReprocessingWork` → `WorkEvent`**. The reason is
explicit in the source: a high inbound event rate must never starve either the
worker-freed signal or already-queued deferred work. That inversion — *drain the
completion channel before the arrival channel* — is the single most important
structural detail, and it is exactly the thing an obvious implementation gets wrong.

Workers are counted, not pooled by a library: `self.current_workers` is incremented on
spawn and decremented on the `WorkerIdle` notification, and the manager spawns only
while `self.current_workers < self.config.max_workers`. `max_workers` defaults to
`cmp::max(1, num_cpus::get())` (`lib.rs`, `impl Default for BeaconProcessorConfig`).

The decrement is driven by a `SendOnDrop` guard, so a panicking worker still returns
its slot and still decrements
`BEACON_PROCESSOR_WORKERS_ACTIVE_GAUGE_BY_TYPE`.

### 1.2 `WorkEvent` / `Work`

```rust
pub struct WorkEvent<E: EthSpec> {
    pub drop_during_sync: bool,
    pub work: Work<E>,
}
```

Two fields. That is the whole envelope. `drop_during_sync` is the shedding policy
expressed as data, not as a branch at the call site: when the node is syncing, the
manager skips any event carrying it and bumps
`BEACON_PROCESSOR_WORK_EVENTS_IGNORED_COUNT`.

`Work<E>` is a **47-variant enum** — `GossipAttestation`, `GossipAttestationBatch`,
`GossipAggregate`, `GossipAggregateBatch`, `GossipBlock`, `GossipDataColumnSidecar`,
`GossipPartialDataColumnSidecar`, `UnknownBlockAttestation`, `UnknownBlockAggregate`,
`UnknownBlockDataColumn`, `DelayedImportBlock`, `RpcBlock`, `RpcBlobs`,
`RpcCustodyColumn`, `ColumnReconstruction`, `ChainSegment`, `ChainSegmentBackfill`,
`Status`, `BlocksByRangeRequest`, `DataColumnsByRangeRequest`, …, `ApiRequestP0`,
`ApiRequestP1`, `Reprocess`. A parallel `WorkType` enum with the same variants and
`#[derive(IntoStaticStr)]` supplies the metric label — so every queue-depth, queue-time
and worker-time histogram is automatically per-work-type.

Each variant carries the work as a **closure or boxed future**, and the dispatch site
picks one of three spawners:

| Spawner | Used for |
|---|---|
| `spawn_blocking()` | CPU-bound sync work — attestations, aggregates, exits, slashings |
| `spawn_async()` | futures — block import, RPC request serving, chain segments |
| `spawn_blocking_with_rayon(RayonPoolType::LowPriority, …)` | `ChainSegmentBackfill` when `enable_backfill_rate_limiting` |

This is the async-reactor / blocking-crypto separation: signature and KZG-shaped work
never runs on a reactor thread; it is handed to a blocking thread whose completion
sends `WorkerIdle` back to the manager.

### 1.3 Queue taxonomy — 47 queues, one per work type

Every `Work` variant has its own queue. There is no shared "gossip queue". Selection
is a flat, hand-ordered `if let Some(item) = …` chain in the manager, and **the order
of that chain *is* the priority policy** — first match wins, so a queue late in the
chain is only served when everything above it is empty.

The full order, top to bottom:

1. `chain_segment_queue`
2. `rpc_block_queue`, `rpc_blob_queue`, `rpc_custody_column_queue`, `rpc_envelope_queue`
3. `delayed_block_queue`, `delayed_envelope_queue`
4. `gossip_block_queue`, `gossip_execution_payload_queue`, `gossip_data_column_queue`,
   `unknown_block_data_column_queue`, `gossip_partial_data_column_queue`,
   `column_reconstruction_queue`
5. `api_request_p0_queue`
6. `aggregate_queue` (batched), `attestation_queue` (batched),
   `attestation_to_convert_queue`, `gossip_payload_attestation_queue`
7. `sync_contribution_queue`, `sync_message_queue`
8. `unknown_block_aggregate_queue`, `unknown_block_attestation_queue`
9. `gossip_execution_payload_bid_queue`, `gossip_proposer_preferences_queue`
10. `status_queue`, then all req/resp serving queues
    (`block_brange_queue`, `block_broots_queue`, `block_bhead_queue`,
    `blob_brange_queue`, `blob_broots_queue`, `dcbroots_queue`, `dcbrange_queue`,
    `payload_envelopes_*`)
11. `gossip_attester_slashing_queue`, `gossip_proposer_slashing_queue`,
    `gossip_voluntary_exit_queue`, `gossip_bls_to_execution_change_queue`
12. `api_request_p1_queue`
13. `backfill_chain_segment`
14. all light-client queues

The shape of that list is the lesson, and it is short:

- **Sync and import outrank everything.** Chain segments and RPC blocks are ahead of
  gossip; a node behind the chain prioritises catching up.
- **Blocks outrank attestations.** Attestations about a block you have not imported
  are worthless.
- **Serving peers outranks slashings outranks backfill.** Everything that is not
  needed to follow the head is at the bottom, and backfill is last-but-one.
- **The two API lanes are split by priority, not by transport** — `ApiRequestP0` sits
  above the whole attestation block, `ApiRequestP1` below it. This is directly
  relevant to this repo: `CoreCommand::Query` is currently one undifferentiated lane.

### 1.4 Backpressure and shedding — three distinct mechanisms

`work_queue.rs` defines two queue types whose overflow behaviour differs, and the
choice per queue is a deliberate policy statement:

```rust
// FifoQueue::push — full: drop the NEW item, log
pub fn push(&mut self, item: T, item_desc: &str) {
    if self.queue.len() == self.max_length {
        error!(...)
    } else {
        self.queue.push_back(item);
    }
}

// LifoQueue::push — full: evict the OLDEST, keep the new
pub fn push(&mut self, item: T) {
    if self.queue.len() == self.max_length {
        self.queue.pop_back();
    }
    self.queue.push_front(item);
}
```

LIFO queues: `aggregate_queue`, `attestation_queue`, `attestation_to_convert_queue`,
`unknown_block_aggregate_queue`, `unknown_block_attestation_queue`,
`sync_message_queue`, `sync_contribution_queue`, `column_reconstruction_queue`.
Everything else is FIFO. The in-source rationale:

- LIFO for attestations/aggregates — *"validator profits rely upon getting fresh
  attestations into blocks. Additionally, later attestations contain more information
  than earlier ones."* Under overload you want the newest attestation, not the oldest.
- FIFO for voluntary exits — prevents **exit censoring**.
- FIFO for slashings — an attacker must not be able to *"flush their slashings from the
  queues with lots of junk messages."*
- FIFO for blocks — *"blocks need to be imported sequentially."*

So overflow policy is chosen per-message-type on an **adversarial** argument, not a
throughput one. That is the part most implementations skip.

The third mechanism is `drop_during_sync` (above), and the fourth is the bounded
inbound channel itself: `DEFAULT_MAX_WORK_EVENT_QUEUE_LEN = 16_384`, with `try_send`
failures counted by `BEACON_PROCESSOR_SEND_ERROR_PER_WORK_TYPE`. Note the layering —
the inbound channel is huge (16k) precisely so that shedding decisions are made by the
*typed* queue with the right policy, not by an untyped channel bound.

### 1.5 Queue lengths are derived from validator count

`BeaconProcessorQueueLengths::from_state()` sizes the attestation queues from the
active validator count of the current state, over-provisioned by
`ACTIVE_VALIDATOR_COUNT_OVERPROVISION_PERCENT` (110 %):

```
attestation_queue = max(active_validators * 110 / 100 / slots_per_epoch, MIN_QUEUE_LEN)
```

with `MIN_QUEUE_LEN = 128` (*"Due to integer division we don't want 0 length queues as
the processor won't process that message type."*). Fixed lengths include
`aggregate_queue: 4096`, `gossip_block_queue: 1024`, `rpc_block_queue: 1024`,
`gossip_data_column_queue: 1024`, `api_request_p0_queue: 1024`,
`api_request_p1_queue: 1024`, `chain_segment_queue: 64`,
`unknown_block_data_column_queue: 256`, `rpc_custody_column_queue: 64`.

This directly contradicts this repo's `docs/dev-conventions.md`-style "bounded queues
with named constants" as currently applied: a compile-time constant cannot be right
for both a 30-validator devnet and a 1 M-validator mainnet. The named constant should
name the *formula*, not the number.

### 1.6 The reprocessing queue is a separate 85 KB subsystem

`work_reprocessing_queue.rs` handles "work that arrived too early" — attestations for
an unknown block, blocks for an unknown parent, sidecars awaiting a block. It is a
delay-line with its own capacity and its own channel back into the manager
(`InboundEvent::ReprocessingWork`), polled *above* new inbound work. This is the
production-grade form of what this repo hand-rolls as `redrive_for_parent` /
`redrive_unknown_proposer` inline in the validation worker
(`services/p2p/src/gossip/validate/pipeline.rs:668`, `:772`) — where a redrive walk
runs *on the same single thread that is supposed to be validating*, at the top of
every loop iteration (`pipeline.rs:397`).

---

## 2. Mapping onto this repo

### 2.1 What this repo already has (do not rebuild)

`services/p2p/src/das/verify_pool.rs:1-60` is, structurally, a Lighthouse blocking
worker pool and a good one:

- dedicated OS threads, explicitly *"not the shared blocking pool"* (module docs)
- `pool_worker_count() = max(2, available_parallelism / 2)` (`verify_pool.rs:55-62`)
- bounded queue `VERIFY_QUEUE_BOUND = 256` with **oldest-dropped IGNORE semantics and
  a drop counter** (`verify_pool.rs:38`) — i.e. it independently reinvented
  `LifoQueue`'s eviction policy for column sidecars
- opportunistic cross-sidecar batching, `CROSS_SIDECAR_BATCH_MIN = 4`, with
  per-sidecar re-verification before any peer is penalised — this is *ahead* of
  Lighthouse's per-item path
- a hard structural cap `HARD_MAX_BLOB_COMMITMENTS = 4096` independent of the
  caller-supplied bound

It is disconnected by exactly one character: `kzg_tx: _` at
`services/p2p/src/service.rs:714` destructures the sender into a discard binding, so
`run_verify_pool_bridge(kzg_rx, …)` (`service.rs:776`) has no producer, and
`column.rs:534` calls `kzg.verify_column_kzg(...)` **inline on the validation task**
instead. Reconnecting that sender is a one-line change and is already Stage 3 scope.

**Conclusion: the blocking-pool half of `beacon_processor` exists here. Only the
scheduler half is missing.**

### 2.2 Loop A — gossip validation (`services/p2p/src/gossip/validate/pipeline.rs`)

Current shape:

```rust
// pipeline.rs:386-402
pub async fn run_validation_pool(pool: ValidationPool, mut gossip_rx: mpsc::Receiver<GossipWork>) {
    while let Some(work) = gossip_rx.recv().await {
        ...
        redrive_unknown_proposer(&pool).await;
        let verdict = validate_one(&pool, &work).await;
        report(&pool, &work, verdict).await;
    }
}
```

One `GossipWork` channel, one sequential consumer, no concurrency, no per-topic
differentiation, and `IN_FLIGHT_VALIDATION_CAP = GOSSIP_BOUND` (`pipeline.rs:904`) as
the only bound. `ValidatorKind` (`pipeline.rs:66-99`) already classifies topics — that
enum is the seed of a work taxonomy and should become the queue key.

Proposed mapping, sized for this repo (not 47 queues — 8):

| Queue | Fed by | Type | Rationale |
|---|---|---|---|
| `gossip_block` | `beacon_block` topic | FIFO | must import sequentially |
| `gossip_data_column` | `data_column_sidecar_*` | FIFO | ordering matters for DA |
| `unknown_parent` | current `redrive_for_parent` park set | FIFO | Lighthouse's reprocessing queue |
| `unknown_proposer` | current `redrive_unknown_proposer` park set | FIFO | ditto |
| `aggregate` | `beacon_aggregate_and_proof` | **LIFO** | fresher is strictly better |
| `attestation` | `beacon_attestation_*` | **LIFO** | fresher is strictly better |
| `sync_message` / `sync_contribution` | sync topics | **LIFO** | same argument |
| `slashing_exit_blstoexec` | the four operation topics | FIFO | censorship resistance |

Sizing: `attestation` queue from active-validator count as Lighthouse does, everything
else fixed. Selection order: block → column → unknown-parent/proposer → aggregate →
attestation → sync → operations.

Concurrency: replace the single `while let` with a manager owning those queues plus a
worker count. Crypto-shaped work (`verify_column_kzg`) goes to the *existing*
`verify_pool` via the reconnected `kzg_tx`; everything else stays on the reactor.

**What does not transfer, and it is the important part.** Lighthouse's scheduler works
because the terminal call is `chain.process_gossip_block()` — an in-process function.
In this repo the block path is:

```rust
// pipeline.rs:617-631
pool.chain_out_tx.send(outbound).await ...
tokio::time::timeout(Duration::from_secs(12), reply_rx).await
```

a gRPC hop to `chain`, awaited for up to **12 seconds**, on the one and only worker.
The chain-side `verdict_timeout` default is 2 s
(`services/p2p/src/chain_stream/client.rs:143-159`), but this local wait is 12 s, so a
chain that is merely slow rather than dead holds the *entire* gossip loop.

Consequently:

- A priority scheduler in front of that hop **cannot** fix the stall. It changes which
  message is stuck at the front; it does not make the front move. Adding workers helps
  (N concurrent 12 s waits instead of one), but concurrency across a gRPC stream with a
  correlation map is a different and larger change than a queue taxonomy.
- Lighthouse's `spawn_blocking` / `spawn_async` split is *meaningless* for a work item
  whose cost is a network wait, not CPU.
- **Therefore: sequence the scheduler after Stage 3.** Before the p2p↔chain edge
  collapses, the highest-value changes on this loop are (a) reconnect `kzg_tx`, (b)
  reduce the 12 s wait to something within a slot, (c) move the two `redrive_*` walks
  off the hot path. After Stage 3, the scheduler is the right and now-effective move.

### 2.3 Loop B — chain core (`services/chain/src/core.rs`)

Current shape: one `mpsc::channel(COMMAND_CHANNEL_CAPACITY)` with
`COMMAND_CHANNEL_CAPACITY = 64` (`core.rs:54`, `:503`), consumed by
`while let Some(cmd) = cmd_rx.blocking_recv()` on one OS thread (`core.rs:921`),
carrying seven mixed variants (`core.rs:71-110`): `ImportBlock`, `ImportBlockGossip`,
`ApplyAttestations`, `Query`, `BlockFor`, `DataAvailable`, `SlotTick`, `Shutdown`.

This is a *closer* match to `beacon_processor` than Loop A, because the terminal work
is genuinely in-process CPU (state transition, fork choice) and there is genuinely one
non-negotiable serialisation point (the `Store`). Two concrete pathologies the
Lighthouse shape fixes directly:

1. **`SlotTick` is dropped under load.** `core.rs:527` uses `try_send` and swallows
   the failure — "drops under load are fine (next slot retries)" — but the review's
   `core.rs:527` finding shows `store.time` advances *only* via this tick, so a
   dropped tick means `get_current_slot()` lags and valid blocks are IGNOREd as
   `future_slot`. In Lighthouse terms, the clock tick is being shed by an untyped
   channel bound with no policy. It belongs in its own always-served lane, or better,
   `on_tick(store, wall_clock_now)` at the top of each import.
2. **`Query` shares a FIFO with `ImportBlock`.** `core.rs:92` even says so: *"single
   FIFO queue in Phase 1; priority lane is Phase 6"*. A `GetHead` behind three block
   imports waits for three state transitions. Lighthouse's `ApiRequestP0` /
   `ApiRequestP1` split is exactly this, and the annotation shows the repo already
   knows.

Proposed mapping — 5 lanes, all in-process, no new crate:

| Lane | Variants | Type | Order |
|---|---|---|---|
| `tick` | `SlotTick`, `Shutdown` | FIFO(4) | 1 — never shed |
| `import` | `ImportBlock`, `ImportBlockGossip`, `DataAvailable` | FIFO | 2 |
| `query_p0` | `Query{Head}` and head-probe reads | FIFO | 3 |
| `attestation` | `ApplyAttestations` | **FIFO** — see below | 4 |
| `query_p1` | `Query{CommitteeShuffling, ValidatorPubkeys, …}` | FIFO | 5 |

`DataAvailable` must stay in the import lane, not above it: it re-drives a parked
block, so re-ordering it ahead of imports gains nothing and risks starving them.

**Why `attestation` is FIFO here and LIFO in Lighthouse.** Lighthouse's argument
("later attestations contain more information") applies to *gossip validation*, where
the terminal work is a signature check and the output is a verdict. Here the terminal
work mutates the fork-choice `Store` and **publishes events**: `apply_attestations`
calls `recompute_and_publish_head` (`services/chain/src/apply_attestations.rs:101`),
which `blocking_send`s `EventInput::head(...)` (`:169`) and
`EventInput::chain_reorg(...)` (`:182`). Storage's write-behind consumes that stream and
uses `HEAD` for slot `S+1` as its primary flush trigger
(`services/storage/src/write_behind.rs:5`, `should_flush_for_head`), with a `WriteCursor`
seq that bounds its loss window. LIFO would let a batch for an older view be processed
after a newer one, emitting a `HEAD` that regresses `acc.open_slot`.

**Invariant to state and enforce in the Loop B design: every event-emitting command
shares a FIFO lane, and lanes never reorder relative to each other for event-emitting
work.** With `ApplyAttestations` on FIFO, the split above satisfies it — imports are
already all in one FIFO lane, `SlotTick`/`Shutdown` emit nothing, and both `Query` lanes
are read-only (`handle_query(&store, …)`, `core.rs:1040-1042`). This closes the
open question flagged in §5.

If a LIFO attestation lane is later wanted for throughput, the prerequisite is that
event emission be lifted out of the command handler and driven by a single ordered
head-publication step — not that the ordering risk be accepted.

Because the `Store` is single-owner, this is a **scheduler-only** change: one thread
still executes, the manager just picks the next command from five queues instead of
one channel. `max_workers` stays 1. That makes it strictly cheaper than the p2p
change and it does not wait on Stage 3.

Two things that do **not** transfer:

- Lighthouse's worker pool. There is one `Store` on one thread by design
  (Architecture §3.1 / `ArcSwap` snapshot reads). Do not introduce workers here.
- `drop_during_sync`. This node has no equivalent sync-state predicate wired yet; the
  nearest analogue is the restore gate. Skip the field until there is a real syncing
  state to test.

### 2.4 What to copy verbatim

1. The **poll-priority inversion** (`WorkerIdle` → deferred → new). Cheap, and the
   thing hand-rolled loops get wrong.
2. `FifoQueue` / `LifoQueue` as two ~30-line types with the overflow policy in the
   type, plus a one-line comment per queue saying **why** that policy. The adversarial
   rationale (exit censoring, slashing flush) is the reusable content.
3. Per-work-type metrics derived from one enum via `IntoStaticStr` — replaces the
   review's "1,600-line god-metric facades" and "racy hand-rolled gauges" (finding 20)
   with a mechanical mapping.
4. Queue lengths as a **function of active validator count** with a floor, not a
   compile-time constant.
5. Extracting the scheduler into its own crate (`cc-work` / `cc-scheduler`) so it is
   testable without a swarm or a gRPC server.

---

## 3. Recommendation for this codebase

**Do Loop B first, and do it now.** A 5-lane scheduler inside `services/chain/src/core.rs`
is self-contained, needs no new dependency, keeps the single-owner `Store` discipline
intact, and directly fixes a *consensus-correctness* bug (dropped `SlotTick` →
`future_slot` IGNOREs) rather than a throughput number. It is also topology-neutral —
it survives Stage 1/2/3 unchanged.

**Do Loop A after Stage 3.** Before the transport collapses, reconnect `kzg_tx`
(one line), shorten the 12 s wait, and hoist the two `redrive_*` walks out of the loop
body. The full queue taxonomy earns its keep only once `validate_one` calls into the
chain in-process.

**Extract the scheduler as a crate either way**, so both loops share `FifoQueue`,
`LifoQueue`, the manager skeleton and the metric derivation.

## 4. Effort estimate

| Piece | Size | Rationale |
|---|---|---|
| `cc-scheduler` crate: `FifoQueue`/`LifoQueue`/manager skeleton + metrics | **S** | ~400 lines, no external deps, fully unit-testable; `work_queue.rs` is 19 KB total including 47 queue declarations |
| Loop B: 5 lanes in `core.rs` | **M** | ~7 call sites in `CoreHandle` (`core.rs:240,273,306,331,351,364,382`) plus the dispatch match at `core.rs:921-1140`; the risk is that `cmd_tx.capacity()`-derived depth metrics (`core.rs:255,291,319`) and `import_path.rs:323,371` backpressure tests both hard-code the single-channel model and must be rewritten |
| Loop A: taxonomy + concurrency | **L** | needs the Stage-3 in-process call *and* concurrent correlation handling; `ValidationPoolState` is behind one `Mutex` (`pipeline.rs:268`) touched by every validator, so N workers means contending on it — expect to shard it |
| Reprocessing queue as a real subsystem | **M** | replaces `redrive_for_parent` / `redrive_unknown_proposer`; Lighthouse spends 85 KB here, but most of that is delay-line bookkeeping this repo already has in `ValidationPoolState.pending` |

Loop B alone: **M**, ~1–2 weeks including test rewrites. Full programme: **L**.

## 5. What I could not determine

- **Whether the 12 s wait is reachable in practice**, because gossip is never
  subscribed in production (study finding 01). No measurement exists; the whole
  head-of-line argument for Loop A is analytic until Stage 3 wires the topics.
- **Lighthouse's actual measured queue-time distributions.** `BEACON_PROCESSOR_QUEUE_TIME`
  exists as a histogram but I found no published mainnet percentiles from the source
  tree; any number I gave would be from a blog post or a Grafana screenshot, not the
  repo.
- **Whether `ACTIVE_VALIDATOR_COUNT_OVERPROVISION_PERCENT` is exactly 110** — the
  fetched summary states 110 % and the formula, but I did not see the literal
  constant line. Treat the formula as verified and the exact percentage as
  approximately-verified.
- **The precise `spawn_async` vs `spawn_blocking` assignment per `Work` variant.** I
  verified the three spawner kinds and several examples, not the full 47-row mapping.
  If the taxonomy is implemented here, re-read `lib.rs`'s dispatch match directly
  rather than trusting this summary.
- **Whether Lighthouse's `LifoQueue` eviction has ever been shown to lose a
  consensus-relevant message.** The design argument is in the source comments; I found
  no incident write-up either way.
- **Whether the FIFO-lane invariant in §2.3 is sufficient, or merely necessary.** I
  verified which commands emit events (`ApplyAttestations` via
  `apply_attestations.rs:101,169,182`; imports via the import path; ticks and queries
  not at all) and that keeping every emitter in a FIFO lane preserves emission order.
  I did **not** trace the `WriteCursor` seq assignment in
  `services/storage/src/write_behind.rs` end to end to confirm it depends only on
  emission order and not on, say, wall-clock arrival or `open_slot` monotonicity across
  lanes. Do that trace before Loop B lands — it is the one thing that could turn an
  **M** into a redesign.
