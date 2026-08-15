# Refactor research — open technical questions

Companion to `architecture-study-2026-08-12.md` (the topology decision) and
`review-develop-2026-08-09.md` (the branch review). Those two settle *what* to build and
*what is broken*. These five answer the questions they left open.

**Out of scope by construction:** consensus-client topology. The study's §5 is the
settled answer — no production client runs the beacon node as internal microservices —
and nothing here revisits it.

Date: 2026-08-15. Every external claim carries a URL and is labelled source-tree vs
documentation vs blog post. Every repo claim carries a `file:line`.

---

| # | Question | One-line verdict |
|---|---|---|
| [Q1](q1-beacon-processor.md) | Lighthouse `beacon_processor` as a template | Adopt the shape (manager → named priority queues → bounded blocking pool); the repo already owns the blocking-pool half in `das/verify_pool.rs`. Do the **chain** loop now (5 lanes, 1 worker — it fixes a dropped-`SlotTick` consensus bug); defer the **gossip** loop until after Stage 3, because its dominant stall is a 12 s cross-process wait a scheduler cannot remove. |
| [Q2](q2-milhouse.md) | milhouse / tree-states integration cost | Cheaper than assumed: the seam is not "only a type alias" — the accessors already match milhouse's API and milhouse 0.9's deps match this workspace exactly. **M** for the in-memory swap (a net deletion, mirroring Lighthouse PR #5533), **L** for on-disk diffs — defer those. The repo's own CC-1H gate measured "clone-dominant" and then applied a hashing-share rule; it answered the wrong question. |
| [Q3](q3-pubkey-cache.md) | The pubkey cache | **Not a 1.5–3 s/block tax — a silent, permanent import stall, and it is live today.** `process_sync_aggregate` has no scan fallback (`sync_aggregate.rs:129` → `CachePoisoned` → `GossipClass::Internal`), and the cache is `skip_deserializing` on `BeaconState` — so `RestoreFromStore` (`restore.rs:437`→`:525`) and storage replay (`replay.rs:644`→`:572`) both fail on the first replayed block, with no gossip involved. Fix is **S**: top up from the registry after every state decode. Stage 0, not Stage 5. |
| [Q4](q4-slashing-db.md) | EIP-3076 slashing-protection DB shape | A dedicated `crates/slashing-protection` — its own **redb** file (pure-Rust, matching the workspace's supply-chain posture; SQLite named as the ecosystem-compatible alternative), `Durability::Immediate`, fail-fast exclusive open, fused `check_and_insert_*` — on the validator-client side of the Stage-5 boundary. It must not ride `services/storage` because that path acknowledges up to **4 s** before it commits (`config/storage.toml:46`) and has a path that claims a failed flush durable (`write_behind.rs:653-657`) — either inverts record-before-sign. **M**; the *decision* costs nothing and should be an ADR now. |
| [Q5](q5-fork-seam.md) | The fork-evolution seam (Stage 4 / "Phase 4.5") | `superstruct` for containers (29-line attribute vs Grandine's ~1,650-line hand-written `combined.rs`); **dispatch the STF by monotone capability predicates** (`fork.gloas_enabled()`), not per-fork modules — that's what Lighthouse does, and it writes each handler once instead of eight times. Decode chokepoints already exist: two functions, five hardcoded-`Fulu` call sites. Gloas modifies **exactly 6** containers and adds 13. Identity bug class is bigger than one bug: **all five** entries of `helpers/constants.rs`'s `pub mod network` are config-scoped in `configs/mainnet.yaml`, and `ChainConfig` parses none of them. Pull that fix forward to Stage 0 — **S**, 2–4 days. |

---

## Cross-cutting findings

Four things surfaced in more than one question and are worth reading as a group.

**1. Sequence milhouse before the Gloas schema work.** Gloas is `−1 / +9` fields on
`BeaconState`. That struct's schema is maintained in four hand-synchronised places
(Q2 §6), one of which — the `StateField` discriminant order — produces a **wrong state
root** rather than a compile error when it drifts. The milhouse swap deletes two of the
four. Doing it first converts the Gloas edit from a four-place synchronised change into
a two-place one, at no extra cost. (Q2 §6, Q5 §4.2.)

**2. `services/chain/src/restore.rs:437` needs one edit that fixes two questions.** It
decodes a state with the raw `from_ssz_bytes` — bypassing the fork chokepoint (Q5) —
and never fills the pubkey cache (Q3), which then breaks the `on_block` loop 88 lines
below at `:525`. One rewrite closes both.

**3. Move caches off `BeaconState`.** Q3's recommendation (pubkey cache onto
`TransitionContext`) is independently what both reference clients do — Lighthouse hangs
it off `BeaconChain`, Grandine makes `pubkey_cache` its own crate. It is also a
*prerequisite* for Q2: `StateCaches` derives `Clone`, so a ~100 MB `HashMap` is deep-copied
on every state clone, which would make milhouse's O(1) clone a lie.

**4. One runtime authority, threaded — the shape all three answers converge on.**
Lighthouse's `ChainSpec` lives in `consensus/types`, exposes `fork_name_at_epoch` as a
data table, and `ForkContext` is *built from* it rather than beside it; the STF takes
`spec: &ChainSpec` and gates on it. This repo has the data (`ChainConfig`) without the
authority — five duplicate fork walks outside `cc-types` (Q5 §1.1), a pubkey cache
inside the state instead of threaded beside it (Q3 §3), and preset-keyed constants where
config-keyed ones belong (Q5 §1). Same fix shape in all three: put the authority in
`cc-types`, thread it, and make omitting it a compile error.

## The one thing to act on this week

**Q3.** It is small, it is a correctness bug rather than an optimisation, and — unlike
most of the study's findings — it is **not** waiting on Stage 3. `RestoreFromStore` and
storage replay both run the state transition on an SSZ-decoded state today, so a node
that has taken a snapshot cannot restore past its anchor. Checkpoint sync is the third
path and *is* gossip-gated; it will present after Stage 3 as *"we wired gossip, peers
are healthy, and the node imports nothing"* with no log line naming the cause.

Eight test harnesses hand-fill the cache that production never fills, and
`state_transition(` appears exactly once in `services/storage/src/replay.rs` — the
production call. That is why no test catches it.

Before quoting the severity, run the 30-line check named in Q3 §5: decode the committed
Hoodi anchor state from SSZ and call `process_block` on a real block. The claim is from
a code-path trace, not an executed reproduction.
