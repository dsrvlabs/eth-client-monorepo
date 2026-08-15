# Spike notes

Findings from the S0a / [ARCH] B.2 spikes. One heading per question.

## Q-9

**Fails** — an uncovered handler does **not** log-and-continue. The harness check is `assert_handler_coverage` at [`crates/spec-tests/src/coverage.rs:18-38`](../../crates/spec-tests/src/coverage.rs): it diffs declared handler names against `Vectors::handlers` and, on any mismatch, returns `Err(Error::Coverage { missing, extra })` (`coverage.rs:37`; `Display` lists both sides at [`crates/spec-tests/src/error.rs:122-130`](../../crates/spec-tests/src/error.rs)). There is no `eprintln!` / `warn!` path in the helper. Callers that use it panic the test ([`crates/types/tests/ssz_static.rs:183-185`](../../crates/types/tests/ssz_static.rs), [`ssz_generic.rs:934-935`](../../crates/types/tests/ssz_generic.rs), [`custody.rs:205-206`](../../crates/types/tests/custody.rs)); STF runners do the same with `assert_eq!` on their on-disk handler sets (e.g. [`crates/state-transition/tests/operations.rs:1128-1138`](../../crates/state-transition/tests/operations.rs)). Scope is **handler-set equality inside a claimed runner**, not a walk of every on-disk case: unowned Fulu runners (`light_client`, `transition`, `fork`, `sync`, `merkle_proof` — [`spec-vectors-layout.md:58`](../../spec-vectors-layout.md)) and `OUT_OF_SCOPE_HANDLERS` (e.g. [`crates/types/tests/custody.rs:41-53`](../../crates/types/tests/custody.rs)) never enter this check and stay silent. Skiplist-empty is already a hard fail ([`crates/spec-tests/src/skiplist.rs:306-314`](../../crates/spec-tests/src/skiplist.rs)).

**W1 measurable as the harness stands: yes.** Extra/missing handlers in a claimed runner fail the suite, and a non-empty `docs/spec-vectors-skiplist.md` fails CI, so "green + skiplist empty" means what M2a/W1's clause says. It does **not** mean [ARCH] §5.6 total-coverage (every on-disk vector claimed or skiplisted); that remains `S4c-04`. No S3a report→fail follow-on — the answer is **fail**, not report.

## Q-10

**Answer: the serve window is never published. There is no production publisher.**
`[ARCH]` §3 / `[PRD]` J-3's "never published" claim holds; the empty-window seed
and the missing publisher are now both verified.

The *intended* publish path exists and is explicitly unwired:

- `StorageServer::publish_window` at `services/storage/src/serve.rs:273-294` is
  marked `#[allow(dead_code)]` with the comment *"exercised by unit tests;
  production callers land with CC-47/48 wiring"*. Every call site is inside
  `#[cfg(test)]` (`serve.rs:1510`, `:2171`, `:2191`, `:2251`, `:2398`, `:2440`,
  `:2485`).
- `StorageServer::derive_and_publish_window` at
  `services/storage/src/serve.rs:296-335` is likewise `#[allow(dead_code)]`
  (*"wired by backfill / hole paths as they land"*) and has **zero** call sites
  in the tree — definition only. That is the only non-test caller of
  `cc_store::write_derived_serve_window`.

What *does* exist, and is not a publisher of a real window:

- The gRPC RPC `WatchServeWindow` (`proto/eth/storage/v1/storage.proto:32`) is
  implemented at `services/storage/src/serve.rs:882-893`. It streams the current
  `watch` channel value; it does not derive or update the window.
- Boot seeds that channel from `empty_window()` (`serve.rs:1084-1095`,
  `earliest_available_slot: u64::MAX` at `:1086-1087`) via `StorageServer::stub`
  (`:225`) or from `load_window_or_default` (`:1097-1123`) via
  `StorageServer::new` (`:249`). The loader returns the empty seed unless
  `KEY_SERVE_WINDOW` is already on disk.
- Persist helpers `put_serve_window` / `write_derived_serve_window` /
  `write_serve_window_from_meta` (`crates/store/src/window.rs:565`, `:579`,
  `:608`) have no production callers. Writes of `KEY_SERVE_WINDOW` in
  `services/storage/src/durable_set.rs` and `bin/cc-store/src/lib.rs` are
  test-fixture / operator-tool only.
- Ingest, prune, write-behind, replay, and migrate never write
  `KEY_SERVE_WINDOW` or call `publish_window`. `PutBackfillBatch`
  (`serve.rs:767-879`) commits blocks, columns, and progress only.
- p2p is a consumer only: `spawn_watch_serve_window_with_metrics`
  (`services/p2p/src/storage_client.rs:389`) applies stream messages at
  `:508-518` onto the ADR-P2-14 `AtomicU64`. Spawned from
  `services/p2p/src/service.rs:394`.

**Implication for `S3a-B-08`:** P0-17c remains a wiring task — attach production
callers to `publish_window` / `derive_and_publish_window` from backfill, prune,
and CGC-raise. There is no hidden live publisher that would invert that framing.

Closes `[PRD]` J-3.

## Q-3

**YES** — `superstruct` composes with milhouse's `List<T, N, U>` third type
parameter. The planned S4 order **4a → 4b → 4c** stands. **`S4-ALT` is not
selected.**

**Settling declaration** (the combination `[Q5]` §6 said nobody had read):

`sigp/lighthouse` `stable` @ `b263df596671`,
`consensus/types/src/state/beacon_state.rs:528-530`

```rust
#[superstruct(only(Altair, Bellatrix, Capella, Deneb, Electra, Fulu, Gloas))]
#[cfg_attr(feature = "arbitrary", arbitrary(default))]
pub inactivity_scores: List<u64, E::ValidatorRegistryLimit>,
```

`List` here is `milhouse::List` (`:11`), on a `#[superstruct]` `BeaconState`
(`:278`, `:433`). `U` is the defaulted third parameter
(`List<T, N, U: UpdateMap<T> = MaxMap<VecMap<T>>>`); writing `List<T, N>` *is*
`List<T, N, U>`.

**The third parameter is also spelled explicitly** on a field of the same
superstruct (`:73-74`, `:479`):

```rust
pub type Validators<E> =
    List<Validator, <E as EthSpec>::ValidatorRegistryLimit, BTreeMap<usize, Validator>>;
// …
pub validators: Validators<E>,
```

That is `U = BTreeMap<usize, Validator>` — a non-default `UpdateMap` — hosted
as an ordinary superstruct field. Superstruct-generated partial getters then
return the milhouse list; e.g. `self.inactivity_scores_mut()?.push(0)` at
`:2040-2041`.

Further `#[superstruct(only(…))]` + milhouse `List` fields on the same struct:
`:495-497` (`previous_epoch_attestations`, Base-only), `:504-507`
(`previous_epoch_participation`), `:613-614` (`pending_deposits`), `:634-635`
(`builders`, Gloas-only). The Gloas −1/+9 field list is already expressed this
way.

**Path note.** `[PLAN]` / `S0a-B-09` name
`consensus/types/src/beacon_state.rs`. On current `stable` the file lives at
`consensus/types/src/state/beacon_state.rs` (moved into the `state/` module).
Same type, new path.

**S4 implication.** ⟡ D-8 ch.2 holds: milhouse (4a) still reduces the
four-place schema to two *before* the Gloas superstruct edit (4b). Do not
invert. No change to `s4-fork-seam.md` dependency tables.
