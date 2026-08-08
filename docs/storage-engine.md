# Storage engine (CC-40b)

Concrete engine seam for Phase 4: one inherent `Engine` API in
`crates/store/src/engine/`, body in `engine/redb.rs`. No trait, no `dyn`
(Architecture §1.1 / ADR P4-01).

## V-4 — version and advisories

| Field | Value |
|---|---|
| **Adopted version** | **`redb` 4.1.0** |
| **Check date** | 2026-08-08 |
| **crates.io latest 4.x** | `4.1.0` (`cargo search redb` / `cargo info redb`) |
| **RustSec / advisory-db** | No `redb` advisories present in rustsec/advisory-db; `cargo deny check` clean after adopt |
| **Licence** | MIT OR Apache-2.0 (already on `deny.toml` allow list) |

**Rule (restated):** a newer **minor** of the 4.x line may be taken without redesign;
a **major** must not be taken without re-running this falsifier (`bin/store-bench`)
on both layouts.

## R-14 — does `TableDefinition` accept runtime names?

| Field | Value |
|---|---|
| **API version** | redb **4.1.0** |
| **Date** | 2026-08-08 |
| **Answer** | **Constructor yes; `open_table` elides name to `'static`.** `TableDefinition::new(name: &'a str)` accepts a short-lived `&str`, and redb copies the name into table metadata. However `WriteTransaction::open_table` / `ReadTransaction::open_table` take `TableDefinition<K, V>` with the name lifetime elided, which rustc resolves as **`'static`**. Runtime-built shard names therefore need a **finite intern pool** (`Box::leak` once per unique name; cardinality ≤ ~390 shards + meta) before open. |
| **Implication** | Shards are still **tables** (preferred). Interning is an engine-body detail, not a schema change. The key-prefix fallback in Architecture §2.4 remains available and is what the **flat** falsifier layout exercises. |
| **Source** | `redb-4.1.0/src/db.rs` (`new(&'a str)`); `transactions.rs` `open_table(..., definition: TableDefinition<K, V>)` — name lifetime elided. Verified 2026-08-08 against 4.1.0. |

## Durability

Config: `config/storage.toml` → `durability = "immediate" | "paranoid"` (default
`immediate`). Env override via figment: `CC_STORAGE_DURABILITY=paranoid`
(service layer; `cc-store` does not call `std::env::var`).

| Setting | redb mapping |
|---|---|
| `immediate` | `Durability::Immediate` (1PC+C) |
| `paranoid` | `Immediate` + `set_two_phase_commit(true)` (2PC) |

## Falsifier results (CC-40 /1–2, §8.2)

Bounds: **reject** if p99 prune-commit latency **> 500 ms** or file **> 2.0×**
theoretical live set.

Machine notes and both layouts are sequential with a delete between (D-11).
No concurrent devnet/build started by the harness. Host load averages ~4–5
(other user processes present; no second store-bench / compose stack).

| Layout | Date (UTC) | Scale | Retention ep | Prune ep | p99 prune commit | file / live | Verdict | Notes |
|---|---|---|---|---|---|---|---|---|
| **sharded** | 2026-08-08T04:56–04:57 | 1.0 | 4096 | 64 | **43.0 ms** (0.0430 s) | **1.169×** | **PASS** | build 46.6 s; file 38.01 GiB; live ≈ 32.50 GiB; 0 samples > 0.5 s; store deleted before flat |
| **flat** | 2026-08-08T04:57–04:59 | 1.0 | 4096 | 64 | **37.0 ms** (0.0370 s) | **1.169×** | **PASS** | build 48.7 s; file 38.01 GiB; live ≈ 32.50 GiB; run after sharded delete |

### Detail — sharded (authoritative)

```text
puts=1179648 live_bytes=34603008000 (32.23 GiB) after build
file_len after build/prune = 40813772800 (38.01 GiB)
p99 prune = 0.0430 s; p99 all = 0.0341 s
COMMIT_SECONDS buckets: ≤0.005s:1171 ≤0.01s:863 ≤0.025s:13 ≤0.05s:65 (all ≤0.5s)
verdict = PASS
```

### Detail — flat (after sharded delete)

```text
puts=1179648 live_bytes=34603008000 (32.23 GiB) after build
file_len after build/prune = 40815501312 (38.01 GiB)
p99 prune = 0.0370 s; p99 all = 0.0300 s
COMMIT_SECONDS buckets: ≤0.005s:1172 ≤0.01s:874 ≤0.025s:28 ≤0.05s:38 (all ≤0.5s)
verdict = PASS
```

**Engine choice:** both layouts pass both bounds. **Keep `redb` 4.1.0.** No
`fjall` substitution in this commit.

### How to re-run

```text
# sharded first, then delete, then flat — no concurrent disk-heavy jobs
cargo run -p cc-store-bench --release -- --layout sharded --epochs 64 --scale 1.0 \
  --data-dir /tmp/cc-store-bench-sharded --durability immediate
# delete happens inside the binary unless --keep
cargo run -p cc-store-bench --release -- --layout flat --epochs 64 --scale 1.0 \
  --data-dir /tmp/cc-store-bench-flat --durability immediate
```

`--scale <1.0` shrinks the retention window for CI harness smoke tests; production
AC numbers above used **scale=1.0**.

## Grep property (CC-40 /3)

```bash
grep -rn "redb" crates/ services/ bin/ --include='*.rs'
# must name the crate only under crates/store/src/engine/
```

(Workspace `Cargo.toml` / `crates/store/Cargo.toml` hold the dependency pin line;
that is the sanctioned exception.)

## API surface (twelve methods)

`Engine::{open, read, batch, commit, table_names, drop_table, file_len, compact}`
plus `ReadTxn::{get, range}` and `Batch::{put, delete, delete_range}` —
Architecture §1.1, implemented as one concrete type.
