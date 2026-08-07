# Self-devnet fixtures (CC-2Ja)

Phase 2 has no block production, so the publisher (CC-2Jd) replays a
**pre-generated** chain of signed blocks and data-column sidecars. This
directory holds the generator parameters and the regeneration contract.

## What is committed

| Path | Purpose |
|------|---------|
| `devnet.toml` | Generator parameters (seed, slot count ≥512, BPO epochs, blob cycle, slot time) |
| `expected-manifest.json` | Expected half of the reproducibility record (seed, slot count, BPO, GVR, head root after golden run) |
| `out/.gitignore` | Keeps the output directory; ignores generated bulk |
| `README.md` | This file |

## What is generated (gitignored)

```
devnet/out/
  config.yaml          # FULU_FORK_EPOCH: 0, BLOB_SCHEDULE @ 5 & 10, scaled disparity
  genesis.ssz          # BeaconState (Fulu)
  keys/                # Deterministic BLS keys from the seed
  chain/slot_NNNNNN/   # block.ssz + column_XXX.ssz + meta.json
  manifest.json        # Full reproducibility record (incl. per-slot roots)
```

A 512-slot fixture with 128 columns per block is multi-gigabyte. Never commit
`devnet/out/chain/`.

## Regenerate

From the monorepo root:

```bash
cargo run -p cc-devnet-gen --release -- --config devnet/devnet.toml
```

Optional output override:

```bash
cargo run -p cc-devnet-gen -- --config devnet/devnet.toml --out /tmp/devnet-out
```

Two runs from the same `devnet.toml` must produce identical
`genesis_validators_root`, per-slot block roots, and a `manifest.json` that
matches the committed expected roots.

## Parameters of note

- **`SECONDS_PER_SLOT=3`** — BPO epochs 5 and 10 arrive in ~16 minutes wall
  time instead of ~64. `MAXIMUM_GOSSIP_CLOCK_DISPARITY` scales with slot time
  (`500 * seconds_per_slot / 12` ms) so CC-20b never hard-codes 500 ms.
- **`blobs_per_block_cycle`** — non-zero and varies so R-4 blob traffic and
  withheld-column scenarios always have columns to act on.
- **Proposers** come from the real EIP-7917 lookahead
  (`cc-state-transition`), not a counter. Headers are BLS-signed with the
  proposer's generated key.
- **Sidecars** carry real cell KZG proofs (`CellKzg` / c-kzg) and a depth-4
  `blob_kzg_commitments` inclusion proof.

## Unit tests

`cargo test -p cc-devnet-gen` uses a short chain (a few slots) so CI stays
fast. Full 512-slot generation is exercised via the committed `devnet.toml`
and manual / release `cargo run`.

## DAG

`cc-devnet-gen` may depend only on
`{cc-types, cc-crypto, cc-state-transition, cc-config}` (plus ordinary
workspace crates). It must never take `cc-proto` or `cc-libp2p`.
