# Hoodi fixture rig (CC-10b)

Real Hoodi `SignedBeaconBlock` + `BeaconState` SSZ and a 40-slot consecutive
block sequence, cached **outside** the git tree. This directory holds only the
**expected roots** (and a few hundred bytes of metadata) so consumers share one
fetch instead of four.

## Pinned anchor (§13/8)

| Field | Value |
|---|---|
| **slot** | `3649472` |
| **epoch** | `114046` (> 54016) |
| **block root** | `0x11e0dfd8bba93823b1100efc4de321da5bcd583661519df2710e266c05712060` |
| **state root** | `0x2d4f2b8d81bcb72c3556846a532fca7679e9349b0dc37c9c0812f383daa41c55` |
| **provider** | `beacon.hoodi.ethpandaops.io` |
| **retrieval date** | `2026-08-06` |
| **block size** | `32115` bytes |
| **state size** | `205205311` bytes (≥ 150 MB) |
| **max blob commitments (40-slot seq)** | `21` (> 9) |
| **block SHA-256** | `a87477f2d26512114cea60cab285733b61b1a5aab1a12df3dc73105b510b4a96` |
| **state SHA-256** | `4904f120e0433b965a9a8bc3900b88542438c3ad2e10ba9ae5ce6c42d5fd50ab` |

Authoritative machine-readable copy: [`hoodi-anchor.toml`](./hoodi-anchor.toml),
[`hoodi-sequence.toml`](./hoodi-sequence.toml). **No SSZ bytes are committed**
(`find crates/types/tests/fixtures -size +64k` is empty).

## Fetch (never implicit)

```bash
bash scripts/fetch-hoodi-fixtures.sh
```

- Cache root: `${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}`
  (**must be outside the git worktree**; the script refuses a cache under the repo)
- Slot directory: `<cache root>/<slot>/`
- Artifacts: `signed_beacon_block.ssz`, `beacon_state.ssz`, `sequence/<slot>.ssz`
- Provider fallbacks match PRD CC-19 (ethpandaops beacon + checkpoint servers,
  ethstaker, sigp, chainsafe, stakely, attestant, publicnode).
- curl is HTTPS-only (`--proto '=https' --proto-redir '=https'`) with size caps
  (block ≤ 8 MiB, state ≤ 400 MiB).

### Modes (pin is source of truth)

| Situation | Behaviour |
|---|---|
| Pin present (`hoodi-anchor.toml` + `hoodi-sequence.toml`), cache digests match | **No-op** exit 0 (re-hashes block/state/sequence against the pin) |
| Pin present, cache missing/incomplete/corrupt | **Restore** the *committed* slot/roots into `$CACHE/<pinned_slot>/`; verify every SHA-256; **never rewrite** manifests |
| No pin yet (bootstrap) | Fetch current finalized; write manifests + cache |
| `--force` | **Re-pin** from current finalized; rewrite manifests (intentional pin advance) |

```bash
# CI / local: fill the committed pin (safe; does not dirty TOML)
bash scripts/fetch-hoodi-fixtures.sh

# Intentional re-pin only — then update the actions/cache key below
bash scripts/fetch-hoodi-fixtures.sh --force
```

Rust tests **never** download. A missing cache fails with the literal string:

```text
run scripts/fetch-hoodi-fixtures.sh
```

## Consumers

| Consumer | Issue | Artifact(s) |
|---|---|---|
| `hash_tree_root` of a ≥ 150 MB finalized state | **CC-10/4** (lands in **CC-10g**) | `beacon_state.ssz` + `state_root` / `state_sha256` |
| BLS fixtures (block sig, RANDAO, sync aggregate) | **CC-11/3** (**CC-11a**) | `signed_beacon_block.ssz` + `genesis_validators_root` |
| CI timing ceiling | **§11.3** (**CC-1C**) | full anchor pair |
| 40-slot offline replay | **CC-18d** | `sequence/*.ssz` + `hoodi-sequence.toml` parent-links |

This issue (**CC-10b**) only ships the rig; it does **not** implement the
root check (CC-10g), BLS fixtures (CC-11a), timing (CC-1C), or replay (CC-18d).

## Local / CI skip contract

| Situation | Behaviour |
|---|---|
| `HOODI_FIXTURES_CACHE` **unset** | Cache-dependent tests **skip with a message** (CI without restored cache stays green). Parent-link tests over the committed TOML still run. |
| `HOODI_FIXTURES_CACHE` **set**, cache present | `HoodiFixtures::open` verifies every SHA-256 and returns paths. |
| `HOODI_FIXTURES_CACHE` **set**, cache absent / corrupt | Error naming the artifact and expected/actual SHA-256; message contains `run scripts/fetch-hoodi-fixtures.sh`. |

```bash
# after fetch — exercise the cache-dependent test locally
export HOODI_FIXTURES_CACHE="${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}"
cargo nextest run -p cc-types --test fixtures
```

## CI cache key

```text
hoodi-fixtures-<slot>-<block_sha256>
```

Current value:

```text
hoodi-fixtures-3649472-a87477f2d26512114cea60cab285733b61b1a5aab1a12df3dc73105b510b4a96
```

Restore path: `${{ env.HOODI_FIXTURES_CACHE }}` → absolute
`$HOME/.cache/cc-hoodi-fixtures` (or a runner-local equivalent).

## `actions/cache` block (CC-10h installs this in `ci.yml`)

**CC-10b defines the block; CC-10h is the only Phase 1 issue permitted to edit
`.github/workflows/ci.yml`.** Install the following step(s) verbatim on every
job that runs `cargo nextest` (or a shard that executes CC-10/4 / §11.3 /
CC-18d). Do not invent a second key format.

```yaml
      # CC-10b Hoodi fixture cache — defined in
      # crates/types/tests/fixtures/README.md; installed by CC-10h.
      - name: Hoodi fixtures cache
        uses: actions/cache@v4
        with:
          path: ${{ env.HOODI_FIXTURES_CACHE }}
          key: hoodi-fixtures-3649472-a87477f2d26512114cea60cab285733b61b1a5aab1a12df3dc73105b510b4a96
          restore-keys: |
            hoodi-fixtures-3649472-

      - name: Ensure Hoodi fixtures (miss → fetch)
        env:
          HOODI_FIXTURES_CACHE: ${{ env.HOODI_FIXTURES_CACHE }}
        run: bash scripts/fetch-hoodi-fixtures.sh
```

Job-level env (same file, CC-10h):

```yaml
env:
  HOODI_FIXTURES_CACHE: /home/runner/.cache/cc-hoodi-fixtures
```

When the cache hits, `fetch-hoodi-fixtures.sh` is a no-op (exit 0). When it
misses, the script populates the path and the subsequent `actions/cache`
post-step saves it. Tests that need SSZ bytes observe `HOODI_FIXTURES_CACHE`
set and open the cache; suites that omit the env (or omit this block) skip
those tests rather than fail.

## Layout on disk

```text
${HOODI_FIXTURES_CACHE}/<slot>/
  .complete
  signed_beacon_block.ssz
  beacon_state.ssz          # ≥ 150 MB
  sequence/
    <slot>.ssz              # non-empty slots
    <slot>.empty            # empty slots (marker only)
```

## Regenerating the pin

**Default `bash scripts/fetch-hoodi-fixtures.sh` never advances the pin** once
`hoodi-anchor.toml` exists — it only restores SSZ for the committed roots.

Finality advances; re-pin only when a consumer needs a newer anchor (or when
`max_blob_commitment_count` would otherwise drop to ≤ 9):

```bash
bash scripts/fetch-hoodi-fixtures.sh --force
# review + commit the refreshed hoodi-anchor.toml / hoodi-sequence.toml
# update the key string in the actions/cache block above to match
```
