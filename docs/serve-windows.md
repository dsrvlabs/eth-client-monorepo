# Serve windows (CC-4A)

Block and column historical serve obligations derived from the consensus-specs
p2p interface. Phase 4 stores and advertises these windows; this document
records the **derivation**, the **arithmetic**, and the **spec-delta** that
inverted the Phase 2 "read from config" rule.

| Field | Value |
|---|---|
| **Pinned tag** | `ethereum/consensus-specs` **`v1.7.0-alpha.13`** |
| **Retrieval date** | **2026-08-08** |
| **Module** | `crates/store/src/window.rs` |

## Spec function

From `specs/phase0/p2p-interface.md` @ `v1.7.0-alpha.13`:

```python
def compute_min_epochs_for_block_requests() -> uint64:
    """
    Compute the minimum number of epochs for which a node must serve blocks.
    """
    return MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT // 2
```

Rust authority (`cc_store::window`):

```rust
// CC-4A: computed, never read from config. The config field, if present, is a cross-check.
// SEC-4A-1: checked_div / checked_add — overflow is Err, never wraps.
fn compute_min_epochs_for_block_requests(
    cfg: &BlockServeWindowCfg,
) -> Result<u64, WindowConfigError> {
    let half = cfg.churn_limit_quotient.checked_div(2).ok_or(...)?;
    cfg.min_validator_withdrawability_delay.checked_add(half).ok_or(...)
}
```

### Security findings

| ID | Finding | Disposition |
|---|---|---|
| **SEC-4A-1** | Wrapping `u64` arithmetic could under-state the floor | **Fixed** — `checked_div` / `checked_add`, fail-closed |
| **SEC-4A-2** | Dual authority: Phase 2 `services/p2p` still has its own floor constant alongside `cc_store::window` | **Wontfix in CC-4A** — single-authority wire-up is **`CC-49`** (Stream N advertisement branch). This issue is Stream S (`crates/store` + `docs/`) and must not own `services/p2p` beyond grep hygiene. |

## Hoodi / mainnet arithmetic (V-1)

Values read from `eth-clients/hoodi` `metadata/config.yaml` @ `main` and the
committed fixtures under `crates/types/tests/fixtures/` on **2026-08-08**:

| Key | Value |
|---|---|
| `MIN_VALIDATOR_WITHDRAWABILITY_DELAY` | **256** |
| `CHURN_LIMIT_QUOTIENT` | **65 536** |
| Computed floor | `256 + 65 536 / 2 = 256 + 32 768 =` **33 024** epochs |
| Vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` on Hoodi | **still present** (`33024`) — cross-check only |
| Same field on mainnet-spec config @ alpha.13 | **absent** |

### Wall-clock duration (blocks)

With `SLOTS_PER_EPOCH = 32` and `SECONDS_PER_SLOT = 12`:

\[
33\,024 \times 32 \times 12\,\mathrm{s} = 12\,681\,216\,\mathrm{s} = 146.77\,\mathrm{days}
\]

(\(12\,681\,216 / 86\,400 \approx 146.77\).)

### Column counterpart

`MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS = 4 096` epochs (Fulu networking
config; not recomputed here):

\[
4\,096 \times 32 \times 12\,\mathrm{s} = 1\,572\,864\,\mathrm{s} = 18.20\,\mathrm{days}
\]

## Spec delta 1 — field removed from configs

`MIN_EPOCHS_FOR_BLOCK_REQUESTS` **was removed from the spec's configs at
`v1.7.0-alpha.13`**:

| Tag | Present in `configs/mainnet.yaml`? |
|---|---|
| `v1.5.0` | yes (`= 33024`, line 123) |
| `v1.6.0` | yes (`= 33024`, line 168) |
| **`v1.7.0-alpha.13`** | **no** — also absent from `configs/minimal.yaml`, `presets/mainnet/phase0.yaml`, and the p2p-interface config table |

Hoodi (`eth-clients/hoodi` `metadata/config.yaml`) still carries the vestigial
field with a comment naming the formula
(`MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT // 2 (= 33,024) epochs`).

**Phase 2** (`CC-26a`, `CC-23c`) instructed "read it from config rather than
recompute". At the pinned tag that instruction points at a deleted field.
**Phase 4 inverts the rule** (`CC-4A`):

1. Compute the floor from the two live config scalars.
2. If a config file still supplies `MIN_EPOCHS_FOR_BLOCK_REQUESTS` and it is
   **unequal** to the computed value, **refuse to start**, naming both numbers.
3. If the field is **absent**, start normally (mainnet-spec path).
4. Production code must not hard-code `33024` / `33_024` — only tests and
   comments may mention the number (`grep` guard, CC-4A /4).

## Consumers (not implemented here)

| Consumer | Issue | Role |
|---|---|---|
| Branch-1 block floor / advertisement | `CC-49` | uses the computed constant |
| `I2` prune-mark guard | `CC-46a` | rejects prune past the floor |
| Block backfill completion | `CC-47b` | target = current − computed floor |
| Window derivation | `CC-48` | extends `window.rs` |

## Retrieval log

| What | Source | Date |
|---|---|---|
| Function body | `ethereum/consensus-specs` `specs/phase0/p2p-interface.md` @ `v1.7.0-alpha.13` | 2026-08-08 |
| Field absence | grep over mainnet/minimal configs + phase0 presets @ alpha.13 | 2026-08-07 (plan) / reconfirmed 2026-08-08 |
| Hoodi vestigial field + scalars | `eth-clients/hoodi` `metadata/config.yaml` @ `main` | 2026-08-08 |
| Fixture scalars | `crates/types/tests/fixtures/{hoodi,mainnet}-config.yaml` | 2026-08-08 |
