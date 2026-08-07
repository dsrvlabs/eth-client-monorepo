# CC-22e hostile-input corpus

Seed corpus and capture-to-corpus layout for the gossip hostile-input harness
(`services/p2p/tests/hostile_input.rs`).

## Source

| Field | Value |
|-------|-------|
| **Source** | Self-devnet-shaped SSZ objects (`Default` containers + minimal non-empty column lists), standing in for CC-2Ja fixture / booking (c) DA-blind gossip capture at M2.2 |
| **Network** | synthetic / mainnet-preset (Hoodi-compatible container shapes) |
| **Date** | 2026-08-07 |
| **RNG seed (harness)** | `0x00CC_22E0_C0B5_5500` (`MASTER_SEED` in `hostile_input.rs`) |

R-9 (CC-29c) requires refreshing these seeds from the rehearsal's captured
Hoodi traffic before the soak. Regeneration is one command (see below).

## Layout

```
corpus/
  README.md                 # this file
  meta.json                 # machine-readable source / network / date
  seeds/
    <topic_family>.ssz      # one real object per Fulu topic family (10)
  sample_capture/           # committed sample for CI script exercise
    slot_NNNNNN/block.ssz
    slot_NNNNNN/column_XXX.ssz
    ops/<topic_family>.ssz  # operation / attestation / sync seeds
```

Topic families (must match the registry's Fulu family set):

- `beacon_block`
- `beacon_aggregate_and_proof`
- `beacon_attestation` (all subnet ids share one seed / bound)
- `data_column_sidecar` (all subnet ids share one seed / bound)
- `sync_committee_contribution_and_proof`
- `sync_committee`
- `voluntary_exit`
- `proposer_slashing`
- `attester_slashing`
- `bls_to_execution_change`

## Generate / refresh seeds

```bash
# Synthetic Default containers (CI / first-time seed):
cargo run -p cc-p2p --bin gen_hostile_corpus --locked

# From a capture (R-9 — overwrites seeds/ from a capture directory):
./scripts/corpus-from-capture.sh path/to/capture services/p2p/tests/fixtures/corpus/seeds

# Capture layouts accepted:
#   1) CC-2Ja chain:  slot_*/block.ssz + slot_*/column_*.ssz
#   2) Flat / ops:    {topic_family}.ssz anywhere under the capture tree
```

CI exercises the script once against `sample_capture/` (see
`capture_to_corpus_script_exercised` in `hostile_input.rs`).

## Harness knobs

| Knob | Value |
|------|-------|
| Random inputs per family | 10 000 |
| Mutated-valid inputs per family | 10 000 |
| Mutations | bit-flip, byte corrupt, truncate, length-field (offset) tampers, append |
| Allocation bound | `max_container_bytes` (CC-22b table) + 64 KiB fixed overhead |
| Panic policy | failure; prints topic, master seed, case index, input prefix |

## Measured runtime

Recorded when the harness was landed / last tuned:

| Machine class | Command | Wall time |
|---------------|---------|-----------|
| dev (2026-08-07, this worktree) | `cargo test -p cc-p2p --test hostile_input hostile_input_no_panic -- --nocapture` | **~0.13 s** (10 families × 10k random × 10k mutated) |

Target: stay inside Phase 0's ten-minute CI `test` budget (CC-07/4).
Generation is in-process from seeds (no 200 000 committed blobs).
