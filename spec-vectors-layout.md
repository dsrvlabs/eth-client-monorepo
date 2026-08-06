# Spec-vector layout manifest

Recorded by `scripts/record-vector-layout.sh` from the on-disk cache for pin
**`v1.7.0-alpha.13`**. Layout is **read off the download, never assumed** (CC-06/5,
Architecture §7.5). Regenerate with:

```bash
bash scripts/record-vector-layout.sh && git diff --exit-code spec-vectors-layout.md
```

| Field | Value |
|---|---|
| **Pinned tag** | `v1.7.0-alpha.13` (from `spec-vectors.lock`) |
| **Cache root** | `${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}` |
| **Tree root** | `<cache root>/v1.7.0-alpha.13/tests` |
| **Trees recorded** | `tests/mainnet/fulu/**`, `tests/general/**` (first three path levels) |

---

## §7.4 consumption contract

Phase 0 ships this contract; Phase 1 implements it in `crates/spec-tests`.

| Element | Value |
|---|---|
| Env var | `SPEC_VECTORS_CACHE` (optional) |
| Cache root | `${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}` |
| Tree root | `<cache root>/<tag>/tests` where `<tag>` is read from `spec-vectors.lock` |
| Readiness | all four `<cache root>/<tag>/.complete-<artifact>` markers exist and their contents equal the lockfile digests, and `tests/` exists |
| Failure mode | panic/error with the literal string `run scripts/fetch-spec-vectors.sh` — **never** an implicit download (CC-06/4) |
| Tag source | `include_str!("…/spec-vectors.lock")` at compile time, so a pin bump forces a rebuild |

Layout under `<cache root>/<tag>/`:

```text
_dl/{general,mainnet,minimal,comptests}.tar.gz   # retained for re-verification
.complete-<artifact>                             # marker; contains verified sha256
tests/                                           # merged unpack root of all four
```

---

## Layout findings (OQ-2 / A-P0-3)

### `operations` vs `block_processing` (A-P0-3)

`operations` **exists as a directory** at `tests/mainnet/fulu/operations/` (children: attestation, attester_slashing, block_header, bls_to_execution_change, consolidation_request, deposit_request, execution_payload, proposer_slashing, sync_aggregate, voluntary_exit, withdrawal_request, withdrawals). There is **no** `block_processing/` directory under Fulu. The research note's Gloas-era inference that operations are emitted only from `block_processing/` does **not** hold for this Fulu tree (A-P0-3 corrected by the download).

### `ssz_generic` and KZG cell vectors (OQ-2 — CC-10 / CC-11)

- `ssz_generic` lives under general at: tests/general/phase0/ssz_generic. Primary path: `tests/general/phase0/ssz_generic/`.
- **No KZG cell-vector suite** (`kzg/`, `compute_cells*`, `verify_cell*`, `recover_cells*`) exists under `tests/**` for pin `v1.7.0-alpha.13`. `general.tar.gz` contains only `tests/general/phase0/ssz_generic/**` and `tests/general/altair/bls/**`. PeerDAS/cell material that *is* present lives under **mainnet/minimal Fulu** as networking and ssz_static cases (e.g. `tests/mainnet/fulu/networking/gossip_data_column_sidecar/`, `…/ssz_static/DataColumnSidecar/`), not as a general-preset KZG proof suite. **CC-10 / CC-11 must not assume a `tests/general/**/kzg` walker for this pin** (OQ-2 half closed by observation).

`general.tar.gz` remains load-bearing for `ssz_generic` (and altair `bls`) even when a dedicated KZG suite is absent from this pin.

### Fulu suite index (top-level under `tests/mainnet/fulu/`)

epoch_processing, finality, fork, fork_choice, light_client, merkle_proof, networking, operations, random, rewards, sanity, ssz_static, sync, transition

### General top-level (under `tests/general/`)

altair, phase0

---

## OQ-1 — Fulu specs vs deployed Hoodi

OQ-1: pin `v1.7.0-alpha.13` `presets/mainnet/fulu` + mainnet config PeerDAS constants **match** deployed Hoodi protocol values (`PRESET_BASE=mainnet`, `NUMBER_OF_COLUMNS=128`, `FIELD_ELEMENTS_PER_CELL=64`, custody/sample knobs); network-identity fields differ as expected (`CONFIG_NAME=hoodi`, `FULU_FORK_VERSION=0x70000910`, `FULU_FORK_EPOCH=50688`). **Deployed behaviour wins** if they ever disagree. Source: `https://beacon.hoodi.ethpandaops.io/eth/v1/config/spec` vs `ethereum/consensus-specs@v1.7.0-alpha.13` `configs/mainnet.yaml` + `presets/mainnet/fulu.yaml`.

---

## `tests/mainnet/fulu` — directories, maxdepth 3

Paths are tree-relative (prefix `tests/…`), sorted with `LC_ALL=C`.

```text
tests/mainnet/fulu
tests/mainnet/fulu/epoch_processing
tests/mainnet/fulu/epoch_processing/effective_balance_updates
tests/mainnet/fulu/epoch_processing/effective_balance_updates/pyspec_tests
tests/mainnet/fulu/epoch_processing/eth1_data_reset
tests/mainnet/fulu/epoch_processing/eth1_data_reset/pyspec_tests
tests/mainnet/fulu/epoch_processing/historical_summaries_update
tests/mainnet/fulu/epoch_processing/historical_summaries_update/pyspec_tests
tests/mainnet/fulu/epoch_processing/inactivity_updates
tests/mainnet/fulu/epoch_processing/inactivity_updates/pyspec_tests
tests/mainnet/fulu/epoch_processing/justification_and_finalization
tests/mainnet/fulu/epoch_processing/justification_and_finalization/pyspec_tests
tests/mainnet/fulu/epoch_processing/participation_flag_updates
tests/mainnet/fulu/epoch_processing/participation_flag_updates/pyspec_tests
tests/mainnet/fulu/epoch_processing/pending_consolidations
tests/mainnet/fulu/epoch_processing/pending_consolidations/pyspec_tests
tests/mainnet/fulu/epoch_processing/pending_deposits
tests/mainnet/fulu/epoch_processing/pending_deposits/pyspec_tests
tests/mainnet/fulu/epoch_processing/proposer_lookahead
tests/mainnet/fulu/epoch_processing/proposer_lookahead/pyspec_tests
tests/mainnet/fulu/epoch_processing/randao_mixes_reset
tests/mainnet/fulu/epoch_processing/randao_mixes_reset/pyspec_tests
tests/mainnet/fulu/epoch_processing/registry_updates
tests/mainnet/fulu/epoch_processing/registry_updates/pyspec_tests
tests/mainnet/fulu/epoch_processing/rewards_and_penalties
tests/mainnet/fulu/epoch_processing/rewards_and_penalties/pyspec_tests
tests/mainnet/fulu/epoch_processing/slashings
tests/mainnet/fulu/epoch_processing/slashings/pyspec_tests
tests/mainnet/fulu/epoch_processing/slashings_reset
tests/mainnet/fulu/epoch_processing/slashings_reset/pyspec_tests
tests/mainnet/fulu/finality
tests/mainnet/fulu/finality/finality
tests/mainnet/fulu/finality/finality/pyspec_tests
tests/mainnet/fulu/fork
tests/mainnet/fulu/fork/fork
tests/mainnet/fulu/fork/fork/pyspec_tests
tests/mainnet/fulu/fork_choice
tests/mainnet/fulu/fork_choice/ex_ante
tests/mainnet/fulu/fork_choice/ex_ante/pyspec_tests
tests/mainnet/fulu/fork_choice/get_head
tests/mainnet/fulu/fork_choice/get_head/pyspec_tests
tests/mainnet/fulu/fork_choice/get_proposer_head
tests/mainnet/fulu/fork_choice/get_proposer_head/pyspec_tests
tests/mainnet/fulu/fork_choice/on_block
tests/mainnet/fulu/fork_choice/on_block/pyspec_tests
tests/mainnet/fulu/light_client
tests/mainnet/fulu/light_client/single_merkle_proof
tests/mainnet/fulu/light_client/single_merkle_proof/BeaconBlockBody
tests/mainnet/fulu/light_client/single_merkle_proof/BeaconState
tests/mainnet/fulu/merkle_proof
tests/mainnet/fulu/merkle_proof/single_merkle_proof
tests/mainnet/fulu/merkle_proof/single_merkle_proof/BeaconBlockBody
tests/mainnet/fulu/networking
tests/mainnet/fulu/networking/compute_columns_for_custody_group
tests/mainnet/fulu/networking/compute_columns_for_custody_group/pyspec_tests
tests/mainnet/fulu/networking/get_custody_groups
tests/mainnet/fulu/networking/get_custody_groups/pyspec_tests
tests/mainnet/fulu/networking/gossip_attester_slashing
tests/mainnet/fulu/networking/gossip_attester_slashing/pyspec_tests
tests/mainnet/fulu/networking/gossip_beacon_aggregate_and_proof
tests/mainnet/fulu/networking/gossip_beacon_aggregate_and_proof/pyspec_tests
tests/mainnet/fulu/networking/gossip_beacon_attestation
tests/mainnet/fulu/networking/gossip_beacon_attestation/pyspec_tests
tests/mainnet/fulu/networking/gossip_beacon_block
tests/mainnet/fulu/networking/gossip_beacon_block/pyspec_tests
tests/mainnet/fulu/networking/gossip_bls_to_execution_change
tests/mainnet/fulu/networking/gossip_bls_to_execution_change/pyspec_tests
tests/mainnet/fulu/networking/gossip_data_column_sidecar
tests/mainnet/fulu/networking/gossip_data_column_sidecar/pyspec_tests
tests/mainnet/fulu/networking/gossip_partial_data_column_sidecar
tests/mainnet/fulu/networking/gossip_partial_data_column_sidecar/pyspec_tests
tests/mainnet/fulu/networking/gossip_proposer_slashing
tests/mainnet/fulu/networking/gossip_proposer_slashing/pyspec_tests
tests/mainnet/fulu/networking/gossip_sync_committee_contribution_and_proof
tests/mainnet/fulu/networking/gossip_sync_committee_contribution_and_proof/pyspec_tests
tests/mainnet/fulu/networking/gossip_sync_committee_message
tests/mainnet/fulu/networking/gossip_sync_committee_message/pyspec_tests
tests/mainnet/fulu/networking/gossip_voluntary_exit
tests/mainnet/fulu/networking/gossip_voluntary_exit/pyspec_tests
tests/mainnet/fulu/operations
tests/mainnet/fulu/operations/attestation
tests/mainnet/fulu/operations/attestation/pyspec_tests
tests/mainnet/fulu/operations/attester_slashing
tests/mainnet/fulu/operations/attester_slashing/pyspec_tests
tests/mainnet/fulu/operations/block_header
tests/mainnet/fulu/operations/block_header/pyspec_tests
tests/mainnet/fulu/operations/bls_to_execution_change
tests/mainnet/fulu/operations/bls_to_execution_change/pyspec_tests
tests/mainnet/fulu/operations/consolidation_request
tests/mainnet/fulu/operations/consolidation_request/pyspec_tests
tests/mainnet/fulu/operations/deposit_request
tests/mainnet/fulu/operations/deposit_request/pyspec_tests
tests/mainnet/fulu/operations/execution_payload
tests/mainnet/fulu/operations/execution_payload/pyspec_tests
tests/mainnet/fulu/operations/proposer_slashing
tests/mainnet/fulu/operations/proposer_slashing/pyspec_tests
tests/mainnet/fulu/operations/sync_aggregate
tests/mainnet/fulu/operations/sync_aggregate/pyspec_tests
tests/mainnet/fulu/operations/voluntary_exit
tests/mainnet/fulu/operations/voluntary_exit/pyspec_tests
tests/mainnet/fulu/operations/withdrawal_request
tests/mainnet/fulu/operations/withdrawal_request/pyspec_tests
tests/mainnet/fulu/operations/withdrawals
tests/mainnet/fulu/operations/withdrawals/pyspec_tests
tests/mainnet/fulu/random
tests/mainnet/fulu/random/random
tests/mainnet/fulu/random/random/pyspec_tests
tests/mainnet/fulu/rewards
tests/mainnet/fulu/rewards/basic
tests/mainnet/fulu/rewards/basic/pyspec_tests
tests/mainnet/fulu/rewards/inactivity_scores
tests/mainnet/fulu/rewards/inactivity_scores/pyspec_tests
tests/mainnet/fulu/rewards/leak
tests/mainnet/fulu/rewards/leak/pyspec_tests
tests/mainnet/fulu/rewards/random
tests/mainnet/fulu/rewards/random/pyspec_tests
tests/mainnet/fulu/sanity
tests/mainnet/fulu/sanity/blocks
tests/mainnet/fulu/sanity/blocks/pyspec_tests
tests/mainnet/fulu/sanity/slots
tests/mainnet/fulu/sanity/slots/pyspec_tests
tests/mainnet/fulu/ssz_static
tests/mainnet/fulu/ssz_static/AggregateAndProof
tests/mainnet/fulu/ssz_static/AggregateAndProof/ssz_random
tests/mainnet/fulu/ssz_static/Attestation
tests/mainnet/fulu/ssz_static/Attestation/ssz_random
tests/mainnet/fulu/ssz_static/AttestationData
tests/mainnet/fulu/ssz_static/AttestationData/ssz_random
tests/mainnet/fulu/ssz_static/AttesterSlashing
tests/mainnet/fulu/ssz_static/AttesterSlashing/ssz_random
tests/mainnet/fulu/ssz_static/BLSToExecutionChange
tests/mainnet/fulu/ssz_static/BLSToExecutionChange/ssz_random
tests/mainnet/fulu/ssz_static/BeaconBlock
tests/mainnet/fulu/ssz_static/BeaconBlock/ssz_random
tests/mainnet/fulu/ssz_static/BeaconBlockBody
tests/mainnet/fulu/ssz_static/BeaconBlockBody/ssz_random
tests/mainnet/fulu/ssz_static/BeaconBlockHeader
tests/mainnet/fulu/ssz_static/BeaconBlockHeader/ssz_random
tests/mainnet/fulu/ssz_static/BeaconState
tests/mainnet/fulu/ssz_static/BeaconState/ssz_random
tests/mainnet/fulu/ssz_static/Checkpoint
tests/mainnet/fulu/ssz_static/Checkpoint/ssz_random
tests/mainnet/fulu/ssz_static/ConsolidationRequest
tests/mainnet/fulu/ssz_static/ConsolidationRequest/ssz_random
tests/mainnet/fulu/ssz_static/ContributionAndProof
tests/mainnet/fulu/ssz_static/ContributionAndProof/ssz_random
tests/mainnet/fulu/ssz_static/DataColumnSidecar
tests/mainnet/fulu/ssz_static/DataColumnSidecar/ssz_random
tests/mainnet/fulu/ssz_static/DataColumnsByRootIdentifier
tests/mainnet/fulu/ssz_static/DataColumnsByRootIdentifier/ssz_random
tests/mainnet/fulu/ssz_static/Deposit
tests/mainnet/fulu/ssz_static/Deposit/ssz_random
tests/mainnet/fulu/ssz_static/DepositData
tests/mainnet/fulu/ssz_static/DepositData/ssz_random
tests/mainnet/fulu/ssz_static/DepositMessage
tests/mainnet/fulu/ssz_static/DepositMessage/ssz_random
tests/mainnet/fulu/ssz_static/DepositRequest
tests/mainnet/fulu/ssz_static/DepositRequest/ssz_random
tests/mainnet/fulu/ssz_static/Eth1Block
tests/mainnet/fulu/ssz_static/Eth1Block/ssz_random
tests/mainnet/fulu/ssz_static/Eth1Data
tests/mainnet/fulu/ssz_static/Eth1Data/ssz_random
tests/mainnet/fulu/ssz_static/ExecutionPayload
tests/mainnet/fulu/ssz_static/ExecutionPayload/ssz_random
tests/mainnet/fulu/ssz_static/ExecutionPayloadHeader
tests/mainnet/fulu/ssz_static/ExecutionPayloadHeader/ssz_random
tests/mainnet/fulu/ssz_static/ExecutionRequests
tests/mainnet/fulu/ssz_static/ExecutionRequests/ssz_random
tests/mainnet/fulu/ssz_static/Fork
tests/mainnet/fulu/ssz_static/Fork/ssz_random
tests/mainnet/fulu/ssz_static/ForkData
tests/mainnet/fulu/ssz_static/ForkData/ssz_random
tests/mainnet/fulu/ssz_static/HistoricalSummary
tests/mainnet/fulu/ssz_static/HistoricalSummary/ssz_random
tests/mainnet/fulu/ssz_static/IndexedAttestation
tests/mainnet/fulu/ssz_static/IndexedAttestation/ssz_random
tests/mainnet/fulu/ssz_static/LightClientBootstrap
tests/mainnet/fulu/ssz_static/LightClientBootstrap/ssz_random
tests/mainnet/fulu/ssz_static/LightClientFinalityUpdate
tests/mainnet/fulu/ssz_static/LightClientFinalityUpdate/ssz_random
tests/mainnet/fulu/ssz_static/LightClientHeader
tests/mainnet/fulu/ssz_static/LightClientHeader/ssz_random
tests/mainnet/fulu/ssz_static/LightClientOptimisticUpdate
tests/mainnet/fulu/ssz_static/LightClientOptimisticUpdate/ssz_random
tests/mainnet/fulu/ssz_static/LightClientUpdate
tests/mainnet/fulu/ssz_static/LightClientUpdate/ssz_random
tests/mainnet/fulu/ssz_static/MatrixEntry
tests/mainnet/fulu/ssz_static/MatrixEntry/ssz_random
tests/mainnet/fulu/ssz_static/PartialDataColumnGroupID
tests/mainnet/fulu/ssz_static/PartialDataColumnGroupID/ssz_random
tests/mainnet/fulu/ssz_static/PartialDataColumnHeader
tests/mainnet/fulu/ssz_static/PartialDataColumnHeader/ssz_random
tests/mainnet/fulu/ssz_static/PartialDataColumnPartsMetadata
tests/mainnet/fulu/ssz_static/PartialDataColumnPartsMetadata/ssz_random
tests/mainnet/fulu/ssz_static/PartialDataColumnSidecar
tests/mainnet/fulu/ssz_static/PartialDataColumnSidecar/ssz_random
tests/mainnet/fulu/ssz_static/PendingConsolidation
tests/mainnet/fulu/ssz_static/PendingConsolidation/ssz_random
tests/mainnet/fulu/ssz_static/PendingDeposit
tests/mainnet/fulu/ssz_static/PendingDeposit/ssz_random
tests/mainnet/fulu/ssz_static/PendingPartialWithdrawal
tests/mainnet/fulu/ssz_static/PendingPartialWithdrawal/ssz_random
tests/mainnet/fulu/ssz_static/PowBlock
tests/mainnet/fulu/ssz_static/PowBlock/ssz_random
tests/mainnet/fulu/ssz_static/ProposerSlashing
tests/mainnet/fulu/ssz_static/ProposerSlashing/ssz_random
tests/mainnet/fulu/ssz_static/SignedAggregateAndProof
tests/mainnet/fulu/ssz_static/SignedAggregateAndProof/ssz_random
tests/mainnet/fulu/ssz_static/SignedBLSToExecutionChange
tests/mainnet/fulu/ssz_static/SignedBLSToExecutionChange/ssz_random
tests/mainnet/fulu/ssz_static/SignedBeaconBlock
tests/mainnet/fulu/ssz_static/SignedBeaconBlock/ssz_random
tests/mainnet/fulu/ssz_static/SignedBeaconBlockHeader
tests/mainnet/fulu/ssz_static/SignedBeaconBlockHeader/ssz_random
tests/mainnet/fulu/ssz_static/SignedContributionAndProof
tests/mainnet/fulu/ssz_static/SignedContributionAndProof/ssz_random
tests/mainnet/fulu/ssz_static/SignedVoluntaryExit
tests/mainnet/fulu/ssz_static/SignedVoluntaryExit/ssz_random
tests/mainnet/fulu/ssz_static/SigningData
tests/mainnet/fulu/ssz_static/SigningData/ssz_random
tests/mainnet/fulu/ssz_static/SingleAttestation
tests/mainnet/fulu/ssz_static/SingleAttestation/ssz_random
tests/mainnet/fulu/ssz_static/SyncAggregate
tests/mainnet/fulu/ssz_static/SyncAggregate/ssz_random
tests/mainnet/fulu/ssz_static/SyncAggregatorSelectionData
tests/mainnet/fulu/ssz_static/SyncAggregatorSelectionData/ssz_random
tests/mainnet/fulu/ssz_static/SyncCommittee
tests/mainnet/fulu/ssz_static/SyncCommittee/ssz_random
tests/mainnet/fulu/ssz_static/SyncCommitteeContribution
tests/mainnet/fulu/ssz_static/SyncCommitteeContribution/ssz_random
tests/mainnet/fulu/ssz_static/SyncCommitteeMessage
tests/mainnet/fulu/ssz_static/SyncCommitteeMessage/ssz_random
tests/mainnet/fulu/ssz_static/Validator
tests/mainnet/fulu/ssz_static/Validator/ssz_random
tests/mainnet/fulu/ssz_static/VoluntaryExit
tests/mainnet/fulu/ssz_static/VoluntaryExit/ssz_random
tests/mainnet/fulu/ssz_static/Withdrawal
tests/mainnet/fulu/ssz_static/Withdrawal/ssz_random
tests/mainnet/fulu/ssz_static/WithdrawalRequest
tests/mainnet/fulu/ssz_static/WithdrawalRequest/ssz_random
tests/mainnet/fulu/sync
tests/mainnet/fulu/sync/optimistic
tests/mainnet/fulu/sync/optimistic/pyspec_tests
tests/mainnet/fulu/transition
tests/mainnet/fulu/transition/core
tests/mainnet/fulu/transition/core/pyspec_tests
```

---

## `tests/general` — directories, maxdepth 3

```text
tests/general
tests/general/altair
tests/general/altair/bls
tests/general/altair/bls/eth_aggregate_pubkeys
tests/general/altair/bls/eth_fast_aggregate_verify
tests/general/phase0
tests/general/phase0/ssz_generic
tests/general/phase0/ssz_generic/basic_progressive_list
tests/general/phase0/ssz_generic/basic_vector
tests/general/phase0/ssz_generic/bitlist
tests/general/phase0/ssz_generic/bitvector
tests/general/phase0/ssz_generic/boolean
tests/general/phase0/ssz_generic/compatible_unions
tests/general/phase0/ssz_generic/containers
tests/general/phase0/ssz_generic/progressive_bitlist
tests/general/phase0/ssz_generic/progressive_containers
tests/general/phase0/ssz_generic/uints
```

---

## Regenerating

```bash
# Requires a ready cache (CC-06a):
bash scripts/fetch-spec-vectors.sh
bash scripts/record-vector-layout.sh
git diff --exit-code spec-vectors-layout.md
```

This file is owned by CC-06b. Do not hand-edit the directory listings; change
the recorder script and re-run.
