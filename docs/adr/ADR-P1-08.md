# ADR-P1-08 — Checkpoint context is derived data behind an LRU of 8

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1
- **Issues:** S1-B-08, CC-16
- **Citations:** 7 sites — `crates/fork-choice/src/checkpoint_context.rs:1,6,41`; `crates/fork-choice/src/on_attestation.rs:25,230,235`; `crates/fork-choice/src/store.rs:138`; capacity at `store.rs:29,205`
- **Provenance:** re-derived from code (2026-08-16)

## Context

The consensus spec's `store_target_checkpoint_state` stores a full
`BeaconState` per checkpoint. With a flat 150–200 MB state that map is an
OOM with a spec citation (`checkpoint_context.rs:5-7`). Phase 1's
`on_attestation` (CC-16) does not need a full state. It needs the fields
the delta pass and committee resolution actually read.

## Decision

Replace `store_target_checkpoint_state` with
`store_target_checkpoint_context` (`on_attestation.rs:230-235`). The store
holds a [`CheckpointContext`] — derived checkpoint data, not a state
clone:

- committee / shuffling shell → `get_beacon_committee` / attesting indices
- effective balances + total active balance → `compute_deltas` at
  justification change
- `fork` + `genesis_validators_root` → signature domain (carried even
  though Phase 1 does not verify store-level attestation signatures)

The map is an LRU of capacity **8**
(`DEFAULT_CHECKPOINT_CONTEXT_CAPACITY`, `store.rs:29,138`). Worst case
~64 MB. **If a future phase finds a read this struct cannot serve, add
the field. Do not reintroduce the full state**
(`checkpoint_context.rs:16-17`).

Effective balances zero slashed / inactive validators so `compute_deltas`
cannot apply weight for them (`checkpoint_context.rs:72-87`).

## Consequences

What this makes easy:

- Attestation weight stays correct without pinning eight full states.
- Signature-domain work later is eight bytes now, not a two-crate change.

What this makes hard:

- A new `on_attestation` read that is not on this struct is an explicit
  field addition, reviewed against the "do not reintroduce the state"
  rule.

What this forbids:

- A `HashMap<Checkpoint, BeaconState>` (or equivalent) as the checkpoint
  cache.
- An unbounded checkpoint-context map.
- Serving `compute_deltas` from live head balances instead of the
  checkpoint snapshot.

## Alternatives considered

**Store the spec's full `BeaconState` per checkpoint.** Rejected in the
citing module: it is an OOM at mainnet size.

**Store nothing and re-derive from the nearest resident state on every
attestation.** Rejected: the delta pass needs a stable justified-epoch
snapshot; re-deriving under a moving head is the bug the LRU exists to
stop.

None further recorded.

## Refactor impact

**Survives.** Interacts with milhouse ([q2] / `ADR-P1-04`) only in that
checkpoint data stays a derived snapshot, not a second copy of
`BeaconState`. S4 must not "fix" attestation by putting full states back
in the store.
