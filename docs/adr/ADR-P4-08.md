# ADR-P4-08 — Prune invariant `I2`: refuse, do not clamp

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-46a
- **Citations:** 1 site — `services/storage/src/prune/blocks.rs:14`
- **Provenance:** re-derived from code (2026-08-16)

## Context

`earliest_available_slot` on the serve window is *derived, never
assigned*. The only legitimate voluntary increase is a block-prune
watermark that stays below the spec serve floor
(`start_slot(current_epoch − floor_epochs)`). A `min` / `clamp` that
silently lowered a too-aggressive mark would hide a config or clock bug
and still advance `PruneMarks.blocks_up_to`. Prysm's rule — do not
voluntarily refuse to serve mandatory block data — is the named
precedent in the refusal log (`prune/blocks.rs:101-108`).

## Decision

Guard `I2` **one level below the window**, on the proposed prune mark
(`i2_check`). If `proposed > start_slot(current_epoch − floor_epochs)`,
**refuse the pass**: increment
`cc_storage_window_increase_rejected_total`, log at `error`, leave
`PruneMarks.blocks_up_to` unchanged. Do not clamp. Do not assign
`earliest_available_slot` directly.

## Consequences

What this makes easy:

- A bad mark is visible (error + counter) and idempotent (marks
  unchanged → next tick retries).
- The serve window cannot jump because a prune helper "helpfully"
  clamped.

What this makes hard:

- An operator who set retention too short sees refused passes, not a
  quietly truncated window. That is the intended failure.

What this forbids:

- A `min(proposed, floor)` / clamp path.
- Assigning `earliest_available_slot` as a config field.
- Advancing `PruneMarks.blocks_up_to` on an `I2` refusal.

## Alternatives considered

**Clamp the mark to the floor and proceed.** Rejected in the module
docs: that is not a guard, it is a silent rewrite of the proposal.

## Refactor impact

**Survives.** Moves with the prune pass into `crates/storage-core` at
S2. The serve-window formula is unchanged by the storage fold.
