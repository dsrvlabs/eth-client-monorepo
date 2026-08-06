# Spec-vectors skip list

This file is **append-only within one named section per owning requirement**.
Each entry must carry a **reason** and the **issue number that removes it**
(R-9 — an issue, not merely a requirement). An empty section is the target;
a stale entry that matches no on-disk case is a hard harness failure.

Entry format (one line per skip):

```text
- `preset/fork/runner/handler[/suite[/case…]]` -- reason text -- CC-XXy
```

The path is relative to `tests/`. It may name a case, a suite, a handler, or
any prefix of a case path. The trailing token must be the issue that deletes
the entry (e.g. `CC-12e`).

## CC-10

## CC-11

## CC-12

## CC-13

## CC-15

- `mainnet/fulu/fork_choice` (`tick` step kind) -- `on_tick` structure landed; vector runner not wired -- CC-15c
- `minimal/fulu/fork_choice` (`tick` step kind) -- `on_tick` structure landed; vector runner not wired -- CC-15c
- `mainnet/fulu/fork_choice` (`block` step kind) -- `on_block` / `compute_pulled_up_tip` landed; vector runner not wired -- CC-15c
- `minimal/fulu/fork_choice` (`block` step kind) -- `on_block` / `compute_pulled_up_tip` landed; vector runner not wired -- CC-15c
- `mainnet/fulu/fork_choice` (`attestation` step kind) -- `on_attestation` not implemented -- CC-16
- `minimal/fulu/fork_choice` (`attestation` step kind) -- `on_attestation` not implemented -- CC-16
- `mainnet/fulu/fork_choice` (`attester_slashing` step kind) -- `on_attester_slashing` not implemented -- CC-16
- `minimal/fulu/fork_choice` (`attester_slashing` step kind) -- `on_attester_slashing` not implemented -- CC-16
- `mainnet/fulu/fork_choice` (`checks` step kind) -- full checks assertion and suite green -- CC-15c
- `minimal/fulu/fork_choice` (`checks` step kind) -- full checks assertion and suite green -- CC-15c
- `minimal/fulu/fork_choice_compliance` -- OQ-3 in-scope compliance suite; same step runner as `fork_choice` -- CC-15c
