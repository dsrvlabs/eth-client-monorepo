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

- `mainnet/fulu/sanity/blocks` -- process_block handlers after process_block_header land in CC-12b–d -- CC-12e
- `minimal/fulu/sanity/blocks` -- process_block handlers after process_block_header land in CC-12b–d -- CC-12e

## CC-13

- `mainnet/fulu/sanity/slots/pyspec_tests/balance_change_affects_proposer` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/double_empty_epoch` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/effective_decrease_balance_updates_lookahead` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/empty_epoch` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/historical_accumulator` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_above_upward_threshold` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_below_upward_threshold` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_compounding` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_different_signature` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/over_epoch_boundary` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/pending_consolidation` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/slots/pyspec_tests/pending_deposit_extra_gwei` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/balance_change_affects_proposer` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/double_empty_epoch` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/effective_decrease_balance_updates_lookahead` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/empty_epoch` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/historical_accumulator` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_above_upward_threshold` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_below_upward_threshold` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_compounding` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/multiple_pending_deposits_same_pubkey_different_signature` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/over_epoch_boundary` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/pending_consolidation` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/slots/pyspec_tests/pending_deposit_extra_gwei` -- needs process_epoch -- CC-13d

## CC-15
