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
- `mainnet/fulu/sanity/blocks/pyspec_tests/attestation` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/balance_driven_status_transitions` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/duplicate_attestation_same_block` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/effective_balance_increase_changes_lookahead` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/empty_epoch_transition` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/full_withdrawal_in_epoch_transition` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/historical_batch` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/inactivity_scores_full_participation_leaking` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/inactivity_scores_leaking` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/many_partial_withdrawals_in_epoch_transition` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/multiple_different_validator_exits_same_block` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/partial_withdrawal_in_epoch_transition` -- needs process_epoch -- CC-13d
- `mainnet/fulu/sanity/blocks/pyspec_tests/voluntary_exit` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/activate_and_partial_withdrawal_max_effective_balance` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/activate_and_partial_withdrawal_overdeposit` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/attestation` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/balance_driven_status_transitions` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/duplicate_attestation_same_block` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/effective_balance_increase_changes_lookahead` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/empty_epoch_transition` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/empty_epoch_transition_large_validator_set` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/empty_epoch_transition_not_finalizing` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/eth1_data_votes_consensus` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/eth1_data_votes_no_consensus` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/full_withdrawal_in_epoch_transition` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/historical_batch` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/inactivity_scores_full_participation_leaking` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/inactivity_scores_leaking` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/many_partial_withdrawals_in_epoch_transition` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/multi_epoch_consolidation_chain` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/multiple_different_validator_exits_same_block` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/partial_withdrawal_in_epoch_transition` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/voluntary_exit` -- needs process_epoch -- CC-13d
- `minimal/fulu/sanity/blocks/pyspec_tests/withdrawal_and_consolidation_effective_balance_updates` -- needs process_epoch -- CC-13d
- `mainnet/fulu/random/random` -- needs process_epoch (adversarial multi-block spans) -- CC-13d
- `minimal/fulu/random/random` -- needs process_epoch (adversarial multi-block spans) -- CC-13d

## CC-15
