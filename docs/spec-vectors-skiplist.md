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

- `mainnet/fulu/ssz_generic` -- ssz_generic handlers land at CC-10f -- CC-10f
- `minimal/fulu/ssz_generic` -- ssz_generic handlers land at CC-10f -- CC-10f

## CC-11

## CC-12

## CC-13

## CC-15
