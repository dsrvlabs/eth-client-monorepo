# ADR-P4-14 — Snapshot containers are never compressed

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-42
- **Citations:** 7 sites — `crates/store/src/snapshots.rs:3,7,29,280`; `config/storage.toml:61`; `services/storage/src/replay.rs:11`; `docs/phase-4-soak.md:154`
- **Provenance:** re-derived from code (2026-08-16)

## Context

A Hoodi BeaconState is ~196–205 MiB uncompressed SSZ
(`docs/phase-4-soak.md:149-154`; `snapshots.rs` cites ~196 MiB).
Compression would make stored length ≠ SSZ length, force `cc-store` (or
the replay task) to know a codec, and couple ring-disk math to a ratio
that moves with the validator set. The ring is depth 4 at a 32-epoch
cadence (~0.70 GB resident, cadence-independent —
`config/storage.toml:61`). Replay + serialize run on the replay task;
the writer only sees a P2 put + oldest-first delete
(`replay.rs:1-11`). `cc-store` never decodes consensus state.

## Decision

Store snapshot values as **opaque uncompressed SSZ**. Stored length
equals SSZ length. `cc-store` never compresses and never decodes the
container (`snapshots.rs:3-7`). Cap a single value at
`MAX_SNAPSHOT_BYTES` (512 MiB). The 2 GiB ring-bytes figure is a
**revisit trigger** for compression, not a licence to compress now.

## Consequences

What this makes easy:

- Disk and soak math are `ring × measured_ssz`. The phase-4 soak clause
  can assert stored length == SSZ length.
- `cc-store` stays consensus-type-blind.

What this makes hard:

- Resident disk is ~0.70 GB for the default ring. Operators who want
  smaller snapshots do not get a flag.

What this forbids:

- Compressing snapshot values (snappy, zstd, or otherwise) in
  `cc-store` or the replay producer.
- Decoding `BeaconState` inside `crates/store` to compress more
  cleverly.
- Treating the 2 GiB ring-bytes trigger as already-taken permission.

## Alternatives considered

**Compress at the 2 GiB ring-bytes line now.** Recorded as a revisit
trigger only (`snapshots.rs:27-29`). Not taken: measured Hoodi states
are ~196 MiB and the default ring is well under the trigger.

**Decode-and-store a slimmer container.** Rejected: `cc-store` does not
decode consensus state.

## Refactor impact

**Survives.** S2 does not change snapshot encoding. milhouse / S4 may
change *what* is snapshotted; uncompressed opaque bytes remain the
store contract unless a new record says otherwise.
