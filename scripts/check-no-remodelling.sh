#!/usr/bin/env bash
# CC-02/4 — consensus objects must not be re-modelled as proto messages.
# Architecture §2.2 invariant 2 / §3.2: BeaconBlock, BeaconState, Attestation (and kin)
# cross service boundaries as `bytes ssz` + metadata only.
#
# Exit 0 with no output when clean; non-zero and print hits on any match.
# CC-03 wires this into the `proto` CI job as a blocking step.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Trailing space on Attestation keeps `AttestationService` / package paths from matching.
if hits="$(grep -rn "message .*BeaconBlock\|message .*BeaconState\|message .*Attestation " proto/ 2>/dev/null || true)" \
  && [[ -n "${hits}" ]]; then
  echo "error: consensus container re-modelled as a proto message (CC-02/4):" >&2
  echo "${hits}" >&2
  exit 1
fi

exit 0
