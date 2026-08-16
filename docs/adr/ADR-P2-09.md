# ADR-P2-09 — Score decay ticks; a bad gossip score does not itself disconnect

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-09, CC-22c
- **Citations:** 3 sites — `services/p2p/src/peer_manager/score.rs:14-17,172-180,188-218`; `services/p2p/src/peer_manager/mod.rs:547-551,657-676,686-702`
- **Provenance:** re-derived from code (2026-08-16)

## Context

The node keeps two score spaces (`score.rs:9-17`):

| Space | Range | Owner | Drives |
|---|---|---|---|
| GossipSub score | ≈ −16000 … +30 | `libp2p-gossipsub` | mesh / gossip / graylist |
| Application score | −100 … +100 | this module | disconnect (`< −20`), ban (`< −50`), eviction, P5 |

GossipSub scores swing hard: a burst of `InvalidMessage` or a graylist
dip can put a peer at −8000 for reasons that are already handled by
gossipsub's own mesh / publish / graylist thresholds. Disconnecting on
that raw number is the death spiral (`score.rs:14-17`): the peer is
kicked, redials, is still graylisted, is kicked again.

Application score is the only disconnect / ban input
(`should_disconnect` / `should_ban` / `enforce_scores` —
`score.rs:199-218`, `mod.rs:686-702`). Gossip couples into app **one-way
and damped**: `app += clamp(gossip_score / 1000, −5, 0)` per decay
interval — never positive (`score.rs:14-16`). Then app decays toward 0
by `APP_SCORE_DECAY = 0.98` once per slot
(`SCORE_DECAY_INTERVAL = 12 s`, `mod.rs:37,657-676`; `score.rs:172-180`).
`set_gossip_score` writes the field for the **next** decay tick; it does
not disconnect (`mod.rs:547-551`). Mesh protection may look at gossip
score to decide eviction candidacy; that is not a disconnect
(`score.rs:188-192`).

`S3a-B-15` (re-dial of just-disconnected bad-`app_score` peers) is a
scheduler bug. This record's policy stays; the fix is not "also
disconnect on gossip score".

## Decision

**Disconnect and ban are `app_score` decisions. A bad gossip score does
not itself disconnect.**

Run a decay tick every slot: sanitize → couple (damped, never-positive)
→ `decay_app_score` → observe → `enforce_scores`. Coupling is the only
path from gossip score into app score. `gossip_score < GossipThreshold`
may lift eviction protection; it must not call `ClosePeer`.

## Consequences

What this makes easy:

- Gossipsub can graylist a peer without the peer-manager death-spiralling
  on Goodbye / redial.
- Penalties (`GossipInvalid`, `ImportInvalid`, …) have one sink
  (`app_score`) and one enforce site.
- `S3a-B-15` can fix the dial scheduler without reopening this policy.

What this makes hard:

- An operator grepping "peer_score" will not find the disconnect
  threshold. The threshold is `APP_SCORE_DISCONNECT = -20`.
- A peer that is only gossip-bad decays back toward 0 unless coupling
  plus penalties pull app under −20. That is intended.

What this forbids:

- `enforce_scores` (or any `ClosePeer` / ban path) branching on
  `gossip_score`.
- Positive gossip-to-app coupling.
- Treating a decay tick as optional instrumentation — it is the only
  coupling / decay site.

## Alternatives considered

**Disconnect when `gossip_score < GossipThreshold` (or Graylist).**
Rejected as the death spiral (`score.rs:16-17`). Gossipsub already
stops meshing / publishing to that peer.

**Two-way or positive coupling** (`app += gossip / N` with a positive
cap). Rejected. A lucky first-delivery burst would wash out real
`ImportInvalid` penalties.

## Refactor impact

**Survives.** `S3a-B-15` patches the dial scheduler, not this policy
(`[ARCH]` §10.4; `s3a-wiring-instrumentation.md`). P0-17a / S3 topic
weights (ADR-P2-10) change gossip numbers, not the app-score disconnect
rule.
