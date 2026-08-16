# S2 exit note

Living record for the S2 exit criteria. Later issues append; they do not rewrite
prior conclusions.

Recorded 2026-08-16 on `feature/s2-b-15-foreign-client-sourcing`. `S2-B-15`
owns only the W10 prerequisite in this file.

**This file does not discharge E2.1–E2.6.** Those belong to `S2-A-15`,
`S2-J-01`, `S2-A-13`/`S2-A-14`, `S2-B-14`, and `S2-A-12`. No A/B scrape was
run. No E2 family numbers are invented.

---

## W10 prerequisite — five foreign clients sourced (`S2-B-15`)

**Conclusion: sourced and scheduled on our calendar. Not confirmed by the
foreign parties. No outreach was sent from this worktree.**

`[PLAN]` A-5 / D12: the five foreign clients for OQ-1 / M2e need external
coordination with lead time; sourcing starts at S2, not when `S3b-W-10`
opens. `S3a-B-26` (E3a.5) may assert *sourced and scheduled* by citing this
section. It must not upgrade that to *contacted*, *meetings booked*, or
*foreign-confirmed* without new evidence.

**What OQ-1 / M2e will require** (`[PLAN]` §5 W10, `s3b-acceptance-windows.md`
`S3b-W-10`): five production consensus clients, 5–10 peers each, venue
**Hoodi**, codec validation **5/5**. Baseline today is **0/5** and **0 peers
for all five** (`docs/phase-4-soak.md` OQ-1 is `NOT_RUN`). This issue does
not run that probe.

### The five implementations

The production CLs. Not a sixth. Not an EL. Not Grandine.

`docs/phase-4-soak.md` CC-4B listed Grandine as a fifth OQ-1 row. That table
is not rewritten here (W10 fills it). **Lodestar is the fifth** for this
program. Grandine is not a substitute.

| # | Implementation | Maintainer | Official public contact | Cite |
|---|---|---|---|---|
| 1 | **Lighthouse** | Sigma Prime | GitHub `sigp/lighthouse`. Discord named in the README Contact section. | [README Contact](https://github.com/sigp/lighthouse/blob/stable/README.md) — *"The best place for discussion is the [Lighthouse Discord server](https://discord.gg/cyAszAh)."* Repo: https://github.com/sigp/lighthouse |
| 2 | **Prysm** | Prysmatic / Offchain Labs | GitHub `OffchainLabs/prysm`. Discord named in the README. | [README](https://github.com/OffchainLabs/prysm/blob/develop/README.md) — *"Need help? Join our [Discord Community](https://discord.gg/qEZK94mFXP)"* (invite documented as non-expiring). Docs repeat it: https://prysm.offchainlabs.com/docs/contribute/contribution-guidelines/ |
| 3 | **Teku** | Consensys | GitHub `Consensys/teku`. `#teku` on Consensys Discord. | [README](https://github.com/Consensys/teku/blob/master/README.md) — *"get in touch in the #teku channel on [Discord](https://discord.gg/teku)"*. User docs: https://docs.teku.consensys.io/ |
| 4 | **Nimbus** | Status | GitHub `status-im/nimbus-eth2`. Discord + Status `#nimbus-general`. | [nimbus-eth2 README badges](https://github.com/status-im/nimbus-eth2/blob/stable/README.md): Discord https://discord.gg/XRxWahP, Status https://join.status.im/nimbus-general. [The Nimbus Guide — Get in touch](https://nimbus.guide/): Status + Discord. |
| 5 | **Lodestar** | ChainSafe | GitHub `ChainSafe/lodestar`. Discord named in the README. | [README](https://github.com/ChainSafe/lodestar/blob/unstable/README.md) — *"submit an issue or join us on [Discord](https://discord.gg/yjyvFRP)"*. Docs: https://chainsafe.github.io/lodestar/ |

Contacts are **public project channels**, not invented personal emails. This
worktree did **not** post in those Discords, open GitHub issues, or send mail.

### Named owner (our side)

**DSRV Stream B (edge & platform).**

No `CODEOWNERS` and no named individual in README, `[PLAN]`, or `[PRD]`. The
documented owner is the org + the stream this issue is assigned to:

- Org / repo: `dsrvlabs/eth-client-monorepo` (`README.md` scaffold table).
- Copyright: `Copyright 2026 DSRV` (`LICENSE`).
- Stream: `[PLAN]` §9 Stream B — *edge & platform* — owns "docs/ADRs, soak
  operations". `S2-B-15` is a Stream B issue.

Do not invent a person's email.

### Tentative window dates

**All dates below are TENTATIVE. Not confirmed by the foreign party.**

Derived from `[PLAN]` §2 / §5, not from a reply.

| Input | Value |
|---|---|
| Today | 2026-08-16 |
| `[PLAN]` date | 2026-08-15 |
| Program wk 1 | S0a (`[PLAN]` §2) |
| Calendar anchor | ISO week containing the plan date: **2026-W33** = 2026-08-10 … 2026-08-16 |
| S3a | wk 26–35 → 2027-02-01 … **2027-04-11** |
| E3a.5 latest | S3a exit: **2027-04-11**. W10 is after this. |
| S3b | wk 36–47 → 2027-04-12 … **2027-07-04** |
| W10 | Hoodi · 2–3 wk · after W2→W3→W5→W8 on the Hoodi serial path (A-4) · outreach may overlap earlier (`[PLAN]` §5 Σ line) |

High-end Hoodi prefix from 2027-04-12: W2 1.5 wk + W3 2 wk + W5 1 wk + W7 1 wk
+ W8 1 wk, after W0 (self-devnet, not Hoodi), lands ~2027-06-10. W10's 3-week
allowance then occupies S3b wk 45–47.

**Shared W10 window (TENTATIVE):** **2027-06-14 – 2027-07-04** (Hoodi).

One probe of all five, 5–10 peers each, codec 5/5. First-dial dates are
staggered inside that window so each row has an explicit calendar date.

| Implementation | Tentative first-dial (Hoodi) | Window close | Foreign confirmation |
|---|---|---|---|
| Lighthouse | **2027-06-14** | 2027-07-04 | **not confirmed** |
| Prysm | **2027-06-16** | 2027-07-04 | **not confirmed** |
| Teku | **2027-06-18** | 2027-07-04 | **not confirmed** |
| Nimbus | **2027-06-21** | 2027-07-04 | **not confirmed** |
| Lodestar | **2027-06-23** | 2027-07-04 | **not confirmed** |

Re-run cost is high (`[PLAN]` §5): a slip reschedules five parties. If the
S3b Hoodi prefix overruns, slide the shared window, keep the stagger, and
append here. Do not silently edit the dates above.

### What this worktree did and did not do

| Check | Result |
|---|---|
| Five production CLs named | **yes** — Lighthouse, Prysm, Teku, Nimbus, Lodestar |
| Public contact + URL per client | **yes** — table above |
| Tentative Hoodi date per client | **yes** — 2027-06-14 … 2027-06-23, close 2027-07-04, labelled TENTATIVE |
| Named owner on our side | **yes** — DSRV Stream B |
| Emails / Discord posts / GitHub issues sent | **no** |
| Meetings booked | **no** |
| Foreign reply received | **no** |
| OQ-1 / M2e probe run | **no** — still `NOT_RUN` |
| E2.1 A/B | **not this issue** |
| `S3a-B-26` closed | **no** — that issue asserts this record at S3a exit |

### What `S3a-B-26` (E3a.5) may quote

Five named implementations, five named public contacts, five tentative
calendar dates, one named owner. That is *sourced and scheduled* on **our**
books.

That is **not** *contacted* and **not** *foreign-confirmed*. E3a.5 does not
require a reply. If S3a wants confirmation before W0, that is new work and
must be appended, not back-dated into this section.
