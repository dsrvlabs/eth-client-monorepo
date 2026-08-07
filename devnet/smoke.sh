#!/usr/bin/env bash
# devnet/smoke.sh — M2.1 strength gate (CC-2Jd booking (a)).
#
# Asserts the *wire* works:
#   1. Anchor serves genesis.ssz, /eth/v1/config/spec, and the state fetch.
#   2. Publisher reaches slot N via *successful* publishes
#      (cc_p2p_backfill_progress_slots advances only after block publish Ok +
#       published counters non-zero).
#   3. node-a has a peer connection (cc_p2p_peers non-zero).
#   4. node-a gossip non-zero on beacon_block and on distinct column subnets.
#   5. Publisher published counters non-zero for block + columns.
#
# Head-following (booking (c) / CC-22d): when CC_DEVNET_SMOKE_HEAD_FOLLOW=1,
# also assert node-a reports non-zero head progress via chain-stream metrics.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

PUB_METRICS="${CC_DEVNET_PUB_METRICS:-http://127.0.0.1:19102/metrics}"
NODE_A_METRICS="${CC_DEVNET_NODE_A_METRICS:-http://127.0.0.1:19112/metrics}"
ANCHOR="${CC_DEVNET_ANCHOR:-http://127.0.0.1:18080}"
SLOT_N="${CC_DEVNET_SMOKE_SLOT_N:-4}"
DEADLINE="${CC_DEVNET_SMOKE_DEADLINE:-240}"
# Minimum distinct column topic labels with non-zero traffic (short fixtures may
# publish all 128; require a meaningful subset for M2.1 gate).
MIN_COLUMN_TOPICS="${CC_DEVNET_SMOKE_MIN_COLUMN_TOPICS:-8}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}
need curl
need python3

scrape() {
  curl -fsS --max-time 5 "$1" 2>/dev/null || true
}

metric_sum() {
  local body="$1"
  local re="$2"
  BODY="${body}" RE="${re}" python3 - <<'PY'
import os, re
pat = re.compile(os.environ["RE"])
total = 0.0
for line in os.environ["BODY"].splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    if not pat.search(line):
        continue
    parts = line.rsplit(None, 1)
    if len(parts) != 2:
        continue
    try:
        total += float(parts[1])
    except ValueError:
        pass
print(total)
PY
}

# Count distinct topic= labels matching re with value > 0.
distinct_topics() {
  local body="$1"
  local re="$2"
  BODY="${body}" RE="${re}" python3 - <<'PY'
import os, re
pat = re.compile(os.environ["RE"])
topics = set()
for line in os.environ["BODY"].splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    if not pat.search(line):
        continue
    parts = line.rsplit(None, 1)
    if len(parts) != 2:
        continue
    try:
        val = float(parts[1])
    except ValueError:
        continue
    if val <= 0:
        continue
    m = re.search(r'topic="([^"]+)"', line)
    if m:
        topics.add(m.group(1))
print(len(topics))
PY
}

echo "==> anchor endpoints"
for path in \
  /genesis.ssz \
  /eth/v1/config/spec \
  /eth/v2/debug/beacon/states/genesis \
  /eth/v1/beacon/genesis; do
  code="$(curl -sS -o /tmp/cc-devnet-anchor.body -w '%{http_code}' --max-time 5 "${ANCHOR}${path}" || true)"
  if [[ "${code}" != "200" ]]; then
    echo "error: anchor ${path} → HTTP ${code}" >&2
    exit 1
  fi
  sz="$(wc -c </tmp/cc-devnet-anchor.body | tr -d ' ')"
  if [[ "${sz}" -lt 2 ]]; then
    echo "error: anchor ${path} empty body" >&2
    exit 1
  fi
  echo "  ${path}: HTTP 200 (${sz} bytes)"
done

echo "==> waiting for publisher slot >= ${SLOT_N} AND published counters (deadline ${DEADLINE}s)"
deadline=$((SECONDS + DEADLINE))
pub_slot=0
p_bb_int=0
while (( SECONDS < deadline )); do
  body="$(scrape "${PUB_METRICS}")"
  # H3: backfill_progress_slots only advances after successful block publish.
  pub_slot="$(metric_sum "${body}" '^cc_p2p_backfill_progress_slots ' | cut -d. -f1)"
  pub_slot="${pub_slot:-0}"
  p_bb="$(metric_sum "${body}" 'cc_p2p_gossip_messages_total\{topic="beacon_block".*verdict="published"')"
  p_bb_int="$(python3 -c "print(int(float('${p_bb}' or 0)))")"
  if (( pub_slot >= SLOT_N && p_bb_int >= SLOT_N )); then
    echo "  publisher slot=${pub_slot} published_blocks=${p_bb_int}"
    break
  fi
  sleep 2
done
if (( pub_slot < SLOT_N || p_bb_int < SLOT_N )); then
  echo "error: publisher slot=${pub_slot} published_blocks=${p_bb_int} (want both >= ${SLOT_N})" >&2
  scrape "${PUB_METRICS}" | head -40 >&2 || true
  exit 1
fi

echo "==> node-a peer connectivity"
deadline=$((SECONDS + 60))
peers_int=0
while (( SECONDS < deadline )); do
  body_a="$(scrape "${NODE_A_METRICS}")"
  peers="$(metric_sum "${body_a}" '^cc_p2p_peers\{')"
  peers_int="$(python3 -c "print(int(float('${peers}' or 0)))")"
  if (( peers_int >= 1 )); then
    break
  fi
  sleep 2
done
if (( peers_int < 1 )); then
  echo "error: node-a cc_p2p_peers sum=${peers_int} (want >= 1)" >&2
  exit 1
fi
echo "  node-a peers sum=${peers_int}"

echo "==> node-a gossip messages (beacon_block + distinct columns)"
# Wait a few more slots for mesh delivery.
sleep "$(( ${CC_DEVNET_SECONDS_PER_SLOT:-3} * 3 ))"
body_a="$(scrape "${NODE_A_METRICS}")"
bb="$(metric_sum "${body_a}" 'cc_p2p_gossip_messages_total\{topic="beacon_block"')"
bb_int="$(python3 -c "print(int(float('${bb}' or 0)))")"
if (( bb_int < 1 )); then
  echo "error: node-a beacon_block gossip_messages_total=${bb}" >&2
  echo "--- node-a metrics (gossip) ---" >&2
  echo "${body_a}" | grep -E 'cc_p2p_gossip_messages' | head -40 >&2 || true
  exit 1
fi
echo "  beacon_block messages=${bb}"

col_topics="$(distinct_topics "${body_a}" 'cc_p2p_gossip_messages_total\{topic="data_column_sidecar_')"
if (( col_topics < MIN_COLUMN_TOPICS )); then
  echo "error: node-a distinct column topics=${col_topics} (want >= ${MIN_COLUMN_TOPICS})" >&2
  echo "${body_a}" | grep -E 'data_column_sidecar_' | head -40 >&2 || true
  exit 1
fi
echo "  distinct column topics=${col_topics} (min ${MIN_COLUMN_TOPICS})"

echo "==> publisher publish counters (columns)"
body_p="$(scrape "${PUB_METRICS}")"
p_col_topics="$(distinct_topics "${body_p}" 'cc_p2p_gossip_messages_total\{topic="data_column_sidecar_')"
if (( p_col_topics < MIN_COLUMN_TOPICS )); then
  echo "error: publisher distinct column topics=${p_col_topics} (want >= ${MIN_COLUMN_TOPICS})" >&2
  exit 1
fi
echo "  publisher distinct column topics=${p_col_topics}"

# CC-22d booking (c): head follows over gossip alone (DA-blind).
# Enabled when chain stream is wired on node-a and publisher is replaying.
if [[ "${CC_DEVNET_SMOKE_HEAD_FOLLOW:-0}" == "1" ]]; then
  echo "==> node-a head-following (CC-22d booking c)"
  # Prefer chain objects sent + verdicts received equality path; require at
  # least SLOT_N chain objects and non-zero accept-class gossip on beacon_block.
  body_a="$(scrape "${NODE_A_METRICS}")"
  sent="$(metric_sum "${body_a}" '^cc_p2p_chain_objects_sent_total ')"
  sent_int="$(python3 -c "print(int(float('${sent}' or 0)))")"
  verdicts="$(metric_sum "${body_a}" '^cc_p2p_chain_verdicts_received_total ')"
  verdicts_int="$(python3 -c "print(int(float('${verdicts}' or 0)))")"
  bb_accept="$(metric_sum "${body_a}" 'cc_p2p_gossip_messages_total\{topic="beacon_block".*verdict="accept"')"
  bb_accept_int="$(python3 -c "print(int(float('${bb_accept}' or 0)))")"
  if (( sent_int < SLOT_N )); then
    echo "error: node-a chain_objects_sent=${sent_int} (want >= ${SLOT_N})" >&2
    exit 1
  fi
  if (( verdicts_int < 1 && bb_accept_int < 1 )); then
    echo "error: node-a no chain verdicts (${verdicts_int}) and no beacon_block accept (${bb_accept_int})" >&2
    exit 1
  fi
  echo "  chain_objects_sent=${sent_int} verdicts=${verdicts_int} beacon_block accept=${bb_accept_int}"
  echo "smoke: PASS (M2.2 head-follow strength)"
else
  echo "smoke: PASS (M2.1 wire strength; set CC_DEVNET_SMOKE_HEAD_FOLLOW=1 for booking c)"
fi
