//! Self-devnet publisher and inert fault modes (CC-2Jd).
//!
//! - **`--publish-fixture`**: plain publisher — loads CC-2Ja's chain fixture,
//!   forces conceptual `cgc = 128`, subscribes to all 128 column subnets, and
//!   publishes each slot's block + sidecars at slot wall-clock cadence.
//! - **Fault kinds** `withhold-column` / `misbehave`: parse and return
//!   "not implemented" (bodies land in CC-2Jb / CC-2Jc). **No-op relay** is
//!   the plain publisher path.
//! - This module does **not** touch the three M2.3 seam files
//!   (`das/custody.rs`, `gossip/validate/column.rs`, `reqresp/columns.rs`).
//!
//! Wire req/resp codec bodies are CC-23a; the fixture store below is what the
//! publisher *will* serve once the codec lands. Unit tests assert by-root /
//! by-range answers from the store today.

#![allow(clippy::unwrap_used, clippy::expect_used)] // tests only below

use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use cc_libp2p::reexport::futures::StreamExt;
use cc_libp2p::reexport::gossipsub::{IdentTopic, MessageAcceptance, TopicHash};
use cc_libp2p::reexport::identity::{self, Keypair};
use cc_libp2p::reexport::{Multiaddr, PeerId, SwarmEvent};
use cc_libp2p::{CcBehaviour, CcBehaviourEvent, SwarmConfig, build_swarm};
use cc_types::{ChainConfig, Epoch, Root};
use discv5::Enr;
use discv5::enr::CombinedKey;
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::fork_digest::compute_fork_digest;
use crate::gossip::{SubnetCounts, TopicName, format_topic_string};
use crate::metrics::{Direction, DirectionLabels, GossipMessageLabels, P2pMetrics};

/// Committed seed string used by `up.sh` / [`derive_node_secret`] (CC-2Jd).
pub const DEVNET_KEY_SEED: &str = "cc-devnet-v1";

/// Number of data-column sidecar subnets (Fulu / mainnet & minimal).
pub const COLUMN_SUBNET_COUNT: u64 = 128;

/// Publisher forces full custody coverage.
pub const PUBLISHER_CGC: u64 = 128;

// ── fault kinds (inert) ─────────────────────────────────────────────────────

/// Adversarial / publisher fault kind.
///
/// Only [`FaultMode::None`] (plain publisher / no-op relay) is implemented in
/// CC-2Jd. The other variants parse and error with a stable message so
/// CC-2Jb/CC-2Jc can fill them without renaming.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FaultMode {
    /// Plain publisher / no-op relay (default).
    #[default]
    None,
    /// CC-2Jb — withhold one or more columns.
    WithholdColumn {
        /// Column indices to withhold (empty until scenario sets them).
        columns: Vec<u64>,
    },
    /// CC-2Jc — misbehave (invalid-column / malformed / spam / …).
    Misbehave {
        /// Kind name as passed on the CLI (kept opaque here).
        kind: String,
    },
}

impl FaultMode {
    /// Parse `--fault-mode` value.
    ///
    /// Accepted forms:
    /// - empty / `none` / `plain` / `relay` → [`FaultMode::None`]
    /// - `withhold-column` / `withhold-column:1,2` → [`FaultMode::WithholdColumn`]
    /// - `misbehave` / `misbehave:spam` → [`FaultMode::Misbehave`]
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty()
            || s.eq_ignore_ascii_case("none")
            || s.eq_ignore_ascii_case("plain")
            || s.eq_ignore_ascii_case("relay")
        {
            return Ok(Self::None);
        }
        if let Some(rest) = s.strip_prefix("withhold-column") {
            let columns = parse_column_list(rest.trim_start_matches([':', '=']))?;
            return Ok(Self::WithholdColumn { columns });
        }
        if let Some(rest) = s.strip_prefix("misbehave") {
            let kind = rest.trim_start_matches([':', '=']).trim();
            let kind = if kind.is_empty() {
                "unspecified".to_owned()
            } else {
                kind.to_owned()
            };
            return Ok(Self::Misbehave { kind });
        }
        bail!("unknown fault mode {s:?}; expected none|withhold-column|misbehave")
    }

    /// Returns `Ok(())` only for the plain publisher. Fault kinds error with
    /// a stable "not implemented" message (CC-2Jd acceptance).
    pub fn ensure_implemented(&self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::WithholdColumn { .. } => {
                bail!("fault mode withhold-column is not implemented (CC-2Jb)")
            }
            Self::Misbehave { kind } => {
                bail!("fault mode misbehave ({kind}) is not implemented (CC-2Jc)")
            }
        }
    }

    /// No-op relay: identity transform of a payload (plain publisher path).
    #[must_use]
    pub fn relay(&self, payload: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::None => Some(payload.to_vec()),
            // Inert until 2Jb/2Jc: refuse to mutate.
            Self::WithholdColumn { .. } | Self::Misbehave { .. } => None,
        }
    }
}

fn parse_column_list(s: &str) -> Result<Vec<u64>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    s.split(',')
        .map(|p| {
            p.trim()
                .parse::<u64>()
                .with_context(|| format!("bad column index {p:?}"))
        })
        .collect()
}

// ── deterministic identity ──────────────────────────────────────────────────

/// Derive a 32-byte secp256k1 secret from the committed seed and a role name.
///
/// `secret = SHA-256(DEVNET_KEY_SEED || ":" || role)`. Deterministic across
/// `up.sh` runs; two runs produce the same peer ids.
#[must_use]
pub fn derive_node_secret(role: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(DEVNET_KEY_SEED.as_bytes());
    h.update(b":");
    h.update(role.as_bytes());
    let digest = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Write raw 32-byte node key with mode `0600` (Unix). Creates parent dirs.
pub fn write_node_key(path: &Path, secret: &[u8; 32]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("create {}", path.display()))?;
        f.write_all(secret)
            .with_context(|| format!("write {}", path.display()))?;
        let mut perms = f.metadata()?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;
    }
    #[cfg(not(unix))]
    {
        fs::write(path, secret).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

/// Load a 32-byte secp256k1 secret from `path`.
pub fn load_node_key(path: &Path) -> Result<[u8; 32]> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() != 32 {
        bail!(
            "node key at {} must be 32 bytes, got {}",
            path.display(),
            bytes.len()
        );
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Build a libp2p [`Keypair`] from a raw 32-byte secp256k1 secret.
pub fn keypair_from_secret(secret: &[u8; 32]) -> Result<Keypair> {
    let mut bytes = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("secp256k1 secret: {e}"))?;
    Ok(Keypair::from(identity::secp256k1::Keypair::from(sk)))
}

/// Build a discv5 [`CombinedKey`] from the same secret.
pub fn combined_key_from_secret(secret: &[u8; 32]) -> Result<CombinedKey> {
    let mut bytes = *secret;
    CombinedKey::secp256k1_from_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("discv5 CombinedKey: {e}"))
}

/// Build a signed ENR for a container (static bootnode wiring).
pub fn build_enr(secret: &[u8; 32], ip: Ipv4Addr, tcp_port: u16, udp_port: u16) -> Result<Enr> {
    let key = combined_key_from_secret(secret)?;
    let enr = Enr::builder()
        .ip4(ip)
        .tcp4(tcp_port)
        .udp4(udp_port)
        .build(&key)
        .map_err(|e| anyhow::anyhow!("enr build: {e}"))?;
    Ok(enr)
}

/// Multiaddr for static dialling (`/ip4/…/tcp/…`).
#[must_use]
pub fn multiaddr_for(ip: Ipv4Addr, tcp_port: u16) -> Multiaddr {
    format!("/ip4/{ip}/tcp/{tcp_port}")
        .parse()
        .expect("static multiaddr is well-formed")
}

// ── fixture store ───────────────────────────────────────────────────────────

/// One slot's block + column sidecars as raw SSZ bytes (CC-2Ja layout).
#[derive(Debug, Clone)]
pub struct SlotFixture {
    /// Slot number.
    pub slot: u64,
    /// Block root from `meta.json` when present (0x-hex).
    pub block_root: Option<[u8; 32]>,
    /// `block.ssz` bytes.
    pub block_ssz: Vec<u8>,
    /// Column index → `column_XXX.ssz` bytes.
    pub columns: HashMap<u64, Vec<u8>>,
}

/// In-memory fixture loaded from `devnet/out/chain/`.
///
/// Serves the data plane for gossip publish and (once CC-23a lands) for
/// `DataColumnSidecarsByRoot` / `ByRange`.
#[derive(Debug, Clone, Default)]
pub struct FixtureStore {
    /// Slots ordered ascending.
    pub slots: Vec<SlotFixture>,
    /// `block_root → slot` for by-root lookup.
    by_root: HashMap<[u8; 32], u64>,
}

impl FixtureStore {
    /// Load `chain_dir/slot_NNNNNN/{block.ssz,column_XXX.ssz,meta.json}`.
    pub fn load(chain_dir: &Path) -> Result<Self> {
        if !chain_dir.is_dir() {
            bail!("fixture chain dir missing: {}", chain_dir.display());
        }
        let mut entries: Vec<PathBuf> = fs::read_dir(chain_dir)
            .with_context(|| format!("read_dir {}", chain_dir.display()))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("slot_"))
            })
            .collect();
        entries.sort();

        let mut slots = Vec::with_capacity(entries.len());
        let mut by_root = HashMap::new();

        for dir in entries {
            let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let slot: u64 = name
                .strip_prefix("slot_")
                .and_then(|s| s.parse().ok())
                .with_context(|| format!("slot dir name {name}"))?;
            let block_ssz = fs::read(dir.join("block.ssz"))
                .with_context(|| format!("read {}/block.ssz", dir.display()))?;
            let mut columns = HashMap::new();
            for e in fs::read_dir(&dir)? {
                let e = e?;
                let fname = e.file_name();
                let fname = fname.to_string_lossy();
                if let Some(rest) = fname.strip_prefix("column_")
                    && let Some(idx_s) = rest.strip_suffix(".ssz")
                    && let Ok(idx) = idx_s.parse::<u64>()
                {
                    columns.insert(idx, fs::read(e.path())?);
                }
            }
            let block_root = read_meta_block_root(&dir.join("meta.json"));
            if let Some(root) = block_root {
                by_root.insert(root, slot);
            }
            slots.push(SlotFixture {
                slot,
                block_root,
                block_ssz,
                columns,
            });
        }
        if slots.is_empty() {
            bail!("no slot_* entries under {}", chain_dir.display());
        }
        Ok(Self { slots, by_root })
    }

    /// Slot range covered (min, max), inclusive.
    #[must_use]
    pub fn slot_range(&self) -> (u64, u64) {
        let min = self.slots.first().map(|s| s.slot).unwrap_or(0);
        let max = self.slots.last().map(|s| s.slot).unwrap_or(0);
        (min, max)
    }

    /// Lookup a slot fixture.
    #[must_use]
    pub fn by_slot(&self, slot: u64) -> Option<&SlotFixture> {
        self.slots.iter().find(|s| s.slot == slot)
    }

    /// `DataColumnSidecarsByRoot` answer from the fixture (store layer).
    #[must_use]
    pub fn sidecar_by_root(&self, block_root: &[u8; 32], column: u64) -> Option<&[u8]> {
        let slot = *self.by_root.get(block_root)?;
        self.by_slot(slot)
            .and_then(|s| s.columns.get(&column).map(|v| v.as_slice()))
    }

    /// `DataColumnSidecarsByRange` answer: all sidecars for `column` in
    /// `[start_slot, start_slot + count)`.
    #[must_use]
    pub fn sidecars_by_range(&self, start_slot: u64, count: u64, column: u64) -> Vec<&[u8]> {
        let end = start_slot.saturating_add(count);
        self.slots
            .iter()
            .filter(|s| s.slot >= start_slot && s.slot < end)
            .filter_map(|s| s.columns.get(&column).map(|v| v.as_slice()))
            .collect()
    }

    /// Max column index observed (expect 127 when full 128 columns present).
    #[must_use]
    pub fn max_column_index(&self) -> Option<u64> {
        self.slots
            .iter()
            .flat_map(|s| s.columns.keys().copied())
            .max()
    }
}

fn read_meta_block_root(path: &Path) -> Option<[u8; 32]> {
    let text = fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let hex = v
        .get("block_root")
        .or_else(|| v.get("root"))
        .and_then(|x| x.as_str())?;
    parse_root_hex(hex).ok()
}

fn parse_root_hex(s: &str) -> Result<[u8; 32]> {
    let s = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    let bytes = hex::decode(s).context("hex decode root")?;
    if bytes.len() != 32 {
        bail!("root must be 32 bytes, got {}", bytes.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

// ── publisher / mesh runtime ────────────────────────────────────────────────

/// How the process behaves on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevnetRole {
    /// Publish fixture at slot cadence (`--publish-fixture`).
    Publisher,
    /// Subscribe + dial static peers; count received gossip (node-a / node-b).
    Peer,
}

/// Runtime configuration for the self-devnet p2p path.
#[derive(Debug, Clone)]
pub struct DevnetRuntimeConfig {
    /// Publisher or passive peer.
    pub role: DevnetRole,
    /// Fixture chain directory (`devnet/out/chain`).
    pub fixture_chain: PathBuf,
    /// Network `config.yaml` for digests / slot time.
    pub config_yaml: PathBuf,
    /// `manifest.json` for genesis validators root + slot count.
    pub manifest_json: PathBuf,
    /// Persisted node key path (32 raw bytes).
    pub node_key_path: PathBuf,
    /// TCP listen multiaddr (e.g. `/ip4/0.0.0.0/tcp/9000`).
    pub listen: Multiaddr,
    /// Static peers to dial (multiaddrs); typically publisher + siblings.
    pub static_peers: Vec<Multiaddr>,
    /// When true, do not dial (single-peer receive-only mode for CC-2Jb).
    pub disable_dial: bool,
    /// Fault mode (must be [`FaultMode::None`] for CC-2Jd).
    pub fault_mode: FaultMode,
    /// Metrics bind address.
    pub metrics_addr: SocketAddr,
    /// gRPC bind (health / GetInfo still served).
    pub grpc_addr: SocketAddr,
    /// Optional wall-clock genesis override (unix seconds). Default: now.
    pub genesis_time_override: Option<u64>,
    /// Seconds per slot override; else from config.yaml.
    pub seconds_per_slot_override: Option<u64>,
    /// Slot to begin publishing from (default: first fixture slot).
    pub start_slot: Option<u64>,
    /// Stop after this many slots published (default: all).
    pub max_slots: Option<u64>,
}

/// Load genesis validators root from `manifest.json`.
pub fn load_gvr_from_manifest(path: &Path) -> Result<Root> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&text).context("manifest json")?;
    let hex = v
        .get("genesis_validators_root")
        .and_then(|x| x.as_str())
        .context("manifest.genesis_validators_root")?;
    let arr = parse_root_hex(hex)?;
    Ok(Root::from_array(arr))
}

/// Topic path-segment labels used for metrics (short names).
#[must_use]
pub fn topic_label(name: TopicName) -> String {
    name.path_segment()
}

/// Build the set of topics the publisher subscribes to / publishes on:
/// `beacon_block` + all 128 `data_column_sidecar_{i}`.
#[must_use]
pub fn publisher_topic_names() -> Vec<TopicName> {
    let mut names = vec![TopicName::BeaconBlock];
    for i in 0..COLUMN_SUBNET_COUNT {
        names.push(TopicName::DataColumnSidecar(i));
    }
    names
}

/// Default peer subscription set at M2.1: block + all columns (sampled set
/// arrives with CC-24a; full subscribe keeps smoke deterministic).
#[must_use]
pub fn peer_topic_names() -> Vec<TopicName> {
    publisher_topic_names()
}

/// Run the self-devnet swarm loop (publisher or peer).
///
/// Also keeps Phase 0 gRPC health + `/metrics` alive on the configured addrs.
pub async fn run_devnet(
    cfg: DevnetRuntimeConfig,
    metrics: P2pMetrics,
    registry: Arc<prometheus_client::registry::Registry>,
) -> Result<()> {
    cfg.fault_mode.ensure_implemented()?;

    let secret = if cfg.node_key_path.exists() {
        load_node_key(&cfg.node_key_path)?
    } else {
        bail!(
            "node key missing at {} — run devnet/up.sh first",
            cfg.node_key_path.display()
        );
    };
    let keypair = keypair_from_secret(&secret)?;
    let local_peer_id = PeerId::from_public_key(&keypair.public());
    info!(%local_peer_id, role = ?cfg.role, "devnet identity");

    let chain_cfg = ChainConfig::from_yaml_file(&cfg.config_yaml)
        .map_err(|e| anyhow::anyhow!("config.yaml: {e}"))?;
    let gvr = load_gvr_from_manifest(&cfg.manifest_json)?;
    let seconds_per_slot = cfg
        .seconds_per_slot_override
        .unwrap_or(chain_cfg.seconds_per_slot.max(1));

    let store = if cfg.role == DevnetRole::Publisher {
        Some(FixtureStore::load(&cfg.fixture_chain)?)
    } else {
        // Peer may still load for by-root self-test when path exists.
        if cfg.fixture_chain.is_dir() {
            FixtureStore::load(&cfg.fixture_chain).ok()
        } else {
            None
        }
    };

    let behaviour = CcBehaviour::new(&keypair, crate::gossip::ethereum_behaviour_config())
        .map_err(|e| anyhow::anyhow!("CcBehaviour: {e}"))?;
    let mut swarm = build_swarm(keypair, behaviour, &SwarmConfig::default())
        .map_err(|e| anyhow::anyhow!("build_swarm: {e}"))?;

    swarm
        .listen_on(cfg.listen.clone())
        .with_context(|| format!("listen on {}", cfg.listen))?;

    // Epoch 0 digest for Fulu-at-genesis devnet.
    let digest = compute_fork_digest(&chain_cfg, gvr, Epoch::new(0));
    let topic_names = match cfg.role {
        DevnetRole::Publisher => publisher_topic_names(),
        DevnetRole::Peer => peer_topic_names(),
    };
    let mut topics: HashMap<String, IdentTopic> = HashMap::new();
    for name in &topic_names {
        let s = format_topic_string(&digest, *name);
        let topic = IdentTopic::new(s.clone());
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .map_err(|e| anyhow::anyhow!("subscribe {s}: {e:?}"))?;
        topics.insert(topic_label(*name), topic);
    }
    info!(count = topics.len(), "subscribed topics");

    // Initial dial of static peers (H2: continuous re-dial keeps recovery alive).
    if cfg.disable_dial {
        info!("dialling disabled (scenario single-peer / no static peers)");
    } else {
        dial_static_peers(&mut swarm, &cfg.static_peers);
    }

    // Metrics exposition (Phase 0 path).
    let metrics_addr = cfg.metrics_addr;
    let reg = Arc::clone(&registry);
    tokio::spawn(async move {
        if let Err(e) = cc_bootstrap::serve_metrics(metrics_addr, reg).await {
            warn!(error = %e, "metrics server exited");
        }
    });

    let genesis_time = cfg.genesis_time_override.unwrap_or_else(now_unix);
    let mut next_publish_slot = cfg
        .start_slot
        .or_else(|| store.as_ref().map(|s| s.slot_range().0));
    let mut published_count: u64 = 0;
    let mut inbound_peers: u64 = 0;
    let mut outbound_peers: u64 = 0;
    // H1: do not catch-up publish until at least one mesh peer is connected
    // (or a timeout elapses so solo publisher can still self-progress).
    let mut mesh_ready = false;
    let mesh_wait_deadline = now_unix().saturating_add(60);

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // H2: periodic re-dial so offline-gap recovery does not need process restart.
    let mut redial_tick = tokio::time::interval(Duration::from_secs(5));
    redial_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        info!(%peer_id, ?endpoint, "connection established");
                        if endpoint.is_dialer() {
                            outbound_peers = outbound_peers.saturating_add(1);
                        } else {
                            inbound_peers = inbound_peers.saturating_add(1);
                        }
                        set_peer_gauges(&metrics, inbound_peers, outbound_peers);
                        if inbound_peers.saturating_add(outbound_peers) > 0 {
                            mesh_ready = true;
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, endpoint, .. } => {
                        info!(%peer_id, "connection closed");
                        if endpoint.is_dialer() {
                            outbound_peers = outbound_peers.saturating_sub(1);
                        } else {
                            inbound_peers = inbound_peers.saturating_sub(1);
                        }
                        set_peer_gauges(&metrics, inbound_peers, outbound_peers);
                        // Immediate re-dial attempt after disconnect (H2).
                        if !cfg.disable_dial {
                            dial_static_peers(&mut swarm, &cfg.static_peers);
                        }
                    }
                    SwarmEvent::Behaviour(CcBehaviourEvent::Gossipsub(ev)) => {
                        handle_gossip_event(ev, &mut swarm, &metrics, &topics);
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        warn!(?peer_id, error = %error, "outgoing connection error");
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        warn!(error = %error, "incoming connection error");
                    }
                    _ => {}
                }
            }
            _ = redial_tick.tick() => {
                if !cfg.disable_dial {
                    dial_static_peers(&mut swarm, &cfg.static_peers);
                }
            }
            _ = tick.tick() => {
                if cfg.role != DevnetRole::Publisher {
                    continue;
                }
                let Some(store) = store.as_ref() else { continue };
                let Some(slot) = next_publish_slot else { continue };
                if let Some(max) = cfg.max_slots
                    && published_count >= max
                {
                    continue;
                }
                // H1: wait for mesh (or timeout) before starting catch-up publish.
                if !mesh_ready {
                    if now_unix() >= mesh_wait_deadline {
                        warn!("mesh wait timed out; publishing without peers");
                        mesh_ready = true;
                    } else {
                        continue;
                    }
                }
                let slot_start = genesis_time.saturating_add(slot.saturating_mul(seconds_per_slot));
                let now = now_unix();
                if now < slot_start {
                    continue;
                }
                let Some(fx) = store.by_slot(slot) else {
                    // Missing slot dir: skip cursor without counting as success.
                    next_publish_slot = Some(slot.saturating_add(1));
                    continue;
                };

                // Block first — H3: only advance slot on successful block publish.
                let mut block_ok = false;
                if let Some(topic) = topics.get("beacon_block")
                    && let Some(payload) = cfg.fault_mode.relay(&fx.block_ssz)
                {
                    match swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
                        Ok(_) => {
                            metrics
                                .gossip_messages
                                .get_or_create(&GossipMessageLabels {
                                    topic: "beacon_block".to_owned(),
                                    verdict: "published".to_owned(),
                                })
                                .inc();
                            block_ok = true;
                        }
                        Err(e) => warn!(slot, error = %e, "publish beacon_block"),
                    }
                }
                if !block_ok {
                    // InsufficientPeers or missing topic: retry same slot next tick.
                    continue;
                }

                // All columns present in fixture (cgc=128 force).
                for (idx, bytes) in &fx.columns {
                    let label = format!("data_column_sidecar_{idx}");
                    let Some(topic) = topics.get(&label) else { continue };
                    if let Some(payload) = cfg.fault_mode.relay(bytes) {
                        match swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
                            Ok(_) => {
                                metrics
                                    .gossip_messages
                                    .get_or_create(&GossipMessageLabels {
                                        topic: label,
                                        verdict: "published".to_owned(),
                                    })
                                    .inc();
                            }
                            Err(e) => warn!(slot, column = idx, error = %e, "publish column"),
                        }
                    }
                }

                metrics.backfill_progress_slots.set(slot as i64);
                published_count = published_count.saturating_add(1);
                info!(slot, published_count, "published fixture slot");
                next_publish_slot = Some(slot.saturating_add(1));
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal");
                break;
            }
        }
    }
    Ok(())
}

fn set_peer_gauges(metrics: &P2pMetrics, inbound: u64, outbound: u64) {
    metrics
        .peers
        .get_or_create(&DirectionLabels {
            direction: Direction::Inbound.as_str().to_owned(),
        })
        .set(inbound as i64);
    metrics
        .peers
        .get_or_create(&DirectionLabels {
            direction: Direction::Outbound.as_str().to_owned(),
        })
        .set(outbound as i64);
}

/// Dial every configured static multiaddr (best-effort; ignores "already dialing").
fn dial_static_peers(swarm: &mut cc_libp2p::Swarm<CcBehaviour>, peers: &[Multiaddr]) {
    for addr in peers {
        match swarm.dial(addr.clone()) {
            Ok(()) => info!(%addr, "dialing static peer"),
            Err(e) => {
                // DialError::DialPeerConditionFalse / NoAddresses etc. are noisy at warn.
                tracing::debug!(%addr, error = %e, "static dial skipped/failed");
            }
        }
    }
}

fn handle_gossip_event(
    ev: cc_libp2p::reexport::gossipsub::Event,
    swarm: &mut cc_libp2p::Swarm<CcBehaviour>,
    metrics: &P2pMetrics,
    topics: &HashMap<String, IdentTopic>,
) {
    use cc_libp2p::reexport::gossipsub::Event;
    match ev {
        Event::Message {
            propagation_source,
            message_id,
            message,
        } => {
            let label = topic_hash_to_label(&message.topic, topics);
            metrics
                .gossip_messages
                .get_or_create(&GossipMessageLabels {
                    topic: label,
                    verdict: "accept".to_owned(),
                })
                .inc();
            // M6: validate_messages() requires explicit Accept so peers re-gossip.
            let _ = swarm.behaviour_mut().gossipsub.report_message_validation_result(
                &message_id,
                &propagation_source,
                MessageAcceptance::Accept,
            );
        }
        Event::Subscribed { peer_id, topic } => {
            info!(%peer_id, %topic, "peer subscribed");
        }
        Event::Unsubscribed { peer_id, topic } => {
            info!(%peer_id, %topic, "peer unsubscribed");
        }
        _ => {}
    }
}

fn topic_hash_to_label(hash: &TopicHash, topics: &HashMap<String, IdentTopic>) -> String {
    for (label, t) in topics {
        if t.hash() == *hash {
            return label.clone();
        }
    }
    hash.to_string()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ── bootnode emission (used by up.sh via `cc-p2p --emit-bootnodes`) ─────────

/// One container's static identity for bootnode wiring.
#[derive(Debug, Clone)]
pub struct BootnodeSpec {
    /// Role name (`publisher`, `node-a`, `node-b`).
    pub role: String,
    /// Docker-DNS hostname (compose service name).
    pub host: String,
    /// Advertised TCP port.
    pub tcp_port: u16,
    /// Advertised UDP port (discv5).
    pub udp_port: u16,
    /// Optional fixed IPv4 for ENR (compose network IP). When `None`, ENR
    /// omits IP and multiaddr uses the hostname via a separate file.
    pub ip: Option<Ipv4Addr>,
}

/// Write keys + `bootnodes.txt` + `multiaddrs.txt` under `out_dir`.
///
/// Two calls with the same seed/specs produce identical peer ids and ENR
/// signatures (same secrets).
pub fn emit_bootnodes(out_dir: &Path, specs: &[BootnodeSpec]) -> Result<()> {
    fs::create_dir_all(out_dir)?;
    let keys_dir = out_dir.join("node_keys");
    fs::create_dir_all(&keys_dir)?;

    let mut enr_lines = Vec::new();
    let mut multi_lines = Vec::new();
    let mut peer_lines = Vec::new();

    for spec in specs {
        let secret = derive_node_secret(&spec.role);
        let key_path = keys_dir.join(format!("{}.key", spec.role));
        write_node_key(&key_path, &secret)?;
        let kp = keypair_from_secret(&secret)?;
        let peer_id = PeerId::from_public_key(&kp.public());

        let ip = spec.ip.unwrap_or(Ipv4Addr::new(127, 0, 0, 1));
        let enr = build_enr(&secret, ip, spec.tcp_port, spec.udp_port)?;
        enr_lines.push(format!(
            "# role={} peer_id={} host={}\n{}",
            spec.role,
            peer_id,
            spec.host,
            enr.to_base64()
        ));
        // Multiaddr uses hostnames for docker DNS when no fixed IP is forced.
        let ma = if spec.ip.is_some() {
            format!("/ip4/{ip}/tcp/{}", spec.tcp_port)
        } else {
            // libp2p dns multiaddr
            format!("/dns/{}/tcp/{}", spec.host, spec.tcp_port)
        };
        multi_lines.push(format!("{} # {} peer_id={}", ma, spec.role, peer_id));
        peer_lines.push(format!("{} {}", spec.role, peer_id));
    }

    fs::write(out_dir.join("bootnodes.txt"), enr_lines.join("\n") + "\n")?;
    fs::write(
        out_dir.join("multiaddrs.txt"),
        multi_lines.join("\n") + "\n",
    )?;
    fs::write(out_dir.join("peer_ids.txt"), peer_lines.join("\n") + "\n")?;
    info!(dir = %out_dir.display(), "wrote bootnodes + keys");
    Ok(())
}

/// Default three-node topology specs (publisher, node-a, node-b).
#[must_use]
pub fn default_bootnode_specs() -> Vec<BootnodeSpec> {
    vec![
        BootnodeSpec {
            role: "publisher".into(),
            host: "publisher".into(),
            tcp_port: 9000,
            udp_port: 9000,
            ip: None,
        },
        BootnodeSpec {
            role: "node-a".into(),
            host: "node-a".into(),
            tcp_port: 9000,
            udp_port: 9000,
            ip: None,
        },
        BootnodeSpec {
            role: "node-b".into(),
            host: "node-b".into(),
            tcp_port: 9000,
            udp_port: 9000,
            ip: None,
        },
    ]
}

// ── CLI helpers ─────────────────────────────────────────────────────────────

/// Parse a listen multiaddr or `host:port` into [`Multiaddr`].
pub fn parse_listen(s: &str) -> Result<Multiaddr> {
    if s.starts_with('/') {
        return Multiaddr::from_str(s).map_err(|e| anyhow::anyhow!("multiaddr: {e}"));
    }
    // host:port → /ip4/0.0.0.0/tcp/port when host is 0.0.0.0 or *
    let (host, port) = s
        .rsplit_once(':')
        .context("listen must be multiaddr or host:port")?;
    let port: u16 = port.parse().context("listen port")?;
    let ip = if host == "0.0.0.0" || host == "*" || host.is_empty() {
        "0.0.0.0"
    } else {
        host
    };
    Multiaddr::from_str(&format!("/ip4/{ip}/tcp/{port}"))
        .map_err(|e| anyhow::anyhow!("multiaddr: {e}"))
}

/// Parse `host:port` into [`SocketAddr`].
pub fn parse_socket_addr(s: &str) -> Result<SocketAddr> {
    if let Ok(a) = SocketAddr::from_str(s) {
        return Ok(a);
    }
    // allow 0.0.0.0:9102 style already covered; try IpAddr
    let (h, p) = s.rsplit_once(':').context("socket addr")?;
    let port: u16 = p.parse()?;
    let ip: IpAddr = h.parse()?;
    Ok(SocketAddr::new(ip, port))
}

/// Read multiaddrs from a file (one per line; `#` comments allowed).
pub fn read_multiaddrs_file(path: &Path) -> Result<Vec<Multiaddr>> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let ma =
            Multiaddr::from_str(line).map_err(|e| anyhow::anyhow!("multiaddr {line:?}: {e}"))?;
        out.push(ma);
    }
    Ok(out)
}

// Silence unused SubnetCounts import warning path for future sampling sets.
#[allow(dead_code)]
fn _subnet_counts_mainnet() -> SubnetCounts {
    SubnetCounts::mainnet()
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn fault_mode_none_parses() {
        assert_eq!(FaultMode::parse("").unwrap(), FaultMode::None);
        assert_eq!(FaultMode::parse("none").unwrap(), FaultMode::None);
        assert_eq!(FaultMode::parse("plain").unwrap(), FaultMode::None);
        FaultMode::None.ensure_implemented().unwrap();
    }

    #[test]
    fn fault_mode_withhold_not_implemented() {
        let m = FaultMode::parse("withhold-column:3,7").unwrap();
        assert!(matches!(
            m,
            FaultMode::WithholdColumn { ref columns } if columns == &[3, 7]
        ));
        let err = m.ensure_implemented().unwrap_err().to_string();
        assert!(err.contains("not implemented"), "{err}");
        assert!(err.contains("CC-2Jb"), "{err}");
    }

    #[test]
    fn fault_mode_misbehave_not_implemented() {
        let m = FaultMode::parse("misbehave:spam").unwrap();
        assert!(matches!(m, FaultMode::Misbehave { ref kind } if kind == "spam"));
        let err = m.ensure_implemented().unwrap_err().to_string();
        assert!(err.contains("not implemented"), "{err}");
        assert!(err.contains("CC-2Jc"), "{err}");
    }

    #[test]
    fn plain_relay_is_identity() {
        let m = FaultMode::None;
        assert_eq!(m.relay(b"abc").as_deref(), Some(b"abc".as_slice()));
    }

    #[test]
    fn key_derivation_is_deterministic() {
        let a = derive_node_secret("publisher");
        let b = derive_node_secret("publisher");
        let c = derive_node_secret("node-a");
        assert_eq!(a, b);
        assert_ne!(a, c);
        let kp1 = keypair_from_secret(&a).unwrap();
        let kp2 = keypair_from_secret(&b).unwrap();
        assert_eq!(
            PeerId::from_public_key(&kp1.public()),
            PeerId::from_public_key(&kp2.public())
        );
    }

    #[test]
    fn emit_bootnodes_stable_peer_ids() {
        let dir1 = tempfile_dir("boot1");
        let dir2 = tempfile_dir("boot2");
        let specs = default_bootnode_specs();
        emit_bootnodes(&dir1, &specs).unwrap();
        emit_bootnodes(&dir2, &specs).unwrap();
        let p1 = fs::read_to_string(dir1.join("peer_ids.txt")).unwrap();
        let p2 = fs::read_to_string(dir2.join("peer_ids.txt")).unwrap();
        assert_eq!(p1, p2);
        assert!(dir1.join("bootnodes.txt").exists());
        assert!(dir1.join("node_keys/publisher.key").exists());
    }

    #[test]
    fn fixture_store_by_root_and_range() {
        let root = tempfile_dir("fixture");
        let slot_dir = root.join("slot_000001");
        fs::create_dir_all(&slot_dir).unwrap();
        fs::write(slot_dir.join("block.ssz"), b"BLOCK1").unwrap();
        fs::write(slot_dir.join("column_000.ssz"), b"COL0").unwrap();
        fs::write(slot_dir.join("column_001.ssz"), b"COL1").unwrap();
        let block_root = [0x11u8; 32];
        let mut meta = fs::File::create(slot_dir.join("meta.json")).unwrap();
        write!(
            meta,
            r#"{{"slot":1,"block_root":"0x{}"}}"#,
            hex::encode(block_root)
        )
        .unwrap();

        let slot2 = root.join("slot_000002");
        fs::create_dir_all(&slot2).unwrap();
        fs::write(slot2.join("block.ssz"), b"BLOCK2").unwrap();
        fs::write(slot2.join("column_000.ssz"), b"COL0s2").unwrap();
        let mut meta2 = fs::File::create(slot2.join("meta.json")).unwrap();
        write!(
            meta2,
            r#"{{"slot":2,"block_root":"0x{}"}}"#,
            hex::encode([0x22u8; 32])
        )
        .unwrap();

        let store = FixtureStore::load(&root).unwrap();
        assert_eq!(store.slot_range(), (1, 2));
        assert_eq!(
            store.sidecar_by_root(&block_root, 0),
            Some(b"COL0".as_slice())
        );
        assert_eq!(
            store.sidecar_by_root(&block_root, 1),
            Some(b"COL1".as_slice())
        );
        assert!(store.sidecar_by_root(&block_root, 2).is_none());
        let range = store.sidecars_by_range(1, 2, 0);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], b"COL0");
        assert_eq!(range[1], b"COL0s2");
    }

    #[test]
    fn publisher_topics_cover_block_and_128_columns() {
        let names = publisher_topic_names();
        assert_eq!(names.len(), 1 + 128);
        assert!(matches!(names[0], TopicName::BeaconBlock));
        assert!(matches!(names[128], TopicName::DataColumnSidecar(127)));
    }

    fn tempfile_dir(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "cc-p2p-fault-{}-{}-{}",
            tag,
            std::process::id(),
            now_unix()
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }
}
