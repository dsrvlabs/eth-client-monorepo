//! `p2p` service — Architecture §4.1, CC-01b / CC-20b + CC-2Jd publisher mode.
//!
//! Phase 0 surface: health + reflection + `GetInfo`.
//! CC-20b: persisted identity (load **before** bind), swarm task, supervisor,
//! clock, §2.2 channel map. Listens; does **not** dial (CC-21c).
//!
//! CC-2Jd: `--publish-fixture` / `--devnet-peer` / `--emit-bootnodes` take a
//! separate self-devnet path (`fault_mode::run_devnet`); absent those flags the
//! existing 20b `run_process` path is preserved.
//!
//! CC-29a: §12 metric families registered into `bs.registry` between `init` and
//! serve.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use cc_bootstrap::{PeerSpec, SignalTrigger, TelemetrySettings};
use cc_config::{EnvTelemetry, ServiceConfig};
use cc_p2p::clock::ClockConfig;
use cc_p2p::engine_stream::build_minimal_engine_stream;
use cc_p2p::fault_mode::{
    BootnodeSpec, DEFAULT_RELEASE_FLAG_PATH, DEFAULT_WITHHOLD_TARGET_ROLE, DevnetRole,
    DevnetRuntimeConfig, FaultMode, default_bootnode_specs, emit_bootnodes, parse_listen,
    parse_socket_addr, read_multiaddrs_file, run_devnet,
};
use cc_p2p::identity::{self, DEFAULT_NODE_KEY_PATH};
use cc_p2p::metrics::P2pMetrics;
use cc_p2p::service::{
    DEFAULT_LISTEN_MULTIADDR, P2pGrpcService, RuntimeConfig, RuntimeError, SERVICE, run_process,
    service_spec,
};
use cc_proto::p2p::p2p_service_server::P2pServiceServer;
use cc_types::CUSTODY_REQUIREMENT;
use clap::Parser;
use serde::Deserialize;
use tonic::service::Routes;

/// Full gRPC path for the Phase 0 RPC (metrics label normalisation).
const GET_INFO_METHOD: &str = "/eth.p2p.v1.P2pService/GetInfo";
/// CC-21d RPC path (metrics label normalisation).
const SET_CGC_METHOD: &str = "/eth.p2p.v1.P2pService/SetCustodyGroupCount";

/// CLI for CC-2Jd publisher / peer / bootnode emission; absent flags → CC-20b.
#[derive(Debug, Parser)]
#[command(
    name = "cc-p2p",
    about = "Consensus client P2P service (CC-20b runtime + CC-2Jd publisher)"
)]
struct Cli {
    /// Replay CC-2Ja fixture onto gossip at slot cadence (forces cgc=128).
    #[arg(long, value_name = "CHAIN_DIR")]
    publish_fixture: Option<PathBuf>,

    /// Peer mesh mode: dial static peers and count gossip (node-a / node-b).
    #[arg(long)]
    devnet_peer: bool,

    /// Fault kind: `none` (default), `withhold-column[=idx,…]`, `misbehave` (CC-2Jc inert).
    /// Fault kind: `none` (default), `withhold-column` (CC-2Jb), or
    /// `misbehave:<kind>` with kind ∈ invalid-column|malformed|spam|custody-refuse|stall-reqresp (CC-2Jc).
    #[arg(long, default_value = "none")]
    fault_mode: String,

    /// Flag file whose presence releases withheld columns for by-root serve (CC-2Jb).
    /// Default when withhold-column: `/fault/cc-release-columns.flag` (or `CC_P2P_FAULT_FLAG`).
    #[arg(long, env = "CC_P2P_FAULT_FLAG")]
    fault_flag_path: Option<PathBuf>,

    /// Role whose sampled set must contain every withheld index (default `node-a`).
    #[arg(long, default_value = DEFAULT_WITHHOLD_TARGET_ROLE, env = "CC_P2P_WITHHOLD_TARGET")]
    withhold_target_role: String,

    /// Target custody group count for the sampled-set precondition (default 4).
    #[arg(long, default_value_t = CUSTODY_REQUIREMENT, env = "CC_P2P_WITHHOLD_TARGET_CGC")]
    withhold_target_cgc: u64,

    /// Write deterministic keys + `bootnodes.txt` under this directory and exit.
    #[arg(long, value_name = "OUT_DIR")]
    emit_bootnodes: Option<PathBuf>,

    /// Path to 32-byte secp256k1 node key (written by `up.sh`).
    #[arg(long, env = "CC_P2P_NODE_KEY_PATH")]
    node_key: Option<PathBuf>,

    /// Listen multiaddr or `host:port` (default `/ip4/0.0.0.0/tcp/9000`).
    #[arg(long, env = "CC_P2P_LISTEN", default_value = "/ip4/0.0.0.0/tcp/9000")]
    listen: String,

    /// File of multiaddrs to dial (one per line).
    #[arg(long, env = "CC_P2P_STATIC_PEERS_FILE")]
    static_peers_file: Option<PathBuf>,

    /// Comma-separated multiaddrs to dial (appended to file entries).
    #[arg(long, env = "CC_P2P_STATIC_PEERS", default_value = "")]
    static_peers: String,

    /// Disable outbound dialling (single-peer mode for CC-2Jb scenarios).
    #[arg(long, env = "CC_P2P_DISABLE_DIAL", default_value_t = false)]
    disable_dial: bool,

    /// Network config.yaml (fork schedule / SECONDS_PER_SLOT).
    #[arg(long, env = "CC_P2P_NETWORK_CONFIG")]
    config_yaml: Option<PathBuf>,

    /// manifest.json with genesis_validators_root.
    #[arg(long, env = "CC_P2P_MANIFEST")]
    manifest: Option<PathBuf>,

    /// Metrics listen `host:port` for publisher/peer mode.
    #[arg(long, env = "CC_P2P_METRICS_ADDR", default_value = "0.0.0.0:9102")]
    metrics_addr: String,

    /// gRPC listen (unused in slim publisher path; reserved).
    #[arg(long, env = "CC_P2P_GRPC_ADDR", default_value = "0.0.0.0:9002")]
    grpc_addr: String,

    /// Wall-clock genesis unix seconds (default: now).
    #[arg(long, env = "CC_P2P_GENESIS_TIME")]
    genesis_time: Option<u64>,

    /// Override seconds-per-slot.
    #[arg(long, env = "CC_P2P_SECONDS_PER_SLOT")]
    seconds_per_slot: Option<u64>,

    /// First slot to publish.
    #[arg(long)]
    start_slot: Option<u64>,

    /// Stop after N published slots.
    #[arg(long)]
    max_slots: Option<u64>,
}

/// Per-service config: shared [`ServiceConfig`] plus p2p runtime fields.
#[derive(Debug, Deserialize)]
struct P2pConfig {
    #[serde(flatten)]
    service: ServiceConfig,
    /// Path to the 32-byte secp256k1 node key.
    #[serde(default = "default_node_key_path")]
    node_key_path: PathBuf,
    /// libp2p listen multiaddr.
    #[serde(default = "default_listen_multiaddr")]
    listen_multiaddr: String,
    /// Slot clock (config-sourced until first `ChainView`).
    #[serde(default)]
    clock: ClockFileConfig,
    /// Peer manager knobs (CC-20c).
    #[serde(default)]
    peer_manager: PeerManagerFileConfig,
    /// Discovery / discv5 (CC-21c).
    #[serde(default)]
    discovery: DiscoveryFileConfig,
    /// Optional consensus-specs network YAML for `ForkContext` / ENR `eth2`.
    #[serde(default)]
    network_config: Option<PathBuf>,
    /// Optional genesis validators root hex (`0x` + 64 hex chars).
    #[serde(default)]
    genesis_validators_root: Option<String>,
    /// When false, do not spawn the discovery task.
    #[serde(default = "default_enable_discovery")]
    enable_discovery: bool,
    /// CC-23b: reject peers with `cgc < CUSTODY_REQUIREMENT` (default false = accept).
    #[serde(default)]
    reject_low_cgc_peers: bool,
    /// CC-48 §5.5: seconds to hold the last advertised window after
    /// `WatchServeWindow` disconnect before collapsing to the cache floor.
    #[serde(default = "default_window_stale_grace_secs")]
    window_stale_grace_secs: u64,
    /// CC-49 /7 — when true (default), Status advertises storage's branch-1
    /// block floor. When false, p2p forces a branch-2-style
    /// `max(block_floor, column_floor)` advertisement while the store still
    /// holds the full window. Default **true** while OQ-1 is NOT_RUN
    /// (ship-as-designed-pending-OQ-1; see `docs/phase-4-soak.md` § OQ-1).
    #[serde(default = "default_advertise_block_floor")]
    advertise_block_floor: bool,
}

fn default_window_stale_grace_secs() -> u64 {
    60
}

fn default_advertise_block_floor() -> bool {
    // OQ-1 NOT_RUN → ship as designed (pending foreign-peer confirmation).
    true
}

fn default_enable_discovery() -> bool {
    true
}

/// `[discovery]` section — maps onto [`cc_p2p::discovery::DiscoveryConfig`].
#[derive(Debug, Deserialize)]
struct DiscoveryFileConfig {
    /// UDP listen IP (default 0.0.0.0).
    #[serde(default = "default_discovery_ip")]
    listen_ip: String,
    /// UDP listen port (default matches TCP listen).
    #[serde(default = "default_discovery_udp")]
    listen_udp: u16,
    /// Bootnode ENR list (`enr:…` strings).
    #[serde(default)]
    boot_nodes: Vec<String>,
    /// Optional bootnodes file (self-devnet: `devnet/out/bootnodes.txt`).
    #[serde(default)]
    boot_nodes_file: Option<PathBuf>,
    /// Min peers per subnet before targeted queries.
    #[serde(default = "default_min_peers_per_subnet")]
    min_peers_per_subnet: usize,
}

impl Default for DiscoveryFileConfig {
    fn default() -> Self {
        Self {
            listen_ip: default_discovery_ip(),
            listen_udp: default_discovery_udp(),
            boot_nodes: Vec::new(),
            boot_nodes_file: None,
            min_peers_per_subnet: default_min_peers_per_subnet(),
        }
    }
}

fn default_discovery_ip() -> String {
    "0.0.0.0".to_owned()
}
fn default_discovery_udp() -> u16 {
    9000
}
fn default_min_peers_per_subnet() -> usize {
    cc_p2p::discovery::DEFAULT_MIN_PEERS_PER_SUBNET
}

/// `[peer_manager]` section — maps onto [`cc_p2p::peer_manager::PeerManagerConfig`].
#[derive(Debug, Deserialize)]
struct PeerManagerFileConfig {
    #[serde(default = "default_target_peers")]
    target_peers: usize,
    #[serde(default = "default_max_peers")]
    max_peers: usize,
    #[serde(default = "default_max_inbound")]
    max_inbound: usize,
    #[serde(default = "default_max_outbound")]
    max_outbound: usize,
    #[serde(default = "default_max_concurrent_dials")]
    max_concurrent_dials: usize,
    /// Static dial targets: `{ peer_id = "...", multiaddr = "/ip4/…/tcp/…" }`.
    #[serde(default)]
    static_peers: Vec<StaticPeerFile>,
}

#[derive(Debug, Deserialize)]
struct StaticPeerFile {
    peer_id: String,
    multiaddr: String,
}

impl Default for PeerManagerFileConfig {
    fn default() -> Self {
        Self {
            target_peers: default_target_peers(),
            max_peers: default_max_peers(),
            max_inbound: default_max_inbound(),
            max_outbound: default_max_outbound(),
            max_concurrent_dials: default_max_concurrent_dials(),
            static_peers: Vec::new(),
        }
    }
}

fn default_target_peers() -> usize {
    cc_p2p::peer_manager::DEFAULT_TARGET_PEERS
}
fn default_max_peers() -> usize {
    cc_p2p::peer_manager::DEFAULT_MAX_PEERS
}
fn default_max_inbound() -> usize {
    cc_p2p::peer_manager::DEFAULT_MAX_INBOUND
}
fn default_max_outbound() -> usize {
    cc_p2p::peer_manager::DEFAULT_MAX_OUTBOUND
}
fn default_max_concurrent_dials() -> usize {
    cc_p2p::peer_manager::DEFAULT_MAX_CONCURRENT_DIALS
}

#[derive(Debug, Deserialize)]
struct ClockFileConfig {
    #[serde(default)]
    genesis_time: u64,
    #[serde(default = "default_seconds_per_slot")]
    seconds_per_slot: u64,
    #[serde(default = "default_slots_per_epoch")]
    slots_per_epoch: u64,
    /// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` in milliseconds (config, never inlined in clock logic).
    #[serde(default = "default_disparity_ms")]
    maximum_gossip_clock_disparity_ms: u64,
    #[serde(default)]
    slot_clock_offset_seconds: i64,
}

impl Default for ClockFileConfig {
    fn default() -> Self {
        Self {
            genesis_time: 0,
            seconds_per_slot: default_seconds_per_slot(),
            slots_per_epoch: default_slots_per_epoch(),
            maximum_gossip_clock_disparity_ms: default_disparity_ms(),
            slot_clock_offset_seconds: 0,
        }
    }
}

fn default_node_key_path() -> PathBuf {
    PathBuf::from(DEFAULT_NODE_KEY_PATH)
}

fn default_listen_multiaddr() -> String {
    DEFAULT_LISTEN_MULTIADDR.to_owned()
}

fn default_seconds_per_slot() -> u64 {
    12
}

fn default_slots_per_epoch() -> u64 {
    32
}

fn default_disparity_ms() -> u64 {
    // Spec default MAXIMUM_GOSSIP_CLOCK_DISPARITY; config is the sole source for clock.rs.
    500
}

impl PeerManagerFileConfig {
    fn to_runtime(&self) -> Result<cc_p2p::peer_manager::PeerManagerConfig, RuntimeError> {
        use cc_libp2p::PeerId;
        use cc_p2p::peer_manager::{PeerManagerConfig, StaticPeer};
        use std::str::FromStr;

        let mut static_peers = Vec::with_capacity(self.static_peers.len());
        for sp in &self.static_peers {
            let peer_id = PeerId::from_str(&sp.peer_id).map_err(|e| {
                RuntimeError::ListenAddr(format!("peer_manager.static_peers peer_id: {e}"))
            })?;
            let addr = sp.multiaddr.parse().map_err(|e| {
                RuntimeError::ListenAddr(format!(
                    "peer_manager.static_peers multiaddr {}: {e}",
                    sp.multiaddr
                ))
            })?;
            static_peers.push(StaticPeer { peer_id, addr });
        }
        Ok(PeerManagerConfig {
            target_peers: self.target_peers,
            max_peers: self.max_peers,
            max_inbound: self.max_inbound,
            max_outbound: self.max_outbound,
            max_concurrent_dials: self.max_concurrent_dials,
            static_peers,
            tick_interval: cc_p2p::peer_manager::DEFAULT_TICK_INTERVAL,
        })
    }
}

impl P2pConfig {
    fn runtime_config(&self) -> Result<RuntimeConfig, RuntimeError> {
        let listen_multiaddr = self
            .listen_multiaddr
            .parse()
            .map_err(|e| RuntimeError::ListenAddr(format!("{}: {e}", self.listen_multiaddr)))?;
        let peer_manager = self.peer_manager.to_runtime()?;
        let listen_ip: std::net::Ipv4Addr = self
            .discovery
            .listen_ip
            .parse()
            .map_err(|e| RuntimeError::ListenAddr(format!("discovery.listen_ip: {e}")))?;
        let discovery = cc_p2p::discovery::DiscoveryConfig {
            listen_ip,
            listen_udp: self.discovery.listen_udp,
            // TCP port is aligned with the libp2p listen multiaddr in `serve`.
            tcp_port: self.discovery.listen_udp,
            boot_nodes: self.discovery.boot_nodes.clone(),
            boot_nodes_file: self.discovery.boot_nodes_file.clone(),
            target_peers: peer_manager.target_peers,
            min_peers_per_subnet: self.discovery.min_peers_per_subnet,
            // Phase 2: empty until CC-2C sets the backbone bitmask.
            interested_attnets: 0,
            enr_strategy: cc_p2p::discovery::EnrSeqStrategy::EnrInsert,
        };
        let genesis_validators_root = match &self.genesis_validators_root {
            Some(hex) => {
                let bytes = cc_types::parse_hex_bytes::<32>(hex).map_err(|e| {
                    RuntimeError::ListenAddr(format!("genesis_validators_root: {e}"))
                })?;
                Some(bytes)
            }
            None => None,
        };
        let chain_uri = self
            .service
            .peers
            .get("chain")
            .map(ToString::to_string)
            .unwrap_or_default();
        // http::Uri Display yields the authority form; empty peers → disable.
        let enable_chain_stream = !chain_uri.is_empty();
        let storage_uri = self
            .service
            .peers
            .get("storage")
            .map(ToString::to_string)
            .unwrap_or_default();
        let enable_storage_client = !storage_uri.is_empty();
        Ok(RuntimeConfig {
            node_key_path: self.node_key_path.clone(),
            listen_multiaddr,
            clock: ClockConfig {
                genesis_time: self.clock.genesis_time,
                seconds_per_slot: self.clock.seconds_per_slot,
                slots_per_epoch: self.clock.slots_per_epoch,
                maximum_gossip_clock_disparity: Duration::from_millis(
                    self.clock.maximum_gossip_clock_disparity_ms,
                ),
                slot_clock_offset_seconds: self.clock.slot_clock_offset_seconds,
            },
            peer_manager,
            discovery,
            network_config_path: self.network_config.clone(),
            genesis_validators_root,
            enable_discovery: self.enable_discovery,
            chain_uri,
            enable_chain_stream,
            storage_uri,
            enable_storage_client,
            window_stale_grace: Duration::from_secs(self.window_stale_grace_secs),
            advertise_block_floor: self.advertise_block_floor,
            heartbeat_interval: Duration::from_secs(1),
            test_swarm_panic: false,
            reject_low_cgc_peers: self.reject_low_cgc_peers,
        })
    }

    fn service_spec(&self) -> cc_bootstrap::ServiceSpec {
        service_spec(
            self.service.grpc_addr,
            self.service.metrics_addr,
            self.service
                .peers
                .iter()
                .map(|(name, uri)| PeerSpec {
                    name: name.clone(),
                    uri: uri.clone(),
                })
                .collect(),
            vec![GET_INFO_METHOD.to_owned(), SET_CGC_METHOD.to_owned()],
        )
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // ── bootnode emission (up.sh) ───────────────────────────────────────────
    if let Some(out) = cli.emit_bootnodes {
        emit_bootnodes(&out, &default_bootnode_specs())?;
        return Ok(());
    }

    let fault = FaultMode::parse(&cli.fault_mode)?;
    // Fail fast on inert kinds even before loading config.
    if cli.publish_fixture.is_some() || cli.devnet_peer {
        fault.ensure_implemented()?;
    }

    // ── publisher / peer mesh (CC-2Jd) ──────────────────────────────────────
    if cli.publish_fixture.is_some() || cli.devnet_peer {
        return run_devnet_mode(cli, fault).await;
    }

    // ── CC-20b path: identity before bind, swarm fatal, metrics ─────────────
    // Fail before any bind (CC-09/2): load config, then identity, then telemetry.
    let cfg = cc_config::load::<P2pConfig>(SERVICE)?;

    // CC-20/2 structural load order: identity **before** anything else that
    // could bind or compute custody. Refuse broad permissions here so we never
    // open ports with an unusable key.
    let identity = identity::load_or_create(&cfg.node_key_path)?;

    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;

    // CC-29a: register §12 families + libp2p-metrics sub-registry between init and serve.
    let p2p_metrics = P2pMetrics::register(&mut bs.registry);

    let runtime_cfg = cfg.runtime_config()?;
    // CC-38b: EngineStream server — minimal attach (sampling tracker + inject
    // pipeline + subscription set). AuthMode stays Unauthenticated (never flip
    // without real mutual auth — KZG skip footgun). Inclusion always verified.
    // Publish is NoopPublisher until gRPC co-owns swarm publish_tx.
    let engine_stream = build_minimal_engine_stream(
        identity.node_id(),
        CUSTODY_REQUIREMENT,
        Some(p2p_metrics.clone()),
    );
    let grpc = P2pGrpcService::new().with_engine_stream(engine_stream);
    // CC-21d: GetInfo + SetCustodyGroupCount (hook unattached in Phase 2).
    // CC-38b: EngineStream attached above.
    let routes = Routes::default().add_service(P2pServiceServer::new(grpc));

    match run_process(
        bs,
        cfg.service_spec(),
        routes,
        runtime_cfg,
        p2p_metrics,
        SignalTrigger::UnixSignals,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(RuntimeError::SwarmPanic { task, payload }) => {
            // ADR P2-13: swarm is process-fatal — non-zero exit for compose restart.
            tracing::error!(task, payload = %payload, "exiting non-zero after swarm panic");
            std::process::exit(1);
        }
        Err(e) => Err(e.into()),
    }
}

async fn run_devnet_mode(cli: Cli, fault: FaultMode) -> Result<()> {
    // Minimal telemetry without full ServiceConfig file (compose injects env).
    let env = EnvTelemetry::from_env();
    let telemetry = TelemetrySettings {
        log_format: env.log_format,
        log_filter: env.log_filter,
    };
    let mut bs = cc_bootstrap::init(SERVICE, telemetry)?;
    let metrics = P2pMetrics::register(&mut bs.registry);
    // Take ownership of the registry for the metrics server.
    let registry = Arc::new(std::mem::take(&mut bs.registry));

    let role = if cli.publish_fixture.is_some() {
        DevnetRole::Publisher
    } else {
        DevnetRole::Peer
    };

    let fixture_chain = cli
        .publish_fixture
        .clone()
        .or_else(|| {
            cli.config_yaml
                .as_ref()
                .and_then(|p| p.parent().map(|dir| dir.join("chain")))
        })
        .unwrap_or_else(|| PathBuf::from("devnet/out/chain"));

    let config_yaml = cli
        .config_yaml
        .clone()
        .unwrap_or_else(|| PathBuf::from("devnet/out/config.yaml"));
    let manifest = cli
        .manifest
        .clone()
        .unwrap_or_else(|| PathBuf::from("devnet/out/manifest.json"));
    let node_key = cli
        .node_key
        .clone()
        .context("--node-key / CC_P2P_NODE_KEY_PATH required in devnet mode")?;

    let mut static_peers = Vec::new();
    if let Some(file) = &cli.static_peers_file {
        static_peers.extend(read_multiaddrs_file(file)?);
    }
    if !cli.static_peers.trim().is_empty() {
        for part in cli.static_peers.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            static_peers.push(
                part.parse()
                    .map_err(|e| anyhow::anyhow!("static peer multiaddr {part:?}: {e}"))?,
            );
        }
    }

    // Peer mode: do not dial self; publisher may dial nothing by default.
    let disable_dial =
        cli.disable_dial || (role == DevnetRole::Publisher && static_peers.is_empty());

    let fault_flag_path = match (&fault, cli.fault_flag_path) {
        (FaultMode::WithholdColumn { .. }, None) => Some(PathBuf::from(DEFAULT_RELEASE_FLAG_PATH)),
        (_, path) => path,
    };

    let cfg = DevnetRuntimeConfig {
        role,
        fixture_chain,
        config_yaml,
        manifest_json: manifest,
        node_key_path: node_key,
        listen: parse_listen(&cli.listen)?,
        static_peers,
        disable_dial,
        fault_mode: fault,
        fault_flag_path,
        withhold_target_role: cli.withhold_target_role,
        withhold_target_cgc: cli.withhold_target_cgc,
        metrics_addr: parse_socket_addr(&cli.metrics_addr)?,
        grpc_addr: parse_socket_addr(&cli.grpc_addr)?,
        genesis_time_override: cli.genesis_time,
        seconds_per_slot_override: cli.seconds_per_slot,
        start_slot: cli.start_slot,
        max_slots: cli.max_slots,
    };

    if cfg.role == DevnetRole::Publisher && !cfg.config_yaml.exists() {
        bail!(
            "missing {}; generate with cc-devnet-gen first",
            cfg.config_yaml.display()
        );
    }

    run_devnet(cfg, metrics, registry).await
}

// Silence unused BootnodeSpec in some builds.
#[allow(dead_code)]
fn _specs() -> Vec<BootnodeSpec> {
    default_bootnode_specs()
}
