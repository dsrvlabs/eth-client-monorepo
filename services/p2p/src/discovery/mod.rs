//! discv5 discovery and ENR ownership (CC-21).
//!
//! - **CC-21a**: A-P2-4 probe + `EnrManager::apply` batching skeleton
//! - **CC-21c**: field encoders (`eth2`/`nfd`/`attnets`/`syncnets`/`cgc`),
//!   predicates, discovery task, dial queue → peer manager
//! - **CC-21d**: runtime `cgc` hook (`set_custody_group_count`) — five §6.4 effects
//! - **CC-2C**: attestation-subnet backbone (`SubnetManager` single writer)
//! - **CC-2D**: `syncnets` half of [`SubnetManager`] + Phase 6
//!   `subscribe_sync_subnets` hook (empty-by-default)

pub mod cgc_hook;
pub mod dial_queue;
pub mod enr;
pub mod predicate;
pub mod subnet_manager;
pub mod task;

pub use cgc_hook::{
    column_family_total, column_family_total_is_half, set_custody_group_count, CgcEffectKind,
    CgcHookError, CgcHookOutcome, CgcHookTarget,
};
pub use dial_queue::{DIAL_QUEUE_BOUND, DialCandidate, DialQueue};
pub use enr::{
    ATTNETS_BIT_LEN, ENR_KEY_ATTNETS, ENR_KEY_CGC, ENR_KEY_ETH2, ENR_KEY_NFD, ENR_KEY_SYNCNETS,
    EnrApplyError, EnrFieldChange, EnrManager, EnrSeqStrategy, SYNCNETS_BIT_LEN, decode_attnets,
    decode_cgc, decode_eth2, decode_nfd, decode_syncnets, encode_attnets, encode_cgc, encode_eth2,
    encode_nfd, encode_syncnets, enr_custody_groups, enr_field_payload, fork_field_changes,
    parse_bootnode, parse_bootnodes, phase2_default_field_changes, read_attnets, read_cgc,
    read_eth2, read_nfd, read_syncnets,
};
pub use predicate::{
    attestation_subnet_predicate, column_predicate, digest_matches, enr_fork_digest,
    generic_peer_predicate, sync_subnet_predicate,
};
pub use subnet_manager::{
    attnets_bitvector, compute_subscribed_subnet, compute_subscribed_subnets, subscription_period,
    SubnetApplyOutcome, SubnetApplyTarget, SubnetEffectKind, SubnetManager, SubnetManagerError,
    SubnetError, SubnetManager, SyncSubnetEffect, SyncSubnetOutcome, SyncSubnetTarget,
    SYNC_SUBNET_COUNT,
};
pub use task::{
    DEFAULT_MIN_PEERS_PER_SUBNET, DiscoveredPeer, DiscoveryConfig, DiscoveryPeerView, DiscoveryTask,
    PRIORITY_BASE, QUERY_INTERVAL_AT_TARGET, QUERY_INTERVAL_BELOW_TARGET, SUBNET_QUERY_COOLDOWN,
    build_enr_manager, deficit_attnets, dial_priority_for_enr, load_bootnodes, multiaddr_from_enr,
    peer_enr_info_from_enr, peer_id_from_enr, run_discovery_task,
};
