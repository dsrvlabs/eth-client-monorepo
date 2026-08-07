//! PeerDAS custody / sampling surface — Architecture §8 / CC-24.
//!
//! | Module | Issue | Role |
//! |--------|-------|------|
//! | [`custody`] | CC-24a | sampled + custodied sets, column-subnet subscription, custody-compatible peers |
//!
//! Verification pool (**CC-24b**), sampling tracker (**CC-24c**), and DA seam
//! substitution (**CC-24d**) land in sibling modules later. No req/resp code
//! lives here.

pub mod custody;

pub use custody::{
    count_custody_compatible_peers, is_peer_custody_compatible, column_subnets_for_groups,
    CustodiedGroups, CustodyManager, SampledGroups,
};
