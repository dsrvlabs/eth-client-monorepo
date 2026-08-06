//! Spec helpers used by the transition (Architecture §5.1 `helpers/`).
//!
//! Bodies grow with CC-12b–d / CC-13; the scaffolding needed for
//! `process_slots` / `process_block_header` lands here.

pub mod accessors;
pub mod misc;
pub mod mutators;
pub mod predicates;

pub use accessors::{get_beacon_proposer_index, get_current_epoch};
pub use misc::compute_epoch_at_slot;
