//! discv5 discovery and ENR ownership (CC-21).
//!
//! CC-21a lands only the A-P2-4 probe surface and the `EnrManager::apply`
//! batching skeleton. Field writers, the discovery task, and dial pipeline
//! are CC-21c.

pub mod enr;

pub use enr::{EnrApplyError, EnrFieldChange, EnrManager, EnrSeqStrategy};
