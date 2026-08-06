//! Fulu networking helpers (Architecture §3.6 / CC-1B).
//!
//! Pure DAS custody-group computation lives here — no BLS, no KZG, no fork
//! choice. Phase 2's p2p stack is the consumer.

pub mod custody;

pub use custody::{
    ColumnIndex, CustodyIndex, SubnetId, compute_columns_for_custody_group,
    compute_subnet_for_data_column_sidecar, get_custody_groups, sampling_size,
};
