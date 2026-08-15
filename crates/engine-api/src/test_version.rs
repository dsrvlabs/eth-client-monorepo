//! Test-only schedule type so moved `config.rs` keeps `crate::version`.

use crate::methods::names;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElForkSchedule {
    pub osaka_time: u64,
    pub bpo1_time: Option<u64>,
    pub bpo2_time: Option<u64>,
    pub amsterdam_time: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionError {
    UnsupportedFork { timestamp: u64 },
}

pub fn method_for(
    _timestamp: u64,
    _cfg: &ElForkSchedule,
) -> Result<&'static str, VersionError> {
    Ok(names::NEW_PAYLOAD_V4)
}
