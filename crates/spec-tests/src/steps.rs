//! `steps.yaml` parsing helpers.
//!
//! Fork-choice (and similar) suites supply an ordered list of steps as YAML.
//! The harness parses YAML only — step-kind dispatch lives in the owning
//! runner (Architecture §10.2).

use std::path::Path;

use serde::de::DeserializeOwned;

use crate::error::Error;

/// Read and deserialize `steps.yaml` at `case_dir/steps.yaml`.
pub fn load_steps<T: DeserializeOwned>(case_dir: &Path) -> Result<T, Error> {
    let path = case_dir.join("steps.yaml");
    let text = std::fs::read_to_string(&path).map_err(|source| Error::Io {
        path: path.clone(),
        source,
    })?;
    serde_yaml::from_str(&text).map_err(|e| Error::Yaml {
        path,
        detail: e.to_string(),
    })
}

/// Load steps as a raw YAML sequence of mappings (common fork-choice shape).
pub fn load_steps_values(case_dir: &Path) -> Result<Vec<serde_yaml::Value>, Error> {
    load_steps(case_dir)
}
