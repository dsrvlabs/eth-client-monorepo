//! `meta.yaml` / `bls_setting` model.
//!
//! Spec definition (`tests/formats/README.md`):
//! - `0` — optional (default when key **or** `meta.yaml` is absent): free to
//!   run with BLS on or off; outcome does not depend on verification.
//! - `1` — BLS required.
//! - `2` — BLS ignored (outcome depends on BLS being off). Phase 1 architecture
//!   names this "must-fail" relative to `BlockSignatureStrategy` selection.
//!
//! [`Meta::default`] therefore uses [`BlsSetting::Optional`] (`0`).

use serde::Deserialize;

/// BLS verification mode declared by a vector case's `meta.yaml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum BlsSetting {
    /// `0` — optional / ignore (default when `meta.yaml` or the key is absent).
    #[default]
    Optional = 0,
    /// `1` — BLS verification required.
    Required = 1,
    /// `2` — BLS ignored / must-fail under verification (spec: "BLS ignored").
    MustFail = 2,
}

impl BlsSetting {
    /// Parse the integer form used in `meta.yaml`.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Optional),
            1 => Some(Self::Required),
            2 => Some(Self::MustFail),
            _ => None,
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Case metadata from `meta.yaml`.
///
/// Only fields the harness itself consumes are typed here. Additional keys in
/// the YAML are ignored so runners stay forward-compatible.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Meta {
    /// BLS verification mode. Defaults to [`BlsSetting::Optional`] (`0`) when
    /// the key is absent — matching the consensus-specs test format.
    #[serde(
        default = "default_bls_setting_u8",
        deserialize_with = "de_bls_setting"
    )]
    pub bls_setting: BlsSetting,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            bls_setting: BlsSetting::Optional,
        }
    }
}

fn default_bls_setting_u8() -> BlsSetting {
    BlsSetting::Optional
}

fn de_bls_setting<'de, D>(deserializer: D) -> Result<BlsSetting, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = u8::deserialize(deserializer)?;
    BlsSetting::from_u8(v).ok_or_else(|| {
        serde::de::Error::custom(format!("bls_setting must be 0, 1, or 2 (got {v})"))
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn default_is_optional_zero() {
        let m = Meta::default();
        assert_eq!(m.bls_setting, BlsSetting::Optional);
        assert_eq!(m.bls_setting.as_u8(), 0);
    }

    #[test]
    fn parses_required() {
        let m: Meta = serde_yaml::from_str("{bls_setting: 1}").expect("yaml");
        assert_eq!(m.bls_setting, BlsSetting::Required);
    }

    #[test]
    fn missing_key_defaults() {
        let m: Meta = serde_yaml::from_str("description: hi").expect("yaml");
        assert_eq!(m.bls_setting, BlsSetting::Optional);
    }
}
