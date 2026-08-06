//! Trusted-setup loading for both KZG backends (Architecture §4.3, CC-11/5).
//!
//! One committed [`TRUSTED_SETUP_JSON`] file is the source of truth. Backend A
//! (`c-kzg`) loads via `KzgSettings::load_trusted_setup` after hex-decoding the
//! G1/G2 points. Backend B (`rust_eth_kzg`, CC-11c) will parse the same JSON
//! via `TrustedSetup::from_json`.

use super::KzgError;
use serde::Deserialize;

/// Embedded mainnet trusted setup (Ethereum consensus-specs `trusted_setup_4096.json`).
pub const TRUSTED_SETUP_JSON: &str = include_str!("../../trusted_setup.json");

/// Default `precompute` for `c-kzg` setup load (0 = no table; benchmark axis later).
pub const DEFAULT_PRECOMPUTE: u64 = 0;

/// Number of G1 points in the mainnet KZG trusted setup.
pub const NUM_G1_POINTS: usize = 4096;

/// Number of G2 points in the mainnet KZG trusted setup (multiproof up to 64).
pub const NUM_G2_POINTS: usize = 65;

/// Compressed G1 point size in bytes.
const BYTES_PER_G1: usize = 48;
/// Compressed G2 point size in bytes.
const BYTES_PER_G2: usize = 96;

#[derive(Debug, Deserialize)]
struct TrustedSetupJson {
    g1_monomial: Vec<String>,
    g1_lagrange: Vec<String>,
    g2_monomial: Vec<String>,
}

/// Decoded flat point bytes from the committed JSON.
#[derive(Debug, Clone)]
pub struct TrustedSetupBytes {
    /// `NUM_G1_POINTS * 48` G1 monomial bytes.
    pub g1_monomial: Vec<u8>,
    /// `NUM_G1_POINTS * 48` G1 Lagrange bytes.
    pub g1_lagrange: Vec<u8>,
    /// `NUM_G2_POINTS * 96` G2 monomial bytes.
    pub g2_monomial: Vec<u8>,
}

impl TrustedSetupBytes {
    /// Parse and hex-decode the committed [`TRUSTED_SETUP_JSON`].
    pub fn from_committed() -> Result<Self, KzgError> {
        Self::from_json(TRUSTED_SETUP_JSON)
    }

    /// Parse and hex-decode a trusted-setup JSON string (Ethereum format).
    pub fn from_json(json: &str) -> Result<Self, KzgError> {
        let parsed: TrustedSetupJson = serde_json::from_str(json)
            .map_err(|e| KzgError::TrustedSetup(format!("JSON parse: {e}")))?;

        if parsed.g1_monomial.len() != NUM_G1_POINTS {
            return Err(KzgError::TrustedSetup(format!(
                "g1_monomial: expected {NUM_G1_POINTS} points, got {}",
                parsed.g1_monomial.len()
            )));
        }
        if parsed.g1_lagrange.len() != NUM_G1_POINTS {
            return Err(KzgError::TrustedSetup(format!(
                "g1_lagrange: expected {NUM_G1_POINTS} points, got {}",
                parsed.g1_lagrange.len()
            )));
        }
        if parsed.g2_monomial.len() != NUM_G2_POINTS {
            return Err(KzgError::TrustedSetup(format!(
                "g2_monomial: expected {NUM_G2_POINTS} points, got {}",
                parsed.g2_monomial.len()
            )));
        }

        Ok(Self {
            g1_monomial: decode_point_list(&parsed.g1_monomial, BYTES_PER_G1)?,
            g1_lagrange: decode_point_list(&parsed.g1_lagrange, BYTES_PER_G1)?,
            g2_monomial: decode_point_list(&parsed.g2_monomial, BYTES_PER_G2)?,
        })
    }
}

fn decode_point_list(points: &[String], expected_len: usize) -> Result<Vec<u8>, KzgError> {
    let mut out = Vec::with_capacity(points.len() * expected_len);
    for (i, hex_str) in points.iter().enumerate() {
        let bytes = decode_hex(hex_str).map_err(|e| {
            KzgError::TrustedSetup(format!("point {i}: {e}"))
        })?;
        if bytes.len() != expected_len {
            return Err(KzgError::TrustedSetup(format!(
                "point {i}: expected {expected_len} bytes, got {}",
                bytes.len()
            )));
        }
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

/// Decode a `0x`-prefixed or bare hex string.
fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if !hex.len().is_multiple_of(2) {
        return Err(format!("odd hex length {}", hex.len()));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex digit {}", b as char)),
    }
}

#[cfg(feature = "kzg-c-kzg")]
mod c_kzg_load {
    use super::{TrustedSetupBytes, DEFAULT_PRECOMPUTE};
    use crate::kzg::KzgError;
    use c_kzg::KzgSettings;

    /// Load `c-kzg` settings from the committed trusted setup.
    pub fn load_c_kzg_settings(precompute: u64) -> Result<KzgSettings, KzgError> {
        let bytes = TrustedSetupBytes::from_committed()?;
        load_c_kzg_settings_from_bytes(&bytes, precompute)
    }

    /// Load `c-kzg` settings from already-decoded setup bytes.
    pub fn load_c_kzg_settings_from_bytes(
        bytes: &TrustedSetupBytes,
        precompute: u64,
    ) -> Result<KzgSettings, KzgError> {
        KzgSettings::load_trusted_setup(
            &bytes.g1_monomial,
            &bytes.g1_lagrange,
            &bytes.g2_monomial,
            precompute,
        )
        .map_err(|e| KzgError::TrustedSetup(e.to_string()))
    }

    /// Load with [`DEFAULT_PRECOMPUTE`].
    pub fn load_c_kzg_settings_default() -> Result<KzgSettings, KzgError> {
        load_c_kzg_settings(DEFAULT_PRECOMPUTE)
    }
}

#[cfg(feature = "kzg-c-kzg")]
pub use c_kzg_load::{
    load_c_kzg_settings, load_c_kzg_settings_default, load_c_kzg_settings_from_bytes,
};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn committed_json_parses_and_has_expected_point_counts() {
        let bytes = TrustedSetupBytes::from_committed().expect("parse committed setup");
        assert_eq!(bytes.g1_monomial.len(), NUM_G1_POINTS * BYTES_PER_G1);
        assert_eq!(bytes.g1_lagrange.len(), NUM_G1_POINTS * BYTES_PER_G1);
        assert_eq!(bytes.g2_monomial.len(), NUM_G2_POINTS * BYTES_PER_G2);
    }

    #[test]
    fn decode_hex_accepts_0x_prefix() {
        assert_eq!(decode_hex("0x0a0b").unwrap(), vec![0x0a, 0x0b]);
        assert_eq!(decode_hex("0A0B").unwrap(), vec![0x0a, 0x0b]);
    }
}
