//! SHA-256 helpers used by domain computation and general consensus hashing.
//!
//! Thin wrappers over `ethereum_hashing` so callers do not take a direct
//! dependency on the hash crate.

/// 32-byte SHA-256 digest of `input`.
#[inline]
pub fn hash_fixed(input: &[u8]) -> [u8; 32] {
    ethereum_hashing::hash_fixed(input)
}

/// SHA-256 of the concatenation of two 32-byte (or arbitrary) slices.
#[inline]
pub fn hash32_concat(h1: &[u8], h2: &[u8]) -> [u8; 32] {
    ethereum_hashing::hash32_concat(h1, h2)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn hash_fixed_empty_matches_sha256() {
        let got = hash_fixed(b"");
        // SHA-256("")
        let expect = [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55,
        ];
        assert_eq!(got, expect);
    }

    #[test]
    fn hash32_concat_differs_from_halves() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let c = hash32_concat(&a, &b);
        assert_ne!(c, a);
        assert_ne!(c, b);
        assert_eq!(c, hash_fixed(&[a.as_slice(), b.as_slice()].concat()));
    }
}
