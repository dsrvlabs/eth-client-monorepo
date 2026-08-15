//! Test-only JWT stand-in. Not `services/engine/src/jwt.rs`; not public.

use std::fmt;

#[derive(Clone)]
pub(crate) struct JwtSecret {
    bytes: [u8; 32],
}

impl fmt::Debug for JwtSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Jwt(<redacted>)")
    }
}

#[derive(Debug)]
pub(crate) struct JwtError(String);

impl fmt::Display for JwtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for JwtError {}

impl JwtSecret {
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    pub fn sign_iat(&self, _iat: u64) -> Result<String, JwtError> {
        Ok("stub.jwt.token".into())
    }

    pub fn sign_now(&self) -> Result<String, JwtError> {
        self.sign_iat(0)
    }

    #[must_use]
    pub fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(64);
        for b in self.bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xf) as usize] as char);
        }
        out
    }
}
