# KZG backend notes / benchmark log

## rust_eth_kzg proof-verification-failure variant (CC-11c)

// KZG-VARIANT: rust_eth_kzg 0.10.0 cell-path proof failure is
// `Error::Verifier(VerifierError::FK20(_))` (FK20 wraps
// `kzg_multi_open::VerifierError::InvalidProof`). The 4844 path uses
// `Error::EIP4844(eip4844::Error::Verifier(eip4844::VerifierError::InvalidProof))`.
// Public detector: `Error::is_proof_invalid()` (nested `VerifierError` is not
// re-exported at the crate root). Adapter maps `is_proof_invalid() == true` →
// `Ok(false)`; every other `Err` → `KzgError` (never `Ok(false)`).

Benchmark measurements and `UsePrecomp` default selection land in **CC-11d**.
