//! Library surface for the `cc-storage` package.
//!
//! Production entry remains the `cc-storage` binary (`src/main.rs`). This crate
//! root exists so integration tests under `tests/` can link the package (Cargo
//! requires a lib target for `[[test]]` harnesses). Service modules stay
//! private to the binary; integration tests exercise workspace deps
//! (`cc-store`, `cc-types`) under this package's harness.

// S2-B-01: writer.rs + serve.rs live in cc-storage-core (binary compiles via #[path]).
use cc_storage_core as _;
