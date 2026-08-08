//! Library surface for the `cc-storage` package.
//!
//! Production entry remains the `cc-storage` binary (`src/main.rs`). This crate
//! root exists so integration tests under `tests/` can link the package (Cargo
//! requires a lib target for `[[test]]` harnesses). Service modules stay
//! private to the binary; integration tests exercise workspace deps
//! (`cc-store`, `cc-types`) under this package's harness.
