//! Library surface for the `cc-storage` package.
//!
//! S2-B-03: production boot lives in `cc-storage-core`. This crate stays a
//! workspace member so the previous topology remains runnable (`[ARCH]` §9.1).
//! Integration tests under `tests/` link this package.

use cc_storage_core as _;
