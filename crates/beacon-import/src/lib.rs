//! S2-A-14: proto-free `import → durable` surface.
//!
//! Production persist is [`cc_seam::ArchiveWrite::ingest_block`] (the P0
//! writer mailbox). This crate only exists so the assertion binary can
//! depend on `on_block` + that persist without `tonic` / `cc-proto`.

#![allow(missing_docs)]
