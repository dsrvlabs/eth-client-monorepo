//! `storage` service — thin shim over [`cc_storage_core`] (S2-B-03).
//!
//! `services/storage` stays a workspace member so the previous topology
//! can still be run for A/B (`[ARCH]` §9.1).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cc_storage_core::run().await
}
