//! `beacon-core` process — opens redb, then starts chain-core + storage-core.

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    cc_beacon_core::run().await
}
