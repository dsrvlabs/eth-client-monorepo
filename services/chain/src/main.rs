//! `chain` service — thin shim over [`cc_chain_core`] (S2-A-03).
//!
//! Fail-before-bind (JWT + `[el_forks]` + KZG), DirectEngine wiring (S1-A-06),
//! and the S1-A-16 liveness sampler live in [`cc_chain::run`].
//! `services/chain` stays a workspace member so the previous topology can
//! still run for A/B (`[ARCH]` §9.1). Restore is not deleted (S2-J-02).

// Multi-thread runtime: EL calls from `chain-core` drive the host runtime.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    cc_chain::run().await
}
