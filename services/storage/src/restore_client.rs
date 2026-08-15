//! Storage → chain `RestoreFromStore` client (CC-45b / Architecture §1.6 / §3.5).
//!
//! Pushes the snapshot + replay set over the existing storage→chain edge
//! (ADR P4-07). Chain cannot pull without a health-DAG cycle.

use std::time::Duration;

use cc_proto::chain::chain_service_client::ChainServiceClient;
use cc_proto::chain::{
    RestoreBlock, RestoreChunk, RestoreDaStatus, RestoreEmpty, RestoreFooter, RestoreHeader,
    RestoreResponse, restore_chunk::Body as RestoreBody,
};
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Status};
use tracing::{debug, info, warn};

/// Default connect timeout for a single restore dial attempt.
pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default overall budget for dial+push retries (matches chain restore grace).
pub(crate) const DEFAULT_PUSH_RETRY_BUDGET: Duration = Duration::from_secs(30);

/// Initial backoff between dial retries.
pub(crate) const DEFAULT_PUSH_BACKOFF_INITIAL: Duration = Duration::from_millis(200);

/// Cap on dial-retry backoff.
pub(crate) const DEFAULT_PUSH_BACKOFF_CAP: Duration = Duration::from_secs(2);

/// Materialised restore stream content (built by `resume`, sent by this client).
#[derive(Debug, Clone)]
pub(crate) struct RestoreStreamPlan {
    /// When true, only an EMPTY frame is sent.
    pub empty: bool,
    pub header: Option<RestoreHeader>,
    /// Snapshot BeaconState SSZ (chunked on send).
    pub state_ssz: Vec<u8>,
    pub blocks: Vec<RestoreBlock>,
    pub footer: Option<RestoreFooter>,
    /// Chunk size for state SSZ (default 1 MiB).
    pub state_chunk_bytes: usize,
}

impl Default for RestoreStreamPlan {
    fn default() -> Self {
        Self {
            empty: false,
            header: None,
            state_ssz: Vec::new(),
            blocks: Vec::new(),
            footer: None,
            state_chunk_bytes: 1024 * 1024,
        }
    }
}

impl RestoreStreamPlan {
    /// Build an EMPTY plan (first-ever start).
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            empty: true,
            ..Self::default()
        }
    }
}

/// Dial+push with exponential backoff until `budget` elapses or success.
///
/// Covers the race where storage opens before chain's accept loop is ready
/// (compose health flips as soon as AwaitingRestore marks aggregate SERVING,
/// but a late bind can still yield transient dial failures). EMPTY and full
/// plans both use this path.
pub(crate) async fn push_restore_with_retry(
    chain_uri: &str,
    plan: RestoreStreamPlan,
    connect_timeout: Duration,
    budget: Duration,
    backoff_initial: Duration,
    backoff_cap: Duration,
) -> Result<RestoreResponse, RestoreClientError> {
    let deadline = tokio::time::Instant::now() + budget;
    let mut backoff = backoff_initial.max(Duration::from_millis(50));
    let mut attempt: u32 = 0;
    loop {
        attempt = attempt.saturating_add(1);
        // Plan is Clone; each attempt needs a fresh stream.
        match push_restore_once(chain_uri, plan.clone(), connect_timeout).await {
            Ok(resp) => {
                if attempt > 1 {
                    info!(attempt, "RestoreFromStore succeeded after retry");
                }
                return Ok(resp);
            }
            Err(e) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    warn!(
                        attempt,
                        error = %e,
                        "RestoreFromStore retry budget exhausted"
                    );
                    return Err(e);
                }
                let sleep_for = backoff.min(remaining).min(backoff_cap);
                warn!(
                    attempt,
                    error = %e,
                    sleep_ms = sleep_for.as_millis() as u64,
                    "RestoreFromStore dial/push failed; retrying"
                );
                tokio::time::sleep(sleep_for).await;
                backoff = (backoff.saturating_mul(2)).min(backoff_cap);
            }
        }
    }
}

async fn push_restore_once(
    chain_uri: &str,
    plan: RestoreStreamPlan,
    connect_timeout: Duration,
) -> Result<RestoreResponse, RestoreClientError> {
    let channel = dial(chain_uri, connect_timeout).await?;
    let mut client = ChainServiceClient::new(channel);
    let outbound = plan_to_stream(plan);
    let response = client
        .restore_from_store(Request::new(outbound))
        .await
        .map_err(RestoreClientError::Rpc)?
        .into_inner();
    info!(
        head_slot = response.head_slot,
        matched_expected = response.matched_expected,
        "RestoreFromStore response received"
    );
    Ok(response)
}

async fn dial(uri: &str, connect_timeout: Duration) -> Result<Channel, RestoreClientError> {
    let endpoint = Channel::from_shared(uri.to_owned())
        .map_err(|e| RestoreClientError::Dial(format!("invalid uri {uri}: {e}")))?
        .connect_timeout(connect_timeout)
        .timeout(Duration::from_secs(600)); // large state transfer
    endpoint
        .connect()
        .await
        .map_err(|e| RestoreClientError::Dial(format!("connect {uri}: {e}")))
}

/// Convert a plan into a tonic-compatible stream of chunks.
fn plan_to_stream(plan: RestoreStreamPlan) -> impl Stream<Item = RestoreChunk> + Send + 'static {
    let (tx, rx) = tokio::sync::mpsc::channel::<RestoreChunk>(8);
    tokio::spawn(async move {
        if plan.empty {
            let _ = tx
                .send(RestoreChunk {
                    body: Some(RestoreBody::Empty(RestoreEmpty {})),
                })
                .await;
            return;
        }
        if let Some(header) = plan.header
            && tx
                .send(RestoreChunk {
                    body: Some(RestoreBody::Header(header)),
                })
                .await
                .is_err()
        {
            return;
        }
        let chunk = plan.state_chunk_bytes.max(64 * 1024);
        for piece in plan.state_ssz.chunks(chunk) {
            if tx
                .send(RestoreChunk {
                    body: Some(RestoreBody::StateSszChunk(piece.to_vec())),
                })
                .await
                .is_err()
            {
                return;
            }
        }
        for b in plan.blocks {
            if tx
                .send(RestoreChunk {
                    body: Some(RestoreBody::Block(b)),
                })
                .await
                .is_err()
            {
                return;
            }
        }
        if let Some(footer) = plan.footer {
            let _ = tx
                .send(RestoreChunk {
                    body: Some(RestoreBody::Footer(footer)),
                })
                .await;
        }
        debug!("restore stream fully enqueued");
    });
    ReceiverStream::new(rx)
}

/// Map store `DaStatus` to the wire enum.
#[must_use]
pub(crate) fn wire_da_status(status: cc_store::DaStatus) -> i32 {
    match status {
        cc_store::DaStatus::Available => RestoreDaStatus::Available as i32,
        cc_store::DaStatus::Deferred => RestoreDaStatus::Deferred as i32,
    }
}

/// Client-side errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RestoreClientError {
    #[error("dial: {0}")]
    Dial(String),
    #[error("rpc: {0}")]
    Rpc(#[from] Status),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn empty_plan_is_empty() {
        let p = RestoreStreamPlan::empty();
        assert!(p.empty);
        assert!(p.header.is_none());
    }

    #[test]
    fn wire_da_status_both_directions() {
        assert_eq!(
            wire_da_status(cc_store::DaStatus::Available),
            RestoreDaStatus::Available as i32
        );
        assert_eq!(
            wire_da_status(cc_store::DaStatus::Deferred),
            RestoreDaStatus::Deferred as i32
        );
        // Never emits UNSPECIFIED (0).
        assert_ne!(wire_da_status(cc_store::DaStatus::Available), 0);
        assert_ne!(wire_da_status(cc_store::DaStatus::Deferred), 0);
    }

    #[tokio::test]
    async fn push_restore_with_retry_exhausts_budget_on_dead_port() {
        // Bound-but-closed port: dial fails; retry loop must return Err after budget.
        let start = std::time::Instant::now();
        let err = push_restore_with_retry(
            "http://127.0.0.1:1",
            RestoreStreamPlan::empty(),
            Duration::from_millis(50),
            Duration::from_millis(300),
            Duration::from_millis(50),
            Duration::from_millis(100),
        )
        .await
        .expect_err("dead port must fail");
        assert!(
            start.elapsed() >= Duration::from_millis(200),
            "should have retried for most of the budget, took {:?}",
            start.elapsed()
        );
        let _ = err;
    }
}
