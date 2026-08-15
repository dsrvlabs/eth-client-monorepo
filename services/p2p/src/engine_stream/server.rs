//! `EngineStream` gRPC server session (CC-38b).
//!
//! engine dials; sidecars go up (`EngineHello` / `InjectColumns`); subscription
//! set and column-branch fetch go down (`SubscriptionSet` / `FetchBlobsRequest`).

use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cc_proto::p2p::{EngineToP2p, P2pToEngine, engine_to_p2p, p2p_to_engine};
use discv5::enr::NodeId;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use super::inject::{ColumnPublisher, InjectPipeline, NoopPublisher, SamplingSink};
use super::subscription::{LocalSubscription, SubscriptionHandle, subscription_to_wire};
use crate::das::{CustodyManager, FixedDeadline, SamplingHandle, SamplingTracker};
use crate::gossip::seen::SeenSets;
use crate::metrics::P2pMetrics;

/// Outbound `P2pToEngine` queue bound (session-local).
const OUTBOUND_BOUND: usize = 64;

/// Shared deps for every `EngineStream` session.
#[derive(Clone)]
pub struct EngineStreamDeps {
    /// §5.5 inject pipeline.
    pub inject: Arc<InjectPipeline>,
    /// Subscription set producer (hello + cgc change).
    pub subscription: SubscriptionHandle,
}

impl EngineStreamDeps {
    /// Construct from inject + subscription handles.
    #[must_use]
    pub fn new(inject: Arc<InjectPipeline>, subscription: SubscriptionHandle) -> Self {
        Self {
            inject,
            subscription,
        }
    }
}

/// Minimal production attach for the host process (CC-38b).
///
/// Builds:
/// - [`CustodyManager`] → sampled column indices as subscription set
/// - [`SamplingTracker`] / [`SamplingHandle`] (metrics on tracker; no DA fan-out
///   until chain-stream co-owns `da_tx`)
/// - shared [`SeenSets`]
/// - [`InjectPipeline::production`] (real KZG + always-on inclusion, unauthenticated)
/// - [`NoopPublisher`] until gRPC task co-owns swarm `publish_tx`
///
/// Returns an [`EngineStreamService`] ready for
/// [`crate::service::P2pGrpcService::with_engine_stream`].
///
/// # AuthMode
///
/// Leaves [`super::AuthMode::Unauthenticated`] (default). Do **not** flip to
/// Authenticated without real mutual auth (see inject module footgun docs).
#[must_use]
pub fn build_minimal_engine_stream(
    node_id: NodeId,
    cgc: u64,
    metrics: Option<P2pMetrics>,
) -> EngineStreamService {
    let custody = CustodyManager::new(node_id, cgc);
    // Fulu: group index == column index; required set is the sampled groups.
    let required: BTreeSet<u64> = custody.sampled().iter().copied().collect();
    let sub = SubscriptionHandle::new(LocalSubscription::from_indices(
        required.iter().copied(),
        custody.cgc(),
    ));

    // Far deadline: host attach does not yet drive end-of-slot recovery from
    // this tracker instance (swarm path owns its own sampling wiring later).
    let far = Instant::now() + Duration::from_secs(365 * 24 * 3600);
    let tracker = SamplingTracker::new(
        required,
        Arc::new(FixedDeadline(far)),
        metrics,
        None, // DA emit co-owned with chain-stream in a later wiring step
    );
    let handle = SamplingHandle::new(tracker);
    let seen = Arc::new(Mutex::new(SeenSets::new()));
    // metrics: None on pipeline — tracker owns columns_received.
    let inject = Arc::new(InjectPipeline::production(
        seen,
        Arc::new(handle) as Arc<dyn SamplingSink>,
        Arc::new(NoopPublisher) as Arc<dyn ColumnPublisher>,
        sub.clone(),
        None,
    ));
    EngineStreamService::new(EngineStreamDeps::new(inject, sub))
}

impl std::fmt::Debug for EngineStreamDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStreamDeps").finish_non_exhaustive()
    }
}

/// gRPC-facing handle that implements the stream protocol against deps.
#[derive(Clone)]
pub struct EngineStreamService {
    deps: Arc<EngineStreamDeps>,
    /// Monotonic outbound seq per process (sessions may interleave).
    next_seq: Arc<AtomicU64>,
}

impl std::fmt::Debug for EngineStreamService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStreamService")
            .finish_non_exhaustive()
    }
}

impl EngineStreamService {
    /// Construct from shared deps.
    #[must_use]
    pub fn new(deps: EngineStreamDeps) -> Self {
        Self {
            deps: Arc::new(deps),
            next_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Cloneable handle for attaching to [`crate::service::P2pGrpcService`].
    #[must_use]
    pub fn deps(&self) -> Arc<EngineStreamDeps> {
        Arc::clone(&self.deps)
    }

    /// Accept one bidirectional stream session.
    pub async fn handle(
        &self,
        request: Request<Streaming<EngineToP2p>>,
    ) -> Result<Response<BoxStreamP2pToEngine>, Status> {
        let inbound = request.into_inner();
        let (out_tx, out_rx) = mpsc::channel::<Result<P2pToEngine, Status>>(OUTBOUND_BOUND);
        let deps = Arc::clone(&self.deps);
        let next_seq = Arc::clone(&self.next_seq);

        tokio::spawn(async move {
            if let Err(e) = run_engine_stream_session(deps, next_seq, inbound, out_tx).await {
                debug!(error = %e, "engine stream session ended with error");
            }
        });

        let stream = ReceiverStream::new(out_rx);
        Ok(Response::new(Box::pin(stream) as BoxStreamP2pToEngine))
    }
}

/// Server-streaming response type matching the generated trait (`BoxStream`).
pub type BoxStreamP2pToEngine =
    Pin<Box<dyn Stream<Item = Result<P2pToEngine, Status>> + Send + 'static>>;

/// Run one session until the inbound stream closes or the outbound channel drops.
///
/// On first `EngineHello` (and immediately if hello races after connect), send
/// the current [`SubscriptionSet`]. On every subsequent subscription watch
/// change, send again.
pub async fn run_engine_stream_session(
    deps: Arc<EngineStreamDeps>,
    next_seq: Arc<AtomicU64>,
    mut inbound: Streaming<EngineToP2p>,
    out_tx: mpsc::Sender<Result<P2pToEngine, Status>>,
) -> Result<(), SessionError> {
    let mut sub_rx = deps.subscription.subscribe();
    let mut hello_seen = false;
    // Per-session seq for downward messages.
    let mut session_seq = 0u64;

    // Drain inbound + subscription changes.
    loop {
        tokio::select! {
            biased;
            msg = inbound.next() => {
                match msg {
                    Some(Ok(m)) => {
                        handle_inbound(
                            &deps,
                            &next_seq,
                            &mut session_seq,
                            &mut hello_seen,
                            &out_tx,
                            m,
                        ).await?;
                    }
                    Some(Err(status)) => {
                        warn!(%status, "engine stream inbound error");
                        return Err(SessionError::Inbound(status));
                    }
                    None => {
                        info!("engine stream inbound closed");
                        return Ok(());
                    }
                }
            }
            changed = sub_rx.changed() => {
                if changed.is_err() {
                    // Publisher dropped — end session cleanly.
                    return Ok(());
                }
                if !hello_seen {
                    // Wait for hello before pushing subscription (session contract).
                    continue;
                }
                let local = sub_rx.borrow_and_update().clone();
                send_subscription(&out_tx, &mut session_seq, &local).await?;
            }
        }
    }
}

async fn handle_inbound(
    deps: &EngineStreamDeps,
    _next_seq: &AtomicU64,
    session_seq: &mut u64,
    hello_seen: &mut bool,
    out_tx: &mpsc::Sender<Result<P2pToEngine, Status>>,
    msg: EngineToP2p,
) -> Result<(), SessionError> {
    match msg.msg {
        Some(engine_to_p2p::Msg::Hello(hello)) => {
            info!(session_id = hello.session_id, "engine stream: EngineHello");
            *hello_seen = true;
            // Reset is a no-op for the shared seen set (global anti-equivocation);
            // session_id is logged for correlation with engine reconnects.
            let local = deps.subscription.current();
            send_subscription(out_tx, session_seq, &local).await?;
        }
        Some(engine_to_p2p::Msg::Inject(inject)) => {
            if !*hello_seen {
                warn!("engine stream: InjectColumns before Hello; processing anyway");
            }
            let results = deps.inject.inject(&inject);
            let new = results
                .iter()
                .filter(|r| r.outcome == super::inject::InjectOutcome::New)
                .count();
            let dup = results
                .iter()
                .filter(|r| r.outcome == super::inject::InjectOutcome::Duplicate)
                .count();
            let rej = results
                .iter()
                .filter(|r| r.outcome == super::inject::InjectOutcome::Rejected)
                .count();
            debug!(
                root = %hex::encode(&inject.beacon_block_root),
                slot = inject.slot,
                sidecars = inject.sidecar_ssz.len(),
                new,
                dup,
                rej,
                trusted_local = inject.trusted_local,
                "engine stream: InjectColumns processed"
            );
        }
        None => {
            debug!(seq = msg.seq, "engine stream: empty EngineToP2p oneof");
        }
    }
    Ok(())
}

async fn send_subscription(
    out_tx: &mpsc::Sender<Result<P2pToEngine, Status>>,
    session_seq: &mut u64,
    local: &super::subscription::LocalSubscription,
) -> Result<(), SessionError> {
    *session_seq = session_seq.saturating_add(1);
    let msg = P2pToEngine {
        seq: *session_seq,
        msg: Some(p2p_to_engine::Msg::Subscriptions(subscription_to_wire(
            local,
        ))),
    };
    out_tx
        .send(Ok(msg))
        .await
        .map_err(|_| SessionError::OutboundClosed)?;
    Ok(())
}

/// Session-level errors (not gRPC status mapping for the whole RPC).
#[derive(Debug)]
pub enum SessionError {
    /// Inbound tonic error.
    Inbound(Status),
    /// Outbound channel closed (client gone).
    OutboundClosed,
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inbound(s) => write!(f, "inbound: {s}"),
            Self::OutboundClosed => write!(f, "outbound closed"),
        }
    }
}

impl std::error::Error for SessionError {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::engine_stream::inject::{
        ColumnPublisher, InjectPipeline, MockSamplingSink, RecordingPublisher, SamplingSink,
        minimal_sidecar_ssz,
    };
    use crate::engine_stream::subscription::{LocalSubscription, SubscriptionHandle};
    use crate::gossip::seen::SeenSets;
    use cc_proto::p2p::{EngineHello, InjectColumns};
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    fn test_deps(sub: LocalSubscription) -> (EngineStreamDeps, Arc<RecordingPublisher>) {
        let seen = Arc::new(Mutex::new(SeenSets::new()));
        let sink = Arc::new(MockSamplingSink::default());
        let publisher = Arc::new(RecordingPublisher::default());
        let handle = SubscriptionHandle::new(sub);
        let inject = Arc::new(InjectPipeline::for_tests(
            seen,
            Arc::clone(&sink) as Arc<dyn SamplingSink>,
            Arc::clone(&publisher) as Arc<dyn ColumnPublisher>,
            handle.clone(),
            None,
        ));
        (
            EngineStreamDeps {
                inject,
                subscription: handle,
            },
            publisher,
        )
    }

    #[tokio::test]
    async fn subscription_set_sent_on_hello_and_cgc_change() {
        let (deps, _pub) = test_deps(LocalSubscription::from_indices(0..8u64, 4));
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let next_seq = Arc::new(AtomicU64::new(1));

        // Simulate hello.
        let mut hello_seen = false;
        let mut session_seq = 0u64;
        handle_inbound(
            &deps,
            &next_seq,
            &mut session_seq,
            &mut hello_seen,
            &out_tx,
            EngineToP2p {
                seq: 1,
                msg: Some(engine_to_p2p::Msg::Hello(EngineHello { session_id: 42 })),
            },
        )
        .await
        .unwrap();

        let first = out_rx.recv().await.unwrap().unwrap();
        match first.msg {
            Some(p2p_to_engine::Msg::Subscriptions(s)) => {
                assert_eq!(s.column_indices.len(), 8);
                assert_eq!(s.cgc, 4);
            }
            other => panic!("expected SubscriptionSet on hello, got {other:?}"),
        }

        // CGC change → new subscription pushed (session loop path).
        deps.subscription
            .set(LocalSubscription::from_indices(0..16u64, 8));
        // Direct send as the session loop would after watch.changed().
        let local = deps.subscription.current();
        send_subscription(&out_tx, &mut session_seq, &local)
            .await
            .unwrap();

        let second = out_rx.recv().await.unwrap().unwrap();
        match second.msg {
            Some(p2p_to_engine::Msg::Subscriptions(s)) => {
                assert_eq!(
                    s.column_indices.len(),
                    16,
                    "column_indices.len() matches subscribed"
                );
                assert_eq!(s.cgc, 8, "cgc matches the hook's value");
            }
            other => panic!("expected SubscriptionSet on cgc change, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn inject_via_session_handler() {
        let (deps, publisher) = test_deps(LocalSubscription::from_indices([0u64, 1], 4));
        let (out_tx, _out_rx) = mpsc::channel(8);
        let next_seq = Arc::new(AtomicU64::new(1));
        let mut hello_seen = true;
        let mut session_seq = 0u64;

        handle_inbound(
            &deps,
            &next_seq,
            &mut session_seq,
            &mut hello_seen,
            &out_tx,
            EngineToP2p {
                seq: 2,
                msg: Some(engine_to_p2p::Msg::Inject(InjectColumns {
                    beacon_block_root: vec![9u8; 32],
                    slot: 99,
                    sidecar_ssz: vec![minimal_sidecar_ssz(0, 99, 1)],
                    trusted_local: true,
                })),
            },
        )
        .await
        .unwrap();

        let pubs = publisher.published.lock().unwrap();
        assert_eq!(pubs.len(), 1);
        assert_eq!(pubs[0].0, 0);
    }
}
