//! Peer health prober (Architecture §4.2, CC-05b).
//!
//! One task per required peer over `tonic_health`'s `HealthClient` on an
//! `Endpoint::connect_lazy()` channel. Interval 3 s, per-probe timeout 1 s,
//! 2 consecutive failures → down, 1 success → up. Never terminates the process.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use tonic::transport::Endpoint;
use tonic_health::ServingStatus;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::server::HealthReporter;
use tracing::{debug, info, warn};

use crate::metrics::Metrics;
use crate::serve::PeerSpec;

/// Probe cadence (Architecture §4.2).
pub const PROBE_INTERVAL: Duration = Duration::from_secs(3);
/// Per-probe RPC timeout.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
/// Consecutive failures required to mark a peer down.
pub const FAIL_THRESHOLD: u32 = 2;

/// Shared peer-up map; prober tasks update it and recompute aggregate `""`.
#[derive(Debug, Clone)]
pub(crate) struct PeerHealthState {
    inner: Arc<Mutex<HashMap<String, bool>>>,
    reporter: HealthReporter,
    /// When true, aggregate recompute is suppressed (shutdown in progress).
    draining: Arc<std::sync::atomic::AtomicBool>,
}

impl PeerHealthState {
    pub(crate) fn new(peers: &[PeerSpec], reporter: HealthReporter) -> Self {
        let mut map = HashMap::with_capacity(peers.len());
        for p in peers {
            map.insert(p.name.clone(), false);
        }
        Self {
            inner: Arc::new(Mutex::new(map)),
            reporter,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(crate) fn mark_draining(&self) {
        self.draining
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Set one peer's up-flag and recompute aggregate health.
    pub(crate) async fn set_peer(&self, peer: &str, up: bool, metrics: &Metrics, service: &str) {
        let mut guard = self.inner.lock().await;
        let prev = guard.insert(peer.to_owned(), up);
        metrics.set_peer_health(service, peer, up);
        if prev != Some(up) {
            info!(peer, up, "peer health transition");
        }
        if self.draining.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let all_up = guard.values().all(|&v| v);
        drop(guard);
        let status = if all_up {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        self.reporter.set_service_status("", status).await;
    }
}

/// Spawn one prober task per peer. Returns join handles (aborted on shutdown).
pub(crate) fn spawn_probers(
    peers: Vec<PeerSpec>,
    state: PeerHealthState,
    metrics: Metrics,
    service: &'static str,
    cancel: watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    peers
        .into_iter()
        .map(|peer| {
            let state = state.clone();
            let metrics = metrics.clone();
            let cancel = cancel.clone();
            crate::spawn("peer-prober", async move {
                run_prober(peer, state, metrics, service, cancel).await;
            })
        })
        .collect()
}

async fn run_prober(
    peer: PeerSpec,
    state: PeerHealthState,
    metrics: Metrics,
    service: &'static str,
    mut cancel: watch::Receiver<bool>,
) {
    let endpoint = match Endpoint::from_shared(peer.uri.to_string()) {
        Ok(ep) => ep,
        Err(e) => {
            warn!(peer = %peer.name, error = %e, "invalid peer URI; prober idle");
            // Keep peer down; wait for cancel.
            let _ = cancel.wait_for(|&v| v).await;
            return;
        }
    };
    // Lazy connect so startup order cannot deadlock at the code level (§4.2).
    let channel = endpoint.connect_lazy();
    let mut client = HealthClient::new(channel);

    let mut consecutive_fails: u32 = 0;
    let mut up = false;
    // Seed metric as down.
    metrics.set_peer_health(service, &peer.name, false);

    loop {
        // Probe first, then sleep — faster recovery on startup.
        let probe = client.check(HealthCheckRequest {
            service: String::new(), // peer aggregate ""
        });
        let result = tokio::time::timeout(PROBE_TIMEOUT, probe).await;

        let success = match result {
            Ok(Ok(resp)) => {
                let status = resp.into_inner().status;
                // SERVING == 1 on the wire (tonic_health pb enum).
                status == tonic_health::pb::health_check_response::ServingStatus::Serving as i32
            }
            Ok(Err(status)) => {
                debug!(peer = %peer.name, %status, "peer health check RPC failed");
                false
            }
            Err(_) => {
                debug!(peer = %peer.name, "peer health check timed out");
                false
            }
        };

        if success {
            consecutive_fails = 0;
            if !up {
                up = true;
                state.set_peer(&peer.name, true, &metrics, service).await;
            }
        } else {
            consecutive_fails = consecutive_fails.saturating_add(1);
            if up && consecutive_fails >= FAIL_THRESHOLD {
                up = false;
                state.set_peer(&peer.name, false, &metrics, service).await;
            }
        }

        tokio::select! {
            _ = cancel.wait_for(|&v| v) => {
                debug!(peer = %peer.name, "prober cancelled");
                return;
            }
            _ = tokio::time::sleep(PROBE_INTERVAL) => {}
        }
    }
}
