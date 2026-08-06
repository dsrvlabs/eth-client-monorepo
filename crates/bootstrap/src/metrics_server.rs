//! Minimal Prometheus text exposition server (Architecture §4.4).
//!
//! Binds `metrics_addr`, answers `GET /metrics` by encoding the registry.
//! No auth, no TLS — intended for the compose network / localhost scrape.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;

use crate::error::Error;

/// Serve OpenMetrics text on `addr` until the listener fails.
///
/// Intended to be spawned by CC-05b's `serve` (and by tests in this crate).
pub async fn serve_metrics(addr: SocketAddr, registry: Arc<Registry>) -> Result<(), Error> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let registry = Arc::clone(&registry);
                async move { handle(req, registry).await }
            });
            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!(error = %err, "metrics connection closed with error");
            }
        });
    }
}

/// Bind an ephemeral port, spawn the server, return `(bound_addr, join_handle)`.
///
/// Used by tests so they can `curl` without racing a fixed port.
pub async fn spawn_metrics_server(
    registry: Arc<Registry>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<Result<(), Error>>), Error> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let registry = Arc::clone(&registry);
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let registry = Arc::clone(&registry);
                    async move { handle(req, registry).await }
                });
                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
        // Unreachable; type the loop error path.
        #[allow(unreachable_code)]
        Ok::<(), Error>(())
    });
    Ok((addr, handle))
}

async fn handle(
    req: Request<Incoming>,
    registry: Arc<Registry>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    if req.method() == Method::GET && req.uri().path() == "/metrics" {
        let mut buf = String::new();
        if encode(&mut buf, &registry).is_err() {
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(full(Bytes::from_static(b"encode error")))
                .unwrap_or_else(|_| Response::new(full(Bytes::new()))));
        }
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(
                hyper::header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .body(full(Bytes::from(buf)))
            .unwrap_or_else(|_| Response::new(full(Bytes::new()))))
    } else {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(full(Bytes::from_static(b"not found")))
            .unwrap_or_else(|_| Response::new(empty())))
    }
}

fn full(body: Bytes) -> BoxBody<Bytes, Infallible> {
    Full::new(body).map_err(|never| match never {}).boxed()
}

fn empty() -> BoxBody<Bytes, Infallible> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
