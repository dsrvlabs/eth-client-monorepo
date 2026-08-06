//! gRPC instrumentation tower `Layer` (Architecture §4.4).
//!
//! Applied by CC-05b's `serve`. Two constraints:
//! 1. `grpc-status` may arrive in **response headers** (trailers-only / unary) or in
//!    **trailers** (streaming) — read headers first, then trailers.
//! 2. `method` is normalised against the known route set; anything else is
//!    `method="unknown"` so a port scanner cannot OOM via unbounded labels.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use http::{HeaderMap, Request, Response};
use http_body::{Body, Frame};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

use crate::metrics::Metrics;

/// Header name carrying the gRPC status code (numeric string).
const GRPC_STATUS: &str = "grpc-status";

/// Unknown gRPC status code (protocol / transport failure).
const CODE_UNKNOWN: &str = "2";

/// Tower layer that records `cc_grpc_requests_total` and
/// `cc_grpc_request_duration_seconds`.
#[derive(Clone, Debug)]
pub struct GrpcMetricsLayer {
    metrics: Metrics,
    service_name: &'static str,
    known_methods: Arc<HashSet<String>>,
}

impl GrpcMetricsLayer {
    /// Build a layer for `service_name` (process name) with a closed method set.
    pub fn new(
        metrics: Metrics,
        service_name: &'static str,
        known_methods: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            metrics,
            service_name,
            known_methods: Arc::new(known_methods.into_iter().collect()),
        }
    }
}

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService {
            inner,
            metrics: self.metrics.clone(),
            service_name: self.service_name,
            known_methods: Arc::clone(&self.known_methods),
        }
    }
}

/// Service wrapper produced by [`GrpcMetricsLayer`].
#[derive(Clone, Debug)]
pub struct GrpcMetricsService<S> {
    inner: S,
    metrics: Metrics,
    service_name: &'static str,
    known_methods: Arc<HashSet<String>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for GrpcMetricsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
    ResBody: Body + Send + 'static,
    ResBody::Data: Send,
    ResBody::Error: Send,
{
    type Response = Response<MetricsBody<ResBody>>;
    type Error = S::Error;
    type Future = GrpcMetricsFuture<S::Future, ResBody, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let path = req.uri().path().to_owned();
        let method = if self.known_methods.contains(&path) {
            path
        } else {
            "unknown".to_owned()
        };
        let start = Instant::now();
        let metrics = self.metrics.clone();
        let service_name = self.service_name;
        let inner = self.inner.call(req);

        GrpcMetricsFuture {
            inner,
            metrics,
            service_name,
            method,
            start,
            _phantom: std::marker::PhantomData,
        }
    }
}

pin_project! {
    /// Future that records metrics once the inner response (and optionally its
    /// trailers) is available.
    pub struct GrpcMetricsFuture<F, ResBody, E> {
        #[pin]
        inner: F,
        metrics: Metrics,
        service_name: &'static str,
        method: String,
        start: Instant,
        _phantom: std::marker::PhantomData<fn() -> (ResBody, E)>,
    }
}

impl<F, ResBody, E> Future for GrpcMetricsFuture<F, ResBody, E>
where
    F: Future<Output = Result<Response<ResBody>, E>>,
    ResBody: Body,
{
    type Output = Result<Response<MetricsBody<ResBody>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                let elapsed = this.start.elapsed().as_secs_f64();
                this.metrics.record_grpc(
                    this.service_name,
                    this.method.as_str(),
                    CODE_UNKNOWN,
                    elapsed,
                );
                Poll::Ready(Err(e))
            }
            Poll::Ready(Ok(response)) => {
                let elapsed = this.start.elapsed().as_secs_f64();
                // Prefer headers (trailers-only unary responses put status here).
                if let Some(code) = status_from_headers(response.headers()) {
                    this.metrics.record_grpc(
                        this.service_name,
                        this.method.as_str(),
                        &code,
                        elapsed,
                    );
                    let (parts, body) = response.into_parts();
                    let body = MetricsBody::recorded(body);
                    Poll::Ready(Ok(Response::from_parts(parts, body)))
                } else {
                    // Streaming / trailers path: wrap body and record when trailers arrive.
                    let (parts, body) = response.into_parts();
                    let body = MetricsBody::pending(
                        body,
                        this.metrics.clone(),
                        this.service_name,
                        this.method.clone(),
                        *this.start,
                    );
                    Poll::Ready(Ok(Response::from_parts(parts, body)))
                }
            }
        }
    }
}

fn status_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(GRPC_STATUS)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
}

pin_project! {
    /// Response body that optionally waits for `grpc-status` in trailers.
    pub struct MetricsBody<B> {
        #[pin]
        inner: B,
        state: BodyState,
    }
}

enum BodyState {
    /// Status already recorded from headers; pass through.
    Recorded,
    /// Waiting for trailers (or end of body without trailers).
    Pending {
        metrics: Metrics,
        service_name: &'static str,
        method: String,
        start: Instant,
        done: bool,
    },
}

impl<B> MetricsBody<B> {
    fn recorded(inner: B) -> Self {
        Self {
            inner,
            state: BodyState::Recorded,
        }
    }

    fn pending(
        inner: B,
        metrics: Metrics,
        service_name: &'static str,
        method: String,
        start: Instant,
    ) -> Self {
        Self {
            inner,
            state: BodyState::Pending {
                metrics,
                service_name,
                method,
                start,
                done: false,
            },
        }
    }

    fn record_once(state: &mut BodyState, code: &str) {
        if let BodyState::Pending {
            metrics,
            service_name,
            method,
            start,
            done,
        } = state
            && !*done
        {
            metrics.record_grpc(
                service_name,
                method.as_str(),
                code,
                start.elapsed().as_secs_f64(),
            );
            *done = true;
        }
    }
}

impl<B> Body for MetricsBody<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(frame))) => {
                if frame.is_trailers()
                    && let Some(trailers) = frame.trailers_ref()
                {
                    let code =
                        status_from_headers(trailers).unwrap_or_else(|| CODE_UNKNOWN.to_owned());
                    MetricsBody::<B>::record_once(this.state, &code);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => {
                MetricsBody::<B>::record_once(this.state, CODE_UNKNOWN);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // Body ended without trailers carrying a status.
                MetricsBody::<B>::record_once(this.state, CODE_UNKNOWN);
                Poll::Ready(None)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::metrics::Metrics;
    use http_body_util::{BodyExt, Empty, Full};
    use prometheus_client::encoding::text::encode;
    use prometheus_client::registry::Registry;
    use std::convert::Infallible;
    use tower::ServiceExt;

    fn setup_metrics() -> (Registry, Metrics) {
        let mut registry = Registry::default();
        let metrics = Metrics::register(&mut registry, "chain", "0.1.0", "deadbeef", "rustc 1");
        (registry, metrics)
    }

    fn layer(metrics: Metrics, known: &[&str]) -> GrpcMetricsLayer {
        GrpcMetricsLayer::new(metrics, "chain", known.iter().map(|s| (*s).to_owned()))
    }

    /// Mock inner service: returns a response with `grpc-status` in **headers**.
    fn mock_unary_ok(
        code: &'static str,
    ) -> impl Service<
        Request<()>,
        Response = Response<Full<bytes::Bytes>>,
        Error = Infallible,
        Future = impl Future<Output = Result<Response<Full<bytes::Bytes>>, Infallible>> + Send,
    > + Clone {
        tower::service_fn(move |_req: Request<()>| async move {
            let mut res = Response::new(Full::new(bytes::Bytes::new()));
            res.headers_mut().insert(GRPC_STATUS, code.parse().unwrap());
            Ok::<_, Infallible>(res)
        })
    }

    #[tokio::test]
    async fn records_code_from_response_headers() {
        let (registry, metrics) = setup_metrics();
        let known = "/eth.chain.v1.ChainService/GetInfo";
        let mut svc = layer(metrics, &[known]).layer(mock_unary_ok("0"));

        let req = Request::builder().uri(known).body(()).unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        // Drain body so the future path is complete.
        let _ = res.into_body().collect().await.unwrap();

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("cc_grpc_requests_total")
                && buf.contains("method=\"/eth.chain.v1.ChainService/GetInfo\"")
                && buf.contains("code=\"0\""),
            "expected counter with code=0 from headers, got:\n{buf}"
        );
    }

    #[tokio::test]
    async fn unknown_route_recorded_as_unknown_method() {
        let (registry, metrics) = setup_metrics();
        let known = "/eth.chain.v1.ChainService/GetInfo";
        let mut svc = layer(metrics, &[known]).layer(mock_unary_ok("0"));

        let req = Request::builder().uri("/scanner/probe").body(()).unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        let _ = res.into_body().collect().await.unwrap();

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("method=\"unknown\""),
            "expected method=unknown, got:\n{buf}"
        );
        assert!(
            !buf.contains("method=\"/scanner/probe\""),
            "unbounded label must not be created:\n{buf}"
        );
    }

    #[tokio::test]
    async fn records_code_from_trailers() {
        let (registry, metrics) = setup_metrics();
        let known = "/eth.chain.v1.ChainService/GetInfo";

        // Body with no header status; trailers carry grpc-status.
        let inner = tower::service_fn(move |_req: Request<()>| async move {
            let mut trailers = HeaderMap::new();
            trailers.insert(GRPC_STATUS, "5".parse().unwrap());
            let body = http_body_util::StreamBody::new(futures::stream::iter([
                Ok::<_, Infallible>(Frame::data(bytes::Bytes::from_static(b"chunk"))),
                Ok(Frame::trailers(trailers)),
            ]));
            Ok::<_, Infallible>(Response::new(body))
        });

        let mut svc = layer(metrics, &[known]).layer(inner);
        let req = Request::builder().uri(known).body(()).unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        let _ = res.into_body().collect().await.unwrap();

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("code=\"5\""),
            "expected code from trailers, got:\n{buf}"
        );
    }

    #[tokio::test]
    async fn empty_body_response_compiles_with_empty() {
        // Sanity: Empty body + header status works.
        let (registry, metrics) = setup_metrics();
        let known = "/eth.chain.v1.ChainService/GetInfo";
        let inner = tower::service_fn(move |_req: Request<()>| async move {
            let mut res = Response::new(Empty::<bytes::Bytes>::new());
            res.headers_mut().insert(GRPC_STATUS, "0".parse().unwrap());
            Ok::<_, Infallible>(res)
        });
        let mut svc = layer(metrics, &[known]).layer(inner);
        let req = Request::builder().uri(known).body(()).unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        let _ = res.into_body().collect().await.unwrap();
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(buf.contains("code=\"0\""));
    }
}
