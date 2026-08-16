//! S1-A-17 — injected engine black-hole.
//!
//! Accepts TCP and EngineService RPCs, then never answers. `NewPayload` and
//! `ForkchoiceUpdated` hang (as do the other unaries) so a chain core that
//! `block_on`s them parks. Compose overlay replaces `engine` with this binary.
//!
//! Bind: first argv or `0.0.0.0:9004`. Not a health peer (ADR-P3-02).

use std::net::SocketAddr;

use cc_proto::engine::engine_service_server::{EngineService, EngineServiceServer};
use cc_proto::engine::{
    FetchBlobsRequest, FetchBlobsResponse, ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse,
    GetEngineStateRequest, GetEngineStateResponse, GetInfoRequest, GetInfoResponse,
    NewPayloadRequest, NewPayloadResponse,
};
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
struct BlackHoleEngine;

#[tonic::async_trait]
impl EngineService for BlackHoleEngine {
    async fn get_info(
        &self,
        _: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        std::future::pending().await
    }

    async fn new_payload(
        &self,
        _: Request<NewPayloadRequest>,
    ) -> Result<Response<NewPayloadResponse>, Status> {
        std::future::pending().await
    }

    async fn forkchoice_updated(
        &self,
        _: Request<ForkchoiceUpdatedRequest>,
    ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
        std::future::pending().await
    }

    async fn get_engine_state(
        &self,
        _: Request<GetEngineStateRequest>,
    ) -> Result<Response<GetEngineStateResponse>, Status> {
        std::future::pending().await
    }

    async fn fetch_blobs(
        &self,
        _: Request<FetchBlobsRequest>,
    ) -> Result<Response<FetchBlobsResponse>, Status> {
        std::future::pending().await
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:9004".to_owned());
    let addr: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(addr).await?;
    eprintln!("cc-engine-blackhole: listening on {addr} (no newPayload/fcU answers)");
    tonic::transport::Server::builder()
        .add_service(EngineServiceServer::new(BlackHoleEngine))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;
    Ok(())
}
