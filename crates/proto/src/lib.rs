//! Generated gRPC/protobuf contracts for the consensus-client MSA.
//!
//! Source of truth is the top-level `proto/` tree (Architecture §3). Code is generated at
//! build time by `build.rs` (`protox` → `tonic_prost_build::compile_fds`); no generated
//! `.rs` files are checked in (CC-02/1, ADR-04).
//!
//! Consensus objects cross service boundaries as `bytes ssz` + metadata only — never as
//! re-modelled proto messages (§2.2 invariant 2). This crate has **zero** workspace
//! dependencies to keep that rule mechanical.
//!
//! Modules mirror protobuf packages: `eth.<service>.v1` lives at `eth::<service>::v1`, and
//! a flat re-export (`common`, `chain`, …) sits at the crate root for ergonomic imports.
//!
//! Vendored `google.rpc` (CC-18a / ADR-P1-14) is generated from
//! `proto/third_party/google/rpc/` so callers can attach `ErrorInfo` reasons
//! (`CURSOR_TOO_OLD`, `CURSOR_UNKNOWN_SESSION`, …) to `tonic::Status`.

// Nested package path so prost cross-package paths (`super::super::common::v1::…`) resolve.
pub mod eth {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("eth.common.v1");
        }
    }
    pub mod chain {
        pub mod v1 {
            tonic::include_proto!("eth.chain.v1");
        }
    }
    pub mod p2p {
        pub mod v1 {
            tonic::include_proto!("eth.p2p.v1");
        }
    }
    pub mod attestation {
        pub mod v1 {
            tonic::include_proto!("eth.attestation.v1");
        }
    }
    pub mod engine {
        pub mod v1 {
            tonic::include_proto!("eth.engine.v1");
        }
    }
    pub mod beacon_api {
        pub mod v1 {
            tonic::include_proto!("eth.beacon_api.v1");
        }
    }
    pub mod storage {
        pub mod v1 {
            tonic::include_proto!("eth.storage.v1");
        }
    }
}

/// Vendored `google.rpc` — `Status`, `ErrorInfo`, and the rest of `error_details.proto`.
pub mod google {
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}

/// `eth.common.v1` — shared `Source` and `BuildInfo` only (CC-02/3).
pub mod common {
    pub use crate::eth::common::v1::*;
}

/// `eth.chain.v1` — `ChainService`.
pub mod chain {
    pub use crate::eth::chain::v1::*;
}

/// `eth.p2p.v1` — `P2pService`.
pub mod p2p {
    pub use crate::eth::p2p::v1::*;
}

/// `eth.attestation.v1` — `AttestationService`.
pub mod attestation {
    pub use crate::eth::attestation::v1::*;
}

/// `eth.engine.v1` — `EngineService`.
pub mod engine {
    pub use crate::eth::engine::v1::*;
}

/// `eth.beacon_api.v1` — `BeaconApiService`.
pub mod beacon_api {
    pub use crate::eth::beacon_api::v1::*;
}

/// `eth.storage.v1` — `StorageService`.
pub mod storage {
    pub use crate::eth::storage::v1::*;
}

/// Flat re-export of `google.rpc` for ergonomic `cc_proto::rpc::ErrorInfo` imports.
pub mod rpc {
    pub use crate::google::rpc::*;
}

/// Encoded `FileDescriptorSet` for every package under `proto/`.
///
/// Fed to `tonic-reflection` (CC-05b) so `grpcurl -plaintext localhost:9001 list` works
/// against a running stub.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/eth_descriptor.bin"));

/// Canonical type URL for `google.rpc.ErrorInfo` (gRPC error-details convention).
pub const ERROR_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.ErrorInfo";

/// Build a `tonic::Status` whose `grpc-status-details-bin` trailer carries a
/// `google.rpc.Status` packing a single `google.rpc.ErrorInfo`.
///
/// **Detail-attachment API shape (tonic 0.14 / §13/6), recorded here so Phase 1
/// call sites share one helper:**
/// 1. Construct `google.rpc.ErrorInfo { reason, domain, metadata }`.
/// 2. Pack into `prost_types::Any` by hand — type URL
///    `type.googleapis.com/google.rpc.ErrorInfo`, value = `ErrorInfo::encode_to_vec`.
///    (`Any::from_msg` needs prost's `Name` trait, which we deliberately leave
///    disabled for the rest of the tree.)
/// 3. Wrap in `google.rpc.Status { code, message, details: [any] }` and
///    `Message::encode_to_vec`.
/// 4. `tonic::Status::with_details(code, message, bytes)` — the bytes land in
///    the `grpc-status-details-bin` trailer (base64, no pad on the wire).
///
/// Read-back is the inverse: `Status::details()` → decode `google.rpc.Status` →
/// find an `Any` whose type URL ends with `google.rpc.ErrorInfo` → decode.
///
/// Domain convention for this monorepo: `"eth.chain.v1"`.
pub fn status_with_error_info(
    code: tonic::Code,
    message: impl Into<String>,
    reason: impl Into<String>,
    domain: impl Into<String>,
) -> tonic::Status {
    use prost::Message;

    let message = message.into();
    let info = rpc::ErrorInfo {
        reason: reason.into(),
        domain: domain.into(),
        metadata: Default::default(),
    };
    let any = prost_types::Any {
        type_url: ERROR_INFO_TYPE_URL.into(),
        value: info.encode_to_vec(),
    };
    let rpc_status = rpc::Status {
        code: code as i32,
        message: message.clone(),
        details: vec![any],
    };
    tonic::Status::with_details(
        code,
        message,
        bytes::Bytes::from(rpc_status.encode_to_vec()),
    )
}

/// Decode the first `google.rpc.ErrorInfo` packed in a `tonic::Status`'s details, if any.
pub fn error_info_from_status(
    status: &tonic::Status,
) -> Result<Option<rpc::ErrorInfo>, prost::DecodeError> {
    use prost::Message;

    let details = status.details();
    if details.is_empty() {
        return Ok(None);
    }
    let rpc_status = rpc::Status::decode(details)?;
    for any in &rpc_status.details {
        if any_type_url_is_error_info(&any.type_url) {
            return Ok(Some(rpc::ErrorInfo::decode(any.value.as_slice())?));
        }
    }
    Ok(None)
}

fn any_type_url_is_error_info(type_url: &str) -> bool {
    type_url == ERROR_INFO_TYPE_URL || type_url.ends_with("/google.rpc.ErrorInfo")
}

#[cfg(test)]
mod smoke {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use prost::Message;

    #[test]
    fn descriptor_and_types_resolve() {
        assert!(!crate::FILE_DESCRIPTOR_SET.is_empty());
        let _ = crate::common::BuildInfo {
            service: "chain".into(),
            version: "0".into(),
            git_sha: "x".into(),
            rustc: "y".into(),
        };
        let _ = crate::chain::GetInfoRequest {};
        let _ = std::any::type_name::<crate::chain::chain_service_server::ChainServiceServer<()>>();
        let _ = std::any::type_name::<
            crate::p2p::p2p_service_client::P2pServiceClient<tonic::transport::Channel>,
        >();
    }

    /// Compile-only construction of every CC-18 / CC-1E request/response type.
    #[test]
    fn cc18_request_response_types_construct() {
        use crate::chain::{
            ApplyAttestationsRequest, ApplyAttestationsResponse, AttestationApplyResult,
            AttestationApplyVerdict, Checkpoint, Cursor, Event, EventKind, GetHeadRequest,
            GetHeadResponse, ImportBlockRequest, ImportBlockResponse, ImportBlockVerdict,
            SubscribeEventsRequest,
        };
        use crate::common::Source;

        // ImportBlockRequest fields are exactly ssz / fork / root / source (parent plan).
        let req = ImportBlockRequest {
            ssz: vec![0u8; 4],
            fork: 5,
            root: vec![1u8; 32],
            source: Source::Gossip as i32,
        };
        assert_eq!(req.fork, 5);
        assert_eq!(req.root.len(), 32);
        assert_eq!(req.source, Source::Gossip as i32);

        let resp = ImportBlockResponse {
            verdict: ImportBlockVerdict::Imported as i32,
            reason: String::new(),
        };
        assert_eq!(resp.verdict, ImportBlockVerdict::Imported as i32);

        // All five first-class verdicts exist (plus UNSPECIFIED).
        for v in [
            ImportBlockVerdict::Unspecified,
            ImportBlockVerdict::Imported,
            ImportBlockVerdict::Duplicate,
            ImportBlockVerdict::DeferredDa,
            ImportBlockVerdict::UnknownParent,
            ImportBlockVerdict::Invalid,
        ] {
            let _ = ImportBlockResponse {
                verdict: v as i32,
                reason: "x".into(),
            };
        }

        let _ = GetHeadRequest {};
        let head = GetHeadResponse {
            head_root: vec![2u8; 32],
            head_slot: 100,
            justified: Some(Checkpoint {
                epoch: 3,
                root: vec![3u8; 32],
            }),
            finalized: Some(Checkpoint {
                epoch: 2,
                root: vec![4u8; 32],
            }),
        };
        assert_eq!(head.head_slot, 100);

        let sub = SubscribeEventsRequest {
            cursor: Some(Cursor {
                session_id: 9,
                seq: 1,
                slot: 100,
                root: vec![5u8; 32],
            }),
        };
        assert_eq!(sub.cursor.as_ref().unwrap().session_id, 9);

        for kind in [
            EventKind::Unspecified,
            EventKind::Head,
            EventKind::ChainReorg,
            EventKind::FinalizedCheckpoint,
            EventKind::BlockImported,
        ] {
            let ev = Event {
                seq: 1,
                slot: 100,
                root: vec![6u8; 32],
                kind: kind as i32,
                payload: vec![],
            };
            assert_eq!(ev.kind, kind as i32);
        }

        // Round-trip one request through prost encode/decode so field numbers are live.
        let bytes = req.encode_to_vec();
        let decoded = ImportBlockRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.ssz, req.ssz);
        assert_eq!(decoded.fork, req.fork);
        assert_eq!(decoded.root, req.root);
        assert_eq!(decoded.source, req.source);

        // CC-1E ApplyAttestations types (additive; field numbers live).
        let apply_req = ApplyAttestationsRequest {
            attestations_ssz: vec![vec![1u8; 8], vec![2u8; 8]],
        };
        assert_eq!(apply_req.attestations_ssz.len(), 2);
        let apply_resp = ApplyAttestationsResponse {
            results: vec![
                AttestationApplyResult {
                    verdict: AttestationApplyVerdict::Applied as i32,
                    reason: String::new(),
                },
                AttestationApplyResult {
                    verdict: AttestationApplyVerdict::Rejected as i32,
                    reason: "unknown block".into(),
                },
            ],
        };
        assert_eq!(apply_resp.results.len(), 2);
        let apply_bytes = apply_req.encode_to_vec();
        let apply_decoded = ApplyAttestationsRequest::decode(apply_bytes.as_slice()).unwrap();
        assert_eq!(apply_decoded.attestations_ssz, apply_req.attestations_ssz);
    }

    /// §13/6 detail-attachment API: pack `ErrorInfo{reason=CURSOR_TOO_OLD}` into a
    /// `tonic::Status` and read it back (CC-18a acceptance).
    #[test]
    fn cursor_too_old_error_info_round_trip() {
        let status = crate::status_with_error_info(
            tonic::Code::FailedPrecondition,
            "cursor fell out of the event ring",
            "CURSOR_TOO_OLD",
            "eth.chain.v1",
        );

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(!status.details().is_empty());

        let info = crate::error_info_from_status(&status)
            .unwrap()
            .expect("ErrorInfo detail present");
        assert_eq!(info.reason, "CURSOR_TOO_OLD");
        assert_eq!(info.domain, "eth.chain.v1");
    }
}
