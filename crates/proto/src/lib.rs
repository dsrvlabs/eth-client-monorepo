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

/// Encoded `FileDescriptorSet` for every package under `proto/`.
///
/// Fed to `tonic-reflection` (CC-05b) so `grpcurl -plaintext localhost:9001 list` works
/// against a running stub.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/eth_descriptor.bin"));

#[cfg(test)]
mod smoke {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

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
}
