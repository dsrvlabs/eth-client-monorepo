//! EngineStream server — the ninth contract, `p2p` side (CC-38b).
//!
//! Architecture §5.2 / §5.5 / §5.6 / §5.8 / ADR P3-07:
//! - gRPC `EngineStream` server (engine dials; sidecars up; subscription +
//!   column-branch fetch down).
//! - §5.5 four-step injection routes `InjectColumns` into **the same entry
//!   point gossip columns use** — [`SamplingTracker::on_column`] with
//!   [`ColumnSource::Engine`] — so there is one producer of `DataAvailable`.
//! - Anti-equivocation `seen` update at the component where the cache lives
//!   ([`crate::gossip::seen`]); no second `seen` structure here.
//! - Publish-iff-subscribed filter (second half of ADR P3-07).
//! - `SubscriptionSet` producer — first consumer of CC-21's
//!   `set_custody_group_count` hook: sends on `EngineHello` and on every cgc
//!   change.
//!
//! # Security residual (S-38a-1 / S-38b-1)
//!
//! `InjectColumns.trusted_local` is a **client-asserted** bool on an
//! **unauthenticated** internal stream.
//!
//! - **KZG:** skip is gated on [`AuthMode::Authenticated`] **and**
//!   `trusted_local`. Without auth, production always re-verifies KZG.
//! - **Inclusion multiproof:** **always** re-verified — never skipped, even
//!   when KZG skip is allowed (S-38b-1).
//!
//! ## AuthMode footgun
//!
//! [`AuthMode::Authenticated`] is a software knob, **not** transport auth.
//! Production hosts must leave the default Unauthenticated until real mutual
//! auth (mTLS / allowlist / token) is wired. Mis-setting Authenticated +
//! engine's always-`trusted_local=true` silently skips **KZG only**.
//!
//! # MP-X3
//!
//! This module calls into the sampling tracker, the anti-equivocation seen set,
//! and the publish path at **named call sites**. It does **not** edit
//! completion logic inside `das/sampling.rs`.

pub mod inject;
pub mod server;
pub mod subscription;

pub use inject::{
    AlwaysValidInclusion, AuthMode, ColumnPublisher, InclusionVerify, InjectCounters,
    InjectOutcome, InjectPipeline, InjectSidecarResult, KzgPolicy, MockSampleCall,
    MockSamplingSink, NoopPublisher, ProductionInclusion, RecordingPublisher, SamplingSink,
    should_skip_kzg,
};
pub use server::{
    EngineStreamDeps, EngineStreamService, build_minimal_engine_stream, run_engine_stream_session,
};
pub use subscription::{
    CgcSubscriptionBridge, LocalSubscription, SubscriptionHandle, subscription_set_from_columns,
    subscription_to_wire,
};
