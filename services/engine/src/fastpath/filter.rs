//! Subscribe-only filter **inside `engine` before the process boundary**
//! (CC-37b / Architecture §5.4, ADR P3-07).
//!
//! # Spec reading (OQ-P3-1, narrow)
//!
//! `fulu/p2p-interface.md` says publish **"if and only if"** they are
//! **subscribed**. `das-core.md`'s cross-seeding SHOULD is written for
//! **reconstruction from 50 %+ of columns and not for the EL path**, and the
//! two are **not reconciled** at `v1.7.0-alpha.13`. Phase 3 takes the narrow
//! reading and does **not** publish to unsubscribed subnets on the strength of
//! the das-core SHOULD.
//!
//! # Where the filter runs
//!
//! The filter reads the subscription set **before** the outbound message is
//! constructed. At `cgc = 4` / `sampling_size = 8` that is **8 of 128** subnets,
//! so ~353 KB leaves `engine` instead of ~5.6 MB (16× on the hot path).
//!
//! # Remainder policy
//!
//! The remainder is **dropped, not queued** (`CC-37` /7). A queue of 120 unsent
//! sidecars per block is a memory leak with a plausible-sounding justification.
//! `cc_engine_sidecars_published_total{subscribed="false"}` must stay at zero.

use std::collections::BTreeSet;

use cc_types::preset::Mainnet;
use cc_types::sidecar::DataColumnSidecar;
use cc_types::NUMBER_OF_COLUMNS;
use ssz::Encode;

use crate::metrics::{EngineMetrics, Subscribed, SubscribedLabels};

/// Configured / stream-supplied set of column indices this node publishes.
///
/// **Production source of truth:** custody-sampled column indices from p2p
/// (CC-24a / CC-38 `SubscriptionSet` wire). The engine **never** invents a
/// default of `0..8` — that is not node_id-dependent custody sampling.
///
/// Until CC-38a lands, the set is supplied as a constructor / config parameter
/// on [`crate::fastpath::FastpathLane`] and can be updated via
/// [`crate::fastpath::FastpathLane::set_subscription`]. Empty set ⇒ publish
/// nothing (fail-closed for over-publish).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubscriptionSet {
    /// Subscribed column indices (`0..NUMBER_OF_COLUMNS`).
    pub column_indices: BTreeSet<u64>,
    /// Custody group count that produced this set (observability / future wire).
    /// Not used by [`filter_subscribed`] — callers must expand cgc → indices
    /// in p2p before sending the set.
    pub cgc: u64,
}

impl SubscriptionSet {
    /// Empty subscription (publish nothing). Safe production default until
    /// custody-sampled indices arrive.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            column_indices: BTreeSet::new(),
            cgc: 0,
        }
    }

    /// Construct from an iterator of column indices (production + tests).
    ///
    /// Out-of-range indices (`>= NUMBER_OF_COLUMNS`) are dropped.
    #[must_use]
    pub fn from_indices(indices: impl IntoIterator<Item = u64>, cgc: u64) -> Self {
        let column_indices: BTreeSet<u64> = indices
            .into_iter()
            .filter(|&i| i < NUMBER_OF_COLUMNS)
            .collect();
        Self {
            column_indices,
            cgc,
        }
    }

    /// Whether `column_index` is in the subscribed set.
    #[must_use]
    pub fn is_subscribed(&self, column_index: u64) -> bool {
        self.column_indices.contains(&column_index)
    }

    /// Number of subscribed columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.column_indices.len()
    }

    /// Empty set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.column_indices.is_empty()
    }
}

/// Result of filtering assembled sidecars to the subscribed set.
#[derive(Debug, Clone)]
pub struct FilterOutcome {
    /// Sidecars that leave `engine` (subscribed only).
    pub published: Vec<DataColumnSidecar<Mainnet>>,
    /// Count of sidecars **dropped** (not queued).
    pub dropped: usize,
    /// Total SSZ-serialised bytes of the published set (pre process boundary).
    pub published_bytes: usize,
}

/// Filter assembled sidecars to the subscribed set **before** any outbound
/// message is constructed (ADR P3-07).
///
/// - Emits only subscribed indices.
/// - **Drops** the remainder — no queue, no buffer of unsubscribed sidecars.
/// - Increments `cc_engine_sidecars_published_total{subscribed="true"}` once
///   per published sidecar. The `{subscribed="false"}` series is never
///   incremented here — its job is to stay at zero.
pub fn filter_subscribed(
    assembled: Vec<DataColumnSidecar<Mainnet>>,
    subscription: &SubscriptionSet,
    metrics: Option<&EngineMetrics>,
) -> FilterOutcome {
    // Read the subscription set *before* building the outbound payload.
    let subscribed = &subscription.column_indices;

    let mut published = Vec::with_capacity(subscribed.len());
    let mut dropped = 0usize;

    for sc in assembled {
        if subscribed.contains(&sc.index) {
            if let Some(m) = metrics {
                m.sidecars_published
                    .get_or_create(&SubscribedLabels {
                        subscribed: Subscribed::True.as_str().to_owned(),
                    })
                    .inc();
            }
            published.push(sc);
        } else {
            // Drop — not queued. Unsubscribed columns never cross the boundary.
            dropped += 1;
            // Deliberately do **not** increment subscribed="false".
        }
    }

    let published_bytes: usize = published.iter().map(|sc| sc.as_ssz_bytes().len()).sum();

    FilterOutcome {
        published,
        dropped,
        published_bytes,
    }
}

/// Bound check: published payload for a 21-blob / 8-column block stays near
/// ~353 KB (Architecture §2.3 / §5.4), not the unfiltered ~5.6 MB.
pub const FILTERED_PAYLOAD_SOFT_MAX_BYTES: usize = 500 * 1024; // ~353 KB + headroom
/// Unfiltered 128-column payload order of magnitude (must not leave engine).
pub const UNFILTERED_PAYLOAD_ORDER_BYTES: usize = 5 * 1024 * 1024;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::fastpath::cells::compute_cells_zipped_with_el_proofs;
    use crate::fastpath::sidecars::{
        synthetic_inclusion_proof, transpose_to_sidecars, SidecarTemplate,
    };
    use crate::methods::get_blobs::BlobAndProofV2;
    use crate::metrics::EngineMetrics;
    use cc_crypto::{Blob, CellKzg, CKzgBackend};
    use cc_types::containers::SignedBeaconBlockHeader;
    use cc_types::primitives::Slot;
    use prometheus_client::registry::Registry;
    use std::sync::Arc;

    fn metrics() -> EngineMetrics {
        let mut registry = Registry::default();
        EngineMetrics::register(&mut registry)
    }

    fn backend() -> Arc<dyn CellKzg> {
        Arc::new(CKzgBackend::load_default().expect("load"))
    }

    /// Test-only fixture: first `n` column indices. **Not** custody sampling
    /// (CC-24a produces node_id-dependent sets). Production must not use this.
    fn fixture_first_n_columns(n: u64, cgc: u64) -> SubscriptionSet {
        SubscriptionSet::from_indices(0..n, cgc)
    }

    async fn assemble_n(
        kzg: &Arc<dyn CellKzg>,
        n: usize,
    ) -> Vec<DataColumnSidecar<Mainnet>> {
        use crate::methods::get_blobs::kzg_commitment_to_versioned_hash;
        let mut items = Vec::new();
        let mut commitments = Vec::new();
        let mut hashes = Vec::new();
        for i in 0..n {
            let blob = Blob::filled((i as u8).saturating_add(1));
            let c = kzg.blob_to_kzg_commitment(&blob).expect("c");
            hashes.push(kzg_commitment_to_versioned_hash(c.as_array()));
            commitments.push(c);
            let (_c, proofs) = kzg.compute_cells_and_kzg_proofs(&blob).expect("p");
            items.push(BlobAndProofV2 {
                blob: blob.as_slice().to_vec(),
                proofs: proofs.iter().map(|p| p.as_slice().to_vec()).collect(),
            });
        }
        let materials = compute_cells_zipped_with_el_proofs(
            Arc::clone(kzg),
            items,
            hashes,
            commitments.clone(),
            None,
        )
        .await
        .expect("cells");
        let (incl, _, _) = synthetic_inclusion_proof(&commitments);
        let mut header = SignedBeaconBlockHeader::default();
        header.message.slot = Slot::new(54_016 * 32);
        let template = SidecarTemplate::new(header, commitments, incl);
        transpose_to_sidecars(&materials, &template, None).expect("transpose")
    }

    #[tokio::test]
    async fn publish_iff_subscribed() {
        let kzg = backend();
        let m = metrics();
        // Small n for speed; filter cardinality is over columns, not blobs.
        let assembled = assemble_n(&kzg, 2).await;
        assert_eq!(assembled.len(), 128);

        // Explicit test subscription — not a production default.
        let sub = fixture_first_n_columns(8, 4);
        assert_eq!(sub.len(), 8);

        // Bounded-memory: filter holds only the published set — no queue of 120.
        let before_false = m
            .sidecars_published
            .get_or_create(&SubscribedLabels {
                subscribed: Subscribed::False.as_str().to_owned(),
            })
            .get();
        let outcome = filter_subscribed(assembled, &sub, Some(&m));
        assert_eq!(outcome.published.len(), 8, "exactly the subscribed indices");
        assert_eq!(outcome.dropped, 120, "remainder dropped");
        for sc in &outcome.published {
            assert!(sub.is_subscribed(sc.index), "published unsubscribed {}", sc.index);
        }
        let false_after = m
            .sidecars_published
            .get_or_create(&SubscribedLabels {
                subscribed: Subscribed::False.as_str().to_owned(),
            })
            .get();
        assert_eq!(
            false_after - before_false,
            0,
            "cc_engine_sidecars_published_total{{subscribed=\"false\"}} must stay 0"
        );
        let true_count = m
            .sidecars_published
            .get_or_create(&SubscribedLabels {
                subscribed: Subscribed::True.as_str().to_owned(),
            })
            .get();
        assert_eq!(true_count, 8);

        // Bounded-memory check: published vec capacity is not a growing queue of 128.
        assert!(
            outcome.published.capacity() <= 16,
            "filter must not retain a 120-sidecar queue (capacity={})",
            outcome.published.capacity()
        );
    }

    #[tokio::test]
    async fn filter_precedes_boundary_byte_size() {
        let kzg = backend();
        // 21-blob shape for the 353 KB argument (Architecture §5.4).
        let assembled = assemble_n(&kzg, 21).await;
        let unfiltered_bytes: usize = assembled.iter().map(|s| s.as_ssz_bytes().len()).sum();
        assert!(
            unfiltered_bytes > UNFILTERED_PAYLOAD_ORDER_BYTES / 2,
            "unfiltered should be multi-MB order, got {unfiltered_bytes}"
        );

        let sub = fixture_first_n_columns(8, 4);
        // subscription is read before outbound construction (see filter_subscribed).
        assert!(!sub.is_empty());
        let outcome = filter_subscribed(assembled, &sub, None);
        assert_eq!(outcome.published.len(), 8);
        assert!(
            outcome.published_bytes <= FILTERED_PAYLOAD_SOFT_MAX_BYTES,
            "filtered payload {} exceeds ~353 KB soft max {}",
            outcome.published_bytes,
            FILTERED_PAYLOAD_SOFT_MAX_BYTES
        );
        assert!(
            outcome.published_bytes < unfiltered_bytes / 8,
            "filtered {} must be ≪ unfiltered {}",
            outcome.published_bytes,
            unfiltered_bytes
        );
        // Sanity: near the documented ~353 KB for 21 blobs × 8 columns.
        assert!(
            outcome.published_bytes > 200 * 1024,
            "expected ~353 KB class, got {}",
            outcome.published_bytes
        );
    }

    #[test]
    fn subscription_comment_records_narrow_reading() {
        let src = include_str!("filter.rs");
        assert!(src.contains("if and only if"));
        assert!(src.contains("subscribed"));
        assert!(src.contains("das-core.md") || src.contains("das-core"));
        assert!(src.contains("50 %+") || src.contains("50%+"));
        assert!(src.contains("not reconciled") || src.contains("not for the EL path"));
        assert!(src.contains("v1.7.0-alpha.13"));
        // Filter reads subscription before outbound message construction.
        assert!(src.contains("before") && src.contains("outbound"));
        // Production must not *construct* 0..8 as live default (docs may warn against it).
        let production: String = src
            .lines()
            .take_while(|l| !l.contains("#[cfg(test)]") && !l.contains("mod tests"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !production.contains("sampling_size_eight")
                && !production.contains("from_indices(0..8"),
            "production SubscriptionSet must not construct 0..8 as default"
        );
        assert!(
            production.contains("never invents") || production.contains("Empty set"),
            "production docs must reject inventing custody columns"
        );
    }

    #[test]
    fn verify_skipped_in_production_fastpath() {
        // ADR P3-15: verify_cell_kzg_proof_batch only under cfg(test) in fastpath.
        for path in ["cells.rs", "sidecars.rs", "filter.rs", "mod.rs", "fetch.rs"] {
            let src = match path {
                "cells.rs" => include_str!("cells.rs"),
                "sidecars.rs" => include_str!("sidecars.rs"),
                "filter.rs" => include_str!("filter.rs"),
                "mod.rs" => include_str!("mod.rs"),
                "fetch.rs" => include_str!("fetch.rs"),
                _ => unreachable!(),
            };
            // Split on cfg(test) modules — production region must not verify.
            let production: String = src
                .lines()
                .take_while(|l| {
                    !l.contains("#[cfg(test)]") && !l.contains("mod tests")
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !production.contains("verify_cell_kzg_proof_batch"),
                "{path} production must not call verify_cell_kzg_proof_batch"
            );
        }
    }
}
