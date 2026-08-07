//! Validator-record cache + `GetValidatorRecords` client (CC-2B / §5.4a).
//!
//! A **4 096-entry LRU with a one-epoch TTL**, keyed by validator index. The
//! cache exists so a slashing flood cannot become a query flood against the
//! chain core thread. Over-bound requests (>256 indices) are rejected with
//! [`RecordsError::OverBound`] — never truncated.

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cc_proto::chain::chain_service_client::ChainServiceClient;
use cc_proto::chain::GetValidatorRecordsRequest;
use cc_types::containers::Validator;
use lru::LruCache;
use ssz::Decode;
use thiserror::Error;
use tonic::transport::Endpoint;
use tracing::debug;

/// Cache capacity (Architecture §5.4a).
pub const VALIDATOR_RECORD_CACHE_BOUND: usize = 4_096;

/// Max indices per `GetValidatorRecords` RPC (CC-27a / §5.4a).
pub const MAX_VALIDATOR_RECORDS_PER_REQUEST: usize = 256;

/// One cached record with the epoch it was fetched in.
#[derive(Debug, Clone)]
struct CacheEntry {
    record: Validator,
    /// Epoch at which the entry was inserted (one-epoch TTL).
    epoch: u64,
}

/// Errors from the validator-record path.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecordsError {
    /// Caller asked for more than [`MAX_VALIDATOR_RECORDS_PER_REQUEST`] in one batch.
    #[error(
        "GetValidatorRecords bound is {bound} indices; got {got} (never truncated)"
    )]
    OverBound {
        /// Configured bound.
        bound: usize,
        /// Requested count.
        got: usize,
    },
    /// Empty indices list.
    #[error("GetValidatorRecords requires a non-empty indices list")]
    Empty,
    /// Underlying transport / RPC / decode failure.
    #[error("GetValidatorRecords failed: {0}")]
    Fetch(String),
    /// Index missing from a successful response (should not happen with a correct chain).
    #[error("validator index {0} missing from GetValidatorRecords response")]
    Missing(u64),
}

/// Successful fetch of a parallel `indices → records` batch + head slot.
#[derive(Debug, Clone)]
pub struct FetchedRecords {
    /// Records aligned with the request (same order/length as requested indices).
    pub records: Vec<Validator>,
    /// Head slot reported by chain.
    pub slot: u64,
}

/// Async source of validator records (unary `chain` query).
pub trait ValidatorRecordSource: Send + Sync {
    /// Fetch records for `indices` (must be non-empty and ≤ 256).
    ///
    /// Returns SSZ-decoded [`Validator`] values **parallel to `indices`**.
    fn fetch_records(
        &self,
        indices: &[u64],
    ) -> Pin<Box<dyn Future<Output = Result<FetchedRecords, RecordsError>> + Send + '_>>;
}

/// Production unary client for [`GetValidatorRecords`](cc_proto::chain).
#[derive(Debug, Clone)]
pub struct RpcValidatorRecordSource {
    /// Chain gRPC URI (e.g. `http://127.0.0.1:50051`).
    pub chain_uri: String,
}

impl ValidatorRecordSource for RpcValidatorRecordSource {
    fn fetch_records(
        &self,
        indices: &[u64],
    ) -> Pin<Box<dyn Future<Output = Result<FetchedRecords, RecordsError>> + Send + '_>> {
        let uri = self.chain_uri.clone();
        let indices = indices.to_vec();
        Box::pin(async move {
            if indices.is_empty() {
                return Err(RecordsError::Empty);
            }
            if indices.len() > MAX_VALIDATOR_RECORDS_PER_REQUEST {
                return Err(RecordsError::OverBound {
                    bound: MAX_VALIDATOR_RECORDS_PER_REQUEST,
                    got: indices.len(),
                });
            }
            let endpoint = Endpoint::from_shared(uri)
                .map_err(|e| RecordsError::Fetch(e.to_string()))?;
            let channel = endpoint
                .connect()
                .await
                .map_err(|e| RecordsError::Fetch(e.to_string()))?;
            let mut client = ChainServiceClient::new(channel);
            let resp = client
                .get_validator_records(GetValidatorRecordsRequest { indices })
                .await
                .map_err(|e| RecordsError::Fetch(e.to_string()))?
                .into_inner();
            let mut records = Vec::with_capacity(resp.ssz.len());
            for bytes in &resp.ssz {
                let v = Validator::from_ssz_bytes(bytes)
                    .map_err(|e| RecordsError::Fetch(format!("ssz decode: {e:?}")))?;
                records.push(v);
            }
            Ok(FetchedRecords {
                records,
                slot: resp.slot,
            })
        })
    }
}

/// In-memory map source for tests (no network).
#[derive(Debug, Default, Clone)]
pub struct MapValidatorRecordSource {
    /// Index → record.
    pub map: Arc<Mutex<HashMap<u64, Validator>>>,
    /// Count of `fetch_records` invocations (RPC call analogue).
    pub calls: Arc<AtomicU64>,
    /// Total indices requested across all calls.
    pub indices_requested: Arc<AtomicU64>,
}

impl MapValidatorRecordSource {
    /// Empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed one record.
    pub fn insert(&self, index: u64, record: Validator) {
        let mut g = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.insert(index, record);
    }

    /// Number of `fetch_records` calls.
    #[must_use]
    pub fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

impl ValidatorRecordSource for MapValidatorRecordSource {
    fn fetch_records(
        &self,
        indices: &[u64],
    ) -> Pin<Box<dyn Future<Output = Result<FetchedRecords, RecordsError>> + Send + '_>> {
        let indices = indices.to_vec();
        Box::pin(async move {
            if indices.is_empty() {
                return Err(RecordsError::Empty);
            }
            if indices.len() > MAX_VALIDATOR_RECORDS_PER_REQUEST {
                return Err(RecordsError::OverBound {
                    bound: MAX_VALIDATOR_RECORDS_PER_REQUEST,
                    got: indices.len(),
                });
            }
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.indices_requested
                .fetch_add(indices.len() as u64, Ordering::Relaxed);
            let g = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut records = Vec::with_capacity(indices.len());
            for &idx in &indices {
                let Some(v) = g.get(&idx).cloned() else {
                    return Err(RecordsError::Missing(idx));
                };
                records.push(v);
            }
            Ok(FetchedRecords {
                records,
                slot: 0,
            })
        })
    }
}

/// 4 096-entry LRU of validator records with a one-epoch TTL.
#[derive(Debug)]
pub struct ValidatorRecordCache {
    inner: Mutex<LruCache<u64, CacheEntry>>,
    /// Number of underlying `fetch_records` batch calls.
    pub fetch_calls: AtomicU64,
    /// Number of indices loaded from the source (cache misses).
    pub miss_loads: AtomicU64,
    /// Number of indices served from the cache.
    pub hits: AtomicU64,
}

impl ValidatorRecordCache {
    /// Capacity [`VALIDATOR_RECORD_CACHE_BOUND`].
    #[must_use]
    pub fn new() -> Self {
        let cap = NonZeroUsize::new(VALIDATOR_RECORD_CACHE_BOUND).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(LruCache::new(cap)),
            fetch_calls: AtomicU64::new(0),
            miss_loads: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    /// Current occupancy.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Configured capacity.
    #[must_use]
    pub const fn bound() -> usize {
        VALIDATOR_RECORD_CACHE_BOUND
    }

    /// Underlying fetch-batch count (for flood tests).
    #[must_use]
    pub fn fetch_call_count(&self) -> u64 {
        self.fetch_calls.load(Ordering::Relaxed)
    }

    /// Load records for `indices` at `current_epoch`, using `source` on misses.
    ///
    /// Batches misses into ≤256-index RPCs. Never truncates a single request
    /// over 256 — that is [`RecordsError::OverBound`] only when a **single**
    /// batch would exceed the bound (internal batches are always ≤256).
    ///
    /// # Errors
    ///
    /// Propagates source errors. Empty `indices` returns an empty map.
    pub async fn get_many(
        &self,
        indices: &[u64],
        current_epoch: u64,
        source: &dyn ValidatorRecordSource,
    ) -> Result<HashMap<u64, Validator>, RecordsError> {
        if indices.is_empty() {
            return Ok(HashMap::new());
        }

        // Dedup while preserving first-seen order for deterministic batches.
        let mut unique = Vec::with_capacity(indices.len());
        {
            let mut seen = std::collections::HashSet::with_capacity(indices.len());
            for &i in indices {
                if seen.insert(i) {
                    unique.push(i);
                }
            }
        }

        let mut out = HashMap::with_capacity(unique.len());
        let mut misses = Vec::new();

        {
            let mut guard = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for &idx in &unique {
                if let Some(entry) = guard.get(&idx) {
                    // One-epoch TTL: entry is valid only for the epoch it was cached.
                    if entry.epoch == current_epoch {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        out.insert(idx, entry.record);
                        continue;
                    }
                }
                misses.push(idx);
            }
        }

        // Fetch misses in ≤256 batches.
        for chunk in misses.chunks(MAX_VALIDATOR_RECORDS_PER_REQUEST) {
            self.fetch_calls.fetch_add(1, Ordering::Relaxed);
            let fetched = source.fetch_records(chunk).await?;
            if fetched.records.len() != chunk.len() {
                return Err(RecordsError::Fetch(format!(
                    "response length {} != request length {}",
                    fetched.records.len(),
                    chunk.len()
                )));
            }
            let mut guard = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (&idx, record) in chunk.iter().zip(fetched.records) {
                self.miss_loads.fetch_add(1, Ordering::Relaxed);
                out.insert(idx, record);
                guard.put(
                    idx,
                    CacheEntry {
                        record,
                        epoch: current_epoch,
                    },
                );
            }
        }

        // Final check: every unique index present.
        for &idx in &unique {
            if !out.contains_key(&idx) {
                return Err(RecordsError::Missing(idx));
            }
        }
        debug!(
            hits = self.hits.load(Ordering::Relaxed),
            misses = self.miss_loads.load(Ordering::Relaxed),
            calls = self.fetch_calls.load(Ordering::Relaxed),
            "validator record cache"
        );
        Ok(out)
    }

    /// Direct put (tests).
    pub fn insert_for_test(&self, index: u64, record: Validator, epoch: u64) {
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.put(index, CacheEntry { record, epoch });
    }

    /// Clear the cache (tests / epoch boundary helper).
    pub fn clear(&self) {
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.clear();
    }
}

impl Default for ValidatorRecordCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Reject a raw >256 query at the p2p edge (never truncate).
///
/// # Errors
///
/// [`RecordsError::OverBound`] when `indices.len() > 256`.
/// [`RecordsError::Empty`] when empty.
pub fn check_record_request_bound(indices: &[u64]) -> Result<(), RecordsError> {
    if indices.is_empty() {
        return Err(RecordsError::Empty);
    }
    if indices.len() > MAX_VALIDATOR_RECORDS_PER_REQUEST {
        return Err(RecordsError::OverBound {
            bound: MAX_VALIDATOR_RECORDS_PER_REQUEST,
            got: indices.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_types::primitives::{BlsPublicKey, Epoch, Gwei, Root};

    fn dummy_validator(i: u64) -> Validator {
        Validator {
            pubkey: BlsPublicKey::from_array({
                let mut pk = [0u8; 48];
                pk[0..8].copy_from_slice(&i.to_le_bytes());
                pk[47] = 0x01;
                pk
            }),
            withdrawal_credentials: Root::from_array([0x01; 32]),
            effective_balance: Gwei::new(32_000_000_000),
            slashed: false,
            activation_eligibility_epoch: Epoch::new(0),
            activation_epoch: Epoch::new(0),
            exit_epoch: Epoch::new(u64::MAX),
            withdrawable_epoch: Epoch::new(u64::MAX),
        }
    }

    #[tokio::test]
    async fn over_bound_never_truncated() {
        let src = MapValidatorRecordSource::new();
        for i in 0..300 {
            src.insert(i, dummy_validator(i));
        }
        let err = src
            .fetch_records(&(0..257).collect::<Vec<_>>())
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RecordsError::OverBound {
                bound: 256,
                got: 257
            }
        ));
        assert_eq!(src.call_count(), 0, "must not call through on over-bound");
    }

    #[tokio::test]
    async fn cache_one_epoch_ttl_and_lru() {
        let cache = ValidatorRecordCache::new();
        let src = MapValidatorRecordSource::new();
        for i in 0..10 {
            src.insert(i, dummy_validator(i));
        }
        let m1 = cache.get_many(&[1, 2, 3], 5, &src).await.unwrap();
        assert_eq!(m1.len(), 3);
        assert_eq!(cache.fetch_call_count(), 1);
        // Same epoch → hits.
        let _ = cache.get_many(&[1, 2, 3], 5, &src).await.unwrap();
        assert_eq!(cache.fetch_call_count(), 1);
        // New epoch → TTL miss, re-fetch.
        let _ = cache.get_many(&[1], 6, &src).await.unwrap();
        assert_eq!(cache.fetch_call_count(), 2);
    }

    #[tokio::test]
    async fn check_bound_helper() {
        assert!(check_record_request_bound(&[0]).is_ok());
        assert!(check_record_request_bound(&[]).is_err());
        let many: Vec<u64> = (0..257).collect();
        assert!(matches!(
            check_record_request_bound(&many),
            Err(RecordsError::OverBound { got: 257, .. })
        ));
    }
}
