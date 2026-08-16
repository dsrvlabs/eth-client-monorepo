//! Chunked deletion with yield point and deadline (Architecture §7.3 / §7.4 / CC-46b).
//!
//! > **Preemption is bought by making the unit small — 512 keys, commit, yield,
//! > check, abandon — not by making the thread cheap.**
//!
//! Contended resource is the **single write transaction** redb permits. A prune
//! pass on any thread blocks write-behind for exactly as long as its batch
//! takes, regardless of thread priority (ADR P4-04). Chunking + `yield_now()`
//! + a wall-clock deadline is how P2 work yields the writer between units.
//!
//! On deadline exceed: **abandon the rest of the pass until the next tick**.
//! Marks are not advanced (caller owns that), so the next tick re-plans from
//! the durable watermark — idempotent because the watermark is the only state
//! the pass carries (`R-10`: falling behind on pruning is a disk problem;
//! falling behind on import is a consensus problem).
//!
//! Designed numbers (§7.4):
//! - `prune_chunk_keys = 512` — ~16 chunks for an 8 200-key column pass in the
//!   un-sharded fallback; one chunk's commit stays far under the 500 ms
//!   falsifier boundary.
//! - `prune_deadline = 2 s` — under one slot; at 16 chunks allows 125 ms/chunk.

use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::metrics::{PassLabels, PrunePass, StorageClass, StorageMetrics};
use crate::writer::{BackgroundChunk, WriterHandle};

use super::PruneError;

/// Default keys per P2 chunk (`storage.prune_chunk_keys`).
pub(crate) const DEFAULT_PRUNE_CHUNK_KEYS: usize = 512;
/// Default wall-clock deadline for one prune pass (`storage.prune_deadline`).
pub(crate) const DEFAULT_PRUNE_DEADLINE: Duration = Duration::from_secs(2);

/// Outcome of submitting a delete list through the chunked P2 loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkSubmitStats {
    /// Number of P2 chunks successfully committed.
    pub chunks: u64,
    /// Total keys submitted (sum of chunk sizes).
    pub keys_submitted: u64,
    /// Largest single chunk (asserted ≤ `prune_chunk_keys` at the submit site).
    pub max_chunk_keys: usize,
    /// `true` when the deadline was hit and the remainder was abandoned.
    pub abandoned: bool,
}

/// Arguments for [`submit_deletes_chunked`] (keeps the free function under the
/// clippy argument limit while remaining explicit at the call site).
pub(crate) struct ChunkSubmitArgs<'a> {
    pub writer: &'a WriterHandle,
    pub metrics: &'a StorageMetrics,
    pub class: StorageClass,
    pub pass: PrunePass,
    pub deletes: &'a [(String, Vec<u8>)],
    pub chunk_keys: usize,
    pub deadline: Duration,
    pub started: Instant,
}

/// Submit `deletes` as P2 chunks of at most `chunk_keys`, yielding between
/// commits and abandoning if `deadline` is exceeded relative to `started`.
///
/// On abandon: increments `cc_storage_prune_deadline_exceeded_total{pass}` by 1
/// and returns `abandoned: true`. Already-committed chunks stay committed;
/// the caller must **not** advance watermarks so the next tick resumes.
pub(crate) async fn submit_deletes_chunked(
    args: ChunkSubmitArgs<'_>,
) -> Result<ChunkSubmitStats, PruneError> {
    let ChunkSubmitArgs {
        writer,
        metrics,
        class,
        pass,
        deletes,
        chunk_keys,
        deadline,
        started,
    } = args;
    let chunk_keys = chunk_keys.max(1);
    let mut offset = 0usize;
    let mut stats = ChunkSubmitStats {
        chunks: 0,
        keys_submitted: 0,
        max_chunk_keys: 0,
        abandoned: false,
    };

    while offset < deletes.len() {
        // Check before starting a new chunk so we never begin work past the deadline.
        if started.elapsed() >= deadline {
            record_deadline_exceeded(metrics, pass);
            stats.abandoned = true;
            return Ok(stats);
        }

        let end = (offset + chunk_keys).min(deletes.len());
        let chunk_deletes = deletes[offset..end].to_vec();
        let n = chunk_deletes.len();
        // Acceptance: never hand the writer more than `chunk_keys` in one batch.
        debug_assert!(n <= chunk_keys);
        stats.max_chunk_keys = stats.max_chunk_keys.max(n);

        let (tx, rx) = oneshot::channel();
        let chunk = BackgroundChunk {
            class,
            puts: Vec::new(),
            deletes: chunk_deletes,
            done: Some(tx),
        };
        if !writer.try_submit_p2(chunk, metrics) {
            return Err(PruneError::QueueFull);
        }
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(PruneError::Writer(e)),
            Err(_) => return Err(PruneError::ShutDown),
        }

        offset = end;
        stats.chunks = stats.chunks.saturating_add(1);
        stats.keys_submitted = stats.keys_submitted.saturating_add(n as u64);

        // Yield so P0 write-behind can run between chunks (§7.3).
        tokio::task::yield_now().await;

        // Post-yield deadline check: abandon remainder until next tick.
        if offset < deletes.len() && started.elapsed() >= deadline {
            record_deadline_exceeded(metrics, pass);
            stats.abandoned = true;
            return Ok(stats);
        }
    }

    Ok(stats)
}

fn record_deadline_exceeded(metrics: &StorageMetrics, pass: PrunePass) {
    metrics
        .prune_deadline_exceeded
        .get_or_create(&PassLabels {
            pass: pass.as_str().to_owned(),
        })
        .inc();
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use crate::writer::{WriterBounds, WriterFaults, spawn_writer};
    use cc_store::engine::{Durability, Engine, EngineOptions};
    use cc_store::meta::TABLE_META;
    use prometheus_client::registry::Registry;
    use std::sync::Arc;
    use tokio::sync::watch;

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn tmp_engine() -> Engine {
        let dir = crate::test_tmpdir::unique_temp_dir("cc-storage-prune-chunk");
        std::fs::create_dir_all(&dir).unwrap();
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        let mut b = eng.batch();
        b.put(TABLE_META, b"__touch__", b"1");
        eng.commit(b).unwrap();
        eng
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunk_never_exceeds_bound_at_submit_site() {
        let eng = Arc::new(tmp_engine());
        let m = metrics();
        let (_tx, rx) = watch::channel(false);
        let writer = spawn_writer(
            Arc::clone(&eng),
            m.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            rx,
            false,
        );
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        // Seed keys so deletes are real.
        {
            let mut b = eng.batch();
            for i in 0..1_200u64 {
                b.put(TABLE_META, &i.to_be_bytes(), b"v");
            }
            eng.commit(b).unwrap();
        }

        let deletes: Vec<(String, Vec<u8>)> = (0..1_200u64)
            .map(|i| (TABLE_META.to_owned(), i.to_be_bytes().to_vec()))
            .collect();
        let chunk_keys = 512usize;
        let stats = submit_deletes_chunked(ChunkSubmitArgs {
            writer: &writer,
            metrics: &m,
            class: StorageClass::Meta,
            pass: PrunePass::Columns,
            deletes: &deletes,
            chunk_keys,
            deadline: Duration::from_secs(30),
            started: Instant::now(),
        })
        .await
        .unwrap();
        assert!(!stats.abandoned);
        assert_eq!(stats.keys_submitted, 1_200);
        assert!(
            stats.max_chunk_keys <= chunk_keys,
            "max chunk {} exceeds bound {chunk_keys}",
            stats.max_chunk_keys
        );
        assert_eq!(stats.chunks, 3); // 512 + 512 + 176
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn artificially_short_deadline_abandons() {
        let eng = Arc::new(tmp_engine());
        let m = metrics();
        let (_tx, rx) = watch::channel(false);
        let writer = spawn_writer(
            Arc::clone(&eng),
            m.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            rx,
            false,
        );
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        {
            let mut b = eng.batch();
            for i in 0..2_000u64 {
                b.put(TABLE_META, &i.to_be_bytes(), b"v");
            }
            eng.commit(b).unwrap();
        }
        let deletes: Vec<(String, Vec<u8>)> = (0..2_000u64)
            .map(|i| (TABLE_META.to_owned(), i.to_be_bytes().to_vec()))
            .collect();

        // Deadline already elapsed → abandon before first chunk, metric +1.
        let started = Instant::now() - Duration::from_secs(10);
        let before = m
            .prune_deadline_exceeded
            .get_or_create(&PassLabels {
                pass: PrunePass::Columns.as_str().to_owned(),
            })
            .get();
        let stats = submit_deletes_chunked(ChunkSubmitArgs {
            writer: &writer,
            metrics: &m,
            class: StorageClass::Meta,
            pass: PrunePass::Columns,
            deletes: &deletes,
            chunk_keys: 512,
            deadline: Duration::from_millis(1),
            started,
        })
        .await
        .unwrap();
        assert!(stats.abandoned);
        assert_eq!(stats.keys_submitted, 0);
        let after = m
            .prune_deadline_exceeded
            .get_or_create(&PassLabels {
                pass: PrunePass::Columns.as_str().to_owned(),
            })
            .get();
        assert_eq!(after, before + 1);
    }
}
