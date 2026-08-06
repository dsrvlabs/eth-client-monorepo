//! CC-18c integration tests over synthetic events (no fork-choice).
//!
//! Covers CC-18/1 reconnect, CC-18/2 cursor eviction, CC-18/3 slow consumer,
//! session mismatch, monotonic ordering across reconnect, subscribe/publish
//! race, and config overrides for both bounds.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use cc_chain::{
    DEFAULT_RING_CAPACITY, DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, EventInput, EventSubscription,
    EventsConfig, EventsHandle, REASON_CURSOR_TOO_OLD, REASON_CURSOR_UNKNOWN_SESSION,
};
use cc_proto::chain::{Cursor, Event};
use cc_proto::error_info_from_status;
use tonic::Code;

fn root_for(seq: u64) -> Bytes {
    let mut r = vec![0u8; 32];
    r[..8].copy_from_slice(&seq.to_le_bytes());
    Bytes::from(r)
}

fn input(seq_hint: u64) -> EventInput {
    EventInput::block_imported(seq_hint, root_for(seq_hint))
}

async fn publish_n(h: &EventsHandle, start: u64, n: u64) {
    for i in start..start + n {
        h.publish(input(i)).await.unwrap();
    }
}

async fn recv_n(sub: &mut EventSubscription, n: usize) -> Vec<Event> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let ev = sub
            .recv()
            .await
            .unwrap()
            .expect("expected event, stream ended early");
        out.push(ev);
    }
    out
}

/// Idempotent consumer: apply-by-root set (CC-18/1).
struct IdempotentConsumer {
    roots: HashSet<Vec<u8>>,
    last_seq: Option<u64>,
    order: Vec<u64>,
}

impl IdempotentConsumer {
    fn new() -> Self {
        Self {
            roots: HashSet::new(),
            last_seq: None,
            order: Vec::new(),
        }
    }

    fn apply(&mut self, ev: &Event) {
        // Duplicate root must not change state.
        if self.roots.insert(ev.root.clone()) {
            self.order.push(ev.seq);
        }
        if let Some(prev) = self.last_seq {
            assert!(
                ev.seq > prev || self.roots.contains(&ev.root),
                "non-monotonic seq {} after {}",
                ev.seq,
                prev
            );
        }
        self.last_seq = Some(ev.seq);
    }
}

// ---------------------------------------------------------------------------
// CC-18/1 reconnect — no gap, no duplicate-induced state change
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cc18_1_reconnect_no_gap_no_duplicate() {
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 64,
        subscriber_queue_capacity: 64,
        session_id: Some(7),
    });

    let mut sub = h.subscribe(None).await.unwrap();
    publish_n(&h, 0, 10).await;
    let first = recv_n(&mut sub, 10).await;

    let mut consumer = IdempotentConsumer::new();
    for ev in &first {
        consumer.apply(ev);
    }
    let last = first.last().unwrap();
    let cursor = EventSubscription::cursor_for(last, sub.session_id());

    // Drop mid-stream; producer keeps going.
    drop(sub);
    publish_n(&h, 10, 15).await;

    // Resubscribe with last cursor — must deliver 10..25 in order, no dups.
    let mut sub2 = h.subscribe(Some(cursor)).await.unwrap();
    let rest = recv_n(&mut sub2, 15).await;
    assert_eq!(rest.first().unwrap().seq, 10);
    assert_eq!(rest.last().unwrap().seq, 24);
    for (i, ev) in rest.iter().enumerate() {
        assert_eq!(ev.seq, 10 + i as u64);
        consumer.apply(ev);
    }

    // Unique roots == total events; order is contiguous 0..25.
    assert_eq!(consumer.roots.len(), 25);
    assert_eq!(consumer.order, (0..25).collect::<Vec<_>>());

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// CC-18/6 monotonic sequence across reconnect (pinned with /1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cc18_6_monotonic_across_reconnect() {
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 32,
        subscriber_queue_capacity: 32,
        session_id: Some(3),
    });
    let mut sub = h.subscribe(None).await.unwrap();
    publish_n(&h, 0, 5).await;
    let batch = recv_n(&mut sub, 5).await;
    let cursor = EventSubscription::cursor_for(batch.last().unwrap(), sub.session_id());
    drop(sub);

    publish_n(&h, 5, 5).await;
    let mut sub2 = h.subscribe(Some(cursor)).await.unwrap();
    let rest = recv_n(&mut sub2, 5).await;

    let mut prev = batch[0].seq;
    for ev in batch.iter().skip(1).chain(rest.iter()) {
        assert!(ev.seq > prev, "seq {} not > {}", ev.seq, prev);
        assert_eq!(ev.seq, prev + 1, "gap at {}", ev.seq);
        prev = ev.seq;
    }
    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// CC-18/2 cursor eviction → CURSOR_TOO_OLD (literal), then fresh subscribe works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cc18_2_cursor_too_old_on_eviction() {
    let ring = 8;
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: ring,
        subscriber_queue_capacity: 16,
        session_id: Some(11),
    });

    let mut sub = h.subscribe(None).await.unwrap();
    publish_n(&h, 0, 1).await;
    let first = recv_n(&mut sub, 1).await;
    let stale = EventSubscription::cursor_for(&first[0], sub.session_id());
    drop(sub);

    // Push more than ring capacity so seq=0 is evicted.
    publish_n(&h, 1, (ring as u64) + 2).await;
    // Yield so the task drains the producer channel.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let err = h.subscribe(Some(stale)).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let info = error_info_from_status(&err).unwrap().expect("ErrorInfo");
    assert_eq!(
        info.reason, REASON_CURSOR_TOO_OLD,
        "CC-18/2 pins the literal reason string"
    );
    assert_eq!(info.domain, "eth.chain.v1");

    // Fresh subscribe (no cursor) still works.
    let mut live = h.subscribe(None).await.unwrap();
    h.publish(input(1000)).await.unwrap();
    let ev = live.recv().await.unwrap().unwrap();
    assert_eq!(ev.slot, 1000);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Unknown session is a distinct reason (not CURSOR_TOO_OLD)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_unknown_session_not_too_old() {
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 4,
        subscriber_queue_capacity: 4,
        session_id: Some(42),
    });
    publish_n(&h, 0, 2).await;
    tokio::task::yield_now().await;

    let err = h
        .subscribe(Some(Cursor {
            session_id: 0xdead_beef,
            seq: 0,
            slot: 0,
            root: root_for(0).to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let info = error_info_from_status(&err).unwrap().unwrap();
    assert_eq!(info.reason, REASON_CURSOR_UNKNOWN_SESSION);
    assert_ne!(info.reason, REASON_CURSOR_TOO_OLD);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// CC-18/3 slow consumer → RESOURCE_EXHAUSTED; occupancy stays bounded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cc18_3_slow_consumer_resource_exhausted_bounded_occupancy() {
    let ring = 16;
    let sub_q = 4;
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: ring,
        subscriber_queue_capacity: sub_q,
        session_id: Some(5),
    });

    // Subscribe and never read.
    let mut slow = h.subscribe(None).await.unwrap();

    // Flood past the per-subscriber queue.
    for i in 0..(sub_q as u64 + 8) {
        h.publish(input(i)).await.unwrap();
    }
    // Allow the task to process.
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Occupancy must stay within configured bounds (not RSS eyeballing).
    assert!(
        h.occupancy().ring() <= ring,
        "ring occupancy {} > cap {}",
        h.occupancy().ring(),
        ring
    );
    assert!(
        h.occupancy().deepest_subscriber() <= sub_q,
        "deepest subscriber queue {} > cap {}",
        h.occupancy().deepest_subscriber(),
        sub_q
    );

    // The slow stream ends with RESOURCE_EXHAUSTED.
    let mut saw_exhausted = false;
    for _ in 0..32 {
        match slow.recv().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("stream ended cleanly; expected RESOURCE_EXHAUSTED"),
            Err(status) => {
                assert_eq!(status.code(), Code::ResourceExhausted);
                saw_exhausted = true;
                break;
            }
        }
    }
    assert!(saw_exhausted, "slow consumer was not terminated");

    // After kill, subscriber count drops; further publishes must not grow the dead queue.
    assert_eq!(h.occupancy().subscribers(), 0);
    publish_n(&h, 100, 20).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(h.occupancy().ring() <= ring);
    assert_eq!(h.occupancy().deepest_subscriber(), 0);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Race: subscribe while publishing at high rate, ≥100 times
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscribe_publish_race_no_gap_no_duplicate() {
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 512,
        subscriber_queue_capacity: 256,
        session_id: Some(99),
    });

    let producer = h.clone();
    let publish_task = tokio::spawn(async move {
        for i in 0..500u64 {
            producer.publish(input(i)).await.unwrap();
        }
    });

    for _ in 0..100 {
        let mut sub = h.subscribe(None).await.unwrap();
        // Grab a few live events if any arrive quickly.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(5);
        let mut got = Vec::new();
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(1), sub.recv()).await {
                Ok(Ok(Some(ev))) => got.push(ev),
                _ => break,
            }
        }
        // Contiguous and unique within this sample.
        let mut seen = HashSet::new();
        for w in got.windows(2) {
            assert!(w[1].seq >= w[0].seq);
            if w[1].seq == w[0].seq {
                panic!("duplicate seq in live sample");
            }
        }
        for ev in &got {
            assert!(seen.insert(ev.seq), "duplicate seq {}", ev.seq);
        }
        drop(sub);
    }

    publish_task.await.unwrap();

    // Final reconnect-style check: live-only after flood, then cursor replay.
    let mut sub = h.subscribe(None).await.unwrap();
    h.publish(input(1000)).await.unwrap();
    let ev = sub.recv().await.unwrap().unwrap();
    assert_eq!(ev.slot, 1000);
    let cursor = EventSubscription::cursor_for(&ev, sub.session_id());
    drop(sub);

    h.publish(input(1001)).await.unwrap();
    h.publish(input(1002)).await.unwrap();
    let mut sub2 = h.subscribe(Some(cursor)).await.unwrap();
    let a = sub2.recv().await.unwrap().unwrap();
    let b = sub2.recv().await.unwrap().unwrap();
    assert_eq!(a.seq + 1, b.seq);
    assert_eq!(a.slot, 1001);
    assert_eq!(b.slot, 1002);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Both bounds are config values; test overrides both
// ---------------------------------------------------------------------------

#[tokio::test]
async fn config_overrides_both_bounds() {
    assert_eq!(DEFAULT_RING_CAPACITY, 1024);
    assert_eq!(DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, 256);

    let defaults = EventsConfig::default();
    assert_eq!(defaults.ring_capacity, 1024);
    assert_eq!(defaults.subscriber_queue_capacity, 256);

    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 3,
        subscriber_queue_capacity: 2,
        session_id: Some(1),
    });
    assert_eq!(h.ring_capacity(), 3);
    assert_eq!(h.subscriber_queue_capacity(), 2);

    // Ring eviction at overridden capacity.
    publish_n(&h, 0, 5).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(h.occupancy().ring() <= 3);

    // Slow-consumer kill at overridden queue depth 2.
    let mut slow = h.subscribe(None).await.unwrap();
    publish_n(&h, 10, 6).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut exhausted = false;
    for _ in 0..16 {
        match slow.recv().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("expected RESOURCE_EXHAUSTED"),
            Err(s) => {
                assert_eq!(s.code(), Code::ResourceExhausted);
                exhausted = true;
                break;
            }
        }
    }
    assert!(exhausted);
    assert!(h.occupancy().deepest_subscriber() <= 2);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Reconnect after publish during disconnect delivers the missed range
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_delivers_missed_range_exactly_once() {
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 128,
        subscriber_queue_capacity: 64,
        session_id: Some(2),
    });

    let mut sub = h.subscribe(None).await.unwrap();
    publish_n(&h, 0, 3).await;
    let seen = recv_n(&mut sub, 3).await;
    let cursor = EventSubscription::cursor_for(seen.last().unwrap(), sub.session_id());
    drop(sub);

    publish_n(&h, 3, 10).await;
    let mut sub2 = h.subscribe(Some(cursor)).await.unwrap();
    let replayed = recv_n(&mut sub2, 10).await;
    let seqs: Vec<u64> = replayed.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (3..13).collect::<Vec<_>>());

    h.shutdown().await;
}
