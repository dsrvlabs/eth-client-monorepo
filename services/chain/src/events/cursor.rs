//! Resume-cursor validation against the event ring (ADR-P1-11).

use cc_proto::chain::Cursor;
use tonic::Status;

use super::cursor_status;
use super::ring::EventRing;

/// `google.rpc.ErrorInfo.reason` when the cursor's sequence was evicted from the ring.
pub const REASON_CURSOR_TOO_OLD: &str = "CURSOR_TOO_OLD";

/// `google.rpc.ErrorInfo.reason` when the cursor was minted by a different process incarnation.
pub const REASON_CURSOR_UNKNOWN_SESSION: &str = "CURSOR_UNKNOWN_SESSION";

/// Validate `cursor` against `ring`. On success the caller may replay from `cursor.seq + 1`.
///
/// Order of checks matters: **session** is rejected before **too-old**, so a stale
/// session never surfaces as `CURSOR_TOO_OLD` (keeps the CC-18/2 pin meaningful).
pub fn validate_cursor(ring: &EventRing, cursor: &Cursor) -> Result<(), Status> {
    if cursor.session_id != ring.session_id() {
        return Err(cursor_status(
            REASON_CURSOR_UNKNOWN_SESSION,
            "cursor session_id does not match this process; server may have restarted",
        ));
    }

    // Future cursor: last-seen seq must be strictly less than next_seq (or equal only
    // when the stream is empty and seq == 0 is treated as "nothing seen yet").
    if ring.next_seq() == 0 {
        // No events published in this session. Only seq == 0 is accepted (empty replay).
        if cursor.seq != 0 {
            return Err(Status::invalid_argument(
                "cursor seq is ahead of an empty event stream",
            ));
        }
        return Ok(());
    }

    if cursor.seq >= ring.next_seq() {
        return Err(Status::invalid_argument(
            "cursor seq is ahead of the event stream",
        ));
    }

    // Eviction: resume point is no longer in (or just before) the ring.
    // `CURSOR_TOO_OLD` when `cursor.seq + 1 < ring.front().seq`.
    if let Some(front) = ring.front() {
        let resume_from = cursor.seq.saturating_add(1);
        if resume_from < front.seq {
            return Err(cursor_status(
                REASON_CURSOR_TOO_OLD,
                "cursor fell out of the event ring; fall back to GetHead and resubscribe",
            ));
        }
    }

    // slot/root are validated against the ring entry when it is still retained.
    if let Some(stored) = ring.get(cursor.seq)
        && (stored.slot != cursor.slot || stored.root.as_ref() != cursor.root.as_slice())
    {
        return Err(Status::invalid_argument(
            "cursor slot/root does not match the ring entry at that seq",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use bytes::Bytes;
    use cc_proto::error_info_from_status;
    use tonic::Code;

    use super::*;
    use crate::events::{EventInput, EventRing};

    fn push_n(ring: &mut EventRing, n: u64) {
        for i in 0..n {
            ring.push(EventInput::block_imported(i, Bytes::from(vec![i as u8])));
        }
    }

    #[test]
    fn session_mismatch_before_too_old() {
        let mut ring = EventRing::new(2, 1);
        push_n(&mut ring, 4); // front seq = 2
        let err = validate_cursor(
            &ring,
            &Cursor {
                session_id: 999,
                seq: 0, // would also be too old
                slot: 0,
                root: vec![0],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        let info = error_info_from_status(&err).unwrap().unwrap();
        assert_eq!(info.reason, REASON_CURSOR_UNKNOWN_SESSION);
    }

    #[test]
    fn eviction_is_cursor_too_old() {
        let mut ring = EventRing::new(2, 1);
        push_n(&mut ring, 4); // retained seq 2,3
        let err = validate_cursor(
            &ring,
            &Cursor {
                session_id: 1,
                seq: 0,
                slot: 0,
                root: vec![0],
            },
        )
        .unwrap_err();
        let info = error_info_from_status(&err).unwrap().unwrap();
        assert_eq!(info.reason, REASON_CURSOR_TOO_OLD);
    }

    #[test]
    fn boundary_cursor_just_before_front_is_ok() {
        let mut ring = EventRing::new(2, 1);
        push_n(&mut ring, 4); // front = 2
        // last seen = 1 (evicted); resume from 2 — still valid
        validate_cursor(
            &ring,
            &Cursor {
                session_id: 1,
                seq: 1,
                slot: 1,
                root: vec![1],
            },
        )
        .unwrap();
    }

    #[test]
    fn slot_root_mismatch_rejected() {
        let mut ring = EventRing::new(4, 1);
        push_n(&mut ring, 2);
        let err = validate_cursor(
            &ring,
            &Cursor {
                session_id: 1,
                seq: 0,
                slot: 99,
                root: vec![0],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
