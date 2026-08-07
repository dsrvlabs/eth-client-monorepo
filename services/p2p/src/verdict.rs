//! One `Verdict` type and one `Verdict → MessageAcceptance` mapping (CC-22/4, §5.3).
//!
//! Every topic validator returns a [`#[must_use]`](Verdict) value. The **single**
//! call site in the swarm task converts it via [`to_message_acceptance`] and
//! reports the result to gossipsub. There is no catch-all arm on the acceptance
//! match — a new reason/acceptance variant fails to compile until classified
//! (Phase 1 §5.3 device).

use cc_libp2p::reexport::MessageAcceptance;
use cc_proto::p2p::{Acceptance, ImportResult, Reason, Verdict as ProtoVerdict};

/// Gossip-validation outcome. Validators return this; the swarm reports once.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// GossipSub class.
    pub acceptance: Acceptance,
    /// Detail reason (maps 1:1 onto Phase 1 `gossip_class()` + detail).
    pub reason: Reason,
    /// Import path outcome when known (blocks); `None` for non-import topics.
    pub import: ImportResult,
    /// Correlation id (block root / synthetic id). Empty when purely local.
    pub correlation_id: Vec<u8>,
}

impl Verdict {
    /// ACCEPT / VALID.
    pub fn accept(correlation_id: impl Into<Vec<u8>>) -> Self {
        Self {
            acceptance: Acceptance::Accept,
            reason: Reason::Valid,
            import: ImportResult::None,
            correlation_id: correlation_id.into(),
        }
    }

    /// IGNORE with an explicit reason.
    pub fn ignore(reason: Reason, correlation_id: impl Into<Vec<u8>>) -> Self {
        Self {
            acceptance: Acceptance::Ignore,
            reason,
            import: ImportResult::None,
            correlation_id: correlation_id.into(),
        }
    }

    /// REJECT with an explicit reason.
    pub fn reject(reason: Reason, correlation_id: impl Into<Vec<u8>>) -> Self {
        Self {
            acceptance: Acceptance::Reject,
            reason,
            import: ImportResult::None,
            correlation_id: correlation_id.into(),
        }
    }

    /// Internal IGNORE (our bug / shed / stall — never a peer penalty).
    pub fn internal(correlation_id: impl Into<Vec<u8>>) -> Self {
        Self::ignore(Reason::Internal, correlation_id)
    }

    /// Build from a chain-stream [`ProtoVerdict`].
    pub fn from_proto(v: &ProtoVerdict) -> Self {
        let acceptance = Acceptance::try_from(v.acceptance).unwrap_or(Acceptance::Ignore);
        let reason = Reason::try_from(v.reason).unwrap_or(Reason::Internal);
        let import = ImportResult::try_from(v.import).unwrap_or(ImportResult::None);
        Self {
            acceptance,
            reason,
            import,
            correlation_id: v.correlation_id.clone(),
        }
    }

    /// Wire form for the chain stream (p2p-authoritative families' upward msg).
    #[must_use]
    pub fn to_proto(&self) -> ProtoVerdict {
        ProtoVerdict {
            correlation_id: self.correlation_id.clone(),
            acceptance: self.acceptance as i32,
            reason: self.reason as i32,
            import: self.import as i32,
        }
    }

    /// Prometheus / log label for the acceptance class.
    #[must_use]
    pub fn acceptance_label(&self) -> &'static str {
        match self.acceptance {
            Acceptance::Accept => "accept",
            Acceptance::Reject => "reject",
            Acceptance::Ignore => "ignore",
            Acceptance::Unspecified => "unspecified",
        }
    }
}

/// Exhaustive `Verdict → MessageAcceptance` mapping — **no catch-all arm**.
///
/// Unspecified is treated as IGNORE (defensive; should never leave a validator).
#[must_use]
pub fn to_message_acceptance(verdict: &Verdict) -> MessageAcceptance {
    match verdict.acceptance {
        Acceptance::Accept => MessageAcceptance::Accept,
        Acceptance::Reject => MessageAcceptance::Reject,
        Acceptance::Ignore => MessageAcceptance::Ignore,
        Acceptance::Unspecified => MessageAcceptance::Ignore,
    }
}

/// Whether a late chain `Reject` after we already reported ACCEPT should
/// apply an application-score penalty (`import_invalid`) without re-reporting.
#[must_use]
pub fn is_late_import_reject(reported: &Verdict, late: &Verdict) -> bool {
    matches!(reported.acceptance, Acceptance::Accept)
        && matches!(late.acceptance, Acceptance::Reject)
}

/// Map a chain reason onto peer-manager [`GossipClass`] for late penalties.
///
/// `Internal` never becomes a network-facing penalty.
#[must_use]
pub fn gossip_class_for_reason(reason: Reason) -> crate::peer_manager::GossipClass {
    use crate::peer_manager::GossipClass;
    match reason {
        Reason::Valid => GossipClass::Accept,
        Reason::Invalid | Reason::InvalidSignature | Reason::NotDescendedFromFinalized => {
            GossipClass::Reject
        }
        Reason::Duplicate
        | Reason::UnknownParent
        | Reason::FutureSlot
        | Reason::DeferredDa
        | Reason::AlreadyKnown
        | Reason::Unspecified => GossipClass::Ignore,
        Reason::Internal => GossipClass::Internal,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn mapping_is_exhaustive_no_wildcard() {
        // Each Acceptance variant is named in `to_message_acceptance` — this
        // test locks the three wire classes + unspecified.
        assert!(matches!(
            to_message_acceptance(&Verdict::accept(vec![])),
            MessageAcceptance::Accept
        ));
        assert!(matches!(
            to_message_acceptance(&Verdict::reject(Reason::Invalid, vec![])),
            MessageAcceptance::Reject
        ));
        assert!(matches!(
            to_message_acceptance(&Verdict::ignore(Reason::Duplicate, vec![])),
            MessageAcceptance::Ignore
        ));
        let unspec = Verdict {
            acceptance: Acceptance::Unspecified,
            reason: Reason::Unspecified,
            import: ImportResult::None,
            correlation_id: vec![],
        };
        assert!(matches!(
            to_message_acceptance(&unspec),
            MessageAcceptance::Ignore
        ));
    }

    #[test]
    fn late_import_reject_detection() {
        let reported = Verdict::accept(vec![1]);
        let late = Verdict::reject(Reason::Invalid, vec![1]);
        assert!(is_late_import_reject(&reported, &late));
        assert!(!is_late_import_reject(&late, &reported));
        assert!(!is_late_import_reject(
            &reported,
            &Verdict::ignore(Reason::Duplicate, vec![1])
        ));
    }

    #[test]
    fn internal_is_never_reject_class() {
        assert_eq!(
            gossip_class_for_reason(Reason::Internal),
            crate::peer_manager::GossipClass::Internal
        );
    }

    #[test]
    fn proto_roundtrip() {
        let v = Verdict::reject(Reason::InvalidSignature, vec![9, 9]);
        let p = v.to_proto();
        let back = Verdict::from_proto(&p);
        assert_eq!(back.acceptance, Acceptance::Reject);
        assert_eq!(back.reason, Reason::InvalidSignature);
        assert_eq!(back.correlation_id, vec![9, 9]);
    }
}
