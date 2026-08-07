//! Spec `get_execution_requests_list` (Electra / EIP-7685).
//!
//! Flattens the typed [`ExecutionRequests`] container into the `Sequence[bytes]`
//! shape the Engine API expects: `request_type || ssz_serialize(request_data)`,
//! in ascending type order, omitting empty request lists.

use cc_types::operations::ExecutionRequests;
use cc_types::preset::Preset;
use ssz::Encode;

/// Spec `DEPOSIT_REQUEST_TYPE` — `Bytes1('0x00')`.
pub const DEPOSIT_REQUEST_TYPE: u8 = 0x00;
/// Spec `WITHDRAWAL_REQUEST_TYPE` — `Bytes1('0x01')`.
pub const WITHDRAWAL_REQUEST_TYPE: u8 = 0x01;
/// Spec `CONSOLIDATION_REQUEST_TYPE` — `Bytes1('0x02')`.
pub const CONSOLIDATION_REQUEST_TYPE: u8 = 0x02;

/// Spec `get_execution_requests_list`.
///
/// Returns `request_type || ssz_serialize(request_data)` for each non-empty
/// request list, in ascending type order (deposit → withdrawal → consolidation).
/// Empty lists are omitted entirely — never emitted as a type-prefixed empty
/// encoding (EIP-7685 / Engine API `-32602` on reverse).
pub fn get_execution_requests_list<P: Preset>(
    execution_requests: &ExecutionRequests<P>,
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(3);

    if !execution_requests.deposits.is_empty() {
        let body = execution_requests.deposits.as_ssz_bytes();
        let mut entry = Vec::with_capacity(1 + body.len());
        entry.push(DEPOSIT_REQUEST_TYPE);
        entry.extend_from_slice(&body);
        out.push(entry);
    }

    if !execution_requests.withdrawals.is_empty() {
        let body = execution_requests.withdrawals.as_ssz_bytes();
        let mut entry = Vec::with_capacity(1 + body.len());
        entry.push(WITHDRAWAL_REQUEST_TYPE);
        entry.extend_from_slice(&body);
        out.push(entry);
    }

    if !execution_requests.consolidations.is_empty() {
        let body = execution_requests.consolidations.as_ssz_bytes();
        let mut entry = Vec::with_capacity(1 + body.len());
        entry.push(CONSOLIDATION_REQUEST_TYPE);
        entry.extend_from_slice(&body);
        out.push(entry);
    }

    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_types::operations::{ConsolidationRequest, DepositRequest, WithdrawalRequest};
    use cc_types::preset::Mainnet;
    use cc_types::primitives::{BlsPublicKey, BlsSignature, ExecutionAddress, Gwei, Root};

    fn push_deposit(reqs: &mut ExecutionRequests<Mainnet>, index: u64) {
        reqs.deposits
            .push(DepositRequest {
                pubkey: BlsPublicKey::ZERO,
                withdrawal_credentials: Root::ZERO,
                amount: Gwei::new(32_000_000_000),
                signature: BlsSignature::ZERO,
                index,
            })
            .expect("under max deposits");
    }

    fn push_withdrawal(reqs: &mut ExecutionRequests<Mainnet>) {
        reqs.withdrawals
            .push(WithdrawalRequest {
                source_address: ExecutionAddress::ZERO,
                validator_pubkey: BlsPublicKey::ZERO,
                amount: Gwei::new(0),
            })
            .expect("under max withdrawals");
    }

    fn push_consolidation(reqs: &mut ExecutionRequests<Mainnet>) {
        reqs.consolidations
            .push(ConsolidationRequest {
                source_address: ExecutionAddress::ZERO,
                source_pubkey: BlsPublicKey::ZERO,
                target_pubkey: BlsPublicKey::ZERO,
            })
            .expect("under max consolidations");
    }

    /// Encoder never produces descending type order.
    #[test]
    fn encoder_never_produces_descending_type_order() {
        let mut reqs = ExecutionRequests::<Mainnet>::default();
        push_consolidation(&mut reqs);
        push_withdrawal(&mut reqs);
        push_deposit(&mut reqs, 0);
        let list = get_execution_requests_list(&reqs);
        let types: Vec<u8> = list.iter().map(|e| e[0]).collect();
        assert!(
            types.windows(2).all(|w| w[0] < w[1]),
            "types must be strictly ascending, got {types:?}"
        );
        assert_eq!(
            types,
            vec![
                DEPOSIT_REQUEST_TYPE,
                WITHDRAWAL_REQUEST_TYPE,
                CONSOLIDATION_REQUEST_TYPE
            ]
        );
    }

    /// Encoder never emits a duplicated type.
    #[test]
    fn encoder_never_produces_duplicated_type() {
        let mut reqs = ExecutionRequests::<Mainnet>::default();
        push_deposit(&mut reqs, 0);
        push_deposit(&mut reqs, 1);
        push_withdrawal(&mut reqs);
        let list = get_execution_requests_list(&reqs);
        let types: Vec<u8> = list.iter().map(|e| e[0]).collect();
        let mut seen = std::collections::HashSet::new();
        for t in &types {
            assert!(seen.insert(*t), "duplicate request type {t:#04x}");
        }
    }

    /// Encoder never emits a type-prefixed empty request list.
    #[test]
    fn encoder_never_emits_empty_request_list() {
        let mut reqs = ExecutionRequests::<Mainnet>::default();
        // Only deposits non-empty; withdrawals and consolidations stay empty.
        push_deposit(&mut reqs, 0);
        let list = get_execution_requests_list(&reqs);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0][0], DEPOSIT_REQUEST_TYPE);
        // No entry for type 0x01 or 0x02 at all.
        assert!(list.iter().all(|e| e[0] == DEPOSIT_REQUEST_TYPE));
        // Payload after the type byte is non-empty (SSZ of the list).
        assert!(
            list[0].len() > 1,
            "type-prefixed empty list must not appear"
        );

        // All empty → empty Sequence[bytes].
        let empty = get_execution_requests_list(&ExecutionRequests::<Mainnet>::default());
        assert!(empty.is_empty());
    }
}
