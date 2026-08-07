//! Committed offline vector for `get_execution_requests_list` (CC-32 /3).
//!
//! Fixture: `tests/vectors/execution_requests.json`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use cc_state_transition::get_execution_requests_list;
use cc_types::operations::{
    ConsolidationRequest, DepositRequest, ExecutionRequests, WithdrawalRequest,
};
use cc_types::preset::Mainnet;
use cc_types::primitives::{BlsPublicKey, BlsSignature, ExecutionAddress, Gwei, Root};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct VectorFile {
    cases: Vec<VectorCase>,
}

#[derive(Debug, Deserialize)]
struct VectorCase {
    name: String,
    input: CaseInput,
    expected_hex: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CaseInput {
    deposits: usize,
    withdrawals: usize,
    consolidations: usize,
}

fn hex_decode(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "hex length must be even: {s}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex byte"))
        .collect()
}

fn build_requests(input: &CaseInput) -> ExecutionRequests<Mainnet> {
    let mut reqs = ExecutionRequests::<Mainnet>::default();
    for _ in 0..input.deposits {
        reqs.deposits
            .push(DepositRequest {
                pubkey: BlsPublicKey::ZERO,
                withdrawal_credentials: Root::ZERO,
                amount: Gwei::new(0),
                signature: BlsSignature::ZERO,
                index: 0,
            })
            .expect("under max deposits");
    }
    for _ in 0..input.withdrawals {
        reqs.withdrawals
            .push(WithdrawalRequest {
                source_address: ExecutionAddress::ZERO,
                validator_pubkey: BlsPublicKey::ZERO,
                amount: Gwei::new(0),
            })
            .expect("under max withdrawals");
    }
    for _ in 0..input.consolidations {
        reqs.consolidations
            .push(ConsolidationRequest {
                source_address: ExecutionAddress::ZERO,
                source_pubkey: BlsPublicKey::ZERO,
                target_pubkey: BlsPublicKey::ZERO,
            })
            .expect("under max consolidations");
    }
    reqs
}

/// CC-32 /3: encoder matches the committed offline vector.
#[test]
fn execution_requests_list_vector() {
    let raw = include_str!("vectors/execution_requests.json");
    let file: VectorFile = serde_json::from_str(raw).expect("parse execution_requests.json");
    assert!(
        file.cases.len() >= 3,
        "vector must carry at least the three shapes (all non-empty; one empty omitted; all empty)"
    );

    for case in &file.cases {
        let reqs = build_requests(&case.input);
        let got = get_execution_requests_list(&reqs);
        let expected: Vec<Vec<u8>> = case.expected_hex.iter().map(|h| hex_decode(h)).collect();
        assert_eq!(
            got, expected,
            "case '{}': encoder output mismatch\n  got:  {:02x?}\n  want: {:02x?}",
            case.name, got, expected
        );
    }
}
