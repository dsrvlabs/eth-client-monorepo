//! CC-22b — committed message-id fixture against specs pin v1.7.0-alpha.13.
//!
//! Cases are generated from the Altair topic-aware preimage:
//! `SHA256(domain ‖ uint64_le(len(topic)) ‖ topic ‖ payload)[:20]`.
//! At least one case uses `MESSAGE_DOMAIN_INVALID_SNAPPY` over raw bytes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

use cc_p2p::gossip::{
    MESSAGE_DOMAIN_INVALID_SNAPPY, MESSAGE_DOMAIN_VALID_SNAPPY, MESSAGE_ID_SIZE,
    compute_message_id, message_id_invalid_snappy, message_id_valid_snappy,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FixtureFile {
    spec_release: String,
    constants_read_date: String,
    domains: FixtureDomains,
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureDomains {
    #[serde(rename = "MESSAGE_DOMAIN_VALID_SNAPPY")]
    valid: String,
    #[serde(rename = "MESSAGE_DOMAIN_INVALID_SNAPPY")]
    invalid: String,
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    name: String,
    topic: String,
    payload_hex: String,
    domain: String,
    id_hex: String,
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/message-id.json")
}

fn load_fixture() -> FixtureFile {
    let text = std::fs::read_to_string(fixture_path()).expect("message-id.json");
    serde_json::from_str(&text).expect("parse message-id.json")
}

fn decode_hex(hex: &str) -> Vec<u8> {
    if hex.is_empty() {
        return Vec::new();
    }
    hex::decode(hex).unwrap_or_else(|e| panic!("hex decode {hex:?}: {e}"))
}

fn parse_domain_hex(s: &str) -> [u8; 4] {
    let raw = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(raw).expect("domain hex");
    assert_eq!(bytes.len(), 4, "domain must be 4 bytes");
    [bytes[0], bytes[1], bytes[2], bytes[3]]
}

#[test]
fn fixture_header_records_spec_release_and_read_date() {
    let f = load_fixture();
    assert_eq!(f.spec_release, "v1.7.0-alpha.13");
    assert!(
        !f.constants_read_date.is_empty(),
        "constants_read_date must be present (§14/1)"
    );
    // Domain constants from the fixture header match the compiled values.
    assert_eq!(
        parse_domain_hex(&f.domains.valid),
        MESSAGE_DOMAIN_VALID_SNAPPY
    );
    assert_eq!(
        parse_domain_hex(&f.domains.invalid),
        MESSAGE_DOMAIN_INVALID_SNAPPY
    );
}

#[test]
fn committed_message_id_fixture_passes() {
    let f = load_fixture();
    assert!(f.cases.len() >= 2, "fixture must carry multiple triples");

    let mut saw_invalid = false;
    for case in &f.cases {
        let payload = decode_hex(&case.payload_hex);
        let expected = decode_hex(&case.id_hex);
        assert_eq!(
            expected.len(),
            MESSAGE_ID_SIZE,
            "case {}: id must be 20 bytes",
            case.name
        );

        let domain = match case.domain.as_str() {
            "valid" => MESSAGE_DOMAIN_VALID_SNAPPY,
            "invalid" => {
                saw_invalid = true;
                MESSAGE_DOMAIN_INVALID_SNAPPY
            }
            other => panic!("case {}: unknown domain {other}", case.name),
        };

        let got = compute_message_id(&case.topic, &payload, domain);
        assert_eq!(
            got.as_slice(),
            expected.as_slice(),
            "case {}: id mismatch\n  topic={}\n  domain={}\n  payload_hex={}",
            case.name,
            case.topic,
            case.domain,
            case.payload_hex
        );

        // Helpers agree with the explicit domain path.
        let via_helper = match case.domain.as_str() {
            "valid" => message_id_valid_snappy(&case.topic, &payload),
            "invalid" => message_id_invalid_snappy(&case.topic, &payload),
            _ => unreachable!(),
        };
        assert_eq!(got, via_helper, "case {}: helper mismatch", case.name);
    }

    assert!(
        saw_invalid,
        "fixture must include at least one invalid-snappy case"
    );
}

#[test]
fn valid_snappy_id_differs_from_invalid_over_same_bytes() {
    let topic = "/eth2/c6ecb76c/beacon_block/ssz_snappy";
    let payload = b"same-bytes";
    assert_ne!(
        message_id_valid_snappy(topic, payload),
        message_id_invalid_snappy(topic, payload)
    );
}
