//! `ssz_generic` runner (CC-10f / Architecture §10.2–§10.3).
//!
//! - One `#[test]` per handler under `tests/general/phase0/ssz_generic/`.
//! - Ad-hoc container types live **here**, not in `crates/types` (so CC-10e's
//!   set-equality registry stays pure).
//! - `valid/`: decode → `value.yaml` equality → re-encode byte equality →
//!   `hash_tree_root` vs `meta.yaml`.
//! - `invalid/`: decode must `Err`; panics are caught and reported with the
//!   case name (never a bare runner abort).
//! - Unrecognised case-name grammar panics with the name quoted.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    non_snake_case
)]

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::str::FromStr;

use alloy_primitives::{U128, U256};
use cc_spec_tests::{Case, Vectors, assert_handler_coverage};
use cc_types::primitives::{Root, parse_hex_bytes};
use serde::Deserialize;
use ssz::{Decode, DecodeError, Encode};
use ssz_derive::{Decode as DecodeDerive, Encode as EncodeDerive};
use ssz_types::{BitList, BitVector, FixedVector, VariableList};
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash as TreeHashDerive;
use typenum::{
    U1, U2, U3, U4, U5, U6, U7, U8, U9, U15, U16, U17, U31, U32, U33, U128 as TU128, U256 as TU256,
    U511, U512, U513, U1024,
};

// ---------------------------------------------------------------------------
// Runner constants
// ---------------------------------------------------------------------------

/// Preset / fork segment for the general-preset suite (from `spec-vectors-layout.md`).
const PRESET: &str = "general";
const FORK: &str = "phase0";
const RUNNER: &str = "ssz_generic";

/// Handlers this runner implements (CC-10f AC). Progressive / union handlers
/// present on disk under this pin are Fulu-out-of-scope (D1 / P1-16) and are
/// excluded from coverage via [`OUT_OF_SCOPE_HANDLERS`].
const HANDLERS: &[&str] = &[
    "basic_vector",
    "bitlist",
    "bitvector",
    "boolean",
    "containers",
    "uints",
];

/// On-disk handlers we intentionally do not exercise (EIP-7688 / EIP-7495).
const OUT_OF_SCOPE_HANDLERS: &[&str] = &[
    "basic_progressive_list",
    "compatible_unions",
    "progressive_bitlist",
    "progressive_containers",
];

/// Progressive container type names that appear under `containers/` but use
/// EIP-7688 progressive SSZ (out of Fulu scope).
const OUT_OF_SCOPE_CONTAINER_TYPES: &[&str] = &["ProgressiveBitsStruct", "ProgressiveTestStruct"];

// ---------------------------------------------------------------------------
// Ad-hoc types (runner-local — NOT in crates/types/src)
// ---------------------------------------------------------------------------

type ByteList256 = VariableList<u8, TU256>;
type Uint16List1024 = VariableList<u16, U1024>;
type Uint16List128 = VariableList<u16, TU128>;

/// Spec generator `SingleFieldTestStruct`.
#[derive(Debug, Clone, PartialEq, Eq, EncodeDerive, DecodeDerive, TreeHashDerive)]
#[allow(non_snake_case)]
struct SingleFieldTestStruct {
    A: u8,
}

/// Spec generator `SmallTestStruct`.
#[derive(Debug, Clone, PartialEq, Eq, EncodeDerive, DecodeDerive, TreeHashDerive)]
#[allow(non_snake_case)]
struct SmallTestStruct {
    A: u16,
    B: u16,
}

/// Spec generator `FixedTestStruct`.
#[derive(Debug, Clone, PartialEq, Eq, EncodeDerive, DecodeDerive, TreeHashDerive)]
#[allow(non_snake_case)]
struct FixedTestStruct {
    A: u8,
    B: u64,
    C: u32,
}

/// Spec generator `VarTestStruct`.
#[derive(Debug, Clone, PartialEq, Eq, EncodeDerive, DecodeDerive, TreeHashDerive)]
#[allow(non_snake_case)]
struct VarTestStruct {
    A: u16,
    B: Uint16List1024,
    C: u8,
}

/// Spec generator `ComplexTestStruct`.
#[derive(Debug, Clone, PartialEq, Eq, EncodeDerive, DecodeDerive, TreeHashDerive)]
#[allow(non_snake_case)]
struct ComplexTestStruct {
    A: u16,
    B: Uint16List128,
    C: u8,
    D: ByteList256,
    E: VarTestStruct,
    F: FixedVector<FixedTestStruct, U4>,
    G: FixedVector<VarTestStruct, U2>,
}

/// Spec generator `BitsStruct`.
#[derive(Debug, Clone, PartialEq, Eq, EncodeDerive, DecodeDerive, TreeHashDerive)]
#[allow(non_snake_case)]
struct BitsStruct {
    A: BitList<U5>,
    B: BitVector<U2>,
    C: BitVector<U1>,
    D: BitList<U6>,
    E: BitVector<U8>,
}

// ---------------------------------------------------------------------------
// YAML helpers for value.yaml equality
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct MetaYaml {
    root: String,
}

#[derive(Debug, Deserialize)]
struct SingleFieldYaml {
    A: u8,
}

#[derive(Debug, Deserialize)]
struct SmallYaml {
    A: u16,
    B: u16,
}

#[derive(Debug, Deserialize)]
struct FixedYaml {
    A: u8,
    B: u64,
    C: u32,
}

#[derive(Debug, Deserialize)]
struct VarYaml {
    A: u16,
    B: Vec<u16>,
    C: u8,
}

#[derive(Debug, Deserialize)]
struct ComplexYaml {
    A: u16,
    B: Vec<u16>,
    C: u8,
    D: String,
    E: VarYaml,
    F: Vec<FixedYaml>,
    G: Vec<VarYaml>,
}

#[derive(Debug, Deserialize)]
struct BitsYaml {
    A: String,
    B: String,
    C: String,
    D: String,
    E: String,
}

fn open_vectors() -> Vectors {
    Vectors::open().expect("spec vector cache must be present; run scripts/fetch-spec-vectors.sh")
}

fn parse_root(hex: &str) -> Root {
    let bytes = parse_hex_bytes::<32>(hex).unwrap_or_else(|e| panic!("bad root hex {hex}: {e}"));
    Root::from_array(bytes)
}

fn parse_hex_vec(s: &str) -> Vec<u8> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.is_empty() {
        return Vec::new();
    }
    if !hex.len().is_multiple_of(2) {
        panic!("odd-length hex {s}");
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex nibble"))
        .collect()
}

fn yaml_u64(v: &serde_yaml::Value) -> u64 {
    match v {
        serde_yaml::Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|i| i as u64))
            .unwrap_or_else(|| panic!("expected u64 number, got {n}")),
        serde_yaml::Value::String(s) => s.parse::<u64>().unwrap_or_else(|e| panic!("u64 {s}: {e}")),
        other => panic!("expected u64 yaml, got {other:?}"),
    }
}

fn yaml_u128(v: &serde_yaml::Value) -> U128 {
    match v {
        serde_yaml::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                U128::from(u)
            } else {
                panic!("expected u128-compatible number, got {n}")
            }
        }
        serde_yaml::Value::String(s) => {
            U128::from_str(s).unwrap_or_else(|e| panic!("U128 {s}: {e}"))
        }
        other => panic!("expected u128 yaml, got {other:?}"),
    }
}

fn yaml_u256(v: &serde_yaml::Value) -> U256 {
    match v {
        serde_yaml::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                U256::from(u)
            } else {
                panic!("expected u256-compatible number, got {n}")
            }
        }
        serde_yaml::Value::String(s) => {
            U256::from_str(s).unwrap_or_else(|e| panic!("U256 {s}: {e}"))
        }
        other => panic!("expected u256 yaml, got {other:?}"),
    }
}

fn var_from_yaml(y: VarYaml) -> VarTestStruct {
    VarTestStruct {
        A: y.A,
        B: VariableList::new(y.B).expect("VarTestStruct.B within 1024"),
        C: y.C,
    }
}

fn fixed_from_yaml(y: FixedYaml) -> FixedTestStruct {
    FixedTestStruct {
        A: y.A,
        B: y.B,
        C: y.C,
    }
}

fn complex_from_yaml(y: ComplexYaml) -> ComplexTestStruct {
    let d_bytes = parse_hex_vec(&y.D);
    let f: Vec<FixedTestStruct> = y.F.into_iter().map(fixed_from_yaml).collect();
    let g: Vec<VarTestStruct> = y.G.into_iter().map(var_from_yaml).collect();
    ComplexTestStruct {
        A: y.A,
        B: VariableList::new(y.B).expect("ComplexTestStruct.B within 128"),
        C: y.C,
        D: VariableList::new(d_bytes).expect("ByteList within 256"),
        E: var_from_yaml(y.E),
        F: FixedVector::new(f).expect("F is Vector[_, 4]"),
        G: FixedVector::new(g).expect("G is Vector[_, 2]"),
    }
}

fn bitlist_from_hex<N: typenum::Unsigned + Clone>(hex: &str) -> BitList<N> {
    let bytes = parse_hex_vec(hex);
    BitList::<N>::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("BitList from {hex}: {e:?}"))
}

fn bitvector_from_hex<N: typenum::Unsigned + Clone>(hex: &str) -> BitVector<N> {
    let bytes = parse_hex_vec(hex);
    BitVector::<N>::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("BitVector from {hex}: {e:?}"))
}

fn bits_from_yaml(y: BitsYaml) -> BitsStruct {
    BitsStruct {
        A: bitlist_from_hex(&y.A),
        B: bitvector_from_hex(&y.B),
        C: bitvector_from_hex(&y.C),
        D: bitlist_from_hex(&y.D),
        E: bitvector_from_hex(&y.E),
    }
}

// ---------------------------------------------------------------------------
// Valid / invalid case drivers
// ---------------------------------------------------------------------------

fn leaf_name(case: &Case) -> &str {
    case.name.rsplit('/').next().expect("case name non-empty")
}

fn is_valid_case(case: &Case) -> bool {
    case.name.starts_with("valid/")
}

fn is_invalid_case(case: &Case) -> bool {
    case.name.starts_with("invalid/")
}

/// Decode → value equality → re-encode → hash_tree_root.
fn assert_valid<T>(case: &Case, decoded: T, expected: T)
where
    T: Encode + TreeHash + PartialEq + Debug,
{
    assert_eq!(
        decoded,
        expected,
        "value.yaml mismatch for {}",
        case.rel_path()
    );
    let bytes = case
        .ssz_bytes("serialized.ssz_snappy")
        .unwrap_or_else(|e| panic!("ssz bytes {}: {e}", case.rel_path()));
    assert_eq!(
        decoded.as_ssz_bytes(),
        bytes,
        "re-encode mismatch for {}",
        case.rel_path()
    );
    let meta: MetaYaml = case
        .yaml("meta.yaml")
        .unwrap_or_else(|e| panic!("meta.yaml {}: {e}", case.rel_path()));
    let expected_root = parse_root(&meta.root);
    let got = Root::from_hash256(decoded.tree_hash_root());
    assert_eq!(
        got,
        expected_root,
        "hash_tree_root mismatch for {}",
        case.rel_path()
    );
}

/// Run `decode` under `catch_unwind` for an `invalid/` case. Must yield `Err`
/// and must not panic.
fn assert_invalid_decode<F>(case: &Case, decode: F)
where
    F: FnOnce() -> Result<(), DecodeError> + std::panic::UnwindSafe,
{
    let rel = case.rel_path();
    let result = std::panic::catch_unwind(decode);
    match result {
        Ok(Ok(())) => panic!("invalid case {rel} decoded successfully (expected Err)"),
        Ok(Err(_)) => {}
        Err(_) => panic!("invalid case {rel} panicked during decode (expected Err, never panic)"),
    }
}

fn load_case_bytes(case: &Case) -> Vec<u8> {
    case.ssz_bytes("serialized.ssz_snappy")
        .unwrap_or_else(|e| panic!("ssz bytes {}: {e}", case.rel_path()))
}

// ---------------------------------------------------------------------------
// Handler: uints  (`uint_<size>_…`)
// ---------------------------------------------------------------------------

fn run_uints(case: &Case) {
    let name = leaf_name(case);
    let size = parse_uint_size(name);
    if is_valid_case(case) {
        let bytes = load_case_bytes(case);
        let yaml: serde_yaml::Value = case
            .yaml("value.yaml")
            .unwrap_or_else(|e| panic!("value.yaml {}: {e}", case.rel_path()));
        match size {
            8 => {
                let v = u8::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
                let expected = yaml_u64(&yaml) as u8;
                assert_valid(case, v, expected);
            }
            16 => {
                let v = u16::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
                let expected = yaml_u64(&yaml) as u16;
                assert_valid(case, v, expected);
            }
            32 => {
                let v = u32::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
                let expected = yaml_u64(&yaml) as u32;
                assert_valid(case, v, expected);
            }
            64 => {
                let v = u64::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
                let expected = yaml_u64(&yaml);
                assert_valid(case, v, expected);
            }
            128 => {
                let v = U128::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
                let expected = yaml_u128(&yaml);
                assert_valid(case, v, expected);
            }
            256 => {
                let v = U256::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
                let expected = yaml_u256(&yaml);
                assert_valid(case, v, expected);
            }
            other => panic!("unrecognised ssz_generic case grammar {name} (uint size {other})"),
        }
    } else if is_invalid_case(case) {
        let bytes = load_case_bytes(case);
        match size {
            8 => assert_invalid_decode(case, || u8::from_ssz_bytes(&bytes).map(|_| ())),
            16 => assert_invalid_decode(case, || u16::from_ssz_bytes(&bytes).map(|_| ())),
            32 => assert_invalid_decode(case, || u32::from_ssz_bytes(&bytes).map(|_| ())),
            64 => assert_invalid_decode(case, || u64::from_ssz_bytes(&bytes).map(|_| ())),
            128 => assert_invalid_decode(case, || U128::from_ssz_bytes(&bytes).map(|_| ())),
            256 => assert_invalid_decode(case, || U256::from_ssz_bytes(&bytes).map(|_| ())),
            other => panic!("unrecognised ssz_generic case grammar {name} (uint size {other})"),
        }
    } else {
        panic!("unrecognised ssz_generic case grammar {name}");
    }
}

fn parse_uint_size(name: &str) -> usize {
    // uint_<size>_…
    let rest = name
        .strip_prefix("uint_")
        .unwrap_or_else(|| panic!("unrecognised ssz_generic case grammar {name}"));
    let size_str = rest.split('_').next().unwrap_or(rest);
    size_str
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("unrecognised ssz_generic case grammar {name}"))
}

// ---------------------------------------------------------------------------
// Handler: boolean  (`true` / `false` / `byte_*`)
// ---------------------------------------------------------------------------

fn run_boolean(case: &Case) {
    let name = leaf_name(case);
    let bytes = load_case_bytes(case);
    if is_valid_case(case) {
        match name {
            "true" | "false" => {
                let v = bool::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
                let expected: bool = case
                    .yaml("value.yaml")
                    .unwrap_or_else(|e| panic!("value.yaml {}: {e}", case.rel_path()));
                assert_valid(case, v, expected);
            }
            other => panic!("unrecognised ssz_generic case grammar {other}"),
        }
    } else if is_invalid_case(case) {
        // byte_0x80, byte_0xff, byte_2, byte_rev_nibble
        if name.starts_with("byte_") {
            assert_invalid_decode(case, || bool::from_ssz_bytes(&bytes).map(|_| ()));
        } else {
            panic!("unrecognised ssz_generic case grammar {name}");
        }
    } else {
        panic!("unrecognised ssz_generic case grammar {name}");
    }
}

// ---------------------------------------------------------------------------
// Handler: bitlist  (`bitlist_<n>_…`)
// ---------------------------------------------------------------------------

fn run_bitlist(case: &Case) {
    let name = leaf_name(case);
    let n = parse_bit_n(name, "bitlist");
    dispatch_bitlist(case, n);
}

fn parse_bit_n(name: &str, prefix: &str) -> usize {
    // bitlist_<n>_…  or bitvec_<n>_…  or bitvector_<n>_…
    let rest = if let Some(r) = name.strip_prefix(&format!("{prefix}_")) {
        r
    } else if prefix == "bitvector" {
        name.strip_prefix("bitvec_")
            .unwrap_or_else(|| panic!("unrecognised ssz_generic case grammar {name}"))
    } else {
        panic!("unrecognised ssz_generic case grammar {name}");
    };
    let n_str = rest.split('_').next().unwrap_or(rest);
    n_str
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("unrecognised ssz_generic case grammar {name}"))
}

fn dispatch_bitlist(case: &Case, n: usize) {
    match n {
        1 => bitlist_case::<U1>(case),
        2 => bitlist_case::<U2>(case),
        3 => bitlist_case::<U3>(case),
        4 => bitlist_case::<U4>(case),
        5 => bitlist_case::<U5>(case),
        6 => bitlist_case::<U6>(case),
        7 => bitlist_case::<U7>(case),
        8 => bitlist_case::<U8>(case),
        9 => bitlist_case::<U9>(case),
        15 => bitlist_case::<U15>(case),
        16 => bitlist_case::<U16>(case),
        17 => bitlist_case::<U17>(case),
        31 => bitlist_case::<U31>(case),
        32 => bitlist_case::<U32>(case),
        33 => bitlist_case::<U33>(case),
        511 => bitlist_case::<U511>(case),
        512 => bitlist_case::<U512>(case),
        513 => bitlist_case::<U513>(case),
        other => {
            let name = leaf_name(case);
            panic!("unrecognised ssz_generic case grammar {name} (bitlist n={other})");
        }
    }
}

fn bitlist_case<N: typenum::Unsigned + Clone + Debug>(case: &Case) {
    let bytes = load_case_bytes(case);
    if is_valid_case(case) {
        let v = BitList::<N>::from_ssz_bytes(&bytes)
            .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
        let hex: String = case
            .yaml("value.yaml")
            .unwrap_or_else(|e| panic!("value.yaml {}: {e}", case.rel_path()));
        let expected = bitlist_from_hex::<N>(&hex);
        assert_valid(case, v, expected);
    } else if is_invalid_case(case) {
        assert_invalid_decode(case, || BitList::<N>::from_ssz_bytes(&bytes).map(|_| ()));
    } else {
        panic!("unrecognised ssz_generic case grammar {}", leaf_name(case));
    }
}

// ---------------------------------------------------------------------------
// Handler: bitvector  (`bitvec_<n>_…`)
// ---------------------------------------------------------------------------

fn run_bitvector(case: &Case) {
    let name = leaf_name(case);
    // invalid may be `bitvec_0` (n=0) — unrepresentable; treat as always-Err.
    if name == "bitvec_0" || name.starts_with("bitvec_0_") {
        if is_invalid_case(case) {
            let bytes = load_case_bytes(case);
            // No legal BitVector<U0>; any decode attempt with a stand-in still Errs
            // on length. Use U1 as a probe type: wrong length → Err.
            assert_invalid_decode(case, || BitVector::<U1>::from_ssz_bytes(&bytes).map(|_| ()));
            return;
        }
        panic!("unrecognised ssz_generic case grammar {name}");
    }
    let n = parse_bit_n(name, "bitvector");
    dispatch_bitvector(case, n);
}

fn dispatch_bitvector(case: &Case, n: usize) {
    match n {
        1 => bitvector_case::<U1>(case),
        2 => bitvector_case::<U2>(case),
        3 => bitvector_case::<U3>(case),
        4 => bitvector_case::<U4>(case),
        5 => bitvector_case::<U5>(case),
        6 => bitvector_case::<U6>(case),
        7 => bitvector_case::<U7>(case),
        8 => bitvector_case::<U8>(case),
        9 => bitvector_case::<U9>(case),
        15 => bitvector_case::<U15>(case),
        16 => bitvector_case::<U16>(case),
        17 => bitvector_case::<U17>(case),
        31 => bitvector_case::<U31>(case),
        32 => bitvector_case::<U32>(case),
        33 => bitvector_case::<U33>(case),
        511 => bitvector_case::<U511>(case),
        512 => bitvector_case::<U512>(case),
        513 => bitvector_case::<U513>(case),
        other => {
            let name = leaf_name(case);
            panic!("unrecognised ssz_generic case grammar {name} (bitvector n={other})");
        }
    }
}

fn bitvector_case<N: typenum::Unsigned + Clone + Debug>(case: &Case) {
    let bytes = load_case_bytes(case);
    if is_valid_case(case) {
        let v = BitVector::<N>::from_ssz_bytes(&bytes)
            .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
        let hex: String = case
            .yaml("value.yaml")
            .unwrap_or_else(|e| panic!("value.yaml {}: {e}", case.rel_path()));
        let expected = bitvector_from_hex::<N>(&hex);
        assert_valid(case, v, expected);
    } else if is_invalid_case(case) {
        assert_invalid_decode(case, || BitVector::<N>::from_ssz_bytes(&bytes).map(|_| ()));
    } else {
        panic!("unrecognised ssz_generic case grammar {}", leaf_name(case));
    }
}

// ---------------------------------------------------------------------------
// Handler: basic_vector  (`vec_<type>_<n>_…`)
// ---------------------------------------------------------------------------

fn run_basic_vector(case: &Case) {
    let name = leaf_name(case);
    let (elem, n) = parse_vec_grammar(name);
    // n == 0 appears only in invalid/
    if n == 0 {
        if !is_invalid_case(case) {
            panic!("unrecognised ssz_generic case grammar {name}");
        }
        let bytes = load_case_bytes(case);
        // FixedVector of length 0 is not constructible; probe with U1.
        match elem {
            "bool" => assert_invalid_decode(case, || {
                FixedVector::<bool, U1>::from_ssz_bytes(&bytes).map(|_| ())
            }),
            "uint8" => assert_invalid_decode(case, || {
                FixedVector::<u8, U1>::from_ssz_bytes(&bytes).map(|_| ())
            }),
            "uint16" => assert_invalid_decode(case, || {
                FixedVector::<u16, U1>::from_ssz_bytes(&bytes).map(|_| ())
            }),
            "uint32" => assert_invalid_decode(case, || {
                FixedVector::<u32, U1>::from_ssz_bytes(&bytes).map(|_| ())
            }),
            "uint64" => assert_invalid_decode(case, || {
                FixedVector::<u64, U1>::from_ssz_bytes(&bytes).map(|_| ())
            }),
            "uint128" => assert_invalid_decode(case, || {
                FixedVector::<U128, U1>::from_ssz_bytes(&bytes).map(|_| ())
            }),
            "uint256" => assert_invalid_decode(case, || {
                FixedVector::<U256, U1>::from_ssz_bytes(&bytes).map(|_| ())
            }),
            other => panic!("unrecognised ssz_generic case grammar {name} (elem {other})"),
        }
        return;
    }
    match elem {
        "bool" => dispatch_vec_bool(case, n),
        "uint8" => dispatch_vec_u8(case, n),
        "uint16" => dispatch_vec_u16(case, n),
        "uint32" => dispatch_vec_u32(case, n),
        "uint64" => dispatch_vec_u64(case, n),
        "uint128" => dispatch_vec_u128(case, n),
        "uint256" => dispatch_vec_u256(case, n),
        other => panic!("unrecognised ssz_generic case grammar {name} (elem {other})"),
    }
}

fn parse_vec_grammar(name: &str) -> (&str, usize) {
    // vec_<type>_<n>_…
    let rest = name
        .strip_prefix("vec_")
        .unwrap_or_else(|| panic!("unrecognised ssz_generic case grammar {name}"));
    let mut parts = rest.splitn(3, '_');
    let elem = parts
        .next()
        .unwrap_or_else(|| panic!("unrecognised ssz_generic case grammar {name}"));
    // uint128 / uint256 split as uint128 — good; uint8 → uint8
    // but splitn on '_' gives uint8 for uint8. For uint128: "uint128" if no extra underscore.
    // Wait: "vec_uint128_4_max" → rest=uint128_4_max → elem=uint128, n=4. Good.
    // "vec_bool_1_max" → bool, 1. Good.
    let n_str = parts
        .next()
        .unwrap_or_else(|| panic!("unrecognised ssz_generic case grammar {name}"));
    let n = n_str
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("unrecognised ssz_generic case grammar {name}"));
    (elem, n)
}

macro_rules! dispatch_vec_n {
    ($fn_name:ident, $ty:ty, $yaml:expr) => {
        fn $fn_name(case: &Case, n: usize) {
            match n {
                1 => vec_case::<$ty, U1>(case, $yaml),
                2 => vec_case::<$ty, U2>(case, $yaml),
                3 => vec_case::<$ty, U3>(case, $yaml),
                4 => vec_case::<$ty, U4>(case, $yaml),
                5 => vec_case::<$ty, U5>(case, $yaml),
                8 => vec_case::<$ty, U8>(case, $yaml),
                16 => vec_case::<$ty, U16>(case, $yaml),
                31 => vec_case::<$ty, U31>(case, $yaml),
                512 => vec_case::<$ty, U512>(case, $yaml),
                513 => vec_case::<$ty, U513>(case, $yaml),
                other => {
                    let name = leaf_name(case);
                    panic!("unrecognised ssz_generic case grammar {name} (vec n={other})");
                }
            }
        }
    };
}

fn yaml_vec_bool(v: serde_yaml::Value) -> Vec<bool> {
    serde_yaml::from_value(v).expect("vec bool yaml")
}
fn yaml_vec_u8(v: serde_yaml::Value) -> Vec<u8> {
    let raw: Vec<u64> = serde_yaml::from_value(v).expect("vec u8 yaml");
    raw.into_iter().map(|x| x as u8).collect()
}
fn yaml_vec_u16(v: serde_yaml::Value) -> Vec<u16> {
    let raw: Vec<u64> = serde_yaml::from_value(v).expect("vec u16 yaml");
    raw.into_iter().map(|x| x as u16).collect()
}
fn yaml_vec_u32(v: serde_yaml::Value) -> Vec<u32> {
    let raw: Vec<u64> = serde_yaml::from_value(v).expect("vec u32 yaml");
    raw.into_iter().map(|x| x as u32).collect()
}
fn yaml_vec_u64(v: serde_yaml::Value) -> Vec<u64> {
    match v {
        serde_yaml::Value::Sequence(seq) => seq.iter().map(yaml_u64).collect(),
        other => panic!("expected sequence for vec u64, got {other:?}"),
    }
}
fn yaml_vec_u128(v: serde_yaml::Value) -> Vec<U128> {
    match v {
        serde_yaml::Value::Sequence(seq) => seq.iter().map(yaml_u128).collect(),
        other => panic!("expected sequence for vec u128, got {other:?}"),
    }
}
fn yaml_vec_u256(v: serde_yaml::Value) -> Vec<U256> {
    match v {
        serde_yaml::Value::Sequence(seq) => seq.iter().map(yaml_u256).collect(),
        other => panic!("expected sequence for vec u256, got {other:?}"),
    }
}

dispatch_vec_n!(dispatch_vec_bool, bool, yaml_vec_bool);
dispatch_vec_n!(dispatch_vec_u8, u8, yaml_vec_u8);
dispatch_vec_n!(dispatch_vec_u16, u16, yaml_vec_u16);
dispatch_vec_n!(dispatch_vec_u32, u32, yaml_vec_u32);
dispatch_vec_n!(dispatch_vec_u64, u64, yaml_vec_u64);
dispatch_vec_n!(dispatch_vec_u128, U128, yaml_vec_u128);
dispatch_vec_n!(dispatch_vec_u256, U256, yaml_vec_u256);

fn vec_case<T, N>(case: &Case, yaml_parse: fn(serde_yaml::Value) -> Vec<T>)
where
    T: Decode + Encode + TreeHash + PartialEq + Debug + Clone + 'static,
    N: typenum::Unsigned + Clone,
{
    let bytes = load_case_bytes(case);
    if is_valid_case(case) {
        let v = FixedVector::<T, N>::from_ssz_bytes(&bytes)
            .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
        let yaml: serde_yaml::Value = case
            .yaml("value.yaml")
            .unwrap_or_else(|e| panic!("value.yaml {}: {e}", case.rel_path()));
        let expected_vec = yaml_parse(yaml);
        let expected = FixedVector::<T, N>::new(expected_vec)
            .unwrap_or_else(|e| panic!("expected vec len {}: {e:?}", case.rel_path()));
        assert_valid(case, v, expected);
    } else if is_invalid_case(case) {
        assert_invalid_decode(case, || {
            FixedVector::<T, N>::from_ssz_bytes(&bytes).map(|_| ())
        });
    } else {
        panic!("unrecognised ssz_generic case grammar {}", leaf_name(case));
    }
}

// ---------------------------------------------------------------------------
// Handler: containers  (`<Type>_…`)
// ---------------------------------------------------------------------------

fn run_containers(case: &Case) {
    let name = leaf_name(case);
    let type_name = container_type_name(name);

    if OUT_OF_SCOPE_CONTAINER_TYPES.contains(&type_name) {
        // EIP-7688 progressive containers — Fulu out of scope (D1).
        return;
    }

    match type_name {
        "SingleFieldTestStruct" => container_case::<SingleFieldTestStruct, _>(case, |c| {
            let y: SingleFieldYaml = c
                .yaml("value.yaml")
                .unwrap_or_else(|e| panic!("value.yaml: {e}"));
            SingleFieldTestStruct { A: y.A }
        }),
        "SmallTestStruct" => container_case::<SmallTestStruct, _>(case, |c| {
            let y: SmallYaml = c
                .yaml("value.yaml")
                .unwrap_or_else(|e| panic!("value.yaml: {e}"));
            SmallTestStruct { A: y.A, B: y.B }
        }),
        "FixedTestStruct" => container_case::<FixedTestStruct, _>(case, |c| {
            let y: FixedYaml = c
                .yaml("value.yaml")
                .unwrap_or_else(|e| panic!("value.yaml: {e}"));
            fixed_from_yaml(y)
        }),
        "VarTestStruct" => container_case::<VarTestStruct, _>(case, |c| {
            let y: VarYaml = c
                .yaml("value.yaml")
                .unwrap_or_else(|e| panic!("value.yaml: {e}"));
            var_from_yaml(y)
        }),
        "ComplexTestStruct" => container_case::<ComplexTestStruct, _>(case, |c| {
            let y: ComplexYaml = c
                .yaml("value.yaml")
                .unwrap_or_else(|e| panic!("value.yaml: {e}"));
            complex_from_yaml(y)
        }),
        "BitsStruct" => container_case::<BitsStruct, _>(case, |c| {
            let y: BitsYaml = c
                .yaml("value.yaml")
                .unwrap_or_else(|e| panic!("value.yaml: {e}"));
            bits_from_yaml(y)
        }),
        other => panic!("unrecognised ssz_generic case grammar {name} (container {other})"),
    }
}

fn container_type_name(name: &str) -> &str {
    // Type names are CamelCase prefixes before the first `_` that begins a
    // non-uppercase suite suffix. Known types are exact prefixes:
    for t in [
        "SingleFieldTestStruct",
        "SmallTestStruct",
        "FixedTestStruct",
        "VarTestStruct",
        "ComplexTestStruct",
        "BitsStruct",
        "ProgressiveBitsStruct",
        "ProgressiveTestStruct",
    ] {
        if name == t || name.starts_with(&format!("{t}_")) {
            return t;
        }
    }
    panic!("unrecognised ssz_generic case grammar {name}");
}

fn container_case<T, F>(case: &Case, expected_from_yaml: F)
where
    T: Decode + Encode + TreeHash + PartialEq + Debug,
    F: FnOnce(&Case) -> T,
{
    let bytes = load_case_bytes(case);
    if is_valid_case(case) {
        let v = T::from_ssz_bytes(&bytes)
            .unwrap_or_else(|e| panic!("decode {}: {e:?}", case.rel_path()));
        let expected = expected_from_yaml(case);
        assert_valid(case, v, expected);
    } else if is_invalid_case(case) {
        assert_invalid_decode(case, || T::from_ssz_bytes(&bytes).map(|_| ()));
    } else {
        panic!("unrecognised ssz_generic case grammar {}", leaf_name(case));
    }
}

// ---------------------------------------------------------------------------
// Suite runners
// ---------------------------------------------------------------------------

fn run_handler(handler: &str, run_case: fn(&Case)) {
    let vectors = open_vectors();
    let cases = vectors
        .cases(PRESET, FORK, RUNNER, handler)
        .unwrap_or_else(|e| panic!("enumerate {handler}: {e}"));
    assert!(
        !cases.is_empty(),
        "expected at least one case for {PRESET}/{FORK}/{RUNNER}/{handler}"
    );
    for case in &cases {
        run_case(case);
    }
}

#[test]
fn uints() {
    run_handler("uints", run_uints);
}

#[test]
fn boolean() {
    run_handler("boolean", run_boolean);
}

#[test]
fn bitlist() {
    run_handler("bitlist", run_bitlist);
}

#[test]
fn bitvector() {
    run_handler("bitvector", run_bitvector);
}

#[test]
fn basic_vector() {
    run_handler("basic_vector", run_basic_vector);
}

#[test]
fn containers() {
    run_handler("containers", run_containers);
}

/// Declared handlers equal on-disk listing (minus progressive/union out-of-scope).
#[test]
fn handler_coverage() {
    let vectors = open_vectors();
    let on_disk = vectors
        .handlers(PRESET, FORK, RUNNER)
        .unwrap_or_else(|e| panic!("handlers: {e}"));
    let relevant: BTreeSet<String> = on_disk
        .into_iter()
        .filter(|h| !OUT_OF_SCOPE_HANDLERS.contains(&h.as_str()))
        .collect();
    assert_handler_coverage(HANDLERS, &relevant)
        .unwrap_or_else(|e| panic!("handler coverage mismatch for {PRESET}/{FORK}/{RUNNER}: {e}"));
}
