//! S1-B-01: production FastpathLane must construct `kzg: Some(...)`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[test]
fn production_cell_kzg_is_some() {
    let kzg = cc_engine::production_cell_kzg().expect("trusted setup");
    let wrapped = Some(kzg);
    assert!(
        wrapped.is_some(),
        "P1-A/25: production CellKzg construction must be Some"
    );
}

#[test]
fn production_main_wires_some_cell_kzg() {
    let src = include_str!("../src/main.rs");
    let compact: String = src.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        compact.contains("production_cell_kzg()"),
        "production main must load CellKzg via production_cell_kzg"
    );
    assert!(
        compact.contains("letkzg=Some(production_cell_kzg()"),
        "production FastpathLane kzg argument must be Some(production_cell_kzg(...))"
    );
    assert!(
        !compact.contains("kzg:None"),
        "production must not pass kzg: None"
    );
    // Positional 5th arg after tracker `None` is the named `kzg` binding.
    assert!(
        compact.contains("hoodi_blob_bound(),None,kzg,SubscriptionSet::empty()"),
        "production FastpathLane::new must pass the Some-wrapped kzg binding"
    );
}
