//! Instrumentation for the §5.1 two-`canonical_root` budget.
//!
//! All state-root computations inside this crate go through
//! [`measured_canonical_root`] so tests can assert a block-slot transition
//! performs exactly two calls (pre-state into `state_roots`, post-state
//! compared against `block.state_root`).
//!
//! Call counts and elapsed wall time are **thread-local** so parallel tests
//! do not interfere. Elapsed time supports the CC-1H early-gate attribution
//! row (hashing share of epoch wall time).

use std::cell::Cell;
use std::time::Instant;

use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::BeaconState;

thread_local! {
    static CANONICAL_ROOT_CALLS: Cell<u64> = const { Cell::new(0) };
    static CANONICAL_ROOT_NS: Cell<u128> = const { Cell::new(0) };
}

/// Reset and return this thread's measured `canonical_root` call count.
pub fn take_canonical_root_call_count() -> u64 {
    CANONICAL_ROOT_CALLS.with(|c| c.replace(0))
}

/// Current measured call count on this thread (does not reset).
pub fn canonical_root_call_count() -> u64 {
    CANONICAL_ROOT_CALLS.with(Cell::get)
}

/// Reset and return this thread's accumulated `canonical_root` wall time (ns).
pub fn take_canonical_root_elapsed_ns() -> u128 {
    CANONICAL_ROOT_NS.with(|c| c.replace(0))
}

/// Current accumulated `canonical_root` wall time on this thread (ns; no reset).
pub fn canonical_root_elapsed_ns() -> u128 {
    CANONICAL_ROOT_NS.with(Cell::get)
}

/// `state.canonical_root()` with call counting and wall-time accumulation.
#[inline]
pub fn measured_canonical_root<P: Preset>(state: &mut BeaconState<P>) -> Root {
    CANONICAL_ROOT_CALLS.with(|c| c.set(c.get() + 1));
    let t0 = Instant::now();
    let root = state.canonical_root();
    let ns = t0.elapsed().as_nanos();
    CANONICAL_ROOT_NS.with(|c| c.set(c.get().saturating_add(ns)));
    root
}
