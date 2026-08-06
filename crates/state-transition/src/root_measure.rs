//! Instrumentation for the §5.1 two-`canonical_root` budget.
//!
//! All state-root computations inside this crate go through
//! [`measured_canonical_root`] so tests can assert a block-slot transition
//! performs exactly two calls (pre-state into `state_roots`, post-state
//! compared against `block.state_root`).
//!
//! Counting is **thread-local** so parallel tests do not interfere.

use std::cell::Cell;

use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::BeaconState;

thread_local! {
    static CANONICAL_ROOT_CALLS: Cell<u64> = const { Cell::new(0) };
}

/// Reset and return this thread's measured `canonical_root` call count.
pub fn take_canonical_root_call_count() -> u64 {
    CANONICAL_ROOT_CALLS.with(|c| c.replace(0))
}

/// Current measured call count on this thread (does not reset).
pub fn canonical_root_call_count() -> u64 {
    CANONICAL_ROOT_CALLS.with(Cell::get)
}

/// `state.canonical_root()` with call counting.
#[inline]
pub fn measured_canonical_root<P: Preset>(state: &mut BeaconState<P>) -> Root {
    CANONICAL_ROOT_CALLS.with(|c| c.set(c.get() + 1));
    state.canonical_root()
}
