//! Deterministic uniform slot sampling for positive / negative probe sides.
//!
//! **SEC-4B-2 / SEC-4B-3:** hard ceiling on sample count so peer Status or CLI
//! cannot force unbounded allocation / wall-clock.

/// Spec-style lookback for the negative window lower bound: `eas − 32_000`.
pub const NEGATIVE_LOOKBACK_SLOTS: u64 = 32_000;

/// Hard ceiling on slots sampled per side (SEC-4B-2 / SEC-4B-3).
///
/// Applies to `--slots`, `--below`, and `--full-window` alike. Larger windows
/// are **strided**, never fully materialised.
pub const MAX_SAMPLE_COUNT: usize = 10_000;

/// Sample up to `n` slots uniformly from the inclusive range `[lo, hi]`.
///
/// Deterministic (no RNG): uses an even stride so re-runs are comparable.
/// Returns an empty vec when `hi < lo` or `n == 0`.
///
/// `n` is clamped to [`MAX_SAMPLE_COUNT`]. Full-range materialisation
/// (`lo..=hi`) only happens when the inclusive count fits in that ceiling;
/// otherwise sampling is strided (SEC-4B-2).
#[must_use]
pub fn sample_slots(lo: u64, hi: u64, n: usize) -> Vec<u64> {
    if n == 0 || hi < lo {
        return Vec::new();
    }
    let n = n.min(MAX_SAMPLE_COUNT);

    // Inclusive count with checked arithmetic — overflow means a pathological
    // window (e.g. eas=0, head=u64::MAX); never materialise it.
    let Some(span) = hi.checked_sub(lo) else {
        return Vec::new();
    };
    let Some(count_u64) = span.checked_add(1) else {
        // span == u64::MAX: entire domain — stride only.
        return even_sample(lo, hi, n);
    };

    if count_u64 <= n as u64 {
        // Safe to materialise: count ≤ n ≤ MAX_SAMPLE_COUNT.
        return (lo..=hi).collect();
    }
    even_sample(lo, hi, n)
}

/// Evenly spaced inclusive samples over `[lo, hi]` (assumes `hi >= lo`, `n >= 1`).
fn even_sample(lo: u64, hi: u64, n: usize) -> Vec<u64> {
    if n == 0 {
        return Vec::new();
    }
    let span = hi - lo;
    if n == 1 {
        return vec![lo.saturating_add(span / 2)];
    }
    let denom = (n as u64 - 1).max(1);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        // u128 intermediate so `span * i` cannot overflow for full u64 domain.
        let offset = ((u128::from(span) * u128::from(i as u64)) / u128::from(denom)) as u64;
        let slot = lo.saturating_add(offset);
        if out.last().copied() != Some(slot) {
            out.push(slot);
        }
    }
    out
}

/// Negative-side sample range: `[eas − lookback, eas)`.
///
/// Returns `(lo, hi_inclusive)` or `None` when `eas == 0` (nothing below).
#[must_use]
pub fn negative_range(eas: u64, lookback: u64) -> Option<(u64, u64)> {
    if eas == 0 {
        return None;
    }
    let lo = eas.saturating_sub(lookback);
    let hi = eas - 1;
    if hi < lo {
        return None;
    }
    Some((lo, hi))
}

/// Positive-side sample count from Status + CLI (SEC-4B-2).
///
/// - Rejects overflowing `head − eas + 1`.
/// - Caps at [`MAX_SAMPLE_COUNT`].
/// - `head < eas` → 0 (empty positive window).
#[must_use]
pub fn positive_sample_budget(
    eas: u64,
    head: u64,
    slots: usize,
    full_window: bool,
) -> Option<usize> {
    if head < eas {
        return Some(0);
    }
    let window = head.checked_sub(eas)?.checked_add(1)?;
    if full_window {
        let w = usize::try_from(window).unwrap_or(MAX_SAMPLE_COUNT);
        Some(w.min(MAX_SAMPLE_COUNT))
    } else {
        Some(slots.min(MAX_SAMPLE_COUNT))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn samples_endpoints_when_n_ge_2() {
        let s = sample_slots(100, 200, 2);
        assert_eq!(s, vec![100, 200]);
    }

    #[test]
    fn samples_full_range_when_n_covers() {
        let s = sample_slots(5, 8, 100);
        assert_eq!(s, vec![5, 6, 7, 8]);
    }

    #[test]
    fn negative_range_below_eas() {
        let (lo, hi) = negative_range(1000, 32_000).unwrap();
        assert_eq!(lo, 0);
        assert_eq!(hi, 999);
    }

    #[test]
    fn negative_range_zero_eas() {
        assert!(negative_range(0, 32_000).is_none());
    }

    #[test]
    fn sample_count_clamped_to_max() {
        let s = sample_slots(0, 1_000_000, MAX_SAMPLE_COUNT * 2);
        assert!(s.len() <= MAX_SAMPLE_COUNT);
        assert_eq!(s.first().copied(), Some(0));
        assert_eq!(s.last().copied(), Some(1_000_000));
    }

    #[test]
    fn full_domain_does_not_materialise() {
        // Would overflow count or be enormous — must stride, not collect all.
        let s = sample_slots(0, u64::MAX, 8);
        assert_eq!(s.len(), 8);
        assert_eq!(s[0], 0);
        assert_eq!(*s.last().unwrap(), u64::MAX);
    }

    #[test]
    fn positive_budget_rejects_overflow_window() {
        // head - eas + 1 overflows when eas=0, head=u64::MAX.
        assert!(positive_sample_budget(0, u64::MAX, 1000, true).is_none());
    }

    #[test]
    fn positive_budget_caps_full_window() {
        let n = positive_sample_budget(0, 50_000, 1000, true).unwrap();
        assert_eq!(n, MAX_SAMPLE_COUNT);
    }

    #[test]
    fn positive_budget_head_below_eas() {
        assert_eq!(positive_sample_budget(100, 50, 1000, false), Some(0));
    }
}
