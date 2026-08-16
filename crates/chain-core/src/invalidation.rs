//! Justified-checkpoint invalidation exit path (Architecture §4.7 step 4, CC-35 /8).
//!
//! When the justified checkpoint itself transitions `NOT_VALIDATED → INVALIDATED`,
//! the ethereum consensus specs offer a **MAY**: alert the user and exit.
//!
//! **We take the MAY.** Continuing on an invalid justification means every
//! subsequent fork-choice decision is computed from a checkpoint the EL says
//! cannot exist — worse than stopping. Compose restarts us; we re-checkpoint-sync;
//! the operator has a `fatal!` line that names the checkpoint and the
//! `latestValidHash` that produced it.
//!
//! Sequence (always in this order):
//! 1. `fatal!` naming checkpoint + `latestValidHash` — **the durable operator signal**
//!    (log shipping survives the crash; the Prometheus counter does not)
//! 2. `cc_chain_justified_invalidated_total` += 1 (process-local; lost on restart)
//! 3. exit (via injected hook in tests; `std::process::exit` in production)
//!
//! **Wiring (import / fcU — not this module):** a single production wrapper must
//! run the walk, then if [`cc_fork_choice::justified_checkpoint_is_invalid`] call
//! this handler. Do not call `apply_invalidation` alone for engine INVALID.

use cc_types::containers::Checkpoint;
use cc_types::primitives::Hash256;

use crate::metrics::ChainMetrics;

/// Exit hook: production uses [`std::process::exit`]; tests inject a recorder.
pub type ExitFn = Box<dyn FnOnce(i32) + Send>;

/// Default production exit: terminate the process with the code passed to the hook.
pub fn process_exit() -> ExitFn {
    Box::new(|code| {
        std::process::exit(code);
    })
}

/// `fatal!` — error-level, process-ending intent. Distinct from ordinary `error!`
/// so operators and the exit-path test can grep for it.
macro_rules! fatal {
    ($($fields:tt)*) => {
        ::tracing::error!(target: "fatal", $($fields)*);
    };
}

/// Handle a justified checkpoint that has become `INVALIDATED`.
///
/// # Spec decision (recorded)
///
/// The spec's MAY was taken: continuing on an invalid justification is worse
/// than stopping. See Architecture §4.7 step 4 / CC-35 /8 / PRD R-9.
///
/// Sequence: `fatal!` (names checkpoint + `latestValidHash`) →
/// `cc_chain_justified_invalidated_total` += 1 → exit.
pub fn handle_justified_checkpoint_invalidated(
    justified: Checkpoint,
    latest_valid_hash: Option<Hash256>,
    metrics: &ChainMetrics,
    exit: ExitFn,
) {
    // fatal! names the checkpoint and the latestValidHash that produced the
    // invalidation so the log is actionable after compose restarts.
    fatal!(
        justified_epoch = justified.epoch.as_u64(),
        justified_root = ?justified.root,
        latest_valid_hash = ?latest_valid_hash,
        "justified checkpoint invalidated; exiting (spec MAY taken — continuing on an invalid justification is worse than stopping)"
    );

    // Spec MAY taken: continuing on an invalid justification is worse than stopping.
    // Counter is process-local (not durable across restart); durable signal is fatal! above.
    metrics.inc_justified_invalidated();

    exit(1);
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::metrics::ChainMetrics;
    use cc_types::primitives::{Epoch, Root};
    use prometheus_client::registry::Registry;
    use std::sync::{Arc, Mutex};

    fn root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn hash(b: u8) -> Hash256 {
        Hash256::from([b; 32])
    }

    /// Writer that appends to a shared buffer for tracing capture.
    struct TestWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// CC-35 /8 — justified checkpoint invalidated: fatal! names checkpoint +
    /// latestValidHash, counter increments by 1, exit hook is reached.
    #[test]
    fn justified_checkpoint_invalidated_exits() {
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let before = metrics.justified_invalidated_count();

        let justified = Checkpoint {
            epoch: Epoch::new(7),
            root: root(0x42),
        };
        let lvh = Some(hash(0xAB));

        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let make_writer = {
            let buf = Arc::clone(&buf);
            move || TestWriter(Arc::clone(&buf))
        };
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_writer(make_writer)
            .with_ansi(false)
            .with_level(true)
            .finish();

        let exit_called = Arc::new(Mutex::new(None::<i32>));
        let exit_hook = {
            let exit_called = Arc::clone(&exit_called);
            Box::new(move |code: i32| {
                *exit_called.lock().unwrap() = Some(code);
            }) as ExitFn
        };

        tracing::subscriber::with_default(subscriber, || {
            handle_justified_checkpoint_invalidated(justified, lvh, &metrics, exit_hook);
        });

        // 1. fatal! names the checkpoint and the latestValidHash.
        let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logged.contains("justified checkpoint invalidated")
                || logged.contains("justified_root"),
            "fatal! must name the justified-checkpoint exit; got:\n{logged}"
        );
        assert!(
            logged.contains("justified_root") || logged.contains(&format!("{:?}", justified.root)),
            "fatal! must name the checkpoint root; got:\n{logged}"
        );
        assert!(
            logged.contains("latest_valid_hash") || logged.contains(&format!("{:?}", hash(0xAB))),
            "fatal! must name the latestValidHash that produced it; got:\n{logged}"
        );
        // Spec MAY taken — recorded in the message.
        assert!(
            logged.contains("spec MAY") || logged.contains("worse than stopping"),
            "fatal! must record that the spec MAY was taken; got:\n{logged}"
        );

        // 2. Counter increments by 1.
        assert_eq!(
            metrics.justified_invalidated_count(),
            before + 1,
            "cc_chain_justified_invalidated_total must increment by 1"
        );

        // 3. Exit path reached (injected hook, not process::exit).
        assert_eq!(
            *exit_called.lock().unwrap(),
            Some(1),
            "exit hook must be called with code 1"
        );
    }
}
