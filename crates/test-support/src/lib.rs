//! Deterministic primitives for AgentForge tests.
//!
//! Core correctness tests must not depend on wall-clock time, operating-system
//! randomness, external networking, or credentials from a developer's host.

mod clock;
mod event_recorder;
mod evidence;
mod ids;
#[cfg(target_os = "linux")]
mod network_sandbox;
mod rng;

pub use clock::FixedClock;
pub use event_recorder::{EventRecorder, RecordedEvent};
pub use evidence::{
    CommandEvidence, EvidencePaths, EvidenceStatus, HermeticTestCommand, RunnerFingerprint,
    TestExecutionConstraints,
};
pub use ids::{DeterministicUuidV7, UuidGenerationError};
#[cfg(target_os = "linux")]
pub use network_sandbox::install_network_deny_filter;
pub use rng::DeterministicRng;

/// Package name used by dependency-boundary tests.
pub const CRATE_NAME: &str = "agentforge-test-support";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
