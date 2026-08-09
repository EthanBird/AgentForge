//! Pure, deterministic domain model for AgentForge.
//!
//! This crate deliberately contains no persistence, transport, clock, or UUID
//! generation ports. Callers supply authoritative facts (including server time)
//! to commands and persist the returned events with their state projection.

pub mod command;
pub mod error;
pub mod event;
pub mod ids;
pub mod state;

pub use command::{
    CommandMetadata, CommandReceipt, IdempotencyScope, ReceiptDecision, check_receipt,
};
pub use error::{DomainError, PublicError};
pub use event::{AggregateId, AggregateType, DomainEventEnvelope, EventEnvelope};
pub use ids::*;
pub use state::{
    Attempt, BudgetReservation, Decision, GovernanceCase, InvocationIntent, InvocationRun, Lease,
    PolicyRevision, RunClaim, RunSignal, SessionCapsule, Submission, Transition, WorkPackage,
    attempt, budget, governance, invocation, lease, policy, run_claim, run_signal, session,
    submission, work_package,
};

/// Package name used by dependency-boundary tests.
pub const CRATE_NAME: &str = "agentforge-domain";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
