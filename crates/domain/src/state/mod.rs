//! Pure aggregate decisions and event application.

use serde::{Deserialize, Serialize};

pub mod attempt;
pub mod budget;
pub mod governance;
pub mod invocation;
pub mod lease;
pub mod policy;
pub mod run_claim;
pub mod run_signal;
pub mod session;
pub mod submission;
pub mod work_package;

pub use attempt::Attempt;
pub use budget::BudgetReservation;
pub use governance::{Decision, GovernanceCase};
pub use invocation::{InvocationIntent, InvocationRun};
pub use lease::Lease;
pub use policy::PolicyRevision;
pub use run_claim::RunClaim;
pub use run_signal::RunSignal;
pub use session::SessionCapsule;
pub use submission::Submission;
pub use work_package::WorkPackage;

/// Result of a successful pure command decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Transition<A, E> {
    pub aggregate: A,
    pub events: Vec<E>,
}

impl<A, E> Transition<A, E> {
    #[must_use]
    pub fn one(aggregate: A, event: E) -> Self {
        Self {
            aggregate,
            events: vec![event],
        }
    }
}
