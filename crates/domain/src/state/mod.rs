//! Pure aggregate decisions and event application.

use serde::{Deserialize, Serialize};

pub mod attempt;
pub mod lease;
pub mod submission;
pub mod work_package;

pub use attempt::Attempt;
pub use lease::Lease;
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
