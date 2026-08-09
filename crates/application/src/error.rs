//! Stable application-layer failures.

use agentforge_domain::{DomainError, EventId, InvocationRunId, PackageId, ProjectId};
use thiserror::Error;

use crate::projection::ProjectionCursor;

pub type ApplicationResult<T> = Result<T, ApplicationError>;

/// Errors produced before an infrastructure adapter is invoked.
///
/// Variants intentionally contain identifiers and stable reason codes only;
/// raw payloads, prompts, credentials, and adapter diagnostics must stay in
/// protected logs.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ApplicationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("application serialization failed")]
    Serialization,
    #[error("projection payload digest is invalid for event {event_id}")]
    ProjectionDigestInvalid { event_id: EventId },
    #[error("projection version {found} is unsupported")]
    ProjectionVersionUnsupported { found: u16 },
    #[error("event {event_id} was reused with different projection content")]
    ProjectionEventConflict { event_id: EventId },
    #[error("project event sequence {event_sequence} was reused")]
    ProjectionSequenceConflict {
        project_id: ProjectId,
        event_sequence: u64,
    },
    #[error("cursor {cursor:?} is at or before the restored project checkpoint")]
    ProjectionBeforeCheckpoint {
        project_id: ProjectId,
        cursor: ProjectionCursor,
    },
    #[error("projection checkpoint belongs to a different project")]
    ProjectionProjectMismatch,
    #[error("projection headers are inconsistent")]
    ProjectionHeaderInvalid,
    #[error("run {run_id} is marked running without a current active claim")]
    ProjectionRunAuthorityInvalid { run_id: InvocationRunId },
    #[error("nonterminal package {package_id} has no next driver")]
    ProjectionActionPathMissing { package_id: PackageId },
    #[error("project projection was not found")]
    ProjectionNotFound,
}
