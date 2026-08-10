//! Application use cases, infrastructure ports, and rebuildable read models.
//!
//! Domain aggregates remain authoritative.  The projection modules consume a
//! deliberately normalized event input and only build disposable operator
//! views; they never feed state back into an aggregate decision.

pub mod error;
pub mod mvp;
pub mod ports;
pub mod projection;
pub mod read_models;

pub use error::{ApplicationError, ApplicationResult};
pub use mvp::{
    AttemptProgressStage, AttemptProgressView, CandidateArtifactChunkReceipt,
    CandidateArtifactView, ClaimPackageInput, ClaimedWork, CompleteCandidateArtifactInput,
    CreateProjectInput, InitCandidateArtifactInput, LeaseReconciliationReport, LeaseView,
    ListOffersQuery, MvpCommand, MvpCommandContext, MvpControlPlane, MvpError, MvpFuture,
    MvpRemoteError, MvpResult, OfferView, PackageExecutionSnapshot, ProjectView,
    PublishPackageInput, PublishedPackage, ReconcileExpiredLeasesQuery, RecordCandidateInput,
    RecordedCandidate, ReleaseLeaseInput, RenewLeaseInput, ReportAttemptProgressInput,
    UploadCandidateArtifactChunkInput,
};
pub use ports::{
    AppendEventsReceipt, Clock, EventAppendPort, EventRecord, IdGenerator, Isolation, PortError,
    PortFuture, PortResult, Repository, StoredAggregate, UnitOfWork, UnitOfWorkFactory,
};
pub use projection::{
    ActivityProjectionInput, AgentProjectionInput, ApplyBatchReceipt,
    BudgetEnvelopeProjectionInput, BudgetIncidentProjectionInput, ControlRoomReducer,
    GovernanceProjectionInput, InMemoryProjectionStore, LineageNodeProjectionInput,
    NodeProjectionInput, PackageActionPathProjectionInput, PackageProjectionInput,
    ProjectionCursor, ProjectionEnvelope, ProjectionEvent, ProjectionInput, ProjectionVersion,
    RunProjectionInput, StoredProjection, WorkGraphNodeProjectionInput,
};
pub use read_models::*;

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-application";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
