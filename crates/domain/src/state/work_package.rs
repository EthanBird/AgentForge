//! WorkPackage aggregate and its declared transition table.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, AttemptId, FencingToken, GitObjectId, IntegrationId, LeaseId, PackageId,
        PackageRevision, PackageRevisionId, ProjectId, SubmissionId,
    },
    state::Transition,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkPackageState {
    Draft,
    Validating,
    Blocked,
    Offered,
    Active,
    Verifying,
    ReworkReady,
    Accepted,
    Integrating,
    RebaseRequired,
    Integrated,
    Closed,
    Cancelled,
    Superseded,
    Failed,
}

impl WorkPackageState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Validating => "validating",
            Self::Blocked => "blocked",
            Self::Offered => "offered",
            Self::Active => "active",
            Self::Verifying => "verifying",
            Self::ReworkReady => "rework_ready",
            Self::Accepted => "accepted",
            Self::Integrating => "integrating",
            Self::RebaseRequired => "rebase_required",
            Self::Integrated => "integrated",
            Self::Closed => "closed",
            Self::Cancelled => "cancelled",
            Self::Superseded => "superseded",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Closed | Self::Cancelled | Self::Superseded | Self::Failed
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WorkPackageCommandKind {
    RequestValidation,
    ValidationFailed,
    SelectNewRevisionAndValidate,
    PublishValidatedPackage,
    GrantLease,
    RecordCandidate,
    LoseAttempt,
    FinalizeVerification,
    EnqueueIntegration,
    ReportIntegrationConflict,
    BeginReintegration,
    ReportIntegrationFailed,
    MarkIntegrated,
    ClosePackage,
    CancelPackage,
    SupersedePackage,
    FailPackage,
}

impl WorkPackageCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestValidation => "request_validation",
            Self::ValidationFailed => "validation_failed",
            Self::SelectNewRevisionAndValidate => "select_new_revision_and_validate",
            Self::PublishValidatedPackage => "publish_validated_package",
            Self::GrantLease => "grant_lease",
            Self::RecordCandidate => "record_candidate",
            Self::LoseAttempt => "lose_attempt",
            Self::FinalizeVerification => "finalize_verification",
            Self::EnqueueIntegration => "enqueue_integration",
            Self::ReportIntegrationConflict => "report_integration_conflict",
            Self::BeginReintegration => "begin_reintegration",
            Self::ReportIntegrationFailed => "report_integration_failed",
            Self::MarkIntegrated => "mark_integrated",
            Self::ClosePackage => "close_package",
            Self::CancelPackage => "cancel_package",
            Self::SupersedePackage => "supersede_package",
            Self::FailPackage => "fail_package",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationOutcome {
    Pass,
    Fail,
    Inconclusive,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishReadiness {
    pub dor_passed: bool,
    pub dag_valid: bool,
    pub budget_available: bool,
    pub permissions_valid: bool,
}

impl PublishReadiness {
    #[must_use]
    pub fn failed_checks(&self) -> Vec<String> {
        [
            (!self.dor_passed).then_some("definition_of_ready"),
            (!self.dag_valid).then_some("dag"),
            (!self.budget_available).then_some("budget"),
            (!self.permissions_valid).then_some("permissions"),
        ]
        .into_iter()
        .flatten()
        .map(str::to_owned)
        .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimReadiness {
    pub dependencies_satisfied: bool,
    pub budget_available: bool,
    pub no_active_lease: bool,
}

impl ClaimReadiness {
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.dependencies_satisfied && self.budget_available && self.no_active_lease
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkPackageCommand {
    RequestValidation {
        expected_version: AggregateVersion,
        revision_exists: bool,
        package_hash_matches: bool,
    },
    ValidationFailed {
        expected_version: AggregateVersion,
        validator_matches_revision: bool,
    },
    SelectNewRevisionAndValidate {
        expected_version: AggregateVersion,
        revision_id: PackageRevisionId,
        revision: PackageRevision,
    },
    PublishValidatedPackage {
        expected_version: AggregateVersion,
        readiness: PublishReadiness,
    },
    GrantLease {
        expected_version: AggregateVersion,
        attempt_id: AttemptId,
        lease_id: LeaseId,
        readiness: ClaimReadiness,
    },
    RecordCandidate {
        expected_version: AggregateVersion,
        attempt_id: AttemptId,
        candidate_sealed: bool,
    },
    LoseAttempt {
        expected_version: AggregateVersion,
        attempt_id: AttemptId,
        recoverable: bool,
    },
    FinalizeVerification {
        expected_version: AggregateVersion,
        attempt_id: AttemptId,
        submission_id: SubmissionId,
        outcome: VerificationOutcome,
        failed_checks: Vec<String>,
        failure_dossier_complete: bool,
    },
    EnqueueIntegration {
        expected_version: AggregateVersion,
        integration_id: IntegrationId,
        lineage_matches: bool,
        candidate_artifact_complete: bool,
    },
    ReportIntegrationConflict {
        expected_version: AggregateVersion,
        integration_id: IntegrationId,
    },
    BeginReintegration {
        expected_version: AggregateVersion,
        integration_id: IntegrationId,
        rebase_candidate_accepted: bool,
    },
    ReportIntegrationFailed {
        expected_version: AggregateVersion,
        integration_id: IntegrationId,
        recoverable: bool,
    },
    MarkIntegrated {
        expected_version: AggregateVersion,
        integration_id: IntegrationId,
        merge_commit: GitObjectId,
        signed_merge_and_l5_passed: bool,
    },
    ClosePackage {
        expected_version: AggregateVersion,
        traceability_updated: bool,
    },
    CancelPackage {
        expected_version: AggregateVersion,
        active_lease_terminated: bool,
    },
    SupersedePackage {
        expected_version: AggregateVersion,
        replacement_revision_id: PackageRevisionId,
    },
    FailPackage {
        expected_version: AggregateVersion,
        policy_authorized: bool,
        reason_code: String,
    },
}

impl WorkPackageCommand {
    #[must_use]
    pub const fn kind(&self) -> WorkPackageCommandKind {
        match self {
            Self::RequestValidation { .. } => WorkPackageCommandKind::RequestValidation,
            Self::ValidationFailed { .. } => WorkPackageCommandKind::ValidationFailed,
            Self::SelectNewRevisionAndValidate { .. } => {
                WorkPackageCommandKind::SelectNewRevisionAndValidate
            }
            Self::PublishValidatedPackage { .. } => WorkPackageCommandKind::PublishValidatedPackage,
            Self::GrantLease { .. } => WorkPackageCommandKind::GrantLease,
            Self::RecordCandidate { .. } => WorkPackageCommandKind::RecordCandidate,
            Self::LoseAttempt { .. } => WorkPackageCommandKind::LoseAttempt,
            Self::FinalizeVerification { .. } => WorkPackageCommandKind::FinalizeVerification,
            Self::EnqueueIntegration { .. } => WorkPackageCommandKind::EnqueueIntegration,
            Self::ReportIntegrationConflict { .. } => {
                WorkPackageCommandKind::ReportIntegrationConflict
            }
            Self::BeginReintegration { .. } => WorkPackageCommandKind::BeginReintegration,
            Self::ReportIntegrationFailed { .. } => WorkPackageCommandKind::ReportIntegrationFailed,
            Self::MarkIntegrated { .. } => WorkPackageCommandKind::MarkIntegrated,
            Self::ClosePackage { .. } => WorkPackageCommandKind::ClosePackage,
            Self::CancelPackage { .. } => WorkPackageCommandKind::CancelPackage,
            Self::SupersedePackage { .. } => WorkPackageCommandKind::SupersedePackage,
            Self::FailPackage { .. } => WorkPackageCommandKind::FailPackage,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> AggregateVersion {
        match self {
            Self::RequestValidation {
                expected_version, ..
            }
            | Self::ValidationFailed {
                expected_version, ..
            }
            | Self::SelectNewRevisionAndValidate {
                expected_version, ..
            }
            | Self::PublishValidatedPackage {
                expected_version, ..
            }
            | Self::GrantLease {
                expected_version, ..
            }
            | Self::RecordCandidate {
                expected_version, ..
            }
            | Self::LoseAttempt {
                expected_version, ..
            }
            | Self::FinalizeVerification {
                expected_version, ..
            }
            | Self::EnqueueIntegration {
                expected_version, ..
            }
            | Self::ReportIntegrationConflict {
                expected_version, ..
            }
            | Self::BeginReintegration {
                expected_version, ..
            }
            | Self::ReportIntegrationFailed {
                expected_version, ..
            }
            | Self::MarkIntegrated {
                expected_version, ..
            }
            | Self::ClosePackage {
                expected_version, ..
            }
            | Self::CancelPackage {
                expected_version, ..
            }
            | Self::SupersedePackage {
                expected_version, ..
            }
            | Self::FailPackage {
                expected_version, ..
            } => *expected_version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkPackageEvent {
    ValidationRequested,
    ValidationRejected,
    RevisionSelected {
        revision_id: PackageRevisionId,
        revision: PackageRevision,
    },
    PackagePublished,
    LeaseGranted {
        attempt_id: AttemptId,
        lease_id: LeaseId,
        fencing_token: FencingToken,
    },
    CandidateRecorded {
        attempt_id: AttemptId,
    },
    AttemptLost {
        attempt_id: AttemptId,
        recoverable: bool,
    },
    VerificationFinalized {
        attempt_id: AttemptId,
        submission_id: SubmissionId,
        outcome: VerificationOutcome,
    },
    IntegrationEnqueued {
        integration_id: IntegrationId,
    },
    IntegrationConflictReported {
        integration_id: IntegrationId,
    },
    ReintegrationBegan {
        integration_id: IntegrationId,
    },
    IntegrationFailedReported {
        integration_id: IntegrationId,
        recoverable: bool,
    },
    PackageIntegrated {
        integration_id: IntegrationId,
        merge_commit: GitObjectId,
    },
    PackageClosed,
    PackageCancelled,
    PackageSuperseded {
        replacement_revision_id: PackageRevisionId,
    },
    PackageFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkPackageTransitionRule {
    pub from: WorkPackageState,
    pub command: WorkPackageCommandKind,
    pub to: WorkPackageState,
}

use WorkPackageCommandKind as C;
use WorkPackageState as S;

/// The only declared WorkPackage state transitions. Conditional commands may
/// have more than one target row; their facts choose the target deterministically.
pub const TRANSITION_TABLE: &[WorkPackageTransitionRule] = &[
    WorkPackageTransitionRule {
        from: S::Draft,
        command: C::RequestValidation,
        to: S::Validating,
    },
    WorkPackageTransitionRule {
        from: S::Validating,
        command: C::ValidationFailed,
        to: S::Blocked,
    },
    WorkPackageTransitionRule {
        from: S::Blocked,
        command: C::SelectNewRevisionAndValidate,
        to: S::Validating,
    },
    WorkPackageTransitionRule {
        from: S::Validating,
        command: C::PublishValidatedPackage,
        to: S::Offered,
    },
    WorkPackageTransitionRule {
        from: S::Offered,
        command: C::GrantLease,
        to: S::Active,
    },
    WorkPackageTransitionRule {
        from: S::ReworkReady,
        command: C::GrantLease,
        to: S::Active,
    },
    WorkPackageTransitionRule {
        from: S::Active,
        command: C::RecordCandidate,
        to: S::Verifying,
    },
    WorkPackageTransitionRule {
        from: S::Active,
        command: C::LoseAttempt,
        to: S::ReworkReady,
    },
    WorkPackageTransitionRule {
        from: S::Active,
        command: C::LoseAttempt,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Verifying,
        command: C::FinalizeVerification,
        to: S::Accepted,
    },
    WorkPackageTransitionRule {
        from: S::Verifying,
        command: C::FinalizeVerification,
        to: S::ReworkReady,
    },
    WorkPackageTransitionRule {
        from: S::Verifying,
        command: C::FinalizeVerification,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Accepted,
        command: C::EnqueueIntegration,
        to: S::Integrating,
    },
    WorkPackageTransitionRule {
        from: S::Integrating,
        command: C::ReportIntegrationConflict,
        to: S::RebaseRequired,
    },
    WorkPackageTransitionRule {
        from: S::RebaseRequired,
        command: C::BeginReintegration,
        to: S::Integrating,
    },
    WorkPackageTransitionRule {
        from: S::Integrating,
        command: C::ReportIntegrationFailed,
        to: S::ReworkReady,
    },
    WorkPackageTransitionRule {
        from: S::Integrating,
        command: C::ReportIntegrationFailed,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Integrating,
        command: C::MarkIntegrated,
        to: S::Integrated,
    },
    WorkPackageTransitionRule {
        from: S::Integrated,
        command: C::ClosePackage,
        to: S::Closed,
    },
    WorkPackageTransitionRule {
        from: S::Draft,
        command: C::CancelPackage,
        to: S::Cancelled,
    },
    WorkPackageTransitionRule {
        from: S::Validating,
        command: C::CancelPackage,
        to: S::Cancelled,
    },
    WorkPackageTransitionRule {
        from: S::Blocked,
        command: C::CancelPackage,
        to: S::Cancelled,
    },
    WorkPackageTransitionRule {
        from: S::Offered,
        command: C::CancelPackage,
        to: S::Cancelled,
    },
    WorkPackageTransitionRule {
        from: S::ReworkReady,
        command: C::CancelPackage,
        to: S::Cancelled,
    },
    WorkPackageTransitionRule {
        from: S::Active,
        command: C::CancelPackage,
        to: S::Cancelled,
    },
    WorkPackageTransitionRule {
        from: S::Draft,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Validating,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Blocked,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Offered,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Active,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Verifying,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::ReworkReady,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Accepted,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Integrating,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::RebaseRequired,
        command: C::SupersedePackage,
        to: S::Superseded,
    },
    WorkPackageTransitionRule {
        from: S::Draft,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Validating,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Blocked,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Offered,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Active,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Verifying,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::ReworkReady,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Accepted,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::Integrating,
        command: C::FailPackage,
        to: S::Failed,
    },
    WorkPackageTransitionRule {
        from: S::RebaseRequired,
        command: C::FailPackage,
        to: S::Failed,
    },
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewWorkPackage {
    pub id: PackageId,
    pub project_id: ProjectId,
    pub selected_revision_id: PackageRevisionId,
    pub selected_revision: PackageRevision,
    pub graph_version: u64,
    pub priority: i16,
    pub max_attempts: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkPackage {
    pub id: PackageId,
    pub project_id: ProjectId,
    pub selected_revision_id: PackageRevisionId,
    pub selected_revision: PackageRevision,
    pub state: WorkPackageState,
    pub graph_version: u64,
    pub priority: i16,
    pub max_attempts: u16,
    pub attempts_started: u16,
    pub last_fencing_token: u64,
    pub active_attempt_id: Option<AttemptId>,
    pub active_lease_id: Option<LeaseId>,
    pub active_fencing_token: Option<FencingToken>,
    pub accepted_submission_id: Option<SubmissionId>,
    pub current_integration_id: Option<IntegrationId>,
    pub integrated_integration_id: Option<IntegrationId>,
    pub integrated_commit: Option<GitObjectId>,
    pub version: AggregateVersion,
}

impl WorkPackage {
    pub fn new(seed: NewWorkPackage) -> Result<Self, DomainError> {
        if seed.max_attempts == 0 {
            return Err(DomainError::InvalidArgument {
                field: "max_attempts".into(),
                reason: "must be non-zero".into(),
            });
        }
        let package = Self {
            id: seed.id,
            project_id: seed.project_id,
            selected_revision_id: seed.selected_revision_id,
            selected_revision: seed.selected_revision,
            state: WorkPackageState::Draft,
            graph_version: seed.graph_version,
            priority: seed.priority,
            max_attempts: seed.max_attempts,
            attempts_started: 0,
            last_fencing_token: 0,
            active_attempt_id: None,
            active_lease_id: None,
            active_fencing_token: None,
            accepted_submission_id: None,
            current_integration_id: None,
            integrated_integration_id: None,
            integrated_commit: None,
            version: AggregateVersion::ZERO,
        };
        package.validate_invariants()?;
        Ok(package)
    }

    pub fn transition(
        &self,
        command: &WorkPackageCommand,
    ) -> Result<Transition<Self, WorkPackageEvent>, DomainError> {
        let event = self.decide(command)?;
        let aggregate = self.apply_event(&event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(&self, command: &WorkPackageCommand) -> Result<WorkPackageEvent, DomainError> {
        if command.expected_version() != self.version {
            return Err(DomainError::StaleVersion);
        }
        if !is_declared(self.state, command.kind()) {
            return Err(invalid_transition(self.state, command.kind().as_str()));
        }

        match command {
            WorkPackageCommand::RequestValidation {
                revision_exists,
                package_hash_matches,
                ..
            } => {
                if !revision_exists {
                    return Err(DomainError::NotFound {
                        resource: "package_revision",
                    });
                }
                if !package_hash_matches {
                    return Err(DomainError::PackageHashMismatch);
                }
                Ok(WorkPackageEvent::ValidationRequested)
            }
            WorkPackageCommand::ValidationFailed {
                validator_matches_revision,
                ..
            } => {
                if !validator_matches_revision {
                    return Err(DomainError::StaleVersion);
                }
                Ok(WorkPackageEvent::ValidationRejected)
            }
            WorkPackageCommand::SelectNewRevisionAndValidate {
                revision_id,
                revision,
                ..
            } => {
                if *revision <= self.selected_revision {
                    return Err(DomainError::InvalidArgument {
                        field: "revision".into(),
                        reason: "must be strictly newer".into(),
                    });
                }
                Ok(WorkPackageEvent::RevisionSelected {
                    revision_id: *revision_id,
                    revision: *revision,
                })
            }
            WorkPackageCommand::PublishValidatedPackage { readiness, .. } => {
                let failed_checks = readiness.failed_checks();
                if failed_checks.is_empty() {
                    Ok(WorkPackageEvent::PackagePublished)
                } else {
                    Err(DomainError::PackageNotReady { failed_checks })
                }
            }
            WorkPackageCommand::GrantLease {
                attempt_id,
                lease_id,
                readiness,
                ..
            } => {
                if self.attempts_started >= self.max_attempts {
                    return Err(DomainError::AttemptLimitReached);
                }
                if !readiness.is_ready()
                    || self.active_attempt_id.is_some()
                    || self.active_lease_id.is_some()
                {
                    return Err(DomainError::PackageNotClaimable);
                }
                let next = self.last_fencing_token.checked_add(1).ok_or(
                    DomainError::InvariantViolation {
                        invariant: "fencing_token_must_not_overflow",
                    },
                )?;
                Ok(WorkPackageEvent::LeaseGranted {
                    attempt_id: *attempt_id,
                    lease_id: *lease_id,
                    fencing_token: FencingToken::new(next)?,
                })
            }
            WorkPackageCommand::RecordCandidate {
                attempt_id,
                candidate_sealed,
                ..
            } => {
                self.require_active_attempt(*attempt_id)?;
                if !candidate_sealed {
                    return Err(DomainError::CandidateArtifactNotComplete);
                }
                Ok(WorkPackageEvent::CandidateRecorded {
                    attempt_id: *attempt_id,
                })
            }
            WorkPackageCommand::LoseAttempt {
                attempt_id,
                recoverable,
                ..
            } => {
                self.require_active_attempt(*attempt_id)?;
                Ok(WorkPackageEvent::AttemptLost {
                    attempt_id: *attempt_id,
                    recoverable: *recoverable,
                })
            }
            WorkPackageCommand::FinalizeVerification {
                attempt_id,
                submission_id,
                outcome,
                failed_checks,
                failure_dossier_complete,
                ..
            } => {
                self.require_active_attempt(*attempt_id)?;
                match outcome {
                    VerificationOutcome::Pass if !failed_checks.is_empty() => {
                        return Err(DomainError::SubmissionNotAcceptable {
                            failed_checks: failed_checks.clone(),
                        });
                    }
                    VerificationOutcome::Fail | VerificationOutcome::Inconclusive
                        if !failure_dossier_complete =>
                    {
                        return Err(DomainError::SubmissionNotAcceptable {
                            failed_checks: vec!["failure_dossier".into()],
                        });
                    }
                    _ => {}
                }
                Ok(WorkPackageEvent::VerificationFinalized {
                    attempt_id: *attempt_id,
                    submission_id: *submission_id,
                    outcome: *outcome,
                })
            }
            WorkPackageCommand::EnqueueIntegration {
                integration_id,
                lineage_matches,
                candidate_artifact_complete,
                ..
            } => {
                let mut failed_checks = Vec::new();
                if !lineage_matches {
                    failed_checks.push("lineage".into());
                }
                if !candidate_artifact_complete {
                    failed_checks.push("candidate_artifact_complete".into());
                }
                if self.accepted_submission_id.is_none() {
                    failed_checks.push("accepted_submission".into());
                }
                if !failed_checks.is_empty() {
                    return Err(DomainError::SubmissionNotAcceptable { failed_checks });
                }
                Ok(WorkPackageEvent::IntegrationEnqueued {
                    integration_id: *integration_id,
                })
            }
            WorkPackageCommand::ReportIntegrationConflict { integration_id, .. } => {
                self.require_current_integration(*integration_id)?;
                Ok(WorkPackageEvent::IntegrationConflictReported {
                    integration_id: *integration_id,
                })
            }
            WorkPackageCommand::BeginReintegration {
                integration_id,
                rebase_candidate_accepted,
                ..
            } => {
                if !rebase_candidate_accepted {
                    return Err(DomainError::InvalidTransition {
                        from: self.state.as_str().into(),
                        command: command.kind().as_str().into(),
                    });
                }
                Ok(WorkPackageEvent::ReintegrationBegan {
                    integration_id: *integration_id,
                })
            }
            WorkPackageCommand::ReportIntegrationFailed {
                integration_id,
                recoverable,
                ..
            } => {
                self.require_current_integration(*integration_id)?;
                Ok(WorkPackageEvent::IntegrationFailedReported {
                    integration_id: *integration_id,
                    recoverable: *recoverable,
                })
            }
            WorkPackageCommand::MarkIntegrated {
                integration_id,
                merge_commit,
                signed_merge_and_l5_passed,
                ..
            } => {
                self.require_current_integration(*integration_id)?;
                if !signed_merge_and_l5_passed {
                    return Err(DomainError::InvalidTransition {
                        from: self.state.as_str().into(),
                        command: command.kind().as_str().into(),
                    });
                }
                Ok(WorkPackageEvent::PackageIntegrated {
                    integration_id: *integration_id,
                    merge_commit: merge_commit.clone(),
                })
            }
            WorkPackageCommand::ClosePackage {
                traceability_updated,
                ..
            } => {
                if !traceability_updated {
                    return Err(DomainError::InvalidTransition {
                        from: self.state.as_str().into(),
                        command: command.kind().as_str().into(),
                    });
                }
                Ok(WorkPackageEvent::PackageClosed)
            }
            WorkPackageCommand::CancelPackage {
                active_lease_terminated,
                ..
            } => {
                if self.state == WorkPackageState::Active && !active_lease_terminated {
                    return Err(DomainError::InvalidTransition {
                        from: self.state.as_str().into(),
                        command: command.kind().as_str().into(),
                    });
                }
                Ok(WorkPackageEvent::PackageCancelled)
            }
            WorkPackageCommand::SupersedePackage {
                replacement_revision_id,
                ..
            } => Ok(WorkPackageEvent::PackageSuperseded {
                replacement_revision_id: *replacement_revision_id,
            }),
            WorkPackageCommand::FailPackage {
                policy_authorized,
                reason_code,
                ..
            } => {
                if !policy_authorized || reason_code.trim().is_empty() {
                    return Err(DomainError::PolicyDenied);
                }
                Ok(WorkPackageEvent::PackageFailed)
            }
        }
    }

    pub fn apply_event(&self, event: &WorkPackageEvent) -> Result<Self, DomainError> {
        let mut next = self.clone();
        match event {
            WorkPackageEvent::ValidationRequested if self.state == S::Draft => {
                next.state = S::Validating;
            }
            WorkPackageEvent::ValidationRejected if self.state == S::Validating => {
                next.state = S::Blocked;
            }
            WorkPackageEvent::RevisionSelected {
                revision_id,
                revision,
            } if self.state == S::Blocked && *revision > self.selected_revision => {
                next.selected_revision_id = *revision_id;
                next.selected_revision = *revision;
                next.state = S::Validating;
            }
            WorkPackageEvent::PackagePublished if self.state == S::Validating => {
                next.state = S::Offered;
            }
            WorkPackageEvent::LeaseGranted {
                attempt_id,
                lease_id,
                fencing_token,
            } if matches!(self.state, S::Offered | S::ReworkReady)
                && self.active_attempt_id.is_none()
                && fencing_token.get() > self.last_fencing_token =>
            {
                next.attempts_started = self.attempts_started.checked_add(1).ok_or(
                    DomainError::InvariantViolation {
                        invariant: "attempt_count_must_not_overflow",
                    },
                )?;
                if next.attempts_started > next.max_attempts {
                    return Err(DomainError::AttemptLimitReached);
                }
                next.last_fencing_token = fencing_token.get();
                next.active_attempt_id = Some(*attempt_id);
                next.active_lease_id = Some(*lease_id);
                next.active_fencing_token = Some(*fencing_token);
                next.state = S::Active;
            }
            WorkPackageEvent::CandidateRecorded { attempt_id }
                if self.state == S::Active && self.active_attempt_id == Some(*attempt_id) =>
            {
                next.active_lease_id = None;
                next.active_fencing_token = None;
                next.state = S::Verifying;
            }
            WorkPackageEvent::AttemptLost {
                attempt_id,
                recoverable,
            } if self.state == S::Active && self.active_attempt_id == Some(*attempt_id) => {
                next.clear_active_lineage();
                next.state = self.failure_target(*recoverable);
            }
            WorkPackageEvent::VerificationFinalized {
                attempt_id,
                submission_id,
                outcome,
            } if self.state == S::Verifying && self.active_attempt_id == Some(*attempt_id) => {
                next.clear_active_lineage();
                match outcome {
                    VerificationOutcome::Pass => {
                        next.accepted_submission_id = Some(*submission_id);
                        next.state = S::Accepted;
                    }
                    VerificationOutcome::Fail | VerificationOutcome::Inconclusive => {
                        next.state = self.failure_target(true);
                    }
                }
            }
            WorkPackageEvent::IntegrationEnqueued { integration_id }
                if self.state == S::Accepted && self.accepted_submission_id.is_some() =>
            {
                next.current_integration_id = Some(*integration_id);
                next.state = S::Integrating;
            }
            WorkPackageEvent::IntegrationConflictReported { integration_id }
                if self.state == S::Integrating
                    && self.current_integration_id == Some(*integration_id) =>
            {
                next.state = S::RebaseRequired;
            }
            WorkPackageEvent::ReintegrationBegan { integration_id }
                if self.state == S::RebaseRequired =>
            {
                next.current_integration_id = Some(*integration_id);
                next.state = S::Integrating;
            }
            WorkPackageEvent::IntegrationFailedReported {
                integration_id,
                recoverable,
            } if self.state == S::Integrating
                && self.current_integration_id == Some(*integration_id) =>
            {
                next.current_integration_id = None;
                next.accepted_submission_id = None;
                next.state = self.failure_target(*recoverable);
            }
            WorkPackageEvent::PackageIntegrated {
                integration_id,
                merge_commit,
            } if self.state == S::Integrating
                && self.current_integration_id == Some(*integration_id) =>
            {
                next.current_integration_id = None;
                next.integrated_integration_id = Some(*integration_id);
                next.integrated_commit = Some(merge_commit.clone());
                next.state = S::Integrated;
            }
            WorkPackageEvent::PackageClosed if self.state == S::Integrated => {
                next.state = S::Closed;
            }
            WorkPackageEvent::PackageCancelled
                if matches!(
                    self.state,
                    S::Draft | S::Validating | S::Blocked | S::Offered | S::ReworkReady | S::Active
                ) =>
            {
                next.clear_active_lineage();
                next.state = S::Cancelled;
            }
            WorkPackageEvent::PackageSuperseded { .. }
                if !matches!(
                    self.state,
                    S::Integrated | S::Closed | S::Cancelled | S::Superseded | S::Failed
                ) =>
            {
                next.clear_active_lineage();
                next.current_integration_id = None;
                next.accepted_submission_id = None;
                next.state = S::Superseded;
            }
            WorkPackageEvent::PackageFailed
                if !matches!(
                    self.state,
                    S::Integrated | S::Closed | S::Cancelled | S::Superseded | S::Failed
                ) =>
            {
                next.clear_active_lineage();
                next.current_integration_id = None;
                next.accepted_submission_id = None;
                next.state = S::Failed;
            }
            _ => return Err(invalid_transition(self.state, event.name())),
        }
        next.version = self.version.checked_next()?;
        next.validate_invariants()?;
        Ok(next)
    }

    pub fn replay(seed: NewWorkPackage, events: &[WorkPackageEvent]) -> Result<Self, DomainError> {
        let mut aggregate = Self::new(seed)?;
        for event in events {
            aggregate = aggregate.apply_event(event)?;
        }
        Ok(aggregate)
    }

    pub fn validate_invariants(&self) -> Result<(), DomainError> {
        if self.attempts_started > self.max_attempts {
            return Err(DomainError::InvariantViolation {
                invariant: "attempts_started_must_not_exceed_max_attempts",
            });
        }
        match self.state {
            S::Active
                if self.active_attempt_id.is_none()
                    || self.active_lease_id.is_none()
                    || self.active_fencing_token.is_none() =>
            {
                return Err(DomainError::InvariantViolation {
                    invariant: "active_package_requires_attempt_and_lease",
                });
            }
            S::Verifying
                if self.active_attempt_id.is_none()
                    || self.active_lease_id.is_some()
                    || self.active_fencing_token.is_some() =>
            {
                return Err(DomainError::InvariantViolation {
                    invariant: "verifying_package_requires_sealed_attempt_without_lease",
                });
            }
            S::Active | S::Verifying => {}
            _ if self.active_attempt_id.is_some()
                || self.active_lease_id.is_some()
                || self.active_fencing_token.is_some() =>
            {
                return Err(DomainError::InvariantViolation {
                    invariant: "inactive_package_must_not_have_active_attempt",
                });
            }
            _ => {}
        }
        let accepted_allowed = matches!(
            self.state,
            S::Accepted | S::Integrating | S::RebaseRequired | S::Integrated | S::Closed
        );
        if self.accepted_submission_id.is_some() != accepted_allowed {
            return Err(DomainError::InvariantViolation {
                invariant: "accepted_submission_pointer_must_match_package_state",
            });
        }
        let integration_active = matches!(self.state, S::Integrating | S::RebaseRequired);
        if self.current_integration_id.is_some() != integration_active {
            return Err(DomainError::InvariantViolation {
                invariant: "current_integration_pointer_must_match_package_state",
            });
        }
        let integrated = matches!(self.state, S::Integrated | S::Closed);
        if self.integrated_integration_id.is_some() != integrated
            || self.integrated_commit.is_some() != integrated
        {
            return Err(DomainError::InvariantViolation {
                invariant: "integrated_id_and_commit_must_appear_together",
            });
        }
        Ok(())
    }

    fn require_active_attempt(&self, attempt_id: AttemptId) -> Result<(), DomainError> {
        if self.active_attempt_id == Some(attempt_id) {
            Ok(())
        } else {
            Err(DomainError::StaleVersion)
        }
    }

    fn require_current_integration(
        &self,
        integration_id: IntegrationId,
    ) -> Result<(), DomainError> {
        if self.current_integration_id == Some(integration_id) {
            Ok(())
        } else {
            Err(DomainError::StaleVersion)
        }
    }

    fn clear_active_lineage(&mut self) {
        self.active_attempt_id = None;
        self.active_lease_id = None;
        self.active_fencing_token = None;
    }

    const fn failure_target(&self, recoverable: bool) -> WorkPackageState {
        if recoverable && self.attempts_started < self.max_attempts {
            WorkPackageState::ReworkReady
        } else {
            WorkPackageState::Failed
        }
    }
}

impl WorkPackageEvent {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::ValidationRequested => "validation_requested",
            Self::ValidationRejected => "validation_rejected",
            Self::RevisionSelected { .. } => "revision_selected",
            Self::PackagePublished => "package_published",
            Self::LeaseGranted { .. } => "lease_granted",
            Self::CandidateRecorded { .. } => "candidate_recorded",
            Self::AttemptLost { .. } => "attempt_lost",
            Self::VerificationFinalized { .. } => "verification_finalized",
            Self::IntegrationEnqueued { .. } => "integration_enqueued",
            Self::IntegrationConflictReported { .. } => "integration_conflict_reported",
            Self::ReintegrationBegan { .. } => "reintegration_began",
            Self::IntegrationFailedReported { .. } => "integration_failed_reported",
            Self::PackageIntegrated { .. } => "package_integrated",
            Self::PackageClosed => "package_closed",
            Self::PackageCancelled => "package_cancelled",
            Self::PackageSuperseded { .. } => "package_superseded",
            Self::PackageFailed => "package_failed",
        }
    }
}

fn is_declared(state: WorkPackageState, command: WorkPackageCommandKind) -> bool {
    TRANSITION_TABLE
        .iter()
        .any(|rule| rule.from == state && rule.command == command)
}

fn invalid_transition(state: WorkPackageState, command: &str) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.into(),
    }
}
