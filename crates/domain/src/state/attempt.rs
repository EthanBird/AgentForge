//! Attempt aggregate and deterministic wake/resume behavior.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, AttemptId, ExecutorId, FencingToken, GitObjectId, LeaseId, NodeId,
        PackageId, PackageRevisionId, ProtocolKey, ServerInstant, Sha256Digest, SubmissionId,
    },
    state::{Transition, submission::SubmissionState},
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Created,
    Leased,
    Preparing,
    Planning,
    Implementing,
    LocalVerify,
    WaitingInput,
    Candidate,
    IsolatedReview,
    CleanReproduce,
    Submitted,
    Passed,
    Rejected,
    Lost,
    Failed,
    Cancelled,
}

impl AttemptState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Leased => "leased",
            Self::Preparing => "preparing",
            Self::Planning => "planning",
            Self::Implementing => "implementing",
            Self::LocalVerify => "local_verify",
            Self::WaitingInput => "waiting_input",
            Self::Candidate => "candidate",
            Self::IsolatedReview => "isolated_review",
            Self::CleanReproduce => "clean_reproduce",
            Self::Submitted => "submitted",
            Self::Passed => "passed",
            Self::Rejected => "rejected",
            Self::Lost => "lost",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Passed | Self::Rejected | Self::Lost | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WakeCondition {
    ArtifactAccepted {
        artifact_id: ProtocolKey,
        digest: Sha256Digest,
    },
    QuestionAnswered {
        question_id: ProtocolKey,
    },
    PermissionDecided {
        request_id: ProtocolKey,
    },
    DependencyIntegrated {
        package_id: PackageId,
    },
    NotBefore {
        at: ServerInstant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WakeFact {
    ArtifactAccepted {
        artifact_id: ProtocolKey,
        digest: Sha256Digest,
    },
    QuestionAnswered {
        question_id: ProtocolKey,
    },
    PermissionDecided {
        request_id: ProtocolKey,
    },
    DependencyIntegrated {
        package_id: PackageId,
    },
    TimeReached {
        now: ServerInstant,
    },
}

impl WakeCondition {
    #[must_use]
    pub fn is_satisfied_by(&self, fact: &WakeFact) -> bool {
        match (self, fact) {
            (
                Self::ArtifactAccepted {
                    artifact_id,
                    digest,
                },
                WakeFact::ArtifactAccepted {
                    artifact_id: actual_id,
                    digest: actual_digest,
                },
            ) => artifact_id == actual_id && digest == actual_digest,
            (
                Self::QuestionAnswered { question_id },
                WakeFact::QuestionAnswered {
                    question_id: actual,
                },
            ) => question_id == actual,
            (
                Self::PermissionDecided { request_id },
                WakeFact::PermissionDecided { request_id: actual },
            ) => request_id == actual,
            (
                Self::DependencyIntegrated { package_id },
                WakeFact::DependencyIntegrated { package_id: actual },
            ) => package_id == actual,
            (Self::NotBefore { at }, WakeFact::TimeReached { now }) => now >= at,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AttemptCommandKind {
    AttachLease,
    StartPreparation,
    BaselineReady,
    ApproveExecutionPlan,
    StartLocalVerification,
    RequestFix,
    RecordCandidate,
    StartIsolatedReview,
    StartCleanReproduce,
    FinalizeSubmission,
    FinalizeRejectedSubmission,
    MarkAttemptPassed,
    MarkAttemptRejected,
    WaitFor,
    Wake,
    ReportProgress,
    MarkLost,
    MarkFailed,
    CancelAttempt,
}

impl AttemptCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AttachLease => "attach_lease",
            Self::StartPreparation => "start_preparation",
            Self::BaselineReady => "baseline_ready",
            Self::ApproveExecutionPlan => "approve_execution_plan",
            Self::StartLocalVerification => "start_local_verification",
            Self::RequestFix => "request_fix",
            Self::RecordCandidate => "record_candidate",
            Self::StartIsolatedReview => "start_isolated_review",
            Self::StartCleanReproduce => "start_clean_reproduce",
            Self::FinalizeSubmission => "finalize_submission",
            Self::FinalizeRejectedSubmission => "finalize_rejected_submission",
            Self::MarkAttemptPassed => "mark_attempt_passed",
            Self::MarkAttemptRejected => "mark_attempt_rejected",
            Self::WaitFor => "wait_for",
            Self::Wake => "wake",
            Self::ReportProgress => "report_progress",
            Self::MarkLost => "mark_lost",
            Self::MarkFailed => "mark_failed",
            Self::CancelAttempt => "cancel_attempt",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SemanticProgress {
    pub checkpoint_digest: Option<Sha256Digest>,
    pub acceptance_result_changed: bool,
    pub candidate_changed: bool,
    pub new_blocker_or_question: bool,
    pub milestone_changed_with_evidence: bool,
}

impl SemanticProgress {
    #[must_use]
    pub const fn is_semantic(&self) -> bool {
        self.checkpoint_digest.is_some()
            || self.acceptance_result_changed
            || self.candidate_changed
            || self.new_blocker_or_question
            || self.milestone_changed_with_evidence
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptCommand {
    AttachLease {
        expected_version: AggregateVersion,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        lineage_matches: bool,
    },
    StartPreparation {
        expected_version: AggregateVersion,
        token_is_current: bool,
        inputs_available: bool,
    },
    BaselineReady {
        expected_version: AggregateVersion,
        snapshot_matches: bool,
    },
    ApproveExecutionPlan {
        expected_version: AggregateVersion,
        plan_covers_contract: bool,
    },
    StartLocalVerification {
        expected_version: AggregateVersion,
        has_candidate_changes: bool,
    },
    RequestFix {
        expected_version: AggregateVersion,
        failure_is_fixable: bool,
        budget_available: bool,
    },
    RecordCandidate {
        expected_version: AggregateVersion,
        candidate_commit: GitObjectId,
        hard_checks_passed: bool,
    },
    StartIsolatedReview {
        expected_version: AggregateVersion,
        reviewer_id: ExecutorId,
    },
    StartCleanReproduce {
        expected_version: AggregateVersion,
        has_unresolved_high_finding: bool,
    },
    FinalizeSubmission {
        expected_version: AggregateVersion,
        submission_id: SubmissionId,
        run_outcome: SubmissionState,
        manifest_signed: bool,
        heads_match: bool,
    },
    FinalizeRejectedSubmission {
        expected_version: AggregateVersion,
        submission_id: SubmissionId,
        run_outcome: SubmissionState,
        failure_dossier_complete: bool,
    },
    MarkAttemptPassed {
        expected_version: AggregateVersion,
    },
    MarkAttemptRejected {
        expected_version: AggregateVersion,
    },
    WaitFor {
        expected_version: AggregateVersion,
        condition: WakeCondition,
    },
    Wake {
        expected_version: AggregateVersion,
        fact: WakeFact,
    },
    ReportProgress {
        expected_version: AggregateVersion,
        progress: SemanticProgress,
    },
    MarkLost {
        expected_version: AggregateVersion,
    },
    MarkFailed {
        expected_version: AggregateVersion,
        reason_code: String,
    },
    CancelAttempt {
        expected_version: AggregateVersion,
    },
}

impl AttemptCommand {
    #[must_use]
    pub const fn kind(&self) -> AttemptCommandKind {
        match self {
            Self::AttachLease { .. } => AttemptCommandKind::AttachLease,
            Self::StartPreparation { .. } => AttemptCommandKind::StartPreparation,
            Self::BaselineReady { .. } => AttemptCommandKind::BaselineReady,
            Self::ApproveExecutionPlan { .. } => AttemptCommandKind::ApproveExecutionPlan,
            Self::StartLocalVerification { .. } => AttemptCommandKind::StartLocalVerification,
            Self::RequestFix { .. } => AttemptCommandKind::RequestFix,
            Self::RecordCandidate { .. } => AttemptCommandKind::RecordCandidate,
            Self::StartIsolatedReview { .. } => AttemptCommandKind::StartIsolatedReview,
            Self::StartCleanReproduce { .. } => AttemptCommandKind::StartCleanReproduce,
            Self::FinalizeSubmission { .. } => AttemptCommandKind::FinalizeSubmission,
            Self::FinalizeRejectedSubmission { .. } => {
                AttemptCommandKind::FinalizeRejectedSubmission
            }
            Self::MarkAttemptPassed { .. } => AttemptCommandKind::MarkAttemptPassed,
            Self::MarkAttemptRejected { .. } => AttemptCommandKind::MarkAttemptRejected,
            Self::WaitFor { .. } => AttemptCommandKind::WaitFor,
            Self::Wake { .. } => AttemptCommandKind::Wake,
            Self::ReportProgress { .. } => AttemptCommandKind::ReportProgress,
            Self::MarkLost { .. } => AttemptCommandKind::MarkLost,
            Self::MarkFailed { .. } => AttemptCommandKind::MarkFailed,
            Self::CancelAttempt { .. } => AttemptCommandKind::CancelAttempt,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> AggregateVersion {
        match self {
            Self::AttachLease {
                expected_version, ..
            }
            | Self::StartPreparation {
                expected_version, ..
            }
            | Self::BaselineReady {
                expected_version, ..
            }
            | Self::ApproveExecutionPlan {
                expected_version, ..
            }
            | Self::StartLocalVerification {
                expected_version, ..
            }
            | Self::RequestFix {
                expected_version, ..
            }
            | Self::RecordCandidate {
                expected_version, ..
            }
            | Self::StartIsolatedReview {
                expected_version, ..
            }
            | Self::StartCleanReproduce {
                expected_version, ..
            }
            | Self::FinalizeSubmission {
                expected_version, ..
            }
            | Self::FinalizeRejectedSubmission {
                expected_version, ..
            }
            | Self::MarkAttemptPassed { expected_version }
            | Self::MarkAttemptRejected { expected_version }
            | Self::WaitFor {
                expected_version, ..
            }
            | Self::Wake {
                expected_version, ..
            }
            | Self::ReportProgress {
                expected_version, ..
            }
            | Self::MarkLost { expected_version }
            | Self::MarkFailed {
                expected_version, ..
            }
            | Self::CancelAttempt { expected_version } => *expected_version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AttemptEvent {
    LeaseAttached {
        lease_id: LeaseId,
    },
    PreparationStarted,
    BaselinePrepared,
    ExecutionPlanApproved,
    LocalVerificationStarted,
    FixRequested,
    CandidateRecorded {
        candidate_commit: GitObjectId,
    },
    IsolatedReviewStarted,
    CleanReproductionStarted,
    SubmissionFinalized {
        submission_id: SubmissionId,
        outcome: SubmissionState,
    },
    WaitStarted {
        condition: WakeCondition,
        resume_state: AttemptState,
    },
    Woken {
        resume_state: AttemptState,
    },
    SemanticProgressReported {
        checkpoint_digest: Option<Sha256Digest>,
    },
    AttemptPassed,
    AttemptRejected,
    AttemptLost,
    AttemptFailed,
    AttemptCancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptTransitionRule {
    pub from: AttemptState,
    pub command: AttemptCommandKind,
    pub to: AttemptState,
}

use AttemptCommandKind as C;
use AttemptState as S;

pub const TRANSITION_TABLE: &[AttemptTransitionRule] = &[
    AttemptTransitionRule {
        from: S::Created,
        command: C::AttachLease,
        to: S::Leased,
    },
    AttemptTransitionRule {
        from: S::Leased,
        command: C::StartPreparation,
        to: S::Preparing,
    },
    AttemptTransitionRule {
        from: S::Preparing,
        command: C::BaselineReady,
        to: S::Planning,
    },
    AttemptTransitionRule {
        from: S::Planning,
        command: C::ApproveExecutionPlan,
        to: S::Implementing,
    },
    AttemptTransitionRule {
        from: S::Implementing,
        command: C::StartLocalVerification,
        to: S::LocalVerify,
    },
    AttemptTransitionRule {
        from: S::LocalVerify,
        command: C::RequestFix,
        to: S::Implementing,
    },
    AttemptTransitionRule {
        from: S::LocalVerify,
        command: C::RecordCandidate,
        to: S::Candidate,
    },
    AttemptTransitionRule {
        from: S::Candidate,
        command: C::StartIsolatedReview,
        to: S::IsolatedReview,
    },
    AttemptTransitionRule {
        from: S::IsolatedReview,
        command: C::StartCleanReproduce,
        to: S::CleanReproduce,
    },
    AttemptTransitionRule {
        from: S::CleanReproduce,
        command: C::FinalizeSubmission,
        to: S::Submitted,
    },
    AttemptTransitionRule {
        from: S::Candidate,
        command: C::FinalizeRejectedSubmission,
        to: S::Submitted,
    },
    AttemptTransitionRule {
        from: S::IsolatedReview,
        command: C::FinalizeRejectedSubmission,
        to: S::Submitted,
    },
    AttemptTransitionRule {
        from: S::CleanReproduce,
        command: C::FinalizeRejectedSubmission,
        to: S::Submitted,
    },
    AttemptTransitionRule {
        from: S::Submitted,
        command: C::MarkAttemptPassed,
        to: S::Passed,
    },
    AttemptTransitionRule {
        from: S::Submitted,
        command: C::MarkAttemptRejected,
        to: S::Rejected,
    },
    AttemptTransitionRule {
        from: S::Planning,
        command: C::WaitFor,
        to: S::WaitingInput,
    },
    AttemptTransitionRule {
        from: S::Implementing,
        command: C::WaitFor,
        to: S::WaitingInput,
    },
    AttemptTransitionRule {
        from: S::WaitingInput,
        command: C::Wake,
        to: S::Planning,
    },
    AttemptTransitionRule {
        from: S::WaitingInput,
        command: C::Wake,
        to: S::Implementing,
    },
    AttemptTransitionRule {
        from: S::Preparing,
        command: C::ReportProgress,
        to: S::Preparing,
    },
    AttemptTransitionRule {
        from: S::Planning,
        command: C::ReportProgress,
        to: S::Planning,
    },
    AttemptTransitionRule {
        from: S::Implementing,
        command: C::ReportProgress,
        to: S::Implementing,
    },
    AttemptTransitionRule {
        from: S::LocalVerify,
        command: C::ReportProgress,
        to: S::LocalVerify,
    },
    AttemptTransitionRule {
        from: S::Created,
        command: C::MarkLost,
        to: S::Lost,
    },
    AttemptTransitionRule {
        from: S::Leased,
        command: C::MarkLost,
        to: S::Lost,
    },
    AttemptTransitionRule {
        from: S::Preparing,
        command: C::MarkLost,
        to: S::Lost,
    },
    AttemptTransitionRule {
        from: S::Planning,
        command: C::MarkLost,
        to: S::Lost,
    },
    AttemptTransitionRule {
        from: S::Implementing,
        command: C::MarkLost,
        to: S::Lost,
    },
    AttemptTransitionRule {
        from: S::LocalVerify,
        command: C::MarkLost,
        to: S::Lost,
    },
    AttemptTransitionRule {
        from: S::WaitingInput,
        command: C::MarkLost,
        to: S::Lost,
    },
    AttemptTransitionRule {
        from: S::Created,
        command: C::MarkFailed,
        to: S::Failed,
    },
    AttemptTransitionRule {
        from: S::Leased,
        command: C::MarkFailed,
        to: S::Failed,
    },
    AttemptTransitionRule {
        from: S::Preparing,
        command: C::MarkFailed,
        to: S::Failed,
    },
    AttemptTransitionRule {
        from: S::Planning,
        command: C::MarkFailed,
        to: S::Failed,
    },
    AttemptTransitionRule {
        from: S::Implementing,
        command: C::MarkFailed,
        to: S::Failed,
    },
    AttemptTransitionRule {
        from: S::LocalVerify,
        command: C::MarkFailed,
        to: S::Failed,
    },
    AttemptTransitionRule {
        from: S::WaitingInput,
        command: C::MarkFailed,
        to: S::Failed,
    },
    AttemptTransitionRule {
        from: S::Created,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::Leased,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::Preparing,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::Planning,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::Implementing,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::LocalVerify,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::WaitingInput,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::Candidate,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::IsolatedReview,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::CleanReproduce,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
    AttemptTransitionRule {
        from: S::Submitted,
        command: C::CancelAttempt,
        to: S::Cancelled,
    },
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewAttempt {
    pub id: AttemptId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub executor_id: ExecutorId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
    pub base_commit: GitObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Attempt {
    pub id: AttemptId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub executor_id: ExecutorId,
    pub node_id: NodeId,
    pub state: AttemptState,
    pub lease_id: Option<LeaseId>,
    pub fencing_token: FencingToken,
    pub base_commit: GitObjectId,
    pub wake_condition: Option<WakeCondition>,
    pub resume_state: Option<AttemptState>,
    pub semantic_progress_seq: u64,
    pub last_checkpoint_digest: Option<Sha256Digest>,
    pub candidate_commit: Option<GitObjectId>,
    pub submission_id: Option<SubmissionId>,
    pub submission_outcome: Option<SubmissionState>,
    pub version: AggregateVersion,
}

impl Attempt {
    #[must_use]
    pub fn new(seed: NewAttempt) -> Self {
        Self {
            id: seed.id,
            package_id: seed.package_id,
            revision_id: seed.revision_id,
            executor_id: seed.executor_id,
            node_id: seed.node_id,
            state: S::Created,
            lease_id: None,
            fencing_token: seed.fencing_token,
            base_commit: seed.base_commit,
            wake_condition: None,
            resume_state: None,
            semantic_progress_seq: 0,
            last_checkpoint_digest: None,
            candidate_commit: None,
            submission_id: None,
            submission_outcome: None,
            version: AggregateVersion::ZERO,
        }
    }

    pub fn transition(
        &self,
        command: &AttemptCommand,
    ) -> Result<Transition<Self, AttemptEvent>, DomainError> {
        let event = self.decide(command)?;
        let aggregate = self.apply_event(&event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(&self, command: &AttemptCommand) -> Result<AttemptEvent, DomainError> {
        if command.expected_version() != self.version {
            return Err(DomainError::StaleVersion);
        }
        if !self.is_declared(command.kind()) {
            return Err(invalid_transition(self.state, command.kind().as_str()));
        }
        match command {
            AttemptCommand::AttachLease {
                lease_id,
                fencing_token,
                lineage_matches,
                ..
            } => {
                if !lineage_matches || *fencing_token != self.fencing_token {
                    return Err(DomainError::StaleLease);
                }
                Ok(AttemptEvent::LeaseAttached {
                    lease_id: *lease_id,
                })
            }
            AttemptCommand::StartPreparation {
                token_is_current,
                inputs_available,
                ..
            } => {
                if !token_is_current {
                    return Err(DomainError::StaleLease);
                }
                if !inputs_available {
                    return Err(DomainError::DependencyUnavailable);
                }
                Ok(AttemptEvent::PreparationStarted)
            }
            AttemptCommand::BaselineReady {
                snapshot_matches, ..
            } => {
                if !snapshot_matches {
                    return Err(DomainError::EvidenceInvalid);
                }
                Ok(AttemptEvent::BaselinePrepared)
            }
            AttemptCommand::ApproveExecutionPlan {
                plan_covers_contract,
                ..
            } => {
                if !plan_covers_contract {
                    return Err(DomainError::ScopeViolation);
                }
                Ok(AttemptEvent::ExecutionPlanApproved)
            }
            AttemptCommand::StartLocalVerification {
                has_candidate_changes,
                ..
            } => {
                if !has_candidate_changes {
                    return Err(DomainError::InvalidArgument {
                        field: "candidate_changes".into(),
                        reason: "at least one candidate change is required".into(),
                    });
                }
                Ok(AttemptEvent::LocalVerificationStarted)
            }
            AttemptCommand::RequestFix {
                failure_is_fixable,
                budget_available,
                ..
            } => {
                if !failure_is_fixable {
                    return Err(DomainError::InvalidTransition {
                        from: self.state.as_str().into(),
                        command: command.kind().as_str().into(),
                    });
                }
                if !budget_available {
                    return Err(DomainError::BudgetExhausted);
                }
                Ok(AttemptEvent::FixRequested)
            }
            AttemptCommand::RecordCandidate {
                candidate_commit,
                hard_checks_passed,
                ..
            } => {
                if !hard_checks_passed {
                    return Err(DomainError::EvidenceInvalid);
                }
                Ok(AttemptEvent::CandidateRecorded {
                    candidate_commit: candidate_commit.clone(),
                })
            }
            AttemptCommand::StartIsolatedReview { reviewer_id, .. } => {
                if *reviewer_id == self.executor_id {
                    return Err(DomainError::PolicyDenied);
                }
                Ok(AttemptEvent::IsolatedReviewStarted)
            }
            AttemptCommand::StartCleanReproduce {
                has_unresolved_high_finding,
                ..
            } => {
                if *has_unresolved_high_finding {
                    return Err(DomainError::EvidenceInvalid);
                }
                Ok(AttemptEvent::CleanReproductionStarted)
            }
            AttemptCommand::FinalizeSubmission {
                submission_id,
                run_outcome,
                manifest_signed,
                heads_match,
                ..
            } => {
                if *run_outcome != SubmissionState::Pass || !manifest_signed || !heads_match {
                    return Err(DomainError::SubmissionNotAcceptable {
                        failed_checks: vec!["candidate_ready".into()],
                    });
                }
                Ok(AttemptEvent::SubmissionFinalized {
                    submission_id: *submission_id,
                    outcome: *run_outcome,
                })
            }
            AttemptCommand::FinalizeRejectedSubmission {
                submission_id,
                run_outcome,
                failure_dossier_complete,
                ..
            } => {
                if !matches!(
                    run_outcome,
                    SubmissionState::Fail | SubmissionState::Inconclusive
                ) || !failure_dossier_complete
                {
                    return Err(DomainError::SubmissionNotAcceptable {
                        failed_checks: vec!["failure_dossier".into()],
                    });
                }
                Ok(AttemptEvent::SubmissionFinalized {
                    submission_id: *submission_id,
                    outcome: *run_outcome,
                })
            }
            AttemptCommand::MarkAttemptPassed { .. } => {
                if self.submission_outcome != Some(SubmissionState::Pass) {
                    return Err(DomainError::InvalidTransition {
                        from: self.state.as_str().into(),
                        command: command.kind().as_str().into(),
                    });
                }
                Ok(AttemptEvent::AttemptPassed)
            }
            AttemptCommand::MarkAttemptRejected { .. } => {
                if !matches!(
                    self.submission_outcome,
                    Some(SubmissionState::Fail | SubmissionState::Inconclusive)
                ) {
                    return Err(DomainError::InvalidTransition {
                        from: self.state.as_str().into(),
                        command: command.kind().as_str().into(),
                    });
                }
                Ok(AttemptEvent::AttemptRejected)
            }
            AttemptCommand::WaitFor { condition, .. } => Ok(AttemptEvent::WaitStarted {
                condition: condition.clone(),
                resume_state: self.state,
            }),
            AttemptCommand::Wake { fact, .. } => {
                let condition =
                    self.wake_condition
                        .as_ref()
                        .ok_or(DomainError::InvariantViolation {
                            invariant: "waiting_attempt_requires_wake_condition",
                        })?;
                if !condition.is_satisfied_by(fact) {
                    return Err(DomainError::WakeConditionUnsatisfied);
                }
                let resume_state = self.resume_state.ok_or(DomainError::InvariantViolation {
                    invariant: "waiting_attempt_requires_resume_state",
                })?;
                Ok(AttemptEvent::Woken { resume_state })
            }
            AttemptCommand::ReportProgress { progress, .. } => {
                if !progress.is_semantic() {
                    return Err(DomainError::InvalidArgument {
                        field: "progress".into(),
                        reason: "heartbeats do not count as semantic progress".into(),
                    });
                }
                if progress.checkpoint_digest.is_some()
                    && progress.checkpoint_digest == self.last_checkpoint_digest
                    && !progress.acceptance_result_changed
                    && !progress.candidate_changed
                    && !progress.new_blocker_or_question
                    && !progress.milestone_changed_with_evidence
                {
                    return Err(DomainError::InvalidArgument {
                        field: "progress".into(),
                        reason: "duplicate checkpoint is not semantic progress".into(),
                    });
                }
                Ok(AttemptEvent::SemanticProgressReported {
                    checkpoint_digest: progress.checkpoint_digest,
                })
            }
            AttemptCommand::MarkLost { .. } => Ok(AttemptEvent::AttemptLost),
            AttemptCommand::MarkFailed { reason_code, .. } => {
                if reason_code.trim().is_empty() {
                    return Err(DomainError::InvalidArgument {
                        field: "reason_code".into(),
                        reason: "must not be empty".into(),
                    });
                }
                Ok(AttemptEvent::AttemptFailed)
            }
            AttemptCommand::CancelAttempt { .. } => Ok(AttemptEvent::AttemptCancelled),
        }
    }

    pub fn apply_event(&self, event: &AttemptEvent) -> Result<Self, DomainError> {
        let mut next = self.clone();
        match event {
            AttemptEvent::LeaseAttached { lease_id } if self.state == S::Created => {
                next.lease_id = Some(*lease_id);
                next.state = S::Leased;
            }
            AttemptEvent::PreparationStarted if self.state == S::Leased => {
                next.state = S::Preparing
            }
            AttemptEvent::BaselinePrepared if self.state == S::Preparing => {
                next.state = S::Planning
            }
            AttemptEvent::ExecutionPlanApproved if self.state == S::Planning => {
                next.state = S::Implementing;
            }
            AttemptEvent::LocalVerificationStarted if self.state == S::Implementing => {
                next.state = S::LocalVerify;
            }
            AttemptEvent::FixRequested if self.state == S::LocalVerify => {
                next.state = S::Implementing;
            }
            AttemptEvent::CandidateRecorded { candidate_commit }
                if self.state == S::LocalVerify && self.candidate_commit.is_none() =>
            {
                next.candidate_commit = Some(candidate_commit.clone());
                next.state = S::Candidate;
            }
            AttemptEvent::IsolatedReviewStarted if self.state == S::Candidate => {
                next.state = S::IsolatedReview;
            }
            AttemptEvent::CleanReproductionStarted if self.state == S::IsolatedReview => {
                next.state = S::CleanReproduce;
            }
            AttemptEvent::SubmissionFinalized {
                submission_id,
                outcome,
            } if (self.state == S::CleanReproduce && *outcome == SubmissionState::Pass)
                || (matches!(
                    self.state,
                    S::Candidate | S::IsolatedReview | S::CleanReproduce
                ) && matches!(
                    outcome,
                    SubmissionState::Fail | SubmissionState::Inconclusive
                )) =>
            {
                next.submission_id = Some(*submission_id);
                next.submission_outcome = Some(*outcome);
                next.state = S::Submitted;
            }
            AttemptEvent::AttemptPassed
                if self.state == S::Submitted
                    && self.submission_outcome == Some(SubmissionState::Pass) =>
            {
                next.state = S::Passed;
            }
            AttemptEvent::AttemptRejected
                if self.state == S::Submitted
                    && matches!(
                        self.submission_outcome,
                        Some(SubmissionState::Fail | SubmissionState::Inconclusive)
                    ) =>
            {
                next.state = S::Rejected;
            }
            AttemptEvent::WaitStarted {
                condition,
                resume_state,
            } if matches!(self.state, S::Planning | S::Implementing)
                && *resume_state == self.state =>
            {
                next.wake_condition = Some(condition.clone());
                next.resume_state = Some(*resume_state);
                next.state = S::WaitingInput;
            }
            AttemptEvent::Woken { resume_state }
                if self.state == S::WaitingInput
                    && self.resume_state == Some(*resume_state)
                    && matches!(resume_state, S::Planning | S::Implementing) =>
            {
                next.wake_condition = None;
                next.resume_state = None;
                next.state = *resume_state;
            }
            AttemptEvent::SemanticProgressReported { checkpoint_digest }
                if matches!(
                    self.state,
                    S::Preparing | S::Planning | S::Implementing | S::LocalVerify
                ) =>
            {
                next.semantic_progress_seq = self.semantic_progress_seq.checked_add(1).ok_or(
                    DomainError::InvariantViolation {
                        invariant: "semantic_progress_seq_must_not_overflow",
                    },
                )?;
                if checkpoint_digest.is_some() {
                    next.last_checkpoint_digest = *checkpoint_digest;
                }
            }
            AttemptEvent::AttemptLost if self.accepts_author_terminalization() => {
                next.clear_wait();
                next.state = S::Lost;
            }
            AttemptEvent::AttemptFailed if self.accepts_author_terminalization() => {
                next.clear_wait();
                next.state = S::Failed;
            }
            AttemptEvent::AttemptCancelled if self.is_nonterminal() => {
                next.clear_wait();
                next.state = S::Cancelled;
            }
            _ => return Err(invalid_transition(self.state, event.name())),
        }
        next.version = self.version.checked_next()?;
        next.validate_invariants()?;
        Ok(next)
    }

    pub fn replay(seed: NewAttempt, events: &[AttemptEvent]) -> Result<Self, DomainError> {
        let mut aggregate = Self::new(seed);
        for event in events {
            aggregate = aggregate.apply_event(event)?;
        }
        Ok(aggregate)
    }

    pub fn validate_invariants(&self) -> Result<(), DomainError> {
        let waiting = self.state == S::WaitingInput;
        if self.wake_condition.is_some() != waiting || self.resume_state.is_some() != waiting {
            return Err(DomainError::InvariantViolation {
                invariant: "wake_condition_and_resume_state_must_match_waiting_state",
            });
        }
        if let Some(resume) = self.resume_state
            && !matches!(resume, S::Planning | S::Implementing)
        {
            return Err(DomainError::InvariantViolation {
                invariant: "waiting_resume_state_must_be_safe",
            });
        }
        let candidate_required = matches!(
            self.state,
            S::Candidate
                | S::IsolatedReview
                | S::CleanReproduce
                | S::Submitted
                | S::Passed
                | S::Rejected
        );
        if candidate_required && self.candidate_commit.is_none() {
            return Err(DomainError::InvariantViolation {
                invariant: "post_candidate_state_requires_immutable_commit",
            });
        }
        let submission_required = matches!(self.state, S::Submitted | S::Passed | S::Rejected);
        if self.submission_id.is_some() != self.submission_outcome.is_some()
            || (submission_required && self.submission_id.is_none())
            || (!submission_required && self.state != S::Cancelled && self.submission_id.is_some())
        {
            return Err(DomainError::InvariantViolation {
                invariant: "submission_facts_must_match_attempt_state",
            });
        }
        Ok(())
    }

    fn is_declared(&self, kind: AttemptCommandKind) -> bool {
        TRANSITION_TABLE
            .iter()
            .any(|rule| rule.from == self.state && rule.command == kind)
    }

    const fn is_nonterminal(&self) -> bool {
        !self.state.is_terminal()
    }

    /// Loss/failure is an author-runtime outcome and is no longer meaningful
    /// after the Candidate commit is sealed. Post-seal failures must create a
    /// terminal rejected Submission through the verification pipeline.
    const fn accepts_author_terminalization(&self) -> bool {
        matches!(
            self.state,
            S::Created
                | S::Leased
                | S::Preparing
                | S::Planning
                | S::Implementing
                | S::LocalVerify
                | S::WaitingInput
        )
    }

    fn clear_wait(&mut self) {
        self.wake_condition = None;
        self.resume_state = None;
    }
}

impl AttemptEvent {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::LeaseAttached { .. } => "lease_attached",
            Self::PreparationStarted => "preparation_started",
            Self::BaselinePrepared => "baseline_prepared",
            Self::ExecutionPlanApproved => "execution_plan_approved",
            Self::LocalVerificationStarted => "local_verification_started",
            Self::FixRequested => "fix_requested",
            Self::CandidateRecorded { .. } => "candidate_recorded",
            Self::IsolatedReviewStarted => "isolated_review_started",
            Self::CleanReproductionStarted => "clean_reproduction_started",
            Self::SubmissionFinalized { .. } => "submission_finalized",
            Self::WaitStarted { .. } => "wait_started",
            Self::Woken { .. } => "woken",
            Self::SemanticProgressReported { .. } => "semantic_progress_reported",
            Self::AttemptPassed => "attempt_passed",
            Self::AttemptRejected => "attempt_rejected",
            Self::AttemptLost => "attempt_lost",
            Self::AttemptFailed => "attempt_failed",
            Self::AttemptCancelled => "attempt_cancelled",
        }
    }
}

fn invalid_transition(state: AttemptState, command: &str) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.into(),
    }
}
