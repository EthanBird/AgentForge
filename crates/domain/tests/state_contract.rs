use std::collections::BTreeSet;

use agentforge_domain::{
    error::DomainError,
    ids::*,
    state::{
        attempt::{
            Attempt, AttemptCommand, AttemptCommandKind, AttemptEvent, AttemptState, NewAttempt,
            SemanticProgress, TRANSITION_TABLE as ATTEMPT_TRANSITIONS, WakeCondition, WakeFact,
        },
        lease::{
            GrantLease, Lease, LeaseCommand, LeaseCommandKind, LeaseState,
            TRANSITION_TABLE as LEASE_TRANSITIONS,
        },
        submission::{
            AcceptanceFacts, CompletedStage, CriterionOutcome, FailureDossier, Submission,
            SubmissionCommand, SubmissionCommandKind, SubmissionEvent, SubmissionRecord,
            SubmissionState, TRANSITION_TABLE as SUBMISSION_TRANSITIONS,
        },
        work_package::{
            ClaimReadiness, NewWorkPackage, PublishReadiness,
            TRANSITION_TABLE as PACKAGE_TRANSITIONS, VerificationOutcome, WorkPackage,
            WorkPackageCommand, WorkPackageCommandKind, WorkPackageState,
        },
    },
};
use time::{Duration, macros::datetime};
use uuid::Uuid;

fn id<T: From<Uuid>>(byte: u8) -> T {
    T::from(Uuid::from_bytes([byte; 16]))
}

fn revision(value: u32) -> PackageRevision {
    PackageRevision::new(value).expect("valid revision")
}

fn token(value: u64) -> FencingToken {
    FencingToken::new(value).expect("valid token")
}

fn oid(byte: u8) -> GitObjectId {
    GitObjectId::new(format!("{byte:02x}").repeat(20)).expect("valid oid")
}

fn at(seconds: i64) -> ServerInstant {
    ServerInstant(datetime!(2026-08-08 00:00 UTC) + Duration::seconds(seconds))
}

fn new_package(max_attempts: u16) -> WorkPackage {
    WorkPackage::new(NewWorkPackage {
        id: id(1),
        project_id: id(2),
        selected_revision_id: id(3),
        selected_revision: revision(1),
        graph_version: 1,
        priority: 0,
        max_attempts,
    })
    .expect("valid package")
}

fn wp_apply(package: WorkPackage, command: WorkPackageCommand) -> WorkPackage {
    package
        .transition(&command)
        .expect("valid transition")
        .aggregate
}

fn reach_package(target: WorkPackageState, max_attempts: u16) -> WorkPackage {
    let mut package = new_package(max_attempts);
    if target == WorkPackageState::Draft {
        return package;
    }
    if target == WorkPackageState::Cancelled {
        let version = package.version;
        return wp_apply(
            package,
            WorkPackageCommand::CancelPackage {
                expected_version: version,
                active_lease_terminated: true,
            },
        );
    }
    if target == WorkPackageState::Superseded {
        let version = package.version;
        return wp_apply(
            package,
            WorkPackageCommand::SupersedePackage {
                expected_version: version,
                replacement_revision_id: id(4),
            },
        );
    }
    if target == WorkPackageState::Failed {
        let version = package.version;
        return wp_apply(
            package,
            WorkPackageCommand::FailPackage {
                expected_version: version,
                policy_authorized: true,
                reason_code: "fatal_policy".into(),
            },
        );
    }

    let version = package.version;
    package = wp_apply(
        package,
        WorkPackageCommand::RequestValidation {
            expected_version: version,
            revision_exists: true,
            package_hash_matches: true,
        },
    );
    if target == WorkPackageState::Validating {
        return package;
    }
    if target == WorkPackageState::Blocked {
        let version = package.version;
        return wp_apply(
            package,
            WorkPackageCommand::ValidationFailed {
                expected_version: version,
                validator_matches_revision: true,
            },
        );
    }
    let version = package.version;
    package = wp_apply(
        package,
        WorkPackageCommand::PublishValidatedPackage {
            expected_version: version,
            readiness: PublishReadiness {
                dor_passed: true,
                dag_valid: true,
                budget_available: true,
                permissions_valid: true,
            },
        },
    );
    if target == WorkPackageState::Offered {
        return package;
    }
    let version = package.version;
    package = wp_apply(
        package,
        WorkPackageCommand::GrantLease {
            expected_version: version,
            attempt_id: id(5),
            lease_id: id(6),
            readiness: ClaimReadiness {
                dependencies_satisfied: true,
                budget_available: true,
                no_active_lease: true,
            },
        },
    );
    if target == WorkPackageState::Active {
        return package;
    }
    if target == WorkPackageState::ReworkReady {
        let version = package.version;
        return wp_apply(
            package,
            WorkPackageCommand::LoseAttempt {
                expected_version: version,
                attempt_id: id(5),
                recoverable: true,
            },
        );
    }
    let version = package.version;
    package = wp_apply(
        package,
        WorkPackageCommand::RecordCandidate {
            expected_version: version,
            attempt_id: id(5),
            candidate_sealed: true,
        },
    );
    if target == WorkPackageState::Verifying {
        return package;
    }
    let version = package.version;
    package = wp_apply(
        package,
        WorkPackageCommand::FinalizeVerification {
            expected_version: version,
            attempt_id: id(5),
            submission_id: id(7),
            outcome: VerificationOutcome::Pass,
            failed_checks: vec![],
            failure_dossier_complete: true,
        },
    );
    if target == WorkPackageState::Accepted {
        return package;
    }
    let version = package.version;
    package = wp_apply(
        package,
        WorkPackageCommand::EnqueueIntegration {
            expected_version: version,
            integration_id: id(8),
            lineage_matches: true,
            candidate_artifact_complete: true,
        },
    );
    if target == WorkPackageState::Integrating {
        return package;
    }
    if target == WorkPackageState::RebaseRequired {
        let version = package.version;
        return wp_apply(
            package,
            WorkPackageCommand::ReportIntegrationConflict {
                expected_version: version,
                integration_id: id(8),
            },
        );
    }
    let version = package.version;
    package = wp_apply(
        package,
        WorkPackageCommand::MarkIntegrated {
            expected_version: version,
            integration_id: id(8),
            merge_commit: oid(9),
            signed_merge_and_l5_passed: true,
        },
    );
    if target == WorkPackageState::Integrated {
        return package;
    }
    assert_eq!(target, WorkPackageState::Closed);
    let version = package.version;
    wp_apply(
        package,
        WorkPackageCommand::ClosePackage {
            expected_version: version,
            traceability_updated: true,
        },
    )
}

fn package_command(
    kind: WorkPackageCommandKind,
    package: &WorkPackage,
    target: WorkPackageState,
) -> WorkPackageCommand {
    let expected_version = package.version;
    match kind {
        WorkPackageCommandKind::RequestValidation => WorkPackageCommand::RequestValidation {
            expected_version,
            revision_exists: true,
            package_hash_matches: true,
        },
        WorkPackageCommandKind::ValidationFailed => WorkPackageCommand::ValidationFailed {
            expected_version,
            validator_matches_revision: true,
        },
        WorkPackageCommandKind::SelectNewRevisionAndValidate => {
            WorkPackageCommand::SelectNewRevisionAndValidate {
                expected_version,
                revision_id: id(10),
                revision: revision(package.selected_revision.get() + 1),
            }
        }
        WorkPackageCommandKind::PublishValidatedPackage => {
            WorkPackageCommand::PublishValidatedPackage {
                expected_version,
                readiness: PublishReadiness {
                    dor_passed: true,
                    dag_valid: true,
                    budget_available: true,
                    permissions_valid: true,
                },
            }
        }
        WorkPackageCommandKind::GrantLease => WorkPackageCommand::GrantLease {
            expected_version,
            attempt_id: id(11),
            lease_id: id(12),
            readiness: ClaimReadiness {
                dependencies_satisfied: true,
                budget_available: true,
                no_active_lease: true,
            },
        },
        WorkPackageCommandKind::RecordCandidate => WorkPackageCommand::RecordCandidate {
            expected_version,
            attempt_id: package.active_attempt_id.unwrap_or_else(|| id(5)),
            candidate_sealed: true,
        },
        WorkPackageCommandKind::LoseAttempt => WorkPackageCommand::LoseAttempt {
            expected_version,
            attempt_id: package.active_attempt_id.unwrap_or_else(|| id(5)),
            recoverable: target == WorkPackageState::ReworkReady,
        },
        WorkPackageCommandKind::FinalizeVerification => {
            let pass = target == WorkPackageState::Accepted;
            WorkPackageCommand::FinalizeVerification {
                expected_version,
                attempt_id: package.active_attempt_id.unwrap_or_else(|| id(5)),
                submission_id: id(13),
                outcome: if pass {
                    VerificationOutcome::Pass
                } else {
                    VerificationOutcome::Fail
                },
                failed_checks: vec![],
                failure_dossier_complete: true,
            }
        }
        WorkPackageCommandKind::EnqueueIntegration => WorkPackageCommand::EnqueueIntegration {
            expected_version,
            integration_id: id(14),
            lineage_matches: true,
            candidate_artifact_complete: true,
        },
        WorkPackageCommandKind::ReportIntegrationConflict => {
            WorkPackageCommand::ReportIntegrationConflict {
                expected_version,
                integration_id: package.current_integration_id.unwrap_or_else(|| id(8)),
            }
        }
        WorkPackageCommandKind::BeginReintegration => WorkPackageCommand::BeginReintegration {
            expected_version,
            integration_id: id(15),
            rebase_candidate_accepted: true,
        },
        WorkPackageCommandKind::ReportIntegrationFailed => {
            WorkPackageCommand::ReportIntegrationFailed {
                expected_version,
                integration_id: package.current_integration_id.unwrap_or_else(|| id(8)),
                recoverable: target == WorkPackageState::ReworkReady,
            }
        }
        WorkPackageCommandKind::MarkIntegrated => WorkPackageCommand::MarkIntegrated {
            expected_version,
            integration_id: package.current_integration_id.unwrap_or_else(|| id(8)),
            merge_commit: oid(16),
            signed_merge_and_l5_passed: true,
        },
        WorkPackageCommandKind::ClosePackage => WorkPackageCommand::ClosePackage {
            expected_version,
            traceability_updated: true,
        },
        WorkPackageCommandKind::CancelPackage => WorkPackageCommand::CancelPackage {
            expected_version,
            active_lease_terminated: true,
        },
        WorkPackageCommandKind::SupersedePackage => WorkPackageCommand::SupersedePackage {
            expected_version,
            replacement_revision_id: id(17),
        },
        WorkPackageCommandKind::FailPackage => WorkPackageCommand::FailPackage {
            expected_version,
            policy_authorized: true,
            reason_code: "unrecoverable".into(),
        },
    }
}

const PACKAGE_STATES: &[WorkPackageState] = &[
    WorkPackageState::Draft,
    WorkPackageState::Validating,
    WorkPackageState::Blocked,
    WorkPackageState::Offered,
    WorkPackageState::Active,
    WorkPackageState::Verifying,
    WorkPackageState::ReworkReady,
    WorkPackageState::Accepted,
    WorkPackageState::Integrating,
    WorkPackageState::RebaseRequired,
    WorkPackageState::Integrated,
    WorkPackageState::Closed,
    WorkPackageState::Cancelled,
    WorkPackageState::Superseded,
    WorkPackageState::Failed,
];

const PACKAGE_COMMANDS: &[WorkPackageCommandKind] = &[
    WorkPackageCommandKind::RequestValidation,
    WorkPackageCommandKind::ValidationFailed,
    WorkPackageCommandKind::SelectNewRevisionAndValidate,
    WorkPackageCommandKind::PublishValidatedPackage,
    WorkPackageCommandKind::GrantLease,
    WorkPackageCommandKind::RecordCandidate,
    WorkPackageCommandKind::LoseAttempt,
    WorkPackageCommandKind::FinalizeVerification,
    WorkPackageCommandKind::EnqueueIntegration,
    WorkPackageCommandKind::ReportIntegrationConflict,
    WorkPackageCommandKind::BeginReintegration,
    WorkPackageCommandKind::ReportIntegrationFailed,
    WorkPackageCommandKind::MarkIntegrated,
    WorkPackageCommandKind::ClosePackage,
    WorkPackageCommandKind::CancelPackage,
    WorkPackageCommandKind::SupersedePackage,
    WorkPackageCommandKind::FailPackage,
];

#[test]
fn every_declared_work_package_transition_passes() {
    for rule in PACKAGE_TRANSITIONS {
        let max_attempts = if rule.to == WorkPackageState::Failed
            && matches!(
                rule.command,
                WorkPackageCommandKind::LoseAttempt
                    | WorkPackageCommandKind::FinalizeVerification
                    | WorkPackageCommandKind::ReportIntegrationFailed
            ) {
            1
        } else {
            3
        };
        let package = reach_package(rule.from, max_attempts);
        let command = package_command(rule.command, &package, rule.to);
        let next = package
            .transition(&command)
            .unwrap_or_else(|error| panic!("{rule:?} failed: {error:?}"))
            .aggregate;
        assert_eq!(next.state, rule.to, "{rule:?}");
        assert_eq!(next.version.get(), package.version.get() + 1);
    }
}

#[test]
fn every_undeclared_work_package_transition_is_stably_invalid() {
    for state in PACKAGE_STATES {
        let package = reach_package(*state, 3);
        for kind in PACKAGE_COMMANDS {
            if PACKAGE_TRANSITIONS
                .iter()
                .any(|rule| rule.from == *state && rule.command == *kind)
            {
                continue;
            }
            let command = package_command(*kind, &package, WorkPackageState::Failed);
            let error = package
                .transition(&command)
                .expect_err("undeclared transition");
            assert!(matches!(error, DomainError::InvalidTransition { .. }));
            assert_eq!(error.code(), "AF_TRANSITION_INVALID");
        }
    }
}

fn new_attempt() -> Attempt {
    Attempt::new(NewAttempt {
        id: id(20),
        package_id: id(21),
        revision_id: id(22),
        executor_id: id(23),
        node_id: id(24),
        fencing_token: token(1),
        base_commit: oid(25),
    })
}

fn attempt_apply(attempt: Attempt, command: AttemptCommand) -> Attempt {
    attempt
        .transition(&command)
        .expect("valid attempt command")
        .aggregate
}

fn reach_attempt(
    target: AttemptState,
    waiting_resume: AttemptState,
    rejected_submission: bool,
) -> Attempt {
    let mut attempt = new_attempt();
    if target == AttemptState::Created {
        return attempt;
    }
    if target == AttemptState::Lost {
        let version = attempt.version;
        return attempt_apply(
            attempt,
            AttemptCommand::MarkLost {
                expected_version: version,
            },
        );
    }
    if target == AttemptState::Failed {
        let version = attempt.version;
        return attempt_apply(
            attempt,
            AttemptCommand::MarkFailed {
                expected_version: version,
                reason_code: "failed".into(),
            },
        );
    }
    if target == AttemptState::Cancelled {
        let version = attempt.version;
        return attempt_apply(
            attempt,
            AttemptCommand::CancelAttempt {
                expected_version: version,
            },
        );
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::AttachLease {
            expected_version: version,
            lease_id: id(26),
            fencing_token: token(1),
            lineage_matches: true,
        },
    );
    if target == AttemptState::Leased {
        return attempt;
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::StartPreparation {
            expected_version: version,
            token_is_current: true,
            inputs_available: true,
        },
    );
    if target == AttemptState::Preparing {
        return attempt;
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::BaselineReady {
            expected_version: version,
            snapshot_matches: true,
        },
    );
    if target == AttemptState::Planning {
        return attempt;
    }
    if target == AttemptState::WaitingInput && waiting_resume == AttemptState::Planning {
        let version = attempt.version;
        return attempt_apply(
            attempt,
            AttemptCommand::WaitFor {
                expected_version: version,
                condition: WakeCondition::NotBefore { at: at(10) },
            },
        );
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::ApproveExecutionPlan {
            expected_version: version,
            plan_covers_contract: true,
        },
    );
    if target == AttemptState::Implementing {
        return attempt;
    }
    if target == AttemptState::WaitingInput {
        let version = attempt.version;
        return attempt_apply(
            attempt,
            AttemptCommand::WaitFor {
                expected_version: version,
                condition: WakeCondition::NotBefore { at: at(10) },
            },
        );
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::StartLocalVerification {
            expected_version: version,
            has_candidate_changes: true,
        },
    );
    if target == AttemptState::LocalVerify {
        return attempt;
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::RecordCandidate {
            expected_version: version,
            candidate_commit: oid(27),
            hard_checks_passed: true,
        },
    );
    if target == AttemptState::Candidate {
        return attempt;
    }
    if target == AttemptState::Submitted && rejected_submission {
        let version = attempt.version;
        return attempt_apply(
            attempt,
            AttemptCommand::FinalizeRejectedSubmission {
                expected_version: version,
                submission_id: id(28),
                run_outcome: SubmissionState::Fail,
                failure_dossier_complete: true,
            },
        );
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::StartIsolatedReview {
            expected_version: version,
            reviewer_id: id(29),
        },
    );
    if target == AttemptState::IsolatedReview {
        return attempt;
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::StartCleanReproduce {
            expected_version: version,
            has_unresolved_high_finding: false,
        },
    );
    if target == AttemptState::CleanReproduce {
        return attempt;
    }
    let version = attempt.version;
    attempt = attempt_apply(
        attempt,
        AttemptCommand::FinalizeSubmission {
            expected_version: version,
            submission_id: id(28),
            run_outcome: SubmissionState::Pass,
            manifest_signed: true,
            heads_match: true,
        },
    );
    if target == AttemptState::Submitted {
        return attempt;
    }
    if target == AttemptState::Passed {
        let version = attempt.version;
        return attempt_apply(
            attempt,
            AttemptCommand::MarkAttemptPassed {
                expected_version: version,
            },
        );
    }
    assert_eq!(target, AttemptState::Rejected);
    let mut rejected = reach_attempt(AttemptState::Submitted, AttemptState::Planning, true);
    let version = rejected.version;
    rejected = attempt_apply(
        rejected,
        AttemptCommand::MarkAttemptRejected {
            expected_version: version,
        },
    );
    rejected
}

fn attempt_command(
    kind: AttemptCommandKind,
    attempt: &Attempt,
    _target: AttemptState,
) -> AttemptCommand {
    let expected_version = attempt.version;
    match kind {
        AttemptCommandKind::AttachLease => AttemptCommand::AttachLease {
            expected_version,
            lease_id: id(30),
            fencing_token: attempt.fencing_token,
            lineage_matches: true,
        },
        AttemptCommandKind::StartPreparation => AttemptCommand::StartPreparation {
            expected_version,
            token_is_current: true,
            inputs_available: true,
        },
        AttemptCommandKind::BaselineReady => AttemptCommand::BaselineReady {
            expected_version,
            snapshot_matches: true,
        },
        AttemptCommandKind::ApproveExecutionPlan => AttemptCommand::ApproveExecutionPlan {
            expected_version,
            plan_covers_contract: true,
        },
        AttemptCommandKind::StartLocalVerification => AttemptCommand::StartLocalVerification {
            expected_version,
            has_candidate_changes: true,
        },
        AttemptCommandKind::RequestFix => AttemptCommand::RequestFix {
            expected_version,
            failure_is_fixable: true,
            budget_available: true,
        },
        AttemptCommandKind::RecordCandidate => AttemptCommand::RecordCandidate {
            expected_version,
            candidate_commit: oid(31),
            hard_checks_passed: true,
        },
        AttemptCommandKind::StartIsolatedReview => AttemptCommand::StartIsolatedReview {
            expected_version,
            reviewer_id: id(32),
        },
        AttemptCommandKind::StartCleanReproduce => AttemptCommand::StartCleanReproduce {
            expected_version,
            has_unresolved_high_finding: false,
        },
        AttemptCommandKind::FinalizeSubmission => AttemptCommand::FinalizeSubmission {
            expected_version,
            submission_id: id(33),
            run_outcome: SubmissionState::Pass,
            manifest_signed: true,
            heads_match: true,
        },
        AttemptCommandKind::FinalizeRejectedSubmission => {
            AttemptCommand::FinalizeRejectedSubmission {
                expected_version,
                submission_id: id(33),
                run_outcome: SubmissionState::Fail,
                failure_dossier_complete: true,
            }
        }
        AttemptCommandKind::MarkAttemptPassed => {
            AttemptCommand::MarkAttemptPassed { expected_version }
        }
        AttemptCommandKind::MarkAttemptRejected => {
            AttemptCommand::MarkAttemptRejected { expected_version }
        }
        AttemptCommandKind::WaitFor => AttemptCommand::WaitFor {
            expected_version,
            condition: WakeCondition::NotBefore { at: at(10) },
        },
        AttemptCommandKind::Wake => AttemptCommand::Wake {
            expected_version,
            fact: WakeFact::TimeReached { now: at(10) },
        },
        AttemptCommandKind::ReportProgress => AttemptCommand::ReportProgress {
            expected_version,
            progress: SemanticProgress {
                checkpoint_digest: Some(Sha256Digest::of_bytes(b"checkpoint")),
                ..SemanticProgress::default()
            },
        },
        AttemptCommandKind::MarkLost => AttemptCommand::MarkLost { expected_version },
        AttemptCommandKind::MarkFailed => AttemptCommand::MarkFailed {
            expected_version,
            reason_code: "failure".into(),
        },
        AttemptCommandKind::CancelAttempt => AttemptCommand::CancelAttempt { expected_version },
    }
}

const ATTEMPT_STATES: &[AttemptState] = &[
    AttemptState::Created,
    AttemptState::Leased,
    AttemptState::Preparing,
    AttemptState::Planning,
    AttemptState::Implementing,
    AttemptState::LocalVerify,
    AttemptState::WaitingInput,
    AttemptState::Candidate,
    AttemptState::IsolatedReview,
    AttemptState::CleanReproduce,
    AttemptState::Submitted,
    AttemptState::Passed,
    AttemptState::Rejected,
    AttemptState::Lost,
    AttemptState::Failed,
    AttemptState::Cancelled,
];

const ATTEMPT_COMMANDS: &[AttemptCommandKind] = &[
    AttemptCommandKind::AttachLease,
    AttemptCommandKind::StartPreparation,
    AttemptCommandKind::BaselineReady,
    AttemptCommandKind::ApproveExecutionPlan,
    AttemptCommandKind::StartLocalVerification,
    AttemptCommandKind::RequestFix,
    AttemptCommandKind::RecordCandidate,
    AttemptCommandKind::StartIsolatedReview,
    AttemptCommandKind::StartCleanReproduce,
    AttemptCommandKind::FinalizeSubmission,
    AttemptCommandKind::FinalizeRejectedSubmission,
    AttemptCommandKind::MarkAttemptPassed,
    AttemptCommandKind::MarkAttemptRejected,
    AttemptCommandKind::WaitFor,
    AttemptCommandKind::Wake,
    AttemptCommandKind::ReportProgress,
    AttemptCommandKind::MarkLost,
    AttemptCommandKind::MarkFailed,
    AttemptCommandKind::CancelAttempt,
];

#[test]
fn every_declared_attempt_transition_passes() {
    for rule in ATTEMPT_TRANSITIONS {
        let rejected = rule.command == AttemptCommandKind::MarkAttemptRejected;
        let waiting_resume = if rule.command == AttemptCommandKind::Wake {
            rule.to
        } else {
            AttemptState::Planning
        };
        let attempt = reach_attempt(rule.from, waiting_resume, rejected);
        let command = attempt_command(rule.command, &attempt, rule.to);
        let next = attempt
            .transition(&command)
            .unwrap_or_else(|error| panic!("{rule:?} failed: {error:?}"))
            .aggregate;
        assert_eq!(next.state, rule.to, "{rule:?}");
    }
}

#[test]
fn every_undeclared_attempt_transition_is_stably_invalid() {
    for state in ATTEMPT_STATES {
        let attempt = reach_attempt(*state, AttemptState::Planning, false);
        for kind in ATTEMPT_COMMANDS {
            if ATTEMPT_TRANSITIONS
                .iter()
                .any(|rule| rule.from == *state && rule.command == *kind)
            {
                continue;
            }
            let command = attempt_command(*kind, &attempt, AttemptState::Failed);
            let error = attempt
                .transition(&command)
                .expect_err("undeclared transition");
            assert!(matches!(error, DomainError::InvalidTransition { .. }));
            assert_eq!(error.code(), "AF_TRANSITION_INVALID");
        }
    }
}

#[test]
fn sealed_attempts_only_allow_the_verification_pipeline_or_cancellation() {
    let expected = [
        (
            AttemptState::Candidate,
            BTreeSet::from([
                AttemptCommandKind::StartIsolatedReview,
                AttemptCommandKind::FinalizeRejectedSubmission,
                AttemptCommandKind::CancelAttempt,
            ]),
        ),
        (
            AttemptState::IsolatedReview,
            BTreeSet::from([
                AttemptCommandKind::StartCleanReproduce,
                AttemptCommandKind::FinalizeRejectedSubmission,
                AttemptCommandKind::CancelAttempt,
            ]),
        ),
        (
            AttemptState::CleanReproduce,
            BTreeSet::from([
                AttemptCommandKind::FinalizeSubmission,
                AttemptCommandKind::FinalizeRejectedSubmission,
                AttemptCommandKind::CancelAttempt,
            ]),
        ),
    ];

    for (state, expected_commands) in expected {
        let declared: BTreeSet<_> = ATTEMPT_TRANSITIONS
            .iter()
            .filter(|rule| rule.from == state)
            .map(|rule| rule.command)
            .collect();
        assert_eq!(
            declared, expected_commands,
            "post-seal whitelist for {state:?}"
        );

        let attempt = reach_attempt(state, AttemptState::Planning, false);
        for command in [
            AttemptCommand::MarkLost {
                expected_version: attempt.version,
            },
            AttemptCommand::MarkFailed {
                expected_version: attempt.version,
                reason_code: "author_runtime_failure".into(),
            },
        ] {
            let error = attempt
                .transition(&command)
                .expect_err("sealed Attempt must not use author terminalization");
            assert!(matches!(error, DomainError::InvalidTransition { .. }));
            assert_eq!(error.code(), "AF_TRANSITION_INVALID");
        }
        for event in [AttemptEvent::AttemptLost, AttemptEvent::AttemptFailed] {
            let error = attempt
                .apply_event(&event)
                .expect_err("event replay must enforce the same post-seal whitelist");
            assert!(matches!(error, DomainError::InvalidTransition { .. }));
        }
    }
}

fn grant() -> GrantLease {
    GrantLease {
        id: id(40),
        package_id: id(41),
        revision_id: id(42),
        attempt_id: id(43),
        holder_node_id: id(44),
        previous_fencing_token: None,
        fencing_token: token(1),
        granted_at: at(0),
        expires_at: at(10),
        max_expires_at: at(30),
    }
}

fn active_lease() -> Lease {
    Lease::transition(None, &LeaseCommand::GrantLease(grant()))
        .expect("grant")
        .aggregate
}

fn reach_lease(state: LeaseState) -> Lease {
    let lease = active_lease();
    if state == LeaseState::Active {
        return lease;
    }
    let command = match state {
        LeaseState::Released => LeaseCommand::ReleaseLease {
            expected_version: lease.version,
            holder_node_id: lease.holder_node_id,
            fencing_token: lease.fencing_token,
            now: at(5),
        },
        LeaseState::Revoked => LeaseCommand::RevokeLease {
            expected_version: lease.version,
            policy_authorized: true,
            reason_code: "policy".into(),
        },
        LeaseState::Expired => LeaseCommand::ExpireLease {
            expected_version: lease.version,
            now: at(10),
        },
        LeaseState::Active => unreachable!(),
    };
    Lease::transition(Some(&lease), &command)
        .expect("terminalize")
        .aggregate
}

fn lease_command(kind: LeaseCommandKind, lease: &Lease) -> LeaseCommand {
    match kind {
        LeaseCommandKind::GrantLease => LeaseCommand::GrantLease(grant()),
        LeaseCommandKind::RenewLease => LeaseCommand::RenewLease {
            expected_version: lease.version,
            holder_node_id: lease.holder_node_id,
            fencing_token: lease.fencing_token,
            now: at(5),
            new_expires_at: at(20),
        },
        LeaseCommandKind::ReleaseLease => LeaseCommand::ReleaseLease {
            expected_version: lease.version,
            holder_node_id: lease.holder_node_id,
            fencing_token: lease.fencing_token,
            now: at(5),
        },
        LeaseCommandKind::RevokeLease => LeaseCommand::RevokeLease {
            expected_version: lease.version,
            policy_authorized: true,
            reason_code: "policy".into(),
        },
        LeaseCommandKind::ExpireLease => LeaseCommand::ExpireLease {
            expected_version: lease.version,
            now: at(10),
        },
    }
}

#[test]
fn every_declared_lease_transition_passes_and_terminal_states_never_recover() {
    let created = Lease::transition(None, &LeaseCommand::GrantLease(grant())).expect("creation");
    assert_eq!(created.aggregate.state, LeaseState::Active);
    for rule in LEASE_TRANSITIONS {
        let lease = active_lease();
        let next = Lease::transition(Some(&lease), &lease_command(rule.command, &lease))
            .unwrap_or_else(|error| panic!("{rule:?}: {error:?}"))
            .aggregate;
        assert_eq!(next.state, rule.to);
    }
    for state in [
        LeaseState::Released,
        LeaseState::Revoked,
        LeaseState::Expired,
    ] {
        let lease = reach_lease(state);
        for kind in [
            LeaseCommandKind::GrantLease,
            LeaseCommandKind::RenewLease,
            LeaseCommandKind::ReleaseLease,
            LeaseCommandKind::RevokeLease,
            LeaseCommandKind::ExpireLease,
        ] {
            let error = Lease::transition(Some(&lease), &lease_command(kind, &lease))
                .expect_err("terminal lease must stay terminal");
            assert!(matches!(error, DomainError::InvalidTransition { .. }));
            assert_eq!(error.code(), "AF_TRANSITION_INVALID");
        }
    }
}

#[test]
fn terminal_lease_precedes_version_cas_for_new_key_retries() {
    for state in [
        LeaseState::Released,
        LeaseState::Revoked,
        LeaseState::Expired,
    ] {
        let lease = reach_lease(state);
        let original_expected_version = AggregateVersion::new(lease.version.get() - 1);
        let commands = [
            LeaseCommand::RenewLease {
                expected_version: original_expected_version,
                holder_node_id: lease.holder_node_id,
                fencing_token: lease.fencing_token,
                now: at(5),
                new_expires_at: at(20),
            },
            LeaseCommand::ReleaseLease {
                expected_version: original_expected_version,
                holder_node_id: lease.holder_node_id,
                fencing_token: lease.fencing_token,
                now: at(5),
            },
            LeaseCommand::RevokeLease {
                expected_version: original_expected_version,
                policy_authorized: true,
                reason_code: "new_key_retry".into(),
            },
            LeaseCommand::ExpireLease {
                expected_version: original_expected_version,
                now: at(10),
            },
        ];
        for command in commands {
            let error = Lease::transition(Some(&lease), &command)
                .expect_err("terminality must win over a stale expected version");
            assert!(matches!(error, DomainError::InvalidTransition { .. }));
            assert_eq!(error.code(), "AF_TRANSITION_INVALID");
        }
    }
}

fn submission_record(state: SubmissionState) -> SubmissionRecord {
    let candidate = state != SubmissionState::Quarantined;
    let pass = state == SubmissionState::Pass;
    let candidate_head = oid(56);
    SubmissionRecord {
        id: id(50),
        protocol_key: ProtocolKey::new("sub-50").expect("key"),
        attempt_id: id(51),
        package_revision_id: id(52),
        candidate_id: candidate.then(|| id(53)),
        candidate_artifact_id: candidate.then(|| id(54)),
        verification_run_id: candidate.then(|| id(55)),
        candidate_commit: candidate.then(|| candidate_head.clone()),
        submitted_head: candidate.then(|| candidate_head.clone()),
        tested_head: pass.then(|| candidate_head.clone()),
        reviewed_head: candidate.then(|| candidate_head.clone()),
        manifest_digest: Sha256Digest::of_bytes(b"manifest"),
        evidence_digest: pass.then(|| Sha256Digest::of_bytes(b"evidence")),
        lease_fencing_token_hash: Sha256Digest::of_bytes(b"fence"),
        state,
        completed_stage: if state == SubmissionState::Quarantined {
            CompletedStage::SalvageRegistration
        } else if pass {
            CompletedStage::CandidateReady
        } else {
            CompletedStage::Reviewing
        },
        failure_dossier: matches!(state, SubmissionState::Fail | SubmissionState::Inconclusive)
            .then(|| FailureDossier {
                evidence_digest: Sha256Digest::of_bytes(b"failure"),
                finding_codes: vec![ProtocolKey::new("finding-1").expect("key")],
            }),
        acceptance_facts: candidate.then(|| AcceptanceFacts {
            hard_criteria: if pass {
                vec![CriterionOutcome::Pass]
            } else {
                vec![]
            },
            unresolved_high_risk_findings: Some(!pass),
            clean_reproduce_passed: pass.then_some(true),
            signature_valid: true,
            lineage_matches: true,
            lease_was_current_at_registration: true,
        }),
    }
}

fn submission_command(kind: SubmissionCommandKind, state: SubmissionState) -> SubmissionCommand {
    match kind {
        SubmissionCommandKind::FinalizeCandidateSubmission => {
            SubmissionCommand::FinalizeCandidateSubmission {
                record: submission_record(state),
            }
        }
        SubmissionCommandKind::RegisterSalvage => SubmissionCommand::RegisterSalvage {
            record: submission_record(SubmissionState::Quarantined),
        },
    }
}

#[test]
fn every_submission_creation_row_passes_and_terminal_states_are_immutable() {
    for rule in SUBMISSION_TRANSITIONS {
        let command = submission_command(rule.command, rule.to);
        let created = Submission::transition(None, &command)
            .unwrap_or_else(|error| panic!("{rule:?}: {error:?}"))
            .aggregate;
        assert_eq!(created.state, rule.to);
        assert!(created.state.is_terminal());
        for second_kind in [
            SubmissionCommandKind::FinalizeCandidateSubmission,
            SubmissionCommandKind::RegisterSalvage,
        ] {
            let second = submission_command(second_kind, SubmissionState::Pass);
            let error = Submission::transition(Some(&created), &second)
                .expect_err("submission must never be overwritten");
            assert!(matches!(error, DomainError::InvalidTransition { .. }));
            assert_eq!(error.code(), "AF_TRANSITION_INVALID");
        }
    }
}

#[test]
fn skipped_or_inconclusive_hard_criteria_can_never_be_candidate_ready() {
    for outcome in [CriterionOutcome::Skipped, CriterionOutcome::Inconclusive] {
        let mut record = submission_record(SubmissionState::Pass);
        record
            .acceptance_facts
            .as_mut()
            .expect("facts")
            .hard_criteria = vec![outcome];
        let error = Submission::transition(
            None,
            &SubmissionCommand::FinalizeCandidateSubmission { record },
        )
        .expect_err("non-pass hard criterion");
        assert!(matches!(error, DomainError::SubmissionNotAcceptable { .. }));
    }
}

#[test]
fn submission_stage_shape_rejects_future_facts_and_head_substitution() {
    let mut provenance = submission_record(SubmissionState::Fail);
    provenance.completed_stage = CompletedStage::ProvenanceCheck;
    provenance.reviewed_head = None;
    provenance
        .acceptance_facts
        .as_mut()
        .expect("facts")
        .unresolved_high_risk_findings = None;
    let valid_provenance = Submission::transition(
        None,
        &SubmissionCommand::FinalizeCandidateSubmission {
            record: provenance.clone(),
        },
    )
    .expect("provenance-only failure is valid");
    assert_eq!(valid_provenance.aggregate.state, SubmissionState::Fail);

    provenance.reviewed_head = provenance.candidate_commit.clone();
    provenance
        .acceptance_facts
        .as_mut()
        .expect("facts")
        .hard_criteria = vec![CriterionOutcome::Fail];
    provenance
        .acceptance_facts
        .as_mut()
        .expect("facts")
        .clean_reproduce_passed = Some(false);
    let error = Submission::transition(
        None,
        &SubmissionCommand::FinalizeCandidateSubmission { record: provenance },
    )
    .expect_err("provenance failure must not contain future-stage facts");
    assert!(matches!(error, DomainError::SubmissionNotAcceptable { .. }));

    let mut reviewing = submission_record(SubmissionState::Inconclusive);
    reviewing.tested_head = reviewing.candidate_commit.clone();
    let error = Submission::transition(
        None,
        &SubmissionCommand::FinalizeCandidateSubmission { record: reviewing },
    )
    .expect_err("reviewing failure must not contain reproduction head");
    assert!(matches!(error, DomainError::SubmissionNotAcceptable { .. }));

    let mut substituted = submission_record(SubmissionState::Pass);
    substituted.candidate_commit = Some(oid(99));
    let error = Submission::transition(
        None,
        &SubmissionCommand::FinalizeCandidateSubmission {
            record: substituted,
        },
    )
    .expect_err("three equal heads cannot substitute another candidate commit");
    assert_eq!(error, DomainError::HeadMismatch);
}

fn assert_invalid_submission_event_on_all_entry_points(
    record: SubmissionRecord,
    failed_check: &str,
) {
    let expected = DomainError::SubmissionNotAcceptable {
        failed_checks: vec![failed_check.to_owned()],
    };
    let command = SubmissionCommand::FinalizeCandidateSubmission {
        record: record.clone(),
    };
    let event = SubmissionEvent::CandidateSubmissionFinalized { record };

    let transition_error = Submission::transition(None, &command)
        .expect_err("invalid Candidate Submission must fail command validation");
    assert_eq!(transition_error, expected);
    assert_eq!(transition_error.code(), "AF_SUBMISSION_NOT_ACCEPTABLE");

    let apply_error = Submission::apply_event(None, &event)
        .expect_err("invalid Candidate Submission must fail direct event apply");
    assert_eq!(apply_error, expected);
    assert_eq!(apply_error.code(), "AF_SUBMISSION_NOT_ACCEPTABLE");

    let replay_error = Submission::replay(&[event])
        .expect_err("invalid Candidate Submission must fail event replay");
    assert_eq!(replay_error, expected);
    assert_eq!(replay_error.code(), "AF_SUBMISSION_NOT_ACCEPTABLE");
}

#[test]
fn provenance_failure_rejects_evidence_bundle_on_transition_apply_and_replay() {
    let mut record = submission_record(SubmissionState::Fail);
    record.completed_stage = CompletedStage::ProvenanceCheck;
    record.reviewed_head = None;
    record.evidence_digest = Some(Sha256Digest::of_bytes(b"premature-evidence"));
    let facts = record.acceptance_facts.as_mut().expect("facts");
    facts.unresolved_high_risk_findings = None;
    facts.clean_reproduce_passed = None;

    assert_invalid_submission_event_on_all_entry_points(record, "provenance_stage_shape");
}

#[test]
fn reviewing_failure_rejects_evidence_bundle_on_transition_apply_and_replay() {
    let mut record = submission_record(SubmissionState::Inconclusive);
    record.completed_stage = CompletedStage::Reviewing;
    record.evidence_digest = Some(Sha256Digest::of_bytes(b"premature-evidence"));

    assert_invalid_submission_event_on_all_entry_points(record, "reviewing_stage_shape");
}
