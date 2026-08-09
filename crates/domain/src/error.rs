//! Stable domain failures and their safe public representation.

use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;

/// Errors returned by pure domain decisions.
///
/// Variants may retain a private diagnostic reason for server logs, while
/// [`DomainError::details`] exposes only allow-listed, sanitized fields.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DomainError {
    #[error("invalid argument `{field}`: {reason}")]
    InvalidArgument { field: String, reason: String },
    #[error("{resource} was not found")]
    NotFound { resource: &'static str },
    #[error("aggregate version is stale")]
    StaleVersion,
    #[error("invalid transition from {from} using {command}")]
    InvalidTransition { from: String, command: String },
    #[error("package is not ready")]
    PackageNotReady { failed_checks: Vec<String> },
    #[error("package is not claimable")]
    PackageNotClaimable,
    #[error("attempt limit reached")]
    AttemptLimitReached,
    #[error("package hash mismatch")]
    PackageHashMismatch,
    #[error("lease is stale")]
    StaleLease,
    #[error("lease has expired")]
    LeaseExpired,
    #[error("wake condition is not satisfied")]
    WakeConditionUnsatisfied,
    #[error("submission is not acceptable")]
    SubmissionNotAcceptable { failed_checks: Vec<String> },
    #[error("domain invariant violated: {invariant}")]
    InvariantViolation { invariant: &'static str },
    #[error("idempotency key was reused with a different payload")]
    IdempotencyKeyReused,
    #[error("signal binding is stale")]
    SignalStale,
    #[error("signals cannot be coalesced")]
    SignalNotCoalescible,
    #[error("invocation intent is not dispatchable")]
    IntentNotDispatchable,
    #[error("invocation run claim is stale")]
    InvocationClaimStale,
    #[error("invocation outcome is unknown")]
    InvocationOutcomeUnknown,
    #[error("invocation run is terminal")]
    InvocationRunTerminal,
    #[error("invocation binding does not match")]
    InvocationBindingMismatch,
    #[error("session capsule binding does not match")]
    CapsuleBindingMismatch,
    #[error("budget reservation failed")]
    BudgetReservationFailed,
    #[error("budget reservation is stale")]
    BudgetReservationStale,
    #[error("adapter capability is missing")]
    AdapterCapabilityMissing,
    #[error("governance case is stale")]
    GovernanceCaseStale,
    #[error("governance action binding is stale")]
    GovernanceActionStale,
    #[error("governance decision has expired")]
    GovernanceDecisionExpired,
    #[error("governance quorum is unmet")]
    GovernanceQuorumUnmet,
    #[error("governance self approval is forbidden")]
    GovernanceSelfApprovalForbidden,
    #[error("governance execution claim is stale")]
    GovernanceExecutionClaimStale,
    #[error("governance execution outcome is unknown")]
    GovernanceOutcomeUnknown,
    #[error("policy revision is stale")]
    PolicyRevisionStale,
    #[error("projection is rebuilding")]
    ProjectionRebuilding,
    #[error("cursor is invalid")]
    CursorInvalid,
    #[error("cursor has expired")]
    CursorExpired,
    #[error("operator note is not executable")]
    NoteNotExecutable,

    // The remaining variants are the complete AFWP/1.0 public error taxonomy.
    #[error("schema is invalid")]
    SchemaInvalid,
    #[error("schema version is unsupported")]
    SchemaVersionUnsupported,
    #[error("package revision conflicts with an existing revision")]
    RevisionConflict,
    #[error("no feasible executor is available")]
    NoFeasibleExecutor,
    #[error("bid has expired")]
    BidExpired,
    #[error("a dependency is unavailable")]
    DependencyUnavailable,
    #[error("scope policy was violated")]
    ScopeViolation,
    #[error("evidence is invalid")]
    EvidenceInvalid,
    #[error("Git heads do not match")]
    HeadMismatch,
    #[error("candidate artifact is not complete")]
    CandidateArtifactNotComplete,
    #[error("verification stage is invalid")]
    VerificationStageInvalid,
    #[error("signature is invalid")]
    SignatureInvalid,
    #[error("required Git base is missing")]
    BaseMissing,
    #[error("Git bundle is invalid")]
    BundleInvalid,
    #[error("destination has a conflict")]
    DestinationConflict,
    #[error("integration target moved")]
    TargetMoved,
    #[error("operation denied by policy")]
    PolicyDenied,
    #[error("task contract is invalid")]
    TaskInvalid,
    #[error("budget is exhausted")]
    BudgetExhausted,
    #[error("operation is forbidden")]
    Forbidden,
    #[error("request was rate limited")]
    RateLimited,
    #[error("internal failure")]
    Internal,
}

impl DomainError {
    /// Stable AFWP wire code. Clients must branch on this value, never messages.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidArgument { .. } => "AF_ARGUMENT_INVALID",
            Self::NotFound { .. } => "AF_NOT_FOUND",
            Self::StaleVersion => "AF_VERSION_STALE",
            Self::InvalidTransition { .. } => "AF_TRANSITION_INVALID",
            Self::PackageNotReady { .. } => "AF_PACKAGE_NOT_READY",
            Self::PackageNotClaimable => "AF_PACKAGE_NOT_CLAIMABLE",
            Self::AttemptLimitReached => "AF_ATTEMPT_LIMIT_REACHED",
            Self::PackageHashMismatch => "AF_PACKAGE_HASH_MISMATCH",
            Self::StaleLease => "AF_LEASE_STALE",
            Self::LeaseExpired => "AF_LEASE_EXPIRED",
            Self::WakeConditionUnsatisfied => "AF_WAKE_CONDITION_UNSATISFIED",
            Self::SubmissionNotAcceptable { .. } => "AF_SUBMISSION_NOT_ACCEPTABLE",
            Self::InvariantViolation { .. } => "AF_INVARIANT_VIOLATION",
            Self::IdempotencyKeyReused => "AF_IDEMPOTENCY_KEY_REUSED",
            Self::SignalStale => "AF_SIGNAL_STALE",
            Self::SignalNotCoalescible => "AF_SIGNAL_NOT_COALESCIBLE",
            Self::IntentNotDispatchable => "AF_INTENT_NOT_DISPATCHABLE",
            Self::InvocationClaimStale => "AF_INVOCATION_CLAIM_STALE",
            Self::InvocationOutcomeUnknown => "AF_INVOCATION_OUTCOME_UNKNOWN",
            Self::InvocationRunTerminal => "AF_INVOCATION_RUN_TERMINAL",
            Self::InvocationBindingMismatch => "AF_INVOCATION_BINDING_MISMATCH",
            Self::CapsuleBindingMismatch => "AF_CAPSULE_BINDING_MISMATCH",
            Self::BudgetReservationFailed => "AF_BUDGET_RESERVATION_FAILED",
            Self::BudgetReservationStale => "AF_BUDGET_RESERVATION_STALE",
            Self::AdapterCapabilityMissing => "AF_ADAPTER_CAPABILITY_MISSING",
            Self::GovernanceCaseStale => "AF_GOVERNANCE_CASE_STALE",
            Self::GovernanceActionStale => "AF_GOVERNANCE_ACTION_STALE",
            Self::GovernanceDecisionExpired => "AF_GOVERNANCE_DECISION_EXPIRED",
            Self::GovernanceQuorumUnmet => "AF_GOVERNANCE_QUORUM_UNMET",
            Self::GovernanceSelfApprovalForbidden => "AF_GOVERNANCE_SELF_APPROVAL_FORBIDDEN",
            Self::GovernanceExecutionClaimStale => "AF_GOVERNANCE_EXECUTION_CLAIM_STALE",
            Self::GovernanceOutcomeUnknown => "AF_GOVERNANCE_OUTCOME_UNKNOWN",
            Self::PolicyRevisionStale => "AF_POLICY_REVISION_STALE",
            Self::ProjectionRebuilding => "AF_PROJECTION_REBUILDING",
            Self::CursorInvalid => "AF_CURSOR_INVALID",
            Self::CursorExpired => "AF_CURSOR_EXPIRED",
            Self::NoteNotExecutable => "AF_NOTE_NOT_EXECUTABLE",
            Self::SchemaInvalid => "AF_SCHEMA_INVALID",
            Self::SchemaVersionUnsupported => "AF_SCHEMA_VERSION_UNSUPPORTED",
            Self::RevisionConflict => "AF_REVISION_CONFLICT",
            Self::NoFeasibleExecutor => "AF_NO_FEASIBLE_EXECUTOR",
            Self::BidExpired => "AF_BID_EXPIRED",
            Self::DependencyUnavailable => "AF_DEPENDENCY_UNAVAILABLE",
            Self::ScopeViolation => "AF_SCOPE_VIOLATION",
            Self::EvidenceInvalid => "AF_EVIDENCE_INVALID",
            Self::HeadMismatch => "AF_HEAD_MISMATCH",
            Self::CandidateArtifactNotComplete => "AF_CANDIDATE_ARTIFACT_NOT_COMPLETE",
            Self::VerificationStageInvalid => "AF_VERIFICATION_STAGE_INVALID",
            Self::SignatureInvalid => "AF_SIGNATURE_INVALID",
            Self::BaseMissing => "AF_BASE_MISSING",
            Self::BundleInvalid => "AF_BUNDLE_INVALID",
            Self::DestinationConflict => "AF_DESTINATION_CONFLICT",
            Self::TargetMoved => "AF_TARGET_MOVED",
            Self::PolicyDenied => "AF_POLICY_DENIED",
            Self::TaskInvalid => "AF_TASK_INVALID",
            Self::BudgetExhausted => "AF_BUDGET_EXHAUSTED",
            Self::Forbidden => "AF_FORBIDDEN",
            Self::RateLimited => "AF_RATE_LIMITED",
            Self::Internal => "AF_INTERNAL",
        }
    }

    /// Whether replaying the same request later is safe and potentially useful.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        matches!(
            self,
            Self::PackageNotClaimable
                | Self::BidExpired
                | Self::DependencyUnavailable
                | Self::CandidateArtifactNotComplete
                | Self::TargetMoved
                | Self::ProjectionRebuilding
                | Self::RateLimited
                | Self::Internal
        )
    }

    /// Safe structured details for an external response.
    ///
    /// Arbitrary diagnostic reasons are intentionally excluded. State names,
    /// command names, fields, resources, and check codes are scrubbed and
    /// length-limited before being exposed.
    #[must_use]
    pub fn details(&self) -> Value {
        match self {
            Self::InvalidArgument { field, .. } => json!({ "field": safe_label(field) }),
            Self::NotFound { resource } => json!({ "resource": safe_label(resource) }),
            Self::InvalidTransition { from, command } => json!({
                "from": safe_label(from),
                "command": safe_label(command),
            }),
            Self::PackageNotReady { failed_checks }
            | Self::SubmissionNotAcceptable { failed_checks } => json!({
                "failed_checks": safe_labels(failed_checks),
            }),
            Self::InvariantViolation { invariant } => {
                json!({ "invariant": safe_label(invariant) })
            }
            _ => json!({}),
        }
    }

    /// Converts to the stable, serialization-ready public error body.
    #[must_use]
    pub fn public(&self) -> PublicError {
        PublicError {
            code: self.code(),
            message: self.public_message(),
            retryable: self.retryable(),
            details: self.details(),
        }
    }

    #[must_use]
    pub const fn public_message(&self) -> &'static str {
        match self {
            Self::InvalidArgument { .. } => "an argument is invalid",
            Self::NotFound { .. } => "the requested resource was not found",
            Self::StaleVersion => "aggregate version is stale",
            Self::InvalidTransition { .. } => "the state transition is invalid",
            Self::PackageNotReady { .. } => "the package is not ready",
            Self::PackageNotClaimable => "the package is not claimable",
            Self::AttemptLimitReached => "the attempt limit has been reached",
            Self::PackageHashMismatch => "the package hash does not match",
            Self::StaleLease => "the lease generation is not current",
            Self::LeaseExpired => "the lease has expired",
            Self::WakeConditionUnsatisfied => "the wake condition is not satisfied",
            Self::SubmissionNotAcceptable { .. } => "the submission is not acceptable",
            Self::InvariantViolation { .. } => "a domain invariant was violated",
            Self::IdempotencyKeyReused => "the idempotency key was reused with a different payload",
            Self::SignalStale => "the signal binding is stale",
            Self::SignalNotCoalescible => "the signals cannot be coalesced",
            Self::IntentNotDispatchable => "the invocation intent is not dispatchable",
            Self::InvocationClaimStale => "the invocation run claim is stale",
            Self::InvocationOutcomeUnknown => "the invocation outcome must be reconciled",
            Self::InvocationRunTerminal => "the invocation run is terminal",
            Self::InvocationBindingMismatch => "the invocation binding does not match",
            Self::CapsuleBindingMismatch => "the session capsule binding does not match",
            Self::BudgetReservationFailed => "the budget reservation could not be created",
            Self::BudgetReservationStale => "the budget reservation is stale",
            Self::AdapterCapabilityMissing => "the adapter lacks a required capability",
            Self::GovernanceCaseStale => "the governance case is stale",
            Self::GovernanceActionStale => "the governance action binding is stale",
            Self::GovernanceDecisionExpired => "the governance decision has expired",
            Self::GovernanceQuorumUnmet => "the governance quorum is unmet",
            Self::GovernanceSelfApprovalForbidden => "self approval is forbidden",
            Self::GovernanceExecutionClaimStale => "the governance execution claim is stale",
            Self::GovernanceOutcomeUnknown => "the governance outcome must be reconciled",
            Self::PolicyRevisionStale => "the policy revision is stale",
            Self::ProjectionRebuilding => "the projection is rebuilding",
            Self::CursorInvalid => "the cursor is invalid",
            Self::CursorExpired => "the cursor has expired",
            Self::NoteNotExecutable => "an operator note is not executable",
            Self::SchemaInvalid => "the schema is invalid",
            Self::SchemaVersionUnsupported => "the schema version is unsupported",
            Self::RevisionConflict => "the package revision conflicts",
            Self::NoFeasibleExecutor => "no feasible executor is available",
            Self::BidExpired => "the bid has expired",
            Self::DependencyUnavailable => "a dependency is unavailable",
            Self::ScopeViolation => "the requested operation violates scope",
            Self::EvidenceInvalid => "the evidence is invalid",
            Self::HeadMismatch => "the Git heads do not match",
            Self::CandidateArtifactNotComplete => "the candidate artifact is not complete",
            Self::VerificationStageInvalid => "the verification stage is invalid",
            Self::SignatureInvalid => "the signature is invalid",
            Self::BaseMissing => "the required Git base is missing",
            Self::BundleInvalid => "the Git bundle is invalid",
            Self::DestinationConflict => "the destination has a conflict",
            Self::TargetMoved => "the integration target moved",
            Self::PolicyDenied => "the operation was denied by policy",
            Self::TaskInvalid => "the task contract is invalid",
            Self::BudgetExhausted => "the budget is exhausted",
            Self::Forbidden => "the operation is forbidden",
            Self::RateLimited => "the request was rate limited",
            Self::Internal => "an internal failure occurred",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PublicError {
    pub code: &'static str,
    pub message: &'static str,
    pub retryable: bool,
    pub details: Value,
}

fn safe_labels(values: &[String]) -> Vec<String> {
    values.iter().map(|value| safe_label(value)).collect()
}

fn safe_label(value: &str) -> String {
    const MAX_PUBLIC_LABEL_LEN: usize = 96;
    const SENSITIVE_MARKERS: &[&str] = &[
        "secret",
        "password",
        "passwd",
        "credential",
        "authorization",
        "bearer",
        "private_key",
        "prompt",
        "source_code",
        "sql",
    ];

    let normalized = value.to_ascii_lowercase();
    if SENSITIVE_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
        || value.contains('/')
        || value.contains('\\')
    {
        return "redacted".into();
    }

    value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.' | ':')
        })
        .take(MAX_PUBLIC_LABEL_LEN)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn all_errors() -> Vec<DomainError> {
        vec![
            DomainError::InvalidArgument {
                field: "field".into(),
                reason: "private diagnostic".into(),
            },
            DomainError::NotFound {
                resource: "package",
            },
            DomainError::StaleVersion,
            DomainError::InvalidTransition {
                from: "draft".into(),
                command: "grant_lease".into(),
            },
            DomainError::PackageNotReady {
                failed_checks: vec!["dor".into()],
            },
            DomainError::PackageNotClaimable,
            DomainError::AttemptLimitReached,
            DomainError::PackageHashMismatch,
            DomainError::StaleLease,
            DomainError::LeaseExpired,
            DomainError::WakeConditionUnsatisfied,
            DomainError::SubmissionNotAcceptable {
                failed_checks: vec!["head_match".into()],
            },
            DomainError::InvariantViolation { invariant: "test" },
            DomainError::IdempotencyKeyReused,
            DomainError::SignalStale,
            DomainError::SignalNotCoalescible,
            DomainError::IntentNotDispatchable,
            DomainError::InvocationClaimStale,
            DomainError::InvocationOutcomeUnknown,
            DomainError::InvocationRunTerminal,
            DomainError::InvocationBindingMismatch,
            DomainError::CapsuleBindingMismatch,
            DomainError::BudgetReservationFailed,
            DomainError::BudgetReservationStale,
            DomainError::AdapterCapabilityMissing,
            DomainError::GovernanceCaseStale,
            DomainError::GovernanceActionStale,
            DomainError::GovernanceDecisionExpired,
            DomainError::GovernanceQuorumUnmet,
            DomainError::GovernanceSelfApprovalForbidden,
            DomainError::GovernanceExecutionClaimStale,
            DomainError::GovernanceOutcomeUnknown,
            DomainError::PolicyRevisionStale,
            DomainError::ProjectionRebuilding,
            DomainError::CursorInvalid,
            DomainError::CursorExpired,
            DomainError::NoteNotExecutable,
            DomainError::SchemaInvalid,
            DomainError::SchemaVersionUnsupported,
            DomainError::RevisionConflict,
            DomainError::NoFeasibleExecutor,
            DomainError::BidExpired,
            DomainError::DependencyUnavailable,
            DomainError::ScopeViolation,
            DomainError::EvidenceInvalid,
            DomainError::HeadMismatch,
            DomainError::CandidateArtifactNotComplete,
            DomainError::VerificationStageInvalid,
            DomainError::SignatureInvalid,
            DomainError::BaseMissing,
            DomainError::BundleInvalid,
            DomainError::DestinationConflict,
            DomainError::TargetMoved,
            DomainError::PolicyDenied,
            DomainError::TaskInvalid,
            DomainError::BudgetExhausted,
            DomainError::Forbidden,
            DomainError::RateLimited,
            DomainError::Internal,
        ]
    }

    #[test]
    fn every_error_has_a_unique_stable_code_and_public_shape() {
        let errors = all_errors();
        let codes: BTreeSet<_> = errors.iter().map(DomainError::code).collect();
        assert_eq!(codes.len(), errors.len());
        for error in errors {
            assert!(error.code().starts_with("AF_"));
            assert!(!error.public_message().is_empty());
            assert!(error.details().is_object());
        }
    }

    #[test]
    fn arbitrary_diagnostics_never_reach_public_details() {
        let error = DomainError::InvalidArgument {
            field: "password/path/to/secret".into(),
            reason: "postgres://admin:credential@internal".into(),
        };
        let encoded = serde_json::to_string(&error.public()).expect("serialize public error");
        assert!(!encoded.contains("credential"));
        assert!(!encoded.contains("postgres"));
        assert!(!encoded.contains("path/to"));
    }
}
