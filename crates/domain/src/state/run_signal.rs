//! Immutable invocation input facts and deterministic coalescing decisions.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        ActorId, AggregateVersion, AttemptId, BossSessionId, CorrelationId, EventId, FencingToken,
        GitObjectId, ObligationId, PackageId, PackageRevisionId, PolicyRevisionId, ProjectId,
        RunSignalId, ServerInstant, SessionCapsuleId, Sha256Digest, VerificationRunId,
    },
    state::Transition,
};

/// A typed invocation target. Free-form subject labels are deliberately absent.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum InvocationSubject {
    Attempt(AttemptId),
    BossSession(BossSessionId),
    VerificationJob(VerificationRunId),
    Obligation(ObligationId),
}

/// Immutable resource versions and digests observed when a signal was recorded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvocationBinding {
    pub package_id: Option<PackageId>,
    pub package_revision_id: Option<PackageRevisionId>,
    pub package_hash: Option<Sha256Digest>,
    pub attempt_id: Option<AttemptId>,
    pub author_fencing_token: Option<FencingToken>,
    pub workspace_head: Option<GitObjectId>,
    pub policy_revision_id: PolicyRevisionId,
    pub executor_fingerprint: Option<Sha256Digest>,
    pub input_capsule_id: Option<SessionCapsuleId>,
    pub input_capsule_digest: Option<Sha256Digest>,
}

impl InvocationBinding {
    pub fn validate_for(&self, subject: InvocationSubject) -> Result<(), DomainError> {
        if self.input_capsule_id.is_some() != self.input_capsule_digest.is_some() {
            return Err(invalid_binding(
                "capsule id and digest must be supplied together",
            ));
        }
        if self.attempt_id.is_some() != self.author_fencing_token.is_some() {
            return Err(invalid_binding(
                "attempt id and author fencing token must be supplied together",
            ));
        }
        if matches!(subject, InvocationSubject::Attempt(_))
            && (self.package_id.is_none()
                || self.package_revision_id.is_none()
                || self.package_hash.is_none()
                || self.workspace_head.is_none())
        {
            return Err(invalid_binding(
                "attempt subjects require package, revision, hash and workspace head",
            ));
        }
        match subject {
            InvocationSubject::Attempt(id) if self.attempt_id == Some(id) => Ok(()),
            InvocationSubject::Attempt(_) => Err(invalid_binding(
                "attempt subject and binding attempt id must match",
            )),
            _ if self.attempt_id.is_none() => Ok(()),
            _ => Err(invalid_binding(
                "non-attempt subjects cannot carry author attempt authority",
            )),
        }
    }
}

fn invalid_binding(_reason: &str) -> DomainError {
    DomainError::InvocationBindingMismatch
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunSignalKind {
    AssignmentGranted,
    WakeConditionSatisfied,
    QuestionAnswered,
    PermissionDecided,
    ArtifactAvailable,
    SemanticDeadlineReached,
    BudgetThresholdReached,
    NudgeRequested,
    DiagnoseRequested,
    PolicyChanged,
    PackageRevisionChanged,
    LeaseGenerationChanged,
    CancelRequested,
    SecurityTermination,
    RoutineDue,
    ReconcileRequested,
}

impl RunSignalKind {
    #[must_use]
    pub const fn forbids_coalescing_with_old_intent(self) -> bool {
        matches!(
            self,
            Self::PermissionDecided
                | Self::PolicyChanged
                | Self::PackageRevisionChanged
                | Self::LeaseGenerationChanged
                | Self::CancelRequested
                | Self::SecurityTermination
        )
    }

    #[must_use]
    pub const fn interrupts_active_run(self) -> bool {
        matches!(
            self,
            Self::PolicyChanged
                | Self::PackageRevisionChanged
                | Self::LeaseGenerationChanged
                | Self::CancelRequested
                | Self::SecurityTermination
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordRunSignal {
    pub id: RunSignalId,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub kind: RunSignalKind,
    pub cause_event_id: Option<EventId>,
    pub causation_id: Option<CorrelationId>,
    pub source_actor_id: ActorId,
    pub binding: InvocationBinding,
    pub normalized_reason: String,
    pub payload_digest: Sha256Digest,
    pub not_before: ServerInstant,
    pub deadline: Option<ServerInstant>,
    pub priority: u8,
    pub recorded_at: ServerInstant,
}

impl RecordRunSignal {
    fn validate(&self) -> Result<(), DomainError> {
        self.binding.validate_for(self.subject)?;
        validate_reason(&self.normalized_reason)?;
        if self
            .deadline
            .is_some_and(|deadline| deadline < self.not_before)
        {
            return Err(DomainError::InvalidArgument {
                field: "deadline".into(),
                reason: "must not precede not_before".into(),
            });
        }
        Ok(())
    }
}

fn validate_reason(reason: &str) -> Result<(), DomainError> {
    let valid = !reason.is_empty()
        && reason.len() <= 96
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(DomainError::InvalidArgument {
            field: "normalized_reason".into(),
            reason: "must be a stable 1-96 character protocol label".into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunSignalCommand {
    Record(RecordRunSignal),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RunSignalCommandKind {
    Record,
}

impl RunSignalCommand {
    #[must_use]
    pub const fn kind(&self) -> RunSignalCommandKind {
        RunSignalCommandKind::Record
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunSignalEvent {
    Recorded(RecordRunSignal),
}

/// An append-only input fact. It has no mutable lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunSignal {
    pub id: RunSignalId,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub kind: RunSignalKind,
    pub cause_event_id: Option<EventId>,
    pub causation_id: Option<CorrelationId>,
    pub source_actor_id: ActorId,
    pub binding: InvocationBinding,
    pub normalized_reason: String,
    pub payload_digest: Sha256Digest,
    pub not_before: ServerInstant,
    pub deadline: Option<ServerInstant>,
    pub priority: u8,
    pub recorded_at: ServerInstant,
    pub version: AggregateVersion,
}

impl RunSignal {
    pub fn transition(
        current: Option<&Self>,
        command: &RunSignalCommand,
    ) -> Result<Transition<Self, RunSignalEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &RunSignalCommand,
    ) -> Result<RunSignalEvent, DomainError> {
        match (current, command) {
            (None, RunSignalCommand::Record(record)) => {
                record.validate()?;
                Ok(RunSignalEvent::Recorded(record.clone()))
            }
            (Some(_), _) => Err(DomainError::InvalidTransition {
                from: "recorded".into(),
                command: "record".into(),
            }),
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &RunSignalEvent,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, RunSignalEvent::Recorded(record)) => {
                record.validate()?;
                Ok(Self {
                    id: record.id,
                    project_id: record.project_id,
                    subject: record.subject,
                    kind: record.kind,
                    cause_event_id: record.cause_event_id,
                    causation_id: record.causation_id,
                    source_actor_id: record.source_actor_id,
                    binding: record.binding.clone(),
                    normalized_reason: record.normalized_reason.clone(),
                    payload_digest: record.payload_digest,
                    not_before: record.not_before,
                    deadline: record.deadline,
                    priority: record.priority,
                    recorded_at: record.recorded_at,
                    version: AggregateVersion::new(1),
                })
            }
            (Some(_), _) => Err(DomainError::InvalidTransition {
                from: "recorded".into(),
                command: "recorded".into(),
            }),
        }
    }

    pub fn replay(events: &[RunSignalEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "run_signal",
        })
    }

    /// Digest used by the scheduler's unique active-intent key.
    pub fn dedup_digest(&self) -> Result<Sha256Digest, DomainError> {
        #[derive(Serialize)]
        struct Material<'a> {
            schema: u8,
            project_id: ProjectId,
            subject: InvocationSubject,
            kind: RunSignalKind,
            normalized_reason: &'a str,
            binding: &'a InvocationBinding,
        }
        serde_json::to_vec(&Material {
            schema: 1,
            project_id: self.project_id,
            subject: self.subject,
            kind: self.kind,
            normalized_reason: &self.normalized_reason,
            binding: &self.binding,
        })
        .map(Sha256Digest::of_bytes)
        .map_err(|_| DomainError::Internal)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalMergeDecision {
    Coalesce,
    StartNewIntent,
    InterruptAndReplace,
    Incompatible,
}

pub fn decide_signal_merge(
    existing: &RunSignal,
    incoming: &RunSignal,
    run_is_active: bool,
) -> Result<SignalMergeDecision, DomainError> {
    if existing.project_id != incoming.project_id
        || existing.subject != incoming.subject
        || existing.binding != incoming.binding
    {
        return Ok(SignalMergeDecision::Incompatible);
    }
    if incoming.kind.interrupts_active_run() && run_is_active {
        return Ok(SignalMergeDecision::InterruptAndReplace);
    }
    if incoming.kind.forbids_coalescing_with_old_intent()
        || existing.dedup_digest()? != incoming.dedup_digest()?
    {
        return Ok(SignalMergeDecision::StartNewIntent);
    }
    Ok(SignalMergeDecision::Coalesce)
}

pub fn require_signal_coalescing(
    existing: &RunSignal,
    incoming: &RunSignal,
) -> Result<(), DomainError> {
    match decide_signal_merge(existing, incoming, false)? {
        SignalMergeDecision::Coalesce => Ok(()),
        SignalMergeDecision::Incompatible => Err(DomainError::SignalStale),
        SignalMergeDecision::StartNewIntent | SignalMergeDecision::InterruptAndReplace => {
            Err(DomainError::SignalNotCoalescible)
        }
    }
}

#[cfg(test)]
mod tests {
    use time::{Duration, macros::datetime};
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(seconds: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-08 00:00 UTC) + Duration::seconds(seconds))
    }

    fn record(byte: u8, kind: RunSignalKind) -> RecordRunSignal {
        RecordRunSignal {
            id: id(byte),
            project_id: id(20),
            subject: InvocationSubject::Attempt(id(21)),
            kind,
            cause_event_id: Some(id(22)),
            causation_id: Some(id(23)),
            source_actor_id: id(24),
            binding: InvocationBinding {
                package_id: Some(id(25)),
                package_revision_id: Some(id(26)),
                package_hash: Some(Sha256Digest::of_bytes(b"package")),
                attempt_id: Some(id(21)),
                author_fencing_token: Some(FencingToken::new(1).expect("fence")),
                workspace_head: Some(GitObjectId::new("ab".repeat(20)).expect("oid")),
                policy_revision_id: id(27),
                executor_fingerprint: Some(Sha256Digest::of_bytes(b"executor")),
                input_capsule_id: None,
                input_capsule_digest: None,
            },
            normalized_reason: "same_reason".into(),
            payload_digest: Sha256Digest::of_bytes(b"payload"),
            not_before: at(0),
            deadline: Some(at(10)),
            priority: 100,
            recorded_at: at(0),
        }
    }

    fn signal(byte: u8, kind: RunSignalKind) -> RunSignal {
        RunSignal::transition(None, &RunSignalCommand::Record(record(byte, kind)))
            .expect("record")
            .aggregate
    }

    #[test]
    fn all_documented_kinds_are_classified_for_active_and_idle_runs() {
        const KINDS: [RunSignalKind; 16] = [
            RunSignalKind::AssignmentGranted,
            RunSignalKind::WakeConditionSatisfied,
            RunSignalKind::QuestionAnswered,
            RunSignalKind::PermissionDecided,
            RunSignalKind::ArtifactAvailable,
            RunSignalKind::SemanticDeadlineReached,
            RunSignalKind::BudgetThresholdReached,
            RunSignalKind::NudgeRequested,
            RunSignalKind::DiagnoseRequested,
            RunSignalKind::PolicyChanged,
            RunSignalKind::PackageRevisionChanged,
            RunSignalKind::LeaseGenerationChanged,
            RunSignalKind::CancelRequested,
            RunSignalKind::SecurityTermination,
            RunSignalKind::RoutineDue,
            RunSignalKind::ReconcileRequested,
        ];
        for (index, kind) in KINDS.into_iter().enumerate() {
            let existing = signal(1, kind);
            let incoming = signal(u8::try_from(index + 2).expect("small"), kind);
            let idle = decide_signal_merge(&existing, &incoming, false).expect("merge");
            let active = decide_signal_merge(&existing, &incoming, true).expect("merge");
            if kind.forbids_coalescing_with_old_intent() {
                assert_eq!(idle, SignalMergeDecision::StartNewIntent);
            } else {
                assert_eq!(idle, SignalMergeDecision::Coalesce);
            }
            if kind.interrupts_active_run() {
                assert_eq!(active, SignalMergeDecision::InterruptAndReplace);
            }
        }
    }

    #[test]
    fn recorded_signal_is_immutable_and_replay_stable() {
        let command = RunSignalCommand::Record(record(1, RunSignalKind::AssignmentGranted));
        let created = RunSignal::transition(None, &command).expect("record");
        assert_eq!(
            RunSignal::replay(&created.events).expect("replay"),
            created.aggregate
        );
        assert!(matches!(
            RunSignal::decide(Some(&created.aggregate), &command),
            Err(DomainError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn scheduler_gets_stable_errors_for_stale_and_forbidden_coalescing() {
        let existing = signal(1, RunSignalKind::AssignmentGranted);
        let disruptive = signal(2, RunSignalKind::PolicyChanged);
        assert_eq!(
            require_signal_coalescing(&existing, &disruptive),
            Err(DomainError::SignalNotCoalescible)
        );

        let mut stale = signal(3, RunSignalKind::AssignmentGranted);
        stale.binding.author_fencing_token = Some(FencingToken::new(2).expect("fence"));
        assert_eq!(
            require_signal_coalescing(&existing, &stale),
            Err(DomainError::SignalStale)
        );
    }
}
