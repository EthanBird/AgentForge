//! Immutable, content-addressed recovery capsules.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, ArtifactRef, DecisionId, GitObjectId, InvocationRunId, ProtocolKey,
        ServerInstant, SessionCapsuleId, Sha256Digest,
    },
    state::{
        Transition,
        run_signal::{InvocationBinding, InvocationSubject},
    },
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// The safe, structured minimum needed to resume an adapter session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionCapsuleContent {
    pub subject: InvocationSubject,
    pub binding: InvocationBinding,
    pub invocation_run_id: InvocationRunId,
    pub adapter_session_ref: Option<ProtocolKey>,
    pub checkpoint_digest: Sha256Digest,
    pub workspace_head: GitObjectId,
    pub acceptance_matrix_digest: Option<Sha256Digest>,
    pub open_blockers_digest: Option<Sha256Digest>,
    pub typed_question_ids: Vec<ProtocolKey>,
    pub approved_decision_ids: Vec<DecisionId>,
    pub last_outcome_digest: Option<Sha256Digest>,
    pub next_safe_action: ProtocolKey,
    pub executor_fingerprint: Sha256Digest,
    pub runtime_fingerprint: Sha256Digest,
    pub prompt_fingerprint: Sha256Digest,
    pub usage_baseline: TokenUsage,
    /// Only an encrypted, policy-authorized artifact reference may be stored.
    pub encrypted_transcript: Option<ArtifactRef>,
}

impl SessionCapsuleContent {
    fn validate(&self) -> Result<(), DomainError> {
        self.binding.validate_for(self.subject)?;
        if self.binding.workspace_head.as_ref() != Some(&self.workspace_head) {
            return Err(DomainError::CapsuleBindingMismatch);
        }
        if self
            .typed_question_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
            || self
                .approved_decision_ids
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(DomainError::InvalidArgument {
                field: "capsule_references".into(),
                reason: "must be strictly sorted and unique".into(),
            });
        }
        if self.encrypted_transcript.as_ref().is_some_and(|artifact| {
            !artifact.uri.starts_with("artifact://") && !artifact.uri.starts_with("https://")
        }) {
            return Err(DomainError::ScopeViolation);
        }
        Ok(())
    }

    fn digest(&self) -> Result<Sha256Digest, DomainError> {
        self.validate()?;
        serde_json::to_vec(self)
            .map(Sha256Digest::of_bytes)
            .map_err(|_| DomainError::Internal)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionCapsuleRef {
    pub id: SessionCapsuleId,
    pub digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordSessionCapsule {
    pub id: SessionCapsuleId,
    pub content: SessionCapsuleContent,
    pub recorded_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionCapsuleCommand {
    Record(Box<RecordSessionCapsule>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SessionCapsuleCommandKind {
    Record,
}

impl SessionCapsuleCommand {
    #[must_use]
    pub const fn kind(&self) -> SessionCapsuleCommandKind {
        SessionCapsuleCommandKind::Record
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SessionCapsuleEvent {
    Recorded {
        record: Box<RecordSessionCapsule>,
        digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionCapsule {
    pub id: SessionCapsuleId,
    pub content: SessionCapsuleContent,
    pub digest: Sha256Digest,
    pub recorded_at: ServerInstant,
    pub version: AggregateVersion,
}

impl SessionCapsule {
    pub fn transition(
        current: Option<&Self>,
        command: &SessionCapsuleCommand,
    ) -> Result<Transition<Self, SessionCapsuleEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &SessionCapsuleCommand,
    ) -> Result<SessionCapsuleEvent, DomainError> {
        match (current, command) {
            (None, SessionCapsuleCommand::Record(record)) => Ok(SessionCapsuleEvent::Recorded {
                digest: record.content.digest()?,
                record: record.clone(),
            }),
            (Some(_), _) => Err(DomainError::InvalidTransition {
                from: "recorded".into(),
                command: "record".into(),
            }),
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &SessionCapsuleEvent,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, SessionCapsuleEvent::Recorded { record, digest }) => {
                if record.content.digest()? != *digest {
                    return Err(DomainError::InvariantViolation {
                        invariant: "session_capsule_digest_must_match_content",
                    });
                }
                Ok(Self {
                    id: record.id,
                    content: record.content.clone(),
                    digest: *digest,
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

    pub fn replay(events: &[SessionCapsuleEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "session_capsule",
        })
    }

    #[must_use]
    pub const fn capsule_ref(&self) -> SessionCapsuleRef {
        SessionCapsuleRef {
            id: self.id,
            digest: self.digest,
        }
    }
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;
    use uuid::Uuid;

    use super::*;
    use crate::ids::{AttemptId, FencingToken, PackageId, PackageRevisionId, PolicyRevisionId};

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn content() -> SessionCapsuleContent {
        let attempt_id: AttemptId = id(1);
        let head = GitObjectId::new("ab".repeat(20)).expect("oid");
        SessionCapsuleContent {
            subject: InvocationSubject::Attempt(attempt_id),
            binding: InvocationBinding {
                package_id: Some(PackageId::from_uuid(Uuid::from_bytes([2; 16]))),
                package_revision_id: Some(PackageRevisionId::from_uuid(Uuid::from_bytes([3; 16]))),
                package_hash: Some(Sha256Digest::of_bytes(b"package")),
                attempt_id: Some(attempt_id),
                author_fencing_token: Some(FencingToken::new(1).expect("fence")),
                workspace_head: Some(head.clone()),
                policy_revision_id: PolicyRevisionId::from_uuid(Uuid::from_bytes([4; 16])),
                executor_fingerprint: Some(Sha256Digest::of_bytes(b"executor")),
                input_capsule_id: None,
                input_capsule_digest: None,
            },
            invocation_run_id: id(5),
            adapter_session_ref: Some(ProtocolKey::new("opaque-session-1").expect("key")),
            checkpoint_digest: Sha256Digest::of_bytes(b"checkpoint"),
            workspace_head: head,
            acceptance_matrix_digest: Some(Sha256Digest::of_bytes(b"ac")),
            open_blockers_digest: None,
            typed_question_ids: vec![],
            approved_decision_ids: vec![],
            last_outcome_digest: None,
            next_safe_action: ProtocolKey::new("resume").expect("key"),
            executor_fingerprint: Sha256Digest::of_bytes(b"executor"),
            runtime_fingerprint: Sha256Digest::of_bytes(b"runtime"),
            prompt_fingerprint: Sha256Digest::of_bytes(b"prompt"),
            usage_baseline: TokenUsage::default(),
            encrypted_transcript: None,
        }
    }

    #[test]
    fn capsule_is_content_addressed_replayable_and_immutable() {
        let command = SessionCapsuleCommand::Record(Box::new(RecordSessionCapsule {
            id: id(6),
            content: content(),
            recorded_at: ServerInstant(datetime!(2026-08-08 00:00 UTC)),
        }));
        let created = SessionCapsule::transition(None, &command).expect("record");
        assert_eq!(
            SessionCapsule::replay(&created.events).expect("replay"),
            created.aggregate
        );
        assert_eq!(
            created.aggregate.digest,
            content().digest().expect("digest")
        );
        assert!(matches!(
            SessionCapsule::decide(Some(&created.aggregate), &command),
            Err(DomainError::InvalidTransition { .. })
        ));
    }
}
