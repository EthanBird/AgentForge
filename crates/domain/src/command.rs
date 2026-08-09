//! Command metadata and idempotent receipt matching.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        ActorId, AggregateVersion, CommandId, CorrelationId, EventId, IdempotencyKey, ProjectId,
        Sha256Digest,
    },
};

/// Metadata common to every external write command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandMetadata {
    pub command_id: CommandId,
    pub actor_id: ActorId,
    pub idempotency_key: IdempotencyKey,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<EventId>,
    /// Absent only for creation commands.
    pub expected_version: Option<AggregateVersion>,
    /// Digest of the canonical request payload, excluding transport metadata.
    pub payload_digest: Sha256Digest,
}

impl CommandMetadata {
    pub fn require_create(&self) -> Result<(), DomainError> {
        if self.expected_version.is_some() {
            return Err(DomainError::InvalidArgument {
                field: "expected_version".into(),
                reason: "must be absent for creation commands".into(),
            });
        }
        Ok(())
    }

    pub fn require_version(&self, current: AggregateVersion) -> Result<(), DomainError> {
        match self.expected_version {
            Some(expected) if expected == current => Ok(()),
            Some(_) => Err(DomainError::StaleVersion),
            None => Err(DomainError::InvalidArgument {
                field: "expected_version".into(),
                reason: "is required for aggregate updates".into(),
            }),
        }
    }
}

/// Minimum protocol scope for an idempotency key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyScope {
    pub project_id: ProjectId,
    pub command_type: String,
    pub actor_id: ActorId,
    pub key: IdempotencyKey,
}

impl IdempotencyScope {
    pub fn new(
        project_id: ProjectId,
        command_type: impl Into<String>,
        actor_id: ActorId,
        key: IdempotencyKey,
    ) -> Result<Self, DomainError> {
        let command_type = command_type.into();
        if command_type.is_empty()
            || command_type.len() > 96
            || !command_type
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(DomainError::InvalidArgument {
                field: "command_type".into(),
                reason: "must be a stable protocol label".into(),
            });
        }
        Ok(Self {
            project_id,
            command_type,
            actor_id,
            key,
        })
    }
}

/// Persisted in the same transaction as state, events, and outbox rows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandReceipt<R> {
    pub scope: IdempotencyScope,
    pub command_id: CommandId,
    pub payload_digest: Sha256Digest,
    pub response: R,
    pub resource_version: AggregateVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptDecision<'a, R> {
    Execute,
    Replay(&'a CommandReceipt<R>),
}

/// Determines whether domain logic should execute or a committed response is replayed.
///
/// Receipt matching must happen before checking the current aggregate or Lease. This
/// lets a caller recover an ACK after the successful command terminalized that object.
pub fn check_receipt<'a, R>(
    existing: Option<&'a CommandReceipt<R>>,
    scope: &IdempotencyScope,
    metadata: &CommandMetadata,
) -> Result<ReceiptDecision<'a, R>, DomainError> {
    let Some(receipt) = existing else {
        return Ok(ReceiptDecision::Execute);
    };
    // The persistence lookup is normally keyed by actor + idempotency key. If
    // an unrelated receipt is supplied, it cannot match this request.
    if receipt.scope.actor_id != scope.actor_id || receipt.scope.key != scope.key {
        return Ok(ReceiptDecision::Execute);
    }
    // Once actor + key match, changing *any* part of the command scope or
    // payload is key reuse, not a fresh command. This prevents callers from
    // moving a committed key across a project or command type.
    if receipt.scope.project_id != scope.project_id
        || receipt.scope.command_type != scope.command_type
        || receipt.payload_digest != metadata.payload_digest
    {
        return Err(DomainError::IdempotencyKeyReused);
    }
    Ok(ReceiptDecision::Replay(receipt))
}

// Aggregate commands are defined next to the states they mutate and re-exported
// here to provide a single discoverable command surface.
pub use crate::state::attempt::{AttemptCommand, AttemptCommandKind};
pub use crate::state::budget::{BudgetReservationCommand, BudgetReservationCommandKind};
pub use crate::state::governance::{
    DecisionCommand, DecisionCommandKind, GovernanceCaseCommand, GovernanceCaseCommandKind,
};
pub use crate::state::invocation::{
    InvocationIntentCommand, InvocationIntentCommandKind, InvocationRunCommand,
    InvocationRunCommandKind,
};
pub use crate::state::lease::{LeaseCommand, LeaseCommandKind};
pub use crate::state::policy::{PolicyRevisionCommand, PolicyRevisionCommandKind};
pub use crate::state::run_claim::{RunClaimCommand, RunClaimCommandKind};
pub use crate::state::run_signal::{RunSignalCommand, RunSignalCommandKind};
pub use crate::state::session::{SessionCapsuleCommand, SessionCapsuleCommandKind};
pub use crate::state::submission::{SubmissionCommand, SubmissionCommandKind};
pub use crate::state::work_package::{WorkPackageCommand, WorkPackageCommandKind};

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn metadata(payload: &[u8]) -> CommandMetadata {
        CommandMetadata {
            command_id: id(1),
            actor_id: id(2),
            idempotency_key: IdempotencyKey::new("release:lease-1").expect("valid key"),
            correlation_id: id(3),
            causation_id: None,
            expected_version: Some(AggregateVersion::new(1)),
            payload_digest: Sha256Digest::of_bytes(payload),
        }
    }

    #[test]
    fn same_key_and_payload_replays_original_receipt() {
        let metadata = metadata(b"same");
        let scope = IdempotencyScope::new(
            id(4),
            "lease.release",
            metadata.actor_id,
            metadata.idempotency_key.clone(),
        )
        .expect("valid scope");
        let receipt = CommandReceipt {
            scope: scope.clone(),
            command_id: metadata.command_id,
            payload_digest: metadata.payload_digest,
            response: "released",
            resource_version: AggregateVersion::new(2),
        };
        assert_eq!(
            check_receipt(Some(&receipt), &scope, &metadata),
            Ok(ReceiptDecision::Replay(&receipt))
        );
    }

    #[test]
    fn same_key_with_different_payload_is_stably_rejected() {
        let first = metadata(b"first");
        let second = metadata(b"second");
        let scope = IdempotencyScope::new(
            id(4),
            "lease.release",
            first.actor_id,
            first.idempotency_key.clone(),
        )
        .expect("valid scope");
        let receipt = CommandReceipt {
            scope: scope.clone(),
            command_id: first.command_id,
            payload_digest: first.payload_digest,
            response: "released",
            resource_version: AggregateVersion::new(2),
        };
        let error = check_receipt(Some(&receipt), &scope, &second).expect_err("must reject reuse");
        assert_eq!(error, DomainError::IdempotencyKeyReused);
        assert_eq!(error.code(), "AF_IDEMPOTENCY_KEY_REUSED");
    }

    #[test]
    fn same_actor_and_key_cannot_move_to_another_command_or_project() {
        let metadata = metadata(b"same");
        let original_scope = IdempotencyScope::new(
            id(4),
            "lease.release",
            metadata.actor_id,
            metadata.idempotency_key.clone(),
        )
        .expect("valid scope");
        let receipt = CommandReceipt {
            scope: original_scope.clone(),
            command_id: metadata.command_id,
            payload_digest: metadata.payload_digest,
            response: "released",
            resource_version: AggregateVersion::new(2),
        };

        let different_command = IdempotencyScope::new(
            original_scope.project_id,
            "lease.revoke",
            metadata.actor_id,
            metadata.idempotency_key.clone(),
        )
        .expect("valid scope");
        assert_eq!(
            check_receipt(Some(&receipt), &different_command, &metadata),
            Err(DomainError::IdempotencyKeyReused)
        );

        let different_project = IdempotencyScope::new(
            id(9),
            "lease.release",
            metadata.actor_id,
            metadata.idempotency_key.clone(),
        )
        .expect("valid scope");
        assert_eq!(
            check_receipt(Some(&receipt), &different_project, &metadata),
            Err(DomainError::IdempotencyKeyReused)
        );
    }

    #[test]
    fn receipt_for_a_different_actor_or_key_is_unrelated() {
        let metadata = metadata(b"same");
        let original_scope = IdempotencyScope::new(
            id(4),
            "lease.release",
            metadata.actor_id,
            metadata.idempotency_key.clone(),
        )
        .expect("valid scope");
        let receipt = CommandReceipt {
            scope: original_scope,
            command_id: metadata.command_id,
            payload_digest: metadata.payload_digest,
            response: "released",
            resource_version: AggregateVersion::new(2),
        };
        let unrelated = IdempotencyScope::new(
            id(4),
            "lease.release",
            id(8),
            IdempotencyKey::new("another-key").expect("key"),
        )
        .expect("valid scope");
        assert_eq!(
            check_receipt(Some(&receipt), &unrelated, &metadata),
            Ok(ReceiptDecision::Execute)
        );
    }
}
