//! Versioned domain event envelopes.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    error::DomainError,
    ids::{
        ActorId, AggregateVersion, AttemptId, BudgetReservationId, CorrelationId, DecisionId,
        EventId, GovernanceCaseId, InvocationIntentId, InvocationRunId, LeaseId, PackageId,
        PolicyRevisionId, RunSignalId, ServerInstant, SessionCapsuleId, Sha256Digest, SubmissionId,
    },
};

pub const EVENT_ENVELOPE_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateType {
    WorkPackage,
    Attempt,
    Lease,
    Submission,
    RunSignal,
    InvocationIntent,
    InvocationRun,
    SessionCapsule,
    BudgetReservation,
    GovernanceCase,
    Decision,
    PolicyRevision,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum AggregateId {
    WorkPackage(PackageId),
    Attempt(AttemptId),
    Lease(LeaseId),
    Submission(SubmissionId),
    RunSignal(RunSignalId),
    InvocationIntent(InvocationIntentId),
    InvocationRun(InvocationRunId),
    SessionCapsule(SessionCapsuleId),
    BudgetReservation(BudgetReservationId),
    GovernanceCase(GovernanceCaseId),
    Decision(DecisionId),
    PolicyRevision(PolicyRevisionId),
}

impl AggregateId {
    #[must_use]
    pub const fn aggregate_type(self) -> AggregateType {
        match self {
            Self::WorkPackage(_) => AggregateType::WorkPackage,
            Self::Attempt(_) => AggregateType::Attempt,
            Self::Lease(_) => AggregateType::Lease,
            Self::Submission(_) => AggregateType::Submission,
            Self::RunSignal(_) => AggregateType::RunSignal,
            Self::InvocationIntent(_) => AggregateType::InvocationIntent,
            Self::InvocationRun(_) => AggregateType::InvocationRun,
            Self::SessionCapsule(_) => AggregateType::SessionCapsule,
            Self::BudgetReservation(_) => AggregateType::BudgetReservation,
            Self::GovernanceCase(_) => AggregateType::GovernanceCase,
            Self::Decision(_) => AggregateType::Decision,
            Self::PolicyRevision(_) => AggregateType::PolicyRevision,
        }
    }
}

/// Append-only event record. Unknown optional top-level metadata is preserved by
/// `optional_metadata`; mandatory extensions must be declared in
/// `required_semantics` and explicitly supported before apply.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope<E> {
    pub envelope_version: u16,
    pub event_id: EventId,
    pub aggregate_type: AggregateType,
    pub aggregate_id: AggregateId,
    /// State-row version produced by the command (one increment per command).
    pub aggregate_version: AggregateVersion,
    /// Per-event aggregate sequence (one increment per event).
    pub aggregate_seq: u64,
    pub event_type: String,
    pub schema_version: u16,
    pub actor_id: ActorId,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<EventId>,
    pub occurred_at: ServerInstant,
    pub payload_digest: Sha256Digest,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_semantics: Vec<String>,
    pub payload: E,
    #[serde(flatten)]
    pub optional_metadata: BTreeMap<String, Value>,
}

/// Name used by the normative domain document.
pub type DomainEventEnvelope<E> = EventEnvelope<E>;

/// Required values for constructing an envelope; keeping this separate prevents
/// positional argument mixups among the several UUID-derived identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventContext {
    pub event_id: EventId,
    pub aggregate_id: AggregateId,
    pub aggregate_version: AggregateVersion,
    pub aggregate_seq: u64,
    pub event_type: String,
    pub schema_version: u16,
    pub actor_id: ActorId,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<EventId>,
    pub occurred_at: ServerInstant,
}

impl<E> EventEnvelope<E>
where
    E: Serialize,
{
    pub fn new(context: EventContext, payload: E) -> Result<Self, DomainError> {
        let aggregate_type = context.aggregate_id.aggregate_type();
        let payload_digest = digest_payload(&payload)?;
        let envelope = Self {
            envelope_version: EVENT_ENVELOPE_VERSION,
            event_id: context.event_id,
            aggregate_type,
            aggregate_id: context.aggregate_id,
            aggregate_version: context.aggregate_version,
            aggregate_seq: context.aggregate_seq,
            event_type: context.event_type,
            schema_version: context.schema_version,
            actor_id: context.actor_id,
            correlation_id: context.correlation_id,
            causation_id: context.causation_id,
            occurred_at: context.occurred_at,
            payload_digest,
            required_semantics: Vec::new(),
            payload,
            optional_metadata: BTreeMap::new(),
        };
        envelope.validate_shape()?;
        Ok(envelope)
    }

    pub fn validate_payload_digest(&self) -> Result<(), DomainError> {
        if digest_payload(&self.payload)? == self.payload_digest {
            Ok(())
        } else {
            Err(DomainError::EvidenceInvalid)
        }
    }

    pub fn validate_for(
        &self,
        maximum_schema_version: u16,
        supported_semantics: &BTreeSet<String>,
    ) -> Result<(), DomainError> {
        self.validate_shape()?;
        if self.schema_version > maximum_schema_version
            || self
                .required_semantics
                .iter()
                .any(|feature| !supported_semantics.contains(feature))
        {
            return Err(DomainError::SchemaVersionUnsupported);
        }
        self.validate_payload_digest()
    }

    pub fn to_json(&self) -> Result<Vec<u8>, DomainError> {
        serde_json::to_vec(self).map_err(|_| DomainError::Internal)
    }

    fn validate_shape(&self) -> Result<(), DomainError> {
        if self.envelope_version != EVENT_ENVELOPE_VERSION {
            return Err(DomainError::SchemaVersionUnsupported);
        }
        if self.aggregate_type != self.aggregate_id.aggregate_type() {
            return Err(DomainError::InvariantViolation {
                invariant: "event_aggregate_type_must_match_id",
            });
        }
        if self.aggregate_version == AggregateVersion::ZERO || self.aggregate_seq == 0 {
            return Err(DomainError::InvalidArgument {
                field: "aggregate_version".into(),
                reason: "event versions and sequences must be non-zero".into(),
            });
        }
        if self.schema_version == 0 || self.event_type.is_empty() {
            return Err(DomainError::InvalidArgument {
                field: "event_type".into(),
                reason: "event type and schema version are required".into(),
            });
        }
        if self
            .optional_metadata
            .keys()
            .any(|key| is_reserved_key(key))
        {
            return Err(DomainError::InvalidArgument {
                field: "optional_metadata".into(),
                reason: "metadata must not shadow envelope fields".into(),
            });
        }
        Ok(())
    }
}

impl<E> EventEnvelope<E>
where
    E: Serialize + DeserializeOwned,
{
    pub fn from_json(bytes: &[u8]) -> Result<Self, DomainError> {
        let envelope: Self =
            serde_json::from_slice(bytes).map_err(|_| DomainError::SchemaInvalid)?;
        envelope.validate_shape()?;
        envelope.validate_payload_digest()?;
        Ok(envelope)
    }
}

fn digest_payload<E: Serialize>(payload: &E) -> Result<Sha256Digest, DomainError> {
    serde_json::to_vec(payload)
        .map(Sha256Digest::of_bytes)
        .map_err(|_| DomainError::Internal)
}

fn is_reserved_key(key: &str) -> bool {
    matches!(
        key,
        "envelope_version"
            | "event_id"
            | "aggregate_type"
            | "aggregate_id"
            | "aggregate_version"
            | "aggregate_seq"
            | "event_type"
            | "schema_version"
            | "actor_id"
            | "correlation_id"
            | "causation_id"
            | "occurred_at"
            | "payload_digest"
            | "required_semantics"
            | "payload"
    )
}

pub use crate::state::attempt::AttemptEvent;
pub use crate::state::budget::BudgetReservationEvent;
pub use crate::state::governance::{DecisionEvent, GovernanceCaseEvent};
pub use crate::state::invocation::{InvocationIntentEvent, InvocationRunEvent};
pub use crate::state::lease::LeaseEvent;
pub use crate::state::policy::PolicyRevisionEvent;
pub use crate::state::run_signal::RunSignalEvent;
pub use crate::state::session::SessionCapsuleEvent;
pub use crate::state::submission::SubmissionEvent;
pub use crate::state::work_package::WorkPackageEvent;

#[cfg(test)]
mod tests {
    use time::macros::datetime;
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    #[test]
    fn encode_decode_preserves_causal_identity_and_digest() {
        let envelope = EventEnvelope::new(
            EventContext {
                event_id: id(1),
                aggregate_id: AggregateId::Attempt(id(2)),
                aggregate_version: AggregateVersion::new(7),
                aggregate_seq: 9,
                event_type: "attempt.checkpointed".into(),
                schema_version: 1,
                actor_id: id(3),
                correlation_id: id(4),
                causation_id: Some(id(5)),
                occurred_at: ServerInstant(datetime!(2026-08-08 00:00 UTC)),
            },
            serde_json::json!({ "checkpoint": "sha256:test" }),
        )
        .expect("valid envelope");
        let bytes = envelope.to_json().expect("encode");
        let decoded: EventEnvelope<Value> = EventEnvelope::from_json(&bytes).expect("decode");
        assert_eq!(decoded.aggregate_id, envelope.aggregate_id);
        assert_eq!(decoded.aggregate_version, envelope.aggregate_version);
        assert_eq!(decoded.actor_id, envelope.actor_id);
        assert_eq!(decoded.correlation_id, envelope.correlation_id);
        assert_eq!(decoded.causation_id, envelope.causation_id);
        assert_eq!(decoded.payload_digest, envelope.payload_digest);
    }

    #[test]
    fn optional_metadata_round_trips_but_required_unknown_semantics_block_apply() {
        let mut envelope = EventEnvelope::new(
            EventContext {
                event_id: id(1),
                aggregate_id: AggregateId::Lease(id(2)),
                aggregate_version: AggregateVersion::new(1),
                aggregate_seq: 1,
                event_type: "lease.granted".into(),
                schema_version: 1,
                actor_id: id(3),
                correlation_id: id(4),
                causation_id: None,
                occurred_at: ServerInstant(datetime!(2026-08-08 00:00 UTC)),
            },
            serde_json::json!({ "generation": 1 }),
        )
        .expect("valid envelope");
        envelope.optional_metadata.insert(
            "trace_vendor".into(),
            serde_json::json!({ "sampled": true }),
        );
        let decoded: EventEnvelope<Value> =
            EventEnvelope::from_json(&envelope.to_json().expect("encode")).expect("decode");
        assert_eq!(decoded.optional_metadata, envelope.optional_metadata);

        envelope.required_semantics.push("future.fencing.v2".into());
        assert_eq!(
            envelope.validate_for(1, &BTreeSet::new()),
            Err(DomainError::SchemaVersionUnsupported)
        );
    }
}
