//! Versioned domain event envelopes.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    error::DomainError,
    ids::{
        ActorId, AggregateVersion, AttemptId, BudgetReservationId, CorrelationId, DecisionId,
        EventId, GovernanceCaseId, InvocationIntentId, InvocationRunId, LeaseId, PackageId,
        PolicyRevisionId, RunClaimId, RunSignalId, ServerInstant, SessionCapsuleId, Sha256Digest,
        SubmissionId,
    },
};

/// Historical envelope format whose payload digest used typed
/// `serde_json::to_vec` bytes. It is read-only.
pub const LEGACY_EVENT_ENVELOPE_VERSION: u16 = 1;
/// Current envelope format. New events use JCS payload bytes so typed payloads
/// and their JSON value representation have one stable digest.
pub const EVENT_ENVELOPE_VERSION: u16 = 2;

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
    RunClaim,
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
    RunClaim(RunClaimId),
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
            Self::RunClaim(_) => AggregateType::RunClaim,
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
        let payload_digest = digest_payload_v2(&payload)?;
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
        let actual = match self.envelope_version {
            LEGACY_EVENT_ENVELOPE_VERSION => digest_payload_v1(&self.payload)?,
            EVENT_ENVELOPE_VERSION => digest_payload_v2(&self.payload)?,
            _ => return Err(DomainError::SchemaVersionUnsupported),
        };
        if actual == self.payload_digest {
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
        // Historical v1 envelopes are accepted only by `from_json` for audit
        // replay/upcast. They must never be appended as if newly emitted.
        if self.envelope_version != EVENT_ENVELOPE_VERSION
            || self.schema_version > maximum_schema_version
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
        if !matches!(
            self.envelope_version,
            LEGACY_EVENT_ENVELOPE_VERSION | EVENT_ENVELOPE_VERSION
        ) {
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

fn digest_payload_v1<E: Serialize>(payload: &E) -> Result<Sha256Digest, DomainError> {
    serde_json::to_vec(payload)
        .map(Sha256Digest::of_bytes)
        .map_err(|_| DomainError::Internal)
}

fn digest_payload_v2<E: Serialize>(payload: &E) -> Result<Sha256Digest, DomainError> {
    serde_json_canonicalizer::to_vec(payload)
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
pub use crate::state::invocation::{
    InvocationIntentEvent, InvocationRunEvent, InvocationRunEventV1, UpcastInvocationRunEventV1,
};
pub use crate::state::lease::LeaseEvent;
pub use crate::state::policy::PolicyRevisionEvent;
pub use crate::state::run_claim::RunClaimEvent;
pub use crate::state::run_signal::RunSignalEvent;
pub use crate::state::session::SessionCapsuleEvent;
pub use crate::state::submission::SubmissionEvent;
pub use crate::state::work_package::WorkPackageEvent;

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
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
        assert_eq!(envelope.envelope_version, EVENT_ENVELOPE_VERSION);
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
    fn payload_digest_is_canonical_across_typed_and_value_representations() {
        #[derive(Serialize)]
        struct TypedPayload {
            zeta: u8,
            alpha: u8,
        }

        let context = EventContext {
            event_id: id(21),
            aggregate_id: AggregateId::InvocationRun(id(22)),
            aggregate_version: AggregateVersion::new(1),
            aggregate_seq: 1,
            event_type: "invocation_run.reserved".into(),
            schema_version: 2,
            actor_id: id(23),
            correlation_id: id(24),
            causation_id: None,
            occurred_at: ServerInstant(datetime!(2026-08-10 00:00 UTC)),
        };
        let typed = EventEnvelope::new(context.clone(), TypedPayload { zeta: 1, alpha: 2 })
            .expect("typed payload");
        let value = EventEnvelope::new(context, serde_json::json!({"alpha": 2, "zeta": 1}))
            .expect("value payload");

        assert_eq!(typed.payload_digest, value.payload_digest);
    }

    #[test]
    fn legacy_v1_digest_is_verified_with_original_bytes_but_cannot_be_reappended() {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        struct LegacyTypedPayload {
            zeta: u8,
            alpha: u8,
        }

        let mut legacy = EventEnvelope::new(
            EventContext {
                event_id: id(31),
                aggregate_id: AggregateId::InvocationRun(id(32)),
                aggregate_version: AggregateVersion::new(1),
                aggregate_seq: 1,
                event_type: "invocation_run.reserved".into(),
                schema_version: 1,
                actor_id: id(33),
                correlation_id: id(34),
                causation_id: None,
                occurred_at: ServerInstant(datetime!(2026-08-10 00:00 UTC)),
            },
            LegacyTypedPayload { zeta: 1, alpha: 2 },
        )
        .expect("current envelope");
        legacy.envelope_version = LEGACY_EVENT_ENVELOPE_VERSION;
        legacy.payload_digest = digest_payload_v1(&legacy.payload).expect("legacy digest");
        let bytes = legacy.to_json().expect("legacy json");
        let decoded: EventEnvelope<LegacyTypedPayload> =
            EventEnvelope::from_json(&bytes).expect("verified historical envelope");

        assert_eq!(decoded, legacy);
        assert_eq!(
            decoded.validate_for(1, &BTreeSet::new()),
            Err(DomainError::SchemaVersionUnsupported)
        );

        let mut tampered: Value = serde_json::from_slice(&bytes).expect("json value");
        tampered["payload"]["alpha"] = Value::from(9);
        assert_eq!(
            EventEnvelope::<LegacyTypedPayload>::from_json(
                &serde_json::to_vec(&tampered).expect("tampered json")
            ),
            Err(DomainError::EvidenceInvalid)
        );
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
