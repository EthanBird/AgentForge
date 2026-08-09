use agentforge_application::{EventRecord, PortError};
use agentforge_domain::{
    ActorId, AggregateId, AggregateType, AggregateVersion, CorrelationId, DomainEventEnvelope,
    EventId, PackageId, ProjectId, ServerInstant, Sha256Digest,
    event::{EVENT_ENVELOPE_VERSION, EventContext},
};
use serde::Serialize;
use time::macros::datetime;
use uuid::Uuid;

fn id<T: From<Uuid>>(byte: u8) -> T {
    T::from(Uuid::from_bytes([byte; 16]))
}

fn envelope() -> DomainEventEnvelope<serde_json::Value> {
    DomainEventEnvelope::new(
        EventContext {
            event_id: id::<EventId>(1),
            aggregate_id: AggregateId::WorkPackage(id::<PackageId>(2)),
            aggregate_version: AggregateVersion::new(3),
            aggregate_seq: 4,
            event_type: "work_package.observed".to_owned(),
            schema_version: 1,
            actor_id: id::<ActorId>(5),
            correlation_id: id::<CorrelationId>(6),
            causation_id: None,
            occurred_at: ServerInstant(datetime!(2026-08-10 00:00 UTC)),
        },
        serde_json::json!({"state": "offered"}),
    )
    .expect("domain envelope")
}

#[test]
fn event_append_record_preserves_validated_identity_and_aggregate_sequence() {
    let project_id = id::<ProjectId>(9);
    let envelope = envelope();
    let record = EventRecord::from_domain(project_id, &envelope).expect("append record");
    assert_eq!(record.project_id, project_id);
    assert_eq!(record.event_id, envelope.event_id);
    assert_eq!(record.aggregate_id, envelope.aggregate_id);
    assert_eq!(record.aggregate_seq, envelope.aggregate_seq);
    assert_eq!(record.envelope_version, EVENT_ENVELOPE_VERSION);
    assert_eq!(record.payload, envelope.payload);
}

#[test]
fn event_append_boundary_rejects_a_malformed_public_envelope() {
    let mut envelope = envelope();
    envelope.aggregate_type = AggregateType::Lease;
    assert_eq!(
        EventRecord::from_domain(id::<ProjectId>(9), &envelope),
        Err(PortError::Integrity)
    );
}

#[test]
fn typed_payload_digest_survives_the_value_storage_boundary() {
    #[derive(Serialize)]
    struct TypedPayload {
        zeta: u8,
        alpha: u8,
    }

    let envelope = DomainEventEnvelope::new(
        EventContext {
            event_id: id::<EventId>(11),
            aggregate_id: AggregateId::WorkPackage(id::<PackageId>(12)),
            aggregate_version: AggregateVersion::new(1),
            aggregate_seq: 1,
            event_type: "work_package.created".to_owned(),
            schema_version: 1,
            actor_id: id::<ActorId>(13),
            correlation_id: id::<CorrelationId>(14),
            causation_id: None,
            occurred_at: ServerInstant(datetime!(2026-08-10 00:00 UTC)),
        },
        TypedPayload { zeta: 1, alpha: 2 },
    )
    .expect("typed envelope");
    let record = EventRecord::from_domain(id::<ProjectId>(15), &envelope).expect("append record");
    let stored_digest = Sha256Digest::of_bytes(
        serde_json_canonicalizer::to_vec(&record.payload).expect("canonical stored payload"),
    );

    assert_eq!(record.payload_digest, envelope.payload_digest);
    assert_eq!(record.payload_digest, stored_digest);
}
