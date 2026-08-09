use agentforge_application::{EventRecord, PortError};
use agentforge_domain::{
    ActorId, AggregateId, AggregateType, AggregateVersion, CorrelationId, DomainEventEnvelope,
    EventId, PackageId, ProjectId, ServerInstant, event::EventContext,
};
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
fn event_append_record_preserves_validated_identity_and_cursor() {
    let project_id = id::<ProjectId>(9);
    let envelope = envelope();
    let record = EventRecord::from_domain(project_id, &envelope).expect("append record");
    assert_eq!(record.project_id, project_id);
    assert_eq!(record.event_id, envelope.event_id);
    assert_eq!(record.aggregate_id, envelope.aggregate_id);
    assert_eq!(record.cursor().event_id, envelope.event_id);
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
