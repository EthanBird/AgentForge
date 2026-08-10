use std::time::Duration;

use agentforge_application::{
    EventAppendPort, EventRecord, Isolation, PortError, UnitOfWork, UnitOfWorkFactory,
};
use agentforge_domain::{
    ActorId, AggregateId, AggregateType, AggregateVersion, CommandId, CommandMetadata,
    CommandReceipt, CorrelationId, EventId, IdempotencyKey, IdempotencyScope, PackageId, ProjectId,
    ServerInstant, Sha256Digest, event::EVENT_ENVELOPE_VERSION,
};
use agentforge_storage_postgres::{
    CommandReceiptLookup, InboxDisposition, LocalNoTlsPostgresUnitOfWorkFactory, OutboxMessage,
    OutboxMessageId, migration,
};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, NoTls, types::Json};
use uuid::Uuid;

#[tokio::test]
async fn transactional_uow_phase1_contract() -> Result<()> {
    let Ok(database_url) = std::env::var("AGENTFORGE_TEST_DATABASE_URL") else {
        eprintln!("skipped: AGENTFORGE_TEST_DATABASE_URL is not configured");
        return Ok(());
    };
    if std::env::var("AGENTFORGE_TEST_ALLOW_SCHEMA_DROP").as_deref() != Ok("1") {
        return Err(anyhow!(
            "AGENTFORGE_TEST_ALLOW_SCHEMA_DROP=1 is required for the isolated schema test"
        ));
    }

    let (mut admin, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .context("connect to PostgreSQL fixture")?;
    let connection_task = tokio::spawn(connection);
    let schema = format!("af_uow_{}", Uuid::now_v7().simple());
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema}, pg_catalog"
        ))
        .await
        .context("create isolated UoW schema")?;
    let test_result = exercise_uow(&mut admin, &database_url, &schema).await;
    admin
        .batch_execute(&format!(
            "SET search_path TO public, pg_catalog; DROP SCHEMA {schema} CASCADE"
        ))
        .await
        .context("drop isolated UoW schema")?;
    drop(admin);
    connection_task
        .await
        .context("join PostgreSQL connection task")??;
    test_result
}

async fn exercise_uow(admin: &mut Client, database_url: &str, schema: &str) -> Result<()> {
    assert_eq!(migration::migrate(admin).await?, vec![1, 2, 3, 4, 5, 6]);
    let factory = LocalNoTlsPostgresUnitOfWorkFactory::new_local_no_tls(database_url, schema)?;
    let project_id = ProjectId::from_uuid(Uuid::now_v7());
    admin
        .execute(
            "INSERT INTO projects (id, protocol_key, name, state) \
             VALUES ($1, $2, 'UoW Fixture', 'ACTIVE')",
            &[project_id.as_uuid(), &format!("uow-{project_id}")],
        )
        .await?;

    let seeded = exercise_event_batches_and_durable_cursors(&factory, admin, project_id).await?;
    exercise_project_commit_order(&factory, admin, project_id).await?;
    exercise_digest_rejection_and_rollback_only(&factory, admin, project_id).await?;
    exercise_receipts(&factory, admin, project_id).await?;
    exercise_outbox_and_inbox(&factory, admin, project_id, seeded.primary_event_id).await?;
    Ok(())
}

struct SeededEvents {
    primary_event_id: EventId,
}

async fn exercise_event_batches_and_durable_cursors(
    factory: &LocalNoTlsPostgresUnitOfWorkFactory,
    admin: &Client,
    project_id: ProjectId,
) -> Result<SeededEvents> {
    let aggregate_a = Uuid::now_v7();
    let aggregate_b = Uuid::now_v7();
    let actor_id = ActorId::from_uuid(Uuid::now_v7());
    let correlation_id = CorrelationId::from_uuid(Uuid::now_v7());
    let event_a1 = EventId::from_uuid(Uuid::now_v7());
    let event_a2 = EventId::from_uuid(Uuid::now_v7());
    let event_b1 = EventId::from_uuid(Uuid::now_v7());

    let mut first = factory.begin(Isolation::ReadCommitted).await?;
    let now = first.server_now().await?;
    let batch = vec![
        event_record(EventSeed {
            project_id,
            aggregate_id: aggregate_b,
            event_id: event_b1,
            actor_id,
            correlation_id,
            aggregate_version: 1,
            aggregate_seq: 1,
            occurred_at: now,
            payload: json!({"aggregate": "b", "step": 1}),
        }),
        event_record(EventSeed {
            project_id,
            aggregate_id: aggregate_a,
            event_id: event_a2,
            actor_id,
            correlation_id,
            aggregate_version: 1,
            aggregate_seq: 2,
            occurred_at: now,
            payload: json!({"aggregate": "a", "step": 2}),
        }),
        event_record(EventSeed {
            project_id,
            aggregate_id: aggregate_a,
            event_id: event_a1,
            actor_id,
            correlation_id,
            aggregate_version: 1,
            aggregate_seq: 1,
            occurred_at: now,
            payload: json!({"aggregate": "a", "step": 1}),
        }),
    ];
    let receipt = first.append_events(&batch).await?;
    assert_eq!(receipt.appended, 3);
    let first_cursor = receipt.first_cursor.expect("first durable cursor");
    let last_cursor = receipt.last_cursor.expect("last durable cursor");
    assert!(first_cursor.event_sequence < last_cursor.event_sequence);
    first.commit().await?;

    let row = admin
        .query_one(
            "SELECT min(global_sequence), max(global_sequence), count(*), \
                    count(DISTINCT global_sequence), min(envelope_version), \
                    max(envelope_version) \
             FROM domain_events WHERE id = ANY($1)",
            &[&vec![
                event_a1.into_uuid(),
                event_a2.into_uuid(),
                event_b1.into_uuid(),
            ]],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(2), 3);
    assert_eq!(row.get::<_, i64>(3), 3);
    assert_eq!(row.get::<_, i16>(4), 2);
    assert_eq!(row.get::<_, i16>(5), 2);
    assert_eq!(
        u64::try_from(row.get::<_, i64>(0))?,
        first_cursor.event_sequence
    );
    assert_eq!(
        u64::try_from(row.get::<_, i64>(1))?,
        last_cursor.event_sequence
    );

    let mut second = factory.begin(Isolation::ReadCommitted).await?;
    let now = second.server_now().await?;
    let second_batch = vec![
        event_record(EventSeed {
            project_id,
            aggregate_id: aggregate_a,
            event_id: EventId::from_uuid(Uuid::now_v7()),
            actor_id,
            correlation_id,
            aggregate_version: 2,
            aggregate_seq: 4,
            occurred_at: now,
            payload: json!({"aggregate": "a", "step": 4}),
        }),
        event_record(EventSeed {
            project_id,
            aggregate_id: aggregate_a,
            event_id: EventId::from_uuid(Uuid::now_v7()),
            actor_id,
            correlation_id,
            aggregate_version: 2,
            aggregate_seq: 3,
            occurred_at: now,
            payload: json!({"aggregate": "a", "step": 3}),
        }),
    ];
    assert_eq!(second.append_events(&second_batch).await?.appended, 2);
    second.commit().await?;

    let head = admin
        .query_one(
            "SELECT aggregate_version, last_event_seq FROM aggregate_event_heads \
             WHERE project_id=$1 AND aggregate_type='WORK_PACKAGE' AND aggregate_id=$2",
            &[project_id.as_uuid(), &aggregate_a],
        )
        .await?;
    assert_eq!(head.get::<_, i64>(0), 2);
    assert_eq!(head.get::<_, i64>(1), 4);

    // A skipped command version cannot advance the head and poisons commit.
    let mut skipped = factory.begin(Isolation::ReadCommitted).await?;
    let skipped_event = event_record(EventSeed {
        project_id,
        aggregate_id: aggregate_a,
        event_id: EventId::from_uuid(Uuid::now_v7()),
        actor_id,
        correlation_id,
        aggregate_version: 4,
        aggregate_seq: 5,
        occurred_at: skipped.server_now().await?,
        payload: json!({"aggregate": "a", "step": 5}),
    });
    assert_eq!(
        skipped.append_events(&[skipped_event]).await,
        Err(PortError::Conflict)
    );
    assert_eq!(skipped.commit().await, Err(PortError::Conflict));

    Ok(SeededEvents {
        primary_event_id: event_a1,
    })
}

async fn exercise_project_commit_order(
    factory: &LocalNoTlsPostgresUnitOfWorkFactory,
    admin: &Client,
    project_id: ProjectId,
) -> Result<()> {
    let actor_id = ActorId::from_uuid(Uuid::now_v7());
    let correlation_id = CorrelationId::from_uuid(Uuid::now_v7());
    let first_event_id = EventId::from_uuid(Uuid::now_v7());
    let second_event_id = EventId::from_uuid(Uuid::now_v7());

    let mut first = factory.begin(Isolation::ReadCommitted).await?;
    let now = first.server_now().await?;
    let first_receipt = first
        .append_events(&[event_record(EventSeed {
            project_id,
            aggregate_id: Uuid::now_v7(),
            event_id: first_event_id,
            actor_id,
            correlation_id,
            aggregate_version: 1,
            aggregate_seq: 1,
            occurred_at: now,
            payload: json!({"writer": 1}),
        })])
        .await?;

    let second_factory = factory.clone();
    let second = tokio::spawn(async move {
        let mut uow = second_factory.begin(Isolation::ReadCommitted).await?;
        let now = uow.server_now().await?;
        let receipt = uow
            .append_events(&[event_record(EventSeed {
                project_id,
                aggregate_id: Uuid::now_v7(),
                event_id: second_event_id,
                actor_id,
                correlation_id,
                aggregate_version: 1,
                aggregate_seq: 1,
                occurred_at: now,
                payload: json!({"writer": 2}),
            })])
            .await?;
        uow.commit().await?;
        Ok::<_, PortError>(receipt)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !second.is_finished(),
        "same-project append must wait for the earlier transaction"
    );
    first.commit().await?;
    let second_receipt = second.await??;
    assert!(
        first_receipt
            .last_cursor
            .expect("first cursor")
            .event_sequence
            < second_receipt
                .first_cursor
                .expect("second cursor")
                .event_sequence
    );

    let ordered: Vec<Uuid> = admin
        .query(
            "SELECT id FROM domain_events WHERE id=ANY($1) ORDER BY global_sequence",
            &[&vec![
                first_event_id.into_uuid(),
                second_event_id.into_uuid(),
            ]],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        ordered,
        vec![first_event_id.into_uuid(), second_event_id.into_uuid()]
    );
    Ok(())
}

async fn exercise_digest_rejection_and_rollback_only(
    factory: &LocalNoTlsPostgresUnitOfWorkFactory,
    admin: &Client,
    project_id: ProjectId,
) -> Result<()> {
    let actor_id = ActorId::from_uuid(Uuid::now_v7());
    let aggregate_id = Uuid::now_v7();
    let scope = scope(
        project_id,
        actor_id,
        "fixture.rollback",
        "fixture:rollback:1",
    )?;
    let metadata = metadata(&scope, b"rollback");
    let receipt = receipt(&scope, &metadata, json!({"must_commit": false}));
    let mut uow = factory.begin(Isolation::ReadCommitted).await?;
    let now = uow.server_now().await?;
    uow.store_command_receipt(
        &receipt,
        future(now),
        Sha256Digest::of_bytes(b"rollback-effect"),
    )
    .await?;
    let mut forged = event_record(EventSeed {
        project_id,
        aggregate_id,
        event_id: EventId::from_uuid(Uuid::now_v7()),
        actor_id,
        correlation_id: metadata.correlation_id,
        aggregate_version: 1,
        aggregate_seq: 1,
        occurred_at: now,
        payload: json!({"must_commit": false, "part": 2}),
    });
    forged.payload_digest = Sha256Digest::of_bytes(b"forged-digest");
    assert_eq!(
        uow.append_events(&[forged]).await,
        Err(PortError::Integrity)
    );
    assert_eq!(uow.commit().await, Err(PortError::Integrity));

    let row = admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM command_receipts WHERE actor_id=$1), \
               (SELECT count(*) FROM aggregate_event_heads WHERE aggregate_id=$2)",
            &[actor_id.as_uuid(), &aggregate_id],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), 0);
    assert_eq!(row.get::<_, i64>(1), 0);

    let unknown_aggregate = Uuid::now_v7();
    let unknown_event_id = EventId::from_uuid(Uuid::now_v7());
    let mut unknown = factory.begin(Isolation::ReadCommitted).await?;
    let mut unknown_envelope = event_record(EventSeed {
        project_id,
        aggregate_id: unknown_aggregate,
        event_id: unknown_event_id,
        actor_id,
        correlation_id: metadata.correlation_id,
        aggregate_version: 1,
        aggregate_seq: 1,
        occurred_at: unknown.server_now().await?,
        payload: json!({"unknown_envelope_version": true}),
    });
    unknown_envelope.envelope_version = EVENT_ENVELOPE_VERSION + 1;
    assert_eq!(
        unknown.append_events(&[unknown_envelope]).await,
        Err(PortError::Integrity)
    );
    assert_eq!(unknown.commit().await, Err(PortError::Integrity));
    let persisted: i64 = admin
        .query_one(
            "SELECT count(*) FROM domain_events WHERE id=$1",
            &[unknown_event_id.as_uuid()],
        )
        .await?
        .get(0);
    assert_eq!(persisted, 0);
    Ok(())
}

async fn exercise_receipts(
    factory: &LocalNoTlsPostgresUnitOfWorkFactory,
    admin: &Client,
    project_id: ProjectId,
) -> Result<()> {
    let actor_id = ActorId::from_uuid(Uuid::now_v7());
    let scope = scope(project_id, actor_id, "fixture.create", "fixture:create:1")?;
    let metadata = metadata(&scope, b"fixture-create-v1");
    let receipt = receipt(
        &scope,
        &metadata,
        json!({"aggregate_id": Uuid::now_v7(), "version": 1}),
    );
    let effect_digest = Sha256Digest::of_bytes(b"event-and-outbox-effect");
    let mut store = factory.begin(Isolation::ReadCommitted).await?;
    let now = store.server_now().await?;
    store
        .store_command_receipt(&receipt, future(now), effect_digest)
        .await?;
    store.commit().await?;

    let mut replay = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        replay.find_command_receipt(&scope, &metadata).await?,
        CommandReceiptLookup::Replay(receipt.clone())
    );
    replay.rollback().await?;

    let mut changed = metadata.clone();
    changed.payload_digest = Sha256Digest::of_bytes(b"changed-request");
    let mut reuse = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        reuse
            .find_command_receipt::<serde_json::Value>(&scope, &changed)
            .await,
        Err(PortError::Conflict)
    );
    reuse.rollback().await?;

    let row = admin
        .query_one(
            "SELECT effect_digest, response_digest FROM command_receipts WHERE actor_id=$1",
            &[actor_id.as_uuid()],
        )
        .await?;
    let stored_effect: Vec<u8> = row.get(0);
    let stored_response: Vec<u8> = row.get(1);
    assert_eq!(stored_effect.as_slice(), effect_digest.as_bytes());
    assert_ne!(stored_effect, stored_response);

    exercise_expired_and_legacy_receipts(factory, admin, project_id).await?;
    Ok(())
}

async fn exercise_expired_and_legacy_receipts(
    factory: &LocalNoTlsPostgresUnitOfWorkFactory,
    admin: &Client,
    project_id: ProjectId,
) -> Result<()> {
    let expired_actor = ActorId::from_uuid(Uuid::now_v7());
    let expired_scope = scope(
        project_id,
        expired_actor,
        "fixture.expired",
        "fixture:expired:1",
    )?;
    let expired_metadata = metadata(&expired_scope, b"expired-request");
    insert_receipt_fixture(
        admin,
        &expired_scope,
        &expired_metadata,
        Some((CommandId::from_uuid(Uuid::now_v7()), 1)),
        true,
        false,
    )
    .await?;
    let mut expired = factory.begin(Isolation::ReadCommitted).await?;
    assert!(matches!(
        expired
            .find_command_receipt::<Value>(&expired_scope, &expired_metadata)
            .await?,
        CommandReceiptLookup::Expired { .. }
    ));
    expired.rollback().await?;

    let mut changed = expired_metadata.clone();
    changed.payload_digest = Sha256Digest::of_bytes(b"expired-but-reused");
    let mut expired_reuse = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        expired_reuse
            .find_command_receipt::<Value>(&expired_scope, &changed)
            .await,
        Err(PortError::Conflict)
    );
    expired_reuse.rollback().await?;

    let legacy_actor = ActorId::from_uuid(Uuid::now_v7());
    let legacy_scope = scope(
        project_id,
        legacy_actor,
        "fixture.legacy",
        "fixture:legacy:1",
    )?;
    let legacy_metadata = metadata(&legacy_scope, b"legacy-request");
    insert_receipt_fixture(admin, &legacy_scope, &legacy_metadata, None, false, false).await?;
    let mut legacy = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        legacy
            .find_command_receipt::<Value>(&legacy_scope, &legacy_metadata)
            .await?,
        CommandReceiptLookup::Legacy {
            response_digest: None
        }
    );
    legacy.rollback().await?;

    let corrupt_actor = ActorId::from_uuid(Uuid::now_v7());
    let corrupt_scope = scope(
        project_id,
        corrupt_actor,
        "fixture.corrupt",
        "fixture:corrupt:1",
    )?;
    let corrupt_metadata = metadata(&corrupt_scope, b"corrupt-request");
    insert_receipt_fixture(
        admin,
        &corrupt_scope,
        &corrupt_metadata,
        Some((CommandId::from_uuid(Uuid::now_v7()), 1)),
        false,
        true,
    )
    .await?;
    let mut corrupt = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        corrupt
            .find_command_receipt::<Value>(&corrupt_scope, &corrupt_metadata)
            .await,
        Err(PortError::Integrity)
    );
    corrupt.rollback().await?;

    let missing_actor = ActorId::from_uuid(Uuid::now_v7());
    let missing_scope = scope(
        project_id,
        missing_actor,
        "fixture.missing",
        "fixture:missing:1",
    )?;
    let missing_metadata = metadata(&missing_scope, b"missing");
    let mut missing = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        missing
            .find_command_receipt::<Value>(&missing_scope, &missing_metadata)
            .await?,
        CommandReceiptLookup::Missing
    );
    missing.rollback().await?;
    Ok(())
}

async fn exercise_outbox_and_inbox(
    factory: &LocalNoTlsPostgresUnitOfWorkFactory,
    admin: &Client,
    project_id: ProjectId,
    event_id: EventId,
) -> Result<()> {
    let first_message = OutboxMessageId::from_uuid(Uuid::from_bytes([0x11; 16]));
    let second_message = OutboxMessageId::from_uuid(Uuid::from_bytes([0x22; 16]));
    let mut enqueue = factory.begin(Isolation::ReadCommitted).await?;
    let now = enqueue.server_now().await?;
    enqueue
        .enqueue_outbox(&[
            OutboxMessage {
                id: second_message,
                project_id,
                event_id,
                topic: "domain.work-package.audit".into(),
                message_key: event_id.to_string(),
                envelope: json!({"event_id": event_id, "route": "audit"}),
                available_at: now,
            },
            OutboxMessage {
                id: first_message,
                project_id,
                event_id,
                topic: "domain.work-package".into(),
                message_key: event_id.to_string(),
                envelope: json!({"event_id": event_id, "route": "primary"}),
                available_at: now,
            },
        ])
        .await?;
    enqueue.commit().await?;

    let first_holder = Uuid::now_v7();
    let second_holder = Uuid::now_v7();
    let mut claim = factory.begin(Isolation::ReadCommitted).await?;
    let claims = claim
        .claim_outbox(first_holder, Duration::from_secs(30), 2)
        .await?;
    assert_eq!(
        claims.iter().map(|claim| claim.id).collect::<Vec<_>>(),
        vec![first_message, second_message]
    );
    assert!(claims.iter().all(|claim| claim.generation == 1));
    claim.commit().await?;

    let first_proof = claims[0].proof(first_holder);
    let second_proof = claims[1].proof(first_holder);
    let mut retry = factory.begin(Isolation::ReadCommitted).await?;
    let retry_at = retry.server_now().await?;
    retry
        .retry_outbox(first_proof, retry_at, "publisher.ack_lost")
        .await?;
    retry.commit().await?;

    let mut reclaim = factory.begin(Isolation::ReadCommitted).await?;
    let reclaimed = reclaim
        .claim_outbox(second_holder, Duration::from_secs(30), 1)
        .await?;
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id, first_message);
    assert_eq!(reclaimed[0].generation, 2);
    reclaim.commit().await?;

    let mut stale = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        stale.complete_outbox(first_proof).await,
        Err(PortError::Conflict)
    );
    assert_eq!(stale.commit().await, Err(PortError::Conflict));

    let payload_digest = Sha256Digest::of_bytes(b"published-envelope");
    let first_result = Sha256Digest::of_bytes(b"projection-effect-primary");
    let second_result = Sha256Digest::of_bytes(b"projection-effect-audit");
    let mut consume = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        consume
            .record_inbox_applied(
                "fixture-projection",
                first_message,
                project_id,
                payload_digest,
                first_result,
            )
            .await?,
        InboxDisposition::Applied
    );
    // Same domain event, different outbox id: this is a distinct message.
    assert_eq!(
        consume
            .record_inbox_applied(
                "fixture-projection",
                second_message,
                project_id,
                payload_digest,
                second_result,
            )
            .await?,
        InboxDisposition::Applied
    );
    consume.commit().await?;

    let mut duplicate = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        duplicate
            .record_inbox_applied(
                "fixture-projection",
                first_message,
                project_id,
                payload_digest,
                first_result,
            )
            .await?,
        InboxDisposition::Replay
    );
    duplicate.rollback().await?;

    let mut changed = factory.begin(Isolation::ReadCommitted).await?;
    assert_eq!(
        changed
            .record_inbox_applied(
                "fixture-projection",
                first_message,
                project_id,
                Sha256Digest::of_bytes(b"changed-envelope"),
                first_result,
            )
            .await,
        Err(PortError::Conflict)
    );
    changed.rollback().await?;

    let mut complete = factory.begin(Isolation::ReadCommitted).await?;
    complete
        .complete_outbox(reclaimed[0].proof(second_holder))
        .await?;
    complete.complete_outbox(second_proof).await?;
    complete.commit().await?;

    let row = admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM inbox_messages WHERE consumer='fixture-projection'), \
               (SELECT count(*) FROM outbox_messages WHERE published_at IS NOT NULL), \
               (SELECT attempts FROM outbox_messages WHERE id=$1), \
               (SELECT attempts FROM outbox_messages WHERE id=$2)",
            &[first_message.as_uuid(), second_message.as_uuid()],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 2);
    assert_eq!(row.get::<_, i32>(2), 2);
    assert_eq!(row.get::<_, i32>(3), 1);
    Ok(())
}

async fn insert_receipt_fixture(
    admin: &Client,
    scope: &IdempotencyScope,
    metadata: &CommandMetadata,
    modern: Option<(CommandId, i64)>,
    expired: bool,
    corrupt_response_digest: bool,
) -> Result<()> {
    let key_hash: [u8; 32] = Sha256::digest(scope.key.as_str().as_bytes()).into();
    let response = json!({"fixture": scope.command_type, "stable": true});
    let response_digest = if corrupt_response_digest {
        [0x44; 32]
    } else {
        Sha256::digest(serde_json_canonicalizer::to_vec(&response)?).into()
    };
    let effect_digest = Sha256Digest::of_bytes(b"fixture-effect");
    let (command_id, resource_version, stored_response_digest) = match modern {
        Some((command_id, version)) => (
            Some(command_id.into_uuid()),
            Some(version),
            Some(response_digest.to_vec()),
        ),
        None => (None, None, None),
    };
    let expiry = if expired {
        "clock_timestamp() - interval '1 second'"
    } else {
        "clock_timestamp() + interval '1 hour'"
    };
    admin
        .execute(
            &format!(
                "INSERT INTO command_receipts \
                 (actor_id, idempotency_key_hash, project_id, command_type, request_hash, \
                  response_classification, response_status, response_body, effect_digest, \
                  replay_until, command_id, resource_version, response_digest) \
                 VALUES ($1,$2,$3,$4,$5,'INTERNAL',200,$6,$7,{expiry},$8,$9,$10)"
            ),
            &[
                scope.actor_id.as_uuid(),
                &&key_hash[..],
                scope.project_id.as_uuid(),
                &scope.command_type,
                &&metadata.payload_digest.as_bytes()[..],
                &Json(&response),
                &&effect_digest.as_bytes()[..],
                &command_id,
                &resource_version,
                &stored_response_digest,
            ],
        )
        .await?;
    Ok(())
}

fn scope(
    project_id: ProjectId,
    actor_id: ActorId,
    command_type: &str,
    key: &str,
) -> Result<IdempotencyScope> {
    Ok(IdempotencyScope::new(
        project_id,
        command_type,
        actor_id,
        IdempotencyKey::new(key)?,
    )?)
}

fn metadata(scope: &IdempotencyScope, payload: &[u8]) -> CommandMetadata {
    CommandMetadata {
        command_id: CommandId::from_uuid(Uuid::now_v7()),
        actor_id: scope.actor_id,
        idempotency_key: scope.key.clone(),
        correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
        causation_id: None,
        expected_version: None,
        payload_digest: Sha256Digest::of_bytes(payload),
    }
}

fn receipt(
    scope: &IdempotencyScope,
    metadata: &CommandMetadata,
    response: Value,
) -> CommandReceipt<Value> {
    CommandReceipt {
        scope: scope.clone(),
        command_id: metadata.command_id,
        payload_digest: metadata.payload_digest,
        response,
        resource_version: AggregateVersion::new(1),
    }
}

fn future(now: ServerInstant) -> ServerInstant {
    ServerInstant::new(now.0 + Duration::from_secs(3_600))
}

struct EventSeed {
    project_id: ProjectId,
    aggregate_id: Uuid,
    event_id: EventId,
    actor_id: ActorId,
    correlation_id: CorrelationId,
    aggregate_version: u64,
    aggregate_seq: u64,
    occurred_at: ServerInstant,
    payload: Value,
}

fn event_record(seed: EventSeed) -> EventRecord {
    let payload_digest = Sha256Digest::of_bytes(
        serde_json_canonicalizer::to_vec(&seed.payload)
            .expect("fixture payload is canonicalizable"),
    );
    EventRecord {
        project_id: seed.project_id,
        event_id: seed.event_id,
        aggregate_type: AggregateType::WorkPackage,
        aggregate_id: AggregateId::WorkPackage(PackageId::from_uuid(seed.aggregate_id)),
        aggregate_version: AggregateVersion::new(seed.aggregate_version),
        aggregate_seq: seed.aggregate_seq,
        event_type: "fixture.changed".into(),
        schema_version: 1,
        envelope_version: EVENT_ENVELOPE_VERSION,
        actor_id: seed.actor_id,
        correlation_id: seed.correlation_id,
        causation_id: None,
        occurred_at: seed.occurred_at,
        payload_digest,
        required_semantics: Vec::new(),
        payload: seed.payload,
        optional_metadata: Default::default(),
    }
}
