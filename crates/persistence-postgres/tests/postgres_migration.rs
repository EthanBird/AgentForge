use agentforge_storage_postgres::migration;
use anyhow::{Context, Result, anyhow};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

#[tokio::test]
async fn migrations_are_atomic_idempotent_and_install_required_constraints() -> Result<()> {
    let Ok(database_url) = std::env::var("AGENTFORGE_TEST_DATABASE_URL") else {
        eprintln!("skipped: AGENTFORGE_TEST_DATABASE_URL is not configured");
        return Ok(());
    };
    if std::env::var("AGENTFORGE_TEST_ALLOW_SCHEMA_DROP").as_deref() != Ok("1") {
        return Err(anyhow!(
            "AGENTFORGE_TEST_ALLOW_SCHEMA_DROP=1 is required for the isolated schema test"
        ));
    }

    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .context("connect to PostgreSQL fixture")?;
    let connection_task = tokio::spawn(connection);
    let schema = format!("af_test_{}", Uuid::now_v7().simple());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema}"
        ))
        .await
        .context("create isolated test schema")?;

    let test_result = exercise_migrations(&mut client).await;
    client
        .batch_execute(&format!(
            "SET search_path TO public; DROP SCHEMA {schema} CASCADE"
        ))
        .await
        .context("drop isolated test schema")?;
    drop(client);
    connection_task
        .await
        .context("join PostgreSQL connection task")??;
    test_result
}

#[tokio::test]
async fn uow_migration_releases_legacy_claims_and_preserves_legacy_receipts() -> Result<()> {
    let Ok(database_url) = std::env::var("AGENTFORGE_TEST_DATABASE_URL") else {
        eprintln!("skipped: AGENTFORGE_TEST_DATABASE_URL is not configured");
        return Ok(());
    };
    if std::env::var("AGENTFORGE_TEST_ALLOW_SCHEMA_DROP").as_deref() != Ok("1") {
        return Err(anyhow!(
            "AGENTFORGE_TEST_ALLOW_SCHEMA_DROP=1 is required for the isolated schema test"
        ));
    }

    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .context("connect to PostgreSQL fixture")?;
    let connection_task = tokio::spawn(connection);
    let schema = format!("af_upgrade_{}", Uuid::now_v7().simple());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema}, pg_catalog"
        ))
        .await?;
    let result = exercise_uow_upgrade(&mut client).await;
    client
        .batch_execute(&format!(
            "SET search_path TO public, pg_catalog; DROP SCHEMA {schema} CASCADE"
        ))
        .await?;
    drop(client);
    connection_task.await??;
    result
}

#[tokio::test]
async fn uow_migration_rejects_permuted_legacy_event_history() -> Result<()> {
    let Ok(database_url) = std::env::var("AGENTFORGE_TEST_DATABASE_URL") else {
        eprintln!("skipped: AGENTFORGE_TEST_DATABASE_URL is not configured");
        return Ok(());
    };
    if std::env::var("AGENTFORGE_TEST_ALLOW_SCHEMA_DROP").as_deref() != Ok("1") {
        return Err(anyhow!(
            "AGENTFORGE_TEST_ALLOW_SCHEMA_DROP=1 is required for the isolated schema test"
        ));
    }

    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .context("connect to PostgreSQL fixture")?;
    let connection_task = tokio::spawn(connection);
    let schema = format!("af_bad_head_{}", Uuid::now_v7().simple());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema}, pg_catalog"
        ))
        .await?;

    let result = exercise_permuted_history_rejection(&mut client).await;
    // The migration error leaves its explicit transaction aborted.
    client.batch_execute("ROLLBACK").await?;
    client
        .batch_execute(&format!(
            "SET search_path TO public, pg_catalog; DROP SCHEMA {schema} CASCADE"
        ))
        .await?;
    drop(client);
    connection_task.await??;
    result
}

async fn exercise_permuted_history_rejection(client: &mut Client) -> Result<()> {
    for migration in &migration::MIGRATIONS[..3] {
        client.batch_execute(migration.sql).await?;
    }

    let project = Uuid::now_v7();
    let aggregate = Uuid::now_v7();
    let actor = Uuid::now_v7();
    let correlation = Uuid::now_v7();
    client
        .execute(
            "INSERT INTO projects (id, protocol_key, name, state)
             VALUES ($1,$2,'Bad Head Fixture','ACTIVE')",
            &[&project, &format!("bad-head-{project}")],
        )
        .await?;
    for (event_seq, aggregate_version) in [(1_i64, 2_i64), (2, 1)] {
        client
            .execute(
                "INSERT INTO domain_events
                 (id, project_id, aggregate_type, aggregate_id, aggregate_version, event_seq,
                  event_type, schema_version, payload, payload_digest, metadata, metadata_digest,
                  correlation_id, actor_id, occurred_at)
                 VALUES ($1,$2,'WORK_PACKAGE',$3,$4,$5,'fixture.permuted',1,'{}',
                         decode(repeat('11',32),'hex'),'{}',decode(repeat('22',32),'hex'),
                         $6,$7,clock_timestamp())",
                &[
                    &Uuid::now_v7(),
                    &project,
                    &aggregate,
                    &aggregate_version,
                    &event_seq,
                    &correlation,
                    &actor,
                ],
            )
            .await?;
    }

    let error = client
        .batch_execute(migration::MIGRATIONS[3].sql)
        .await
        .expect_err("permuted legacy history must block head derivation");
    assert_eq!(
        error.as_db_error().map(|error| error.code()),
        Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION)
    );
    Ok(())
}

async fn exercise_uow_upgrade(client: &mut Client) -> Result<()> {
    for migration in &migration::MIGRATIONS[..3] {
        client.batch_execute(migration.sql).await?;
    }

    let project = Uuid::now_v7();
    let aggregate = Uuid::now_v7();
    let event = Uuid::now_v7();
    let outbox = Uuid::now_v7();
    let holder = Uuid::now_v7();
    let actor = Uuid::now_v7();
    client
        .execute(
            "INSERT INTO projects (id, protocol_key, name, state)
             VALUES ($1,$2,'Upgrade Fixture','ACTIVE')",
            &[&project, &format!("upgrade-{project}")],
        )
        .await?;
    client
        .execute(
            "INSERT INTO domain_events
             (id, project_id, aggregate_type, aggregate_id, aggregate_version, event_seq,
              event_type, schema_version, payload, payload_digest, metadata, metadata_digest,
              correlation_id, actor_id, occurred_at)
             VALUES ($1,$2,'WORK_PACKAGE',$3,1,1,'fixture.created',1,'{}',
                     decode(repeat('11',32),'hex'),'{}',decode(repeat('22',32),'hex'),
                     $4,$5,clock_timestamp())",
            &[&event, &project, &aggregate, &Uuid::now_v7(), &actor],
        )
        .await?;
    client
        .execute(
            "INSERT INTO outbox_messages
             (id, project_id, event_id, topic, message_key, envelope, claimed_by,
              claim_expires_at, attempts)
             VALUES ($1,$2,$3,'fixture','fixture','{}',$4,
                     clock_timestamp()+interval '1 minute',1)",
            &[&outbox, &project, &event, &holder],
        )
        .await?;
    client
        .execute(
            "INSERT INTO command_receipts
             (actor_id, idempotency_key_hash, project_id, command_type, request_hash,
              response_classification, response_status, response_body, effect_digest,
              replay_until)
             VALUES ($1,decode(repeat('33',32),'hex'),$2,'fixture.legacy',
                     decode(repeat('44',32),'hex'),'INTERNAL',200,'{}',
                     decode(repeat('55',32),'hex'),clock_timestamp()+interval '1 hour')",
            &[&actor, &project],
        )
        .await?;

    client.batch_execute(migration::MIGRATIONS[3].sql).await?;
    let row = client
        .query_one(
            "SELECT claimed_by, claim_expires_at, claim_generation,
                    (SELECT aggregate_version FROM aggregate_event_heads
                     WHERE project_id=$2 AND aggregate_id=$3),
                    (SELECT last_event_seq FROM aggregate_event_heads
                     WHERE project_id=$2 AND aggregate_id=$3)
             FROM outbox_messages WHERE id=$1",
            &[&outbox, &project, &aggregate],
        )
        .await?;
    assert_eq!(row.get::<_, Option<Uuid>>(0), None);
    assert_eq!(row.get::<_, Option<time::OffsetDateTime>>(1), None);
    assert_eq!(row.get::<_, i64>(2), 0);
    assert_eq!(row.get::<_, i64>(3), 1);
    assert_eq!(row.get::<_, i64>(4), 1);
    let legacy_envelope_version: i16 = client
        .query_one(
            "SELECT envelope_version FROM domain_events WHERE id=$1",
            &[&event],
        )
        .await?
        .get(0);
    assert_eq!(legacy_envelope_version, 1);

    let legacy = client
        .query_one(
            "SELECT command_id, resource_version, response_digest
             FROM command_receipts WHERE actor_id=$1",
            &[&actor],
        )
        .await?;
    assert_eq!(legacy.get::<_, Option<Uuid>>(0), None);
    assert_eq!(legacy.get::<_, Option<i64>>(1), None);
    assert_eq!(legacy.get::<_, Option<Vec<u8>>>(2), None);

    let old_unique_exists: bool = client
        .query_one(
            "SELECT EXISTS (
               SELECT 1 FROM pg_constraint
               WHERE conrelid='domain_events'::regclass
                 AND conname='domain_events_aggregate_type_aggregate_id_aggregate_version_key'
             )",
            &[],
        )
        .await?
        .get(0);
    assert!(!old_unique_exists);
    Ok(())
}

async fn exercise_migrations(client: &mut Client) -> Result<()> {
    assert_eq!(migration::migrate(client).await?, vec![1, 2, 3, 4]);
    assert!(migration::migrate(client).await?.is_empty());

    let installed: Vec<String> = client
        .query(
            "SELECT tablename FROM pg_tables \
             WHERE schemaname = current_schema() ORDER BY tablename",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for required in [
        "agentforge_schema_migrations",
        "aggregate_event_heads",
        "attempts",
        "budget_reservations",
        "command_receipts",
        "domain_events",
        "governance_cases",
        "governance_decisions",
        "governance_execution_claims",
        "governance_execution_receipts",
        "invocation_intents",
        "invocation_runs",
        "leases",
        "outbox_messages",
        "projection_changes",
        "policy_scopes",
        "policy_activations",
        "run_claims",
        "run_signals",
        "session_capsules",
        "work_packages",
    ] {
        assert!(
            installed.iter().any(|table| table == required),
            "missing table {required}"
        );
    }

    let versions: Vec<i32> = client
        .query(
            "SELECT version FROM agentforge_schema_migrations ORDER BY version",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(versions, vec![1, 2, 3, 4]);

    let required_indexes: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = current_schema() \
             AND indexname IN (\
               'invocation_intents_one_active_dedup_idx',\
               'budget_reservations_one_active_intent_idx',\
               'run_claims_one_active_idx',\
               'governance_cases_inbox_idx',\
               'governance_execution_claims_one_active_idx',\
               'policy_revisions_one_active_idx',\
               'projection_changes_resume_idx'\
             )",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(required_indexes, 7);
    exercise_negative_contracts(client).await?;
    exercise_migration_prefix_rejection(client).await?;
    Ok(())
}

async fn exercise_negative_contracts(client: &mut Client) -> Result<()> {
    let project = Uuid::now_v7();
    let other_project = Uuid::now_v7();
    let policy_scope = Uuid::now_v7();
    let policy_revision = Uuid::now_v7();
    let signal = Uuid::now_v7();
    let intent = Uuid::now_v7();
    let account = Uuid::now_v7();
    let reservation = Uuid::now_v7();
    let run = Uuid::now_v7();
    let second_run = Uuid::now_v7();
    let second_reservation = Uuid::now_v7();
    let self_parented_reservation = Uuid::now_v7();
    let claim = Uuid::now_v7();
    let package = Uuid::now_v7();
    let other_package = Uuid::now_v7();
    let revision = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    let lease = Uuid::now_v7();
    let governance_case = Uuid::now_v7();
    let decision_one = Uuid::now_v7();
    let decision_two = Uuid::now_v7();
    let execution_claim = Uuid::now_v7();
    let execution_receipt = Uuid::now_v7();
    let attempt_governance_case = Uuid::now_v7();
    let stale_execution_claim = Uuid::now_v7();
    let zero_authorization_claim = Uuid::now_v7();
    let wrong_lease = Uuid::now_v7();
    let actor_one = Uuid::now_v7();
    let actor_two = Uuid::now_v7();
    let domain_event = Uuid::now_v7();

    client
        .batch_execute(&format!(
            r#"
            INSERT INTO projects (id, protocol_key, name, state)
            VALUES
              ('{project}', 'project-one', 'Project One', 'ACTIVE'),
              ('{other_project}', 'project-two', 'Project Two', 'ACTIVE');

            INSERT INTO domain_events
              (id, project_id, aggregate_type, aggregate_id, aggregate_version, event_seq,
               event_type, schema_version, envelope_version, payload, payload_digest, metadata,
               metadata_digest, correlation_id, actor_id, occurred_at)
            VALUES
              ('{domain_event}', '{project}', 'PROJECT', '{project}', 1, 1,
               'ProjectFixtureCreated', 1, 2, '{{}}', decode(repeat('39', 32), 'hex'), '{{}}',
               decode(repeat('40', 32), 'hex'), gen_random_uuid(), '{actor_one}',
               clock_timestamp());

            INSERT INTO policy_scopes
              (id, project_id, scope_type, subject_scope_id, category)
            VALUES
              ('{policy_scope}', '{project}', 'PROJECT', '{project}', 'GOVERNANCE');
            INSERT INTO policy_revisions
              (id, policy_scope_id, project_id, revision, state, schema_version,
               canonical_document, document_digest, created_by, reason_code, activated_at)
            VALUES
              ('{policy_revision}', '{policy_scope}', '{project}', 1, 'ACTIVE', '1',
               '{{}}', decode(repeat('11', 32), 'hex'), '{actor_one}', 'initial', clock_timestamp());
            UPDATE policy_scopes SET current_revision_id = '{policy_revision}', version = 1
            WHERE id = '{policy_scope}';
            INSERT INTO policy_activations
              (id, project_id, policy_scope_id, revision_id, expected_scope_version,
               impact_report_digest, action_digest, activated_by, reason_code)
            VALUES
              (gen_random_uuid(), '{project}', '{policy_scope}', '{policy_revision}', 0,
               decode(repeat('10', 32), 'hex'), decode(repeat('11', 32), 'hex'),
               '{actor_one}', 'initial');

            INSERT INTO run_signals
              (id, project_id, subject_type, subject_id, kind, source_actor_id,
               policy_revision_id, binding, binding_digest, normalized_reason,
               payload_digest, source_dedup_digest, not_before, priority)
            VALUES
              ('{signal}', '{project}', 'BOSS_SESSION', '{project}', 'NUDGE_REQUESTED',
               '{actor_one}', '{policy_revision}', '{{}}', decode(repeat('12', 32), 'hex'),
               'fixture', decode(repeat('13', 32), 'hex'), decode(repeat('14', 32), 'hex'),
               clock_timestamp(), 50);
            INSERT INTO invocation_intents
              (id, project_id, subject_type, subject_id, policy_revision_id, binding,
               binding_digest, dedup_digest, state, primary_signal_id)
            VALUES
              ('{intent}', '{project}', 'BOSS_SESSION', '{project}', '{policy_revision}', '{{}}',
               decode(repeat('12', 32), 'hex'), decode(repeat('15', 32), 'hex'),
               'PENDING', '{signal}');
            INSERT INTO invocation_intent_signals
              (intent_id, signal_id, project_id, binding_digest, policy_revision_id)
            VALUES
              ('{intent}', '{signal}', '{project}', decode(repeat('12', 32), 'hex'),
               '{policy_revision}');

            INSERT INTO budget_accounts
              (id, project_id, scope_type, scope_id, category, dimension, limit_units,
               reserved_units)
            VALUES
              ('{account}', '{project}', 'PROJECT', '{project}', 'AUTHOR_MODEL', 'TOKENS',
               1000, 100);
            INSERT INTO budget_reservations
              (id, project_id, account_id, intent_id, purpose_type, purpose_id,
               amount_units, state)
            VALUES
              ('{reservation}', '{project}', '{account}', '{intent}', 'INVOCATION_RUN',
               '{run}', 100, 'ACTIVE');
            INSERT INTO invocation_runs
              (id, intent_id, project_id, subject_type, subject_id, policy_revision_id,
               binding, binding_digest, adapter_id, executor_id, executor_fingerprint,
               budget_reservation_id, state, external_invocation_key)
            VALUES
              ('{run}', '{intent}', '{project}', 'BOSS_SESSION', '{project}',
               '{policy_revision}', '{{}}', decode(repeat('12', 32), 'hex'), 'fixture',
               '{actor_one}', decode(repeat('16', 32), 'hex'), '{reservation}',
               'RESERVED', 'fixture-invocation-one');
            UPDATE invocation_intents
            SET state = 'DISPATCHED', invocation_run_id = '{run}', version = 1
            WHERE id = '{intent}';

            UPDATE invocation_runs SET current_claim_generation = 1 WHERE id = '{run}';
            INSERT INTO run_claims
              (id, run_id, project_id, claim_request_id, generation, holder_id,
               token_hash, state, expires_at)
            VALUES
              ('{claim}', '{run}', '{project}', gen_random_uuid(), 1, '{actor_one}',
               decode(repeat('17', 32), 'hex'), 'ACTIVE', clock_timestamp() + interval '5 minutes');

            INSERT INTO work_packages
              (id, project_id, protocol_key, state, max_attempts)
            VALUES
              ('{package}', '{project}', 'PKG-TEST', 'ACTIVE', 3),
              ('{other_package}', '{other_project}', 'PKG-OTHER', 'ACTIVE', 3);
            INSERT INTO graph_versions
              (project_id, graph_version, base_graph_version, patch, patch_hash, created_by)
            VALUES
              ('{project}', 1, 0, '{{}}', decode(repeat('41', 32), 'hex'), '{actor_one}');
            INSERT INTO package_revisions
              (id, package_id, revision, schema_version, canonical_document, package_hash,
               base_commit, git_object_format, input_snapshot, created_by)
            VALUES
              ('{revision}', '{package}', 1, '1', '{{}}', decode(repeat('18', 32), 'hex'),
               repeat('a', 40), 'sha1', '{{}}', '{actor_one}');
            UPDATE work_packages SET selected_revision_id = '{revision}' WHERE id = '{package}';
            INSERT INTO attempts
              (id, protocol_key, package_id, revision_id, executor_id, node_id, state,
               fencing_token, base_commit)
            VALUES
              ('{attempt}', 'ATTEMPT-TEST', '{package}', '{revision}', '{actor_one}',
               '{actor_two}', 'LEASED', 1, repeat('a', 40));
            INSERT INTO leases
              (id, protocol_key, package_id, revision_id, attempt_id, holder_node_id,
               fencing_token, state, granted_at, expires_at, max_expires_at)
            VALUES
              ('{lease}', 'LEASE-TEST', '{package}', '{revision}', '{attempt}', '{actor_two}',
               1, 'ACTIVE', clock_timestamp(), clock_timestamp() + interval '5 minutes',
               clock_timestamp() + interval '10 minutes');
            UPDATE attempts SET lease_id = '{lease}' WHERE id = '{attempt}';

            INSERT INTO governance_cases
              (id, project_id, kind, risk, subject_type, subject_id, subject_version,
               authorization_epoch, requested_by, author_actor_id, package_id,
               package_revision_id, package_hash, attempt_id, attempt_lease_id,
               author_fencing_token, policy_revision_id, normalized_action, action_digest,
               resource_snapshot_digest, required_quorum, expires_at, timeout_behavior,
               state, version)
            VALUES
              ('{attempt_governance_case}', '{project}', 'OPERATIONAL_INTERVENTION', 'HIGH',
               'ATTEMPT', '{attempt}', 1, 1, '{actor_one}', '{actor_one}', '{package}',
               '{revision}', decode(repeat('18', 32), 'hex'), '{attempt}', '{lease}', 1,
               '{policy_revision}', '{{"kind":"revoke_author_lease"}}',
               decode(repeat('43', 32), 'hex'), decode(repeat('44', 32), 'hex'),
               '{{"count":1}}', clock_timestamp() + interval '10 minutes', 'DENY',
               'QUORUM_REACHED', 1);

            INSERT INTO governance_cases
              (id, project_id, kind, risk, subject_type, subject_id, subject_version,
               authorization_epoch, requested_by, author_actor_id, policy_revision_id,
               normalized_action, action_digest, resource_snapshot_digest, required_quorum,
               expires_at, timeout_behavior, state, version)
            VALUES
              ('{governance_case}', '{project}', 'OPERATIONAL_INTERVENTION', 'HIGH',
               'PROJECT', '{project}', 1, 1, '{actor_one}', '{actor_one}', '{policy_revision}',
               '{{"kind":"pause_project_dispatch"}}', decode(repeat('21', 32), 'hex'),
               decode(repeat('22', 32), 'hex'), '{{"count":2}}',
               clock_timestamp() + interval '10 minutes', 'DENY', 'QUORUM_REACHED', 3);
            INSERT INTO governance_decisions
              (id, case_id, project_id, case_version, authorization_epoch, actor_id,
               actor_role_snapshot_digest, conclusion, action_digest, policy_revision_id,
               rationale_code, idempotency_key_hash, request_digest, expires_at, signature)
            VALUES
              ('{decision_one}', '{governance_case}', '{project}', 1, 1, '{actor_one}',
               decode(repeat('23', 32), 'hex'), 'APPROVE', decode(repeat('21', 32), 'hex'),
               '{policy_revision}', 'evidence_sufficient', decode(repeat('24', 32), 'hex'),
               decode(repeat('25', 32), 'hex'), clock_timestamp() + interval '5 minutes',
               decode('aa', 'hex')),
              ('{decision_two}', '{governance_case}', '{project}', 2, 1, '{actor_two}',
               decode(repeat('26', 32), 'hex'), 'APPROVE', decode(repeat('21', 32), 'hex'),
               '{policy_revision}', 'evidence_sufficient', decode(repeat('27', 32), 'hex'),
               decode(repeat('28', 32), 'hex'), clock_timestamp() + interval '5 minutes',
               decode('bb', 'hex'));

            BEGIN;
            INSERT INTO governance_execution_claims
              (id, case_id, project_id, action_digest, policy_revision_id,
               authorization_epoch, generation, holder_actor_id, token_hash,
               authorization_digest, observed_case_version, observed_subject_version,
               state, issued_at, expires_at)
            VALUES
              ('{execution_claim}', '{governance_case}', '{project}',
               decode(repeat('21', 32), 'hex'), '{policy_revision}', 1, 1, '{actor_two}',
               decode(repeat('29', 32), 'hex'), decode(repeat('42', 32), 'hex'),
               3, 1, 'ACTIVE', clock_timestamp(),
               clock_timestamp() + interval '5 minutes');
            UPDATE governance_cases
            SET state = 'EXECUTING', execution_claim_id = '{execution_claim}',
                execution_claim_generation = 1, version = 4
            WHERE id = '{governance_case}' AND state = 'QUORUM_REACHED' AND version = 3;
            COMMIT;

            BEGIN;
            INSERT INTO governance_execution_receipts
              (id, case_id, project_id, action_digest, execution_claim_id,
               execution_claim_generation, executor_actor_id, status,
               external_effect_key, effect_digest, evidence_refs, started_at)
            VALUES
              ('{execution_receipt}', '{governance_case}', '{project}',
               decode(repeat('21', 32), 'hex'), '{execution_claim}', 1, '{actor_two}',
               'SUCCEEDED', 'fixture:effect:one', decode(repeat('30', 32), 'hex'),
               '[{{"artifact_id":"receipt-proof","uri":"artifact://receipt-proof","digest":"30"}}]',
               clock_timestamp());
            UPDATE governance_execution_claims
            SET state = 'COMPLETED', completed_at = clock_timestamp()
            WHERE id = '{execution_claim}';
            UPDATE governance_cases
            SET state = 'APPLIED', execution_receipt_id = '{execution_receipt}',
                terminalized_at = clock_timestamp(), version = 5
            WHERE id = '{governance_case}';
            COMMIT;
            "#
        ))
        .await
        .context("install persistence invariant fixtures")?;

    client
        .batch_execute(&format!(
            r#"
            INSERT INTO governance_execution_claims
              (id, case_id, project_id, action_digest, policy_revision_id,
               authorization_epoch, generation, holder_actor_id, token_hash,
               authorization_digest, observed_case_version, observed_subject_version,
               observed_attempt_id, observed_attempt_lease_id,
               observed_author_fencing_token, state, issued_at, expires_at)
            VALUES
              ('{zero_authorization_claim}', '{attempt_governance_case}', '{project}',
               decode(repeat('43', 32), 'hex'), '{policy_revision}', 1, 1, '{actor_two}',
               decode(repeat('45', 32), 'hex'), decode(repeat('00', 32), 'hex'), 1, 1,
               '{attempt}', '{lease}', 1, 'ACTIVE', clock_timestamp(),
               clock_timestamp() + interval '5 minutes');
            "#
        ))
        .await
        .expect_err("an execution claim must carry a non-zero authorization digest");

    client.batch_execute("BEGIN").await?;
    client
        .batch_execute(&format!(
            r#"
            INSERT INTO governance_execution_claims
              (id, case_id, project_id, action_digest, policy_revision_id,
               authorization_epoch, generation, holder_actor_id, token_hash,
               authorization_digest, observed_case_version, observed_subject_version,
               observed_attempt_id, observed_attempt_lease_id,
               observed_author_fencing_token, state, issued_at, expires_at)
            VALUES
              ('{stale_execution_claim}', '{attempt_governance_case}', '{project}',
               decode(repeat('43', 32), 'hex'), '{policy_revision}', 1, 1, '{actor_two}',
               decode(repeat('45', 32), 'hex'), decode(repeat('46', 32), 'hex'), 1, 1,
               '{attempt}', '{wrong_lease}', 1, 'ACTIVE', clock_timestamp(),
               clock_timestamp() + interval '5 minutes');
            UPDATE governance_cases
            SET state = 'EXECUTING', execution_claim_id = '{stale_execution_claim}',
                execution_claim_generation = 1, version = 2
            WHERE id = '{attempt_governance_case}'
              AND state = 'QUORUM_REACHED' AND version = 1;
            "#
        ))
        .await?;
    client
        .batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
        .await
        .expect_err("begin execution must reject a stale observed attempt/lease binding");
    client.batch_execute("ROLLBACK").await?;

    client
        .execute(
            "INSERT INTO governance_execution_receipts
             (id, case_id, project_id, action_digest, execution_claim_id,
              execution_claim_generation, executor_actor_id, status, effect_digest,
              started_at)
             VALUES ($1,$2,$3,decode(repeat('21',32),'hex'),$4,1,$5,'FAILED',
              decode(repeat('47',32),'hex'),clock_timestamp())",
            &[
                &Uuid::now_v7(),
                &governance_case,
                &project,
                &execution_claim,
                &actor_one,
            ],
        )
        .await
        .expect_err("a receipt executor must be the execution claim holder");

    client
        .execute(
            "INSERT INTO governance_execution_receipts
             (id, case_id, project_id, action_digest, execution_claim_id,
              execution_claim_generation, executor_actor_id, status, effect_digest,
              started_at)
             VALUES ($1,$2,$3,decode(repeat('21',32),'hex'),$4,1,$5,'SUCCEEDED',
              decode(repeat('00',32),'hex'),clock_timestamp())",
            &[
                &Uuid::now_v7(),
                &governance_case,
                &project,
                &execution_claim,
                &actor_two,
            ],
        )
        .await
        .expect_err("a successful receipt needs an effect key, non-zero digest, and evidence");

    client
        .execute(
            "UPDATE leases SET state='REVOKED', terminal_reason='test' WHERE id=$1",
            &[&lease],
        )
        .await?;
    client
        .execute(
            "UPDATE leases SET state='ACTIVE', terminal_reason=NULL WHERE id=$1",
            &[&lease],
        )
        .await
        .expect_err("terminal author lease must not revive");

    client
        .execute(
            "INSERT INTO domain_events
             (id, project_id, aggregate_type, aggregate_id, aggregate_version, event_seq,
              event_type, schema_version, envelope_version, payload, payload_digest, metadata,
              metadata_digest, correlation_id, actor_id, occurred_at)
             VALUES ($1,$2,'PROJECT',$2,2,2,'ProjectFixtureChanged',1,3,'{}',
                     decode(repeat('39',32),'hex'),'{}',decode(repeat('40',32),'hex'),
                     gen_random_uuid(),$3,clock_timestamp())",
            &[&Uuid::now_v7(), &project, &actor_one],
        )
        .await
        .expect_err("unknown event envelope versions must be rejected");

    client
        .execute(
            "UPDATE attempts SET fencing_token=2 WHERE id=$1",
            &[&attempt],
        )
        .await
        .expect_err("attempt revision/fencing binding must be immutable");

    client
        .execute(
            "UPDATE budget_accounts SET category='RUNNER' WHERE id=$1",
            &[&account],
        )
        .await
        .expect_err("budget account category must be immutable");

    client
        .execute(
            "UPDATE policy_scopes SET category='ROUTING' WHERE id=$1",
            &[&policy_scope],
        )
        .await
        .expect_err("policy scope/category binding must be immutable");

    client
        .execute(
            "UPDATE policy_revisions SET state='SUPERSEDED' WHERE id=$1",
            &[&policy_revision],
        )
        .await
        .expect_err("the current policy revision must remain active");

    client
        .execute(
            "INSERT INTO package_edges
             (project_id, graph_version, from_package_id, to_package_id, kind)
             VALUES ($1,1,$2,$3,'HARD_DEPENDENCY')",
            &[&project, &package, &other_package],
        )
        .await
        .expect_err("package graph edges cannot cross project boundaries");

    client
        .execute(
            "INSERT INTO outbox_messages
             (id, project_id, event_id, topic, message_key, envelope)
             VALUES ($1,$2,$3,'fixture','fixture','{}')",
            &[&Uuid::now_v7(), &other_project, &domain_event],
        )
        .await
        .expect_err("outbox messages cannot bind an event from another project");

    let inbox_message = Uuid::now_v7();
    client
        .execute(
            "INSERT INTO inbox_messages
             (consumer, message_id, project_id, payload_digest, applied_at, result_digest)
             VALUES ('migration-fixture', $1, $2,
                     decode(repeat('11', 32), 'hex'), clock_timestamp(),
                     decode(repeat('22', 32), 'hex'))",
            &[&inbox_message, &project],
        )
        .await?;
    client
        .execute(
            "UPDATE inbox_messages SET result_digest=decode(repeat('33', 32), 'hex')
             WHERE consumer='migration-fixture' AND message_id=$1",
            &[&inbox_message],
        )
        .await
        .expect_err("inbox dedupe facts must be immutable");

    client
        .execute(
            "INSERT INTO budget_reservations
             (id, project_id, account_id, parent_reservation_id, purpose_type,
              purpose_id, amount_units, state)
             VALUES ($1,$2,$3,$1,'ARTIFACT',$4,1,'ACTIVE')",
            &[
                &self_parented_reservation,
                &project,
                &account,
                &Uuid::now_v7(),
            ],
        )
        .await
        .expect_err("a budget reservation cannot parent itself");

    client
        .execute(
            "UPDATE invocation_runs SET current_claim_generation=3 WHERE id=$1",
            &[&run],
        )
        .await
        .expect_err("run claim generations cannot skip or regress");

    client
        .execute(
            "UPDATE run_claims SET state='COMPLETED',
             result_digest=decode(repeat('38',32),'hex'), completed_at=clock_timestamp()
             WHERE id=$1",
            &[&claim],
        )
        .await?;
    client
        .execute(
            "UPDATE run_claims SET state='ACTIVE', result_digest=NULL, completed_at=NULL
             WHERE id=$1",
            &[&claim],
        )
        .await
        .expect_err("terminal run claim must not revive");

    client
        .execute(
            "UPDATE invocation_runs SET state='RUNNING' WHERE id=$1",
            &[&run],
        )
        .await
        .expect_err("a running invocation must have an unexpired current active claim");

    client
        .execute(
            "UPDATE invocation_runs SET state='FAILED', outcome='OUTCOME_UNKNOWN',
             outcome_digest=decode(repeat('37',32),'hex'), terminalized_at=clock_timestamp()
             WHERE id=$1",
            &[&run],
        )
        .await?;
    client
        .execute(
            "UPDATE invocation_runs SET state='RESERVED', outcome=NULL, outcome_digest=NULL,
             terminalized_at=NULL WHERE id=$1",
            &[&run],
        )
        .await
        .expect_err("terminal invocation run must not revive");

    client
        .execute(
            "UPDATE governance_cases SET action_digest=decode(repeat('31',32),'hex') WHERE id=$1",
            &[&governance_case],
        )
        .await
        .expect_err("governance action binding must be immutable");

    client
        .execute(
            "UPDATE policy_scopes SET current_revision_id=NULL, version=version+1 WHERE id=$1",
            &[&policy_scope],
        )
        .await
        .expect_err("policy activation pointer change requires immutable activation history");

    client
        .execute(
            "INSERT INTO governance_decisions
             (id, case_id, project_id, case_version, authorization_epoch, actor_id,
              actor_role_snapshot_digest, conclusion, action_digest, policy_revision_id, rationale_code,
              idempotency_key_hash, request_digest, expires_at, signature)
             VALUES ($1,$2,$3,1,1,$4,decode(repeat('32',32),'hex'),'APPROVE',
              decode(repeat('33',32),'hex'),$5,'bad_digest',decode(repeat('34',32),'hex'),
              decode(repeat('35',32),'hex'),clock_timestamp()+interval '5 minutes',decode('cc','hex'))",
            &[&Uuid::now_v7(), &governance_case, &project, &Uuid::now_v7(), &policy_revision],
        )
        .await
        .expect_err("decision digest must match the immutable case action");

    client
        .execute(
            "INSERT INTO budget_reservations
             (id, project_id, account_id, intent_id, purpose_type, purpose_id,
              amount_units, state)
             VALUES ($1,$2,$3,$4,'INVOCATION_RUN',$5,1,'ACTIVE')",
            &[
                &Uuid::now_v7(),
                &other_project,
                &account,
                &intent,
                &Uuid::now_v7(),
            ],
        )
        .await
        .expect_err("reservation cannot cross project/account/intent scope");

    client
        .batch_execute(&format!(
            r#"
            UPDATE budget_reservations
            SET state='SETTLED', terminal_at=clock_timestamp(), spent_units=amount_units
            WHERE id='{reservation}';
            INSERT INTO budget_reservations
              (id, project_id, account_id, intent_id, purpose_type, purpose_id,
               amount_units, spent_units, state, terminal_at)
            VALUES
              ('{second_reservation}', '{project}', '{account}', '{intent}',
               'INVOCATION_RUN', '{second_run}', 1, 1, 'SETTLED', clock_timestamp());
            "#
        ))
        .await?;
    client
        .execute(
            "INSERT INTO invocation_runs
             (id, intent_id, project_id, subject_type, subject_id, policy_revision_id,
              binding, binding_digest, adapter_id, executor_id, executor_fingerprint,
              budget_reservation_id, state, external_invocation_key)
             VALUES ($1,$2,$3,'BOSS_SESSION',$3,$4,'{}',decode(repeat('12',32),'hex'),
              'fixture',$5,decode(repeat('36',32),'hex'),$6,'RESERVED','fixture-invocation-two')",
            &[
                &second_run,
                &intent,
                &project,
                &policy_revision,
                &actor_two,
                &second_reservation,
            ],
        )
        .await
        .expect_err("one invocation intent must not create a second run");

    Ok(())
}

async fn exercise_migration_prefix_rejection(client: &mut Client) -> Result<()> {
    client
        .execute(
            "DELETE FROM agentforge_schema_migrations WHERE version = 2",
            &[],
        )
        .await?;
    let error = migration::migrate(client)
        .await
        .expect_err("a migration-history gap must be rejected");
    assert!(matches!(
        error,
        migration::MigrationError::AppliedVersionGap {
            expected: 2,
            actual: 3
        }
    ));
    Ok(())
}
