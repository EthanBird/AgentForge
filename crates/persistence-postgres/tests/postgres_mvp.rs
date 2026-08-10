use std::sync::Arc;

use agentforge_application::{
    ClaimPackageInput, CreateProjectInput, ListOffersQuery, MvpCommand, MvpCommandContext,
    MvpControlPlane, PublishPackageInput, ReconcileExpiredLeasesQuery, ReleaseLeaseInput,
    RenewLeaseInput,
};
use agentforge_domain::{
    ActorId, AggregateVersion, CommandId, CorrelationId, ExecutorId, GitObjectId, IdempotencyKey,
    NodeId, PackageId, PackageRevision, PackageRevisionId, ProjectId, ProtocolKey, Sha256Digest,
    lease::LeaseState,
};
use agentforge_storage_postgres::{PostgresMvpControlPlane, migration};
use anyhow::{Context, Result, anyhow};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mvp_market_claim_and_lease_contract() -> Result<()> {
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
    let schema = format!("af_mvp_{}", Uuid::now_v7().simple());
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema}, pg_catalog"
        ))
        .await
        .context("create isolated MVP schema")?;

    let test_result = exercise_mvp(&mut admin, &database_url, &schema).await;
    admin
        .batch_execute(&format!(
            "SET search_path TO public, pg_catalog; DROP SCHEMA {schema} CASCADE"
        ))
        .await
        .context("drop isolated MVP schema")?;
    drop(admin);
    connection_task
        .await
        .context("join PostgreSQL connection task")??;
    test_result
}

async fn exercise_mvp(
    admin: &mut tokio_postgres::Client,
    database_url: &str,
    schema: &str,
) -> Result<()> {
    assert_eq!(migration::migrate(admin).await?, vec![1, 2, 3, 4]);
    let control = Arc::new(PostgresMvpControlPlane::new_local_no_tls(
        database_url,
        schema,
    )?);
    assert!(
        control.ready().await?,
        "migrated typed schema must be ready"
    );
    let current = migration::MIGRATIONS.last().expect("MVP migration");
    admin
        .execute(
            "UPDATE agentforge_schema_migrations SET source_digest = $1 WHERE version = $2",
            &[
                &format!("sha256:{}", "0".repeat(64)),
                &i32::try_from(current.version)?,
            ],
        )
        .await?;
    assert!(
        !control.ready().await?,
        "migration digest drift must fail readiness"
    );
    admin
        .execute(
            "UPDATE agentforge_schema_migrations SET source_digest = $1 WHERE version = $2",
            &[&current.digest(), &i32::try_from(current.version)?],
        )
        .await?;
    assert!(control.ready().await?, "restored manifest must be ready");

    let project_id = ProjectId::from_uuid(Uuid::now_v7());
    let project_command = MvpCommand {
        context: context("project-create", None),
        input: CreateProjectInput {
            project_id,
            protocol_key: ProtocolKey::new(format!("project-{project_id}"))?,
            name: "MVP contract".into(),
        },
    };
    let created = control.create_project(&project_command).await?;
    assert_eq!(control.create_project(&project_command).await?, created);

    let package_id = PackageId::from_uuid(Uuid::now_v7());
    let revision_id = PackageRevisionId::from_uuid(Uuid::now_v7());
    let canonical_document = json!({
        "schema_version": "afwp-1.0",
        "package_key": format!("wp-{package_id}"),
        "title": "Compile the MVP fixture"
    });
    let canonical_bytes = serde_json_canonicalizer::to_vec(&canonical_document)?;
    let package_hash = Sha256Digest::of_bytes(&canonical_bytes);
    let input_snapshot = json!({"fixtures": []});
    let publish_command = MvpCommand {
        context: context("package-publish", None),
        input: PublishPackageInput {
            project_id,
            package_id,
            package_key: ProtocolKey::new(format!("wp-{package_id}"))?,
            revision_id,
            revision: PackageRevision::new(1)?,
            schema_version: "afwp-1.0".into(),
            canonical_document: canonical_document.clone(),
            package_hash,
            base_commit: GitObjectId::new("1".repeat(40))?,
            git_object_format: "sha1".into(),
            input_snapshot: input_snapshot.clone(),
            created_by: ActorId::from_uuid(Uuid::now_v7()),
            graph_version: 0,
            priority: 75,
            max_attempts: 3,
        },
    };
    let published = control.publish_package(&publish_command).await?;
    assert_eq!(control.publish_package(&publish_command).await?, published);
    let offers = control
        .list_offers(ListOffersQuery {
            project_id,
            limit: 10,
        })
        .await?;
    assert_eq!(offers.len(), 1);
    assert_eq!(offers[0].package_id, package_id);

    let mut contenders = Vec::new();
    for index in 0..20_u8 {
        let command = MvpCommand {
            context: context(&format!("claim-{index}"), Some(published.version)),
            input: ClaimPackageInput {
                project_id,
                package_id,
                executor_id: ExecutorId::from_uuid(Uuid::now_v7()),
                node_id: NodeId::from_uuid(Uuid::now_v7()),
                lease_seconds: 60,
                max_lease_seconds: 600,
            },
        };
        let task_control = Arc::clone(&control);
        contenders.push(tokio::spawn(async move {
            let result = task_control.claim_package(&command).await;
            (command, result)
        }));
    }

    let mut winner = None;
    let mut rejected = 0;
    for contender in contenders {
        let (command, result) = contender.await?;
        match result {
            Ok(claim) => {
                assert!(
                    winner.replace((command, claim)).is_none(),
                    "one winner only"
                );
            }
            Err(error) => {
                assert!(
                    matches!(
                        error.code(),
                        "AF_CONFLICT"
                            | "AF_STALE_VERSION"
                            | "AF_TRANSITION_INVALID"
                            | "AF_PACKAGE_NOT_CLAIMABLE"
                    ),
                    "unexpected claim failure: {}",
                    error.code()
                );
                rejected += 1;
            }
        }
    }
    assert_eq!(rejected, 19);
    let (winning_command, claimed) = winner.expect("one claim succeeds");
    assert_eq!(control.claim_package(&winning_command).await?, claimed);
    assert_eq!(claimed.execution.revision, PackageRevision::new(1)?);
    assert_eq!(claimed.execution.package_hash, package_hash);
    assert_eq!(claimed.execution.canonical_document, canonical_document);
    assert_eq!(claimed.execution.input_snapshot, input_snapshot);
    assert_eq!(claimed.execution.base_commit.as_str(), "1".repeat(40));
    assert_eq!(claimed.execution.git_object_format, "sha1");
    assert!(claimed.granted_at < claimed.expires_at);
    assert!(claimed.expires_at <= claimed.max_expires_at);
    assert!(
        control
            .list_offers(ListOffersQuery {
                project_id,
                limit: 10,
            })
            .await?
            .is_empty()
    );

    let mut changed_claim = winning_command.clone();
    changed_claim.input.lease_seconds += 1;
    let reuse = control
        .claim_package(&changed_claim)
        .await
        .expect_err("same key with a changed body must fail");
    assert_eq!(reuse.code(), "AF_IDEMPOTENCY_KEY_REUSED");

    let renew_command = MvpCommand {
        context: context("lease-renew", Some(claimed.lease_version)),
        input: RenewLeaseInput {
            project_id,
            lease_id: claimed.lease_id,
            node_id: winning_command.input.node_id,
            fencing_token: claimed.fencing_token,
            extend_by_seconds: 30,
        },
    };
    let renewed = control.renew_lease(&renew_command).await?;
    assert_eq!(renewed.version, AggregateVersion::new(2));
    assert_eq!(control.renew_lease(&renew_command).await?, renewed);
    assert_eq!(
        control.get_lease(project_id, claimed.lease_id).await?,
        renewed
    );

    let release_command = MvpCommand {
        context: context("lease-release", Some(renewed.version)),
        input: ReleaseLeaseInput {
            project_id,
            lease_id: claimed.lease_id,
            node_id: winning_command.input.node_id,
            fencing_token: claimed.fencing_token,
        },
    };
    let released = control.release_lease(&release_command).await?;
    assert_eq!(released.state, LeaseState::Released);
    assert_eq!(released.version, AggregateVersion::new(3));
    assert_eq!(control.release_lease(&release_command).await?, released);

    let terminal_retry = MvpCommand {
        context: context("lease-release-new-key", Some(renewed.version)),
        input: release_command.input,
    };
    assert_eq!(
        control
            .release_lease(&terminal_retry)
            .await
            .expect_err("terminal lease cannot be released twice")
            .code(),
        "AF_TRANSITION_INVALID"
    );

    let reoffers = control
        .list_offers(ListOffersQuery {
            project_id,
            limit: 10,
        })
        .await?;
    assert_eq!(reoffers.len(), 1);
    assert_eq!(reoffers[0].package_id, package_id);
    let second_claim_command = MvpCommand {
        context: context("claim-after-release", Some(reoffers[0].version)),
        input: ClaimPackageInput {
            project_id,
            package_id,
            executor_id: ExecutorId::from_uuid(Uuid::now_v7()),
            node_id: NodeId::from_uuid(Uuid::now_v7()),
            lease_seconds: 5,
            max_lease_seconds: 60,
        },
    };
    let second_claim = control.claim_package(&second_claim_command).await?;
    assert_eq!(second_claim.fencing_token.get(), 2);
    assert_ne!(second_claim.attempt_id, claimed.attempt_id);
    assert_ne!(second_claim.lease_id, claimed.lease_id);
    admin.query_one("SELECT pg_sleep(5.2)", &[]).await?;
    let expiry = control
        .reconcile_expired_leases(ReconcileExpiredLeasesQuery {
            project_id,
            limit: 10,
        })
        .await?;
    assert_eq!(expiry.scanned, 1);
    assert_eq!(expiry.expired, 1);
    assert_eq!(expiry.conflicted, 0);
    assert_eq!(
        control
            .reconcile_expired_leases(ReconcileExpiredLeasesQuery {
                project_id,
                limit: 10,
            })
            .await?
            .scanned,
        0
    );

    let after_expiry = control
        .list_offers(ListOffersQuery {
            project_id,
            limit: 10,
        })
        .await?;
    assert_eq!(after_expiry.len(), 1);
    let third_claim = control
        .claim_package(&MvpCommand {
            context: context("claim-after-expiry", Some(after_expiry[0].version)),
            input: ClaimPackageInput {
                project_id,
                package_id,
                executor_id: ExecutorId::from_uuid(Uuid::now_v7()),
                node_id: NodeId::from_uuid(Uuid::now_v7()),
                lease_seconds: 60,
                max_lease_seconds: 600,
            },
        })
        .await?;
    assert_eq!(third_claim.fencing_token.get(), 3);

    let row = admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM attempts WHERE package_id=$1), \
               (SELECT count(*) FROM leases WHERE package_id=$1), \
               (SELECT count(*) FROM leases WHERE package_id=$1 AND state='ACTIVE'), \
               (SELECT count(*) FROM domain_events WHERE project_id=$2), \
               (SELECT count(*) FROM outbox_messages WHERE project_id=$2), \
               (SELECT count(*) FROM command_receipts WHERE project_id=$2), \
               (SELECT count(*) FROM attempts WHERE package_id=$1 AND state='LOST'), \
               (SELECT next_fencing_token FROM work_packages WHERE id=$1)",
            &[package_id.as_uuid(), project_id.as_uuid()],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), 3);
    assert_eq!(row.get::<_, i64>(1), 3);
    assert_eq!(row.get::<_, i64>(2), 1);
    assert_eq!(row.get::<_, i64>(3), 18);
    assert_eq!(row.get::<_, i64>(4), 18);
    assert_eq!(row.get::<_, i64>(5), 8);
    assert_eq!(row.get::<_, i64>(6), 2);
    assert_eq!(row.get::<_, i64>(7), 3);
    Ok(())
}

fn context(key: &str, expected_version: Option<AggregateVersion>) -> MvpCommandContext {
    MvpCommandContext {
        command_id: CommandId::from_uuid(Uuid::now_v7()),
        actor_id: ActorId::from_uuid(Uuid::now_v7()),
        idempotency_key: IdempotencyKey::new(key).expect("test idempotency key"),
        correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
        causation_id: None,
        expected_version,
    }
}
