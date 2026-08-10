//! PostgreSQL-backed typed command surface for the runnable MVP.

use agentforge_application::{
    ClaimPackageInput, ClaimedWork, CreateProjectInput, EventAppendPort, EventRecord, Isolation,
    LeaseReconciliationReport, LeaseView, ListOffersQuery, MvpCommand, MvpControlPlane, MvpError,
    MvpFuture, MvpResult, OfferView, ProjectView, PublishPackageInput, PublishedPackage,
    ReconcileExpiredLeasesQuery, ReleaseLeaseInput, RenewLeaseInput, UnitOfWork, UnitOfWorkFactory,
};
use agentforge_domain::{
    ActorId, AggregateId, AggregateVersion, Attempt, AttemptId, CommandId, CommandMetadata,
    CommandReceipt, CorrelationId, DomainEventEnvelope, EventContext, EventId, FencingToken,
    GitObjectId, IdempotencyKey, IdempotencyScope, Lease, LeaseId, PackageRevision,
    PackageRevisionId, ProjectId, ProtocolKey, ServerInstant, Sha256Digest, WorkPackage,
    attempt::{AttemptCommand, AttemptState, NewAttempt, WakeCondition},
    command::ReceiptDecision,
    lease::{GrantLease, LeaseCommand, LeaseState},
    work_package::{
        ClaimReadiness, NewWorkPackage, PublishReadiness, WorkPackageCommand, WorkPackageState,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use time::Duration;
use tokio_postgres::{Row, types::Json};
use uuid::Uuid;

use crate::migration::MIGRATIONS;
use crate::uow::{
    CommandReceiptLookup, LocalNoTlsPostgresUnitOfWorkFactory, OutboxMessage, OutboxMessageId,
    PostgresUnitOfWork, map_database_error,
};

const RECEIPT_REPLAY_HOURS: i64 = 24;
const EVENT_SCHEMA_VERSION: u16 = 1;
const DOMAIN_EVENT_TOPIC: &str = "agentforge.domain.v1";

#[derive(Clone, Copy, Debug)]
enum LeaseTermination {
    Release {
        holder_node_id: agentforge_domain::NodeId,
        fencing_token: FencingToken,
    },
    Expire,
}

impl LeaseTermination {
    fn command(self, expected_version: AggregateVersion, now: ServerInstant) -> LeaseCommand {
        match self {
            Self::Release {
                holder_node_id,
                fencing_token,
            } => LeaseCommand::ReleaseLease {
                expected_version,
                holder_node_id,
                fencing_token,
                now,
            },
            Self::Expire => LeaseCommand::ExpireLease {
                expected_version,
                now,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct PostgresMvpControlPlane {
    factory: LocalNoTlsPostgresUnitOfWorkFactory,
}

impl PostgresMvpControlPlane {
    pub fn new_local_no_tls(
        database_url: impl Into<String>,
        trusted_schema: impl Into<String>,
    ) -> MvpResult<Self> {
        Ok(Self {
            factory: LocalNoTlsPostgresUnitOfWorkFactory::new_local_no_tls(
                database_url,
                trusted_schema,
            )?,
        })
    }

    async fn ready_inner(&self) -> MvpResult<bool> {
        let uow = self.factory.begin(Isolation::ReadCommitted).await?;
        let result = async {
            let rows = uow
                .client()?
                .query(
                    "SELECT version, name, source_digest \
                     FROM agentforge_schema_migrations ORDER BY version",
                    &[],
                )
                .await
                .map_err(map_database_error)?;
            if rows.len() != MIGRATIONS.len() {
                return Ok(false);
            }
            for (row, expected) in rows.iter().zip(MIGRATIONS) {
                let version: i32 = row
                    .try_get(0)
                    .map_err(|_| agentforge_application::PortError::Integrity)?;
                let name: String = row
                    .try_get(1)
                    .map_err(|_| agentforge_application::PortError::Integrity)?;
                let digest: String = row
                    .try_get(2)
                    .map_err(|_| agentforge_application::PortError::Integrity)?;
                if u32::try_from(version).ok() != Some(expected.version)
                    || name != expected.name
                    || digest != expected.digest()
                {
                    return Ok(false);
                }
            }

            let row = uow
                .client()?
                .query_one(
                    "SELECT ARRAY[
                       to_regclass('projects'),
                       to_regclass('work_packages'),
                       to_regclass('attempts'),
                       to_regclass('leases'),
                       to_regclass('domain_events'),
                       to_regclass('outbox_messages'),
                       to_regclass('command_receipts')
                     ]::text[]",
                    &[],
                )
                .await
                .map_err(map_database_error)?;
            let required_tables: Vec<Option<String>> = row
                .try_get(0)
                .map_err(|_| agentforge_application::PortError::Integrity)?;
            Ok(required_tables.len() == 7 && required_tables.iter().all(Option::is_some))
        }
        .await;
        finish(uow, result).await
    }

    async fn create_project_inner(
        &self,
        command: &MvpCommand<CreateProjectInput>,
    ) -> MvpResult<ProjectView> {
        let metadata = command.context.metadata(&command.input)?;
        metadata.require_create()?;
        validate_project_input(&command.input)?;
        let scope = scope(command.input.project_id, "project.create", &metadata)?;
        let mut uow = self.factory.begin(Isolation::Serializable).await?;
        let result = async {
            if let Some(response) = replay::<ProjectView>(&mut uow, &scope, &metadata).await? {
                return Ok(response);
            }
            let version = AggregateVersion::new(1);
            let changed = uow
                .client()?
                .execute(
                    "INSERT INTO projects \
                     (id, protocol_key, name, state, graph_version, version, event_seq) \
                     VALUES ($1, $2, $3, 'ACTIVE', 0, 1, 0)",
                    &[
                        command.input.project_id.as_uuid(),
                        &command.input.protocol_key.as_str(),
                        &command.input.name.trim(),
                    ],
                )
                .await
                .map_err(map_database_error)?;
            if changed != 1 {
                return Err(MvpError::Port(agentforge_application::PortError::Integrity));
            }
            let response = ProjectView {
                project_id: command.input.project_id,
                protocol_key: command.input.protocol_key.clone(),
                name: command.input.name.trim().to_owned(),
                version,
            };
            store_receipt(
                &mut uow,
                scope,
                &metadata,
                response.clone(),
                version,
                effect_digest(&response)?,
            )
            .await?;
            Ok(response)
        }
        .await;
        finish(uow, result).await
    }

    async fn publish_package_inner(
        &self,
        command: &MvpCommand<PublishPackageInput>,
    ) -> MvpResult<PublishedPackage> {
        let metadata = command.context.metadata(&command.input)?;
        metadata.require_create()?;
        validate_publish_input(&command.input)?;
        let scope = scope(command.input.project_id, "package.publish", &metadata)?;
        let mut uow = self.factory.begin(Isolation::Serializable).await?;
        let result = async {
            if let Some(response) = replay::<PublishedPackage>(&mut uow, &scope, &metadata).await? {
                return Ok(response);
            }
            let project = uow
                .client()?
                .query_opt(
                    "SELECT graph_version FROM projects WHERE id = $1 AND state = 'ACTIVE' FOR SHARE",
                    &[command.input.project_id.as_uuid()],
                )
                .await
                .map_err(map_database_error)?
                .ok_or(agentforge_application::PortError::NotFound)?;
            let graph_version: i64 = project
                .try_get(0)
                .map_err(|_| agentforge_application::PortError::Integrity)?;
            if u64::try_from(graph_version).ok() != Some(command.input.graph_version) {
                return Err(agentforge_domain::DomainError::StaleVersion.into());
            }

            let package = WorkPackage::new(NewWorkPackage {
                id: command.input.package_id,
                project_id: command.input.project_id,
                selected_revision_id: command.input.revision_id,
                selected_revision: command.input.revision,
                graph_version: command.input.graph_version,
                priority: command.input.priority,
                max_attempts: command.input.max_attempts,
            })?;
            let validation = package.transition(&WorkPackageCommand::RequestValidation {
                expected_version: package.version,
                revision_exists: true,
                package_hash_matches: true,
            })?;
            let published = validation.aggregate.transition(
                &WorkPackageCommand::PublishValidatedPackage {
                    expected_version: validation.aggregate.version,
                    readiness: PublishReadiness {
                        dor_passed: true,
                        dag_valid: true,
                        budget_available: true,
                        permissions_valid: true,
                    },
                },
            )?;

            insert_published_package(&uow, &command.input, &published.aggregate).await?;
            let now = uow.server_now().await?;
            let first = build_event(
                command.input.project_id,
                AggregateId::WorkPackage(command.input.package_id),
                validation.aggregate.version,
                1,
                &metadata,
                now,
                validation.events[0].clone(),
            )?;
            let second = build_event(
                command.input.project_id,
                AggregateId::WorkPackage(command.input.package_id),
                published.aggregate.version,
                2,
                &metadata,
                now,
                published.events[0].clone(),
            )?;
            uow.append_events(std::slice::from_ref(&first.record)).await?;
            uow.append_events(std::slice::from_ref(&second.record)).await?;
            uow.enqueue_outbox(&[
                first.outbox(now, command.input.package_id.to_string()),
                second.outbox(now, command.input.package_id.to_string()),
            ])
            .await?;

            let response = PublishedPackage {
                project_id: command.input.project_id,
                package_id: command.input.package_id,
                package_key: command.input.package_key.clone(),
                revision_id: command.input.revision_id,
                state: published.aggregate.state,
                version: published.aggregate.version,
            };
            let effect = effect_digest(&json!({
                "package_id": response.package_id,
                "revision_id": response.revision_id,
                "event_ids": [first.record.event_id, second.record.event_id],
                "version": response.version,
            }))?;
            store_receipt(
                &mut uow,
                scope,
                &metadata,
                response.clone(),
                response.version,
                effect,
            )
            .await?;
            Ok(response)
        }
        .await;
        finish(uow, result).await
    }

    async fn list_offers_inner(&self, query: ListOffersQuery) -> MvpResult<Vec<OfferView>> {
        if query.limit == 0 || query.limit > 100 {
            return Err(agentforge_domain::DomainError::InvalidArgument {
                field: "limit".into(),
                reason: "must be between 1 and 100".into(),
            }
            .into());
        }
        let uow = self.factory.begin(Isolation::ReadCommitted).await?;
        let result = async {
            let limit = i64::from(query.limit);
            let rows = uow
                .client()?
                .query(
                    "SELECT w.id, w.protocol_key, w.selected_revision_id, r.revision, w.state, \
                            w.priority, w.attempts_started, w.max_attempts, w.version \
                     FROM work_packages w \
                     JOIN package_revisions r ON r.id = w.selected_revision_id \
                     WHERE w.project_id = $1 AND w.state IN ('OFFERED', 'REWORK_READY') \
                     ORDER BY w.priority DESC, w.created_at, w.id LIMIT $2",
                    &[query.project_id.as_uuid(), &limit],
                )
                .await
                .map_err(map_database_error)?;
            rows.into_iter()
                .map(|row| decode_offer(query.project_id, &row))
                .collect()
        }
        .await;
        finish(uow, result).await
    }

    async fn claim_package_inner(
        &self,
        command: &MvpCommand<ClaimPackageInput>,
    ) -> MvpResult<ClaimedWork> {
        let metadata = command.context.metadata(&command.input)?;
        validate_claim_input(&command.input)?;
        let scope = scope(command.input.project_id, "package.claim", &metadata)?;
        let mut uow = self.factory.begin(Isolation::Serializable).await?;
        let result = async {
            if let Some(response) = replay::<ClaimedWork>(&mut uow, &scope, &metadata).await? {
                return Ok(response);
            }

            let loaded =
                load_package_for_update(&uow, command.input.project_id, command.input.package_id)
                    .await?;
            metadata.require_version(loaded.package.version)?;

            let attempt_id = AttemptId::from_uuid(Uuid::now_v7());
            let lease_id = LeaseId::from_uuid(Uuid::now_v7());
            let package_transition =
                loaded.package.transition(&WorkPackageCommand::GrantLease {
                    expected_version: loaded.package.version,
                    attempt_id,
                    lease_id,
                    readiness: ClaimReadiness {
                        dependencies_satisfied: loaded.dependencies_satisfied,
                        budget_available: true,
                        no_active_lease: loaded.package.active_lease_id.is_none(),
                    },
                })?;
            let fencing_token = package_transition
                .aggregate
                .active_fencing_token
                .ok_or(agentforge_application::PortError::Integrity)?;

            let attempt = Attempt::new(NewAttempt {
                id: attempt_id,
                package_id: loaded.package.id,
                revision_id: loaded.package.selected_revision_id,
                executor_id: command.input.executor_id,
                node_id: command.input.node_id,
                fencing_token,
                base_commit: loaded.base_commit.clone(),
            });
            let attempt_transition = attempt.transition(&AttemptCommand::AttachLease {
                expected_version: attempt.version,
                lease_id,
                fencing_token,
                lineage_matches: true,
            })?;

            let now = uow.server_now().await?;
            let expires_at =
                ServerInstant(now.0 + Duration::seconds(i64::from(command.input.lease_seconds)));
            let max_expires_at = ServerInstant(
                now.0 + Duration::seconds(i64::from(command.input.max_lease_seconds)),
            );
            let previous_fencing_token = if loaded.package.last_fencing_token == 0 {
                None
            } else {
                Some(FencingToken::new(loaded.package.last_fencing_token)?)
            };
            let lease_transition = Lease::transition(
                None,
                &LeaseCommand::GrantLease(GrantLease {
                    id: lease_id,
                    package_id: loaded.package.id,
                    revision_id: loaded.package.selected_revision_id,
                    attempt_id,
                    holder_node_id: command.input.node_id,
                    previous_fencing_token,
                    fencing_token,
                    granted_at: now,
                    expires_at,
                    max_expires_at,
                }),
            )?;

            insert_claim_rows(
                &uow,
                &loaded,
                &package_transition.aggregate,
                &attempt_transition.aggregate,
                &lease_transition.aggregate,
            )
            .await?;

            let package_event_seq = loaded
                .event_seq
                .checked_add(1)
                .ok_or(agentforge_application::PortError::Integrity)?;
            let package_event = build_event(
                command.input.project_id,
                AggregateId::WorkPackage(loaded.package.id),
                package_transition.aggregate.version,
                package_event_seq,
                &metadata,
                now,
                package_transition.events[0].clone(),
            )?;
            let attempt_event = build_event(
                command.input.project_id,
                AggregateId::Attempt(attempt_id),
                attempt_transition.aggregate.version,
                1,
                &metadata,
                now,
                attempt_transition.events[0].clone(),
            )?;
            let lease_event = build_event(
                command.input.project_id,
                AggregateId::Lease(lease_id),
                lease_transition.aggregate.version,
                1,
                &metadata,
                now,
                lease_transition.events[0].clone(),
            )?;
            let records = [
                package_event.record.clone(),
                attempt_event.record.clone(),
                lease_event.record.clone(),
            ];
            uow.append_events(&records).await?;
            uow.enqueue_outbox(&[
                package_event.outbox(now, loaded.package.id.to_string()),
                attempt_event.outbox(now, attempt_id.to_string()),
                lease_event.outbox(now, lease_id.to_string()),
            ])
            .await?;

            let response = ClaimedWork {
                project_id: command.input.project_id,
                package_id: loaded.package.id,
                revision_id: loaded.package.selected_revision_id,
                attempt_id,
                lease_id,
                fencing_token,
                expires_at,
                package_version: package_transition.aggregate.version,
                attempt_version: attempt_transition.aggregate.version,
                lease_version: lease_transition.aggregate.version,
            };
            let effect = effect_digest(&json!({
                "package_id": response.package_id,
                "attempt_id": response.attempt_id,
                "lease_id": response.lease_id,
                "event_ids": records.map(|record| record.event_id),
            }))?;
            store_receipt(
                &mut uow,
                scope,
                &metadata,
                response.clone(),
                response.package_version,
                effect,
            )
            .await?;
            Ok(response)
        }
        .await;
        finish(uow, result).await
    }

    async fn renew_lease_inner(
        &self,
        command: &MvpCommand<RenewLeaseInput>,
    ) -> MvpResult<LeaseView> {
        if command.input.extend_by_seconds == 0 || command.input.extend_by_seconds > 86_400 {
            return Err(agentforge_domain::DomainError::InvalidArgument {
                field: "extend_by_seconds".into(),
                reason: "must be between 1 and 86400".into(),
            }
            .into());
        }
        self.mutate_lease(
            command.input.project_id,
            command.input.lease_id,
            "lease.renew",
            command.context.metadata(&command.input)?,
            |lease, now, expected_version| LeaseCommand::RenewLease {
                expected_version,
                holder_node_id: command.input.node_id,
                fencing_token: command.input.fencing_token,
                now,
                new_expires_at: ServerInstant(
                    lease.expires_at.0
                        + Duration::seconds(i64::from(command.input.extend_by_seconds)),
                ),
            },
        )
        .await
    }

    async fn release_lease_inner(
        &self,
        command: &MvpCommand<ReleaseLeaseInput>,
    ) -> MvpResult<LeaseView> {
        let metadata = command.context.metadata(&command.input)?;
        self.terminate_lease_inner(
            command.input.project_id,
            command.input.lease_id,
            "lease.release",
            metadata,
            LeaseTermination::Release {
                holder_node_id: command.input.node_id,
                fencing_token: command.input.fencing_token,
            },
        )
        .await
    }

    async fn terminate_lease_inner(
        &self,
        project_id: ProjectId,
        lease_id: LeaseId,
        command_type: &'static str,
        metadata: CommandMetadata,
        termination: LeaseTermination,
    ) -> MvpResult<LeaseView> {
        let scope = scope(project_id, command_type, &metadata)?;
        let mut uow = self.factory.begin(Isolation::Serializable).await?;
        let result = async {
            if let Some(response) = replay::<LeaseView>(&mut uow, &scope, &metadata).await? {
                return Ok(response);
            }

            // The preliminary read acquires no row lock. Lease bindings are
            // immutable, so it is safe to use them to acquire the canonical
            // Package -> Attempt -> Lease lock order below.
            let preview = load_lease(&uow, project_id, lease_id, false).await?;
            let package =
                load_package_for_update(&uow, project_id, preview.lease.package_id).await?;
            let attempt =
                load_attempt_for_update(&uow, preview.lease.package_id, preview.lease.attempt_id)
                    .await?;
            let loaded = load_lease(&uow, project_id, lease_id, true).await?;
            let now = uow.server_now().await?;
            let lease_transition = Lease::transition(
                Some(&loaded.lease),
                &termination.command(required_expected_version(&metadata)?, now),
            )?;
            validate_active_work_binding(&package.package, &attempt.attempt, &loaded.lease)?;
            let attempt_transition = attempt.attempt.transition(&AttemptCommand::MarkLost {
                expected_version: attempt.attempt.version,
            })?;
            let package_transition =
                package
                    .package
                    .transition(&WorkPackageCommand::LoseAttempt {
                        expected_version: package.package.version,
                        attempt_id: attempt.attempt.id,
                        recoverable: true,
                    })?;

            let package_event_seq = next_event_seq(package.event_seq)?;
            let attempt_event_seq = next_event_seq(attempt.event_seq)?;
            let lease_event_seq = next_event_seq(loaded.event_seq)?;
            update_attempt_after_loss(
                &uow,
                &attempt,
                &attempt_transition.aggregate,
                attempt_event_seq,
            )
            .await?;
            update_lease(&uow, &loaded, &lease_transition.aggregate, lease_event_seq).await?;
            update_package_after_loss(
                &uow,
                &package,
                &package_transition.aggregate,
                package_event_seq,
            )
            .await?;

            let package_event = build_event(
                project_id,
                AggregateId::WorkPackage(package.package.id),
                package_transition.aggregate.version,
                package_event_seq,
                &metadata,
                now,
                package_transition.events[0].clone(),
            )?;
            let attempt_event = build_event(
                project_id,
                AggregateId::Attempt(attempt.attempt.id),
                attempt_transition.aggregate.version,
                attempt_event_seq,
                &metadata,
                now,
                attempt_transition.events[0].clone(),
            )?;
            let lease_event = build_event(
                project_id,
                AggregateId::Lease(loaded.lease.id),
                lease_transition.aggregate.version,
                lease_event_seq,
                &metadata,
                now,
                lease_transition.events[0].clone(),
            )?;
            let records = [
                package_event.record.clone(),
                attempt_event.record.clone(),
                lease_event.record.clone(),
            ];
            uow.append_events(&records).await?;
            uow.enqueue_outbox(&[
                package_event.outbox(now, package.package.id.to_string()),
                attempt_event.outbox(now, attempt.attempt.id.to_string()),
                lease_event.outbox(now, loaded.lease.id.to_string()),
            ])
            .await?;

            let response = lease_view(project_id, &lease_transition.aggregate);
            store_receipt(
                &mut uow,
                scope,
                &metadata,
                response.clone(),
                response.version,
                effect_digest(&json!({
                    "package_id": package.package.id,
                    "attempt_id": attempt.attempt.id,
                    "lease_id": response.lease_id,
                    "event_ids": records.map(|record| record.event_id),
                }))?,
            )
            .await?;
            Ok(response)
        }
        .await;
        finish(uow, result).await
    }

    async fn mutate_lease<F>(
        &self,
        project_id: ProjectId,
        lease_id: LeaseId,
        command_type: &'static str,
        metadata: CommandMetadata,
        decide: F,
    ) -> MvpResult<LeaseView>
    where
        F: FnOnce(&Lease, ServerInstant, AggregateVersion) -> LeaseCommand,
    {
        let scope = scope(project_id, command_type, &metadata)?;
        let mut uow = self.factory.begin(Isolation::Serializable).await?;
        let result = async {
            if let Some(response) = replay::<LeaseView>(&mut uow, &scope, &metadata).await? {
                return Ok(response);
            }
            let loaded = load_lease(&uow, project_id, lease_id, true).await?;
            let expected_version = required_expected_version(&metadata)?;
            let now = uow.server_now().await?;
            let transition = Lease::transition(
                Some(&loaded.lease),
                &decide(&loaded.lease, now, expected_version),
            )?;
            let event_seq = loaded
                .event_seq
                .checked_add(1)
                .ok_or(agentforge_application::PortError::Integrity)?;
            update_lease(&uow, &loaded, &transition.aggregate, event_seq).await?;
            let event = build_event(
                project_id,
                AggregateId::Lease(lease_id),
                transition.aggregate.version,
                event_seq,
                &metadata,
                now,
                transition.events[0].clone(),
            )?;
            uow.append_events(std::slice::from_ref(&event.record))
                .await?;
            uow.enqueue_outbox(&[event.outbox(now, lease_id.to_string())])
                .await?;
            let response = lease_view(project_id, &transition.aggregate);
            store_receipt(
                &mut uow,
                scope,
                &metadata,
                response.clone(),
                response.version,
                effect_digest(&json!({
                    "lease_id": response.lease_id,
                    "event_id": event.record.event_id,
                    "version": response.version,
                }))?,
            )
            .await?;
            Ok(response)
        }
        .await;
        finish(uow, result).await
    }

    async fn get_lease_inner(
        &self,
        project_id: ProjectId,
        lease_id: LeaseId,
    ) -> MvpResult<LeaseView> {
        let uow = self.factory.begin(Isolation::ReadCommitted).await?;
        let result = async {
            let loaded = load_lease(&uow, project_id, lease_id, false).await?;
            Ok(lease_view(project_id, &loaded.lease))
        }
        .await;
        finish(uow, result).await
    }

    async fn reconcile_expired_leases_inner(
        &self,
        query: ReconcileExpiredLeasesQuery,
    ) -> MvpResult<LeaseReconciliationReport> {
        if query.limit == 0 || query.limit > 1_000 {
            return Err(agentforge_domain::DomainError::InvalidArgument {
                field: "limit".into(),
                reason: "must be between 1 and 1000".into(),
            }
            .into());
        }
        let uow = self.factory.begin(Isolation::ReadCommitted).await?;
        let candidates = async {
            let limit = i64::from(query.limit);
            let rows = uow
                .client()?
                .query(
                    "SELECT l.id, l.version \
                     FROM leases l JOIN work_packages w ON w.id = l.package_id \
                     WHERE w.project_id = $1 AND l.state = 'ACTIVE' \
                       AND l.expires_at <= clock_timestamp() \
                     ORDER BY l.expires_at, l.id LIMIT $2",
                    &[query.project_id.as_uuid(), &limit],
                )
                .await
                .map_err(map_database_error)?;
            rows.into_iter()
                .map(|row| {
                    let lease_id = LeaseId::from_uuid(
                        row.try_get(0)
                            .map_err(|_| agentforge_application::PortError::Integrity)?,
                    );
                    let version = version_from_i64(
                        row.try_get(1)
                            .map_err(|_| agentforge_application::PortError::Integrity)?,
                    )?;
                    Ok((lease_id, version))
                })
                .collect::<MvpResult<Vec<_>>>()
        }
        .await;
        let candidates = finish(uow, candidates).await?;
        let scanned = u16::try_from(candidates.len())
            .map_err(|_| agentforge_application::PortError::Integrity)?;
        let mut expired = 0_u16;
        let mut conflicted = 0_u16;
        for (lease_id, expected_version) in candidates {
            let payload = json!({
                "project_id": query.project_id,
                "lease_id": lease_id,
                "expected_version": expected_version,
            });
            let metadata = CommandMetadata {
                command_id: CommandId::from_uuid(Uuid::now_v7()),
                actor_id: expiry_reconciler_actor(),
                idempotency_key: IdempotencyKey::new(format!(
                    "lease-expire:{lease_id}:v{}",
                    expected_version.get()
                ))?,
                correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
                causation_id: None,
                expected_version: Some(expected_version),
                payload_digest: effect_digest(&payload)?,
            };
            match self
                .terminate_lease_inner(
                    query.project_id,
                    lease_id,
                    "lease.expire",
                    metadata,
                    LeaseTermination::Expire,
                )
                .await
            {
                Ok(_) => expired = expired.saturating_add(1),
                Err(error)
                    if matches!(
                        error.code(),
                        "AF_CONFLICT"
                            | "AF_NOT_FOUND"
                            | "AF_STALE_VERSION"
                            | "AF_TRANSITION_INVALID"
                    ) =>
                {
                    conflicted = conflicted.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(LeaseReconciliationReport {
            project_id: query.project_id,
            scanned,
            expired,
            conflicted,
        })
    }
}

impl MvpControlPlane for PostgresMvpControlPlane {
    fn ready(&self) -> MvpFuture<'_, bool> {
        Box::pin(self.ready_inner())
    }

    fn create_project<'a>(
        &'a self,
        command: &'a MvpCommand<CreateProjectInput>,
    ) -> MvpFuture<'a, ProjectView> {
        Box::pin(self.create_project_inner(command))
    }

    fn publish_package<'a>(
        &'a self,
        command: &'a MvpCommand<PublishPackageInput>,
    ) -> MvpFuture<'a, PublishedPackage> {
        Box::pin(self.publish_package_inner(command))
    }

    fn list_offers(&self, query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>> {
        Box::pin(self.list_offers_inner(query))
    }

    fn claim_package<'a>(
        &'a self,
        command: &'a MvpCommand<ClaimPackageInput>,
    ) -> MvpFuture<'a, ClaimedWork> {
        Box::pin(self.claim_package_inner(command))
    }

    fn renew_lease<'a>(
        &'a self,
        command: &'a MvpCommand<RenewLeaseInput>,
    ) -> MvpFuture<'a, LeaseView> {
        Box::pin(self.renew_lease_inner(command))
    }

    fn release_lease<'a>(
        &'a self,
        command: &'a MvpCommand<ReleaseLeaseInput>,
    ) -> MvpFuture<'a, LeaseView> {
        Box::pin(self.release_lease_inner(command))
    }

    fn get_lease(&self, project_id: ProjectId, lease_id: LeaseId) -> MvpFuture<'_, LeaseView> {
        Box::pin(self.get_lease_inner(project_id, lease_id))
    }

    fn reconcile_expired_leases(
        &self,
        query: ReconcileExpiredLeasesQuery,
    ) -> MvpFuture<'_, LeaseReconciliationReport> {
        Box::pin(self.reconcile_expired_leases_inner(query))
    }
}

async fn finish<T>(uow: PostgresUnitOfWork, result: MvpResult<T>) -> MvpResult<T> {
    match result {
        Ok(value) => {
            uow.commit().await?;
            Ok(value)
        }
        Err(error) => {
            let _ = uow.rollback().await;
            Err(error)
        }
    }
}

fn scope(
    project_id: ProjectId,
    command_type: &str,
    metadata: &CommandMetadata,
) -> MvpResult<IdempotencyScope> {
    Ok(IdempotencyScope::new(
        project_id,
        command_type,
        metadata.actor_id,
        metadata.idempotency_key.clone(),
    )?)
}

async fn replay<R>(
    uow: &mut PostgresUnitOfWork,
    scope: &IdempotencyScope,
    metadata: &CommandMetadata,
) -> MvpResult<Option<R>>
where
    R: serde::de::DeserializeOwned,
{
    let lookup = match uow.find_command_receipt(scope, metadata).await {
        Err(agentforge_application::PortError::Conflict) => {
            return Err(agentforge_domain::DomainError::IdempotencyKeyReused.into());
        }
        Err(error) => return Err(error.into()),
        Ok(lookup) => lookup,
    };
    match lookup {
        CommandReceiptLookup::Missing => Ok(None),
        CommandReceiptLookup::Replay(receipt) => {
            match agentforge_domain::check_receipt(Some(&receipt), scope, metadata)? {
                ReceiptDecision::Replay(_) => {}
                ReceiptDecision::Execute => {
                    return Err(agentforge_application::PortError::Integrity.into());
                }
            }
            Ok(Some(receipt.response))
        }
        CommandReceiptLookup::Expired { .. } => Err(MvpError::IdempotencyResultExpired),
        CommandReceiptLookup::Legacy { .. } => Err(MvpError::IdempotencyResultLegacy),
    }
}

async fn store_receipt<R>(
    uow: &mut PostgresUnitOfWork,
    scope: IdempotencyScope,
    metadata: &CommandMetadata,
    response: R,
    resource_version: AggregateVersion,
    effect_digest: Sha256Digest,
) -> MvpResult<()>
where
    R: Serialize,
{
    let receipt = CommandReceipt {
        scope,
        command_id: metadata.command_id,
        payload_digest: metadata.payload_digest,
        response,
        resource_version,
    };
    let replay_until =
        ServerInstant(uow.server_now().await?.0 + Duration::hours(RECEIPT_REPLAY_HOURS));
    uow.store_command_receipt(&receipt, replay_until, effect_digest)
        .await?;
    Ok(())
}

fn validate_project_input(input: &CreateProjectInput) -> MvpResult<()> {
    if input.project_id.as_uuid().is_nil() || input.name.trim().is_empty() || input.name.len() > 200
    {
        return Err(agentforge_domain::DomainError::InvalidArgument {
            field: "project".into(),
            reason: "id must be non-nil and name must contain 1-200 characters".into(),
        }
        .into());
    }
    Ok(())
}

fn validate_publish_input(input: &PublishPackageInput) -> MvpResult<()> {
    if input.project_id.as_uuid().is_nil()
        || input.package_id.as_uuid().is_nil()
        || input.revision_id.as_uuid().is_nil()
        || input.created_by.as_uuid().is_nil()
        || input.schema_version.trim().is_empty()
        || !matches!(input.git_object_format.as_str(), "sha1" | "sha256")
        || input.priority < 0
        || input.priority > 100
        || input.max_attempts == 0
        || input.max_attempts > 100
        || !input.canonical_document.is_object()
        || !input.input_snapshot.is_object()
        || input.package_hash.as_bytes().iter().all(|byte| *byte == 0)
    {
        return Err(agentforge_domain::DomainError::InvalidArgument {
            field: "package".into(),
            reason: "publish package input failed the MVP shape contract".into(),
        }
        .into());
    }
    let expected_format = if input.base_commit.as_str().len() == 40 {
        "sha1"
    } else {
        "sha256"
    };
    if input.git_object_format != expected_format {
        return Err(agentforge_domain::DomainError::InvalidArgument {
            field: "git_object_format".into(),
            reason: "must match the base commit object id".into(),
        }
        .into());
    }
    let canonical_bytes = serde_json_canonicalizer::to_vec(&input.canonical_document)
        .map_err(|_| agentforge_application::PortError::Serialization)?;
    if Sha256Digest::of_bytes(canonical_bytes) != input.package_hash {
        return Err(agentforge_domain::DomainError::PackageHashMismatch.into());
    }
    Ok(())
}

fn validate_claim_input(input: &ClaimPackageInput) -> MvpResult<()> {
    if input.project_id.as_uuid().is_nil()
        || input.package_id.as_uuid().is_nil()
        || input.executor_id.as_uuid().is_nil()
        || input.node_id.as_uuid().is_nil()
        || !(5..=3_600).contains(&input.lease_seconds)
        || input.max_lease_seconds < input.lease_seconds
        || input.max_lease_seconds > 86_400
    {
        return Err(agentforge_domain::DomainError::InvalidArgument {
            field: "claim".into(),
            reason: "ids must be non-nil and lease windows must satisfy 5s <= lease <= max <= 24h"
                .into(),
        }
        .into());
    }
    Ok(())
}

struct LoadedPackage {
    package: WorkPackage,
    base_commit: GitObjectId,
    event_seq: u64,
    dependencies_satisfied: bool,
}

struct LoadedAttempt {
    attempt: Attempt,
    event_seq: u64,
}

async fn load_attempt_for_update(
    uow: &PostgresUnitOfWork,
    package_id: agentforge_domain::PackageId,
    attempt_id: AttemptId,
) -> MvpResult<LoadedAttempt> {
    let row = uow
        .client()?
        .query_opt(
            "SELECT revision_id, executor_id, node_id, state, lease_id, fencing_token, \
                    base_commit, wake_condition, resume_state, semantic_progress_seq, \
                    last_checkpoint_digest, candidate_commit, version, event_seq \
             FROM attempts WHERE id = $1 AND package_id = $2 FOR UPDATE",
            &[attempt_id.as_uuid(), package_id.as_uuid()],
        )
        .await
        .map_err(map_database_error)?
        .ok_or(agentforge_application::PortError::NotFound)?;
    let wake_condition = row
        .try_get::<_, Option<Json<Value>>>(7)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(|Json(value)| {
            serde_json::from_value::<WakeCondition>(value)
                .map_err(|_| agentforge_application::PortError::Integrity)
        })
        .transpose()?;
    let resume_state = row
        .try_get::<_, Option<String>>(8)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(|state| parse_attempt_state(&state))
        .transpose()?;
    let checkpoint = row
        .try_get::<_, Option<Vec<u8>>>(10)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(|bytes| {
            let bytes: [u8; 32] = bytes
                .try_into()
                .map_err(|_| agentforge_application::PortError::Integrity)?;
            Ok::<Sha256Digest, agentforge_application::PortError>(Sha256Digest::from_bytes(bytes))
        })
        .transpose()?;
    let candidate_commit = row
        .try_get::<_, Option<String>>(11)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(GitObjectId::new)
        .transpose()?;
    let version = version_from_i64(
        row.try_get(12)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let event_seq = u64::try_from(
        row.try_get::<_, i64>(13)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    Ok(LoadedAttempt {
        attempt: Attempt {
            id: attempt_id,
            package_id,
            revision_id: PackageRevisionId::from_uuid(
                row.try_get(0)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            executor_id: agentforge_domain::ExecutorId::from_uuid(
                row.try_get(1)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            node_id: agentforge_domain::NodeId::from_uuid(
                row.try_get(2)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            state: parse_attempt_state(
                &row.try_get::<_, String>(3)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            )?,
            lease_id: row
                .try_get::<_, Option<Uuid>>(4)
                .map_err(|_| agentforge_application::PortError::Integrity)?
                .map(LeaseId::from_uuid),
            fencing_token: FencingToken::new(
                u64::try_from(
                    row.try_get::<_, i64>(5)
                        .map_err(|_| agentforge_application::PortError::Integrity)?,
                )
                .map_err(|_| agentforge_application::PortError::Integrity)?,
            )?,
            base_commit: GitObjectId::new(
                row.try_get::<_, String>(6)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            )?,
            wake_condition,
            resume_state,
            semantic_progress_seq: u64::try_from(
                row.try_get::<_, i64>(9)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            )
            .map_err(|_| agentforge_application::PortError::Integrity)?,
            last_checkpoint_digest: checkpoint,
            candidate_commit,
            submission_id: None,
            submission_outcome: None,
            version,
        },
        event_seq,
    })
}

async fn load_package_for_update(
    uow: &PostgresUnitOfWork,
    project_id: ProjectId,
    package_id: agentforge_domain::PackageId,
) -> MvpResult<LoadedPackage> {
    let row = uow
        .client()?
        .query_opt(
            "SELECT w.selected_revision_id, r.revision, w.state, p.graph_version, \
                    w.priority, w.max_attempts, w.attempts_started, w.next_fencing_token, \
                    w.active_attempt_id, a.lease_id, a.fencing_token, w.accepted_submission_id, \
                    w.integrated_integration_id, w.integrated_commit, w.version, w.event_seq, \
                    r.base_commit, \
                    NOT EXISTS ( \
                        SELECT 1 FROM package_edges e \
                        JOIN work_packages dependency ON dependency.id = e.from_package_id \
                        WHERE e.project_id = w.project_id \
                          AND e.graph_version = p.graph_version \
                          AND e.to_package_id = w.id \
                          AND e.kind IN ('HARD_DEPENDENCY', 'ARTIFACT_DEPENDENCY', 'GATE', \
                                         'INTEGRATION_AFTER') \
                          AND dependency.state NOT IN ('INTEGRATED', 'CLOSED') \
                    ) AS dependencies_satisfied \
             FROM work_packages w \
             JOIN projects p ON p.id = w.project_id \
             JOIN package_revisions r ON r.id = w.selected_revision_id AND r.package_id = w.id \
             LEFT JOIN attempts a ON a.id = w.active_attempt_id AND a.package_id = w.id \
             WHERE w.id = $1 AND w.project_id = $2 \
             FOR UPDATE OF w",
            &[package_id.as_uuid(), project_id.as_uuid()],
        )
        .await
        .map_err(map_database_error)?
        .ok_or(agentforge_application::PortError::NotFound)?;

    let revision_id = PackageRevisionId::from_uuid(
        row.try_get(0)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    );
    let revision = PackageRevision::new(
        u32::try_from(
            row.try_get::<_, i32>(1)
                .map_err(|_| agentforge_application::PortError::Integrity)?,
        )
        .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let state = parse_package_state(
        &row.try_get::<_, String>(2)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let graph_version = u64::try_from(
        row.try_get::<_, i64>(3)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    let max_attempts = u16::try_from(
        row.try_get::<_, i32>(5)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    let attempts_started = u16::try_from(
        row.try_get::<_, i32>(6)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    let last_fencing_token = u64::try_from(
        row.try_get::<_, i64>(7)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    let active_attempt_id = row
        .try_get::<_, Option<Uuid>>(8)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(AttemptId::from_uuid);
    let active_lease_id = row
        .try_get::<_, Option<Uuid>>(9)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(LeaseId::from_uuid);
    let active_fencing_token_value = row
        .try_get::<_, Option<i64>>(10)
        .map_err(|_| agentforge_application::PortError::Integrity)?;
    let active_fencing_token = match active_fencing_token_value {
        Some(value) => {
            Some(FencingToken::new(u64::try_from(value).map_err(|_| {
                agentforge_application::PortError::Integrity
            })?)?)
        }
        None => None,
    };
    let accepted_submission_id = row
        .try_get::<_, Option<Uuid>>(11)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(agentforge_domain::SubmissionId::from_uuid);
    let integrated_integration_id = row
        .try_get::<_, Option<Uuid>>(12)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(agentforge_domain::IntegrationId::from_uuid);
    let integrated_commit = row
        .try_get::<_, Option<String>>(13)
        .map_err(|_| agentforge_application::PortError::Integrity)?
        .map(GitObjectId::new)
        .transpose()?;
    let version = version_from_i64(
        row.try_get(14)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let event_seq = u64::try_from(
        row.try_get::<_, i64>(15)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    let base_commit = GitObjectId::new(
        row.try_get::<_, String>(16)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let dependencies_satisfied = row
        .try_get(17)
        .map_err(|_| agentforge_application::PortError::Integrity)?;
    Ok(LoadedPackage {
        package: WorkPackage {
            id: package_id,
            project_id,
            selected_revision_id: revision_id,
            selected_revision: revision,
            state,
            graph_version,
            priority: row
                .try_get(4)
                .map_err(|_| agentforge_application::PortError::Integrity)?,
            max_attempts,
            attempts_started,
            last_fencing_token,
            active_attempt_id,
            active_lease_id,
            active_fencing_token,
            accepted_submission_id,
            current_integration_id: None,
            integrated_integration_id,
            integrated_commit,
            version,
        },
        base_commit,
        event_seq,
        dependencies_satisfied,
    })
}

async fn insert_claim_rows(
    uow: &PostgresUnitOfWork,
    loaded: &LoadedPackage,
    package: &WorkPackage,
    attempt: &Attempt,
    lease: &Lease,
) -> MvpResult<()> {
    let attempt_version = version_to_i64(attempt.version)?;
    let fencing_token = u64_to_i64(lease.fencing_token.get())?;
    let attempt_inserted = uow
        .client()?
        .execute(
            "INSERT INTO attempts \
             (id, protocol_key, package_id, revision_id, executor_id, node_id, state, lease_id, \
              fencing_token, base_commit, version, event_seq) \
             VALUES ($1, $2, $3, $4, $5, $6, 'LEASED', $7, $8, $9, $10, 1)",
            &[
                attempt.id.as_uuid(),
                &format!("attempt-{}", attempt.id),
                attempt.package_id.as_uuid(),
                attempt.revision_id.as_uuid(),
                attempt.executor_id.as_uuid(),
                attempt.node_id.as_uuid(),
                lease.id.as_uuid(),
                &fencing_token,
                &attempt.base_commit.as_str(),
                &attempt_version,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if attempt_inserted != 1 {
        return Err(agentforge_application::PortError::Integrity.into());
    }

    let lease_version = version_to_i64(lease.version)?;
    let lease_inserted = uow
        .client()?
        .execute(
            "INSERT INTO leases \
             (id, protocol_key, package_id, revision_id, attempt_id, holder_node_id, \
              fencing_token, state, granted_at, expires_at, max_expires_at, version, event_seq) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'ACTIVE', $8, $9, $10, $11, 1)",
            &[
                lease.id.as_uuid(),
                &format!("lease-{}", lease.id),
                lease.package_id.as_uuid(),
                lease.revision_id.as_uuid(),
                lease.attempt_id.as_uuid(),
                lease.holder_node_id.as_uuid(),
                &fencing_token,
                &lease.granted_at.0,
                &lease.expires_at.0,
                &lease.max_expires_at.0,
                &lease_version,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if lease_inserted != 1 {
        return Err(agentforge_application::PortError::Integrity.into());
    }

    let package_version = version_to_i64(package.version)?;
    let previous_version = version_to_i64(loaded.package.version)?;
    let previous_event_seq = u64_to_i64(loaded.event_seq)?;
    let next_event_seq_value = loaded
        .event_seq
        .checked_add(1)
        .ok_or(agentforge_application::PortError::Integrity)?;
    let next_event_seq = u64_to_i64(next_event_seq_value)?;
    let attempts_started = i32::from(package.attempts_started);
    let package_updated = uow
        .client()?
        .execute(
            "UPDATE work_packages \
             SET state = 'ACTIVE', attempts_started = $3, next_fencing_token = $4, \
                 active_attempt_id = $5, version = $6, event_seq = $7, \
                 updated_at = clock_timestamp() \
             WHERE id = $1 AND project_id = $2 AND version = $8 AND event_seq = $9 \
               AND state IN ('OFFERED', 'REWORK_READY') AND active_attempt_id IS NULL",
            &[
                package.id.as_uuid(),
                package.project_id.as_uuid(),
                &attempts_started,
                &fencing_token,
                attempt.id.as_uuid(),
                &package_version,
                &next_event_seq,
                &previous_version,
                &previous_event_seq,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if package_updated != 1 {
        return Err(agentforge_application::PortError::Conflict.into());
    }
    Ok(())
}

fn validate_active_work_binding(
    package: &WorkPackage,
    attempt: &Attempt,
    lease: &Lease,
) -> MvpResult<()> {
    if package.state != WorkPackageState::Active
        || package.active_attempt_id != Some(attempt.id)
        || package.active_lease_id != Some(lease.id)
        || package.active_fencing_token != Some(lease.fencing_token)
        || attempt.package_id != package.id
        || attempt.revision_id != package.selected_revision_id
        || attempt.lease_id != Some(lease.id)
        || attempt.fencing_token != lease.fencing_token
        || lease.package_id != package.id
        || lease.revision_id != package.selected_revision_id
        || lease.attempt_id != attempt.id
    {
        return Err(agentforge_domain::DomainError::StaleLease.into());
    }
    Ok(())
}

async fn update_attempt_after_loss(
    uow: &PostgresUnitOfWork,
    loaded: &LoadedAttempt,
    attempt: &Attempt,
    event_seq: u64,
) -> MvpResult<()> {
    let version = version_to_i64(attempt.version)?;
    let previous_version = version_to_i64(loaded.attempt.version)?;
    let event_seq = u64_to_i64(event_seq)?;
    let previous_event_seq = u64_to_i64(loaded.event_seq)?;
    let changed = uow
        .client()?
        .execute(
            "UPDATE attempts \
             SET state = 'LOST', wake_condition = NULL, resume_state = NULL, \
                 version = $3, event_seq = $4, updated_at = clock_timestamp() \
             WHERE id = $1 AND package_id = $2 AND version = $5 AND event_seq = $6 \
               AND state NOT IN ('CANDIDATE', 'ISOLATED_REVIEW', 'CLEAN_REPRODUCE', \
                                 'SUBMITTED', 'PASSED', 'REJECTED', 'LOST', 'FAILED', \
                                 'CANCELLED')",
            &[
                attempt.id.as_uuid(),
                attempt.package_id.as_uuid(),
                &version,
                &event_seq,
                &previous_version,
                &previous_event_seq,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if changed != 1 {
        return Err(agentforge_application::PortError::Conflict.into());
    }
    Ok(())
}

async fn update_package_after_loss(
    uow: &PostgresUnitOfWork,
    loaded: &LoadedPackage,
    package: &WorkPackage,
    event_seq: u64,
) -> MvpResult<()> {
    let version = version_to_i64(package.version)?;
    let previous_version = version_to_i64(loaded.package.version)?;
    let event_seq = u64_to_i64(event_seq)?;
    let previous_event_seq = u64_to_i64(loaded.event_seq)?;
    let state = package.state.as_str().to_ascii_uppercase();
    let active_attempt = loaded
        .package
        .active_attempt_id
        .ok_or(agentforge_application::PortError::Integrity)?;
    let changed = uow
        .client()?
        .execute(
            "UPDATE work_packages \
             SET state = $3, active_attempt_id = NULL, version = $4, event_seq = $5, \
                 updated_at = clock_timestamp() \
             WHERE id = $1 AND project_id = $2 AND state = 'ACTIVE' \
               AND active_attempt_id = $6 AND version = $7 AND event_seq = $8",
            &[
                package.id.as_uuid(),
                package.project_id.as_uuid(),
                &state,
                &version,
                &event_seq,
                active_attempt.as_uuid(),
                &previous_version,
                &previous_event_seq,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if changed != 1 {
        return Err(agentforge_application::PortError::Conflict.into());
    }
    Ok(())
}

struct LoadedLease {
    lease: Lease,
    event_seq: u64,
}

async fn load_lease(
    uow: &PostgresUnitOfWork,
    project_id: ProjectId,
    lease_id: LeaseId,
    for_update: bool,
) -> MvpResult<LoadedLease> {
    let lock = if for_update { " FOR UPDATE OF l" } else { "" };
    let sql = format!(
        "SELECT l.package_id, l.revision_id, l.attempt_id, l.holder_node_id, \
                l.fencing_token, l.state, l.granted_at, l.expires_at, l.max_expires_at, \
                l.version, l.event_seq \
         FROM leases l JOIN work_packages w ON w.id = l.package_id \
         WHERE l.id = $1 AND w.project_id = $2{lock}"
    );
    let row = uow
        .client()?
        .query_opt(&sql, &[lease_id.as_uuid(), project_id.as_uuid()])
        .await
        .map_err(map_database_error)?
        .ok_or(agentforge_application::PortError::NotFound)?;
    let fencing_token = FencingToken::new(
        u64::try_from(
            row.try_get::<_, i64>(4)
                .map_err(|_| agentforge_application::PortError::Integrity)?,
        )
        .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let state = parse_lease_state(
        &row.try_get::<_, String>(5)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let version = version_from_i64(
        row.try_get(9)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let event_seq = u64::try_from(
        row.try_get::<_, i64>(10)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    Ok(LoadedLease {
        lease: Lease {
            id: lease_id,
            package_id: agentforge_domain::PackageId::from_uuid(
                row.try_get(0)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            revision_id: PackageRevisionId::from_uuid(
                row.try_get(1)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            attempt_id: AttemptId::from_uuid(
                row.try_get(2)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            holder_node_id: agentforge_domain::NodeId::from_uuid(
                row.try_get(3)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            fencing_token,
            state,
            granted_at: ServerInstant(
                row.try_get(6)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            expires_at: ServerInstant(
                row.try_get(7)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            max_expires_at: ServerInstant(
                row.try_get(8)
                    .map_err(|_| agentforge_application::PortError::Integrity)?,
            ),
            version,
        },
        event_seq,
    })
}

async fn update_lease(
    uow: &PostgresUnitOfWork,
    loaded: &LoadedLease,
    lease: &Lease,
    event_seq: u64,
) -> MvpResult<()> {
    let version = version_to_i64(lease.version)?;
    let previous_version = version_to_i64(loaded.lease.version)?;
    let event_seq = u64_to_i64(event_seq)?;
    let previous_event_seq = u64_to_i64(loaded.event_seq)?;
    let fencing_token = u64_to_i64(lease.fencing_token.get())?;
    let state = lease.state.as_str().to_ascii_uppercase();
    let changed = uow
        .client()?
        .execute(
            "UPDATE leases \
             SET state = $2, expires_at = $3, version = $4, event_seq = $5, \
                 updated_at = clock_timestamp() \
             WHERE id = $1 AND version = $6 AND event_seq = $7 AND state = 'ACTIVE' \
               AND holder_node_id = $8 AND fencing_token = $9",
            &[
                lease.id.as_uuid(),
                &state,
                &lease.expires_at.0,
                &version,
                &event_seq,
                &previous_version,
                &previous_event_seq,
                lease.holder_node_id.as_uuid(),
                &fencing_token,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if changed != 1 {
        return Err(agentforge_application::PortError::Conflict.into());
    }
    Ok(())
}

fn lease_view(project_id: ProjectId, lease: &Lease) -> LeaseView {
    LeaseView {
        project_id,
        package_id: lease.package_id,
        revision_id: lease.revision_id,
        attempt_id: lease.attempt_id,
        lease_id: lease.id,
        holder_node_id: lease.holder_node_id,
        fencing_token: lease.fencing_token,
        state: lease.state,
        granted_at: lease.granted_at,
        expires_at: lease.expires_at,
        max_expires_at: lease.max_expires_at,
        version: lease.version,
    }
}

fn parse_lease_state(value: &str) -> MvpResult<LeaseState> {
    match value {
        "ACTIVE" => Ok(LeaseState::Active),
        "RELEASED" => Ok(LeaseState::Released),
        "REVOKED" => Ok(LeaseState::Revoked),
        "EXPIRED" => Ok(LeaseState::Expired),
        _ => Err(agentforge_application::PortError::Integrity.into()),
    }
}

fn parse_attempt_state(value: &str) -> MvpResult<AttemptState> {
    let state = match value {
        "CREATED" => AttemptState::Created,
        "LEASED" => AttemptState::Leased,
        "PREPARING" => AttemptState::Preparing,
        "PLANNING" => AttemptState::Planning,
        "IMPLEMENTING" => AttemptState::Implementing,
        "LOCAL_VERIFY" => AttemptState::LocalVerify,
        "WAITING_INPUT" => AttemptState::WaitingInput,
        "CANDIDATE" => AttemptState::Candidate,
        "ISOLATED_REVIEW" => AttemptState::IsolatedReview,
        "CLEAN_REPRODUCE" => AttemptState::CleanReproduce,
        "SUBMITTED" => AttemptState::Submitted,
        "PASSED" => AttemptState::Passed,
        "REJECTED" => AttemptState::Rejected,
        "LOST" => AttemptState::Lost,
        "FAILED" => AttemptState::Failed,
        "CANCELLED" => AttemptState::Cancelled,
        _ => return Err(agentforge_application::PortError::Integrity.into()),
    };
    Ok(state)
}

fn required_expected_version(metadata: &CommandMetadata) -> MvpResult<AggregateVersion> {
    metadata
        .expected_version
        .ok_or_else(|| agentforge_domain::DomainError::InvalidArgument {
            field: "expected_version".into(),
            reason: "is required for aggregate updates".into(),
        })
        .map_err(MvpError::from)
}

fn next_event_seq(current: u64) -> MvpResult<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| agentforge_application::PortError::Integrity.into())
}

fn expiry_reconciler_actor() -> ActorId {
    // UUIDv8-shaped stable service identity. It is not a credential; the
    // control-plane composition authorizes this internal scheduler capability.
    let mut bytes = *b"AF-LEASE-EXPIRY!";
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    ActorId::from_uuid(Uuid::from_bytes(bytes))
}

async fn insert_published_package(
    uow: &PostgresUnitOfWork,
    input: &PublishPackageInput,
    package: &WorkPackage,
) -> MvpResult<()> {
    let version = i64::try_from(package.version.get())
        .map_err(|_| agentforge_application::PortError::Integrity)?;
    let event_seq = version;
    let max_attempts = i32::from(package.max_attempts);
    let revision = i32::try_from(input.revision.get())
        .map_err(|_| agentforge_application::PortError::Integrity)?;
    let inserted = uow
        .client()?
        .execute(
            "INSERT INTO work_packages \
             (id, project_id, protocol_key, selected_revision_id, state, priority, max_attempts, \
              attempts_started, next_fencing_token, version, event_seq) \
             VALUES ($1, $2, $3, NULL, 'OFFERED', $4, $5, 0, 0, $6, $7)",
            &[
                input.package_id.as_uuid(),
                input.project_id.as_uuid(),
                &input.package_key.as_str(),
                &package.priority,
                &max_attempts,
                &version,
                &event_seq,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if inserted != 1 {
        return Err(agentforge_application::PortError::Integrity.into());
    }
    uow.client()?
        .execute(
            "INSERT INTO package_revisions \
             (id, package_id, revision, schema_version, canonical_document, package_hash, \
              base_commit, git_object_format, input_snapshot, created_by) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &[
                input.revision_id.as_uuid(),
                input.package_id.as_uuid(),
                &revision,
                &input.schema_version,
                &Json(&input.canonical_document),
                &&input.package_hash.as_bytes()[..],
                &input.base_commit.as_str(),
                &input.git_object_format,
                &Json(&input.input_snapshot),
                input.created_by.as_uuid(),
            ],
        )
        .await
        .map_err(map_database_error)?;
    let selected = uow
        .client()?
        .execute(
            "UPDATE work_packages SET selected_revision_id = $2 WHERE id = $1 AND version = $3",
            &[
                input.package_id.as_uuid(),
                input.revision_id.as_uuid(),
                &version,
            ],
        )
        .await
        .map_err(map_database_error)?;
    if selected != 1 {
        return Err(agentforge_application::PortError::Conflict.into());
    }
    Ok(())
}

fn decode_offer(project_id: ProjectId, row: &Row) -> MvpResult<OfferView> {
    let package_id = agentforge_domain::PackageId::from_uuid(
        row.try_get(0)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    );
    let package_key = ProtocolKey::new(
        row.try_get::<_, String>(1)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let revision_id = agentforge_domain::PackageRevisionId::from_uuid(
        row.try_get(2)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    );
    let revision = PackageRevision::new(
        u32::try_from(
            row.try_get::<_, i32>(3)
                .map_err(|_| agentforge_application::PortError::Integrity)?,
        )
        .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let state = parse_package_state(
        &row.try_get::<_, String>(4)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    let priority = row
        .try_get(5)
        .map_err(|_| agentforge_application::PortError::Integrity)?;
    let attempts_started = u16::try_from(
        row.try_get::<_, i32>(6)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    let max_attempts = u16::try_from(
        row.try_get::<_, i32>(7)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )
    .map_err(|_| agentforge_application::PortError::Integrity)?;
    let version = version_from_i64(
        row.try_get(8)
            .map_err(|_| agentforge_application::PortError::Integrity)?,
    )?;
    Ok(OfferView {
        project_id,
        package_id,
        package_key,
        revision_id,
        revision,
        state,
        priority,
        attempts_started,
        max_attempts,
        version,
    })
}

fn parse_package_state(value: &str) -> MvpResult<WorkPackageState> {
    let state = match value {
        "DRAFT" => WorkPackageState::Draft,
        "VALIDATING" => WorkPackageState::Validating,
        "BLOCKED" => WorkPackageState::Blocked,
        "OFFERED" => WorkPackageState::Offered,
        "ACTIVE" => WorkPackageState::Active,
        "VERIFYING" => WorkPackageState::Verifying,
        "REWORK_READY" => WorkPackageState::ReworkReady,
        "ACCEPTED" => WorkPackageState::Accepted,
        "INTEGRATING" => WorkPackageState::Integrating,
        "REBASE_REQUIRED" => WorkPackageState::RebaseRequired,
        "INTEGRATED" => WorkPackageState::Integrated,
        "CLOSED" => WorkPackageState::Closed,
        "CANCELLED" => WorkPackageState::Cancelled,
        "SUPERSEDED" => WorkPackageState::Superseded,
        "FAILED" => WorkPackageState::Failed,
        _ => return Err(agentforge_application::PortError::Integrity.into()),
    };
    Ok(state)
}

struct BuiltEvent {
    record: EventRecord,
    envelope: Value,
}

impl BuiltEvent {
    fn outbox(&self, available_at: ServerInstant, aggregate_key: String) -> OutboxMessage {
        OutboxMessage {
            id: OutboxMessageId::from_uuid(Uuid::now_v7()),
            project_id: self.record.project_id,
            event_id: self.record.event_id,
            topic: DOMAIN_EVENT_TOPIC.to_owned(),
            message_key: aggregate_key,
            envelope: self.envelope.clone(),
            available_at,
        }
    }
}

fn build_event<E>(
    project_id: ProjectId,
    aggregate_id: AggregateId,
    aggregate_version: AggregateVersion,
    aggregate_seq: u64,
    metadata: &CommandMetadata,
    occurred_at: ServerInstant,
    payload: E,
) -> MvpResult<BuiltEvent>
where
    E: Serialize,
{
    let event_type = match serde_json::to_value(&payload)
        .map_err(|_| agentforge_application::PortError::Serialization)?
    {
        Value::String(name) => name,
        Value::Object(ref object) if object.len() == 1 => object
            .keys()
            .next()
            .cloned()
            .ok_or(agentforge_application::PortError::Serialization)?,
        _ => std::any::type_name::<E>()
            .rsplit("::")
            .next()
            .unwrap_or("domain_event")
            .to_owned(),
    };
    let envelope = DomainEventEnvelope::new(
        EventContext {
            event_id: EventId::from_uuid(Uuid::now_v7()),
            aggregate_id,
            aggregate_version,
            aggregate_seq,
            event_type,
            schema_version: EVENT_SCHEMA_VERSION,
            actor_id: metadata.actor_id,
            correlation_id: metadata.correlation_id,
            causation_id: metadata.causation_id,
            occurred_at,
        },
        payload,
    )?;
    let value = serde_json::to_value(&envelope)
        .map_err(|_| agentforge_application::PortError::Serialization)?;
    let record = EventRecord::from_domain(project_id, &envelope)?;
    Ok(BuiltEvent {
        record,
        envelope: value,
    })
}

fn effect_digest<T: Serialize>(value: &T) -> MvpResult<Sha256Digest> {
    let bytes = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| agentforge_application::PortError::Serialization)?;
    Ok(Sha256Digest::of_bytes(bytes))
}

fn version_from_i64(value: i64) -> MvpResult<AggregateVersion> {
    let value = u64::try_from(value).map_err(|_| agentforge_application::PortError::Integrity)?;
    if value == 0 {
        return Err(agentforge_application::PortError::Integrity.into());
    }
    Ok(AggregateVersion::new(value))
}

fn version_to_i64(value: AggregateVersion) -> MvpResult<i64> {
    u64_to_i64(value.get())
}

fn u64_to_i64(value: u64) -> MvpResult<i64> {
    i64::try_from(value).map_err(|_| agentforge_application::PortError::Integrity.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn package_state_decoder_is_exhaustive_for_the_database_contract() {
        for state in [
            "DRAFT",
            "VALIDATING",
            "BLOCKED",
            "OFFERED",
            "ACTIVE",
            "VERIFYING",
            "REWORK_READY",
            "ACCEPTED",
            "INTEGRATING",
            "REBASE_REQUIRED",
            "INTEGRATED",
            "CLOSED",
            "CANCELLED",
            "SUPERSEDED",
            "FAILED",
        ] {
            parse_package_state(state).expect("known database package state");
        }
        assert!(parse_package_state("UNKNOWN").is_err());
    }
}
