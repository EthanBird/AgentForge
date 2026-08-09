//! Transaction-bound PostgreSQL implementation of the Phase 1 application ports.
//!
//! This module deliberately does not implement the generic aggregate repository
//! port. AgentForge's typed PostgreSQL tables remain the canonical state store;
//! typed repository adapters arrive in Phase 2. Phase 1 owns only the transaction
//! lifecycle and the event/receipt/outbox/inbox facts that can be made complete
//! without bypassing those relational invariants.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::Duration,
};

use agentforge_application::{
    AppendEventsReceipt, EventAppendPort, EventRecord, Isolation, PortError, PortFuture,
    PortResult, ProjectionCursor, UnitOfWork, UnitOfWorkFactory,
};
use agentforge_domain::{
    AggregateId, AggregateType, AggregateVersion, CommandMetadata, CommandReceipt, EventId,
    IdempotencyScope, ProjectId, ServerInstant, Sha256Digest, event::EVENT_ENVELOPE_VERSION,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, Row, config::Host, error::SqlState, types::Json};
use uuid::Uuid;

/// PostgreSQL UoW factory. Phase 1 opens one database connection per UoW so
/// the transaction can be owned and satisfy the frozen `'static` port.
#[derive(Clone)]
pub struct LocalNoTlsPostgresUnitOfWorkFactory {
    database_url: String,
    trusted_schema: String,
}

impl fmt::Debug for LocalNoTlsPostgresUnitOfWorkFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalNoTlsPostgresUnitOfWorkFactory")
            .field("database_url", &"<redacted>")
            .field("trusted_schema", &self.trusted_schema)
            .finish()
    }
}

impl LocalNoTlsPostgresUnitOfWorkFactory {
    /// Creates the Phase 1 local-development factory. Because this adapter uses
    /// `NoTls`, every configured host must be a Unix socket or an explicit
    /// loopback address. A network-capable TLS factory is a Phase 2 deliverable.
    pub fn new_local_no_tls(
        database_url: impl Into<String>,
        trusted_schema: impl Into<String>,
    ) -> PortResult<Self> {
        let database_url = database_url.into();
        validate_local_database_url(&database_url)?;
        let trusted_schema = trusted_schema.into();
        validate_schema(&trusted_schema)?;
        Ok(Self {
            database_url,
            trusted_schema,
        })
    }
}

impl UnitOfWorkFactory for LocalNoTlsPostgresUnitOfWorkFactory {
    type Uow = PostgresUnitOfWork;

    fn begin(&self, isolation: Isolation) -> PortFuture<'_, Self::Uow> {
        let database_url = self.database_url.clone();
        let trusted_schema = self.trusted_schema.clone();
        Box::pin(async move {
            let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
                .await
                .map_err(map_database_error)?;
            let connection_task = tokio::spawn(connection);
            let begin = match isolation {
                Isolation::ReadCommitted => "BEGIN ISOLATION LEVEL READ COMMITTED",
                Isolation::Serializable => "BEGIN ISOLATION LEVEL SERIALIZABLE",
            };
            if let Err(error) = client.batch_execute(begin).await {
                drop(client);
                let _ = connection_task.await;
                return Err(map_database_error(error));
            }

            let setup_result = client
                .batch_execute(
                    "SET LOCAL statement_timeout = '5s'; \
                     SET LOCAL lock_timeout = '5s'; \
                     SET LOCAL idle_in_transaction_session_timeout = '10s'",
                )
                .await;
            if let Err(error) = setup_result {
                let _ = client.batch_execute("ROLLBACK").await;
                drop(client);
                let _ = connection_task.await;
                return Err(map_database_error(error));
            }

            // Explicitly place pg_temp last so a temporary relation cannot
            // shadow a trusted application table. The schema is mandatory at
            // construction, so no transaction inherits a database/user-level
            // search_path by accident.
            let statement =
                format!("SET LOCAL search_path TO pg_catalog, \"{trusted_schema}\", pg_temp");
            if let Err(error) = client.batch_execute(&statement).await {
                let _ = client.batch_execute("ROLLBACK").await;
                drop(client);
                let _ = connection_task.await;
                return Err(map_database_error(error));
            }
            Ok(PostgresUnitOfWork {
                client: Some(client),
                connection_task: Some(connection_task),
                rollback_only: None,
            })
        })
    }
}

/// One owned PostgreSQL transaction. No method starts a nested transaction.
pub struct PostgresUnitOfWork {
    client: Option<Client>,
    connection_task: Option<JoinHandle<Result<(), tokio_postgres::Error>>>,
    rollback_only: Option<PortError>,
}

impl PostgresUnitOfWork {
    fn client(&self) -> PortResult<&Client> {
        self.client.as_ref().ok_or(PortError::Integrity)
    }

    fn poison(&mut self, error: &PortError) {
        if self.rollback_only.is_none() {
            self.rollback_only = Some(error.clone());
        }
    }

    /// Returns database-authoritative time inside the current transaction.
    pub async fn server_now(&mut self) -> PortResult<ServerInstant> {
        let result = async {
            let row = self
                .client()?
                .query_one("SELECT clock_timestamp()", &[])
                .await
                .map_err(map_database_error)?;
            row.try_get(0)
                .map(ServerInstant::new)
                .map_err(|_| PortError::Integrity)
        }
        .await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn finish(mut self, statement: &'static str) -> PortResult<()> {
        let result = match self.client.as_ref() {
            Some(client) => client
                .batch_execute(statement)
                .await
                .map_err(map_database_error),
            None => Err(PortError::Integrity),
        };
        drop(self.client.take());
        let connection_result = match self.connection_task.take() {
            Some(task) => task.await.map_err(|_| PortError::Unavailable)?,
            None => return Err(PortError::Integrity),
        };
        result?;
        connection_result.map_err(map_database_error)
    }

    /// Receipt-first lookup. Actor/key peers are serialized before any receipt
    /// read. A tombstone is never returned as a miss, so expiry or a legacy row
    /// can never authorize the command to execute again.
    pub async fn find_command_receipt<R>(
        &mut self,
        scope: &IdempotencyScope,
        metadata: &CommandMetadata,
    ) -> PortResult<CommandReceiptLookup<R>>
    where
        R: DeserializeOwned,
    {
        let result = self.find_command_receipt_inner(scope, metadata).await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn find_command_receipt_inner<R>(
        &self,
        scope: &IdempotencyScope,
        metadata: &CommandMetadata,
    ) -> PortResult<CommandReceiptLookup<R>>
    where
        R: DeserializeOwned,
    {
        if scope.actor_id != metadata.actor_id || scope.key != metadata.idempotency_key {
            return Err(PortError::Integrity);
        }
        let client = self.client()?;
        lock_idempotency_key(client, scope).await?;
        let key_hash = idempotency_key_hash(scope);
        let row = client
            .query_opt(
                "SELECT project_id, command_type, request_hash, response_body, command_id, \
                 resource_version, response_digest, replay_until, clock_timestamp() \
                 FROM command_receipts \
                 WHERE actor_id = $1 AND idempotency_key_hash = $2",
                &[scope.actor_id.as_uuid(), &&key_hash[..]],
            )
            .await
            .map_err(map_database_error)?;
        let Some(row) = row else {
            return Ok(CommandReceiptLookup::Missing);
        };

        let project_id: Uuid = try_get(&row, 0)?;
        let command_type: String = try_get(&row, 1)?;
        let request_hash: Vec<u8> = try_get(&row, 2)?;
        let request_digest = bytes_to_digest(&request_hash)?;
        // Reuse conflicts take precedence over expiry and legacy handling.
        if project_id != scope.project_id.into_uuid()
            || command_type != scope.command_type
            || request_digest != metadata.payload_digest
        {
            return Err(PortError::Conflict);
        }

        let response: Option<Json<Value>> = try_get(&row, 3)?;
        let command_id: Option<Uuid> = try_get(&row, 4)?;
        let resource_version: Option<i64> = try_get(&row, 5)?;
        let stored_response_digest: Option<Vec<u8>> = try_get(&row, 6)?;
        let replay_until: time::OffsetDateTime = try_get(&row, 7)?;
        let database_now: time::OffsetDateTime = try_get(&row, 8)?;
        let response_digest = stored_response_digest
            .as_deref()
            .map(bytes_to_digest)
            .transpose()?;

        let (Some(Json(response)), Some(command_id), Some(resource_version), Some(response_digest)) =
            (response, command_id, resource_version, response_digest)
        else {
            return Ok(CommandReceiptLookup::Legacy { response_digest });
        };

        let actual_response_digest = digest_json(&response)?;
        if actual_response_digest != response_digest {
            return Err(PortError::Integrity);
        }
        if replay_until <= database_now {
            return Ok(CommandReceiptLookup::Expired { response_digest });
        }

        let response = serde_json::from_value(response).map_err(|_| PortError::Integrity)?;
        Ok(CommandReceiptLookup::Replay(CommandReceipt {
            scope: IdempotencyScope::new(
                ProjectId::from_uuid(project_id),
                command_type,
                scope.actor_id,
                scope.key.clone(),
            )
            .map_err(|_| PortError::Integrity)?,
            command_id: agentforge_domain::CommandId::from_uuid(command_id),
            payload_digest: request_digest,
            response,
            resource_version: i64_to_version(resource_version)?,
        }))
    }

    /// Stores a token-free success receipt in the same transaction as events
    /// and outbox rows. `effect_digest` is supplied by the use case and is never
    /// substituted with the wire response digest.
    pub async fn store_command_receipt<R>(
        &mut self,
        receipt: &CommandReceipt<R>,
        replay_until: ServerInstant,
        effect_digest: Sha256Digest,
    ) -> PortResult<()>
    where
        R: Serialize,
    {
        let result = self
            .store_command_receipt_inner(receipt, replay_until, effect_digest)
            .await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn store_command_receipt_inner<R>(
        &self,
        receipt: &CommandReceipt<R>,
        replay_until: ServerInstant,
        effect_digest: Sha256Digest,
    ) -> PortResult<()>
    where
        R: Serialize,
    {
        if digest_is_zero(effect_digest) {
            return Err(PortError::Integrity);
        }
        let client = self.client()?;
        lock_idempotency_key(client, &receipt.scope).await?;
        let key_hash = idempotency_key_hash(&receipt.scope);
        let response =
            serde_json::to_value(&receipt.response).map_err(|_| PortError::Serialization)?;
        let response_digest = digest_json(&response)?;
        let version = version_to_i64(receipt.resource_version)?;
        let changed = client
            .execute(
                "INSERT INTO command_receipts \
                 (actor_id, idempotency_key_hash, project_id, command_type, request_hash, \
                  response_classification, response_status, response_body, effect_digest, \
                  replay_until, command_id, resource_version, response_digest) \
                 SELECT $1, $2, $3, $4, $5, 'INTERNAL', 200, $6, $7, $8, $9, $10, $11 \
                 WHERE $8 > clock_timestamp()",
                &[
                    receipt.scope.actor_id.as_uuid(),
                    &&key_hash[..],
                    receipt.scope.project_id.as_uuid(),
                    &receipt.scope.command_type,
                    &&receipt.payload_digest.as_bytes()[..],
                    &Json(&response),
                    &&effect_digest.as_bytes()[..],
                    &replay_until.0,
                    receipt.command_id.as_uuid(),
                    &version,
                    &&response_digest.as_bytes()[..],
                ],
            )
            .await
            .map_err(map_database_error)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(PortError::Integrity)
        }
    }

    /// Enqueues externally visible envelopes after their event rows. The FK
    /// makes an event-less outbox entry impossible, including before commit.
    pub async fn enqueue_outbox(&mut self, messages: &[OutboxMessage]) -> PortResult<()> {
        let result = self.enqueue_outbox_inner(messages).await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn enqueue_outbox_inner(&self, messages: &[OutboxMessage]) -> PortResult<()> {
        let client = self.client()?;
        for message in messages {
            validate_outbox_message(message)?;
            client
                .execute(
                    "INSERT INTO outbox_messages \
                     (id, project_id, event_id, topic, message_key, envelope, available_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        message.id.as_uuid(),
                        message.project_id.as_uuid(),
                        message.event_id.as_uuid(),
                        &message.topic,
                        &message.message_key,
                        &Json(&message.envelope),
                        &message.available_at.0,
                    ],
                )
                .await
                .map_err(map_database_error)?;
        }
        Ok(())
    }

    /// Claims a deterministic batch. Membership and returned order are both
    /// fixed by `(available_at,id)`; `SKIP LOCKED` is internal relay behavior.
    pub async fn claim_outbox(
        &mut self,
        holder_id: Uuid,
        lease: Duration,
        batch_size: u16,
    ) -> PortResult<Vec<OutboxClaim>> {
        let result = self.claim_outbox_inner(holder_id, lease, batch_size).await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn claim_outbox_inner(
        &self,
        holder_id: Uuid,
        lease: Duration,
        batch_size: u16,
    ) -> PortResult<Vec<OutboxClaim>> {
        if holder_id.is_nil()
            || lease < Duration::from_secs(1)
            || batch_size == 0
            || batch_size > 100
        {
            return Err(PortError::Integrity);
        }
        let lease_seconds = lease.as_secs_f64();
        if !lease_seconds.is_finite() || lease_seconds > 300.0 {
            return Err(PortError::Integrity);
        }
        let rows = self
            .client()?
            .query(
                "WITH candidates AS ( \
                   SELECT id FROM outbox_messages \
                   WHERE published_at IS NULL AND available_at <= clock_timestamp() \
                     AND (claimed_by IS NULL OR claim_expires_at <= clock_timestamp()) \
                   ORDER BY available_at, id \
                   FOR UPDATE SKIP LOCKED LIMIT $1 \
                 ), claimed AS ( \
                   UPDATE outbox_messages AS message \
                   SET claimed_by = $2, \
                       claim_expires_at = clock_timestamp() + make_interval(secs => $3), \
                       claim_generation = message.claim_generation + 1, \
                       attempts = message.attempts + 1 \
                   FROM candidates WHERE message.id = candidates.id \
                   RETURNING message.id, message.project_id, message.event_id, message.topic, \
                     message.message_key, message.envelope, message.claim_generation, \
                     message.attempts, message.claim_expires_at, message.available_at \
                 ) \
                 SELECT id, project_id, event_id, topic, message_key, envelope, \
                        claim_generation, attempts, claim_expires_at \
                 FROM claimed ORDER BY available_at, id",
                &[&i64::from(batch_size), &holder_id, &lease_seconds],
            )
            .await
            .map_err(map_database_error)?;
        rows.into_iter().map(decode_outbox_claim).collect()
    }

    /// Completes only the current, unexpired publisher proof.
    pub async fn complete_outbox(&mut self, proof: OutboxClaimProof) -> PortResult<()> {
        let result = self.complete_outbox_inner(proof).await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn complete_outbox_inner(&self, proof: OutboxClaimProof) -> PortResult<()> {
        let generation = u64_to_i64(proof.generation)?;
        let changed = self
            .client()?
            .execute(
                "UPDATE outbox_messages \
                 SET published_at = clock_timestamp(), claimed_by = NULL, claim_expires_at = NULL \
                 WHERE id = $1 AND claimed_by = $2 AND claim_generation = $3 \
                   AND claim_expires_at > clock_timestamp() AND published_at IS NULL",
                &[proof.message_id.as_uuid(), &proof.holder_id, &generation],
            )
            .await
            .map_err(map_database_error)?;
        expect_one(changed)
    }

    /// Releases only the current proof and schedules a deterministic retry.
    pub async fn retry_outbox(
        &mut self,
        proof: OutboxClaimProof,
        available_at: ServerInstant,
        error_code: &str,
    ) -> PortResult<()> {
        let result = self
            .retry_outbox_inner(proof, available_at, error_code)
            .await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn retry_outbox_inner(
        &self,
        proof: OutboxClaimProof,
        available_at: ServerInstant,
        error_code: &str,
    ) -> PortResult<()> {
        if error_code.is_empty()
            || error_code.len() > 96
            || !error_code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(PortError::Integrity);
        }
        let generation = u64_to_i64(proof.generation)?;
        let changed = self
            .client()?
            .execute(
                "UPDATE outbox_messages \
                 SET available_at = $4, last_error_code = $5, \
                     claimed_by = NULL, claim_expires_at = NULL \
                 WHERE id = $1 AND claimed_by = $2 AND claim_generation = $3 \
                   AND claim_expires_at > clock_timestamp() AND published_at IS NULL",
                &[
                    proof.message_id.as_uuid(),
                    &proof.holder_id,
                    &generation,
                    &available_at.0,
                    &error_code,
                ],
            )
            .await
            .map_err(map_database_error)?;
        expect_one(changed)
    }

    /// Inserts the consumer receipt before applying the local effect. Because
    /// both occur in this UoW, rollback removes both and commit preserves both.
    /// The dedupe identity is the outbox/broker message id, not the domain event.
    pub async fn record_inbox_applied(
        &mut self,
        consumer: &str,
        message_id: OutboxMessageId,
        project_id: ProjectId,
        payload_digest: Sha256Digest,
        result_digest: Sha256Digest,
    ) -> PortResult<InboxDisposition> {
        let result = self
            .record_inbox_applied_inner(
                consumer,
                message_id,
                project_id,
                payload_digest,
                result_digest,
            )
            .await;
        if let Err(error) = &result {
            self.poison(error);
        }
        result
    }

    async fn record_inbox_applied_inner(
        &self,
        consumer: &str,
        message_id: OutboxMessageId,
        project_id: ProjectId,
        payload_digest: Sha256Digest,
        result_digest: Sha256Digest,
    ) -> PortResult<InboxDisposition> {
        validate_consumer(consumer)?;
        if message_id.as_uuid().is_nil() {
            return Err(PortError::Integrity);
        }
        let client = self.client()?;
        let lock_key = format!("inbox:{consumer}:{}", message_id.as_uuid());
        client
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&lock_key],
            )
            .await
            .map_err(map_database_error)?;
        if let Some(row) = client
            .query_opt(
                "SELECT project_id, payload_digest, result_digest FROM inbox_messages \
                 WHERE consumer = $1 AND message_id = $2",
                &[&consumer, message_id.as_uuid()],
            )
            .await
            .map_err(map_database_error)?
        {
            let stored_project: Uuid = try_get(&row, 0)?;
            let stored_payload: Vec<u8> = try_get(&row, 1)?;
            let stored_result: Option<Vec<u8>> = try_get(&row, 2)?;
            if stored_project != project_id.into_uuid()
                || stored_payload.as_slice() != payload_digest.as_bytes()
                || stored_result.as_deref() != Some(result_digest.as_bytes())
            {
                return Err(PortError::Conflict);
            }
            return Ok(InboxDisposition::Replay);
        }
        client
            .execute(
                "INSERT INTO inbox_messages \
                 (consumer, message_id, project_id, payload_digest, applied_at, result_digest) \
                 VALUES ($1, $2, $3, $4, clock_timestamp(), $5)",
                &[
                    &consumer,
                    message_id.as_uuid(),
                    project_id.as_uuid(),
                    &&payload_digest.as_bytes()[..],
                    &&result_digest.as_bytes()[..],
                ],
            )
            .await
            .map_err(map_database_error)?;
        Ok(InboxDisposition::Applied)
    }

    async fn append_events_inner(&self, events: &[EventRecord]) -> PortResult<AppendEventsReceipt> {
        let batches = prepare_event_batches(events)?;
        if batches.is_empty() {
            return Ok(AppendEventsReceipt {
                appended: 0,
                first_cursor: None,
                last_cursor: None,
            });
        }
        let client = self.client()?;
        lock_event_projects(client, &batches).await?;

        let mut first_cursor = None;
        let mut last_cursor = None;
        for batch in batches {
            advance_event_head(client, &batch).await?;
            for event in batch.events {
                let cursor = append_event(client, event).await?;
                if first_cursor.is_none() {
                    first_cursor = Some(cursor);
                }
                last_cursor = Some(cursor);
            }
        }
        Ok(AppendEventsReceipt {
            appended: events.len(),
            first_cursor,
            last_cursor,
        })
    }
}

impl EventAppendPort for PostgresUnitOfWork {
    fn append_events<'a>(
        &'a mut self,
        events: &'a [EventRecord],
    ) -> PortFuture<'a, AppendEventsReceipt> {
        Box::pin(async move {
            let result = self.append_events_inner(events).await;
            if let Err(error) = &result {
                self.poison(error);
            }
            result
        })
    }
}

impl UnitOfWork for PostgresUnitOfWork {
    fn commit(self) -> PortFuture<'static, ()> {
        Box::pin(async move {
            if let Some(error) = self.rollback_only.clone() {
                self.finish("ROLLBACK").await?;
                return Err(error);
            }
            self.finish("COMMIT").await
        })
    }

    fn rollback(self) -> PortFuture<'static, ()> {
        Box::pin(self.finish("ROLLBACK"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandReceiptLookup<R> {
    Missing,
    Replay(CommandReceipt<R>),
    Expired {
        response_digest: Sha256Digest,
    },
    Legacy {
        response_digest: Option<Sha256Digest>,
    },
}

/// Stable identity placed on the broker envelope and used by Inbox dedupe.
/// Keeping it distinct from `EventId` prevents one domain event with multiple
/// routed outbox messages from collapsing into a single consumer receipt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxMessageId(Uuid);

impl OutboxMessageId {
    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxMessage {
    pub id: OutboxMessageId,
    pub project_id: ProjectId,
    pub event_id: EventId,
    pub topic: String,
    pub message_key: String,
    pub envelope: Value,
    pub available_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxClaim {
    pub id: OutboxMessageId,
    pub project_id: ProjectId,
    pub event_id: EventId,
    pub topic: String,
    pub message_key: String,
    pub envelope: Value,
    pub generation: u64,
    pub attempts: u32,
    pub expires_at: ServerInstant,
}

impl OutboxClaim {
    #[must_use]
    pub const fn proof(&self, holder_id: Uuid) -> OutboxClaimProof {
        OutboxClaimProof {
            message_id: self.id,
            holder_id,
            generation: self.generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxClaimProof {
    pub message_id: OutboxMessageId,
    pub holder_id: Uuid,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxDisposition {
    Applied,
    Replay,
}

struct EventBatch<'a> {
    project_id: Uuid,
    aggregate_type: &'static str,
    aggregate_id: Uuid,
    aggregate_version: i64,
    first_event_seq: i64,
    last_event_seq: i64,
    events: Vec<&'a EventRecord>,
}

fn prepare_event_batches(events: &[EventRecord]) -> PortResult<Vec<EventBatch<'_>>> {
    let mut event_ids = BTreeSet::new();
    let mut groups: BTreeMap<(Uuid, &'static str, Uuid), Vec<&EventRecord>> = BTreeMap::new();
    for event in events {
        validate_event(event)?;
        if !event_ids.insert(event.event_id.into_uuid()) {
            return Err(PortError::Integrity);
        }
        let aggregate_id = aggregate_uuid(event.aggregate_type, event.aggregate_id)?;
        groups
            .entry((
                event.project_id.into_uuid(),
                aggregate_type_label(event.aggregate_type),
                aggregate_id,
            ))
            .or_default()
            .push(event);
    }

    groups
        .into_iter()
        .map(|((project_id, aggregate_type, aggregate_id), mut events)| {
            events.sort_by_key(|event| event.aggregate_seq);
            let first = events.first().ok_or(PortError::Integrity)?;
            let aggregate_version = version_to_i64(first.aggregate_version)?;
            let first_event_seq = u64_to_i64(first.aggregate_seq)?;
            let mut expected_seq = first.aggregate_seq;
            for event in &events {
                if version_to_i64(event.aggregate_version)? != aggregate_version
                    || event.aggregate_seq != expected_seq
                {
                    return Err(PortError::Integrity);
                }
                expected_seq = expected_seq.checked_add(1).ok_or(PortError::Integrity)?;
            }
            let last_event_seq =
                u64_to_i64(events.last().ok_or(PortError::Integrity)?.aggregate_seq)?;
            Ok(EventBatch {
                project_id,
                aggregate_type,
                aggregate_id,
                aggregate_version,
                first_event_seq,
                last_event_seq,
                events,
            })
        })
        .collect()
}

fn validate_event(event: &EventRecord) -> PortResult<()> {
    let aggregate_id = aggregate_uuid(event.aggregate_type, event.aggregate_id)?;
    if event.project_id.as_uuid().is_nil()
        || event.event_id.as_uuid().is_nil()
        || aggregate_id.is_nil()
        || event.actor_id.as_uuid().is_nil()
        || event.correlation_id.as_uuid().is_nil()
        || event
            .causation_id
            .is_some_and(|causation_id| causation_id.as_uuid().is_nil())
        || event.aggregate_version == AggregateVersion::ZERO
        || event.aggregate_seq == 0
        || event.schema_version == 0
        || event.envelope_version != EVENT_ENVELOPE_VERSION
        || event.event_type.is_empty()
    {
        return Err(PortError::Integrity);
    }
    let actual_digest = digest_json(&event.payload)?;
    if actual_digest != event.payload_digest {
        return Err(PortError::Integrity);
    }
    Ok(())
}

async fn lock_event_projects(client: &Client, batches: &[EventBatch<'_>]) -> PortResult<()> {
    let projects = batches
        .iter()
        .map(|batch| batch.project_id)
        .collect::<BTreeSet<_>>();
    for project_id in projects {
        client
            .query_one(
                "SELECT pg_advisory_xact_lock(\
                   hashtextextended('agentforge:event-project:' || $1::uuid::text, 0))",
                &[&project_id],
            )
            .await
            .map_err(map_database_error)?;
    }
    Ok(())
}

async fn advance_event_head(client: &Client, batch: &EventBatch<'_>) -> PortResult<()> {
    let current = client
        .query_opt(
            "SELECT aggregate_version, last_event_seq FROM aggregate_event_heads \
             WHERE project_id = $1 AND aggregate_type = $2 AND aggregate_id = $3 \
             FOR UPDATE",
            &[
                &batch.project_id,
                &batch.aggregate_type,
                &batch.aggregate_id,
            ],
        )
        .await
        .map_err(map_database_error)?;
    match current {
        None => {
            if batch.aggregate_version != 1 || batch.first_event_seq != 1 {
                return Err(PortError::Conflict);
            }
            client
                .execute(
                    "INSERT INTO aggregate_event_heads \
                     (project_id, aggregate_type, aggregate_id, aggregate_version, last_event_seq) \
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &batch.project_id,
                        &batch.aggregate_type,
                        &batch.aggregate_id,
                        &batch.aggregate_version,
                        &batch.last_event_seq,
                    ],
                )
                .await
                .map_err(map_database_error)?;
        }
        Some(row) => {
            let current_version: i64 = try_get(&row, 0)?;
            let current_seq: i64 = try_get(&row, 1)?;
            if current_version.checked_add(1) != Some(batch.aggregate_version)
                || current_seq.checked_add(1) != Some(batch.first_event_seq)
            {
                return Err(PortError::Conflict);
            }
            let changed = client
                .execute(
                    "UPDATE aggregate_event_heads \
                     SET aggregate_version = $4, last_event_seq = $5, \
                         updated_at = clock_timestamp() \
                     WHERE project_id = $1 AND aggregate_type = $2 AND aggregate_id = $3 \
                       AND aggregate_version = $6 AND last_event_seq = $7",
                    &[
                        &batch.project_id,
                        &batch.aggregate_type,
                        &batch.aggregate_id,
                        &batch.aggregate_version,
                        &batch.last_event_seq,
                        &current_version,
                        &current_seq,
                    ],
                )
                .await
                .map_err(map_database_error)?;
            expect_one(changed)?;
        }
    }
    Ok(())
}

async fn append_event(client: &Client, event: &EventRecord) -> PortResult<ProjectionCursor> {
    let aggregate_id = aggregate_uuid(event.aggregate_type, event.aggregate_id)?;
    let aggregate_type = aggregate_type_label(event.aggregate_type);
    let metadata = json!({
        "required_semantics": event.required_semantics,
        "optional_metadata": event.optional_metadata,
    });
    let metadata_digest = digest_json(&metadata)?;
    let aggregate_version = version_to_i64(event.aggregate_version)?;
    let aggregate_seq = u64_to_i64(event.aggregate_seq)?;
    let schema_version = i16::try_from(event.schema_version).map_err(|_| PortError::Integrity)?;
    let envelope_version =
        i16::try_from(event.envelope_version).map_err(|_| PortError::Integrity)?;
    let row = client
        .query_one(
            "INSERT INTO domain_events \
             (id, project_id, aggregate_type, aggregate_id, aggregate_version, event_seq, \
              event_type, schema_version, envelope_version, payload, payload_digest, metadata, \
              metadata_digest, causation_id, correlation_id, actor_id, occurred_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) \
             RETURNING global_sequence",
            &[
                event.event_id.as_uuid(),
                event.project_id.as_uuid(),
                &aggregate_type,
                &aggregate_id,
                &aggregate_version,
                &aggregate_seq,
                &event.event_type,
                &schema_version,
                &envelope_version,
                &Json(&event.payload),
                &&event.payload_digest.as_bytes()[..],
                &Json(&metadata),
                &&metadata_digest.as_bytes()[..],
                &event.causation_id.map(EventId::into_uuid),
                event.correlation_id.as_uuid(),
                event.actor_id.as_uuid(),
                &event.occurred_at.0,
            ],
        )
        .await
        .map_err(map_database_error)?;
    let global_sequence: i64 = try_get(&row, 0)?;
    let global_sequence = u64::try_from(global_sequence).map_err(|_| PortError::Integrity)?;
    Ok(ProjectionCursor::new(
        event.occurred_at,
        event.event_id,
        global_sequence,
    ))
}

async fn lock_idempotency_key(client: &Client, scope: &IdempotencyScope) -> PortResult<()> {
    let lock_key = format!("command:{}:{}", scope.actor_id, scope.key);
    client
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

fn decode_outbox_claim(row: Row) -> PortResult<OutboxClaim> {
    let generation: i64 = try_get(&row, 6)?;
    let attempts: i32 = try_get(&row, 7)?;
    let Json(envelope): Json<Value> = try_get(&row, 5)?;
    Ok(OutboxClaim {
        id: OutboxMessageId::from_uuid(try_get(&row, 0)?),
        project_id: ProjectId::from_uuid(try_get(&row, 1)?),
        event_id: EventId::from_uuid(try_get(&row, 2)?),
        topic: try_get(&row, 3)?,
        message_key: try_get(&row, 4)?,
        envelope,
        generation: u64::try_from(generation).map_err(|_| PortError::Integrity)?,
        attempts: u32::try_from(attempts).map_err(|_| PortError::Integrity)?,
        expires_at: ServerInstant::new(try_get(&row, 8)?),
    })
}

fn validate_outbox_message(message: &OutboxMessage) -> PortResult<()> {
    let valid_label = |value: &str| {
        !value.is_empty()
            && value.len() <= 200
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
            })
    };
    if message.id.as_uuid().is_nil()
        || message.project_id.as_uuid().is_nil()
        || message.event_id.as_uuid().is_nil()
        || !valid_label(&message.topic)
        || !valid_label(&message.message_key)
    {
        return Err(PortError::Integrity);
    }
    Ok(())
}

fn validate_consumer(consumer: &str) -> PortResult<()> {
    if consumer.is_empty()
        || consumer.len() > 96
        || !consumer
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(PortError::Integrity);
    }
    Ok(())
}

fn validate_schema(schema: &str) -> PortResult<()> {
    if schema.is_empty()
        || schema.len() > 63
        || !schema
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(PortError::Integrity);
    }
    Ok(())
}

fn validate_local_database_url(database_url: &str) -> PortResult<()> {
    let config = database_url
        .parse::<tokio_postgres::Config>()
        .map_err(|_| PortError::Integrity)?;
    let hosts = config.get_hosts();
    if hosts.is_empty()
        || hosts.iter().any(|host| match host {
            Host::Unix(_) => false,
            Host::Tcp(host) => !matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1"),
        })
        // `hostaddr` overrides DNS/connect routing when it is present. Checking
        // only `host` would let `host=localhost hostaddr=<remote>` bypass this
        // local-only NoTls boundary.
        || config
            .get_hostaddrs()
            .iter()
            .any(|address| !address.is_loopback())
    {
        return Err(PortError::Integrity);
    }
    Ok(())
}

fn aggregate_uuid(kind: AggregateType, id: AggregateId) -> PortResult<Uuid> {
    match (kind, id) {
        (AggregateType::WorkPackage, AggregateId::WorkPackage(value)) => Ok(value.into_uuid()),
        (AggregateType::Attempt, AggregateId::Attempt(value)) => Ok(value.into_uuid()),
        (AggregateType::Lease, AggregateId::Lease(value)) => Ok(value.into_uuid()),
        (AggregateType::Submission, AggregateId::Submission(value)) => Ok(value.into_uuid()),
        (AggregateType::RunSignal, AggregateId::RunSignal(value)) => Ok(value.into_uuid()),
        (AggregateType::InvocationIntent, AggregateId::InvocationIntent(value)) => {
            Ok(value.into_uuid())
        }
        (AggregateType::InvocationRun, AggregateId::InvocationRun(value)) => Ok(value.into_uuid()),
        (AggregateType::RunClaim, AggregateId::RunClaim(value)) => Ok(value.into_uuid()),
        (AggregateType::SessionCapsule, AggregateId::SessionCapsule(value)) => {
            Ok(value.into_uuid())
        }
        (AggregateType::BudgetReservation, AggregateId::BudgetReservation(value)) => {
            Ok(value.into_uuid())
        }
        (AggregateType::GovernanceCase, AggregateId::GovernanceCase(value)) => {
            Ok(value.into_uuid())
        }
        (AggregateType::Decision, AggregateId::Decision(value)) => Ok(value.into_uuid()),
        (AggregateType::PolicyRevision, AggregateId::PolicyRevision(value)) => {
            Ok(value.into_uuid())
        }
        _ => Err(PortError::Integrity),
    }
}

const fn aggregate_type_label(kind: AggregateType) -> &'static str {
    match kind {
        AggregateType::WorkPackage => "WORK_PACKAGE",
        AggregateType::Attempt => "ATTEMPT",
        AggregateType::Lease => "LEASE",
        AggregateType::Submission => "SUBMISSION",
        AggregateType::RunSignal => "RUN_SIGNAL",
        AggregateType::InvocationIntent => "INVOCATION_INTENT",
        AggregateType::InvocationRun => "INVOCATION_RUN",
        AggregateType::RunClaim => "RUN_CLAIM",
        AggregateType::SessionCapsule => "SESSION_CAPSULE",
        AggregateType::BudgetReservation => "BUDGET_RESERVATION",
        AggregateType::GovernanceCase => "GOVERNANCE_CASE",
        AggregateType::Decision => "DECISION",
        AggregateType::PolicyRevision => "POLICY_REVISION",
    }
}

fn idempotency_key_hash(scope: &IdempotencyScope) -> [u8; 32] {
    Sha256::digest(scope.key.as_str().as_bytes()).into()
}

fn digest_json(value: &Value) -> PortResult<Sha256Digest> {
    serde_json_canonicalizer::to_vec(value)
        .map(|bytes| Sha256Digest::of_bytes(&bytes))
        .map_err(|_| PortError::Serialization)
}

fn digest_is_zero(digest: Sha256Digest) -> bool {
    digest.as_bytes().iter().all(|byte| *byte == 0)
}

fn bytes_to_digest(bytes: &[u8]) -> PortResult<Sha256Digest> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| PortError::Integrity)?;
    Ok(Sha256Digest::from_bytes(bytes))
}

fn version_to_i64(version: AggregateVersion) -> PortResult<i64> {
    u64_to_i64(version.get())
}

fn u64_to_i64(value: u64) -> PortResult<i64> {
    i64::try_from(value).map_err(|_| PortError::Integrity)
}

fn i64_to_version(value: i64) -> PortResult<AggregateVersion> {
    let value = u64::try_from(value).map_err(|_| PortError::Integrity)?;
    if value == 0 {
        return Err(PortError::Integrity);
    }
    Ok(AggregateVersion::new(value))
}

fn expect_one(changed: u64) -> PortResult<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(PortError::Conflict)
    }
}

fn try_get<T>(row: &Row, index: usize) -> PortResult<T>
where
    T: tokio_postgres::types::FromSqlOwned,
{
    row.try_get(index).map_err(|_| PortError::Integrity)
}

fn map_database_error(error: tokio_postgres::Error) -> PortError {
    match error.as_db_error().map(|error| error.code()) {
        Some(&SqlState::UNIQUE_VIOLATION)
        | Some(&SqlState::T_R_SERIALIZATION_FAILURE)
        | Some(&SqlState::T_R_DEADLOCK_DETECTED) => PortError::Conflict,
        Some(&SqlState::CHECK_VIOLATION)
        | Some(&SqlState::FOREIGN_KEY_VIOLATION)
        | Some(&SqlState::NOT_NULL_VIOLATION) => PortError::Integrity,
        _ if error.is_closed() => PortError::Unavailable,
        _ => PortError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_labels_are_protocol_stable() {
        assert_eq!(
            aggregate_type_label(AggregateType::WorkPackage),
            "WORK_PACKAGE"
        );
        assert_eq!(
            aggregate_type_label(AggregateType::InvocationRun),
            "INVOCATION_RUN"
        );
    }

    #[test]
    fn label_and_consumer_validation_fail_closed() {
        assert!(validate_consumer("projection.v1").is_ok());
        assert_eq!(validate_consumer("bad consumer"), Err(PortError::Integrity));
    }

    #[test]
    fn factory_debug_redacts_database_credentials() {
        let factory = LocalNoTlsPostgresUnitOfWorkFactory::new_local_no_tls(
            "postgres://agentforge:do-not-print@127.0.0.1/agentforge",
            "public",
        )
        .expect("loopback database");
        let rendered = format!("{factory:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("do-not-print"));
        assert!(!rendered.contains("postgres://"));
    }

    #[test]
    fn no_tls_factory_rejects_non_loopback_network_hosts() {
        assert!(matches!(
            LocalNoTlsPostgresUnitOfWorkFactory::new_local_no_tls(
                "postgres://agentforge@example.internal/agentforge",
                "public"
            ),
            Err(PortError::Integrity)
        ));
        assert!(matches!(
            LocalNoTlsPostgresUnitOfWorkFactory::new_local_no_tls(
                "host=localhost hostaddr=203.0.113.10 user=agentforge dbname=agentforge",
                "public"
            ),
            Err(PortError::Integrity)
        ));
        assert!(matches!(
            LocalNoTlsPostgresUnitOfWorkFactory::new_local_no_tls(
                "postgres://agentforge@127.0.0.1/agentforge",
                "public, pg_temp"
            ),
            Err(PortError::Integrity)
        ));
        assert!(
            LocalNoTlsPostgresUnitOfWorkFactory::new_local_no_tls(
                "host=/var/run/postgresql user=agentforge dbname=agentforge",
                "public"
            )
            .is_ok()
        );
    }
}
