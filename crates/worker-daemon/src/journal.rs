//! SQLite WAL-backed Worker Journal.
//!
//! Every command is reduced and persisted in one `BEGIN IMMEDIATE`
//! transaction: Inbox receipt, hash-chained fact, materialized Attempt state,
//! and outbound notification either commit together or all roll back.

use std::{path::Path, str::FromStr};

use agentforge_domain::{
    ActorId, AttemptId, IdempotencyKey, ProtocolKey, ServerInstant, Sha256Digest,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::runtime::{
    AttemptGrant, WorkerAttemptState, WorkerCommandEnvelope, WorkerError, WorkerFact, grant_fact,
};

const JOURNAL_SCHEMA_VERSION: i64 = 1;
const OUTBOX_DESTINATION: &str = "control-plane.worker-events";

const SCHEMA: &str = r#"
CREATE TABLE attempts (
  attempt_id TEXT PRIMARY KEY,
  package_id TEXT NOT NULL,
  package_revision INTEGER NOT NULL CHECK (package_revision > 0),
  package_hash TEXT NOT NULL,
  lease_id TEXT NOT NULL,
  lease_generation INTEGER NOT NULL CHECK (lease_generation > 0),
  phase TEXT NOT NULL CHECK (phase IN (
    'granted', 'preparing', 'baseline', 'planning', 'implementing', 'local_verifying',
    'waiting_input', 'sealing_candidate', 'handing_off_candidate', 'salvaging',
    'author_complete', 'local_failed', 'local_cancelled'
  )),
  version INTEGER NOT NULL CHECK (version > 0),
  journal_seq INTEGER NOT NULL CHECK (journal_seq > 0),
  state_json TEXT NOT NULL,
  state_digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (version = journal_seq)
);

CREATE TABLE journal_entries (
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
  seq INTEGER NOT NULL CHECK (seq > 0),
  event_id TEXT NOT NULL UNIQUE,
  fact_json TEXT NOT NULL,
  fact_digest TEXT NOT NULL,
  previous_entry_digest TEXT,
  entry_digest TEXT NOT NULL UNIQUE,
  occurred_at TEXT NOT NULL,
  PRIMARY KEY (attempt_id, seq)
);

CREATE TABLE inbox (
  actor_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  message_id TEXT NOT NULL UNIQUE,
  request_digest TEXT NOT NULL,
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  received_at TEXT NOT NULL,
  PRIMARY KEY (actor_id, idempotency_key)
);

CREATE TABLE outbox (
  outbox_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
  destination TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  available_at TEXT NOT NULL,
  delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
  delivered_at TEXT,
  last_error_code TEXT,
  UNIQUE (destination, idempotency_key)
);

CREATE TABLE operations (
  operation_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
  idempotency_key TEXT NOT NULL,
  kind TEXT NOT NULL,
  idempotency_class TEXT NOT NULL CHECK (idempotency_class IN (
    'pure', 'idempotent', 'query_then_retry', 'non_repeatable'
  )),
  state TEXT NOT NULL CHECK (state IN ('pending', 'completed')),
  request_digest TEXT NOT NULL,
  result_digest TEXT,
  planned_at TEXT NOT NULL,
  deadline_at TEXT NOT NULL,
  finished_at TEXT,
  UNIQUE (attempt_id, idempotency_key),
  CHECK ((state = 'pending' AND result_digest IS NULL AND finished_at IS NULL)
      OR (state = 'completed' AND result_digest IS NOT NULL AND finished_at IS NOT NULL))
);

CREATE INDEX operations_pending_idx
  ON operations (attempt_id, planned_at, operation_id)
  WHERE state = 'pending';

CREATE INDEX outbox_pending_idx
  ON outbox (available_at, outbox_id)
  WHERE delivered_at IS NULL;

CREATE TRIGGER attempts_binding_is_immutable
BEFORE UPDATE ON attempts
WHEN NEW.attempt_id <> OLD.attempt_id
  OR NEW.package_id <> OLD.package_id
  OR NEW.package_revision <> OLD.package_revision
  OR NEW.package_hash <> OLD.package_hash
  OR NEW.lease_id <> OLD.lease_id
  OR NEW.lease_generation <> OLD.lease_generation
  OR NEW.created_at <> OLD.created_at
BEGIN
  SELECT RAISE(ABORT, 'attempt binding is immutable');
END;

CREATE TRIGGER journal_entries_are_immutable
BEFORE UPDATE ON journal_entries
BEGIN
  SELECT RAISE(ABORT, 'journal entries are immutable');
END;

CREATE TRIGGER journal_entries_cannot_be_deleted
BEFORE DELETE ON journal_entries
BEGIN
  SELECT RAISE(ABORT, 'journal entries cannot be deleted');
END;

CREATE TRIGGER inbox_is_immutable
BEFORE UPDATE ON inbox
BEGIN
  SELECT RAISE(ABORT, 'inbox receipts are immutable');
END;

CREATE TRIGGER inbox_cannot_be_deleted
BEFORE DELETE ON inbox
BEGIN
  SELECT RAISE(ABORT, 'inbox receipts cannot be deleted');
END;

CREATE TRIGGER outbox_payload_is_immutable
BEFORE UPDATE ON outbox
WHEN NEW.outbox_id <> OLD.outbox_id
  OR NEW.attempt_id <> OLD.attempt_id
  OR NEW.destination <> OLD.destination
  OR NEW.idempotency_key <> OLD.idempotency_key
  OR NEW.payload_json <> OLD.payload_json
  OR NEW.payload_digest <> OLD.payload_digest
  OR NEW.available_at <> OLD.available_at
BEGIN
  SELECT RAISE(ABORT, 'outbox payload is immutable');
END;

CREATE TRIGGER outbox_cannot_be_deleted
BEFORE DELETE ON outbox
BEGIN
  SELECT RAISE(ABORT, 'outbox rows cannot be deleted');
END;

CREATE TRIGGER operations_request_is_immutable
BEFORE UPDATE ON operations
WHEN NEW.operation_id <> OLD.operation_id
  OR NEW.attempt_id <> OLD.attempt_id
  OR NEW.idempotency_key <> OLD.idempotency_key
  OR NEW.kind <> OLD.kind
  OR NEW.idempotency_class <> OLD.idempotency_class
  OR NEW.request_digest <> OLD.request_digest
  OR NEW.planned_at <> OLD.planned_at
  OR NEW.deadline_at <> OLD.deadline_at
BEGIN
  SELECT RAISE(ABORT, 'operation request is immutable');
END;

CREATE TRIGGER operations_state_is_monotonic
BEFORE UPDATE ON operations
WHEN OLD.state <> 'pending'
  OR NEW.state <> 'completed'
  OR NEW.result_digest IS NULL
  OR NEW.finished_at IS NULL
BEGIN
  SELECT RAISE(ABORT, 'operation state is monotonic');
END;

CREATE TRIGGER operations_cannot_be_deleted
BEFORE DELETE ON operations
BEGIN
  SELECT RAISE(ABORT, 'operations cannot be deleted');
END;
"#;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalCommand {
    Grant {
        grant: AttemptGrant,
    },
    Apply {
        attempt_id: AttemptId,
        command: WorkerCommandEnvelope,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalRequest {
    pub message_id: Uuid,
    pub actor_id: ActorId,
    pub idempotency_key: IdempotencyKey,
    pub command: JournalCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalDisposition {
    Applied(WorkerAttemptState),
    Replay(WorkerAttemptState),
}

impl JournalDisposition {
    #[must_use]
    pub fn state(&self) -> &WorkerAttemptState {
        match self {
            Self::Applied(state) | Self::Replay(state) => state,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOutbox {
    pub outbox_id: Uuid,
    pub attempt_id: AttemptId,
    pub idempotency_key: String,
    pub payload: Value,
    pub payload_digest: Sha256Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationIdempotencyClass {
    Pure,
    Idempotent,
    QueryThenRetry,
    NonRepeatable,
}

impl OperationIdempotencyClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pure => "pure",
            Self::Idempotent => "idempotent",
            Self::QueryThenRetry => "query_then_retry",
            Self::NonRepeatable => "non_repeatable",
        }
    }

    fn parse(value: &str) -> JournalResult<Self> {
        match value {
            "pure" => Ok(Self::Pure),
            "idempotent" => Ok(Self::Idempotent),
            "query_then_retry" => Ok(Self::QueryThenRetry),
            "non_repeatable" => Ok(Self::NonRepeatable),
            _ => Err(JournalError::Integrity),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationPlan {
    pub operation_id: Uuid,
    pub attempt_id: AttemptId,
    pub idempotency_key: ProtocolKey,
    pub kind: ProtocolKey,
    pub idempotency_class: OperationIdempotencyClass,
    pub request_digest: Sha256Digest,
    pub planned_at: ServerInstant,
    pub deadline_at: ServerInstant,
}

impl OperationPlan {
    fn validate(&self) -> JournalResult<()> {
        if self.operation_id.is_nil()
            || self.attempt_id.as_uuid().is_nil()
            || self.request_digest.as_bytes().iter().all(|byte| *byte == 0)
            || self.deadline_at <= self.planned_at
        {
            return Err(JournalError::Runtime(WorkerError::InvalidArgument(
                "operation_plan",
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationPlanDisposition {
    Planned,
    Existing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationCompletion {
    pub operation_id: Uuid,
    pub result_digest: Sha256Digest,
    pub finished_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOperation {
    pub plan: OperationPlan,
}

impl OperationCompletion {
    fn validate(&self) -> JournalResult<()> {
        if self.operation_id.is_nil() || self.result_digest.as_bytes().iter().all(|byte| *byte == 0)
        {
            return Err(JournalError::Runtime(WorkerError::InvalidArgument(
                "operation_completion",
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error(transparent)]
    Runtime(#[from] WorkerError),
    #[error("SQLite Journal operation failed")]
    Sqlite(#[source] rusqlite::Error),
    #[error("SQLite Journal serialization failed")]
    Serialization,
    #[error("SQLite Journal schema version is unsupported")]
    UnsupportedSchema,
    #[error("SQLite Journal integrity verification failed")]
    Integrity,
    #[error("idempotency key was reused with a different command")]
    IdempotencyKeyReused,
    #[cfg(test)]
    #[error("injected Journal crash")]
    InjectedCrash,
}

fn load_operation_by_key(
    connection: &Connection,
    attempt_id: AttemptId,
    idempotency_key: &str,
) -> JournalResult<Option<OperationPlan>> {
    connection
        .query_row(
            "SELECT operation_id, attempt_id, idempotency_key, kind, idempotency_class, \
                    request_digest, planned_at, deadline_at \
             FROM operations WHERE attempt_id = ?1 AND idempotency_key = ?2",
            params![attempt_id.to_string(), idempotency_key],
            raw_operation_row,
        )
        .optional()?
        .map(decode_operation)
        .transpose()
}

fn validate_pending_completion(
    transaction: &Transaction<'_>,
    attempt_id: AttemptId,
    request: &JournalRequest,
    completion: &OperationCompletion,
) -> JournalResult<()> {
    completion.validate()?;
    let JournalCommand::Apply { command, .. } = &request.command else {
        return Err(JournalError::Runtime(WorkerError::InvalidArgument(
            "operation_command",
        )));
    };
    if completion.finished_at != command.observed_at {
        return Err(JournalError::Runtime(WorkerError::TimeRegressed));
    }
    let row = transaction
        .query_row(
            "SELECT attempt_id, idempotency_key, state, planned_at, deadline_at \
             FROM operations WHERE operation_id = ?1",
            [completion.operation_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or(JournalError::Integrity)?;
    if AttemptId::from_str(&row.0).map_err(|_| JournalError::Integrity)? != attempt_id
        || row.1 != request.idempotency_key.as_str()
        || row.2 != "pending"
        || completion.finished_at < parse_instant(&row.3)?
        || completion.finished_at > parse_instant(&row.4)?
    {
        return Err(JournalError::Integrity);
    }
    Ok(())
}

fn finish_operation(
    transaction: &Transaction<'_>,
    completion: &OperationCompletion,
) -> JournalResult<()> {
    let changed = transaction.execute(
        "UPDATE operations SET state = 'completed', result_digest = ?2, finished_at = ?3 \
         WHERE operation_id = ?1 AND state = 'pending'",
        params![
            completion.operation_id.to_string(),
            completion.result_digest.to_string(),
            instant_text(completion.finished_at),
        ],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(JournalError::Integrity)
    }
}

struct RawOperation {
    operation_id: String,
    attempt_id: String,
    idempotency_key: String,
    kind: String,
    idempotency_class: String,
    request_digest: String,
    planned_at: String,
    deadline_at: String,
}

fn raw_operation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawOperation> {
    Ok(RawOperation {
        operation_id: row.get(0)?,
        attempt_id: row.get(1)?,
        idempotency_key: row.get(2)?,
        kind: row.get(3)?,
        idempotency_class: row.get(4)?,
        request_digest: row.get(5)?,
        planned_at: row.get(6)?,
        deadline_at: row.get(7)?,
    })
}

fn decode_operation(raw: RawOperation) -> JournalResult<OperationPlan> {
    let plan = OperationPlan {
        operation_id: Uuid::parse_str(&raw.operation_id).map_err(|_| JournalError::Integrity)?,
        attempt_id: AttemptId::from_str(&raw.attempt_id).map_err(|_| JournalError::Integrity)?,
        idempotency_key: ProtocolKey::new(raw.idempotency_key)
            .map_err(|_| JournalError::Integrity)?,
        kind: ProtocolKey::new(raw.kind).map_err(|_| JournalError::Integrity)?,
        idempotency_class: OperationIdempotencyClass::parse(&raw.idempotency_class)?,
        request_digest: Sha256Digest::from_str(&raw.request_digest)
            .map_err(|_| JournalError::Integrity)?,
        planned_at: parse_instant(&raw.planned_at)?,
        deadline_at: parse_instant(&raw.deadline_at)?,
    };
    plan.validate()?;
    Ok(plan)
}

impl JournalError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Runtime(error) => error.code(),
            Self::Sqlite(_) => "AF_WORKER_JOURNAL_UNAVAILABLE",
            Self::Serialization => "AF_SERIALIZATION",
            Self::UnsupportedSchema => "AF_WORKER_JOURNAL_SCHEMA_UNSUPPORTED",
            Self::Integrity => "AF_WORKER_JOURNAL_INTEGRITY",
            Self::IdempotencyKeyReused => "AF_IDEMPOTENCY_KEY_REUSED",
            #[cfg(test)]
            Self::InjectedCrash => "AF_TEST_CRASH_INJECTED",
        }
    }
}

impl From<rusqlite::Error> for JournalError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

pub type JournalResult<T> = Result<T, JournalError>;

/// Single-writer Journal handle. Mutating operations require `&mut self`, so a
/// daemon can own it in exactly one actor and expose reads through messages.
pub struct Journal {
    connection: Connection,
}

impl Journal {
    pub fn open(path: impl AsRef<Path>) -> JournalResult<Self> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        let mode: String =
            connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(JournalError::Integrity);
        }
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "wal_autocheckpoint", 1_000_i64)?;
        let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        if synchronous != 2 {
            return Err(JournalError::Integrity);
        }
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        match version {
            0 => {
                connection.execute_batch("BEGIN IMMEDIATE")?;
                let initialized = connection
                    .execute_batch(SCHEMA)
                    .and_then(|()| {
                        connection.pragma_update(None, "user_version", JOURNAL_SCHEMA_VERSION)
                    })
                    .and_then(|()| connection.execute_batch("COMMIT"));
                if let Err(error) = initialized {
                    let _ = connection.execute_batch("ROLLBACK");
                    return Err(error.into());
                }
            }
            JOURNAL_SCHEMA_VERSION => {}
            _ => return Err(JournalError::UnsupportedSchema),
        }
        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(JournalError::Integrity);
        }
        Ok(Self { connection })
    }

    pub fn handle(&mut self, request: &JournalRequest) -> JournalResult<JournalDisposition> {
        self.handle_inner(request, None, CrashPoint::None)
    }

    pub fn plan_operation(
        &mut self,
        plan: &OperationPlan,
    ) -> JournalResult<OperationPlanDisposition> {
        plan.validate()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            load_operation_by_key(&transaction, plan.attempt_id, plan.idempotency_key.as_str())?
        {
            if existing == *plan {
                transaction.commit()?;
                return Ok(OperationPlanDisposition::Existing);
            }
            return Err(JournalError::IdempotencyKeyReused);
        }
        if transaction
            .query_row(
                "SELECT 1 FROM operations WHERE operation_id = ?1",
                [plan.operation_id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(JournalError::IdempotencyKeyReused);
        }
        let state = load_state(&transaction, plan.attempt_id)?
            .ok_or(JournalError::Runtime(WorkerError::HistoryEmpty))?;
        if state.phase().is_terminal() {
            return Err(JournalError::Runtime(WorkerError::InvalidTransition));
        }
        transaction.execute(
            "INSERT INTO operations \
             (operation_id, attempt_id, idempotency_key, kind, idempotency_class, state, \
              request_digest, planned_at, deadline_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?7, ?8)",
            params![
                plan.operation_id.to_string(),
                plan.attempt_id.to_string(),
                plan.idempotency_key.as_str(),
                plan.kind.as_str(),
                plan.idempotency_class.as_str(),
                plan.request_digest.to_string(),
                instant_text(plan.planned_at),
                instant_text(plan.deadline_at),
            ],
        )?;
        transaction.commit()?;
        Ok(OperationPlanDisposition::Planned)
    }

    pub fn complete_operation(
        &mut self,
        request: &JournalRequest,
        completion: &OperationCompletion,
    ) -> JournalResult<JournalDisposition> {
        self.handle_inner(request, Some(completion), CrashPoint::None)
    }

    pub fn pending_operations(
        &self,
        attempt_id: AttemptId,
    ) -> JournalResult<Vec<PendingOperation>> {
        let mut statement = self.connection.prepare(
            "SELECT operation_id, attempt_id, idempotency_key, kind, idempotency_class, \
                    request_digest, planned_at, deadline_at \
             FROM operations WHERE attempt_id = ?1 AND state = 'pending' \
             ORDER BY planned_at, operation_id",
        )?;
        let rows = statement.query_map([attempt_id.to_string()], raw_operation_row)?;
        rows.map(|row| {
            Ok(PendingOperation {
                plan: decode_operation(row?)?,
            })
        })
        .collect()
    }

    pub fn load_attempt(&self, attempt_id: AttemptId) -> JournalResult<Option<WorkerAttemptState>> {
        load_state(&self.connection, attempt_id)
    }

    pub fn recover_nonterminal(&self) -> JournalResult<Vec<WorkerAttemptState>> {
        let mut statement = self.connection.prepare(
            "SELECT attempt_id FROM attempts \
             WHERE phase NOT IN ('author_complete', 'local_failed', 'local_cancelled') \
             ORDER BY attempt_id",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let attempt_ids = rows
            .map(|row| AttemptId::from_str(&row?).map_err(|_| JournalError::Integrity))
            .collect::<JournalResult<Vec<_>>>()?;
        drop(statement);
        attempt_ids
            .into_iter()
            .map(|attempt_id| self.verify_attempt(attempt_id))
            .collect()
    }

    pub fn pending_outbox(&self, limit: u16) -> JournalResult<Vec<PendingOutbox>> {
        if limit == 0 || limit > 1_000 {
            return Err(JournalError::Runtime(WorkerError::InvalidArgument(
                "outbox_limit",
            )));
        }
        let mut statement = self.connection.prepare(
            "SELECT outbox_id, attempt_id, idempotency_key, payload_json, payload_digest \
             FROM outbox WHERE delivered_at IS NULL \
             ORDER BY available_at, outbox_id LIMIT ?1",
        )?;
        let rows = statement.query_map([i64::from(limit)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (outbox_id, attempt_id, key, payload, digest) = row?;
            let payload: Value =
                serde_json::from_str(&payload).map_err(|_| JournalError::Integrity)?;
            let payload_digest =
                Sha256Digest::from_str(&digest).map_err(|_| JournalError::Integrity)?;
            if digest_json(&payload)? != payload_digest {
                return Err(JournalError::Integrity);
            }
            Ok(PendingOutbox {
                outbox_id: Uuid::parse_str(&outbox_id).map_err(|_| JournalError::Integrity)?,
                attempt_id: AttemptId::from_str(&attempt_id)
                    .map_err(|_| JournalError::Integrity)?,
                idempotency_key: key,
                payload,
                payload_digest,
            })
        })
        .collect()
    }

    pub fn mark_outbox_delivered(
        &mut self,
        outbox_id: Uuid,
        delivered_at: agentforge_domain::ServerInstant,
    ) -> JournalResult<bool> {
        let changed = self.connection.execute(
            "UPDATE outbox SET delivered_at = ?2, delivery_attempts = delivery_attempts + 1, \
                    last_error_code = NULL \
             WHERE outbox_id = ?1 AND delivered_at IS NULL",
            params![outbox_id.to_string(), instant_text(delivered_at)],
        )?;
        Ok(changed == 1)
    }

    pub fn verify_attempt(&self, attempt_id: AttemptId) -> JournalResult<WorkerAttemptState> {
        let mut statement = self.connection.prepare(
            "SELECT seq, event_id, fact_json, fact_digest, previous_entry_digest, entry_digest \
             FROM journal_entries WHERE attempt_id = ?1 ORDER BY seq",
        )?;
        let rows = statement.query_map([attempt_id.to_string()], |row| {
            Ok(EntryRow {
                seq: row.get(0)?,
                event_id: row.get(1)?,
                fact_json: row.get(2)?,
                fact_digest: row.get(3)?,
                previous_entry_digest: row.get(4)?,
                entry_digest: row.get(5)?,
            })
        })?;
        let mut previous = None;
        let mut facts = Vec::new();
        for row in rows {
            let row = row?;
            let expected_seq =
                i64::try_from(facts.len() + 1).map_err(|_| JournalError::Integrity)?;
            if row.seq != expected_seq || row.previous_entry_digest != previous {
                return Err(JournalError::Integrity);
            }
            let fact: WorkerFact = decode_stored(&row.fact_json)?;
            let fact_digest = digest_json(&fact)?;
            if fact_digest.to_string() != row.fact_digest {
                return Err(JournalError::Integrity);
            }
            let event_id = Uuid::parse_str(&row.event_id).map_err(|_| JournalError::Integrity)?;
            let entry_digest = journal_entry_digest(
                attempt_id,
                u64::try_from(row.seq).map_err(|_| JournalError::Integrity)?,
                event_id,
                fact_digest,
                previous.as_deref(),
            )?;
            if entry_digest.to_string() != row.entry_digest {
                return Err(JournalError::Integrity);
            }
            previous = Some(row.entry_digest);
            facts.push(fact);
        }
        let replayed = WorkerAttemptState::replay(&facts)?;
        let stored = self
            .load_attempt(attempt_id)?
            .ok_or(JournalError::Integrity)?;
        if replayed != stored {
            return Err(JournalError::Integrity);
        }
        Ok(stored)
    }

    fn handle_inner(
        &mut self,
        request: &JournalRequest,
        completion: Option<&OperationCompletion>,
        crash: CrashPoint,
    ) -> JournalResult<JournalDisposition> {
        if request.message_id.is_nil() || request.actor_id.as_uuid().is_nil() {
            return Err(JournalError::Runtime(WorkerError::InvalidArgument(
                "request_identity",
            )));
        }
        let request_digest = digest_json(&serde_json::json!({
            "command": &request.command,
            "operation_completion": completion,
        }))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some((stored_digest, response, response_digest)) = transaction
            .query_row(
                "SELECT request_digest, response_json, response_digest FROM inbox \
                 WHERE actor_id = ?1 AND idempotency_key = ?2",
                params![
                    request.actor_id.to_string(),
                    request.idempotency_key.as_str()
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
        {
            if stored_digest != request_digest.to_string() {
                return Err(JournalError::IdempotencyKeyReused);
            }
            let state: WorkerAttemptState = decode_stored(&response)?;
            state.validate()?;
            if digest_json(&state)?.to_string() != response_digest {
                return Err(JournalError::Integrity);
            }
            transaction.commit()?;
            return Ok(JournalDisposition::Replay(state));
        }

        let request_attempt_id = match &request.command {
            JournalCommand::Grant { grant } => grant.attempt_id,
            JournalCommand::Apply { attempt_id, .. } => *attempt_id,
        };
        if let Some(completion) = completion {
            validate_pending_completion(&transaction, request_attempt_id, request, completion)?;
        }

        let (state, fact, initial) = match &request.command {
            JournalCommand::Grant { grant } => {
                if load_state(&transaction, grant.attempt_id)?.is_some() {
                    return Err(JournalError::Runtime(WorkerError::InvalidTransition));
                }
                let fact = grant_fact(grant.clone())?;
                let state = WorkerAttemptState::replay(std::slice::from_ref(&fact))?;
                insert_attempt(&transaction, &state)?;
                (state, fact, true)
            }
            JournalCommand::Apply {
                attempt_id,
                command,
            } => {
                let current = load_state(&transaction, *attempt_id)?
                    .ok_or(JournalError::Runtime(WorkerError::HistoryEmpty))?;
                let transition = current.transition(command)?;
                (transition.aggregate, transition.fact, false)
            }
        };

        append_fact(&transaction, request.message_id, &state, &fact)?;
        crash.hit(CrashPoint::AfterJournal)?;
        if !initial {
            update_attempt(&transaction, &state)?;
        }
        crash.hit(CrashPoint::AfterProjection)?;
        if let Some(completion) = completion {
            finish_operation(&transaction, completion)?;
        }
        crash.hit(CrashPoint::AfterOperation)?;
        insert_outbox(&transaction, request.message_id, &state, &fact)?;
        crash.hit(CrashPoint::AfterOutbox)?;
        insert_receipt(&transaction, request, request_digest, &state)?;
        crash.hit(CrashPoint::AfterReceipt)?;
        transaction.commit()?;
        Ok(JournalDisposition::Applied(state))
    }

    #[cfg(test)]
    fn handle_with_crash(
        &mut self,
        request: &JournalRequest,
        crash: CrashPoint,
    ) -> JournalResult<JournalDisposition> {
        self.handle_inner(request, None, crash)
    }

    #[cfg(test)]
    fn complete_with_crash(
        &mut self,
        request: &JournalRequest,
        completion: &OperationCompletion,
        crash: CrashPoint,
    ) -> JournalResult<JournalDisposition> {
        self.handle_inner(request, Some(completion), crash)
    }
}

fn insert_attempt(transaction: &Transaction<'_>, state: &WorkerAttemptState) -> JournalResult<()> {
    let state_json = encode(state)?;
    let state_digest = digest_json(state)?;
    let changed = transaction.execute(
        "INSERT INTO attempts \
         (attempt_id, package_id, package_revision, package_hash, lease_id, lease_generation, \
          phase, version, journal_seq, state_json, state_digest, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            state.attempt_id.to_string(),
            state.package_id.to_string(),
            i64::from(state.package_revision.get()),
            state.package_hash.to_string(),
            state.lease_id.to_string(),
            u64_to_i64(state.lease_generation.get())?,
            state.phase.as_str(),
            u64_to_i64(state.version.get())?,
            u64_to_i64(state.journal_seq)?,
            state_json,
            state_digest.to_string(),
            instant_text(state.created_at),
            instant_text(state.updated_at),
        ],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(JournalError::Integrity)
    }
}

fn update_attempt(transaction: &Transaction<'_>, state: &WorkerAttemptState) -> JournalResult<()> {
    let previous_version = state
        .version
        .get()
        .checked_sub(1)
        .ok_or(JournalError::Integrity)?;
    let previous_seq = state
        .journal_seq
        .checked_sub(1)
        .ok_or(JournalError::Integrity)?;
    let state_json = encode(state)?;
    let state_digest = digest_json(state)?;
    let changed = transaction.execute(
        "UPDATE attempts SET phase = ?2, version = ?3, journal_seq = ?4, state_json = ?5, \
                state_digest = ?6, updated_at = ?7 \
         WHERE attempt_id = ?1 AND version = ?8 AND journal_seq = ?9",
        params![
            state.attempt_id.to_string(),
            state.phase.as_str(),
            u64_to_i64(state.version.get())?,
            u64_to_i64(state.journal_seq)?,
            state_json,
            state_digest.to_string(),
            instant_text(state.updated_at),
            u64_to_i64(previous_version)?,
            u64_to_i64(previous_seq)?,
        ],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(JournalError::Integrity)
    }
}

fn append_fact(
    transaction: &Transaction<'_>,
    event_id: Uuid,
    state: &WorkerAttemptState,
    fact: &WorkerFact,
) -> JournalResult<()> {
    let previous = transaction
        .query_row(
            "SELECT entry_digest FROM journal_entries \
             WHERE attempt_id = ?1 ORDER BY seq DESC LIMIT 1",
            [state.attempt_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let fact_json = encode(fact)?;
    let fact_digest = digest_json(fact)?;
    let entry_digest = journal_entry_digest(
        state.attempt_id,
        state.journal_seq,
        event_id,
        fact_digest,
        previous.as_deref(),
    )?;
    transaction.execute(
        "INSERT INTO journal_entries \
         (attempt_id, seq, event_id, fact_json, fact_digest, previous_entry_digest, \
          entry_digest, occurred_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            state.attempt_id.to_string(),
            u64_to_i64(state.journal_seq)?,
            event_id.to_string(),
            fact_json,
            fact_digest.to_string(),
            previous,
            entry_digest.to_string(),
            instant_text(fact.observed_at),
        ],
    )?;
    Ok(())
}

fn insert_outbox(
    transaction: &Transaction<'_>,
    outbox_id: Uuid,
    state: &WorkerAttemptState,
    fact: &WorkerFact,
) -> JournalResult<()> {
    let payload = serde_json::json!({
        "schema": "af-worker-event/1",
        "attempt_id": state.attempt_id,
        "attempt_seq": state.journal_seq,
        "lease_generation": state.lease_generation,
        "fact": fact,
    });
    let payload_json = encode(&payload)?;
    let payload_digest = digest_json(&payload)?;
    transaction.execute(
        "INSERT INTO outbox \
         (outbox_id, attempt_id, destination, idempotency_key, payload_json, payload_digest, \
          available_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            outbox_id.to_string(),
            state.attempt_id.to_string(),
            OUTBOX_DESTINATION,
            format!("attempt:{}:seq:{}", state.attempt_id, state.journal_seq),
            payload_json,
            payload_digest.to_string(),
            instant_text(fact.observed_at),
        ],
    )?;
    Ok(())
}

fn insert_receipt(
    transaction: &Transaction<'_>,
    request: &JournalRequest,
    request_digest: Sha256Digest,
    state: &WorkerAttemptState,
) -> JournalResult<()> {
    let response_json = encode(state)?;
    let response_digest = digest_json(state)?;
    transaction.execute(
        "INSERT INTO inbox \
         (actor_id, idempotency_key, message_id, request_digest, response_json, \
          response_digest, received_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            request.actor_id.to_string(),
            request.idempotency_key.as_str(),
            request.message_id.to_string(),
            request_digest.to_string(),
            response_json,
            response_digest.to_string(),
            instant_text(state.updated_at),
        ],
    )?;
    Ok(())
}

fn load_state(
    connection: &Connection,
    attempt_id: AttemptId,
) -> JournalResult<Option<WorkerAttemptState>> {
    let row = connection
        .query_row(
            "SELECT phase, version, journal_seq, state_json, state_digest \
             FROM attempts WHERE attempt_id = ?1",
            [attempt_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((phase, version, seq, state_json, stored_digest)) = row else {
        return Ok(None);
    };
    let state: WorkerAttemptState = decode_stored(&state_json)?;
    state.validate()?;
    if state.attempt_id != attempt_id
        || state.phase.as_str() != phase
        || i64::try_from(state.version.get()).ok() != Some(version)
        || i64::try_from(state.journal_seq).ok() != Some(seq)
        || digest_json(&state)?.to_string() != stored_digest
    {
        return Err(JournalError::Integrity);
    }
    Ok(Some(state))
}

fn journal_entry_digest(
    attempt_id: AttemptId,
    seq: u64,
    event_id: Uuid,
    fact_digest: Sha256Digest,
    previous_entry_digest: Option<&str>,
) -> JournalResult<Sha256Digest> {
    digest_json(&serde_json::json!({
        "attempt_id": attempt_id,
        "seq": seq,
        "event_id": event_id,
        "fact_digest": fact_digest,
        "previous_entry_digest": previous_entry_digest,
    }))
}

fn encode<T: Serialize>(value: &T) -> JournalResult<String> {
    serde_json::to_string(value).map_err(|_| JournalError::Serialization)
}

fn decode_stored<T: DeserializeOwned>(value: &str) -> JournalResult<T> {
    serde_json::from_str(value).map_err(|_| JournalError::Integrity)
}

fn digest_json<T: Serialize>(value: &T) -> JournalResult<Sha256Digest> {
    let bytes = serde_json_canonicalizer::to_vec(value).map_err(|_| JournalError::Serialization)?;
    Ok(Sha256Digest::of_bytes(bytes))
}

fn instant_text(value: agentforge_domain::ServerInstant) -> String {
    value.0.unix_timestamp_nanos().to_string()
}

fn parse_instant(value: &str) -> JournalResult<ServerInstant> {
    let nanos = value.parse::<i128>().map_err(|_| JournalError::Integrity)?;
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map(ServerInstant)
        .map_err(|_| JournalError::Integrity)
}

fn u64_to_i64(value: u64) -> JournalResult<i64> {
    i64::try_from(value).map_err(|_| JournalError::Integrity)
}

struct EntryRow {
    seq: i64,
    event_id: String,
    fact_json: String,
    fact_digest: String,
    previous_entry_digest: Option<String>,
    entry_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    None,
    AfterJournal,
    AfterProjection,
    AfterOperation,
    AfterOutbox,
    AfterReceipt,
}

impl CrashPoint {
    fn hit(self, current: Self) -> JournalResult<()> {
        #[cfg(test)]
        if self == current {
            return Err(JournalError::InjectedCrash);
        }
        let _ = current;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use agentforge_domain::{
        AggregateVersion, FencingToken, GitObjectId, LeaseId, PackageId, PackageRevision,
        ServerInstant,
    };
    use tempfile::TempDir;
    use time::macros::datetime;

    use super::*;
    use crate::runtime::{WorkerCommandKind, WorkerPhase};

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(second: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(second))
    }

    fn fixture() -> (TempDir, Journal) {
        let directory = tempfile::tempdir().expect("temporary journal");
        let journal = Journal::open(directory.path().join("worker.sqlite3")).expect("journal");
        (directory, journal)
    }

    fn grant() -> AttemptGrant {
        AttemptGrant {
            attempt_id: id(1),
            package_id: PackageId::from_uuid(Uuid::from_bytes([2; 16])),
            package_revision: PackageRevision::new(1).expect("revision"),
            package_hash: Sha256Digest::of_bytes("package"),
            base_commit: GitObjectId::new("1".repeat(40)).expect("commit"),
            lease_id: LeaseId::from_uuid(Uuid::from_bytes([3; 16])),
            lease_generation: FencingToken::new(4).expect("generation"),
            lease_expires_at: at(60),
            granted_at: at(0),
        }
    }

    fn request(key: &str, message: u8, command: JournalCommand) -> JournalRequest {
        JournalRequest {
            message_id: Uuid::from_bytes([message; 16]),
            actor_id: id(9),
            idempotency_key: IdempotencyKey::new(key).expect("key"),
            command,
        }
    }

    fn grant_request() -> JournalRequest {
        request("grant-1", 10, JournalCommand::Grant { grant: grant() })
    }

    fn apply_request(
        state: &WorkerAttemptState,
        key: &str,
        message: u8,
        second: i64,
        command: WorkerCommandKind,
    ) -> JournalRequest {
        request(
            key,
            message,
            JournalCommand::Apply {
                attempt_id: state.attempt_id,
                command: WorkerCommandEnvelope {
                    expected_version: state.version,
                    observed_at: at(second),
                    command,
                },
            },
        )
    }

    fn bring_to_implementing(journal: &mut Journal) -> WorkerAttemptState {
        let mut state = journal
            .handle(&grant_request())
            .expect("grant")
            .state()
            .clone();
        for (key, message, second, command) in [
            ("prepare-1", 11, 1, WorkerCommandKind::BeginPreparation),
            (
                "workspace-1",
                12,
                2,
                WorkerCommandKind::WorkspacePrepared {
                    workspace_digest: Sha256Digest::of_bytes("workspace"),
                },
            ),
            (
                "baseline-1",
                13,
                3,
                WorkerCommandKind::BaselineFinished {
                    passed: true,
                    evidence_digest: Sha256Digest::of_bytes("baseline"),
                    failure_code: None,
                },
            ),
            (
                "plan-1",
                14,
                4,
                WorkerCommandKind::PlanAccepted {
                    plan_digest: Sha256Digest::of_bytes("plan"),
                },
            ),
        ] {
            state = journal
                .handle(&apply_request(&state, key, message, second, command))
                .expect("advance")
                .state()
                .clone();
        }
        assert_eq!(state.phase, WorkerPhase::Implementing);
        state
    }

    #[test]
    fn journal_is_wal_full_hash_chained_and_receipt_first() {
        let (_directory, mut journal) = fixture();
        let request = grant_request();
        let applied = journal.handle(&request).expect("grant applied");
        assert!(matches!(applied, JournalDisposition::Applied(_)));
        assert_eq!(applied.state().phase, WorkerPhase::Granted);
        assert!(matches!(
            journal.handle(&request).expect("exact replay"),
            JournalDisposition::Replay(_)
        ));
        assert_eq!(journal.pending_outbox(10).expect("outbox").len(), 1);
        assert_eq!(
            journal
                .verify_attempt(grant().attempt_id)
                .expect("verified")
                .journal_seq,
            1
        );

        let mut changed = request;
        changed.command = JournalCommand::Grant {
            grant: AttemptGrant {
                lease_expires_at: at(61),
                ..grant()
            },
        };
        assert_eq!(
            journal
                .handle(&changed)
                .expect_err("changed command cannot replay")
                .code(),
            "AF_IDEMPOTENCY_KEY_REUSED"
        );
    }

    #[test]
    fn every_transaction_crash_point_rolls_back_before_exact_retry() {
        for (index, crash) in [
            CrashPoint::AfterJournal,
            CrashPoint::AfterProjection,
            CrashPoint::AfterOperation,
            CrashPoint::AfterOutbox,
            CrashPoint::AfterReceipt,
        ]
        .into_iter()
        .enumerate()
        {
            let (directory, mut journal) = fixture();
            let request = grant_request();
            assert_eq!(
                journal
                    .handle_with_crash(&request, crash)
                    .expect_err("crash injected")
                    .code(),
                "AF_TEST_CRASH_INJECTED"
            );
            drop(journal);
            let mut reopened =
                Journal::open(directory.path().join("worker.sqlite3")).expect("reopen");
            assert!(
                reopened
                    .load_attempt(grant().attempt_id)
                    .expect("load")
                    .is_none()
            );
            assert!(reopened.pending_outbox(10).expect("outbox").is_empty());
            assert!(matches!(
                reopened.handle(&request).expect("retry"),
                JournalDisposition::Applied(_)
            ));
            assert_eq!(
                reopened.pending_outbox(10).expect("outbox").len(),
                1,
                "crash point {index}"
            );
        }
    }

    #[test]
    fn committed_grant_survives_every_later_command_crash_boundary() {
        for crash in [
            CrashPoint::AfterJournal,
            CrashPoint::AfterProjection,
            CrashPoint::AfterOperation,
            CrashPoint::AfterOutbox,
            CrashPoint::AfterReceipt,
        ] {
            let (directory, mut journal) = fixture();
            let granted = journal
                .handle(&grant_request())
                .expect("grant")
                .state()
                .clone();
            let prepare = request(
                "prepare-1",
                11,
                JournalCommand::Apply {
                    attempt_id: granted.attempt_id,
                    command: WorkerCommandEnvelope {
                        expected_version: granted.version,
                        observed_at: at(1),
                        command: WorkerCommandKind::BeginPreparation,
                    },
                },
            );
            assert!(journal.handle_with_crash(&prepare, crash).is_err());
            drop(journal);

            let mut reopened =
                Journal::open(directory.path().join("worker.sqlite3")).expect("reopen");
            let unchanged = reopened
                .verify_attempt(granted.attempt_id)
                .expect("verified grant");
            assert_eq!(unchanged.phase, WorkerPhase::Granted);
            assert_eq!(unchanged.version, AggregateVersion::new(1));
            assert_eq!(reopened.pending_outbox(10).expect("outbox").len(), 1);
            let applied = reopened.handle(&prepare).expect("exact retry");
            assert_eq!(applied.state().phase, WorkerPhase::Preparing);
            assert_eq!(reopened.pending_outbox(10).expect("outbox").len(), 2);
        }
    }

    #[test]
    fn non_repeatable_operation_is_planned_before_effect_and_completed_atomically() {
        let (directory, mut journal) = fixture();
        let state = bring_to_implementing(&mut journal);
        let plan = OperationPlan {
            operation_id: Uuid::from_bytes([21; 16]),
            attempt_id: state.attempt_id,
            idempotency_key: ProtocolKey::new("turn-1").expect("key"),
            kind: ProtocolKey::new("agent-turn").expect("kind"),
            idempotency_class: OperationIdempotencyClass::NonRepeatable,
            request_digest: Sha256Digest::of_bytes("turn request"),
            planned_at: at(5),
            deadline_at: at(30),
        };
        assert_eq!(
            journal.plan_operation(&plan).expect("plan"),
            OperationPlanDisposition::Planned
        );
        assert_eq!(
            journal.plan_operation(&plan).expect("exact plan replay"),
            OperationPlanDisposition::Existing
        );
        let mut changed = plan.clone();
        changed.request_digest = Sha256Digest::of_bytes("different request");
        assert_eq!(
            journal
                .plan_operation(&changed)
                .expect_err("operation key cannot be reused")
                .code(),
            "AF_IDEMPOTENCY_KEY_REUSED"
        );
        assert_eq!(
            journal
                .pending_operations(state.attempt_id)
                .expect("pending")
                .len(),
            1
        );

        let turn = apply_request(
            &state,
            "turn-1",
            22,
            6,
            WorkerCommandKind::TurnProducedChanges {
                turn_id: ProtocolKey::new("turn-1").expect("turn"),
                tree: GitObjectId::new("2".repeat(40)).expect("tree"),
                model_claimed_done: true,
            },
        );
        let completion = OperationCompletion {
            operation_id: plan.operation_id,
            result_digest: Sha256Digest::of_bytes("turn result"),
            finished_at: at(6),
        };
        assert!(
            journal
                .complete_with_crash(&turn, &completion, CrashPoint::AfterOperation)
                .is_err()
        );
        drop(journal);

        let mut reopened = Journal::open(directory.path().join("worker.sqlite3")).expect("reopen");
        assert_eq!(
            reopened
                .pending_operations(state.attempt_id)
                .expect("pending")
                .len(),
            1
        );
        assert_eq!(
            reopened
                .load_attempt(state.attempt_id)
                .expect("load")
                .expect("attempt")
                .phase,
            WorkerPhase::Implementing
        );
        let completed = reopened
            .complete_operation(&turn, &completion)
            .expect("complete operation");
        assert_eq!(completed.state().phase, WorkerPhase::LocalVerifying);
        assert_eq!(completed.state().turns_completed, 1);
        assert!(
            reopened
                .pending_operations(state.attempt_id)
                .expect("pending")
                .is_empty()
        );
        assert!(matches!(
            reopened
                .complete_operation(&turn, &completion)
                .expect("ACK-loss replay"),
            JournalDisposition::Replay(_)
        ));
        let mut changed_completion = completion;
        changed_completion.result_digest = Sha256Digest::of_bytes("substituted result");
        assert_eq!(
            reopened
                .complete_operation(&turn, &changed_completion)
                .expect_err("completion digest is receipt-bound")
                .code(),
            "AF_IDEMPOTENCY_KEY_REUSED"
        );
    }

    #[test]
    fn immutable_ledger_triggers_and_state_digest_fail_closed() {
        let (_directory, mut journal) = fixture();
        let state = journal
            .handle(&grant_request())
            .expect("grant")
            .state()
            .clone();
        assert!(
            journal
                .connection
                .execute(
                    "UPDATE journal_entries SET fact_json = '{}' WHERE attempt_id = ?1",
                    [state.attempt_id.to_string()],
                )
                .is_err()
        );
        assert!(
            journal
                .connection
                .execute(
                    "UPDATE outbox SET payload_json = '{}' WHERE attempt_id = ?1",
                    [state.attempt_id.to_string()],
                )
                .is_err()
        );
        journal
            .connection
            .execute(
                "UPDATE attempts SET state_json = '{}' WHERE attempt_id = ?1",
                [state.attempt_id.to_string()],
            )
            .expect("projection corruption fixture");
        assert_eq!(
            journal
                .load_attempt(state.attempt_id)
                .expect_err("corrupt state must fail closed")
                .code(),
            "AF_WORKER_JOURNAL_INTEGRITY"
        );
    }

    #[test]
    fn recovery_replays_state_and_outbox_delivery_is_idempotent() {
        let (directory, mut journal) = fixture();
        let granted = journal
            .handle(&grant_request())
            .expect("grant")
            .state()
            .clone();
        let begin = request(
            "prepare-1",
            11,
            JournalCommand::Apply {
                attempt_id: granted.attempt_id,
                command: WorkerCommandEnvelope {
                    expected_version: AggregateVersion::new(1),
                    observed_at: at(1),
                    command: WorkerCommandKind::BeginPreparation,
                },
            },
        );
        let preparing = journal.handle(&begin).expect("prepare").state().clone();
        assert_eq!(preparing.phase, WorkerPhase::Preparing);
        assert_eq!(preparing.version, AggregateVersion::new(2));
        assert_eq!(journal.pending_outbox(10).expect("outbox").len(), 2);
        let first = journal.pending_outbox(1).expect("first").remove(0);
        assert!(
            journal
                .mark_outbox_delivered(first.outbox_id, at(2))
                .expect("deliver")
        );
        assert!(
            !journal
                .mark_outbox_delivered(first.outbox_id, at(3))
                .expect("duplicate delivery")
        );
        drop(journal);

        let reopened = Journal::open(directory.path().join("worker.sqlite3")).expect("reopen");
        assert_eq!(
            reopened.recover_nonterminal().expect("recover nonterminal"),
            vec![preparing.clone()]
        );
        assert_eq!(
            reopened
                .verify_attempt(preparing.attempt_id)
                .expect("hash chain"),
            preparing
        );
        assert_eq!(reopened.pending_outbox(10).expect("pending").len(), 1);
    }
}
