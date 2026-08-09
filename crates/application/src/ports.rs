//! Infrastructure-independent application ports.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};

use agentforge_domain::{
    ActorId, AggregateId, AggregateType, AggregateVersion, CorrelationId, DomainEventEnvelope,
    EventId, ProjectId, ServerInstant, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::projection::ProjectionCursor;

pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = PortResult<T>> + Send + 'a>>;
pub type PortResult<T> = Result<T, PortError>;

/// Sanitized failures shared by repository adapters.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PortError {
    #[error("requested record was not found")]
    NotFound,
    #[error("optimistic concurrency check failed")]
    Conflict,
    #[error("infrastructure is temporarily unavailable")]
    Unavailable,
    #[error("stored data failed an integrity check")]
    Integrity,
    #[error("adapter serialization failed")]
    Serialization,
}

impl PortError {
    #[must_use]
    pub const fn retryable(&self) -> bool {
        matches!(self, Self::Conflict | Self::Unavailable)
    }
}

/// Aggregate plus the authoritative version observed in the repository.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredAggregate<A> {
    pub aggregate: A,
    pub version: AggregateVersion,
}

/// Generic aggregate repository implemented on a transaction-bound unit of work.
///
/// A use case constrains its UoW with `Repository<WorkPackage>` or any later
/// aggregate type.  This avoids a central mega-trait that must change whenever
/// Invocation or Governance gains another aggregate.
pub trait Repository<A>: Send {
    type Id: Copy + Send + Sync;

    fn load<'a>(
        &'a mut self,
        project_id: ProjectId,
        id: Self::Id,
    ) -> PortFuture<'a, Option<StoredAggregate<A>>>;

    /// Persists a domain-produced aggregate using create (`None`) or CAS
    /// (`Some(version)`) semantics.  Repositories must not invoke domain
    /// transitions or silently merge stale states.
    fn save<'a>(
        &'a mut self,
        project_id: ProjectId,
        id: Self::Id,
        aggregate: &'a A,
        expected_version: Option<AggregateVersion>,
    ) -> PortFuture<'a, AggregateVersion>;
}

/// Fully materialized event row handed to an append-only adapter.
///
/// `payload` remains JSON because one transaction can append heterogeneous
/// domain event enums.  The digest is always validated before construction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub project_id: ProjectId,
    pub event_id: EventId,
    pub aggregate_type: AggregateType,
    pub aggregate_id: AggregateId,
    pub aggregate_version: AggregateVersion,
    pub aggregate_seq: u64,
    pub event_type: String,
    pub schema_version: u16,
    pub actor_id: ActorId,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<EventId>,
    pub occurred_at: ServerInstant,
    pub payload_digest: Sha256Digest,
    pub required_semantics: Vec<String>,
    pub payload: Value,
    pub optional_metadata: BTreeMap<String, Value>,
}

impl EventRecord {
    pub fn from_domain<E>(
        project_id: ProjectId,
        envelope: &DomainEventEnvelope<E>,
    ) -> PortResult<Self>
    where
        E: Serialize,
    {
        // Appending does not interpret required semantics, but it must still
        // reject a malformed envelope.  Treating the envelope's declared
        // semantics as supported lets `validate_for` perform all shape and
        // digest checks without making this persistence boundary a policy
        // engine.
        let declared_semantics = envelope
            .required_semantics
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        envelope
            .validate_for(u16::MAX, &declared_semantics)
            .map_err(|_| PortError::Integrity)?;
        Ok(Self {
            project_id,
            event_id: envelope.event_id,
            aggregate_type: envelope.aggregate_type,
            aggregate_id: envelope.aggregate_id,
            aggregate_version: envelope.aggregate_version,
            aggregate_seq: envelope.aggregate_seq,
            event_type: envelope.event_type.clone(),
            schema_version: envelope.schema_version,
            actor_id: envelope.actor_id,
            correlation_id: envelope.correlation_id,
            causation_id: envelope.causation_id,
            occurred_at: envelope.occurred_at,
            payload_digest: envelope.payload_digest,
            required_semantics: envelope.required_semantics.clone(),
            payload: serde_json::to_value(&envelope.payload)
                .map_err(|_| PortError::Serialization)?,
            optional_metadata: envelope.optional_metadata.clone(),
        })
    }

    #[must_use]
    pub const fn cursor(&self) -> ProjectionCursor {
        ProjectionCursor::new(self.occurred_at, self.event_id, self.aggregate_seq)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendEventsReceipt {
    pub appended: usize,
    pub first_cursor: Option<ProjectionCursor>,
    pub last_cursor: Option<ProjectionCursor>,
}

/// Event appends participate in the same transaction as aggregate rows.
pub trait EventAppendPort: Send {
    fn append_events<'a>(
        &'a mut self,
        events: &'a [EventRecord],
    ) -> PortFuture<'a, AppendEventsReceipt>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Isolation {
    ReadCommitted,
    Serializable,
}

/// Transaction lifecycle.  Repository capabilities are added as trait bounds
/// to individual use cases, while event append is mandatory for every UoW.
pub trait UnitOfWork: EventAppendPort + Send + Sized + 'static {
    fn commit(self) -> PortFuture<'static, ()>;
    fn rollback(self) -> PortFuture<'static, ()>;
}

pub trait UnitOfWorkFactory: Send + Sync {
    type Uow: UnitOfWork;

    fn begin(&self, isolation: Isolation) -> PortFuture<'_, Self::Uow>;
}

/// Authoritative server time.  Domain commands receive this value explicitly.
pub trait Clock: Send + Sync {
    fn now(&self) -> ServerInstant;
}

/// UUID creation boundary.  Callers immediately wrap the UUID in the required
/// strongly typed domain identifier.
pub trait IdGenerator: Send {
    fn next_uuid(&mut self) -> Uuid;
}
