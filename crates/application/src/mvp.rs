//! Stable application contract for the first runnable AgentForge MVP.
//!
//! The HTTP layer and Worker daemon depend on this trait rather than on SQL.
//! A PostgreSQL adapter must implement every write as a receipt-first atomic
//! transaction over canonical typed rows, events, receipts, and Outbox facts.

use std::{future::Future, pin::Pin};

use agentforge_domain::{
    ActorId, AggregateVersion, AttemptId, CommandId, CommandMetadata, CorrelationId, EventId,
    ExecutorId, FencingToken, GitObjectId, IdempotencyKey, LeaseId, NodeId, PackageId,
    PackageRevision, PackageRevisionId, ProjectId, ProtocolKey, ServerInstant, Sha256Digest,
    lease::LeaseState, work_package::WorkPackageState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::PortError;

pub type MvpResult<T> = Result<T, MvpError>;
pub type MvpFuture<'a, T> = Pin<Box<dyn Future<Output = MvpResult<T>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MvpError {
    #[error(transparent)]
    Domain(#[from] agentforge_domain::DomainError),
    #[error(transparent)]
    Port(#[from] PortError),
    #[error("the idempotent result is no longer replayable")]
    IdempotencyResultExpired,
    #[error("the idempotent result predates the current response protocol")]
    IdempotencyResultLegacy,
}

impl MvpError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Domain(error) => error.code(),
            Self::Port(PortError::NotFound) => "AF_NOT_FOUND",
            Self::Port(PortError::Conflict) => "AF_CONFLICT",
            Self::Port(PortError::Unavailable) => "AF_UNAVAILABLE",
            Self::Port(PortError::Integrity) => "AF_STORAGE_INTEGRITY",
            Self::Port(PortError::Serialization) => "AF_SERIALIZATION",
            Self::IdempotencyResultExpired => "AF_IDEMPOTENCY_RESULT_EXPIRED",
            Self::IdempotencyResultLegacy => "AF_IDEMPOTENCY_RESULT_LEGACY",
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        matches!(self, Self::Port(error) if error.retryable())
    }
}

/// Transport-independent metadata for an externally visible command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MvpCommandContext {
    pub command_id: CommandId,
    pub actor_id: ActorId,
    pub idempotency_key: IdempotencyKey,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<EventId>,
    pub expected_version: Option<AggregateVersion>,
}

impl MvpCommandContext {
    pub fn metadata<T: Serialize>(&self, input: &T) -> MvpResult<CommandMetadata> {
        let bytes = serde_json_canonicalizer::to_vec(input)
            .map_err(|_| MvpError::Port(PortError::Serialization))?;
        Ok(CommandMetadata {
            command_id: self.command_id,
            actor_id: self.actor_id,
            idempotency_key: self.idempotency_key.clone(),
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            expected_version: self.expected_version,
            payload_digest: Sha256Digest::of_bytes(bytes),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MvpCommand<T> {
    pub context: MvpCommandContext,
    pub input: T,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateProjectInput {
    pub project_id: ProjectId,
    pub protocol_key: ProtocolKey,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectView {
    pub project_id: ProjectId,
    pub protocol_key: ProtocolKey,
    pub name: String,
    pub version: AggregateVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishPackageInput {
    pub project_id: ProjectId,
    pub package_id: PackageId,
    pub package_key: ProtocolKey,
    pub revision_id: PackageRevisionId,
    pub revision: PackageRevision,
    pub schema_version: String,
    pub canonical_document: Value,
    pub package_hash: Sha256Digest,
    pub base_commit: GitObjectId,
    pub git_object_format: String,
    pub input_snapshot: Value,
    pub created_by: ActorId,
    pub graph_version: u64,
    pub priority: i16,
    pub max_attempts: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishedPackage {
    pub project_id: ProjectId,
    pub package_id: PackageId,
    pub package_key: ProtocolKey,
    pub revision_id: PackageRevisionId,
    pub state: WorkPackageState,
    pub version: AggregateVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListOffersQuery {
    pub project_id: ProjectId,
    pub limit: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OfferView {
    pub project_id: ProjectId,
    pub package_id: PackageId,
    pub package_key: ProtocolKey,
    pub revision_id: PackageRevisionId,
    pub revision: PackageRevision,
    pub state: WorkPackageState,
    pub priority: i16,
    pub attempts_started: u16,
    pub max_attempts: u16,
    pub version: AggregateVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimPackageInput {
    pub project_id: ProjectId,
    pub package_id: PackageId,
    pub executor_id: ExecutorId,
    pub node_id: NodeId,
    pub lease_seconds: u32,
    pub max_lease_seconds: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageExecutionSnapshot {
    pub revision: PackageRevision,
    pub package_hash: Sha256Digest,
    pub base_commit: GitObjectId,
    pub git_object_format: String,
    pub canonical_document: Value,
    pub input_snapshot: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimedWork {
    pub project_id: ProjectId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub attempt_id: AttemptId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub max_expires_at: ServerInstant,
    pub package_version: AggregateVersion,
    pub attempt_version: AggregateVersion,
    pub lease_version: AggregateVersion,
    pub execution: PackageExecutionSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RenewLeaseInput {
    pub project_id: ProjectId,
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
    pub extend_by_seconds: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseLeaseInput {
    pub project_id: ProjectId,
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseView {
    pub project_id: ProjectId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub attempt_id: AttemptId,
    pub lease_id: LeaseId,
    pub holder_node_id: NodeId,
    pub fencing_token: FencingToken,
    pub state: LeaseState,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub max_expires_at: ServerInstant,
    pub updated_at: ServerInstant,
    pub version: AggregateVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReconcileExpiredLeasesQuery {
    pub project_id: ProjectId,
    pub limit: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseReconciliationReport {
    pub project_id: ProjectId,
    pub scanned: u16,
    pub expired: u16,
    pub conflicted: u16,
}

/// Versioned MVP command/query surface used by HTTP, CLI, and Worker adapters.
pub trait MvpControlPlane: Send + Sync {
    /// Reports whether the command service can safely accept MVP traffic.
    ///
    /// A durable adapter must verify its schema contract as well as basic
    /// connectivity; a successful socket connection alone is not sufficient.
    fn ready(&self) -> MvpFuture<'_, bool>;

    fn create_project<'a>(
        &'a self,
        command: &'a MvpCommand<CreateProjectInput>,
    ) -> MvpFuture<'a, ProjectView>;

    fn publish_package<'a>(
        &'a self,
        command: &'a MvpCommand<PublishPackageInput>,
    ) -> MvpFuture<'a, PublishedPackage>;

    fn list_offers(&self, query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>>;

    fn claim_package<'a>(
        &'a self,
        command: &'a MvpCommand<ClaimPackageInput>,
    ) -> MvpFuture<'a, ClaimedWork>;

    fn renew_lease<'a>(
        &'a self,
        command: &'a MvpCommand<RenewLeaseInput>,
    ) -> MvpFuture<'a, LeaseView>;

    fn release_lease<'a>(
        &'a self,
        command: &'a MvpCommand<ReleaseLeaseInput>,
    ) -> MvpFuture<'a, LeaseView>;

    fn get_lease(&self, project_id: ProjectId, lease_id: LeaseId) -> MvpFuture<'_, LeaseView>;

    /// Reconciles a bounded, database-clock-selected batch of expired Leases.
    /// Implementations must use the same Package -> Attempt -> Lease atomic
    /// terminalization path as an explicit Release.
    fn reconcile_expired_leases(
        &self,
        query: ReconcileExpiredLeasesQuery,
    ) -> MvpFuture<'_, LeaseReconciliationReport>;
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    #[test]
    fn command_digest_excludes_transport_metadata_and_is_key_order_stable() {
        let context = MvpCommandContext {
            command_id: id(1),
            actor_id: id(2),
            idempotency_key: IdempotencyKey::new("publish-1").expect("valid key"),
            correlation_id: id(3),
            causation_id: None,
            expected_version: None,
        };
        let first = serde_json::json!({"b": 2, "a": 1});
        let second = serde_json::json!({"a": 1, "b": 2});
        assert_eq!(
            context.metadata(&first).expect("metadata").payload_digest,
            context.metadata(&second).expect("metadata").payload_digest
        );
    }

    #[test]
    fn mvp_error_codes_are_stable_and_sanitized() {
        assert_eq!(MvpError::Port(PortError::Conflict).code(), "AF_CONFLICT");
        assert_eq!(
            MvpError::IdempotencyResultExpired.code(),
            "AF_IDEMPOTENCY_RESULT_EXPIRED"
        );
        assert!(!MvpError::Port(PortError::Integrity).retryable());
    }
}
