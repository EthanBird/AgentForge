//! Stable application contract for the first runnable AgentForge MVP.
//!
//! The HTTP layer and Worker daemon depend on this trait rather than on SQL.
//! A PostgreSQL adapter must implement every write as a receipt-first atomic
//! transaction over canonical typed rows, events, receipts, and Outbox facts.

use std::{future::Future, pin::Pin};

use agentforge_domain::{
    ActorId, AggregateVersion, ArtifactRef, AttemptId, CandidateArtifactId, CandidateArtifactState,
    CandidateId, CommandId, CommandMetadata, CorrelationId, EventId, ExecutorId, FencingToken,
    GitObjectId, IdempotencyKey, LeaseId, NodeId, PackageId, PackageRevision, PackageRevisionId,
    ProjectId, ProtocolKey, ServerInstant, Sha256Digest, lease::LeaseState,
    work_package::WorkPackageState,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
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
    #[error(transparent)]
    Remote(#[from] MvpRemoteError),
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
            Self::Remote(error) => error.code(),
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self {
            Self::Domain(error) => error.retryable(),
            Self::Port(error) => error.retryable(),
            Self::Remote(error) => error.retryable(),
            Self::IdempotencyResultExpired | Self::IdempotencyResultLegacy => false,
        }
    }
}

/// Stable error codes accepted from the versioned HTTP adapter. Unknown codes
/// are not converted into this enum: clients fail closed as a serialization
/// error rather than trusting an unversioned server diagnostic.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MvpRemoteError {
    #[error("remote resource identifier is invalid")]
    ResourceIdInvalid,
    #[error("remote command header is invalid")]
    CommandHeaderInvalid,
    #[error("remote path and request body do not match")]
    PathBodyMismatch,
    #[error("remote Project access was denied")]
    ProjectAccessDenied,
    #[error("remote command service is unavailable")]
    CommandServiceUnavailable,
    #[error("remote resource was not found")]
    NotFound,
    #[error("remote Lease expired")]
    LeaseExpired,
    #[error("remote policy denied the command")]
    PolicyDenied,
    #[error("remote durable state conflicts with the command")]
    Conflict,
    #[error("remote aggregate version is stale")]
    StaleVersion,
    #[error("remote command argument is invalid")]
    ArgumentInvalid,
    #[error("remote Lease generation is stale")]
    LeaseStale,
    #[error("remote idempotency key was reused")]
    IdempotencyKeyReused,
    #[error("remote transition is invalid")]
    TransitionInvalid,
    #[error("remote Package is not claimable")]
    PackageNotClaimable,
    #[error("remote Package hash does not match")]
    PackageHashMismatch,
    #[error("remote evidence is invalid")]
    EvidenceInvalid,
    #[error("remote Candidate Artifact is not complete")]
    CandidateArtifactNotComplete,
    #[error("remote dependency is unavailable")]
    Unavailable,
    #[error("remote storage integrity check failed")]
    StorageIntegrity,
    #[error("remote serialization failed")]
    Serialization,
    #[error("remote idempotent result expired")]
    IdempotencyResultExpired,
    #[error("remote idempotent result uses a legacy protocol")]
    IdempotencyResultLegacy,
}

impl MvpRemoteError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ResourceIdInvalid => "AF_RESOURCE_ID_INVALID",
            Self::CommandHeaderInvalid => "AF_COMMAND_HEADER_INVALID",
            Self::PathBodyMismatch => "AF_PATH_BODY_MISMATCH",
            Self::ProjectAccessDenied => "AF_PROJECT_ACCESS_DENIED",
            Self::CommandServiceUnavailable => "AF_COMMAND_SERVICE_UNAVAILABLE",
            Self::NotFound => "AF_NOT_FOUND",
            Self::LeaseExpired => "AF_LEASE_EXPIRED",
            Self::PolicyDenied => "AF_POLICY_DENIED",
            Self::Conflict => "AF_CONFLICT",
            Self::StaleVersion => "AF_VERSION_STALE",
            Self::ArgumentInvalid => "AF_ARGUMENT_INVALID",
            Self::LeaseStale => "AF_LEASE_STALE",
            Self::IdempotencyKeyReused => "AF_IDEMPOTENCY_KEY_REUSED",
            Self::TransitionInvalid => "AF_TRANSITION_INVALID",
            Self::PackageNotClaimable => "AF_PACKAGE_NOT_CLAIMABLE",
            Self::PackageHashMismatch => "AF_PACKAGE_HASH_MISMATCH",
            Self::EvidenceInvalid => "AF_EVIDENCE_INVALID",
            Self::CandidateArtifactNotComplete => "AF_CANDIDATE_ARTIFACT_NOT_COMPLETE",
            Self::Unavailable => "AF_UNAVAILABLE",
            Self::StorageIntegrity => "AF_STORAGE_INTEGRITY",
            Self::Serialization => "AF_SERIALIZATION",
            Self::IdempotencyResultExpired => "AF_IDEMPOTENCY_RESULT_EXPIRED",
            Self::IdempotencyResultLegacy => "AF_IDEMPOTENCY_RESULT_LEGACY",
        }
    }

    pub fn parse(code: &str) -> Option<Self> {
        Some(match code {
            "AF_RESOURCE_ID_INVALID" => Self::ResourceIdInvalid,
            "AF_COMMAND_HEADER_INVALID" => Self::CommandHeaderInvalid,
            "AF_PATH_BODY_MISMATCH" => Self::PathBodyMismatch,
            "AF_PROJECT_ACCESS_DENIED" => Self::ProjectAccessDenied,
            "AF_COMMAND_SERVICE_UNAVAILABLE" => Self::CommandServiceUnavailable,
            "AF_NOT_FOUND" => Self::NotFound,
            "AF_LEASE_EXPIRED" => Self::LeaseExpired,
            "AF_POLICY_DENIED" => Self::PolicyDenied,
            "AF_CONFLICT" => Self::Conflict,
            "AF_VERSION_STALE" => Self::StaleVersion,
            "AF_ARGUMENT_INVALID" => Self::ArgumentInvalid,
            "AF_LEASE_STALE" => Self::LeaseStale,
            "AF_IDEMPOTENCY_KEY_REUSED" => Self::IdempotencyKeyReused,
            "AF_TRANSITION_INVALID" => Self::TransitionInvalid,
            "AF_PACKAGE_NOT_CLAIMABLE" => Self::PackageNotClaimable,
            "AF_PACKAGE_HASH_MISMATCH" => Self::PackageHashMismatch,
            "AF_EVIDENCE_INVALID" => Self::EvidenceInvalid,
            "AF_CANDIDATE_ARTIFACT_NOT_COMPLETE" => Self::CandidateArtifactNotComplete,
            "AF_UNAVAILABLE" => Self::Unavailable,
            "AF_STORAGE_INTEGRITY" => Self::StorageIntegrity,
            "AF_SERIALIZATION" => Self::Serialization,
            "AF_IDEMPOTENCY_RESULT_EXPIRED" => Self::IdempotencyResultExpired,
            "AF_IDEMPOTENCY_RESULT_LEGACY" => Self::IdempotencyResultLegacy,
            _ => return None,
        })
    }

    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::CommandServiceUnavailable
                | Self::PackageNotClaimable
                | Self::CandidateArtifactNotComplete
                | Self::Unavailable
        )
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
#[serde(deny_unknown_fields)]
pub struct ListOffersQuery {
    pub project_id: ProjectId,
    pub limit: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// Author-side reservation for one immutable Candidate bundle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitCandidateArtifactInput {
    pub project_id: ProjectId,
    pub attempt_id: AttemptId,
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
    pub package_hash: Sha256Digest,
    pub base_commit: GitObjectId,
    pub candidate_commit: GitObjectId,
    pub tree_hash: GitObjectId,
    pub author_evidence_digest: Sha256Digest,
    pub expected_bundle_digest: Sha256Digest,
    pub expected_bundle_size_bytes: u64,
    pub chunk_digests: Vec<Sha256Digest>,
    pub upload_ttl_seconds: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateArtifactView {
    pub project_id: ProjectId,
    pub artifact_id: CandidateArtifactId,
    pub candidate_id: CandidateId,
    pub attempt_id: AttemptId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub candidate_commit: GitObjectId,
    pub tree_hash: GitObjectId,
    pub state: CandidateArtifactState,
    pub expected_bundle_digest: Sha256Digest,
    pub expected_bundle_size_bytes: u64,
    pub chunk_digests: Vec<Sha256Digest>,
    pub bundle: Option<ArtifactRef>,
    pub created_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub updated_at: ServerInstant,
    pub version: AggregateVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadCandidateArtifactChunkInput {
    pub project_id: ProjectId,
    pub artifact_id: CandidateArtifactId,
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
    pub chunk_index: u32,
    pub digest: Sha256Digest,
    #[serde(with = "canonical_base64_bytes")]
    pub content: Vec<u8>,
}

mod canonical_base64_bytes {
    use super::{BASE64_STANDARD, Deserialize, Serialize};
    use base64::Engine as _;
    use serde::{Deserializer, Serializer, de::Error as _, ser::Error as _};

    const MAX_CHUNK_BYTES: usize = 1_048_576;
    const MAX_ENCODED_BYTES: usize = MAX_CHUNK_BYTES.div_ceil(3) * 4;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if bytes.is_empty() || bytes.len() > MAX_CHUNK_BYTES {
            return Err(S::Error::custom(
                "Candidate Artifact chunk bytes are out of bounds",
            ));
        }
        BASE64_STANDARD.encode(bytes).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        if encoded.is_empty() || encoded.len() > MAX_ENCODED_BYTES {
            return Err(D::Error::custom(
                "Candidate Artifact chunk Base64 is out of bounds",
            ));
        }
        let bytes = BASE64_STANDARD
            .decode(&encoded)
            .map_err(|_| D::Error::custom("Candidate Artifact chunk Base64 is invalid"))?;
        if bytes.len() > MAX_CHUNK_BYTES || BASE64_STANDARD.encode(&bytes) != encoded {
            return Err(D::Error::custom(
                "Candidate Artifact chunk Base64 is not canonical",
            ));
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateArtifactChunkReceipt {
    pub artifact_id: CandidateArtifactId,
    pub chunk_index: u32,
    pub digest: Sha256Digest,
    pub size_bytes: u32,
    pub artifact_version: AggregateVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteCandidateArtifactInput {
    pub project_id: ProjectId,
    pub artifact_id: CandidateArtifactId,
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
    pub bundle_protocol_key: ProtocolKey,
    pub bundle_uri: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenewLeaseInput {
    pub project_id: ProjectId,
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
    pub extend_by_seconds: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseLeaseInput {
    pub project_id: ProjectId,
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub fencing_token: FencingToken,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    fn init_candidate_artifact<'a>(
        &'a self,
        command: &'a MvpCommand<InitCandidateArtifactInput>,
    ) -> MvpFuture<'a, CandidateArtifactView>;

    fn upload_candidate_artifact_chunk<'a>(
        &'a self,
        command: &'a MvpCommand<UploadCandidateArtifactChunkInput>,
    ) -> MvpFuture<'a, CandidateArtifactChunkReceipt>;

    fn complete_candidate_artifact<'a>(
        &'a self,
        command: &'a MvpCommand<CompleteCandidateArtifactInput>,
    ) -> MvpFuture<'a, CandidateArtifactView>;

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

    #[test]
    fn candidate_chunk_wire_uses_bounded_canonical_base64() {
        let input = UploadCandidateArtifactChunkInput {
            project_id: id(1),
            artifact_id: id(2),
            lease_id: id(3),
            node_id: id(4),
            fencing_token: FencingToken::new(1).expect("fencing token"),
            chunk_index: 0,
            digest: Sha256Digest::of_bytes([0_u8, 1, 2, 3]),
            content: vec![0, 1, 2, 3],
        };
        let value = serde_json::to_value(&input).expect("serialize chunk");
        assert_eq!(value["content"], "AAECAw==");
        assert_eq!(
            serde_json::from_value::<UploadCandidateArtifactChunkInput>(value)
                .expect("decode canonical chunk"),
            input
        );

        let mut non_canonical = serde_json::to_value(&input).expect("serialize chunk");
        non_canonical["content"] = serde_json::json!("AAECAw");
        assert!(
            serde_json::from_value::<UploadCandidateArtifactChunkInput>(non_canonical).is_err()
        );
        let mut byte_array = serde_json::to_value(&input).expect("serialize chunk");
        byte_array["content"] = serde_json::json!([0, 1, 2, 3]);
        assert!(serde_json::from_value::<UploadCandidateArtifactChunkInput>(byte_array).is_err());

        let mut empty = input;
        empty.content.clear();
        assert!(serde_json::to_value(empty).is_err());
    }
}
