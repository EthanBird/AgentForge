//! Candidate-first artifact, immutable Candidate, and independent verification state.

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, ArtifactRef, AttemptId, CandidateArtifactId, CandidateId, FencingToken,
        GitObjectId, LeaseId, PackageId, PackageRevisionId, ServerInstant, Sha256Digest,
        VerificationRunId, VerificationStageResultId,
    },
    state::Transition,
};

const MAX_CHUNKS: usize = 4_096;
const MAX_BRANCH_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateArtifactState {
    Uploading,
    Assembling,
    Complete,
    Rejected,
    Quarantined,
    Expired,
}

impl CandidateArtifactState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uploading => "uploading",
            Self::Assembling => "assembling",
            Self::Complete => "complete",
            Self::Rejected => "rejected",
            Self::Quarantined => "quarantined",
            Self::Expired => "expired",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Rejected | Self::Quarantined | Self::Expired
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveCandidateArtifact {
    pub id: CandidateArtifactId,
    pub reserved_candidate_id: CandidateId,
    pub attempt_id: AttemptId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub package_hash: Sha256Digest,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub base_commit: GitObjectId,
    pub candidate_commit: GitObjectId,
    pub tree_hash: GitObjectId,
    pub author_evidence_digest: Sha256Digest,
    pub expected_bundle_digest: Sha256Digest,
    pub expected_bundle_size_bytes: u64,
    pub chunk_digests: Vec<Sha256Digest>,
    pub created_at: ServerInstant,
    pub expires_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CandidateArtifactCommand {
    Reserve(ReserveCandidateArtifact),
    BeginAssembly {
        expected_version: AggregateVersion,
        observed_at: ServerInstant,
    },
    Complete {
        expected_version: AggregateVersion,
        bundle: ArtifactRef,
        observed_size_bytes: u64,
        observed_chunk_digests: Vec<Sha256Digest>,
        completed_at: ServerInstant,
    },
    Reject {
        expected_version: AggregateVersion,
        reason_code: String,
        rejected_at: ServerInstant,
    },
    Quarantine {
        expected_version: AggregateVersion,
        policy_authorized: bool,
        reason_code: String,
        quarantined_at: ServerInstant,
    },
    Expire {
        expected_version: AggregateVersion,
        observed_at: ServerInstant,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CandidateArtifactCommandKind {
    Reserve,
    BeginAssembly,
    Complete,
    Reject,
    Quarantine,
    Expire,
}

impl CandidateArtifactCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserve => "reserve",
            Self::BeginAssembly => "begin_assembly",
            Self::Complete => "complete",
            Self::Reject => "reject",
            Self::Quarantine => "quarantine",
            Self::Expire => "expire",
        }
    }
}

impl CandidateArtifactCommand {
    #[must_use]
    pub const fn kind(&self) -> CandidateArtifactCommandKind {
        match self {
            Self::Reserve(_) => CandidateArtifactCommandKind::Reserve,
            Self::BeginAssembly { .. } => CandidateArtifactCommandKind::BeginAssembly,
            Self::Complete { .. } => CandidateArtifactCommandKind::Complete,
            Self::Reject { .. } => CandidateArtifactCommandKind::Reject,
            Self::Quarantine { .. } => CandidateArtifactCommandKind::Quarantine,
            Self::Expire { .. } => CandidateArtifactCommandKind::Expire,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Reserve(_) => None,
            Self::BeginAssembly {
                expected_version, ..
            }
            | Self::Complete {
                expected_version, ..
            }
            | Self::Reject {
                expected_version, ..
            }
            | Self::Quarantine {
                expected_version, ..
            }
            | Self::Expire {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CandidateArtifactEvent {
    Reserved {
        reservation: Box<ReserveCandidateArtifact>,
    },
    AssemblyStarted {
        observed_at: ServerInstant,
    },
    Completed {
        bundle: ArtifactRef,
        observed_size_bytes: u64,
        observed_chunk_digests: Vec<Sha256Digest>,
        completed_at: ServerInstant,
    },
    Rejected {
        reason_code: String,
        rejected_at: ServerInstant,
    },
    Quarantined {
        reason_code: String,
        quarantined_at: ServerInstant,
    },
    Expired {
        observed_at: ServerInstant,
    },
}

impl CandidateArtifactEvent {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Reserved { .. } => "reserved",
            Self::AssemblyStarted { .. } => "assembly_started",
            Self::Completed { .. } => "completed",
            Self::Rejected { .. } => "rejected",
            Self::Quarantined { .. } => "quarantined",
            Self::Expired { .. } => "expired",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CandidateArtifact {
    id: CandidateArtifactId,
    reserved_candidate_id: CandidateId,
    attempt_id: AttemptId,
    package_id: PackageId,
    revision_id: PackageRevisionId,
    package_hash: Sha256Digest,
    lease_id: LeaseId,
    fencing_token: FencingToken,
    base_commit: GitObjectId,
    candidate_commit: GitObjectId,
    tree_hash: GitObjectId,
    author_evidence_digest: Sha256Digest,
    expected_bundle_digest: Sha256Digest,
    expected_bundle_size_bytes: u64,
    chunk_digests: Vec<Sha256Digest>,
    bundle: Option<ArtifactRef>,
    state: CandidateArtifactState,
    created_at: ServerInstant,
    expires_at: ServerInstant,
    updated_at: ServerInstant,
    version: AggregateVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateArtifactSnapshot {
    pub id: CandidateArtifactId,
    pub reserved_candidate_id: CandidateId,
    pub attempt_id: AttemptId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub package_hash: Sha256Digest,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub base_commit: GitObjectId,
    pub candidate_commit: GitObjectId,
    pub tree_hash: GitObjectId,
    pub author_evidence_digest: Sha256Digest,
    pub expected_bundle_digest: Sha256Digest,
    pub expected_bundle_size_bytes: u64,
    pub chunk_digests: Vec<Sha256Digest>,
    pub bundle: Option<ArtifactRef>,
    pub state: CandidateArtifactState,
    pub created_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub updated_at: ServerInstant,
    pub version: AggregateVersion,
}

impl<'de> Deserialize<'de> for CandidateArtifact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let snapshot = CandidateArtifactSnapshot::deserialize(deserializer)?;
        Self::restore_snapshot(snapshot).map_err(de::Error::custom)
    }
}

impl CandidateArtifact {
    pub fn transition(
        current: Option<&Self>,
        command: &CandidateArtifactCommand,
    ) -> Result<Transition<Self, CandidateArtifactEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &CandidateArtifactCommand,
    ) -> Result<CandidateArtifactEvent, DomainError> {
        match (current, command) {
            (None, CandidateArtifactCommand::Reserve(reservation)) => {
                validate_reservation(reservation)?;
                Ok(CandidateArtifactEvent::Reserved {
                    reservation: Box::new(reservation.clone()),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "candidate_artifact",
            }),
            (Some(artifact), CandidateArtifactCommand::Reserve(_)) => {
                Err(invalid_artifact_transition(artifact.state, command.kind()))
            }
            (Some(artifact), command) => {
                if artifact.state.is_terminal() {
                    return Err(invalid_artifact_transition(artifact.state, command.kind()));
                }
                if command.expected_version() != Some(artifact.version) {
                    return Err(DomainError::StaleVersion);
                }
                match command {
                    CandidateArtifactCommand::BeginAssembly { observed_at, .. }
                        if artifact.state == CandidateArtifactState::Uploading =>
                    {
                        artifact.authorize_time(*observed_at)?;
                        Ok(CandidateArtifactEvent::AssemblyStarted {
                            observed_at: *observed_at,
                        })
                    }
                    CandidateArtifactCommand::Complete {
                        bundle,
                        observed_size_bytes,
                        observed_chunk_digests,
                        completed_at,
                        ..
                    } if artifact.state == CandidateArtifactState::Assembling => {
                        artifact.validate_completion(
                            bundle,
                            *observed_size_bytes,
                            observed_chunk_digests,
                            *completed_at,
                        )?;
                        Ok(CandidateArtifactEvent::Completed {
                            bundle: bundle.clone(),
                            observed_size_bytes: *observed_size_bytes,
                            observed_chunk_digests: observed_chunk_digests.clone(),
                            completed_at: *completed_at,
                        })
                    }
                    CandidateArtifactCommand::Reject {
                        reason_code,
                        rejected_at,
                        ..
                    } if valid_reason(reason_code) && *rejected_at >= artifact.updated_at => {
                        Ok(CandidateArtifactEvent::Rejected {
                            reason_code: reason_code.clone(),
                            rejected_at: *rejected_at,
                        })
                    }
                    CandidateArtifactCommand::Quarantine {
                        policy_authorized,
                        reason_code,
                        quarantined_at,
                        ..
                    } if *policy_authorized
                        && valid_reason(reason_code)
                        && *quarantined_at >= artifact.updated_at =>
                    {
                        Ok(CandidateArtifactEvent::Quarantined {
                            reason_code: reason_code.clone(),
                            quarantined_at: *quarantined_at,
                        })
                    }
                    CandidateArtifactCommand::Expire { observed_at, .. }
                        if *observed_at >= artifact.expires_at =>
                    {
                        Ok(CandidateArtifactEvent::Expired {
                            observed_at: *observed_at,
                        })
                    }
                    _ => Err(invalid_artifact_transition(artifact.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &CandidateArtifactEvent,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, CandidateArtifactEvent::Reserved { reservation }) => {
                validate_reservation(reservation)?;
                Ok(Self {
                    id: reservation.id,
                    reserved_candidate_id: reservation.reserved_candidate_id,
                    attempt_id: reservation.attempt_id,
                    package_id: reservation.package_id,
                    revision_id: reservation.revision_id,
                    package_hash: reservation.package_hash,
                    lease_id: reservation.lease_id,
                    fencing_token: reservation.fencing_token,
                    base_commit: reservation.base_commit.clone(),
                    candidate_commit: reservation.candidate_commit.clone(),
                    tree_hash: reservation.tree_hash.clone(),
                    author_evidence_digest: reservation.author_evidence_digest,
                    expected_bundle_digest: reservation.expected_bundle_digest,
                    expected_bundle_size_bytes: reservation.expected_bundle_size_bytes,
                    chunk_digests: reservation.chunk_digests.clone(),
                    bundle: None,
                    state: CandidateArtifactState::Uploading,
                    created_at: reservation.created_at,
                    expires_at: reservation.expires_at,
                    updated_at: reservation.created_at,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "candidate_artifact",
            }),
            (Some(artifact), _) if artifact.state.is_terminal() => {
                Err(invalid_artifact_event(artifact.state, event.name()))
            }
            (Some(artifact), CandidateArtifactEvent::AssemblyStarted { observed_at })
                if artifact.state == CandidateArtifactState::Uploading =>
            {
                artifact.authorize_time(*observed_at)?;
                artifact.advance(CandidateArtifactState::Assembling, *observed_at, None)
            }
            (
                Some(artifact),
                CandidateArtifactEvent::Completed {
                    bundle,
                    observed_size_bytes,
                    observed_chunk_digests,
                    completed_at,
                },
            ) if artifact.state == CandidateArtifactState::Assembling => {
                artifact.validate_completion(
                    bundle,
                    *observed_size_bytes,
                    observed_chunk_digests,
                    *completed_at,
                )?;
                artifact.advance(
                    CandidateArtifactState::Complete,
                    *completed_at,
                    Some(bundle.clone()),
                )
            }
            (
                Some(artifact),
                CandidateArtifactEvent::Rejected {
                    reason_code,
                    rejected_at,
                },
            ) if valid_reason(reason_code) && *rejected_at >= artifact.updated_at => {
                artifact.advance(CandidateArtifactState::Rejected, *rejected_at, None)
            }
            (
                Some(artifact),
                CandidateArtifactEvent::Quarantined {
                    reason_code,
                    quarantined_at,
                },
            ) if valid_reason(reason_code) && *quarantined_at >= artifact.updated_at => {
                artifact.advance(CandidateArtifactState::Quarantined, *quarantined_at, None)
            }
            (Some(artifact), CandidateArtifactEvent::Expired { observed_at })
                if *observed_at >= artifact.expires_at =>
            {
                artifact.advance(CandidateArtifactState::Expired, *observed_at, None)
            }
            (Some(artifact), event) => Err(invalid_artifact_event(artifact.state, event.name())),
        }
    }

    pub fn replay(events: &[CandidateArtifactEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "candidate_artifact",
        })
    }

    pub fn restore_snapshot(snapshot: CandidateArtifactSnapshot) -> Result<Self, DomainError> {
        let artifact = Self {
            id: snapshot.id,
            reserved_candidate_id: snapshot.reserved_candidate_id,
            attempt_id: snapshot.attempt_id,
            package_id: snapshot.package_id,
            revision_id: snapshot.revision_id,
            package_hash: snapshot.package_hash,
            lease_id: snapshot.lease_id,
            fencing_token: snapshot.fencing_token,
            base_commit: snapshot.base_commit,
            candidate_commit: snapshot.candidate_commit,
            tree_hash: snapshot.tree_hash,
            author_evidence_digest: snapshot.author_evidence_digest,
            expected_bundle_digest: snapshot.expected_bundle_digest,
            expected_bundle_size_bytes: snapshot.expected_bundle_size_bytes,
            chunk_digests: snapshot.chunk_digests,
            bundle: snapshot.bundle,
            state: snapshot.state,
            created_at: snapshot.created_at,
            expires_at: snapshot.expires_at,
            updated_at: snapshot.updated_at,
            version: snapshot.version,
        };
        artifact.validate_shape()?;
        Ok(artifact)
    }

    #[must_use]
    pub fn snapshot(&self) -> CandidateArtifactSnapshot {
        CandidateArtifactSnapshot {
            id: self.id,
            reserved_candidate_id: self.reserved_candidate_id,
            attempt_id: self.attempt_id,
            package_id: self.package_id,
            revision_id: self.revision_id,
            package_hash: self.package_hash,
            lease_id: self.lease_id,
            fencing_token: self.fencing_token,
            base_commit: self.base_commit.clone(),
            candidate_commit: self.candidate_commit.clone(),
            tree_hash: self.tree_hash.clone(),
            author_evidence_digest: self.author_evidence_digest,
            expected_bundle_digest: self.expected_bundle_digest,
            expected_bundle_size_bytes: self.expected_bundle_size_bytes,
            chunk_digests: self.chunk_digests.clone(),
            bundle: self.bundle.clone(),
            state: self.state,
            created_at: self.created_at,
            expires_at: self.expires_at,
            updated_at: self.updated_at,
            version: self.version,
        }
    }

    fn validate_shape(&self) -> Result<(), DomainError> {
        validate_reservation(&ReserveCandidateArtifact {
            id: self.id,
            reserved_candidate_id: self.reserved_candidate_id,
            attempt_id: self.attempt_id,
            package_id: self.package_id,
            revision_id: self.revision_id,
            package_hash: self.package_hash,
            lease_id: self.lease_id,
            fencing_token: self.fencing_token,
            base_commit: self.base_commit.clone(),
            candidate_commit: self.candidate_commit.clone(),
            tree_hash: self.tree_hash.clone(),
            author_evidence_digest: self.author_evidence_digest,
            expected_bundle_digest: self.expected_bundle_digest,
            expected_bundle_size_bytes: self.expected_bundle_size_bytes,
            chunk_digests: self.chunk_digests.clone(),
            created_at: self.created_at,
            expires_at: self.expires_at,
        })?;
        if self.version == AggregateVersion::ZERO
            || self.updated_at < self.created_at
            || match self.state {
                CandidateArtifactState::Uploading => {
                    self.version != AggregateVersion::new(1) || self.updated_at != self.created_at
                }
                CandidateArtifactState::Assembling => {
                    self.version != AggregateVersion::new(2) || self.updated_at >= self.expires_at
                }
                CandidateArtifactState::Complete => self.version != AggregateVersion::new(3),
                CandidateArtifactState::Rejected | CandidateArtifactState::Quarantined => {
                    !matches!(self.version.get(), 2 | 3)
                }
                CandidateArtifactState::Expired => {
                    !matches!(self.version.get(), 2 | 3) || self.updated_at < self.expires_at
                }
            }
            || (self.state == CandidateArtifactState::Complete) != self.bundle.is_some()
        {
            return Err(DomainError::InvariantViolation {
                invariant: "candidate_artifact_snapshot_shape",
            });
        }
        if let Some(bundle) = &self.bundle {
            self.validate_completion(
                bundle,
                self.expected_bundle_size_bytes,
                &self.chunk_digests,
                self.updated_at,
            )?;
        }
        Ok(())
    }

    fn authorize_time(&self, observed_at: ServerInstant) -> Result<(), DomainError> {
        if observed_at < self.updated_at {
            return Err(DomainError::InvalidArgument {
                field: "observed_at".into(),
                reason: "must be monotonic".into(),
            });
        }
        if observed_at >= self.expires_at {
            return Err(DomainError::LeaseExpired);
        }
        Ok(())
    }

    fn validate_completion(
        &self,
        bundle: &ArtifactRef,
        observed_size_bytes: u64,
        observed_chunk_digests: &[Sha256Digest],
        completed_at: ServerInstant,
    ) -> Result<(), DomainError> {
        self.authorize_time(completed_at)?;
        if bundle.digest != self.expected_bundle_digest
            || observed_size_bytes != self.expected_bundle_size_bytes
            || observed_chunk_digests != self.chunk_digests
            || !valid_artifact_ref(bundle)
        {
            return Err(DomainError::EvidenceInvalid);
        }
        Ok(())
    }

    fn advance(
        &self,
        state: CandidateArtifactState,
        updated_at: ServerInstant,
        bundle: Option<ArtifactRef>,
    ) -> Result<Self, DomainError> {
        let mut next = self.clone();
        next.state = state;
        next.updated_at = updated_at;
        next.bundle = bundle;
        next.version = self.version.checked_next()?;
        next.validate_shape()?;
        Ok(next)
    }

    #[must_use]
    pub const fn id(&self) -> CandidateArtifactId {
        self.id
    }

    #[must_use]
    pub const fn reserved_candidate_id(&self) -> CandidateId {
        self.reserved_candidate_id
    }

    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    #[must_use]
    pub const fn package_id(&self) -> PackageId {
        self.package_id
    }

    #[must_use]
    pub const fn revision_id(&self) -> PackageRevisionId {
        self.revision_id
    }

    #[must_use]
    pub const fn package_hash(&self) -> Sha256Digest {
        self.package_hash
    }

    #[must_use]
    pub const fn lease_id(&self) -> LeaseId {
        self.lease_id
    }

    #[must_use]
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    #[must_use]
    pub const fn state(&self) -> CandidateArtifactState {
        self.state
    }

    #[must_use]
    pub const fn version(&self) -> AggregateVersion {
        self.version
    }

    #[must_use]
    pub const fn candidate_commit(&self) -> &GitObjectId {
        &self.candidate_commit
    }

    #[must_use]
    pub const fn base_commit(&self) -> &GitObjectId {
        &self.base_commit
    }

    #[must_use]
    pub const fn tree_hash(&self) -> &GitObjectId {
        &self.tree_hash
    }

    #[must_use]
    pub const fn author_evidence_digest(&self) -> Sha256Digest {
        self.author_evidence_digest
    }

    #[must_use]
    pub const fn expected_bundle_digest(&self) -> Sha256Digest {
        self.expected_bundle_digest
    }

    #[must_use]
    pub const fn expected_bundle_size_bytes(&self) -> u64 {
        self.expected_bundle_size_bytes
    }

    #[must_use]
    pub fn chunk_digests(&self) -> &[Sha256Digest] {
        &self.chunk_digests
    }

    #[must_use]
    pub const fn created_at(&self) -> ServerInstant {
        self.created_at
    }

    #[must_use]
    pub const fn expires_at(&self) -> ServerInstant {
        self.expires_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> ServerInstant {
        self.updated_at
    }

    #[must_use]
    pub const fn bundle(&self) -> Option<&ArtifactRef> {
        self.bundle.as_ref()
    }
}

fn validate_reservation(reservation: &ReserveCandidateArtifact) -> Result<(), DomainError> {
    if reservation.id.as_uuid().is_nil()
        || reservation.reserved_candidate_id.as_uuid().is_nil()
        || reservation.attempt_id.as_uuid().is_nil()
        || reservation.package_id.as_uuid().is_nil()
        || reservation.revision_id.as_uuid().is_nil()
        || digest_is_zero(reservation.package_hash)
        || reservation.lease_id.as_uuid().is_nil()
        || reservation.base_commit.as_str().len() != reservation.candidate_commit.as_str().len()
        || reservation.candidate_commit.as_str().len() != reservation.tree_hash.as_str().len()
        || !valid_git_oid(&reservation.base_commit)
        || !valid_git_oid(&reservation.candidate_commit)
        || !valid_git_oid(&reservation.tree_hash)
        || digest_is_zero(reservation.author_evidence_digest)
        || digest_is_zero(reservation.expected_bundle_digest)
        || reservation.expected_bundle_size_bytes == 0
        || reservation.chunk_digests.is_empty()
        || reservation.chunk_digests.len() > MAX_CHUNKS
        || reservation
            .chunk_digests
            .iter()
            .copied()
            .any(digest_is_zero)
        || reservation.created_at >= reservation.expires_at
    {
        return Err(DomainError::InvalidArgument {
            field: "candidate_artifact".into(),
            reason: "reservation binding is invalid".into(),
        });
    }
    Ok(())
}

fn valid_artifact_ref(reference: &ArtifactRef) -> bool {
    !reference.uri.is_empty()
        && reference.uri.len() <= 2_048
        && reference.uri.starts_with("artifact://")
        && !reference.uri.chars().any(char::is_control)
        && !digest_is_zero(reference.digest)
}

fn digest_is_zero(digest: Sha256Digest) -> bool {
    digest.as_bytes().iter().all(|byte| *byte == 0)
}

fn valid_git_oid(object_id: &GitObjectId) -> bool {
    object_id.as_str().bytes().any(|byte| byte != b'0')
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= 128
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn invalid_artifact_transition(
    state: CandidateArtifactState,
    command: CandidateArtifactCommandKind,
) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_artifact_event(state: CandidateArtifactState, event: &str) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: event.into(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealCandidate {
    pub id: CandidateId,
    pub branch: String,
    pub sealed_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRecord {
    pub id: CandidateId,
    pub attempt_id: AttemptId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub package_hash: Sha256Digest,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub base_commit: GitObjectId,
    pub candidate_commit: GitObjectId,
    pub tree_hash: GitObjectId,
    pub branch: String,
    pub author_evidence_digest: Sha256Digest,
    pub bundle_artifact_id: CandidateArtifactId,
    pub bundle: ArtifactRef,
    pub sealed_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CandidateEvent {
    Sealed { record: CandidateRecord },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CandidateCommandKind {
    Seal,
}

/// Immutable Candidate reconstructed only through its event stream and the
/// bound COMPLETE artifact. Direct deserialization would bypass that proof.
///
/// ```compile_fail
/// let _: agentforge_domain::Candidate = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Candidate {
    record: CandidateRecord,
    version: AggregateVersion,
}

impl Candidate {
    pub fn transition(
        current: Option<&Self>,
        command: &SealCandidate,
        artifact: &CandidateArtifact,
    ) -> Result<Transition<Self, CandidateEvent>, DomainError> {
        let event = Self::decide(current, command, artifact)?;
        let aggregate = Self::apply_event(current, &event, artifact)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &SealCandidate,
        artifact: &CandidateArtifact,
    ) -> Result<CandidateEvent, DomainError> {
        if current.is_some() {
            return Err(DomainError::InvalidTransition {
                from: "sealed".into(),
                command: "seal".into(),
            });
        }
        let bundle = artifact
            .bundle()
            .cloned()
            .ok_or(DomainError::CandidateArtifactNotComplete)?;
        if artifact.state() != CandidateArtifactState::Complete
            || command.id != artifact.reserved_candidate_id()
            || command.sealed_at < artifact.updated_at
        {
            return Err(DomainError::CandidateArtifactNotComplete);
        }
        let record = CandidateRecord {
            id: command.id,
            attempt_id: artifact.attempt_id(),
            package_id: artifact.package_id(),
            revision_id: artifact.revision_id(),
            package_hash: artifact.package_hash(),
            lease_id: artifact.lease_id(),
            fencing_token: artifact.fencing_token(),
            base_commit: artifact.base_commit().clone(),
            candidate_commit: artifact.candidate_commit().clone(),
            tree_hash: artifact.tree_hash().clone(),
            branch: command.branch.clone(),
            author_evidence_digest: artifact.author_evidence_digest(),
            bundle_artifact_id: artifact.id(),
            bundle,
            sealed_at: command.sealed_at,
        };
        validate_candidate_record(&record)?;
        Ok(CandidateEvent::Sealed { record })
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &CandidateEvent,
        artifact: &CandidateArtifact,
    ) -> Result<Self, DomainError> {
        if current.is_some() {
            return Err(DomainError::InvalidTransition {
                from: "sealed".into(),
                command: "sealed".into(),
            });
        }
        let CandidateEvent::Sealed { record } = event;
        validate_candidate_record(record)?;
        validate_candidate_artifact_binding(record, artifact)?;
        Ok(Self {
            record: record.clone(),
            version: AggregateVersion::new(1),
        })
    }

    pub fn replay(
        events: &[CandidateEvent],
        artifact: &CandidateArtifact,
    ) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event, artifact)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "candidate",
        })
    }

    #[must_use]
    pub const fn record(&self) -> &CandidateRecord {
        &self.record
    }

    #[must_use]
    pub const fn version(&self) -> AggregateVersion {
        self.version
    }
}

fn validate_candidate_record(record: &CandidateRecord) -> Result<(), DomainError> {
    if record.id.as_uuid().is_nil()
        || record.attempt_id.as_uuid().is_nil()
        || record.package_id.as_uuid().is_nil()
        || record.revision_id.as_uuid().is_nil()
        || digest_is_zero(record.package_hash)
        || record.lease_id.as_uuid().is_nil()
        || record.base_commit.as_str().len() != record.candidate_commit.as_str().len()
        || record.candidate_commit.as_str().len() != record.tree_hash.as_str().len()
        || !valid_git_oid(&record.base_commit)
        || !valid_git_oid(&record.candidate_commit)
        || !valid_git_oid(&record.tree_hash)
        || digest_is_zero(record.author_evidence_digest)
        || !valid_branch(&record.branch)
        || !valid_artifact_ref(&record.bundle)
    {
        return Err(DomainError::InvariantViolation {
            invariant: "candidate_record_shape",
        });
    }
    Ok(())
}

fn validate_candidate_artifact_binding(
    record: &CandidateRecord,
    artifact: &CandidateArtifact,
) -> Result<(), DomainError> {
    if artifact.state() != CandidateArtifactState::Complete
        || record.id != artifact.reserved_candidate_id()
        || record.attempt_id != artifact.attempt_id()
        || record.package_id != artifact.package_id()
        || record.revision_id != artifact.revision_id()
        || record.package_hash != artifact.package_hash()
        || record.lease_id != artifact.lease_id()
        || record.fencing_token != artifact.fencing_token()
        || record.base_commit != *artifact.base_commit()
        || record.candidate_commit != *artifact.candidate_commit()
        || record.tree_hash != *artifact.tree_hash()
        || record.author_evidence_digest != artifact.author_evidence_digest()
        || record.bundle_artifact_id != artifact.id()
        || artifact.bundle() != Some(&record.bundle)
        || record.sealed_at < artifact.updated_at
    {
        return Err(DomainError::CandidateArtifactNotComplete);
    }
    Ok(())
}

fn valid_branch(branch: &str) -> bool {
    branch.starts_with("refs/heads/agentforge/")
        && branch.len() <= MAX_BRANCH_BYTES
        && !branch.ends_with('/')
        && !branch.contains("..")
        && !branch.contains("//")
        && !branch.contains("@{")
        && branch
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationRunState {
    Queued,
    ProvenanceCheck,
    Reviewing,
    Reproducing,
    Pass,
    Fail,
    Inconclusive,
    Cancelled,
}

impl VerificationRunState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::ProvenanceCheck => "provenance_check",
            Self::Reviewing => "reviewing",
            Self::Reproducing => "reproducing",
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Inconclusive => "inconclusive",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Pass | Self::Fail | Self::Inconclusive | Self::Cancelled
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStage {
    ProvenanceCheck,
    Reviewing,
    Reproducing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueVerificationRun {
    pub id: VerificationRunId,
    pub candidate_id: CandidateId,
    pub candidate_commit: GitObjectId,
    pub queued_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VerificationRunCommand {
    Queue(QueueVerificationRun),
    StartProvenance {
        expected_version: AggregateVersion,
        started_at: ServerInstant,
    },
    StartReview {
        expected_version: AggregateVersion,
        started_at: ServerInstant,
    },
    StartReproduction {
        expected_version: AggregateVersion,
        reviewed_head: GitObjectId,
        started_at: ServerInstant,
    },
    Finalize {
        expected_version: AggregateVersion,
        outcome: VerificationRunState,
        terminal_stage: VerificationStage,
        terminal_stage_result_id: VerificationStageResultId,
        reviewed_head: Option<GitObjectId>,
        tested_head: Option<GitObjectId>,
        evidence_digest: Sha256Digest,
        terminalized_at: ServerInstant,
    },
    Cancel {
        expected_version: AggregateVersion,
        cancelled_at: ServerInstant,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum VerificationRunCommandKind {
    Queue,
    StartProvenance,
    StartReview,
    StartReproduction,
    Finalize,
    Cancel,
}

impl VerificationRunCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::StartProvenance => "start_provenance",
            Self::StartReview => "start_review",
            Self::StartReproduction => "start_reproduction",
            Self::Finalize => "finalize",
            Self::Cancel => "cancel",
        }
    }
}

impl VerificationRunCommand {
    #[must_use]
    pub const fn kind(&self) -> VerificationRunCommandKind {
        match self {
            Self::Queue(_) => VerificationRunCommandKind::Queue,
            Self::StartProvenance { .. } => VerificationRunCommandKind::StartProvenance,
            Self::StartReview { .. } => VerificationRunCommandKind::StartReview,
            Self::StartReproduction { .. } => VerificationRunCommandKind::StartReproduction,
            Self::Finalize { .. } => VerificationRunCommandKind::Finalize,
            Self::Cancel { .. } => VerificationRunCommandKind::Cancel,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Queue(_) => None,
            Self::StartProvenance {
                expected_version, ..
            }
            | Self::StartReview {
                expected_version, ..
            }
            | Self::StartReproduction {
                expected_version, ..
            }
            | Self::Finalize {
                expected_version, ..
            }
            | Self::Cancel {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VerificationRunEvent {
    Queued {
        record: QueueVerificationRun,
    },
    ProvenanceStarted {
        started_at: ServerInstant,
    },
    ReviewStarted {
        started_at: ServerInstant,
    },
    ReproductionStarted {
        reviewed_head: GitObjectId,
        started_at: ServerInstant,
    },
    Finalized {
        outcome: VerificationRunState,
        terminal_stage: VerificationStage,
        terminal_stage_result_id: VerificationStageResultId,
        reviewed_head: Option<GitObjectId>,
        tested_head: Option<GitObjectId>,
        evidence_digest: Sha256Digest,
        terminalized_at: ServerInstant,
    },
    Cancelled {
        cancelled_at: ServerInstant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerificationRun {
    id: VerificationRunId,
    candidate_id: CandidateId,
    candidate_commit: GitObjectId,
    state: VerificationRunState,
    terminal_stage: Option<VerificationStage>,
    terminal_stage_result_id: Option<VerificationStageResultId>,
    reviewed_head: Option<GitObjectId>,
    tested_head: Option<GitObjectId>,
    evidence_digest: Option<Sha256Digest>,
    queued_at: ServerInstant,
    updated_at: ServerInstant,
    terminalized_at: Option<ServerInstant>,
    version: AggregateVersion,
}

/// VerificationRun is reconstructed only by replaying its event stream against
/// the immutable Candidate. Direct deserialization would bypass that lineage.
///
/// ```compile_fail
/// let _: agentforge_domain::VerificationRun = serde_json::from_str("{}").unwrap();
/// ```
impl VerificationRun {
    pub fn transition(
        current: Option<&Self>,
        command: &VerificationRunCommand,
        candidate: &Candidate,
    ) -> Result<Transition<Self, VerificationRunEvent>, DomainError> {
        let event = Self::decide(current, command, candidate)?;
        let aggregate = Self::apply_event(current, &event, candidate)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &VerificationRunCommand,
        candidate: &Candidate,
    ) -> Result<VerificationRunEvent, DomainError> {
        match (current, command) {
            (None, VerificationRunCommand::Queue(record)) => {
                validate_queue(record, candidate)?;
                Ok(VerificationRunEvent::Queued {
                    record: record.clone(),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "verification_run",
            }),
            (Some(run), VerificationRunCommand::Queue(_)) => {
                Err(invalid_run_transition(run.state, command.kind()))
            }
            (Some(run), command) => {
                run.validate_candidate_binding(candidate)?;
                if run.state.is_terminal() {
                    return Err(invalid_run_transition(run.state, command.kind()));
                }
                if command.expected_version() != Some(run.version) {
                    return Err(DomainError::StaleVersion);
                }
                match command {
                    VerificationRunCommand::StartProvenance { started_at, .. }
                        if run.state == VerificationRunState::Queued =>
                    {
                        run.validate_time(*started_at)?;
                        Ok(VerificationRunEvent::ProvenanceStarted {
                            started_at: *started_at,
                        })
                    }
                    VerificationRunCommand::StartReview { started_at, .. }
                        if run.state == VerificationRunState::ProvenanceCheck =>
                    {
                        run.validate_time(*started_at)?;
                        Ok(VerificationRunEvent::ReviewStarted {
                            started_at: *started_at,
                        })
                    }
                    VerificationRunCommand::StartReproduction {
                        reviewed_head,
                        started_at,
                        ..
                    } if run.state == VerificationRunState::Reviewing
                        && *reviewed_head == run.candidate_commit =>
                    {
                        run.validate_time(*started_at)?;
                        Ok(VerificationRunEvent::ReproductionStarted {
                            reviewed_head: reviewed_head.clone(),
                            started_at: *started_at,
                        })
                    }
                    VerificationRunCommand::Finalize {
                        outcome,
                        terminal_stage,
                        terminal_stage_result_id,
                        reviewed_head,
                        tested_head,
                        evidence_digest,
                        terminalized_at,
                        ..
                    } => {
                        run.validate_terminal(
                            *outcome,
                            *terminal_stage,
                            *terminal_stage_result_id,
                            reviewed_head.as_ref(),
                            tested_head.as_ref(),
                            *evidence_digest,
                            *terminalized_at,
                        )?;
                        Ok(VerificationRunEvent::Finalized {
                            outcome: *outcome,
                            terminal_stage: *terminal_stage,
                            terminal_stage_result_id: *terminal_stage_result_id,
                            reviewed_head: reviewed_head.clone(),
                            tested_head: tested_head.clone(),
                            evidence_digest: *evidence_digest,
                            terminalized_at: *terminalized_at,
                        })
                    }
                    VerificationRunCommand::Cancel { cancelled_at, .. } => {
                        run.validate_time(*cancelled_at)?;
                        Ok(VerificationRunEvent::Cancelled {
                            cancelled_at: *cancelled_at,
                        })
                    }
                    _ => Err(invalid_run_transition(run.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &VerificationRunEvent,
        candidate: &Candidate,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, VerificationRunEvent::Queued { record }) => {
                validate_queue(record, candidate)?;
                Ok(Self {
                    id: record.id,
                    candidate_id: record.candidate_id,
                    candidate_commit: record.candidate_commit.clone(),
                    state: VerificationRunState::Queued,
                    terminal_stage: None,
                    terminal_stage_result_id: None,
                    reviewed_head: None,
                    tested_head: None,
                    evidence_digest: None,
                    queued_at: record.queued_at,
                    updated_at: record.queued_at,
                    terminalized_at: None,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "verification_run",
            }),
            (Some(run), _) if run.validate_candidate_binding(candidate).is_err() => {
                Err(DomainError::HeadMismatch)
            }
            (Some(run), _) if run.state.is_terminal() => {
                Err(invalid_run_event(run.state, "terminal_event"))
            }
            (Some(run), VerificationRunEvent::ProvenanceStarted { started_at })
                if run.state == VerificationRunState::Queued =>
            {
                run.advance(VerificationRunState::ProvenanceCheck, *started_at)
            }
            (Some(run), VerificationRunEvent::ReviewStarted { started_at })
                if run.state == VerificationRunState::ProvenanceCheck =>
            {
                run.advance(VerificationRunState::Reviewing, *started_at)
            }
            (
                Some(run),
                VerificationRunEvent::ReproductionStarted {
                    reviewed_head,
                    started_at,
                },
            ) if run.state == VerificationRunState::Reviewing
                && *reviewed_head == run.candidate_commit =>
            {
                let mut next = run.advance(VerificationRunState::Reproducing, *started_at)?;
                next.reviewed_head = Some(reviewed_head.clone());
                next.validate_shape()?;
                Ok(next)
            }
            (
                Some(run),
                VerificationRunEvent::Finalized {
                    outcome,
                    terminal_stage,
                    terminal_stage_result_id,
                    reviewed_head,
                    tested_head,
                    evidence_digest,
                    terminalized_at,
                },
            ) => {
                run.validate_terminal(
                    *outcome,
                    *terminal_stage,
                    *terminal_stage_result_id,
                    reviewed_head.as_ref(),
                    tested_head.as_ref(),
                    *evidence_digest,
                    *terminalized_at,
                )?;
                let mut next = run.advance(*outcome, *terminalized_at)?;
                next.terminal_stage = Some(*terminal_stage);
                next.terminal_stage_result_id = Some(*terminal_stage_result_id);
                next.reviewed_head = reviewed_head.clone();
                next.tested_head = tested_head.clone();
                next.evidence_digest = Some(*evidence_digest);
                next.terminalized_at = Some(*terminalized_at);
                next.validate_shape()?;
                Ok(next)
            }
            (Some(run), VerificationRunEvent::Cancelled { cancelled_at }) => {
                run.validate_time(*cancelled_at)?;
                let mut next = run.advance(VerificationRunState::Cancelled, *cancelled_at)?;
                next.terminalized_at = Some(*cancelled_at);
                next.validate_shape()?;
                Ok(next)
            }
            (Some(run), _) => Err(invalid_run_event(run.state, "event")),
        }
    }

    pub fn replay(
        events: &[VerificationRunEvent],
        candidate: &Candidate,
    ) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event, candidate)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "verification_run",
        })
    }

    fn advance(
        &self,
        state: VerificationRunState,
        updated_at: ServerInstant,
    ) -> Result<Self, DomainError> {
        self.validate_time(updated_at)?;
        let mut next = self.clone();
        next.state = state;
        next.updated_at = updated_at;
        next.version = self.version.checked_next()?;
        Ok(next)
    }

    fn validate_time(&self, observed_at: ServerInstant) -> Result<(), DomainError> {
        if observed_at < self.updated_at {
            return Err(DomainError::InvalidArgument {
                field: "observed_at".into(),
                reason: "must be monotonic".into(),
            });
        }
        Ok(())
    }

    fn validate_candidate_binding(&self, candidate: &Candidate) -> Result<(), DomainError> {
        if self.candidate_id != candidate.record().id
            || self.candidate_commit != candidate.record().candidate_commit
        {
            return Err(DomainError::HeadMismatch);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_terminal(
        &self,
        outcome: VerificationRunState,
        terminal_stage: VerificationStage,
        terminal_stage_result_id: VerificationStageResultId,
        reviewed_head: Option<&GitObjectId>,
        tested_head: Option<&GitObjectId>,
        evidence_digest: Sha256Digest,
        terminalized_at: ServerInstant,
    ) -> Result<(), DomainError> {
        self.validate_time(terminalized_at)?;
        if terminal_stage_result_id.as_uuid().is_nil()
            || digest_is_zero(evidence_digest)
            || !matches!(
                outcome,
                VerificationRunState::Pass
                    | VerificationRunState::Fail
                    | VerificationRunState::Inconclusive
            )
        {
            return Err(DomainError::VerificationStageInvalid);
        }
        let stage_matches_state = matches!(
            (self.state, terminal_stage),
            (
                VerificationRunState::ProvenanceCheck,
                VerificationStage::ProvenanceCheck
            ) | (
                VerificationRunState::Reviewing,
                VerificationStage::Reviewing
            ) | (
                VerificationRunState::Reproducing,
                VerificationStage::Reproducing
            )
        );
        if !stage_matches_state {
            return Err(DomainError::VerificationStageInvalid);
        }
        let heads_valid = match terminal_stage {
            VerificationStage::ProvenanceCheck => reviewed_head.is_none() && tested_head.is_none(),
            VerificationStage::Reviewing => {
                tested_head.is_none()
                    && reviewed_head.is_none_or(|head| *head == self.candidate_commit)
            }
            VerificationStage::Reproducing => {
                reviewed_head == Some(&self.candidate_commit)
                    && tested_head.is_none_or(|head| *head == self.candidate_commit)
            }
        };
        let pass_valid = outcome != VerificationRunState::Pass
            || (terminal_stage == VerificationStage::Reproducing
                && reviewed_head == Some(&self.candidate_commit)
                && tested_head == Some(&self.candidate_commit));
        if !heads_valid || !pass_valid {
            return Err(DomainError::HeadMismatch);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), DomainError> {
        let version_matches_state = match self.state {
            VerificationRunState::Queued => self.version.get() == 1,
            VerificationRunState::ProvenanceCheck => self.version.get() == 2,
            VerificationRunState::Reviewing => self.version.get() == 3,
            VerificationRunState::Reproducing => self.version.get() == 4,
            VerificationRunState::Pass => self.version.get() == 5,
            VerificationRunState::Fail | VerificationRunState::Inconclusive => {
                matches!(
                    (self.terminal_stage, self.version.get()),
                    (Some(VerificationStage::ProvenanceCheck), 3)
                        | (Some(VerificationStage::Reviewing), 4)
                        | (Some(VerificationStage::Reproducing), 5)
                )
            }
            VerificationRunState::Cancelled => matches!(self.version.get(), 2..=5),
        };
        if self.id.as_uuid().is_nil()
            || self.candidate_id.as_uuid().is_nil()
            || self.version == AggregateVersion::ZERO
            || !version_matches_state
            || self.updated_at < self.queued_at
            || self
                .reviewed_head
                .as_ref()
                .is_some_and(|head| *head != self.candidate_commit)
            || self
                .tested_head
                .as_ref()
                .is_some_and(|head| *head != self.candidate_commit)
            || self.evidence_digest.is_some_and(digest_is_zero)
        {
            return Err(DomainError::InvariantViolation {
                invariant: "verification_run_snapshot_shape",
            });
        }
        if self.state.is_terminal() {
            if self.terminalized_at.is_none()
                || self.terminalized_at != Some(self.updated_at)
                || (self.state == VerificationRunState::Cancelled)
                    != (self.terminal_stage.is_none()
                        && self.terminal_stage_result_id.is_none()
                        && self.evidence_digest.is_none())
                || (self.state != VerificationRunState::Cancelled)
                    != (self.terminal_stage.is_some()
                        && self.terminal_stage_result_id.is_some()
                        && self.evidence_digest.is_some())
            {
                return Err(DomainError::InvariantViolation {
                    invariant: "verification_run_terminal_shape",
                });
            }
        } else if self.terminal_stage.is_some()
            || self.terminal_stage_result_id.is_some()
            || self.evidence_digest.is_some()
            || self.terminalized_at.is_some()
        {
            return Err(DomainError::InvariantViolation {
                invariant: "verification_run_nonterminal_shape",
            });
        }
        match self.state {
            VerificationRunState::Queued
            | VerificationRunState::ProvenanceCheck
            | VerificationRunState::Reviewing => {
                if self.reviewed_head.is_some() || self.tested_head.is_some() {
                    return Err(DomainError::InvariantViolation {
                        invariant: "verification_run_future_head_shape",
                    });
                }
            }
            VerificationRunState::Reproducing => {
                if self.reviewed_head.as_ref() != Some(&self.candidate_commit)
                    || self.tested_head.is_some()
                {
                    return Err(DomainError::InvariantViolation {
                        invariant: "verification_run_reproducing_shape",
                    });
                }
            }
            VerificationRunState::Pass => {
                if self.reviewed_head.as_ref() != Some(&self.candidate_commit)
                    || self.tested_head.as_ref() != Some(&self.candidate_commit)
                    || self.terminal_stage != Some(VerificationStage::Reproducing)
                {
                    return Err(DomainError::HeadMismatch);
                }
            }
            VerificationRunState::Fail | VerificationRunState::Inconclusive => {
                let terminal_shape = match self.terminal_stage {
                    Some(VerificationStage::ProvenanceCheck) => {
                        self.reviewed_head.is_none() && self.tested_head.is_none()
                    }
                    Some(VerificationStage::Reviewing) => self.tested_head.is_none(),
                    Some(VerificationStage::Reproducing) => {
                        self.reviewed_head.as_ref() == Some(&self.candidate_commit)
                    }
                    None => false,
                };
                if !terminal_shape {
                    return Err(DomainError::InvariantViolation {
                        invariant: "verification_run_failure_stage_shape",
                    });
                }
            }
            VerificationRunState::Cancelled => {
                if self.tested_head.is_some() {
                    return Err(DomainError::InvariantViolation {
                        invariant: "cancelled_verification_run_has_no_terminal_heads",
                    });
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn id(&self) -> VerificationRunId {
        self.id
    }

    #[must_use]
    pub const fn candidate_id(&self) -> CandidateId {
        self.candidate_id
    }

    #[must_use]
    pub const fn state(&self) -> VerificationRunState {
        self.state
    }

    #[must_use]
    pub const fn version(&self) -> AggregateVersion {
        self.version
    }
}

fn validate_queue(record: &QueueVerificationRun, candidate: &Candidate) -> Result<(), DomainError> {
    if record.id.as_uuid().is_nil()
        || record.candidate_id.as_uuid().is_nil()
        || !valid_git_oid(&record.candidate_commit)
    {
        return Err(DomainError::InvalidArgument {
            field: "verification_run".into(),
            reason: "identifiers must be non-nil".into(),
        });
    }
    if record.candidate_id != candidate.record().id
        || record.candidate_commit != candidate.record().candidate_commit
    {
        return Err(DomainError::HeadMismatch);
    }
    Ok(())
}

fn invalid_run_transition(
    state: VerificationRunState,
    command: VerificationRunCommandKind,
) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_run_event(state: VerificationRunState, event: &str) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: event.into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use time::macros::datetime;
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(second: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(second))
    }

    fn reservation() -> ReserveCandidateArtifact {
        ReserveCandidateArtifact {
            id: id(1),
            reserved_candidate_id: id(2),
            attempt_id: id(3),
            package_id: id(5),
            revision_id: id(6),
            package_hash: Sha256Digest::of_bytes("package"),
            lease_id: id(4),
            fencing_token: FencingToken::new(7).expect("fence"),
            base_commit: GitObjectId::new("1".repeat(40)).expect("base"),
            candidate_commit: GitObjectId::new("2".repeat(40)).expect("commit"),
            tree_hash: GitObjectId::new("3".repeat(40)).expect("tree"),
            author_evidence_digest: Sha256Digest::of_bytes("evidence"),
            expected_bundle_digest: Sha256Digest::of_bytes("bundle"),
            expected_bundle_size_bytes: 128,
            chunk_digests: vec![Sha256Digest::of_bytes("chunk")],
            created_at: at(0),
            expires_at: at(60),
        }
    }

    fn complete_artifact() -> (CandidateArtifact, Vec<CandidateArtifactEvent>) {
        let reserved =
            CandidateArtifact::transition(None, &CandidateArtifactCommand::Reserve(reservation()))
                .expect("reserve");
        let assembling = CandidateArtifact::transition(
            Some(&reserved.aggregate),
            &CandidateArtifactCommand::BeginAssembly {
                expected_version: reserved.aggregate.version(),
                observed_at: at(1),
            },
        )
        .expect("assembly");
        let bundle = ArtifactRef {
            artifact_id: crate::ProtocolKey::new("candidate-bundle-1").expect("key"),
            uri: "artifact://candidate-artifacts/1/bundle".into(),
            digest: reservation().expected_bundle_digest,
        };
        let completed = CandidateArtifact::transition(
            Some(&assembling.aggregate),
            &CandidateArtifactCommand::Complete {
                expected_version: assembling.aggregate.version(),
                bundle,
                observed_size_bytes: reservation().expected_bundle_size_bytes,
                observed_chunk_digests: reservation().chunk_digests,
                completed_at: at(2),
            },
        )
        .expect("complete");
        let events = reserved
            .events
            .into_iter()
            .chain(assembling.events)
            .chain(completed.events.clone())
            .collect();
        (completed.aggregate, events)
    }

    #[test]
    fn artifact_completion_is_content_bound_replayable_and_terminal() {
        let (artifact, events) = complete_artifact();
        assert_eq!(artifact.state(), CandidateArtifactState::Complete);
        assert_eq!(
            CandidateArtifact::replay(&events).expect("replay"),
            artifact
        );
        let changed = CandidateArtifactCommand::Complete {
            expected_version: artifact.version(),
            bundle: artifact.bundle().expect("bundle").clone(),
            observed_size_bytes: 129,
            observed_chunk_digests: reservation().chunk_digests,
            completed_at: at(3),
        };
        assert!(matches!(
            CandidateArtifact::decide(Some(&artifact), &changed),
            Err(DomainError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn artifact_digest_size_chunk_and_expiry_mismatches_fail_closed() {
        let reserved =
            CandidateArtifact::transition(None, &CandidateArtifactCommand::Reserve(reservation()))
                .expect("reserve");
        let assembling = CandidateArtifact::transition(
            Some(&reserved.aggregate),
            &CandidateArtifactCommand::BeginAssembly {
                expected_version: reserved.aggregate.version(),
                observed_at: at(1),
            },
        )
        .expect("assembly");
        for (digest, size, chunks, time) in [
            (
                Sha256Digest::of_bytes("wrong"),
                128,
                reservation().chunk_digests,
                at(2),
            ),
            (
                reservation().expected_bundle_digest,
                127,
                vec![Sha256Digest::of_bytes("chunk")],
                at(2),
            ),
            (
                reservation().expected_bundle_digest,
                128,
                vec![Sha256Digest::of_bytes("wrong")],
                at(2),
            ),
            (
                reservation().expected_bundle_digest,
                128,
                vec![Sha256Digest::of_bytes("chunk")],
                at(60),
            ),
        ] {
            let error = CandidateArtifact::transition(
                Some(&assembling.aggregate),
                &CandidateArtifactCommand::Complete {
                    expected_version: assembling.aggregate.version(),
                    bundle: ArtifactRef {
                        artifact_id: crate::ProtocolKey::new("bundle").expect("key"),
                        uri: "artifact://bundle".into(),
                        digest,
                    },
                    observed_size_bytes: size,
                    observed_chunk_digests: chunks,
                    completed_at: time,
                },
            )
            .expect_err("mismatch");
            assert!(matches!(
                error,
                DomainError::EvidenceInvalid | DomainError::LeaseExpired
            ));
        }
    }

    #[test]
    fn validated_deserialization_rejects_complete_artifact_without_bundle() {
        let (artifact, _) = complete_artifact();
        let mut value = serde_json::to_value(artifact).expect("serialize");
        value["bundle"] = json!(null);
        assert!(serde_json::from_value::<CandidateArtifact>(value).is_err());
    }

    #[test]
    fn candidate_sealing_binds_exact_complete_artifact_and_is_immutable() {
        let (artifact, _) = complete_artifact();
        let command = SealCandidate {
            id: artifact.reserved_candidate_id(),
            branch: "refs/heads/agentforge/package-1/attempt-1".into(),
            sealed_at: at(3),
        };
        let sealed = Candidate::transition(None, &command, &artifact).expect("seal");
        assert_eq!(
            Candidate::replay(&sealed.events, &artifact).expect("replay"),
            sealed.aggregate
        );
        assert!(Candidate::transition(Some(&sealed.aggregate), &command, &artifact).is_err());

        let mut event = sealed.events[0].clone();
        let CandidateEvent::Sealed { record } = &mut event;
        record.tree_hash = GitObjectId::new("4".repeat(40)).expect("tree");
        assert_eq!(
            Candidate::apply_event(None, &event, &artifact).expect_err("tampered binding"),
            DomainError::CandidateArtifactNotComplete
        );
    }

    fn queued_run() -> (Candidate, VerificationRun) {
        let (artifact, _) = complete_artifact();
        let candidate = Candidate::transition(
            None,
            &SealCandidate {
                id: artifact.reserved_candidate_id(),
                branch: "refs/heads/agentforge/package-1/attempt-1".into(),
                sealed_at: at(3),
            },
            &artifact,
        )
        .expect("candidate")
        .aggregate;
        let run = VerificationRun::transition(
            None,
            &VerificationRunCommand::Queue(QueueVerificationRun {
                id: id(10),
                candidate_id: candidate.record().id,
                candidate_commit: candidate.record().candidate_commit.clone(),
                queued_at: at(3),
            }),
            &candidate,
        )
        .expect("queue")
        .aggregate;
        (candidate, run)
    }

    #[test]
    fn verification_pass_requires_three_equal_heads_and_replays() {
        let (candidate, queued) = queued_run();
        let provenance = VerificationRun::transition(
            Some(&queued),
            &VerificationRunCommand::StartProvenance {
                expected_version: queued.version(),
                started_at: at(4),
            },
            &candidate,
        )
        .expect("provenance");
        let review = VerificationRun::transition(
            Some(&provenance.aggregate),
            &VerificationRunCommand::StartReview {
                expected_version: provenance.aggregate.version(),
                started_at: at(5),
            },
            &candidate,
        )
        .expect("review");
        let head = candidate.record().candidate_commit.clone();
        let reproduce = VerificationRun::transition(
            Some(&review.aggregate),
            &VerificationRunCommand::StartReproduction {
                expected_version: review.aggregate.version(),
                reviewed_head: head.clone(),
                started_at: at(6),
            },
            &candidate,
        )
        .expect("reproduce");
        let pass = VerificationRun::transition(
            Some(&reproduce.aggregate),
            &VerificationRunCommand::Finalize {
                expected_version: reproduce.aggregate.version(),
                outcome: VerificationRunState::Pass,
                terminal_stage: VerificationStage::Reproducing,
                terminal_stage_result_id: id(12),
                reviewed_head: Some(head.clone()),
                tested_head: Some(head),
                evidence_digest: Sha256Digest::of_bytes("verification"),
                terminalized_at: at(7),
            },
            &candidate,
        )
        .expect("pass");
        let events = vec![
            VerificationRunEvent::Queued {
                record: QueueVerificationRun {
                    id: id(10),
                    candidate_id: candidate.record().id,
                    candidate_commit: candidate.record().candidate_commit.clone(),
                    queued_at: at(3),
                },
            },
            provenance.events[0].clone(),
            review.events[0].clone(),
            reproduce.events[0].clone(),
            pass.events[0].clone(),
        ];
        assert_eq!(pass.aggregate.state(), VerificationRunState::Pass);
        assert_eq!(
            VerificationRun::replay(&events, &candidate).expect("replay"),
            pass.aggregate
        );
    }

    #[test]
    fn early_failure_rejects_future_stage_heads_and_terminal_replay_mutation() {
        let (candidate, queued) = queued_run();
        let provenance = VerificationRun::transition(
            Some(&queued),
            &VerificationRunCommand::StartProvenance {
                expected_version: queued.version(),
                started_at: at(4),
            },
            &candidate,
        )
        .expect("provenance");
        let malformed = VerificationRunCommand::Finalize {
            expected_version: provenance.aggregate.version(),
            outcome: VerificationRunState::Fail,
            terminal_stage: VerificationStage::ProvenanceCheck,
            terminal_stage_result_id: id(12),
            reviewed_head: Some(candidate.record().candidate_commit.clone()),
            tested_head: None,
            evidence_digest: Sha256Digest::of_bytes("failure"),
            terminalized_at: at(5),
        };
        assert_eq!(
            VerificationRun::transition(Some(&provenance.aggregate), &malformed, &candidate)
                .expect_err("future head"),
            DomainError::HeadMismatch
        );

        let failed = VerificationRun::transition(
            Some(&provenance.aggregate),
            &VerificationRunCommand::Finalize {
                expected_version: provenance.aggregate.version(),
                outcome: VerificationRunState::Fail,
                terminal_stage: VerificationStage::ProvenanceCheck,
                terminal_stage_result_id: id(12),
                reviewed_head: None,
                tested_head: None,
                evidence_digest: Sha256Digest::of_bytes("failure"),
                terminalized_at: at(5),
            },
            &candidate,
        )
        .expect("fail");
        assert!(
            VerificationRun::apply_event(
                Some(&failed.aggregate),
                &VerificationRunEvent::Cancelled {
                    cancelled_at: at(6),
                },
                &candidate,
            )
            .is_err()
        );
    }

    #[test]
    fn verification_queue_rejects_a_different_candidate_lineage() {
        let (candidate, _) = queued_run();
        let error = VerificationRun::transition(
            None,
            &VerificationRunCommand::Queue(QueueVerificationRun {
                id: id(10),
                candidate_id: id(99),
                candidate_commit: candidate.record().candidate_commit.clone(),
                queued_at: at(3),
            }),
            &candidate,
        )
        .expect_err("candidate mismatch");
        assert_eq!(error, DomainError::HeadMismatch);
    }
}
