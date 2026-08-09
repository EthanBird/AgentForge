//! Version-bound governance cases, immutable decisions, and execution receipts.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        ActorId, AggregateVersion, ArtifactRef, AttemptId, BudgetAccountId, DecisionId,
        ExecutionReceiptId, ExecutorId, FencingToken, GovernanceCaseId, GovernanceExecutionClaimId,
        InvocationRunId, LeaseId, NodeId, PackageId, PackageRevisionId, PolicyRevisionId,
        ProjectId, ProtocolKey, ServerInstant, Sha256Digest,
    },
    state::Transition,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceCaseKind {
    PermissionRequest,
    BudgetChange,
    PlanPatchApproval,
    PolicyActivation,
    RoutingException,
    SecurityResponse,
    ConflictResolution,
    OutcomeUnknown,
    ManualIntegration,
    OperationalIntervention,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum GovernanceSubject {
    Project(ProjectId),
    Package(PackageId),
    PackageRevision(PackageRevisionId),
    Attempt(AttemptId),
    InvocationRun(InvocationRunId),
    BudgetAccount(BudgetAccountId),
    PolicyRevision(PolicyRevisionId),
    Executor(ExecutorId),
    Node(NodeId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionedSubject {
    pub subject: GovernanceSubject,
    pub expected_version: AggregateVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackageBinding {
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub package_hash: Sha256Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttemptFencingBinding {
    pub attempt_id: AttemptId,
    pub lease_id: LeaseId,
    pub author_fencing_token: FencingToken,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    Graceful,
    Reconcile,
    SecurityTerminate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TypedGovernanceAction {
    GrantCapability {
        scope: ProtocolKey,
        capability: ProtocolKey,
        limits_digest: Sha256Digest,
        until: ServerInstant,
    },
    RejectPermission {
        request_id: ProtocolKey,
        reason_code: ProtocolKey,
    },
    ApprovePlanPatch {
        plan_patch_id: ProtocolKey,
        expected_graph_version: u64,
    },
    ApproveBudgetChange {
        account_id: BudgetAccountId,
        delta_units: i64,
        category: ProtocolKey,
        expires_at: ServerInstant,
    },
    PauseProjectDispatch {
        project_id: ProjectId,
        expected_project_version: AggregateVersion,
    },
    ResumeProjectDispatch {
        project_id: ProjectId,
        expected_project_version: AggregateVersion,
    },
    CancelInvocationRun {
        run_id: InvocationRunId,
        expected_run_version: AggregateVersion,
        stop_mode: StopMode,
    },
    RevokeAuthorLease {
        lease_id: LeaseId,
        expected_generation: FencingToken,
        salvage_policy: ProtocolKey,
    },
    DrainExecutor {
        executor_id: ExecutorId,
        mode: ProtocolKey,
    },
    QuarantineNode {
        node_id: NodeId,
        evidence_digest: Sha256Digest,
    },
    ApproveRoutingException {
        routing_decision_digest: Sha256Digest,
        allowed_fingerprint: Sha256Digest,
        expires_at: ServerInstant,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApprovalQuorum {
    pub required: u16,
    pub policy_digest: Sha256Digest,
}

impl ApprovalQuorum {
    fn validate(self) -> Result<(), DomainError> {
        if self.required == 0 {
            Err(DomainError::InvalidArgument {
                field: "required_quorum".into(),
                reason: "must be non-zero".into(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutBehavior {
    Deny,
    Defer,
    Cancel,
    Supersede,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenGovernanceCase {
    pub id: GovernanceCaseId,
    pub project_id: ProjectId,
    pub kind: GovernanceCaseKind,
    pub risk: GovernanceRisk,
    pub subject: VersionedSubject,
    pub requested_by: ActorId,
    pub author_actor_id: Option<ActorId>,
    pub package_binding: Option<PackageBinding>,
    pub attempt_binding: Option<AttemptFencingBinding>,
    pub invocation_run_id: Option<InvocationRunId>,
    pub invocation_claim_generation: Option<u64>,
    pub policy_revision_id: PolicyRevisionId,
    pub normalized_action: TypedGovernanceAction,
    pub action_digest: Sha256Digest,
    pub resource_snapshot_digest: Sha256Digest,
    pub evidence_refs: Vec<ArtifactRef>,
    pub required_quorum: ApprovalQuorum,
    pub due_at: Option<ServerInstant>,
    pub expires_at: ServerInstant,
    pub timeout_behavior: TimeoutBehavior,
    pub opened_at: ServerInstant,
}

impl OpenGovernanceCase {
    fn validate(&self) -> Result<(), DomainError> {
        if self.subject.expected_version == AggregateVersion::ZERO {
            return Err(DomainError::InvalidArgument {
                field: "subject.expected_version".into(),
                reason: "must be non-zero".into(),
            });
        }
        self.required_quorum.validate()?;
        self.validate_action_subject_binding()?;
        if self.opened_at >= self.expires_at
            || self
                .due_at
                .is_some_and(|due| due < self.opened_at || due > self.expires_at)
        {
            return Err(DomainError::InvalidArgument {
                field: "case_window".into(),
                reason: "must satisfy opened_at <= due_at <= expires_at".into(),
            });
        }
        if self.compute_action_digest()? != self.action_digest {
            return Err(DomainError::EvidenceInvalid);
        }
        Ok(())
    }

    pub fn compute_action_digest(&self) -> Result<Sha256Digest, DomainError> {
        #[derive(Serialize)]
        struct Material<'a> {
            schema: u8,
            project_id: ProjectId,
            kind: GovernanceCaseKind,
            risk: GovernanceRisk,
            subject: VersionedSubject,
            authorization_epoch: u64,
            requested_by: ActorId,
            author_actor_id: Option<ActorId>,
            package_binding: &'a Option<PackageBinding>,
            attempt_binding: Option<AttemptFencingBinding>,
            invocation_run_id: Option<InvocationRunId>,
            invocation_claim_generation: Option<u64>,
            policy_revision_id: PolicyRevisionId,
            normalized_action: &'a TypedGovernanceAction,
            resource_snapshot_digest: Sha256Digest,
            required_quorum: ApprovalQuorum,
            expires_at: ServerInstant,
            timeout_behavior: TimeoutBehavior,
        }
        serde_json::to_vec(&Material {
            schema: 1,
            project_id: self.project_id,
            kind: self.kind,
            risk: self.risk,
            subject: self.subject,
            authorization_epoch: 1,
            requested_by: self.requested_by,
            author_actor_id: self.author_actor_id,
            package_binding: &self.package_binding,
            attempt_binding: self.attempt_binding,
            invocation_run_id: self.invocation_run_id,
            invocation_claim_generation: self.invocation_claim_generation,
            policy_revision_id: self.policy_revision_id,
            normalized_action: &self.normalized_action,
            resource_snapshot_digest: self.resource_snapshot_digest,
            required_quorum: self.required_quorum,
            expires_at: self.expires_at,
            timeout_behavior: self.timeout_behavior,
        })
        .map(Sha256Digest::of_bytes)
        .map_err(|_| DomainError::Internal)
    }

    fn validate_action_subject_binding(&self) -> Result<(), DomainError> {
        let valid = match (&self.subject.subject, &self.normalized_action) {
            (
                GovernanceSubject::BudgetAccount(subject_id),
                TypedGovernanceAction::ApproveBudgetChange { account_id, .. },
            ) => subject_id == account_id,
            (
                GovernanceSubject::Project(subject_id),
                TypedGovernanceAction::PauseProjectDispatch {
                    project_id,
                    expected_project_version,
                }
                | TypedGovernanceAction::ResumeProjectDispatch {
                    project_id,
                    expected_project_version,
                },
            ) => {
                subject_id == project_id
                    && self.subject.expected_version == *expected_project_version
            }
            (
                GovernanceSubject::InvocationRun(subject_id),
                TypedGovernanceAction::CancelInvocationRun {
                    run_id,
                    expected_run_version,
                    ..
                },
            ) => {
                subject_id == run_id
                    && self.subject.expected_version == *expected_run_version
                    && self.invocation_run_id == Some(*run_id)
                    && self
                        .invocation_claim_generation
                        .is_some_and(|generation| generation > 0)
            }
            (
                GovernanceSubject::Attempt(subject_id),
                TypedGovernanceAction::RevokeAuthorLease {
                    lease_id,
                    expected_generation,
                    ..
                },
            ) => self.attempt_binding.is_some_and(|binding| {
                binding.attempt_id == *subject_id
                    && binding.lease_id == *lease_id
                    && binding.author_fencing_token == *expected_generation
            }),
            (
                GovernanceSubject::Executor(subject_id),
                TypedGovernanceAction::DrainExecutor { executor_id, .. },
            ) => subject_id == executor_id,
            (
                GovernanceSubject::Node(subject_id),
                TypedGovernanceAction::QuarantineNode { node_id, .. },
            ) => subject_id == node_id,
            (
                _,
                TypedGovernanceAction::ApproveBudgetChange { .. }
                | TypedGovernanceAction::PauseProjectDispatch { .. }
                | TypedGovernanceAction::ResumeProjectDispatch { .. }
                | TypedGovernanceAction::CancelInvocationRun { .. }
                | TypedGovernanceAction::RevokeAuthorLease { .. }
                | TypedGovernanceAction::DrainExecutor { .. }
                | TypedGovernanceAction::QuarantineNode { .. },
            ) => false,
            _ => true,
        };
        if !valid {
            return Err(DomainError::GovernanceActionStale);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceCaseState {
    NeedsDecision,
    QuorumReached,
    Executing,
    Reconciling,
    Applied,
    Denied,
    ChangesRequested,
    Deferred,
    Expired,
    Cancelled,
    Superseded,
}

impl GovernanceCaseState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NeedsDecision => "needs_decision",
            Self::QuorumReached => "quorum_reached",
            Self::Executing => "executing",
            Self::Reconciling => "reconciling",
            Self::Applied => "applied",
            Self::Denied => "denied",
            Self::ChangesRequested => "changes_requested",
            Self::Deferred => "deferred",
            Self::Expired => "expired",
            Self::Cancelled => "cancelled",
            Self::Superseded => "superseded",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Applied
                | Self::Denied
                | Self::ChangesRequested
                | Self::Expired
                | Self::Cancelled
                | Self::Superseded
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionConclusion {
    Approve,
    Deny,
    RequestChanges,
    Defer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecisionSignature {
    pub signature_digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordDecision {
    pub id: DecisionId,
    pub case_id: GovernanceCaseId,
    pub case_version: AggregateVersion,
    pub authorization_epoch: u64,
    pub actor_id: ActorId,
    pub actor_role_snapshot: Sha256Digest,
    pub conclusion: DecisionConclusion,
    pub action_digest: Sha256Digest,
    pub policy_revision_id: PolicyRevisionId,
    pub rationale_code: ProtocolKey,
    pub note_ref: Option<ArtifactRef>,
    pub decided_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub signature: DecisionSignature,
}

impl RecordDecision {
    fn validate(&self) -> Result<(), DomainError> {
        if self.case_version == AggregateVersion::ZERO
            || self.authorization_epoch == 0
            || self.decided_at >= self.expires_at
        {
            return Err(DomainError::InvalidArgument {
                field: "decision_binding".into(),
                reason: "case version and a valid decision window are required".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecisionCommand {
    Record(Box<RecordDecision>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DecisionCommandKind {
    Record,
}

impl DecisionCommand {
    #[must_use]
    pub const fn kind(&self) -> DecisionCommandKind {
        DecisionCommandKind::Record
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DecisionEvent {
    Recorded(Box<RecordDecision>),
}

/// A single append-only authorization fact, not an execution result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub id: DecisionId,
    pub case_id: GovernanceCaseId,
    pub case_version: AggregateVersion,
    pub authorization_epoch: u64,
    pub actor_id: ActorId,
    pub actor_role_snapshot: Sha256Digest,
    pub conclusion: DecisionConclusion,
    pub action_digest: Sha256Digest,
    pub policy_revision_id: PolicyRevisionId,
    pub rationale_code: ProtocolKey,
    pub note_ref: Option<ArtifactRef>,
    pub decided_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub signature: DecisionSignature,
    pub version: AggregateVersion,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionApplicability {
    Applicable,
    Stale,
    Expired,
}

impl Decision {
    pub fn transition(
        current: Option<&Self>,
        command: &DecisionCommand,
    ) -> Result<Transition<Self, DecisionEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &DecisionCommand,
    ) -> Result<DecisionEvent, DomainError> {
        match (current, command) {
            (None, DecisionCommand::Record(record)) => {
                record.validate()?;
                Ok(DecisionEvent::Recorded(record.clone()))
            }
            (Some(_), _) => Err(DomainError::InvalidTransition {
                from: "recorded".into(),
                command: "record_decision".into(),
            }),
        }
    }

    pub fn apply_event(current: Option<&Self>, event: &DecisionEvent) -> Result<Self, DomainError> {
        match (current, event) {
            (None, DecisionEvent::Recorded(record)) => {
                record.validate()?;
                Ok(Self {
                    id: record.id,
                    case_id: record.case_id,
                    case_version: record.case_version,
                    authorization_epoch: record.authorization_epoch,
                    actor_id: record.actor_id,
                    actor_role_snapshot: record.actor_role_snapshot,
                    conclusion: record.conclusion,
                    action_digest: record.action_digest,
                    policy_revision_id: record.policy_revision_id,
                    rationale_code: record.rationale_code.clone(),
                    note_ref: record.note_ref.clone(),
                    decided_at: record.decided_at,
                    expires_at: record.expires_at,
                    signature: record.signature,
                    version: AggregateVersion::new(1),
                })
            }
            (Some(_), _) => Err(DomainError::InvalidTransition {
                from: "recorded".into(),
                command: "decision_recorded".into(),
            }),
        }
    }

    pub fn replay(events: &[DecisionEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "decision",
        })
    }

    #[must_use]
    pub fn applicability(
        &self,
        case: &GovernanceCase,
        now: ServerInstant,
    ) -> DecisionApplicability {
        if now >= self.expires_at || now >= case.expires_at {
            DecisionApplicability::Expired
        } else if self.case_id != case.id
            || self.authorization_epoch != case.authorization_epoch
            || self.action_digest != case.action_digest
            || self.policy_revision_id != case.policy_revision_id
        {
            DecisionApplicability::Stale
        } else {
            DecisionApplicability::Applicable
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionReceiptStatus {
    Succeeded,
    Failed,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernanceExecutionClaim {
    pub id: GovernanceExecutionClaimId,
    pub generation: u64,
    pub holder_actor_id: ActorId,
    pub token_hash: Sha256Digest,
    pub authorization_digest: Sha256Digest,
    pub issued_at: ServerInstant,
    pub expires_at: ServerInstant,
}

impl GovernanceExecutionClaim {
    fn validate(self, now: ServerInstant) -> Result<(), DomainError> {
        if self.id.as_uuid().is_nil()
            || self.generation == 0
            || self.holder_actor_id.as_uuid().is_nil()
            || self.issued_at > now
            || now >= self.expires_at
            || !digest_is_nonzero(self.token_hash)
            || !digest_is_nonzero(self.authorization_digest)
        {
            return Err(DomainError::GovernanceExecutionClaimStale);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub id: ExecutionReceiptId,
    pub case_id: GovernanceCaseId,
    pub action_digest: Sha256Digest,
    pub execution_claim_id: GovernanceExecutionClaimId,
    pub execution_claim_generation: u64,
    pub executor_actor_id: ActorId,
    pub status: ExecutionReceiptStatus,
    pub external_effect_key: Option<ProtocolKey>,
    pub effect_digest: Sha256Digest,
    pub evidence_refs: Vec<ArtifactRef>,
    pub started_at: ServerInstant,
    pub observed_at: ServerInstant,
}

impl ExecutionReceipt {
    fn validate(&self, case: &GovernanceCase) -> Result<(), DomainError> {
        if self.case_id != case.id {
            return Err(DomainError::GovernanceCaseStale);
        }
        if self.action_digest != case.action_digest {
            return Err(DomainError::GovernanceActionStale);
        }
        let claim = case
            .execution_claim
            .ok_or(DomainError::GovernanceExecutionClaimStale)?;
        if self.execution_claim_id != claim.id
            || self.execution_claim_generation != claim.generation
            || self.executor_actor_id != claim.holder_actor_id
            || self.started_at < claim.issued_at
            || self.observed_at >= claim.expires_at
        {
            return Err(DomainError::GovernanceExecutionClaimStale);
        }
        if self.observed_at < self.started_at {
            return Err(DomainError::EvidenceInvalid);
        }
        if self.status == ExecutionReceiptStatus::Succeeded
            && (self.external_effect_key.is_none()
                || !digest_is_nonzero(self.effect_digest)
                || self.evidence_refs.is_empty()
                || !self.evidence_refs.iter().all(artifact_ref_is_valid))
        {
            return Err(DomainError::EvidenceInvalid);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecisionSummary {
    pub id: DecisionId,
    pub case_id: GovernanceCaseId,
    pub case_version: AggregateVersion,
    pub authorization_epoch: u64,
    pub actor_id: ActorId,
    pub actor_role_snapshot: Sha256Digest,
    pub conclusion: DecisionConclusion,
    pub action_digest: Sha256Digest,
    pub policy_revision_id: PolicyRevisionId,
    pub decided_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub accepted_at: ServerInstant,
    pub authorization_evidence_digest: Sha256Digest,
    pub signature: DecisionSignature,
}

impl DecisionSummary {
    fn accepted(
        decision: &Decision,
        accepted_at: ServerInstant,
        authorization_evidence_digest: Sha256Digest,
    ) -> Self {
        Self {
            id: decision.id,
            case_id: decision.case_id,
            case_version: decision.case_version,
            authorization_epoch: decision.authorization_epoch,
            actor_id: decision.actor_id,
            actor_role_snapshot: decision.actor_role_snapshot,
            conclusion: decision.conclusion,
            action_digest: decision.action_digest,
            policy_revision_id: decision.policy_revision_id,
            decided_at: decision.decided_at,
            expires_at: decision.expires_at,
            accepted_at,
            authorization_evidence_digest,
            signature: decision.signature,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GovernanceCaseCommandKind {
    Open,
    RecordDecision,
    BeginApprovedAction,
    RecordExecutionReceipt,
    Expire,
    Supersede,
    Cancel,
}

impl GovernanceCaseCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open_governance_case",
            Self::RecordDecision => "record_decision",
            Self::BeginApprovedAction => "begin_approved_action",
            Self::RecordExecutionReceipt => "record_execution_receipt",
            Self::Expire => "expire_governance_case",
            Self::Supersede => "supersede_governance_case",
            Self::Cancel => "cancel_governance_case",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GovernanceCaseCommand {
    Open(Box<OpenGovernanceCase>),
    RecordDecision {
        expected_version: AggregateVersion,
        decision: Box<Decision>,
        actor_is_independent: bool,
        actor_is_authorized: bool,
        authorization_evidence_digest: Sha256Digest,
        now: ServerInstant,
    },
    BeginApprovedAction {
        expected_version: AggregateVersion,
        observed_subject_version: AggregateVersion,
        observed_policy_revision_id: PolicyRevisionId,
        observed_action_digest: Sha256Digest,
        observed_attempt_binding: Option<AttemptFencingBinding>,
        observed_invocation_claim_generation: Option<u64>,
        execution_claim: Box<GovernanceExecutionClaim>,
        now: ServerInstant,
    },
    RecordExecutionReceipt {
        expected_version: AggregateVersion,
        receipt: Box<ExecutionReceipt>,
    },
    Expire {
        expected_version: AggregateVersion,
        now: ServerInstant,
    },
    Supersede {
        expected_version: AggregateVersion,
        replacement_case_id: Option<GovernanceCaseId>,
        reason_code: String,
    },
    Cancel {
        expected_version: AggregateVersion,
        reason_code: String,
        cancelled_at: ServerInstant,
    },
}

impl GovernanceCaseCommand {
    #[must_use]
    pub const fn kind(&self) -> GovernanceCaseCommandKind {
        match self {
            Self::Open(_) => GovernanceCaseCommandKind::Open,
            Self::RecordDecision { .. } => GovernanceCaseCommandKind::RecordDecision,
            Self::BeginApprovedAction { .. } => GovernanceCaseCommandKind::BeginApprovedAction,
            Self::RecordExecutionReceipt { .. } => {
                GovernanceCaseCommandKind::RecordExecutionReceipt
            }
            Self::Expire { .. } => GovernanceCaseCommandKind::Expire,
            Self::Supersede { .. } => GovernanceCaseCommandKind::Supersede,
            Self::Cancel { .. } => GovernanceCaseCommandKind::Cancel,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Open(_) => None,
            Self::RecordDecision {
                expected_version, ..
            }
            | Self::BeginApprovedAction {
                expected_version, ..
            }
            | Self::RecordExecutionReceipt {
                expected_version, ..
            }
            | Self::Expire {
                expected_version, ..
            }
            | Self::Supersede {
                expected_version, ..
            }
            | Self::Cancel {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GovernanceCaseEvent {
    Opened(Box<OpenGovernanceCase>),
    DecisionRecorded {
        decision: DecisionSummary,
    },
    DeniedByDecision {
        decision: DecisionSummary,
    },
    ChangesRequested {
        decision: DecisionSummary,
    },
    Deferred {
        decision: DecisionSummary,
    },
    ApprovedActionStarted {
        execution_claim: GovernanceExecutionClaim,
        observed_subject_version: AggregateVersion,
        observed_policy_revision_id: PolicyRevisionId,
        observed_action_digest: Sha256Digest,
        observed_attempt_binding: Option<AttemptFencingBinding>,
        observed_invocation_claim_generation: Option<u64>,
        started_at: ServerInstant,
    },
    ExecutionReceiptRecorded {
        receipt: Box<ExecutionReceipt>,
    },
    Expired {
        expired_at: ServerInstant,
        default_action: TimeoutBehavior,
    },
    Superseded {
        replacement_case_id: Option<GovernanceCaseId>,
        reason_code: String,
    },
    Cancelled {
        reason_code: String,
        cancelled_at: ServerInstant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernanceCase {
    pub id: GovernanceCaseId,
    pub project_id: ProjectId,
    pub kind: GovernanceCaseKind,
    pub risk: GovernanceRisk,
    pub subject: VersionedSubject,
    pub authorization_epoch: u64,
    pub requested_by: ActorId,
    pub author_actor_id: Option<ActorId>,
    pub package_binding: Option<PackageBinding>,
    pub attempt_binding: Option<AttemptFencingBinding>,
    pub invocation_run_id: Option<InvocationRunId>,
    pub invocation_claim_generation: Option<u64>,
    pub policy_revision_id: PolicyRevisionId,
    pub normalized_action: TypedGovernanceAction,
    pub action_digest: Sha256Digest,
    pub resource_snapshot_digest: Sha256Digest,
    pub evidence_refs: Vec<ArtifactRef>,
    pub required_quorum: ApprovalQuorum,
    pub due_at: Option<ServerInstant>,
    pub expires_at: ServerInstant,
    pub timeout_behavior: TimeoutBehavior,
    pub state: GovernanceCaseState,
    pub decisions: Vec<DecisionSummary>,
    pub execution_claim_generation: Option<u64>,
    pub execution_claim: Option<GovernanceExecutionClaim>,
    pub execution_receipt_id: Option<ExecutionReceiptId>,
    pub replacement_case_id: Option<GovernanceCaseId>,
    pub terminal_reason: Option<String>,
    pub opened_at: ServerInstant,
    pub version: AggregateVersion,
}

impl GovernanceCase {
    pub fn transition(
        current: Option<&Self>,
        command: &GovernanceCaseCommand,
    ) -> Result<Transition<Self, GovernanceCaseEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &GovernanceCaseCommand,
    ) -> Result<GovernanceCaseEvent, DomainError> {
        match (current, command) {
            (None, GovernanceCaseCommand::Open(open)) => {
                open.validate()?;
                Ok(GovernanceCaseEvent::Opened(open.clone()))
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "governance_case",
            }),
            (Some(case), GovernanceCaseCommand::Open(_)) => {
                Err(invalid_case(case.state, command.kind()))
            }
            (Some(case), command) => {
                if case.state.is_terminal() {
                    return Err(invalid_case(case.state, command.kind()));
                }
                if command.expected_version() != Some(case.version) {
                    return Err(DomainError::GovernanceCaseStale);
                }
                match command {
                    GovernanceCaseCommand::RecordDecision {
                        decision,
                        actor_is_independent,
                        actor_is_authorized,
                        authorization_evidence_digest,
                        now,
                        ..
                    } if matches!(
                        case.state,
                        GovernanceCaseState::NeedsDecision | GovernanceCaseState::Deferred
                    ) =>
                    {
                        match decision.applicability(case, *now) {
                            DecisionApplicability::Applicable => {}
                            DecisionApplicability::Stale => {
                                return Err(DomainError::GovernanceActionStale);
                            }
                            DecisionApplicability::Expired => {
                                return Err(DomainError::GovernanceDecisionExpired);
                            }
                        }
                        if !*actor_is_authorized {
                            return Err(DomainError::PolicyDenied);
                        }
                        if !*actor_is_independent
                            && matches!(case.risk, GovernanceRisk::High | GovernanceRisk::Critical)
                        {
                            return Err(DomainError::GovernanceSelfApprovalForbidden);
                        }
                        if matches!(case.risk, GovernanceRisk::High | GovernanceRisk::Critical)
                            && (decision.actor_id == case.requested_by
                                || case.author_actor_id == Some(decision.actor_id))
                        {
                            return Err(DomainError::GovernanceSelfApprovalForbidden);
                        }
                        if case.decisions.iter().any(|known| known.id == decision.id)
                            || case
                                .decisions
                                .iter()
                                .any(|known| known.actor_id == decision.actor_id)
                        {
                            return Err(DomainError::PolicyDenied);
                        }
                        let summary = DecisionSummary::accepted(
                            decision,
                            *now,
                            *authorization_evidence_digest,
                        );
                        match decision.conclusion {
                            DecisionConclusion::Approve => {
                                Ok(GovernanceCaseEvent::DecisionRecorded { decision: summary })
                            }
                            DecisionConclusion::Deny => {
                                Ok(GovernanceCaseEvent::DeniedByDecision { decision: summary })
                            }
                            DecisionConclusion::RequestChanges => {
                                Ok(GovernanceCaseEvent::ChangesRequested { decision: summary })
                            }
                            DecisionConclusion::Defer => {
                                Ok(GovernanceCaseEvent::Deferred { decision: summary })
                            }
                        }
                    }
                    GovernanceCaseCommand::BeginApprovedAction {
                        observed_subject_version,
                        observed_policy_revision_id,
                        observed_action_digest,
                        observed_attempt_binding,
                        observed_invocation_claim_generation,
                        execution_claim,
                        now,
                        ..
                    } if case.state == GovernanceCaseState::QuorumReached => {
                        validate_execution_start(
                            case,
                            ObservedExecutionBinding {
                                subject_version: *observed_subject_version,
                                policy_revision_id: *observed_policy_revision_id,
                                action_digest: *observed_action_digest,
                                attempt_binding: *observed_attempt_binding,
                                invocation_claim_generation: *observed_invocation_claim_generation,
                            },
                            **execution_claim,
                            *now,
                        )?;
                        Ok(GovernanceCaseEvent::ApprovedActionStarted {
                            execution_claim: **execution_claim,
                            observed_subject_version: *observed_subject_version,
                            observed_policy_revision_id: *observed_policy_revision_id,
                            observed_action_digest: *observed_action_digest,
                            observed_attempt_binding: *observed_attempt_binding,
                            observed_invocation_claim_generation:
                                *observed_invocation_claim_generation,
                            started_at: *now,
                        })
                    }
                    GovernanceCaseCommand::BeginApprovedAction { .. }
                        if matches!(
                            case.state,
                            GovernanceCaseState::NeedsDecision | GovernanceCaseState::Deferred
                        ) =>
                    {
                        Err(DomainError::GovernanceQuorumUnmet)
                    }
                    GovernanceCaseCommand::RecordExecutionReceipt { receipt, .. }
                        if matches!(
                            case.state,
                            GovernanceCaseState::Executing | GovernanceCaseState::Reconciling
                        ) =>
                    {
                        receipt.validate(case)?;
                        if Some(receipt.execution_claim_generation)
                            != case.execution_claim_generation
                        {
                            return Err(DomainError::GovernanceExecutionClaimStale);
                        }
                        Ok(GovernanceCaseEvent::ExecutionReceiptRecorded {
                            receipt: receipt.clone(),
                        })
                    }
                    GovernanceCaseCommand::Expire { now, .. } if *now >= case.expires_at => {
                        Ok(GovernanceCaseEvent::Expired {
                            expired_at: *now,
                            default_action: case.timeout_behavior,
                        })
                    }
                    GovernanceCaseCommand::Supersede {
                        replacement_case_id,
                        reason_code,
                        ..
                    } if valid_reason(reason_code)
                        && replacement_case_id.is_none_or(|id| id != case.id) =>
                    {
                        Ok(GovernanceCaseEvent::Superseded {
                            replacement_case_id: *replacement_case_id,
                            reason_code: reason_code.clone(),
                        })
                    }
                    GovernanceCaseCommand::Cancel {
                        reason_code,
                        cancelled_at,
                        ..
                    } if !matches!(
                        case.state,
                        GovernanceCaseState::Executing | GovernanceCaseState::Reconciling
                    ) && valid_reason(reason_code) =>
                    {
                        Ok(GovernanceCaseEvent::Cancelled {
                            reason_code: reason_code.clone(),
                            cancelled_at: *cancelled_at,
                        })
                    }
                    _ => Err(invalid_case(case.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &GovernanceCaseEvent,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, GovernanceCaseEvent::Opened(open)) => {
                open.validate()?;
                Ok(Self {
                    id: open.id,
                    project_id: open.project_id,
                    kind: open.kind,
                    risk: open.risk,
                    subject: open.subject,
                    authorization_epoch: 1,
                    requested_by: open.requested_by,
                    author_actor_id: open.author_actor_id,
                    package_binding: open.package_binding.clone(),
                    attempt_binding: open.attempt_binding,
                    invocation_run_id: open.invocation_run_id,
                    invocation_claim_generation: open.invocation_claim_generation,
                    policy_revision_id: open.policy_revision_id,
                    normalized_action: open.normalized_action.clone(),
                    action_digest: open.action_digest,
                    resource_snapshot_digest: open.resource_snapshot_digest,
                    evidence_refs: open.evidence_refs.clone(),
                    required_quorum: open.required_quorum,
                    due_at: open.due_at,
                    expires_at: open.expires_at,
                    timeout_behavior: open.timeout_behavior,
                    state: GovernanceCaseState::NeedsDecision,
                    decisions: Vec::new(),
                    execution_claim_generation: None,
                    execution_claim: None,
                    execution_receipt_id: None,
                    replacement_case_id: None,
                    terminal_reason: None,
                    opened_at: open.opened_at,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "governance_case",
            }),
            (Some(case), _) if case.state.is_terminal() => {
                Err(invalid_case_event(case.state, event))
            }
            (Some(case), GovernanceCaseEvent::DecisionRecorded { decision })
                if matches!(
                    case.state,
                    GovernanceCaseState::NeedsDecision | GovernanceCaseState::Deferred
                ) =>
            {
                validate_accepted_decision(case, decision, DecisionConclusion::Approve)?;
                let mut next = case.next_version()?;
                next.decisions.push(*decision);
                let approvals = next
                    .decisions
                    .iter()
                    .filter(|known| known.conclusion == DecisionConclusion::Approve)
                    .count();
                next.state = if approvals >= usize::from(next.required_quorum.required) {
                    GovernanceCaseState::QuorumReached
                } else {
                    GovernanceCaseState::NeedsDecision
                };
                Ok(next)
            }
            (Some(case), GovernanceCaseEvent::DeniedByDecision { decision })
                if matches!(
                    case.state,
                    GovernanceCaseState::NeedsDecision | GovernanceCaseState::Deferred
                ) =>
            {
                validate_accepted_decision(case, decision, DecisionConclusion::Deny)?;
                case.with_decision_terminal(
                    *decision,
                    GovernanceCaseState::Denied,
                    "decision_denied",
                )
            }
            (Some(case), GovernanceCaseEvent::ChangesRequested { decision })
                if matches!(
                    case.state,
                    GovernanceCaseState::NeedsDecision | GovernanceCaseState::Deferred
                ) =>
            {
                validate_accepted_decision(case, decision, DecisionConclusion::RequestChanges)?;
                case.with_decision_terminal(
                    *decision,
                    GovernanceCaseState::ChangesRequested,
                    "changes_requested",
                )
            }
            (Some(case), GovernanceCaseEvent::Deferred { decision })
                if matches!(
                    case.state,
                    GovernanceCaseState::NeedsDecision | GovernanceCaseState::Deferred
                ) =>
            {
                validate_accepted_decision(case, decision, DecisionConclusion::Defer)?;
                let mut next = case.next_version()?;
                next.decisions.push(*decision);
                next.state = GovernanceCaseState::Deferred;
                Ok(next)
            }
            (
                Some(case),
                GovernanceCaseEvent::ApprovedActionStarted {
                    execution_claim,
                    observed_subject_version,
                    observed_policy_revision_id,
                    observed_action_digest,
                    observed_attempt_binding,
                    observed_invocation_claim_generation,
                    started_at,
                },
            ) if case.state == GovernanceCaseState::QuorumReached => {
                validate_execution_start(
                    case,
                    ObservedExecutionBinding {
                        subject_version: *observed_subject_version,
                        policy_revision_id: *observed_policy_revision_id,
                        action_digest: *observed_action_digest,
                        attempt_binding: *observed_attempt_binding,
                        invocation_claim_generation: *observed_invocation_claim_generation,
                    },
                    *execution_claim,
                    *started_at,
                )?;
                let mut next = case.next_version()?;
                next.state = GovernanceCaseState::Executing;
                next.execution_claim_generation = Some(execution_claim.generation);
                next.execution_claim = Some(*execution_claim);
                Ok(next)
            }
            (Some(case), GovernanceCaseEvent::ExecutionReceiptRecorded { receipt })
                if matches!(
                    case.state,
                    GovernanceCaseState::Executing | GovernanceCaseState::Reconciling
                ) =>
            {
                receipt.validate(case)?;
                if Some(receipt.execution_claim_generation) != case.execution_claim_generation {
                    return Err(DomainError::GovernanceExecutionClaimStale);
                }
                let mut next = case.next_version()?;
                next.execution_receipt_id = Some(receipt.id);
                next.state = match receipt.status {
                    ExecutionReceiptStatus::Succeeded => GovernanceCaseState::Applied,
                    ExecutionReceiptStatus::Failed => GovernanceCaseState::Denied,
                    ExecutionReceiptStatus::OutcomeUnknown => GovernanceCaseState::Reconciling,
                };
                Ok(next)
            }
            (
                Some(case),
                GovernanceCaseEvent::Expired {
                    expired_at,
                    default_action,
                },
            ) if *expired_at >= case.expires_at && *default_action == case.timeout_behavior => {
                let mut next = case.next_version()?;
                next.state = GovernanceCaseState::Expired;
                next.terminal_reason = Some(
                    match default_action {
                        TimeoutBehavior::Deny => "timeout_default_deny",
                        TimeoutBehavior::Defer => "timeout_default_defer",
                        TimeoutBehavior::Cancel => "timeout_default_cancel",
                        TimeoutBehavior::Supersede => "timeout_default_supersede",
                    }
                    .into(),
                );
                Ok(next)
            }
            (
                Some(case),
                GovernanceCaseEvent::Superseded {
                    replacement_case_id,
                    reason_code,
                },
            ) if valid_reason(reason_code)
                && replacement_case_id.is_none_or(|id| id != case.id) =>
            {
                let mut next = case.next_version()?;
                next.state = GovernanceCaseState::Superseded;
                next.replacement_case_id = *replacement_case_id;
                next.terminal_reason = Some(reason_code.clone());
                Ok(next)
            }
            (
                Some(case),
                GovernanceCaseEvent::Cancelled {
                    reason_code,
                    cancelled_at: _,
                },
            ) if !matches!(
                case.state,
                GovernanceCaseState::Executing | GovernanceCaseState::Reconciling
            ) && valid_reason(reason_code) =>
            {
                let mut next = case.next_version()?;
                next.state = GovernanceCaseState::Cancelled;
                next.terminal_reason = Some(reason_code.clone());
                Ok(next)
            }
            (Some(case), _) => Err(invalid_case_event(case.state, event)),
        }
    }

    pub fn replay(events: &[GovernanceCaseEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "governance_case",
        })
    }

    fn next_version(&self) -> Result<Self, DomainError> {
        let mut next = self.clone();
        next.version = self.version.checked_next()?;
        Ok(next)
    }

    fn with_decision_terminal(
        &self,
        decision: DecisionSummary,
        state: GovernanceCaseState,
        reason: &str,
    ) -> Result<Self, DomainError> {
        let mut next = self.next_version()?;
        next.decisions.push(decision);
        next.state = state;
        next.terminal_reason = Some(reason.into());
        Ok(next)
    }
}

fn validate_accepted_decision(
    case: &GovernanceCase,
    decision: &DecisionSummary,
    expected_conclusion: DecisionConclusion,
) -> Result<(), DomainError> {
    let binding_is_valid = decision.case_id == case.id
        && decision.case_version == case.version
        && decision.authorization_epoch == case.authorization_epoch
        && decision.policy_revision_id == case.policy_revision_id
        && decision.action_digest == case.action_digest;
    let time_is_valid = decision.decided_at <= decision.accepted_at
        && decision.accepted_at < decision.expires_at
        && decision.accepted_at < case.expires_at;
    let authorization_is_valid = digest_is_nonzero(decision.actor_role_snapshot)
        && digest_is_nonzero(decision.authorization_evidence_digest)
        && digest_is_nonzero(decision.signature.signature_digest)
        && (!matches!(case.risk, GovernanceRisk::High | GovernanceRisk::Critical)
            || (decision.actor_id != case.requested_by
                && case.author_actor_id != Some(decision.actor_id)));
    let identity_is_unique = !case
        .decisions
        .iter()
        .any(|known| known.id == decision.id || known.actor_id == decision.actor_id);
    if decision.conclusion != expected_conclusion
        || !binding_is_valid
        || !time_is_valid
        || !authorization_is_valid
        || !identity_is_unique
    {
        return Err(DomainError::EvidenceInvalid);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ObservedExecutionBinding {
    subject_version: AggregateVersion,
    policy_revision_id: PolicyRevisionId,
    action_digest: Sha256Digest,
    attempt_binding: Option<AttemptFencingBinding>,
    invocation_claim_generation: Option<u64>,
}

fn validate_execution_start(
    case: &GovernanceCase,
    observed: ObservedExecutionBinding,
    execution_claim: GovernanceExecutionClaim,
    started_at: ServerInstant,
) -> Result<(), DomainError> {
    if started_at >= case.expires_at {
        return Err(DomainError::GovernanceDecisionExpired);
    }
    if observed.subject_version != case.subject.expected_version
        || observed.policy_revision_id != case.policy_revision_id
        || observed.action_digest != case.action_digest
        || observed.attempt_binding != case.attempt_binding
        || observed.invocation_claim_generation != case.invocation_claim_generation
    {
        return Err(DomainError::GovernanceActionStale);
    }
    execution_claim.validate(started_at)?;
    let applicable_approvals = case
        .decisions
        .iter()
        .filter(|decision| {
            decision.conclusion == DecisionConclusion::Approve
                && decision.case_id == case.id
                && decision.authorization_epoch == case.authorization_epoch
                && decision.policy_revision_id == case.policy_revision_id
                && decision.action_digest == case.action_digest
                && (decision.actor_id != case.requested_by
                    || !matches!(case.risk, GovernanceRisk::High | GovernanceRisk::Critical))
                && (case.author_actor_id != Some(decision.actor_id)
                    || !matches!(case.risk, GovernanceRisk::High | GovernanceRisk::Critical))
                && started_at < decision.expires_at
        })
        .count();
    if applicable_approvals < usize::from(case.required_quorum.required) {
        return Err(DomainError::GovernanceDecisionExpired);
    }
    Ok(())
}

fn digest_is_nonzero(digest: Sha256Digest) -> bool {
    digest.as_bytes().iter().any(|byte| *byte != 0)
}

fn artifact_ref_is_valid(reference: &ArtifactRef) -> bool {
    ProtocolKey::new(reference.artifact_id.as_str()).is_ok()
        && !reference.uri.trim().is_empty()
        && !reference.uri.chars().any(char::is_control)
        && digest_is_nonzero(reference.digest)
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= 96
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn invalid_case(state: GovernanceCaseState, command: GovernanceCaseCommandKind) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_case_event(state: GovernanceCaseState, event: &GovernanceCaseEvent) -> DomainError {
    let command = match event {
        GovernanceCaseEvent::Opened(_) => "opened",
        GovernanceCaseEvent::DecisionRecorded { .. } => "decision_recorded",
        GovernanceCaseEvent::DeniedByDecision { .. } => "denied_by_decision",
        GovernanceCaseEvent::ChangesRequested { .. } => "changes_requested",
        GovernanceCaseEvent::Deferred { .. } => "deferred",
        GovernanceCaseEvent::ApprovedActionStarted { .. } => "approved_action_started",
        GovernanceCaseEvent::ExecutionReceiptRecorded { .. } => "execution_receipt_recorded",
        GovernanceCaseEvent::Expired { .. } => "expired",
        GovernanceCaseEvent::Superseded { .. } => "superseded",
        GovernanceCaseEvent::Cancelled { .. } => "cancelled",
    };
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.into(),
    }
}

#[cfg(test)]
mod tests {
    use time::{Duration, macros::datetime};
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(seconds: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-08 00:00 UTC) + Duration::seconds(seconds))
    }

    fn execution_claim(
        issued_at: ServerInstant,
        expires_at: ServerInstant,
    ) -> GovernanceExecutionClaim {
        GovernanceExecutionClaim {
            id: id(13),
            generation: 1,
            holder_actor_id: id(14),
            token_hash: Sha256Digest::of_bytes(b"claim-token"),
            authorization_digest: Sha256Digest::of_bytes(b"claim-authorization"),
            issued_at,
            expires_at,
        }
    }

    fn open() -> OpenGovernanceCase {
        let mut open = OpenGovernanceCase {
            id: id(1),
            project_id: id(2),
            kind: GovernanceCaseKind::OperationalIntervention,
            risk: GovernanceRisk::High,
            subject: VersionedSubject {
                subject: GovernanceSubject::InvocationRun(id(3)),
                expected_version: AggregateVersion::new(4),
            },
            requested_by: id(5),
            author_actor_id: Some(id(10)),
            package_binding: None,
            attempt_binding: None,
            invocation_run_id: Some(id(3)),
            invocation_claim_generation: Some(4),
            policy_revision_id: id(6),
            normalized_action: TypedGovernanceAction::CancelInvocationRun {
                run_id: id(3),
                expected_run_version: AggregateVersion::new(4),
                stop_mode: StopMode::Reconcile,
            },
            action_digest: Sha256Digest::of_bytes(b"placeholder"),
            resource_snapshot_digest: Sha256Digest::of_bytes(b"snapshot"),
            evidence_refs: vec![],
            required_quorum: ApprovalQuorum {
                required: 1,
                policy_digest: Sha256Digest::of_bytes(b"quorum"),
            },
            due_at: Some(at(5)),
            expires_at: at(10),
            timeout_behavior: TimeoutBehavior::Deny,
            opened_at: at(0),
        };
        open.action_digest = open.compute_action_digest().expect("digest");
        open
    }

    fn decision(case: &GovernanceCase, case_version: AggregateVersion) -> Decision {
        Decision::transition(
            None,
            &DecisionCommand::Record(Box::new(RecordDecision {
                id: id(7),
                case_id: case.id,
                case_version,
                authorization_epoch: case.authorization_epoch,
                actor_id: id(8),
                actor_role_snapshot: Sha256Digest::of_bytes(b"role"),
                conclusion: DecisionConclusion::Approve,
                action_digest: case.action_digest,
                policy_revision_id: case.policy_revision_id,
                rationale_code: ProtocolKey::new("approved_after_review").expect("key"),
                note_ref: None,
                decided_at: at(1),
                expires_at: at(9),
                signature: DecisionSignature {
                    signature_digest: Sha256Digest::of_bytes(b"signature"),
                },
            })),
        )
        .expect("decision")
        .aggregate
    }

    fn record_approval(case: &GovernanceCase) -> Transition<GovernanceCase, GovernanceCaseEvent> {
        let approval = decision(case, case.version);
        GovernanceCase::transition(
            Some(case),
            &GovernanceCaseCommand::RecordDecision {
                expected_version: case.version,
                decision: Box::new(approval),
                actor_is_independent: true,
                actor_is_authorized: true,
                authorization_evidence_digest: Sha256Digest::of_bytes(b"authorization"),
                now: at(2),
            },
        )
        .expect("record approval")
    }

    #[test]
    fn decision_is_immutable_and_remains_bound_to_the_authorization_epoch() {
        let opened =
            GovernanceCase::transition(None, &GovernanceCaseCommand::Open(Box::new(open())))
                .expect("open");
        let approval = decision(&opened.aggregate, opened.aggregate.version);
        assert_eq!(
            approval.applicability(&opened.aggregate, at(2)),
            DecisionApplicability::Applicable
        );
        let changed = GovernanceCase {
            version: AggregateVersion::new(2),
            ..opened.aggregate.clone()
        };
        assert_eq!(
            approval.applicability(&changed, at(2)),
            DecisionApplicability::Applicable
        );
        assert!(matches!(
            Decision::decide(
                Some(&approval),
                &DecisionCommand::Record(Box::new(RecordDecision {
                    id: id(9),
                    case_id: approval.case_id,
                    case_version: approval.case_version,
                    authorization_epoch: approval.authorization_epoch,
                    actor_id: id(10),
                    actor_role_snapshot: approval.actor_role_snapshot,
                    conclusion: DecisionConclusion::Deny,
                    action_digest: approval.action_digest,
                    policy_revision_id: approval.policy_revision_id,
                    rationale_code: ProtocolKey::new("deny").expect("key"),
                    note_ref: None,
                    decided_at: at(2),
                    expires_at: at(9),
                    signature: approval.signature,
                }))
            ),
            Err(DomainError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn timeout_records_explicit_default_action_and_terminality() {
        let opened =
            GovernanceCase::transition(None, &GovernanceCaseCommand::Open(Box::new(open())))
                .expect("open");
        let expired = GovernanceCase::transition(
            Some(&opened.aggregate),
            &GovernanceCaseCommand::Expire {
                expected_version: opened.aggregate.version,
                now: at(10),
            },
        )
        .expect("expire");
        assert_eq!(expired.aggregate.state, GovernanceCaseState::Expired);
        assert!(matches!(
            expired.events[0],
            GovernanceCaseEvent::Expired {
                default_action: TimeoutBehavior::Deny,
                ..
            }
        ));
        assert_eq!(
            GovernanceCase::replay(&[opened.events[0].clone(), expired.events[0].clone()])
                .expect("replay"),
            expired.aggregate
        );
    }

    #[test]
    fn quorum_is_recomputed_from_validated_decisions_and_cannot_be_forged() {
        let mut input = open();
        input.required_quorum.required = 2;
        input.action_digest = input.compute_action_digest().expect("digest");
        let opened =
            GovernanceCase::transition(None, &GovernanceCaseCommand::Open(Box::new(input)))
                .expect("open");
        let approval = decision(&opened.aggregate, opened.aggregate.version);
        let summary =
            DecisionSummary::accepted(&approval, at(2), Sha256Digest::of_bytes(b"authorization"));
        let after_one = GovernanceCase::apply_event(
            Some(&opened.aggregate),
            &GovernanceCaseEvent::DecisionRecorded { decision: summary },
        )
        .expect("apply one approval");
        assert_eq!(after_one.state, GovernanceCaseState::NeedsDecision);

        let mut unauthorized = summary;
        unauthorized.id = id(11);
        unauthorized.actor_id = after_one.requested_by;
        unauthorized.case_version = after_one.version;
        assert_eq!(
            GovernanceCase::apply_event(
                Some(&after_one),
                &GovernanceCaseEvent::DecisionRecorded {
                    decision: unauthorized,
                },
            ),
            Err(DomainError::EvidenceInvalid)
        );
    }

    #[test]
    fn approved_action_rechecks_expiry_binding_quorum_and_event_shape() {
        let opened =
            GovernanceCase::transition(None, &GovernanceCaseCommand::Open(Box::new(open())))
                .expect("open");
        let approved = record_approval(&opened.aggregate).aggregate;
        assert_eq!(approved.state, GovernanceCaseState::QuorumReached);

        let late = GovernanceCaseCommand::BeginApprovedAction {
            expected_version: approved.version,
            observed_subject_version: approved.subject.expected_version,
            observed_policy_revision_id: approved.policy_revision_id,
            observed_action_digest: approved.action_digest,
            observed_attempt_binding: approved.attempt_binding,
            observed_invocation_claim_generation: approved.invocation_claim_generation,
            execution_claim: Box::new(execution_claim(at(10), at(20))),
            now: approved.expires_at,
        };
        assert_eq!(
            GovernanceCase::decide(Some(&approved), &late),
            Err(DomainError::GovernanceDecisionExpired)
        );
        assert_eq!(
            GovernanceCase::apply_event(
                Some(&approved),
                &GovernanceCaseEvent::ApprovedActionStarted {
                    execution_claim: execution_claim(at(10), at(20)),
                    observed_subject_version: approved.subject.expected_version,
                    observed_policy_revision_id: approved.policy_revision_id,
                    observed_action_digest: approved.action_digest,
                    observed_attempt_binding: approved.attempt_binding,
                    observed_invocation_claim_generation: approved.invocation_claim_generation,
                    started_at: approved.expires_at,
                },
            ),
            Err(DomainError::GovernanceDecisionExpired)
        );

        let stale_action = GovernanceCaseCommand::BeginApprovedAction {
            expected_version: approved.version,
            observed_subject_version: approved.subject.expected_version,
            observed_policy_revision_id: approved.policy_revision_id,
            observed_action_digest: Sha256Digest::of_bytes(b"changed-action"),
            observed_attempt_binding: approved.attempt_binding,
            observed_invocation_claim_generation: approved.invocation_claim_generation,
            execution_claim: Box::new(execution_claim(at(3), at(8))),
            now: at(3),
        };
        assert_eq!(
            GovernanceCase::decide(Some(&approved), &stale_action),
            Err(DomainError::GovernanceActionStale)
        );

        let stale_invocation_claim = GovernanceCaseCommand::BeginApprovedAction {
            expected_version: approved.version,
            observed_subject_version: approved.subject.expected_version,
            observed_policy_revision_id: approved.policy_revision_id,
            observed_action_digest: approved.action_digest,
            observed_attempt_binding: approved.attempt_binding,
            observed_invocation_claim_generation: Some(5),
            execution_claim: Box::new(execution_claim(at(3), at(8))),
            now: at(3),
        };
        assert_eq!(
            GovernanceCase::decide(Some(&approved), &stale_invocation_claim),
            Err(DomainError::GovernanceActionStale)
        );

        let stale_attempt_binding = GovernanceCaseEvent::ApprovedActionStarted {
            execution_claim: execution_claim(at(3), at(8)),
            observed_subject_version: approved.subject.expected_version,
            observed_policy_revision_id: approved.policy_revision_id,
            observed_action_digest: approved.action_digest,
            observed_attempt_binding: Some(AttemptFencingBinding {
                attempt_id: id(91),
                lease_id: id(92),
                author_fencing_token: FencingToken::new(1).expect("fencing token"),
            }),
            observed_invocation_claim_generation: approved.invocation_claim_generation,
            started_at: at(3),
        };
        assert_eq!(
            GovernanceCase::apply_event(Some(&approved), &stale_attempt_binding),
            Err(DomainError::GovernanceActionStale)
        );
    }

    #[test]
    fn execution_claim_rejects_invalid_identity_generation_holder_and_window() {
        let opened =
            GovernanceCase::transition(None, &GovernanceCaseCommand::Open(Box::new(open())))
                .expect("open");
        let approved = record_approval(&opened.aggregate).aggregate;
        let event_for = |execution_claim| GovernanceCaseEvent::ApprovedActionStarted {
            execution_claim,
            observed_subject_version: approved.subject.expected_version,
            observed_policy_revision_id: approved.policy_revision_id,
            observed_action_digest: approved.action_digest,
            observed_attempt_binding: approved.attempt_binding,
            observed_invocation_claim_generation: approved.invocation_claim_generation,
            started_at: at(3),
        };

        let mut invalid_id = execution_claim(at(3), at(8));
        invalid_id.id = GovernanceExecutionClaimId::from_uuid(Uuid::nil());
        assert_eq!(
            GovernanceCase::apply_event(Some(&approved), &event_for(invalid_id)),
            Err(DomainError::GovernanceExecutionClaimStale)
        );

        let mut invalid_generation = execution_claim(at(3), at(8));
        invalid_generation.generation = 0;
        assert_eq!(
            GovernanceCase::apply_event(Some(&approved), &event_for(invalid_generation)),
            Err(DomainError::GovernanceExecutionClaimStale)
        );

        let mut invalid_holder = execution_claim(at(3), at(8));
        invalid_holder.holder_actor_id = ActorId::from_uuid(Uuid::nil());
        assert_eq!(
            GovernanceCase::apply_event(Some(&approved), &event_for(invalid_holder)),
            Err(DomainError::GovernanceExecutionClaimStale)
        );

        let expired = execution_claim(at(1), at(3));
        assert_eq!(
            GovernanceCase::apply_event(Some(&approved), &event_for(expired)),
            Err(DomainError::GovernanceExecutionClaimStale)
        );
    }

    #[test]
    fn governance_commands_return_stable_specific_staleness_errors() {
        let opened =
            GovernanceCase::transition(None, &GovernanceCaseCommand::Open(Box::new(open())))
                .expect("open");
        let approval = decision(&opened.aggregate, opened.aggregate.version);
        assert_eq!(
            GovernanceCase::decide(
                Some(&opened.aggregate),
                &GovernanceCaseCommand::RecordDecision {
                    expected_version: AggregateVersion::new(99),
                    decision: Box::new(approval.clone()),
                    actor_is_independent: true,
                    actor_is_authorized: true,
                    authorization_evidence_digest: Sha256Digest::of_bytes(b"authorization"),
                    now: at(2),
                },
            ),
            Err(DomainError::GovernanceCaseStale)
        );

        let mut stale = approval;
        stale.action_digest = Sha256Digest::of_bytes(b"stale");
        assert_eq!(
            GovernanceCase::decide(
                Some(&opened.aggregate),
                &GovernanceCaseCommand::RecordDecision {
                    expected_version: opened.aggregate.version,
                    decision: Box::new(stale),
                    actor_is_independent: true,
                    actor_is_authorized: true,
                    authorization_evidence_digest: Sha256Digest::of_bytes(b"authorization"),
                    now: at(2),
                },
            ),
            Err(DomainError::GovernanceActionStale)
        );

        let expired = decision(&opened.aggregate, opened.aggregate.version);
        assert_eq!(
            GovernanceCase::decide(
                Some(&opened.aggregate),
                &GovernanceCaseCommand::RecordDecision {
                    expected_version: opened.aggregate.version,
                    decision: Box::new(expired),
                    actor_is_independent: true,
                    actor_is_authorized: true,
                    authorization_evidence_digest: Sha256Digest::of_bytes(b"authorization"),
                    now: at(9),
                },
            ),
            Err(DomainError::GovernanceDecisionExpired)
        );
    }

    #[test]
    fn open_rejects_a_typed_action_bound_to_a_different_subject() {
        let mut mismatched = open();
        mismatched.normalized_action = TypedGovernanceAction::CancelInvocationRun {
            run_id: id(99),
            expected_run_version: AggregateVersion::new(4),
            stop_mode: StopMode::Reconcile,
        };
        mismatched.action_digest = mismatched.compute_action_digest().expect("digest");
        assert_eq!(
            GovernanceCase::decide(None, &GovernanceCaseCommand::Open(Box::new(mismatched)),),
            Err(DomainError::GovernanceActionStale)
        );
    }

    #[test]
    fn action_digest_binds_quorum_and_invocation_claim_generation() {
        let baseline = open();
        let baseline_digest = baseline.compute_action_digest().expect("baseline digest");

        let mut changed_quorum = baseline.clone();
        changed_quorum.required_quorum.required = 2;
        assert_ne!(
            baseline_digest,
            changed_quorum
                .compute_action_digest()
                .expect("quorum digest")
        );

        let mut changed_claim = baseline;
        changed_claim.invocation_claim_generation = Some(5);
        assert_ne!(
            baseline_digest,
            changed_claim.compute_action_digest().expect("claim digest")
        );
    }

    #[test]
    fn receipt_must_match_the_issued_claim_holder_and_evidence_shape() {
        let opened =
            GovernanceCase::transition(None, &GovernanceCaseCommand::Open(Box::new(open())))
                .expect("open");
        let approval = record_approval(&opened.aggregate);
        let approved = approval.aggregate.clone();
        let claim = execution_claim(at(3), at(8));
        let started = GovernanceCase::transition(
            Some(&approved),
            &GovernanceCaseCommand::BeginApprovedAction {
                expected_version: approved.version,
                observed_subject_version: approved.subject.expected_version,
                observed_policy_revision_id: approved.policy_revision_id,
                observed_action_digest: approved.action_digest,
                observed_attempt_binding: approved.attempt_binding,
                observed_invocation_claim_generation: approved.invocation_claim_generation,
                execution_claim: Box::new(claim),
                now: at(3),
            },
        )
        .expect("start");
        let executing = started.aggregate.clone();

        let receipt = ExecutionReceipt {
            id: id(15),
            case_id: executing.id,
            action_digest: executing.action_digest,
            execution_claim_id: claim.id,
            execution_claim_generation: claim.generation,
            executor_actor_id: claim.holder_actor_id,
            status: ExecutionReceiptStatus::Succeeded,
            external_effect_key: Some(ProtocolKey::new("effect-1").expect("key")),
            effect_digest: Sha256Digest::of_bytes(b"effect"),
            evidence_refs: vec![ArtifactRef {
                artifact_id: ProtocolKey::new("receipt-proof").expect("key"),
                uri: "artifact://governance/receipt-proof".into(),
                digest: Sha256Digest::of_bytes(b"proof"),
            }],
            started_at: at(3),
            observed_at: at(4),
        };

        let mut wrong_holder = receipt.clone();
        wrong_holder.executor_actor_id = id(99);
        assert_eq!(
            GovernanceCase::apply_event(
                Some(&executing),
                &GovernanceCaseEvent::ExecutionReceiptRecorded {
                    receipt: Box::new(wrong_holder),
                },
            ),
            Err(DomainError::GovernanceExecutionClaimStale)
        );

        let mut missing_evidence = receipt.clone();
        missing_evidence.evidence_refs.clear();
        assert_eq!(
            GovernanceCase::apply_event(
                Some(&executing),
                &GovernanceCaseEvent::ExecutionReceiptRecorded {
                    receipt: Box::new(missing_evidence),
                },
            ),
            Err(DomainError::EvidenceInvalid)
        );

        let mut zero_effect = receipt.clone();
        zero_effect.effect_digest = Sha256Digest::from_bytes([0; 32]);
        assert_eq!(
            GovernanceCase::apply_event(
                Some(&executing),
                &GovernanceCaseEvent::ExecutionReceiptRecorded {
                    receipt: Box::new(zero_effect),
                },
            ),
            Err(DomainError::EvidenceInvalid)
        );

        let mut empty_uri = receipt.clone();
        empty_uri.evidence_refs[0].uri = " \t".into();
        assert_eq!(
            GovernanceCase::apply_event(
                Some(&executing),
                &GovernanceCaseEvent::ExecutionReceiptRecorded {
                    receipt: Box::new(empty_uri),
                },
            ),
            Err(DomainError::EvidenceInvalid)
        );

        let mut zero_artifact_digest = receipt.clone();
        zero_artifact_digest.evidence_refs[0].digest = Sha256Digest::from_bytes([0; 32]);
        let malformed_event = GovernanceCaseEvent::ExecutionReceiptRecorded {
            receipt: Box::new(zero_artifact_digest),
        };
        assert_eq!(
            GovernanceCase::replay(&[
                opened.events[0].clone(),
                approval.events[0].clone(),
                started.events[0].clone(),
                malformed_event,
            ]),
            Err(DomainError::EvidenceInvalid)
        );

        let malformed_id = serde_json::json!({
            "artifact_id": "not/a/protocol/key",
            "uri": "artifact://governance/proof",
            "digest": Sha256Digest::of_bytes(b"proof"),
        });
        assert!(serde_json::from_value::<ArtifactRef>(malformed_id).is_err());

        let applied = GovernanceCase::apply_event(
            Some(&executing),
            &GovernanceCaseEvent::ExecutionReceiptRecorded {
                receipt: Box::new(receipt),
            },
        )
        .expect("valid receipt");
        assert_eq!(applied.state, GovernanceCaseState::Applied);
    }
}
