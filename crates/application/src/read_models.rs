//! Disposable, project-scoped read models for the Control Room.

use std::collections::{BTreeMap, BTreeSet};

use agentforge_domain::{
    ActorId, AttemptId, BudgetReservationId, CandidateId, CorrelationId, DecisionId, EventId,
    ExecutorId, GitObjectId, GovernanceCaseId, IntegrationId, InvocationIntentId, InvocationRunId,
    NodeId, PackageId, PackageRevisionId, PolicyRevisionId, ProjectId, ProtocolKey, RunClaimId,
    RunClaimToken, RunSignalId, ServerInstant, SessionCapsuleId, Sha256Digest, SubmissionId,
    VerificationRunId,
    state::{
        governance::{
            GovernanceCaseKind as DomainGovernanceCaseKind,
            GovernanceCaseState as DomainGovernanceCaseState,
            GovernanceRisk as DomainGovernanceRisk,
        },
        invocation::{
            InvocationIntentState as DomainInvocationIntentState,
            InvocationRunState as DomainInvocationRunState, RunClaimState as DomainRunClaimState,
        },
        run_signal::RunSignalKind as DomainRunSignalKind,
        work_package::WorkPackageState,
    },
};
use serde::{Deserialize, Serialize};

pub const READ_MODEL_VERSION: u16 = 1;

/// Rebuild watermark shared by every project-scoped projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectionHeader {
    pub projection_version: u16,
    pub last_event_id: Option<EventId>,
    pub last_event_sequence: u64,
    pub source_digest: Sha256Digest,
    pub as_of: Option<ServerInstant>,
    pub rebuilt_at: Option<ServerInstant>,
    pub staleness_ms: u64,
    /// Stable, non-sensitive reason code such as `projection_lag`.
    pub degraded_reason: Option<ProtocolKey>,
}

impl ProjectionHeader {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            projection_version: READ_MODEL_VERSION,
            last_event_id: None,
            last_event_sequence: 0,
            source_digest: Sha256Digest::of_bytes([]),
            as_of: None,
            rebuilt_at: None,
            staleness_ms: 0,
            degraded_reason: None,
        }
    }

    #[must_use]
    pub fn advanced(
        &self,
        event_id: EventId,
        event_sequence: u64,
        occurred_at: ServerInstant,
        payload_digest: Sha256Digest,
    ) -> Self {
        let mut source = Vec::with_capacity(104);
        source.extend_from_slice(self.source_digest.as_bytes());
        source.extend_from_slice(payload_digest.as_bytes());
        source.extend_from_slice(event_id.as_uuid().as_bytes());
        source.extend_from_slice(&event_sequence.to_be_bytes());
        source.extend_from_slice(&occurred_at.0.unix_timestamp_nanos().to_be_bytes());
        Self {
            projection_version: READ_MODEL_VERSION,
            last_event_id: Some(event_id),
            last_event_sequence: event_sequence,
            source_digest: Sha256Digest::of_bytes(source),
            as_of: Some(occurred_at),
            // The reference reducer has no wall clock. Production query
            // adapters may overlay operational lag, while this deterministic
            // value means "rebuilt through this source event".
            rebuilt_at: Some(occurred_at),
            staleness_ms: 0,
            degraded_reason: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Secret,
}

/// Text entering a projection. Confidential and secret values are discarded by
/// the constructor and deserializer before an event can enter the replay ledger.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ClassifiedText {
    display: String,
    classification: DataClassification,
    redacted: bool,
}

impl ClassifiedText {
    #[must_use]
    pub fn new(value: impl Into<String>, classification: DataClassification) -> Self {
        match classification {
            DataClassification::Public | DataClassification::Internal => Self {
                display: value.into(),
                classification,
                redacted: false,
            },
            DataClassification::Confidential | DataClassification::Secret => Self {
                // Drop the original at the boundary. Projection events and
                // retained replay ledgers therefore never contain the secret.
                display: format!("[redacted:{}]", classification.as_str()),
                classification,
                redacted: true,
            },
        }
    }

    #[must_use]
    pub fn public(value: impl Into<String>) -> Self {
        Self::new(value, DataClassification::Public)
    }

    #[must_use]
    pub fn internal(value: impl Into<String>) -> Self {
        Self::new(value, DataClassification::Internal)
    }

    #[must_use]
    pub fn secret(value: impl Into<String>) -> Self {
        Self::new(value, DataClassification::Secret)
    }

    #[must_use]
    pub fn project(&self) -> ProjectedText {
        ProjectedText {
            display: self.display.clone(),
            classification: self.classification,
            redacted: self.redacted,
        }
    }
}

#[derive(Deserialize)]
struct TextWire {
    display: String,
    classification: DataClassification,
}

impl<'de> Deserialize<'de> for ClassifiedText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = TextWire::deserialize(deserializer)?;
        Ok(Self::new(wire.display, wire.classification))
    }
}

impl DataClassification {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Secret => "secret",
        }
    }
}

/// The only text type stored in operator-facing read models.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProjectedText {
    display: String,
    classification: DataClassification,
    redacted: bool,
}

impl ProjectedText {
    #[must_use]
    pub(crate) fn system(value: impl Into<String>) -> Self {
        Self {
            display: value.into(),
            classification: DataClassification::Public,
            redacted: false,
        }
    }

    #[must_use]
    pub fn display(&self) -> &str {
        &self.display
    }

    #[must_use]
    pub const fn classification(&self) -> DataClassification {
        self.classification
    }

    #[must_use]
    pub const fn is_redacted(&self) -> bool {
        self.redacted
    }
}

impl<'de> Deserialize<'de> for ProjectedText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = TextWire::deserialize(deserializer)?;
        Ok(ClassifiedText::new(wire.display, wire.classification).project())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationRunState {
    Reserved,
    Starting,
    Running,
    Reconciling,
    Completed,
    Failed,
    Cancelled,
}

impl InvocationRunState {
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Reserved | Self::Starting | Self::Running | Self::Reconciling
        )
    }

    #[must_use]
    pub const fn needs_attention(self) -> bool {
        matches!(self, Self::Failed)
    }
}

impl From<DomainInvocationRunState> for InvocationRunState {
    fn from(value: DomainInvocationRunState) -> Self {
        match value {
            DomainInvocationRunState::Reserved => Self::Reserved,
            DomainInvocationRunState::Starting => Self::Starting,
            DomainInvocationRunState::Running => Self::Running,
            DomainInvocationRunState::Reconciling => Self::Reconciling,
            DomainInvocationRunState::Completed => Self::Completed,
            DomainInvocationRunState::Failed => Self::Failed,
            DomainInvocationRunState::Cancelled => Self::Cancelled,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunSignalKind {
    AssignmentGranted,
    WakeConditionSatisfied,
    QuestionAnswered,
    PermissionDecided,
    ArtifactAvailable,
    SemanticDeadlineReached,
    BudgetThresholdReached,
    NudgeRequested,
    DiagnoseRequested,
    PolicyChanged,
    PackageRevisionChanged,
    LeaseGenerationChanged,
    CancelRequested,
    SecurityTermination,
    RoutineDue,
    ReconcileRequested,
}

impl From<DomainRunSignalKind> for RunSignalKind {
    fn from(value: DomainRunSignalKind) -> Self {
        match value {
            DomainRunSignalKind::AssignmentGranted => Self::AssignmentGranted,
            DomainRunSignalKind::WakeConditionSatisfied => Self::WakeConditionSatisfied,
            DomainRunSignalKind::QuestionAnswered => Self::QuestionAnswered,
            DomainRunSignalKind::PermissionDecided => Self::PermissionDecided,
            DomainRunSignalKind::ArtifactAvailable => Self::ArtifactAvailable,
            DomainRunSignalKind::SemanticDeadlineReached => Self::SemanticDeadlineReached,
            DomainRunSignalKind::BudgetThresholdReached => Self::BudgetThresholdReached,
            DomainRunSignalKind::NudgeRequested => Self::NudgeRequested,
            DomainRunSignalKind::DiagnoseRequested => Self::DiagnoseRequested,
            DomainRunSignalKind::PolicyChanged => Self::PolicyChanged,
            DomainRunSignalKind::PackageRevisionChanged => Self::PackageRevisionChanged,
            DomainRunSignalKind::LeaseGenerationChanged => Self::LeaseGenerationChanged,
            DomainRunSignalKind::CancelRequested => Self::CancelRequested,
            DomainRunSignalKind::SecurityTermination => Self::SecurityTermination,
            DomainRunSignalKind::RoutineDue => Self::RoutineDue,
            DomainRunSignalKind::ReconcileRequested => Self::ReconcileRequested,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationIntentState {
    Pending,
    Claimed,
    Dispatched,
    Satisfied,
    Cancelled,
    DeadLetter,
}

impl From<DomainInvocationIntentState> for InvocationIntentState {
    fn from(value: DomainInvocationIntentState) -> Self {
        match value {
            DomainInvocationIntentState::Pending => Self::Pending,
            DomainInvocationIntentState::Claimed => Self::Claimed,
            DomainInvocationIntentState::Dispatched => Self::Dispatched,
            DomainInvocationIntentState::Satisfied => Self::Satisfied,
            DomainInvocationIntentState::Cancelled => Self::Cancelled,
            DomainInvocationIntentState::DeadLetter => Self::DeadLetter,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunClaimState {
    Active,
    Completed,
    Expired,
    Revoked,
    Superseded,
}

impl From<DomainRunClaimState> for RunClaimState {
    fn from(value: DomainRunClaimState) -> Self {
        match value {
            DomainRunClaimState::Active => Self::Active,
            DomainRunClaimState::Completed => Self::Completed,
            DomainRunClaimState::Expired => Self::Expired,
            DomainRunClaimState::Revoked => Self::Revoked,
            DomainRunClaimState::Superseded => Self::Superseded,
        }
    }
}

/// Operator-facing run label. Unlike the aggregate state, `Running` is only
/// emitted while an authoritative, unexpired RunClaim is present.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunAuthorityStatus {
    Reserved,
    Starting,
    Running,
    ClaimMissing,
    ClaimExpired,
    Reconciling,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunRow {
    pub run_id: InvocationRunId,
    pub intent_id: InvocationIntentId,
    pub intent_state: InvocationIntentState,
    pub signal_ids: BTreeSet<RunSignalId>,
    pub signal_kinds: BTreeSet<RunSignalKind>,
    pub attempt_id: AttemptId,
    pub package_id: PackageId,
    pub executor_id: ExecutorId,
    pub node_id: NodeId,
    pub state: InvocationRunState,
    pub status: RunAuthorityStatus,
    pub claim_id: Option<RunClaimId>,
    pub claim_generation: Option<RunClaimToken>,
    pub claim_state: Option<RunClaimState>,
    pub claim_expires_at: Option<ServerInstant>,
    pub model: ProjectedText,
    pub adapter: ProjectedText,
    pub summary: ProjectedText,
    pub requested_capabilities: BTreeSet<ProtocolKey>,
    pub session_capsule_id: SessionCapsuleId,
    pub session_capsule_digest: Sha256Digest,
    pub budget_reservation_id: Option<BudgetReservationId>,
    pub failure_code: Option<ProtocolKey>,
    pub created_at: ServerInstant,
    pub updated_at: ServerInstant,
}

impl RunRow {
    /// `RUNNING` is authoritative only with a current active RunClaim. The
    /// normalized input mapper is responsible for checking server time.
    #[must_use]
    pub fn authority_status_at(&self, as_of: ServerInstant) -> RunAuthorityStatus {
        match self.state {
            InvocationRunState::Reserved => RunAuthorityStatus::Reserved,
            InvocationRunState::Starting => RunAuthorityStatus::Starting,
            InvocationRunState::Running
                if self.claim_id.is_none()
                    || self.claim_generation.is_none()
                    || !matches!(self.claim_state, Some(RunClaimState::Active)) =>
            {
                RunAuthorityStatus::ClaimMissing
            }
            InvocationRunState::Running
                if self
                    .claim_expires_at
                    .is_none_or(|claim_expires_at| claim_expires_at <= as_of) =>
            {
                RunAuthorityStatus::ClaimExpired
            }
            InvocationRunState::Running => RunAuthorityStatus::Running,
            InvocationRunState::Reconciling => RunAuthorityStatus::Reconciling,
            InvocationRunState::Completed => RunAuthorityStatus::Completed,
            InvocationRunState::Failed => RunAuthorityStatus::Failed,
            InvocationRunState::Cancelled => RunAuthorityStatus::Cancelled,
        }
    }

    #[must_use]
    pub const fn is_authoritatively_running(&self) -> bool {
        matches!(self.status, RunAuthorityStatus::Running)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub runs: BTreeMap<InvocationRunId, RunRow>,
}

impl RunReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            runs: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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

impl From<DomainGovernanceCaseKind> for GovernanceCaseKind {
    fn from(value: DomainGovernanceCaseKind) -> Self {
        match value {
            DomainGovernanceCaseKind::PermissionRequest => Self::PermissionRequest,
            DomainGovernanceCaseKind::BudgetChange => Self::BudgetChange,
            DomainGovernanceCaseKind::PlanPatchApproval => Self::PlanPatchApproval,
            DomainGovernanceCaseKind::PolicyActivation => Self::PolicyActivation,
            DomainGovernanceCaseKind::RoutingException => Self::RoutingException,
            DomainGovernanceCaseKind::SecurityResponse => Self::SecurityResponse,
            DomainGovernanceCaseKind::ConflictResolution => Self::ConflictResolution,
            DomainGovernanceCaseKind::OutcomeUnknown => Self::OutcomeUnknown,
            DomainGovernanceCaseKind::ManualIntegration => Self::ManualIntegration,
            DomainGovernanceCaseKind::OperationalIntervention => Self::OperationalIntervention,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceRisk {
    Critical,
    High,
    Medium,
    Low,
}

impl From<DomainGovernanceRisk> for GovernanceRisk {
    fn from(value: DomainGovernanceRisk) -> Self {
        match value {
            DomainGovernanceRisk::Critical => Self::Critical,
            DomainGovernanceRisk::High => Self::High,
            DomainGovernanceRisk::Medium => Self::Medium,
            DomainGovernanceRisk::Low => Self::Low,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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

impl From<DomainGovernanceCaseState> for GovernanceCaseState {
    fn from(value: DomainGovernanceCaseState) -> Self {
        match value {
            DomainGovernanceCaseState::NeedsDecision => Self::NeedsDecision,
            DomainGovernanceCaseState::QuorumReached => Self::QuorumReached,
            DomainGovernanceCaseState::Executing => Self::Executing,
            DomainGovernanceCaseState::Reconciling => Self::Reconciling,
            DomainGovernanceCaseState::Applied => Self::Applied,
            DomainGovernanceCaseState::Denied => Self::Denied,
            DomainGovernanceCaseState::ChangesRequested => Self::ChangesRequested,
            DomainGovernanceCaseState::Deferred => Self::Deferred,
            DomainGovernanceCaseState::Expired => Self::Expired,
            DomainGovernanceCaseState::Cancelled => Self::Cancelled,
            DomainGovernanceCaseState::Superseded => Self::Superseded,
        }
    }
}

impl GovernanceCaseState {
    #[must_use]
    pub const fn needs_decision(self) -> bool {
        matches!(self, Self::NeedsDecision | Self::Deferred)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernanceCaseRow {
    pub case_id: GovernanceCaseId,
    pub kind: GovernanceCaseKind,
    pub risk: GovernanceRisk,
    pub state: GovernanceCaseState,
    pub summary: ProjectedText,
    pub why_now: ProjectedText,
    pub source: Option<LineageEntityId>,
    pub target: Option<LineageEntityId>,
    pub target_version: Option<u64>,
    pub action_digest: Option<Sha256Digest>,
    pub decision_id: Option<DecisionId>,
    pub policy_revision_id: Option<PolicyRevisionId>,
    pub effect_preview: Vec<ProjectedText>,
    pub decide_by: Option<ServerInstant>,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernanceInboxReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub cases: BTreeMap<GovernanceCaseId, GovernanceCaseRow>,
}

impl GovernanceInboxReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            cases: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLifecycleStatus {
    Active,
    Paused,
    Quarantined,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentAvailabilityStatus {
    Available,
    Reserved,
    Executing,
    WaitingEvent,
    Degraded,
    Quarantined,
    Offline,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseActivityStatus {
    None,
    Active,
    Expiring,
    Fenced,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeConnectivityStatus {
    Online,
    Degraded,
    Offline,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentRow {
    pub executor_id: ExecutorId,
    pub name: ProjectedText,
    pub lifecycle: AgentLifecycleStatus,
    pub availability: AgentAvailabilityStatus,
    pub lease_activity: LeaseActivityStatus,
    pub node_id: Option<NodeId>,
    pub active_run_id: Option<InvocationRunId>,
    pub model: ProjectedText,
    pub adapter: ProjectedText,
    pub capabilities: BTreeSet<ProtocolKey>,
    pub effective_policy_revision_id: Option<PolicyRevisionId>,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeRow {
    pub node_id: NodeId,
    pub name: ProjectedText,
    pub connectivity: NodeConnectivityStatus,
    pub trust_zone: ProjectedText,
    pub capacity_units: u32,
    pub allocated_units: u32,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FleetReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub agents: BTreeMap<ExecutorId, AgentRow>,
    pub nodes: BTreeMap<NodeId, NodeRow>,
}

impl FleetReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            agents: BTreeMap::new(),
            nodes: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetDimension {
    MoneyMicrounits,
    Tokens,
    ComputeMillis,
    WallTimeMillis,
    Attempts,
    Retries,
    ToolCalls,
    GitWrites,
    VerificationRuns,
    RiskUnits,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum BudgetScope {
    Project,
    Agent(ExecutorId),
    Package(PackageId),
    Run(InvocationRunId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BudgetUsage {
    pub limit: u64,
    pub reserved: u64,
    pub consumed: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BudgetEnvelopeRow {
    pub reservation_id: BudgetReservationId,
    pub scope: BudgetScope,
    pub usage: BTreeMap<BudgetDimension, BudgetUsage>,
    pub paused: bool,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetIncidentStatus {
    Open,
    KeptPaused,
    Raised,
    RaisedAndResumed,
    Resolved,
}

impl BudgetIncidentStatus {
    #[must_use]
    pub const fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BudgetIncidentRow {
    pub reservation_id: BudgetReservationId,
    pub dimension: BudgetDimension,
    pub status: BudgetIncidentStatus,
    pub observed: u64,
    pub limit: u64,
    pub affected_in_flight: u32,
    pub affected_queued: u32,
    pub summary: ProjectedText,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BudgetReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub envelopes: BTreeMap<BudgetReservationId, BudgetEnvelopeRow>,
    pub incidents: BTreeMap<BudgetReservationId, BudgetIncidentRow>,
}

impl BudgetReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            envelopes: BTreeMap::new(),
            incidents: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum LineageEntityId {
    Requirement(ProtocolKey),
    Package(PackageId),
    PackageRevision(PackageRevisionId),
    Attempt(AttemptId),
    InvocationRun(InvocationRunId),
    Candidate(CandidateId),
    VerificationRun(VerificationRunId),
    Submission(SubmissionId),
    Integration(IntegrationId),
    Commit(GitObjectId),
}

impl LineageEntityId {
    /// JSON-object-safe, stable identity used by the node index.
    #[must_use]
    pub fn stable_key(&self) -> String {
        match self {
            Self::Requirement(id) => format!("requirement:{id}"),
            Self::Package(id) => format!("package:{id}"),
            Self::PackageRevision(id) => format!("package_revision:{id}"),
            Self::Attempt(id) => format!("attempt:{id}"),
            Self::InvocationRun(id) => format!("invocation_run:{id}"),
            Self::Candidate(id) => format!("candidate:{id}"),
            Self::VerificationRun(id) => format!("verification_run:{id}"),
            Self::Submission(id) => format!("submission:{id}"),
            Self::Integration(id) => format!("integration:{id}"),
            Self::Commit(id) => format!("commit:{id}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageNodeStatus {
    Pending,
    Active,
    Passed,
    Failed,
    Accepted,
    Integrated,
    Superseded,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LineageNode {
    pub id: LineageEntityId,
    pub label: ProjectedText,
    pub status: LineageNodeStatus,
    pub evidence_digest: Option<Sha256Digest>,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageRelation {
    Revises,
    Attempts,
    Invokes,
    Produces,
    Verifies,
    Submits,
    Accepts,
    Integrates,
    Commits,
    Causes,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct LineageEdge {
    pub from: LineageEntityId,
    pub to: LineageEntityId,
    pub relation: LineageRelation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LineageReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub nodes: BTreeMap<String, LineageNode>,
    pub edges: BTreeSet<LineageEdge>,
}

impl LineageReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            nodes: BTreeMap::new(),
            edges: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackageCard {
    pub package_id: PackageId,
    pub protocol_key: ProtocolKey,
    pub state: WorkPackageState,
    pub priority: u8,
    pub summary: ProjectedText,
    pub active_attempt_id: Option<AttemptId>,
    /// Explicit liveness owner for every nonterminal package.  `None` is shown
    /// as a Control Room incident rather than being interpreted as idle.
    pub action_path: Option<PackageActionPath>,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum PackageNextDriver {
    ActiveRun(InvocationRunId),
    QueuedIntent(InvocationIntentId),
    Governance(GovernanceCaseId),
    Monitor,
    Human,
    Blocker,
    Recovery,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackageActionPath {
    pub driver: PackageNextDriver,
    pub reason: ProjectedText,
    pub due_at: Option<ServerInstant>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlRoomCounters {
    pub active_runs: u64,
    pub needs_decision: u64,
    pub open_budget_incidents: u64,
    pub online_nodes: u64,
    pub available_agents: u64,
    pub packages_without_action_path: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum AttentionSource {
    Package(PackageId),
    Run(InvocationRunId),
    Governance(GovernanceCaseId),
    Budget(BudgetReservationId),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlRoomAttentionItem {
    pub risk: GovernanceRisk,
    pub source: AttentionSource,
    pub title: ProjectedText,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectControlRoomReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub packages: BTreeMap<PackageId, PackageCard>,
    pub package_counts: BTreeMap<WorkPackageState, u64>,
    pub counters: ControlRoomCounters,
    pub attention: Vec<ControlRoomAttentionItem>,
}

impl ProjectControlRoomReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            packages: BTreeMap::new(),
            package_counts: BTreeMap::new(),
            counters: ControlRoomCounters {
                active_runs: 0,
                needs_decision: 0,
                open_budget_incidents: 0,
                online_nodes: 0,
                available_agents: 0,
                packages_without_action_path: 0,
            },
            attention: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyState {
    Ready,
    Waiting,
    Blocked,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkGraphNode {
    pub package_id: PackageId,
    pub revision_id: Option<PackageRevisionId>,
    pub state: WorkPackageState,
    pub dependency_state: DependencyState,
    pub blocker_code: Option<ProtocolKey>,
    pub blocker_summary: Option<ProjectedText>,
    pub attempt_id: Option<AttemptId>,
    pub candidate_id: Option<CandidateId>,
    pub integration_id: Option<IntegrationId>,
    pub criticality: u16,
    pub updated_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkGraphRelation {
    HardDependency,
    ArtifactDependency,
    SoftContext,
    ReviewOf,
    Gate,
    ConflictsWith,
    Mutex,
    IntegrationAfter,
    Supersedes,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WorkGraphEdge {
    pub from_package_id: PackageId,
    pub to_package_id: PackageId,
    pub relation: WorkGraphRelation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkGraphReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub nodes: BTreeMap<PackageId, WorkGraphNode>,
    pub edges: BTreeSet<WorkGraphEdge>,
    pub critical_path: Vec<PackageId>,
}

impl WorkGraphReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            nodes: BTreeMap::new(),
            edges: BTreeSet::new(),
            critical_path: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityResult {
    Pending,
    Succeeded,
    Failed,
    Denied,
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityItem {
    pub event_id: EventId,
    pub actor_id: ActorId,
    pub responsible_actor_id: Option<ActorId>,
    pub typed_action: ProtocolKey,
    pub subject: LineageEntityId,
    pub result: ActivityResult,
    pub summary: ProjectedText,
    pub correlation_id: CorrelationId,
    pub occurred_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityReadModel {
    pub project_id: ProjectId,
    pub header: ProjectionHeader,
    pub items: BTreeMap<EventId, ActivityItem>,
}

impl ActivityReadModel {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            header: ProjectionHeader::empty(),
            items: BTreeMap::new(),
        }
    }
}

/// Complete disposable projection for one project.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectReadModels {
    pub project_control_room: ProjectControlRoomReadModel,
    pub runs: RunReadModel,
    pub governance_inbox: GovernanceInboxReadModel,
    pub fleet: FleetReadModel,
    pub budget: BudgetReadModel,
    pub lineage: LineageReadModel,
    pub work_graph: WorkGraphReadModel,
    pub activity: ActivityReadModel,
}

impl ProjectReadModels {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_control_room: ProjectControlRoomReadModel::new(project_id),
            runs: RunReadModel::new(project_id),
            governance_inbox: GovernanceInboxReadModel::new(project_id),
            fleet: FleetReadModel::new(project_id),
            budget: BudgetReadModel::new(project_id),
            lineage: LineageReadModel::new(project_id),
            work_graph: WorkGraphReadModel::new(project_id),
            activity: ActivityReadModel::new(project_id),
        }
    }

    #[must_use]
    pub const fn project_id(&self) -> ProjectId {
        self.project_control_room.project_id
    }

    #[must_use]
    pub fn is_project_consistent(&self, project_id: ProjectId) -> bool {
        self.project_control_room.project_id == project_id
            && self.runs.project_id == project_id
            && self.governance_inbox.project_id == project_id
            && self.fleet.project_id == project_id
            && self.budget.project_id == project_id
            && self.lineage.project_id == project_id
            && self.work_graph.project_id == project_id
            && self.activity.project_id == project_id
    }

    #[must_use]
    pub fn header(&self) -> &ProjectionHeader {
        &self.project_control_room.header
    }

    /// All eight disposable surfaces advance at the same source watermark in
    /// the reference reducer. A stored checkpoint with split headers is never
    /// accepted as an authoritative snapshot.
    #[must_use]
    pub fn headers_are_consistent(&self) -> bool {
        let header = self.header();
        header == &self.runs.header
            && header == &self.governance_inbox.header
            && header == &self.fleet.header
            && header == &self.budget.header
            && header == &self.lineage.header
            && header == &self.work_graph.header
            && header == &self.activity.header
    }

    /// Structural checks needed before a checkpoint can become a replay
    /// baseline. These checks contain no authorization decisions.
    #[must_use]
    pub fn checkpoint_invariants_hold(&self) -> bool {
        self.headers_are_consistent()
            && self.project_control_room.packages.iter().all(|(id, row)| {
                *id == row.package_id && (row.state.is_terminal() || row.action_path.is_some())
            })
            && self.runs.runs.iter().all(|(id, row)| *id == row.run_id)
            && self
                .governance_inbox
                .cases
                .iter()
                .all(|(id, row)| *id == row.case_id)
            && self
                .fleet
                .agents
                .iter()
                .all(|(id, row)| *id == row.executor_id)
            && self.fleet.nodes.iter().all(|(id, row)| *id == row.node_id)
            && self
                .lineage
                .nodes
                .iter()
                .all(|(key, row)| key == &row.id.stable_key())
            && self
                .work_graph
                .nodes
                .iter()
                .all(|(id, row)| *id == row.package_id)
            && self
                .activity
                .items
                .iter()
                .all(|(id, row)| *id == row.event_id)
    }

    pub fn advance_headers(
        &mut self,
        event_id: EventId,
        event_sequence: u64,
        occurred_at: ServerInstant,
        payload_digest: Sha256Digest,
    ) {
        let header = self.project_control_room.header.advanced(
            event_id,
            event_sequence,
            occurred_at,
            payload_digest,
        );
        self.project_control_room.header = header.clone();
        self.runs.header = header.clone();
        self.governance_inbox.header = header.clone();
        self.fleet.header = header.clone();
        self.budget.header = header.clone();
        self.lineage.header = header.clone();
        self.work_graph.header = header.clone();
        self.activity.header = header;
    }
}
