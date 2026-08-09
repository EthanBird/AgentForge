//! Deterministic Control Room projection reducer and reference store.

use std::collections::{BTreeMap, BTreeSet};

use agentforge_domain::{
    ActorId, AttemptId, BudgetReservationId, CorrelationId, DecisionId, EventId, ExecutorId,
    GovernanceCaseId, InvocationIntentId, InvocationRunId, NodeId, PackageId, PolicyRevisionId,
    ProjectId, ProtocolKey, RunClaimId, RunClaimToken, RunSignalId, ServerInstant,
    SessionCapsuleId, Sha256Digest, state::work_package::WorkPackageState,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    ApplicationError, ApplicationResult,
    read_models::{
        ActivityItem, ActivityResult, AgentAvailabilityStatus, AgentLifecycleStatus, AgentRow,
        BudgetDimension, BudgetEnvelopeRow, BudgetIncidentRow, BudgetIncidentStatus, BudgetScope,
        BudgetUsage, ClassifiedText, ControlRoomAttentionItem, ControlRoomCounters,
        DependencyState, FleetReadModel, GovernanceCaseKind, GovernanceCaseRow,
        GovernanceCaseState, GovernanceRisk, InvocationIntentState, InvocationRunState,
        LeaseActivityStatus, LineageEdge, LineageEntityId, LineageNode, LineageNodeStatus,
        NodeConnectivityStatus, NodeRow, PackageActionPath, PackageCard, PackageNextDriver,
        ProjectReadModels, ProjectedText, READ_MODEL_VERSION, RunAuthorityStatus, RunClaimState,
        RunRow, RunSignalKind, WorkGraphEdge, WorkGraphNode,
    },
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProjectionVersion(u16);

impl ProjectionVersion {
    pub const CURRENT: Self = Self(READ_MODEL_VERSION);

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ProjectionCursor {
    pub occurred_at: ServerInstant,
    pub event_id: EventId,
    pub event_sequence: u64,
}

impl Ord for ProjectionCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.event_sequence
            .cmp(&other.event_sequence)
            .then_with(|| self.occurred_at.cmp(&other.occurred_at))
            .then_with(|| self.event_id.cmp(&other.event_id))
    }
}

impl PartialOrd for ProjectionCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ProjectionCursor {
    #[must_use]
    pub const fn new(occurred_at: ServerInstant, event_id: EventId, event_sequence: u64) -> Self {
        Self {
            occurred_at,
            event_id,
            event_sequence,
        }
    }
}

/// Digest-protected unit used for both normalized events and checkpoints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectionEnvelope<T> {
    pub projection_version: ProjectionVersion,
    pub project_id: ProjectId,
    pub cursor: ProjectionCursor,
    pub last_event_sequence: u64,
    pub source_digest: Sha256Digest,
    pub as_of: ServerInstant,
    pub rebuilt_at: ServerInstant,
    pub staleness_ms: u64,
    pub degraded_reason: Option<ProtocolKey>,
    pub payload_digest: Sha256Digest,
    pub payload: T,
}

impl<T> ProjectionEnvelope<T>
where
    T: Serialize,
{
    pub fn new(
        project_id: ProjectId,
        cursor: ProjectionCursor,
        payload: T,
    ) -> ApplicationResult<Self> {
        let payload_digest = canonical_digest(&payload)?;
        Ok(Self {
            projection_version: ProjectionVersion::CURRENT,
            project_id,
            cursor,
            last_event_sequence: cursor.event_sequence,
            source_digest: payload_digest,
            as_of: cursor.occurred_at,
            rebuilt_at: cursor.occurred_at,
            staleness_ms: 0,
            degraded_reason: None,
            payload_digest,
            payload,
        })
    }

    pub fn validate(&self) -> ApplicationResult<()> {
        if self.projection_version != ProjectionVersion::CURRENT {
            return Err(ApplicationError::ProjectionVersionUnsupported {
                found: self.projection_version.get(),
            });
        }
        if canonical_digest(&self.payload)? != self.payload_digest {
            return Err(ApplicationError::ProjectionDigestInvalid {
                event_id: self.cursor.event_id,
            });
        }
        if self.cursor.event_sequence == 0
            || self.last_event_sequence != self.cursor.event_sequence
            || self.as_of < self.cursor.occurred_at
        {
            return Err(ApplicationError::ProjectionDigestInvalid {
                event_id: self.cursor.event_id,
            });
        }
        Ok(())
    }
}

fn canonical_digest<T: Serialize>(value: &T) -> ApplicationResult<Sha256Digest> {
    serde_json_canonicalizer::to_vec(value)
        .map(Sha256Digest::of_bytes)
        .map_err(|_| ApplicationError::Serialization)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackageProjectionInput {
    pub package_id: PackageId,
    pub protocol_key: ProtocolKey,
    pub state: WorkPackageState,
    pub priority: u8,
    pub summary: ClassifiedText,
    pub active_attempt_id: Option<AttemptId>,
    pub action_path: Option<PackageActionPathProjectionInput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackageActionPathProjectionInput {
    pub driver: PackageNextDriver,
    pub reason: ClassifiedText,
    pub due_at: Option<ServerInstant>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunProjectionInput {
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
    pub claim_id: Option<RunClaimId>,
    pub claim_generation: Option<RunClaimToken>,
    pub claim_state: Option<RunClaimState>,
    pub claim_expires_at: Option<ServerInstant>,
    pub model: ClassifiedText,
    pub adapter: ClassifiedText,
    pub summary: ClassifiedText,
    pub requested_capabilities: BTreeSet<ProtocolKey>,
    pub session_capsule_id: SessionCapsuleId,
    pub session_capsule_digest: Sha256Digest,
    pub budget_reservation_id: Option<BudgetReservationId>,
    pub failure_code: Option<ProtocolKey>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernanceProjectionInput {
    pub case_id: GovernanceCaseId,
    pub kind: GovernanceCaseKind,
    pub risk: GovernanceRisk,
    pub state: GovernanceCaseState,
    pub summary: ClassifiedText,
    pub why_now: ClassifiedText,
    pub source: Option<LineageEntityId>,
    pub target: Option<LineageEntityId>,
    pub target_version: Option<u64>,
    pub action_digest: Option<Sha256Digest>,
    pub decision_id: Option<DecisionId>,
    pub policy_revision_id: Option<PolicyRevisionId>,
    pub effect_preview: Vec<ClassifiedText>,
    pub decide_by: Option<ServerInstant>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentProjectionInput {
    pub executor_id: ExecutorId,
    pub name: ClassifiedText,
    pub lifecycle: AgentLifecycleStatus,
    pub availability: AgentAvailabilityStatus,
    pub lease_activity: LeaseActivityStatus,
    pub node_id: Option<NodeId>,
    pub active_run_id: Option<InvocationRunId>,
    pub model: ClassifiedText,
    pub adapter: ClassifiedText,
    pub capabilities: BTreeSet<ProtocolKey>,
    pub effective_policy_revision_id: Option<PolicyRevisionId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeProjectionInput {
    pub node_id: NodeId,
    pub name: ClassifiedText,
    pub connectivity: NodeConnectivityStatus,
    pub trust_zone: ClassifiedText,
    pub capacity_units: u32,
    pub allocated_units: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BudgetEnvelopeProjectionInput {
    pub reservation_id: BudgetReservationId,
    pub scope: BudgetScope,
    pub usage: BTreeMap<BudgetDimension, BudgetUsage>,
    pub paused: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BudgetIncidentProjectionInput {
    pub reservation_id: BudgetReservationId,
    pub dimension: BudgetDimension,
    pub status: BudgetIncidentStatus,
    pub observed: u64,
    pub limit: u64,
    pub affected_in_flight: u32,
    pub affected_queued: u32,
    pub summary: ClassifiedText,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LineageNodeProjectionInput {
    pub id: LineageEntityId,
    pub label: ClassifiedText,
    pub status: LineageNodeStatus,
    pub evidence_digest: Option<Sha256Digest>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkGraphNodeProjectionInput {
    pub package_id: PackageId,
    pub revision_id: Option<agentforge_domain::PackageRevisionId>,
    pub state: WorkPackageState,
    pub dependency_state: DependencyState,
    pub blocker_code: Option<ProtocolKey>,
    pub blocker_summary: Option<ClassifiedText>,
    pub attempt_id: Option<AttemptId>,
    pub candidate_id: Option<agentforge_domain::CandidateId>,
    pub integration_id: Option<agentforge_domain::IntegrationId>,
    pub criticality: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityProjectionInput {
    pub actor_id: ActorId,
    pub responsible_actor_id: Option<ActorId>,
    pub typed_action: ProtocolKey,
    pub subject: LineageEntityId,
    pub result: ActivityResult,
    pub summary: ClassifiedText,
    pub correlation_id: CorrelationId,
}

/// Stable anti-corruption input between domain-event mappers and disposable UI
/// projections.  Upserts make rebuilding safe while retaining the source event
/// cursor and digest in the enclosing envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum ProjectionInput {
    PackageObserved(PackageProjectionInput),
    RunObserved(RunProjectionInput),
    GovernanceCaseObserved(GovernanceProjectionInput),
    AgentObserved(AgentProjectionInput),
    NodeObserved(NodeProjectionInput),
    BudgetEnvelopeObserved(BudgetEnvelopeProjectionInput),
    BudgetIncidentObserved(BudgetIncidentProjectionInput),
    LineageNodeObserved(LineageNodeProjectionInput),
    LineageEdgeObserved(LineageEdge),
    WorkGraphNodeObserved(WorkGraphNodeProjectionInput),
    WorkGraphEdgeObserved(WorkGraphEdge),
    CriticalPathObserved(Vec<PackageId>),
    ActivityObserved(ActivityProjectionInput),
}

pub type ProjectionEvent = ProjectionEnvelope<ProjectionInput>;
pub type StoredProjection = ProjectionEnvelope<ProjectReadModels>;

/// Pure reducer: no clock, ID generation, repository, or aggregate mutation.
#[derive(Clone, Copy, Debug, Default)]
pub struct ControlRoomReducer;

impl ControlRoomReducer {
    pub fn apply(
        read_models: &mut ProjectReadModels,
        event: &ProjectionEvent,
    ) -> ApplicationResult<()> {
        event.validate()?;
        if event.source_digest != event.payload_digest
            || event.as_of != event.cursor.occurred_at
            || event.rebuilt_at != event.cursor.occurred_at
            || event.staleness_ms != 0
            || event.degraded_reason.is_some()
        {
            return Err(ApplicationError::ProjectionDigestInvalid {
                event_id: event.cursor.event_id,
            });
        }
        if read_models.project_id() != event.project_id {
            return Err(ApplicationError::ProjectionProjectMismatch);
        }
        if event.cursor.event_sequence <= read_models.header().last_event_sequence {
            return Err(ApplicationError::ProjectionSequenceConflict {
                project_id: event.project_id,
                event_sequence: event.cursor.event_sequence,
            });
        }
        let occurred_at = event.cursor.occurred_at;
        match &event.payload {
            ProjectionInput::PackageObserved(value) => {
                if !value.state.is_terminal() && value.action_path.is_none() {
                    return Err(ApplicationError::ProjectionActionPathMissing {
                        package_id: value.package_id,
                    });
                }
                read_models.project_control_room.packages.insert(
                    value.package_id,
                    PackageCard {
                        package_id: value.package_id,
                        protocol_key: value.protocol_key.clone(),
                        state: value.state,
                        priority: value.priority,
                        summary: value.summary.project(),
                        active_attempt_id: value.active_attempt_id,
                        action_path: value.action_path.as_ref().map(|path| PackageActionPath {
                            driver: path.driver.clone(),
                            reason: path.reason.project(),
                            due_at: path.due_at,
                        }),
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::RunObserved(value) => {
                let active_claim_is_current =
                    matches!(value.claim_state, Some(RunClaimState::Active))
                        && value.claim_id.is_some()
                        && value.claim_generation.is_some()
                        && value
                            .claim_expires_at
                            .is_some_and(|expires_at| expires_at > occurred_at);
                if matches!(value.state, InvocationRunState::Running) && !active_claim_is_current {
                    return Err(ApplicationError::ProjectionRunAuthorityInvalid {
                        run_id: value.run_id,
                    });
                }
                let created_at = read_models
                    .runs
                    .runs
                    .get(&value.run_id)
                    .map_or(occurred_at, |run| run.created_at);
                read_models.runs.runs.insert(
                    value.run_id,
                    RunRow {
                        run_id: value.run_id,
                        intent_id: value.intent_id,
                        intent_state: value.intent_state,
                        signal_ids: value.signal_ids.clone(),
                        signal_kinds: value.signal_kinds.clone(),
                        attempt_id: value.attempt_id,
                        package_id: value.package_id,
                        executor_id: value.executor_id,
                        node_id: value.node_id,
                        state: value.state,
                        status: RunAuthorityStatus::Reserved,
                        claim_id: value.claim_id,
                        claim_generation: value.claim_generation,
                        claim_state: value.claim_state,
                        claim_expires_at: value.claim_expires_at,
                        model: value.model.project(),
                        adapter: value.adapter.project(),
                        summary: value.summary.project(),
                        requested_capabilities: value.requested_capabilities.clone(),
                        session_capsule_id: value.session_capsule_id,
                        session_capsule_digest: value.session_capsule_digest,
                        budget_reservation_id: value.budget_reservation_id,
                        failure_code: value.failure_code.clone(),
                        created_at,
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::GovernanceCaseObserved(value) => {
                read_models.governance_inbox.cases.insert(
                    value.case_id,
                    GovernanceCaseRow {
                        case_id: value.case_id,
                        kind: value.kind,
                        risk: value.risk,
                        state: value.state,
                        summary: value.summary.project(),
                        why_now: value.why_now.project(),
                        source: value.source.clone(),
                        target: value.target.clone(),
                        target_version: value.target_version,
                        action_digest: value.action_digest,
                        decision_id: value.decision_id,
                        policy_revision_id: value.policy_revision_id,
                        effect_preview: value
                            .effect_preview
                            .iter()
                            .map(ClassifiedText::project)
                            .collect(),
                        decide_by: value.decide_by,
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::AgentObserved(value) => {
                read_models.fleet.agents.insert(
                    value.executor_id,
                    AgentRow {
                        executor_id: value.executor_id,
                        name: value.name.project(),
                        lifecycle: value.lifecycle,
                        availability: value.availability,
                        lease_activity: value.lease_activity,
                        node_id: value.node_id,
                        active_run_id: value.active_run_id,
                        model: value.model.project(),
                        adapter: value.adapter.project(),
                        capabilities: value.capabilities.clone(),
                        effective_policy_revision_id: value.effective_policy_revision_id,
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::NodeObserved(value) => {
                read_models.fleet.nodes.insert(
                    value.node_id,
                    NodeRow {
                        node_id: value.node_id,
                        name: value.name.project(),
                        connectivity: value.connectivity,
                        trust_zone: value.trust_zone.project(),
                        capacity_units: value.capacity_units,
                        allocated_units: value.allocated_units,
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::BudgetEnvelopeObserved(value) => {
                read_models.budget.envelopes.insert(
                    value.reservation_id,
                    BudgetEnvelopeRow {
                        reservation_id: value.reservation_id,
                        scope: value.scope,
                        usage: value.usage.clone(),
                        paused: value.paused,
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::BudgetIncidentObserved(value) => {
                read_models.budget.incidents.insert(
                    value.reservation_id,
                    BudgetIncidentRow {
                        reservation_id: value.reservation_id,
                        dimension: value.dimension,
                        status: value.status,
                        observed: value.observed,
                        limit: value.limit,
                        affected_in_flight: value.affected_in_flight,
                        affected_queued: value.affected_queued,
                        summary: value.summary.project(),
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::LineageNodeObserved(value) => {
                read_models.lineage.nodes.insert(
                    value.id.stable_key(),
                    LineageNode {
                        id: value.id.clone(),
                        label: value.label.project(),
                        status: value.status,
                        evidence_digest: value.evidence_digest,
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::LineageEdgeObserved(value) => {
                read_models.lineage.edges.insert(value.clone());
            }
            ProjectionInput::WorkGraphNodeObserved(value) => {
                read_models.work_graph.nodes.insert(
                    value.package_id,
                    WorkGraphNode {
                        package_id: value.package_id,
                        revision_id: value.revision_id,
                        state: value.state,
                        dependency_state: value.dependency_state,
                        blocker_code: value.blocker_code.clone(),
                        blocker_summary: value
                            .blocker_summary
                            .as_ref()
                            .map(ClassifiedText::project),
                        attempt_id: value.attempt_id,
                        candidate_id: value.candidate_id,
                        integration_id: value.integration_id,
                        criticality: value.criticality,
                        updated_at: occurred_at,
                    },
                );
            }
            ProjectionInput::WorkGraphEdgeObserved(value) => {
                read_models.work_graph.edges.insert(value.clone());
            }
            ProjectionInput::CriticalPathObserved(value) => {
                read_models.work_graph.critical_path.clone_from(value);
            }
            ProjectionInput::ActivityObserved(value) => {
                read_models.activity.items.insert(
                    event.cursor.event_id,
                    ActivityItem {
                        event_id: event.cursor.event_id,
                        actor_id: value.actor_id,
                        responsible_actor_id: value.responsible_actor_id,
                        typed_action: value.typed_action.clone(),
                        subject: value.subject.clone(),
                        result: value.result,
                        summary: value.summary.project(),
                        correlation_id: value.correlation_id,
                        occurred_at,
                    },
                );
            }
        }
        for run in read_models.runs.runs.values_mut() {
            run.status = run.authority_status_at(occurred_at);
        }
        rebuild_control_room(read_models);
        read_models.advance_headers(
            event.cursor.event_id,
            event.last_event_sequence,
            occurred_at,
            event.payload_digest,
        );
        Ok(())
    }
}

fn rebuild_control_room(read_models: &mut ProjectReadModels) {
    let control_room = &mut read_models.project_control_room;
    control_room.package_counts.clear();
    for package in control_room.packages.values() {
        *control_room
            .package_counts
            .entry(package.state)
            .or_default() += 1;
    }

    control_room.counters = ControlRoomCounters {
        active_runs: count(
            read_models
                .runs
                .runs
                .values()
                .filter(|run| run.is_authoritatively_running()),
        ),
        needs_decision: count(
            read_models
                .governance_inbox
                .cases
                .values()
                .filter(|case| case.state.needs_decision()),
        ),
        open_budget_incidents: count(
            read_models
                .budget
                .incidents
                .values()
                .filter(|incident| incident.status.is_open()),
        ),
        online_nodes: count(
            read_models
                .fleet
                .nodes
                .values()
                .filter(|node| node.connectivity == NodeConnectivityStatus::Online),
        ),
        available_agents: count(
            read_models
                .fleet
                .agents
                .values()
                .filter(|agent| agent.availability == AgentAvailabilityStatus::Available),
        ),
        packages_without_action_path: count(
            control_room
                .packages
                .values()
                .filter(|package| !package.state.is_terminal() && package.action_path.is_none()),
        ),
    };

    let mut attention = Vec::new();
    for case in read_models
        .governance_inbox
        .cases
        .values()
        .filter(|case| case.state.needs_decision())
    {
        attention.push(ControlRoomAttentionItem {
            risk: case.risk,
            source: crate::read_models::AttentionSource::Governance(case.case_id),
            title: case.summary.clone(),
            updated_at: case.updated_at,
        });
    }
    for incident in read_models
        .budget
        .incidents
        .values()
        .filter(|incident| incident.status.is_open())
    {
        attention.push(ControlRoomAttentionItem {
            risk: GovernanceRisk::Critical,
            source: crate::read_models::AttentionSource::Budget(incident.reservation_id),
            title: incident.summary.clone(),
            updated_at: incident.updated_at,
        });
    }
    for run in read_models
        .runs
        .runs
        .values()
        .filter(|run| run.state.needs_attention())
    {
        attention.push(ControlRoomAttentionItem {
            risk: GovernanceRisk::High,
            source: crate::read_models::AttentionSource::Run(run.run_id),
            title: run.summary.clone(),
            updated_at: run.updated_at,
        });
    }
    for package in control_room.packages.values().filter(|package| {
        matches!(
            package.state,
            WorkPackageState::Blocked | WorkPackageState::RebaseRequired | WorkPackageState::Failed
        ) || package.action_path.as_ref().is_some_and(|path| {
            matches!(
                path.driver,
                PackageNextDriver::Human | PackageNextDriver::Blocker | PackageNextDriver::Recovery
            )
        }) || (!package.state.is_terminal() && package.action_path.is_none())
    }) {
        let missing_path = !package.state.is_terminal() && package.action_path.is_none();
        attention.push(ControlRoomAttentionItem {
            risk: if package.state == WorkPackageState::Failed || missing_path {
                GovernanceRisk::Critical
            } else {
                GovernanceRisk::High
            },
            source: crate::read_models::AttentionSource::Package(package.package_id),
            title: package.action_path.as_ref().map_or_else(
                || {
                    if missing_path {
                        ProjectedText::system("package has no next driver")
                    } else {
                        package.summary.clone()
                    }
                },
                |path| path.reason.clone(),
            ),
            updated_at: package.updated_at,
        });
    }
    attention.sort_by(|left, right| {
        left.risk
            .cmp(&right.risk)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
            .then_with(|| left.source.cmp(&right.source))
    });
    control_room.attention = attention;
}

fn count<T>(values: impl Iterator<Item = T>) -> u64 {
    u64::try_from(values.count()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ApplyBatchReceipt {
    pub accepted: usize,
    pub duplicates: usize,
    pub last_cursor_by_project: BTreeMap<ProjectId, ProjectionCursor>,
}

/// Reference implementation that retains normalized events and rebuilds each
/// affected project in cursor order.  It is intentionally not a production
/// database; persistence adapters can use it as a differential oracle.
#[derive(Clone, Debug, Default)]
pub struct InMemoryProjectionStore {
    baselines: BTreeMap<ProjectId, StoredProjection>,
    events: BTreeMap<ProjectionCursor, ProjectionEvent>,
    event_cursors: BTreeMap<EventId, ProjectionCursor>,
    sequence_cursors: BTreeMap<(ProjectId, u64), ProjectionCursor>,
    projections: BTreeMap<ProjectId, ProjectReadModels>,
}

impl InMemoryProjectionStore {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            baselines: BTreeMap::new(),
            events: BTreeMap::new(),
            event_cursors: BTreeMap::new(),
            sequence_cursors: BTreeMap::new(),
            projections: BTreeMap::new(),
        }
    }

    pub fn from_checkpoint(checkpoint: StoredProjection) -> ApplicationResult<Self> {
        let mut store = Self::new();
        store.restore(checkpoint)?;
        Ok(store)
    }

    /// Creates an empty, digestible projection before its first source event.
    pub fn initialize_project(&mut self, project_id: ProjectId) {
        self.projections
            .entry(project_id)
            .or_insert_with(|| ProjectReadModels::new(project_id));
    }

    pub fn restore(&mut self, checkpoint: StoredProjection) -> ApplicationResult<()> {
        checkpoint.validate()?;
        if !checkpoint
            .payload
            .is_project_consistent(checkpoint.project_id)
        {
            return Err(ApplicationError::ProjectionProjectMismatch);
        }
        validate_checkpoint_headers(&checkpoint)?;
        let project_id = checkpoint.project_id;
        self.baselines.insert(project_id, checkpoint);
        self.rebuild_project(project_id)?;
        Ok(())
    }

    pub fn ingest(&mut self, event: ProjectionEvent) -> ApplicationResult<ApplyBatchReceipt> {
        self.ingest_batch([event])
    }

    /// Atomically validates, deduplicates, sorts, and applies an input batch.
    /// A late event is inserted into the retained ledger and causes a complete
    /// deterministic rebuild of only its project.
    pub fn ingest_batch(
        &mut self,
        events: impl IntoIterator<Item = ProjectionEvent>,
    ) -> ApplicationResult<ApplyBatchReceipt> {
        let mut candidate = self.clone();
        let mut receipt = ApplyBatchReceipt::default();
        let mut affected = BTreeSet::new();

        for event in events {
            event.validate()?;
            if let Some(existing_cursor) = candidate.event_cursors.get(&event.cursor.event_id) {
                let existing = candidate
                    .events
                    .get(existing_cursor)
                    .expect("event cursor index must point at an event");
                if existing == &event {
                    receipt.duplicates += 1;
                    continue;
                }
                return Err(ApplicationError::ProjectionEventConflict {
                    event_id: event.cursor.event_id,
                });
            }

            if let Some(existing_cursor) = candidate
                .sequence_cursors
                .get(&(event.project_id, event.cursor.event_sequence))
            {
                let existing = candidate
                    .events
                    .get(existing_cursor)
                    .expect("sequence cursor index must point at an event");
                if existing == &event {
                    receipt.duplicates += 1;
                    continue;
                }
                return Err(ApplicationError::ProjectionSequenceConflict {
                    project_id: event.project_id,
                    event_sequence: event.cursor.event_sequence,
                });
            }

            if let Some(checkpoint) = candidate.baselines.get(&event.project_id) {
                if event.cursor.event_sequence == checkpoint.cursor.event_sequence {
                    if event.cursor == checkpoint.cursor {
                        receipt.duplicates += 1;
                        continue;
                    }
                    return Err(ApplicationError::ProjectionSequenceConflict {
                        project_id: event.project_id,
                        event_sequence: event.cursor.event_sequence,
                    });
                }
                if event.cursor < checkpoint.cursor {
                    return Err(ApplicationError::ProjectionBeforeCheckpoint {
                        project_id: event.project_id,
                        cursor: event.cursor,
                    });
                }
            }

            if let Some(existing) = candidate.events.get(&event.cursor) {
                if existing == &event {
                    receipt.duplicates += 1;
                    continue;
                }
                return Err(ApplicationError::ProjectionEventConflict {
                    event_id: event.cursor.event_id,
                });
            }

            affected.insert(event.project_id);
            candidate
                .event_cursors
                .insert(event.cursor.event_id, event.cursor);
            candidate.sequence_cursors.insert(
                (event.project_id, event.cursor.event_sequence),
                event.cursor,
            );
            candidate.events.insert(event.cursor, event);
            receipt.accepted += 1;
        }

        for project_id in affected {
            candidate.rebuild_project(project_id)?;
            if let Some(cursor) = candidate.last_cursor(project_id) {
                receipt.last_cursor_by_project.insert(project_id, cursor);
            }
        }
        *self = candidate;
        Ok(receipt)
    }

    #[must_use]
    pub fn projection(&self, project_id: ProjectId) -> Option<&ProjectReadModels> {
        self.projections.get(&project_id)
    }

    pub fn checkpoint(&self, project_id: ProjectId) -> ApplicationResult<StoredProjection> {
        let payload = self
            .projection(project_id)
            .ok_or(ApplicationError::ProjectionNotFound)?
            .clone();
        let cursor = self
            .last_cursor(project_id)
            .ok_or(ApplicationError::ProjectionNotFound)?;
        let header = payload.header().clone();
        let mut checkpoint = ProjectionEnvelope::new(project_id, cursor, payload)?;
        checkpoint.last_event_sequence = header.last_event_sequence;
        checkpoint.source_digest = header.source_digest;
        checkpoint.as_of = header.as_of.ok_or(ApplicationError::ProjectionNotFound)?;
        checkpoint.rebuilt_at = header
            .rebuilt_at
            .ok_or(ApplicationError::ProjectionNotFound)?;
        checkpoint.staleness_ms = header.staleness_ms;
        checkpoint.degraded_reason = header.degraded_reason;
        validate_checkpoint_headers(&checkpoint)?;
        Ok(checkpoint)
    }

    pub fn replay_digest(&self, project_id: ProjectId) -> ApplicationResult<Sha256Digest> {
        self.projection(project_id)
            .ok_or(ApplicationError::ProjectionNotFound)
            .and_then(canonical_digest)
    }

    #[must_use]
    pub fn last_cursor(&self, project_id: ProjectId) -> Option<ProjectionCursor> {
        let baseline = self.baselines.get(&project_id).map(|value| value.cursor);
        self.events
            .iter()
            .rev()
            .find_map(|(cursor, event)| (event.project_id == project_id).then_some(*cursor))
            .or(baseline)
    }

    /// Reference cursor-resume query.  Transport adapters sign/encode the
    /// cursor; this layer deliberately works with the decoded stable tuple.
    #[must_use]
    pub fn events_after(
        &self,
        project_id: ProjectId,
        cursor: ProjectionCursor,
    ) -> Vec<ProjectionEvent> {
        self.events
            .range((
                std::ops::Bound::Excluded(cursor),
                std::ops::Bound::Unbounded,
            ))
            .filter_map(|(_, event)| (event.project_id == project_id).then_some(event.clone()))
            .collect()
    }

    fn rebuild_project(&mut self, project_id: ProjectId) -> ApplicationResult<()> {
        let baseline = self.baselines.get(&project_id);
        let mut projection = baseline.map_or_else(
            || ProjectReadModels::new(project_id),
            |checkpoint| checkpoint.payload.clone(),
        );
        let after = baseline.map(|checkpoint| checkpoint.cursor);
        for (cursor, event) in &self.events {
            if event.project_id != project_id || after.is_some_and(|value| *cursor <= value) {
                continue;
            }
            ControlRoomReducer::apply(&mut projection, event)?;
        }
        self.projections.insert(project_id, projection);
        Ok(())
    }
}

fn validate_checkpoint_headers(checkpoint: &StoredProjection) -> ApplicationResult<()> {
    let header = checkpoint.payload.header();
    if !checkpoint.payload.checkpoint_invariants_hold()
        || header.projection_version != checkpoint.projection_version.get()
        || header.last_event_id != Some(checkpoint.cursor.event_id)
        || header.last_event_sequence != checkpoint.last_event_sequence
        || header.source_digest != checkpoint.source_digest
        || header.as_of != Some(checkpoint.as_of)
        || header.rebuilt_at != Some(checkpoint.rebuilt_at)
        || header.staleness_ms != checkpoint.staleness_ms
        || header.degraded_reason != checkpoint.degraded_reason
    {
        return Err(ApplicationError::ProjectionHeaderInvalid);
    }
    Ok(())
}

/// Compile-time assertion that checkpoint payloads remain independently
/// serializable and deserializable for adapter implementations.
fn _projection_payload_contract<T>()
where
    T: Serialize + DeserializeOwned,
{
}

#[allow(dead_code)]
fn _read_model_contracts() {
    _projection_payload_contract::<ProjectionInput>();
    _projection_payload_contract::<ProjectReadModels>();
    _projection_payload_contract::<FleetReadModel>();
    let _ = ProjectedText::system("contract");
}
