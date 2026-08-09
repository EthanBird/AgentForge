use std::collections::{BTreeMap, BTreeSet};

use agentforge_application::{
    ActivityProjectionInput, ActivityResult, AgentAvailabilityStatus, AgentLifecycleStatus,
    AgentProjectionInput, BudgetDimension, BudgetEnvelopeProjectionInput,
    BudgetIncidentProjectionInput, BudgetIncidentStatus, BudgetScope, BudgetUsage, ClassifiedText,
    DependencyState, GovernanceCaseKind, GovernanceCaseState, GovernanceProjectionInput,
    GovernanceRisk, InMemoryProjectionStore, InvocationIntentState, InvocationRunState,
    LeaseActivityStatus, LineageEdge, LineageEntityId, LineageNodeProjectionInput,
    LineageNodeStatus, LineageRelation, NodeConnectivityStatus, NodeProjectionInput,
    PackageActionPathProjectionInput, PackageNextDriver, PackageProjectionInput, ProjectedText,
    ProjectionEnvelope, ProjectionInput, RunClaimState, RunProjectionInput, RunSignalKind,
    WorkGraphEdge, WorkGraphNodeProjectionInput, WorkGraphRelation,
};
use agentforge_domain::{
    BudgetReservationId, EventId, InvocationRunId, PackageId, ProjectId, ProtocolKey,
    ServerInstant, Sha256Digest, state::work_package::WorkPackageState,
};
use time::{Duration, macros::datetime};
use uuid::Uuid;

fn id<T: From<Uuid>>(byte: u8) -> T {
    T::from(Uuid::from_bytes([byte; 16]))
}

fn event(
    project_id: ProjectId,
    ordinal: u8,
    payload: ProjectionInput,
) -> ProjectionEnvelope<ProjectionInput> {
    let occurred_at =
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + Duration::seconds(ordinal.into()));
    ProjectionEnvelope::new(
        project_id,
        agentforge_application::ProjectionCursor::new(occurred_at, id(ordinal), u64::from(ordinal)),
        payload,
    )
    .expect("valid projection event")
}

fn package_input(
    state: WorkPackageState,
    mut action_path: Option<PackageActionPathProjectionInput>,
) -> ProjectionInput {
    if !state.is_terminal() && action_path.is_none() {
        action_path = Some(PackageActionPathProjectionInput {
            driver: PackageNextDriver::Monitor,
            reason: ClassifiedText::internal("waiting for the next authoritative event"),
            due_at: None,
        });
    }
    ProjectionInput::PackageObserved(PackageProjectionInput {
        package_id: id(10),
        protocol_key: ProtocolKey::new("pkg-control-room").expect("protocol key"),
        state,
        priority: 80,
        summary: ClassifiedText::public("control room package"),
        active_attempt_id: Some(id(11)),
        action_path,
    })
}

fn run_input(state: InvocationRunState, summary: ClassifiedText) -> ProjectionInput {
    ProjectionInput::RunObserved(RunProjectionInput {
        run_id: id(20),
        intent_id: id(24),
        intent_state: InvocationIntentState::Dispatched,
        signal_ids: BTreeSet::from([id(25)]),
        signal_kinds: BTreeSet::from([RunSignalKind::AssignmentGranted]),
        attempt_id: id(11),
        package_id: id(10),
        executor_id: id(21),
        node_id: id(22),
        state,
        claim_id: Some(id(27)),
        claim_generation: Some(agentforge_domain::RunClaimToken::new(1).expect("claim token")),
        claim_state: Some(if state.is_active() {
            RunClaimState::Active
        } else {
            RunClaimState::Completed
        }),
        claim_expires_at: Some(ServerInstant(datetime!(2026-08-10 01:00 UTC))),
        model: ClassifiedText::public("gpt-5.6-sol"),
        adapter: ClassifiedText::public("jcode"),
        summary,
        requested_capabilities: BTreeSet::from([
            ProtocolKey::new("rust.application").expect("capability")
        ]),
        session_capsule_id: id(26),
        session_capsule_digest: Sha256Digest::of_bytes(b"context"),
        budget_reservation_id: Some(id(23)),
        failure_code: state
            .needs_attention()
            .then(|| ProtocolKey::new("AF_RUN_LOST").expect("failure code")),
    })
}

#[test]
fn replay_digest_and_projection_are_independent_of_arrival_order() {
    let project_id = id(1);
    let inputs = vec![
        event(
            project_id,
            1,
            package_input(
                WorkPackageState::Active,
                Some(PackageActionPathProjectionInput {
                    driver: PackageNextDriver::ActiveRun(id(20)),
                    reason: ClassifiedText::public("run is executing"),
                    due_at: None,
                }),
            ),
        ),
        event(
            project_id,
            2,
            run_input(
                InvocationRunState::Running,
                ClassifiedText::public("executing"),
            ),
        ),
        event(
            project_id,
            5,
            run_input(
                InvocationRunState::Failed,
                ClassifiedText::public("run failed"),
            ),
        ),
    ];

    let mut ordered = InMemoryProjectionStore::new();
    ordered
        .ingest_batch(inputs.clone())
        .expect("ordered replay");

    let mut unordered = InMemoryProjectionStore::new();
    for input in inputs.into_iter().rev() {
        unordered.ingest(input).expect("late input causes rebuild");
    }

    assert_eq!(
        ordered.projection(project_id),
        unordered.projection(project_id)
    );
    assert_eq!(
        ordered.replay_digest(project_id).expect("digest"),
        unordered.replay_digest(project_id).expect("digest")
    );
    let run = &ordered
        .projection(project_id)
        .expect("projection")
        .runs
        .runs[&id(20)];
    assert_eq!(run.state, InvocationRunState::Failed);
    assert!(run.created_at < run.updated_at);
}

#[test]
fn duplicate_is_idempotent_but_reused_event_id_and_tampering_are_rejected() {
    let project_id = id(1);
    let original = event(
        project_id,
        1,
        package_input(
            WorkPackageState::Offered,
            Some(PackageActionPathProjectionInput {
                driver: PackageNextDriver::QueuedIntent(id(31)),
                reason: ClassifiedText::public("intent is queued"),
                due_at: None,
            }),
        ),
    );
    let mut store = InMemoryProjectionStore::new();
    let receipt = store
        .ingest_batch([original.clone(), original.clone()])
        .expect("duplicate is harmless");
    assert_eq!(receipt.accepted, 1);
    assert_eq!(receipt.duplicates, 1);

    let conflicting = ProjectionEnvelope::new(
        project_id,
        original.cursor,
        package_input(WorkPackageState::Cancelled, None),
    )
    .expect("valid conflicting envelope");
    let error = store.ingest(conflicting).expect_err("event id reuse");
    assert!(matches!(
        error,
        agentforge_application::ApplicationError::ProjectionEventConflict { .. }
    ));

    let mut tampered = original;
    tampered.payload = package_input(WorkPackageState::Failed, None);
    let error = InMemoryProjectionStore::new()
        .ingest(tampered)
        .expect_err("digest mismatch");
    assert!(matches!(
        error,
        agentforge_application::ApplicationError::ProjectionDigestInvalid { .. }
    ));
}

#[test]
fn checkpoint_resume_has_no_gap_and_matches_full_replay() {
    let project_id = id(1);
    let inputs = vec![
        event(
            project_id,
            1,
            package_input(
                WorkPackageState::Active,
                Some(PackageActionPathProjectionInput {
                    driver: PackageNextDriver::ActiveRun(id(20)),
                    reason: ClassifiedText::public("running"),
                    due_at: None,
                }),
            ),
        ),
        event(
            project_id,
            2,
            run_input(
                InvocationRunState::Running,
                ClassifiedText::public("started"),
            ),
        ),
        event(
            project_id,
            3,
            run_input(
                InvocationRunState::Completed,
                ClassifiedText::public("completed"),
            ),
        ),
    ];

    let mut full = InMemoryProjectionStore::new();
    full.ingest_batch(inputs.clone()).expect("full replay");

    let mut prefix = InMemoryProjectionStore::new();
    prefix
        .ingest_batch(inputs[..2].iter().cloned())
        .expect("prefix");
    let checkpoint = prefix.checkpoint(project_id).expect("checkpoint");
    let tail = full.events_after(project_id, checkpoint.cursor);
    assert_eq!(tail.len(), 1);

    let mut resumed =
        InMemoryProjectionStore::from_checkpoint(checkpoint.clone()).expect("restore checkpoint");
    resumed.ingest_batch(tail).expect("resume tail");
    assert_eq!(
        full.replay_digest(project_id).expect("full digest"),
        resumed.replay_digest(project_id).expect("resumed digest")
    );
    assert_eq!(full.projection(project_id), resumed.projection(project_id));

    let repeated_checkpoint_event = inputs[1].clone();
    let receipt = resumed
        .ingest(repeated_checkpoint_event)
        .expect("at-least-once resume repeats checkpoint event");
    assert_eq!(receipt.duplicates, 1);
}

#[test]
fn projects_are_isolated_and_sensitive_text_never_enters_read_models() {
    let project_a = id(1);
    let project_b = id(2);
    let secret = "sk-live-do-not-project";
    let sanitized_run = run_input(InvocationRunState::Running, ClassifiedText::secret(secret));
    assert!(
        !serde_json::to_string(&sanitized_run)
            .expect("serialize normalized input")
            .contains(secret),
        "secret must be discarded before the projection event ledger"
    );
    let mut store = InMemoryProjectionStore::new();
    store
        .ingest_batch([
            event(project_a, 1, sanitized_run),
            event(
                project_b,
                2,
                ProjectionInput::PackageObserved(PackageProjectionInput {
                    package_id: id(40),
                    protocol_key: ProtocolKey::new("project-b-only").expect("key"),
                    state: WorkPackageState::Offered,
                    priority: 50,
                    summary: ClassifiedText::public("B package"),
                    active_attempt_id: None,
                    action_path: Some(PackageActionPathProjectionInput {
                        driver: PackageNextDriver::Human,
                        reason: ClassifiedText::secret(secret),
                        due_at: None,
                    }),
                }),
            ),
        ])
        .expect("multi-project replay");

    let serialized_a = serde_json::to_string(store.projection(project_a).expect("project A"))
        .expect("serialize A");
    let serialized_b = serde_json::to_string(store.projection(project_b).expect("project B"))
        .expect("serialize B");
    assert!(!serialized_a.contains(secret));
    assert!(!serialized_b.contains(secret));
    assert!(serialized_a.contains("[redacted:secret]"));
    assert!(!serialized_a.contains("project-b-only"));
    assert!(!serialized_b.contains(&id::<InvocationRunId>(20).to_string()));
    assert_eq!(
        store.projection(project_a).expect("A").project_id(),
        project_a
    );
    assert_eq!(
        store.projection(project_b).expect("B").project_id(),
        project_b
    );
}

#[test]
fn deserialization_cannot_smuggle_secret_text_into_events_or_checkpoints() {
    let secret = "bearer-this-must-disappear";
    let classified: ClassifiedText = serde_json::from_value(serde_json::json!({
        "display": secret,
        "classification": "secret",
        "redacted": false
    }))
    .expect("classified text wire value");
    let projected: ProjectedText = serde_json::from_value(serde_json::json!({
        "display": secret,
        "classification": "confidential",
        "redacted": false
    }))
    .expect("projected text wire value");

    let classified_json = serde_json::to_string(&classified).expect("classified JSON");
    let projected_json = serde_json::to_string(&projected).expect("projected JSON");
    assert!(!classified_json.contains(secret));
    assert!(!projected_json.contains(secret));
    assert!(classified_json.contains("[redacted:secret]"));
    assert!(projected_json.contains("[redacted:confidential]"));
}

#[test]
fn control_room_exposes_next_driver_and_actionable_cross_surface_counts() {
    let project_id = id(1);
    let reservation_id: BudgetReservationId = id(60);
    let governance_id = id(61);
    let package_id: PackageId = id(10);
    let mut store = InMemoryProjectionStore::new();
    store
        .ingest_batch([
            event(
                project_id,
                1,
                package_input(WorkPackageState::Offered, None),
            ),
            event(
                project_id,
                2,
                ProjectionInput::AgentObserved(AgentProjectionInput {
                    executor_id: id(21),
                    name: ClassifiedText::public("worker"),
                    lifecycle: AgentLifecycleStatus::Active,
                    availability: AgentAvailabilityStatus::Available,
                    lease_activity: LeaseActivityStatus::None,
                    node_id: Some(id(22)),
                    active_run_id: None,
                    model: ClassifiedText::public("model"),
                    adapter: ClassifiedText::public("jcode"),
                    capabilities: BTreeSet::from([ProtocolKey::new("rust").expect("capability")]),
                    effective_policy_revision_id: None,
                }),
            ),
            event(
                project_id,
                3,
                ProjectionInput::NodeObserved(NodeProjectionInput {
                    node_id: id(22),
                    name: ClassifiedText::public("node"),
                    connectivity: NodeConnectivityStatus::Online,
                    trust_zone: ClassifiedText::public("lan"),
                    capacity_units: 4,
                    allocated_units: 1,
                }),
            ),
            event(
                project_id,
                4,
                ProjectionInput::GovernanceCaseObserved(GovernanceProjectionInput {
                    case_id: governance_id,
                    kind: GovernanceCaseKind::PlanPatchApproval,
                    risk: GovernanceRisk::High,
                    state: GovernanceCaseState::NeedsDecision,
                    summary: ClassifiedText::public("approve architecture"),
                    why_now: ClassifiedText::public("blocks implementation"),
                    source: Some(LineageEntityId::Package(package_id)),
                    target: Some(LineageEntityId::Package(package_id)),
                    target_version: Some(3),
                    action_digest: Some(Sha256Digest::of_bytes(b"action")),
                    decision_id: None,
                    policy_revision_id: None,
                    effect_preview: vec![ClassifiedText::public("publish revision")],
                    decide_by: None,
                }),
            ),
            event(
                project_id,
                5,
                ProjectionInput::BudgetEnvelopeObserved(BudgetEnvelopeProjectionInput {
                    reservation_id,
                    scope: BudgetScope::Package(package_id),
                    usage: BTreeMap::from([(
                        BudgetDimension::Tokens,
                        BudgetUsage {
                            limit: 100,
                            reserved: 20,
                            consumed: 90,
                        },
                    )]),
                    paused: true,
                }),
            ),
            event(
                project_id,
                6,
                ProjectionInput::BudgetIncidentObserved(BudgetIncidentProjectionInput {
                    reservation_id,
                    dimension: BudgetDimension::Tokens,
                    status: BudgetIncidentStatus::Open,
                    observed: 110,
                    limit: 100,
                    affected_in_flight: 1,
                    affected_queued: 2,
                    summary: ClassifiedText::public("token budget exceeded"),
                }),
            ),
            event(
                project_id,
                7,
                ProjectionInput::LineageNodeObserved(LineageNodeProjectionInput {
                    id: LineageEntityId::Package(package_id),
                    label: ClassifiedText::public("package"),
                    status: LineageNodeStatus::Active,
                    evidence_digest: None,
                }),
            ),
            event(
                project_id,
                8,
                ProjectionInput::LineageEdgeObserved(LineageEdge {
                    from: LineageEntityId::Requirement(
                        ProtocolKey::new("req-1").expect("requirement key"),
                    ),
                    to: LineageEntityId::Package(package_id),
                    relation: LineageRelation::Causes,
                }),
            ),
            event(
                project_id,
                9,
                ProjectionInput::WorkGraphNodeObserved(WorkGraphNodeProjectionInput {
                    package_id,
                    revision_id: Some(id(71)),
                    state: WorkPackageState::Active,
                    dependency_state: DependencyState::Blocked,
                    blocker_code: Some(ProtocolKey::new("waiting-review").expect("blocker")),
                    blocker_summary: Some(ClassifiedText::public("waiting for review")),
                    attempt_id: Some(id(11)),
                    candidate_id: None,
                    integration_id: None,
                    criticality: 100,
                }),
            ),
            event(
                project_id,
                10,
                ProjectionInput::WorkGraphEdgeObserved(WorkGraphEdge {
                    from_package_id: package_id,
                    to_package_id: id(72),
                    relation: WorkGraphRelation::HardDependency,
                }),
            ),
            event(
                project_id,
                11,
                ProjectionInput::CriticalPathObserved(vec![package_id, id(72)]),
            ),
            event(
                project_id,
                12,
                ProjectionInput::ActivityObserved(ActivityProjectionInput {
                    actor_id: id(73),
                    responsible_actor_id: Some(id(74)),
                    typed_action: ProtocolKey::new("governance.requested").expect("action"),
                    subject: LineageEntityId::Package(package_id),
                    result: ActivityResult::Pending,
                    summary: ClassifiedText::public("decision requested"),
                    correlation_id: id(75),
                }),
            ),
        ])
        .expect("all read-model surfaces");

    let view = store.projection(project_id).expect("projection");
    assert_eq!(view.project_control_room.counters.needs_decision, 1);
    assert_eq!(view.project_control_room.counters.open_budget_incidents, 1);
    assert_eq!(view.project_control_room.counters.available_agents, 1);
    assert_eq!(view.project_control_room.counters.online_nodes, 1);
    assert_eq!(
        view.project_control_room
            .counters
            .packages_without_action_path,
        0
    );
    assert_eq!(view.governance_inbox.cases.len(), 1);
    assert_eq!(view.budget.envelopes.len(), 1);
    assert_eq!(view.lineage.nodes.len(), 1);
    assert_eq!(view.lineage.edges.len(), 1);
    assert_eq!(view.work_graph.nodes.len(), 1);
    assert_eq!(view.work_graph.edges.len(), 1);
    assert_eq!(view.work_graph.critical_path.len(), 2);
    assert_eq!(view.activity.items.len(), 1);
    assert_eq!(view.header(), &view.activity.header);
    assert_eq!(view.header().last_event_sequence, 12);

    store
        .ingest(event(
            project_id,
            13,
            package_input(
                WorkPackageState::Active,
                Some(PackageActionPathProjectionInput {
                    driver: PackageNextDriver::Governance(governance_id),
                    reason: ClassifiedText::internal("awaiting exact target decision"),
                    due_at: None,
                }),
            ),
        ))
        .expect("next driver update");
    assert_eq!(
        store
            .projection(project_id)
            .expect("updated")
            .project_control_room
            .counters
            .packages_without_action_path,
        0
    );
    let serialized = serde_json::to_vec(store.projection(project_id).expect("complete view"))
        .expect("all read models, including lineage, serialize");
    assert!(!serialized.is_empty());
    store.replay_digest(project_id).expect("JCS replay digest");
}

#[test]
fn empty_project_projection_has_a_stable_full_model_digest() {
    let project_id = id(1);
    let mut left = InMemoryProjectionStore::new();
    let mut right = InMemoryProjectionStore::new();
    left.initialize_project(project_id);
    right.initialize_project(project_id);

    let left_view = left.projection(project_id).expect("empty view");
    assert!(left_view.runs.runs.is_empty());
    assert!(left_view.work_graph.nodes.is_empty());
    assert!(left_view.activity.items.is_empty());
    assert_eq!(left_view.header().last_event_id, None);
    assert_eq!(left_view.header().last_event_sequence, 0);
    assert_eq!(
        serde_json::to_vec(left_view).expect("empty full projection JSON"),
        serde_json::to_vec(right.projection(project_id).expect("right empty")).expect("right JSON")
    );
    assert_eq!(
        left.replay_digest(project_id).expect("left digest"),
        right.replay_digest(project_id).expect("right digest")
    );
}

#[test]
fn conflicting_batch_is_atomic() {
    let project_id = id(1);
    let original = event(
        project_id,
        1,
        package_input(WorkPackageState::Offered, None),
    );
    let conflict = ProjectionEnvelope::new(
        project_id,
        original.cursor,
        package_input(WorkPackageState::Cancelled, None),
    )
    .expect("conflict envelope");
    let mut store = InMemoryProjectionStore::new();
    assert!(store.ingest_batch([original, conflict]).is_err());
    assert!(store.projection(project_id).is_none());
}

#[test]
fn restored_checkpoint_rejects_older_unknown_history() {
    let project_id = id(1);
    let mut prefix = InMemoryProjectionStore::new();
    prefix
        .ingest(event(
            project_id,
            2,
            package_input(WorkPackageState::Offered, None),
        ))
        .expect("prefix");
    let checkpoint = prefix.checkpoint(project_id).expect("checkpoint");
    let mut resumed = InMemoryProjectionStore::from_checkpoint(checkpoint).expect("restore");
    let older = event(project_id, 1, package_input(WorkPackageState::Draft, None));
    let error = resumed.ingest(older).expect_err("history is unavailable");
    assert!(matches!(
        error,
        agentforge_application::ApplicationError::ProjectionBeforeCheckpoint { .. }
    ));
}

#[test]
fn cursor_contains_the_source_event_identity() {
    let project_id = id(1);
    let input = event(project_id, 77, package_input(WorkPackageState::Draft, None));
    assert_eq!(input.cursor.event_id, id::<EventId>(77));
}

#[test]
fn nonterminal_package_without_next_driver_is_rejected_at_projection_boundary() {
    let project_id = id(1);
    let payload = ProjectionInput::PackageObserved(PackageProjectionInput {
        package_id: id(10),
        protocol_key: ProtocolKey::new("pkg-no-driver").expect("key"),
        state: WorkPackageState::Offered,
        priority: 10,
        summary: ClassifiedText::public("must have an action path"),
        active_attempt_id: None,
        action_path: None,
    });
    let error = InMemoryProjectionStore::new()
        .ingest(event(project_id, 1, payload))
        .expect_err("nonterminal package needs an explicit driver");
    assert!(matches!(
        error,
        agentforge_application::ApplicationError::ProjectionActionPathMissing { .. }
    ));
}

#[test]
fn running_label_requires_current_claim_and_expires_at_projection_as_of() {
    let project_id = id(1);
    let mut invalid = run_input(
        InvocationRunState::Running,
        ClassifiedText::public("invalid authority"),
    );
    let ProjectionInput::RunObserved(run) = &mut invalid else {
        unreachable!("run fixture")
    };
    run.claim_generation = None;
    let error = InMemoryProjectionStore::new()
        .ingest(event(project_id, 1, invalid))
        .expect_err("running without a fenced claim");
    assert!(matches!(
        error,
        agentforge_application::ApplicationError::ProjectionRunAuthorityInvalid { .. }
    ));

    let mut store = InMemoryProjectionStore::new();
    store
        .ingest(event(
            project_id,
            1,
            run_input(
                InvocationRunState::Running,
                ClassifiedText::public("current claim"),
            ),
        ))
        .expect("current claim");
    assert_eq!(
        store
            .projection(project_id)
            .expect("view")
            .project_control_room
            .counters
            .active_runs,
        1
    );

    let later = ServerInstant(datetime!(2026-08-10 02:00 UTC));
    let later_event = ProjectionEnvelope::new(
        project_id,
        agentforge_application::ProjectionCursor::new(later, id(2), 2),
        ProjectionInput::NodeObserved(NodeProjectionInput {
            node_id: id(22),
            name: ClassifiedText::public("node"),
            connectivity: NodeConnectivityStatus::Online,
            trust_zone: ClassifiedText::internal("lan"),
            capacity_units: 4,
            allocated_units: 1,
        }),
    )
    .expect("later source event");
    store.ingest(later_event).expect("advance projection as_of");
    let view = store.projection(project_id).expect("advanced view");
    assert_eq!(view.project_control_room.counters.active_runs, 0);
    assert_eq!(
        view.runs.runs[&id(20)].status,
        agentforge_application::RunAuthorityStatus::ClaimExpired
    );
}

#[test]
fn full_checkpoint_round_trips_through_json_and_jcs() {
    let project_id = id(1);
    let mut store = InMemoryProjectionStore::new();
    store
        .ingest(event(
            project_id,
            1,
            package_input(WorkPackageState::Offered, None),
        ))
        .expect("projection event");
    let checkpoint = store.checkpoint(project_id).expect("checkpoint");
    let bytes = serde_json::to_vec(&checkpoint).expect("serialize complete checkpoint");
    let decoded: agentforge_application::StoredProjection =
        serde_json::from_slice(&bytes).expect("deserialize complete checkpoint");
    assert_eq!(decoded, checkpoint);
    let restored = InMemoryProjectionStore::from_checkpoint(decoded).expect("validated restore");
    assert_eq!(
        restored.replay_digest(project_id).expect("restored JCS"),
        store.replay_digest(project_id).expect("source JCS")
    );
}
