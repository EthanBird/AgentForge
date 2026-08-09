//! Immutable policy revisions with explicit staging, activation CAS, and supersession.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        ActorId, AggregateVersion, GovernanceCaseId, PolicyId, PolicyRevisionId, ProjectId,
        ServerInstant, Sha256Digest,
    },
    state::Transition,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyCategory {
    Routing,
    Budget,
    Governance,
    Security,
    Retention,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PolicyScope {
    pub project_id: ProjectId,
    pub category: PolicyCategory,
}

/// The transactional serialization point for one policy scope.
///
/// Application code must compare-and-swap this head in the same unit of work
/// that appends `PolicyRevisionEvent::Activated` or `Superseded`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PolicyScopeHead {
    pub scope: PolicyScope,
    pub current_revision_id: Option<PolicyRevisionId>,
    pub version: AggregateVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolicyScopeCas {
    pub expected: PolicyScopeHead,
    pub observed: PolicyScopeHead,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyRevisionState {
    Draft,
    Staged,
    Active,
    Superseded,
}

impl PolicyRevisionState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Staged => "staged",
            Self::Active => "active",
            Self::Superseded => "superseded",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Superseded)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreatePolicyRevision {
    pub id: PolicyRevisionId,
    pub policy_id: PolicyId,
    pub scope: PolicyScope,
    pub base_revision_id: Option<PolicyRevisionId>,
    pub canonical_document_digest: Sha256Digest,
    pub schema_version: String,
    pub created_by: ActorId,
    pub reason_code: String,
    pub created_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PolicyRevisionCommandKind {
    Create,
    Stage,
    Activate,
    Supersede,
}

impl PolicyRevisionCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create_policy_revision",
            Self::Stage => "stage_policy_revision",
            Self::Activate => "activate_policy_revision",
            Self::Supersede => "supersede_policy_revision",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyRevisionCommand {
    Create(CreatePolicyRevision),
    Stage {
        expected_version: AggregateVersion,
        validation_digest: Sha256Digest,
        simulation_snapshot_digest: Sha256Digest,
        impact_report_digest: Sha256Digest,
        staged_at: ServerInstant,
    },
    Activate {
        expected_version: AggregateVersion,
        scope_cas: PolicyScopeCas,
        governance_case_id: GovernanceCaseId,
        action_digest: Sha256Digest,
        activated_at: ServerInstant,
    },
    Supersede {
        expected_version: AggregateVersion,
        scope_cas: PolicyScopeCas,
        superseding_revision_id: PolicyRevisionId,
        reason_code: String,
        superseded_at: ServerInstant,
    },
}

impl PolicyRevisionCommand {
    #[must_use]
    pub const fn kind(&self) -> PolicyRevisionCommandKind {
        match self {
            Self::Create(_) => PolicyRevisionCommandKind::Create,
            Self::Stage { .. } => PolicyRevisionCommandKind::Stage,
            Self::Activate { .. } => PolicyRevisionCommandKind::Activate,
            Self::Supersede { .. } => PolicyRevisionCommandKind::Supersede,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Create(_) => None,
            Self::Stage {
                expected_version, ..
            }
            | Self::Activate {
                expected_version, ..
            }
            | Self::Supersede {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PolicyRevisionEvent {
    Created(CreatePolicyRevision),
    Staged {
        validation_digest: Sha256Digest,
        simulation_snapshot_digest: Sha256Digest,
        impact_report_digest: Sha256Digest,
        staged_at: ServerInstant,
    },
    Activated {
        scope_head_before: PolicyScopeHead,
        governance_case_id: GovernanceCaseId,
        action_digest: Sha256Digest,
        activated_at: ServerInstant,
    },
    Superseded {
        scope_head_before: PolicyScopeHead,
        superseding_revision_id: PolicyRevisionId,
        reason_code: String,
        superseded_at: ServerInstant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyRevision {
    pub id: PolicyRevisionId,
    pub policy_id: PolicyId,
    pub scope: PolicyScope,
    pub base_revision_id: Option<PolicyRevisionId>,
    pub canonical_document_digest: Sha256Digest,
    pub schema_version: String,
    pub created_by: ActorId,
    pub reason_code: String,
    pub state: PolicyRevisionState,
    pub validation_digest: Option<Sha256Digest>,
    pub simulation_snapshot_digest: Option<Sha256Digest>,
    pub impact_report_digest: Option<Sha256Digest>,
    pub activation_case_id: Option<GovernanceCaseId>,
    pub activation_action_digest: Option<Sha256Digest>,
    pub activated_against_revision_id: Option<PolicyRevisionId>,
    pub activated_against_scope_version: Option<AggregateVersion>,
    pub superseding_revision_id: Option<PolicyRevisionId>,
    pub superseded_against_scope_version: Option<AggregateVersion>,
    pub superseded_reason_code: Option<String>,
    pub created_at: ServerInstant,
    pub staged_at: Option<ServerInstant>,
    pub activated_at: Option<ServerInstant>,
    pub superseded_at: Option<ServerInstant>,
    pub version: AggregateVersion,
}

impl PolicyRevision {
    pub fn transition(
        current: Option<&Self>,
        command: &PolicyRevisionCommand,
    ) -> Result<Transition<Self, PolicyRevisionEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &PolicyRevisionCommand,
    ) -> Result<PolicyRevisionEvent, DomainError> {
        match (current, command) {
            (None, PolicyRevisionCommand::Create(create)) => {
                validate_create(create)?;
                Ok(PolicyRevisionEvent::Created(create.clone()))
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "policy_revision",
            }),
            (Some(revision), PolicyRevisionCommand::Create(_)) => {
                Err(invalid_transition(revision.state, command.kind()))
            }
            (Some(revision), command) => {
                if revision.state.is_terminal() {
                    return Err(invalid_transition(revision.state, command.kind()));
                }
                if command.expected_version() != Some(revision.version) {
                    return Err(DomainError::StaleVersion);
                }
                match command {
                    PolicyRevisionCommand::Stage {
                        validation_digest,
                        simulation_snapshot_digest,
                        impact_report_digest,
                        staged_at,
                        ..
                    } if revision.state == PolicyRevisionState::Draft
                        && *staged_at >= revision.created_at =>
                    {
                        Ok(PolicyRevisionEvent::Staged {
                            validation_digest: *validation_digest,
                            simulation_snapshot_digest: *simulation_snapshot_digest,
                            impact_report_digest: *impact_report_digest,
                            staged_at: *staged_at,
                        })
                    }
                    PolicyRevisionCommand::Activate {
                        scope_cas,
                        governance_case_id,
                        action_digest,
                        activated_at,
                        ..
                    } if revision.state == PolicyRevisionState::Staged => {
                        validate_scope_cas(revision, *scope_cas, revision.base_revision_id)?;
                        let staged_at =
                            revision.staged_at.ok_or(DomainError::InvariantViolation {
                                invariant: "staged_policy_requires_staged_at",
                            })?;
                        if *activated_at < staged_at {
                            return Err(invalid_time("activated_at"));
                        }
                        Ok(PolicyRevisionEvent::Activated {
                            scope_head_before: scope_cas.expected,
                            governance_case_id: *governance_case_id,
                            action_digest: *action_digest,
                            activated_at: *activated_at,
                        })
                    }
                    PolicyRevisionCommand::Supersede {
                        scope_cas,
                        superseding_revision_id,
                        reason_code,
                        superseded_at,
                        ..
                    } if revision.state == PolicyRevisionState::Active => {
                        validate_scope_cas(revision, *scope_cas, Some(revision.id))?;
                        if *superseding_revision_id == revision.id {
                            return Err(DomainError::RevisionConflict);
                        }
                        if reason_code.trim().is_empty() {
                            return Err(invalid_text("reason_code"));
                        }
                        if revision
                            .activated_at
                            .is_none_or(|activated_at| *superseded_at < activated_at)
                        {
                            return Err(invalid_time("superseded_at"));
                        }
                        Ok(PolicyRevisionEvent::Superseded {
                            scope_head_before: scope_cas.expected,
                            superseding_revision_id: *superseding_revision_id,
                            reason_code: reason_code.clone(),
                            superseded_at: *superseded_at,
                        })
                    }
                    PolicyRevisionCommand::Stage { staged_at, .. }
                        if *staged_at < revision.created_at =>
                    {
                        Err(invalid_time("staged_at"))
                    }
                    _ => Err(invalid_transition(revision.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &PolicyRevisionEvent,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, PolicyRevisionEvent::Created(create)) => {
                validate_create(create)?;
                Ok(Self {
                    id: create.id,
                    policy_id: create.policy_id,
                    scope: create.scope,
                    base_revision_id: create.base_revision_id,
                    canonical_document_digest: create.canonical_document_digest,
                    schema_version: create.schema_version.clone(),
                    created_by: create.created_by,
                    reason_code: create.reason_code.clone(),
                    state: PolicyRevisionState::Draft,
                    validation_digest: None,
                    simulation_snapshot_digest: None,
                    impact_report_digest: None,
                    activation_case_id: None,
                    activation_action_digest: None,
                    activated_against_revision_id: None,
                    activated_against_scope_version: None,
                    superseding_revision_id: None,
                    superseded_against_scope_version: None,
                    superseded_reason_code: None,
                    created_at: create.created_at,
                    staged_at: None,
                    activated_at: None,
                    superseded_at: None,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "policy_revision",
            }),
            (Some(revision), _) if revision.state.is_terminal() => {
                Err(invalid_event(revision.state, event))
            }
            (
                Some(revision),
                PolicyRevisionEvent::Staged {
                    validation_digest,
                    simulation_snapshot_digest,
                    impact_report_digest,
                    staged_at,
                },
            ) if revision.state == PolicyRevisionState::Draft
                && *staged_at >= revision.created_at =>
            {
                let mut next = revision.next_version()?;
                next.state = PolicyRevisionState::Staged;
                next.validation_digest = Some(*validation_digest);
                next.simulation_snapshot_digest = Some(*simulation_snapshot_digest);
                next.impact_report_digest = Some(*impact_report_digest);
                next.staged_at = Some(*staged_at);
                Ok(next)
            }
            (
                Some(revision),
                PolicyRevisionEvent::Activated {
                    scope_head_before,
                    governance_case_id,
                    action_digest,
                    activated_at,
                },
            ) if revision.state == PolicyRevisionState::Staged
                && scope_head_is_valid(*scope_head_before)
                && scope_head_before.scope == revision.scope
                && scope_head_before.current_revision_id == revision.base_revision_id
                && revision
                    .staged_at
                    .is_some_and(|staged_at| *activated_at >= staged_at) =>
            {
                let mut next = revision.next_version()?;
                next.state = PolicyRevisionState::Active;
                next.activation_case_id = Some(*governance_case_id);
                next.activation_action_digest = Some(*action_digest);
                next.activated_against_revision_id = scope_head_before.current_revision_id;
                next.activated_against_scope_version = Some(scope_head_before.version);
                next.activated_at = Some(*activated_at);
                Ok(next)
            }
            (
                Some(revision),
                PolicyRevisionEvent::Superseded {
                    scope_head_before,
                    superseding_revision_id,
                    reason_code,
                    superseded_at,
                },
            ) if revision.state == PolicyRevisionState::Active
                && scope_head_is_valid(*scope_head_before)
                && scope_head_before.scope == revision.scope
                && scope_head_before.current_revision_id == Some(revision.id)
                && *superseding_revision_id != revision.id
                && !reason_code.trim().is_empty()
                && revision
                    .activated_at
                    .is_some_and(|activated_at| *superseded_at >= activated_at) =>
            {
                let mut next = revision.next_version()?;
                next.state = PolicyRevisionState::Superseded;
                next.superseding_revision_id = Some(*superseding_revision_id);
                next.superseded_against_scope_version = Some(scope_head_before.version);
                next.superseded_reason_code = Some(reason_code.clone());
                next.superseded_at = Some(*superseded_at);
                Ok(next)
            }
            (Some(revision), _) => Err(invalid_event(revision.state, event)),
        }
    }

    pub fn replay(events: &[PolicyRevisionEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "policy_revision",
        })
    }

    fn next_version(&self) -> Result<Self, DomainError> {
        let mut next = self.clone();
        next.version = self.version.checked_next()?;
        Ok(next)
    }
}

fn validate_create(create: &CreatePolicyRevision) -> Result<(), DomainError> {
    if create.base_revision_id == Some(create.id) {
        return Err(DomainError::RevisionConflict);
    }
    if create.schema_version.trim().is_empty() {
        return Err(invalid_text("schema_version"));
    }
    if create.reason_code.trim().is_empty() {
        return Err(invalid_text("reason_code"));
    }
    Ok(())
}

fn validate_scope_cas(
    revision: &PolicyRevision,
    scope_cas: PolicyScopeCas,
    required_current_revision_id: Option<PolicyRevisionId>,
) -> Result<(), DomainError> {
    if scope_cas.expected != scope_cas.observed
        || !scope_head_is_valid(scope_cas.expected)
        || scope_cas.expected.scope != revision.scope
        || scope_cas.expected.current_revision_id != required_current_revision_id
    {
        return Err(DomainError::PolicyRevisionStale);
    }
    Ok(())
}

fn scope_head_is_valid(head: PolicyScopeHead) -> bool {
    match head.current_revision_id {
        Some(_) => head.version != AggregateVersion::ZERO,
        None => head.version == AggregateVersion::ZERO,
    }
}

fn invalid_text(field: &'static str) -> DomainError {
    DomainError::InvalidArgument {
        field: field.into(),
        reason: "must not be blank".into(),
    }
}

fn invalid_time(field: &'static str) -> DomainError {
    DomainError::InvalidArgument {
        field: field.into(),
        reason: "must not precede the prior lifecycle fact".into(),
    }
}

fn invalid_transition(
    state: PolicyRevisionState,
    command: PolicyRevisionCommandKind,
) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_event(state: PolicyRevisionState, event: &PolicyRevisionEvent) -> DomainError {
    let command = match event {
        PolicyRevisionEvent::Created(_) => "policy_revision_created",
        PolicyRevisionEvent::Staged { .. } => "policy_revision_staged",
        PolicyRevisionEvent::Activated { .. } => "policy_revision_activated",
        PolicyRevisionEvent::Superseded { .. } => "policy_revision_superseded",
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
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + Duration::seconds(seconds))
    }

    fn create(base_revision_id: Option<PolicyRevisionId>) -> CreatePolicyRevision {
        CreatePolicyRevision {
            id: id(1),
            policy_id: id(2),
            scope: PolicyScope {
                project_id: id(3),
                category: PolicyCategory::Governance,
            },
            base_revision_id,
            canonical_document_digest: Sha256Digest::of_bytes(b"policy-v2"),
            schema_version: "af-policy/1".into(),
            created_by: id(4),
            reason_code: "tighten_quorum".into(),
            created_at: at(0),
        }
    }

    fn scope_head(
        current_revision_id: Option<PolicyRevisionId>,
        version: AggregateVersion,
    ) -> PolicyScopeHead {
        PolicyScopeHead {
            scope: PolicyScope {
                project_id: id(3),
                category: PolicyCategory::Governance,
            },
            current_revision_id,
            version,
        }
    }

    #[test]
    fn draft_staged_active_superseded_is_replayable_and_content_immutable() {
        let base = Some(id(9));
        let created =
            PolicyRevision::transition(None, &PolicyRevisionCommand::Create(create(base)))
                .expect("create");
        let staged = PolicyRevision::transition(
            Some(&created.aggregate),
            &PolicyRevisionCommand::Stage {
                expected_version: created.aggregate.version,
                validation_digest: Sha256Digest::of_bytes(b"validation"),
                simulation_snapshot_digest: Sha256Digest::of_bytes(b"snapshot"),
                impact_report_digest: Sha256Digest::of_bytes(b"impact"),
                staged_at: at(1),
            },
        )
        .expect("stage");
        let activated = PolicyRevision::transition(
            Some(&staged.aggregate),
            &PolicyRevisionCommand::Activate {
                expected_version: staged.aggregate.version,
                scope_cas: PolicyScopeCas {
                    expected: scope_head(base, AggregateVersion::new(7)),
                    observed: scope_head(base, AggregateVersion::new(7)),
                },
                governance_case_id: id(5),
                action_digest: Sha256Digest::of_bytes(b"activate"),
                activated_at: at(2),
            },
        )
        .expect("activate");
        let superseded = PolicyRevision::transition(
            Some(&activated.aggregate),
            &PolicyRevisionCommand::Supersede {
                expected_version: activated.aggregate.version,
                scope_cas: PolicyScopeCas {
                    expected: scope_head(Some(activated.aggregate.id), AggregateVersion::new(8)),
                    observed: scope_head(Some(activated.aggregate.id), AggregateVersion::new(8)),
                },
                superseding_revision_id: id(6),
                reason_code: "replacement_activated".into(),
                superseded_at: at(3),
            },
        )
        .expect("supersede");
        assert_eq!(superseded.aggregate.state, PolicyRevisionState::Superseded);
        assert_eq!(
            superseded.aggregate.canonical_document_digest,
            created.aggregate.canonical_document_digest
        );
        assert_eq!(
            PolicyRevision::replay(&[
                created.events[0].clone(),
                staged.events[0].clone(),
                activated.events[0].clone(),
                superseded.events[0].clone(),
            ])
            .expect("replay"),
            superseded.aggregate
        );
    }

    #[test]
    fn activation_cas_rejects_a_changed_current_revision() {
        let base = Some(id(9));
        let created =
            PolicyRevision::transition(None, &PolicyRevisionCommand::Create(create(base)))
                .expect("create");
        let staged = PolicyRevision::transition(
            Some(&created.aggregate),
            &PolicyRevisionCommand::Stage {
                expected_version: created.aggregate.version,
                validation_digest: Sha256Digest::of_bytes(b"validation"),
                simulation_snapshot_digest: Sha256Digest::of_bytes(b"snapshot"),
                impact_report_digest: Sha256Digest::of_bytes(b"impact"),
                staged_at: at(1),
            },
        )
        .expect("stage")
        .aggregate;
        assert_eq!(
            PolicyRevision::decide(
                Some(&staged),
                &PolicyRevisionCommand::Activate {
                    expected_version: staged.version,
                    scope_cas: PolicyScopeCas {
                        expected: scope_head(base, AggregateVersion::new(7)),
                        observed: scope_head(Some(id(8)), AggregateVersion::new(8)),
                    },
                    governance_case_id: id(5),
                    action_digest: Sha256Digest::of_bytes(b"activate"),
                    activated_at: at(2),
                }
            ),
            Err(DomainError::PolicyRevisionStale)
        );
    }

    #[test]
    fn activation_cas_rejects_same_revision_id_with_a_new_scope_version() {
        let base = Some(id(9));
        let created =
            PolicyRevision::transition(None, &PolicyRevisionCommand::Create(create(base)))
                .expect("create");
        let staged = PolicyRevision::transition(
            Some(&created.aggregate),
            &PolicyRevisionCommand::Stage {
                expected_version: created.aggregate.version,
                validation_digest: Sha256Digest::of_bytes(b"validation"),
                simulation_snapshot_digest: Sha256Digest::of_bytes(b"snapshot"),
                impact_report_digest: Sha256Digest::of_bytes(b"impact"),
                staged_at: at(1),
            },
        )
        .expect("stage")
        .aggregate;
        assert_eq!(
            PolicyRevision::decide(
                Some(&staged),
                &PolicyRevisionCommand::Activate {
                    expected_version: staged.version,
                    scope_cas: PolicyScopeCas {
                        expected: scope_head(base, AggregateVersion::new(7)),
                        observed: scope_head(base, AggregateVersion::new(8)),
                    },
                    governance_case_id: id(5),
                    action_digest: Sha256Digest::of_bytes(b"activate"),
                    activated_at: at(2),
                },
            ),
            Err(DomainError::PolicyRevisionStale)
        );
    }

    #[test]
    fn superseded_is_terminal_before_version_cas_and_replay_cannot_revive_it() {
        let event = PolicyRevisionEvent::Created(create(None));
        let mut revision = PolicyRevision::apply_event(None, &event).expect("create");
        revision.state = PolicyRevisionState::Superseded;
        revision.superseding_revision_id = Some(id(7));
        revision.superseded_at = Some(at(3));
        assert!(matches!(
            PolicyRevision::decide(
                Some(&revision),
                &PolicyRevisionCommand::Stage {
                    expected_version: AggregateVersion::new(0),
                    validation_digest: Sha256Digest::of_bytes(b"validation"),
                    simulation_snapshot_digest: Sha256Digest::of_bytes(b"snapshot"),
                    impact_report_digest: Sha256Digest::of_bytes(b"impact"),
                    staged_at: at(4),
                }
            ),
            Err(DomainError::InvalidTransition { .. })
        ));
        assert!(PolicyRevision::apply_event(Some(&revision), &event).is_err());
    }
}
