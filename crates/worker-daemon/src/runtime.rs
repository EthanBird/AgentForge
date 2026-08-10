//! Pure, replayable Worker Attempt state machine.

use agentforge_domain::{
    AggregateVersion, AttemptId, FencingToken, GitObjectId, LeaseId, PackageId, PackageRevision,
    ProtocolKey, ServerInstant, Sha256Digest,
    attempt::{WakeCondition, WakeFact},
};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPhase {
    Granted,
    Preparing,
    Baseline,
    Planning,
    Implementing,
    LocalVerifying,
    WaitingInput,
    SealingCandidate,
    HandingOffCandidate,
    Salvaging,
    AuthorComplete,
    LocalFailed,
    LocalCancelled,
}

impl WorkerPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Preparing => "preparing",
            Self::Baseline => "baseline",
            Self::Planning => "planning",
            Self::Implementing => "implementing",
            Self::LocalVerifying => "local_verifying",
            Self::WaitingInput => "waiting_input",
            Self::SealingCandidate => "sealing_candidate",
            Self::HandingOffCandidate => "handing_off_candidate",
            Self::Salvaging => "salvaging",
            Self::AuthorComplete => "author_complete",
            Self::LocalFailed => "local_failed",
            Self::LocalCancelled => "local_cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::AuthorComplete | Self::LocalFailed | Self::LocalCancelled
        )
    }

    #[must_use]
    pub const fn can_resume_from_wait(self) -> bool {
        matches!(self, Self::Planning | Self::Implementing)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptGrant {
    pub attempt_id: AttemptId,
    pub package_id: PackageId,
    pub package_revision: PackageRevision,
    pub package_hash: Sha256Digest,
    pub base_commit: GitObjectId,
    pub lease_id: LeaseId,
    pub lease_generation: FencingToken,
    pub lease_expires_at: ServerInstant,
    pub granted_at: ServerInstant,
}

impl AttemptGrant {
    fn validate(&self) -> WorkerResult<()> {
        if self.attempt_id.as_uuid().is_nil()
            || self.package_id.as_uuid().is_nil()
            || self.lease_id.as_uuid().is_nil()
            || self.package_hash.as_bytes().iter().all(|byte| *byte == 0)
            || self.lease_expires_at <= self.granted_at
        {
            return Err(WorkerError::InvalidArgument("attempt_grant"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateSnapshot {
    pub commit: GitObjectId,
    pub tree: GitObjectId,
    pub author_evidence_digest: Sha256Digest,
}

impl CandidateSnapshot {
    fn validate(&self) -> WorkerResult<()> {
        if self
            .author_evidence_digest
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(WorkerError::InvalidArgument("author_evidence_digest"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseLossReason {
    HigherGeneration,
    Expired,
    Revoked,
    ServerRejected,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerAttemptState {
    pub(crate) attempt_id: AttemptId,
    pub(crate) package_id: PackageId,
    pub(crate) package_revision: PackageRevision,
    pub(crate) package_hash: Sha256Digest,
    pub(crate) base_commit: GitObjectId,
    pub(crate) lease_id: LeaseId,
    pub(crate) lease_generation: FencingToken,
    pub(crate) lease_expires_at: ServerInstant,
    pub(crate) phase: WorkerPhase,
    pub(crate) resume_phase: Option<WorkerPhase>,
    pub(crate) wake_condition: Option<WakeCondition>,
    pub(crate) plan_digest: Option<Sha256Digest>,
    pub(crate) current_tree: Option<GitObjectId>,
    pub(crate) last_verification_digest: Option<Sha256Digest>,
    pub(crate) candidate: Option<CandidateSnapshot>,
    pub(crate) candidate_id: Option<ProtocolKey>,
    pub(crate) terminal_code: Option<ProtocolKey>,
    pub(crate) turns_completed: u32,
    pub(crate) version: AggregateVersion,
    pub(crate) journal_seq: u64,
    pub(crate) created_at: ServerInstant,
    pub(crate) updated_at: ServerInstant,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerAttemptStateWire {
    attempt_id: AttemptId,
    package_id: PackageId,
    package_revision: PackageRevision,
    package_hash: Sha256Digest,
    base_commit: GitObjectId,
    lease_id: LeaseId,
    lease_generation: FencingToken,
    lease_expires_at: ServerInstant,
    phase: WorkerPhase,
    resume_phase: Option<WorkerPhase>,
    wake_condition: Option<WakeCondition>,
    plan_digest: Option<Sha256Digest>,
    current_tree: Option<GitObjectId>,
    last_verification_digest: Option<Sha256Digest>,
    candidate: Option<CandidateSnapshot>,
    candidate_id: Option<ProtocolKey>,
    terminal_code: Option<ProtocolKey>,
    turns_completed: u32,
    version: AggregateVersion,
    journal_seq: u64,
    created_at: ServerInstant,
    updated_at: ServerInstant,
}

impl TryFrom<WorkerAttemptStateWire> for WorkerAttemptState {
    type Error = WorkerError;

    fn try_from(wire: WorkerAttemptStateWire) -> Result<Self, Self::Error> {
        let state = Self {
            attempt_id: wire.attempt_id,
            package_id: wire.package_id,
            package_revision: wire.package_revision,
            package_hash: wire.package_hash,
            base_commit: wire.base_commit,
            lease_id: wire.lease_id,
            lease_generation: wire.lease_generation,
            lease_expires_at: wire.lease_expires_at,
            phase: wire.phase,
            resume_phase: wire.resume_phase,
            wake_condition: wire.wake_condition,
            plan_digest: wire.plan_digest,
            current_tree: wire.current_tree,
            last_verification_digest: wire.last_verification_digest,
            candidate: wire.candidate,
            candidate_id: wire.candidate_id,
            terminal_code: wire.terminal_code,
            turns_completed: wire.turns_completed,
            version: wire.version,
            journal_seq: wire.journal_seq,
            created_at: wire.created_at,
            updated_at: wire.updated_at,
        };
        state.validate()?;
        Ok(state)
    }
}

impl<'de> Deserialize<'de> for WorkerAttemptState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        WorkerAttemptStateWire::deserialize(deserializer)?
            .try_into()
            .map_err(de::Error::custom)
    }
}

impl WorkerAttemptState {
    pub fn transition(&self, command: &WorkerCommandEnvelope) -> WorkerResult<WorkerTransition> {
        self.validate()?;
        let fact = decide(self, command)?;
        let mut aggregate = self.clone();
        apply(&mut aggregate, &fact)?;
        Ok(WorkerTransition { aggregate, fact })
    }

    pub fn replay(facts: &[WorkerFact]) -> WorkerResult<Self> {
        let (first, rest) = facts.split_first().ok_or(WorkerError::HistoryEmpty)?;
        let WorkerFactKind::AttemptGranted { grant } = &first.kind else {
            return Err(WorkerError::HistoryMalformed);
        };
        if first.observed_at != grant.granted_at {
            return Err(WorkerError::HistoryMalformed);
        }
        let mut state = from_grant(grant)?;
        for fact in rest {
            apply(&mut state, fact)?;
        }
        Ok(state)
    }

    pub fn validate(&self) -> WorkerResult<()> {
        if self.attempt_id.as_uuid().is_nil()
            || self.package_id.as_uuid().is_nil()
            || self.lease_id.as_uuid().is_nil()
            || self.package_hash.as_bytes().iter().all(|byte| *byte == 0)
            || self.lease_expires_at <= self.created_at
            || self.updated_at < self.created_at
            || self.version == AggregateVersion::ZERO
            || self.version.get() != self.journal_seq
        {
            return Err(WorkerError::HistoryMalformed);
        }
        let waiting_shape = self.phase == WorkerPhase::WaitingInput;
        if waiting_shape
            != (self
                .resume_phase
                .is_some_and(WorkerPhase::can_resume_from_wait)
                && self.wake_condition.is_some())
            || (!waiting_shape && (self.resume_phase.is_some() || self.wake_condition.is_some()))
        {
            return Err(WorkerError::HistoryMalformed);
        }
        let has_candidate = self.candidate.is_some();
        let candidate_required = matches!(
            self.phase,
            WorkerPhase::HandingOffCandidate | WorkerPhase::AuthorComplete
        );
        let candidate_allowed = candidate_required
            || matches!(
                self.phase,
                WorkerPhase::Salvaging | WorkerPhase::LocalCancelled | WorkerPhase::LocalFailed
            );
        if (candidate_required && !has_candidate)
            || (has_candidate && !candidate_allowed)
            || (self.phase == WorkerPhase::AuthorComplete) != self.candidate_id.is_some()
            || (self.phase != WorkerPhase::AuthorComplete && self.candidate_id.is_some())
            || self.candidate.as_ref().is_some_and(|candidate| {
                candidate.validate().is_err() || self.current_tree.as_ref() != Some(&candidate.tree)
            })
        {
            return Err(WorkerError::HistoryMalformed);
        }
        let requires_tree = matches!(
            self.phase,
            WorkerPhase::LocalVerifying
                | WorkerPhase::SealingCandidate
                | WorkerPhase::HandingOffCandidate
                | WorkerPhase::AuthorComplete
        );
        if requires_tree && self.current_tree.is_none() {
            return Err(WorkerError::HistoryMalformed);
        }
        if self.current_tree.is_some() != (self.turns_completed > 0)
            || (self.turns_completed > 0 && self.plan_digest.is_none())
            || matches!(
                self.phase,
                WorkerPhase::Implementing
                    | WorkerPhase::LocalVerifying
                    | WorkerPhase::SealingCandidate
                    | WorkerPhase::HandingOffCandidate
                    | WorkerPhase::AuthorComplete
            ) && self.plan_digest.is_none()
            || matches!(
                self.phase,
                WorkerPhase::Granted
                    | WorkerPhase::Preparing
                    | WorkerPhase::Baseline
                    | WorkerPhase::Planning
            ) && (self.plan_digest.is_some()
                || self.current_tree.is_some()
                || self.turns_completed != 0)
        {
            return Err(WorkerError::HistoryMalformed);
        }
        let terminal_code_required = matches!(
            self.phase,
            WorkerPhase::LocalFailed | WorkerPhase::LocalCancelled
        );
        if terminal_code_required != self.terminal_code.is_some() {
            return Err(WorkerError::HistoryMalformed);
        }
        Ok(())
    }

    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    #[must_use]
    pub const fn phase(&self) -> WorkerPhase {
        self.phase
    }

    #[must_use]
    pub const fn version(&self) -> AggregateVersion {
        self.version
    }

    #[must_use]
    pub const fn journal_seq(&self) -> u64 {
        self.journal_seq
    }

    #[must_use]
    pub const fn lease_generation(&self) -> FencingToken {
        self.lease_generation
    }

    #[must_use]
    pub const fn lease_expires_at(&self) -> ServerInstant {
        self.lease_expires_at
    }

    #[must_use]
    pub const fn plan_digest(&self) -> Option<Sha256Digest> {
        self.plan_digest
    }

    #[must_use]
    pub fn current_tree(&self) -> Option<&GitObjectId> {
        self.current_tree.as_ref()
    }

    #[must_use]
    pub const fn turns_completed(&self) -> u32 {
        self.turns_completed
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerCommandKind {
    BeginPreparation,
    WorkspacePrepared {
        workspace_digest: Sha256Digest,
    },
    BaselineFinished {
        passed: bool,
        evidence_digest: Sha256Digest,
        failure_code: Option<ProtocolKey>,
    },
    PlanAccepted {
        plan_digest: Sha256Digest,
    },
    TurnProducedChanges {
        turn_id: ProtocolKey,
        tree: GitObjectId,
        model_claimed_done: bool,
    },
    VerificationFinished {
        passed: bool,
        evidence_digest: Sha256Digest,
        failure_code: Option<ProtocolKey>,
    },
    WaitForInput {
        condition: WakeCondition,
    },
    Wake {
        fact: WakeFact,
    },
    SealCandidate {
        candidate: CandidateSnapshot,
        observed_generation: FencingToken,
    },
    ConfirmCandidateHandoff {
        candidate_id: ProtocolKey,
        observed_generation: FencingToken,
    },
    LoseLease {
        reason: LeaseLossReason,
        observed_generation: FencingToken,
    },
    Cancel {
        reason_code: ProtocolKey,
    },
    Fail {
        reason_code: ProtocolKey,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCommandEnvelope {
    pub expected_version: AggregateVersion,
    pub observed_at: ServerInstant,
    pub command: WorkerCommandKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerFact {
    pub observed_at: ServerInstant,
    pub kind: WorkerFactKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerFactKind {
    AttemptGranted {
        grant: AttemptGrant,
    },
    PreparationStarted,
    WorkspacePrepared {
        workspace_digest: Sha256Digest,
    },
    BaselinePassed {
        evidence_digest: Sha256Digest,
    },
    BaselineFailed {
        evidence_digest: Sha256Digest,
        failure_code: ProtocolKey,
    },
    PlanRecorded {
        plan_digest: Sha256Digest,
    },
    TurnRecorded {
        turn_id: ProtocolKey,
        tree: GitObjectId,
        model_claimed_done: bool,
    },
    VerificationPassed {
        evidence_digest: Sha256Digest,
    },
    VerificationFailed {
        evidence_digest: Sha256Digest,
        failure_code: ProtocolKey,
    },
    WaitingForInput {
        condition: WakeCondition,
        resume_phase: WorkerPhase,
    },
    Woken {
        fact: WakeFact,
        resume_phase: WorkerPhase,
    },
    CandidateSealed {
        candidate: CandidateSnapshot,
        lease_generation: FencingToken,
    },
    CandidateHandoffConfirmed {
        candidate_id: ProtocolKey,
        lease_generation: FencingToken,
    },
    LeaseLost {
        reason: LeaseLossReason,
        observed_generation: FencingToken,
    },
    Cancelled {
        reason_code: ProtocolKey,
    },
    Failed {
        reason_code: ProtocolKey,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerTransition {
    pub aggregate: WorkerAttemptState,
    pub fact: WorkerFact,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum WorkerError {
    #[error("invalid Worker argument: {0}")]
    InvalidArgument(&'static str),
    #[error("Worker Attempt version is stale")]
    StaleVersion,
    #[error("Worker Attempt transition is not allowed")]
    InvalidTransition,
    #[error("Worker Attempt history is empty")]
    HistoryEmpty,
    #[error("Worker Attempt history is malformed")]
    HistoryMalformed,
    #[error("Wake fact does not satisfy the persisted condition")]
    WakeMismatch,
    #[error("Lease generation is stale")]
    LeaseStale,
    #[error("Worker fact time regressed")]
    TimeRegressed,
}

impl WorkerError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidArgument(_) => "AF_WORKER_ARGUMENT_INVALID",
            Self::StaleVersion => "AF_STALE_VERSION",
            Self::InvalidTransition => "AF_TRANSITION_INVALID",
            Self::HistoryEmpty | Self::HistoryMalformed => "AF_WORKER_HISTORY_INVALID",
            Self::WakeMismatch => "AF_WAKE_CONDITION_UNMET",
            Self::LeaseStale => "AF_LEASE_STALE",
            Self::TimeRegressed => "AF_TIME_REGRESSED",
        }
    }
}

pub fn grant_fact(grant: AttemptGrant) -> WorkerResult<WorkerFact> {
    grant.validate()?;
    Ok(WorkerFact {
        observed_at: grant.granted_at,
        kind: WorkerFactKind::AttemptGranted { grant },
    })
}

pub fn from_grant(grant: &AttemptGrant) -> WorkerResult<WorkerAttemptState> {
    grant.validate()?;
    let state = WorkerAttemptState {
        attempt_id: grant.attempt_id,
        package_id: grant.package_id,
        package_revision: grant.package_revision,
        package_hash: grant.package_hash,
        base_commit: grant.base_commit.clone(),
        lease_id: grant.lease_id,
        lease_generation: grant.lease_generation,
        lease_expires_at: grant.lease_expires_at,
        phase: WorkerPhase::Granted,
        resume_phase: None,
        wake_condition: None,
        plan_digest: None,
        current_tree: None,
        last_verification_digest: None,
        candidate: None,
        candidate_id: None,
        terminal_code: None,
        turns_completed: 0,
        version: AggregateVersion::new(1),
        journal_seq: 1,
        created_at: grant.granted_at,
        updated_at: grant.granted_at,
    };
    state.validate()?;
    Ok(state)
}

fn decide(state: &WorkerAttemptState, command: &WorkerCommandEnvelope) -> WorkerResult<WorkerFact> {
    if state.phase.is_terminal() {
        return Err(WorkerError::InvalidTransition);
    }
    if command.expected_version != state.version {
        return Err(WorkerError::StaleVersion);
    }
    if command.observed_at < state.updated_at {
        return Err(WorkerError::TimeRegressed);
    }

    let kind = match &command.command {
        WorkerCommandKind::BeginPreparation if state.phase == WorkerPhase::Granted => {
            WorkerFactKind::PreparationStarted
        }
        WorkerCommandKind::WorkspacePrepared { workspace_digest }
            if state.phase == WorkerPhase::Preparing =>
        {
            require_digest(workspace_digest, "workspace_digest")?;
            WorkerFactKind::WorkspacePrepared {
                workspace_digest: *workspace_digest,
            }
        }
        WorkerCommandKind::BaselineFinished {
            passed,
            evidence_digest,
            failure_code,
        } if state.phase == WorkerPhase::Baseline => {
            require_digest(evidence_digest, "evidence_digest")?;
            if *passed {
                if failure_code.is_some() {
                    return Err(WorkerError::InvalidArgument("failure_code"));
                }
                WorkerFactKind::BaselinePassed {
                    evidence_digest: *evidence_digest,
                }
            } else {
                WorkerFactKind::BaselineFailed {
                    evidence_digest: *evidence_digest,
                    failure_code: failure_code
                        .clone()
                        .ok_or(WorkerError::InvalidArgument("failure_code"))?,
                }
            }
        }
        WorkerCommandKind::PlanAccepted { plan_digest } if state.phase == WorkerPhase::Planning => {
            require_digest(plan_digest, "plan_digest")?;
            WorkerFactKind::PlanRecorded {
                plan_digest: *plan_digest,
            }
        }
        WorkerCommandKind::TurnProducedChanges {
            turn_id,
            tree,
            model_claimed_done,
        } if state.phase == WorkerPhase::Implementing => WorkerFactKind::TurnRecorded {
            turn_id: turn_id.clone(),
            tree: tree.clone(),
            model_claimed_done: *model_claimed_done,
        },
        WorkerCommandKind::VerificationFinished {
            passed,
            evidence_digest,
            failure_code,
        } if state.phase == WorkerPhase::LocalVerifying => {
            require_digest(evidence_digest, "evidence_digest")?;
            if *passed {
                if failure_code.is_some() {
                    return Err(WorkerError::InvalidArgument("failure_code"));
                }
                WorkerFactKind::VerificationPassed {
                    evidence_digest: *evidence_digest,
                }
            } else {
                WorkerFactKind::VerificationFailed {
                    evidence_digest: *evidence_digest,
                    failure_code: failure_code
                        .clone()
                        .ok_or(WorkerError::InvalidArgument("failure_code"))?,
                }
            }
        }
        WorkerCommandKind::WaitForInput { condition } if state.phase.can_resume_from_wait() => {
            WorkerFactKind::WaitingForInput {
                condition: condition.clone(),
                resume_phase: state.phase,
            }
        }
        WorkerCommandKind::Wake { fact } if state.phase == WorkerPhase::WaitingInput => {
            let condition = state
                .wake_condition
                .as_ref()
                .ok_or(WorkerError::HistoryMalformed)?;
            if !condition.is_satisfied_by(fact) {
                return Err(WorkerError::WakeMismatch);
            }
            WorkerFactKind::Woken {
                fact: fact.clone(),
                resume_phase: state
                    .resume_phase
                    .filter(|phase| phase.can_resume_from_wait())
                    .ok_or(WorkerError::HistoryMalformed)?,
            }
        }
        WorkerCommandKind::SealCandidate {
            candidate,
            observed_generation,
        } if state.phase == WorkerPhase::SealingCandidate => {
            require_generation(state, *observed_generation)?;
            candidate.validate()?;
            WorkerFactKind::CandidateSealed {
                candidate: candidate.clone(),
                lease_generation: *observed_generation,
            }
        }
        WorkerCommandKind::ConfirmCandidateHandoff {
            candidate_id,
            observed_generation,
        } if state.phase == WorkerPhase::HandingOffCandidate => {
            require_generation(state, *observed_generation)?;
            WorkerFactKind::CandidateHandoffConfirmed {
                candidate_id: candidate_id.clone(),
                lease_generation: *observed_generation,
            }
        }
        WorkerCommandKind::LoseLease {
            reason,
            observed_generation,
        } => {
            if *reason == LeaseLossReason::HigherGeneration {
                if observed_generation.get() <= state.lease_generation.get() {
                    return Err(WorkerError::LeaseStale);
                }
            } else if observed_generation != &state.lease_generation {
                return Err(WorkerError::LeaseStale);
            }
            WorkerFactKind::LeaseLost {
                reason: *reason,
                observed_generation: *observed_generation,
            }
        }
        WorkerCommandKind::Cancel { reason_code } => WorkerFactKind::Cancelled {
            reason_code: reason_code.clone(),
        },
        WorkerCommandKind::Fail { reason_code } => WorkerFactKind::Failed {
            reason_code: reason_code.clone(),
        },
        _ => return Err(WorkerError::InvalidTransition),
    };
    Ok(WorkerFact {
        observed_at: command.observed_at,
        kind,
    })
}

pub fn apply(state: &mut WorkerAttemptState, fact: &WorkerFact) -> WorkerResult<()> {
    state.validate()?;
    if state.phase.is_terminal() || fact.observed_at < state.updated_at {
        return Err(if state.phase.is_terminal() {
            WorkerError::InvalidTransition
        } else {
            WorkerError::TimeRegressed
        });
    }
    match &fact.kind {
        WorkerFactKind::AttemptGranted { .. } => return Err(WorkerError::HistoryMalformed),
        WorkerFactKind::PreparationStarted if state.phase == WorkerPhase::Granted => {
            state.phase = WorkerPhase::Preparing;
        }
        WorkerFactKind::WorkspacePrepared { workspace_digest }
            if state.phase == WorkerPhase::Preparing =>
        {
            require_digest(workspace_digest, "workspace_digest")?;
            state.phase = WorkerPhase::Baseline;
        }
        WorkerFactKind::BaselinePassed { evidence_digest }
            if state.phase == WorkerPhase::Baseline =>
        {
            require_digest(evidence_digest, "evidence_digest")?;
            state.last_verification_digest = Some(*evidence_digest);
            state.phase = WorkerPhase::Planning;
        }
        WorkerFactKind::BaselineFailed {
            evidence_digest,
            failure_code,
        } if state.phase == WorkerPhase::Baseline => {
            require_digest(evidence_digest, "evidence_digest")?;
            state.last_verification_digest = Some(*evidence_digest);
            state.terminal_code = Some(failure_code.clone());
            state.phase = WorkerPhase::LocalFailed;
        }
        WorkerFactKind::PlanRecorded { plan_digest } if state.phase == WorkerPhase::Planning => {
            require_digest(plan_digest, "plan_digest")?;
            state.plan_digest = Some(*plan_digest);
            state.phase = WorkerPhase::Implementing;
        }
        WorkerFactKind::TurnRecorded { tree, .. } if state.phase == WorkerPhase::Implementing => {
            state.current_tree = Some(tree.clone());
            state.turns_completed = state
                .turns_completed
                .checked_add(1)
                .ok_or(WorkerError::HistoryMalformed)?;
            state.phase = WorkerPhase::LocalVerifying;
        }
        WorkerFactKind::VerificationPassed { evidence_digest }
            if state.phase == WorkerPhase::LocalVerifying =>
        {
            require_digest(evidence_digest, "evidence_digest")?;
            state.last_verification_digest = Some(*evidence_digest);
            state.phase = WorkerPhase::SealingCandidate;
        }
        WorkerFactKind::VerificationFailed {
            evidence_digest, ..
        } if state.phase == WorkerPhase::LocalVerifying => {
            require_digest(evidence_digest, "evidence_digest")?;
            state.last_verification_digest = Some(*evidence_digest);
            state.phase = WorkerPhase::Implementing;
        }
        WorkerFactKind::WaitingForInput {
            condition,
            resume_phase,
        } if state.phase == *resume_phase && resume_phase.can_resume_from_wait() => {
            state.wake_condition = Some(condition.clone());
            state.resume_phase = Some(*resume_phase);
            state.phase = WorkerPhase::WaitingInput;
        }
        WorkerFactKind::Woken { fact, resume_phase }
            if state.phase == WorkerPhase::WaitingInput
                && resume_phase.can_resume_from_wait()
                && state.resume_phase == Some(*resume_phase)
                && state
                    .wake_condition
                    .as_ref()
                    .is_some_and(|condition| condition.is_satisfied_by(fact)) =>
        {
            state.phase = *resume_phase;
            state.resume_phase = None;
            state.wake_condition = None;
        }
        WorkerFactKind::CandidateSealed {
            candidate,
            lease_generation,
        } if state.phase == WorkerPhase::SealingCandidate => {
            require_generation(state, *lease_generation)?;
            candidate.validate()?;
            if state.current_tree.as_ref() != Some(&candidate.tree) {
                return Err(WorkerError::InvalidArgument("candidate_tree"));
            }
            state.candidate = Some(candidate.clone());
            state.phase = WorkerPhase::HandingOffCandidate;
        }
        WorkerFactKind::CandidateHandoffConfirmed {
            candidate_id,
            lease_generation,
        } if state.phase == WorkerPhase::HandingOffCandidate => {
            require_generation(state, *lease_generation)?;
            if state.candidate.is_none() {
                return Err(WorkerError::HistoryMalformed);
            }
            state.candidate_id = Some(candidate_id.clone());
            state.phase = WorkerPhase::AuthorComplete;
        }
        WorkerFactKind::LeaseLost {
            reason,
            observed_generation,
        } if state.phase != WorkerPhase::Salvaging => {
            if *reason == LeaseLossReason::HigherGeneration {
                if observed_generation.get() <= state.lease_generation.get() {
                    return Err(WorkerError::LeaseStale);
                }
            } else if observed_generation != &state.lease_generation {
                return Err(WorkerError::LeaseStale);
            }
            state.resume_phase = None;
            state.wake_condition = None;
            state.phase = WorkerPhase::Salvaging;
        }
        WorkerFactKind::Cancelled { reason_code } => {
            state.resume_phase = None;
            state.wake_condition = None;
            state.terminal_code = Some(reason_code.clone());
            state.phase = WorkerPhase::LocalCancelled;
        }
        WorkerFactKind::Failed { reason_code } => {
            state.resume_phase = None;
            state.wake_condition = None;
            state.terminal_code = Some(reason_code.clone());
            state.phase = WorkerPhase::LocalFailed;
        }
        _ => return Err(WorkerError::InvalidTransition),
    }
    state.version = state
        .version
        .checked_next()
        .map_err(|_| WorkerError::HistoryMalformed)?;
    state.journal_seq = state
        .journal_seq
        .checked_add(1)
        .ok_or(WorkerError::HistoryMalformed)?;
    state.updated_at = fact.observed_at;
    state.validate()
}

fn require_digest(digest: &Sha256Digest, field: &'static str) -> WorkerResult<()> {
    if digest.as_bytes().iter().all(|byte| *byte == 0) {
        Err(WorkerError::InvalidArgument(field))
    } else {
        Ok(())
    }
}

fn require_generation(
    state: &WorkerAttemptState,
    observed_generation: FencingToken,
) -> WorkerResult<()> {
    if observed_generation == state.lease_generation {
        Ok(())
    } else {
        Err(WorkerError::LeaseStale)
    }
}

pub type WorkerResult<T> = Result<T, WorkerError>;

#[cfg(test)]
mod tests {
    use agentforge_domain::{PackageRevision, attempt::WakeFact};
    use time::macros::datetime;
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(second: u8) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(i64::from(second)))
    }

    fn digest(value: &str) -> Sha256Digest {
        Sha256Digest::of_bytes(value)
    }

    fn grant() -> AttemptGrant {
        AttemptGrant {
            attempt_id: id(1),
            package_id: id(2),
            package_revision: PackageRevision::new(1).expect("revision"),
            package_hash: digest("package"),
            base_commit: GitObjectId::new("1".repeat(40)).expect("commit"),
            lease_id: id(3),
            lease_generation: FencingToken::new(4).expect("generation"),
            lease_expires_at: at(60),
            granted_at: at(0),
        }
    }

    fn command(
        state: &WorkerAttemptState,
        second: u8,
        command: WorkerCommandKind,
    ) -> WorkerCommandEnvelope {
        WorkerCommandEnvelope {
            expected_version: state.version,
            observed_at: at(second),
            command,
        }
    }

    fn advance(
        state: WorkerAttemptState,
        second: u8,
        command_kind: WorkerCommandKind,
    ) -> WorkerAttemptState {
        state
            .transition(&command(&state, second, command_kind))
            .expect("legal transition")
            .aggregate
    }

    #[test]
    fn happy_path_requires_local_verification_before_author_complete() {
        let mut facts = vec![grant_fact(grant()).expect("grant fact")];
        let mut state = WorkerAttemptState::replay(&facts).expect("grant replay");
        for (second, next) in [
            (1, WorkerCommandKind::BeginPreparation),
            (
                2,
                WorkerCommandKind::WorkspacePrepared {
                    workspace_digest: digest("workspace"),
                },
            ),
            (
                3,
                WorkerCommandKind::BaselineFinished {
                    passed: true,
                    evidence_digest: digest("baseline"),
                    failure_code: None,
                },
            ),
            (
                4,
                WorkerCommandKind::PlanAccepted {
                    plan_digest: digest("plan"),
                },
            ),
            (
                5,
                WorkerCommandKind::TurnProducedChanges {
                    turn_id: ProtocolKey::new("turn-1").expect("turn"),
                    tree: GitObjectId::new("2".repeat(40)).expect("tree"),
                    model_claimed_done: false,
                },
            ),
            (
                6,
                WorkerCommandKind::VerificationFinished {
                    passed: true,
                    evidence_digest: digest("verify"),
                    failure_code: None,
                },
            ),
            (
                7,
                WorkerCommandKind::SealCandidate {
                    candidate: CandidateSnapshot {
                        commit: GitObjectId::new("3".repeat(40)).expect("commit"),
                        tree: GitObjectId::new("2".repeat(40)).expect("tree"),
                        author_evidence_digest: digest("evidence"),
                    },
                    observed_generation: FencingToken::new(4).expect("generation"),
                },
            ),
            (
                8,
                WorkerCommandKind::ConfirmCandidateHandoff {
                    candidate_id: ProtocolKey::new("candidate-1").expect("candidate"),
                    observed_generation: FencingToken::new(4).expect("generation"),
                },
            ),
        ] {
            let transition = state
                .transition(&command(&state, second, next))
                .expect("legal transition");
            facts.push(transition.fact);
            state = transition.aggregate;
        }
        assert_eq!(state.phase, WorkerPhase::AuthorComplete);
        assert_eq!(state.journal_seq, 9);
        assert_eq!(WorkerAttemptState::replay(&facts).expect("replay"), state);
        assert_eq!(
            state
                .transition(&command(&state, 9, WorkerCommandKind::BeginPreparation))
                .expect_err("terminal state cannot recover")
                .code(),
            "AF_TRANSITION_INVALID"
        );
    }

    #[test]
    fn waiting_requires_a_matching_fact_and_restores_only_semantic_phases() {
        let mut state = from_grant(&grant()).expect("grant");
        state = advance(state, 1, WorkerCommandKind::BeginPreparation);
        state = advance(
            state,
            2,
            WorkerCommandKind::WorkspacePrepared {
                workspace_digest: digest("workspace"),
            },
        );
        state = advance(
            state,
            3,
            WorkerCommandKind::BaselineFinished {
                passed: true,
                evidence_digest: digest("baseline"),
                failure_code: None,
            },
        );
        let question = ProtocolKey::new("question-1").expect("question");
        state = advance(
            state,
            4,
            WorkerCommandKind::WaitForInput {
                condition: WakeCondition::QuestionAnswered {
                    question_id: question.clone(),
                },
            },
        );
        assert_eq!(state.phase, WorkerPhase::WaitingInput);
        assert_eq!(state.resume_phase, Some(WorkerPhase::Planning));
        let mismatch = WorkerCommandKind::Wake {
            fact: WakeFact::QuestionAnswered {
                question_id: ProtocolKey::new("question-2").expect("question"),
            },
        };
        assert_eq!(
            state
                .transition(&command(&state, 5, mismatch))
                .expect_err("wrong wake")
                .code(),
            "AF_WAKE_CONDITION_UNMET"
        );
        let salvaged = advance(
            state.clone(),
            5,
            WorkerCommandKind::LoseLease {
                reason: LeaseLossReason::Expired,
                observed_generation: state.lease_generation,
            },
        );
        assert_eq!(salvaged.phase, WorkerPhase::Salvaging);
        assert_eq!(salvaged.resume_phase, None);
        assert_eq!(salvaged.wake_condition, None);
        state = advance(
            state,
            6,
            WorkerCommandKind::Wake {
                fact: WakeFact::QuestionAnswered {
                    question_id: question,
                },
            },
        );
        assert_eq!(state.phase, WorkerPhase::Planning);
    }

    #[test]
    fn baseline_blocker_is_terminal_and_higher_generation_is_salvage_only() {
        let mut state = from_grant(&grant()).expect("grant");
        state = advance(state, 1, WorkerCommandKind::BeginPreparation);
        state = advance(
            state,
            2,
            WorkerCommandKind::WorkspacePrepared {
                workspace_digest: digest("workspace"),
            },
        );
        let failed = advance(
            state.clone(),
            3,
            WorkerCommandKind::BaselineFinished {
                passed: false,
                evidence_digest: digest("failure"),
                failure_code: Some(ProtocolKey::new("baseline-blocked").expect("code")),
            },
        );
        assert_eq!(failed.phase, WorkerPhase::LocalFailed);

        let stale = WorkerCommandKind::LoseLease {
            reason: LeaseLossReason::HigherGeneration,
            observed_generation: state.lease_generation,
        };
        assert_eq!(
            state
                .transition(&command(&state, 3, stale))
                .expect_err("same generation is not higher")
                .code(),
            "AF_LEASE_STALE"
        );
        let next = FencingToken::new(state.lease_generation.get() + 1).expect("generation");
        state = advance(
            state,
            4,
            WorkerCommandKind::LoseLease {
                reason: LeaseLossReason::HigherGeneration,
                observed_generation: next,
            },
        );
        assert_eq!(state.phase, WorkerPhase::Salvaging);
        assert!(
            state
                .transition(&command(
                    &state,
                    5,
                    WorkerCommandKind::SealCandidate {
                        candidate: CandidateSnapshot {
                            commit: GitObjectId::new("3".repeat(40)).expect("commit"),
                            tree: GitObjectId::new("2".repeat(40)).expect("tree"),
                            author_evidence_digest: digest("evidence"),
                        },
                        observed_generation: state.lease_generation,
                    },
                ))
                .is_err()
        );
    }

    #[test]
    fn deserialization_cannot_bypass_worker_state_invariants() {
        let state = from_grant(&grant()).expect("grant");
        let mut value = serde_json::to_value(&state).expect("serialize state");
        value["journal_seq"] = serde_json::json!(99);
        let error = serde_json::from_value::<WorkerAttemptState>(value)
            .expect_err("version and journal sequence must stay correlated");
        assert!(error.to_string().contains("history is malformed"));

        let mut value = serde_json::to_value(&state).expect("serialize state");
        value["phase"] = serde_json::json!("waiting_input");
        assert!(
            serde_json::from_value::<WorkerAttemptState>(value).is_err(),
            "waiting state requires a persisted condition and resume phase"
        );

        let mut value = serde_json::to_value(&state).expect("serialize state");
        value["phase"] = serde_json::json!("implementing");
        assert!(
            serde_json::from_value::<WorkerAttemptState>(value).is_err(),
            "implementing requires a durable plan digest"
        );
    }
}
