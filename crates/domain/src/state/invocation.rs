//! Invocation intent scheduling, run claims, and immutable activation windows.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, BudgetReservationId, CorrelationId, ExecutorId, IntentClaimToken,
        InvocationIntentId, InvocationRunId, LeaseId, NodeId, ProjectId, ProtocolKey, RunClaimId,
        RunClaimToken, RunSignalId, ServerInstant, Sha256Digest,
    },
    state::{
        Transition,
        run_signal::{InvocationBinding, InvocationSubject, RunSignal},
        session::{SessionCapsuleRef, TokenUsage},
    },
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvocationSignalRef {
    pub id: RunSignalId,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub binding: InvocationBinding,
    pub dedup_digest: Sha256Digest,
    pub not_before: ServerInstant,
}

impl InvocationSignalRef {
    pub fn from_signal(signal: &RunSignal) -> Result<Self, DomainError> {
        Ok(Self {
            id: signal.id,
            project_id: signal.project_id,
            subject: signal.subject,
            binding: signal.binding.clone(),
            dedup_digest: signal.dedup_digest()?,
            not_before: signal.not_before,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IntentClaim {
    pub token: IntentClaimToken,
    pub claimed_by: NodeId,
    pub claimed_at: ServerInstant,
    pub claim_until: ServerInstant,
}

impl IntentClaim {
    fn validate(&self, now: ServerInstant) -> Result<(), DomainError> {
        if self.claimed_at > now || now >= self.claim_until {
            return Err(DomainError::InvalidArgument {
                field: "intent_claim_window".into(),
                reason: "must satisfy claimed_at <= now < claim_until".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateInvocationIntent {
    pub id: InvocationIntentId,
    pub signal: InvocationSignalRef,
    pub created_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationIntentState {
    Pending,
    Claimed,
    Dispatched,
    Satisfied,
    Cancelled,
    DeadLetter,
}

impl InvocationIntentState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Dispatched => "dispatched",
            Self::Satisfied => "satisfied",
            Self::Cancelled => "cancelled",
            Self::DeadLetter => "dead_letter",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Satisfied | Self::Cancelled | Self::DeadLetter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum InvocationIntentCommandKind {
    Create,
    AttachEquivalentSignal,
    Claim,
    RetryExpiredClaim,
    Dispatch,
    Satisfy,
    Cancel,
    DeadLetter,
}

impl InvocationIntentCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create_intent",
            Self::AttachEquivalentSignal => "attach_equivalent_signal",
            Self::Claim => "claim_intent",
            Self::RetryExpiredClaim => "retry_intent_claim",
            Self::Dispatch => "dispatch_intent",
            Self::Satisfy => "satisfy_intent",
            Self::Cancel => "cancel_intent",
            Self::DeadLetter => "dead_letter_intent",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationIntentCommand {
    Create(CreateInvocationIntent),
    AttachEquivalentSignal {
        expected_version: AggregateVersion,
        signal: InvocationSignalRef,
    },
    Claim {
        expected_version: AggregateVersion,
        claim: IntentClaim,
        now: ServerInstant,
    },
    RetryExpiredClaim {
        expected_version: AggregateVersion,
        now: ServerInstant,
    },
    Dispatch {
        expected_version: AggregateVersion,
        claim_token: IntentClaimToken,
        invocation_run_id: InvocationRunId,
        binding_is_current: bool,
        deadline_not_elapsed: bool,
        run_claim_and_budget_reserved: bool,
    },
    Satisfy {
        expected_version: AggregateVersion,
        invocation_run_id: InvocationRunId,
        run_is_terminal: bool,
        outcome_registered: bool,
    },
    Cancel {
        expected_version: AggregateVersion,
        reason_code: String,
    },
    DeadLetter {
        expected_version: AggregateVersion,
        retry_limit_exhausted: bool,
        governance_case_created: bool,
        reason_code: String,
    },
}

impl InvocationIntentCommand {
    #[must_use]
    pub const fn kind(&self) -> InvocationIntentCommandKind {
        match self {
            Self::Create(_) => InvocationIntentCommandKind::Create,
            Self::AttachEquivalentSignal { .. } => {
                InvocationIntentCommandKind::AttachEquivalentSignal
            }
            Self::Claim { .. } => InvocationIntentCommandKind::Claim,
            Self::RetryExpiredClaim { .. } => InvocationIntentCommandKind::RetryExpiredClaim,
            Self::Dispatch { .. } => InvocationIntentCommandKind::Dispatch,
            Self::Satisfy { .. } => InvocationIntentCommandKind::Satisfy,
            Self::Cancel { .. } => InvocationIntentCommandKind::Cancel,
            Self::DeadLetter { .. } => InvocationIntentCommandKind::DeadLetter,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Create(_) => None,
            Self::AttachEquivalentSignal {
                expected_version, ..
            }
            | Self::Claim {
                expected_version, ..
            }
            | Self::RetryExpiredClaim {
                expected_version, ..
            }
            | Self::Dispatch {
                expected_version, ..
            }
            | Self::Satisfy {
                expected_version, ..
            }
            | Self::Cancel {
                expected_version, ..
            }
            | Self::DeadLetter {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InvocationIntentEvent {
    Created(CreateInvocationIntent),
    EquivalentSignalAttached { signal: InvocationSignalRef },
    Claimed { claim: IntentClaim },
    ClaimExpired,
    Dispatched { invocation_run_id: InvocationRunId },
    Satisfied,
    Cancelled { reason_code: String },
    DeadLettered { reason_code: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvocationIntent {
    pub id: InvocationIntentId,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub binding: InvocationBinding,
    pub dedup_digest: Sha256Digest,
    pub state: InvocationIntentState,
    pub primary_signal_id: RunSignalId,
    pub signal_ids: BTreeSet<RunSignalId>,
    pub signal_count: u32,
    pub not_before: ServerInstant,
    pub claim: Option<IntentClaim>,
    pub last_claim_token: Option<IntentClaimToken>,
    pub invocation_run_id: Option<InvocationRunId>,
    pub terminal_reason: Option<String>,
    pub created_at: ServerInstant,
    pub version: AggregateVersion,
}

impl InvocationIntent {
    pub fn transition(
        current: Option<&Self>,
        command: &InvocationIntentCommand,
    ) -> Result<Transition<Self, InvocationIntentEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &InvocationIntentCommand,
    ) -> Result<InvocationIntentEvent, DomainError> {
        match (current, command) {
            (None, InvocationIntentCommand::Create(create)) => {
                validate_intent_signal(&create.signal)?;
                Ok(InvocationIntentEvent::Created(create.clone()))
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "invocation_intent",
            }),
            (Some(intent), InvocationIntentCommand::Create(_)) => {
                Err(invalid_intent(intent.state, command.kind()))
            }
            (Some(intent), command) => {
                if intent.state.is_terminal() {
                    return Err(invalid_intent(intent.state, command.kind()));
                }
                if command.expected_version() != Some(intent.version) {
                    return Err(DomainError::StaleVersion);
                }
                match command {
                    InvocationIntentCommand::AttachEquivalentSignal { signal, .. }
                        if intent.state == InvocationIntentState::Pending =>
                    {
                        validate_intent_signal(signal)?;
                        if signal.project_id != intent.project_id
                            || signal.subject != intent.subject
                            || signal.binding != intent.binding
                            || signal.dedup_digest != intent.dedup_digest
                        {
                            return Err(DomainError::SignalNotCoalescible);
                        }
                        if intent.signal_ids.contains(&signal.id) {
                            return Err(DomainError::InvalidArgument {
                                field: "signal_id".into(),
                                reason: "is already attached".into(),
                            });
                        }
                        intent.signal_count.checked_add(1).ok_or(
                            DomainError::InvariantViolation {
                                invariant: "invocation_intent_signal_count_must_not_overflow",
                            },
                        )?;
                        Ok(InvocationIntentEvent::EquivalentSignalAttached {
                            signal: signal.clone(),
                        })
                    }
                    InvocationIntentCommand::Claim { claim, now, .. }
                        if intent.state == InvocationIntentState::Pending =>
                    {
                        if *now < intent.not_before {
                            return Err(DomainError::IntentNotDispatchable);
                        }
                        claim.validate(*now)?;
                        let expected = match intent.last_claim_token {
                            Some(previous) => previous.checked_next()?,
                            None => IntentClaimToken::new(1)?,
                        };
                        if claim.token != expected {
                            return Err(DomainError::IntentNotDispatchable);
                        }
                        Ok(InvocationIntentEvent::Claimed { claim: *claim })
                    }
                    InvocationIntentCommand::RetryExpiredClaim { now, .. }
                        if intent.state == InvocationIntentState::Claimed =>
                    {
                        let claim = intent.claim.ok_or(DomainError::InvariantViolation {
                            invariant: "claimed_intent_must_have_claim",
                        })?;
                        if *now < claim.claim_until {
                            return Err(invalid_intent(intent.state, command.kind()));
                        }
                        Ok(InvocationIntentEvent::ClaimExpired)
                    }
                    InvocationIntentCommand::Dispatch {
                        claim_token,
                        invocation_run_id,
                        binding_is_current,
                        deadline_not_elapsed,
                        run_claim_and_budget_reserved,
                        ..
                    } if intent.state == InvocationIntentState::Claimed => {
                        if intent.claim.map(|claim| claim.token) != Some(*claim_token) {
                            return Err(DomainError::IntentNotDispatchable);
                        }
                        if !*binding_is_current
                            || !*deadline_not_elapsed
                            || !*run_claim_and_budget_reserved
                        {
                            return Err(DomainError::IntentNotDispatchable);
                        }
                        Ok(InvocationIntentEvent::Dispatched {
                            invocation_run_id: *invocation_run_id,
                        })
                    }
                    InvocationIntentCommand::Satisfy {
                        invocation_run_id,
                        run_is_terminal,
                        outcome_registered,
                        ..
                    } if intent.state == InvocationIntentState::Dispatched
                        && intent.invocation_run_id == Some(*invocation_run_id)
                        && *run_is_terminal
                        && *outcome_registered =>
                    {
                        Ok(InvocationIntentEvent::Satisfied)
                    }
                    InvocationIntentCommand::Cancel { reason_code, .. }
                        if matches!(
                            intent.state,
                            InvocationIntentState::Pending | InvocationIntentState::Claimed
                        ) && valid_reason(reason_code) =>
                    {
                        Ok(InvocationIntentEvent::Cancelled {
                            reason_code: reason_code.clone(),
                        })
                    }
                    InvocationIntentCommand::DeadLetter {
                        retry_limit_exhausted,
                        governance_case_created,
                        reason_code,
                        ..
                    } if matches!(
                        intent.state,
                        InvocationIntentState::Pending | InvocationIntentState::Claimed
                    ) && *retry_limit_exhausted
                        && *governance_case_created
                        && valid_reason(reason_code) =>
                    {
                        Ok(InvocationIntentEvent::DeadLettered {
                            reason_code: reason_code.clone(),
                        })
                    }
                    _ => Err(invalid_intent(intent.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &InvocationIntentEvent,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, InvocationIntentEvent::Created(create)) => {
                validate_intent_signal(&create.signal)?;
                Ok(Self {
                    id: create.id,
                    project_id: create.signal.project_id,
                    subject: create.signal.subject,
                    binding: create.signal.binding.clone(),
                    dedup_digest: create.signal.dedup_digest,
                    state: InvocationIntentState::Pending,
                    primary_signal_id: create.signal.id,
                    signal_ids: BTreeSet::from([create.signal.id]),
                    signal_count: 1,
                    not_before: create.signal.not_before,
                    claim: None,
                    last_claim_token: None,
                    invocation_run_id: None,
                    terminal_reason: None,
                    created_at: create.created_at,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "invocation_intent",
            }),
            (Some(intent), _) if intent.state.is_terminal() => {
                Err(invalid_intent_event(intent.state, event))
            }
            (Some(intent), InvocationIntentEvent::EquivalentSignalAttached { signal })
                if intent.state == InvocationIntentState::Pending
                    && signal.project_id == intent.project_id
                    && signal.subject == intent.subject
                    && signal.binding == intent.binding
                    && signal.dedup_digest == intent.dedup_digest
                    && !intent.signal_ids.contains(&signal.id) =>
            {
                let mut next = intent.next_version()?;
                next.signal_ids.insert(signal.id);
                next.signal_count =
                    intent
                        .signal_count
                        .checked_add(1)
                        .ok_or(DomainError::InvariantViolation {
                            invariant: "invocation_intent_signal_count_must_not_overflow",
                        })?;
                next.not_before = next.not_before.max(signal.not_before);
                Ok(next)
            }
            (Some(intent), InvocationIntentEvent::Claimed { claim })
                if intent.state == InvocationIntentState::Pending =>
            {
                let mut next = intent.with_state(InvocationIntentState::Claimed)?;
                next.claim = Some(*claim);
                next.last_claim_token = Some(claim.token);
                Ok(next)
            }
            (Some(intent), InvocationIntentEvent::ClaimExpired)
                if intent.state == InvocationIntentState::Claimed && intent.claim.is_some() =>
            {
                let mut next = intent.with_state(InvocationIntentState::Pending)?;
                next.claim = None;
                Ok(next)
            }
            (Some(intent), InvocationIntentEvent::Dispatched { invocation_run_id })
                if intent.state == InvocationIntentState::Claimed && intent.claim.is_some() =>
            {
                let mut next = intent.with_state(InvocationIntentState::Dispatched)?;
                next.invocation_run_id = Some(*invocation_run_id);
                next.claim = None;
                Ok(next)
            }
            (Some(intent), InvocationIntentEvent::Satisfied)
                if intent.state == InvocationIntentState::Dispatched
                    && intent.invocation_run_id.is_some() =>
            {
                intent.with_state(InvocationIntentState::Satisfied)
            }
            (Some(intent), InvocationIntentEvent::Cancelled { reason_code })
                if matches!(
                    intent.state,
                    InvocationIntentState::Pending | InvocationIntentState::Claimed
                ) && valid_reason(reason_code) =>
            {
                let mut next = intent.with_state(InvocationIntentState::Cancelled)?;
                next.claim = None;
                next.terminal_reason = Some(reason_code.clone());
                Ok(next)
            }
            (Some(intent), InvocationIntentEvent::DeadLettered { reason_code })
                if matches!(
                    intent.state,
                    InvocationIntentState::Pending | InvocationIntentState::Claimed
                ) && valid_reason(reason_code) =>
            {
                let mut next = intent.with_state(InvocationIntentState::DeadLetter)?;
                next.claim = None;
                next.terminal_reason = Some(reason_code.clone());
                Ok(next)
            }
            (Some(intent), _) => Err(invalid_intent_event(intent.state, event)),
        }
    }

    pub fn replay(events: &[InvocationIntentEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "invocation_intent",
        })
    }

    fn next_version(&self) -> Result<Self, DomainError> {
        let mut next = self.clone();
        next.version = self.version.checked_next()?;
        Ok(next)
    }

    fn with_state(&self, state: InvocationIntentState) -> Result<Self, DomainError> {
        let mut next = self.next_version()?;
        next.state = state;
        Ok(next)
    }
}

fn validate_intent_signal(signal: &InvocationSignalRef) -> Result<(), DomainError> {
    signal.binding.validate_for(signal.subject)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunClaimState {
    Active,
    Completed,
    Expired,
    Revoked,
    Superseded,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunClaim {
    pub id: RunClaimId,
    pub run_id: InvocationRunId,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
    pub state: RunClaimState,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
}

impl RunClaim {
    fn validate_active(&self, run_id: InvocationRunId) -> Result<(), DomainError> {
        if self.run_id != run_id || self.state != RunClaimState::Active {
            return Err(DomainError::InvocationClaimStale);
        }
        if self.granted_at >= self.expires_at {
            return Err(DomainError::InvalidArgument {
                field: "run_claim_window".into(),
                reason: "must satisfy granted_at < expires_at".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunClaimProof {
    pub claim_id: RunClaimId,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvocationStartMaterial {
    pub intent_id: InvocationIntentId,
    pub intent_version: AggregateVersion,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub binding: InvocationBinding,
    pub signal_ids: Vec<RunSignalId>,
    pub adapter_id: ProtocolKey,
    pub executor_id: ExecutorId,
    pub executor_fingerprint: Sha256Digest,
    pub routing_decision_id: Option<CorrelationId>,
    pub input_capsule: Option<SessionCapsuleRef>,
    pub budget_reservation_id: BudgetReservationId,
    pub author_lease_id: Option<LeaseId>,
    pub run_claim_id: RunClaimId,
    pub run_claim_generation: RunClaimToken,
    pub external_invocation_key: ProtocolKey,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvocationStartContext {
    pub material: InvocationStartMaterial,
    pub context_digest: Sha256Digest,
}

impl InvocationStartContext {
    pub fn new(material: InvocationStartMaterial) -> Result<Self, DomainError> {
        validate_start_material(&material)?;
        let digest = digest_start_material(&material)?;
        Ok(Self {
            material,
            context_digest: digest,
        })
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        validate_start_material(&self.material)?;
        if digest_start_material(&self.material)? != self.context_digest {
            return Err(DomainError::InvariantViolation {
                invariant: "invocation_start_context_digest_must_match_material",
            });
        }
        Ok(())
    }
}

fn digest_start_material(material: &InvocationStartMaterial) -> Result<Sha256Digest, DomainError> {
    serde_json::to_vec(material)
        .map(Sha256Digest::of_bytes)
        .map_err(|_| DomainError::Internal)
}

fn validate_start_material(material: &InvocationStartMaterial) -> Result<(), DomainError> {
    material.binding.validate_for(material.subject)?;
    if material.intent_version == AggregateVersion::ZERO || material.signal_ids.is_empty() {
        return Err(DomainError::InvalidArgument {
            field: "invocation_start_context".into(),
            reason: "intent version and signal ids are required".into(),
        });
    }
    if material
        .signal_ids
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(DomainError::InvalidArgument {
            field: "signal_ids".into(),
            reason: "must be strictly sorted and unique".into(),
        });
    }
    if material.subject.is_attempt() != material.author_lease_id.is_some() {
        return Err(DomainError::InvocationBindingMismatch);
    }
    if material.binding.executor_fingerprint != Some(material.executor_fingerprint) {
        return Err(DomainError::InvocationBindingMismatch);
    }
    if material.input_capsule.map(|reference| reference.id) != material.binding.input_capsule_id
        || material.input_capsule.map(|reference| reference.digest)
            != material.binding.input_capsule_digest
    {
        return Err(DomainError::CapsuleBindingMismatch);
    }
    Ok(())
}

impl InvocationSubject {
    #[must_use]
    const fn is_attempt(self) -> bool {
        matches!(self, Self::Attempt(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReserveInvocationRun {
    pub id: InvocationRunId,
    pub start_context: InvocationStartContext,
    pub run_claim: RunClaim,
    pub reserved_at: ServerInstant,
}

impl ReserveInvocationRun {
    fn validate(&self) -> Result<(), DomainError> {
        self.start_context.validate()?;
        self.run_claim.validate_active(self.id)?;
        if self.run_claim.id != self.start_context.material.run_claim_id
            || self.run_claim.claim_generation != self.start_context.material.run_claim_generation
            || self.reserved_at < self.run_claim.granted_at
            || self.reserved_at >= self.run_claim.expires_at
        {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Reconciling => "reconciling",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationOutcome {
    Progressed,
    WaitingInput,
    CandidateProposed,
    PlanProposed,
    DecisionRequested,
    NoProgress,
    InfrastructureFailure,
    OutcomeUnknown,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum InvocationRunCommandKind {
    Reserve,
    MarkDispatchStarted,
    ObserveAdapterStarted,
    BeginReconciliation,
    Complete,
    Fail,
    Cancel,
}

impl InvocationRunCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserve => "reserve_invocation_run",
            Self::MarkDispatchStarted => "mark_dispatch_started",
            Self::ObserveAdapterStarted => "observe_adapter_started",
            Self::BeginReconciliation => "begin_reconciliation",
            Self::Complete => "complete_invocation_run",
            Self::Fail => "fail_invocation_run",
            Self::Cancel => "cancel_invocation_run",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationRunCommand {
    Reserve(Box<ReserveInvocationRun>),
    MarkDispatchStarted {
        expected_version: AggregateVersion,
        proof: RunClaimProof,
        outbox_dispatch_id: ProtocolKey,
        started_at: ServerInstant,
    },
    ObserveAdapterStarted {
        expected_version: AggregateVersion,
        proof: RunClaimProof,
        external_invocation_key: ProtocolKey,
        session_proof_digest: Sha256Digest,
        observed_at: ServerInstant,
    },
    BeginReconciliation {
        expected_version: AggregateVersion,
        reason_code: String,
        started_at: ServerInstant,
    },
    Complete {
        expected_version: AggregateVersion,
        proof: RunClaimProof,
        outcome: InvocationOutcome,
        outcome_digest: Sha256Digest,
        output_capsule: Option<SessionCapsuleRef>,
        usage: TokenUsage,
        budget_settled: bool,
        completed_at: ServerInstant,
    },
    Fail {
        expected_version: AggregateVersion,
        reason_code: String,
        evidence_digest: Sha256Digest,
        budget_settled: bool,
        failed_at: ServerInstant,
    },
    Cancel {
        expected_version: AggregateVersion,
        reason_code: String,
        stop_or_reconciliation_obligation_created: bool,
        cancelled_at: ServerInstant,
    },
}

impl InvocationRunCommand {
    #[must_use]
    pub const fn kind(&self) -> InvocationRunCommandKind {
        match self {
            Self::Reserve(_) => InvocationRunCommandKind::Reserve,
            Self::MarkDispatchStarted { .. } => InvocationRunCommandKind::MarkDispatchStarted,
            Self::ObserveAdapterStarted { .. } => InvocationRunCommandKind::ObserveAdapterStarted,
            Self::BeginReconciliation { .. } => InvocationRunCommandKind::BeginReconciliation,
            Self::Complete { .. } => InvocationRunCommandKind::Complete,
            Self::Fail { .. } => InvocationRunCommandKind::Fail,
            Self::Cancel { .. } => InvocationRunCommandKind::Cancel,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Reserve(_) => None,
            Self::MarkDispatchStarted {
                expected_version, ..
            }
            | Self::ObserveAdapterStarted {
                expected_version, ..
            }
            | Self::BeginReconciliation {
                expected_version, ..
            }
            | Self::Complete {
                expected_version, ..
            }
            | Self::Fail {
                expected_version, ..
            }
            | Self::Cancel {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InvocationRunEvent {
    Reserved(Box<ReserveInvocationRun>),
    DispatchStarted {
        outbox_dispatch_id: ProtocolKey,
        started_at: ServerInstant,
    },
    AdapterStarted {
        external_invocation_key: ProtocolKey,
        session_proof_digest: Sha256Digest,
        observed_at: ServerInstant,
    },
    ReconciliationStarted {
        reason_code: String,
        started_at: ServerInstant,
    },
    Completed {
        proof: RunClaimProof,
        outcome: InvocationOutcome,
        outcome_digest: Sha256Digest,
        output_capsule: Option<SessionCapsuleRef>,
        usage: TokenUsage,
        budget_settled: bool,
        completed_at: ServerInstant,
    },
    Failed {
        reason_code: String,
        evidence_digest: Sha256Digest,
        budget_settled: bool,
        failed_at: ServerInstant,
    },
    Cancelled {
        reason_code: String,
        stop_or_reconciliation_obligation_created: bool,
        cancelled_at: ServerInstant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvocationRun {
    pub id: InvocationRunId,
    /// Creation-time binding; no later event can replace it.
    pub start_context: InvocationStartContext,
    pub state: InvocationRunState,
    pub current_claim: RunClaim,
    pub outbox_dispatch_id: Option<ProtocolKey>,
    pub session_proof_digest: Option<Sha256Digest>,
    pub output_capsule: Option<SessionCapsuleRef>,
    pub outcome: Option<InvocationOutcome>,
    pub outcome_digest: Option<Sha256Digest>,
    pub usage: Option<TokenUsage>,
    pub terminal_reason: Option<String>,
    pub terminal_evidence_digest: Option<Sha256Digest>,
    pub reserved_at: ServerInstant,
    pub started_at: Option<ServerInstant>,
    pub terminal_at: Option<ServerInstant>,
    pub version: AggregateVersion,
}

impl InvocationRun {
    pub fn transition(
        current: Option<&Self>,
        command: &InvocationRunCommand,
    ) -> Result<Transition<Self, InvocationRunEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &InvocationRunCommand,
    ) -> Result<InvocationRunEvent, DomainError> {
        match (current, command) {
            (None, InvocationRunCommand::Reserve(reserve)) => {
                reserve.validate()?;
                Ok(InvocationRunEvent::Reserved(reserve.clone()))
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "invocation_run",
            }),
            (Some(run), InvocationRunCommand::Reserve(_)) => {
                Err(invalid_run(run.state, command.kind()))
            }
            (Some(run), command) => {
                if run.state.is_terminal() {
                    return Err(DomainError::InvocationRunTerminal);
                }
                if command.expected_version() != Some(run.version) {
                    return Err(DomainError::StaleVersion);
                }
                match command {
                    InvocationRunCommand::MarkDispatchStarted {
                        proof,
                        outbox_dispatch_id,
                        started_at,
                        ..
                    } if run.state == InvocationRunState::Reserved => {
                        run.authorize(*proof, *started_at)?;
                        Ok(InvocationRunEvent::DispatchStarted {
                            outbox_dispatch_id: outbox_dispatch_id.clone(),
                            started_at: *started_at,
                        })
                    }
                    InvocationRunCommand::ObserveAdapterStarted {
                        proof,
                        external_invocation_key,
                        session_proof_digest,
                        observed_at,
                        ..
                    } if run.state == InvocationRunState::Starting => {
                        run.authorize(*proof, *observed_at)?;
                        if external_invocation_key
                            != &run.start_context.material.external_invocation_key
                        {
                            return Err(DomainError::EvidenceInvalid);
                        }
                        Ok(InvocationRunEvent::AdapterStarted {
                            external_invocation_key: external_invocation_key.clone(),
                            session_proof_digest: *session_proof_digest,
                            observed_at: *observed_at,
                        })
                    }
                    InvocationRunCommand::BeginReconciliation {
                        reason_code,
                        started_at,
                        ..
                    } if matches!(
                        run.state,
                        InvocationRunState::Starting | InvocationRunState::Running
                    ) && valid_reason(reason_code) =>
                    {
                        run.validate_time(*started_at)?;
                        Ok(InvocationRunEvent::ReconciliationStarted {
                            reason_code: reason_code.clone(),
                            started_at: *started_at,
                        })
                    }
                    InvocationRunCommand::Complete {
                        proof,
                        outcome,
                        outcome_digest,
                        output_capsule,
                        usage,
                        budget_settled,
                        completed_at,
                        ..
                    } if matches!(
                        run.state,
                        InvocationRunState::Running | InvocationRunState::Reconciling
                    ) && *budget_settled
                        && *outcome != InvocationOutcome::OutcomeUnknown =>
                    {
                        run.authorize(*proof, *completed_at)?;
                        run.validate_time(*completed_at)?;
                        Ok(InvocationRunEvent::Completed {
                            proof: *proof,
                            outcome: *outcome,
                            outcome_digest: *outcome_digest,
                            output_capsule: *output_capsule,
                            usage: *usage,
                            budget_settled: *budget_settled,
                            completed_at: *completed_at,
                        })
                    }
                    InvocationRunCommand::Complete {
                        outcome: InvocationOutcome::OutcomeUnknown,
                        ..
                    } => Err(DomainError::InvocationOutcomeUnknown),
                    InvocationRunCommand::Fail {
                        reason_code,
                        evidence_digest,
                        budget_settled,
                        failed_at,
                        ..
                    } if valid_reason(reason_code) && *budget_settled => {
                        run.validate_time(*failed_at)?;
                        Ok(InvocationRunEvent::Failed {
                            reason_code: reason_code.clone(),
                            evidence_digest: *evidence_digest,
                            budget_settled: *budget_settled,
                            failed_at: *failed_at,
                        })
                    }
                    InvocationRunCommand::Cancel {
                        reason_code,
                        stop_or_reconciliation_obligation_created,
                        cancelled_at,
                        ..
                    } if valid_reason(reason_code)
                        && *stop_or_reconciliation_obligation_created =>
                    {
                        run.validate_time(*cancelled_at)?;
                        Ok(InvocationRunEvent::Cancelled {
                            reason_code: reason_code.clone(),
                            stop_or_reconciliation_obligation_created:
                                *stop_or_reconciliation_obligation_created,
                            cancelled_at: *cancelled_at,
                        })
                    }
                    _ => Err(invalid_run(run.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &InvocationRunEvent,
    ) -> Result<Self, DomainError> {
        match (current, event) {
            (None, InvocationRunEvent::Reserved(reserve)) => {
                reserve.validate()?;
                Ok(Self {
                    id: reserve.id,
                    start_context: reserve.start_context.clone(),
                    state: InvocationRunState::Reserved,
                    current_claim: reserve.run_claim.clone(),
                    outbox_dispatch_id: None,
                    session_proof_digest: None,
                    output_capsule: None,
                    outcome: None,
                    outcome_digest: None,
                    usage: None,
                    terminal_reason: None,
                    terminal_evidence_digest: None,
                    reserved_at: reserve.reserved_at,
                    started_at: None,
                    terminal_at: None,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "invocation_run",
            }),
            (Some(run), _) if run.state.is_terminal() => Err(invalid_run_event(run.state, event)),
            (
                Some(run),
                InvocationRunEvent::DispatchStarted {
                    outbox_dispatch_id,
                    started_at,
                },
            ) if run.state == InvocationRunState::Reserved
                && *started_at >= run.reserved_at
                && *started_at < run.current_claim.expires_at =>
            {
                let mut next = run.with_state(InvocationRunState::Starting)?;
                next.outbox_dispatch_id = Some(outbox_dispatch_id.clone());
                next.started_at = Some(*started_at);
                Ok(next)
            }
            (
                Some(run),
                InvocationRunEvent::AdapterStarted {
                    external_invocation_key,
                    session_proof_digest,
                    observed_at,
                },
            ) if run.state == InvocationRunState::Starting
                && external_invocation_key
                    == &run.start_context.material.external_invocation_key
                && run.time_valid(*observed_at) =>
            {
                let mut next = run.with_state(InvocationRunState::Running)?;
                next.session_proof_digest = Some(*session_proof_digest);
                Ok(next)
            }
            (
                Some(run),
                InvocationRunEvent::ReconciliationStarted {
                    reason_code,
                    started_at,
                },
            ) if matches!(
                run.state,
                InvocationRunState::Starting | InvocationRunState::Running
            ) && valid_reason(reason_code)
                && run.time_valid(*started_at) =>
            {
                run.with_state(InvocationRunState::Reconciling)
            }
            (
                Some(run),
                InvocationRunEvent::Completed {
                    proof,
                    outcome,
                    outcome_digest,
                    output_capsule,
                    usage,
                    budget_settled,
                    completed_at,
                },
            ) if matches!(
                run.state,
                InvocationRunState::Running | InvocationRunState::Reconciling
            ) && *outcome != InvocationOutcome::OutcomeUnknown
                && *budget_settled
                && run.time_valid(*completed_at) =>
            {
                run.authorize(*proof, *completed_at)?;
                let mut next = run.terminalize(InvocationRunState::Completed, *completed_at)?;
                next.outcome = Some(*outcome);
                next.outcome_digest = Some(*outcome_digest);
                next.output_capsule = *output_capsule;
                next.usage = Some(*usage);
                Ok(next)
            }
            (
                Some(run),
                InvocationRunEvent::Failed {
                    reason_code,
                    evidence_digest,
                    budget_settled,
                    failed_at,
                },
            ) if valid_reason(reason_code) && *budget_settled && run.time_valid(*failed_at) => {
                let mut next = run.terminalize(InvocationRunState::Failed, *failed_at)?;
                next.terminal_reason = Some(reason_code.clone());
                next.terminal_evidence_digest = Some(*evidence_digest);
                Ok(next)
            }
            (
                Some(run),
                InvocationRunEvent::Cancelled {
                    reason_code,
                    stop_or_reconciliation_obligation_created,
                    cancelled_at,
                },
            ) if valid_reason(reason_code)
                && *stop_or_reconciliation_obligation_created
                && run.time_valid(*cancelled_at) =>
            {
                let mut next = run.terminalize(InvocationRunState::Cancelled, *cancelled_at)?;
                next.terminal_reason = Some(reason_code.clone());
                next.outcome = Some(InvocationOutcome::Cancelled);
                Ok(next)
            }
            (Some(run), _) => Err(invalid_run_event(run.state, event)),
        }
    }

    pub fn replay(events: &[InvocationRunEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "invocation_run",
        })
    }

    fn authorize_identity(&self, proof: RunClaimProof) -> Result<(), DomainError> {
        if proof.claim_id != self.current_claim.id
            || proof.claim_generation != self.current_claim.claim_generation
            || proof.holder_node_id != self.current_claim.holder_node_id
            || self.current_claim.state != RunClaimState::Active
        {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }

    fn authorize(&self, proof: RunClaimProof, now: ServerInstant) -> Result<(), DomainError> {
        self.authorize_identity(proof)?;
        if now >= self.current_claim.expires_at {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }

    fn validate_time(&self, time: ServerInstant) -> Result<(), DomainError> {
        if self.time_valid(time) {
            Ok(())
        } else {
            Err(DomainError::InvalidArgument {
                field: "occurred_at".into(),
                reason: "must not precede run start".into(),
            })
        }
    }

    fn time_valid(&self, time: ServerInstant) -> bool {
        time >= self.started_at.unwrap_or(self.reserved_at)
    }

    fn with_state(&self, state: InvocationRunState) -> Result<Self, DomainError> {
        let mut next = self.clone();
        next.state = state;
        next.version = self.version.checked_next()?;
        Ok(next)
    }

    fn terminalize(
        &self,
        state: InvocationRunState,
        terminal_at: ServerInstant,
    ) -> Result<Self, DomainError> {
        if !state.is_terminal() {
            return Err(DomainError::InvariantViolation {
                invariant: "invocation_run_terminal_state_required",
            });
        }
        let mut next = self.with_state(state)?;
        next.current_claim.state = RunClaimState::Completed;
        next.terminal_at = Some(terminal_at);
        Ok(next)
    }
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= 96
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn invalid_intent(
    state: InvocationIntentState,
    command: InvocationIntentCommandKind,
) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_intent_event(
    state: InvocationIntentState,
    event: &InvocationIntentEvent,
) -> DomainError {
    let command = match event {
        InvocationIntentEvent::Created(_) => "created",
        InvocationIntentEvent::EquivalentSignalAttached { .. } => "equivalent_signal_attached",
        InvocationIntentEvent::Claimed { .. } => "claimed",
        InvocationIntentEvent::ClaimExpired => "claim_expired",
        InvocationIntentEvent::Dispatched { .. } => "dispatched",
        InvocationIntentEvent::Satisfied => "satisfied",
        InvocationIntentEvent::Cancelled { .. } => "cancelled",
        InvocationIntentEvent::DeadLettered { .. } => "dead_lettered",
    };
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.into(),
    }
}

fn invalid_run(state: InvocationRunState, command: InvocationRunCommandKind) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_run_event(state: InvocationRunState, event: &InvocationRunEvent) -> DomainError {
    let command = match event {
        InvocationRunEvent::Reserved(_) => "reserved",
        InvocationRunEvent::DispatchStarted { .. } => "dispatch_started",
        InvocationRunEvent::AdapterStarted { .. } => "adapter_started",
        InvocationRunEvent::ReconciliationStarted { .. } => "reconciliation_started",
        InvocationRunEvent::Completed { .. } => "completed",
        InvocationRunEvent::Failed { .. } => "failed",
        InvocationRunEvent::Cancelled { .. } => "cancelled",
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
    use crate::ids::{
        AttemptId, FencingToken, GitObjectId, PackageId, PackageRevisionId, PolicyRevisionId,
    };

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(seconds: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-08 00:00 UTC) + Duration::seconds(seconds))
    }

    fn binding() -> InvocationBinding {
        let attempt: AttemptId = id(1);
        InvocationBinding {
            package_id: Some(PackageId::from_uuid(Uuid::from_bytes([2; 16]))),
            package_revision_id: Some(PackageRevisionId::from_uuid(Uuid::from_bytes([3; 16]))),
            package_hash: Some(Sha256Digest::of_bytes(b"package")),
            attempt_id: Some(attempt),
            author_fencing_token: Some(FencingToken::new(9).expect("author fence")),
            workspace_head: Some(GitObjectId::new("ab".repeat(20)).expect("oid")),
            policy_revision_id: PolicyRevisionId::from_uuid(Uuid::from_bytes([4; 16])),
            executor_fingerprint: Some(Sha256Digest::of_bytes(b"executor")),
            input_capsule_id: None,
            input_capsule_digest: None,
        }
    }

    fn start_context() -> InvocationStartContext {
        InvocationStartContext::new(InvocationStartMaterial {
            intent_id: id(5),
            intent_version: AggregateVersion::new(3),
            project_id: id(6),
            subject: InvocationSubject::Attempt(id(1)),
            binding: binding(),
            signal_ids: vec![id(7)],
            adapter_id: ProtocolKey::new("jcode").expect("key"),
            executor_id: id(8),
            executor_fingerprint: Sha256Digest::of_bytes(b"executor"),
            routing_decision_id: Some(id(9)),
            input_capsule: None,
            budget_reservation_id: id(10),
            author_lease_id: Some(LeaseId::from_uuid(Uuid::from_bytes([11; 16]))),
            run_claim_id: id(12),
            run_claim_generation: RunClaimToken::new(1).expect("generation"),
            external_invocation_key: ProtocolKey::new("run-external-1").expect("key"),
        })
        .expect("context")
    }

    fn reserve() -> ReserveInvocationRun {
        ReserveInvocationRun {
            id: id(13),
            start_context: start_context(),
            run_claim: RunClaim {
                id: id(12),
                run_id: id(13),
                claim_generation: RunClaimToken::new(1).expect("generation"),
                holder_node_id: id(14),
                state: RunClaimState::Active,
                granted_at: at(0),
                expires_at: at(100),
            },
            reserved_at: at(0),
        }
    }

    fn proof() -> RunClaimProof {
        RunClaimProof {
            claim_id: id(12),
            claim_generation: RunClaimToken::new(1).expect("generation"),
            holder_node_id: id(14),
        }
    }

    fn running_run_with_events() -> (InvocationRun, Vec<InvocationRunEvent>) {
        let reserved =
            InvocationRun::transition(None, &InvocationRunCommand::Reserve(Box::new(reserve())))
                .expect("reserve");
        let starting = InvocationRun::transition(
            Some(&reserved.aggregate),
            &InvocationRunCommand::MarkDispatchStarted {
                expected_version: reserved.aggregate.version,
                proof: proof(),
                outbox_dispatch_id: ProtocolKey::new("dispatch-malicious-test").expect("key"),
                started_at: at(1),
            },
        )
        .expect("starting");
        let running = InvocationRun::transition(
            Some(&starting.aggregate),
            &InvocationRunCommand::ObserveAdapterStarted {
                expected_version: starting.aggregate.version,
                proof: proof(),
                external_invocation_key: ProtocolKey::new("run-external-1").expect("key"),
                session_proof_digest: Sha256Digest::of_bytes(b"session"),
                observed_at: at(2),
            },
        )
        .expect("running");
        (
            running.aggregate,
            vec![
                reserved.events[0].clone(),
                starting.events[0].clone(),
                running.events[0].clone(),
            ],
        )
    }

    fn intent_signal() -> InvocationSignalRef {
        InvocationSignalRef {
            id: id(20),
            project_id: id(6),
            subject: InvocationSubject::Attempt(id(1)),
            binding: binding(),
            dedup_digest: Sha256Digest::of_bytes(b"dedup"),
            not_before: at(0),
        }
    }

    #[test]
    fn intent_claim_expiry_dispatch_and_satisfaction_are_replayable() {
        let created = InvocationIntent::transition(
            None,
            &InvocationIntentCommand::Create(CreateInvocationIntent {
                id: id(21),
                signal: intent_signal(),
                created_at: at(0),
            }),
        )
        .expect("create");
        let claimed_once = InvocationIntent::transition(
            Some(&created.aggregate),
            &InvocationIntentCommand::Claim {
                expected_version: created.aggregate.version,
                claim: IntentClaim {
                    token: IntentClaimToken::new(1).expect("token"),
                    claimed_by: id(22),
                    claimed_at: at(0),
                    claim_until: at(2),
                },
                now: at(0),
            },
        )
        .expect("claim");
        let retried = InvocationIntent::transition(
            Some(&claimed_once.aggregate),
            &InvocationIntentCommand::RetryExpiredClaim {
                expected_version: claimed_once.aggregate.version,
                now: at(2),
            },
        )
        .expect("retry");
        let claimed_twice = InvocationIntent::transition(
            Some(&retried.aggregate),
            &InvocationIntentCommand::Claim {
                expected_version: retried.aggregate.version,
                claim: IntentClaim {
                    token: IntentClaimToken::new(2).expect("token"),
                    claimed_by: id(23),
                    claimed_at: at(3),
                    claim_until: at(10),
                },
                now: at(3),
            },
        )
        .expect("reclaim");
        let dispatched = InvocationIntent::transition(
            Some(&claimed_twice.aggregate),
            &InvocationIntentCommand::Dispatch {
                expected_version: claimed_twice.aggregate.version,
                claim_token: IntentClaimToken::new(2).expect("token"),
                invocation_run_id: id(13),
                binding_is_current: true,
                deadline_not_elapsed: true,
                run_claim_and_budget_reserved: true,
            },
        )
        .expect("dispatch");
        let satisfied = InvocationIntent::transition(
            Some(&dispatched.aggregate),
            &InvocationIntentCommand::Satisfy {
                expected_version: dispatched.aggregate.version,
                invocation_run_id: id(13),
                run_is_terminal: true,
                outcome_registered: true,
            },
        )
        .expect("satisfy");
        let events = [
            created.events[0].clone(),
            claimed_once.events[0].clone(),
            retried.events[0].clone(),
            claimed_twice.events[0].clone(),
            dispatched.events[0].clone(),
            satisfied.events[0].clone(),
        ];
        assert_eq!(
            InvocationIntent::replay(&events).expect("replay"),
            satisfied.aggregate
        );
        assert_eq!(satisfied.aggregate.state, InvocationIntentState::Satisfied);
        assert!(matches!(
            InvocationIntent::decide(
                Some(&satisfied.aggregate),
                &InvocationIntentCommand::Cancel {
                    expected_version: AggregateVersion::new(1),
                    reason_code: "late".into(),
                }
            ),
            Err(DomainError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn run_follows_documented_states_and_preserves_start_context() {
        let reserved =
            InvocationRun::transition(None, &InvocationRunCommand::Reserve(Box::new(reserve())))
                .expect("reserve");
        let context = reserved.aggregate.start_context.clone();
        let starting = InvocationRun::transition(
            Some(&reserved.aggregate),
            &InvocationRunCommand::MarkDispatchStarted {
                expected_version: reserved.aggregate.version,
                proof: proof(),
                outbox_dispatch_id: ProtocolKey::new("dispatch-1").expect("key"),
                started_at: at(1),
            },
        )
        .expect("starting");
        let running = InvocationRun::transition(
            Some(&starting.aggregate),
            &InvocationRunCommand::ObserveAdapterStarted {
                expected_version: starting.aggregate.version,
                proof: proof(),
                external_invocation_key: ProtocolKey::new("run-external-1").expect("key"),
                session_proof_digest: Sha256Digest::of_bytes(b"session"),
                observed_at: at(2),
            },
        )
        .expect("running");
        let completed = InvocationRun::transition(
            Some(&running.aggregate),
            &InvocationRunCommand::Complete {
                expected_version: running.aggregate.version,
                proof: proof(),
                outcome: InvocationOutcome::Progressed,
                outcome_digest: Sha256Digest::of_bytes(b"outcome"),
                output_capsule: None,
                usage: TokenUsage {
                    input_tokens: 10,
                    output_tokens: 20,
                },
                budget_settled: true,
                completed_at: at(3),
            },
        )
        .expect("complete");
        assert_eq!(completed.aggregate.state, InvocationRunState::Completed);
        assert_eq!(completed.aggregate.start_context, context);
        let events = [
            reserved.events[0].clone(),
            starting.events[0].clone(),
            running.events[0].clone(),
            completed.events[0].clone(),
        ];
        assert_eq!(
            InvocationRun::replay(&events).expect("replay"),
            completed.aggregate
        );
    }

    #[test]
    fn task_lease_and_run_claim_are_independent_and_terminal_run_never_recovers() {
        let reserved =
            InvocationRun::transition(None, &InvocationRunCommand::Reserve(Box::new(reserve())))
                .expect("reserve");
        assert_ne!(
            reserved
                .aggregate
                .start_context
                .material
                .author_lease_id
                .map(|id| id.into_uuid()),
            Some(reserved.aggregate.current_claim.id.into_uuid())
        );
        let cancelled = InvocationRun::transition(
            Some(&reserved.aggregate),
            &InvocationRunCommand::Cancel {
                expected_version: reserved.aggregate.version,
                reason_code: "operator_cancel".into(),
                stop_or_reconciliation_obligation_created: true,
                cancelled_at: at(1),
            },
        )
        .expect("cancel")
        .aggregate;
        assert!(matches!(
            InvocationRun::decide(
                Some(&cancelled),
                &InvocationRunCommand::Fail {
                    expected_version: AggregateVersion::new(1),
                    reason_code: "late".into(),
                    evidence_digest: Sha256Digest::of_bytes(b"late"),
                    budget_settled: true,
                    failed_at: at(2),
                }
            ),
            Err(DomainError::InvocationRunTerminal)
        ));
    }

    #[test]
    fn malicious_terminal_events_cannot_bypass_claim_budget_or_stop_obligation() {
        let (running, prefix) = running_run_with_events();
        let wrong_claim = RunClaimProof {
            holder_node_id: id(99),
            ..proof()
        };
        let completed_with_stale_claim = InvocationRunEvent::Completed {
            proof: wrong_claim,
            outcome: InvocationOutcome::Progressed,
            outcome_digest: Sha256Digest::of_bytes(b"outcome"),
            output_capsule: None,
            usage: TokenUsage::default(),
            budget_settled: true,
            completed_at: at(3),
        };
        assert_eq!(
            InvocationRun::apply_event(Some(&running), &completed_with_stale_claim),
            Err(DomainError::InvocationClaimStale)
        );
        let mut malicious_replay = prefix.clone();
        malicious_replay.push(completed_with_stale_claim);
        assert_eq!(
            InvocationRun::replay(&malicious_replay),
            Err(DomainError::InvocationClaimStale)
        );

        let completed_without_settlement = InvocationRunEvent::Completed {
            proof: proof(),
            outcome: InvocationOutcome::Progressed,
            outcome_digest: Sha256Digest::of_bytes(b"outcome"),
            output_capsule: None,
            usage: TokenUsage::default(),
            budget_settled: false,
            completed_at: at(3),
        };
        assert!(InvocationRun::apply_event(Some(&running), &completed_without_settlement).is_err());
        let mut malicious_replay = prefix.clone();
        malicious_replay.push(completed_without_settlement);
        assert!(InvocationRun::replay(&malicious_replay).is_err());

        let failed_without_settlement = InvocationRunEvent::Failed {
            reason_code: "adapter_failed".into(),
            evidence_digest: Sha256Digest::of_bytes(b"evidence"),
            budget_settled: false,
            failed_at: at(3),
        };
        assert!(InvocationRun::apply_event(Some(&running), &failed_without_settlement).is_err());
        let mut malicious_replay = prefix.clone();
        malicious_replay.push(failed_without_settlement);
        assert!(InvocationRun::replay(&malicious_replay).is_err());

        let cancelled_without_obligation = InvocationRunEvent::Cancelled {
            reason_code: "operator_cancel".into(),
            stop_or_reconciliation_obligation_created: false,
            cancelled_at: at(3),
        };
        assert!(InvocationRun::apply_event(Some(&running), &cancelled_without_obligation).is_err());
        let mut malicious_replay = prefix;
        malicious_replay.push(cancelled_without_obligation);
        assert!(InvocationRun::replay(&malicious_replay).is_err());
    }

    #[test]
    fn tampered_start_context_is_rejected() {
        let mut context = start_context();
        context.material.adapter_id = ProtocolKey::new("other").expect("key");
        assert!(matches!(
            context.validate(),
            Err(DomainError::InvariantViolation { .. })
        ));
    }
}
