//! Invocation intent scheduling and immutable activation windows.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    event::{AggregateId, AggregateType, EventEnvelope, LEGACY_EVENT_ENVELOPE_VERSION},
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

// Preserve the established public import path while ownership lives in the
// independent run_claim module.
pub use crate::state::run_claim::{
    GrantRunClaim, RunClaim, RunClaimBinding, RunClaimCommand, RunClaimEvent, RunClaimProof,
    RunClaimState, RunClaimTakeoverAuthorization, SchedulerPreemptionReason,
    VerifiedRunClaimHistory,
};

pub const INVOCATION_RUN_EVENT_SCHEMA_VERSION: u16 = 2;
pub const INVOCATION_RUN_LEGACY_SCHEMA_VERSION: u16 = 1;

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

/// Authorization for a non-governance cancellation.
///
/// A dedicated wrapper keeps cancellation authorization explicit in commands
/// and durable events. Governance-authorized cancellation can be added as a
/// separate variant once the aggregate can verify a governance execution
/// claim; until then, a caller must hold the current invocation run claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CancellationAuthorization {
    pub current_claim: RunClaimProof,
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
    pub run_claim_binding: RunClaimBinding,
    pub reserved_at: ServerInstant,
}

/// Explicit decoder target for schema-v1 reservation payloads that embedded a
/// mutable claim snapshot. Upcasting never reuses the v1 payload digest as a
/// v2 digest; persistence must emit a new envelope with schema version 2.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReserveInvocationRunV1 {
    pub id: InvocationRunId,
    pub start_context: InvocationStartContext,
    pub run_claim: EmbeddedRunClaimV1,
    pub reserved_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EmbeddedRunClaimV1 {
    pub id: RunClaimId,
    pub run_id: InvocationRunId,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
    pub state: RunClaimState,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
}

impl TryFrom<ReserveInvocationRunV1> for ReserveInvocationRun {
    type Error = DomainError;

    fn try_from(legacy: ReserveInvocationRunV1) -> Result<Self, Self::Error> {
        if legacy.run_claim.run_id != legacy.id
            || legacy.run_claim.state != RunClaimState::Active
            || legacy.run_claim.claim_generation.get() != 1
            || legacy.reserved_at < legacy.run_claim.granted_at
            || legacy.reserved_at >= legacy.run_claim.expires_at
        {
            return Err(DomainError::InvocationClaimStale);
        }
        let upcast = Self {
            id: legacy.id,
            start_context: legacy.start_context,
            run_claim_binding: RunClaimBinding {
                claim_id: legacy.run_claim.id,
                claim_generation: legacy.run_claim.claim_generation,
                holder_node_id: legacy.run_claim.holder_node_id,
            },
            reserved_at: legacy.reserved_at,
        };
        upcast.validate()?;
        Ok(upcast)
    }
}

impl ReserveInvocationRun {
    fn validate(&self) -> Result<(), DomainError> {
        self.start_context.validate()?;
        self.run_claim_binding.validate()?;
        if self.run_claim_binding.claim_generation.get() != 1
            || self.run_claim_binding.claim_id != self.start_context.material.run_claim_id
            || self.run_claim_binding.claim_generation
                != self.start_context.material.run_claim_generation
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
        proof: RunClaimProof,
        reason_code: String,
        evidence_digest: Sha256Digest,
        budget_settled: bool,
        failed_at: ServerInstant,
    },
    Cancel {
        expected_version: AggregateVersion,
        authorization: CancellationAuthorization,
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
        proof: RunClaimProof,
        outbox_dispatch_id: ProtocolKey,
        started_at: ServerInstant,
    },
    AdapterStarted {
        proof: RunClaimProof,
        external_invocation_key: ProtocolKey,
        session_proof_digest: Sha256Digest,
        observed_at: ServerInstant,
    },
    ReconciliationStarted {
        reason_code: String,
        started_at: ServerInstant,
    },
    ClaimAdvanced {
        previous_claim: RunClaimBinding,
        current_claim: RunClaimBinding,
        advanced_at: ServerInstant,
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
        proof: RunClaimProof,
        reason_code: String,
        evidence_digest: Sha256Digest,
        budget_settled: bool,
        failed_at: ServerInstant,
    },
    Cancelled {
        authorization: CancellationAuthorization,
        reason_code: String,
        stop_or_reconciliation_obligation_created: bool,
        cancelled_at: ServerInstant,
    },
}

/// Exact `origin/main@58ebbb6` schema-v1 decoder target. Main v1 embedded the
/// mutable claim in the reservation event and omitted claim proof fields from
/// dispatch and adapter observations. The unpublished `4cba219`/`0786513`
/// preview also called itself v1 but used `run_claim_binding` plus proof fields;
/// that distinct wire shape is intentionally rejected rather than silently
/// reinterpreted as main v1.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum InvocationRunEventV1 {
    Reserved(Box<ReserveInvocationRunV1>),
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
        proof: RunClaimProof,
        reason_code: String,
        evidence_digest: Sha256Digest,
        budget_settled: bool,
        failed_at: ServerInstant,
    },
    Cancelled {
        authorization: CancellationAuthorization,
        reason_code: String,
        stop_or_reconciliation_obligation_created: bool,
        cancelled_at: ServerInstant,
    },
}

/// A verified legacy audit envelope paired with its in-memory v2 event. The
/// legacy envelope and digest are retained verbatim; upcasting never creates a
/// replacement envelope or rewrites historical audit identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpcastInvocationRunEventV1 {
    legacy_envelope: EventEnvelope<InvocationRunEventV1>,
    event: InvocationRunEvent,
}

impl UpcastInvocationRunEventV1 {
    #[must_use]
    pub const fn legacy_envelope(&self) -> &EventEnvelope<InvocationRunEventV1> {
        &self.legacy_envelope
    }

    #[must_use]
    pub const fn event(&self) -> &InvocationRunEvent {
        &self.event
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedInvocationRunV1Replay {
    run: InvocationRun,
    current_claim: RunClaim,
    events: Vec<UpcastInvocationRunEventV1>,
}

impl AuthorizedInvocationRunV1Replay {
    #[must_use]
    pub const fn run(&self) -> &InvocationRun {
        &self.run
    }

    #[must_use]
    pub fn events(&self) -> &[UpcastInvocationRunEventV1] {
        &self.events
    }
}

impl InvocationRunEvent {
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        INVOCATION_RUN_EVENT_SCHEMA_VERSION
    }
}

/// Authoritative runs cannot be deserialized directly; persistence must decode
/// an untrusted snapshot DTO and call [`InvocationRun::restore_snapshot`] with
/// a verified claim history.
///
/// ```compile_fail
/// let _: agentforge_domain::InvocationRun = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InvocationRun {
    id: InvocationRunId,
    /// Creation-time binding; no later event can replace it.
    start_context: InvocationStartContext,
    state: InvocationRunState,
    /// Immutable identity only. Liveness and expiry live in RunClaim.
    run_claim_binding: RunClaimBinding,
    /// Current authority head. Schema-v1 snapshots must use the explicit
    /// `InvocationRunSnapshotV1::upcast` boundary to populate this field.
    current_claim_head: RunClaimBinding,
    outbox_dispatch_id: Option<ProtocolKey>,
    session_proof_digest: Option<Sha256Digest>,
    output_capsule: Option<SessionCapsuleRef>,
    outcome: Option<InvocationOutcome>,
    outcome_digest: Option<Sha256Digest>,
    usage: Option<TokenUsage>,
    terminal_reason: Option<String>,
    terminal_evidence_digest: Option<Sha256Digest>,
    reserved_at: ServerInstant,
    updated_at: ServerInstant,
    started_at: Option<ServerInstant>,
    adapter_started_at: Option<ServerInstant>,
    reconciliation_started_at: Option<ServerInstant>,
    terminal_at: Option<ServerInstant>,
    version: AggregateVersion,
}

/// Current, schema-v2 persistence snapshot. Constructing an authoritative run
/// from this DTO requires a fully validated claim history; the embedded heads
/// are identity references and never self-certify execution authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationRunSnapshotV2 {
    pub id: InvocationRunId,
    pub start_context: InvocationStartContext,
    pub state: InvocationRunState,
    #[serde(alias = "run_claim_binding")]
    pub initial_claim_binding: RunClaimBinding,
    pub current_claim_head: RunClaimBinding,
    pub outbox_dispatch_id: Option<ProtocolKey>,
    pub session_proof_digest: Option<Sha256Digest>,
    pub output_capsule: Option<SessionCapsuleRef>,
    pub outcome: Option<InvocationOutcome>,
    pub outcome_digest: Option<Sha256Digest>,
    pub usage: Option<TokenUsage>,
    pub terminal_reason: Option<String>,
    pub terminal_evidence_digest: Option<Sha256Digest>,
    pub reserved_at: ServerInstant,
    pub updated_at: ServerInstant,
    pub started_at: Option<ServerInstant>,
    pub adapter_started_at: Option<ServerInstant>,
    pub reconciliation_started_at: Option<ServerInstant>,
    pub terminal_at: Option<ServerInstant>,
    pub version: AggregateVersion,
}

/// Explicit schema-v1 snapshot decoder. Migration must supply the separately
/// materialized v2 claim head before authorization can resume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationRunSnapshotV1 {
    pub id: InvocationRunId,
    pub start_context: InvocationStartContext,
    pub state: InvocationRunState,
    pub current_claim: EmbeddedRunClaimV1,
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

impl InvocationRunSnapshotV1 {
    /// Snapshot-only recovery is deliberately limited to positions whose last
    /// event time is present in main-v1 state. Running, Reconciling and all
    /// terminal positions must use `upcast_with_verified_replay`; otherwise an
    /// adapter/reconciliation time would have to be guessed.
    pub fn upcast(self, claim: &RunClaim) -> Result<InvocationRun, DomainError> {
        self.validate_main_v1_claim(claim)?;
        if matches!(
            self.state,
            InvocationRunState::Running | InvocationRunState::Reconciling
        ) || self.state.is_terminal()
        {
            return Err(DomainError::SchemaVersionUnsupported);
        }
        let legacy_binding = RunClaimBinding {
            claim_id: self.current_claim.id,
            claim_generation: self.current_claim.claim_generation,
            holder_node_id: self.current_claim.holder_node_id,
        };
        let run = InvocationRun {
            id: self.id,
            start_context: self.start_context,
            state: self.state,
            run_claim_binding: legacy_binding,
            current_claim_head: legacy_binding,
            outbox_dispatch_id: self.outbox_dispatch_id,
            session_proof_digest: self.session_proof_digest,
            output_capsule: self.output_capsule,
            outcome: self.outcome,
            outcome_digest: self.outcome_digest,
            usage: self.usage,
            terminal_reason: self.terminal_reason,
            terminal_evidence_digest: self.terminal_evidence_digest,
            reserved_at: self.reserved_at,
            updated_at: self
                .terminal_at
                .or(self.started_at)
                .unwrap_or(self.reserved_at),
            started_at: self.started_at,
            adapter_started_at: None,
            reconciliation_started_at: None,
            terminal_at: self.terminal_at,
            version: self.version,
        };
        validate_invocation_run_snapshot(&run.snapshot())?;
        validate_snapshot_claim_heads(&run, std::slice::from_ref(claim))?;
        Ok(run)
    }

    /// Restores any main-v1 position after the original event envelopes have
    /// been digest-verified, replayed, and cross-authorized against independent
    /// claim history. Event replay supplies exact adapter/reconciliation times.
    pub fn upcast_with_verified_replay(
        self,
        replay: &AuthorizedInvocationRunV1Replay,
    ) -> Result<InvocationRun, DomainError> {
        self.validate_main_v1_claim(&replay.current_claim)?;
        let run = &replay.run;
        if self.id != run.id
            || self.start_context != run.start_context
            || self.state != run.state
            || self.outbox_dispatch_id != run.outbox_dispatch_id
            || self.session_proof_digest != run.session_proof_digest
            || self.output_capsule != run.output_capsule
            || self.outcome != run.outcome
            || self.outcome_digest != run.outcome_digest
            || self.usage != run.usage
            || self.terminal_reason != run.terminal_reason
            || self.terminal_evidence_digest != run.terminal_evidence_digest
            || self.reserved_at != run.reserved_at
            || self.started_at != run.started_at
            || self.terminal_at != run.terminal_at
            || self.version != run.version
        {
            return Err(DomainError::EvidenceInvalid);
        }
        Ok(run.clone())
    }

    fn validate_main_v1_claim(&self, claim: &RunClaim) -> Result<(), DomainError> {
        self.start_context.validate()?;
        let legacy_binding = RunClaimBinding {
            claim_id: self.current_claim.id,
            claim_generation: self.current_claim.claim_generation,
            holder_node_id: self.current_claim.holder_node_id,
        };
        let expected_claim_state = if self.state.is_terminal() {
            RunClaimState::Completed
        } else {
            RunClaimState::Active
        };
        if self.current_claim.run_id != self.id
            || self.current_claim.state != expected_claim_state
            || self.current_claim.claim_generation.get() != 1
            || legacy_binding != claim.binding()
            || claim.run_id() != self.id
            || claim.state() != expected_claim_state
            || self.current_claim.granted_at != claim.granted_at()
            || self.current_claim.expires_at != claim.expires_at()
            || (self.state.is_terminal() && claim.terminal_at() != self.terminal_at)
            || self.start_context.material.run_claim_id != legacy_binding.claim_id
            || self.start_context.material.run_claim_generation != legacy_binding.claim_generation
        {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)] // Exposed only after an atomic application UoW port exists.
pub(crate) struct InvocationRunTerminalTransition {
    run: Transition<InvocationRun, InvocationRunEvent>,
    claim: Transition<RunClaim, RunClaimEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)] // Exposed only after an atomic application UoW port exists.
pub(crate) struct InvocationRunClaimAdvanceTransition {
    run: Transition<InvocationRun, InvocationRunEvent>,
    previous_claim: RunClaim,
    previous_claim_events: Vec<RunClaimEvent>,
    current_claim: Transition<RunClaim, RunClaimEvent>,
}

impl InvocationRun {
    /// Restores a current snapshot only after validating both the run shape and
    /// the complete independent claim chain. This is the production snapshot
    /// boundary; arbitrary run/claim heads are not accepted as authority.
    pub fn restore_snapshot(
        snapshot: InvocationRunSnapshotV2,
        history: &VerifiedRunClaimHistory,
    ) -> Result<Self, DomainError> {
        validate_invocation_run_snapshot(&snapshot)?;
        let run = Self {
            id: snapshot.id,
            start_context: snapshot.start_context,
            state: snapshot.state,
            run_claim_binding: snapshot.initial_claim_binding,
            current_claim_head: snapshot.current_claim_head,
            outbox_dispatch_id: snapshot.outbox_dispatch_id,
            session_proof_digest: snapshot.session_proof_digest,
            output_capsule: snapshot.output_capsule,
            outcome: snapshot.outcome,
            outcome_digest: snapshot.outcome_digest,
            usage: snapshot.usage,
            terminal_reason: snapshot.terminal_reason,
            terminal_evidence_digest: snapshot.terminal_evidence_digest,
            reserved_at: snapshot.reserved_at,
            updated_at: snapshot.updated_at,
            started_at: snapshot.started_at,
            adapter_started_at: snapshot.adapter_started_at,
            reconciliation_started_at: snapshot.reconciliation_started_at,
            terminal_at: snapshot.terminal_at,
            version: snapshot.version,
        };
        validate_snapshot_claim_heads(&run, history.claims())?;
        Ok(run)
    }

    #[must_use]
    pub fn snapshot(&self) -> InvocationRunSnapshotV2 {
        InvocationRunSnapshotV2 {
            id: self.id,
            start_context: self.start_context.clone(),
            state: self.state,
            initial_claim_binding: self.run_claim_binding,
            current_claim_head: self.current_claim_head,
            outbox_dispatch_id: self.outbox_dispatch_id.clone(),
            session_proof_digest: self.session_proof_digest,
            output_capsule: self.output_capsule,
            outcome: self.outcome,
            outcome_digest: self.outcome_digest,
            usage: self.usage,
            terminal_reason: self.terminal_reason.clone(),
            terminal_evidence_digest: self.terminal_evidence_digest,
            reserved_at: self.reserved_at,
            updated_at: self.updated_at,
            started_at: self.started_at,
            adapter_started_at: self.adapter_started_at,
            reconciliation_started_at: self.reconciliation_started_at,
            terminal_at: self.terminal_at,
            version: self.version,
        }
    }

    #[must_use]
    pub const fn id(&self) -> InvocationRunId {
        self.id
    }

    #[must_use]
    pub const fn state(&self) -> InvocationRunState {
        self.state
    }

    #[must_use]
    pub const fn initial_claim_binding(&self) -> RunClaimBinding {
        self.run_claim_binding
    }

    #[must_use]
    pub const fn reserved_at(&self) -> ServerInstant {
        self.reserved_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> ServerInstant {
        self.updated_at
    }

    #[must_use]
    pub const fn started_at(&self) -> Option<ServerInstant> {
        self.started_at
    }

    #[must_use]
    pub const fn terminal_at(&self) -> Option<ServerInstant> {
        self.terminal_at
    }

    #[must_use]
    pub const fn version(&self) -> AggregateVersion {
        self.version
    }

    #[must_use]
    pub const fn start_context(&self) -> &InvocationStartContext {
        &self.start_context
    }

    /// Canonical reservation boundary: an embedded binding cannot self-certify
    /// a nonexistent, revoked, or expired claim.
    pub fn reserve_with_claim(
        reserve: ReserveInvocationRun,
        claim: &RunClaim,
        now: ServerInstant,
    ) -> Result<Transition<Self, InvocationRunEvent>, DomainError> {
        if now != reserve.reserved_at
            || reserve.run_claim_binding != claim.binding()
            || claim.generation().get() != 1
        {
            return Err(DomainError::InvocationClaimStale);
        }
        claim.authorize_for_run(reserve.id, claim.proof(), now)?;
        Self::transition(None, &InvocationRunCommand::Reserve(Box::new(reserve)))
    }

    /// Canonical claim-authorized nonterminal transition boundary.
    pub fn transition_with_claim(
        &self,
        claim: &RunClaim,
        command: &InvocationRunCommand,
        now: ServerInstant,
    ) -> Result<Transition<Self, InvocationRunEvent>, DomainError> {
        if !matches!(
            command,
            InvocationRunCommand::MarkDispatchStarted { .. }
                | InvocationRunCommand::ObserveAdapterStarted { .. }
        ) || claim_command_time(command) != Some(now)
        {
            return Err(invalid_run(self.state, command.kind()));
        }
        let proof = command_claim_proof(command).ok_or(DomainError::InvocationClaimStale)?;
        self.authorize_claim(claim, proof, now)?;
        Self::transition(Some(self), command)
    }

    /// Dispatcher-owned reconciliation does not borrow RunClaim authority.
    pub fn begin_reconciliation(
        &self,
        command: &InvocationRunCommand,
    ) -> Result<Transition<Self, InvocationRunEvent>, DomainError> {
        if !matches!(command, InvocationRunCommand::BeginReconciliation { .. }) {
            return Err(invalid_run(self.state, command.kind()));
        }
        Self::transition(Some(self), command)
    }

    /// Produces both terminal transitions for one application UoW. Persisting
    /// only one side is invalid; callers commit both state/event streams in the
    /// same transaction.
    #[allow(dead_code)] // Kept crate-private until an atomic persistence port exists.
    pub(crate) fn terminalize_with_claim(
        &self,
        claim: &RunClaim,
        command: &InvocationRunCommand,
        now: ServerInstant,
    ) -> Result<InvocationRunTerminalTransition, DomainError> {
        if !matches!(
            command,
            InvocationRunCommand::Complete { .. }
                | InvocationRunCommand::Fail { .. }
                | InvocationRunCommand::Cancel { .. }
        ) || claim_command_time(command) != Some(now)
        {
            return Err(invalid_run(self.state, command.kind()));
        }
        let proof = command_claim_proof(command).ok_or(DomainError::InvocationClaimStale)?;
        self.authorize_claim(claim, proof, now)?;
        let run = Self::transition(Some(self), command)?;
        let result_digest = match command {
            InvocationRunCommand::Complete { outcome_digest, .. } => Some(*outcome_digest),
            InvocationRunCommand::Fail {
                evidence_digest, ..
            } => Some(*evidence_digest),
            InvocationRunCommand::Cancel { .. } => None,
            _ => unreachable!("guarded above"),
        };
        let claim = claim.release_for_terminal(proof, result_digest, now)?;
        if !run.aggregate.state.is_terminal() || !claim.aggregate.state().is_terminal() {
            return Err(DomainError::InvariantViolation {
                invariant: "terminal_invocation_run_requires_terminal_run_claim",
            });
        }
        Ok(InvocationRunTerminalTransition { run, claim })
    }

    /// Atomically decides old-claim supersession, exact successor creation,
    /// and the run's authoritative head advance.
    #[allow(dead_code)] // Kept crate-private until an atomic persistence port exists.
    pub(crate) fn advance_claim(
        &self,
        previous_claim: &RunClaim,
        mut current_grant: GrantRunClaim,
        authorization: RunClaimTakeoverAuthorization,
        expected_run_version: AggregateVersion,
        advanced_at: ServerInstant,
    ) -> Result<InvocationRunClaimAdvanceTransition, DomainError> {
        if self.state.is_terminal() || expected_run_version != self.version {
            return Err(DomainError::InvocationClaimStale);
        }
        self.validate_time(advanced_at)?;
        if current_grant.granted_at != advanced_at
            || authorization.authorized_at() != advanced_at
            || previous_claim.run_id() != self.id
            || previous_claim.binding() != self.current_claim_binding()
        {
            return Err(DomainError::InvocationClaimStale);
        }
        validate_takeover_authorization(previous_claim, &authorization, advanced_at)?;
        current_grant.previous_generation = Some(previous_claim.generation());
        current_grant.predecessor_claim_id = Some(previous_claim.id());
        current_grant.takeover_authorization = Some(authorization);
        let current_binding = RunClaimBinding {
            claim_id: current_grant.id,
            claim_generation: current_grant.claim_generation,
            holder_node_id: current_grant.holder_node_id,
        };
        let (previous_claim_head, previous_claim_events) = match previous_claim.state() {
            RunClaimState::Active if advanced_at >= previous_claim.expires_at() => {
                let transition = previous_claim.execute(&RunClaimCommand::Expire {
                    expected_version: previous_claim.version(),
                    expired_at: advanced_at,
                })?;
                (transition.aggregate, transition.events)
            }
            RunClaimState::Active => {
                let transition =
                    previous_claim.supersede_for_successor(&RunClaimCommand::Supersede {
                        expected_version: previous_claim.version(),
                        superseding_claim_id: current_grant.id,
                        superseding_generation: current_grant.claim_generation,
                        superseded_at: advanced_at,
                    })?;
                (transition.aggregate, transition.events)
            }
            RunClaimState::Expired | RunClaimState::Revoked => (previous_claim.clone(), Vec::new()),
            RunClaimState::Completed | RunClaimState::Superseded => {
                return Err(DomainError::InvocationClaimStale);
            }
        };
        let current_claim =
            RunClaim::grant_successor_from_predecessor(&previous_claim_head, current_grant)?;
        current_claim.aggregate.authorize_for_run(
            self.id,
            current_claim.aggregate.proof(),
            advanced_at,
        )?;
        let event = InvocationRunEvent::ClaimAdvanced {
            previous_claim: previous_claim_head.binding(),
            current_claim: current_binding,
            advanced_at,
        };
        let aggregate = Self::apply_event(Some(self), &event)?;
        Ok(InvocationRunClaimAdvanceTransition {
            run: Transition::one(aggregate, event),
            previous_claim: previous_claim_head,
            previous_claim_events,
            current_claim,
        })
    }

    /// Verifies every original v1 envelope digest before converting its
    /// payload in memory. The returned objects preserve the source envelopes;
    /// callers must use `replay_main_v1_authorized` (or `replay_authorized`) before
    /// treating the resulting run as authoritative.
    pub fn upcast_main_v1_envelopes<B: AsRef<[u8]>>(
        encoded_envelopes: &[B],
    ) -> Result<Vec<UpcastInvocationRunEventV1>, DomainError> {
        let mut current = None;
        let mut expected_run_id = None;
        let mut upcast = Vec::with_capacity(encoded_envelopes.len());
        for (index, bytes) in encoded_envelopes.iter().enumerate() {
            let envelope = EventEnvelope::<InvocationRunEventV1>::from_json(bytes.as_ref())?;
            let expected_seq = u64::try_from(index)
                .map_err(|_| DomainError::Internal)?
                .checked_add(1)
                .ok_or(DomainError::Internal)?;
            if envelope.envelope_version != LEGACY_EVENT_ENVELOPE_VERSION
                || envelope.schema_version != INVOCATION_RUN_LEGACY_SCHEMA_VERSION
                || envelope.aggregate_type != AggregateType::InvocationRun
                || envelope.aggregate_seq != expected_seq
                || !envelope.required_semantics.is_empty()
            {
                return Err(DomainError::SchemaVersionUnsupported);
            }
            let aggregate_id = match envelope.aggregate_id {
                AggregateId::InvocationRun(id) => id,
                _ => return Err(DomainError::InvocationBindingMismatch),
            };
            if expected_run_id.is_some_and(|id| id != aggregate_id) {
                return Err(DomainError::InvocationBindingMismatch);
            }
            expected_run_id = Some(aggregate_id);

            let event = upcast_invocation_run_event_v1(envelope.payload.clone(), current.as_ref())?;
            if invocation_run_event_time(&event) != envelope.occurred_at {
                return Err(DomainError::EvidenceInvalid);
            }
            let next = Self::apply_event(current.as_ref(), &event)?;
            if next.id != aggregate_id || next.version != envelope.aggregate_version {
                return Err(DomainError::EvidenceInvalid);
            }
            current = Some(next);
            upcast.push(UpcastInvocationRunEventV1 {
                legacy_envelope: envelope,
                event,
            });
        }
        if upcast.is_empty() {
            return Err(DomainError::NotFound {
                resource: "invocation_run",
            });
        }
        Ok(upcast)
    }

    /// Complete historical boundary: legacy digest verification, payload
    /// upcast, run replay, and independent claim-history authorization happen
    /// as one operation.
    pub fn replay_main_v1_authorized<B: AsRef<[u8]>>(
        encoded_envelopes: &[B],
        history: &VerifiedRunClaimHistory,
    ) -> Result<AuthorizedInvocationRunV1Replay, DomainError> {
        let upcast = Self::upcast_main_v1_envelopes(encoded_envelopes)?;
        let events = upcast
            .iter()
            .map(|item| item.event.clone())
            .collect::<Vec<_>>();
        let run = Self::replay_authorized(&events, history)?;
        let current_claim = history
            .get(run.current_claim_binding().claim_id)
            .ok_or(DomainError::InvocationClaimStale)?
            .clone();
        Ok(AuthorizedInvocationRunV1Replay {
            run,
            current_claim,
            events: upcast,
        })
    }

    /// Replays the run only after all independent claim streams have been
    /// replayed and linked into a verified history.
    pub fn replay_authorized(
        events: &[InvocationRunEvent],
        history: &VerifiedRunClaimHistory,
    ) -> Result<Self, DomainError> {
        let run = Self::replay(events)?;
        validate_run_claim_replay(&run, events, history)?;
        Ok(run)
    }

    #[must_use]
    pub fn current_claim_binding(&self) -> RunClaimBinding {
        self.current_claim_head
    }

    fn transition(
        current: Option<&Self>,
        command: &InvocationRunCommand,
    ) -> Result<Transition<Self, InvocationRunEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    fn decide(
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
                        run.validate_claim_binding(*proof)?;
                        run.validate_time(*started_at)?;
                        Ok(InvocationRunEvent::DispatchStarted {
                            proof: *proof,
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
                        run.validate_claim_binding(*proof)?;
                        run.validate_time(*observed_at)?;
                        if external_invocation_key
                            != &run.start_context.material.external_invocation_key
                        {
                            return Err(DomainError::EvidenceInvalid);
                        }
                        Ok(InvocationRunEvent::AdapterStarted {
                            proof: *proof,
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
                        run.validate_claim_binding(*proof)?;
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
                        proof,
                        reason_code,
                        evidence_digest,
                        budget_settled,
                        failed_at,
                        ..
                    } if valid_reason(reason_code) && *budget_settled => {
                        run.validate_claim_binding(*proof)?;
                        run.validate_time(*failed_at)?;
                        Ok(InvocationRunEvent::Failed {
                            proof: *proof,
                            reason_code: reason_code.clone(),
                            evidence_digest: *evidence_digest,
                            budget_settled: *budget_settled,
                            failed_at: *failed_at,
                        })
                    }
                    InvocationRunCommand::Cancel {
                        authorization,
                        reason_code,
                        stop_or_reconciliation_obligation_created,
                        cancelled_at,
                        ..
                    } if valid_reason(reason_code)
                        && *stop_or_reconciliation_obligation_created =>
                    {
                        run.validate_cancellation_binding(*authorization)?;
                        run.validate_time(*cancelled_at)?;
                        Ok(InvocationRunEvent::Cancelled {
                            authorization: *authorization,
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

    fn apply_event(
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
                    run_claim_binding: reserve.run_claim_binding,
                    current_claim_head: reserve.run_claim_binding,
                    outbox_dispatch_id: None,
                    session_proof_digest: None,
                    output_capsule: None,
                    outcome: None,
                    outcome_digest: None,
                    usage: None,
                    terminal_reason: None,
                    terminal_evidence_digest: None,
                    reserved_at: reserve.reserved_at,
                    updated_at: reserve.reserved_at,
                    started_at: None,
                    adapter_started_at: None,
                    reconciliation_started_at: None,
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
                    proof,
                    outbox_dispatch_id,
                    started_at,
                },
            ) if run.state == InvocationRunState::Reserved && *started_at >= run.reserved_at => {
                run.validate_claim_binding(*proof)?;
                let mut next = run.with_state(InvocationRunState::Starting, *started_at)?;
                next.outbox_dispatch_id = Some(outbox_dispatch_id.clone());
                next.started_at = Some(*started_at);
                Ok(next)
            }
            (
                Some(run),
                InvocationRunEvent::AdapterStarted {
                    proof,
                    external_invocation_key,
                    session_proof_digest,
                    observed_at,
                },
            ) if run.state == InvocationRunState::Starting
                && external_invocation_key
                    == &run.start_context.material.external_invocation_key
                && run.time_valid(*observed_at) =>
            {
                run.validate_claim_binding(*proof)?;
                let mut next = run.with_state(InvocationRunState::Running, *observed_at)?;
                next.session_proof_digest = Some(*session_proof_digest);
                next.adapter_started_at = Some(*observed_at);
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
                let mut next = run.with_state(InvocationRunState::Reconciling, *started_at)?;
                next.reconciliation_started_at = Some(*started_at);
                Ok(next)
            }
            (
                Some(run),
                InvocationRunEvent::ClaimAdvanced {
                    previous_claim,
                    current_claim,
                    advanced_at,
                },
            ) if !run.state.is_terminal() && run.time_valid(*advanced_at) => {
                if *previous_claim != run.current_claim_binding()
                    || previous_claim.claim_id == current_claim.claim_id
                    || previous_claim.claim_generation.checked_next()?
                        != current_claim.claim_generation
                {
                    return Err(DomainError::InvocationClaimStale);
                }
                previous_claim.validate()?;
                current_claim.validate()?;
                let mut next = run.clone();
                next.current_claim_head = *current_claim;
                next.updated_at = *advanced_at;
                next.version = run.version.checked_next()?;
                Ok(next)
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
                run.validate_claim_binding(*proof)?;
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
                    proof,
                    reason_code,
                    evidence_digest,
                    budget_settled,
                    failed_at,
                },
            ) if valid_reason(reason_code) && *budget_settled && run.time_valid(*failed_at) => {
                run.validate_claim_binding(*proof)?;
                let mut next = run.terminalize(InvocationRunState::Failed, *failed_at)?;
                next.terminal_reason = Some(reason_code.clone());
                next.terminal_evidence_digest = Some(*evidence_digest);
                Ok(next)
            }
            (
                Some(run),
                InvocationRunEvent::Cancelled {
                    authorization,
                    reason_code,
                    stop_or_reconciliation_obligation_created,
                    cancelled_at,
                },
            ) if valid_reason(reason_code)
                && *stop_or_reconciliation_obligation_created
                && run.time_valid(*cancelled_at) =>
            {
                run.validate_cancellation_binding(*authorization)?;
                let mut next = run.terminalize(InvocationRunState::Cancelled, *cancelled_at)?;
                next.terminal_reason = Some(reason_code.clone());
                next.outcome = Some(InvocationOutcome::Cancelled);
                Ok(next)
            }
            (Some(run), _) => Err(invalid_run_event(run.state, event)),
        }
    }

    fn replay(events: &[InvocationRunEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "invocation_run",
        })
    }

    fn validate_claim_binding(&self, proof: RunClaimProof) -> Result<(), DomainError> {
        self.current_claim_binding().validate_proof(proof)
    }

    fn validate_cancellation_binding(
        &self,
        authorization: CancellationAuthorization,
    ) -> Result<(), DomainError> {
        self.validate_claim_binding(authorization.current_claim)
    }

    /// Cross-aggregate authorization used by application command handlers.
    ///
    /// The immutable run binding and the current independent claim must both
    /// agree. The application persists both transitions in one transaction.
    pub fn authorize_claim(
        &self,
        claim: &RunClaim,
        proof: RunClaimProof,
        now: ServerInstant,
    ) -> Result<(), DomainError> {
        self.validate_claim_binding(proof)?;
        claim.authorize_for_run(self.id, proof, now)
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
        time >= self.updated_at
    }

    fn with_state(
        &self,
        state: InvocationRunState,
        occurred_at: ServerInstant,
    ) -> Result<Self, DomainError> {
        if occurred_at < self.updated_at {
            return Err(DomainError::InvalidArgument {
                field: "occurred_at".into(),
                reason: "must not precede the previous run event".into(),
            });
        }
        let mut next = self.clone();
        next.state = state;
        next.updated_at = occurred_at;
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
        let mut next = self.with_state(state, terminal_at)?;
        next.terminal_at = Some(terminal_at);
        Ok(next)
    }
}

fn validate_invocation_run_snapshot(snapshot: &InvocationRunSnapshotV2) -> Result<(), DomainError> {
    snapshot.start_context.validate()?;
    snapshot.initial_claim_binding.validate()?;
    snapshot.current_claim_head.validate()?;
    if snapshot.id.as_uuid().is_nil()
        || snapshot.version == AggregateVersion::ZERO
        || snapshot.updated_at < snapshot.reserved_at
        || snapshot.start_context.material.run_claim_id != snapshot.initial_claim_binding.claim_id
        || snapshot.initial_claim_binding.claim_generation.get() != 1
        || snapshot.start_context.material.run_claim_generation
            != snapshot.initial_claim_binding.claim_generation
        || snapshot.current_claim_head.claim_generation
            < snapshot.initial_claim_binding.claim_generation
        || (snapshot.current_claim_head.claim_generation
            == snapshot.initial_claim_binding.claim_generation
            && snapshot.current_claim_head != snapshot.initial_claim_binding)
        || snapshot.started_at.is_some_and(|started_at| {
            started_at < snapshot.reserved_at || started_at > snapshot.updated_at
        })
        || snapshot
            .adapter_started_at
            .is_some_and(|adapter_started_at| {
                snapshot.started_at.is_none_or(|started_at| {
                    adapter_started_at < started_at || adapter_started_at > snapshot.updated_at
                })
            })
        || snapshot
            .reconciliation_started_at
            .is_some_and(|reconciliation_started_at| {
                snapshot.started_at.is_none_or(|started_at| {
                    reconciliation_started_at < started_at
                        || reconciliation_started_at > snapshot.updated_at
                }) || snapshot
                    .adapter_started_at
                    .is_some_and(|adapter_started_at| {
                        reconciliation_started_at < adapter_started_at
                    })
            })
        || snapshot.session_proof_digest.is_some() != snapshot.adapter_started_at.is_some()
        || (snapshot.outbox_dispatch_id.is_none()
            && (snapshot.started_at.is_some()
                || snapshot.adapter_started_at.is_some()
                || snapshot.reconciliation_started_at.is_some()
                || snapshot.session_proof_digest.is_some()))
        || snapshot
            .terminal_at
            .is_some_and(|terminal_at| terminal_at != snapshot.updated_at)
    {
        return Err(DomainError::InvocationClaimStale);
    }

    let no_terminal_payload = snapshot.terminal_at.is_none()
        && snapshot.output_capsule.is_none()
        && snapshot.outcome.is_none()
        && snapshot.outcome_digest.is_none()
        && snapshot.usage.is_none()
        && snapshot.terminal_reason.is_none()
        && snapshot.terminal_evidence_digest.is_none();
    let valid_state_shape = match snapshot.state {
        InvocationRunState::Reserved => {
            snapshot.outbox_dispatch_id.is_none()
                && snapshot.session_proof_digest.is_none()
                && snapshot.started_at.is_none()
                && snapshot.adapter_started_at.is_none()
                && snapshot.reconciliation_started_at.is_none()
                && no_terminal_payload
        }
        InvocationRunState::Starting => {
            snapshot.outbox_dispatch_id.is_some()
                && snapshot.session_proof_digest.is_none()
                && snapshot.started_at.is_some()
                && snapshot.adapter_started_at.is_none()
                && snapshot.reconciliation_started_at.is_none()
                && no_terminal_payload
        }
        InvocationRunState::Running => {
            snapshot.outbox_dispatch_id.is_some()
                && snapshot.started_at.is_some()
                && snapshot.adapter_started_at.is_some()
                && snapshot.reconciliation_started_at.is_none()
                && snapshot.session_proof_digest.is_some()
                && no_terminal_payload
        }
        InvocationRunState::Reconciling => {
            snapshot.outbox_dispatch_id.is_some()
                && snapshot.started_at.is_some()
                && snapshot.reconciliation_started_at.is_some()
                && no_terminal_payload
        }
        InvocationRunState::Completed => {
            snapshot.version.get() >= 4
                && snapshot.outbox_dispatch_id.is_some()
                && snapshot.started_at.is_some()
                && (snapshot.adapter_started_at.is_some()
                    || snapshot.reconciliation_started_at.is_some())
                && snapshot.terminal_at == Some(snapshot.updated_at)
                && snapshot
                    .outcome
                    .is_some_and(|outcome| outcome != InvocationOutcome::OutcomeUnknown)
                && snapshot.outcome_digest.is_some()
                && snapshot.usage.is_some()
                && snapshot.terminal_reason.is_none()
                && snapshot.terminal_evidence_digest.is_none()
        }
        InvocationRunState::Failed => {
            snapshot.version.get() >= 2
                && snapshot.terminal_at == Some(snapshot.updated_at)
                && snapshot.output_capsule.is_none()
                && snapshot.outcome.is_none()
                && snapshot.outcome_digest.is_none()
                && snapshot.usage.is_none()
                && snapshot
                    .terminal_reason
                    .as_deref()
                    .is_some_and(valid_reason)
                && snapshot.terminal_evidence_digest.is_some()
        }
        InvocationRunState::Cancelled => {
            snapshot.version.get() >= 2
                && snapshot.terminal_at == Some(snapshot.updated_at)
                && snapshot.output_capsule.is_none()
                && snapshot.outcome == Some(InvocationOutcome::Cancelled)
                && snapshot.outcome_digest.is_none()
                && snapshot.usage.is_none()
                && snapshot
                    .terminal_reason
                    .as_deref()
                    .is_some_and(valid_reason)
                && snapshot.terminal_evidence_digest.is_none()
        }
    };
    if !valid_state_shape {
        return Err(DomainError::InvariantViolation {
            invariant: "invocation_run_snapshot_state_shape_must_be_valid",
        });
    }
    Ok(())
}

fn validate_snapshot_claim_heads(
    run: &InvocationRun,
    claims: &[RunClaim],
) -> Result<(), DomainError> {
    let mut for_run = claims.iter().filter(|claim| claim.run_id() == run.id);
    let first = for_run.next().ok_or(DomainError::InvocationClaimStale)?;
    let mut lowest = first;
    let mut highest = first;
    for claim in for_run {
        if claim.generation() < lowest.generation() {
            lowest = claim;
        }
        if claim.generation() > highest.generation() {
            highest = claim;
        }
    }
    let terminal_result_digest = match run.state {
        InvocationRunState::Completed => run.outcome_digest,
        InvocationRunState::Failed => run.terminal_evidence_digest,
        InvocationRunState::Cancelled => None,
        InvocationRunState::Reserved
        | InvocationRunState::Starting
        | InvocationRunState::Running
        | InvocationRunState::Reconciling => None,
    };
    if lowest.generation().get() != 1
        || lowest.binding() != run.run_claim_binding
        || lowest.granted_at() > run.reserved_at
        || lowest
            .terminal_at()
            .is_some_and(|terminal_at| terminal_at < run.reserved_at)
        || highest.binding() != run.current_claim_head
        || highest.granted_at() > run.updated_at
        || (run.state.is_terminal()
            && (highest.state() != RunClaimState::Completed
                || highest.terminal_at() != run.terminal_at
                || highest.result_digest() != terminal_result_digest))
        || (!run.state.is_terminal() && highest.state() != RunClaimState::Active)
    {
        return Err(DomainError::InvocationClaimStale);
    }
    Ok(())
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= 96
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn claim_command_time(command: &InvocationRunCommand) -> Option<ServerInstant> {
    match command {
        InvocationRunCommand::MarkDispatchStarted { started_at, .. } => Some(*started_at),
        InvocationRunCommand::ObserveAdapterStarted { observed_at, .. } => Some(*observed_at),
        InvocationRunCommand::Complete { completed_at, .. } => Some(*completed_at),
        InvocationRunCommand::Fail { failed_at, .. } => Some(*failed_at),
        InvocationRunCommand::Cancel { cancelled_at, .. } => Some(*cancelled_at),
        InvocationRunCommand::Reserve(_) | InvocationRunCommand::BeginReconciliation { .. } => None,
    }
}

fn upcast_invocation_run_event_v1(
    event: InvocationRunEventV1,
    current: Option<&InvocationRun>,
) -> Result<InvocationRunEvent, DomainError> {
    let current_proof = || {
        current
            .map(InvocationRun::current_claim_binding)
            .map(|binding| RunClaimProof {
                claim_id: binding.claim_id,
                claim_generation: binding.claim_generation,
                holder_node_id: binding.holder_node_id,
            })
            .ok_or(DomainError::InvocationClaimStale)
    };
    match event {
        InvocationRunEventV1::Reserved(reserve) => Ok(InvocationRunEvent::Reserved(Box::new(
            ReserveInvocationRun::try_from(*reserve)?,
        ))),
        InvocationRunEventV1::DispatchStarted {
            outbox_dispatch_id,
            started_at,
        } => Ok(InvocationRunEvent::DispatchStarted {
            proof: current_proof()?,
            outbox_dispatch_id,
            started_at,
        }),
        InvocationRunEventV1::AdapterStarted {
            external_invocation_key,
            session_proof_digest,
            observed_at,
        } => Ok(InvocationRunEvent::AdapterStarted {
            proof: current_proof()?,
            external_invocation_key,
            session_proof_digest,
            observed_at,
        }),
        InvocationRunEventV1::ReconciliationStarted {
            reason_code,
            started_at,
        } => Ok(InvocationRunEvent::ReconciliationStarted {
            reason_code,
            started_at,
        }),
        InvocationRunEventV1::Completed {
            proof,
            outcome,
            outcome_digest,
            output_capsule,
            usage,
            budget_settled,
            completed_at,
        } => Ok(InvocationRunEvent::Completed {
            proof,
            outcome,
            outcome_digest,
            output_capsule,
            usage,
            budget_settled,
            completed_at,
        }),
        InvocationRunEventV1::Failed {
            proof,
            reason_code,
            evidence_digest,
            budget_settled,
            failed_at,
        } => Ok(InvocationRunEvent::Failed {
            proof,
            reason_code,
            evidence_digest,
            budget_settled,
            failed_at,
        }),
        InvocationRunEventV1::Cancelled {
            authorization,
            reason_code,
            stop_or_reconciliation_obligation_created,
            cancelled_at,
        } => Ok(InvocationRunEvent::Cancelled {
            authorization,
            reason_code,
            stop_or_reconciliation_obligation_created,
            cancelled_at,
        }),
    }
}

const fn invocation_run_event_time(event: &InvocationRunEvent) -> ServerInstant {
    match event {
        InvocationRunEvent::Reserved(reserve) => reserve.reserved_at,
        InvocationRunEvent::DispatchStarted { started_at, .. }
        | InvocationRunEvent::ReconciliationStarted { started_at, .. } => *started_at,
        InvocationRunEvent::AdapterStarted { observed_at, .. } => *observed_at,
        InvocationRunEvent::ClaimAdvanced { advanced_at, .. } => *advanced_at,
        InvocationRunEvent::Completed { completed_at, .. } => *completed_at,
        InvocationRunEvent::Failed { failed_at, .. } => *failed_at,
        InvocationRunEvent::Cancelled { cancelled_at, .. } => *cancelled_at,
    }
}

#[allow(dead_code)] // Used only by the crate-private atomic takeover decision.
fn validate_takeover_authorization(
    claim: &RunClaim,
    authorization: &RunClaimTakeoverAuthorization,
    advanced_at: ServerInstant,
) -> Result<(), DomainError> {
    let allowed = match authorization {
        RunClaimTakeoverAuthorization::Scheduler { reason, .. } => match reason {
            SchedulerPreemptionReason::ClaimExpired => {
                claim.state() == RunClaimState::Expired
                    || (claim.state() == RunClaimState::Active && advanced_at >= claim.expires_at())
            }
            SchedulerPreemptionReason::NodeRevoked => claim.state() == RunClaimState::Revoked,
            SchedulerPreemptionReason::HolderUnavailable => {
                claim.state() == RunClaimState::Active && advanced_at < claim.expires_at()
            }
        },
        RunClaimTakeoverAuthorization::Governance { decision_id, .. } => {
            !decision_id.as_uuid().is_nil()
                && matches!(
                    claim.state(),
                    RunClaimState::Active | RunClaimState::Expired | RunClaimState::Revoked
                )
        }
    };
    if !allowed || authorization.authorized_at() != advanced_at {
        return Err(DomainError::InvocationClaimStale);
    }
    Ok(())
}

fn command_claim_proof(command: &InvocationRunCommand) -> Option<RunClaimProof> {
    match command {
        InvocationRunCommand::MarkDispatchStarted { proof, .. }
        | InvocationRunCommand::ObserveAdapterStarted { proof, .. }
        | InvocationRunCommand::Complete { proof, .. }
        | InvocationRunCommand::Fail { proof, .. } => Some(*proof),
        InvocationRunCommand::Cancel { authorization, .. } => Some(authorization.current_claim),
        InvocationRunCommand::Reserve(_) | InvocationRunCommand::BeginReconciliation { .. } => None,
    }
}

fn validate_run_claim_replay(
    run: &InvocationRun,
    events: &[InvocationRunEvent],
    history: &VerifiedRunClaimHistory,
) -> Result<(), DomainError> {
    let initial = history
        .get(run.run_claim_binding.claim_id)
        .ok_or(DomainError::InvocationClaimStale)?;
    if initial.run_id() != run.id || initial.binding() != run.run_claim_binding {
        return Err(DomainError::InvocationClaimStale);
    }
    let mut authoritative_binding = run.run_claim_binding;
    for event in events {
        if let InvocationRunEvent::ClaimAdvanced {
            previous_claim,
            current_claim,
            advanced_at,
        } = event
        {
            let previous = history
                .get(previous_claim.claim_id)
                .ok_or(DomainError::InvocationClaimStale)?;
            let current = history
                .get(current_claim.claim_id)
                .ok_or(DomainError::InvocationClaimStale)?;
            if *previous_claim != authoritative_binding
                || previous.binding() != *previous_claim
                || current.binding() != *current_claim
                || previous.run_id() != run.id
                || current.run_id() != run.id
                || !matches!(
                    previous.state(),
                    RunClaimState::Superseded | RunClaimState::Expired | RunClaimState::Revoked
                )
                || previous.terminal_at().is_none_or(|at| at > *advanced_at)
                || (previous.state() == RunClaimState::Superseded
                    && (previous.superseding_claim_id() != Some(current.id())
                        || previous.superseding_generation() != Some(current.generation())))
                || current.granted_at() != *advanced_at
            {
                return Err(DomainError::InvocationClaimStale);
            }
            authoritative_binding = *current_claim;
            continue;
        }

        let Some((proof, occurred_at, terminal_digest)) = run_event_claim_fact(event) else {
            continue;
        };
        authoritative_binding.validate_proof(proof)?;
        let claim = history
            .get(authoritative_binding.claim_id)
            .ok_or(DomainError::InvocationClaimStale)?;
        let is_terminal_effect = terminal_digest.is_some();
        if claim.binding() != authoritative_binding
            || claim.run_id() != run.id
            || occurred_at < claim.granted_at()
            || occurred_at >= claim.expires_at()
            || claim.terminal_at().is_some_and(|terminal_at| {
                if is_terminal_effect {
                    occurred_at > terminal_at
                } else {
                    occurred_at >= terminal_at
                }
            })
        {
            return Err(DomainError::InvocationClaimStale);
        }
        if let Some(expected_digest) = terminal_digest
            && (claim.state() != RunClaimState::Completed
                || claim.terminal_at() != Some(occurred_at)
                || claim.result_digest() != expected_digest)
        {
            return Err(DomainError::InvariantViolation {
                invariant: "terminal_run_event_must_match_completed_claim_effect",
            });
        }
    }
    let current = history
        .get(run.current_claim_binding().claim_id)
        .ok_or(DomainError::InvocationClaimStale)?;
    if current.run_id() != run.id || current.binding() != run.current_claim_binding() {
        return Err(DomainError::InvocationClaimStale);
    }
    if run.state.is_terminal() && current.state() != RunClaimState::Completed {
        return Err(DomainError::InvariantViolation {
            invariant: "invocation_run_and_current_claim_terminality_must_match",
        });
    }
    if !run.state.is_terminal() && current.state() != RunClaimState::Active {
        return Err(DomainError::InvocationClaimStale);
    }
    Ok(())
}

fn run_event_claim_fact(
    event: &InvocationRunEvent,
) -> Option<(RunClaimProof, ServerInstant, Option<Option<Sha256Digest>>)> {
    match event {
        InvocationRunEvent::Reserved(reserve) => Some((
            RunClaimProof {
                claim_id: reserve.run_claim_binding.claim_id,
                claim_generation: reserve.run_claim_binding.claim_generation,
                holder_node_id: reserve.run_claim_binding.holder_node_id,
            },
            reserve.reserved_at,
            None,
        )),
        InvocationRunEvent::DispatchStarted {
            proof, started_at, ..
        } => Some((*proof, *started_at, None)),
        InvocationRunEvent::AdapterStarted {
            proof, observed_at, ..
        } => Some((*proof, *observed_at, None)),
        InvocationRunEvent::Completed {
            proof,
            outcome_digest,
            completed_at,
            ..
        } => Some((*proof, *completed_at, Some(Some(*outcome_digest)))),
        InvocationRunEvent::Failed {
            proof,
            evidence_digest,
            failed_at,
            ..
        } => Some((*proof, *failed_at, Some(Some(*evidence_digest)))),
        InvocationRunEvent::Cancelled {
            authorization,
            cancelled_at,
            ..
        } => Some((authorization.current_claim, *cancelled_at, Some(None))),
        InvocationRunEvent::ReconciliationStarted { .. }
        | InvocationRunEvent::ClaimAdvanced { .. } => None,
    }
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
        InvocationRunEvent::ClaimAdvanced { .. } => "claim_advanced",
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
    use crate::{
        event::{EVENT_ENVELOPE_VERSION, EventContext},
        ids::{
            AttemptId, FencingToken, GitObjectId, PackageId, PackageRevisionId, PolicyRevisionId,
        },
        state::run_claim::GrantRunClaim,
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
            run_claim_binding: RunClaimBinding {
                claim_id: id(12),
                claim_generation: RunClaimToken::new(1).expect("generation"),
                holder_node_id: id(14),
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

    fn independent_claim() -> RunClaim {
        RunClaim::grant_initial(GrantRunClaim {
            id: id(12),
            run_id: id(13),
            previous_generation: None,
            predecessor_claim_id: None,
            takeover_authorization: None,
            claim_generation: RunClaimToken::new(1).expect("generation"),
            holder_node_id: id(14),
            granted_at: at(0),
            expires_at: at(100),
        })
        .expect("grant independent claim")
        .aggregate
    }

    fn successor_grant(granted_at: ServerInstant, expires_at: ServerInstant) -> GrantRunClaim {
        GrantRunClaim {
            id: id(15),
            run_id: id(13),
            previous_generation: None,
            predecessor_claim_id: None,
            takeover_authorization: None,
            claim_generation: RunClaimToken::new(2).expect("generation"),
            holder_node_id: id(16),
            granted_at,
            expires_at,
        }
    }

    fn legacy_event_bytes(
        payload: InvocationRunEventV1,
        sequence: u64,
        occurred_at: ServerInstant,
    ) -> Vec<u8> {
        let mut envelope = EventEnvelope::new(
            EventContext {
                event_id: id(sequence as u8 + 40),
                aggregate_id: AggregateId::InvocationRun(id(13)),
                aggregate_version: AggregateVersion::new(sequence),
                aggregate_seq: sequence,
                event_type: format!("invocation_run.legacy_{sequence}"),
                schema_version: INVOCATION_RUN_LEGACY_SCHEMA_VERSION,
                actor_id: id(50),
                correlation_id: id(51),
                causation_id: None,
                occurred_at,
            },
            payload,
        )
        .expect("temporary v2 envelope");
        assert_eq!(envelope.envelope_version, EVENT_ENVELOPE_VERSION);
        envelope.envelope_version = LEGACY_EVENT_ENVELOPE_VERSION;
        envelope.payload_digest = Sha256Digest::of_bytes(
            serde_json::to_vec(&envelope.payload).expect("legacy typed payload bytes"),
        );
        envelope.to_json().expect("legacy envelope json")
    }

    fn legacy_snapshot(run: &InvocationRun, claim: &RunClaim) -> InvocationRunSnapshotV1 {
        InvocationRunSnapshotV1 {
            id: run.id,
            start_context: run.start_context.clone(),
            state: run.state,
            // Actual main-v1 mutated the embedded claim to Completed together
            // with a terminal run; nonterminal positions remained Active.
            current_claim: EmbeddedRunClaimV1 {
                id: claim.id(),
                run_id: claim.run_id(),
                claim_generation: claim.generation(),
                holder_node_id: claim.holder_node_id(),
                state: if run.state.is_terminal() {
                    RunClaimState::Completed
                } else {
                    RunClaimState::Active
                },
                granted_at: claim.granted_at(),
                expires_at: claim.expires_at(),
            },
            outbox_dispatch_id: run.outbox_dispatch_id.clone(),
            session_proof_digest: run.session_proof_digest,
            output_capsule: run.output_capsule,
            outcome: run.outcome,
            outcome_digest: run.outcome_digest,
            usage: run.usage,
            terminal_reason: run.terminal_reason.clone(),
            terminal_evidence_digest: run.terminal_evidence_digest,
            reserved_at: run.reserved_at,
            started_at: run.started_at,
            terminal_at: run.terminal_at,
            version: run.version,
        }
    }

    fn running_run_with_events() -> (InvocationRun, RunClaim, Vec<InvocationRunEvent>) {
        let claim = independent_claim();
        let reserved =
            InvocationRun::reserve_with_claim(reserve(), &claim, at(0)).expect("reserve");
        let starting = reserved
            .aggregate
            .transition_with_claim(
                &claim,
                &InvocationRunCommand::MarkDispatchStarted {
                    expected_version: reserved.aggregate.version,
                    proof: proof(),
                    outbox_dispatch_id: ProtocolKey::new("dispatch-malicious-test").expect("key"),
                    started_at: at(1),
                },
                at(1),
            )
            .expect("starting");
        let running = starting
            .aggregate
            .transition_with_claim(
                &claim,
                &InvocationRunCommand::ObserveAdapterStarted {
                    expected_version: starting.aggregate.version,
                    proof: proof(),
                    external_invocation_key: ProtocolKey::new("run-external-1").expect("key"),
                    session_proof_digest: Sha256Digest::of_bytes(b"session"),
                    observed_at: at(2),
                },
                at(2),
            )
            .expect("running");
        (
            running.aggregate,
            claim,
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
        let (running, claim, mut events) = running_run_with_events();
        let context = running.start_context.clone();
        let completed = running
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Complete {
                    expected_version: running.version,
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
                at(3),
            )
            .expect("complete");
        assert_eq!(completed.run.aggregate.state, InvocationRunState::Completed);
        assert_eq!(completed.claim.aggregate.state(), RunClaimState::Completed);
        assert_eq!(completed.run.aggregate.start_context, context);
        events.push(completed.run.events[0].clone());
        let still_active =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![claim.snapshot()])
                .expect("active claim history");
        assert!(InvocationRun::replay_authorized(&events, &still_active).is_err());
        let claim_history = VerifiedRunClaimHistory::from_validated_snapshots(vec![
            completed.claim.aggregate.snapshot(),
        ])
        .expect("claim history");
        assert_eq!(
            InvocationRun::replay_authorized(&events, &claim_history).expect("replay"),
            completed.run.aggregate
        );
    }

    #[test]
    fn task_lease_and_run_claim_are_independent_and_terminal_run_never_recovers() {
        let claim = independent_claim();
        let reserved =
            InvocationRun::reserve_with_claim(reserve(), &claim, at(0)).expect("reserve");
        let immutable_binding = reserved.aggregate.run_claim_binding;
        reserved
            .aggregate
            .authorize_claim(&claim, proof(), at(1))
            .expect("cross aggregate authorization");
        assert_eq!(
            reserved.aggregate.authorize_claim(&claim, proof(), at(100)),
            Err(DomainError::InvocationClaimStale)
        );
        assert_ne!(
            reserved
                .aggregate
                .start_context
                .material
                .author_lease_id
                .map(|id| id.into_uuid()),
            Some(reserved.aggregate.run_claim_binding.claim_id.into_uuid())
        );
        let cancelled = reserved
            .aggregate
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Cancel {
                    expected_version: reserved.aggregate.version,
                    authorization: CancellationAuthorization {
                        current_claim: proof(),
                    },
                    reason_code: "operator_cancel".into(),
                    stop_or_reconciliation_obligation_created: true,
                    cancelled_at: at(1),
                },
                at(1),
            )
            .expect("cancel");
        let cancelled_run = cancelled.run.aggregate;
        assert_eq!(cancelled_run.run_claim_binding, immutable_binding);
        assert_eq!(cancelled.claim.aggregate.state(), RunClaimState::Completed);
        assert!(matches!(
            InvocationRun::decide(
                Some(&cancelled_run),
                &InvocationRunCommand::Fail {
                    expected_version: AggregateVersion::new(1),
                    proof: proof(),
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
    fn canonical_boundary_rejects_nonexistent_expired_and_revoked_claims() {
        let claim = independent_claim();
        let mut missing_reserve = reserve();
        missing_reserve.run_claim_binding.claim_id = id(99);
        assert_eq!(
            InvocationRun::reserve_with_claim(missing_reserve, &claim, at(0)),
            Err(DomainError::InvocationClaimStale)
        );

        let expired = claim
            .execute(&RunClaimCommand::Expire {
                expected_version: claim.version(),
                expired_at: at(100),
            })
            .expect("expire")
            .aggregate;
        let mut expired_reserve = reserve();
        expired_reserve.reserved_at = at(100);
        assert_eq!(
            InvocationRun::reserve_with_claim(expired_reserve, &expired, at(100)),
            Err(DomainError::InvocationClaimStale)
        );

        let claim = independent_claim();
        let revoked = claim
            .execute(&RunClaimCommand::Revoke {
                expected_version: claim.version(),
                reason_code: "node_quarantined".into(),
                revoked_at: at(1),
            })
            .expect("revoke")
            .aggregate;
        let mut revoked_reserve = reserve();
        revoked_reserve.reserved_at = at(1);
        assert_eq!(
            InvocationRun::reserve_with_claim(revoked_reserve, &revoked, at(1)),
            Err(DomainError::InvocationClaimStale)
        );
    }

    #[test]
    fn public_run_transition_rejects_expired_revoked_and_mismatched_claim_witnesses() {
        let claim = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &claim, at(0))
            .expect("reserve")
            .aggregate;
        let dispatch =
            |expected_version, proof, started_at| InvocationRunCommand::MarkDispatchStarted {
                expected_version,
                proof,
                outbox_dispatch_id: ProtocolKey::new("public-boundary-dispatch").expect("key"),
                started_at,
            };

        let expired = claim
            .execute(&RunClaimCommand::Expire {
                expected_version: claim.version(),
                expired_at: at(100),
            })
            .expect("expire")
            .aggregate;
        assert_eq!(
            reserved.transition_with_claim(
                &expired,
                &dispatch(reserved.version, claim.proof(), at(100)),
                at(100),
            ),
            Err(DomainError::InvocationClaimStale)
        );

        let claim = independent_claim();
        let revoked = claim
            .execute(&RunClaimCommand::Revoke {
                expected_version: claim.version(),
                reason_code: "node_quarantined".into(),
                revoked_at: at(1),
            })
            .expect("revoke")
            .aggregate;
        assert_eq!(
            reserved.transition_with_claim(
                &revoked,
                &dispatch(reserved.version, claim.proof(), at(1)),
                at(1),
            ),
            Err(DomainError::InvocationClaimStale)
        );

        let unrelated = RunClaim::grant_initial(GrantRunClaim {
            id: id(70),
            run_id: id(71),
            previous_generation: None,
            predecessor_claim_id: None,
            takeover_authorization: None,
            claim_generation: RunClaimToken::new(1).expect("generation"),
            holder_node_id: id(72),
            granted_at: at(0),
            expires_at: at(100),
        })
        .expect("unrelated claim")
        .aggregate;
        assert_eq!(
            reserved.transition_with_claim(
                &unrelated,
                &dispatch(reserved.version, unrelated.proof(), at(1)),
                at(1),
            ),
            Err(DomainError::InvocationClaimStale)
        );
    }

    #[test]
    fn v2_snapshot_restore_requires_valid_shape_and_complete_claim_history() {
        let claim = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &claim, at(0))
            .expect("reserve")
            .aggregate;
        let history = VerifiedRunClaimHistory::from_validated_snapshots(vec![claim.snapshot()])
            .expect("history");
        assert_eq!(
            InvocationRun::restore_snapshot(reserved.snapshot(), &history).expect("restore"),
            reserved
        );

        let mut arbitrary_head = reserved.snapshot();
        arbitrary_head.current_claim_head = RunClaimBinding {
            claim_id: id(80),
            claim_generation: RunClaimToken::new(2).expect("generation"),
            holder_node_id: id(81),
        };
        assert_eq!(
            InvocationRun::restore_snapshot(arbitrary_head, &history),
            Err(DomainError::InvocationClaimStale)
        );

        let mut regressed_time = reserved.snapshot();
        regressed_time.updated_at = at(-1);
        assert_eq!(
            InvocationRun::restore_snapshot(regressed_time, &history),
            Err(DomainError::InvocationClaimStale)
        );

        let mut forged_terminal = reserved.snapshot();
        forged_terminal.state = InvocationRunState::Completed;
        forged_terminal.terminal_at = Some(at(1));
        forged_terminal.updated_at = at(1);
        assert!(InvocationRun::restore_snapshot(forged_terminal, &history).is_err());
    }

    #[test]
    fn terminal_snapshot_restore_correlates_claim_time_and_effect_for_every_terminal_state() {
        let (running, claim, _) = running_run_with_events();
        let outcome_digest = Sha256Digest::of_bytes(b"snapshot-outcome");
        let evidence_digest = Sha256Digest::of_bytes(b"snapshot-evidence");
        let completed = running
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Complete {
                    expected_version: running.version(),
                    proof: claim.proof(),
                    outcome: InvocationOutcome::Progressed,
                    outcome_digest,
                    output_capsule: None,
                    usage: TokenUsage {
                        input_tokens: 3,
                        output_tokens: 5,
                    },
                    budget_settled: true,
                    completed_at: at(3),
                },
                at(3),
            )
            .expect("complete");
        let failed = running
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Fail {
                    expected_version: running.version(),
                    proof: claim.proof(),
                    reason_code: "adapter_failed".into(),
                    evidence_digest,
                    budget_settled: true,
                    failed_at: at(3),
                },
                at(3),
            )
            .expect("fail");
        let cancelled = running
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Cancel {
                    expected_version: running.version(),
                    authorization: CancellationAuthorization {
                        current_claim: claim.proof(),
                    },
                    reason_code: "operator_cancel".into(),
                    stop_or_reconciliation_obligation_created: true,
                    cancelled_at: at(3),
                },
                at(3),
            )
            .expect("cancel");

        for (run, terminal_claim) in [
            (&completed.run.aggregate, &completed.claim.aggregate),
            (&failed.run.aggregate, &failed.claim.aggregate),
            (&cancelled.run.aggregate, &cancelled.claim.aggregate),
        ] {
            let history =
                VerifiedRunClaimHistory::from_validated_snapshots(vec![terminal_claim.snapshot()])
                    .expect("terminal history");
            assert_eq!(
                InvocationRun::restore_snapshot(run.snapshot(), &history)
                    .expect("terminal snapshot roundtrip"),
                *run
            );
        }

        let mut wrong_completed_digest = completed.claim.aggregate.snapshot();
        wrong_completed_digest.result_digest = Some(Sha256Digest::of_bytes(b"wrong-outcome"));
        let wrong_completed_history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![wrong_completed_digest])
                .expect("shape-valid but effect-mismatched claim");
        assert_eq!(
            InvocationRun::restore_snapshot(
                completed.run.aggregate.snapshot(),
                &wrong_completed_history,
            ),
            Err(DomainError::InvocationClaimStale)
        );

        let mut wrong_terminal_time = completed.claim.aggregate.snapshot();
        wrong_terminal_time.updated_at = at(4);
        wrong_terminal_time.terminal_at = Some(at(4));
        let wrong_time_history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![wrong_terminal_time])
                .expect("shape-valid but time-mismatched claim");
        assert_eq!(
            InvocationRun::restore_snapshot(
                completed.run.aggregate.snapshot(),
                &wrong_time_history
            ),
            Err(DomainError::InvocationClaimStale)
        );

        let mut missing_failed_digest = failed.claim.aggregate.snapshot();
        missing_failed_digest.result_digest = None;
        let missing_failed_history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![missing_failed_digest])
                .expect("shape-valid but missing failure effect");
        assert_eq!(
            InvocationRun::restore_snapshot(
                failed.run.aggregate.snapshot(),
                &missing_failed_history
            ),
            Err(DomainError::InvocationClaimStale)
        );

        let mut forged_cancelled_digest = cancelled.claim.aggregate.snapshot();
        forged_cancelled_digest.result_digest = Some(Sha256Digest::of_bytes(b"forged-cancel"));
        let forged_cancelled_history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![forged_cancelled_digest])
                .expect("shape-valid but forged cancellation effect");
        assert_eq!(
            InvocationRun::restore_snapshot(
                cancelled.run.aggregate.snapshot(),
                &forged_cancelled_history,
            ),
            Err(DomainError::InvocationClaimStale)
        );
    }

    #[test]
    fn takeover_advances_exactly_one_generation_and_fences_old_holder() {
        let claim = independent_claim();
        let reserved =
            InvocationRun::reserve_with_claim(reserve(), &claim, at(0)).expect("reserve");
        let advanced = reserved
            .aggregate
            .advance_claim(
                &claim,
                GrantRunClaim {
                    id: id(15),
                    run_id: reserved.aggregate.id,
                    previous_generation: Some(claim.generation()),
                    predecessor_claim_id: None,
                    takeover_authorization: None,
                    claim_generation: RunClaimToken::new(2).expect("generation"),
                    holder_node_id: id(16),
                    granted_at: at(1),
                    expires_at: at(200),
                },
                RunClaimTakeoverAuthorization::Scheduler {
                    reason: SchedulerPreemptionReason::HolderUnavailable,
                    authorized_at: at(1),
                },
                reserved.aggregate.version,
                at(1),
            )
            .expect("advance claim");
        assert_eq!(advanced.run.aggregate.run_claim_binding, claim.binding());
        assert_eq!(
            advanced.run.aggregate.current_claim_binding(),
            advanced.current_claim.aggregate.binding()
        );
        assert_eq!(advanced.previous_claim.state(), RunClaimState::Superseded);
        assert_eq!(
            advanced.current_claim.aggregate.state(),
            RunClaimState::Active
        );
        assert_eq!(
            advanced
                .run
                .aggregate
                .authorize_claim(&advanced.previous_claim, claim.proof(), at(2)),
            Err(DomainError::InvocationClaimStale)
        );
        advanced
            .run
            .aggregate
            .authorize_claim(
                &advanced.current_claim.aggregate,
                advanced.current_claim.aggregate.proof(),
                at(2),
            )
            .expect("generation two is authoritative");
        let generation_two_dispatch = InvocationRunCommand::MarkDispatchStarted {
            expected_version: advanced.run.aggregate.version,
            proof: advanced.current_claim.aggregate.proof(),
            outbox_dispatch_id: ProtocolKey::new("dispatch-generation-two").expect("key"),
            started_at: at(2),
        };
        assert_eq!(
            advanced.run.aggregate.transition_with_claim(
                &advanced.previous_claim,
                &InvocationRunCommand::MarkDispatchStarted {
                    expected_version: advanced.run.aggregate.version,
                    proof: claim.proof(),
                    outbox_dispatch_id: ProtocolKey::new("dispatch-stale-generation").expect("key"),
                    started_at: at(2),
                },
                at(2),
            ),
            Err(DomainError::InvocationClaimStale)
        );
        advanced
            .run
            .aggregate
            .transition_with_claim(
                &advanced.current_claim.aggregate,
                &generation_two_dispatch,
                at(2),
            )
            .expect("generation two command");

        let history = VerifiedRunClaimHistory::from_validated_snapshots(vec![
            advanced.previous_claim.snapshot(),
            advanced.current_claim.aggregate.snapshot(),
        ])
        .expect("linked history");
        let replayed = InvocationRun::replay_authorized(
            &[reserved.events[0].clone(), advanced.run.events[0].clone()],
            &history,
        )
        .expect("cross-stream replay");
        assert_eq!(replayed, advanced.run.aggregate);
    }

    #[test]
    fn takeover_accepts_server_expiry_expired_revoked_and_governance_preemption() {
        let active = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &active, at(0))
            .expect("reserve")
            .aggregate;
        let server_expired = reserved
            .advance_claim(
                &active,
                successor_grant(at(100), at(200)),
                RunClaimTakeoverAuthorization::Scheduler {
                    reason: SchedulerPreemptionReason::ClaimExpired,
                    authorized_at: at(100),
                },
                reserved.version,
                at(100),
            )
            .expect("server-time expiry takeover");
        assert_eq!(
            server_expired.previous_claim.state(),
            RunClaimState::Expired
        );
        assert!(matches!(
            server_expired.previous_claim_events.as_slice(),
            [RunClaimEvent::Expired { expired_at }] if *expired_at == at(100)
        ));
        assert_eq!(server_expired.current_claim.aggregate.generation().get(), 2);

        let active = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &active, at(0))
            .expect("reserve")
            .aggregate;
        let expired = active
            .execute(&RunClaimCommand::Expire {
                expected_version: active.version(),
                expired_at: at(100),
            })
            .expect("expire")
            .aggregate;
        let already_expired = reserved
            .advance_claim(
                &expired,
                successor_grant(at(101), at(200)),
                RunClaimTakeoverAuthorization::Scheduler {
                    reason: SchedulerPreemptionReason::ClaimExpired,
                    authorized_at: at(101),
                },
                reserved.version,
                at(101),
            )
            .expect("already expired takeover");
        assert!(already_expired.previous_claim_events.is_empty());
        assert_eq!(
            already_expired.previous_claim.state(),
            RunClaimState::Expired
        );

        let active = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &active, at(0))
            .expect("reserve")
            .aggregate;
        let revoked = active
            .execute(&RunClaimCommand::Revoke {
                expected_version: active.version(),
                reason_code: "node_quarantined".into(),
                revoked_at: at(1),
            })
            .expect("revoke")
            .aggregate;
        let revoked_takeover = reserved
            .advance_claim(
                &revoked,
                successor_grant(at(2), at(200)),
                RunClaimTakeoverAuthorization::Scheduler {
                    reason: SchedulerPreemptionReason::NodeRevoked,
                    authorized_at: at(2),
                },
                reserved.version,
                at(2),
            )
            .expect("revoked takeover");
        assert!(revoked_takeover.previous_claim_events.is_empty());
        assert_eq!(
            revoked_takeover.previous_claim.state(),
            RunClaimState::Revoked
        );

        let active = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &active, at(0))
            .expect("reserve")
            .aggregate;
        let governed = reserved
            .advance_claim(
                &active,
                successor_grant(at(1), at(200)),
                RunClaimTakeoverAuthorization::Governance {
                    decision_id: id(60),
                    action_digest: Sha256Digest::of_bytes(b"authorized-preemption"),
                    authorized_at: at(1),
                },
                reserved.version,
                at(1),
            )
            .expect("governance takeover");
        assert_eq!(governed.previous_claim.state(), RunClaimState::Superseded);
        assert_eq!(
            governed.previous_claim.superseding_claim_id(),
            Some(governed.current_claim.aggregate.id())
        );
    }

    #[test]
    fn takeover_rejects_mismatched_preemption_authority_and_regressed_time() {
        let active = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &active, at(0))
            .expect("reserve")
            .aggregate;
        assert_eq!(
            reserved.advance_claim(
                &active,
                successor_grant(at(1), at(200)),
                RunClaimTakeoverAuthorization::Scheduler {
                    reason: SchedulerPreemptionReason::NodeRevoked,
                    authorized_at: at(1),
                },
                reserved.version,
                at(1),
            ),
            Err(DomainError::InvocationClaimStale)
        );

        let starting = reserved
            .transition_with_claim(
                &active,
                &InvocationRunCommand::MarkDispatchStarted {
                    expected_version: reserved.version,
                    proof: active.proof(),
                    outbox_dispatch_id: ProtocolKey::new("monotonic-dispatch").expect("key"),
                    started_at: at(2),
                },
                at(2),
            )
            .expect("starting")
            .aggregate;
        assert!(
            starting
                .advance_claim(
                    &active,
                    successor_grant(at(1), at(200)),
                    RunClaimTakeoverAuthorization::Scheduler {
                        reason: SchedulerPreemptionReason::HolderUnavailable,
                        authorized_at: at(1),
                    },
                    starting.version,
                    at(1),
                )
                .is_err()
        );
    }

    #[test]
    fn malicious_terminal_events_cannot_bypass_claim_budget_or_stop_obligation() {
        let claim = independent_claim();
        let reserved =
            InvocationRun::reserve_with_claim(reserve(), &claim, at(0)).expect("reserve");
        let wrong_dispatch = InvocationRunEvent::DispatchStarted {
            proof: RunClaimProof {
                holder_node_id: id(99),
                ..proof()
            },
            outbox_dispatch_id: ProtocolKey::new("forged-dispatch").expect("key"),
            started_at: at(1),
        };
        assert_eq!(
            InvocationRun::apply_event(Some(&reserved.aggregate), &wrong_dispatch),
            Err(DomainError::InvocationClaimStale)
        );
        assert_eq!(
            InvocationRun::replay(&[reserved.events[0].clone(), wrong_dispatch]),
            Err(DomainError::InvocationClaimStale)
        );

        let (running, _claim, prefix) = running_run_with_events();
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

        let fail_with_stale_claim = InvocationRunCommand::Fail {
            expected_version: running.version,
            proof: wrong_claim,
            reason_code: "adapter_failed".into(),
            evidence_digest: Sha256Digest::of_bytes(b"evidence"),
            budget_settled: true,
            failed_at: at(3),
        };
        assert_eq!(
            InvocationRun::decide(Some(&running), &fail_with_stale_claim),
            Err(DomainError::InvocationClaimStale)
        );
        let failed_with_stale_claim = InvocationRunEvent::Failed {
            proof: wrong_claim,
            reason_code: "adapter_failed".into(),
            evidence_digest: Sha256Digest::of_bytes(b"evidence"),
            budget_settled: true,
            failed_at: at(3),
        };
        assert_eq!(
            InvocationRun::apply_event(Some(&running), &failed_with_stale_claim),
            Err(DomainError::InvocationClaimStale)
        );
        let mut malicious_replay = prefix.clone();
        malicious_replay.push(failed_with_stale_claim);
        assert_eq!(
            InvocationRun::replay(&malicious_replay),
            Err(DomainError::InvocationClaimStale)
        );

        let failed_without_settlement = InvocationRunEvent::Failed {
            proof: proof(),
            reason_code: "adapter_failed".into(),
            evidence_digest: Sha256Digest::of_bytes(b"evidence"),
            budget_settled: false,
            failed_at: at(3),
        };
        assert!(InvocationRun::apply_event(Some(&running), &failed_without_settlement).is_err());
        let mut malicious_replay = prefix.clone();
        malicious_replay.push(failed_without_settlement);
        assert!(InvocationRun::replay(&malicious_replay).is_err());

        let stale_cancellation_authorization = CancellationAuthorization {
            current_claim: wrong_claim,
        };
        let cancel_with_stale_claim = InvocationRunCommand::Cancel {
            expected_version: running.version,
            authorization: stale_cancellation_authorization,
            reason_code: "operator_cancel".into(),
            stop_or_reconciliation_obligation_created: true,
            cancelled_at: at(3),
        };
        assert_eq!(
            InvocationRun::decide(Some(&running), &cancel_with_stale_claim),
            Err(DomainError::InvocationClaimStale)
        );
        let cancelled_with_stale_claim = InvocationRunEvent::Cancelled {
            authorization: stale_cancellation_authorization,
            reason_code: "operator_cancel".into(),
            stop_or_reconciliation_obligation_created: true,
            cancelled_at: at(3),
        };
        assert_eq!(
            InvocationRun::apply_event(Some(&running), &cancelled_with_stale_claim),
            Err(DomainError::InvocationClaimStale)
        );
        let mut malicious_replay = prefix.clone();
        malicious_replay.push(cancelled_with_stale_claim);
        assert_eq!(
            InvocationRun::replay(&malicious_replay),
            Err(DomainError::InvocationClaimStale)
        );

        let cancelled_without_obligation = InvocationRunEvent::Cancelled {
            authorization: CancellationAuthorization {
                current_claim: proof(),
            },
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

    #[test]
    fn v1_envelopes_verify_legacy_digest_before_upcast_and_authorized_replay() {
        let claim = independent_claim();
        let embedded = EmbeddedRunClaimV1 {
            id: claim.id(),
            run_id: claim.run_id(),
            claim_generation: claim.generation(),
            holder_node_id: claim.holder_node_id(),
            state: RunClaimState::Active,
            granted_at: claim.granted_at(),
            expires_at: claim.expires_at(),
        };
        let outcome_digest = Sha256Digest::of_bytes(b"legacy-outcome");
        let encoded = vec![
            legacy_event_bytes(
                InvocationRunEventV1::Reserved(Box::new(ReserveInvocationRunV1 {
                    id: id(13),
                    start_context: start_context(),
                    run_claim: embedded,
                    reserved_at: at(0),
                })),
                1,
                at(0),
            ),
            legacy_event_bytes(
                InvocationRunEventV1::DispatchStarted {
                    outbox_dispatch_id: ProtocolKey::new("legacy-dispatch").expect("key"),
                    started_at: at(1),
                },
                2,
                at(1),
            ),
            legacy_event_bytes(
                InvocationRunEventV1::AdapterStarted {
                    external_invocation_key: ProtocolKey::new("run-external-1").expect("key"),
                    session_proof_digest: Sha256Digest::of_bytes(b"legacy-session"),
                    observed_at: at(2),
                },
                3,
                at(2),
            ),
            legacy_event_bytes(
                InvocationRunEventV1::Completed {
                    proof: claim.proof(),
                    outcome: InvocationOutcome::Progressed,
                    outcome_digest,
                    output_capsule: None,
                    usage: TokenUsage {
                        input_tokens: 2,
                        output_tokens: 3,
                    },
                    budget_settled: true,
                    completed_at: at(3),
                },
                4,
                at(3),
            ),
        ];

        let upcast = InvocationRun::upcast_main_v1_envelopes(&encoded).expect("verified upcast");
        assert_eq!(upcast.len(), 4);
        assert_eq!(
            upcast[1].event(),
            &InvocationRunEvent::DispatchStarted {
                proof: claim.proof(),
                outbox_dispatch_id: ProtocolKey::new("legacy-dispatch").expect("key"),
                started_at: at(1),
            }
        );
        assert!(
            upcast
                .iter()
                .all(|event| event.legacy_envelope().envelope_version
                    == LEGACY_EVENT_ENVELOPE_VERSION)
        );

        let terminal_claim = claim
            .release_for_terminal(claim.proof(), Some(outcome_digest), at(3))
            .expect("matching terminal claim")
            .aggregate;
        let history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![terminal_claim.snapshot()])
                .expect("validated claim history");
        let replay = InvocationRun::replay_main_v1_authorized(&encoded, &history)
            .expect("authorized legacy replay");
        assert_eq!(replay.run().state(), InvocationRunState::Completed);
        assert_eq!(replay.events().len(), encoded.len());

        // Real main-v1 terminal snapshots stored the embedded claim as
        // Completed. They are writable only after the original event stream
        // supplied the exact adapter/reconciliation timestamps.
        let terminal_snapshot = legacy_snapshot(replay.run(), &replay.current_claim);
        assert_eq!(
            terminal_snapshot.clone().upcast(&replay.current_claim),
            Err(DomainError::SchemaVersionUnsupported)
        );
        assert_eq!(
            terminal_snapshot
                .clone()
                .upcast_with_verified_replay(&replay)
                .expect("verified terminal snapshot"),
            *replay.run()
        );
        let mut mismatched_terminal_snapshot = terminal_snapshot;
        mismatched_terminal_snapshot.started_at = Some(at(0));
        assert_eq!(
            mismatched_terminal_snapshot.upcast_with_verified_replay(&replay),
            Err(DomainError::EvidenceInvalid)
        );

        let active_history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![claim.snapshot()])
                .expect("active claim history");
        let running_replay =
            InvocationRun::replay_main_v1_authorized(&encoded[..3], &active_history)
                .expect("running legacy replay");
        let running_snapshot = legacy_snapshot(running_replay.run(), &claim);
        assert_eq!(
            running_snapshot.clone().upcast(&claim),
            Err(DomainError::SchemaVersionUnsupported)
        );
        assert_eq!(
            running_snapshot
                .upcast_with_verified_replay(&running_replay)
                .expect("verified running snapshot"),
            *running_replay.run()
        );

        let mut tampered: serde_json::Value =
            serde_json::from_slice(&encoded[1]).expect("legacy json");
        tampered["payload"]["DispatchStarted"]["outbox_dispatch_id"] =
            serde_json::Value::from("tampered-dispatch");
        let tampered = serde_json::to_vec(&tampered).expect("tampered json");
        assert_eq!(
            InvocationRun::upcast_main_v1_envelopes(&[tampered]),
            Err(DomainError::EvidenceInvalid)
        );
    }

    #[test]
    fn unpublished_preview_v1_wire_shape_is_not_treated_as_main_v1() {
        #[derive(Serialize)]
        enum PreviewInvocationRunEventV1 {
            DispatchStarted {
                proof: RunClaimProof,
                outbox_dispatch_id: ProtocolKey,
                started_at: ServerInstant,
            },
        }

        let payload = PreviewInvocationRunEventV1::DispatchStarted {
            proof: proof(),
            outbox_dispatch_id: ProtocolKey::new("preview-dispatch").expect("key"),
            started_at: at(1),
        };
        let mut envelope = EventEnvelope::new(
            EventContext {
                event_id: id(91),
                aggregate_id: AggregateId::InvocationRun(id(13)),
                aggregate_version: AggregateVersion::new(1),
                aggregate_seq: 1,
                event_type: "invocation_run.preview_dispatch_started".into(),
                schema_version: INVOCATION_RUN_LEGACY_SCHEMA_VERSION,
                actor_id: id(50),
                correlation_id: id(51),
                causation_id: None,
                occurred_at: at(1),
            },
            payload,
        )
        .expect("preview envelope");
        envelope.envelope_version = LEGACY_EVENT_ENVELOPE_VERSION;
        envelope.payload_digest = Sha256Digest::of_bytes(
            serde_json::to_vec(&envelope.payload).expect("preview typed payload bytes"),
        );
        let encoded = envelope.to_json().expect("preview json");

        assert_eq!(
            InvocationRun::upcast_main_v1_envelopes(&[encoded]),
            Err(DomainError::SchemaInvalid)
        );
    }

    #[test]
    fn main_v1_reconciling_without_adapter_requires_verified_replay_and_roundtrips() {
        let claim = independent_claim();
        let embedded = EmbeddedRunClaimV1 {
            id: claim.id(),
            run_id: claim.run_id(),
            claim_generation: claim.generation(),
            holder_node_id: claim.holder_node_id(),
            state: RunClaimState::Active,
            granted_at: claim.granted_at(),
            expires_at: claim.expires_at(),
        };
        let outcome_digest = Sha256Digest::of_bytes(b"reconciled-without-adapter");
        let mut encoded = vec![
            legacy_event_bytes(
                InvocationRunEventV1::Reserved(Box::new(ReserveInvocationRunV1 {
                    id: id(13),
                    start_context: start_context(),
                    run_claim: embedded,
                    reserved_at: at(0),
                })),
                1,
                at(0),
            ),
            legacy_event_bytes(
                InvocationRunEventV1::DispatchStarted {
                    outbox_dispatch_id: ProtocolKey::new("reconcile-dispatch").expect("key"),
                    started_at: at(1),
                },
                2,
                at(1),
            ),
            legacy_event_bytes(
                InvocationRunEventV1::ReconciliationStarted {
                    reason_code: "adapter_start_uncertain".into(),
                    started_at: at(2),
                },
                3,
                at(2),
            ),
        ];
        let active_history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![claim.snapshot()])
                .expect("active claim history");
        let reconciling = InvocationRun::replay_main_v1_authorized(&encoded, &active_history)
            .expect("reconciling replay");
        let reconciling_v2 = reconciling.run().snapshot();
        assert_eq!(reconciling_v2.state, InvocationRunState::Reconciling);
        assert_eq!(reconciling_v2.adapter_started_at, None);
        assert_eq!(reconciling_v2.session_proof_digest, None);
        assert_eq!(reconciling_v2.reconciliation_started_at, Some(at(2)));

        let legacy_reconciling = legacy_snapshot(reconciling.run(), &claim);
        assert_eq!(
            legacy_reconciling.clone().upcast(&claim),
            Err(DomainError::SchemaVersionUnsupported)
        );
        assert_eq!(
            legacy_reconciling
                .upcast_with_verified_replay(&reconciling)
                .expect("verified reconciliation timestamp"),
            *reconciling.run()
        );

        encoded.push(legacy_event_bytes(
            InvocationRunEventV1::Completed {
                proof: claim.proof(),
                outcome: InvocationOutcome::Progressed,
                outcome_digest,
                output_capsule: None,
                usage: TokenUsage {
                    input_tokens: 1,
                    output_tokens: 1,
                },
                budget_settled: true,
                completed_at: at(3),
            },
            4,
            at(3),
        ));
        let terminal_claim = claim
            .release_for_terminal(claim.proof(), Some(outcome_digest), at(3))
            .expect("terminal claim")
            .aggregate;
        let terminal_history =
            VerifiedRunClaimHistory::from_validated_snapshots(vec![terminal_claim.snapshot()])
                .expect("terminal history");
        let completed = InvocationRun::replay_main_v1_authorized(&encoded, &terminal_history)
            .expect("completed replay");
        let completed_v2 = completed.run().snapshot();
        assert_eq!(completed_v2.state, InvocationRunState::Completed);
        assert_eq!(completed_v2.adapter_started_at, None);
        assert_eq!(completed_v2.session_proof_digest, None);
        assert_eq!(completed_v2.reconciliation_started_at, Some(at(2)));
        assert_eq!(
            InvocationRun::restore_snapshot(completed_v2, &terminal_history)
                .expect("v2 snapshot roundtrip"),
            *completed.run()
        );

        let terminal_v1 = legacy_snapshot(completed.run(), &terminal_claim);
        assert_eq!(
            terminal_v1.clone().upcast(&terminal_claim),
            Err(DomainError::SchemaVersionUnsupported)
        );
        assert_eq!(
            terminal_v1
                .upcast_with_verified_replay(&completed)
                .expect("terminal main-v1 cross-check"),
            *completed.run()
        );
    }

    #[test]
    fn v1_snapshot_json_and_legacy_digests_are_golden_for_all_run_states() {
        let claim = independent_claim();
        let reserved = InvocationRun::reserve_with_claim(reserve(), &claim, at(0))
            .expect("reserve")
            .aggregate;
        let starting = reserved
            .transition_with_claim(
                &claim,
                &InvocationRunCommand::MarkDispatchStarted {
                    expected_version: reserved.version,
                    proof: claim.proof(),
                    outbox_dispatch_id: ProtocolKey::new("golden-dispatch").expect("key"),
                    started_at: at(1),
                },
                at(1),
            )
            .expect("starting")
            .aggregate;
        let running = starting
            .transition_with_claim(
                &claim,
                &InvocationRunCommand::ObserveAdapterStarted {
                    expected_version: starting.version,
                    proof: claim.proof(),
                    external_invocation_key: ProtocolKey::new("run-external-1").expect("key"),
                    session_proof_digest: Sha256Digest::of_bytes(b"golden-session"),
                    observed_at: at(2),
                },
                at(2),
            )
            .expect("running")
            .aggregate;
        let reconciling = running
            .begin_reconciliation(&InvocationRunCommand::BeginReconciliation {
                expected_version: running.version,
                reason_code: "adapter_timeout".into(),
                started_at: at(3),
            })
            .expect("reconciling")
            .aggregate;
        let completed = running
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Complete {
                    expected_version: running.version,
                    proof: claim.proof(),
                    outcome: InvocationOutcome::Progressed,
                    outcome_digest: Sha256Digest::of_bytes(b"golden-outcome"),
                    output_capsule: None,
                    usage: TokenUsage {
                        input_tokens: 5,
                        output_tokens: 8,
                    },
                    budget_settled: true,
                    completed_at: at(3),
                },
                at(3),
            )
            .expect("completed");
        let failed = running
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Fail {
                    expected_version: running.version,
                    proof: claim.proof(),
                    reason_code: "adapter_failed".into(),
                    evidence_digest: Sha256Digest::of_bytes(b"golden-evidence"),
                    budget_settled: true,
                    failed_at: at(3),
                },
                at(3),
            )
            .expect("failed");
        let cancelled = running
            .terminalize_with_claim(
                &claim,
                &InvocationRunCommand::Cancel {
                    expected_version: running.version,
                    authorization: CancellationAuthorization {
                        current_claim: claim.proof(),
                    },
                    reason_code: "operator_cancel".into(),
                    stop_or_reconciliation_obligation_created: true,
                    cancelled_at: at(3),
                },
                at(3),
            )
            .expect("cancelled");

        let cases = [
            (
                "reserved",
                legacy_snapshot(&reserved, &claim),
                claim.clone(),
            ),
            (
                "starting",
                legacy_snapshot(&starting, &claim),
                claim.clone(),
            ),
            ("running", legacy_snapshot(&running, &claim), claim.clone()),
            (
                "reconciling",
                legacy_snapshot(&reconciling, &claim),
                claim.clone(),
            ),
            (
                "completed",
                legacy_snapshot(&completed.run.aggregate, &completed.claim.aggregate),
                completed.claim.aggregate,
            ),
            (
                "failed",
                legacy_snapshot(&failed.run.aggregate, &failed.claim.aggregate),
                failed.claim.aggregate,
            ),
            (
                "cancelled",
                legacy_snapshot(&cancelled.run.aggregate, &cancelled.claim.aggregate),
                cancelled.claim.aggregate,
            ),
        ];

        let golden_digests = [
            (
                "reserved",
                "sha256:23c1ccbd7db7910dbd346a881bc2d4d3e24e78c169cbfe519839cf5357430ca9",
            ),
            (
                "starting",
                "sha256:313695ce719d79d1ff2b8de48101fb05ce96a3671ed419c07ed6527859b7fc4c",
            ),
            (
                "running",
                "sha256:dfb92632768ba973cc73131e63e039caade2bda5eccf96471c1303bee6d5cf47",
            ),
            (
                "reconciling",
                "sha256:2ba1bcfdb488672e6e1035bb77dae01d5f0613ced2f7531509fd9097a77cbbaa",
            ),
            (
                "completed",
                "sha256:320d158d1aecd13ac0a79a7e1509ef5a617b449c4a1a70ccf11e0a017ec9269e",
            ),
            (
                "failed",
                "sha256:fbc445e588e12c5dec0012de2f2ac5723b4dfcd47062e44771d307b0b3c4468a",
            ),
            (
                "cancelled",
                "sha256:6d56e39c0672421dbef7c1bc6b687fa52d117d6a0593760efd9382787e085326",
            ),
        ];

        for ((state, snapshot, migrated_claim), (golden_state, golden_digest)) in
            cases.into_iter().zip(golden_digests)
        {
            assert_eq!(state, golden_state);
            let json = serde_json::to_vec(&snapshot).expect("golden json");
            let value: serde_json::Value = serde_json::from_slice(&json).expect("json value");
            assert_eq!(value["state"], state);
            assert_eq!(
                serde_json::from_slice::<InvocationRunSnapshotV1>(&json).expect("round trip"),
                snapshot
            );
            if matches!(state, "reserved" | "starting") {
                assert_eq!(
                    snapshot
                        .clone()
                        .upcast(&migrated_claim)
                        .expect("snapshot-only safe position")
                        .state()
                        .as_str(),
                    state
                );
            } else {
                assert_eq!(
                    snapshot.clone().upcast(&migrated_claim),
                    Err(DomainError::SchemaVersionUnsupported)
                );
            }
            assert_eq!(
                Sha256Digest::of_bytes(&json).to_prefixed_hex(),
                golden_digest
            );
        }
    }

    #[test]
    fn v1_claim_snapshots_require_explicit_upcast_and_v2_events_are_marked() {
        let claim_transition = RunClaim::grant_initial(GrantRunClaim {
            id: id(12),
            run_id: id(13),
            previous_generation: None,
            predecessor_claim_id: None,
            takeover_authorization: None,
            claim_generation: RunClaimToken::new(1).expect("generation"),
            holder_node_id: id(14),
            granted_at: at(0),
            expires_at: at(100),
        })
        .expect("claim");
        let legacy_claim = EmbeddedRunClaimV1 {
            id: claim_transition.aggregate.id(),
            run_id: claim_transition.aggregate.run_id(),
            claim_generation: claim_transition.aggregate.generation(),
            holder_node_id: claim_transition.aggregate.holder_node_id(),
            state: claim_transition.aggregate.state(),
            granted_at: claim_transition.aggregate.granted_at(),
            expires_at: claim_transition.aggregate.expires_at(),
        };
        let upgraded_reserve = ReserveInvocationRun::try_from(ReserveInvocationRunV1 {
            id: id(13),
            start_context: start_context(),
            run_claim: legacy_claim.clone(),
            reserved_at: at(0),
        })
        .expect("reservation upcast");
        let reserved =
            InvocationRun::reserve_with_claim(upgraded_reserve, &claim_transition.aggregate, at(0))
                .expect("reserve");
        let snapshot = InvocationRunSnapshotV1 {
            id: reserved.aggregate.id,
            start_context: reserved.aggregate.start_context.clone(),
            state: reserved.aggregate.state,
            current_claim: legacy_claim,
            outbox_dispatch_id: None,
            session_proof_digest: None,
            output_capsule: None,
            outcome: None,
            outcome_digest: None,
            usage: None,
            terminal_reason: None,
            terminal_evidence_digest: None,
            reserved_at: reserved.aggregate.reserved_at,
            started_at: None,
            terminal_at: None,
            version: reserved.aggregate.version,
        };
        assert_eq!(
            snapshot
                .upcast(&claim_transition.aggregate)
                .expect("snapshot upcast"),
            reserved.aggregate
        );
        assert_eq!(
            reserved.events[0].schema_version(),
            INVOCATION_RUN_EVENT_SCHEMA_VERSION
        );
        assert_eq!(
            claim_transition.events[0].schema_version(),
            crate::state::run_claim::RUN_CLAIM_EVENT_SCHEMA_VERSION
        );
        assert_ne!(
            INVOCATION_RUN_LEGACY_SCHEMA_VERSION,
            INVOCATION_RUN_EVENT_SCHEMA_VERSION
        );
    }
}
