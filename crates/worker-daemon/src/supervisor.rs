//! Deterministic Turn Pump over the durable Journal boundary.

use std::collections::{BTreeMap, VecDeque};

use agentforge_domain::{
    ActorId, AttemptId, GitObjectId, IdempotencyKey, ProtocolKey, ServerInstant, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    journal::{
        Journal, JournalCommand, JournalError, JournalRequest, OperationCompletion,
        OperationIdempotencyClass, OperationPlan, PendingOperation,
    },
    runtime::{
        LeaseLossReason, WorkerAttemptState, WorkerCommandEnvelope, WorkerCommandKind, WorkerPhase,
    },
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnRequest {
    pub attempt_id: AttemptId,
    pub expected_version: agentforge_domain::AggregateVersion,
    pub turn_number: u32,
    pub plan_digest: Sha256Digest,
    pub previous_tree: Option<GitObjectId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnOutcome {
    pub turn_id: ProtocolKey,
    pub tree: GitObjectId,
    pub model_claimed_done: bool,
    pub sanitized_result_digest: Sha256Digest,
}

impl TurnOutcome {
    fn validate(&self) -> SupervisorResult<()> {
        if self
            .sanitized_result_digest
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(SupervisorError::ExecutorContract);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationRequest {
    pub attempt_id: AttemptId,
    pub expected_version: agentforge_domain::AggregateVersion,
    pub tree: GitObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationOutcome {
    pub all_hard_passed: bool,
    pub evidence_digest: Sha256Digest,
    pub failure_code: Option<ProtocolKey>,
}

impl VerificationOutcome {
    fn validate(&self) -> SupervisorResult<()> {
        if self
            .evidence_digest
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
            || self.all_hard_passed == self.failure_code.is_some()
        {
            return Err(SupervisorError::VerifierContract);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ExecutionError {
    #[error("executor outcome is unknown")]
    OutcomeUnknown,
    #[error("executor failed before producing an effect: {0}")]
    Failed(ProtocolKey),
}

pub trait TurnExecutor {
    fn start(
        &mut self,
        operation_id: Uuid,
        request: &TurnRequest,
    ) -> Result<TurnOutcome, ExecutionError>;

    fn query(&mut self, operation_id: Uuid) -> Result<Option<TurnOutcome>, ExecutionError>;
}

pub trait LocalVerifier {
    fn verify(
        &mut self,
        operation_id: Uuid,
        request: &VerificationRequest,
    ) -> Result<VerificationOutcome, ExecutionError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CycleContext {
    pub turn_operation_id: Uuid,
    pub verification_operation_id: Uuid,
    pub planned_at: ServerInstant,
    pub turn_deadline_at: ServerInstant,
    pub turn_finished_at: ServerInstant,
    pub verification_deadline_at: ServerInstant,
    pub verification_finished_at: ServerInstant,
}

impl CycleContext {
    fn validate(self) -> SupervisorResult<()> {
        if self.turn_operation_id.is_nil()
            || self.verification_operation_id.is_nil()
            || self.turn_operation_id == self.verification_operation_id
            || self.turn_deadline_at <= self.planned_at
            || self.turn_finished_at < self.planned_at
            || self.turn_finished_at > self.turn_deadline_at
            || self.verification_deadline_at <= self.turn_finished_at
            || self.verification_finished_at < self.turn_finished_at
            || self.verification_finished_at > self.verification_deadline_at
        {
            return Err(SupervisorError::InvalidContext);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnBudget {
    pub max_turns: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StopReason {
    LocalCandidateReady,
    BudgetExhausted,
    LeaseLost,
    Cancelled,
    FatalInfrastructureFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CycleOutcome {
    Continue(WorkerAttemptState),
    Stopped {
        reason: StopReason,
        state: WorkerAttemptState,
    },
}

impl CycleOutcome {
    #[must_use]
    pub fn state(&self) -> &WorkerAttemptState {
        match self {
            Self::Continue(state) | Self::Stopped { state, .. } => state,
        }
    }
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("Turn cycle context is invalid")]
    InvalidContext,
    #[error("executor returned a malformed result")]
    ExecutorContract,
    #[error("verifier returned a malformed result")]
    VerifierContract,
    #[error("non-repeatable operation outcome remains unknown")]
    OutcomeUnknown,
    #[error("executor failed: {0}")]
    ExecutorFailed(ProtocolKey),
    #[error("pending Worker operation does not match the durable Attempt state")]
    RecoveryConflict,
}

impl SupervisorError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Journal(error) => error.code(),
            Self::InvalidContext => "AF_WORKER_CYCLE_INVALID",
            Self::ExecutorContract => "AF_EXECUTOR_RESULT_INVALID",
            Self::VerifierContract => "AF_VERIFIER_RESULT_INVALID",
            Self::OutcomeUnknown => "AF_EXECUTOR_OUTCOME_UNKNOWN",
            Self::ExecutorFailed(_) => "AF_EXECUTOR_FAILED",
            Self::RecoveryConflict => "AF_WORKER_RECOVERY_CONFLICT",
        }
    }
}

pub type SupervisorResult<T> = Result<T, SupervisorError>;

pub struct Supervisor<'a, E, V> {
    journal: &'a mut Journal,
    executor: &'a mut E,
    verifier: &'a mut V,
    actor_id: ActorId,
}

impl<'a, E, V> Supervisor<'a, E, V>
where
    E: TurnExecutor,
    V: LocalVerifier,
{
    pub fn new(
        journal: &'a mut Journal,
        executor: &'a mut E,
        verifier: &'a mut V,
        actor_id: ActorId,
    ) -> SupervisorResult<Self> {
        if actor_id.as_uuid().is_nil() {
            return Err(SupervisorError::InvalidContext);
        }
        Ok(Self {
            journal,
            executor,
            verifier,
            actor_id,
        })
    }

    pub fn drive_cycle(
        &mut self,
        attempt_id: AttemptId,
        budget: TurnBudget,
        context: CycleContext,
    ) -> SupervisorResult<CycleOutcome> {
        context.validate()?;
        let mut state = self
            .journal
            .load_attempt(attempt_id)?
            .ok_or(JournalError::Runtime(
                crate::runtime::WorkerError::HistoryEmpty,
            ))?;
        match state.phase() {
            WorkerPhase::SealingCandidate => {
                return Ok(CycleOutcome::Stopped {
                    reason: StopReason::LocalCandidateReady,
                    state,
                });
            }
            WorkerPhase::Salvaging => {
                return Ok(CycleOutcome::Stopped {
                    reason: StopReason::LeaseLost,
                    state,
                });
            }
            WorkerPhase::LocalCancelled => {
                return Ok(CycleOutcome::Stopped {
                    reason: StopReason::Cancelled,
                    state,
                });
            }
            WorkerPhase::LocalFailed => {
                return Ok(CycleOutcome::Stopped {
                    reason: StopReason::FatalInfrastructureFailure,
                    state,
                });
            }
            WorkerPhase::Implementing | WorkerPhase::LocalVerifying => {}
            _ => return Err(SupervisorError::InvalidContext),
        }

        if let Some(recovered) = self.recover_pending(&state, context)? {
            state = recovered;
            if context.planned_at >= state.lease_expires_at {
                state = self.record(
                    &state,
                    context.turn_operation_id,
                    format!("lease-expired:v{}", state.version().get()),
                    context.planned_at,
                    WorkerCommandKind::LoseLease {
                        reason: LeaseLossReason::Expired,
                        observed_generation: state.lease_generation(),
                    },
                )?;
                return Ok(CycleOutcome::Stopped {
                    reason: StopReason::LeaseLost,
                    state,
                });
            }
            return Ok(if state.phase() == WorkerPhase::SealingCandidate {
                CycleOutcome::Stopped {
                    reason: StopReason::LocalCandidateReady,
                    state,
                }
            } else {
                CycleOutcome::Continue(state)
            });
        }

        if context.planned_at >= state.lease_expires_at {
            state = self.record(
                &state,
                context.turn_operation_id,
                format!("lease-expired:v{}", state.version().get()),
                context.planned_at,
                WorkerCommandKind::LoseLease {
                    reason: LeaseLossReason::Expired,
                    observed_generation: state.lease_generation(),
                },
            )?;
            return Ok(CycleOutcome::Stopped {
                reason: StopReason::LeaseLost,
                state,
            });
        }

        if state.phase() == WorkerPhase::Implementing {
            if budget.max_turns == 0 || state.turns_completed() >= budget.max_turns {
                state = self.record(
                    &state,
                    context.turn_operation_id,
                    format!("budget-exhausted:v{}", state.version().get()),
                    context.planned_at,
                    WorkerCommandKind::Fail {
                        reason_code: ProtocolKey::new("budget-exhausted")
                            .map_err(|_| SupervisorError::InvalidContext)?,
                    },
                )?;
                return Ok(CycleOutcome::Stopped {
                    reason: StopReason::BudgetExhausted,
                    state,
                });
            }
            state = self.run_turn(&state, context)?;
        }

        let state = self.run_verification(&state, context)?;
        if state.phase() == WorkerPhase::SealingCandidate {
            Ok(CycleOutcome::Stopped {
                reason: StopReason::LocalCandidateReady,
                state,
            })
        } else {
            Ok(CycleOutcome::Continue(state))
        }
    }

    fn recover_pending(
        &mut self,
        state: &WorkerAttemptState,
        context: CycleContext,
    ) -> SupervisorResult<Option<WorkerAttemptState>> {
        let mut pending = self.journal.pending_operations(state.attempt_id())?;
        if pending.len() > 1 {
            return Err(SupervisorError::RecoveryConflict);
        }
        let Some(pending) = pending.pop() else {
            return Ok(None);
        };
        let recovered = match pending.plan.kind.as_str() {
            "agent-turn" if state.phase() == WorkerPhase::Implementing => {
                self.recover_turn(state, &pending, context)?
            }
            "local-verification" if state.phase() == WorkerPhase::LocalVerifying => {
                self.recover_verification(state, &pending, context)?
            }
            _ => return Err(SupervisorError::RecoveryConflict),
        };
        Ok(Some(recovered))
    }

    fn recover_turn(
        &mut self,
        state: &WorkerAttemptState,
        pending: &PendingOperation,
        context: CycleContext,
    ) -> SupervisorResult<WorkerAttemptState> {
        let request = self.turn_request(state)?;
        let expected_key = ProtocolKey::new(format!("turn-{}", request.turn_number))
            .map_err(|_| SupervisorError::InvalidContext)?;
        Self::validate_pending_plan(
            pending,
            state,
            &expected_key,
            "agent-turn",
            OperationIdempotencyClass::NonRepeatable,
            digest(&request)?,
            context.turn_finished_at,
        )?;
        let outcome = match self.executor.query(pending.plan.operation_id) {
            Ok(Some(outcome)) => outcome,
            Ok(None) | Err(ExecutionError::OutcomeUnknown) => {
                return Err(SupervisorError::OutcomeUnknown);
            }
            Err(ExecutionError::Failed(code)) => {
                return Err(SupervisorError::ExecutorFailed(code));
            }
        };
        outcome.validate()?;
        self.complete_turn(state, &pending.plan, outcome, context.turn_finished_at)
    }

    fn recover_verification(
        &mut self,
        state: &WorkerAttemptState,
        pending: &PendingOperation,
        context: CycleContext,
    ) -> SupervisorResult<WorkerAttemptState> {
        let request = self.verification_request(state)?;
        let expected_key = ProtocolKey::new(format!("verify-turn-{}", state.turns_completed()))
            .map_err(|_| SupervisorError::InvalidContext)?;
        Self::validate_pending_plan(
            pending,
            state,
            &expected_key,
            "local-verification",
            OperationIdempotencyClass::Idempotent,
            digest(&request)?,
            context.verification_finished_at,
        )?;
        let outcome = self
            .verifier
            .verify(pending.plan.operation_id, &request)
            .map_err(|error| match error {
                ExecutionError::OutcomeUnknown => SupervisorError::OutcomeUnknown,
                ExecutionError::Failed(code) => SupervisorError::ExecutorFailed(code),
            })?;
        outcome.validate()?;
        self.complete_verification(
            state,
            &pending.plan,
            outcome,
            context.verification_finished_at,
        )
    }

    fn validate_pending_plan(
        pending: &PendingOperation,
        state: &WorkerAttemptState,
        expected_key: &ProtocolKey,
        expected_kind: &str,
        expected_class: OperationIdempotencyClass,
        expected_request_digest: Sha256Digest,
        finished_at: ServerInstant,
    ) -> SupervisorResult<()> {
        let plan = &pending.plan;
        if plan.attempt_id != state.attempt_id()
            || &plan.idempotency_key != expected_key
            || plan.kind.as_str() != expected_kind
            || plan.idempotency_class != expected_class
            || plan.request_digest != expected_request_digest
            || finished_at < plan.planned_at
            || finished_at > plan.deadline_at
        {
            return Err(SupervisorError::RecoveryConflict);
        }
        Ok(())
    }

    fn run_turn(
        &mut self,
        state: &WorkerAttemptState,
        context: CycleContext,
    ) -> SupervisorResult<WorkerAttemptState> {
        let request = self.turn_request(state)?;
        let request_digest = digest(&request)?;
        let key = ProtocolKey::new(format!("turn-{}", request.turn_number))
            .map_err(|_| SupervisorError::InvalidContext)?;
        let plan = OperationPlan {
            operation_id: context.turn_operation_id,
            attempt_id: state.attempt_id(),
            idempotency_key: key.clone(),
            kind: ProtocolKey::new("agent-turn").map_err(|_| SupervisorError::InvalidContext)?,
            idempotency_class: OperationIdempotencyClass::NonRepeatable,
            request_digest,
            planned_at: context.planned_at,
            deadline_at: context.turn_deadline_at,
        };
        self.journal.plan_operation(&plan)?;
        let outcome = match self.executor.start(plan.operation_id, &request) {
            Ok(outcome) => outcome,
            Err(ExecutionError::OutcomeUnknown) => match self.executor.query(plan.operation_id) {
                Ok(Some(outcome)) => outcome,
                Ok(None) | Err(ExecutionError::OutcomeUnknown) => {
                    return Err(SupervisorError::OutcomeUnknown);
                }
                Err(ExecutionError::Failed(code)) => {
                    return Err(SupervisorError::ExecutorFailed(code));
                }
            },
            Err(ExecutionError::Failed(code)) => return Err(SupervisorError::ExecutorFailed(code)),
        };
        outcome.validate()?;
        self.complete_turn(state, &plan, outcome, context.turn_finished_at)
    }

    fn turn_request(&self, state: &WorkerAttemptState) -> SupervisorResult<TurnRequest> {
        Ok(TurnRequest {
            attempt_id: state.attempt_id(),
            expected_version: state.version(),
            turn_number: state
                .turns_completed()
                .checked_add(1)
                .ok_or(SupervisorError::InvalidContext)?,
            plan_digest: state.plan_digest().ok_or(SupervisorError::InvalidContext)?,
            previous_tree: state.current_tree().cloned(),
        })
    }

    fn complete_turn(
        &mut self,
        state: &WorkerAttemptState,
        plan: &OperationPlan,
        outcome: TurnOutcome,
        finished_at: ServerInstant,
    ) -> SupervisorResult<WorkerAttemptState> {
        let command = JournalRequest {
            message_id: plan.operation_id,
            actor_id: self.actor_id,
            idempotency_key: IdempotencyKey::new(plan.idempotency_key.as_str())
                .map_err(|_| SupervisorError::InvalidContext)?,
            command: JournalCommand::Apply {
                attempt_id: state.attempt_id(),
                command: WorkerCommandEnvelope {
                    expected_version: state.version(),
                    observed_at: finished_at,
                    command: WorkerCommandKind::TurnProducedChanges {
                        turn_id: outcome.turn_id,
                        tree: outcome.tree,
                        model_claimed_done: outcome.model_claimed_done,
                    },
                },
            },
        };
        Ok(self
            .journal
            .complete_operation(
                &command,
                &OperationCompletion {
                    operation_id: plan.operation_id,
                    result_digest: outcome.sanitized_result_digest,
                    finished_at,
                },
            )?
            .state()
            .clone())
    }

    fn run_verification(
        &mut self,
        state: &WorkerAttemptState,
        context: CycleContext,
    ) -> SupervisorResult<WorkerAttemptState> {
        if state.phase() != WorkerPhase::LocalVerifying {
            return Err(SupervisorError::InvalidContext);
        }
        let request = self.verification_request(state)?;
        let key = ProtocolKey::new(format!("verify-turn-{}", state.turns_completed()))
            .map_err(|_| SupervisorError::InvalidContext)?;
        let plan = OperationPlan {
            operation_id: context.verification_operation_id,
            attempt_id: state.attempt_id(),
            idempotency_key: key.clone(),
            kind: ProtocolKey::new("local-verification")
                .map_err(|_| SupervisorError::InvalidContext)?,
            idempotency_class: OperationIdempotencyClass::Idempotent,
            request_digest: digest(&request)?,
            planned_at: context.turn_finished_at,
            deadline_at: context.verification_deadline_at,
        };
        self.journal.plan_operation(&plan)?;
        let outcome =
            self.verifier
                .verify(plan.operation_id, &request)
                .map_err(|error| match error {
                    ExecutionError::OutcomeUnknown => SupervisorError::OutcomeUnknown,
                    ExecutionError::Failed(code) => SupervisorError::ExecutorFailed(code),
                })?;
        outcome.validate()?;
        self.complete_verification(state, &plan, outcome, context.verification_finished_at)
    }

    fn verification_request(
        &self,
        state: &WorkerAttemptState,
    ) -> SupervisorResult<VerificationRequest> {
        Ok(VerificationRequest {
            attempt_id: state.attempt_id(),
            expected_version: state.version(),
            tree: state
                .current_tree()
                .cloned()
                .ok_or(SupervisorError::InvalidContext)?,
        })
    }

    fn complete_verification(
        &mut self,
        state: &WorkerAttemptState,
        plan: &OperationPlan,
        outcome: VerificationOutcome,
        finished_at: ServerInstant,
    ) -> SupervisorResult<WorkerAttemptState> {
        let result_digest = digest(&outcome)?;
        let command = JournalRequest {
            message_id: plan.operation_id,
            actor_id: self.actor_id,
            idempotency_key: IdempotencyKey::new(plan.idempotency_key.as_str())
                .map_err(|_| SupervisorError::InvalidContext)?,
            command: JournalCommand::Apply {
                attempt_id: state.attempt_id(),
                command: WorkerCommandEnvelope {
                    expected_version: state.version(),
                    observed_at: finished_at,
                    command: WorkerCommandKind::VerificationFinished {
                        passed: outcome.all_hard_passed,
                        evidence_digest: outcome.evidence_digest,
                        failure_code: outcome.failure_code,
                    },
                },
            },
        };
        Ok(self
            .journal
            .complete_operation(
                &command,
                &OperationCompletion {
                    operation_id: plan.operation_id,
                    result_digest,
                    finished_at,
                },
            )?
            .state()
            .clone())
    }

    fn record(
        &mut self,
        state: &WorkerAttemptState,
        message_id: Uuid,
        key: String,
        observed_at: ServerInstant,
        command: WorkerCommandKind,
    ) -> SupervisorResult<WorkerAttemptState> {
        Ok(self
            .journal
            .handle(&JournalRequest {
                message_id,
                actor_id: self.actor_id,
                idempotency_key: IdempotencyKey::new(key)
                    .map_err(|_| SupervisorError::InvalidContext)?,
                command: JournalCommand::Apply {
                    attempt_id: state.attempt_id,
                    command: WorkerCommandEnvelope {
                        expected_version: state.version,
                        observed_at,
                        command,
                    },
                },
            })?
            .state()
            .clone())
    }
}

fn digest<T: Serialize>(value: &T) -> SupervisorResult<Sha256Digest> {
    serde_json_canonicalizer::to_vec(value)
        .map(Sha256Digest::of_bytes)
        .map_err(|_| SupervisorError::InvalidContext)
}

#[derive(Default)]
pub struct FakeTurnExecutor {
    scripted: VecDeque<(TurnOutcome, bool)>,
    results: BTreeMap<Uuid, TurnOutcome>,
    pub starts: u32,
    pub queries: u32,
}

impl FakeTurnExecutor {
    #[must_use]
    pub fn scripted(outcomes: impl IntoIterator<Item = (TurnOutcome, bool)>) -> Self {
        Self {
            scripted: outcomes.into_iter().collect(),
            results: BTreeMap::new(),
            starts: 0,
            queries: 0,
        }
    }
}

impl TurnExecutor for FakeTurnExecutor {
    fn start(
        &mut self,
        operation_id: Uuid,
        _request: &TurnRequest,
    ) -> Result<TurnOutcome, ExecutionError> {
        self.starts = self.starts.saturating_add(1);
        if let Some(existing) = self.results.get(&operation_id) {
            return Ok(existing.clone());
        }
        let (outcome, drop_ack) = self.scripted.pop_front().ok_or_else(|| {
            ExecutionError::Failed(ProtocolKey::new("fake-script-empty").expect("constant key"))
        })?;
        self.results.insert(operation_id, outcome.clone());
        if drop_ack {
            Err(ExecutionError::OutcomeUnknown)
        } else {
            Ok(outcome)
        }
    }

    fn query(&mut self, operation_id: Uuid) -> Result<Option<TurnOutcome>, ExecutionError> {
        self.queries = self.queries.saturating_add(1);
        Ok(self.results.get(&operation_id).cloned())
    }
}

#[derive(Default)]
pub struct FakeVerifier {
    scripted: VecDeque<VerificationOutcome>,
    pub runs: u32,
}

impl FakeVerifier {
    #[must_use]
    pub fn scripted(outcomes: impl IntoIterator<Item = VerificationOutcome>) -> Self {
        Self {
            scripted: outcomes.into_iter().collect(),
            runs: 0,
        }
    }
}

impl LocalVerifier for FakeVerifier {
    fn verify(
        &mut self,
        _operation_id: Uuid,
        _request: &VerificationRequest,
    ) -> Result<VerificationOutcome, ExecutionError> {
        self.runs = self.runs.saturating_add(1);
        self.scripted.pop_front().ok_or_else(|| {
            ExecutionError::Failed(ProtocolKey::new("fake-script-empty").expect("constant key"))
        })
    }
}

#[cfg(test)]
mod tests {
    use agentforge_domain::{FencingToken, LeaseId, PackageId, PackageRevision};
    use tempfile::TempDir;
    use time::macros::datetime;

    use super::*;
    use crate::{journal::JournalDisposition, runtime::AttemptGrant};

    fn at(second: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(second))
    }

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn request(
        state: &WorkerAttemptState,
        actor_id: ActorId,
        message: u8,
        key: &str,
        second: i64,
        command: WorkerCommandKind,
    ) -> JournalRequest {
        JournalRequest {
            message_id: Uuid::from_bytes([message; 16]),
            actor_id,
            idempotency_key: IdempotencyKey::new(key).expect("key"),
            command: JournalCommand::Apply {
                attempt_id: state.attempt_id,
                command: WorkerCommandEnvelope {
                    expected_version: state.version,
                    observed_at: at(second),
                    command,
                },
            },
        }
    }

    fn implementing_fixture() -> (TempDir, Journal, WorkerAttemptState, ActorId) {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut journal = Journal::open(directory.path().join("journal.sqlite3")).expect("journal");
        let actor_id = id(9);
        let grant = AttemptGrant {
            attempt_id: id(1),
            package_id: PackageId::from_uuid(Uuid::from_bytes([2; 16])),
            package_revision: PackageRevision::new(1).expect("revision"),
            package_hash: Sha256Digest::of_bytes("package"),
            base_commit: GitObjectId::new("1".repeat(40)).expect("commit"),
            lease_id: LeaseId::from_uuid(Uuid::from_bytes([3; 16])),
            lease_generation: FencingToken::new(4).expect("generation"),
            lease_expires_at: at(100),
            granted_at: at(0),
        };
        let mut state = journal
            .handle(&JournalRequest {
                message_id: Uuid::from_bytes([10; 16]),
                actor_id,
                idempotency_key: IdempotencyKey::new("grant").expect("key"),
                command: JournalCommand::Grant { grant },
            })
            .expect("grant")
            .state()
            .clone();
        for (message, key, second, command) in [
            (11, "prepare", 1, WorkerCommandKind::BeginPreparation),
            (
                12,
                "workspace",
                2,
                WorkerCommandKind::WorkspacePrepared {
                    workspace_digest: Sha256Digest::of_bytes("workspace"),
                },
            ),
            (
                13,
                "baseline",
                3,
                WorkerCommandKind::BaselineFinished {
                    passed: true,
                    evidence_digest: Sha256Digest::of_bytes("baseline"),
                    failure_code: None,
                },
            ),
            (
                14,
                "plan",
                4,
                WorkerCommandKind::PlanAccepted {
                    plan_digest: Sha256Digest::of_bytes("plan"),
                },
            ),
        ] {
            state = match journal
                .handle(&request(&state, actor_id, message, key, second, command))
                .expect("advance")
            {
                JournalDisposition::Applied(state) | JournalDisposition::Replay(state) => state,
            };
        }
        assert_eq!(state.phase(), WorkerPhase::Implementing);
        (directory, journal, state, actor_id)
    }

    fn cycle(seed: u8, start: i64) -> CycleContext {
        CycleContext {
            turn_operation_id: Uuid::from_bytes([seed; 16]),
            verification_operation_id: Uuid::from_bytes([seed + 1; 16]),
            planned_at: at(start),
            turn_deadline_at: at(start + 10),
            turn_finished_at: at(start + 1),
            verification_deadline_at: at(start + 11),
            verification_finished_at: at(start + 2),
        }
    }

    fn turn(number: u8, claimed_done: bool) -> TurnOutcome {
        TurnOutcome {
            turn_id: ProtocolKey::new(format!("turn-{number}")).expect("turn"),
            tree: GitObjectId::new(format!("{number:x}").repeat(40)).expect("tree"),
            model_claimed_done: claimed_done,
            sanitized_result_digest: Sha256Digest::of_bytes(format!("turn-{number}-result")),
        }
    }

    #[test]
    fn early_model_done_cannot_bypass_failed_hard_criterion() {
        let (_directory, mut journal, state, actor_id) = implementing_fixture();
        let mut executor =
            FakeTurnExecutor::scripted([(turn(2, true), true), (turn(3, true), false)]);
        let mut verifier = FakeVerifier::scripted([
            VerificationOutcome {
                all_hard_passed: false,
                evidence_digest: Sha256Digest::of_bytes("verification-failed"),
                failure_code: Some(ProtocolKey::new("hard-ac-failed").expect("code")),
            },
            VerificationOutcome {
                all_hard_passed: true,
                evidence_digest: Sha256Digest::of_bytes("verification-passed"),
                failure_code: None,
            },
        ]);
        let mut supervisor = Supervisor::new(&mut journal, &mut executor, &mut verifier, actor_id)
            .expect("supervisor");
        let first = supervisor
            .drive_cycle(
                state.attempt_id(),
                TurnBudget { max_turns: 3 },
                cycle(20, 5),
            )
            .expect("first cycle");
        assert!(matches!(first, CycleOutcome::Continue(_)));
        assert_eq!(first.state().phase(), WorkerPhase::Implementing);
        assert_eq!(first.state().turns_completed(), 1);

        let second = supervisor
            .drive_cycle(
                state.attempt_id(),
                TurnBudget { max_turns: 3 },
                cycle(30, 8),
            )
            .expect("second cycle");
        assert!(matches!(
            second,
            CycleOutcome::Stopped {
                reason: StopReason::LocalCandidateReady,
                ..
            }
        ));
        assert_eq!(second.state().phase(), WorkerPhase::SealingCandidate);
        assert_eq!(second.state().turns_completed(), 2);
        assert_eq!(executor.starts, 2);
        assert_eq!(executor.queries, 1, "lost turn ACK must query, not restart");
        assert_eq!(verifier.runs, 2);
    }

    #[test]
    fn budget_and_expired_lease_stop_before_starting_an_executor() {
        let (_directory, mut journal, state, actor_id) = implementing_fixture();
        let mut executor = FakeTurnExecutor::default();
        let mut verifier = FakeVerifier::default();
        let mut supervisor = Supervisor::new(&mut journal, &mut executor, &mut verifier, actor_id)
            .expect("supervisor");
        let budget = supervisor
            .drive_cycle(
                state.attempt_id(),
                TurnBudget { max_turns: 0 },
                cycle(40, 5),
            )
            .expect("budget stop");
        assert!(matches!(
            budget,
            CycleOutcome::Stopped {
                reason: StopReason::BudgetExhausted,
                ..
            }
        ));
        assert_eq!(executor.starts, 0);

        let (_directory, mut journal, state, actor_id) = implementing_fixture();
        let mut executor = FakeTurnExecutor::default();
        let mut verifier = FakeVerifier::default();
        let mut supervisor = Supervisor::new(&mut journal, &mut executor, &mut verifier, actor_id)
            .expect("supervisor");
        let expired = supervisor
            .drive_cycle(
                state.attempt_id(),
                TurnBudget { max_turns: 3 },
                cycle(50, 100),
            )
            .expect("lease stop");
        assert!(matches!(
            expired,
            CycleOutcome::Stopped {
                reason: StopReason::LeaseLost,
                ..
            }
        ));
        assert_eq!(expired.state().phase(), WorkerPhase::Salvaging);
        assert_eq!(executor.starts, 0);
    }

    #[test]
    fn restart_queries_non_repeatable_turn_and_resumes_pending_verification() {
        let (directory, mut journal, state, actor_id) = implementing_fixture();
        let journal_path = directory.path().join("journal.sqlite3");
        let turn_operation_id = Uuid::from_bytes([70; 16]);
        let turn_request = TurnRequest {
            attempt_id: state.attempt_id(),
            expected_version: state.version(),
            turn_number: 1,
            plan_digest: state.plan_digest().expect("plan"),
            previous_tree: None,
        };
        journal
            .plan_operation(&OperationPlan {
                operation_id: turn_operation_id,
                attempt_id: state.attempt_id(),
                idempotency_key: ProtocolKey::new("turn-1").expect("key"),
                kind: ProtocolKey::new("agent-turn").expect("kind"),
                idempotency_class: OperationIdempotencyClass::NonRepeatable,
                request_digest: digest(&turn_request).expect("digest"),
                planned_at: at(5),
                deadline_at: at(15),
            })
            .expect("durably plan turn");
        drop(journal);

        let mut journal = Journal::open(&journal_path).expect("reopen");
        let mut executor = FakeTurnExecutor::default();
        executor.results.insert(turn_operation_id, turn(7, true));
        let mut verifier = FakeVerifier::default();
        let recovered = {
            let mut supervisor =
                Supervisor::new(&mut journal, &mut executor, &mut verifier, actor_id)
                    .expect("supervisor");
            supervisor
                .drive_cycle(
                    state.attempt_id(),
                    TurnBudget { max_turns: 3 },
                    cycle(80, 5),
                )
                .expect("recover turn")
        };
        assert!(matches!(recovered, CycleOutcome::Continue(_)));
        assert_eq!(recovered.state().phase(), WorkerPhase::LocalVerifying);
        assert_eq!(executor.starts, 0, "a recovered turn must never restart");
        assert_eq!(executor.queries, 1);
        assert_eq!(verifier.runs, 0);
        assert!(
            journal
                .pending_operations(state.attempt_id())
                .expect("pending")
                .is_empty()
        );

        let state = recovered.state();
        let verification_operation_id = Uuid::from_bytes([71; 16]);
        let verification_request = VerificationRequest {
            attempt_id: state.attempt_id(),
            expected_version: state.version(),
            tree: state.current_tree().cloned().expect("tree"),
        };
        journal
            .plan_operation(&OperationPlan {
                operation_id: verification_operation_id,
                attempt_id: state.attempt_id(),
                idempotency_key: ProtocolKey::new("verify-turn-1").expect("key"),
                kind: ProtocolKey::new("local-verification").expect("kind"),
                idempotency_class: OperationIdempotencyClass::Idempotent,
                request_digest: digest(&verification_request).expect("digest"),
                planned_at: at(6),
                deadline_at: at(20),
            })
            .expect("durably plan verification");
        drop(journal);

        let mut journal = Journal::open(&journal_path).expect("reopen again");
        let mut executor = FakeTurnExecutor::default();
        let mut verifier = FakeVerifier::scripted([VerificationOutcome {
            all_hard_passed: true,
            evidence_digest: Sha256Digest::of_bytes("recovered-verification"),
            failure_code: None,
        }]);
        let completed = Supervisor::new(&mut journal, &mut executor, &mut verifier, actor_id)
            .expect("supervisor")
            .drive_cycle(
                state.attempt_id(),
                TurnBudget { max_turns: 3 },
                cycle(90, 7),
            )
            .expect("recover verification");
        assert!(matches!(
            completed,
            CycleOutcome::Stopped {
                reason: StopReason::LocalCandidateReady,
                ..
            }
        ));
        assert_eq!(completed.state().phase(), WorkerPhase::SealingCandidate);
        assert_eq!(executor.starts, 0);
        assert_eq!(verifier.runs, 1);
        assert!(
            journal
                .pending_operations(state.attempt_id())
                .expect("pending")
                .is_empty()
        );
    }
}
