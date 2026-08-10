//! Explicit deterministic MVP driver used only when `driver_mode=fixture`.

use std::collections::BTreeSet;

use agentforge_domain::{
    ActorId, AttemptId, GitObjectId, IdempotencyKey, ProtocolKey, ServerInstant, Sha256Digest,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    journal::{Journal, JournalCommand, JournalError, JournalRequest},
    runtime::{WorkerAttemptState, WorkerCommandEnvelope, WorkerCommandKind, WorkerPhase},
    supervisor::{
        CycleContext, CycleOutcome, ExecutionError, LocalVerifier, Supervisor, SupervisorError,
        TurnBudget, TurnExecutor, TurnOutcome, TurnRequest, VerificationOutcome,
        VerificationRequest,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixtureDriveIds {
    pub preparation: Uuid,
    pub workspace: Uuid,
    pub baseline: Uuid,
    pub plan: Uuid,
    pub turn_operation: Uuid,
    pub verification_operation: Uuid,
}

impl FixtureDriveIds {
    fn validate(self) -> FixtureDriverResult<()> {
        let values = [
            self.preparation,
            self.workspace,
            self.baseline,
            self.plan,
            self.turn_operation,
            self.verification_operation,
        ];
        let unique = values.into_iter().collect::<BTreeSet<_>>();
        if unique.len() != values.len() || unique.iter().any(Uuid::is_nil) {
            return Err(FixtureDriverError::InvalidContext);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixtureDriveContext {
    pub observed_at: ServerInstant,
    pub operation_timeout_seconds: u32,
    pub ids: FixtureDriveIds,
}

impl FixtureDriveContext {
    fn validate(self) -> FixtureDriverResult<()> {
        self.ids.validate()?;
        if !(1..=3_600).contains(&self.operation_timeout_seconds) {
            return Err(FixtureDriverError::InvalidContext);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureDriveOutcome {
    pub state: WorkerAttemptState,
    pub cycle_executed: bool,
    pub local_candidate_ready: bool,
}

#[derive(Debug, Error)]
pub enum FixtureDriverError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Supervisor(#[from] SupervisorError),
    #[error("fixture driver context is invalid")]
    InvalidContext,
    #[error("fixture driver supports only SHA-1 Git object repositories")]
    UnsupportedGitObjectFormat,
}

impl FixtureDriverError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Journal(error) => error.code(),
            Self::Supervisor(error) => error.code(),
            Self::InvalidContext => "AF_WORKER_FIXTURE_CONTEXT_INVALID",
            Self::UnsupportedGitObjectFormat => "AF_WORKER_FIXTURE_GIT_FORMAT_UNSUPPORTED",
        }
    }
}

pub type FixtureDriverResult<T> = Result<T, FixtureDriverError>;

pub fn drive_fixture_attempt(
    journal: &mut Journal,
    actor_id: ActorId,
    attempt_id: AttemptId,
    max_turns: u32,
    context: FixtureDriveContext,
) -> FixtureDriverResult<FixtureDriveOutcome> {
    context.validate()?;
    if actor_id.as_uuid().is_nil()
        || attempt_id.as_uuid().is_nil()
        || !(1..=100).contains(&max_turns)
    {
        return Err(FixtureDriverError::InvalidContext);
    }
    let execution = journal
        .load_execution_snapshot(attempt_id)?
        .ok_or(FixtureDriverError::InvalidContext)?;
    if execution.base_commit.as_str().len() != 40 {
        return Err(FixtureDriverError::UnsupportedGitObjectFormat);
    }
    let mut state = journal
        .load_attempt(attempt_id)?
        .ok_or(FixtureDriverError::InvalidContext)?;

    loop {
        let (message_id, label, command) = match state.phase() {
            WorkerPhase::Granted => (
                context.ids.preparation,
                "prepare",
                WorkerCommandKind::BeginPreparation,
            ),
            WorkerPhase::Preparing => (
                context.ids.workspace,
                "workspace",
                WorkerCommandKind::WorkspacePrepared {
                    workspace_digest: fixture_digest("workspace", &state, execution.package_hash),
                },
            ),
            WorkerPhase::Baseline => (
                context.ids.baseline,
                "baseline",
                WorkerCommandKind::BaselineFinished {
                    passed: true,
                    evidence_digest: fixture_digest("baseline", &state, execution.package_hash),
                    failure_code: None,
                },
            ),
            WorkerPhase::Planning => (
                context.ids.plan,
                "plan",
                WorkerCommandKind::PlanAccepted {
                    plan_digest: fixture_digest("plan", &state, execution.package_hash),
                },
            ),
            _ => break,
        };
        state = apply_local(
            journal,
            actor_id,
            message_id,
            &state,
            context.observed_at,
            label,
            command,
        )?;
    }

    if !matches!(
        state.phase(),
        WorkerPhase::Implementing | WorkerPhase::LocalVerifying
    ) {
        return Ok(FixtureDriveOutcome {
            local_candidate_ready: state.phase() == WorkerPhase::SealingCandidate,
            state,
            cycle_executed: false,
        });
    }

    let cycle = cycle_context(journal, &state, context)?;
    let mut executor = FixtureTurnExecutor;
    let mut verifier = FixtureVerifier;
    let outcome = Supervisor::new(journal, &mut executor, &mut verifier, actor_id)?.drive_cycle(
        state.attempt_id(),
        TurnBudget { max_turns },
        cycle,
    )?;
    let state = outcome.state().clone();
    Ok(FixtureDriveOutcome {
        local_candidate_ready: matches!(
            outcome,
            CycleOutcome::Stopped {
                reason: crate::supervisor::StopReason::LocalCandidateReady,
                ..
            }
        ),
        state,
        cycle_executed: true,
    })
}

fn cycle_context(
    journal: &Journal,
    state: &WorkerAttemptState,
    context: FixtureDriveContext,
) -> FixtureDriverResult<CycleContext> {
    let pending = journal.pending_operations(state.attempt_id())?;
    if pending.len() > 1 {
        return Err(FixtureDriverError::InvalidContext);
    }
    let timeout = time::Duration::seconds(i64::from(context.operation_timeout_seconds));
    let fresh = || CycleContext {
        turn_operation_id: context.ids.turn_operation,
        verification_operation_id: context.ids.verification_operation,
        planned_at: context.observed_at,
        turn_deadline_at: ServerInstant(context.observed_at.0 + timeout),
        turn_finished_at: context.observed_at,
        verification_deadline_at: ServerInstant(context.observed_at.0 + timeout),
        verification_finished_at: context.observed_at,
    };
    let Some(pending) = pending.first() else {
        return Ok(fresh());
    };
    if context.observed_at > pending.plan.deadline_at {
        return Err(FixtureDriverError::Supervisor(
            SupervisorError::RecoveryConflict,
        ));
    }
    match pending.plan.kind.as_str() {
        "agent-turn" => Ok(CycleContext {
            turn_operation_id: pending.plan.operation_id,
            verification_operation_id: context.ids.verification_operation,
            planned_at: pending.plan.planned_at,
            turn_deadline_at: pending.plan.deadline_at,
            turn_finished_at: context.observed_at,
            verification_deadline_at: ServerInstant(context.observed_at.0 + timeout),
            verification_finished_at: context.observed_at,
        }),
        "local-verification" => Ok(CycleContext {
            turn_operation_id: context.ids.turn_operation,
            verification_operation_id: pending.plan.operation_id,
            planned_at: pending.plan.planned_at,
            turn_deadline_at: pending.plan.deadline_at,
            turn_finished_at: pending.plan.planned_at,
            verification_deadline_at: pending.plan.deadline_at,
            verification_finished_at: context.observed_at,
        }),
        _ => Err(FixtureDriverError::Supervisor(
            SupervisorError::RecoveryConflict,
        )),
    }
}

fn apply_local(
    journal: &mut Journal,
    actor_id: ActorId,
    message_id: Uuid,
    state: &WorkerAttemptState,
    observed_at: ServerInstant,
    label: &str,
    command: WorkerCommandKind,
) -> FixtureDriverResult<WorkerAttemptState> {
    let key = IdempotencyKey::new(format!(
        "fixture-{label}:{}:v{}",
        state.attempt_id(),
        state.version().get()
    ))
    .map_err(|_| FixtureDriverError::InvalidContext)?;
    Ok(journal
        .handle(&JournalRequest {
            message_id,
            actor_id,
            idempotency_key: key,
            command: JournalCommand::Apply {
                attempt_id: state.attempt_id(),
                command: WorkerCommandEnvelope {
                    expected_version: state.version(),
                    observed_at,
                    command,
                },
            },
        })?
        .state()
        .clone())
}

fn fixture_digest(
    label: &str,
    state: &WorkerAttemptState,
    package_hash: Sha256Digest,
) -> Sha256Digest {
    Sha256Digest::of_bytes(format!(
        "agentforge-fixture-driver:v1\n{label}\n{}\n{}\n{}",
        state.attempt_id(),
        state.base_commit(),
        package_hash
    ))
}

#[derive(Clone, Copy, Debug, Default)]
struct FixtureTurnExecutor;

impl TurnExecutor for FixtureTurnExecutor {
    fn start(
        &mut self,
        operation_id: Uuid,
        _request: &TurnRequest,
    ) -> Result<TurnOutcome, ExecutionError> {
        Ok(fixture_turn_outcome(operation_id))
    }

    fn query(&mut self, operation_id: Uuid) -> Result<Option<TurnOutcome>, ExecutionError> {
        Ok(Some(fixture_turn_outcome(operation_id)))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FixtureVerifier;

impl LocalVerifier for FixtureVerifier {
    fn verify(
        &mut self,
        operation_id: Uuid,
        _request: &VerificationRequest,
    ) -> Result<VerificationOutcome, ExecutionError> {
        Ok(VerificationOutcome {
            all_hard_passed: true,
            evidence_digest: Sha256Digest::of_bytes(format!(
                "agentforge-fixture-verification:v1\n{operation_id}"
            )),
            failure_code: None,
        })
    }
}

fn fixture_turn_outcome(operation_id: Uuid) -> TurnOutcome {
    let digest = Sha256Digest::of_bytes(format!("agentforge-fixture-turn:v1\n{operation_id}"));
    let hex = digest.to_string();
    let hex = hex.strip_prefix("sha256:").expect("digest prefix");
    TurnOutcome {
        turn_id: ProtocolKey::new(format!(
            "fixture-{}",
            &operation_id.simple().to_string()[..16]
        ))
        .expect("fixture key"),
        tree: GitObjectId::new(hex[..40].to_owned()).expect("SHA-1 shaped fixture object"),
        model_claimed_done: true,
        sanitized_result_digest: digest,
    }
}

#[cfg(test)]
mod tests {
    use agentforge_application::PackageExecutionSnapshot;
    use agentforge_domain::{FencingToken, LeaseId, PackageId, PackageRevision};
    use time::macros::datetime;

    use super::*;
    use crate::{
        journal::{JournalDisposition, OperationIdempotencyClass, OperationPlan},
        runtime::AttemptGrant,
    };

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(second: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(second))
    }

    fn ids(seed: u8) -> FixtureDriveIds {
        FixtureDriveIds {
            preparation: Uuid::from_bytes([seed; 16]),
            workspace: Uuid::from_bytes([seed + 1; 16]),
            baseline: Uuid::from_bytes([seed + 2; 16]),
            plan: Uuid::from_bytes([seed + 3; 16]),
            turn_operation: Uuid::from_bytes([seed + 4; 16]),
            verification_operation: Uuid::from_bytes([seed + 5; 16]),
        }
    }

    fn granted(base_commit: &str) -> (tempfile::TempDir, Journal, WorkerAttemptState, ActorId) {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut journal = Journal::open(directory.path().join("journal.sqlite3")).expect("journal");
        let actor_id = id(9);
        let canonical_document = serde_json::json!({"package": "fixture-driver"});
        let execution = PackageExecutionSnapshot {
            revision: PackageRevision::new(1).expect("revision"),
            package_hash: Sha256Digest::of_bytes(
                serde_json_canonicalizer::to_vec(&canonical_document).expect("JCS"),
            ),
            base_commit: GitObjectId::new(base_commit).expect("base commit"),
            git_object_format: if base_commit.len() == 40 {
                "sha1".to_owned()
            } else {
                "sha256".to_owned()
            },
            canonical_document,
            input_snapshot: serde_json::json!({"fixtures": []}),
        };
        let state = match journal
            .handle(&JournalRequest {
                message_id: Uuid::from_bytes([10; 16]),
                actor_id,
                idempotency_key: IdempotencyKey::new("fixture-grant").expect("key"),
                command: JournalCommand::Grant {
                    grant: AttemptGrant {
                        attempt_id: id(1),
                        package_id: id::<PackageId>(2),
                        package_revision: PackageRevision::new(1).expect("revision"),
                        package_hash: execution.package_hash,
                        base_commit: execution.base_commit.clone(),
                        lease_id: id::<LeaseId>(3),
                        lease_generation: FencingToken::new(1).expect("generation"),
                        lease_expires_at: at(600),
                        granted_at: at(0),
                    },
                    execution,
                },
            })
            .expect("grant")
        {
            JournalDisposition::Applied(state) | JournalDisposition::Replay(state) => state,
        };
        (directory, journal, state, actor_id)
    }

    #[test]
    fn fixture_driver_reaches_local_candidate_and_is_restart_idempotent() {
        let (_directory, mut journal, state, actor_id) = granted(&"1".repeat(40));
        let first = drive_fixture_attempt(
            &mut journal,
            actor_id,
            state.attempt_id(),
            3,
            FixtureDriveContext {
                observed_at: at(1),
                operation_timeout_seconds: 60,
                ids: ids(20),
            },
        )
        .expect("drive");
        assert!(first.cycle_executed);
        assert!(first.local_candidate_ready);
        assert_eq!(first.state.phase(), WorkerPhase::SealingCandidate);
        assert_eq!(first.state.turns_completed(), 1);
        assert!(
            journal
                .pending_operations(state.attempt_id())
                .expect("pending")
                .is_empty()
        );

        let replay = drive_fixture_attempt(
            &mut journal,
            actor_id,
            state.attempt_id(),
            3,
            FixtureDriveContext {
                observed_at: at(2),
                operation_timeout_seconds: 60,
                ids: ids(40),
            },
        )
        .expect("ready replay");
        assert!(!replay.cycle_executed);
        assert!(replay.local_candidate_ready);
        assert_eq!(replay.state, first.state);
    }

    #[test]
    fn fixture_driver_rejects_sha256_repository_before_advancing_attempt() {
        let (_directory, mut journal, state, actor_id) = granted(&"2".repeat(64));
        let error = drive_fixture_attempt(
            &mut journal,
            actor_id,
            state.attempt_id(),
            3,
            FixtureDriveContext {
                observed_at: at(1),
                operation_timeout_seconds: 60,
                ids: ids(60),
            },
        )
        .expect_err("SHA-256 repository");
        assert_eq!(error.code(), "AF_WORKER_FIXTURE_GIT_FORMAT_UNSUPPORTED");
        assert_eq!(
            journal
                .load_attempt(state.attempt_id())
                .expect("load")
                .expect("attempt")
                .phase(),
            WorkerPhase::Granted
        );
    }

    #[test]
    fn fixture_executor_query_is_stable_without_process_memory() {
        let operation_id = Uuid::from_bytes([90; 16]);
        let expected = fixture_turn_outcome(operation_id);
        let mut executor = FixtureTurnExecutor;
        assert_eq!(
            executor.query(operation_id).expect("query"),
            Some(expected.clone())
        );
        assert_eq!(
            executor
                .start(
                    operation_id,
                    &TurnRequest {
                        attempt_id: id(1),
                        expected_version: agentforge_domain::AggregateVersion::new(1),
                        turn_number: 1,
                        plan_digest: Sha256Digest::of_bytes("plan"),
                        previous_tree: None,
                    },
                )
                .expect("start"),
            expected
        );
    }

    #[test]
    fn fixture_driver_recovers_a_planned_turn_after_journal_reopen() {
        let (directory, mut journal, mut state, actor_id) = granted(&"3".repeat(40));
        let package_hash = state.package_hash();
        for (message_id, label, command) in [
            (
                Uuid::from_bytes([100; 16]),
                "prepare",
                WorkerCommandKind::BeginPreparation,
            ),
            (
                Uuid::from_bytes([101; 16]),
                "workspace",
                WorkerCommandKind::WorkspacePrepared {
                    workspace_digest: fixture_digest("workspace", &state, package_hash),
                },
            ),
            (
                Uuid::from_bytes([102; 16]),
                "baseline",
                WorkerCommandKind::BaselineFinished {
                    passed: true,
                    evidence_digest: fixture_digest("baseline", &state, package_hash),
                    failure_code: None,
                },
            ),
            (
                Uuid::from_bytes([103; 16]),
                "plan",
                WorkerCommandKind::PlanAccepted {
                    plan_digest: fixture_digest("plan", &state, package_hash),
                },
            ),
        ] {
            state = apply_local(
                &mut journal,
                actor_id,
                message_id,
                &state,
                at(1),
                label,
                command,
            )
            .expect("advance fixture phase");
        }
        assert_eq!(state.phase(), WorkerPhase::Implementing);

        let operation_id = Uuid::from_bytes([104; 16]);
        let request = TurnRequest {
            attempt_id: state.attempt_id(),
            expected_version: state.version(),
            turn_number: 1,
            plan_digest: state.plan_digest().expect("plan"),
            previous_tree: None,
        };
        journal
            .plan_operation(&OperationPlan {
                operation_id,
                attempt_id: state.attempt_id(),
                idempotency_key: ProtocolKey::new("turn-1").expect("key"),
                kind: ProtocolKey::new("agent-turn").expect("kind"),
                idempotency_class: OperationIdempotencyClass::NonRepeatable,
                request_digest: Sha256Digest::of_bytes(
                    serde_json_canonicalizer::to_vec(&request).expect("JCS"),
                ),
                planned_at: at(1),
                deadline_at: at(60),
            })
            .expect("plan operation");
        let attempt_id = state.attempt_id();
        let path = directory.path().join("journal.sqlite3");
        drop(journal);

        let mut journal = Journal::open(path).expect("reopen journal");
        let recovered = drive_fixture_attempt(
            &mut journal,
            actor_id,
            attempt_id,
            3,
            FixtureDriveContext {
                observed_at: at(2),
                operation_timeout_seconds: 60,
                ids: ids(110),
            },
        )
        .expect("recover planned turn");
        assert_eq!(recovered.state.phase(), WorkerPhase::LocalVerifying);
        assert_eq!(
            recovered.state.current_tree(),
            Some(&fixture_turn_outcome(operation_id).tree)
        );

        let verified = drive_fixture_attempt(
            &mut journal,
            actor_id,
            attempt_id,
            3,
            FixtureDriveContext {
                observed_at: at(3),
                operation_timeout_seconds: 60,
                ids: ids(120),
            },
        )
        .expect("verify recovered turn");
        assert!(verified.local_candidate_ready);
        assert_eq!(verified.state.phase(), WorkerPhase::SealingCandidate);
    }
}
