//! Worker daemon composition root and deterministic scheduling loop.

use std::{future::Future, sync::Arc, time::Duration};

use agentforge_application::{
    AttemptProgressStage, AttemptProgressView, CompleteCandidateArtifactInput,
    InitCandidateArtifactInput, MvpCommand, MvpCommandContext, ReportAttemptProgressInput,
    UploadCandidateArtifactChunkInput,
};
use agentforge_domain::{
    AggregateVersion, AttemptId, CommandId, CorrelationId, GitObjectId, IdempotencyKey, ProjectId,
    ProtocolKey, ServerInstant, Sha256Digest,
};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::{
    config::{ConfigError, WorkerDaemonConfig, WorkerDriverMode},
    fixture_driver::{
        FixtureDriveContext, FixtureDriveIds, FixtureDriverError, drive_fixture_attempt,
    },
    journal::{
        AttemptProgressCommandIntentRecord, CandidateArtifactCommandIntentRecord,
        CandidateArtifactCommandResponse, CandidateArtifactControlCommand, Journal, JournalCommand,
        JournalError, JournalRequest,
    },
    lifecycle::{
        ClaimIntent, LeaseMaintenanceIntent, LeaseMaintenanceOutcome, LifecycleError,
        WorkerControlPlane, claim_offer, execute_attempt_progress_command_intent,
        execute_candidate_artifact_command_intent, maintain_attempt, poll_offer,
        resume_claim_intent, resume_lease_command_intent,
    },
    runtime::{CandidateSnapshot, WorkerCommandEnvelope, WorkerCommandKind, WorkerPhase},
};

/// Trusted time and identifier boundary. Tests inject a deterministic source;
/// production obtains both values only here, never inside domain transitions.
pub trait DaemonRuntime: Send {
    fn now(&mut self) -> ServerInstant;
    fn next_uuid(&mut self) -> Uuid;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDaemonRuntime;

impl DaemonRuntime for SystemDaemonRuntime {
    fn now(&mut self) -> ServerInstant {
        ServerInstant(OffsetDateTime::now_utc())
    }

    fn next_uuid(&mut self) -> Uuid {
        Uuid::now_v7()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DaemonTickReport {
    pub resumed_claims: u16,
    pub resumed_progress_commands: u16,
    pub resumed_artifact_commands: u16,
    pub resumed_lease_commands: u16,
    pub artifact_commands_completed: u16,
    pub candidate_artifacts_completed: u16,
    pub progress_commands_completed: u16,
    pub maintenance_noops: u16,
    pub renewed: u16,
    pub released: u16,
    pub stopped: u16,
    pub claimed: u16,
    pub active_attempts: u16,
    pub driven_attempts: u16,
    pub local_candidates_ready: u16,
}

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    FixtureDriver(#[from] FixtureDriverError),
    #[error("daemon runtime produced an invalid identifier")]
    InvalidRuntimeId,
    #[error("attempt {0} has no durable Project binding")]
    MissingProjectBinding(AttemptId),
    #[error("attempt {0} has an invalid fixture Candidate Artifact command history")]
    InvalidArtifactWorkflow(AttemptId),
    #[error("attempt {0} has an invalid central Attempt progress history")]
    InvalidProgressWorkflow(AttemptId),
    #[error("daemon counter exceeded its bounded representation")]
    CounterOverflow,
}

impl DaemonError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Config(error) => error.code(),
            Self::Journal(error) => error.code(),
            Self::Lifecycle(error) => error.code(),
            Self::FixtureDriver(error) => error.code(),
            Self::InvalidRuntimeId => "AF_WORKER_RUNTIME_ID_INVALID",
            Self::MissingProjectBinding(_) => "AF_WORKER_PROJECT_BINDING_MISSING",
            Self::InvalidArtifactWorkflow(_) => "AF_WORKER_ARTIFACT_WORKFLOW_INVALID",
            Self::InvalidProgressWorkflow(_) => "AF_WORKER_PROGRESS_WORKFLOW_INVALID",
            Self::CounterOverflow => "AF_WORKER_COUNTER_OVERFLOW",
        }
    }
}

pub type DaemonResult<T> = Result<T, DaemonError>;

/// Owns the single-writer Journal and drives all remote mutations through the
/// durable lifecycle intents. One failed step aborts the tick; its pending
/// intent remains the first action on the next tick or process restart.
pub struct WorkerDaemon<R> {
    control: Arc<dyn WorkerControlPlane>,
    journal: Journal,
    config: WorkerDaemonConfig,
    runtime: R,
}

impl<R: DaemonRuntime> WorkerDaemon<R> {
    pub fn open(
        control: Arc<dyn WorkerControlPlane>,
        config: WorkerDaemonConfig,
        runtime: R,
    ) -> DaemonResult<Self> {
        config.validate()?;
        let journal = Journal::open(&config.journal_path)?;
        Self::from_parts(control, journal, config, runtime)
    }

    pub fn from_parts(
        control: Arc<dyn WorkerControlPlane>,
        journal: Journal,
        config: WorkerDaemonConfig,
        runtime: R,
    ) -> DaemonResult<Self> {
        config.validate()?;
        Ok(Self {
            control,
            journal,
            config,
            runtime,
        })
    }

    #[must_use]
    pub const fn config(&self) -> &WorkerDaemonConfig {
        &self.config
    }

    #[must_use]
    pub const fn journal(&self) -> &Journal {
        &self.journal
    }

    /// Executes one deterministic scheduling turn:
    ///
    /// 1. replay pending Claim mutations;
    /// 2. replay pending central Attempt Progress mutations;
    /// 3. replay pending Candidate Artifact mutations;
    /// 4. replay pending Renew/Release mutations;
    /// 5. reconcile and maintain every locally owned Lease;
    /// 6. drive fixture work, central Progress, then deterministic Artifact upload;
    /// 7. poll and Claim only the remaining execution capacity.
    pub async fn tick(&mut self) -> DaemonResult<DaemonTickReport> {
        let observed_at = self.runtime.now();
        let mut report = DaemonTickReport::default();

        for record in self.journal.pending_claim_intents()? {
            resume_claim_intent(self.control.as_ref(), &mut self.journal, &record).await?;
            increment(&mut report.resumed_claims)?;
        }
        for record in self.journal.pending_attempt_progress_command_intents()? {
            execute_attempt_progress_command_intent(
                self.control.as_ref(),
                &mut self.journal,
                &record,
            )
            .await?;
            increment(&mut report.resumed_progress_commands)?;
            increment(&mut report.progress_commands_completed)?;
        }
        for record in self.journal.pending_candidate_artifact_command_intents()? {
            let response = execute_candidate_artifact_command_intent(
                self.control.as_ref(),
                &mut self.journal,
                &record,
                observed_at,
            )
            .await?;
            increment(&mut report.resumed_artifact_commands)?;
            record_artifact_response(&mut report, &response)?;
        }
        for record in self.journal.pending_lease_command_intents()? {
            let outcome =
                resume_lease_command_intent(self.control.as_ref(), &mut self.journal, &record)
                    .await?;
            increment(&mut report.resumed_lease_commands)?;
            record_maintenance_outcome(&mut report, &outcome)?;
        }

        for state in self.journal.lease_maintenance_attempts()? {
            let project_id = self
                .journal
                .project_for_attempt(state.attempt_id())?
                .ok_or(DaemonError::MissingProjectBinding(state.attempt_id()))?;
            if !self.config.project_ids.contains(&project_id) {
                return Err(DaemonError::MissingProjectBinding(state.attempt_id()));
            }
            let intent = self.maintenance_intent(project_id, state.attempt_id(), observed_at)?;
            let outcome = maintain_attempt(
                self.control.as_ref(),
                &mut self.journal,
                self.config.identity(),
                self.config.lease_policy(),
                &intent,
            )
            .await?;
            record_maintenance_outcome(&mut report, &outcome)?;
        }

        if self.config.driver_mode == WorkerDriverMode::Fixture {
            let runnable = self
                .journal
                .recover_nonterminal()?
                .into_iter()
                .filter(|state| {
                    matches!(
                        state.phase(),
                        WorkerPhase::Granted
                            | WorkerPhase::Preparing
                            | WorkerPhase::Baseline
                            | WorkerPhase::Planning
                            | WorkerPhase::Implementing
                            | WorkerPhase::LocalVerifying
                            | WorkerPhase::SealingCandidate
                    )
                })
                .collect::<Vec<_>>();
            for state in runnable {
                let context = FixtureDriveContext {
                    observed_at,
                    operation_timeout_seconds: self.config.operation_timeout_seconds,
                    ids: self.fixture_drive_ids()?,
                };
                let outcome = drive_fixture_attempt(
                    &mut self.journal,
                    self.config.actor_id,
                    state.attempt_id(),
                    self.config.max_turns,
                    context,
                )?;
                if outcome.cycle_executed {
                    increment(&mut report.driven_attempts)?;
                }
                if outcome.local_candidate_ready {
                    increment(&mut report.local_candidates_ready)?;
                }
                if outcome.state.phase() == WorkerPhase::SealingCandidate {
                    self.seal_fixture_candidate(&outcome.state, observed_at)?;
                }
            }

            let handing_off = self
                .journal
                .recover_nonterminal()?
                .into_iter()
                .filter(|state| state.phase() == WorkerPhase::HandingOffCandidate)
                .collect::<Vec<_>>();
            for state in handing_off {
                let progress = self
                    .drive_fixture_attempt_progress(&state, observed_at, &mut report)
                    .await?;
                self.drive_fixture_candidate_artifact(
                    &state,
                    progress.version,
                    observed_at,
                    &mut report,
                )
                .await?;
            }
        }

        let active = self
            .journal
            .recover_nonterminal()?
            .into_iter()
            .filter(|state| state.phase() != WorkerPhase::Salvaging)
            .count();
        report.active_attempts = u16::try_from(active).map_err(|_| DaemonError::CounterOverflow)?;
        let mut available = self.config.capacity.saturating_sub(report.active_attempts);
        let projects = self.config.project_ids.clone();

        while available > 0 {
            let mut claimed_in_round = false;
            for project_id in projects.iter().copied() {
                if available == 0 {
                    break;
                }
                let Some(offer) =
                    poll_offer(self.control.as_ref(), project_id, self.config.offer_limit).await?
                else {
                    continue;
                };
                let intent = self.claim_intent(offer, observed_at)?;
                claim_offer(
                    self.control.as_ref(),
                    &mut self.journal,
                    self.config.identity(),
                    &intent,
                )
                .await?;
                increment(&mut report.claimed)?;
                increment(&mut report.active_attempts)?;
                available -= 1;
                claimed_in_round = true;
            }
            if !claimed_in_round {
                break;
            }
        }
        Ok(report)
    }

    /// Runs ticks with delay semantics: a slow control-plane call never causes
    /// a burst of catch-up mutations. Shutdown is observed between in-flight
    /// calls; a process crash is recovered through the durable intent ledger.
    pub async fn run_until_shutdown<F>(&mut self, shutdown: F) -> DaemonResult<()>
    where
        F: Future<Output = ()> + Send,
    {
        let mut interval =
            tokio::time::interval(Duration::from_secs(u64::from(self.config.tick_seconds)));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = interval.tick() => {
                    self.tick().await?;
                }
            }
        }
    }

    fn claim_intent(
        &mut self,
        offer: agentforge_application::OfferView,
        created_at: ServerInstant,
    ) -> DaemonResult<ClaimIntent> {
        let intent_id = self.next_uuid()?;
        Ok(ClaimIntent {
            offer,
            command_id: CommandId::from(self.next_uuid()?),
            correlation_id: CorrelationId::from(self.next_uuid()?),
            idempotency_key: idempotency_key("worker-claim", intent_id)?,
            local_message_id: intent_id,
            lease_seconds: self.config.lease_seconds,
            max_lease_seconds: self.config.max_lease_seconds,
            created_at,
        })
    }

    fn maintenance_intent(
        &mut self,
        project_id: ProjectId,
        attempt_id: AttemptId,
        observed_at: ServerInstant,
    ) -> DaemonResult<LeaseMaintenanceIntent> {
        let intent_id = self.next_uuid()?;
        Ok(LeaseMaintenanceIntent {
            project_id,
            attempt_id,
            intent_id,
            command_id: CommandId::from(self.next_uuid()?),
            correlation_id: CorrelationId::from(self.next_uuid()?),
            idempotency_key: idempotency_key("worker-lease", intent_id)?,
            observed_at,
        })
    }

    fn fixture_drive_ids(&mut self) -> DaemonResult<FixtureDriveIds> {
        Ok(FixtureDriveIds {
            preparation: self.next_uuid()?,
            workspace: self.next_uuid()?,
            baseline: self.next_uuid()?,
            plan: self.next_uuid()?,
            turn_operation: self.next_uuid()?,
            verification_operation: self.next_uuid()?,
        })
    }

    fn seal_fixture_candidate(
        &mut self,
        state: &crate::runtime::WorkerAttemptState,
        observed_at: ServerInstant,
    ) -> DaemonResult<()> {
        let tree = state
            .current_tree()
            .cloned()
            .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
        let author_evidence_digest = state
            .last_verification_digest()
            .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
        let candidate_commit = fixture_candidate_commit(state.attempt_id(), &tree)?;
        let message_id = self.next_uuid()?;
        self.journal.handle(&JournalRequest {
            message_id,
            actor_id: self.config.actor_id,
            idempotency_key: idempotency_key("fixture-seal", message_id)?,
            command: JournalCommand::Apply {
                attempt_id: state.attempt_id(),
                command: WorkerCommandEnvelope {
                    expected_version: state.version(),
                    observed_at,
                    command: WorkerCommandKind::SealCandidate {
                        candidate: CandidateSnapshot {
                            commit: candidate_commit,
                            tree,
                            author_evidence_digest,
                        },
                        observed_generation: state.lease_generation(),
                    },
                },
            },
        })?;
        Ok(())
    }

    async fn drive_fixture_attempt_progress(
        &mut self,
        state: &crate::runtime::WorkerAttemptState,
        observed_at: ServerInstant,
        report: &mut DaemonTickReport,
    ) -> DaemonResult<AttemptProgressView> {
        let claimed = self
            .journal
            .claimed_work_for_attempt(state.attempt_id())?
            .ok_or(DaemonError::InvalidProgressWorkflow(state.attempt_id()))?;
        let stages = [
            AttemptProgressStage::Preparing,
            AttemptProgressStage::Planning,
            AttemptProgressStage::Implementing,
            AttemptProgressStage::LocalVerify,
        ];
        let history = self
            .journal
            .attempt_progress_command_history(state.attempt_id())?;
        if history.len() > stages.len() {
            return Err(DaemonError::InvalidProgressWorkflow(state.attempt_id()));
        }

        let mut expected_version = claimed.attempt_version;
        let mut last_response = None;
        for (index, entry) in history.iter().enumerate() {
            let response = entry
                .response
                .ok_or(DaemonError::InvalidProgressWorkflow(state.attempt_id()))?;
            if entry.record.command.input.stage != stages[index]
                || entry.record.command.context.expected_version != Some(expected_version)
            {
                return Err(DaemonError::InvalidProgressWorkflow(state.attempt_id()));
            }
            expected_version = response.version;
            last_response = Some(response);
        }

        for stage in stages.into_iter().skip(history.len()) {
            let record = self.fixture_attempt_progress_intent(
                state,
                &claimed,
                stage,
                expected_version,
                observed_at,
            )?;
            let response = execute_attempt_progress_command_intent(
                self.control.as_ref(),
                &mut self.journal,
                &record,
            )
            .await?;
            increment(&mut report.progress_commands_completed)?;
            expected_version = response.version;
            last_response = Some(response);
        }

        let response =
            last_response.ok_or(DaemonError::InvalidProgressWorkflow(state.attempt_id()))?;
        if response.state != agentforge_domain::attempt::AttemptState::LocalVerify
            || response.semantic_progress_seq != 4
        {
            return Err(DaemonError::InvalidProgressWorkflow(state.attempt_id()));
        }
        Ok(response)
    }

    fn fixture_attempt_progress_intent(
        &mut self,
        state: &crate::runtime::WorkerAttemptState,
        claimed: &agentforge_application::ClaimedWork,
        stage: AttemptProgressStage,
        expected_version: AggregateVersion,
        created_at: ServerInstant,
    ) -> DaemonResult<AttemptProgressCommandIntentRecord> {
        let intent_id = self.next_uuid()?;
        Ok(AttemptProgressCommandIntentRecord {
            intent_id,
            attempt_id: state.attempt_id(),
            command: MvpCommand {
                context: MvpCommandContext {
                    command_id: CommandId::from(self.next_uuid()?),
                    actor_id: self.config.actor_id,
                    idempotency_key: idempotency_key("worker-attempt-progress", intent_id)?,
                    correlation_id: CorrelationId::from(self.next_uuid()?),
                    causation_id: None,
                    expected_version: Some(expected_version),
                },
                input: ReportAttemptProgressInput {
                    project_id: claimed.project_id,
                    attempt_id: state.attempt_id(),
                    lease_id: state.lease_id(),
                    node_id: self.config.node_id,
                    fencing_token: state.lease_generation(),
                    stage,
                    evidence_digest: fixture_progress_evidence(
                        state,
                        stage,
                        self.config.runtime_fingerprint,
                    )?,
                },
            },
            created_at,
        })
    }

    async fn drive_fixture_candidate_artifact(
        &mut self,
        state: &crate::runtime::WorkerAttemptState,
        attempt_version: AggregateVersion,
        observed_at: ServerInstant,
        report: &mut DaemonTickReport,
    ) -> DaemonResult<()> {
        let claimed = self
            .journal
            .claimed_work_for_attempt(state.attempt_id())?
            .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
        let bundle = fixture_candidate_bundle(state, &claimed)?;
        let bundle_digest = Sha256Digest::of_bytes(&bundle);
        let history = self
            .journal
            .candidate_artifact_command_history(state.attempt_id())?;

        let mut init = None;
        let mut chunk = None;
        let mut complete = None;
        for entry in history {
            let response = entry
                .response
                .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
            let record = entry.record;
            match response {
                CandidateArtifactCommandResponse::Init { artifact } => {
                    if !matches!(
                        &record.command,
                        CandidateArtifactControlCommand::Init { .. }
                    ) || init.is_some()
                    {
                        return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
                    }
                    init = Some((record, artifact));
                }
                CandidateArtifactCommandResponse::UploadChunk { receipt } => {
                    if !matches!(
                        &record.command,
                        CandidateArtifactControlCommand::UploadChunk { .. }
                    ) || chunk.is_some()
                    {
                        return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
                    }
                    chunk = Some((record, receipt));
                }
                CandidateArtifactCommandResponse::Complete { artifact } => {
                    if !matches!(
                        &record.command,
                        CandidateArtifactControlCommand::Complete { .. }
                    ) || complete.is_some()
                    {
                        return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
                    }
                    complete = Some((record, artifact));
                }
            }
        }

        if complete.is_some() && (init.is_none() || chunk.is_none()) {
            return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
        }

        if init.is_none() {
            if chunk.is_some() {
                return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
            }
            let record = self.fixture_artifact_init(
                state,
                &claimed,
                &bundle,
                bundle_digest,
                attempt_version,
                observed_at,
            )?;
            let response = execute_candidate_artifact_command_intent(
                self.control.as_ref(),
                &mut self.journal,
                &record,
                observed_at,
            )
            .await?;
            record_artifact_response(report, &response)?;
            let CandidateArtifactCommandResponse::Init { artifact } = response else {
                return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
            };
            init = Some((record, artifact));
        }

        let (_, artifact) = init
            .as_ref()
            .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
        if artifact.expected_bundle_digest != bundle_digest
            || artifact.chunk_digests.as_slice() != [bundle_digest]
            || artifact.expected_bundle_size_bytes
                != u64::try_from(bundle.len())
                    .map_err(|_| DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?
        {
            return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
        }
        if complete.is_some() {
            return Ok(());
        }

        if chunk.is_none() {
            let record =
                self.fixture_artifact_chunk(state, artifact, bundle, bundle_digest, observed_at)?;
            let response = execute_candidate_artifact_command_intent(
                self.control.as_ref(),
                &mut self.journal,
                &record,
                observed_at,
            )
            .await?;
            record_artifact_response(report, &response)?;
            let CandidateArtifactCommandResponse::UploadChunk { receipt } = response else {
                return Err(DaemonError::InvalidArtifactWorkflow(state.attempt_id()));
            };
            chunk = Some((record, receipt));
        }

        chunk
            .as_ref()
            .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
        let record = self.fixture_artifact_complete(state, artifact, observed_at)?;
        let response = execute_candidate_artifact_command_intent(
            self.control.as_ref(),
            &mut self.journal,
            &record,
            observed_at,
        )
        .await?;
        record_artifact_response(report, &response)?;
        Ok(())
    }

    fn fixture_artifact_init(
        &mut self,
        state: &crate::runtime::WorkerAttemptState,
        claimed: &agentforge_application::ClaimedWork,
        bundle: &[u8],
        bundle_digest: Sha256Digest,
        attempt_version: AggregateVersion,
        created_at: ServerInstant,
    ) -> DaemonResult<CandidateArtifactCommandIntentRecord> {
        let candidate = state
            .candidate()
            .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
        let (intent_id, context) =
            self.artifact_command_context("worker-artifact-init", attempt_version)?;
        Ok(CandidateArtifactCommandIntentRecord {
            intent_id,
            attempt_id: state.attempt_id(),
            command: CandidateArtifactControlCommand::Init {
                command: MvpCommand {
                    context,
                    input: InitCandidateArtifactInput {
                        project_id: claimed.project_id,
                        attempt_id: state.attempt_id(),
                        lease_id: state.lease_id(),
                        node_id: self.config.node_id,
                        fencing_token: state.lease_generation(),
                        package_hash: state.package_hash(),
                        base_commit: state.base_commit().clone(),
                        candidate_commit: candidate.commit.clone(),
                        tree_hash: candidate.tree.clone(),
                        author_evidence_digest: candidate.author_evidence_digest,
                        expected_bundle_digest: bundle_digest,
                        expected_bundle_size_bytes: u64::try_from(bundle.len()).map_err(|_| {
                            DaemonError::InvalidArtifactWorkflow(state.attempt_id())
                        })?,
                        chunk_digests: vec![bundle_digest],
                        upload_ttl_seconds: 600,
                    },
                },
            },
            created_at,
        })
    }

    fn fixture_artifact_chunk(
        &mut self,
        state: &crate::runtime::WorkerAttemptState,
        artifact: &agentforge_application::CandidateArtifactView,
        bundle: Vec<u8>,
        bundle_digest: Sha256Digest,
        created_at: ServerInstant,
    ) -> DaemonResult<CandidateArtifactCommandIntentRecord> {
        let (intent_id, context) =
            self.artifact_command_context("worker-artifact-chunk", artifact.version)?;
        Ok(CandidateArtifactCommandIntentRecord {
            intent_id,
            attempt_id: state.attempt_id(),
            command: CandidateArtifactControlCommand::UploadChunk {
                command: MvpCommand {
                    context,
                    input: UploadCandidateArtifactChunkInput {
                        project_id: artifact.project_id,
                        artifact_id: artifact.artifact_id,
                        lease_id: state.lease_id(),
                        node_id: self.config.node_id,
                        fencing_token: state.lease_generation(),
                        chunk_index: 0,
                        digest: bundle_digest,
                        content: bundle,
                    },
                },
            },
            created_at,
        })
    }

    fn fixture_artifact_complete(
        &mut self,
        state: &crate::runtime::WorkerAttemptState,
        artifact: &agentforge_application::CandidateArtifactView,
        created_at: ServerInstant,
    ) -> DaemonResult<CandidateArtifactCommandIntentRecord> {
        let (intent_id, context) =
            self.artifact_command_context("worker-artifact-complete", artifact.version)?;
        Ok(CandidateArtifactCommandIntentRecord {
            intent_id,
            attempt_id: state.attempt_id(),
            command: CandidateArtifactControlCommand::Complete {
                command: MvpCommand {
                    context,
                    input: CompleteCandidateArtifactInput {
                        project_id: artifact.project_id,
                        artifact_id: artifact.artifact_id,
                        lease_id: state.lease_id(),
                        node_id: self.config.node_id,
                        fencing_token: state.lease_generation(),
                        bundle_protocol_key: ProtocolKey::new(format!(
                            "candidate-bundle-{}",
                            state.attempt_id().as_uuid().simple()
                        ))
                        .map_err(|_| DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?,
                        bundle_uri: format!(
                            "artifact://candidate-artifacts/{}/fixture-bundle",
                            state.attempt_id()
                        ),
                    },
                },
            },
            created_at,
        })
    }

    fn artifact_command_context(
        &mut self,
        prefix: &str,
        expected_version: AggregateVersion,
    ) -> DaemonResult<(Uuid, MvpCommandContext)> {
        let intent_id = self.next_uuid()?;
        Ok((
            intent_id,
            MvpCommandContext {
                command_id: CommandId::from(self.next_uuid()?),
                actor_id: self.config.actor_id,
                idempotency_key: idempotency_key(prefix, intent_id)?,
                correlation_id: CorrelationId::from(self.next_uuid()?),
                // The MVP responses do not expose the server Event ID. Do not
                // type-coerce a Command ID into a fabricated causation fact.
                causation_id: None,
                expected_version: Some(expected_version),
            },
        ))
    }

    fn next_uuid(&mut self) -> DaemonResult<Uuid> {
        let value = self.runtime.next_uuid();
        if value.is_nil() {
            Err(DaemonError::InvalidRuntimeId)
        } else {
            Ok(value)
        }
    }
}

fn idempotency_key(prefix: &str, value: Uuid) -> DaemonResult<IdempotencyKey> {
    IdempotencyKey::new(format!("{prefix}:{value}")).map_err(|_| DaemonError::InvalidRuntimeId)
}

fn increment(value: &mut u16) -> DaemonResult<()> {
    *value = value.checked_add(1).ok_or(DaemonError::CounterOverflow)?;
    Ok(())
}

fn record_maintenance_outcome(
    report: &mut DaemonTickReport,
    outcome: &LeaseMaintenanceOutcome,
) -> DaemonResult<()> {
    match outcome {
        LeaseMaintenanceOutcome::NoAction(_) => increment(&mut report.maintenance_noops),
        LeaseMaintenanceOutcome::Renewed { .. } => increment(&mut report.renewed),
        LeaseMaintenanceOutcome::Released { .. } => increment(&mut report.released),
        LeaseMaintenanceOutcome::Stopped(_) => increment(&mut report.stopped),
    }
}

fn record_artifact_response(
    report: &mut DaemonTickReport,
    response: &CandidateArtifactCommandResponse,
) -> DaemonResult<()> {
    increment(&mut report.artifact_commands_completed)?;
    if matches!(response, CandidateArtifactCommandResponse::Complete { .. }) {
        increment(&mut report.candidate_artifacts_completed)?;
    }
    Ok(())
}

fn fixture_candidate_commit(
    attempt_id: AttemptId,
    tree: &GitObjectId,
) -> DaemonResult<GitObjectId> {
    let digest = Sha256Digest::of_bytes(format!(
        "agentforge.fixture.candidate-commit.v1\n{attempt_id}\n{tree}"
    ));
    let digest = digest.to_string();
    GitObjectId::new(digest["sha256:".len()..][..40].to_owned())
        .map_err(|_| DaemonError::InvalidArtifactWorkflow(attempt_id))
}

fn fixture_candidate_bundle(
    state: &crate::runtime::WorkerAttemptState,
    claimed: &agentforge_application::ClaimedWork,
) -> DaemonResult<Vec<u8>> {
    let candidate = state
        .candidate()
        .ok_or(DaemonError::InvalidArtifactWorkflow(state.attempt_id()))?;
    serde_json_canonicalizer::to_vec(&serde_json::json!({
        "attempt_id": state.attempt_id(),
        "author_evidence_digest": candidate.author_evidence_digest,
        "base_commit": state.base_commit(),
        "candidate_commit": candidate.commit,
        "package_hash": state.package_hash(),
        "package_id": state.package_id(),
        "project_id": claimed.project_id,
        "revision": claimed.execution.revision,
        "revision_id": claimed.revision_id,
        "schema": "agentforge.fixture.candidate-bundle.v1",
        "tree_hash": candidate.tree,
    }))
    .map_err(|_| DaemonError::InvalidArtifactWorkflow(state.attempt_id()))
}

fn fixture_progress_evidence(
    state: &crate::runtime::WorkerAttemptState,
    stage: AttemptProgressStage,
    runtime_fingerprint: Sha256Digest,
) -> DaemonResult<Sha256Digest> {
    match stage {
        AttemptProgressStage::Planning => state
            .plan_digest()
            .ok_or(DaemonError::InvalidProgressWorkflow(state.attempt_id())),
        AttemptProgressStage::LocalVerify => state
            .last_verification_digest()
            .ok_or(DaemonError::InvalidProgressWorkflow(state.attempt_id())),
        AttemptProgressStage::Preparing | AttemptProgressStage::Implementing => {
            let tree = if stage == AttemptProgressStage::Implementing {
                Some(
                    state
                        .current_tree()
                        .ok_or(DaemonError::InvalidProgressWorkflow(state.attempt_id()))?,
                )
            } else {
                None
            };
            let evidence = serde_json_canonicalizer::to_vec(&serde_json::json!({
                "attempt_id": state.attempt_id(),
                "base_commit": state.base_commit(),
                "package_hash": state.package_hash(),
                "runtime_fingerprint": runtime_fingerprint,
                "schema": "agentforge.fixture.attempt-progress-evidence.v1",
                "stage": stage,
                "tree": tree,
                "turns_completed": state.turns_completed(),
            }))
            .map_err(|_| DaemonError::InvalidProgressWorkflow(state.attempt_id()))?;
            Ok(Sha256Digest::of_bytes(evidence))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use agentforge_application::{
        CandidateArtifactChunkReceipt, CandidateArtifactView, ClaimPackageInput, ClaimedWork,
        CompleteCandidateArtifactInput, InitCandidateArtifactInput, LeaseView, ListOffersQuery,
        MvpCommand, MvpError, MvpFuture, OfferView, PackageExecutionSnapshot, PortError,
        ReleaseLeaseInput, RenewLeaseInput, UploadCandidateArtifactChunkInput,
    };
    use agentforge_domain::{
        ActorId, AggregateVersion, ArtifactRef, CandidateArtifactId, CandidateArtifactState,
        CandidateId, ExecutorId, FencingToken, GitObjectId, LeaseId, NodeId, PackageId,
        PackageRevision, ProtocolKey, Sha256Digest, attempt::AttemptState, lease::LeaseState,
        work_package::WorkPackageState,
    };
    use time::macros::datetime;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(second: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(second))
    }

    fn execution(name: &str) -> PackageExecutionSnapshot {
        let canonical_document = serde_json::json!({"package": name});
        PackageExecutionSnapshot {
            revision: PackageRevision::new(1).expect("revision"),
            package_hash: Sha256Digest::of_bytes(
                serde_json_canonicalizer::to_vec(&canonical_document).expect("JCS"),
            ),
            base_commit: GitObjectId::new("1".repeat(40)).expect("commit"),
            git_object_format: "sha1".to_owned(),
            canonical_document,
            input_snapshot: serde_json::json!({"fixtures": []}),
        }
    }

    fn offer(package_byte: u8, name: &str) -> OfferView {
        OfferView {
            project_id: id(1),
            package_id: id(package_byte),
            package_key: ProtocolKey::new(name).expect("package key"),
            revision_id: id(package_byte.wrapping_add(20)),
            revision: PackageRevision::new(1).expect("revision"),
            state: WorkPackageState::Offered,
            priority: 10,
            attempts_started: 0,
            max_attempts: 3,
            version: AggregateVersion::new(1),
        }
    }

    fn claimed(offer: &OfferView, attempt_byte: u8, lease_byte: u8) -> ClaimedWork {
        ClaimedWork {
            project_id: offer.project_id,
            package_id: offer.package_id,
            revision_id: offer.revision_id,
            attempt_id: id(attempt_byte),
            lease_id: id(lease_byte),
            fencing_token: FencingToken::new(1).expect("generation"),
            granted_at: at(0),
            expires_at: at(60),
            max_expires_at: at(600),
            package_version: AggregateVersion::new(2),
            attempt_version: AggregateVersion::new(2),
            lease_version: AggregateVersion::new(1),
            execution: execution(offer.package_key.as_str()),
        }
    }

    fn lease(work: &ClaimedWork, node_id: NodeId) -> LeaseView {
        LeaseView {
            project_id: work.project_id,
            package_id: work.package_id,
            revision_id: work.revision_id,
            attempt_id: work.attempt_id,
            lease_id: work.lease_id,
            holder_node_id: node_id,
            fencing_token: work.fencing_token,
            state: LeaseState::Active,
            granted_at: work.granted_at,
            expires_at: work.expires_at,
            max_expires_at: work.max_expires_at,
            updated_at: work.granted_at,
            version: work.lease_version,
        }
    }

    struct FakeControl {
        offers: Mutex<Vec<OfferView>>,
        work: BTreeMap<PackageId, ClaimedWork>,
        node_id: NodeId,
        leases: Mutex<BTreeMap<LeaseId, LeaseView>>,
        claim_receipts: Mutex<BTreeMap<String, ClaimedWork>>,
        renew_receipts: Mutex<BTreeMap<String, LeaseView>>,
        release_receipts: Mutex<BTreeMap<String, LeaseView>>,
        attempt_versions: Mutex<BTreeMap<AttemptId, AggregateVersion>>,
        attempt_progress_sequences: Mutex<BTreeMap<AttemptId, u64>>,
        progress_receipts: Mutex<BTreeMap<String, AttemptProgressView>>,
        artifacts: Mutex<BTreeMap<CandidateArtifactId, CandidateArtifactView>>,
        artifact_chunks: Mutex<BTreeMap<(CandidateArtifactId, u32), Vec<u8>>>,
        artifact_init_receipts: Mutex<BTreeMap<String, CandidateArtifactView>>,
        artifact_chunk_receipts: Mutex<BTreeMap<String, CandidateArtifactChunkReceipt>>,
        artifact_complete_receipts: Mutex<BTreeMap<String, CandidateArtifactView>>,
        now: Mutex<ServerInstant>,
        claims: Mutex<u16>,
        renewals: Mutex<u16>,
        progress_effects: Mutex<u16>,
        artifact_effects: Mutex<u16>,
        fail_after_claim: Mutex<bool>,
        fail_after_progress: Mutex<bool>,
        fail_after_artifact_init: Mutex<bool>,
    }

    impl FakeControl {
        fn new(offers: Vec<OfferView>, work: Vec<ClaimedWork>, node_id: NodeId) -> Self {
            let attempt_versions = work
                .iter()
                .map(|claimed| (claimed.attempt_id, claimed.attempt_version))
                .collect();
            let attempt_progress_sequences =
                work.iter().map(|claimed| (claimed.attempt_id, 0)).collect();
            Self {
                offers: Mutex::new(offers),
                work: work
                    .into_iter()
                    .map(|claimed| (claimed.package_id, claimed))
                    .collect(),
                node_id,
                leases: Mutex::new(BTreeMap::new()),
                claim_receipts: Mutex::new(BTreeMap::new()),
                renew_receipts: Mutex::new(BTreeMap::new()),
                release_receipts: Mutex::new(BTreeMap::new()),
                attempt_versions: Mutex::new(attempt_versions),
                attempt_progress_sequences: Mutex::new(attempt_progress_sequences),
                progress_receipts: Mutex::new(BTreeMap::new()),
                artifacts: Mutex::new(BTreeMap::new()),
                artifact_chunks: Mutex::new(BTreeMap::new()),
                artifact_init_receipts: Mutex::new(BTreeMap::new()),
                artifact_chunk_receipts: Mutex::new(BTreeMap::new()),
                artifact_complete_receipts: Mutex::new(BTreeMap::new()),
                now: Mutex::new(at(0)),
                claims: Mutex::new(0),
                renewals: Mutex::new(0),
                progress_effects: Mutex::new(0),
                artifact_effects: Mutex::new(0),
                fail_after_claim: Mutex::new(false),
                fail_after_progress: Mutex::new(false),
                fail_after_artifact_init: Mutex::new(false),
            }
        }

        fn set_now(&self, now: ServerInstant) {
            *self.now.lock().expect("now lock") = now;
        }
    }

    impl WorkerControlPlane for FakeControl {
        fn list_offers(&self, query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>> {
            let offers = self
                .offers
                .lock()
                .expect("offers lock")
                .iter()
                .filter(|offer| offer.project_id == query.project_id)
                .take(usize::from(query.limit))
                .cloned()
                .collect();
            Box::pin(async move { Ok(offers) })
        }

        fn claim_package<'a>(
            &'a self,
            command: &'a MvpCommand<ClaimPackageInput>,
        ) -> MvpFuture<'a, ClaimedWork> {
            let key = command.context.idempotency_key.as_str().to_owned();
            if let Some(response) = self
                .claim_receipts
                .lock()
                .expect("claim receipts")
                .get(&key)
                .cloned()
            {
                return Box::pin(async move { Ok(response) });
            }
            let Some(response) = self.work.get(&command.input.package_id).cloned() else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            if command.input.node_id != self.node_id
                || command.input.project_id != response.project_id
            {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }
            let before = self.offers.lock().expect("offers lock").len();
            self.offers
                .lock()
                .expect("offers lock")
                .retain(|offer| offer.package_id != response.package_id);
            if self.offers.lock().expect("offers lock").len() == before {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }
            self.leases
                .lock()
                .expect("leases lock")
                .insert(response.lease_id, lease(&response, self.node_id));
            self.claim_receipts
                .lock()
                .expect("claim receipts")
                .insert(key, response.clone());
            *self.claims.lock().expect("claims lock") += 1;
            if std::mem::take(&mut *self.fail_after_claim.lock().expect("failure lock")) {
                return Box::pin(async { Err(MvpError::Port(PortError::Unavailable)) });
            }
            Box::pin(async move { Ok(response) })
        }

        fn report_attempt_progress<'a>(
            &'a self,
            command: &'a MvpCommand<ReportAttemptProgressInput>,
        ) -> MvpFuture<'a, AttemptProgressView> {
            let key = command.context.idempotency_key.as_str().to_owned();
            if let Some(response) = self
                .progress_receipts
                .lock()
                .expect("progress receipts")
                .get(&key)
                .copied()
            {
                return Box::pin(async move { Ok(response) });
            }
            let Some(work) = self
                .work
                .values()
                .find(|work| work.attempt_id == command.input.attempt_id)
            else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            let now = *self.now.lock().expect("now lock");
            let authority_matches = self
                .leases
                .lock()
                .expect("leases lock")
                .get(&command.input.lease_id)
                .is_some_and(|lease| {
                    lease.state == LeaseState::Active
                        && lease.holder_node_id == command.input.node_id
                        && lease.fencing_token == command.input.fencing_token
                        && now < lease.expires_at
                });
            if !authority_matches
                || command.input.project_id != work.project_id
                || command.input.node_id != self.node_id
            {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }

            let mut versions = self.attempt_versions.lock().expect("attempt versions");
            let Some(version) = versions.get_mut(&work.attempt_id) else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            let mut sequences = self
                .attempt_progress_sequences
                .lock()
                .expect("attempt progress sequences");
            let Some(sequence) = sequences.get_mut(&work.attempt_id) else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            let expected = match *sequence {
                0 => (AttemptProgressStage::Preparing, AttemptState::Preparing),
                1 => (AttemptProgressStage::Planning, AttemptState::Planning),
                2 => (
                    AttemptProgressStage::Implementing,
                    AttemptState::Implementing,
                ),
                3 => (AttemptProgressStage::LocalVerify, AttemptState::LocalVerify),
                _ => return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) }),
            };
            if command.context.expected_version != Some(*version)
                || command.input.stage != expected.0
            {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }
            *version = AggregateVersion::new(version.get() + 2);
            *sequence += 1;
            let response = AttemptProgressView {
                project_id: work.project_id,
                package_id: work.package_id,
                attempt_id: work.attempt_id,
                lease_id: work.lease_id,
                fencing_token: work.fencing_token,
                state: expected.1,
                semantic_progress_seq: *sequence,
                updated_at: now,
                version: *version,
            };
            drop(sequences);
            drop(versions);
            self.progress_receipts
                .lock()
                .expect("progress receipts")
                .insert(key, response);
            *self.progress_effects.lock().expect("progress effects") += 1;
            if std::mem::take(
                &mut *self
                    .fail_after_progress
                    .lock()
                    .expect("progress failure lock"),
            ) {
                return Box::pin(async { Err(MvpError::Port(PortError::Unavailable)) });
            }
            Box::pin(async move { Ok(response) })
        }

        fn init_candidate_artifact<'a>(
            &'a self,
            command: &'a MvpCommand<InitCandidateArtifactInput>,
        ) -> MvpFuture<'a, CandidateArtifactView> {
            let key = command.context.idempotency_key.as_str().to_owned();
            if let Some(response) = self
                .artifact_init_receipts
                .lock()
                .expect("artifact init receipts")
                .get(&key)
                .cloned()
            {
                return Box::pin(async move { Ok(response) });
            }
            let Some(work) = self
                .work
                .values()
                .find(|work| work.attempt_id == command.input.attempt_id)
            else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            let lease = self.leases.lock().expect("leases lock");
            let authority_matches = lease.get(&command.input.lease_id).is_some_and(|lease| {
                lease.state == LeaseState::Active
                    && lease.holder_node_id == command.input.node_id
                    && lease.fencing_token == command.input.fencing_token
            });
            drop(lease);
            if !authority_matches
                || command.input.project_id != work.project_id
                || command.input.package_hash != work.execution.package_hash
                || command.input.base_commit != work.execution.base_commit
                || command.context.expected_version
                    != self
                        .attempt_versions
                        .lock()
                        .expect("attempt versions")
                        .get(&work.attempt_id)
                        .copied()
            {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }
            let now = *self.now.lock().expect("now lock");
            let artifact_id = CandidateArtifactId::from_uuid(*command.context.command_id.as_uuid());
            let response = CandidateArtifactView {
                project_id: command.input.project_id,
                artifact_id,
                candidate_id: CandidateId::from_uuid(*command.context.correlation_id.as_uuid()),
                attempt_id: command.input.attempt_id,
                package_id: work.package_id,
                revision_id: work.revision_id,
                lease_id: command.input.lease_id,
                fencing_token: command.input.fencing_token,
                candidate_commit: command.input.candidate_commit.clone(),
                tree_hash: command.input.tree_hash.clone(),
                state: CandidateArtifactState::Uploading,
                expected_bundle_digest: command.input.expected_bundle_digest,
                expected_bundle_size_bytes: command.input.expected_bundle_size_bytes,
                chunk_digests: command.input.chunk_digests.clone(),
                bundle: None,
                created_at: now,
                expires_at: ServerInstant(
                    now.0 + time::Duration::seconds(i64::from(command.input.upload_ttl_seconds)),
                ),
                updated_at: now,
                version: AggregateVersion::new(1),
            };
            self.artifacts
                .lock()
                .expect("artifacts")
                .insert(artifact_id, response.clone());
            self.artifact_init_receipts
                .lock()
                .expect("artifact init receipts")
                .insert(key, response.clone());
            *self.artifact_effects.lock().expect("artifact effects") += 1;
            if std::mem::take(
                &mut *self
                    .fail_after_artifact_init
                    .lock()
                    .expect("artifact failure lock"),
            ) {
                return Box::pin(async { Err(MvpError::Port(PortError::Unavailable)) });
            }
            Box::pin(async move { Ok(response) })
        }

        fn upload_candidate_artifact_chunk<'a>(
            &'a self,
            command: &'a MvpCommand<UploadCandidateArtifactChunkInput>,
        ) -> MvpFuture<'a, CandidateArtifactChunkReceipt> {
            let key = command.context.idempotency_key.as_str().to_owned();
            if let Some(response) = self
                .artifact_chunk_receipts
                .lock()
                .expect("artifact chunk receipts")
                .get(&key)
                .copied()
            {
                return Box::pin(async move { Ok(response) });
            }
            let artifacts = self.artifacts.lock().expect("artifacts");
            let Some(artifact) = artifacts.get(&command.input.artifact_id) else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            let chunk_index = usize::try_from(command.input.chunk_index).ok();
            if artifact.state != CandidateArtifactState::Uploading
                || artifact.project_id != command.input.project_id
                || artifact.lease_id != command.input.lease_id
                || artifact.fencing_token != command.input.fencing_token
                || command.context.expected_version != Some(artifact.version)
                || chunk_index.and_then(|index| artifact.chunk_digests.get(index))
                    != Some(&command.input.digest)
                || Sha256Digest::of_bytes(&command.input.content) != command.input.digest
            {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }
            let response = CandidateArtifactChunkReceipt {
                artifact_id: command.input.artifact_id,
                chunk_index: command.input.chunk_index,
                digest: command.input.digest,
                size_bytes: u32::try_from(command.input.content.len()).expect("fixture chunk size"),
                artifact_version: artifact.version,
            };
            drop(artifacts);
            self.artifact_chunks
                .lock()
                .expect("artifact chunks")
                .insert(
                    (command.input.artifact_id, command.input.chunk_index),
                    command.input.content.clone(),
                );
            self.artifact_chunk_receipts
                .lock()
                .expect("artifact chunk receipts")
                .insert(key, response);
            *self.artifact_effects.lock().expect("artifact effects") += 1;
            Box::pin(async move { Ok(response) })
        }

        fn complete_candidate_artifact<'a>(
            &'a self,
            command: &'a MvpCommand<CompleteCandidateArtifactInput>,
        ) -> MvpFuture<'a, CandidateArtifactView> {
            let key = command.context.idempotency_key.as_str().to_owned();
            if let Some(response) = self
                .artifact_complete_receipts
                .lock()
                .expect("artifact complete receipts")
                .get(&key)
                .cloned()
            {
                return Box::pin(async move { Ok(response) });
            }
            let mut artifacts = self.artifacts.lock().expect("artifacts");
            let Some(artifact) = artifacts.get_mut(&command.input.artifact_id) else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            let chunks = self.artifact_chunks.lock().expect("artifact chunks");
            let mut bundle = Vec::new();
            for (index, digest) in artifact.chunk_digests.iter().enumerate() {
                let Some(content) = chunks.get(&(
                    artifact.artifact_id,
                    u32::try_from(index).expect("fixture index"),
                )) else {
                    return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
                };
                if Sha256Digest::of_bytes(content) != *digest {
                    return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
                }
                bundle.extend_from_slice(content);
            }
            drop(chunks);
            if artifact.state != CandidateArtifactState::Uploading
                || artifact.project_id != command.input.project_id
                || artifact.lease_id != command.input.lease_id
                || artifact.fencing_token != command.input.fencing_token
                || command.context.expected_version != Some(artifact.version)
                || Sha256Digest::of_bytes(&bundle) != artifact.expected_bundle_digest
                || u64::try_from(bundle.len()).ok() != Some(artifact.expected_bundle_size_bytes)
            {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }
            artifact.state = CandidateArtifactState::Complete;
            artifact.bundle = Some(ArtifactRef {
                artifact_id: command.input.bundle_protocol_key.clone(),
                uri: command.input.bundle_uri.clone(),
                digest: artifact.expected_bundle_digest,
            });
            artifact.updated_at = *self.now.lock().expect("now lock");
            artifact.version = AggregateVersion::new(artifact.version.get() + 2);
            let response = artifact.clone();
            drop(artifacts);
            self.artifact_complete_receipts
                .lock()
                .expect("artifact complete receipts")
                .insert(key, response.clone());
            *self.artifact_effects.lock().expect("artifact effects") += 1;
            Box::pin(async move { Ok(response) })
        }

        fn get_lease(&self, project_id: ProjectId, lease_id: LeaseId) -> MvpFuture<'_, LeaseView> {
            let response = self
                .leases
                .lock()
                .expect("leases lock")
                .get(&lease_id)
                .filter(|lease| lease.project_id == project_id)
                .cloned();
            Box::pin(async move { response.ok_or(MvpError::Port(PortError::NotFound)) })
        }

        fn renew_lease<'a>(
            &'a self,
            command: &'a MvpCommand<RenewLeaseInput>,
        ) -> MvpFuture<'a, LeaseView> {
            let key = command.context.idempotency_key.as_str().to_owned();
            if let Some(response) = self
                .renew_receipts
                .lock()
                .expect("renew receipts")
                .get(&key)
                .cloned()
            {
                return Box::pin(async move { Ok(response) });
            }
            let mut leases = self.leases.lock().expect("leases lock");
            let Some(current) = leases.get_mut(&command.input.lease_id) else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            if current.project_id != command.input.project_id
                || current.holder_node_id != command.input.node_id
                || current.fencing_token != command.input.fencing_token
                || command.context.expected_version != Some(current.version)
            {
                return Box::pin(async { Err(MvpError::Port(PortError::Conflict)) });
            }
            current.expires_at = ServerInstant(
                (current.expires_at.0
                    + time::Duration::seconds(i64::from(command.input.extend_by_seconds)))
                .min(current.max_expires_at.0),
            );
            current.updated_at = *self.now.lock().expect("now lock");
            current.version = AggregateVersion::new(current.version.get() + 1);
            let response = current.clone();
            drop(leases);
            self.renew_receipts
                .lock()
                .expect("renew receipts")
                .insert(key, response.clone());
            *self.renewals.lock().expect("renewals lock") += 1;
            Box::pin(async move { Ok(response) })
        }

        fn release_lease<'a>(
            &'a self,
            command: &'a MvpCommand<ReleaseLeaseInput>,
        ) -> MvpFuture<'a, LeaseView> {
            let key = command.context.idempotency_key.as_str().to_owned();
            if let Some(response) = self
                .release_receipts
                .lock()
                .expect("release receipts")
                .get(&key)
                .cloned()
            {
                return Box::pin(async move { Ok(response) });
            }
            let mut leases = self.leases.lock().expect("leases lock");
            let Some(current) = leases.get_mut(&command.input.lease_id) else {
                return Box::pin(async { Err(MvpError::Port(PortError::NotFound)) });
            };
            current.state = LeaseState::Released;
            current.updated_at = *self.now.lock().expect("now lock");
            current.version = AggregateVersion::new(current.version.get() + 1);
            let response = current.clone();
            drop(leases);
            self.release_receipts
                .lock()
                .expect("release receipts")
                .insert(key, response.clone());
            Box::pin(async move { Ok(response) })
        }
    }

    #[derive(Clone, Copy)]
    struct FakeRuntime {
        now: ServerInstant,
        next: u128,
    }

    impl DaemonRuntime for FakeRuntime {
        fn now(&mut self) -> ServerInstant {
            self.now
        }

        fn next_uuid(&mut self) -> Uuid {
            let value = Uuid::from_u128(self.next);
            self.next += 1;
            value
        }
    }

    fn config(path: std::path::PathBuf) -> WorkerDaemonConfig {
        WorkerDaemonConfig {
            schema_version: 1,
            actor_id: id::<ActorId>(40),
            executor_id: id::<ExecutorId>(41),
            node_id: id::<NodeId>(42),
            runtime_fingerprint: Sha256Digest::of_bytes("fake-jcode-v1"),
            project_ids: vec![id(1)],
            control_plane_url: "http://127.0.0.1:8080".to_owned(),
            request_timeout_seconds: 10,
            max_response_bytes: 2_097_152,
            journal_path: path,
            capacity: 1,
            offer_limit: 10,
            lease_seconds: 60,
            max_lease_seconds: 600,
            renew_before_seconds: 15,
            extend_by_seconds: 30,
            tick_seconds: 5,
            driver_mode: crate::config::WorkerDriverMode::LeaseOnly,
            max_turns: 3,
            operation_timeout_seconds: 60,
        }
    }

    fn fixture() -> (
        tempfile::TempDir,
        WorkerDaemonConfig,
        Arc<FakeControl>,
        FakeRuntime,
    ) {
        let directory = tempfile::tempdir().expect("tempdir");
        let first = offer(2, "daemon-first");
        let second = offer(3, "daemon-second");
        let control = Arc::new(FakeControl::new(
            vec![first.clone(), second.clone()],
            vec![claimed(&first, 10, 11), claimed(&second, 12, 13)],
            id(42),
        ));
        let config = config(directory.path().join("worker.sqlite3"));
        let runtime = FakeRuntime {
            now: at(0),
            next: 10_000,
        };
        (directory, config, control, runtime)
    }

    #[tokio::test]
    async fn tick_claims_to_capacity_then_renews_without_claiming_another_offer() {
        let (_directory, config, control, runtime) = fixture();
        let mut daemon = WorkerDaemon::open(control.clone(), config, runtime).expect("daemon");

        let first = daemon.tick().await.expect("first tick");
        assert_eq!(first.claimed, 1);
        assert_eq!(first.active_attempts, 1);
        assert_eq!(*control.claims.lock().expect("claims"), 1);
        assert_eq!(control.offers.lock().expect("offers").len(), 1);

        daemon.runtime.now = at(50);
        control.set_now(at(50));
        let second = daemon.tick().await.expect("renew tick");
        assert_eq!(second.renewed, 1);
        assert_eq!(second.claimed, 0);
        assert_eq!(second.active_attempts, 1);
        assert_eq!(*control.renewals.lock().expect("renewals"), 1);
        assert_eq!(control.offers.lock().expect("offers").len(), 1);
    }

    #[tokio::test]
    async fn restart_replays_pending_claim_before_polling_new_work() {
        let (_directory, config, control, runtime) = fixture();
        *control.fail_after_claim.lock().expect("failure") = true;
        let mut first =
            WorkerDaemon::open(control.clone(), config.clone(), runtime).expect("daemon");
        let error = first.tick().await.expect_err("ACK loss");
        assert_eq!(error.code(), "AF_UNAVAILABLE");
        assert_eq!(
            first
                .journal()
                .pending_claim_intents()
                .expect("pending")
                .len(),
            1
        );
        assert_eq!(*control.claims.lock().expect("claims"), 1);
        drop(first);

        let mut restarted = WorkerDaemon::open(
            control.clone(),
            config,
            FakeRuntime {
                now: at(1),
                next: 20_000,
            },
        )
        .expect("restart");
        let report = restarted.tick().await.expect("recovery tick");
        assert_eq!(report.resumed_claims, 1);
        assert_eq!(report.claimed, 0);
        assert_eq!(report.active_attempts, 1);
        assert!(
            restarted
                .journal()
                .pending_claim_intents()
                .expect("pending")
                .is_empty()
        );
        assert_eq!(*control.claims.lock().expect("claims"), 1);
        assert_eq!(control.offers.lock().expect("offers").len(), 1);
    }

    #[tokio::test]
    async fn fixture_mode_uploads_a_complete_candidate_artifact_without_overclaiming() {
        let (_directory, mut config, control, runtime) = fixture();
        config.driver_mode = crate::config::WorkerDriverMode::Fixture;
        let mut daemon = WorkerDaemon::open(control.clone(), config, runtime).expect("daemon");
        assert_eq!(daemon.tick().await.expect("claim tick").claimed, 1);

        daemon.runtime.now = at(1);
        control.set_now(at(1));
        let report = daemon.tick().await.expect("fixture drive tick");
        assert_eq!(report.driven_attempts, 1);
        assert_eq!(report.local_candidates_ready, 1);
        assert_eq!(report.progress_commands_completed, 4);
        assert_eq!(report.artifact_commands_completed, 3);
        assert_eq!(report.candidate_artifacts_completed, 1);
        assert_eq!(
            report.claimed, 0,
            "a local Candidate still owns its author Lease and capacity"
        );
        assert_eq!(report.active_attempts, 1);
        assert_eq!(
            daemon
                .journal()
                .load_attempt(id(10))
                .expect("load first")
                .expect("first attempt")
                .phase(),
            WorkerPhase::HandingOffCandidate
        );
        let history = daemon
            .journal()
            .candidate_artifact_command_history(id(10))
            .expect("artifact history");
        assert_eq!(history.len(), 3);
        assert!(history.iter().all(|entry| entry.response.is_some()));
        let progress = daemon
            .journal()
            .attempt_progress_command_history(id(10))
            .expect("progress history");
        assert_eq!(progress.len(), 4);
        assert_eq!(
            progress.last().and_then(|entry| entry.response.as_ref()),
            Some(&AttemptProgressView {
                project_id: id(1),
                package_id: id(2),
                attempt_id: id(10),
                lease_id: id(11),
                fencing_token: FencingToken::new(1).expect("generation"),
                state: AttemptState::LocalVerify,
                semantic_progress_seq: 4,
                updated_at: at(1),
                version: AggregateVersion::new(10),
            })
        );
        let CandidateArtifactControlCommand::Init { command } =
            &history.first().expect("artifact init").record.command
        else {
            panic!("artifact init")
        };
        assert_eq!(
            command.context.expected_version,
            Some(AggregateVersion::new(10))
        );
        assert_eq!(*control.progress_effects.lock().expect("effects"), 4);
        assert_eq!(*control.artifact_effects.lock().expect("effects"), 3);
        assert!(
            daemon
                .journal()
                .load_attempt(id(12))
                .expect("load second")
                .is_none()
        );
    }

    #[tokio::test]
    async fn restart_replays_the_same_pending_artifact_init_before_planning_later_steps() {
        let (_directory, mut config, control, runtime) = fixture();
        config.driver_mode = crate::config::WorkerDriverMode::Fixture;
        let mut first =
            WorkerDaemon::open(control.clone(), config.clone(), runtime).expect("daemon");
        assert_eq!(first.tick().await.expect("claim tick").claimed, 1);

        first.runtime.now = at(1);
        control.set_now(at(1));
        *control
            .fail_after_artifact_init
            .lock()
            .expect("artifact failure") = true;
        let error = first.tick().await.expect_err("artifact Init ACK loss");
        assert_eq!(error.code(), "AF_UNAVAILABLE");
        assert_eq!(
            first
                .journal()
                .attempt_progress_command_history(id(10))
                .expect("progress before Artifact")
                .len(),
            4
        );
        let pending = first
            .journal()
            .pending_candidate_artifact_command_intents()
            .expect("pending artifact");
        assert_eq!(pending.len(), 1);
        assert!(matches!(
            pending[0].command,
            CandidateArtifactControlCommand::Init { .. }
        ));
        assert_eq!(*control.artifact_effects.lock().expect("effects"), 1);
        assert_eq!(*control.progress_effects.lock().expect("effects"), 4);
        drop(first);

        control.set_now(at(2));
        let mut restarted = WorkerDaemon::open(
            control.clone(),
            config,
            FakeRuntime {
                now: at(2),
                next: 30_000,
            },
        )
        .expect("restart");
        let report = restarted.tick().await.expect("artifact recovery");
        assert_eq!(report.resumed_artifact_commands, 1);
        assert_eq!(report.resumed_progress_commands, 0);
        assert_eq!(report.progress_commands_completed, 0);
        assert_eq!(report.artifact_commands_completed, 3);
        assert_eq!(report.candidate_artifacts_completed, 1);
        assert_eq!(*control.artifact_effects.lock().expect("effects"), 3);
        assert_eq!(*control.progress_effects.lock().expect("effects"), 4);
        assert!(
            restarted
                .journal()
                .pending_candidate_artifact_command_intents()
                .expect("pending artifact")
                .is_empty()
        );
        let history = restarted
            .journal()
            .candidate_artifact_command_history(id(10))
            .expect("artifact history");
        assert_eq!(history.len(), 3);
        assert!(history.iter().all(|entry| entry.response.is_some()));
    }

    #[tokio::test]
    async fn restart_replays_progress_before_creating_any_candidate_artifact() {
        let (_directory, mut config, control, runtime) = fixture();
        config.driver_mode = crate::config::WorkerDriverMode::Fixture;
        let mut first =
            WorkerDaemon::open(control.clone(), config.clone(), runtime).expect("daemon");
        assert_eq!(first.tick().await.expect("claim tick").claimed, 1);

        first.runtime.now = at(1);
        control.set_now(at(1));
        *control
            .fail_after_progress
            .lock()
            .expect("progress failure") = true;
        let error = first.tick().await.expect_err("Progress ACK loss");
        assert_eq!(error.code(), "AF_UNAVAILABLE");
        assert_eq!(
            first
                .journal()
                .pending_attempt_progress_command_intents()
                .expect("pending progress")
                .len(),
            1
        );
        assert!(
            first
                .journal()
                .pending_candidate_artifact_command_intents()
                .expect("no Artifact side effect planned")
                .is_empty()
        );
        assert_eq!(*control.progress_effects.lock().expect("effects"), 1);
        assert_eq!(*control.artifact_effects.lock().expect("effects"), 0);
        drop(first);

        control.set_now(at(2));
        let mut restarted = WorkerDaemon::open(
            control.clone(),
            config,
            FakeRuntime {
                now: at(2),
                next: 40_000,
            },
        )
        .expect("restart");
        let report = restarted.tick().await.expect("progress recovery");
        assert_eq!(report.resumed_progress_commands, 1);
        assert_eq!(report.progress_commands_completed, 4);
        assert_eq!(report.artifact_commands_completed, 3);
        assert_eq!(report.candidate_artifacts_completed, 1);
        assert_eq!(*control.progress_effects.lock().expect("effects"), 4);
        assert_eq!(*control.artifact_effects.lock().expect("effects"), 3);
        assert!(
            restarted
                .journal()
                .pending_attempt_progress_command_intents()
                .expect("progress completed")
                .is_empty()
        );
        let progress = restarted
            .journal()
            .attempt_progress_command_history(id(10))
            .expect("progress history");
        assert_eq!(progress.len(), 4);
        assert!(progress.iter().all(|entry| entry.response.is_some()));
    }

    #[test]
    fn invalid_runtime_identifier_fails_before_registering_a_remote_mutation() {
        let (_directory, config, control, mut runtime) = fixture();
        runtime.next = 0;
        let mut daemon = WorkerDaemon::open(control, config, runtime).expect("daemon");
        let result = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(daemon.tick())
            .expect_err("nil ID");
        assert_eq!(result.code(), "AF_WORKER_RUNTIME_ID_INVALID");
        assert!(
            daemon
                .journal()
                .pending_claim_intents()
                .expect("pending")
                .is_empty()
        );
    }
}
