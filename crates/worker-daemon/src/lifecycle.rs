//! Worker-side Claim handoff and startup Lease reconciliation.

use agentforge_application::{
    ClaimPackageInput, ClaimedWork, LeaseView, ListOffersQuery, MvpCommand, MvpCommandContext,
    MvpControlPlane, MvpError, MvpFuture, OfferView, PackageExecutionSnapshot,
};
use agentforge_domain::{
    ActorId, CommandId, CorrelationId, ExecutorId, IdempotencyKey, NodeId, ProjectId,
    ServerInstant, Sha256Digest, lease::LeaseState, work_package::WorkPackageState,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    journal::{Journal, JournalCommand, JournalError, JournalRequest},
    runtime::{
        AttemptGrant, LeaseLossReason, WorkerAttemptState, WorkerCommandEnvelope,
        WorkerCommandKind, WorkerPhase,
    },
};

const MAX_INLINE_EXECUTION_BYTES: usize = 1_048_576;

/// Narrow control-plane surface required by the Worker lifecycle. An HTTP/mTLS
/// adapter and the in-process MVP service can implement the same boundary.
pub trait WorkerControlPlane: Send + Sync {
    fn list_offers(&self, query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>>;

    fn claim_package<'a>(
        &'a self,
        command: &'a MvpCommand<ClaimPackageInput>,
    ) -> MvpFuture<'a, ClaimedWork>;

    fn get_lease(
        &self,
        project_id: ProjectId,
        lease_id: agentforge_domain::LeaseId,
    ) -> MvpFuture<'_, LeaseView>;
}

impl<T> WorkerControlPlane for T
where
    T: MvpControlPlane + ?Sized,
{
    fn list_offers(&self, query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>> {
        MvpControlPlane::list_offers(self, query)
    }

    fn claim_package<'a>(
        &'a self,
        command: &'a MvpCommand<ClaimPackageInput>,
    ) -> MvpFuture<'a, ClaimedWork> {
        MvpControlPlane::claim_package(self, command)
    }

    fn get_lease(
        &self,
        project_id: ProjectId,
        lease_id: agentforge_domain::LeaseId,
    ) -> MvpFuture<'_, LeaseView> {
        MvpControlPlane::get_lease(self, project_id, lease_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerIdentity {
    pub actor_id: ActorId,
    pub executor_id: ExecutorId,
    pub node_id: NodeId,
}

impl WorkerIdentity {
    fn validate(self) -> LifecycleResult<()> {
        if self.actor_id.as_uuid().is_nil()
            || self.executor_id.as_uuid().is_nil()
            || self.node_id.as_uuid().is_nil()
        {
            return Err(LifecycleError::InvalidIntent);
        }
        Ok(())
    }
}

/// A selected Offer plus stable request identity. A production daemon must
/// persist this value before the remote Claim so an ACK-loss retry selects the
/// same Package and idempotency key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimIntent {
    pub offer: OfferView,
    pub command_id: CommandId,
    pub correlation_id: CorrelationId,
    pub idempotency_key: IdempotencyKey,
    pub local_message_id: Uuid,
    pub lease_seconds: u32,
    pub max_lease_seconds: u32,
}

impl ClaimIntent {
    fn validate(&self) -> LifecycleResult<()> {
        if self.command_id.as_uuid().is_nil()
            || self.correlation_id.as_uuid().is_nil()
            || self.local_message_id.is_nil()
            || !(5..=3_600).contains(&self.lease_seconds)
            || self.max_lease_seconds < self.lease_seconds
            || self.max_lease_seconds > 86_400
            || !matches!(
                self.offer.state,
                WorkPackageState::Offered | WorkPackageState::ReworkReady
            )
        {
            return Err(LifecycleError::InvalidIntent);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedAttempt {
    pub state: WorkerAttemptState,
    pub execution: PackageExecutionSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconcileIntent {
    pub project_id: ProjectId,
    pub attempt_id: agentforge_domain::AttemptId,
    pub observed_at: ServerInstant,
    pub local_message_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileDisposition {
    Current(WorkerAttemptState),
    LeaseUpdated(WorkerAttemptState),
    Stopped(WorkerAttemptState),
}

impl ReconcileDisposition {
    #[must_use]
    pub fn state(&self) -> &WorkerAttemptState {
        match self {
            Self::Current(state) | Self::LeaseUpdated(state) | Self::Stopped(state) => state,
        }
    }
}

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error(transparent)]
    Control(#[from] MvpError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("Worker lifecycle intent is invalid")]
    InvalidIntent,
    #[error("control plane returned a response that does not match the request")]
    InvalidResponse,
}

impl LifecycleError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Control(error) => error.code(),
            Self::Journal(error) => error.code(),
            Self::InvalidIntent => "AF_WORKER_INTENT_INVALID",
            Self::InvalidResponse => "AF_WORKER_CONTROL_RESPONSE_INVALID",
        }
    }
}

pub type LifecycleResult<T> = Result<T, LifecycleError>;

pub async fn poll_offer(
    control: &dyn WorkerControlPlane,
    project_id: ProjectId,
    limit: u16,
) -> LifecycleResult<Option<OfferView>> {
    if project_id.as_uuid().is_nil() || limit == 0 || limit > 1_000 {
        return Err(LifecycleError::InvalidIntent);
    }
    let offers = control
        .list_offers(ListOffersQuery { project_id, limit })
        .await?;
    if offers.iter().any(|offer| {
        offer.project_id != project_id
            || !matches!(
                offer.state,
                WorkPackageState::Offered | WorkPackageState::ReworkReady
            )
    }) {
        return Err(LifecycleError::InvalidResponse);
    }
    Ok(offers.into_iter().next())
}

pub async fn claim_offer(
    control: &dyn WorkerControlPlane,
    journal: &mut Journal,
    identity: WorkerIdentity,
    intent: &ClaimIntent,
) -> LifecycleResult<ClaimedAttempt> {
    identity.validate()?;
    intent.validate()?;
    let command = MvpCommand {
        context: MvpCommandContext {
            command_id: intent.command_id,
            actor_id: identity.actor_id,
            idempotency_key: intent.idempotency_key.clone(),
            correlation_id: intent.correlation_id,
            causation_id: None,
            expected_version: Some(intent.offer.version),
        },
        input: ClaimPackageInput {
            project_id: intent.offer.project_id,
            package_id: intent.offer.package_id,
            executor_id: identity.executor_id,
            node_id: identity.node_id,
            lease_seconds: intent.lease_seconds,
            max_lease_seconds: intent.max_lease_seconds,
        },
    };
    let claimed = control.claim_package(&command).await?;
    validate_claim_response(&claimed, intent)?;
    let grant = AttemptGrant {
        attempt_id: claimed.attempt_id,
        package_id: claimed.package_id,
        package_revision: claimed.execution.revision,
        package_hash: claimed.execution.package_hash,
        base_commit: claimed.execution.base_commit.clone(),
        lease_id: claimed.lease_id,
        lease_generation: claimed.fencing_token,
        lease_expires_at: claimed.expires_at,
        granted_at: claimed.granted_at,
    };
    let local_key = IdempotencyKey::new(format!(
        "grant:{}:g{}",
        claimed.attempt_id,
        claimed.fencing_token.get()
    ))
    .map_err(|_| LifecycleError::InvalidResponse)?;
    let state = journal
        .handle(&JournalRequest {
            message_id: intent.local_message_id,
            actor_id: identity.actor_id,
            idempotency_key: local_key,
            command: JournalCommand::Grant {
                grant,
                execution: claimed.execution.clone(),
            },
        })?
        .state()
        .clone();
    Ok(ClaimedAttempt {
        state,
        execution: claimed.execution,
    })
}

pub async fn reconcile_attempt(
    control: &dyn WorkerControlPlane,
    journal: &mut Journal,
    identity: WorkerIdentity,
    intent: ReconcileIntent,
) -> LifecycleResult<ReconcileDisposition> {
    identity.validate()?;
    if intent.project_id.as_uuid().is_nil()
        || intent.attempt_id.as_uuid().is_nil()
        || intent.local_message_id.is_nil()
    {
        return Err(LifecycleError::InvalidIntent);
    }
    let state = journal
        .load_attempt(intent.attempt_id)?
        .ok_or(LifecycleError::InvalidIntent)?;
    if state.phase().is_terminal() || state.phase() == WorkerPhase::Salvaging {
        return Ok(ReconcileDisposition::Stopped(state));
    }
    let lease = control
        .get_lease(intent.project_id, state.lease_id())
        .await?;
    if lease.project_id != intent.project_id
        || lease.package_id != state.package_id()
        || lease.attempt_id != state.attempt_id()
        || lease.lease_id != state.lease_id()
    {
        return Err(LifecycleError::InvalidResponse);
    }

    if lease.holder_node_id != identity.node_id
        || lease.fencing_token != state.lease_generation()
        || lease.state != LeaseState::Active
    {
        let reason = if lease.fencing_token.get() > state.lease_generation().get() {
            LeaseLossReason::HigherGeneration
        } else if lease.state == LeaseState::Expired {
            LeaseLossReason::Expired
        } else if lease.state == LeaseState::Revoked {
            LeaseLossReason::Revoked
        } else {
            LeaseLossReason::ServerRejected
        };
        let observed_generation = if reason == LeaseLossReason::HigherGeneration {
            lease.fencing_token
        } else {
            state.lease_generation()
        };
        let stopped = apply_local(
            journal,
            identity.actor_id,
            intent.local_message_id,
            &state,
            max_instant(intent.observed_at, state.updated_at()),
            format!(
                "lease-reconcile:{}:v{}",
                state.lease_id(),
                lease.version.get()
            ),
            WorkerCommandKind::LoseLease {
                reason,
                observed_generation,
            },
        )?;
        return Ok(ReconcileDisposition::Stopped(stopped));
    }

    if lease.expires_at < state.lease_expires_at()
        || lease.updated_at < lease.granted_at
        || lease.expires_at > lease.max_expires_at
    {
        return Err(LifecycleError::InvalidResponse);
    }
    let mut state = state;
    let mut updated = false;
    if lease.expires_at > state.lease_expires_at() {
        let observed_at = max_instant(lease.updated_at, state.updated_at());
        state = apply_local(
            journal,
            identity.actor_id,
            intent.local_message_id,
            &state,
            observed_at,
            format!("lease-sync:{}:v{}", state.lease_id(), lease.version.get()),
            WorkerCommandKind::RenewLease {
                observed_generation: state.lease_generation(),
                new_expires_at: lease.expires_at,
            },
        )?;
        updated = true;
    }
    if intent.observed_at >= lease.expires_at {
        let stopped = apply_local(
            journal,
            identity.actor_id,
            next_message_id(intent.local_message_id),
            &state,
            max_instant(intent.observed_at, state.updated_at()),
            format!(
                "lease-expired:{}:v{}",
                state.lease_id(),
                lease.version.get()
            ),
            WorkerCommandKind::LoseLease {
                reason: LeaseLossReason::Expired,
                observed_generation: state.lease_generation(),
            },
        )?;
        return Ok(ReconcileDisposition::Stopped(stopped));
    }
    Ok(if updated {
        ReconcileDisposition::LeaseUpdated(state)
    } else {
        ReconcileDisposition::Current(state)
    })
}

fn validate_claim_response(claimed: &ClaimedWork, intent: &ClaimIntent) -> LifecycleResult<()> {
    let execution_bytes = serde_json_canonicalizer::to_vec(&claimed.execution.canonical_document)
        .map_err(|_| LifecycleError::InvalidResponse)?;
    let input_bytes = serde_json_canonicalizer::to_vec(&claimed.execution.input_snapshot)
        .map_err(|_| LifecycleError::InvalidResponse)?;
    let expected_object_format = if claimed.execution.base_commit.as_str().len() == 40 {
        "sha1"
    } else {
        "sha256"
    };
    if claimed.project_id != intent.offer.project_id
        || claimed.package_id != intent.offer.package_id
        || claimed.revision_id != intent.offer.revision_id
        || claimed.execution.revision != intent.offer.revision
        || claimed.package_version.get() != intent.offer.version.get().saturating_add(1)
        || claimed.attempt_id.as_uuid().is_nil()
        || claimed.lease_id.as_uuid().is_nil()
        || claimed.granted_at >= claimed.expires_at
        || claimed.expires_at > claimed.max_expires_at
        || claimed.execution.git_object_format != expected_object_format
        || !claimed.execution.canonical_document.is_object()
        || !claimed.execution.input_snapshot.is_object()
        || execution_bytes
            .len()
            .checked_add(input_bytes.len())
            .is_none_or(|size| size > MAX_INLINE_EXECUTION_BYTES)
        || Sha256Digest::of_bytes(execution_bytes) != claimed.execution.package_hash
    {
        return Err(LifecycleError::InvalidResponse);
    }
    Ok(())
}

fn apply_local(
    journal: &mut Journal,
    actor_id: ActorId,
    message_id: Uuid,
    state: &WorkerAttemptState,
    observed_at: ServerInstant,
    key: String,
    command: WorkerCommandKind,
) -> LifecycleResult<WorkerAttemptState> {
    Ok(journal
        .handle(&JournalRequest {
            message_id,
            actor_id,
            idempotency_key: IdempotencyKey::new(key).map_err(|_| LifecycleError::InvalidIntent)?,
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

fn max_instant(left: ServerInstant, right: ServerInstant) -> ServerInstant {
    if left >= right { left } else { right }
}

fn next_message_id(value: Uuid) -> Uuid {
    let mut bytes = *value.as_bytes();
    bytes[15] = bytes[15].wrapping_add(1);
    let next = Uuid::from_bytes(bytes);
    if next.is_nil() {
        Uuid::from_bytes([1; 16])
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agentforge_domain::{
        AggregateVersion, FencingToken, GitObjectId, LeaseId, PackageRevision, ProtocolKey,
    };
    use tempfile::TempDir;
    use time::macros::datetime;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(second: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(second))
    }

    fn execution() -> PackageExecutionSnapshot {
        let canonical_document = serde_json::json!({"package": "worker-lifecycle"});
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

    fn offer() -> OfferView {
        OfferView {
            project_id: id(1),
            package_id: id(2),
            package_key: ProtocolKey::new("worker-fixture").expect("key"),
            revision_id: id(3),
            revision: PackageRevision::new(1).expect("revision"),
            state: WorkPackageState::Offered,
            priority: 10,
            attempts_started: 0,
            max_attempts: 3,
            version: AggregateVersion::new(1),
        }
    }

    fn claimed() -> ClaimedWork {
        ClaimedWork {
            project_id: id(1),
            package_id: id(2),
            revision_id: id(3),
            attempt_id: id(4),
            lease_id: id(5),
            fencing_token: FencingToken::new(1).expect("generation"),
            granted_at: at(0),
            expires_at: at(60),
            max_expires_at: at(600),
            package_version: AggregateVersion::new(2),
            attempt_version: AggregateVersion::new(2),
            lease_version: AggregateVersion::new(1),
            execution: execution(),
        }
    }

    fn lease(state: LeaseState, expires_at: ServerInstant, updated_at: ServerInstant) -> LeaseView {
        LeaseView {
            project_id: id(1),
            package_id: id(2),
            revision_id: id(3),
            attempt_id: id(4),
            lease_id: id(5),
            holder_node_id: id(8),
            fencing_token: FencingToken::new(1).expect("generation"),
            state,
            granted_at: at(0),
            expires_at,
            max_expires_at: at(600),
            updated_at,
            version: AggregateVersion::new(if state == LeaseState::Active { 2 } else { 3 }),
        }
    }

    struct FakeControl {
        offers: Vec<OfferView>,
        claimed: ClaimedWork,
        lease: Mutex<LeaseView>,
        claims: Mutex<u32>,
    }

    impl WorkerControlPlane for FakeControl {
        fn list_offers(&self, _query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>> {
            let offers = self.offers.clone();
            Box::pin(async move { Ok(offers) })
        }

        fn claim_package<'a>(
            &'a self,
            _command: &'a MvpCommand<ClaimPackageInput>,
        ) -> MvpFuture<'a, ClaimedWork> {
            let claimed = self.claimed.clone();
            *self.claims.lock().expect("claims lock") += 1;
            Box::pin(async move { Ok(claimed) })
        }

        fn get_lease(
            &self,
            _project_id: ProjectId,
            _lease_id: LeaseId,
        ) -> MvpFuture<'_, LeaseView> {
            let lease = self.lease.lock().expect("lease lock").clone();
            Box::pin(async move { Ok(lease) })
        }
    }

    fn identity() -> WorkerIdentity {
        WorkerIdentity {
            actor_id: id(6),
            executor_id: id(7),
            node_id: id(8),
        }
    }

    fn intent() -> ClaimIntent {
        ClaimIntent {
            offer: offer(),
            command_id: id(10),
            correlation_id: id(11),
            idempotency_key: IdempotencyKey::new("claim-worker-fixture").expect("key"),
            local_message_id: Uuid::from_bytes([12; 16]),
            lease_seconds: 60,
            max_lease_seconds: 600,
        }
    }

    fn fixture() -> (TempDir, Journal, FakeControl) {
        let directory = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(directory.path().join("worker.sqlite3")).expect("journal");
        let control = FakeControl {
            offers: vec![offer()],
            claimed: claimed(),
            lease: Mutex::new(lease(LeaseState::Active, at(60), at(0))),
            claims: Mutex::new(0),
        };
        (directory, journal, control)
    }

    #[tokio::test]
    async fn selected_offer_claim_and_local_grant_are_exactly_replayable() -> LifecycleResult<()> {
        let (_directory, mut journal, control) = fixture();
        assert_eq!(poll_offer(&control, id(1), 10).await?, Some(offer()));
        let first = claim_offer(&control, &mut journal, identity(), &intent())
            .await
            .expect("claim");
        let replay = claim_offer(&control, &mut journal, identity(), &intent())
            .await
            .expect("claim replay");
        assert_eq!(first, replay);
        assert_eq!(first.state.phase(), WorkerPhase::Granted);
        assert_eq!(
            journal
                .load_execution_snapshot(first.state.attempt_id())
                .expect("snapshot"),
            Some(execution())
        );
        assert_eq!(*control.claims.lock().expect("claims"), 2);
        Ok(())
    }

    #[tokio::test]
    async fn startup_reconciliation_imports_a_missed_renewal_then_stops_on_revoke() {
        let (_directory, mut journal, control) = fixture();
        let claimed = claim_offer(&control, &mut journal, identity(), &intent())
            .await
            .expect("claim");
        *control.lease.lock().expect("lease") = lease(LeaseState::Active, at(90), at(20));
        let renewed = reconcile_attempt(
            &control,
            &mut journal,
            identity(),
            ReconcileIntent {
                project_id: id(1),
                attempt_id: claimed.state.attempt_id(),
                observed_at: at(30),
                local_message_id: Uuid::from_bytes([20; 16]),
            },
        )
        .await
        .expect("reconcile renewal");
        assert!(matches!(renewed, ReconcileDisposition::LeaseUpdated(_)));
        assert_eq!(renewed.state().lease_expires_at(), at(90));

        *control.lease.lock().expect("lease") = lease(LeaseState::Revoked, at(90), at(40));
        let stopped = reconcile_attempt(
            &control,
            &mut journal,
            identity(),
            ReconcileIntent {
                project_id: id(1),
                attempt_id: claimed.state.attempt_id(),
                observed_at: at(40),
                local_message_id: Uuid::from_bytes([21; 16]),
            },
        )
        .await
        .expect("reconcile revoke");
        assert!(matches!(stopped, ReconcileDisposition::Stopped(_)));
        assert_eq!(stopped.state().phase(), WorkerPhase::Salvaging);
    }

    #[tokio::test]
    async fn tampered_claim_execution_snapshot_never_enters_the_journal() {
        let (_directory, mut journal, mut control) = fixture();
        control.claimed.execution.package_hash = Sha256Digest::of_bytes("tampered");
        let error = claim_offer(&control, &mut journal, identity(), &intent())
            .await
            .expect_err("tampered response");
        assert_eq!(error.code(), "AF_WORKER_CONTROL_RESPONSE_INVALID");
        assert!(
            journal
                .load_attempt(control.claimed.attempt_id)
                .expect("load")
                .is_none()
        );
    }
}
