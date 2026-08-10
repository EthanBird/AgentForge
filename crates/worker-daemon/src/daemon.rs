//! Worker daemon composition root and deterministic scheduling loop.

use std::{future::Future, sync::Arc, time::Duration};

use agentforge_domain::{
    AttemptId, CommandId, CorrelationId, IdempotencyKey, ProjectId, ServerInstant,
};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::{
    config::{ConfigError, WorkerDaemonConfig},
    journal::{Journal, JournalError},
    lifecycle::{
        ClaimIntent, LeaseMaintenanceIntent, LeaseMaintenanceOutcome, LifecycleError,
        WorkerControlPlane, claim_offer, maintain_attempt, poll_offer, resume_claim_intent,
        resume_lease_command_intent,
    },
    runtime::WorkerPhase,
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
    pub resumed_lease_commands: u16,
    pub maintenance_noops: u16,
    pub renewed: u16,
    pub released: u16,
    pub stopped: u16,
    pub claimed: u16,
    pub active_attempts: u16,
}

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error("daemon runtime produced an invalid identifier")]
    InvalidRuntimeId,
    #[error("attempt {0} has no durable Project binding")]
    MissingProjectBinding(AttemptId),
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
            Self::InvalidRuntimeId => "AF_WORKER_RUNTIME_ID_INVALID",
            Self::MissingProjectBinding(_) => "AF_WORKER_PROJECT_BINDING_MISSING",
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
    /// 2. replay pending Renew/Release mutations;
    /// 3. reconcile and maintain every locally owned Lease;
    /// 4. poll and Claim only the remaining execution capacity.
    pub async fn tick(&mut self) -> DaemonResult<DaemonTickReport> {
        let observed_at = self.runtime.now();
        let mut report = DaemonTickReport::default();

        for record in self.journal.pending_claim_intents()? {
            resume_claim_intent(self.control.as_ref(), &mut self.journal, &record).await?;
            increment(&mut report.resumed_claims)?;
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use agentforge_application::{
        ClaimPackageInput, ClaimedWork, LeaseView, ListOffersQuery, MvpCommand, MvpError,
        MvpFuture, OfferView, PackageExecutionSnapshot, PortError, ReleaseLeaseInput,
        RenewLeaseInput,
    };
    use agentforge_domain::{
        ActorId, AggregateVersion, ExecutorId, FencingToken, GitObjectId, LeaseId, NodeId,
        PackageId, PackageRevision, ProtocolKey, Sha256Digest, lease::LeaseState,
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
        now: Mutex<ServerInstant>,
        claims: Mutex<u16>,
        renewals: Mutex<u16>,
        fail_after_claim: Mutex<bool>,
    }

    impl FakeControl {
        fn new(offers: Vec<OfferView>, work: Vec<ClaimedWork>, node_id: NodeId) -> Self {
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
                now: Mutex::new(at(0)),
                claims: Mutex::new(0),
                renewals: Mutex::new(0),
                fail_after_claim: Mutex::new(false),
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
