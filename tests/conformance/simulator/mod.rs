//! In-memory AFWP protocol fault simulator.
//!
//! This source is compiled by `agentforge-test-support`'s integration tests so
//! the workspace keeps its authoritative package count. It deliberately has no
//! socket, database, or Git adapter.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
};

use agentforge_test_support::{DeterministicRng, DeterministicUuidV7, FixedClock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const SCHEDULE_EPOCH_MILLIS: i64 = 1_754_611_200_000;
const COORDINATOR_SERVICE_ID: &str = "verification-coordinator";

/// Stable service identity allowed to finalize a verified Candidate.
#[must_use]
pub const fn coordinator_service_id() -> &'static str {
    COORDINATOR_SERVICE_ID
}

/// Deterministic verification job capability created with a Candidate.
#[must_use]
pub fn verification_job_id_for(candidate_id: &str) -> String {
    format!("verify:{candidate_id}")
}

/// The Lease identity and generation presented by an author command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeaseProof {
    pub lease_id: Uuid,
    pub attempt_id: Uuid,
    pub generation: u64,
}

/// A server-issued Lease used by generated author clients.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeaseGrant {
    pub lease_id: Uuid,
    pub attempt_id: Uuid,
    pub generation: u64,
    pub expires_at_unix_ms: i64,
}

impl LeaseGrant {
    /// Returns the immutable proof fields stored with an author command.
    #[must_use]
    pub fn proof(&self) -> LeaseProof {
        LeaseProof {
            lease_id: self.lease_id,
            attempt_id: self.attempt_id,
            generation: self.generation,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ActiveLease {
    grant: LeaseGrant,
    accepts_author_writes: bool,
}

/// Author-side effects guarded by the active Task Lease.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthorWrite {
    Checkpoint { checkpoint_id: String },
    RenewLease { extend_by_millis: i64 },
    RegisterArtifact { artifact_id: String },
    RecordCandidate { candidate_id: String },
}

/// A normalized author command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorCommand {
    pub actor_id: String,
    pub idempotency_key: String,
    pub lease: LeaseProof,
    pub write: AuthorWrite,
}

impl AuthorCommand {
    fn request_hash(&self) -> String {
        #[derive(Serialize)]
        struct RequestBody<'a> {
            lease: &'a LeaseProof,
            write: &'a AuthorWrite,
        }

        let body = serde_json::to_vec(&RequestBody {
            lease: &self.lease,
            write: &self.write,
        })
        .expect("author request body is serializable");
        sha256(&body)
    }
}

/// An independent verification Coordinator command.
///
/// It intentionally contains no author Lease proof. Authorization uses the
/// service identity and verification job capability, while
/// `expected_job_version` is the CAS input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CoordinatorCommand {
    pub service_id: String,
    pub idempotency_key: String,
    pub verification_job_id: String,
    pub expected_job_version: u64,
    pub candidate_id: String,
    pub submission_id: String,
}

impl CoordinatorCommand {
    fn request_hash(&self) -> String {
        #[derive(Serialize)]
        struct RequestBody<'a> {
            verification_job_id: &'a str,
            expected_job_version: u64,
            candidate_id: &'a str,
            submission_id: &'a str,
        }

        let body = serde_json::to_vec(&RequestBody {
            verification_job_id: &self.verification_job_id,
            expected_job_version: self.expected_job_version,
            candidate_id: &self.candidate_id,
            submission_id: &self.submission_id,
        })
        .expect("Coordinator request body is serializable");
        sha256(&body)
    }
}

/// Stable protocol failures used by the simulator.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProtocolError {
    IdempotencyKeyReused,
    LeaseStale,
    LeaseExpired,
    VersionStale,
    Forbidden,
    InvalidTransition,
}

impl ProtocolError {
    /// Returns the stable HTTP-boundary error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::IdempotencyKeyReused => "AF_IDEMPOTENCY_KEY_REUSED",
            Self::LeaseStale => "AF_LEASE_STALE",
            Self::LeaseExpired => "AF_LEASE_EXPIRED",
            Self::VersionStale => "AF_VERSION_STALE",
            Self::Forbidden => "AF_AUTH_FORBIDDEN",
            Self::InvalidTransition => "AF_TRANSITION_INVALID",
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for ProtocolError {}

/// The byte-stable business response stored in a command receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandResponse {
    pub effect_id: Uuid,
    pub aggregate_version: u64,
    pub resource_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum PrincipalRole {
    Author,
    Coordinator,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct ReceiptKey {
    role: PrincipalRole,
    principal_id: String,
    idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CommandReceipt {
    key: ReceiptKey,
    request_hash: String,
    response: CommandResponse,
}

/// Where the server obtained a command result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultOrigin {
    NewCommit,
    ReceiptReplay,
    Rejected,
}

/// Result of one client delivery. A missing `client_response` models a lost ACK.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandHandling {
    pub client_response: Option<Result<CommandResponse, ProtocolError>>,
    pub server_response: Option<CommandResponse>,
    pub origin: ResultOrigin,
    pub newly_committed: bool,
}

/// Fault injected at command handling time.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandFault {
    None,
    DropResponseAfterCommit,
    ExpireBeforeAuthorization,
}

/// Immutable protocol effect committed with an outbox row and receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProtocolEffect {
    Checkpoint {
        checkpoint_id: String,
    },
    ArtifactRegistered {
        artifact_id: String,
        author_generation: u64,
    },
    LeaseRenewed {
        lease_id: String,
        new_expires_at_unix_ms: i64,
        author_generation: u64,
    },
    CandidateRecorded {
        candidate_id: String,
        verification_job_id: String,
        author_generation: u64,
    },
    SubmissionFinalized {
        submission_id: String,
        candidate_id: String,
        verification_job_id: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DomainEffect {
    effect_id: Uuid,
    aggregate_version: u64,
    author_generation: Option<u64>,
    effect: ProtocolEffect,
}

/// Event held by the transactional outbox and delivered to the inbox.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutboxEvent {
    pub event_id: Uuid,
    pub effect_id: Uuid,
    pub aggregate_version: u64,
    pub author_generation: Option<u64>,
    pub effect: ProtocolEffect,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OutboxRow {
    event: OutboxEvent,
    publisher_acknowledged: bool,
}

/// Publisher delivery failures.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryFault {
    None,
    Duplicate,
    Reorder,
    DuplicateAndReorder,
    LosePublisherAck,
}

/// Counts from one outbox-to-inbox delivery attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliveryReport {
    pub delivered: usize,
    pub newly_applied: usize,
    pub pending_after_delivery: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct Inbox {
    received_event_ids: BTreeSet<Uuid>,
    applied_effects: BTreeMap<Uuid, ProtocolEffect>,
}

impl Inbox {
    fn consume(&mut self, events: Vec<OutboxEvent>) -> usize {
        let mut applied = 0;
        for event in events {
            if self.received_event_ids.insert(event.event_id)
                && self
                    .applied_effects
                    .insert(event.effect_id, event.effect)
                    .is_none()
            {
                applied += 1;
            }
        }
        applied
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CandidateRecord {
    candidate_id: String,
    author_generation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct VerificationJob {
    verification_job_id: String,
    candidate_id: String,
    version: u64,
    terminal: bool,
}

/// In-memory aggregate, verification job, receipt store, transactional outbox,
/// and deduplicating inbox.
#[derive(Clone, Debug)]
pub struct ProtocolSimulator {
    seed: u64,
    clock: FixedClock,
    ids: DeterministicUuidV7,
    delivery_rng: DeterministicRng,
    generation_counter: u64,
    aggregate_version: u64,
    current_lease: Option<ActiveLease>,
    candidate: Option<CandidateRecord>,
    verification_job: Option<VerificationJob>,
    formal_submission_id: Option<String>,
    effects: Vec<DomainEffect>,
    receipts: BTreeMap<ReceiptKey, CommandReceipt>,
    outbox: Vec<OutboxRow>,
    inbox: Inbox,
}

impl ProtocolSimulator {
    /// Creates an empty deterministic simulator.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let clock = FixedClock::from_unix_timestamp_millis(SCHEDULE_EPOCH_MILLIS);
        Self {
            seed,
            ids: DeterministicUuidV7::new(clock.clone(), seed),
            delivery_rng: DeterministicRng::new(seed).fork("outbox-delivery"),
            clock,
            generation_counter: 0,
            aggregate_version: 0,
            current_lease: None,
            candidate: None,
            verification_job: None,
            formal_submission_id: None,
            effects: Vec::new(),
            receipts: BTreeMap::new(),
            outbox: Vec::new(),
            inbox: Inbox::default(),
        }
    }

    /// Reassigns the package and issues the next strictly increasing generation.
    pub fn reassign(&mut self, ttl_millis: i64) -> Result<LeaseGrant, ProtocolError> {
        if ttl_millis <= 0 || self.candidate.is_some() {
            return Err(ProtocolError::InvalidTransition);
        }
        self.generation_counter = self
            .generation_counter
            .checked_add(1)
            .ok_or(ProtocolError::InvalidTransition)?;
        let grant = LeaseGrant {
            lease_id: self.ids.next_uuid(),
            attempt_id: self.ids.next_uuid(),
            generation: self.generation_counter,
            expires_at_unix_ms: self
                .clock
                .unix_timestamp_millis()
                .checked_add(ttl_millis)
                .ok_or(ProtocolError::InvalidTransition)?,
        };
        self.current_lease = Some(ActiveLease {
            grant: grant.clone(),
            accepts_author_writes: true,
        });
        Ok(grant)
    }

    /// Advances only the virtual server clock.
    pub fn advance_clock(&self, millis: i64) {
        self.clock.advance_millis(millis);
    }

    /// Moves the server clock exactly to the current Lease's expiry.
    pub fn expire_current(&self) -> bool {
        let Some(lease) = &self.current_lease else {
            return false;
        };
        let now = self.clock.unix_timestamp_millis();
        if now < lease.grant.expires_at_unix_ms {
            self.clock
                .advance_millis(lease.grant.expires_at_unix_ms - now);
        }
        true
    }

    /// Handles an author command. Receipt lookup precedes Lease fencing.
    pub fn handle_author(
        &mut self,
        command: &AuthorCommand,
        fault: CommandFault,
    ) -> CommandHandling {
        if fault == CommandFault::ExpireBeforeAuthorization {
            self.expire_current();
        }

        let key = ReceiptKey {
            role: PrincipalRole::Author,
            principal_id: command.actor_id.clone(),
            idempotency_key: command.idempotency_key.clone(),
        };
        let request_hash = command.request_hash();
        if let Some(handling) = self.replay_receipt(&key, &request_hash, fault) {
            return handling;
        }
        if let Err(error) = self.authorize_author(&command.lease) {
            return rejected(error);
        }
        if matches!(command.write, AuthorWrite::RecordCandidate { .. }) && self.candidate.is_some()
        {
            return rejected(ProtocolError::InvalidTransition);
        }

        if let AuthorWrite::RenewLease { extend_by_millis } = &command.write {
            if *extend_by_millis <= 0 {
                return rejected(ProtocolError::InvalidTransition);
            }
            let Some(renewed_expires_at) = self.current_lease.as_ref().and_then(|lease| {
                lease
                    .grant
                    .expires_at_unix_ms
                    .checked_add(*extend_by_millis)
            }) else {
                return rejected(ProtocolError::InvalidTransition);
            };
            let lease_id = self
                .current_lease
                .as_ref()
                .expect("authorized Lease remains present")
                .grant
                .lease_id
                .to_string();
            self.current_lease
                .as_mut()
                .expect("authorized Lease remains present")
                .grant
                .expires_at_unix_ms = renewed_expires_at;
            let response = self.commit_effect(
                key,
                request_hash,
                Some(command.lease.generation),
                ProtocolEffect::LeaseRenewed {
                    lease_id: lease_id.clone(),
                    new_expires_at_unix_ms: renewed_expires_at,
                    author_generation: command.lease.generation,
                },
                lease_id,
            );
            return successful_handling(response, ResultOrigin::NewCommit, true, fault);
        }

        let (effect, resource_id) = match &command.write {
            AuthorWrite::Checkpoint { checkpoint_id } => (
                ProtocolEffect::Checkpoint {
                    checkpoint_id: checkpoint_id.clone(),
                },
                checkpoint_id.clone(),
            ),
            AuthorWrite::RegisterArtifact { artifact_id } => (
                ProtocolEffect::ArtifactRegistered {
                    artifact_id: artifact_id.clone(),
                    author_generation: command.lease.generation,
                },
                artifact_id.clone(),
            ),
            AuthorWrite::RecordCandidate { candidate_id } => {
                let verification_job_id = verification_job_id_for(candidate_id);
                (
                    ProtocolEffect::CandidateRecorded {
                        candidate_id: candidate_id.clone(),
                        verification_job_id,
                        author_generation: command.lease.generation,
                    },
                    candidate_id.clone(),
                )
            }
            AuthorWrite::RenewLease { .. } => {
                unreachable!("RenewLease returns through its dedicated effect path")
            }
        };
        let response = self.commit_effect(
            key,
            request_hash,
            Some(command.lease.generation),
            effect,
            resource_id,
        );

        if let AuthorWrite::RecordCandidate { candidate_id } = &command.write {
            let verification_job_id = verification_job_id_for(candidate_id);
            self.candidate = Some(CandidateRecord {
                candidate_id: candidate_id.clone(),
                author_generation: command.lease.generation,
            });
            self.verification_job = Some(VerificationJob {
                verification_job_id,
                candidate_id: candidate_id.clone(),
                version: 1,
                terminal: false,
            });
            if let Some(lease) = &mut self.current_lease {
                lease.accepts_author_writes = false;
            }
        }

        successful_handling(response, ResultOrigin::NewCommit, true, fault)
    }

    /// Finalizes a terminal Submission from an immutable Candidate.
    ///
    /// This path never authorizes against the current author Lease. It verifies
    /// Coordinator identity, verification job capability, and job-version CAS.
    pub fn finalize_submission(
        &mut self,
        command: &CoordinatorCommand,
        fault: CommandFault,
    ) -> CommandHandling {
        if fault == CommandFault::ExpireBeforeAuthorization {
            self.expire_current();
        }

        let key = ReceiptKey {
            role: PrincipalRole::Coordinator,
            principal_id: command.service_id.clone(),
            idempotency_key: command.idempotency_key.clone(),
        };
        let request_hash = command.request_hash();
        if let Some(handling) = self.replay_receipt(&key, &request_hash, fault) {
            return handling;
        }
        if command.service_id != COORDINATOR_SERVICE_ID {
            return rejected(ProtocolError::Forbidden);
        }

        let Some(candidate) = self.candidate.clone() else {
            return rejected(ProtocolError::InvalidTransition);
        };
        let Some(job) = self.verification_job.clone() else {
            return rejected(ProtocolError::InvalidTransition);
        };
        if candidate.candidate_id != command.candidate_id
            || job.candidate_id != command.candidate_id
            || job.verification_job_id != command.verification_job_id
        {
            return rejected(ProtocolError::InvalidTransition);
        }
        if job.version != command.expected_job_version {
            return rejected(ProtocolError::VersionStale);
        }
        if job.terminal || self.formal_submission_id.is_some() {
            return rejected(ProtocolError::InvalidTransition);
        }

        let effect = ProtocolEffect::SubmissionFinalized {
            submission_id: command.submission_id.clone(),
            candidate_id: command.candidate_id.clone(),
            verification_job_id: command.verification_job_id.clone(),
        };
        let response = self.commit_effect(
            key,
            request_hash,
            None,
            effect,
            command.submission_id.clone(),
        );
        let verification_job = self
            .verification_job
            .as_mut()
            .expect("validated verification job remains present");
        verification_job.version = verification_job
            .version
            .checked_add(1)
            .expect("verification job version must not overflow");
        verification_job.terminal = true;
        self.formal_submission_id = Some(command.submission_id.clone());

        successful_handling(response, ResultOrigin::NewCommit, true, fault)
    }

    /// Publishes all pending outbox rows and immediately consumes the batch.
    pub fn publish_to_inbox(&mut self, fault: DeliveryFault) -> DeliveryReport {
        let pending: Vec<usize> = self
            .outbox
            .iter()
            .enumerate()
            .filter_map(|(index, row)| (!row.publisher_acknowledged).then_some(index))
            .collect();
        let mut batch: Vec<OutboxEvent> = pending
            .iter()
            .map(|index| self.outbox[*index].event.clone())
            .collect();

        if matches!(
            fault,
            DeliveryFault::Duplicate | DeliveryFault::DuplicateAndReorder
        ) {
            let duplicate = batch.clone();
            batch.extend(duplicate);
        }
        if matches!(
            fault,
            DeliveryFault::Reorder | DeliveryFault::DuplicateAndReorder
        ) {
            self.delivery_rng.shuffle(&mut batch);
        }
        if fault != DeliveryFault::LosePublisherAck {
            for index in pending {
                self.outbox[index].publisher_acknowledged = true;
            }
        }

        let delivered = batch.len();
        let newly_applied = self.inbox.consume(batch);
        DeliveryReport {
            delivered,
            newly_applied,
            pending_after_delivery: self.pending_outbox_count(),
        }
    }

    /// Returns the current generation, including expired or author-complete
    /// Lease rows.
    #[must_use]
    pub fn current_generation(&self) -> Option<u64> {
        self.current_lease
            .as_ref()
            .map(|lease| lease.grant.generation)
    }

    /// Returns a committed author receipt response.
    #[must_use]
    pub fn author_receipt_response(
        &self,
        actor_id: &str,
        idempotency_key: &str,
    ) -> Option<&CommandResponse> {
        self.receipt_response(PrincipalRole::Author, actor_id, idempotency_key)
    }

    /// Returns a committed Coordinator receipt response.
    #[must_use]
    pub fn coordinator_receipt_response(
        &self,
        service_id: &str,
        idempotency_key: &str,
    ) -> Option<&CommandResponse> {
        self.receipt_response(PrincipalRole::Coordinator, service_id, idempotency_key)
    }

    /// Number of committed domain effects.
    #[must_use]
    pub fn domain_effect_count(&self) -> usize {
        self.effects.len()
    }

    /// Number of artifact registration domain effects.
    #[must_use]
    pub fn artifact_count(&self) -> usize {
        self.effects
            .iter()
            .filter(|effect| matches!(effect.effect, ProtocolEffect::ArtifactRegistered { .. }))
            .count()
    }

    /// Number of durable outbox rows.
    #[must_use]
    pub fn outbox_event_count(&self) -> usize {
        self.outbox.len()
    }

    /// Number of outbox rows that can be published again.
    #[must_use]
    pub fn pending_outbox_count(&self) -> usize {
        self.outbox
            .iter()
            .filter(|row| !row.publisher_acknowledged)
            .count()
    }

    /// Number of immutable Candidate effects in the aggregate.
    #[must_use]
    pub fn candidate_count(&self) -> usize {
        self.effects
            .iter()
            .filter(|effect| matches!(effect.effect, ProtocolEffect::CandidateRecorded { .. }))
            .count()
    }

    /// Number of terminal Submission effects in the aggregate.
    #[must_use]
    pub fn formal_submission_count(&self) -> usize {
        self.effects
            .iter()
            .filter(|effect| matches!(effect.effect, ProtocolEffect::SubmissionFinalized { .. }))
            .count()
    }

    /// Number of Candidate effects applied by the inbox projection.
    #[must_use]
    pub fn projected_candidate_count(&self) -> usize {
        self.inbox
            .applied_effects
            .values()
            .filter(|effect| matches!(effect, ProtocolEffect::CandidateRecorded { .. }))
            .count()
    }

    /// Number of Submission effects applied by the inbox projection.
    #[must_use]
    pub fn projected_submission_count(&self) -> usize {
        self.inbox
            .applied_effects
            .values()
            .filter(|effect| matches!(effect, ProtocolEffect::SubmissionFinalized { .. }))
            .count()
    }

    /// Validates Candidate-first, Coordinator-finalized, and cross-store
    /// protocol invariants.
    pub fn validate(&self) -> Result<(), String> {
        let candidate_effects = self.candidate_count();
        let submission_effects = self.formal_submission_count();
        if candidate_effects > 1 {
            return Err("more than one immutable Candidate domain effect".to_owned());
        }
        if submission_effects > 1 {
            return Err("more than one terminal Submission domain effect".to_owned());
        }
        if self.projected_candidate_count() > 1 {
            return Err("more than one Candidate inbox effect".to_owned());
        }
        if self.projected_submission_count() > 1 {
            return Err("more than one Submission inbox effect".to_owned());
        }
        if self.candidate.is_some() != (candidate_effects == 1) {
            return Err("Candidate aggregate and domain effects disagree".to_owned());
        }
        if self.formal_submission_id.is_some() != (submission_effects == 1) {
            return Err("Submission aggregate and domain effects disagree".to_owned());
        }
        if submission_effects > candidate_effects {
            return Err("Submission exists without an immutable Candidate".to_owned());
        }
        if self.candidate.is_some() != self.verification_job.is_some() {
            return Err("Candidate and verification job existence disagree".to_owned());
        }
        if let (Some(candidate), Some(job)) = (&self.candidate, &self.verification_job) {
            if candidate.candidate_id != job.candidate_id
                || verification_job_id_for(&candidate.candidate_id) != job.verification_job_id
            {
                return Err("Candidate and verification job identity disagree".to_owned());
            }
            if job.terminal != self.formal_submission_id.is_some() {
                return Err("verification job terminal state and Submission disagree".to_owned());
            }
        }

        for effect in &self.effects {
            match (&effect.effect, effect.author_generation) {
                (ProtocolEffect::Checkpoint { .. }, Some(_)) => {}
                (
                    ProtocolEffect::ArtifactRegistered {
                        author_generation, ..
                    }
                    | ProtocolEffect::LeaseRenewed {
                        author_generation, ..
                    }
                    | ProtocolEffect::CandidateRecorded {
                        author_generation, ..
                    },
                    Some(generation),
                ) if *author_generation == generation => {}
                (ProtocolEffect::SubmissionFinalized { .. }, None) => {}
                _ => {
                    return Err(
                        "Coordinator effect was tied to an author generation or vice versa"
                            .to_owned(),
                    );
                }
            }
        }
        if let Some(candidate) = &self.candidate {
            let candidate_effect = self.effects.iter().find_map(|effect| match &effect.effect {
                ProtocolEffect::CandidateRecorded {
                    candidate_id,
                    author_generation,
                    ..
                } => Some((candidate_id, author_generation)),
                _ => None,
            });
            if candidate_effect != Some((&candidate.candidate_id, &candidate.author_generation)) {
                return Err("immutable Candidate fields disagree with its effect".to_owned());
            }
        }
        if let (Some(submission_id), Some(candidate), Some(job)) = (
            &self.formal_submission_id,
            &self.candidate,
            &self.verification_job,
        ) {
            let matches_terminal_effect = self.effects.iter().any(|effect| {
                matches!(
                    &effect.effect,
                    ProtocolEffect::SubmissionFinalized {
                        submission_id: effect_submission,
                        candidate_id,
                        verification_job_id,
                    } if effect_submission == submission_id
                        && candidate_id == &candidate.candidate_id
                        && verification_job_id == &job.verification_job_id
                )
            });
            if !matches_terminal_effect {
                return Err("Submission lineage disagrees with Candidate/job".to_owned());
            }
        }

        if self.effects.len() != self.outbox.len() {
            return Err("domain effect and outbox row counts disagree".to_owned());
        }
        let effect_ids: BTreeSet<_> = self.effects.iter().map(|effect| effect.effect_id).collect();
        if effect_ids.len() != self.effects.len() {
            return Err("duplicate domain effect identifier".to_owned());
        }
        let event_ids: BTreeSet<_> = self.outbox.iter().map(|row| row.event.event_id).collect();
        if event_ids.len() != self.outbox.len() {
            return Err("duplicate outbox event identifier".to_owned());
        }
        if self
            .outbox
            .iter()
            .any(|row| !effect_ids.contains(&row.event.effect_id))
        {
            return Err("outbox event has no committed domain effect".to_owned());
        }
        if self.receipts.values().any(|receipt| {
            !effect_ids.contains(&receipt.response.effect_id)
                || receipt.response.aggregate_version == 0
        }) {
            return Err("receipt has no committed domain effect".to_owned());
        }
        if self
            .inbox
            .applied_effects
            .keys()
            .any(|effect_id| !effect_ids.contains(effect_id))
        {
            return Err("inbox projection has no committed domain effect".to_owned());
        }
        Ok(())
    }

    /// Returns a deterministic digest of all simulated durable state.
    #[must_use]
    pub fn state_digest(&self) -> String {
        #[derive(Serialize)]
        struct Snapshot<'a> {
            seed: u64,
            now_unix_ms: i64,
            generation_counter: u64,
            aggregate_version: u64,
            current_lease: &'a Option<ActiveLease>,
            candidate: &'a Option<CandidateRecord>,
            verification_job: &'a Option<VerificationJob>,
            formal_submission_id: &'a Option<String>,
            effects: &'a [DomainEffect],
            receipts: Vec<&'a CommandReceipt>,
            outbox: &'a [OutboxRow],
            inbox: &'a Inbox,
        }

        let bytes = serde_json::to_vec(&Snapshot {
            seed: self.seed,
            now_unix_ms: self.clock.unix_timestamp_millis(),
            generation_counter: self.generation_counter,
            aggregate_version: self.aggregate_version,
            current_lease: &self.current_lease,
            candidate: &self.candidate,
            verification_job: &self.verification_job,
            formal_submission_id: &self.formal_submission_id,
            effects: &self.effects,
            receipts: self.receipts.values().collect(),
            outbox: &self.outbox,
            inbox: &self.inbox,
        })
        .expect("simulator snapshot is serializable");
        sha256(&bytes)
    }

    fn authorize_author(&self, proof: &LeaseProof) -> Result<(), ProtocolError> {
        let lease = self
            .current_lease
            .as_ref()
            .ok_or(ProtocolError::LeaseStale)?;
        if !lease.accepts_author_writes
            || lease.grant.lease_id != proof.lease_id
            || lease.grant.attempt_id != proof.attempt_id
            || lease.grant.generation != proof.generation
        {
            return Err(ProtocolError::LeaseStale);
        }
        if self.clock.unix_timestamp_millis() >= lease.grant.expires_at_unix_ms {
            return Err(ProtocolError::LeaseExpired);
        }
        Ok(())
    }

    fn replay_receipt(
        &self,
        key: &ReceiptKey,
        request_hash: &str,
        fault: CommandFault,
    ) -> Option<CommandHandling> {
        self.receipts.get(key).map(|receipt| {
            if receipt.request_hash == request_hash {
                successful_handling(
                    receipt.response.clone(),
                    ResultOrigin::ReceiptReplay,
                    false,
                    fault,
                )
            } else {
                rejected(ProtocolError::IdempotencyKeyReused)
            }
        })
    }

    fn commit_effect(
        &mut self,
        key: ReceiptKey,
        request_hash: String,
        author_generation: Option<u64>,
        effect: ProtocolEffect,
        resource_id: String,
    ) -> CommandResponse {
        let next_version = self
            .aggregate_version
            .checked_add(1)
            .expect("simulator aggregate version must not overflow");
        let effect_id = self.ids.next_uuid();
        let event_id = self.ids.next_uuid();
        let response = CommandResponse {
            effect_id,
            aggregate_version: next_version,
            resource_id,
        };

        // These writes are the simulator's single database transaction.
        self.aggregate_version = next_version;
        self.effects.push(DomainEffect {
            effect_id,
            aggregate_version: next_version,
            author_generation,
            effect: effect.clone(),
        });
        self.outbox.push(OutboxRow {
            event: OutboxEvent {
                event_id,
                effect_id,
                aggregate_version: next_version,
                author_generation,
                effect,
            },
            publisher_acknowledged: false,
        });
        self.receipts.insert(
            key.clone(),
            CommandReceipt {
                key,
                request_hash,
                response: response.clone(),
            },
        );
        response
    }

    fn receipt_response(
        &self,
        role: PrincipalRole,
        principal_id: &str,
        idempotency_key: &str,
    ) -> Option<&CommandResponse> {
        self.receipts
            .get(&ReceiptKey {
                role,
                principal_id: principal_id.to_owned(),
                idempotency_key: idempotency_key.to_owned(),
            })
            .map(|receipt| &receipt.response)
    }
}

fn successful_handling(
    response: CommandResponse,
    origin: ResultOrigin,
    newly_committed: bool,
    fault: CommandFault,
) -> CommandHandling {
    let client_response =
        (fault != CommandFault::DropResponseAfterCommit).then(|| Ok(response.clone()));
    CommandHandling {
        client_response,
        server_response: Some(response),
        origin,
        newly_committed,
    }
}

fn rejected(error: ProtocolError) -> CommandHandling {
    CommandHandling {
        client_response: Some(Err(error)),
        server_response: None,
        origin: ResultOrigin::Rejected,
        newly_committed: false,
    }
}

/// Additional deterministic oracle used to demonstrate failure-seed replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InvariantOracle {
    Protocol,
    MaximumGeneration { maximum: u64 },
}

/// One generated scheduler action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleAction {
    Reassign {
        ttl_millis: i64,
    },
    AdvanceClock {
        millis: i64,
    },
    ExpireCurrent,
    HandleAuthor {
        command: AuthorCommand,
        fault: CommandFault,
    },
    RetryAuthor {
        command_index: usize,
        fault: CommandFault,
    },
    ReuseAuthorKeyWithDifferentPayload {
        command_index: usize,
    },
    FinalizeSubmission {
        command: CoordinatorCommand,
        fault: CommandFault,
    },
    RetryCoordinator {
        command_index: usize,
        fault: CommandFault,
    },
    ReuseCoordinatorKeyWithDifferentPayload {
        command_index: usize,
    },
    Publish {
        fault: DeliveryFault,
    },
}

/// An action and its stable outcome, retained for shrinking/debugging.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduleTraceEntry {
    pub step: usize,
    pub action: ScheduleAction,
    pub outcome: String,
}

/// Fault and ordering cases actually reached by a schedule.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduleCoverage {
    pub candidate_receipt_replay: bool,
    pub finalize_receipt_replay: bool,
    pub finalize_before_candidate_rejected: bool,
    pub stale_generation_rejected: bool,
    pub expired_author_rejected: bool,
    pub renew_stale_generation_rejected: bool,
    pub renew_expired_rejected: bool,
    pub renew_fresh_committed: bool,
    pub renew_receipt_replay: bool,
    pub register_artifact_stale_generation_rejected: bool,
    pub register_artifact_expired_rejected: bool,
    pub register_artifact_fresh_committed: bool,
    pub register_artifact_receipt_replay: bool,
    pub command_response_dropped: bool,
    pub duplicate_delivery: bool,
    pub reordered_delivery: bool,
    pub publisher_ack_lost: bool,
}

impl ScheduleCoverage {
    /// Merges reached cases from one schedule into an aggregate matrix.
    pub fn merge(&mut self, other: &Self) {
        self.candidate_receipt_replay |= other.candidate_receipt_replay;
        self.finalize_receipt_replay |= other.finalize_receipt_replay;
        self.finalize_before_candidate_rejected |= other.finalize_before_candidate_rejected;
        self.stale_generation_rejected |= other.stale_generation_rejected;
        self.expired_author_rejected |= other.expired_author_rejected;
        self.renew_stale_generation_rejected |= other.renew_stale_generation_rejected;
        self.renew_expired_rejected |= other.renew_expired_rejected;
        self.renew_fresh_committed |= other.renew_fresh_committed;
        self.renew_receipt_replay |= other.renew_receipt_replay;
        self.register_artifact_stale_generation_rejected |=
            other.register_artifact_stale_generation_rejected;
        self.register_artifact_expired_rejected |= other.register_artifact_expired_rejected;
        self.register_artifact_fresh_committed |= other.register_artifact_fresh_committed;
        self.register_artifact_receipt_replay |= other.register_artifact_receipt_replay;
        self.command_response_dropped |= other.command_response_dropped;
        self.duplicate_delivery |= other.duplicate_delivery;
        self.reordered_delivery |= other.reordered_delivery;
        self.publisher_ack_lost |= other.publisher_ack_lost;
    }
}

/// Successful seeded schedule report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduleReport {
    pub seed: u64,
    pub steps: usize,
    pub trace: Vec<ScheduleTraceEntry>,
    pub coverage: ScheduleCoverage,
    pub state_digest: String,
}

/// Persistable failure artifact containing the exact replay seed and oracle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduleFailure {
    pub schema_version: String,
    pub seed: u64,
    pub steps: usize,
    pub failed_at_step: usize,
    pub invariant: String,
    pub oracle: InvariantOracle,
    pub trace: Vec<ScheduleTraceEntry>,
    pub state_digest: String,
}

impl ScheduleFailure {
    /// Writes a stable failure artifact and returns its path.
    pub fn persist(&self, directory: &Path, test_name: &str) -> io::Result<PathBuf> {
        fs::create_dir_all(directory)?;
        let safe_name: String = test_name
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                    character
                } else {
                    '_'
                }
            })
            .collect();
        let path = directory.join(format!("{safe_name}__seed-{}.json", self.seed));
        let mut bytes = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        bytes.push(b'\n');
        fs::write(&path, bytes)?;
        Ok(path)
    }

    /// Loads an artifact produced by [`Self::persist`].
    pub fn load(path: &Path) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
}

/// Runs a schedule using only the protocol invariants.
pub fn run_seed(seed: u64, steps: usize) -> Result<ScheduleReport, Box<ScheduleFailure>> {
    run_seed_with_oracle(seed, steps, InvariantOracle::Protocol)
}

/// Runs a schedule with an extra replayable invariant oracle.
pub fn run_seed_with_oracle(
    seed: u64,
    steps: usize,
    oracle: InvariantOracle,
) -> Result<ScheduleReport, Box<ScheduleFailure>> {
    let mut simulator = ProtocolSimulator::new(seed);
    let mut scheduler_rng = DeterministicRng::new(seed).fork("protocol-schedule");
    let mut grants = Vec::new();
    let mut author_commands = Vec::new();
    let mut coordinator_commands = Vec::new();
    let mut trace = Vec::with_capacity(steps.saturating_add(1));
    let mut coverage = ScheduleCoverage::default();

    let initial_action = ScheduleAction::Reassign { ttl_millis: 100 };
    let initial = simulator
        .reassign(100)
        .expect("initial deterministic Lease grant must succeed");
    grants.push(initial.clone());
    trace.push(ScheduleTraceEntry {
        step: 0,
        action: initial_action,
        outcome: format!("granted:g{}", initial.generation),
    });
    validate_schedule(&simulator, seed, steps, 0, &oracle, &trace)?;

    for step in 1..=steps {
        let action = generate_action(
            seed,
            step,
            &mut scheduler_rng,
            &grants,
            &author_commands,
            &coordinator_commands,
        );
        let outcome = execute_action(
            &mut simulator,
            &action,
            &mut grants,
            &mut author_commands,
            &mut coordinator_commands,
            &mut coverage,
        );
        trace.push(ScheduleTraceEntry {
            step,
            action,
            outcome,
        });
        validate_schedule(&simulator, seed, steps, step, &oracle, &trace)?;
    }

    Ok(ScheduleReport {
        seed,
        steps,
        trace,
        coverage,
        state_digest: simulator.state_digest(),
    })
}

/// Replays a persisted failure and requires the exact failure to recur.
pub fn replay_persisted_failure(path: &Path) -> io::Result<ScheduleFailure> {
    let expected = ScheduleFailure::load(path)?;
    match run_seed_with_oracle(expected.seed, expected.steps, expected.oracle.clone()) {
        Ok(_) => Err(io::Error::other(
            "persisted simulator failure did not reproduce",
        )),
        Err(actual) if *actual == expected => Ok(*actual),
        Err(_) => Err(io::Error::other(
            "persisted simulator failure reproduced differently",
        )),
    }
}

fn generate_action(
    seed: u64,
    step: usize,
    rng: &mut DeterministicRng,
    grants: &[LeaseGrant],
    author_commands: &[AuthorCommand],
    coordinator_commands: &[CoordinatorCommand],
) -> ScheduleAction {
    let choice = rng.range_u64(0, 20);
    match choice {
        0 => ScheduleAction::Reassign {
            ttl_millis: i64::try_from(rng.range_u64(10, 151)).expect("TTL fits in i64"),
        },
        1 => ScheduleAction::AdvanceClock {
            millis: i64::try_from(rng.range_u64(0, 61)).expect("clock step fits in i64"),
        },
        2 => ScheduleAction::ExpireCurrent,
        3..=10 => {
            let grant_index = if rng.ratio(3, 4) {
                grants.len() - 1
            } else {
                rng.index(grants.len())
            };
            let write = match choice {
                4 | 7 => AuthorWrite::RecordCandidate {
                    candidate_id: format!("candidate-{seed}-{step}"),
                },
                5 | 8 | 9 => AuthorWrite::RenewLease {
                    extend_by_millis: i64::try_from(rng.range_u64(1, 101))
                        .expect("renew extension fits in i64"),
                },
                6 | 10 => AuthorWrite::RegisterArtifact {
                    artifact_id: format!("artifact-{seed}-{step}"),
                },
                _ => AuthorWrite::Checkpoint {
                    checkpoint_id: format!("checkpoint-{seed}-{step}"),
                },
            };
            let fault = match choice {
                7 | 8 => CommandFault::DropResponseAfterCommit,
                9 | 10 => CommandFault::ExpireBeforeAuthorization,
                _ => CommandFault::None,
            };
            ScheduleAction::HandleAuthor {
                command: AuthorCommand {
                    actor_id: "worker-tokyo-03".to_owned(),
                    idempotency_key: format!("author:seed:{seed}:step:{step}"),
                    lease: grants[grant_index].proof(),
                    write,
                },
                fault,
            }
        }
        11 if !author_commands.is_empty() => ScheduleAction::RetryAuthor {
            command_index: rng.index(author_commands.len()),
            fault: if rng.ratio(1, 4) {
                CommandFault::DropResponseAfterCommit
            } else {
                CommandFault::None
            },
        },
        12 if !author_commands.is_empty() => ScheduleAction::ReuseAuthorKeyWithDifferentPayload {
            command_index: rng.index(author_commands.len()),
        },
        13..=15 => {
            let candidate_id = generated_candidate_id(seed, step, rng, author_commands);
            ScheduleAction::FinalizeSubmission {
                command: CoordinatorCommand {
                    service_id: if rng.ratio(19, 20) {
                        COORDINATOR_SERVICE_ID.to_owned()
                    } else {
                        "author-worker".to_owned()
                    },
                    idempotency_key: format!("coordinator:seed:{seed}:step:{step}"),
                    verification_job_id: verification_job_id_for(&candidate_id),
                    expected_job_version: if choice == 15 { 0 } else { 1 },
                    candidate_id,
                    submission_id: format!("submission-{seed}-{step}"),
                },
                fault: if choice == 14 {
                    CommandFault::DropResponseAfterCommit
                } else {
                    CommandFault::None
                },
            }
        }
        16 if !coordinator_commands.is_empty() => ScheduleAction::RetryCoordinator {
            command_index: rng.index(coordinator_commands.len()),
            fault: if rng.ratio(1, 4) {
                CommandFault::DropResponseAfterCommit
            } else {
                CommandFault::None
            },
        },
        17 if !coordinator_commands.is_empty() => {
            ScheduleAction::ReuseCoordinatorKeyWithDifferentPayload {
                command_index: rng.index(coordinator_commands.len()),
            }
        }
        18 => ScheduleAction::Publish {
            fault: match rng.range_u64(0, 4) {
                0 => DeliveryFault::None,
                1 => DeliveryFault::Duplicate,
                2 => DeliveryFault::Reorder,
                _ => DeliveryFault::DuplicateAndReorder,
            },
        },
        _ => ScheduleAction::Publish {
            fault: DeliveryFault::LosePublisherAck,
        },
    }
}

fn generated_candidate_id(
    seed: u64,
    step: usize,
    rng: &mut DeterministicRng,
    author_commands: &[AuthorCommand],
) -> String {
    let candidates: Vec<&str> = author_commands
        .iter()
        .filter_map(|command| match &command.write {
            AuthorWrite::RecordCandidate { candidate_id } => Some(candidate_id.as_str()),
            AuthorWrite::Checkpoint { .. }
            | AuthorWrite::RenewLease { .. }
            | AuthorWrite::RegisterArtifact { .. } => None,
        })
        .collect();
    if candidates.is_empty() {
        format!("candidate-future-{seed}-{step}")
    } else {
        candidates[rng.index(candidates.len())].to_owned()
    }
}

fn execute_action(
    simulator: &mut ProtocolSimulator,
    action: &ScheduleAction,
    grants: &mut Vec<LeaseGrant>,
    author_commands: &mut Vec<AuthorCommand>,
    coordinator_commands: &mut Vec<CoordinatorCommand>,
    coverage: &mut ScheduleCoverage,
) -> String {
    match action {
        ScheduleAction::Reassign { ttl_millis } => match simulator.reassign(*ttl_millis) {
            Ok(grant) => {
                let outcome = format!("granted:g{}", grant.generation);
                grants.push(grant);
                outcome
            }
            Err(error) => error.code().to_owned(),
        },
        ScheduleAction::AdvanceClock { millis } => {
            simulator.advance_clock(*millis);
            format!("advanced:{millis}")
        }
        ScheduleAction::ExpireCurrent => format!("expired:{}", simulator.expire_current()),
        ScheduleAction::HandleAuthor { command, fault } => {
            author_commands.push(command.clone());
            let current_generation = simulator.current_generation().unwrap_or_default();
            let handling = simulator.handle_author(command, *fault);
            observe_author_handling(command, current_generation, &handling, coverage);
            handling_summary(&handling)
        }
        ScheduleAction::RetryAuthor {
            command_index,
            fault,
        } => {
            let command = &author_commands[*command_index];
            let current_generation = simulator.current_generation().unwrap_or_default();
            let handling = simulator.handle_author(command, *fault);
            observe_author_handling(command, current_generation, &handling, coverage);
            handling_summary(&handling)
        }
        ScheduleAction::ReuseAuthorKeyWithDifferentPayload { command_index } => {
            let mut changed = author_commands[*command_index].clone();
            changed.write = match changed.write {
                AuthorWrite::Checkpoint { .. } => AuthorWrite::RegisterArtifact {
                    artifact_id: format!("changed-artifact-{command_index}"),
                },
                AuthorWrite::RecordCandidate { .. } => AuthorWrite::Checkpoint {
                    checkpoint_id: format!("changed-checkpoint-{command_index}"),
                },
                AuthorWrite::RenewLease { extend_by_millis } => AuthorWrite::RenewLease {
                    extend_by_millis: extend_by_millis.saturating_add(1),
                },
                AuthorWrite::RegisterArtifact { .. } => AuthorWrite::RegisterArtifact {
                    artifact_id: format!("changed-artifact-{command_index}"),
                },
            };
            let current_generation = simulator.current_generation().unwrap_or_default();
            let handling = simulator.handle_author(&changed, CommandFault::None);
            observe_author_handling(&changed, current_generation, &handling, coverage);
            handling_summary(&handling)
        }
        ScheduleAction::FinalizeSubmission { command, fault } => {
            coordinator_commands.push(command.clone());
            let candidate_missing = simulator.candidate_count() == 0;
            let handling = simulator.finalize_submission(command, *fault);
            observe_coordinator_handling(candidate_missing, &handling, coverage);
            handling_summary(&handling)
        }
        ScheduleAction::RetryCoordinator {
            command_index,
            fault,
        } => {
            let candidate_missing = simulator.candidate_count() == 0;
            let handling =
                simulator.finalize_submission(&coordinator_commands[*command_index], *fault);
            observe_coordinator_handling(candidate_missing, &handling, coverage);
            handling_summary(&handling)
        }
        ScheduleAction::ReuseCoordinatorKeyWithDifferentPayload { command_index } => {
            let mut changed = coordinator_commands[*command_index].clone();
            changed.submission_id = format!("changed-submission-{command_index}");
            let candidate_missing = simulator.candidate_count() == 0;
            let handling = simulator.finalize_submission(&changed, CommandFault::None);
            observe_coordinator_handling(candidate_missing, &handling, coverage);
            handling_summary(&handling)
        }
        ScheduleAction::Publish { fault } => {
            coverage.duplicate_delivery |= matches!(
                fault,
                DeliveryFault::Duplicate | DeliveryFault::DuplicateAndReorder
            );
            coverage.reordered_delivery |= matches!(
                fault,
                DeliveryFault::Reorder | DeliveryFault::DuplicateAndReorder
            );
            coverage.publisher_ack_lost |= *fault == DeliveryFault::LosePublisherAck;
            let report = simulator.publish_to_inbox(*fault);
            format!(
                "delivered:{}:applied:{}:pending:{}",
                report.delivered, report.newly_applied, report.pending_after_delivery
            )
        }
    }
}

fn observe_author_handling(
    command: &AuthorCommand,
    current_generation: u64,
    handling: &CommandHandling,
    coverage: &mut ScheduleCoverage,
) {
    if handling.client_response.is_none() {
        coverage.command_response_dropped = true;
    }
    if matches!(command.write, AuthorWrite::RecordCandidate { .. })
        && handling.origin == ResultOrigin::ReceiptReplay
    {
        coverage.candidate_receipt_replay = true;
    }
    if command.lease.generation < current_generation
        && handling.client_response == Some(Err(ProtocolError::LeaseStale))
    {
        coverage.stale_generation_rejected = true;
    }
    if handling.client_response == Some(Err(ProtocolError::LeaseExpired)) {
        coverage.expired_author_rejected = true;
    }
    match &command.write {
        AuthorWrite::RenewLease { .. } => {
            coverage.renew_receipt_replay |= handling.origin == ResultOrigin::ReceiptReplay;
            coverage.renew_fresh_committed |=
                handling.origin == ResultOrigin::NewCommit && handling.newly_committed;
            coverage.renew_stale_generation_rejected |= command.lease.generation
                < current_generation
                && handling.client_response == Some(Err(ProtocolError::LeaseStale));
            coverage.renew_expired_rejected |=
                handling.client_response == Some(Err(ProtocolError::LeaseExpired));
        }
        AuthorWrite::RegisterArtifact { .. } => {
            coverage.register_artifact_receipt_replay |=
                handling.origin == ResultOrigin::ReceiptReplay;
            coverage.register_artifact_fresh_committed |=
                handling.origin == ResultOrigin::NewCommit && handling.newly_committed;
            coverage.register_artifact_stale_generation_rejected |= command.lease.generation
                < current_generation
                && handling.client_response == Some(Err(ProtocolError::LeaseStale));
            coverage.register_artifact_expired_rejected |=
                handling.client_response == Some(Err(ProtocolError::LeaseExpired));
        }
        AuthorWrite::Checkpoint { .. } | AuthorWrite::RecordCandidate { .. } => {}
    }
}

fn observe_coordinator_handling(
    candidate_missing: bool,
    handling: &CommandHandling,
    coverage: &mut ScheduleCoverage,
) {
    if handling.client_response.is_none() {
        coverage.command_response_dropped = true;
    }
    if handling.origin == ResultOrigin::ReceiptReplay {
        coverage.finalize_receipt_replay = true;
    }
    if candidate_missing && handling.origin == ResultOrigin::Rejected {
        coverage.finalize_before_candidate_rejected = true;
    }
}

fn handling_summary(handling: &CommandHandling) -> String {
    match &handling.client_response {
        Some(Ok(response)) => format!(
            "ok:{}:{}:{:?}",
            response.aggregate_version, handling.newly_committed, handling.origin
        ),
        Some(Err(error)) => error.code().to_owned(),
        None => format!("response_dropped:{:?}", handling.origin),
    }
}

fn validate_schedule(
    simulator: &ProtocolSimulator,
    seed: u64,
    steps: usize,
    failed_at_step: usize,
    oracle: &InvariantOracle,
    trace: &[ScheduleTraceEntry],
) -> Result<(), Box<ScheduleFailure>> {
    let protocol_result = simulator.validate();
    let extra_result = match oracle {
        InvariantOracle::Protocol => Ok(()),
        InvariantOracle::MaximumGeneration { maximum } => {
            if simulator.current_generation().unwrap_or_default() <= *maximum {
                Ok(())
            } else {
                Err(format!(
                    "generation {} exceeds replay oracle maximum {maximum}",
                    simulator.current_generation().unwrap_or_default()
                ))
            }
        }
    };
    protocol_result.and(extra_result).map_err(|invariant| {
        Box::new(ScheduleFailure {
            schema_version: "agentforge.simulator-failure/v1".to_owned(),
            seed,
            steps,
            failed_at_step,
            invariant,
            oracle: oracle.clone(),
            trace: trace.to_vec(),
            state_digest: simulator.state_digest(),
        })
    })
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
