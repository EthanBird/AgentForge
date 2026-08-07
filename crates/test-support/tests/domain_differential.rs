use agentforge_domain::{
    ActorId, AggregateVersion, Attempt, AttemptId, CandidateArtifactId, CandidateId, CommandId,
    CommandMetadata, CommandReceipt, CorrelationId, DomainError, ExecutorId, FencingToken,
    GitObjectId, IdempotencyKey, IdempotencyScope, Lease, LeaseId, NodeId, PackageId,
    PackageRevision, PackageRevisionId, ProjectId, ProtocolKey, ReceiptDecision, ServerInstant,
    Sha256Digest, Submission, SubmissionId, VerificationRunId,
    attempt::{AttemptCommand, AttemptEvent, AttemptState, NewAttempt},
    check_receipt,
    lease::{GrantLease, LeaseCommand, LeaseProof as DomainLeaseProof, LeaseState},
    submission::{
        AcceptanceFacts, CompletedStage, CriterionOutcome, SubmissionCommand, SubmissionRecord,
        SubmissionState,
    },
    work_package::{
        ClaimReadiness, NewWorkPackage, PublishReadiness, VerificationOutcome, WorkPackageCommand,
        WorkPackageState,
    },
};
use time::OffsetDateTime;
use uuid::Uuid;

#[path = "../../../tests/conformance/simulator/mod.rs"]
#[allow(dead_code)] // This differential binary intentionally exercises a simulator subset.
mod simulator;

use simulator::{
    AuthorCommand, AuthorWrite, CommandFault, CoordinatorCommand, ProtocolError, ProtocolSimulator,
    ResultOrigin, coordinator_service_id, verification_job_id_for,
};

const EPOCH_MILLIS: i64 = 1_754_611_200_000;

fn typed_id<T: From<Uuid>>(value: u128) -> T {
    T::from(Uuid::from_u128(value))
}

fn server_time(unix_millis: i64) -> ServerInstant {
    ServerInstant::new(
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(unix_millis) * 1_000_000)
            .expect("fixture timestamp is representable"),
    )
}

struct RealDomainTrace {
    package: agentforge_domain::WorkPackage,
    leases: Vec<Lease>,
    attempts: Vec<Attempt>,
    attempt_seeds: Vec<NewAttempt>,
    attempt_events: Vec<Vec<AttemptEvent>>,
    submission: Option<Submission>,
    domain_event_count: usize,
    mapped_effect_count: usize,
    mapped_outbox_count: usize,
    project_id: ProjectId,
    actor_id: ActorId,
    holder_node_id: NodeId,
    next_command_id: u128,
}

impl RealDomainTrace {
    fn offered() -> Self {
        let project_id = typed_id(1);
        let mut package = agentforge_domain::WorkPackage::new(NewWorkPackage {
            id: typed_id(2),
            project_id,
            selected_revision_id: typed_id(3),
            selected_revision: PackageRevision::new(1).unwrap(),
            graph_version: 1,
            priority: 0,
            max_attempts: 16,
        })
        .unwrap();
        let mut domain_event_count = 0;
        for command in [
            WorkPackageCommand::RequestValidation {
                expected_version: package.version,
                revision_exists: true,
                package_hash_matches: true,
            },
            WorkPackageCommand::PublishValidatedPackage {
                expected_version: AggregateVersion::new(1),
                readiness: PublishReadiness {
                    dor_passed: true,
                    dag_valid: true,
                    budget_available: true,
                    permissions_valid: true,
                },
            },
        ] {
            let transition = package.transition(&command).unwrap();
            domain_event_count += transition.events.len();
            package = transition.aggregate;
        }
        assert_eq!(package.state, WorkPackageState::Offered);
        Self {
            package,
            leases: Vec::new(),
            attempts: Vec::new(),
            attempt_seeds: Vec::new(),
            attempt_events: Vec::new(),
            submission: None,
            domain_event_count,
            mapped_effect_count: 0,
            mapped_outbox_count: 0,
            project_id,
            actor_id: typed_id(4),
            holder_node_id: typed_id(5),
            next_command_id: 100,
        }
    }

    fn transition_package(&mut self, command: WorkPackageCommand) {
        let transition = self.package.transition(&command).unwrap();
        self.domain_event_count += transition.events.len();
        self.package = transition.aggregate;
    }

    fn reassign(&mut self, grant: &simulator::LeaseGrant, granted_at_millis: i64) {
        if self.package.state == WorkPackageState::Active {
            let attempt_id = self
                .package
                .active_attempt_id
                .expect("active package has an Attempt");
            self.transition_current_attempt(AttemptCommand::MarkLost {
                expected_version: self.current_attempt().version,
            })
            .unwrap();
            assert_eq!(self.current_attempt().state, AttemptState::Lost);
            let current = self.leases.last().expect("active package has a Lease");
            let transition = Lease::transition(
                Some(current),
                &LeaseCommand::RevokeLease {
                    expected_version: current.version,
                    policy_authorized: true,
                    reason_code: "reassigned".to_owned(),
                },
            )
            .unwrap();
            self.domain_event_count += transition.events.len();
            *self.leases.last_mut().unwrap() = transition.aggregate;
            self.transition_package(WorkPackageCommand::LoseAttempt {
                expected_version: self.package.version,
                attempt_id,
                recoverable: true,
            });
        }

        let attempt_id = AttemptId::from(grant.attempt_id);
        let lease_id = LeaseId::from(grant.lease_id);
        self.transition_package(WorkPackageCommand::GrantLease {
            expected_version: self.package.version,
            attempt_id,
            lease_id,
            readiness: ClaimReadiness {
                dependencies_satisfied: true,
                budget_available: true,
                no_active_lease: true,
            },
        });
        let fencing_token = FencingToken::new(grant.generation).unwrap();
        assert_eq!(self.package.active_fencing_token, Some(fencing_token));
        let transition = Lease::transition(
            None,
            &LeaseCommand::GrantLease(GrantLease {
                id: lease_id,
                package_id: self.package.id,
                revision_id: self.package.selected_revision_id,
                attempt_id,
                holder_node_id: self.holder_node_id,
                previous_fencing_token: (grant.generation > 1)
                    .then(|| FencingToken::new(grant.generation - 1).unwrap()),
                fencing_token,
                granted_at: server_time(granted_at_millis),
                expires_at: server_time(grant.expires_at_unix_ms),
                max_expires_at: server_time(grant.expires_at_unix_ms + 10_000),
            }),
        )
        .unwrap();
        self.domain_event_count += transition.events.len();
        self.leases.push(transition.aggregate);

        let seed = NewAttempt {
            id: attempt_id,
            package_id: self.package.id,
            revision_id: self.package.selected_revision_id,
            executor_id: typed_id::<ExecutorId>(6),
            node_id: self.holder_node_id,
            fencing_token,
            base_commit: GitObjectId::new("b".repeat(40)).unwrap(),
        };
        self.attempts.push(Attempt::new(seed.clone()));
        self.attempt_seeds.push(seed);
        self.attempt_events.push(Vec::new());
        self.transition_current_attempt(AttemptCommand::AttachLease {
            expected_version: AggregateVersion::ZERO,
            lease_id,
            fencing_token,
            lineage_matches: true,
        })
        .unwrap();
        assert_eq!(self.current_attempt().state, AttemptState::Leased);
    }

    fn current_lease(&self) -> &Lease {
        self.leases.last().expect("trace has a current Lease")
    }

    fn proof_for(&self, index: usize) -> DomainLeaseProof {
        let lease = &self.leases[index];
        DomainLeaseProof {
            lease_id: lease.id,
            attempt_id: lease.attempt_id,
            fencing_token: lease.fencing_token,
            expected_lease_version: lease.version,
        }
    }

    fn current_attempt(&self) -> &Attempt {
        self.attempts.last().expect("trace has a current Attempt")
    }

    fn transition_current_attempt(&mut self, command: AttemptCommand) -> Result<(), DomainError> {
        let index = self.attempts.len() - 1;
        let transition = self.attempts[index].transition(&command)?;
        self.domain_event_count += transition.events.len();
        self.attempt_events[index].extend(transition.events);
        self.attempts[index] = transition.aggregate;
        assert_eq!(
            Attempt::replay(
                self.attempt_seeds[index].clone(),
                &self.attempt_events[index]
            )?,
            self.attempts[index]
        );
        Ok(())
    }

    fn prepare_current_attempt_for_candidate(&mut self) {
        let commands = [
            AttemptCommand::StartPreparation {
                expected_version: self.current_attempt().version,
                token_is_current: true,
                inputs_available: true,
            },
            AttemptCommand::BaselineReady {
                expected_version: AggregateVersion::new(2),
                snapshot_matches: true,
            },
            AttemptCommand::ApproveExecutionPlan {
                expected_version: AggregateVersion::new(3),
                plan_covers_contract: true,
            },
            AttemptCommand::StartLocalVerification {
                expected_version: AggregateVersion::new(4),
                has_candidate_changes: true,
            },
        ];
        for command in commands {
            self.transition_current_attempt(command).unwrap();
        }
        assert_eq!(self.current_attempt().state, AttemptState::LocalVerify);
    }

    fn metadata(
        &mut self,
        key: &str,
        payload: &[u8],
        expected_version: AggregateVersion,
    ) -> CommandMetadata {
        let command_id = self.next_command_id;
        self.next_command_id += 1;
        CommandMetadata {
            command_id: typed_id::<CommandId>(command_id),
            actor_id: self.actor_id,
            idempotency_key: IdempotencyKey::new(key).unwrap(),
            correlation_id: typed_id::<CorrelationId>(10),
            causation_id: None,
            expected_version: Some(expected_version),
            payload_digest: Sha256Digest::of_bytes(payload),
        }
    }

    fn scope(&self, metadata: &CommandMetadata, command_type: &str) -> IdempotencyScope {
        IdempotencyScope::new(
            self.project_id,
            command_type,
            metadata.actor_id,
            metadata.idempotency_key.clone(),
        )
        .unwrap()
    }

    fn renew(&mut self, new_expires_at_millis: i64) {
        let current = self.current_lease();
        let transition = Lease::transition(
            Some(current),
            &LeaseCommand::RenewLease {
                expected_version: current.version,
                holder_node_id: self.holder_node_id,
                fencing_token: current.fencing_token,
                now: server_time(EPOCH_MILLIS),
                new_expires_at: server_time(new_expires_at_millis),
            },
        )
        .unwrap();
        self.domain_event_count += transition.events.len();
        *self.leases.last_mut().unwrap() = transition.aggregate;
    }

    fn record_candidate(&mut self, now_millis: i64) {
        let lease = self.current_lease().clone();
        agentforge_domain::lease::authorize_attempt_write(
            &lease,
            &self.proof_for(self.leases.len() - 1),
            server_time(now_millis),
        )
        .unwrap();
        self.transition_current_attempt(AttemptCommand::RecordCandidate {
            expected_version: self.current_attempt().version,
            candidate_commit: GitObjectId::new("a".repeat(40)).unwrap(),
            hard_checks_passed: true,
        })
        .unwrap();
        assert_eq!(self.current_attempt().state, AttemptState::Candidate);
        self.transition_package(WorkPackageCommand::RecordCandidate {
            expected_version: self.package.version,
            attempt_id: lease.attempt_id,
            candidate_sealed: true,
        });
        let transition = Lease::transition(
            Some(&lease),
            &LeaseCommand::ReleaseLease {
                expected_version: lease.version,
                holder_node_id: self.holder_node_id,
                fencing_token: lease.fencing_token,
                now: server_time(now_millis),
            },
        )
        .unwrap();
        self.domain_event_count += transition.events.len();
        *self.leases.last_mut().unwrap() = transition.aggregate;
        self.mapped_effect_count += 1;
        self.mapped_outbox_count += 1;
    }

    fn advance_current_attempt_to_clean_reproduce(&mut self) {
        self.transition_current_attempt(AttemptCommand::StartIsolatedReview {
            expected_version: self.current_attempt().version,
            reviewer_id: typed_id::<ExecutorId>(7),
        })
        .unwrap();
        assert_eq!(self.current_attempt().state, AttemptState::IsolatedReview);
        self.transition_current_attempt(AttemptCommand::StartCleanReproduce {
            expected_version: self.current_attempt().version,
            has_unresolved_high_finding: false,
        })
        .unwrap();
        assert_eq!(self.current_attempt().state, AttemptState::CleanReproduce);
    }

    fn finalize_submission(&mut self) {
        let attempt_id = self
            .package
            .active_attempt_id
            .expect("Verifying package retains Candidate Attempt lineage");
        let submission_id = typed_id::<SubmissionId>(500);
        let candidate_commit = GitObjectId::new("a".repeat(40)).unwrap();
        let record = SubmissionRecord {
            id: submission_id,
            protocol_key: ProtocolKey::new("candidate-1").unwrap(),
            attempt_id,
            package_revision_id: self.package.selected_revision_id,
            candidate_id: Some(typed_id::<CandidateId>(501)),
            candidate_artifact_id: Some(typed_id::<CandidateArtifactId>(502)),
            verification_run_id: Some(typed_id::<VerificationRunId>(503)),
            candidate_commit: Some(candidate_commit.clone()),
            submitted_head: Some(candidate_commit.clone()),
            tested_head: Some(candidate_commit.clone()),
            reviewed_head: Some(candidate_commit),
            manifest_digest: Sha256Digest::of_bytes(b"manifest"),
            evidence_digest: Some(Sha256Digest::of_bytes(b"evidence")),
            lease_fencing_token_hash: Sha256Digest::of_bytes(b"g5"),
            state: SubmissionState::Pass,
            completed_stage: CompletedStage::CandidateReady,
            failure_dossier: None,
            acceptance_facts: Some(AcceptanceFacts {
                hard_criteria: vec![CriterionOutcome::Pass],
                unresolved_high_risk_findings: Some(false),
                clean_reproduce_passed: Some(true),
                signature_valid: true,
                lineage_matches: true,
                lease_was_current_at_registration: true,
            }),
        };
        let transition = Submission::transition(
            None,
            &SubmissionCommand::FinalizeCandidateSubmission { record },
        )
        .unwrap();
        self.domain_event_count += transition.events.len();
        assert!(transition.aggregate.candidate_ready());
        self.submission = Some(transition.aggregate);
        self.transition_current_attempt(AttemptCommand::FinalizeSubmission {
            expected_version: self.current_attempt().version,
            submission_id,
            run_outcome: SubmissionState::Pass,
            manifest_signed: true,
            heads_match: true,
        })
        .unwrap();
        assert_eq!(self.current_attempt().state, AttemptState::Submitted);
        self.transition_current_attempt(AttemptCommand::MarkAttemptPassed {
            expected_version: self.current_attempt().version,
        })
        .unwrap();
        assert_eq!(self.current_attempt().state, AttemptState::Passed);
        self.transition_package(WorkPackageCommand::FinalizeVerification {
            expected_version: self.package.version,
            attempt_id,
            submission_id,
            outcome: VerificationOutcome::Pass,
            failed_checks: Vec::new(),
            failure_dossier_complete: false,
        });
        self.mapped_effect_count += 1;
        self.mapped_outbox_count += 1;
    }
}

fn author_command(grant: &simulator::LeaseGrant, key: &str, write: AuthorWrite) -> AuthorCommand {
    AuthorCommand {
        actor_id: "worker-tokyo-03".to_owned(),
        idempotency_key: key.to_owned(),
        lease: grant.proof(),
        write,
    }
}

fn assert_effect_counts(simulator: &ProtocolSimulator, real: &RealDomainTrace) {
    assert_eq!(simulator.domain_effect_count(), real.mapped_effect_count);
    assert_eq!(simulator.outbox_event_count(), real.mapped_outbox_count);
    simulator.validate().unwrap();
}

fn simulator_error(handling: &simulator::CommandHandling) -> ProtocolError {
    handling
        .client_response
        .as_ref()
        .expect("rejection is delivered")
        .as_ref()
        .expect_err("handling must be rejected")
        .to_owned()
}

#[test]
fn candidate_first_fencing_and_ack_replay_match_real_domain() {
    let mut simulator = ProtocolSimulator::new(0xD1FF_EA11);
    let mut real = RealDomainTrace::offered();

    let mut grants = Vec::new();
    for index in 0..3 {
        let event_count_before = real.domain_event_count;
        let grant = simulator.reassign(1_000).unwrap();
        real.reassign(&grant, EPOCH_MILLIS);
        assert_eq!(
            real.domain_event_count - event_count_before,
            if index == 0 { 3 } else { 6 }
        );
        assert_eq!(grant.generation, real.current_lease().fencing_token.get());
        assert_eq!(real.package.state, WorkPackageState::Active);
        grants.push(grant);
        assert_effect_counts(&simulator, &real);
    }
    let generation_three = grants.last().unwrap().clone();

    // Renew commits one simulator effect/outbox row and one real LeaseRenewed
    // event. Receipt replay must not repeat either durable mutation.
    let renew_command = author_command(
        &generation_three,
        "diff:renew:g3",
        AuthorWrite::RenewLease {
            extend_by_millis: 250,
        },
    );
    let renew_metadata = real.metadata(
        &renew_command.idempotency_key,
        b"renew:g3:250",
        real.current_lease().version,
    );
    let renew_scope = real.scope(&renew_metadata, "lease.renew");
    assert_eq!(
        check_receipt::<String>(None, &renew_scope, &renew_metadata),
        Ok(ReceiptDecision::Execute)
    );
    let renew = simulator.handle_author(&renew_command, CommandFault::None);
    assert_eq!(renew.origin, ResultOrigin::NewCommit);
    let renew_response = renew.server_response.clone().unwrap();
    let event_count_before = real.domain_event_count;
    real.renew(generation_three.expires_at_unix_ms + 250);
    assert_eq!(real.domain_event_count - event_count_before, 1);
    real.mapped_effect_count += 1;
    real.mapped_outbox_count += 1;
    let renew_receipt = CommandReceipt {
        scope: renew_scope.clone(),
        command_id: renew_metadata.command_id,
        payload_digest: renew_metadata.payload_digest,
        response: "renewed".to_owned(),
        resource_version: real.current_lease().version,
    };
    assert_effect_counts(&simulator, &real);

    // Artifact registration maps to the real Lease write guard plus the real
    // receipt decision; the Artifact persistence adapter owns its one effect.
    let artifact_command = author_command(
        &generation_three,
        "diff:artifact:g3",
        AuthorWrite::RegisterArtifact {
            artifact_id: "artifact-diff-g3".to_owned(),
        },
    );
    let artifact_metadata = real.metadata(
        &artifact_command.idempotency_key,
        b"artifact:g3",
        real.current_lease().version,
    );
    let artifact_scope = real.scope(&artifact_metadata, "artifact.register");
    assert_eq!(
        check_receipt::<String>(None, &artifact_scope, &artifact_metadata),
        Ok(ReceiptDecision::Execute)
    );
    agentforge_domain::lease::authorize_attempt_write(
        real.current_lease(),
        &real.proof_for(2),
        server_time(EPOCH_MILLIS),
    )
    .unwrap();
    let artifact = simulator.handle_author(&artifact_command, CommandFault::None);
    assert_eq!(artifact.origin, ResultOrigin::NewCommit);
    let artifact_response = artifact.server_response.clone().unwrap();
    let event_count_before = real.domain_event_count;
    real.mapped_effect_count += 1;
    real.mapped_outbox_count += 1;
    let artifact_receipt = CommandReceipt {
        scope: artifact_scope.clone(),
        command_id: artifact_metadata.command_id,
        payload_digest: artifact_metadata.payload_digest,
        response: "artifact-diff-g3".to_owned(),
        resource_version: real.current_lease().version,
    };
    assert_eq!(real.domain_event_count, event_count_before);
    assert_eq!(simulator.artifact_count(), 1);
    assert_effect_counts(&simulator, &real);

    let generation_four = simulator.reassign(1_000).unwrap();
    let event_count_before = real.domain_event_count;
    real.reassign(&generation_four, EPOCH_MILLIS);
    assert_eq!(real.domain_event_count - event_count_before, 6);
    assert_eq!(generation_four.generation, 4);

    // Exact receipts replay before the old generation is fenced.
    let event_count_before = real.domain_event_count;
    let renew_replay = simulator.handle_author(&renew_command, CommandFault::None);
    assert_eq!(renew_replay.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(renew_replay.server_response, Some(renew_response));
    assert_eq!(
        check_receipt(Some(&renew_receipt), &renew_scope, &renew_metadata),
        Ok(ReceiptDecision::Replay(&renew_receipt))
    );
    let artifact_replay = simulator.handle_author(&artifact_command, CommandFault::None);
    assert_eq!(artifact_replay.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(artifact_replay.server_response, Some(artifact_response));
    assert_eq!(
        check_receipt(Some(&artifact_receipt), &artifact_scope, &artifact_metadata),
        Ok(ReceiptDecision::Replay(&artifact_receipt))
    );
    assert_eq!(real.domain_event_count, event_count_before);
    assert_effect_counts(&simulator, &real);

    // New keys on generation three are fenced identically for Renew and
    // RegisterArtifact.
    let stale_proof = real.proof_for(2);
    for write in [
        AuthorWrite::RenewLease {
            extend_by_millis: 250,
        },
        AuthorWrite::RegisterArtifact {
            artifact_id: "artifact-stale".to_owned(),
        },
    ] {
        let stale = simulator.handle_author(
            &author_command(&generation_three, &format!("diff:stale:{write:?}"), write),
            CommandFault::None,
        );
        let domain_error = agentforge_domain::lease::authorize_attempt_write(
            real.current_lease(),
            &stale_proof,
            server_time(EPOCH_MILLIS),
        )
        .unwrap_err();
        assert_eq!(simulator_error(&stale).code(), domain_error.code());
        assert_eq!(domain_error, DomainError::StaleLease);
    }

    // Expiry is also checked by both implementations for each formal handler.
    let current_proof = real.proof_for(3);
    for (index, write) in [
        AuthorWrite::RenewLease {
            extend_by_millis: 250,
        },
        AuthorWrite::RegisterArtifact {
            artifact_id: "artifact-expired".to_owned(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let expired = simulator.handle_author(
            &author_command(&generation_four, &format!("diff:expired:{index}"), write),
            if index == 0 {
                CommandFault::ExpireBeforeAuthorization
            } else {
                CommandFault::None
            },
        );
        let domain_error = agentforge_domain::lease::authorize_attempt_write(
            real.current_lease(),
            &current_proof,
            server_time(generation_four.expires_at_unix_ms),
        )
        .unwrap_err();
        assert_eq!(simulator_error(&expired).code(), domain_error.code());
        assert_eq!(domain_error, DomainError::LeaseExpired);
    }
    assert_effect_counts(&simulator, &real);

    // A fresh generation records Candidate first. The server commits despite
    // ACK loss, and an exact retry replays after the real Lease was released.
    let generation_five = simulator.reassign(1_000).unwrap();
    let event_count_before = real.domain_event_count;
    real.reassign(&generation_five, generation_four.expires_at_unix_ms);
    assert_eq!(real.domain_event_count - event_count_before, 6);
    let event_count_before = real.domain_event_count;
    real.prepare_current_attempt_for_candidate();
    assert_eq!(real.domain_event_count - event_count_before, 4);
    let candidate_command = author_command(
        &generation_five,
        "diff:candidate",
        AuthorWrite::RecordCandidate {
            candidate_id: "candidate-1".to_owned(),
        },
    );
    let candidate_metadata = real.metadata(
        &candidate_command.idempotency_key,
        b"candidate-1",
        real.package.version,
    );
    let candidate_scope = real.scope(&candidate_metadata, "candidate.record");
    let candidate =
        simulator.handle_author(&candidate_command, CommandFault::DropResponseAfterCommit);
    assert!(candidate.client_response.is_none());
    assert_eq!(candidate.origin, ResultOrigin::NewCommit);
    let event_count_before = real.domain_event_count;
    real.record_candidate(generation_four.expires_at_unix_ms);
    assert_eq!(real.domain_event_count - event_count_before, 3);
    let candidate_receipt = CommandReceipt {
        scope: candidate_scope.clone(),
        command_id: candidate_metadata.command_id,
        payload_digest: candidate_metadata.payload_digest,
        response: "candidate-1".to_owned(),
        resource_version: real.package.version,
    };
    assert_eq!(real.package.state, WorkPackageState::Verifying);
    assert_eq!(real.current_lease().state, LeaseState::Released);
    assert_eq!(real.current_attempt().state, AttemptState::Candidate);
    for command in [
        AttemptCommand::MarkLost {
            expected_version: real.current_attempt().version,
        },
        AttemptCommand::MarkFailed {
            expected_version: real.current_attempt().version,
            reason_code: "late-author-terminalization".to_owned(),
        },
    ] {
        let error = real.current_attempt().transition(&command).unwrap_err();
        assert_eq!(
            error,
            DomainError::InvalidTransition {
                from: "candidate".to_owned(),
                command: command.kind().as_str().to_owned(),
            }
        );
        assert_eq!(error.code(), ProtocolError::InvalidTransition.code());
    }
    assert_eq!(simulator.candidate_count(), 1);
    assert_eq!(simulator.formal_submission_count(), 0);
    assert_effect_counts(&simulator, &real);

    let candidate_replay = simulator.handle_author(&candidate_command, CommandFault::None);
    let event_count_before = real.domain_event_count;
    assert_eq!(candidate_replay.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(
        check_receipt(
            Some(&candidate_receipt),
            &candidate_scope,
            &candidate_metadata
        ),
        Ok(ReceiptDecision::Replay(&candidate_receipt))
    );
    assert_eq!(real.domain_event_count, event_count_before);
    assert_effect_counts(&simulator, &real);
    let event_count_before = real.domain_event_count;
    real.advance_current_attempt_to_clean_reproduce();
    assert_eq!(real.domain_event_count - event_count_before, 2);

    // Only the independent Coordinator may create the terminal Submission.
    let finalize_command = CoordinatorCommand {
        service_id: coordinator_service_id().to_owned(),
        idempotency_key: "diff:finalize".to_owned(),
        verification_job_id: verification_job_id_for("candidate-1"),
        expected_job_version: 1,
        candidate_id: "candidate-1".to_owned(),
        submission_id: "submission-1".to_owned(),
    };
    let finalize_metadata = real.metadata(
        &finalize_command.idempotency_key,
        b"submission-1",
        real.package.version,
    );
    let finalize_scope = real.scope(&finalize_metadata, "submission.finalize");
    let finalized =
        simulator.finalize_submission(&finalize_command, CommandFault::DropResponseAfterCommit);
    assert!(finalized.client_response.is_none());
    assert_eq!(finalized.origin, ResultOrigin::NewCommit);
    let event_count_before = real.domain_event_count;
    real.finalize_submission();
    assert_eq!(real.domain_event_count - event_count_before, 4);
    let finalize_receipt = CommandReceipt {
        scope: finalize_scope.clone(),
        command_id: finalize_metadata.command_id,
        payload_digest: finalize_metadata.payload_digest,
        response: "submission-1".to_owned(),
        resource_version: real.package.version,
    };
    assert_eq!(real.package.state, WorkPackageState::Accepted);
    assert_eq!(
        real.submission.as_ref().unwrap().state,
        SubmissionState::Pass
    );
    assert_eq!(simulator.formal_submission_count(), 1);
    assert_effect_counts(&simulator, &real);

    let finalize_replay = simulator.finalize_submission(&finalize_command, CommandFault::None);
    let event_count_before = real.domain_event_count;
    assert_eq!(finalize_replay.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(
        check_receipt(Some(&finalize_receipt), &finalize_scope, &finalize_metadata),
        Ok(ReceiptDecision::Replay(&finalize_receipt))
    );
    assert_eq!(real.domain_event_count, event_count_before);
    assert_effect_counts(&simulator, &real);
    assert!(real.domain_event_count > real.mapped_effect_count);
}

#[test]
fn seeded_generation_fencing_subset_matches_real_domain_error_codes() {
    for seed in 0_u64..128 {
        let mut simulator = ProtocolSimulator::new(seed);
        let first = simulator.reassign(500).unwrap();
        let current = simulator.reassign(500).unwrap();
        let current_domain = Lease::transition(
            None,
            &LeaseCommand::GrantLease(GrantLease {
                id: LeaseId::from(current.lease_id),
                package_id: typed_id::<PackageId>(u128::from(seed) + 1_000),
                revision_id: typed_id::<PackageRevisionId>(u128::from(seed) + 2_000),
                attempt_id: AttemptId::from(current.attempt_id),
                holder_node_id: typed_id::<NodeId>(u128::from(seed) + 3_000),
                previous_fencing_token: Some(FencingToken::new(1).unwrap()),
                fencing_token: FencingToken::new(2).unwrap(),
                granted_at: server_time(EPOCH_MILLIS),
                expires_at: server_time(current.expires_at_unix_ms),
                max_expires_at: server_time(current.expires_at_unix_ms + 500),
            }),
        )
        .unwrap()
        .aggregate;
        let old_proof = DomainLeaseProof {
            lease_id: LeaseId::from(first.lease_id),
            attempt_id: AttemptId::from(first.attempt_id),
            fencing_token: FencingToken::new(1).unwrap(),
            expected_lease_version: AggregateVersion::new(1),
        };
        let write = if seed % 2 == 0 {
            AuthorWrite::RenewLease {
                extend_by_millis: 10,
            }
        } else {
            AuthorWrite::RegisterArtifact {
                artifact_id: format!("artifact-{seed}"),
            }
        };
        let handling = simulator.handle_author(
            &author_command(&first, &format!("seeded-stale-{seed}"), write),
            CommandFault::None,
        );
        let domain_error = agentforge_domain::lease::authorize_attempt_write(
            &current_domain,
            &old_proof,
            server_time(EPOCH_MILLIS),
        )
        .unwrap_err();
        assert_eq!(simulator_error(&handling).code(), domain_error.code());
    }
}
