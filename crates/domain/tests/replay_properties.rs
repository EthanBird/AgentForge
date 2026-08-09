use agentforge_domain::{
    ids::*,
    state::{
        attempt::{Attempt, AttemptCommand, AttemptEvent, NewAttempt},
        lease::{
            GrantLease, Lease, LeaseCommand, LeaseEvent, LeaseProof, LeaseState,
            authorize_attempt_write,
        },
        run_claim::{
            GrantRunClaim, RunClaim, RunClaimCommand, RunClaimEvent, VerifiedRunClaimHistory,
        },
        submission::{
            AcceptanceFacts, CompletedStage, CriterionOutcome, Submission, SubmissionCommand,
            SubmissionEvent, SubmissionRecord, SubmissionState,
        },
        work_package::{
            ClaimReadiness, NewWorkPackage, PublishReadiness, VerificationOutcome, WorkPackage,
            WorkPackageCommand, WorkPackageState,
        },
    },
};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed};
use time::{Duration, macros::datetime};
use uuid::Uuid;

fn id<T: From<Uuid>>(value: u128) -> T {
    T::from(Uuid::from_u128(value))
}

fn oid(value: u8) -> GitObjectId {
    GitObjectId::new(format!("{value:02x}").repeat(20)).expect("oid")
}

fn at(seconds: i64) -> ServerInstant {
    ServerInstant(datetime!(2026-08-08 00:00 UTC) + Duration::seconds(seconds))
}

fn package_seed(max_attempts: u16) -> NewWorkPackage {
    NewWorkPackage {
        id: id(1),
        project_id: id(2),
        selected_revision_id: id(3),
        selected_revision: PackageRevision::new(1).expect("revision"),
        graph_version: 1,
        priority: 0,
        max_attempts,
    }
}

#[test]
fn replay_matches_online_apply_for_all_five_aggregates() {
    let seed = package_seed(3);
    let mut package = WorkPackage::new(seed.clone()).expect("package");
    let mut package_events = Vec::new();
    for make_command in [0_u8, 1, 2, 3, 4, 5] {
        let command = match make_command {
            0 => WorkPackageCommand::RequestValidation {
                expected_version: package.version,
                revision_exists: true,
                package_hash_matches: true,
            },
            1 => WorkPackageCommand::PublishValidatedPackage {
                expected_version: package.version,
                readiness: PublishReadiness {
                    dor_passed: true,
                    dag_valid: true,
                    budget_available: true,
                    permissions_valid: true,
                },
            },
            2 => WorkPackageCommand::GrantLease {
                expected_version: package.version,
                attempt_id: id(4),
                lease_id: id(5),
                readiness: ClaimReadiness {
                    dependencies_satisfied: true,
                    budget_available: true,
                    no_active_lease: true,
                },
            },
            3 => WorkPackageCommand::RecordCandidate {
                expected_version: package.version,
                attempt_id: id(4),
                candidate_sealed: true,
            },
            4 => WorkPackageCommand::FinalizeVerification {
                expected_version: package.version,
                attempt_id: id(4),
                submission_id: id(6),
                outcome: VerificationOutcome::Pass,
                failed_checks: vec![],
                failure_dossier_complete: true,
            },
            5 => WorkPackageCommand::EnqueueIntegration {
                expected_version: package.version,
                integration_id: id(7),
                lineage_matches: true,
                candidate_artifact_complete: true,
            },
            _ => unreachable!(),
        };
        let transition = package.transition(&command).expect("package transition");
        package_events.extend(transition.events);
        package = transition.aggregate;
    }
    assert_eq!(
        WorkPackage::replay(seed, &package_events).expect("package replay"),
        package
    );

    let attempt_seed = NewAttempt {
        id: id(10),
        package_id: id(1),
        revision_id: id(3),
        executor_id: id(11),
        node_id: id(12),
        fencing_token: FencingToken::new(1).expect("token"),
        base_commit: oid(13),
    };
    let mut attempt = Attempt::new(attempt_seed.clone());
    let mut attempt_events: Vec<AttemptEvent> = Vec::new();
    let commands = [
        AttemptCommand::AttachLease {
            expected_version: AggregateVersion::new(0),
            lease_id: id(5),
            fencing_token: FencingToken::new(1).expect("token"),
            lineage_matches: true,
        },
        AttemptCommand::StartPreparation {
            expected_version: AggregateVersion::new(1),
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
        AttemptCommand::RecordCandidate {
            expected_version: AggregateVersion::new(5),
            candidate_commit: oid(14),
            hard_checks_passed: true,
        },
    ];
    for command in commands {
        let transition = attempt.transition(&command).expect("attempt transition");
        attempt_events.extend(transition.events);
        attempt = transition.aggregate;
    }
    assert_eq!(
        Attempt::replay(attempt_seed, &attempt_events).expect("attempt replay"),
        attempt
    );

    let grant = GrantLease {
        id: id(20),
        package_id: id(1),
        revision_id: id(3),
        attempt_id: id(10),
        holder_node_id: id(12),
        previous_fencing_token: None,
        fencing_token: FencingToken::new(1).expect("token"),
        granted_at: at(0),
        expires_at: at(10),
        max_expires_at: at(30),
    };
    let first = Lease::transition(None, &LeaseCommand::GrantLease(grant)).expect("grant");
    let mut lease_events: Vec<LeaseEvent> = first.events;
    let renewed = Lease::transition(
        Some(&first.aggregate),
        &LeaseCommand::RenewLease {
            expected_version: first.aggregate.version,
            holder_node_id: first.aggregate.holder_node_id,
            fencing_token: first.aggregate.fencing_token,
            now: at(5),
            new_expires_at: at(20),
        },
    )
    .expect("renew");
    lease_events.extend(renewed.events);
    let released = Lease::transition(
        Some(&renewed.aggregate),
        &LeaseCommand::ReleaseLease {
            expected_version: renewed.aggregate.version,
            holder_node_id: renewed.aggregate.holder_node_id,
            fencing_token: renewed.aggregate.fencing_token,
            now: at(15),
        },
    )
    .expect("release");
    lease_events.extend(released.events);
    assert_eq!(
        Lease::replay(&lease_events).expect("lease replay"),
        released.aggregate
    );

    let granted_claim = RunClaim::grant_initial(GrantRunClaim {
        id: id(21),
        run_id: id(22),
        previous_generation: None,
        predecessor_claim_id: None,
        takeover_authorization: None,
        claim_generation: RunClaimToken::new(1).expect("generation"),
        holder_node_id: id(12),
        granted_at: at(0),
        expires_at: at(10),
    })
    .expect("grant run claim");
    let mut claim_events: Vec<RunClaimEvent> = granted_claim.events;
    let renewed_claim = granted_claim
        .aggregate
        .execute(&RunClaimCommand::Renew {
            expected_version: granted_claim.aggregate.version(),
            proof: granted_claim.aggregate.proof(),
            now: at(5),
            new_expires_at: at(20),
        })
        .expect("renew run claim");
    claim_events.extend(renewed_claim.events);
    let expired_claim = renewed_claim
        .aggregate
        .execute(&RunClaimCommand::Expire {
            expected_version: renewed_claim.aggregate.version(),
            expired_at: at(20),
        })
        .expect("expire run claim");
    claim_events.extend(expired_claim.events);
    let history = VerifiedRunClaimHistory::replay(&[claim_events]).expect("run claim replay");
    assert_eq!(
        history.get(expired_claim.aggregate.id()),
        Some(&expired_claim.aggregate)
    );

    let head = oid(30);
    let record = SubmissionRecord {
        id: id(31),
        protocol_key: ProtocolKey::new("sub-replay-1").expect("key"),
        attempt_id: id(10),
        package_revision_id: id(3),
        candidate_id: Some(id(32)),
        candidate_artifact_id: Some(id(33)),
        verification_run_id: Some(id(34)),
        candidate_commit: Some(head.clone()),
        submitted_head: Some(head.clone()),
        tested_head: Some(head.clone()),
        reviewed_head: Some(head),
        manifest_digest: Sha256Digest::of_bytes(b"manifest"),
        evidence_digest: Some(Sha256Digest::of_bytes(b"evidence")),
        lease_fencing_token_hash: Sha256Digest::of_bytes(b"fence"),
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
    let created = Submission::transition(
        None,
        &SubmissionCommand::FinalizeCandidateSubmission { record },
    )
    .expect("submission");
    let events: Vec<SubmissionEvent> = created.events;
    assert_eq!(
        Submission::replay(&events).expect("submission replay"),
        created.aggregate
    );
}

fn bootstrap_offered() -> WorkPackage {
    let mut package = WorkPackage::new(package_seed(u16::MAX)).expect("package");
    package = package
        .transition(&WorkPackageCommand::RequestValidation {
            expected_version: package.version,
            revision_exists: true,
            package_hash_matches: true,
        })
        .expect("validation")
        .aggregate;
    package
        .transition(&WorkPackageCommand::PublishValidatedPackage {
            expected_version: package.version,
            readiness: PublishReadiness {
                dor_passed: true,
                dag_valid: true,
                budget_available: true,
                permissions_valid: true,
            },
        })
        .expect("publish")
        .aggregate
}

proptest! {
    // Fixed seed makes this schedule corpus reproducible in addition to
    // proptest's persisted `*.proptest-regressions` failure database.
    #![proptest_config(ProptestConfig {
        cases: 256,
        rng_seed: RngSeed::Fixed(0xA63E_17F0_2026_0808),
        ..ProptestConfig::default()
    })]

    #[test]
    fn arbitrary_command_sequences_never_have_two_active_generations(
        operations in prop::collection::vec((any::<u8>(), any::<u8>()), 0..256),
    ) {
        let mut package = bootstrap_offered();
        let mut leases: Vec<Lease> = Vec::new();
        let mut terminal_states: Vec<Option<LeaseState>> = Vec::new();
        let mut granted_generations: Vec<u64> = Vec::new();

        for (index, (operation, selector)) in operations.into_iter().enumerate() {
            let active_index = leases.iter().position(|lease| lease.state == LeaseState::Active);

            match operation % 9 {
                // Reassign/claim. WorkPackage and Lease decisions are computed
                // first, then committed together to model the atomic use case.
                0 if matches!(package.state, WorkPackageState::Offered | WorkPackageState::ReworkReady) => {
                    let attempt_id = id(1_000 + index as u128 * 2);
                    let lease_id = id(1_001 + index as u128 * 2);
                    let package_transition = package
                        .transition(&WorkPackageCommand::GrantLease {
                            expected_version: package.version,
                            attempt_id,
                            lease_id,
                            readiness: ClaimReadiness {
                                dependencies_satisfied: true,
                                budget_available: true,
                                no_active_lease: active_index.is_none(),
                            },
                        })
                        .expect("claimable package grant must decide");
                    let generation = package_transition
                        .aggregate
                        .active_fencing_token
                        .expect("active generation");
                    let previous = granted_generations
                        .last()
                        .copied()
                        .map(|value| FencingToken::new(value).expect("prior token"));
                    let lease_transition = Lease::transition(
                        None,
                        &LeaseCommand::GrantLease(GrantLease {
                            id: lease_id,
                            package_id: package.id,
                            revision_id: package.selected_revision_id,
                            attempt_id,
                            holder_node_id: id(20_000 + index as u128),
                            previous_fencing_token: previous,
                            fencing_token: generation,
                            granted_at: at(index as i64 * 100),
                            expires_at: at(index as i64 * 100 + 10),
                            max_expires_at: at(index as i64 * 100 + 30),
                        }),
                    )
                    .expect("valid monotonic grant");
                    package = package_transition.aggregate;
                    leases.push(lease_transition.aggregate);
                    terminal_states.push(None);
                    granted_generations.push(generation.get());
                }
                // Renew the current generation without creating another row.
                1 if active_index.is_some() => {
                    let row = active_index.expect("checked");
                    let lease = &leases[row];
                    if lease.expires_at < lease.max_expires_at {
                        let proposed = ServerInstant(lease.expires_at.0 + Duration::seconds(1));
                        let new_expires_at = proposed.min(lease.max_expires_at);
                        let renewed = Lease::transition(
                            Some(lease),
                            &LeaseCommand::RenewLease {
                                expected_version: lease.version,
                                holder_node_id: lease.holder_node_id,
                                fencing_token: lease.fencing_token,
                                now: ServerInstant(lease.granted_at.0 + Duration::seconds(5)),
                                new_expires_at,
                            },
                        )
                        .expect("active renew");
                        leases[row] = renewed.aggregate;
                    }
                }
                // Release, revoke, and expire each terminalize the current row
                // and atomically make the package eligible for a later generation.
                2..=4 if active_index.is_some() => {
                    let row = active_index.expect("checked");
                    let lease = &leases[row];
                    let lease_command = match operation % 9 {
                        2 => LeaseCommand::ReleaseLease {
                            expected_version: lease.version,
                            holder_node_id: lease.holder_node_id,
                            fencing_token: lease.fencing_token,
                            now: ServerInstant(lease.granted_at.0 + Duration::seconds(5)),
                        },
                        3 => LeaseCommand::RevokeLease {
                            expected_version: lease.version,
                            policy_authorized: true,
                            reason_code: "property_revoke".into(),
                        },
                        4 => LeaseCommand::ExpireLease {
                            expected_version: lease.version,
                            now: lease.expires_at,
                        },
                        _ => unreachable!(),
                    };
                    let lease_transition =
                        Lease::transition(Some(lease), &lease_command).expect("terminalize lease");
                    let package_transition = package
                        .transition(&WorkPackageCommand::LoseAttempt {
                            expected_version: package.version,
                            attempt_id: lease.attempt_id,
                            recoverable: true,
                        })
                        .expect("lose current attempt");
                    terminal_states[row] = Some(lease_transition.aggregate.state);
                    leases[row] = lease_transition.aggregate;
                    package = package_transition.aggregate;
                }
                // Candidate sealing closes the author Lease but keeps the Attempt
                // lineage through verification.
                5 if active_index.is_some() => {
                    let row = active_index.expect("checked");
                    let lease = &leases[row];
                    let lease_transition = Lease::transition(
                        Some(lease),
                        &LeaseCommand::ReleaseLease {
                            expected_version: lease.version,
                            holder_node_id: lease.holder_node_id,
                            fencing_token: lease.fencing_token,
                            now: ServerInstant(lease.granted_at.0 + Duration::seconds(5)),
                        },
                    )
                    .expect("candidate closes lease");
                    let package_transition = package
                        .transition(&WorkPackageCommand::RecordCandidate {
                            expected_version: package.version,
                            attempt_id: lease.attempt_id,
                            candidate_sealed: true,
                        })
                        .expect("record candidate");
                    terminal_states[row] = Some(LeaseState::Released);
                    leases[row] = lease_transition.aggregate;
                    package = package_transition.aggregate;
                }
                6 if package.state == WorkPackageState::Verifying => {
                    package = package
                        .transition(&WorkPackageCommand::FinalizeVerification {
                            expected_version: package.version,
                            attempt_id: package.active_attempt_id.expect("verification attempt"),
                            submission_id: id(30_000 + index as u128),
                            outcome: VerificationOutcome::Fail,
                            failed_checks: vec![],
                            failure_dossier_complete: true,
                        })
                        .expect("failed verification returns to rework")
                        .aggregate;
                }
                // Replay a new-key write against a terminal historical row. It
                // must fail and leave the stored row byte-for-byte unchanged.
                7 if terminal_states.iter().any(Option::is_some) => {
                    let candidates: Vec<_> = terminal_states
                        .iter()
                        .enumerate()
                        .filter_map(|(row, state)| state.map(|_| row))
                        .collect();
                    let row = candidates[usize::from(selector) % candidates.len()];
                    let before = leases[row].clone();
                    let stale = LeaseCommand::RenewLease {
                        expected_version: before.version,
                        holder_node_id: before.holder_node_id,
                        fencing_token: before.fencing_token,
                        now: before.granted_at,
                        new_expires_at: before.max_expires_at,
                    };
                    let error = Lease::transition(Some(&before), &stale)
                        .expect_err("terminal generation must not revive");
                    prop_assert!(
                        matches!(
                            error,
                            agentforge_domain::DomainError::InvalidTransition { .. }
                        ),
                        "terminal generation accepted a write"
                    );
                    prop_assert_eq!(&leases[row], &before);
                    let proof = LeaseProof {
                        lease_id: before.id,
                        attempt_id: before.attempt_id,
                        fencing_token: before.fencing_token,
                        expected_lease_version: before.version,
                    };
                    prop_assert_eq!(
                        authorize_attempt_write(&before, &proof, before.granted_at),
                        Err(agentforge_domain::DomainError::StaleLease)
                    );
                }
                // A stale fencing proof against the live row never changes it.
                8 if active_index.is_some() => {
                    let row = active_index.expect("checked");
                    let before = leases[row].clone();
                    let wrong_token = before.fencing_token.checked_next().expect("next token");
                    let stale = LeaseCommand::RenewLease {
                        expected_version: before.version,
                        holder_node_id: before.holder_node_id,
                        fencing_token: wrong_token,
                        now: before.granted_at,
                        new_expires_at: before.max_expires_at,
                    };
                    prop_assert_eq!(
                        Lease::transition(Some(&before), &stale),
                        Err(agentforge_domain::DomainError::StaleLease)
                    );
                    prop_assert_eq!(&leases[row], &before);
                }
                _ => {}
            }

            // Assert every prefix, not merely the final schedule.
            let active_rows: Vec<_> = leases
                .iter()
                .filter(|lease| lease.state == LeaseState::Active)
                .collect();
            prop_assert!(active_rows.len() <= 1);
            prop_assert!(granted_generations.windows(2).all(|pair| pair[0] < pair[1]));
            if package.state == WorkPackageState::Active {
                prop_assert_eq!(active_rows.len(), 1);
                prop_assert_eq!(
                    Some(active_rows[0].fencing_token),
                    package.active_fencing_token
                );
            } else {
                prop_assert!(active_rows.is_empty());
            }
            for (row, terminal) in terminal_states.iter().enumerate() {
                if let Some(expected_terminal) = terminal {
                    prop_assert_eq!(leases[row].state, *expected_terminal);
                    prop_assert!(leases[row].state.is_terminal());
                }
            }
        }
    }
}
