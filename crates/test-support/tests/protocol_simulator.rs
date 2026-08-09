use std::path::PathBuf;

#[path = "../../../tests/conformance/simulator/mod.rs"]
pub mod simulator;

use simulator::{
    AuthorCommand, AuthorWrite, CommandFault, CoordinatorCommand, DeliveryFault, InvariantOracle,
    ProtocolError, ProtocolSimulator, ResultOrigin, ScheduleCoverage, ScheduleFailure,
    coordinator_service_id, replay_persisted_failure, run_seed, run_seed_with_oracle,
    verification_job_id_for,
};
use tempfile::tempdir;

fn checkpoint(grant: &simulator::LeaseGrant, key: &str, id: &str) -> AuthorCommand {
    AuthorCommand {
        actor_id: "worker-tokyo-03".to_owned(),
        idempotency_key: key.to_owned(),
        lease: grant.proof(),
        write: AuthorWrite::Checkpoint {
            checkpoint_id: id.to_owned(),
        },
    }
}

fn candidate(grant: &simulator::LeaseGrant, key: &str, id: &str) -> AuthorCommand {
    AuthorCommand {
        actor_id: "worker-tokyo-03".to_owned(),
        idempotency_key: key.to_owned(),
        lease: grant.proof(),
        write: AuthorWrite::RecordCandidate {
            candidate_id: id.to_owned(),
        },
    }
}

fn renew(grant: &simulator::LeaseGrant, key: &str, extend_by_millis: i64) -> AuthorCommand {
    AuthorCommand {
        actor_id: "worker-tokyo-03".to_owned(),
        idempotency_key: key.to_owned(),
        lease: grant.proof(),
        write: AuthorWrite::RenewLease { extend_by_millis },
    }
}

fn artifact(grant: &simulator::LeaseGrant, key: &str, id: &str) -> AuthorCommand {
    AuthorCommand {
        actor_id: "worker-tokyo-03".to_owned(),
        idempotency_key: key.to_owned(),
        lease: grant.proof(),
        write: AuthorWrite::RegisterArtifact {
            artifact_id: id.to_owned(),
        },
    }
}

fn finalize(key: &str, candidate_id: &str, submission_id: &str) -> CoordinatorCommand {
    CoordinatorCommand {
        service_id: coordinator_service_id().to_owned(),
        idempotency_key: key.to_owned(),
        verification_job_id: verification_job_id_for(candidate_id),
        expected_job_version: 1,
        candidate_id: candidate_id.to_owned(),
        submission_id: submission_id.to_owned(),
    }
}

#[test]
fn ten_thousand_seeded_schedules_never_double_candidate_or_submission() {
    let failure_directory = std::env::var_os("AGENTFORGE_FAILURE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("agentforge-simulator-failures"));
    let mut coverage = ScheduleCoverage::default();

    for seed in 0..10_000 {
        match run_seed(seed, 64) {
            Ok(report) => coverage.merge(&report.coverage),
            Err(failure) => {
                let path = failure
                    .persist(&failure_directory, "ten_thousand_seeded_schedules")
                    .expect("failed seed must be persisted");
                panic!(
                    "seed {seed} violated {} at step {}; replay {}",
                    failure.invariant,
                    failure.failed_at_step,
                    path.display()
                );
            }
        }
    }

    assert!(coverage.candidate_receipt_replay);
    assert!(coverage.finalize_receipt_replay);
    assert!(coverage.finalize_before_candidate_rejected);
    assert!(coverage.stale_generation_rejected);
    assert!(coverage.expired_author_rejected);
    assert!(coverage.renew_stale_generation_rejected);
    assert!(coverage.renew_expired_rejected);
    assert!(coverage.renew_fresh_committed);
    assert!(coverage.renew_receipt_replay);
    assert!(coverage.register_artifact_stale_generation_rejected);
    assert!(coverage.register_artifact_expired_rejected);
    assert!(coverage.register_artifact_fresh_committed);
    assert!(coverage.register_artifact_receipt_replay);
    assert!(coverage.command_response_dropped);
    assert!(coverage.duplicate_delivery);
    assert!(coverage.reordered_delivery);
    assert!(coverage.publisher_ack_lost);
}

#[test]
fn renew_handler_covers_fresh_replay_stale_g3_and_expired_current() {
    let mut simulator = ProtocolSimulator::new(303);
    simulator.reassign(1_000).unwrap();
    simulator.reassign(1_000).unwrap();
    let generation_three = simulator.reassign(1_000).unwrap();
    let fresh = renew(&generation_three, "renew:g3:fresh", 250);

    let committed = simulator.handle_author(&fresh, CommandFault::None);
    assert_eq!(committed.origin, ResultOrigin::NewCommit);
    assert!(committed.newly_committed);
    let committed_response = committed.client_response.unwrap().unwrap();
    assert_eq!(simulator.domain_effect_count(), 1);
    assert_eq!(simulator.outbox_event_count(), 1);

    let generation_four = simulator.reassign(1_000).unwrap();
    let replay = simulator.handle_author(&fresh, CommandFault::None);
    assert_eq!(replay.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(replay.client_response, Some(Ok(committed_response)));

    let stale = simulator.handle_author(
        &renew(&generation_three, "renew:g3:stale", 250),
        CommandFault::None,
    );
    assert_eq!(stale.client_response, Some(Err(ProtocolError::LeaseStale)));

    let expired = simulator.handle_author(
        &renew(&generation_four, "renew:g4:expired", 250),
        CommandFault::ExpireBeforeAuthorization,
    );
    assert_eq!(
        expired.client_response,
        Some(Err(ProtocolError::LeaseExpired))
    );
    assert_eq!(simulator.domain_effect_count(), 1);
    assert_eq!(simulator.outbox_event_count(), 1);
    simulator.validate().unwrap();
}

#[test]
fn artifact_handler_covers_fresh_replay_stale_g3_and_expired_current() {
    let mut simulator = ProtocolSimulator::new(404);
    simulator.reassign(1_000).unwrap();
    simulator.reassign(1_000).unwrap();
    let generation_three = simulator.reassign(1_000).unwrap();
    let fresh = artifact(&generation_three, "artifact:g3:fresh", "artifact-g3");

    let committed = simulator.handle_author(&fresh, CommandFault::None);
    assert_eq!(committed.origin, ResultOrigin::NewCommit);
    assert!(committed.newly_committed);
    let committed_response = committed.client_response.unwrap().unwrap();
    assert_eq!(simulator.artifact_count(), 1);
    assert_eq!(simulator.domain_effect_count(), 1);
    assert_eq!(simulator.outbox_event_count(), 1);

    let generation_four = simulator.reassign(1_000).unwrap();
    let replay = simulator.handle_author(&fresh, CommandFault::None);
    assert_eq!(replay.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(replay.client_response, Some(Ok(committed_response)));

    let stale = simulator.handle_author(
        &artifact(&generation_three, "artifact:g3:stale", "artifact-stale"),
        CommandFault::None,
    );
    assert_eq!(stale.client_response, Some(Err(ProtocolError::LeaseStale)));

    let expired = simulator.handle_author(
        &artifact(&generation_four, "artifact:g4:expired", "artifact-expired"),
        CommandFault::ExpireBeforeAuthorization,
    );
    assert_eq!(
        expired.client_response,
        Some(Err(ProtocolError::LeaseExpired))
    );
    assert_eq!(simulator.artifact_count(), 1);
    assert_eq!(simulator.domain_effect_count(), 1);
    assert_eq!(simulator.outbox_event_count(), 1);
    simulator.validate().unwrap();
}

#[test]
fn candidate_and_finalize_ack_loss_each_commit_one_effect_and_outbox() {
    let mut simulator = ProtocolSimulator::new(81);
    let grant = simulator.reassign(1_000).unwrap();
    let candidate_command = candidate(&grant, "candidate:key-1", "candidate-1");

    let first_candidate =
        simulator.handle_author(&candidate_command, CommandFault::DropResponseAfterCommit);
    assert!(first_candidate.client_response.is_none());
    assert_eq!(first_candidate.origin, ResultOrigin::NewCommit);
    assert!(first_candidate.newly_committed);
    let candidate_response = simulator
        .author_receipt_response(
            &candidate_command.actor_id,
            &candidate_command.idempotency_key,
        )
        .unwrap()
        .clone();

    let candidate_retry = simulator.handle_author(&candidate_command, CommandFault::None);
    assert_eq!(
        candidate_retry.client_response,
        Some(Ok(candidate_response))
    );
    assert_eq!(candidate_retry.origin, ResultOrigin::ReceiptReplay);
    assert!(!candidate_retry.newly_committed);
    assert_eq!(simulator.candidate_count(), 1);
    assert_eq!(simulator.formal_submission_count(), 0);
    assert_eq!(simulator.domain_effect_count(), 1);
    assert_eq!(simulator.outbox_event_count(), 1);

    // Candidate recording closed the author Lease. Expiring that row must not
    // prevent the independent Coordinator from finalizing the bound job.
    let finalize_command = finalize("finalize:key-1", "candidate-1", "submission-1");
    let first_finalize =
        simulator.finalize_submission(&finalize_command, CommandFault::ExpireBeforeAuthorization);
    assert_eq!(first_finalize.origin, ResultOrigin::NewCommit);
    assert!(first_finalize.newly_committed);
    let committed_finalize = simulator
        .coordinator_receipt_response(
            &finalize_command.service_id,
            &finalize_command.idempotency_key,
        )
        .unwrap()
        .clone();

    // Lose the ACK on a receipt replay as well; a later retry recovers exactly
    // the originally committed response without another domain transaction.
    let dropped_replay =
        simulator.finalize_submission(&finalize_command, CommandFault::DropResponseAfterCommit);
    assert!(dropped_replay.client_response.is_none());
    assert_eq!(dropped_replay.origin, ResultOrigin::ReceiptReplay);
    let finalize_retry = simulator.finalize_submission(&finalize_command, CommandFault::None);
    assert_eq!(finalize_retry.client_response, Some(Ok(committed_finalize)));
    assert_eq!(finalize_retry.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(simulator.candidate_count(), 1);
    assert_eq!(simulator.formal_submission_count(), 1);
    assert_eq!(simulator.domain_effect_count(), 2);
    assert_eq!(simulator.outbox_event_count(), 2);
    simulator.validate().unwrap();
}

#[test]
fn generation_three_is_fenced_after_four_but_exact_author_receipt_replays() {
    let mut simulator = ProtocolSimulator::new(44);
    simulator.reassign(1_000).unwrap();
    simulator.reassign(1_000).unwrap();
    let generation_three = simulator.reassign(1_000).unwrap();
    let committed_command = checkpoint(&generation_three, "g3:committed", "checkpoint-g3");
    let committed = simulator.handle_author(&committed_command, CommandFault::None);
    let committed_response = committed.client_response.unwrap().unwrap();

    let generation_four = simulator.reassign(1_000).unwrap();
    assert_eq!(generation_four.generation, 4);
    for stale_write in [
        checkpoint(&generation_three, "g3:new-checkpoint", "late-checkpoint"),
        candidate(&generation_three, "g3:new-candidate", "late-candidate"),
    ] {
        let rejected = simulator.handle_author(&stale_write, CommandFault::None);
        assert_eq!(
            rejected.client_response,
            Some(Err(ProtocolError::LeaseStale))
        );
        assert!(!rejected.newly_committed);
    }

    // Receipt lookup precedes both current-generation and current-expiry checks.
    let replay =
        simulator.handle_author(&committed_command, CommandFault::ExpireBeforeAuthorization);
    assert_eq!(replay.client_response, Some(Ok(committed_response)));
    assert_eq!(replay.origin, ResultOrigin::ReceiptReplay);
    assert_eq!(simulator.domain_effect_count(), 1);
    assert_eq!(simulator.outbox_event_count(), 1);
    simulator.validate().unwrap();
}

#[test]
fn finalize_before_candidate_is_rejected_then_same_command_can_succeed() {
    let mut simulator = ProtocolSimulator::new(101);
    let finalize_command = finalize("finalize:future", "candidate-future", "submission-future");

    let early = simulator.finalize_submission(&finalize_command, CommandFault::None);
    assert_eq!(
        early.client_response,
        Some(Err(ProtocolError::InvalidTransition))
    );
    assert_eq!(simulator.domain_effect_count(), 0);

    let grant = simulator.reassign(1_000).unwrap();
    let recorded = simulator.handle_author(
        &candidate(&grant, "candidate:future", "candidate-future"),
        CommandFault::None,
    );
    assert!(recorded.client_response.unwrap().is_ok());
    let finalized = simulator.finalize_submission(&finalize_command, CommandFault::None);
    assert!(finalized.client_response.unwrap().is_ok());
    assert_eq!(simulator.candidate_count(), 1);
    assert_eq!(simulator.formal_submission_count(), 1);
    simulator.validate().unwrap();
}

#[test]
fn coordinator_job_cas_prevents_a_second_terminal_submission() {
    let mut simulator = ProtocolSimulator::new(202);
    let grant = simulator.reassign(1_000).unwrap();
    simulator.handle_author(
        &candidate(&grant, "candidate:cas", "candidate-cas"),
        CommandFault::None,
    );
    simulator.finalize_submission(
        &finalize("finalize:one", "candidate-cas", "submission-one"),
        CommandFault::None,
    );

    let second = simulator.finalize_submission(
        &finalize("finalize:two", "candidate-cas", "submission-two"),
        CommandFault::None,
    );
    assert_eq!(
        second.client_response,
        Some(Err(ProtocolError::VersionStale))
    );
    assert_eq!(simulator.formal_submission_count(), 1);
    assert_eq!(simulator.domain_effect_count(), 2);
    assert_eq!(simulator.outbox_event_count(), 2);
    simulator.validate().unwrap();
}

#[test]
fn duplicate_reordered_and_lost_ack_delivery_is_deduplicated_by_inbox() {
    let mut simulator = ProtocolSimulator::new(912);
    let grant = simulator.reassign(1_000).unwrap();
    simulator.handle_author(
        &candidate(&grant, "candidate:delivery", "candidate-delivery"),
        CommandFault::None,
    );
    simulator.finalize_submission(
        &finalize(
            "finalize:delivery",
            "candidate-delivery",
            "submission-delivery",
        ),
        CommandFault::None,
    );

    let first = simulator.publish_to_inbox(DeliveryFault::LosePublisherAck);
    assert_eq!(first.delivered, 2);
    assert_eq!(first.newly_applied, 2);
    assert_eq!(first.pending_after_delivery, 2);

    let duplicate = simulator.publish_to_inbox(DeliveryFault::DuplicateAndReorder);
    assert_eq!(duplicate.delivered, 4);
    assert_eq!(duplicate.newly_applied, 0);
    assert_eq!(duplicate.pending_after_delivery, 0);
    assert_eq!(simulator.projected_candidate_count(), 1);
    assert_eq!(simulator.projected_submission_count(), 1);
    simulator.validate().unwrap();
}

#[test]
fn expiry_injection_rejects_author_without_effect_receipt_or_outbox() {
    let mut simulator = ProtocolSimulator::new(55);
    let grant = simulator.reassign(5).unwrap();
    let command = checkpoint(&grant, "expired:key", "expired-checkpoint");

    let handling = simulator.handle_author(&command, CommandFault::ExpireBeforeAuthorization);
    assert_eq!(
        handling.client_response,
        Some(Err(ProtocolError::LeaseExpired))
    );
    assert_eq!(simulator.domain_effect_count(), 0);
    assert_eq!(simulator.outbox_event_count(), 0);
    assert!(
        simulator
            .author_receipt_response(&command.actor_id, &command.idempotency_key)
            .is_none()
    );
}

#[test]
fn failure_seed_persists_and_replays_as_a_single_case() {
    let oracle = InvariantOracle::MaximumGeneration { maximum: 0 };
    let failure = run_seed_with_oracle(424_242, 32, oracle).unwrap_err();
    assert_eq!(failure.seed, 424_242);
    assert_eq!(failure.failed_at_step, 0);

    let directory = tempdir().unwrap();
    let path = failure
        .persist(directory.path(), "intentional-replay-fixture")
        .unwrap();
    let loaded = ScheduleFailure::load(&path).unwrap();
    assert_eq!(loaded, *failure);
    assert_eq!(replay_persisted_failure(&path).unwrap(), *failure);
}

#[test]
fn same_schedule_seed_has_identical_trace_coverage_and_state_digest() {
    assert_eq!(run_seed(73, 96).unwrap(), run_seed(73, 96).unwrap());
}

/// Standalone replay hook:
/// `AGENTFORGE_REPLAY_FAILURE=/path/to/failure.json cargo test -p
/// agentforge-test-support --test protocol_simulator replay_failure_from_env`
#[test]
fn replay_failure_from_env() {
    let Some(path) = std::env::var_os("AGENTFORGE_REPLAY_FAILURE") else {
        return;
    };
    replay_persisted_failure(&PathBuf::from(path)).unwrap();
}
