BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- One immutable Candidate has exactly one official verification lineage.
-- The author handoff transaction must also advance the Attempt/Package,
-- close the author Lease, and enqueue the verifier obligation before commit.
ALTER TABLE verification_runs
    ADD CONSTRAINT verification_runs_one_per_candidate UNIQUE (candidate_id);

CREATE UNIQUE INDEX obligations_one_verify_candidate_idx
    ON obligations (project_id, subject_type, subject_id)
    WHERE obligation_type = 'VERIFY_CANDIDATE';

CREATE FUNCTION enforce_candidate_handoff_atomic() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM attempts attempt
          JOIN work_packages package
            ON package.id = attempt.package_id
           AND package.project_id = NEW.project_id
          JOIN leases author_lease
            ON author_lease.id = NEW.lease_id
           AND author_lease.attempt_id = attempt.id
           AND author_lease.package_id = attempt.package_id
           AND author_lease.revision_id = attempt.revision_id
           AND author_lease.fencing_token = attempt.fencing_token
          JOIN verification_runs run
            ON run.candidate_id = NEW.id
           AND run.project_id = NEW.project_id
           AND run.candidate_commit = NEW.candidate_commit
          JOIN obligations obligation
            ON obligation.project_id = NEW.project_id
           AND obligation.subject_type = 'VERIFICATION_RUN'
           AND obligation.subject_id = run.id
           AND obligation.obligation_type = 'VERIFY_CANDIDATE'
         WHERE attempt.id = NEW.attempt_id
           AND attempt.package_id = NEW.package_id
           AND attempt.revision_id = NEW.revision_id
           AND attempt.lease_id = NEW.lease_id
           AND attempt.fencing_token = NEW.fencing_token
           AND attempt.state = 'CANDIDATE'
           AND attempt.candidate_commit = NEW.candidate_commit
           AND package.state = 'VERIFYING'
           AND package.active_attempt_id = attempt.id
           AND author_lease.state = 'RELEASED'
           AND run.state = 'QUEUED'
           AND run.version = 1
           AND run.event_seq = 1
           AND run.queued_at = NEW.sealed_at
           AND run.updated_at = NEW.sealed_at
           AND obligation.state = 'PENDING'
           AND obligation.due_at = NEW.sealed_at
           AND obligation.owner_id IS NULL
           AND obligation.owner_generation = 0
           AND obligation.claim_expires_at IS NULL
           AND obligation.attempts = 0
           AND obligation.payload ->> 'candidate_id' = NEW.id::text
           AND obligation.payload ->> 'verification_run_id' = run.id::text
           AND obligation.payload ->> 'attempt_id' = NEW.attempt_id::text
           AND obligation.payload ->> 'artifact_id' = NEW.bundle_artifact_id::text
           AND obligation.payload ->> 'candidate_commit' = NEW.candidate_commit
    ) THEN
        RAISE EXCEPTION 'Candidate handoff is not atomic: %', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER candidate_handoff_is_atomic
AFTER INSERT ON candidates
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_candidate_handoff_atomic();

COMMIT;
