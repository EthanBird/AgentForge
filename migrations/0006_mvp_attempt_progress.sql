BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Author progress is a typed, immutable fact ledger. It supplements the
-- Attempt aggregate/event stream with the evidence digest that justified each
-- one-step phase transition; it is not a generic JSON snapshot.
CREATE TABLE attempt_progress (
    progress_id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    attempt_id uuid NOT NULL,
    package_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    lease_id uuid NOT NULL,
    node_id uuid NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    stage text NOT NULL CHECK (stage IN (
        'PREPARING', 'PLANNING', 'IMPLEMENTING', 'LOCAL_VERIFY'
    )),
    evidence_digest bytea NOT NULL CHECK (
        octet_length(evidence_digest) = 32
        AND evidence_digest <> decode(repeat('00', 32), 'hex')),
    state_before text NOT NULL,
    state_after text NOT NULL,
    semantic_progress_seq bigint NOT NULL CHECK (semantic_progress_seq > 0),
    attempt_version bigint NOT NULL CHECK (attempt_version > 0),
    recorded_at timestamptz NOT NULL,
    UNIQUE (attempt_id, semantic_progress_seq),
    UNIQUE (attempt_id, attempt_version),
    FOREIGN KEY (package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (attempt_id, package_id, revision_id, fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token),
    FOREIGN KEY (lease_id, attempt_id, package_id, revision_id, fencing_token)
        REFERENCES leases(id, attempt_id, package_id, revision_id, fencing_token),
    CHECK (
        (stage = 'PREPARING'
         AND state_before = 'LEASED' AND state_after = 'PREPARING')
        OR (stage = 'PLANNING'
            AND state_before = 'PREPARING' AND state_after = 'PLANNING')
        OR (stage = 'IMPLEMENTING'
            AND state_before = 'PLANNING' AND state_after = 'IMPLEMENTING')
        OR (stage = 'LOCAL_VERIFY'
            AND state_before = 'IMPLEMENTING' AND state_after = 'LOCAL_VERIFY')
    )
);

CREATE INDEX attempt_progress_recorded_idx
    ON attempt_progress (project_id, recorded_at, progress_id);

CREATE FUNCTION enforce_attempt_progress_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    current_attempt_state text;
    current_attempt_version bigint;
    current_progress_seq bigint;
    current_attempt_updated_at timestamptz;
    current_lease_state text;
    current_holder uuid;
    current_lease_expiry timestamptz;
BEGIN
    SELECT attempt.state, attempt.version, attempt.semantic_progress_seq,
           attempt.updated_at,
           lease.state, lease.holder_node_id, lease.expires_at
      INTO current_attempt_state, current_attempt_version, current_progress_seq,
           current_attempt_updated_at,
           current_lease_state, current_holder, current_lease_expiry
      FROM attempts attempt
      JOIN work_packages package
        ON package.id = attempt.package_id
       AND package.project_id = NEW.project_id
      JOIN leases lease
        ON lease.id = NEW.lease_id
       AND lease.attempt_id = attempt.id
       AND lease.package_id = attempt.package_id
       AND lease.revision_id = attempt.revision_id
       AND lease.fencing_token = attempt.fencing_token
     WHERE attempt.id = NEW.attempt_id
       AND attempt.package_id = NEW.package_id
       AND attempt.revision_id = NEW.revision_id
       AND attempt.lease_id = lease.id
       AND attempt.fencing_token = NEW.fencing_token
       AND package.state = 'ACTIVE'
       AND package.active_attempt_id = attempt.id
       AND package.active_lease_id = lease.id
       AND package.active_fencing_token = lease.fencing_token;
    IF NOT FOUND
       OR current_attempt_state <> NEW.state_after
       OR current_attempt_version <> NEW.attempt_version
       OR current_progress_seq <> NEW.semantic_progress_seq
       OR current_attempt_updated_at IS DISTINCT FROM NEW.recorded_at
       OR current_lease_state <> 'ACTIVE'
       OR current_holder <> NEW.node_id
       OR NEW.recorded_at >= current_lease_expiry
       OR clock_timestamp() >= current_lease_expiry THEN
        RAISE EXCEPTION 'Attempt progress does not match current author authority: %',
            NEW.progress_id USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER attempt_progress_matches_current_authority
BEFORE INSERT ON attempt_progress
FOR EACH ROW EXECUTE FUNCTION enforce_attempt_progress_insert();

CREATE TRIGGER attempt_progress_is_immutable
BEFORE UPDATE OR DELETE ON attempt_progress
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

COMMIT;
