BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Candidate-first aggregates are canonical typed rows. The event head accepts
-- the three new aggregate kinds only after their typed authority exists in the
-- same migration transaction.
LOCK TABLE aggregate_event_heads IN ACCESS EXCLUSIVE MODE;
ALTER TABLE aggregate_event_heads
    DROP CONSTRAINT aggregate_event_heads_aggregate_type_check;
ALTER TABLE aggregate_event_heads
    ADD CONSTRAINT aggregate_event_heads_aggregate_type_check CHECK (aggregate_type IN (
        'WORK_PACKAGE', 'ATTEMPT', 'LEASE', 'CANDIDATE_ARTIFACT', 'CANDIDATE',
        'VERIFICATION_RUN', 'SUBMISSION', 'RUN_SIGNAL', 'INVOCATION_INTENT',
        'INVOCATION_RUN', 'RUN_CLAIM', 'SESSION_CAPSULE', 'BUDGET_RESERVATION',
        'GOVERNANCE_CASE', 'DECISION', 'POLICY_REVISION'
    ));

CREATE FUNCTION candidate_sha256_array_is_valid(digests bytea[]) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    item bytea;
BEGIN
    IF cardinality(digests) < 1 OR cardinality(digests) > 4096 THEN
        RETURN false;
    END IF;
    FOREACH item IN ARRAY digests LOOP
        IF item IS NULL
           OR octet_length(item) <> 32
           OR item = decode(repeat('00', 32), 'hex') THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END;
$$;

CREATE TABLE candidate_artifacts (
    id uuid PRIMARY KEY,
    reserved_candidate_id uuid NOT NULL UNIQUE,
    project_id uuid NOT NULL REFERENCES projects(id),
    attempt_id uuid NOT NULL,
    package_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    package_hash bytea NOT NULL CHECK (
        octet_length(package_hash) = 32
        AND package_hash <> decode(repeat('00', 32), 'hex')),
    lease_id uuid NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    base_commit text NOT NULL CHECK (
        length(base_commit) IN (40, 64)
        AND base_commit ~ '^[0-9a-f]+$'
        AND base_commit !~ '^0+$'),
    candidate_commit text NOT NULL CHECK (
        length(candidate_commit) IN (40, 64)
        AND candidate_commit ~ '^[0-9a-f]+$'
        AND candidate_commit !~ '^0+$'),
    tree_hash text NOT NULL CHECK (
        length(tree_hash) IN (40, 64)
        AND tree_hash ~ '^[0-9a-f]+$'
        AND tree_hash !~ '^0+$'),
    author_evidence_digest bytea NOT NULL CHECK (
        octet_length(author_evidence_digest) = 32
        AND author_evidence_digest <> decode(repeat('00', 32), 'hex')),
    expected_bundle_digest bytea NOT NULL CHECK (
        octet_length(expected_bundle_digest) = 32
        AND expected_bundle_digest <> decode(repeat('00', 32), 'hex')),
    expected_bundle_size_bytes bigint NOT NULL CHECK (
        expected_bundle_size_bytes BETWEEN 1 AND 16777216),
    expected_chunk_digests bytea[] NOT NULL CHECK (
        candidate_sha256_array_is_valid(expected_chunk_digests)),
    state text NOT NULL CHECK (state IN (
        'UPLOADING', 'ASSEMBLING', 'COMPLETE', 'REJECTED', 'QUARANTINED', 'EXPIRED'
    )),
    bundle_protocol_key text,
    bundle_uri text,
    bundle_digest bytea CHECK (
        bundle_digest IS NULL OR (
            octet_length(bundle_digest) = 32
            AND bundle_digest <> decode(repeat('00', 32), 'hex'))),
    bundle_size_bytes bigint CHECK (
        bundle_size_bytes IS NULL OR bundle_size_bytes BETWEEN 1 AND 16777216),
    version bigint NOT NULL CHECK (version > 0),
    event_seq bigint NOT NULL CHECK (event_seq > 0),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    completed_at timestamptz,
    UNIQUE (id, reserved_candidate_id),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, attempt_id, package_id, revision_id, package_hash,
            lease_id, fencing_token),
    FOREIGN KEY (package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (revision_id, package_id, package_hash)
        REFERENCES package_revisions(id, package_id, package_hash),
    FOREIGN KEY (attempt_id, package_id, revision_id, fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token),
    FOREIGN KEY (lease_id, attempt_id, package_id, revision_id, fencing_token)
        REFERENCES leases(id, attempt_id, package_id, revision_id, fencing_token),
    CHECK (created_at < expires_at),
    CHECK (updated_at >= created_at),
    CHECK (length(base_commit) = length(candidate_commit)),
    CHECK (length(candidate_commit) = length(tree_hash)),
    CHECK (cardinality(expected_chunk_digests) <= expected_bundle_size_bytes),
    CHECK (
        (state = 'COMPLETE'
         AND bundle_protocol_key IS NOT NULL
         AND bundle_protocol_key ~ '^[A-Za-z0-9._-]{1,128}$'
         AND bundle_uri IS NOT NULL
         AND bundle_uri LIKE 'artifact://%'
         AND length(bundle_uri) <= 2048
         AND bundle_digest = expected_bundle_digest
         AND bundle_size_bytes = expected_bundle_size_bytes
         AND completed_at = updated_at)
        OR
        (state <> 'COMPLETE'
         AND bundle_protocol_key IS NULL
         AND bundle_uri IS NULL
         AND bundle_digest IS NULL
         AND bundle_size_bytes IS NULL
         AND completed_at IS NULL)
    ),
    CHECK (state = 'EXPIRED' OR updated_at < expires_at),
    CHECK (state <> 'EXPIRED' OR updated_at >= expires_at)
);

CREATE INDEX candidate_artifacts_upload_idx
    ON candidate_artifacts (expires_at, id)
    WHERE state IN ('UPLOADING', 'ASSEMBLING');
CREATE INDEX candidate_artifacts_attempt_idx
    ON candidate_artifacts (attempt_id, created_at DESC, id);

CREATE FUNCTION enforce_candidate_artifact_write() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    author_lease_state text;
    author_lease_expires_at timestamptz;
    attempt_base_commit text;
    selected_revision uuid;
    received_chunks bigint;
    received_bytes numeric;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Candidate Artifact cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF OLD.state IN ('COMPLETE', 'REJECTED', 'QUARANTINED', 'EXPIRED') THEN
            RAISE EXCEPTION 'terminal Candidate Artifact is immutable: %', OLD.id
                USING ERRCODE = '55000';
        END IF;
        IF NEW.id <> OLD.id
           OR NEW.reserved_candidate_id <> OLD.reserved_candidate_id
           OR NEW.project_id <> OLD.project_id
           OR NEW.attempt_id <> OLD.attempt_id
           OR NEW.package_id <> OLD.package_id
           OR NEW.revision_id <> OLD.revision_id
           OR NEW.package_hash <> OLD.package_hash
           OR NEW.lease_id <> OLD.lease_id
           OR NEW.fencing_token <> OLD.fencing_token
           OR NEW.base_commit <> OLD.base_commit
           OR NEW.candidate_commit <> OLD.candidate_commit
           OR NEW.tree_hash <> OLD.tree_hash
           OR NEW.author_evidence_digest <> OLD.author_evidence_digest
           OR NEW.expected_bundle_digest <> OLD.expected_bundle_digest
           OR NEW.expected_bundle_size_bytes <> OLD.expected_bundle_size_bytes
           OR NEW.expected_chunk_digests <> OLD.expected_chunk_digests
           OR NEW.created_at <> OLD.created_at
           OR NEW.expires_at <> OLD.expires_at THEN
            RAISE EXCEPTION 'Candidate Artifact binding is immutable: %', OLD.id
                USING ERRCODE = '55000';
        END IF;
        IF NEW.version <> OLD.version + 1
           OR NEW.event_seq <> OLD.event_seq + 1
           OR NEW.updated_at < OLD.updated_at THEN
            RAISE EXCEPTION 'Candidate Artifact version/time is not monotonic: %', OLD.id
                USING ERRCODE = '40001';
        END IF;
        IF NOT (
            (OLD.state = 'UPLOADING' AND NEW.state IN (
                'ASSEMBLING', 'REJECTED', 'QUARANTINED', 'EXPIRED'))
            OR
            (OLD.state = 'ASSEMBLING' AND NEW.state IN (
                'COMPLETE', 'REJECTED', 'QUARANTINED', 'EXPIRED'))
        ) THEN
            RAISE EXCEPTION 'Candidate Artifact transition is invalid: % -> %',
                OLD.state, NEW.state USING ERRCODE = '55000';
        END IF;
    ELSIF NEW.version <> 1 OR NEW.event_seq <> 1 OR NEW.state <> 'UPLOADING'
          OR NEW.updated_at <> NEW.created_at THEN
        RAISE EXCEPTION 'Candidate Artifact must begin as version one UPLOADING'
            USING ERRCODE = '23514';
    END IF;

    SELECT lease.state, lease.expires_at, attempt.base_commit, package.selected_revision_id
      INTO author_lease_state, author_lease_expires_at, attempt_base_commit, selected_revision
      FROM leases lease
      JOIN attempts attempt ON attempt.id = lease.attempt_id
      JOIN work_packages package ON package.id = lease.package_id
     WHERE lease.id = NEW.lease_id
       AND lease.attempt_id = NEW.attempt_id
       AND lease.package_id = NEW.package_id
       AND lease.revision_id = NEW.revision_id
       AND lease.fencing_token = NEW.fencing_token
     FOR UPDATE OF lease, attempt, package;
    IF NOT FOUND
       OR attempt_base_commit <> NEW.base_commit
       OR selected_revision IS DISTINCT FROM NEW.revision_id
       OR NEW.expires_at > author_lease_expires_at THEN
        RAISE EXCEPTION 'Candidate Artifact author binding is stale: %', NEW.id
            USING ERRCODE = '23503';
    END IF;

    IF (TG_OP = 'INSERT' OR NEW.state IN ('ASSEMBLING', 'COMPLETE'))
       AND (author_lease_state <> 'ACTIVE'
            OR NEW.updated_at >= author_lease_expires_at
            OR clock_timestamp() >= author_lease_expires_at) THEN
        RAISE EXCEPTION 'Candidate Artifact author Lease is not active: %', NEW.lease_id
            USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.state = 'COMPLETE' THEN
        SELECT count(*), coalesce(sum(size_bytes), 0)
          INTO received_chunks, received_bytes
          FROM candidate_artifact_chunks
         WHERE artifact_id = NEW.id;
        IF received_chunks <> cardinality(NEW.expected_chunk_digests)
           OR received_bytes <> NEW.expected_bundle_size_bytes THEN
            RAISE EXCEPTION 'Candidate Artifact chunks are incomplete: %', NEW.id
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER candidate_artifacts_binding_is_immutable
BEFORE INSERT OR UPDATE OR DELETE ON candidate_artifacts
FOR EACH ROW EXECUTE FUNCTION enforce_candidate_artifact_write();

CREATE TABLE candidate_artifact_chunks (
    artifact_id uuid NOT NULL REFERENCES candidate_artifacts(id),
    chunk_index integer NOT NULL CHECK (chunk_index BETWEEN 0 AND 4095),
    digest bytea NOT NULL CHECK (
        octet_length(digest) = 32
        AND digest <> decode(repeat('00', 32), 'hex')),
    size_bytes integer NOT NULL CHECK (size_bytes BETWEEN 1 AND 1048576),
    content bytea NOT NULL,
    received_at timestamptz NOT NULL,
    PRIMARY KEY (artifact_id, chunk_index),
    CHECK (octet_length(content) = size_bytes)
);

CREATE FUNCTION enforce_candidate_artifact_chunk() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    artifact_state text;
    artifact_expires_at timestamptz;
    expected_digest bytea;
    expected_count integer;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Candidate Artifact chunk is immutable'
            USING ERRCODE = '55000';
    END IF;
    SELECT state, expires_at, expected_chunk_digests[NEW.chunk_index + 1],
           cardinality(expected_chunk_digests)
      INTO artifact_state, artifact_expires_at, expected_digest, expected_count
      FROM candidate_artifacts
     WHERE id = NEW.artifact_id
     FOR UPDATE;
    IF NOT FOUND
       OR artifact_state NOT IN ('UPLOADING', 'ASSEMBLING')
       OR NEW.received_at >= artifact_expires_at
       OR NEW.chunk_index >= expected_count
       OR expected_digest IS DISTINCT FROM NEW.digest
       OR sha256(NEW.content) <> NEW.digest THEN
        RAISE EXCEPTION 'Candidate Artifact chunk does not match reservation: %.%',
            NEW.artifact_id, NEW.chunk_index USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER candidate_artifact_chunks_match_reservation
BEFORE INSERT OR UPDATE OR DELETE ON candidate_artifact_chunks
FOR EACH ROW EXECUTE FUNCTION enforce_candidate_artifact_chunk();

CREATE TABLE candidates (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    attempt_id uuid NOT NULL,
    package_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    package_hash bytea NOT NULL CHECK (
        octet_length(package_hash) = 32
        AND package_hash <> decode(repeat('00', 32), 'hex')),
    lease_id uuid NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    base_commit text NOT NULL,
    candidate_commit text NOT NULL,
    tree_hash text NOT NULL,
    branch text NOT NULL CHECK (
        length(branch) BETWEEN 24 AND 255
        AND branch ~ '^refs/heads/agentforge/[A-Za-z0-9._/-]+$'
        AND right(branch, 1) <> '/'
        AND position('..' in branch) = 0
        AND position('//' in branch) = 0
        AND position('@{' in branch) = 0),
    author_evidence_digest bytea NOT NULL CHECK (
        octet_length(author_evidence_digest) = 32
        AND author_evidence_digest <> decode(repeat('00', 32), 'hex')),
    bundle_artifact_id uuid NOT NULL,
    bundle_protocol_key text NOT NULL CHECK (
        bundle_protocol_key ~ '^[A-Za-z0-9._-]{1,128}$'),
    bundle_uri text NOT NULL CHECK (
        bundle_uri LIKE 'artifact://%' AND length(bundle_uri) <= 2048),
    bundle_digest bytea NOT NULL CHECK (
        octet_length(bundle_digest) = 32
        AND bundle_digest <> decode(repeat('00', 32), 'hex')),
    sealed_at timestamptz NOT NULL,
    version bigint NOT NULL CHECK (version = 1),
    event_seq bigint NOT NULL CHECK (event_seq = 1),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, candidate_commit),
    FOREIGN KEY (bundle_artifact_id, id)
        REFERENCES candidate_artifacts(id, reserved_candidate_id),
    FOREIGN KEY (attempt_id, package_id, revision_id, fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token),
    FOREIGN KEY (lease_id, attempt_id, package_id, revision_id, fencing_token)
        REFERENCES leases(id, attempt_id, package_id, revision_id, fencing_token),
    CHECK (length(base_commit) IN (40, 64) AND base_commit ~ '^[0-9a-f]+$'),
    CHECK (length(candidate_commit) = length(base_commit)
           AND candidate_commit ~ '^[0-9a-f]+$' AND candidate_commit !~ '^0+$'),
    CHECK (length(tree_hash) = length(base_commit)
           AND tree_hash ~ '^[0-9a-f]+$' AND tree_hash !~ '^0+$')
);

CREATE INDEX candidates_package_sealed_idx
    ON candidates (package_id, sealed_at DESC, id);

CREATE FUNCTION enforce_candidate_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    artifact candidate_artifacts%ROWTYPE;
BEGIN
    SELECT * INTO artifact FROM candidate_artifacts
     WHERE id = NEW.bundle_artifact_id
     FOR SHARE;
    IF NOT FOUND
       OR artifact.state <> 'COMPLETE'
       OR artifact.reserved_candidate_id <> NEW.id
       OR artifact.project_id <> NEW.project_id
       OR artifact.attempt_id <> NEW.attempt_id
       OR artifact.package_id <> NEW.package_id
       OR artifact.revision_id <> NEW.revision_id
       OR artifact.package_hash <> NEW.package_hash
       OR artifact.lease_id <> NEW.lease_id
       OR artifact.fencing_token <> NEW.fencing_token
       OR artifact.base_commit <> NEW.base_commit
       OR artifact.candidate_commit <> NEW.candidate_commit
       OR artifact.tree_hash <> NEW.tree_hash
       OR artifact.author_evidence_digest <> NEW.author_evidence_digest
       OR artifact.bundle_protocol_key <> NEW.bundle_protocol_key
       OR artifact.bundle_uri <> NEW.bundle_uri
       OR artifact.bundle_digest <> NEW.bundle_digest
       OR NEW.sealed_at < artifact.updated_at
       OR NOT EXISTS (
           SELECT 1 FROM leases author_lease
            WHERE author_lease.id = artifact.lease_id
              AND author_lease.state = 'ACTIVE'
              AND NEW.sealed_at < author_lease.expires_at
              AND clock_timestamp() < author_lease.expires_at
       ) THEN
        RAISE EXCEPTION 'Candidate Artifact % is not complete or its lineage changed',
            NEW.bundle_artifact_id USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER candidates_match_complete_artifact
BEFORE INSERT ON candidates
FOR EACH ROW EXECUTE FUNCTION enforce_candidate_insert();
CREATE TRIGGER candidates_are_immutable
BEFORE UPDATE OR DELETE ON candidates
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE verification_runs (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    candidate_id uuid NOT NULL,
    candidate_commit text NOT NULL,
    state text NOT NULL CHECK (state IN (
        'QUEUED', 'PROVENANCE_CHECK', 'REVIEWING', 'REPRODUCING',
        'PASS', 'FAIL', 'INCONCLUSIVE', 'CANCELLED'
    )),
    terminal_stage text CHECK (terminal_stage IN (
        'PROVENANCE_CHECK', 'REVIEWING', 'REPRODUCING'
    )),
    terminal_stage_result_id uuid,
    reviewed_head text,
    tested_head text,
    evidence_digest bytea CHECK (
        evidence_digest IS NULL OR (
            octet_length(evidence_digest) = 32
            AND evidence_digest <> decode(repeat('00', 32), 'hex'))),
    queued_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    terminalized_at timestamptz,
    version bigint NOT NULL CHECK (version > 0),
    event_seq bigint NOT NULL CHECK (event_seq > 0),
    UNIQUE (id, project_id),
    FOREIGN KEY (candidate_id, project_id, candidate_commit)
        REFERENCES candidates(id, project_id, candidate_commit),
    CHECK (length(candidate_commit) IN (40, 64)
           AND candidate_commit ~ '^[0-9a-f]+$' AND candidate_commit !~ '^0+$'),
    CHECK (updated_at >= queued_at),
    CHECK (reviewed_head IS NULL OR reviewed_head = candidate_commit),
    CHECK (tested_head IS NULL OR tested_head = candidate_commit),
    CHECK (terminal_stage_result_id IS NULL
           OR terminal_stage_result_id <> '00000000-0000-0000-0000-000000000000'),
    CHECK (
        (state = 'QUEUED' AND version = 1)
        OR (state = 'PROVENANCE_CHECK' AND version = 2)
        OR (state = 'REVIEWING' AND version = 3)
        OR (state = 'REPRODUCING' AND version = 4)
        OR (state = 'PASS' AND version = 5)
        OR (state IN ('FAIL', 'INCONCLUSIVE') AND (
            (terminal_stage = 'PROVENANCE_CHECK' AND version = 3)
            OR (terminal_stage = 'REVIEWING' AND version = 4)
            OR (terminal_stage = 'REPRODUCING' AND version = 5)))
        OR (state = 'CANCELLED' AND version BETWEEN 2 AND 5)
    ),
    CHECK (event_seq = version),
    CHECK (
        (state IN ('QUEUED', 'PROVENANCE_CHECK', 'REVIEWING', 'REPRODUCING')
         AND terminal_stage IS NULL
         AND terminal_stage_result_id IS NULL
         AND evidence_digest IS NULL
         AND terminalized_at IS NULL)
        OR
        (state IN ('PASS', 'FAIL', 'INCONCLUSIVE')
         AND terminal_stage IS NOT NULL
         AND terminal_stage_result_id IS NOT NULL
         AND evidence_digest IS NOT NULL
         AND terminalized_at = updated_at)
        OR
        (state = 'CANCELLED'
         AND terminal_stage IS NULL
         AND terminal_stage_result_id IS NULL
         AND evidence_digest IS NULL
         AND terminalized_at = updated_at)
    ),
    CHECK (
        (state IN ('QUEUED', 'PROVENANCE_CHECK', 'REVIEWING')
         AND reviewed_head IS NULL AND tested_head IS NULL)
        OR
        (state = 'REPRODUCING'
         AND reviewed_head = candidate_commit AND tested_head IS NULL)
        OR
        (state = 'PASS'
         AND terminal_stage = 'REPRODUCING'
         AND reviewed_head = candidate_commit AND tested_head = candidate_commit)
        OR
        (state IN ('FAIL', 'INCONCLUSIVE') AND (
            (terminal_stage = 'PROVENANCE_CHECK'
             AND reviewed_head IS NULL AND tested_head IS NULL)
            OR (terminal_stage = 'REVIEWING' AND tested_head IS NULL)
            OR (terminal_stage = 'REPRODUCING'
                AND reviewed_head = candidate_commit
                AND (tested_head IS NULL OR tested_head = candidate_commit))))
        OR
        (state = 'CANCELLED'
         AND tested_head IS NULL
         AND (reviewed_head IS NULL OR reviewed_head = candidate_commit))
    )
);

CREATE INDEX verification_runs_queue_idx
    ON verification_runs (queued_at, id) WHERE state = 'QUEUED';
CREATE INDEX verification_runs_candidate_idx
    ON verification_runs (candidate_id, queued_at DESC, id);

CREATE FUNCTION enforce_verification_run_write() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'verification run cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'QUEUED' OR NEW.version <> 1 OR NEW.event_seq <> 1
           OR NEW.updated_at <> NEW.queued_at THEN
            RAISE EXCEPTION 'verification run must begin QUEUED at version one'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.state IN ('PASS', 'FAIL', 'INCONCLUSIVE', 'CANCELLED') THEN
        RAISE EXCEPTION 'terminal verification run is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.project_id <> OLD.project_id
       OR NEW.candidate_id <> OLD.candidate_id
       OR NEW.candidate_commit <> OLD.candidate_commit
       OR NEW.queued_at <> OLD.queued_at THEN
        RAISE EXCEPTION 'verification run binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.version <> OLD.version + 1
       OR NEW.event_seq <> OLD.event_seq + 1
       OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'verification run version/time is not monotonic: %', OLD.id
            USING ERRCODE = '40001';
    END IF;
    IF NOT (
        (OLD.state = 'QUEUED' AND NEW.state IN ('PROVENANCE_CHECK', 'CANCELLED'))
        OR
        (OLD.state = 'PROVENANCE_CHECK'
         AND NEW.state IN ('REVIEWING', 'FAIL', 'INCONCLUSIVE', 'CANCELLED'))
        OR
        (OLD.state = 'REVIEWING'
         AND NEW.state IN ('REPRODUCING', 'FAIL', 'INCONCLUSIVE', 'CANCELLED'))
        OR
        (OLD.state = 'REPRODUCING'
         AND NEW.state IN ('PASS', 'FAIL', 'INCONCLUSIVE', 'CANCELLED'))
    ) THEN
        RAISE EXCEPTION 'verification run transition is invalid: % -> %',
            OLD.state, NEW.state USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER verification_runs_binding_is_immutable
BEFORE INSERT OR UPDATE OR DELETE ON verification_runs
FOR EACH ROW EXECUTE FUNCTION enforce_verification_run_write();

COMMIT;
