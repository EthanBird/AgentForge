BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE projects (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    name text NOT NULL,
    state text NOT NULL CHECK (state IN ('ACTIVE', 'PAUSED', 'CLOSED')),
    graph_version bigint NOT NULL DEFAULT 0 CHECK (graph_version >= 0),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE graph_versions (
    project_id uuid NOT NULL REFERENCES projects(id),
    graph_version bigint NOT NULL CHECK (graph_version > 0),
    base_graph_version bigint NOT NULL CHECK (base_graph_version >= 0),
    patch jsonb NOT NULL,
    patch_hash bytea NOT NULL CHECK (octet_length(patch_hash) = 32),
    created_by uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, graph_version),
    UNIQUE (project_id, patch_hash)
);

CREATE TABLE work_packages (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    protocol_key text NOT NULL,
    selected_revision_id uuid,
    state text NOT NULL CHECK (state IN (
        'DRAFT', 'VALIDATING', 'BLOCKED', 'OFFERED', 'ACTIVE', 'VERIFYING',
        'REWORK_READY', 'ACCEPTED', 'INTEGRATING', 'REBASE_REQUIRED',
        'INTEGRATED', 'CLOSED', 'CANCELLED', 'SUPERSEDED', 'FAILED'
    )),
    priority smallint NOT NULL DEFAULT 50 CHECK (priority BETWEEN 0 AND 100),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    attempts_started integer NOT NULL DEFAULT 0 CHECK (attempts_started >= 0),
    next_fencing_token bigint NOT NULL DEFAULT 0 CHECK (next_fencing_token >= 0),
    active_attempt_id uuid,
    accepted_submission_id uuid,
    integrated_integration_id uuid,
    integrated_commit text,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (attempts_started <= max_attempts),
    CHECK ((state IN ('INTEGRATED', 'CLOSED')) =
           (integrated_commit IS NOT NULL AND integrated_integration_id IS NOT NULL)),
    UNIQUE (project_id, protocol_key),
    UNIQUE (id, project_id)
);

CREATE INDEX work_packages_market_idx
    ON work_packages (priority DESC, created_at, id)
    WHERE state IN ('OFFERED', 'REWORK_READY');
CREATE INDEX work_packages_project_state_idx
    ON work_packages (project_id, state, id);

CREATE TABLE package_revisions (
    id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision integer NOT NULL CHECK (revision > 0),
    schema_version text NOT NULL,
    canonical_document jsonb NOT NULL,
    package_hash bytea NOT NULL CHECK (octet_length(package_hash) = 32),
    base_commit text NOT NULL,
    git_object_format text NOT NULL CHECK (git_object_format IN ('sha1', 'sha256')),
    input_snapshot jsonb NOT NULL,
    created_by uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, package_id),
    UNIQUE (id, package_id, package_hash),
    UNIQUE (package_id, revision),
    UNIQUE (package_id, package_hash)
);

CREATE FUNCTION reject_immutable_row_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'immutable relation: %', TG_TABLE_NAME USING ERRCODE = '55000';
END;
$$;

CREATE FUNCTION reject_terminal_state_revival() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state = ANY (string_to_array(TG_ARGV[0], ','))
       AND NEW.state IS DISTINCT FROM OLD.state THEN
        RAISE EXCEPTION 'terminal state cannot transition: %.% % -> %',
            TG_TABLE_NAME, OLD.id, OLD.state, NEW.state USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER package_revisions_are_immutable
BEFORE UPDATE OR DELETE ON package_revisions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

ALTER TABLE work_packages
    ADD CONSTRAINT work_packages_selected_revision_fk
    FOREIGN KEY (selected_revision_id, id)
    REFERENCES package_revisions(id, package_id);

CREATE TABLE package_edges (
    project_id uuid NOT NULL REFERENCES projects(id),
    graph_version bigint NOT NULL,
    from_package_id uuid NOT NULL,
    to_package_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN (
        'HARD_DEPENDENCY', 'ARTIFACT_DEPENDENCY', 'SOFT_CONTEXT', 'REVIEW_OF',
        'GATE', 'CONFLICTS_WITH', 'MUTEX', 'INTEGRATION_AFTER', 'SUPERSEDES'
    )),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (project_id, graph_version, from_package_id, to_package_id, kind),
    FOREIGN KEY (project_id, graph_version)
        REFERENCES graph_versions(project_id, graph_version),
    FOREIGN KEY (from_package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (to_package_id, project_id)
        REFERENCES work_packages(id, project_id),
    CHECK (from_package_id <> to_package_id)
);

CREATE INDEX package_edges_to_idx
    ON package_edges (project_id, graph_version, to_package_id, kind);

CREATE TABLE attempts (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision_id uuid NOT NULL REFERENCES package_revisions(id),
    executor_id uuid NOT NULL,
    node_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN (
        'CREATED', 'LEASED', 'PREPARING', 'PLANNING', 'IMPLEMENTING',
        'LOCAL_VERIFY', 'WAITING_INPUT', 'CANDIDATE', 'ISOLATED_REVIEW',
        'CLEAN_REPRODUCE', 'SUBMITTED', 'PASSED', 'REJECTED', 'LOST',
        'FAILED', 'CANCELLED'
    )),
    lease_id uuid,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    base_commit text NOT NULL,
    wake_condition jsonb,
    resume_state text,
    semantic_progress_seq bigint NOT NULL DEFAULT 0 CHECK (semantic_progress_seq >= 0),
    last_semantic_progress_at timestamptz,
    last_checkpoint_digest bytea CHECK (
        last_checkpoint_digest IS NULL OR octet_length(last_checkpoint_digest) = 32),
    candidate_commit text,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state = 'WAITING_INPUT') = (wake_condition IS NOT NULL)),
    CHECK ((state = 'WAITING_INPUT') = (resume_state IS NOT NULL)),
    UNIQUE (id, package_id),
    UNIQUE (id, package_id, revision_id),
    UNIQUE (id, package_id, revision_id, fencing_token),
    FOREIGN KEY (revision_id, package_id)
        REFERENCES package_revisions(id, package_id)
);

CREATE INDEX attempts_package_created_idx ON attempts (package_id, created_at DESC);
CREATE INDEX attempts_stalled_idx ON attempts (last_semantic_progress_at, id)
    WHERE state IN ('PLANNING', 'IMPLEMENTING', 'LOCAL_VERIFY');

CREATE FUNCTION enforce_attempt_binding_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'attempt cannot be deleted: %', OLD.id USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.protocol_key <> OLD.protocol_key
       OR NEW.package_id <> OLD.package_id
       OR NEW.revision_id <> OLD.revision_id
       OR NEW.executor_id <> OLD.executor_id
       OR NEW.node_id <> OLD.node_id
       OR NEW.fencing_token <> OLD.fencing_token
       OR NEW.base_commit <> OLD.base_commit
       OR NEW.created_at <> OLD.created_at
       OR (OLD.lease_id IS NOT NULL AND NEW.lease_id IS DISTINCT FROM OLD.lease_id)
       OR (OLD.candidate_commit IS NOT NULL
           AND NEW.candidate_commit IS DISTINCT FROM OLD.candidate_commit) THEN
        RAISE EXCEPTION 'attempt binding is immutable: %', OLD.id USING ERRCODE = '55000';
    END IF;
    IF OLD.state IN ('PASSED', 'REJECTED', 'LOST', 'FAILED', 'CANCELLED')
       AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal attempt is immutable: %', OLD.id USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER attempts_binding_is_immutable
BEFORE UPDATE OR DELETE ON attempts
FOR EACH ROW EXECUTE FUNCTION enforce_attempt_binding_immutability();

CREATE TABLE leases (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision_id uuid NOT NULL REFERENCES package_revisions(id),
    attempt_id uuid NOT NULL UNIQUE REFERENCES attempts(id),
    holder_node_id uuid NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    state text NOT NULL CHECK (state IN ('ACTIVE', 'RELEASED', 'REVOKED', 'EXPIRED')),
    granted_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    max_expires_at timestamptz NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    terminal_reason text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (granted_at < expires_at),
    CHECK (expires_at <= max_expires_at),
    UNIQUE (revision_id, fencing_token),
    UNIQUE (id, attempt_id, fencing_token),
    UNIQUE (id, attempt_id, package_id, revision_id, fencing_token),
    FOREIGN KEY (revision_id, package_id)
        REFERENCES package_revisions(id, package_id),
    FOREIGN KEY (attempt_id, package_id, revision_id, fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX leases_one_active_revision_idx
    ON leases (revision_id) WHERE state = 'ACTIVE';
CREATE INDEX leases_expiry_idx ON leases (expires_at, id) WHERE state = 'ACTIVE';

CREATE FUNCTION enforce_lease_binding_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'lease cannot be deleted: %', OLD.id USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.protocol_key <> OLD.protocol_key
       OR NEW.package_id <> OLD.package_id
       OR NEW.revision_id <> OLD.revision_id
       OR NEW.attempt_id <> OLD.attempt_id
       OR NEW.holder_node_id <> OLD.holder_node_id
       OR NEW.fencing_token <> OLD.fencing_token
       OR NEW.granted_at <> OLD.granted_at
       OR NEW.max_expires_at <> OLD.max_expires_at THEN
        RAISE EXCEPTION 'lease binding is immutable: %', OLD.id USING ERRCODE = '55000';
    END IF;
    IF OLD.state <> 'ACTIVE' AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal lease is immutable: %', OLD.id USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER leases_binding_is_immutable
BEFORE UPDATE OR DELETE ON leases
FOR EACH ROW EXECUTE FUNCTION enforce_lease_binding_immutability();

CREATE TRIGGER leases_terminal_state_is_irreversible
BEFORE UPDATE ON leases
FOR EACH ROW EXECUTE FUNCTION reject_terminal_state_revival('RELEASED,REVOKED,EXPIRED');

ALTER TABLE attempts
    ADD CONSTRAINT attempts_lease_fk
    FOREIGN KEY (lease_id, id, package_id, revision_id, fencing_token)
    REFERENCES leases(id, attempt_id, package_id, revision_id, fencing_token)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE work_packages
    ADD CONSTRAINT work_packages_active_attempt_fk
    FOREIGN KEY (active_attempt_id, id) REFERENCES attempts(id, package_id);

COMMIT;
