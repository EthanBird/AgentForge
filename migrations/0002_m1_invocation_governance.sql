BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Policy identity is stable while every document revision is immutable.  The
-- mutable scope row is the CAS activation pointer; activation history is added
-- after GovernanceCase exists.
CREATE TABLE policy_scopes (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    scope_type text NOT NULL CHECK (scope_type IN (
        'PROJECT', 'PACKAGE', 'EXECUTOR', 'NODE'
    )),
    subject_scope_id uuid NOT NULL,
    category text NOT NULL CHECK (category IN (
        'ROUTING', 'BUDGET', 'GOVERNANCE', 'SECURITY', 'RETENTION'
    )),
    current_revision_id uuid,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, project_id),
    UNIQUE (project_id, scope_type, subject_scope_id, category),
    CHECK (
        (current_revision_id IS NULL AND version = 0)
        OR (current_revision_id IS NOT NULL AND version > 0)
    )
);

CREATE FUNCTION enforce_policy_scope_binding_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'policy scope cannot be deleted: %', OLD.id USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.project_id <> OLD.project_id
       OR NEW.scope_type <> OLD.scope_type
       OR NEW.subject_scope_id <> OLD.subject_scope_id
       OR NEW.category <> OLD.category
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'policy scope binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.current_revision_id IS NOT DISTINCT FROM OLD.current_revision_id
       AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'policy scope version changes only with activation: %', OLD.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER policy_scopes_binding_is_immutable
BEFORE UPDATE OR DELETE ON policy_scopes
FOR EACH ROW EXECUTE FUNCTION enforce_policy_scope_binding_immutability();

CREATE TABLE policy_revisions (
    id uuid PRIMARY KEY,
    policy_scope_id uuid NOT NULL,
    project_id uuid NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    state text NOT NULL CHECK (state IN ('DRAFT', 'STAGED', 'ACTIVE', 'SUPERSEDED')),
    schema_version text NOT NULL,
    canonical_document jsonb NOT NULL,
    document_digest bytea NOT NULL CHECK (octet_length(document_digest) = 32),
    base_revision_id uuid,
    created_by uuid NOT NULL,
    reason_code text NOT NULL CHECK (btrim(reason_code) <> ''),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_at timestamptz,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, policy_scope_id),
    UNIQUE (policy_scope_id, revision),
    UNIQUE (policy_scope_id, document_digest),
    FOREIGN KEY (policy_scope_id, project_id)
        REFERENCES policy_scopes(id, project_id),
    FOREIGN KEY (base_revision_id, project_id, policy_scope_id)
        REFERENCES policy_revisions(id, project_id, policy_scope_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (state IN ('DRAFT', 'STAGED') AND activated_at IS NULL)
        OR
        (state IN ('ACTIVE', 'SUPERSEDED') AND activated_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX policy_revisions_one_active_idx
    ON policy_revisions (policy_scope_id) WHERE state = 'ACTIVE';

ALTER TABLE policy_scopes
    ADD CONSTRAINT policy_scopes_current_revision_fk
    FOREIGN KEY (current_revision_id, project_id, id)
    REFERENCES policy_revisions(id, project_id, policy_scope_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION enforce_policy_revision_content_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'policy revision cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.policy_scope_id <> OLD.policy_scope_id
       OR NEW.project_id <> OLD.project_id
       OR NEW.revision <> OLD.revision
       OR NEW.schema_version <> OLD.schema_version
       OR NEW.canonical_document <> OLD.canonical_document
       OR NEW.document_digest <> OLD.document_digest
       OR NEW.base_revision_id IS DISTINCT FROM OLD.base_revision_id
       OR NEW.created_by <> OLD.created_by
       OR NEW.reason_code <> OLD.reason_code
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'policy revision content is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER policy_revision_content_is_immutable
BEFORE UPDATE OR DELETE ON policy_revisions
FOR EACH ROW EXECUTE FUNCTION enforce_policy_revision_content_immutability();

CREATE TABLE run_signals (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL CHECK (subject_type IN (
        'PROJECT', 'PACKAGE', 'ATTEMPT', 'BOSS_SESSION', 'VERIFICATION_JOB',
        'OBLIGATION', 'GOVERNANCE'
    )),
    subject_id uuid NOT NULL,
    package_id uuid,
    package_revision_id uuid,
    package_hash bytea CHECK (
        package_hash IS NULL OR octet_length(package_hash) = 32),
    attempt_id uuid,
    author_fencing_token bigint CHECK (
        author_fencing_token IS NULL OR author_fencing_token > 0),
    kind text NOT NULL CHECK (kind IN (
        'ASSIGNMENT_GRANTED', 'WAKE_CONDITION_SATISFIED', 'QUESTION_ANSWERED',
        'PERMISSION_DECIDED', 'ARTIFACT_AVAILABLE', 'SEMANTIC_DEADLINE_REACHED',
        'BUDGET_THRESHOLD_REACHED', 'NUDGE_REQUESTED', 'DIAGNOSE_REQUESTED',
        'POLICY_CHANGED', 'PACKAGE_REVISION_CHANGED', 'LEASE_GENERATION_CHANGED',
        'CANCEL_REQUESTED', 'SECURITY_TERMINATION', 'ROUTINE_DUE',
        'RECONCILE_REQUESTED'
    )),
    cause_event_id uuid,
    causation_id uuid,
    source_actor_id uuid NOT NULL,
    policy_revision_id uuid NOT NULL,
    binding jsonb NOT NULL,
    binding_digest bytea NOT NULL CHECK (octet_length(binding_digest) = 32),
    normalized_reason text NOT NULL CHECK (btrim(normalized_reason) <> ''),
    payload_digest bytea NOT NULL CHECK (octet_length(payload_digest) = 32),
    source_dedup_digest bytea NOT NULL CHECK (octet_length(source_dedup_digest) = 32),
    not_before timestamptz NOT NULL,
    deadline timestamptz,
    priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 100),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, binding_digest, policy_revision_id),
    UNIQUE (project_id, source_dedup_digest),
    UNIQUE (project_id, cause_event_id, kind, subject_type, subject_id,
            binding_digest, payload_digest),
    FOREIGN KEY (package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (package_revision_id, package_id, package_hash)
        REFERENCES package_revisions(id, package_id, package_hash),
    FOREIGN KEY (attempt_id, package_id, package_revision_id, author_fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token),
    FOREIGN KEY (policy_revision_id, project_id)
        REFERENCES policy_revisions(id, project_id),
    CHECK (deadline IS NULL OR deadline > not_before),
    CHECK ((package_revision_id IS NULL) = (package_hash IS NULL)),
    CHECK (subject_type <> 'PROJECT' OR subject_id = project_id),
    CHECK (subject_type <> 'PACKAGE' OR subject_id = package_id),
    CHECK (
        (subject_type = 'ATTEMPT'
         AND package_id IS NOT NULL
         AND package_revision_id IS NOT NULL
         AND package_hash IS NOT NULL
         AND attempt_id IS NOT NULL
         AND subject_id = attempt_id
         AND author_fencing_token IS NOT NULL)
        OR
        (subject_type <> 'ATTEMPT'
         AND attempt_id IS NULL
         AND author_fencing_token IS NULL)
    )
);

CREATE INDEX run_signals_subject_idx
    ON run_signals (project_id, subject_type, subject_id, not_before, id);
CREATE TRIGGER run_signals_are_immutable
BEFORE UPDATE OR DELETE ON run_signals
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE invocation_intents (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL CHECK (subject_type IN (
        'PROJECT', 'PACKAGE', 'ATTEMPT', 'BOSS_SESSION', 'VERIFICATION_JOB',
        'OBLIGATION', 'GOVERNANCE'
    )),
    subject_id uuid NOT NULL,
    package_id uuid,
    package_revision_id uuid,
    package_hash bytea CHECK (
        package_hash IS NULL OR octet_length(package_hash) = 32),
    attempt_id uuid,
    author_fencing_token bigint CHECK (
        author_fencing_token IS NULL OR author_fencing_token > 0),
    policy_revision_id uuid NOT NULL,
    binding jsonb NOT NULL,
    binding_digest bytea NOT NULL CHECK (octet_length(binding_digest) = 32),
    dedup_digest bytea NOT NULL CHECK (octet_length(dedup_digest) = 32),
    state text NOT NULL CHECK (state IN (
        'PENDING', 'CLAIMED', 'DISPATCHED', 'SATISFIED', 'CANCELLED', 'DEAD_LETTER'
    )),
    primary_signal_id uuid NOT NULL,
    signal_count integer NOT NULL DEFAULT 1 CHECK (signal_count > 0),
    priority smallint NOT NULL DEFAULT 50 CHECK (priority BETWEEN 0 AND 100),
    deadline timestamptz,
    claimed_by uuid,
    claim_token_hash bytea CHECK (
        claim_token_hash IS NULL OR octet_length(claim_token_hash) = 32),
    claim_until timestamptz,
    invocation_run_id uuid,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, binding_digest, policy_revision_id),
    UNIQUE (id, project_id, binding_digest, policy_revision_id, subject_type, subject_id),
    FOREIGN KEY (primary_signal_id, project_id, binding_digest, policy_revision_id)
        REFERENCES run_signals(id, project_id, binding_digest, policy_revision_id),
    FOREIGN KEY (package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (package_revision_id, package_id, package_hash)
        REFERENCES package_revisions(id, package_id, package_hash),
    FOREIGN KEY (attempt_id, package_id, package_revision_id, author_fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token),
    FOREIGN KEY (policy_revision_id, project_id)
        REFERENCES policy_revisions(id, project_id),
    CHECK ((claimed_by IS NULL) = (claim_token_hash IS NULL)),
    CHECK ((claimed_by IS NULL) = (claim_until IS NULL)),
    CHECK ((state = 'CLAIMED') = (claimed_by IS NOT NULL)),
    CHECK ((package_revision_id IS NULL) = (package_hash IS NULL)),
    CHECK (subject_type <> 'PROJECT' OR subject_id = project_id),
    CHECK (subject_type <> 'PACKAGE' OR subject_id = package_id),
    CHECK (
        (state IN ('DISPATCHED', 'SATISFIED') AND invocation_run_id IS NOT NULL)
        OR
        (state IN ('PENDING', 'CLAIMED', 'CANCELLED', 'DEAD_LETTER')
         AND invocation_run_id IS NULL)
    ),
    CHECK (
        (subject_type = 'ATTEMPT'
         AND package_id IS NOT NULL
         AND package_revision_id IS NOT NULL
         AND package_hash IS NOT NULL
         AND attempt_id IS NOT NULL
         AND subject_id = attempt_id
         AND author_fencing_token IS NOT NULL)
        OR
        (subject_type <> 'ATTEMPT'
         AND attempt_id IS NULL
         AND author_fencing_token IS NULL)
    )
);

CREATE UNIQUE INDEX invocation_intents_one_active_dedup_idx
    ON invocation_intents (project_id, dedup_digest)
    WHERE state IN ('PENDING', 'CLAIMED', 'DISPATCHED');
CREATE INDEX invocation_intents_ready_idx
    ON invocation_intents (priority DESC, created_at, id)
    WHERE state IN ('PENDING', 'CLAIMED');

CREATE FUNCTION enforce_invocation_intent_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'invocation intent cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.project_id <> OLD.project_id
       OR NEW.subject_type <> OLD.subject_type
       OR NEW.subject_id <> OLD.subject_id
       OR NEW.package_id IS DISTINCT FROM OLD.package_id
       OR NEW.package_revision_id IS DISTINCT FROM OLD.package_revision_id
       OR NEW.package_hash IS DISTINCT FROM OLD.package_hash
       OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
       OR NEW.author_fencing_token IS DISTINCT FROM OLD.author_fencing_token
       OR NEW.policy_revision_id <> OLD.policy_revision_id
       OR NEW.binding <> OLD.binding
       OR NEW.binding_digest <> OLD.binding_digest
       OR NEW.dedup_digest <> OLD.dedup_digest
       OR NEW.primary_signal_id <> OLD.primary_signal_id
       OR (OLD.invocation_run_id IS NOT NULL
           AND NEW.invocation_run_id IS DISTINCT FROM OLD.invocation_run_id) THEN
        RAISE EXCEPTION 'invocation intent binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state IN ('SATISFIED', 'CANCELLED', 'DEAD_LETTER')
       AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal invocation intent is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER invocation_intents_binding_is_immutable
BEFORE UPDATE OR DELETE ON invocation_intents
FOR EACH ROW EXECUTE FUNCTION enforce_invocation_intent_immutability();

CREATE TABLE invocation_intent_signals (
    intent_id uuid NOT NULL,
    signal_id uuid NOT NULL,
    project_id uuid NOT NULL,
    binding_digest bytea NOT NULL CHECK (octet_length(binding_digest) = 32),
    policy_revision_id uuid NOT NULL,
    attached_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (intent_id, signal_id),
    UNIQUE (signal_id),
    FOREIGN KEY (intent_id, project_id, binding_digest, policy_revision_id)
        REFERENCES invocation_intents(id, project_id, binding_digest, policy_revision_id),
    FOREIGN KEY (signal_id, project_id, binding_digest, policy_revision_id)
        REFERENCES run_signals(id, project_id, binding_digest, policy_revision_id)
);
CREATE TRIGGER invocation_intent_signals_are_immutable
BEFORE UPDATE OR DELETE ON invocation_intent_signals
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE FUNCTION check_invocation_intent_signal_set() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    checked_intent_id uuid;
    expected_count integer;
    primary_signal uuid;
    actual_count bigint;
BEGIN
    IF TG_TABLE_NAME = 'invocation_intents' THEN
        checked_intent_id := NEW.id;
    ELSE
        checked_intent_id := NEW.intent_id;
    END IF;
    SELECT signal_count, primary_signal_id
      INTO expected_count, primary_signal
      FROM invocation_intents
     WHERE id = checked_intent_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT count(*) INTO actual_count
      FROM invocation_intent_signals
     WHERE intent_id = checked_intent_id;
    IF actual_count <> expected_count OR NOT EXISTS (
        SELECT 1 FROM invocation_intent_signals
         WHERE intent_id = checked_intent_id AND signal_id = primary_signal
    ) THEN
        RAISE EXCEPTION 'intent % signal_count/primary mapping mismatch', checked_intent_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER invocation_intent_signal_set_is_consistent
AFTER INSERT OR UPDATE ON invocation_intents
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_invocation_intent_signal_set();

CREATE CONSTRAINT TRIGGER invocation_intent_signal_mapping_is_consistent
AFTER INSERT OR UPDATE ON invocation_intent_signals
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_invocation_intent_signal_set();

CREATE FUNCTION reject_noncoalescible_signal_attachment() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM invocation_intents i
          JOIN run_signals s ON s.id = NEW.signal_id
         WHERE i.id = NEW.intent_id
           AND i.project_id = s.project_id
           AND i.subject_type = s.subject_type
           AND i.subject_id = s.subject_id
    ) THEN
        RAISE EXCEPTION 'signal % subject does not match intent %',
            NEW.signal_id, NEW.intent_id USING ERRCODE = '23514';
    END IF;
    IF NEW.signal_id <> (
        SELECT primary_signal_id FROM invocation_intents WHERE id = NEW.intent_id
    ) AND EXISTS (
        SELECT 1 FROM run_signals
         WHERE id = NEW.signal_id
           AND kind IN (
             'PERMISSION_DECIDED', 'POLICY_CHANGED', 'PACKAGE_REVISION_CHANGED',
             'LEASE_GENERATION_CHANGED', 'CANCEL_REQUESTED', 'SECURITY_TERMINATION'
           )
    ) THEN
        RAISE EXCEPTION 'signal % must create a successor intent', NEW.signal_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER invocation_intent_signals_reject_noncoalescible
BEFORE INSERT ON invocation_intent_signals
FOR EACH ROW EXECUTE FUNCTION reject_noncoalescible_signal_attachment();

CREATE TABLE session_capsules (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL CHECK (subject_type IN (
        'ATTEMPT', 'BOSS_SESSION', 'VERIFICATION_JOB', 'OBLIGATION'
    )),
    subject_id uuid NOT NULL,
    package_id uuid,
    package_revision_id uuid,
    package_hash bytea CHECK (
        package_hash IS NULL OR octet_length(package_hash) = 32),
    attempt_id uuid,
    author_fencing_token bigint CHECK (
        author_fencing_token IS NULL OR author_fencing_token > 0),
    previous_capsule_id uuid,
    adapter_id text NOT NULL CHECK (btrim(adapter_id) <> ''),
    adapter_session_ref_ciphertext bytea,
    content_uri text NOT NULL CHECK (btrim(content_uri) <> ''),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    manifest jsonb NOT NULL,
    security_level text NOT NULL CHECK (security_level IN (
        'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    )),
    executor_fingerprint bytea NOT NULL CHECK (octet_length(executor_fingerprint) = 32),
    workspace_head text,
    created_by_run_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, content_digest),
    UNIQUE (id, project_id, content_digest, subject_type, subject_id),
    UNIQUE (id, project_id, subject_type, subject_id),
    UNIQUE (project_id, content_digest),
    FOREIGN KEY (previous_capsule_id, project_id, subject_type, subject_id)
        REFERENCES session_capsules(id, project_id, subject_type, subject_id),
    FOREIGN KEY (package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (package_revision_id, package_id, package_hash)
        REFERENCES package_revisions(id, package_id, package_hash),
    FOREIGN KEY (attempt_id, package_id, package_revision_id, author_fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token),
    CHECK ((package_revision_id IS NULL) = (package_hash IS NULL)),
    CHECK (
        (subject_type = 'ATTEMPT'
         AND package_id IS NOT NULL
         AND package_revision_id IS NOT NULL
         AND package_hash IS NOT NULL
         AND attempt_id IS NOT NULL
         AND subject_id = attempt_id
         AND author_fencing_token IS NOT NULL)
        OR
        (subject_type <> 'ATTEMPT'
         AND attempt_id IS NULL
         AND author_fencing_token IS NULL)
    )
);

CREATE TRIGGER session_capsules_are_immutable
BEFORE UPDATE OR DELETE ON session_capsules
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE budget_accounts (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    scope_type text NOT NULL CHECK (scope_type IN (
        'PROJECT', 'PACKAGE', 'ATTEMPT', 'VERIFICATION', 'INTEGRATION'
    )),
    scope_id uuid NOT NULL,
    category text NOT NULL CHECK (category IN (
        'AUTHOR_MODEL', 'AUTHOR_COMPUTE', 'RUNNER', 'REVIEWER', 'ARTIFACT',
        'INTEGRATION', 'RECOVERY'
    )),
    dimension text NOT NULL CHECK (dimension IN (
        'MICROCENTS', 'TOKENS', 'WALL_MILLIS', 'COMPUTE_MILLIS',
        'TRANSFER_BYTES', 'RETRY_COUNT', 'GIT_MUTATIONS', 'RISK_UNITS'
    )),
    limit_units bigint NOT NULL CHECK (limit_units >= 0),
    reserved_units bigint NOT NULL DEFAULT 0 CHECK (reserved_units >= 0),
    spent_units bigint NOT NULL DEFAULT 0 CHECK (spent_units >= 0),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, category),
    UNIQUE (project_id, scope_type, scope_id, category, dimension),
    CHECK (reserved_units + spent_units <= limit_units)
);

CREATE FUNCTION enforce_budget_account_binding_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'budget account cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.project_id <> OLD.project_id
       OR NEW.scope_type <> OLD.scope_type
       OR NEW.scope_id <> OLD.scope_id
       OR NEW.category <> OLD.category
       OR NEW.dimension <> OLD.dimension THEN
        RAISE EXCEPTION 'budget account scope/category is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER budget_accounts_binding_is_immutable
BEFORE UPDATE OR DELETE ON budget_accounts
FOR EACH ROW EXECUTE FUNCTION enforce_budget_account_binding_immutability();

CREATE TABLE budget_reservations (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    account_id uuid NOT NULL,
    parent_reservation_id uuid,
    intent_id uuid,
    purpose_type text NOT NULL CHECK (purpose_type IN (
        'ATTEMPT', 'INVOCATION_RUN', 'VERIFICATION_RUN', 'ARTIFACT', 'INTEGRATION'
    )),
    purpose_id uuid NOT NULL,
    amount_units bigint NOT NULL CHECK (amount_units > 0),
    allocated_units bigint NOT NULL DEFAULT 0 CHECK (allocated_units >= 0),
    spent_units bigint NOT NULL DEFAULT 0 CHECK (spent_units >= 0),
    state text NOT NULL CHECK (state IN (
        'ACTIVE', 'SETTLED', 'RELEASED', 'EXPIRED', 'CANCELLED'
    )),
    expires_at timestamptz,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminal_at timestamptz,
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, account_id),
    UNIQUE (id, project_id, intent_id),
    UNIQUE (purpose_type, purpose_id, account_id),
    FOREIGN KEY (account_id, project_id)
        REFERENCES budget_accounts(id, project_id),
    FOREIGN KEY (parent_reservation_id, project_id, account_id)
        REFERENCES budget_reservations(id, project_id, account_id),
    FOREIGN KEY (intent_id, project_id)
        REFERENCES invocation_intents(id, project_id),
    CHECK (parent_reservation_id IS DISTINCT FROM id),
    CHECK (allocated_units + spent_units <= amount_units),
    CHECK ((state = 'ACTIVE') = (terminal_at IS NULL))
);

CREATE INDEX budget_reservations_expiry_idx
    ON budget_reservations (expires_at, id)
    WHERE state = 'ACTIVE' AND expires_at IS NOT NULL;
CREATE UNIQUE INDEX budget_reservations_one_active_intent_idx
    ON budget_reservations (intent_id)
    WHERE state = 'ACTIVE' AND intent_id IS NOT NULL;

CREATE FUNCTION enforce_budget_reservation_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'budget reservation cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.project_id <> OLD.project_id
       OR NEW.account_id <> OLD.account_id
       OR NEW.parent_reservation_id IS DISTINCT FROM OLD.parent_reservation_id
       OR NEW.intent_id IS DISTINCT FROM OLD.intent_id
       OR NEW.purpose_type <> OLD.purpose_type
       OR NEW.purpose_id <> OLD.purpose_id
       OR NEW.amount_units <> OLD.amount_units
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'budget reservation binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state <> 'ACTIVE' AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal budget reservation is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER budget_reservations_binding_is_immutable
BEFORE UPDATE OR DELETE ON budget_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_budget_reservation_immutability();

CREATE TABLE invocation_runs (
    id uuid PRIMARY KEY,
    intent_id uuid NOT NULL UNIQUE,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL CHECK (subject_type IN (
        'PROJECT', 'PACKAGE', 'ATTEMPT', 'BOSS_SESSION', 'VERIFICATION_JOB',
        'OBLIGATION', 'GOVERNANCE'
    )),
    subject_id uuid NOT NULL,
    package_id uuid,
    package_revision_id uuid,
    package_hash bytea CHECK (
        package_hash IS NULL OR octet_length(package_hash) = 32),
    attempt_id uuid,
    author_fencing_token bigint CHECK (
        author_fencing_token IS NULL OR author_fencing_token > 0),
    policy_revision_id uuid NOT NULL,
    binding jsonb NOT NULL,
    binding_digest bytea NOT NULL CHECK (octet_length(binding_digest) = 32),
    adapter_id text NOT NULL CHECK (btrim(adapter_id) <> ''),
    executor_id uuid NOT NULL,
    executor_fingerprint bytea NOT NULL CHECK (octet_length(executor_fingerprint) = 32),
    routing_decision_id uuid,
    input_capsule_id uuid,
    input_capsule_digest bytea CHECK (
        input_capsule_digest IS NULL OR octet_length(input_capsule_digest) = 32),
    budget_reservation_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN (
        'RESERVED', 'STARTING', 'RUNNING', 'RECONCILING',
        'COMPLETED', 'FAILED', 'CANCELLED'
    )),
    current_claim_generation bigint NOT NULL DEFAULT 0 CHECK (current_claim_generation >= 0),
    external_invocation_key text NOT NULL UNIQUE CHECK (btrim(external_invocation_key) <> ''),
    output_capsule_id uuid,
    output_capsule_digest bytea CHECK (
        output_capsule_digest IS NULL OR octet_length(output_capsule_digest) = 32),
    outcome text CHECK (outcome IN (
        'PROGRESSED', 'WAITING_INPUT', 'CANDIDATE_PROPOSED', 'PLAN_PROPOSED',
        'DECISION_REQUESTED', 'NO_PROGRESS', 'INFRASTRUCTURE_FAILURE',
        'OUTCOME_UNKNOWN', 'CANCELLED'
    )),
    outcome_digest bytea CHECK (
        outcome_digest IS NULL OR octet_length(outcome_digest) = 32),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminalized_at timestamptz,
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, subject_type, subject_id),
    UNIQUE (id, intent_id),
    FOREIGN KEY (
        intent_id, project_id, binding_digest, policy_revision_id, subject_type, subject_id
    ) REFERENCES invocation_intents(
        id, project_id, binding_digest, policy_revision_id, subject_type, subject_id
    ),
    FOREIGN KEY (package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (package_revision_id, package_id, package_hash)
        REFERENCES package_revisions(id, package_id, package_hash),
    FOREIGN KEY (attempt_id, package_id, package_revision_id, author_fencing_token)
        REFERENCES attempts(id, package_id, revision_id, fencing_token),
    FOREIGN KEY (policy_revision_id, project_id)
        REFERENCES policy_revisions(id, project_id),
    FOREIGN KEY (
        input_capsule_id, project_id, input_capsule_digest, subject_type, subject_id
    ) REFERENCES session_capsules(
        id, project_id, content_digest, subject_type, subject_id
    ),
    FOREIGN KEY (budget_reservation_id, project_id, intent_id)
        REFERENCES budget_reservations(id, project_id, intent_id),
    FOREIGN KEY (
        output_capsule_id, project_id, output_capsule_digest, subject_type, subject_id
    ) REFERENCES session_capsules(
        id, project_id, content_digest, subject_type, subject_id
    )
        DEFERRABLE INITIALLY DEFERRED,
    CHECK ((package_revision_id IS NULL) = (package_hash IS NULL)),
    CHECK (subject_type <> 'PROJECT' OR subject_id = project_id),
    CHECK (subject_type <> 'PACKAGE' OR subject_id = package_id),
    CHECK ((input_capsule_id IS NULL) = (input_capsule_digest IS NULL)),
    CHECK ((output_capsule_id IS NULL) = (output_capsule_digest IS NULL)),
    CHECK ((state IN ('COMPLETED', 'FAILED', 'CANCELLED')) =
           (terminalized_at IS NOT NULL)),
    CHECK ((state IN ('COMPLETED', 'FAILED', 'CANCELLED')) =
           (outcome IS NOT NULL AND outcome_digest IS NOT NULL)),
    CHECK (state <> 'COMPLETED' OR outcome IN (
        'PROGRESSED', 'WAITING_INPUT', 'CANDIDATE_PROPOSED', 'PLAN_PROPOSED',
        'DECISION_REQUESTED', 'NO_PROGRESS'
    )),
    CHECK (state <> 'FAILED' OR outcome IN (
        'INFRASTRUCTURE_FAILURE', 'OUTCOME_UNKNOWN'
    )),
    CHECK (state <> 'CANCELLED' OR outcome = 'CANCELLED'),
    CHECK (
        (subject_type = 'ATTEMPT'
         AND package_id IS NOT NULL
         AND package_revision_id IS NOT NULL
         AND package_hash IS NOT NULL
         AND attempt_id IS NOT NULL
         AND subject_id = attempt_id
         AND author_fencing_token IS NOT NULL)
        OR
        (subject_type <> 'ATTEMPT'
         AND attempt_id IS NULL
         AND author_fencing_token IS NULL)
    )
);

CREATE INDEX invocation_runs_active_idx
    ON invocation_runs (created_at, id)
    WHERE state IN ('RESERVED', 'STARTING', 'RUNNING', 'RECONCILING');

CREATE FUNCTION enforce_invocation_run_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'invocation run cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.intent_id <> OLD.intent_id
       OR NEW.project_id <> OLD.project_id
       OR NEW.subject_type <> OLD.subject_type
       OR NEW.subject_id <> OLD.subject_id
       OR NEW.package_id IS DISTINCT FROM OLD.package_id
       OR NEW.package_revision_id IS DISTINCT FROM OLD.package_revision_id
       OR NEW.package_hash IS DISTINCT FROM OLD.package_hash
       OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
       OR NEW.author_fencing_token IS DISTINCT FROM OLD.author_fencing_token
       OR NEW.policy_revision_id <> OLD.policy_revision_id
       OR NEW.binding <> OLD.binding
       OR NEW.binding_digest <> OLD.binding_digest
       OR NEW.adapter_id <> OLD.adapter_id
       OR NEW.executor_id <> OLD.executor_id
       OR NEW.executor_fingerprint <> OLD.executor_fingerprint
       OR NEW.routing_decision_id IS DISTINCT FROM OLD.routing_decision_id
       OR NEW.input_capsule_id IS DISTINCT FROM OLD.input_capsule_id
       OR NEW.input_capsule_digest IS DISTINCT FROM OLD.input_capsule_digest
       OR NEW.budget_reservation_id <> OLD.budget_reservation_id
       OR NEW.external_invocation_key <> OLD.external_invocation_key
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'invocation run binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.current_claim_generation < OLD.current_claim_generation
       OR NEW.current_claim_generation > OLD.current_claim_generation + 1 THEN
        RAISE EXCEPTION 'run claim generation must advance exactly once: %', OLD.id
            USING ERRCODE = '23514';
    END IF;
    IF OLD.state IN ('COMPLETED', 'FAILED', 'CANCELLED')
       AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal invocation run is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER invocation_runs_binding_is_immutable
BEFORE UPDATE OR DELETE ON invocation_runs
FOR EACH ROW EXECUTE FUNCTION enforce_invocation_run_immutability();

ALTER TABLE invocation_intents
    ADD CONSTRAINT invocation_intents_run_fk
    FOREIGN KEY (invocation_run_id, id)
    REFERENCES invocation_runs(id, intent_id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE session_capsules
    ADD CONSTRAINT session_capsules_created_by_run_fk
    FOREIGN KEY (created_by_run_id, project_id, subject_type, subject_id)
    REFERENCES invocation_runs(id, project_id, subject_type, subject_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE run_claims (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    project_id uuid NOT NULL,
    claim_request_id uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    holder_id uuid NOT NULL,
    token_hash bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    state text NOT NULL CHECK (state IN (
        'ACTIVE', 'COMPLETED', 'EXPIRED', 'REVOKED', 'SUPERSEDED'
    )),
    expires_at timestamptz NOT NULL,
    result_digest bytea CHECK (
        result_digest IS NULL OR octet_length(result_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (run_id, claim_request_id),
    UNIQUE (run_id, generation),
    UNIQUE (run_id, project_id, generation),
    FOREIGN KEY (run_id, project_id)
        REFERENCES invocation_runs(id, project_id),
    CHECK ((state = 'ACTIVE') = (completed_at IS NULL)),
    CHECK ((state = 'COMPLETED') = (result_digest IS NOT NULL)),
    CHECK (expires_at > created_at)
);

CREATE UNIQUE INDEX run_claims_one_active_idx
    ON run_claims (run_id) WHERE state = 'ACTIVE';
CREATE INDEX run_claims_expiry_idx
    ON run_claims (expires_at, id) WHERE state = 'ACTIVE';

CREATE FUNCTION enforce_run_claim_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'run claim cannot be deleted: %', OLD.id USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.run_id <> OLD.run_id
       OR NEW.project_id <> OLD.project_id
       OR NEW.claim_request_id <> OLD.claim_request_id
       OR NEW.generation <> OLD.generation
       OR NEW.holder_id <> OLD.holder_id
       OR NEW.token_hash <> OLD.token_hash
       OR NEW.expires_at <> OLD.expires_at
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'run claim binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state <> 'ACTIVE' AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal run claim is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER run_claims_binding_is_immutable
BEFORE UPDATE OR DELETE ON run_claims
FOR EACH ROW EXECUTE FUNCTION enforce_run_claim_immutability();

CREATE FUNCTION check_active_run_claim_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'ACTIVE' AND NOT EXISTS (
        SELECT 1 FROM invocation_runs r
        WHERE r.id = NEW.run_id
          AND r.project_id = NEW.project_id
          AND r.current_claim_generation = NEW.generation
          AND r.state IN ('RESERVED', 'STARTING', 'RUNNING', 'RECONCILING')
    ) THEN
        RAISE EXCEPTION 'active run claim is not current for run % generation %',
            NEW.run_id, NEW.generation USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER run_claim_generation_is_current
AFTER INSERT OR UPDATE ON run_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_active_run_claim_generation();

CREATE FUNCTION check_invocation_run_claim_and_budget() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.current_claim_generation > 0 AND NOT EXISTS (
        SELECT 1 FROM run_claims c
         WHERE c.run_id = NEW.id
           AND c.project_id = NEW.project_id
           AND c.generation = NEW.current_claim_generation
    ) THEN
        RAISE EXCEPTION 'run % current claim generation % has no claim',
            NEW.id, NEW.current_claim_generation USING ERRCODE = '23514';
    END IF;
    IF NEW.state IN ('STARTING', 'RUNNING') AND NOT EXISTS (
        SELECT 1 FROM run_claims c
         WHERE c.run_id = NEW.id
           AND c.project_id = NEW.project_id
           AND c.generation = NEW.current_claim_generation
           AND c.state = 'ACTIVE'
           AND c.expires_at > clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'live run % lacks an unexpired current active claim', NEW.id
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state IN ('COMPLETED', 'FAILED', 'CANCELLED') AND EXISTS (
        SELECT 1 FROM run_claims c
         WHERE c.run_id = NEW.id AND c.state = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION 'terminal run % still has an active claim', NEW.id
            USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM budget_reservations b
         WHERE b.id = NEW.budget_reservation_id
           AND b.project_id = NEW.project_id
           AND b.intent_id = NEW.intent_id
           AND b.purpose_type = 'INVOCATION_RUN'
           AND b.purpose_id = NEW.id
           AND (
             b.state = 'ACTIVE'
             OR NEW.state IN ('COMPLETED', 'FAILED', 'CANCELLED')
           )
    ) THEN
        RAISE EXCEPTION 'run % budget reservation binding/state mismatch', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER invocation_run_claim_and_budget_are_consistent
AFTER INSERT OR UPDATE ON invocation_runs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_invocation_run_claim_and_budget();

CREATE FUNCTION check_budget_reservation_has_no_live_run() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state <> 'ACTIVE' AND EXISTS (
        SELECT 1 FROM invocation_runs r
         WHERE r.budget_reservation_id = NEW.id
           AND r.state IN ('RESERVED', 'STARTING', 'RUNNING', 'RECONCILING')
    ) THEN
        RAISE EXCEPTION 'reservation % terminalized while invocation run is live', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER budget_reservation_live_run_is_consistent
AFTER UPDATE ON budget_reservations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_budget_reservation_has_no_live_run();

CREATE TABLE governance_cases (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    kind text NOT NULL CHECK (kind IN (
        'PERMISSION_REQUEST', 'BUDGET_CHANGE', 'PLAN_PATCH_APPROVAL',
        'POLICY_ACTIVATION', 'ROUTING_EXCEPTION', 'SECURITY_RESPONSE',
        'CONFLICT_RESOLUTION', 'OUTCOME_UNKNOWN', 'MANUAL_INTEGRATION',
        'OPERATIONAL_INTERVENTION'
    )),
    risk text NOT NULL CHECK (risk IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    subject_version bigint NOT NULL CHECK (subject_version > 0),
    authorization_epoch bigint NOT NULL DEFAULT 1 CHECK (authorization_epoch > 0),
    requested_by uuid NOT NULL,
    author_actor_id uuid,
    package_id uuid,
    package_revision_id uuid,
    package_hash bytea CHECK (package_hash IS NULL OR octet_length(package_hash) = 32),
    attempt_id uuid,
    attempt_lease_id uuid,
    author_fencing_token bigint CHECK (
        author_fencing_token IS NULL OR author_fencing_token > 0),
    invocation_run_id uuid,
    invocation_claim_generation bigint CHECK (
        invocation_claim_generation IS NULL OR invocation_claim_generation > 0),
    policy_revision_id uuid NOT NULL,
    normalized_action jsonb NOT NULL,
    action_digest bytea NOT NULL CHECK (octet_length(action_digest) = 32),
    resource_snapshot_digest bytea NOT NULL CHECK (
        octet_length(resource_snapshot_digest) = 32),
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    required_quorum jsonb NOT NULL,
    due_at timestamptz,
    expires_at timestamptz NOT NULL,
    timeout_behavior text NOT NULL CHECK (timeout_behavior IN (
        'DENY', 'DEFER', 'CANCEL', 'SUPERSEDE'
    )),
    state text NOT NULL CHECK (state IN (
        'NEEDS_DECISION', 'QUORUM_REACHED', 'EXECUTING', 'RECONCILING',
        'APPLIED', 'DENIED', 'CHANGES_REQUESTED', 'DEFERRED', 'EXPIRED',
        'CANCELLED', 'SUPERSEDED'
    )),
    execution_claim_id uuid,
    execution_claim_generation bigint CHECK (
        execution_claim_generation IS NULL OR execution_claim_generation > 0),
    execution_receipt_id uuid,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminalized_at timestamptz,
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, action_digest, policy_revision_id),
    UNIQUE (
        id, project_id, action_digest, policy_revision_id, authorization_epoch
    ),
    UNIQUE (
        id, project_id, action_digest, policy_revision_id,
        authorization_epoch, subject_version
    ),
    FOREIGN KEY (package_id, project_id)
        REFERENCES work_packages(id, project_id),
    FOREIGN KEY (package_revision_id, package_id, package_hash)
        REFERENCES package_revisions(id, package_id, package_hash),
    FOREIGN KEY (
        attempt_lease_id, attempt_id, package_id, package_revision_id,
        author_fencing_token
    ) REFERENCES leases(id, attempt_id, package_id, revision_id, fencing_token),
    FOREIGN KEY (invocation_run_id, project_id)
        REFERENCES invocation_runs(id, project_id),
    FOREIGN KEY (invocation_run_id, project_id, invocation_claim_generation)
        REFERENCES run_claims(run_id, project_id, generation),
    FOREIGN KEY (policy_revision_id, project_id)
        REFERENCES policy_revisions(id, project_id),
    CHECK (expires_at > created_at),
    CHECK (due_at IS NULL OR (due_at >= created_at AND due_at <= expires_at)),
    CHECK ((state IN (
        'APPLIED', 'DENIED', 'CHANGES_REQUESTED', 'EXPIRED', 'CANCELLED', 'SUPERSEDED'
    )) = (terminalized_at IS NOT NULL)),
    CHECK (state <> 'APPLIED' OR execution_receipt_id IS NOT NULL),
    CHECK (jsonb_typeof(normalized_action) = 'object'),
    CHECK (jsonb_typeof(evidence_refs) = 'array'),
    CHECK (jsonb_typeof(required_quorum) = 'object'),
    CHECK (
        (package_id IS NULL
         AND package_revision_id IS NULL
         AND package_hash IS NULL)
        OR
        (package_id IS NOT NULL
         AND package_revision_id IS NOT NULL
         AND package_hash IS NOT NULL)
    ),
    CHECK (
        (attempt_id IS NULL
         AND attempt_lease_id IS NULL
         AND author_fencing_token IS NULL)
        OR
        (package_id IS NOT NULL
         AND package_revision_id IS NOT NULL
         AND attempt_id IS NOT NULL
         AND attempt_lease_id IS NOT NULL
         AND author_fencing_token IS NOT NULL)
    ),
    CHECK ((invocation_run_id IS NULL) = (invocation_claim_generation IS NULL)),
    CHECK ((execution_claim_id IS NULL) = (execution_claim_generation IS NULL)),
    CHECK (state NOT IN ('EXECUTING', 'RECONCILING', 'APPLIED')
           OR execution_claim_id IS NOT NULL)
);

CREATE INDEX governance_cases_inbox_idx
    ON governance_cases (project_id, state, risk, due_at, id)
    WHERE state IN (
        'NEEDS_DECISION', 'QUORUM_REACHED', 'EXECUTING', 'RECONCILING', 'DEFERRED'
    );

CREATE FUNCTION enforce_governance_case_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'governance case cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.project_id <> OLD.project_id
       OR NEW.kind <> OLD.kind
       OR NEW.risk <> OLD.risk
       OR NEW.subject_type <> OLD.subject_type
       OR NEW.subject_id <> OLD.subject_id
       OR NEW.subject_version <> OLD.subject_version
       OR NEW.authorization_epoch <> OLD.authorization_epoch
       OR NEW.requested_by <> OLD.requested_by
       OR NEW.author_actor_id IS DISTINCT FROM OLD.author_actor_id
       OR NEW.package_id IS DISTINCT FROM OLD.package_id
       OR NEW.package_revision_id IS DISTINCT FROM OLD.package_revision_id
       OR NEW.package_hash IS DISTINCT FROM OLD.package_hash
       OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
       OR NEW.attempt_lease_id IS DISTINCT FROM OLD.attempt_lease_id
       OR NEW.author_fencing_token IS DISTINCT FROM OLD.author_fencing_token
       OR NEW.invocation_run_id IS DISTINCT FROM OLD.invocation_run_id
       OR NEW.invocation_claim_generation IS DISTINCT FROM OLD.invocation_claim_generation
       OR NEW.policy_revision_id <> OLD.policy_revision_id
       OR NEW.normalized_action <> OLD.normalized_action
       OR NEW.action_digest <> OLD.action_digest
       OR NEW.resource_snapshot_digest <> OLD.resource_snapshot_digest
       OR NEW.required_quorum <> OLD.required_quorum
       OR NEW.expires_at <> OLD.expires_at
       OR NEW.timeout_behavior <> OLD.timeout_behavior
       OR NEW.created_at <> OLD.created_at
       OR (OLD.execution_claim_id IS NOT NULL
           AND NEW.execution_claim_id IS DISTINCT FROM OLD.execution_claim_id)
       OR (OLD.execution_claim_generation IS NOT NULL
           AND NEW.execution_claim_generation IS DISTINCT FROM OLD.execution_claim_generation)
       OR (OLD.execution_receipt_id IS NOT NULL
           AND NEW.execution_receipt_id IS DISTINCT FROM OLD.execution_receipt_id) THEN
        RAISE EXCEPTION 'governance case action binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state IN (
        'APPLIED', 'DENIED', 'CHANGES_REQUESTED', 'EXPIRED', 'CANCELLED', 'SUPERSEDED'
    ) AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal governance case is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.execution_claim_id IS DISTINCT FROM OLD.execution_claim_id
       OR NEW.execution_claim_generation IS DISTINCT FROM OLD.execution_claim_generation THEN
        IF OLD.execution_claim_id IS NOT NULL
           OR OLD.execution_claim_generation IS NOT NULL
           OR OLD.state <> 'QUORUM_REACHED'
           OR NEW.state <> 'EXECUTING'
           OR NEW.execution_claim_id IS NULL
           OR NEW.execution_claim_generation IS NULL
           OR NEW.version <> OLD.version + 1 THEN
            RAISE EXCEPTION 'governance execution start CAS failed for case %', OLD.id
                USING ERRCODE = '40001';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER governance_cases_action_is_immutable
BEFORE UPDATE OR DELETE ON governance_cases
FOR EACH ROW EXECUTE FUNCTION enforce_governance_case_immutability();

CREATE TABLE operator_notes (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    author_id uuid NOT NULL,
    body_artifact_uri text NOT NULL CHECK (btrim(body_artifact_uri) <> ''),
    body_digest bytea NOT NULL CHECK (octet_length(body_digest) = 32),
    mentions jsonb NOT NULL DEFAULT '[]'::jsonb,
    supersedes_note_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, subject_type, subject_id),
    FOREIGN KEY (supersedes_note_id, project_id, subject_type, subject_id)
        REFERENCES operator_notes(id, project_id, subject_type, subject_id),
    CHECK (jsonb_typeof(mentions) = 'array')
);

CREATE TRIGGER operator_notes_are_immutable
BEFORE UPDATE OR DELETE ON operator_notes
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE governance_decisions (
    id uuid PRIMARY KEY,
    case_id uuid NOT NULL,
    project_id uuid NOT NULL,
    case_version bigint NOT NULL CHECK (case_version > 0),
    authorization_epoch bigint NOT NULL CHECK (authorization_epoch > 0),
    actor_id uuid NOT NULL,
    actor_role_snapshot_digest bytea NOT NULL CHECK (
        octet_length(actor_role_snapshot_digest) = 32),
    conclusion text NOT NULL CHECK (conclusion IN (
        'APPROVE', 'DENY', 'REQUEST_CHANGES', 'DEFER'
    )),
    action_digest bytea NOT NULL CHECK (octet_length(action_digest) = 32),
    policy_revision_id uuid NOT NULL,
    rationale_code text NOT NULL CHECK (btrim(rationale_code) <> ''),
    note_id uuid,
    idempotency_key_hash bytea NOT NULL CHECK (
        octet_length(idempotency_key_hash) = 32),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    signature bytea NOT NULL,
    UNIQUE (id, project_id),
    UNIQUE (case_id, actor_id, idempotency_key_hash),
    FOREIGN KEY (
        case_id, project_id, action_digest, policy_revision_id, authorization_epoch
    ) REFERENCES governance_cases(
        id, project_id, action_digest, policy_revision_id, authorization_epoch
    ),
    FOREIGN KEY (note_id, project_id)
        REFERENCES operator_notes(id, project_id),
    CHECK (expires_at > decided_at)
);

CREATE TRIGGER governance_decisions_are_immutable
BEFORE UPDATE OR DELETE ON governance_decisions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE governance_execution_claims (
    id uuid PRIMARY KEY,
    case_id uuid NOT NULL,
    project_id uuid NOT NULL,
    action_digest bytea NOT NULL CHECK (octet_length(action_digest) = 32),
    policy_revision_id uuid NOT NULL,
    authorization_epoch bigint NOT NULL CHECK (authorization_epoch > 0),
    generation bigint NOT NULL CHECK (generation > 0),
    holder_actor_id uuid NOT NULL,
    token_hash bytea NOT NULL CHECK (
        octet_length(token_hash) = 32
        AND token_hash <> decode(repeat('00', 32), 'hex')),
    authorization_digest bytea NOT NULL CHECK (
        octet_length(authorization_digest) = 32
        AND authorization_digest <> decode(repeat('00', 32), 'hex')),
    observed_case_version bigint NOT NULL CHECK (observed_case_version > 0),
    observed_subject_version bigint NOT NULL CHECK (observed_subject_version > 0),
    observed_attempt_id uuid,
    observed_attempt_lease_id uuid,
    observed_author_fencing_token bigint CHECK (
        observed_author_fencing_token IS NULL OR observed_author_fencing_token > 0),
    observed_invocation_claim_generation bigint CHECK (
        observed_invocation_claim_generation IS NULL
        OR observed_invocation_claim_generation > 0),
    state text NOT NULL CHECK (state IN (
        'ACTIVE', 'COMPLETED', 'EXPIRED', 'REVOKED', 'SUPERSEDED'
    )),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    completed_at timestamptz,
    UNIQUE (case_id, generation),
    UNIQUE (case_id, project_id, generation, action_digest),
    UNIQUE (id, case_id, project_id, generation, action_digest),
    UNIQUE (
        id, case_id, project_id, generation, action_digest, holder_actor_id
    ),
    FOREIGN KEY (
        case_id, project_id, action_digest, policy_revision_id,
        authorization_epoch, observed_subject_version
    ) REFERENCES governance_cases(
        id, project_id, action_digest, policy_revision_id,
        authorization_epoch, subject_version
    ),
    CHECK (issued_at < expires_at),
    CHECK ((state = 'ACTIVE') = (completed_at IS NULL)),
    CHECK (
        (observed_attempt_id IS NULL
         AND observed_attempt_lease_id IS NULL
         AND observed_author_fencing_token IS NULL)
        OR
        (observed_attempt_id IS NOT NULL
         AND observed_attempt_lease_id IS NOT NULL
         AND observed_author_fencing_token IS NOT NULL)
    )
);

CREATE UNIQUE INDEX governance_execution_claims_one_active_idx
    ON governance_execution_claims (case_id) WHERE state = 'ACTIVE';

CREATE FUNCTION enforce_governance_execution_claim_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'governance execution claim cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.case_id <> OLD.case_id
       OR NEW.project_id <> OLD.project_id
       OR NEW.action_digest <> OLD.action_digest
       OR NEW.policy_revision_id <> OLD.policy_revision_id
       OR NEW.authorization_epoch <> OLD.authorization_epoch
       OR NEW.generation <> OLD.generation
       OR NEW.holder_actor_id <> OLD.holder_actor_id
       OR NEW.token_hash <> OLD.token_hash
       OR NEW.authorization_digest <> OLD.authorization_digest
       OR NEW.observed_case_version <> OLD.observed_case_version
       OR NEW.observed_subject_version <> OLD.observed_subject_version
       OR NEW.observed_attempt_id IS DISTINCT FROM OLD.observed_attempt_id
       OR NEW.observed_attempt_lease_id IS DISTINCT FROM OLD.observed_attempt_lease_id
       OR NEW.observed_author_fencing_token
            IS DISTINCT FROM OLD.observed_author_fencing_token
       OR NEW.observed_invocation_claim_generation
            IS DISTINCT FROM OLD.observed_invocation_claim_generation
       OR NEW.issued_at <> OLD.issued_at
       OR NEW.expires_at <> OLD.expires_at THEN
        RAISE EXCEPTION 'governance execution claim binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state <> 'ACTIVE' AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal governance execution claim is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER governance_execution_claims_binding_is_immutable
BEFORE UPDATE OR DELETE ON governance_execution_claims
FOR EACH ROW EXECUTE FUNCTION enforce_governance_execution_claim_immutability();

CREATE FUNCTION check_governance_execution_start() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'ACTIVE' AND NOT EXISTS (
        SELECT 1
          FROM governance_cases g
         WHERE g.id = NEW.case_id
           AND g.project_id = NEW.project_id
           AND g.state = 'EXECUTING'
           AND g.execution_claim_id = NEW.id
           AND g.execution_claim_generation = NEW.generation
           AND g.version = NEW.observed_case_version + 1
           AND g.subject_version = NEW.observed_subject_version
           AND g.authorization_epoch = NEW.authorization_epoch
           AND g.policy_revision_id = NEW.policy_revision_id
           AND g.action_digest = NEW.action_digest
           AND g.attempt_id IS NOT DISTINCT FROM NEW.observed_attempt_id
           AND g.attempt_lease_id IS NOT DISTINCT FROM NEW.observed_attempt_lease_id
           AND g.author_fencing_token
                IS NOT DISTINCT FROM NEW.observed_author_fencing_token
           AND g.invocation_claim_generation
                IS NOT DISTINCT FROM NEW.observed_invocation_claim_generation
           AND g.expires_at > NEW.issued_at
           AND (
             g.attempt_id IS NULL
             OR EXISTS (
                 SELECT 1
                   FROM attempts a
                   JOIN leases l
                     ON l.id = a.lease_id
                    AND l.attempt_id = a.id
                    AND l.package_id = a.package_id
                    AND l.revision_id = a.revision_id
                    AND l.fencing_token = a.fencing_token
                  WHERE a.id = g.attempt_id
                    AND l.id = g.attempt_lease_id
                    AND l.fencing_token = g.author_fencing_token
                    AND l.state = 'ACTIVE'
                    AND l.expires_at > NEW.issued_at
             )
           )
           AND (
             g.invocation_run_id IS NULL
             OR EXISTS (
                 SELECT 1
                   FROM invocation_runs r
                   JOIN run_claims c
                     ON c.run_id = r.id
                    AND c.project_id = r.project_id
                    AND c.generation = r.current_claim_generation
                  WHERE r.id = g.invocation_run_id
                    AND r.project_id = g.project_id
                    AND r.current_claim_generation = g.invocation_claim_generation
                    AND c.state = 'ACTIVE'
                    AND c.expires_at > NEW.issued_at
             )
           )
    ) THEN
        RAISE EXCEPTION 'governance execution claim % has stale observed bindings', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER governance_execution_start_is_current
AFTER INSERT OR UPDATE ON governance_execution_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_governance_execution_start();

CREATE TABLE governance_execution_receipts (
    id uuid PRIMARY KEY,
    case_id uuid NOT NULL,
    project_id uuid NOT NULL,
    action_digest bytea NOT NULL CHECK (octet_length(action_digest) = 32),
    execution_claim_id uuid NOT NULL,
    execution_claim_generation bigint NOT NULL CHECK (execution_claim_generation > 0),
    executor_actor_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN (
        'SUCCEEDED', 'FAILED', 'OUTCOME_UNKNOWN'
    )),
    external_effect_key text CHECK (
        external_effect_key IS NULL OR btrim(external_effect_key) <> ''),
    effect_digest bytea NOT NULL CHECK (octet_length(effect_digest) = 32),
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    started_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        id, case_id, project_id, action_digest,
        execution_claim_id, execution_claim_generation
    ),
    UNIQUE (case_id, execution_claim_generation, effect_digest),
    FOREIGN KEY (
        execution_claim_id, case_id, project_id, execution_claim_generation,
        action_digest, executor_actor_id
    )
        REFERENCES governance_execution_claims(
            id, case_id, project_id, generation, action_digest, holder_actor_id
        ),
    CHECK (jsonb_typeof(evidence_refs) = 'array'),
    CHECK (observed_at >= started_at),
    CHECK (
        status <> 'SUCCEEDED'
        OR (
            external_effect_key IS NOT NULL
            AND btrim(external_effect_key) <> ''
            AND effect_digest <> decode(repeat('00', 32), 'hex')
            AND jsonb_array_length(evidence_refs) > 0
        )
    )
);

CREATE TRIGGER governance_execution_receipts_are_immutable
BEFORE UPDATE OR DELETE ON governance_execution_receipts
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE FUNCTION check_governance_execution_receipt_window() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM governance_execution_claims c
         WHERE c.id = NEW.execution_claim_id
           AND c.case_id = NEW.case_id
           AND c.project_id = NEW.project_id
           AND c.generation = NEW.execution_claim_generation
           AND c.action_digest = NEW.action_digest
           AND c.holder_actor_id = NEW.executor_actor_id
           AND NEW.started_at >= c.issued_at
           AND NEW.observed_at < c.expires_at
    ) THEN
        RAISE EXCEPTION 'governance receipt % is outside its execution claim window', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER governance_execution_receipt_window_is_valid
AFTER INSERT ON governance_execution_receipts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_governance_execution_receipt_window();

ALTER TABLE governance_cases
    ADD CONSTRAINT governance_cases_execution_claim_fk
    FOREIGN KEY (
        execution_claim_id, id, project_id, execution_claim_generation, action_digest
    ) REFERENCES governance_execution_claims(
        id, case_id, project_id, generation, action_digest
    )
    DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT governance_cases_execution_receipt_fk
    FOREIGN KEY (
        execution_receipt_id, id, project_id, action_digest,
        execution_claim_id, execution_claim_generation
    ) REFERENCES governance_execution_receipts(
        id, case_id, project_id, action_digest,
        execution_claim_id, execution_claim_generation
    )
    DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION check_governance_case_execution_state() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'EXECUTING' AND NOT EXISTS (
        SELECT 1 FROM governance_execution_claims c
         WHERE c.id = NEW.execution_claim_id
           AND c.case_id = NEW.id
           AND c.project_id = NEW.project_id
           AND c.generation = NEW.execution_claim_generation
           AND c.action_digest = NEW.action_digest
           AND c.state = 'ACTIVE'
           AND c.expires_at > clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'executing governance case % lacks its active execution claim', NEW.id
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state IN (
        'APPLIED', 'DENIED', 'CHANGES_REQUESTED', 'EXPIRED', 'CANCELLED', 'SUPERSEDED'
    ) AND EXISTS (
        SELECT 1 FROM governance_execution_claims c
         WHERE c.case_id = NEW.id AND c.state = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION 'terminal governance case % still has an active execution claim', NEW.id
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'APPLIED' AND NOT EXISTS (
        SELECT 1 FROM governance_execution_receipts r
         WHERE r.id = NEW.execution_receipt_id
           AND r.case_id = NEW.id
           AND r.project_id = NEW.project_id
           AND r.action_digest = NEW.action_digest
           AND r.execution_claim_id = NEW.execution_claim_id
           AND r.execution_claim_generation = NEW.execution_claim_generation
           AND r.status = 'SUCCEEDED'
    ) THEN
        RAISE EXCEPTION 'applied governance case % lacks a successful receipt', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER governance_case_execution_state_is_consistent
AFTER INSERT OR UPDATE ON governance_cases
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_governance_case_execution_state();

CREATE FUNCTION check_execution_claim_terminal_case() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'ACTIVE' AND EXISTS (
        SELECT 1 FROM governance_cases g
         WHERE g.id = NEW.case_id
           AND g.state IN (
             'APPLIED', 'DENIED', 'CHANGES_REQUESTED', 'EXPIRED', 'CANCELLED', 'SUPERSEDED'
           )
    ) THEN
        RAISE EXCEPTION 'active execution claim targets terminal case %', NEW.case_id
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state <> 'ACTIVE' AND EXISTS (
        SELECT 1 FROM governance_cases g
         WHERE g.id = NEW.case_id
           AND g.execution_claim_id = NEW.id
           AND g.execution_claim_generation = NEW.generation
           AND g.state = 'EXECUTING'
    ) THEN
        RAISE EXCEPTION 'execution claim % terminalized while case % is executing',
            NEW.id, NEW.case_id USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER execution_claim_terminal_case_is_consistent
AFTER INSERT OR UPDATE ON governance_execution_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_execution_claim_terminal_case();

CREATE TABLE policy_activations (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL,
    policy_scope_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    previous_revision_id uuid,
    governance_case_id uuid,
    expected_scope_version bigint NOT NULL CHECK (expected_scope_version >= 0),
    impact_report_digest bytea NOT NULL CHECK (octet_length(impact_report_digest) = 32),
    action_digest bytea NOT NULL CHECK (octet_length(action_digest) = 32),
    activated_by uuid NOT NULL,
    reason_code text NOT NULL CHECK (btrim(reason_code) <> ''),
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (policy_scope_id, expected_scope_version),
    FOREIGN KEY (policy_scope_id, project_id)
        REFERENCES policy_scopes(id, project_id),
    FOREIGN KEY (revision_id, project_id, policy_scope_id)
        REFERENCES policy_revisions(id, project_id, policy_scope_id),
    FOREIGN KEY (previous_revision_id, project_id, policy_scope_id)
        REFERENCES policy_revisions(id, project_id, policy_scope_id),
    FOREIGN KEY (governance_case_id, project_id, action_digest, revision_id)
        REFERENCES governance_cases(id, project_id, action_digest, policy_revision_id)
);

CREATE TRIGGER policy_activations_are_immutable
BEFORE UPDATE OR DELETE ON policy_activations
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE FUNCTION check_policy_scope_activation_history() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    previous_revision uuid;
    previous_version bigint;
BEGIN
    IF TG_OP = 'INSERT' THEN
        previous_revision := NULL;
        previous_version := 0;
    ELSE
        previous_revision := OLD.current_revision_id;
        previous_version := OLD.version;
    END IF;
    IF NEW.current_revision_id IS DISTINCT FROM previous_revision THEN
        IF NEW.version <> previous_version + 1 THEN
            RAISE EXCEPTION 'policy scope % activation must increment version once', NEW.id
                USING ERRCODE = '23514';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM policy_activations a
             WHERE a.policy_scope_id = NEW.id
               AND a.project_id = NEW.project_id
               AND a.revision_id = NEW.current_revision_id
               AND a.previous_revision_id IS NOT DISTINCT FROM previous_revision
               AND a.expected_scope_version = previous_version
        ) THEN
            RAISE EXCEPTION 'policy scope % activation lacks immutable history', NEW.id
                USING ERRCODE = '23514';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM policy_revisions r
             WHERE r.id = NEW.current_revision_id
               AND r.policy_scope_id = NEW.id
               AND r.project_id = NEW.project_id
               AND r.state = 'ACTIVE'
        ) THEN
            RAISE EXCEPTION 'policy scope % current revision is not active', NEW.id
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER policy_scope_activation_has_history
AFTER INSERT OR UPDATE ON policy_scopes
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_policy_scope_activation_history();

CREATE FUNCTION check_current_policy_revision_state() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM policy_scopes s
         WHERE s.id = NEW.policy_scope_id
           AND s.project_id = NEW.project_id
           AND s.current_revision_id = NEW.id
    ) AND NEW.state <> 'ACTIVE' THEN
        RAISE EXCEPTION 'current policy revision % must remain active', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER current_policy_revision_state_is_consistent
AFTER UPDATE ON policy_revisions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_current_policy_revision_state();

CREATE FUNCTION check_policy_activation_reached_scope() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM policy_scopes s
         WHERE s.id = NEW.policy_scope_id
           AND s.project_id = NEW.project_id
           AND s.current_revision_id = NEW.revision_id
           AND s.version = NEW.expected_scope_version + 1
    ) THEN
        RAISE EXCEPTION 'policy activation % did not reach its scope CAS pointer', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER policy_activation_reached_scope
AFTER INSERT ON policy_activations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_policy_activation_reached_scope();

COMMIT;
