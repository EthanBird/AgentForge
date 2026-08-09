BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE domain_events (
    id uuid PRIMARY KEY,
    global_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    project_id uuid NOT NULL REFERENCES projects(id),
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    event_seq bigint NOT NULL CHECK (event_seq > 0),
    event_type text NOT NULL,
    schema_version smallint NOT NULL CHECK (schema_version > 0),
    payload jsonb NOT NULL,
    payload_digest bytea NOT NULL CHECK (octet_length(payload_digest) = 32),
    metadata jsonb NOT NULL,
    metadata_digest bytea NOT NULL CHECK (octet_length(metadata_digest) = 32),
    causation_id uuid,
    correlation_id uuid NOT NULL,
    actor_id uuid NOT NULL,
    occurred_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (aggregate_type, aggregate_id, event_seq),
    UNIQUE (aggregate_type, aggregate_id, aggregate_version),
    UNIQUE (id, project_id),
    UNIQUE (id, project_id, global_sequence)
);

CREATE INDEX domain_events_project_cursor_idx
    ON domain_events (project_id, global_sequence);
CREATE INDEX domain_events_correlation_idx
    ON domain_events (project_id, correlation_id, occurred_at, id);
CREATE TRIGGER domain_events_are_immutable
BEFORE UPDATE OR DELETE ON domain_events
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

ALTER TABLE run_signals
    ADD CONSTRAINT run_signals_cause_event_fk
    FOREIGN KEY (cause_event_id, project_id)
    REFERENCES domain_events(id, project_id);

CREATE TABLE outbox_messages (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    event_id uuid NOT NULL,
    topic text NOT NULL,
    message_key text NOT NULL,
    envelope jsonb NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_by uuid,
    claim_expires_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    published_at timestamptz,
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (event_id, topic),
    FOREIGN KEY (event_id, project_id)
        REFERENCES domain_events(id, project_id),
    CHECK ((claimed_by IS NULL) = (claim_expires_at IS NULL))
);

CREATE INDEX outbox_messages_pending_idx
    ON outbox_messages (available_at, id)
    WHERE published_at IS NULL;

CREATE TABLE inbox_messages (
    consumer text NOT NULL,
    message_id uuid NOT NULL,
    project_id uuid NOT NULL REFERENCES projects(id),
    payload_digest bytea NOT NULL CHECK (octet_length(payload_digest) = 32),
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_at timestamptz,
    result_digest bytea CHECK (result_digest IS NULL OR octet_length(result_digest) = 32),
    PRIMARY KEY (consumer, message_id),
    CHECK ((applied_at IS NULL) = (result_digest IS NULL))
);

CREATE TABLE sensitive_response_envelopes (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    ciphertext_ref text NOT NULL,
    ciphertext_digest bytea NOT NULL CHECK (octet_length(ciphertext_digest) = 32),
    expires_at timestamptz NOT NULL,
    destroyed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, project_id),
    CHECK (expires_at > created_at)
);

CREATE FUNCTION enforce_sensitive_response_envelope_immutability() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'sensitive response envelope cannot be deleted: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.id <> OLD.id
       OR NEW.project_id <> OLD.project_id
       OR NEW.ciphertext_ref <> OLD.ciphertext_ref
       OR NEW.ciphertext_digest <> OLD.ciphertext_digest
       OR NEW.expires_at <> OLD.expires_at
       OR NEW.created_at <> OLD.created_at
       OR (OLD.destroyed_at IS NOT NULL AND NEW.destroyed_at IS DISTINCT FROM OLD.destroyed_at) THEN
        RAISE EXCEPTION 'sensitive response envelope binding is immutable: %', OLD.id
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sensitive_response_envelope_is_immutable
BEFORE UPDATE OR DELETE ON sensitive_response_envelopes
FOR EACH ROW EXECUTE FUNCTION enforce_sensitive_response_envelope_immutability();

CREATE TABLE command_receipts (
    actor_id uuid NOT NULL,
    idempotency_key_hash bytea NOT NULL CHECK (octet_length(idempotency_key_hash) = 32),
    project_id uuid NOT NULL REFERENCES projects(id),
    command_type text NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    response_classification text NOT NULL CHECK (response_classification IN (
        'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    )),
    response_status smallint NOT NULL CHECK (response_status BETWEEN 100 AND 599),
    response_body jsonb,
    sensitive_response_envelope_id uuid,
    effect_digest bytea NOT NULL CHECK (octet_length(effect_digest) = 32),
    replay_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (actor_id, idempotency_key_hash),
    FOREIGN KEY (sensitive_response_envelope_id, project_id)
        REFERENCES sensitive_response_envelopes(id, project_id),
    CHECK ((response_body IS NULL) <> (sensitive_response_envelope_id IS NULL)),
    CHECK (
        (response_classification IN ('PUBLIC', 'INTERNAL')
         AND response_body IS NOT NULL)
        OR
        (response_classification IN ('CONFIDENTIAL', 'RESTRICTED')
         AND sensitive_response_envelope_id IS NOT NULL)
    )
);

CREATE INDEX command_receipts_replay_idx ON command_receipts (replay_until);
CREATE TRIGGER command_receipts_are_immutable
BEFORE UPDATE OR DELETE ON command_receipts
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE obligations (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    obligation_type text NOT NULL,
    state text NOT NULL CHECK (state IN (
        'PENDING', 'CLAIMED', 'SATISFIED', 'FAILED', 'CANCELLED'
    )),
    due_at timestamptz NOT NULL,
    owner_id uuid,
    owner_generation bigint NOT NULL DEFAULT 0 CHECK (owner_generation >= 0),
    claim_expires_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    backoff_until timestamptz,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    payload jsonb NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, obligation_type, subject_type, subject_id, fingerprint),
    CHECK (attempts <= max_attempts)
);

CREATE INDEX obligations_due_idx ON obligations (due_at, id)
    WHERE state = 'PENDING';
CREATE INDEX obligations_claim_expiry_idx ON obligations (claim_expires_at, id)
    WHERE state = 'CLAIMED';

CREATE TABLE projection_offsets (
    projection_name text NOT NULL,
    project_id uuid NOT NULL REFERENCES projects(id),
    last_event_sequence bigint,
    last_event_id uuid,
    projection_version bigint NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    source_digest bytea NOT NULL CHECK (octet_length(source_digest) = 32),
    status text NOT NULL DEFAULT 'HEALTHY' CHECK (status IN (
        'HEALTHY', 'REBUILDING', 'DEGRADED'
    )),
    degraded_reason text,
    rebuilt_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_name, project_id),
    FOREIGN KEY (last_event_id, project_id, last_event_sequence)
        REFERENCES domain_events(id, project_id, global_sequence),
    CHECK ((last_event_sequence IS NULL) = (last_event_id IS NULL)),
    CHECK ((status = 'DEGRADED') = (degraded_reason IS NOT NULL))
);

CREATE TABLE projection_documents (
    projection_name text NOT NULL,
    project_id uuid NOT NULL REFERENCES projects(id),
    resource_id uuid NOT NULL,
    projection_version bigint NOT NULL CHECK (projection_version > 0),
    schema_version smallint NOT NULL CHECK (schema_version > 0),
    data_classification text NOT NULL CHECK (data_classification IN (
        'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    )),
    acl_scope_digest bytea NOT NULL CHECK (octet_length(acl_scope_digest) = 32),
    redaction_digest bytea NOT NULL CHECK (octet_length(redaction_digest) = 32),
    document jsonb NOT NULL,
    document_digest bytea NOT NULL CHECK (octet_length(document_digest) = 32),
    source_digest bytea NOT NULL CHECK (octet_length(source_digest) = 32),
    source_event_sequence bigint NOT NULL,
    source_occurred_at timestamptz NOT NULL,
    source_event_id uuid NOT NULL,
    rebuilt_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_name, project_id, resource_id),
    FOREIGN KEY (source_event_id, project_id, source_event_sequence)
        REFERENCES domain_events(id, project_id, global_sequence)
);

CREATE INDEX projection_documents_page_idx
    ON projection_documents (projection_name, project_id, source_occurred_at DESC,
                             source_event_id DESC, resource_id);

CREATE TABLE projection_changes (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    projection_name text NOT NULL,
    resource_id uuid NOT NULL,
    projection_version bigint NOT NULL CHECK (projection_version > 0),
    source_event_sequence bigint NOT NULL,
    source_occurred_at timestamptz NOT NULL,
    source_event_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (projection_name, project_id, resource_id, projection_version),
    FOREIGN KEY (source_event_id, project_id, source_event_sequence)
        REFERENCES domain_events(id, project_id, global_sequence)
);

CREATE INDEX projection_changes_resume_idx
    ON projection_changes (project_id, sequence);

CREATE TRIGGER projection_changes_are_immutable
BEFORE UPDATE OR DELETE ON projection_changes
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

COMMIT;
