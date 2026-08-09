BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Phase 1 advances an event head atomically with an event batch. It does not
-- introduce a generic JSON aggregate store: the typed tables from 0001/0002
-- remain the only canonical business state and receive typed repositories in
-- Phase 2.
CREATE TABLE aggregate_event_heads (
    project_id uuid NOT NULL REFERENCES projects(id),
    aggregate_type text NOT NULL CHECK (aggregate_type IN (
        'WORK_PACKAGE', 'ATTEMPT', 'LEASE', 'SUBMISSION', 'RUN_SIGNAL',
        'INVOCATION_INTENT', 'INVOCATION_RUN', 'RUN_CLAIM', 'SESSION_CAPSULE',
        'BUDGET_RESERVATION', 'GOVERNANCE_CASE', 'DECISION', 'POLICY_REVISION'
    )),
    aggregate_id uuid NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    last_event_seq bigint NOT NULL CHECK (last_event_seq > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, aggregate_type, aggregate_id)
);

-- This migration is a coordinated writer-quiescence boundary. Acquire the
-- ledger lock before inspecting or deriving any history, and retain it through
-- the envelope-version/default and uniqueness changes. A legacy writer already
-- in flight must finish before validation; one that resumes after COMMIT fails
-- closed because envelope_version no longer has a default.
LOCK TABLE domain_events IN ACCESS EXCLUSIVE MODE;

-- Existing ledgers were constrained to one event per aggregate version. Reject
-- a malformed historic sequence before deriving the head used by the new UoW.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM domain_events
         GROUP BY project_id, aggregate_type, aggregate_id
        HAVING min(event_seq) <> 1
            OR max(event_seq) <> count(*)
            OR min(aggregate_version) <> 1
            OR max(aggregate_version) <> count(DISTINCT aggregate_version)
            OR bool_or(event_seq <> aggregate_version)
    ) THEN
        RAISE EXCEPTION 'domain event history is not contiguous; refusing to derive heads'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

INSERT INTO aggregate_event_heads
    (project_id, aggregate_type, aggregate_id, aggregate_version, last_event_seq)
SELECT project_id, aggregate_type, aggregate_id,
       max(aggregate_version), max(event_seq)
  FROM domain_events
 GROUP BY project_id, aggregate_type, aggregate_id;

-- Payload digest protocol is independent of each event's payload schema. Rows
-- written before this migration used envelope v1 (typed serde JSON bytes).
-- New appends must state v2 explicitly (JCS); dropping the default prevents a
-- future writer from silently misclassifying evidence as legacy.
ALTER TABLE domain_events
    ADD COLUMN envelope_version smallint NOT NULL DEFAULT 1
        CHECK (envelope_version IN (1, 2));
ALTER TABLE domain_events
    ALTER COLUMN envelope_version DROP DEFAULT;

-- One command version may emit multiple events. Per-aggregate event_seq remains
-- unique; aggregate_event_heads serializes and closes each version batch.
ALTER TABLE domain_events
    DROP CONSTRAINT domain_events_aggregate_type_aggregate_id_aggregate_version_key;

-- A publisher proof is (message, holder, generation, unexpired lease). Quiesce
-- and safely release any pre-generation claims while holding an exclusive table
-- lock; they will be delivered again under the new fencing protocol.
LOCK TABLE outbox_messages IN ACCESS EXCLUSIVE MODE;
UPDATE outbox_messages
   SET claimed_by = NULL, claim_expires_at = NULL
 WHERE claimed_by IS NOT NULL;

ALTER TABLE outbox_messages
    ADD COLUMN claim_generation bigint NOT NULL DEFAULT 0
        CHECK (claim_generation >= 0);

ALTER TABLE outbox_messages
    ADD CONSTRAINT outbox_claim_generation_shape CHECK (
        (claimed_by IS NULL AND claim_generation >= 0)
        OR
        (claimed_by IS NOT NULL AND claim_generation > 0)
    );

CREATE INDEX outbox_messages_claim_recovery_idx
    ON outbox_messages (claim_expires_at, id)
    WHERE published_at IS NULL AND claimed_by IS NOT NULL;

-- Existing rows remain explicit legacy tombstones: all three fields are NULL.
-- New Phase 1 rows always populate the complete identity/response binding.
ALTER TABLE command_receipts
    ADD COLUMN command_id uuid,
    ADD COLUMN resource_version bigint CHECK (
        resource_version IS NULL OR resource_version > 0),
    ADD COLUMN response_digest bytea CHECK (
        response_digest IS NULL OR octet_length(response_digest) = 32),
    ADD CONSTRAINT command_receipts_phase1_shape CHECK (
        (command_id IS NULL AND resource_version IS NULL AND response_digest IS NULL)
        OR
        (command_id IS NOT NULL AND resource_version IS NOT NULL AND response_digest IS NOT NULL)
    );

-- Inbox receipts are permanent dedupe facts, just like command receipts.
CREATE TRIGGER inbox_messages_are_immutable
BEFORE UPDATE OR DELETE ON inbox_messages
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

COMMIT;
