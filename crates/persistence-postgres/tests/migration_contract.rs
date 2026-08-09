use std::{fs, path::PathBuf};

use agentforge_storage_postgres::migration::{MIGRATIONS, validate_manifest};

#[test]
fn migration_manifest_and_repository_files_match() {
    validate_manifest().expect("migration manifest is valid");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let expected = [
        "migrations/0001_m1_core.sql",
        "migrations/0002_m1_invocation_governance.sql",
        "migrations/0003_m1_events_projections.sql",
        "migrations/0004_m1_uow.sql",
    ];
    assert_eq!(MIGRATIONS.len(), expected.len());
    for (migration, relative_path) in MIGRATIONS.iter().zip(expected) {
        let bytes = fs::read(root.join(relative_path)).expect("migration file exists");
        assert_eq!(bytes, migration.sql.as_bytes(), "embedded SQL drifted");
    }
}

#[test]
fn invocation_and_governance_sql_match_the_frozen_contract() {
    let core = MIGRATIONS[0].sql;
    for required in [
        "attempts_binding_is_immutable",
        "BEFORE UPDATE OR DELETE ON attempts",
        "FOREIGN KEY (from_package_id, project_id)",
        "FOREIGN KEY (to_package_id, project_id)",
        "BEFORE UPDATE OR DELETE ON leases",
    ] {
        assert!(
            core.contains(required),
            "missing core persistence invariant: {required}"
        );
    }

    let invocation = MIGRATIONS[1].sql;
    for required in [
        "'PENDING', 'CLAIMED', 'DISPATCHED', 'SATISFIED', 'CANCELLED', 'DEAD_LETTER'",
        "'RESERVED', 'STARTING', 'RUNNING', 'RECONCILING'",
        "'COMPLETED', 'FAILED', 'CANCELLED'",
        "external_invocation_key text NOT NULL UNIQUE",
        "intent_id uuid NOT NULL UNIQUE",
        "claim_request_id uuid NOT NULL",
        "package_hash bytea CHECK",
        "REFERENCES package_revisions(id, package_id, package_hash)",
        "signal % subject does not match intent %",
        "session_capsules_created_by_run_fk",
        "UNIQUE (project_id, content_digest)",
        "'AUTHOR_MODEL', 'AUTHOR_COMPUTE', 'RUNNER', 'REVIEWER', 'ARTIFACT'",
        "parent_reservation_id uuid",
        "governance_execution_claims",
        "governance_execution_receipts",
        "authorization_epoch bigint NOT NULL DEFAULT 1 CHECK (authorization_epoch > 0)",
        "author_actor_id uuid",
        "attempt_lease_id uuid",
        "invocation_claim_generation bigint CHECK",
        "authorization_digest bytea NOT NULL CHECK",
        "observed_case_version bigint NOT NULL CHECK (observed_case_version > 0)",
        "observed_attempt_lease_id uuid",
        "observed_invocation_claim_generation bigint CHECK",
        "execution_claim_id uuid NOT NULL",
        "holder_actor_id uuid NOT NULL",
        "governance_execution_start_is_current",
        "governance execution claim % has stale observed bindings",
        "governance_execution_receipt_window_is_valid",
        "governance_cases_execution_claim_fk",
        "jsonb_array_length(evidence_refs) > 0",
        "effect_digest <> decode(repeat('00', 32), 'hex')",
        "governance_decisions_are_immutable",
        "operator_notes_are_immutable",
        "policy_scopes",
        "policy_scope_activation_has_history",
        "policy_activation_reached_scope",
        "current_policy_revision_state_is_consistent",
        "budget_accounts_binding_is_immutable",
        "CHECK (parent_reservation_id IS DISTINCT FROM id)",
        "live run % lacks an unexpired current active claim",
        "BEFORE UPDATE OR DELETE ON invocation_runs",
        "BEFORE UPDATE OR DELETE ON run_claims",
        "'ROUTING', 'BUDGET', 'GOVERNANCE', 'SECURITY', 'RETENTION'",
        "'DENY', 'DEFER', 'CANCEL', 'SUPERSEDE'",
    ] {
        assert!(
            invocation.contains(required),
            "missing frozen SQL contract: {required}"
        );
    }
    for forbidden in [
        "WAITING_SAFE_POINT",
        "PARTIALLY_APPLIED",
        "governance_case_id uuid NOT NULL UNIQUE",
    ] {
        assert!(
            !invocation.contains(forbidden),
            "obsolete contract leaked into SQL: {forbidden}"
        );
    }
}

#[test]
fn projection_sql_uses_a_durable_monotonic_cursor() {
    let projection = MIGRATIONS[2].sql;
    for required in [
        "global_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE",
        "last_event_sequence bigint",
        "source_digest bytea NOT NULL",
        "response_classification text NOT NULL",
        "acl_scope_digest bytea NOT NULL",
        "redaction_digest bytea NOT NULL",
        "projection_changes_are_immutable",
        "run_signals_cause_event_fk",
        "FOREIGN KEY (event_id, project_id)",
        "BEFORE UPDATE OR DELETE ON sensitive_response_envelopes",
    ] {
        assert!(
            projection.contains(required),
            "missing projection invariant: {required}"
        );
    }
    assert!(
        !projection.contains("last_occurred_at timestamptz"),
        "wall clock must not be the durable replay cursor"
    );
}

#[test]
fn compose_fixture_is_digest_pinned_and_has_no_plaintext_password() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let compose = fs::read_to_string(root.join("deploy/compose/postgres.yml"))
        .expect("compose fixture exists");
    assert!(compose.contains("postgres:17-bookworm@sha256:"));
    assert!(compose.contains("POSTGRES_PASSWORD_FILE"));
    assert!(!compose.contains("POSTGRES_PASSWORD:"));
    assert!(compose.contains("127.0.0.1:"));
    assert!(compose.contains("no-new-privileges:true"));
}
