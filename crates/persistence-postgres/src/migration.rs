//! Ordered, content-addressed SQL migrations.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::Client;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub sql: &'static str,
}

impl Migration {
    #[must_use]
    pub fn digest(&self) -> String {
        format!(
            "sha256:{}",
            hex::encode(Sha256::digest(self.sql.as_bytes()))
        )
    }
}

pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "m1_core",
        sql: include_str!("../../../migrations/0001_m1_core.sql"),
    },
    Migration {
        version: 2,
        name: "m1_invocation_governance",
        sql: include_str!("../../../migrations/0002_m1_invocation_governance.sql"),
    },
    Migration {
        version: 3,
        name: "m1_events_projections",
        sql: include_str!("../../../migrations/0003_m1_events_projections.sql"),
    },
    Migration {
        version: 4,
        name: "m1_uow",
        sql: include_str!("../../../migrations/0004_m1_uow.sql"),
    },
    Migration {
        version: 5,
        name: "mvp_candidates",
        sql: include_str!("../../../migrations/0005_mvp_candidates.sql"),
    },
    Migration {
        version: 6,
        name: "mvp_attempt_progress",
        sql: include_str!("../../../migrations/0006_mvp_attempt_progress.sql"),
    },
];

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MigrationManifestError {
    #[error("migration versions must be contiguous; expected {expected}, found {actual}")]
    NonContiguousVersion { expected: u32, actual: u32 },
    #[error("duplicate migration name: {0}")]
    DuplicateName(&'static str),
    #[error("migration {version} must be wrapped in one explicit transaction")]
    MissingTransaction { version: u32 },
    #[error("migration {version} contains a forbidden destructive statement")]
    DestructiveStatement { version: u32 },
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error(transparent)]
    Manifest(#[from] MigrationManifestError),
    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),
    #[error("database has unknown migration version {version}")]
    UnknownAppliedVersion { version: u32 },
    #[error("applied migrations must be a contiguous prefix; expected {expected}, found {actual}")]
    AppliedVersionGap { expected: u32, actual: u32 },
    #[error("migration {version} drift: expected {expected}, database has {actual}")]
    Drift {
        version: u32,
        expected: String,
        actual: String,
    },
    #[error("migration {version} name drift: expected {expected}, database has {actual}")]
    NameDrift {
        version: u32,
        expected: &'static str,
        actual: String,
    },
}

pub fn validate_manifest() -> Result<(), MigrationManifestError> {
    let mut names = BTreeSet::new();
    for (index, migration) in MIGRATIONS.iter().enumerate() {
        let expected = u32::try_from(index + 1).expect("migration count fits in u32");
        if migration.version != expected {
            return Err(MigrationManifestError::NonContiguousVersion {
                expected,
                actual: migration.version,
            });
        }
        if !names.insert(migration.name) {
            return Err(MigrationManifestError::DuplicateName(migration.name));
        }

        let normalized = migration.sql.trim().to_ascii_uppercase();
        if !normalized.starts_with("BEGIN;") || !normalized.ends_with("COMMIT;") {
            return Err(MigrationManifestError::MissingTransaction {
                version: migration.version,
            });
        }
        if normalized.contains("DROP TABLE")
            || normalized.contains("TRUNCATE ")
            || normalized.contains("DELETE FROM")
        {
            return Err(MigrationManifestError::DestructiveStatement {
                version: migration.version,
            });
        }
    }
    Ok(())
}

const MIGRATION_LOCK_SQL: &str =
    "SELECT pg_advisory_lock(hashtextextended('agentforge.schema-migration.v1', 0))";
const MIGRATION_UNLOCK_SQL: &str =
    "SELECT pg_advisory_unlock(hashtextextended('agentforge.schema-migration.v1', 0))";

/// Applies every pending migration while holding a database-scoped advisory lock.
///
/// The SQL files retain explicit `BEGIN`/`COMMIT` markers for human review. The
/// runner removes only those two markers and executes the body plus the history
/// insert in one driver transaction, so schema and checksum cannot diverge.
pub async fn migrate(client: &mut Client) -> Result<Vec<u32>, MigrationError> {
    validate_manifest()?;
    client.batch_execute(MIGRATION_LOCK_SQL).await?;
    let result = migrate_while_locked(client).await;
    let unlock_result = client.batch_execute(MIGRATION_UNLOCK_SQL).await;
    match (result, unlock_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(MigrationError::Database(error)),
        (Ok(applied), Ok(())) => Ok(applied),
    }
}

async fn migrate_while_locked(client: &mut Client) -> Result<Vec<u32>, MigrationError> {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS agentforge_schema_migrations (\
             version integer PRIMARY KEY CHECK (version > 0),\
             name text NOT NULL UNIQUE,\
             source_digest text NOT NULL CHECK (source_digest ~ '^sha256:[0-9a-f]{64}$'),\
             applied_at timestamptz NOT NULL DEFAULT clock_timestamp()\
             )",
        )
        .await?;

    let rows = client
        .query(
            "SELECT version, name, source_digest \
             FROM agentforge_schema_migrations ORDER BY version",
            &[],
        )
        .await?;

    let mut applied_prefix = 0_u32;
    for row in rows {
        let raw_version: i32 = row.get(0);
        let version =
            u32::try_from(raw_version).map_err(|_| MigrationError::UnknownAppliedVersion {
                version: raw_version.unsigned_abs(),
            })?;
        let Some(expected) = MIGRATIONS
            .iter()
            .find(|migration| migration.version == version)
        else {
            return Err(MigrationError::UnknownAppliedVersion { version });
        };
        let expected_prefix = applied_prefix
            .checked_add(1)
            .expect("migration prefix length fits in u32");
        if version != expected_prefix {
            return Err(MigrationError::AppliedVersionGap {
                expected: expected_prefix,
                actual: version,
            });
        }
        let actual_name: String = row.get(1);
        if actual_name != expected.name {
            return Err(MigrationError::NameDrift {
                version,
                expected: expected.name,
                actual: actual_name,
            });
        }
        let actual_digest: String = row.get(2);
        let expected_digest = expected.digest();
        if actual_digest != expected_digest {
            return Err(MigrationError::Drift {
                version,
                expected: expected_digest,
                actual: actual_digest,
            });
        }
        applied_prefix = version;
    }
    let mut applied = Vec::new();
    for migration in MIGRATIONS
        .iter()
        .filter(|migration| migration.version > applied_prefix)
    {
        let transaction = client.transaction().await?;
        transaction
            .batch_execute(transaction_body(migration.sql))
            .await?;
        transaction
            .execute(
                "INSERT INTO agentforge_schema_migrations \
                 (version, name, source_digest) VALUES ($1, $2, $3)",
                &[
                    &i32::try_from(migration.version).expect("migration version fits i32"),
                    &migration.name,
                    &migration.digest(),
                ],
            )
            .await?;
        transaction.commit().await?;
        applied.push(migration.version);
    }
    Ok(applied)
}

fn transaction_body(sql: &str) -> &str {
    let trimmed = sql.trim();
    let without_begin = trimmed
        .strip_prefix("BEGIN;")
        .expect("validated migration starts with BEGIN");
    without_begin
        .strip_suffix("COMMIT;")
        .expect("validated migration ends with COMMIT")
        .trim()
}

#[cfg(test)]
mod tests {
    use super::{MIGRATIONS, transaction_body, validate_manifest};

    #[test]
    fn migration_manifest_is_contiguous_transactional_and_additive() {
        validate_manifest().expect("repository migration manifest must be valid");
        assert_eq!(MIGRATIONS.len(), 6);
        assert!(MIGRATIONS.iter().all(|migration| {
            let digest = migration.digest();
            digest.len() == 71 && digest.starts_with("sha256:")
        }));
    }

    #[test]
    fn transaction_body_removes_only_the_review_wrapper() {
        let body = transaction_body("BEGIN;\nSELECT 'BEGIN; COMMIT;';\nCOMMIT;\n");
        assert_eq!(body, "SELECT 'BEGIN; COMMIT;';");
    }

    #[test]
    fn invocation_governance_constraints_are_present() {
        let sql = MIGRATIONS[1].sql;
        for required in [
            "invocation_intents_one_active_dedup_idx",
            "run_claims_one_active_idx",
            "budget_accounts",
            "CHECK (reserved_units + spent_units <= limit_units)",
            "parent_reservation_id",
            "external_invocation_key",
            "governance_cases_inbox_idx",
            "governance_decisions_are_immutable",
            "governance_execution_receipts",
            "policy_revisions_one_active_idx",
        ] {
            assert!(sql.contains(required), "missing SQL invariant: {required}");
        }
    }

    #[test]
    fn event_and_projection_ledgers_have_dedupe_and_resume_keys() {
        let sql = MIGRATIONS[2].sql;
        for required in [
            "UNIQUE (aggregate_type, aggregate_id, event_seq)",
            "PRIMARY KEY (consumer, message_id)",
            "PRIMARY KEY (actor_id, idempotency_key_hash)",
            "projection_changes_resume_idx",
            "global_sequence",
            "last_event_sequence",
            "source_digest",
            "source_occurred_at",
            "source_event_id",
        ] {
            assert!(sql.contains(required), "missing SQL invariant: {required}");
        }
    }

    #[test]
    fn uow_event_head_receipt_and_fencing_constraints_are_present() {
        let sql = MIGRATIONS[3].sql;
        for required in [
            "CREATE TABLE aggregate_event_heads",
            "PRIMARY KEY (project_id, aggregate_type, aggregate_id)",
            "LOCK TABLE domain_events IN ACCESS EXCLUSIVE MODE",
            "bool_or(event_seq <> aggregate_version)",
            "ADD COLUMN envelope_version smallint NOT NULL DEFAULT 1",
            "CHECK (envelope_version IN (1, 2))",
            "ALTER COLUMN envelope_version DROP DEFAULT",
            "DROP CONSTRAINT domain_events_aggregate_type_aggregate_id_aggregate_version_key",
            "LOCK TABLE outbox_messages IN ACCESS EXCLUSIVE MODE",
            "claim_generation bigint NOT NULL DEFAULT 0",
            "outbox_claim_generation_shape",
            "outbox_messages_claim_recovery_idx",
            "ADD COLUMN command_id uuid",
            "ADD COLUMN resource_version bigint",
            "ADD COLUMN response_digest bytea",
            "command_receipts_phase1_shape",
            "inbox_messages_are_immutable",
        ] {
            assert!(sql.contains(required), "missing SQL invariant: {required}");
        }
        let domain_events_lock = sql
            .find("LOCK TABLE domain_events IN ACCESS EXCLUSIVE MODE")
            .expect("domain event migration lock");
        for protected_operation in [
            "DO $$",
            "INSERT INTO aggregate_event_heads",
            "ADD COLUMN envelope_version",
            "DROP CONSTRAINT domain_events_aggregate_type_aggregate_id_aggregate_version_key",
        ] {
            assert!(
                domain_events_lock < sql.find(protected_operation).expect(protected_operation),
                "domain_events must be locked before {protected_operation}"
            );
        }
        assert!(
            !sql.contains("aggregate_snapshots"),
            "Phase 1 must not install a generic JSON aggregate authority"
        );
    }

    #[test]
    fn candidate_first_tables_and_event_heads_are_present() {
        let sql = MIGRATIONS[4].sql;
        for required in [
            "CREATE TABLE candidate_artifacts",
            "CREATE TABLE candidate_artifact_chunks",
            "CREATE TABLE candidates",
            "CREATE TABLE verification_runs",
            "candidate_artifact_chunks_match_reservation",
            "candidate_artifacts_binding_is_immutable",
            "candidates_are_immutable",
            "verification_runs_binding_is_immutable",
            "'CANDIDATE_ARTIFACT'",
            "'CANDIDATE'",
            "'VERIFICATION_RUN'",
        ] {
            assert!(sql.contains(required), "missing SQL invariant: {required}");
        }
        assert!(
            !sql.contains("aggregate_snapshots"),
            "Candidate state must remain in typed canonical tables"
        );
    }
}
