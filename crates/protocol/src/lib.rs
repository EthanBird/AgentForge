//! Versioned wire contracts and deterministic validation for AgentForge.

pub mod canonical;
pub mod lint;
pub mod model;
pub mod schema;
pub mod strict_json;

use thiserror::Error;

pub use canonical::{compute_package_hash, package_hash, verify_package_hash};
pub use lint::{
    CandidateReadyReport, ExternalFactRequirement, GraphLintReport, LintFinding, LintProfile,
    LintReport, lint_candidate_ready, lint_candidate_ready_value, lint_candidate_submission,
    lint_publish, lint_work_graph, lint_work_graph_json,
};
pub use model::{Afwp, Submission};
pub use schema::{SchemaDescriptor, SchemaKind, SchemaViolation};

/// Package name used by dependency-boundary tests.
pub const CRATE_NAME: &str = "agentforge-protocol";

/// Strict wire-document decoding error.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// JSON syntax, UTF-8, number range or duplicate-key failure.
    #[error("AF_SCHEMA_INVALID: {0}")]
    StrictJson(#[from] strict_json::StrictJsonError),
    /// The document selects a protocol version not supported by this build.
    #[error("AF_SCHEMA_VERSION_UNSUPPORTED: received {received}, supported {supported}")]
    UnsupportedVersion {
        /// Received wire value.
        received: String,
        /// Supported wire value.
        supported: &'static str,
    },
    /// Draft 2020-12 validation failure.
    #[error("AF_SCHEMA_INVALID: {violations:?}")]
    Schema {
        /// Deterministically sorted violations.
        violations: Vec<SchemaViolation>,
    },
    /// The schema and Rust model unexpectedly disagree.
    #[error("AF_SCHEMA_INVALID: typed decoding failed: {0}")]
    Typed(#[from] serde_json::Error),
}

/// Strictly decode a schema-valid AFWP/1.0 document.
pub fn parse_afwp(input: &[u8]) -> Result<Afwp, DecodeError> {
    let value = strict_json::from_slice(input)?;
    reject_unknown_version(&value, SchemaKind::Afwp)?;
    schema::validate(SchemaKind::Afwp, &value)
        .map_err(|violations| DecodeError::Schema { violations })?;
    serde_json::from_value(value).map_err(DecodeError::Typed)
}

/// Strictly decode a schema-valid candidate or salvage Submission/1.0.
pub fn parse_submission(input: &[u8]) -> Result<Submission, DecodeError> {
    let value = strict_json::from_slice(input)?;
    reject_unknown_version(&value, SchemaKind::Submission)?;
    schema::validate(SchemaKind::Submission, &value)
        .map_err(|violations| DecodeError::Schema { violations })?;
    serde_json::from_value(value).map_err(DecodeError::Typed)
}

fn reject_unknown_version(value: &serde_json::Value, kind: SchemaKind) -> Result<(), DecodeError> {
    let Some(received) = value
        .get("schema_version")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(());
    };
    if received == kind.schema_version() {
        Ok(())
    } else {
        Err(DecodeError::UnsupportedVersion {
            received: received.to_owned(),
            supported: kind.schema_version(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_repository_examples_decode_to_strict_types() {
        let afwp = parse_afwp(include_bytes!("../../../examples/afwp-lease-fencing.json")).unwrap();
        assert_eq!(afwp.requirements.len(), 4);

        let candidate = parse_submission(include_bytes!(
            "../../../examples/submission-lease-fencing.json"
        ))
        .unwrap();
        assert!(candidate.git.is_some());

        let salvage = parse_submission(include_bytes!(
            "../../../examples/submission-salvage-lease-fencing.json"
        ))
        .unwrap();
        assert!(salvage.salvage.is_some());
    }

    #[test]
    fn serde_model_rejects_unknown_nested_fields() {
        let mut value =
            strict_json::from_slice(include_bytes!("../../../examples/afwp-lease-fencing.json"))
                .unwrap();
        value["goal"]["unknown"] = serde_json::json!(true);
        let error = serde_json::from_value::<Afwp>(value).unwrap_err();
        assert!(error.to_string().contains("unknown field `unknown`"));
    }
}
