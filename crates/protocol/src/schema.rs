//! Embedded, immutable Draft 2020-12 schemas and validation helpers.

use std::sync::OnceLock;

use jsonschema::error::ValidationErrorKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

const AFWP_SCHEMA: &str = include_str!("../../../schemas/afwp.schema.json");
const SUBMISSION_SCHEMA: &str = include_str!("../../../schemas/submission.schema.json");

/// A schema artifact embedded into this protocol build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaKind {
    /// AgentForge Work Package schema.
    Afwp,
    /// Candidate and salvage Submission schema.
    Submission,
}

impl SchemaKind {
    /// Stable protocol version accepted by this schema.
    #[must_use]
    pub const fn schema_version(self) -> &'static str {
        match self {
            Self::Afwp => "afwp/1.0",
            Self::Submission => "agentforge/submission/1.0",
        }
    }

    /// Immutable schema artifact identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Afwp => "https://agentforge.dev/schemas/afwp/1.0.0/schema.json",
            Self::Submission => "https://agentforge.dev/schemas/submission/1.0.0/schema.json",
        }
    }

    /// Exact embedded JSON bytes, including repository formatting.
    #[must_use]
    pub const fn text(self) -> &'static str {
        match self {
            Self::Afwp => AFWP_SCHEMA,
            Self::Submission => SUBMISSION_SCHEMA,
        }
    }
}

/// Public metadata for one immutable embedded schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaDescriptor {
    /// Human and API name.
    pub name: SchemaKind,
    /// Wire-level version selected by instances.
    pub schema_version: &'static str,
    /// Versioned JSON Schema `$id`.
    #[serde(rename = "$id")]
    pub id: &'static str,
    /// SHA-256 of the exact embedded schema artifact bytes.
    pub sha256: String,
}

/// A stable JSON Schema validation diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaViolation {
    /// Stable AgentForge wire error code.
    pub code: &'static str,
    /// JSON Pointer into the rejected instance.
    pub pointer: String,
    /// JSON Schema keyword that rejected the value.
    pub keyword: String,
    /// Diagnostic text for logs; callers must branch on `code`, not this text.
    pub message: String,
}

/// An internal schema artifact is malformed and cannot be used.
#[derive(Debug, Error)]
pub enum SchemaBuildError {
    /// The embedded artifact is not syntactically valid JSON.
    #[error("embedded {schema:?} schema is invalid JSON: {source}")]
    Json {
        /// Schema being built.
        schema: SchemaKind,
        /// JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// The embedded artifact is not a valid/compilable Draft 2020-12 schema.
    #[error("embedded {schema:?} schema does not compile: {message}")]
    Compile {
        /// Schema being built.
        schema: SchemaKind,
        /// Compiler diagnostic.
        message: String,
    },
}

/// Return metadata for all schemas supported by this build.
#[must_use]
pub fn list() -> Vec<SchemaDescriptor> {
    [SchemaKind::Afwp, SchemaKind::Submission]
        .into_iter()
        .map(descriptor)
        .collect()
}

/// Return metadata for one embedded schema.
#[must_use]
pub fn descriptor(kind: SchemaKind) -> SchemaDescriptor {
    SchemaDescriptor {
        name: kind,
        schema_version: kind.schema_version(),
        id: kind.id(),
        sha256: digest(kind),
    }
}

/// Compute the digest of the exact schema artifact embedded at compile time.
#[must_use]
pub fn digest(kind: SchemaKind) -> String {
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(kind.text().as_bytes()))
    )
}

/// Decode and return an embedded schema document.
pub fn document(kind: SchemaKind) -> Result<Value, SchemaBuildError> {
    serde_json::from_str(kind.text()).map_err(|source| SchemaBuildError::Json {
        schema: kind,
        source,
    })
}

/// Validate an instance with the selected Draft 2020-12 schema.
///
/// All findings are returned in deterministic pointer/keyword/message order.
pub fn validate(kind: SchemaKind, instance: &Value) -> Result<(), Vec<SchemaViolation>> {
    match try_validate(kind, instance) {
        Ok(findings) if findings.is_empty() => Ok(()),
        Ok(findings) => Err(findings),
        Err(error) => Err(vec![SchemaViolation {
            code: "AF_SCHEMA_INVALID",
            pointer: String::new(),
            keyword: "schema".to_owned(),
            message: error.to_string(),
        }]),
    }
}

fn try_validate(
    kind: SchemaKind,
    instance: &Value,
) -> Result<Vec<SchemaViolation>, SchemaBuildError> {
    let validator = compiled_validator(kind)?;

    let mut findings: Vec<_> = validator
        .iter_errors(instance)
        .map(|error| {
            let mut pointer = error.instance_path().as_str().to_owned();
            match error.kind() {
                ValidationErrorKind::Required { property } => {
                    if let Some(property) = property.as_str() {
                        push_pointer_token(&mut pointer, property);
                    }
                }
                ValidationErrorKind::AdditionalProperties { unexpected }
                | ValidationErrorKind::UnevaluatedProperties { unexpected } => {
                    if let Some(property) = unexpected.iter().min() {
                        push_pointer_token(&mut pointer, property);
                    }
                }
                _ => {}
            }
            SchemaViolation {
                code: "AF_SCHEMA_INVALID",
                pointer,
                keyword: error.kind().keyword().to_owned(),
                message: error.to_string(),
            }
        })
        .collect();
    findings.sort_by(|left, right| {
        (&left.pointer, &left.keyword, &left.message).cmp(&(
            &right.pointer,
            &right.keyword,
            &right.message,
        ))
    });
    findings.dedup_by(|left, right| {
        left.pointer == right.pointer
            && left.keyword == right.keyword
            && left.message == right.message
    });
    Ok(findings)
}

fn compiled_validator(
    kind: SchemaKind,
) -> Result<&'static jsonschema::Validator, SchemaBuildError> {
    static AFWP: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();
    static SUBMISSION: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();
    let slot = match kind {
        SchemaKind::Afwp => &AFWP,
        SchemaKind::Submission => &SUBMISSION,
    };
    match slot.get_or_init(|| {
        let schema: Value = serde_json::from_str(kind.text()).map_err(|error| error.to_string())?;
        jsonschema::draft202012::options()
            .should_validate_formats(true)
            .build(&schema)
            .map_err(|error| error.to_string())
    }) {
        Ok(validator) => Ok(validator),
        Err(message) => Err(SchemaBuildError::Compile {
            schema: kind,
            message: message.clone(),
        }),
    }
}

fn push_pointer_token(pointer: &mut String, token: &str) {
    pointer.push('/');
    for character in token.chars() {
        match character {
            '~' => pointer.push_str("~0"),
            '/' => pointer.push_str("~1"),
            other => pointer.push(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_schema_metadata_matches_documents() {
        for item in list() {
            let value = document(item.name).unwrap();
            assert_eq!(value["$id"], item.id);
            assert_eq!(
                value["$schema"],
                "https://json-schema.org/draft/2020-12/schema"
            );
            assert!(item.sha256.starts_with("sha256:"));
            assert_eq!(item.sha256.len(), 71);
        }
    }

    #[test]
    fn missing_property_points_at_missing_member() {
        let value = serde_json::json!({});
        let findings = validate(SchemaKind::Afwp, &value).unwrap_err();
        assert!(findings.iter().any(|item| item.pointer == "/package_id"));
    }
}
