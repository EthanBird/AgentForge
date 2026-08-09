//! AFWP-C14N-1 canonicalization, hashing and verification.

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::schema::{self, SchemaKind, SchemaViolation};
use crate::strict_json::{self, StrictJsonError};

/// Failures produced by AFWP-C14N-1.
#[derive(Debug, Error)]
pub enum HashError {
    /// Strict JSON parsing failed before any canonicalization was attempted.
    #[error("AF_SCHEMA_INVALID: {0}")]
    StrictJson(#[from] StrictJsonError),
    /// The document selects an unsupported AFWP schema version.
    #[error("AF_SCHEMA_VERSION_UNSUPPORTED: received {received}, supported afwp/1.0")]
    UnsupportedVersion {
        /// Received `schema_version` string.
        received: String,
    },
    /// The document is not a structurally valid AFWP/1.0 instance.
    #[error("AF_SCHEMA_INVALID: {violations:?}")]
    SchemaInvalid {
        /// Deterministically ordered schema diagnostics.
        violations: Vec<SchemaViolation>,
    },
    /// AFWP-C14N-1 only applies to a top-level JSON object.
    #[error("AF_SCHEMA_INVALID: AFWP root must be an object")]
    RootNotObject,
    /// The schema-valid instance unexpectedly lacks `package_hash`.
    #[error("AF_SCHEMA_INVALID: /package_hash is required")]
    MissingPackageHash,
    /// JCS serialization failed.
    #[error("JCS canonicalization failed: {0}")]
    Canonicalization(#[from] serde_json::Error),
    /// The declared hash is not the AFWP-C14N-1 hash.
    #[error("AF_PACKAGE_HASH_MISMATCH: declared {declared}, computed {computed}")]
    Mismatch {
        /// Hash present in the document.
        declared: String,
        /// Hash recomputed from the canonical signing value.
        computed: String,
    },
}

/// Strictly parse and hash an AFWP JSON document using AFWP-C14N-1.
pub fn package_hash(input: &[u8]) -> Result<String, HashError> {
    let value = strict_json::from_slice(input)?;
    compute_package_hash(&value)
}

/// Compute AFWP-C14N-1 for an already strictly parsed JSON value.
///
/// This performs Draft 2020-12 schema validation before removing exactly the
/// top-level `package_hash` member.
pub fn compute_package_hash(value: &Value) -> Result<String, HashError> {
    if let Some(received) = value.get("schema_version").and_then(Value::as_str)
        && received != SchemaKind::Afwp.schema_version()
    {
        return Err(HashError::UnsupportedVersion {
            received: received.to_owned(),
        });
    }
    schema::validate(SchemaKind::Afwp, value)
        .map_err(|violations| HashError::SchemaInvalid { violations })?;
    let canonical = canonical_bytes_without_package_hash(value)?;
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(&canonical))
    ))
}

/// Return the JCS bytes used as AFWP-C14N-1 hash input.
pub fn canonical_bytes_without_package_hash(value: &Value) -> Result<Vec<u8>, HashError> {
    let mut signing_value = value.clone();
    let object = signing_value
        .as_object_mut()
        .ok_or(HashError::RootNotObject)?;
    if object.remove("package_hash").is_none() {
        return Err(HashError::MissingPackageHash);
    }
    serde_json_canonicalizer::to_vec(&signing_value).map_err(HashError::Canonicalization)
}

/// Strictly parse an AFWP and verify its declared `package_hash`.
pub fn verify_package_hash(input: &[u8]) -> Result<(), HashError> {
    let value = strict_json::from_slice(input)?;
    verify_package_hash_value(&value)
}

/// Verify `package_hash` on an already strictly parsed AFWP value.
pub fn verify_package_hash_value(value: &Value) -> Result<(), HashError> {
    let declared = value
        .as_object()
        .ok_or(HashError::RootNotObject)?
        .get("package_hash")
        .and_then(Value::as_str)
        .ok_or(HashError::MissingPackageHash)?
        .to_owned();
    let computed = compute_package_hash(value)?;
    if declared == computed {
        Ok(())
    } else {
        Err(HashError::Mismatch { declared, computed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &[u8] = include_bytes!("../../../../examples/afwp-lease-fencing.json");

    #[test]
    fn example_matches_normative_vector() {
        assert_eq!(
            package_hash(EXAMPLE).unwrap(),
            "sha256:e02271c1d96c4b82fa250eecaac34ea4a8542639b9d87d7cf4b26d11ac95b83f"
        );
        verify_package_hash(EXAMPLE).unwrap();
    }

    #[test]
    fn only_top_level_hash_is_removed() {
        let mut value = strict_json::from_slice(EXAMPLE).unwrap();
        value["goal"]["glossary"]["package_hash"] = Value::String("nested-a".to_owned());
        let first = compute_package_hash(&value).unwrap();
        value["goal"]["glossary"]["package_hash"] = Value::String("nested-b".to_owned());
        let second = compute_package_hash(&value).unwrap();
        assert_ne!(first, second);
    }
}
