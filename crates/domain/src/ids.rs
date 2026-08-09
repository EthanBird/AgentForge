//! Strong identifiers and content-addressed values used by the domain.

use std::{fmt, num::NonZeroU32, num::NonZeroU64, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::DomainError;

macro_rules! id_type {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(
                Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
            )]
            #[serde(transparent)]
            pub struct $name(Uuid);

            impl $name {
                /// Constructs a typed identifier from a UUID supplied by a trusted boundary.
                #[must_use]
                pub const fn from_uuid(value: Uuid) -> Self {
                    Self(value)
                }

                #[must_use]
                pub const fn as_uuid(&self) -> &Uuid {
                    &self.0
                }

                #[must_use]
                pub const fn into_uuid(self) -> Uuid {
                    self.0
                }
            }

            impl From<Uuid> for $name {
                fn from(value: Uuid) -> Self {
                    Self::from_uuid(value)
                }
            }

            impl From<$name> for Uuid {
                fn from(value: $name) -> Self {
                    value.into_uuid()
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    self.0.fmt(formatter)
                }
            }

            impl FromStr for $name {
                type Err = uuid::Error;

                fn from_str(value: &str) -> Result<Self, Self::Err> {
                    Uuid::parse_str(value).map(Self)
                }
            }
        )+
    };
}

id_type!(
    ProjectId,
    PackageId,
    PackageRevisionId,
    AttemptId,
    LeaseId,
    CandidateArtifactId,
    CandidateId,
    VerificationRunId,
    VerificationStageResultId,
    SubmissionId,
    IntegrationId,
    RelayTicketId,
    ObligationId,
    ActorId,
    ExecutorId,
    NodeId,
    EventId,
    CommandId,
    CorrelationId,
);

/// A stable AFWP/Submission key. It is not an internal aggregate identifier.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProtocolKey(String);

impl ProtocolKey {
    pub const MAX_LEN: usize = 128;

    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= Self::MAX_LEN
            && value.is_ascii()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if valid {
            Ok(Self(value))
        } else {
            Err(DomainError::InvalidArgument {
                field: "protocol_key".into(),
                reason: "must be 1-128 ASCII letters, digits, '.', '_' or '-'".into(),
            })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ProtocolKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ProtocolKey {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for ProtocolKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProtocolKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A stable idempotency token. The raw key is deliberately omitted from errors.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub const MAX_LEN: usize = 200;

    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.is_empty() || value.len() > Self::MAX_LEN || value.chars().any(char::is_control) {
            return Err(DomainError::InvalidArgument {
                field: "idempotency_key".into(),
                reason: "must be 1-200 printable characters".into(),
            });
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for IdempotencyKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IdempotencyKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PackageRevision(NonZeroU32);

impl PackageRevision {
    pub fn new(value: u32) -> Result<Self, DomainError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidArgument {
                field: "revision".into(),
                reason: "must be non-zero".into(),
            })
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AggregateVersion(u64);

impl AggregateVersion {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::InvariantViolation {
                invariant: "aggregate_version_must_not_overflow",
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FencingToken(NonZeroU64);

impl FencingToken {
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidArgument {
                field: "fencing_token".into(),
                reason: "must be non-zero".into(),
            })
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub fn checked_next(self) -> Result<Self, DomainError> {
        self.get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(DomainError::InvariantViolation {
                invariant: "fencing_token_must_not_overflow",
            })
    }
}

/// SHA-256 value serialized in the protocol form `sha256:<hex-lower>`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub const PREFIX: &'static str = "sha256:";

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn of_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self(Sha256::digest(bytes.as_ref()).into())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn parse(value: &str) -> Result<Self, DomainError> {
        let hex = value
            .strip_prefix(Self::PREFIX)
            .ok_or_else(|| DomainError::InvalidArgument {
                field: "sha256_digest".into(),
                reason: "missing sha256 prefix".into(),
            })?;
        if hex.len() != 64 {
            return Err(DomainError::InvalidArgument {
                field: "sha256_digest".into(),
                reason: "digest must contain 64 hexadecimal digits".into(),
            });
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            match (high, low) {
                (Some(high), Some(low)) => bytes[index] = (high << 4) | low,
                _ => {
                    return Err(DomainError::InvalidArgument {
                        field: "sha256_digest".into(),
                        reason: "digest must be lower-case hexadecimal".into(),
                    });
                }
            }
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn to_prefixed_hex(self) -> String {
        let mut value = String::with_capacity(Self::PREFIX.len() + 64);
        value.push_str(Self::PREFIX);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in self.0 {
            value.push(char::from(HEX[usize::from(byte >> 4)]));
            value.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        value
    }
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_prefixed_hex())
    }
}

impl FromStr for Sha256Digest {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_prefixed_hex())
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

/// A full repository-native Git object ID (SHA-1 or SHA-256).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GitObjectId(String);

impl GitObjectId {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        let valid_length = matches!(value.len(), 40 | 64);
        if !valid_length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(DomainError::InvalidArgument {
                field: "git_object_id".into(),
                reason: "must be a full 40- or 64-digit hexadecimal object ID".into(),
            });
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GitObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for GitObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GitObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServerInstant(pub OffsetDateTime);

impl ServerInstant {
    #[must_use]
    pub const fn new(value: OffsetDateTime) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: ProtocolKey,
    pub uri: String,
    pub digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackageSnapshot {
    pub package_hash: Sha256Digest,
    pub base_commit: GitObjectId,
    pub toolchain_lock_hash: Sha256Digest,
    pub input_artifacts: Vec<ArtifactRef>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_round_trip_is_protocol_stable() {
        let digest = Sha256Digest::of_bytes(b"agentforge");
        let encoded = serde_json::to_string(&digest).expect("serialize digest");
        let decoded: Sha256Digest = serde_json::from_str(&encoded).expect("deserialize digest");
        assert_eq!(decoded, digest);
        assert!(digest.to_string().starts_with("sha256:"));
    }

    #[test]
    fn rejects_uppercase_digest() {
        let value = format!("sha256:{}", "AA".repeat(32));
        assert!(Sha256Digest::parse(&value).is_err());
    }
}
