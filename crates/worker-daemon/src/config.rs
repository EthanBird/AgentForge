//! Strict Worker daemon configuration and stable node fingerprinting.

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use agentforge_domain::{ActorId, ExecutorId, NodeId, ProjectId, Sha256Digest};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::lifecycle::{LeaseMaintenancePolicy, WorkerIdentity};

const CONFIG_SCHEMA_VERSION: u16 = 1;
const MAX_CONFIG_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDaemonConfig {
    pub schema_version: u16,
    pub actor_id: ActorId,
    pub executor_id: ExecutorId,
    pub node_id: NodeId,
    pub runtime_fingerprint: Sha256Digest,
    pub project_ids: Vec<ProjectId>,
    pub journal_path: PathBuf,
    pub capacity: u16,
    pub offer_limit: u16,
    pub lease_seconds: u32,
    pub max_lease_seconds: u32,
    pub renew_before_seconds: u32,
    pub extend_by_seconds: u32,
    pub tick_seconds: u32,
}

impl WorkerDaemonConfig {
    pub fn load(path: impl AsRef<Path>) -> ConfigResult<Self> {
        let path = path.as_ref();
        let metadata = fs::symlink_metadata(path).map_err(ConfigError::Io)?;
        if !metadata.file_type().is_file() || metadata.len() > MAX_CONFIG_BYTES {
            return Err(ConfigError::Invalid);
        }
        let bytes = fs::read(path).map_err(ConfigError::Io)?;
        let config: Self = serde_json::from_slice(&bytes).map_err(|_| ConfigError::Decode)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> ConfigResult<()> {
        let projects = self
            .project_ids
            .iter()
            .map(|project_id| project_id.as_uuid())
            .collect::<BTreeSet<_>>();
        let journal_is_safe_absolute = self.journal_path.is_absolute()
            && self
                .journal_path
                .components()
                .all(|component| !matches!(component, Component::ParentDir));
        if self.schema_version != CONFIG_SCHEMA_VERSION
            || self.actor_id.as_uuid().is_nil()
            || self.executor_id.as_uuid().is_nil()
            || self.node_id.as_uuid().is_nil()
            || self
                .runtime_fingerprint
                .as_bytes()
                .iter()
                .all(|byte| *byte == 0)
            || self.project_ids.is_empty()
            || self.project_ids.len() > 64
            || projects.len() != self.project_ids.len()
            || projects.iter().any(|project_id| project_id.is_nil())
            || !journal_is_safe_absolute
            || self.capacity == 0
            || self.capacity > 64
            || self.offer_limit == 0
            || self.offer_limit > 1_000
            || !(5..=3_600).contains(&self.lease_seconds)
            || self.max_lease_seconds < self.lease_seconds
            || self.max_lease_seconds > 86_400
            || self.renew_before_seconds == 0
            || self.renew_before_seconds > self.lease_seconds
            || self.extend_by_seconds == 0
            || self.extend_by_seconds > 86_400
            || self.tick_seconds == 0
            || self.tick_seconds > 300
        {
            return Err(ConfigError::Invalid);
        }
        Ok(())
    }

    pub fn identity(&self) -> WorkerIdentity {
        WorkerIdentity {
            actor_id: self.actor_id,
            executor_id: self.executor_id,
            node_id: self.node_id,
        }
    }

    pub const fn lease_policy(&self) -> LeaseMaintenancePolicy {
        LeaseMaintenancePolicy {
            renew_before_seconds: self.renew_before_seconds,
            extend_by_seconds: self.extend_by_seconds,
        }
    }

    pub fn node_fingerprint(&self) -> ConfigResult<Sha256Digest> {
        self.validate()?;
        let bytes = serde_json_canonicalizer::to_vec(self).map_err(|_| ConfigError::Decode)?;
        Ok(Sha256Digest::of_bytes(bytes))
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Worker configuration could not be read")]
    Io(#[source] std::io::Error),
    #[error("Worker configuration is not valid JSON")]
    Decode,
    #[error("Worker configuration violates the v1 contract")]
    Invalid,
}

impl ConfigError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "AF_WORKER_CONFIG_UNAVAILABLE",
            Self::Decode => "AF_WORKER_CONFIG_DECODE",
            Self::Invalid => "AF_WORKER_CONFIG_INVALID",
        }
    }
}

pub type ConfigResult<T> = Result<T, ConfigError>;

#[cfg(test)]
mod tests {
    use std::fs;

    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn config(path: PathBuf) -> WorkerDaemonConfig {
        WorkerDaemonConfig {
            schema_version: 1,
            actor_id: id(1),
            executor_id: id(2),
            node_id: id(3),
            runtime_fingerprint: Sha256Digest::of_bytes("jcode-v1+prompt-v1+tools-v1"),
            project_ids: vec![id(4)],
            journal_path: path,
            capacity: 1,
            offer_limit: 10,
            lease_seconds: 60,
            max_lease_seconds: 600,
            renew_before_seconds: 15,
            extend_by_seconds: 30,
            tick_seconds: 5,
        }
    }

    #[test]
    fn fingerprint_is_stable_and_changes_with_runtime_or_policy() {
        let first = config(PathBuf::from("/var/lib/agentforge/worker.sqlite3"));
        let mut second = first.clone();
        assert_eq!(
            first.node_fingerprint().expect("fingerprint"),
            second.node_fingerprint().expect("fingerprint")
        );
        second.runtime_fingerprint = Sha256Digest::of_bytes("jcode-v2+prompt-v1+tools-v1");
        assert_ne!(
            first.node_fingerprint().expect("fingerprint"),
            second.node_fingerprint().expect("fingerprint")
        );
        second = first.clone();
        second.renew_before_seconds += 1;
        assert_ne!(
            first.node_fingerprint().expect("fingerprint"),
            second.node_fingerprint().expect("fingerprint")
        );
    }

    #[test]
    fn loader_is_strict_bounded_and_rejects_unsafe_paths_or_duplicate_projects() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("worker.json");
        let valid = config(directory.path().join("worker.sqlite3"));
        fs::write(&path, serde_json::to_vec(&valid).expect("JSON")).expect("write config");
        assert_eq!(WorkerDaemonConfig::load(&path).expect("load"), valid);

        let mut duplicate = valid.clone();
        duplicate.project_ids.push(duplicate.project_ids[0]);
        assert_eq!(
            duplicate.validate().expect_err("duplicate project").code(),
            "AF_WORKER_CONFIG_INVALID"
        );
        let mut relative = valid;
        relative.journal_path = PathBuf::from("../worker.sqlite3");
        assert!(relative.validate().is_err());

        let with_unknown = serde_json::json!({
            "schema_version": 1,
            "actor_id": id::<ActorId>(1),
            "executor_id": id::<ExecutorId>(2),
            "node_id": id::<NodeId>(3),
            "runtime_fingerprint": Sha256Digest::of_bytes("runtime"),
            "project_ids": [id::<ProjectId>(4)],
            "journal_path": directory.path().join("worker.sqlite3"),
            "capacity": 1,
            "offer_limit": 10,
            "lease_seconds": 60,
            "max_lease_seconds": 600,
            "renew_before_seconds": 15,
            "extend_by_seconds": 30,
            "tick_seconds": 5,
            "unexpected": true
        });
        fs::write(&path, serde_json::to_vec(&with_unknown).expect("JSON"))
            .expect("write unknown config");
        assert_eq!(
            WorkerDaemonConfig::load(&path)
                .expect_err("unknown field")
                .code(),
            "AF_WORKER_CONFIG_DECODE"
        );
    }
}
