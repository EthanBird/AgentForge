//! Replaceable read/write boundary for Control Room projections.
//!
//! The in-memory implementation is a local reference adapter. A production
//! adapter must persist both [`StoreEpoch`] and [`DurableChangeSequence`] and
//! atomically validate resume positions when opening a change subscription.

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use agentforge_application::{ProjectReadModels, ProjectionCursor, StoredProjection};
use agentforge_domain::{ProjectId, ServerInstant, Sha256Digest};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

const CHANGE_LOG_CAPACITY: usize = 2_048;
const BROADCAST_CAPACITY: usize = 512;

pub type SourceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProjectionSourceError>> + Send + 'a>>;
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;
pub type ProjectionChangeStream =
    Pin<Box<dyn Stream<Item = Result<ProjectionChange, ProjectionSourceError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreEpoch(Uuid);

impl StoreEpoch {
    #[must_use]
    pub const fn new(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> Uuid {
        self.0
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct DurableChangeSequence(u64);

impl DurableChangeSequence {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct StorePosition {
    pub epoch: StoreEpoch,
    pub sequence: DurableChangeSequence,
}

impl StorePosition {
    #[must_use]
    pub const fn new(epoch: StoreEpoch, sequence: DurableChangeSequence) -> Self {
        Self { epoch, sequence }
    }
}

#[derive(Clone, Debug)]
pub struct ControlRoomSnapshot {
    pub position: StorePosition,
    pub projection_version: u64,
    pub source_digest: Sha256Digest,
    pub source_cursor: ProjectionCursor,
    pub read_models: ProjectReadModels,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionResource {
    ProjectReadModels,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProjectionChange {
    pub epoch: StoreEpoch,
    pub sequence: DurableChangeSequence,
    pub resource: ProjectionResource,
    pub project_id: ProjectId,
    pub projection_version: u64,
    pub source_occurred_at: ServerInstant,
}

impl ProjectionChange {
    #[must_use]
    pub const fn position(&self) -> StorePosition {
        StorePosition::new(self.epoch, self.sequence)
    }
}

pub struct ProjectionSubscription {
    pub changes: ProjectionChangeStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionSourceError {
    CursorExpired,
    ChangeFeedLagged,
    Unavailable,
}

impl std::fmt::Display for ProjectionSourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CursorExpired => formatter.write_str("projection cursor expired"),
            Self::ChangeFeedLagged => formatter.write_str("projection change feed lagged"),
            Self::Unavailable => formatter.write_str("projection source unavailable"),
        }
    }
}

impl std::error::Error for ProjectionSourceError {}

/// Async read boundary consumed by every Control Room HTTP endpoint.
pub trait ProjectionSource: Send + Sync + 'static {
    fn snapshot(&self, project_id: ProjectId) -> SourceFuture<'_, Option<ControlRoomSnapshot>>;

    /// Opens a project-scoped feed after atomically validating the adapter-owned
    /// epoch and sequence. `None` starts at the retained head for a fresh client.
    fn subscribe(
        &self,
        project_id: ProjectId,
        resume: Option<StorePosition>,
    ) -> SourceFuture<'_, ProjectionSubscription>;

    fn ready(&self) -> SourceFuture<'_, bool>;
}

/// Async write boundary implemented by projection stores.
pub trait ControlRoomStore: ProjectionSource {
    fn replace(&self, checkpoint: StoredProjection) -> StoreFuture<'_, DurableChangeSequence>;
}

#[derive(Clone)]
pub struct ProjectionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    sender: broadcast::Sender<ProjectionChange>,
}

#[derive(Debug)]
struct RegistryInner {
    epoch: StoreEpoch,
    next_sequence: DurableChangeSequence,
    projects: BTreeMap<ProjectId, ProjectSnapshot>,
    changes: VecDeque<ProjectionChange>,
}

#[derive(Clone, Debug)]
struct ProjectSnapshot {
    last_change_sequence: DurableChangeSequence,
    projection_version: u64,
    source_digest: Sha256Digest,
    source_cursor: ProjectionCursor,
    read_models: ProjectReadModels,
}

impl ProjectionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::with_epoch(StoreEpoch::new(Uuid::now_v7()))
    }

    fn with_epoch(epoch: StoreEpoch) -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                epoch,
                next_sequence: DurableChangeSequence::ZERO,
                projects: BTreeMap::new(),
                changes: VecDeque::new(),
            })),
            sender,
        }
    }

    async fn replace_inner(
        &self,
        checkpoint: StoredProjection,
    ) -> anyhow::Result<DurableChangeSequence> {
        checkpoint.validate()?;
        validate_checkpoint_for_serving(&checkpoint)?;
        let mut inner = self.inner.write().await;
        if let Some(current) = inner.projects.get(&checkpoint.project_id) {
            if checkpoint.cursor < current.source_cursor {
                anyhow::bail!("projection checkpoint cursor moved backwards");
            }
            if checkpoint.cursor == current.source_cursor {
                if checkpoint.payload == current.read_models {
                    return Ok(current.last_change_sequence);
                }
                anyhow::bail!("projection checkpoint reused a cursor with different content");
            }
        }

        let next_sequence = inner
            .next_sequence
            .get()
            .checked_add(1)
            .map(DurableChangeSequence::new)
            .ok_or_else(|| anyhow::anyhow!("projection change sequence exhausted"))?;
        inner.next_sequence = next_sequence;
        let epoch = inner.epoch;
        let projection_version =
            inner
                .projects
                .get(&checkpoint.project_id)
                .map_or(Ok(1), |snapshot| {
                    snapshot
                        .projection_version
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("projection version exhausted"))
                })?;
        let change = ProjectionChange {
            epoch,
            sequence: next_sequence,
            resource: ProjectionResource::ProjectReadModels,
            project_id: checkpoint.project_id,
            projection_version,
            source_occurred_at: checkpoint.cursor.occurred_at,
        };
        inner.projects.insert(
            checkpoint.project_id,
            ProjectSnapshot {
                last_change_sequence: next_sequence,
                projection_version,
                source_digest: checkpoint.payload_digest,
                source_cursor: checkpoint.cursor,
                read_models: checkpoint.payload,
            },
        );
        inner.changes.push_back(change.clone());
        while inner.changes.len() > CHANGE_LOG_CAPACITY {
            inner.changes.pop_front();
        }
        drop(inner);
        let _ = self.sender.send(change);
        Ok(next_sequence)
    }

    async fn snapshot_inner(&self, project_id: ProjectId) -> Option<ControlRoomSnapshot> {
        let inner = self.inner.read().await;
        inner
            .projects
            .get(&project_id)
            .map(|snapshot| ControlRoomSnapshot {
                position: StorePosition::new(inner.epoch, snapshot.last_change_sequence),
                projection_version: snapshot.projection_version,
                source_digest: snapshot.source_digest,
                source_cursor: snapshot.source_cursor,
                read_models: snapshot.read_models.clone(),
            })
    }

    async fn subscribe_inner(
        &self,
        project_id: ProjectId,
        resume: Option<StorePosition>,
    ) -> Result<ProjectionSubscription, ProjectionSourceError> {
        let mut receiver = self.sender.subscribe();
        let inner = self.inner.read().await;
        let position =
            resume.unwrap_or(StorePosition::new(inner.epoch, DurableChangeSequence::ZERO));
        let after = position.sequence.get();
        let expired = position.epoch != inner.epoch
            || after > inner.next_sequence.get()
            || inner
                .changes
                .front()
                .is_some_and(|oldest| after > 0 && after.saturating_add(1) < oldest.sequence.get());
        if expired {
            return Err(ProjectionSourceError::CursorExpired);
        }
        let backlog = inner
            .changes
            .iter()
            .filter(|change| change.project_id == project_id && change.sequence.get() > after)
            .cloned()
            .collect::<Vec<_>>();
        let live_after = inner.next_sequence;
        drop(inner);

        let changes = async_stream::stream! {
            for change in backlog {
                yield Ok(change);
            }
            loop {
                match receiver.recv().await {
                    Ok(change)
                        if change.project_id == project_id && change.sequence > live_after =>
                    {
                        yield Ok(change);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        yield Err(ProjectionSourceError::ChangeFeedLagged);
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Ok(ProjectionSubscription {
            changes: Box::pin(changes),
        })
    }
}

impl ProjectionSource for ProjectionRegistry {
    fn snapshot(&self, project_id: ProjectId) -> SourceFuture<'_, Option<ControlRoomSnapshot>> {
        Box::pin(async move { Ok(self.snapshot_inner(project_id).await) })
    }

    fn subscribe(
        &self,
        project_id: ProjectId,
        resume: Option<StorePosition>,
    ) -> SourceFuture<'_, ProjectionSubscription> {
        Box::pin(self.subscribe_inner(project_id, resume))
    }

    fn ready(&self) -> SourceFuture<'_, bool> {
        Box::pin(async { Ok(!self.inner.read().await.projects.is_empty()) })
    }
}

impl ControlRoomStore for ProjectionRegistry {
    fn replace(&self, checkpoint: StoredProjection) -> StoreFuture<'_, DurableChangeSequence> {
        Box::pin(self.replace_inner(checkpoint))
    }
}

impl Default for ProjectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn next_change(
    changes: &mut ProjectionChangeStream,
) -> Option<Result<ProjectionChange, ProjectionSourceError>> {
    std::future::poll_fn(|context| match changes.as_mut().poll_next(context) {
        Poll::Ready(item) => Poll::Ready(item),
        Poll::Pending => Poll::Pending,
    })
    .await
}

fn validate_checkpoint_for_serving(checkpoint: &StoredProjection) -> anyhow::Result<()> {
    let header = checkpoint.payload.header();
    let valid = checkpoint
        .payload
        .is_project_consistent(checkpoint.project_id)
        && checkpoint.payload.checkpoint_invariants_hold()
        && header.projection_version == checkpoint.projection_version.get()
        && header.last_event_id == Some(checkpoint.cursor.event_id)
        && header.last_event_sequence == checkpoint.last_event_sequence
        && header.source_digest == checkpoint.source_digest
        && header.as_of == Some(checkpoint.as_of)
        && header.rebuilt_at == Some(checkpoint.rebuilt_at)
        && header.staleness_ms == checkpoint.staleness_ms
        && header.degraded_reason == checkpoint.degraded_reason;
    if !valid {
        anyhow::bail!("projection checkpoint metadata is inconsistent");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use agentforge_application::{
        ProjectReadModels, ProjectionCursor, ProjectionEnvelope, ProjectionHeader, StoredProjection,
    };
    use agentforge_domain::{EventId, ProjectId, ProtocolKey, ServerInstant, Sha256Digest};
    use time::{Duration, macros::datetime};
    use uuid::Uuid;

    use super::{
        ControlRoomStore, DurableChangeSequence, ProjectionRegistry, ProjectionSource,
        ProjectionSourceError, StoreEpoch, StorePosition, next_change,
    };

    fn project() -> ProjectId {
        ProjectId::from_str("018f0000-0000-7000-8000-000000000001").expect("valid project id")
    }

    fn checkpoint(project_id: ProjectId, sequence: u64) -> StoredProjection {
        let occurred_at = ServerInstant(
            datetime!(2026-08-10 00:00 UTC)
                + Duration::seconds(i64::try_from(sequence).expect("small sequence")),
        );
        let event_id = EventId::from(Uuid::from_u128(u128::from(sequence)));
        let source_digest = Sha256Digest::of_bytes(sequence.to_be_bytes());
        let header = ProjectionHeader {
            projection_version: 1,
            last_event_id: Some(event_id),
            last_event_sequence: sequence,
            source_digest,
            as_of: Some(occurred_at),
            rebuilt_at: Some(occurred_at),
            staleness_ms: 0,
            degraded_reason: None,
        };
        let mut payload = ProjectReadModels::new(project_id);
        payload.project_control_room.header = header.clone();
        payload.runs.header = header.clone();
        payload.governance_inbox.header = header.clone();
        payload.fleet.header = header.clone();
        payload.budget.header = header.clone();
        payload.lineage.header = header.clone();
        payload.work_graph.header = header.clone();
        payload.activity.header = header;
        let cursor = ProjectionCursor::new(occurred_at, event_id, sequence);
        let mut checkpoint =
            ProjectionEnvelope::new(project_id, cursor, payload).expect("envelope");
        checkpoint.source_digest = source_digest;
        checkpoint.as_of = occurred_at;
        checkpoint.rebuilt_at = occurred_at;
        checkpoint
    }

    #[tokio::test]
    async fn registry_is_monotonic_idempotent_and_rejects_split_headers() {
        let project_id = project();
        let registry = ProjectionRegistry::new();
        let first = checkpoint(project_id, 1);
        assert_eq!(
            registry.replace(first.clone()).await.expect("insert"),
            DurableChangeSequence::new(1)
        );
        assert_eq!(
            registry.replace(first.clone()).await.expect("replay"),
            DurableChangeSequence::new(1)
        );

        let mut conflicting = first;
        conflicting
            .payload
            .project_control_room
            .counters
            .active_runs = 1;
        conflicting = ProjectionEnvelope::new(project_id, conflicting.cursor, conflicting.payload)
            .expect("conflicting envelope");
        conflicting.source_digest = Sha256Digest::of_bytes(1_u64.to_be_bytes());
        assert!(registry.replace(conflicting).await.is_err());

        let mut split = checkpoint(project_id, 2);
        split.payload.activity.header.degraded_reason =
            Some(ProtocolKey::new("split_header").expect("key"));
        split = ProjectionEnvelope::new(project_id, split.cursor, split.payload)
            .expect("split envelope");
        split.source_digest = Sha256Digest::of_bytes(2_u64.to_be_bytes());
        assert!(registry.replace(split).await.is_err());

        assert!(registry.replace(checkpoint(project_id, 0)).await.is_err());
    }

    #[tokio::test]
    async fn source_contract_replays_changes_and_invalidates_a_rebuilt_store_epoch() {
        let project_id = project();
        let first_epoch = StoreEpoch::new(Uuid::from_bytes([3; 16]));
        let rebuilt_epoch = StoreEpoch::new(Uuid::from_bytes([4; 16]));
        let first = ProjectionRegistry::with_epoch(first_epoch);
        first
            .replace(checkpoint(project_id, 1))
            .await
            .expect("write first store");

        let mut subscription = first
            .subscribe(
                project_id,
                Some(StorePosition::new(first_epoch, DurableChangeSequence::ZERO)),
            )
            .await
            .expect("resume current epoch");
        let change = next_change(&mut subscription.changes)
            .await
            .expect("backlog item")
            .expect("valid change");
        assert_eq!(change.sequence, DurableChangeSequence::new(1));

        let rebuilt = ProjectionRegistry::with_epoch(rebuilt_epoch);
        assert!(matches!(
            rebuilt.subscribe(project_id, Some(change.position())).await,
            Err(ProjectionSourceError::CursorExpired)
        ));
    }
}
