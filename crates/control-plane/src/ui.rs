//! Read-only Control Room query API and resumable projection notifications.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};

use agentforge_application::{ProjectReadModels, StoredProjection};
use agentforge_domain::{ProjectId, ServerInstant, Sha256Digest};
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response, Sse, sse::Event},
    routing::get,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::{RwLock, broadcast};
use tower_http::{set_header::SetResponseHeaderLayer, trace::TraceLayer};
use uuid::Uuid;

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_CSS: &str = include_str!("../assets/app.css");
const APP_JS: &str = include_str!("../assets/app.js");
const CHANGE_LOG_CAPACITY: usize = 2_048;
const BROADCAST_CAPACITY: usize = 512;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct CursorCodec {
    key: Arc<[u8; 32]>,
}

impl CursorCodec {
    pub fn from_hex(value: &str) -> anyhow::Result<Self> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            anyhow::bail!("cursor HMAC key must be exactly 64 lowercase hex characters");
        }
        let bytes = hex::decode(value)?;
        let key: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("cursor HMAC key must decode to 32 bytes"))?;
        Ok(Self { key: Arc::new(key) })
    }

    #[must_use]
    pub fn encode(&self, project_id: ProjectId, epoch: Uuid, sequence: u64) -> String {
        let payload = format!("v1.{project_id}.{epoch}.{sequence}");
        let mut mac =
            HmacSha256::new_from_slice(self.key.as_ref()).expect("HMAC key length is valid");
        mac.update(payload.as_bytes());
        format!("{payload}.{}", hex::encode(mac.finalize().into_bytes()))
    }

    fn decode(
        &self,
        value: &str,
        expected_project: ProjectId,
        expected_epoch: Uuid,
    ) -> Result<u64, UiError> {
        let mut parts = value.rsplitn(2, '.');
        let signature = parts.next().ok_or(UiError::InvalidCursor)?;
        let payload = parts.next().ok_or(UiError::InvalidCursor)?;
        let signature = hex::decode(signature).map_err(|_| UiError::InvalidCursor)?;
        let mut mac =
            HmacSha256::new_from_slice(self.key.as_ref()).expect("HMAC key length is valid");
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| UiError::InvalidCursor)?;

        let mut fields = payload.split('.');
        if fields.next() != Some("v1") {
            return Err(UiError::InvalidCursor);
        }
        let project = fields
            .next()
            .ok_or(UiError::InvalidCursor)?
            .parse::<ProjectId>()
            .map_err(|_| UiError::InvalidCursor)?;
        if project != expected_project {
            return Err(UiError::InvalidCursor);
        }
        let epoch = fields
            .next()
            .ok_or(UiError::InvalidCursor)?
            .parse::<Uuid>()
            .map_err(|_| UiError::InvalidCursor)?;
        if epoch != expected_epoch {
            return Err(UiError::CursorExpired);
        }
        let sequence = fields
            .next()
            .ok_or(UiError::InvalidCursor)?
            .parse::<u64>()
            .map_err(|_| UiError::InvalidCursor)?;
        if fields.next().is_some() {
            return Err(UiError::InvalidCursor);
        }
        Ok(sequence)
    }
}

#[derive(Clone)]
pub struct ControlPlaneState {
    registry: ProjectionRegistry,
    cursor: CursorCodec,
    allowed_projects: Arc<BTreeSet<ProjectId>>,
}

impl ControlPlaneState {
    pub fn for_projects(
        cursor: CursorCodec,
        allowed_projects: impl IntoIterator<Item = ProjectId>,
    ) -> anyhow::Result<Self> {
        let allowed_projects = allowed_projects.into_iter().collect::<BTreeSet<_>>();
        if allowed_projects.is_empty() {
            anyhow::bail!("at least one local Control Room project must be allowed");
        }
        Ok(Self {
            registry: ProjectionRegistry::new(),
            cursor,
            allowed_projects: Arc::new(allowed_projects),
        })
    }

    #[must_use]
    pub fn registry(&self) -> ProjectionRegistry {
        self.registry.clone()
    }

    fn authorize_project(&self, project_id: ProjectId) -> Result<(), UiError> {
        if self.allowed_projects.contains(&project_id) {
            Ok(())
        } else {
            Err(UiError::ProjectNotFound)
        }
    }
}

#[derive(Clone)]
pub struct ProjectionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    sender: broadcast::Sender<ProjectionChange>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    epoch: Uuid,
    next_sequence: u64,
    projects: BTreeMap<ProjectId, ProjectSnapshot>,
    changes: VecDeque<ProjectionChange>,
}

#[derive(Clone, Debug)]
struct ProjectSnapshot {
    last_change_sequence: u64,
    projection_version: u64,
    source_digest: Sha256Digest,
    source_cursor: agentforge_application::ProjectionCursor,
    read_models: ProjectReadModels,
}

#[derive(Clone, Debug, Serialize)]
struct ProjectionChange {
    epoch: Uuid,
    sequence: u64,
    resource: &'static str,
    project_id: ProjectId,
    projection_version: u64,
    source_occurred_at: ServerInstant,
}

impl ProjectionRegistry {
    #[must_use]
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                epoch: Uuid::now_v7(),
                ..RegistryInner::default()
            })),
            sender,
        }
    }

    pub async fn replace(&self, checkpoint: StoredProjection) -> anyhow::Result<u64> {
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
        inner.next_sequence = inner
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("projection change sequence exhausted"))?;
        let sequence = inner.next_sequence;
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
            sequence,
            resource: "project_read_models",
            project_id: checkpoint.project_id,
            projection_version,
            source_occurred_at: checkpoint.cursor.occurred_at,
        };
        inner.projects.insert(
            checkpoint.project_id,
            ProjectSnapshot {
                last_change_sequence: sequence,
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
        Ok(sequence)
    }

    async fn snapshot(&self, project_id: ProjectId) -> Option<ProjectSnapshot> {
        self.inner.read().await.projects.get(&project_id).cloned()
    }

    async fn epoch(&self) -> Uuid {
        self.inner.read().await.epoch
    }

    async fn subscribe(
        &self,
        project_id: ProjectId,
        after: u64,
    ) -> Result<(Vec<ProjectionChange>, broadcast::Receiver<ProjectionChange>), UiError> {
        let receiver = self.sender.subscribe();
        let inner = self.inner.read().await;
        let expired = after > inner.next_sequence
            || inner
                .changes
                .front()
                .is_some_and(|oldest| after > 0 && after.saturating_add(1) < oldest.sequence);
        if expired {
            return Err(UiError::CursorExpired);
        }
        let backlog = inner
            .changes
            .iter()
            .filter(|change| change.project_id == project_id && change.sequence > after)
            .cloned()
            .collect();
        Ok((backlog, receiver))
    }
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

impl Default for ProjectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub fn router(state: ControlPlaneState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/app.css", get(styles))
        .route("/assets/app.js", get(script))
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route(
            "/v1/projects/{project_id}/control-room",
            get(mission_control),
        )
        .route("/v1/projects/{project_id}/control-room-stream", get(events))
        .fallback(not_found)
        .layer(middleware::from_fn(local_origin_guard))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, no-store"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("no-referrer"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static(
                "default-src 'self'; connect-src 'self'; img-src 'self' data:; \
                 style-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; \
                 frame-ancestors 'none'; form-action 'self'",
            ),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn local_origin_guard(request: Request, next: Next) -> Response {
    let Some(host) = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| is_loopback_authority(value))
    else {
        return local_origin_rejected();
    };

    let origins = request.headers().get_all(header::ORIGIN);
    let mut origins = origins.iter();
    if let Some(origin) = origins.next() {
        let origin_is_valid = origins.next().is_none()
            && origin.to_str().ok().is_some_and(|origin| {
                origin
                    .strip_prefix("http://")
                    .is_some_and(|authority| same_authority(authority, host))
            });
        if !origin_is_valid {
            return local_origin_rejected();
        }
    }

    next.run(request).await
}

fn is_loopback_authority(value: &str) -> bool {
    split_authority(value).is_some_and(|(host, _port)| {
        host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" || host == "[::1]"
    })
}

fn same_authority(left: &str, right: &str) -> bool {
    match (split_authority(left), split_authority(right)) {
        (Some((left_host, left_port)), Some((right_host, right_port))) => {
            left_host.eq_ignore_ascii_case(right_host) && left_port == right_port
        }
        _ => false,
    }
}

fn split_authority(value: &str) -> Option<(&str, Option<u16>)> {
    if value.is_empty()
        || value.bytes().any(|byte| byte.is_ascii_whitespace())
        || value.contains(['/', '?', '#', '@'])
    {
        return None;
    }

    if let Some(rest) = value.strip_prefix("[::1]") {
        return split_port("[::1]", rest);
    }
    if value.starts_with('[') {
        return None;
    }
    match value.rsplit_once(':') {
        Some((host, port)) => parse_port(port).map(|port| (host, Some(port))),
        None => Some((value, None)),
    }
}

fn split_port<'a>(host: &'a str, rest: &str) -> Option<(&'a str, Option<u16>)> {
    if rest.is_empty() {
        Some((host, None))
    } else {
        rest.strip_prefix(':')
            .and_then(parse_port)
            .map(|port| (host, Some(port)))
    }
}

fn parse_port(value: &str) -> Option<u16> {
    value
        .parse::<u16>()
        .ok()
        .filter(|port| *port != 0 && !value.is_empty())
}

fn local_origin_rejected() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "code": "AF_LOCAL_ORIGIN_REQUIRED",
            "message": "Control Room requests require a same-origin loopback Host"
        })),
    )
        .into_response()
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn styles() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

async fn script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        APP_JS,
    )
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<ControlPlaneState>) -> StatusCode {
    if state.registry.inner.read().await.projects.is_empty() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::NO_CONTENT
    }
}

#[derive(Debug, Serialize)]
struct MissionControlResponse {
    projection_version: u64,
    source_cursor: agentforge_application::ProjectionCursor,
    #[serde(flatten)]
    read_models: ProjectReadModels,
}

async fn mission_control(
    State(state): State<ControlPlaneState>,
    Path(project_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UiError> {
    let project_id = parse_project(&project_id)?;
    state.authorize_project(project_id)?;
    let snapshot = state
        .registry
        .snapshot(project_id)
        .await
        .ok_or(UiError::ProjectNotFound)?;
    let etag = format!("\"{}\"", snapshot.source_digest);
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        == Some(etag.as_str())
    {
        return Ok(StatusCode::NOT_MODIFIED.into_response());
    }
    let mut response = Json(MissionControlResponse {
        projection_version: snapshot.projection_version,
        source_cursor: snapshot.source_cursor,
        read_models: snapshot.read_models,
    })
    .into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("digest is a valid ETag"),
    );
    Ok(response)
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    cursor: Option<String>,
}

async fn events(
    State(state): State<ControlPlaneState>,
    Path(project_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, UiError> {
    let project_id = parse_project(&project_id)?;
    state.authorize_project(project_id)?;
    let epoch = state.registry.epoch().await;
    let reconnect_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok());
    let after = reconnect_cursor
        .or(query.cursor.as_deref())
        .map_or(Ok(0), |cursor| {
            state.cursor.decode(cursor, project_id, epoch)
        })?;
    let (backlog, mut receiver) = state.registry.subscribe(project_id, after).await?;
    let codec = state.cursor.clone();
    let stream = async_stream::stream! {
        for change in backlog {
            yield Ok(change_event(&codec, &change));
        }
        loop {
            match receiver.recv().await {
                Ok(change) if change.project_id == project_id => {
                    yield Ok(change_event(&codec, &change));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    yield Ok(Event::default().event("projection.reset").data("{}"));
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(20))
            .text("transport-keepalive"),
    ))
}

fn change_event(codec: &CursorCodec, change: &ProjectionChange) -> Event {
    let data = serde_json::to_string(change).expect("projection change is serializable");
    Event::default()
        .event("projection.invalidated")
        .id(codec.encode(change.project_id, change.epoch, change.sequence))
        .data(data)
}

fn parse_project(value: &str) -> Result<ProjectId, UiError> {
    value.parse().map_err(|_| UiError::InvalidProjectId)
}

#[derive(Clone, Copy, Debug)]
enum UiError {
    InvalidProjectId,
    InvalidCursor,
    CursorExpired,
    ProjectNotFound,
}

impl IntoResponse for UiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::InvalidProjectId => (
                StatusCode::BAD_REQUEST,
                "AF_PROJECT_ID_INVALID",
                "project_id must be a UUID",
            ),
            Self::InvalidCursor => (
                StatusCode::BAD_REQUEST,
                "AF_CURSOR_INVALID",
                "cursor is invalid or belongs to another project",
            ),
            Self::CursorExpired => (
                StatusCode::CONFLICT,
                "AF_CURSOR_EXPIRED",
                "cursor is outside the retained projection window; fetch a new snapshot",
            ),
            Self::ProjectNotFound => (
                StatusCode::NOT_FOUND,
                "AF_PROJECT_NOT_FOUND",
                "project projection was not found",
            ),
        };
        (
            status,
            Json(serde_json::json!({ "code": code, "message": message })),
        )
            .into_response()
    }
}

async fn not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "code": "AF_ROUTE_NOT_FOUND",
            "message": "route not found"
        })),
    )
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

    use super::{CursorCodec, ProjectionRegistry, UiError, is_loopback_authority, same_authority};

    fn project(value: &str) -> ProjectId {
        ProjectId::from_str(value).expect("valid project id")
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

    #[test]
    fn cursor_is_project_bound_and_tamper_evident() {
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let first = project("018f0000-0000-7000-8000-000000000001");
        let second = project("018f0000-0000-7000-8000-000000000002");
        let epoch = Uuid::from_bytes([3; 16]);
        let token = codec.encode(first, epoch, 37);
        assert_eq!(codec.decode(&token, first, epoch).expect("decode"), 37);
        assert!(codec.decode(&token, second, epoch).is_err());
        assert!(matches!(
            codec.decode(&token, first, Uuid::from_bytes([4; 16])),
            Err(UiError::CursorExpired)
        ));
        let mut tampered = token;
        tampered.push('0');
        assert!(codec.decode(&tampered, first, epoch).is_err());
        assert!(CursorCodec::from_hex(&"AA".repeat(32)).is_err());
    }

    #[test]
    fn local_authority_guard_rejects_dns_rebinding_and_cross_origin_ports() {
        for authority in [
            "localhost",
            "LOCALHOST:8080",
            "127.0.0.1:8080",
            "[::1]:8080",
        ] {
            assert!(is_loopback_authority(authority), "{authority}");
        }
        for authority in [
            "agentforge.example",
            "127.0.0.2:8080",
            "localhost.example:8080",
            "localhost:0",
            "localhost:invalid",
            "localhost:8080/path",
            "user@localhost:8080",
        ] {
            assert!(!is_loopback_authority(authority), "{authority}");
        }

        assert!(same_authority("LOCALHOST:8080", "localhost:8080"));
        assert!(same_authority("[::1]:8080", "[::1]:8080"));
        assert!(!same_authority("localhost:8081", "localhost:8080"));
        assert!(!same_authority("localhost:8080", "127.0.0.1:8080"));
    }

    #[tokio::test]
    async fn registry_is_monotonic_idempotent_and_rejects_split_headers() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let registry = ProjectionRegistry::new();
        let first = checkpoint(project_id, 1);
        assert_eq!(registry.replace(first.clone()).await.expect("insert"), 1);
        assert_eq!(registry.replace(first.clone()).await.expect("replay"), 1);

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
}
