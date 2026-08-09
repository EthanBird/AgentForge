//! Read-only Control Room query API and resumable projection notifications.

use std::{convert::Infallible, sync::Arc, time::Duration};

use agentforge_application::ProjectReadModels;
use agentforge_domain::ProjectId;
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
use tower_http::{set_header::SetResponseHeaderLayer, trace::TraceLayer};

use crate::{
    access::{ActorContext, LocalProjectAuthorizer, ProjectAuthorizer},
    control_room::{
        DurableChangeSequence, ProjectionChange, ProjectionRegistry, ProjectionSource,
        ProjectionSourceError, StoreEpoch, StorePosition, next_change,
    },
};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_CSS: &str = include_str!("../assets/app.css");
const APP_JS: &str = include_str!("../assets/app.js");
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
    pub fn encode(&self, project_id: ProjectId, position: StorePosition) -> String {
        let payload = format!(
            "v2.{project_id}.{}.{}",
            position.epoch.get(),
            position.sequence.get()
        );
        let mut mac =
            HmacSha256::new_from_slice(self.key.as_ref()).expect("HMAC key length is valid");
        mac.update(payload.as_bytes());
        format!("{payload}.{}", hex::encode(mac.finalize().into_bytes()))
    }

    fn decode(&self, value: &str, expected_project: ProjectId) -> Result<StorePosition, UiError> {
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
        if fields.next() != Some("v2") {
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
        let epoch = StoreEpoch::new(
            fields
                .next()
                .ok_or(UiError::InvalidCursor)?
                .parse()
                .map_err(|_| UiError::InvalidCursor)?,
        );
        let sequence = DurableChangeSequence::new(
            fields
                .next()
                .ok_or(UiError::InvalidCursor)?
                .parse::<u64>()
                .map_err(|_| UiError::InvalidCursor)?,
        );
        if fields.next().is_some() {
            return Err(UiError::InvalidCursor);
        }
        Ok(StorePosition::new(epoch, sequence))
    }
}

#[derive(Clone)]
pub struct ControlPlaneState {
    source: Arc<dyn ProjectionSource>,
    cursor: CursorCodec,
    actor: ActorContext,
    authorizer: Option<Arc<dyn ProjectAuthorizer>>,
}

impl ControlPlaneState {
    /// Builds the explicit loopback reference composition and returns its write
    /// handle separately. HTTP handlers retain only the `ProjectionSource` trait.
    pub fn local_reference(
        cursor: CursorCodec,
        allowed_projects: impl IntoIterator<Item = ProjectId>,
    ) -> anyhow::Result<(Self, ProjectionRegistry)> {
        let authorizer = LocalProjectAuthorizer::new(allowed_projects)?;
        let store = ProjectionRegistry::new();
        let state = Self::with_source(
            Arc::new(store.clone()),
            cursor,
            ActorContext::local_reference(),
            Some(Arc::new(authorizer)),
        );
        Ok((state, store))
    }

    #[must_use]
    pub fn with_source(
        source: Arc<dyn ProjectionSource>,
        cursor: CursorCodec,
        actor: ActorContext,
        authorizer: Option<Arc<dyn ProjectAuthorizer>>,
    ) -> Self {
        Self {
            source,
            cursor,
            actor,
            authorizer,
        }
    }

    async fn authorize_project(&self, project_id: ProjectId) -> Result<(), UiError> {
        let authorizer = self.authorizer.as_ref().ok_or(UiError::AccessDenied)?;
        authorizer
            .authorize(&self.actor, project_id)
            .await
            .map_err(|_| UiError::AccessDenied)
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
    match state.source.ready().await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
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
    state.authorize_project(project_id).await?;
    let snapshot = state
        .source
        .snapshot(project_id)
        .await
        .map_err(UiError::from_source)?
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
    state.authorize_project(project_id).await?;
    let reconnect_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok());
    let resume = reconnect_cursor
        .or(query.cursor.as_deref())
        .map(|cursor| state.cursor.decode(cursor, project_id))
        .transpose()?;
    let mut subscription = state
        .source
        .subscribe(project_id, resume)
        .await
        .map_err(UiError::from_source)?;
    let codec = state.cursor.clone();
    let stream = async_stream::stream! {
        while let Some(change) = next_change(&mut subscription.changes).await {
            match change {
                Ok(change) => yield Ok(change_event(&codec, &change)),
                Err(ProjectionSourceError::ChangeFeedLagged) => {
                    yield Ok(Event::default().event("projection.reset").data("{}"));
                    break;
                }
                Err(ProjectionSourceError::CursorExpired | ProjectionSourceError::Unavailable) => {
                    yield Ok(Event::default().event("projection.reset").data("{}"));
                    break;
                }
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
        .id(codec.encode(change.project_id, change.position()))
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
    AccessDenied,
    SourceUnavailable,
}

impl UiError {
    const fn from_source(error: ProjectionSourceError) -> Self {
        match error {
            ProjectionSourceError::CursorExpired => Self::CursorExpired,
            ProjectionSourceError::ChangeFeedLagged | ProjectionSourceError::Unavailable => {
                Self::SourceUnavailable
            }
        }
    }
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
            Self::AccessDenied => (
                StatusCode::FORBIDDEN,
                "AF_PROJECT_ACCESS_DENIED",
                "the request actor is not authorized for this project",
            ),
            Self::SourceUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "AF_PROJECTION_SOURCE_UNAVAILABLE",
                "the Control Room projection source is unavailable",
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
    use std::{str::FromStr, sync::Arc};

    use agentforge_application::{
        ProjectReadModels, ProjectionCursor, ProjectionEnvelope, ProjectionHeader, StoredProjection,
    };
    use agentforge_domain::{EventId, ProjectId, ServerInstant, Sha256Digest};
    use axum::{
        extract::{Path, Query, State},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
    };
    use time::{Duration, macros::datetime};
    use uuid::Uuid;

    use crate::{
        access::{ActorContext, LocalProjectAuthorizer, NonLocalActor},
        control_room::{
            ControlRoomSnapshot, DurableChangeSequence, ProjectionRegistry, ProjectionSource,
            ProjectionSourceError, ProjectionSubscription, SourceFuture, StoreEpoch, StorePosition,
        },
    };

    use super::{
        ControlPlaneState, CursorCodec, EventsQuery, events, is_loopback_authority,
        mission_control, same_authority,
    };

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

    struct FixedProjectionSource {
        project_id: ProjectId,
        snapshot: ControlRoomSnapshot,
    }

    impl ProjectionSource for FixedProjectionSource {
        fn snapshot(&self, project_id: ProjectId) -> SourceFuture<'_, Option<ControlRoomSnapshot>> {
            Box::pin(
                async move { Ok((project_id == self.project_id).then(|| self.snapshot.clone())) },
            )
        }

        fn subscribe(
            &self,
            _project_id: ProjectId,
            _resume: Option<StorePosition>,
        ) -> SourceFuture<'_, ProjectionSubscription> {
            Box::pin(async { Err(ProjectionSourceError::Unavailable) })
        }

        fn ready(&self) -> SourceFuture<'_, bool> {
            Box::pin(async { Ok(true) })
        }
    }

    #[test]
    fn cursor_is_project_bound_and_tamper_evident() {
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let first = project("018f0000-0000-7000-8000-000000000001");
        let second = project("018f0000-0000-7000-8000-000000000002");
        let position = StorePosition::new(
            StoreEpoch::new(Uuid::from_bytes([3; 16])),
            DurableChangeSequence::new(37),
        );
        let token = codec.encode(first, position);
        assert_eq!(codec.decode(&token, first).expect("decode"), position);
        assert!(codec.decode(&token, second).is_err());
        let mut tampered = token;
        tampered.push('0');
        assert!(codec.decode(&tampered, first).is_err());
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
    async fn http_snapshot_uses_projection_source_and_local_authorizer() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let checkpoint = checkpoint(project_id, 1);
        let source = FixedProjectionSource {
            project_id,
            snapshot: ControlRoomSnapshot {
                position: StorePosition::new(
                    StoreEpoch::new(Uuid::from_bytes([7; 16])),
                    DurableChangeSequence::new(9),
                ),
                projection_version: 4,
                source_digest: checkpoint.payload_digest,
                source_cursor: checkpoint.cursor,
                read_models: checkpoint.payload,
            },
        };
        let state = ControlPlaneState::with_source(
            Arc::new(source),
            CursorCodec::from_hex(&"11".repeat(32)).expect("valid key"),
            ActorContext::local_reference(),
            Some(Arc::new(
                LocalProjectAuthorizer::new([project_id]).expect("local allowlist"),
            )),
        );

        let response =
            mission_control(State(state), Path(project_id.to_string()), HeaderMap::new())
                .await
                .expect("authorized snapshot");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("etag"));
    }

    #[tokio::test]
    async fn sse_rejects_cross_project_tampered_and_expired_cursors() {
        let first = project("018f0000-0000-7000-8000-000000000001");
        let second = project("018f0000-0000-7000-8000-000000000002");
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let stale_position = StorePosition::new(
            StoreEpoch::new(Uuid::from_bytes([3; 16])),
            DurableChangeSequence::new(1),
        );
        let first_cursor = codec.encode(first, stale_position);
        let (state, _store) =
            ControlPlaneState::local_reference(codec, [first, second]).expect("local state");

        let cross_project = events(
            State(state.clone()),
            Path(second.to_string()),
            HeaderMap::new(),
            Query(EventsQuery {
                cursor: Some(first_cursor.clone()),
            }),
        )
        .await;
        let error = match cross_project {
            Ok(_) => panic!("cross-project cursor must fail"),
            Err(error) => error,
        };
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);

        let mut tampered_cursor = first_cursor.clone();
        tampered_cursor.push('0');
        let tampered = events(
            State(state.clone()),
            Path(first.to_string()),
            HeaderMap::new(),
            Query(EventsQuery {
                cursor: Some(tampered_cursor),
            }),
        )
        .await;
        let error = match tampered {
            Ok(_) => panic!("tampered cursor must fail"),
            Err(error) => error,
        };
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);

        let expired = events(
            State(state),
            Path(first.to_string()),
            HeaderMap::new(),
            Query(EventsQuery {
                cursor: Some(first_cursor),
            }),
        )
        .await;
        let error = match expired {
            Ok(_) => panic!("a cursor from a previous process epoch must fail"),
            Err(error) => error,
        };
        assert_eq!(error.into_response().status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn sse_without_a_non_local_authorizer_fails_closed() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let state = ControlPlaneState::with_source(
            Arc::new(ProjectionRegistry::new()),
            CursorCodec::from_hex(&"11".repeat(32)).expect("valid key"),
            ActorContext::NonLocal(
                NonLocalActor::new("test-subject").expect("typed non-local actor"),
            ),
            None,
        );
        let response = events(
            State(state),
            Path(project_id.to_string()),
            HeaderMap::new(),
            Query(EventsQuery { cursor: None }),
        )
        .await;
        let error = match response {
            Ok(_) => panic!("missing authorizer must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn sse_denied_by_the_local_project_authorizer_fails_closed() {
        let allowed = project("018f0000-0000-7000-8000-000000000001");
        let denied = project("018f0000-0000-7000-8000-000000000002");
        let (state, _store) = ControlPlaneState::local_reference(
            CursorCodec::from_hex(&"11".repeat(32)).expect("valid key"),
            [allowed],
        )
        .expect("local reference");
        let response = events(
            State(state),
            Path(denied.to_string()),
            HeaderMap::new(),
            Query(EventsQuery { cursor: None }),
        )
        .await;
        let error = match response {
            Ok(_) => panic!("project allowlist denial must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
    }
}
