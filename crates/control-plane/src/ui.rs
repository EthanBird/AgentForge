//! Read-only Control Room query API and resumable projection notifications.

use std::{convert::Infallible, sync::Arc, time::Duration};

use agentforge_application::{MvpControlPlane, ProjectReadModels, ProjectionEnvelope};
use agentforge_domain::{ActorId, ProjectId, Sha256Digest};
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
    access::{
        ActorContext, AuthorizationGrant, LocalLoopbackActorExtractor, LocalProjectAuthorizer,
        ProjectAuthorizer, RequestActorExtractor, server_now,
    },
    control_room::{
        DurableChangeSequence, ProjectionChange, ProjectionRegistry, ProjectionSource,
        ProjectionSourceError, StoreEpoch, StorePosition, next_change,
    },
};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_CSS: &str = include_str!("../assets/app.css");
const APP_JS: &str = include_str!("../assets/app.js");
const STREAM_CURSOR_HEADER: &str = "x-agentforge-stream-cursor";
const AUTHORIZATION_RECHECK_INTERVAL: Duration = Duration::from_secs(10);
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
    pub fn encode(
        &self,
        project_id: ProjectId,
        position: StorePosition,
        grant: &AuthorizationGrant,
    ) -> String {
        let authorization_binding = authorization_binding_digest(grant);
        let payload = format!(
            "v3.{project_id}.{}.{}.{}.{}.{}",
            position.epoch.get(),
            position.sequence.get(),
            authorization_binding,
            grant.authorization_epoch,
            grant.expires_at.0.unix_timestamp(),
        );
        let mut mac =
            HmacSha256::new_from_slice(self.key.as_ref()).expect("HMAC key length is valid");
        mac.update(payload.as_bytes());
        format!("{payload}.{}", hex::encode(mac.finalize().into_bytes()))
    }

    fn decode(
        &self,
        value: &str,
        expected_project: ProjectId,
        grant: &AuthorizationGrant,
    ) -> Result<StorePosition, UiError> {
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
        if fields.next() != Some("v3") {
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
        let scope_digest = fields
            .next()
            .ok_or(UiError::InvalidCursor)?
            .parse::<Sha256Digest>()
            .map_err(|_| UiError::InvalidCursor)?;
        let authorization_epoch = fields
            .next()
            .ok_or(UiError::InvalidCursor)?
            .parse::<uuid::Uuid>()
            .map_err(|_| UiError::InvalidCursor)?;
        let expires_at = fields
            .next()
            .ok_or(UiError::InvalidCursor)?
            .parse::<i64>()
            .map_err(|_| UiError::InvalidCursor)?;
        if fields.next().is_some() {
            return Err(UiError::InvalidCursor);
        }
        if scope_digest != authorization_binding_digest(grant)
            || authorization_epoch != grant.authorization_epoch
        {
            return Err(UiError::InvalidCursor);
        }
        if expires_at > grant.expires_at.0.unix_timestamp()
            || server_now().0.unix_timestamp() >= expires_at
        {
            return Err(UiError::CursorExpired);
        }
        Ok(StorePosition::new(epoch, sequence))
    }
}

fn authorization_binding_digest(grant: &AuthorizationGrant) -> Sha256Digest {
    Sha256Digest::of_bytes(format!(
        "control-room-cursor-scope:v1\n{}\n{}\n{}\n{}\n{}",
        grant.tenant.as_str(),
        grant.actor.as_str(),
        grant.project_id,
        grant.scope_digest,
        grant.authorization_epoch,
    ))
}

#[derive(Clone)]
pub struct ControlPlaneState {
    source: Arc<dyn ProjectionSource>,
    cursor: CursorCodec,
    actor_extractor: Option<Arc<dyn RequestActorExtractor>>,
    authorizer: Option<Arc<dyn ProjectAuthorizer>>,
    commands: Option<Arc<dyn MvpControlPlane>>,
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
            Some(Arc::new(LocalLoopbackActorExtractor)),
            Some(Arc::new(authorizer)),
        );
        Ok((state, store))
    }

    #[must_use]
    pub fn with_source(
        source: Arc<dyn ProjectionSource>,
        cursor: CursorCodec,
        actor_extractor: Option<Arc<dyn RequestActorExtractor>>,
        authorizer: Option<Arc<dyn ProjectAuthorizer>>,
    ) -> Self {
        Self {
            source,
            cursor,
            actor_extractor,
            authorizer,
            commands: None,
        }
    }

    #[must_use]
    pub fn with_commands(mut self, commands: Arc<dyn MvpControlPlane>) -> Self {
        self.commands = Some(commands);
        self
    }

    pub(crate) fn command_service(&self) -> Option<Arc<dyn MvpControlPlane>> {
        self.commands.clone()
    }

    pub(crate) async fn authorize_command_actor(
        &self,
        headers: &HeaderMap,
        project_id: ProjectId,
    ) -> Result<ActorId, ()> {
        let authorization = self
            .authorize_project(headers, project_id)
            .await
            .map_err(|_| ())?;
        let material = format!(
            "agentforge-http-actor:v1\n{}\n{}",
            authorization.actor.tenant().as_str(),
            authorization.actor.actor().as_str(),
        );
        let digest = Sha256Digest::of_bytes(material);
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(ActorId::from_uuid(uuid::Uuid::from_bytes(bytes)))
    }

    async fn authorize_project(
        &self,
        headers: &HeaderMap,
        project_id: ProjectId,
    ) -> Result<RequestAuthorization, UiError> {
        let extractor = self.actor_extractor.as_ref().ok_or(UiError::AccessDenied)?;
        let authorizer = self.authorizer.as_ref().ok_or(UiError::AccessDenied)?;
        let actor = extractor
            .extract(headers)
            .await
            .map_err(|_| UiError::AccessDenied)?;
        let grant = authorizer
            .authorize(&actor, project_id)
            .await
            .map_err(|_| UiError::AccessDenied)?;
        if !grant.valid_for(&actor, project_id, server_now()) {
            return Err(UiError::AccessDenied);
        }
        Ok(RequestAuthorization { actor, grant })
    }
}

struct RequestAuthorization {
    actor: ActorContext,
    grant: AuthorizationGrant,
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
        .merge(crate::mvp_api::routes())
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

pub(crate) async fn ready(State(state): State<ControlPlaneState>) -> StatusCode {
    let projections_ready = matches!(state.source.ready().await, Ok(true));
    let commands_ready = match state.command_service() {
        Some(commands) => matches!(commands.ready().await, Ok(true)),
        None => false,
    };
    if projections_ready && commands_ready {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
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
    let authorization = state.authorize_project(&headers, project_id).await?;
    let head_before = state.source.head().await.map_err(UiError::from_source)?;
    let snapshot = state
        .source
        .snapshot(project_id)
        .await
        .map_err(UiError::from_source)?
        .ok_or(UiError::ProjectNotFound)?;
    let head_after = state.source.head().await.map_err(UiError::from_source)?;
    validate_source_snapshot(project_id, &snapshot, head_before, head_after)?;
    let etag = format!("\"{}\"", snapshot.source_digest);
    let stream_cursor = state
        .cursor
        .encode(project_id, snapshot.position, &authorization.grant);
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        == Some(etag.as_str())
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        attach_snapshot_headers(&mut response, &etag, &stream_cursor)?;
        return Ok(response);
    }
    let mut response = Json(MissionControlResponse {
        projection_version: snapshot.projection_version,
        source_cursor: snapshot.source_cursor,
        read_models: snapshot.read_models,
    })
    .into_response();
    attach_snapshot_headers(&mut response, &etag, &stream_cursor)?;
    Ok(response)
}

fn validate_source_snapshot(
    project_id: ProjectId,
    snapshot: &crate::control_room::ControlRoomSnapshot,
    head_before: StorePosition,
    head_after: StorePosition,
) -> Result<(), UiError> {
    let header = snapshot.read_models.header();
    let canonical_payload_digest = ProjectionEnvelope::new(
        project_id,
        snapshot.source_cursor,
        snapshot.read_models.clone(),
    )
    .map_err(|_| UiError::SourceContractInvalid)?
    .payload_digest;
    let position_is_monotonic = head_before.epoch == snapshot.position.epoch
        && snapshot.position.epoch == head_after.epoch
        && head_before.sequence <= snapshot.position.sequence
        && snapshot.position.sequence <= head_after.sequence;
    let cursor_matches_headers = snapshot.source_cursor.event_sequence != 0
        && header.last_event_id == Some(snapshot.source_cursor.event_id)
        && header.last_event_sequence == snapshot.source_cursor.event_sequence
        && header.as_of == Some(snapshot.source_cursor.occurred_at);
    if snapshot.project_id != project_id
        || snapshot.projection_version == 0
        || snapshot.position.sequence == DurableChangeSequence::ZERO
        || !position_is_monotonic
        || !snapshot.read_models.is_project_consistent(project_id)
        || !snapshot.read_models.checkpoint_invariants_hold()
        || !cursor_matches_headers
        || snapshot.source_digest != canonical_payload_digest
    {
        return Err(UiError::SourceContractInvalid);
    }
    Ok(())
}

fn attach_snapshot_headers(
    response: &mut Response,
    etag: &str,
    stream_cursor: &str,
) -> Result<(), UiError> {
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(etag).map_err(|_| UiError::SourceContractInvalid)?,
    );
    response.headers_mut().insert(
        HeaderName::from_static(STREAM_CURSOR_HEADER),
        HeaderValue::from_str(stream_cursor).map_err(|_| UiError::SourceContractInvalid)?,
    );
    Ok(())
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
    let authorization = state.authorize_project(&headers, project_id).await?;
    let reconnect_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok());
    let cursor = reconnect_cursor
        .or(query.cursor.as_deref())
        .ok_or(UiError::CursorRequired)?;
    let resume = state
        .cursor
        .decode(cursor, project_id, &authorization.grant)?;
    let mut subscription = state
        .source
        .subscribe(project_id, resume)
        .await
        .map_err(UiError::from_source)?;
    let subscription_contract_is_valid = subscription.project_id == project_id
        && subscription.epoch == resume.epoch
        && subscription.after == resume.sequence;
    let codec = state.cursor.clone();
    let actor = authorization.actor;
    let grant = authorization.grant;
    let authorizer = state.authorizer.clone().ok_or(UiError::AccessDenied)?;
    let stream = async_stream::stream! {
        if !subscription_contract_is_valid {
            yield Ok(projection_reset_event("source_contract_failure"));
        } else {
            let mut last_sequence = resume.sequence;
            loop {
                tokio::select! {
                    () = tokio::time::sleep(AUTHORIZATION_RECHECK_INTERVAL) => {
                        if refresh_authorization(
                            authorizer.as_ref(),
                            &actor,
                            project_id,
                            &grant,
                        ).await.is_none() {
                            break;
                        }
                    }
                    change = next_change(&mut subscription.changes) => {
                        let Some(change) = change else {
                            break;
                        };
                        match change {
                            Ok(change) => {
                                let source_is_valid = change.project_id == project_id
                                    && change.epoch == resume.epoch
                                    && change.sequence > last_sequence;
                                if !source_is_valid {
                                    yield Ok(projection_reset_event("source_contract_failure"));
                                    break;
                                }
                                let Some(current_grant) = refresh_authorization(
                                    authorizer.as_ref(),
                                    &actor,
                                    project_id,
                                    &grant,
                                ).await else {
                                    break;
                                };
                                // Authorization can expire while an async
                                // authorizer is resolving. Re-read the clock and
                                // compare the epoch/scope immediately before
                                // emitting protected data.
                                if !grants_allow_emit(
                                    &grant,
                                    &current_grant,
                                    &actor,
                                    project_id,
                                    server_now(),
                                ) {
                                    break;
                                }
                                last_sequence = change.sequence;
                                yield Ok(change_event(&codec, &change, &current_grant));
                            }
                            Err(ProjectionSourceError::ChangeFeedLagged) => {
                                yield Ok(projection_reset_event("change_feed_lagged"));
                                break;
                            }
                            Err(ProjectionSourceError::CursorExpired) => {
                                yield Ok(projection_reset_event("cursor_expired"));
                                break;
                            }
                            Err(ProjectionSourceError::Unavailable) => break,
                        }
                    }
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

async fn refresh_authorization(
    authorizer: &dyn ProjectAuthorizer,
    actor: &ActorContext,
    project_id: ProjectId,
    initial: &AuthorizationGrant,
) -> Option<AuthorizationGrant> {
    let current = authorizer.authorize(actor, project_id).await.ok()?;
    grants_allow_emit(initial, &current, actor, project_id, server_now()).then_some(current)
}

fn grants_allow_emit(
    initial: &AuthorizationGrant,
    current: &AuthorizationGrant,
    actor: &ActorContext,
    project_id: ProjectId,
    now: agentforge_domain::ServerInstant,
) -> bool {
    initial.valid_for(actor, project_id, now)
        && current.valid_for(actor, project_id, now)
        && initial.same_authority(current)
}

fn projection_reset_event(reason: &'static str) -> Event {
    Event::default()
        .event("projection.reset")
        .data(serde_json::json!({ "reason": reason }).to_string())
}

fn change_event(
    codec: &CursorCodec,
    change: &ProjectionChange,
    grant: &AuthorizationGrant,
) -> Event {
    let data = serde_json::to_string(change).expect("projection change is serializable");
    Event::default()
        .event("projection.invalidated")
        .id(codec.encode(change.project_id, change.position(), grant))
        .data(data)
}

fn parse_project(value: &str) -> Result<ProjectId, UiError> {
    value.parse().map_err(|_| UiError::InvalidProjectId)
}

#[derive(Clone, Copy, Debug)]
enum UiError {
    InvalidProjectId,
    CursorRequired,
    InvalidCursor,
    CursorExpired,
    ProjectNotFound,
    AccessDenied,
    SourceUnavailable,
    SourceContractInvalid,
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
            Self::CursorRequired => (
                StatusCode::BAD_REQUEST,
                "AF_CURSOR_REQUIRED",
                "a snapshot stream cursor is required",
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
            Self::SourceContractInvalid => (
                StatusCode::SERVICE_UNAVAILABLE,
                "AF_PROJECTION_SOURCE_CONTRACT_INVALID",
                "the Control Room projection source returned inconsistent data",
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
    use std::{
        str::FromStr,
        sync::{Arc, RwLock},
    };

    use agentforge_application::{
        ProjectReadModels, ProjectionCursor, ProjectionEnvelope, ProjectionHeader, StoredProjection,
    };
    use agentforge_domain::{EventId, ProjectId, ServerInstant, Sha256Digest};
    use axum::{
        body::to_bytes,
        extract::{Path, Query, State},
        http::{HeaderMap, HeaderValue, StatusCode, header},
        response::IntoResponse,
    };
    use time::{Duration, OffsetDateTime, macros::datetime};
    use uuid::Uuid;

    use crate::{
        access::{
            ActorContext, ActorExtractionFuture, AuthenticationError, AuthorizationFuture,
            AuthorizationGrant, LocalLoopbackActorExtractor, NonLocalActor, ProjectAuthorizer,
            RequestActorExtractor, server_now,
        },
        control_room::{
            ControlRoomSnapshot, DurableChangeSequence, ProjectionChange, ProjectionRegistry,
            ProjectionResource, ProjectionSource, ProjectionSourceError, ProjectionSubscription,
            SourceFuture, StoreEpoch, StorePosition,
        },
    };

    use super::{
        ControlPlaneState, CursorCodec, EventsQuery, STREAM_CURSOR_HEADER, events,
        is_loopback_authority, mission_control, refresh_authorization, same_authority,
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
        head_position: StorePosition,
        snapshot: ControlRoomSnapshot,
    }

    impl ProjectionSource for FixedProjectionSource {
        fn head(&self) -> SourceFuture<'_, StorePosition> {
            Box::pin(async { Ok(self.head_position) })
        }

        fn snapshot(&self, project_id: ProjectId) -> SourceFuture<'_, Option<ControlRoomSnapshot>> {
            Box::pin(
                async move { Ok((project_id == self.project_id).then(|| self.snapshot.clone())) },
            )
        }

        fn subscribe(
            &self,
            _project_id: ProjectId,
            _resume: StorePosition,
        ) -> SourceFuture<'_, ProjectionSubscription> {
            Box::pin(async { Err(ProjectionSourceError::Unavailable) })
        }

        fn ready(&self) -> SourceFuture<'_, bool> {
            Box::pin(async { Ok(true) })
        }
    }

    struct MalformedSubscriptionSource {
        position: StorePosition,
        wrong_project_id: ProjectId,
    }

    impl ProjectionSource for MalformedSubscriptionSource {
        fn head(&self) -> SourceFuture<'_, StorePosition> {
            Box::pin(async { Ok(self.position) })
        }

        fn snapshot(
            &self,
            _project_id: ProjectId,
        ) -> SourceFuture<'_, Option<ControlRoomSnapshot>> {
            Box::pin(async { Ok(None) })
        }

        fn subscribe(
            &self,
            _project_id: ProjectId,
            resume: StorePosition,
        ) -> SourceFuture<'_, ProjectionSubscription> {
            Box::pin(async move {
                let changes = async_stream::stream! {
                    if false {
                        yield Err(ProjectionSourceError::Unavailable);
                    }
                };
                Ok(ProjectionSubscription {
                    project_id: self.wrong_project_id,
                    epoch: resume.epoch,
                    after: resume.sequence,
                    changes: Box::pin(changes),
                })
            })
        }

        fn ready(&self) -> SourceFuture<'_, bool> {
            Box::pin(async { Ok(true) })
        }
    }

    struct MalformedChangeSource {
        position: StorePosition,
        change_project_id: ProjectId,
        change_epoch: StoreEpoch,
        change_sequence: DurableChangeSequence,
    }

    impl ProjectionSource for MalformedChangeSource {
        fn head(&self) -> SourceFuture<'_, StorePosition> {
            Box::pin(async { Ok(self.position) })
        }

        fn snapshot(
            &self,
            _project_id: ProjectId,
        ) -> SourceFuture<'_, Option<ControlRoomSnapshot>> {
            Box::pin(async { Ok(None) })
        }

        fn subscribe(
            &self,
            project_id: ProjectId,
            resume: StorePosition,
        ) -> SourceFuture<'_, ProjectionSubscription> {
            Box::pin(async move {
                let change = ProjectionChange {
                    epoch: self.change_epoch,
                    sequence: self.change_sequence,
                    resource: ProjectionResource::ProjectReadModels,
                    project_id: self.change_project_id,
                    projection_version: 2,
                    source_occurred_at: ServerInstant(datetime!(2026-08-10 00:00 UTC)),
                };
                let changes = async_stream::stream! {
                    yield Ok(change);
                };
                Ok(ProjectionSubscription {
                    project_id,
                    epoch: resume.epoch,
                    after: resume.sequence,
                    changes: Box::pin(changes),
                })
            })
        }

        fn ready(&self) -> SourceFuture<'_, bool> {
            Box::pin(async { Ok(true) })
        }
    }

    struct DelayedAuthorizer {
        grant: AuthorizationGrant,
        delay: std::time::Duration,
    }

    impl ProjectAuthorizer for DelayedAuthorizer {
        fn authorize<'a>(
            &'a self,
            _actor: &'a ActorContext,
            _project_id: ProjectId,
        ) -> AuthorizationFuture<'a> {
            Box::pin(async move {
                tokio::time::sleep(self.delay).await;
                Ok(self.grant.clone())
            })
        }
    }

    #[derive(Clone)]
    struct TestAuthorizer {
        epoch: Arc<RwLock<Uuid>>,
        expires_at: ServerInstant,
    }

    impl TestAuthorizer {
        fn active() -> Self {
            Self {
                epoch: Arc::new(RwLock::new(Uuid::from_bytes([8; 16]))),
                expires_at: ServerInstant(
                    OffsetDateTime::from_unix_timestamp(253_402_300_799)
                        .expect("year 9999 is representable"),
                ),
            }
        }

        fn grant(&self, actor: &ActorContext, project_id: ProjectId) -> AuthorizationGrant {
            let tenant = actor.tenant();
            let actor_identity = actor.actor();
            AuthorizationGrant {
                project_id,
                scope_digest: Sha256Digest::of_bytes(format!("test-scope:{project_id}")),
                tenant,
                actor: actor_identity,
                authorization_epoch: *self.epoch.read().expect("authorization epoch lock"),
                expires_at: self.expires_at,
            }
        }

        fn rotate_epoch(&self) {
            *self.epoch.write().expect("authorization epoch lock") = Uuid::from_bytes([9; 16]);
        }
    }

    impl ProjectAuthorizer for TestAuthorizer {
        fn authorize<'a>(
            &'a self,
            actor: &'a ActorContext,
            project_id: ProjectId,
        ) -> AuthorizationFuture<'a> {
            Box::pin(async move { Ok(self.grant(actor, project_id)) })
        }
    }

    struct HeaderActorExtractor;

    impl RequestActorExtractor for HeaderActorExtractor {
        fn extract<'a>(&'a self, headers: &'a HeaderMap) -> ActorExtractionFuture<'a> {
            let result = headers
                .get("x-test-tenant")
                .and_then(|value| value.to_str().ok())
                .zip(
                    headers
                        .get("x-test-actor")
                        .and_then(|value| value.to_str().ok()),
                )
                .ok_or(AuthenticationError::InvalidCredentials)
                .and_then(|(tenant, actor)| {
                    NonLocalActor::new(tenant.to_owned(), actor.to_owned())
                        .map(ActorContext::NonLocal)
                        .map_err(|_| AuthenticationError::InvalidCredentials)
                });
            Box::pin(async move { result })
        }
    }

    fn test_state(
        source: Arc<dyn ProjectionSource>,
        cursor: CursorCodec,
        authorizer: Arc<dyn ProjectAuthorizer>,
    ) -> ControlPlaneState {
        ControlPlaneState::with_source(
            source,
            cursor,
            Some(Arc::new(LocalLoopbackActorExtractor)),
            Some(authorizer),
        )
    }

    #[test]
    fn cursor_is_actor_scoped_project_bound_tamper_evident_and_expiring() {
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let first = project("018f0000-0000-7000-8000-000000000001");
        let second = project("018f0000-0000-7000-8000-000000000002");
        let actor = ActorContext::local_reference();
        let authorizer = TestAuthorizer::active();
        let first_grant = authorizer.grant(&actor, first);
        let position = StorePosition::new(
            StoreEpoch::new(Uuid::from_bytes([3; 16])),
            DurableChangeSequence::new(37),
        );
        let token = codec.encode(first, position, &first_grant);
        assert_eq!(
            codec.decode(&token, first, &first_grant).expect("decode"),
            position
        );
        assert!(
            codec
                .decode(&token, second, &authorizer.grant(&actor, second))
                .is_err()
        );
        authorizer.rotate_epoch();
        assert!(
            codec
                .decode(&token, first, &authorizer.grant(&actor, first))
                .is_err()
        );
        let mut tampered = token;
        tampered.push('0');
        assert!(codec.decode(&tampered, first, &first_grant).is_err());

        let mut expired_grant = first_grant.clone();
        expired_grant.expires_at = ServerInstant(OffsetDateTime::UNIX_EPOCH);
        let expired = codec.encode(first, position, &expired_grant);
        assert!(codec.decode(&expired, first, &expired_grant).is_err());
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
        let position = StorePosition::new(
            StoreEpoch::new(Uuid::from_bytes([7; 16])),
            DurableChangeSequence::new(9),
        );
        let source = FixedProjectionSource {
            project_id,
            head_position: position,
            snapshot: ControlRoomSnapshot {
                project_id,
                position,
                projection_version: 4,
                source_digest: checkpoint.payload_digest,
                source_cursor: checkpoint.cursor,
                read_models: checkpoint.payload,
            },
        };
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let authorizer = TestAuthorizer::active();
        let state = test_state(
            Arc::new(source),
            codec.clone(),
            Arc::new(authorizer.clone()),
        );

        let response = mission_control(
            State(state.clone()),
            Path(project_id.to_string()),
            HeaderMap::new(),
        )
        .await
        .expect("authorized snapshot");
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response.headers().get(header::ETAG).expect("ETag").clone();
        let stream_cursor = response
            .headers()
            .get(STREAM_CURSOR_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("snapshot stream cursor");
        assert_eq!(
            codec
                .decode(
                    stream_cursor,
                    project_id,
                    &authorizer.grant(&ActorContext::local_reference(), project_id),
                )
                .expect("decode snapshot handoff"),
            position
        );

        let mut conditional = HeaderMap::new();
        conditional.insert(header::IF_NONE_MATCH, etag);
        let not_modified = mission_control(State(state), Path(project_id.to_string()), conditional)
            .await
            .expect("conditional snapshot");
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
        assert!(not_modified.headers().contains_key(STREAM_CURSOR_HEADER));
    }

    #[tokio::test]
    async fn request_headers_produce_distinct_actor_scoped_snapshot_cursors() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let checkpoint = checkpoint(project_id, 1);
        let position = StorePosition::new(
            StoreEpoch::new(Uuid::from_bytes([7; 16])),
            DurableChangeSequence::new(9),
        );
        let source = FixedProjectionSource {
            project_id,
            head_position: position,
            snapshot: ControlRoomSnapshot {
                project_id,
                position,
                projection_version: 1,
                source_digest: checkpoint.payload_digest,
                source_cursor: checkpoint.cursor,
                read_models: checkpoint.payload,
            },
        };
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let authorizer = TestAuthorizer::active();
        let state = ControlPlaneState::with_source(
            Arc::new(source),
            codec.clone(),
            Some(Arc::new(HeaderActorExtractor)),
            Some(Arc::new(authorizer.clone())),
        );

        let mut first_headers = HeaderMap::new();
        first_headers.insert("x-test-tenant", HeaderValue::from_static("tenant-a"));
        first_headers.insert("x-test-actor", HeaderValue::from_static("actor-a"));
        let first_response = mission_control(
            State(state.clone()),
            Path(project_id.to_string()),
            first_headers,
        )
        .await
        .expect("first request actor");
        let first_cursor = first_response
            .headers()
            .get(STREAM_CURSOR_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("first stream cursor")
            .to_owned();

        let mut second_headers = HeaderMap::new();
        second_headers.insert("x-test-tenant", HeaderValue::from_static("tenant-a"));
        second_headers.insert("x-test-actor", HeaderValue::from_static("actor-b"));
        let second_response =
            mission_control(State(state), Path(project_id.to_string()), second_headers)
                .await
                .expect("second request actor");
        let second_cursor = second_response
            .headers()
            .get(STREAM_CURSOR_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("second stream cursor");
        assert_ne!(first_cursor, second_cursor);

        let first_actor =
            ActorContext::NonLocal(NonLocalActor::new("tenant-a", "actor-a").expect("first actor"));
        let second_actor = ActorContext::NonLocal(
            NonLocalActor::new("tenant-a", "actor-b").expect("second actor"),
        );
        assert!(
            codec
                .decode(
                    &first_cursor,
                    project_id,
                    &authorizer.grant(&first_actor, project_id),
                )
                .is_ok()
        );
        assert!(
            codec
                .decode(
                    &first_cursor,
                    project_id,
                    &authorizer.grant(&second_actor, project_id),
                )
                .is_err()
        );
    }

    #[tokio::test]
    async fn http_rejects_a_source_snapshot_for_the_wrong_project() {
        let requested = project("018f0000-0000-7000-8000-000000000001");
        let wrong = project("018f0000-0000-7000-8000-000000000002");
        let checkpoint = checkpoint(wrong, 1);
        let source = FixedProjectionSource {
            project_id: requested,
            head_position: StorePosition::new(
                StoreEpoch::new(Uuid::from_bytes([7; 16])),
                DurableChangeSequence::new(1),
            ),
            snapshot: ControlRoomSnapshot {
                project_id: wrong,
                position: StorePosition::new(
                    StoreEpoch::new(Uuid::from_bytes([7; 16])),
                    DurableChangeSequence::new(1),
                ),
                projection_version: 1,
                source_digest: checkpoint.payload_digest,
                source_cursor: checkpoint.cursor,
                read_models: checkpoint.payload,
            },
        };
        let state = test_state(
            Arc::new(source),
            CursorCodec::from_hex(&"11".repeat(32)).expect("valid key"),
            Arc::new(TestAuthorizer::active()),
        );
        let response =
            mission_control(State(state), Path(requested.to_string()), HeaderMap::new()).await;
        let error = response.expect_err("wrong-project snapshot must fail");
        assert_eq!(
            error.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn http_rejects_unverified_snapshot_digest_cursor_and_position() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let checkpoint = checkpoint(project_id, 1);
        let epoch = StoreEpoch::new(Uuid::from_bytes([7; 16]));
        let position = StorePosition::new(epoch, DurableChangeSequence::new(9));
        let valid = ControlRoomSnapshot {
            project_id,
            position,
            projection_version: 1,
            source_digest: checkpoint.payload_digest,
            source_cursor: checkpoint.cursor,
            read_models: checkpoint.payload,
        };
        let mut bad_digest = valid.clone();
        bad_digest.source_digest = Sha256Digest::of_bytes("not the JCS payload");
        let unverified_etag = HeaderValue::from_str(&format!("\"{}\"", bad_digest.source_digest))
            .expect("digest is a valid ETag");
        let mut bad_cursor = valid.clone();
        bad_cursor.source_cursor = ProjectionCursor::new(
            bad_cursor.source_cursor.occurred_at,
            EventId::from(Uuid::from_u128(99)),
            bad_cursor.source_cursor.event_sequence,
        );
        let regressing_position = valid;

        for (snapshot, head_position, conditional_etag) in [
            (bad_digest, position, Some(unverified_etag)),
            (bad_cursor, position, None),
            (
                regressing_position,
                StorePosition::new(epoch, DurableChangeSequence::new(10)),
                None,
            ),
        ] {
            let state = test_state(
                Arc::new(FixedProjectionSource {
                    project_id,
                    head_position,
                    snapshot,
                }),
                CursorCodec::from_hex(&"11".repeat(32)).expect("valid key"),
                Arc::new(TestAuthorizer::active()),
            );
            let mut headers = HeaderMap::new();
            if let Some(etag) = conditional_etag {
                headers.insert(header::IF_NONE_MATCH, etag);
            }
            let response =
                mission_control(State(state), Path(project_id.to_string()), headers).await;
            let error = response.expect_err("unverified snapshots must fail closed");
            assert_eq!(
                error.into_response().status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
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
        let authorizer = TestAuthorizer::active();
        let first_cursor = codec.encode(
            first,
            stale_position,
            &authorizer.grant(&ActorContext::local_reference(), first),
        );
        let state = test_state(
            Arc::new(ProjectionRegistry::new()),
            codec,
            Arc::new(authorizer),
        );

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
    async fn sse_without_request_actor_extractor_or_authorizer_fails_closed() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let state = ControlPlaneState::with_source(
            Arc::new(ProjectionRegistry::new()),
            CursorCodec::from_hex(&"11".repeat(32)).expect("valid key"),
            None,
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
    async fn sse_requires_a_snapshot_handoff_cursor() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let (state, _store) = ControlPlaneState::local_reference(
            CursorCodec::from_hex(&"11".repeat(32)).expect("valid key"),
            [project_id],
        )
        .expect("local reference");
        let response = events(
            State(state),
            Path(project_id.to_string()),
            HeaderMap::new(),
            Query(EventsQuery { cursor: None }),
        )
        .await;
        let error = match response {
            Ok(_) => panic!("cursor-free SSE must not bypass the retained hot floor"),
            Err(error) => error,
        };
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn sse_resets_on_inconsistent_subscription_scope_from_the_source() {
        let requested = project("018f0000-0000-7000-8000-000000000001");
        let wrong = project("018f0000-0000-7000-8000-000000000002");
        let position = StorePosition::new(
            StoreEpoch::new(Uuid::from_bytes([7; 16])),
            DurableChangeSequence::new(4),
        );
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let authorizer = TestAuthorizer::active();
        let cursor = codec.encode(
            requested,
            position,
            &authorizer.grant(&ActorContext::local_reference(), requested),
        );
        let state = test_state(
            Arc::new(MalformedSubscriptionSource {
                position,
                wrong_project_id: wrong,
            }),
            codec,
            Arc::new(authorizer),
        );
        let response = events(
            State(state),
            Path(requested.to_string()),
            HeaderMap::new(),
            Query(EventsQuery {
                cursor: Some(cursor),
            }),
        )
        .await
        .expect("source contract failure is reported in-band")
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1_024)
            .await
            .expect("read finite reset stream");
        let body = std::str::from_utf8(&body).expect("SSE is UTF-8");
        assert!(body.contains("projection.reset"), "{body}");
        assert!(body.contains("source_contract_failure"), "{body}");
    }

    #[tokio::test]
    async fn sse_resets_before_emitting_cross_scope_or_non_monotonic_changes() {
        let requested = project("018f0000-0000-7000-8000-000000000001");
        let wrong = project("018f0000-0000-7000-8000-000000000002");
        let position = StorePosition::new(
            StoreEpoch::new(Uuid::from_bytes([7; 16])),
            DurableChangeSequence::new(4),
        );
        let codec = CursorCodec::from_hex(&"11".repeat(32)).expect("valid key");
        let authorizer = TestAuthorizer::active();
        let cursor = codec.encode(
            requested,
            position,
            &authorizer.grant(&ActorContext::local_reference(), requested),
        );
        let next = DurableChangeSequence::new(5);
        let wrong_epoch = StoreEpoch::new(Uuid::from_bytes([8; 16]));
        for (change_project_id, change_epoch, change_sequence) in [
            (wrong, position.epoch, next),
            (requested, wrong_epoch, next),
            (requested, position.epoch, position.sequence),
        ] {
            let state = test_state(
                Arc::new(MalformedChangeSource {
                    position,
                    change_project_id,
                    change_epoch,
                    change_sequence,
                }),
                codec.clone(),
                Arc::new(authorizer.clone()),
            );
            let response = events(
                State(state),
                Path(requested.to_string()),
                HeaderMap::new(),
                Query(EventsQuery {
                    cursor: Some(cursor.clone()),
                }),
            )
            .await
            .expect("source contract failure is reported in-band")
            .into_response();
            let body = to_bytes(response.into_body(), 1_024)
                .await
                .expect("read finite reset stream");
            let body = std::str::from_utf8(&body).expect("SSE is UTF-8");
            assert!(body.contains("projection.reset"), "{body}");
            assert!(body.contains("source_contract_failure"), "{body}");
            assert!(!body.contains("projection.invalidated"), "{body}");
        }
    }

    #[tokio::test]
    async fn authorization_epoch_rotation_revokes_an_existing_stream_grant() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let actor = ActorContext::local_reference();
        let authorizer = TestAuthorizer::active();
        let initial = authorizer.grant(&actor, project_id);
        assert!(
            refresh_authorization(&authorizer, &actor, project_id, &initial)
                .await
                .is_some()
        );
        authorizer.rotate_epoch();
        assert!(
            refresh_authorization(&authorizer, &actor, project_id, &initial)
                .await
                .is_none()
        );

        let mut expired = initial;
        expired.expires_at = ServerInstant(OffsetDateTime::UNIX_EPOCH);
        assert!(
            refresh_authorization(&authorizer, &actor, project_id, &expired)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn authorization_expiring_during_authorizer_await_is_not_emitted() {
        let project_id = project("018f0000-0000-7000-8000-000000000001");
        let actor = ActorContext::local_reference();
        let mut initial = TestAuthorizer::active().grant(&actor, project_id);
        initial.expires_at = ServerInstant(server_now().0 + Duration::milliseconds(40));
        assert!(initial.valid_for(&actor, project_id, server_now()));
        let delayed = DelayedAuthorizer {
            grant: initial.clone(),
            delay: std::time::Duration::from_millis(80),
        };

        assert!(
            refresh_authorization(&delayed, &actor, project_id, &initial)
                .await
                .is_none()
        );
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
