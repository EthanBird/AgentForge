//! Versioned HTTP command surface for the runnable local MVP.

use agentforge_application::{
    ClaimPackageInput, CreateProjectInput, LeaseView, ListOffersQuery, MvpCommand,
    MvpCommandContext, MvpError, OfferView, ProjectView, PublishPackageInput, PublishedPackage,
    ReleaseLeaseInput, RenewLeaseInput,
};
use agentforge_domain::{
    ActorId, AggregateVersion, CommandId, CorrelationId, EventId, IdempotencyKey, LeaseId,
    PackageId, ProjectId,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ui::ControlPlaneState;

const IDEMPOTENCY_KEY: &str = "idempotency-key";
const COMMAND_ID: &str = "x-agentforge-command-id";
const CORRELATION_ID: &str = "x-agentforge-correlation-id";
const CAUSATION_ID: &str = "x-agentforge-causation-id";

pub(crate) fn routes() -> Router<ControlPlaneState> {
    Router::new()
        .route("/api/v1/projects", post(create_project))
        .route(
            "/api/v1/projects/{project_id}/packages",
            post(publish_package),
        )
        .route("/api/v1/projects/{project_id}/offers", get(list_offers))
        .route(
            "/api/v1/projects/{project_id}/packages/{package_id}/claim",
            post(claim_package),
        )
        .route(
            "/api/v1/projects/{project_id}/leases/{lease_id}",
            get(get_lease),
        )
        .route(
            "/api/v1/projects/{project_id}/leases/{lease_id}/renew",
            post(renew_lease),
        )
        .route(
            "/api/v1/projects/{project_id}/leases/{lease_id}/release",
            post(release_lease),
        )
}

async fn create_project(
    State(state): State<ControlPlaneState>,
    headers: HeaderMap,
    Json(input): Json<CreateProjectInput>,
) -> Result<(StatusCode, Json<ProjectView>), ApiError> {
    let actor = authorize(&state, &headers, input.project_id).await?;
    let command = MvpCommand {
        context: command_context(&headers, actor, false)?,
        input,
    };
    let response = service(&state)?.create_project(&command).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn publish_package(
    State(state): State<ControlPlaneState>,
    Path(project_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<PublishPackageInput>,
) -> Result<(StatusCode, Json<PublishedPackage>), ApiError> {
    let project_id = parse_project(&project_id)?;
    require_same(project_id, input.project_id, "project_id")?;
    let actor = authorize(&state, &headers, project_id).await?;
    let command = MvpCommand {
        context: command_context(&headers, actor, false)?,
        input,
    };
    let response = service(&state)?.publish_package(&command).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct OffersQuery {
    limit: Option<u16>,
}

async fn list_offers(
    State(state): State<ControlPlaneState>,
    Path(project_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<OffersQuery>,
) -> Result<Json<Vec<OfferView>>, ApiError> {
    let project_id = parse_project(&project_id)?;
    authorize(&state, &headers, project_id).await?;
    let response = service(&state)?
        .list_offers(ListOffersQuery {
            project_id,
            limit: query.limit.unwrap_or(50),
        })
        .await?;
    Ok(Json(response))
}

async fn claim_package(
    State(state): State<ControlPlaneState>,
    Path((project_id, package_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<ClaimPackageInput>,
) -> Result<(StatusCode, Json<agentforge_application::ClaimedWork>), ApiError> {
    let project_id = parse_project(&project_id)?;
    let package_id = parse_package(&package_id)?;
    require_same(project_id, input.project_id, "project_id")?;
    require_same(package_id, input.package_id, "package_id")?;
    let actor = authorize(&state, &headers, project_id).await?;
    let command = MvpCommand {
        context: command_context(&headers, actor, true)?,
        input,
    };
    let response = service(&state)?.claim_package(&command).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_lease(
    State(state): State<ControlPlaneState>,
    Path((project_id, lease_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<LeaseView>, ApiError> {
    let project_id = parse_project(&project_id)?;
    let lease_id = parse_lease(&lease_id)?;
    authorize(&state, &headers, project_id).await?;
    Ok(Json(
        service(&state)?.get_lease(project_id, lease_id).await?,
    ))
}

async fn renew_lease(
    State(state): State<ControlPlaneState>,
    Path((project_id, lease_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<RenewLeaseInput>,
) -> Result<Json<LeaseView>, ApiError> {
    let project_id = parse_project(&project_id)?;
    let lease_id = parse_lease(&lease_id)?;
    require_same(project_id, input.project_id, "project_id")?;
    require_same(lease_id, input.lease_id, "lease_id")?;
    let actor = authorize(&state, &headers, project_id).await?;
    let command = MvpCommand {
        context: command_context(&headers, actor, true)?,
        input,
    };
    Ok(Json(service(&state)?.renew_lease(&command).await?))
}

async fn release_lease(
    State(state): State<ControlPlaneState>,
    Path((project_id, lease_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<ReleaseLeaseInput>,
) -> Result<Json<LeaseView>, ApiError> {
    let project_id = parse_project(&project_id)?;
    let lease_id = parse_lease(&lease_id)?;
    require_same(project_id, input.project_id, "project_id")?;
    require_same(lease_id, input.lease_id, "lease_id")?;
    let actor = authorize(&state, &headers, project_id).await?;
    let command = MvpCommand {
        context: command_context(&headers, actor, true)?,
        input,
    };
    Ok(Json(service(&state)?.release_lease(&command).await?))
}

fn service(
    state: &ControlPlaneState,
) -> Result<std::sync::Arc<dyn agentforge_application::MvpControlPlane>, ApiError> {
    state.command_service().ok_or(ApiError::NotConfigured)
}

async fn authorize(
    state: &ControlPlaneState,
    headers: &HeaderMap,
    project_id: ProjectId,
) -> Result<ActorId, ApiError> {
    state
        .authorize_command_actor(headers, project_id)
        .await
        .map_err(|()| ApiError::AccessDenied)
}

fn command_context(
    headers: &HeaderMap,
    actor_id: ActorId,
    version_required: bool,
) -> Result<MvpCommandContext, ApiError> {
    let idempotency_key = required_header(headers, IDEMPOTENCY_KEY)?;
    let expected_version = headers
        .get(header::IF_MATCH)
        .map(|value| {
            let value = value.to_str().map_err(|_| ApiError::InvalidHeader)?;
            let value = value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or(value);
            let version = value
                .parse::<u64>()
                .ok()
                .filter(|version| *version != 0)
                .ok_or(ApiError::InvalidHeader)?;
            Ok::<AggregateVersion, ApiError>(AggregateVersion::new(version))
        })
        .transpose()?;
    if expected_version.is_some() == !version_required {
        return Err(ApiError::InvalidHeader);
    }
    Ok(MvpCommandContext {
        command_id: optional_uuid_header(headers, COMMAND_ID)?
            .map(CommandId::from_uuid)
            .unwrap_or_else(|| CommandId::from_uuid(Uuid::now_v7())),
        actor_id,
        idempotency_key: IdempotencyKey::new(idempotency_key)
            .map_err(|_| ApiError::InvalidHeader)?,
        correlation_id: optional_uuid_header(headers, CORRELATION_ID)?
            .map(CorrelationId::from_uuid)
            .unwrap_or_else(|| CorrelationId::from_uuid(Uuid::now_v7())),
        causation_id: optional_uuid_header(headers, CAUSATION_ID)?.map(EventId::from_uuid),
        expected_version,
    })
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    headers
        .get(name)
        .ok_or(ApiError::InvalidHeader)?
        .to_str()
        .ok()
        .map(str::to_owned)
        .filter(|value| !value.is_empty())
        .ok_or(ApiError::InvalidHeader)
}

fn optional_uuid_header(headers: &HeaderMap, name: &'static str) -> Result<Option<Uuid>, ApiError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| ApiError::InvalidHeader)?
                .parse::<Uuid>()
                .map_err(|_| ApiError::InvalidHeader)
        })
        .transpose()
}

fn parse_project(value: &str) -> Result<ProjectId, ApiError> {
    value.parse().map_err(|_| ApiError::InvalidPath)
}

fn parse_package(value: &str) -> Result<PackageId, ApiError> {
    value.parse().map_err(|_| ApiError::InvalidPath)
}

fn parse_lease(value: &str) -> Result<LeaseId, ApiError> {
    value.parse().map_err(|_| ApiError::InvalidPath)
}

fn require_same<T: Eq>(left: T, right: T, _field: &'static str) -> Result<(), ApiError> {
    (left == right)
        .then_some(())
        .ok_or(ApiError::PathBodyMismatch)
}

#[derive(Debug)]
enum ApiError {
    InvalidPath,
    InvalidHeader,
    PathBodyMismatch,
    AccessDenied,
    NotConfigured,
    Command(MvpError),
}

impl From<MvpError> for ApiError {
    fn from(error: MvpError) -> Self {
        Self::Command(error)
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
    retryable: bool,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, retryable) = match self {
            Self::InvalidPath => (
                StatusCode::BAD_REQUEST,
                "AF_RESOURCE_ID_INVALID",
                "resource identifiers must be UUIDs",
                false,
            ),
            Self::InvalidHeader => (
                StatusCode::BAD_REQUEST,
                "AF_COMMAND_HEADER_INVALID",
                "Idempotency-Key and update If-Match headers must be valid",
                false,
            ),
            Self::PathBodyMismatch => (
                StatusCode::BAD_REQUEST,
                "AF_PATH_BODY_MISMATCH",
                "path and body resource identifiers must match",
                false,
            ),
            Self::AccessDenied => (
                StatusCode::FORBIDDEN,
                "AF_PROJECT_ACCESS_DENIED",
                "the request actor is not authorized for this project",
                false,
            ),
            Self::NotConfigured => (
                StatusCode::SERVICE_UNAVAILABLE,
                "AF_COMMAND_SERVICE_UNAVAILABLE",
                "the command service is not configured",
                true,
            ),
            Self::Command(error) => command_error_response(&error),
        };
        (
            status,
            Json(ErrorBody {
                code,
                message,
                retryable,
            }),
        )
            .into_response()
    }
}

fn command_error_response(error: &MvpError) -> (StatusCode, &'static str, &'static str, bool) {
    let code = error.code();
    let status = match code {
        "AF_NOT_FOUND" => StatusCode::NOT_FOUND,
        "AF_LEASE_EXPIRED" => StatusCode::GONE,
        "AF_POLICY_DENIED" => StatusCode::FORBIDDEN,
        "AF_CONFLICT"
        | "AF_STALE_VERSION"
        | "AF_LEASE_STALE"
        | "AF_IDEMPOTENCY_KEY_REUSED"
        | "AF_TRANSITION_INVALID"
        | "AF_PACKAGE_NOT_CLAIMABLE" => StatusCode::CONFLICT,
        "AF_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
        "AF_STORAGE_INTEGRITY" | "AF_SERIALIZATION" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };
    let message = if status.is_server_error() {
        "the command could not be completed by the service"
    } else if status == StatusCode::NOT_FOUND {
        "the requested resource was not found"
    } else if status == StatusCode::FORBIDDEN {
        "the command is not authorized"
    } else if status == StatusCode::GONE {
        "the Lease authorization window has expired"
    } else if status == StatusCode::CONFLICT {
        "the command conflicts with current durable state"
    } else {
        "the command did not satisfy its validation contract"
    };
    (status, code, message, error.retryable())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agentforge_application::{ClaimedWork, MvpControlPlane, MvpFuture, ProjectView};
    use agentforge_domain::{ExecutorId, NodeId, ProtocolKey};

    use super::*;

    struct CreateOnlyService;

    impl MvpControlPlane for CreateOnlyService {
        fn create_project<'a>(
            &'a self,
            command: &'a MvpCommand<CreateProjectInput>,
        ) -> MvpFuture<'a, ProjectView> {
            Box::pin(async move {
                Ok(ProjectView {
                    project_id: command.input.project_id,
                    protocol_key: command.input.protocol_key.clone(),
                    name: command.input.name.clone(),
                    version: AggregateVersion::new(1),
                })
            })
        }

        fn publish_package<'a>(
            &'a self,
            _command: &'a MvpCommand<PublishPackageInput>,
        ) -> MvpFuture<'a, PublishedPackage> {
            unavailable()
        }

        fn list_offers(&self, _query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>> {
            unavailable()
        }

        fn claim_package<'a>(
            &'a self,
            _command: &'a MvpCommand<ClaimPackageInput>,
        ) -> MvpFuture<'a, ClaimedWork> {
            unavailable()
        }

        fn renew_lease<'a>(
            &'a self,
            _command: &'a MvpCommand<RenewLeaseInput>,
        ) -> MvpFuture<'a, LeaseView> {
            unavailable()
        }

        fn release_lease<'a>(
            &'a self,
            _command: &'a MvpCommand<ReleaseLeaseInput>,
        ) -> MvpFuture<'a, LeaseView> {
            unavailable()
        }

        fn get_lease(
            &self,
            _project_id: ProjectId,
            _lease_id: LeaseId,
        ) -> MvpFuture<'_, LeaseView> {
            unavailable()
        }
    }

    fn unavailable<'a, T>() -> MvpFuture<'a, T> {
        Box::pin(async {
            Err(MvpError::Port(
                agentforge_application::PortError::Unavailable,
            ))
        })
    }

    fn test_state(project_id: ProjectId) -> ControlPlaneState {
        let (state, _) = ControlPlaneState::local_reference(
            crate::ui::CursorCodec::from_hex(&"11".repeat(32)).expect("cursor key"),
            [project_id],
        )
        .expect("local state");
        state.with_commands(Arc::new(CreateOnlyService))
    }

    #[test]
    fn command_headers_are_server_actor_bound_and_require_if_match_for_updates() {
        let actor = ActorId::from_uuid(Uuid::from_bytes([1; 16]));
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY, "claim-1".parse().expect("header"));
        headers.insert(header::IF_MATCH, "\"2\"".parse().expect("header"));
        let context = command_context(&headers, actor, true).expect("valid update context");
        assert_eq!(context.actor_id, actor);
        assert_eq!(context.expected_version, Some(AggregateVersion::new(2)));
        assert!(command_context(&headers, actor, false).is_err());
        headers.remove(header::IF_MATCH);
        assert!(command_context(&headers, actor, true).is_err());
        assert!(command_context(&headers, actor, false).is_ok());
    }

    #[test]
    fn command_errors_have_stable_http_classes_without_internal_details() {
        let error = MvpError::Domain(agentforge_domain::DomainError::StaleLease);
        let (status, code, message, _) = command_error_response(&error);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(code, "AF_LEASE_STALE");
        assert!(!message.contains("token"));
    }

    #[tokio::test]
    async fn create_handler_uses_authorized_server_actor_and_returns_created() {
        let project_id = ProjectId::from_uuid(Uuid::now_v7());
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY, "project-create".parse().expect("header"));
        let input = CreateProjectInput {
            project_id,
            protocol_key: ProtocolKey::new(format!("project-{project_id}")).expect("key"),
            name: "MVP".into(),
        };
        let (status, Json(view)) =
            create_project(State(test_state(project_id)), headers, Json(input.clone()))
                .await
                .expect("create response");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(view.project_id, input.project_id);
        assert_eq!(view.version, AggregateVersion::new(1));
    }

    #[tokio::test]
    async fn claim_handler_rejects_path_body_mismatch_before_dispatch() {
        let project_id = ProjectId::from_uuid(Uuid::now_v7());
        let path_package = PackageId::from_uuid(Uuid::now_v7());
        let body_package = PackageId::from_uuid(Uuid::now_v7());
        let input = ClaimPackageInput {
            project_id,
            package_id: body_package,
            executor_id: ExecutorId::from_uuid(Uuid::now_v7()),
            node_id: NodeId::from_uuid(Uuid::now_v7()),
            lease_seconds: 60,
            max_lease_seconds: 600,
        };
        let error = claim_package(
            State(test_state(project_id)),
            Path((project_id.to_string(), path_package.to_string())),
            HeaderMap::new(),
            Json(input),
        )
        .await
        .expect_err("mismatched package id");
        assert!(matches!(error, ApiError::PathBodyMismatch));
    }
}
