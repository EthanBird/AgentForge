//! Request-scoped actor extraction and project authorization grants.

use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc};

use agentforge_domain::{ProjectId, ServerInstant, Sha256Digest};
use axum::http::HeaderMap;
use time::OffsetDateTime;
use uuid::Uuid;

pub type ActorExtractionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ActorContext, AuthenticationError>> + Send + 'a>>;
pub type AuthorizationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AuthorizationGrant, AuthorizationError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TenantIdentity(Arc<str>);

impl TenantIdentity {
    pub fn new(value: impl Into<Arc<str>>) -> anyhow::Result<Self> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 200 || value.chars().any(char::is_control) {
            anyhow::bail!("tenant identity must be 1-200 printable characters");
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ActorIdentity(Arc<str>);

impl ActorIdentity {
    pub fn new(value: impl Into<Arc<str>>) -> anyhow::Result<Self> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 200 || value.chars().any(char::is_control) {
            anyhow::bail!("actor identity must be 1-200 printable characters");
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalLoopbackActor {
    _private: (),
}

impl LocalLoopbackActor {
    #[must_use]
    pub const fn reference_process() -> Self {
        Self { _private: () }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NonLocalActor {
    tenant: TenantIdentity,
    actor: ActorIdentity,
}

impl NonLocalActor {
    pub fn new(tenant: impl Into<Arc<str>>, actor: impl Into<Arc<str>>) -> anyhow::Result<Self> {
        Ok(Self {
            tenant: TenantIdentity::new(tenant)?,
            actor: ActorIdentity::new(actor)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActorContext {
    LocalLoopback(LocalLoopbackActor),
    NonLocal(NonLocalActor),
}

impl ActorContext {
    #[must_use]
    pub const fn local_reference() -> Self {
        Self::LocalLoopback(LocalLoopbackActor::reference_process())
    }

    pub fn tenant(&self) -> TenantIdentity {
        match self {
            Self::LocalLoopback(_) => {
                TenantIdentity::new("local-loopback").expect("static tenant is valid")
            }
            Self::NonLocal(actor) => actor.tenant.clone(),
        }
    }

    pub fn actor(&self) -> ActorIdentity {
        match self {
            Self::LocalLoopback(_) => {
                ActorIdentity::new("local-control-room").expect("static actor is valid")
            }
            Self::NonLocal(actor) => actor.actor.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationError {
    MissingExtractor,
    InvalidCredentials,
}

impl std::fmt::Display for AuthenticationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("request actor could not be authenticated")
    }
}

impl std::error::Error for AuthenticationError {}

/// Extracts one actor for one HTTP request. Production extractors are expected
/// to verify their credential headers before constructing `ActorContext`.
pub trait RequestActorExtractor: Send + Sync + 'static {
    fn extract<'a>(&'a self, headers: &'a HeaderMap) -> ActorExtractionFuture<'a>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalLoopbackActorExtractor;

impl RequestActorExtractor for LocalLoopbackActorExtractor {
    fn extract<'a>(&'a self, _headers: &'a HeaderMap) -> ActorExtractionFuture<'a> {
        Box::pin(async { Ok(ActorContext::local_reference()) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationGrant {
    pub project_id: ProjectId,
    pub tenant: TenantIdentity,
    pub actor: ActorIdentity,
    pub scope_digest: Sha256Digest,
    pub authorization_epoch: Uuid,
    pub expires_at: ServerInstant,
}

impl AuthorizationGrant {
    #[must_use]
    pub fn valid_for(
        &self,
        actor: &ActorContext,
        project_id: ProjectId,
        now: ServerInstant,
    ) -> bool {
        self.project_id == project_id
            && self.tenant == actor.tenant()
            && self.actor == actor.actor()
            && now < self.expires_at
    }

    #[must_use]
    pub fn same_authority(&self, other: &Self) -> bool {
        self.project_id == other.project_id
            && self.tenant == other.tenant
            && self.actor == other.actor
            && self.scope_digest == other.scope_digest
            && self.authorization_epoch == other.authorization_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationError {
    Denied,
    Expired,
    InvalidGrant,
}

impl std::fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("project access denied")
    }
}

impl std::error::Error for AuthorizationError {}

pub trait ProjectAuthorizer: Send + Sync + 'static {
    fn authorize<'a>(
        &'a self,
        actor: &'a ActorContext,
        project_id: ProjectId,
    ) -> AuthorizationFuture<'a>;
}

#[derive(Clone, Debug)]
pub struct LocalProjectAuthorizer {
    allowed_projects: Arc<BTreeSet<ProjectId>>,
    authorization_epoch: Uuid,
}

impl LocalProjectAuthorizer {
    pub fn new(allowed_projects: impl IntoIterator<Item = ProjectId>) -> anyhow::Result<Self> {
        let allowed_projects = allowed_projects.into_iter().collect::<BTreeSet<_>>();
        if allowed_projects.is_empty() {
            anyhow::bail!("at least one local Control Room project must be allowed");
        }
        Ok(Self {
            allowed_projects: Arc::new(allowed_projects),
            authorization_epoch: Uuid::now_v7(),
        })
    }
}

impl ProjectAuthorizer for LocalProjectAuthorizer {
    fn authorize<'a>(
        &'a self,
        actor: &'a ActorContext,
        project_id: ProjectId,
    ) -> AuthorizationFuture<'a> {
        Box::pin(async move {
            if !matches!(actor, ActorContext::LocalLoopback(_))
                || !self.allowed_projects.contains(&project_id)
            {
                return Err(AuthorizationError::Denied);
            }
            let tenant = actor.tenant();
            let actor_identity = actor.actor();
            let scope_material = format!(
                "control-room-read:v1:{}:{}:{project_id}",
                tenant.as_str(),
                actor_identity.as_str()
            );
            Ok(AuthorizationGrant {
                project_id,
                tenant,
                actor: actor_identity,
                scope_digest: Sha256Digest::of_bytes(scope_material),
                authorization_epoch: self.authorization_epoch,
                expires_at: ServerInstant(
                    OffsetDateTime::from_unix_timestamp(253_402_300_799)
                        .expect("year 9999 is representable"),
                ),
            })
        })
    }
}

#[must_use]
pub fn server_now() -> ServerInstant {
    ServerInstant(OffsetDateTime::now_utc())
}
