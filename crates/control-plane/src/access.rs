//! Strongly typed request actor and project authorization boundary.

use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc};

use agentforge_domain::ProjectId;

pub type AuthorizationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + 'a>>;

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
    subject: Arc<str>,
}

impl NonLocalActor {
    pub fn new(subject: impl Into<Arc<str>>) -> anyhow::Result<Self> {
        let subject = subject.into();
        if subject.trim().is_empty() {
            anyhow::bail!("non-local actor subject cannot be empty");
        }
        Ok(Self { subject })
    }

    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationError {
    Denied,
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
}

impl LocalProjectAuthorizer {
    pub fn new(allowed_projects: impl IntoIterator<Item = ProjectId>) -> anyhow::Result<Self> {
        let allowed_projects = allowed_projects.into_iter().collect::<BTreeSet<_>>();
        if allowed_projects.is_empty() {
            anyhow::bail!("at least one local Control Room project must be allowed");
        }
        Ok(Self {
            allowed_projects: Arc::new(allowed_projects),
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
            if matches!(actor, ActorContext::LocalLoopback(_))
                && self.allowed_projects.contains(&project_id)
            {
                Ok(())
            } else {
                Err(AuthorizationError::Denied)
            }
        })
    }
}
