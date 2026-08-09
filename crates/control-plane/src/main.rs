use std::{collections::BTreeSet, net::SocketAddr, str::FromStr};

use agentforge_control_plane::ui::{ControlPlaneState, CursorCodec, router};
use agentforge_domain::ProjectId;
use anyhow::{Context, Result};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let bind = std::env::var("AGENTFORGE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let address = SocketAddr::from_str(&bind).context("parse AGENTFORGE_BIND")?;
    anyhow::ensure!(
        address.ip().is_loopback(),
        "M1 Control Room is loopback-only until the authentication adapter is installed"
    );
    let cursor_key = std::env::var("AGENTFORGE_CURSOR_HMAC_KEY")
        .context("AGENTFORGE_CURSOR_HMAC_KEY must be 64 lowercase hex characters")?;
    let allowed_projects = parse_allowed_projects(
        &std::env::var("AGENTFORGE_LOCAL_PROJECT_IDS")
            .context("AGENTFORGE_LOCAL_PROJECT_IDS must contain comma-separated Project UUIDs")?,
    )?;
    let (state, _local_store) =
        ControlPlaneState::local_reference(CursorCodec::from_hex(&cursor_key)?, allowed_projects)?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .context("bind AgentForge control plane")?;
    info!(%address, "AgentForge control plane listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve AgentForge control plane")
}

fn parse_allowed_projects(value: &str) -> Result<BTreeSet<ProjectId>> {
    let projects = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| ProjectId::from_str(value).context("parse local Control Room Project UUID"))
        .collect::<Result<BTreeSet<_>>>()?;
    anyhow::ensure!(
        !projects.is_empty(),
        "AGENTFORGE_LOCAL_PROJECT_IDS must contain at least one Project UUID"
    );
    Ok(projects)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C signal handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::parse_allowed_projects;

    #[test]
    fn local_project_allowlist_is_nonempty_typed_and_deduplicated() {
        let first = "018f0000-0000-7000-8000-000000000001";
        let second = "018f0000-0000-7000-8000-000000000002";
        let projects = parse_allowed_projects(&format!("{first}, {second},{first}"))
            .expect("valid local allowlist");
        assert_eq!(projects.len(), 2);
        assert!(parse_allowed_projects("").is_err());
        assert!(parse_allowed_projects("not-a-uuid").is_err());
    }
}
