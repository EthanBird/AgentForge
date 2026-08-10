use std::{path::PathBuf, sync::Arc};

use agentforge_worker_daemon::{
    config::WorkerDaemonConfig,
    daemon::{SystemDaemonRuntime, WorkerDaemon},
    http_control::LoopbackHttpControlPlane,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::var_os("AGENTFORGE_WORKER_CONFIG")
        .map(PathBuf::from)
        .ok_or("AGENTFORGE_WORKER_CONFIG must point to the Worker JSON configuration")?;
    let config = WorkerDaemonConfig::load(config_path)?;
    let node_fingerprint = config.node_fingerprint()?;
    let node_id = config.node_id;
    let project_count = config.project_ids.len();
    let capacity = config.capacity;
    let control = LoopbackHttpControlPlane::from_config(&config)?;
    let mut daemon = WorkerDaemon::open(Arc::new(control), config, SystemDaemonRuntime)?;

    eprintln!(
        "AgentForge Worker starting: node_id={node_id} fingerprint={node_fingerprint} projects={project_count} capacity={capacity}"
    );
    daemon.run_until_shutdown(shutdown_signal()).await?;
    eprintln!("AgentForge Worker stopped: node_id={node_id}");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
