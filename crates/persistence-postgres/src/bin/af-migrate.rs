use agentforge_storage_postgres::migration;
use anyhow::{Context, Result};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<()> {
    let database_url =
        std::env::var("AGENTFORGE_DATABASE_URL").context("AGENTFORGE_DATABASE_URL must be set")?;
    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .context("connect to PostgreSQL")?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection closed: {error}");
        }
    });

    let applied = migration::migrate(&mut client)
        .await
        .context("apply AgentForge migrations")?;
    if applied.is_empty() {
        println!("AgentForge schema is current");
    } else {
        println!("Applied AgentForge migrations: {applied:?}");
    }
    drop(client);
    connection_task.await.context("join PostgreSQL driver")?;
    Ok(())
}
