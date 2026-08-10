use std::path::{Path, PathBuf};
use std::process::ExitCode;

use agentforge_application::{
    ClaimPackageInput, CreateProjectInput, ListOffersQuery, MvpCommand as CommandEnvelope,
    MvpControlPlane, PublishPackageInput, ReleaseLeaseInput, RenewLeaseInput,
};
use agentforge_domain::{LeaseId, ProjectId};
use agentforge_protocol::{
    lint_candidate_ready, lint_publish, lint_work_graph_json, package_hash, schema,
};
use agentforge_storage_postgres::PostgresMvpControlPlane;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, Parser)]
#[command(name = "af-cli", version, about = "AgentForge protocol utility")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect immutable schemas embedded in this build.
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Hash and lint AgentForge Work Packages.
    Afwp {
        #[command(subcommand)]
        command: AfwpCommand,
    },
    /// Lint immutable candidate and salvage Submission manifests.
    Submission {
        #[command(subcommand)]
        command: SubmissionCommand,
    },
    /// Validate a complete WorkGraph snapshot.
    Graph {
        #[command(subcommand)]
        command: GraphCommand,
    },
    /// Execute the local MVP command surface directly against PostgreSQL.
    Mvp {
        /// Loopback PostgreSQL connection string; defaults to AGENTFORGE_DATABASE_URL.
        #[arg(long)]
        database_url: Option<String>,
        /// Trusted PostgreSQL schema containing AgentForge migrations.
        #[arg(long, default_value = "public")]
        database_schema: String,
        #[command(subcommand)]
        command: MvpAdminCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SchemaCommand {
    /// Print schema_version, versioned $id and exact artifact SHA-256 as JSON.
    List,
}

#[derive(Debug, Subcommand)]
enum AfwpCommand {
    /// Compute AFWP-C14N-1 after strict JSON and Schema validation.
    Hash {
        /// Path to an AFWP JSON document.
        path: PathBuf,
    },
    /// Run a semantic lint profile and emit a machine-readable JSON report.
    Lint {
        /// Path to an AFWP JSON document.
        path: PathBuf,
        /// Semantic validation profile.
        #[arg(long, value_enum, default_value_t = Profile::Publish)]
        profile: Profile,
    },
}

#[derive(Debug, Subcommand)]
enum SubmissionCommand {
    /// Run a static Submission profile and emit a machine-readable JSON report.
    Lint {
        /// Path to a Submission Manifest JSON document.
        path: PathBuf,
        /// Semantic validation profile.
        #[arg(long, value_enum, default_value_t = SubmissionProfile::CandidateReady)]
        profile: SubmissionProfile,
    },
}

#[derive(Debug, Subcommand)]
enum GraphCommand {
    /// Validate `{ "packages": [AFWP...] }` or a top-level AFWP array.
    Validate {
        /// Path to the strict JSON WorkGraph snapshot.
        path: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum MvpAdminCommand {
    /// Create a Project from a versioned command-envelope JSON file.
    ProjectCreate { request: PathBuf },
    /// Publish a typed Work Package from a command-envelope JSON file.
    PackagePublish { request: PathBuf },
    /// List claimable Offers for a Project.
    Offers {
        project_id: ProjectId,
        #[arg(long, default_value_t = 50)]
        limit: u16,
    },
    /// Atomically Claim one Package and create its Attempt and Lease.
    PackageClaim { request: PathBuf },
    /// Read a Lease by Project and Lease ID.
    LeaseGet {
        project_id: ProjectId,
        lease_id: LeaseId,
    },
    /// Renew a Lease from a command-envelope JSON file.
    LeaseRenew { request: PathBuf },
    /// Release a Lease from a command-envelope JSON file.
    LeaseRelease { request: PathBuf },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Profile {
    Publish,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SubmissionProfile {
    CandidateReady,
}

#[tokio::main]
async fn main() -> Result<ExitCode> {
    match Cli::parse().command {
        Command::Schema {
            command: SchemaCommand::List,
        } => {
            println!("{}", serde_json::to_string_pretty(&schema::list())?);
            Ok(ExitCode::SUCCESS)
        }
        Command::Afwp {
            command: AfwpCommand::Hash { path },
        } => {
            let input = read(&path)?;
            println!("{}", package_hash(&input)?);
            Ok(ExitCode::SUCCESS)
        }
        Command::Afwp {
            command: AfwpCommand::Lint { path, profile },
        } => {
            let input = read(&path)?;
            let report = match profile {
                Profile::Publish => lint_publish(&input),
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(if report.valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        Command::Submission {
            command: SubmissionCommand::Lint { path, profile },
        } => {
            let input = read(&path)?;
            let report = match profile {
                SubmissionProfile::CandidateReady => lint_candidate_ready(&input),
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(if report.valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        Command::Graph {
            command: GraphCommand::Validate { path },
        } => {
            let input = read(&path)?;
            let report = lint_work_graph_json(&input);
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(if report.valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        Command::Mvp {
            database_url,
            database_schema,
            command,
        } => {
            let database_url = database_url
                .or_else(|| std::env::var("AGENTFORGE_DATABASE_URL").ok())
                .context(
                    "--database-url or AGENTFORGE_DATABASE_URL is required for MVP commands",
                )?;
            let control = PostgresMvpControlPlane::new_local_no_tls(database_url, database_schema)
                .context("configure PostgreSQL MVP command adapter")?;
            match command {
                MvpAdminCommand::ProjectCreate { request } => {
                    let command = read_json::<CommandEnvelope<CreateProjectInput>>(&request)?;
                    print_json(&control.create_project(&command).await?)?;
                }
                MvpAdminCommand::PackagePublish { request } => {
                    let command = read_json::<CommandEnvelope<PublishPackageInput>>(&request)?;
                    print_json(&control.publish_package(&command).await?)?;
                }
                MvpAdminCommand::Offers { project_id, limit } => {
                    print_json(
                        &control
                            .list_offers(ListOffersQuery { project_id, limit })
                            .await?,
                    )?;
                }
                MvpAdminCommand::PackageClaim { request } => {
                    let command = read_json::<CommandEnvelope<ClaimPackageInput>>(&request)?;
                    print_json(&control.claim_package(&command).await?)?;
                }
                MvpAdminCommand::LeaseGet {
                    project_id,
                    lease_id,
                } => {
                    print_json(&control.get_lease(project_id, lease_id).await?)?;
                }
                MvpAdminCommand::LeaseRenew { request } => {
                    let command = read_json::<CommandEnvelope<RenewLeaseInput>>(&request)?;
                    print_json(&control.renew_lease(&command).await?)?;
                }
                MvpAdminCommand::LeaseRelease { request } => {
                    let command = read_json::<CommandEnvelope<ReleaseLeaseInput>>(&request)?;
                    print_json(&control.release_lease(&command).await?)?;
                }
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn read(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(&read(path)?).with_context(|| {
        format!(
            "failed to decode strict command JSON from {}",
            path.display()
        )
    })
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
