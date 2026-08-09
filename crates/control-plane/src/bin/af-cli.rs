use std::path::{Path, PathBuf};
use std::process::ExitCode;

use agentforge_protocol::{
    lint_candidate_ready, lint_publish, lint_work_graph_json, package_hash, schema,
};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Profile {
    Publish,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SubmissionProfile {
    CandidateReady,
}

fn main() -> Result<ExitCode> {
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
    }
}

fn read(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}
