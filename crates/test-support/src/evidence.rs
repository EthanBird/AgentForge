use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

/// Stable runner attributes bound into test evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunnerFingerprint {
    /// Logical runner or image name.
    pub runner_id: String,
    /// Immutable OCI digest when a container runner is used.
    pub image_digest: Option<String>,
    /// Source commit under test, when available.
    pub source_commit: Option<String>,
    /// Rust compilation target operating system.
    pub operating_system: String,
    /// Rust compilation target architecture.
    pub architecture: String,
}

impl RunnerFingerprint {
    /// Creates a fingerprint whose variable fields are supplied explicitly.
    #[must_use]
    pub fn new(
        runner_id: impl Into<String>,
        image_digest: Option<String>,
        source_commit: Option<String>,
    ) -> Self {
        Self {
            runner_id: runner_id.into(),
            image_digest,
            source_commit,
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
        }
    }

    /// Returns a content fingerprint over the stable runner attributes.
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("runner fingerprint is serializable");
        sha256(&bytes)
    }
}

/// Result classification recorded in JSON and JUnit evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvidenceStatus {
    /// The command exited successfully.
    Pass,
    /// The command failed or was terminated without an exit code.
    Fail,
}

/// Deterministic command evidence.
///
/// Runtime duration is intentionally absent, so identical seed/clock inputs
/// produce byte-identical JSON and JUnit output.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandEvidence {
    /// Version of this evidence document.
    pub schema_version: String,
    /// Stable test or criterion name.
    pub case_name: String,
    /// Exact argument vector; no shell reconstruction is performed.
    pub argv: Vec<String>,
    /// Child exit code, or `None` if it terminated by signal.
    pub exit_code: Option<i32>,
    /// Schedule or fixture seed.
    pub seed: u64,
    /// Digest of the runner attributes below.
    pub runner_fingerprint: String,
    /// Attributes used to calculate `runner_fingerprint`.
    pub runner: RunnerFingerprint,
    /// Pass/fail classification.
    pub status: EvidenceStatus,
    /// SHA-256 of captured standard output.
    pub stdout_sha256: String,
    /// SHA-256 of captured standard error.
    pub stderr_sha256: String,
}

impl CommandEvidence {
    /// Creates evidence from an already captured process result.
    #[must_use]
    pub fn from_capture(
        case_name: impl Into<String>,
        argv: Vec<String>,
        exit_code: Option<i32>,
        seed: u64,
        runner: RunnerFingerprint,
        stdout: &[u8],
        stderr: &[u8],
    ) -> Self {
        let status = if exit_code == Some(0) {
            EvidenceStatus::Pass
        } else {
            EvidenceStatus::Fail
        };
        let runner_fingerprint = runner.digest();
        Self {
            schema_version: "agentforge.test-evidence/v1".to_owned(),
            case_name: case_name.into(),
            argv,
            exit_code,
            seed,
            runner_fingerprint,
            runner,
            status,
            stdout_sha256: sha256(stdout),
            stderr_sha256: sha256(stderr),
        }
    }

    /// Encodes stable, pretty-printed JSON with a trailing newline.
    pub fn json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Encodes a single-case JUnit report without runtime duration fields.
    #[must_use]
    pub fn junit_xml(&self) -> String {
        let failures = u8::from(self.status == EvidenceStatus::Fail);
        let argv = serde_json::to_string(&self.argv).expect("argv is serializable");
        let exit_code = self
            .exit_code
            .map_or_else(|| "signal".to_owned(), |code| code.to_string());
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"{}\" tests=\"1\" failures=\"{}\">\n  <properties>\n    <property name=\"argv\" value=\"{}\"/>\n    <property name=\"exit_code\" value=\"{}\"/>\n    <property name=\"seed\" value=\"{}\"/>\n    <property name=\"runner_fingerprint\" value=\"{}\"/>\n  </properties>\n  <testcase name=\"{}\">\n",
            xml_escape(&self.case_name),
            failures,
            xml_escape(&argv),
            xml_escape(&exit_code),
            self.seed,
            xml_escape(&self.runner_fingerprint),
            xml_escape(&self.case_name),
        );
        if self.status == EvidenceStatus::Fail {
            xml.push_str(&format!(
                "    <failure message=\"command exited with {}\"/>\n",
                xml_escape(&exit_code)
            ));
        }
        xml.push_str(&format!(
            "    <system-out>sha256:{}</system-out>\n    <system-err>sha256:{}</system-err>\n  </testcase>\n</testsuite>\n",
            xml_escape(self.stdout_sha256.trim_start_matches("sha256:")),
            xml_escape(self.stderr_sha256.trim_start_matches("sha256:")),
        ));
        xml
    }

    /// Writes adjacent JSON and JUnit files.
    pub fn write_files(&self, directory: &Path, stem: &str) -> io::Result<EvidencePaths> {
        fs::create_dir_all(directory)?;
        let paths = EvidencePaths {
            json: directory.join(format!("{stem}.json")),
            junit: directory.join(format!("{stem}.junit.xml")),
        };
        let json = self.json_bytes().map_err(io::Error::other)?;
        fs::write(&paths.json, json)?;
        fs::write(&paths.junit, self.junit_xml())?;
        Ok(paths)
    }
}

/// Paths produced by [`CommandEvidence::write_files`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidencePaths {
    /// JSON evidence path.
    pub json: PathBuf,
    /// JUnit evidence path.
    pub junit: PathBuf,
}

/// Mandatory execution policy for command fixtures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TestExecutionConstraints {
    /// Tests never authorize external network access.
    pub external_network_allowed: bool,
    /// Tests never inherit Git credentials from the host.
    pub host_git_credentials_allowed: bool,
    /// Test commands never run through a shell.
    pub shell_allowed: bool,
}

impl TestExecutionConstraints {
    /// The only policy supported by [`HermeticTestCommand`].
    pub const HERMETIC: Self = Self {
        external_network_allowed: false,
        host_git_credentials_allowed: false,
        shell_allowed: false,
    };
}

/// Runs the current test binary as an isolated child fixture.
///
/// The executable cannot be replaced with `curl`, `git`, a shell, or another
/// host program. The child receives a cleared environment, a temporary home,
/// disabled Git credential prompting, and a kernel-enforced deny-network
/// policy installed by a trusted sibling runner.
/// This keeps conformance fixtures local and prevents accidental credential
/// inheritance.
#[derive(Debug)]
pub struct HermeticTestCommand {
    executable: PathBuf,
    sandbox_executable: PathBuf,
    args: Vec<OsString>,
    fixture_environment: BTreeMap<String, String>,
    seed: u64,
    runner: RunnerFingerprint,
}

impl HermeticTestCommand {
    /// Creates a command constrained to the current test executable.
    pub fn current_test_binary(seed: u64, runner: RunnerFingerprint) -> io::Result<Self> {
        let executable = std::env::current_exe()?;
        let sandbox_executable = sandbox_executable_for(&executable)?;
        Ok(Self {
            executable,
            sandbox_executable,
            args: Vec::new(),
            fixture_environment: BTreeMap::new(),
            seed,
            runner,
        })
    }

    /// Appends one argument without shell interpretation.
    #[must_use]
    pub fn arg(mut self, argument: impl AsRef<OsStr>) -> Self {
        self.args.push(argument.as_ref().to_owned());
        self
    }

    /// Adds a fixture-only environment entry.
    ///
    /// Only `AGENTFORGE_TEST_*` names are accepted, preventing callers from
    /// re-introducing credential, proxy, or Git configuration variables.
    pub fn fixture_env(mut self, key: &str, value: &str) -> io::Result<Self> {
        if !key.starts_with("AGENTFORGE_TEST_") {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "fixture environment keys must start with AGENTFORGE_TEST_",
            ));
        }
        self.fixture_environment
            .insert(key.to_owned(), value.to_owned());
        Ok(self)
    }

    /// Returns the enforced process policy.
    #[must_use]
    pub const fn constraints(&self) -> TestExecutionConstraints {
        TestExecutionConstraints::HERMETIC
    }

    /// Executes the fixture and captures evidence.
    pub fn run(self, case_name: &str) -> io::Result<CommandEvidence> {
        let sandbox = TempDir::new()?;
        let home = sandbox.path().join("home");
        let xdg = sandbox.path().join("xdg");
        fs::create_dir_all(&home)?;
        fs::create_dir_all(&xdg)?;

        let mut command = Command::new(&self.sandbox_executable);
        command
            .arg(&self.executable)
            .args(&self.args)
            .current_dir(sandbox.path())
            .env_clear()
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &xdg)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", sandbox.path().join("empty.gitconfig"))
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("AGENTFORGE_TEST_NETWORK_POLICY", "deny")
            .env("AGENTFORGE_TEST_GIT_CREDENTIAL_POLICY", "isolated")
            .envs(&self.fixture_environment);
        let output = command.output()?;

        let mut argv = Vec::with_capacity(self.args.len() + 1);
        argv.push(self.executable.to_string_lossy().into_owned());
        argv.extend(
            self.args
                .iter()
                .map(|argument| argument.to_string_lossy().into_owned()),
        );
        Ok(CommandEvidence::from_capture(
            case_name,
            argv,
            output.status.code(),
            self.seed,
            self.runner,
            &output.stdout,
            &output.stderr,
        ))
    }
}

#[cfg(target_os = "linux")]
fn sandbox_executable_for(test_executable: &Path) -> io::Result<PathBuf> {
    let deps_directory = test_executable.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "current test executable has no parent directory",
        )
    })?;
    if deps_directory.file_name() != Some(OsStr::new("deps")) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "current test executable is not in a Cargo profile deps directory",
        ));
    }
    let profile_directory = deps_directory.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "Cargo deps directory has no profile parent",
        )
    })?;
    let runner = profile_directory.join(format!(
        "af-hermetic-runner{}",
        std::env::consts::EXE_SUFFIX
    ));
    let canonical_profile = profile_directory.canonicalize()?;
    let canonical_runner = runner.canonicalize().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "trusted hermetic runner is unavailable at {}: {error}",
                runner.display()
            ),
        )
    })?;
    if canonical_runner.parent() != Some(canonical_profile.as_path()) || !canonical_runner.is_file()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "trusted hermetic runner escaped the Cargo profile directory",
        ));
    }
    Ok(canonical_runner)
}

#[cfg(not(target_os = "linux"))]
fn sandbox_executable_for(_test_executable: &Path) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "kernel-enforced hermetic networking requires Linux seccomp",
    ))
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
