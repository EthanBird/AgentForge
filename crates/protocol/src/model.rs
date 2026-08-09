//! Strict serde models for AFWP/1.0 and Submission/1.0.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

/// An AgentForge Work Package wire document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Afwp {
    pub schema_version: String,
    pub package_id: String,
    pub revision: u64,
    pub package_hash: String,
    pub project_id: String,
    pub graph_version: u64,
    pub parent_id: Option<String>,
    pub kind: WorkKind,
    pub title: String,
    pub metadata: Option<Metadata>,
    pub goal: Goal,
    pub requirements: Vec<Requirement>,
    pub scope: Scope,
    pub snapshot: Snapshot,
    pub dependencies: Vec<Dependency>,
    pub interfaces: Option<Vec<Interface>>,
    pub routing: Routing,
    pub permissions: Permissions,
    pub scheduling: Scheduling,
    pub conflicts: Conflicts,
    pub communication: Option<Communication>,
    pub delegation: Option<Delegation>,
    pub acceptance: Acceptance,
    pub deliverables: Vec<AfwpDeliverable>,
    pub completion: Completion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkKind {
    Research,
    Architecture,
    Specification,
    Implementation,
    Test,
    Review,
    Integration,
    Operations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub created_by: String,
    pub labels: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Goal {
    pub background: String,
    pub objective: String,
    pub value: String,
    pub glossary: Option<BTreeMap<String, String>>,
    pub assumptions: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirement {
    pub id: String,
    pub level: RequirementLevel,
    pub source: String,
    pub text: String,
    pub error_semantics: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementLevel {
    Must,
    Should,
    May,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub must_do: Vec<String>,
    pub should_do: Option<Vec<String>>,
    pub non_goals: Vec<String>,
    pub allowed_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub repository: String,
    pub base_commit: String,
    pub toolchain_locks: Vec<ToolchainLock>,
    pub inputs: Vec<InputArtifact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolchainLock {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputArtifact {
    pub artifact_id: String,
    pub uri: String,
    pub sha256: String,
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    pub package_id: String,
    pub revision: u64,
    pub edge: DependencyEdge,
    pub condition: DependencyCondition,
    pub artifact_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyEdge {
    Blocks,
    ProvidesContract,
    UsesArtifact,
    IntegrationAfter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyCondition {
    Accepted,
    Integrated,
    ArtifactAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interface {
    pub id: String,
    pub kind: InterfaceKind,
    pub artifact_ref: String,
    pub compatibility: Compatibility,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceKind {
    Openapi,
    Asyncapi,
    Protobuf,
    JsonSchema,
    Database,
    Cli,
    UiContract,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compatibility {
    Exact,
    Backward,
    Forward,
    Full,
    BreakingAllowed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Routing {
    pub task_class: String,
    pub risk: Risk,
    pub complexity: f64,
    pub required: RequiredRouting,
    pub preferred: PreferredRouting,
    pub minimum_first_pass_probability: f64,
    pub deadline_minutes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredRouting {
    pub capabilities: Vec<String>,
    pub tools: Vec<String>,
    pub os: Vec<String>,
    pub modalities: Vec<String>,
    pub hardware: Option<Vec<String>>,
    pub security_level: SecurityLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityLevel {
    Public,
    ProjectPrivate,
    Restricted,
    Regulated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreferredRouting {
    pub capability_weights: BTreeMap<String, f64>,
    pub task_profiles: Vec<String>,
    pub model_families: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permissions {
    pub network_allowlist: Vec<String>,
    pub git_write_prefix: String,
    pub external_side_effects: ExternalSideEffects,
    pub secrets: Vec<SecretGrant>,
    pub max_artifact_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalSideEffects {
    Deny,
    ApprovalRequired,
    AllowDeclared,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretGrant {
    pub name: String,
    pub delivery: SecretDelivery,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretDelivery {
    HostBrokerOnly,
    FileEphemeral,
    EnvironmentEphemeral,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scheduling {
    pub mode: SchedulingMode,
    pub redundancy: Option<u64>,
    pub priority: u64,
    pub max_budget_units: f64,
    pub max_attempts: u64,
    pub offline_grace_seconds: u64,
    pub lease: LeaseSettings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulingMode {
    Exclusive,
    SealedBid,
    Redundant,
    AuthorReviewerPair,
    Tournament,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseSettings {
    pub ttl_seconds: u64,
    pub renew_after_seconds: u64,
    pub max_execution_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conflicts {
    pub expected_write_set: Vec<String>,
    pub mutex: Vec<String>,
    pub integration_after: Vec<String>,
    pub merge_strategy: MergeStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    MergeCommit,
    Squash,
    RebaseThenMerge,
    ManualIntegration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Communication {
    pub autonomous_decisions: Vec<String>,
    pub must_escalate: Vec<String>,
    pub question_timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delegation {
    pub allowed: bool,
    pub max_depth: u64,
    pub max_children: u64,
    pub max_budget_units: f64,
    pub allowed_kinds: Option<Vec<WorkKind>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acceptance {
    pub criteria: Vec<Criterion>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Criterion {
    pub id: String,
    pub covers: Vec<String>,
    pub kind: CriterionKind,
    pub given: String,
    pub when: String,
    pub then: String,
    pub runner_image: Option<String>,
    pub argv: Option<Vec<String>>,
    pub working_directory: Option<String>,
    pub environment: Option<BTreeMap<String, String>>,
    pub expect: Option<Expectation>,
    pub resources: Option<Resources>,
    pub evidence: Option<Vec<String>>,
    pub allow: Option<Vec<String>>,
    pub deny: Option<Vec<String>>,
    pub hard: bool,
    pub flaky_retry_limit: u64,
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriterionKind {
    Command,
    IntegrationTest,
    PropertyTest,
    ChangedPaths,
    Schema,
    BenchmarkDelta,
    SecurityScan,
    VisualDiff,
    Artifact,
    ManualGate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expectation {
    pub exit_code: Option<i64>,
    pub stdout_contains: Option<Vec<String>>,
    pub stderr_contains: Option<Vec<String>>,
    pub submission_state: Option<SubmissionState>,
    pub json_assertions: Option<Vec<JsonAssertion>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SubmissionState {
    Received,
    ProvenanceCheck,
    Reproducing,
    Reviewing,
    Pass,
    Fail,
    Inconclusive,
    Quarantined,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonAssertion {
    pub pointer: String,
    pub operator: AssertionOperator,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssertionOperator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Contains,
    Matches,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub node_class: Option<String>,
    pub cpu_limit: Option<f64>,
    pub memory_mb: Option<u64>,
    pub gpu_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AfwpDeliverable {
    pub id: String,
    pub kind: AfwpDeliverableKind,
    pub path: Option<String>,
    pub media_type: Option<String>,
    pub required: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AfwpDeliverableKind {
    GitCommit,
    Source,
    Test,
    Document,
    Report,
    Image,
    Binary,
    Sbom,
    EvidenceBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub branch_pattern: String,
    pub commit_trailers: Vec<String>,
    pub rollback: String,
    pub definition_of_done: Vec<String>,
}

/// An immutable candidate or salvage Submission Manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    pub schema_version: String,
    pub submission_id: String,
    pub submission_kind: SubmissionKind,
    pub candidate_id: Option<Uuid>,
    pub candidate_artifact_id: Option<Uuid>,
    pub verification_run_id: Option<Uuid>,
    pub terminal_outcome: TerminalOutcome,
    pub completed_stage: CompletedStage,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub package_id: String,
    pub package_revision: u64,
    pub package_hash: String,
    pub attempt_id: String,
    pub lease: SubmissionLease,
    pub git: Option<SubmissionGit>,
    pub deliverables: Option<Vec<SubmissionDeliverable>>,
    pub criteria: Option<Vec<CriterionResult>>,
    pub review: Option<Review>,
    pub clean_reproduction: Option<CleanReproduction>,
    pub evidence_bundle: Option<EvidenceBundle>,
    pub salvage: Option<Salvage>,
    pub failure_dossier: Option<FailureDossier>,
    pub decisions: Option<Vec<String>>,
    pub residual_risks: Option<Vec<String>>,
    pub provenance: Provenance,
    pub lineage: Lineage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionKind {
    Candidate,
    Salvage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TerminalOutcome {
    Pass,
    Fail,
    Inconclusive,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletedStage {
    ProvenanceCheck,
    Reviewing,
    Reproducing,
    CandidateReady,
    SalvageRegistration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionLease {
    pub lease_id: String,
    pub generation: u64,
    pub fencing_token_hash: String,
    #[serde(with = "time::serde::rfc3339")]
    pub issued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionGit {
    pub repository: String,
    pub base_commit: String,
    pub candidate_commit: String,
    pub tree_hash: String,
    pub branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionDeliverable {
    pub id: String,
    pub kind: SubmissionDeliverableKind,
    pub path: String,
    pub sha256: String,
    pub media_type: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionDeliverableKind {
    Source,
    Test,
    Document,
    Report,
    Image,
    Binary,
    Sbom,
    Evidence,
    GitCommit,
    EvidenceBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CriterionResult {
    pub acceptance_id: String,
    pub status: CriterionStatus,
    pub hard: bool,
    pub runner_digest: String,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    pub duration_ms: u64,
    pub exit_code: Option<i64>,
    pub attempts: Option<u64>,
    pub evidence_refs: Vec<String>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CriterionStatus {
    Pass,
    Fail,
    Inconclusive,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewFinding {
    pub id: String,
    pub severity: FindingSeverity,
    pub status: FindingStatus,
    pub summary: String,
    pub location: Option<String>,
    pub acceptance_id: Option<String>,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingStatus {
    Open,
    Resolved,
    Waived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Review {
    pub reviewed_head: String,
    pub reviewer_id: String,
    pub reviewer_executor_id: String,
    pub verdict: ReviewVerdict,
    pub findings: Vec<ReviewFinding>,
    pub report_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewVerdict {
    Pass,
    Fail,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanReproduction {
    pub tested_head: String,
    pub runner_digest: String,
    pub verdict: ReproductionVerdict,
    pub report_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReproductionVerdict {
    Pass,
    Fail,
    Inconclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceBundle {
    pub uri: String,
    pub sha256: String,
    pub manifest_sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Salvage {
    pub artifact_uri: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub base_commit: String,
    pub candidate_commit: Option<String>,
    pub quarantine_reason: QuarantineReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineReason {
    LeaseExpired,
    LeaseRevoked,
    GenerationSuperseded,
    NodeSessionLost,
    ManualQuarantine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDossier {
    pub code: String,
    pub summary: String,
    pub retryable: bool,
    pub evidence_digest: String,
    pub evidence_refs: Vec<String>,
    pub remediation: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub agent_id: String,
    pub executor_id: String,
    pub node_id: String,
    pub executor_fingerprint: String,
    pub jcode_version: String,
    pub toolchain_digest: String,
    pub signature: SubmissionSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionSignature {
    pub algorithm: SignatureAlgorithm,
    pub key_id: String,
    pub signer_role: SignerRole,
    pub signed_digest: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignatureAlgorithm {
    Ed25519,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignerRole {
    VerificationCoordinator,
    SalvageRegistrar,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lineage {
    pub parent_submission_id: Option<String>,
    pub supersedes: Vec<String>,
}
