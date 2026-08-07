//! Deterministic semantic linting for publishable AFWP documents.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;

use crate::canonical::{self, HashError};
use crate::model::{
    Afwp, CompletedStage, CriterionKind, CriterionStatus, Delegation, ExternalSideEffects,
    FindingSeverity, FindingStatus, ReproductionVerdict, RequirementLevel, ReviewVerdict,
    SchedulingMode, SignerRole, Submission, SubmissionDeliverableKind, SubmissionKind,
    TerminalOutcome,
};
use crate::schema::{self, SchemaKind};
use crate::strict_json;

/// Semantic validation profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LintProfile {
    /// Definition-of-Ready checks before `VALIDATING -> OFFERED`.
    Publish,
    /// Static checks for a terminal candidate that is ready for integration.
    #[serde(rename = "candidate-ready")]
    CandidateReady,
}

/// A stable, machine-readable linter finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LintFinding {
    /// Stable code intended for programmatic assertions.
    pub code: &'static str,
    /// RFC 6901 JSON Pointer into the input document.
    pub pointer: String,
    /// Human diagnostic; consumers must not branch on this text.
    pub message: String,
}

/// Complete deterministic result for one lint invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LintReport {
    /// Profile that produced the report.
    pub profile: LintProfile,
    /// True only when no hard finding was produced.
    pub valid: bool,
    /// Findings sorted by pointer and stable code.
    pub findings: Vec<LintFinding>,
}

/// Result of validating a complete WorkGraph snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphLintReport {
    /// True only when every package and every graph edge is valid.
    pub valid: bool,
    /// Findings use normalized `/packages/<index>/...` pointers for both
    /// accepted input encodings.
    pub findings: Vec<LintFinding>,
}

/// A validation step that cannot be established from a standalone signed
/// Submission Manifest and therefore must be supplied by a trusted adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExternalFactRequirement {
    /// Stable machine-readable requirement code.
    pub code: &'static str,
    /// Manifest location whose claim must be corroborated.
    pub pointer: String,
    /// Description of the trusted external fact.
    pub message: String,
}

/// Static `candidate-ready` result. `valid=true` means the manifest is
/// internally consistent; `external_facts_required` still have to be checked
/// before accepting or integrating it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidateReadyReport {
    /// Always `candidate_ready`.
    pub profile: LintProfile,
    /// Whether all checks possible from the standalone manifest passed.
    pub valid: bool,
    /// Deterministic static failures.
    pub findings: Vec<LintFinding>,
    /// Trusted registry, database, Git and artifact facts intentionally not
    /// claimed by this static profile.
    pub external_facts_required: Vec<ExternalFactRequirement>,
}

impl CandidateReadyReport {
    fn new(mut findings: Vec<LintFinding>, include_external_facts: bool) -> Self {
        sort_findings(&mut findings);
        Self {
            profile: LintProfile::CandidateReady,
            valid: findings.is_empty(),
            findings,
            external_facts_required: if include_external_facts {
                candidate_external_facts()
            } else {
                Vec::new()
            },
        }
    }
}

impl LintReport {
    fn new(mut findings: Vec<LintFinding>) -> Self {
        sort_findings(&mut findings);
        Self {
            profile: LintProfile::Publish,
            valid: findings.is_empty(),
            findings,
        }
    }
}

/// Strictly parse, schema-check, hash-check and semantically lint an AFWP.
#[must_use]
pub fn lint_publish(input: &[u8]) -> LintReport {
    let value = match strict_json::from_slice(input) {
        Ok(value) => value,
        Err(error) => {
            return LintReport::new(vec![finding("AF_SCHEMA_INVALID", "", error.to_string())]);
        }
    };
    lint_publish_value(&value)
}

/// Schema-check, hash-check and semantically lint an already strict JSON value.
#[must_use]
pub fn lint_publish_value(value: &Value) -> LintReport {
    let mut findings = Vec::new();
    if let Some(received) = value.get("schema_version").and_then(Value::as_str)
        && received != SchemaKind::Afwp.schema_version()
    {
        findings.push(finding(
            "AF_SCHEMA_VERSION_UNSUPPORTED",
            "/schema_version",
            format!(
                "received {received}, supported {}",
                SchemaKind::Afwp.schema_version()
            ),
        ));
        return LintReport::new(findings);
    }
    if let Err(violations) = schema::validate(SchemaKind::Afwp, value) {
        findings.extend(violations.into_iter().map(|violation| LintFinding {
            code: violation.code,
            pointer: violation.pointer,
            message: violation.message,
        }));
        return LintReport::new(findings);
    }

    if let Err(error) = canonical::verify_package_hash_value(value) {
        match error {
            HashError::Mismatch { declared, computed } => findings.push(finding(
                "AF_PACKAGE_HASH_MISMATCH",
                "/package_hash",
                format!("declared {declared}, computed {computed}"),
            )),
            other => findings.push(finding("AF_SCHEMA_INVALID", "", other.to_string())),
        }
    }

    let package: Afwp = match serde_json::from_value(value.clone()) {
        Ok(package) => package,
        Err(error) => {
            findings.push(finding("AF_SCHEMA_INVALID", "", error.to_string()));
            return LintReport::new(findings);
        }
    };
    findings.extend(lint_package(&package));
    LintReport::new(findings)
}

/// Strictly parse and statically validate a terminal candidate Submission.
///
/// This function does not pretend that a standalone JSON document can prove
/// database fencing history, Git reachability, artifact bytes, identity-registry
/// authorization or an Ed25519 signature. Those checks are returned explicitly
/// in `external_facts_required`.
#[must_use]
pub fn lint_candidate_ready(input: &[u8]) -> CandidateReadyReport {
    let value = match strict_json::from_slice(input) {
        Ok(value) => value,
        Err(error) => {
            return CandidateReadyReport::new(
                vec![finding("AF_SCHEMA_INVALID", "", error.to_string())],
                false,
            );
        }
    };
    lint_candidate_ready_value(&value)
}

/// Schema-check and statically validate an already strict Submission value.
#[must_use]
pub fn lint_candidate_ready_value(value: &Value) -> CandidateReadyReport {
    if let Some(received) = value.get("schema_version").and_then(Value::as_str)
        && received != SchemaKind::Submission.schema_version()
    {
        return CandidateReadyReport::new(
            vec![finding(
                "AF_SCHEMA_VERSION_UNSUPPORTED",
                "/schema_version",
                format!(
                    "received {received}, supported {}",
                    SchemaKind::Submission.schema_version()
                ),
            )],
            false,
        );
    }
    if let Err(violations) = schema::validate(SchemaKind::Submission, value) {
        let role_only = !violations.is_empty()
            && violations.iter().all(|violation| {
                violation.pointer == "/provenance/signature/signer_role"
                    && violation.keyword == "const"
            });
        return CandidateReadyReport::new(
            violations
                .into_iter()
                .map(|violation| LintFinding {
                    code: if role_only {
                        "AF_SIGNATURE_INVALID"
                    } else {
                        violation.code
                    },
                    pointer: violation.pointer,
                    message: violation.message,
                })
                .collect(),
            false,
        );
    }
    let submission: Submission = match serde_json::from_value(value.clone()) {
        Ok(submission) => submission,
        Err(error) => {
            return CandidateReadyReport::new(
                vec![finding("AF_SCHEMA_INVALID", "", error.to_string())],
                false,
            );
        }
    };
    CandidateReadyReport::new(lint_candidate_submission(&submission), true)
}

/// Run the static candidate-ready checks on a schema-valid typed Submission.
#[must_use]
pub fn lint_candidate_submission(submission: &Submission) -> Vec<LintFinding> {
    let mut findings = Vec::new();
    if submission.submission_kind != SubmissionKind::Candidate {
        findings.push(finding(
            "AF_LINT_SUBMISSION_KIND_NOT_CANDIDATE",
            "/submission_kind",
            "candidate-ready requires submission_kind=candidate",
        ));
    }
    if submission.terminal_outcome != TerminalOutcome::Pass {
        findings.push(finding(
            "AF_LINT_TERMINAL_OUTCOME_NOT_PASS",
            "/terminal_outcome",
            "candidate-ready requires terminal_outcome=PASS",
        ));
    }
    if submission.completed_stage != CompletedStage::CandidateReady {
        findings.push(finding(
            "AF_LINT_COMPLETED_STAGE_NOT_CANDIDATE_READY",
            "/completed_stage",
            "candidate-ready requires completed_stage=candidate_ready",
        ));
    }
    lint_candidate_identifiers(submission, &mut findings);
    lint_candidate_criteria(submission, &mut findings);
    lint_candidate_review_and_heads(submission, &mut findings);
    lint_candidate_evidence(submission, &mut findings);
    if submission.provenance.signature.signer_role != SignerRole::VerificationCoordinator {
        findings.push(finding(
            "AF_LINT_SIGNER_ROLE_INVALID",
            "/provenance/signature/signer_role",
            "candidate-ready manifests require verification_coordinator",
        ));
    }
    sort_findings(&mut findings);
    findings
}

fn lint_candidate_identifiers(submission: &Submission, findings: &mut Vec<LintFinding>) {
    let identifiers = [
        ("/candidate_id", submission.candidate_id),
        ("/candidate_artifact_id", submission.candidate_artifact_id),
        ("/verification_run_id", submission.verification_run_id),
    ];
    let mut seen = BTreeMap::new();
    for (pointer, identifier) in identifiers {
        let Some(identifier) = identifier else {
            findings.push(finding(
                "AF_LINT_CANDIDATE_IDENTIFIER_REQUIRED",
                pointer,
                "candidate-ready requires all candidate identity UUIDs",
            ));
            continue;
        };
        if identifier.is_nil() {
            findings.push(finding(
                "AF_LINT_CANDIDATE_IDENTIFIER_INVALID",
                pointer,
                "candidate identity UUIDs must not be nil",
            ));
        }
        if let Some(previous) = seen.insert(identifier, pointer) {
            findings.push(finding(
                "AF_LINT_CANDIDATE_IDENTIFIER_DUPLICATE",
                pointer,
                format!("candidate identity duplicates {previous}"),
            ));
        }
    }
}

fn lint_candidate_criteria(submission: &Submission, findings: &mut Vec<LintFinding>) {
    let Some(criteria) = &submission.criteria else {
        findings.push(finding(
            "AF_LINT_CRITERIA_REQUIRED",
            "/criteria",
            "candidate-ready requires criterion results",
        ));
        return;
    };
    if !criteria.iter().any(|criterion| criterion.hard) {
        findings.push(finding(
            "AF_LINT_HARD_CRITERION_REQUIRED",
            "/criteria",
            "candidate-ready requires at least one hard criterion result",
        ));
    }
    let mut seen = BTreeMap::new();
    for (index, criterion) in criteria.iter().enumerate() {
        if let Some(previous) = seen.insert(criterion.acceptance_id.as_str(), index) {
            findings.push(finding(
                "AF_LINT_DUPLICATE_CRITERION_RESULT",
                format!("/criteria/{index}/acceptance_id"),
                format!("criterion result duplicates /criteria/{previous}/acceptance_id"),
            ));
        }
        if criterion.hard && criterion.status != CriterionStatus::Pass {
            findings.push(finding(
                "AF_LINT_HARD_CRITERION_NOT_PASS",
                format!("/criteria/{index}/status"),
                format!("hard criterion {} is not PASS", criterion.acceptance_id),
            ));
        }
    }
}

fn lint_candidate_review_and_heads(submission: &Submission, findings: &mut Vec<LintFinding>) {
    let Some(git) = &submission.git else {
        findings.push(finding(
            "AF_LINT_GIT_FACTS_REQUIRED",
            "/git",
            "candidate-ready requires immutable candidate Git facts",
        ));
        return;
    };
    let Some(review) = &submission.review else {
        findings.push(finding(
            "AF_LINT_REVIEW_REQUIRED",
            "/review",
            "candidate-ready requires an independent review",
        ));
        return;
    };
    if review.verdict != ReviewVerdict::Pass {
        findings.push(finding(
            "AF_LINT_REVIEW_NOT_PASS",
            "/review/verdict",
            "candidate-ready requires review.verdict=pass",
        ));
    }
    if review.reviewer_id == submission.provenance.agent_id {
        findings.push(finding(
            "AF_LINT_REVIEW_NOT_INDEPENDENT",
            "/review/reviewer_id",
            "reviewer actor must differ from the author agent",
        ));
    }
    if review.reviewer_executor_id == submission.provenance.executor_id {
        findings.push(finding(
            "AF_LINT_REVIEW_NOT_INDEPENDENT",
            "/review/reviewer_executor_id",
            "reviewer executor must differ from the author executor",
        ));
    }
    if review.reviewed_head != git.candidate_commit {
        findings.push(finding(
            "AF_LINT_HEAD_MISMATCH",
            "/review/reviewed_head",
            "ReviewedHead must equal CandidateHead/SubmittedHead",
        ));
    }
    for (index, review_finding) in review.findings.iter().enumerate() {
        if matches!(
            review_finding.severity,
            FindingSeverity::Critical | FindingSeverity::High
        ) && review_finding.status == FindingStatus::Open
        {
            findings.push(finding(
                "AF_LINT_REVIEW_FINDING_UNRESOLVED",
                format!("/review/findings/{index}/status"),
                format!(
                    "{} is an unresolved critical/high finding",
                    review_finding.id
                ),
            ));
        }
    }
    let Some(reproduction) = &submission.clean_reproduction else {
        findings.push(finding(
            "AF_LINT_CLEAN_REPRODUCTION_REQUIRED",
            "/clean_reproduction",
            "candidate-ready requires clean reproduction",
        ));
        return;
    };
    if reproduction.verdict != ReproductionVerdict::Pass {
        findings.push(finding(
            "AF_LINT_CLEAN_REPRODUCTION_NOT_PASS",
            "/clean_reproduction/verdict",
            "candidate-ready requires clean_reproduction.verdict=pass",
        ));
    }
    if reproduction.tested_head != git.candidate_commit {
        findings.push(finding(
            "AF_LINT_HEAD_MISMATCH",
            "/clean_reproduction/tested_head",
            "TestedHead must equal CandidateHead/SubmittedHead",
        ));
    }
}

fn lint_candidate_evidence(submission: &Submission, findings: &mut Vec<LintFinding>) {
    let Some(bundle) = &submission.evidence_bundle else {
        findings.push(finding(
            "AF_LINT_EVIDENCE_BUNDLE_REQUIRED",
            "/evidence_bundle",
            "candidate-ready requires an evidence bundle manifest",
        ));
        return;
    };
    let Some(deliverables) = &submission.deliverables else {
        findings.push(finding(
            "AF_LINT_DELIVERABLES_REQUIRED",
            "/deliverables",
            "candidate-ready requires registered deliverables",
        ));
        return;
    };
    let mut deliverable_ids = BTreeMap::new();
    let mut evidence_deliverables = Vec::new();
    for (index, deliverable) in deliverables.iter().enumerate() {
        if let Some(previous) = deliverable_ids.insert(deliverable.id.as_str(), index) {
            findings.push(finding(
                "AF_LINT_DUPLICATE_DELIVERABLE_ID",
                format!("/deliverables/{index}/id"),
                format!("deliverable duplicates /deliverables/{previous}/id"),
            ));
        }
        if deliverable.kind == SubmissionDeliverableKind::EvidenceBundle {
            evidence_deliverables.push((index, deliverable));
        }
    }
    if evidence_deliverables.len() != 1 {
        findings.push(finding(
            "AF_LINT_EVIDENCE_DELIVERABLE_REQUIRED",
            "/deliverables",
            "candidate-ready requires exactly one evidence_bundle deliverable",
        ));
        return;
    }
    let (index, deliverable) = evidence_deliverables[0];
    if deliverable.sha256 != bundle.sha256 {
        findings.push(finding(
            "AF_LINT_EVIDENCE_DIGEST_MISMATCH",
            format!("/deliverables/{index}/sha256"),
            "evidence deliverable digest must equal evidence_bundle.sha256",
        ));
    }
    if deliverable.size_bytes != Some(bundle.size_bytes) {
        findings.push(finding(
            "AF_LINT_EVIDENCE_SIZE_MISMATCH",
            format!("/deliverables/{index}/size_bytes"),
            "evidence deliverable size must equal evidence_bundle.size_bytes",
        ));
    }
}

fn candidate_external_facts() -> Vec<ExternalFactRequirement> {
    vec![
        external_fact(
            "AF_EXTERNAL_AFWP_CONTRACT_REQUIRED",
            "/package_id",
            "published AFWP must confirm package tuple, base, criteria and required deliverables",
        ),
        external_fact(
            "AF_EXTERNAL_FENCING_PROVENANCE_REQUIRED",
            "/lease",
            "database history must confirm attempt ownership and current fencing at Candidate registration",
        ),
        external_fact(
            "AF_EXTERNAL_GIT_PROVENANCE_REQUIRED",
            "/git",
            "trusted Git must confirm reachability, tree hash, branch, changed paths and trailers",
        ),
        external_fact(
            "AF_EXTERNAL_EVIDENCE_BYTES_REQUIRED",
            "/evidence_bundle",
            "artifact storage must confirm bundle bytes, manifest digest and internal references",
        ),
        external_fact(
            "AF_EXTERNAL_SIGNATURE_TRUST_REQUIRED",
            "/provenance/signature",
            "identity registry and Ed25519 verification must confirm key role, status, revocation and signature",
        ),
    ]
}

fn external_fact(
    code: &'static str,
    pointer: impl Into<String>,
    message: impl Into<String>,
) -> ExternalFactRequirement {
    ExternalFactRequirement {
        code,
        pointer: pointer.into(),
        message: message.into(),
    }
}

/// Run publish semantics on a typed package (without repeating schema/hash).
#[must_use]
pub fn lint_package(package: &Afwp) -> Vec<LintFinding> {
    let mut findings = Vec::new();

    duplicate_ids(
        package.requirements.iter().map(|item| item.id.as_str()),
        "/requirements",
        "AF_LINT_DUPLICATE_REQUIREMENT_ID",
        &mut findings,
    );
    duplicate_ids(
        package
            .acceptance
            .criteria
            .iter()
            .map(|item| item.id.as_str()),
        "/acceptance/criteria",
        "AF_LINT_DUPLICATE_CRITERION_ID",
        &mut findings,
    );
    duplicate_ids(
        package.deliverables.iter().map(|item| item.id.as_str()),
        "/deliverables",
        "AF_LINT_DUPLICATE_DELIVERABLE_ID",
        &mut findings,
    );
    if let Some(interfaces) = &package.interfaces {
        duplicate_ids(
            interfaces.iter().map(|item| item.id.as_str()),
            "/interfaces",
            "AF_LINT_DUPLICATE_INTERFACE_ID",
            &mut findings,
        );
    }

    let requirement_ids: BTreeSet<_> = package
        .requirements
        .iter()
        .map(|requirement| requirement.id.as_str())
        .collect();
    let hard_coverage: BTreeSet<_> = package
        .acceptance
        .criteria
        .iter()
        .filter(|criterion| criterion.hard)
        .flat_map(|criterion| criterion.covers.iter().map(String::as_str))
        .collect();

    for (criterion_index, criterion) in package.acceptance.criteria.iter().enumerate() {
        for (cover_index, covered) in criterion.covers.iter().enumerate() {
            if !requirement_ids.contains(covered.as_str()) {
                findings.push(finding(
                    "AF_LINT_COVERS_UNKNOWN_REQUIREMENT",
                    format!("/acceptance/criteria/{criterion_index}/covers/{cover_index}"),
                    format!("criterion references missing requirement {covered}"),
                ));
            }
        }
    }
    for (index, requirement) in package.requirements.iter().enumerate() {
        if requirement.level == RequirementLevel::Must
            && !hard_coverage.contains(requirement.id.as_str())
        {
            findings.push(finding(
                "AF_LINT_MUST_REQUIREMENT_UNCOVERED",
                format!("/requirements/{index}/id"),
                format!(
                    "MUST requirement {} is not covered by a hard criterion",
                    requirement.id
                ),
            ));
        }
    }

    if !package
        .acceptance
        .criteria
        .iter()
        .any(|criterion| criterion.hard)
    {
        findings.push(finding(
            "AF_LINT_HARD_CRITERION_REQUIRED",
            "/acceptance/criteria",
            "at least one hard criterion is required",
        ));
    }
    lint_command_criteria(package, &mut findings);
    lint_changed_paths(package, &mut findings);
    lint_lease(package, &mut findings);
    lint_dependencies(package, &mut findings);
    lint_delegation_local(package, &mut findings);
    lint_branch_permissions(package, &mut findings);

    sort_findings(&mut findings);
    findings
}

fn lint_command_criteria(package: &Afwp, findings: &mut Vec<LintFinding>) {
    for (index, criterion) in package.acceptance.criteria.iter().enumerate() {
        let executable = matches!(
            criterion.kind,
            CriterionKind::Command
                | CriterionKind::IntegrationTest
                | CriterionKind::PropertyTest
                | CriterionKind::BenchmarkDelta
                | CriterionKind::SecurityScan
        );
        if !executable {
            continue;
        }
        if criterion.timeout_seconds.is_none() {
            findings.push(finding(
                "AF_LINT_COMMAND_TIMEOUT_REQUIRED",
                format!("/acceptance/criteria/{index}/timeout_seconds"),
                "executable criteria require an explicit timeout",
            ));
        }
        if criterion
            .runner_image
            .as_deref()
            .is_none_or(|image| !has_immutable_sha256_suffix(image))
        {
            findings.push(finding(
                "AF_LINT_RUNNER_DIGEST_REQUIRED",
                format!("/acceptance/criteria/{index}/runner_image"),
                "runner_image must be pinned by an immutable sha256 digest",
            ));
        }
    }
}

fn has_immutable_sha256_suffix(image: &str) -> bool {
    let Some((name, digest)) = image.rsplit_once("@sha256:") else {
        return false;
    };
    !name.is_empty()
        && digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn lint_changed_paths(package: &Afwp, findings: &mut Vec<LintFinding>) {
    let hard_gates: Vec<_> = package
        .acceptance
        .criteria
        .iter()
        .enumerate()
        .filter(|(_, criterion)| criterion.kind == CriterionKind::ChangedPaths && criterion.hard)
        .collect();
    if hard_gates.is_empty() {
        findings.push(finding(
            "AF_LINT_CHANGED_PATHS_REQUIRED",
            "/acceptance/criteria",
            "a hard changed_paths criterion is required",
        ));
        return;
    }

    let allowed: BTreeSet<_> = hard_gates
        .iter()
        .flat_map(|(_, criterion)| criterion.allow.iter().flatten().map(String::as_str))
        .collect();
    let denied: BTreeSet<_> = hard_gates
        .iter()
        .flat_map(|(_, criterion)| criterion.deny.iter().flatten().map(String::as_str))
        .collect();
    let pointer_index = hard_gates[0].0;
    for path in &package.scope.allowed_paths {
        if !allowed.contains(path.as_str()) {
            findings.push(finding(
                "AF_LINT_CHANGED_PATH_ALLOW_INCOMPLETE",
                format!("/acceptance/criteria/{pointer_index}/allow"),
                format!("changed_paths allow does not cover {path}"),
            ));
        }
    }
    for (criterion_index, criterion) in &hard_gates {
        for (allow_index, path) in criterion.allow.iter().flatten().enumerate() {
            if !package.scope.allowed_paths.contains(path) {
                findings.push(finding(
                    "AF_LINT_CHANGED_PATH_ALLOW_ESCALATION",
                    format!("/acceptance/criteria/{criterion_index}/allow/{allow_index}"),
                    format!("changed_paths allow adds undeclared scope pattern {path}"),
                ));
            }
        }
    }
    for path in &package.scope.forbidden_paths {
        if !denied.contains(path.as_str()) {
            findings.push(finding(
                "AF_LINT_CHANGED_PATH_DENY_INCOMPLETE",
                format!("/acceptance/criteria/{pointer_index}/deny"),
                format!("changed_paths deny does not cover {path}"),
            ));
        }
    }
}

fn lint_lease(package: &Afwp, findings: &mut Vec<LintFinding>) {
    let lease = &package.scheduling.lease;
    if lease.renew_after_seconds >= lease.ttl_seconds {
        findings.push(finding(
            "AF_LINT_LEASE_RENEW_NOT_BEFORE_TTL",
            "/scheduling/lease/renew_after_seconds",
            "renew_after_seconds must be less than ttl_seconds",
        ));
    }
    if lease.ttl_seconds > lease.max_execution_seconds {
        findings.push(finding(
            "AF_LINT_LEASE_TTL_EXCEEDS_EXECUTION",
            "/scheduling/lease/ttl_seconds",
            "ttl_seconds must not exceed max_execution_seconds",
        ));
    }
    if package.scheduling.mode != SchedulingMode::Redundant
        && package.scheduling.redundancy.is_some()
    {
        findings.push(finding(
            "AF_LINT_REDUNDANCY_FORBIDDEN",
            "/scheduling/redundancy",
            "redundancy is only meaningful in redundant mode",
        ));
    }
}

fn lint_dependencies(package: &Afwp, findings: &mut Vec<LintFinding>) {
    let mut seen = BTreeSet::new();
    for (index, dependency) in package.dependencies.iter().enumerate() {
        if dependency.package_id == package.package_id {
            findings.push(finding(
                "AF_LINT_DEPENDENCY_SELF_CYCLE",
                format!("/dependencies/{index}/package_id"),
                "a package cannot depend on itself",
            ));
        }
        let key = (
            dependency.package_id.as_str(),
            dependency.revision,
            dependency.edge,
            dependency.condition,
        );
        if !seen.insert(key) {
            findings.push(finding(
                "AF_LINT_DUPLICATE_DEPENDENCY",
                format!("/dependencies/{index}"),
                "duplicate dependency edge",
            ));
        }
    }
    for (index, package_id) in package.conflicts.integration_after.iter().enumerate() {
        if !package
            .dependencies
            .iter()
            .any(|dependency| dependency.package_id == *package_id)
        {
            findings.push(finding(
                "AF_LINT_INTEGRATION_DEPENDENCY_MISSING",
                format!("/conflicts/integration_after/{index}"),
                format!("integration_after package {package_id} has no exact dependency"),
            ));
        }
    }
}

fn lint_delegation_local(package: &Afwp, findings: &mut Vec<LintFinding>) {
    if package.parent_id.as_deref() == Some(package.package_id.as_str()) {
        findings.push(finding(
            "AF_LINT_PARENT_SELF_CYCLE",
            "/parent_id",
            "a package cannot be its own parent",
        ));
    }
    let Some(delegation) = &package.delegation else {
        return;
    };
    let kinds_nonempty = delegation
        .allowed_kinds
        .as_ref()
        .is_some_and(|kinds| !kinds.is_empty());
    if !delegation.allowed
        && (delegation.max_depth != 0
            || delegation.max_children != 0
            || delegation.max_budget_units != 0.0
            || kinds_nonempty)
    {
        findings.push(finding(
            "AF_LINT_DELEGATION_DISABLED_GRANT",
            "/delegation",
            "delegation=false requires zero limits and no allowed kinds",
        ));
    }
    if delegation.max_budget_units > package.scheduling.max_budget_units {
        findings.push(finding(
            "AF_LINT_DELEGATION_BUDGET_EXCEEDS_PACKAGE",
            "/delegation/max_budget_units",
            "delegation budget cannot exceed the package budget",
        ));
    }
}

fn lint_branch_permissions(package: &Afwp, findings: &mut Vec<LintFinding>) {
    let prefix = package
        .permissions
        .git_write_prefix
        .trim_start_matches("refs/heads/");
    if grants_protected_branch(prefix) {
        findings.push(finding(
            "AF_LINT_PROTECTED_BRANCH_GRANTED",
            "/permissions/git_write_prefix",
            "git_write_prefix must not grant a protected branch",
        ));
    }
    let branch = package.completion.branch_pattern.as_str();
    if is_protected_branch(branch) {
        findings.push(finding(
            "AF_LINT_PROTECTED_BRANCH_GRANTED",
            "/completion/branch_pattern",
            "completion branch_pattern must not name a protected branch",
        ));
    }
}

fn is_protected_branch(value: &str) -> bool {
    matches!(value, "main" | "master" | "develop" | "production")
        || value.starts_with("main/")
        || value.starts_with("master/")
        || value.starts_with("production/")
}

fn grants_protected_branch(prefix: &str) -> bool {
    prefix.is_empty()
        || ["main", "master", "develop", "production"]
            .iter()
            .any(|protected| protected.starts_with(prefix))
        || is_protected_branch(prefix)
}

/// Validate exact dependencies, DAG shape and delegation subset rules across a
/// complete WorkGraph snapshot. Package-local publish linting intentionally
/// cannot claim these checks without the other graph members.
#[must_use]
pub fn lint_work_graph(packages: &[Afwp]) -> Vec<LintFinding> {
    let mut findings = Vec::new();
    let mut by_id = BTreeMap::new();
    for (index, package) in packages.iter().enumerate() {
        if let Some(previous) = by_id.insert(package.package_id.as_str(), index) {
            findings.push(graph_finding(
                "AF_LINT_GRAPH_DUPLICATE_PACKAGE_ID",
                index,
                "/package_id",
                format!("package_id also appears at /packages/{previous}/package_id"),
            ));
        }
    }

    let mut adjacency = vec![Vec::<(usize, usize)>::new(); packages.len()];
    for (package_index, package) in packages.iter().enumerate() {
        for (dependency_index, dependency) in package.dependencies.iter().enumerate() {
            match by_id.get(dependency.package_id.as_str()).copied() {
                None => findings.push(graph_finding(
                    "AF_LINT_DEPENDENCY_PACKAGE_MISSING",
                    package_index,
                    &format!("/dependencies/{dependency_index}/package_id"),
                    format!("dependency package {} is absent", dependency.package_id),
                )),
                Some(target_index) => {
                    if packages[target_index].revision != dependency.revision {
                        findings.push(graph_finding(
                            "AF_LINT_DEPENDENCY_REVISION_MISMATCH",
                            package_index,
                            &format!("/dependencies/{dependency_index}/revision"),
                            format!(
                                "dependency pins revision {}, graph contains {}",
                                dependency.revision, packages[target_index].revision
                            ),
                        ));
                    }
                    if packages[target_index].project_id != package.project_id {
                        findings.push(graph_finding(
                            "AF_LINT_DEPENDENCY_PROJECT_MISMATCH",
                            package_index,
                            &format!("/dependencies/{dependency_index}/package_id"),
                            "dependency target belongs to a different project",
                        ));
                    }
                    if packages[target_index].graph_version != package.graph_version {
                        findings.push(graph_finding(
                            "AF_LINT_DEPENDENCY_GRAPH_VERSION_MISMATCH",
                            package_index,
                            &format!("/dependencies/{dependency_index}/package_id"),
                            "dependency target belongs to a different graph_version",
                        ));
                    }
                    adjacency[package_index].push((target_index, dependency_index));
                }
            }
        }
    }
    lint_graph_cycles(&adjacency, &mut findings);
    lint_graph_delegation(packages, &by_id, &mut findings);
    sort_findings(&mut findings);
    findings
}

/// Strictly validate a WorkGraph encoded either as `{ "packages": [AFWP...] }`
/// or directly as `[AFWP...]`.
///
/// JSON Pointers in the result are normalized to the object form so callers do
/// not need separate assertion logic for the two encodings.
#[must_use]
pub fn lint_work_graph_json(input: &[u8]) -> GraphLintReport {
    let root = match strict_json::from_slice(input) {
        Ok(value) => value,
        Err(error) => {
            return GraphLintReport {
                valid: false,
                findings: vec![finding("AF_SCHEMA_INVALID", "", error.to_string())],
            };
        }
    };
    let package_values = match root {
        Value::Array(packages) => packages,
        Value::Object(mut object) => {
            if object.len() != 1 || !object.contains_key("packages") {
                return GraphLintReport {
                    valid: false,
                    findings: vec![finding(
                        "AF_GRAPH_FORMAT_INVALID",
                        "",
                        "graph object must contain exactly one member: packages",
                    )],
                };
            }
            match object.remove("packages") {
                Some(Value::Array(packages)) => packages,
                _ => {
                    return GraphLintReport {
                        valid: false,
                        findings: vec![finding(
                            "AF_GRAPH_FORMAT_INVALID",
                            "/packages",
                            "packages must be an array",
                        )],
                    };
                }
            }
        }
        _ => {
            return GraphLintReport {
                valid: false,
                findings: vec![finding(
                    "AF_GRAPH_FORMAT_INVALID",
                    "",
                    "graph must be an object with packages or an AFWP array",
                )],
            };
        }
    };

    if package_values.is_empty() {
        return GraphLintReport {
            valid: false,
            findings: vec![finding(
                "AF_GRAPH_FORMAT_INVALID",
                "/packages",
                "packages must contain at least one AFWP",
            )],
        };
    }

    let mut findings = Vec::new();
    let mut packages = Vec::with_capacity(package_values.len());
    let mut every_package_decoded = true;
    for (index, value) in package_values.iter().enumerate() {
        let report = lint_publish_value(value);
        findings.extend(report.findings.into_iter().map(|mut item| {
            item.pointer = format!("/packages/{index}{}", item.pointer);
            item
        }));
        match schema::validate(SchemaKind::Afwp, value) {
            Ok(()) => match serde_json::from_value::<Afwp>(value.clone()) {
                Ok(package) => packages.push(package),
                Err(_) => every_package_decoded = false,
            },
            Err(_) => every_package_decoded = false,
        }
    }
    if every_package_decoded {
        findings.extend(lint_work_graph(&packages));
    }
    sort_findings(&mut findings);
    GraphLintReport {
        valid: findings.is_empty(),
        findings,
    }
}

fn lint_graph_cycles(adjacency: &[Vec<(usize, usize)>], findings: &mut Vec<LintFinding>) {
    fn visit(
        node: usize,
        adjacency: &[Vec<(usize, usize)>],
        colors: &mut [u8],
        findings: &mut Vec<LintFinding>,
    ) {
        colors[node] = 1;
        for &(next, dependency_index) in &adjacency[node] {
            match colors[next] {
                0 => visit(next, adjacency, colors, findings),
                1 => findings.push(graph_finding(
                    "AF_LINT_DEPENDENCY_CYCLE",
                    node,
                    &format!("/dependencies/{dependency_index}/package_id"),
                    "dependency edge closes a directed cycle",
                )),
                _ => {}
            }
        }
        colors[node] = 2;
    }

    let mut colors = vec![0_u8; adjacency.len()];
    for node in 0..adjacency.len() {
        if colors[node] == 0 {
            visit(node, adjacency, &mut colors, findings);
        }
    }
}

fn lint_graph_delegation(
    packages: &[Afwp],
    by_id: &BTreeMap<&str, usize>,
    findings: &mut Vec<LintFinding>,
) {
    let mut children: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (child_index, child) in packages.iter().enumerate() {
        let Some(parent_id) = child.parent_id.as_deref() else {
            continue;
        };
        let Some(parent_index) = by_id.get(parent_id).copied() else {
            findings.push(graph_finding(
                "AF_LINT_PARENT_PACKAGE_MISSING",
                child_index,
                "/parent_id",
                format!("parent package {parent_id} is absent"),
            ));
            continue;
        };
        children.entry(parent_index).or_default().push(child_index);
        let parent = &packages[parent_index];
        if parent.project_id != child.project_id || parent.graph_version != child.graph_version {
            findings.push(graph_finding(
                "AF_LINT_PARENT_GRAPH_MISMATCH",
                child_index,
                "/parent_id",
                "parent and child must belong to the same project and graph_version",
            ));
        }
        let Some(grant) = parent.delegation.as_ref().filter(|grant| grant.allowed) else {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_UNAUTHORIZED",
                child_index,
                "/parent_id",
                format!("parent {} does not authorize delegation", parent.package_id),
            ));
            continue;
        };
        lint_child_subset(parent, grant, child, child_index, findings);
    }

    for (parent_index, child_indices) in children {
        let parent = &packages[parent_index];
        let Some(grant) = &parent.delegation else {
            continue;
        };
        if child_indices.len() as u64 > grant.max_children {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_CHILD_LIMIT_EXCEEDED",
                parent_index,
                "/delegation/max_children",
                format!(
                    "{} direct children exceed limit {}",
                    child_indices.len(),
                    grant.max_children
                ),
            ));
        }
        let total_budget: f64 = child_indices
            .iter()
            .map(|&index| packages[index].scheduling.max_budget_units)
            .sum();
        if total_budget > grant.max_budget_units {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_BUDGET_EXCEEDED",
                parent_index,
                "/delegation/max_budget_units",
                format!("child budgets total {total_budget}"),
            ));
        }
    }

    for (index, _) in packages.iter().enumerate() {
        let mut descendant = index;
        let mut distance = 1_u64;
        let mut seen = BTreeSet::from([index]);
        while let Some(parent_id) = packages[descendant].parent_id.as_deref() {
            let Some(parent_index) = by_id.get(parent_id).copied() else {
                break;
            };
            if !seen.insert(parent_index) {
                findings.push(graph_finding(
                    "AF_LINT_PARENT_CYCLE",
                    descendant,
                    "/parent_id",
                    "parent lineage contains a directed cycle",
                ));
                break;
            }
            if let Some(grant) = &packages[parent_index].delegation
                && distance > grant.max_depth
            {
                findings.push(graph_finding(
                    "AF_LINT_DELEGATION_DEPTH_EXCEEDED",
                    index,
                    "/parent_id",
                    format!(
                        "descendant distance {distance} exceeds ancestor grant {}",
                        grant.max_depth,
                    ),
                ));
                break;
            }
            descendant = parent_index;
            distance += 1;
        }
    }
}

fn lint_child_subset(
    parent: &Afwp,
    grant: &Delegation,
    child: &Afwp,
    child_index: usize,
    findings: &mut Vec<LintFinding>,
) {
    if grant
        .allowed_kinds
        .as_ref()
        .is_none_or(|kinds| !kinds.contains(&child.kind))
    {
        findings.push(graph_finding(
            "AF_LINT_DELEGATION_KIND_FORBIDDEN",
            child_index,
            "/kind",
            "child kind is outside the parent grant",
        ));
    }
    if child.scheduling.max_budget_units > grant.max_budget_units {
        findings.push(graph_finding(
            "AF_LINT_DELEGATION_BUDGET_EXCEEDED",
            child_index,
            "/scheduling/max_budget_units",
            "child budget exceeds the parent delegation grant",
        ));
    }
    if let Some(child_grant) = &child.delegation {
        let child_kinds: BTreeSet<_> = child_grant
            .allowed_kinds
            .iter()
            .flatten()
            .copied()
            .collect();
        let parent_kinds: BTreeSet<_> = grant.allowed_kinds.iter().flatten().copied().collect();
        let depth_limit = grant.max_depth.saturating_sub(1);
        if child_grant.max_depth > depth_limit
            || child_grant.max_children > grant.max_children
            || child_grant.max_budget_units > grant.max_budget_units
            || !child_kinds.is_subset(&parent_kinds)
        {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_GRANT_ESCALATION",
                child_index,
                "/delegation",
                "child delegation grant exceeds its parent grant",
            ));
        }
    }
    let parent_network: BTreeSet<_> = parent
        .permissions
        .network_allowlist
        .iter()
        .map(String::as_str)
        .collect();
    for (index, domain) in child.permissions.network_allowlist.iter().enumerate() {
        if !parent_network.contains(domain.as_str()) {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_PERMISSION_ESCALATION",
                child_index,
                &format!("/permissions/network_allowlist/{index}"),
                format!("network domain {domain} is absent from the parent grant"),
            ));
        }
    }
    if !child
        .permissions
        .git_write_prefix
        .starts_with(&parent.permissions.git_write_prefix)
    {
        findings.push(graph_finding(
            "AF_LINT_DELEGATION_PERMISSION_ESCALATION",
            child_index,
            "/permissions/git_write_prefix",
            "child git prefix is not nested under the parent prefix",
        ));
    }
    if side_effect_rank(child.permissions.external_side_effects)
        > side_effect_rank(parent.permissions.external_side_effects)
    {
        findings.push(graph_finding(
            "AF_LINT_DELEGATION_PERMISSION_ESCALATION",
            child_index,
            "/permissions/external_side_effects",
            "child external side-effect authority exceeds the parent",
        ));
    }
    let parent_secrets: BTreeSet<_> = parent
        .permissions
        .secrets
        .iter()
        .map(|secret| (&secret.name, secret.delivery, &secret.scope))
        .collect();
    for (index, secret) in child.permissions.secrets.iter().enumerate() {
        if !parent_secrets.contains(&(&secret.name, secret.delivery, &secret.scope)) {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_PERMISSION_ESCALATION",
                child_index,
                &format!("/permissions/secrets/{index}"),
                format!("secret grant {} is absent from the parent", secret.name),
            ));
        }
    }
    if let Some(parent_limit) = parent.permissions.max_artifact_bytes
        && child
            .permissions
            .max_artifact_bytes
            .is_none_or(|child_limit| child_limit > parent_limit)
    {
        findings.push(graph_finding(
            "AF_LINT_DELEGATION_PERMISSION_ESCALATION",
            child_index,
            "/permissions/max_artifact_bytes",
            "child artifact limit exceeds or removes the parent limit",
        ));
    }
    let parent_paths: BTreeSet<_> = parent
        .scope
        .allowed_paths
        .iter()
        .map(String::as_str)
        .collect();
    for (index, path) in child.scope.allowed_paths.iter().enumerate() {
        if !parent_paths.contains(path.as_str()) {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_SCOPE_ESCALATION",
                child_index,
                &format!("/scope/allowed_paths/{index}"),
                format!("child path pattern {path} is absent from parent scope"),
            ));
        }
    }
    let child_forbidden: BTreeSet<_> = child
        .scope
        .forbidden_paths
        .iter()
        .map(String::as_str)
        .collect();
    for path in &parent.scope.forbidden_paths {
        if !child_forbidden.contains(path.as_str()) {
            findings.push(graph_finding(
                "AF_LINT_DELEGATION_SCOPE_ESCALATION",
                child_index,
                "/scope/forbidden_paths",
                format!("child scope does not preserve parent prohibition {path}"),
            ));
        }
    }
}

const fn side_effect_rank(value: ExternalSideEffects) -> u8 {
    match value {
        ExternalSideEffects::Deny => 0,
        ExternalSideEffects::ApprovalRequired => 1,
        ExternalSideEffects::AllowDeclared => 2,
    }
}

fn duplicate_ids<'a>(
    ids: impl Iterator<Item = &'a str>,
    base_pointer: &str,
    code: &'static str,
    findings: &mut Vec<LintFinding>,
) {
    let mut seen = BTreeMap::new();
    for (index, id) in ids.enumerate() {
        if let Some(first) = seen.insert(id, index) {
            findings.push(finding(
                code,
                format!("{base_pointer}/{index}/id"),
                format!("duplicate id {id}; first declared at {base_pointer}/{first}/id"),
            ));
        }
    }
}

fn graph_finding(
    code: &'static str,
    package_index: usize,
    suffix: &str,
    message: impl Into<String>,
) -> LintFinding {
    finding(code, format!("/packages/{package_index}{suffix}"), message)
}

fn finding(
    code: &'static str,
    pointer: impl Into<String>,
    message: impl Into<String>,
) -> LintFinding {
    LintFinding {
        code,
        pointer: pointer.into(),
        message: message.into(),
    }
}

fn sort_findings(findings: &mut Vec<LintFinding>) {
    findings.sort_by(|left, right| {
        (&left.pointer, left.code, &left.message).cmp(&(&right.pointer, right.code, &right.message))
    });
    findings.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &[u8] = include_bytes!("../../../../examples/afwp-lease-fencing.json");

    #[test]
    fn repository_example_is_publish_ready() {
        let report = lint_publish(EXAMPLE);
        assert!(report.valid, "{:#?}", report.findings);
    }

    #[test]
    fn duplicate_json_key_is_a_stable_failure() {
        let report = lint_publish(br#"{"schema_version":"afwp/1.0","schema_version":"afwp/1.0"}"#);
        assert_eq!(report.findings[0].code, "AF_SCHEMA_INVALID");
    }

    #[test]
    fn graph_detects_missing_exact_dependency() {
        let value = strict_json::from_slice(EXAMPLE).unwrap();
        let package: Afwp = serde_json::from_value(value).unwrap();
        let findings = lint_work_graph(&[package]);
        assert!(
            findings
                .iter()
                .any(|item| item.code == "AF_LINT_DEPENDENCY_PACKAGE_MISSING")
        );
    }
}
