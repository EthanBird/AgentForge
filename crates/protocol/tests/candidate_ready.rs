use agentforge_protocol::lint::lint_candidate_ready;
use agentforge_protocol::strict_json;
use serde_json::{Value, json};

const CANDIDATE: &[u8] = include_bytes!("../../../examples/submission-lease-fencing.json");
const SALVAGE: &[u8] = include_bytes!("../../../examples/submission-salvage-lease-fencing.json");

fn candidate() -> Value {
    strict_json::from_slice(CANDIDATE).unwrap()
}

fn codes(value: &Value) -> Vec<(&'static str, String)> {
    lint_candidate_ready(&serde_json::to_vec(value).unwrap())
        .findings
        .into_iter()
        .map(|finding| (finding.code, finding.pointer))
        .collect()
}

fn assert_finding(value: &Value, code: &'static str, pointer: &str) {
    let findings = codes(value);
    assert!(
        findings
            .iter()
            .any(|finding| finding.0 == code && finding.1 == pointer),
        "expected {code} at {pointer}, got {findings:?}"
    );
}

#[test]
fn repository_candidate_is_statically_ready_and_lists_external_facts() {
    let report = lint_candidate_ready(CANDIDATE);
    assert!(report.valid, "{:?}", report.findings);
    assert!(report.external_facts_required.iter().any(|fact| {
        fact.code == "AF_EXTERNAL_SIGNATURE_TRUST_REQUIRED"
            && fact.pointer == "/provenance/signature"
    }));
}

#[test]
fn every_head_equality_has_a_mutation_failure() {
    let replacement = json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    let mut reviewed = candidate();
    reviewed["review"]["reviewed_head"] = replacement.clone();
    assert_finding(&reviewed, "AF_LINT_HEAD_MISMATCH", "/review/reviewed_head");

    let mut tested = candidate();
    tested["clean_reproduction"]["tested_head"] = replacement.clone();
    assert_finding(
        &tested,
        "AF_LINT_HEAD_MISMATCH",
        "/clean_reproduction/tested_head",
    );

    let mut submitted_candidate = candidate();
    submitted_candidate["git"]["candidate_commit"] = replacement;
    let findings = codes(&submitted_candidate);
    assert!(findings.iter().any(|finding| {
        finding.0 == "AF_LINT_HEAD_MISMATCH" && finding.1 == "/review/reviewed_head"
    }));
    assert!(findings.iter().any(|finding| {
        finding.0 == "AF_LINT_HEAD_MISMATCH" && finding.1 == "/clean_reproduction/tested_head"
    }));
}

#[test]
fn every_hard_failure_status_is_rejected() {
    for status in ["FAIL", "INCONCLUSIVE", "SKIPPED"] {
        let mut value = candidate();
        value["criteria"][0]["status"] = json!(status);
        assert_finding(
            &value,
            "AF_LINT_HARD_CRITERION_NOT_PASS",
            "/criteria/0/status",
        );
    }

    let mut no_hard = candidate();
    for criterion in no_hard["criteria"].as_array_mut().unwrap() {
        criterion["hard"] = json!(false);
    }
    assert_finding(&no_hard, "AF_LINT_HARD_CRITERION_REQUIRED", "/criteria");
}

#[test]
fn review_failure_and_unresolved_high_severity_are_rejected() {
    for verdict in ["fail", "blocked"] {
        let mut value = candidate();
        value["review"]["verdict"] = json!(verdict);
        assert_finding(&value, "AF_LINT_REVIEW_NOT_PASS", "/review/verdict");
    }
    for severity in ["critical", "high"] {
        let mut value = candidate();
        value["review"]["findings"][0]["severity"] = json!(severity);
        value["review"]["findings"][0]["status"] = json!("open");
        assert_finding(
            &value,
            "AF_LINT_REVIEW_FINDING_UNRESOLVED",
            "/review/findings/0/status",
        );
    }
}

#[test]
fn reviewer_actor_and_executor_must_be_independent() {
    let mut actor = candidate();
    actor["review"]["reviewer_id"] = actor["provenance"]["agent_id"].clone();
    assert_finding(
        &actor,
        "AF_LINT_REVIEW_NOT_INDEPENDENT",
        "/review/reviewer_id",
    );

    let mut executor = candidate();
    executor["review"]["reviewer_executor_id"] = executor["provenance"]["executor_id"].clone();
    assert_finding(
        &executor,
        "AF_LINT_REVIEW_NOT_INDEPENDENT",
        "/review/reviewer_executor_id",
    );
}

#[test]
fn every_clean_reproduction_failure_is_rejected() {
    for verdict in ["fail", "inconclusive"] {
        let mut value = candidate();
        value["clean_reproduction"]["verdict"] = json!(verdict);
        assert_finding(
            &value,
            "AF_LINT_CLEAN_REPRODUCTION_NOT_PASS",
            "/clean_reproduction/verdict",
        );
    }
}

#[test]
fn outcome_stage_and_kind_are_profile_gates() {
    for outcome in ["FAIL", "INCONCLUSIVE"] {
        let mut value = candidate();
        value["terminal_outcome"] = json!(outcome);
        value["completed_stage"] = json!("reproducing");
        value["failure_dossier"] = json!({
            "code": "AF_TEST_FAILURE",
            "summary": "fixture failure",
            "retryable": false,
            "evidence_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "evidence_refs": ["artifact://failure/report.json"]
        });
        assert_finding(
            &value,
            "AF_LINT_TERMINAL_OUTCOME_NOT_PASS",
            "/terminal_outcome",
        );
        assert_finding(
            &value,
            "AF_LINT_COMPLETED_STAGE_NOT_CANDIDATE_READY",
            "/completed_stage",
        );
    }

    let salvage = strict_json::from_slice(SALVAGE).unwrap();
    assert_finding(
        &salvage,
        "AF_LINT_SUBMISSION_KIND_NOT_CANDIDATE",
        "/submission_kind",
    );
}

#[test]
fn evidence_manifest_and_deliverable_must_agree() {
    let mut digest = candidate();
    digest["deliverables"][3]["sha256"] =
        json!("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert_finding(
        &digest,
        "AF_LINT_EVIDENCE_DIGEST_MISMATCH",
        "/deliverables/3/sha256",
    );

    let mut size = candidate();
    size["deliverables"][3]["size_bytes"] = json!(1);
    assert_finding(
        &size,
        "AF_LINT_EVIDENCE_SIZE_MISMATCH",
        "/deliverables/3/size_bytes",
    );

    let mut missing = candidate();
    missing["deliverables"].as_array_mut().unwrap().pop();
    assert_finding(
        &missing,
        "AF_LINT_EVIDENCE_DELIVERABLE_REQUIRED",
        "/deliverables",
    );
}

#[test]
fn candidate_identity_uuid_set_is_non_nil_and_distinct() {
    let mut duplicate = candidate();
    duplicate["candidate_artifact_id"] = duplicate["candidate_id"].clone();
    assert_finding(
        &duplicate,
        "AF_LINT_CANDIDATE_IDENTIFIER_DUPLICATE",
        "/candidate_artifact_id",
    );

    let mut nil = candidate();
    nil["verification_run_id"] = json!("00000000-0000-0000-0000-000000000000");
    assert_finding(
        &nil,
        "AF_LINT_CANDIDATE_IDENTIFIER_INVALID",
        "/verification_run_id",
    );
}

#[test]
fn candidate_signer_role_structure_is_schema_enforced() {
    let mut value = candidate();
    value["provenance"]["signature"]["signer_role"] = json!("salvage_registrar");
    assert_finding(
        &value,
        "AF_SIGNATURE_INVALID",
        "/provenance/signature/signer_role",
    );
}

#[test]
fn duplicate_criterion_and_deliverable_ids_are_rejected() {
    let mut criteria = candidate();
    let duplicate = criteria["criteria"][0].clone();
    criteria["criteria"].as_array_mut().unwrap().push(duplicate);
    assert_finding(
        &criteria,
        "AF_LINT_DUPLICATE_CRITERION_RESULT",
        "/criteria/5/acceptance_id",
    );

    let mut deliverables = candidate();
    deliverables["deliverables"][1]["id"] = deliverables["deliverables"][0]["id"].clone();
    assert_finding(
        &deliverables,
        "AF_LINT_DUPLICATE_DELIVERABLE_ID",
        "/deliverables/1/id",
    );
}
