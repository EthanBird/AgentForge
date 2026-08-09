use agentforge_protocol::canonical::compute_package_hash;
use agentforge_protocol::lint::{lint_publish, lint_work_graph_json};
use agentforge_protocol::schema::{self, SchemaKind};
use agentforge_protocol::strict_json;
use serde_json::{Value, json};

const EXAMPLE: &[u8] = include_bytes!("../../../examples/afwp-lease-fencing.json");

fn example() -> Value {
    strict_json::from_slice(EXAMPLE).unwrap()
}

fn lint_rehashed(mut value: Value) -> Vec<&'static str> {
    let hash = compute_package_hash(&value).unwrap();
    value["package_hash"] = Value::String(hash);
    lint_publish(&serde_json::to_vec(&value).unwrap())
        .findings
        .into_iter()
        .map(|finding| finding.code)
        .collect()
}

#[test]
fn schema_negatives_report_precise_pointers() {
    let mut missing = example();
    missing.as_object_mut().unwrap().remove("package_id");
    let report = lint_publish(&serde_json::to_vec(&missing).unwrap());
    assert!(
        report
            .findings
            .iter()
            .any(|item| { item.code == "AF_SCHEMA_INVALID" && item.pointer == "/package_id" })
    );

    let mut unknown = example();
    unknown["scope"]["surprise"] = json!(true);
    let report = lint_publish(&serde_json::to_vec(&unknown).unwrap());
    assert!(
        report
            .findings
            .iter()
            .any(|item| { item.code == "AF_SCHEMA_INVALID" && item.pointer == "/scope/surprise" })
    );

    for (pointer, replacement) in [
        ("/package_id", json!("not-a-package")),
        ("/package_hash", json!("sha256:ABC")),
        ("/snapshot/base_commit", json!("deadbeef")),
    ] {
        let mut value = example();
        *value.pointer_mut(pointer).unwrap() = replacement;
        let report = lint_publish(&serde_json::to_vec(&value).unwrap());
        assert!(
            report
                .findings
                .iter()
                .any(|item| { item.code == "AF_SCHEMA_INVALID" && item.pointer == pointer }),
            "{pointer}: {:?}",
            report.findings
        );
    }
}

#[test]
fn every_top_level_required_member_has_its_own_pointer() {
    let schema = schema::document(SchemaKind::Afwp).unwrap();
    let required = schema["required"].as_array().unwrap();
    for member in required {
        let member = member.as_str().unwrap();
        let mut value = example();
        value.as_object_mut().unwrap().remove(member);
        let report = lint_publish(&serde_json::to_vec(&value).unwrap());
        let pointer = format!("/{member}");
        assert!(
            report
                .findings
                .iter()
                .any(|item| item.code == "AF_SCHEMA_INVALID" && item.pointer == pointer),
            "missing {member}: {:?}",
            report.findings
        );
    }
}

#[test]
fn publish_semantics_have_stable_codes() {
    let mut duplicate = example();
    duplicate["requirements"][1]["id"] = duplicate["requirements"][0]["id"].clone();
    assert!(lint_rehashed(duplicate).contains(&"AF_LINT_DUPLICATE_REQUIREMENT_ID"));

    let mut dangling = example();
    dangling["acceptance"]["criteria"][0]["covers"][0] = json!("REQ-MISSING-99");
    let codes = lint_rehashed(dangling);
    assert!(codes.contains(&"AF_LINT_COVERS_UNKNOWN_REQUIREMENT"));
    assert!(codes.contains(&"AF_LINT_MUST_REQUIREMENT_UNCOVERED"));

    let mut uncovered = example();
    uncovered["acceptance"]["criteria"][0]["hard"] = json!(false);
    assert!(lint_rehashed(uncovered).contains(&"AF_LINT_MUST_REQUIREMENT_UNCOVERED"));

    let mut lease = example();
    lease["scheduling"]["lease"]["renew_after_seconds"] = json!(1200);
    assert!(lint_rehashed(lease).contains(&"AF_LINT_LEASE_RENEW_NOT_BEFORE_TTL"));

    let mut changed_paths = example();
    changed_paths["acceptance"]["criteria"][3]["deny"] = json!([".github/**"]);
    assert!(lint_rehashed(changed_paths).contains(&"AF_LINT_CHANGED_PATH_DENY_INCOMPLETE"));

    let mut widened_paths = example();
    widened_paths["acceptance"]["criteria"][3]["allow"]
        .as_array_mut()
        .unwrap()
        .push(json!("**"));
    assert!(lint_rehashed(widened_paths).contains(&"AF_LINT_CHANGED_PATH_ALLOW_ESCALATION"));

    let mut protected_prefix = example();
    protected_prefix["permissions"]["git_write_prefix"] = json!("mai");
    assert!(lint_rehashed(protected_prefix).contains(&"AF_LINT_PROTECTED_BRANCH_GRANTED"));
}

#[test]
fn duplicate_key_is_rejected_before_schema_validation() {
    let report = lint_publish(br#"{"schema_version":"afwp/1.0","schema_version":"afwp/1.0"}"#);
    assert_eq!(report.findings[0].code, "AF_SCHEMA_INVALID");
}

#[test]
fn integer_valued_float_aliases_outside_safe_range_are_rejected() {
    let input = std::str::from_utf8(EXAMPLE).unwrap();
    for unsafe_revision in ["9007199254740992.0", "9007199254740993.0"] {
        let mutated = input.replacen(
            "\"revision\": 3",
            &format!("\"revision\": {unsafe_revision}"),
            1,
        );
        let report = lint_publish(mutated.as_bytes());
        assert_eq!(report.findings[0].code, "AF_SCHEMA_INVALID");
        assert!(report.findings[0].message.contains("safe range"));
    }
}

#[test]
fn graph_accepts_both_fixed_encodings() {
    let mut package = example();
    package["parent_id"] = Value::Null;
    package["dependencies"] = json!([]);
    package["conflicts"]["integration_after"] = json!([]);
    let hash = compute_package_hash(&package).unwrap();
    package["package_hash"] = Value::String(hash);

    let array = serde_json::to_vec(&json!([package.clone()])).unwrap();
    assert!(lint_work_graph_json(&array).valid);
    let object = serde_json::to_vec(&json!({"packages": [package]})).unwrap();
    assert!(lint_work_graph_json(&object).valid);
}

#[test]
fn graph_detects_cycle_revision_and_delegation_violations() {
    let mut first = example();
    first["package_id"] = json!("wp-graph-first");
    first["parent_id"] = Value::Null;
    first["revision"] = json!(1);
    first["dependencies"] = json!([{
        "package_id": "wp-graph-second",
        "revision": 2,
        "edge": "blocks",
        "condition": "accepted"
    }]);
    first["conflicts"]["integration_after"] = json!([]);
    first["delegation"] = json!({
        "allowed": false,
        "max_depth": 0,
        "max_children": 0,
        "max_budget_units": 0,
        "allowed_kinds": []
    });
    let hash = compute_package_hash(&first).unwrap();
    first["package_hash"] = Value::String(hash);

    let mut second = first.clone();
    second["package_id"] = json!("wp-graph-second");
    second["parent_id"] = json!("wp-graph-first");
    second["dependencies"][0]["package_id"] = json!("wp-graph-first");
    second["dependencies"][0]["revision"] = json!(999);
    let hash = compute_package_hash(&second).unwrap();
    second["package_hash"] = Value::String(hash);

    let report =
        lint_work_graph_json(&serde_json::to_vec(&json!({"packages": [first, second]})).unwrap());
    let codes: Vec<_> = report.findings.iter().map(|item| item.code).collect();
    assert!(codes.contains(&"AF_LINT_DEPENDENCY_CYCLE"));
    assert!(codes.contains(&"AF_LINT_DEPENDENCY_REVISION_MISMATCH"));
    assert!(codes.contains(&"AF_LINT_DELEGATION_UNAUTHORIZED"));
}
