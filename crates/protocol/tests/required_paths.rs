use agentforge_protocol::schema::{self, SchemaKind};
use agentforge_protocol::strict_json;
use serde_json::Value;
use serde_json::json;

const AFWP: &[u8] = include_bytes!("../../../examples/afwp-lease-fencing.json");
const CANDIDATE: &[u8] = include_bytes!("../../../examples/submission-lease-fencing.json");
const SALVAGE: &[u8] = include_bytes!("../../../examples/submission-salvage-lease-fencing.json");

#[derive(Debug)]
struct MemberPath {
    pointer: String,
    parent: String,
    key: String,
}

#[test]
fn every_actually_required_path_in_all_repository_fixtures_is_precise() {
    let afwp_count = assert_all_required_deletions(AFWP, SchemaKind::Afwp);
    let candidate_count = assert_all_required_deletions(CANDIDATE, SchemaKind::Submission);
    let salvage_count = assert_all_required_deletions(SALVAGE, SchemaKind::Submission);
    let early_failure_count =
        assert_all_required_deletions_value(early_provenance_failure(), SchemaKind::Submission);

    // Guard against accidentally reducing this recursive matrix to a shallow
    // top-level-only check.
    assert!(afwp_count > 100, "only {afwp_count} AFWP paths exercised");
    assert!(
        candidate_count > 80,
        "only {candidate_count} candidate paths exercised"
    );
    assert!(
        salvage_count > 30,
        "only {salvage_count} salvage paths exercised"
    );
    assert!(
        early_failure_count > 35,
        "only {early_failure_count} early-failure paths exercised"
    );
}

fn assert_all_required_deletions(input: &[u8], kind: SchemaKind) -> usize {
    let value = strict_json::from_slice(input).unwrap();
    assert_all_required_deletions_value(value, kind)
}

fn assert_all_required_deletions_value(value: Value, kind: SchemaKind) -> usize {
    schema::validate(kind, &value).unwrap();
    let mut members = Vec::new();
    collect_members(&value, "", &mut members);

    let mut required_count = 0;
    for member in members {
        let mut mutated = value.clone();
        mutated
            .pointer_mut(&member.parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(&member.key);
        let Err(findings) = schema::validate(kind, &mutated) else {
            continue;
        };
        if !findings.iter().any(|finding| finding.keyword == "required") {
            continue;
        }
        required_count += 1;
        assert!(
            findings.iter().any(|finding| {
                finding.keyword == "required" && finding.pointer == member.pointer
            }),
            "deleting {:?} produced imprecise findings: {:?}",
            member,
            findings
        );
    }
    required_count
}

#[test]
fn provenance_failure_rejects_every_future_stage_placeholder() {
    let candidate = strict_json::from_slice(CANDIDATE).unwrap();

    for (property, mut future_value) in [
        ("criteria", candidate["criteria"].clone()),
        ("review", candidate["review"].clone()),
        (
            "clean_reproduction",
            candidate["clean_reproduction"].clone(),
        ),
        ("evidence_bundle", candidate["evidence_bundle"].clone()),
    ] {
        if property == "criteria" {
            future_value[0]["status"] = json!("SKIPPED");
        }
        let mut mutated = early_provenance_failure();
        mutated[property] = future_value;
        let findings = schema::validate(SchemaKind::Submission, &mutated)
            .expect_err("a provenance failure must reject future-stage placeholders");
        let pointer = format!("/{property}");
        assert!(
            findings.iter().any(|finding| {
                finding.pointer == pointer || finding.pointer.starts_with(&format!("{pointer}/"))
            }),
            "forged {property} produced imprecise findings: {findings:?}"
        );
    }
}

fn early_provenance_failure() -> Value {
    let mut value = strict_json::from_slice(CANDIDATE).unwrap();
    value["terminal_outcome"] = json!("FAIL");
    value["completed_stage"] = json!("provenance_check");
    let object = value.as_object_mut().unwrap();
    for property in [
        "deliverables",
        "criteria",
        "review",
        "clean_reproduction",
        "evidence_bundle",
        "decisions",
        "residual_risks",
    ] {
        object.remove(property);
    }
    object.insert(
        "failure_dossier".to_owned(),
        json!({
            "code": "AF_PROVENANCE_FAILURE",
            "summary": "trusted provenance could not be established",
            "retryable": false,
            "evidence_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "evidence_refs": ["artifact://failure/provenance.json"]
        }),
    );
    value
}

fn collect_members(value: &Value, parent: &str, output: &mut Vec<MemberPath>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let token = escape_pointer_token(key);
                let pointer = format!("{parent}/{token}");
                output.push(MemberPath {
                    pointer: pointer.clone(),
                    parent: parent.to_owned(),
                    key: key.clone(),
                });
                collect_members(child, &pointer, output);
            }
        }
        Value::Array(array) => {
            for (index, child) in array.iter().enumerate() {
                collect_members(child, &format!("{parent}/{index}"), output);
            }
        }
        _ => {}
    }
}

fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}
