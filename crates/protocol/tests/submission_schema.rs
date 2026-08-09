use agentforge_protocol::schema::{self, SchemaKind};
use agentforge_protocol::{parse_submission, strict_json};
use serde_json::{Value, json};

const CANDIDATE: &[u8] = include_bytes!("../../../examples/submission-lease-fencing.json");
const SALVAGE: &[u8] = include_bytes!("../../../examples/submission-salvage-lease-fencing.json");

#[test]
fn candidate_and_salvage_examples_are_strictly_typed() {
    parse_submission(CANDIDATE).unwrap();
    parse_submission(SALVAGE).unwrap();
}

#[test]
fn submission_required_members_and_patterns_have_precise_pointers() {
    let schema_document = schema::document(SchemaKind::Submission).unwrap();
    for member in schema_document["required"].as_array().unwrap() {
        let member = member.as_str().unwrap();
        let mut value = strict_json::from_slice(CANDIDATE).unwrap();
        value.as_object_mut().unwrap().remove(member);
        assert_invalid_at(&value, &format!("/{member}"));
    }

    for (pointer, replacement) in [
        ("/submission_id", json!("bad")),
        ("/package_hash", json!("sha256:ABC")),
        ("/git/candidate_commit", json!("deadbeef")),
        ("/candidate_id", json!("not-a-uuid")),
    ] {
        let mut value = strict_json::from_slice(CANDIDATE).unwrap();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert_invalid_at(&value, pointer);
    }

    let mut missing_conditional = strict_json::from_slice(CANDIDATE).unwrap();
    missing_conditional
        .as_object_mut()
        .unwrap()
        .remove("candidate_id");
    assert_invalid_at(&missing_conditional, "/candidate_id");
}

#[test]
fn submission_unknown_fields_are_rejected_at_their_location() {
    let mut value = strict_json::from_slice(SALVAGE).unwrap();
    value["provenance"]["signature"]["unknown"] = Value::Bool(true);
    assert_invalid_at(&value, "/provenance/signature/unknown");
}

fn assert_invalid_at(value: &Value, expected_pointer: &str) {
    let findings = schema::validate(SchemaKind::Submission, value).unwrap_err();
    assert!(
        findings
            .iter()
            .any(|finding| finding.pointer == expected_pointer),
        "{expected_pointer}: {findings:?}"
    );
}
