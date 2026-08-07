use agentforge_protocol::canonical::{
    canonical_bytes_without_package_hash, compute_package_hash, package_hash, verify_package_hash,
};
use agentforge_protocol::strict_json;
use proptest::prelude::*;
use serde_json::Value;
use serde_json::json;
use std::io::Write;
use std::process::{Command, Stdio};

const EXAMPLE: &[u8] = include_bytes!("../../../examples/afwp-lease-fencing.json");
const EMPTY_HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn reverse_object_key_render(value: &Value, output: &mut String) {
    match value {
        Value::Object(object) => {
            output.push('{');
            for (index, (key, value)) in object.iter().rev().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).unwrap());
                output.push(':');
                reverse_object_key_render(value, output);
            }
            output.push('}');
        }
        Value::Array(array) => {
            output.push('[');
            for (index, value) in array.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                reverse_object_key_render(value, output);
            }
            output.push(']');
        }
        _ => output.push_str(&serde_json::to_string(value).unwrap()),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_000))]

    #[test]
    fn whitespace_and_object_key_order_do_not_change_hash(label in "[A-Za-z0-9._-]{0,64}") {
        let mut value = strict_json::from_slice(EXAMPLE).unwrap();
        value["metadata"]["labels"]["property"] = Value::String(label);
        value["package_hash"] = Value::String(EMPTY_HASH.to_owned());

        let pretty = serde_json::to_vec_pretty(&value).unwrap();
        let mut reverse = String::new();
        reverse_object_key_render(&value, &mut reverse);
        prop_assert_eq!(package_hash(&pretty).unwrap(), package_hash(reverse.as_bytes()).unwrap());
    }
}

#[test]
fn array_string_and_number_changes_are_content_changes() {
    let original = strict_json::from_slice(EXAMPLE).unwrap();
    let original_hash = compute_package_hash(&original).unwrap();

    let mut reordered = original.clone();
    reordered["requirements"].as_array_mut().unwrap().swap(0, 1);
    assert_ne!(compute_package_hash(&reordered).unwrap(), original_hash);

    let mut changed_string = original.clone();
    changed_string["title"] = Value::String("Implement transactional lease fencing v2".to_owned());
    assert_ne!(
        compute_package_hash(&changed_string).unwrap(),
        original_hash
    );

    let mut changed_number = original;
    changed_number["scheduling"]["priority"] = Value::from(89);
    assert_ne!(
        compute_package_hash(&changed_number).unwrap(),
        original_hash
    );
}

#[test]
fn verify_rejects_mismatch_without_rewriting_the_document() {
    let mut value = strict_json::from_slice(EXAMPLE).unwrap();
    value["title"] = Value::String("Changed but still schema valid".to_owned());
    let bytes = serde_json::to_vec(&value).unwrap();
    let before = bytes.clone();
    let error = verify_package_hash(&bytes).unwrap_err();
    assert!(error.to_string().contains("AF_PACKAGE_HASH_MISMATCH"));
    assert_eq!(bytes, before);
}

#[test]
fn explicit_afwp_c14n_boundaries_match_jcs_semantics() {
    let base = strict_json::from_slice(EXAMPLE).unwrap();
    let base_hash = compute_package_hash(&base).unwrap();

    // `package_hash` is the sole server-derived top-level member excluded by
    // AFWP-C14N-1; schema-sanctioned extension labels remain signed content.
    let mut different_declared_hash = base.clone();
    different_declared_hash["package_hash"] = Value::String(EMPTY_HASH.to_owned());
    assert_eq!(
        compute_package_hash(&different_declared_hash).unwrap(),
        base_hash
    );
    let mut extension = base.clone();
    extension["metadata"]["labels"]["x-protocol-extension"] = Value::String("retained".to_owned());
    assert_ne!(compute_package_hash(&extension).unwrap(), base_hash);

    // JCS does not normalize Unicode, and an optional empty array is distinct
    // from an absent member.
    let mut nfc = base.clone();
    nfc["title"] = Value::String("Caf\u{e9} protocol".to_owned());
    let mut nfd = base.clone();
    nfd["title"] = Value::String("Cafe\u{301} protocol".to_owned());
    assert_ne!(
        compute_package_hash(&nfc).unwrap(),
        compute_package_hash(&nfd).unwrap()
    );
    let mut empty_optional_array = base.clone();
    empty_optional_array["interfaces"] = json!([]);
    let mut missing_optional_array = base.clone();
    missing_optional_array
        .as_object_mut()
        .unwrap()
        .remove("interfaces");
    assert_ne!(
        compute_package_hash(&empty_optional_array).unwrap(),
        compute_package_hash(&missing_optional_array).unwrap()
    );

    // Insignificant CRLF/LF whitespace and equivalent ECMAScript number
    // spellings converge on the same canonical bytes, including negative zero.
    let lf = String::from_utf8(EXAMPLE.to_vec()).unwrap();
    let crlf = lf.replace('\n', "\r\n");
    assert_eq!(
        package_hash(lf.as_bytes()).unwrap(),
        package_hash(crlf.as_bytes()).unwrap()
    );
    let decimal = lf.replacen("\"complexity\": 0.72", "\"complexity\": 7.2e-1", 1);
    assert_eq!(
        package_hash(lf.as_bytes()).unwrap(),
        package_hash(decimal.as_bytes()).unwrap()
    );
    let negative_zero = lf.replacen("\"complexity\": 0.72", "\"complexity\": -0", 1);
    let positive_zero = lf.replacen("\"complexity\": 0.72", "\"complexity\": 0.0", 1);
    assert_eq!(
        package_hash(negative_zero.as_bytes()).unwrap(),
        package_hash(positive_zero.as_bytes()).unwrap()
    );

    // RFC 8785 sorts object names by UTF-16 code units, which differs from
    // Unicode scalar ordering for this non-BMP/BMP pair.
    let utf16_order = json!({
        "package_hash": EMPTY_HASH,
        "\u{e000}": 1,
        "\u{10000}": 2
    });
    let canonical =
        String::from_utf8(canonical_bytes_without_package_hash(&utf16_order).unwrap()).unwrap();
    assert_eq!(
        canonical,
        format!("{{\"{}\":2,\"{}\":1}}", '\u{10000}', '\u{e000}')
    );
}

#[test]
fn one_thousand_plus_edge_vectors_match_independent_javascript_jcs() {
    let base = strict_json::from_slice(EXAMPLE).unwrap();
    let mut vectors = Vec::with_capacity(1_008);

    for title in [
        "Caf\u{e9} protocol",
        "Cafe\u{301} protocol",
        "line one\nline two",
        "non-BMP \u{10000} protocol",
    ] {
        let mut value = base.clone();
        value["title"] = Value::String(title.to_owned());
        vectors.push(value);
    }
    for complexity in [-0.0, 0.0, 1.0e-7] {
        let mut value = base.clone();
        value["routing"]["complexity"] =
            Value::Number(serde_json::Number::from_f64(complexity).unwrap());
        vectors.push(value);
    }
    let mut empty_optional_array = base.clone();
    empty_optional_array["interfaces"] = json!([]);
    vectors.push(empty_optional_array);
    let mut missing_optional_array = base.clone();
    missing_optional_array
        .as_object_mut()
        .unwrap()
        .remove("interfaces");
    vectors.push(missing_optional_array);
    let mut extension = base.clone();
    extension["metadata"]["labels"]["x-protocol-extension"] =
        Value::String("retained \u{10000}".to_owned());
    vectors.push(extension);

    for case in 0..1_000 {
        let mut value = base.clone();
        value["metadata"]["labels"]["cross-language-vector"] =
            Value::String(format!("case-{case:04}"));
        value["routing"]["complexity"] =
            Value::Number(serde_json::Number::from_f64(f64::from(case) / 999.0).unwrap());
        value["scheduling"]["priority"] = Value::from(case % 101);
        if case % 2 == 1 {
            value["requirements"].as_array_mut().unwrap().swap(0, 1);
        }
        vectors.push(value);
    }
    let rust_hashes: Vec<_> = vectors
        .iter()
        .map(|value| compute_package_hash(value).unwrap())
        .collect();

    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("jcs_reference.mjs");
    let mut child = Command::new("node")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Node.js is required by the cross-language JCS conformance gate");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&vectors).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Node reference implementation failed"
    );
    let javascript_hashes: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rust_hashes, javascript_hashes);
}
