use std::{
    fs, io,
    net::{TcpListener, TcpStream, UdpSocket},
    path::Path,
};

use agentforge_test_support::{
    CommandEvidence, DeterministicRng, DeterministicUuidV7, EventRecorder, EvidenceStatus,
    FixedClock, HermeticTestCommand, RunnerFingerprint, TestExecutionConstraints,
};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;
use uuid::{Variant, Version};

const CHILD_MARKER: &str = "AGENTFORGE_TEST_INTENTIONAL_FAILURE";
const NETWORK_CHILD_MARKER: &str = "AGENTFORGE_TEST_NETWORK_PROBE";

// Referencing this Cargo-provided path makes the trusted sibling runner an
// explicit integration-test build target. HermeticTestCommand independently
// resolves and validates the sibling path before executing it.
#[cfg(target_os = "linux")]
const HERMETIC_RUNNER: &str = env!("CARGO_BIN_EXE_af-hermetic-runner");

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FixtureEvent {
    value: u64,
}

fn deterministic_transcript(seed: u64) -> Vec<u8> {
    let clock = FixedClock::from_unix_timestamp_millis(1_754_611_200_123);
    let mut rng = DeterministicRng::new(seed);
    let mut ids = DeterministicUuidV7::new(clock.clone(), seed);
    let mut recorder = EventRecorder::new(clock.clone(), seed);

    let first_id = ids.next_uuid();
    recorder.record(FixtureEvent {
        value: rng.next_u64(),
    });
    clock.advance_millis(25);
    let second_id = ids.next_uuid();
    recorder.record(FixtureEvent {
        value: rng.next_u64(),
    });

    serde_json::to_vec(&serde_json::json!({
        "first_id": first_id,
        "second_id": second_id,
        "events": recorder.events(),
    }))
    .unwrap()
}

#[test]
fn same_seed_and_clock_are_byte_identical() {
    let first = deterministic_transcript(0xA63E_7F01);
    let second = deterministic_transcript(0xA63E_7F01);
    assert_eq!(first, second);

    let value: serde_json::Value = serde_json::from_slice(&first).unwrap();
    for field in ["first_id", "second_id"] {
        let id = uuid::Uuid::parse_str(value[field].as_str().unwrap()).unwrap();
        assert_eq!(id.get_version(), Some(Version::SortRand));
        assert_eq!(id.get_variant(), Variant::RFC4122);
    }
    assert!(
        uuid::Uuid::parse_str(value["first_id"].as_str().unwrap()).unwrap()
            < uuid::Uuid::parse_str(value["second_id"].as_str().unwrap()).unwrap()
    );
}

#[test]
fn deterministic_rng_can_be_forked_without_consuming_parent() {
    let parent = DeterministicRng::new(41);
    let mut first = parent.fork("schedule");
    let mut second = parent.fork("schedule");
    let mut different = parent.fork("evidence");

    assert_eq!(first.next_u64(), second.next_u64());
    assert_ne!(first.next_u64(), different.next_u64());
}

#[test]
fn json_and_junit_evidence_are_deterministic_and_complete() {
    let runner = RunnerFingerprint::new(
        "runner-fixture",
        Some("sha256:1111111111111111111111111111111111111111111111111111111111111111".to_owned()),
        Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
    );
    let build = || {
        CommandEvidence::from_capture(
            "intentional-failure",
            vec!["fixture-fail".to_owned(), "--case".to_owned()],
            Some(17),
            424_242,
            runner.clone(),
            b"fixture stdout\n",
            b"intentional failure\n",
        )
    };
    let first = build();
    let second = build();

    assert_eq!(first.status, EvidenceStatus::Fail);
    assert_eq!(first.json_bytes().unwrap(), second.json_bytes().unwrap());
    assert_eq!(first.junit_xml(), second.junit_xml());
    assert_eq!(
        first.json_bytes().unwrap(),
        include_bytes!("../../../tests/fixtures/evidence/intentional-failure.json")
    );
    assert_eq!(
        first.junit_xml().as_bytes(),
        include_bytes!("../../../tests/fixtures/evidence/intentional-failure.junit.xml")
    );

    let parsed: serde_json::Value = serde_json::from_slice(&first.json_bytes().unwrap()).unwrap();
    assert_eq!(parsed["argv"][0], "fixture-fail");
    assert_eq!(parsed["exit_code"], 17);
    assert_eq!(parsed["seed"], 424_242);
    assert!(
        parsed["runner_fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );

    let directory = tempdir().unwrap();
    let paths = first
        .write_files(directory.path(), "intentional-failure")
        .unwrap();
    assert_eq!(fs::read(paths.json).unwrap(), first.json_bytes().unwrap());
    assert_eq!(fs::read_to_string(paths.junit).unwrap(), first.junit_xml());
}

#[test]
fn hermetic_runner_captures_a_real_intentional_failure() {
    #[cfg(target_os = "linux")]
    assert!(Path::new(HERMETIC_RUNNER).is_file());

    let runner = RunnerFingerprint::new("hermetic-fixture", None, None);
    let command = HermeticTestCommand::current_test_binary(9_001, runner)
        .unwrap()
        .arg("--exact")
        .arg("intentional_failure_fixture_child")
        .arg("--nocapture")
        .fixture_env(CHILD_MARKER, "1")
        .unwrap();

    assert_eq!(command.constraints(), TestExecutionConstraints::HERMETIC);
    let evidence = command.run("intentional-failure-child").unwrap();
    assert_eq!(evidence.exit_code, Some(17));
    assert_eq!(evidence.seed, 9_001);
    assert_eq!(evidence.status, EvidenceStatus::Fail);
    assert_eq!(evidence.argv[1], "--exact");
}

#[cfg(target_os = "linux")]
#[test]
fn hermetic_runner_denies_tcp_and_udp_in_the_kernel() {
    assert!(Path::new(HERMETIC_RUNNER).is_file());
    let command = HermeticTestCommand::current_test_binary(
        9_002,
        RunnerFingerprint::new("network-denial-fixture", None, None),
    )
    .unwrap()
    .arg("--exact")
    .arg("network_denial_fixture_child")
    .arg("--nocapture")
    .fixture_env(NETWORK_CHILD_MARKER, "1")
    .unwrap();

    let evidence = command.run("network-denial-child").unwrap();
    assert_eq!(evidence.exit_code, Some(0));
    assert_eq!(evidence.status, EvidenceStatus::Pass);
}

#[cfg(not(target_os = "linux"))]
#[test]
fn hermetic_runner_fails_closed_without_seccomp() {
    let error = HermeticTestCommand::current_test_binary(
        9_002,
        RunnerFingerprint::new("network-denial-fixture", None, None),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

#[test]
fn hermetic_runner_rejects_host_configuration_environment() {
    let runner = RunnerFingerprint::new("hermetic-fixture", None, None);
    let error = HermeticTestCommand::current_test_binary(1, runner)
        .unwrap()
        .fixture_env("GIT_ASKPASS", "/host/credential-helper")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);

    let constraints = HermeticTestCommand::current_test_binary(
        2,
        RunnerFingerprint::new("policy-fixture", None, None),
    )
    .unwrap()
    .constraints();
    assert_eq!(constraints, TestExecutionConstraints::HERMETIC);
}

#[test]
fn intentional_failure_fixture_child() {
    if std::env::var_os(CHILD_MARKER).is_none() {
        return;
    }

    assert_eq!(
        std::env::var("AGENTFORGE_TEST_NETWORK_POLICY").as_deref(),
        Ok("deny")
    );
    assert_eq!(
        std::env::var("AGENTFORGE_TEST_GIT_CREDENTIAL_POLICY").as_deref(),
        Ok("isolated")
    );
    for forbidden in [
        "GITHUB_TOKEN",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ] {
        assert!(
            std::env::var_os(forbidden).is_none(),
            "inherited {forbidden}"
        );
    }
    std::process::exit(17);
}

#[test]
fn network_denial_fixture_child() {
    if std::env::var_os(NETWORK_CHILD_MARKER).is_none() {
        return;
    }

    assert_isolated_fixture_environment();
    assert_permission_denied(TcpListener::bind("127.0.0.1:0").unwrap_err());
    assert_permission_denied(TcpStream::connect("127.0.0.1:9").unwrap_err());
    assert_permission_denied(UdpSocket::bind("127.0.0.1:0").unwrap_err());
}

fn assert_isolated_fixture_environment() {
    assert_eq!(
        std::env::var("AGENTFORGE_TEST_NETWORK_POLICY").as_deref(),
        Ok("deny")
    );
    assert_eq!(
        std::env::var("AGENTFORGE_TEST_GIT_CREDENTIAL_POLICY").as_deref(),
        Ok("isolated")
    );
    let home = std::env::var("HOME").expect("fixture HOME must be set");
    let xdg = std::env::var("XDG_CONFIG_HOME").expect("fixture XDG_CONFIG_HOME must be set");
    assert!(Path::new(&home).is_dir());
    assert!(Path::new(&xdg).is_dir());
    assert_eq!(std::env::var("GIT_CONFIG_NOSYSTEM").as_deref(), Ok("1"));
    assert_eq!(std::env::var("GIT_TERMINAL_PROMPT").as_deref(), Ok("0"));
    for forbidden in [
        "GITHUB_TOKEN",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ] {
        assert!(
            std::env::var_os(forbidden).is_none(),
            "inherited {forbidden}"
        );
    }
}

fn assert_permission_denied(error: io::Error) {
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.raw_os_error(), Some(1));
}
