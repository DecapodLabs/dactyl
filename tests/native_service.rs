//! Opt-in native service proof. The external fixture owns policy, observation
//! and cleanup. This does not provision a catalog or claim hosted acceptance.
#![cfg(feature = "supabase")]
#[path = "support/native_scenarios.rs"]
mod native_scenarios;
#[path = "support/protected_fixture.rs"]
mod protected;
use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, DatastoreRoute, OpenOptions, Operation,
    OperationResult, StorageContext, SupabaseRpcConfig,
};
use serde::Deserialize;
use std::path::Path;

#[test]
fn scenario_profiles_refuse_missing_fixture() {
    for name in [
        "local_native_scenarios",
        "protected_hosted_native_scenarios",
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([name, "--ignored", "--exact"])
            .env_remove("DACTYL_NATIVE_FIXTURE")
            .env_remove("DACTYL_NATIVE_HOSTED_PROOF")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(text.contains("unavailable is not a pass"));
        assert!(text.contains("1 failed"));
    }
}

#[test]
#[ignore = "requires protected disposable loopback native fixture v2"]
fn local_native_scenarios() {
    native_scenarios::run(native_scenarios::Profile::LocalNative)
        .unwrap_or_else(|e| panic!("unavailable is not a pass: {e}"));
}

#[test]
#[ignore = "requires protected disposable HTTPS native fixture v2 and explicit hosted opt-in"]
fn protected_hosted_native_scenarios() {
    native_scenarios::run(native_scenarios::Profile::HostedNative)
        .unwrap_or_else(|e| panic!("unavailable is not a pass: {e}"));
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    version: u16,
    disposable: bool,
    endpoint: String,
    publishable_key: String,
    schema: String,
    token: String,
    context: serde_json::Value,
    read_sql: String,
}

#[test]
#[ignore = "requires an independently provisioned protected native service fixture"]
fn local_native_read_only_contract() {
    let path = std::env::var("DACTYL_NATIVE_FIXTURE")
        .expect("unavailable is not a pass: protected native fixture required");
    let bytes = protected::read(Path::new(&path))
        .unwrap_or_else(|e| panic!("unavailable is not a pass: {e}"));
    let f: Fixture = native_scenarios::parse(&bytes)
        .unwrap_or_else(|e| panic!("unavailable is not a pass: {e}"));
    assert!(f.version == 1 && f.disposable && !f.read_sql.is_empty() && f.read_sql.len() <= 32768);
    native_scenarios::endpoint(&f.endpoint, native_scenarios::Profile::LocalNative, false)
        .unwrap_or_else(|e| panic!("unavailable is not a pass: {e}"));
    let route = DatastoreRoute::supabase_rpc(
        SupabaseRpcConfig::v1(f.endpoint, f.publishable_key).with_schema(f.schema),
        Some(f.token),
    );
    let connection = Connection::open_with_options_and_context(
        route,
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        },
        Some(StorageContext::new(1, f.context).unwrap()),
    )
    .unwrap_or_else(|_| panic!("native connection configuration rejected"));
    let result = connection
        .atomic(&[
            Operation::read(&f.read_sql, vec![]),
            Operation::read(&f.read_sql, vec![]),
        ])
        .unwrap_or_else(|_| panic!("native read-only batch failed"));
    assert_eq!(result.results.len(), 2);
    assert!(result
        .results
        .iter()
        .all(|r| matches!(r, OperationResult::Rows(_))));
    let denied = connection
        .atomic(&[Operation::write(
            "INSERT INTO forbidden_fixture VALUES (1)",
            vec![],
        )])
        .err()
        .unwrap_or_else(|| panic!("client read-only mutation accepted"));
    assert_eq!(denied.adapter_kind(), Some(AdapterErrorKind::ReadOnly));
    println!("native read-only atomic service contract passed; independent server observation and cleanup remain external");
}
