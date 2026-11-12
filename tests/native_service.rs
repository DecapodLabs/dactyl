//! Opt-in native service proof. The external fixture owns policy, observation
//! and cleanup. This does not provision a catalog or claim hosted acceptance.
#![cfg(feature = "supabase")]
use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, DatastoreRoute, OpenOptions, Operation,
    OperationResult, StorageContext, SupabaseRpcConfig,
};
use serde::Deserialize;
use std::io::Read;

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
    let metadata = std::fs::symlink_metadata(&path).expect("native fixture unavailable");
    assert!(
        metadata.is_file() && metadata.len() <= 65536,
        "unsafe native fixture"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            metadata.permissions().mode() & 0o077,
            0,
            "native fixture must be private"
        );
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .unwrap()
        .take(65537)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 65536);
    let f: Fixture = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("invalid protected native fixture"));
    assert!(f.version == 1 && f.disposable && !f.read_sql.is_empty() && f.read_sql.len() <= 32768);
    let origin = reqwest::Url::parse(&f.endpoint).expect("invalid native fixture origin");
    assert!(
        origin.scheme() == "http"
            && matches!(origin.host_str(), Some("127.0.0.1" | "[::1]" | "::1")),
        "fixture must be disposable loopback"
    );
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
    .expect("native connection failed");
    let result = connection
        .atomic(&[
            Operation::read(&f.read_sql, vec![]),
            Operation::read(&f.read_sql, vec![]),
        ])
        .expect("native read-only batch failed");
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
        .expect_err("read-only mutation accepted");
    assert_eq!(denied.adapter_kind(), Some(AdapterErrorKind::ReadOnly));
    println!("native read-only atomic service contract passed; independent server observation and cleanup remain external");
}
