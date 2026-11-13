//! Service-owned native scenarios: client evidence only, never DB/cleanup proof.
use super::protected;
use dactyl_db::{
    AccessMode, AtomicResult, Connection, DactylError, DatastoreRoute, OpenOptions, Operation,
    OperationKind, OperationResult, StorageContext, SupabaseRpcConfig,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

// No Debug/Serialize on credential-bearing input.
#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    LocalNative,
    HostedNative,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Principal {
    token: String,
    context: StorageContext,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    version: u16,
    profile: Profile,
    disposable: bool,
    independent_principals: bool,
    endpoint: String,
    publishable_key: String,
    schema: String,
    client_revision: String,
    service_revision: String,
    principals: Vec<Principal>,
    scenarios: Vec<Scenario>,
    receipt_path: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    principal: usize,
    access_mode: AccessMode,
    operations: Vec<Operation>,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum Expected {
    Success {
        results: Vec<OperationResult>,
    },
    Error {
        kind: String,
        origin: String,
        mutation: String,
    },
}

fn require(condition: bool, reason: &'static str) -> protected::Check<()> {
    if condition {
        Ok(())
    } else {
        Err(reason)
    }
}

pub fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> protected::Check<T> {
    // JSON diagnostics may quote credential-bearing fields and values.
    serde_json::from_slice(bytes).map_err(|_| "invalid protected native fixture")
}

pub fn endpoint(endpoint: &str, profile: Profile, hosted: bool) -> protected::Check<()> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| "invalid native fixture origin")?;
    require(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "native fixture requires a credential-free origin",
    )?;
    match profile {
        Profile::LocalNative => require(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some_and(|host| {
                    host.trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
                }),
            "local fixture requires a literal loopback origin",
        ),
        Profile::HostedNative => require(
            hosted && url.scheme() == "https",
            "hosted fixture requires explicit opt-in and HTTPS",
        ),
    }
}

fn connection(
    f: &Fixture,
    principal: usize,
    access_mode: AccessMode,
) -> protected::Check<Connection> {
    let p = &f.principals[principal];
    Connection::open_with_options_and_context(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&f.endpoint, &f.publishable_key).with_schema(&f.schema),
            Some(p.token.clone()),
        ),
        OpenOptions {
            access_mode,
            ..OpenOptions::default()
        },
        Some(p.context.clone()),
    )
    .map_err(|_| "native connection configuration rejected")
}

fn validate(f: &Fixture, profile: Profile, hosted: bool) -> protected::Check<()> {
    require(
        f.version == 2 && f.profile == profile,
        "native fixture version/profile mismatch",
    )?;
    require(
        f.disposable && f.independent_principals,
        "disposable independent fixture required",
    )?;
    endpoint(&f.endpoint, profile, hosted)?;
    for revision in [&f.client_revision, &f.service_revision] {
        require(
            revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()),
            "exact client and service source revisions required",
        )?;
    }
    require(
        (2..=8).contains(&f.principals.len()),
        "two to eight ordinary principals required",
    )?;
    for (index, p) in f.principals.iter().enumerate() {
        require(
            !p.token.is_empty()
                && p.token.bytes().all(|b| (0x21..=0x7e).contains(&b))
                && !f.principals[..index]
                    .iter()
                    .any(|other| other.token == p.token),
            "distinct ordinary bearer tokens required",
        )?;
        StorageContext::new(p.context.version(), p.context.payload().clone())
            .map_err(|_| "invalid opaque context envelope")?;
        connection(f, index, AccessMode::ReadOnly)?; // Config validation, no request.
    }
    require(
        (1..=32).contains(&f.scenarios.len()),
        "one to thirty-two scenarios required",
    )?;
    for scenario in &f.scenarios {
        require(
            scenario.principal < f.principals.len(),
            "invalid scenario principal index",
        )?;
        require(
            (1..=16).contains(&scenario.operations.len()),
            "one to sixteen operations required",
        )?;
        for op in &scenario.operations {
            require(
                !op.sql().is_empty()
                    && op.sql().len() <= 32768
                    && op.params().len() <= 64
                    && op.kind() != OperationKind::Schema,
                "invalid native scenario operation",
            )?;
        }
        match &scenario.expected {
            Expected::Success { results } => require(
                results.len() == scenario.operations.len(),
                "expected results must match operation count",
            )?,
            Expected::Error {
                kind,
                origin,
                mutation,
            } => {
                require(
                    [
                        "Authentication",
                        "Authorization",
                        "ReadOnly",
                        "Constraint",
                        "Conflict",
                        "VersionConflict",
                        "TransactionAborted",
                        "Capability",
                        "Query",
                        "Protocol",
                        "Transport",
                        "Timeout",
                        "Unavailable",
                        "Quota",
                        "RateLimited",
                        "Value",
                        "InvalidOperation",
                        "IdempotencyConflict",
                        "IdempotencyInProgress",
                    ]
                    .contains(&kind.as_str()),
                    "unsupported expected adapter category",
                )?;
                require(
                    ["LocalBudget", "RemotePolicy", "MalformedResponse", "Other"]
                        .contains(&origin.as_str())
                        && ["NotAttempted", "RolledBack", "Uncertain"].contains(&mutation.as_str()),
                    "invalid expected failure boundary",
                )?;
            }
        }
    }
    require(
        f.receipt_path.is_absolute() && f.receipt_path.file_name().is_some(),
        "receipt must name a new absolute file",
    )
}

fn matches(expected: &Expected, actual: Result<AtomicResult, DactylError>) -> bool {
    match (expected, actual) {
        (Expected::Success { results }, Ok(actual)) => actual.results == *results,
        (
            Expected::Error {
                kind,
                origin,
                mutation,
            },
            Err(error),
        ) => {
            error
                .adapter_kind()
                .is_some_and(|actual| format!("{actual:?}") == *kind)
                && format!("{:?}", error.failure_origin()) == *origin
                && format!("{:?}", error.mutation_outcome()) == *mutation
        }
        _ => false,
    }
}

fn receipt(f: &Fixture, completed: usize) -> Value {
    json!({
        "receipt_version": 1,
        "profile": if f.profile == Profile::HostedNative { "hosted_native" } else { "local_native" },
        "sdk_version": env!("CARGO_PKG_VERSION"), "wire_version": 1,
        "operator_declared_client_revision": f.client_revision,
        "operator_declared_service_revision": f.service_revision,
        "evidence_class": "client_scenarios_only",
        "status": if completed == f.scenarios.len() { "client_scenarios_passed" } else { "incomplete" },
        "completed_scenarios": completed, "total_scenarios": f.scenarios.len(),
        "independent_database_observation": "pending",
        "physical_read_only_and_transactions": "pending",
        "principal_provisioning_and_authorization": "pending",
        "connection_mode": "unverified", "cleanup": "pending"
    })
}

pub fn run(profile: Profile) -> protected::Check<()> {
    let path = std::env::var_os("DACTYL_NATIVE_FIXTURE")
        .filter(|p| !p.is_empty())
        .ok_or("protected native fixture required")?;
    let f: Fixture = parse(&protected::read(Path::new(&path))?)?;
    validate(
        &f,
        profile,
        std::env::var("DACTYL_NATIVE_HOSTED_PROOF").is_ok_and(|v| v == "1"),
    )?;
    exercise(&f)
}

fn exercise(f: &Fixture) -> protected::Check<()> {
    // Persist incomplete evidence before any request; never overwrite a prior run.
    let mut output = protected::reserve(&f.receipt_path)?;
    protected::write(&mut output, &receipt(f, 0))?;
    for (index, scenario) in f.scenarios.iter().enumerate() {
        let db = connection(f, scenario.principal, scenario.access_mode)?;
        require(
            matches(&scenario.expected, db.atomic(&scenario.operations)),
            "native scenario mismatch; inspect private fixture index and independent observer",
        )?;
        protected::write(&mut output, &receipt(f, index + 1))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    #[test]
    fn documented_fixture_matches_the_public_result_serialization() {
        let text = include_str!("../../docs/supabase-conformance.md");
        let example = text
            .split_once("```json\n")
            .unwrap()
            .1
            .split_once("\n```")
            .unwrap()
            .0;
        let f: Fixture = parse(example.as_bytes()).unwrap();
        validate(&f, Profile::LocalNative, false).unwrap();
    }

    // Synthetic only. A real service and independent observer remain external.
    fn serve(listener: TcpListener, lose_second_response: bool) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            for index in 0..2 {
                let deadline = Instant::now() + Duration::from_secs(10);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(5))
                        }
                        Err(_) => panic!("synthetic native request unavailable"),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let (headers, offset, length) = loop {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).unwrap();
                    assert!(n > 0 && bytes.len() + n < 65536);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (headers, end + 4, length);
                    }
                };
                while bytes.len() < offset + length {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).unwrap();
                    assert!(n > 0 && bytes.len() + n < 65536);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let body: Value = serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
                assert!(headers.starts_with(if index == 0 {
                    "POST /rest/v1/rpc/dactyl_read_batch_v1 "
                } else {
                    "POST /rest/v1/rpc/dactyl_batch_v1 "
                }));
                assert_eq!(body["request"]["context"]["payload"]["opaque"], index + 1);
                assert!(headers.contains(if index == 0 {
                    "synthetic-one"
                } else {
                    "synthetic-two"
                }));
                if index == 1 && lose_second_response {
                    continue;
                }
                let response = r#"{"results":[{"columns":[],"rows":[],"affected_rows":0,"generated_keys":[]}]}"#;
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
            }
            std::thread::sleep(Duration::from_millis(150));
            assert!(
                listener.accept().is_err(),
                "scenario runner replayed a request"
            );
        })
    }

    #[test]
    fn native_scenarios_route_principals_record_progress_and_never_replay() {
        for lost in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let directory = tempfile::tempdir().unwrap();
            let mut value = fixture();
            value["endpoint"] = json!(format!("http://{}", listener.local_addr().unwrap()));
            value["receipt_path"] = json!(directory.path().join("receipt"));
            value["scenarios"].as_array_mut().unwrap().push(json!({"principal":1,"access_mode":"read_write",
                "operations":[{"kind":"write","sql":"service-owned synthetic mutation","params":[]}],
                "expected":if lost { json!({"outcome":"error","kind":"Transport","origin":"Other","mutation":"Uncertain"}) }
                    else { json!({"outcome":"success","results":[{"Write":{"affected_rows":1,"generated_keys":[]}}]}) }}));
            let f: Fixture = parse(&serde_json::to_vec(&value).unwrap()).unwrap();
            validate(&f, Profile::LocalNative, false).unwrap();
            let server = serve(listener, lost);
            let result = exercise(&f);
            server.join().unwrap();
            // The synthetic success response has zero affected rows, so the
            // one-row expectation fails after the first completed scenario.
            assert_eq!(result.is_ok(), lost);
            let receipt: Value =
                serde_json::from_slice(&std::fs::read(&f.receipt_path).unwrap()).unwrap();
            assert_eq!(receipt["completed_scenarios"], if lost { 2 } else { 1 });
            assert_eq!(
                receipt["status"],
                if lost {
                    "client_scenarios_passed"
                } else {
                    "incomplete"
                }
            );
            assert!(exercise(&f).is_err()); // Existing receipt forbids another run before any request.
        }
    }
    fn fixture() -> Value {
        json!({"version":2,"profile":"local_native","disposable":true,
            "independent_principals":true,"endpoint":"http://127.0.0.1:9",
            "publishable_key":"sb_publishable_fixture","schema":"dactyl_api",
            "client_revision":"1111111111111111111111111111111111111111",
            "service_revision":"2222222222222222222222222222222222222222",
            "principals":[{"token":"synthetic-one","context":{"version":1,"payload":{"opaque":1}}},
                {"token":"synthetic-two","context":{"version":1,"payload":{"opaque":2}}}],
            "scenarios":[{"principal":0,"access_mode":"read_only",
                "operations":[{"kind":"read","sql":"SELECT service_owned_statement()","params":[]}],
                "expected":{"outcome":"success","results":[{"Rows":[]}]}}],
            "receipt_path":"/private/native-client-receipt"})
    }
    fn valid(value: &Value) -> bool {
        parse::<Fixture>(&serde_json::to_vec(value).unwrap())
            .is_ok_and(|f| validate(&f, Profile::LocalNative, false).is_ok())
    }
    #[test]
    fn validation_is_offline_and_fails_closed() {
        assert!(valid(&fixture()));
        for (field, value) in [
            ("version", json!(1)),
            ("disposable", json!(false)),
            ("independent_principals", json!(false)),
            ("client_revision", json!("main")),
            ("receipt_path", json!("relative")),
            ("schema", json!("bad/schema")),
            ("principals", json!([])),
            ("scenarios", json!([])),
        ] {
            let mut f = fixture();
            f[field] = value;
            assert!(!valid(&f), "{field}");
        }
        let mut f = fixture();
        f["principals"][1]["token"] = json!("synthetic-one");
        assert!(!valid(&f));
        let mut f = fixture();
        f["scenarios"][0]["principal"] = json!(2);
        assert!(!valid(&f));
        let mut f = fixture();
        f["scenarios"][0]["expected"]["results"] = json!([]);
        assert!(!valid(&f));
        assert_eq!(
            parse::<Fixture>(br#"{"secret-value":"do-not-echo"}"#).err(),
            Some("invalid protected native fixture")
        );
    }
    #[test]
    fn origins_reject_credentials_and_remote_local_targets() {
        for bad in [
            "http://localhost",
            "http://example.test",
            "http://user:secret@127.0.0.1",
            "http://127.0.0.1/path",
            "http://127.0.0.1?token=secret",
            "http://127.0.0.1/#secret",
        ] {
            assert!(endpoint(bad, Profile::LocalNative, false).is_err());
        }
        assert!(endpoint("http://[::1]:9", Profile::LocalNative, false).is_ok());
        assert!(endpoint("https://example.test", Profile::HostedNative, false).is_err());
        assert!(endpoint("http://example.test", Profile::HostedNative, true).is_err());
        assert!(endpoint("https://example.test", Profile::HostedNative, true).is_ok());
    }
    #[test]
    fn result_matching_preserves_typed_values_and_uncertainty() {
        let expected = Expected::Error {
            kind: "Authorization".into(),
            origin: "RemotePolicy".into(),
            mutation: "Uncertain".into(),
        };
        let error = || DactylError::Adapter {
            kind: dactyl_db::AdapterErrorKind::Authorization,
            code: Some("authorization_denied".into()),
            message: "rejected request".into(),
        };
        assert!(!matches(&expected, Err(error())));
        let rolled_back = Expected::Error {
            kind: "Authorization".into(),
            origin: "RemotePolicy".into(),
            mutation: "RolledBack".into(),
        };
        assert!(matches(&rolled_back, Err(error())));
        assert!(!matches(&expected, Ok(AtomicResult::default())));
        let expected = Expected::Success {
            results: vec![OperationResult::Rows(dactyl_db::Rows(vec![
                dactyl_db::Row {
                    columns: vec!["value".into()],
                    values: vec![json!(true)],
                },
            ]))],
        };
        assert!(!matches(
            &expected,
            Ok(AtomicResult {
                results: vec![OperationResult::Rows(dactyl_db::Rows(vec![
                    dactyl_db::Row {
                        columns: vec!["value".into()],
                        values: vec![json!(1)]
                    }
                ],))]
            })
        ));
    }
    #[test]
    fn receipts_contain_no_fixture_data_and_preserve_pending_evidence() {
        let f: Fixture = parse(&serde_json::to_vec(&fixture()).unwrap()).unwrap();
        let text = receipt(&f, 0).to_string();
        for secret in [
            "synthetic-one",
            "synthetic-two",
            "127.0.0.1",
            "service_owned_statement",
            "opaque",
            "sb_publishable",
        ] {
            assert!(!text.contains(secret));
        }
        assert_eq!(receipt(&f, 0)["status"], "incomplete");
        assert_eq!(receipt(&f, 1)["status"], "client_scenarios_passed");
        assert_eq!(receipt(&f, 1)["cleanup"], "pending");
        assert_eq!(
            receipt(&f, 1)["physical_read_only_and_transactions"],
            "pending"
        );
    }
}
