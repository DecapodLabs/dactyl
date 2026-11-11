//! Synthetic native RPC evidence only; no PostgreSQL or hosted proof is implied.
#![cfg(feature = "supabase")]
use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, DatastoreRoute, GeneratedKey, OpenOptions, Operation,
    OperationResult, Parameter, StorageContext, SupabaseRpcConfig,
};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
const KEY: &str = "sb_publishable_fixture";
const BEARER: &str = "synthetic-user";
fn context() -> StorageContext {
    StorageContext::new(17, json!({"future":{"opaque":[1,false,"value"]}})).unwrap()
}
fn route(origin: &str) -> DatastoreRoute {
    DatastoreRoute::supabase_rpc(SupabaseRpcConfig::v1(origin, KEY), Some(BEARER.into()))
}
fn open(origin: &str, access_mode: AccessMode) -> Connection {
    Connection::open_with_options_and_context(
        route(origin),
        OpenOptions {
            access_mode,
            ..OpenOptions::default()
        },
        Some(context()),
    )
    .unwrap()
}
/// A single-request server returning a fixed response, never a database.
fn fixture(
    status: u16,
    body: Value,
) -> (
    String,
    mpsc::Receiver<(String, Value)>,
    thread::JoinHandle<()>,
) {
    fixture_raw(status, body.to_string(), "")
}

fn fixture_raw(
    status: u16,
    body: String,
    extra_headers: &str,
) -> (
    String,
    mpsc::Receiver<(String, Value)>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (sender, receiver) = mpsc::channel();
    let extra_headers = extra_headers.to_owned();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0; 4096];
        let (headers, body_start, length) = loop {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
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
        while bytes.len() - body_start < length {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
        }
        let payload = serde_json::from_slice(&bytes[body_start..body_start + length]).unwrap();
        sender.send((headers, payload)).unwrap();
        if status == 0 {
            drop(stream);
            listener.set_nonblocking(true).unwrap();
            thread::sleep(Duration::from_millis(150));
            assert!(
                listener.accept().is_err(),
                "mutation was replayed after response loss"
            );
            return;
        }
        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}", body.len()).ok();
    });
    (endpoint, receiver, handle)
}

fn empty_result(affected: u64) -> Value {
    json!({"columns":[],"rows":[],"affected_rows":affected,"generated_keys":[]})
}

#[test]
fn headers_named_envelope_and_opaque_context() {
    let (origin, received, server) = fixture(
        200,
        json!({"columns":["n","b","i","r","s","blob"],"rows":[{"n":null,"b":true,"i":-9223372036854775808_i64,"r":1.25,"s":"text","blob":[0,255]}],"affected_rows":0,"generated_keys":[]}),
    );
    let params = vec![
        Parameter::Null,
        Parameter::Bool(true),
        Parameter::Integer(i64::MIN),
        Parameter::Real(1.25),
        Parameter::Text("text".into()),
        Parameter::Blob(vec![0, 255]),
    ];
    let rows = open(&origin, AccessMode::ReadWrite)
        .read("fixture", &params)
        .unwrap();
    let (headers, body) = received.recv().unwrap();
    server.join().unwrap();
    let mut lines = headers.lines();
    assert_eq!(
        lines.next(),
        Some("POST /rest/v1/rpc/dactyl_query_v1 HTTP/1.1")
    );
    let headers: std::collections::HashMap<_, _> = lines
        .filter_map(|line| {
            let (n, v) = line.split_once(':')?;
            Some((n.to_ascii_lowercase(), v.trim().to_owned()))
        })
        .collect();
    assert_eq!(
        headers.get("authorization"),
        Some(&format!("Bearer {BEARER}"))
    );
    assert_eq!(headers.get("apikey").map(String::as_str), Some(KEY));
    for (name, value) in [
        ("accept", "application/json"),
        ("content-type", "application/json"),
        ("content-profile", "dactyl_api"),
        ("accept-profile", "dactyl_api"),
        ("dactyl-contract-version", "1"),
    ] {
        assert_eq!(headers.get(name).map(String::as_str), Some(value));
    }
    assert_eq!(
        body,
        json!({"request":{"version":1,"kind":"read","sql":"fixture","params":params,"access_mode":"read_only","context":context()}})
    );
    assert_eq!(rows.as_slice()[0].get_int("i").unwrap(), i64::MIN);
    assert_eq!(rows.as_slice()[0].get_blob("blob").unwrap(), vec![0, 255]);
}
#[test]
fn one_batch_retains_order_zero_cas_and_explicit_keys() {
    let mut first = empty_result(1);
    first["generated_keys"] = json!(["created", 42]);
    let (origin, received, server) = fixture(
        200,
        json!({"results":[first,empty_result(0),empty_result(0)]}),
    );
    let operations = vec![
        Operation::write("insert", vec![Parameter::Text("bound".into())]),
        Operation::write("cas", vec![]),
        Operation::read("observe", vec![]),
    ];
    let result = open(&origin, AccessMode::ReadWrite)
        .atomic(&operations)
        .unwrap();
    assert_eq!(result.results.len(), 3);
    match &result.results[0] {
        OperationResult::Write(w) => assert_eq!(
            w.generated_keys,
            vec![
                GeneratedKey::Text("created".into()),
                GeneratedKey::Integer(42)
            ]
        ),
        _ => panic!("write shape"),
    }
    match &result.results[1] {
        OperationResult::Write(w) => assert_eq!(w.affected_rows, 0),
        _ => panic!("CAS shape"),
    }
    assert!(matches!(result.results[2], OperationResult::Rows(_)));
    let (h, b) = received.recv().unwrap();
    server.join().unwrap();
    assert!(h.starts_with("POST /rest/v1/rpc/dactyl_batch_v1 "));
    assert_eq!(
        b,
        json!({"request":{"version":1,"operations":operations,"access_mode":"read_write","context":context()}})
    );
}
#[test]
fn native_configuration_is_explicit_and_secure() {
    for origin in [
        "https://project.example/rest/v1",
        "https://user:password@project.example",
        "https://project.example?secret=1",
        "https://project.example#fragment",
        "http://localhost:1",
        "http://remote.example",
        "postgres://db/project",
    ] {
        assert!(route(origin).validate().is_err());
    }
    for key in [
        "",
        "sb_secret_PRIVATE",
        "eyJhbGci.legacy.anon",
        "sb_publishable_",
        "sb_publishable_bad\r\n",
    ] {
        assert!(DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1("https://project.example", key),
            None
        )
        .validate()
        .is_err());
    }
    for schema in ["", "9schema", "schema,path", "schema/path"] {
        assert!(DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1("https://project.example", KEY).with_schema(schema),
            None
        )
        .validate()
        .is_err());
    }
    let config =
        SupabaseRpcConfig::v1("https://private-origin.example", KEY).with_schema("private_schema");
    let r = DatastoreRoute::supabase_rpc(config.clone(), Some(BEARER.into()));
    let debug = format!("{config:?} {r:?}");
    for private in ["private-origin", KEY, "private_schema", BEARER] {
        assert!(!debug.contains(private));
    }
    assert_eq!(config.contract_version(), 1);
    assert!(r.validate().is_ok());
    assert!(Connection::open(r).is_err());
    assert!(Connection::open_with_context(
        DatastoreRoute::supabase_rpc(config, None),
        Some(context())
    )
    .is_err());
    assert!(
        DatastoreRoute::supabase("https://project.supabase.co", None)
            .supabase_rpc_config()
            .is_none()
    );
}
#[test]
fn read_only_empty_and_invalid_values_fail_without_network() {
    let db = open("http://127.0.0.1:1", AccessMode::ReadOnly);
    assert!(db.atomic(&[]).unwrap().results.is_empty());
    assert_eq!(
        db.write("write", &[]).unwrap_err().adapter_kind(),
        Some(AdapterErrorKind::ReadOnly)
    );
    assert_eq!(
        db.atomic(&[Operation::schema("ddl", vec![])])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::ReadOnly)
    );
    assert_eq!(
        db.read("read", &[Parameter::Real(f64::NAN)])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Value)
    );
}
#[test]
fn postgrest_errors_are_typed_and_redacted() {
    for (status, code, kind) in [
        (403, "authorization_denied", AdapterErrorKind::Authorization),
        (422, "unsupported_query", AdapterErrorKind::Capability),
        (406, "PGRST106", AdapterErrorKind::Capability),
        (404, "PGRST202", AdapterErrorKind::Capability),
        (401, "PGRST301", AdapterErrorKind::Authentication),
        (409, "23505", AdapterErrorKind::Constraint),
        (409, "40001", AdapterErrorKind::Conflict),
        (500, "57014", AdapterErrorKind::Timeout),
        (400, "25006", AdapterErrorKind::ReadOnly),
        (403, "42501", AdapterErrorKind::Authorization),
        (502, "unknown-secret", AdapterErrorKind::Unavailable),
    ] {
        let (origin, received, server) = fixture(
            status,
            json!({"code":code,"message":"message-secret","details":"sql-secret","hint":"credential-secret"}),
        );
        let error = open(&origin, AccessMode::ReadWrite)
            .write("fixture", &[])
            .unwrap_err();
        received.recv().unwrap();
        server.join().unwrap();
        assert_eq!(error.adapter_kind(), Some(kind));
        assert!(!format!("{error:?}").contains("secret"));
        if code == "PGRST106" {
            assert_eq!(error.adapter_code(), Some("unsupported_schema"));
        }
    }
}
#[test]
fn malformed_native_results_are_explicit_failures() {
    for body in [
        json!({"results":[]}),
        json!({"results":[{"columns":[],"rows":[],"affected_rows":0}]}),
        json!({"results":[empty_result(0),empty_result(0)]}),
    ] {
        let (origin, received, server) = fixture(200, body);
        let error = open(&origin, AccessMode::ReadWrite)
            .atomic(&[Operation::read("fixture", vec![])])
            .unwrap_err();
        received.recv().unwrap();
        server.join().unwrap();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
    }
    let (origin, received, server) = fixture(
        200,
        json!({"columns":["value"],"rows":[{"value":{"unsupported":true}}],"affected_rows":1,"generated_keys":[]}),
    );
    assert_eq!(
        open(&origin, AccessMode::ReadWrite)
            .write("fixture", &[])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Capability)
    );
    received.recv().unwrap();
    server.join().unwrap();
}
#[test]
fn redirect_never_forwards_the_credentials_or_retries_batch() {
    let destination = TcpListener::bind("127.0.0.1:0").unwrap();
    destination.set_nonblocking(true).unwrap();
    let (origin, received, server) = fixture_raw(
        307,
        "{}".into(),
        &format!("Location: http://{}\r\n", destination.local_addr().unwrap()),
    );
    assert_eq!(
        open(&origin, AccessMode::ReadWrite)
            .atomic(&[Operation::write("mutation", vec![])])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Protocol)
    );
    received.recv().unwrap();
    server.join().unwrap();
    assert!(destination.accept().is_err());
}

#[test]
fn lost_write_response_is_not_retried() {
    let (origin, received, server) = fixture(0, empty_result(1));
    let error = open(&origin, AccessMode::ReadWrite)
        .atomic(&[Operation::write("mutation", vec![])])
        .unwrap_err();
    received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Transport));
}

