//! Synthetic HTTP contract evidence only. These fixtures do not execute SQL or
//! prove PostgreSQL transactions, RLS, prepared statements, or live Supabase.
#![cfg(feature = "supabase")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, Datastore, DatastoreRoute, DatastoreTransport,
    GeneratedKey, OpenOptions, Operation, OperationResult, Parameter, StorageContext,
};
use serde_json::{json, Value};

fn context() -> StorageContext {
    StorageContext::new(
        1,
        json!({"opaque_target":"fixture", "opaque_session":"synthetic"}),
    )
    .unwrap()
}

fn open(endpoint: &str, access_mode: AccessMode) -> Connection {
    Connection::open_with_options_and_context(
        DatastoreRoute::supabase(endpoint, Some("synthetic-user-bearer".into())),
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
        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}", body.len()).ok();
    });
    (endpoint, receiver, handle)
}

fn empty_result(affected: u64) -> Value {
    json!({"columns":[],"rows":[],"affected_rows":affected,"generated_keys":[]})
}

#[test]
fn provider_transport_and_debug_do_not_expose_credentials() {
    let route = DatastoreRoute::supabase(
        "https://service.example/path-secret",
        Some("token-secret".into()),
    );
    assert_eq!(route.datastore(), Datastore::Supabase);
    assert_eq!(route.transport(), DatastoreTransport::Http);
    assert_eq!(
        DatastoreRoute::sqlite("local").transport(),
        DatastoreTransport::Local
    );
    assert_eq!(
        DatastoreRoute::neon("https://neon.example", None).transport(),
        DatastoreTransport::Http
    );
    let debug = format!("{route:?}");
    assert!(!debug.contains("path-secret"));
    assert!(!debug.contains("token-secret"));
}

#[test]
fn pre_auth_endpoint_validation_needs_no_credentials_or_network() {
    assert!(
        DatastoreRoute::supabase("https://offline-service.example", None)
            .validate()
            .is_ok()
    );
    assert!(DatastoreRoute::supabase("http://service.example", None)
        .validate()
        .is_err());
    assert!(DatastoreRoute::supabase("http://127.0.0.1:1", None)
        .validate()
        .is_ok());
    assert!(DatastoreRoute::supabase("http://[::1]:1", None)
        .validate()
        .is_ok());
    assert!(DatastoreRoute::supabase("http://127.0.0.1.example", None)
        .validate()
        .is_err());
}

#[test]
fn configuration_is_fail_closed_and_redacted() {
    for endpoint in [
        "postgres://user:password@db.example/db",
        "https://user:secret@service.example",
        "http://service.example",
        "http://localhost",
        "https://service.example?token=secret",
        "https://service.example#secret",
        "bad-secret",
    ] {
        let error = Connection::open_with_context(
            DatastoreRoute::supabase(endpoint, Some("bearer-secret".into())),
            Some(context()),
        )
        .err()
        .unwrap();
        assert!(!format!("{error:?}").contains("secret"));
    }
    for token in [
        None,
        Some("".into()),
        Some("  ".into()),
        Some("\r\nsecret".into()),
    ] {
        assert!(Connection::open_with_context(
            DatastoreRoute::supabase("https://service.example", token),
            Some(context())
        )
        .is_err());
    }
    let error = Connection::open(DatastoreRoute::supabase(
        "https://service.example",
        Some("bearer".into()),
    ))
    .err()
    .unwrap();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authentication));
}

#[test]
fn read_forwards_bearer_context_parameters_and_read_only_scope() {
    let response = json!({"columns":["nil","flag","int","real","text","blob"],"rows":[{"nil":null,"flag":true,"int":9223372036854775807_i64,"real":1.25,"text":"hello","blob":[0,255]}],"affected_rows":1});
    let (endpoint, received, server) = fixture(200, response);
    let db = open(&endpoint, AccessMode::ReadWrite);
    let rows = db
        .read(
            "select $1, $2, $3, $4, $5, $6",
            &[
                Parameter::Null,
                true.into(),
                42_i64.into(),
                1.25.into(),
                "text".into(),
                Parameter::Blob(vec![0, 255]),
            ],
        )
        .unwrap();
    let (headers, request) = received.recv().unwrap();
    assert!(headers.starts_with("POST /query "));
    assert!(headers
        .to_lowercase()
        .contains("authorization: bearer synthetic-user-bearer"));
    assert_eq!(request["context"], serde_json::to_value(context()).unwrap());
    assert_eq!(
        request["params"],
        json!([null, true, 42, 1.25, "text", [0, 255]])
    );
    assert_eq!(request["kind"], "read");
    assert_eq!(request["access_mode"], "read_only");
    let row = &rows.as_slice()[0];
    assert!(row.is_null("nil").unwrap());
    assert!(row.get_bool("flag").unwrap());
    assert_eq!(row.get_int("int").unwrap(), i64::MAX);
    assert_eq!(row.get_real("real").unwrap(), 1.25);
    assert_eq!(row.get_str("text").unwrap(), "hello");
    assert_eq!(row.get_blob("blob").unwrap(), vec![0, 255]);
    server.join().unwrap();
}

#[test]
fn explicit_affected_counts_keys_and_zero_row_cas() {
    for (affected, keys) in [(0, json!([])), (1, json!([42, "explicit-id"]))] {
        let mut response = empty_result(affected);
        response["generated_keys"] = keys;
        let (endpoint, received, server) = fixture(200, response);
        let result = open(&endpoint, AccessMode::ReadWrite)
            .write_result(
                "update tasks set title=$1 where id=$2 and version=$3",
                &["new".into(), "id".into(), 1.into()],
            )
            .unwrap();
        assert_eq!(result.affected_rows, affected);
        if affected == 1 {
            assert_eq!(
                result.generated_keys,
                vec![
                    GeneratedKey::Integer(42),
                    GeneratedKey::Text("explicit-id".into())
                ]
            );
        }
        assert_eq!(received.recv().unwrap().1["kind"], "write");
        server.join().unwrap();
    }
}

#[test]
fn atomic_is_one_ordered_request_and_preserves_result_order() {
    let (endpoint, received, server) = fixture(
        200,
        json!({"results":[empty_result(1),{"columns":["id"],"rows":[{"id":"a"}],"affected_rows":1},empty_result(0)]}),
    );
    let operations = vec![
        Operation::write("insert task", vec!["a".into()]),
        Operation::read("select task", vec![]),
        Operation::write("stale cas", vec![]),
    ];
    let result = open(&endpoint, AccessMode::ReadWrite)
        .atomic(&operations)
        .unwrap();
    assert!(
        matches!(&result.results[0], OperationResult::Write(write) if write.affected_rows == 1)
    );
    assert!(matches!(&result.results[1], OperationResult::Rows(rows) if rows.len() == 1));
    assert!(
        matches!(&result.results[2], OperationResult::Write(write) if write.affected_rows == 0)
    );
    let (headers, request) = received.recv().unwrap();
    assert!(headers.starts_with("POST /batch "));
    assert_eq!(
        request["operations"],
        serde_json::to_value(operations).unwrap()
    );
    assert_eq!(request["context"], serde_json::to_value(context()).unwrap());
    server.join().unwrap();
}

#[test]
fn read_only_and_unsupported_maintenance_fail_before_network() {
    let db = open("http://127.0.0.1:9", AccessMode::ReadOnly);
    assert_eq!(
        db.write("delete from tasks", &[])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::ReadOnly)
    );
    assert_eq!(
        db.atomic(&[Operation::schema("create table x (id text)", vec![])])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::ReadOnly)
    );
    assert!(db.atomic(&[]).unwrap().results.is_empty());
    assert_eq!(
        db.inspect_schema().unwrap_err().adapter_kind(),
        Some(AdapterErrorKind::Capability)
    );
    assert_eq!(
        db.verify_integrity().unwrap_err().adapter_kind(),
        Some(AdapterErrorKind::Capability)
    );
    assert_eq!(
        db.read("select $1", &[Parameter::Real(f64::NAN)])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Value)
    );
}

#[test]
fn errors_propagate_without_echoing_server_secrets() {
    for (status, code, kind) in [
        (401, "invalid_token", AdapterErrorKind::Authentication),
        (403, "authorization_denied", AdapterErrorKind::Authorization),
        (
            401,
            "authentication_required",
            AdapterErrorKind::Authentication,
        ),
        (
            403,
            "repository_not_authorized",
            AdapterErrorKind::Authorization,
        ),
        (400, "unsupported_query", AdapterErrorKind::Capability),
        (409, "row_conflict", AdapterErrorKind::Conflict),
        (409, "unique_violation", AdapterErrorKind::Constraint),
        (
            409,
            "transaction_aborted",
            AdapterErrorKind::TransactionAborted,
        ),
        (403, "read_only", AdapterErrorKind::ReadOnly),
        (503, "storage_unavailable", AdapterErrorKind::Unavailable),
        (500, "secret-unknown-code", AdapterErrorKind::Storage),
    ] {
        let (endpoint, received, server) = fixture(
            status,
            json!({"error":{"code":code,"message":"password=secret-server-data"}}),
        );
        let error = open(&endpoint, AccessMode::ReadWrite)
            .read("select secret-query", &[])
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(kind));
        assert!(!format!("{error:?}").contains("secret"));
        received.recv().unwrap();
        server.join().unwrap();
    }
}

#[test]
fn malformed_results_fail_instead_of_inventing_counts_or_nulls() {
    for response in [
        json!({"columns":[],"rows":[]}),
        json!({"columns":["x"],"rows":[{}],"affected_rows":1}),
        json!({"columns":["x","x"],"rows":[{"x":1}],"affected_rows":1}),
        json!({"columns":["x"],"rows":[[1]],"affected_rows":1}),
    ] {
        let (endpoint, received, server) = fixture(200, response);
        assert_eq!(
            open(&endpoint, AccessMode::ReadWrite)
                .read("select 1", &[])
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );
        received.recv().unwrap();
        server.join().unwrap();
    }
    let (endpoint, received, server) = fixture(200, json!({"results":[]}));
    assert_eq!(
        open(&endpoint, AccessMode::ReadWrite)
            .atomic(&[Operation::read("select 1", vec![])])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Protocol)
    );
    received.recv().unwrap();
    server.join().unwrap();
}

#[test]
fn unsupported_cells_are_capability_errors() {
    for cell in [
        json!({"secret":"object"}),
        json!([256]),
        json!(18446744073709551615_u64),
    ] {
        let (endpoint, received, server) = fixture(
            200,
            json!({"columns":["x"],"rows":[{"x":cell}],"affected_rows":1}),
        );
        assert_eq!(
            open(&endpoint, AccessMode::ReadWrite)
                .read("select x", &[])
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        received.recv().unwrap();
        server.join().unwrap();
    }
}

#[test]
fn redirects_are_not_followed_and_transport_errors_are_redacted() {
    let (endpoint, received, server) = fixture_raw(
        302,
        "redirect-secret".into(),
        "Location: http://127.0.0.1:9/secret-destination\r\n",
    );
    let error = open(&endpoint, AccessMode::ReadWrite)
        .read("select 1", &[])
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
    assert!(!format!("{error:?}").contains("secret"));
    received.recv().unwrap();
    server.join().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/secret-path", listener.local_addr().unwrap());
    drop(listener);
    let error = open(&endpoint, AccessMode::ReadWrite)
        .read("secret-query", &[])
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Transport));
    assert!(!format!("{error:?}").contains("secret"));
}

#[test]
fn request_and_response_size_limits_are_enforced() {
    let db = open("http://127.0.0.1:9", AccessMode::ReadWrite);
    let error = db
        .write(
            "update tasks set title=$1",
            &[Parameter::Text("x".repeat(4 * 1024 * 1024))],
        )
        .unwrap_err();
    assert_eq!(error.adapter_code(), Some("payload_too_large"));
    let mut exact_limit = empty_result(0).to_string();
    exact_limit.push_str(&" ".repeat(4 * 1024 * 1024 - exact_limit.len()));
    let (endpoint, received, server) = fixture_raw(200, exact_limit, "");
    assert!(open(&endpoint, AccessMode::ReadWrite)
        .read("select 1", &[])
        .is_ok());
    received.recv().unwrap();
    server.join().unwrap();
    let (endpoint, received, server) = fixture_raw(200, " ".repeat(4 * 1024 * 1024 + 1), "");
    let error = open(&endpoint, AccessMode::ReadWrite)
        .read("select 1", &[])
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
    received.recv().unwrap();
    server.join().unwrap();
}

#[test]
fn stalled_service_has_a_bounded_total_request_timeout() {
    assert_stalled_request_times_out(false);
}

#[test]
fn stalled_response_body_preserves_the_timeout_category() {
    assert_stalled_request_times_out(true);
}

fn assert_stalled_request_times_out(send_headers: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (stop, wait) = mpsc::channel::<()>();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut bytes = [0; 4096];
        assert!(socket.read(&mut bytes).unwrap() > 0);
        if send_headers {
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            socket.flush().unwrap();
        }
        // Keep the connection open until the client has observed its timeout.
        wait.recv_timeout(Duration::from_secs(25)).unwrap();
    });
    let db = open(&endpoint, AccessMode::ReadWrite);
    let started = std::time::Instant::now();
    let error = db.read("select 1", &[]).unwrap_err();
    stop.send(()).unwrap();
    server.join().unwrap();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Timeout));
    assert!(started.elapsed() < Duration::from_secs(23));
}

#[tokio::test(flavor = "current_thread")]
async fn sync_facade_is_safe_inside_a_current_thread_runtime() {
    assert_runtime_lifecycle_is_safe();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_facade_is_safe_inside_a_multithread_runtime() {
    assert_runtime_lifecycle_is_safe();
}

fn assert_runtime_lifecycle_is_safe() {
    drop(open("http://127.0.0.1:9", AccessMode::ReadWrite));
    let (endpoint, received, server) = fixture(200, empty_result(1));
    let db = open(&endpoint, AccessMode::ReadWrite);
    assert_eq!(
        db.write("update tasks set title=$1", &["async caller".into()])
            .unwrap(),
        1
    );
    drop(db);
    received.recv().unwrap();
    server.join().unwrap();

    let (endpoint, received, server) = fixture(
        401,
        json!({"error":{"code":"invalid_token","message":"secret"}}),
    );
    let db = open(&endpoint, AccessMode::ReadWrite);
    let error = db.read("select 1", &[]).unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authentication));
    assert!(!format!("{error:?}").contains("secret"));
    drop(db);
    received.recv().unwrap();
    server.join().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let db = open(&endpoint, AccessMode::ReadWrite);
    assert_eq!(
        db.read("select 1", &[]).unwrap_err().adapter_kind(),
        Some(AdapterErrorKind::Transport)
    );
    drop(db);
}
