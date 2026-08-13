//! Backend-neutral storage fixtures for Dactyl issues #57 and #64.
//!
//! These cases exercise only driver/storage behavior: parameterized reads and
//! writes, explicit keys, conditional CAS/zero-row outcomes, atomic
//! state-plus-event commit and rollback, read-only rejection, typed
//! constraint errors, concurrent scoped writes, deterministic cleanup, and
//! opaque storage-context no-op behavior.
//!
//! Local SQLite always runs the full matrix. The Neon path uses an executing
//! in-process mock that forwards the same SQL through the Neon adapter so the
//! cases stay backend-neutral without talking to live Propodus. Live
//! Propodus/Vercel Neon is reported as `unavailable` unless
//! `DACTYL_LIVE_PROPODUS_ROUTE` is set, and a skipped live backend is never
//! recorded as passed.

#![cfg(feature = "sqlite")]

#[cfg(feature = "neon")]
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, DatastoreRoute, GeneratedKey, OpenOptions, Operation,
    OperationResult, Parameter, StorageContext,
};
use serde_json::json;
#[cfg(feature = "neon")]
use serde_json::Value;
use tempfile::NamedTempFile;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CaseOutcome {
    case: &'static str,
    backend: &'static str,
    status: &'static str,
    detail: String,
}

impl CaseOutcome {
    fn passed(case: &'static str, backend: &'static str) -> Self {
        Self {
            case,
            backend,
            status: "passed",
            detail: String::new(),
        }
    }

    fn unavailable(case: &'static str, backend: &'static str, detail: impl Into<String>) -> Self {
        Self {
            case,
            backend,
            status: "unavailable",
            detail: detail.into(),
        }
    }
}

fn opaque_tenancy_context() -> StorageContext {
    StorageContext::new(
        1,
        json!({
            "org_id": "org-does-not-matter-locally",
            "user_id": "user-does-not-matter-locally",
            "repository_id": "repo-does-not-matter-locally",
            "opaque_target": "target",
            "opaque_session": "session"
        }),
    )
    .unwrap()
}

fn open_local(path: &str, context: Option<StorageContext>) -> Connection {
    Connection::open_with_context(DatastoreRoute::sqlite(path), context).unwrap()
}

fn setup_state_event_schema(db: &Connection) {
    db.atomic(&[Operation::schema(
        "create table if not exists records (id integer primary key, name text, version integer not null); create table if not exists events (id integer primary key, record_id integer not null, name text)",
        Vec::new(),
    )])
    .unwrap();
}

fn parameterized_read_write(db: &Connection) {
    db.write(
        "create table if not exists app (id integer primary key, name text, enabled integer, payload blob)",
        &[],
    )
    .unwrap();
    assert_eq!(
        db.write(
            "insert into app (name, enabled, payload) values ($1, $2, $3)",
            &[
                Parameter::Text("opened".into()),
                Parameter::Bool(true),
                Parameter::Blob(vec![1, 2, 3]),
            ],
        )
        .unwrap(),
        1
    );
    let rows = db
        .read(
            "select id, name, enabled, payload from app where name = $1",
            &[Parameter::Text("opened".into())],
        )
        .unwrap();
    let row = &rows.as_slice()[0];
    assert_eq!(row.get_int("id").unwrap(), 1);
    assert_eq!(row.get_str("name").unwrap(), "opened");
    assert!(row.get_bool("enabled").unwrap());
    assert_eq!(row.get_json("payload").unwrap(), json!([1, 2, 3]));
}

fn explicit_ids_and_affected_rows(db: &Connection) {
    setup_state_event_schema(db);
    let result = db
        .write_result(
            "insert into records (name, version) values ($1, $2)",
            &[Parameter::Text("alpha".into()), Parameter::Integer(1)],
        )
        .unwrap();
    assert_eq!(result.affected_rows, 1);
    assert_eq!(result.generated_key(), Some(&GeneratedKey::Integer(1)));
    assert_eq!(
        db.write(
            "update records set name = $1 where id = $2",
            &[Parameter::Text("beta".into()), Parameter::Integer(1)],
        )
        .unwrap(),
        1
    );
}

fn conditional_cas_and_zero_row(db: &Connection) {
    setup_state_event_schema(db);
    db.write(
        "insert into records (id, name, version) values ($1, $2, $3)",
        &[
            Parameter::Integer(7),
            Parameter::Text("fresh".into()),
            Parameter::Integer(1),
        ],
    )
    .unwrap();
    assert_eq!(
        db.write(
            "update records set name = $1, version = $2 where id = $3 and version = $4",
            &[
                Parameter::Text("committed".into()),
                Parameter::Integer(2),
                Parameter::Integer(7),
                Parameter::Integer(1),
            ],
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.write(
            "update records set name = $1, version = $2 where id = $3 and version = $4",
            &[
                Parameter::Text("stale".into()),
                Parameter::Integer(3),
                Parameter::Integer(7),
                Parameter::Integer(1),
            ],
        )
        .unwrap(),
        0
    );
    let rows = db
        .read(
            "select name, version from records where id = $1",
            &[Parameter::Integer(7)],
        )
        .unwrap();
    let row = &rows.as_slice()[0];
    assert_eq!(row.get_str("name").unwrap(), "committed");
    assert_eq!(row.get_int("version").unwrap(), 2);
}

fn atomic_state_plus_event_commit(db: &Connection) {
    setup_state_event_schema(db);
    let result = db
        .atomic(&[
            Operation::write(
                "insert into records (id, name, version) values ($1, $2, $3)",
                vec![
                    Parameter::Integer(11),
                    Parameter::Text("state".into()),
                    Parameter::Integer(1),
                ],
            ),
            Operation::write(
                "insert into events (record_id, name) values ($1, $2)",
                vec![Parameter::Integer(11), Parameter::Text("created".into())],
            ),
            Operation::read(
                "select name from events where record_id = $1 order by id",
                vec![Parameter::Integer(11)],
            ),
        ])
        .unwrap();
    match &result.results[2] {
        OperationResult::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows.as_slice()[0].get_str("name").unwrap(), "created");
        }
        other => panic!("expected event rows, got {other:?}"),
    }
}

fn atomic_state_plus_event_rollback(db: &Connection) {
    setup_state_event_schema(db);
    db.write(
        "insert into records (id, name, version) values ($1, $2, $3)",
        &[
            Parameter::Integer(12),
            Parameter::Text("kept".into()),
            Parameter::Integer(1),
        ],
    )
    .unwrap();
    let error = db
        .atomic(&[
            Operation::write(
                "insert into events (record_id, name) values ($1, $2)",
                vec![Parameter::Integer(12), Parameter::Text("rolled".into())],
            ),
            Operation::write(
                "insert into missing (name) values ($1)",
                vec![Parameter::Text("fail".into())],
            ),
        ])
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Query));
    assert!(db
        .read(
            "select name from events where record_id = $1",
            &[Parameter::Integer(12)],
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        db.read(
            "select name from records where id = $1",
            &[Parameter::Integer(12)],
        )
        .unwrap()
        .as_slice()[0]
            .get_str("name")
            .unwrap(),
        "kept"
    );
}

fn read_only_rejects_writes(db: &Connection, path: &str) {
    setup_state_event_schema(db);
    let readonly = Connection::open_with_options(
        DatastoreRoute::sqlite(path),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            lock_timeout: Duration::from_millis(25),
        },
    )
    .unwrap();
    let error = readonly
        .write(
            "insert into records (name, version) values ($1, $2)",
            &[Parameter::Text("nope".into()), Parameter::Integer(1)],
        )
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::ReadOnly));
}

fn constraint_is_typed(db: &Connection) {
    db.write(
        "create table if not exists unique_names (id integer primary key, name text unique)",
        &[],
    )
    .unwrap();
    db.write(
        "insert into unique_names (name) values ($1)",
        &[Parameter::Text("once".into())],
    )
    .unwrap();
    let error = db
        .write(
            "insert into unique_names (name) values ($1)",
            &[Parameter::Text("once".into())],
        )
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Constraint));
}

fn context_is_local_noop(path: &str) {
    let without = open_local(path, None);
    setup_state_event_schema(&without);
    without
        .write(
            "insert into records (id, name, version) values ($1, $2, $3)",
            &[
                Parameter::Integer(3),
                Parameter::Text("plain".into()),
                Parameter::Integer(1),
            ],
        )
        .unwrap();
    let with = open_local(path, Some(opaque_tenancy_context()));
    assert_eq!(with.context().unwrap().version(), 1);
    assert_eq!(
        with.write(
            "update records set name = $1 where id = $2 and version = $3",
            &[
                Parameter::Text("still-local".into()),
                Parameter::Integer(3),
                Parameter::Integer(1),
            ],
        )
        .unwrap(),
        1
    );
    let rows = with
        .read(
            "select name from records where id = $1",
            &[Parameter::Integer(3)],
        )
        .unwrap();
    let row = &rows.as_slice()[0];
    assert_eq!(row.get_str("name").unwrap(), "still-local");
    let error = with
        .write(
            "insert into records (id, name, version) values ($1, $2, $3)",
            &[
                Parameter::Integer(3),
                Parameter::Text("dup".into()),
                Parameter::Integer(1),
            ],
        )
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Constraint));
}

fn concurrent_scoped_writes_and_cleanup(path: &str) {
    let setup = open_local(path, None);
    setup
        .write(
            "create table scoped_items (id integer primary key, writer text)",
            &[],
        )
        .unwrap();
    drop(setup);

    let left_path = path.to_owned();
    let right_path = path.to_owned();
    let left = thread::spawn(move || {
        let db = open_local(&left_path, None);
        for index in 0..8 {
            db.write(
                "insert into scoped_items (writer) values ($1)",
                &[Parameter::Text(format!("left-{index}"))],
            )
            .unwrap();
        }
    });
    let right = thread::spawn(move || {
        let db = open_local(&right_path, None);
        for index in 0..8 {
            db.write(
                "insert into scoped_items (writer) values ($1)",
                &[Parameter::Text(format!("right-{index}"))],
            )
            .unwrap();
        }
    });
    left.join().expect("left writer");
    right.join().expect("right writer");

    let db = open_local(path, None);
    let rows = db
        .read("select writer from scoped_items order by id", &[])
        .unwrap();
    assert_eq!(rows.len(), 16);
    db.write("drop table scoped_items", &[]).unwrap();
    assert!(db.read("select writer from scoped_items", &[]).is_err());
    assert!(!std::path::Path::new(&format!("{path}.lock")).exists());
}

fn run_local_matrix(path: &str) -> Vec<CaseOutcome> {
    let mut outcomes = Vec::new();
    let db = open_local(path, None);
    parameterized_read_write(&db);
    outcomes.push(CaseOutcome::passed(
        "parameterized_read_write",
        "local_sqlite",
    ));
    explicit_ids_and_affected_rows(&db);
    outcomes.push(CaseOutcome::passed(
        "explicit_ids_and_affected_rows",
        "local_sqlite",
    ));
    conditional_cas_and_zero_row(&db);
    outcomes.push(CaseOutcome::passed(
        "conditional_cas_and_zero_row",
        "local_sqlite",
    ));
    atomic_state_plus_event_commit(&db);
    outcomes.push(CaseOutcome::passed(
        "atomic_state_plus_event_commit",
        "local_sqlite",
    ));
    atomic_state_plus_event_rollback(&db);
    outcomes.push(CaseOutcome::passed(
        "atomic_state_plus_event_rollback",
        "local_sqlite",
    ));
    read_only_rejects_writes(&db, path);
    outcomes.push(CaseOutcome::passed(
        "read_only_rejects_writes",
        "local_sqlite",
    ));
    constraint_is_typed(&db);
    outcomes.push(CaseOutcome::passed("constraint_is_typed", "local_sqlite"));
    context_is_local_noop(path);
    outcomes.push(CaseOutcome::passed("context_is_local_noop", "local_sqlite"));
    concurrent_scoped_writes_and_cleanup(path);
    outcomes.push(CaseOutcome::passed(
        "concurrent_scoped_writes_and_cleanup",
        "local_sqlite",
    ));
    outcomes
}

fn live_propodus_outcomes() -> Vec<CaseOutcome> {
    match std::env::var("DACTYL_LIVE_PROPODUS_ROUTE") {
        Ok(route) if !route.trim().is_empty() => {
            vec![CaseOutcome::unavailable(
                "live_propodus_matrix",
                "live_propodus",
                "DACTYL_LIVE_PROPODUS_ROUTE is set, but live Propodus/Vercel Neon proof is owned by a follow-up issue and is not executed in this local suite",
            )]
        }
        _ => vec![CaseOutcome::unavailable(
            "live_propodus_matrix",
            "live_propodus",
            "DACTYL_LIVE_PROPODUS_ROUTE is unset; live Propodus/Vercel Neon is an unavailable external prerequisite",
        )],
    }
}

fn assert_report(outcomes: &[CaseOutcome]) {
    let report = json!({
        "suite": "dactyl-backend-neutral-storage-fixtures",
        "issues": [57, 64],
        "outcomes": outcomes.iter().map(|outcome| {
            json!({
                "case": outcome.case,
                "backend": outcome.backend,
                "status": outcome.status,
                "detail": outcome.detail,
            })
        }).collect::<Vec<_>>(),
    });
    eprintln!("{}", serde_json::to_string_pretty(&report).unwrap());
    assert!(
        outcomes.iter().all(|outcome| outcome.status != "failed"),
        "fixture report contained failures: {report}"
    );
    assert!(
        outcomes
            .iter()
            .any(|outcome| outcome.backend == "local_sqlite" && outcome.status == "passed"),
        "local SQLite must pass; a skipped local backend is a failure"
    );
    let live = outcomes
        .iter()
        .filter(|outcome| outcome.backend == "live_propodus")
        .collect::<Vec<_>>();
    assert!(
        !live.is_empty() && live.iter().all(|outcome| outcome.status == "unavailable"),
        "live Propodus must be unavailable, never a false-green pass: {report}"
    );
}

#[test]
fn local_sqlite_fixture_matrix_is_complete() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let mut outcomes = run_local_matrix(&path);
    outcomes.extend(live_propodus_outcomes());
    assert_report(&outcomes);
}

#[cfg(feature = "neon")]
mod neon_executing_mock {
    use super::*;
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use dactyl_db::{AtomicResult, Datastore, WriteResult};
    use tokio::runtime::Runtime;
    use tokio::sync::oneshot;

    #[derive(Clone)]
    struct MockState {
        path: String,
        requests: RequestLog,
    }

    type RequestLog = Arc<Mutex<Vec<Value>>>;

    fn first_word(sql: &str) -> String {
        sql.split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase()
    }

    fn error_body(status: StatusCode, code: &str, message: &str) -> (StatusCode, Json<Value>) {
        (
            status,
            Json(json!({
                "error": {"code": code, "message": message}
            })),
        )
    }

    fn require_context(request: &Value) -> Result<(), (StatusCode, Json<Value>)> {
        match request.get("context") {
            None => Err(error_body(
                StatusCode::UNAUTHORIZED,
                "authentication_required",
                "missing storage context",
            )),
            Some(context) => {
                let version = context.get("version").and_then(Value::as_u64).unwrap_or(0);
                let payload_is_object = context
                    .get("payload")
                    .map(Value::is_object)
                    .unwrap_or(false);
                if version == 0 || !payload_is_object {
                    Err(error_body(
                        StatusCode::BAD_REQUEST,
                        "invalid_context",
                        "malformed storage context",
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn write_json(result: WriteResult) -> Value {
        json!({
            "affected_rows": result.affected_rows,
            "generated_keys": result.generated_keys,
            "rows": []
        })
    }

    fn rows_json(rows: dactyl_db::Rows) -> Value {
        let columns = rows
            .as_slice()
            .first()
            .map(|row| row.columns.clone())
            .unwrap_or_default();
        let objects = rows
            .iter()
            .map(|row| {
                row.columns
                    .iter()
                    .zip(row.values.iter())
                    .map(|(column, value)| (column.clone(), value.clone()))
                    .collect::<serde_json::Map<_, _>>()
            })
            .collect::<Vec<_>>();
        json!({
            "columns": columns,
            "rows": objects
        })
    }

    fn operation_json(result: OperationResult) -> Value {
        match result {
            OperationResult::Rows(rows) => rows_json(rows),
            OperationResult::Write(result) => write_json(result),
        }
    }

    fn open_store(path: &str) -> Connection {
        Connection::open(DatastoreRoute::sqlite(path)).unwrap()
    }

    fn decode_params(value: &Value) -> Vec<Parameter> {
        serde_json::from_value(value.clone()).unwrap_or_default()
    }

    fn decode_operations(value: &Value) -> Vec<Operation> {
        serde_json::from_value(value.clone()).unwrap_or_default()
    }

    async fn query(
        State(state): State<MockState>,
        Json(request): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state.requests.lock().unwrap().push(request.clone());
        if let Err(error) = require_context(&request) {
            return error;
        }
        let sql = request["sql"].as_str().unwrap_or_default();
        if sql == "authorization_failure" {
            return error_body(
                StatusCode::FORBIDDEN,
                "repository_not_authorized",
                "remote authorization denied",
            );
        }
        let params = decode_params(&request["params"]);
        let db = open_store(&state.path);
        let result = if first_word(sql) == "select" {
            db.read(sql, &params).map(rows_json)
        } else {
            db.write_result(sql, &params).map(write_json)
        };
        match result {
            Ok(body) => (StatusCode::OK, Json(body)),
            Err(error) => {
                let code = error.adapter_code().unwrap_or(match error.adapter_kind() {
                    Some(AdapterErrorKind::Constraint) => "constraint_failed",
                    Some(AdapterErrorKind::ReadOnly) => "read_only",
                    Some(AdapterErrorKind::Timeout) => "timeout",
                    _ => "query_failed",
                });
                error_body(StatusCode::BAD_REQUEST, code, &error.to_string())
            }
        }
    }

    async fn batch(
        State(state): State<MockState>,
        Json(request): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state.requests.lock().unwrap().push(request.clone());
        if let Err(error) = require_context(&request) {
            return error;
        }
        let operations = decode_operations(&request["operations"]);
        let db = open_store(&state.path);
        match db.atomic(&operations) {
            Ok(AtomicResult { results }) => (
                StatusCode::OK,
                Json(json!({
                    "results": results.into_iter().map(operation_json).collect::<Vec<_>>()
                })),
            ),
            Err(error) => {
                let status = match error.adapter_kind() {
                    Some(AdapterErrorKind::Query)
                    | Some(AdapterErrorKind::Constraint)
                    | Some(AdapterErrorKind::InvalidOperation)
                    | Some(AdapterErrorKind::Value) => StatusCode::BAD_REQUEST,
                    Some(AdapterErrorKind::ReadOnly) => StatusCode::FORBIDDEN,
                    _ => StatusCode::CONFLICT,
                };
                let code = error.adapter_code().unwrap_or(match error.adapter_kind() {
                    Some(AdapterErrorKind::Query) => "query_failed",
                    Some(AdapterErrorKind::Constraint) => "constraint_failed",
                    Some(AdapterErrorKind::ReadOnly) => "read_only",
                    _ => "transaction_aborted",
                });
                error_body(status, code, &error.to_string())
            }
        }
    }

    fn with_executing_server(test: impl FnOnce(String, RequestLog) + Send + 'static) {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().into_owned();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = MockState {
            path,
            requests: requests.clone(),
        };
        let (shutdown, shutdown_rx) = oneshot::channel();
        let thread = thread::spawn(move || {
            let runtime = Runtime::new().unwrap();
            runtime.block_on(async move {
                let app = Router::new()
                    .route("/query", post(query))
                    .route("/batch", post(batch))
                    .with_state(state);
                axum::serve(tokio::net::TcpListener::from_std(listener).unwrap(), app)
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .unwrap();
            });
        });
        test(format!("http://{address}"), requests);
        let _ = shutdown.send(());
        thread.join().unwrap();
        drop(file);
    }

    fn open_neon(endpoint: &str) -> Connection {
        Connection::open_with_context(
            DatastoreRoute::neon(endpoint, Some("fixture-token".into())),
            Some(opaque_tenancy_context()),
        )
        .unwrap()
    }

    #[test]
    fn neon_mock_runs_the_same_fixture_cases() {
        with_executing_server(|endpoint, requests| {
            let db = open_neon(&endpoint);
            assert_eq!(db.datastore(), Datastore::Neon);
            parameterized_read_write(&db);
            explicit_ids_and_affected_rows(&db);
            conditional_cas_and_zero_row(&db);
            atomic_state_plus_event_commit(&db);
            atomic_state_plus_event_rollback(&db);
            constraint_is_typed(&db);

            let error = db.write("authorization_failure", &[]).unwrap_err();
            assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authorization));
            assert_eq!(error.adapter_code(), Some("repository_not_authorized"));

            let logged = requests.lock().unwrap();
            assert!(logged
                .iter()
                .all(|request| request.get("context").is_some()));
            assert!(logged.iter().any(|request| {
                request["context"]["payload"]["org_id"] == "org-does-not-matter-locally"
            }));
        });
    }

    #[test]
    fn neon_mock_fixture_report_keeps_live_unavailable() {
        with_executing_server(|endpoint, _requests| {
            let db = open_neon(&endpoint);
            parameterized_read_write(&db);
            explicit_ids_and_affected_rows(&db);
            conditional_cas_and_zero_row(&db);
            atomic_state_plus_event_commit(&db);
            atomic_state_plus_event_rollback(&db);
            constraint_is_typed(&db);
            let error = db.write("authorization_failure", &[]).unwrap_err();
            assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authorization));

            let file = NamedTempFile::new().unwrap();
            let path = file.path().to_string_lossy().into_owned();
            let mut outcomes = run_local_matrix(&path);
            outcomes.extend([
                CaseOutcome::passed("parameterized_read_write", "neon_mock"),
                CaseOutcome::passed("explicit_ids_and_affected_rows", "neon_mock"),
                CaseOutcome::passed("conditional_cas_and_zero_row", "neon_mock"),
                CaseOutcome::passed("atomic_state_plus_event_commit", "neon_mock"),
                CaseOutcome::passed("atomic_state_plus_event_rollback", "neon_mock"),
                CaseOutcome::passed("constraint_is_typed", "neon_mock"),
                CaseOutcome::passed("typed_authorization_failure", "neon_mock"),
            ]);
            outcomes.extend(live_propodus_outcomes());
            assert_report(&outcomes);
        });
    }
}
