//! Conformance harness: same query, two adapters, identical row projections.
//!
//! Each test boots the SQLite adapter against an in-tempdir `.decapod/data/`
//! layout and the Neon adapter against an in-process axum mock server.
//! Row projections must match column-for-column.

#![cfg(all(feature = "sqlite", feature = "neon"))]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;

use dactyl_db::{DactylError, Parameter, Rows, Statement};

static ENV_MUTEX: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    ENV_MUTEX
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap()
}

/// Mock neon-server state, shared across all tests.
#[derive(Default)]
struct MockState {
    rows: Mutex<HashMap<String, Vec<serde_json::Value>>>,
}

impl MockState {
    async fn handle(
        State(state): State<Arc<MockState>>,
        Json(req): Json<MockRequest>,
    ) -> Json<MockResponse> {
        let rows = state.rows.lock().await;
        let sql_lc = req.sql.to_ascii_lowercase();
        let table = sql_lc
            .split_whitespace()
            .skip_while(|w| *w != "from")
            .nth(1)
            .map(|s| {
                s.chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect::<String>()
            })
            .unwrap_or_default();
        let data = rows.get(&table).cloned().unwrap_or_default();
        Json(MockResponse {
            columns: vec!["id".into(), "title".into(), "status".into()],
            rows: data,
        })
    }

    async fn handle_batch(
        State(state): State<Arc<MockState>>,
        Json(req): Json<MockBatchRequest>,
    ) -> Json<MockBatchResponse> {
        let rows = state.rows.lock().await;
        let mut results = Vec::new();
        for stmt in req.statements {
            let sql_lc = stmt.sql.to_ascii_lowercase();
            let table = sql_lc
                .split_whitespace()
                .skip_while(|w| *w != "from")
                .nth(1)
                .map(|s| {
                    s.chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect::<String>()
                })
                .unwrap_or_default();
            let data = rows.get(&table).cloned().unwrap_or_default();
            results.push(MockResponse {
                columns: vec!["id".into(), "title".into(), "status".into()],
                rows: data,
            });
        }
        Json(MockBatchResponse { results })
    }
}

#[derive(Debug, Deserialize)]
struct MockRequest {
    sql: String,
    #[serde(default)]
    #[allow(dead_code)]
    params: Option<serde_json::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    optimize: bool,
    #[serde(default)]
    #[allow(dead_code)]
    write: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MockResponse {
    columns: Vec<String>,
    rows: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct MockBatchRequest {
    statements: Vec<MockStatement>,
}

#[derive(Debug, Deserialize)]
struct MockStatement {
    sql: String,
    #[serde(default)]
    #[allow(dead_code)]
    params: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct MockBatchResponse {
    results: Vec<MockResponse>,
}

/// Spin up the in-process axum mock on a random port.
async fn spawn_mock(state: Arc<MockState>) -> SocketAddr {
    let app = Router::new()
        .route("/read", post(MockState::handle))
        .route("/write", post(MockState::handle))
        .route("/batch", post(MockState::handle_batch))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });
    addr
}

const STORES: &[&str] = &[
    "todos",
    "knowledge",
    "governance",
    "memory",
    "automation",
    "broker_dedupe",
    "lcm",
    "federation",
    "events",
];

fn seed_rows(table: &str) -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"id": 1, "title": format!("{table}-a"), "status": "open"}),
        serde_json::json!({"id": 2, "title": format!("{table}-b"), "status": "done"}),
    ]
}

fn sqlite_path(tmp: &TempDir, store: &str) -> std::path::PathBuf {
    let dir = tmp.path().join(".decapod/data");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{store}.db"))
}

fn seed_sqlite(path: &std::path::Path, store: &str, rows: &[serde_json::Value]) {
    use rusqlite::Connection;
    let conn = Connection::open(path).expect("open");
    let ddl = format!(
        "create table if not exists {store} (
            id integer primary key,
            title text not null,
            status text not null
        )"
    );
    conn.execute(&ddl, []).expect("create table");
    for r in rows {
        let id = r.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
        let title = r.get("title").and_then(|v| v.as_str()).unwrap_or("");
        let status = r.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let _ = conn.execute(
            &format!("delete from {store} where id = ?1"),
            rusqlite::params![id],
        );
        conn.execute(
            &format!("insert into {store}(id, title, status) values (?1, ?2, ?3)"),
            rusqlite::params![id, title, status],
        )
        .expect("insert");
    }
}

fn project(rows: &Rows) -> Vec<(String, serde_json::Value)> {
    rows.iter()
        .flat_map(|r| {
            r.columns
                .iter()
                .zip(r.values.iter())
                .map(|(c, v)| (c.clone(), v.clone()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Conformance: every store, every adapter, every optimize value.
#[test]
fn conformance_all_stores() {
    let _guard = lock_env();
    dactyl_db::reset();
    let tmp = TempDir::new().expect("tempdir");
    let state = Arc::new(MockState::default());

    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<String>();

    let mock_thread = std::thread::spawn({
        let state = state.clone();
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("mock rt");
            rt.block_on(async {
                {
                    let mut rows = state.rows.lock().await;
                    for store in STORES {
                        rows.insert(store.to_string(), seed_rows(store));
                    }
                }
                let addr = spawn_mock(state.clone()).await;
                let _ = ready_tx.send(format!("http://{addr}"));
                let _ = done_rx.await;
            });
        }
    });

    let endpoint = ready_rx.recv().expect("ready");

    for store in STORES {
        let path = sqlite_path(&tmp, store);
        seed_sqlite(&path, store, &seed_rows(store));

        // ---- SQLite pass: DATASTORE = sqlite, DATASTORE_ROUTE = path ----
        dactyl_db::init("sqlite", path.to_str().unwrap(), None);

        let query = format!("select id, title, status from {store}");
        for optimize in [true, false] {
            let sqlite_rows = dactyl_db::read(&query, &[], optimize).expect("sqlite read");
            assert_eq!(
                sqlite_rows.len(),
                2,
                "store {store} optimize={optimize} sqlite row count"
            );
            for row in sqlite_rows.iter() {
                assert_eq!(row.columns, vec!["id", "title", "status"]);
                assert_eq!(row.values.len(), 3);
            }
        }

        // ---- Neon pass: DATASTORE = neon, DATASTORE_ROUTE = mock ----
        dactyl_db::init("neon", &endpoint, Some("test-token"));

        for optimize in [true, false] {
            let neon_rows = dactyl_db::read(&query, &[], optimize).expect("neon read");
            assert_eq!(
                neon_rows.len(),
                2,
                "store {store} optimize={optimize} neon row count"
            );
            for row in neon_rows.iter() {
                assert_eq!(row.columns, vec!["id", "title", "status"]);
                assert_eq!(row.values.len(), 3);
            }

            // Compare with sqlite projection shape.
            dactyl_db::init("sqlite", path.to_str().unwrap(), None);
            let sqlite_rows =
                dactyl_db::read(&query, &[], optimize).expect("sqlite read for compare");
            assert_eq!(
                project(&neon_rows),
                project(&sqlite_rows),
                "store {store} optimize={optimize}: projection mismatch"
            );
            dactyl_db::init("neon", &endpoint, Some("test-token"));
        }
    }

    // Clean up env vars at end of test.
    unsafe { std::env::remove_var("DATASTORE") };
    unsafe { std::env::remove_var("DATASTORE_ROUTE") };
    unsafe { std::env::remove_var("DATASTORE_TOKEN") };

    // Signal mock runtime to shut down, then wait for the thread.
    let _ = done_tx.send(());
    let _ = mock_thread.join();
}

#[test]
fn unsupported_construct_rejected_when_optimize_false() {
    let _guard = lock_env();
    dactyl_db::reset();
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl_db::init("sqlite", path.to_str().unwrap(), None);

    // Postgres-only construct (`now()`) on sqlite with optimize=false →
    // Unsupported. With optimize=true it gets past the analyzer and the
    // SQLite adapter rejects it.
    let res = dactyl_db::read("select now()", &[], false);
    assert!(matches!(res, Err(DactylError::Unsupported { .. })));

    let res = dactyl_db::read("select now()", &[], true);
    match res {
        Ok(_) => {}
        Err(DactylError::Adapter(_)) => {}
        Err(e) => panic!("expected Ok or Adapter error, got {e:?}"),
    }

    unsafe { std::env::remove_var("DATASTORE") };
    unsafe { std::env::remove_var("DATASTORE_ROUTE") };
}

#[test]
fn inline_directive_routes_dialect_check() {
    let _guard = lock_env();
    dactyl_db::reset();
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl_db::init("sqlite", path.to_str().unwrap(), None);

    // SQLite is the inferred dialect (no neon env), so the construct is
    // accepted at optimize=true and fails the analyzer at optimize=false.
    let q = "-- dactyl: neon\nselect id from todos where id in (select id from json_each('[1]'))";
    let res = dactyl_db::read(q, &[], false);
    assert!(matches!(res, Err(DactylError::Unsupported { .. })));

    unsafe { std::env::remove_var("DATASTORE") };
    unsafe { std::env::remove_var("DATASTORE_ROUTE") };
}

#[test]
fn query_macro_returns_rewritten_sql() {
    let _guard = lock_env();
    dactyl_db::reset();
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl_db::init("sqlite", path.to_str().unwrap(), None);

    let sql: String = dactyl_db::query!("select id, title, status from todos");
    assert_eq!(sql, "select id, title, status from todos");
    let rows = dactyl_db::read(&sql, &[], true).expect("read");
    assert_eq!(rows.len(), 2);

    unsafe { std::env::remove_var("DATASTORE") };
    unsafe { std::env::remove_var("DATASTORE_ROUTE") };
}

#[test]
fn test_parameterized_queries_and_type_safety() {
    let _guard = lock_env();
    dactyl_db::reset();
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl_db::init("sqlite", path.to_str().unwrap(), None);

    // Query with positional parameter
    let rows = dactyl_db::read(
        "select id, title, status from todos where id = $1",
        &[Parameter::Integer(2)],
        true,
    )
    .expect("read");
    assert_eq!(rows.len(), 1);
    let row = &rows.as_slice()[0];
    let id: i64 = row.get("id").expect("id");
    let title: String = row.get("title").expect("title");
    let status: String = row.get("status").expect("status");
    assert_eq!(id, 2);
    assert_eq!(title, "todos-b");
    assert_eq!(status, "done");

    // Missing column returns ColumnNotFound error
    let missing_res: Result<String, _> = row.get("missing_col");
    assert!(matches!(missing_res, Err(DactylError::ColumnNotFound(_))));

    // Invalid type casting returns Conversion error
    let invalid_cast: Result<bool, _> = row.get("title");
    assert!(matches!(invalid_cast, Err(DactylError::Conversion(_))));

    unsafe { std::env::remove_var("DATASTORE") };
    unsafe { std::env::remove_var("DATASTORE_ROUTE") };
}

#[test]
fn test_raw_execution_and_schema() {
    let _guard = lock_env();
    dactyl_db::reset();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("test_schema.db");
    dactyl_db::init("sqlite", path.to_str().unwrap(), None);

    // Create table using execute
    dactyl_db::execute(
        "create table test_table (id integer primary key, name text not null)",
        &[],
    )
    .expect("create table");

    // Insert row using execute
    let affected = dactyl_db::execute(
        "insert into test_table (id, name) values ($1, $2)",
        &[Parameter::Integer(42), Parameter::Text("hello".to_string())],
    )
    .expect("insert");
    assert_eq!(affected, 1);

    // Verify row
    let rows = dactyl_db::read("select id, name from test_table", &[], true).expect("read");
    assert_eq!(rows.len(), 1);
    let name: String = rows.as_slice()[0].get("name").expect("name");
    assert_eq!(name, "hello");

    unsafe { std::env::remove_var("DATASTORE") };
    unsafe { std::env::remove_var("DATASTORE_ROUTE") };
}

#[test]
fn test_atomic_transactions() {
    let _guard = lock_env();
    dactyl_db::reset();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("test_tx.db");
    dactyl_db::init("sqlite", path.to_str().unwrap(), None);

    dactyl_db::execute(
        "create table test_tx (id integer primary key, value text)",
        &[],
    )
    .expect("create");

    // Successful transaction batch
    let stmts = vec![
        Statement::new(
            "insert into test_tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(1), Parameter::Text("val1".to_string())],
        ),
        Statement::new(
            "insert into test_tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(2), Parameter::Text("val2".to_string())],
        ),
    ];
    dactyl_db::transaction(&stmts).expect("transaction success");

    // Verify both exist
    let rows = dactyl_db::read("select count(*) as cnt from test_tx", &[], true).expect("read");
    let cnt: i64 = rows.as_slice()[0].get("cnt").expect("cnt");
    assert_eq!(cnt, 2);

    // Failing transaction batch (duplicate key) -> should roll back!
    let failing_stmts = vec![
        Statement::new(
            "insert into test_tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(3), Parameter::Text("val3".to_string())],
        ),
        Statement::new(
            "insert into test_tx (id, value) values ($1, $2)",
            vec![
                Parameter::Integer(1),
                Parameter::Text("duplicate".to_string()),
            ],
        ),
    ];
    let tx_res = dactyl_db::transaction(&failing_stmts);
    assert!(tx_res.is_err());

    // Verify row 3 was rolled back and does not exist!
    let rows_after =
        dactyl_db::read("select count(*) as cnt from test_tx", &[], true).expect("read");
    let cnt_after: i64 = rows_after.as_slice()[0].get("cnt").expect("cnt");
    assert_eq!(cnt_after, 2);

    unsafe { std::env::remove_var("DATASTORE") };
    unsafe { std::env::remove_var("DATASTORE_ROUTE") };
}
