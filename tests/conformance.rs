//! Conformance harness: same query, two adapters, identical row projections.
//!
//! Each test boots the SQLite adapter against an in-tempdir `.decapod/data/`
//! layout (booting the schema via `adapter/sqlite/schema.rs`) and the Neon
//! adapter against an in-process axum mock server that serves the same
//! JSON shape. Row projections must match column-for-column for both
//! `optimize = true` and `optimize = false`.

#![cfg(all(feature = "sqlite", feature = "neon"))]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;

use dactyl::{DactylConfig, DactylError, Rows};

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
        // Extract the table name by looking for `from <name>` (lowercased).
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
}

#[derive(Debug, Deserialize)]
struct MockRequest {
    #[allow(dead_code)]
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

#[derive(Debug, Serialize)]
struct MockResponse {
    columns: Vec<String>,
    rows: Vec<serde_json::Value>,
}

/// Spin up the in-process axum mock on a random port.
async fn spawn_mock(state: Arc<MockState>) -> SocketAddr {
    let app = Router::new()
        .route("/read", post(MockState::handle))
        .route("/write", post(MockState::handle))
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

/// Seed the SQLite file by opening it (which triggers `schema::bootstrap`)
/// and inserting a couple of rows into the per-store table.
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

#[test]
fn conformance_all_stores() {
    let tmp = TempDir::new().expect("tempdir");
    let state = Arc::new(MockState::default());

    // Coordinate: a tokio oneshot signals when the test is done so the
    // background mock runtime can shut down. We use tokio channels (async)
    // because the runtime polls them instead of blocking the worker thread.
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
                // Park until done — async, so the runtime keeps polling
                // spawned tasks.
                let _ = done_rx.await;
            });
        }
    });

    let endpoint = ready_rx.recv().expect("ready");

    for store in STORES {
        let path = sqlite_path(&tmp, store);
        seed_sqlite(&path, store, &seed_rows(store));

        dactyl::init(DactylConfig::sqlite(path.to_str().unwrap())).expect("init sqlite");
        let query = format!("select id, title, status from {store}");
        for optimize in [true, false] {
            let sqlite_rows = dactyl::read("sqlite", &query, optimize).expect("sqlite read");
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

        dactyl::init(DactylConfig::neon(
            endpoint.clone(),
            Some("test-token".into()),
            None,
        ))
        .expect("init neon");

        for optimize in [true, false] {
            let neon_rows = dactyl::read("neon", &query, optimize).expect("neon read");
            assert_eq!(
                neon_rows.len(),
                2,
                "store {store} optimize={optimize} neon row count"
            );
            for row in neon_rows.iter() {
                assert_eq!(row.columns, vec!["id", "title", "status"]);
                assert_eq!(row.values.len(), 3);
            }

            let sqlite_rows =
                dactyl::read("sqlite", &query, optimize).expect("sqlite read for compare");
            assert_eq!(
                project(&neon_rows),
                project(&sqlite_rows),
                "store {store} optimize={optimize}: projection mismatch"
            );
        }
    }

    // Signal the mock runtime to shut down and wait for the thread.
    let _ = done_tx.send(());
    let _ = mock_thread.join();
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

#[test]
fn unknown_datastore_errors() {
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl::init(DactylConfig::sqlite(path.to_str().unwrap())).expect("init");

    let res: Result<Rows, DactylError> = dactyl::read("nosuch", "select 1", true);
    assert!(matches!(res, Err(DactylError::UnknownDatastore(_))));
}

#[test]
fn dialect_mismatch_rejected_when_optimize_false() {
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl::init(DactylConfig::sqlite(path.to_str().unwrap())).expect("init");

    // Postgres-only construct (`now()`) against sqlite, optimize = false →
    // dialect mismatch.
    let res = dactyl::read("sqlite", "select now()", false);
    assert!(matches!(res, Err(DactylError::DialectMismatch { .. })));

    // optimize = true → proceeds past the dialect check; the SQLite adapter
    // may still fail to parse `now()` (it has no such function), which is
    // fine — the assertion is that we got past the analyzer.
    let res = dactyl::read("sqlite", "select now()", true);
    match res {
        Ok(_) => {}
        Err(DactylError::Adapter(_)) => {}
        Err(e) => panic!("expected Ok or Adapter error, got {e:?}"),
    }
}

#[test]
fn inline_directive_routes_dialect_check() {
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl::init(DactylConfig::sqlite(path.to_str().unwrap())).expect("init");

    // sqlite-only construct; the inline directive flips dialect to neon →
    // mismatch.
    let q = "-- dactyl: neon\nselect id from todos where id in (select id from json_each('[1]'))";
    let res = dactyl::read("sqlite", q, false);
    assert!(matches!(res, Err(DactylError::DialectMismatch { .. })));
}

#[test]
fn query_macro_lexical_analysis_runs_at_compile_time() {
    // The literal below is analyzed at compile time by `query!`. A SQLite-only
    // construct (json_each) against the active SQLite datastore under
    // optimize=false would fail to compile; we keep it portable here.
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "todos");
    seed_sqlite(&path, "todos", &seed_rows("todos"));
    dactyl::init(DactylConfig::sqlite(path.to_str().unwrap())).expect("init");

    let (sql, ds) = dactyl::query!("select id, title, status from todos");
    assert_eq!(sql, "select id, title, status from todos");
    assert_eq!(ds, "sqlite");
    // The macro runs the runtime analyzer; reading should succeed.
    let _ = dactyl::read("sqlite", "select id, title, status from todos", true).expect("read");
}
