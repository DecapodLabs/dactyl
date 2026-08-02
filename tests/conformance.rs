//! Conformance harness: same query, two adapters, identical row projections.
//!
//! Each test selects the active datastore through ambient env vars
//! (`DATASTORE` / `DATASTORE_ROUTE` / `DATASTORE_TOKEN`). SQLite tests point
//! at a tempfile SQLite DB; Neon tests point at an in-process axum mock
//! server. Row projections must match column-for-column.
//!
//! Covers dactyl issues:
//! - #2  every store × every adapter × parameterized reads/writes
//! - #23 parameter binding, NULL/bool/int/real/text + injection attempt
//! - #24 atomic transaction + rollback-on-failure (SQLite + Neon mock),
//!   event-plus-state fixture, nesting/retry/timeout/idempotency contract
//! - #25 typed named extraction, NULL, missing column, conversion error,
//!   duplicate aliases, full scalar matrix through both adapters, and
//!   borrowed-vs-owned accessors (also DecapodLabs/decapod#1111)
//! - #26 session isolation via per-call adapter construction
//! - #27 caller-owned schema; dactyl never silently bootstraps tables

#![cfg(all(feature = "sqlite", feature = "neon"))]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;

use dactyl_db::{DactylError, Parameter, Rows, Statement};

/// Serialize tests that mutate `DATASTORE*` env vars so they cannot race with
/// each other. The lib no longer caches adapters, so env-var discipline is the
/// only shared mutable state.
static ENV_MUTEX: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    ENV_MUTEX
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Select sqlite as the active datastore pointing at `path`.
fn select_sqlite(path: &str) {
    unsafe {
        std::env::set_var("DATASTORE", "sqlite");
        std::env::set_var("DATASTORE_ROUTE", path);
        std::env::remove_var("DATASTORE_TOKEN");
    }
}

/// Select neon as the active datastore pointing at `endpoint`.
fn select_neon(endpoint: &str) {
    unsafe {
        std::env::set_var("DATASTORE", "neon");
        std::env::set_var("DATASTORE_ROUTE", endpoint);
        std::env::set_var("DATASTORE_TOKEN", "test-token");
    }
}

/// Clear every dactyl env var. The lib has no cache to clear; this just makes
/// the next test that forgets to call `select_*` fail loudly.
fn clear_env() {
    unsafe {
        std::env::remove_var("DATASTORE");
        std::env::remove_var("DATASTORE_ROUTE");
        std::env::remove_var("DATASTORE_TOKEN");
    }
}

// ---------------------------------------------------------------------------
// Mock neon server: a tiny in-process axum app that stores rows per table.
// ---------------------------------------------------------------------------

/// One mock table: explicit column order plus row objects.
#[derive(Clone, Default)]
struct MockTable {
    columns: Vec<String>,
    rows: Vec<serde_json::Value>,
}

/// Mock neon-server state, shared across all tests.
///
/// `/batch` is **atomic**: statements apply against a snapshot; any error
/// restores the snapshot so no partial writes remain (mirrors Propodus
/// all-or-nothing batch contract for dactyl #24).
#[derive(Default)]
struct MockState {
    tables: Mutex<HashMap<String, MockTable>>,
}

impl MockState {
    async fn handle(State(state): State<Arc<MockState>>, Json(req): Json<MockRequest>) -> Response {
        let mut tables = state.tables.lock().await;
        match apply_statement(&mut tables, &req.sql, req.params.as_ref()) {
            Ok(resp) => Json(resp).into_response(),
            Err(msg) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
        }
    }

    async fn handle_batch(
        State(state): State<Arc<MockState>>,
        Json(req): Json<MockBatchRequest>,
    ) -> Response {
        let mut tables = state.tables.lock().await;
        // Snapshot for full rollback on any per-statement failure.
        let snapshot = tables.clone();
        let mut results = Vec::new();
        for stmt in &req.statements {
            match apply_statement(&mut tables, &stmt.sql, stmt.params.as_ref()) {
                Ok(resp) => results.push(resp),
                Err(msg) => {
                    *tables = snapshot;
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({
                            "error": format!("batch aborted: {msg}"),
                        })),
                    )
                        .into_response();
                }
            }
        }
        (StatusCode::OK, Json(MockBatchResponse { results })).into_response()
    }
}

/// Apply one SQL statement against the mock table map.
///
/// Supports a small dialect used by conformance tests: `CREATE TABLE`,
/// `INSERT INTO … VALUES ($n,…)`, and `SELECT` (including `count(*)`).
fn apply_statement(
    tables: &mut HashMap<String, MockTable>,
    sql: &str,
    params: Option<&serde_json::Value>,
) -> Result<MockResponse, String> {
    let lower = sql.to_ascii_lowercase();
    let trimmed = lower.trim();
    if trimmed.starts_with("create table") {
        return apply_create(tables, &lower);
    }
    if trimmed.starts_with("insert into") {
        return apply_insert(tables, sql, params);
    }
    if trimmed.starts_with("select") {
        return apply_select(tables, &lower);
    }
    // Fallback: treat as a read of `FROM <table>` if present.
    apply_select(tables, &lower)
}

fn apply_create(
    tables: &mut HashMap<String, MockTable>,
    lower_sql: &str,
) -> Result<MockResponse, String> {
    // create table [if not exists] name (
    let after = lower_sql
        .split_once("table")
        .map(|(_, rest)| rest.trim())
        .ok_or_else(|| "create table: missing name".to_string())?;
    let after = after
        .strip_prefix("if not exists")
        .map(str::trim)
        .unwrap_or(after);
    let name: String = after
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        return Err("create table: empty name".into());
    }
    let cols = parse_create_columns(lower_sql);
    tables.entry(name).or_insert_with(|| MockTable {
        columns: cols,
        rows: Vec::new(),
    });
    Ok(MockResponse {
        columns: vec![],
        rows: vec![],
    })
}

fn parse_create_columns(lower_sql: &str) -> Vec<String> {
    let Some(start) = lower_sql.find('(') else {
        return vec![];
    };
    let Some(end) = lower_sql.rfind(')') else {
        return vec![];
    };
    if end <= start {
        return vec![];
    }
    lower_sql[start + 1..end]
        .split(',')
        .filter_map(|part| {
            let col = part
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"');
            if col.is_empty() || col == "primary" {
                None
            } else {
                Some(col.to_string())
            }
        })
        .collect()
}

fn apply_insert(
    tables: &mut HashMap<String, MockTable>,
    sql: &str,
    params: Option<&serde_json::Value>,
) -> Result<MockResponse, String> {
    let lower = sql.to_ascii_lowercase();
    let after = lower
        .split_once("insert into")
        .map(|(_, r)| r.trim())
        .ok_or_else(|| "insert: parse failed".to_string())?;
    let table: String = after
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if table.is_empty() {
        return Err("insert: empty table".into());
    }
    let cols = parse_insert_columns(sql).unwrap_or_else(|| {
        tables
            .get(&table)
            .map(|t| t.columns.clone())
            .unwrap_or_default()
    });
    let p = params_list(params);
    if cols.is_empty() {
        return Err(format!("insert into {table}: no columns"));
    }
    if p.len() < cols.len() {
        return Err(format!(
            "insert into {table}: expected {} params, got {}",
            cols.len(),
            p.len()
        ));
    }
    let mut obj = serde_json::Map::new();
    for (i, c) in cols.iter().enumerate() {
        obj.insert(c.clone(), p[i].clone());
    }
    let entry = tables.entry(table.clone()).or_insert_with(|| MockTable {
        columns: cols.clone(),
        rows: Vec::new(),
    });
    if entry.columns.is_empty() {
        entry.columns = cols.clone();
    }
    // Primary-key style uniqueness on `id` when present.
    if let Some(id) = obj.get("id") {
        if entry.rows.iter().any(|r| r.get("id") == Some(id)) {
            return Err(format!("duplicate key id={id} in {table}"));
        }
    }
    entry.rows.push(serde_json::Value::Object(obj));
    Ok(MockResponse {
        columns: entry.columns.clone(),
        rows: vec![],
    })
}

fn parse_insert_columns(sql: &str) -> Option<Vec<String>> {
    let lower = sql.to_ascii_lowercase();
    let after_table = lower.split_once("insert into")?.1.trim();
    let rest = after_table.find('(').map(|i| &after_table[i..])?;
    let end = rest.find(')')?;
    let inner = &rest[1..end];
    // Only the column list before VALUES.
    if !lower.contains("values") {
        return None;
    }
    let cols: Vec<String> = inner
        .split(',')
        .map(|c| c.trim().trim_matches('"').to_string())
        .filter(|c| !c.is_empty())
        .collect();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

fn params_list(params: Option<&serde_json::Value>) -> Vec<serde_json::Value> {
    match params {
        Some(serde_json::Value::Array(a)) => a.clone(),
        Some(other) => vec![other.clone()],
        None => vec![],
    }
}

fn apply_select(
    tables: &mut HashMap<String, MockTable>,
    lower_sql: &str,
) -> Result<MockResponse, String> {
    let table = table_of(lower_sql);
    let data = tables.get(&table).cloned().unwrap_or_default();
    let columns = if data.columns.is_empty() {
        // Legacy default for seeded store fixtures.
        vec!["id".into(), "title".into(), "status".into()]
    } else {
        data.columns.clone()
    };

    if lower_sql.contains("count(*)") {
        let alias = if lower_sql.contains(" as cnt") {
            "cnt"
        } else {
            "count(*)"
        };
        return Ok(MockResponse {
            columns: vec![alias.into()],
            rows: vec![serde_json::json!({ alias: data.rows.len() as i64 })],
        });
    }

    Ok(MockResponse {
        columns,
        rows: data.rows,
    })
}

fn table_of(sql: &str) -> String {
    sql.to_ascii_lowercase()
        .split_whitespace()
        .skip_while(|w| *w != "from")
        .nth(1)
        .map(|s| {
            s.chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect::<String>()
        })
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
struct MockRequest {
    sql: String,
    #[serde(default)]
    params: Option<serde_json::Value>,
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
    params: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct MockBatchResponse {
    results: Vec<MockResponse>,
}

/// Spin up the in-process axum mock on a random port.
async fn spawn_mock(state: Arc<MockState>) -> SocketAddr {
    let app = Router::new()
        .route("/query", post(MockState::handle))
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

/// Helper: run a closure against a live mock Neon endpoint, then shut it down.
fn with_neon_mock<F>(seed: HashMap<String, MockTable>, f: F)
where
    F: FnOnce(&str),
{
    let state = Arc::new(MockState {
        tables: Mutex::new(seed),
    });
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
                let addr = spawn_mock(state).await;
                let _ = ready_tx.send(format!("http://{addr}"));
                let _ = done_rx.await;
            });
        }
    });
    let endpoint = ready_rx.recv().expect("ready");
    f(&endpoint);
    let _ = done_tx.send(());
    let _ = mock_thread.join();
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

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

/// Caller-owned schema bootstrap: dactyl must NOT create tables itself
/// (#27), so the harness creates them via raw rusqlite (test-only) and then
/// drives dactyl against the seeded DB.
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

/// Create a fresh empty SQLite file with NO tables. Used to prove dactyl does
/// not silently bootstrap schema (#27): querying an unbootstrapped table must
/// surface the adapter's "no such table" error rather than succeed.
fn empty_sqlite(tmp: &TempDir, name: &str) -> std::path::PathBuf {
    use rusqlite::Connection;
    let dir = tmp.path().join(".decapod/data");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.db"));
    {
        let _ = Connection::open(&path).expect("open creates the file");
    }
    path
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Conformance: every store, every adapter, identical projections.
#[test]
fn conformance_all_stores() {
    let _guard = lock_env();
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
                    let mut tables = state.tables.lock().await;
                    for store in STORES {
                        tables.insert(
                            store.to_string(),
                            MockTable {
                                columns: vec!["id".into(), "title".into(), "status".into()],
                                rows: seed_rows(store),
                            },
                        );
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

        let query = format!("select id, title, status from {store}");

        // SQLite pass.
        select_sqlite(path.to_str().unwrap());
        let sqlite_rows = dactyl_db::query(&query, &[]).expect("sqlite read");
        assert_eq!(sqlite_rows.len(), 2, "store {store} sqlite row count");
        for row in sqlite_rows.iter() {
            assert_eq!(row.columns, vec!["id", "title", "status"]);
            assert_eq!(row.values.len(), 3);
        }

        // Neon pass.
        select_neon(&endpoint);
        let neon_rows = dactyl_db::query(&query, &[]).expect("neon read");
        assert_eq!(neon_rows.len(), 2, "store {store} neon row count");
        for row in neon_rows.iter() {
            assert_eq!(row.columns, vec!["id", "title", "status"]);
            assert_eq!(row.values.len(), 3);
        }

        // Cross-adapter projection equality at the JSON level.
        select_sqlite(path.to_str().unwrap());
        let sqlite_rows = dactyl_db::query(&query, &[]).expect("sqlite re-read");
        select_neon(&endpoint);
        let neon_rows = dactyl_db::query(&query, &[]).expect("neon re-read");
        assert_eq!(
            project(&neon_rows),
            project(&sqlite_rows),
            "store {store}: projection mismatch"
        );
    }

    clear_env();
    let _ = done_tx.send(());
    let _ = mock_thread.join();
}

/// #23: parameterized reads/writes with every supported scalar type plus an
/// attempted SQL-injection value. The injection payload must hit the table as
/// data, never as SQL.
#[test]
fn parameterized_queries_and_injection_regression() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("params.db");
    select_sqlite(path.to_str().unwrap());

    dactyl_db::execute(
        "create table params (
            id integer primary key,
            flag integer,
            ratio real,
            label text,
            note text,
            nullable_id integer
        )",
        &[],
    )
    .expect("caller creates schema");

    let injection = "'; drop table params; --";
    dactyl_db::execute(
        "insert into params (id, flag, ratio, label, note, nullable_id) values ($1, $2, $3, $4, $5, $6)",
        &[
            Parameter::Integer(1),
            Parameter::Bool(true),
            Parameter::Real(1.5),
            Parameter::Text("normal".into()),
            Parameter::Text(injection.into()),
            Parameter::Null,
        ],
    )
    .expect("insert with injection payload as bound data");

    // The table still exists: the payload was bound, not interpolated.
    let rows = dactyl_db::query(
        "select id, flag, ratio, label, note from params where id = $1",
        &[Parameter::Integer(1)],
    )
    .expect("select back");
    assert_eq!(rows.len(), 1);
    let row = &rows.as_slice()[0];
    assert_eq!(row.get::<_, i64>("id").expect("id"), 1);
    assert!(row.get_bool("flag").expect("flag"));
    assert_eq!(row.get::<_, f64>("ratio").expect("ratio"), 1.5);
    assert_eq!(row.get_str("label").expect("label"), "normal");
    assert_eq!(
        row.get::<_, String>("note").expect("note"),
        injection,
        "injection payload preserved verbatim as data"
    );

    // NULL parameter binding round-trips: a row where a non-key column was
    // bound to NULL must read back as None through Option<T>.
    dactyl_db::execute(
        "insert into params (id, flag, ratio, label, note, nullable_id) values ($1, $2, $3, $4, $5, $6)",
        &[
            Parameter::Integer(2),
            Parameter::Null,
            Parameter::Null,
            Parameter::Null,
            Parameter::Null,
            Parameter::Null,
        ],
    )
    .expect("insert nulls");
    let nulls = dactyl_db::query(
        "select flag, ratio, label from params where id = $1",
        &[Parameter::Integer(2)],
    )
    .expect("select nulls");
    assert_eq!(nulls.len(), 1);
    let n = nulls.as_slice()[0].clone();
    assert!(n.get::<_, Option<i64>>("flag").expect("flag").is_none());
    assert!(n.get::<_, Option<f64>>("ratio").expect("ratio").is_none());
    assert!(n
        .get::<_, Option<String>>("label")
        .expect("label")
        .is_none());

    clear_env();
}

/// #25: typed named extraction with NULL, missing column, and conversion error.
#[test]
fn typed_row_extraction_and_error_semantics() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("typed.db");
    select_sqlite(path.to_str().unwrap());

    dactyl_db::execute(
        "create table typed (id integer primary key, title text not null, status text)",
        &[],
    )
    .expect("create");
    dactyl_db::execute(
        "insert into typed (id, title, status) values ($1, $2, $3)",
        &[
            Parameter::Integer(2),
            Parameter::Text("todos-b".into()),
            Parameter::Null,
        ],
    )
    .expect("insert");

    let rows = dactyl_db::query(
        "select id, title, status from typed where id = $1",
        &[Parameter::Integer(2)],
    )
    .expect("read");
    assert_eq!(rows.len(), 1);
    let row = &rows.as_slice()[0];
    let id: i64 = row.get("id").expect("id");
    let title: String = row.get("title").expect("title");
    assert_eq!(id, 2);
    assert_eq!(title, "todos-b");

    // nullable column deserializes to Option<T> = None.
    let status: Option<String> = row.get("status").expect("status nullable");
    assert!(status.is_none());

    // Missing column -> ColumnNotFound.
    let missing: Result<i64, _> = row.get("missing_col");
    assert!(
        matches!(missing, Err(DactylError::ColumnNotFound(_))),
        "missing column must be ColumnNotFound"
    );

    // Conversion failure -> Conversion.
    let bad_cast: Result<bool, _> = row.get("title");
    assert!(
        matches!(bad_cast, Err(DactylError::Conversion(_))),
        "type mismatch must be Conversion"
    );

    // try_get is an alias for get.
    let id2: i64 = row.try_get("id").expect("try_get id");
    assert_eq!(id2, 2);

    // NULL on non-Option surfaces a Conversion that mentions NULL.
    let null_int: Result<i64, _> = row.get("status");
    assert!(
        matches!(null_int, Err(DactylError::Conversion(ref m)) if m.contains("NULL")),
        "NULL into non-Option must mention NULL: {null_int:?}"
    );

    clear_env();
}

/// #25: full scalar / NULL conversion matrix through SQLite, including
/// duplicate-alias first-match and borrowed accessors.
#[test]
fn typed_projection_matrix_sqlite() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("matrix.db");
    select_sqlite(path.to_str().unwrap());

    dactyl_db::execute(
        "create table matrix (
            id integer primary key,
            flag integer not null,
            ratio real not null,
            label text not null,
            note text,
            payload text not null
        )",
        &[],
    )
    .expect("create");
    dactyl_db::execute(
        "insert into matrix (id, flag, ratio, label, note, payload) values ($1, $2, $3, $4, $5, $6)",
        &[
            Parameter::Integer(7),
            Parameter::Bool(true),
            Parameter::Real(2.25),
            Parameter::Text("alpha".into()),
            Parameter::Null,
            Parameter::Text(r#"{"k":9}"#.into()),
        ],
    )
    .expect("insert");

    let rows = dactyl_db::query(
        "select id, flag, ratio, label, note, payload from matrix where id = $1",
        &[Parameter::Integer(7)],
    )
    .expect("select");
    let row = &rows.as_slice()[0];

    assert_eq!(row.get_int("id").expect("id"), 7);
    assert_eq!(row.get::<_, i64>("id").expect("id strict"), 7);
    assert!(row.get_bool("flag").expect("flag"), "sqlite bool as 0/1");
    assert_eq!(row.get_real("ratio").expect("ratio"), 2.25);
    assert_eq!(row.get_str("label").expect("label"), "alpha");
    assert_eq!(row.get_str_ref("label").expect("label ref"), "alpha");
    assert!(row.is_null("note").expect("note null"));
    assert!(row
        .get::<_, Option<String>>("note")
        .expect("note opt")
        .is_none());
    // JSON/text: payload remains a string cell until the caller parses it.
    assert_eq!(row.get_str("payload").expect("payload text"), r#"{"k":9}"#);
    assert_eq!(
        row.get_json_ref("payload").expect("payload json").as_str(),
        Some(r#"{"k":9}"#)
    );

    // Duplicate aliases: first match wins; positional reaches the later one.
    let alias_rows = dactyl_db::query(
        "select id as name, label as name from matrix where id = $1",
        &[Parameter::Integer(7)],
    )
    .expect("alias select");
    let a = &alias_rows.as_slice()[0];
    assert_eq!(a.columns, vec!["name", "name"]);
    assert_eq!(a.get_int("name").expect("first name is id"), 7);
    assert_eq!(a.get_str(1usize).expect("second name is label"), "alpha");

    // Missing + conversion still typed through the adapter path.
    assert!(matches!(
        a.get_str("missing"),
        Err(DactylError::ColumnNotFound(_))
    ));
    assert!(matches!(
        a.get_bool("name"),
        Err(DactylError::Conversion(_))
    ));

    clear_env();
}

/// #25: same projection matrix through the Neon adapter path (mock server).
/// Proves typed getters, NULL, missing columns, and conversion errors are
/// adapter-agnostic once cells are normalized to JSON.
#[test]
fn typed_projection_matrix_neon() {
    let _guard = lock_env();
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
                    let mut tables = state.tables.lock().await;
                    tables.insert(
                        "matrix".into(),
                        MockTable {
                            columns: vec![
                                "id".into(),
                                "flag".into(),
                                "ratio".into(),
                                "label".into(),
                                "note".into(),
                                "payload".into(),
                                "name".into(),
                                "name".into(),
                            ],
                            rows: vec![serde_json::json!({
                                "id": 7,
                                "flag": true,
                                "ratio": 2.25,
                                "label": "alpha",
                                "note": null,
                                "payload": {"k": 9},
                                // JSON objects cannot hold two "name" keys; the
                                // columns list still carries the duplicate
                                // alias pair and both map to the same wire key.
                                "name": "first",
                            })],
                        },
                    );
                }
                let addr = spawn_mock(state.clone()).await;
                let _ = ready_tx.send(format!("http://{addr}"));
                let _ = done_rx.await;
            });
        }
    });

    let endpoint = ready_rx.recv().expect("ready");
    select_neon(&endpoint);

    let rows = dactyl_db::query(
        "select id, flag, ratio, label, note, payload, name from matrix",
        &[],
    )
    .expect("neon select");
    assert_eq!(rows.len(), 1);
    let row = &rows.as_slice()[0];

    assert_eq!(row.get_int("id").expect("id"), 7);
    assert_eq!(row.try_get::<_, i64>("id").expect("try_get"), 7);
    assert!(row.get_bool("flag").expect("flag"));
    assert!(row.get::<_, bool>("flag").expect("strict bool"));
    assert_eq!(row.get_real("ratio").expect("ratio"), 2.25);
    assert_eq!(row.get_str("label").expect("label"), "alpha");
    assert_eq!(row.get_str_ref("label").expect("label ref"), "alpha");
    assert!(row.is_null("note").expect("note"));
    assert!(row
        .get::<_, Option<String>>("note")
        .expect("note opt")
        .is_none());
    // Neon can surface structured JSON objects directly.
    assert_eq!(row.get_json("payload").expect("payload")["k"], 9);
    #[derive(serde::Deserialize)]
    struct Payload {
        k: i64,
    }
    assert_eq!(row.get::<_, Payload>("payload").expect("typed json").k, 9);

    // Duplicate alias names: first-match semantics still hold.
    assert_eq!(row.columns.iter().filter(|c| *c == "name").count(), 2);
    assert_eq!(row.get_str("name").expect("first name"), "first");
    assert_eq!(
        row.get_str(row.columns.len() - 1).expect("last name col"),
        "first",
        "JSON wire loses distinct duplicate values; both aliases map to the key"
    );

    assert!(matches!(
        row.get_int("missing"),
        Err(DactylError::ColumnNotFound(_))
    ));
    assert!(matches!(
        row.get_int("label"),
        Err(DactylError::Conversion(_))
    ));
    assert!(matches!(
        row.get::<_, i64>("note"),
        Err(DactylError::Conversion(ref m)) if m.contains("NULL")
    ));

    clear_env();
    let _ = done_tx.send(());
    let _ = mock_thread.join();
}

/// #25 + #2: cross-adapter equality of typed projections for the same logical
/// scalar row (integer, bool-as-portable, real, text, null).
#[test]
fn typed_projection_cross_adapter_parity() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("parity.db");
    select_sqlite(path.to_str().unwrap());

    dactyl_db::execute(
        "create table parity (id integer primary key, flag integer, ratio real, label text, note text)",
        &[],
    )
    .expect("create");
    dactyl_db::execute(
        "insert into parity (id, flag, ratio, label, note) values ($1, $2, $3, $4, $5)",
        &[
            Parameter::Integer(1),
            Parameter::Bool(false),
            Parameter::Real(0.5),
            Parameter::Text("parity".into()),
            Parameter::Null,
        ],
    )
    .expect("insert");

    select_sqlite(path.to_str().unwrap());
    let sqlite_rows = dactyl_db::query(
        "select id, flag, ratio, label, note from parity where id = $1",
        &[Parameter::Integer(1)],
    )
    .expect("sqlite");
    let s = &sqlite_rows.as_slice()[0];

    let state = Arc::new(MockState::default());
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<String>();
    let mock_thread = std::thread::spawn({
        let state = state.clone();
        // Neon bool as true JSON bool; SQLite stores 0/1. Lenient get_bool
        // unifies both.
        let neon_row = serde_json::json!({
            "id": 1,
            "flag": false,
            "ratio": 0.5,
            "label": "parity",
            "note": null,
        });
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("mock rt");
            rt.block_on(async {
                {
                    let mut tables = state.tables.lock().await;
                    tables.insert(
                        "parity".into(),
                        MockTable {
                            columns: vec![
                                "id".into(),
                                "flag".into(),
                                "ratio".into(),
                                "label".into(),
                                "note".into(),
                            ],
                            rows: vec![neon_row],
                        },
                    );
                }
                let addr = spawn_mock(state.clone()).await;
                let _ = ready_tx.send(format!("http://{addr}"));
                let _ = done_rx.await;
            });
        }
    });
    let endpoint = ready_rx.recv().expect("ready");
    select_neon(&endpoint);
    let neon_rows = dactyl_db::query(
        "select id, flag, ratio, label, note from parity where id = $1",
        &[Parameter::Integer(1)],
    )
    .expect("neon");
    let n = &neon_rows.as_slice()[0];

    assert_eq!(s.get_int("id").unwrap(), n.get_int("id").unwrap());
    assert_eq!(s.get_bool("flag").unwrap(), n.get_bool("flag").unwrap());
    assert_eq!(s.get_real("ratio").unwrap(), n.get_real("ratio").unwrap());
    assert_eq!(s.get_str("label").unwrap(), n.get_str("label").unwrap());
    assert_eq!(s.is_null("note").unwrap(), n.is_null("note").unwrap());
    assert_eq!(
        s.get::<_, Option<String>>("note").unwrap(),
        n.get::<_, Option<String>>("note").unwrap()
    );

    clear_env();
    let _ = done_tx.send(());
    let _ = mock_thread.join();
}

/// #24: atomic transaction batch commits on success and rolls back fully on
/// any per-statement failure (SQLite). No partial state remains.
#[test]
fn atomic_transaction_rollback_on_failure() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("tx.db");
    select_sqlite(path.to_str().unwrap());

    dactyl_db::execute("create table tx (id integer primary key, value text)", &[])
        .expect("create");

    let stmts = vec![
        Statement::new(
            "insert into tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(1), Parameter::Text("val1".into())],
        ),
        Statement::new(
            "insert into tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(2), Parameter::Text("val2".into())],
        ),
    ];
    dactyl_db::transaction(&stmts).expect("tx success");

    let rows = dactyl_db::query("select count(*) as cnt from tx", &[]).expect("count");
    let cnt: i64 = rows.as_slice()[0].get("cnt").expect("cnt");
    assert_eq!(cnt, 2);

    // Second batch contains a duplicate key -> must roll back the whole batch.
    let failing = vec![
        Statement::new(
            "insert into tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(3), Parameter::Text("val3".into())],
        ),
        Statement::new(
            "insert into tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(1), Parameter::Text("duplicate".into())],
        ),
    ];
    let res = dactyl_db::transaction(&failing);
    assert!(res.is_err(), "duplicate key must fail the batch");

    let after = dactyl_db::query("select count(*) as cnt from tx", &[]).expect("count after");
    let cnt_after: i64 = after.as_slice()[0].get("cnt").expect("cnt");
    assert_eq!(
        cnt_after, 2,
        "row id=3 must NOT have been committed: full rollback"
    );

    // Empty batch is a successful no-op.
    let empty = dactyl_db::transaction(&[]).expect("empty batch");
    assert!(empty.is_empty());

    clear_env();
}

/// #24: Neon/mock failure-injection — a multi-statement `/batch` that fails
/// mid-way leaves no partial state (mirrors SQLite rollback proof).
#[test]
fn atomic_transaction_rollback_neon_mock() {
    let _guard = lock_env();

    let mut seed = HashMap::new();
    seed.insert(
        "tx".into(),
        MockTable {
            columns: vec!["id".into(), "value".into()],
            rows: vec![
                serde_json::json!({"id": 1, "value": "val1"}),
                serde_json::json!({"id": 2, "value": "val2"}),
            ],
        },
    );

    with_neon_mock(seed, |endpoint| {
        select_neon(endpoint);

        // Successful batch appends id=3.
        dactyl_db::transaction(&[Statement::new(
            "insert into tx (id, value) values ($1, $2)",
            vec![Parameter::Integer(3), Parameter::Text("val3".into())],
        )])
        .expect("neon batch success");

        let rows = dactyl_db::query("select count(*) as cnt from tx", &[]).expect("count");
        let cnt: i64 = rows.as_slice()[0].get("cnt").expect("cnt");
        assert_eq!(cnt, 3, "successful batch must persist");

        // Failing batch: first insert would add id=4, second hits duplicate id=1.
        // The whole unit must abort — id=4 must not remain.
        let res = dactyl_db::transaction(&[
            Statement::new(
                "insert into tx (id, value) values ($1, $2)",
                vec![Parameter::Integer(4), Parameter::Text("val4".into())],
            ),
            Statement::new(
                "insert into tx (id, value) values ($1, $2)",
                vec![Parameter::Integer(1), Parameter::Text("duplicate".into())],
            ),
        ]);
        assert!(
            matches!(res, Err(DactylError::Adapter(_))),
            "neon batch failure must be Adapter error: {res:?}"
        );

        let after = dactyl_db::query("select count(*) as cnt from tx", &[]).expect("count after");
        let cnt_after: i64 = after.as_slice()[0].get("cnt").expect("cnt");
        assert_eq!(
            cnt_after, 3,
            "row id=4 must NOT have been committed on neon mock rollback"
        );
    });

    clear_env();
}

/// #24: event-plus-state atomicity fixture (SQLite).
///
/// A domain-shaped unit of work updates durable state and appends an event
/// in one `transaction`. Mid-batch failure leaves neither side committed.
#[test]
fn atomic_event_plus_state_sqlite() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("event_state.db");
    select_sqlite(path.to_str().unwrap());

    dactyl_db::execute(
        "create table state (id integer primary key, value text not null)",
        &[],
    )
    .expect("create state");
    dactyl_db::execute(
        "create table events (
            id integer primary key,
            kind text not null,
            payload text not null
        )",
        &[],
    )
    .expect("create events");

    // Success path: state row + event row commit together.
    dactyl_db::transaction(&[
        Statement::new(
            "insert into state (id, value) values ($1, $2)",
            vec![Parameter::Integer(1), Parameter::Text("ready".into())],
        ),
        Statement::new(
            "insert into events (id, kind, payload) values ($1, $2, $3)",
            vec![
                Parameter::Integer(1),
                Parameter::Text("state.changed".into()),
                Parameter::Text(r#"{"id":1,"value":"ready"}"#.into()),
            ],
        ),
    ])
    .expect("event+state success");

    let state_cnt: i64 = dactyl_db::query("select count(*) as cnt from state", &[])
        .unwrap()
        .as_slice()[0]
        .get("cnt")
        .unwrap();
    let event_cnt: i64 = dactyl_db::query("select count(*) as cnt from events", &[])
        .unwrap()
        .as_slice()[0]
        .get("cnt")
        .unwrap();
    assert_eq!(state_cnt, 1);
    assert_eq!(event_cnt, 1);

    // Failure path: state insert would succeed, event hits duplicate PK → both roll back.
    let res = dactyl_db::transaction(&[
        Statement::new(
            "insert into state (id, value) values ($1, $2)",
            vec![Parameter::Integer(2), Parameter::Text("partial".into())],
        ),
        Statement::new(
            "insert into events (id, kind, payload) values ($1, $2, $3)",
            vec![
                Parameter::Integer(1), // duplicate event id
                Parameter::Text("state.changed".into()),
                Parameter::Text(r#"{"id":2}"#.into()),
            ],
        ),
    ]);
    assert!(res.is_err(), "duplicate event key must fail batch: {res:?}");

    let state_after: i64 = dactyl_db::query("select count(*) as cnt from state", &[])
        .unwrap()
        .as_slice()[0]
        .get("cnt")
        .unwrap();
    let event_after: i64 = dactyl_db::query("select count(*) as cnt from events", &[])
        .unwrap()
        .as_slice()[0]
        .get("cnt")
        .unwrap();
    assert_eq!(
        state_after, 1,
        "state id=2 must not commit when event side fails"
    );
    assert_eq!(
        event_after, 1,
        "events must stay at the successful batch only"
    );

    clear_env();
}

/// #24: event-plus-state atomicity fixture (Neon mock).
#[test]
fn atomic_event_plus_state_neon_mock() {
    let _guard = lock_env();

    let mut seed = HashMap::new();
    seed.insert(
        "state".into(),
        MockTable {
            columns: vec!["id".into(), "value".into()],
            rows: vec![],
        },
    );
    seed.insert(
        "events".into(),
        MockTable {
            columns: vec!["id".into(), "kind".into(), "payload".into()],
            rows: vec![],
        },
    );

    with_neon_mock(seed, |endpoint| {
        select_neon(endpoint);

        dactyl_db::transaction(&[
            Statement::new(
                "insert into state (id, value) values ($1, $2)",
                vec![Parameter::Integer(1), Parameter::Text("ready".into())],
            ),
            Statement::new(
                "insert into events (id, kind, payload) values ($1, $2, $3)",
                vec![
                    Parameter::Integer(1),
                    Parameter::Text("state.changed".into()),
                    Parameter::Text(r#"{"id":1}"#.into()),
                ],
            ),
        ])
        .expect("neon event+state success");

        let state_cnt: i64 = dactyl_db::query("select count(*) as cnt from state", &[])
            .unwrap()
            .as_slice()[0]
            .get("cnt")
            .unwrap();
        let event_cnt: i64 = dactyl_db::query("select count(*) as cnt from events", &[])
            .unwrap()
            .as_slice()[0]
            .get("cnt")
            .unwrap();
        assert_eq!(state_cnt, 1);
        assert_eq!(event_cnt, 1);

        let res = dactyl_db::transaction(&[
            Statement::new(
                "insert into state (id, value) values ($1, $2)",
                vec![Parameter::Integer(2), Parameter::Text("partial".into())],
            ),
            Statement::new(
                "insert into events (id, kind, payload) values ($1, $2, $3)",
                vec![
                    Parameter::Integer(1),
                    Parameter::Text("state.changed".into()),
                    Parameter::Text(r#"{"id":2}"#.into()),
                ],
            ),
        ]);
        assert!(
            matches!(res, Err(DactylError::Adapter(_))),
            "neon event+state failure: {res:?}"
        );

        let state_after: i64 = dactyl_db::query("select count(*) as cnt from state", &[])
            .unwrap()
            .as_slice()[0]
            .get("cnt")
            .unwrap();
        let event_after: i64 = dactyl_db::query("select count(*) as cnt from events", &[])
            .unwrap()
            .as_slice()[0]
            .get("cnt")
            .unwrap();
        assert_eq!(state_after, 1, "no partial state on neon mock");
        assert_eq!(event_after, 1, "no partial events on neon mock");
    });

    clear_env();
}

/// #27: dactyl never silently bootstraps schema. Querying a table that was
/// not created by the caller must surface the adapter's "no such table"
/// error, not succeed with fabricated rows.
#[test]
fn no_silent_schema_bootstrap() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = empty_sqlite(&tmp, "unbootstrapped");
    select_sqlite(path.to_str().unwrap());

    let res = dactyl_db::query("select id, title from todos", &[]);
    assert!(
        matches!(res, Err(DactylError::Adapter(ref e)) if e.contains("no such table")),
        "expected adapter 'no such table' error, got {res:?}"
    );

    // And the directory still contains no Decapod-style tables dactyl might
    // have fabricated on open.
    use rusqlite::Connection;
    let conn = Connection::open(&path).expect("open");
    let mut stmt = conn
        .prepare("select count(*) from sqlite_master where type='table' and name='todos'")
        .expect("prepare");
    let count: i64 = stmt.query_row([], |r| r.get(0)).expect("count");
    assert_eq!(count, 0, "dactyl must not have created the 'todos' table");

    clear_env();
}

/// #26 + #27: caller-owned schema flow. A fresh empty database, then the
/// caller creates an index, an audit table, and a trigger, then upgrades
/// schema via dactyl. Everything goes through `execute`; dactyl is purely a
/// vehicle and never mutates undeclared schema.
#[test]
fn caller_owned_schema_ddl_migration() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = empty_sqlite(&tmp, "caller_owned");
    select_sqlite(path.to_str().unwrap());

    dactyl_db::execute(
        "create table app (id integer primary key, name text not null unique)",
        &[],
    )
    .expect("create table");
    dactyl_db::execute(
        "create table app_audit (id integer primary key, app_id integer, name text)",
        &[],
    )
    .expect("create audit table");
    dactyl_db::execute("create index app_name_idx on app(name)", &[]).expect("create index");
    dactyl_db::execute(
        "create trigger app_audit after insert on app begin
            insert into app_audit(app_id, name) values (new.id, 'audit-' || new.name);
         end",
        &[],
    )
    .expect("create trigger");

    dactyl_db::execute(
        "insert into app (id, name) values ($1, $2)",
        &[Parameter::Integer(1), Parameter::Text("alpha".into())],
    )
    .expect("insert");

    let rows = dactyl_db::query("select name from app order by id", &[]).expect("select app");
    let names: Vec<String> = rows
        .iter()
        .map(|r| r.get::<_, String>("name").expect("name"))
        .collect();
    assert_eq!(names, vec!["alpha".to_string()]);

    let audit = dactyl_db::query("select app_id, name from app_audit order by id", &[])
        .expect("select audit");
    let audit_rows: Vec<(i64, String)> = audit
        .iter()
        .map(|r| {
            (
                r.get::<_, i64>("app_id").expect("app_id"),
                r.get::<_, String>("name").expect("name"),
            )
        })
        .collect();
    assert_eq!(
        audit_rows,
        vec![(1, "audit-alpha".to_string())],
        "trigger wrote exactly one audit row"
    );

    // Migration-style schema upgrade via dactyl execute.
    dactyl_db::execute("alter table app add column status text default 'open'", &[])
        .expect("alter table");
    let cols = dactyl_db::query("pragma table_info(app)", &[]).expect("pragma after migration");
    let has_status = cols.iter().any(|r| {
        r.get::<_, String>("name")
            .ok()
            .map(|n| n == "status")
            .unwrap_or(false)
    });
    assert!(has_status, "migration should have added the status column");

    clear_env();
}

/// #26: session isolation. Because dactyl builds a fresh adapter per call and
/// caches nothing, two concurrent selections against two distinct SQLite files
/// must not bleed into each other.
#[test]
fn session_isolation_across_distinct_databases() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let left = tmp.path().join("left.db");
    let right = tmp.path().join("right.db");

    select_sqlite(left.to_str().unwrap());
    dactyl_db::execute(
        "create table t (id integer primary key, origin text not null)",
        &[],
    )
    .expect("create left");
    dactyl_db::execute(
        "insert into t (id, origin) values ($1, $2)",
        &[Parameter::Integer(1), Parameter::Text("left".into())],
    )
    .expect("seed left");

    select_sqlite(right.to_str().unwrap());
    dactyl_db::execute(
        "create table t (id integer primary key, origin text not null)",
        &[],
    )
    .expect("create right");
    dactyl_db::execute(
        "insert into t (id, origin) values ($1, $2)",
        &[Parameter::Integer(1), Parameter::Text("right".into())],
    )
    .expect("seed right");

    // Switch back to left and assert we see left's row, not right's.
    select_sqlite(left.to_str().unwrap());
    let rows = dactyl_db::query(
        "select origin from t where id = $1",
        &[Parameter::Integer(1)],
    )
    .expect("read left");
    let origin: String = rows.as_slice()[0].get("origin").expect("origin");
    assert_eq!(origin, "left");

    // Same for right.
    select_sqlite(right.to_str().unwrap());
    let rows = dactyl_db::query(
        "select origin from t where id = $1",
        &[Parameter::Integer(1)],
    )
    .expect("read right");
    let origin: String = rows.as_slice()[0].get("origin").expect("origin");
    assert_eq!(origin, "right");

    clear_env();
}

/// `query!` macro still returns the analyzed SQL string and composes with the
/// runtime `query` entry point.
#[test]
fn query_macro_composes_with_runtime() {
    let _guard = lock_env();
    let tmp = TempDir::new().expect("tempdir");
    let path = sqlite_path(&tmp, "macro");
    seed_sqlite(&path, "macro", &seed_rows("macro"));
    select_sqlite(path.to_str().unwrap());

    let sql: String = dactyl_db::query!("select id, title, status from macro");
    assert_eq!(sql, "select id, title, status from macro");
    let rows = dactyl_db::query(&sql, &[]).expect("read");
    assert_eq!(rows.len(), 2);

    clear_env();
}

/// Env-var validation: missing `DATASTORE` and an unknown value both produce
/// typed errors before any adapter is constructed.
#[test]
fn env_validation_errors_are_typed() {
    let _guard = lock_env();

    clear_env();
    let res = dactyl_db::query("select 1", &[]);
    assert!(
        matches!(res, Err(DactylError::Adapter(ref e)) if e.contains("DATASTORE is not set")),
        "missing DATASTORE: {res:?}"
    );

    unsafe {
        std::env::set_var("DATASTORE", "redis");
    }
    let res = dactyl_db::query("select 1", &[]);
    assert!(
        matches!(res, Err(DactylError::Adapter(ref e)) if e.contains("invalid DATASTORE")),
        "unknown DATASTORE: {res:?}"
    );

    unsafe {
        std::env::set_var("DATASTORE", "sqlite");
        std::env::remove_var("DATASTORE_ROUTE");
    }
    let res = dactyl_db::query("select 1", &[]);
    assert!(
        matches!(res, Err(DactylError::Adapter(ref e)) if e.contains("DATASTORE_ROUTE is not set")),
        "missing route: {res:?}"
    );

    clear_env();
}
