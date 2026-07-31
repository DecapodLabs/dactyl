//! SQLite adapter.
//!
//! Holds a `rusqlite::Connection` opened against the configured path and
//! bootstraps the 9 store tables on first connect.

mod schema;

use std::sync::{Arc, Mutex};

use rusqlite::{params_from_iter, types::Value as SqlValue, Connection};

use crate::adapter::Adapter;
use crate::error::DactylError;
use crate::rows::{Row, Rows};

/// Opaque handle to the SQLite adapter.
///
/// The handle is intentionally `Arc<Inner>` so the adapter can be cloned into
/// the global registry without taking ownership of the caller's connection.
/// `rusqlite::Connection` is `!Sync`, so we serialize execution through a
/// `Mutex`.
#[derive(Clone)]
pub struct SqliteAdapter {
    inner: Arc<Inner>,
}

struct Inner {
    conn: Mutex<Connection>,
}

impl SqliteAdapter {
    /// Open (or create) the SQLite file at `path` and bootstrap the schema.
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        schema::bootstrap(&conn)?;
        Ok(Self {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
            }),
        })
    }
}

impl Adapter for SqliteAdapter {
    fn execute(
        &self,
        query: &str,
        params: Option<&serde_json::Value>,
        _optimize: bool,
        _write: bool,
    ) -> Result<Rows, DactylError> {
        let conn = self
            .inner
            .conn
            .lock()
            .map_err(|e| DactylError::Adapter(format!("sqlite lock poisoned: {e}")))?;
        let mapped = map_params(params);
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = mapped
            .as_deref()
            .map(|v| v.iter().map(|x| x as &dyn rusqlite::types::ToSql).collect())
            .unwrap_or_default();
        let mut stmt = conn
            .prepare(query)
            .map_err(|e| DactylError::Adapter(format!("sqlite prepare: {e}")))?;

        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("").to_string())
            .collect();

        let rows_iter = stmt
            .query_map(params_from_iter(param_refs.iter().copied()), |row| {
                let mut cells = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let v = row.get::<_, SqlValue>(i)?;
                    cells.push(sql_to_json(v));
                }
                Ok(cells)
            })
            .map_err(|e| DactylError::Adapter(format!("sqlite query: {e}")))?;

        let mut rows = Vec::new();
        for row in rows_iter {
            let cells = row.map_err(|e| DactylError::Adapter(format!("sqlite row: {e}")))?;
            rows.push(Row {
                columns: column_names.clone(),
                values: cells,
            });
        }
        Ok(Rows(rows))
    }
}

fn map_params(params: Option<&serde_json::Value>) -> Option<Vec<SqlValue>> {
    let v = params?;
    let arr = v.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        out.push(json_to_sql(item));
    }
    Some(out)
}

fn json_to_sql(v: &serde_json::Value) -> SqlValue {
    match v {
        serde_json::Value::Null => SqlValue::Null,
        serde_json::Value::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                SqlValue::Real(f)
            } else {
                SqlValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => SqlValue::Text(s.clone()),
        other => SqlValue::Text(other.to_string()),
    }
}

fn sql_to_json(v: SqlValue) -> serde_json::Value {
    match v {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(i) => serde_json::Value::Number(i.into()),
        SqlValue::Real(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        SqlValue::Text(s) => serde_json::Value::String(s),
        SqlValue::Blob(b) => serde_json::Value::String(format!("<blob {} bytes>", b.len())),
    }
}
