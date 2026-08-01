//! SQLite adapter.
//!
//! Holds a `rusqlite::Connection` opened against the configured path. The
//! adapter is constructed per call from [`crate::build_adapter`] and lives
//! for the duration of that call, so there is no shared connection cache and
//! the connection is never mutated across threads.
//!
//! Schemas are managed by the caller — [`SqliteAdapter::open`] opens (or
//! creates) the file but never bootstraps any tables. Callers own and version
//! their schema through explicit [`crate::execute`] / DDL statements
//! (dactyl #27).

use std::sync::Mutex;

use rusqlite::{params_from_iter, types::Value as SqlValue, Connection};

use crate::adapter::Adapter;
use crate::error::DactylError;
use crate::rows::{Parameter, Row, Rows};
use crate::Statement;

/// Opaque handle to the SQLite adapter.
///
/// `rusqlite::Connection` is `!Sync`, so execution is serialized through an
/// internal `Mutex`. The mutex is harmless in practice because each call
/// constructs and drops its own adapter, but it keeps the type `Send + Sync`
/// for callers that choose to hold an adapter longer.
pub struct SqliteAdapter {
    conn: Mutex<Connection>,
}

impl SqliteAdapter {
    /// Open (or create) the SQLite file at `path`. No tables are created.
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl Adapter for SqliteAdapter {
    fn execute(&self, query: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DactylError::Adapter(format!("sqlite lock poisoned: {e}")))?;
        let translated_sql = translate_placeholders_to_sqlite(query);
        let mapped = map_params(params);
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = mapped
            .iter()
            .map(|x| x as &dyn rusqlite::types::ToSql)
            .collect();
        let mut stmt = conn
            .prepare(&translated_sql)
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

    fn execute_raw(&self, query: &str, params: &[Parameter]) -> Result<u64, DactylError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DactylError::Adapter(format!("sqlite lock poisoned: {e}")))?;
        let translated_sql = translate_placeholders_to_sqlite(query);
        let mapped = map_params(params);
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = mapped
            .iter()
            .map(|x| x as &dyn rusqlite::types::ToSql)
            .collect();
        let affected = conn
            .execute(
                &translated_sql,
                params_from_iter(param_refs.iter().copied()),
            )
            .map_err(|e| DactylError::Adapter(format!("sqlite execute_raw: {e}")))?;
        Ok(affected as u64)
    }

    fn execute_batch(&self, statements: &[Statement]) -> Result<Vec<Rows>, DactylError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| DactylError::Adapter(format!("sqlite lock poisoned: {e}")))?;
        let tx = conn
            .transaction()
            .map_err(|e| DactylError::Adapter(format!("sqlite transaction begin: {e}")))?;

        let mut results = Vec::with_capacity(statements.len());
        for stmt_info in statements {
            let translated_sql = translate_placeholders_to_sqlite(&stmt_info.sql);
            let mapped = map_params(&stmt_info.params);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> = mapped
                .iter()
                .map(|x| x as &dyn rusqlite::types::ToSql)
                .collect();
            let mut stmt = tx
                .prepare(&translated_sql)
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
            results.push(Rows(rows));
        }
        tx.commit()
            .map_err(|e| DactylError::Adapter(format!("sqlite transaction commit: {e}")))?;
        Ok(results)
    }
}

fn map_params(params: &[Parameter]) -> Vec<SqlValue> {
    params
        .iter()
        .map(|p| match p {
            Parameter::Null => SqlValue::Null,
            Parameter::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
            Parameter::Integer(i) => SqlValue::Integer(*i),
            Parameter::Real(f) => SqlValue::Real(*f),
            Parameter::Text(s) => SqlValue::Text(s.clone()),
        })
        .collect()
}

fn translate_placeholders_to_sqlite(query: &str) -> String {
    let mut out = String::new();
    let mut chars = query.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' {
            if let Some(next_c) = chars.peek() {
                if next_c.is_ascii_digit() {
                    out.push('?');
                    continue;
                }
            }
        }
        out.push(c);
    }
    out
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
