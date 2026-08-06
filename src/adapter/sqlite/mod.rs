//! SQLite adapter.
//!
//! Holds a `rusqlite::Connection` opened against the configured path. The
//! adapter is constructed per public [`crate::Connection`] and lives for that
//! connection's duration. There is no process-wide connection cache.
//!
//! Schemas are managed by the caller — [`SqliteAdapter::open`] opens (or
//! creates) the file but never bootstraps any tables. Callers own and version
//! their schema through explicit [`crate::execute`] / DDL statements
//! (dactyl #27).

use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{params_from_iter, types::Value as SqlValue, Connection, OpenFlags};

use crate::adapter::Adapter;
use crate::error::DactylError;
use crate::rows::{Parameter, Row, Rows};
use crate::SqliteJournalMode;
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
    /// Open SQLite with the connection policy supplied by the public dactyl
    /// connection boundary.
    pub fn open_with_options(
        path: &str,
        read_only: bool,
        busy_timeout: Duration,
        foreign_keys: bool,
        journal_mode: Option<SqliteJournalMode>,
    ) -> rusqlite::Result<Self> {
        if !read_only && path != ":memory:" {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                }
            }
        }
        let conn = if read_only {
            Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?
        } else {
            Connection::open(path)?
        };
        conn.busy_timeout(busy_timeout)?;
        if foreign_keys {
            conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        }
        if !read_only {
            if let Some(mode) = journal_mode {
                let requested = format!("PRAGMA journal_mode={};", mode.as_sql());
                if conn.query_row(&requested, [], |_| Ok(())).is_err()
                    && mode == SqliteJournalMode::Wal
                {
                    conn.query_row("PRAGMA journal_mode=DELETE;", [], |_| Ok(()))?;
                }
            }
        }
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

    fn execute_script(&self, query: &str) -> Result<(), DactylError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DactylError::Adapter(format!("sqlite lock poisoned: {e}")))?;
        conn.execute_batch(query)
            .map_err(|e| DactylError::Adapter(format!("sqlite execute script: {e}")))
    }

    fn last_insert_id(&self) -> Result<i64, DactylError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DactylError::Adapter(format!("sqlite lock poisoned: {e}")))?;
        Ok(conn.last_insert_rowid())
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
            Parameter::Blob(b) => SqlValue::Blob(b.clone()),
        })
        .collect()
}

fn translate_placeholders_to_sqlite(query: &str) -> String {
    let mut out = String::new();
    let bytes = query.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;

        // A PostgreSQL-style placeholder is data when it appears inside a
        // string or comment. Keep those regions byte-for-byte intact.
        if c == '\'' || c == '"' {
            let quote = bytes[i];
            out.push(c);
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i] as char);
                if bytes[i] == quote {
                    if i + 1 < bytes.len() && bytes[i + 1] == quote {
                        out.push(bytes[i + 1] as char);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() {
                let ch = bytes[i] as char;
                out.push(ch);
                i += 1;
                if ch == '\n' {
                    break;
                }
            }
            continue;
        }
        if c == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            i += 2;
            while i < bytes.len() {
                out.push(bytes[i] as char);
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    out.push('/');
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }

        if c == '$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            out.push('?');
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            continue;
        }

        out.push(c);
        i += 1;
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
        SqlValue::Blob(b) => serde_json::Value::Array(
            b.into_iter()
                .map(|byte| serde_json::Value::Number((byte as u64).into()))
                .collect(),
        ),
    }
}
