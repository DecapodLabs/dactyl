//! SQLite-backed local storage.
//!
//! Dactyl owns the narrow driver boundary around this connection: route
//! selection, parameter binding, atomic batches, access mode, row/result
//! normalization, and stable error categories. SQLite owns file compatibility,
//! SQL execution, locking, journaling, and constraint enforcement.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use rusqlite::types::{Value, ValueRef};
use rusqlite::{params_from_iter, Connection as SqliteConnection, Error as SqliteError};
use rusqlite::{ErrorCode, OpenFlags};

use crate::adapter::Adapter;
use crate::contract::{
    AccessMode, AtomicResult, GeneratedKey, OpenOptions, Operation, OperationKind, OperationResult,
    WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Row, Rows};
use crate::schema::{
    ColumnSchema, ForeignKeyAction, ForeignKeySchema, IndexSchema, StoreSchema, TableSchema,
};

const SCHEMA_DESCRIPTION_VERSION: u32 = 1;

pub struct SqliteAdapter {
    connection: Mutex<SqliteConnection>,
    options: OpenOptions,
}

impl SqliteAdapter {
    pub fn open_with_options(path: &str, options: OpenOptions) -> Result<Self, DactylError> {
        if path != ":memory:" {
            let path_ref = Path::new(path);
            if options.access_mode == AccessMode::ReadOnly && !path_ref.exists() {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::NotFound,
                    "missing_database",
                    format!("SQLite database does not exist: {path}"),
                ));
            }
            if options.access_mode == AccessMode::ReadWrite {
                if let Some(parent) = path_ref
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                {
                    fs::create_dir_all(parent).map_err(|error| {
                        DactylError::adapter_with_code(
                            AdapterErrorKind::Storage,
                            "create_parent_failed",
                            format!("create SQLite parent directory: {error}"),
                        )
                    })?;
                }
            }
        }

        let connection = if path == ":memory:" {
            SqliteConnection::open_in_memory()
        } else {
            let flags = match options.access_mode {
                AccessMode::ReadWrite => {
                    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
                }
                AccessMode::ReadOnly => OpenFlags::SQLITE_OPEN_READ_ONLY,
            };
            SqliteConnection::open_with_flags(path, flags)
        }
        .map_err(|error| sqlite_error("open SQLite database", error))?;

        connection
            .busy_timeout(options.lock_timeout)
            .map_err(|error| sqlite_error("configure SQLite busy timeout", error))?;
        connection
            .execute_batch("PRAGMA foreign_keys = ON")
            .map_err(|error| sqlite_error("enable SQLite foreign keys", error))?;

        Ok(Self {
            connection: Mutex::new(connection),
            options,
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, SqliteConnection>, DactylError> {
        self.connection.lock().map_err(|_| {
            DactylError::adapter(AdapterErrorKind::Storage, "SQLite connection lock poisoned")
        })
    }
}

impl Adapter for SqliteAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let connection = self.connection()?;
        query_rows(&connection, sql, params)
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        ensure_writable(self.options.access_mode)?;
        let connection = self.connection()?;
        execute_write(&connection, sql, params)
    }

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        if operations.is_empty() {
            return Ok(AtomicResult::default());
        }

        let mutates = operations
            .iter()
            .any(|operation| operation.kind() != OperationKind::Read);
        if mutates {
            ensure_writable(self.options.access_mode)?;
        }

        let connection = self.connection()?;
        if mutates {
            connection
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(|error| sqlite_error("begin SQLite transaction", error))?;
        }

        let result = (|| {
            let mut results = Vec::with_capacity(operations.len());
            for operation in operations {
                results.push(execute_operation(&connection, operation)?);
            }
            Ok(AtomicResult { results })
        })();

        if !mutates {
            return result;
        }

        match result {
            Ok(value) => {
                if let Err(error) = connection.execute_batch("COMMIT") {
                    let _ = connection.execute_batch("ROLLBACK");
                    Err(sqlite_error("commit SQLite transaction", error))
                } else {
                    Ok(value)
                }
            }
            Err(error) => {
                let _ = connection.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn access_mode(&self) -> AccessMode {
        self.options.access_mode
    }

    fn inspect_schema(&self) -> Result<StoreSchema, DactylError> {
        let connection = self.connection()?;
        inspect_schema(&connection)
    }
}

fn execute_operation(
    connection: &SqliteConnection,
    operation: &Operation,
) -> Result<OperationResult, DactylError> {
    let first = first_word(operation.sql());
    match operation.kind() {
        OperationKind::Read => {
            if !is_query(first.as_deref()) {
                return Err(adapter_error(
                    AdapterErrorKind::InvalidOperation,
                    "read operation requires a query statement",
                ));
            }
            Ok(OperationResult::Rows(query_rows(
                connection,
                operation.sql(),
                operation.params(),
            )?))
        }
        OperationKind::Write => Ok(OperationResult::Write(execute_write(
            connection,
            operation.sql(),
            operation.params(),
        )?)),
        OperationKind::Schema => {
            if !matches!(first.as_deref(), Some("create" | "alter" | "drop")) {
                return Err(adapter_error(
                    AdapterErrorKind::InvalidOperation,
                    "schema operation requires CREATE, ALTER, or DROP SQL",
                ));
            }
            if has_multiple_statements(operation.sql()) {
                if !operation.params().is_empty() {
                    return Err(adapter_error(
                        AdapterErrorKind::Capability,
                        "multi-statement schema SQL cannot bind parameters",
                    ));
                }
                connection
                    .execute_batch(operation.sql())
                    .map_err(|error| sqlite_error("execute SQLite schema batch", error))?;
                Ok(OperationResult::Write(WriteResult::default()))
            } else {
                Ok(OperationResult::Write(execute_write(
                    connection,
                    operation.sql(),
                    operation.params(),
                )?))
            }
        }
    }
}

fn execute_write(
    connection: &SqliteConnection,
    sql: &str,
    params: &[Parameter],
) -> Result<WriteResult, DactylError> {
    let values = sqlite_values(params);
    connection
        .execute(sql, params_from_iter(values.iter()))
        .map_err(|error| sqlite_error("execute SQLite write", error))?;
    let generated_keys = if first_word(sql).as_deref() == Some("insert") {
        let rowid = connection.last_insert_rowid();
        if rowid != 0 {
            vec![GeneratedKey::Integer(rowid)]
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    Ok(WriteResult {
        affected_rows: connection.changes(),
        generated_keys,
    })
}

fn query_rows(
    connection: &SqliteConnection,
    sql: &str,
    params: &[Parameter],
) -> Result<Rows, DactylError> {
    let values = sqlite_values(params);
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| sqlite_error("prepare SQLite query", error))?;
    let columns = statement
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let column_count = statement.column_count();
    let mut result_rows = Vec::new();
    let mut query = statement
        .query(params_from_iter(values.iter()))
        .map_err(|error| sqlite_error("run SQLite query", error))?;
    while let Some(row) = query
        .next()
        .map_err(|error| sqlite_error("read SQLite row", error))?
    {
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            values.push(value_to_json(
                row.get_ref(index)
                    .map_err(|error| sqlite_error("read SQLite value", error))?,
            )?);
        }
        result_rows.push(Row {
            columns: columns.clone(),
            values,
        });
    }
    Ok(Rows(result_rows))
}

fn sqlite_values(params: &[Parameter]) -> Vec<Value> {
    params
        .iter()
        .map(|parameter| match parameter {
            Parameter::Null => Value::Null,
            Parameter::Bool(value) => Value::Integer(i64::from(*value)),
            Parameter::Integer(value) => Value::Integer(*value),
            Parameter::Real(value) => Value::Real(*value),
            Parameter::Text(value) => Value::Text(value.clone()),
            Parameter::Blob(value) => Value::Blob(value.clone()),
        })
        .collect()
}

fn value_to_json(value: ValueRef<'_>) -> Result<serde_json::Value, DactylError> {
    match value {
        ValueRef::Null => Ok(serde_json::Value::Null),
        ValueRef::Integer(value) => Ok(serde_json::Value::Number(value.into())),
        ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                adapter_error(AdapterErrorKind::Value, "SQLite returned a non-finite REAL")
            }),
        ValueRef::Text(value) => String::from_utf8(value.to_vec())
            .map(serde_json::Value::String)
            .map_err(|error| {
                adapter_error(
                    AdapterErrorKind::Value,
                    format!("SQLite returned invalid UTF-8 text: {error}"),
                )
            }),
        ValueRef::Blob(value) => Ok(serde_json::Value::Array(
            value
                .iter()
                .map(|byte| serde_json::Value::Number(u64::from(*byte).into()))
                .collect(),
        )),
    }
}

fn inspect_schema(connection: &SqliteConnection) -> Result<StoreSchema, DactylError> {
    let mut table_statement = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .map_err(|error| sqlite_error("prepare SQLite table catalog", error))?;
    let table_names = table_statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| sqlite_error("read SQLite table catalog", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sqlite_error("decode SQLite table catalog", error))?;
    drop(table_statement);

    let mut tables = Vec::with_capacity(table_names.len());
    let mut indexes = Vec::new();
    for table_name in table_names {
        let table_indexes = indexes_for_table(connection, &table_name)?;
        let unique_columns = table_indexes
            .iter()
            .filter(|index| index.unique && index.columns.len() == 1)
            .flat_map(|index| index.columns.first().cloned())
            .collect::<Vec<_>>();
        let columns = columns_for_table(connection, &table_name, &unique_columns)?;
        let foreign_keys = foreign_keys_for_table(connection, &table_name)?;
        let count_sql = format!("SELECT COUNT(*) FROM {}", quote_identifier(&table_name));
        let row_count = connection
            .query_row(&count_sql, [], |row| row.get::<_, i64>(0))
            .map_err(|error| sqlite_error("count SQLite table rows", error))?;
        tables.push(TableSchema {
            name: table_name.clone(),
            columns,
            unique_constraints: table_indexes
                .iter()
                .filter(|index| index.unique)
                .map(|index| index.columns.clone())
                .collect(),
            foreign_keys,
            row_count: u64::try_from(row_count).unwrap_or_default(),
        });
        indexes.extend(table_indexes.into_iter().map(|index| IndexSchema {
            name: index.name,
            table: table_name.clone(),
            columns: index.columns,
            unique: index.unique,
        }));
    }
    indexes.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(StoreSchema {
        format_version: SCHEMA_DESCRIPTION_VERSION,
        tables,
        indexes,
    })
}

#[derive(Debug)]
struct IndexInfo {
    name: String,
    unique: bool,
    columns: Vec<String>,
}

fn indexes_for_table(
    connection: &SqliteConnection,
    table_name: &str,
) -> Result<Vec<IndexInfo>, DactylError> {
    let sql = format!("PRAGMA index_list({})", quote_identifier(table_name));
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| sqlite_error("prepare SQLite index catalog", error))?;
    let names = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(2)? != 0))
        })
        .map_err(|error| sqlite_error("read SQLite index catalog", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sqlite_error("decode SQLite index catalog", error))?;
    drop(statement);

    let mut indexes = Vec::with_capacity(names.len());
    for (name, unique) in names {
        let sql = format!("PRAGMA index_info({})", quote_identifier(&name));
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| sqlite_error("prepare SQLite index columns", error))?;
        let columns = statement
            .query_map([], |row| row.get::<_, Option<String>>(2))
            .map_err(|error| sqlite_error("read SQLite index columns", error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| sqlite_error("decode SQLite index columns", error))?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        indexes.push(IndexInfo {
            name,
            unique,
            columns,
        });
    }
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(indexes)
}

fn columns_for_table(
    connection: &SqliteConnection,
    table_name: &str,
    unique_columns: &[String],
) -> Result<Vec<ColumnSchema>, DactylError> {
    let sql = format!("PRAGMA table_info({})", quote_identifier(table_name));
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| sqlite_error("prepare SQLite column catalog", error))?;
    let columns = statement
        .query_map([], |row| {
            let name = row.get::<_, String>(1)?;
            let default = row
                .get::<_, Option<String>>(4)?
                .map(|value| default_value(&value));
            Ok(ColumnSchema {
                unique: unique_columns.iter().any(|column| column == &name),
                name,
                primary_key: row.get::<_, i64>(5)? != 0,
                not_null: row.get::<_, i64>(3)? != 0,
                default,
            })
        })
        .map_err(|error| sqlite_error("read SQLite column catalog", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sqlite_error("decode SQLite column catalog", error))?;
    Ok(columns)
}

fn foreign_keys_for_table(
    connection: &SqliteConnection,
    table_name: &str,
) -> Result<Vec<ForeignKeySchema>, DactylError> {
    let sql = format!("PRAGMA foreign_key_list({})", quote_identifier(table_name));
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| sqlite_error("prepare SQLite foreign-key catalog", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                row.get::<_, String>(6)?,
            ))
        })
        .map_err(|error| sqlite_error("read SQLite foreign-key catalog", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sqlite_error("decode SQLite foreign-key catalog", error))?;

    type ForeignKeyParts = (String, ForeignKeyAction, Vec<(i64, String, String)>);
    let mut grouped: BTreeMap<i64, ForeignKeyParts> = BTreeMap::new();
    for (id, sequence, ref_table, column, ref_column, action) in rows {
        let entry = grouped
            .entry(id)
            .or_insert_with(|| (ref_table.clone(), foreign_key_action(&action), Vec::new()));
        entry.2.push((sequence, column, ref_column));
    }
    Ok(grouped
        .into_values()
        .map(|(ref_table, on_delete, mut columns)| {
            columns.sort_by_key(|(sequence, _, _)| *sequence);
            ForeignKeySchema {
                columns: columns
                    .iter()
                    .map(|(_, column, _)| column.clone())
                    .collect(),
                ref_table,
                ref_columns: columns
                    .iter()
                    .map(|(_, _, ref_column)| ref_column.clone())
                    .collect(),
                on_delete,
            }
        })
        .collect())
}

fn default_value(value: &str) -> serde_json::Value {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        serde_json::Value::Null
    } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        serde_json::Value::String(trimmed[1..trimmed.len() - 1].replace("''", "'"))
    } else if let Ok(value) = serde_json::from_str(trimmed) {
        value
    } else {
        serde_json::Value::String(trimmed.to_owned())
    }
}

fn foreign_key_action(action: &str) -> ForeignKeyAction {
    match action.to_ascii_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "SET NULL" => ForeignKeyAction::SetNull,
        "SET DEFAULT" => ForeignKeyAction::SetDefault,
        "NO ACTION" => ForeignKeyAction::NoAction,
        "RESTRICT" => ForeignKeyAction::Restrict,
        _ => ForeignKeyAction::NoAction,
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sqlite_error(operation: &str, error: SqliteError) -> DactylError {
    let message = error.to_string();
    let (kind, code) = match &error {
        SqliteError::SqliteFailure(sqlite_error, detail) => {
            let text = detail.as_deref().unwrap_or(&message).to_ascii_lowercase();
            match sqlite_error.code {
                ErrorCode::DatabaseBusy => (AdapterErrorKind::Busy, "busy"),
                ErrorCode::DatabaseLocked => (AdapterErrorKind::Locked, "locked"),
                ErrorCode::ReadOnly => (AdapterErrorKind::ReadOnly, "read_only"),
                ErrorCode::ConstraintViolation => {
                    let code = if text.contains("foreign key") {
                        "foreign_key_violation"
                    } else if text.contains("not null") {
                        "not_null_violation"
                    } else if text.contains("unique") {
                        "unique_violation"
                    } else {
                        "constraint_failed"
                    };
                    (AdapterErrorKind::Constraint, code)
                }
                ErrorCode::NotADatabase | ErrorCode::DatabaseCorrupt => {
                    (AdapterErrorKind::Capability, "invalid_database")
                }
                ErrorCode::CannotOpen => (AdapterErrorKind::Unavailable, "cannot_open"),
                ErrorCode::DiskFull | ErrorCode::SystemIoFailure => {
                    (AdapterErrorKind::Storage, "storage_failure")
                }
                ErrorCode::ParameterOutOfRange => {
                    (AdapterErrorKind::InvalidOperation, "invalid_parameters")
                }
                _ => (AdapterErrorKind::Query, "sqlite_error"),
            }
        }
        SqliteError::InvalidParameterCount(_, _)
        | SqliteError::InvalidParameterName(_)
        | SqliteError::ToSqlConversionFailure(_) => {
            (AdapterErrorKind::InvalidOperation, "invalid_parameters")
        }
        SqliteError::MultipleStatement => (AdapterErrorKind::Capability, "multiple_statements"),
        SqliteError::InvalidQuery | SqliteError::ExecuteReturnedResults => {
            (AdapterErrorKind::InvalidOperation, "invalid_query")
        }
        SqliteError::FromSqlConversionFailure(_, _, _)
        | SqliteError::InvalidColumnIndex(_)
        | SqliteError::InvalidColumnName(_)
        | SqliteError::InvalidColumnType(_, _, _)
        | SqliteError::IntegralValueOutOfRange(_, _)
        | SqliteError::Utf8Error(_) => (AdapterErrorKind::Value, "value_error"),
        _ => (AdapterErrorKind::Query, "sqlite_error"),
    };
    DactylError::adapter_with_code(kind, code, format!("{operation}: {message}"))
}

fn adapter_error(kind: AdapterErrorKind, message: impl Into<String>) -> DactylError {
    DactylError::adapter(kind, message)
}

fn ensure_writable(mode: AccessMode) -> Result<(), DactylError> {
    if mode == AccessMode::ReadOnly {
        Err(adapter_error(
            AdapterErrorKind::ReadOnly,
            "route is read-only",
        ))
    } else {
        Ok(())
    }
}

fn first_word(sql: &str) -> Option<String> {
    sql.split_whitespace()
        .next()
        .map(|word| word.trim_matches(';').to_ascii_lowercase())
}

fn is_query(first: Option<&str>) -> bool {
    matches!(
        first,
        Some("select" | "with" | "pragma" | "explain" | "values")
    )
}

fn has_multiple_statements(sql: &str) -> bool {
    let mut quoted = false;
    let mut chars = sql.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\'' {
            if quoted && chars.peek() == Some(&'\'') {
                let _ = chars.next();
            } else {
                quoted = !quoted;
            }
        } else if character == ';' && !quoted && chars.clone().any(|next| !next.is_whitespace()) {
            return true;
        }
    }
    false
}
