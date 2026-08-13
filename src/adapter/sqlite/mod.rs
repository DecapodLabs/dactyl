//! SQLite-backed local storage through the host's shared SQLite ABI.
//!
//! Dactyl owns the narrow driver boundary: route selection, parameter
//! binding, atomic batches, access mode, row/result normalization, and stable
//! error categories. SQLite owns file compatibility, SQL execution, locking,
//! journaling, and constraint enforcement. No SQLite Rust wrapper, bundled
//! amalgamation, or `libsqlite3-sys` dependency is used.

mod ffi;

use std::collections::BTreeMap;
use std::ffi::{c_int, c_void, CString};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

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

struct SqliteConnection {
    api: Arc<ffi::Api>,
    database: *mut ffi::sqlite3,
}

// SQLite serializes access according to the flags supplied at open time. The
// adapter additionally holds the connection behind a Mutex, so the raw handle
// is never concurrently used by safe Dactyl code.
unsafe impl Send for SqliteConnection {}
unsafe impl Sync for SqliteConnection {}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        if !self.database.is_null() {
            unsafe {
                let _ = self.api.close(self.database);
            }
        }
    }
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
            validate_existing_sqlite_header(path_ref)?;
        }

        let api = Arc::new(ffi::Api::load()?);
        let database = open_database(&api, path, options.access_mode)?;
        let connection = SqliteConnection { api, database };
        let timeout = options
            .lock_timeout
            .as_millis()
            .try_into()
            .unwrap_or(c_int::MAX);
        let result = (|| {
            let code = unsafe { connection.api.busy_timeout(connection.database, timeout) };
            if code != ffi::SQLITE_OK {
                return Err(connection.error("configure SQLite busy timeout", code));
            }
            exec_sql(&connection, "PRAGMA foreign_keys = ON")
                .map_err(|error| connection.error_from_failure("enable SQLite foreign keys", error))
        })();
        if let Err(error) = result {
            drop(connection);
            return Err(error);
        }

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

fn open_database(
    api: &Arc<ffi::Api>,
    path: &str,
    access_mode: AccessMode,
) -> Result<*mut ffi::sqlite3, DactylError> {
    let filename = CString::new(path).map_err(|_| {
        DactylError::adapter_with_code(
            AdapterErrorKind::InvalidOperation,
            "invalid_path",
            "SQLite path contains an interior NUL byte",
        )
    })?;
    let flags = match access_mode {
        AccessMode::ReadWrite => ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
        AccessMode::ReadOnly => ffi::SQLITE_OPEN_READONLY,
    } | ffi::SQLITE_OPEN_FULLMUTEX;
    let mut database = std::ptr::null_mut();
    let code = unsafe { api.open_v2(filename.as_ptr(), &mut database, flags) };
    if code != ffi::SQLITE_OK {
        let error = if database.is_null() {
            DactylError::adapter_with_code(
                AdapterErrorKind::Unavailable,
                "cannot_open",
                format!("open SQLite database {path}: SQLite error code {code}"),
            )
        } else {
            let failure = unsafe { api.failure(database) };
            map_sqlite_failure("open SQLite database", failure)
        };
        if !database.is_null() {
            unsafe {
                let _ = api.close(database);
            }
        }
        return Err(error);
    }
    if database.is_null() {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Unavailable,
            "cannot_open",
            format!("open SQLite database {path}: SQLite returned no handle"),
        ));
    }
    Ok(database)
}

fn validate_existing_sqlite_header(path: &Path) -> Result<(), DactylError> {
    let metadata = fs::metadata(path).map_err(|error| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Storage,
            "stat_database_failed",
            format!("inspect SQLite database: {error}"),
        )
    })?;
    if metadata.len() == 0 {
        return Ok(());
    }

    let mut file = fs::File::open(path).map_err(|error| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Storage,
            "read_database_header_failed",
            format!("read SQLite database header: {error}"),
        )
    })?;
    let mut header = [0_u8; 16];
    if file.read_exact(&mut header).is_err() || header != *b"SQLite format 3\0" {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "invalid_database",
            "existing local file is not a SQLite database",
        ));
    }
    Ok(())
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
            exec_sql(&connection, "BEGIN IMMEDIATE").map_err(|error| {
                connection.error_from_failure("begin SQLite transaction", error)
            })?;
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
                if let Err(error) = exec_sql(&connection, "COMMIT") {
                    let _ = exec_sql(&connection, "ROLLBACK");
                    Err(connection.error_from_failure("commit SQLite transaction", error))
                } else {
                    Ok(value)
                }
            }
            Err(error) => {
                let _ = exec_sql(&connection, "ROLLBACK");
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
                exec_sql(connection, operation.sql()).map_err(|error| {
                    connection.error_from_failure("execute SQLite schema batch", error)
                })?;
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
    let mut statement = Statement::prepare(connection, sql)?;
    statement.bind_all(params)?;
    loop {
        match statement.step()? {
            ffi::SQLITE_ROW => continue,
            ffi::SQLITE_DONE => break,
            _ => unreachable!("Statement::step maps non-row/non-done to an error"),
        }
    }
    let generated_keys = if first_word(sql).as_deref() == Some("insert") {
        let rowid = unsafe { connection.api.last_insert_rowid(connection.database) };
        if rowid != 0 {
            vec![GeneratedKey::Integer(rowid)]
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    Ok(WriteResult {
        affected_rows: unsafe { connection.api.changes64(connection.database) }
            .try_into()
            .unwrap_or_default(),
        generated_keys,
    })
}

fn query_rows(
    connection: &SqliteConnection,
    sql: &str,
    params: &[Parameter],
) -> Result<Rows, DactylError> {
    let mut statement = Statement::prepare(connection, sql)?;
    statement.bind_all(params)?;
    let column_count = unsafe { connection.api.column_count(statement.statement) };
    let columns = (0..column_count)
        .map(|index| statement.column_name(index))
        .collect::<Result<Vec<_>, _>>()?;
    let mut result_rows = Vec::new();
    while statement.step()? == ffi::SQLITE_ROW {
        let values = (0..column_count)
            .map(|index| statement.value(index))
            .collect::<Result<Vec<_>, _>>()?;
        result_rows.push(Row {
            columns: columns.clone(),
            values,
        });
    }
    Ok(Rows(result_rows))
}

struct Statement<'a> {
    connection: &'a SqliteConnection,
    statement: *mut ffi::sqlite3_stmt,
}

impl<'a> Statement<'a> {
    fn prepare(connection: &'a SqliteConnection, sql: &str) -> Result<Self, DactylError> {
        let sql = CString::new(sql).map_err(|_| {
            adapter_error(
                AdapterErrorKind::InvalidOperation,
                "SQL contains an interior NUL byte",
            )
        })?;
        let mut statement = std::ptr::null_mut();
        let code = unsafe {
            connection
                .api
                .prepare_v2(connection.database, sql.as_ptr(), &mut statement)
        };
        if code != ffi::SQLITE_OK {
            return Err(connection.error("prepare SQLite statement", code));
        }
        if statement.is_null() {
            return Err(adapter_error(
                AdapterErrorKind::Query,
                "SQLite returned an empty statement handle",
            ));
        }
        Ok(Self {
            connection,
            statement,
        })
    }

    fn bind_all(&mut self, params: &[Parameter]) -> Result<(), DactylError> {
        for (index, parameter) in params.iter().enumerate() {
            let index = c_int::try_from(index + 1).map_err(|_| {
                adapter_error(
                    AdapterErrorKind::InvalidOperation,
                    "too many SQLite parameters",
                )
            })?;
            let code = unsafe {
                match parameter {
                    Parameter::Null => self.connection.api.bind_null(self.statement, index),
                    Parameter::Bool(value) => {
                        self.connection
                            .api
                            .bind_int64(self.statement, index, i64::from(*value))
                    }
                    Parameter::Integer(value) => {
                        self.connection
                            .api
                            .bind_int64(self.statement, index, *value)
                    }
                    Parameter::Real(value) => {
                        self.connection
                            .api
                            .bind_double(self.statement, index, *value)
                    }
                    Parameter::Text(value) => self.connection.api.bind_text(
                        self.statement,
                        index,
                        value.as_ptr().cast(),
                        c_int::try_from(value.len()).map_err(|_| {
                            adapter_error(
                                AdapterErrorKind::Value,
                                "SQLite text parameter is too large",
                            )
                        })?,
                    ),
                    Parameter::Blob(value) => self.connection.api.bind_blob(
                        self.statement,
                        index,
                        value.as_ptr().cast::<c_void>(),
                        c_int::try_from(value.len()).map_err(|_| {
                            adapter_error(
                                AdapterErrorKind::Value,
                                "SQLite blob parameter is too large",
                            )
                        })?,
                    ),
                }
            };
            if code != ffi::SQLITE_OK {
                return Err(self.connection.error("bind SQLite parameter", code));
            }
        }
        Ok(())
    }

    fn step(&mut self) -> Result<c_int, DactylError> {
        let code = unsafe { self.connection.api.step(self.statement) };
        if matches!(code, ffi::SQLITE_ROW | ffi::SQLITE_DONE) {
            Ok(code)
        } else {
            Err(self.connection.error("step SQLite statement", code))
        }
    }

    fn column_name(&self, index: c_int) -> Result<String, DactylError> {
        let value = unsafe { self.connection.api.column_name(self.statement, index) };
        if value.is_null() {
            return Err(adapter_error(
                AdapterErrorKind::Value,
                "SQLite returned a null column name",
            ));
        }
        Ok(unsafe { std::ffi::CStr::from_ptr(value) }
            .to_string_lossy()
            .into_owned())
    }

    fn value(&self, index: c_int) -> Result<serde_json::Value, DactylError> {
        let kind = unsafe { self.connection.api.column_type(self.statement, index) };
        unsafe {
            match kind {
                ffi::SQLITE_NULL => Ok(serde_json::Value::Null),
                ffi::SQLITE_INTEGER => Ok(serde_json::Value::Number(
                    self.connection
                        .api
                        .column_int64(self.statement, index)
                        .into(),
                )),
                ffi::SQLITE_FLOAT => serde_json::Number::from_f64(
                    self.connection.api.column_double(self.statement, index),
                )
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    adapter_error(AdapterErrorKind::Value, "SQLite returned a non-finite REAL")
                }),
                ffi::SQLITE_TEXT => {
                    let pointer = self.connection.api.column_text(self.statement, index);
                    let length = self.connection.api.column_bytes(self.statement, index);
                    if pointer.is_null() && length != 0 {
                        return Err(adapter_error(
                            AdapterErrorKind::Value,
                            "SQLite returned a null text pointer",
                        ));
                    }
                    String::from_utf8(
                        std::slice::from_raw_parts(pointer, length.max(0) as usize).to_vec(),
                    )
                    .map(serde_json::Value::String)
                    .map_err(|error| {
                        adapter_error(
                            AdapterErrorKind::Value,
                            format!("SQLite returned invalid UTF-8 text: {error}"),
                        )
                    })
                }
                ffi::SQLITE_BLOB => {
                    let pointer = self.connection.api.column_blob(self.statement, index);
                    let length = self.connection.api.column_bytes(self.statement, index);
                    if pointer.is_null() && length != 0 {
                        return Err(adapter_error(
                            AdapterErrorKind::Value,
                            "SQLite returned a null blob pointer",
                        ));
                    }
                    Ok(serde_json::Value::Array(
                        std::slice::from_raw_parts(pointer.cast::<u8>(), length.max(0) as usize)
                            .iter()
                            .map(|byte| serde_json::Value::Number(u64::from(*byte).into()))
                            .collect(),
                    ))
                }
                _ => Err(adapter_error(
                    AdapterErrorKind::Value,
                    "SQLite returned an unknown value type",
                )),
            }
        }
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        if !self.statement.is_null() {
            unsafe {
                let _ = self.connection.api.finalize(self.statement);
            }
        }
    }
}

fn exec_sql(connection: &SqliteConnection, sql: &str) -> Result<(), ffi::SqliteFailure> {
    let sql = CString::new(sql).map_err(|_| ffi::SqliteFailure {
        code: ffi::SQLITE_ERROR,
        extended_code: ffi::SQLITE_ERROR,
        message: "SQL contains an interior NUL byte".to_string(),
    })?;
    let code = unsafe { connection.api.exec(connection.database, sql.as_ptr()) };
    if code == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(unsafe { connection.api.failure(connection.database) })
    }
}

impl SqliteConnection {
    fn error(&self, operation: &str, code: c_int) -> DactylError {
        map_sqlite_failure(
            operation,
            ffi::SqliteFailure {
                code,
                extended_code: unsafe { self.api.extended_errcode(self.database) },
                message: unsafe { self.api.failure(self.database).message },
            },
        )
    }

    fn error_from_failure(&self, operation: &str, failure: ffi::SqliteFailure) -> DactylError {
        map_sqlite_failure(operation, failure)
    }
}

fn map_sqlite_failure(operation: &str, failure: ffi::SqliteFailure) -> DactylError {
    let text = failure.message.to_ascii_lowercase();
    let (kind, code) = match failure.code {
        ffi::SQLITE_BUSY => (AdapterErrorKind::Busy, "busy"),
        ffi::SQLITE_LOCKED => (AdapterErrorKind::Locked, "locked"),
        ffi::SQLITE_READONLY => (AdapterErrorKind::ReadOnly, "read_only"),
        ffi::SQLITE_CONSTRAINT => {
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
        ffi::SQLITE_NOTADB | ffi::SQLITE_CORRUPT => {
            (AdapterErrorKind::Capability, "invalid_database")
        }
        ffi::SQLITE_CANTOPEN => (AdapterErrorKind::Unavailable, "cannot_open"),
        ffi::SQLITE_IOERR => (AdapterErrorKind::Storage, "storage_failure"),
        ffi::SQLITE_RANGE => (AdapterErrorKind::InvalidOperation, "invalid_parameters"),
        _ => (AdapterErrorKind::Query, "sqlite_error"),
    };
    DactylError::adapter_with_code(
        kind,
        code,
        format!(
            "{operation}: {} (extended code {})",
            failure.message, failure.extended_code
        ),
    )
}

fn inspect_schema(connection: &SqliteConnection) -> Result<StoreSchema, DactylError> {
    let table_rows = query_rows(
        connection,
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        &[],
    )?;
    let mut tables = Vec::with_capacity(table_rows.len());
    let mut indexes = Vec::new();
    for row in table_rows.iter() {
        let table_name = row_string(row, 0)?;
        let table_indexes = indexes_for_table(connection, &table_name)?;
        let unique_columns = table_indexes
            .iter()
            .filter(|index| index.unique && index.columns.len() == 1)
            .flat_map(|index| index.columns.first().cloned())
            .collect::<Vec<_>>();
        let columns = columns_for_table(connection, &table_name, &unique_columns)?;
        let foreign_keys = foreign_keys_for_table(connection, &table_name)?;
        let count_sql = format!("SELECT COUNT(*) FROM {}", quote_identifier(&table_name));
        let count_rows = query_rows(connection, &count_sql, &[])?;
        let row_count = row_i64(
            count_rows.as_slice().first().ok_or_else(|| {
                adapter_error(AdapterErrorKind::Value, "SQLite returned no table count")
            })?,
            0,
        )?;
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
    let rows = query_rows(connection, &sql, &[])?;
    let mut indexes = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let name = row_string(row, 1)?;
        let unique = row_i64(row, 2)? != 0;
        let sql = format!("PRAGMA index_info({})", quote_identifier(&name));
        let columns = query_rows(connection, &sql, &[])?
            .iter()
            .filter_map(|row| {
                row_value(row, 2).and_then(|value| value.as_str().map(ToOwned::to_owned))
            })
            .collect();
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
    query_rows(connection, &sql, &[])?
        .iter()
        .map(|row| {
            let name = row_string(row, 1)?;
            let default = row_value(row, 4)
                .and_then(|value| value.as_str())
                .map(default_value);
            Ok(ColumnSchema {
                unique: unique_columns.iter().any(|column| column == &name),
                name,
                primary_key: row_i64(row, 5)? != 0,
                not_null: row_i64(row, 3)? != 0,
                default,
            })
        })
        .collect()
}

fn foreign_keys_for_table(
    connection: &SqliteConnection,
    table_name: &str,
) -> Result<Vec<ForeignKeySchema>, DactylError> {
    let sql = format!("PRAGMA foreign_key_list({})", quote_identifier(table_name));
    let rows = query_rows(connection, &sql, &[])?;
    type ForeignKeyParts = (String, ForeignKeyAction, Vec<(i64, String, String)>);
    let mut grouped: BTreeMap<i64, ForeignKeyParts> = BTreeMap::new();
    for row in rows.iter() {
        let id = row_i64(row, 0)?;
        let sequence = row_i64(row, 1)?;
        let ref_table = row_string(row, 2)?;
        let column = row_string(row, 3)?;
        let ref_column = row_value(row, 4)
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let action = row_string(row, 6)?;
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

fn row_value(row: &Row, index: usize) -> Option<&serde_json::Value> {
    row.values.get(index)
}

fn row_string(row: &Row, index: usize) -> Result<String, DactylError> {
    row_value(row, index)
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            adapter_error(
                AdapterErrorKind::Value,
                "SQLite catalog returned a non-text value",
            )
        })
}

fn row_i64(row: &Row, index: usize) -> Result<i64, DactylError> {
    row_value(row, index)
        .and_then(|value| value.as_i64())
        .ok_or_else(|| {
            adapter_error(
                AdapterErrorKind::Value,
                "SQLite catalog returned a non-integer value",
            )
        })
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
