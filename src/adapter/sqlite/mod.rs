//! Small SQLite C-API driver.
//!
//! This module intentionally talks to SQLite through `libsqlite3-sys` rather
//! than exposing or depending on the high-level `rusqlite` API. The safe
//! wrapper below contains only the primitives Dactyl needs: open, bind,
//! step, read cells, execute, and finalize.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::slice;

use libsqlite3_sys as ffi;

use crate::adapter::Adapter;
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Row, Rows};

/// A private SQLite connection. No SQLite handle or C type crosses the public
/// Dactyl API boundary.
pub struct SqliteAdapter {
    db: *mut ffi::sqlite3,
}

impl SqliteAdapter {
    pub fn open(path: &str) -> Result<Self, DactylError> {
        if path != ":memory:" {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|error| {
                        DactylError::adapter(AdapterErrorKind::Storage, error.to_string())
                    })?;
                }
            }
        }

        let filename = CString::new(path)
            .map_err(|_| DactylError::Config("SQLite path contains NUL".into()))?;
        let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE;
        let mut db = ptr::null_mut();
        let code = unsafe { ffi::sqlite3_open_v2(filename.as_ptr(), &mut db, flags, ptr::null()) };
        if code != ffi::SQLITE_OK {
            let error = sqlite_error(db, code, "open");
            if !db.is_null() {
                unsafe { ffi::sqlite3_close(db) };
            }
            return Err(error);
        }

        Ok(Self { db })
    }

    fn prepare(&self, sql: &str) -> Result<StatementHandle, DactylError> {
        let sql = CString::new(sql)
            .map_err(|_| DactylError::adapter(AdapterErrorKind::Query, "SQL contains NUL"))?;
        let mut statement = ptr::null_mut();
        let code = unsafe {
            ffi::sqlite3_prepare_v2(self.db, sql.as_ptr(), -1, &mut statement, ptr::null_mut())
        };
        if code != ffi::SQLITE_OK {
            return Err(sqlite_error(self.db, code, "prepare"));
        }
        Ok(StatementHandle { ptr: statement })
    }

    fn bind(
        &self,
        statement: *mut ffi::sqlite3_stmt,
        params: &[Parameter],
    ) -> Result<(), DactylError> {
        let expected = unsafe { ffi::sqlite3_bind_parameter_count(statement) } as usize;
        if expected != params.len() {
            return Err(DactylError::adapter(
                AdapterErrorKind::Query,
                format!("expected {expected} parameters, received {}", params.len()),
            ));
        }
        for (index, parameter) in params.iter().enumerate() {
            let index = (index + 1) as c_int;
            let code = unsafe {
                match parameter {
                    Parameter::Null => ffi::sqlite3_bind_null(statement, index),
                    Parameter::Bool(value) => {
                        ffi::sqlite3_bind_int(statement, index, i32::from(*value))
                    }
                    Parameter::Integer(value) => ffi::sqlite3_bind_int64(statement, index, *value),
                    Parameter::Real(value) => ffi::sqlite3_bind_double(statement, index, *value),
                    Parameter::Text(value) => bind_bytes(statement, index, value.as_bytes(), true),
                    Parameter::Blob(value) => bind_bytes(statement, index, value, false),
                }
            };
            if code != ffi::SQLITE_OK {
                return Err(sqlite_error(self.db, code, "bind"));
            }
        }
        Ok(())
    }

    fn run(&self, sql: &str, params: &[Parameter]) -> Result<(Rows, u64), DactylError> {
        let statement = self.prepare(sql)?;
        self.bind(statement.ptr, params)?;
        let column_count = unsafe { ffi::sqlite3_column_count(statement.ptr) } as usize;
        let columns = (0..column_count)
            .map(|index| unsafe { column_name(statement.ptr, index as c_int) })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        loop {
            let code = unsafe { ffi::sqlite3_step(statement.ptr) };
            match code {
                ffi::SQLITE_ROW => {
                    let values = (0..column_count)
                        .map(|index| unsafe { column_value(statement.ptr, index as c_int) })
                        .collect();
                    rows.push(Row {
                        columns: columns.clone(),
                        values,
                    });
                }
                ffi::SQLITE_DONE => break,
                code => return Err(sqlite_error(self.db, code, "step")),
            }
        }
        let affected = unsafe { ffi::sqlite3_changes(self.db) } as u64;
        Ok((Rows(rows), affected))
    }
}

impl Drop for SqliteAdapter {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe { ffi::sqlite3_close(self.db) };
        }
    }
}

impl Adapter for SqliteAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        self.run(sql, params).map(|(rows, _)| rows)
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
        self.run(sql, params).map(|(_, affected)| affected)
    }
}

struct StatementHandle {
    ptr: *mut ffi::sqlite3_stmt,
}

impl Drop for StatementHandle {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { ffi::sqlite3_finalize(self.ptr) };
        }
    }
}

unsafe fn bind_bytes(
    statement: *mut ffi::sqlite3_stmt,
    index: c_int,
    bytes: &[u8],
    text: bool,
) -> c_int {
    let length = match c_int::try_from(bytes.len()) {
        Ok(length) => length,
        Err(_) => return ffi::SQLITE_TOOBIG,
    };
    let pointer = if bytes.is_empty() {
        ptr::null()
    } else {
        bytes.as_ptr()
    };
    if text {
        ffi::sqlite3_bind_text(
            statement,
            index,
            pointer.cast::<c_char>(),
            length,
            ffi::SQLITE_TRANSIENT(),
        )
    } else {
        ffi::sqlite3_bind_blob(
            statement,
            index,
            pointer.cast::<c_void>(),
            length,
            ffi::SQLITE_TRANSIENT(),
        )
    }
}

unsafe fn column_name(statement: *mut ffi::sqlite3_stmt, index: c_int) -> String {
    let pointer = ffi::sqlite3_column_name(statement, index);
    if pointer.is_null() {
        String::new()
    } else {
        CStr::from_ptr(pointer).to_string_lossy().into_owned()
    }
}

unsafe fn column_value(statement: *mut ffi::sqlite3_stmt, index: c_int) -> serde_json::Value {
    match ffi::sqlite3_column_type(statement, index) {
        ffi::SQLITE_NULL => serde_json::Value::Null,
        ffi::SQLITE_INTEGER => {
            serde_json::Value::Number((ffi::sqlite3_column_int64(statement, index) as i64).into())
        }
        ffi::SQLITE_FLOAT => {
            serde_json::Number::from_f64(ffi::sqlite3_column_double(statement, index))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        ffi::SQLITE_TEXT => {
            let pointer = ffi::sqlite3_column_text(statement, index);
            let length = ffi::sqlite3_column_bytes(statement, index).max(0) as usize;
            if pointer.is_null() {
                serde_json::Value::String(String::new())
            } else {
                let bytes = slice::from_raw_parts(pointer, length);
                serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned())
            }
        }
        ffi::SQLITE_BLOB => {
            let pointer = ffi::sqlite3_column_blob(statement, index);
            let length = ffi::sqlite3_column_bytes(statement, index).max(0) as usize;
            let bytes = if pointer.is_null() || length == 0 {
                &[]
            } else {
                slice::from_raw_parts(pointer.cast::<u8>(), length)
            };
            serde_json::Value::Array(
                bytes
                    .iter()
                    .map(|byte| serde_json::Value::Number((*byte as u64).into()))
                    .collect(),
            )
        }
        _ => serde_json::Value::Null,
    }
}

fn sqlite_error(db: *mut ffi::sqlite3, code: c_int, operation: &str) -> DactylError {
    sqlite_error_with_detail(db, code, operation, None)
}

fn sqlite_error_with_detail(
    db: *mut ffi::sqlite3,
    code: c_int,
    operation: &str,
    detail: Option<String>,
) -> DactylError {
    let primary = code & 0xff;
    let kind = match primary {
        ffi::SQLITE_BUSY => AdapterErrorKind::Busy,
        ffi::SQLITE_LOCKED => AdapterErrorKind::Locked,
        ffi::SQLITE_CONSTRAINT => AdapterErrorKind::Constraint,
        ffi::SQLITE_READONLY => AdapterErrorKind::ReadOnly,
        ffi::SQLITE_IOERR | ffi::SQLITE_CANTOPEN | ffi::SQLITE_CORRUPT | ffi::SQLITE_NOTADB => {
            AdapterErrorKind::Storage
        }
        ffi::SQLITE_ERROR | ffi::SQLITE_SCHEMA | ffi::SQLITE_AUTH => AdapterErrorKind::Query,
        _ => AdapterErrorKind::Unknown,
    };
    let message = detail.unwrap_or_else(|| {
        if db.is_null() {
            format!("{operation} failed with SQLite code {code}")
        } else {
            let pointer = unsafe { ffi::sqlite3_errmsg(db) };
            if pointer.is_null() {
                format!("{operation} failed with SQLite code {code}")
            } else {
                format!(
                    "{operation}: {}",
                    unsafe { CStr::from_ptr(pointer) }.to_string_lossy()
                )
            }
        }
    });
    DactylError::adapter(kind, message)
}
