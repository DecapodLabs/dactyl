//! Minimal, runtime-loaded declarations for the host SQLite C ABI.
//!
//! This is intentionally private to Dactyl. The crate does not depend on a
//! Rust SQLite wrapper, `libsqlite3-sys`, or the SQLite amalgamation. It loads
//! the host's shared SQLite library on each supported platform and resolves
//! only the symbols used by the connector.

#![allow(non_camel_case_types)]

use std::ffi::{c_char, c_double, c_int, c_void, CStr};

use libloading::{Library, Symbol};

use crate::error::{AdapterErrorKind, DactylError};

#[repr(C)]
pub struct sqlite3 {
    _private: [u8; 0],
}

#[repr(C)]
pub struct sqlite3_stmt {
    _private: [u8; 0],
}

pub type sqlite3_destructor_type = Option<unsafe extern "C" fn(*mut c_void)>;

type OpenV2 = unsafe extern "C" fn(*const c_char, *mut *mut sqlite3, c_int, *const c_char) -> c_int;
type Close = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type Errmsg = unsafe extern "C" fn(*mut sqlite3) -> *const c_char;
type Errcode = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type BusyTimeout = unsafe extern "C" fn(*mut sqlite3, c_int) -> c_int;
type Exec = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    Option<unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int>,
    *mut c_void,
    *mut *mut c_char,
) -> c_int;
type PrepareV2 = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    c_int,
    *mut *mut sqlite3_stmt,
    *mut *const c_char,
) -> c_int;
type Finalize = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type BindNull = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_int;
type BindInt64 = unsafe extern "C" fn(*mut sqlite3_stmt, c_int, i64) -> c_int;
type BindDouble = unsafe extern "C" fn(*mut sqlite3_stmt, c_int, c_double) -> c_int;
type BindText = unsafe extern "C" fn(
    *mut sqlite3_stmt,
    c_int,
    *const c_char,
    c_int,
    sqlite3_destructor_type,
) -> c_int;
type BindBlob = unsafe extern "C" fn(
    *mut sqlite3_stmt,
    c_int,
    *const c_void,
    c_int,
    sqlite3_destructor_type,
) -> c_int;
type Step = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type ColumnCount = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type ColumnName = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_char;
type ColumnType = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_int;
type ColumnInt64 = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> i64;
type ColumnDouble = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_double;
type ColumnText = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const u8;
type ColumnBytes = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_int;
type ColumnBlob = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_void;
type Changes64 = unsafe extern "C" fn(*mut sqlite3) -> i64;
type LastInsertRowid = unsafe extern "C" fn(*mut sqlite3) -> i64;

/// The native error information needed by Dactyl's stable mapper.
#[derive(Debug)]
pub struct SqliteFailure {
    pub code: c_int,
    pub extended_code: c_int,
    pub message: String,
}

/// Loaded SQLite symbols. The `Library` must remain alive for every function
/// pointer, so it is stored alongside the copied symbols.
pub struct Api {
    _library: Library,
    open_v2: OpenV2,
    close: Close,
    errmsg: Errmsg,
    errcode: Errcode,
    extended_errcode: Errcode,
    busy_timeout: BusyTimeout,
    exec: Exec,
    prepare_v2: PrepareV2,
    finalize: Finalize,
    bind_null: BindNull,
    bind_int64: BindInt64,
    bind_double: BindDouble,
    bind_text: BindText,
    bind_blob: BindBlob,
    step: Step,
    column_count: ColumnCount,
    column_name: ColumnName,
    column_type: ColumnType,
    column_int64: ColumnInt64,
    column_double: ColumnDouble,
    column_text: ColumnText,
    column_bytes: ColumnBytes,
    column_blob: ColumnBlob,
    changes64: Changes64,
    last_insert_rowid: LastInsertRowid,
}

impl Api {
    pub fn load() -> Result<Self, DactylError> {
        let candidates = library_candidates();
        let mut failures = Vec::new();
        let library = candidates
            .iter()
            .find_map(|name| match unsafe { Library::new(name) } {
                Ok(library) => Some(library),
                Err(error) => {
                    failures.push(format!("{name}: {error}"));
                    None
                }
            })
            .ok_or_else(|| {
                DactylError::adapter_with_code(
                    AdapterErrorKind::Unavailable,
                    "sqlite_runtime_unavailable",
                    format!(
                        "could not load a host SQLite shared library; tried {} ({})",
                        candidates.join(", "),
                        failures.join("; ")
                    ),
                )
            })?;

        macro_rules! symbol {
            ($name:literal, $type:ty) => {
                load_symbol::<$type>(&library, concat!($name, "\0").as_bytes(), $name)?
            };
        }

        Ok(Self {
            open_v2: symbol!("sqlite3_open_v2", OpenV2),
            close: symbol!("sqlite3_close", Close),
            errmsg: symbol!("sqlite3_errmsg", Errmsg),
            errcode: symbol!("sqlite3_errcode", Errcode),
            extended_errcode: symbol!("sqlite3_extended_errcode", Errcode),
            busy_timeout: symbol!("sqlite3_busy_timeout", BusyTimeout),
            exec: symbol!("sqlite3_exec", Exec),
            prepare_v2: symbol!("sqlite3_prepare_v2", PrepareV2),
            finalize: symbol!("sqlite3_finalize", Finalize),
            bind_null: symbol!("sqlite3_bind_null", BindNull),
            bind_int64: symbol!("sqlite3_bind_int64", BindInt64),
            bind_double: symbol!("sqlite3_bind_double", BindDouble),
            bind_text: symbol!("sqlite3_bind_text", BindText),
            bind_blob: symbol!("sqlite3_bind_blob", BindBlob),
            step: symbol!("sqlite3_step", Step),
            column_count: symbol!("sqlite3_column_count", ColumnCount),
            column_name: symbol!("sqlite3_column_name", ColumnName),
            column_type: symbol!("sqlite3_column_type", ColumnType),
            column_int64: symbol!("sqlite3_column_int64", ColumnInt64),
            column_double: symbol!("sqlite3_column_double", ColumnDouble),
            column_text: symbol!("sqlite3_column_text", ColumnText),
            column_bytes: symbol!("sqlite3_column_bytes", ColumnBytes),
            column_blob: symbol!("sqlite3_column_blob", ColumnBlob),
            changes64: symbol!("sqlite3_changes64", Changes64),
            last_insert_rowid: symbol!("sqlite3_last_insert_rowid", LastInsertRowid),
            _library: library,
        })
    }

    pub unsafe fn open_v2(
        &self,
        filename: *const c_char,
        database: *mut *mut sqlite3,
        flags: c_int,
    ) -> c_int {
        (self.open_v2)(filename, database, flags, std::ptr::null())
    }

    pub unsafe fn close(&self, database: *mut sqlite3) -> c_int {
        (self.close)(database)
    }

    pub unsafe fn failure(&self, database: *mut sqlite3) -> SqliteFailure {
        let message = (self.errmsg)(database);
        let message = if message.is_null() {
            "SQLite operation failed".to_string()
        } else {
            CStr::from_ptr(message).to_string_lossy().into_owned()
        };
        SqliteFailure {
            code: (self.errcode)(database),
            extended_code: (self.extended_errcode)(database),
            message,
        }
    }

    pub unsafe fn busy_timeout(&self, database: *mut sqlite3, milliseconds: c_int) -> c_int {
        (self.busy_timeout)(database, milliseconds)
    }

    pub unsafe fn extended_errcode(&self, database: *mut sqlite3) -> c_int {
        (self.extended_errcode)(database)
    }

    pub unsafe fn exec(&self, database: *mut sqlite3, sql: *const c_char) -> c_int {
        (self.exec)(
            database,
            sql,
            None,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    }

    pub unsafe fn finalize(&self, statement: *mut sqlite3_stmt) -> c_int {
        (self.finalize)(statement)
    }

    pub unsafe fn prepare_v2(
        &self,
        database: *mut sqlite3,
        sql: *const c_char,
        statement: *mut *mut sqlite3_stmt,
    ) -> c_int {
        (self.prepare_v2)(database, sql, -1, statement, std::ptr::null_mut())
    }

    pub unsafe fn bind_null(&self, statement: *mut sqlite3_stmt, index: c_int) -> c_int {
        (self.bind_null)(statement, index)
    }

    pub unsafe fn bind_int64(
        &self,
        statement: *mut sqlite3_stmt,
        index: c_int,
        value: i64,
    ) -> c_int {
        (self.bind_int64)(statement, index, value)
    }

    pub unsafe fn bind_double(
        &self,
        statement: *mut sqlite3_stmt,
        index: c_int,
        value: c_double,
    ) -> c_int {
        (self.bind_double)(statement, index, value)
    }

    pub unsafe fn bind_text(
        &self,
        statement: *mut sqlite3_stmt,
        index: c_int,
        value: *const c_char,
        byte_count: c_int,
    ) -> c_int {
        (self.bind_text)(statement, index, value, byte_count, None)
    }

    pub unsafe fn bind_blob(
        &self,
        statement: *mut sqlite3_stmt,
        index: c_int,
        value: *const c_void,
        byte_count: c_int,
    ) -> c_int {
        (self.bind_blob)(statement, index, value, byte_count, None)
    }

    pub unsafe fn step(&self, statement: *mut sqlite3_stmt) -> c_int {
        (self.step)(statement)
    }

    pub unsafe fn column_count(&self, statement: *mut sqlite3_stmt) -> c_int {
        (self.column_count)(statement)
    }

    pub unsafe fn column_name(&self, statement: *mut sqlite3_stmt, column: c_int) -> *const c_char {
        (self.column_name)(statement, column)
    }

    pub unsafe fn column_type(&self, statement: *mut sqlite3_stmt, column: c_int) -> c_int {
        (self.column_type)(statement, column)
    }

    pub unsafe fn column_int64(&self, statement: *mut sqlite3_stmt, column: c_int) -> i64 {
        (self.column_int64)(statement, column)
    }

    pub unsafe fn column_double(&self, statement: *mut sqlite3_stmt, column: c_int) -> c_double {
        (self.column_double)(statement, column)
    }

    pub unsafe fn column_text(&self, statement: *mut sqlite3_stmt, column: c_int) -> *const u8 {
        (self.column_text)(statement, column)
    }

    pub unsafe fn column_bytes(&self, statement: *mut sqlite3_stmt, column: c_int) -> c_int {
        (self.column_bytes)(statement, column)
    }

    pub unsafe fn column_blob(&self, statement: *mut sqlite3_stmt, column: c_int) -> *const c_void {
        (self.column_blob)(statement, column)
    }

    pub unsafe fn changes64(&self, database: *mut sqlite3) -> i64 {
        (self.changes64)(database)
    }

    pub unsafe fn last_insert_rowid(&self, database: *mut sqlite3) -> i64 {
        (self.last_insert_rowid)(database)
    }
}

fn load_symbol<T: Copy>(
    library: &Library,
    name: &[u8],
    display_name: &str,
) -> Result<T, DactylError> {
    let symbol: Symbol<'_, T> = unsafe { library.get(name) }.map_err(|error| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Unavailable,
            "sqlite_runtime_incompatible",
            format!("host SQLite library is missing {display_name}: {error}"),
        )
    })?;
    Ok(*symbol)
}

fn library_candidates() -> Vec<String> {
    let mut candidates = Vec::new();
    if let Ok(path) = std::env::var("DACTYL_SQLITE_LIBRARY") {
        if !path.trim().is_empty() {
            candidates.push(path);
        }
    }
    candidates.extend(
        platform_library_names()
            .iter()
            .map(|name| (*name).to_string()),
    );
    candidates
}

#[cfg(target_os = "windows")]
fn platform_library_names() -> &'static [&'static str] {
    &["sqlite3.dll"]
}

#[cfg(target_os = "macos")]
fn platform_library_names() -> &'static [&'static str] {
    &["/usr/lib/libsqlite3.dylib", "libsqlite3.dylib"]
}

#[cfg(target_os = "ios")]
fn platform_library_names() -> &'static [&'static str] {
    &["/usr/lib/libsqlite3.dylib", "libsqlite3.dylib"]
}

#[cfg(target_os = "android")]
fn platform_library_names() -> &'static [&'static str] {
    &["libsqlite.so", "libsqlite.so.0"]
}

#[cfg(all(
    unix,
    not(any(target_os = "macos", target_os = "ios", target_os = "android"))
))]
fn platform_library_names() -> &'static [&'static str] {
    &["libsqlite3.so.0", "libsqlite3.so", "libsqlite3.dylib"]
}

#[cfg(not(any(unix, target_os = "windows")))]
fn platform_library_names() -> &'static [&'static str] {
    &["sqlite3"]
}

pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ERROR: c_int = 1;
pub const SQLITE_BUSY: c_int = 5;
pub const SQLITE_LOCKED: c_int = 6;
pub const SQLITE_READONLY: c_int = 8;
pub const SQLITE_IOERR: c_int = 10;
pub const SQLITE_CORRUPT: c_int = 11;
pub const SQLITE_CANTOPEN: c_int = 14;
pub const SQLITE_CONSTRAINT: c_int = 19;
pub const SQLITE_RANGE: c_int = 25;
pub const SQLITE_NOTADB: c_int = 26;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;

pub const SQLITE_OPEN_READONLY: c_int = 0x0000_0001;
pub const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
pub const SQLITE_OPEN_CREATE: c_int = 0x0000_0004;
pub const SQLITE_OPEN_FULLMUTEX: c_int = 0x0001_0000;

pub const SQLITE_INTEGER: c_int = 1;
pub const SQLITE_FLOAT: c_int = 2;
pub const SQLITE_TEXT: c_int = 3;
pub const SQLITE_BLOB: c_int = 4;
pub const SQLITE_NULL: c_int = 5;
