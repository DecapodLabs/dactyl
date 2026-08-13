//! Explicit SQLite-to-Dactyl conversion.
//!
//! Opening a SQLite file as a Dactyl route still fails closed. This module is
//! the supported migration boundary: inspect the source read-only, convert into
//! a native snapshot, and publish through a temporary file plus optional backup.

use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use rusqlite::types::ValueRef;
use rusqlite::{Connection as SqliteConn, OpenFlags};
use serde::Serialize;

use super::{
    execute_sql, load_store, persist_store, store_schema, AdapterErrorKind, DactylError, Store,
};
use crate::contract::OperationKind;

const SQLITE_MAGIC: &[u8] = b"SQLite format 3";

/// Result of a successful conversion or an idempotent no-op.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ImportReport {
    pub source: String,
    pub destination: String,
    pub already_converted: bool,
    pub tables: u64,
    pub indexes: u64,
    pub rows: u64,
    pub backup: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Missing,
    Empty,
    Sqlite,
    Dactyl,
    Unknown,
}

/// Convert a SQLite database into a Dactyl snapshot.
///
/// `source` and `destination` may be the same path. In that case the original
/// SQLite file is moved to `$path.legacy-sqlite` after a complete snapshot has
/// been written to a temporary file. A failed conversion leaves the SQLite
/// source authoritative.
pub fn import_sqlite_file(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<ImportReport, DactylError> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    let source_kind = sniff(source)?;
    let dest_kind = sniff(destination)?;
    let same_path = paths_equal(source, destination);

    match source_kind {
        FileKind::Missing => {
            return Err(coded(
                AdapterErrorKind::NotFound,
                "missing_input",
                format!("import source {} does not exist", source.display()),
            ))
        }
        FileKind::Unknown => {
            return Err(coded(
                AdapterErrorKind::Capability,
                "not_sqlite",
                format!("{} is not a SQLite or Dactyl store", source.display()),
            ))
        }
        FileKind::Empty => {
            return Err(coded(
                AdapterErrorKind::Capability,
                "not_sqlite",
                format!("{} is empty", source.display()),
            ))
        }
        FileKind::Dactyl if same_path => {
            let store = load_store(destination, crate::contract::AccessMode::ReadOnly)?;
            return Ok(report(source, destination, true, &store, None));
        }
        FileKind::Dactyl => {
            return Err(coded(
                AdapterErrorKind::Capability,
                "already_dactyl",
                format!(
                    "{} is already a Dactyl snapshot; copy it or open it directly",
                    source.display()
                ),
            ))
        }
        FileKind::Sqlite => {}
    }

    if dest_kind == FileKind::Sqlite && !same_path {
        return Err(coded(
            AdapterErrorKind::Capability,
            "destination_is_sqlite",
            "refusing to overwrite a different SQLite file; import in place or choose a new path",
        ));
    }

    if destination.exists() && dest_readonly(destination) {
        return Err(coded(
            AdapterErrorKind::ReadOnly,
            "read_only_destination",
            format!(
                "import destination {} is not writable",
                destination.display()
            ),
        ));
    }

    let imported = convert_sqlite(source)?;

    if dest_kind == FileKind::Dactyl {
        let existing = load_store(destination, crate::contract::AccessMode::ReadOnly)?;
        if stores_equivalent(&imported, &existing) {
            return Ok(report(source, destination, true, &imported, None));
        }
        return Err(coded(
            AdapterErrorKind::Conflict,
            "divergent_destination",
            "destination is already a Dactyl snapshot and does not match the source",
        ));
    }

    if let Some(parent) = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            coded(
                AdapterErrorKind::Storage,
                "replacement_failed",
                format!("create destination parent: {error}"),
            )
        })?;
    }

    if same_path {
        publish_in_place(destination, &imported)
    } else {
        persist_store(destination, &imported).map_err(|error| {
            coded(
                AdapterErrorKind::Storage,
                "replacement_failed",
                format!("persist converted store: {error}"),
            )
        })?;
        Ok(report(source, destination, false, &imported, None))
    }
}

fn publish_in_place(path: &Path, store: &Store) -> Result<ImportReport, DactylError> {
    let tmp = PathBuf::from(format!("{}.import.tmp", path.display()));
    let backup = PathBuf::from(format!("{}.legacy-sqlite", path.display()));
    let _ = fs::remove_file(&tmp);
    persist_store(&tmp, store).map_err(|error| {
        let _ = fs::remove_file(&tmp);
        coded(
            AdapterErrorKind::Storage,
            "replacement_failed",
            format!("write import temp: {error}"),
        )
    })?;
    if backup.exists() {
        fs::remove_file(&backup).map_err(|error| {
            let _ = fs::remove_file(&tmp);
            coded(
                AdapterErrorKind::Storage,
                "replacement_failed",
                format!("replace previous backup: {error}"),
            )
        })?;
    }
    fs::rename(path, &backup).map_err(|error| {
        let _ = fs::remove_file(&tmp);
        coded(
            AdapterErrorKind::Storage,
            "replacement_failed",
            format!("backup source: {error}"),
        )
    })?;
    if let Err(error) = fs::rename(&tmp, path) {
        let _ = fs::rename(&backup, path);
        let _ = fs::remove_file(&tmp);
        return Err(coded(
            AdapterErrorKind::Storage,
            "replacement_failed",
            format!("replace destination: {error}"),
        ));
    }
    Ok(report(
        path,
        path,
        false,
        store,
        Some(backup.display().to_string()),
    ))
}

fn convert_sqlite(path: &Path) -> Result<Store, DactylError> {
    let conn = SqliteConn::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|error| corrupt(format!("open SQLite source: {error}")))?;
    reject_unsupported(&conn)?;

    let mut store = Store::default();
    let tables = query_master(&conn, "table")?;
    for (name, sql) in &tables {
        if name.starts_with("sqlite_") {
            continue;
        }
        let sql = sql.as_ref().ok_or_else(|| {
            coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("table {name} has no CREATE statement"),
            )
        })?;
        if sql.to_ascii_lowercase().contains("without rowid") {
            return Err(coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("WITHOUT ROWID is not supported: {name}"),
            ));
        }
        execute_sql(&mut store, sql, &[], OperationKind::Schema).map_err(|error| {
            coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("unsupported CREATE TABLE for {name}: {error}"),
            )
        })?;
    }

    for (name, sql) in query_master(&conn, "index")? {
        let Some(sql) = sql else {
            continue;
        };
        if name.starts_with("sqlite_") {
            continue;
        }
        execute_sql(&mut store, &sql, &[], OperationKind::Schema).map_err(|error| {
            coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("unsupported CREATE INDEX for {name}: {error}"),
            )
        })?;
    }

    for (name, _) in &tables {
        if name.starts_with("sqlite_") {
            continue;
        }
        import_table_rows(&conn, &mut store, name)?;
    }
    for (table_name, table) in &store.tables {
        for row in &table.rows {
            super::validate_foreign_keys(&store, table_name, row)?;
        }
    }
    Ok(store)
}

fn import_table_rows(conn: &SqliteConn, store: &mut Store, name: &str) -> Result<(), DactylError> {
    let quoted = quote_ident(name);
    let mut statement = conn
        .prepare(&format!("select * from {quoted}"))
        .map_err(|error| corrupt(format!("select {name}: {error}")))?;
    let width = statement.column_count();
    let mut rows = statement
        .query([])
        .map_err(|error| corrupt(format!("query {name}: {error}")))?;
    let indexes = store
        .indexes
        .values()
        .filter(|index| index.table == super::normalize(name) && index.unique)
        .cloned()
        .collect::<Vec<_>>();
    while let Some(row) = rows
        .next()
        .map_err(|error| corrupt(format!("read {name}: {error}")))?
    {
        let mut values = Vec::with_capacity(width);
        for index in 0..width {
            values.push(sqlite_value(row.get_ref(index).map_err(|error| {
                corrupt(format!("read {name} column {index}: {error}"))
            })?)?);
        }
        let table = store
            .tables
            .get_mut(&super::normalize(name))
            .ok_or_else(|| super::missing_table(name))?;
        if values.len() != table.columns.len() {
            return Err(coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("row width mismatch for {name}"),
            ));
        }
        super::validate(table, &values, None, &indexes)?;
        if let Some(index) = table.columns.iter().position(|column| column.primary_key) {
            if let Some(value) = values[index].as_i64() {
                table.next_id = table.next_id.max(value + 1);
            }
        }
        table.rows.push(values);
    }
    Ok(())
}

fn sqlite_value(value: ValueRef<'_>) -> Result<serde_json::Value, DactylError> {
    match value {
        ValueRef::Null => Ok(serde_json::Value::Null),
        ValueRef::Integer(value) => Ok(value.into()),
        ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                coded(
                    AdapterErrorKind::Value,
                    "unsupported_value",
                    "non-finite REAL cannot be imported",
                )
            }),
        ValueRef::Text(value) => Ok(String::from_utf8_lossy(value).into_owned().into()),
        ValueRef::Blob(value) => Ok(serde_json::Value::Array(
            value
                .iter()
                .map(|byte| serde_json::Value::Number(u64::from(*byte).into()))
                .collect(),
        )),
    }
}

fn reject_unsupported(conn: &SqliteConn) -> Result<(), DactylError> {
    let mut statement = conn
        .prepare("select type, name from sqlite_master where type in ('view', 'trigger')")
        .map_err(|error| corrupt(format!("inspect sqlite_master: {error}")))?;
    let mut rows = statement
        .query([])
        .map_err(|error| corrupt(format!("query sqlite_master: {error}")))?;
    if let Some(row) = rows
        .next()
        .map_err(|error| corrupt(format!("read sqlite_master: {error}")))?
    {
        let kind: String = row
            .get(0)
            .map_err(|error| corrupt(format!("read object type: {error}")))?;
        let name: String = row
            .get(1)
            .map_err(|error| corrupt(format!("read object name: {error}")))?;
        return Err(coded(
            AdapterErrorKind::Capability,
            "unsupported_schema",
            format!("{kind} {name} cannot be imported"),
        ));
    }
    Ok(())
}

fn query_master(
    conn: &SqliteConn,
    kind: &str,
) -> Result<Vec<(String, Option<String>)>, DactylError> {
    let mut statement = conn
        .prepare("select name, sql from sqlite_master where type = ?1 order by name")
        .map_err(|error| corrupt(format!("prepare sqlite_master: {error}")))?;
    let rows = statement
        .query_map([kind], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|error| corrupt(format!("query {kind}s: {error}")))?;
    let mut objects = Vec::new();
    for row in rows {
        objects.push(row.map_err(|error| corrupt(format!("read {kind}: {error}")))?);
    }
    Ok(objects)
}

fn sniff(path: &Path) -> Result<FileKind, DactylError> {
    if !path.exists() {
        return Ok(FileKind::Missing);
    }
    let mut file = File::open(path).map_err(|error| {
        coded(
            AdapterErrorKind::Storage,
            "corrupt_input",
            format!("open {}: {error}", path.display()),
        )
    })?;
    let mut header = [0_u8; 16];
    let read = file.read(&mut header).map_err(|error| {
        coded(
            AdapterErrorKind::Storage,
            "corrupt_input",
            format!("read {}: {error}", path.display()),
        )
    })?;
    if read == 0 {
        return Ok(FileKind::Empty);
    }
    if header.starts_with(SQLITE_MAGIC) {
        return Ok(FileKind::Sqlite);
    }
    if header[0] == b'{' {
        return Ok(FileKind::Dactyl);
    }
    Ok(FileKind::Unknown)
}

fn dest_readonly(path: &Path) -> bool {
    fs::metadata(path)
        .map(|meta| meta.permissions().readonly())
        .unwrap_or(false)
}

fn stores_equivalent(left: &Store, right: &Store) -> bool {
    store_schema(left) == store_schema(right) && table_rows(left) == table_rows(right)
}

fn table_rows(store: &Store) -> Vec<(String, Vec<Vec<serde_json::Value>>)> {
    store
        .tables
        .iter()
        .map(|(name, table)| (name.clone(), table.rows.clone()))
        .collect()
}

fn report(
    source: &Path,
    destination: &Path,
    already_converted: bool,
    store: &Store,
    backup: Option<String>,
) -> ImportReport {
    let schema = store_schema(store);
    ImportReport {
        source: source.display().to_string(),
        destination: destination.display().to_string(),
        already_converted,
        tables: schema.tables.len() as u64,
        indexes: schema.indexes.len() as u64,
        rows: schema.row_count(),
        backup,
    }
}

fn paths_equal(left: &Path, right: &Path) -> bool {
    left == right
        || fs::canonicalize(left)
            .ok()
            .zip(fs::canonicalize(right).ok())
            .is_some_and(|(left, right)| left == right)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn coded(kind: AdapterErrorKind, code: &str, message: impl Into<String>) -> DactylError {
    DactylError::adapter_with_code(kind, code, message)
}

fn corrupt(message: impl Into<String>) -> DactylError {
    coded(AdapterErrorKind::Storage, "corrupt_input", message)
}
