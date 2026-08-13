//! Explicit, pure-Rust SQLite-to-Dactyl conversion.
//!
//! Opening a SQLite file as a Dactyl route still fails closed. This module is
//! the supported migration boundary: inspect a consistent read-only byte
//! snapshot with a small SQLite b-tree/record reader, convert into a native
//! snapshot, and publish through a temporary file plus optional backup.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Serialize;

use super::{
    execute_sql, load_store, persist_store, store_schema, AdapterErrorKind, DactylError, Store,
};
use crate::contract::OperationKind;

const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";
const SQLITE_HEADER_SIZE: usize = 100;

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

/// Convert a SQLite database into a Dactyl snapshot without a native database
/// dependency, SQLite subprocess, or caller-owned SQLite handle.
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
            let store = load_import_store(destination)?;
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
        let existing = load_import_store(destination)?;
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
    let reader = SqliteReader::open(path)?;
    let objects = reader.schema_objects()?;
    reject_unsupported_objects(&objects)?;

    let mut store = Store::default();
    let tables = objects
        .iter()
        .filter(|object| object.kind == "table" && !object.name.starts_with("sqlite_"))
        .collect::<Vec<_>>();
    for object in &tables {
        let sql = object.sql.as_ref().ok_or_else(|| {
            coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("table {} has no CREATE statement", object.name),
            )
        })?;
        reject_unsupported_table_sql(&object.name, sql)?;
        execute_sql(&mut store, sql, &[], OperationKind::Schema).map_err(|error| {
            coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("unsupported CREATE TABLE for {}: {error}", object.name),
            )
        })?;
    }

    for object in objects
        .iter()
        .filter(|object| object.kind == "index" && !object.name.starts_with("sqlite_"))
    {
        let Some(sql) = object.sql.as_ref() else {
            continue;
        };
        execute_sql(&mut store, sql, &[], OperationKind::Schema).map_err(|error| {
            coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("unsupported CREATE INDEX for {}: {error}", object.name),
            )
        })?;
    }

    for object in tables {
        let rows = reader.table_rows(object.root_page)?;
        let integer_primary_key = object.sql.as_deref().and_then(integer_primary_key_column);
        import_table_rows(
            &mut store,
            &object.name,
            rows,
            integer_primary_key.as_deref(),
        )?;
    }
    for (table_name, table) in &store.tables {
        for row in &table.rows {
            super::validate_foreign_keys(&store, table_name, row)?;
        }
    }
    Ok(store)
}

fn import_table_rows(
    store: &mut Store,
    name: &str,
    rows: Vec<(i64, Vec<serde_json::Value>)>,
    integer_primary_key: Option<&str>,
) -> Result<(), DactylError> {
    let indexes = store
        .indexes
        .values()
        .filter(|index| index.table == super::normalize(name) && index.unique)
        .cloned()
        .collect::<Vec<_>>();
    let table = store
        .tables
        .get_mut(&super::normalize(name))
        .ok_or_else(|| super::missing_table(name))?;
    let integer_primary_key = integer_primary_key.map(super::normalize);
    for (rowid, mut values) in rows {
        if values.len() + 1 == table.columns.len() {
            let Some(index) = table.columns.iter().position(|column| {
                column.primary_key && integer_primary_key.as_deref() == Some(column.name.as_str())
            }) else {
                return Err(coded(
                    AdapterErrorKind::Capability,
                    "unsupported_schema",
                    format!("row width mismatch for {name}"),
                ));
            };
            values.insert(index, rowid.into());
        } else if let Some(index) = table.columns.iter().position(|column| {
            column.primary_key && integer_primary_key.as_deref() == Some(column.name.as_str())
        }) {
            if values.get(index).is_some_and(serde_json::Value::is_null) {
                values[index] = rowid.into();
            }
        }
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
                table.next_id = table.next_id.max(value.saturating_add(1));
            }
        }
        table.rows.push(values);
    }
    Ok(())
}

fn reject_unsupported_objects(objects: &[SchemaObject]) -> Result<(), DactylError> {
    for object in objects {
        if matches!(object.kind.as_str(), "view" | "trigger") {
            return Err(coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("{} {} cannot be imported", object.kind, object.name),
            ));
        }
        if !matches!(object.kind.as_str(), "table" | "index") {
            return Err(coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("unsupported SQLite object {} {}", object.kind, object.name),
            ));
        }
    }
    Ok(())
}

fn reject_unsupported_table_sql(name: &str, sql: &str) -> Result<(), DactylError> {
    let keywords = sql_keywords(sql);
    for keyword in [
        "autoincrement",
        "check",
        "collate",
        "conflict",
        "deferrable",
        "generated",
        "initially",
        "match",
        "strict",
    ] {
        if keywords.iter().any(|candidate| candidate == keyword) {
            return Err(coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("unsupported SQLite schema construct {keyword} in table {name}"),
            ));
        }
    }
    if keywords.windows(2).any(|pair| pair == ["on", "update"]) {
        return Err(coded(
            AdapterErrorKind::Capability,
            "unsupported_schema",
            format!("ON UPDATE foreign keys are not supported in table {name}"),
        ));
    }
    Ok(())
}

fn sql_keywords(sql: &str) -> Vec<String> {
    let mut keywords = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '-' && chars.peek() == Some(&'-') {
            chars.next();
            for value in chars.by_ref() {
                if value == '\n' {
                    break;
                }
            }
            continue;
        }
        if character == '/' && chars.peek() == Some(&'*') {
            chars.next();
            while let Some(value) = chars.next() {
                if value == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    break;
                }
            }
            continue;
        }
        if character == '\'' {
            while let Some(value) = chars.next() {
                if value == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if matches!(character, '"' | '`' | '[') {
            let closing = if character == '[' { ']' } else { character };
            while let Some(value) = chars.next() {
                if value == closing {
                    if chars.peek() == Some(&closing) && closing != ']' {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if character.is_ascii_alphanumeric() || character == '_' {
            let mut keyword = character.to_ascii_lowercase().to_string();
            while let Some(&value) = chars.peek() {
                if value.is_ascii_alphanumeric() || value == '_' {
                    keyword.push(value.to_ascii_lowercase());
                    chars.next();
                } else {
                    break;
                }
            }
            keywords.push(keyword);
        }
    }
    keywords
}

fn integer_primary_key_column(sql: &str) -> Option<String> {
    let keywords = sql_keywords(sql);
    keywords
        .windows(4)
        .enumerate()
        .find(|(index, window)| {
            window[1] == "integer"
                && window[2] == "primary"
                && window[3] == "key"
                && keywords.get(index + 4).map(String::as_str) != Some("desc")
        })
        .map(|(_, window)| window[0].clone())
}

#[derive(Debug, Clone)]
struct SchemaObject {
    kind: String,
    name: String,
    root_page: u32,
    sql: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum TextEncoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

struct SqliteReader {
    bytes: Vec<u8>,
    page_size: usize,
    usable_size: usize,
    encoding: TextEncoding,
}

impl SqliteReader {
    fn open(path: &Path) -> Result<Self, DactylError> {
        let before = fs::metadata(path)
            .map_err(|error| corrupt(format!("inspect SQLite source: {error}")))?;
        let bytes =
            fs::read(path).map_err(|error| corrupt(format!("read SQLite source: {error}")))?;
        let after = fs::metadata(path)
            .map_err(|error| corrupt(format!("reinspect SQLite source: {error}")))?;
        if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
            return Err(coded(
                AdapterErrorKind::Storage,
                "source_changed",
                "SQLite source changed during the read-only import",
            ));
        }
        if bytes.len() < SQLITE_HEADER_SIZE || !bytes.starts_with(SQLITE_MAGIC) {
            return Err(coded(
                AdapterErrorKind::Storage,
                "corrupt_input",
                "SQLite header is missing or truncated",
            ));
        }
        let raw_page_size = read_u16(&bytes, 16)? as usize;
        let page_size = if raw_page_size == 1 {
            65_536
        } else {
            raw_page_size
        };
        if !(page_size == 65_536 || (512..=32_768).contains(&page_size))
            || !page_size.is_power_of_two()
        {
            return Err(coded(
                AdapterErrorKind::Capability,
                "unsupported_schema",
                format!("unsupported SQLite page size {page_size}"),
            ));
        }
        let reserved = bytes[20] as usize;
        if reserved >= page_size || bytes.len() % page_size != 0 {
            return Err(corrupt("invalid SQLite page geometry"));
        }
        let encoding = match read_u32(&bytes, 56)? {
            1 => TextEncoding::Utf8,
            2 => TextEncoding::Utf16Le,
            3 => TextEncoding::Utf16Be,
            value => {
                return Err(coded(
                    AdapterErrorKind::Capability,
                    "unsupported_value",
                    format!("unsupported SQLite text encoding {value}"),
                ))
            }
        };
        Ok(Self {
            bytes,
            page_size,
            usable_size: page_size - reserved,
            encoding,
        })
    }

    fn schema_objects(&self) -> Result<Vec<SchemaObject>, DactylError> {
        let rows = self.table_rows(1)?;
        let mut objects = Vec::with_capacity(rows.len());
        for (_, row) in rows {
            if row.len() != 5 {
                return Err(corrupt("SQLite schema row has an unexpected column count"));
            }
            let kind = json_text(&row[0], "schema type")?;
            let name = json_text(&row[1], "schema name")?;
            let root_page = row[3].as_i64().ok_or_else(|| {
                coded(
                    AdapterErrorKind::Capability,
                    "unsupported_schema",
                    format!("SQLite object {name} has no root page"),
                )
            })?;
            if root_page < 0 || root_page > u32::MAX as i64 {
                return Err(corrupt(format!(
                    "invalid root page for SQLite object {name}"
                )));
            }
            let sql = if row[4].is_null() {
                None
            } else {
                Some(json_text(&row[4], "schema SQL")?)
            };
            objects.push(SchemaObject {
                kind,
                name,
                root_page: root_page as u32,
                sql,
            });
        }
        objects.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(objects)
    }

    fn table_rows(
        &self,
        root_page: u32,
    ) -> Result<Vec<(i64, Vec<serde_json::Value>)>, DactylError> {
        if root_page == 0 {
            return Err(corrupt("SQLite table has a zero root page"));
        }
        let mut rows = Vec::new();
        let mut visited = HashSet::new();
        self.collect_table_page(root_page, &mut rows, &mut visited)?;
        Ok(rows)
    }

    fn collect_table_page(
        &self,
        page_number: u32,
        rows: &mut Vec<(i64, Vec<serde_json::Value>)>,
        visited: &mut HashSet<u32>,
    ) -> Result<(), DactylError> {
        if !visited.insert(page_number) {
            return Err(corrupt("SQLite b-tree contains a page cycle"));
        }
        let page = self.page(page_number)?;
        let header = if page_number == 1 { 100 } else { 0 };
        if page.len() < header + 8 {
            return Err(corrupt("SQLite b-tree page header is truncated"));
        }
        match page[header] {
            0x05 => {
                if page.len() < header + 12 {
                    return Err(corrupt("SQLite interior page header is truncated"));
                }
                let cells = read_u16(page, header + 3)? as usize;
                let rightmost = read_u32(page, header + 8)?;
                for index in 0..cells {
                    let pointer = read_u16(page, header + 12 + index * 2)? as usize;
                    let left_child = read_u32(page, pointer)?;
                    let (_, key_end) = read_varint(page, pointer + 4)?;
                    if pointer + 4 + key_end > page.len() {
                        return Err(corrupt("SQLite interior cell is truncated"));
                    }
                    self.collect_table_page(left_child, rows, visited)?;
                }
                self.collect_table_page(rightmost, rows, visited)?;
            }
            0x0d => {
                let cells = read_u16(page, header + 3)? as usize;
                for index in 0..cells {
                    let pointer = read_u16(page, header + 8 + index * 2)? as usize;
                    let (rowid, payload) = self.read_table_leaf_payload(page, pointer)?;
                    rows.push((rowid, self.decode_record(&payload)?));
                }
            }
            value => {
                return Err(coded(
                    AdapterErrorKind::Capability,
                    "unsupported_schema",
                    format!("unsupported SQLite table b-tree page type 0x{value:02x}"),
                ))
            }
        }
        Ok(())
    }

    fn read_table_leaf_payload(
        &self,
        page: &[u8],
        pointer: usize,
    ) -> Result<(i64, Vec<u8>), DactylError> {
        let (payload_size, after_payload_size) = read_varint(page, pointer)?;
        let (rowid, after_rowid) = read_varint(page, pointer + after_payload_size)?;
        let content_start = pointer + after_payload_size + after_rowid;
        let payload_size =
            usize::try_from(payload_size).map_err(|_| corrupt("SQLite payload is too large"))?;
        let max_local = self.usable_size.saturating_sub(35);
        let min_local = ((self.usable_size.saturating_sub(12)) * 32 / 255).saturating_sub(23);
        let local_size = if payload_size <= max_local {
            payload_size
        } else {
            let surplus =
                min_local + (payload_size - min_local) % self.usable_size.saturating_sub(4).max(1);
            if surplus > max_local {
                min_local
            } else {
                surplus
            }
        };
        if content_start.checked_add(local_size).is_none()
            || content_start + local_size > page.len()
        {
            return Err(corrupt("SQLite table cell payload is truncated"));
        }
        let mut payload = page[content_start..content_start + local_size].to_vec();
        if local_size == payload_size {
            return Ok((rowid as i64, payload));
        }
        let overflow_pointer = content_start + local_size;
        let next = read_u32(page, overflow_pointer)?;
        let mut remaining = payload_size - local_size;
        let mut visited = HashSet::new();
        let bytes_per_overflow_page = self.usable_size.saturating_sub(4);
        if bytes_per_overflow_page == 0 {
            return Err(corrupt("SQLite overflow page has no payload area"));
        }
        let mut page_number = next;
        while remaining > 0 {
            if page_number == 0 || !visited.insert(page_number) {
                return Err(corrupt("SQLite overflow chain is invalid"));
            }
            let overflow = self.page(page_number)?;
            let take = remaining.min(bytes_per_overflow_page);
            if overflow.len() < 4 + take {
                return Err(corrupt("SQLite overflow page is truncated"));
            }
            payload.extend_from_slice(&overflow[4..4 + take]);
            remaining -= take;
            page_number = read_u32(overflow, 0)?;
        }
        Ok((rowid as i64, payload))
    }

    fn decode_record(&self, payload: &[u8]) -> Result<Vec<serde_json::Value>, DactylError> {
        let (header_size, serial_start) = read_varint(payload, 0)?;
        let header_size = usize::try_from(header_size)
            .map_err(|_| corrupt("SQLite record header is too large"))?;
        if header_size < serial_start || header_size > payload.len() {
            return Err(corrupt("SQLite record header is truncated"));
        }
        let mut serial_types = Vec::new();
        let mut position = serial_start;
        while position < header_size {
            let (serial_type, consumed) = read_varint(payload, position)?;
            serial_types.push(serial_type);
            position += consumed;
        }
        if position != header_size {
            return Err(corrupt("SQLite record header has invalid serial types"));
        }
        let mut data_position = header_size;
        serial_types
            .into_iter()
            .map(|serial_type| self.decode_serial_value(serial_type, payload, &mut data_position))
            .collect()
    }

    fn decode_serial_value(
        &self,
        serial_type: u64,
        payload: &[u8],
        position: &mut usize,
    ) -> Result<serde_json::Value, DactylError> {
        let fixed_size = match serial_type {
            0 | 8 | 9 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 | 7 => 8,
            _ if serial_type >= 12 => ((serial_type - 12) / 2) as usize,
            _ => {
                return Err(corrupt(format!(
                    "reserved SQLite serial type {serial_type}"
                )))
            }
        };
        let end = position
            .checked_add(fixed_size)
            .ok_or_else(|| corrupt("SQLite record value is too large"))?;
        if end > payload.len() {
            return Err(corrupt("SQLite record value is truncated"));
        }
        let value = match serial_type {
            0 => serde_json::Value::Null,
            1..=6 => signed_integer(&payload[*position..end], serial_type as usize).into(),
            7 => serde_json::Number::from_f64(f64::from_bits(read_u64(payload, *position)?))
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    coded(
                        AdapterErrorKind::Value,
                        "unsupported_value",
                        "non-finite REAL cannot be imported",
                    )
                })?,
            8 => serde_json::Value::Number(0.into()),
            9 => serde_json::Value::Number(1.into()),
            value if value % 2 == 0 => serde_json::Value::Array(
                payload[*position..end]
                    .iter()
                    .map(|byte| serde_json::Value::Number(u64::from(*byte).into()))
                    .collect(),
            ),
            _ => decode_text(&payload[*position..end], self.encoding)?,
        };
        *position = end;
        Ok(value)
    }

    fn page(&self, page_number: u32) -> Result<&[u8], DactylError> {
        let page_number =
            usize::try_from(page_number).map_err(|_| corrupt("SQLite page number is too large"))?;
        if page_number == 0 {
            return Err(corrupt("SQLite page number is zero"));
        }
        let start = (page_number - 1)
            .checked_mul(self.page_size)
            .ok_or_else(|| corrupt("SQLite page offset overflow"))?;
        let end = start
            .checked_add(self.page_size)
            .ok_or_else(|| corrupt("SQLite page end overflow"))?;
        self.bytes
            .get(start..end)
            .ok_or_else(|| corrupt(format!("SQLite page {page_number} is outside the file")))
    }
}

fn decode_text(bytes: &[u8], encoding: TextEncoding) -> Result<serde_json::Value, DactylError> {
    let text = match encoding {
        TextEncoding::Utf8 => String::from_utf8(bytes.to_vec()).map_err(|_| {
            coded(
                AdapterErrorKind::Value,
                "unsupported_value",
                "SQLite text is not valid UTF-8",
            )
        })?,
        TextEncoding::Utf16Le => decode_utf16(bytes, true)?,
        TextEncoding::Utf16Be => decode_utf16(bytes, false)?,
    };
    Ok(serde_json::Value::String(text))
}

fn decode_utf16(bytes: &[u8], little_endian: bool) -> Result<String, DactylError> {
    if bytes.len() % 2 != 0 {
        return Err(coded(
            AdapterErrorKind::Value,
            "unsupported_value",
            "SQLite UTF-16 text has an odd byte length",
        ));
    }
    let units = bytes
        .chunks_exact(2)
        .map(|chunk| {
            if little_endian {
                u16::from_le_bytes([chunk[0], chunk[1]])
            } else {
                u16::from_be_bytes([chunk[0], chunk[1]])
            }
        })
        .collect::<Vec<_>>();
    String::from_utf16(&units).map_err(|_| {
        coded(
            AdapterErrorKind::Value,
            "unsupported_value",
            "SQLite UTF-16 text is invalid",
        )
    })
}

fn read_varint(bytes: &[u8], start: usize) -> Result<(u64, usize), DactylError> {
    let mut value = 0_u64;
    for index in 0..9 {
        let byte = *bytes
            .get(start + index)
            .ok_or_else(|| corrupt("SQLite varint is truncated"))?;
        if index == 8 {
            return Ok(((value << 8) | u64::from(byte), 9));
        }
        value = (value << 7) | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(corrupt("SQLite varint is invalid"))
}

fn signed_integer(bytes: &[u8], serial_type: usize) -> i64 {
    let mut value = 0_u64;
    for byte in bytes {
        value = (value << 8) | u64::from(*byte);
    }
    let width = match serial_type {
        1 => 8,
        2 => 16,
        3 => 24,
        4 => 32,
        5 => 48,
        6 => 64,
        _ => 64,
    };
    if width < 64 && value & (1_u64 << (width - 1)) != 0 {
        (value | (!0_u64 << width)) as i64
    } else {
        value as i64
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, DactylError> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| corrupt("SQLite u16 is truncated"))?;
    Ok(u16::from_be_bytes([value[0], value[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, DactylError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| corrupt("SQLite u32 is truncated"))?;
    Ok(u32::from_be_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, DactylError> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| corrupt("SQLite u64 is truncated"))?;
    Ok(u64::from_be_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

fn json_text(value: &serde_json::Value, label: &str) -> Result<String, DactylError> {
    value.as_str().map(str::to_owned).ok_or_else(|| {
        coded(
            AdapterErrorKind::Capability,
            "unsupported_schema",
            format!("SQLite {label} is not text"),
        )
    })
}

fn load_import_store(path: &Path) -> Result<Store, DactylError> {
    match load_store(path, crate::contract::AccessMode::ReadOnly) {
        Ok(store) => Ok(store),
        Err(DactylError::Adapter {
            kind: AdapterErrorKind::Storage,
            code: None,
            message,
        }) => Err(coded(AdapterErrorKind::Storage, "corrupt_input", message)),
        Err(error) => Err(error),
    }
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

fn coded(kind: AdapterErrorKind, code: &str, message: impl Into<String>) -> DactylError {
    DactylError::adapter_with_code(kind, code, message)
}

fn corrupt(message: impl Into<String>) -> DactylError {
    coded(AdapterErrorKind::Storage, "corrupt_input", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_decoder_rejects_invalid_utf8_without_replacement() {
        let error = decode_text(&[0xff, 0xfe], TextEncoding::Utf8).unwrap_err();
        assert_eq!(error.adapter_code(), Some("unsupported_value"));
    }

    #[test]
    fn schema_keyword_scanner_ignores_literals_comments_and_identifiers() {
        let sql = "create table \"check\" (note text default 'check -- collate') -- check\n";
        let keywords = sql_keywords(sql);
        assert!(!keywords.iter().any(|keyword| keyword == "check"));
        assert!(reject_unsupported_table_sql("check", sql).is_ok());
        assert!(reject_unsupported_table_sql(
            "bad",
            "create table bad (value text check (length(value) > 0))"
        )
        .is_err());
    }

    #[test]
    fn signed_integer_decoding_preserves_sqlite_widths() {
        assert_eq!(signed_integer(&[0xff], 1), -1);
        assert_eq!(signed_integer(&[0xff, 0xff, 0xff], 3), -1);
        assert_eq!(
            signed_integer(&[0x80, 0, 0, 0, 0, 0], 5),
            -140_737_488_355_328
        );
    }
}
