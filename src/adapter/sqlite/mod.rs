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
use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::adapter::Adapter;
use crate::contract::{
    AccessMode, AtomicResult, BackupResult, GeneratedKey, IntegrityReport, OpenOptions, Operation,
    OperationKind, OperationResult, RecoveryJournalMode, RecoveryOptions, RecoveryResult,
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
    path: String,
}

struct SqliteConnection {
    api: Arc<ffi::Api>,
    database: *mut ffi::sqlite3,
    path: String,
}

// SQLite serializes access according to the flags supplied at open time. The
// adapter additionally holds the connection behind a Mutex, so the raw handle
// is never concurrently used by safe Dactyl code.
unsafe impl Send for SqliteConnection {}
unsafe impl Sync for SqliteConnection {}

static OPEN_CONNECTIONS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> = OnceLock::new();

fn open_connections() -> &'static Mutex<BTreeMap<PathBuf, usize>> {
    OPEN_CONNECTIONS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn path_key(path: &str) -> Option<PathBuf> {
    (path != ":memory:").then(|| {
        fs::canonicalize(path).unwrap_or_else(|_| {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                std::env::current_dir()
                    .map(|directory| directory.join(&path))
                    .unwrap_or(path)
            }
        })
    })
}

fn register_connection(path: &str) -> Result<(), DactylError> {
    let Some(path) = path_key(path) else {
        return Ok(());
    };
    let mut connections = open_connections().lock().map_err(|_| {
        DactylError::adapter(
            AdapterErrorKind::Storage,
            "SQLite connection registry lock poisoned",
        )
    })?;
    *connections.entry(path).or_default() += 1;
    Ok(())
}

fn unregister_connection(path: &str) {
    let Some(path) = path_key(path) else {
        return;
    };
    if let Ok(mut connections) = open_connections().lock() {
        if let Some(count) = connections.get_mut(&path) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                connections.remove(&path);
            }
        }
    }
}

fn has_other_connection(path: &str) -> Result<bool, DactylError> {
    let Some(path) = path_key(path) else {
        return Ok(false);
    };
    let connections = open_connections().lock().map_err(|_| {
        DactylError::adapter(
            AdapterErrorKind::Storage,
            "SQLite connection registry lock poisoned",
        )
    })?;
    Ok(connections.get(&path).copied().unwrap_or_default() > 1)
}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        if !self.database.is_null() {
            unsafe {
                let _ = self.api.close(self.database);
            }
        }
    }
}

impl SqliteConnection {
    fn close(&mut self) -> Result<(), DactylError> {
        if self.database.is_null() {
            return Ok(());
        }
        let code = unsafe { self.api.close(self.database) };
        if code == ffi::SQLITE_OK {
            self.database = std::ptr::null_mut();
            Ok(())
        } else {
            Err(self.error("close SQLite database", code))
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
            if !(options.access_mode == AccessMode::ReadWrite && !path_ref.exists()) {
                validate_existing_sqlite_header(path_ref)?;
            }
        }

        let api = Arc::new(ffi::Api::load()?);
        let database = open_database(&api, path, options.access_mode)?;
        let connection = SqliteConnection {
            api,
            database,
            path: path.to_string(),
        };
        let result = configure_connection(&connection, options);
        if let Err(error) = result {
            drop(connection);
            return Err(error);
        }

        register_connection(path)?;
        Ok(Self {
            connection: Mutex::new(connection),
            options,
            path: path.to_string(),
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, SqliteConnection>, DactylError> {
        self.connection.lock().map_err(|_| {
            DactylError::adapter(AdapterErrorKind::Storage, "SQLite connection lock poisoned")
        })
    }
}

impl Drop for SqliteAdapter {
    fn drop(&mut self) {
        unregister_connection(&self.path);
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

fn configure_connection(
    connection: &SqliteConnection,
    options: OpenOptions,
) -> Result<(), DactylError> {
    let timeout = options
        .lock_timeout
        .as_millis()
        .try_into()
        .unwrap_or(c_int::MAX);
    let code = unsafe { connection.api.busy_timeout(connection.database, timeout) };
    if code != ffi::SQLITE_OK {
        return Err(connection.error("configure SQLite busy timeout", code));
    }
    exec_sql(connection, "PRAGMA foreign_keys = ON")
        .map_err(|error| connection.error_from_failure("enable SQLite foreign keys", error))
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
            AdapterErrorKind::Corrupt,
            "malformed_database",
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

    fn verify_integrity(&self) -> Result<IntegrityReport, DactylError> {
        let connection = self.connection()?;
        verify_integrity(&connection)
    }

    fn backup(&self, destination: &Path) -> Result<BackupResult, DactylError> {
        let connection = self.connection()?;
        backup_database(&connection, destination, self.options.lock_timeout)
    }

    fn recover_from_dump_reload(
        &mut self,
        options: &RecoveryOptions,
    ) -> Result<RecoveryResult, DactylError> {
        if self.options.access_mode == AccessMode::ReadOnly {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::ReadOnly,
                "read_only",
                "SQLite recovery requires a read-write connection",
            ));
        }
        if has_other_connection(&self.path)? {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Conflict,
                "open_connections",
                "SQLite recovery requires all other Dactyl connections to be closed",
            ));
        }
        if options.journal_mode != RecoveryJournalMode::Delete {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Capability,
                "unsupported_recovery_journal_mode",
                "logical recovery currently activates DELETE journal mode only",
            ));
        }
        recover_database(self, options)
    }
}

fn verify_integrity(connection: &SqliteConnection) -> Result<IntegrityReport, DactylError> {
    let rows = query_rows(connection, "PRAGMA integrity_check", &[])?;
    let messages = rows
        .iter()
        .map(|row| row_string(row, 0))
        .collect::<Result<Vec<_>, _>>()?;
    if messages.is_empty() || messages.iter().any(|message| message != "ok") {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Corrupt,
            "integrity_check_failed",
            format!(
                "SQLite integrity_check failed: {}",
                if messages.is_empty() {
                    "no result".to_string()
                } else {
                    messages.join("; ")
                }
            ),
        ));
    }
    Ok(IntegrityReport {
        journal_mode: pragma_text(connection, "journal_mode")?,
        user_version: pragma_integer(connection, "user_version")?,
        application_id: pragma_integer(connection, "application_id")?,
    })
}

fn pragma_text(connection: &SqliteConnection, name: &str) -> Result<String, DactylError> {
    let rows = query_rows(connection, &format!("PRAGMA {name}"), &[])?;
    rows.as_slice()
        .first()
        .and_then(|row| row_value(row, 0))
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            adapter_error(
                AdapterErrorKind::Value,
                format!("SQLite PRAGMA {name} returned no text value"),
            )
        })
}

fn pragma_integer(connection: &SqliteConnection, name: &str) -> Result<i64, DactylError> {
    let rows = query_rows(connection, &format!("PRAGMA {name}"), &[])?;
    rows.as_slice()
        .first()
        .and_then(|row| row_value(row, 0))
        .and_then(|value| value.as_i64())
        .ok_or_else(|| {
            adapter_error(
                AdapterErrorKind::Value,
                format!("SQLite PRAGMA {name} returned no integer value"),
            )
        })
}

fn backup_database(
    source: &SqliteConnection,
    destination: &Path,
    lock_timeout: Duration,
) -> Result<BackupResult, DactylError> {
    if destination == Path::new(&source.path)
        || path_key(&source.path).as_ref() == path_key(&destination.to_string_lossy()).as_ref()
    {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Conflict,
            "backup_destination_is_source",
            "SQLite backup destination must differ from the active database",
        ));
    }
    ensure_parent(destination)?;
    ensure_absent(destination, "backup destination")?;
    ensure_absent(&sidecar_path(destination, "-wal"), "backup WAL sidecar")?;
    ensure_absent(&sidecar_path(destination, "-shm"), "backup SHM sidecar")?;

    let temporary = temporary_path(destination, "backup")?;
    let result = (|| {
        let api = source.api.clone();
        let database = open_database(&api, &temporary.to_string_lossy(), AccessMode::ReadWrite)?;
        let mut destination_connection = SqliteConnection {
            api,
            database,
            path: temporary.to_string_lossy().into_owned(),
        };
        configure_connection(
            &destination_connection,
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                lock_timeout,
            },
        )?;
        perform_backup(source, &destination_connection, lock_timeout)?;
        let destination_report = verify_integrity(&destination_connection)?;
        destination_connection.close()?;
        sync_database_files(&temporary, false)?;
        publish_temp_file(&temporary, destination)?;
        let bytes = fs::metadata(destination)
            .map_err(|error| storage_io("stat SQLite backup", error))?
            .len();
        Ok(BackupResult {
            destination: destination.to_string_lossy().into_owned(),
            source_journal_mode: pragma_text(source, "journal_mode")?,
            destination_journal_mode: destination_report.journal_mode,
            bytes,
        })
    })();

    if result.is_err() {
        let _ = remove_file_if_exists(&temporary);
        let _ = remove_file_if_exists(&sidecar_path(&temporary, "-wal"));
        let _ = remove_file_if_exists(&sidecar_path(&temporary, "-shm"));
    }
    result
}

fn perform_backup(
    source: &SqliteConnection,
    destination: &SqliteConnection,
    lock_timeout: Duration,
) -> Result<(), DactylError> {
    let main = CString::new("main").expect("static SQLite schema name has no NUL");
    let backup = unsafe {
        source.api.backup_init(
            destination.database,
            main.as_ptr(),
            source.database,
            main.as_ptr(),
        )
    };
    if backup.is_null() {
        let failure = unsafe { destination.api.failure(destination.database) };
        return Err(destination.error_from_failure("initialize SQLite online backup", failure));
    }

    let deadline = std::time::Instant::now() + lock_timeout;
    let mut result = Ok(());
    loop {
        let code = unsafe { source.api.backup_step(backup, 64) };
        match code {
            ffi::SQLITE_DONE => break,
            ffi::SQLITE_OK => continue,
            ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED if std::time::Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(1));
            }
            ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => {
                result = Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Timeout,
                    "backup_timeout",
                    "SQLite online backup exceeded the configured lock timeout",
                ));
                break;
            }
            _ => {
                result = Err(destination.error("step SQLite online backup", code));
                break;
            }
        }
    }
    let finish_code = unsafe { source.api.backup_finish(backup) };
    if result.is_ok() && finish_code != ffi::SQLITE_OK {
        result = Err(destination.error("finish SQLite online backup", finish_code));
    }
    result
}

#[derive(Debug)]
struct SchemaObject {
    kind: String,
    name: String,
    sql: String,
}

fn recover_database(
    adapter: &mut SqliteAdapter,
    options: &RecoveryOptions,
) -> Result<RecoveryResult, DactylError> {
    if adapter.path == ":memory:" {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_memory_recovery",
            "SQLite dump-reload recovery requires a file-backed database",
        ));
    }
    let source_path = PathBuf::from(&adapter.path);
    let archive_path = PathBuf::from(&options.preserve_original_at);
    validate_recovery_paths(&source_path, &archive_path)?;
    ensure_absent(&archive_path, "recovery archive")?;
    ensure_absent(&sidecar_path(&archive_path, "-wal"), "recovery WAL archive")?;
    ensure_absent(&sidecar_path(&archive_path, "-shm"), "recovery SHM archive")?;
    let temporary = temporary_path(&source_path, "recovery")?;

    let result = recover_database_inner(adapter, &source_path, &archive_path, &temporary);
    if result.is_err() {
        let _ = remove_file_if_exists(&temporary);
        let _ = remove_file_if_exists(&sidecar_path(&temporary, "-wal"));
        let _ = remove_file_if_exists(&sidecar_path(&temporary, "-shm"));
    }
    result
}

fn recover_database_inner(
    adapter: &mut SqliteAdapter,
    source_path: &Path,
    archive_path: &Path,
    temporary: &Path,
) -> Result<RecoveryResult, DactylError> {
    let connection = adapter.connection.get_mut().map_err(|_| {
        DactylError::adapter(AdapterErrorKind::Storage, "SQLite connection lock poisoned")
    })?;
    exec_sql(connection, "BEGIN EXCLUSIVE")
        .map_err(|error| connection.error_from_failure("begin SQLite recovery", error))?;

    let rebuild = rebuild_database(connection, temporary, adapter.options.lock_timeout);
    let rollback = exec_sql(connection, "ROLLBACK");
    let rebuild = match (rebuild, rollback) {
        (Ok(value), Ok(())) => value,
        (Err(error), Ok(())) => return Err(error),
        (Err(error), Err(rollback)) => {
            return Err(recovery_rollback_error(
                error,
                connection.error_from_failure("rollback SQLite recovery snapshot", rollback),
            ))
        }
        (Ok(_), Err(error)) => {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Storage,
                "recovery_rollback_failed",
                format!(
                    "rollback SQLite recovery snapshot: {}",
                    connection.error_from_failure("rollback SQLite recovery snapshot", error)
                ),
            ))
        }
    };
    sync_database_files(temporary, false)?;
    connection.close()?;

    let moved_sidecars = match move_original_to_archive(source_path, archive_path) {
        Ok(value) => value,
        Err(error) => {
            if let Err(rollback) = reopen_connection(adapter, source_path) {
                return Err(recovery_rollback_error(error, rollback));
            }
            return Err(error);
        }
    };
    if let Err(error) = sync_database_files(archive_path, true) {
        let rollback = restore_original_from_archive(source_path, archive_path, moved_sidecars);
        if let Err(rollback) = rollback {
            return Err(recovery_rollback_error(error, rollback));
        }
        if let Err(rollback) = reopen_connection(adapter, source_path) {
            return Err(recovery_rollback_error(error, rollback));
        }
        return Err(error);
    }
    if let Err(error) = fs::rename(temporary, source_path)
        .map_err(|error| storage_io("activate recovered SQLite database", error))
    {
        let rollback_error =
            restore_original_from_archive(source_path, archive_path, moved_sidecars);
        if let Err(rollback_error) = rollback_error {
            return Err(recovery_rollback_error(error, rollback_error));
        }
        reopen_connection(adapter, source_path)?;
        return Err(error);
    }
    if let Err(error) = sync_parent(source_path) {
        let failed_path = unused_path(source_path, "failed-recovery")?;
        let rollback_error = (|| {
            fs::rename(source_path, &failed_path).map_err(|error| {
                storage_io("quarantine unsynced recovered SQLite database", error)
            })?;
            restore_original_from_archive(source_path, archive_path, moved_sidecars)
        })();
        if let Err(rollback_error) = rollback_error {
            return Err(recovery_rollback_error(error, rollback_error));
        }
        reopen_connection(adapter, source_path)?;
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Storage,
            "recovery_sync_failed",
            format!("sync recovered SQLite database before activation: {error}"),
        ));
    }
    if let Err(error) = reopen_connection(adapter, source_path) {
        let failed_path = unused_path(source_path, "failed-recovery")?;
        let rollback_error = (|| {
            let connection = adapter.connection.get_mut().map_err(|_| {
                DactylError::adapter(AdapterErrorKind::Storage, "SQLite connection lock poisoned")
            })?;
            let _ = connection.close();
            fs::rename(source_path, &failed_path).map_err(|error| {
                storage_io("quarantine unreopenable recovered SQLite database", error)
            })?;
            restore_original_from_archive(source_path, archive_path, moved_sidecars)
        })();
        if let Err(rollback_error) = rollback_error {
            return Err(recovery_rollback_error(error, rollback_error));
        }
        reopen_connection(adapter, source_path)?;
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Unavailable,
            "recovery_reopen_failed",
            format!("reopen recovered SQLite database: {error}"),
        ));
    }

    Ok(RecoveryResult {
        active_path: source_path.to_string_lossy().into_owned(),
        preserved_original_path: archive_path.to_string_lossy().into_owned(),
        journal_mode: RecoveryJournalMode::Delete,
        user_version: rebuild.user_version,
        application_id: rebuild.application_id,
    })
}

#[derive(Debug)]
struct RebuildResult {
    user_version: i64,
    application_id: i64,
}

fn rebuild_database(
    source: &SqliteConnection,
    temporary: &Path,
    lock_timeout: Duration,
) -> Result<RebuildResult, DactylError> {
    let api = source.api.clone();
    let database = open_database(&api, &temporary.to_string_lossy(), AccessMode::ReadWrite)?;
    let mut destination = SqliteConnection {
        api,
        database,
        path: temporary.to_string_lossy().into_owned(),
    };
    let result = (|| {
        configure_connection(
            &destination,
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                lock_timeout,
            },
        )?;
        exec_sql(&destination, "PRAGMA foreign_keys = OFF").map_err(|error| {
            destination.error_from_failure("disable recovery foreign keys", error)
        })?;
        let user_version = pragma_integer(source, "user_version")?;
        let application_id = pragma_integer(source, "application_id")?;
        let objects = schema_objects(source)?;
        for object in objects.iter().filter(|object| object.kind == "table") {
            if object.sql.to_ascii_lowercase().contains("virtual table") {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Capability,
                    "unsupported_recovery_object",
                    format!(
                        "logical recovery does not support virtual table {}",
                        object.name
                    ),
                ));
            }
            exec_sql(&destination, &object.sql).map_err(|error| {
                destination.error_from_failure("create recovered SQLite table", error)
            })?;
        }
        for object in objects.iter().filter(|object| object.kind == "table") {
            copy_table_rows(source, &destination, &object.name)?;
        }
        if has_sqlite_sequence(source)? {
            copy_table_rows(source, &destination, "sqlite_sequence")?;
        }
        for object in objects
            .iter()
            .filter(|object| matches!(object.kind.as_str(), "index" | "view" | "trigger"))
        {
            exec_sql(&destination, &object.sql).map_err(|error| {
                destination.error_from_failure("create recovered SQLite schema object", error)
            })?;
        }
        exec_sql(
            &destination,
            &format!("PRAGMA user_version = {user_version}"),
        )
        .map_err(|error| destination.error_from_failure("restore SQLite user_version", error))?;
        exec_sql(
            &destination,
            &format!("PRAGMA application_id = {application_id}"),
        )
        .map_err(|error| destination.error_from_failure("restore SQLite application_id", error))?;
        exec_sql(&destination, "PRAGMA journal_mode = DELETE").map_err(|error| {
            destination.error_from_failure("set recovered SQLite journal mode", error)
        })?;
        let report = verify_integrity(&destination)?;
        if !report.journal_mode.eq_ignore_ascii_case("delete") {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Storage,
                "recovery_journal_mode_mismatch",
                format!("recovered SQLite journal mode is {}", report.journal_mode),
            ));
        }
        Ok(RebuildResult {
            user_version,
            application_id,
        })
    })();
    let close = destination.close();
    match (result, close) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn schema_objects(source: &SqliteConnection) -> Result<Vec<SchemaObject>, DactylError> {
    let rows = query_rows(
        source,
        "SELECT type, name, sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'view' THEN 2 WHEN 'trigger' THEN 3 ELSE 4 END, name",
        &[],
    )?;
    rows.iter()
        .map(|row| {
            Ok(SchemaObject {
                kind: row_string(row, 0)?,
                name: row_string(row, 1)?,
                sql: row_string(row, 2)?,
            })
        })
        .collect()
}

fn copy_table_rows(
    source: &SqliteConnection,
    destination: &SqliteConnection,
    table_name: &str,
) -> Result<(), DactylError> {
    let columns = table_columns(source, table_name)?;
    if columns.is_empty() {
        return Ok(());
    }
    let quoted_columns = columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    let select_sql = format!(
        "SELECT {} FROM {} NOT INDEXED",
        quoted_columns.join(", "),
        quote_identifier(table_name)
    );
    let placeholders = std::iter::repeat("?")
        .take(columns.len())
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_identifier(table_name),
        quoted_columns.join(", "),
        placeholders
    );
    let mut statement = Statement::prepare(source, &select_sql)?;
    while statement.step()? == ffi::SQLITE_ROW {
        let params = (0..columns.len())
            .map(|index| {
                let index = c_int::try_from(index).map_err(|_| {
                    adapter_error(AdapterErrorKind::Value, "too many SQLite table columns")
                })?;
                parameter_from_value(statement.value(index)?)
            })
            .collect::<Result<Vec<_>, _>>()?;
        execute_write(destination, &insert_sql, &params)?;
    }
    Ok(())
}

fn table_columns(source: &SqliteConnection, table_name: &str) -> Result<Vec<String>, DactylError> {
    let rows = query_rows(
        source,
        &format!("PRAGMA table_info({})", quote_identifier(table_name)),
        &[],
    )?;
    rows.iter().map(|row| row_string(row, 1)).collect()
}

fn has_sqlite_sequence(source: &SqliteConnection) -> Result<bool, DactylError> {
    Ok(!query_rows(
        source,
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_sequence'",
        &[],
    )?
    .is_empty())
}

fn parameter_from_value(value: serde_json::Value) -> Result<Parameter, DactylError> {
    match value {
        serde_json::Value::Null => Ok(Parameter::Null),
        serde_json::Value::Bool(value) => Ok(Parameter::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(Parameter::Integer)
            .or_else(|| value.as_f64().map(Parameter::Real))
            .ok_or_else(|| adapter_error(AdapterErrorKind::Value, "SQLite number is out of range")),
        serde_json::Value::String(value) => Ok(Parameter::Text(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|byte| u8::try_from(byte).ok())
                    .ok_or_else(|| {
                        adapter_error(AdapterErrorKind::Value, "SQLite blob byte is out of range")
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Parameter::Blob),
        serde_json::Value::Object(_) => Err(adapter_error(
            AdapterErrorKind::Value,
            "SQLite row value cannot be restored as a parameter",
        )),
    }
}

fn validate_recovery_paths(source: &Path, archive: &Path) -> Result<(), DactylError> {
    let source_parent = source.parent().unwrap_or_else(|| Path::new("."));
    let archive_parent = archive.parent().unwrap_or_else(|| Path::new("."));
    if source_parent != archive_parent || source == archive {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::InvalidOperation,
            "recovery_paths_must_share_parent",
            "SQLite recovery archive must be a different path in the database directory",
        ));
    }
    Ok(())
}

fn move_original_to_archive(source: &Path, archive: &Path) -> Result<(bool, bool), DactylError> {
    fs::rename(source, archive)
        .map_err(|error| storage_io("preserve original SQLite database", error))?;
    let source_wal = sidecar_path(source, "-wal");
    let archive_wal = sidecar_path(archive, "-wal");
    let moved_wal = source_wal.exists();
    if moved_wal {
        if let Err(error) = fs::rename(&source_wal, &archive_wal) {
            let _ = fs::rename(archive, source);
            return Err(storage_io("preserve original SQLite WAL sidecar", error));
        }
    }
    let source_shm = sidecar_path(source, "-shm");
    let archive_shm = sidecar_path(archive, "-shm");
    let moved_shm = source_shm.exists();
    if moved_shm {
        if let Err(error) = fs::rename(&source_shm, &archive_shm) {
            if moved_wal {
                let _ = fs::rename(&archive_wal, &source_wal);
            }
            let _ = fs::rename(archive, source);
            return Err(storage_io("preserve original SQLite SHM sidecar", error));
        }
    }
    Ok((moved_wal, moved_shm))
}

fn restore_original_from_archive(
    source: &Path,
    archive: &Path,
    moved_sidecars: (bool, bool),
) -> Result<(), DactylError> {
    fs::rename(archive, source)
        .map_err(|error| storage_io("rollback original SQLite database", error))?;
    if moved_sidecars.0 {
        fs::rename(sidecar_path(archive, "-wal"), sidecar_path(source, "-wal"))
            .map_err(|error| storage_io("rollback original SQLite WAL sidecar", error))?;
    }
    if moved_sidecars.1 {
        fs::rename(sidecar_path(archive, "-shm"), sidecar_path(source, "-shm"))
            .map_err(|error| storage_io("rollback original SQLite SHM sidecar", error))?;
    }
    Ok(())
}

fn reopen_connection(adapter: &mut SqliteAdapter, path: &Path) -> Result<(), DactylError> {
    let connection = adapter.connection.get_mut().map_err(|_| {
        DactylError::adapter(AdapterErrorKind::Storage, "SQLite connection lock poisoned")
    })?;
    let database = open_database(
        &connection.api,
        &path.to_string_lossy(),
        adapter.options.access_mode,
    )?;
    connection.database = database;
    if let Err(error) = configure_connection(connection, adapter.options) {
        let _ = connection.close();
        return Err(error);
    }
    Ok(())
}

fn recovery_rollback_error(original: DactylError, rollback: DactylError) -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Storage,
        "recovery_rollback_failed",
        format!("SQLite recovery failed: {original}; rollback failed: {rollback}"),
    )
}

fn perform_file_sync(path: &Path) -> Result<(), DactylError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| storage_io("sync SQLite file", error))
}

fn sync_database_files(path: &Path, include_sidecars: bool) -> Result<(), DactylError> {
    perform_file_sync(path)?;
    if include_sidecars {
        for suffix in ["-wal", "-shm"] {
            let sidecar = sidecar_path(path, suffix);
            if sidecar.exists() {
                perform_file_sync(&sidecar)?;
            }
        }
    }
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<(), DactylError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_io("sync SQLite database directory", error))
}

fn publish_temp_file(temporary: &Path, destination: &Path) -> Result<(), DactylError> {
    fs::rename(temporary, destination)
        .map_err(|error| storage_io("publish SQLite backup", error))?;
    if let Err(error) = sync_parent(destination) {
        let rollback = fs::rename(destination, temporary)
            .map_err(|rollback| storage_io("rollback SQLite backup publication", rollback));
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback) => Err(recovery_rollback_error(error, rollback)),
        };
    }
    Ok(())
}

fn ensure_parent(path: &Path) -> Result<(), DactylError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create SQLite maintenance directory", error))?;
    }
    Ok(())
}

fn ensure_absent(path: &Path, description: &str) -> Result<(), DactylError> {
    if path.exists() {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Conflict,
            "path_exists",
            format!("{description} already exists: {}", path.display()),
        ));
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<(), DactylError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage_io("remove SQLite maintenance file", error)),
    }
}

fn temporary_path(base: &Path, label: &str) -> Result<PathBuf, DactylError> {
    let parent = base.parent().unwrap_or_else(|| Path::new("."));
    let file_name = base
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            adapter_error(
                AdapterErrorKind::InvalidOperation,
                "SQLite path is not valid UTF-8",
            )
        })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..32_u32 {
        let candidate = parent.join(format!(
            ".{file_name}.dactyl-{label}-{}-{}",
            std::process::id(),
            now.saturating_add(u128::from(attempt))
        ));
        match FsOpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(storage_io(
                    "create SQLite maintenance temporary file",
                    error,
                ))
            }
        }
    }
    Err(adapter_error(
        AdapterErrorKind::Storage,
        "could not allocate a unique SQLite maintenance temporary file",
    ))
}

fn unused_path(base: &Path, label: &str) -> Result<PathBuf, DactylError> {
    let parent = base
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = base
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            adapter_error(
                AdapterErrorKind::InvalidOperation,
                "SQLite path is not valid UTF-8",
            )
        })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..32_u32 {
        let candidate = parent.join(format!(
            ".{file_name}.dactyl-{label}-{}-{}",
            std::process::id(),
            now.saturating_add(u128::from(attempt))
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(adapter_error(
        AdapterErrorKind::Storage,
        "could not allocate a unique SQLite quarantine path",
    ))
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn storage_io(operation: &str, error: std::io::Error) -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Storage,
        "filesystem_failure",
        format!("{operation}: {error}"),
    )
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
        ffi::SQLITE_NOTADB | ffi::SQLITE_CORRUPT => (AdapterErrorKind::Corrupt, "corrupt_database"),
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
