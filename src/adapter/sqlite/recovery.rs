//! Filesystem custody for explicit recovery. Callers must quiesce both SQLite
//! clients and filesystem name writers in the trusted database directory.
use super::*;
use rustix::fs::{fchown, renameat_with, Gid, RenameFlags, Uid, CWD};
use std::fs::{DirBuilder, Metadata, Permissions};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};

const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];

#[derive(Clone, Debug)]
struct Identity {
    dev: u64,
    ino: u64,
}
impl Identity {
    fn of(metadata: &Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
    fn matches(&self, metadata: &Metadata) -> bool {
        self.dev == metadata.dev() && self.ino == metadata.ino()
    }
    fn check(&self, path: &Path) -> Result<(), DactylError> {
        let metadata =
            fs::symlink_metadata(path).map_err(|e| storage_io("inspect recovery path", e))?;
        if metadata.file_type().is_symlink() || !self.matches(&metadata) {
            return Err(conflict("recovery_path_changed", path));
        }
        Ok(())
    }
}

fn conflict(code: &str, path: &Path) -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Conflict,
        code,
        format!(
            "SQLite recovery path is unsafe or changed: {}",
            path.display()
        ),
    )
}

fn regular(path: &Path) -> Result<Metadata, DactylError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|e| storage_io("inspect recovery file", e))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(conflict("unsafe_recovery_file", path));
    }
    Ok(metadata)
}

fn reject_extended_metadata(path: &Path, directory: bool) -> Result<(), DactylError> {
    // POSIX ACLs and MAC labels cannot be safely reconstructed from mode bits.
    // This maintenance primitive deliberately supports plain POSIX metadata;
    // never discard source policy or activate an inherited default ACL/label.
    let mut value = [0_u8; 1024];
    match rustix::fs::listxattr(path, &mut value) {
        Ok(length) => {
            let unsupported = value[..length].split(|byte| *byte == 0).any(|name| {
                name.starts_with(b"security.")
                    || name.starts_with(b"trusted.")
                    || (name.starts_with(b"system.")
                        && !(directory && name == b"system.posix_acl_access"))
            });
            if !unsupported {
                return Ok(());
            }
            Err(DactylError::adapter_with_code(AdapterErrorKind::Capability, "recovery_metadata_unsupported",
                "SQLite recovery cannot preserve source ACLs/MAC labels or inherited directory access policy"))
        }
        Err(rustix::io::Errno::OPNOTSUPP) => Ok(()),
        Err(rustix::io::Errno::RANGE) => Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "recovery_metadata_unsupported",
            "SQLite recovery extended metadata exceeds the bounded policy inspection",
        )),
        Err(error) => Err(storage_io(
            "inspect SQLite recovery extended metadata",
            error.into(),
        )),
    }
}

fn absent(path: &Path) -> Result<(), DactylError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage_io("inspect recovery destination", error)),
        Ok(_) => Err(conflict("path_exists", path)),
    }
}

fn check_directory(path: &Path) -> Result<Identity, DactylError> {
    // Refuse symlink components, including a caller-selected symlinked parent.
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            continue;
        }
        let metadata = fs::symlink_metadata(ancestor)
            .map_err(|e| storage_io("inspect recovery directory", e))?;
        if !metadata.is_dir() {
            return Err(conflict("unsafe_recovery_directory", ancestor));
        }
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|e| storage_io("inspect recovery directory", e))?;
    Ok(Identity::of(&metadata))
}

fn move_noreplace(from: &Path, to: &Path, identity: &Identity) -> Result<(), DactylError> {
    identity.check(from)?;
    renameat_with(CWD, from, CWD, to, RenameFlags::NOREPLACE).map_err(|error| {
        if error == rustix::io::Errno::EXIST {
            conflict("path_exists", to)
        } else if error == rustix::io::Errno::XDEV {
            DactylError::adapter_with_code(
                AdapterErrorKind::InvalidOperation,
                "recovery_cross_filesystem",
                "SQLite recovery must remain on one filesystem",
            )
        } else if matches!(
            error,
            rustix::io::Errno::NOSYS | rustix::io::Errno::OPNOTSUPP | rustix::io::Errno::INVAL
        ) {
            DactylError::adapter_with_code(
                AdapterErrorKind::Capability,
                "secure_recovery_unsupported",
                "filesystem lacks atomic no-replace recovery rename",
            )
        } else {
            storage_io(
                "move SQLite recovery file without replacement",
                error.into(),
            )
        }
    })?;
    identity.check(to)
}

struct Stage {
    path: PathBuf,
    identity: Identity,
    parent: PathBuf,
    parent_identity: Identity,
    file: File,
    file_identity: Identity,
    retain: bool,
    journal_identity: Option<Identity>,
    quarantined: bool,
}
impl Stage {
    fn create(source: &Path, metadata: &Metadata) -> Result<Self, DactylError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self::create_with_nonce(source, metadata, now)
    }
    fn create_with_nonce(
        source: &Path,
        metadata: &Metadata,
        now: u128,
    ) -> Result<Self, DactylError> {
        let parent = source
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent_identity = check_directory(parent)?;
        if parent_identity.dev != metadata.dev() {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::InvalidOperation,
                "recovery_cross_filesystem",
                "SQLite recovery staging parent and active file must share a filesystem",
            ));
        }
        for attempt in 0..32_u32 {
            let path = parent.join(format!(
                ".dactyl-recovery-{}-{}",
                std::process::id(),
                now.saturating_add(u128::from(attempt))
            ));
            match DirBuilder::new().mode(0o700).create(&path) {
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(storage_io(
                        "create private SQLite recovery directory",
                        error,
                    ))
                }
                Ok(()) => {}
            }
            let directory = fs::symlink_metadata(&path)
                .map_err(|e| storage_io("inspect private recovery directory", e))?;
            if !directory.is_dir()
                || directory.mode() & 0o777 != 0o700
                || directory.dev() != metadata.dev()
            {
                return Err(conflict("unsafe_recovery_staging", &path));
            }
            let file = private_file(&path.join("rebuilt.db"))?;
            let file_identity = Identity::of(
                &file
                    .metadata()
                    .map_err(|e| storage_io("inspect recovery inode", e))?,
            );
            let mut stage = Self {
                path,
                identity: Identity::of(&directory),
                parent: parent.to_path_buf(),
                parent_identity,
                file,
                file_identity,
                retain: false,
                journal_identity: None,
                quarantined: false,
            };
            // Establish whether ownership can be preserved before reading data or
            // disturbing the original. The new inode stays 0600 until validated.
            let current = stage
                .file
                .metadata()
                .map_err(|e| storage_io("inspect recovery owner", e))?;
            if current.uid() != metadata.uid() || current.gid() != metadata.gid() {
                fchown(
                    &stage.file,
                    Some(Uid::from_raw(metadata.uid())),
                    Some(Gid::from_raw(metadata.gid())),
                )
                .map_err(|e| {
                    DactylError::adapter_with_code(
                        AdapterErrorKind::Authorization,
                        "recovery_ownership_unavailable",
                        format!("cannot preserve SQLite recovery ownership: {e}"),
                    )
                })?;
            }
            let journal = private_file(&sidecar_path(&stage.rebuilt(), "-journal"))?;
            stage.journal_identity =
                Some(Identity::of(&journal.metadata().map_err(|e| {
                    storage_io("inspect private recovery journal", e)
                })?));
            stage.check()?;
            sync_parent(&stage.path)?;
            checkpoint(Phase::Created, &mut stage)?;
            return Ok(stage);
        }
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Conflict,
            "recovery_staging_exhausted",
            "could not allocate fresh private SQLite recovery staging",
        ))
    }
    fn rebuilt(&self) -> PathBuf {
        self.path.join("rebuilt.db")
    }
    fn check(&self) -> Result<(), DactylError> {
        self.parent_identity.check(&self.parent)?;
        self.identity.check(&self.path)?;
        let metadata = fs::symlink_metadata(&self.path)
            .map_err(|e| storage_io("inspect recovery staging", e))?;
        if metadata.mode() & 0o777 != 0o700 {
            return Err(conflict("unsafe_recovery_staging", &self.path));
        }
        Ok(())
    }
    fn apply_mode(&self, source: &Metadata) -> Result<(), DactylError> {
        self.file_identity.check(&self.rebuilt())?;
        self.file
            .set_permissions(Permissions::from_mode(source.mode() & 0o666))
            .map_err(|e| storage_io("preserve recovered SQLite mode", e))?;
        self.file
            .sync_all()
            .map_err(|e| storage_io("sync recovered SQLite metadata", e))
    }
    fn sync(&self) -> Result<(), DactylError> {
        self.check()?;
        sync_parent(&self.rebuilt())?;
        sync_parent(&self.path)
    }
    fn quarantine(&mut self, source: &Path) -> Result<(), DactylError> {
        self.retain = true;
        self.check()?;
        self.file_identity.check(source)?;
        // Only our new inode is restricted; never chmod the original/archive.
        self.file
            .set_permissions(Permissions::from_mode(0o600))
            .map_err(|e| storage_io("restrict failed recovery inode", e))?;
        // Reopening DELETE mode does not create sidecars. An unexpected one
        // cannot be attributed to this operation, so preserve it untouched.
        for suffix in SIDECARS {
            absent(&sidecar_path(source, suffix))?;
        }
        let failed = self.path.join("failed.db");
        move_noreplace(source, &failed, &self.file_identity)?;
        self.quarantined = true;
        checkpoint(Phase::Quarantined, self)?;
        self.file
            .sync_all()
            .map_err(|e| storage_io("sync failed recovery inode", e))?;
        self.sync()
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if self.retain || self.check().is_err() {
            return;
        }
        // Never recursively traverse a replaced staging directory or unknown
        // contents. All SQLite-created sidecars are confined to this fresh 0700
        // directory while its identity is held for this operation.
        let rebuilt = self.rebuilt();
        if let Ok(metadata) = fs::symlink_metadata(&rebuilt) {
            if !self.file_identity.matches(&metadata) {
                return;
            }
            let _ = fs::remove_file(&rebuilt);
        }
        let journal = sidecar_path(&rebuilt, "-journal");
        if let Some(identity) = &self.journal_identity {
            if identity.check(&journal).is_ok() {
                let _ = fs::remove_file(journal);
            }
        }
        let _ = fs::remove_dir(&self.path);
        let _ = sync_parent(&self.path);
    }
}

fn private_file(path: &Path) -> Result<File, DactylError> {
    FsOpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| storage_io("create private SQLite recovery file", e))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Created,
    Rebuilding,
    Rebuilt,
    Synced,
    Archived,
    Activated,
    Reopen,
    Quarantined,
}
#[cfg(not(test))]
fn checkpoint(_: Phase, _: &mut Stage) -> Result<(), DactylError> {
    Ok(())
}
#[cfg(test)]
type RecoveryHook = Box<dyn FnMut(Phase, &mut Stage) -> Result<(), DactylError>>;
#[cfg(test)]
thread_local! { static HOOK: std::cell::RefCell<Option<RecoveryHook>> = std::cell::RefCell::new(None); }
#[cfg(test)]
fn checkpoint(phase: Phase, stage: &mut Stage) -> Result<(), DactylError> {
    HOOK.with(|hook| match hook.borrow_mut().as_mut() {
        Some(hook) => hook(phase, stage),
        None => Ok(()),
    })
}

pub(super) fn recover(
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
    let source = PathBuf::from(&adapter.path);
    let archive = PathBuf::from(&options.preserve_original_at);
    validate_recovery_paths(&source, &archive)?;
    let metadata = regular(&source)?;
    if metadata.mode() & 0o7111 != 0 {
        return Err(conflict("unsafe_recovery_mode", &source));
    }
    reject_extended_metadata(&source, false)?;
    reject_extended_metadata(
        source
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
        true,
    )?;
    if adapter.file_identity != Some((metadata.dev(), metadata.ino())) {
        return Err(conflict("recovery_path_changed", &source));
    }
    let original = Identity::of(&metadata);
    absent(&archive)?;
    for suffix in SIDECARS {
        absent(&sidecar_path(&archive, suffix))?;
        let sidecar = sidecar_path(&source, suffix);
        if fs::symlink_metadata(&sidecar).is_ok() {
            regular(&sidecar)?;
        }
    }
    let mut stage = Stage::create(&source, &metadata)?;
    let temporary = stage.rebuilt();
    let connection = adapter
        .connection
        .get_mut()
        .map_err(|_| adapter_error(AdapterErrorKind::Storage, "SQLite connection lock poisoned"))?;
    exec_sql(connection, "BEGIN EXCLUSIVE")
        .map_err(|e| connection.error_from_failure("begin SQLite recovery", e))?;
    let rebuild = rebuild_database(connection, &temporary, adapter.options.lock_timeout, || {
        checkpoint(Phase::Rebuilding, &mut stage)
    });
    let rollback = exec_sql(connection, "ROLLBACK")
        .map_err(|e| connection.error_from_failure("rollback SQLite recovery snapshot", e));
    let rebuild = match (rebuild, rollback) {
        (Ok(value), Ok(())) => value,
        (Err(error), Ok(())) => return Err(error),
        (Err(error), Err(rollback)) => return Err(recovery_rollback_error(error, rollback)),
        (Ok(_), Err(error)) => {
            return Err(recovery_rollback_error(
                error,
                adapter_error(AdapterErrorKind::Storage, "source snapshot rollback failed"),
            ))
        }
    };
    checkpoint(Phase::Rebuilt, &mut stage)?;
    stage.check()?;
    original.check(&source)?;
    stage.apply_mode(&metadata)?;
    sync_database_files(&temporary, false)?;
    checkpoint(Phase::Synced, &mut stage)?;
    stage.check()?;
    original.check(&source)?;
    connection.close()?;
    let mut moved: Vec<(PathBuf, PathBuf, Identity)> = Vec::new();
    let preserve = (|| {
        // Recheck every destination after rebuild. The actual moves are also
        // atomic no-clobber, including symlink and dangling-symlink collisions.
        for suffix in SIDECARS {
            absent(&sidecar_path(&archive, suffix))?;
        }
        move_noreplace(&source, &archive, &original)?;
        moved.push((source.clone(), archive.clone(), original.clone()));
        for suffix in SIDECARS {
            let from = sidecar_path(&source, suffix);
            match fs::symlink_metadata(&from) {
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(storage_io("inspect original SQLite sidecar", e)),
                Ok(_) => {
                    let identity = Identity::of(&regular(&from)?);
                    let to = sidecar_path(&archive, suffix);
                    move_noreplace(&from, &to, &identity)?;
                    moved.push((from, to, identity));
                }
            }
        }
        sync_database_files(&archive, true)?;
        checkpoint(Phase::Archived, &mut stage)?;
        stage.check()
    })();
    if let Err(error) = preserve {
        return rollback_original(adapter, error, &moved, &source, &original);
    }
    let activate = (|| {
        for suffix in SIDECARS {
            absent(&sidecar_path(&source, suffix))?;
        }
        move_noreplace(&temporary, &source, &stage.file_identity)?;
        Ok(())
    })();
    if let Err(error) = activate {
        return rollback_original(adapter, error, &moved, &source, &original);
    }
    let activation = (|| {
        checkpoint(Phase::Activated, &mut stage)?;
        stage.sync().map_err(|error| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Storage,
                "recovery_sync_failed",
                format!("sync activated SQLite recovery: {error}"),
            )
        })?;
        checkpoint(Phase::Reopen, &mut stage)?;
        stage.file_identity.check(&source)?;
        reopen_connection(adapter, &source).map_err(|error| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Unavailable,
                "recovery_reopen_failed",
                format!("reopen recovered SQLite database: {error}"),
            )
        })
    })();
    if let Err(error) = activation {
        let error = DactylError::adapter_with_code(
            error.adapter_kind().unwrap_or(AdapterErrorKind::Storage),
            error.adapter_code().unwrap_or("recovery_failed"),
            format!(
                "{error}; recovery diagnostics retained in {}",
                stage.path.display()
            ),
        );
        if let Ok(connection) = adapter.connection.get_mut() {
            let _ = connection.close();
        }
        if let Err(rollback) = stage.quarantine(&source) {
            let error = recovery_rollback_error(error, rollback);
            if stage.quarantined {
                return rollback_original(adapter, error, &moved, &source, &original);
            }
            return Err(error);
        }
        return rollback_original(adapter, error, &moved, &source, &original);
    }
    Ok(RecoveryResult {
        active_path: source.to_string_lossy().into_owned(),
        preserved_original_path: archive.to_string_lossy().into_owned(),
        journal_mode: RecoveryJournalMode::Delete,
        user_version: rebuild.user_version,
        application_id: rebuild.application_id,
    })
}

fn rollback_original<T>(
    adapter: &mut SqliteAdapter,
    error: DactylError,
    moved: &[(PathBuf, PathBuf, Identity)],
    source: &Path,
    original: &Identity,
) -> Result<T, DactylError> {
    // Restore sidecars before the main file; never replace a new name supplied
    // by another writer. On collision the archived original remains intact.
    for (from, to, identity) in moved.iter().rev() {
        if let Err(rollback) = move_noreplace(to, from, identity) {
            return Err(recovery_rollback_error(error, rollback));
        }
    }
    if let Err(rollback) = original.check(source) {
        return Err(recovery_rollback_error(error, rollback));
    }
    let sync = sync_database_files(source, true);
    let reopen = reopen_connection(adapter, source);
    if let Err(rollback) = sync.and(reopen) {
        return Err(recovery_rollback_error(error, rollback));
    }
    Err(error)
}

#[cfg(test)]
mod tests;

#[derive(Debug)]
struct SchemaObject {
    kind: String,
    name: String,
    sql: String,
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
    before_validation: impl FnOnce() -> Result<(), DactylError>,
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
        // Keep the pre-created 0600 rollback journal throughout the rebuild.
        // The final switch to DELETE removes it before single-file activation.
        exec_sql(&destination, "PRAGMA journal_mode = PERSIST").map_err(|error| {
            destination.error_from_failure("configure private recovery journal", error)
        })?;
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
        before_validation()?;
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
    if source_parent != archive_parent
        || source == archive
        || ["-wal", "-shm", "-journal"].iter().any(|suffix| {
            archive == sidecar_path(source, suffix) || source == sidecar_path(archive, suffix)
        })
    {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::InvalidOperation,
            "recovery_paths_must_share_parent",
            "SQLite recovery archive must be a different path in the database directory",
        ));
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
    #[cfg(unix)]
    {
        adapter.file_identity = file_identity(&path.to_string_lossy());
    }
    Ok(())
}
