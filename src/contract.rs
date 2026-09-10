//! Backend-neutral physical operations.
//!
//! These types deliberately stop at the storage boundary. Callers own schema
//! policy, migration ordering, retry policy, and domain meaning; Dactyl owns
//! only execution, atomicity, access mode, and result normalization.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::rows::{Parameter, Rows};

/// The first version of the opaque storage-context envelope.
pub const STORAGE_CONTEXT_VERSION: u16 = 1;

/// Caller-owned context forwarded to a remote storage service.
///
/// Dactyl validates only the envelope: the version must be non-zero and the
/// payload must be a JSON object. The payload's fields and meaning belong to
/// the caller and the remote service; Dactyl does not interpret organization,
/// repository, membership, or authorization semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageContext {
    version: u16,
    payload: serde_json::Value,
}

impl StorageContext {
    /// Build a versioned opaque context without adopting its domain schema.
    pub fn new(
        version: u16,
        payload: serde_json::Value,
    ) -> Result<Self, crate::error::DactylError> {
        let context = Self { version, payload };
        context.validate()?;
        Ok(context)
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    /// Return the untouched caller-owned payload.
    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }

    pub(crate) fn validate(&self) -> Result<(), crate::error::DactylError> {
        if self.version == 0 {
            return Err(crate::error::DactylError::adapter_with_code(
                crate::error::AdapterErrorKind::Protocol,
                "invalid_context",
                "storage context version must be non-zero",
            ));
        }
        if !self.payload.is_object() {
            return Err(crate::error::DactylError::adapter_with_code(
                crate::error::AdapterErrorKind::Protocol,
                "invalid_context",
                "storage context payload must be a JSON object",
            ));
        }
        Ok(())
    }
}

/// Whether an opened route may mutate durable state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

/// The physical kind of an operation. Schema operations are caller-supplied
/// and are not migrations: Dactyl never assigns ids or ordering to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Read,
    Write,
    Schema,
}

/// An opaque, backend-neutral operation accepted by [`crate::Connection`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    pub(crate) kind: OperationKind,
    pub(crate) sql: String,
    #[serde(default)]
    pub(crate) params: Vec<Parameter>,
}

impl Operation {
    pub fn read(sql: impl Into<String>, params: impl Into<Vec<Parameter>>) -> Self {
        Self {
            kind: OperationKind::Read,
            sql: sql.into(),
            params: params.into(),
        }
    }

    pub fn write(sql: impl Into<String>, params: impl Into<Vec<Parameter>>) -> Self {
        Self {
            kind: OperationKind::Write,
            sql: sql.into(),
            params: params.into(),
        }
    }

    pub fn schema(sql: impl Into<String>, params: impl Into<Vec<Parameter>>) -> Self {
        Self {
            kind: OperationKind::Schema,
            sql: sql.into(),
            params: params.into(),
        }
    }

    pub fn kind(&self) -> OperationKind {
        self.kind
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn params(&self) -> &[Parameter] {
        &self.params
    }
}

/// A generated key explicitly returned by a write, never an ambient handle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GeneratedKey {
    Integer(i64),
    Text(String),
}

/// The normalized result of one physical write.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WriteResult {
    pub affected_rows: u64,
    #[serde(default)]
    pub generated_keys: Vec<GeneratedKey>,
}

impl WriteResult {
    pub fn generated_key(&self) -> Option<&GeneratedKey> {
        self.generated_keys.first()
    }
}

/// A result in an atomic batch, preserving operation order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OperationResult {
    Rows(Rows),
    Write(WriteResult),
}

/// The result of an opaque atomic batch.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AtomicResult {
    pub results: Vec<OperationResult>,
}

/// Options that affect physical opening, not migration or application policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub access_mode: AccessMode,
    pub lock_timeout: Duration,
}

/// The journal mode selected for a recovered SQLite database.
///
/// Logical dump/reload recovery intentionally activates the replacement in
/// rollback-journal mode. Re-enabling WAL is a separate, explicit caller
/// operation after the recovered connection has been reopened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryJournalMode {
    Delete,
}

/// Explicit operator input for logical SQLite recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryOptions {
    /// A new path at which Dactyl preserves the original database file and
    /// any `-wal`/`-shm` sidecars before activating the replacement.
    pub preserve_original_at: String,
    /// The journal mode used by the replacement. This is currently explicit
    /// and fixed to DELETE because it keeps activation a single-file atomic
    /// replacement; the enum leaves the decision visible in the result.
    pub journal_mode: RecoveryJournalMode,
}

impl RecoveryOptions {
    pub fn new(preserve_original_at: impl Into<String>, journal_mode: RecoveryJournalMode) -> Self {
        Self {
            preserve_original_at: preserve_original_at.into(),
            journal_mode,
        }
    }
}

/// A successful SQLite integrity verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub journal_mode: String,
    pub user_version: i64,
    pub application_id: i64,
}

/// The result of an online SQLite backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupResult {
    pub destination: String,
    pub source_journal_mode: String,
    pub destination_journal_mode: String,
    pub bytes: u64,
}

/// The result of a verified, atomically activated logical recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryResult {
    pub active_path: String,
    pub preserved_original_path: String,
    pub journal_mode: RecoveryJournalMode,
    pub user_version: i64,
    pub application_id: i64,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            access_mode: AccessMode::ReadWrite,
            lock_timeout: Duration::from_millis(250),
        }
    }
}
