//! Private backend adapters.

use crate::contract::{
    AccessMode, AtomicResult, BackupResult, IntegrityReport, Operation, RecoveryOptions,
    RecoveryResult, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::inspection::ScanOutcome;
use crate::rows::{Parameter, Row, Rows};
use crate::schema::StoreSchema;

/// The small operation seam Dactyl needs from each backend.
pub trait Adapter {
    fn inspect_snapshot(
        &self,
        _limits: crate::SnapshotLimits,
        _inspect: &mut dyn FnMut(&mut dyn crate::SnapshotReader) -> Result<(), DactylError>,
    ) -> Result<(), DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_snapshot_inspection",
            "bounded snapshot inspection is unavailable for this datastore",
        ))
    }

    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError>;
    fn visit_rows(
        &self,
        sql: &str,
        params: &[Parameter],
        visitor: &mut dyn FnMut(&Row) -> Result<bool, DactylError>,
    ) -> Result<ScanOutcome, DactylError>;
    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError>;
    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError>;
    fn access_mode(&self) -> AccessMode;
    fn has_table(&self, _table: &str) -> Result<bool, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_schema_inspection",
            "schema inspection is a local-store operation",
        ))
    }
    fn has_column(&self, _table: &str, _column: &str) -> Result<bool, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_schema_inspection",
            "schema inspection is a local-store operation",
        ))
    }
    fn inspect_schema(&self) -> Result<StoreSchema, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_schema_inspection",
            "schema inspection is a local-store operation",
        ))
    }

    fn verify_integrity(&self) -> Result<IntegrityReport, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_integrity_verification",
            "integrity verification is a local SQLite operation",
        ))
    }

    fn backup(&self, _destination: &std::path::Path) -> Result<BackupResult, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_backup",
            "SQLite backup is unavailable for this datastore",
        ))
    }

    fn recover_from_dump_reload(
        &mut self,
        _options: &RecoveryOptions,
    ) -> Result<RecoveryResult, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_recovery",
            "SQLite dump-reload recovery is unavailable for this datastore",
        ))
    }
}

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "neon")]
pub mod neon;

#[cfg(feature = "supabase")]
pub(crate) mod supabase;
