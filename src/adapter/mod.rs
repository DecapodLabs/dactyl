//! Private backend adapters.

use std::future::Future;
use std::pin::Pin;

use crate::connector::{CloudResponseMetadata, EffectProvenance, OperationAttempt};
use crate::contract::{
    AccessMode, AtomicResult, BackupResult, IntegrityReport, Operation, OperationKind,
    RecoveryOptions, RecoveryResult, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::inspection::ScanOutcome;
use crate::rows::{Parameter, Row, Rows};
use crate::schema::StoreSchema;

pub(crate) type AdapterFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DactylError>> + Send + 'a>>;

/// The small operation seam Dactyl needs from each backend.
pub trait Adapter: Send + Sync {
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
    fn read_async<'a>(&'a self, sql: &'a str, params: &'a [Parameter]) -> AdapterFuture<'a, Rows> {
        Box::pin(std::future::ready(self.read(sql, params)))
    }
    fn visit_rows(
        &self,
        sql: &str,
        params: &[Parameter],
        visitor: &mut dyn FnMut(&Row) -> Result<bool, DactylError>,
    ) -> Result<ScanOutcome, DactylError>;
    fn visit_rows_async<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
        visitor: &'a mut (dyn FnMut(&Row) -> Result<bool, DactylError> + Send),
    ) -> AdapterFuture<'a, ScanOutcome> {
        Box::pin(std::future::ready(self.visit_rows(sql, params, visitor)))
    }
    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError>;
    fn write_async<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
    ) -> AdapterFuture<'a, WriteResult> {
        Box::pin(std::future::ready(self.write(sql, params)))
    }
    fn write_with_attempt(
        &self,
        _sql: &str,
        _params: &[Parameter],
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<WriteResult, DactylError> {
        metadata_out.set_provenance(EffectProvenance::local_preflight(
            Some(OperationKind::Write),
            Some(attempt),
            Some("cloud_connector_required"),
            None,
        ));
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "cloud_connector_required",
            "local preflight: cloud effect provenance requires a cloud connector",
        ))
    }
    fn write_with_attempt_async<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
        attempt: &'a OperationAttempt,
        metadata_out: &'a mut CloudResponseMetadata,
    ) -> AdapterFuture<'a, WriteResult> {
        Box::pin(std::future::ready(self.write_with_attempt(
            sql,
            params,
            attempt,
            metadata_out,
        )))
    }
    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError>;
    fn atomic_async<'a>(&'a self, operations: &'a [Operation]) -> AdapterFuture<'a, AtomicResult> {
        Box::pin(std::future::ready(self.atomic(operations)))
    }
    fn atomic_with_attempt(
        &self,
        _operations: &[Operation],
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<AtomicResult, DactylError> {
        metadata_out.set_provenance(EffectProvenance::local_preflight(
            Some(OperationKind::Write),
            Some(attempt),
            Some("cloud_connector_required"),
            None,
        ));
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "cloud_connector_required",
            "local preflight: cloud effect provenance requires a cloud connector",
        ))
    }
    fn atomic_with_attempt_async<'a>(
        &'a self,
        operations: &'a [Operation],
        attempt: &'a OperationAttempt,
        metadata_out: &'a mut CloudResponseMetadata,
    ) -> AdapterFuture<'a, AtomicResult> {
        Box::pin(std::future::ready(self.atomic_with_attempt(
            operations,
            attempt,
            metadata_out,
        )))
    }
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
