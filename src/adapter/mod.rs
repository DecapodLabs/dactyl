//! Private backend adapters.

use crate::contract::{AccessMode, AtomicResult, Operation, WriteResult};
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Rows};
use crate::schema::StoreSchema;

/// The small operation seam Dactyl needs from each backend.
pub trait Adapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError>;
    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError>;
    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError>;
    fn access_mode(&self) -> AccessMode;
    fn inspect_schema(&self) -> Result<StoreSchema, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_schema_inspection",
            "schema inspection is a local-store operation",
        ))
    }
}

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "neon")]
pub mod neon;
