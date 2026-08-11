//! Private backend adapters.

use crate::contract::{AccessMode, AtomicResult, Operation, WriteResult};
use crate::error::DactylError;
use crate::rows::{Parameter, Rows};

/// The small operation seam Dactyl needs from each backend.
pub trait Adapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError>;
    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError>;
    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError>;
    fn access_mode(&self) -> AccessMode;
}

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "neon")]
pub mod neon;
