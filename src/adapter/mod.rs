//! Adapter trait — internal to dactyl.
//!
//! Both `SqliteAdapter` and `NeonAdapter` live behind their respective
//! feature gates and are reachable as `dactyl::adapter::sqlite::*` /
//! `dactyl::adapter::neon::*`. Nothing else at the crate root re-exports
//! the underlying types.

use crate::error::DactylError;
use crate::rows::{Parameter, Rows};
use crate::Statement;

/// Internal trait every adapter implements.
pub trait Adapter: Send + Sync {
    /// Execute a query against the backing datastore.
    fn execute(
        &self,
        query: &str,
        params: &[Parameter],
        optimize: bool,
        write: bool,
    ) -> Result<Rows, DactylError>;

    /// Execute a raw schema/DDL/migration operation.
    fn execute_raw(&self, query: &str, params: &[Parameter]) -> Result<u64, DactylError>;

    /// Execute an atomic batch of statements.
    fn execute_batch(&self, statements: &[Statement]) -> Result<Vec<Rows>, DactylError>;
}

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "neon")]
pub mod neon;
