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
///
/// Adapters are constructed per call from [`crate::build_adapter`] and dropped
/// at the end of the call — there is no shared cache, so implementations do
/// not need to be `Sync` across calls. The trait is kept `Send + Sync` so a
/// caller could, if it chose to, hold an adapter across awaited points.
pub trait Adapter: Send + Sync {
    /// Execute any SQL statement (read or write) and return its rows.
    ///
    /// Parameters are bound by the adapter — never interpolated into `query`.
    fn execute(&self, query: &str, params: &[Parameter]) -> Result<Rows, DactylError>;

    /// Execute a raw schema/DDL/migration operation and return affected rows.
    fn execute_raw(&self, query: &str, params: &[Parameter]) -> Result<u64, DactylError>;

    /// Execute an atomic batch of statements.
    fn execute_batch(&self, statements: &[Statement]) -> Result<Vec<Rows>, DactylError>;
}

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "neon")]
pub mod neon;
